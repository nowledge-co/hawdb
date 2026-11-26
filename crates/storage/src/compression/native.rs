// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::backend::{
    CodecProgress, CompressionBackend, CompressionDecoder, CompressionEncoder, DecoderState,
    EncodeOperation, EncoderState,
};
use super::{context, FrameHeader};
use crate::background::CheckpointWorkContext;
use std::io;
use zstd::stream::raw::{InBuffer, OutBuffer};
use zstd::zstd_safe::zstd_sys as sys;

// Memory admission is qualified separately from the private custom-memory ABI.
const _: () = assert!(
    matches!(sys::ZSTD_VERSION_NUMBER, 10507 | 10600),
    "requalify Zstd memory admission before updating the bindings"
);

/// Test fixture: statically linked Zstd with Rust allocation callbacks.
/// Production hosts must supply their own compression implementation.
#[derive(Debug)]
pub struct TestZstdBackend;

impl CompressionBackend for TestZstdBackend {
    fn encoder(
        &self,
        level: i32,
        work: Option<&CheckpointWorkContext>,
    ) -> io::Result<EncoderState> {
        EncoderState::new(
            match work {
                Some(work) => context::CompressionContext::new_with_work_context(level, work)?,
                None => context::CompressionContext::new(level)?,
            },
            work,
        )
    }

    fn decoder(&self, work: Option<&CheckpointWorkContext>) -> io::Result<DecoderState> {
        DecoderState::new(
            match work {
                Some(work) => context::DecompressionContext::new_with_work_context(work)?,
                None => context::DecompressionContext::new()?,
            },
            work,
        )
    }

    fn encoder_workspace_bytes(&self, level: i32) -> io::Result<usize> {
        context::require_version()?;
        if level != 3 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "workspace bound is qualified for level 3",
            ));
        }
        Ok(8 * 1024 * 1024)
    }

    fn decoder_context_bytes(&self) -> io::Result<usize> {
        context::require_version()?;
        Ok(256 * 1024)
    }

    fn decoder_buffer_bytes(&self, header: &FrameHeader) -> io::Result<usize> {
        context::require_version()?;
        if header.skippable {
            return Ok(0);
        }
        let output = header.content_bytes.unwrap_or(u64::MAX).min(
            header
                .window_bytes
                .max(1024)
                .checked_add(
                    header
                        .block_bytes
                        .checked_mul(2)
                        .and_then(|bytes| bytes.checked_add(64))
                        .ok_or_else(|| io::Error::other("Zstd buffer bound overflow"))?,
                )
                .ok_or_else(|| io::Error::other("Zstd buffer bound overflow"))?,
        );
        usize::try_from(
            output
                .checked_add(header.block_bytes.max(4))
                .ok_or_else(|| io::Error::other("Zstd buffer bound overflow"))?,
        )
        .map_err(|_| io::Error::other("Zstd buffer bound exceeds address space"))
    }

    fn compress_bound(&self, input_len: usize) -> io::Result<usize> {
        context::require_version()?;
        Ok(zstd::zstd_safe::compress_bound(input_len))
    }
}

impl CompressionEncoder for context::CompressionContext {
    fn set_pledged_src_size(&mut self, bytes: u64) -> io::Result<()> {
        self.set_pledged_src_size(bytes)
    }

    fn step(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        operation: EncodeOperation,
    ) -> io::Result<CodecProgress> {
        let mut input = InBuffer::around(input);
        let mut output = OutBuffer::around(output);
        let directive = match operation {
            EncodeOperation::Continue => sys::ZSTD_EndDirective::ZSTD_e_continue,
            EncodeOperation::Flush => sys::ZSTD_EndDirective::ZSTD_e_flush,
            EncodeOperation::Finish => sys::ZSTD_EndDirective::ZSTD_e_end,
        };
        let remaining =
            context::CompressionContext::step(self, &mut input, &mut output, directive)?;
        Ok(CodecProgress {
            consumed: input.pos(),
            produced: output.pos(),
            remaining,
        })
    }

    fn close(self: Box<Self>) -> io::Result<()> {
        (*self).close()
    }
}

impl CompressionDecoder for context::DecompressionContext {
    fn reset(&mut self) -> io::Result<()> {
        self.reset()
    }
    fn retained_bytes(&self) -> usize {
        self.accounted_bytes()
    }
    fn step(&mut self, input: &[u8], output: &mut [u8]) -> io::Result<CodecProgress> {
        let mut input = InBuffer::around(input);
        let mut output = OutBuffer::around(output);
        let remaining = self.decompress_stream(&mut output, &mut input)?;
        Ok(CodecProgress {
            consumed: input.pos(),
            produced: output.pos(),
            remaining,
        })
    }
}
