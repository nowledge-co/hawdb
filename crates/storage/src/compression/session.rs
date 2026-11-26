// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::backend::{self, CodecProgress, DecoderState, EncodeOperation, EncoderState};
use crate::background::CheckpointWorkContext;
use std::io;

pub(super) struct CompressionContext {
    state: EncoderState,
    failed: bool,
}

impl CompressionContext {
    pub(super) fn new(level: i32, work: Option<&CheckpointWorkContext>) -> io::Result<Self> {
        Ok(Self {
            state: backend::selected()?.encoder(level, work)?,
            failed: false,
        })
    }

    pub(super) fn set_pledged_src_size(&mut self, bytes: u64) -> io::Result<()> {
        self.ready()?;
        let result = self.state.encoder.set_pledged_src_size(bytes);
        self.failed = result.is_err();
        result
    }

    fn ready(&self) -> io::Result<()> {
        if self.failed {
            Err(io::Error::other(
                "compression encoder requires reset after failure",
            ))
        } else {
            Ok(())
        }
    }

    pub(super) fn step(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        operation: EncodeOperation,
    ) -> io::Result<CodecProgress> {
        self.ready()?;
        let result = self
            .state
            .encoder
            .step(input, output, operation)
            .and_then(|progress| progress.checked(input.len(), output.len()));
        self.failed = result.is_err();
        result
    }

    pub(super) fn close(self) -> io::Result<()> {
        self.ready()?;
        self.state.close()
    }
}

/// Backend-neutral decoder with modern-frame validation and failure fencing.
pub struct DecompressionContext {
    state: DecoderState,
    failed: bool,
    finished: bool,
    prefix: [u8; 4],
    prefix_length: usize,
    prefix_position: usize,
}

impl DecompressionContext {
    pub fn new() -> io::Result<Self> {
        Self::with_work_context(None)
    }

    pub(super) fn with_work_context(work: Option<&CheckpointWorkContext>) -> io::Result<Self> {
        Ok(Self {
            state: backend::selected()?.decoder(work)?,
            failed: false,
            finished: false,
            prefix: [0; 4],
            prefix_length: 0,
            prefix_position: 0,
        })
    }

    pub fn reset(&mut self) -> io::Result<()> {
        self.failed = true;
        self.state.decoder.reset()?;
        self.prefix_length = 0;
        self.prefix_position = 0;
        self.finished = false;
        self.failed = false;
        Ok(())
    }

    pub fn sizeof(&self) -> usize {
        self.state
            .decoder
            .retained_bytes()
            .saturating_add(self.state.state_bytes)
    }

    /// Decode at most one frame. Positions are relative to these slices.
    pub fn step(&mut self, input: &[u8], output: &mut [u8]) -> io::Result<CodecProgress> {
        if self.failed {
            return Err(io::Error::other(
                "Zstd decoder requires reset after failure",
            ));
        }
        let result = self.step_inner(input, output);
        self.failed = result.is_err();
        result
    }

    fn step_inner(&mut self, input: &[u8], output: &mut [u8]) -> io::Result<CodecProgress> {
        if self.finished {
            self.reset()?;
        }
        let mut consumed = 0;
        if self.prefix_length < 4 {
            consumed = (4 - self.prefix_length).min(input.len());
            self.prefix[self.prefix_length..self.prefix_length + consumed]
                .copy_from_slice(&input[..consumed]);
            self.prefix_length += consumed;
            if self.prefix_length < 4 {
                return Ok(CodecProgress {
                    consumed,
                    produced: 0,
                    remaining: 4 - self.prefix_length,
                });
            }
            // Legacy Zstd decoders can bypass custom allocator callbacks.
            // Enforce this boundary for every provider before native decoding.
            let magic = u32::from_le_bytes(self.prefix);
            if magic != 0xfd2fb528 && magic & 0xfffffff0 != 0x184d2a50 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid Zstd frame magic",
                ));
            }
        }
        while self.prefix_position < 4 {
            let prefix = &self.prefix[self.prefix_position..];
            let progress = self
                .state
                .decoder
                .step(prefix, output)?
                .checked(prefix.len(), output.len())?;
            self.prefix_position += progress.consumed;
            self.finished = progress.remaining == 0;
            if consumed > 0 || progress.produced > 0 || self.finished {
                return Ok(CodecProgress {
                    consumed,
                    ..progress
                });
            }
            if progress.consumed == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "compression decoder made no progress",
                ));
            }
        }
        let remaining_input = &input[consumed..];
        let progress = self
            .state
            .decoder
            .step(remaining_input, output)?
            .checked(remaining_input.len(), output.len())?;
        self.finished = progress.remaining == 0;
        Ok(CodecProgress {
            consumed: consumed + progress.consumed,
            ..progress
        })
    }
}
