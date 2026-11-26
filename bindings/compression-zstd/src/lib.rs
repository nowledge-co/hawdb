// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Native codec owned by the language bindings, outside the database dependency graph.

use hawdb::compression::*;
use hawdb::RuntimeMemoryPermit;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Once};
use zstd::zstd_safe::{self as safe, InBuffer, OutBuffer};

const MIB: usize = 1024 * 1024;
const ENCODER_NATIVE_BYTES: usize = 7 * MIB;
const DECODER_NATIVE_BYTES: usize = 192 * 1024;
const MAX_WINDOW: u64 = 1 << 27;
const MAX_BUFFER_UNITS: usize = 129;

/// Install the binding's backend unless a host already installed its own.
pub fn install() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        let _ = install_compression_backend(Arc::new(ZstdBackend));
    });
}

#[derive(Debug)]
pub struct ZstdBackend;

fn native<T>(result: Result<T, usize>) -> io::Result<T> {
    result.map_err(|error| io::Error::other(safe::get_error_name(error)))
}

fn qualified() -> io::Result<()> {
    // These versions free the old streaming decode buffer before replacement.
    // Bounds must be requalified before accepting a different implementation.
    if matches!(safe::version_number(), 10507 | 10600) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unqualified binding Zstd version",
        ))
    }
}

struct Reservation {
    permit: Option<Box<dyn RuntimeMemoryPermit>>,
    work: CheckpointWorkContext,
    bytes: usize,
}

impl Reservation {
    fn acquire(work: Option<&CheckpointWorkContext>, bytes: usize) -> io::Result<Option<Self>> {
        let Some(work) = work else { return Ok(None) };
        let permit = catch_unwind(AssertUnwindSafe(|| work.reserve_memory(bytes)))
            .map_err(|_| Self::panicked(work, bytes, "reserve"))?
            .map_err(io::Error::other)?;
        Ok(permit.map(|permit| Self {
            permit: Some(permit),
            work: work.clone(),
            bytes,
        }))
    }

    fn panicked(work: &CheckpointWorkContext, bytes: usize, phase: &str) -> io::Error {
        io::Error::other(work.record_failure(CheckpointWorkError::Allocation {
            bytes: bytes as u64,
            reason: format!("binding Zstd memory callback panicked during {phase}"),
        }))
    }

    fn release(&mut self) -> io::Result<()> {
        let permit = self.permit.take();
        catch_unwind(AssertUnwindSafe(|| drop(permit)))
            .map_err(|_| Self::panicked(&self.work, self.bytes, "release"))
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

impl CompressionBackend for ZstdBackend {
    fn encoder(
        &self,
        level: i32,
        work: Option<&CheckpointWorkContext>,
    ) -> io::Result<EncoderState> {
        self.encoder_workspace_bytes(level)?;
        let memory = Reservation::acquire(work, ENCODER_NATIVE_BYTES)?;
        let mut context = safe::CCtx::try_create().ok_or(io::ErrorKind::OutOfMemory)?;
        native(context.set_parameter(safe::CParameter::CompressionLevel(level)))?;
        EncoderState::new(Encoder { context, memory }, work)
    }

    fn decoder(&self, work: Option<&CheckpointWorkContext>) -> io::Result<DecoderState> {
        DecoderState::new(Decoder::new(work)?, work)
    }

    fn encoder_workspace_bytes(&self, level: i32) -> io::Result<usize> {
        qualified()?;
        if level != 3 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "binding Zstd supports compression level 3",
            ));
        }
        Ok(8 * MIB)
    }

    fn decoder_context_bytes(&self) -> io::Result<usize> {
        qualified()?;
        Ok(256 * 1024)
    }

    fn decoder_buffer_bytes(&self, header: &FrameHeader) -> io::Result<usize> {
        qualified()?;
        if header.skippable {
            return Ok(0);
        }
        if header.window_bytes > MAX_WINDOW || header.block_bytes > 128 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Zstd window exceeds binding limit",
            ));
        }
        let output = header
            .content_bytes
            .unwrap_or(u64::MAX)
            .min(header.window_bytes.max(1024) + 2 * header.block_bytes + 64);
        let bytes =
            usize::try_from(output + header.block_bytes.max(4)).map_err(io::Error::other)?;
        Ok(bytes.div_ceil(MIB) * MIB)
    }

    fn compress_bound(&self, input_len: usize) -> io::Result<usize> {
        qualified()?;
        Ok(safe::compress_bound(input_len))
    }
}

struct Encoder {
    // Physical native release must precede the permit refund.
    context: safe::CCtx<'static>,
    memory: Option<Reservation>,
}

impl CompressionEncoder for Encoder {
    fn set_pledged_src_size(&mut self, bytes: u64) -> io::Result<()> {
        native(self.context.set_pledged_src_size(Some(bytes))).map(|_| ())
    }

    fn step(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        operation: EncodeOperation,
    ) -> io::Result<CodecProgress> {
        let mut input = InBuffer::around(input);
        let mut output = OutBuffer::around(output);
        let operation = match operation {
            EncodeOperation::Continue => safe::zstd_sys::ZSTD_EndDirective::ZSTD_e_continue,
            EncodeOperation::Flush => safe::zstd_sys::ZSTD_EndDirective::ZSTD_e_flush,
            EncodeOperation::Finish => safe::zstd_sys::ZSTD_EndDirective::ZSTD_e_end,
        };
        let remaining = native(
            self.context
                .compress_stream2(&mut output, &mut input, operation),
        )?;
        Ok(CodecProgress {
            consumed: input.pos(),
            produced: output.pos(),
            remaining,
        })
    }

    fn close(self: Box<Self>) -> io::Result<()> {
        let Self { context, memory } = *self;
        drop(context);
        memory
            .map(|mut memory| memory.release())
            .transpose()
            .map(|_| ())
    }
}

struct Decoder {
    context: safe::DCtx<'static>,
    _memory: Option<Reservation>,
    // Fixed bookkeeping is covered by DecoderState's admission; no hidden Vec growth.
    buffers: [Option<Reservation>; MAX_BUFFER_UNITS],
    buffer_units: usize,
    work: Option<CheckpointWorkContext>,
    header: [u8; 18],
    header_len: usize,
    header_position: usize,
    admitted: bool,
}

impl Decoder {
    fn new(work: Option<&CheckpointWorkContext>) -> io::Result<Self> {
        qualified()?;
        let memory = Reservation::acquire(work, DECODER_NATIVE_BYTES)?;
        let mut context = safe::DCtx::try_create().ok_or(io::ErrorKind::OutOfMemory)?;
        native(context.init())?;
        native(context.set_parameter(safe::DParameter::WindowLogMax(27)))?;
        Ok(Self {
            context,
            _memory: memory,
            buffers: std::array::from_fn(|_| None),
            buffer_units: 0,
            work: work.cloned(),
            header: [0; 18],
            header_len: 0,
            header_position: 0,
            admitted: false,
        })
    }

    fn header_size(&self) -> usize {
        if self.header_len < 5 {
            return 5;
        }
        let magic = u32::from_le_bytes(self.header[..4].try_into().unwrap());
        if magic & 0xfffffff0 == 0x184d2a50 {
            return 8;
        }
        let descriptor = self.header[4];
        let single = descriptor & 0x20 != 0;
        5 + usize::from(!single)
            + [0, 1, 2, 4][(descriptor & 3) as usize]
            + [usize::from(single), 2, 4, 8][(descriptor >> 6) as usize]
    }
}

impl CompressionDecoder for Decoder {
    fn reset(&mut self) -> io::Result<()> {
        native(self.context.reset(safe::ResetDirective::SessionOnly))?;
        self.header_len = 0;
        self.header_position = 0;
        self.admitted = false;
        Ok(())
    }

    fn retained_bytes(&self) -> usize {
        self.context.sizeof()
    }

    fn step(&mut self, input: &[u8], output: &mut [u8]) -> io::Result<CodecProgress> {
        let mut consumed = 0;
        if !self.admitted {
            loop {
                let need = self.header_size();
                let take = (need - self.header_len).min(input.len() - consumed);
                self.header[self.header_len..self.header_len + take]
                    .copy_from_slice(&input[consumed..consumed + take]);
                self.header_len += take;
                consumed += take;
                if self.header_len < self.header_size() {
                    if consumed < input.len() {
                        continue;
                    }
                    return Ok(CodecProgress {
                        consumed,
                        produced: 0,
                        remaining: 1,
                    });
                }
                let header = parse_frame_header(&self.header[..self.header_len])?;
                let units = ZstdBackend.decoder_buffer_bytes(&header)? / MIB;
                while self.buffer_units < units {
                    self.buffers[self.buffer_units] =
                        Reservation::acquire(self.work.as_ref(), MIB)?;
                    self.buffer_units += 1;
                }
                self.admitted = true;
                break;
            }
        }
        let mut output = OutBuffer::around(output);
        if self.header_position < self.header_len {
            let mut prefix = InBuffer::around(&self.header[self.header_position..self.header_len]);
            let remaining = native(self.context.decompress_stream(&mut output, &mut prefix))?;
            self.header_position += prefix.pos();
            if remaining == 0
                || self.header_position < self.header_len
                || output.pos() == output.capacity()
            {
                return Ok(CodecProgress {
                    consumed,
                    produced: output.pos(),
                    remaining,
                });
            }
        }
        let mut input = InBuffer::around(&input[consumed..]);
        let remaining = native(self.context.decompress_stream(&mut output, &mut input))?;
        Ok(CodecProgress {
            consumed: consumed + input.pos(),
            produced: output.pos(),
            remaining,
        })
    }
}

#[cfg(test)]
mod tests;
