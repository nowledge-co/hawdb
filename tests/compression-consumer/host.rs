// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Independent host adapter. No HawDB built-in codec types or native ABI shims.
//! This fixture deliberately uses coarse workspace admission, not a production
//! allocator recommendation. The host owns the foreign library's allocator.

use hawdb::compression::*;
use hawdb::RuntimeMemoryPermit;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use zstd::zstd_safe::{self as safe, InBuffer, OutBuffer};

const ENCODER_BYTES: usize = 8 * 1024 * 1024;
const DECODER_BYTES: usize = 129 * 1024 * 1024;

#[derive(Debug, Default)]
pub struct HostBackend {
    pub encoders: AtomicUsize,
    pub decoders: AtomicUsize,
    pub fault: Arc<AtomicUsize>,
}

fn native<T>(result: Result<T, usize>) -> io::Result<T> {
    result.map_err(|error| io::Error::other(safe::get_error_name(error)))
}

fn reserve(
    work: Option<&CheckpointWorkContext>,
    bytes: usize,
) -> io::Result<Option<Box<dyn RuntimeMemoryPermit>>> {
    work.map(|work| work.reserve_memory(bytes))
        .transpose()
        .map_err(io::Error::other)
        .map(Option::flatten)
}

impl CompressionBackend for HostBackend {
    fn encoder(
        &self,
        level: i32,
        work: Option<&CheckpointWorkContext>,
    ) -> io::Result<EncoderState> {
        let memory = reserve(work, ENCODER_BYTES)?;
        let mut context = safe::CCtx::try_create().ok_or(io::ErrorKind::OutOfMemory)?;
        native(context.set_parameter(safe::CParameter::CompressionLevel(level)))?;
        self.encoders.fetch_add(1, Ordering::SeqCst);
        EncoderState::new(
            HostEncoder {
                context,
                fault: Arc::clone(&self.fault),
                _memory: memory,
            },
            work,
        )
    }

    fn decoder(&self, work: Option<&CheckpointWorkContext>) -> io::Result<DecoderState> {
        // Bound the complete permitted native window before any native growth.
        let memory = reserve(work, DECODER_BYTES)?;
        let mut context = safe::DCtx::try_create().ok_or(io::ErrorKind::OutOfMemory)?;
        native(context.init())?;
        self.decoders.fetch_add(1, Ordering::SeqCst);
        DecoderState::new(
            HostDecoder {
                context,
                fault: Arc::clone(&self.fault),
                _memory: memory,
            },
            work,
        )
    }

    fn encoder_workspace_bytes(&self, _level: i32) -> io::Result<usize> {
        Ok(ENCODER_BYTES)
    }
    fn decoder_context_bytes(&self) -> io::Result<usize> {
        Ok(256 * 1024)
    }
    fn decoder_buffer_bytes(&self, header: &FrameHeader) -> io::Result<usize> {
        if header.skippable {
            return Ok(0);
        }
        let output = header
            .content_bytes
            .unwrap_or(u64::MAX)
            .min(header.window_bytes.max(1024) + 2 * header.block_bytes + 64);
        usize::try_from(output + header.block_bytes.max(4)).map_err(io::Error::other)
    }
    fn compress_bound(&self, input_len: usize) -> io::Result<usize> {
        Ok(safe::compress_bound(input_len))
    }
}

struct HostEncoder {
    context: safe::CCtx<'static>,
    fault: Arc<AtomicUsize>,
    // Release the native allocation before refunding this permit.
    _memory: Option<Box<dyn RuntimeMemoryPermit>>,
}

impl CompressionEncoder for HostEncoder {
    fn set_pledged_src_size(&mut self, bytes: u64) -> io::Result<()> {
        native(self.context.set_pledged_src_size(Some(bytes))).map(|_| ())
    }
    fn step(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        operation: EncodeOperation,
    ) -> io::Result<CodecProgress> {
        match self.fault.load(Ordering::SeqCst) {
            1 => {
                return Ok(CodecProgress {
                    consumed: input.len() + 1,
                    produced: 0,
                    remaining: 1,
                })
            }
            5 => {
                return Ok(CodecProgress {
                    consumed: 0,
                    produced: output.len() + 1,
                    remaining: 1,
                })
            }
            2 => {
                return Ok(CodecProgress {
                    consumed: 0,
                    produced: 0,
                    remaining: 1,
                })
            }
            3 => return Err(io::Error::other("host encoder refusal")),
            _ => {}
        }
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
        assert!(self.context.sizeof() <= ENCODER_BYTES);
        Ok(CodecProgress {
            consumed: input.pos(),
            produced: output.pos(),
            remaining,
        })
    }
    fn close(self: Box<Self>) -> io::Result<()> {
        let refused = self.fault.load(Ordering::SeqCst) == 4;
        drop(self);
        if refused {
            Err(io::Error::other("host encoder cleanup refusal"))
        } else {
            Ok(())
        }
    }
}

struct HostDecoder {
    context: safe::DCtx<'static>,
    fault: Arc<AtomicUsize>,
    _memory: Option<Box<dyn RuntimeMemoryPermit>>,
}

impl CompressionDecoder for HostDecoder {
    fn reset(&mut self) -> io::Result<()> {
        native(self.context.reset(safe::ResetDirective::SessionOnly)).map(|_| ())
    }
    fn retained_bytes(&self) -> usize {
        self.context.sizeof()
    }
    fn step(&mut self, input: &[u8], output: &mut [u8]) -> io::Result<CodecProgress> {
        match self.fault.load(Ordering::SeqCst) {
            1 => {
                return Ok(CodecProgress {
                    consumed: input.len() + 1,
                    produced: 0,
                    remaining: 1,
                })
            }
            5 => {
                return Ok(CodecProgress {
                    consumed: 0,
                    produced: output.len() + 1,
                    remaining: 1,
                })
            }
            2 => {
                return Ok(CodecProgress {
                    consumed: 0,
                    produced: 0,
                    remaining: 1,
                })
            }
            3 => return Err(io::Error::other("host decoder refusal")),
            _ => {}
        }
        let mut input = InBuffer::around(input);
        let mut output = OutBuffer::around(output);
        let remaining = native(self.context.decompress_stream(&mut output, &mut input))?;
        assert!(self.context.sizeof() <= DECODER_BYTES);
        Ok(CodecProgress {
            consumed: input.pos(),
            produced: output.pos(),
            remaining,
        })
    }
}
