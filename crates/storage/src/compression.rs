// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Streaming Zstd frames through one immutable, host-selectable process backend.

mod backend;
#[cfg(any(test, feature = "test-zstd"))]
mod context;
mod frame;
#[cfg(any(test, feature = "test-zstd"))]
mod native;
mod output;
mod session;

use crate::background::{CheckpointWorkContext, CheckpointWorkError};
pub use backend::{
    decoder_buffer_bytes, decoder_context_bytes, encoder_workspace_bytes,
    install_compression_backend, CodecProgress, CompressionBackend,
    CompressionBackendAlreadyInitialized, CompressionDecoder, CompressionEncoder, DecoderState,
    EncodeOperation, EncoderState,
};
pub use frame::{parse_frame_header, FrameHeader};
use hawdb_core::RuntimeMemoryPermit;
#[cfg(any(test, feature = "test-zstd"))]
pub use native::TestZstdBackend;
pub(crate) use output::AdmittedCompressionOutput;
use session::CompressionContext;
pub use session::DecompressionContext;
use std::io::{self, BufRead, BufReader, Read, Write};

const FRAME_STAGING_BYTES: usize = 32 * 1024;

fn staging(capacity: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
    if bytes.capacity() != capacity {
        return Err(io::Error::other(
            "compression staging exceeds admitted capacity",
        ));
    }
    bytes.resize(capacity, 0);
    Ok(bytes)
}

pub struct Encoder<W: Write> {
    writer: W,
    context: CompressionContext,
    buffer: Vec<u8>,
    failed: bool,
    _buffer_memory: Option<Box<dyn RuntimeMemoryPermit>>,
}

impl<W: Write> Encoder<W> {
    pub fn new(writer: W, level: i32) -> io::Result<Self> {
        Ok(Self {
            writer,
            context: CompressionContext::new(level, None)?,
            buffer: staging(FRAME_STAGING_BYTES)?,
            failed: false,
            _buffer_memory: None,
        })
    }

    pub(crate) fn new_with_work_context(
        writer: W,
        level: i32,
        work: &CheckpointWorkContext,
    ) -> io::Result<Self> {
        Self::with_staging_capacity(writer, level, FRAME_STAGING_BYTES, work)
    }

    pub(crate) fn with_staging_capacity(
        writer: W,
        level: i32,
        capacity: usize,
        work: &CheckpointWorkContext,
    ) -> io::Result<Self> {
        if capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty compression staging buffer",
            ));
        }
        let memory = work.reserve_memory(capacity).map_err(io::Error::other)?;
        let buffer = staging(capacity).map_err(|error| {
            io::Error::other(work.record_failure(CheckpointWorkError::Allocation {
                bytes: capacity as u64,
                reason: error.to_string(),
            }))
        })?;
        Ok(Self {
            writer,
            context: CompressionContext::new(level, Some(work))?,
            buffer,
            failed: false,
            _buffer_memory: memory,
        })
    }

    pub(crate) fn set_pledged_src_size(&mut self, bytes: u64) -> io::Result<()> {
        self.context.set_pledged_src_size(bytes)
    }

    fn ready(&self) -> io::Result<()> {
        if self.failed {
            Err(io::Error::other("compression writer failed"))
        } else {
            Ok(())
        }
    }

    fn complete(&mut self, operation: EncodeOperation) -> io::Result<()> {
        self.ready()?;
        self.failed = true;
        loop {
            let progress = self.context.step(&[], &mut self.buffer, operation)?;
            self.writer.write_all(&self.buffer[..progress.produced])?;
            if progress.remaining == 0 {
                break;
            }
            if progress.produced == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "compression encoder made no progress",
                ));
            }
        }
        self.failed = false;
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<W> {
        self.complete(EncodeOperation::Finish)?;
        self.context.close()?;
        Ok(self.writer)
    }
}

impl<W: Write> Write for Encoder<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.ready()?;
        if bytes.is_empty() {
            return Ok(0);
        }
        self.failed = true;
        loop {
            let progress = self
                .context
                .step(bytes, &mut self.buffer, EncodeOperation::Continue)?;
            self.writer.write_all(&self.buffer[..progress.produced])?;
            if progress.consumed > 0 {
                self.failed = false;
                return Ok(progress.consumed);
            }
            if progress.produced == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "compression encoder made no progress",
                ));
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.complete(EncodeOperation::Flush)?;
        let result = self.writer.flush();
        self.failed = result.is_err();
        result
    }
}

pub struct Decoder<R> {
    reader: R,
    context: DecompressionContext,
    // EOF is valid only after a complete frame, never before the first one.
    boundary: bool,
    failed: bool,
}

impl<R: Read> Decoder<BufReader<R>> {
    pub fn new(reader: R) -> io::Result<Self> {
        // At least one maximum-size Zstd block plus framing, independent of C ABI.
        Self::with_buffer(BufReader::with_capacity(128 * 1024 + 3, reader))
    }
}

impl<R: BufRead> Decoder<R> {
    pub(crate) fn with_buffer_and_work_context(
        reader: R,
        work: &CheckpointWorkContext,
    ) -> io::Result<Self> {
        Ok(Self {
            reader,
            context: DecompressionContext::with_work_context(Some(work))?,
            boundary: false,
            failed: false,
        })
    }

    pub fn with_buffer(reader: R) -> io::Result<Self> {
        Ok(Self {
            reader,
            context: DecompressionContext::new()?,
            boundary: false,
            failed: false,
        })
    }

    fn read_inner(&mut self, output: &mut [u8]) -> io::Result<usize> {
        loop {
            let input = loop {
                match self.reader.fill_buf() {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    result => break result?,
                }
            };
            if input.is_empty() && self.boundary {
                return Ok(0);
            }
            let empty = input.is_empty();
            let progress = self.context.step(input, output)?;
            self.reader.consume(progress.consumed);
            self.boundary = progress.remaining == 0;
            if progress.produced > 0 {
                return Ok(progress.produced);
            }
            if !self.boundary && progress.consumed == 0 {
                return Err(io::Error::new(
                    if empty {
                        io::ErrorKind::UnexpectedEof
                    } else {
                        io::ErrorKind::InvalidData
                    },
                    if empty {
                        "incomplete frame"
                    } else {
                        "compression decoder made no progress"
                    },
                ));
            }
        }
    }
}

impl<R: BufRead> Read for Decoder<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.failed {
            return Err(io::Error::other("compression reader failed"));
        }
        if bytes.is_empty() {
            return Ok(0);
        }
        let result = self.read_inner(bytes);
        self.failed = result.is_err();
        result
    }
}

/// Compress independent Zstd frames with one reusable backend context.
pub struct FrameEncoder {
    context: CompressionContext,
    staging: Vec<u8>,
    failed: bool,
}

impl FrameEncoder {
    pub fn new(level: i32) -> io::Result<Self> {
        Ok(Self {
            context: CompressionContext::new(level, None)?,
            staging: staging(FRAME_STAGING_BYTES)?,
            failed: false,
        })
    }
    pub const fn staging_bytes() -> usize {
        FRAME_STAGING_BYTES
    }

    /// Release backend workspace and surface final cleanup errors before
    /// publishing a sequence of completed frames.
    pub fn close(self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("compression frame encoder failed"));
        }
        self.context.close()
    }

    pub fn frame<W: Write>(
        &mut self,
        content_len: Option<u64>,
        output: W,
    ) -> io::Result<FrameWriter<'_, W>> {
        if self.failed {
            return Err(io::Error::other("compression frame encoder failed"));
        }
        if let Some(bytes) = content_len {
            self.context.set_pledged_src_size(bytes)?;
        }
        // Only successful finish makes this context available for another frame.
        self.failed = true;
        Ok(FrameWriter {
            encoder: self,
            output,
            failed: false,
        })
    }
}

pub struct FrameWriter<'a, W: Write> {
    encoder: &'a mut FrameEncoder,
    output: W,
    failed: bool,
}

impl<W: Write> FrameWriter<'_, W> {
    pub fn finish(mut self) -> io::Result<W> {
        if self.failed {
            return Err(io::Error::other("compression frame writer failed"));
        }
        loop {
            let progress = self.encoder.context.step(
                &[],
                &mut self.encoder.staging,
                EncodeOperation::Finish,
            )?;
            self.output
                .write_all(&self.encoder.staging[..progress.produced])?;
            if progress.remaining == 0 {
                self.encoder.failed = false;
                return Ok(self.output);
            }
            if progress.produced == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "compression encoder made no progress",
                ));
            }
        }
    }
}

impl<W: Write> Write for FrameWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.failed {
            return Err(io::Error::other("compression frame writer failed"));
        }
        if bytes.is_empty() {
            return Ok(0);
        }
        self.failed = true;
        loop {
            let progress = self.encoder.context.step(
                bytes,
                &mut self.encoder.staging,
                EncodeOperation::Continue,
            )?;
            self.output
                .write_all(&self.encoder.staging[..progress.produced])?;
            if progress.consumed > 0 {
                self.failed = false;
                return Ok(progress.consumed);
            }
            if progress.produced == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "compression encoder made no progress",
                ));
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("compression frame writer failed"));
        }
        let result = self.output.flush();
        self.failed = result.is_err();
        result
    }
}

pub fn encode_all(mut source: impl Read, level: i32) -> io::Result<Vec<u8>> {
    let mut encoder = Encoder::new(Vec::new(), level)?;
    io::copy(&mut source, &mut encoder)?;
    encoder.finish()
}

pub(crate) fn encode_slice(source: &[u8], level: i32) -> io::Result<Vec<u8>> {
    let mut encoder = Encoder::new(Vec::new(), level)?;
    encoder.set_pledged_src_size(source.len() as u64)?;
    encoder.write_all(source)?;
    encoder.finish()
}

#[cfg(test)]
mod tests;
