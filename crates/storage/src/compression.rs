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

//! Internal Zstd streams whose native contexts use the host's Rust allocator.
//!
//! Keep admission at the caller: selecting an allocation backend does not
//! reserve query memory or charge a second time for an admitted workspace.

mod context;
mod output;
use crate::background::{CheckpointWorkContext, CheckpointWorkError};
use context::CompressionContext;
pub use context::DecompressionContext;
use hawdb_core::RuntimeMemoryPermit;
pub(crate) use output::AdmittedCompressionOutput;
use std::io::{self, BufRead, BufReader, Read, Write};
use zstd::stream::raw::{InBuffer, Operation, OutBuffer, WriteBuf};
use zstd::stream::zio;

pub struct Encoder<W: Write> {
    writer: zio::Writer<W, CompressionContext>,
    _buffer_memory: Option<Box<dyn RuntimeMemoryPermit>>,
}

impl<W: Write> Encoder<W> {
    pub fn new(writer: W, level: i32) -> io::Result<Self> {
        Ok(Self {
            writer: zio::Writer::new(writer, CompressionContext::new(level)?),
            _buffer_memory: None,
        })
    }

    pub(crate) fn new_with_work_context(
        writer: W,
        level: i32,
        work: &CheckpointWorkContext,
    ) -> io::Result<Self> {
        Self::with_staging_capacity(writer, level, 32 * 1024, work)
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
                "empty Zstd staging buffer",
            ));
        }
        let memory = work.reserve_memory(capacity).map_err(io::Error::other)?;
        let mut buffer = Vec::new();
        buffer.try_reserve_exact(capacity).map_err(|error| {
            io::Error::other(work.record_failure(CheckpointWorkError::Allocation {
                bytes: capacity as u64,
                reason: error.to_string(),
            }))
        })?;
        if buffer.capacity() != capacity {
            return Err(io::Error::other(work.record_failure(
                CheckpointWorkError::Allocation {
                    bytes: capacity as u64,
                    reason: "Zstd output buffer exceeds admitted capacity".into(),
                },
            )));
        }
        Ok(Self {
            writer: zio::Writer::with_output_buffer(
                buffer,
                writer,
                CompressionContext::new_with_work_context(level, work)?,
            ),
            _buffer_memory: memory,
        })
    }

    pub(crate) fn set_pledged_src_size(&mut self, bytes: u64) -> io::Result<()> {
        self.writer.operation_mut().set_pledged_src_size(bytes)
    }

    pub fn finish(mut self) -> io::Result<W> {
        self.writer.finish()?;
        let (writer, context) = self.writer.into_inner();
        // Release native workspace before the caller releases its admission.
        context.close()?;
        Ok(writer)
    }
}

impl<W: Write> Write for Encoder<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.writer.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

pub struct Decoder<R> {
    reader: zio::Reader<R, DecompressionContext>,
}

impl<R: Read> Decoder<BufReader<R>> {
    pub fn new(reader: R) -> io::Result<Self> {
        Self::with_buffer(BufReader::with_capacity(
            zstd::zstd_safe::DCtx::in_size(),
            reader,
        ))
    }
}

impl<R: BufRead> Decoder<R> {
    pub(crate) fn with_buffer_and_work_context(
        reader: R,
        work: &CheckpointWorkContext,
    ) -> io::Result<Self> {
        Ok(Self {
            reader: zio::Reader::new(reader, DecompressionContext::new_with_work_context(work)?),
        })
    }

    pub fn with_buffer(reader: R) -> io::Result<Self> {
        Ok(Self {
            reader: zio::Reader::new(reader, DecompressionContext::new()?),
        })
    }
}

impl<R: BufRead> Read for Decoder<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.reader.read(bytes)
    }
}

impl Operation for DecompressionContext {
    fn run<C: WriteBuf + ?Sized>(
        &mut self,
        input: &mut InBuffer<'_>,
        output: &mut OutBuffer<'_, C>,
    ) -> io::Result<usize> {
        self.decompress_stream(output, input)
    }

    fn flush<C: WriteBuf + ?Sized>(&mut self, output: &mut OutBuffer<'_, C>) -> io::Result<usize> {
        self.run(&mut InBuffer::around(&[]), output)?;
        Ok(usize::from(output.pos() == output.capacity()))
    }

    fn reinit(&mut self) -> io::Result<()> {
        self.reset()
    }

    fn finish<C: WriteBuf + ?Sized>(
        &mut self,
        _output: &mut OutBuffer<'_, C>,
        finished_frame: bool,
    ) -> io::Result<usize> {
        if finished_frame {
            Ok(0)
        } else {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete frame",
            ))
        }
    }
}

/// Compresses a sequence of independent Zstd frames with one native context.
///
/// Each frame declares its exact content size, so a reader can decode and
/// verify it alone. Search content blocks use one frame per block (#1917);
/// reusing the context avoids a native workspace allocation per frame.
pub struct FrameEncoder {
    context: CompressionContext,
    staging: Vec<u8>,
}

/// Staging output for one compression step; flushed to the sink after each step.
const FRAME_STAGING_BYTES: usize = 32 * 1024;

impl FrameEncoder {
    pub fn new(level: i32) -> io::Result<Self> {
        let mut staging = Vec::new();
        staging
            .try_reserve_exact(FRAME_STAGING_BYTES)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        Ok(Self {
            context: CompressionContext::new(level)?,
            staging,
        })
    }

    /// Bytes retained by the Rust staging buffer, excluding native workspace.
    pub const fn staging_bytes() -> usize {
        FRAME_STAGING_BYTES
    }

    /// Starts one frame of exactly `content_len` bytes, or of unknown size
    /// when `None`. Every byte written goes to `output` compressed; call
    /// [`FrameWriter::finish`] to end the frame. A failed frame leaves the
    /// encoder unusable.
    pub fn frame<W: Write>(
        &mut self,
        content_len: Option<u64>,
        output: W,
    ) -> io::Result<FrameWriter<'_, W>> {
        if let Some(bytes) = content_len {
            self.context.set_pledged_src_size(bytes)?;
        }
        Ok(FrameWriter {
            encoder: self,
            output,
        })
    }
}

pub struct FrameWriter<'a, W: Write> {
    encoder: &'a mut FrameEncoder,
    output: W,
}

impl<W: Write> FrameWriter<'_, W> {
    fn drain(&mut self) -> io::Result<()> {
        if !self.encoder.staging.is_empty() {
            self.output.write_all(&self.encoder.staging)?;
            self.encoder.staging.clear();
        }
        Ok(())
    }

    /// Ends the frame and returns the sink.
    pub fn finish(mut self) -> io::Result<W> {
        loop {
            let FrameEncoder { context, staging } = &mut *self.encoder;
            let mut output = OutBuffer::around(staging);
            let remaining = context.step(
                &mut InBuffer::around(&[]),
                &mut output,
                zstd::zstd_safe::zstd_sys::ZSTD_EndDirective::ZSTD_e_end,
            )?;
            self.drain()?;
            if remaining == 0 {
                return Ok(self.output);
            }
        }
    }
}

impl<W: Write> Write for FrameWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut input = InBuffer::around(bytes);
        while input.pos() < bytes.len() {
            let FrameEncoder { context, staging } = &mut *self.encoder;
            let mut output = OutBuffer::around(staging);
            context.step(
                &mut input,
                &mut output,
                zstd::zstd_safe::zstd_sys::ZSTD_EndDirective::ZSTD_e_continue,
            )?;
            self.drain()?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

pub fn encode_all(mut source: impl Read, level: i32) -> io::Result<Vec<u8>> {
    let mut encoder = Encoder::new(Vec::new(), level)?;
    io::copy(&mut source, &mut encoder)?;
    encoder.finish()
}

/// Compress a known complete slice without advertising an unnecessarily large
/// unknown-size frame window to streaming readers.
pub(crate) fn encode_slice(source: &[u8], level: i32) -> io::Result<Vec<u8>> {
    let mut encoder = Encoder::new(Vec::new(), level)?;
    encoder.set_pledged_src_size(source.len() as u64)?;
    encoder.write_all(source)?;
    encoder.finish()
}

#[cfg(test)]
mod tests;
