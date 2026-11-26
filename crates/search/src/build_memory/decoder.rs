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

//! Admit provider workspace before decoding each Zstd frame.

use crate::build_control::checkpoint;
use crate::build_memory::BuildMemory;
use crate::{HawDBError, Result};
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::{QueryMemoryAccount, QueryMemoryLease};
use hawdb_storage::compression::DecompressionContext;
use std::io::{self, BufRead, Read};

// zstd 1.5.7/1.6.0 DCtx includes its entropy tables and fixed block scratch. Its
// streaming in/out allocation is admitted separately from each frame header.
#[cfg(test)]
const CONTEXT_BYTES: usize = 256 * 1024;
const DEFAULT_MAX_WINDOW: u64 = (1 << 27) + 1;

pub(crate) struct Decoder<'a, R> {
    input: R,
    context: DecompressionContext,
    context_bytes: usize,
    header: [u8; 18],
    position: usize,
    length: usize,
    boundary: bool,
    task: &'a RuntimeTaskContext,
    // Native state and input drop before their capacity owners.
    _context_memory: QueryMemoryLease,
    buffer_memory: QueryMemoryLease,
}

impl<'a, R: BufRead> Decoder<'a, R> {
    pub(crate) fn new(
        input: R,
        memory: &BuildMemory,
        task: &'a RuntimeTaskContext,
    ) -> Result<Self> {
        Self::with_account(input, &memory.spool, task)
    }

    pub(crate) fn with_account(
        input: R,
        memory: &QueryMemoryAccount,
        task: &'a RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let context_bytes = hawdb_storage::compression::decoder_context_bytes()?;
        let context_memory = memory.reserve(context_bytes)?;
        let buffer_memory = memory.reserve(0)?;
        let context = DecompressionContext::new()?;
        if context.sizeof() > context_bytes {
            return Err(HawDBError::Execution(
                "search hydration decoder context exceeded admission".into(),
            ));
        }
        Ok(Self {
            input,
            context,
            context_bytes,
            header: [0; 18],
            position: 0,
            length: 0,
            boundary: true,
            task,
            _context_memory: context_memory,
            buffer_memory,
        })
    }

    fn begin_frame(&mut self) -> io::Result<bool> {
        checkpoint(self.task).map_err(io::Error::other)?;
        if self.input.fill_buf()?.is_empty() {
            return Ok(false);
        }
        self.input.read_exact(&mut self.header[..5])?;
        let magic = u32::from_le_bytes(self.header[..4].try_into().unwrap());
        self.length = if magic & 0xfffffff0 == 0x184d2a50 {
            8
        } else if magic == 0xfd2fb528 {
            let descriptor = self.header[4];
            let single = descriptor & 0x20 != 0;
            let dictionary = [0, 1, 2, 4][(descriptor & 3) as usize];
            let content = [usize::from(single), 2, 4, 8][(descriptor >> 6) as usize];
            5 + usize::from(!single) + dictionary + content
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid zstd frame magic",
            ));
        };
        self.input.read_exact(&mut self.header[5..self.length])?;
        let required = frame_buffer_bytes(&self.header[..self.length]).map_err(io::Error::other)?;
        // The provider contract covers retained buffers and replacement overlap
        // within the maximum bound of all frames seen by this context.
        self.buffer_memory
            .grow(required.saturating_sub(self.buffer_memory.bytes()))
            .map_err(io::Error::other)?;
        self.context.reset()?;
        self.position = 0;
        self.boundary = false;
        Ok(true)
    }
}

impl<R: BufRead> Read for Decoder<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            checkpoint(self.task).map_err(io::Error::other)?;
            if self.boundary && !self.begin_frame()? {
                return Ok(0);
            }
            let header = self.position < self.length;
            let input = if header {
                &self.header[self.position..self.length]
            } else {
                self.input.fill_buf()?
            };
            let empty = input.is_empty();
            let limit = output.len().min(8192);
            let progress = self.context.step(input, &mut output[..limit])?;
            let remaining = progress.remaining;
            let consumed = progress.consumed;
            let written = progress.produced;
            if header {
                self.position += consumed;
            } else {
                self.input.consume(consumed);
            }
            let admitted = self
                .context_bytes
                .checked_add(self.buffer_memory.bytes())
                .ok_or_else(|| io::Error::other("decoder admission overflow"))?;
            if self.context.sizeof() > admitted {
                return Err(io::Error::other(
                    "search hydration native buffers exceeded admission",
                ));
            }
            self.boundary = remaining == 0;
            if written > 0 {
                return Ok(written);
            }
            if !self.boundary && consumed == 0 {
                return Err(io::Error::new(
                    if empty {
                        io::ErrorKind::UnexpectedEof
                    } else {
                        io::ErrorKind::InvalidData
                    },
                    "search hydration zstd stream made no progress",
                ));
            }
        }
    }
}

/// Decodes one complete content-block frame at a time into a caller buffer,
/// reusing one admitted native context across blocks (#1917).
///
/// The context and its pinned in/out buffers are admitted exactly as the
/// streaming [`Decoder`] admits them. Output is never larger than the caller's
/// verified block length, and every input byte must belong to the frame.
pub(crate) struct BlockDecoder<'a> {
    context: DecompressionContext,
    context_bytes: usize,
    task: Option<&'a RuntimeTaskContext>,
    buffer_bytes: usize,
    // Native state drops before its capacity owners.
    _context_memory: Option<QueryMemoryLease>,
    buffer_memory: Option<QueryMemoryLease>,
}

impl<'a> BlockDecoder<'a> {
    pub(crate) fn new(
        memory: Option<&QueryMemoryAccount>,
        task: Option<&'a RuntimeTaskContext>,
    ) -> Result<Self> {
        if let Some(task) = task {
            checkpoint(task)?;
        }
        let context_bytes = hawdb_storage::compression::decoder_context_bytes()?;
        let context_memory = memory
            .map(|memory| memory.reserve(context_bytes))
            .transpose()?;
        let buffer_memory = memory.map(|memory| memory.reserve(0)).transpose()?;
        let context = DecompressionContext::new()?;
        if context.sizeof() > context_bytes {
            return Err(HawDBError::Execution(
                "search content block decoder context exceeded admission".into(),
            ));
        }
        Ok(Self {
            context,
            context_bytes,
            task,
            buffer_bytes: 0,
            _context_memory: context_memory,
            buffer_memory,
        })
    }

    /// Decodes `input`, which must be exactly one zstd frame, into `output`,
    /// which it must fill exactly.
    pub(crate) fn decode(&mut self, input: &[u8], output: &mut [u8]) -> Result<()> {
        let invalid = |reason: &str| HawDBError::Storage(format!("search content block {reason}"));
        if let Some(task) = self.task {
            checkpoint(task)?;
        }
        if input.len() < 5 || u32::from_le_bytes(input[..4].try_into().unwrap()) != 0xfd2fb528 {
            return Err(invalid("is not one zstd frame"));
        }
        let descriptor = input[4];
        let single = descriptor & 0x20 != 0;
        let dictionary = [0, 1, 2, 4][(descriptor & 3) as usize];
        let content = [usize::from(single), 2, 4, 8][(descriptor >> 6) as usize];
        let header = 5 + usize::from(!single) + dictionary + content;
        if input.len() < header {
            return Err(invalid("frame header is truncated"));
        }
        let required = frame_buffer_bytes(&input[..header])?;
        if required > self.buffer_bytes {
            if let Some(memory) = &mut self.buffer_memory {
                memory.grow(required - self.buffer_bytes)?;
            }
            self.buffer_bytes = required;
        }
        self.context.reset()?;
        let mut consumed = 0;
        let mut produced = 0;
        loop {
            if let Some(task) = self.task {
                checkpoint(task)?;
            }
            let progress = self
                .context
                .step(&input[consumed..], &mut output[produced..])
                .map_err(|error| invalid(&format!("decode failed: {error}")))?;
            let admitted = self
                .context_bytes
                .checked_add(self.buffer_bytes)
                .ok_or_else(|| HawDBError::Execution("decoder admission overflow".into()))?;
            if self.context.sizeof() > admitted {
                return Err(HawDBError::Execution(
                    "search content block native buffers exceeded admission".into(),
                ));
            }
            consumed += progress.consumed;
            produced += progress.produced;
            if progress.remaining == 0 {
                break;
            }
            if progress.consumed == 0 && progress.produced == 0 {
                return Err(invalid(if produced == output.len() {
                    "inflates past its indexed length"
                } else {
                    "frame is truncated"
                }));
            }
        }
        if consumed != input.len() {
            return Err(invalid("has bytes after its frame"));
        }
        if produced != output.len() {
            return Err(invalid("inflates short of its indexed length"));
        }
        Ok(())
    }
}

fn frame_buffer_bytes(header: &[u8]) -> Result<usize> {
    let header = hawdb_storage::compression::parse_frame_header(header)?;
    if header.window_bytes > DEFAULT_MAX_WINDOW {
        return Err(HawDBError::Storage(
            "search hydration zstd window exceeds the native limit".into(),
        ));
    }
    Ok(hawdb_storage::compression::decoder_buffer_bytes(&header)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor};

    #[test]
    fn admitted_decoder_preserves_frames_and_releases_native_owners() {
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let mut bytes = zstd::stream::encode_all(&b"first"[..], 3).unwrap();
        bytes.extend_from_slice(&0x184d2a5fu32.to_le_bytes());
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(b"abc");
        bytes.extend(zstd::stream::encode_all(&b"second"[..], 3).unwrap());
        let oracle = zstd::stream::decode_all(bytes.as_slice()).unwrap();
        for capacity in [1, 5, 8192] {
            let input = BufReader::with_capacity(capacity, Cursor::new(&bytes));
            let mut decoder = Decoder::new(input, &memory, &task).unwrap();
            let mut output = Vec::new();
            decoder.read_to_end(&mut output).unwrap();
            assert_eq!(output, oracle);
            assert!(decoder.context.sizeof() <= CONTEXT_BYTES + decoder.buffer_memory.bytes());
            drop(decoder);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
    }

    #[test]
    fn decoder_admits_actual_windows_and_rejects_one_short_before_native_growth() {
        use hawdb_core::RuntimeMemoryReservation;
        use std::io::Write;
        let original = vec![b'x'; 1023];
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        encoder.write_all(&original).unwrap();
        let compressed = encoder.finish().unwrap();
        assert_eq!(compressed[4] & 0x20, 0);
        for window in [0, 7, 56, 63, 88, 95] {
            let mut bytes = compressed.clone();
            bytes[5] = window;
            // A noncanonical four-byte zero dictionary ID remains valid.
            bytes[4] |= 3;
            bytes.splice(6..6, [0; 4]);
            assert_eq!(
                zstd::stream::decode_all(bytes.as_slice()).unwrap(),
                original
            );
            let required = frame_buffer_bytes(&bytes[..10]).unwrap();
            for short in [false, true] {
                let budget = CONTEXT_BYTES + required - usize::from(short);
                let task = RuntimeTaskContext::default()
                    .with_memory_reservation(RuntimeMemoryReservation::new(budget as u64, 0));
                let memory = BuildMemory::new(&task).unwrap();
                let mut decoder = Decoder::new(Cursor::new(&bytes), &memory, &task).unwrap();
                let mut output = Vec::new();
                let result = decoder.read_to_end(&mut output);
                if short {
                    assert!(result.unwrap_err().to_string().contains("memory"));
                    assert!(output.is_empty());
                    assert!(decoder.context.sizeof() <= CONTEXT_BYTES);
                } else {
                    result.unwrap();
                    assert_eq!(output, original);
                }
                drop(decoder);
                assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            }
        }
    }

    #[test]
    fn decoder_preserves_content_size_frames_and_rejects_reserved_lengths() {
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        for size in [0, 1, 255, 256, 65535, 65536] {
            let input = vec![b'x'; size];
            let bytes = zstd::bulk::compress(&input, 3).unwrap();
            let mut decoder = Decoder::new(Cursor::new(bytes), &memory, &task).unwrap();
            let mut output = Vec::new();
            decoder.read_to_end(&mut output).unwrap();
            assert_eq!(output, input);
            drop(decoder);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
        for size in [u64::MAX, u64::MAX - 1, 1 << 32] {
            let mut header = vec![0x28, 0xb5, 0x2f, 0xfd, 0xe0];
            header.extend_from_slice(&size.to_le_bytes());
            assert!(frame_buffer_bytes(&header).is_err());
            let mut decoder = Decoder::new(Cursor::new(header), &memory, &task).unwrap();
            assert!(decoder.read(&mut [0; 1]).is_err());
            drop(decoder);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
    }
}
