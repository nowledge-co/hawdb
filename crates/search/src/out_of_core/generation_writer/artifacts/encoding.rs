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

use crate::build_control::{checkpoint, CheckedWriter};
use crate::build_memory::{reserve_capacity, BuildMemory};
use crate::document_encoding::{SegmentEncoding, HEX_BUFFER_BYTES};
use crate::{search_snapshot_compression_header, HawDBError, Result, SEARCH_COMPRESSION_LEVEL};
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;
use hawdb_integrity::Crc32cHasher;
use std::io::{self, Write};

// Pinned zstd 1.5.7, level 3, one worker, no dictionary/LDM/sequence producer.
// Two conservative native workspace allowances plus zstd 0.13.3's 32 KiB
// Rust writer buffer. Requalify this envelope when upgrading either dependency.
pub(super) const COMPRESSION_WORKSPACE_BYTES: usize = 8 * 1024 * 1024 + 32 * 1024;
const HEADER_CAPACITY: usize = 512;
const _: () = assert!(SEARCH_COMPRESSION_LEVEL == 3);

#[cfg(test)]
pub(super) fn encode_segment_payload<T: std::borrow::Borrow<crate::SearchDocument>>(
    encoding: &SegmentEncoding<'_, T>,
    segment_id: u64,
    name: &str,
    max_uncompressed_bytes: u64,
    max_compressed_bytes: u64,
) -> Result<Vec<u8>> {
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task)?;
    encode_segment_payload_with_context(
        encoding,
        segment_id,
        name,
        max_uncompressed_bytes,
        max_compressed_bytes,
        &memory,
        &task,
    )
    .map(|output| output.bytes)
}

pub(super) fn encode_segment_payload_with_context<T: std::borrow::Borrow<crate::SearchDocument>>(
    encoding: &SegmentEncoding<'_, T>,
    segment_id: u64,
    name: &str,
    max_uncompressed_bytes: u64,
    max_compressed_bytes: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<CompressedBuffer> {
    checkpoint(task)?;
    if encoding.len() as u64 > max_uncompressed_bytes {
        return Err(HawDBError::Storage(format!(
            "search generation {name} segment {segment_id} requires {} bytes, exceeding {max_uncompressed_bytes}",
            encoding.len(),
        )));
    }
    crate::build_memory::compression::require_qualified_zstd("workspace admission")?;
    let workspace = memory.retained.reserve(COMPRESSION_WORKSPACE_BYTES)?;
    let _scratch = memory
        .spool
        .reserve(HEX_BUFFER_BYTES + 3 * HEADER_CAPACITY)?;
    let buffer = CompressedBuffer::new(max_compressed_bytes, memory, task)?;
    let result = (|| {
        #[cfg(test)]
        evidence::started();
        let mut encoder = zstd::stream::write::Encoder::new(buffer, SEARCH_COMPRESSION_LEVEL)?;
        let mut digest = Crc32cHasher::new();
        encoding.write_to(&mut CheckedWriter::new(
            &mut DigestWriter {
                writer: &mut encoder,
                digest: &mut digest,
            },
            Some(task),
        ))?;
        let mut compressed = encoder.finish()?;
        drop(workspace);
        checkpoint(task).map_err(io::Error::other)?;
        let header = search_snapshot_compression_header(
            digest.finish(),
            compressed.digest.finish(),
            encoding.len(),
            compressed.bytes.len(),
        );
        if header.capacity() > HEADER_CAPACITY {
            return Err(io::Error::other(
                "search compression header exceeded admission",
            ));
        }
        // Grow and move in place: retaining a second compressed payload would
        // defeat admission even after eliminating the uncompressed text copy.
        compressed.reserve(header.len())?;
        let payload_len = compressed.bytes.len();
        compressed.bytes.resize(payload_len + header.len(), 0);
        compressed.bytes.copy_within(..payload_len, header.len());
        compressed.bytes[..header.len()].copy_from_slice(header.as_bytes());
        checkpoint(task).map_err(io::Error::other)?;
        Ok(compressed)
    })();
    result.map_err(|error: io::Error| {
        HawDBError::Storage(format!(
            "search generation {name} segment {segment_id} encoding failed: {error}"
        ))
    })
}

#[derive(Debug)]
pub(super) struct CompressedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    digest: Crc32cHasher,
    task: RuntimeTaskContext,
    memory: QueryMemoryLease,
}

impl CompressedBuffer {
    fn new(limit: u64, memory: &BuildMemory, task: &RuntimeTaskContext) -> Result<Self> {
        Ok(Self {
            bytes: Vec::new(),
            limit: usize::try_from(limit).unwrap_or(usize::MAX),
            digest: Crc32cHasher::new(),
            task: task.clone(),
            memory: memory.retained.reserve(0)?,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.bytes.len()
    }

    fn reserve(&mut self, additional: usize) -> io::Result<()> {
        checkpoint(&self.task).map_err(io::Error::other)?;
        let required = self.bytes.len().checked_add(additional).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "compressed size overflow")
        })?;
        if required > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "requires {required} compressed bytes, exceeding {}",
                    self.limit
                ),
            ));
        }
        if required <= self.bytes.capacity() {
            return Ok(());
        }
        // Bound geometric growth by the admitted limit. Exact growth on every
        // zstd block can repeatedly copy an otherwise admitted large payload.
        let capacity = required.max(self.bytes.capacity().saturating_mul(2).min(self.limit));
        reserve_capacity(&mut self.bytes, capacity, &mut self.memory).map_err(io::Error::other)
    }
}

impl AsRef<[u8]> for CompressedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Write for CompressedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        #[cfg(test)]
        evidence::output();
        self.reserve(bytes.len())?;
        self.bytes.extend_from_slice(bytes);
        self.digest.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        checkpoint(&self.task).map_err(io::Error::other)
    }
}

struct DigestWriter<'a, W> {
    writer: &'a mut W,
    digest: &'a mut Crc32cHasher,
}

impl<W: Write> Write for DigestWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.writer.write(bytes)?;
        self.digest.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod evidence {
    use hawdb_core::RuntimeCancellationToken;
    use std::cell::{Cell, RefCell};
    thread_local! {
        static STARTS: Cell<usize> = const { Cell::new(0) };
        static CANCEL: RefCell<Option<RuntimeCancellationToken>> = const { RefCell::new(None) };
    }
    pub(super) fn started() {
        STARTS.set(STARTS.get() + 1);
    }
    pub(super) fn output() {
        if let Some(token) = CANCEL.with_borrow_mut(Option::take) {
            token.cancel();
        }
    }
    pub(crate) fn take_starts() -> usize {
        STARTS.replace(0)
    }
    pub(crate) fn cancel_on_output(token: RuntimeCancellationToken) {
        CANCEL.with_borrow_mut(|value| *value = Some(token));
    }
}
