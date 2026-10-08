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

//! Bounded compression directly into a private artifact tail.
//!
//! The envelope length is known only after compression. Move the new tail
//! backwards in bounded blocks to insert it; no other segment is touched.
//! An error poisons the generation writer, so an incomplete tail is never
//! published or offered as a retryable record.

use super::*;
use crate::document_encoding::RecordSource;
use hawdb_storage::file_io::File;
use std::io::{Read, Seek, SeekFrom};

pub(in super::super) fn append_segment<T: RecordSource>(
    output: &mut File,
    encoding: &SegmentEncoding<'_, T>,
    max_uncompressed_bytes: u64,
    max_compressed_bytes: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<(u64, u64)> {
    checkpoint(task)?;
    if encoding.len() as u64 > max_uncompressed_bytes {
        return Err(HawDBError::Storage(
            "search document segment exceeds encoded admission".into(),
        ));
    }
    crate::build_memory::compression::require_qualified_zstd("workspace admission")?;
    let workspace = memory.retained.reserve(COMPRESSION_WORKSPACE_BYTES)?;
    let _scratch = memory
        .spool
        .reserve(2 * HEX_BUFFER_BYTES + 3 * HEADER_CAPACITY)?;
    let start = output.stream_position()?;
    let mut compressed = LimitedWriter {
        output,
        bytes: 0,
        limit: max_compressed_bytes,
        digest: Crc32cHasher::new(),
        task,
    };
    #[cfg(test)]
    evidence::started();
    let mut encoder =
        hawdb_storage::compression::Encoder::new(&mut compressed, SEARCH_COMPRESSION_LEVEL)?;
    let mut digest = Crc32cHasher::new();
    encoding.write_to(&mut CheckedWriter::new(
        &mut DigestWriter {
            writer: &mut encoder,
            digest: &mut digest,
        },
        Some(task),
    ))?;
    encoder.finish()?;
    drop(workspace);
    checkpoint(task)?;
    let compressed_bytes = compressed.bytes;
    let header = search_snapshot_compression_header(
        digest.finish(),
        compressed.digest.finish(),
        encoding.len(),
        usize::try_from(compressed_bytes)
            .map_err(|_| HawDBError::Storage("compressed segment exceeds usize".into()))?,
    );
    if header.capacity() > HEADER_CAPACITY {
        return Err(HawDBError::Execution(
            "search compression header exceeded admission".into(),
        ));
    }
    let length = compressed_bytes
        .checked_add(header.len() as u64)
        .filter(|length| *length <= max_compressed_bytes)
        .ok_or_else(|| {
            HawDBError::Storage(
                "search segment requires compressed bytes exceeding its admission".into(),
            )
        })?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| HawDBError::Storage("search artifact offset overflow".into()))?;
    let output = compressed.output;
    let mut buffer = [0; HEX_BUFFER_BYTES];
    let mut remaining = compressed_bytes;
    while remaining != 0 {
        checkpoint(task)?;
        let count = remaining.min(buffer.len() as u64) as usize;
        remaining -= count as u64;
        output.seek(SeekFrom::Start(start + remaining))?;
        output.read_exact(&mut buffer[..count])?;
        output.seek(SeekFrom::Start(start + remaining + header.len() as u64))?;
        output.write_all(&buffer[..count])?;
    }
    output.seek(SeekFrom::Start(start))?;
    output.write_all(header.as_bytes())?;
    let mut checksum = Crc32cHasher::new();
    checksum.update(header.as_bytes());
    let mut remaining = compressed_bytes;
    while remaining != 0 {
        checkpoint(task)?;
        let count = remaining.min(buffer.len() as u64) as usize;
        output.read_exact(&mut buffer[..count])?;
        checksum.update(&buffer[..count]);
        remaining -= count as u64;
    }
    debug_assert_eq!(output.stream_position()?, end);
    checkpoint(task)?;
    Ok((length, checksum.finish()))
}

struct LimitedWriter<'a> {
    output: &'a mut File,
    bytes: u64,
    limit: u64,
    digest: Crc32cHasher,
    task: &'a RuntimeTaskContext,
}

impl Write for LimitedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        checkpoint(self.task).map_err(io::Error::other)?;
        #[cfg(test)]
        evidence::output();
        if self
            .bytes
            .checked_add(bytes.len() as u64)
            .is_none_or(|bytes| bytes > self.limit)
        {
            return Err(io::Error::other(
                "search segment requires compressed bytes exceeding its admission",
            ));
        }
        let count = self.output.write(bytes)?;
        self.bytes += count as u64;
        self.digest.update(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}
