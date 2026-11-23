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
//! Content segments are framed as independently compressed blocks (#1917).
//! The header and index slots are sized before compression and filled last;
//! no other segment is touched. An error poisons the generation writer, so an
//! incomplete tail is never published or offered as a retryable record.

use super::*;
use crate::document_encoding::RecordSource;
use hawdb_storage::file_io::File;
use std::io::{Seek, SeekFrom};

/// Appends one `blocks1` content segment (#1917) and returns its length and
/// the segment digest CRC32C(header || index) recorded by the descriptor.
///
/// Block boundaries follow from the records' admitted encoded lengths, so the
/// index size is known before any block is written: the header and index
/// slots are skipped, blocks are compressed straight into the tail, and the
/// slots are filled last. Memory is the compression workspace, one staging
/// buffer and the index; no block is buffered whole.
pub(in super::super) fn append_block_segment<T: RecordSource>(
    output: &mut File,
    encoding: &SegmentEncoding<'_, T>,
    max_uncompressed_bytes: u64,
    max_compressed_bytes: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<(u64, u64)> {
    use crate::out_of_core::content_blocks::{
        encode_entry, entry_len, records, Header, WrittenBlock, BLOCK_TARGET_BYTES, HEADER_BYTES,
    };
    let invalid = |reason: &str| HawDBError::Storage(format!("search content blocks {reason}"));
    checkpoint(task)?;
    if encoding.len() as u64 > max_uncompressed_bytes {
        return Err(HawDBError::Storage(
            "search document segment exceeds encoded admission".into(),
        ));
    }
    let documents = encoding
        .documents()
        .ok_or_else(|| invalid("frame only document segments"))?;
    if documents.is_empty() {
        return Err(invalid("cannot frame an empty segment"));
    }
    // Plan blocks from admitted lengths: (first record, end record, bytes).
    let mut plan_memory = memory.spool.reserve(0)?;
    let mut plan: Vec<(usize, usize, u64)> = Vec::new();
    let mut index_len = 0usize;
    let mut start_record = 0;
    let mut block_bytes = 0u64;
    let mut total_bytes = 0u64;
    for (position, document) in documents.iter().enumerate() {
        checkpoint(task)?;
        let content = records::content_len(document, Some(task))?;
        let bytes = records::record_len(document.header(), content)?;
        total_bytes = total_bytes
            .checked_add(bytes)
            .filter(|total| *total <= max_uncompressed_bytes)
            .ok_or_else(|| {
                HawDBError::Storage("search document segment exceeds encoded admission".into())
            })?;
        if position != start_record && block_bytes + bytes > BLOCK_TARGET_BYTES {
            if plan.len() == plan.capacity() {
                let capacity = (plan.len() + 1).max(2 * plan.len());
                reserve_capacity(&mut plan, capacity, &mut plan_memory)?;
            }
            plan.push((start_record, position, block_bytes));
            start_record = position;
            block_bytes = 0;
        }
        block_bytes += bytes;
    }
    if plan.len() == plan.capacity() {
        let capacity = plan.len() + 1;
        reserve_capacity(&mut plan, capacity, &mut plan_memory)?;
    }
    plan.push((start_record, documents.len(), block_bytes));
    for (first, end, bytes) in &plan {
        if *bytes > u64::from(u32::MAX) {
            return Err(invalid("block exceeds the indexed length"));
        }
        index_len = index_len
            .checked_add(entry_len(
                documents[*first].header().id,
                documents[*end - 1].header().id,
            )?)
            .ok_or_else(|| invalid("index size overflow"))?;
    }
    if index_len as u64 > max_uncompressed_bytes {
        return Err(invalid("index exceeds its admission"));
    }
    let frame = (HEADER_BYTES + index_len) as u64;
    let mut index_memory = memory.spool.reserve(0)?;
    let mut index = Vec::new();
    reserve_capacity(&mut index, HEADER_BYTES + index_len, &mut index_memory)?;
    index.resize(HEADER_BYTES, 0);

    crate::build_memory::compression::require_qualified_zstd("workspace admission")?;
    let workspace = memory.retained.reserve(COMPRESSION_WORKSPACE_BYTES)?;
    let _scratch = memory.spool.reserve(
        2 * HEX_BUFFER_BYTES + hawdb_storage::compression::FrameEncoder::staging_bytes(),
    )?;
    let start = output.stream_position()?;
    let blocks_start = start
        .checked_add(frame)
        .ok_or_else(|| HawDBError::Storage("search artifact offset overflow".into()))?;
    output.seek(SeekFrom::Start(blocks_start))?;
    #[cfg(test)]
    evidence::started();
    let mut encoder = hawdb_storage::compression::FrameEncoder::new(SEARCH_COMPRESSION_LEVEL)?;
    let mut compressed_total = 0u64;
    let mut uncompressed_total = 0u64;
    let limit = max_compressed_bytes.checked_sub(frame).ok_or_else(|| {
        HawDBError::Storage(
            "search segment requires compressed bytes exceeding its admission".into(),
        )
    })?;
    for (first, end, bytes) in &plan {
        checkpoint(task)?;
        let mut sink = LimitedWriter {
            output: &mut *output,
            bytes: 0,
            limit: limit - compressed_total,
            digest: Crc32cHasher::new(),
            task,
        };
        // Every frame declares its block's length; zstd still bounds the
        // window of large single-record blocks by the compression level.
        let declared = Some(*bytes);
        let mut digest = Crc32cHasher::new();
        let mut counted = CountingWriter {
            writer: encoder.frame(declared, &mut sink)?,
            bytes: 0,
        };
        {
            let mut digested = DigestWriter {
                writer: &mut counted,
                digest: &mut digest,
            };
            let mut writer = CheckedWriter::new(&mut digested, Some(task));
            for document in &documents[*first..*end] {
                let content = records::content_len(document, Some(task))?;
                records::write_record(&mut writer, document, content)?;
            }
        }
        if counted.bytes != *bytes {
            return Err(invalid("records disagree with their admitted length"));
        }
        counted.writer.finish()?;
        let compressed_len =
            u32::try_from(sink.bytes).map_err(|_| invalid("block exceeds the indexed length"))?;
        compressed_total += sink.bytes;
        uncompressed_total += *bytes;
        encode_entry(
            &mut index,
            &WrittenBlock {
                compressed_len,
                uncompressed_len: *bytes as u32,
                compressed_crc: sink.digest.finish_u32(),
                uncompressed_crc: digest.finish_u32(),
                document_count: u32::try_from(end - first)
                    .map_err(|_| invalid("block holds too many records"))?,
                first: documents[*first].header().id,
                last: documents[*end - 1].header().id,
            },
        )?;
    }
    drop(encoder);
    drop(workspace);
    checkpoint(task)?;
    if index.len() != HEADER_BYTES + index_len {
        return Err(invalid("index disagrees with its planned length"));
    }
    let header = Header {
        block_count: u32::try_from(plan.len()).map_err(|_| invalid("too many blocks"))?,
        document_count: u32::try_from(documents.len()).map_err(|_| invalid("too many records"))?,
        index_len: index_len as u32,
        uncompressed_len: uncompressed_total,
    };
    index[..HEADER_BYTES].copy_from_slice(&header.encode());
    let length = frame
        .checked_add(compressed_total)
        .filter(|length| *length <= max_compressed_bytes)
        .ok_or_else(|| {
            HawDBError::Storage(
                "search segment requires compressed bytes exceeding its admission".into(),
            )
        })?;
    let end = start + length;
    debug_assert_eq!(output.stream_position()?, end);
    output.seek(SeekFrom::Start(start))?;
    output.write_all(&index)?;
    output.seek(SeekFrom::Start(end))?;
    let mut checksum = Crc32cHasher::new();
    checksum.update(&index);
    checkpoint(task)?;
    Ok((length, checksum.finish()))
}

struct CountingWriter<W> {
    writer: W,
    bytes: u64,
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.writer.write(bytes)?;
        self.bytes += written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
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
