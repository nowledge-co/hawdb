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

use super::*;
use crate::{decode_search_document_line, parse_snapshot_header};
use hawdb_integrity::Crc32cHasher;
use std::io::{self, BufRead, BufReader};

const INPUT_BYTES: usize = 8192;

pub(super) struct RangeReader<'a> {
    pub(super) file: &'a File,
    pub(super) offset: u64,
    pub(super) remaining: u64,
}

impl Read for RangeReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = self
            .remaining
            .min(output.len() as u64)
            .min(INPUT_BYTES as u64) as usize;
        if count == 0 {
            return Ok(0);
        }
        let end = self
            .offset
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::other("hydration range offset overflow"))?;
        read_search_range(self.file, self.offset, &mut output[..count])
            .map_err(io::Error::other)?;
        self.offset = end;
        self.remaining -= count as u64;
        Ok(count)
    }
}

pub(super) struct CheckedReader<R> {
    pub(super) inner: R,
    pub(super) digest: Crc32cHasher,
    pub(super) count: u64,
}

impl<R> CheckedReader<R> {
    pub(super) fn new(inner: R) -> Self {
        Self {
            inner,
            digest: Crc32cHasher::new(),
            count: 0,
        }
    }
}

impl<R: Read> Read for CheckedReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let limit = output.len().min(INPUT_BYTES);
        let count = self.inner.read(&mut output[..limit])?;
        self.digest.update(&output[..count]);
        self.count += count as u64;
        Ok(count)
    }
}

fn invalid(reason: impl std::fmt::Display) -> HawDBError {
    HawDBError::Storage(format!("search hydration {reason}"))
}

#[derive(Debug)]
struct Selection {
    documents: Vec<SearchDocument>,
    bytes: u64,
    peak_document_bytes: u64,
}

impl SearchOutOfCoreReader {
    pub(super) fn read_selected_hydration_segment(
        &self,
        segment: &SearchSegmentDescriptorEntry,
        ids: &BTreeSet<String>,
        remaining_bytes: u64,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<Vec<SearchDocument>> {
        let range = segment
            .payload_range
            .ok_or_else(|| invalid("segment has no payload range"))?;
        let input = RangeReader {
            file: &self.payload,
            offset: range.offset,
            remaining: range.length,
        };
        let selected = read_selected(
            input,
            range.length,
            range.checksum,
            segment,
            ids,
            self.config.max_uncompressed_segment_bytes.get(),
            remaining_bytes,
        )?;
        metrics.segment_range_reads = metrics.segment_range_reads.saturating_add(1);
        metrics.segment_bytes_read = metrics.segment_bytes_read.saturating_add(range.length);
        metrics.hydration_segment_bytes_read = metrics
            .hydration_segment_bytes_read
            .saturating_add(range.length);
        metrics.peak_segment_document_bytes = metrics
            .peak_segment_document_bytes
            .max(selected.peak_document_bytes);
        Ok(selected.documents)
    }
}

fn read_selected(
    input: impl Read,
    length: u64,
    checksum: u64,
    segment: &SearchSegmentDescriptorEntry,
    ids: &BTreeSet<String>,
    max_uncompressed_bytes: u64,
    remaining_bytes: u64,
) -> Result<Selection> {
    let mut range = CheckedReader::new(input.take(length));
    let mut buffered = BufReader::with_capacity(INPUT_BYTES, &mut range);
    let selected = read_envelope(
        &mut buffered,
        length,
        segment,
        ids,
        max_uncompressed_bytes,
        remaining_bytes,
    );
    // Preserve range-integrity precedence even if header parsing fails early.
    io::copy(&mut buffered, &mut io::sink())?;
    drop(buffered);
    if range.count != length {
        return Err(invalid("payload length mismatch"));
    }
    if range.digest.finish() != checksum {
        return Err(invalid("payload checksum mismatch"));
    }
    selected
}

fn read_envelope(
    buffered: &mut impl BufRead,
    length: u64,
    segment: &SearchSegmentDescriptorEntry,
    ids: &BTreeSet<String>,
    max_uncompressed_bytes: u64,
    remaining_bytes: u64,
) -> Result<Selection> {
    let mut header = Vec::new();
    // Preserve the envelope grammar, including noncanonical numeric spellings.
    // Header storage remains admitted by the compressed range, not a new cap.
    while !header.ends_with(b"\n\n") {
        if buffered.read_until(b'\n', &mut header)? == 0 {
            return Err(invalid("compressed envelope missing header terminator"));
        }
    }
    let compressed_bytes = length
        .checked_sub(header.len() as u64)
        .ok_or_else(|| invalid("compressed length underflow"))?;
    let header = std::str::from_utf8(&header[..header.len() - 2])
        .map_err(|_| invalid("compressed envelope header is not UTF-8"))?;
    let header = parse_snapshot_header(header)?;
    let expected_compressed_len = header
        .compressed_len
        .ok_or_else(|| invalid("missing compressed_len"))? as u64;
    let expected_compressed_checksum = header
        .compressed_checksum
        .ok_or_else(|| invalid("missing compressed_checksum"))?;
    let expected_len = header
        .uncompressed_len
        .ok_or_else(|| invalid("missing uncompressed_len"))? as u64;
    let expected_checksum = header
        .uncompressed_checksum
        .ok_or_else(|| invalid("missing uncompressed_checksum"))?;
    if compressed_bytes != expected_compressed_len {
        return Err(invalid("compressed length mismatch"));
    }
    if expected_len > max_uncompressed_bytes {
        return Err(invalid(format_args!("uncompressed payload requires {expected_len} bytes, exceeding {max_uncompressed_bytes}")));
    }

    let mut compressed = CheckedReader::new(buffered);
    let decoder = zstd::stream::read::Decoder::with_buffer(BufReader::with_capacity(
        INPUT_BYTES,
        &mut compressed,
    ))?;
    // A false small declaration must not inflate to the caller's larger limit.
    let mut inflated = CheckedReader::new(decoder.take(expected_len.saturating_add(1)));
    let mut text = BufReader::with_capacity(INPUT_BYTES, &mut inflated);
    let selected = select_documents(&mut text, segment, ids, remaining_bytes);
    // Results remain private until the entire segment has passed integrity.
    // Even an early syntax/budget error cannot hide a later read failure.
    let drained = io::copy(&mut text, &mut io::sink());
    drop(text);
    let inflated_count = inflated.count;
    let inflated_checksum = inflated.digest.finish();
    #[cfg(test)]
    tests::record_inflated_bytes(inflated_count);
    drop(inflated);
    let compressed_drained = io::copy(&mut compressed, &mut io::sink());
    let compressed_count = compressed.count;
    let compressed_checksum = compressed.digest.finish();
    compressed_drained?;
    if compressed_count != expected_compressed_len
        || compressed_checksum != expected_compressed_checksum
    {
        return Err(invalid("compressed checksum or length mismatch"));
    }
    drained?;
    if inflated_count != expected_len {
        return Err(invalid("uncompressed length mismatch"));
    }
    if inflated_checksum != expected_checksum {
        return Err(invalid("uncompressed checksum mismatch"));
    }
    selected
}

fn select_documents(
    text: &mut impl BufRead,
    segment: &SearchSegmentDescriptorEntry,
    ids: &BTreeSet<String>,
    remaining_bytes: u64,
) -> Result<Selection> {
    let mut selected = Selection {
        documents: Vec::new(),
        bytes: 0,
        peak_document_bytes: 0,
    };
    let mut previous = None::<String>;
    let mut count = 0usize;
    let mut line = String::new();
    loop {
        line.clear();
        if text.read_line(&mut line)? == 0 {
            break;
        }
        // Match str::lines(), including CRLF and a final line without newline.
        let line = match line.strip_suffix('\n') {
            Some(line) => line.strip_suffix('\r').unwrap_or(line),
            None => &line,
        };
        if line.is_empty() || line == "HAWDB_SEARCH_SEGMENT_V1" {
            continue;
        }
        let document = decode_search_document_line(line)?;
        if count == 0 && document.id != segment.first_document_id {
            return Err(invalid("document bounds do not match its descriptor"));
        }
        if previous
            .as_ref()
            .is_some_and(|previous| previous >= &document.id)
        {
            return Err(invalid("documents are not strictly ordered"));
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| invalid("document count overflow"))?;
        if count > segment.document_count {
            return Err(invalid("document count exceeds its descriptor"));
        }
        previous = Some(document.id.clone());
        let bytes = search_document_bytes(&document);
        selected.peak_document_bytes = selected
            .peak_document_bytes
            .max(selected.bytes.saturating_add(bytes));
        if ids.contains(&document.id) {
            selected.bytes = selected
                .bytes
                .checked_add(bytes)
                .ok_or_else(|| invalid("byte count overflow"))?;
            if selected.bytes > remaining_bytes {
                return Err(invalid(format_args!(
                    "requires {} bytes, exceeding {remaining_bytes}",
                    selected.bytes
                )));
            }
            selected.documents.push(document);
        }
    }
    if count != segment.document_count
        || previous.as_deref() != Some(segment.last_document_id.as_str())
    {
        return Err(invalid(
            "document count or bounds do not match its descriptor",
        ));
    }
    Ok(selected)
}

#[cfg(test)]
mod tests;
