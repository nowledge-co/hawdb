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

impl SearchOutOfCoreSegmentReader {
    pub(super) fn read_selected_hydration_segment(
        &self,
        config: &SearchOutOfCoreConfig,
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
            config.max_uncompressed_segment_bytes.get(),
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
    read_validated(
        input,
        length,
        checksum,
        max_uncompressed_bytes,
        None,
        |text| select_documents(text, segment, ids, remaining_bytes),
    )
}

#[derive(Clone, Copy)]
pub(super) struct ReadAdmission<'a> {
    pub(super) memory: &'a crate::build_memory::BuildMemory,
    pub(super) task: &'a hawdb_core::RuntimeTaskContext,
    pub(super) max_header_bytes: usize,
}

pub(super) fn read_validated<T>(
    input: impl Read,
    length: u64,
    checksum: u64,
    max_uncompressed_bytes: u64,
    admission: Option<ReadAdmission<'_>>,
    select: impl FnOnce(&mut dyn BufRead) -> Result<T>,
) -> Result<T> {
    // Three buffered readers coexist; draining also owns bounded copy scratch.
    let _buffers = admission
        .map(|admission| admission.memory.spool.reserve(4 * INPUT_BYTES))
        .transpose()?;
    let mut range = CheckedReader::new(input.take(length));
    let mut buffered = BufReader::with_capacity(INPUT_BYTES, &mut range);
    let selected = read_envelope(
        &mut buffered,
        length,
        max_uncompressed_bytes,
        admission,
        select,
    );
    if let Some(admission) = admission {
        crate::build_control::checkpoint(admission.task)?;
    }
    // Preserve range-integrity precedence even if header parsing fails early.
    io::copy(&mut buffered, &mut io::sink())?;
    drop(buffered);
    if range.count != length {
        return Err(invalid("payload length mismatch"));
    }
    if range.digest.finish() != checksum {
        return Err(invalid("payload checksum mismatch"));
    }
    if let Some(admission) = admission {
        crate::build_control::checkpoint(admission.task)?;
    }
    selected
}

fn read_envelope<T>(
    buffered: &mut impl BufRead,
    length: u64,
    max_uncompressed_bytes: u64,
    admission: Option<ReadAdmission<'_>>,
    select: impl FnOnce(&mut dyn BufRead) -> Result<T>,
) -> Result<T> {
    let mut header_memory = admission
        .map(|admission| admission.memory.spool.reserve(0))
        .transpose()?;
    let mut header = Vec::new();
    // The governed path admits the bounded envelope before copying each chunk.
    while !header.ends_with(b"\n\n") {
        if let Some(admission) = admission {
            crate::build_control::checkpoint(admission.task)?;
        }
        let available = match buffered.fill_buf() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if available.is_empty() {
            return Err(invalid("compressed envelope missing header terminator"));
        }
        let count = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        let required = header
            .len()
            .checked_add(count)
            .ok_or_else(|| invalid("envelope size overflow"))?;
        if admission.is_some_and(|admission| required > admission.max_header_bytes) {
            return Err(invalid("envelope header exceeds admission"));
        }
        if let Some(memory) = &mut header_memory {
            crate::build_memory::reserve_capacity(&mut header, required, memory)?;
        }
        header.extend_from_slice(&available[..count]);
        buffered.consume(count);
    }
    let compressed_bytes = length
        .checked_sub(header.len() as u64)
        .ok_or_else(|| invalid("compressed length underflow"))?;
    let header = std::str::from_utf8(&header[..header.len() - 2])
        .map_err(|_| invalid("compressed envelope header is not UTF-8"))?;
    let header = {
        let _parse_memory = admission
            .map(|admission| {
                let slots = crate::build_memory::checked_mul(
                    header.len(),
                    2 * std::mem::size_of::<&str>(),
                )?;
                admission
                    .memory
                    .spool
                    .reserve(crate::build_memory::checked_add(
                        slots,
                        6 * crate::build_memory::SET_ENTRY_BYTES,
                    )?)
            })
            .transpose()?;
        parse_snapshot_header(header)?
    };
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
    let compressed_input = BufReader::with_capacity(INPUT_BYTES, &mut compressed);
    let (selected, drained, inflated_count, inflated_checksum) = if let Some(admission) = admission
    {
        let decoder = crate::build_memory::decoder::Decoder::new(
            compressed_input,
            admission.memory,
            admission.task,
        )?;
        read_inflated(decoder, expected_len, select)
    } else {
        let decoder = zstd::stream::read::Decoder::with_buffer(compressed_input)?;
        read_inflated(decoder, expected_len, select)
    };
    if let Some(admission) = admission {
        crate::build_control::checkpoint(admission.task)?;
    }
    let compressed_drained = io::copy(&mut compressed, &mut io::sink());
    let compressed_count = compressed.count;
    let compressed_checksum = compressed.digest.finish();
    compressed_drained?;
    if compressed_count != expected_compressed_len
        || compressed_checksum != expected_compressed_checksum
    {
        return Err(invalid("compressed checksum or length mismatch"));
    }
    if let Err(error) = drained {
        // A failed decoder admission or sink can leave no resumable decoder.
        // Preserve that first failure instead of replacing it with a drain error.
        return selected.and_then(|_| Err(error.into()));
    }
    if inflated_count != expected_len {
        return Err(invalid("uncompressed length mismatch"));
    }
    if inflated_checksum != expected_checksum {
        return Err(invalid("uncompressed checksum mismatch"));
    }
    selected
}

fn read_inflated<T>(
    decoder: impl Read,
    expected_len: u64,
    select: impl FnOnce(&mut dyn BufRead) -> Result<T>,
) -> (Result<T>, io::Result<u64>, u64, u64) {
    // A false small declaration must not inflate to the caller's larger limit.
    let mut inflated = CheckedReader::new(decoder.take(expected_len.saturating_add(1)));
    let mut text = BufReader::with_capacity(INPUT_BYTES, &mut inflated);
    let selected = select(&mut text);
    // Results remain private until the entire segment has passed integrity.
    let drained = io::copy(&mut text, &mut io::sink());
    drop(text);
    #[cfg(test)]
    tests::record_inflated_bytes(inflated.count);
    (selected, drained, inflated.count, inflated.digest.finish())
}

fn select_documents(
    text: &mut dyn BufRead,
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

pub(super) mod selected_body;
pub(super) mod source;
