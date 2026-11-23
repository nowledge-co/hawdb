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
use crate::parse_snapshot_header;
use hawdb_core::{ResourceBudgetError, ResourceBudgetKind, ResourceBudgetScope};
use hawdb_integrity::Crc32cHasher;
use std::io::{self, BufRead, BufReader};

pub(super) mod query;
mod record;
pub(super) mod sidecar;
pub(super) use record::read_record;

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

/// The record count and id bounds one scanned range must match: a whole
/// descriptor segment, or one indexed content block (#1917).
#[derive(Debug, Clone, Copy)]
pub(super) struct Bounds<'a> {
    pub(super) first: &'a str,
    pub(super) last: &'a str,
    pub(super) count: usize,
}

impl<'a> Bounds<'a> {
    pub(super) fn of(segment: &'a SearchSegmentDescriptorEntry) -> Self {
        Self {
            first: &segment.first_document_id,
            last: &segment.last_document_id,
            count: segment.document_count,
        }
    }

    pub(super) fn of_block<B: std::ops::Deref<Target = [u8]>>(
        index: &'a super::content_blocks::Index<B>,
        entry: &super::content_blocks::Entry,
    ) -> Self {
        Self {
            first: index.first(entry),
            last: index.last(entry),
            count: entry.document_count as usize,
        }
    }
}

/// Positioned reads of one segment of an unaccounted payload file.
pub(super) struct PlainSource<'a> {
    pub(super) file: &'a File,
    pub(super) base: u64,
}

impl super::content_blocks::SegmentSource for PlainSource<'_> {
    fn read_exact_at(&self, offset: u64, output: &mut [u8]) -> Result<()> {
        let offset = self
            .base
            .checked_add(offset)
            .ok_or_else(|| invalid("range offset overflow"))?;
        read_search_range(self.file, offset, output)
    }
}

/// Routes the distinct candidate ids in ascending order, reading each lexical
/// document block once for all of them (#1917), and visits each id with its
/// `(artifact_index, segment)`.
pub(super) fn route_candidates<'r, 'c>(
    reader: &'r SearchOutOfCoreReader,
    candidates: &'c [SearchScoredCandidate],
    query: Option<crate::lexical_projection::QueryContext<'_>>,
    metrics: &mut SearchOutOfCoreMetrics,
    mut visit: impl FnMut(&'c str, usize, &'r SearchSegmentDescriptorEntry) -> Result<()>,
) -> Result<()> {
    let _memory = query
        .map(|query| {
            query
                .memory
                .reserve(candidates.len().saturating_mul(size_of::<&str>()))
        })
        .transpose()?;
    let mut ids = candidates
        .iter()
        .map(|candidate| candidate.id.as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    let routes = reader.segments_for_documents_with_context(&ids, query, metrics)?;
    for (id, route) in ids.into_iter().zip(&routes.routes) {
        let (artifact_index, segment) =
            route.ok_or_else(|| invalid("candidate has no visible document route"))?;
        visit(id, artifact_index, segment)?;
    }
    Ok(())
}

#[derive(Debug)]
pub(super) struct Selection {
    documents: Vec<SearchDocument>,
    bytes: u64,
    peak_document_bytes: u64,
}

/// The ids a selective read must release, and whether their documents carry
/// the stored embedding (text-only hits never rescore it, #1917).
#[derive(Clone, Copy)]
pub(super) struct Wanted<'a> {
    pub(super) ids: &'a BTreeSet<String>,
    pub(super) embeddings: bool,
}

#[derive(Clone, Copy)]
pub(super) struct HydrationBudget {
    pub(super) limit: u64,
    pub(super) used: u64,
}

impl SearchOutOfCoreSegmentReader {
    pub(super) fn read_selected_hydration_segment(
        &self,
        config: &SearchOutOfCoreConfig,
        segment: &SearchSegmentDescriptorEntry,
        wanted: Wanted<'_>,
        hydrated_bytes: u64,
        metrics: &mut SearchOutOfCoreMetrics,
        cache: &super::content_blocks::ContentCache,
    ) -> Result<(Vec<SearchDocument>, u64)> {
        let range = segment
            .payload_range
            .ok_or_else(|| invalid("segment has no payload range"))?;
        if self.content_encoding == super::content_blocks::ContentEncoding::Blocks1 {
            let cache = super::content_blocks::CacheScope::new(
                cache,
                self.payload_sha256,
                self.payload_generation,
            );
            return self.read_selected_blocks(
                config,
                segment,
                wanted,
                hydrated_bytes,
                metrics,
                cache,
            );
        }
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
            wanted,
            config.max_uncompressed_segment_bytes.get(),
            HydrationBudget {
                limit: config.max_hydrated_bytes.get(),
                used: hydrated_bytes,
            },
        )?;
        metrics.segment_range_reads = metrics.segment_range_reads.saturating_add(1);
        metrics.segment_bytes_read = metrics.segment_bytes_read.saturating_add(range.length);
        metrics.hydration_segment_bytes_read = metrics
            .hydration_segment_bytes_read
            .saturating_add(range.length);
        metrics.peak_segment_document_bytes = metrics
            .peak_segment_document_bytes
            .max(selected.peak_document_bytes);
        Ok((selected.documents, selected.bytes))
    }
}

impl SearchOutOfCoreSegmentReader {
    /// Reads and verifies only the indexed blocks that can hold `ids`.
    fn read_selected_blocks(
        &self,
        config: &SearchOutOfCoreConfig,
        segment: &SearchSegmentDescriptorEntry,
        wanted: Wanted<'_>,
        hydrated_bytes: u64,
        metrics: &mut SearchOutOfCoreMetrics,
        cache: Option<super::content_blocks::CacheScope<'_>>,
    ) -> Result<(Vec<SearchDocument>, u64)> {
        let ids = wanted.ids;
        use super::content_blocks::{self as blocks, Expect, Reads};
        let range = segment
            .payload_range
            .ok_or_else(|| invalid("segment has no payload range"))?;
        let source = PlainSource {
            file: &self.payload,
            base: range.offset,
        };
        let mut reads = Reads::default();
        let mut peak = 0u64;
        let result = (|| {
            let index = blocks::load_index(
                &source,
                cache,
                range.offset,
                Expect {
                    length: range.length,
                    checksum: range.checksum,
                    max_uncompressed_bytes: config.max_uncompressed_segment_bytes.get(),
                    descriptor: Some((
                        segment.document_count,
                        &segment.first_document_id,
                        &segment.last_document_id,
                    )),
                },
                None,
                &mut reads,
            )?;
            let mut needed = ids
                .iter()
                .filter_map(|id| index.find(id))
                .collect::<Vec<_>>();
            needed.dedup();
            let mut decoder = None;
            let mut documents = Vec::new();
            let mut bytes = 0u64;
            for block in needed {
                let entry = index.entries()[block];
                let bounds = Bounds::of_block(&index, &entry);
                let budget = HydrationBudget {
                    limit: config.max_hydrated_bytes.get(),
                    used: hydrated_bytes.saturating_add(bytes),
                };
                let selected = if u64::from(entry.uncompressed_len)
                    > blocks::MAX_BUFFERED_BLOCK_BYTES
                {
                    reads.range_reads += 1;
                    reads.bytes_read += u64::from(entry.compressed_len);
                    reads.blocks_read += 1;
                    reads.bytes_inflated += u64::from(entry.uncompressed_len);
                    blocks::stream_blocks(
                        RangeReader {
                            file: &self.payload,
                            offset: range.offset + entry.offset,
                            remaining: u64::from(entry.compressed_len),
                        },
                        &index,
                        block..block + 1,
                        None,
                        |text| select_documents(text, bounds, wanted, budget),
                    )?
                } else {
                    let decoder = match &mut decoder {
                        Some(decoder) => decoder,
                        None => decoder
                            .insert(crate::build_memory::decoder::BlockDecoder::new(None, None)?),
                    };
                    let body = blocks::load_block(
                        &source,
                        cache,
                        range.offset,
                        &entry,
                        decoder,
                        None,
                        &mut reads,
                    )?;
                    select_block_records(&body, bounds, wanted, budget)?
                };
                peak = peak.max(bytes.saturating_add(selected.peak_document_bytes));
                bytes = bytes.saturating_add(selected.bytes);
                documents.extend(selected.documents);
            }
            Ok((documents, bytes))
        })();
        reads.record(metrics);
        metrics.peak_segment_document_bytes = metrics.peak_segment_document_bytes.max(peak);
        result
    }
}

fn read_selected(
    input: impl Read,
    length: u64,
    checksum: u64,
    segment: &SearchSegmentDescriptorEntry,
    wanted: Wanted<'_>,
    max_uncompressed_bytes: u64,
    budget: HydrationBudget,
) -> Result<Selection> {
    read_validated(
        input,
        length,
        checksum,
        max_uncompressed_bytes,
        None,
        |text| select_documents(text, Bounds::of(segment), wanted, budget),
    )
}

#[derive(Clone, Copy)]
pub(super) struct ReadAdmission<'a> {
    pub(super) memory: &'a crate::build_memory::BuildMemory,
    pub(super) task: &'a hawdb_core::RuntimeTaskContext,
    pub(super) max_header_bytes: usize,
}

#[derive(Clone, Copy)]
pub(super) struct DecodeAdmission<'a> {
    pub(super) memory: &'a hawdb_executor::QueryMemoryAccount,
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
    read_validated_with_account(
        input,
        length,
        checksum,
        max_uncompressed_bytes,
        admission.map(|admission| DecodeAdmission {
            memory: &admission.memory.spool,
            task: admission.task,
            max_header_bytes: admission.max_header_bytes,
        }),
        select,
    )
}

pub(super) fn read_validated_with_account<T>(
    input: impl Read,
    length: u64,
    checksum: u64,
    max_uncompressed_bytes: u64,
    admission: Option<DecodeAdmission<'_>>,
    select: impl FnOnce(&mut dyn BufRead) -> Result<T>,
) -> Result<T> {
    // Three buffered readers coexist; draining also owns bounded copy scratch.
    let _buffers = admission
        .map(|admission| admission.memory.reserve(4 * INPUT_BYTES))
        .transpose()?;
    let mut range = CheckedReader::new(input.take(length));
    // The artifact's encoding is gated by the selector; the segment's own
    // magic selects the frame reader (#1917).
    let mut magic = [0; 8];
    let prefix = read_prefix(&mut range, &mut magic)?;
    if magic[..prefix] == super::content_blocks::MAGIC[..] {
        return super::content_blocks::read_segment(
            &mut range,
            length,
            checksum,
            max_uncompressed_bytes,
            admission,
            select,
        );
    }
    let mut buffered = BufReader::with_capacity(INPUT_BYTES, (&magic[..prefix]).chain(&mut range));
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
    io::copy(&mut buffered, &mut io::sink()).map_err(crate::build_memory::read::io_error)?;
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

/// Reads up to `output.len()` bytes, fewer only at the end of the range.
fn read_prefix(input: &mut impl Read, output: &mut [u8]) -> Result<usize> {
    let mut count = 0;
    while count < output.len() {
        match input.read(&mut output[count..]) {
            Ok(0) => break,
            Ok(read) => count += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(crate::build_memory::read::io_error(error)),
        }
    }
    Ok(count)
}

fn read_envelope<T>(
    buffered: &mut impl BufRead,
    length: u64,
    max_uncompressed_bytes: u64,
    admission: Option<DecodeAdmission<'_>>,
    select: impl FnOnce(&mut dyn BufRead) -> Result<T>,
) -> Result<T> {
    let mut header_memory = admission
        .map(|admission| admission.memory.reserve(0))
        .transpose()?;
    let mut header = Vec::new();
    // The governed path admits the bounded envelope before copying each chunk.
    while !header.ends_with(b"\n\n") {
        if let Some(admission) = admission {
            crate::build_control::checkpoint(admission.task)?;
        }
        let available = match buffered.fill_buf() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result.map_err(crate::build_memory::read::io_error)?,
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
                admission.memory.reserve(crate::build_memory::checked_add(
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
        let decoder = crate::build_memory::decoder::Decoder::with_account(
            compressed_input,
            admission.memory,
            admission.task,
        )?;
        read_inflated(decoder, expected_len, select)
    } else {
        let decoder = hawdb_storage::compression::Decoder::with_buffer(compressed_input)?;
        read_inflated(decoder, expected_len, select)
    };
    if let Some(admission) = admission {
        crate::build_control::checkpoint(admission.task)?;
    }
    let compressed_drained = io::copy(&mut compressed, &mut io::sink());
    let compressed_count = compressed.count;
    let compressed_checksum = compressed.digest.finish();
    compressed_drained.map_err(crate::build_memory::read::io_error)?;
    if compressed_count != expected_compressed_len
        || compressed_checksum != expected_compressed_checksum
    {
        return Err(invalid("compressed checksum or length mismatch"));
    }
    if let Err(error) = drained {
        // A failed decoder admission or sink can leave no resumable decoder.
        // Preserve that first failure instead of replacing it with a drain error.
        return selected.and_then(|_| Err(crate::build_memory::read::io_error(error)));
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

/// A document line's id, the [`search_document_bytes`] its decoded document
/// would have, and how many of those bytes are its vector, read without
/// decoding its title, content, vector or metadata. Metadata keys of one
/// document are distinct, as written.
fn summarize_document_line(line: &str) -> Result<(String, u64, u64)> {
    let mut fields = line.split('\t');
    let (Some("doc"), Some(id), Some(title), Some(content), Some(embedding), Some(metadata), None) = (
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
    ) else {
        return Err(HawDBError::Storage(format!(
            "invalid search document line: {line}"
        )));
    };
    let hex = |raw: &str| -> Result<u64> {
        if raw.len().is_multiple_of(2) {
            Ok((raw.len() / 2) as u64)
        } else {
            Err(HawDBError::Storage(format!(
                "invalid hex string length: {}",
                raw.len()
            )))
        }
    };
    // Counting separators bytewise, rather than splitting, keeps a vector of
    // a thousand text-encoded components cheap to skip.
    let dimension = if embedding.is_empty() {
        0
    } else {
        embedding.bytes().filter(|byte| *byte == b',').count() as u64 + 1
    };
    let vector = dimension.saturating_mul(std::mem::size_of::<f32>() as u64);
    let mut bytes = hex(id)?
        .saturating_add(hex(title)?)
        .saturating_add(hex(content)?)
        .saturating_add(vector);
    if !metadata.is_empty() {
        for pair in metadata.split(';') {
            let Some((key, value)) = pair.split_once('=') else {
                return Err(HawDBError::Storage(format!(
                    "invalid metadata pair: {pair}"
                )));
            };
            bytes = bytes.saturating_add(hex(key)?).saturating_add(hex(value)?);
        }
    }
    Ok((crate::decode_string(id)?, bytes, vector))
}

/// [`select_documents`] over one buffered block of binary records (#1917):
/// records are parsed in place, and only the selected ones are decoded.
fn select_block_records(
    body: &[u8],
    bounds: Bounds<'_>,
    wanted: Wanted<'_>,
    budget: HydrationBudget,
) -> Result<Selection> {
    let remaining_bytes = budget.limit - budget.used;
    let mut selected = Selection {
        documents: Vec::new(),
        bytes: 0,
        peak_document_bytes: 0,
    };
    super::content_blocks::records::scan(body, bounds, |record| {
        // Text-only hits hold no vector, so they are not charged for it.
        let bytes = record.document_bytes()
            - if wanted.embeddings {
                0
            } else {
                (record.dimension() * size_of::<f32>()) as u64
            };
        selected.peak_document_bytes = selected
            .peak_document_bytes
            .max(selected.bytes.saturating_add(bytes));
        if !wanted.ids.contains(record.id) {
            return Ok(());
        }
        let document = record.decode(wanted.embeddings)?;
        debug_assert_eq!(search_document_bytes(&document), bytes);
        admit_selected(&mut selected, bytes, budget, remaining_bytes)?;
        selected.documents.push(document);
        Ok(())
    })?;
    Ok(selected)
}

/// Charges one selected document to the hydration byte budget.
fn admit_selected(
    selected: &mut Selection,
    bytes: u64,
    budget: HydrationBudget,
    remaining_bytes: u64,
) -> Result<()> {
    let previous_bytes = selected.bytes;
    selected.bytes = selected
        .bytes
        .checked_add(bytes)
        .ok_or_else(|| invalid("byte count overflow"))?;
    if selected.bytes > remaining_bytes {
        return Err(ResourceBudgetError {
            resource: ResourceBudgetKind::MemoryBytes,
            scope: ResourceBudgetScope::Operator,
            owner: "search hydration".to_string(),
            memory_class: None,
            requested: bytes,
            // Preserve the full operation boundary across physical segments.
            used: budget.used + previous_bytes,
            budget: budget.limit,
            budget_option: "SearchOutOfCoreConfig.max_hydrated_bytes",
            message: format!(
                "search hydration requires {} bytes, exceeding {remaining_bytes}",
                selected.bytes
            ),
        }
        .into());
    }
    Ok(())
}

/// Verifies a text record stream's order, bounds and count like
/// [`select_documents`] with nothing selected, reading each record through
/// one admitted line buffer of at most `max_record_bytes`.
pub(super) fn verify_text_records(
    text: &mut dyn BufRead,
    segment: Bounds<'_>,
    max_record_bytes: u64,
    memory: &hawdb_executor::QueryMemoryAccount,
    task: &hawdb_core::RuntimeTaskContext,
) -> Result<()> {
    let mut line_memory = memory.reserve(0)?;
    let mut line = Vec::new();
    let mut previous = None::<String>;
    let mut count = 0usize;
    while read_record(
        text,
        &mut line,
        usize::try_from(max_record_bytes).unwrap_or(usize::MAX),
        task,
        &mut line_memory,
    )? {
        let record = std::str::from_utf8(&line).map_err(|_| invalid("invalid record UTF-8"))?;
        let record = match record.strip_suffix('\n') {
            Some(record) => record.strip_suffix('\r').unwrap_or(record),
            None => record,
        };
        if record.is_empty() || record == "HAWDB_SEARCH_SEGMENT_V1" {
            continue;
        }
        let (id, _, _) = summarize_document_line(record)?;
        if count == 0 && id != segment.first {
            return Err(invalid("document bounds do not match its descriptor"));
        }
        if previous.as_ref().is_some_and(|previous| previous >= &id) {
            return Err(invalid("documents are not strictly ordered"));
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| invalid("document count overflow"))?;
        if count > segment.count {
            return Err(invalid("document count exceeds its descriptor"));
        }
        previous = Some(id);
    }
    if count != segment.count || previous.as_deref() != Some(segment.last) {
        return Err(invalid(
            "document count or bounds do not match its descriptor",
        ));
    }
    Ok(())
}

pub(super) fn select_documents(
    text: &mut dyn BufRead,
    segment: Bounds<'_>,
    wanted: Wanted<'_>,
    budget: HydrationBudget,
) -> Result<Selection> {
    let ids = wanted.ids;
    let remaining_bytes = budget.limit - budget.used;
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
        // Only the selected lines are decoded (#1611). Every line still
        // proves the segment's order, bounds and count from its id; the
        // checksummed range already fences the bytes of the others.
        // Text-only hits hold no vector, so they are not charged for it.
        let (id, bytes, vector) = summarize_document_line(line)?;
        let bytes = if wanted.embeddings {
            bytes
        } else {
            bytes - vector
        };
        if count == 0 && id != segment.first {
            return Err(invalid("document bounds do not match its descriptor"));
        }
        if previous.as_ref().is_some_and(|previous| previous >= &id) {
            return Err(invalid("documents are not strictly ordered"));
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| invalid("document count overflow"))?;
        if count > segment.count {
            return Err(invalid("document count exceeds its descriptor"));
        }
        selected.peak_document_bytes = selected
            .peak_document_bytes
            .max(selected.bytes.saturating_add(bytes));
        let selected_line = ids.contains(&id);
        previous = Some(id);
        if selected_line {
            let document = crate::decode_search_document_line_with(line, wanted.embeddings)?;
            debug_assert_eq!(search_document_bytes(&document), bytes);
            admit_selected(&mut selected, bytes, budget, remaining_bytes)?;
            selected.documents.push(document);
        }
    }
    if count != segment.count || previous.as_deref() != Some(segment.last) {
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
