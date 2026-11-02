// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Binary raw-vector sidecar segments (`HAWDB_SEARCH_OUT_OF_CORE_LAYOUT_V2`).
//!
//! One segment stores the embeddings of one descriptor segment as fixed-stride
//! little-endian `f32` rows behind a small checksummed head:
//!
//! ```text
//! [0, 8)      magic `HDVSEG02`
//! [8, 12)     row count `n` (u32 LE)
//! [12, 16)    dimension `d` (u32 LE; 0 only when n == 0)
//! [16, 20)    head length `h` (u32 LE; multiple of 4, includes the head CRC)
//! [20, 24)    ID blob length (u32 LE)
//! [24, ..)    n cumulative ID end offsets (u32 LE)
//! [.., ..)    UTF-8 document IDs, strictly increasing, zero-padded to 4 bytes
//! [.., h-4)   n row CRC32C values (u32 LE)
//! [h-4, h)    CRC32C of bytes [0, h-4)
//! [h, ..)     n rows of d little-endian f32 values
//! ```
//!
//! Every byte is covered by either the head CRC or one row CRC, so a reader can
//! verify exactly the bytes it reads: the head plus the selected rows. Unselected
//! rows are neither read nor decoded. Row ordinals are implicit
//! (`vector_ordinal_base + index`), so they cannot disagree with the layout.

use super::{SearchSegmentDescriptorEntry, SearchVectorDocument};
use crate::error::{HawDBError, Result};
use hawdb_integrity::Crc32cHasher;

pub(super) const MAGIC: &[u8; 8] = b"HDVSEG02";
const FIXED_BYTES: usize = 24;
/// One read normally covers the complete head and nearby leading rows.
const PREFIX_BYTES: usize = 8 << 10;
/// Contiguous selected rows are read together up to this many bytes.
const MAX_RUN_BYTES: usize = 256 << 10;

fn invalid(segment_id: u64, reason: &str) -> HawDBError {
    HawDBError::Storage(format!(
        "search segment {segment_id} vector sidecar {reason}"
    ))
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut hasher = Crc32cHasher::new();
    hasher.update(bytes);
    hasher.finish_u32()
}

fn align4(value: usize) -> Option<usize> {
    value.checked_add(3).map(|value| value & !3)
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("four-byte slice"),
    )
}

fn checked_u32(value: usize, what: &str) -> Result<u32> {
    u32::try_from(value)
        .map_err(|_| HawDBError::Storage(format!("search vector segment {what} exceeds u32")))
}

fn head_len(count: usize, id_bytes: usize) -> Option<usize> {
    let ids_end = FIXED_BYTES
        .checked_add(count.checked_mul(4)?)?
        .checked_add(id_bytes)?;
    align4(ids_end)?
        .checked_add(count.checked_mul(4)?)?
        .checked_add(4)
}

/// Exact encoded size for `count` rows whose IDs total `id_bytes` bytes.
pub(super) fn encoded_len(count: usize, id_bytes: usize, dimension: usize) -> Result<usize> {
    head_len(count, id_bytes)
        .and_then(|head| head.checked_add(count.checked_mul(dimension)?.checked_mul(4)?))
        .ok_or_else(|| HawDBError::Storage("search vector segment size overflows".into()))
}

/// Encodes ordered `(id, embedding)` rows. The caller supplies the rows in
/// document-ID order and has already admitted `encoded_len` bytes.
pub(super) fn encode_into(
    rows: &[(&str, &[f32])],
    output: &mut Vec<u8>,
    mut checkpoint: impl FnMut() -> Result<()>,
) -> Result<()> {
    let dimension = rows.first().map_or(0, |(_, embedding)| embedding.len());
    let id_bytes = rows
        .iter()
        .try_fold(0usize, |total, (id, _)| total.checked_add(id.len()))
        .ok_or_else(|| HawDBError::Storage("search vector segment IDs overflow".into()))?;
    let head = head_len(rows.len(), id_bytes)
        .ok_or_else(|| HawDBError::Storage("search vector segment head overflows".into()))?;
    let total = encoded_len(rows.len(), id_bytes, dimension)?;
    let start = output.len();
    output.reserve_exact(total);
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&checked_u32(rows.len(), "row count")?.to_le_bytes());
    output.extend_from_slice(&checked_u32(dimension, "dimension")?.to_le_bytes());
    output.extend_from_slice(&checked_u32(head, "head")?.to_le_bytes());
    output.extend_from_slice(&checked_u32(id_bytes, "ID blob")?.to_le_bytes());
    let mut end = 0usize;
    for (id, _) in rows {
        end += id.len();
        output.extend_from_slice(&checked_u32(end, "ID offset")?.to_le_bytes());
    }
    for (index, (id, embedding)) in rows.iter().enumerate() {
        if index != 0 && rows[index - 1].0 >= *id {
            return Err(HawDBError::Storage(
                "search vector segment IDs are not strictly increasing".into(),
            ));
        }
        if embedding.is_empty() || embedding.len() != dimension {
            return Err(HawDBError::Storage(format!(
                "search document {id} has an invalid embedding dimension"
            )));
        }
        output.extend_from_slice(id.as_bytes());
    }
    output.resize(
        start + align4(output.len() - start).expect("bounded by the admitted head"),
        0,
    );
    let crc_offset = output.len();
    output.resize(crc_offset + rows.len() * 4, 0);
    output.extend_from_slice(&[0; 4]);
    debug_assert_eq!(output.len() - start, head);
    let mut row = Vec::with_capacity(dimension * 4);
    for (index, (id, embedding)) in rows.iter().enumerate() {
        if index.is_multiple_of(64) {
            checkpoint()?;
        }
        row.clear();
        for value in *embedding {
            if !value.is_finite() {
                return Err(HawDBError::Storage(format!(
                    "search document {id} has a non-finite embedding value"
                )));
            }
            row.extend_from_slice(&value.to_le_bytes());
        }
        let checksum = crc32c(&row).to_le_bytes();
        output[crc_offset + index * 4..crc_offset + index * 4 + 4].copy_from_slice(&checksum);
        output.extend_from_slice(&row);
    }
    let head_checksum = crc32c(&output[start..start + head - 4]).to_le_bytes();
    output[start + head - 4..start + head].copy_from_slice(&head_checksum);
    debug_assert_eq!(output.len() - start, total);
    Ok(())
}

/// What the layout and descriptor promise about one stored segment.
#[derive(Clone, Copy)]
pub(super) struct Expected<'a> {
    pub(super) segment: &'a SearchSegmentDescriptorEntry,
    pub(super) payload_len: u64,
    pub(super) count: usize,
    pub(super) vector_ordinal_base: u64,
    pub(super) dimension: Option<usize>,
}

/// Physical reads, memory admission and cancellation supplied by one reader.
pub(super) trait Source {
    /// Reads `output.len()` bytes at `offset` relative to the segment payload.
    fn read_exact_at(&mut self, offset: u64, output: &mut [u8]) -> Result<()>;
    /// Admits `bytes` additional transient bytes before they are allocated.
    fn reserve(&mut self, bytes: usize) -> Result<()>;
    fn checkpoint(&mut self) -> Result<()>;
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct ReadStats {
    pub(super) reads: u64,
    pub(super) bytes: u64,
    pub(super) peak_bytes: u64,
}

struct Head {
    bytes: Vec<u8>,
    count: usize,
    dimension: usize,
    head_len: usize,
    ids_start: usize,
    crcs_start: usize,
    id_padding: usize,
}

impl Head {
    fn id(&self, index: usize) -> &str {
        let start = if index == 0 {
            0
        } else {
            u32_at(&self.bytes, FIXED_BYTES + (index - 1) * 4) as usize
        };
        let end = u32_at(&self.bytes, FIXED_BYTES + index * 4) as usize;
        self.ids()
            .get(start..end)
            .expect("ID boundaries were validated with the head")
    }

    fn ids(&self) -> &str {
        std::str::from_utf8(&self.bytes[self.ids_start..self.crcs_start - self.id_padding])
            .expect("the ID blob was validated with the head")
    }

    fn row_crc(&self, index: usize) -> u32 {
        u32_at(&self.bytes, self.crcs_start + index * 4)
    }

    fn row_bytes(&self) -> usize {
        self.dimension * 4
    }

    fn row_offset(&self, index: usize) -> usize {
        self.head_len + index * self.row_bytes()
    }
}

fn read_head(
    source: &mut impl Source,
    expected: Expected<'_>,
    stats: &mut ReadStats,
) -> Result<Head> {
    let segment_id = expected.segment.segment_id;
    let payload_len = usize::try_from(expected.payload_len)
        .map_err(|_| invalid(segment_id, "exceeds the platform address space"))?;
    if payload_len < FIXED_BYTES {
        return Err(invalid(segment_id, "is shorter than its header"));
    }
    let prefix = payload_len.min(PREFIX_BYTES);
    source.reserve(prefix)?;
    let mut bytes = vec![0u8; prefix];
    source.read_exact_at(0, &mut bytes)?;
    stats.reads += 1;
    stats.bytes += prefix as u64;
    if &bytes[..8] != MAGIC {
        // V1 stored zstd-compressed text behind the shared compressed envelope.
        return Err(invalid(
            segment_id,
            if bytes.starts_with(b"HAWDB_COMPRESSED_V1\n") {
                "uses the superseded text format; rebuild the search projection"
            } else {
                "has an invalid header"
            },
        ));
    }
    let count = u32_at(&bytes, 8) as usize;
    let dimension = u32_at(&bytes, 12) as usize;
    let stored_head = u32_at(&bytes, 16) as usize;
    let id_bytes = u32_at(&bytes, 20) as usize;
    let head = head_len(count, id_bytes);
    let total = encoded_len(count, id_bytes, dimension).ok();
    if count != expected.count
        || (count == 0) != (dimension == 0)
        || (count != 0 && expected.dimension != Some(dimension))
        || head != Some(stored_head)
        || total != Some(payload_len)
    {
        return Err(invalid(segment_id, "header does not match its layout"));
    }
    if stored_head > prefix {
        let missing = stored_head - prefix;
        source.reserve(missing)?;
        bytes.resize(stored_head, 0);
        source.read_exact_at(prefix as u64, &mut bytes[prefix..])?;
        stats.reads += 1;
        stats.bytes += missing as u64;
    }
    if crc32c(&bytes[..stored_head - 4]) != u32_at(&bytes, stored_head - 4) {
        return Err(invalid(segment_id, "head checksum mismatch"));
    }
    let ids_start = FIXED_BYTES + count * 4;
    let crcs_start = align4(ids_start + id_bytes).expect("validated head size");
    // One UTF-8 pass over the blob; each ID must then start and end on a
    // character boundary, which `str::get` checks.
    let blob = std::str::from_utf8(&bytes[ids_start..ids_start + id_bytes])
        .map_err(|_| invalid(segment_id, "has a non-UTF-8 ID"))?;
    let mut previous: Option<&str> = None;
    let mut start = 0usize;
    for index in 0..count {
        if index.is_multiple_of(1024) {
            source.checkpoint()?;
        }
        let end = u32_at(&bytes, FIXED_BYTES + index * 4) as usize;
        let id = (end >= start)
            .then(|| blob.get(start..end))
            .flatten()
            .ok_or_else(|| invalid(segment_id, "has invalid ID offsets"))?;
        if previous.is_some_and(|previous| previous >= id)
            || id < expected.segment.first_document_id.as_str()
            || id > expected.segment.last_document_id.as_str()
        {
            return Err(invalid(segment_id, "count, bounds, or ordering mismatch"));
        }
        previous = Some(id);
        start = end;
    }
    if start != id_bytes
        || bytes[ids_start + id_bytes..crcs_start]
            .iter()
            .any(|b| *b != 0)
    {
        return Err(invalid(segment_id, "has invalid ID offsets"));
    }
    stats.peak_bytes = stats.peak_bytes.max(bytes.len() as u64);
    Ok(Head {
        bytes,
        count,
        dimension,
        head_len: stored_head,
        ids_start,
        crcs_start,
        id_padding: crcs_start - ids_start - id_bytes,
    })
}

fn decode_row(
    segment_id: u64,
    head: &Head,
    index: usize,
    row: &[u8],
    document: &mut SearchVectorDocument,
) -> Result<()> {
    if crc32c(row) != head.row_crc(index) {
        return Err(invalid(segment_id, "row checksum mismatch"));
    }
    document.id.clear();
    document.id.push_str(head.id(index));
    document.embedding.clear();
    document.embedding.extend(
        row.chunks_exact(4)
            .map(|value| f32::from_le_bytes(value.try_into().expect("four-byte chunk"))),
    );
    if document.embedding.iter().any(|value| !value.is_finite()) {
        return Err(invalid(segment_id, "has an invalid embedding"));
    }
    Ok(())
}

/// Reads, verifies and visits only rows whose ordinal is selected.
///
/// The head (IDs and row checksums) is always verified. Each selected row is
/// verified against its own checksum before it is decoded. The visitor's
/// document is reused between rows.
pub(super) fn visit_selected(
    source: &mut impl Source,
    expected: Expected<'_>,
    mut selected: impl FnMut(u64) -> bool,
    mut visit: impl FnMut(&SearchVectorDocument) -> Result<()>,
) -> Result<ReadStats> {
    let segment_id = expected.segment.segment_id;
    let mut stats = ReadStats::default();
    let head = read_head(source, expected, &mut stats)?;
    if head.count == 0 {
        return Ok(stats);
    }
    let row_bytes = head.row_bytes();
    let rows_per_run = (MAX_RUN_BYTES / row_bytes).max(1);
    source.reserve(
        row_bytes
            .checked_add(head.dimension * 4)
            .ok_or_else(|| invalid(segment_id, "row size overflows"))?,
    )?;
    let mut document = SearchVectorDocument {
        id: String::new(),
        embedding: Vec::with_capacity(head.dimension),
    };
    let mut run = Vec::new();
    let mut run_reserved = 0usize;
    let mut index = 0usize;
    while index < head.count {
        source.checkpoint()?;
        let ordinal = expected
            .vector_ordinal_base
            .checked_add(index as u64)
            .ok_or_else(|| invalid(segment_id, "ordinal overflows"))?;
        if !selected(ordinal) {
            index += 1;
            continue;
        }
        // Extend over directly following selected rows.
        let start = index;
        let mut end = index + 1;
        while end < head.count
            && end - start < rows_per_run
            && selected(ordinal + (end - start) as u64)
        {
            end += 1;
        }
        let first = head.row_offset(start);
        let last = head.row_offset(end);
        if last <= head.bytes.len() {
            for row in start..end {
                let offset = head.row_offset(row);
                decode_row(
                    segment_id,
                    &head,
                    row,
                    &head.bytes[offset..offset + row_bytes],
                    &mut document,
                )?;
                visit(&document)?;
            }
        } else {
            let length = last - first;
            if length > run_reserved {
                source.reserve(length - run_reserved)?;
                run_reserved = length;
            }
            run.resize(length, 0);
            source.read_exact_at(first as u64, &mut run)?;
            stats.reads += 1;
            stats.bytes += length as u64;
            for row in start..end {
                let offset = (row - start) * row_bytes;
                decode_row(
                    segment_id,
                    &head,
                    row,
                    &run[offset..offset + row_bytes],
                    &mut document,
                )?;
                visit(&document)?;
            }
        }
        index = end;
    }
    stats.peak_bytes = stats
        .peak_bytes
        .max((head.bytes.len() + run.len() + head.dimension * 4) as u64);
    Ok(stats)
}

/// Visits every document ID of a segment without reading any vector row.
#[cfg(feature = "vector-search")]
pub(super) fn visit_ids(
    source: &mut impl Source,
    expected: Expected<'_>,
    mut visit: impl FnMut(u64, &str) -> Result<()>,
) -> Result<ReadStats> {
    let mut stats = ReadStats::default();
    let head = read_head(source, expected, &mut stats)?;
    for index in 0..head.count {
        if index.is_multiple_of(1024) {
            source.checkpoint()?;
        }
        visit(expected.vector_ordinal_base + index as u64, head.id(index))?;
    }
    Ok(stats)
}

#[cfg(test)]
pub(super) struct SliceSource<'a>(pub(super) &'a [u8]);

#[cfg(test)]
impl Source for SliceSource<'_> {
    fn read_exact_at(&mut self, offset: u64, output: &mut [u8]) -> Result<()> {
        let start = offset as usize;
        let bytes = self
            .0
            .get(start..start + output.len())
            .ok_or_else(|| HawDBError::Storage("short test read".into()))?;
        output.copy_from_slice(bytes);
        Ok(())
    }
    fn reserve(&mut self, _bytes: usize) -> Result<()> {
        Ok(())
    }
    fn checkpoint(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
