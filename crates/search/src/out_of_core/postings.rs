// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Per-segment metadata value postings (`HAWDB_SEARCH_OUT_OF_CORE_LAYOUT_V3`).
//!
//! Each segment's metadata artifact holds, after its metadata sidecar, a
//! postings range that maps every value of every plain metadata field (see
//! [`indexed_field`]) to the segment documents holding it. An `Eq` or `In`
//! candidate filter then reads the matching documents instead of decoding
//! every record of the segment (#1613). Values are stored normalized exactly
//! as the filter compares them ([`crate::normalize_metadata_filter_value`]).
//!
//! The payload is uncompressed; the layout range checksum covers every byte.
//! All integers are little-endian `u32`:
//!
//! ```text
//! [0, 8)      magic `HDPOST01`
//! [8, 12)     document count `n` (the segment's document count)
//! [12, 16)    field count `f`
//! [16, ..)    n cumulative document ID end offsets
//! [.., ..)    UTF-8 document IDs in segment (strictly increasing) order
//! [.., ..)    n bytes: 1 when the document has a vector, else 0; vector
//!             ordinals are `vector_ordinal_base + preceding vectors`
//! f fields, names strictly increasing:
//!   name length, UTF-8 name, value count `v`
//!   (`v == 0`: the field has more than MAX_DISTINCT_VALUES values or a
//!   value longer than MAX_VALUE_BYTES in this segment and is not indexed)
//!   v values, strictly increasing:
//!     value length, UTF-8 normalized value, entry count `e >= 1`,
//!     e strictly increasing document indexes below `n`
//! ```
//!
//! A field absent from the payload has no value in any segment document.

use super::*;
use crate::build_memory::{checked_add as add, checked_mul as mul, reserve_capacity};
use crate::document_encoding::HeaderSource;
use crate::lexical_projection::QueryContext;
use hawdb_executor::{QueryMemoryAccount, QueryMemoryLease};

#[cfg(test)]
mod tests;

pub(super) const MAGIC: &[u8; 8] = b"HDPOST01";
const FIXED_BYTES: usize = 16;
/// A field with more distinct normalized values in one segment is not indexed;
/// its filters keep the metadata scan for that segment.
pub(super) const MAX_DISTINCT_VALUES: usize = 4096;
/// A field holding a longer normalized value in a segment is not indexed
/// either: postings are uncompressed, so free-text metadata stays in the
/// compressed sidecar only.
pub(super) const MAX_VALUE_BYTES: usize = 256;
/// One bit per predicate in the per-document match mask.
const MAX_PREDICATES: usize = u64::BITS as usize;

/// Single-valued metadata fields compared with [`crate::metadata_value_matches`]
/// normalization. `kind` has its own matching, `labels` and `metadata.*` are
/// list-valued, `space_id` has a default and the document ID is not metadata.
pub(super) fn indexed_field(name: &str) -> bool {
    !matches!(
        name,
        "kind" | "labels" | "space_id" | crate::SEARCH_DOCUMENT_ID_FIELD
    ) && !name.starts_with("metadata.")
}

fn invalid(segment_id: u64, reason: &str) -> HawDBError {
    HawDBError::Storage(format!(
        "search segment {segment_id} metadata postings {reason}"
    ))
}

fn checked_u32(value: usize) -> Result<u32> {
    u32::try_from(value)
        .map_err(|_| HawDBError::Storage("search metadata postings value exceeds u32".into()))
}

#[cfg(test)]
thread_local! {
    static LEGACY_LAYOUT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static READ_DISABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static DISTINCT_CAP: std::cell::Cell<usize> = const { std::cell::Cell::new(MAX_DISTINCT_VALUES) };
}

/// Test-only: segments hold two documents in tests, below the real cap.
#[cfg(test)]
pub(super) fn set_distinct_cap(cap: usize) {
    DISTINCT_CAP.with(|value| value.set(cap));
}

fn distinct_cap() -> usize {
    #[cfg(test)]
    return DISTINCT_CAP.with(std::cell::Cell::get);
    #[cfg(not(test))]
    MAX_DISTINCT_VALUES
}

/// Test-only: write layout V2 without postings, as earlier writers did.
#[cfg(test)]
pub(super) fn write_legacy_layout(enabled: bool) {
    LEGACY_LAYOUT.with(|value| value.set(enabled));
}

/// Test-only: build candidate sets with the metadata scan only.
#[cfg(test)]
pub(super) fn disable_read(disabled: bool) {
    READ_DISABLED.with(|value| value.set(disabled));
}

pub(super) fn write_enabled() -> bool {
    #[cfg(test)]
    if LEGACY_LAYOUT.with(std::cell::Cell::get) {
        return false;
    }
    true
}

pub(super) fn layout_format() -> &'static str {
    if write_enabled() {
        OUT_OF_CORE_LAYOUT_FORMAT
    } else {
        POSTINGLESS_OUT_OF_CORE_LAYOUT_FORMAT
    }
}

fn read_enabled() -> bool {
    #[cfg(test)]
    if READ_DISABLED.with(std::cell::Cell::get) {
        return false;
    }
    true
}

/// Accounts encoder scratch and the retained payload separately.
#[derive(Clone, Copy)]
pub(super) struct EncodeAdmission<'a> {
    pub(super) scratch: &'a QueryMemoryAccount,
    pub(super) payload: &'a QueryMemoryAccount,
}

pub(super) struct Encoded {
    pub(super) payload: Vec<u8>,
    _memory: Option<QueryMemoryLease>,
}

struct Entry<'a> {
    field: &'a str,
    value: String,
    document: u32,
}

/// The distinct values of one field's sorted entries, or `None` when the
/// field is not indexed in this segment.
fn indexed_values(group: &[Entry<'_>], cap: usize) -> Option<usize> {
    let distinct = 1 + group
        .windows(2)
        .filter(|pair| pair[0].value != pair[1].value)
        .count();
    (distinct <= cap
        && group
            .iter()
            .all(|entry| entry.value.len() <= MAX_VALUE_BYTES))
    .then_some(distinct)
}

/// Encodes the postings of one segment's ordered documents, or `None` when
/// the payload would exceed `max_bytes` (the segment then keeps the scan).
pub(super) fn encode<D: HeaderSource>(
    documents: &[D],
    admission: Option<EncodeAdmission<'_>>,
    max_bytes: u64,
    mut checkpoint: impl FnMut() -> Result<()>,
) -> Result<Option<Encoded>> {
    checkpoint()?;
    let cap = distinct_cap();
    let count = checked_u32(documents.len())?;
    let mut entry_count = 0usize;
    let mut id_bytes = 0usize;
    for (index, document) in documents.iter().enumerate() {
        let header = document.header();
        if index != 0 && documents[index - 1].header().id >= header.id {
            return Err(HawDBError::Storage(
                "search metadata postings document IDs are not strictly increasing".into(),
            ));
        }
        id_bytes = add(id_bytes, header.id.len())?;
        entry_count = add(
            entry_count,
            header
                .metadata
                .keys()
                .filter(|key| indexed_field(key))
                .count(),
        )?;
    }
    checked_u32(id_bytes)?;
    let mut scratch = admission
        .map(|admission| admission.scratch.reserve(0))
        .transpose()?;
    let mut entries = Vec::new();
    match &mut scratch {
        Some(lease) => reserve_capacity(&mut entries, entry_count, lease)?,
        None => entries.reserve_exact(entry_count),
    }
    for (index, document) in documents.iter().enumerate() {
        if index.is_multiple_of(64) {
            checkpoint()?;
        }
        let header = document.header();
        for (field, value) in header.metadata {
            if !indexed_field(field) {
                continue;
            }
            // Lowercasing expands UTF-8 by at most 3/2 and `to_lowercase`
            // at most doubles its capacity: admit three times the input.
            let trimmed = value.trim();
            let admitted = mul(trimmed.len(), 3)?;
            if let Some(lease) = &mut scratch {
                lease.grow(admitted)?;
            }
            let value = crate::normalize_metadata_filter_value(trimmed);
            if let Some(lease) = &mut scratch {
                if value.capacity() > admitted {
                    return Err(HawDBError::Execution(
                        "search metadata postings value exceeded admission".into(),
                    ));
                }
                lease.shrink(admitted - value.capacity());
            }
            entries.push(Entry {
                field,
                value,
                document: index as u32,
            });
        }
    }
    checkpoint()?;
    entries.sort_unstable_by(|left, right| {
        (left.field, &left.value, left.document).cmp(&(right.field, &right.value, right.document))
    });
    checkpoint()?;

    // Size the payload, deciding per field whether it is indexed.
    let mut length = add(
        add(FIXED_BYTES, mul(documents.len(), 4)?)?,
        add(id_bytes, documents.len())?,
    )?;
    let mut field_count = 0usize;
    let mut start = 0usize;
    while start < entries.len() {
        let field = entries[start].field;
        let end = start + entries[start..].partition_point(|entry| entry.field == field);
        field_count += 1;
        length = add(length, add(8, field.len())?)?;
        if let Some(distinct) = indexed_values(&entries[start..end], cap) {
            let value_bytes = entries[start..end]
                .iter()
                .enumerate()
                .filter(|(offset, entry)| {
                    *offset == 0 || entries[start + offset - 1].value != entry.value
                })
                .try_fold(0usize, |total, (_, entry)| {
                    total.checked_add(entry.value.len())
                })
                .ok_or_else(|| HawDBError::Storage("search metadata postings overflow".into()))?;
            length = add(
                length,
                add(add(mul(distinct, 8)?, value_bytes)?, mul(end - start, 4)?)?,
            )?;
        }
        start = end;
    }
    if length as u64 > max_bytes {
        return Ok(None);
    }

    let mut memory = admission
        .map(|admission| admission.payload.reserve(0))
        .transpose()?;
    let mut payload = Vec::new();
    match &mut memory {
        Some(lease) => reserve_capacity(&mut payload, length, lease)?,
        None => payload.reserve_exact(length),
    }
    payload.extend_from_slice(MAGIC);
    payload.extend_from_slice(&count.to_le_bytes());
    payload.extend_from_slice(&checked_u32(field_count)?.to_le_bytes());
    let mut end = 0usize;
    for document in documents {
        end += document.header().id.len();
        payload.extend_from_slice(&checked_u32(end)?.to_le_bytes());
    }
    for document in documents {
        payload.extend_from_slice(document.header().id.as_bytes());
    }
    for document in documents {
        payload.push(u8::from(document.header().embedding.is_some()));
    }
    let mut start = 0usize;
    while start < entries.len() {
        checkpoint()?;
        let field = entries[start].field;
        let end = start + entries[start..].partition_point(|entry| entry.field == field);
        let group = &entries[start..end];
        payload.extend_from_slice(&checked_u32(field.len())?.to_le_bytes());
        payload.extend_from_slice(field.as_bytes());
        let Some(distinct) = indexed_values(group, cap) else {
            payload.extend_from_slice(&0u32.to_le_bytes());
            start = end;
            continue;
        };
        payload.extend_from_slice(&checked_u32(distinct)?.to_le_bytes());
        let mut value_start = 0usize;
        while value_start < group.len() {
            let value = &group[value_start].value;
            let value_end =
                value_start + group[value_start..].partition_point(|entry| &entry.value == value);
            payload.extend_from_slice(&checked_u32(value.len())?.to_le_bytes());
            payload.extend_from_slice(value.as_bytes());
            payload.extend_from_slice(&checked_u32(value_end - value_start)?.to_le_bytes());
            for entry in &group[value_start..value_end] {
                payload.extend_from_slice(&entry.document.to_le_bytes());
            }
            value_start = value_end;
        }
        start = end;
    }
    if payload.len() != length {
        return Err(HawDBError::Storage(
            "search metadata postings encoding did not match its admitted length".into(),
        ));
    }
    drop(entries);
    drop(scratch);
    Ok(Some(Encoded {
        payload,
        _memory: memory,
    }))
}

/// The `Eq`/`In` predicates of a candidate filter, values normalized.
pub(super) struct Query {
    predicates: Vec<(String, BTreeSet<String>)>,
    _memory: Option<QueryMemoryLease>,
}

impl Query {
    /// `None` unless every predicate is an `Eq` or `In` on an indexed field.
    pub(super) fn new(
        predicates: &hawdb_optimizer_predicate::SearchPredicateSet,
        memory: Option<&QueryMemoryAccount>,
    ) -> Result<Option<Self>> {
        use hawdb_optimizer_predicate::SearchPredicateOp;
        if !read_enabled()
            || predicates.is_empty()
            || predicates.is_unsatisfiable()
            || predicates.predicates().len() > MAX_PREDICATES
            || predicates.predicates().iter().any(|predicate| {
                !indexed_field(predicate.field().name())
                    || !matches!(
                        predicate.op(),
                        SearchPredicateOp::Eq(_) | SearchPredicateOp::In(_)
                    )
            })
        {
            return Ok(None);
        }
        let predicates = predicates.predicates();
        let mut bytes = mul(predicates.len(), size_of::<(String, BTreeSet<String>)>())?;
        for predicate in predicates {
            bytes = add(bytes, predicate.field().name().len())?;
            for_each_expected(predicate.op(), |value| {
                bytes = add(
                    bytes,
                    add(
                        mul(value.trim().len(), 3)?,
                        crate::build_memory::SET_ENTRY_BYTES,
                    )?,
                )?;
                Ok(())
            })?;
        }
        let memory = memory.map(|memory| memory.reserve(bytes)).transpose()?;
        let mut normalized = Vec::with_capacity(predicates.len());
        for predicate in predicates {
            let mut values = BTreeSet::new();
            for_each_expected(predicate.op(), |value| {
                values.insert(crate::normalize_metadata_filter_value(value));
                Ok(())
            })?;
            normalized.push((predicate.field().name().to_string(), values));
        }
        let predicates = normalized;
        Ok(Some(Self {
            predicates,
            _memory: memory,
        }))
    }
}

fn for_each_expected(
    op: &hawdb_optimizer_predicate::SearchPredicateOp,
    mut visit: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    use hawdb_optimizer_predicate::SearchPredicateOp;
    match op {
        SearchPredicateOp::Eq(value) => visit(value.as_str()),
        SearchPredicateOp::In(values) => values.iter().try_for_each(|value| visit(value.as_str())),
        _ => Err(HawDBError::Execution(
            "search metadata postings support only Eq and In predicates".into(),
        )),
    }
}

/// A validated postings payload of one segment.
pub(super) struct Segment<'a> {
    bytes: &'a [u8],
    segment_id: u64,
    count: usize,
    ids_at: usize,
    flags_at: usize,
    fields_at: usize,
    field_count: usize,
}

fn u32_at(bytes: &[u8], offset: usize) -> Option<usize> {
    let end = offset.checked_add(4)?;
    Some(u32::from_le_bytes(bytes.get(offset..end)?.try_into().ok()?) as usize)
}

impl<'a> Segment<'a> {
    /// Validates the document table against the descriptor and layout.
    pub(super) fn parse(
        bytes: &'a [u8],
        segment: &SearchSegmentDescriptorEntry,
        layout: &SearchOutOfCoreSegmentLayout,
    ) -> Result<Self> {
        let segment_id = segment.segment_id;
        let truncated = || invalid(segment_id, "are truncated");
        if bytes.get(..8) != Some(MAGIC.as_slice()) {
            return Err(invalid(segment_id, "have an invalid header"));
        }
        let count = u32_at(bytes, 8).ok_or_else(truncated)?;
        let field_count = u32_at(bytes, 12).ok_or_else(truncated)?;
        if count != segment.document_count
            || layout
                .postings
                .is_none_or(|range| range.entry_count != count)
            || count == 0
        {
            return Err(invalid(
                segment_id,
                "document count differs from its segment",
            ));
        }
        let ids_at = count
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(FIXED_BYTES))
            .ok_or_else(truncated)?;
        let id_bytes = u32_at(bytes, ids_at - 4).ok_or_else(truncated)?;
        let flags_at = ids_at.checked_add(id_bytes).ok_or_else(truncated)?;
        let fields_at = flags_at.checked_add(count).ok_or_else(truncated)?;
        if fields_at > bytes.len() {
            return Err(truncated());
        }
        let parsed = Self {
            bytes,
            segment_id,
            count,
            ids_at,
            flags_at,
            fields_at,
            field_count,
        };
        let mut previous: Option<&str> = None;
        let mut start = 0usize;
        for index in 0..count {
            let end = u32_at(bytes, FIXED_BYTES + index * 4).ok_or_else(truncated)?;
            if end < start || end > id_bytes {
                return Err(invalid(segment_id, "have invalid document ID offsets"));
            }
            let id = std::str::from_utf8(&bytes[ids_at + start..ids_at + end])
                .map_err(|_| invalid(segment_id, "have a non-UTF-8 document ID"))?;
            if previous.is_some_and(|previous| previous >= id)
                || (index == 0 && id != segment.first_document_id)
                || (index + 1 == count && id != segment.last_document_id)
            {
                return Err(invalid(
                    segment_id,
                    "document IDs differ from the segment bounds or order",
                ));
            }
            previous = Some(id);
            start = end;
        }
        let mut vectors = 0usize;
        for flag in &bytes[flags_at..fields_at] {
            match flag {
                0 => {}
                1 => vectors += 1,
                _ => return Err(invalid(segment_id, "have an invalid vector flag")),
            }
        }
        if vectors != layout.vectors.entry_count {
            return Err(invalid(segment_id, "vector count differs from its layout"));
        }
        Ok(parsed)
    }

    fn id(&self, index: usize) -> &'a str {
        let start = match index {
            0 => 0,
            _ => u32_at(self.bytes, FIXED_BYTES + (index - 1) * 4).expect("validated offset"),
        };
        let end = u32_at(self.bytes, FIXED_BYTES + index * 4).expect("validated offset");
        std::str::from_utf8(&self.bytes[self.ids_at + start..self.ids_at + end])
            .expect("validated ID")
    }

    /// Ascending indexes of the documents satisfying every predicate, or
    /// `None` when a predicate's field is not indexed in this segment.
    /// Every field record is validated, whether or not a predicate reads it.
    fn select(
        &self,
        query: &Query,
        memory: Option<&QueryMemoryAccount>,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<(Vec<usize>, Option<QueryMemoryLease>)>> {
        let segment_id = self.segment_id;
        let truncated = || invalid(segment_id, "are truncated");
        let bytes = self.bytes;
        let mask_bytes = mul(self.count, size_of::<u64>())?;
        let mut mask_memory = memory
            .map(|memory| memory.reserve(mask_bytes))
            .transpose()?;
        let mut masks = vec![0u64; self.count];
        let mut found = 0u64;
        let mut capped = false;
        let mut offset = self.fields_at;
        let mut previous_field: Option<&[u8]> = None;
        for _ in 0..self.field_count {
            checkpoint()?;
            let length = u32_at(bytes, offset).ok_or_else(truncated)?;
            let name = bytes
                .get(offset + 4..offset + 4 + length)
                .ok_or_else(truncated)?;
            if previous_field.is_some_and(|previous| previous >= name) {
                return Err(invalid(segment_id, "fields are not strictly ordered"));
            }
            previous_field = Some(name);
            let name = std::str::from_utf8(name)
                .map_err(|_| invalid(segment_id, "have a non-UTF-8 field"))?;
            offset += 4 + length;
            let value_count = u32_at(bytes, offset).ok_or_else(truncated)?;
            offset += 4;
            let mut readers = 0u64;
            for (bit, (field, _)) in query.predicates.iter().enumerate() {
                if field == name {
                    readers |= 1 << bit;
                }
            }
            found |= readers;
            if value_count == 0 {
                capped |= readers != 0;
                continue;
            }
            let mut previous_value: Option<&[u8]> = None;
            let mut field_entries = 0usize;
            for _ in 0..value_count {
                let length = u32_at(bytes, offset).ok_or_else(truncated)?;
                let value = bytes
                    .get(offset + 4..offset + 4 + length)
                    .ok_or_else(truncated)?;
                if previous_value.is_some_and(|previous| previous >= value) {
                    return Err(invalid(segment_id, "values are not strictly ordered"));
                }
                previous_value = Some(value);
                let value = std::str::from_utf8(value)
                    .map_err(|_| invalid(segment_id, "have a non-UTF-8 value"))?;
                offset += 4 + length;
                let entries = u32_at(bytes, offset).ok_or_else(truncated)?;
                offset += 4;
                field_entries = field_entries.saturating_add(entries);
                if entries == 0 || field_entries > self.count {
                    return Err(invalid(segment_id, "have an invalid entry count"));
                }
                let mut matches = 0u64;
                if readers != 0 {
                    for (bit, (_, values)) in query.predicates.iter().enumerate() {
                        if readers & (1 << bit) != 0 && values.contains(value) {
                            matches |= 1 << bit;
                        }
                    }
                }
                let mut previous_document = None;
                for _ in 0..entries {
                    let document = u32_at(bytes, offset).ok_or_else(truncated)?;
                    offset += 4;
                    if document >= self.count
                        || previous_document.is_some_and(|previous| previous >= document)
                    {
                        return Err(invalid(segment_id, "have an invalid document index"));
                    }
                    previous_document = Some(document);
                    masks[document] |= matches;
                }
            }
        }
        if offset != bytes.len() {
            return Err(invalid(segment_id, "have trailing bytes"));
        }
        if capped {
            return Ok(None);
        }
        let all = match query.predicates.len() {
            MAX_PREDICATES => u64::MAX,
            count => (1u64 << count) - 1,
        };
        let mut selected = Vec::new();
        if found == all {
            let count = masks.iter().filter(|mask| **mask == all).count();
            match &mut mask_memory {
                Some(lease) => reserve_capacity(&mut selected, count, lease)?,
                None => selected.reserve_exact(count),
            }
            selected.extend((0..self.count).filter(|index| masks[*index] == all));
        }
        // A predicate whose field no document holds matches nothing.
        drop(masks);
        if let Some(lease) = &mut mask_memory {
            lease.shrink(mask_bytes);
        }
        Ok(Some((selected, mask_memory)))
    }
}

impl SearchOutOfCoreReader {
    /// Visits, in segment order, the documents of `segment` that satisfy
    /// `query` according to its postings. Returns `false` without visiting
    /// when the segment has no postings or a predicate's field is not
    /// indexed in it; the caller then scans its metadata.
    pub(super) fn visit_postings_segment(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        query: &Query,
        metrics: &mut SearchOutOfCoreMetrics,
        context: Option<QueryContext<'_>>,
        mut visit: impl FnMut(&SearchMetadataDocument) -> Result<()>,
    ) -> Result<bool> {
        let layout = Self::artifact_layout_range(artifact, segment.segment_id)?;
        let Some(range) = layout.postings else {
            return Ok(false);
        };
        let bytes = self.read_postings_range(artifact, segment, range, metrics, context)?;
        let postings = Segment::parse(&bytes, segment, layout)?;
        let mut checkpoint = || context.map_or(Ok(()), |context| context.checkpoint());
        let Some((selected, _memory)) = postings.select(
            query,
            context.map(|context| context.memory),
            &mut checkpoint,
        )?
        else {
            return Ok(false);
        };
        let mut vectors = 0u64;
        let mut flag = 0usize;
        for (visited, index) in selected.into_iter().enumerate() {
            if visited.is_multiple_of(64) {
                checkpoint()?;
            }
            vectors += postings.bytes[postings.flags_at + flag..postings.flags_at + index]
                .iter()
                .map(|flag| u64::from(*flag))
                .sum::<u64>();
            flag = index;
            let vector_ordinal = match postings.bytes[postings.flags_at + index] {
                1 => Some(
                    layout
                        .vector_ordinal_base
                        .checked_add(vectors)
                        .ok_or_else(|| invalid(segment.segment_id, "vector ordinal overflows"))?,
                ),
                _ => None,
            };
            let id = postings.id(index);
            let _memory = context
                .map(|context| {
                    context
                        .memory
                        .reserve(add(size_of::<SearchMetadataDocument>(), id.len())?)
                })
                .transpose()?;
            visit(&SearchMetadataDocument {
                id: id.to_string(),
                vector_ordinal,
                metadata: BTreeMap::new(),
            })?;
        }
        Ok(true)
    }

    fn read_postings_range(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        range: SearchOutOfCoreRange,
        metrics: &mut SearchOutOfCoreMetrics,
        context: Option<QueryContext<'_>>,
    ) -> Result<PostingsBytes> {
        if range.length > self.config.max_uncompressed_segment_bytes.get() {
            return Err(invalid(
                segment.segment_id,
                "exceed the uncompressed segment budget",
            ));
        }
        let Some(context) = context else {
            let values = read_out_of_core_payload_range(
                &artifact.metadata_payload,
                range,
                segment.segment_id,
                "postings",
                metrics,
            )?;
            metrics.metadata_segment_bytes_read = metrics
                .metadata_segment_bytes_read
                .saturating_add(range.length);
            return Ok(PostingsBytes {
                values,
                _memory: None,
            });
        };
        let length = usize::try_from(range.length).map_err(|_| {
            invalid(
                segment.segment_id,
                "length exceeds the platform address space",
            )
        })?;
        let mut memory = context.memory.reserve(0)?;
        let mut values = Vec::new();
        reserve_capacity(&mut values, length, &mut memory)?;
        values.resize(length, 0);
        let mut input = crate::build_memory::read::QueryFileRange {
            file: &artifact.metadata_payload,
            expected_length: Some(artifact.metadata_payload_length),
            offset: range.offset,
            remaining: range.length,
            context,
            require_execution: true,
        };
        input
            .read_exact(&mut values)
            .map_err(crate::build_memory::read::io_error)?;
        metrics.segment_range_reads = metrics.segment_range_reads.saturating_add(1);
        metrics.segment_bytes_read = metrics.segment_bytes_read.saturating_add(range.length);
        metrics.metadata_segment_bytes_read = metrics
            .metadata_segment_bytes_read
            .saturating_add(range.length);
        let checksum = checksum_bytes(&values);
        if checksum != range.checksum {
            return Err(HawDBError::Storage(format!(
                "search segment {} postings payload checksum mismatch: expected {}, got {checksum}",
                segment.segment_id, range.checksum
            )));
        }
        Ok(PostingsBytes {
            values,
            _memory: Some(memory),
        })
    }
}

/// Payload bytes die before the reservation that admitted them.
pub(super) struct PostingsBytes {
    values: Vec<u8>,
    _memory: Option<QueryMemoryLease>,
}

impl std::ops::Deref for PostingsBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.values
    }
}
