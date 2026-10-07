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

//! Private page encoding under checkpoint work control. Ordinary encoding stays
//! an independent wire/error reference; hard resources remain a separate gate.

use super::*;
use crate::background::{CheckpointWorkContext, CheckpointWorkError};
use crate::relational::ordered_key::{
    encode_ordered_relational_key_with_work_context, CheckpointKeyEncodeError,
};

pub(super) fn work_error(error: CheckpointWorkError) -> RelationalRowPageError {
    RelationalRowPageError::Admission(error.to_string())
}

pub(super) fn ordered_key(
    key: &RelationalKey,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalRowPageError> {
    encode_ordered_relational_key_with_work_context(key, work).map_err(|error| match error {
        CheckpointKeyEncodeError::Key(crate::relational::RelationalError::Corruption(message)) => {
            RelationalRowPageError::Admission(message)
        }
        CheckpointKeyEncodeError::Key(error) => {
            RelationalRowPageError::Admission(error.to_string())
        }
        CheckpointKeyEncodeError::Work(error) => work_error(error),
    })
}

fn compare(
    left: &[u8],
    right: &[u8],
    work: &CheckpointWorkContext,
) -> Result<std::cmp::Ordering, RelationalRowPageError> {
    for (left, right) in left.chunks(64 * 1024).zip(right.chunks(64 * 1024)) {
        let unit = work.start_unit().map_err(work_error)?;
        let result = left.cmp(right);
        unit.finish();
        if result != std::cmp::Ordering::Equal {
            return Ok(result);
        }
    }
    let unit = work.start_unit().map_err(work_error)?;
    let result = left.len().cmp(&right.len());
    unit.finish();
    Ok(result)
}

pub(super) fn append(
    output: &mut Vec<u8>,
    bytes: &[u8],
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPageError> {
    // Reserve before copies so block writes never recopy a growing complete
    // buffer. The allocator and actual capacities still require hard admission.
    let unit = work.start_unit().map_err(work_error)?;
    output.reserve(bytes.len());
    unit.finish();
    for block in bytes.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        output.extend_from_slice(block);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

pub(super) fn encode(
    page: &ImmutableRelationalRowPage,
    limits: RelationalRowPageLimits,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalRowPageError> {
    let unit = work.start_unit().map_err(work_error)?;
    validate_page_for_encode(page, limits)?;

    let mut slots = Vec::with_capacity(page.rows.len());
    let mut key_payload = Vec::new();
    let mut row_payload = Vec::new();
    let mut lower_bound: Option<Vec<u8>> = None;
    let mut previous_key: Option<Vec<u8>> = None;
    unit.finish();
    for entry in &page.rows {
        let key = ordered_key(&entry.primary_key, work)?;
        let unit = work.start_unit().map_err(work_error)?;
        if key.len() > limits.max_key_bytes.get() {
            return Err(RelationalRowPageError::Admission(format!(
                "primary key contains {} bytes, exceeding limit {}",
                key.len(),
                limits.max_key_bytes
            )));
        }
        unit.finish();
        if previous_key
            .as_ref()
            .map(|previous| compare(previous, &key, work))
            .transpose()?
            .is_some_and(|ordering| ordering != std::cmp::Ordering::Less)
        {
            return Err(RelationalRowPageError::Admission(
                "row primary keys are not strictly increasing".to_string(),
            ));
        }

        let row = encode_row(&entry.row, page.column_count, limits, work)?;
        let unit = work.start_unit().map_err(work_error)?;
        if row.len() > limits.max_row_bytes.get() {
            return Err(RelationalRowPageError::Admission(format!(
                "encoded row contains {} bytes, exceeding limit {}",
                row.len(),
                limits.max_row_bytes
            )));
        }
        let slot = RowSlot {
            key_offset: u32_len(key_payload.len(), "key offset")?,
            key_len: u32_len(key.len(), "key length")?,
            row_offset: u32_len(row_payload.len(), "row offset")?,
            row_len: u32_len(row.len(), "row length")?,
        };
        let next_key_bytes = key_payload.len().checked_add(key.len()).ok_or_else(|| {
            RelationalRowPageError::Admission("key payload length overflow".to_string())
        })?;
        let next_row_bytes = row_payload.len().checked_add(row.len()).ok_or_else(|| {
            RelationalRowPageError::Admission("row payload length overflow".to_string())
        })?;
        let directory_bytes = page.rows.len().checked_mul(ROW_SLOT_BYTES).ok_or_else(|| {
            RelationalRowPageError::Admission("row slot directory length overflow".to_string())
        })?;
        let lower_len = lower_bound.as_ref().map_or(key.len(), Vec::len);
        let projected_bytes = ROW_PAGE_HEADER_BYTES
            .checked_add(lower_len)
            .and_then(|bytes| bytes.checked_add(key.len()))
            .and_then(|bytes| bytes.checked_add(directory_bytes))
            .and_then(|bytes| bytes.checked_add(next_key_bytes))
            .and_then(|bytes| bytes.checked_add(next_row_bytes))
            .ok_or_else(|| {
                RelationalRowPageError::Admission("encoded page size overflow".to_string())
            })?;
        if projected_bytes > limits.max_page_bytes.get() {
            return Err(RelationalRowPageError::Admission(format!(
                "encoded page would contain {projected_bytes} bytes, exceeding limit {}",
                limits.max_page_bytes
            )));
        }
        unit.finish();
        append(&mut key_payload, &key, work)?;
        append(&mut row_payload, &row, work)?;
        let unit = work.start_unit().map_err(work_error)?;
        slots.push(slot);
        unit.finish();
        if lower_bound.is_none() {
            let mut lower = Vec::new();
            append(&mut lower, &key, work)?;
            lower_bound = Some(lower);
        }
        previous_key = Some(key);
    }

    let unit = work.start_unit().map_err(work_error)?;
    let lower_bound = lower_bound.expect("validated row page has a lower key bound");
    let upper_bound = previous_key.expect("validated row page has an upper key bound");
    let directory_len = slots.len().checked_mul(ROW_SLOT_BYTES).ok_or_else(|| {
        RelationalRowPageError::Admission("row slot directory length overflow".to_string())
    })?;
    let payload_len = lower_bound
        .len()
        .checked_add(upper_bound.len())
        .and_then(|bytes| bytes.checked_add(directory_len))
        .and_then(|bytes| bytes.checked_add(key_payload.len()))
        .and_then(|bytes| bytes.checked_add(row_payload.len()))
        .ok_or_else(|| {
            RelationalRowPageError::Admission("encoded page payload overflow".to_string())
        })?;
    let total_len = ROW_PAGE_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(|| {
            RelationalRowPageError::Admission("encoded page length overflow".to_string())
        })?;
    if total_len > limits.max_page_bytes.get() {
        return Err(RelationalRowPageError::Admission(format!(
            "encoded page contains {total_len} bytes, exceeding limit {}",
            limits.max_page_bytes
        )));
    }

    let mut encoded = Vec::with_capacity(total_len);
    encoded.extend_from_slice(ROW_PAGE_MAGIC);
    encoded.extend_from_slice(&ROW_PAGE_VERSION.to_le_bytes());
    encoded.extend_from_slice(&0u16.to_le_bytes());
    encoded.extend_from_slice(&page.generation.to_le_bytes());
    encoded.extend_from_slice(&page.source_commit_epoch.to_le_bytes());
    encoded.extend_from_slice(&page.page_id.get().to_le_bytes());
    encoded.extend_from_slice(&u32_len(page.rows.len(), "row count")?.to_le_bytes());
    encoded.extend_from_slice(&u32_len(page.column_count, "column count")?.to_le_bytes());
    encoded.extend_from_slice(&u32_len(lower_bound.len(), "lower key length")?.to_le_bytes());
    encoded.extend_from_slice(&u32_len(upper_bound.len(), "upper key length")?.to_le_bytes());
    encoded.extend_from_slice(&u32_len(directory_len, "directory length")?.to_le_bytes());
    encoded.extend_from_slice(&u64_len(key_payload.len(), "key payload length")?.to_le_bytes());
    encoded.extend_from_slice(&u64_len(row_payload.len(), "row payload length")?.to_le_bytes());
    encoded.extend_from_slice(page.schema_digest.as_bytes());
    debug_assert_eq!(encoded.len(), INTEGRITY_PREFIX_BYTES);
    encoded.extend_from_slice(&[0; 4 + SHA256_BYTES]);
    debug_assert_eq!(encoded.len(), ROW_PAGE_HEADER_BYTES);
    unit.finish();
    append(&mut encoded, &lower_bound, work)?;
    append(&mut encoded, &upper_bound, work)?;
    for slot in slots {
        let unit = work.start_unit().map_err(work_error)?;
        slot.encode(&mut encoded);
        unit.finish();
    }
    append(&mut encoded, &key_payload, work)?;
    append(&mut encoded, &row_payload, work)?;
    let mut hasher = IntegrityHasher::new();
    for region in [
        &encoded[..INTEGRITY_PREFIX_BYTES],
        &encoded[ROW_PAGE_HEADER_BYTES..],
    ] {
        for block in region.chunks(64 * 1024) {
            let unit = work.start_unit().map_err(work_error)?;
            hasher.update(block);
            unit.finish();
        }
    }
    let unit = work.start_unit().map_err(work_error)?;
    let digest = hasher.finish();
    encoded[104..108].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded[108..140].copy_from_slice(digest.sha256.as_bytes());
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(encoded)
}

fn encode_row(
    row: &RelationalRow,
    column_count: usize,
    limits: RelationalRowPageLimits,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalRowPageError> {
    let unit = work.start_unit().map_err(work_error)?;
    if row.values().len() != column_count {
        return Err(RelationalRowPageError::Admission(format!(
            "row contains {} values, expected {column_count}",
            row.values().len()
        )));
    }
    let directory_len = column_count.checked_mul(VALUE_SLOT_BYTES).ok_or_else(|| {
        RelationalRowPageError::Admission("value slot directory length overflow".to_string())
    })?;
    let header_len = 4usize.checked_add(directory_len).ok_or_else(|| {
        RelationalRowPageError::Admission("encoded row header length overflow".to_string())
    })?;
    if header_len > limits.max_row_bytes.get() {
        return Err(RelationalRowPageError::Admission(format!(
            "row directory contains {header_len} bytes, exceeding row limit {}",
            limits.max_row_bytes
        )));
    }
    let mut slots = Vec::with_capacity(column_count);
    let mut payload = Vec::new();
    unit.finish();
    for value in row.values() {
        let unit = work.start_unit().map_err(work_error)?;
        let offset = u32_len(payload.len(), "value offset")?;
        let before = payload.len();
        unit.finish();
        encode_value(&mut payload, value, limits, work)?;
        let unit = work.start_unit().map_err(work_error)?;
        let length = payload.len().checked_sub(before).ok_or_else(|| {
            RelationalRowPageError::Admission("value length underflow".to_string())
        })?;
        slots.push((offset, u32_len(length, "value length")?));
        let projected_len = header_len.checked_add(payload.len()).ok_or_else(|| {
            RelationalRowPageError::Admission("encoded row length overflow".to_string())
        })?;
        if projected_len > limits.max_row_bytes.get() {
            return Err(RelationalRowPageError::Admission(format!(
                "encoded row would contain {projected_len} bytes, exceeding limit {}",
                limits.max_row_bytes
            )));
        }
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
    let mut encoded = Vec::with_capacity(header_len + payload.len());
    encoded.extend_from_slice(&u32_len(column_count, "row value count")?.to_le_bytes());
    unit.finish();
    for (offset, length) in slots {
        let unit = work.start_unit().map_err(work_error)?;
        encoded.extend_from_slice(&offset.to_le_bytes());
        encoded.extend_from_slice(&length.to_le_bytes());
        unit.finish();
    }
    append(&mut encoded, &payload, work)?;
    work.checkpoint().map_err(work_error)?;
    Ok(encoded)
}

fn encode_value(
    encoded: &mut Vec<u8>,
    value: &RelationalValue,
    limits: RelationalRowPageLimits,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPageError> {
    let unit = work.start_unit().map_err(work_error)?;
    let variable = match value {
        RelationalValue::Text(value) => Some((4, value.as_bytes(), "TEXT length")),
        RelationalValue::Bytea(value) => Some((5, value.as_slice(), "BYTEA length")),
        _ => None,
    };
    if let Some((tag, bytes, context)) = variable {
        value::validate_inline_value_len(bytes.len(), limits)?;
        encoded.push(tag);
        encoded.extend_from_slice(&u32_len(bytes.len(), context)?.to_le_bytes());
        unit.finish();
        append(encoded, bytes, work)?;
    } else {
        value::encode_value(encoded, value, limits)?;
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

#[cfg(test)]
mod tests;
