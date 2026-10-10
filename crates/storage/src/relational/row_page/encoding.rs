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

//! One row-page/row grammar, specialized for ordinary and cooperative work.
//! Work hooks preserve the existing unit, comparison, copy and integrity paths.

use super::*;
use crate::background::{CheckpointWorkContext, CheckpointWorkUnit};

pub(super) trait EncodeUnit {
    fn finish(self);
}

pub(super) trait EncodeWork {
    type Unit: EncodeUnit;
    fn start_unit(&self) -> Result<Self::Unit, RelationalRowPageError>;
    fn checkpoint(&self) -> Result<(), RelationalRowPageError>;
    fn ordered_key(&self, key: &RelationalKey) -> Result<Vec<u8>, RelationalRowPageError>;
    fn compare(
        &self,
        left: &[u8],
        right: &[u8],
    ) -> Result<std::cmp::Ordering, RelationalRowPageError>;
    fn append(&self, output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), RelationalRowPageError>;
    fn integrity(&self, encoded: &mut [u8]) -> Result<(), RelationalRowPageError>;
}

pub(super) struct NoWork;
pub(super) struct NoWorkUnit;
impl EncodeUnit for NoWorkUnit {
    #[inline]
    fn finish(self) {}
}
impl EncodeWork for NoWork {
    type Unit = NoWorkUnit;
    #[inline]
    fn start_unit(&self) -> Result<NoWorkUnit, RelationalRowPageError> {
        Ok(NoWorkUnit)
    }
    #[inline]
    fn checkpoint(&self) -> Result<(), RelationalRowPageError> {
        Ok(())
    }
    fn ordered_key(&self, key: &RelationalKey) -> Result<Vec<u8>, RelationalRowPageError> {
        encode_ordered_relational_key(key)
            .map_err(|error| RelationalRowPageError::Admission(error.to_string()))
    }
    fn compare(
        &self,
        left: &[u8],
        right: &[u8],
    ) -> Result<std::cmp::Ordering, RelationalRowPageError> {
        Ok(left.cmp(right))
    }
    fn append(&self, output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), RelationalRowPageError> {
        output.extend_from_slice(bytes);
        Ok(())
    }
    fn integrity(&self, encoded: &mut [u8]) -> Result<(), RelationalRowPageError> {
        write_integrity(encoded);
        Ok(())
    }
}

pub(super) struct ControlledWork<'a>(pub(super) &'a CheckpointWorkContext);
impl EncodeUnit for CheckpointWorkUnit {
    fn finish(self) {
        CheckpointWorkUnit::finish(self);
    }
}
impl EncodeWork for ControlledWork<'_> {
    type Unit = CheckpointWorkUnit;
    fn start_unit(&self) -> Result<Self::Unit, RelationalRowPageError> {
        self.0.start_unit().map_err(checkpoint::work_error)
    }
    fn checkpoint(&self) -> Result<(), RelationalRowPageError> {
        self.0.checkpoint().map_err(checkpoint::work_error)
    }
    fn ordered_key(&self, key: &RelationalKey) -> Result<Vec<u8>, RelationalRowPageError> {
        checkpoint::ordered_key(key, self.0)
    }
    fn compare(
        &self,
        left: &[u8],
        right: &[u8],
    ) -> Result<std::cmp::Ordering, RelationalRowPageError> {
        checkpoint::compare(left, right, self.0)
    }
    fn append(&self, output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), RelationalRowPageError> {
        checkpoint::append(output, bytes, self.0)
    }
    fn integrity(&self, encoded: &mut [u8]) -> Result<(), RelationalRowPageError> {
        let work = self;
        let mut hasher = IntegrityHasher::new();
        for region in [
            &encoded[..INTEGRITY_PREFIX_BYTES],
            &encoded[ROW_PAGE_HEADER_BYTES..],
        ] {
            for block in region.chunks(64 * 1024) {
                let unit = work.start_unit()?;
                hasher.update(block);
                unit.finish();
            }
        }
        let unit = work.start_unit()?;
        let digest = hasher.finish();
        encoded[104..108].copy_from_slice(&digest.crc32c.get().to_le_bytes());
        encoded[108..140].copy_from_slice(digest.sha256.as_bytes());
        unit.finish();
        Ok(())
    }
}

pub(super) fn encode<W: EncodeWork>(
    page: &ImmutableRelationalRowPage,
    limits: RelationalRowPageLimits,
    work: &W,
) -> Result<Vec<u8>, RelationalRowPageError> {
    let unit = work.start_unit()?;
    validate_page_for_encode(page, limits)?;

    let mut slots = Vec::with_capacity(page.rows.len());
    let mut key_payload = Vec::new();
    let mut row_payload = Vec::new();
    let mut lower_bound: Option<Vec<u8>> = None;
    let mut previous_key: Option<Vec<u8>> = None;
    unit.finish();
    for entry in &page.rows {
        let key = work.ordered_key(&entry.primary_key)?;
        let unit = work.start_unit()?;
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
            .map(|previous| work.compare(previous, &key))
            .transpose()?
            .is_some_and(|ordering| ordering != std::cmp::Ordering::Less)
        {
            return Err(RelationalRowPageError::Admission(
                "row primary keys are not strictly increasing".to_string(),
            ));
        }

        let row = encode_row(&entry.row, page.column_count, limits, work)?;
        let unit = work.start_unit()?;
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
        work.append(&mut key_payload, &key)?;
        work.append(&mut row_payload, &row)?;
        let unit = work.start_unit()?;
        slots.push(slot);
        unit.finish();
        if lower_bound.is_none() {
            let mut lower = Vec::new();
            work.append(&mut lower, &key)?;
            lower_bound = Some(lower);
        }
        previous_key = Some(key);
    }

    let unit = work.start_unit()?;
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
    work.append(&mut encoded, &lower_bound)?;
    work.append(&mut encoded, &upper_bound)?;
    for slot in slots {
        let unit = work.start_unit()?;
        slot.encode(&mut encoded);
        unit.finish();
    }
    work.append(&mut encoded, &key_payload)?;
    work.append(&mut encoded, &row_payload)?;
    work.integrity(&mut encoded)?;
    work.checkpoint()?;
    Ok(encoded)
}

pub(super) fn encode_row<W: EncodeWork>(
    row: &RelationalRow,
    column_count: usize,
    limits: RelationalRowPageLimits,
    work: &W,
) -> Result<Vec<u8>, RelationalRowPageError> {
    let unit = work.start_unit()?;
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
        let unit = work.start_unit()?;
        let offset = u32_len(payload.len(), "value offset")?;
        let before = payload.len();
        unit.finish();
        value::encode_value_with_work(&mut payload, value, limits, work)?;
        let unit = work.start_unit()?;
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
    let unit = work.start_unit()?;
    let mut encoded = Vec::with_capacity(header_len + payload.len());
    encoded.extend_from_slice(&u32_len(column_count, "row value count")?.to_le_bytes());
    unit.finish();
    for (offset, length) in slots {
        let unit = work.start_unit()?;
        encoded.extend_from_slice(&offset.to_le_bytes());
        encoded.extend_from_slice(&length.to_le_bytes());
        unit.finish();
    }
    work.append(&mut encoded, &payload)?;
    work.checkpoint()?;
    Ok(encoded)
}
