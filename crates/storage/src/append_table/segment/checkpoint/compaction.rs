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

//! Cooperative reads for private append compaction.

use super::*;
use crate::append_table::checkpoint::{CheckpointAppendRows, CompactionFailure as Failure};
use crate::append_table::segment;
use crate::background::CheckpointBytes;
use crate::relational::decode_relational_row_payload;
use std::io::{Cursor, Read};

impl AppendSegmentReader {
    pub(in crate::append_table) fn checkpoint_block_with_work_context(
        &self,
        descriptor: &AppendSegmentBlockDescriptor,
        max_payload_bytes: usize,
        work: &CheckpointWorkContext,
    ) -> Result<CheckpointAppendRows, Failure> {
        let start = self
            .payload_start
            .checked_add(segment::to_usize(
                descriptor.payload_offset,
                "block offset",
            )?)
            .ok_or_else(|| {
                AppendTableError::Corruption("append block offset overflow".to_string())
            })?;
        let len = segment::to_usize(descriptor.compressed_bytes, "compressed block length")?;
        let end = start.checked_add(len).ok_or_else(|| {
            AppendTableError::Corruption("append block length overflow".to_string())
        })?;
        let mut compressed = CheckpointBytes::new(len, work)?;
        let mut position = start;
        while position < end {
            let unit = work.start_unit()?;
            let wave = if matches!(self.source, AppendSegmentSource::File(_)) {
                work.io_wave()?
            } else {
                None
            };
            // Acquisition itself may observe or trigger cancellation. Recheck
            // before allocating or reading the admitted chunk.
            work.checkpoint()?;
            let chunk_len = (end - position).min(64 * 1024);
            let chunk_memory = work.reserve_memory(chunk_len)?;
            let chunk = self.source.read_range(position, chunk_len)?;
            if chunk.capacity() != chunk_len {
                return Err(work
                    .record_failure(crate::background::CheckpointWorkError::Allocation {
                        bytes: chunk.capacity() as u64,
                        reason: "append source read exceeds admitted chunk capacity".into(),
                    })
                    .into());
            }
            position += chunk_len;
            drop(wave);
            unit.finish();
            compressed.append(&chunk, work)?;
            drop(chunk);
            drop(chunk_memory);
        }
        let digest = work.integrity(&compressed)?;
        if digest.crc32c.get() != descriptor.payload_crc32c
            || digest.sha256 != descriptor.payload_sha256
        {
            return Err(
                AppendTableError::Corruption("append block checksum mismatch".to_string()).into(),
            );
        }
        let expected = segment::to_usize(descriptor.decoded_bytes, "decoded block length")?;
        if expected > self.config.max_decoded_block_bytes {
            return Err(AppendTableError::Admission(format!(
                "append block declares {expected} decoded bytes, exceeding limit {}",
                self.config.max_decoded_block_bytes
            ))
            .into());
        }
        let unit = work.start_unit()?;
        let decoder =
            zstd::stream::read::Decoder::new(Cursor::new(&*compressed)).map_err(|error| {
                AppendTableError::Corruption(format!(
                    "failed to open append block decoder: {error}"
                ))
            })?;
        let limit = u64::try_from(self.config.max_decoded_block_bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let mut decoder = decoder.take(limit);
        unit.finish();
        let mut decoded = CheckpointBytes::new(expected, work)?;
        let mut chunk = [0_u8; 64 * 1024];
        loop {
            let unit = work.start_unit()?;
            let count = decoder.read(&mut chunk).map_err(|error| {
                AppendTableError::Corruption(format!("failed to decompress append block: {error}"))
            })?;
            unit.finish();
            if count == 0 {
                break;
            }
            decoded.append(&chunk[..count], work)?;
        }
        if decoded.len() != expected {
            return Err(AppendTableError::Corruption(format!(
                "append block decoded length mismatch: expected {expected}, got {}",
                decoded.len()
            ))
            .into());
        }
        decode_block(descriptor, &decoded, max_payload_bytes, self.config, work)
    }
}

fn decode_block(
    descriptor: &AppendSegmentBlockDescriptor,
    decoded: &[u8],
    max_payload_bytes: usize,
    config: AppendSegmentConfig,
    work: &CheckpointWorkContext,
) -> Result<CheckpointAppendRows, Failure> {
    let unit = work.start_unit()?;
    let mut decoder = segment::Decoder::new(decoded);
    let row_count = decoder.count(config.max_rows, "append block rows")?;
    if row_count != descriptor.row_count as usize {
        return Err(AppendTableError::Corruption(format!(
            "append block row count mismatch: expected {}, got {row_count}",
            descriptor.row_count
        ))
        .into());
    }
    let mut value_count = 0usize;
    let mut references = BTreeMap::new();
    unit.finish();
    let mut rows = CheckpointAppendRows::new(row_count, work)?;
    for _ in 0..row_count {
        let unit = work.start_unit()?;
        let key = segment::decode_append_key(
            decoder.bytes(config.max_key_bytes, "append order key")?,
            false,
        )?;
        if rows.last().is_some_and(|prior| prior.order_key >= key) {
            return Err(AppendTableError::Corruption(
                "append block order keys are duplicated or unordered".to_string(),
            )
            .into());
        }
        let row_payload = decoder.bytes(config.max_row_bytes, "append row")?;
        let row = decode_relational_row_payload(
            row_payload,
            config.max_values.saturating_sub(value_count),
            config.max_value_bytes(),
        )
        .map_err(segment::map_relational)?;
        value_count = value_count.checked_add(row.values().len()).ok_or_else(|| {
            AppendTableError::Corruption("append block value count overflow".to_string())
        })?;
        if value_count > config.max_values {
            return Err(AppendTableError::Admission(format!(
                "append block values exceed limit {}",
                config.max_values
            ))
            .into());
        }
        unit.finish();
        for value in row.values() {
            let unit = work.start_unit()?;
            if let RelationalValue::Overflow(reference) = value
                && let Some(existing) = references.insert(reference.digest, *reference)
                && existing != *reference
            {
                return Err(AppendTableError::Corruption(
                    "append overflow digest has conflicting references".to_string(),
                )
                .into());
            }
            unit.finish();
        }
        let unit = work.start_unit()?;
        rows.push_owned(AppendTableRow {
            table: descriptor.table.clone(),
            partition_key: descriptor.partition_key.clone(),
            order_key: key,
            row,
        })?;
        unit.finish();
    }
    let unit = work.start_unit()?;
    let overflow_count = decoder.count(config.max_values, "append overflow values")?;
    let mut envelopes = BTreeMap::new();
    unit.finish();
    for _ in 0..overflow_count {
        let unit = work.start_unit()?;
        let digest = Sha256Digest::from_bytes(
            decoder
                .take(32, "append overflow digest")?
                .try_into()
                .expect("fixed SHA-256"),
        );
        let envelope = decoder.bytes(
            config.max_row_bytes.saturating_add(32),
            "append overflow envelope",
        )?;
        if envelopes.insert(digest, envelope).is_some() {
            return Err(AppendTableError::Corruption(
                "append block contains duplicate overflow envelopes".to_string(),
            )
            .into());
        }
        unit.finish();
    }
    decoder.finish("append block")?;
    if rows.last().map(|row| &row.order_key) != Some(&descriptor.max_order_key) {
        return Err(AppendTableError::Corruption(
            "append block maximum order key differs from its descriptor".to_string(),
        )
        .into());
    }
    if references.len() != envelopes.len() {
        return Err(AppendTableError::Corruption(
            "append block overflow envelopes do not match its row closure".to_string(),
        )
        .into());
    }
    for digest in references.keys() {
        let unit = work.start_unit()?;
        if !envelopes.contains_key(digest) {
            return Err(AppendTableError::Corruption(
                "append block overflow envelopes do not match its row closure".to_string(),
            )
            .into());
        }
        unit.finish();
    }
    let mut budget = RelationalHydrationBudget {
        max_rows: rows.len().max(1),
        max_compressed_bytes: config.max_decoded_block_bytes.min(max_payload_bytes.max(1)),
        max_decompressed_bytes: config.max_decoded_block_bytes.min(max_payload_bytes.max(1)),
        max_memory_bytes: config.max_decoded_block_bytes.min(max_payload_bytes.max(1)),
        ..RelationalHydrationBudget::default()
    };
    let mut hydrated: BTreeMap<Sha256Digest, RelationalValue> = BTreeMap::new();
    for row in rows.as_mut_slice() {
        let mut values = Vec::with_capacity(row.row.values().len());
        for value in row.row.values() {
            let unit = work.start_unit()?;
            let value = if let RelationalValue::Overflow(reference) = value {
                if let Some(value) = hydrated.get(&reference.digest) {
                    value.clone()
                } else {
                    let envelope = envelopes.get(&reference.digest).ok_or_else(|| {
                        AppendTableError::Corruption(format!(
                            "append block is missing overflow envelope {}",
                            reference.digest
                        ))
                    })?;
                    let value = crate::relational::overflow::decode_overflow_envelope(
                        reference,
                        envelope,
                        &mut budget,
                        None,
                    )
                    .map_err(segment::map_relational)?;
                    hydrated.insert(reference.digest, value.clone());
                    value
                }
            } else {
                value.clone()
            };
            values.push(value);
            unit.finish();
        }
        let unit = work.start_unit()?;
        row.row = RelationalRow::new(values);
        unit.finish();
    }
    work.checkpoint()?;
    Ok(rows)
}
