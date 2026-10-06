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

//! Cooperative construction of the existing V2 append segment.
//!
//! Rows, blocks and copies have separate cancellation/admission boundaries.
//! Complete buffers, variable-sized key comparisons and Arc conversion still
//! need the owner's resource reservation; this is not a byte-ledger substitute.

use super::*;
use crate::append_table::checkpoint::work_error;
use crate::background::CheckpointWorkContext;
use std::io::Write;

pub(super) fn encode(
    generation: u64,
    source_commit_epoch: u64,
    rows: &[AppendTableRow],
    config: AppendSegmentConfig,
    work: &CheckpointWorkContext,
) -> Result<AppendSegmentWriteOutput, AppendTableError> {
    work.checkpoint().map_err(work_error)?;
    validate_config(config)?;
    if rows.len() > config.max_rows {
        return Err(AppendTableError::Admission(format!(
            "append segment contains {} rows, exceeding limit {}",
            rows.len(),
            config.max_rows
        )));
    }
    for pair in rows.windows(2) {
        let unit = work.start_unit().map_err(work_error)?;
        if !compare_rows(&pair[0], &pair[1]).is_lt() {
            return Err(AppendTableError::Constraint(
                "append segment rows must be strictly ordered by table, partition, and order key"
                    .to_string(),
            ));
        }
        unit.finish();
    }

    let mut descriptors = Vec::new();
    let mut payload = Vec::new();
    let mut start = 0;
    let mut partition_end = 0;
    while start < rows.len() {
        let first = &rows[start];
        if start == partition_end {
            partition_end = start + 1;
            while partition_end < rows.len() {
                let unit = work.start_unit().map_err(work_error)?;
                let row = &rows[partition_end];
                let same = row.table == first.table && row.partition_key == first.partition_key;
                unit.finish();
                if !same {
                    break;
                }
                partition_end += 1;
            }
        }
        let (end, raw) = encode_block(rows, start, partition_end, config, work)?;
        if descriptors.len() == config.max_blocks {
            return Err(AppendTableError::Admission(format!(
                "append segment block count exceeds limit {}",
                config.max_blocks
            )));
        }
        if raw.len() > config.max_decoded_block_bytes {
            return Err(AppendTableError::Admission(format!(
                "append block contains {} decoded bytes, exceeding limit {}",
                raw.len(),
                config.max_decoded_block_bytes
            )));
        }
        let compressed = compress(&raw, config.compression_level, work)?;
        if compressed.len() > config.max_compressed_block_bytes {
            return Err(AppendTableError::Admission(format!(
                "append block contains {} compressed bytes, exceeding limit {}",
                compressed.len(),
                config.max_compressed_block_bytes
            )));
        }
        let payload_offset = u64::try_from(payload.len()).map_err(|_| {
            AppendTableError::Admission("append payload offset overflows u64".to_string())
        })?;
        let digest = work.integrity(&compressed).map_err(work_error)?;
        copy_bytes(&mut payload, &compressed, work)?;
        let unit = work.start_unit().map_err(work_error)?;
        descriptors.push(AppendSegmentBlockDescriptor {
            table: first.table.clone(),
            partition_key: first.partition_key.clone(),
            min_order_key: first.order_key.clone(),
            max_order_key: rows[end - 1].order_key.clone(),
            row_count: u32::try_from(end - start).map_err(|_| {
                AppendTableError::Admission("append block row count overflows u32".to_string())
            })?,
            payload_offset,
            compressed_bytes: u64::try_from(compressed.len()).map_err(|_| {
                AppendTableError::Admission(
                    "append compressed block length overflows u64".to_string(),
                )
            })?,
            decoded_bytes: u64::try_from(raw.len()).map_err(|_| {
                AppendTableError::Admission("append decoded block length overflows u64".to_string())
            })?,
            payload_crc32c: digest.crc32c.get(),
            payload_sha256: digest.sha256,
        });
        unit.finish();
        start = end;
    }

    let directory = directory(&descriptors, config, work)?;
    let total_len = SEGMENT_HEADER_BYTES
        .checked_add(directory.len())
        .and_then(|len| len.checked_add(payload.len()))
        .ok_or_else(|| AppendTableError::Admission("append segment size overflow".to_string()))?;
    if total_len > config.max_segment_bytes {
        return Err(AppendTableError::Admission(format!(
            "append segment contains {total_len} bytes, exceeding limit {}",
            config.max_segment_bytes
        )));
    }
    let body_digest = integrity_slices(&[&directory, &payload], work)?;
    let unit = work.start_unit().map_err(work_error)?;
    let mut encoded = Vec::with_capacity(total_len);
    encoded.extend_from_slice(SEGMENT_MAGIC);
    encoded.extend_from_slice(&SEGMENT_VERSION.to_le_bytes());
    encoded.extend_from_slice(&0u16.to_le_bytes());
    encoded.extend_from_slice(&generation.to_le_bytes());
    encoded.extend_from_slice(&source_commit_epoch.to_le_bytes());
    encoded.extend_from_slice(&(descriptors.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&(directory.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&body_digest.crc32c.get().to_le_bytes());
    encoded.extend_from_slice(body_digest.sha256.as_bytes());
    encoded.extend_from_slice(&0u64.to_le_bytes());
    debug_assert_eq!(encoded.len(), SEGMENT_HEADER_BYTES);
    unit.finish();
    copy_bytes(&mut encoded, &directory, work)?;
    copy_bytes(&mut encoded, &payload, work)?;
    let artifact_digest = work.integrity(&encoded).map_err(work_error)?;
    let unit = work.start_unit().map_err(work_error)?;
    let artifact = AppendSegmentArtifactMetadata {
        encoded_len: encoded.len() as u64,
        encoded_crc32c: artifact_digest.crc32c.get(),
        encoded_sha256: artifact_digest.sha256,
    };
    let output = AppendSegmentWriteOutput {
        generation,
        source_commit_epoch,
        encoded: Arc::from(encoded),
        descriptors,
        artifact,
    };
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(output)
}

fn encode_block(
    rows: &[AppendTableRow],
    start: usize,
    partition_end: usize,
    config: AppendSegmentConfig,
    work: &CheckpointWorkContext,
) -> Result<(usize, Vec<u8>), AppendTableError> {
    let mut encoder = Encoder::default();
    encoder.u32(0);
    let mut overflows = std::collections::BTreeMap::new();
    let mut end = start;
    while end < partition_end {
        let row = &rows[end];
        let order_key = key(&row.order_key, config.max_key_bytes, work)?;
        let (externalized, row_overflows) = externalize(&row.row, config, work)?;
        let row_payload =
            crate::relational::encode_relational_row_payload_with_work_context(&externalized, work)
                .map_err(map_relational)?;
        if row_payload.len() > config.max_row_bytes {
            return Err(AppendTableError::Admission(format!(
                "append row contains {} bytes, exceeding limit {}",
                row_payload.len(),
                config.max_row_bytes
            )));
        }
        let mut new_overflow_bytes = 0usize;
        for overflow in &row_overflows {
            let unit = work.start_unit().map_err(work_error)?;
            if overflows.contains_key(&overflow.reference.digest) {
                unit.finish();
                continue;
            }
            new_overflow_bytes = new_overflow_bytes
                .checked_add(SHA256_BYTES + 8)
                .and_then(|bytes| bytes.checked_add(overflow.bytes.len()))
                .ok_or_else(|| {
                    AppendTableError::Admission("append block size overflow".to_string())
                })?;
            unit.finish();
        }
        let next_len = encoder
            .len()
            .checked_add(16)
            .and_then(|len| len.checked_add(order_key.len()))
            .and_then(|len| len.checked_add(row_payload.len()))
            .and_then(|len| len.checked_add(new_overflow_bytes))
            .ok_or_else(|| AppendTableError::Admission("append block size overflow".to_string()))?;
        if end > start && next_len > config.target_decoded_block_bytes {
            break;
        }
        encode_bytes(&mut encoder, &order_key, "append order key", work)?;
        encode_bytes(&mut encoder, &row_payload, "append row payload", work)?;
        for overflow in row_overflows {
            let unit = work.start_unit().map_err(work_error)?;
            let existing = overflows.insert(overflow.reference.digest, overflow.clone());
            unit.finish();
            if let Some(existing) = existing {
                let same = existing.reference == overflow.reference
                    && bytes_equal(&existing.bytes, &overflow.bytes, work)?;
                if !same {
                    return Err(AppendTableError::Corruption(
                        "append overflow digest has conflicting envelopes".to_string(),
                    ));
                }
            }
        }
        end += 1;
    }
    let unit = work.start_unit().map_err(work_error)?;
    let row_count = u32::try_from(end - start).map_err(|_| {
        AppendTableError::Admission("append block row count exceeds durable limit".to_string())
    })?;
    encoder.overwrite_u32(0, row_count);
    encoder.u32(u32::try_from(overflows.len()).map_err(|_| {
        AppendTableError::Admission("append overflow count exceeds durable limit".to_string())
    })?);
    unit.finish();
    for (digest, overflow) in overflows {
        let unit = work.start_unit().map_err(work_error)?;
        encoder.raw(digest.as_bytes());
        unit.finish();
        encode_bytes(
            &mut encoder,
            &overflow.bytes,
            "append overflow envelope",
            work,
        )?;
    }
    work.checkpoint().map_err(work_error)?;
    Ok((end, encoder.finish()))
}

fn externalize(
    row: &RelationalRow,
    config: AppendSegmentConfig,
    work: &CheckpointWorkContext,
) -> Result<
    (
        RelationalRow,
        Vec<crate::relational::overflow::EncodedRelationalOverflow>,
    ),
    AppendTableError,
> {
    let overflow_config = RelationalOverflowConfig {
        threshold_bytes: config.overflow_threshold_bytes,
        compression_level: config.overflow_compression_level,
        max_value_bytes: config.max_row_bytes,
    };
    let mut values = Vec::with_capacity(row.values().len());
    let mut overflows = Vec::new();
    for value in row.values() {
        work.checkpoint().map_err(work_error)?;
        let encoded = match value {
            RelationalValue::Text(value) if value.len() >= config.overflow_threshold_bytes => Some(
                crate::relational::overflow::encode_overflow_envelope_with_work_context(
                    RelationalScalarType::Text,
                    value.as_bytes(),
                    overflow_config,
                    work,
                )
                .map_err(map_relational)?,
            ),
            RelationalValue::Bytea(value) if value.len() >= config.overflow_threshold_bytes => {
                Some(
                    crate::relational::overflow::encode_overflow_envelope_with_work_context(
                        RelationalScalarType::Bytea,
                        value,
                        overflow_config,
                        work,
                    )
                    .map_err(map_relational)?,
                )
            }
            RelationalValue::Overflow(_) => {
                return Err(AppendTableError::Constraint(
                    "append input rows cannot contain unresolved overflow references".to_string(),
                ));
            }
            _ => None,
        };
        let value = if let Some(encoded) = encoded {
            let unit = work.start_unit().map_err(work_error)?;
            let value = RelationalValue::Overflow(encoded.reference);
            overflows.push(encoded);
            unit.finish();
            value
        } else {
            clone_value(value, work)?
        };
        let unit = work.start_unit().map_err(work_error)?;
        values.push(value);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok((RelationalRow::new(values), overflows))
}

fn directory(
    descriptors: &[AppendSegmentBlockDescriptor],
    config: AppendSegmentConfig,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, AppendTableError> {
    let mut encoder = Encoder::default();
    for descriptor in descriptors {
        encode_bytes(
            &mut encoder,
            descriptor.table.as_bytes(),
            "append block table",
            work,
        )?;
        encode_bytes(
            &mut encoder,
            &key(&descriptor.partition_key, config.max_key_bytes, work)?,
            "append partition key",
            work,
        )?;
        encode_bytes(
            &mut encoder,
            &key(&descriptor.min_order_key, config.max_key_bytes, work)?,
            "append minimum order key",
            work,
        )?;
        encode_bytes(
            &mut encoder,
            &key(&descriptor.max_order_key, config.max_key_bytes, work)?,
            "append maximum order key",
            work,
        )?;
        let unit = work.start_unit().map_err(work_error)?;
        encoder.u32(descriptor.row_count);
        encoder.u64(descriptor.payload_offset);
        encoder.u64(descriptor.compressed_bytes);
        encoder.u64(descriptor.decoded_bytes);
        encoder.u32(descriptor.payload_crc32c);
        encoder.raw(descriptor.payload_sha256.as_bytes());
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(encoder.finish())
}

fn copy_bytes(
    target: &mut Vec<u8>,
    source: &[u8],
    work: &CheckpointWorkContext,
) -> Result<(), AppendTableError> {
    for bytes in source.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        target.extend_from_slice(bytes);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

fn encode_bytes(
    encoder: &mut Encoder,
    bytes: &[u8],
    context: &str,
    work: &CheckpointWorkContext,
) -> Result<(), AppendTableError> {
    let unit = work.start_unit().map_err(work_error)?;
    let len = u64::try_from(bytes.len())
        .map_err(|_| AppendTableError::Admission(format!("{context} length overflows u64")))?;
    encoder.u64(len);
    unit.finish();
    for chunk in bytes.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        encoder.raw(chunk);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

fn integrity_slices(
    slices: &[&[u8]],
    work: &CheckpointWorkContext,
) -> Result<hawdb_integrity::IntegrityDigest, AppendTableError> {
    let mut hasher = IntegrityHasher::new();
    for slice in slices {
        for chunk in slice.chunks(64 * 1024) {
            let unit = work.start_unit().map_err(work_error)?;
            hasher.update(chunk);
            unit.finish();
        }
    }
    work.checkpoint().map_err(work_error)?;
    Ok(hasher.finish())
}

fn compress(
    bytes: &[u8],
    level: i32,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, AppendTableError> {
    let error = |error| {
        AppendTableError::Corruption(format!("failed to compress append segment block: {error}"))
    };
    let unit = work.start_unit().map_err(work_error)?;
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), level).map_err(error)?;
    unit.finish();
    for chunk in bytes.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        encoder.write_all(chunk).map_err(error)?;
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
    let compressed = encoder.finish().map_err(error)?;
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(compressed)
}

#[cfg(test)]
mod tests;

fn key(
    key: &RelationalKey,
    max_key_bytes: usize,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, AppendTableError> {
    let unit = work.start_unit().map_err(work_error)?;
    if key.0.is_empty() {
        let encoded = vec![GLOBAL_PARTITION_TAG];
        unit.finish();
        work.checkpoint().map_err(work_error)?;
        return Ok(encoded);
    }
    unit.finish();
    let bytes = crate::relational::encode_relational_primary_key_with_work_context(key, work)
        .map_err(map_relational)?;
    let encoded_len = bytes.len().saturating_add(1);
    if encoded_len > max_key_bytes {
        return Err(AppendTableError::Admission(format!(
            "append key contains {encoded_len} bytes, exceeding limit {max_key_bytes}"
        )));
    }
    let unit = work.start_unit().map_err(work_error)?;
    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.push(ORDERED_KEY_TAG);
    unit.finish();
    copy_bytes(&mut encoded, &bytes, work)?;
    Ok(encoded)
}

fn bytes_equal(
    left: &[u8],
    right: &[u8],
    work: &CheckpointWorkContext,
) -> Result<bool, AppendTableError> {
    work.checkpoint().map_err(work_error)?;
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.chunks(64 * 1024).zip(right.chunks(64 * 1024)) {
        let unit = work.start_unit().map_err(work_error)?;
        let equal = left == right;
        unit.finish();
        work.checkpoint().map_err(work_error)?;
        if !equal {
            return Ok(false);
        }
    }
    work.checkpoint().map_err(work_error)?;
    Ok(true)
}

fn clone_value(
    value: &RelationalValue,
    work: &CheckpointWorkContext,
) -> Result<RelationalValue, AppendTableError> {
    let unit = work.start_unit().map_err(work_error)?;
    match value {
        RelationalValue::Text(text) => {
            let mut copy = String::new();
            unit.finish();
            let mut start = 0;
            while start < text.len() {
                let mut end = (start + 64 * 1024).min(text.len());
                while !text.is_char_boundary(end) {
                    end -= 1;
                }
                let unit = work.start_unit().map_err(work_error)?;
                copy.push_str(&text[start..end]);
                unit.finish();
                start = end;
            }
            work.checkpoint().map_err(work_error)?;
            Ok(RelationalValue::Text(copy))
        }
        RelationalValue::Bytea(bytes) => {
            let mut copy = Vec::new();
            unit.finish();
            copy_bytes(&mut copy, bytes, work)?;
            Ok(RelationalValue::Bytea(copy))
        }
        _ => {
            let copy = value.clone();
            unit.finish();
            work.checkpoint().map_err(work_error)?;
            Ok(copy)
        }
    }
}
