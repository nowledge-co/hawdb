use super::{
    RelationalError, RelationalOverflowSegment, RelationalRow, RelationalScalarType,
    RelationalState, RelationalTableSchema, RelationalValue,
};
use skein_integrity::{crc32c, integrity_digest};
use std::collections::BTreeSet;
use std::io::{Cursor, Read};
use std::sync::Arc;

const OVERFLOW_MAGIC: &[u8; 8] = b"SKOVFL01";
const OVERFLOW_CODEC_RAW: u8 = 0;
const OVERFLOW_CODEC_ZSTD: u8 = 1;
const OVERFLOW_HEADER_BYTES: usize = 32;
const DEFAULT_ZSTD_LEVEL: i32 = 3;
const MIN_ZSTD_SAVINGS_BYTES: usize = 64;
const MIN_ZSTD_SAVINGS_PERCENT: usize = 25;
const COMPRESSION_SAMPLE_BYTES: usize = 4 * 1024;

pub const DEFAULT_RELATIONAL_OVERFLOW_THRESHOLD_BYTES: usize = 4 * 1024;
pub const DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalOverflowConfig {
    pub threshold_bytes: usize,
    pub compression_level: i32,
    pub max_value_bytes: usize,
}

impl Default for RelationalOverflowConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: DEFAULT_RELATIONAL_OVERFLOW_THRESHOLD_BYTES,
            compression_level: DEFAULT_ZSTD_LEVEL,
            max_value_bytes: DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelationalOverflowRef {
    pub digest: String,
    pub scalar_type: RelationalScalarType,
    pub compressed_bytes: usize,
    pub uncompressed_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalHydrationBudget {
    pub max_rows: usize,
    pub max_compressed_bytes: usize,
    pub max_decompressed_bytes: usize,
    pub max_memory_bytes: usize,
    pub hydrated_rows: usize,
    pub compressed_bytes: usize,
    pub decompressed_bytes: usize,
    pub memory_bytes: usize,
}

impl Default for RelationalHydrationBudget {
    fn default() -> Self {
        Self {
            max_rows: 1_000,
            max_compressed_bytes: DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
            max_decompressed_bytes: DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
            max_memory_bytes: DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
            hydrated_rows: 0,
            compressed_bytes: 0,
            decompressed_bytes: 0,
            memory_bytes: 0,
        }
    }
}

pub(super) fn externalize_row(
    state: &mut RelationalState,
    schema: &RelationalTableSchema,
    row: &mut RelationalRow,
    config: RelationalOverflowConfig,
) -> Result<(), RelationalError> {
    let protected = protected_column_positions(schema)?;
    let values = Arc::make_mut(&mut row.values);
    for (position, value) in values.iter_mut().enumerate() {
        if protected.contains(&position) {
            continue;
        }
        let (scalar_type, raw) = match value {
            RelationalValue::Text(text) if text.len() >= config.threshold_bytes => {
                (RelationalScalarType::Text, text.as_bytes().to_vec())
            }
            RelationalValue::Bytea(bytes) if bytes.len() >= config.threshold_bytes => {
                (RelationalScalarType::Bytea, bytes.clone())
            }
            _ => continue,
        };
        if raw.len() > config.max_value_bytes {
            return Err(RelationalError::Admission(format!(
                "overflow value contains {} bytes, exceeding max_value_bytes {}",
                raw.len(),
                config.max_value_bytes
            )));
        }
        let envelope = encode_envelope(scalar_type, &raw, config.compression_level)?;
        let digest = integrity_digest(&envelope).sha256.to_string();
        let reference = RelationalOverflowRef {
            digest: digest.clone(),
            scalar_type,
            compressed_bytes: envelope.len() - OVERFLOW_HEADER_BYTES,
            uncompressed_bytes: raw.len(),
        };
        state
            .overflow_segments
            .entry(digest)
            .or_insert_with(|| RelationalOverflowSegment::Inline(Arc::from(envelope)));
        *value = RelationalValue::Overflow(reference);
    }
    Ok(())
}

pub(super) fn hydrate_row(
    state: &RelationalState,
    row: &RelationalRow,
    budget: &mut RelationalHydrationBudget,
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<RelationalRow, RelationalError> {
    runtime_checkpoint(task_context)?;
    let mut staged_budget = *budget;
    if staged_budget.hydrated_rows >= staged_budget.max_rows {
        return Err(RelationalError::Admission(format!(
            "relational hydration exceeds max_rows {}",
            staged_budget.max_rows
        )));
    }
    staged_budget.hydrated_rows += 1;
    let mut values = row.values.to_vec();
    for value in &mut values {
        let RelationalValue::Overflow(reference) = value else {
            continue;
        };
        let segment = state
            .overflow_segments
            .get(&reference.digest)
            .ok_or_else(|| {
                RelationalError::Corruption(format!(
                    "missing overflow segment {}",
                    reference.digest
                ))
            })?;
        runtime_checkpoint(task_context)?;
        let envelope = segment.read()?;
        let hydrated = decode_envelope(reference, &envelope, &mut staged_budget, task_context)?;
        *value = match reference.scalar_type {
            RelationalScalarType::Text => {
                RelationalValue::Text(String::from_utf8(hydrated).map_err(|error| {
                    RelationalError::Corruption(format!(
                        "overflow text is not valid UTF-8: {error}"
                    ))
                })?)
            }
            RelationalScalarType::Bytea => RelationalValue::Bytea(hydrated),
            _ => {
                return Err(RelationalError::Corruption(
                    "overflow segment has a non-payload scalar type".to_string(),
                ));
            }
        };
    }
    *budget = staged_budget;
    Ok(RelationalRow::new(values))
}

fn runtime_checkpoint(
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<(), RelationalError> {
    task_context.map_or(Ok(()), |context| {
        context.checkpoint().map_err(|reason| {
            RelationalError::Admission(format!("relational hydration stopped: {reason}"))
        })
    })
}

pub(super) fn prune_unreachable_segments(state: &mut RelationalState) {
    let reachable = state
        .segments
        .values()
        .flat_map(|segment| segment.rows.values())
        .flat_map(|row| row.values.iter())
        .filter_map(|value| match value {
            RelationalValue::Overflow(reference) => Some(reference.digest.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    state
        .overflow_segments
        .retain(|digest, _| reachable.contains(digest));
}

fn protected_column_positions(
    schema: &RelationalTableSchema,
) -> Result<BTreeSet<usize>, RelationalError> {
    let names = schema
        .primary_key
        .iter()
        .chain(schema.unique_constraints.iter().flatten())
        .chain(
            schema
                .foreign_keys
                .iter()
                .flat_map(|key| key.columns.iter()),
        )
        .chain(schema.indexes.iter().flat_map(|index| index.columns.iter()))
        .collect::<BTreeSet<_>>();
    names
        .into_iter()
        .map(|name| {
            schema.column_position(name).ok_or_else(|| {
                RelationalError::Schema(format!(
                    "overflow protection references unknown column {name}"
                ))
            })
        })
        .collect()
}

fn encode_envelope(
    scalar_type: RelationalScalarType,
    raw: &[u8],
    compression_level: i32,
) -> Result<Vec<u8>, RelationalError> {
    let sample = &raw[..raw.len().min(COMPRESSION_SAMPLE_BYTES)];
    let compressed_sample = compress(sample, compression_level)?;
    let should_compress = compression_is_worthwhile(sample.len(), compressed_sample.len());
    let compressed = if should_compress && sample.len() != raw.len() {
        Some(compress(raw, compression_level)?)
    } else if should_compress {
        Some(compressed_sample)
    } else {
        None
    };
    let (codec, payload) = compressed
        .as_ref()
        .map_or((OVERFLOW_CODEC_RAW, raw), |compressed| {
            (OVERFLOW_CODEC_ZSTD, compressed.as_slice())
        });
    let mut envelope = Vec::with_capacity(OVERFLOW_HEADER_BYTES + payload.len());
    envelope.extend_from_slice(OVERFLOW_MAGIC);
    envelope.push(codec);
    envelope.push(match scalar_type {
        RelationalScalarType::Text => 1,
        RelationalScalarType::Bytea => 2,
        _ => {
            return Err(RelationalError::Schema(
                "only TEXT and BYTEA can use overflow storage".to_string(),
            ));
        }
    });
    envelope.extend_from_slice(&[0; 2]);
    envelope.extend_from_slice(&(raw.len() as u64).to_le_bytes());
    envelope.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    envelope.extend_from_slice(&crc32c(raw).get().to_le_bytes());
    envelope.extend_from_slice(payload);
    Ok(envelope)
}

fn compress(raw: &[u8], compression_level: i32) -> Result<Vec<u8>, RelationalError> {
    zstd::stream::encode_all(raw, compression_level).map_err(|error| {
        RelationalError::Durability(format!("failed to compress overflow value: {error}"))
    })
}

fn compression_is_worthwhile(raw_bytes: usize, compressed_bytes: usize) -> bool {
    let saved_bytes = raw_bytes.saturating_sub(compressed_bytes);
    saved_bytes >= MIN_ZSTD_SAVINGS_BYTES
        && saved_bytes.saturating_mul(100) >= raw_bytes.saturating_mul(MIN_ZSTD_SAVINGS_PERCENT)
}

fn decode_envelope(
    reference: &RelationalOverflowRef,
    envelope: &[u8],
    budget: &mut RelationalHydrationBudget,
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<Vec<u8>, RelationalError> {
    runtime_checkpoint(task_context)?;
    if envelope.len() < OVERFLOW_HEADER_BYTES || &envelope[..8] != OVERFLOW_MAGIC {
        return Err(RelationalError::Corruption(
            "invalid overflow envelope header".to_string(),
        ));
    }
    if integrity_digest(envelope).sha256.to_string() != reference.digest {
        return Err(RelationalError::Corruption(
            "overflow envelope digest mismatch".to_string(),
        ));
    }
    let codec = envelope[8];
    if !matches!(codec, OVERFLOW_CODEC_RAW | OVERFLOW_CODEC_ZSTD) {
        return Err(RelationalError::Corruption(
            "unsupported overflow codec".to_string(),
        ));
    }
    let scalar_type = match envelope[9] {
        1 => RelationalScalarType::Text,
        2 => RelationalScalarType::Bytea,
        _ => {
            return Err(RelationalError::Corruption(
                "invalid overflow scalar type".to_string(),
            ));
        }
    };
    let uncompressed_bytes = read_u64(&envelope[12..20])? as usize;
    let compressed_bytes = read_u64(&envelope[20..28])? as usize;
    let checksum = read_u32(&envelope[28..32])?;
    if scalar_type != reference.scalar_type
        || uncompressed_bytes != reference.uncompressed_bytes
        || compressed_bytes != reference.compressed_bytes
        || envelope.len() != OVERFLOW_HEADER_BYTES.saturating_add(compressed_bytes)
    {
        return Err(RelationalError::Corruption(
            "overflow envelope metadata mismatch".to_string(),
        ));
    }
    admit_hydration(reference, budget)?;
    let payload = &envelope[OVERFLOW_HEADER_BYTES..];
    let decoded = if codec == OVERFLOW_CODEC_RAW {
        if compressed_bytes != uncompressed_bytes {
            return Err(RelationalError::Corruption(
                "raw overflow envelope length mismatch".to_string(),
            ));
        }
        payload.to_vec()
    } else {
        let decoder = zstd::stream::read::Decoder::new(Cursor::new(payload)).map_err(|error| {
            RelationalError::Corruption(format!("failed to initialize overflow decoder: {error}"))
        })?;
        let mut decoder = decoder.take(uncompressed_bytes.saturating_add(1) as u64);
        let mut decoded = Vec::with_capacity(uncompressed_bytes.min(1024 * 1024));
        let mut chunk = [0_u8; 64 * 1024];
        loop {
            runtime_checkpoint(task_context)?;
            let read = decoder.read(&mut chunk).map_err(|error| {
                RelationalError::Corruption(format!("failed to decode overflow value: {error}"))
            })?;
            if read == 0 {
                break;
            }
            decoded.extend_from_slice(&chunk[..read]);
        }
        decoded
    };
    if decoded.len() != uncompressed_bytes || crc32c(&decoded).get() != checksum {
        return Err(RelationalError::Corruption(
            "overflow decoded length or checksum mismatch".to_string(),
        ));
    }
    budget.compressed_bytes += compressed_bytes;
    budget.decompressed_bytes += uncompressed_bytes;
    budget.memory_bytes += uncompressed_bytes;
    Ok(decoded)
}

fn admit_hydration(
    reference: &RelationalOverflowRef,
    budget: &RelationalHydrationBudget,
) -> Result<(), RelationalError> {
    if budget
        .compressed_bytes
        .saturating_add(reference.compressed_bytes)
        > budget.max_compressed_bytes
        || budget
            .decompressed_bytes
            .saturating_add(reference.uncompressed_bytes)
            > budget.max_decompressed_bytes
        || budget
            .memory_bytes
            .saturating_add(reference.uncompressed_bytes)
            > budget.max_memory_bytes
    {
        return Err(RelationalError::Admission(
            "overflow hydration exceeds compressed, decompressed, or memory budget".to_string(),
        ));
    }
    Ok(())
}

fn read_u64(bytes: &[u8]) -> Result<u64, RelationalError> {
    bytes
        .try_into()
        .map(u64::from_le_bytes)
        .map_err(|_| RelationalError::Corruption("truncated overflow u64".to_string()))
}

fn read_u32(bytes: &[u8]) -> Result<u32, RelationalError> {
    bytes
        .try_into()
        .map(u32::from_le_bytes)
        .map_err(|_| RelationalError::Corruption("truncated overflow u32".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incompressible_payload_uses_raw_overflow_codec() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let raw = (0..16 * 1024)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();

        let envelope = encode_envelope(RelationalScalarType::Bytea, &raw, 3).unwrap();
        assert_eq!(envelope[8], OVERFLOW_CODEC_RAW);

        let reference = RelationalOverflowRef {
            digest: integrity_digest(&envelope).sha256.to_string(),
            scalar_type: RelationalScalarType::Bytea,
            compressed_bytes: raw.len(),
            uncompressed_bytes: raw.len(),
        };
        let mut budget = RelationalHydrationBudget::default();
        assert_eq!(
            decode_envelope(&reference, &envelope, &mut budget, None).unwrap(),
            raw
        );
        assert_eq!(budget.compressed_bytes, raw.len());
        assert_eq!(budget.decompressed_bytes, raw.len());
    }

    #[test]
    fn compressible_payload_keeps_zstd_overflow_codec() {
        let raw = vec![b'x'; 16 * 1024];
        let envelope = encode_envelope(RelationalScalarType::Bytea, &raw, 3).unwrap();
        assert_eq!(envelope[8], OVERFLOW_CODEC_ZSTD);
    }

    #[test]
    fn compression_policy_requires_material_absolute_and_relative_savings() {
        assert!(compression_is_worthwhile(4096, 3072));
        assert!(!compression_is_worthwhile(4096, 3073));
        assert!(!compression_is_worthwhile(100, 50));
    }
}
