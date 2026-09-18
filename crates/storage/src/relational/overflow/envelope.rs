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

//! Canonical v1 codec for immutable relational overflow values.

use super::{
    runtime_checkpoint, RelationalHydrationBudget, RelationalOverflowConfig, RelationalOverflowRef,
};
use crate::relational::{RelationalError, RelationalScalarType, RelationalValue};
use hawdb_integrity::{crc32c, integrity_digest};
use std::io::{Cursor, Read};
use std::sync::Arc;

const OVERFLOW_MAGIC: &[u8; 8] = b"SKOVFL01";
const OVERFLOW_CODEC_RAW: u8 = 0;
const OVERFLOW_CODEC_ZSTD: u8 = 1;
pub(super) const OVERFLOW_HEADER_BYTES: usize = 32;
pub(super) const DEFAULT_ZSTD_LEVEL: i32 = 3;
const MIN_ZSTD_SAVINGS_BYTES: usize = 64;
const MIN_ZSTD_SAVINGS_PERCENT: usize = 25;
const COMPRESSION_SAMPLE_BYTES: usize = 4 * 1024;
const DECODE_CHUNK_BYTES: usize = 64 * 1024;
const INITIAL_DECODE_CAPACITY_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncodedRelationalOverflow {
    pub(crate) reference: RelationalOverflowRef,
    pub(crate) bytes: Arc<[u8]>,
}

pub(crate) fn encode_overflow_envelope(
    scalar_type: RelationalScalarType,
    raw: &[u8],
    config: RelationalOverflowConfig,
) -> Result<EncodedRelationalOverflow, RelationalError> {
    let scalar_tag = encode_scalar_type(scalar_type)?;
    if raw.is_empty() {
        return Err(RelationalError::Admission(
            "overflow value must not be empty".to_string(),
        ));
    }
    if raw.len() > config.max_value_bytes {
        return Err(RelationalError::Admission(format!(
            "overflow value contains {} bytes, exceeding max_value_bytes {}",
            raw.len(),
            config.max_value_bytes
        )));
    }

    let sample = &raw[..raw.len().min(COMPRESSION_SAMPLE_BYTES)];
    let compressed_sample = compress(sample, config.compression_level)?;
    let should_compress = compression_is_worthwhile(sample.len(), compressed_sample.len());
    let compressed = if should_compress && sample.len() != raw.len() {
        Some(compress(raw, config.compression_level)?)
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
    let encoded_len = OVERFLOW_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or_else(|| RelationalError::Admission("overflow envelope size overflow".to_string()))?;
    let uncompressed_bytes = u64::try_from(raw.len()).map_err(|_| {
        RelationalError::Admission("overflow value length does not fit u64".to_string())
    })?;
    let compressed_bytes = u64::try_from(payload.len()).map_err(|_| {
        RelationalError::Admission("overflow payload length does not fit u64".to_string())
    })?;
    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.extend_from_slice(OVERFLOW_MAGIC);
    encoded.push(codec);
    encoded.push(scalar_tag);
    encoded.extend_from_slice(&0_u16.to_le_bytes());
    encoded.extend_from_slice(&uncompressed_bytes.to_le_bytes());
    encoded.extend_from_slice(&compressed_bytes.to_le_bytes());
    encoded.extend_from_slice(&crc32c(raw).get().to_le_bytes());
    encoded.extend_from_slice(payload);
    debug_assert_eq!(encoded.len(), encoded_len);

    let digest = integrity_digest(&encoded).sha256;
    Ok(EncodedRelationalOverflow {
        reference: RelationalOverflowRef {
            digest,
            scalar_type,
            compressed_bytes,
            uncompressed_bytes,
        },
        bytes: Arc::from(encoded),
    })
}

pub(crate) fn decode_overflow_envelope(
    reference: &RelationalOverflowRef,
    encoded: &[u8],
    budget: &mut RelationalHydrationBudget,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<RelationalValue, RelationalError> {
    runtime_checkpoint(task_context)?;
    let header = decode_header(reference, encoded)?;
    let next_budget = admitted_hydration_budget(reference, budget)?;
    let payload = &encoded[OVERFLOW_HEADER_BYTES..];
    let decoded = match header.codec {
        OVERFLOW_CODEC_RAW => {
            if header.compressed_bytes != header.uncompressed_bytes {
                return Err(RelationalError::Corruption(
                    "raw overflow envelope length mismatch".to_string(),
                ));
            }
            payload.to_vec()
        }
        OVERFLOW_CODEC_ZSTD => decode_zstd(payload, header.uncompressed_bytes, task_context)?,
        _ => unreachable!("validated overflow codec"),
    };
    if decoded.len() != header.uncompressed_bytes || crc32c(&decoded).get() != header.checksum {
        return Err(RelationalError::Corruption(
            "overflow decoded length or checksum mismatch".to_string(),
        ));
    }
    let value = match header.scalar_type {
        RelationalScalarType::Text => {
            RelationalValue::Text(String::from_utf8(decoded).map_err(|error| {
                RelationalError::Corruption(format!("overflow text is not valid UTF-8: {error}"))
            })?)
        }
        RelationalScalarType::Bytea => RelationalValue::Bytea(decoded),
        _ => unreachable!("validated overflow scalar type"),
    };
    *budget = next_budget;
    Ok(value)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecodedHeader {
    codec: u8,
    scalar_type: RelationalScalarType,
    compressed_bytes: usize,
    uncompressed_bytes: usize,
    checksum: u32,
}

fn decode_header(
    reference: &RelationalOverflowRef,
    encoded: &[u8],
) -> Result<DecodedHeader, RelationalError> {
    if encoded.len() < OVERFLOW_HEADER_BYTES || &encoded[..8] != OVERFLOW_MAGIC {
        return Err(RelationalError::Corruption(
            "invalid overflow envelope header".to_string(),
        ));
    }
    if encoded[10..12] != [0, 0] {
        return Err(RelationalError::Corruption(
            "overflow envelope has unsupported flags".to_string(),
        ));
    }
    if integrity_digest(encoded).sha256 != reference.digest {
        return Err(RelationalError::Corruption(
            "overflow envelope digest mismatch".to_string(),
        ));
    }
    let codec = encoded[8];
    if !matches!(codec, OVERFLOW_CODEC_RAW | OVERFLOW_CODEC_ZSTD) {
        return Err(RelationalError::Corruption(
            "unsupported overflow codec".to_string(),
        ));
    }
    let scalar_type = decode_scalar_type(encoded[9])?;
    let uncompressed_bytes = usize::try_from(read_u64(&encoded[12..20])?).map_err(|_| {
        RelationalError::Corruption("overflow uncompressed length overflows usize".to_string())
    })?;
    let compressed_bytes = usize::try_from(read_u64(&encoded[20..28])?).map_err(|_| {
        RelationalError::Corruption("overflow compressed length overflows usize".to_string())
    })?;
    let checksum = read_u32(&encoded[28..32])?;
    let expected_len = OVERFLOW_HEADER_BYTES
        .checked_add(compressed_bytes)
        .ok_or_else(|| {
            RelationalError::Corruption("overflow envelope size overflow".to_string())
        })?;
    if scalar_type != reference.scalar_type
        || u64::try_from(uncompressed_bytes).ok() != Some(reference.uncompressed_bytes)
        || u64::try_from(compressed_bytes).ok() != Some(reference.compressed_bytes)
        || encoded.len() != expected_len
        || compressed_bytes == 0
        || uncompressed_bytes == 0
    {
        return Err(RelationalError::Corruption(
            "overflow envelope metadata mismatch".to_string(),
        ));
    }
    Ok(DecodedHeader {
        codec,
        scalar_type,
        compressed_bytes,
        uncompressed_bytes,
        checksum,
    })
}

fn decode_zstd(
    payload: &[u8],
    uncompressed_bytes: usize,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<Vec<u8>, RelationalError> {
    let decoder = zstd::stream::read::Decoder::new(Cursor::new(payload)).map_err(|error| {
        RelationalError::Corruption(format!("failed to initialize overflow decoder: {error}"))
    })?;
    let read_limit = uncompressed_bytes
        .checked_add(1)
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| RelationalError::Corruption("overflow decode limit overflow".to_string()))?;
    let mut decoder = decoder.take(read_limit);
    let mut decoded = Vec::with_capacity(uncompressed_bytes.min(INITIAL_DECODE_CAPACITY_BYTES));
    let mut chunk = [0_u8; DECODE_CHUNK_BYTES];
    loop {
        runtime_checkpoint(task_context)?;
        let read = decoder.read(&mut chunk).map_err(|error| {
            RelationalError::Corruption(format!("failed to decode overflow value: {error}"))
        })?;
        if read == 0 {
            break;
        }
        let next_len = decoded.len().checked_add(read).ok_or_else(|| {
            RelationalError::Corruption("overflow decoded length overflow".to_string())
        })?;
        if next_len > uncompressed_bytes {
            return Err(RelationalError::Corruption(
                "overflow payload expands beyond its declared length".to_string(),
            ));
        }
        decoded.extend_from_slice(&chunk[..read]);
    }
    Ok(decoded)
}

fn admitted_hydration_budget(
    reference: &RelationalOverflowRef,
    budget: &RelationalHydrationBudget,
) -> Result<RelationalHydrationBudget, RelationalError> {
    let compressed_bytes = usize::try_from(reference.compressed_bytes).map_err(|_| {
        RelationalError::Admission("overflow compressed length exceeds this target".to_string())
    })?;
    let uncompressed_bytes = usize::try_from(reference.uncompressed_bytes).map_err(|_| {
        RelationalError::Admission("overflow uncompressed length exceeds this target".to_string())
    })?;
    let mut next = *budget;
    next.compressed_bytes = next
        .compressed_bytes
        .checked_add(compressed_bytes)
        .ok_or_else(|| RelationalError::Admission("hydration byte count overflow".to_string()))?;
    next.decompressed_bytes = next
        .decompressed_bytes
        .checked_add(uncompressed_bytes)
        .ok_or_else(|| RelationalError::Admission("hydration byte count overflow".to_string()))?;
    next.memory_bytes = next
        .memory_bytes
        .checked_add(uncompressed_bytes)
        .ok_or_else(|| RelationalError::Admission("hydration byte count overflow".to_string()))?;
    if next.compressed_bytes > next.max_compressed_bytes
        || next.decompressed_bytes > next.max_decompressed_bytes
        || next.memory_bytes > next.max_memory_bytes
    {
        return Err(RelationalError::Admission(
            "overflow hydration exceeds compressed, decompressed, or memory budget".to_string(),
        ));
    }
    Ok(next)
}

pub(in crate::relational) fn admit_overflow_hydration(
    reference: &RelationalOverflowRef,
    budget: &RelationalHydrationBudget,
) -> Result<(), RelationalError> {
    admitted_hydration_budget(reference, budget).map(|_| ())
}

fn encode_scalar_type(scalar_type: RelationalScalarType) -> Result<u8, RelationalError> {
    match scalar_type {
        RelationalScalarType::Text => Ok(1),
        RelationalScalarType::Bytea => Ok(2),
        _ => Err(RelationalError::Schema(
            "only TEXT and BYTEA can use overflow storage".to_string(),
        )),
    }
}

fn decode_scalar_type(tag: u8) -> Result<RelationalScalarType, RelationalError> {
    match tag {
        1 => Ok(RelationalScalarType::Text),
        2 => Ok(RelationalScalarType::Bytea),
        _ => Err(RelationalError::Corruption(
            "invalid overflow scalar type".to_string(),
        )),
    }
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
    fn raw_v1_fixture_is_stable_and_round_trips() {
        let raw = b"123456789";
        let encoded = encode_overflow_envelope(
            RelationalScalarType::Bytea,
            raw,
            RelationalOverflowConfig::default(),
        )
        .unwrap();
        let expected = [
            b'S', b'K', b'O', b'V', b'F', b'L', b'0', b'1', 0, 2, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 9,
            0, 0, 0, 0, 0, 0, 0, 0x83, 0x92, 0x06, 0xe3, b'1', b'2', b'3', b'4', b'5', b'6', b'7',
            b'8', b'9',
        ];
        assert_eq!(encoded.bytes.as_ref(), expected);

        let mut budget = RelationalHydrationBudget::default();
        assert_eq!(
            decode_overflow_envelope(&encoded.reference, &encoded.bytes, &mut budget, None)
                .unwrap(),
            RelationalValue::Bytea(raw.to_vec())
        );
        assert_eq!(budget.compressed_bytes, raw.len());
        assert_eq!(budget.decompressed_bytes, raw.len());
    }

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
        let encoded = encode_overflow_envelope(
            RelationalScalarType::Bytea,
            &raw,
            RelationalOverflowConfig::default(),
        )
        .unwrap();
        assert_eq!(encoded.bytes[8], OVERFLOW_CODEC_RAW);
    }

    #[test]
    fn compressible_payload_keeps_zstd_overflow_codec() {
        let raw = vec![b'x'; 16 * 1024];
        let encoded = encode_overflow_envelope(
            RelationalScalarType::Text,
            &raw,
            RelationalOverflowConfig::default(),
        )
        .unwrap();
        assert_eq!(encoded.bytes[8], OVERFLOW_CODEC_ZSTD);
        let mut budget = RelationalHydrationBudget::default();
        assert_eq!(
            decode_overflow_envelope(&encoded.reference, &encoded.bytes, &mut budget, None)
                .unwrap(),
            RelationalValue::Text("x".repeat(raw.len()))
        );
    }

    #[test]
    fn encoder_rejects_empty_oversize_and_non_payload_values() {
        let config = RelationalOverflowConfig {
            max_value_bytes: 8,
            ..RelationalOverflowConfig::default()
        };
        assert!(matches!(
            encode_overflow_envelope(RelationalScalarType::Text, b"", config),
            Err(RelationalError::Admission(_))
        ));
        assert!(matches!(
            encode_overflow_envelope(RelationalScalarType::Bytea, &[0; 9], config),
            Err(RelationalError::Admission(_))
        ));
        assert!(matches!(
            encode_overflow_envelope(RelationalScalarType::BigInt, b"1234", config),
            Err(RelationalError::Schema(_))
        ));
    }

    #[test]
    fn decoder_rejects_reserved_flags_with_a_matching_digest() {
        let mut encoded = encode_overflow_envelope(
            RelationalScalarType::Bytea,
            b"payload",
            RelationalOverflowConfig::default(),
        )
        .unwrap();
        let bytes = Arc::make_mut(&mut encoded.bytes);
        bytes[10] = 1;
        encoded.reference.digest = integrity_digest(bytes).sha256;
        let initial = RelationalHydrationBudget::default();
        let mut budget = initial;
        assert!(matches!(
            decode_overflow_envelope(&encoded.reference, bytes, &mut budget, None),
            Err(RelationalError::Corruption(_))
        ));
        assert_eq!(budget, initial);
    }

    #[test]
    fn decoder_rejects_truncation_without_charging_the_budget() {
        let encoded = encode_overflow_envelope(
            RelationalScalarType::Text,
            &vec![b'x'; 16 * 1024],
            RelationalOverflowConfig::default(),
        )
        .unwrap();
        let initial = RelationalHydrationBudget::default();
        let mut budget = initial;
        assert!(matches!(
            decode_overflow_envelope(
                &encoded.reference,
                &encoded.bytes[..encoded.bytes.len() - 1],
                &mut budget,
                None,
            ),
            Err(RelationalError::Corruption(_))
        ));
        assert_eq!(budget, initial);
    }

    #[test]
    fn decoder_rejects_payload_corruption_with_a_matching_digest() {
        let mut encoded = encode_overflow_envelope(
            RelationalScalarType::Bytea,
            b"payload",
            RelationalOverflowConfig::default(),
        )
        .unwrap();
        let bytes = Arc::make_mut(&mut encoded.bytes);
        *bytes.last_mut().unwrap() ^= 0xff;
        encoded.reference.digest = integrity_digest(bytes).sha256;
        let initial = RelationalHydrationBudget::default();
        let mut budget = initial;
        assert!(matches!(
            decode_overflow_envelope(&encoded.reference, bytes, &mut budget, None),
            Err(RelationalError::Corruption(_))
        ));
        assert_eq!(budget, initial);
    }

    #[test]
    fn hydration_admission_is_atomic() {
        let encoded = encode_overflow_envelope(
            RelationalScalarType::Text,
            b"payload",
            RelationalOverflowConfig::default(),
        )
        .unwrap();
        let initial = RelationalHydrationBudget {
            max_decompressed_bytes: 6,
            ..RelationalHydrationBudget::default()
        };
        let mut budget = initial;
        assert!(matches!(
            decode_overflow_envelope(&encoded.reference, &encoded.bytes, &mut budget, None),
            Err(RelationalError::Admission(_))
        ));
        assert_eq!(budget, initial);
    }

    #[test]
    fn compression_policy_requires_material_absolute_and_relative_savings() {
        assert!(compression_is_worthwhile(4096, 3072));
        assert!(!compression_is_worthwhile(4096, 3073));
        assert!(!compression_is_worthwhile(100, 50));
    }
}
