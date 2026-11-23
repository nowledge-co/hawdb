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

mod checkpoint;
pub(crate) use checkpoint::validation::validate_overflow_envelope_with_work_context;
pub(crate) use checkpoint::{
    encode_overflow_envelope_with_work_context, CheckpointRelationalOverflow,
};

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

fn encode_header(
    codec: u8,
    scalar_tag: u8,
    uncompressed_bytes: u64,
    compressed_bytes: u64,
    checksum: u32,
) -> [u8; OVERFLOW_HEADER_BYTES] {
    let mut header = [0; OVERFLOW_HEADER_BYTES];
    header[..8].copy_from_slice(OVERFLOW_MAGIC);
    header[8] = codec;
    header[9] = scalar_tag;
    header[12..20].copy_from_slice(&uncompressed_bytes.to_le_bytes());
    header[20..28].copy_from_slice(&compressed_bytes.to_le_bytes());
    header[28..].copy_from_slice(&checksum.to_le_bytes());
    header
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
    encoded.extend_from_slice(&encode_header(
        codec,
        scalar_tag,
        uncompressed_bytes,
        compressed_bytes,
        crc32c(raw).get(),
    ));
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
    decode_overflow_envelope_with_allocations(reference, encoded, budget, task_context, None)
}

pub(in crate::relational) fn decode_overflow_envelope_with_allocations(
    reference: &RelationalOverflowRef,
    encoded: &[u8],
    budget: &mut RelationalHydrationBudget,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<RelationalValue, RelationalError> {
    runtime_checkpoint(task_context)?;
    let header = decode_header(reference, encoded, allocations)?;
    let next_budget = admitted_hydration_budget(reference, budget)?;
    let payload = &encoded[OVERFLOW_HEADER_BYTES..];
    let decoded = match header.codec {
        OVERFLOW_CODEC_RAW => {
            if header.compressed_bytes != header.uncompressed_bytes {
                return Err(RelationalError::Corruption(
                    "raw overflow envelope length mismatch".to_string(),
                ));
            }
            match allocations {
                Some(context) => {
                    let mut decoded = context
                        .vector_capacity(payload.len())
                        .map_err(RelationalError::from_admission)?;
                    for chunk in payload.chunks(DECODE_CHUNK_BYTES) {
                        let unit = context.start_unit().map_err(RelationalError::from_work)?;
                        decoded.extend_from_slice(chunk);
                        unit.finish();
                    }
                    decoded
                }
                None => payload.to_vec(),
            }
        }
        OVERFLOW_CODEC_ZSTD => decode_zstd(
            payload,
            header.uncompressed_bytes,
            task_context,
            allocations,
        )?,
        _ => unreachable!("validated overflow codec"),
    };
    if decoded.len() != header.uncompressed_bytes {
        return Err(RelationalError::Corruption(
            "overflow decoded length or checksum mismatch".to_string(),
        ));
    }
    let checksum = match allocations {
        Some(context) => {
            context
                .integrity(&decoded)
                .map_err(RelationalError::from_work)?
                .crc32c
        }
        None => crc32c(&decoded),
    };
    if checksum.get() != header.checksum {
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
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<DecodedHeader, RelationalError> {
    validate_header_prefix(encoded)?;
    let digest = match allocations {
        Some(context) => context
            .integrity(encoded)
            .map_err(RelationalError::from_work)?,
        None => integrity_digest(encoded),
    };
    if digest.sha256 != reference.digest {
        return Err(RelationalError::Corruption(
            "overflow envelope digest mismatch".to_string(),
        ));
    }
    decode_verified_header(reference, encoded)
}

fn validate_header_prefix(encoded: &[u8]) -> Result<(), RelationalError> {
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
    Ok(())
}

// Only fixed-size fields are shared with the independent ordinary decoder.
fn decode_verified_header(
    reference: &RelationalOverflowRef,
    encoded: &[u8],
) -> Result<DecodedHeader, RelationalError> {
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
    allocations: Option<&crate::background::CheckpointDecodeContext>,
) -> Result<Vec<u8>, RelationalError> {
    let mut decoded = match allocations {
        Some(context) => context
            .vector_capacity(uncompressed_bytes)
            .map_err(RelationalError::from_admission)?,
        None => Vec::with_capacity(uncompressed_bytes.min(INITIAL_DECODE_CAPACITY_BYTES)),
    };
    // The payload already implements BufRead; avoid an additional buffered copy.
    let decoder = match allocations {
        Some(context) => {
            crate::compression::Decoder::with_buffer_and_work_context(Cursor::new(payload), context)
        }
        None => crate::compression::Decoder::with_buffer(Cursor::new(payload)),
    }
    .map_err(|error| {
        RelationalError::Corruption(format!("failed to initialize overflow decoder: {error}"))
    })?;
    let read_limit = uncompressed_bytes
        .checked_add(1)
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| RelationalError::Corruption("overflow decode limit overflow".to_string()))?;
    let mut decoder = decoder.take(read_limit);
    let mut chunk = [0_u8; DECODE_CHUNK_BYTES];
    loop {
        if let Some(context) = allocations {
            context.checkpoint().map_err(RelationalError::from_work)?;
        }
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
    crate::compression::encode_slice(raw, compression_level).map_err(|error| {
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
    fn controlled_overflow_output_denies_before_copy_and_preserves_budget() {
        use crate::background::{
            CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkContext,
            CheckpointWorkError,
        };
        use hawdb_qos::{
            IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
            RuntimeWorkRequest,
        };
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let random = (0..128 * 1024)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        let ascii = random.iter().map(|byte| byte & 0x7f).collect::<Vec<_>>();
        for (scalar, raw, codec) in [
            (RelationalScalarType::Bytea, random, OVERFLOW_CODEC_RAW),
            (RelationalScalarType::Text, ascii, OVERFLOW_CODEC_RAW),
            (
                RelationalScalarType::Bytea,
                vec![7; 128 * 1024],
                OVERFLOW_CODEC_ZSTD,
            ),
            (
                RelationalScalarType::Text,
                "中".repeat(64 * 1024).into_bytes(),
                OVERFLOW_CODEC_ZSTD,
            ),
        ] {
            let encoded =
                encode_overflow_envelope(scalar, &raw, RelationalOverflowConfig::default())
                    .unwrap();
            assert_eq!(encoded.bytes[8], codec);
            let mut ordinary_budget = RelationalHydrationBudget::default();
            let ordinary = decode_overflow_envelope(
                &encoded.reference,
                &encoded.bytes,
                &mut ordinary_budget,
                None,
            )
            .unwrap();
            for (bytes, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
                let governor = RuntimeGovernor::detect(
                    RuntimeGovernorConfig {
                        memory_budget_bytes: Some(bytes),
                        ..RuntimeGovernorConfig::shared_host()
                    },
                    IoConcurrencyBudget::new(2, 1),
                );
                governor.pin_resources();
                let permit = governor
                    .try_admit(RuntimeWorkRequest::background_maintenance(bytes))
                    .unwrap();
                let work = CheckpointWorkContext::new(
                    permit.bind_task_context(RuntimeTaskContext::default()),
                );
                let mut budget = RelationalHydrationBudget::default();
                let before = budget;
                let observer = crate::test_allocator::AllocationObservation::start();
                let result = work.classify(|scoped| {
                    let context = CheckpointDecodeContext {
                        work: scoped.clone(),
                        memory: Default::default(),
                    };
                    let value = decode_overflow_envelope_with_allocations(
                        &encoded.reference,
                        &encoded.bytes,
                        &mut budget,
                        Some(scoped.task_context()),
                        Some(&context),
                    )?;
                    Ok::<_, RelationalError>((value, context.memory.into_inner()))
                });
                let large_allocations = observer.finish();
                if succeeds {
                    let (value, memory) = result.unwrap();
                    assert_eq!(value, ordinary);
                    assert_eq!(budget, ordinary_budget);
                    assert!(matches!(work.task_context().reserve_working_memory(bytes),
                        Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes <= bytes - raw.len() as u64));
                    drop(value);
                    drop(memory);
                } else {
                    assert!(
                        matches!(
                            result,
                            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                                _
                            )))
                        ),
                        "{result:?}"
                    );
                    assert_eq!(large_allocations, 0);
                    assert_eq!(budget, before);
                }
                assert!(matches!(work.task_context().reserve_working_memory(bytes),
                    Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == bytes));
                drop(work);
                drop(permit);
                assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
            }
        }
    }

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
