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
use crate::background::CheckpointWorkProbe;
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler, WorkClass, WorkRequest};
use std::sync::atomic::Ordering;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    })
}

fn envelope(
    raw: &[u8],
    scalar: RelationalScalarType,
    codec: u8,
) -> (RelationalOverflowRef, Vec<u8>) {
    let payload = if codec == OVERFLOW_CODEC_RAW {
        raw.to_vec()
    } else {
        zstd::stream::encode_all(raw, 3).unwrap()
    };
    let mut bytes = Vec::new();
    bytes.extend_from_slice(OVERFLOW_MAGIC);
    bytes.extend_from_slice(&[codec, encode_scalar_type(scalar).unwrap(), 0, 0]);
    bytes.extend_from_slice(&(raw.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&crc32c(raw).get().to_le_bytes());
    bytes.extend_from_slice(&payload);
    let reference = RelationalOverflowRef {
        digest: integrity_digest(&bytes).sha256,
        scalar_type: scalar,
        compressed_bytes: payload.len() as u64,
        uncompressed_bytes: raw.len() as u64,
    };
    (reference, bytes)
}

fn parity(reference: &RelationalOverflowRef, bytes: &[u8], initial: RelationalHydrationBudget) {
    let mut ordinary_budget = initial;
    let ordinary = decode_overflow_envelope(reference, bytes, &mut ordinary_budget, None)
        .map(|_| ())
        .map_err(|error| error.to_string());
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let mut controlled_budget = initial;
    let controlled = validate_overflow_envelope_with_work_context(
        reference,
        bytes,
        &mut controlled_budget,
        &probe.context(local.clone()),
    )
    .map_err(|error| error.to_string());
    assert_eq!(controlled, ordinary);
    assert_eq!(controlled_budget, ordinary_budget);
    probe.assert_released(&local);
    assert!(probe.peak_units.load(Ordering::SeqCst) <= 1);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
}

#[test]
fn checkpoint_units_overflow_validation_matches_full_decode_for_both_codecs_and_scalars() {
    for codec in [OVERFLOW_CODEC_RAW, OVERFLOW_CODEC_ZSTD] {
        let mut binary = Vec::with_capacity(3 * DECODE_CHUNK_BYTES + 17);
        let mut random = 0x1234_5678u32;
        for _ in 0..binary.capacity() {
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            binary.push(random as u8);
        }
        for raw in [binary, vec![0; 5 * DECODE_CHUNK_BYTES + 19], vec![42]] {
            let (reference, bytes) = envelope(&raw, RelationalScalarType::Bytea, codec);
            let mut budget = RelationalHydrationBudget::default();
            assert_eq!(
                decode_overflow_envelope(&reference, &bytes, &mut budget, None).unwrap(),
                RelationalValue::Bytea(raw)
            );
            parity(&reference, &bytes, RelationalHydrationBudget::default());
        }
        for prefix in [
            DECODE_CHUNK_BYTES - 3,
            DECODE_CHUNK_BYTES - 2,
            DECODE_CHUNK_BYTES - 1,
        ] {
            for scalar in ["é", "界", "🙂"] {
                let text = format!("{}{}{}", "a".repeat(prefix), scalar, "é界🙂".repeat(25000));
                let (reference, bytes) =
                    envelope(text.as_bytes(), RelationalScalarType::Text, codec);
                let mut budget = RelationalHydrationBudget::default();
                assert_eq!(
                    decode_overflow_envelope(&reference, &bytes, &mut budget, None).unwrap(),
                    RelationalValue::Text(text)
                );
                parity(&reference, &bytes, RelationalHydrationBudget::default());
            }
        }
    }
}

#[test]
fn checkpoint_units_overflow_validation_preserves_global_utf8_diagnostics_and_checksum_priority() {
    let invalid: &[&[u8]] = &[
        &[0xff],
        &[0x80],
        &[0xc0, 0xaf],
        &[0xed, 0xa0, 0x80],
        &[0xf4, 0x90, 0x80, 0x80],
        &[0xe2, 0x82, b'x'],
        &[0xe2],
        &[0xe2, 0x82],
        &[0xf0, 0x9f, 0x99],
    ];
    for codec in [OVERFLOW_CODEC_RAW, OVERFLOW_CODEC_ZSTD] {
        for offset in [
            0,
            DECODE_CHUNK_BYTES - 3,
            DECODE_CHUNK_BYTES - 2,
            DECODE_CHUNK_BYTES - 1,
            DECODE_CHUNK_BYTES,
            2 * DECODE_CHUNK_BYTES - 1,
        ] {
            for pattern in invalid {
                for suffix in [false, true] {
                    let mut raw = vec![b'a'; offset];
                    raw.extend_from_slice(pattern);
                    if suffix {
                        raw.extend_from_slice(&vec![b'z'; DECODE_CHUNK_BYTES + 5]);
                    }
                    let (mut reference, mut bytes) =
                        envelope(&raw, RelationalScalarType::Text, codec);
                    parity(&reference, &bytes, RelationalHydrationBudget::default());
                    bytes[28] ^= 1;
                    reference.digest = integrity_digest(&bytes).sha256;
                    parity(&reference, &bytes, RelationalHydrationBudget::default());
                    let error = decode_overflow_envelope(
                        &reference,
                        &bytes,
                        &mut RelationalHydrationBudget::default(),
                        None,
                    )
                    .unwrap_err()
                    .to_string();
                    assert!(error.contains("decoded length or checksum mismatch"));
                    parity(
                        &reference,
                        &bytes,
                        RelationalHydrationBudget {
                            max_decompressed_bytes: 0,
                            ..RelationalHydrationBudget::default()
                        },
                    );
                }
            }
        }
    }
}

#[test]
fn checkpoint_units_overflow_validation_preserves_header_decode_and_budget_failures() {
    for codec in [OVERFLOW_CODEC_RAW, OVERFLOW_CODEC_ZSTD] {
        let raw = vec![b'x'; 2 * DECODE_CHUNK_BYTES + 7];
        let (reference, bytes) = envelope(&raw, RelationalScalarType::Text, codec);
        for length in [0, 7, 31, 32, bytes.len() - 1] {
            parity(
                &reference,
                &bytes[..length],
                RelationalHydrationBudget::default(),
            );
        }
        for at in [0, 8, 9, 10, 12, 20, 28, 32, bytes.len() - 1] {
            let mut corrupted = bytes.clone();
            corrupted[at] ^= 0xff;
            parity(&reference, &corrupted, RelationalHydrationBudget::default());
            let mut rebound = reference;
            rebound.digest = integrity_digest(&corrupted).sha256;
            parity(&rebound, &corrupted, RelationalHydrationBudget::default());
        }
        for declared in [0, 1, raw.len() - 1, raw.len() + 1] {
            let mut corrupted = bytes.clone();
            corrupted[12..20].copy_from_slice(&(declared as u64).to_le_bytes());
            let rebound = RelationalOverflowRef {
                uncompressed_bytes: declared as u64,
                digest: integrity_digest(&corrupted).sha256,
                ..reference
            };
            parity(&rebound, &corrupted, RelationalHydrationBudget::default());
        }
        for initial in [
            RelationalHydrationBudget {
                max_compressed_bytes: 0,
                ..RelationalHydrationBudget::default()
            },
            RelationalHydrationBudget {
                max_decompressed_bytes: 0,
                ..RelationalHydrationBudget::default()
            },
            RelationalHydrationBudget {
                max_memory_bytes: 0,
                ..RelationalHydrationBudget::default()
            },
            RelationalHydrationBudget {
                compressed_bytes: usize::MAX,
                ..RelationalHydrationBudget::default()
            },
            RelationalHydrationBudget {
                decompressed_bytes: usize::MAX,
                ..RelationalHydrationBudget::default()
            },
            RelationalHydrationBudget {
                memory_bytes: usize::MAX,
                ..RelationalHydrationBudget::default()
            },
            RelationalHydrationBudget {
                compressed_bytes: 17,
                decompressed_bytes: 19,
                memory_bytes: 23,
                ..RelationalHydrationBudget::default()
            },
        ] {
            parity(&reference, &bytes, initial);
        }
    }
}

#[test]
fn checkpoint_units_overflow_validation_cancels_each_actual_unit_without_consuming_budget() {
    for codec in [OVERFLOW_CODEC_RAW, OVERFLOW_CODEC_ZSTD] {
        for scalar in [RelationalScalarType::Text, RelationalScalarType::Bytea] {
            let (reference, bytes) =
                envelope(&vec![b'a'; 3 * DECODE_CHUNK_BYTES + 17], scalar, codec);
            let initial = RelationalHydrationBudget::default();
            let local = scheduler();
            let baseline = Arc::new(CheckpointWorkProbe::default());
            let mut baseline_budget = initial;
            validate_overflow_envelope_with_work_context(
                &reference,
                &bytes,
                &mut baseline_budget,
                &baseline.context(local.clone()),
            )
            .unwrap();
            let units = baseline.completed.load(Ordering::SeqCst);
            assert!(units > 3);
            baseline.assert_released(&local);
            let digest = integrity_digest(&bytes);
            for stop in 1..=units {
                let cancelled = Arc::new(CheckpointWorkProbe::default());
                cancelled.cancel_after.store(stop, Ordering::SeqCst);
                let mut budget = initial;
                let error = validate_overflow_envelope_with_work_context(
                    &reference,
                    &bytes,
                    &mut budget,
                    &cancelled.context(local.clone()),
                )
                .unwrap_err();
                assert!(
                    error.to_string().contains("checkpoint build stopped"),
                    "{error:?}"
                );
                assert_eq!(budget, initial);
                assert_eq!(integrity_digest(&bytes), digest);
                cancelled.assert_released(&local);
                parity(&reference, &bytes, initial);
            }
        }
    }
}

#[test]
fn checkpoint_units_overflow_validation_denies_before_payload_then_retries_complete_input() {
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let held = local
        .try_start(WorkRequest::background(WorkClass::Mutation, 1))
        .unwrap();
    let (reference, bytes) = envelope(
        &vec![b'x'; 3 * DECODE_CHUNK_BYTES],
        RelationalScalarType::Text,
        OVERFLOW_CODEC_ZSTD,
    );
    let initial = RelationalHydrationBudget::default();
    let mut budget = initial;
    let error =
        validate_overflow_envelope_with_work_context(&reference, &bytes, &mut budget, &work)
            .unwrap_err();
    assert!(error.to_string().contains("admission deferred"));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
    assert_eq!(budget, initial);
    drop(held);
    probe.assert_released(&local);
    parity(&reference, &bytes, initial);
}
