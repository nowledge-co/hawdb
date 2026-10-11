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

struct Input<'a> {
    inner: SliceDecodeInput<'a>,
    work: Option<CheckpointWorkContext>,
    fail_at: Option<usize>,
}
impl DecodeInput for Input<'_> {
    fn checkpoint_work_context(&self) -> Option<&CheckpointWorkContext> {
        self.work.as_ref()
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn position(&self) -> usize {
        self.inner.position()
    }
    fn read_exact(&mut self, output: &mut [u8]) -> Result<(), RelationalError> {
        if let Some(work) = &self.work {
            work.checkpoint().map_err(work_error)?;
        }
        if self
            .fail_at
            .is_some_and(|at| self.position().saturating_add(output.len()) >= at)
        {
            return Err(RelationalError::Corruption(
                "injected field input failure".into(),
            ));
        }
        self.inner.read_exact(output)
    }
    fn finish(self) -> Result<(), RelationalError> {
        self.inner.finish()
    }
}
fn decoder<'a>(
    bytes: &'a [u8],
    work: Option<CheckpointWorkContext>,
    limits: RelationalDecodeLimits,
) -> Decoder<Input<'a>> {
    Decoder::new(
        Input {
            inner: SliceDecodeInput { bytes, offset: 0 },
            work,
            fail_at: None,
        },
        limits,
        true,
    )
}
fn field(bytes: &[u8]) -> Vec<u8> {
    let mut output = (bytes.len() as u64).to_le_bytes().to_vec();
    output.extend_from_slice(bytes);
    output
}

#[test]
fn checkpoint_units_relational_decode_utf8_preserves_all_bytes_across_split_scalars() {
    let local = scheduler();
    let mut fixtures = vec![String::new(), "a".into(), "界é🙂".repeat(20000)];
    for prefix in [
        BLOCK_BYTES - 3,
        BLOCK_BYTES - 2,
        BLOCK_BYTES - 1,
        BLOCK_BYTES,
        BLOCK_BYTES + 1,
        2 * BLOCK_BYTES - 1,
    ] {
        for scalar in ["é", "界", "🙂"] {
            fixtures.push(format!(
                "{}{}{}",
                "a".repeat(prefix),
                scalar,
                "suffix-界".repeat(37)
            ));
        }
    }
    for expected in fixtures {
        let bytes = field(expected.as_bytes());
        let mut ordinary = decoder(&bytes, None, RelationalDecodeLimits::checkpoint());
        let reference = ordinary.string().unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let mut controlled = decoder(
            &bytes,
            Some(probe.context(local.clone())),
            RelationalDecodeLimits::checkpoint(),
        );
        assert!(controlled.string().unwrap() == reference);
        assert_eq!(controlled.value_bytes, ordinary.value_bytes);
        assert_eq!(controlled.input.position(), ordinary.input.position());
        controlled.finish().unwrap();
        ordinary.finish().unwrap();
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
        probe.assert_released(&local);
    }
}

#[test]
fn checkpoint_units_relational_decode_utf8_preserves_global_error_offsets_and_budget_priority() {
    let local = scheduler();
    let invalid: &[&[u8]] = &[
        &[0x80],
        &[0xc0, 0x80],
        &[0xc2],
        &[0xe2, 0x82],
        &[0xf0, 0x9f, 0x99],
        &[0xed, 0xa0, 0x80],
        &[0xf4, 0x90, 0x80, 0x80],
        &[0xe0, 0x80, 0x80],
        &[0xe2, 0x82, b'x'],
        &[0xf0, 0x9f, 0x99, b'x'],
    ];
    for prefix in [
        0,
        BLOCK_BYTES - 3,
        BLOCK_BYTES - 2,
        BLOCK_BYTES - 1,
        BLOCK_BYTES,
        BLOCK_BYTES + 1,
        2 * BLOCK_BYTES - 1,
    ] {
        for suffix in invalid {
            let mut payload = vec![b'a'; prefix];
            payload.extend_from_slice(suffix);
            let bytes = field(&payload);
            let mut ordinary = decoder(&bytes, None, RelationalDecodeLimits::checkpoint());
            let error = ordinary.string().unwrap_err();
            let probe = Arc::new(CheckpointWorkProbe::default());
            let mut controlled = decoder(
                &bytes,
                Some(probe.context(local.clone())),
                RelationalDecodeLimits::checkpoint(),
            );
            assert_eq!(
                controlled.string().unwrap_err().to_string(),
                error.to_string()
            );
            assert_eq!(controlled.value_bytes, ordinary.value_bytes);
            assert_eq!(controlled.input.position(), ordinary.input.position());
            probe.assert_released(&local);
        }
    }
    let bytes = field(&vec![0x80; BLOCK_BYTES + 17]);
    for (prior, budget) in [(0, 1), (usize::MAX, usize::MAX)] {
        let limits = RelationalDecodeLimits {
            max_record_bytes: budget,
            ..RelationalDecodeLimits::checkpoint()
        };
        let mut ordinary = decoder(&bytes, None, limits);
        ordinary.value_bytes = prior;
        let probe = Arc::new(CheckpointWorkProbe::default());
        let mut controlled = decoder(&bytes, Some(probe.context(local.clone())), limits);
        controlled.value_bytes = prior;
        assert_eq!(
            controlled.string().unwrap_err().to_string(),
            ordinary.string().unwrap_err().to_string()
        );
        assert_eq!(controlled.value_bytes, ordinary.value_bytes);
        probe.assert_released(&local);
    }
    let bytes = field(&vec![0x80; 2 * BLOCK_BYTES + 1]);
    let mut ordinary = decoder(&bytes, None, RelationalDecodeLimits::checkpoint());
    ordinary.input.fail_at = Some(BLOCK_BYTES + 9);
    let probe = Arc::new(CheckpointWorkProbe::default());
    let mut controlled = decoder(
        &bytes,
        Some(probe.context(local.clone())),
        RelationalDecodeLimits::checkpoint(),
    );
    controlled.input.fail_at = ordinary.input.fail_at;
    assert_eq!(
        controlled.string().unwrap_err().to_string(),
        ordinary.string().unwrap_err().to_string()
    );
    assert_eq!(controlled.value_bytes, 0);
    probe.assert_released(&local);
}

#[test]
fn checkpoint_units_relational_decode_length_and_whole_field_truncation_match_ordinary() {
    let local = scheduler();
    let complete = field(&vec![b'a'; BLOCK_BYTES + 17]);
    for phase in 0..2 {
        for truncated in [
            0,
            1,
            7,
            8,
            9,
            BLOCK_BYTES,
            complete.len() - 1,
            complete.len(),
        ] {
            for max in [0, BLOCK_BYTES, 2 * BLOCK_BYTES] {
                let bytes = &complete[..truncated];
                let limits = RelationalDecodeLimits {
                    max_value_bytes: max,
                    ..RelationalDecodeLimits::checkpoint()
                };
                let mut ordinary = decoder(bytes, None, limits);
                let probe = Arc::new(CheckpointWorkProbe::default());
                let mut controlled = decoder(bytes, Some(probe.context(local.clone())), limits);
                let expected = if phase == 0 {
                    ordinary.string().map(String::into_bytes)
                } else {
                    ordinary.bounded_bytes(max, "BYTEA value")
                };
                let actual = if phase == 0 {
                    controlled.string().map(String::into_bytes)
                } else {
                    controlled.bounded_bytes(max, "BYTEA value")
                };
                assert_eq!(
                    actual.map_err(|error| error.to_string()),
                    expected.map_err(|error| error.to_string())
                );
                assert_eq!(controlled.input.position(), ordinary.input.position());
                assert_eq!(controlled.value_bytes, ordinary.value_bytes);
                probe.assert_released(&local);
            }
        }
    }
}

fn schema() -> RelationalTableSchema {
    let mut schema = RelationalTableSchema {
        name: "table-界".repeat(10000),
        columns: Vec::new(),
        primary_key: vec!["id".into()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    };
    for id in 0..1025 {
        let name = if id == 0 {
            "id".into()
        } else {
            format!("column-{id}-界")
        };
        let default = if id == 0 {
            None
        } else {
            Some(RelationalColumnDefault::Literal(RelationalValue::Text(
                if id == 1024 {
                    "🙂界".repeat(10000)
                } else {
                    format!("default-{id}")
                },
            )))
        };
        schema.columns.push(RelationalColumnSchema {
            name: name.clone(),
            scalar_type: if id == 0 {
                RelationalScalarType::BigInt
            } else {
                RelationalScalarType::Text
            },
            nullable: false,
            default,
        });
        if id != 0 {
            schema.unique_constraints.push(vec![name.clone()]);
            schema.foreign_keys.push(RelationalForeignKeySchema {
                columns: vec![name.clone()],
                referenced_table: "parents".into(),
                referenced_columns: vec!["id".into()],
                on_delete: RelationalReferentialAction::Restrict,
                on_update: RelationalReferentialAction::Cascade,
            });
            schema.indexes.push(RelationalIndexSchema {
                name: format!("index-{id}"),
                columns: vec![name],
                unique: id % 2 == 0,
            });
        }
    }
    schema
}
fn payloads() -> [Vec<u8>; 4] {
    let bytes = field(&vec![17; 2 * BLOCK_BYTES + 17]);
    let text = field("🙂界".repeat(20000).as_bytes());
    let mut encoder = Encoder::default();
    encoder.table_schema(&schema()).unwrap();
    let schema = encoder.finish();
    let values = (0..1025)
        .map(|id| match id % 6 {
            0 => RelationalValue::BigInt(id),
            1 => RelationalValue::Boolean(true),
            2 => RelationalValue::DoublePrecision(id as f64 / 3.0),
            3 => RelationalValue::Text(if id == 1023 {
                "界🙂".repeat(10000)
            } else {
                format!("value-{id}")
            }),
            4 => RelationalValue::Bytea(vec![0; if id == 1024 { BLOCK_BYTES + 1 } else { 3 }]),
            5 => RelationalValue::Uuid(Uuid::from_u128(id as u128)),
            _ => unreachable!(),
        })
        .collect();
    let mut encoder = Encoder::default();
    encoder.row(&RelationalRow::new(values)).unwrap();
    let row = encoder.finish();
    [bytes, text, schema, row]
}
#[derive(Debug, PartialEq, Eq)]
enum Decoded {
    Bytes(Vec<u8>),
    Text(String),
    Schema(RelationalTableSchema),
    Row(RelationalRow),
}
fn run(decoder: &mut Decoder<Input<'_>>, phase: usize) -> Result<Decoded, RelationalError> {
    match phase {
        0 => decoder
            .bounded_bytes(decoder.limits.max_value_bytes, "BYTEA value")
            .map(Decoded::Bytes),
        1 => decoder.string().map(Decoded::Text),
        2 => decoder.table_schema().map(Decoded::Schema),
        3 => decoder.row().map(Decoded::Row),
        _ => unreachable!(),
    }
}

#[test]
fn checkpoint_units_relational_decode_wide_schema_row_and_owned_vector_transfer_preserve_values() {
    let local = scheduler();
    for (phase, bytes) in payloads().iter().enumerate() {
        let mut ordinary = decoder(bytes, None, RelationalDecodeLimits::checkpoint());
        let expected = run(&mut ordinary, phase).unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let mut controlled = decoder(
            bytes,
            Some(probe.context(local.clone())),
            RelationalDecodeLimits::checkpoint(),
        );
        assert!(
            run(&mut controlled, phase).unwrap() == expected,
            "all values and schema fields must match: {phase}"
        );
        assert_eq!(controlled.values, ordinary.values);
        assert_eq!(controlled.value_bytes, ordinary.value_bytes);
        controlled.finish().unwrap();
        ordinary.finish().unwrap();
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&local);
    }
    let values = (0..1025).map(RelationalValue::BigInt).collect::<Vec<_>>();
    let pointer = values.as_ptr();
    let row = RelationalRow::from_checkpoint_values(values);
    assert_eq!(
        row.values().as_ptr(),
        pointer,
        "private transfer must preserve the owned allocation"
    );
    let retained = row.clone();
    assert!(Arc::ptr_eq(&row.values, &retained.values));
    let mut changed = row.clone();
    Arc::make_mut(&mut changed.values)[0] = RelationalValue::BigInt(-1);
    assert_eq!(row.values()[0], RelationalValue::BigInt(0));
    assert_eq!(retained.values()[0], RelationalValue::BigInt(0));
    assert_eq!(changed.values()[0], RelationalValue::BigInt(-1));
    let mut spare = Vec::with_capacity(2048);
    spare.push(RelationalValue::BigInt(1));
    let public = RelationalRow::new(spare);
    assert_eq!(public.values.capacity(), public.values().len());
}

#[test]
fn checkpoint_units_relational_decode_overflow_hash_and_retention_match_ordinary_and_cancel_each_unit(
) {
    let local = scheduler();
    let mut seed = 97u64;
    let raw = (0..2 * BLOCK_BYTES + 17)
        .map(|_| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            (seed >> 32) as u8
        })
        .collect::<Vec<_>>();
    let bytes = field(&raw);
    for retain in [false, true] {
        let mut ordinary = decoder(&bytes, None, RelationalDecodeLimits::checkpoint());
        ordinary.retain_overflow_bytes = retain;
        let expected = ordinary.overflow_segment().unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let mut controlled = decoder(
            &bytes,
            Some(probe.context(local.clone())),
            RelationalDecodeLimits::checkpoint(),
        );
        controlled.retain_overflow_bytes = retain;
        let actual = controlled.overflow_segment().unwrap();
        assert_eq!(actual.payload_offset, expected.payload_offset);
        assert_eq!(actual.len, expected.len);
        assert_eq!(actual.digest, integrity_digest(&raw));
        assert_eq!(actual.digest, expected.digest);
        assert_eq!(actual.bytes, expected.bytes);
        let total = probe.completed.load(Ordering::SeqCst);
        assert_eq!(total, 1 + raw.len().div_ceil(BLOCK_BYTES));
        probe.assert_released(&local);
        for cancel_at in 1..=total {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(cancel_at, Ordering::SeqCst);
            let mut controlled = decoder(
                &bytes,
                Some(probe.context(local.clone())),
                RelationalDecodeLimits::checkpoint(),
            );
            controlled.retain_overflow_bytes = retain;
            let error = controlled
                .overflow_segment()
                .err()
                .expect("overflow decode must be stopped");
            assert!(
                matches!(error,RelationalError::Admission(ref text) if text.contains("stopped")),
                "{error:?}"
            );
            probe.assert_released(&local);
            let retry = Arc::new(CheckpointWorkProbe::default());
            let mut controlled = decoder(
                &bytes,
                Some(retry.context(local.clone())),
                RelationalDecodeLimits::checkpoint(),
            );
            controlled.retain_overflow_bytes = retain;
            let recovered = controlled.overflow_segment().unwrap();
            assert_eq!(recovered.digest, expected.digest);
            assert_eq!(recovered.bytes, expected.bytes);
            retry.assert_released(&local);
        }
    }
}

#[test]
fn checkpoint_units_relational_decode_cancellation_preserves_source_and_retries_complete_fields() {
    let local = scheduler();
    for (phase, bytes) in payloads().iter().enumerate() {
        let source_digest = integrity_digest(bytes);
        let mut ordinary = decoder(bytes, None, RelationalDecodeLimits::checkpoint());
        let expected = run(&mut ordinary, phase).unwrap();
        let reference = Arc::new(CheckpointWorkProbe::default());
        let mut controlled = decoder(
            bytes,
            Some(reference.context(local.clone())),
            RelationalDecodeLimits::checkpoint(),
        );
        run(&mut controlled, phase).unwrap();
        let total = reference.completed.load(Ordering::SeqCst);
        reference.assert_released(&local);
        let boundaries = [1, 2, 17, 256, 257, total / 2, total - 1, total]
            .into_iter()
            .filter(|at| *at > 0 && *at <= total)
            .collect::<BTreeSet<_>>();
        for cancel_at in boundaries {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(cancel_at, Ordering::SeqCst);
            let mut controlled = decoder(
                bytes,
                Some(probe.context(local.clone())),
                RelationalDecodeLimits::checkpoint(),
            );
            let error = run(&mut controlled, phase).unwrap_err();
            assert!(
                matches!(error,RelationalError::Admission(ref text) if text.contains("stopped")),
                "{error:?}"
            );
            probe.assert_released(&local);
            assert_eq!(integrity_digest(bytes), source_digest);
            let retry = Arc::new(CheckpointWorkProbe::default());
            let mut controlled = decoder(
                bytes,
                Some(retry.context(local.clone())),
                RelationalDecodeLimits::checkpoint(),
            );
            assert!(run(&mut controlled, phase).unwrap() == expected);
            controlled.finish().unwrap();
            retry.assert_released(&local);
        }
    }
}

#[test]
fn checkpoint_units_relational_decode_denies_before_payload_and_retries_complete_fields() {
    let local = scheduler();
    for (phase, bytes) in payloads().iter().enumerate() {
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(local.clone());
        let held = local
            .try_start(WorkRequest::background(WorkClass::Mutation, 1))
            .unwrap();
        let mut controlled = decoder(bytes, Some(work), RelationalDecodeLimits::checkpoint());
        let error = run(&mut controlled, phase).unwrap_err();
        assert!(
            matches!(error,RelationalError::Admission(ref text) if text.contains("admission deferred")),
            "{error:?}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
        assert_eq!(
            controlled.input.position(),
            if phase == 2 {
                8
            } else if phase == 3 {
                4
            } else {
                8
            }
        );
        drop(held);
        probe.assert_released(&local);
        let mut ordinary = decoder(bytes, None, RelationalDecodeLimits::checkpoint());
        let expected = run(&mut ordinary, phase).unwrap();
        let retry = Arc::new(CheckpointWorkProbe::default());
        let mut controlled = decoder(
            bytes,
            Some(retry.context(local.clone())),
            RelationalDecodeLimits::checkpoint(),
        );
        assert!(run(&mut controlled, phase).unwrap() == expected);
        controlled.finish().unwrap();
        retry.assert_released(&local);
    }
}
