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
use crate::relational::{
    RelationalHydrationBudget, RelationalMutationLimits, RelationalOverflowConfig, RelationalStore,
};
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
use std::sync::atomic::Ordering;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    })
}

struct Fixture {
    state: RelationalState,
    expected: Vec<RelationalRow>,
    directory: std::path::PathBuf,
    checkpoint: Vec<u8>,
}

impl Fixture {
    fn new() -> Self {
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "hawdb-relational-controlled-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let store = RelationalStore::with_overflow_config(
            RelationalMutationLimits::default(),
            RelationalOverflowConfig {
                threshold_bytes: 512,
                ..RelationalOverflowConfig::default()
            },
        );
        let column = |name: &str, scalar_type, nullable, default| RelationalColumnSchema {
            name: name.into(),
            scalar_type,
            nullable,
            default,
        };
        let parents = RelationalTableSchema {
            name: "parents".into(),
            columns: vec![column("id", RelationalScalarType::BigInt, false, None)],
            primary_key: vec!["id".into()],
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        };
        let schema = RelationalTableSchema {
            name: "messages".into(),
            columns: vec![
                column(
                    "id",
                    RelationalScalarType::BigInt,
                    false,
                    Some(RelationalColumnDefault::Literal(RelationalValue::BigInt(
                        -1,
                    ))),
                ),
                column("parent", RelationalScalarType::BigInt, true, None),
                column(
                    "text",
                    RelationalScalarType::Text,
                    false,
                    Some(RelationalColumnDefault::Literal(RelationalValue::Text(
                        "default-界".repeat(7000),
                    ))),
                ),
                column(
                    "bytes",
                    RelationalScalarType::Bytea,
                    false,
                    Some(RelationalColumnDefault::Literal(RelationalValue::Bytea(
                        vec![0; 65537],
                    ))),
                ),
                column(
                    "flag",
                    RelationalScalarType::Boolean,
                    true,
                    Some(RelationalColumnDefault::Literal(RelationalValue::Null)),
                ),
                column(
                    "amount",
                    RelationalScalarType::DoublePrecision,
                    false,
                    Some(RelationalColumnDefault::Literal(
                        RelationalValue::DoublePrecision(1.5),
                    )),
                ),
                column(
                    "uuid",
                    RelationalScalarType::Uuid,
                    false,
                    Some(RelationalColumnDefault::UuidV7),
                ),
            ],
            primary_key: vec!["id".into()],
            unique_constraints: vec![vec!["id".into()]],
            foreign_keys: vec![RelationalForeignKeySchema {
                columns: vec!["parent".into()],
                referenced_table: "parents".into(),
                referenced_columns: vec!["id".into()],
                on_delete: RelationalReferentialAction::Restrict,
                on_update: RelationalReferentialAction::Cascade,
            }],
            indexes: vec![RelationalIndexSchema {
                name: "flag-界".repeat(10000),
                columns: vec!["flag".into()],
                unique: false,
            }],
        };
        store
            .commit(
                RelationalTransaction {
                    writes: vec![
                        RelationalWrite::CreateTable(parents),
                        RelationalWrite::CreateTable(schema),
                    ],
                },
                |_, _| Ok(()),
            )
            .unwrap();
        let mut random = 97u64;
        let expected = (0..512)
            .map(|id| {
                let bytes = if id == 257 {
                    (0..131073)
                        .map(|_| {
                            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                            (random >> 32) as u8
                        })
                        .collect()
                } else {
                    vec![id as u8; 257]
                };
                RelationalRow::new(vec![
                    RelationalValue::BigInt(id),
                    RelationalValue::Null,
                    RelationalValue::Text(if id == 511 {
                        "界".repeat(25000)
                    } else {
                        format!("row-{id}-界")
                    }),
                    RelationalValue::Bytea(bytes),
                    if id % 7 == 0 {
                        RelationalValue::Null
                    } else {
                        RelationalValue::Boolean(id % 2 == 0)
                    },
                    RelationalValue::DoublePrecision(id as f64 / 3.0),
                    RelationalValue::Uuid(Uuid::from_u128(id as u128 + 1)),
                ])
            })
            .collect::<Vec<_>>();
        store
            .commit(
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "messages".into(),
                        rows: expected.clone(),
                        mode: RelationalInsertMode::Error,
                    }],
                },
                |_, _| Ok(()),
            )
            .unwrap();
        let snapshot = store.snapshot().unwrap();
        let state = snapshot.value().clone();
        let checkpoint = encode_relational_checkpoint(41, &state).unwrap();
        std::fs::write(directory.join("source.hawdb"), &checkpoint).unwrap();
        Self {
            state,
            expected,
            directory,
            checkpoint,
        }
    }

    fn file_state(&self) -> RelationalState {
        decode_relational_checkpoint_file(
            &self.directory.join("source.hawdb"),
            RelationalDecodeLimits::checkpoint(),
        )
        .unwrap()
        .state
    }

    fn assert_rows(&self, state: &RelationalState) {
        assert_eq!(state.row_count("messages"), 512);
        for (id, expected) in self.expected.iter().enumerate() {
            let actual = state
                .hydrate_row(
                    "messages",
                    &RelationalKey(vec![RelationalValue::BigInt(id as i64)]),
                    &mut RelationalHydrationBudget::default(),
                )
                .unwrap()
                .unwrap();
            assert!(actual == *expected, "complete logical row differs: {id}");
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn encode(
    state: &RelationalState,
    limit: usize,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalError> {
    let mut writer = Cursor::new(Vec::new());
    encode_relational_checkpoint_with_work_context(
        &mut writer,
        41,
        state,
        limit,
        CheckpointOutputIo::Memory,
        work,
    )?;
    Ok(writer.into_inner())
}

#[test]
fn checkpoint_units_relational_image_matches_ordinary_bytes_for_inline_and_file_overflow() {
    let fixture = Fixture::new();
    let local = scheduler();
    for state in [&fixture.state, &fixture.file_state()] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = encode(
            state,
            RelationalDecodeLimits::checkpoint().max_record_bytes,
            &probe.context(local.clone()),
        )
        .unwrap();
        assert!(
            actual == fixture.checkpoint,
            "complete legacy checkpoint bytes differ"
        );
        assert!(probe.completed.load(Ordering::SeqCst) > 6000);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&local);
        let decoded =
            decode_relational_checkpoint(&actual, RelationalDecodeLimits::checkpoint()).unwrap();
        assert_eq!(decoded.epoch, 41);
        fixture.assert_rows(&decoded.state);
        if state.file_backed_overflow_segment_count() == 0 {
            assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
        } else {
            assert!(probe.io_waves.load(Ordering::SeqCst) > 4);
        }
    }
}

#[test]
fn checkpoint_units_relational_image_cancels_each_read_wave_and_cpu_then_retries_complete() {
    let fixture = Fixture::new();
    let state = fixture.file_state();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = encode(
        &state,
        fixture.checkpoint.len(),
        &probe.context(local.clone()),
    )
    .unwrap();
    assert!(actual == fixture.checkpoint);
    let units = probe.completed.load(Ordering::SeqCst);
    let waves = probe.io_waves.load(Ordering::SeqCst);
    probe.assert_released(&local);
    for wave in 1..=waves {
        // Each measured cold cut retains a fresh native-handle lookup.
        let state = fixture.file_state();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_on_io_wave.store(wave, Ordering::SeqCst);
        let error = encode(
            &state,
            fixture.checkpoint.len(),
            &probe.context(local.clone()),
        )
        .unwrap_err();
        assert!(
            matches!(error,RelationalError::Admission(ref text) if text.contains("cancelled")),
            "{error:?}"
        );
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), wave);
        probe.assert_released(&local);
        assert!(
            std::fs::read(fixture.directory.join("source.hawdb")).unwrap() == fixture.checkpoint
        );
    }
    for limit in [1, 17, 511, 1025, units / 2, units - 1, units] {
        // Each measured cold cut retains a fresh native-handle lookup.
        let state = fixture.file_state();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        let error = encode(
            &state,
            fixture.checkpoint.len(),
            &probe.context(local.clone()),
        )
        .unwrap_err();
        assert!(
            matches!(error,RelationalError::Admission(ref text) if text.contains("cancelled")),
            "{error:?}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), limit);
        probe.assert_released(&local);
    }
    let probe = Arc::new(CheckpointWorkProbe::default());
    let retry = encode(
        &state,
        fixture.checkpoint.len(),
        &probe.context(local.clone()),
    )
    .unwrap();
    assert!(retry == fixture.checkpoint);
    probe.assert_released(&local);
    fixture.assert_rows(
        &decode_relational_checkpoint(&retry, RelationalDecodeLimits::checkpoint())
            .unwrap()
            .state,
    );
}

#[test]
fn checkpoint_units_relational_image_preserves_budget_and_reachability_error_order() {
    let fixture = Fixture::new();
    let local = scheduler();
    for scenario in 0..5 {
        let mut state = fixture.state.clone();
        let limit = match scenario {
            0 => HEADER_BYTES - 1,
            1 => fixture.checkpoint.len() - 1,
            _ => RelationalDecodeLimits::checkpoint().max_record_bytes,
        };
        match scenario {
            2 => {
                let digest = *state.overflow_segments.keys().next().unwrap();
                state.overflow_segments.remove(&digest);
            }
            3 => {
                state.segments.remove("messages");
            }
            4 => {
                let digest = *state.overflow_segments.keys().next().unwrap();
                state.overflow_segments.insert(
                    digest,
                    RelationalOverflowSegment::Inline(Arc::from(b"invalid digest".as_slice())),
                );
            }
            _ => {}
        }
        let expected =
            encode_relational_checkpoint_to_writer(&mut Cursor::new(Vec::new()), 41, &state, limit)
                .unwrap_err();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = encode(&state, limit, &probe.context(local.clone())).unwrap_err();
        assert_eq!(actual, expected, "scenario={scenario}");
        probe.assert_released(&local);
    }
}

#[test]
fn checkpoint_units_relational_wide_schema_defaults_keys_and_foreign_metadata_preserve_bytes() {
    let mut schema = RelationalTableSchema {
        name: "schema-界".repeat(20000),
        columns: Vec::new(),
        primary_key: vec!["id".into()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    };
    for id in 0..1025 {
        let name = format!(
            "column-{id}-{}",
            "界".repeat(if id == 1024 { 25000 } else { 16 })
        );
        schema.columns.push(RelationalColumnSchema {
            name: name.clone(),
            scalar_type: RelationalScalarType::Text,
            nullable: true,
            default: Some(RelationalColumnDefault::Literal(RelationalValue::Text(
                "default-界".repeat(if id % 257 == 0 { 10000 } else { 1 }),
            ))),
        });
        schema.unique_constraints.push(vec![name.clone()]);
        schema.foreign_keys.push(RelationalForeignKeySchema {
            columns: vec![name.clone()],
            referenced_table: format!("parent-{id}-界"),
            referenced_columns: vec![name.clone()],
            on_delete: RelationalReferentialAction::NoAction,
            on_update: RelationalReferentialAction::Restrict,
        });
        schema.indexes.push(RelationalIndexSchema {
            name: format!("index-{id}-界"),
            columns: vec![name],
            unique: id % 2 == 0,
        });
    }
    let mut expected = Encoder::default();
    expected.table_schema(&schema).unwrap();
    let expected = expected.finish();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let mut actual = Encoder::default();
    table_schema_with_work_context(&mut actual, &schema, &probe.context(local.clone())).unwrap();
    assert!(actual.finish() == expected, "full wide schema bytes differ");
    assert!(probe.completed.load(Ordering::SeqCst) > 10000);
    probe.assert_released(&local);
}

#[test]
fn checkpoint_units_relational_mount_preserves_complete_rows_schemas_and_index_modes() {
    let fixture = Fixture::new();
    let path = fixture.directory.join("source.hawdb");
    for index_load in [
        RelationalCheckpointIndexLoad::MaterializedPostings,
        RelationalCheckpointIndexLoad::OmitMaterializedPostings,
    ] {
        let expected = decode_relational_checkpoint_file_with_index_load(
            &path,
            RelationalDecodeLimits::checkpoint(),
            index_load,
        )
        .unwrap();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = decode_relational_checkpoint_file_with_work_context(
            &path,
            RelationalDecodeLimits::checkpoint(),
            index_load,
            &probe.context(local.clone()),
        )
        .unwrap();
        assert_eq!(actual.epoch, expected.epoch);
        assert_eq!(
            actual.state.schemas.iter().collect::<Vec<_>>(),
            expected.state.schemas.iter().collect::<Vec<_>>()
        );
        assert_eq!(actual.state.file_backed_overflow_segment_count(), 2);
        assert_eq!(
            actual.state.materialized_index_postings_resident,
            expected.state.materialized_index_postings_resident
        );
        for name in actual.state.schemas.keys() {
            let actual_indexes = &actual.state.segments.get(name).unwrap().indexes;
            let expected_indexes = &expected.state.segments.get(name).unwrap().indexes;
            assert_eq!(
                actual_indexes.keys().collect::<Vec<_>>(),
                expected_indexes.keys().collect::<Vec<_>>()
            );
            let postings = |index: &crate::relational::RelationalIndexPages| {
                index
                    .pages
                    .iter()
                    .flat_map(|page| page.iter())
                    .map(|(key, postings)| {
                        (key.clone(), postings.iter().cloned().collect::<Vec<_>>())
                    })
                    .collect::<Vec<_>>()
            };
            for (name, index) in actual_indexes {
                assert_eq!(
                    postings(index),
                    postings(expected_indexes.get(name).unwrap())
                );
            }
        }
        assert_eq!(
            probe.io_waves.load(Ordering::SeqCst),
            3 + (fixture.checkpoint.len() - HEADER_BYTES).div_ceil(64 * 1024)
        );
        assert!(probe.completed.load(Ordering::SeqCst) > 6000);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&local);
        fixture.assert_rows(&actual.state);
        assert!(std::fs::read(&path).unwrap() == fixture.checkpoint);
    }
}

#[test]
fn checkpoint_units_relational_mount_cancels_each_io_and_cpu_then_reopens_complete() {
    let fixture = Fixture::new();
    let path = fixture.directory.join("source.hawdb");
    let limits = RelationalDecodeLimits::checkpoint();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let recovered = decode_relational_checkpoint_file_with_work_context(
        &path,
        limits,
        RelationalCheckpointIndexLoad::MaterializedPostings,
        &probe.context(local.clone()),
    )
    .unwrap();
    fixture.assert_rows(&recovered.state);
    let waves = probe.io_waves.load(Ordering::SeqCst);
    let units = probe.completed.load(Ordering::SeqCst);
    probe.assert_released(&local);
    for wave in 1..=waves {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_on_io_wave.store(wave, Ordering::SeqCst);
        let error = decode_relational_checkpoint_file_with_work_context(
            &path,
            limits,
            RelationalCheckpointIndexLoad::MaterializedPostings,
            &probe.context(local.clone()),
        )
        .unwrap_err();
        assert!(
            matches!(error, RelationalError::Admission(ref text) if text.contains("cancelled")),
            "{error:?}"
        );
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), wave);
        probe.assert_released(&local);
        assert!(std::fs::read(&path).unwrap() == fixture.checkpoint);
    }
    for limit in [1, 17, 511, 1025, units / 2, units - 1, units] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        let error = decode_relational_checkpoint_file_with_work_context(
            &path,
            limits,
            RelationalCheckpointIndexLoad::MaterializedPostings,
            &probe.context(local.clone()),
        )
        .unwrap_err();
        assert!(
            matches!(error, RelationalError::Admission(ref text) if text.contains("cancelled")),
            "{error:?}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), limit);
        probe.assert_released(&local);
    }
    let probe = Arc::new(CheckpointWorkProbe::default());
    let retry = decode_relational_checkpoint_file_with_work_context(
        &path,
        limits,
        RelationalCheckpointIndexLoad::MaterializedPostings,
        &probe.context(local.clone()),
    )
    .unwrap();
    probe.assert_released(&local);
    fixture.assert_rows(&retry.state);
    assert!(std::fs::read(&path).unwrap() == fixture.checkpoint);
}

#[test]
fn checkpoint_units_relational_mount_matches_ordinary_corruption_and_budget_errors() {
    let fixture = Fixture::new();
    let path = fixture.directory.join("corrupt.hawdb");
    let local = scheduler();
    for scenario in 0..13 {
        let mut bytes = fixture.checkpoint.clone();
        let mut limits = RelationalDecodeLimits::checkpoint();
        match scenario {
            0 => bytes.truncate(HEADER_BYTES - 1),
            1 => bytes[0] ^= 1,
            2 => bytes[8] ^= 1,
            3 => bytes[10] ^= 1,
            4 => {
                bytes.pop();
            }
            5 => bytes[28] ^= 1,
            6 => bytes[32] ^= 1,
            7 => bytes[HEADER_BYTES + 4 + 8] = 255,
            8 => limits.max_record_bytes = bytes.len() - 1,
            9 => limits.max_tables = 1,
            10 => limits.max_rows = 511,
            11 => limits.max_values = 1,
            12 => limits.max_overflow_bytes = 1,
            _ => unreachable!(),
        }
        std::fs::write(&path, &bytes).unwrap();
        let expected = decode_relational_checkpoint_file_with_index_load(
            &path,
            limits,
            RelationalCheckpointIndexLoad::MaterializedPostings,
        )
        .unwrap_err();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let error = decode_relational_checkpoint_file_with_work_context(
            &path,
            limits,
            RelationalCheckpointIndexLoad::MaterializedPostings,
            &probe.context(local.clone()),
        )
        .unwrap_err();
        assert_eq!(error, expected, "scenario={scenario}");
        probe.assert_released(&local);
        assert!(std::fs::read(&path).unwrap() == bytes);
    }
    assert!(std::fs::read(fixture.directory.join("source.hawdb")).unwrap() == fixture.checkpoint);
}

#[test]
fn checkpoint_units_relational_mount_denies_before_io_and_retries_after_release() {
    let fixture = Fixture::new();
    let path = fixture.directory.join("source.hawdb");
    let local = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        max_background_operations_by_class: [Some(1); hawdb_qos::WORK_CLASS_COUNT],
        ..LocalQosPolicy::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let held = local
        .try_start(hawdb_qos::WorkRequest::background(
            hawdb_qos::WorkClass::Mutation,
            1,
        ))
        .unwrap();
    let error = decode_relational_checkpoint_file_with_work_context(
        &path,
        RelationalDecodeLimits::checkpoint(),
        RelationalCheckpointIndexLoad::MaterializedPostings,
        &work,
    )
    .unwrap_err();
    assert!(
        matches!(error, RelationalError::Admission(ref text) if text.contains("admission deferred")),
        "{error:?}"
    );
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
    assert!(std::fs::read(&path).unwrap() == fixture.checkpoint);
    drop(held);
    probe.assert_released(&local);
    let retry = Arc::new(CheckpointWorkProbe::default());
    let recovered = decode_relational_checkpoint_file_with_work_context(
        &path,
        RelationalDecodeLimits::checkpoint(),
        RelationalCheckpointIndexLoad::MaterializedPostings,
        &retry.context(local.clone()),
    )
    .unwrap();
    retry.assert_released(&local);
    fixture.assert_rows(&recovered.state);
    assert!(std::fs::read(&path).unwrap() == fixture.checkpoint);
}
