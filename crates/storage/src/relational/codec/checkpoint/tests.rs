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
            materialized_view: None,
            name: "parents".into(),
            columns: vec![column("id", RelationalScalarType::BigInt, false, None)],
            primary_key: vec!["id".into()],
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        };
        let schema = RelationalTableSchema {
            materialized_view: None,
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
        // Payload units are per written block (#1520), not per field.
        let units = probe.completed.load(Ordering::SeqCst);
        assert!(units >= (actual.len() - HEADER_BYTES).div_ceil(PAYLOAD_BLOCK_BYTES));
        assert!(units < 512, "{units} units for 512 rows");
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
fn checkpoint_streams_wide_inline_rows_without_a_row_sized_allocation() {
    use hawdb_core::RuntimeTaskContext;
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot,
    };
    let fixture = Fixture::new();
    let store = RelationalStore::with_overflow_config(
        RelationalMutationLimits::default(),
        RelationalOverflowConfig {
            threshold_bytes: usize::MAX,
            ..RelationalOverflowConfig::default()
        },
    );
    let row = RelationalRow::new(vec![RelationalValue::Text("界\0🙂".repeat(65536))]);
    store
        .commit(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        materialized_view: None,
                        name: "wide".into(),
                        columns: vec![RelationalColumnSchema {
                            name: "value".into(),
                            scalar_type: RelationalScalarType::Text,
                            nullable: false,
                            default: None,
                        }],
                        primary_key: vec!["value".into()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: Vec::new(),
                    }),
                    RelationalWrite::Insert {
                        table: "wide".into(),
                        rows: vec![row.clone()],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            |_, _| Ok(()),
        )
        .unwrap();
    let snapshot = store.snapshot().unwrap();
    let state = snapshot.value();
    let expected = encode_relational_checkpoint(41, state).unwrap();
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(std::num::NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let admission = governor
        .try_admit_incremental_maintenance(0, 32 * 1024, 1, RuntimeTaskContext::default())
        .unwrap();
    let work = CheckpointWorkContext::new(admission.task_context().unwrap().clone());
    let path = fixture.directory.join("streamed-wide.hawdb");
    let mut writer = File::create(&path).unwrap();
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        encode_relational_checkpoint_with_work_context(
            &mut writer,
            41,
            state,
            RelationalDecodeLimits::checkpoint().max_record_bytes,
            CheckpointOutputIo::File,
            work,
        )
    });
    let allocations = observation.finish();
    drop(writer);
    assert!(result.is_ok(), "streaming row must fit 32 KiB: {result:?}");
    assert_eq!(allocations, 0, "streaming checkpoint allocated a whole row");
    assert_eq!(std::fs::read(&path).unwrap(), expected);
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);

    let run = |cpu_cut: usize, io_cut: usize| {
        use crate::background::{CheckpointOperationError, CheckpointWorkError};
        let admission = governor
            .try_admit_incremental_maintenance(0, 32 * 1024, 1, RuntimeTaskContext::default())
            .unwrap();
        let task = admission.task_context().unwrap().clone();
        let probe = Arc::new(CheckpointWorkProbe {
            cancellation: task.cancellation().clone(),
            ..CheckpointWorkProbe::default()
        });
        let local = scheduler();
        local.set_telemetry_sink(Some(probe.clone()));
        probe.cancel_after.store(cpu_cut, Ordering::SeqCst);
        probe.cancel_on_io_wave.store(io_cut, Ordering::SeqCst);
        let work = CheckpointWorkContext::new(task.with_io_wave_controller(probe.clone()))
            .with_scheduler(local.clone());
        let mut writer = File::create(&path).unwrap();
        let result = work.classify(|work| {
            encode_relational_checkpoint_with_work_context(
                &mut writer,
                41,
                state,
                RelationalDecodeLimits::checkpoint().max_record_bytes,
                CheckpointOutputIo::File,
                work,
            )
        });
        drop(writer);
        if cpu_cut == 0 && io_cut == 0 {
            assert!(result.is_ok(), "complete streaming retry: {result:?}");
            assert_eq!(std::fs::read(&path).unwrap(), expected);
        } else {
            assert!(
                matches!(
                    result,
                    Err(CheckpointOperationError::Work(
                        CheckpointWorkError::Stopped(
                            hawdb_core::RuntimeCancellationReason::Cancelled
                        ) | CheckpointWorkError::Io(hawdb_core::RuntimeIoWaveError::Stopped(
                            hawdb_core::RuntimeCancellationReason::Cancelled
                        ))
                    ))
                ),
                "streaming cancellation {cpu_cut}/{io_cut}: {result:?}"
            );
            if cpu_cut > 0 {
                assert_eq!(probe.completed.load(Ordering::SeqCst), cpu_cut);
            } else {
                assert_eq!(probe.io_waves.load(Ordering::SeqCst), io_cut);
            }
        }
        let counts = (
            probe.completed.load(Ordering::SeqCst),
            probe.io_waves.load(Ordering::SeqCst),
        );
        probe.assert_released(&local);
        assert_eq!(admission.memory_report().live_accounted_bytes, 0);
        drop(work);
        drop(admission);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        counts
    };
    let (cpu_units, io_waves) = run(0, 0);
    assert!(cpu_units > 16 && io_waves > 8);
    for cut in 1..=cpu_units {
        run(cut, 0);
    }
    for cut in 1..=io_waves {
        run(0, cut);
    }
    assert_eq!(run(0, 0), (cpu_units, io_waves));
    println!("verified {cpu_units} streaming CPU cuts and {io_waves} IO cuts, then complete retry");
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
    // Units are per block rather than per field (#1520), so every cut is
    // enumerated.
    for limit in 1..=units {
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
        materialized_view: None,
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

const NUMERIC_COLUMNS: usize = 12;

/// Many small fields per row: the shape whose per-field writes made #1520.
fn numeric_state(rows: i64) -> RelationalState {
    let store = RelationalStore::with_overflow_config(
        RelationalMutationLimits::default(),
        RelationalOverflowConfig::default(),
    );
    let column = |name: String, scalar_type| RelationalColumnSchema {
        name,
        scalar_type,
        nullable: true,
        default: None,
    };
    let mut columns = vec![RelationalColumnSchema {
        nullable: false,
        ..column("id".into(), RelationalScalarType::BigInt)
    }];
    columns.extend((0..6).map(|index| column(format!("i{index}"), RelationalScalarType::BigInt)));
    columns.extend(
        (0..3).map(|index| column(format!("d{index}"), RelationalScalarType::DoublePrecision)),
    );
    columns.push(column("flag".into(), RelationalScalarType::Boolean));
    columns.push(column("note".into(), RelationalScalarType::Text));
    assert_eq!(columns.len(), NUMERIC_COLUMNS);
    let mut writes = vec![RelationalWrite::CreateTable(RelationalTableSchema {
        materialized_view: None,
        name: "lines".into(),
        columns,
        primary_key: vec!["id".into()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    })];
    for start in (0..rows).step_by(1000) {
        let rows =
            (start..(start + 1000).min(rows))
                .map(|id| {
                    let mut values = vec![RelationalValue::BigInt(id)];
                    values.extend((0..6).map(|index| RelationalValue::BigInt(id * 7 + index)));
                    values.extend((0..3).map(|index| {
                        RelationalValue::DoublePrecision(id as f64 / (index + 2) as f64)
                    }));
                    values.push(if id % 5 == 0 {
                        RelationalValue::Null
                    } else {
                        RelationalValue::Boolean(id % 2 == 0)
                    });
                    values.push(RelationalValue::Text(format!("n{id}")));
                    RelationalRow::new(values)
                })
                .collect();
        writes.push(RelationalWrite::Insert {
            table: "lines".into(),
            rows,
            mode: RelationalInsertMode::Error,
        });
        store
            .commit(
                RelationalTransaction {
                    writes: std::mem::take(&mut writes),
                },
                |_, _| Ok(()),
            )
            .unwrap();
    }
    store.snapshot().unwrap().value().clone()
}

/// Test-only physical write counter: one `write` call per payload block.
#[derive(Default)]
struct CountingWriter {
    inner: Cursor<Vec<u8>>,
    writes: usize,
    fail_from_write: Option<usize>,
    live_memory: Option<Arc<std::sync::atomic::AtomicU64>>,
    live_at_write: Vec<u64>,
}

impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.writes += 1;
        if let Some(live) = &self.live_memory {
            self.live_at_write.push(live.load(Ordering::SeqCst));
        }
        if self
            .fail_from_write
            .is_some_and(|first| self.writes >= first)
        {
            return Err(std::io::Error::other("injected checkpoint write failure"));
        }
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for CountingWriter {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(position)
    }
}

fn encode_counted(
    state: &RelationalState,
    work: &CheckpointWorkContext,
    mut writer: CountingWriter,
) -> (Result<u64, RelationalError>, CountingWriter) {
    let result = encode_relational_checkpoint_with_work_context(
        &mut writer,
        41,
        state,
        RelationalDecodeLimits::checkpoint().max_record_bytes,
        CheckpointOutputIo::Memory,
        work,
    );
    (result, writer)
}

#[test]
fn checkpoint_buffered_payload_writes_and_units_scale_with_bytes_not_fields() {
    let local = scheduler();
    let measure = |rows: i64| {
        let state = numeric_state(rows);
        let expected = encode_relational_checkpoint(41, &state).unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let (result, writer) = encode_counted(
            &state,
            &probe.context(local.clone()),
            CountingWriter::default(),
        );
        assert_eq!(result.unwrap(), expected.len() as u64);
        assert!(
            *writer.inner.get_ref() == expected,
            "buffered checkpoint bytes differ from the ordinary encoder"
        );
        let payload = expected.len() - HEADER_BYTES;
        // Full blocks, the final partial block, the written-through overflow
        // section and the header.
        assert!(
            writer.writes <= payload.div_ceil(PAYLOAD_BLOCK_BYTES) + 3,
            "{} writes for {payload} payload bytes",
            writer.writes
        );
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&local);
        let fields = rows as usize * NUMERIC_COLUMNS;
        (
            expected.len(),
            fields,
            writer.writes,
            probe.completed.load(Ordering::SeqCst),
        )
    };
    let (small_bytes, small_fields, small_writes, small_units) = measure(1_000);
    let (large_bytes, large_fields, large_writes, large_units) = measure(9_000);
    let blocks = (large_bytes - small_bytes).div_ceil(PAYLOAD_BLOCK_BYTES);
    let groups = (large_fields - small_fields).div_ceil(REACHABILITY_VALUES_PER_UNIT);
    let (writes, units, fields) = (
        large_writes - small_writes,
        large_units - small_units,
        large_fields - small_fields,
    );
    eprintln!(
        "{fields} extra fields / {} extra bytes: {writes} extra writes, {units} extra units",
        large_bytes - small_bytes
    );
    assert!(writes <= blocks + 1, "{writes} writes for {blocks} blocks");
    // One unit per written block and one per reachability value group.
    assert!(
        units <= blocks + groups + 2,
        "{units} units for {blocks} blocks and {groups} value groups"
    );
    // The unbuffered encoder took at least two units and one write per field.
    assert!(fields > 500 * writes && fields > 100 * units);
}

#[test]
fn checkpoint_buffered_payload_surfaces_write_errors_at_flush() {
    let local = scheduler();
    let assert_write_error = |error: RelationalError| {
        assert!(
            matches!(
                error,
                RelationalError::Durability(ref text)
                    if text.contains("failed to stream relational checkpoint payload")
                        && text.contains("injected checkpoint write failure")
            ),
            "{error:?}"
        );
    };
    // The whole payload fits one block: the first physical write is the
    // final flush, so the error must surface there rather than be dropped.
    let small = numeric_state(10);
    assert!(encode_relational_checkpoint(41, &small).unwrap().len() < PAYLOAD_BLOCK_BYTES);
    let probe = Arc::new(CheckpointWorkProbe::default());
    let (result, writer) = encode_counted(
        &small,
        &probe.context(local.clone()),
        CountingWriter {
            fail_from_write: Some(1),
            ..CountingWriter::default()
        },
    );
    assert_write_error(result.unwrap_err());
    assert_eq!(
        writer.writes, 1,
        "nothing is written after the failed flush"
    );
    assert!(writer.inner.get_ref().is_empty());
    probe.assert_released(&local);

    // A mid-stream block failure stops the stream before the header.
    let large = numeric_state(9_000);
    let probe = Arc::new(CheckpointWorkProbe::default());
    let (result, writer) = encode_counted(
        &large,
        &probe.context(local.clone()),
        CountingWriter {
            fail_from_write: Some(3),
            ..CountingWriter::default()
        },
    );
    assert_write_error(result.unwrap_err());
    assert_eq!(writer.writes, 3);
    assert_eq!(
        writer.inner.get_ref().len(),
        HEADER_BYTES + 2 * PAYLOAD_BLOCK_BYTES
    );
    assert!(writer.inner.get_ref()[..HEADER_BYTES]
        .iter()
        .all(|byte| *byte == 0));
    probe.assert_released(&local);
}

#[test]
fn checkpoint_buffered_payload_observes_cancellation_at_every_unit() {
    let local = scheduler();
    let state = numeric_state(9_000);
    let expected = encode_relational_checkpoint(41, &state).unwrap();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let (result, _) = encode_counted(
        &state,
        &probe.context(local.clone()),
        CountingWriter::default(),
    );
    result.unwrap();
    let units = probe.completed.load(Ordering::SeqCst);
    for limit in 1..=units {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        let (result, writer) = encode_counted(
            &state,
            &probe.context(local.clone()),
            CountingWriter::default(),
        );
        let error = result.unwrap_err();
        assert!(
            matches!(error, RelationalError::Admission(ref text) if text.contains("cancelled")),
            "cut {limit}: {error:?}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), limit);
        // Every physical write is its own unit: none follows the stop.
        assert!(
            writer.writes <= limit,
            "cut {limit}: {} writes",
            writer.writes
        );
        probe.assert_released(&local);
    }
    let probe = Arc::new(CheckpointWorkProbe::default());
    let (result, writer) = encode_counted(
        &state,
        &probe.context(local.clone()),
        CountingWriter::default(),
    );
    result.unwrap();
    assert!(
        *writer.inner.get_ref() == expected,
        "retry after cancellation differs"
    );
    probe.assert_released(&local);
}

#[derive(Debug, Default)]
struct RecordingMemory {
    deny: bool,
    reserved: std::sync::Mutex<Vec<u64>>,
    live: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Debug)]
struct RecordingLease(u64, Arc<std::sync::atomic::AtomicU64>);

impl hawdb_core::RuntimeMemoryPermit for RecordingLease {
    fn bytes(&self) -> u64 {
        self.0
    }
}

impl Drop for RecordingLease {
    fn drop(&mut self) {
        self.1.fetch_sub(self.0, Ordering::SeqCst);
    }
}

impl hawdb_core::RuntimeMemoryController for RecordingMemory {
    fn reserve(
        &self,
        bytes: u64,
        _ceiling: u64,
    ) -> Result<Box<dyn hawdb_core::RuntimeMemoryPermit>, hawdb_core::RuntimeMemoryError> {
        self.reserved.lock().unwrap().push(bytes);
        if self.deny {
            return Err(hawdb_core::RuntimeMemoryError::AdmissionDenied {
                requested_bytes: bytes,
                available_bytes: 0,
                retryable: true,
            });
        }
        self.live.fetch_add(bytes, Ordering::SeqCst);
        Ok(Box::new(RecordingLease(bytes, self.live.clone())))
    }
}

#[test]
fn checkpoint_buffered_payload_admits_its_staging_block_and_falls_back_when_denied() {
    let local = scheduler();
    let state = numeric_state(2_000);
    let expected = encode_relational_checkpoint(41, &state).unwrap();
    for deny in [false, true] {
        let memory = Arc::new(RecordingMemory {
            deny,
            ..RecordingMemory::default()
        });
        let probe = Arc::new(CheckpointWorkProbe::default());
        local.set_telemetry_sink(Some(probe.clone()));
        let task = hawdb_core::RuntimeTaskContext::without_deadline(probe.cancellation.clone())
            .with_io_wave_controller(probe.clone())
            .with_memory_controller(memory.clone());
        let work = CheckpointWorkContext::new(task).with_scheduler(local.clone());
        let mut writer = CountingWriter {
            live_memory: Some(memory.live.clone()),
            ..CountingWriter::default()
        };
        // A refused staging lease must not be recorded as the work's failure.
        let result = work.classify(|work| {
            encode_relational_checkpoint_with_work_context(
                &mut writer,
                41,
                &state,
                RelationalDecodeLimits::checkpoint().max_record_bytes,
                CheckpointOutputIo::Memory,
                work,
            )
        });
        assert!(result.is_ok(), "deny={deny}: {result:?}");
        assert!(
            *writer.inner.get_ref() == expected,
            "deny={deny}: bytes differ"
        );
        assert_eq!(
            *memory.reserved.lock().unwrap(),
            [PAYLOAD_BLOCK_BYTES as u64]
        );
        assert_eq!(memory.live.load(Ordering::SeqCst), 0);
        let fields = 2_000 * NUMERIC_COLUMNS;
        if deny {
            // Unadmitted: the original write-through path, one write per call.
            assert!(writer.writes > fields);
            assert!(writer.live_at_write.iter().all(|live| *live == 0));
        } else {
            let payload = expected.len() - HEADER_BYTES;
            assert!(writer.writes <= payload.div_ceil(PAYLOAD_BLOCK_BYTES) + 3);
            // The lease is charged while blocks are written and released
            // before the overflow section and the header.
            assert_eq!(writer.live_at_write[0], PAYLOAD_BLOCK_BYTES as u64);
            assert_eq!(writer.live_at_write.last(), Some(&0));
        }
        probe.assert_released(&local);
    }
}
