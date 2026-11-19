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
use crate::append_table::AppendOrderMode;
use crate::background::CheckpointWorkProbe;
use crate::relational::{
    RelationalColumnSchema, RelationalRow, RelationalScalarType, RelationalValue,
};
use hawdb_core::RuntimeCancellationReason;
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
use std::sync::atomic::{AtomicU64, Ordering};

#[path = "tests/reader_memory.rs"]
mod reader_memory;

#[path = "tests/segment_memory.rs"]
mod segment_memory;

#[path = "tests/root_ownership.rs"]
mod root_ownership;

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-append-publication-work-{}-{nanos}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    })
}

#[test]
fn checkpoint_state_preserves_multigeneration_watermarks_defaults_and_cancellation() {
    use crate::append_table::AppendState;
    use crate::background::{CheckpointOperationError, CheckpointWorkError};
    use crate::relational::RelationalColumnDefault;
    use hawdb_core::{RuntimeCancellationReason, RuntimeMemoryError};

    let directory = Directory::new();
    let mut schema = AppendTableSchema {
        name: "events".into(),
        columns: [
            ("stream", RelationalScalarType::Text),
            ("sequence", RelationalScalarType::BigInt),
        ]
        .into_iter()
        .map(|(name, scalar_type)| RelationalColumnSchema {
            name: name.into(),
            scalar_type,
            nullable: false,
            default: None,
        })
        .collect(),
        partition_key: vec!["stream".into()],
        order_key: vec!["sequence".into()],
        order_mode: AppendOrderMode::CommitSequence,
    };
    for (name, scalar_type, value) in [
        (
            "text",
            RelationalScalarType::Text,
            RelationalValue::Text("默认;\n\\".into()),
        ),
        (
            "bytes",
            RelationalScalarType::Bytea,
            RelationalValue::Bytea(vec![0, 255, 17]),
        ),
    ] {
        schema.columns.push(RelationalColumnSchema {
            name: name.into(),
            scalar_type,
            nullable: false,
            default: Some(RelationalColumnDefault::Literal(value)),
        });
    }
    let schemas = BTreeMap::from([("events".into(), schema)]);
    let mut rows = (1..=12)
        .map(|sequence| {
            let stream = RelationalValue::Text(format!("分区;{}\n", sequence % 3));
            AppendTableRow {
                table: "events".into(),
                partition_key: RelationalKey(vec![stream.clone()]),
                order_key: RelationalKey(vec![RelationalValue::BigInt(sequence)]),
                row: RelationalRow::new(vec![
                    stream,
                    RelationalValue::BigInt(sequence),
                    RelationalValue::Text("默认;\n\\".into()),
                    RelationalValue::Bytea(vec![0, 255, 17]),
                ]),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(compare_rows);
    let base = rows
        .iter()
        .filter(|row| row.order_key.0[0] <= RelationalValue::BigInt(6))
        .cloned()
        .collect::<Vec<_>>();
    let suffix = rows
        .iter()
        .filter(|row| row.order_key.0[0] > RelationalValue::BigInt(6))
        .cloned()
        .collect::<Vec<_>>();
    let config = AppendPublicationConfig::default();
    let mut previous = None;
    for (generation, rows, watermark) in [(1, &base, 6), (2, &suffix, 12)] {
        let generated = BTreeMap::from([("events".into(), watermark)]);
        let report = AppendPublisher::publish_candidate_with_state(
            &directory.0,
            generation,
            generation,
            previous.as_ref(),
            AppendPublicationState::new(&schemas, &generated),
            rows,
            config,
        )
        .unwrap();
        previous = Some(
            AppendGenerationReader::open_bound(&directory.0, report.generation_artifacts, config)
                .unwrap(),
        );
    }
    let reader = previous.unwrap();
    let expected = AppendState::from_checkpoint_with_generated_order_watermarks(
        reader.manifest().schemas.clone(),
        reader.watermarks(),
        reader.generated_order_watermarks().clone(),
    )
    .unwrap();
    assert_eq!(reader.watermarks()["events"].len(), 3);
    assert_eq!(reader.generated_order_watermarks()["events"], 12);
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    let actual = work
        .classify(|work| reader.checkpoint_state_with_work_context(work))
        .unwrap();
    assert_eq!(actual, expected);
    let total = probe.completed.load(Ordering::SeqCst);
    assert!(total > 12);
    drop(actual);
    probe.assert_released(&scheduler);
    for boundary in 1..=total {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(boundary, Ordering::SeqCst);
        let work = probe.context(scheduler.clone());
        let result = work.classify(|work| reader.checkpoint_state_with_work_context(work));
        assert!(
            matches!(
                result,
                Err(CheckpointOperationError::Work(
                    CheckpointWorkError::Stopped(RuntimeCancellationReason::Cancelled)
                        | CheckpointWorkError::Memory(RuntimeMemoryError::Stopped(
                            RuntimeCancellationReason::Cancelled
                        ))
                ))
            ),
            "cancellation boundary {boundary}/{total}: {result:?}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), boundary);
        probe.assert_released(&scheduler);
        assert_eq!(reader.manifest().schemas, schemas);
        assert_eq!(reader.checkpoint_rows(12).unwrap(), rows);
    }
}

fn assert_stopped<T: std::fmt::Debug>(result: Result<T, AppendTableError>) {
    assert!(
        matches!(
            &result,
            Err(AppendTableError::Stopped(
                RuntimeCancellationReason::Cancelled
            ))
        ),
        "{result:?}"
    );
}

#[test]
fn checkpoint_units_append_artifact_cancels_every_wave_and_preserves_published_evidence() {
    let bytes = (0..4 * 64 * 1024 + 9)
        .map(|offset| ((offset * 17 + offset / 257) % 251) as u8)
        .collect::<Vec<_>>();
    let scheduler = scheduler();
    let baseline = Directory::new();
    let probe = Arc::new(CheckpointWorkProbe::default());
    write_artifact(
        &baseline.0,
        "candidate",
        &bytes,
        Some(&probe.context(scheduler.clone())),
    )
    .unwrap();
    assert_eq!(fs::read(baseline.0.join("candidate")).unwrap(), bytes);
    assert!(!baseline.0.join("candidate.tmp").exists());
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 8);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    probe.assert_released(&scheduler);
    for wave in 1..=8 {
        let directory = Directory::new();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_on_io_wave.store(wave, Ordering::SeqCst);
        assert_stopped(write_artifact(
            &directory.0,
            "candidate",
            &bytes,
            Some(&probe.context(scheduler.clone())),
        ));
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), wave);
        assert!(!directory.0.join("candidate.tmp").exists());
        assert!(!directory.0.join("candidate").exists());
        probe.assert_released(&scheduler);
        let retry = Arc::new(CheckpointWorkProbe::default());
        write_artifact(
            &directory.0,
            "candidate",
            &bytes,
            Some(&retry.context(scheduler.clone())),
        )
        .unwrap();
        assert_eq!(fs::read(directory.0.join("candidate")).unwrap(), bytes);
        retry.assert_released(&scheduler);
    }
    let directory = Directory::new();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(8, Ordering::SeqCst);
    let error = write_artifact(
        &directory.0,
        "candidate",
        &bytes,
        Some(&probe.context(scheduler.clone())),
    )
    .unwrap_err();
    // Cancellation after durable rename is a lost reply, not a rollback of
    // the complete private artifact; retain it as evidence for its owner.
    assert_eq!(fs::read(directory.0.join("candidate")).unwrap(), bytes);
    assert!(!directory.0.join("candidate.tmp").exists());
    let public = hawdb_core::HawDBError::from_storage_error(error.clone());
    assert!(
        matches!(
            public,
            hawdb_core::HawDBError::PublishOutcomeUnknown {
                ref destination,
                ..
            } if destination == &directory.0.join("candidate")
        ),
        "{public:?}"
    );
    assert!(matches!(error, AppendTableError::PublishOutcomeUnknown(_)));
    probe.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_append_artifact_preserves_unowned_temporary_file() {
    let directory = Directory::new();
    let temporary = directory.0.join("candidate.tmp");
    fs::write(&temporary, b"earlier interrupted candidate").unwrap();
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    assert!(write_artifact(
        &directory.0,
        "candidate",
        b"new",
        Some(&probe.context(scheduler.clone()))
    )
    .is_err());
    assert_eq!(
        fs::read(&temporary).unwrap(),
        b"earlier interrupted candidate"
    );
    assert!(!directory.0.join("candidate").exists());
    probe.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_append_publication_matches_legacy_bytes_and_complete_reopen() {
    let mut state = 0x8a13_5cdf_u64;
    let rows = (0..2048)
        .map(|sequence| {
            let payload = (0..768)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state as u8
                })
                .collect();
            AppendTableRow {
                table: "events".into(),
                partition_key: RelationalKey(vec![RelationalValue::BigInt(7)]),
                order_key: RelationalKey(vec![RelationalValue::BigInt(sequence)]),
                row: RelationalRow::new(vec![
                    RelationalValue::BigInt(7),
                    RelationalValue::BigInt(sequence),
                    RelationalValue::Bytea(payload),
                ]),
            }
        })
        .collect::<Vec<_>>();
    let schema = AppendTableSchema {
        name: "events".into(),
        columns: [
            ("stream", RelationalScalarType::BigInt),
            ("sequence", RelationalScalarType::BigInt),
            ("payload", RelationalScalarType::Bytea),
        ]
        .into_iter()
        .map(|(name, scalar_type)| RelationalColumnSchema {
            name: name.into(),
            scalar_type,
            nullable: false,
            default: None,
        })
        .collect(),
        partition_key: vec!["stream".into()],
        order_key: vec!["sequence".into()],
        order_mode: AppendOrderMode::CallerProvided,
    };
    let schemas = BTreeMap::from([("events".into(), schema)]);
    let generated = BTreeMap::new();
    let state = AppendPublicationState::new(&schemas, &generated);
    let config = AppendPublicationConfig::default();
    let baseline = Directory::new();
    let expected = AppendPublisher::publish_candidate_with_state(
        &baseline.0,
        9,
        77,
        None,
        state,
        &rows,
        config,
    )
    .unwrap();
    let directory = Directory::new();
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = AppendPublisher::publish_checkpoint_with_work_context(
        AppendCheckpointPublicationRequest {
            directory: &directory.0,
            generation: 9,
            source_commit_epoch: 77,
            previous: None,
            state,
            rows: &rows,
            config,
        },
        &probe.context(scheduler.clone()),
    )
    .unwrap();
    assert_eq!(actual, expected);
    for file in [append_segment_file(9), append_generation_manifest_file(9)] {
        assert_eq!(
            fs::read(directory.0.join(&file)).unwrap(),
            fs::read(baseline.0.join(&file)).unwrap()
        );
    }
    let reader =
        AppendGenerationReader::open_bound(&directory.0, actual.generation_artifacts, config)
            .unwrap();
    assert_eq!(reader.checkpoint_rows(2048).unwrap(), rows);
    assert!(probe.io_waves.load(Ordering::SeqCst) > 20);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    probe.assert_released(&scheduler);
}

fn manifest_fixture() -> AppendGenerationManifest {
    let schemas = (0..500)
        .map(|index| {
            let name = format!("table-{index:04}-🦀{}", "x".repeat(130));
            let schema = AppendTableSchema {
                name: name.clone(),
                columns: vec![RelationalColumnSchema {
                    name: "sequence".into(),
                    scalar_type: RelationalScalarType::BigInt,
                    nullable: false,
                    default: None,
                }],
                partition_key: vec![],
                order_key: vec!["sequence".into()],
                order_mode: AppendOrderMode::CallerProvided,
            };
            (name, schema)
        })
        .collect();
    AppendGenerationManifest {
        generation: 5000,
        source_commit_epoch: 10000,
        previous_generation: Some(4000),
        root_set_digest: integrity_digest(&[]).sha256,
        schemas,
        generated_order_watermarks: BTreeMap::new(),
        segments: (1..=3000)
            .map(|generation| AppendSegmentBinding {
                generation,
                source_commit_epoch: generation * 2,
                artifact: AppendSegmentArtifactMetadata {
                    encoded_len: generation + 128,
                    encoded_crc32c: generation as u32,
                    encoded_sha256: integrity_digest(&generation.to_le_bytes()).sha256,
                },
            })
            .collect(),
    }
}

fn wide_manifest() -> AppendGenerationManifest {
    let wide = "p".repeat(512 * 1024);
    let schema = AppendTableSchema {
        name: "events".into(),
        columns: ["sequence", &wide]
            .into_iter()
            .map(|name| RelationalColumnSchema {
                name: name.into(),
                scalar_type: RelationalScalarType::BigInt,
                nullable: false,
                default: None,
            })
            .collect(),
        partition_key: vec![],
        order_key: vec!["sequence".into()],
        order_mode: AppendOrderMode::CallerProvided,
    };
    AppendGenerationManifest {
        generation: 1,
        source_commit_epoch: 1,
        previous_generation: None,
        schemas: BTreeMap::from([("events".into(), schema)]),
        generated_order_watermarks: BTreeMap::new(),
        root_set_digest: integrity_digest(&[]).sha256,
        segments: vec![],
    }
}

fn manifest_governor() -> hawdb_qos::RuntimeGovernor {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot,
    };
    use std::num::NonZeroUsize;
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    governor
}

#[test]
fn manifest_producer_denies_a_wide_schema_before_copying_or_encoding() {
    use crate::background::{CheckpointOperationError, CheckpointWorkError};
    use hawdb_core::RuntimeTaskContext;

    let manifest = wide_manifest();
    let config = AppendPublicationConfig::default();
    let reference = encode_manifest_payload(&manifest, config).unwrap();
    assert!(reference.len() > 512 * 1024);
    let governor = manifest_governor();
    let admission = governor
        .try_admit_incremental_maintenance(0, 32 * 1024, 1, RuntimeTaskContext::default())
        .unwrap();
    let local = scheduler();
    let work = CheckpointWorkContext::new(admission.task_context().unwrap().clone())
        .with_scheduler(local.clone());
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| manifest_payload(&manifest, config, work));
    let copies = observation.finish();
    let denied = matches!(
        result,
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            _
        )))
    );
    drop(result);
    assert_eq!(
        manifest.schemas["events"].columns[1].name,
        "p".repeat(512 * 1024)
    );
    assert_eq!(
        encode_manifest_payload(&manifest, config).unwrap(),
        reference
    );
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    assert_eq!(local.state().running_background_operations, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert!(
        denied,
        "32 KiB producer admitted a 512 KiB schema with {copies} large allocations"
    );
    assert_eq!(
        copies, 0,
        "deny before a wide schema clone or encoding allocation"
    );
}

#[test]
fn wide_manifest_publication_uses_one_admitted_buffer_and_reopens_complete_schema() {
    use hawdb_core::RuntimeTaskContext;

    let manifest = wide_manifest();
    let directory = Directory::new();
    let governor = manifest_governor();
    let admission = governor
        .try_admit_incremental_maintenance(0, 1024 * 1024, 1, RuntimeTaskContext::default())
        .unwrap();
    let local = scheduler();
    let work = CheckpointWorkContext::new(admission.task_context().unwrap().clone())
        .with_scheduler(local.clone());
    let observation = crate::test_allocator::AllocationObservation::start();
    let report = AppendPublisher::publish_checkpoint_with_work_context(
        AppendCheckpointPublicationRequest {
            directory: &directory.0,
            generation: 1,
            source_commit_epoch: 1,
            previous: None,
            state: AppendPublicationState::new(
                &manifest.schemas,
                &manifest.generated_order_watermarks,
            ),
            rows: &[],
            config: AppendPublicationConfig::default(),
        },
        &work,
    )
    .unwrap();
    assert_eq!(
        observation.finish(),
        1,
        "encode directly into one final admitted manifest allocation"
    );
    let memory = admission.memory_report();
    assert!(memory.peak_accounted_bytes >= 512 * 1024);
    assert!(memory.peak_accounted_bytes <= 1024 * 1024);
    assert_eq!(memory.live_accounted_bytes, 0);
    assert_eq!(local.state().running_background_operations, 0);
    let reader = AppendGenerationReader::open_bound(
        &directory.0,
        report.generation_artifacts,
        AppendPublicationConfig::default(),
    )
    .unwrap();
    assert_eq!(reader.manifest().schemas, manifest.schemas);
    assert!(reader.checkpoint_rows(0).unwrap().is_empty());
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn manifest_default_bytea_denial_and_admitted_publication_preserve_wal_bytes() {
    use crate::append_table::{encode_append_wal_batch, AppendTransaction, AppendWrite};
    use crate::background::{CheckpointOperationError, CheckpointWorkError};
    use crate::relational::RelationalColumnDefault;
    use hawdb_core::RuntimeTaskContext;

    let mut manifest = wide_manifest();
    let schema = manifest.schemas.get_mut("events").unwrap();
    schema.columns[1].name = "payload".into();
    schema.columns[1].scalar_type = RelationalScalarType::Bytea;
    schema.columns[1].default = Some(RelationalColumnDefault::Literal(RelationalValue::Bytea(
        vec![173; 512 * 1024],
    )));
    let reference = encode_append_wal_batch(
        0,
        &AppendTransaction {
            writes: vec![AppendWrite::CreateTable {
                schema: schema.clone(),
            }],
        },
    )
    .unwrap();
    let config = AppendPublicationConfig::default();
    let governor = manifest_governor();
    let denied_admission = governor
        .try_admit_incremental_maintenance(0, 32 * 1024, 1, RuntimeTaskContext::default())
        .unwrap();
    let local = scheduler();
    let work = CheckpointWorkContext::new(denied_admission.task_context().unwrap().clone())
        .with_scheduler(local.clone());
    let mut borrowed = super::super::manifest_encoding::Manifest::from(&manifest);
    let observation = crate::test_allocator::AllocationObservation::start();
    let result =
        work.classify(|work| super::super::manifest_encoding::encode(&mut borrowed, config, work));
    assert_eq!(observation.finish(), 0);
    assert!(matches!(
        result,
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            _
        )))
    ));
    assert_eq!(borrowed.root_set_digest, manifest.root_set_digest);
    assert_eq!(denied_admission.memory_report().live_accounted_bytes, 0);
    assert_eq!(local.state().running_background_operations, 0);
    drop(work);
    drop(denied_admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);

    let admission = governor
        .try_admit_incremental_maintenance(0, 1024 * 1024, 1, RuntimeTaskContext::default())
        .unwrap();
    let work = CheckpointWorkContext::new(admission.task_context().unwrap().clone())
        .with_scheduler(local.clone());
    let directory = Directory::new();
    let observation = crate::test_allocator::AllocationObservation::start();
    let report = AppendPublisher::publish_checkpoint_with_work_context(
        AppendCheckpointPublicationRequest {
            directory: &directory.0,
            generation: manifest.generation,
            source_commit_epoch: manifest.source_commit_epoch,
            previous: None,
            state: AppendPublicationState::new(
                &manifest.schemas,
                &manifest.generated_order_watermarks,
            ),
            rows: &[],
            config,
        },
        &work,
    )
    .unwrap();
    assert_eq!(observation.finish(), 1);
    let encoded = fs::read(directory.0.join(append_generation_manifest_file(1))).unwrap();
    let payload = &encoded[MANIFEST_HEADER_BYTES..];
    assert_eq!(&payload[..8], &(reference.len() as u64).to_le_bytes());
    assert_eq!(&payload[8..], reference.as_slice());
    let reader =
        AppendGenerationReader::open_bound(&directory.0, report.generation_artifacts, config)
            .unwrap();
    assert_eq!(reader.manifest().schemas, manifest.schemas);
    assert!(reader.checkpoint_rows(0).unwrap().is_empty());
    let memory = admission.memory_report();
    assert!(memory.peak_accounted_bytes >= reference.len() as u64);
    assert!(memory.peak_accounted_bytes <= 1024 * 1024);
    assert_eq!(memory.live_accounted_bytes, 0);
    assert_eq!(local.state().running_background_operations, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn manifest_buffer_retains_admission_after_task_close_and_cancels_each_unit() {
    use crate::background::{CheckpointOperationError, CheckpointWorkError};
    use hawdb_core::{RuntimeCancellationReason, RuntimeMemoryError, RuntimeTaskContext};

    let manifest = wide_manifest();
    let config = AppendPublicationConfig::default();
    let governor = manifest_governor();
    let admission = governor
        .try_admit_incremental_maintenance(0, 1024 * 1024, 1, RuntimeTaskContext::default())
        .unwrap();
    let local = scheduler();
    let task = admission.task_context().unwrap().clone();
    let probe = Arc::new(CheckpointWorkProbe {
        cancellation: task.cancellation().clone(),
        ..CheckpointWorkProbe::default()
    });
    let work = CheckpointWorkContext::new(task).with_scheduler(local.clone());
    // The governor also charges the concrete allocation lease. Measure that
    // charge independently; no schema or output buffer is allocated here.
    let lease = work.reserve_memory(0).unwrap().unwrap();
    let lease_bytes = admission.memory_report().live_accounted_bytes;
    assert!(lease_bytes > 0);
    drop(lease);
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    local.set_telemetry_sink(Some(probe.clone()));
    let mut borrowed = super::super::manifest_encoding::Manifest::from(&manifest);
    let encoded = work
        .classify(|work| super::super::manifest_encoding::encode(&mut borrowed, config, work))
        .unwrap();
    let total = probe.completed.load(Ordering::SeqCst);
    assert!(total > 32);
    assert_eq!(
        admission.memory_report().live_accounted_bytes,
        encoded.len() as u64 + lease_bytes
    );
    probe.assert_released(&local);
    let bytes = encoded.len() as u64 + lease_bytes;
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, bytes);
    assert_eq!(
        decode_manifest(&encoded, config).unwrap().schemas,
        manifest.schemas
    );
    drop(encoded);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);

    for boundary in 1..=total {
        let admission = governor
            .try_admit_incremental_maintenance(0, 1024 * 1024, 1, RuntimeTaskContext::default())
            .unwrap();
        let local = scheduler();
        let task = admission.task_context().unwrap().clone();
        let probe = Arc::new(CheckpointWorkProbe {
            cancellation: task.cancellation().clone(),
            ..CheckpointWorkProbe::default()
        });
        local.set_telemetry_sink(Some(probe.clone()));
        probe.cancel_after.store(boundary, Ordering::SeqCst);
        let work = CheckpointWorkContext::new(task).with_scheduler(local.clone());
        let mut borrowed = super::super::manifest_encoding::Manifest::from(&manifest);
        let result = work
            .classify(|work| super::super::manifest_encoding::encode(&mut borrowed, config, work));
        assert!(
            matches!(
                result,
                Err(CheckpointOperationError::Work(
                    CheckpointWorkError::Stopped(RuntimeCancellationReason::Cancelled)
                        | CheckpointWorkError::Memory(RuntimeMemoryError::Stopped(
                            RuntimeCancellationReason::Cancelled
                        ))
                ))
            ),
            "cancellation boundary {boundary}/{total}: {result:?}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), boundary);
        assert_eq!(borrowed.root_set_digest, manifest.root_set_digest);
        assert_eq!(admission.memory_report().live_accounted_bytes, 0);
        probe.assert_released(&local);
        drop(work);
        drop(admission);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
    println!("verified all {total} cancellation boundaries and final buffer ownership");
}

#[test]
fn manifest_reader_denies_a_wide_schema_before_loading_or_decoding() {
    use crate::background::{CheckpointOperationError, CheckpointWorkError};
    use hawdb_core::RuntimeTaskContext;

    let manifest = wide_manifest();
    let directory = Directory::new();
    let config = AppendPublicationConfig::default();
    let report = AppendPublisher::publish_candidate_with_state(
        &directory.0,
        1,
        1,
        None,
        AppendPublicationState::new(&manifest.schemas, &manifest.generated_order_watermarks),
        &[],
        config,
    )
    .unwrap();
    let path = directory.0.join(append_generation_manifest_file(1));
    let reference = fs::read(&path).unwrap();
    let governor = manifest_governor();
    let admission = governor
        .try_admit_incremental_maintenance(0, 32 * 1024, 1, RuntimeTaskContext::default())
        .unwrap();
    let local = scheduler();
    let work = CheckpointWorkContext::new(admission.task_context().unwrap().clone())
        .with_scheduler(local.clone());
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        AppendGenerationReader::open_bound_with_work_context(
            &directory.0,
            report.generation_artifacts,
            config,
            work,
        )
    });
    let allocations = observation.finish();
    let denied = matches!(
        result,
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            _
        )))
    );
    if let Ok(reader) = &result {
        assert_eq!(reader.manifest().schemas, manifest.schemas);
    }
    drop(result);
    assert_eq!(fs::read(&path).unwrap(), reference);
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    assert_eq!(local.state().running_background_operations, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert!(
        denied,
        "32 KiB reader loaded a 512 KiB schema with {allocations} unadmitted large allocations"
    );
    assert_eq!(
        allocations, 0,
        "deny before loading or decoding the wide manifest"
    );
}

#[test]
fn manifest_reader_retains_decoded_schema_admission_through_last_clone() {
    use hawdb_core::RuntimeTaskContext;

    let manifest = wide_manifest();
    let directory = Directory::new();
    let config = AppendPublicationConfig::default();
    let report = AppendPublisher::publish_candidate(
        &directory.0,
        1,
        1,
        None,
        &manifest.schemas,
        &[],
        config,
    )
    .unwrap();
    let governor = manifest_governor();
    let admission = governor
        .try_admit_incremental_maintenance(0, 2 * 1024 * 1024, 1, RuntimeTaskContext::default())
        .unwrap();
    let local = scheduler();
    let work = CheckpointWorkContext::new(admission.task_context().unwrap().clone())
        .with_scheduler(local.clone());
    let reader = AppendGenerationReader::open_bound_with_work_context(
        &directory.0,
        report.generation_artifacts,
        config,
        &work,
    )
    .unwrap();
    assert_eq!(reader.manifest().schemas, manifest.schemas);
    let clone = reader.clone();
    let retained = admission.memory_report().live_accounted_bytes;
    assert!(
        retained >= 512 * 1024,
        "the reader retains a 512 KiB decoded schema, but only {retained} bytes remain charged"
    );
    assert_eq!(local.state().running_background_operations, 0);
    drop(reader);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, retained);
    assert_eq!(clone.manifest().schemas, manifest.schemas);
    drop(clone);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_append_manifest_preserves_complete_payload_integrity_and_decode() {
    let mut manifest = manifest_fixture();
    let config = AppendPublicationConfig::default();
    let reference_payload = encode_manifest_payload(&manifest, config).unwrap();
    assert!(reference_payload.len() > 4 * 64 * 1024);
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    let payload = manifest_payload(&manifest, config, &work).unwrap();
    assert_eq!(&*payload, reference_payload.as_slice());
    manifest.root_set_digest = integrity_digest(&payload).sha256;
    let encoded = manifest_envelope(&manifest, &payload, config, &work).unwrap();
    assert_eq!(
        &*encoded,
        encode_manifest_with_payload(&manifest, reference_payload, config)
            .unwrap()
            .as_slice()
    );
    assert_eq!(decode_manifest(&encoded, config).unwrap(), manifest);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    probe.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_append_manifest_copying_cancels_entries_and_binding_chunks() {
    let mut manifest = manifest_fixture();
    manifest.generated_order_watermarks = manifest
        .schemas
        .keys()
        .enumerate()
        .map(|(index, name)| (name.clone(), index as i64))
        .collect();
    let original = manifest.clone();
    let observation = crate::test_allocator::AllocationObservation::start_all();
    let mut borrowed = super::super::manifest_encoding::Manifest::from(&manifest);
    let copied = borrowed;
    assert_eq!(
        observation.finish_bytes(),
        0,
        "capturing a manifest view allocates no schema or segment directory"
    );
    assert_eq!(copied.schemas, &manifest.schemas);
    assert_eq!(
        copied.generated_order_watermarks,
        &manifest.generated_order_watermarks
    );
    assert_eq!(copied.segments, manifest.segments.as_slice());
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let encoded = super::super::manifest_encoding::encode(
        &mut borrowed,
        AppendPublicationConfig::default(),
        &probe.context(scheduler.clone()),
    )
    .unwrap();
    let total = probe.completed.load(Ordering::SeqCst);
    let mut expected = manifest.clone();
    expected.root_set_digest = borrowed.root_set_digest;
    assert_eq!(
        decode_manifest(&encoded, AppendPublicationConfig::default()).unwrap(),
        expected
    );
    drop(encoded);
    probe.assert_released(&scheduler);
    for boundary in [2, 17, total - 2, total] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(boundary, Ordering::SeqCst);
        let mut borrowed = super::super::manifest_encoding::Manifest::from(&manifest);
        assert_stopped(super::super::manifest_encoding::encode(
            &mut borrowed,
            AppendPublicationConfig::default(),
            &probe.context(scheduler.clone()),
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), boundary);
        assert_eq!(borrowed.root_set_digest, original.root_set_digest);
        assert_eq!(manifest, original);
        probe.assert_released(&scheduler);
    }
}

#[test]
fn checkpoint_units_append_manifest_cancel_encoding_copy_and_hash_keep_legacy_limits() {
    let mut manifest = manifest_fixture();
    let config = AppendPublicationConfig::default();
    let payload = encode_manifest_payload(&manifest, config).unwrap();
    manifest.root_set_digest = integrity_digest(&payload).sha256;
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(2, Ordering::SeqCst);
    assert_stopped(manifest_payload(
        &manifest,
        config,
        &probe.context(scheduler.clone()),
    ));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 2);
    probe.assert_released(&scheduler);
    // Admitted allocation and fixed header, payload chunks, two covered header slices, then two
    // payload hash chunks. The latter cut is inside the integrity closure.
    for limit in [2, 2 + payload.len().div_ceil(64 * 1024) + 2 + 2] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        assert_stopped(manifest_envelope(
            &manifest,
            &payload,
            config,
            &probe.context(scheduler.clone()),
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), limit);
        probe.assert_released(&scheduler);
    }
    let tight = AppendPublicationConfig {
        max_manifest_bytes: payload.len() + MANIFEST_HEADER_BYTES - 1,
        ..config
    };
    assert_eq!(
        manifest_envelope(&manifest, &payload, tight, &Default::default()).unwrap_err(),
        encode_manifest_with_payload(&manifest, payload, tight).unwrap_err()
    );
    let tight = AppendPublicationConfig {
        max_schema_bytes: 32,
        ..config
    };
    assert_eq!(
        manifest_payload(&manifest, tight, &Default::default()).unwrap_err(),
        encode_manifest_payload(&manifest, tight).unwrap_err()
    );
}

fn validation_reference(
    request: AppendCheckpointPublicationRequest<'_>,
) -> Result<(), AppendTableError> {
    validate_publication_request(
        request.generation,
        request.source_commit_epoch,
        request.previous,
        request.state.schemas,
        request.state.generated_order_watermarks,
        request.rows,
        request.config,
    )
}

#[test]
fn checkpoint_units_append_validation_preserves_complete_generated_watermarks_and_cancels() {
    let schema = AppendTableSchema {
        name: "generated".into(),
        columns: ["stream", "sequence"]
            .into_iter()
            .map(|name| RelationalColumnSchema {
                name: name.into(),
                scalar_type: RelationalScalarType::BigInt,
                nullable: false,
                default: None,
            })
            .collect(),
        partition_key: vec!["stream".into()],
        order_key: vec!["sequence".into()],
        order_mode: AppendOrderMode::CommitSequence,
    };
    let schemas = BTreeMap::from([("generated".into(), schema)]);
    let mut rows = (1..=4097)
        .map(|sequence| AppendTableRow {
            table: "generated".into(),
            partition_key: RelationalKey(vec![RelationalValue::BigInt(sequence % 37)]),
            order_key: RelationalKey(vec![RelationalValue::BigInt(sequence)]),
            row: RelationalRow::new(vec![
                RelationalValue::BigInt(sequence % 37),
                RelationalValue::BigInt(sequence),
            ]),
        })
        .collect::<Vec<_>>();
    rows.sort_by(compare_rows);
    let base_rows = rows
        .iter()
        .filter(|row| row.order_key.0[0] <= RelationalValue::BigInt(64))
        .cloned()
        .collect::<Vec<_>>();
    let segment =
        AppendSegmentWriter::encode(1, 10, &base_rows, AppendSegmentConfig::default()).unwrap();
    let previous = AppendGenerationReader {
        manifest: AppendGenerationManifest {
            generation: 1,
            source_commit_epoch: 10,
            previous_generation: None,
            schemas: schemas.clone(),
            generated_order_watermarks: BTreeMap::from([("generated".into(), 64)]),
            root_set_digest: integrity_digest(&[]).sha256,
            segments: vec![AppendSegmentBinding {
                generation: 1,
                source_commit_epoch: 10,
                artifact: segment.artifact,
            }],
        }
        .into(),
        segments: vec![
            AppendSegmentReader::open(segment.encoded, AppendSegmentConfig::default()).unwrap(),
        ]
        .into(),
    };
    let suffix = rows
        .iter()
        .filter(|row| row.order_key.0[0] > RelationalValue::BigInt(64))
        .cloned()
        .collect::<Vec<_>>();
    let generated = BTreeMap::from([("generated".into(), 4097)]);
    let request = AppendCheckpointPublicationRequest {
        directory: Path::new("unused-validation-only"),
        generation: 2,
        source_commit_epoch: 11,
        previous: Some(&previous),
        state: AppendPublicationState::new(&schemas, &generated),
        rows: &suffix,
        config: AppendPublicationConfig::default(),
    };
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    let actual = effective_watermarks(Some(&previous), &suffix, &work).unwrap();
    assert_eq!(
        actual,
        effective_publication_watermarks(Some(&previous), &suffix)
    );
    assert_eq!(actual["generated"].len(), 37);
    validate_request(&request, Some(&work)).unwrap();
    validation_reference(request).unwrap();
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    probe.assert_released(&scheduler);
    for boundary in [17, 127, 2048, 4097] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(boundary, Ordering::SeqCst);
        assert_stopped(validate_request(
            &request,
            Some(&probe.context(scheduler.clone())),
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), boundary);
        probe.assert_released(&scheduler);
    }
    let baseline = previous.manifest.clone();
    for changed in [
        BTreeMap::new(),
        BTreeMap::from([("generated".into(), -1)]),
        BTreeMap::from([("generated".into(), 4096)]),
        BTreeMap::from([("unknown".into(), 4097)]),
    ] {
        let request = AppendCheckpointPublicationRequest {
            state: AppendPublicationState::new(&schemas, &changed),
            ..request
        };
        assert_eq!(
            validate_request(&request, Some(&CheckpointWorkContext::default())),
            validation_reference(request)
        );
    }
    for request in [
        AppendCheckpointPublicationRequest {
            generation: 0,
            ..request
        },
        AppendCheckpointPublicationRequest {
            generation: 1,
            ..request
        },
        AppendCheckpointPublicationRequest {
            source_commit_epoch: 0,
            ..request
        },
        AppendCheckpointPublicationRequest {
            source_commit_epoch: 9,
            ..request
        },
    ] {
        assert_eq!(
            validate_request(&request, Some(&CheckpointWorkContext::default())),
            validation_reference(request)
        );
    }
    let mut mismatched = schemas.clone();
    mismatched.get_mut("generated").unwrap().name = "wrong".into();
    let bad = AppendCheckpointPublicationRequest {
        state: AppendPublicationState::new(&mismatched, &generated),
        ..request
    };
    assert_eq!(
        validate_request(&bad, Some(&CheckpointWorkContext::default())),
        validation_reference(bad)
    );
    let mut unknown = suffix.clone();
    unknown[17].table = "missing".into();
    let bad = AppendCheckpointPublicationRequest {
        rows: &unknown,
        ..request
    };
    assert_eq!(
        validate_request(&bad, Some(&CheckpointWorkContext::default())),
        validation_reference(bad)
    );
    let retry = Arc::new(CheckpointWorkProbe::default());
    validate_request(&request, Some(&retry.context(scheduler.clone()))).unwrap();
    retry.assert_released(&scheduler);
    assert_eq!(previous.manifest, baseline);
}

#[test]
fn checkpoint_units_append_manifest_decode_preserves_complete_state_and_corruption_errors() {
    let mut manifest = manifest_fixture();
    let config = AppendPublicationConfig::default();
    let payload = encode_manifest_payload(&manifest, config).unwrap();
    manifest.root_set_digest = integrity_digest(&payload).sha256;
    let encoded = encode_manifest_with_payload(&manifest, payload, config).unwrap();
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    assert_eq!(
        read::manifest_decode(&encoded, config, &work).unwrap(),
        manifest
    );
    assert_eq!(
        read::manifest_decode(&encoded, config, &work).unwrap(),
        decode_manifest(&encoded, config).unwrap()
    );
    probe.assert_released(&scheduler);
    for boundary in [2, 17, 512, 3000] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(boundary, Ordering::SeqCst);
        assert_stopped(read::manifest_decode(
            &encoded,
            config,
            &probe.context(scheduler.clone()),
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), boundary);
        probe.assert_released(&scheduler);
    }
    for offset in [
        0,
        8,
        10,
        12,
        28,
        36,
        40,
        44,
        52,
        84,
        88,
        120,
        124,
        128,
        encoded.len() - 1,
    ] {
        let mut corrupt = encoded.clone();
        corrupt[offset] ^= 255;
        assert_eq!(
            read::manifest_decode(&corrupt, config, &CheckpointWorkContext::default())
                .map(CheckpointAppendManifest::into_unadmitted),
            decode_manifest(&corrupt, config)
        );
    }
    for end in [0, 7, 8, 120, 128, encoded.len() - 1] {
        assert_eq!(
            read::manifest_decode(&encoded[..end], config, &CheckpointWorkContext::default())
                .map(CheckpointAppendManifest::into_unadmitted),
            decode_manifest(&encoded[..end], config)
        );
    }
}

#[test]
fn checkpoint_units_append_private_mount_cancels_every_io_then_reopens_complete_rows() {
    let directory = Directory::new();
    let mut random = 0x5a17_8ca3_u64;
    let rows = (1..=512)
        .map(|sequence| AppendTableRow {
            table: "events".into(),
            partition_key: RelationalKey(Vec::new()),
            order_key: RelationalKey(vec![RelationalValue::BigInt(sequence)]),
            row: RelationalRow::new(vec![
                RelationalValue::BigInt(sequence),
                RelationalValue::Bytea(
                    (0..3500)
                        .map(|_| {
                            random ^= random << 13;
                            random ^= random >> 7;
                            random ^= random << 17;
                            random as u8
                        })
                        .collect(),
                ),
            ]),
        })
        .collect::<Vec<_>>();
    let schema = AppendTableSchema {
        name: "events".into(),
        columns: [
            ("sequence", RelationalScalarType::BigInt),
            ("payload", RelationalScalarType::Bytea),
        ]
        .into_iter()
        .map(|(name, scalar_type)| RelationalColumnSchema {
            name: name.into(),
            scalar_type,
            nullable: false,
            default: None,
        })
        .collect(),
        partition_key: vec![],
        order_key: vec!["sequence".into()],
        order_mode: AppendOrderMode::CallerProvided,
    };
    let schemas = BTreeMap::from([("events".into(), schema)]);
    let generated = BTreeMap::new();
    let config = AppendPublicationConfig {
        segment: AppendSegmentConfig {
            target_decoded_block_bytes: 128 * 1024,
            ..AppendSegmentConfig::default()
        },
        ..AppendPublicationConfig::default()
    };
    let report = AppendPublisher::publish_candidate_with_state(
        &directory.0,
        7,
        91,
        None,
        AppendPublicationState::new(&schemas, &generated),
        &rows,
        config,
    )
    .unwrap();
    let manifest_path = directory.0.join(append_generation_manifest_file(7));
    let segment_path = directory.0.join(append_segment_file(7));
    let manifest_bytes = fs::read(&manifest_path).unwrap();
    let segment_bytes = fs::read(&segment_path).unwrap();
    let reference =
        AppendGenerationReader::open_bound(&directory.0, report.generation_artifacts, config)
            .unwrap();
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let reader = AppendGenerationReader::open_bound_with_work_context(
        &directory.0,
        report.generation_artifacts,
        config,
        &probe.context(scheduler.clone()),
    )
    .unwrap();
    assert_eq!(reader.manifest(), reference.manifest());
    assert_eq!(reader.watermarks(), reference.watermarks());
    assert_eq!(reader.checkpoint_rows(512).unwrap(), rows);
    let total = probe.completed.load(Ordering::SeqCst);
    let waves = probe.io_waves.load(Ordering::SeqCst);
    assert!(waves > 20);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    probe.assert_released(&scheduler);
    drop(reader);
    for wave in 1..=waves {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_on_io_wave.store(wave, Ordering::SeqCst);
        assert_stopped(AppendGenerationReader::open_bound_with_work_context(
            &directory.0,
            report.generation_artifacts,
            config,
            &probe.context(scheduler.clone()),
        ));
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), wave);
        probe.assert_released(&scheduler);
    }
    for boundary in [1, 17, total / 2, total - 1, total] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(boundary, Ordering::SeqCst);
        assert_stopped(AppendGenerationReader::open_bound_with_work_context(
            &directory.0,
            report.generation_artifacts,
            config,
            &probe.context(scheduler.clone()),
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), boundary);
        probe.assert_released(&scheduler);
    }
    assert_eq!(fs::read(&manifest_path).unwrap(), manifest_bytes);
    assert_eq!(fs::read(&segment_path).unwrap(), segment_bytes);
    let retry = Arc::new(CheckpointWorkProbe::default());
    let reader = AppendGenerationReader::open_bound_with_work_context(
        &directory.0,
        report.generation_artifacts,
        config,
        &retry.context(scheduler.clone()),
    )
    .unwrap();
    assert_eq!(reader.checkpoint_rows(512).unwrap(), rows);
    retry.assert_released(&scheduler);
    let mut corrupt = segment_bytes.clone();
    *corrupt.last_mut().unwrap() ^= 255;
    fs::write(&segment_path, &corrupt).unwrap();
    assert_eq!(
        AppendGenerationReader::open_bound_with_work_context(
            &directory.0,
            report.generation_artifacts,
            config,
            &CheckpointWorkContext::default()
        )
        .unwrap_err(),
        AppendGenerationReader::open_bound(&directory.0, report.generation_artifacts, config)
            .unwrap_err()
    );
    fs::write(&segment_path, &segment_bytes).unwrap();
}
