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
use crate::relational::{RelationalColumnSchema, RelationalRow, RelationalScalarType};
use hawdb_qos::{
    LocalQosPermit, LocalQosPolicy, LocalQosScheduler, QosTelemetryEvent, QosTelemetryOutcome,
    QosTelemetryPhase, QosTelemetrySink, WorkClass, WorkRequest,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

mod memory;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-append-compaction-work-{}-{nanos}-{}",
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
        max_background_operations_by_class: [Some(1); hawdb_qos::WORK_CLASS_COUNT],
        ..LocalQosPolicy::default()
    })
}

struct Fixture {
    directory: Directory,
    schemas: BTreeMap<String, AppendTableSchema>,
    previous: AppendGenerationReader,
    live: Vec<AppendTableRow>,
    all: Vec<AppendTableRow>,
    config: AppendPublicationConfig,
}
impl Fixture {
    fn new() -> Self {
        let directory = Directory::new();
        let schemas = BTreeMap::from([(
            "events".into(),
            AppendTableSchema {
                name: "events".into(),
                columns: [
                    ("id", RelationalScalarType::BigInt),
                    ("payload", RelationalScalarType::Bytea),
                    ("text", RelationalScalarType::Text),
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
                order_key: vec!["id".into()],
                order_mode: AppendOrderMode::CallerProvided,
            },
        )]);
        let config = AppendPublicationConfig {
            compact_after_segments: 2,
            segment: AppendSegmentConfig {
                overflow_threshold_bytes: 512,
                target_decoded_block_bytes: 128 * 1024,
                ..AppendSegmentConfig::default()
            },
            ..AppendPublicationConfig::default()
        };
        let mut random = 0xa1b2c3d4_u64;
        let all = (0..2071_i64)
            .map(|id| AppendTableRow {
                table: "events".into(),
                partition_key: RelationalKey(vec![]),
                order_key: RelationalKey(vec![RelationalValue::BigInt(id)]),
                row: RelationalRow::new(vec![
                    RelationalValue::BigInt(id),
                    RelationalValue::Bytea(
                        (0..if id % 31 == 0 { 7000 } else { 257 })
                            .map(|_| {
                                random ^= random << 13;
                                random ^= random >> 7;
                                random ^= random << 17;
                                random as u8
                            })
                            .collect(),
                    ),
                    RelationalValue::Text(format!("{}-{id}", "x🦀".repeat(257))),
                ]),
            })
            .collect::<Vec<_>>();
        let first = AppendPublisher::publish_candidate(
            &directory.0,
            1,
            1,
            None,
            &schemas,
            &all[..1031],
            config,
        )
        .unwrap();
        let first =
            AppendGenerationReader::open_bound(&directory.0, first.generation_artifacts, config)
                .unwrap();
        let second = AppendPublisher::publish_candidate(
            &directory.0,
            2,
            2,
            Some(&first),
            &schemas,
            &all[1031..2062],
            config,
        )
        .unwrap();
        let previous =
            AppendGenerationReader::open_bound(&directory.0, second.generation_artifacts, config)
                .unwrap();
        let live = all[2062..].to_vec();
        Self {
            directory,
            schemas,
            previous,
            live,
            all,
            config,
        }
    }
    fn authority(&self) -> Vec<Vec<u8>> {
        [
            append_generation_manifest_file(2),
            append_segment_file(1),
            append_segment_file(2),
        ]
        .into_iter()
        .map(|name| fs::read(self.directory.0.join(name)).unwrap())
        .collect()
    }
}

#[test]
fn checkpoint_units_append_compaction_matches_reference_and_complete_published_reopen() {
    let fixture = Fixture::new();
    let reference = super::super::super::plan_compaction(
        Some(&fixture.previous),
        &fixture.live,
        fixture.config,
    )
    .unwrap();
    assert_eq!(reference.checkpoint_rows.as_ref().unwrap(), &fixture.all);
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = plan_compaction(
        Some(&fixture.previous),
        &fixture.live,
        fixture.config,
        &probe.context(scheduler.clone()),
    )
    .unwrap();
    assert_eq!(actual.due, reference.due);
    assert_eq!(actual.checkpoint_rows, reference.checkpoint_rows);
    let total = probe.completed.load(Ordering::SeqCst);
    assert!(total > fixture.all.len() * 4);
    assert!(probe.io_waves.load(Ordering::SeqCst) > 8);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    probe.assert_released(&scheduler);
    let expected_dir = Directory::new();
    let actual_dir = Directory::new();
    let expected = AppendPublisher::publish_candidate(
        &expected_dir.0,
        3,
        3,
        Some(&fixture.previous),
        &fixture.schemas,
        &fixture.live,
        fixture.config,
    )
    .unwrap();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let generated = BTreeMap::new();
    let actual = AppendPublisher::publish_checkpoint_with_work_context(
        AppendCheckpointPublicationRequest {
            directory: &actual_dir.0,
            generation: 3,
            source_commit_epoch: 3,
            previous: Some(&fixture.previous),
            state: AppendPublicationState::new(&fixture.schemas, &generated),
            rows: &fixture.live,
            config: fixture.config,
        },
        &probe.context(scheduler.clone()),
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.compacted_segments, 2);
    assert_eq!(actual.rows_rewritten, fixture.all.len());
    for name in [append_generation_manifest_file(3), append_segment_file(3)] {
        assert_eq!(
            fs::read(actual_dir.0.join(&name)).unwrap(),
            fs::read(expected_dir.0.join(name)).unwrap()
        );
    }
    let reopened = AppendGenerationReader::open_bound(
        &actual_dir.0,
        actual.generation_artifacts,
        fixture.config,
    )
    .unwrap();
    assert_eq!(reopened.segment_bindings().len(), 1);
    assert_eq!(
        reopened.checkpoint_rows(fixture.all.len()).unwrap(),
        fixture.all
    );
    probe.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_append_compaction_cancels_each_io_and_recovers_all_source_rows() {
    let fixture = Fixture::new();
    let authority = fixture.authority();
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    plan_compaction(
        Some(&fixture.previous),
        &fixture.live,
        fixture.config,
        &probe.context(scheduler.clone()),
    )
    .unwrap();
    let total = probe.completed.load(Ordering::SeqCst);
    let waves = probe.io_waves.load(Ordering::SeqCst);
    probe.assert_released(&scheduler);
    for wave in 1..=waves {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_on_io_wave.store(wave, Ordering::SeqCst);
        let result = plan_compaction(
            Some(&fixture.previous),
            &fixture.live,
            fixture.config,
            &probe.context(scheduler.clone()),
        );
        assert!(
            matches!(&result, Err(AppendTableError::Admission(message)) if message.contains("checkpoint build") && message.contains("cancelled")),
            "{:?}",
            result.as_ref().err()
        );
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), wave);
        assert_eq!(fixture.authority(), authority);
        probe.assert_released(&scheduler);
    }
    for boundary in [2, 17, total / 4, total / 2, total * 3 / 4, total] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(boundary, Ordering::SeqCst);
        let result = plan_compaction(
            Some(&fixture.previous),
            &fixture.live,
            fixture.config,
            &probe.context(scheduler.clone()),
        );
        assert!(
            matches!(&result, Err(AppendTableError::Admission(message)) if message.contains("checkpoint build") && message.contains("cancelled")),
            "{:?}",
            result.as_ref().err()
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), boundary);
        assert_eq!(fixture.authority(), authority);
        probe.assert_released(&scheduler);
    }
    for wave in [2, 4, 6] {
        let destination = Directory::new();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_on_io_wave.store(wave, Ordering::SeqCst);
        let generated = BTreeMap::new();
        let result = AppendPublisher::publish_checkpoint_with_work_context(
            AppendCheckpointPublicationRequest {
                directory: &destination.0,
                generation: 3,
                source_commit_epoch: 3,
                previous: Some(&fixture.previous),
                state: AppendPublicationState::new(&fixture.schemas, &generated),
                rows: &fixture.live,
                config: fixture.config,
            },
            &probe.context(scheduler.clone()),
        );
        assert!(
            matches!(&result, Err(AppendTableError::Admission(message))
            if message.contains("checkpoint build") && message.contains("cancelled")),
            "{:?}",
            result.as_ref().err()
        );
        assert!(!destination
            .0
            .join(append_generation_manifest_file(3))
            .exists());
        assert!(!destination.0.join(append_segment_file(3)).exists());
        assert_eq!(fixture.authority(), authority);
        probe.assert_released(&scheduler);
        let retry = Arc::new(CheckpointWorkProbe::default());
        let report = AppendPublisher::publish_checkpoint_with_work_context(
            AppendCheckpointPublicationRequest {
                directory: &destination.0,
                generation: 3,
                source_commit_epoch: 3,
                previous: Some(&fixture.previous),
                state: AppendPublicationState::new(&fixture.schemas, &generated),
                rows: &fixture.live,
                config: fixture.config,
            },
            &retry.context(scheduler.clone()),
        )
        .unwrap();
        let reopened = AppendGenerationReader::open_bound(
            &destination.0,
            report.generation_artifacts,
            fixture.config,
        )
        .unwrap();
        assert_eq!(
            reopened.checkpoint_rows(fixture.all.len()).unwrap(),
            fixture.all
        );
        retry.assert_released(&scheduler);
    }
    let retry = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        plan_compaction(
            Some(&fixture.previous),
            &fixture.live,
            fixture.config,
            &retry.context(scheduler.clone())
        )
        .unwrap()
        .checkpoint_rows
        .unwrap(),
        fixture.all
    );
    retry.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_append_compaction_preserves_budget_deferral_and_work_denial() {
    let fixture = Fixture::new();
    let scheduler = scheduler();
    for config in [
        AppendPublicationConfig {
            max_compaction_rows: 2070,
            ..fixture.config
        },
        AppendPublicationConfig {
            max_compaction_payload_bytes: 1,
            ..fixture.config
        },
        AppendPublicationConfig {
            max_compaction_payload_bytes: 64 * 1024,
            ..fixture.config
        },
        AppendPublicationConfig {
            compact_after_segments: 3,
            ..fixture.config
        },
    ] {
        let reference =
            super::super::super::plan_compaction(Some(&fixture.previous), &fixture.live, config)
                .unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = plan_compaction(
            Some(&fixture.previous),
            &fixture.live,
            config,
            &probe.context(scheduler.clone()),
        )
        .unwrap();
        assert_eq!(actual.due, reference.due);
        assert_eq!(actual.checkpoint_rows, reference.checkpoint_rows);
        assert!(actual.checkpoint_rows.is_none());
        probe.assert_released(&scheduler);
    }
    let denied = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(0),
        ..LocalQosPolicy::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    let result = plan_compaction(
        Some(&fixture.previous),
        &fixture.live,
        fixture.config,
        &probe.context(denied.clone()),
    );
    assert!(
        matches!(&result, Err(AppendTableError::Admission(message)) if message.contains("checkpoint unit admission deferred")),
        "{:?}",
        result.as_ref().err()
    );
    probe.assert_released(&denied);
}

#[derive(Debug)]
struct DenyNextUnit {
    probe: Arc<CheckpointWorkProbe>,
    scheduler: LocalQosScheduler,
    after: usize,
    blocker: Mutex<Option<LocalQosPermit>>,
}

impl QosTelemetrySink for DenyNextUnit {
    fn record_qos(&self, event: QosTelemetryEvent) {
        self.probe.record_qos(event);
        if event.phase == QosTelemetryPhase::Completion
            && event.outcome == QosTelemetryOutcome::Completed
            && self.probe.completed.load(Ordering::SeqCst) == self.after
        {
            let permit = self
                .scheduler
                .try_start(WorkRequest::background(WorkClass::Mutation, 1))
                .unwrap();
            *self.blocker.lock().unwrap() = Some(permit);
        }
    }
}

#[test]
fn checkpoint_units_append_compaction_does_not_defer_mid_read_qos_denial() {
    let fixture = Fixture::new();
    let authority = fixture.authority();
    for after in [31, 100, 300] {
        let scheduler = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(scheduler.clone());
        let sink = Arc::new(DenyNextUnit {
            probe: probe.clone(),
            scheduler: scheduler.clone(),
            after,
            blocker: Mutex::new(None),
        });
        scheduler.set_telemetry_sink(Some(sink.clone()));
        let result = plan_compaction(
            Some(&fixture.previous),
            &fixture.live,
            fixture.config,
            &work,
        );
        assert!(
            matches!(&result, Err(AppendTableError::Admission(message))
            if message.contains("checkpoint unit admission deferred")),
            "{:?}",
            result.as_ref().err()
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), after);
        assert!(probe.io_waves.load(Ordering::SeqCst) > 0);
        drop(sink.blocker.lock().unwrap().take());
        scheduler.set_telemetry_sink(None);
        probe.assert_released(&scheduler);
        assert_eq!(fixture.authority(), authority);
    }
}
