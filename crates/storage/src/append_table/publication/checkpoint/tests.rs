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
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
use std::sync::atomic::{AtomicU64, Ordering};

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

fn assert_stopped<T: std::fmt::Debug>(result: Result<T, AppendTableError>) {
    assert!(
        matches!(&result, Err(AppendTableError::Admission(message)) if message.contains("checkpoint build") && message.contains("cancelled")),
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
    assert_stopped(write_artifact(
        &directory.0,
        "candidate",
        &bytes,
        Some(&probe.context(scheduler.clone())),
    ));
    // Cancellation after durable rename is a lost reply, not a rollback of
    // the complete private artifact; retain it as evidence for its owner.
    assert_eq!(fs::read(directory.0.join("candidate")).unwrap(), bytes);
    assert!(!directory.0.join("candidate.tmp").exists());
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
