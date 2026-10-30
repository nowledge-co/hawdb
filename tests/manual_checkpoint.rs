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

//! Public App checkpoint contract, with and without background support compiled in.
#![cfg(not(target_arch = "wasm32"))]

use hawdb::{ConcurrentDatabase, Database, DatabaseConfig, QueryOutput, Result, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const VALUES: &str = "MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id";

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb-manual-checkpoint-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

enum Host {
    Ordinary(Box<Database>),
    Concurrent(ConcurrentDatabase),
}

impl Host {
    fn checkpoint(&mut self) -> Result<()> {
        match self {
            Self::Ordinary(db) => db.checkpoint(),
            Self::Concurrent(db) => db.checkpoint(),
        }
    }

    fn values(&mut self) -> QueryOutput {
        match self {
            Self::Ordinary(db) => db.query(VALUES).unwrap(),
            Self::Concurrent(db) => db.query(VALUES).unwrap(),
        }
    }

    fn checkpoint_epoch(&self) -> u64 {
        match self {
            Self::Ordinary(db) => {
                db.storage_pressure_snapshot()
                    .unwrap()
                    .checkpoint_commit_epoch
            }
            Self::Concurrent(db) => {
                db.storage_pressure_snapshot()
                    .unwrap()
                    .checkpoint_commit_epoch
            }
        }
    }
}

fn expected(ids: &[i64]) -> Vec<BTreeMap<String, Value>> {
    ids.iter()
        .map(|id| {
            BTreeMap::from([
                ("id".into(), Value::Int(*id)),
                ("body".into(), Value::String("p".repeat(512))),
            ])
        })
        .collect()
}

fn copy_project(source: &Path, target: &Path) {
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let destination = target.join(entry.file_name());
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            std::fs::create_dir(&destination).unwrap();
            copy_project(&entry.path(), &destination);
        } else {
            assert!(kind.is_file());
            std::fs::copy(entry.path(), destination).unwrap();
        }
    }
}

fn manual(config: DatabaseConfig, concurrent: bool) {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config).unwrap();
    let write = |db: &mut Database, id| {
        db.query_with_params(
            "CREATE (:Memory {id: $id, body: $body})",
            &BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("body".into(), Value::String("p".repeat(512))),
            ]),
        )
        .unwrap();
    };
    write(&mut db, 1);
    let mut old = db.begin_read_transaction().unwrap();
    write(&mut db, 2);
    let pressure = db.storage_pressure_snapshot().unwrap();
    assert!(pressure.current_commit_epoch > pressure.checkpoint_commit_epoch);
    assert!(pressure.wal_age_millis < 3_600_000);
    assert!(
        pressure
            .wal_pressure_ratio_per_million
            .is_some_and(|ratio| ratio < 700_000),
        "fixture must remain below the automatic WAL soft threshold: {pressure:?}"
    );
    assert!(
        pressure
            .delta_pressure_ratio_per_million
            .is_none_or(|ratio| ratio < 700_000),
        "fixture must remain below the automatic delta soft threshold: {pressure:?}"
    );
    let captured_epoch = db.commit_epoch().unwrap();
    let mut host = if concurrent {
        Host::Concurrent(db.into_concurrent())
    } else {
        Host::Ordinary(Box::new(db))
    };
    host.checkpoint().unwrap();
    assert_eq!(host.checkpoint_epoch(), captured_epoch);
    assert_eq!(host.values().rows, expected(&[1, 2]));
    assert_eq!(old.query(VALUES).unwrap().rows, expected(&[1]));

    // Recover an independent copy while the caller and historical reader are
    // still alive. Closing either handle must not complete an enqueued operation.
    let copied = Fixture::new();
    copy_project(&fixture.0, &copied.0);
    let mut recovered = Database::open_with_config(
        &copied.0,
        DatabaseConfig {
            read_only: true,
            recovery_mode: hawdb::RecoveryMode::Strict,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let report = recovered.storage_recovery_report().unwrap();
    assert_eq!(report.checkpoint_commit_epoch, Some(captured_epoch));
    assert_eq!(report.replayed_wal_entries, 0);
    assert_eq!(recovered.query(VALUES).unwrap().rows, expected(&[1, 2]));
    assert!(recovered.checkpoint().is_err());
    assert!(recovered.into_concurrent().checkpoint().is_err());
    host.checkpoint().unwrap();
    assert_eq!(old.query(VALUES).unwrap().rows, expected(&[1]));
    drop(old);
    drop(host);
    let mut reopened = Database::open(&fixture.0).unwrap();
    assert_eq!(reopened.query(VALUES).unwrap().rows, expected(&[1, 2]));
}

fn config() -> DatabaseConfig {
    DatabaseConfig {
        automatic_checkpoint_max_age: Duration::from_secs(3600),
        ..DatabaseConfig::default()
    }
}

#[test]
fn manual_checkpoint_below_automatic_thresholds_completes_durable_publication() {
    for concurrent in [false, true] {
        manual(config(), concurrent);
    }
}

#[test]
fn manual_checkpoint_without_background_capability_completes_durable_publication() {
    for concurrent in [false, true] {
        let mut config = config();
        config.runtime_capabilities.background_maintenance = false;
        manual(config, concurrent);
    }
}

#[test]
fn manual_checkpoint_without_background_admission_completes_durable_publication() {
    for concurrent in [false, true] {
        let mut config = config();
        config.local_qos_policy.background_enabled = false;
        manual(config, concurrent);
    }
}

fn embedded_governor() -> hawdb::RuntimeGovernor {
    hawdb::RuntimeGovernor::new(
        hawdb::RuntimeGovernorConfig::shared_host(),
        hawdb::RuntimeResourceSnapshot::from_parts(
            hawdb::RuntimeResourceBudget::from_limits(
                std::num::NonZeroUsize::new(8).unwrap(),
                None,
                None,
            ),
            hawdb::RuntimeMemorySnapshot::from_limits(
                Some(8 << 30),
                Some(8 << 30),
                None,
                None,
                None,
            ),
        ),
        hawdb::IoConcurrencyBudget::new(1, 1),
    )
}

fn embedded_handle(
    root: &Fixture,
    governor: hawdb::RuntimeGovernor,
) -> hawdb::NowledgeMemEmbeddedStoreHandle {
    let options = hawdb::NowledgeMemOpenOptions::graph_only(
        &root.0,
        hawdb::NowledgeMemGraphMode::WritableCutover,
    )
    .with_database_config(config());
    let (handle, _) =
        hawdb::NowledgeMemEmbeddedStoreHandle::open_with_options_and_runtime_governor(
            options, governor,
        )
        .unwrap();
    handle
        .query_with_report("CREATE (:Memory {id: 1, body: 'checkpoint body'})")
        .unwrap();
    handle
}

fn embedded_state_after_reopen(root: &Fixture) -> (u64, Option<u64>, usize) {
    let mut recovered = Database::open_with_config(
        &root.0,
        DatabaseConfig {
            read_only: true,
            recovery_mode: hawdb::RecoveryMode::Strict,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let report = recovered.storage_recovery_report().unwrap();
    assert_eq!(
        recovered.query(VALUES).unwrap().rows,
        vec![BTreeMap::from([
            ("id".into(), Value::Int(1)),
            ("body".into(), Value::String("checkpoint body".into())),
        ])],
    );
    (
        recovered.commit_epoch().unwrap(),
        report.checkpoint_commit_epoch,
        report.replayed_wal_entries,
    )
}

fn copied_embedded_state(root: &Fixture) -> (u64, Option<u64>, usize) {
    let copied = Fixture::new();
    copy_project(&root.0, &copied.0);
    embedded_state_after_reopen(&copied)
}

#[test]
fn embedded_checkpoint_is_available_with_background_io_saturated() {
    let root = Fixture::new();
    let governor = embedded_governor();
    let handle = embedded_handle(&root, governor.clone());
    let pending = copied_embedded_state(&root);
    assert!(pending.0 > pending.1.unwrap_or(0));
    assert!(pending.2 > 0);
    let background = governor
        .try_admit(hawdb::RuntimeWorkRequest::io(
            hawdb::RuntimeWorkPriority::Background,
            1,
            0,
        ))
        .unwrap();
    let before = governor.snapshot();
    assert_eq!(before.active_background_io_slots, 1);
    assert_eq!(before.active_foreground_io_slots, 0);

    let checkpoint = handle.checkpoint();
    assert!(
        checkpoint.is_ok(),
        "an explicit foreground checkpoint must not be rejected by retained background I/O: {checkpoint:?}",
    );
    let after = governor.snapshot();
    assert_eq!(after.active_background_io_slots, 1);
    assert_eq!(after.active_foreground_io_slots, 0);
    assert_eq!(after.active_cpu_slots, 0);
    assert_eq!(after.admitted_memory_bytes, before.admitted_memory_bytes);

    // Reopen an independent copy while both original owners remain alive.
    assert_eq!(
        copied_embedded_state(&root),
        (pending.0, Some(pending.0), 0)
    );
    drop(background);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(handle);
    assert_eq!(
        embedded_state_after_reopen(&root),
        (pending.0, Some(pending.0), 0)
    );
}

#[test]
fn embedded_checkpoint_retains_foreground_io_refusal_and_refunds() {
    let root = Fixture::new();
    let governor = embedded_governor();
    let handle = embedded_handle(&root, governor.clone());
    let pending = copied_embedded_state(&root);
    assert!(pending.0 > pending.1.unwrap_or(0));
    assert!(pending.2 > 0);
    let foreground = governor
        .try_admit(hawdb::RuntimeWorkRequest::io(
            hawdb::RuntimeWorkPriority::Foreground,
            1,
            0,
        ))
        .unwrap();
    let before = governor.snapshot();
    assert_eq!(before.active_foreground_io_slots, 1);
    let checkpoint = handle.checkpoint();
    assert!(
        matches!(checkpoint, Err(hawdb::HawDBError::Execution(ref reason))
            if reason == "runtime admission io_saturated: requested 1, available 0"),
        "foreground admission must remain effective: {checkpoint:?}",
    );
    let after = governor.snapshot();
    assert_eq!(after.active_foreground_io_slots, 1);
    assert_eq!(after.active_background_io_slots, 0);
    assert_eq!(after.active_cpu_slots, 0);
    assert_eq!(after.admitted_memory_bytes, before.admitted_memory_bytes);
    // The refusal cannot publish a checkpoint or change recoverable rows,
    // observed before the retained owner is released or a retry is attempted.
    assert_eq!(copied_embedded_state(&root), pending);
    drop(foreground);
    assert_eq!(governor.snapshot().active_foreground_io_slots, 0);
    assert!(!handle.storage_handle_poisoned().unwrap());
    handle.checkpoint().unwrap();
    drop(handle);
    assert_eq!(
        embedded_state_after_reopen(&root),
        (pending.0, Some(pending.0), 0)
    );
}
