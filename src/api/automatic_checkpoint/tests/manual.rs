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
use crate::{ConcurrentDatabase, DatabaseReadTransaction, QueryOutput};

const VALUES: &str = "MATCH (n:Memory) RETURN n.id AS id ORDER BY id";

trait ManualHost {
    fn write_second(&mut self);
    fn checkpoint(&mut self) -> Result<()>;
    fn watermark(&self) -> (u64, u64);
    fn values(&mut self) -> QueryOutput;
    fn report(&self) -> Option<AutomaticCheckpointReport>;
}

impl ManualHost for Database {
    fn write_second(&mut self) {
        self.query("CREATE (:Memory {id: 2})").unwrap();
    }

    fn checkpoint(&mut self) -> Result<()> {
        Database::checkpoint(self)
    }

    fn watermark(&self) -> (u64, u64) {
        let pressure = self.storage_pressure_snapshot().unwrap();
        (
            pressure.checkpoint_commit_epoch,
            pressure.current_commit_epoch,
        )
    }

    fn values(&mut self) -> QueryOutput {
        self.query(VALUES).unwrap()
    }

    fn report(&self) -> Option<AutomaticCheckpointReport> {
        self.automatic_checkpoint_report().unwrap()
    }
}

impl ManualHost for ConcurrentDatabase {
    fn write_second(&mut self) {
        self.query("CREATE (:Memory {id: 2})").unwrap();
    }

    fn checkpoint(&mut self) -> Result<()> {
        ConcurrentDatabase::checkpoint(self)
    }

    fn watermark(&self) -> (u64, u64) {
        let pressure = self.storage_pressure_snapshot().unwrap();
        (
            pressure.checkpoint_commit_epoch,
            pressure.current_commit_epoch,
        )
    }

    fn values(&mut self) -> QueryOutput {
        self.query(VALUES).unwrap()
    }

    fn report(&self) -> Option<AutomaticCheckpointReport> {
        self.automatic_checkpoint_report().unwrap()
    }
}

#[derive(Clone, Copy)]
enum BackgroundMode {
    Enabled,
    CapabilityDisabled,
    PolicyDisabled,
}

fn seed(mode: BackgroundMode) -> (Fixture, Database, DatabaseReadTransaction) {
    let fixture = Fixture::new();
    let mut config = DatabaseConfig {
        automatic_checkpoint_max_age: Duration::from_secs(3600),
        ..DatabaseConfig::default()
    };
    match mode {
        BackgroundMode::Enabled => {}
        BackgroundMode::CapabilityDisabled => {
            config.runtime_capabilities.background_maintenance = false;
        }
        BackgroundMode::PolicyDisabled => {
            config.local_qos_policy.background_enabled = false;
        }
    }
    let mut db = Database::open_with_config(&fixture.0, config).unwrap();
    db.query("CREATE (:Memory {id: 1})").unwrap();
    let debt = db
        .runtime
        .peek()
        .unwrap()
        .store
        .checkpoint_debt_snapshot()
        .unwrap();
    assert!(debt.commit_epoch > debt.checkpoint_commit_epoch);
    assert!(
        !soft_pressure(debt),
        "fixture must be below automatic byte thresholds"
    );
    assert!(debt.wal_age_millis < 3_600_000);
    let old = db.begin_read_transaction().unwrap();
    (fixture, db, old)
}

fn expected(ids: &[i64]) -> Vec<std::collections::BTreeMap<String, crate::Value>> {
    ids.iter()
        .map(|id| std::collections::BTreeMap::from([("id".into(), crate::Value::Int(*id))]))
        .collect()
}

fn complete_manual<T: ManualHost>(
    fixture: &Fixture,
    mut host: T,
    mut old: DatabaseReadTransaction,
    mode: BackgroundMode,
) {
    let before = host.watermark();
    host.write_second();
    host.checkpoint().unwrap();
    let after = host.watermark();
    assert!(
        after.0 > before.0,
        "return must follow actual checkpoint publication"
    );
    assert!(after.1 >= 2, "return must cover both captured commits");
    assert_eq!(
        after.0, after.1,
        "manual publication must cover the captured source"
    );
    assert_eq!(host.values().rows, expected(&[1, 2]));
    assert_eq!(old.query(VALUES).unwrap().rows, expected(&[1]));
    match mode {
        BackgroundMode::CapabilityDisabled => assert!(host.report().is_none()),
        BackgroundMode::Enabled | BackgroundMode::PolicyDisabled => {
            assert_eq!(host.report().unwrap().completed_checkpoints, 0);
        }
    }
    // Copy every selected artifact before closing either the host or old
    // reader. Open the copy independently, preserving the source WAL too.
    // This checks completed publication without a same-project open lock or
    // relying on handle closure to flush an enqueued manual operation.
    let copied = Fixture::new();
    copy_project(&fixture.0, &copied.0);
    let mut recovered = Database::open_with_config(
        &copied.0,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let recovery = recovered.storage_recovery_report().unwrap();
    assert_eq!(recovery.checkpoint_commit_epoch, Some(after.0));
    assert_eq!(recovery.replayed_wal_entries, 0);
    assert_eq!(recovered.query(VALUES).unwrap().rows, expected(&[1, 2]));
    drop(recovered);
    drop(old);
    drop(host);
    let mut read_only = Database::open_with_config(
        &fixture.0,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    assert_eq!(read_only.query(VALUES).unwrap().rows, expected(&[1, 2]));
    assert!(read_only.checkpoint().is_err());
    assert!(read_only.into_concurrent().checkpoint().is_err());
    let mut reopened = Database::open(&fixture.0).unwrap();
    assert_eq!(reopened.query(VALUES).unwrap().rows, expected(&[1, 2]));
}

fn copy_project(source: &std::path::Path, target: &std::path::Path) {
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let destination = target.join(entry.file_name());
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            std::fs::create_dir(&destination).unwrap();
            copy_project(&entry.path(), &destination);
        } else {
            assert!(
                kind.is_file(),
                "fixture project contains an unexpected special file"
            );
            std::fs::copy(entry.path(), destination).unwrap();
        }
    }
}

fn direct(mode: BackgroundMode, concurrent: bool) {
    let (fixture, db, old) = seed(mode);
    if concurrent {
        complete_manual(&fixture, db.into_concurrent(), old, mode);
    } else {
        complete_manual(&fixture, db, old, mode);
    }
}

#[test]
fn ordinary_manual_below_automatic_thresholds_is_durable() {
    direct(BackgroundMode::Enabled, false);
}

#[test]
fn concurrent_manual_below_automatic_thresholds_is_durable() {
    direct(BackgroundMode::Enabled, true);
}

#[test]
fn ordinary_manual_with_background_capability_disabled_is_durable() {
    direct(BackgroundMode::CapabilityDisabled, false);
}

#[test]
fn concurrent_manual_with_background_capability_disabled_is_durable() {
    direct(BackgroundMode::CapabilityDisabled, true);
}

#[test]
fn ordinary_manual_with_background_policy_disabled_is_durable() {
    direct(BackgroundMode::PolicyDisabled, false);
}

#[test]
fn concurrent_manual_with_background_policy_disabled_is_durable() {
    direct(BackgroundMode::PolicyDisabled, true);
}

fn overlapping_manual<T: ManualHost + Send + 'static>(
    fixture: &Fixture,
    host: T,
    mut old: DatabaseReadTransaction,
    control: Arc<Control>,
    resume: std::sync::mpsc::Sender<()>,
) {
    let before = host.watermark();
    let (done, returned) = std::sync::mpsc::channel();
    let caller = std::thread::spawn(move || {
        let mut host = host;
        let result = host.checkpoint();
        done.send(()).unwrap();
        (host, result)
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if control.lock().unwrap().suspensions > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "manual caller did not suspend the active owner"
        );
        std::thread::yield_now();
    }
    assert_eq!(
        returned.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    );
    control.lock().unwrap().prefix_seal_probe = None;
    resume.send(()).unwrap();
    returned.recv_timeout(Duration::from_secs(15)).unwrap();
    let (mut host, result) = caller.join().unwrap();
    result.unwrap();
    let after = host.watermark();
    assert!(after.0 > before.0);
    assert!(after.1 >= 1);
    assert_eq!(host.values().rows, expected(&[1]));
    let completed = host.report().unwrap().completed_checkpoints;
    host.write_second();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if host.report().unwrap().completed_checkpoints > completed {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "automatic work did not resume after manual completion"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(host.values().rows, expected(&[1, 2]));
    assert_eq!(old.query(VALUES).unwrap().rows, expected(&[1]));
    drop(old);
    drop(host);
    let mut reopened = Database::open(&fixture.0).unwrap();
    assert_eq!(reopened.query(VALUES).unwrap().rows, expected(&[1, 2]));
}

fn overlap(concurrent: bool) {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(
        &fixture.0,
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let suspension = db.runtime.suspend_automatic_checkpoint().unwrap().unwrap();
    let control = db.runtime.checkpoint_control_for_test();
    let (sealed, observation) = std::sync::mpsc::channel();
    let (resume, continuation) = std::sync::mpsc::channel();
    control.lock().unwrap().prefix_seal_probe = Some(Arc::new(PrefixSealProbe {
        sealed,
        resume: Mutex::new(continuation),
    }));
    db.query("CREATE (:Memory {id: 1})").unwrap();
    let old = db.begin_read_transaction().unwrap();
    drop(suspension);
    assert!(observation.recv_timeout(Duration::from_secs(15)).unwrap() >= 1);
    assert_eq!(control.lock().unwrap().phase, Phase::Preparing);
    if concurrent {
        overlapping_manual(&fixture, db.into_concurrent(), old, control, resume);
    } else {
        overlapping_manual(&fixture, db, old, control, resume);
    }
}

#[test]
fn ordinary_manual_during_automatic_preparation_resumes_automatic_progress() {
    overlap(false);
}

#[test]
fn concurrent_manual_during_automatic_preparation_resumes_automatic_progress() {
    overlap(true);
}

fn complete_while_pending<T: ManualHost>(
    fixture: &Fixture,
    mut host: T,
    mut old: DatabaseReadTransaction,
    control: &Control,
    governor: &RuntimeGovernor,
) {
    let before = host.watermark();
    assert!(control.lock().unwrap().pending.is_some());
    // Background admission stays unavailable throughout the manual operation.
    host.checkpoint().unwrap();
    let after = host.watermark();
    assert!(after.0 > before.0);
    assert_eq!(after.0, after.1);
    assert_eq!(after.1, before.1);
    assert!(after.1 >= 2);
    assert!(control.lock().unwrap().pending.is_none());
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(host.values().rows, expected(&[1, 2]));
    assert_eq!(old.query(VALUES).unwrap().rows, expected(&[1]));

    let copied = Fixture::new();
    copy_project(&fixture.0, &copied.0);
    let mut recovered = Database::open_with_config(
        &copied.0,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let report = recovered.storage_recovery_report().unwrap();
    assert_eq!(report.checkpoint_commit_epoch, Some(after.0));
    assert_eq!(report.replayed_wal_entries, 0);
    assert_eq!(recovered.query(VALUES).unwrap().rows, expected(&[1, 2]));
    drop(recovered);
    drop(old);
    drop(host);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

fn pending_manual(concurrent: bool) {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(
        &fixture.0,
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let suspension = db.runtime.suspend_automatic_checkpoint().unwrap().unwrap();
    let control = db.runtime.checkpoint_control_for_test();
    let governor = governor(512 * 1024 * 1024);
    db.set_runtime_governor(governor.clone());
    let (sealed, observation) = std::sync::mpsc::channel();
    let (resume, continuation) = std::sync::mpsc::channel();
    control.lock().unwrap().prefix_seal_probe = Some(Arc::new(PrefixSealProbe {
        sealed,
        resume: Mutex::new(continuation),
    }));
    db.query("CREATE (:Memory {id: 1})").unwrap();
    let old = db.begin_read_transaction().unwrap();
    drop(suspension);
    observation.recv_timeout(Duration::from_secs(15)).unwrap();
    db.query("CREATE (:Memory {id: 2})").unwrap();
    control.lock().unwrap().prefix_seal_probe = None;
    let mut resources = governor.snapshot().resources;
    resources.memory.pressure = hawdb_qos::RuntimeMemoryPressure::Critical;
    governor.update_resources(resources);
    resume.send(()).unwrap();
    wait_for(&db, |report| report.deferred_attempts > 0);
    if concurrent {
        complete_while_pending(&fixture, db.into_concurrent(), old, &control, &governor);
    } else {
        complete_while_pending(&fixture, db, old, &control, &governor);
    }
}

#[test]
fn ordinary_manual_completes_while_automatic_memory_admission_is_denied() {
    pending_manual(false);
}

#[test]
fn concurrent_manual_completes_while_automatic_memory_admission_is_denied() {
    pending_manual(true);
}

fn finish_manual_retirement<T: ManualHost>(
    fixture: &Fixture,
    mut host: T,
    mut old: DatabaseReadTransaction,
    control: &Control,
    governor: &RuntimeGovernor,
) {
    // Background memory pressure remains critical for the entire manual call.
    assert_eq!(
        governor.snapshot().resources.memory.pressure,
        hawdb_qos::RuntimeMemoryPressure::Critical
    );
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    host.checkpoint().unwrap();
    let state = control.lock().unwrap();
    assert_eq!(state.phase, Phase::Idle);
    assert!(state.retired.is_none());
    drop(state);
    let watermark = host.watermark();
    assert_eq!(watermark.0, watermark.1);
    assert!(watermark.0 >= 1);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(host.values().rows, expected(&[1]));
    assert_eq!(old.query(VALUES).unwrap().rows, expected(&[1]));
    let copied = Fixture::new();
    copy_project(&fixture.0, &copied.0);
    let mut recovered = Database::open_with_config(
        &copied.0,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let report = recovered.storage_recovery_report().unwrap();
    assert_eq!(report.checkpoint_commit_epoch, Some(watermark.0));
    assert_eq!(report.replayed_wal_entries, 0);
    assert_eq!(recovered.query(VALUES).unwrap().rows, expected(&[1]));
    drop(recovered);
    drop(old);
    drop(host);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

fn denied_retirement_manual(concurrent: bool) {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(
        &fixture.0,
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let suspension = db.runtime.suspend_automatic_checkpoint().unwrap().unwrap();
    let control = db.runtime.checkpoint_control_for_test();
    let governor = retirement_governor();
    db.set_runtime_governor(governor.clone());
    db.query("CREATE (:Memory {id: 1})").unwrap();
    let old = db.begin_read_transaction().unwrap();
    drop(suspension);
    wait_for(&db, |report| report.completed_checkpoints == 1);
    let mut resources = governor.snapshot().resources;
    resources.memory.pressure = hawdb_qos::RuntimeMemoryPressure::Critical;
    governor.update_resources(resources);
    drop(db.runtime.get_mut().unwrap());
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let state = control.lock().unwrap();
        if state.retired.is_some() && state.report.deferred_attempts > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "retirement did not defer: {:?}",
            state.report
        );
        drop(state);
        std::thread::sleep(Duration::from_millis(5));
    }
    if concurrent {
        finish_manual_retirement(&fixture, db.into_concurrent(), old, &control, &governor);
    } else {
        finish_manual_retirement(&fixture, db, old, &control, &governor);
    }
}

#[test]
fn ordinary_manual_completes_while_adopted_retirement_admission_is_denied() {
    denied_retirement_manual(false);
}

#[test]
fn concurrent_manual_completes_while_adopted_retirement_admission_is_denied() {
    denied_retirement_manual(true);
}
