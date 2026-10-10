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
use crate::Value;
use std::collections::BTreeMap;

fn add(store: &mut GraphStore, catalog: &mut Catalog, id: i64) {
    store
        .create_node(
            catalog,
            "Memory",
            BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("body".into(), Value::String("r".repeat(512))),
            ]),
        )
        .unwrap();
}

fn assert_released(governor: &RuntimeGovernor) {
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.admitted_memory_bytes, 0);
    assert_eq!(snapshot.active_background_tasks, 0);
    assert_eq!(snapshot.active_background_io_slots, 0);
}

fn assert_ids(db: &mut Database, total: i64) {
    let actual = db
        .query("MATCH (n:Memory) RETURN n.id AS id ORDER BY n.id")
        .unwrap()
        .rows;
    assert_eq!(
        actual,
        (1..=total)
            .map(|id| BTreeMap::from([("id".into(), Value::Int(id))]))
            .collect::<Vec<_>>()
    );
}

#[test]
fn persistent_preparation_io_failure_stops_and_manual_checkpoint_rearms_the_owner() {
    let fixture = Fixture::new();
    let governor = retirement_governor();
    let mut db = Database::open_with_config(
        &fixture.0,
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.set_runtime_governor(governor.clone());
    // A real filesystem operation fails before any selector or WAL mutation.
    let root = db
        .runtime
        .get()
        .unwrap()
        .store
        .durable_root_path()
        .unwrap()
        .to_path_buf();
    let generation = db
        .runtime
        .get()
        .unwrap()
        .store
        .checkpoint_debt_snapshot()
        .unwrap()
        .wal_generation
        .checked_add(1)
        .unwrap();
    let obstruction = root.join(format!(".checkpoint.{generation}.prepare"));
    std::fs::write(&obstruction, b"not a directory").unwrap();
    db.query("CREATE (:Memory {id: 1})").unwrap();
    wait_for(&db, |r| r.operation_retry_exhausted && !r.preparing);
    let report = db.automatic_checkpoint_report().unwrap().unwrap();
    assert_eq!(report.operation_failures, u64::from(MAX_OPERATION_FAILURES));
    assert_eq!(report.completed_checkpoints, 0);
    assert_released(&governor);
    // Writes advance the captured source, but cannot restart a failed circuit.
    for id in 2..=4 {
        db.query_with_params(
            "CREATE (:Memory {id: $id})",
            &BTreeMap::from([("id".into(), Value::Int(id))]),
        )
        .unwrap();
    }
    assert_ids(&mut db, 4);
    let before = db.storage_pressure_snapshot().unwrap();
    std::fs::remove_file(&obstruction).unwrap();
    // Clearing an OS fault alone does not cause another unbounded retry loop.
    assert!(
        db.automatic_checkpoint_report()
            .unwrap()
            .unwrap()
            .operation_retry_exhausted
    );
    db.checkpoint().unwrap();
    let after = db.storage_pressure_snapshot().unwrap();
    assert_eq!(after.checkpoint_commit_epoch, after.current_commit_epoch);
    assert!(after.checkpoint_commit_epoch > before.checkpoint_commit_epoch);
    assert!(
        !db.automatic_checkpoint_report()
            .unwrap()
            .unwrap()
            .operation_retry_exhausted
    );
    db.query("CREATE (:Memory {id: 5})").unwrap();
    wait_for(&db, |r| r.completed_checkpoints == 1);
    assert_ids(&mut db, 5);
    assert_eq!(
        db.automatic_checkpoint_report()
            .unwrap()
            .unwrap()
            .operation_failures,
        3
    );
    drop(db);
    assert_released(&governor);
    let mut reopened = Database::open_with_config(
        &fixture.0,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    assert_ids(&mut reopened, 5);
}

#[test]
fn private_wal_io_failure_releases_a_parked_candidate_without_poisoning_the_writer() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    add(&mut store, &mut catalog, 1);
    let old = store.snapshot_for_read();
    let control = Arc::new(Control::default());
    let governor = retirement_governor();
    let (sealed, observation) = std::sync::mpsc::channel();
    let (resume, continuation) = std::sync::mpsc::channel();
    control.lock().unwrap().prefix_seal_probe = Some(Arc::new(PrefixSealProbe {
        sealed,
        resume: Mutex::new(continuation),
    }));
    let mut owner = Owner::start(
        control.clone(),
        &store,
        &catalog,
        Arc::new(Mutex::new(ReaderPins::default())),
        &DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
        LocalQosScheduler::new(hawdb_qos::LocalQosPolicy::default()),
        Some(governor.clone()),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        observation.recv_timeout(Duration::from_secs(15)).unwrap(),
        1
    );
    add(&mut store, &mut catalog, 2);
    {
        let mut state = control.lock().unwrap();
        let retired = control.submit(&mut state, &store, &catalog);
        state.prefix_seal_probe = None;
        drop(state);
        drop(retired);
    }
    let private_wal = fixture.0.join("wal.1.hawdb");
    let saved_wal = fixture.0.join("private-wal-fault-fixture");
    std::fs::rename(&private_wal, &saved_wal).unwrap();
    resume.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let state = control.lock().unwrap();
        if state.report.operation_retry_exhausted && state.phase == Phase::Idle {
            assert!(state.pending.is_none());
            assert!(state.selected.is_none());
            assert_eq!(state.report.operation_failures, 3);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "private WAL fault did not stop: {:?}",
            state.report
        );
        drop(state);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_released(&governor);
    assert_eq!(store.scan_nodes(None).count(), 2);
    assert_eq!(old.scan_nodes(None).count(), 1);
    add(&mut store, &mut catalog, 3);
    {
        let mut state = control.lock().unwrap();
        let retired = control.submit(&mut state, &store, &catalog);
        assert!(state.report.operation_retry_exhausted);
        drop(state);
        drop(retired);
    }
    // The failed private namespace never became disk authority.
    owner.stop();
    std::fs::remove_file(&saved_wal).unwrap();
    store.checkpoint(&catalog).unwrap();
    assert_eq!(store.scan_nodes(None).count(), 3);
    assert_eq!(old.scan_nodes(None).count(), 1);
    drop(old);
    drop(store);
    let mut recovered_catalog = Catalog::default();
    let recovered = GraphStore::open(&fixture.0, &mut recovered_catalog).unwrap();
    assert_eq!(recovered.scan_nodes(None).count(), 3);
}
