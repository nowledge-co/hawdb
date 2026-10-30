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
use std::num::NonZeroUsize;

fn assert_values(store: &GraphStore, count: usize) {
    assert_eq!(store.node_count_for_label(None), count);
    for id in 0..count {
        let node = store
            .node_owned(hawdb_storage::NodeId(id as u64))
            .unwrap()
            .unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64 + 1));
    }
}

#[test]
fn contended_publication_releases_io_before_waiting_for_the_writer_gate() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("id".into(), Value::Int(1))]),
        )
        .unwrap();
    let old = store.snapshot_for_read();
    let control = Arc::new(Control::default());
    let governor = retirement_governor();
    let permit = governor
        .try_admit(
            RuntimeWorkRequest::io(hawdb_qos::RuntimeWorkPriority::Background, 0, 0)
                .with_io_wave_slots(1),
        )
        .unwrap();
    let io_context = permit.bind_task_context(RuntimeTaskContext::default());
    // Retain access to the physical wave pool without occupying the sole
    // background task slot that the checkpoint owner needs.
    drop(permit);
    let (paused, observed) = std::sync::mpsc::channel();
    let (resume, continuation) = std::sync::mpsc::channel();
    control.lock().unwrap().publication_io_probe = Some(Arc::new(OwnerPauseProbe {
        paused,
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
    observed.recv_timeout(Duration::from_secs(15)).unwrap();
    let mut state = control.lock_frontend().unwrap();
    assert_eq!(state.phase, Phase::Preparing);
    assert_eq!(governor.snapshot().active_background_io_slots, 1);
    resume.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while governor.snapshot().active_background_io_slots != 0
        || governor.snapshot().active_cpu_slots != 0
        || !control.publication_requested.load(Ordering::Acquire)
    {
        assert!(
            Instant::now() < deadline,
            "publication retained I/O while waiting for Control"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert!(control.publication_requested.load(Ordering::Acquire));
    // Keep the writer mutex held: the same single-slot physical pool must
    // already be reusable, without waiting for frontend mutation to finish.
    let wave = io_context
        .acquire_io_wave(NonZeroUsize::MIN)
        .unwrap()
        .unwrap();
    assert_eq!(governor.snapshot().active_background_io_slots, 1);
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("id".into(), Value::Int(2))]),
        )
        .unwrap();
    let retired = control.submit(&mut state, &store, &catalog);
    assert_values(&store, 2);
    assert_values(&old, 1);
    drop(wave);
    drop(io_context);
    drop(state);
    drop(retired);
    loop {
        let state = control.lock().unwrap();
        if state.selected.is_some() {
            assert_eq!(state.report.completed_checkpoints, 1);
            assert_eq!(
                state.attempts_started, 1,
                "a sealed publication must resume its parked execution within the same attempt"
            );
            assert!(state.report.deferred_attempts >= 1);
            assert_eq!(state.report.operation_failures, 0);
            assert_eq!(state.report.failed_attempts, 0);
            assert_eq!(state.report.planning_scans, 1);
            break;
        }
        drop(state);
        assert!(
            Instant::now() < deadline,
            "the retained candidate did not resume"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    {
        let mut state = control.lock_frontend().unwrap();
        control.adopt(&mut state, &mut store).unwrap();
    }
    owner.stop();
    assert_values(&store, 2);
    assert_values(&old, 1);
    drop(old);
    drop(store);
    drop(owner);
    drop(control);
    let resources = governor.snapshot();
    assert_eq!(resources.admitted_memory_bytes, 0);
    assert_eq!(resources.active_cpu_slots, 0);
    assert_eq!(resources.active_background_tasks, 0);
    assert_eq!(resources.active_background_io_slots, 0);
    let reopened = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_values(&reopened, 2);
}

#[test]
fn embedded_checkpoint_admits_foreground_while_actual_owner_holds_io() {
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
    let governor = RuntimeGovernor::new(
        hawdb_qos::RuntimeGovernorConfig::shared_host(),
        hawdb_qos::RuntimeResourceSnapshot::from_parts(
            hawdb_qos::RuntimeResourceBudget::from_limits(
                NonZeroUsize::new(8).unwrap(),
                None,
                None,
            ),
            hawdb_qos::RuntimeMemorySnapshot::from_limits(
                Some(8 << 30),
                Some(8 << 30),
                None,
                None,
                None,
            ),
        ),
        hawdb_qos::IoConcurrencyBudget::new(1, 1),
    );
    governor.pin_resources();
    db.set_runtime_governor(governor.clone());
    db.query("CREATE (:Memory {id: 1})").unwrap();
    let captured_epoch = db.commit_epoch().unwrap();
    let mut old = db.begin_read_transaction().unwrap();
    let (paused, observed) = std::sync::mpsc::channel();
    let (resume, continuation) = std::sync::mpsc::channel();
    let (waiting, waited) = std::sync::mpsc::channel();
    {
        let mut state = control.lock().unwrap();
        state.publication_io_probe = Some(Arc::new(OwnerPauseProbe {
            paused,
            resume: Mutex::new(continuation),
        }));
        state.manual_wait_probe = Some(waiting);
    }
    let graph = crate::NowledgeMemGraph::from_database_with_runtime_governor(
        db,
        crate::NowledgeMemGraphMode::WritableCutover,
        governor.clone(),
    );
    let handle = crate::NowledgeMemEmbeddedStoreHandle::new(crate::NowledgeMemEmbeddedStore::new(
        graph, None,
    ))
    .unwrap();

    struct ResetWaitProbe(Arc<Control>);
    impl Drop for ResetWaitProbe {
        fn drop(&mut self) {
            self.0
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .manual_wait_probe = None;
        }
    }

    std::thread::scope(|scope| {
        let _reset_wait_probe = ResetWaitProbe(control.clone());
        // This guard must drop before scoped callers join, including unwinding.
        let pause = HandoffPauseForTest { observed, resume };
        drop(suspension);
        pause.wait(Duration::from_secs(15)).unwrap();
        assert_eq!(control.lock().unwrap().phase, Phase::Preparing);
        assert_eq!(governor.snapshot().active_background_io_slots, 1);
        let (done, returned) = std::sync::mpsc::channel();
        let checkpoint_handle = &handle;
        let caller = scope.spawn(move || {
            done.send(
                checkpoint_handle
                    .checkpoint()
                    .map_err(|error| error.to_string()),
            )
            .unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(15);
        let phase = loop {
            match returned.try_recv() {
                Ok(result) => {
                    panic!("embedded checkpoint returned before draining its owner: {result:?}")
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("checkpoint caller stopped")
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
            }
            match waited.try_recv() {
                Ok(phase) => break phase,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("manual wait observer stopped")
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
            }
            assert!(
                Instant::now() < deadline,
                "manual caller did not suspend the actual owner"
            );
            std::thread::yield_now();
        };
        assert_eq!(phase, Phase::Preparing);
        assert!(control.lock().unwrap().suspensions > 0);
        let admitted = governor.snapshot();
        assert_eq!(admitted.active_background_io_slots, 1);
        assert_eq!(admitted.active_foreground_io_slots, 1);
        drop(pause);
        let checkpoint = returned.recv_timeout(Duration::from_secs(15)).unwrap();
        caller.join().unwrap();
        checkpoint.unwrap();
    });

    fn copy_family(source: &std::path::Path, target: &std::path::Path) {
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let destination = target.join(entry.file_name());
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                std::fs::create_dir(&destination).unwrap();
                copy_family(&entry.path(), &destination);
            } else {
                assert!(kind.is_file());
                std::fs::copy(entry.path(), destination).unwrap();
            }
        }
    }

    let copied = Fixture::new();
    copy_family(&fixture.0, &copied.0);
    let mut reopened = Database::open_with_config(
        &copied.0,
        DatabaseConfig {
            read_only: true,
            recovery_mode: crate::RecoveryMode::Strict,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let recovery = reopened.storage_recovery_report().unwrap();
    assert_eq!(recovery.checkpoint_commit_epoch, Some(captured_epoch));
    assert_eq!(recovery.replayed_wal_entries, 0);
    let values = "MATCH (n:Memory) RETURN n.id AS id ORDER BY id";
    let expected = vec![BTreeMap::from([("id".into(), Value::Int(1))])];
    assert_eq!(reopened.query(values).unwrap().rows, expected);
    assert_eq!(old.query(values).unwrap().rows, expected);
    drop(old);
    drop(handle);
    let resources = governor.snapshot();
    assert_eq!(resources.active_foreground_tasks, 0);
    assert_eq!(resources.active_background_tasks, 0);
    assert_eq!(resources.active_foreground_io_slots, 0);
    assert_eq!(resources.active_background_io_slots, 0);
    assert_eq!(resources.active_cpu_slots, 0);
    assert_eq!(resources.admitted_memory_bytes, 0);
}
