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
use crate::{DurabilityPolicy, Value};
use std::collections::BTreeMap;

fn values(store: &GraphStore) -> Vec<(i64, String)> {
    let mut rows: Vec<_> = store
        .scan_nodes(None)
        .map(|node| {
            let Value::Int(id) = node.properties["id"] else {
                panic!("unexpected id")
            };
            let Value::String(body) = &node.properties["body"] else {
                panic!("unexpected body")
            };
            (id, body.clone())
        })
        .collect();
    rows.sort_unstable();
    rows
}

fn wait(control: &Control, predicate: impl Fn(AutomaticCheckpointReport) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let report = control.report().unwrap().unwrap();
        if predicate(report) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "owner did not advance: {report:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn denied_suffix_resumes(durability: DurabilityPolicy) {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability(&fixture.0, &mut catalog, durability).unwrap();
    let body = "r".repeat(512);
    let create = |store: &mut GraphStore, catalog: &mut Catalog, id| {
        store
            .create_node(
                catalog,
                "Memory",
                BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("body".into(), Value::String(body.clone())),
                ]),
            )
            .unwrap();
    };
    create(&mut store, &mut catalog, 1);
    let old = store.snapshot_for_read();
    let control = Arc::new(Control::default());
    let governor = governor(512 * 1024 * 1024);
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
    let base = fixture.0.join("checkpoint.1.hawdb");
    let bytes = std::fs::read(&base).unwrap();
    create(&mut store, &mut catalog, 2);
    {
        let mut state = control.lock().unwrap();
        let retired = control.submit(&mut state, &store, &catalog);
        state.prefix_seal_probe = None;
        drop(state);
        drop(retired);
    }
    let mut resources = governor.snapshot().resources;
    resources.memory.pressure = hawdb_qos::RuntimeMemoryPressure::Critical;
    governor.update_resources(resources);
    resume.send(()).unwrap();
    wait(&control, |report| {
        report.deferred_attempts + report.failed_attempts > 0
    });
    assert_eq!(
        std::fs::read(&base).expect("temporary memory denial must keep the same prepared base"),
        bytes
    );
    assert_eq!(control.report().unwrap().unwrap().completed_checkpoints, 0);
    assert_eq!(control.report().unwrap().unwrap().operation_failures, 0);
    assert!(!control.report().unwrap().unwrap().operation_retry_exhausted);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    resources.memory.pressure = hawdb_qos::RuntimeMemoryPressure::Normal;
    governor.update_resources(resources);
    // Resource recovery alone must resume; no new write or manual checkpoint.
    wait(&control, |report| report.completed_checkpoints == 1);
    assert_eq!(std::fs::read(&base).unwrap(), bytes);
    {
        let mut state = control.lock().unwrap();
        control.adopt(&mut state, &mut store).unwrap();
    }
    owner.stop();
    assert_eq!(values(&store), vec![(1, body.clone()), (2, body.clone())]);
    assert_eq!(values(&old), vec![(1, body.clone())]);
    drop(old);
    drop(store);
    drop(catalog);
    drop(owner);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let mut recovered_catalog = Catalog::default();
    let recovered = GraphStore::open(&fixture.0, &mut recovered_catalog).unwrap();
    assert_eq!(values(&recovered), vec![(1, body.clone()), (2, body)]);
}

#[test]
fn synchronous_denial_keeps_the_base_and_recovery_resumes_without_a_write() {
    denied_suffix_resumes(DurabilityPolicy::SyncOnEveryWrite);
}

#[test]
fn relaxed_denial_keeps_the_base_and_recovery_resumes_without_a_write() {
    denied_suffix_resumes(DurabilityPolicy::SyncOnCheckpoint);
}

#[test]
fn shutdown_releases_pending_candidate_while_observers_keep_control_alive() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    let body = "s".repeat(512);
    let properties = BTreeMap::from([
        ("id".into(), Value::Int(1)),
        ("body".into(), Value::String(body.clone())),
    ]);
    store
        .create_node(&mut catalog, "Memory", properties)
        .unwrap();
    let control = Arc::new(Control::default());
    let governor = governor(512 * 1024 * 1024);
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
    observation.recv_timeout(Duration::from_secs(15)).unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".into(), Value::Int(2)),
                ("body".into(), Value::String(body.clone())),
            ]),
        )
        .unwrap();
    {
        let mut state = control.lock().unwrap();
        let retired = control.submit(&mut state, &store, &catalog);
        state.prefix_seal_probe = None;
        drop(state);
        drop(retired);
    }
    let mut resources = governor.snapshot().resources;
    resources.memory.pressure = hawdb_qos::RuntimeMemoryPressure::Critical;
    governor.update_resources(resources);
    resume.send(()).unwrap();
    wait(&control, |report| report.deferred_attempts > 0);
    assert!(control.lock().unwrap().pending.is_some());
    assert!(governor.snapshot().admitted_memory_bytes > 0);
    let base = fixture.0.join("checkpoint.1.hawdb");
    assert!(base.exists());
    owner.stop();
    assert!(
        !base.exists(),
        "an unselected private base must be reclaimed"
    );
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    control.ensure_healthy().unwrap();
    drop(store);
    // Control and Owner are deliberately still alive: they must not retain
    // either the writer lock or a lease on the abandoned private candidate.
    let recovered = GraphStore::open_read_only_with_durability(
        &fixture.0,
        &mut catalog,
        DurabilityPolicy::SyncOnEveryWrite,
        hawdb_storage::config::RecoveryMode::Strict,
    )
    .unwrap();
    assert_eq!(values(&recovered), vec![(1, body.clone()), (2, body)]);
}
