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

#[test]
fn denied_candidate_releases_execution_and_reuses_its_memory_after_recovery() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    let create = |store: &mut GraphStore, catalog: &mut Catalog, id| {
        store
            .create_node(
                catalog,
                "Memory",
                BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("body".into(), Value::String("park".repeat(128))),
                ]),
            )
            .unwrap();
    };
    create(&mut store, &mut catalog, 1);
    let old = store.snapshot_for_read();
    let control = Arc::new(Control::default());
    let resources = governor(200 * 1024 * 1024);
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
        Some(resources.clone()),
    )
    .unwrap()
    .unwrap();
    observation.recv_timeout(Duration::from_secs(15)).unwrap();
    let base = fixture.0.join("checkpoint.1.hawdb");
    let bytes = std::fs::read(&base).unwrap();
    let admitted_memory = resources.snapshot().admitted_memory_bytes;
    create(&mut store, &mut catalog, 2);
    {
        let mut state = control.lock().unwrap();
        let retired = control.submit(&mut state, &store, &catalog);
        state.prefix_seal_probe = None;
        drop(state);
        drop(retired);
    }
    let mut snapshot = resources.snapshot().resources;
    snapshot.memory.pressure = hawdb_qos::RuntimeMemoryPressure::Critical;
    resources.update_resources(snapshot);
    resume.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let state = control.lock().unwrap();
        if state.pending.is_some() && state.phase == Phase::Idle {
            break;
        }
        drop(state);
        assert!(Instant::now() < deadline, "candidate did not park");
        std::thread::sleep(Duration::from_millis(5));
    }
    let parked = resources.snapshot();
    assert_eq!(
        parked.active_cpu_slots, 0,
        "a denied private candidate must not occupy CPU"
    );
    assert_eq!(parked.active_background_tasks, 0);
    assert_eq!(parked.active_blocking_tasks, 0);
    assert_eq!(parked.active_background_io_slots, 0);
    assert_eq!(parked.admitted_memory_bytes, admitted_memory);
    assert_eq!(std::fs::read(&base).unwrap(), bytes);
    // Normal-pressure recovery must reuse the retained memory under a budget
    // that cannot admit a second conservative whole-candidate reservation.
    assert!(admitted_memory * 2 > resources.snapshot().limits.memory_budget_bytes);
    snapshot.memory.pressure = hawdb_qos::RuntimeMemoryPressure::Normal;
    resources.update_resources(snapshot);
    loop {
        let mut state = control.lock().unwrap();
        if state.phase == Phase::Handoff {
            control.adopt(&mut state, &mut store).unwrap();
            break;
        }
        drop(state);
        assert!(Instant::now() < deadline, "candidate did not resume");
        std::thread::sleep(Duration::from_millis(5));
    }
    owner.stop();
    assert_eq!(std::fs::read(&base).unwrap(), bytes);
    assert_eq!(old.node_count_for_label(None), 1);
    assert_eq!(store.node_count_for_label(None), 2);
    for id in 0..2 {
        let node = store
            .node_owned(hawdb_storage::NodeId(id))
            .unwrap()
            .unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64 + 1));
        assert_eq!(node.properties["body"], Value::String("park".repeat(128)));
    }
    drop(old);
    drop(owner);
    drop(store);
    assert_eq!(resources.snapshot().admitted_memory_bytes, 0);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 2);
    for id in 0..2 {
        let node = recovered
            .node_owned(hawdb_storage::NodeId(id))
            .unwrap()
            .unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64 + 1));
        assert_eq!(node.properties["body"], Value::String("park".repeat(128)));
    }
}
