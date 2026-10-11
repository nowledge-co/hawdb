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

#[test]
fn occupied_physical_wave_parks_cleanup_and_release_finishes_without_a_host_call() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".into(), Value::Int(17)),
                ("body".into(), Value::String("io".repeat(256))),
            ]),
        )
        .unwrap();
    let old = store.snapshot_for_read();
    let control = Arc::new(Control::default());
    let governor = retirement_governor();
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
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if control.lock().unwrap().phase == Phase::Handoff {
            break;
        }
        assert!(Instant::now() < deadline, "owner did not select a prefix");
        std::thread::sleep(Duration::from_millis(5));
    }
    let memory = governor.snapshot().admitted_memory_bytes;
    let permit = governor
        .try_admit(
            RuntimeWorkRequest::io(hawdb_qos::RuntimeWorkPriority::Background, 0, 0)
                .with_io_wave_slots(1),
        )
        .unwrap();
    let wave = permit
        .bind_task_context(RuntimeTaskContext::default())
        .acquire_io_wave(NonZeroUsize::MIN)
        .unwrap()
        .unwrap();
    // Leave only a real in-flight I/O charge, not a competing CPU/task slot.
    drop(permit);
    {
        let mut state = control.lock_frontend().unwrap();
        control.adopt(&mut state, &mut store).unwrap();
    }
    loop {
        let state = control.lock().unwrap();
        if state.retired.is_some() && state.report.deferred_attempts > 0 {
            break;
        }
        assert!(Instant::now() < deadline, "cleanup did not yield for I/O");
        drop(state);
        std::thread::sleep(Duration::from_millis(5));
    }
    let parked = governor.snapshot();
    assert_eq!(parked.active_cpu_slots, 0);
    assert_eq!(parked.active_background_tasks, 0);
    assert_eq!(parked.active_background_io_slots, 1);
    assert_eq!(parked.admitted_memory_bytes, memory);
    assert!(
        store
            .storage_pressure_snapshot(None)
            .generation_reclamation_retry_required
    );
    drop(wave);
    loop {
        let state = control.lock().unwrap();
        if state.phase == Phase::Idle && state.retired.is_none() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "I/O recovery did not finish cleanup"
        );
        drop(state);
        std::thread::sleep(Duration::from_millis(5));
    }
    owner.stop();
    assert!(
        !store
            .storage_pressure_snapshot(None)
            .generation_reclamation_retry_required
    );
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    for view in [&store, &old] {
        assert_eq!(view.node_count_for_label(None), 1);
        let node = view.node_owned(hawdb_storage::NodeId(0)).unwrap().unwrap();
        assert_eq!(node.properties["id"], Value::Int(17));
        assert_eq!(node.properties["body"], Value::String("io".repeat(256)));
    }
    drop(old);
    drop(owner);
    drop(store);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let reopened = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    let node = reopened
        .node_owned(hawdb_storage::NodeId(0))
        .unwrap()
        .unwrap();
    assert_eq!(node.properties["id"], Value::Int(17));
    assert_eq!(node.properties["body"], Value::String("io".repeat(256)));
}

#[test]
fn invalid_retirement_runtime_fails_closed_and_preserves_published_data() {
    let fixture = Fixture::new();
    let other_fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("id".into(), Value::Int(23))]),
        )
        .unwrap();
    let old = store.snapshot_for_read();
    let control = Arc::new(Control::default());
    let governor = retirement_governor();
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
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if control.lock().unwrap().phase == Phase::Handoff {
            break;
        }
        assert!(Instant::now() < deadline, "owner did not select a prefix");
        std::thread::sleep(Duration::from_millis(5));
    }
    let published = fixture.0.join("checkpoint.1.hawdb");
    let bytes = std::fs::read(&published).unwrap();
    let mut resources = governor.snapshot().resources;
    resources.memory.pressure = hawdb_qos::RuntimeMemoryPressure::Critical;
    governor.update_resources(resources);
    let mut other_catalog = Catalog::default();
    let other = GraphStore::open(&other_fixture.0, &mut other_catalog).unwrap();
    let replaced = {
        let mut state = control.lock_frontend().unwrap();
        control.adopt(&mut state, &mut store).unwrap();
        let retired = state.retired.as_mut().unwrap();
        std::mem::replace(
            &mut retired.selected,
            Source::capture(&other, &other_catalog),
        )
    };
    drop(replaced);
    loop {
        if control.ensure_healthy().is_err() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "invalid retirement stayed healthy"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    owner.stop();
    assert_eq!(control.lock().unwrap().report.failed_attempts, 1);
    assert_eq!(std::fs::read(&published).unwrap(), bytes);
    assert!(
        !other
            .storage_pressure_snapshot(None)
            .generation_reclamation_retry_required
    );
    for view in [&store, &old] {
        assert_eq!(view.node_count_for_label(None), 1);
        assert_eq!(
            view.node_owned(hawdb_storage::NodeId(0))
                .unwrap()
                .unwrap()
                .properties["id"],
            Value::Int(23)
        );
    }
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(old);
    drop(owner);
    drop(store);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let reopened = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(reopened.node_count_for_label(None), 1);
    assert_eq!(
        reopened
            .node_owned(hawdb_storage::NodeId(0))
            .unwrap()
            .unwrap()
            .properties["id"],
        Value::Int(23)
    );
}
