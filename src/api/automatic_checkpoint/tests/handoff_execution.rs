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

fn resources() -> RuntimeGovernor {
    let governor = RuntimeGovernor::new(
        hawdb_qos::RuntimeGovernorConfig {
            cpu_slot_limit: Some(NonZeroUsize::MIN),
            background_task_limit: Some(NonZeroUsize::MIN),
            memory_budget_bytes: Some(200 * 1024 * 1024),
            ..hawdb_qos::RuntimeGovernorConfig::shared_host()
        },
        hawdb_qos::RuntimeResourceSnapshot::from_parts(
            hawdb_qos::RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            hawdb_qos::RuntimeMemorySnapshot::from_limits(
                Some(1 << 30),
                Some(1 << 30),
                None,
                None,
                None,
            ),
        ),
        hawdb_qos::IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    governor
}

fn wait(control: &Control, predicate: impl Fn(&State) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let state = control.lock().unwrap();
        if predicate(&state) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "owner stalled at {:?}: {:?}",
            state.phase,
            state.report
        );
        drop(state);
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn assert_values(store: &GraphStore) {
    assert_eq!(store.node_count_for_label(None), 2);
    for id in 0..2 {
        let node = store
            .node_owned(hawdb_storage::NodeId(id))
            .unwrap()
            .unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64));
        assert_eq!(node.properties["body"], Value::String("h".repeat(512)));
    }
}

fn parked_handoff(shutdown: bool) {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    for id in 0..2 {
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("body".into(), Value::String("h".repeat(512))),
                ]),
            )
            .unwrap();
    }
    let old = store.snapshot_for_read();
    let control = Arc::new(Control::default());
    let resources = resources();
    let (waiting, observed) = std::sync::mpsc::channel();
    control.lock().unwrap().owner_wait_probe = Some(waiting);
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
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if observed
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap()
            == Phase::Handoff
        {
            break;
        }
    }
    let selected = resources.snapshot();
    assert_eq!(
        selected.active_cpu_slots, 0,
        "waiting for frontend adoption must not occupy CPU"
    );
    assert_eq!(selected.active_background_tasks, 0);
    assert_eq!(selected.active_background_io_slots, 0);
    assert!(selected.admitted_memory_bytes > 0);
    let base = fixture.0.join("checkpoint.1.hawdb");
    let bytes = std::fs::read(&base).unwrap();
    let other = resources
        .try_admit(RuntimeWorkRequest::background_maintenance(0))
        .unwrap();
    {
        let mut state = control.lock_frontend().unwrap();
        control.adopt(&mut state, &mut store).unwrap();
    }
    wait(&control, |state| {
        state.phase == Phase::Retiring
            && state.retired.is_some()
            && state.report.deferred_attempts > 0
    });
    assert_eq!(resources.snapshot().active_cpu_slots, 1);
    assert_eq!(resources.snapshot().active_background_tasks, 1);
    assert_eq!(resources.snapshot().active_background_io_slots, 0);
    assert_eq!(
        resources.snapshot().admitted_memory_bytes,
        selected.admitted_memory_bytes
    );
    assert_eq!(std::fs::read(&base).unwrap(), bytes);
    assert_values(&store);
    assert_values(&old);
    if shutdown {
        let (stopped, done) = std::sync::mpsc::channel();
        let closer = std::thread::spawn(move || {
            owner.stop();
            stopped.send(owner).unwrap();
        });
        let result = done.recv_timeout(Duration::from_secs(15));
        drop(other);
        closer.join().unwrap();
        owner = result.expect("shutdown waited for an unrelated execution admission");
    } else {
        drop(other);
        wait(&control, |state| {
            state.phase == Phase::Idle && state.retired.is_none()
        });
        owner.stop();
    }
    assert_eq!(resources.snapshot().active_cpu_slots, 0);
    assert_eq!(resources.snapshot().active_background_tasks, 0);
    assert_eq!(resources.snapshot().active_background_io_slots, 0);
    control.ensure_healthy().unwrap();
    assert_values(&store);
    assert_values(&old);
    drop(old);
    drop(owner);
    drop(store);
    assert_eq!(resources.snapshot().admitted_memory_bytes, 0);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_values(&recovered);
}

#[test]
fn handoff_yields_execution_and_retirement_reacquires_after_other_work_finishes() {
    parked_handoff(false);
}

#[test]
fn shutdown_of_adopted_retirement_does_not_wait_for_other_work_admission() {
    parked_handoff(true);
}
