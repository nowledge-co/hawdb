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
use hawdb_qos::RuntimeAdmissionCode;
use std::collections::BTreeMap;

fn add(store: &mut GraphStore, catalog: &mut Catalog, id: i64, body: &str) {
    store
        .create_node(
            catalog,
            "Memory",
            BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("body".into(), Value::String(body.into())),
            ]),
        )
        .unwrap();
}

fn occupy_memory(resources: &RuntimeGovernor) -> hawdb_qos::RuntimeRetainedMemory {
    let admission = resources
        .try_admit(RuntimeWorkRequest::foreground_query(1, 0))
        .unwrap();
    let held = admission
        .reserve_retained_memory(100 * 1024 * 1024)
        .unwrap();
    drop(admission);
    assert_eq!(resources.snapshot().active_cpu_slots, 0);
    held
}

#[test]
fn cached_planning_checks_fresh_cancellation_and_source_identity_before_recovery() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    add(&mut store, &mut catalog, 0, "first");
    let mut source = Source::capture(&store, &catalog);
    let resources = retirement_governor();
    let held = occupy_memory(&resources);
    let scheduler = LocalQosScheduler::new(hawdb_qos::LocalQosPolicy::default());
    let mut report = PreparationReport::default();
    assert!(prepare(
        &mut source,
        &scheduler,
        &resources,
        &RuntimeTaskContext::default(),
        &mut report,
    )
    .is_err());
    assert_eq!(report.planning_scans, 1);
    assert!(report.admission_denial.unwrap().is_retryable());
    let admissions = resources.snapshot().admissions;
    let cancelled = RuntimeTaskContext::default();
    cancelled.cancellation().cancel();
    assert!(prepare(&mut source, &scheduler, &resources, &cancelled, &mut report,).is_err());
    assert_eq!(resources.snapshot().admissions, admissions);
    assert_eq!(report.planning_scans, 1);
    assert_eq!(report.planning_cache_hits, 0);
    assert!(report.admission_denial.is_none());
    let old_cache = source.planned_memory;
    let body = "new".repeat(4096);
    add(&mut store, &mut catalog, 1, &body);
    let expected = store.checkpoint_candidate_admission_bytes().unwrap();
    assert!(expected > old_cache.unwrap().1);
    let mut changed = Source::capture(&store, &catalog);
    // Even retaining an old scalar cannot reuse it for another full identity.
    changed.planned_memory = old_cache;
    assert!(prepare(
        &mut changed,
        &scheduler,
        &resources,
        &RuntimeTaskContext::default(),
        &mut report,
    )
    .is_err());
    assert_eq!(report.planning_scans, 2);
    assert_eq!(report.planning_cache_hits, 0);
    assert_eq!(report.admission_denial.unwrap().requested, expected);
    assert!(report.admission_denial.unwrap().is_retryable());
    drop(held);
    let (candidate, admission) = prepare(
        &mut changed,
        &scheduler,
        &resources,
        &RuntimeTaskContext::default(),
        &mut report,
    )
    .unwrap()
    .unwrap();
    assert_eq!(report.planning_scans, 2);
    assert_eq!(report.planning_cache_hits, 1);
    assert!(report.admission_denial.is_none());
    drop(candidate);
    drop(admission);
    assert_eq!(resources.snapshot().admitted_memory_bytes, 0);
    drop(source);
    drop(changed);
    drop(store);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 2);
    for (id, body) in [(0, "first"), (1, body.as_str())] {
        let node = recovered
            .node_owned(hawdb_storage::NodeId(id))
            .unwrap()
            .unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64));
        assert_eq!(node.properties["body"], Value::String(body.into()));
    }
}

#[test]
fn actual_owner_reports_cached_memory_denial_and_recovers_without_frontend_work() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    let body = "r".repeat(512);
    assert!(store.begin_wal_sync_group().unwrap());
    for id in 0..32 {
        add(&mut store, &mut catalog, id, &body);
    }
    store.finish_wal_sync_group().unwrap();
    let estimate = store.checkpoint_candidate_admission_bytes().unwrap();
    let resources = retirement_governor();
    let held = occupy_memory(&resources);
    let control = Arc::new(Control::default());
    let mut owner = Owner::start(
        Arc::clone(&control),
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
        let report = control.report().unwrap().unwrap();
        if report.deferred_attempts >= 3 {
            assert_eq!(report.planning_scans, 1);
            assert!(report.planning_cache_hits >= 2);
            let denial = report.preparation_admission_denial.unwrap();
            assert_eq!(denial.code, RuntimeAdmissionCode::MemorySaturated);
            assert_eq!(denial.requested, estimate);
            assert!(denial.is_retryable());
            break;
        }
        assert!(Instant::now() < deadline, "owner did not retry: {report:?}");
        std::thread::sleep(Duration::from_millis(5));
    }
    // Releasing the competing memory is the only trigger. No submit, write,
    // frontend read or manual checkpoint repairs the owner's cached source.
    drop(held);
    loop {
        let report = control.report().unwrap().unwrap();
        if report.waiting_for_handoff {
            assert_eq!(report.planning_scans, 1);
            assert!(report.planning_cache_hits >= 3);
            assert!(report.preparation_admission_denial.is_none());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "owner did not recover: {report:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    {
        let mut state = control.lock_frontend().unwrap();
        control.adopt(&mut state, &mut store).unwrap();
    }
    owner.stop();
    drop(owner);
    drop(control);
    drop(store);
    assert_eq!(resources.snapshot().admitted_memory_bytes, 0);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 32);
    for id in 0..32 {
        let node = recovered
            .node_owned(hawdb_storage::NodeId(id))
            .unwrap()
            .unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64));
        assert_eq!(node.properties["body"], Value::String(body.clone()));
    }
}
