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

const MEMORY_BYTES: u64 = 16 * 1024 * 1024;

fn resources(bytes: u64) -> RuntimeGovernor {
    let resources = RuntimeGovernor::new(
        hawdb_qos::RuntimeGovernorConfig {
            cpu_slot_limit: Some(std::num::NonZeroUsize::MIN),
            background_task_limit: Some(std::num::NonZeroUsize::MIN),
            memory_budget_bytes: Some(bytes),
            ..hawdb_qos::RuntimeGovernorConfig::shared_host()
        },
        hawdb_qos::RuntimeResourceSnapshot::from_parts(
            hawdb_qos::RuntimeResourceBudget::from_limits(std::num::NonZeroUsize::MIN, None, None),
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
    resources.pin_resources();
    resources
}

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

#[test]
fn cancelled_queued_preparation_releases_its_source_before_worker_available() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    let source = Source::capture(&store, &catalog);
    let identity = source.store.checkpoint_source_identity();
    let executor = Arc::new(
        Scheduler::new(SchedulerConfig {
            business_workers: 1,
            background_workers: 1,
            queue_capacity: 1,
            ..SchedulerConfig::default()
        })
        .unwrap(),
    );
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocker = executor
        .spawn_background(TaskKey::new(1), Priority::High, move || {
            let _ = started_tx.send(());
            let _ = release_rx.recv_timeout(Duration::from_secs(10));
        })
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let task = RuntimeTaskContext::default();
    let running_task = task.clone();
    let running_executor = Arc::clone(&executor);
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let qos = LocalQosScheduler::new(hawdb_qos::LocalQosPolicy::default());
        let resources = resources(MEMORY_BYTES);
        let completed = scheduled_prepare(
            &running_executor,
            TaskKey::new(2),
            source,
            PreparationReport::default(),
            &qos,
            &resources,
            &running_task,
        );
        let _ = completed_tx.send(completed);
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while executor.background_queued() == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let was_queued = executor.background_queued() == 1;
    task.cancellation().cancel();
    let completed = completed_rx.recv_timeout(Duration::from_secs(2));
    let _ = release_tx.send(());
    worker.join().unwrap();
    blocker.join().unwrap();
    assert!(was_queued);
    let (source, _, result) =
        completed.expect("cancelled queued preparation must release its source");
    assert!(matches!(result, Err(HawDBError::RuntimeTaskStopped(_))));
    assert_eq!(source.store.checkpoint_source_identity(), identity);
}

#[test]
fn permanently_denied_retries_traverse_an_unchanged_source_only_once() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    let body = "p".repeat(512);
    assert!(store.begin_wal_sync_group().unwrap());
    for id in 0..1025 {
        add(&mut store, &mut catalog, id, &body);
    }
    store.finish_wal_sync_group().unwrap();
    let identity = store.checkpoint_source_identity().unwrap();
    let estimate = store.checkpoint_candidate_admission_bytes().unwrap();
    assert!(estimate > MEMORY_BYTES);
    let resources = resources(OWNER_MEMORY_BYTES);
    let scheduler = LocalQosScheduler::new(hawdb_qos::LocalQosPolicy::default());
    let mut source = Source::capture(&store, &catalog);
    let mut report = PreparationReport::default();
    for _ in 0..8 {
        assert!(prepare(
            &mut source,
            &scheduler,
            &resources,
            &RuntimeTaskContext::default(),
            &mut report,
        )
        .is_err());
        let denial = report.admission_denial.unwrap();
        assert_eq!(denial.code, RuntimeAdmissionCode::MemorySaturated);
        assert!(denial.requested > 0 && denial.requested < estimate);
        assert_eq!(denial.available, 0);
        assert!(report.memory_denial.is_some());
        assert!(!denial.is_retryable());
        assert_eq!(
            resources.snapshot().admitted_memory_bytes,
            OWNER_MEMORY_BYTES
        );
        assert_eq!(resources.snapshot().active_background_tasks, 0);
        assert_eq!(resources.snapshot().active_background_io_slots, 0);
        assert_eq!(store.checkpoint_source_identity(), Some(identity));
    }
    assert_eq!(report.planning_scans, 1);
    assert_eq!(report.planning_cache_hits, 7);
    drop(source);
    assert_eq!(resources.snapshot().admitted_memory_bytes, 0);
    drop(store);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 1025);
    for id in 0..1025 {
        let node = recovered
            .node_owned(hawdb_storage::NodeId(id))
            .unwrap()
            .unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64));
        assert_eq!(node.properties["body"], Value::String(body.clone()));
    }
}

#[test]
fn original_fixed_budget_prepares_and_publishes_all_1025_rows() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    let body = "p".repeat(512);
    assert!(store.begin_wal_sync_group().unwrap());
    for id in 0..1025 {
        add(&mut store, &mut catalog, id, &body);
    }
    store.finish_wal_sync_group().unwrap();
    assert!(store.checkpoint_candidate_admission_bytes().unwrap() > MEMORY_BYTES);
    let resources = resources(MEMORY_BYTES);
    let scheduler = LocalQosScheduler::new(hawdb_qos::LocalQosPolicy::default());
    let mut source = Source::capture(&store, &catalog);
    let mut report = PreparationReport::default();
    let (mut candidate, admission) = prepare(
        &mut source,
        &scheduler,
        &resources,
        &RuntimeTaskContext::default(),
        &mut report,
    )
    .unwrap()
    .unwrap();
    assert_eq!(report.planning_scans, 1);
    assert!(report.admission_denial.is_none());
    assert!(report.memory_denial.is_none());
    let memory = admission.runtime.memory_report();
    assert!(memory.incremental);
    assert_eq!(memory.upfront_reserved_bytes, OWNER_MEMORY_BYTES);
    assert!(memory.peak_accounted_bytes > 0);
    assert!(memory.peak_accounted_bytes + OWNER_MEMORY_BYTES <= MEMORY_BYTES);
    assert_eq!(candidate.commit_epoch(), 1025);
    candidate.catch_up(&store).unwrap();
    catalog = store
        .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
        .unwrap();
    drop(candidate);
    drop(admission);
    drop(source);
    drop(store);
    assert_eq!(resources.snapshot().admitted_memory_bytes, 0);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 1025);
    for id in 0..1025 {
        let node = recovered
            .node_owned(hawdb_storage::NodeId(id))
            .unwrap()
            .unwrap();
        assert_eq!(node.properties["id"], Value::Int(id as i64));
        assert_eq!(node.properties["body"], Value::String(body.clone()));
    }
}
