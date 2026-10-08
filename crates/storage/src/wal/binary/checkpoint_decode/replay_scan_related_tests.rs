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
use crate::background::CheckpointWorkProbe;
use crate::store::GraphStore;
use hawdb_core::Catalog;
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeTaskContext, RuntimeWorkRequest, WorkClass, WorkRequest,
};
use std::sync::atomic::Ordering;

fn nodes(count: u64) -> Vec<WalOp> {
    (0..count)
        .map(|id| WalOp::CreateNode {
            id: NodeId(id),
            label: "M".into(),
            properties: BTreeMap::from([("id".into(), Value::Int(id as i64))]),
        })
        .collect()
}

fn corrupt() -> WalOp {
    let mut operations = nodes(1057);
    operations.push(WalOp::Relational {
        record: Vec::new().into(),
    });
    WalOp::Batch(operations)
}

fn governor() -> RuntimeGovernor {
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    governor
}

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    })
}

fn assert_retry_matches_ordinary(work: &CheckpointWorkContext) {
    let operations = WalOp::Batch(nodes(65));
    let mut ordinary = GraphStore::in_memory();
    let mut ordinary_catalog = Catalog::default();
    ordinary
        .apply_replayed_wal_transaction(&mut ordinary_catalog, operations.clone())
        .unwrap();
    let mut retry = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    retry
        .apply_replayed_checkpoint_wal_transaction(&mut catalog, operations, work)
        .unwrap();
    assert_eq!(retry.commit_epoch(), 1);
    assert_eq!(retry.node_count_for_label(None), 65);
    assert_eq!(catalog.label_id("M"), ordinary_catalog.label_id("M"));
    for id in 0..65 {
        assert_eq!(
            retry.node_owned(NodeId(id)).unwrap(),
            ordinary.node_owned(NodeId(id)).unwrap()
        );
    }
    assert_eq!(
        retry.search_projection_graph_changes_after(0),
        ordinary.search_projection_graph_changes_after(0)
    );
}

#[test]
fn checkpoint_units_wal_replay_prescan_cancels_every_visit_before_mutation_and_fully_retries() {
    let source = corrupt();
    let source_bytes = encode_binary_wal_record(
        &WalEntry {
            lsn: 17,
            op: source.clone(),
        },
        19,
    )
    .unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4 * 1024 * 1024))
        .unwrap();
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    scheduler.set_telemetry_sink(Some(probe.clone()));
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
        .with_scheduler(scheduler.clone());
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let diagnostic = store
        .apply_replayed_checkpoint_wal_transaction(&mut catalog, source.clone(), &work)
        .unwrap_err();
    assert!(diagnostic
        .to_string()
        .contains("invalid relational durable record header"));
    let visits = probe.completed.load(Ordering::SeqCst);
    assert_eq!(visits, 1058);
    probe.assert_released(&scheduler);
    drop(store);
    drop(work);
    for stop in 1..=visits {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(stop, Ordering::SeqCst);
        scheduler.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(scheduler.clone());
        let mut store = GraphStore::in_memory();
        let mut catalog = Catalog::default();
        let error = store
            .apply_replayed_checkpoint_wal_transaction(&mut catalog, source.clone(), &work)
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            HawDBError::from_storage_error(work.checkpoint().unwrap_err()).to_string()
        );
        assert_eq!(store.commit_epoch(), 0);
        assert_eq!(store.node_count_for_label(None), 0);
        assert_eq!(catalog.label_id("M"), None);
        assert!(store.search_projection_graph_changes_after(0).is_empty());
        assert_eq!(probe.completed.load(Ordering::SeqCst), stop);
        probe.assert_released(&scheduler);
        drop(store);
        drop(work);
        scheduler.set_telemetry_sink(None);
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
                .with_scheduler(scheduler.clone());
        assert_retry_matches_ordinary(&work);
        assert_eq!(governor.snapshot().admissions, 1);
    }
    assert_eq!(
        encode_binary_wal_record(
            &WalEntry {
                lsn: 17,
                op: source
            },
            19
        )
        .unwrap(),
        source_bytes
    );
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_wal_replay_prescan_denial_leaves_no_graph_catalog_or_capture_changes() {
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4 * 1024 * 1024))
        .unwrap();
    let scheduler = scheduler();
    let held = scheduler
        .try_start(WorkRequest::background(WorkClass::Mutation, 1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
        .with_scheduler(scheduler.clone());
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    assert!(store
        .apply_replayed_checkpoint_wal_transaction(&mut catalog, corrupt(), &work)
        .is_err());
    assert_eq!(store.commit_epoch(), 0);
    assert_eq!(store.node_count_for_label(None), 0);
    assert_eq!(catalog.label_id("M"), None);
    assert!(store.search_projection_graph_changes_after(0).is_empty());
    drop(held);
    assert_retry_matches_ordinary(&work);
    assert_eq!(governor.snapshot().admissions, 1);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
