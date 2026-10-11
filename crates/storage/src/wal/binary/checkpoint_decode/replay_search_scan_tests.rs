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
    RuntimeTaskContext, RuntimeWorkRequest,
};
use std::sync::atomic::Ordering;

fn source() -> (GraphStore, Catalog, NodeId) {
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let label = store
        .create_node(
            &mut catalog,
            "Label",
            BTreeMap::from([("name".into(), Value::String("before".into()))]),
        )
        .unwrap();
    assert_eq!(label, NodeId(0));
    for id in 0..1057 {
        let node = store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("id".into(), Value::String(format!("m-{id}")))]),
            )
            .unwrap();
        assert_eq!(node, NodeId(id + 1));
        store
            .create_relationship(&mut catalog, node, label, "HAS_LABEL", BTreeMap::new())
            .unwrap();
    }
    (store, catalog, label)
}

fn operation(label: NodeId) -> WalOp {
    WalOp::SetNodeProperty {
        id: label,
        property: "name".into(),
        value: Value::String("after".into()),
    }
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

#[test]
fn checkpoint_units_wal_replay_search_capture_admits_all_1057_label_neighbors() {
    let (mut store, mut catalog, label) = source();
    let base_epoch = store.commit_epoch();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4 * 1024 * 1024))
        .unwrap();
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    scheduler.set_telemetry_sink(Some(probe.clone()));
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
        .with_scheduler(scheduler.clone());
    store
        .apply_replayed_checkpoint_wal_transaction(&mut catalog, operation(label), &work)
        .unwrap();
    let units = probe.completed.load(Ordering::SeqCst);
    assert_eq!(store.commit_epoch(), base_epoch + 1);
    assert_eq!(store.node_count_for_label(None), 1058);
    assert_eq!(store.relationship_count_for_type(None), 1057);
    let changes = store.search_projection_graph_changes_after(base_epoch);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].commit_epoch, base_epoch + 1);
    assert_eq!(changes[0].upsert_node_ids, (0..1058).collect::<Vec<_>>());
    assert!(changes[0].delete_document_ids.is_empty());
    assert_eq!(
        store.node_owned(label).unwrap().unwrap().properties["name"],
        Value::String("after".into())
    );
    probe.assert_released(&scheduler);
    drop(store);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert!(
        units >= 1057,
        "a real search capture visited1057 label relationships with only {units} admitted units"
    );
}

#[test]
fn checkpoint_units_wal_replay_search_capture_cancels_before_mutation_or_changefeed_append() {
    let (mut store, mut catalog, label) = source();
    let base_epoch = store.commit_epoch();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4 * 1024 * 1024))
        .unwrap();
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(2, Ordering::SeqCst);
    scheduler.set_telemetry_sink(Some(probe.clone()));
    let work = CheckpointWorkContext::new(permit.bind_task_context(
        RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
    ))
    .with_scheduler(scheduler.clone());
    assert!(store
        .apply_replayed_checkpoint_wal_transaction(&mut catalog, operation(label), &work)
        .is_err());
    assert_eq!(store.commit_epoch(), base_epoch);
    assert_eq!(store.node_count_for_label(None), 1058);
    assert_eq!(store.relationship_count_for_type(None), 1057);
    probe.assert_released(&scheduler);
    assert_eq!(
        store.node_owned(label).unwrap().unwrap().properties["name"],
        Value::String("before".into()),
        "cancellation must interrupt capture before applying the label mutation"
    );
    assert!(store
        .search_projection_graph_changes_after(base_epoch)
        .is_empty());
    drop(store);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
