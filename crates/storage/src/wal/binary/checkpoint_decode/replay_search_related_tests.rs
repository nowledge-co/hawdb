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

fn source(memories: u64, unrelated: u64) -> (GraphStore, Catalog) {
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    store
        .create_node(
            &mut catalog,
            "Label",
            BTreeMap::from([("name".into(), Value::String("before".into()))]),
        )
        .unwrap();
    for id in 0..memories {
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("id".into(), Value::String(format!("m-{id}")))]),
            )
            .unwrap();
    }
    for _ in 0..unrelated {
        store
            .create_relationship(&mut catalog, NodeId(1), NodeId(2), "OTHER", BTreeMap::new())
            .unwrap();
    }
    for id in 1..=memories {
        let (source, target) = if id % 2 == 0 {
            (NodeId(0), NodeId(id))
        } else {
            (NodeId(id), NodeId(0))
        };
        store
            .create_relationship(&mut catalog, source, target, "HAS_LABEL", BTreeMap::new())
            .unwrap();
    }
    (store, catalog)
}

fn operation(unrelated: u64) -> WalOp {
    WalOp::Batch(vec![
        WalOp::SetNodeProperty {
            id: NodeId(0),
            property: "name".into(),
            value: Value::String("after".into()),
        },
        WalOp::SetNodeProperty {
            id: NodeId(1),
            property: "id".into(),
            value: Value::String("界🦀after".into()),
        },
        WalOp::DeleteRelationship {
            id: RelId(unrelated),
        },
    ])
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

fn assert_complete(
    actual: &GraphStore,
    catalog: &Catalog,
    expected: &GraphStore,
    expected_catalog: &Catalog,
    memories: u64,
    relationships: u64,
    base_epoch: u64,
) {
    assert_eq!(actual.commit_epoch(), base_epoch + 1);
    assert_eq!(actual.node_count_for_label(None), memories as usize + 1);
    assert_eq!(
        actual.relationship_count_for_type(None),
        relationships as usize - 1
    );
    assert_eq!(
        catalog.label_id("Memory"),
        expected_catalog.label_id("Memory")
    );
    for id in 0..=memories {
        assert_eq!(
            actual.node_owned(NodeId(id)).unwrap(),
            expected.node_owned(NodeId(id)).unwrap()
        );
    }
    for id in 0..relationships {
        assert_eq!(
            actual.relationship_owned(RelId(id)).unwrap(),
            expected.relationship_owned(RelId(id)).unwrap()
        );
    }
    assert_eq!(
        actual.search_projection_graph_changes_after(base_epoch),
        expected.search_projection_graph_changes_after(base_epoch)
    );
    let changes = actual.search_projection_graph_changes_after(base_epoch);
    assert_eq!(changes.len(), 1);
    assert_eq!(
        changes[0].upsert_node_ids,
        (0..=memories).collect::<Vec<_>>()
    );
    assert_eq!(changes[0].delete_document_ids, vec!["memory:m-0"]);
}

#[test]
fn checkpoint_units_wal_replay_search_capture_admits_nonmatching_relationship_prefix() {
    let (mut store, mut catalog) = source(2, 1057);
    let base_epoch = store.commit_epoch();
    let mut expected = store.snapshot();
    let mut expected_catalog = catalog.clone();
    expected
        .apply_replayed_wal_transaction(&mut expected_catalog, operation(1057))
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
    store
        .apply_replayed_checkpoint_wal_transaction(&mut catalog, operation(1057), &work)
        .unwrap();
    assert_complete(
        &store,
        &catalog,
        &expected,
        &expected_catalog,
        2,
        1059,
        base_epoch,
    );
    assert!(
        probe.completed.load(Ordering::SeqCst) >= 1057,
        "nonmatching relationships must not be hidden inside Iterator::next"
    );
    probe.assert_released(&scheduler);
    drop(store);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_wal_replay_search_capture_cancels_every_unit_and_retries_both_directions() {
    let (source, catalog) = source(32, 4);
    let base_epoch = source.commit_epoch();
    let original = encode_binary_wal_record(
        &WalEntry {
            lsn: 17,
            op: operation(4),
        },
        19,
    )
    .unwrap();
    let mut expected = source.snapshot();
    let mut expected_catalog = catalog.clone();
    expected
        .apply_replayed_wal_transaction(&mut expected_catalog, operation(4))
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
    let mut store = source.snapshot();
    let mut replay_catalog = catalog.clone();
    store
        .apply_replayed_checkpoint_wal_transaction(&mut replay_catalog, operation(4), &work)
        .unwrap();
    assert_complete(
        &store,
        &replay_catalog,
        &expected,
        &expected_catalog,
        32,
        36,
        base_epoch,
    );
    let units = probe.completed.load(Ordering::SeqCst);
    assert!(units >= 36);
    probe.assert_released(&scheduler);
    drop(store);
    drop(work);
    for stop in 1..=units {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(stop, Ordering::SeqCst);
        scheduler.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(scheduler.clone());
        let mut store = source.snapshot();
        let mut replay_catalog = catalog.clone();
        assert!(store
            .apply_replayed_checkpoint_wal_transaction(&mut replay_catalog, operation(4), &work)
            .is_err());
        assert_eq!(store.commit_epoch(), base_epoch);
        probe.assert_released(&scheduler);
        drop(store);
        drop(work);
        scheduler.set_telemetry_sink(None);
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
                .with_scheduler(scheduler.clone());
        let mut retry = source.snapshot();
        let mut retry_catalog = catalog.clone();
        retry
            .apply_replayed_checkpoint_wal_transaction(&mut retry_catalog, operation(4), &work)
            .unwrap();
        assert_complete(
            &retry,
            &retry_catalog,
            &expected,
            &expected_catalog,
            32,
            36,
            base_epoch,
        );
        assert_eq!(governor.snapshot().admissions, 1);
        assert_eq!(source.commit_epoch(), base_epoch);
        assert_eq!(
            source.node_owned(NodeId(0)).unwrap().unwrap().properties["name"],
            Value::String("before".into())
        );
        assert!(source
            .search_projection_graph_changes_after(base_epoch)
            .is_empty());
    }
    assert_eq!(
        encode_binary_wal_record(
            &WalEntry {
                lsn: 17,
                op: operation(4)
            },
            19
        )
        .unwrap(),
        original
    );
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
