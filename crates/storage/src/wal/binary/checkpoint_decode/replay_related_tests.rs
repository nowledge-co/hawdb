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
use crate::wal::checkpoint::CheckpointWalEntry;
use hawdb_core::Catalog;
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeTaskContext, RuntimeWorkRequest, WorkClass, WorkRequest,
};
use std::sync::atomic::Ordering;

fn nested_entry() -> WalEntry {
    WalEntry {
        lsn: 17,
        op: WalOp::Batch(vec![
            WalOp::CreateNodeLabel { label: "M".into() },
            WalOp::Batch(vec![
                WalOp::CreateNode {
                    id: NodeId(0),
                    label: "M".into(),
                    properties: BTreeMap::from([("id".into(), Value::Int(0))]),
                },
                WalOp::CreateNode {
                    id: NodeId(1),
                    label: "M".into(),
                    properties: BTreeMap::from([("id".into(), Value::Int(1))]),
                },
                WalOp::Batch(Vec::new()),
            ]),
            WalOp::CreateRelationshipType {
                rel_type: "R".into(),
            },
            WalOp::CreateRelationship {
                id: RelId(0),
                source: NodeId(0),
                target: NodeId(1),
                rel_type: "R".into(),
                properties: BTreeMap::from([("weight".into(), Value::Int(7))]),
            },
            WalOp::SetNodeProperty {
                id: NodeId(0),
                property: "body".into(),
                value: Value::String("界🦀".repeat(1025)),
            },
            WalOp::SetRelationshipProperty {
                id: RelId(0),
                property: "weight".into(),
                value: Value::Int(9),
            },
        ]),
    }
}

fn bytes() -> Vec<u8> {
    let WalOp::Batch(operations) = nested_entry().op else {
        unreachable!();
    };
    let mut flattened = Vec::new();
    for operation in operations {
        match operation {
            WalOp::Batch(operations) => {
                for operation in operations {
                    match operation {
                        WalOp::Batch(empty) => assert!(empty.is_empty()),
                        operation => flattened.push(operation),
                    }
                }
            }
            operation => flattened.push(operation),
        }
    }
    encode_binary_wal_record(
        &WalEntry {
            lsn: 17,
            op: WalOp::Batch(flattened),
        },
        19,
    )
    .unwrap()
}

fn decode(bytes: &[u8], work: &CheckpointWorkContext) -> CheckpointWalEntry {
    match decode_binary_wal_record_with_work_context(bytes, work).unwrap() {
        BinaryWalRecordDecode::Entry { entry, .. } => entry,
        BinaryWalRecordDecode::Corrupt(reason) => panic!("valid replay record: {reason}"),
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

fn assert_complete(store: &GraphStore, catalog: &Catalog, bytes: &[u8]) {
    let entry = match super::super::decode_binary_wal_record(bytes).unwrap() {
        BinaryWalRecordDecode::Entry { entry, .. } => entry,
        BinaryWalRecordDecode::Corrupt(reason) => panic!("ordinary reference: {reason}"),
    };
    let mut expected = GraphStore::in_memory();
    let mut expected_catalog = Catalog::default();
    expected
        .apply_replayed_wal_transaction(&mut expected_catalog, entry.op)
        .unwrap();
    assert_eq!(store.commit_epoch(), 1);
    assert_eq!(store.node_count_for_label(None), 2);
    assert_eq!(catalog.label_id("M"), expected_catalog.label_id("M"));
    assert_eq!(catalog.rel_type_id("R"), expected_catalog.rel_type_id("R"));
    for id in 0..2 {
        assert_eq!(
            store.node_owned(NodeId(id)).unwrap(),
            expected.node_owned(NodeId(id)).unwrap()
        );
    }
    assert_eq!(
        store.relationship_owned(RelId(0)).unwrap(),
        expected.relationship_owned(RelId(0)).unwrap()
    );
    assert_eq!(
        store.node_owned(NodeId(0)).unwrap().unwrap().properties["body"],
        Value::String("界🦀".repeat(1025))
    );
    assert_eq!(
        store
            .relationship_owned(RelId(0))
            .unwrap()
            .unwrap()
            .properties["weight"],
        Value::Int(9)
    );
}

#[test]
fn checkpoint_units_wal_replay_valid_batch_preserves_complete_schema_graph_and_one_epoch() {
    let bytes = bytes();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4 * 1024 * 1024))
        .unwrap();
    let scheduler = scheduler();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
        .with_scheduler(scheduler);
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    decode(&bytes, &work)
        .replay_into(&mut store, &mut catalog, &work)
        .unwrap();
    assert_complete(&store, &catalog, &bytes);
    drop(store);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_wal_replay_nested_wire_batch_retains_original_rejection() {
    let error = encode_binary_wal_record(&nested_entry(), 19).unwrap_err();
    assert!(error
        .to_string()
        .contains("nested WAL batches cannot be encoded"));
}

#[test]
fn checkpoint_units_wal_replay_cancel_every_unit_keeps_epoch_unpublished_and_fully_retries() {
    let bytes = bytes();
    let source_bytes = bytes.clone();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4 * 1024 * 1024))
        .unwrap();
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let task = permit.bind_task_context(RuntimeTaskContext::without_deadline(
        probe.cancellation.clone(),
    ));
    let decode_work = CheckpointWorkContext::new(task.clone());
    let decoded = decode(&bytes, &decode_work);
    scheduler.set_telemetry_sink(Some(probe.clone()));
    let work = CheckpointWorkContext::new(task).with_scheduler(scheduler.clone());
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    decoded
        .replay_into(&mut store, &mut catalog, &work)
        .unwrap();
    let units = probe.completed.load(Ordering::SeqCst);
    assert!(units >= 7);
    assert_complete(&store, &catalog, &bytes);
    probe.assert_released(&scheduler);
    drop(store);
    drop(work);
    drop(decode_work);
    for stop in 1..=units {
        scheduler.set_telemetry_sink(None);
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(stop, Ordering::SeqCst);
        let task = permit.bind_task_context(RuntimeTaskContext::without_deadline(
            probe.cancellation.clone(),
        ));
        let decode_work = CheckpointWorkContext::new(task.clone());
        let decoded = decode(&bytes, &decode_work);
        scheduler.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(task).with_scheduler(scheduler.clone());
        let mut store = GraphStore::in_memory();
        let mut catalog = Catalog::default();
        assert!(decoded
            .replay_into(&mut store, &mut catalog, &work)
            .is_err());
        assert_eq!(store.commit_epoch(), 0);
        assert_eq!(bytes, source_bytes);
        probe.assert_released(&scheduler);
        drop(store);
        drop(work);
        drop(decode_work);
        scheduler.set_telemetry_sink(None);
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
                .with_scheduler(scheduler.clone());
        let mut retry = GraphStore::in_memory();
        let mut retry_catalog = Catalog::default();
        decode(&bytes, &work)
            .replay_into(&mut retry, &mut retry_catalog, &work)
            .unwrap();
        assert_complete(&retry, &retry_catalog, &bytes);
        drop(retry);
        drop(work);
        assert_eq!(governor.snapshot().admissions, 1);
    }
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_wal_replay_denial_precedes_mutation_and_retries_same_reservation() {
    let bytes = bytes();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4 * 1024 * 1024))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let decode_work = CheckpointWorkContext::new(task.clone());
    let decoded = decode(&bytes, &decode_work);
    let scheduler = scheduler();
    let work = CheckpointWorkContext::new(task).with_scheduler(scheduler.clone());
    let held = scheduler
        .try_start(WorkRequest::background(WorkClass::Mutation, 1))
        .unwrap();
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    assert!(decoded
        .replay_into(&mut store, &mut catalog, &work)
        .is_err());
    assert_eq!(store.commit_epoch(), 0);
    assert_eq!(store.node_count_for_label(None), 0);
    assert!(catalog.is_empty());
    held.finish_with_outcome(true);
    decode(&bytes, &work)
        .replay_into(&mut store, &mut catalog, &work)
        .unwrap();
    assert_complete(&store, &catalog, &bytes);
    assert_eq!(governor.snapshot().admissions, 1);
    drop(store);
    drop(work);
    drop(decode_work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
