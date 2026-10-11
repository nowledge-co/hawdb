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
use crate::background::{CheckpointWorkContext, CheckpointWorkProbe};
use hawdb_core::RuntimeMemoryError;
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeTaskContext, RuntimeWorkRequest, WorkClass, WorkRequest,
};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

const CEILING: u64 = 4 * 1024 * 1024;

fn assert_retained_then_refunded(store: GraphStore, task: &RuntimeTaskContext, idle: u64) {
    let retained = available(task);
    assert!(
        retained < idle,
        "successful replay retains primary allocation charges"
    );
    let pin = store.snapshot();
    drop(store);
    assert_eq!(
        available(task),
        retained,
        "the snapshot keeps the actual allocations alive"
    );
    drop(pin);
    assert_eq!(
        available(task),
        idle,
        "the last data pin releases all replay allocations"
    );
}

fn source() -> (PathBuf, GraphStore, Catalog) {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-delta-related-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &directory,
        &mut catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            max_out_of_core_delta_bytes: Some(2 * 1024 * 1024),
            ..WalReplayConfig::default()
        },
    )
    .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("id".into(), Value::String("before".into()))]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Label",
            BTreeMap::from([("name".into(), Value::String("label".into()))]),
        )
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            NodeId(0),
            NodeId(1),
            "HAS_LABEL",
            BTreeMap::from([("before".into(), Value::Bool(true))]),
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    assert!(store.is_out_of_core());
    (directory, store, catalog)
}

fn operation() -> WalOp {
    let values = Value::Map(BTreeMap::from([(
        "界\0".into(),
        Value::List(vec![
            Value::Null,
            Value::Bool(false),
            Value::Int(i64::MIN),
            Value::Float(1.25),
            Value::String("界🙂\0".into()),
            Value::Binary(vec![0, 1, 255]),
            Value::Uuid(hawdb_core::Uuid::from_u128(7)),
        ]),
    )]));
    WalOp::Batch(vec![
        WalOp::CreateNode {
            id: NodeId(2),
            label: "Memory".into(),
            properties: BTreeMap::from([("payload".into(), values.clone())]),
        },
        WalOp::SetNodeProperty {
            id: NodeId(0),
            property: "payload".into(),
            value: values.clone(),
        },
        WalOp::SetNodeProperty {
            id: NodeId(0),
            property: "id".into(),
            value: Value::String("after界".into()),
        },
        WalOp::SetRelationshipProperty {
            id: RelId(0),
            property: "payload".into(),
            value: values.clone(),
        },
        WalOp::SetRelationshipProperty {
            id: RelId(0),
            property: "before".into(),
            value: Value::Bool(false),
        },
        WalOp::CreateRelationship {
            id: RelId(1),
            source: NodeId(2),
            target: NodeId(1),
            rel_type: "HAS_LABEL".into(),
            properties: BTreeMap::from([("payload".into(), values)]),
        },
        WalOp::DeleteRelationship { id: RelId(0) },
        WalOp::DeleteNode { id: NodeId(0) },
        WalOp::CreateNodeLabel {
            label: "Additional".into(),
        },
    ])
}

fn governor() -> RuntimeGovernor {
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(CEILING),
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

fn available(task: &RuntimeTaskContext) -> u64 {
    match task.reserve_working_memory(CEILING) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => available_bytes,
        _ => panic!("the exact ceiling must probe the real working-memory remainder"),
    }
}

fn assert_complete(actual: &GraphStore, catalog: &Catalog, expected: &GraphStore, other: &Catalog) {
    assert_eq!(actual.commit_epoch(), expected.commit_epoch());
    assert_eq!(
        actual
            .node_records_owned()
            .collect::<Result<Vec<_>>>()
            .unwrap(),
        expected
            .node_records_owned()
            .collect::<Result<Vec<_>>>()
            .unwrap()
    );
    assert_eq!(
        actual
            .relationship_records_owned()
            .collect::<Result<Vec<_>>>()
            .unwrap(),
        expected
            .relationship_records_owned()
            .collect::<Result<Vec<_>>>()
            .unwrap()
    );
    for name in ["Memory", "Label", "Additional"] {
        assert_eq!(catalog.label_id(name), other.label_id(name));
    }
    assert_eq!(
        catalog.rel_type_id("HAS_LABEL"),
        other.rel_type_id("HAS_LABEL")
    );
    assert_eq!(
        actual.search_projection_graph_changes_after(0),
        expected.search_projection_graph_changes_after(0)
    );
}

#[test]
fn checkpoint_units_wal_replay_delta_estimation_cancels_every_unit_and_retries_complete_state() {
    let (directory, source, catalog) = source();
    let identity = source.checkpoint_source_identity();
    let source_nodes = source
        .node_records_owned()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let source_relationships = source
        .relationship_records_owned()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let mut expected = source.snapshot();
    let mut other = catalog.clone();
    expected
        .apply_replayed_wal_transaction(&mut other, operation())
        .unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let scheduler = scheduler();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let idle = available(&task);
    let probe = Arc::new(CheckpointWorkProbe::default());
    scheduler.set_telemetry_sink(Some(probe.clone()));
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
    let mut actual = source.snapshot();
    let mut current = catalog.clone();
    actual
        .apply_replayed_checkpoint_wal_transaction(&mut current, operation(), &work)
        .unwrap();
    assert_complete(&actual, &current, &expected, &other);
    let units = probe.completed.load(Ordering::SeqCst);
    assert!(units > 50);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    probe.assert_released(&scheduler);
    assert_retained_then_refunded(actual, &task, idle);
    drop(work);
    for stop in 1..=units {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(stop, Ordering::SeqCst);
        scheduler.set_telemetry_sink(Some(probe.clone()));
        let stopped = permit.bind_task_context(RuntimeTaskContext::without_deadline(
            probe.cancellation.clone(),
        ));
        let work = CheckpointWorkContext::new(stopped).with_scheduler(scheduler.clone());
        let mut actual = source.snapshot();
        let mut current = catalog.clone();
        actual
            .apply_replayed_checkpoint_wal_transaction(&mut current, operation(), &work)
            .unwrap_err();
        assert_eq!(actual.commit_epoch(), source.commit_epoch());
        assert_eq!(source.checkpoint_source_identity(), identity);
        assert_eq!(
            source
                .node_records_owned()
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            source_nodes
        );
        assert_eq!(
            source
                .relationship_records_owned()
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            source_relationships
        );
        assert!(source
            .search_projection_graph_changes_after(source.commit_epoch())
            .is_empty());
        assert_eq!(catalog.label_id("Additional"), None);
        probe.assert_released(&scheduler);
        drop(actual);
        assert_eq!(available(&task), idle);
        drop(work);
        scheduler.set_telemetry_sink(None);
        let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
        let mut retry = source.snapshot();
        let mut current = catalog.clone();
        retry
            .apply_replayed_checkpoint_wal_transaction(&mut current, operation(), &work)
            .unwrap();
        assert_complete(&retry, &current, &expected, &other);
        assert_retained_then_refunded(retry, &task, idle);
        assert_eq!(governor.snapshot().admissions, 1);
    }
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(expected);
    drop(source);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_wal_replay_delta_estimation_denies_tree_memory_before_mutation_and_retries() {
    let (directory, source, catalog) = source();
    let mut expected = source.snapshot();
    let mut other = catalog.clone();
    expected
        .apply_replayed_wal_transaction(&mut other, operation())
        .unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let idle = available(&task);
    let held = task.reserve_working_memory(idle - 1024).unwrap().unwrap();
    let work = CheckpointWorkContext::new(task.clone());
    let before = available(&task);
    let mut actual = source.snapshot();
    let mut current = catalog.clone();
    let error = actual
        .apply_replayed_checkpoint_wal_transaction(&mut current, operation(), &work)
        .unwrap_err();
    assert!(
        error.to_string().contains("remaining reservation"),
        "{error}"
    );
    assert_complete(&actual, &current, &source, &catalog);
    assert_eq!(available(&task), before);
    drop(held);
    actual
        .apply_replayed_checkpoint_wal_transaction(&mut current, operation(), &work)
        .unwrap();
    assert_complete(&actual, &current, &expected, &other);
    assert_retained_then_refunded(actual, &task, idle);
    assert_eq!(governor.snapshot().admissions, 1);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(expected);
    drop(source);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_wal_replay_delta_estimation_denies_local_work_before_mutation_and_retries() {
    let (directory, source, catalog) = source();
    let mut expected = source.snapshot();
    let mut other = catalog.clone();
    expected
        .apply_replayed_wal_transaction(&mut other, operation())
        .unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let scheduler = scheduler();
    let held = scheduler
        .try_start(WorkRequest::background(WorkClass::Mutation, 1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let idle = available(&task);
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
    let mut actual = source.snapshot();
    let mut current = catalog.clone();
    actual
        .apply_replayed_checkpoint_wal_transaction(&mut current, operation(), &work)
        .unwrap_err();
    assert_complete(&actual, &current, &source, &catalog);
    assert_eq!(available(&task), idle);
    drop(held);
    actual
        .apply_replayed_checkpoint_wal_transaction(&mut current, operation(), &work)
        .unwrap();
    assert_complete(&actual, &current, &expected, &other);
    assert_retained_then_refunded(actual, &task, idle);
    assert_eq!(governor.snapshot().admissions, 1);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(expected);
    drop(source);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_wal_replay_delta_estimation_preserves_exact_ordinary_limit_diagnostics() {
    let (directory, source, catalog) = source();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let idle = available(&task);
    let work = CheckpointWorkContext::new(task.clone());
    for limit in [0, 1, 64, 1000, 10_000, u64::MAX] {
        let mut ordinary = source.snapshot();
        ordinary.max_out_of_core_delta_bytes = Some(limit);
        let mut other = catalog.clone();
        let expected = ordinary.apply_replayed_wal_transaction(&mut other, operation());
        let mut actual = source.snapshot();
        actual.max_out_of_core_delta_bytes = Some(limit);
        let mut current = catalog.clone();
        let result =
            actual.apply_replayed_checkpoint_wal_transaction(&mut current, operation(), &work);
        let succeeded = result.is_ok();
        assert_eq!(
            result.map_err(|e| e.to_string()),
            expected.map_err(|e| e.to_string())
        );
        assert_complete(&actual, &current, &ordinary, &other);
        if succeeded {
            assert_retained_then_refunded(actual, &task, idle);
        } else {
            assert_eq!(available(&task), idle);
            drop(actual);
        }
    }
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(source);
    std::fs::remove_dir_all(directory).unwrap();
}
