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
use crate::canonical::CheckpointRecord;
use hawdb_core::RuntimeMemoryError;
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeTaskContext, RuntimeWorkRequest,
};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

const CEILING: u64 = 8 * 1024 * 1024;

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

fn available(task: &RuntimeTaskContext) -> u64 {
    match task.reserve_working_memory(CEILING) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => available_bytes,
        _ => panic!("exact-ceiling probe must reveal real working-memory remainder"),
    }
}

fn source() -> (PathBuf, GraphStore, Catalog) {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-native-decoded-owner-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let store = GraphStore::open_with_durability_and_replay_config(
        &directory,
        &mut catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            max_out_of_core_delta_bytes: Some(32 * 1024 * 1024),
            ..Default::default()
        },
    )
    .unwrap();
    (directory, store, catalog)
}

fn dictionary_source() -> (PathBuf, GraphStore, Catalog, NodeRecord) {
    dictionary_source_with_key_len(1024)
}

fn dictionary_source_with_key_len(length: usize) -> (PathBuf, GraphStore, Catalog, NodeRecord) {
    let (directory, mut store, mut catalog) = source();
    let properties = (0..1057)
        .map(|i| {
            (
                format!("k{i:04}-{}", "x".repeat(length)),
                Value::Int(i as i64),
            )
        })
        .collect();
    let id = store
        .create_node(&mut catalog, "Source", properties)
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    let expected = store.node_owned(id).unwrap().unwrap();
    assert_eq!(expected.properties.len(), 1057);
    (directory, store, catalog, expected)
}

#[test]
fn checkpoint_units_wal_replay_native_decode_key_capacity_matches_actual_remaining_budget_delta() {
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let idle = available(&task);
    let mut retained = Vec::new();
    for length in [0, 1024] {
        let (directory, store, _catalog, expected) = dictionary_source_with_key_len(length);
        let decoded = store
            .checkpoint_mounted_node(expected.id, &work)
            .unwrap()
            .unwrap();
        assert_eq!(&*decoded, &expected);
        retained.push(idle - available(&task));
        drop(decoded);
        assert_eq!(available(&task), idle);
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }
    // Both records have the same encoded ID payload, tree cardinality and
    // allocation-inventory count. Only the actual owned key capacities differ.
    // The task-wide admission ceiling cannot establish this byte-level claim.
    assert_eq!(retained[1] - retained[0], 1057 * 1024);
    assert_eq!(governor.snapshot().admissions, 1);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
}

#[test]
fn checkpoint_units_wal_replay_native_decode_record_retains_admission_after_execution_closes() {
    let (directory, store, _catalog, expected) = dictionary_source();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let decoded = store
        .checkpoint_mounted_node(expected.id, &work)
        .unwrap()
        .unwrap();
    assert_eq!(&*decoded, &expected);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert!(
        governor.snapshot().admitted_memory_bytes
            >= expected
                .properties
                .keys()
                .map(|key| key.len() as u64)
                .sum::<u64>()
    );
    assert_eq!(&*decoded, &expected);
    drop(decoded);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_wal_replay_native_decode_dictionary_denial_retries_same_reservation() {
    let (directory, store, _catalog, expected) = dictionary_source();
    let identity = store.checkpoint_source_identity();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let idle = available(&task);
    let held = task
        .reserve_working_memory(idle - 128 * 1024)
        .unwrap()
        .unwrap();
    let before = available(&task);
    let work = CheckpointWorkContext::new(task.clone());
    let error = store
        .checkpoint_mounted_node(expected.id, &work)
        .unwrap_err();
    assert!(
        error.to_string().contains("remaining reservation"),
        "{error}"
    );
    assert!(!matches!(error, HawDBError::StorageIntegrity(_)), "{error}");
    assert_eq!(available(&task), before);
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert!(!store.canonical_base.as_ref().unwrap().is_poisoned());
    drop(held);
    let decoded = store
        .checkpoint_mounted_node(expected.id, &work)
        .unwrap()
        .unwrap();
    assert_eq!(&*decoded, &expected);
    assert!(available(&task) < idle);
    drop(decoded);
    assert_eq!(available(&task), idle);
    assert_eq!(governor.snapshot().admissions, 1);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

enum Decoded {
    Node(CheckpointRecord<NodeRecord>),
    Relationship(CheckpointRecord<RelRecord>),
}

fn lookup(store: &GraphStore, relationship: bool, work: &CheckpointWorkContext) -> Result<Decoded> {
    if relationship {
        store
            .checkpoint_mounted_relationship(RelId(0), work)
            .map(|value| Decoded::Relationship(value.unwrap()))
    } else {
        store
            .checkpoint_mounted_node(NodeId(0), work)
            .map(|value| Decoded::Node(value.unwrap()))
    }
}

fn compare(store: &GraphStore, relationship: bool, decoded: &Decoded) {
    match decoded {
        Decoded::Node(value) => {
            assert!(!relationship);
            assert_eq!(&**value, &store.node_owned(NodeId(0)).unwrap().unwrap());
        }
        Decoded::Relationship(value) => {
            assert!(relationship);
            assert_eq!(
                &**value,
                &store.relationship_owned(RelId(0)).unwrap().unwrap()
            );
        }
    }
}

fn mixed_source() -> (PathBuf, GraphStore, Catalog) {
    let (directory, mut store, mut catalog) = source();
    let properties = BTreeMap::from([
        ("binary".into(), Value::Binary(vec![0x9a; 70_003])),
        (
            "text".into(),
            Value::String(format!("{}🌏done", "界".repeat(21_846))),
        ),
        (
            "list".into(),
            Value::List((0..1057).map(Value::Int).collect()),
        ),
        (
            "nested".into(),
            Value::Map(BTreeMap::from([
                ("empty".into(), Value::String(String::new())),
                ("null".into(), Value::Null),
                ("false".into(), Value::Bool(false)),
                ("true".into(), Value::Bool(true)),
                ("float".into(), Value::Float(-0.0)),
                (
                    "uuid".into(),
                    Value::Uuid(hawdb_core::Uuid::from_bytes([0xa5; 16])),
                ),
                ("map".into(), Value::Map(BTreeMap::new())),
                ("list".into(), Value::List(Vec::new())),
            ])),
        ),
    ]);
    let from = store
        .create_node(&mut catalog, "Source", properties.clone())
        .unwrap();
    let to = store
        .create_node(&mut catalog, "Target", BTreeMap::new())
        .unwrap();
    store
        .create_relationship(&mut catalog, from, to, "LINK", properties)
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    assert!(
        store
            .canonical_base
            .as_ref()
            .unwrap()
            .property_spill_manifest()
            .unwrap()
            .value_count
            >= 2
    );
    (directory, store, catalog)
}

#[test]
fn checkpoint_units_wal_replay_native_decode_nested_spill_cancels_every_unit_and_retries() {
    let (directory, store, _catalog) = mixed_source();
    let identity = store.checkpoint_source_identity();
    let physical = std::fs::read(store.canonical_base.as_ref().unwrap().path()).unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let idle = available(&task);
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    for relationship in [false, true] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        scheduler.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
        let decoded = lookup(&store, relationship, &work).unwrap();
        compare(&store, relationship, &decoded);
        drop(decoded);
        assert_eq!(available(&task), idle);
        let units = probe.completed.load(Ordering::SeqCst);
        assert!(units >= 1057);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&scheduler);
        drop(work);
        for stop in 1..=units {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(stop, Ordering::SeqCst);
            scheduler.set_telemetry_sink(Some(probe.clone()));
            let stopped = permit.bind_task_context(RuntimeTaskContext::without_deadline(
                probe.cancellation.clone(),
            ));
            let work = CheckpointWorkContext::new(stopped).with_scheduler(scheduler.clone());
            let error = match lookup(&store, relationship, &work) {
                Err(error) => error,
                Ok(_) => panic!("cancellation at unit {stop}/{units} escaped"),
            };
            assert!(!matches!(error, HawDBError::StorageIntegrity(_)), "{error}");
            assert!(probe.cancellation.is_cancelled());
            probe.assert_released(&scheduler);
            assert_eq!(available(&task), idle);
            assert_eq!(store.checkpoint_source_identity(), identity);
            assert!(!store.canonical_base.as_ref().unwrap().is_poisoned());
            drop(work);
            scheduler.set_telemetry_sink(None);
            let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
            let decoded = lookup(&store, relationship, &work).unwrap();
            compare(&store, relationship, &decoded);
            drop(decoded);
            assert_eq!(available(&task), idle);
            assert_eq!(governor.snapshot().admissions, 1);
        }
    }
    assert_eq!(
        std::fs::read(store.canonical_base.as_ref().unwrap().path()).unwrap(),
        physical
    );
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(store);
    let mut catalog = Catalog::default();
    let reopened = GraphStore::open(&directory, &mut catalog).unwrap();
    assert_eq!(
        reopened.node_owned(NodeId(0)).unwrap().unwrap().properties["list"],
        Value::List((0..1057).map(Value::Int).collect())
    );
    assert_eq!(
        reopened
            .relationship_owned(RelId(0))
            .unwrap()
            .unwrap()
            .properties["binary"],
        Value::Binary(vec![0x9a; 70_003])
    );
    drop(reopened);
    std::fs::remove_dir_all(directory).unwrap();
}
