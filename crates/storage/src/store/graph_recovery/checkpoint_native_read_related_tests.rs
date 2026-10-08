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
use crate::mutation::MatchedRelationshipCreate;
use hawdb_core::RuntimeMemoryError;
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeTaskContext, RuntimeWorkRequest,
};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

const CEILING: u64 = 4 * 1024 * 1024;

fn source(relationships: bool) -> (PathBuf, GraphStore, Catalog) {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-native-point-{}",
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
    if relationships {
        store
            .create_node(&mut catalog, "Source", BTreeMap::new())
            .unwrap();
        store
            .create_node(&mut catalog, "Target", BTreeMap::new())
            .unwrap();
        let operations = (0..1057)
            .map(|_| {
                GraphMutation::CreateRelationshipsBetweenMatches(MatchedRelationshipCreate {
                    source_label: "Source".into(),
                    source_filter: None,
                    rel_type: "LINK".into(),
                    rel_properties: BTreeMap::new(),
                    target_label: "Target".into(),
                    target_filter: None,
                })
            })
            .collect();
        store.commit_mutations(&mut catalog, operations).unwrap();
    } else {
        let operations = (0..1057)
            .map(|_| GraphMutation::CreateNode {
                label: "Source".into(),
                properties: BTreeMap::new(),
            })
            .collect();
        store.commit_mutations(&mut catalog, operations).unwrap();
    }
    store.checkpoint(&catalog).unwrap();
    assert!(store.is_out_of_core());
    let manifest = store.canonical_base.as_ref().unwrap().manifest();
    if relationships {
        assert_eq!(manifest.relationship_count, 1057);
        assert_eq!(manifest.relationship_segment_count, 1);
    } else {
        assert_eq!(manifest.node_count, 1057);
        assert_eq!(manifest.node_segment_count, 1);
    }
    (directory, store, catalog)
}

#[derive(Debug, PartialEq)]
enum Record {
    Node(NodeRecord),
    Relationship(RelRecord),
}

fn ordinary(store: &GraphStore, relationships: bool, id: u64) -> Result<Option<Record>> {
    if relationships {
        store
            .relationship_owned(RelId(id))
            .map(|r| r.map(Record::Relationship))
    } else {
        store.node_owned(NodeId(id)).map(|r| r.map(Record::Node))
    }
}

fn lookup(
    store: &GraphStore,
    relationships: bool,
    id: u64,
    work: &CheckpointWorkContext,
) -> Result<Option<Record>> {
    if relationships {
        store
            .checkpoint_relationship_owned(RelId(id), work)
            .map(|r| r.map(Record::Relationship))
    } else {
        store
            .checkpoint_node_owned(NodeId(id), work)
            .map(|r| r.map(Record::Node))
    }
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
        _ => panic!("the exact ceiling must expose the working-memory remainder"),
    }
}

#[test]
fn checkpoint_units_wal_replay_native_point_cancels_every_visit_and_retries_complete_records() {
    for relationships in [false, true] {
        let (directory, source, _catalog) = source(relationships);
        let identity = source.checkpoint_source_identity();
        let physical = std::fs::read(source.canonical_base.as_ref().unwrap().path()).unwrap();
        let expected = ordinary(&source, relationships, 1056).unwrap();
        assert!(expected.is_some());
        let governor = governor();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let idle = available(&task);
        let scheduler = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        scheduler.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
        assert_eq!(
            lookup(&source, relationships, 1056, &work).unwrap(),
            expected
        );
        let units = probe.completed.load(Ordering::SeqCst);
        assert!(units >= 1057);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&scheduler);
        assert_eq!(available(&task), idle);
        drop(work);
        for stop in 1..=units {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(stop, Ordering::SeqCst);
            scheduler.set_telemetry_sink(Some(probe.clone()));
            let stopped = permit.bind_task_context(RuntimeTaskContext::without_deadline(
                probe.cancellation.clone(),
            ));
            let work = CheckpointWorkContext::new(stopped).with_scheduler(scheduler.clone());
            let error = lookup(&source, relationships, 1056, &work).unwrap_err();
            assert!(!matches!(error, HawDBError::StorageIntegrity(_)), "{error}");
            assert!(probe.cancellation.is_cancelled());
            assert!(!source.canonical_base.as_ref().unwrap().is_poisoned());
            assert_eq!(source.checkpoint_source_identity(), identity);
            probe.assert_released(&scheduler);
            assert_eq!(available(&task), idle);
            drop(work);
            scheduler.set_telemetry_sink(None);
            let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
            assert_eq!(
                lookup(&source, relationships, 1056, &work).unwrap(),
                expected
            );
            assert_eq!(available(&task), idle);
            assert_eq!(governor.snapshot().admissions, 1);
        }
        assert_eq!(
            std::fs::read(source.canonical_base.as_ref().unwrap().path()).unwrap(),
            physical
        );
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        drop(source);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn checkpoint_units_wal_replay_native_point_denies_memory_without_poison_and_retries_same_reservation(
) {
    for relationships in [false, true] {
        let (directory, source, _catalog) = source(relationships);
        let expected = ordinary(&source, relationships, 1056).unwrap();
        let governor = governor();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let idle = available(&task);
        let held = task.reserve_working_memory(idle - 4096).unwrap().unwrap();
        let before = available(&task);
        let work = CheckpointWorkContext::new(task.clone());
        let error = lookup(&source, relationships, 1056, &work).unwrap_err();
        assert!(
            error.to_string().contains("remaining reservation"),
            "{error}"
        );
        assert!(!matches!(error, HawDBError::StorageIntegrity(_)), "{error}");
        assert!(!source.canonical_base.as_ref().unwrap().is_poisoned());
        assert_eq!(available(&task), before);
        drop(held);
        assert_eq!(
            lookup(&source, relationships, 1056, &work).unwrap(),
            expected
        );
        assert_eq!(available(&task), idle);
        assert_eq!(governor.snapshot().admissions, 1);
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        drop(source);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn checkpoint_units_wal_replay_native_point_zero_io_reservation_denies_before_metadata_lookup() {
    for relationships in [false, true] {
        let (directory, source, _catalog) = source(relationships);
        let cache = source.segment_cache_snapshot().unwrap();
        let governor = governor();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(CEILING))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let idle = available(&task);
        let work = CheckpointWorkContext::new(task.clone());
        let error = lookup(&source, relationships, 1056, &work).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("exceeding the 0-slot reservation"),
            "{error}"
        );
        assert!(!matches!(error, HawDBError::StorageIntegrity(_)), "{error}");
        assert!(!source.canonical_base.as_ref().unwrap().is_poisoned());
        assert_eq!(source.segment_cache_snapshot().unwrap(), cache);
        assert_eq!(available(&task), idle);
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        drop(source);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn checkpoint_units_wal_replay_native_point_keeps_absent_id_semantics_and_corruption_poison() {
    for relationships in [false, true] {
        let (directory, source, _catalog) = source(relationships);
        let governor = governor();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(CEILING).with_io_wave_slots(1))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let idle = available(&task);
        let work = CheckpointWorkContext::new(task.clone());
        for id in [0, 1056, 1057, u64::MAX] {
            assert_eq!(
                lookup(&source, relationships, id, &work).unwrap(),
                ordinary(&source, relationships, id).unwrap()
            );
            assert_eq!(available(&task), idle);
        }
        let path = source.canonical_base.as_ref().unwrap().path();
        let mut bytes = std::fs::read(path).unwrap();
        let last = bytes.last_mut().unwrap();
        *last ^= 1;
        std::fs::write(path, bytes).unwrap();
        let error = lookup(&source, relationships, 1056, &work).unwrap_err();
        assert!(matches!(error, HawDBError::StorageIntegrity(_)), "{error}");
        assert!(source.canonical_base.as_ref().unwrap().is_poisoned());
        assert!(ordinary(&source, relationships, 1056).is_err());
        assert_eq!(available(&task), idle);
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        drop(source);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
