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
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeTaskContext, RuntimeWorkRequest,
};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

fn source() -> (PathBuf, GraphStore, Catalog, NodeRecord) {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-native-decode-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &directory,
        &mut catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            max_out_of_core_delta_bytes: Some(32 * 1024 * 1024),
            ..WalReplayConfig::default()
        },
    )
    .unwrap();
    let properties = (0..1057)
        .map(|i| {
            (
                format!("k{i:04}-{}", "x".repeat(1024)),
                Value::Int(i as i64),
            )
        })
        .collect();
    let id = store
        .create_node(&mut catalog, "Source", properties)
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    assert!(store.is_out_of_core());
    let expected = store.node_owned(id).unwrap().unwrap();
    assert_eq!(expected.properties.len(), 1057);
    assert!(expected.properties.keys().map(String::len).sum::<usize>() > 1024 * 1024);
    (directory, store, catalog, expected)
}

fn governor(bytes: u64) -> RuntimeGovernor {
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(bytes),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    governor
}

#[test]
fn checkpoint_units_wal_replay_native_decode_visits_all_1057_dictionary_properties() {
    let (directory, store, _catalog, expected) = source();
    let identity = store.checkpoint_source_identity();
    let ceiling = 8 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    scheduler.set_telemetry_sink(Some(probe.clone()));
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
    assert_eq!(
        store.checkpoint_node_owned(expected.id, &work).unwrap(),
        Some(expected)
    );
    let units = probe.completed.load(Ordering::SeqCst);
    assert!(
        units >= 1057,
        "1057 decoded properties visited with only {units} work units"
    );
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    probe.assert_released(&scheduler);
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert!(!store.canonical_base.as_ref().unwrap().is_poisoned());
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_wal_replay_native_decode_denies_unadmitted_dictionary_key_clones() {
    let (directory, store, _catalog, expected) = source();
    let identity = store.checkpoint_source_identity();
    let ceiling = 128 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let error = store.checkpoint_node_owned(expected.id, &work).unwrap_err();
    assert!(
        error.to_string().contains("remaining reservation"),
        "{error}"
    );
    assert!(!matches!(error, HawDBError::StorageIntegrity(_)), "{error}");
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert!(!store.canonical_base.as_ref().unwrap().is_poisoned());
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}
