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
use std::sync::atomic::Ordering;

fn assert_rejected_estimate_is_cooperative(operation: WalOp, minimum_visits: usize) {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-delta-estimate-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &directory,
        &mut catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            max_out_of_core_delta_bytes: Some(32_000),
            ..WalReplayConfig::default()
        },
    )
    .unwrap();
    store
        .create_node(&mut catalog, "Anchor", BTreeMap::new())
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    assert!(store.is_out_of_core());
    let epoch = store.commit_epoch();
    let source = store.checkpoint_source_identity();
    let mut ordinary = store.snapshot();
    let mut ordinary_catalog = catalog.clone();
    let expected = ordinary
        .apply_replayed_wal_transaction(&mut ordinary_catalog, operation.clone())
        .unwrap_err()
        .to_string();
    assert!(expected.contains("out-of-core mutation delta admission rejected"));

    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4 * 1024 * 1024))
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    scheduler.set_telemetry_sink(Some(probe.clone()));
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
        .with_scheduler(scheduler.clone());
    let error = store
        .apply_replayed_checkpoint_wal_transaction(&mut catalog, operation, &work)
        .unwrap_err();
    assert_eq!(error.to_string(), expected);
    assert_eq!(store.commit_epoch(), epoch);
    assert_eq!(store.checkpoint_source_identity(), source);
    assert_eq!(store.node_count_for_label(None), 1);
    assert!(store
        .search_projection_graph_changes_after(epoch)
        .is_empty());
    assert_eq!(catalog.label_id("CheckpointDelta"), None);
    probe.assert_released(&scheduler);
    let visits = probe.completed.load(Ordering::SeqCst);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(ordinary);
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
    assert!(
        visits >= minimum_visits,
        "real out-of-core replay estimated {minimum_visits} entries before rejection with only {visits} admitted units"
    );
}

#[test]
fn checkpoint_units_wal_replay_delta_estimation_admits_1057_operations_before_rejection() {
    assert_rejected_estimate_is_cooperative(
        WalOp::Batch(
            (1..=1057)
                .map(|id| WalOp::CreateNode {
                    id: NodeId(id),
                    label: "CheckpointDelta".into(),
                    properties: BTreeMap::new(),
                })
                .collect(),
        ),
        1057,
    );
}

#[test]
fn checkpoint_units_wal_replay_delta_estimation_admits_1057_nested_values_before_rejection() {
    assert_rejected_estimate_is_cooperative(
        WalOp::CreateNode {
            id: NodeId(1),
            label: "CheckpointDelta".into(),
            properties: BTreeMap::from([(
                "payload".into(),
                Value::List((0..1057).map(|_| Value::String("x".repeat(32))).collect()),
            )]),
        },
        1057,
    );
}
