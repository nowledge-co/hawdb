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
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeTaskContext, RuntimeWorkRequest,
};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

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

fn operation(relationships: bool) -> WalOp {
    let update = if relationships {
        WalOp::SetRelationshipProperty {
            id: RelId(1056),
            property: "value".into(),
            value: Value::Int(7),
        }
    } else {
        WalOp::SetNodeProperty {
            id: NodeId(1056),
            property: "value".into(),
            value: Value::Int(7),
        }
    };
    // Delta estimation must point-read the last real record before the later
    // malformed relational operation prevents all graph/changefeed mutation.
    WalOp::Batch(vec![
        update,
        WalOp::Relational {
            record: Vec::new().into(),
        },
    ])
}

fn run(relationships: bool, bytes: u64) -> (usize, String) {
    let (directory, mut store, mut catalog) = source(relationships);
    let identity = store.checkpoint_source_identity();
    let epoch = store.commit_epoch();
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(bytes),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(bytes).with_io_wave_slots(1))
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
        .apply_replayed_checkpoint_wal_transaction(&mut catalog, operation(relationships), &work)
        .unwrap_err();
    assert_eq!(store.commit_epoch(), epoch);
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert!(store
        .search_projection_graph_changes_after(epoch)
        .is_empty());
    probe.assert_released(&scheduler);
    let visits = probe.completed.load(Ordering::SeqCst);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
    (visits, error.to_string())
}

#[test]
fn checkpoint_units_wal_replay_native_point_admits_all_1057_node_record_visits() {
    let (visits, error) = run(false, 4 * 1024 * 1024);
    assert!(
        error.contains("invalid relational durable record header"),
        "{error}"
    );
    assert!(
        visits >= 1057,
        "real 1057-node point read reached late corruption with only {visits} admitted units"
    );
}

#[test]
fn checkpoint_units_wal_replay_native_point_admits_all_1057_relationship_record_visits() {
    let (visits, error) = run(true, 4 * 1024 * 1024);
    assert!(
        error.contains("invalid relational durable record header"),
        "{error}"
    );
    assert!(visits >= 1057, "real 1057-relationship point read reached late corruption with only {visits} admitted units");
}

#[test]
fn checkpoint_units_wal_replay_native_point_denies_unadmitted_full_segment_buffers() {
    for relationships in [false, true] {
        let (_, error) = run(relationships, 4096);
        assert!(
            error.contains("remaining reservation"),
            "real segment point read escaped a 4096-byte reservation: {error}"
        );
    }
}
