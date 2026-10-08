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

#[test]
fn checkpoint_units_wal_replay_prescan_admits_1057_operations_before_late_corruption() {
    let mut operations: Vec<_> = (0..1057)
        .map(|id| WalOp::CreateNode {
            id: NodeId(id),
            label: String::new(),
            properties: BTreeMap::new(),
        })
        .collect();
    // The real relational capture scan must visit this complete prefix before
    // diagnosing the final malformed record. No replay mutation may run first.
    operations.push(WalOp::Relational {
        record: Vec::new().into(),
    });
    let mut ordinary = GraphStore::in_memory();
    let mut ordinary_catalog = Catalog::default();
    let expected = ordinary
        .apply_replayed_wal_transaction(&mut ordinary_catalog, WalOp::Batch(operations.clone()))
        .unwrap_err()
        .to_string();
    assert_eq!(ordinary.commit_epoch(), 0);
    assert_eq!(ordinary.node_count_for_label(None), 0);

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
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let error = store
        .apply_replayed_checkpoint_wal_transaction(&mut catalog, WalOp::Batch(operations), &work)
        .unwrap_err();
    assert_eq!(error.to_string(), expected);
    assert_eq!(store.commit_epoch(), 0);
    assert_eq!(store.node_count_for_label(None), 0);
    probe.assert_released(&scheduler);
    let units = probe.completed.load(Ordering::SeqCst);
    drop(store);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert!(
        units >= 1057,
        "a real 1057-operation pre-scan reached late corruption with only {units} admitted units"
    );
}
