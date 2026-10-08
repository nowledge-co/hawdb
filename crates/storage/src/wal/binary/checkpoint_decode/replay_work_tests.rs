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
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeTaskContext, RuntimeWorkRequest,
};
use std::sync::atomic::Ordering;

#[test]
fn checkpoint_units_wal_replay_admits_every_operation_in_1057_operation_transaction() {
    let operations = (0..1057)
        .map(|id| WalOp::CreateNode {
            id: NodeId(id),
            label: String::new(),
            properties: BTreeMap::new(),
        })
        .collect();
    let bytes = encode_binary_wal_record(
        &WalEntry {
            lsn: 17,
            op: WalOp::Batch(operations),
        },
        19,
    )
    .unwrap();
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
    let decoded = match decode_binary_wal_record_with_work_context(&bytes, &work).unwrap() {
        BinaryWalRecordDecode::Entry { entry, .. } => entry,
        BinaryWalRecordDecode::Corrupt(reason) => panic!("valid transaction was corrupt: {reason}"),
    };
    let before_replay = probe.completed.load(Ordering::SeqCst);
    let mut store = crate::store::GraphStore::in_memory();
    let mut catalog = hawdb_core::Catalog::default();
    decoded
        .replay_into(&mut store, &mut catalog, &work)
        .unwrap();
    let replay_units = probe.completed.load(Ordering::SeqCst) - before_replay;
    assert_eq!(store.commit_epoch(), 1);
    assert_eq!(store.node_count_for_label(None), 1057);
    for id in 0..1057 {
        assert!(store.node_owned(NodeId(id)).unwrap().is_some());
    }
    probe.assert_released(&scheduler);
    assert!(
        replay_units >= 1057,
        "1057 real graph mutations completed with only {replay_units} admitted replay units"
    );
    drop(store);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
