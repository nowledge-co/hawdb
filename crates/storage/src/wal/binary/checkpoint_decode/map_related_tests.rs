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
use crate::background::{CheckpointAllocationOwner, CheckpointWorkProbe};
use hawdb_core::RuntimeMemoryError;
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeTaskContext,
    RuntimeWorkRequest, WorkClass, WorkRequest,
};
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;

fn governor(ceiling: u64) -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

fn remaining(task: &RuntimeTaskContext) -> u64 {
    let ceiling = task.memory_reservation().unwrap().memory_bytes();
    match task.reserve_working_memory(ceiling) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => available_bytes,
        _ => panic!("the exact-ceiling probe must reach the governor ledger"),
    }
}

fn permit_overhead() -> u64 {
    let governor = governor(1024);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1024))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let before = remaining(&task);
    let lease = task.reserve_working_memory(1).unwrap().unwrap();
    assert_eq!(lease.bytes(), 1);
    let overhead = before - remaining(&task) - 1;
    drop(lease);
    assert_eq!(remaining(&task), before);
    overhead
}

fn entries() -> [WalEntry; 2] {
    [
        WalEntry {
            lsn: 17,
            op: WalOp::SetNodeProperty {
                id: NodeId(3),
                property: String::new(),
                value: Value::Map(BTreeMap::from([(String::new(), Value::Int(37))])),
            },
        },
        WalEntry {
            lsn: 17,
            op: WalOp::CreateNode {
                id: NodeId(3),
                label: String::new(),
                properties: BTreeMap::from([(String::new(), Value::Int(37))]),
            },
        },
    ]
}

#[test]
fn checkpoint_units_wal_decode_map_memory_one_byte_denial_and_full_same_reservation_retry() {
    let overhead = permit_overhead();
    let node_bytes = 2 * map_memory::MapMemory::node_bytes() as u64;
    let required = node_bytes + CheckpointAllocationOwner::METADATA_BYTES as u64 + 2 * overhead;
    for entry in entries() {
        let bytes = encode_binary_wal_record(&entry, 19).unwrap();
        let ceiling = required + overhead;
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let held = task.reserve_working_memory(1).unwrap().unwrap();
        assert_eq!(remaining(&task), required - 1);
        let work = CheckpointWorkContext::new(task.clone());
        let error = match decode_binary_wal_record_with_work_context(&bytes, &work) {
            Err(HawDBError::Execution(error)) => error,
            _ => panic!("one byte of missing node coverage must defer before insertion"),
        };
        assert!(error.contains(&format!("requested {} bytes", node_bytes + overhead)));
        assert!(error.contains(&format!("{}-byte remaining", node_bytes + overhead - 1)));
        assert_eq!(remaining(&task), required - 1);
        drop(held);
        super::tests::parity(&bytes, &work);
        assert_eq!(remaining(&task), ceiling);
        assert_eq!(governor.snapshot().admissions, 1);
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn checkpoint_units_wal_decode_map_memory_split_growth_preserves_properties_and_nested_values() {
    let map = (0..1057)
        .map(|id| (format!("{id:016x}"), Value::Int(id)))
        .collect::<BTreeMap<_, _>>();
    for op in [
        WalOp::CreateNode {
            id: NodeId(3),
            label: "label".into(),
            properties: map.clone(),
        },
        WalOp::SetNodeProperty {
            id: NodeId(3),
            property: "value".into(),
            value: Value::Map(BTreeMap::from([
                ("left".into(), Value::Map(map.clone())),
                ("right".into(), Value::Map(map.clone())),
            ])),
        },
    ] {
        let bytes = encode_binary_wal_record(&WalEntry { lsn: 17, op }, 19).unwrap();
        let ceiling = 4 * 1024 * 1024;
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let decoded = match decode_binary_wal_record_with_work_context(&bytes, &work).unwrap() {
            BinaryWalRecordDecode::Entry { entry, .. } => entry,
            BinaryWalRecordDecode::Corrupt(_) => panic!("complete split-growth record must decode"),
        };
        assert_eq!(encode_binary_wal_record(&decoded, 19).unwrap(), bytes);
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
        assert_eq!(encode_binary_wal_record(&decoded, 19).unwrap(), bytes);
        drop(decoded);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn checkpoint_units_wal_decode_map_memory_cancel_every_unit_and_retry_same_governor() {
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    for entry in entries() {
        let bytes = encode_binary_wal_record(&entry, 19).unwrap();
        let baseline = Arc::new(CheckpointWorkProbe::default());
        scheduler.set_telemetry_sink(Some(baseline.clone()));
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
                .with_scheduler(scheduler.clone());
        super::tests::parity(&bytes, &work);
        let units = baseline.completed.load(Ordering::SeqCst);
        assert!(units > 10);
        baseline.assert_released(&scheduler);
        drop(work);
        for stop in 0..=units {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(stop, Ordering::SeqCst);
            scheduler.set_telemetry_sink(Some(probe.clone()));
            let work = CheckpointWorkContext::new(permit.bind_task_context(
                RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
            ))
            .with_scheduler(scheduler.clone());
            let held = (stop == 0).then(|| {
                scheduler
                    .try_start(WorkRequest::background(WorkClass::Mutation, 1))
                    .unwrap()
            });
            assert!(matches!(
                decode_binary_wal_record_with_work_context(&bytes, &work),
                Err(HawDBError::Execution(_))
            ));
            drop(held);
            probe.assert_released(&scheduler);
            drop(work);
            scheduler.set_telemetry_sink(None);
            let task = permit.bind_task_context(RuntimeTaskContext::default());
            assert_eq!(remaining(&task), ceiling);
            let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
            super::tests::parity(&bytes, &work);
            assert_eq!(remaining(&task), ceiling);
        }
    }
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
