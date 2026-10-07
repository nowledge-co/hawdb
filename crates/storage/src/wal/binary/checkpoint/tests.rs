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
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
    WorkClass, WorkRequest,
};
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn governor(bytes: u64) -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(bytes),
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

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    })
}

fn payload_entry() -> WalEntry {
    WalEntry {
        lsn: u64::MAX,
        op: WalOp::SetNodeProperty {
            id: NodeId(u64::MAX),
            property: "body\0中文".into(),
            value: Value::Map(BTreeMap::from([
                (
                    "list".into(),
                    Value::List(vec![
                        Value::Null,
                        Value::Bool(true),
                        Value::Int(i64::MIN),
                        Value::Float(f64::from_bits(0xfff8000000000123)),
                    ]),
                ),
                ("text".into(), Value::String("\0中文🦀".repeat(8193))),
                (
                    "bytes".into(),
                    Value::Binary((0..3 * 64 * 1024 + 7).map(|i| (i % 256) as u8).collect()),
                ),
                ("empty".into(), Value::Map(BTreeMap::new())),
            ])),
        },
    }
}

#[test]
fn checkpoint_units_wal_payload_complete_bytes_match_all_ops_values_and_envelopes() {
    let local = scheduler();
    let work = CheckpointWorkContext::default().with_scheduler(local.clone());
    let mut ops = super::super::tests::sample_ops();
    ops.push(payload_entry().op);
    for value in [
        Value::Uuid(hawdb_core::Uuid::from_bytes([0; 16])),
        Value::Uuid(hawdb_core::Uuid::from_bytes([255; 16])),
        Value::String(String::new()),
        Value::Binary(Vec::new()),
        Value::Bool(false),
    ] {
        ops.push(WalOp::SetNodeProperty {
            id: NodeId(1),
            property: "scalar".into(),
            value,
        });
    }
    for integer in [i64::MIN, -1, 0, 1, i64::MAX] {
        ops.push(WalOp::SetRelationshipProperty {
            id: RelId(1),
            property: "integer".into(),
            value: Value::Int(integer),
        });
    }
    let at_limit = (1..MAX_VALUE_DEPTH).fold(Value::Null, |value, _| Value::List(vec![value]));
    ops.push(WalOp::SetNodeProperty {
        id: NodeId(1),
        property: "depth_limit".into(),
        value: at_limit,
    });
    for bits in [
        0,
        u64::MAX,
        0x8000000000000000,
        0x7ff0000000000000,
        0x7ff8000000000123,
    ] {
        ops.push(WalOp::SetRelationshipProperty {
            id: RelId(u64::MAX),
            property: "bits".into(),
            value: Value::Float(f64::from_bits(bits)),
        });
    }
    for value in [Value::List(Vec::new()), Value::Map(BTreeMap::new())] {
        let mut deep = value;
        for _ in 0..8 {
            deep = Value::List(vec![Value::Map(BTreeMap::from([(
                "\0key中文".into(),
                deep,
            )]))]);
        }
        ops.push(WalOp::SetNodeProperty {
            id: NodeId(0),
            property: String::new(),
            value: deep,
        });
    }
    let all = WalOp::Batch(
        ops.iter()
            .filter(|op| !matches!(op, WalOp::Batch(_)))
            .cloned()
            .collect(),
    );
    ops.push(all);
    for op in ops {
        let entry = WalEntry { lsn: u64::MAX, op };
        for epoch in [0, 1, u64::MAX] {
            assert_eq!(
                &*encode_binary_wal_record_with_work_context(&entry, epoch, &work).unwrap(),
                encode_binary_wal_record(&entry, epoch).unwrap()
            );
        }
    }
    assert_eq!(local.snapshot().running_background_operations, 0);
}

#[test]
fn checkpoint_units_wal_payload_admits_exact_capacity_and_retains_bytes_after_execution() {
    let entry = payload_entry();
    let expected = encode_binary_wal_record(&entry, 23).unwrap();
    let requested = expected.len() as u64 + 24;
    let denied = governor(requested - 1);
    let permit = denied
        .try_admit(RuntimeWorkRequest::background_maintenance(requested - 1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let result = work.classify(|work| encode_binary_wal_record_with_work_context(&entry, 23, work));
    assert!(
        matches!(result, Err(crate::background::CheckpointOperationError::Work(CheckpointWorkError::Memory(RuntimeMemoryError::ReservationExceeded { requested_bytes, available_bytes }))) if requested_bytes == requested && available_bytes == requested - 1)
    );
    drop(work);
    drop(permit);
    assert_eq!(denied.snapshot().admitted_memory_bytes, 0);

    let allowed = governor(requested);
    let permit = allowed
        .try_admit(RuntimeWorkRequest::background_maintenance(requested))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler());
    let output = encode_binary_wal_record_with_work_context(&entry, 23, &work).unwrap();
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes: 0,
            ..
        })
    ));
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(allowed.snapshot().active_background_tasks, 0);
    assert_eq!(allowed.snapshot().active_cpu_slots, 0);
    assert_eq!(allowed.snapshot().admitted_memory_bytes, requested);
    assert_eq!(&*output, expected);
    drop(output);
    assert_eq!(allowed.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_wal_payload_cancels_every_actual_unit_and_fully_retries_same_reservation() {
    let entry = payload_entry();
    let expected = encode_binary_wal_record(&entry, 31).unwrap();
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    let baseline_work = baseline.context(local.clone());
    assert_eq!(
        &*baseline_work
            .classify(|work| encode_binary_wal_record_with_work_context(&entry, 31, work))
            .unwrap_or_else(|_| panic!("baseline admitted payload encoding must complete")),
        expected
    );
    let units = baseline.completed.load(Ordering::SeqCst);
    assert!(units > 100);
    baseline.assert_released(&local);
    for stop in 0..=units {
        let ceiling = 2 * 1024 * 1024;
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(stop, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let task = permit.bind_task_context(RuntimeTaskContext::without_deadline(
            probe.cancellation.clone(),
        ));
        let work = CheckpointWorkContext::new(task).with_scheduler(local.clone());
        let held = (stop == 0).then(|| {
            local
                .try_start(WorkRequest::background(WorkClass::Mutation, 1))
                .unwrap()
        });
        let result =
            work.classify(|work| encode_binary_wal_record_with_work_context(&entry, 31, work));
        assert!(
            matches!(result, Err(crate::background::CheckpointOperationError::Work(CheckpointWorkError::Admission(_))) if stop == 0)
                || matches!(result, Err(crate::background::CheckpointOperationError::Work(CheckpointWorkError::Stopped(_))) if stop > 0)
        );
        drop(held);
        probe.assert_released(&local);
        drop(work);
        local.set_telemetry_sink(None);
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        assert!(
            matches!(task.reserve_working_memory(ceiling), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
        let output = encode_binary_wal_record_with_work_context(&entry, 31, &work).unwrap();
        assert_eq!(&*output, expected);
        drop(output);
        assert!(
            matches!(task.reserve_working_memory(ceiling), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(local.snapshot().running_background_operations, 0);
    }
}

#[test]
fn checkpoint_units_wal_payload_preserves_invalid_value_and_nested_batch_diagnostic_priority() {
    let mut deep = Value::Null;
    for _ in 0..MAX_VALUE_DEPTH {
        deep = Value::List(vec![deep]);
    }
    let invalid = WalOp::SetNodeProperty {
        id: NodeId(1),
        property: "deep".into(),
        value: deep,
    };
    for op in [
        invalid.clone(),
        WalOp::Batch(vec![WalOp::Batch(Vec::new())]),
        WalOp::Batch(vec![WalOp::Batch(Vec::new()), invalid]),
    ] {
        let entry = WalEntry { lsn: 1, op };
        let expected = encode_binary_wal_record(&entry, 1).unwrap_err().to_string();
        let governor = governor(1);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(1))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
                .with_scheduler(scheduler());
        let actual = encode_binary_wal_record_with_work_context(&entry, 1, &work)
            .unwrap_err()
            .to_string();
        assert_eq!(actual, expected);
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}
