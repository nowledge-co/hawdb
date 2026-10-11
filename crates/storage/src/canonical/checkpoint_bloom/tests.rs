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
use crate::background::{CheckpointOperationError, CheckpointWorkProbe};
use crate::canonical::{encode_string, encode_value, node_property_bloom_key, MAX_VALUE_DEPTH};
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(1),
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

fn assert_no_working_memory(task: &RuntimeTaskContext) {
    match task.reserve_working_memory(1) {
        Ok(lease) => drop(lease),
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes: 1, ..
        }) => {}
        other => panic!("hashing must not retain working memory: {other:?}"),
    }
}

fn reference(label: LabelId, property: &str, value: &Value) -> u64 {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&label.0.to_le_bytes());
    encode_string(property, &mut bytes).unwrap();
    encode_value(value, &mut bytes, 0).unwrap();
    crate::cache::content_digest(&bytes).0
}

fn source() -> (LabelId, String, Value, u64) {
    let label = LabelId(23);
    let property = format!("{}🦀\0\t\n", "界".repeat(87_723));
    let value = Value::Map(BTreeMap::from([
        ("binary".into(), Value::Binary(vec![0x9f; 257 * 1024 + 3])),
        (
            "nested".into(),
            Value::List(vec![
                Value::String(format!("{}🦀\0", "字".repeat(87_723))),
                Value::Map(BTreeMap::from([
                    (String::new(), Value::Bool(true)),
                    ("null".into(), Value::Null),
                ])),
                Value::List((0..16).map(Value::Int).collect()),
            ]),
        ),
    ]));
    let expected = reference(label, &property, &value);
    (label, property, value, expected)
}

#[test]
fn checkpoint_units_canonical_bloom_memory_related_matches_buffered_codec_allocation_free() {
    let mut values = vec![
        Value::Null,
        Value::Bool(false),
        Value::Bool(true),
        Value::Int(i64::MIN),
        Value::Int(i64::MAX),
        Value::Float(-0.0),
        Value::Float(f64::from_bits(0x7ff8_0123_4567_89ab)),
        Value::Float(f64::INFINITY),
        Value::Uuid(hawdb_core::Uuid::from_bytes([0x9f; 16])),
        Value::List(Vec::new()),
        Value::Map(BTreeMap::new()),
    ];
    for count in [0, 1, 65535, 65536, 65537, 257 * 1024 + 3] {
        values.push(Value::Binary(vec![0x9f; count]));
        values.push(Value::String("界".repeat(count / 3) + "🦀\0"));
    }
    let (label, property, nested, _) = source();
    values.push(nested);
    for value in &values {
        let expected = reference(label, &property, value);
        let observation = crate::test_allocator::AllocationObservation::start_all();
        let actual = node_property_bloom_key(label, &property, value).unwrap();
        let allocations = observation.finish();
        assert_eq!(actual, expected);
        assert_eq!(allocations, 0);
    }
}

#[test]
fn checkpoint_units_canonical_bloom_memory_related_every_actual_unit_cancels_and_same_reservation_retries(
) {
    let (label, property, value, expected) = source();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let baseline = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(baseline.clone()));
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    assert_eq!(
        key(label, &property, &value, Some(&work)).unwrap(),
        expected
    );
    let total = baseline.completed.load(Ordering::SeqCst);
    assert!(total > 64);
    assert_eq!(baseline.peak_units.load(Ordering::SeqCst), 1);
    assert_eq!(baseline.io_waves.load(Ordering::SeqCst), 0);
    baseline.assert_released(&local);
    assert_no_working_memory(&task);
    drop(work);
    for cut in 1..=total {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        assert!(matches!(
            key(label, &property, &value, Some(&work)),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), cut);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&local);
        assert_no_working_memory(&task);
        assert_eq!(
            node_property_bloom_key(label, &property, &value).unwrap(),
            expected
        );
    }
    local.set_telemetry_sink(None);
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    assert_eq!(
        key(label, &property, &value, Some(&work)).unwrap(),
        expected
    );
    assert_no_working_memory(&task);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
}

#[test]
fn checkpoint_units_canonical_bloom_memory_related_stopped_io_adapter_retains_nested_work_classification(
) {
    let (label, property, value, expected) = source();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(2, Ordering::SeqCst);
    local.set_telemetry_sink(Some(probe.clone()));
    let work = CheckpointWorkContext::new(permit.bind_task_context(
        RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
    ))
    .with_scheduler(local.clone());
    assert!(matches!(
        work.classify(|work| key(label, &property, &value, Some(work))),
        Err(CheckpointOperationError::Work(
            CheckpointWorkError::Stopped(_)
        ))
    ));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 2);
    probe.assert_released(&local);
    assert_no_working_memory(&task);
    drop(work);
    local.set_telemetry_sink(None);
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    assert_eq!(
        key(label, &property, &value, Some(&work)).unwrap(),
        expected
    );
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_bloom_memory_related_preserves_nested_value_error_and_retries() {
    let mut value = Value::Null;
    for _ in 0..MAX_VALUE_DEPTH + 2 {
        value = Value::List(vec![value]);
    }
    let label = LabelId(23);
    let mut ordinary = Vec::new();
    let expected = encode_value(&value, &mut ordinary, 0)
        .unwrap_err()
        .to_string();
    assert_eq!(
        node_property_bloom_key(label, "payload", &value)
            .unwrap_err()
            .to_string(),
        expected
    );
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    assert_eq!(
        key(label, "payload", &value, Some(&work))
            .unwrap_err()
            .to_string(),
        expected
    );
    assert_no_working_memory(&task);
    let valid = Value::Binary(vec![0x9f; 257 * 1024 + 3]);
    assert_eq!(
        key(label, "payload", &valid, Some(&work)).unwrap(),
        reference(label, "payload", &valid)
    );
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
