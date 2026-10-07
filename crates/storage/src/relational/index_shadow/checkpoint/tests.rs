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
use crate::background::CheckpointOperationError;
use crate::relational::RelationalColumnSchema;
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
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

fn schema(value: RelationalValue) -> RelationalTableSchema {
    RelationalTableSchema {
        name: "checkpoint_schema_memory".into(),
        columns: vec![
            RelationalColumnSchema {
                name: "id".into(),
                scalar_type: RelationalScalarType::BigInt,
                nullable: false,
                default: None,
            },
            RelationalColumnSchema {
                name: "payload".into(),
                scalar_type: value.scalar_type().unwrap_or(RelationalScalarType::Text),
                nullable: true,
                default: Some(RelationalColumnDefault::Literal(value)),
            },
        ],
        primary_key: vec!["id".into()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    }
}

fn retry_and_check_source(schema: &RelationalTableSchema, expected: Sha256Digest) {
    let ceiling = 128 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    assert_eq!(
        relational_schema_digest_with_work_context(schema, &work).unwrap(),
        expected
    );
    assert!(matches!(
        task.reserve_working_memory(ceiling),
        Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
            if available_bytes == ceiling
    ));
    assert_eq!(relational_schema_digest(schema).unwrap(), expected);
    drop(work);
    drop(task);
    drop(permit);
    let closed = governor.snapshot();
    assert_eq!(closed.active_cpu_slots, 0);
    assert_eq!(closed.active_background_tasks, 0);
    assert_eq!(closed.admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_schema_memory_scalar_defaults_deny_before_allocating_and_fully_retry() {
    for value in [
        RelationalValue::Null,
        RelationalValue::Boolean(true),
        RelationalValue::BigInt(i64::MIN),
        RelationalValue::DoublePrecision(f64::from_bits(0xfff8_0000_0000_0023)),
        RelationalValue::Uuid(hawdb_core::Uuid::from_u128(17)),
    ] {
        let source = schema(value);
        let expected = relational_schema_digest(&source).unwrap();
        let governor = governor(1);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(1))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        assert!(matches!(
            work.classify(|work| relational_schema_digest_with_work_context(&source, work)),
            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                RuntimeMemoryError::ReservationExceeded { .. }
            )))
        ));
        assert_eq!(relational_schema_digest(&source).unwrap(), expected);
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        retry_and_check_source(&source, expected);
    }
}

#[test]
fn checkpoint_units_schema_memory_escape_scratch_denies_exactly_one_byte_and_fully_retries() {
    for value in [
        RelationalValue::Text("\0".repeat(64 * 1024 + 7)),
        RelationalValue::Bytea(vec![0; 64 * 1024 + 7]),
    ] {
        let source = schema(value);
        let expected = relational_schema_digest(&source).unwrap();
        // Include the actual governor's concrete 24-byte allocation lease.
        let requested = 64 * 1024 + 24;
        let ceiling = requested - 1;
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        assert!(matches!(
            work.classify(|work| relational_schema_digest_with_work_context(&source, work)),
            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                RuntimeMemoryError::ReservationExceeded { requested_bytes, available_bytes }
            ))) if requested_bytes == requested && available_bytes == requested - 1
        ));
        assert_eq!(relational_schema_digest(&source).unwrap(), expected);
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        retry_and_check_source(&source, expected);
    }
}

#[test]
fn checkpoint_units_schema_memory_cancels_every_actual_unit_without_leaks_and_fully_retries() {
    use crate::background::CheckpointWorkProbe;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler, WorkClass, WorkRequest};

    let mut values = vec![
        RelationalValue::Null,
        RelationalValue::Boolean(false),
        RelationalValue::BigInt(i64::MAX),
        RelationalValue::DoublePrecision(-0.0),
        RelationalValue::Uuid(hawdb_core::Uuid::from_u128(u128::MAX)),
        RelationalValue::Text("界🙂\0".repeat(17_000)),
    ];
    for length in [
        0,
        1,
        32 * 1024 - 1,
        32 * 1024,
        32 * 1024 + 1,
        3 * 64 * 1024 + 7,
    ] {
        values.push(RelationalValue::Bytea(
            (0..length)
                .map(|index| ((index * 71) % 256) as u8)
                .collect(),
        ));
    }
    for value in values {
        let source = schema(value);
        let expected = relational_schema_digest(&source).unwrap();
        let local = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(1),
            max_total_background_operations: Some(1),
            ..Default::default()
        });
        let baseline = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            relational_schema_digest_with_work_context(&source, &baseline.context(local.clone()))
                .unwrap(),
            expected
        );
        let units = baseline.completed.load(Ordering::SeqCst);
        assert!(units > 10);
        baseline.assert_released(&local);
        for stop in 0..=units {
            let ceiling = 128 * 1024;
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
            let error = relational_schema_digest_with_work_context(&source, &work).unwrap_err();
            assert!(
                error.to_string().contains(if stop == 0 {
                    "admission deferred"
                } else {
                    "stopped"
                }),
                "{error:?}"
            );
            drop(held);
            probe.assert_released(&local);
            assert_eq!(relational_schema_digest(&source).unwrap(), expected);
            drop(work);

            let fresh = permit.bind_task_context(RuntimeTaskContext::default());
            assert!(
                matches!(fresh.reserve_working_memory(ceiling), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
            );
            let retry = Arc::new(CheckpointWorkProbe::default());
            local.set_telemetry_sink(Some(retry.clone()));
            let retry_work =
                CheckpointWorkContext::new(fresh.clone()).with_scheduler(local.clone());
            assert_eq!(
                relational_schema_digest_with_work_context(&source, &retry_work).unwrap(),
                expected
            );
            retry.assert_released(&local);
            assert!(
                matches!(fresh.reserve_working_memory(ceiling), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
            );
            assert_eq!(relational_schema_digest(&source).unwrap(), expected);
            local.set_telemetry_sink(None);
            drop(retry_work);
            drop(fresh);
            drop(permit);
            let closed = governor.snapshot();
            assert_eq!(closed.active_cpu_slots, 0);
            assert_eq!(closed.active_background_tasks, 0);
            assert_eq!(closed.admitted_memory_bytes, 0);
        }
    }
}

#[test]
fn checkpoint_units_schema_memory_preserves_unsupported_default_diagnostics_without_leaking() {
    let reference = crate::relational::RelationalOverflowRef {
        digest: hawdb_integrity::integrity_digest(b"unsupported default").sha256,
        scalar_type: RelationalScalarType::Bytea,
        compressed_bytes: 19,
        uncompressed_bytes: 8192,
    };
    let source = schema(RelationalValue::Overflow(reference));
    let expected = relational_schema_digest(&source).unwrap_err().to_string();
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let error =
        match work.classify(|work| relational_schema_digest_with_work_context(&source, work)) {
            Err(CheckpointOperationError::Operation(error)) => error,
            other => panic!(
                "unsupported default must retain its ordinary diagnostic: {}",
                match other {
                    Err(CheckpointOperationError::Work(error)) => error.to_string(),
                    Ok(digest) => format!("unexpected digest {digest}"),
                    Err(CheckpointOperationError::Operation(_)) => unreachable!(),
                }
            ),
        };
    assert_eq!(error.to_string(), expected);
    assert!(
        matches!(task.reserve_working_memory(1), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == 1)
    );
    assert_eq!(
        relational_schema_digest(&source).unwrap_err().to_string(),
        expected
    );
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_schema_memory_pressure_defers_and_retries_the_same_admitted_task() {
    for value in [
        RelationalValue::Boolean(true),
        RelationalValue::Text("\0界🙂".repeat(17_000)),
        RelationalValue::Bytea(vec![0; 64 * 1024 + 7]),
    ] {
        let source = schema(value);
        let expected = relational_schema_digest(&source).unwrap();
        let ceiling = 128 * 1024;
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let work = CheckpointWorkContext::new(task.clone());
        let normal = RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        );
        let mut critical = normal;
        critical.memory.pressure = hawdb_qos::RuntimeMemoryPressure::Critical;
        assert!(governor.update_resources(critical));
        assert!(matches!(
            work.classify(|work| relational_schema_digest_with_work_context(&source, work)),
            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                RuntimeMemoryError::Pressure
            )))
        ));
        assert_eq!(relational_schema_digest(&source).unwrap(), expected);
        assert_eq!(governor.snapshot().admissions, 1);
        assert!(governor.update_resources(normal));
        assert_eq!(
            relational_schema_digest_with_work_context(&source, &work).unwrap(),
            expected
        );
        assert!(
            matches!(task.reserve_working_memory(ceiling), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        assert_eq!(governor.snapshot().admissions, 1);
        assert_eq!(relational_schema_digest(&source).unwrap(), expected);
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}
