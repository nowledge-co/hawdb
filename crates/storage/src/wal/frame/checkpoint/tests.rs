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

#[test]
fn checkpoint_units_wal_framing_complete_bytes_match_every_block_position_and_fragment_boundary() {
    let local = scheduler();
    let work = CheckpointWorkContext::default().with_scheduler(local.clone());
    let small = [0, 1, 255, 3, 0, 5, 9];
    for position in 0..WAL_BLOCK_BYTES as u64 {
        assert_eq!(
            &*frame_binary_wal_record_with_work_context(17, &small, position, &work).unwrap(),
            frame_binary_wal_record(17, &small, position)
        );
    }
    for length in [
        0,
        1,
        14,
        15,
        32_752,
        32_753,
        32_754,
        65_536,
        5 * WAL_BLOCK_BYTES + 7,
    ] {
        let payload: Vec<_> = (0..length).map(|i| ((i * 71) % 256) as u8).collect();
        for position in [0, 1, 32_752, 32_753, 32_754, 32_767, u64::MAX] {
            for generation in [0, 1, u64::MAX] {
                assert_eq!(
                    &*frame_binary_wal_record_with_work_context(
                        generation, &payload, position, &work
                    )
                    .unwrap(),
                    frame_binary_wal_record(generation, &payload, position)
                );
            }
        }
    }
    assert_eq!(local.snapshot().running_background_operations, 0);
}

#[test]
fn checkpoint_units_wal_framing_admits_exact_capacity_and_retains_output_after_execution_closes() {
    let payload: Vec<_> = (0..3 * 64 * 1024 + 7)
        .map(|i| ((i * 17) % 256) as u8)
        .collect();
    for position in [0, 32_753, 32_754, 32_767] {
        let expected = frame_binary_wal_record(23, &payload, position);
        let requested = expected.len() as u64 + 24;
        let denied = governor(requested - 1);
        let permit = denied
            .try_admit(RuntimeWorkRequest::background_maintenance(requested - 1))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        assert!(matches!(
            frame_binary_wal_record_with_work_context(23, &payload, position, &work),
            Err(CheckpointWorkError::Memory(RuntimeMemoryError::ReservationExceeded { requested_bytes, available_bytes }))
                if requested_bytes == requested && available_bytes == requested - 1
        ));
        drop(work);
        drop(permit);
        assert_eq!(denied.snapshot().admitted_memory_bytes, 0);

        let allowed = governor(requested);
        let permit = allowed
            .try_admit(RuntimeWorkRequest::background_maintenance(requested))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let work = CheckpointWorkContext::new(task.clone());
        let output =
            frame_binary_wal_record_with_work_context(23, &payload, position, &work).unwrap();
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
        let closed = allowed.snapshot();
        assert_eq!(closed.active_background_tasks, 0);
        assert_eq!(closed.active_cpu_slots, 0);
        assert_eq!(closed.admitted_memory_bytes, requested);
        assert_eq!(&*output, expected);
        drop(output);
        assert_eq!(allowed.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn checkpoint_units_wal_framing_cancels_every_actual_unit_and_fully_retries_same_reservation() {
    let payload: Vec<_> = (0..3 * 64 * 1024 + 7)
        .map(|i| ((i * 71) % 256) as u8)
        .collect();
    let expected = frame_binary_wal_record(31, &payload, 32_767);
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        &*frame_binary_wal_record_with_work_context(
            31,
            &payload,
            32_767,
            &baseline.context(local.clone())
        )
        .unwrap(),
        expected
    );
    let units = baseline.completed.load(Ordering::SeqCst);
    assert!(units > 15);
    baseline.assert_released(&local);
    for stop in 0..=units {
        let ceiling = 512 * 1024;
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
        let result = frame_binary_wal_record_with_work_context(31, &payload, 32_767, &work);
        assert!(
            matches!(result, Err(CheckpointWorkError::Admission(_)) if stop == 0)
                || matches!(result, Err(CheckpointWorkError::Stopped(_)) if stop > 0)
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
        let output =
            frame_binary_wal_record_with_work_context(31, &payload, 32_767, &work).unwrap();
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
