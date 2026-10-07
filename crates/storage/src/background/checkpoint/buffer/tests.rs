// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot,
};

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

#[test]
fn checkpoint_units_memory_buffer_reserves_before_allocation_and_preserves_full_retained_bytes() {
    let governor = governor(4120);
    let permit = governor
        .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(4120))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let expected = vec![0xa5; 4096];
    let mut bytes = CheckpointBytes::new(expected.len(), &work).unwrap();
    bytes.append(&expected, &work).unwrap();
    drop(permit);
    let idle = governor.snapshot();
    assert_eq!(idle.active_cpu_slots, 0);
    assert_eq!(idle.active_background_tasks, 0);
    assert_eq!(idle.admitted_memory_bytes, 4120);
    assert_eq!(&*bytes, expected);
    assert!(matches!(
        CheckpointBytes::new(1, &work),
        Err(CheckpointWorkError::Memory(RuntimeMemoryError::Closed))
    ));
    drop(bytes);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_memory_buffer_growth_admits_both_capacities_and_preserves_source_on_denial() {
    let governor = governor(8240);
    let permit = governor
        .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(8240))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let expected = vec![0x5a; 4096];
    let mut bytes = CheckpointBytes::new(expected.len(), &work).unwrap();
    bytes.append(&expected, &work).unwrap();
    // 4096 + 4097 bytes and two 24-byte ownership leases need one more byte.
    assert!(matches!(
        bytes.append(&[7], &work),
        Err(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded {
                available_bytes: 4120,
                requested_bytes: 4121
            }
        ))
    ));
    assert_eq!(&*bytes, expected);
    drop(bytes);
    let mut retry = CheckpointBytes::new(4097, &work).unwrap();
    retry.append(&expected, &work).unwrap();
    retry.append(&[7], &work).unwrap();
    assert_eq!(&retry[..4096], expected);
    assert_eq!(retry[4096], 7);
    drop(retry);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_memory_buffer_cancels_each_copy_boundary_and_retries_without_capacity_leaks() {
    use crate::background::CheckpointWorkProbe;
    let source = vec![0x3c; 64 * 1024 + 1];
    let ceiling = 2 * source.len() as u64 + 1 + 48;
    let local = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    });
    let measure = governor(ceiling);
    let permit = measure
        .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(
            ceiling,
        ))
        .unwrap();
    let seed = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let mut baseline = CheckpointBytes::new(source.len(), &seed).unwrap();
    baseline.append(&source, &seed).unwrap();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = CheckpointWorkContext::new(
        permit.bind_task_context(probe.context(local.clone()).task.clone()),
    )
    .with_scheduler(local.clone());
    baseline.append(&[7], &work).unwrap();
    let units = probe.completed.load(std::sync::atomic::Ordering::SeqCst);
    assert!(units >= 4);
    probe.assert_released(&local);
    drop(baseline);
    drop(permit);
    for limit in 1..=units {
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(
                ceiling,
            ))
            .unwrap();
        let seed =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let mut bytes = CheckpointBytes::new(source.len(), &seed).unwrap();
        bytes.append(&source, &seed).unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe
            .cancel_after
            .store(limit, std::sync::atomic::Ordering::SeqCst);
        let work = CheckpointWorkContext::new(
            permit.bind_task_context(probe.context(local.clone()).task.clone()),
        )
        .with_scheduler(local.clone());
        assert!(matches!(
            bytes.append(&[7], &work),
            Err(CheckpointWorkError::Stopped(_))
        ));
        assert_eq!(&*bytes, source);
        probe.assert_released(&local);
        bytes.append(&[7], &seed).unwrap();
        assert_eq!(&bytes[..source.len()], source);
        assert_eq!(bytes[source.len()], 7);
        drop(bytes);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn checkpoint_units_memory_buffer_allocator_failure_is_typed_and_never_returns_partial_bytes() {
    assert!(matches!(
        CheckpointBytes::new(usize::MAX, &CheckpointWorkContext::default()),
        Err(CheckpointWorkError::Allocation { .. })
    ));
}

#[test]
fn checkpoint_units_shared_copy_retains_memory_until_the_last_clone_drops() {
    for length in [0usize, 1, 64 * 1024 + 1] {
        let ceiling = length as u64 + 4096;
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(
                ceiling,
            ))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let expected = (0..length)
            .map(|ordinal| (ordinal.wrapping_mul(79) % 256) as u8)
            .collect::<Vec<_>>();
        let output = work.arc_bytes(&expected).unwrap();
        let retained = output.clone();
        assert_eq!(output.as_ref(), expected.as_slice());
        drop(permit);
        let closed = governor.snapshot();
        assert_eq!(closed.active_cpu_slots, 0);
        assert_eq!(closed.active_background_tasks, 0);
        assert_eq!(closed.admitted_memory_bytes, ceiling);
        drop(output);
        assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
        assert_eq!(retained.as_ref(), expected.as_slice());
        assert!(matches!(
            CheckpointBytes::new(0, &work),
            Err(CheckpointWorkError::Memory(RuntimeMemoryError::Closed))
        ));
        drop(work);
        assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
        assert_eq!(retained.as_ref(), expected.as_slice());
        drop(retained);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn checkpoint_units_shared_copy_admits_empty_allocation_ownership_before_copy() {
    let governor = governor(1);
    let permit = governor
        .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    assert!(matches!(
        work.arc_bytes(&[]),
        Err(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        ))
    ));
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_shared_copy_overlap_denied_by_one_byte_preserves_complete_retry() {
    let expected = vec![0xa7; 64 * 1024 + 1];
    let ceiling = 1024 * 1024;
    let measure = governor(ceiling);
    let permit = measure
        .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(
            ceiling,
        ))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let first = work.arc_bytes(&expected).unwrap();
    // Query through the shared controller, within the static task ceiling.
    // A larger request returns that static ceiling before consulting usage.
    let available = match work.task.reserve_working_memory(ceiling) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => available_bytes,
        other => panic!("expected an over-limit reservation: {other:?}"),
    };
    let charged = ceiling - available;
    assert!(charged > expected.len() as u64);
    drop(first);
    drop(permit);
    assert_eq!(measure.snapshot().admitted_memory_bytes, 0);

    let ceiling = 2 * charged - 1;
    let bounded = governor(ceiling);
    let permit = bounded
        .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(
            ceiling,
        ))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let first = work.arc_bytes(&expected).unwrap();
    let retained = first.clone();
    assert!(matches!(
        work.arc_bytes(&expected),
        Err(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded {
                requested_bytes,
                available_bytes,
            }
        )) if requested_bytes == charged && available_bytes == charged - 1
    ));
    assert_eq!(first.as_ref(), expected.as_slice());
    assert_eq!(retained.as_ref(), expected.as_slice());
    drop(first);
    drop(retained);
    let retry = work.arc_bytes(&expected).unwrap();
    assert_eq!(retry.as_ref(), expected.as_slice());
    drop(permit);
    assert_eq!(bounded.snapshot().active_background_tasks, 0);
    assert_eq!(bounded.snapshot().admitted_memory_bytes, ceiling);
    drop(retry);
    assert_eq!(bounded.snapshot().admitted_memory_bytes, 0);
}
