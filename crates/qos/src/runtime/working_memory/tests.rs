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
use crate::{
    ProcessMemoryCapabilities, ProcessMemoryPolicyConfig, ProcessMemorySnapshot,
    RuntimeMemorySnapshot, RuntimeResourceBudget,
};
use std::num::NonZeroU64;

fn governor(policy: Option<ProcessMemoryPolicy>) -> RuntimeGovernor {
    RuntimeGovernor::new_inner(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(100),
            background_task_limit: NonZeroUsize::new(1),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
        policy,
    )
}

fn request() -> RuntimeWorkRequest {
    RuntimeWorkRequest::background_maintenance(100).with_io_slots(1)
}

#[test]
fn working_memory_children_share_actual_capacity_and_refund_denial_without_another_admission() {
    let governor = governor(None);
    let permit = governor.try_admit(request()).unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let other = task.child();
    let first = task.reserve_working_memory(40).unwrap().unwrap();
    let second = other.reserve_working_memory(12).unwrap().unwrap();
    assert_eq!(first.bytes(), 40);
    assert_eq!(second.bytes(), 12);
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes: 0,
            ..
        })
    ));
    assert!(matches!(
        other.reserve_working_memory(u64::MAX),
        Err(RuntimeMemoryError::ReservationExceeded { .. })
    ));
    assert_eq!(governor.snapshot().admissions, 1);
    drop(first);
    let retry = other.reserve_working_memory(40).unwrap().unwrap();
    drop(second);
    drop(retry);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn working_memory_lower_task_ceiling_does_not_reset_shared_capacity() {
    let governor = governor(None);
    let permit = governor.try_admit(request()).unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let bounded = task
        .child()
        .with_memory_reservation(RuntimeMemoryReservation::new(64, 0));
    let first = task.reserve_working_memory(40).unwrap().unwrap();
    assert!(matches!(
        bounded.reserve_working_memory(1),
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes: 0,
            ..
        })
    ));
    drop(first);
    let second = bounded.reserve_working_memory(40).unwrap().unwrap();
    assert!(matches!(
        bounded.reserve_working_memory(1),
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes: 0,
            ..
        })
    ));
    drop(second);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn working_memory_closure_releases_execution_but_retains_allocations_until_final_drop() {
    let governor = governor(None);
    let permit = governor.try_admit(request()).unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let allocation = task.reserve_working_memory(40).unwrap().unwrap();
    drop(permit);
    let idle = governor.snapshot();
    assert_eq!(idle.active_background_tasks, 0);
    assert_eq!(idle.active_cpu_slots, 0);
    assert_eq!(idle.active_blocking_tasks, 0);
    assert_eq!(idle.active_background_io_slots, 0);
    assert_eq!(idle.admitted_memory_bytes, 100);
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::Closed)
    ));
    assert_eq!(
        governor.try_admit(request()).unwrap_err().code,
        RuntimeAdmissionCode::MemorySaturated
    );
    drop(allocation);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::Closed)
    ));
    drop(governor.try_admit(request()).unwrap());
    assert_eq!(governor.snapshot().completions, 2);
}

#[test]
fn working_memory_parallel_children_never_overgrant_and_survive_owner_closure() {
    let governor = governor(None);
    let permit = governor.try_admit(request()).unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let entered = Arc::new(std::sync::Barrier::new(17));
    let released = Arc::new(std::sync::Barrier::new(17));
    let admitted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let jobs = (0..16)
        .map(|_| {
            let task = task.child();
            let entered = entered.clone();
            let released = released.clone();
            let admitted = admitted.clone();
            std::thread::spawn(move || {
                let allocation = task.reserve_working_memory(1);
                if allocation.is_ok() {
                    admitted.fetch_add(1, Ordering::SeqCst);
                }
                entered.wait();
                released.wait();
                match allocation {
                    Ok(allocation) => drop(allocation),
                    Err(RuntimeMemoryError::ReservationExceeded { .. }) => {}
                    error => panic!("unexpected allocation result: {error:?}"),
                }
            })
        })
        .collect::<Vec<_>>();
    entered.wait();
    let allocation_count = admitted.load(Ordering::SeqCst);
    drop(permit);
    let retained = governor.snapshot();
    released.wait();
    for job in jobs {
        job.join().unwrap();
    }
    assert_eq!(allocation_count, 4);
    assert_eq!(retained.active_cpu_slots, 0);
    assert_eq!(retained.admitted_memory_bytes, 100);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().admissions, 1);
}

#[test]
fn working_memory_cancellation_precedes_allocation_and_refunds_all_resources() {
    let governor = governor(None);
    let permit = governor.try_admit(request()).unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    task.cancellation().cancel();
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::Stopped(_))
    ));
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn working_memory_critical_pressure_defers_new_allocation_and_recovery_reuses_same_reservation() {
    let governor = governor(None);
    let permit = governor.try_admit(request()).unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    mutex_lock(&governor.inner.state).resources.memory.pressure = RuntimeMemoryPressure::Critical;
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::Pressure)
    ));
    assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    mutex_lock(&governor.inner.state).resources.memory.pressure = RuntimeMemoryPressure::Normal;
    let allocation = task.reserve_working_memory(40).unwrap().unwrap();
    assert_eq!(governor.snapshot().admissions, 1);
    drop(allocation);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn working_memory_retention_keeps_shared_process_policy_charged_and_releases_last_owner() {
    let policy = ProcessMemoryPolicy::new(ProcessMemoryPolicyConfig::new(
        NonZeroU64::new(120).unwrap(),
    ));
    policy.update(ProcessMemorySnapshot {
        capabilities: ProcessMemoryCapabilities {
            resident_memory: true,
            ..Default::default()
        },
        resident_bytes: 20,
        peak_resident_bytes: 20,
        total_page_faults: None,
        minor_page_faults: None,
        major_page_faults: None,
    });
    let first = governor(Some(policy.clone()));
    let second = governor(Some(policy.clone()));
    let permit = first.try_admit(request()).unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let allocation = task.reserve_working_memory(40).unwrap().unwrap();
    drop(permit);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 100);
    assert!(second
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .is_err());
    drop(allocation);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
    drop(second.try_admit(request()).unwrap());
    assert_eq!(first.snapshot().admitted_memory_bytes, 0);
    assert_eq!(second.snapshot().admitted_memory_bytes, 0);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
}
