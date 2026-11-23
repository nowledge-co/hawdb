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

#[test]
fn fixed_memory_transfer_releases_the_only_execution_slot_without_an_admission_gap() {
    let governor = governor(None);
    let request =
        RuntimeWorkRequest::new(RuntimeWorkPriority::Foreground, RuntimeWorkKind::Control)
            .with_cpu_slots(1)
            .with_blocking(true)
            .with_io_slots(1)
            .with_memory_bytes(80);
    let permit = governor.try_admit(request).unwrap();
    let retained = permit.into_retained_memory().unwrap();
    let after = governor.snapshot();
    assert_eq!(retained.bytes(), 80);
    assert_eq!(after.admitted_memory_bytes, 80);
    assert_eq!(after.active_cpu_slots, 0);
    assert_eq!(after.active_foreground_tasks, 0);
    assert_eq!(after.active_blocking_tasks, 0);
    assert_eq!(after.active_foreground_io_slots, 0);
    let execution_request = request.with_retained_memory_bytes(retained.bytes());
    let error = governor
        .try_admit(execution_request.with_memory_bytes(21))
        .unwrap_err();
    assert!(!error.is_retryable());
    assert_eq!(error.requested, 101);
    assert_eq!(error.available, 100);
    let execution = governor
        .try_admit(execution_request.with_memory_bytes(20))
        .unwrap();
    assert!(retained.shares_governor_with(&execution));
    assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    drop(retained);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 20);
    drop(execution);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn transferred_memory_keeps_the_process_reservation_and_rejects_impossible_execution() {
    let policy =
        ProcessMemoryPolicy::new(ProcessMemoryPolicyConfig::new(NonZeroU64::new(90).unwrap()));
    policy.update(ProcessMemorySnapshot {
        capabilities: ProcessMemoryCapabilities {
            resident_memory: true,
            ..Default::default()
        },
        resident_bytes: 0,
        peak_resident_bytes: 0,
        total_page_faults: None,
        minor_page_faults: None,
        major_page_faults: None,
    });
    let governor = governor(Some(policy.clone()));
    let request = RuntimeWorkRequest::foreground_query(80, 0);
    let retained = governor
        .try_admit(request)
        .unwrap()
        .into_retained_memory()
        .unwrap();
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 80);
    let execution_request = request.with_retained_memory_bytes(retained.bytes());
    let error = governor
        .try_admit(execution_request.with_memory_bytes(11))
        .unwrap_err();
    assert!(!error.is_retryable());
    assert_eq!(error.requested, 91);
    assert_eq!(error.available, 90);
    let execution = governor
        .try_admit(execution_request.with_memory_bytes(10))
        .unwrap();
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 90);
    drop(execution);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 80);
    drop(retained);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn retained_floor_is_rechecked_after_a_capacity_change() {
    let governor = governor(None);
    let retained = governor
        .try_admit(RuntimeWorkRequest::foreground_query(60, 0))
        .unwrap()
        .into_retained_memory()
        .unwrap();
    let request =
        RuntimeWorkRequest::foreground_query(40, 0).with_retained_memory_bytes(retained.bytes());
    drop(governor.try_admit(request).unwrap());
    mutex_lock(&governor.inner.state)
        .limits
        .memory_capacity_bytes = 99;
    assert!(!governor.try_admit(request).unwrap_err().is_retryable());
    drop(retained);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn retained_floor_cannot_overflow_the_hard_capacity_check() {
    let request = RuntimeWorkRequest::foreground_query(u64::MAX, 0).with_retained_memory_bytes(1);
    assert!(request.exceeds_memory_capacity(u64::MAX));
    assert!(!request
        .with_memory_bytes(u64::MAX - 1)
        .exceeds_memory_capacity(u64::MAX));
}

#[test]
fn elastic_memory_transfer_is_rejected_without_leaking_resources() {
    let governor = governor(None);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(80))
        .unwrap();
    assert!(permit.into_retained_memory().is_none());
    let after = governor.snapshot();
    assert_eq!(after.admitted_memory_bytes, 0);
    assert_eq!(after.active_cpu_slots, 0);
    assert_eq!(after.active_background_tasks, 0);
}

#[test]
fn retained_memory_preserves_shared_work_until_the_last_active_owner() {
    let governor = governor(None);
    let request = RuntimeWorkRequest::background_maintenance(60)
        .with_blocking(true)
        .with_io_slots(1);
    let permit = Arc::new(governor.try_admit(request).unwrap());
    let other = permit.clone();
    let retained = permit.reserve_retained_memory(20).unwrap();
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 1);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 80);
    assert!(governor.try_admit(request).is_err());
    drop(other);
    let idle = governor.snapshot();
    assert_eq!(idle.admitted_memory_bytes, retained.bytes());
    assert_eq!(idle.active_background_tasks, 0);
    assert_eq!(idle.active_blocking_tasks, 0);
    assert_eq!(idle.active_cpu_slots, 0);
    assert_eq!(idle.active_background_io_slots, 0);
    let next = governor.try_admit(request).unwrap();
    drop(retained);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 60);
    drop(next);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn retained_memory_admits_exact_remaining_bytes_and_rejects_one_more() {
    let governor = governor(None);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(90))
        .unwrap();
    let error = permit.reserve_retained_memory(11).unwrap_err();
    assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
    assert_eq!(error.available, 10);
    assert!(error.is_retryable());
    assert!(!permit
        .reserve_retained_memory(101)
        .unwrap_err()
        .is_retryable());
    assert_eq!(governor.snapshot().admitted_memory_bytes, 90);
    let retained = permit.reserve_retained_memory(10).unwrap();
    assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    assert!(permit.reserve_retained_memory(1).is_err());
    drop(retained);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 90);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn retained_memory_honors_shared_process_policy_and_missing_samples() {
    let policy = ProcessMemoryPolicy::new(ProcessMemoryPolicyConfig::new(
        NonZeroU64::new(100).unwrap(),
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
    let request = RuntimeWorkRequest::background_maintenance(60);
    let permit = first.try_admit(request).unwrap();
    assert_eq!(
        permit.reserve_retained_memory(21).unwrap_err().code,
        RuntimeAdmissionCode::MemorySaturated
    );
    let retained = permit.reserve_retained_memory(20).unwrap();
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 80);
    drop(permit);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 20);
    let next = second.try_admit(request).unwrap();
    policy.clear_sample();
    assert_eq!(
        next.reserve_retained_memory(1).unwrap_err().code,
        RuntimeAdmissionCode::MemoryPressure
    );
    assert_eq!(second.snapshot().admitted_memory_bytes, 60);
    drop(next);
    drop(retained);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
    assert_eq!(first.snapshot().admitted_memory_bytes, 0);
    assert_eq!(second.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn retained_memory_respects_critical_background_pressure() {
    let governor = governor(None);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(60))
        .unwrap();
    mutex_lock(&governor.inner.state).resources.memory.pressure = RuntimeMemoryPressure::Critical;
    assert_eq!(
        permit.reserve_retained_memory(1).unwrap_err().code,
        RuntimeAdmissionCode::MemoryPressure
    );
    assert_eq!(governor.snapshot().admitted_memory_bytes, 60);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn concurrent_retained_admission_cannot_overbook_the_governor() {
    let governor = governor(None);
    let permit = Arc::new(
        governor
            .try_admit(RuntimeWorkRequest::background_maintenance(20))
            .unwrap(),
    );
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let workers = (0..2)
        .map(|_| {
            let permit = permit.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                permit.reserve_retained_memory(60)
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 80);
    drop(results);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn resident_charge_never_refuses_and_holds_admission_until_released() {
    // Already-resident memory is charged without a process-memory
    // reservation: the resident sample already includes it.
    let policy = ProcessMemoryPolicy::new(ProcessMemoryPolicyConfig::new(
        NonZeroU64::new(1 << 20).unwrap(),
    ));
    policy.update(ProcessMemorySnapshot {
        capabilities: ProcessMemoryCapabilities {
            resident_memory: true,
            ..Default::default()
        },
        resident_bytes: 0,
        peak_resident_bytes: 0,
        total_page_faults: None,
        minor_page_faults: None,
        major_page_faults: None,
    });
    let governor = governor(Some(policy.clone()));
    let permit = governor
        .try_admit(RuntimeWorkRequest::new(
            RuntimeWorkPriority::Foreground,
            RuntimeWorkKind::Control,
        ))
        .unwrap();
    let mut owner = permit.resident_memory_owner();
    assert_eq!(owner.bytes(), 0);
    assert!(owner.shares_governor_with(&permit));
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    owner.charge_resident(150);
    assert_eq!(owner.bytes(), 150);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 150);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
    let refused = permit.reserve_retained_memory(1).unwrap_err();
    assert_eq!(refused.code, RuntimeAdmissionCode::MemorySaturated);
    drop(owner);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
