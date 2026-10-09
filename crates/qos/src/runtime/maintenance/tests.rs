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
use hawdb_core::RuntimeMemoryError;
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

fn work(governor: &RuntimeGovernor) -> RuntimeMaintenanceWork {
    governor
        .try_admit_resumable_maintenance(100, 1, RuntimeTaskContext::default())
        .unwrap()
}

fn idle(governor: &RuntimeGovernor) {
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.active_cpu_slots, 0);
    assert_eq!(snapshot.active_background_tasks, 0);
    assert_eq!(snapshot.active_blocking_tasks, 0);
    assert_eq!(snapshot.active_background_io_slots, 0);
}

#[test]
fn pause_keeps_original_memory_and_fresh_execution_shares_live_allocation_capacity() {
    let governor = governor(None);
    let parent = RuntimeTaskContext::default();
    let mut work = governor
        .try_admit_resumable_maintenance(100, 1, parent.clone())
        .unwrap();
    let old = work.task_context().unwrap().clone();
    let first = old.reserve_working_memory(40).unwrap().unwrap();
    work.pause();
    work.pause();
    idle(&governor);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    assert!(work.task_context().is_none());
    assert!(old.checkpoint().is_err());
    assert!(parent.checkpoint().is_ok());
    work.try_resume(parent.clone()).unwrap();
    let fresh = work.task_context().unwrap().clone();
    assert!(fresh.checkpoint().is_ok());
    assert!(old.checkpoint().is_err());
    let second = fresh.child().reserve_working_memory(12).unwrap().unwrap();
    assert!(matches!(
        fresh.reserve_working_memory(1),
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes: 0,
            ..
        })
    ));
    assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    assert_eq!(governor.snapshot().active_cpu_slots, 1);
    assert_eq!(governor.snapshot().active_background_tasks, 1);
    // A repeated resume neither re-admits nor revives the previous context.
    work.try_resume(parent).unwrap();
    assert_eq!(governor.snapshot().admissions, 2);
    drop(work);
    idle(&governor);
    assert!(fresh.checkpoint().is_err());
    assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    drop(first);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    drop(second);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().completions, 2);
}

#[test]
fn paused_candidate_keeps_conservative_memory_even_without_tracked_allocations() {
    let governor = governor(None);
    let mut work = work(&governor);
    work.pause();
    idle(&governor);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    assert_eq!(
        governor
            .try_admit(RuntimeWorkRequest::foreground_query(1, 0))
            .unwrap_err()
            .code,
        RuntimeAdmissionCode::MemorySaturated
    );
    drop(work);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn occupied_execution_slot_denies_resume_without_losing_retained_candidate_memory() {
    let governor = governor(None);
    let mut work = work(&governor);
    work.pause();
    let other = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(0))
        .unwrap();
    let error = work.try_resume(RuntimeTaskContext::default()).unwrap_err();
    assert_eq!(error.code, RuntimeAdmissionCode::BackgroundTaskSaturated);
    assert!(error.is_retryable());
    assert!(work.task_context().is_none());
    assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    assert_eq!(governor.snapshot().active_background_tasks, 1);
    drop(other);
    work.try_resume(RuntimeTaskContext::default()).unwrap();
    assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    drop(work);
    idle(&governor);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn in_flight_wave_stays_charged_after_execution_parks_and_stale_context_cannot_acquire() {
    let governor = governor(None);
    let mut work = work(&governor);
    let old = work.task_context().unwrap().clone();
    let wave = old.acquire_io_wave(NonZeroUsize::MIN).unwrap().unwrap();
    work.pause();
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 1);
    assert!(matches!(
        old.try_acquire_io_wave(NonZeroUsize::MIN),
        Err(RuntimeIoWaveError::Stopped(_))
    ));
    work.try_resume(RuntimeTaskContext::default()).unwrap();
    let fresh = work.task_context().unwrap();
    assert!(matches!(
        fresh.try_acquire_io_wave(NonZeroUsize::MIN).unwrap(),
        hawdb_core::RuntimeIoWaveTryAcquire::Pending
    ));
    drop(wave);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    let fresh_wave = fresh.acquire_io_wave(NonZeroUsize::MIN).unwrap().unwrap();
    drop(work);
    assert_eq!(governor.snapshot().active_background_io_slots, 1);
    drop(fresh_wave);
    idle(&governor);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn pressure_denial_preserves_original_reservation_across_repeated_execution_epochs() {
    let governor = governor(None);
    let mut work = work(&governor);
    let allocation = work
        .task_context()
        .unwrap()
        .reserve_working_memory(40)
        .unwrap()
        .unwrap();
    for _ in 0..3 {
        let old = work.task_context().unwrap().clone();
        work.pause();
        let mut resources = governor.snapshot().resources;
        resources.memory.pressure = RuntimeMemoryPressure::Critical;
        governor.update_resources(resources);
        let error = work.try_resume(RuntimeTaskContext::default()).unwrap_err();
        assert_eq!(error.code, RuntimeAdmissionCode::MemoryPressure);
        assert!(error.is_retryable());
        assert!(work.task_context().is_none());
        idle(&governor);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
        resources.memory.pressure = RuntimeMemoryPressure::Normal;
        governor.update_resources(resources);
        work.try_resume(RuntimeTaskContext::default()).unwrap();
        assert!(old.checkpoint().is_err());
        assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    }
    drop(allocation);
    drop(work);
    idle(&governor);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().admissions, 4);
    assert_eq!(governor.snapshot().completions, 4);
}

#[test]
fn resumed_context_preserves_parent_cancellation_deadline_and_lower_memory_ceiling() {
    let governor = governor(None);
    let mut work = work(&governor);
    work.pause();
    let parent = RuntimeTaskContext::with_timeout(Duration::from_secs(1))
        .with_memory_reservation(RuntimeMemoryReservation::new(64, 0));
    work.try_resume(parent.clone()).unwrap();
    let task = work.task_context().unwrap().clone();
    assert_eq!(task.deadline(), parent.deadline());
    assert!(matches!(
        task.reserve_working_memory(65),
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes: 64,
            ..
        })
    ));
    parent.cancellation().cancel();
    assert!(task.checkpoint().is_err());
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::Stopped(_))
    ));
    drop(work);
    idle(&governor);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn shared_process_memory_policy_is_charged_once_and_refunded_after_last_allocation() {
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
    let mut work = work(&first);
    let allocation = work
        .task_context()
        .unwrap()
        .reserve_working_memory(40)
        .unwrap()
        .unwrap();
    work.pause();
    idle(&first);
    assert_eq!(
        second
            .try_admit(RuntimeWorkRequest::background_maintenance(1))
            .unwrap_err()
            .code,
        RuntimeAdmissionCode::MemorySaturated
    );
    work.try_resume(RuntimeTaskContext::default()).unwrap();
    assert_eq!(first.snapshot().admitted_memory_bytes, 100);
    drop(work);
    idle(&first);
    assert_eq!(
        second
            .try_admit(RuntimeWorkRequest::background_maintenance(1))
            .unwrap_err()
            .code,
        RuntimeAdmissionCode::MemorySaturated
    );
    drop(allocation);
    drop(
        second
            .try_admit(RuntimeWorkRequest::background_maintenance(100))
            .unwrap(),
    );
    assert_eq!(first.snapshot().admitted_memory_bytes, 0);
    assert_eq!(second.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn resume_fails_closed_when_host_rss_sample_disappears_despite_no_new_memory_charge() {
    let policy = ProcessMemoryPolicy::new(ProcessMemoryPolicyConfig::new(
        NonZeroU64::new(120).unwrap(),
    ));
    let sample = ProcessMemorySnapshot {
        capabilities: ProcessMemoryCapabilities {
            resident_memory: true,
            ..Default::default()
        },
        resident_bytes: 20,
        peak_resident_bytes: 20,
        total_page_faults: None,
        minor_page_faults: None,
        major_page_faults: None,
    };
    policy.update(sample);
    let governor = governor(Some(policy.clone()));
    let mut work = work(&governor);
    work.pause();
    policy.clear_sample();
    let error = work.try_resume(RuntimeTaskContext::default()).unwrap_err();
    assert_eq!(error.code, RuntimeAdmissionCode::MemoryPressure);
    assert!(error.is_retryable());
    assert!(work.task_context().is_none());
    idle(&governor);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    policy.update(sample);
    let full = work.try_resume(RuntimeTaskContext::default()).unwrap_err();
    assert_eq!(full.code, RuntimeAdmissionCode::MemorySaturated);
    assert!(full.is_retryable());
    assert!(work.task_context().is_none());
    idle(&governor);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 100);
    // A fresh sample at the same full limit is not pressure recovery. The
    // host must actually observe headroom before execution can be re-admitted.
    policy.update(ProcessMemorySnapshot {
        resident_bytes: 0,
        ..sample
    });
    work.try_resume(RuntimeTaskContext::default()).unwrap();
    assert_eq!(governor.snapshot().admitted_memory_bytes, 100);
    drop(work);
    idle(&governor);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
