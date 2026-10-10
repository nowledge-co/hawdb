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
use hawdb_core::{RuntimeMemoryController, RuntimeMemoryError};
use std::num::NonZeroU64;

const OWNER_BYTES: u64 = 128;
const GOVERNOR_BYTES: u64 = 4096;

fn governor(policy: Option<ProcessMemoryPolicy>) -> RuntimeGovernor {
    RuntimeGovernor::new_inner(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(GOVERNOR_BYTES),
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
        .try_admit_incremental_maintenance(OWNER_BYTES, 1 << 30, 1, RuntimeTaskContext::default())
        .unwrap()
}

#[test]
fn incremental_allocations_refund_each_unit_without_a_dataset_sized_reservation() {
    let governor = governor(None);
    let work = work(&governor);
    let task = work.task_context().unwrap().clone();
    assert_eq!(governor.snapshot().admitted_memory_bytes, OWNER_BYTES);
    for _ in 0..256 {
        let memory = task.reserve_working_memory(512).unwrap().unwrap();
        let buffer = vec![7u8; 512];
        assert_eq!(memory.bytes(), buffer.len() as u64);
        let running = governor.snapshot();
        assert!(running.admitted_memory_bytes > OWNER_BYTES + 512);
        assert!(running.admitted_memory_bytes <= GOVERNOR_BYTES);
        assert_eq!(running.active_cpu_slots, 1);
        assert_eq!(running.active_background_tasks, 1);
        assert_eq!(running.admissions, 1);
        drop(buffer);
        drop(memory);
        assert_eq!(governor.snapshot().admitted_memory_bytes, OWNER_BYTES);
    }
    drop(work);
    // Closed task clones still own the shared allocation controller.
    assert_eq!(governor.snapshot().admitted_memory_bytes, OWNER_BYTES);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    drop(task);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn incremental_pause_resume_keeps_actual_leases_and_preserves_a_lower_parent_ceiling() {
    let governor = governor(None);
    let mut work = work(&governor);
    let old = work.task_context().unwrap().clone();
    let retained = old.reserve_working_memory(512).unwrap().unwrap();
    let charged = governor.snapshot().admitted_memory_bytes;
    work.pause();
    assert_eq!(governor.snapshot().admitted_memory_bytes, charged);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert!(matches!(
        old.reserve_working_memory(1),
        Err(RuntimeMemoryError::Stopped(_))
    ));
    work.try_resume(
        RuntimeTaskContext::default().with_memory_reservation(RuntimeMemoryReservation::new(64, 0)),
    )
    .unwrap();
    let fresh = work.task_context().unwrap().clone();
    assert!(matches!(
        fresh.reserve_working_memory(1),
        Err(RuntimeMemoryError::AdmissionDenied {
            retryable: true,
            available_bytes: 0,
            ..
        })
    ));
    assert!(matches!(
        fresh.reserve_working_memory(65),
        Err(RuntimeMemoryError::ReservationExceeded { .. })
    ));
    drop(retained);
    let next = fresh.reserve_working_memory(1).unwrap().unwrap();
    drop(next);
    drop(work);
    drop(old);
    drop(fresh);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn incremental_denial_is_permanent_only_when_the_allocation_cannot_fit_capacity() {
    let governor = governor(None);
    let mut first = work(&governor);
    let task = first.task_context().unwrap().clone();
    let allocation = task.reserve_working_memory(512).unwrap().unwrap();
    assert!(matches!(
        task.reserve_working_memory(GOVERNOR_BYTES + 1),
        Err(RuntimeMemoryError::AdmissionDenied {
            retryable: false,
            ..
        })
    ));
    first.pause();
    let mut second = work(&governor);
    let other = second.task_context().unwrap().clone();
    let competing = other.reserve_working_memory(2600).unwrap().unwrap();
    second.pause();
    first.try_resume(RuntimeTaskContext::default()).unwrap();
    let fresh = first.task_context().unwrap().clone();
    let before = governor.snapshot().admitted_memory_bytes;
    assert!(matches!(
        fresh.reserve_working_memory(1024),
        Err(RuntimeMemoryError::AdmissionDenied {
            retryable: true,
            ..
        })
    ));
    assert_eq!(governor.snapshot().admitted_memory_bytes, before);
    drop(competing);
    let recovered = fresh.reserve_working_memory(1024).unwrap().unwrap();
    drop(recovered);
    drop(allocation);
    drop(first);
    drop(second);
    drop(task);
    drop(other);
    drop(fresh);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn incremental_owner_floor_makes_an_unfit_unit_a_permanent_denial() {
    let governor = governor(None);
    let work = work(&governor);
    let task = work.task_context().unwrap().clone();
    let denial = task
        .reserve_working_memory(GOVERNOR_BYTES - OWNER_BYTES)
        .unwrap_err();
    let unchanged = governor.snapshot().admitted_memory_bytes;
    drop(work);
    drop(task);
    assert_eq!(unchanged, OWNER_BYTES);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert!(matches!(
        denial,
        RuntimeMemoryError::AdmissionDenied {
            retryable: false,
            ..
        }
    ));
}

#[test]
fn incremental_pressure_denial_recovers_on_the_same_owner_and_closure_stays_closed() {
    let governor = governor(None);
    let work = work(&governor);
    let task = work.task_context().unwrap().clone();
    let ledger = working_memory::GovernorMemoryBinding(work.memory.clone());
    let retained = task.reserve_working_memory(512).unwrap().unwrap();
    let healthy = governor.snapshot().resources;
    let mut critical = healthy;
    critical.memory.pressure = RuntimeMemoryPressure::Critical;
    governor.update_resources(critical);
    let before = governor.snapshot().admitted_memory_bytes;
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::AdmissionDenied {
            retryable: true,
            ..
        })
    ));
    assert_eq!(governor.snapshot().admitted_memory_bytes, before);
    governor.update_resources(healthy);
    let recovered = task.reserve_working_memory(1).unwrap().unwrap();
    drop(recovered);
    drop(work);
    assert!(matches!(
        ledger.reserve(1, u64::MAX),
        Err(RuntimeMemoryError::Closed)
    ));
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(retained);
    assert_eq!(governor.snapshot().admitted_memory_bytes, OWNER_BYTES);
    drop(task);
    drop(ledger);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn incremental_parallel_allocations_share_one_capacity_and_outlive_execution() {
    let governor = governor(None);
    let work = work(&governor);
    let task = work.task_context().unwrap().clone();
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
                let allocation = task.reserve_working_memory(512);
                if allocation.is_ok() {
                    admitted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                entered.wait();
                released.wait();
                match allocation {
                    Ok(allocation) => drop(allocation),
                    Err(RuntimeMemoryError::AdmissionDenied {
                        retryable: true, ..
                    }) => {}
                    error => panic!("unexpected allocation result: {error:?}"),
                }
            })
        })
        .collect::<Vec<_>>();
    entered.wait();
    let count = admitted.load(std::sync::atomic::Ordering::SeqCst);
    let retained = governor.snapshot();
    drop(work);
    let closed = governor.snapshot();
    released.wait();
    for job in jobs {
        job.join().unwrap();
    }
    let after_allocations = governor.snapshot().admitted_memory_bytes;
    drop(task);
    assert!(
        count > 0 && count < 16,
        "the fixture must reach actual admission pressure"
    );
    assert!(retained.admitted_memory_bytes > OWNER_BYTES);
    assert!(retained.admitted_memory_bytes <= GOVERNOR_BYTES);
    assert_eq!(closed.active_cpu_slots, 0);
    assert_eq!(closed.admitted_memory_bytes, retained.admitted_memory_bytes);
    assert_eq!(after_allocations, OWNER_BYTES);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn incremental_allocations_compete_across_governors_with_one_process_policy() {
    let policy = ProcessMemoryPolicy::new(ProcessMemoryPolicyConfig::new(
        NonZeroU64::new(5000).unwrap(),
    ));
    policy.update(ProcessMemorySnapshot {
        capabilities: ProcessMemoryCapabilities {
            resident_memory: true,
            ..Default::default()
        },
        resident_bytes: 64,
        peak_resident_bytes: 64,
        total_page_faults: None,
        minor_page_faults: None,
        major_page_faults: None,
    });
    let first = governor(Some(policy.clone()));
    let second = governor(Some(policy.clone()));
    let first_work = work(&first);
    let first_task = first_work.task_context().unwrap().clone();
    let retained = first_task.reserve_working_memory(3000).unwrap().unwrap();
    let second_work = work(&second);
    let second_task = second_work.task_context().unwrap().clone();
    let before = policy.snapshot().unobserved_reserved_bytes;
    assert!(matches!(
        second_task.reserve_working_memory(2000),
        Err(RuntimeMemoryError::AdmissionDenied {
            retryable: true,
            ..
        })
    ));
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, before);
    drop(first_work);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, before);
    drop(retained);
    let recovered = second_task.reserve_working_memory(2000).unwrap().unwrap();
    drop(recovered);
    drop(first_task);
    drop(second_work);
    drop(second_task);
    assert_eq!(first.snapshot().admitted_memory_bytes, 0);
    assert_eq!(second.snapshot().admitted_memory_bytes, 0);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
}
