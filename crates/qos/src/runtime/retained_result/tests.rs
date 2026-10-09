// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    ProcessMemoryCapabilities, ProcessMemoryPolicyConfig, ProcessMemorySnapshot,
    RuntimeMemorySnapshot, RuntimeResourceBudget,
};
use std::num::NonZeroU64;

fn governor(
    bytes: u64,
    handles: usize,
    memory: u64,
    policy: Option<ProcessMemoryPolicy>,
) -> RuntimeGovernor {
    RuntimeGovernor::new_inner(
        RuntimeGovernorConfig {
            result_budget_bytes: bytes,
            retained_result_handle_limit: NonZeroUsize::new(handles).unwrap(),
            memory_budget_bytes: Some(memory),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
        policy,
    )
}

fn request(bytes: u64) -> RuntimeWorkRequest {
    RuntimeWorkRequest::foreground_query(0, bytes)
}

#[test]
fn shared_payload_survives_permit_close_without_work_slots_or_double_charge() {
    let governor = governor(4096, 4, 64 * 1024, None);
    let permit = governor.try_admit(request(4096)).unwrap();
    let first = permit.reserve_retained_result(512, 32).unwrap();
    let first_bytes = governor.retained_result_snapshot().retained_bytes;
    let second = first.try_retain(16).unwrap();
    assert_eq!(second.capacity_bytes(), 512);
    assert_eq!(
        governor.retained_result_snapshot().retained_bytes,
        first_bytes + second.handle_bytes()
    );
    drop(permit);
    let idle = governor.snapshot();
    assert_eq!(idle.active_cpu_slots, 0);
    assert_eq!(idle.active_foreground_tasks, 0);
    assert_eq!(
        idle.admitted_memory_bytes,
        governor.retained_result_snapshot().retained_bytes
    );
    drop(first);
    let retained = governor.retained_result_snapshot();
    assert_eq!(retained.buffer_owners, 1);
    assert_eq!(retained.view_handles, 1);
    let third = second.try_retain(0).unwrap();
    drop(second);
    assert_eq!(governor.retained_result_snapshot().buffer_owners, 1);
    drop(third);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    assert_eq!(governor.retained_result_snapshot().buffer_owners, 0);
    assert_eq!(governor.retained_result_snapshot().view_handles, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn multiple_permits_share_handle_capacity_and_release_allows_retry() {
    let governor = governor(4096, 2, 64 * 1024, None);
    let first_permit = governor.try_admit(request(4096)).unwrap();
    let second_permit = governor.clone().try_admit(request(4096)).unwrap();
    let first = first_permit.reserve_retained_result(128, 0).unwrap();
    let second = second_permit.reserve_retained_result(128, 0).unwrap();
    let before = governor.retained_result_snapshot();
    let error = first.try_retain(0).unwrap_err();
    assert!(matches!(
        error,
        RuntimeRetainedResultError::Capacity {
            resource: RuntimeRetainedResultResource::Handles,
            retryable: true,
            ..
        }
    ));
    let after = governor.retained_result_snapshot();
    assert_eq!(after.retained_bytes, before.retained_bytes);
    assert_eq!(after.buffer_owners, 2);
    assert_eq!(after.view_handles, 2);
    assert_eq!(after.backpressure_events, before.backpressure_events + 1);
    drop(second);
    let retry = first.try_retain(0).unwrap();
    assert_eq!(governor.retained_result_snapshot().buffer_owners, 1);
    assert_eq!(governor.retained_result_snapshot().view_handles, 2);
    drop(retry);
}

#[test]
fn byte_capacity_is_aggregate_and_includes_native_owner_and_handles() {
    let bytes = 512
        + RuntimeRetainedResult::owner_overhead_bytes()
        + RuntimeRetainedResult::handle_overhead_bytes();
    let governor = governor(bytes, 8, 64 * 1024, None);
    let permit = governor.try_admit(request(bytes)).unwrap();
    let full = permit.reserve_retained_result(512, 0).unwrap();
    assert_eq!(governor.retained_result_snapshot().retained_bytes, bytes);
    assert!(matches!(
        full.try_retain(0),
        Err(RuntimeRetainedResultError::Capacity {
            resource: RuntimeRetainedResultResource::Bytes,
            available: 0,
            retryable: true,
            ..
        })
    ));
    assert!(permit.reserve_retained_result(0, 0).is_err());
    drop(full);
    let retry = permit.reserve_retained_result(512, 0).unwrap();
    assert_eq!(governor.retained_result_snapshot().view_handles, 1);
    drop(retry);
}

#[test]
fn oversized_and_overflowing_requests_never_admit_or_change_existing_owners() {
    let governor = governor(4096, 8, 64 * 1024, None);
    let permit = governor.try_admit(request(1024)).unwrap();
    let before = governor.retained_result_snapshot();
    let error = permit.reserve_retained_result(1024, 0).unwrap_err();
    assert!(matches!(
        error,
        RuntimeRetainedResultError::Capacity {
            resource: RuntimeRetainedResultResource::QueryBytes,
            retryable: false,
            ..
        }
    ));
    assert_eq!(
        permit.reserve_retained_result(u64::MAX, 0).unwrap_err(),
        RuntimeRetainedResultError::SizeOverflow
    );
    assert_eq!(
        permit.reserve_retained_result(0, u64::MAX).unwrap_err(),
        RuntimeRetainedResultError::SizeOverflow
    );
    assert_eq!(governor.retained_result_snapshot(), before);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 1024);
}

#[test]
fn failed_handle_memory_admission_rolls_back_buffer_memory_without_peak_growth() {
    let owner_bytes = 512 + RuntimeRetainedResult::owner_overhead_bytes();
    let governor = governor(4096, 8, 4096 + owner_bytes, None);
    let permit = governor.try_admit(request(4096)).unwrap();
    let before = governor.retained_result_snapshot();
    assert!(matches!(
        permit.reserve_retained_result(512, 0),
        Err(RuntimeRetainedResultError::Admission(_))
    ));
    assert_eq!(governor.retained_result_snapshot(), before);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 4096);
}

#[test]
fn handle_retain_after_close_still_honors_the_process_policy() {
    let policy = ProcessMemoryPolicy::new(ProcessMemoryPolicyConfig::new(
        NonZeroU64::new(64 * 1024).unwrap(),
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
    let governor = governor(4096, 8, 64 * 1024, Some(policy.clone()));
    let permit = governor.try_admit(request(4096)).unwrap();
    let first = permit.reserve_retained_result(512, 0).unwrap();
    drop(permit);
    let before = governor.retained_result_snapshot();
    assert_eq!(
        policy.snapshot().unobserved_reserved_bytes,
        before.retained_bytes
    );
    policy.clear_sample();
    assert!(matches!(
        first.try_retain(0),
        Err(RuntimeRetainedResultError::Admission(
            RuntimeAdmissionError {
                code: RuntimeAdmissionCode::MemoryPressure,
                ..
            }
        ))
    ));
    assert_eq!(governor.retained_result_snapshot(), before);
    drop(first);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
}

#[test]
fn concurrent_retains_cannot_multiply_the_shared_handle_allowance() {
    let governor = governor(64 * 1024, 4, 256 * 1024, None);
    let permit = governor.try_admit(request(64 * 1024)).unwrap();
    let first = permit.reserve_retained_result(512, 0).unwrap();
    drop(permit);
    let barrier = std::sync::Barrier::new(17);
    let successful = std::sync::atomic::AtomicUsize::new(0);
    let (successes, live) = std::thread::scope(|scope| {
        for _ in 0..16 {
            scope.spawn(|| {
                barrier.wait();
                let view = first.try_retain(0);
                if view.is_ok() {
                    successful.fetch_add(1, Ordering::Relaxed);
                }
                barrier.wait();
                barrier.wait();
                drop(view);
            });
        }
        barrier.wait();
        barrier.wait();
        let successes = successful.load(Ordering::Relaxed);
        let live = governor.retained_result_snapshot();
        barrier.wait();
        (successes, live)
    });
    assert_eq!(successes, 3);
    assert_eq!(live.view_handles, 4);
    assert_eq!(live.buffer_owners, 1);
    assert_eq!(live.peak_view_handles, 4);
    assert_eq!(governor.retained_result_snapshot().view_handles, 1);
    drop(first);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
}
