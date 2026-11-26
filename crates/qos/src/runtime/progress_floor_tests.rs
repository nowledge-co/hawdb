// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! An idle governor admits one Foreground request above the dynamic memory
//! budget, up to capacity, so a quiet store makes progress on a busy host
//! (#2003).

use super::*;
use crate::{RuntimeMemorySnapshot, RuntimeResourceBudget};

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// The typed read reservation with defaults and Nowledge Mem's result limit:
/// 256 MiB working memory plus a 64 MiB result ceiling.
const TYPED_READ: RuntimeWorkRequest =
    RuntimeWorkRequest::foreground_query(256 * MIB, 64 * MIB).with_io_slots(1);

fn resources(available: u64) -> RuntimeResourceSnapshot {
    RuntimeResourceSnapshot::from_parts(
        RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
        RuntimeMemorySnapshot::from_limits(Some(8 * GIB), Some(available), None, None, None),
    )
}

/// A pinned 8 GiB host with `available` free: capacity is 2 GiB and the
/// dynamic budget is a quarter of `available` while nothing is admitted.
fn governor(available: u64) -> RuntimeGovernor {
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            result_budget_bytes: 64 * MIB,
            ..RuntimeGovernorConfig::shared_host()
        },
        resources(available),
        IoConcurrencyBudget::new(4, 1),
    );
    governor.pin_resources();
    governor
}

fn assert_retryable_memory_saturated(error: RuntimeAdmissionError) {
    assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated, "{error}");
    assert!(error.is_retryable(), "{error}");
}

#[test]
fn idle_governor_admits_one_foreground_request_above_the_dynamic_budget() {
    let governor = governor(GIB);
    let limits = governor.snapshot().limits;
    assert_eq!(limits.memory_capacity_bytes, 2 * GIB);
    assert_eq!(limits.memory_budget_bytes, 256 * MIB);

    let first = governor.try_admit(TYPED_READ).unwrap();
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.admitted_memory_bytes, 320 * MIB);
    assert_eq!(snapshot.active_foreground_tasks, 1);
    // The floor permit is honestly reported above the dynamic budget.
    assert!(snapshot.overcommitted);

    // A second concurrent request, however small, sees the exhausted budget.
    assert_retryable_memory_saturated(
        governor
            .try_admit(RuntimeWorkRequest::foreground_query(MIB, 0))
            .unwrap_err(),
    );
    assert_retryable_memory_saturated(governor.try_admit(TYPED_READ).unwrap_err());

    drop(first);
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.admitted_memory_bytes, 0);
    assert_eq!(snapshot.active_foreground_tasks, 0);
    // Once idle again the floor admits the next request.
    drop(governor.try_admit(TYPED_READ).unwrap());
}

#[test]
fn progress_floor_requires_no_active_permit() {
    let governor = governor(GIB);
    // Any active permit, even one reserving no memory, disables the floor.
    let foreground = governor
        .try_admit(RuntimeWorkRequest::foreground_query(0, 0).with_blocking(false))
        .unwrap();
    assert_retryable_memory_saturated(governor.try_admit(TYPED_READ).unwrap_err());
    drop(foreground);

    let background = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(0))
        .unwrap();
    assert_retryable_memory_saturated(governor.try_admit(TYPED_READ).unwrap_err());
    drop(background);

    drop(governor.try_admit(TYPED_READ).unwrap());
}

#[test]
fn progress_floor_does_not_apply_to_background_work() {
    let governor = governor(GIB);
    assert_retryable_memory_saturated(
        governor
            .try_admit(RuntimeWorkRequest::background_maintenance(320 * MIB))
            .unwrap_err(),
    );
    // A Foreground request lowered into the Background pool is Background.
    let lowered = RuntimeWorkRequest {
        priority: RuntimeWorkPriority::Background,
        ..TYPED_READ
    };
    assert_retryable_memory_saturated(governor.try_admit(lowered).unwrap_err());
    // Background work within the dynamic budget is admitted as before.
    drop(
        governor
            .try_admit(RuntimeWorkRequest::background_maintenance(128 * MIB))
            .unwrap(),
    );
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn progress_floor_keeps_the_capacity_refusal_non_retryable() {
    // 4 GiB free: a 1 GiB dynamic budget below the 2 GiB capacity.
    let governor = governor(4 * GIB);
    let error = governor
        .try_admit(RuntimeWorkRequest::foreground_query(2 * GIB, 1))
        .unwrap_err();
    assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
    assert!(!error.is_retryable());
    assert_eq!(error.available, 2 * GIB);
    // Exactly capacity is admitted by the floor.
    drop(
        governor
            .try_admit(RuntimeWorkRequest::foreground_query(
                2 * GIB - 64 * MIB,
                64 * MIB,
            ))
            .unwrap(),
    );
}

#[test]
fn progress_floor_counts_retained_memory_against_capacity() {
    // Retain 1.5 GiB while the host has room, then lose availability.
    let governor = governor(8 * GIB);
    let retained = governor
        .try_admit(RuntimeWorkRequest::foreground_query(1536 * MIB, 0))
        .unwrap()
        .into_retained_memory()
        .unwrap();
    assert!(governor.update_resources(resources(GIB)));
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.active_foreground_tasks, 0);
    assert_eq!(snapshot.admitted_memory_bytes, 1536 * MIB);

    // 1.5 GiB + 640 MiB exceeds the 2 GiB capacity: still a transient refusal.
    assert_retryable_memory_saturated(
        governor
            .try_admit(RuntimeWorkRequest::foreground_query(576 * MIB, 64 * MIB))
            .unwrap_err(),
    );
    // 1.5 GiB + 320 MiB fits the capacity, and nothing else runs.
    drop(governor.try_admit(TYPED_READ).unwrap());
    drop(retained);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn queued_foreground_waiter_takes_the_floor_once_the_holder_releases() {
    let governor = governor(GIB);
    let first = governor.try_admit(TYPED_READ).unwrap();
    std::thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            governor.admit_waiting(
                TYPED_READ,
                &RuntimeTaskContext::default(),
                Some(Instant::now() + Duration::from_secs(20)),
            )
        });
        let started = Instant::now();
        while governor.snapshot().retryable_admission_rejections == 0 {
            assert!(started.elapsed() < Duration::from_secs(20));
            std::thread::yield_now();
        }
        assert_eq!(governor.snapshot().queued_admission_waiters, 1);
        drop(first);
        let permit = waiter.join().unwrap().unwrap();
        assert_eq!(governor.snapshot().admitted_memory_bytes, 320 * MIB);
        drop(permit);
    });
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn progress_floor_never_exceeds_sensed_available_memory() {
    // 64 MiB free: the dynamic budget is 16 MiB and the floor admits at most
    // what the host reports available, not the 2 GiB capacity.
    let constrained = governor(64 * MIB);
    assert_eq!(constrained.snapshot().limits.memory_budget_bytes, 16 * MIB);
    assert_retryable_memory_saturated(constrained.try_admit(TYPED_READ).unwrap_err());
    drop(
        constrained
            .try_admit(RuntimeWorkRequest::foreground_query(32 * MIB, 32 * MIB))
            .unwrap(),
    );

    // Sensed zero headroom stays authoritative.
    let exhausted = governor(0);
    assert_retryable_memory_saturated(
        exhausted
            .try_admit(RuntimeWorkRequest::foreground_query(1, 0))
            .unwrap_err(),
    );
}
