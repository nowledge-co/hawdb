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

//! Batched local admission of checkpoint builder units (#1616).

use super::tests::CheckpointWorkProbe;
use super::*;
use hawdb_core::time::Duration;
use hawdb_core::RuntimeCancellationToken;
use hawdb_qos::LocalQosPolicy;
use std::sync::atomic::{AtomicBool, AtomicUsize};

const N: usize = CHECKPOINT_UNITS_PER_ADMISSION;

fn one_operation() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    })
}

fn batches(units: usize) -> u64 {
    units.div_ceil(N) as u64
}

#[test]
fn unit_progress_and_admission_counts_add_up_exactly() {
    let scheduler = one_operation();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let progress = Arc::new(AtomicU64::new(0));
    let work = probe
        .context(scheduler.clone())
        .with_progress_counter(progress.clone());
    let units = 1_000;
    for _ in 0..units {
        work.start_unit().unwrap().finish();
    }
    let stats = work.unit_batch_stats();
    assert_eq!(progress.load(Ordering::Relaxed), units as u64);
    assert_eq!(stats.units, units as u64);
    assert_eq!(stats.finished_units, units as u64);
    assert_eq!(stats.abandoned_units, 0);
    assert_eq!(probe.started.load(Ordering::SeqCst), units);
    assert_eq!(probe.completed.load(Ordering::SeqCst), units);
    // One admission and one clock read per batch instead of one admission
    // and two clock reads per unit.
    assert_eq!(stats.admissions, batches(units));
    assert_eq!(
        probe.admissions.load(Ordering::SeqCst),
        batches(units) as usize
    );
    assert_eq!(stats.clock_reads, batches(units));
    assert_eq!(stats.refusals, 0);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    probe.assert_released(&scheduler);
    // The last batch completes when the owner ends it.
    assert_eq!(probe.active_units.load(Ordering::SeqCst), 1);
    work.release_unit_batch();
    assert_eq!(probe.active_units.load(Ordering::SeqCst), 0);
    assert_eq!(work.unit_batch_stats().clock_reads, batches(units) + 1);

    // Classification and abandoned units are counted, but are not progress.
    work.classify(|work| {
        for index in 0..10 {
            let unit = work.start_unit().unwrap();
            if index % 3 == 0 {
                drop(unit);
            } else {
                unit.finish();
            }
        }
        Ok::<_, ()>(())
    })
    .unwrap();
    let stats = work.unit_batch_stats();
    assert_eq!(stats.units, units as u64 + 11);
    assert_eq!(stats.abandoned_units, 4);
    assert_eq!(stats.finished_units + stats.abandoned_units, stats.units);
    assert_eq!(progress.load(Ordering::Relaxed), units as u64 + 6);
    assert_eq!(
        probe.started.load(Ordering::SeqCst),
        probe.completed.load(Ordering::SeqCst)
    );
    // `classify` ends its batch.
    assert_eq!(probe.active_units.load(Ordering::SeqCst), 0);
    probe.assert_released(&scheduler);
}

#[test]
fn cancellation_at_every_unit_inside_and_across_batches_stops_the_next_unit() {
    let scheduler = one_operation();
    for cut in 1..=(2 * N + 3) {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cut, Ordering::SeqCst);
        let progress = Arc::new(AtomicU64::new(0));
        let work = probe
            .context(scheduler.clone())
            .with_progress_counter(progress.clone());
        let error = loop {
            match work.start_unit() {
                Ok(unit) => unit.finish(),
                Err(error) => break error,
            }
        };
        assert_eq!(
            error,
            CheckpointWorkError::Stopped(RuntimeCancellationReason::Cancelled),
            "cut={cut}"
        );
        // Not one unit starts after the cancellation, wherever it falls in
        // the batch.
        assert_eq!(probe.started.load(Ordering::SeqCst), cut, "cut={cut}");
        assert_eq!(probe.completed.load(Ordering::SeqCst), cut, "cut={cut}");
        assert_eq!(progress.load(Ordering::Relaxed), cut as u64);
        let stats = work.unit_batch_stats();
        assert_eq!(stats.units, cut as u64);
        assert_eq!(stats.admissions, batches(cut), "cut={cut}");
        // A stopped task does not keep its idle batch.
        assert_eq!(probe.active_units.load(Ordering::SeqCst), 0, "cut={cut}");
        probe.assert_released(&scheduler);
    }
}

#[test]
fn cancellation_from_another_thread_is_observed_by_the_next_unit() {
    let scheduler = one_operation();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    let cancelled = Arc::new(AtomicBool::new(false));
    let canceller = {
        let token: RuntimeCancellationToken = probe.cancellation.clone();
        let cancelled = cancelled.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            token.cancel();
            cancelled.store(true, Ordering::SeqCst);
        })
    };
    let mut after_cancel = 0;
    loop {
        let observed = cancelled.load(Ordering::SeqCst);
        match work.start_unit() {
            Ok(unit) => {
                assert!(!observed, "a unit started after cancellation was published");
                unit.finish();
            }
            Err(error) => {
                assert!(matches!(error, CheckpointWorkError::Stopped(_)));
                break;
            }
        }
        after_cancel += usize::from(observed);
    }
    canceller.join().unwrap();
    assert_eq!(after_cancel, 0);
    probe.assert_released(&scheduler);
}

#[test]
fn abandoned_unit_fails_its_batch_and_releases_it_immediately() {
    let scheduler = one_operation();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    for _ in 0..10 {
        work.start_unit().unwrap().finish();
    }
    let unit = work.start_unit().unwrap();
    assert_eq!(scheduler.state().running_background_operations, 1);
    drop(unit);
    // Released at once with a failed outcome, without waiting for the
    // batch bound or the owner.
    assert_eq!(probe.active_units.load(Ordering::SeqCst), 0);
    assert_eq!(scheduler.state().running_background_operations, 0);
    let stats = work.unit_batch_stats();
    assert_eq!(
        (stats.units, stats.abandoned_units, stats.admissions),
        (11, 1, 1)
    );
    work.start_unit().unwrap().finish();
    assert_eq!(work.unit_batch_stats().admissions, 2);
    probe.assert_released(&scheduler);
}

#[test]
fn competing_background_work_preempts_an_idle_batch_at_the_next_unit() {
    let scheduler = one_operation();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    for _ in 0..10 {
        work.start_unit().unwrap().finish();
    }
    // A running unit keeps its operation, as a per-unit permit did.
    let running = work.start_unit().unwrap();
    assert!(scheduler
        .try_start(WorkRequest::background(WorkClass::Projection, 1))
        .is_err());
    running.finish();
    // Between units the batch is idle: competing work takes the operation
    // mid-batch, and the builder's very next unit is deferred.
    let competitor = scheduler
        .try_start(WorkRequest::background(WorkClass::Projection, 1))
        .unwrap();
    assert!(matches!(
        work.start_unit(),
        Err(CheckpointWorkError::Admission(QosAdmission::Defer { .. }))
    ));
    assert_eq!(probe.started.load(Ordering::SeqCst), 11);
    let stats = work.unit_batch_stats();
    assert_eq!((stats.admissions, stats.refusals), (2, 1));
    competitor.finish();
    work.start_unit().unwrap().finish();
    assert_eq!(work.unit_batch_stats().admissions, 3);
    probe.assert_released(&scheduler);
}

#[test]
fn nested_units_join_the_running_batch_under_one_operation() {
    let scheduler = one_operation();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    let outer = work.start_unit().unwrap();
    for _ in 0..(2 * N) {
        let inner = work.clone().start_unit().unwrap();
        assert_eq!(scheduler.state().running_background_operations, 1);
        inner.finish();
    }
    outer.finish();
    let stats = work.unit_batch_stats();
    assert_eq!(stats.units, 2 * N as u64 + 1);
    // A running batch cannot be replaced; the bound applies once it is idle.
    assert_eq!(stats.admissions, 1);
    work.start_unit().unwrap().finish();
    assert_eq!(work.unit_batch_stats().admissions, 2);
    probe.assert_released(&scheduler);
}

#[test]
fn deadline_is_checked_at_every_batch_boundary() {
    let scheduler = one_operation();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.context(scheduler.clone());
    let expired = RuntimeTaskContext::new(
        probe.cancellation.clone(),
        Some(Instant::now() - Duration::from_millis(1)),
    );
    let work = CheckpointWorkContext::new(expired).with_scheduler(scheduler.clone());
    assert!(matches!(
        work.start_unit(),
        Err(CheckpointWorkError::Stopped(
            RuntimeCancellationReason::DeadlineExceeded
        ))
    ));
    assert_eq!(work.unit_batch_stats().admissions, 0);

    let short = RuntimeTaskContext::new(
        probe.cancellation.clone(),
        Some(Instant::now() + Duration::from_millis(50)),
    );
    let work = CheckpointWorkContext::new(short).with_scheduler(scheduler.clone());
    work.start_unit().unwrap().finish();
    std::thread::sleep(std::time::Duration::from_millis(60));
    // The deadline passed inside the batch: at most the remaining units of
    // this batch run before the next boundary observes it.
    for _ in 1..N {
        work.start_unit().unwrap().finish();
    }
    assert!(matches!(
        work.start_unit(),
        Err(CheckpointWorkError::Stopped(
            RuntimeCancellationReason::DeadlineExceeded
        ))
    ));
    assert_eq!(work.unit_batch_stats().units, N as u64);
    assert_eq!(scheduler.state().running_background_operations, 0);
    // The ordinary task checkpoint still observes the deadline immediately.
    assert!(work.checkpoint().is_err());
}

#[test]
fn input_io_waves_stay_per_item_inside_batched_units() {
    let scheduler = one_operation();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    let items = 3 * N + 7;
    let mut input = 0..items;
    let mut seen = 0;
    loop {
        let (item, unit) = work.next_input(&mut input, false).unwrap();
        unit.finish();
        if item.is_none() {
            break;
        }
        seen += 1;
    }
    assert_eq!(seen, items);
    // Runtime-governor I/O admission, where foreground work takes priority,
    // is still requested once per item.
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), items + 1);
    assert_eq!(work.unit_batch_stats().admissions, batches(items + 1));
    probe.assert_released(&scheduler);
}

#[test]
fn contexts_sharing_a_one_operation_budget_interleave_their_batches() {
    let scheduler = one_operation();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let first = probe.context(scheduler.clone());
    let second =
        CheckpointWorkContext::new(first.task_context().clone()).with_scheduler(scheduler.clone());
    let alternations = AtomicUsize::new(0);
    for _ in 0..(3 * N) {
        first.start_unit().unwrap().finish();
        second.start_unit().unwrap().finish();
        alternations.fetch_add(1, Ordering::Relaxed);
    }
    // Each idle batch is reclaimed by the other context's admission.
    assert_eq!(first.unit_batch_stats().admissions, 3 * N as u64);
    assert_eq!(second.unit_batch_stats().admissions, 3 * N as u64);
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(alternations.load(Ordering::Relaxed), 3 * N);
}

/// #1651: clones are one shared handle. Units started through any clone are
/// charged to the same batch and progress counter, and the builder methods
/// copy on write, so an already published handle never changes.
#[test]
fn clones_share_one_handle_and_builders_copy_on_write() {
    let scheduler = one_operation();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let progress = Arc::new(AtomicU64::new(0));
    let work = probe
        .context(scheduler.clone())
        .with_progress_counter(progress.clone());
    let clone = work.clone();
    assert!(work.is_same(&clone));
    for index in 0..(3 * N + 5) {
        let context = if index % 2 == 0 { &work } else { &clone };
        context.start_unit().unwrap().finish();
    }
    let units = 3 * N as u64 + 5;
    assert_eq!(progress.load(Ordering::Relaxed), units);
    assert_eq!(work.unit_batch_stats(), clone.unit_batch_stats());
    assert_eq!(work.unit_batch_stats().units, units);
    assert_eq!(work.unit_batch_stats().admissions, batches(units as usize));

    // A builder applied to a clone leaves the original handle untouched but
    // keeps charging the same batch.
    let other = Arc::new(AtomicU64::new(0));
    let retracked = clone.with_progress_counter(other.clone());
    assert!(!work.is_same(&retracked));
    retracked.start_unit().unwrap().finish();
    work.start_unit().unwrap().finish();
    assert_eq!(other.load(Ordering::Relaxed), 1);
    assert_eq!(progress.load(Ordering::Relaxed), units + 1);
    assert_eq!(work.unit_batch_stats().units, units + 2);
    assert_eq!(retracked.unit_batch_stats(), work.unit_batch_stats());
    work.release_unit_batch();
    probe.assert_released(&scheduler);
}

/// #1651: retained builders re-bind per record; only another context (a new
/// attempt or a classification scope) replaces the stored one.
#[test]
fn rebind_replaces_only_another_context() {
    let work = CheckpointWorkContext::default().with_scheduler(one_operation());
    let mut bound = work.clone();
    let before = super::tests::rebinds();
    for _ in 0..1_000 {
        bound.rebind(&work);
    }
    assert_eq!(super::tests::rebinds(), before);
    assert!(bound.is_same(&work));

    let forked = work.fork_handle();
    assert!(!forked.is_same(&work));
    bound.rebind(&forked);
    assert_eq!(super::tests::rebinds(), before + 1);
    assert!(bound.is_same(&forked));

    work.classify(|scoped| {
        assert!(!scoped.is_same(&work));
        bound.rebind(scoped);
        bound.rebind(scoped);
        Ok::<_, ()>(())
    })
    .unwrap();
    assert_eq!(super::tests::rebinds(), before + 2);
    let fresh = CheckpointWorkContext::default();
    bound.rebind(&fresh);
    assert_eq!(super::tests::rebinds(), before + 3);
    assert!(bound.is_same(&fresh));
}

/// Without a scheduler each unit still observes cancellation and counts
/// progress exactly; a unit without a progress counter holds nothing.
#[test]
fn unscheduled_units_count_progress_and_observe_cancellation_at_every_unit() {
    for cut in [1usize, 2, N - 1, N, N + 1, 3 * N] {
        let token = RuntimeCancellationToken::new();
        let progress = Arc::new(AtomicU64::new(0));
        let work = CheckpointWorkContext::new(RuntimeTaskContext::without_deadline(token.clone()))
            .with_progress_counter(progress.clone());
        let mut started = 0usize;
        let error = loop {
            match work.start_unit() {
                Ok(unit) => {
                    started += 1;
                    unit.finish();
                    if started == cut {
                        token.cancel();
                    }
                }
                Err(error) => break error,
            }
        };
        assert_eq!(
            error,
            CheckpointWorkError::Stopped(RuntimeCancellationReason::Cancelled)
        );
        assert_eq!(started, cut);
        assert_eq!(progress.load(Ordering::Relaxed), cut as u64);
        assert_eq!(work.unit_batch_stats(), CheckpointUnitBatchStats::default());
    }
    let plain = CheckpointWorkContext::default();
    let unit = plain.start_unit().unwrap();
    assert!(unit.accounting.is_none());
    unit.finish();
}
