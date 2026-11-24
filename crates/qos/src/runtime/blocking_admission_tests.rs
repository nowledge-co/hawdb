// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Synchronous callers wait fairly in the admission queue (#1947).

use super::*;
use crate::{RuntimeMemorySnapshot, RuntimeResourceBudget};
use hawdb_core::RuntimeCancellationToken;
use std::thread;

const GIB: u64 = 1024 * 1024 * 1024;
/// Far below the test build's 60 s re-check interval: finishing within it
/// proves a signal, not the periodic re-check, woke the parked thread.
const PROMPT: Duration = Duration::from_secs(20);

fn one_cpu_governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig::shared_host(),
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(8 * GIB), Some(6 * GIB), None, None, None),
        ),
        IoConcurrencyBudget::new(4, 1),
    )
}

fn request(priority: RuntimeWorkPriority) -> RuntimeWorkRequest {
    RuntimeWorkRequest::query(priority, 0, 0).with_blocking(false)
}

fn wait_for_queue(governor: &RuntimeGovernor, waiters: usize) {
    let started = Instant::now();
    while governor.snapshot().queued_admission_waiters < waiters {
        assert!(started.elapsed() < PROMPT, "waiters never queued");
        thread::yield_now();
    }
}

#[test]
fn waiting_caller_is_admitted_when_capacity_frees() {
    let governor = one_cpu_governor();
    let held = governor
        .try_admit(request(RuntimeWorkPriority::Foreground))
        .unwrap();
    let waiting = thread::spawn({
        let governor = governor.clone();
        move || {
            governor.admit_waiting(
                request(RuntimeWorkPriority::Foreground),
                &RuntimeTaskContext::default(),
                Instant::now().checked_add(PROMPT),
            )
        }
    });
    wait_for_queue(&governor, 1);
    let released = Instant::now();
    drop(held);
    let permit = waiting.join().unwrap().unwrap();
    assert!(released.elapsed() < PROMPT);
    drop(permit);
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.queued_admission_waiters, 0);
    assert_eq!(snapshot.admission_waits, 1);
}

#[test]
fn background_waiter_yields_to_a_later_foreground_waiter() {
    let governor = one_cpu_governor();
    let held = governor
        .try_admit(request(RuntimeWorkPriority::Foreground))
        .unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let spawn = |priority: RuntimeWorkPriority| {
        let governor = governor.clone();
        let order = Arc::clone(&order);
        thread::spawn(move || {
            let permit = governor
                .admit_waiting(request(priority), &RuntimeTaskContext::default(), None)
                .unwrap();
            order.lock().unwrap().push(priority);
            drop(permit);
        })
    };
    let background = spawn(RuntimeWorkPriority::Background);
    wait_for_queue(&governor, 1);
    {
        // Keep the background waiter unaged however slowly this host runs.
        let mut state = mutex_lock(&governor.inner.state);
        state.admission_waiters[0].enqueued_at = Instant::now() + Duration::from_secs(3600);
    }
    let foreground = spawn(RuntimeWorkPriority::Foreground);
    wait_for_queue(&governor, 2);
    drop(held);
    foreground.join().unwrap();
    background.join().unwrap();
    assert_eq!(
        *order.lock().unwrap(),
        [
            RuntimeWorkPriority::Foreground,
            RuntimeWorkPriority::Background
        ]
    );
    assert_eq!(governor.snapshot().queued_admission_waiters, 0);
}

#[test]
fn admission_deadline_returns_the_retryable_refusal() {
    let governor = one_cpu_governor();
    let _held = governor
        .try_admit(request(RuntimeWorkPriority::Foreground))
        .unwrap();
    let error = governor
        .admit_waiting(
            request(RuntimeWorkPriority::Background),
            &RuntimeTaskContext::default(),
            Instant::now().checked_add(Duration::from_millis(20)),
        )
        .unwrap_err();
    let RuntimeAdmissionWaitError::Refused {
        error: refusal,
        waited,
    } = error
    else {
        panic!("expected a refusal, got {error:?}");
    };
    assert!(refusal.is_retryable());
    assert_eq!(refusal.code, RuntimeAdmissionCode::CpuSaturated);
    // A refusal from the queue reports how long the caller waited.
    assert!(waited.is_some_and(|waited| waited >= Duration::from_millis(20)));
    assert_eq!(governor.snapshot().queued_admission_waiters, 0);
}

#[test]
fn unsatisfiable_request_is_refused_without_waiting() {
    let governor = one_cpu_governor();
    let error = governor
        .admit_waiting(
            request(RuntimeWorkPriority::Foreground).with_cpu_slots(2),
            &RuntimeTaskContext::default(),
            None,
        )
        .unwrap_err();
    let RuntimeAdmissionWaitError::Refused {
        error: refusal,
        waited,
    } = error
    else {
        panic!("expected a refusal, got {error:?}");
    };
    assert!(!refusal.is_retryable());
    assert_eq!(waited, None);
    assert_eq!(governor.snapshot().queued_admission_waiters, 0);
}

#[test]
fn cancellation_while_queued_stops_the_wait() {
    let governor = one_cpu_governor();
    let _held = governor
        .try_admit(request(RuntimeWorkPriority::Foreground))
        .unwrap();
    let token = RuntimeCancellationToken::new();
    let waiting = thread::spawn({
        let governor = governor.clone();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        move || governor.admit_waiting(request(RuntimeWorkPriority::Foreground), &context, None)
    });
    wait_for_queue(&governor, 1);
    let cancelled = Instant::now();
    token.cancel();
    assert_eq!(
        waiting.join().unwrap().unwrap_err(),
        RuntimeAdmissionWaitError::Stopped(RuntimeCancellationReason::Cancelled)
    );
    assert!(cancelled.elapsed() < PROMPT);
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.queued_admission_waiters, 0);
    assert_eq!(snapshot.cancellations, 1);
}

#[test]
fn task_deadline_while_queued_stops_the_wait() {
    let governor = one_cpu_governor();
    let _held = governor
        .try_admit(request(RuntimeWorkPriority::Foreground))
        .unwrap();
    let error = governor
        .admit_waiting(
            request(RuntimeWorkPriority::Foreground),
            &RuntimeTaskContext::with_timeout(Duration::from_millis(20)),
            None,
        )
        .unwrap_err();
    assert_eq!(
        error,
        RuntimeAdmissionWaitError::Stopped(RuntimeCancellationReason::DeadlineExceeded)
    );
    assert_eq!(governor.snapshot().queued_admission_waiters, 0);
}

#[test]
fn background_waiter_wakes_itself_when_it_ages() {
    // A foreground waiter queued ahead is never admitted while its own
    // request cannot fit; once the background waiter ages it is selected and
    // must re-check without any release signal.
    let governor = one_cpu_governor();
    let held = governor
        .try_admit(request(RuntimeWorkPriority::Foreground))
        .unwrap();
    let foreground = governor.admission_waiter(RuntimeWorkPriority::Foreground);
    let waiting = thread::spawn({
        let governor = governor.clone();
        move || {
            governor.admit_waiting(
                RuntimeWorkRequest::new(RuntimeWorkPriority::Background, RuntimeWorkKind::Control),
                &RuntimeTaskContext::default(),
                Instant::now().checked_add(PROMPT),
            )
        }
    });
    let permit = waiting.join().unwrap().unwrap();
    drop((permit, foreground, held));
}

#[test]
fn queued_waiter_precedes_later_immediate_and_waiting_callers() {
    // A writer that waits is never overtaken by work that arrives after it:
    // same-priority waiters are FIFO and an immediate `try_admit` caller
    // yields with a retryable `QueuedAhead` even while capacity is free.
    let governor = one_cpu_governor();
    let held = governor
        .try_admit(request(RuntimeWorkPriority::Foreground))
        .unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let spawn = |name: &'static str| {
        let governor = governor.clone();
        let order = Arc::clone(&order);
        thread::spawn(move || {
            let permit = governor
                .admit_waiting(
                    request(RuntimeWorkPriority::Foreground),
                    &RuntimeTaskContext::default(),
                    None,
                )
                .unwrap();
            order.lock().unwrap().push(name);
            permit
        })
    };
    let writer = spawn("writer");
    wait_for_queue(&governor, 1);
    let reader = spawn("reader");
    wait_for_queue(&governor, 2);
    drop(held);
    let writer_permit = writer.join().unwrap();
    assert_eq!(*order.lock().unwrap(), ["writer"]);
    drop(writer_permit);
    let reader_permit = reader.join().unwrap();
    assert_eq!(*order.lock().unwrap(), ["writer", "reader"]);

    let blocker = governor.admission_waiter(RuntimeWorkPriority::Foreground);
    drop(reader_permit);
    // Capacity is free, but the queued waiter goes first.
    let error = governor
        .try_admit(request(RuntimeWorkPriority::Foreground))
        .unwrap_err();
    assert_eq!(error.code, RuntimeAdmissionCode::QueuedAhead);
    assert!(error.is_retryable());
    let permit = governor
        .try_admit_waiter(&blocker, request(RuntimeWorkPriority::Foreground))
        .unwrap();
    drop((permit, blocker));
    assert_eq!(governor.snapshot().queued_admission_waiters, 0);
}

#[test]
fn strict_foreground_precedence_admits_users_past_aged_background_waiters() {
    // With strict Foreground precedence, user requests (immediate Foreground)
    // outrank queued Background work even after it ages; an immediate
    // Background request still yields to it. By default the aged waiter
    // blocks immediate Foreground requests too (#303).
    let governor = one_cpu_governor();
    let replay = governor.admission_waiter(RuntimeWorkPriority::Background);
    {
        let mut state = mutex_lock(&governor.inner.state);
        state.admission_waiters[0].enqueued_at = Instant::now() - BACKGROUND_ADMISSION_AGING;
    }
    assert_eq!(
        governor
            .try_admit(request(RuntimeWorkPriority::Foreground))
            .unwrap_err()
            .code,
        RuntimeAdmissionCode::QueuedAhead
    );
    governor.set_foreground_preempts_queued_background(true);
    let shadow = governor
        .try_admit(request(RuntimeWorkPriority::Background))
        .unwrap_err();
    assert_eq!(shadow.code, RuntimeAdmissionCode::QueuedAhead);
    assert!(shadow.is_retryable());
    let user = governor
        .try_admit(request(RuntimeWorkPriority::Foreground))
        .unwrap();
    // While the user request runs the replay is refused for capacity only.
    assert_eq!(
        governor
            .try_admit_waiter(&replay, request(RuntimeWorkPriority::Background))
            .unwrap_err()
            .code,
        RuntimeAdmissionCode::CpuSaturated
    );
    drop(user);
    let permit = governor
        .try_admit_waiter(&replay, request(RuntimeWorkPriority::Background))
        .unwrap();
    drop((permit, replay));
}

#[test]
fn short_foreground_wait_takes_the_slot_a_background_task_releases() {
    // A Background task holds the shared CPU slot and Background waiters are
    // queued; a Foreground caller with a short admission deadline is admitted
    // as soon as the slot frees, ahead of the queued Background waiter, and
    // its trace records the time it queued.
    let governor = one_cpu_governor();
    let running = governor
        .try_admit(request(RuntimeWorkPriority::Background))
        .unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let replay = thread::spawn({
        let governor = governor.clone();
        let order = Arc::clone(&order);
        move || {
            let permit = governor
                .admit_waiting(
                    request(RuntimeWorkPriority::Background),
                    &RuntimeTaskContext::default(),
                    None,
                )
                .unwrap();
            order.lock().unwrap().push(RuntimeWorkPriority::Background);
            drop(permit);
        }
    });
    wait_for_queue(&governor, 1);
    {
        let mut state = mutex_lock(&governor.inner.state);
        state.admission_waiters[0].enqueued_at = Instant::now() + Duration::from_secs(3600);
    }
    let trace = hawdb_core::RuntimeAdmissionTrace::new();
    let mailbox = thread::spawn({
        let governor = governor.clone();
        let order = Arc::clone(&order);
        let context = RuntimeTaskContext::default().with_admission_trace(trace.clone());
        move || {
            let permit = governor
                .admit_waiting(
                    request(RuntimeWorkPriority::Foreground),
                    &context,
                    Instant::now().checked_add(PROMPT),
                )
                .unwrap();
            order.lock().unwrap().push(RuntimeWorkPriority::Foreground);
            drop(permit);
        }
    });
    wait_for_queue(&governor, 2);
    let released = Instant::now();
    drop(running);
    mailbox.join().unwrap();
    replay.join().unwrap();
    assert!(released.elapsed() < PROMPT);
    assert_eq!(
        *order.lock().unwrap(),
        [
            RuntimeWorkPriority::Foreground,
            RuntimeWorkPriority::Background
        ]
    );
    assert_eq!(trace.admissions(), 1);
    assert!(trace.queued_for() > Duration::ZERO);
}

#[test]
fn foreground_waiter_yields_to_aged_background_waiters_at_most_once() {
    // Three aged Background waiters (replay, retrieval, maintenance) and one
    // short-deadline Foreground waiter: exactly one Background admission may
    // overtake the Foreground waiter, then it is next.
    for strict in [false, true] {
        let governor = one_cpu_governor();
        governor.set_foreground_preempts_queued_background(strict);
        let held = governor
            .try_admit(request(RuntimeWorkPriority::Foreground))
            .unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let spawn = |priority: RuntimeWorkPriority, wait_until: Option<Instant>| {
            let governor = governor.clone();
            let order = Arc::clone(&order);
            thread::spawn(move || {
                let permit = governor
                    .admit_waiting(
                        request(priority),
                        &RuntimeTaskContext::default(),
                        wait_until,
                    )
                    .unwrap();
                order.lock().unwrap().push(priority);
                drop(permit);
            })
        };
        let background = (0..3)
            .map(|queued| {
                let waiter = spawn(RuntimeWorkPriority::Background, None);
                wait_for_queue(&governor, queued + 1);
                waiter
            })
            .collect::<Vec<_>>();
        let foreground = spawn(
            RuntimeWorkPriority::Foreground,
            Instant::now().checked_add(Duration::from_secs(10)),
        );
        wait_for_queue(&governor, 4);
        {
            // Every Background waiter has aged before capacity frees.
            let mut state = mutex_lock(&governor.inner.state);
            for waiter in &mut state.admission_waiters {
                if waiter.priority == RuntimeWorkPriority::Background {
                    waiter.enqueued_at = Instant::now() - BACKGROUND_ADMISSION_AGING;
                }
            }
        }
        drop(held);
        foreground.join().unwrap();
        for waiter in background {
            waiter.join().unwrap();
        }
        assert_eq!(
            *order.lock().unwrap(),
            [
                RuntimeWorkPriority::Background,
                RuntimeWorkPriority::Foreground,
                RuntimeWorkPriority::Background,
                RuntimeWorkPriority::Background,
            ],
            "strict = {strict}"
        );
        assert_eq!(governor.snapshot().queued_admission_waiters, 0);
    }
}

#[test]
fn a_new_foreground_waiter_may_again_be_overtaken_once() {
    // The once-only rule is per Foreground waiter: Background work still
    // progresses past each newly queued Foreground waiter, so neither class
    // starves.
    let governor = one_cpu_governor();
    let held = governor
        .try_admit(request(RuntimeWorkPriority::Foreground))
        .unwrap();
    let first_background = governor.admission_waiter(RuntimeWorkPriority::Background);
    let second_background = governor.admission_waiter(RuntimeWorkPriority::Background);
    let first_foreground = governor.admission_waiter(RuntimeWorkPriority::Foreground);
    {
        let mut state = mutex_lock(&governor.inner.state);
        for waiter in &mut state.admission_waiters {
            waiter.enqueued_at = Instant::now() - BACKGROUND_ADMISSION_AGING;
        }
    }
    let background = request(RuntimeWorkPriority::Background);
    let foreground = request(RuntimeWorkPriority::Foreground);
    drop(held);
    drop(
        governor
            .try_admit_waiter(&first_background, background)
            .unwrap(),
    );
    // The overtaken Foreground waiter is next, ahead of the aged waiter.
    assert_eq!(
        governor
            .try_admit_waiter(&second_background, background)
            .unwrap_err()
            .code,
        RuntimeAdmissionCode::QueuedAhead
    );
    drop(
        governor
            .try_admit_waiter(&first_foreground, foreground)
            .unwrap(),
    );
    // A Foreground waiter queued later has not yielded yet.
    let second_foreground = governor.admission_waiter(RuntimeWorkPriority::Foreground);
    assert_eq!(
        governor
            .try_admit_waiter(&second_foreground, foreground)
            .unwrap_err()
            .code,
        RuntimeAdmissionCode::QueuedAhead
    );
    drop(
        governor
            .try_admit_waiter(&second_background, background)
            .unwrap(),
    );
    drop(
        governor
            .try_admit_waiter(&second_foreground, foreground)
            .unwrap(),
    );
    drop((
        first_background,
        second_background,
        first_foreground,
        second_foreground,
    ));
    assert_eq!(governor.snapshot().queued_admission_waiters, 0);
}
