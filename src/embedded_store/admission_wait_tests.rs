// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Handle reads and writes wait fairly in the admission queue (#1947).

use super::*;
use crate::search::{SearchProjectionKind, SearchProjectionRow};
use crate::{Database, SearchIndex};
use hawdb_core::{RuntimeAdmissionTrace, RuntimeCancellationReason, RuntimeCancellationToken};
use hawdb_qos::{RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// Generous bound for anything that should happen promptly; a lost wake-up
/// or a guard held while queued shows up as exceeding it.
const PROMPT: Duration = Duration::from_secs(20);

/// A one-CPU host: every handle read and write needs the single CPU slot.
fn one_cpu_governor() -> RuntimeGovernor {
    governor(1, 2)
}

fn governor(cpus: usize, foreground_io_depth: usize) -> RuntimeGovernor {
    governor_with_io(cpus, foreground_io_depth, 1)
}

fn governor_with_io(
    cpus: usize,
    foreground_io_depth: usize,
    background_io_depth: usize,
) -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(2 << 30),
            // Statement transactions reserve the mutation result ceiling.
            result_budget_bytes: 128 << 20,
            // One Background task at a time, as on a one-CPU host.
            background_task_limit: NonZeroUsize::new(1),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(cpus).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(8 << 30), Some(6 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(foreground_io_depth, background_io_depth),
    )
}

/// A one-CPU governor configured for the host's tiers: user requests
/// (immediate Foreground) never yield to queued Background work.
fn tiered_governor() -> RuntimeGovernor {
    let governor = one_cpu_governor();
    governor.set_foreground_preempts_queued_background(true);
    governor
}

fn handle(governor: &RuntimeGovernor) -> EmbeddedStoreHandle {
    let mut database = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(16),
        max_read_result_payload_bytes: Some(16 * 1024),
        ..DatabaseConfig::default()
    });
    database
        .query("CREATE (:Memory {id: 'seed', title: 'Seed memory'})")
        .unwrap();
    let graph = EmbeddedGraph::from_database_with_runtime_governor(
        database,
        GraphMode::WritableCutover,
        governor.clone(),
    );
    let mut index = SearchIndex::in_memory();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "seed".into(),
            title: "Seed memory".into(),
            body: "seed".into(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    let projection = SearchProjection::from_index(index);
    EmbeddedStoreHandle::new(EmbeddedStore::new(graph, Some(projection))).unwrap()
}

fn hold_cpu(governor: &RuntimeGovernor) -> RuntimePermit {
    governor
        .try_admit(RuntimeWorkRequest::foreground_query(0, 0).with_blocking(false))
        .unwrap()
}

fn budget() -> EmbeddedReadSnapshotBudget {
    EmbeddedReadSnapshotBudget {
        max_rows: 4,
        max_payload_bytes: 4096,
    }
}

fn count_memories(handle: &EmbeddedStoreHandle, context: &RuntimeTaskContext) -> Result<usize> {
    handle.with_bounded_read_snapshot_context(budget(), context, |snapshot| {
        let output = snapshot.query_cypher(
            "MATCH (m:Memory) RETURN count(m) AS memories",
            &BTreeMap::new(),
            1,
        )?;
        match output.rows.first().and_then(|row| row.value(0)) {
            Some(Value::Int(count)) => Ok(usize::try_from(*count).unwrap()),
            other => panic!("unexpected count row {other:?}"),
        }
    })
}

fn shadow_reader(wait: Duration) -> RuntimeTaskContext {
    RuntimeTaskContext::default()
        .with_admission_priority(RuntimeWorkPriority::Background)
        .with_admission_wait(wait)
}

fn wait_for_queue(governor: &RuntimeGovernor, waiters: usize) {
    let started = Instant::now();
    while governor.snapshot().queued_admission_waiters < waiters {
        assert!(started.elapsed() < PROMPT, "admission waiters never queued");
        thread::yield_now();
    }
}

fn assert_idle(governor: &RuntimeGovernor) {
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.queued_admission_waiters, 0);
    assert_eq!(snapshot.active_cpu_slots, 0);
    assert_eq!(snapshot.admitted_memory_bytes, 0);
    assert_eq!(snapshot.admissions, snapshot.completions);
}

#[test]
fn default_context_still_refuses_at_once() {
    let governor = one_cpu_governor();
    let handle = handle(&governor);
    let held = hold_cpu(&governor);
    let started = Instant::now();
    let error = count_memories(&handle, &RuntimeTaskContext::default()).unwrap_err();
    let HawDBError::RuntimeAdmissionRefused(refusal) = error else {
        panic!("expected an admission refusal, got {error:?}");
    };
    assert!(refusal.retryable);
    assert_eq!(refusal.waited, None, "an immediate refusal never queued");
    assert!(started.elapsed() < PROMPT);
    assert_eq!(governor.snapshot().queued_admission_waiters, 0);
    // A background priority alone does not queue either.
    let background =
        RuntimeTaskContext::default().with_admission_priority(RuntimeWorkPriority::Background);
    assert!(matches!(
        count_memories(&handle, &background),
        Err(HawDBError::RuntimeAdmissionRefused(_))
    ));
    drop(held);
    assert_eq!(
        count_memories(&handle, &RuntimeTaskContext::default()).unwrap(),
        1
    );
    assert_idle(&governor);
}

#[test]
fn background_reader_runs_at_background_priority() {
    let governor = one_cpu_governor();
    let handle = handle(&governor);
    handle
        .with_bounded_read_snapshot_context(budget(), &shadow_reader(PROMPT), |_| {
            let snapshot = governor.snapshot();
            assert_eq!(snapshot.active_background_tasks, 1);
            assert_eq!(snapshot.active_foreground_tasks, 0);
            Ok(())
        })
        .unwrap();
    // Queries take the same policy through planning and execution.
    let output = handle
        .query_with_params_with_report_options_context(
            "MATCH (m:Memory) RETURN m.id AS id",
            &BTreeMap::new(),
            EmbeddedQueryReportOptions::default(),
            &shadow_reader(PROMPT),
        )
        .unwrap();
    assert_eq!(output.output.rows.len(), 1);
    assert_idle(&governor);
}

#[test]
fn waiting_reader_is_admitted_when_capacity_frees() {
    let governor = one_cpu_governor();
    let handle = handle(&governor);
    let held = hold_cpu(&governor);
    let trace = RuntimeAdmissionTrace::new();
    let reader = thread::spawn({
        let handle = handle.clone();
        let context = shadow_reader(PROMPT).with_admission_trace(trace.clone());
        move || count_memories(&handle, &context)
    });
    wait_for_queue(&governor, 1);
    let released = Instant::now();
    drop(held);
    assert_eq!(reader.join().unwrap().unwrap(), 1);
    // The caller sees that its one admission queued.
    assert_eq!(trace.admissions(), 1);
    assert!(trace.queued_for() > Duration::ZERO);
    let immediate = RuntimeAdmissionTrace::new();
    count_memories(
        &handle,
        &RuntimeTaskContext::default().with_admission_trace(immediate.clone()),
    )
    .unwrap();
    assert_eq!(immediate.admissions(), 1);
    assert_eq!(immediate.queued_for(), Duration::ZERO);
    assert!(released.elapsed() < PROMPT);
    assert_eq!(governor.snapshot().admission_waits, 1);
    assert_idle(&governor);
}

#[test]
fn admission_wait_deadline_returns_the_typed_retryable_refusal() {
    let governor = one_cpu_governor();
    let handle = handle(&governor);
    let _held = hold_cpu(&governor);
    let error = count_memories(&handle, &shadow_reader(Duration::from_millis(30))).unwrap_err();
    let HawDBError::RuntimeAdmissionRefused(refusal) = error else {
        panic!("expected an admission refusal, got {error:?}");
    };
    assert!(refusal.retryable);
    // The wait deadline expired while queued: distinguishable from an
    // immediate refusal, and reported with the time spent queued.
    assert!(refusal
        .waited
        .is_some_and(|waited| waited >= Duration::from_millis(30)));
    assert!(error.to_string().contains("ms queued"));
    assert_eq!(governor.snapshot().queued_admission_waiters, 0);
    assert_eq!(governor.snapshot().admission_waits, 1);
}

#[test]
fn cancellation_while_queued_returns_the_typed_stop() {
    let governor = one_cpu_governor();
    let handle = handle(&governor);
    let held = hold_cpu(&governor);
    let token = RuntimeCancellationToken::new();
    let reader = thread::spawn({
        let handle = handle.clone();
        let context = RuntimeTaskContext::without_deadline(token.clone())
            .with_admission_priority(RuntimeWorkPriority::Background)
            .with_admission_wait(Duration::MAX);
        move || count_memories(&handle, &context)
    });
    wait_for_queue(&governor, 1);
    token.cancel();
    assert!(matches!(
        reader.join().unwrap(),
        Err(HawDBError::RuntimeTaskStopped(
            RuntimeCancellationReason::Cancelled
        ))
    ));
    // The task's own deadline stops a wait without an admission deadline.
    let writer = handle.with_transaction_context(
        &RuntimeTaskContext::with_timeout(Duration::from_millis(30))
            .with_admission_wait(Duration::MAX),
        |tx| tx.query("CREATE (:Memory {id: 'never'})").map(|_| ()),
    );
    assert!(matches!(
        writer,
        Err(HawDBError::RuntimeTaskStopped(
            RuntimeCancellationReason::DeadlineExceeded
        ))
    ));
    drop(held);
    assert_eq!(
        count_memories(&handle, &RuntimeTaskContext::default()).unwrap(),
        1
    );
    assert_idle(&governor);
}

#[test]
fn queued_callers_hold_no_store_guard() {
    let governor = one_cpu_governor();
    let handle = handle(&governor);
    let held = hold_cpu(&governor);
    let wait = RuntimeTaskContext::default().with_admission_wait(PROMPT);
    // A statement transaction plans under the store read guard, then queues.
    let transaction = thread::spawn({
        let handle = handle.clone();
        let wait = wait.clone();
        move || {
            handle.transaction_context(
                &wait,
                &[crate::NowledgeGraphStatement {
                    cypher: "CREATE (:Memory {id: 'queued-transaction'})".into(),
                    parameters: BTreeMap::new(),
                }],
            )
        }
    });
    while governor.snapshot().queued_admission_waiters < 1 {
        assert!(
            !transaction.is_finished(),
            "{:?}",
            transaction.join().unwrap()
        );
        thread::yield_now();
    }
    // So does typed maintenance.
    let maintenance = thread::spawn({
        let handle = handle.clone();
        let wait = wait.clone();
        move || {
            handle
                .admit_typed_maintenance_context(RuntimeWorkPriority::Foreground, 0, 1, &wait)
                .map(drop)
        }
    });
    while governor.snapshot().queued_admission_waiters < 2 {
        assert!(
            !maintenance.is_finished(),
            "{:?}",
            maintenance.join().unwrap()
        );
        assert!(
            !transaction.is_finished(),
            "{:?}",
            transaction.join().unwrap()
        );
        thread::yield_now();
    }
    // The permit holder can still take the exclusive write guard and publish.
    let (sender, receiver) = mpsc::channel();
    let writer = thread::spawn({
        let handle = handle.clone();
        move || {
            let mut store = handle.write_store().unwrap();
            store
                .graph_mut()
                .database_mut()
                .query("CREATE (:Memory {id: 'guarded'})")
                .unwrap();
            drop(store);
            sender.send(()).unwrap();
        }
    });
    receiver
        .recv_timeout(PROMPT)
        .expect("a queued caller blocked the write guard");
    writer.join().unwrap();
    drop(held);
    transaction.join().unwrap().unwrap();
    maintenance.join().unwrap().unwrap();
    assert_eq!(
        count_memories(&handle, &RuntimeTaskContext::default()).unwrap(),
        3
    );
    assert_idle(&governor);
}

#[test]
fn reader_waits_out_a_writer_inside_its_transaction() {
    let governor = one_cpu_governor();
    let handle = handle(&governor);
    let (queued_sender, queued) = mpsc::channel::<()>();
    let writer = thread::spawn({
        let handle = handle.clone();
        let governor = governor.clone();
        move || {
            handle.with_transaction(|tx| {
                tx.query("CREATE (:Memory {id: 'committed-while-queued'})")?;
                // Commit only after the reader has queued behind this permit.
                queued.recv_timeout(PROMPT).unwrap();
                wait_for_queue(&governor, 1);
                Ok(())
            })
        }
    });
    let started = Instant::now();
    while governor.snapshot().active_cpu_slots == 0 {
        assert!(started.elapsed() < PROMPT, "writer never admitted");
        thread::yield_now();
    }
    queued_sender.send(()).unwrap();
    let count = count_memories(&handle, &shadow_reader(PROMPT)).unwrap();
    writer.join().unwrap().unwrap();
    assert_eq!(count, 2);
    assert_idle(&governor);
}

/// Mirrors an embedding host: one background shadow-comparison reader loops
/// beside a foreground replay writer on a one-CPU governor. With admission
/// waits, the writer is never refused and the reader still progresses.
#[test]
fn replay_writer_is_never_refused_beside_a_background_shadow_reader() {
    const COMMITS: usize = 300;
    let governor = one_cpu_governor();
    let handle = handle(&governor);
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicUsize::new(0));
    let reader = thread::spawn({
        let handle = handle.clone();
        let stop = Arc::clone(&stop);
        let reads = Arc::clone(&reads);
        move || {
            while !stop.load(Ordering::Acquire) {
                match count_memories(&handle, &shadow_reader(Duration::from_secs(5))) {
                    Ok(_) => {
                        reads.fetch_add(1, Ordering::AcqRel);
                    }
                    // The reader's own wait window may elapse on a slow
                    // host; the writer's never does.
                    Err(HawDBError::RuntimeAdmissionRefused(_)) => {}
                    Err(error) => panic!("shadow read failed: {error}"),
                }
            }
        }
    });
    let replay = RuntimeTaskContext::default().with_admission_wait(Duration::from_secs(60));
    let replay_started = Instant::now();
    for commit in 0..COMMITS {
        handle
            .with_transaction_context(&replay, |tx| {
                tx.query_with_params(
                    "CREATE (:Memory {id: $id})",
                    &BTreeMap::from([("id".to_string(), Value::Int(commit as i64))]),
                )
                .map(|_| ())
            })
            .unwrap_or_else(|error| panic!("replay commit {commit} failed: {error}"));
        // A replay applies changes as they arrive, not back to back.
        thread::sleep(Duration::from_millis(1));
    }
    // Foreground waiters precede the reader only until it ages (100 ms); the
    // paced replay lasts at least 300 ms, so the reader progressed during it.
    let observed = reads.load(Ordering::Acquire);
    assert!(replay_started.elapsed() >= Duration::from_millis(300));
    assert!(observed > 0, "the shadow reader was starved during replay");
    // Let the reader observe the final state at least once.
    let started = Instant::now();
    while reads.load(Ordering::Acquire) == observed {
        assert!(started.elapsed() < PROMPT, "shadow reader made no progress");
        thread::yield_now();
    }
    stop.store(true, Ordering::Release);
    reader.join().unwrap();
    assert_eq!(
        count_memories(&handle, &RuntimeTaskContext::default()).unwrap(),
        COMMITS + 1
    );
    assert_idle(&governor);
}

/// Loops reads under `context`, retrying every retryable refusal at once,
/// until `stop`; returns (reads, refusals, `queued_ahead` refusals).
fn spawn_greedy_reader(
    handle: &EmbeddedStoreHandle,
    stop: &Arc<AtomicBool>,
    context: RuntimeTaskContext,
) -> thread::JoinHandle<(usize, usize, usize)> {
    let handle = handle.clone();
    let stop = Arc::clone(stop);
    thread::spawn(move || {
        let (mut reads, mut refusals, mut queued_ahead) = (0, 0, 0);
        while !stop.load(Ordering::Acquire) {
            match count_memories(&handle, &context) {
                Ok(_) => reads += 1,
                Err(HawDBError::RuntimeAdmissionRefused(refusal)) if refusal.retryable => {
                    refusals += 1;
                    queued_ahead += usize::from(refusal.code == "queued_ahead");
                }
                Err(error) => panic!("greedy read failed: {error}"),
            }
        }
        (reads, refusals, queued_ahead)
    })
}

fn replay_commits(handle: &EmbeddedStoreHandle, commits: usize) {
    replay_commits_with(
        handle,
        commits,
        &RuntimeTaskContext::default().with_admission_wait(Duration::from_secs(60)),
    );
}

fn replay_commits_with(handle: &EmbeddedStoreHandle, commits: usize, replay: &RuntimeTaskContext) {
    for commit in 0..commits {
        handle
            .with_transaction_context(replay, |tx| {
                tx.query_with_params(
                    "CREATE (:Memory {id: $id})",
                    &BTreeMap::from([("id".to_string(), Value::Int(commit as i64))]),
                )
                .map(|_| ())
            })
            .unwrap_or_else(|error| panic!("replay commit {commit} failed: {error}"));
    }
}

/// The starvation an embedding host reported: a Foreground reader loops
/// back to back, retrying each retryable refusal, beside a Foreground replay
/// writer on a one-CPU governor. A waiting writer is queued; the queued
/// waiter is selected ahead of every later immediate request (which yields
/// with `QueuedAhead`), so the writer finishes every commit.
#[test]
fn waiting_writer_is_not_starved_by_a_greedy_foreground_reader() {
    const COMMITS: usize = 300;
    let governor = one_cpu_governor();
    let handle = handle(&governor);
    let stop = Arc::new(AtomicBool::new(false));
    let reader = spawn_greedy_reader(&handle, &stop, RuntimeTaskContext::default());
    let started = Instant::now();
    replay_commits(&handle, COMMITS);
    assert!(started.elapsed() < PROMPT);
    stop.store(true, Ordering::Release);
    reader.join().unwrap();
    assert_eq!(
        count_memories(&handle, &RuntimeTaskContext::default()).unwrap(),
        COMMITS + 1
    );
    assert_idle(&governor);
}

/// When the reader waits too, both are same-priority waiters: the queue is
/// FIFO, so a writer re-queued after its commit runs after the reader that
/// queued during that commit, and both progress.
#[test]
fn waiting_reader_and_writer_alternate_in_fifo_order() {
    const COMMITS: usize = 300;
    let governor = one_cpu_governor();
    let handle = handle(&governor);
    let stop = Arc::new(AtomicBool::new(false));
    let reader = spawn_greedy_reader(
        &handle,
        &stop,
        RuntimeTaskContext::default().with_admission_wait(Duration::from_secs(60)),
    );
    let started = Instant::now();
    replay_commits(&handle, COMMITS);
    assert!(started.elapsed() < PROMPT);
    // Read the counter before stopping: the reader progressed during replay.
    let reader_progressed = {
        let probe = Instant::now();
        loop {
            if governor.snapshot().admissions > (COMMITS * 2) as u64 {
                break true;
            }
            if probe.elapsed() > PROMPT {
                break false;
            }
            thread::yield_now();
        }
    };
    stop.store(true, Ordering::Release);
    let (reads, refusals, _) = reader.join().unwrap();
    assert!(reader_progressed && reads > 0);
    assert_eq!(refusals, 0, "a waiting reader is never refused");
    assert_idle(&governor);
}

/// The multi-CPU variant: two greedy readers saturate a one-slot foreground
/// I/O depth (`io_saturated`), and a third reader that waits still completes
/// every read within the bound.
#[test]
fn waiting_reader_is_not_starved_by_greedy_readers_on_io_depth() {
    const READS: usize = 100;
    let governor = governor(7, 1);
    let handle = handle(&governor);
    let stop = Arc::new(AtomicBool::new(false));
    let greedy = [
        spawn_greedy_reader(&handle, &stop, RuntimeTaskContext::default()),
        spawn_greedy_reader(&handle, &stop, RuntimeTaskContext::default()),
    ];
    let waiting = RuntimeTaskContext::default().with_admission_wait(Duration::from_secs(60));
    let started = Instant::now();
    for read in 0..READS {
        count_memories(&handle, &waiting)
            .unwrap_or_else(|error| panic!("waiting read {read} failed: {error}"));
    }
    assert!(started.elapsed() < PROMPT);
    stop.store(true, Ordering::Release);
    for reader in greedy {
        reader.join().unwrap();
    }
    assert_idle(&governor);
}

/// The recommended replay writer: ranked Background, waiting, in the
/// Foreground pool.
fn background_replay() -> RuntimeTaskContext {
    RuntimeTaskContext::default()
        .with_admission_queue_priority(RuntimeWorkPriority::Background)
        .with_admission_wait(Duration::from_secs(60))
}

/// Host tiering, top tier. A replay writer ranked Background is queued for
/// the only CPU and has aged past the 100 ms boundary. By default it then
/// makes an immediate user request yield (`queued_ahead`); under strict
/// Foreground precedence the user request is refused only for the capacity
/// actually held, so it is admitted the moment the CPU is free. A shadow read
/// ranked Background yields to the queued replay either way.
#[test]
fn user_request_is_not_queued_behind_an_aged_background_ranked_replay() {
    let governor = tiered_governor();
    let handle = handle(&governor);
    let held = hold_cpu(&governor);
    let replay = thread::spawn({
        let handle = handle.clone();
        move || replay_commits_with(&handle, 1, &background_replay())
    });
    wait_for_queue(&governor, 1);
    // Let the queued replay age (100 ms).
    thread::sleep(Duration::from_millis(150));
    let user = || count_memories(&handle, &RuntimeTaskContext::default());
    governor.set_foreground_preempts_queued_background(false);
    assert!(matches!(
        user(),
        Err(HawDBError::RuntimeAdmissionRefused(refusal)) if refusal.code == "queued_ahead"
    ));
    governor.set_foreground_preempts_queued_background(true);
    assert!(matches!(
        user(),
        Err(HawDBError::RuntimeAdmissionRefused(refusal)) if refusal.code != "queued_ahead"
    ));
    let shadow = count_memories(
        &handle,
        &RuntimeTaskContext::default()
            .with_admission_queue_priority(RuntimeWorkPriority::Background),
    )
    .unwrap_err();
    assert!(matches!(
        shadow,
        HawDBError::RuntimeAdmissionRefused(refusal)
            if refusal.code == "queued_ahead" && refusal.retryable
    ));
    // The ranked replay stays in the Foreground pool.
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    drop(held);
    replay.join().unwrap();
    assert_eq!(user().unwrap(), 2);
    assert_idle(&governor);
}

/// Host tiering, lower tiers: a Background shadow reader looping without a
/// wait beside a Background replay writer that waits. The queued replay is
/// selected, so every immediate shadow read yields (`queued_ahead`) until
/// the replay is admitted, and the replay finishes every commit.
#[test]
fn background_replay_is_not_starved_by_a_greedy_background_shadow_reader() {
    const COMMITS: usize = 300;
    let governor = tiered_governor();
    let handle = handle(&governor);
    let stop = Arc::new(AtomicBool::new(false));
    let shadow = spawn_greedy_reader(
        &handle,
        &stop,
        RuntimeTaskContext::default()
            .with_admission_queue_priority(RuntimeWorkPriority::Background),
    );
    let started = Instant::now();
    replay_commits_with(&handle, COMMITS, &background_replay());
    assert!(started.elapsed() < PROMPT);
    stop.store(true, Ordering::Release);
    shadow.join().unwrap();
    assert_eq!(
        count_memories(&handle, &RuntimeTaskContext::default()).unwrap(),
        COMMITS + 1
    );
    assert_idle(&governor);
}

/// Host tiering: user requests looping as immediate Foreground work are
/// never refused with `queued_ahead` because a Background replay is queued;
/// their only refusals are for capacity another task holds.
#[test]
fn queued_background_replay_never_makes_user_requests_queue_ahead() {
    const ATTEMPTS: usize = 50;
    let governor = tiered_governor();
    let handle = handle(&governor);
    let stop = Arc::new(AtomicBool::new(false));
    let user = spawn_greedy_reader(&handle, &stop, RuntimeTaskContext::default());
    // Users outrank replay, so a replay attempt may expire; bound each one.
    let replay = RuntimeTaskContext::default()
        .with_admission_priority(RuntimeWorkPriority::Background)
        .with_admission_wait(Duration::from_millis(20));
    for commit in 0..ATTEMPTS {
        match handle.with_transaction_context(&replay, |tx| {
            tx.query_with_params(
                "CREATE (:Memory {id: $id})",
                &BTreeMap::from([("id".to_string(), Value::Int(commit as i64))]),
            )
            .map(|_| ())
        }) {
            Ok(()) => {}
            Err(HawDBError::RuntimeAdmissionRefused(refusal)) => {
                assert!(refusal.retryable && refusal.waited.is_some());
            }
            Err(error) => panic!("replay attempt {commit} failed: {error}"),
        }
    }
    stop.store(true, Ordering::Release);
    let (reads, refusals, queued_ahead) = user.join().unwrap();
    assert!(reads + refusals > 0, "the user loop never ran");
    assert_eq!(queued_ahead, 0, "a queued Background replay refused a user");
    assert_idle(&governor);
}

/// The mailbox path: a Foreground caller with a short admission deadline is
/// admitted as soon as a running Background task releases the shared CPU
/// slot, ahead of a queued Background replay, and sees how long it queued.
#[test]
fn short_foreground_wait_is_admitted_when_background_work_releases_the_slot() {
    let governor = one_cpu_governor();
    let handle = handle(&governor);
    // A Background task holds the shared CPU slot.
    let running = governor
        .try_admit(
            RuntimeWorkRequest::query(RuntimeWorkPriority::Background, 0, 0).with_blocking(false),
        )
        .unwrap();
    let replay = thread::spawn({
        let handle = handle.clone();
        move || replay_commits_with(&handle, 1, &background_replay())
    });
    wait_for_queue(&governor, 1);
    let trace = RuntimeAdmissionTrace::new();
    let mailbox = thread::spawn({
        let handle = handle.clone();
        let context = RuntimeTaskContext::default()
            .with_admission_wait(Duration::from_secs(10))
            .with_admission_trace(trace.clone());
        move || {
            let count = count_memories(&handle, &context);
            (count, Instant::now())
        }
    });
    wait_for_queue(&governor, 2);
    drop(running);
    let (count, mailbox_done) = mailbox.join().unwrap();
    replay.join().unwrap();
    // Admitted before the replay committed, unless the replay had already
    // aged past the mailbox (bounded to one replay admission).
    let count = count.unwrap();
    assert!(count == 1 || count == 2);
    assert!(mailbox_done.elapsed() < PROMPT);
    assert_eq!(trace.admissions(), 1);
    assert!(trace.queued_for() > Duration::ZERO);
    assert_idle(&governor);
}

/// Typed search takes the caller's policy too: a shadow-comparison search at
/// Background priority uses the Background pool (here held, so it queues)
/// while the same Foreground search is admitted at once, and the queued
/// search completes when the pool frees. A Background search reserves the
/// projection's seed I/O depth (2 here) from the Background I/O depth, so
/// that depth must be at least as large or the refusal is terminal.
#[test]
fn shadow_search_runs_at_background_priority_and_waits() {
    let governor = governor_with_io(1, 2, 2);
    let handle = handle(&governor);
    let request = crate::SearchCandidateRequest::text("seed", 1);
    let background_pool = governor
        .try_admit(RuntimeWorkRequest::new(
            RuntimeWorkPriority::Background,
            RuntimeWorkKind::Control,
        ))
        .unwrap();
    let trace = RuntimeAdmissionTrace::new();
    let shadow = thread::spawn({
        let handle = handle.clone();
        let request = request.clone();
        let context = shadow_reader(PROMPT).with_admission_trace(trace.clone());
        move || handle.search_candidates_with_report_context(&request, &context)
    });
    while governor.snapshot().queued_admission_waiters < 1 {
        assert!(
            !shadow.is_finished(),
            "{:?}",
            shadow.join().unwrap().map(|_| ())
        );
        thread::yield_now();
    }
    // The foreground search does not compete for the Background pool.
    let foreground = handle
        .search_candidates_with_report_context(&request, &RuntimeTaskContext::default())
        .unwrap();
    assert_eq!(foreground.accept_bounded().hits.len(), 1);
    drop(background_pool);
    assert_eq!(
        shadow.join().unwrap().unwrap().accept_bounded().hits.len(),
        1
    );
    assert_eq!(trace.admissions(), 1);
    assert!(trace.queued_for() > Duration::ZERO);
    let documents = handle
        .search_projection_documents_with_report_context(
            &["memory:seed".to_string()],
            1,
            &shadow_reader(PROMPT),
        )
        .unwrap();
    assert_eq!(documents.documents.len(), 1);
    assert_idle(&governor);
}

#[cfg(feature = "background-maintenance")]
mod recommended_tiering {
    //! The host tiering recommended for #1947 on a durable store whose writes
    //! trigger automatic checkpoints, with the default Background limit (one
    //! task below eight CPUs) on four CPUs:
    //!
    //! - user requests: default context (immediate, Foreground);
    //! - replay writer: queue rank Background, with a wait;
    //! - shadow reads: queue rank Background, with a wait, held open a while.
    //!
    //! Ranking never moves work into the Background pool, so the checkpoint
    //! the writer's commits depend on always finds its slot.

    use super::*;

    struct Directory(std::path::PathBuf);

    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn durable_handle(name: &str, governor: &RuntimeGovernor) -> (Directory, EmbeddedStoreHandle) {
        let directory = Directory(
            std::env::temp_dir().join(format!("hawdb-1947-tiering-{name}-{}", std::process::id())),
        );
        let _ = std::fs::remove_dir_all(&directory.0);
        std::fs::create_dir_all(&directory.0).unwrap();
        let mut database = Database::open_with_config(
            directory.0.join("graph"),
            DatabaseConfig {
                automatic_checkpoint_max_age: Duration::from_millis(20),
                max_read_result_rows: Some(16),
                max_read_result_payload_bytes: Some(16 * 1024),
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        database
            .query("CREATE (:Memory {id: 'seed', title: 'Seed memory'})")
            .unwrap();
        let graph = EmbeddedGraph::from_database_with_runtime_governor(
            database,
            GraphMode::WritableCutover,
            governor.clone(),
        );
        let handle = EmbeddedStoreHandle::new(EmbeddedStore::new(graph, None)).unwrap();
        (directory, handle)
    }

    fn four_cpus_default_background_limit() -> RuntimeGovernor {
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(2 << 30),
                result_budget_bytes: 128 << 20,
                ..RuntimeGovernorConfig::shared_host()
            },
            RuntimeResourceSnapshot::from_parts(
                RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
                RuntimeMemorySnapshot::from_limits(Some(8 << 30), Some(6 << 30), None, None, None),
            ),
            IoConcurrencyBudget::new(2, 1),
        );
        assert_eq!(governor.snapshot().limits.background_task_limit.get(), 1);
        governor.set_foreground_preempts_queued_background(true);
        governor
    }

    fn completed_checkpoints(handle: &EmbeddedStoreHandle) -> u64 {
        handle
            .read_store()
            .unwrap()
            .graph
            .database()
            .automatic_checkpoint_report()
            .unwrap()
            .map_or(0, |report| report.completed_checkpoints)
    }

    fn ranked(wait: Duration) -> RuntimeTaskContext {
        RuntimeTaskContext::default()
            .with_admission_queue_priority(RuntimeWorkPriority::Background)
            .with_admission_wait(wait)
    }

    fn run(name: &str, replay: RuntimeTaskContext) {
        const COMMITS: usize = 100;
        let governor = four_cpus_default_background_limit();
        let (_directory, handle) = durable_handle(name, &governor);
        let stop = Arc::new(AtomicBool::new(false));
        let shadow_reads = Arc::new(AtomicUsize::new(0));
        let shadows = (0..2)
            .map(|_| {
                let handle = handle.clone();
                let stop = Arc::clone(&stop);
                let shadow_reads = Arc::clone(&shadow_reads);
                thread::spawn(move || {
                    while !stop.load(Ordering::Acquire) {
                        // A long shadow read keeps its admission while open.
                        let read = handle.with_bounded_graph_read_snapshot_context(
                            budget(),
                            &ranked(PROMPT),
                            |snapshot| {
                                snapshot.query_cypher(
                                    "MATCH (m:Memory) RETURN count(m) AS memories",
                                    &BTreeMap::new(),
                                    1,
                                )?;
                                thread::sleep(Duration::from_millis(20));
                                Ok(())
                            },
                        );
                        match read {
                            Ok(()) => {
                                shadow_reads.fetch_add(1, Ordering::AcqRel);
                            }
                            Err(HawDBError::RuntimeAdmissionRefused(refusal))
                                if refusal.retryable => {}
                            Err(error) => panic!("shadow read failed: {error}"),
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        let user = spawn_greedy_reader(&handle, &stop, RuntimeTaskContext::default());
        let checkpoints = completed_checkpoints(&handle);
        let started = Instant::now();
        for commit in 0..COMMITS {
            handle
                .with_transaction_context(&replay, |tx| {
                    tx.query_with_params(
                        "CREATE (:Memory {id: $id})",
                        &BTreeMap::from([("id".to_string(), Value::Int(commit as i64))]),
                    )
                    .map(|_| ())
                })
                .unwrap_or_else(|error| panic!("replay commit {commit} failed: {error}"));
            thread::sleep(Duration::from_millis(2));
        }
        // Checkpoints keep completing beside the ranked work.
        while completed_checkpoints(&handle) <= checkpoints {
            assert!(
                started.elapsed() < PROMPT,
                "no automatic checkpoint completed"
            );
            thread::sleep(Duration::from_millis(5));
        }
        stop.store(true, Ordering::Release);
        for shadow in shadows {
            shadow.join().unwrap();
        }
        let (_, _, queued_ahead) = user.join().unwrap();
        assert_eq!(queued_ahead, 0, "ranked work made a user request queue");
        assert!(shadow_reads.load(Ordering::Acquire) > 0);
        assert_eq!(governor.snapshot().active_background_tasks, 0);
        assert_eq!(governor.snapshot().queued_admission_waiters, 0);
    }

    #[test]
    fn ranked_replay_commits_beside_long_shadow_reads_and_checkpoints() {
        run("ranked", ranked(Duration::from_secs(60)));
    }

    #[test]
    fn a_background_priority_writer_stays_out_of_the_background_pool() {
        // The earlier advice (`with_admission_priority(Background)`) no longer
        // puts a writer in the one-slot Background pool, where it would hold
        // the slot its own commit's checkpoint needs.
        run(
            "background-priority",
            RuntimeTaskContext::default()
                .with_admission_priority(RuntimeWorkPriority::Background)
                .with_admission_wait(Duration::from_secs(60)),
        );
    }
}

/// The self-deadlock precondition from review: a writer must never hold the
/// Background pool, because its commit can wait for an automatic checkpoint
/// that needs that pool (one task below eight CPUs). A context asking for
/// Background priority only lowers a transaction's queue rank.
#[test]
fn transactions_never_hold_the_background_pool() {
    let governor = one_cpu_governor();
    let handle = handle(&governor);
    let background = RuntimeTaskContext::default()
        .with_admission_priority(RuntimeWorkPriority::Background)
        .with_admission_wait(PROMPT);
    handle
        .with_transaction_context(&background, |tx| {
            let snapshot = governor.snapshot();
            assert_eq!(snapshot.active_background_tasks, 0);
            assert_eq!(snapshot.active_foreground_tasks, 1);
            tx.query("CREATE (:Memory {id: 'typed'})").map(|_| ())
        })
        .unwrap();
    let governor_view = governor.clone();
    let statements = [crate::NowledgeGraphStatement {
        cypher: "CREATE (:Memory {id: 'statement'})".into(),
        parameters: BTreeMap::new(),
    }];
    handle
        .transaction_context(&background, &statements)
        .unwrap();
    assert_eq!(governor_view.snapshot().active_background_tasks, 0);
    // Reads may still opt into the Background pool explicitly.
    handle
        .with_bounded_read_snapshot_context(budget(), &background, |_| {
            assert_eq!(governor.snapshot().active_background_tasks, 1);
            Ok(())
        })
        .unwrap();
    assert_idle(&governor);
}
