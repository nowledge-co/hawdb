// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A real commit parked on checkpoint capacity must not be held there by the
//! admission policy of handle callers (#1947 review): the owner's first
//! attempt is admitted and the parked commit succeeds.

use super::*;
use crate::embedded_store::{EmbeddedGraph, GraphMode};
use crate::embedded_store::{EmbeddedReadSnapshotBudget, EmbeddedStore, EmbeddedStoreHandle};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernorConfig, RuntimeMemorySnapshot, RuntimeResourceBudget,
    RuntimeResourceSnapshot, RuntimeWorkPriority,
};

const BOUND: Duration = Duration::from_secs(30);

/// Default limits for `cpus`: one Background task below eight CPUs.
fn host_governor(cpus: usize) -> RuntimeGovernor {
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 << 30),
            result_budget_bytes: 128 << 20,
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(
                std::num::NonZeroUsize::new(cpus).unwrap(),
                None,
                None,
            ),
            RuntimeMemorySnapshot::from_limits(Some(16 << 30), Some(16 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(1, 1),
    );
    assert_eq!(governor.snapshot().limits.background_task_limit.get(), 1);
    governor
}

fn ranked() -> RuntimeTaskContext {
    RuntimeTaskContext::default()
        .with_admission_queue_priority(RuntimeWorkPriority::Background)
        .with_admission_wait(BOUND)
}

/// `writer` commits through the handle while the owner is paused before its
/// first attempt and the overlay is near capacity. `shadow` optionally holds a
/// read open throughout; `queue_b` queues a caller short of Foreground I/O.
fn parked_writer_commits(
    cpus: usize,
    writer: RuntimeTaskContext,
    shadow: Option<RuntimeTaskContext>,
    queue_b: bool,
) {
    let fixture = Fixture::new();
    let (mut db, suspension, mut next) = seeded(&fixture);
    let control = db.runtime.checkpoint_control_for_test();
    while !preparation_needs_capacity(&control.lock().unwrap()) {
        create(&mut db, next).unwrap();
        next += 1;
    }
    let governor = host_governor(cpus);
    let graph = EmbeddedGraph::from_database_with_runtime_governor(
        db,
        GraphMode::WritableCutover,
        governor.clone(),
    );
    let handle = EmbeddedStoreHandle::new(EmbeddedStore::new(graph, None)).unwrap();

    let (release_read, read_released) = mpsc::channel::<()>();
    let shadow = shadow.map(|context| {
        let (opened, open) = mpsc::channel();
        let handle = handle.clone();
        let reader = std::thread::spawn(move || {
            handle.with_bounded_graph_read_snapshot_context(
                EmbeddedReadSnapshotBudget {
                    max_rows: 1,
                    max_payload_bytes: 4096,
                },
                &context,
                |_| {
                    opened.send(()).unwrap();
                    read_released.recv_timeout(BOUND).unwrap();
                    Ok(())
                },
            )
        });
        open.recv_timeout(BOUND).unwrap();
        reader
    });

    let (probe, paused, resume) = pause();
    let (waiting, waited) = mpsc::channel();
    let before = {
        let mut state = control.lock().unwrap();
        state.idle_start_probe = Some(probe);
        state.frontend_wait_probe = Some(waiting);
        state.report.clone()
    };
    drop(suspension);
    paused.recv_timeout(BOUND).unwrap();
    let (finished, committed) = mpsc::channel();
    let producer = std::thread::spawn({
        let handle = handle.clone();
        move || {
            let result = handle.with_transaction_context(&writer, |tx| {
                tx.query_with_params(
                    "CREATE (:Memory {id: $id, body: $body})",
                    &std::collections::BTreeMap::from([
                        ("id".into(), crate::Value::Int(next)),
                        // Small: near capacity, the statement must still be
                        // accepted so that the commit is what waits.
                        ("body".into(), crate::Value::String("b".into())),
                    ]),
                )
                .map(|_| ())
            });
            finished.send(result).unwrap();
        }
    });
    // A is admitted and parked on the owner, not merely queued for admission.
    if waited.recv_timeout(Duration::from_secs(10)).is_err() {
        let state = control.lock().unwrap();
        panic!(
            "writer did not park: result {:?}, phase {:?}, enabled {}, suspensions {}, pressure {}, headroom {}, capacity {}, governor {:?}",
            committed.try_recv(), state.phase, state.enabled, state.suspensions,
            pressure_pending(&state), needs_headroom(&state), preparation_needs_capacity(&state),
            governor.snapshot(),
        );
    }
    assert!(governor.snapshot().active_cpu_slots >= 1);
    assert!(matches!(
        committed.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    let queued = queue_b.then(|| {
        let io = RuntimeWorkRequest::io(RuntimeWorkPriority::Foreground, 1, 0)
            .with_cpu_slots(0)
            .with_blocking(false);
        let holder = governor.try_admit(io).unwrap();
        let waiter = governor.admission_waiter(RuntimeWorkPriority::Foreground);
        assert!(governor
            .try_admit_waiter(&waiter, io.with_cpu_slots(1))
            .is_err());
        assert_eq!(governor.snapshot().queued_admission_waiters, 1);
        (holder, waiter)
    });
    resume.send(()).unwrap();
    let result = committed.recv_timeout(BOUND);
    let report = control.lock().unwrap().report.clone();
    // Release every test-only holder before judging the outcome.
    control.lock().unwrap().frontend_wait_probe = None;
    drop(queued);
    let _ = release_read.send(());
    producer.join().unwrap();
    if let Some(reader) = shadow {
        reader.join().unwrap().unwrap();
    }
    result.expect("the parked commit stalled").unwrap();
    assert_eq!(
        report.deferred_attempts, before.deferred_attempts,
        "{report:?}"
    );
    assert!(
        report.completed_checkpoints > before.completed_checkpoints,
        "{report:?}"
    );
}

#[test]
fn recommended_replay_writer_commits_on_four_cpus() {
    parked_writer_commits(4, ranked(), None, false);
}

#[test]
fn background_priority_writer_does_not_hold_its_checkpoints_slot() {
    parked_writer_commits(
        4,
        RuntimeTaskContext::default()
            .with_admission_priority(RuntimeWorkPriority::Background)
            .with_admission_wait(BOUND),
        None,
        false,
    );
}

#[test]
fn long_ranked_shadow_read_does_not_block_the_writers_checkpoint() {
    parked_writer_commits(4, ranked(), Some(ranked()), false);
}

#[test]
fn caller_queued_for_io_does_not_block_the_writers_checkpoint_on_two_cpus() {
    parked_writer_commits(2, ranked(), None, true);
}
