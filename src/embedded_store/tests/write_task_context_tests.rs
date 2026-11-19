// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Write transactions under a caller `RuntimeTaskContext` (#1821). The
//! deadline and cancellation are honored at admission, while waiting for
//! another writer's locks, during statements and just before commit. A stop
//! is the typed, non-poisoning `HawDBError::RuntimeTaskStopped`, rolls back,
//! and leaves the handle usable. Once the commit is applying it is never
//! interrupted.

use super::*;
use crate::{RuntimeCancellationReason, RuntimeCancellationToken, RuntimeTaskContext};
use std::sync::Arc;
use std::time::Instant;

fn handle() -> EmbeddedStoreHandle {
    let mut database = Database::new();
    database.query("CREATE (:Memory {id: 'x', v: 0})").unwrap();
    let graph = EmbeddedGraph::from_database(database, GraphMode::WritableCutover);
    EmbeddedStoreHandle::new(EmbeddedStore::new(graph, None)).unwrap()
}

fn ids(handle: &EmbeddedStoreHandle) -> Vec<Value> {
    handle
        .query_with_report("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
        .unwrap()
        .output
        .rows
        .iter()
        .map(|row| row["id"].clone())
        .collect()
}

fn value(handle: &EmbeddedStoreHandle) -> Value {
    handle
        .query_with_report("MATCH (m:Memory {id: 'x'}) RETURN m.v AS v")
        .unwrap()
        .output
        .rows[0]["v"]
        .clone()
}

fn cancellable() -> (RuntimeCancellationToken, RuntimeTaskContext) {
    let token = RuntimeCancellationToken::new();
    let context = RuntimeTaskContext::without_deadline(token.clone());
    (token, context)
}

fn assert_stopped<T: std::fmt::Debug>(result: crate::Result<T>, reason: RuntimeCancellationReason) {
    match result {
        Err(HawDBError::RuntimeTaskStopped(actual)) => assert_eq!(actual, reason),
        other => panic!("expected a typed {reason} stop, got {other:?}"),
    }
}

/// The handle still accepts a write and reports it.
fn assert_usable(handle: &EmbeddedStoreHandle, id: &str) {
    handle
        .with_transaction(|transaction| {
            transaction.query_with_params(
                "CREATE (:Memory {id: $id})",
                &BTreeMap::from([("id".to_string(), Value::String(id.to_string()))]),
            )
        })
        .unwrap();
    assert!(ids(handle).contains(&Value::String(id.to_string())));
}

#[test]
fn a_stopped_task_is_refused_before_admission() {
    let handle = handle();
    let (token, context) = cancellable();
    token.cancel();
    let mut ran = false;
    assert_stopped(
        handle.with_transaction_context(&context, |_| {
            ran = true;
            Ok(())
        }),
        RuntimeCancellationReason::Cancelled,
    );
    assert!(!ran, "the callback must not run");
    let expired = RuntimeTaskContext::new(RuntimeCancellationToken::new(), Some(Instant::now()));
    assert_stopped(
        handle.transaction_context(
            &expired,
            &[NowledgeGraphStatement {
                cypher: "CREATE (:Memory {id: 'late'})".to_string(),
                parameters: BTreeMap::new(),
            }],
        ),
        RuntimeCancellationReason::DeadlineExceeded,
    );
    assert_eq!(ids(&handle), vec![Value::String("x".into())]);
    assert_eq!(
        handle
            .runtime_governor_snapshot()
            .unwrap()
            .admitted_memory_bytes,
        0
    );
    assert_usable(&handle, "after");
}

#[test]
fn the_deadline_ends_a_wait_for_another_writers_locks() {
    let handle = handle();
    let (locked, wait_locked) = mpsc::channel();
    let (release, wait_release) = mpsc::channel::<()>();
    let holder = {
        let handle = handle.clone();
        thread::spawn(move || {
            handle.with_transaction(|transaction| {
                transaction.query("MATCH (m:Memory {id: 'x'}) SET m.v = 1")?;
                locked.send(()).unwrap();
                wait_release.recv().unwrap();
                Ok(())
            })
        })
    };
    wait_locked.recv().unwrap();

    // The default lock timeout is seconds; the caller's deadline ends the
    // wait for the holder's lock long before that.
    let started = Instant::now();
    let deadline = RuntimeTaskContext::new(
        RuntimeCancellationToken::new(),
        Some(started + Duration::from_millis(150)),
    );
    let mut ran_after_lock = false;
    let result = handle.with_transaction_context(&deadline, |transaction| {
        transaction.query("MATCH (m:Memory {id: 'x'}) SET m.v = 2")?;
        ran_after_lock = true;
        Ok(())
    });
    let waited = started.elapsed();
    assert_stopped(result, RuntimeCancellationReason::DeadlineExceeded);
    assert!(!ran_after_lock);
    assert!(waited < Duration::from_secs(2), "waited {waited:?}");

    // Cancellation ends such a wait too.
    let (token, context) = cancellable();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        token.cancel();
    });
    assert_stopped(
        handle.with_transaction_context(&context, |transaction| {
            transaction.query("MATCH (m:Memory {id: 'x'}) SET m.v = 3")
        }),
        RuntimeCancellationReason::Cancelled,
    );
    canceller.join().unwrap();

    // The holder was never disturbed and commits; the stopped writers left
    // nothing behind.
    release.send(()).unwrap();
    holder.join().unwrap().unwrap();
    assert_eq!(value(&handle), Value::Int(1));
    handle
        .with_transaction_context(&RuntimeTaskContext::default(), |transaction| {
            transaction.query("MATCH (m:Memory {id: 'x'}) SET m.v = 2")
        })
        .unwrap();
    assert_eq!(value(&handle), Value::Int(2));
}

#[test]
fn a_stop_during_statements_or_before_commit_rolls_back() {
    let handle = handle();
    // Between statements: the next statement stops and everything staged so
    // far is rolled back.
    let (token, context) = cancellable();
    assert_stopped(
        handle.with_transaction_context(&context, |transaction| {
            transaction.query("CREATE (:Memory {id: 'staged'})")?;
            token.cancel();
            transaction.query("CREATE (:Memory {id: 'second'})")
        }),
        RuntimeCancellationReason::Cancelled,
    );
    // After the last statement, before commit: the commit is refused.
    let (token, context) = cancellable();
    assert_stopped(
        handle.with_transaction_context(&context, |transaction| {
            transaction.query("CREATE (:Memory {id: 'uncommitted'})")?;
            token.cancel();
            Ok(())
        }),
        RuntimeCancellationReason::Cancelled,
    );
    assert_eq!(ids(&handle), vec![Value::String("x".into())]);
    assert_eq!(
        handle
            .runtime_governor_snapshot()
            .unwrap()
            .admitted_memory_bytes,
        0
    );
    assert_usable(&handle, "after");
}

#[test]
fn a_stop_after_the_commit_boundary_does_not_interrupt_the_commit() {
    let handle = handle();
    let (token, context) = cancellable();
    let token = Arc::new(token);
    let cancel = Arc::clone(&token);
    crate::api::set_commit_boundary_hook(Box::new(move || {
        cancel.cancel();
    }));
    handle
        .with_transaction_context(&context, |transaction| {
            transaction.query("CREATE (:Memory {id: 'durable'})")
        })
        .unwrap();
    assert!(token.is_cancelled(), "the hook ran inside the commit");
    assert!(ids(&handle).contains(&Value::String("durable".into())));
    assert_usable(&handle, "after");
}
