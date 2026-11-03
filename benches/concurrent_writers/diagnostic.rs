// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Instrumented diagnosis, deliberately separate from performance acceptance.

use super::{expected, latency, nanos, readers, COMMITS, READ, SMOKE, UPDATE};
use hawdb::{
    ConcurrentDatabase, ConcurrentDatabaseMetrics, ConcurrentPhaseMetrics,
    ConcurrentTransactionOptions, Database, Value, WalGroupCommitConfig,
};
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::sync::{Barrier, Mutex};
use std::time::Instant;

const WRITERS: usize = 2;

struct Transaction {
    id: usize,
    boundaries: [u64; 5],
}

fn phase(before: ConcurrentPhaseMetrics, after: ConcurrentPhaseMetrics) -> JsonValue {
    json!({
        "count": after.count.checked_sub(before.count).unwrap(),
        "total_nanos": after.total_nanos.checked_sub(before.total_nanos).unwrap(),
    })
}

fn metrics(before: ConcurrentDatabaseMetrics, after: ConcurrentDatabaseMetrics) -> JsonValue {
    assert!(before.enabled && after.enabled);
    json!({
        "sequencer_wait": phase(before.sequencer_wait, after.sequencer_wait),
        "sequencer_hold": phase(before.sequencer_hold, after.sequencer_hold),
        "commit_execution": phase(before.commit_execution, after.commit_execution),
        "group_durability": phase(before.group_durability, after.group_durability),
        "read_publication": phase(before.read_publication, after.read_publication),
        "graph_statement_executions": after.graph_statement_executions
            .checked_sub(before.graph_statement_executions).unwrap(),
        "graph_statement_replays": after.graph_statement_replays
            .checked_sub(before.graph_statement_replays).unwrap(),
        "graph_statement_reuses": after.graph_statement_reuses
            .checked_sub(before.graph_statement_reuses).unwrap(),
    })
}

fn measure(reader_count: usize, serialized: bool) -> JsonValue {
    let database = ConcurrentDatabase::new_with_wal_group_commit(
        Database::new(),
        WalGroupCommitConfig::disabled(),
    );
    let mut seed = database
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    for id in 0..COMMITS {
        seed.query_with_params(
            if reader_count == 0 {
                "CREATE (:MvccCounter {id: $id, value: 0})"
            } else {
                "CREATE (:MvccCounter {id: $id, value: 0, mirror: 0})"
            },
            &BTreeMap::from([("id".into(), Value::Int(id.try_into().unwrap()))]),
        )
        .unwrap();
    }
    seed.commit().unwrap();
    database.checkpoint().unwrap();
    assert_eq!(database.query(READ).unwrap().rows, expected(0));
    let initial_epoch = database.commit_epoch().unwrap();
    let mut pinned = (reader_count > 0).then(|| database.begin_read_transaction().unwrap());
    if let Some(snapshot) = &mut pinned {
        readers::verify(&snapshot.query(readers::READ).unwrap(), 0);
    }
    database.enable_concurrency_metrics();
    let before = database.concurrency_metrics();
    let host_gate = Mutex::new(());
    let ready = Barrier::new(WRITERS + reader_count + 1);
    let go = Barrier::new(WRITERS + reader_count + 1);
    let origin = Instant::now();
    let (elapsed, transactions, reads) = std::thread::scope(|scope| {
        let workers = (0..WRITERS)
            .map(|worker| {
                let database = &database;
                let host_gate = &host_gate;
                let ready = &ready;
                let go = &go;
                scope.spawn(move || {
                    let requests = (worker..COMMITS)
                        .step_by(WRITERS)
                        .map(|id| {
                            (
                                id,
                                BTreeMap::from([("id".into(), Value::Int(id.try_into().unwrap()))]),
                            )
                        })
                        .collect::<Vec<_>>();
                    let mut transactions = Vec::with_capacity(requests.len());
                    ready.wait();
                    go.wait();
                    for (id, params) in requests {
                        let started = nanos(origin);
                        let _guard = serialized.then(|| host_gate.lock().unwrap());
                        let begin_started = nanos(origin);
                        let mut tx = database
                            .begin_transaction(ConcurrentTransactionOptions::optimistic())
                            .unwrap();
                        let query_started = nanos(origin);
                        tx.query_with_params(
                            if reader_count == 0 {
                                UPDATE
                            } else {
                                readers::UPDATE
                            },
                            &params,
                        )
                        .unwrap();
                        let commit_started = nanos(origin);
                        tx.commit().unwrap();
                        let finished = nanos(origin);
                        transactions.push(Transaction {
                            id,
                            boundaries: [
                                started,
                                begin_started,
                                query_started,
                                commit_started,
                                finished,
                            ],
                        });
                    }
                    transactions
                })
            })
            .collect::<Vec<_>>();
        let read_workers = (0..reader_count)
            .map(|worker| {
                let database = &database;
                let ready = &ready;
                let go = &go;
                scope.spawn(move || {
                    readers::measure(database, worker, ready, go, origin, initial_epoch)
                })
            })
            .collect::<Vec<_>>();
        ready.wait();
        let started = Instant::now();
        go.wait();
        let transactions = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        let reads = read_workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        (nanos(started), transactions, reads)
    });
    let after = database.concurrency_metrics();
    assert_eq!(transactions.len(), COMMITS);
    let mut ids = transactions.iter().map(|tx| tx.id).collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, (0..COMMITS).collect::<Vec<_>>());
    assert_eq!(
        database.commit_epoch().unwrap(),
        initial_epoch + COMMITS as u64
    );
    assert_eq!(database.query(READ).unwrap().rows, expected(1));
    if let Some(snapshot) = &mut pinned {
        assert_eq!(snapshot.commit_epoch(), initial_epoch);
        readers::verify(&snapshot.query(readers::READ).unwrap(), 0);
        readers::verify(&database.query(readers::READ).unwrap(), COMMITS as u64);
    }
    let mut phases: [Vec<u64>; 4] = std::array::from_fn(|_| Vec::with_capacity(COMMITS));
    let mut totals = Vec::with_capacity(COMMITS);
    let mut intervals = Vec::with_capacity(COMMITS);
    for tx in &transactions {
        let mut total = 0;
        for (samples, pair) in phases.iter_mut().zip(tx.boundaries.windows(2)) {
            let elapsed = pair[1].checked_sub(pair[0]).unwrap();
            samples.push(elapsed);
            total += elapsed;
        }
        assert_eq!(total, tx.boundaries[4] - tx.boundaries[0]);
        totals.push(total);
        intervals.push((tx.boundaries[0], tx.boundaries[4]));
    }
    json!({
        "schema": "sort-writer-phase-diagnostic-v1",
        "purpose": "instrumented diagnosis only; not performance acceptance",
        "debug_assertions": SMOKE,
        "durable": false, "grouped": false, "writers": WRITERS,
        "readers": reader_count, "serialized_control": serialized,
        "commits": COMMITS, "elapsed_ns": elapsed,
        "transaction_latency": latency(&totals),
        "host_gate": latency(&phases[0]),
        "begin": latency(&phases[1]),
        "query": latency(&phases[2]),
        "commit": latency(&phases[3]),
        "coordinator": metrics(before, after),
        "coordinator_semantics": "cumulative overlapping phases; never sum or infer per-commit quantiles",
        "transaction_boundary_order": ["start", "begin", "query", "commit", "finish"],
        "transactions": transactions.iter().map(|tx| json!({
            "id": tx.id, "boundaries_ns": tx.boundaries,
        })).collect::<Vec<_>>(),
        "read_workload": readers::report(&reads, &intervals, elapsed, initial_epoch),
        "verified_rows": COMMITS, "pinned_snapshot_verified": reader_count > 0,
    })
}

pub(super) fn run() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert!(args
        .iter()
        .all(|arg| arg == "--phase-diagnostic" || arg == "--bench"));
    assert_eq!(
        args.iter()
            .filter(|arg| arg.as_str() == "--phase-diagnostic")
            .count(),
        1
    );
    for readers in [0, 2] {
        for serialized in [false, true] {
            println!("{}", measure(readers, serialized));
        }
    }
}
