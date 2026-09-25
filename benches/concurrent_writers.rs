// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Local fixed-work MVCC qualification. The serialized control holds a host
//! mutex across the same transaction lifecycle; it is not a historical binary.

use hawdb::{
    ConcurrentDatabase, ConcurrentTransactionOptions, Database, Value, WalGroupCommitConfig,
};
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::sync::{Barrier, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SMOKE: bool = cfg!(debug_assertions);
const COMMITS: usize = if SMOKE { 16 } else { 256 };
const ROUNDS: usize = if SMOKE { 1 } else { 5 };
const UPDATE: &str = "MATCH (n:MvccCounter) WHERE n.id = $id SET n.value = 1";
const READ: &str = "MATCH (n:MvccCounter) RETURN n.id AS id, n.value AS value ORDER BY id";

fn nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos().try_into().unwrap()
}

fn latency(samples: &[u64]) -> JsonValue {
    assert!(!samples.is_empty());
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    json!({
        "count": sorted.len(),
        "p50_ns": sorted[(sorted.len() * 50).div_ceil(100) - 1],
        "p95_ns": sorted[(sorted.len() * 95).div_ceil(100) - 1],
        "max_ns": sorted.last().unwrap(),
        "samples_ns": samples,
    })
}

fn expected(value: i64) -> Vec<BTreeMap<String, Value>> {
    (0..COMMITS)
        .map(|id| {
            BTreeMap::from([
                ("id".into(), Value::Int(id.try_into().unwrap())),
                ("value".into(), Value::Int(value)),
            ])
        })
        .collect()
}

fn measure(path: &Path, durable: bool, grouped: bool, writers: usize, serial: bool) -> JsonValue {
    let database = if durable {
        Database::open(path).unwrap()
    } else {
        Database::new()
    };
    let config = if grouped {
        WalGroupCommitConfig::benchmark_candidate(
            NonZeroUsize::new(8).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            Duration::from_micros(100),
        )
        .unwrap()
    } else {
        WalGroupCommitConfig::disabled()
    };
    let database = ConcurrentDatabase::new_with_wal_group_commit(database, config);
    let mut seed = database
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    for id in 0..COMMITS {
        seed.query_with_params(
            "CREATE (:MvccCounter {id: $id, value: 0})",
            &BTreeMap::from([("id".into(), Value::Int(id.try_into().unwrap()))]),
        )
        .unwrap();
    }
    seed.commit().unwrap();
    database.checkpoint().unwrap();
    assert_eq!(database.query(READ).unwrap().rows, expected(0));
    let initial_epoch = database.commit_epoch().unwrap();
    let before = database.wal_group_commit_snapshot().unwrap();
    let serial_gate = Mutex::new(());
    // Threads and per-request parameters are prepared before the timed start.
    let ready = Barrier::new(writers + 1);
    let go = Barrier::new(writers + 1);
    let (elapsed, workers) = std::thread::scope(|scope| {
        let handles = (0..writers)
            .map(|worker| {
                let database = &database;
                let serial_gate = &serial_gate;
                let ready = &ready;
                let go = &go;
                scope.spawn(move || {
                    let requests = (worker..COMMITS)
                        .step_by(writers)
                        .map(|id| {
                            BTreeMap::from([("id".into(), Value::Int(id.try_into().unwrap()))])
                        })
                        .collect::<Vec<_>>();
                    let mut transaction_latencies = Vec::with_capacity(requests.len());
                    let mut commit_latencies = Vec::with_capacity(requests.len());
                    ready.wait();
                    go.wait();
                    let worker_started = Instant::now();
                    for params in requests {
                        let started = Instant::now();
                        let _guard = serial.then(|| serial_gate.lock().unwrap());
                        let mut tx = database
                            .begin_transaction(ConcurrentTransactionOptions::optimistic())
                            .unwrap();
                        tx.query_with_params(UPDATE, &params).unwrap();
                        let commit_started = Instant::now();
                        // Disjoint keys must commit once, without hidden retries.
                        tx.commit().unwrap();
                        commit_latencies.push(nanos(commit_started));
                        transaction_latencies.push(nanos(started));
                    }
                    (
                        worker,
                        nanos(worker_started),
                        transaction_latencies,
                        commit_latencies,
                    )
                })
            })
            .collect::<Vec<_>>();
        ready.wait();
        let started = Instant::now();
        go.wait();
        let workers = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        (nanos(started), workers)
    });
    let after = database.wal_group_commit_snapshot().unwrap();
    if grouped {
        assert_eq!(
            after.submitted_commits - before.submitted_commits,
            COMMITS as u64
        );
        assert_eq!(
            after.completed_commits - before.completed_commits,
            COMMITS as u64
        );
        assert_eq!(
            after.grouped_wal_entries - before.grouped_wal_entries,
            COMMITS as u64
        );
        assert!(
            (1..=COMMITS as u64).contains(&(after.shared_sync_count - before.shared_sync_count))
        );
    }
    assert_eq!(
        database.commit_epoch().unwrap() - initial_epoch,
        COMMITS as u64
    );
    assert_eq!(database.query(READ).unwrap().rows, expected(1));
    let transaction_samples = workers
        .iter()
        .flat_map(|w| w.2.iter().copied())
        .collect::<Vec<_>>();
    let commit_samples = workers
        .iter()
        .flat_map(|w| w.3.iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(transaction_samples.len(), COMMITS);
    drop(database);
    if durable {
        let mut reopened = Database::open(path).unwrap();
        assert_eq!(reopened.commit_epoch(), initial_epoch + COMMITS as u64);
        assert_eq!(reopened.query(READ).unwrap().rows, expected(1));
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
    json!({
        "durable": durable, "grouped": grouped, "writers": writers, "serialized_control": serial,
        "commits": COMMITS, "elapsed_ns": elapsed,
        "commits_per_second": COMMITS as f64 * 1e9 / elapsed as f64,
        "transaction_latency": latency(&transaction_samples),
        "commit_latency": latency(&commit_samples),
        "workers": workers.iter().map(|w| json!({
            "worker": w.0, "elapsed_ns": w.1, "commits": w.2.len(),
        })).collect::<Vec<_>>(),
        "group_commit": {
            "submitted": after.submitted_commits - before.submitted_commits,
            "completed": after.completed_commits - before.completed_commits,
            "shared_syncs": after.shared_sync_count - before.shared_sync_count,
            "grouped_entries": after.grouped_wal_entries - before.grouped_wal_entries,
            "fsync_micros": after.total_fsync_micros - before.total_fsync_micros,
        },
        "verified_rows": COMMITS, "reopen_verified": durable,
    })
}

fn main() {
    let mut selected_round = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bench" => {} // Cargo supplies this for harness-free benchmarks.
            "--round" => {
                assert!(selected_round.is_none(), "--round may appear only once");
                let round = args
                    .next()
                    .expect("--round requires an index")
                    .parse::<usize>()
                    .expect("round must be an unsigned integer");
                assert!(round < ROUNDS, "round must be below {ROUNDS}");
                selected_round = Some(round);
            }
            _ => panic!("unknown benchmark argument: {arg}"),
        }
    }
    let root = std::env::temp_dir().join(format!(
        "hawdb-concurrent-writers-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    for round in 0..ROUNDS {
        if selected_round.is_some_and(|selected| selected != round) {
            continue;
        }
        // Reverse case order every round to reduce consistent order bias.
        let mut cases = Vec::new();
        for (durable, grouped) in [(false, false), (true, false), (true, true)] {
            for writers in [1, 4, 8] {
                for serial in [true, false] {
                    cases.push((durable, grouped, writers, serial));
                }
            }
        }
        if round % 2 == 1 {
            cases.reverse();
        }
        for (case, (durable, grouped, writers, serial)) in cases.into_iter().enumerate() {
            let result = measure(
                &root.join(format!("{round}-{case}")),
                durable,
                grouped,
                writers,
                serial,
            );
            println!(
                "{}",
                json!({"schema": "concurrent-writers-v1", "smoke": SMOKE, "round": round, "result": result})
            );
        }
    }
    std::fs::remove_dir(root).unwrap();
}
