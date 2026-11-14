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

#[path = "concurrent_writers/readers.rs"]
mod readers;

const SMOKE: bool = cfg!(debug_assertions);
const COMMITS: usize = if SMOKE { 16 } else { 256 };
const ROUNDS: usize = if SMOKE { 1 } else { 5 };
const UPDATE: &str = "MATCH (n:MvccCounter) WHERE n.id = $id SET n.value = 1";
const READ: &str = "MATCH (n:MvccCounter) RETURN n.id AS id, n.value AS value ORDER BY id";

// The original gate baseline returned a bare epoch from Database. Keep one
// identical harness across that API change; this oracle is outside timing.
trait ReopenedEpoch {
    fn checked(self) -> u64;
}
impl ReopenedEpoch for u64 {
    fn checked(self) -> u64 {
        self
    }
}
impl ReopenedEpoch for hawdb::Result<u64> {
    fn checked(self) -> u64 {
        self.unwrap()
    }
}

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
        "p99_ns": sorted[(sorted.len() * 99).div_ceil(100) - 1],
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum WalFixture {
    PostCheckpoint,
    Reopened,
    AfterRelaxed,
}

fn measure(
    path: &Path,
    durable: bool,
    grouped: bool,
    writers: usize,
    serial: bool,
    fixture: WalFixture,
    readers: usize,
) -> JsonValue {
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
            if readers == 0 {
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
    let reopened_before_measure = durable && fixture != WalFixture::PostCheckpoint;
    let mut relaxed_append_ns = None;
    let database = if reopened_before_measure {
        // Checkpoint rotation clears the captured WAL owner. Reopen before
        // timing to exercise captured append fences on every measured commit.
        drop(database);
        if fixture == WalFixture::AfterRelaxed {
            let mut relaxed =
                Database::open_with_durability(path, hawdb::DurabilityPolicy::SyncOnCheckpoint)
                    .unwrap();
            relaxed.commit_epoch().unwrap();
            // Keep counter rows unchanged while admitting a real relaxed WAL
            // interval. Measure its first-append fence separately from commits.
            let started = Instant::now();
            relaxed.query("CREATE (:FenceSeed {id: 0})").unwrap();
            relaxed_append_ns = Some(nanos(started));
            drop(relaxed);
        }
        ConcurrentDatabase::new_with_wal_group_commit(Database::open(path).unwrap(), config)
    } else {
        database
    };
    assert_eq!(database.query(READ).unwrap().rows, expected(0));
    let initial_epoch = database.commit_epoch().unwrap();
    let mut pinned = (readers > 0).then(|| database.begin_read_transaction().unwrap());
    if let Some(snapshot) = &mut pinned {
        assert_eq!(snapshot.commit_epoch(), initial_epoch);
        readers::verify(&snapshot.query(readers::READ).unwrap(), 0);
    }
    let before = database.wal_group_commit_snapshot().unwrap();
    let serial_gate = Mutex::new(());
    // Threads and per-request parameters are prepared before the timed start.
    let ready = Barrier::new(writers + readers + 1);
    let go = Barrier::new(writers + readers + 1);
    let origin = Instant::now();
    let (elapsed, workers, reader_measurements) = std::thread::scope(|scope| {
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
                    let mut intervals = Vec::with_capacity(requests.len());
                    ready.wait();
                    go.wait();
                    let worker_started = Instant::now();
                    for params in requests {
                        let started = Instant::now();
                        let _guard = serial.then(|| serial_gate.lock().unwrap());
                        let mut tx = database
                            .begin_transaction(ConcurrentTransactionOptions::optimistic())
                            .unwrap();
                        tx.query_with_params(
                            if readers == 0 {
                                UPDATE
                            } else {
                                readers::UPDATE
                            },
                            &params,
                        )
                        .unwrap();
                        let commit_started = Instant::now();
                        // Disjoint keys must commit once, without hidden retries.
                        tx.commit().unwrap();
                        commit_latencies.push(nanos(commit_started));
                        transaction_latencies.push(nanos(started));
                        if readers > 0 {
                            intervals.push((
                                started
                                    .duration_since(origin)
                                    .as_nanos()
                                    .try_into()
                                    .unwrap(),
                                nanos(origin),
                            ));
                        }
                    }
                    (
                        worker,
                        nanos(worker_started),
                        transaction_latencies,
                        commit_latencies,
                        intervals,
                    )
                })
            })
            .collect::<Vec<_>>();
        let reader_handles = (0..readers)
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
        let workers = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        let reader_measurements = reader_handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        (nanos(started), workers, reader_measurements)
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
    if let Some(snapshot) = &mut pinned {
        assert_eq!(snapshot.commit_epoch(), initial_epoch);
        readers::verify(&snapshot.query(readers::READ).unwrap(), 0);
        readers::verify(&database.query(readers::READ).unwrap(), COMMITS as u64);
    }
    drop(pinned);
    let writes = workers
        .iter()
        .flat_map(|w| w.4.iter().copied())
        .collect::<Vec<_>>();
    let read_report = readers::report(&reader_measurements, &writes, elapsed, initial_epoch);
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
        let mut reopened = Database::open_with_config(
            path,
            hawdb::DatabaseConfig {
                recovery_mode: hawdb::RecoveryMode::Strict,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            reopened.commit_epoch().checked(),
            initial_epoch + COMMITS as u64
        );
        assert_eq!(reopened.query(READ).unwrap().rows, expected(1));
        if readers > 0 {
            readers::verify(&reopened.query(readers::READ).unwrap(), COMMITS as u64);
        }
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
    json!({
        "durable": durable, "grouped": grouped, "writers": writers, "serialized_control": serial,
        "readers": readers, "read_workload": read_report,
        "reopened_before_measure": reopened_before_measure,
        "commits": COMMITS, "elapsed_ns": elapsed,
        "commits_per_second": COMMITS as f64 * 1e9 / elapsed as f64,
        "transaction_latency": latency(&transaction_samples),
        "commit_latency": latency(&commit_samples),
        "workers": workers.iter().map(|w| json!({
            "worker": w.0, "elapsed_ns": w.1, "commits": w.2.len(),
            "transaction_intervals_ns": w.4,
        })).collect::<Vec<_>>(),
        "group_commit": {
            "submitted": after.submitted_commits - before.submitted_commits,
            "completed": after.completed_commits - before.completed_commits,
            "shared_syncs": after.shared_sync_count - before.shared_sync_count,
            "grouped_entries": after.grouped_wal_entries - before.grouped_wal_entries,
            "fsync_micros": after.total_fsync_micros - before.total_fsync_micros,
        },
        "verified_rows": COMMITS, "reopen_verified": durable,
        "after_relaxed_interval": durable && fixture == WalFixture::AfterRelaxed,
        "first_relaxed_append_ns": relaxed_append_ns,
    })
}

fn main() {
    let mut selected_round = None;
    let mut selected_writers = None;
    let mut selected_readers = None;
    let mut fixture = WalFixture::PostCheckpoint;
    let mut fixture_parent = None;
    let mut pessimistic_statements = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bench" => {} // Cargo supplies this for harness-free benchmarks.
            "--reopen-before-measure" => {
                assert!(
                    fixture == WalFixture::PostCheckpoint,
                    "choose one WAL fixture"
                );
                fixture = WalFixture::Reopened;
            }
            "--after-relaxed-interval" => {
                assert!(
                    fixture == WalFixture::PostCheckpoint,
                    "choose one WAL fixture"
                );
                fixture = WalFixture::AfterRelaxed;
            }
            "--pessimistic-statements" => {
                assert!(
                    pessimistic_statements.is_none(),
                    "duplicate statement count"
                );
                let statements = args
                    .next()
                    .expect("missing statement count")
                    .parse::<usize>()
                    .unwrap();
                assert!([1, 8, 32].contains(&statements));
                pessimistic_statements = Some(statements);
            }
            "--fixture-parent" => {
                assert!(
                    fixture_parent.is_none(),
                    "--fixture-parent may appear only once"
                );
                let path = std::path::PathBuf::from(
                    args.next().expect("--fixture-parent requires a directory"),
                );
                assert!(
                    path.is_dir(),
                    "fixture parent must be an existing directory"
                );
                fixture_parent = Some(path);
            }
            "--writers" => {
                assert!(selected_writers.is_none(), "--writers may appear only once");
                let writers = args
                    .next()
                    .expect("--writers requires a count")
                    .parse::<usize>()
                    .expect("writers must be an unsigned integer");
                assert!(
                    (1..=COMMITS).contains(&writers),
                    "each writer requires at least one transaction"
                );
                selected_writers = Some(writers);
            }
            "--readers" => {
                assert!(selected_readers.is_none(), "--readers may appear only once");
                let readers = args
                    .next()
                    .expect("--readers requires a count")
                    .parse::<usize>()
                    .expect("readers must be an unsigned integer");
                selected_readers = Some(readers);
            }
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
    if pessimistic_statements.is_some() {
        assert!(
            fixture != WalFixture::AfterRelaxed,
            "relaxed fixture requires the optimistic workload"
        );
        assert!(
            selected_readers.unwrap_or(0) == 0,
            "covered graph workload has no reader workers"
        );
        assert!(
            selected_writers.is_none_or(|writers| [1, 4, 8].contains(&writers)),
            "covered graph workload supports 1, 4 or 8 writers"
        );
    }
    let root = fixture_parent
        .unwrap_or_else(std::env::temp_dir)
        .join(format!(
            "hawdb-concurrent-writers-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir(&root).unwrap();
    let writer_counts = selected_writers.map_or_else(|| vec![1, 4, 8], |count| vec![count]);
    for round in 0..ROUNDS {
        if selected_round.is_some_and(|selected| selected != round) {
            continue;
        }
        if let Some(statements) = pessimistic_statements {
            let mut cases = [
                (false, 1),
                (false, 4),
                (false, 8),
                (true, 1),
                (true, 4),
                (true, 8),
            ];
            if round % 2 == 1 {
                cases.reverse();
            }
            for (durable, writers) in cases {
                if selected_writers.is_some_and(|selected| selected != writers) {
                    continue;
                }
                let result = measure_covered_graph(
                    &root.join(format!("{round}-{durable}-{writers}")),
                    durable,
                    writers,
                    statements,
                    fixture == WalFixture::Reopened,
                );
                println!(
                    "{}",
                    json!({"schema": "covered-graph-writers-v1", "smoke": SMOKE, "round": round, "result": result})
                );
            }
            continue;
        }
        // Reverse case order every round to reduce consistent order bias.
        let mut cases = Vec::new();
        for (durable, grouped) in [(false, false), (true, false), (true, true)] {
            for &writers in &writer_counts {
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
                fixture,
                selected_readers.unwrap_or(0),
            );
            println!(
                "{}",
                json!({"schema": "concurrent-writers-v2", "smoke": SMOKE, "round": round, "result": result})
            );
        }
    }
    std::fs::remove_dir(root).unwrap();
}

/// Fixed disjoint writers. Each wave acquires its first locks before any commit,
/// then finishes all commits before the next wave. No retries hide epoch aborts.
/// Barriers are part of both revisions' measured workload, not engine parallelism
/// evidence. Existing optimistic/default benchmark selection remains unchanged.
fn measure_covered_graph(
    path: &Path,
    durable: bool,
    writers: usize,
    statements: usize,
    reopen_before_measure: bool,
) -> JsonValue {
    let mut database = if durable {
        Database::open(path).unwrap()
    } else {
        Database::new()
    };
    for id in 0..writers {
        database
            .query_with_params(
                "CREATE (:MvccCounter {id: $id, value: 0})",
                &BTreeMap::from([("id".into(), Value::Int(id as i64))]),
            )
            .unwrap();
    }
    database.checkpoint().unwrap();
    let reopened_before_measure = durable && reopen_before_measure;
    let database = if reopened_before_measure {
        drop(database);
        Database::open(path).unwrap()
    } else {
        database
    }
    .into_concurrent();
    let epoch = database.commit_epoch().unwrap();
    let barrier = Barrier::new(writers);
    let started = Instant::now();
    let samples = std::thread::scope(|scope| {
        let workers = (0..writers)
            .map(|id| {
                let database = &database;
                let barrier = &barrier;
                scope.spawn(move || {
                    let mut staging = Vec::new();
                    let mut commits = Vec::new();
                    for wave in 0..COMMITS / writers {
                        let mut tx = database
                            .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                                Duration::from_secs(5),
                            ))
                            .unwrap();
                        let stage_started = Instant::now();
                        for statement in 0..statements {
                            tx.query_with_params(
                                "MATCH (n:MvccCounter) WHERE n.id = $id SET n.value = $value",
                                &BTreeMap::from([
                                    ("id".into(), Value::Int(id as i64)),
                                    (
                                        "value".into(),
                                        Value::Int((wave * statements + statement + 1) as i64),
                                    ),
                                ]),
                            )
                            .unwrap();
                        }
                        staging.push(nanos(stage_started));
                        barrier.wait();
                        let commit_started = Instant::now();
                        tx.commit().unwrap();
                        commits.push(nanos(commit_started));
                        barrier.wait();
                    }
                    (staging, commits)
                })
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    let elapsed = nanos(started);
    let value = (COMMITS / writers * statements) as i64;
    let expected = (0..writers)
        .map(|id| {
            BTreeMap::from([
                ("id".into(), Value::Int(id as i64)),
                ("value".into(), Value::Int(value)),
            ])
        })
        .collect::<Vec<_>>();
    assert_eq!(database.commit_epoch().unwrap(), epoch + COMMITS as u64);
    assert_eq!(database.query(READ).unwrap().rows, expected);
    drop(database);
    if durable {
        let mut reopened = Database::open_with_config(
            path,
            hawdb::DatabaseConfig {
                recovery_mode: hawdb::RecoveryMode::Strict,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(reopened.query(READ).unwrap().rows, expected);
        assert_eq!(reopened.commit_epoch().checked(), epoch + COMMITS as u64);
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
    json!({
        "durable": durable, "writers": writers, "statements_per_transaction": statements,
        "commits": COMMITS, "elapsed_ns": elapsed, "group_commit": "disabled",
        "reopened_before_measure": reopened_before_measure,
        "staging_latency": latency(&samples.iter().flat_map(|sample| sample.0.iter().copied()).collect::<Vec<_>>()),
        "commit_latency": latency(&samples.iter().flat_map(|sample| sample.1.iter().copied()).collect::<Vec<_>>()),
        "verified_rows": writers, "reopen_verified": durable,
    })
}
