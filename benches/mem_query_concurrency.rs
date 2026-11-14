// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual ordinary Mem adapter qualification. Use identical job files and build
//! profiles on both revisions. Qualification instrumentation changes timings;
//! compare uninstrumented latency separately from allocator/owner observations.

#[cfg(feature = "qualification")]
#[path = "../bindings/benchmarks/native_alloc.rs"]
mod native_profile;

use hawdb::{
    DatabaseConfig, EmbeddedReadOptions, EmbeddedStoreHandle, EmbeddedStoreOpenOptions, GraphMode,
    StorageResidencyMode, Value,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Barrier;
use std::time::Instant;

type Store = EmbeddedStoreHandle;
const WRITE: &str =
    "MATCH (n:Boundary {id: $id}) SET n.value = n.value + 1 RETURN n.value AS value";
const POINT: &str = "MATCH (n:Boundary {id: $id}) RETURN n.value AS value LIMIT 1";
const SCAN: &str = "MATCH (n:Boundary) RETURN n.value AS value LIMIT $limit";
const PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReadKind {
    Point,
    Stream,
    Report,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Job {
    path: PathBuf,
    writers: usize,
    readers: usize,
    writer_iterations: usize,
    reader_iterations: usize,
    rows: usize,
    read_kind: ReadKind,
    out_of_core: bool,
    #[serde(default)]
    sustain_readers: bool,
}

struct Progress {
    committed: AtomicUsize,
    writers_remaining: AtomicUsize,
}

struct WriterCompletion<'a>(&'a AtomicUsize);

impl Drop for WriterCompletion<'_> {
    fn drop(&mut self) {
        // Readers stop after normal completion or a writer panic; a failed
        // worker still causes the process to fail at join/result validation.
        self.0.fetch_sub(1, Ordering::Release);
    }
}

#[derive(Serialize)]
struct Sample {
    started_ns: u64,
    elapsed_ns: u64,
    acknowledged_writes: usize,
}

#[derive(Default, Serialize)]
struct Worker {
    writer: bool,
    index: usize,
    samples: Vec<Sample>,
    errors: BTreeMap<String, usize>,
    successful: usize,
    checksum: u64,
    reported_plan_cache_hits: usize,
    reported_plan_cache_misses: usize,
}

fn nanos(elapsed: std::time::Duration) -> u64 {
    elapsed.as_nanos().min(u64::MAX as u128) as u64
}

fn integer(value: &Value) -> u64 {
    let Value::Int(value) = value else {
        panic!("expected integer fixture value")
    };
    (*value).try_into().expect("nonnegative fixture value")
}

fn parameters(id: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([("id".into(), Value::Int(id.try_into().unwrap()))])
}

fn read(store: &Store, job: &Job, id: usize) -> hawdb::Result<u64> {
    let options = EmbeddedReadOptions {
        max_rows: Some(job.rows),
        max_estimated_payload_bytes: Some(PAYLOAD_BYTES),
    };
    match job.read_kind {
        ReadKind::Point => {
            let output = store.read_query_with_params(POINT, &parameters(id), &options)?;
            assert_eq!(output.output.rows.len(), 1);
            Ok(integer(&output.output.rows[0]["value"]))
        }
        ReadKind::Report => {
            let output = store.query_with_params_with_report(POINT, &parameters(id))?;
            assert_eq!(output.output.rows.len(), 1);
            Ok(integer(&output.output.rows[0]["value"]))
        }
        ReadKind::Stream => {
            let mut sum = 0u64;
            let mut rows = 0;
            store.read_query_with_params_streaming(
                SCAN,
                &BTreeMap::from([("limit".into(), Value::Int(job.rows.try_into().unwrap()))]),
                &options,
                |row| {
                    rows += 1;
                    sum += integer(&row["value"]);
                    Ok(())
                },
            )?;
            assert_eq!(rows, job.rows);
            Ok(sum)
        }
    }
}

fn worker(
    store: &Store,
    job: &Job,
    index: usize,
    ready: &Barrier,
    go: &Barrier,
    origin: Instant,
    progress: &Progress,
) -> Worker {
    let writer = index < job.writers;
    let _completion = writer.then(|| WriterCompletion(&progress.writers_remaining));
    let iterations = if writer {
        job.writer_iterations
    } else {
        job.reader_iterations
    };
    let mut result = Worker {
        writer,
        index,
        samples: Vec::with_capacity(iterations),
        ..Worker::default()
    };
    let params = parameters(index);
    ready.wait();
    go.wait();
    let mut iteration = 0;
    while iteration < iterations
        || (!writer
            && job.sustain_readers
            && progress.writers_remaining.load(Ordering::Acquire) > 0)
    {
        let started = Instant::now();
        let output = if writer {
            store
                .query_with_params_with_report(WRITE, &params)
                .map(|output| {
                    assert_eq!(output.output.rows.len(), 1);
                    result.reported_plan_cache_hits += usize::from(output.report.plan_cache_hit);
                    result.reported_plan_cache_misses += usize::from(output.report.plan_cache_miss);
                    progress.committed.fetch_add(1, Ordering::Relaxed);
                    integer(&output.output.rows[0]["value"])
                })
        } else {
            read(store, job, iteration % job.writers)
        };
        let elapsed_ns = nanos(started.elapsed());
        result.samples.push(Sample {
            started_ns: nanos(started.duration_since(origin)),
            elapsed_ns,
            acknowledged_writes: progress.committed.load(Ordering::Relaxed),
        });
        match output {
            Ok(value) => {
                result.successful += 1;
                result.checksum = result.checksum.wrapping_add(value);
            }
            Err(error) => *result.errors.entry(error.to_string()).or_default() += 1,
        }
        iteration += 1;
    }
    result
}

fn latency(workers: &[Worker], writer: bool) -> JsonValue {
    let mut samples: Vec<_> = workers
        .iter()
        .filter(|w| w.writer == writer)
        .flat_map(|w| w.samples.iter().map(|s| s.elapsed_ns))
        .collect();
    samples.sort_unstable();
    let percentile = |percent: usize| samples[(samples.len() * percent).div_ceil(100) - 1];
    json!({"calls": samples.len(), "p50_ns": percentile(50), "p95_ns": percentile(95),
        "p99_ns": percentile(99), "max_ns": samples.last(),
        "includes_failed_calls": true})
}

fn main() {
    let job_path = std::env::args()
        .nth(1)
        .expect("usage: mem_query_concurrency job.json");
    let job: Job = serde_json::from_slice(&std::fs::read(job_path).unwrap()).unwrap();
    assert!((1..=64).contains(&job.writers) && (1..=64).contains(&job.readers));
    assert!((job.writers..=4096).contains(&job.rows));
    assert!((1..=1_000_000).contains(&job.writer_iterations));
    assert!((1..=1_000_000).contains(&job.reader_iterations));
    // Refuse an existing path; this local tool never overwrites another database.
    std::fs::create_dir(&job.path).expect("new fixture directory");
    let config = DatabaseConfig {
        storage_residency_mode: if job.out_of_core {
            StorageResidencyMode::OutOfCore
        } else {
            StorageResidencyMode::Materialized
        },
        max_read_result_rows: Some(job.rows),
        max_read_result_payload_bytes: Some(PAYLOAD_BYTES),
        ..DatabaseConfig::default()
    };
    let options =
        EmbeddedStoreOpenOptions::graph_only(job.path.join("graph"), GraphMode::WritableCutover)
            .with_database_config(config);
    let (store, _) = Store::open_with_options(options.clone()).unwrap();
    store
        .query_with_report("CREATE CONSTRAINT ON :Boundary(id) ASSERT UNIQUE")
        .unwrap();
    for id in 0..job.rows {
        store
            .query_with_params_with_report(
                "CREATE (:Boundary {id: $id, value: 0})",
                &parameters(id),
            )
            .unwrap();
    }
    store.checkpoint().unwrap();
    drop(store);
    let (store, _) = Store::open_with_options(options.clone()).unwrap();
    let residency_before = store.storage_residency_report().unwrap();
    assert_eq!(residency_before.out_of_core, job.out_of_core);
    for id in 0..job.writers {
        store
            .query_with_params_with_report(WRITE, &parameters(id))
            .unwrap();
        read(&store, &job, id).unwrap();
    }
    let resources = store.runtime_governor_snapshot().unwrap();
    let ready = Barrier::new(job.writers + job.readers + 1);
    let go = Barrier::new(job.writers + job.readers + 1);
    let progress = Progress {
        committed: AtomicUsize::new(0),
        writers_remaining: AtomicUsize::new(job.writers),
    };
    let origin = Instant::now();
    let (workers, elapsed_ns, instrumentation) = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..job.writers + job.readers)
            .map(|index| {
                let (store, job, ready, go, progress) = (&store, &job, &ready, &go, &progress);
                scope.spawn(move || worker(store, job, index, ready, go, origin, progress))
            })
            .collect();
        ready.wait();
        #[cfg(feature = "qualification")]
        let locks_before = store.publication_lock_metrics();
        #[cfg(feature = "qualification")]
        let preparation_before = hawdb::query_preparation_metrics();
        #[cfg(feature = "qualification")]
        let allocations_before = native_profile::snapshot();
        let started = Instant::now();
        go.wait();
        let workers: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        let elapsed_ns = nanos(started.elapsed());
        #[cfg(feature = "qualification")]
        let allocations_after = native_profile::snapshot();
        #[cfg(feature = "qualification")]
        let instrumentation = json!({
            "lock_before": locks_before, "lock_after": store.publication_lock_metrics(),
            "preparation_before": preparation_before,
            "preparation_after": hawdb::query_preparation_metrics(),
            "allocation_calls": allocations_after.allocation_calls - allocations_before.allocation_calls,
            "allocated_bytes": allocations_after.allocated_bytes - allocations_before.allocated_bytes,
            "deallocation_calls": allocations_after.deallocation_calls - allocations_before.deallocation_calls,
            "deallocated_bytes": allocations_after.deallocated_bytes - allocations_before.deallocated_bytes,
            "reallocation_calls": allocations_after.reallocation_calls - allocations_before.reallocation_calls,
            "live_requested_bytes_before": allocations_before.live_requested_bytes,
            "live_requested_bytes_after": allocations_after.live_requested_bytes,
            "process_peak_requested_bytes": allocations_after.process_peak_requested_bytes,
        });
        #[cfg(not(feature = "qualification"))]
        let instrumentation = JsonValue::Null;
        (workers, elapsed_ns, instrumentation)
    });
    let after = store.runtime_governor_snapshot().unwrap();
    let residency_after = store.storage_residency_report().unwrap();
    let successful_writes: usize = workers
        .iter()
        .filter(|w| w.writer)
        .map(|w| w.successful)
        .sum();
    assert_eq!(
        progress.committed.load(Ordering::Relaxed),
        successful_writes
    );
    let errors: usize = workers.iter().flat_map(|w| w.errors.values()).sum();
    drop(store);
    let (reopened, _) = Store::open_with_options(options).unwrap();
    for worker in workers.iter().filter(|w| w.writer) {
        let actual = reopened
            .query_with_params_with_report(POINT, &parameters(worker.index))
            .unwrap();
        assert_eq!(
            integer(&actual.output.rows[0]["value"]),
            1 + worker.successful as u64
        );
    }
    println!(
        "{}",
        json!({
            "protocol": "hawdb-mem-query-concurrency-v2", "job": job,
            "profile": "repository bench", "instrumented": cfg!(feature = "qualification"),
            "durability": "SyncOnEveryWrite (facade default)", "elapsed_ns": elapsed_ns,
            "writes": latency(&workers, true), "reads": latency(&workers, false),
            "successful_writes": successful_writes, "errors": errors, "durable_reopen": true,
            "cpu": {"host_parallelism": resources.resources.cpu.host_parallelism,
                "cgroup_quota_parallelism": resources.resources.cpu.cgroup_quota_parallelism,
                "cpuset_parallelism": resources.resources.cpu.cpuset_parallelism,
                "effective_parallelism": resources.resources.cpu.effective_parallelism},
            "runtime_before": format!("{resources:?}"), "runtime_after": format!("{after:?}"),
            "residency_before": format!("{residency_before:?}"),
            "residency_after": format!("{residency_after:?}"),
            "instrumentation": instrumentation, "workers": workers,
        })
    );
    if errors != 0 {
        std::process::exit(1);
    }
}
