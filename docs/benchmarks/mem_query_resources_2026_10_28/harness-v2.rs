// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual synthetic Mem query qualification. Run one matrix cell per process.
//! Timed reads use the embedded facade; setup is outside the timing window.
//! Process RSS includes setup and native heaps; allocator traffic is Rust-only.

#[cfg(feature = "qualification")]
#[path = "../bindings/benchmarks/native_alloc.rs"]
mod native_profile;

use hawdb::{ConcurrentDatabase, Database, DatabaseConfig, Value};
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Barrier,
};
use std::time::Instant;

type Parameters = BTreeMap<String, Value>;

struct Job {
    candidates: usize,
    space_rows: usize,
    body_bytes: usize,
    repeats: usize,
    background: bool,
}

fn populate(db: &mut Database, job: &Job) -> hawdb::Result<()> {
    db.query("CREATE NODE TABLE Memory")?;
    db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE STRING NOT NULL")?;
    db.query("CREATE PROPERTY ON NODE TABLE Memory(created_at) TYPE INT NOT NULL")?;
    for start in (0..job.space_rows * 2).step_by(64) {
        let rows = (start..(start + 64).min(job.space_rows * 2))
            .map(|index| {
                Value::Map(BTreeMap::from([
                    ("id".into(), Value::String(format!("memory-{index:08}"))),
                    (
                        "space".into(),
                        Value::String(
                            if index < job.space_rows {
                                "default"
                            } else {
                                "team"
                            }
                            .into(),
                        ),
                    ),
                    ("created".into(), Value::Int((index / 4) as i64)),
                    ("body".into(), Value::String("x".repeat(job.body_bytes))),
                ]))
            })
            .collect();
        db.query_with_params("UNWIND $rows AS r CREATE (:Memory {id: r.id, space_id: r.space, created_at: r.created, content: r.body})", &BTreeMap::from([("rows".into(), Value::List(rows))]))?;
    }
    db.query("CREATE INDEX ON :Memory(id)")?;
    db.query("CREATE INDEX ON :Memory(space_id, created_at, id)")?;
    db.query("CREATE (:Counter {id: 'background', value: 0})")?;
    Ok(())
}

fn ranking(job: &Job) -> (String, Parameters) {
    let mut cases: [String; 5] = std::array::from_fn(|_| "CASE m.id".into());
    let mut params = Parameters::new();
    let mut ids = Vec::new();
    for index in 0..job.candidates {
        let id = Value::String(format!("memory-{index:08}"));
        ids.push(id.clone());
        params.insert(format!("id_{index}"), id);
        for (column, expression) in cases.iter_mut().enumerate() {
            let key = format!("input_{column}_{index}");
            expression.push_str(&format!(" WHEN $id_{index} THEN ${key}"));
            params.insert(
                key,
                if column == 1 {
                    Value::String("evidence".into())
                } else {
                    Value::Int(index as i64)
                },
            );
        }
    }
    for expression in &mut cases {
        expression.push_str(" ELSE null END");
    }
    params.insert("candidate_ids".into(), Value::List(ids));
    let [score, reason, ordinal, confidence, temporal] = cases;
    (format!("MATCH (m:Memory) WHERE m.id IN $candidate_ids AND m.space_id = 'default' RETURN m.id AS id, {score} AS score, {reason} AS reason, {ordinal} AS ordinal, {confidence} AS confidence, {temporal} AS temporal ORDER BY ordinal"), params)
}

fn expand_setup(db: &mut Database, job: &Job) -> hawdb::Result<()> {
    for index in 0..job.candidates {
        db.query_with_params(
            "MATCH (s:Memory), (m:Memory) WHERE s.id = $seed AND m.id = $target CREATE (s)-[:LINK]->(m)",
            &BTreeMap::from([
                ("seed".into(), Value::String("memory-00000000".into())),
                ("target".into(), Value::String(format!("memory-{index:08}"))),
            ]),
        )?;
    }
    Ok(())
}

fn expand() -> (String, Parameters) {
    (
        "MATCH (s:Memory {id: $seed})-[:LINK]->(m:Memory) RETURN m.id AS id".into(),
        BTreeMap::from([("seed".into(), Value::String("memory-00000000".into()))]),
    )
}

#[cfg(unix)]
fn process_usage() -> (u64, u64) {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes a complete rusage on success; no borrowed
    // pointer escapes, and the return code is checked before assume_init.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    // SAFETY: the successful call above initialized the complete value.
    let usage = unsafe { usage.assume_init() };
    let micros = |value: libc::timeval| (value.tv_sec as u64) * 1_000_000 + value.tv_usec as u64;
    let rss = usage.ru_maxrss as u64;
    #[cfg(target_os = "macos")]
    let bytes = rss;
    #[cfg(not(target_os = "macos"))]
    let bytes = rss * 1024;
    (micros(usage.ru_utime) + micros(usage.ru_stime), bytes)
}

#[cfg(not(unix))]
fn process_usage() -> (u64, u64) {
    (0, 0)
}

struct StopOnDrop<'a>(&'a AtomicBool);
impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn measure(
    db: &ConcurrentDatabase,
    job: &Job,
    query: &str,
    params: &mut Parameters,
) -> hawdb::Result<JsonValue> {
    // Warm parser/physical cache before measuring; vary parameter values while
    // retaining their shapes to exercise binding, not identical-value replay.
    db.begin_read_transaction()?.query_with_params_bounded(
        query,
        params,
        Some(job.candidates.max(20)),
    )?;
    let ready = Barrier::new(if job.background { 2 } else { 1 });
    let stop = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let writer = job.background.then(|| {
            scope.spawn(|| {
                ready.wait();
                let mut writes = 0;
                while !stop.load(Ordering::Acquire) {
                    db.query("MATCH (n:Counter {id: 'background'}) SET n.value = n.value + 1")
                        .unwrap();
                    writes += 1;
                }
                writes
            })
        });
        let completion = StopOnDrop(&stop);
        #[cfg(feature = "qualification")]
        let allocations_before = native_profile::snapshot();
        #[cfg(feature = "qualification")]
        let preparation_before = hawdb::query_preparation_metrics();
        let (cpu_before, _) = process_usage();
        ready.wait();
        let samples = (0..job.repeats).map(|iteration| {
            if let Some(value) = params.get_mut("input_0_0") { *value = Value::Int(iteration as i64); }
            let start = Instant::now();
            let output = db.begin_read_transaction()?.query_with_params_bounded_profile(query, params, Some(job.candidates.max(20)))?;
            let elapsed_ns = start.elapsed().as_nanos();
            let profile = output.execution_profile;
            let memory = profile.pipeline_memory_report;
            let Value::Map(report) = hawdb_executor::observer::pipeline_memory_report_value(&memory) else { unreachable!() };
            // Optional report keys keep the same harness buildable on the
            // baseline revision, before dispatch instrumentation existed.
            let metric = |name| match report.get(name) { Some(Value::Int(value)) => *value, _ => 0 };
            let ids = output.output.rows.iter().map(|row| row["id"].clone()).collect::<Vec<_>>();
            let expected_rows = job.candidates;
            assert_eq!(ids.len(), expected_rows);
            let mut actual_ids = ids.clone();
            actual_ids.sort();
            let expected_ids = (0..job.candidates).map(|i| Value::String(format!("memory-{i:08}"))).collect::<Vec<_>>();
            assert_eq!(actual_ids, expected_ids, "benchmark must retain the complete candidate cohort");
            Ok(json!({"elapsed_ns": elapsed_ns, "rows": ids.len(),
                "query_peak_bytes": memory.query_memory_peak_bytes,
                "query_completion_bytes": memory.query_memory_completion_bytes,
                "intermediate_rows": memory.intermediate_rows,
                "intermediate_payload_bytes": memory.intermediate_payload_bytes,
                "output_payload_bytes": memory.output_payload_bytes,
                "scan_candidates": profile.scan_pruning_reports.iter().map(|scan| scan.candidate_count_before_filter).sum::<usize>(),
                "case_tables": metric("simple_case_dispatch_tables"),
                "case_lookups": metric("simple_case_dispatch_lookups")}))
        }).collect::<hawdb::Result<Vec<_>>>();
        drop(completion);
        let writes = writer.map(|thread| thread.join().unwrap()).unwrap_or(0);
        let (cpu_after, peak_rss_bytes) = process_usage();
        let samples = samples?;
        #[cfg(feature = "qualification")]
        let instrumentation = {
            let after = native_profile::snapshot();
            json!({"preparation_before": preparation_before,
                "preparation_after": hawdb::query_preparation_metrics(),
                "allocation_calls": after.allocation_calls - allocations_before.allocation_calls,
                "allocated_bytes": after.allocated_bytes - allocations_before.allocated_bytes,
                "live_requested_bytes_before": allocations_before.live_requested_bytes,
                "live_requested_bytes_after": after.live_requested_bytes,
                "process_peak_requested_bytes": after.process_peak_requested_bytes})
        };
        #[cfg(not(feature = "qualification"))]
        let instrumentation = JsonValue::Null;
        let mut latencies = samples
            .iter()
            .map(|sample| sample["elapsed_ns"].as_u64().unwrap())
            .collect::<Vec<_>>();
        latencies.sort_unstable();
        Ok(
            json!({"samples": samples, "p95_ns": latencies[(latencies.len() * 95).div_ceil(100).saturating_sub(1)],
            "cpu_us": cpu_after.saturating_sub(cpu_before), "process_peak_rss_bytes": peak_rss_bytes,
            "completed_background_writes": writes, "instrumentation": instrumentation}),
        )
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if !(5..=6).contains(&args.len()) {
        return Err(
            "expected candidates space_rows body_bytes repeats background(0|1) [ranking|expand]"
                .into(),
        );
    }
    let workload = args.get(5).map(String::as_str).unwrap_or("ranking");
    if !["ranking", "expand"].contains(&workload) {
        return Err("invalid workload".into());
    }
    let job = Job {
        candidates: args[0].parse()?,
        space_rows: args[1].parse()?,
        body_bytes: args[2].parse()?,
        repeats: args[3].parse()?,
        background: args[4] == "1",
    };
    if ![20, 100, 512].contains(&job.candidates)
        || !(512..=16384).contains(&job.space_rows)
        || !(1..=65536).contains(&job.body_bytes)
        || !(3..=100).contains(&job.repeats)
        || job.candidates > job.space_rows
        || !["0", "1"].contains(&args[4].as_str())
    {
        return Err("invalid benchmark cell".into());
    }
    let mut database = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(16),
        ..Default::default()
    });
    populate(&mut database, &job)?;
    if workload == "expand" {
        expand_setup(&mut database, &job)?;
    }
    let database = ConcurrentDatabase::new(database);
    let (query, mut params) = if workload == "expand" {
        expand()
    } else {
        ranking(&job)
    };
    let ranking = measure(&database, &job, &query, &mut params)?;
    println!(
        "{}",
        json!({"protocol": "hawdb-mem-query-resources-v2", "workload": workload, "candidates": job.candidates,
        "space_rows": job.space_rows, "body_bytes": job.body_bytes, "repeats": job.repeats, "background": job.background,
        "profile": if cfg!(debug_assertions) {"checked"} else {"release"}, "instrumented": cfg!(feature = "qualification"),
        "cache": "warm; shape-stable parameter rebinding", "storage": "in-memory; no persistence/durability claim",
        "cpu_scope": "process including background writer", "rss_scope": "process high-water including fixture setup",
        "result": ranking})
    );
    Ok(())
}
