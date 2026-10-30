// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual synthetic Mem query qualification. Run one matrix cell per process.
//! Timed reads use the embedded facade; setup is outside the timing window.
//! Process RSS includes setup and native heaps; allocator traffic is Rust-only.

#[cfg(feature = "qualification")]
#[path = "mem_query_alloc.rs"]
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
    mixed_shapes: bool,
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

/// The admitted ranking join shape from Mem's hawdb_search_scoring.rs:
/// canonical usage columns, five CASE inputs, and three scoped-read bindings.
/// HostScorer decay and authority math are outside this query-phase benchmark.
fn mem_ranking(job: &Job) -> (String, Parameters) {
    let (simple, mut params) = ranking(job);
    let start = simple.find("CASE m.id").expect("ranking CASE projection");
    let end = simple.rfind(" ORDER BY ordinal").expect("ranking ordering");
    let projection = simple[start..end].replace(" AS temporal", " AS temporal_boost");
    for index in 0..job.candidates {
        params.insert(
            format!("input_0_{index}"),
            Value::Float(index as f64 / job.candidates as f64),
        );
        params.insert(
            format!("input_3_{index}"),
            if index % 5 == 0 {
                Value::Null
            } else {
                Value::Float(0.8)
            },
        );
        params.insert(format!("input_4_{index}"), Value::Float(1.0));
    }
    params.insert("cross_space".into(), Value::Bool(false));
    params.insert(
        "space_ids".into(),
        Value::List(vec![Value::String("default".into())]),
    );
    params.insert("default_space_requested".into(), Value::Bool(true));
    (
        format!(
            "MATCH (m:Memory) WHERE m.id IN $candidate_ids \
        AND ($cross_space = true OR m.space_id IN $space_ids OR \
        ($default_space_requested = true AND (m.space_id IS NULL OR m.space_id = ''))) \
        RETURN m.id AS id, m.importance AS importance, m.pagerank_score AS pagerank_score, \
        m.last_accessed_at AS last_accessed_at, m.access_count AS access_count, \
        {projection} ORDER BY ordinal ASC"
        ),
        params,
    )
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
    #[cfg(feature = "qualification")]
    let before_cold = native_profile::snapshot();
    #[cfg(feature = "qualification")]
    native_profile::reset_window_peak();
    // Warm shared binding/catalog state; each read fork has a private physical
    // cache. Vary values within a shape instead of replaying identical values.
    db.begin_read_transaction()?.query_with_params_bounded(
        query,
        params,
        Some(job.candidates.max(20)),
    )?;
    #[cfg(feature = "qualification")]
    let cold = {
        let after = native_profile::snapshot();
        let peak = native_profile::window_peak();
        json!({"live_before": before_cold.live_requested_bytes, "live_after": after.live_requested_bytes, "peak": peak})
    };
    #[cfg(feature = "qualification")]
    native_profile::reset_window_peak();
    if job.mixed_shapes {
        let original = params["candidate_ids"].clone();
        if let Value::List(ids) = params.get_mut("candidate_ids").unwrap() {
            ids.truncate(job.candidates - 3);
        }
        db.begin_read_transaction()?.query_with_params_bounded(
            query,
            params,
            Some(job.candidates),
        )?;
        params.insert("candidate_ids".into(), original);
    }
    #[cfg(feature = "qualification")]
    native_profile::reset_window_peak();
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
        let full_ids = params.get("candidate_ids").cloned();
        let samples = (0..job.repeats).map(|iteration| {
            let expected_rows = if job.mixed_shapes { job.candidates - (iteration % 2) * 3 } else { job.candidates };
            if job.mixed_shapes {
                let Value::List(ids) = full_ids.as_ref().unwrap() else { unreachable!() };
                params.insert("candidate_ids".into(), Value::List(ids[..expected_rows].to_vec()));
            }
            if let Some(value) = params.get_mut("input_0_0") {
                *value = if matches!(value, Value::Float(_)) { Value::Float(iteration as f64 / 100.0) } else { Value::Int(iteration as i64) };
            }
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
            assert_eq!(ids.len(), expected_rows);
            let mut actual_ids = ids.clone();
            actual_ids.sort();
            let expected_ids = (0..expected_rows).map(|i| Value::String(format!("memory-{i:08}"))).collect::<Vec<_>>();
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
            json!({"cold_query": cold, "warm_peak_requested_bytes": native_profile::window_peak(),
                "preparation_before": preparation_before,
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

#[cfg(feature = "qualification")]
fn template_memory_probe(query: &str, parameters: &Parameters) -> hawdb::Result<JsonValue> {
    let statement = hawdb_cypher::parse(query)?;
    let before = native_profile::snapshot();
    native_profile::reset_window_peak();
    let plan = hawdb_plan_cache::parameterize_logical_plan(&statement, parameters)?;
    let planning_peak = native_profile::window_peak();
    let before_template = native_profile::snapshot();
    let template = hawdb_plan_cache::ParameterBindingTemplate::new(&plan, parameters)
        .expect("ranking fixture has complete parameter coverage");
    let template_bytes =
        native_profile::snapshot().live_requested_bytes - before_template.live_requested_bytes;
    native_profile::reset_window_peak();
    let rebound = template
        .reuse(parameters)
        .expect("same shape must be reusable");
    let reuse_peak = native_profile::window_peak();
    drop(rebound);
    let reuse_live_after = native_profile::snapshot().live_requested_bytes;
    drop(template);
    drop(plan);
    let after_drop = native_profile::snapshot();
    Ok(
        json!({"scope": "isolated logical planning, template heap payload, and reuse clone; excludes cache keys, LFU containers, Arc headers and physical plans",
        "planning_peak_above_start": planning_peak.saturating_sub(before.live_requested_bytes),
        "template_heap_bytes": template_bytes,
        "default_128_template_heap_bytes": template_bytes * 128,
        "reuse_peak_above_retained": reuse_peak.saturating_sub(reuse_live_after),
        "live_after_drop_delta": after_drop.live_requested_bytes as i128 - before.live_requested_bytes as i128}),
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if !(5..=6).contains(&args.len()) {
        return Err(
            "expected candidates space_rows body_bytes repeats background(0|1) [ranking|expand|mem-ranking|mixed-ranking]"
                .into(),
        );
    }
    let workload = args.get(5).map(String::as_str).unwrap_or("ranking");
    if !["ranking", "expand", "mem-ranking", "mixed-ranking"].contains(&workload) {
        return Err("invalid workload".into());
    }
    let job = Job {
        candidates: args[0].parse()?,
        space_rows: args[1].parse()?,
        body_bytes: args[2].parse()?,
        repeats: args[3].parse()?,
        background: args[4] == "1",
        mixed_shapes: workload == "mixed-ranking",
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
    } else if ["mem-ranking", "mixed-ranking"].contains(&workload) {
        mem_ranking(&job)
    } else {
        ranking(&job)
    };
    let ranking = measure(&database, &job, &query, &mut params)?;
    #[cfg(feature = "qualification")]
    let template_memory = if workload == "expand" {
        JsonValue::Null
    } else {
        template_memory_probe(&query, &params)?
    };
    #[cfg(not(feature = "qualification"))]
    let template_memory = JsonValue::Null;
    println!(
        "{}",
        json!({"protocol": "hawdb-mem-query-resources-v2", "workload": workload, "candidates": job.candidates,
        "space_rows": job.space_rows, "body_bytes": job.body_bytes, "repeats": job.repeats, "background": job.background,
        "profile": if cfg!(debug_assertions) {"checked"} else {"release"}, "instrumented": cfg!(feature = "qualification"),
        "cache": if job.mixed_shapes {"warm; alternating list lengths at fixed query text"} else {"warm; shape-stable parameter rebinding"}, "storage": "in-memory; no persistence/durability claim",
        "cpu_scope": "process including background writer", "rss_scope": "process high-water including fixture setup",
        "result": ranking, "template_memory": template_memory})
    );
    Ok(())
}
