// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb::{HawDBEmbedded, QueryOutput, Value};
use std::collections::BTreeMap;
use std::time::Instant;

type ProbeResult<T> = Result<T, Box<dyn std::error::Error>>;

const INSERT: &str = "CREATE (:Item {id: $id, score: $score})";
const POINT: &str = "MATCH (n:Item) WHERE n.id = $id RETURN n.id AS id, n.score AS score";

fn query(
    db: &mut HawDBEmbedded,
    admitted: bool,
    text: &str,
    params: &BTreeMap<String, Value>,
) -> ProbeResult<QueryOutput> {
    if admitted {
        Ok(db.query_with_params_admitted(text, params)?)
    } else {
        Ok(db.database_mut().query_with_params(text, params)?)
    }
}

fn parameters(id: usize) -> ProbeResult<BTreeMap<String, Value>> {
    let id = i64::try_from(id)?;
    Ok(BTreeMap::from([
        ("id".into(), Value::Int(id)),
        ("score".into(), Value::Int(id * 3)),
    ]))
}

fn verify(output: &QueryOutput, id: usize) -> ProbeResult<()> {
    let id = i64::try_from(id)?;
    if output.rows.len() != 1
        || output.rows[0].get("id") != Some(&Value::Int(id))
        || output.rows[0].get("score") != Some(&Value::Int(id * 3))
    {
        return Err("point result mismatch".into());
    }
    Ok(())
}

fn timed_points(
    db: &mut HawDBEmbedded,
    admitted: bool,
    rows: usize,
    rotating: bool,
) -> ProbeResult<Vec<u64>> {
    let mut times = Vec::new();
    for iteration in 0..100 {
        let id = if rotating { iteration * 97 % rows } else { 0 };
        let params = parameters(id)?;
        let started = Instant::now();
        let output = query(db, admitted, POINT, &params)?;
        times.push(u64::try_from(started.elapsed().as_nanos())?);
        verify(&output, id)?;
        std::hint::black_box(output);
    }
    Ok(times)
}

fn main() -> ProbeResult<()> {
    let mut args = std::env::args().skip(1);
    let rows: usize = args.next().ok_or("missing row count")?.parse()?;
    let path = args.next().ok_or("missing backend path")?;
    let api = args.next().ok_or("missing API mode")?;
    let parameter_mode = args.next().unwrap_or_else(|| "stable".into());
    let rotating = match parameter_mode.as_str() {
        "stable" => false,
        "rotating" => true,
        _ => return Err("parameter mode must be stable or rotating".into()),
    };
    let admitted = match api.as_str() {
        "raw" => false,
        "admitted" => true,
        _ => return Err("API mode must be raw or admitted".into()),
    };
    if !(9..=10_000).contains(&rows) || args.next().is_some() {
        return Err(
            "expected 9..=10000 rows, new path or '-', API mode, and optional parameter mode"
                .into(),
        );
    }
    if path != "-" && std::path::Path::new(&path).exists() {
        return Err("file backend requires a new path".into());
    }
    let mut db = if path == "-" {
        HawDBEmbedded::open_in_memory()
    } else {
        HawDBEmbedded::open(&path)?
    };
    for statement in [
        "CREATE NODE TABLE Item",
        "CREATE PROPERTY ON NODE TABLE Item(id) TYPE INT",
        "CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT",
        "CREATE INDEX ON :Item(id)",
    ] {
        query(&mut db, admitted, statement, &BTreeMap::new())?;
    }
    for id in 0..4 {
        query(&mut db, admitted, INSERT, &parameters(id)?)?;
    }
    let initial = db
        .database()
        .explain_query_with_params(POINT, &parameters(0)?)?;
    verify(&query(&mut db, admitted, POINT, &parameters(0)?)?, 0)?;
    let started = Instant::now();
    for id in 4..rows {
        query(&mut db, admitted, INSERT, &parameters(id)?)?;
    }
    let growth_ns = u64::try_from(started.elapsed().as_nanos())?;
    let cache_before_hot = db.database().plan_cache_stats()?;
    let hot_ns = timed_points(&mut db, admitted, rows, rotating)?;
    let cache_after_hot = db.database().plan_cache_stats()?;
    // Bound diagnostics may refresh statistics. Keep them after the hot timer.
    let diagnostic_id = if rotating { rows - 1 } else { 0 };
    let diagnostic = db
        .database_mut()
        .explain_analyze_query_with_params(POINT, &parameters(diagnostic_id)?)?;
    verify(&diagnostic.output, diagnostic_id)?;
    let refreshed = db
        .database()
        .explain_query_with_params(POINT, &parameters(diagnostic_id)?)?;
    let refreshed_ns = timed_points(&mut db, admitted, rows, rotating)?;
    println!(
        "{}",
        serde_json::json!({
            "status": "ok",
            "rows": rows,
            "backend": if path == "-" { "memory" } else { "file" },
            "api": api,
            "parameter_mode": parameter_mode,
            "initial_rows": 4,
            "independent_growth_statements": rows - 4,
            "point_calls_per_phase": 100,
            "growth_ns": growth_ns,
            "hot_ns": hot_ns,
            "refreshed_ns": refreshed_ns,
            "initial_plan": initial.physical_plan.explain(0),
            "after_hot_diagnostic_plan": diagnostic.physical_plan.explain(0),
            "refreshed_plan": refreshed.physical_plan.explain(0),
            "after_hot_diagnostic_lookup": diagnostic.plan_cache_lookup.as_str(),
            "refreshed_lookup": refreshed.plan_cache_lookup.as_str(),
            "after_hot_diagnostic_execution_profile": format!("{:?}", diagnostic.execution_profile),
            "database_cache_hits": cache_after_hot.hits - cache_before_hot.hits,
            "database_cache_misses": cache_after_hot.misses - cache_before_hot.misses,
            "diagnostic_scope": "Plans, decisions and execution profile describe raw Database diagnostics, not the private read-transaction plans used by admitted calls.",
            "after_hot_diagnostic_decisions": diagnostic.trace.decisions,
            "refreshed_decisions": refreshed.trace.decisions,
            "durability": "SyncOnEveryWrite",
            "scope": "Exploratory mixed growth and indexed point control. Point input construction and verification outside individual query timers; growth timer includes parameters and loop work. Bound diagnostics run only after hot queries. Database-cache counters do not describe each admitted read transaction's private cache. Raw/admitted APIs, fresh database, unchanged defaults; no Arrow or source-reuse claim."
        })
    );
    Ok(())
}
