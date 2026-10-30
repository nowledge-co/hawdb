// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual, source-identical baseline/candidate cache qualification. One cell
//! per process; no storage IO or background writers in the timing window.

#[cfg(feature = "qualification")]
#[path = "mem_query_alloc.rs"]
mod native_profile;

use hawdb::{Database, DatabaseConfig, DatabaseReadTransaction, QueryStreamOptions, Value};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Instant;

type Parameters = BTreeMap<String, Value>;

fn fixture(
    candidates: usize,
    space_rows: usize,
    body_bytes: usize,
) -> (Database, String, Parameters) {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(16),
        ..Default::default()
    });
    for start in (0..space_rows * 2).step_by(64) {
        let rows = (start..(start + 64).min(space_rows * 2))
            .map(|index| {
                Value::Map(BTreeMap::from([
                    ("id".into(), Value::String(format!("memory-{index:08}"))),
                    (
                        "space".into(),
                        Value::String(
                            if index < space_rows {
                                "default"
                            } else {
                                "team"
                            }
                            .into(),
                        ),
                    ),
                    ("body".into(), Value::String("x".repeat(body_bytes))),
                ]))
            })
            .collect();
        db.query_with_params(
            "UNWIND $rows AS r CREATE (:Memory {id: r.id, space_id: r.space, content: r.body})",
            &BTreeMap::from([("rows".into(), Value::List(rows))]),
        )
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    let mut query = "MATCH (m:Memory) WHERE m.id IN $ids AND ($cross_space = true OR m.space_id = $space) RETURN m.id AS id, m.content AS content".to_owned();
    let mut parameters = BTreeMap::from([
        (
            "ids".into(),
            Value::List(
                (0..candidates)
                    .map(|index| Value::String(format!("memory-{index:08}")))
                    .collect(),
            ),
        ),
        ("cross_space".into(), Value::Bool(false)),
        ("space".into(), Value::String("default".into())),
    ]);
    for field in 0..5 {
        query.push_str(", CASE m.id");
        for index in 0..candidates {
            query.push_str(&format!(" WHEN $id_{index} THEN $score_{field}_{index}"));
            parameters.insert(
                format!("id_{index}"),
                Value::String(format!("memory-{index:08}")),
            );
            parameters.insert(
                format!("score_{field}_{index}"),
                Value::Float(index as f64 + field as f64 / 8.0),
            );
        }
        query.push_str(&format!(" ELSE null END AS score_{field}"));
    }
    query.push_str(" ORDER BY id");
    (db, query, parameters)
}

fn run(
    read: &mut DatabaseReadTransaction,
    query: &str,
    parameters: &Parameters,
    candidates: usize,
    body_bytes: usize,
    changed: f64,
) -> u64 {
    let mut rows = Vec::with_capacity(candidates);
    let start = Instant::now();
    let output = read
        .query_with_params_streaming(
            query,
            parameters,
            QueryStreamOptions {
                max_rows: Some(candidates),
                max_payload_bytes: Some(candidates * (body_bytes + 2048)),
            },
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    let elapsed = u64::try_from(start.elapsed().as_nanos()).unwrap();
    assert_eq!(rows.len(), candidates);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row["id"], Value::String(format!("memory-{index:08}")));
        assert_eq!(row["content"], Value::String("x".repeat(body_bytes)));
        for field in 0..5 {
            let expected = if index == 0 && field == 0 {
                changed
            } else {
                index as f64 + field as f64 / 8.0
            };
            let Value::Float(actual) = row[&format!("score_{field}")] else {
                panic!("score type");
            };
            assert_eq!(actual.to_bits(), expected.to_bits());
        }
    }
    assert_eq!(
        output
            .execution_profile
            .pipeline_memory_report
            .query_memory_completion_bytes,
        0
    );
    elapsed
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert_eq!(
        args.len(),
        5,
        "candidates space_rows body_bytes repeats reused|fork"
    );
    let candidates: usize = args[0].parse().unwrap();
    let space_rows: usize = args[1].parse().unwrap();
    let body_bytes: usize = args[2].parse().unwrap();
    let repeats: usize = args[3].parse().unwrap();
    let fork = match args[4].as_str() {
        "reused" => false,
        "fork" => true,
        _ => panic!("cache mode"),
    };
    assert!([20, 100, 512].contains(&candidates));
    assert!((512..=8192).contains(&space_rows));
    assert!((1..=16384).contains(&body_bytes));
    assert!((3..=100).contains(&repeats));
    let (db, query, mut parameters) = fixture(candidates, space_rows, body_bytes);
    let mut read = db.begin_read_transaction().unwrap();
    #[cfg(feature = "qualification")]
    let before_cold = native_profile::snapshot();
    #[cfg(feature = "qualification")]
    native_profile::reset_window_peak();
    let cold_ns = run(&mut read, &query, &parameters, candidates, body_bytes, 0.0);
    #[cfg(feature = "qualification")]
    let cold_memory = {
        let after = native_profile::snapshot();
        json!({"allocated_bytes": after.allocated_bytes - before_cold.allocated_bytes,
            "allocation_calls": after.allocation_calls - before_cold.allocation_calls,
            "live_delta": after.live_requested_bytes as i128 - before_cold.live_requested_bytes as i128,
            "peak_above_start": native_profile::window_peak().saturating_sub(before_cold.live_requested_bytes)})
    };
    #[cfg(not(feature = "qualification"))]
    let cold_memory = serde_json::Value::Null;
    for iteration in 0..3 {
        let changed = iteration as f64 / 16.0;
        parameters.insert("score_0_0".into(), Value::Float(changed));
        run(
            &mut read,
            &query,
            &parameters,
            candidates,
            body_bytes,
            changed,
        );
    }
    let before_cache = read.plan_cache_stats();
    #[cfg(feature = "qualification")]
    let before = native_profile::snapshot();
    #[cfg(feature = "qualification")]
    native_profile::reset_window_peak();
    let mut samples_ns = Vec::with_capacity(repeats);
    for iteration in 0..repeats {
        let changed = -(iteration as f64) / 16.0;
        parameters.insert("score_0_0".into(), Value::Float(changed));
        let elapsed = if fork {
            run(
                &mut db.begin_read_transaction().unwrap(),
                &query,
                &parameters,
                candidates,
                body_bytes,
                changed,
            )
        } else {
            run(
                &mut read,
                &query,
                &parameters,
                candidates,
                body_bytes,
                changed,
            )
        };
        samples_ns.push(elapsed);
    }
    #[cfg(feature = "qualification")]
    let allocations = {
        let after = native_profile::snapshot();
        json!({"allocated_bytes": after.allocated_bytes - before.allocated_bytes,
            "allocation_calls": after.allocation_calls - before.allocation_calls,
            "live_delta": after.live_requested_bytes as i128 - before.live_requested_bytes as i128,
            "peak_above_start": native_profile::window_peak().saturating_sub(before.live_requested_bytes)})
    };
    #[cfg(not(feature = "qualification"))]
    let allocations = serde_json::Value::Null;
    let after_cache = read.plan_cache_stats();
    let mut sorted = samples_ns.clone();
    sorted.sort_unstable();
    println!(
        "{}",
        json!({"protocol": "physical-plan-cache-v1", "candidates": candidates,
        "space_rows": space_rows, "body_bytes": body_bytes, "repeats": repeats,
        "mode": args[4], "profile": if cfg!(debug_assertions) {"checked"} else {"release"},
        "instrumented": cfg!(feature = "qualification"), "cold_ns": cold_ns,
        "samples_ns": samples_ns, "median_ns": sorted[sorted.len()/2],
        "p95_ns": sorted[(sorted.len()*95).div_ceil(100)-1],
        "cache_hits": after_cache.hits-before_cache.hits, "cache_misses": after_cache.misses-before_cache.misses,
        "cache_bypasses": after_cache.bypasses-before_cache.bypasses,
        "cache_entries": after_cache.entries, "cold_memory": cold_memory, "warm_memory": allocations,
        "parity": "every id, body and all five f64 score bits asserted for every request"})
    );
}
