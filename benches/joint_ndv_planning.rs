// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual source-bound statistics experiment. Fixture loading, explain,
//! complete result assertions and output drop are outside each timed call.

use hawdb::{Database, QueryRequest, QueryStreamOptions, Value};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Instant;

fn load(
    db: &mut Database,
    label: &str,
    rows: usize,
    keys: usize,
    correlated: bool,
    unique_pair: bool,
) -> hawdb::Result<()> {
    for start in (0..rows).step_by(1024) {
        let values = (start..(start + 1024).min(rows))
            .map(|id| {
                Value::Map(BTreeMap::from([
                    ("id".into(), Value::Int(id as i64)),
                    ("a".into(), Value::Int((id % keys) as i64)),
                    (
                        "b".into(),
                        Value::Int(
                            (if correlated {
                                id % keys
                            } else if unique_pair {
                                id / keys
                            } else {
                                (id / keys) % keys
                            }) as i64,
                        ),
                    ),
                ]))
            })
            .collect();
        db.query_with_params(
            &format!("UNWIND $rows AS r CREATE (:{label} {{id: r.id, a: r.a, b: r.b}})"),
            &BTreeMap::from([("rows".into(), Value::List(values))]),
        )?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert_eq!(
        args.len(),
        3,
        "group-correlated|group-independent|group-noindex|join rows repeats"
    );
    let scenario = &args[0];
    let rows: usize = args[1].parse()?;
    let repeats: usize = args[2].parse()?;
    assert!((200..=100_000).contains(&rows) && (1..=100).contains(&repeats));
    let join = scenario == "join";
    assert!(
        !join || rows <= 5000,
        "bound the quadratic one-key join fixture"
    );
    let correlated = matches!(scenario.as_str(), "group-correlated" | "group-noindex");
    assert!(join || correlated || scenario == "group-independent");
    let keys = if join || rows >= 10_000 { 100 } else { 20 };
    let indexed = scenario != "group-noindex";
    let mut db = Database::new();
    let start = Instant::now();
    for label in if join {
        vec!["Left", "Right"]
    } else {
        vec!["Item"]
    } {
        load(&mut db, label, rows, keys, correlated, join)?;
        if indexed {
            db.query(&format!("CREATE INDEX ON :{label}(a, b)"))?;
        }
    }
    let setup_ns = start.elapsed().as_nanos();
    let limit = if join { 2 * rows } else { 2 * keys };
    let params = BTreeMap::from([("limit".into(), Value::Int(limit as i64))]);
    let query = if join {
        "MATCH (l:Left), (r:Right) WHERE l.a = r.a AND l.b = r.b RETURN l.id AS left_id, r.id AS right_id ORDER BY left_id, right_id LIMIT $limit"
    } else {
        "MATCH (n:Item) RETURN n.a AS a, n.b AS b, count(n) AS total ORDER BY a, b LIMIT $limit"
    };
    let mut counts = BTreeMap::<_, i64>::new();
    if !join {
        for id in 0..rows {
            *counts
                .entry((
                    id % keys,
                    if correlated {
                        id % keys
                    } else {
                        (id / keys) % keys
                    },
                ))
                .or_default() += 1;
        }
    }
    let groups = counts.into_iter().take(limit).collect::<Vec<_>>();
    let start = Instant::now();
    let explain = db.explain_query_with_params(query, &params)?;
    let explain_ns = start.elapsed().as_nanos();
    println!(
        "{}",
        json!({"event":"plan","scenario":scenario,"rows":rows,"keys":keys,"limit":limit,"setup_ns":setup_ns,"explain_ns":explain_ns,
        "plan":explain.physical_plan.explain(0),"cost":format!("{:?}",explain.trace.selected_plan_cost_breakdown),"decisions":explain.trace.decisions,
        "estimates":explain.trace.selected_plan_cardinality_estimates.iter().map(|entry|json!({"operator":format!("{:?}",entry.operator),"rows":entry.estimated_rows})).collect::<Vec<_>>()})
    );
    let mut read = db.begin_read_transaction()?;
    for run in 0..=repeats {
        let before_cache = read.plan_cache_stats();
        #[cfg(feature = "qualification")]
        let before = hawdb::query_preparation_metrics();
        let mut output = Vec::new();
        let start = Instant::now();
        let report = read.query_request_streaming(
            QueryRequest::new(query)
                .with_params(&params)
                .with_output_limits(QueryStreamOptions {
                    max_rows: Some(limit),
                    max_payload_bytes: Some(limit * 256 + 1024),
                }),
            |row| {
                output.push(row);
                Ok(())
            },
        )?;
        let elapsed_ns = start.elapsed().as_nanos();
        #[cfg(feature = "qualification")]
        let after = hawdb::query_preparation_metrics();
        if join {
            assert_eq!(output.len(), rows);
            for (id, row) in output.iter().enumerate() {
                assert_eq!(row["left_id"], Value::Int(id as i64));
                assert_eq!(row["right_id"], Value::Int(id as i64));
            }
        } else {
            assert_eq!(output.len(), groups.len());
            for (row, ((a, b), count)) in output.iter().zip(&groups) {
                assert_eq!(row["a"], Value::Int(*a as i64));
                assert_eq!(row["b"], Value::Int(*b as i64));
                assert_eq!(row["total"], Value::Int(*count));
            }
        }
        let after_cache = read.plan_cache_stats();
        let hits = after_cache.hits - before_cache.hits;
        if run > 0 {
            assert_eq!(hits, 1, "warm physical template hit");
        }
        let profile = report.execution_profile;
        assert!(report.fully_streamed);
        assert_eq!(
            profile.pipeline_memory_report.query_memory_completion_bytes,
            0
        );
        let mut sample = json!({"event":"query","scenario":scenario,"rows":rows,"run":run,"warmup":run==0,"elapsed_ns":elapsed_ns,"output_rows":output.len(),"physical_hits":hits,"physical_misses":after_cache.misses-before_cache.misses,
            "query_peak_bytes":profile.pipeline_memory_report.query_memory_peak_bytes,"intermediate_rows":profile.pipeline_memory_report.intermediate_rows,
            "blocking_peak_bytes":profile.blocking_operator_memory_reports.iter().map(|entry|entry.peak_tracked_bytes).max().unwrap_or(0),
            "actual_rows":profile.operator_cardinality_profiles.iter().map(|entry|json!({"operator":format!("{:?}",entry.operator),"rows":entry.actual_rows})).collect::<Vec<_>>(),
            "scans":profile.scan_pruning_reports.iter().map(|entry|json!({"candidates":entry.candidate_count_before_filter,"output_rows":entry.output_count})).collect::<Vec<_>>()});
        #[cfg(feature = "qualification")]
        {
            sample["parse_ns"] = json!(after.cypher_parse_nanos - before.cypher_parse_nanos);
            sample["plan_ns"] = json!(after.cypher_plan_nanos - before.cypher_plan_nanos);
        }
        sample["parity"] = json!(true);
        println!("{sample}");
    }
    Ok(())
}
