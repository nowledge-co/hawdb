// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual directed two-hop cost qualification on one pinned synthetic graph.
//! Run with `cargo run --locked --release --example optimizer_path_statistics`.

use hawdb::optimizer::{
    OptimizerQualificationOptions, OptimizerRuleAssertion, OptimizerSearchDirective,
    OptionalOptimizerRule,
};
use hawdb::{Database, QueryRequest, Value};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let expect_kept_start = match args.as_slice() {
        [] => false,
        [flag] if flag == "--expect-kept-start" => true,
        _ => return Err("usage: optimizer_path_statistics [--expect-kept-start]".into()),
    };
    let mut db = Database::new();
    for query in [
        "CREATE INDEX ON :A(id)",
        "CREATE INDEX ON :B(key)",
        "CREATE INDEX ON :X(id)",
        "CREATE INDEX ON :C(id)",
    ] {
        db.query(query)?;
    }
    for (label, property, count) in [
        ("A", "id", 2_000),
        ("B", "key", 20),
        ("X", "id", 20),
        ("C", "id", 20),
    ] {
        db.query_with_params(
            &format!("UNWIND $rows AS r CREATE (:{label} {{{property}: r.id}})"),
            &BTreeMap::from([(
                "rows".into(),
                Value::List(
                    (0..count)
                        .map(|id| Value::Map(BTreeMap::from([("id".into(), Value::Int(id))])))
                        .collect(),
                ),
            )]),
        )?;
    }
    let roots = BTreeMap::from([(
        "rows".into(),
        Value::List(
            (0..2_000)
                .map(|id| {
                    Value::Map(BTreeMap::from([
                        ("id".into(), Value::Int(id)),
                        ("slot".into(), Value::Int(id % 20)),
                    ]))
                })
                .collect(),
        ),
    )]);
    for query in [
        "UNWIND $rows AS r MATCH (a:A {id: r.id}), (b:B {key: r.slot}) CREATE (a)-[:R]->(b)",
        "UNWIND $rows AS r MATCH (a:A {id: r.id}), (x:X {id: r.slot}) CREATE (a)-[:R]->(x)",
    ] {
        db.query_with_params(query, &roots)?;
    }
    let slots = BTreeMap::from([(
        "rows".into(),
        Value::List(
            (0..20)
                .map(|id| Value::Map(BTreeMap::from([("id".into(), Value::Int(id))])))
                .collect(),
        ),
    )]);
    for query in [
        "UNWIND $rows AS r MATCH (x:X {id: r.id}), (b:B {key: r.id}) CREATE (x)-[:R]->(b)",
        "UNWIND $rows AS r MATCH (b:B {key: r.id}), (c:C {id: r.id}) CREATE (b)-[:R]->(c)",
    ] {
        db.query_with_params(query, &slots)?;
    }
    let query = "MATCH (a:A)-[:R*2..2]->(b:B {key: 0})-[:R]->(c:C) RETURN a.id AS id ORDER BY id";
    let mut read = db.begin_read_transaction()?;
    let mut timings: [Vec<f64>; 2] = Default::default();
    let mut profiles = Vec::new();
    let mut expected = None;
    for round in 0..17 {
        // Alternate which strategy runs first; both fully execute on the same
        // snapshot with cache admission bypassed by the qualification API.
        for slot in 0..2 {
            let strategy = (slot + round) % 2;
            let options = if strategy == 0 {
                OptimizerQualificationOptions::default()
                    .without_rule(OptionalOptimizerRule::CostBasedPatternStart)
            } else {
                OptimizerQualificationOptions::default()
            };
            let start = Instant::now();
            let output = read.qualify_optimizer_query(
                QueryRequest::new(query),
                options,
                OptimizerSearchDirective::Auto,
            )?;
            let elapsed = start.elapsed().as_secs_f64() * 1_000.0;
            let chosen = output
                .verify_rules(&[OptimizerRuleAssertion::Required(
                    OptionalOptimizerRule::CostBasedPatternStart,
                )])
                .is_ok();
            assert_eq!(
                chosen,
                strategy == 1 && !expect_kept_start,
                "{:?}",
                output.optimizer_trace.decisions
            );
            let result = output.result?;
            assert_eq!(result.output.rows.len(), 100);
            for (index, row) in result.output.rows.iter().enumerate() {
                assert_eq!(row.get("id"), Some(&Value::Int((index * 20) as i64)));
            }
            let rows = format!("{:?}", result.output.rows);
            if let Some(expected) = &expected {
                assert_eq!(&rows, expected);
            } else {
                expected = Some(rows);
            }
            if round >= 2 {
                timings[strategy].push(elapsed);
            }
            if round == 2 {
                profiles.push(
                    json!({"strategy": strategy, "snapshot_epoch": output.snapshot_epoch,
                    "estimated_rows": output.optimizer_trace.selected_plan_cost.estimated_rows,
                    "cost": output.optimizer_trace.selected_plan_cost.cost,
                    "query_memory_peak_bytes": result.execution_profile.pipeline_memory_report.query_memory_peak_bytes,
                    "decisions": output.optimizer_trace.decisions,
                    "profile": format!("{:?}", result.execution_profile)}),
                );
            }
        }
    }
    let medians = timings.each_ref().map(|samples| {
        let mut sorted = samples.clone();
        sorted.sort_by(f64::total_cmp);
        sorted[sorted.len() / 2]
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "query": query, "roots": 2_000, "targets": 20, "rows": 100,
            "warmup_rounds": 2, "measurement_rounds": 15,
            "strategy_0": "text-order start; cost-based start disabled",
            "strategy_1": "ordinary optimizer",
            "expect_kept_start": expect_kept_start,
            "samples_ms": timings, "medians_ms": medians,
            "speedup": medians[0] / medians[1], "profiles": profiles
        }))?
    );
    Ok(())
}
