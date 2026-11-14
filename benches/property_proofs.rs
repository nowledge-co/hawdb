// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual proof-elision experiment. Check every returned row outside timing.

use hawdb::{Database, QueryRequest, QueryStreamOptions, Value};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert_eq!(
        args.len(),
        4,
        "distinct|constant|constant-source|group|nullable|point|computed|schema rows warm|cold repeats"
    );
    let kind = &args[0];
    let rows: usize = args[1].parse()?;
    let cold = match args[2].as_str() {
        "warm" => false,
        "cold" => true,
        _ => panic!("unknown cache mode"),
    };
    let repeats: usize = args[3].parse()?;
    assert!((64..=20000).contains(&rows) && (1..=20).contains(&repeats));
    let started = Instant::now();
    let mut db = Database::new();
    for query in [
        "CREATE NODE TABLE Item",
        "CREATE PROPERTY ON NODE TABLE Item(external) TYPE INT NOT NULL",
        "CREATE PROPERTY ON NODE TABLE Item(space) TYPE STRING NOT NULL",
        "CREATE PROPERTY ON NODE TABLE Item(optional) TYPE INT",
        "CREATE CONSTRAINT ON :Item(external) ASSERT UNIQUE",
        "CREATE CONSTRAINT ON :Item(optional) ASSERT UNIQUE",
    ] {
        db.query(query)?;
    }
    if kind == "schema" {
        for table in 0..64 {
            db.query(&format!("CREATE NODE TABLE Unrelated{table}"))?;
            for property in 0..8 {
                db.query(&format!(
                    "CREATE PROPERTY ON NODE TABLE Unrelated{table}(field{property}) TYPE STRING"
                ))?;
            }
        }
    }
    let data = (0..rows)
        .map(|ordinal| {
            let value = (ordinal * 7919) % rows;
            Value::Map(BTreeMap::from([
                ("value".into(), Value::Int(value as i64)),
                (
                    "optional".into(),
                    if value.is_multiple_of(2) {
                        Value::Int(value as i64)
                    } else {
                        Value::Null
                    },
                ),
            ]))
        })
        .collect();
    // Use a bijective deterministic permutation for every supported row count.
    assert_eq!(gcd(7919, rows), 1);
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Item {external:r.value,space:'mem',optional:r.optional})",
        &BTreeMap::from([("rows".into(), Value::List(data))]),
    )?;
    let setup_ns = started.elapsed().as_nanos();
    let body=match kind.as_str() {
        "distinct"|"schema"=>"MATCH (n:Item) RETURN DISTINCT n.external AS id ORDER BY id",
        "constant"=>"MATCH (n:Item) WHERE n.space = $space RETURN n.external AS id, n.space AS space ORDER BY space",
        "constant-source"=>"MATCH (n:Item) WHERE n.space = $space RETURN n.external AS id ORDER BY n.space",
        "group"=>"MATCH (n:Item) WITH n.external AS id, count(*) AS original RETURN id, count(*) AS total ORDER BY id",
        "nullable"=>"MATCH (n:Item) RETURN DISTINCT n.optional AS id",
        "point"=>"MATCH (n:Item) WHERE n.external = $id RETURN n.external AS id",
        "computed"=>"MATCH (n:Item) RETURN DISTINCT n.external + 1 AS id ORDER BY id",
        _=>return Err("unknown workload".into()),
    };
    let cypher = if cold {
        format!("CYPHER system.optimizer_search = 'memo' {body}")
    } else {
        body.into()
    };
    let parameters = BTreeMap::from([
        ("space".into(), Value::String("mem".into())),
        ("id".into(), Value::Int(20)),
    ]);
    let expected_ids = match kind.as_str() {
        "point" => vec![Value::Int(20)],
        "nullable" => std::iter::once(Value::Null)
            .chain((0..rows).step_by(2).map(|id| Value::Int(id as i64)))
            .collect(),
        "computed" => (1..=rows).map(|id| Value::Int(id as i64)).collect(),
        _ => (0..rows).map(|id| Value::Int(id as i64)).collect(),
    };
    let mut expected = expected_ids
        .into_iter()
        .map(|id| {
            let mut row = BTreeMap::from([("id".into(), id)]);
            if kind == "constant" {
                row.insert("space".into(), Value::String("mem".into()));
            }
            if kind == "group" {
                row.insert("total".into(), Value::Int(1));
            }
            row
        })
        .collect::<Vec<_>>();
    expected.sort();
    let mut read = db.begin_read_transaction()?;
    let started = Instant::now();
    let explain = read.explain_query_with_params(&cypher, &parameters)?;
    println!(
        "{}",
        json!({"event":"plan","kind":kind,"rows":rows,"cache":args[2],"setup_ns":setup_ns,
        "explain_ns":started.elapsed().as_nanos(),"plan":explain.trace.selected_plan,
        "decisions":explain.trace.decisions})
    );
    for run in 0..=repeats {
        let before_cache = read.plan_cache_stats();
        #[cfg(feature = "qualification")]
        let before = hawdb::query_preparation_metrics();
        let mut output = Vec::new();
        let started = Instant::now();
        let report = read.query_request_streaming(
            QueryRequest::new(&cypher)
                .with_params(&parameters)
                .with_output_limits(QueryStreamOptions {
                    max_rows: Some(rows),
                    max_payload_bytes: Some(rows * 128),
                }),
            |row| {
                output.push(row);
                Ok(())
            },
        )?;
        let elapsed_ns = started.elapsed().as_nanos();
        #[cfg(feature = "qualification")]
        let after = hawdb::query_preparation_metrics();
        if !matches!(kind.as_str(), "constant" | "constant-source" | "nullable") {
            assert_eq!(output, expected);
        }
        output.sort();
        assert_eq!(output, expected);
        assert!(report.fully_streamed);
        let cache = read.plan_cache_stats();
        if run > 0 && !cold {
            assert_eq!(cache.hits - before_cache.hits, 1);
            assert_eq!(cache.misses - before_cache.misses, 0);
        }
        let profile = report.execution_profile;
        assert_eq!(
            profile.pipeline_memory_report.query_memory_completion_bytes,
            0
        );
        let sample = json!({"event":"query","run":run,"elapsed_ns":elapsed_ns,"parity":true,
            "output_rows":output.len(),"physical_hits":cache.hits-before_cache.hits,"physical_misses":cache.misses-before_cache.misses,
            "query_peak_bytes":profile.pipeline_memory_report.query_memory_peak_bytes,
            "blocking":profile.blocking_operator_memory_reports.iter().map(|entry|json!({"operator":entry.operator,
                "input_rows":entry.input_rows,"peak_bytes":entry.peak_tracked_bytes,"spill_bytes":entry.spilled_bytes})).collect::<Vec<_>>()});
        #[cfg(feature = "qualification")]
        let sample = {
            let mut sample = sample;
            sample["plan_ns"] = json!(after.cypher_plan_nanos - before.cypher_plan_nanos);
            sample
        };
        println!("{sample}");
    }
    Ok(())
}
fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
