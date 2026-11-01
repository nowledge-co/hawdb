// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual source-bound row-goal comparison. One fresh process per matrix cell;
//! setup, explain and complete result assertions are outside each timed call.

use hawdb::{Database, QueryRequest, Value};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Instant;

fn populate(db: &mut Database, rows: usize, skewed: bool) -> hawdb::Result<()> {
    db.query("CREATE NODE TABLE Item")?;
    db.query("CREATE PROPERTY ON NODE TABLE Item(rank) TYPE INT NOT NULL")?;
    db.query("CREATE PROPERTY ON NODE TABLE Item(id) TYPE STRING NOT NULL")?;
    for start in (0..rows).step_by(1024) {
        let batch = (start..(start + 1024).min(rows))
            .map(|id| {
                let kind = if skewed { id / (rows / 100) } else { id % 100 };
                Value::Map(BTreeMap::from([
                    ("id".into(), Value::String(format!("item-{id:08}"))),
                    ("rank".into(), Value::Int(id as i64)),
                    ("space".into(), Value::String("default".into())),
                    ("kind".into(), Value::Int(kind as i64)),
                ]))
            })
            .collect();
        db.query_with_params(
            "UNWIND $rows AS r CREATE (:Item {id: r.id, rank: r.rank, space: r.space, kind: r.kind})",
            &BTreeMap::from([("rows".into(), Value::List(batch))]),
        )?;
    }
    db.query("CREATE CONSTRAINT ON :Item(id) ASSERT UNIQUE")?;
    db.query("CREATE INDEX ON :Item(kind)")?;
    db.query("CREATE INDEX ON :Item(space, rank, id)")?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert_eq!(
        args.len(),
        5,
        "rows offset repeats uniform|skewed filtered|prefix"
    );
    let rows: usize = args[0].parse()?;
    let offset: usize = args[1].parse()?;
    let repeats: usize = args[2].parse()?;
    let skewed = match args[3].as_str() {
        "uniform" => false,
        "skewed" => true,
        _ => return Err("expected uniform or skewed".into()),
    };
    let prefix_only = match args[4].as_str() {
        "filtered" => false,
        "prefix" => true,
        _ => return Err("expected filtered or prefix".into()),
    };
    assert!((100..=1_000_000).contains(&rows) && rows.is_multiple_of(100));
    assert!((1..=100).contains(&repeats));
    let mut db = Database::new();
    let start = Instant::now();
    populate(&mut db, rows, skewed)?;
    let setup_ns = start.elapsed().as_nanos();
    let params = BTreeMap::from([
        ("space".into(), Value::String("default".into())),
        ("kind".into(), Value::Int(99)),
    ]);
    for bounded in [true, false] {
        if prefix_only && !bounded {
            continue;
        }
        let window = if bounded {
            format!(" SKIP {offset} LIMIT 20")
        } else {
            String::new()
        };
        let residual = if prefix_only {
            ""
        } else {
            " AND n.kind = $kind"
        };
        let query = format!("MATCH (n:Item) WHERE n.space = $space{residual} RETURN n.rank AS rank, n.id AS id ORDER BY rank ASC, id ASC{window}");
        let start = Instant::now();
        let explain = db.explain_query_with_params(&query, &params)?;
        let explain_ns = start.elapsed().as_nanos();
        let expected = (0..rows)
            .filter(|id| {
                if prefix_only {
                    true
                } else if skewed {
                    id / (rows / 100) == 99
                } else {
                    id % 100 == 99
                }
            })
            .skip(if bounded { offset } else { 0 })
            .take(if bounded { 20 } else { usize::MAX })
            .collect::<Vec<_>>();
        println!(
            "{}",
            json!({"event":"plan", "rows":rows,"offset":offset,"skewed":skewed,"prefix_only":prefix_only,"bounded":bounded,"setup_ns":setup_ns,"explain_ns":explain_ns,
            "plan":explain.physical_plan.explain(0), "cost":format!("{:?}",explain.trace.selected_plan_cost_breakdown),"decisions":explain.trace.decisions})
        );
        let mut read = db.begin_read_transaction()?;
        for run in 0..=repeats {
            let mut output = Vec::new();
            #[cfg(feature = "qualification")]
            let before = hawdb::query_preparation_metrics();
            let start = Instant::now();
            let report = read.query_request_streaming(
                QueryRequest::new(&query).with_params(&params),
                |row| {
                    output.push(row);
                    Ok(())
                },
            )?;
            let elapsed_ns = start.elapsed().as_nanos();
            #[cfg(feature = "qualification")]
            let after = hawdb::query_preparation_metrics();
            assert_eq!(output.len(), expected.len());
            for (actual, expected) in output.iter().zip(&expected) {
                assert_eq!(actual["rank"], Value::Int(*expected as i64));
                assert_eq!(actual["id"], Value::String(format!("item-{expected:08}")));
            }
            let profile = report.execution_profile;
            assert_eq!(
                profile.pipeline_memory_report.query_memory_completion_bytes,
                0
            );
            let mut sample = json!({"event":"query","rows":rows,"offset":offset,"skewed":skewed,"prefix_only":prefix_only,"bounded":bounded,"run":run,"warmup":run==0,"elapsed_ns":elapsed_ns,"output_rows":output.len(),"fully_streamed":report.fully_streamed,
                "query_peak_bytes":profile.pipeline_memory_report.query_memory_peak_bytes,
                "blocking_peak_bytes":profile.blocking_operator_memory_reports.iter().map(|item| item.peak_tracked_bytes).max().unwrap_or(0),
                "scans":profile.scan_pruning_reports.iter().map(|item|json!({"strategy":format!("{:?}",item.strategy),"candidates":item.candidate_count_before_filter,"output_rows":item.output_count})).collect::<Vec<_>>()});
            #[cfg(feature = "qualification")]
            {
                sample["parse_ns"] = json!(after.cypher_parse_nanos - before.cypher_parse_nanos);
                sample["plan_ns"] = json!(after.cypher_plan_nanos - before.cypher_plan_nanos);
            }
            // Keep the same harness in ordinary and instrumented builds.
            sample["parity"] = json!(true);
            println!("{sample}");
        }
    }
    Ok(())
}
