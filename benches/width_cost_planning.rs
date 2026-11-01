// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual source-bound width/sort experiment; complete output checks are
//! outside timing. Reopen means a fresh handle, not a device-cold cache.

use hawdb::{
    Database, DatabaseConfig, QueryRequest, QueryStreamOptions, StorageResidencyMode, Value, Vector,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

struct BenchDirectory(PathBuf);
impl BenchDirectory {
    fn new() -> std::io::Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "hawdb-width-bench-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for BenchDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert_eq!(args.len(), 8, "narrow|text|varchar|vector rows limit payload_bytes random|ascending|descending|native blocking_mib memory|materialized|canonical|auto repeats");
    let kind = &args[0];
    let rows: usize = args[1].parse()?;
    let limit: usize = args[2].parse()?;
    let bytes: usize = args[3].parse()?;
    let order = &args[4];
    let memory_mib: usize = args[5].parse()?;
    let residency = &args[6];
    let repeats: usize = args[7].parse()?;
    assert!((64..=100_000).contains(&rows) && (1..=rows).contains(&limit));
    assert!(bytes <= 8192 && (1..=512).contains(&memory_mib) && (1..=100).contains(&repeats));
    let body = match kind.as_str() {
        "narrow" => Value::Int(7),
        "text" | "varchar" => Value::String("x".repeat(bytes)),
        "vector" => {
            assert!(bytes >= 4 && bytes.is_multiple_of(4));
            Value::Vector(Vector::new(&vec![0.25; bytes / 4])?)
        }
        _ => return Err("unknown payload kind".into()),
    };
    let mut config = DatabaseConfig {
        execution_memory: hawdb::executor::ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(memory_mib * 1024 * 1024).unwrap(),
            query_memory_bytes: NonZeroUsize::new(256 * 1024 * 1024).unwrap(),
            ..Default::default()
        },
        storage_residency_mode: StorageResidencyMode::Materialized,
        ..Default::default()
    };
    let directory = BenchDirectory::new()?;
    let persistent = residency != "memory";
    assert!(["memory", "materialized", "canonical", "auto"].contains(&residency.as_str()));
    let started = Instant::now();
    let mut db = if persistent {
        Database::open_with_config(directory.path(), config.clone())?
    } else {
        Database::new_with_config(config.clone())
    };
    db.query("CREATE NODE TABLE Item")?;
    for (name, value_type) in [
        ("id", "STRING".to_string()),
        ("rank", "INT".into()),
        ("kind", "STRING".into()),
        (
            "body",
            match kind.as_str() {
                "narrow" => "INT".into(),
                "text" => "TEXT".into(),
                "varchar" => "STRING".into(),
                "vector" => format!("VECTOR({})", bytes / 4),
                _ => unreachable!(),
            },
        ),
    ] {
        db.query(&format!(
            "CREATE PROPERTY ON NODE TABLE Item({name}) TYPE {value_type} NOT NULL"
        ))?;
    }
    let mut ranks = (0..rows).collect::<Vec<_>>();
    match order.as_str() {
        "ascending" => {}
        "descending" => ranks.reverse(),
        "random" | "native" => {
            let mut seed = 0x4372_98a2_1578_abcdu64;
            for i in (1..rows).rev() {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                ranks.swap(i, (seed as usize) % (i + 1));
            }
        }
        _ => return Err("unknown order".into()),
    }
    let mut expected_ids = vec![String::new(); rows];
    for start in (0..rows).step_by(256) {
        let values = (start..(start + 256).min(rows))
            .map(|id| {
                let identity = format!("item-{id:08}");
                expected_ids[ranks[id]] = identity.clone();
                Value::Map(BTreeMap::from([
                    ("id".into(), Value::String(identity)),
                    ("rank".into(), Value::Int(ranks[id] as i64)),
                    ("body".into(), body.clone()),
                ]))
            })
            .collect();
        db.query_with_params(
            "UNWIND $rows AS r CREATE (:Item {kind:'note', id:r.id, rank:r.rank, body:r.body})",
            &BTreeMap::from([("rows".into(), Value::List(values))]),
        )?;
    }
    if order == "native" {
        db.query("CREATE CONSTRAINT ON :Item(id) ASSERT UNIQUE")?;
        db.query("CREATE INDEX ON :Item(kind, rank, id)")?;
    }
    if persistent {
        db.checkpoint()?;
        drop(db);
        config.storage_residency_mode = match residency.as_str() {
            "canonical" => StorageResidencyMode::OutOfCore,
            "auto" => StorageResidencyMode::Auto,
            _ => StorageResidencyMode::Materialized,
        };
        db = Database::open_with_config(directory.path(), config)?;
    }
    let setup_ns = started.elapsed().as_nanos();
    let query="MATCH (n:Item) WHERE n.kind = 'note' RETURN n.rank AS rank, n.id AS id, n.body AS body ORDER BY rank, id LIMIT $limit";
    let params = BTreeMap::from([("limit".into(), Value::Int(limit as i64))]);
    let started = Instant::now();
    let explain = db.explain_query_with_params(query, &params)?;
    let explain_ns = started.elapsed().as_nanos();
    println!(
        "{}",
        json!({"event":"plan","kind":kind,"rows":rows,"limit":limit,"bytes":bytes,"order":order,"blocking_mib":memory_mib,"residency":residency,"setup_ns":setup_ns,"explain_ns":explain_ns,"plan":explain.physical_plan.explain(0),"cost":format!("{:?}",explain.trace.selected_plan_cost_breakdown),"decisions":explain.trace.decisions})
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
                    max_payload_bytes: Some(limit * (bytes + 1024) + 1024),
                }),
            |row| {
                output.push(row);
                Ok(())
            },
        )?;
        let elapsed_ns = start.elapsed().as_nanos();
        #[cfg(feature = "qualification")]
        let after = hawdb::query_preparation_metrics();
        assert_eq!(output.len(), limit);
        for (rank, row) in output.iter().enumerate() {
            assert_eq!(row["rank"], Value::Int(rank as i64));
            assert_eq!(row["id"], Value::String(expected_ids[rank].clone()));
            assert_eq!(row["body"], body);
        }
        let after_cache = read.plan_cache_stats();
        if run > 0 {
            assert_eq!(after_cache.hits - before_cache.hits, 1);
            assert_eq!(after_cache.misses - before_cache.misses, 0);
        }
        assert!(report.fully_streamed);
        let profile = report.execution_profile;
        assert_eq!(
            profile.pipeline_memory_report.query_memory_completion_bytes,
            0
        );
        let sample = json!({"event":"query","run":run,"warmup":run==0,"elapsed_ns":elapsed_ns,"output_rows":output.len(),"physical_hits":after_cache.hits-before_cache.hits,"physical_misses":after_cache.misses-before_cache.misses,"query_peak_bytes":profile.pipeline_memory_report.query_memory_peak_bytes,"blocking":profile.blocking_operator_memory_reports.iter().map(|entry|json!({"operator":entry.operator,"peak":entry.peak_tracked_bytes,"input_rows":entry.input_rows,"spill_bytes":entry.spilled_bytes,"spill_runs":entry.spill_run_count,"spill_rows":entry.spilled_rows})).collect::<Vec<_>>(),"parity":true});
        #[cfg(feature = "qualification")]
        let sample = {
            let mut sample = sample;
            sample["parse_ns"] = json!(after.cypher_parse_nanos - before.cypher_parse_nanos);
            sample["plan_ns"] = json!(after.cypher_plan_nanos - before.cypher_plan_nanos);
            sample
        };
        println!("{sample}");
    }
    Ok(())
}
