// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual native experiment over one shared checkpoint image. Fixture writes
//! and opening are separate; each query includes preparation and full execution.

use hawdb::{Database, DatabaseConfig, JoinSamplingLimits, Value};
use hawdb_core::Catalog;
use hawdb_storage::config::StorageResidencyMode;
use hawdb_storage::store::{DurabilityPolicy, GraphStore, WalReplayConfig};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Instant;

struct Directory(std::path::PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn populate(
    path: &std::path::Path,
    hot: usize,
    two: bool,
    skewed: bool,
    indexed: bool,
) -> hawdb::Result<()> {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        path,
        &mut catalog,
        DurabilityPolicy::SyncOnCheckpoint,
        WalReplayConfig::default(),
    )?;
    let mut groups = Vec::new();
    for label in if two {
        &["A", "B", "C"][..]
    } else {
        &["A", "B"][..]
    } {
        let mut ids = Vec::new();
        for id in 0..4 * hot {
            ids.push(store.create_node(
                &mut catalog,
                label,
                BTreeMap::from([
                    ("id".into(), Value::Int(id as i64)),
                    ("kind".into(), Value::Int(i64::from(id < hot))),
                ]),
            )?);
        }
        store.create_property_index(&mut catalog, label, "id")?;
        if indexed {
            store.create_property_index(&mut catalog, label, "kind")?;
        }
        groups.push(ids);
    }
    for id in 0..4 * hot {
        let degree = if id < hot && skewed { 16 } else { 4 };
        for offset in 0..degree {
            let target = if !skewed {
                (id * 4 + offset) % (4 * hot)
            } else if id < hot && offset < 4 {
                (id * 4 + offset) % hot
            } else {
                hot + (id * 16 + offset) % (3 * hot)
            };
            store.create_relationship(
                &mut catalog,
                groups[0][id],
                groups[1][target],
                "R",
                BTreeMap::new(),
            )?;
        }
    }
    if two {
        for id in 0..4 * hot {
            for offset in 0..4 {
                let target = if !skewed {
                    (id * 4 + offset) % (4 * hot)
                } else if id < hot && offset == 0 {
                    id
                } else {
                    hot + (id * 4 + offset) % (3 * hot)
                };
                store.create_relationship(
                    &mut catalog,
                    groups[1][id],
                    groups[2][target],
                    "S",
                    BTreeMap::new(),
                )?;
            }
        }
    }
    store.checkpoint(&catalog)?;
    drop(store);
    // Initialize facade-owned system tables once, before either measured open.
    let mut db = Database::open(path)?;
    db.checkpoint()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert!((5..=6).contains(&args.len()), "one|two|point materialized|out-of-core hot_rows repeats skewed|uniform|no-index|refuse [candidate-first]");
    let two = args[0] == "two";
    assert!(two || args[0] == "one" || args[0] == "point");
    let mode = match args[1].as_str() {
        "materialized" => StorageResidencyMode::Materialized,
        "out-of-core" => StorageResidencyMode::OutOfCore,
        _ => return Err("unknown residency".into()),
    };
    let hot: usize = args[2].parse()?;
    let repeats: usize = args[3].parse()?;
    assert!((4..=4096).contains(&hot) && (1..=100).contains(&repeats));
    assert!(["skewed", "uniform", "no-index", "refuse"].contains(&args[4].as_str()));
    let indexed = args[4] != "no-index";
    let skewed = args[4] != "uniform";
    let directory = Directory(std::env::temp_dir().join(format!(
        "hawdb-join-sampling-bench-{}",
        hawdb_core::generate_uuidv7()?
    )));
    let start = Instant::now();
    populate(&directory.0, hot, two, skewed, indexed)?;
    println!(
        "{}",
        json!({"event":"fixture","scenario":args,"setup_ns":start.elapsed().as_nanos(),
        "note":"same checkpoint; fixture fits RAM; OutOfCore denotes residency only; storage-cache-cold is not OS-cache-cold"})
    );
    let query = if args[0] == "point" {
        "MATCH (a:A {id: $id}) RETURN count(*) AS total"
    } else if two {
        "MATCH (a:A {kind: $kind})-[:R]->(b:B {kind: $kind})-[:S]->(c:C {kind: $kind}) RETURN count(*) AS total"
    } else {
        "MATCH (a:A {kind: $kind})-[:R]->(b:B {kind: $kind}) RETURN count(*) AS total"
    };
    let params = BTreeMap::from([("kind".into(), Value::Int(1)), ("id".into(), Value::Int(0))]);
    let expected = if args[0] == "point" {
        1
    } else if args[4] == "uniform" {
        hot
    } else {
        4 * hot
    };
    let order = if args.get(5).is_some_and(|arg| arg == "candidate-first") {
        [true, false]
    } else {
        [false, true]
    };
    for enabled in order {
        let limits = JoinSamplingLimits {
            max_probes: if args[4] == "refuse" { 0 } else { 1024 },
            ..Default::default()
        };
        let start = Instant::now();
        let db = Database::open_with_config(
            &directory.0,
            DatabaseConfig {
                read_only: true,
                recovery_mode: hawdb::RecoveryMode::Strict,
                storage_residency_mode: mode,
                join_sampling: enabled.then_some(limits),
                ..Default::default()
            },
        )?;
        let mut read = db.begin_read_transaction()?;
        println!(
            "{}",
            json!({"event":"open","enabled":enabled,"open_ns":start.elapsed().as_nanos(), "sampling_limits":enabled.then(|| format!("{limits:?}"))})
        );
        for iteration in 0..=repeats {
            let before_cache = read.plan_cache_stats();
            let before = hawdb::query_preparation_metrics();
            let start = Instant::now();
            let output = read.query_with_params(query, &params)?;
            let total_ns = start.elapsed().as_nanos();
            let after = hawdb::query_preparation_metrics();
            assert_eq!(output.rows.len(), 1);
            assert_eq!(
                output.rows.iter().next().unwrap()["total"],
                Value::Int(expected as i64)
            );
            let after_cache = read.plan_cache_stats();
            println!(
                "{}",
                json!({"event":"query","enabled":enabled,"iteration":iteration,
                "first_call":iteration==0,"total_ns":total_ns,
                "parse_ns":after.cypher_parse_nanos-before.cypher_parse_nanos,
                "planning_including_sampling_ns":after.cypher_plan_nanos-before.cypher_plan_nanos,
                "sampling_ns":after.join_sampling_nanos-before.join_sampling_nanos,
                "sampling_calls":after.join_sampling_calls-before.join_sampling_calls,
                "cache_hits":after_cache.hits-before_cache.hits,"cache_misses":after_cache.misses-before_cache.misses,
                "rows_checked":expected})
            );
        }
        let explain = read.explain_query_with_params(query, &params)?;
        println!(
            "{}",
            json!({"event":"plan","enabled":enabled,"plan":explain.physical_plan.explain(0),
            "decisions":explain.trace.decisions,"cost":format!("{:?}",explain.trace.selected_plan_cost_breakdown)})
        );
    }
    Ok(())
}
