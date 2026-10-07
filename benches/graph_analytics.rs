// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Local qualification over the embedded runtime. Optional --database PATH
//! PROJECTION BLOCKING_KIB reads a prepared export without changing its head.

use hawdb::{
    Database, DatabaseConfig, DurabilityPolicy, ProcessMemorySnapshot, QueryStreamOptions,
    StorageResidencyMode, Value,
};
use hawdb_core::schema::Catalog;
use hawdb_storage::config::WalReplayConfig;
use hawdb_storage::store::GraphStore;
use hawdb_storage::{NodeId, RelId};
use serde_json::json;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn main() {
    let mut arguments: Vec<_> = std::env::args_os().skip(1).collect();
    // Cargo appends this flag even for harness-free benchmark executables.
    if arguments.last().is_some_and(|arg| arg == "--bench") {
        arguments.pop();
    }
    if arguments.first().is_some_and(|arg| arg == "--database") {
        assert_eq!(
            arguments.len(),
            4,
            "--database PATH PROJECTION BLOCKING_KIB"
        );
        let projection = arguments[2].to_str().expect("UTF-8 projection name");
        let budget = arguments[3]
            .to_str()
            .unwrap()
            .parse::<usize>()
            .unwrap()
            .checked_mul(1024)
            .unwrap();
        measure(Path::new(&arguments[1]), projection, budget);
        return;
    }
    let shapes = if cfg!(debug_assertions) {
        vec![
            ("sparse_smoke", 512, 700, 1024 * 1024),
            ("scaled_smoke", 1024, 1400, 2 * 1024 * 1024),
            ("edge_heavy_smoke", 128, 16256, 96 * 1024),
        ]
    } else {
        vec![
            (
                "representative_size_synthetic",
                33000,
                45000,
                24 * 1024 * 1024,
            ),
            ("scaled_synthetic", 66000, 90000, 48 * 1024 * 1024),
            ("edge_heavy_synthetic", 256, 65280, 192 * 1024),
        ]
    };
    for (name, nodes, edges, budget) in shapes {
        let path = std::env::temp_dir().join(format!(
            "hawdb-analytics-bench-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        seed(&path, nodes, edges);
        // Exclude fixture construction and give every shape an independent RSS
        // high-water mark. This process is only a developer benchmark wrapper.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--database")
            .arg(&path)
            .arg("graph")
            .arg((budget / 1024).to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        println!(
            "graph_analytics {}",
            json!({
                "fixture": name, "synthetic": true, "nodes": nodes, "relationships": edges,
                "measurements": serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
            })
        );
        std::fs::remove_dir_all(path).unwrap();
    }
}

fn seed(path: &Path, nodes: usize, edges: usize) {
    {
        let mut catalog = Catalog::default();
        // Explicit relaxed durability is confined to disposable fixture setup;
        // its complete checkpoint precedes the measured read-only reopen.
        let mut store = GraphStore::open_with_durability_and_replay_config(
            path,
            &mut catalog,
            DurabilityPolicy::SyncOnCheckpoint,
            WalReplayConfig {
                max_batch_operations: Some(nodes + edges),
                ..WalReplayConfig::default()
            },
        )
        .unwrap();
        let node_rows = (0..nodes)
            .map(|id| {
                (
                    NodeId(id as u64),
                    "Memory".into(),
                    BTreeMap::from([("id".into(), Value::Int(id as i64))]),
                )
            })
            .collect();
        let relationship_rows = (0..edges)
            .map(|edge| {
                let source = edge % nodes;
                let target = (source + 1 + edge / nodes) % nodes;
                (
                    RelId(edge as u64),
                    NodeId(source as u64),
                    NodeId(target as u64),
                    "LINK".into(),
                    BTreeMap::new(),
                )
            })
            .collect();
        store
            .import_graph_snapshot_rows(&mut catalog, node_rows, relationship_rows)
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    let mut db = Database::open(path).unwrap();
    db.query("CALL project_graph('graph', ['Memory'], ['LINK'])")
        .unwrap();
    db.checkpoint().unwrap();
}

fn measure(path: &Path, projection: &str, budget: usize) {
    let mut config = DatabaseConfig {
        read_only: true,
        max_read_result_rows: Some(200000),
        max_read_result_payload_bytes: Some(64 * 1024 * 1024),
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 2 * 1024 * 1024,
        ..DatabaseConfig::default()
    };
    config.execution_memory.blocking_operator_bytes = NonZeroUsize::new(budget).unwrap();
    config.execution_memory.query_memory_bytes = NonZeroUsize::new(128 * 1024 * 1024).unwrap();
    config.execution_memory.batch_rows = NonZeroUsize::new(64).unwrap();
    let opened = Instant::now();
    let db = Database::open_with_config(path, config).unwrap();
    let open_nanos = opened.elapsed().as_nanos();
    let epoch = db.commit_epoch().unwrap();
    let mut reader = db.begin_read_transaction().unwrap();
    let baseline = ProcessMemorySnapshot::capture().unwrap();
    let graph = projection.replace('\\', "\\\\").replace('\'', "\\'");
    let mut records = Vec::new();
    for (algorithm, columns) in [
        ("page_rank", "node, pagerank_score"),
        ("louvain", "node, level, louvain_id"),
    ] {
        let bytes_before = db
            .storage_residency_report()
            .unwrap()
            .graph_index_reads
            .adjacency_bytes_read;
        let start = Instant::now();
        let report = reader.query_with_params_streaming(
            &format!("CALL {algorithm}('{graph}', maxIterations := $iterations, maxLevels := $levels) RETURN {columns}"),
            &BTreeMap::from([("iterations".into(), Value::Int(3)), ("levels".into(), Value::Int(2))]),
            QueryStreamOptions { max_rows: Some(200000), max_payload_bytes: Some(64 * 1024 * 1024) },
            |_| Ok(()),
        ).unwrap();
        assert!(report.fully_streamed);
        let memory = ProcessMemorySnapshot::capture().unwrap();
        let bytes_read = db
            .storage_residency_report()
            .unwrap()
            .graph_index_reads
            .adjacency_bytes_read
            - bytes_before;
        records.push(json!({
            "algorithm": algorithm, "max_iterations": 3, "max_levels": 2,
            "output_rows": report.output_rows, "wall_nanos": start.elapsed().as_nanos(),
            "peak_rss": memory.peak_resident_bytes, "baseline_peak_rss": baseline.peak_resident_bytes,
            "adjacency_bytes_read": bytes_read,
            "query_peak_bytes": report.execution_profile.pipeline_memory_report.query_memory_peak_bytes,
            "operators": report.execution_profile.blocking_operator_memory_reports.iter().map(|operator| json!({
                "operator": operator.operator, "budget_bytes": operator.budget_bytes,
                "peak_tracked_bytes": operator.peak_tracked_bytes,
            })).collect::<Vec<_>>(),
        }));
    }
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    println!(
        "{}",
        json!({
            "source_epoch": epoch, "read_only": true, "open_nanos": open_nanos,
            "blocking_budget_bytes": budget, "query_budget_bytes": 128 * 1024 * 1024,
            "segment_cache_bytes": 2 * 1024 * 1024, "results": records,
        })
    );
}
