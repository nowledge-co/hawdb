// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual embedded checkpoint benchmark, including sealed-root publication.
use hawdb::{BranchSelector, Database, DatabaseConfig, StorageResidencyMode, Value};
use hawdb_storage::durable_manifest::DurableManifest;
use serde_json::json;
use std::path::Path;
use std::time::Instant;

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert_eq!(
        args.len(),
        5,
        "usage: graph_checkpoint_carry_bench <new-root> <nodes> <edges> <rounds> <carry|rebuild>"
    );
    let root = Path::new(&args[0]);
    assert!(!root.exists(), "fixture path must be new");
    let nodes: usize = args[1].parse().unwrap();
    let edges: usize = args[2].parse().unwrap();
    let rounds: usize = args[3].parse().unwrap();
    assert!(nodes > 1 && edges > 0 && rounds > 0);
    assert!(matches!(args[4].as_str(), "carry" | "rebuild"));
    for workload in ["node_property", "relationship_property"] {
        let path = root.join(workload);
        let mut config = DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            ..Default::default()
        };
        // Keep all checkpoint work inside the explicit timed calls.
        config.runtime_capabilities.background_maintenance = false;
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query("CREATE INDEX ON :Note(id)").unwrap();
        for id in 0..nodes {
            db.query(&format!("CREATE (:Note {{id: {id}, value: 0}})"))
                .unwrap();
        }
        for index in 0..edges {
            db.query(&format!("MATCH (s:Note {{id: {}}}), (t:Note {{id: {}}}) CREATE (s)-[:LINK {{weight: 0}}]->(t)", index % nodes, (index * 7 + 1) % nodes)).unwrap();
        }
        db.checkpoint().unwrap();
        let branch = db
            .describe_branch(BranchSelector::Name("main".into()))
            .unwrap()
            .id;
        let branch_path = path.join("branches").join(branch.to_string());
        let manifest_path = branch_path.join("runtime/manifest.hawdb");
        let initial = DurableManifest::load(&manifest_path)
            .unwrap()
            .canonical_adjacency_generation_artifacts
            .unwrap();
        for round in 0..rounds + 2 {
            let value = round as i64 + 1;
            let (update, verify, count) = if workload == "node_property" {
                (
                    format!("MATCH (n:Note) SET n.value = {value}"),
                    "MATCH (n:Note) RETURN n.value AS value",
                    nodes,
                )
            } else {
                (
                    format!("MATCH ()-[r:LINK]->() SET r.weight = {value}"),
                    "MATCH ()-[r:LINK]->() RETURN r.weight AS value",
                    edges,
                )
            };
            let mutation_started = Instant::now();
            db.query(&update).unwrap();
            let mutation_elapsed = mutation_started.elapsed();
            let started = Instant::now();
            db.checkpoint().unwrap();
            let elapsed = started.elapsed();
            let manifest = DurableManifest::load(&manifest_path).unwrap();
            let binding = manifest.canonical_adjacency_generation_artifacts.unwrap();
            assert_eq!(
                binding == initial,
                args[4] == "carry" && workload == "node_property"
            );
            let rows = db.query(verify).unwrap().rows;
            assert_eq!(rows.len(), count);
            assert!(rows.iter().all(|row| row["value"] == Value::Int(value)));
            let mut graph_bytes = 0;
            let mut adjacency_bytes = 0;
            for entry in std::fs::read_dir(branch_path.join("data")).unwrap() {
                let entry = entry.unwrap();
                let name = entry.file_name();
                let name = name.to_str().unwrap();
                if hawdb_storage::artifact_files::parse_graph_generation_file(name)
                    == Some(manifest.checkpoint_epoch)
                {
                    let bytes = entry.metadata().unwrap().len();
                    graph_bytes += bytes;
                    if name.starts_with("adjacency") {
                        adjacency_bytes += bytes;
                    }
                }
            }
            println!(
                "{}",
                json!({"api":"Database::checkpoint","automatic_checkpoint":false,"workload":workload,"nodes":nodes,"edges":edges,"round":round,"warmup":round<2,"checkpoint_ns":elapsed.as_nanos(),"mutation_ns":mutation_elapsed.as_nanos(),"new_graph_bytes":graph_bytes,"new_adjacency_bytes":adjacency_bytes,"durability":"SyncOnEveryWrite","residency":"OutOfCore"})
            );
        }
        db.scrub_storage().unwrap();
        drop(db);
        let mut reopened = Database::open_with_config(&path, config).unwrap();
        assert_eq!(
            reopened
                .query("MATCH (n:Note) RETURN n.id")
                .unwrap()
                .rows
                .len(),
            nodes
        );
        assert_eq!(
            reopened
                .query("MATCH ()-[r:LINK]->() RETURN r.weight")
                .unwrap()
                .rows
                .len(),
            edges
        );
        reopened.scrub_storage().unwrap();
    }
}
