// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual synchronized checkpoint measurements; never part of CI.
use hawdb_storage::config::{DurabilityPolicy, StorageResidencyMode, WalReplayConfig};
use hawdb_storage::durable_manifest::DurableManifest;
use hawdb_storage::mutation::RelationshipPropertyUpdate;
use hawdb_storage::schema::Catalog;
use hawdb_storage::store::GraphStore;
use hawdb_storage::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

fn main() {
    let args = std::env::args()
        .skip(1)
        .filter(|argument| argument != "--bench")
        .collect::<Vec<_>>();
    assert_eq!(
        args.len(),
        5,
        "usage: graph_checkpoint_carry <new-root> <nodes> <edges> <rounds> <carry|rebuild>"
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
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::SyncOnEveryWrite,
            WalReplayConfig {
                residency_mode: StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
        let mut ids = Vec::new();
        for _ in 0..nodes {
            ids.push(
                store
                    .create_node(
                        &mut catalog,
                        "Note",
                        BTreeMap::from([("value".into(), Value::Int(0))]),
                    )
                    .unwrap(),
            );
        }
        let mut relationships = Vec::new();
        for index in 0..edges {
            relationships.push(
                store
                    .create_relationship(
                        &mut catalog,
                        ids[index % nodes],
                        ids[(index * 7 + 1) % nodes],
                        "LINK",
                        BTreeMap::from([("weight".into(), Value::Int(0))]),
                    )
                    .unwrap(),
            );
        }
        store.checkpoint(&catalog).unwrap();
        let initial = DurableManifest::load(&path.join("manifest.hawdb"))
            .unwrap()
            .canonical_adjacency_generation_artifacts
            .unwrap();
        for round in 0..rounds + 2 {
            let value = Value::Int(round as i64 + 1);
            if workload == "node_property" {
                store
                    .set_node_property(&mut catalog, "Note", None, "value", value.clone())
                    .unwrap();
            } else {
                store
                    .set_relationship_property(
                        &mut catalog,
                        RelationshipPropertyUpdate {
                            source_label: "Note".into(),
                            filter: None,
                            rel_type: "LINK".into(),
                            target_label: "Note".into(),
                            target_filter: None,
                            rel_filter: None,
                            property: "weight".into(),
                            value: value.clone(),
                        },
                    )
                    .unwrap();
            }
            let started = Instant::now();
            store.checkpoint(&catalog).unwrap();
            let elapsed = started.elapsed();
            let manifest = DurableManifest::load(&path.join("manifest.hawdb")).unwrap();
            let binding = manifest.canonical_adjacency_generation_artifacts.unwrap();
            let should_carry = args[4] == "carry" && workload == "node_property";
            assert_eq!(binding == initial, should_carry);
            if workload == "node_property" {
                for id in &ids {
                    assert_eq!(
                        store.node_owned(*id).unwrap().unwrap().properties["value"],
                        value
                    );
                }
            } else {
                for id in &relationships {
                    assert_eq!(
                        store.relationship_owned(*id).unwrap().unwrap().properties["weight"],
                        value
                    );
                }
            }
            let mut graph_bytes = 0;
            let mut adjacency_bytes = 0;
            for entry in std::fs::read_dir(&path).unwrap() {
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
                json!({"workload":workload,"nodes":nodes,"edges":edges,"round":round,"warmup":round<2,"checkpoint_ns":elapsed.as_nanos(),"new_graph_bytes":graph_bytes,"new_adjacency_bytes":adjacency_bytes,"durability":"SyncOnEveryWrite","residency":"OutOfCore"})
            );
        }
        store.scrub_storage().unwrap();
        drop(store);
        let mut reopened = GraphStore::open(&path, &mut catalog).unwrap();
        assert_eq!(reopened.node_count_for_label(None), nodes);
        for id in &relationships {
            assert!(reopened.relationship_owned(*id).unwrap().is_some());
        }
        reopened.scrub_storage().unwrap();
    }
}
