// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Same-harness scan-work controls for pinned label-domain full verification.
use hawdb_storage::config::{DurabilityPolicy, StorageResidencyMode, WalReplayConfig};
use hawdb_storage::schema::Catalog;
use hawdb_storage::store::{
    GraphStore, ProjectionFeedLimits, ProjectionFullSourceItem, ProjectionFullSourceLimits,
};
use hawdb_storage::{NodeId, Value};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::time::Instant;

fn limits(work: usize, items: usize, bytes: usize) -> ProjectionFeedLimits {
    ProjectionFeedLimits {
        work: NonZeroUsize::new(work).unwrap(),
        items: NonZeroUsize::new(items).unwrap(),
        bytes: NonZeroUsize::new(bytes).unwrap(),
    }
}
fn main() {
    let root = std::env::args().nth(1).expect("new fixture root");
    let root = std::path::Path::new(&root);
    std::fs::create_dir(root).unwrap();
    let mut results = Vec::new();
    for (name, unrelated, selected, stride, payload) in [
        ("small", 32u64, 2u64, 1u64, 128usize),
        ("bulk", 4096, 32, 1, 1024),
        ("sparse", 256, 4, 4096, 128),
    ] {
        for cold in [false, true] {
            for repeat in 0..3 {
                let path = root.join(format!("{name}-{cold}-{repeat}"));
                let mut catalog = Catalog::default();
                let mut store = GraphStore::open(&path, &mut catalog).unwrap();
                let rows = (0..unrelated + selected)
                    .map(|i| {
                        (
                            NodeId(i * stride),
                            if i < unrelated {
                                "Other".to_owned()
                            } else {
                                "Memory".to_owned()
                            },
                            BTreeMap::from([("body".into(), Value::String("x".repeat(payload)))]),
                        )
                    })
                    .collect::<Vec<_>>();
                let expected = (unrelated..unrelated + selected)
                    .map(|id| NodeId(id * stride))
                    .collect::<BTreeSet<_>>();
                store
                    .import_graph_snapshot_rows(&mut catalog, rows, vec![])
                    .unwrap();
                store.checkpoint(&catalog).unwrap();
                if cold {
                    drop(store);
                    store = GraphStore::open_with_durability_and_replay_config(
                        &path,
                        &mut catalog,
                        DurabilityPolicy::default(),
                        WalReplayConfig {
                            residency_mode: StorageResidencyMode::OutOfCore,
                            ..Default::default()
                        },
                    )
                    .unwrap();
                }
                let label = catalog.label_id("Memory").unwrap();
                let started = Instant::now();
                let mut source = store
                    .capture_projection_source_domains(
                        limits(8_000_000, 8_000_000, 128 << 20),
                        ProjectionFullSourceLimits::default(),
                        Default::default(),
                        Some(vec![label]),
                        Some(vec![]),
                    )
                    .unwrap();
                let mut found = BTreeSet::new();
                let mut steps = 0;
                loop {
                    let step = source
                        .visit_next(limits(256, 64, 1 << 20), |item| {
                            if let ProjectionFullSourceItem::GraphNodeCandidate { node_id } = item {
                                assert!(found.insert(NodeId(node_id)));
                            }
                            Ok(())
                        })
                        .unwrap();
                    steps += 1;
                    if step.complete {
                        break;
                    }
                }
                let elapsed = started.elapsed().as_nanos();
                let usage = source.finish().unwrap();
                assert_eq!(found, expected, "complete independent fixture reference");
                results.push(json!({"case":name,"cold":cold,"repeat":repeat,"unrelated":unrelated,"selected":selected,"stride":stride,"payload_bytes":payload,"work":usage.work,"items":usage.items,"bytes":usage.bytes,"steps":steps,"elapsed_ns":elapsed,"exact_match":true}));
                drop(store);
                std::fs::remove_dir_all(path).unwrap();
            }
        }
    }
    println!("{}", json!({"cases":results}));
    std::fs::remove_dir(root).unwrap();
}
