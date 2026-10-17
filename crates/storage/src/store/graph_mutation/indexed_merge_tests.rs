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

use super::*;
use crate::config::{DurabilityPolicy, StorageResidencyMode, WalReplayConfig};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

fn properties(key: Value, suffix: i64) -> BTreeMap<String, Value> {
    BTreeMap::from([("key".into(), key), ("suffix".into(), Value::Int(suffix))])
}

fn merge(key: i64) -> GraphMutation {
    GraphMutation::MergeNode {
        label: "Item".into(),
        match_properties: BTreeMap::from([("key".into(), Value::Int(key))]),
        on_create_properties: BTreeMap::new(),
        on_match_assignments: Vec::new(),
        post_merge_assignments: Vec::new(),
    }
}

fn commit(
    store: &mut GraphStore,
    catalog: &mut Catalog,
    graph: Vec<GraphMutation>,
) -> MutationSummary {
    store
        .commit_kernel_write_batch(
            catalog,
            KernelWriteBatch {
                graph,
                ..KernelWriteBatch::default()
            },
            MutationLimits::default(),
        )
        .unwrap()
}

fn scan_match(
    store: &GraphStore,
    label: LabelId,
    properties: &BTreeMap<String, Value>,
) -> Option<NodeId> {
    let mut found = None;
    store
        .visit_nodes_owned(Some(label), |node| {
            if properties
                .iter()
                .all(|(key, value)| node.properties.get(key) == Some(value))
            {
                found = Some(node.id);
                GraphScanControl::Stop
            } else {
                GraphScanControl::Continue
            }
        })
        .unwrap();
    found
}

#[test]
fn indexed_merge_matches_scan_for_scalar_kinds_residuals_and_fallbacks() {
    for kind in 0..4 {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let values = [
            Value::Null,
            Value::Bool(false),
            Value::Int(0),
            Value::Float(0.0),
            Value::Float(-0.0),
            Value::Float(f64::NAN),
            Value::String("a\0λ".into()),
            Value::List(vec![Value::Int(1)]),
        ];
        for value in &values {
            for label in ["Other", "Item"] {
                for suffix in [0, 1, 1] {
                    store
                        .create_node(&mut catalog, label, properties(value.clone(), suffix))
                        .unwrap();
                }
            }
        }
        store
            .create_node(&mut catalog, "Item", BTreeMap::new())
            .unwrap();
        match kind {
            0 => {}
            1 => {
                store
                    .create_property_index(&mut catalog, "Item", "key")
                    .unwrap();
            }
            2 => {
                store
                    .create_range_property_index(&mut catalog, "Item", "key")
                    .unwrap();
            }
            3 => {
                store
                    .create_full_text_property_index(&mut catalog, "Item", "key")
                    .unwrap();
            }
            _ => unreachable!(),
        }
        for label in ["Item", "Other", "Empty"] {
            let label_id = catalog.get_or_create_label(label);
            let mut predicates = vec![
                BTreeMap::new(),
                BTreeMap::from([("absent".into(), Value::Null)]),
            ];
            for value in values.iter().chain([Value::Int(99)].iter()) {
                predicates.push(BTreeMap::from([("key".into(), value.clone())]));
                for suffix in [0, 1, 2] {
                    predicates.push(properties(value.clone(), suffix));
                }
            }
            for predicate in predicates {
                assert_eq!(
                    store
                        .find_node_by_label_and_properties(&catalog, label_id, &predicate)
                        .unwrap(),
                    scan_match(&store, label_id, &predicate),
                    "kind={kind} label={label} predicate={predicate:?}",
                );
            }
        }
    }
}

#[test]
fn indexed_merge_does_not_use_unbackfilled_batch_indexes() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let existing = store
        .create_node(&mut catalog, "Item", properties(Value::Int(7), 0))
        .unwrap();
    let result = commit(
        &mut store,
        &mut catalog,
        vec![
            GraphMutation::CreateIndex {
                label: "Item".into(),
                property: "key".into(),
            },
            merge(7),
            merge(8),
            merge(8),
        ],
    );
    assert_eq!(result.rows[1]["node_id"], Value::Int(existing.0 as i64));
    assert_eq!(result.rows[1]["created"], Value::Bool(false));
    assert_eq!(result.rows[2]["created"], Value::Bool(true));
    assert_eq!(result.rows[3]["created"], Value::Bool(false));
    assert_eq!(result.rows[2]["node_id"], result.rows[3]["node_id"]);
    assert_eq!(store.node_count_for_label(None), 2);
    assert_eq!(
        commit(&mut store, &mut catalog, vec![merge(7)]).rows[0]["created"],
        Value::Bool(false)
    );
}

fn test_dir() -> std::path::PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    loop {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("hawdb-indexed-merge-{}-{id}", std::process::id()));
        match std::fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => panic!("reserve test directory: {error}"),
        }
    }
}

#[test]
fn indexed_merge_preserves_snapshots_atomic_rejection_and_recovery() {
    for residency_mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = test_dir();
        let config = WalReplayConfig {
            residency_mode,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            config,
        )
        .unwrap();
        store
            .create_property_index(&mut catalog, "Item", "key")
            .unwrap();
        let first = store
            .create_node(&mut catalog, "Item", properties(Value::Int(1), 0))
            .unwrap();
        let second = store
            .create_node(&mut catalog, "Item", properties(Value::Int(1), 1))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        assert_eq!(
            store.canonical_base_out_of_core,
            residency_mode == StorageResidencyMode::OutOfCore
        );
        let snapshot = store.snapshot();
        store
            .set_node_properties_by_ids(
                &mut catalog,
                &[first],
                &[NodeSetAssignment {
                    property: "key".into(),
                    value: NodeSetValue::Value(Value::Int(2)),
                }],
            )
            .unwrap();
        let result = commit(
            &mut store,
            &mut catalog,
            vec![merge(1), merge(2), merge(3), merge(3)],
        );
        assert_eq!(result.rows[0]["node_id"], Value::Int(second.0 as i64));
        assert_eq!(result.rows[1]["node_id"], Value::Int(first.0 as i64));
        assert_eq!(result.rows[2]["created"], Value::Bool(true));
        assert_eq!(result.rows[3]["created"], Value::Bool(false));
        let label = catalog.label_id("Item").unwrap();
        let old_key = BTreeMap::from([("key".into(), Value::Int(1))]);
        assert_eq!(
            snapshot
                .find_node_by_label_and_properties(&catalog, label, &old_key)
                .unwrap(),
            Some(first)
        );
        store
            .delete_nodes(
                &mut catalog,
                "Item",
                Some(&PropertyFilter::IdEq {
                    value: Value::Int(second.0 as i64),
                }),
                false,
            )
            .unwrap();
        let before = store.commit_epoch();
        let error = store
            .commit_kernel_write_batch(
                &mut catalog,
                KernelWriteBatch {
                    graph: vec![merge(1), merge(4)],
                    ..KernelWriteBatch::default()
                },
                MutationLimits {
                    max_operations: NonZeroUsize::MIN,
                    ..MutationLimits::default()
                },
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("max_mutation_operations"),
            "{error}"
        );
        assert_eq!(store.commit_epoch(), before);
        assert_eq!(
            store
                .find_node_by_label_and_properties(&catalog, label, &old_key)
                .unwrap(),
            None
        );
        assert_eq!(
            snapshot
                .find_node_by_label_and_properties(&catalog, label, &old_key)
                .unwrap(),
            Some(first)
        );
        drop(snapshot);
        drop(store);
        for checkpoint in [false, true] {
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                config,
            )
            .unwrap();
            assert_eq!(store.node_count_for_label(Some(label)), 2);
            for key in [1, 2, 3, 4] {
                let predicate = BTreeMap::from([("key".into(), Value::Int(key))]);
                assert_eq!(
                    store
                        .find_node_by_label_and_properties(&catalog, label, &predicate)
                        .unwrap(),
                    scan_match(&store, label, &predicate)
                );
            }
            let result = commit(&mut store, &mut catalog, vec![merge(2), merge(3)]);
            assert!(result
                .rows
                .iter()
                .all(|row| row["created"] == Value::Bool(false)));
            if !checkpoint {
                store.checkpoint(&catalog).unwrap();
            }
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
#[ignore = "manual release benchmark for indexed kernel merges"]
fn indexed_merge_scaling_probe() {
    for nodes in [8usize, 1_000, 16_000] {
        for existing in [true, false] {
            for sample in 0..4 {
                let mut catalog = Catalog::default();
                let mut store = GraphStore::in_memory();
                store
                    .create_property_index(&mut catalog, "Item", "key")
                    .unwrap();
                commit(
                    &mut store,
                    &mut catalog,
                    (0..nodes)
                        .map(|key| GraphMutation::CreateNode {
                            label: "Item".into(),
                            properties: BTreeMap::from([
                                ("key".into(), Value::Int(key as i64)),
                                ("name".into(), Value::String(format!("n{key}"))),
                            ]),
                        })
                        .collect(),
                );
                let keys = (0..1_000)
                    .map(|offset| {
                        if existing {
                            offset * nodes / 1_000
                        } else {
                            nodes + offset
                        }
                    })
                    .collect::<Vec<_>>();
                let batch = keys.iter().map(|key| merge(*key as i64)).collect();
                let start = Instant::now();
                let result = commit(&mut store, &mut catalog, batch);
                let elapsed = start.elapsed().as_secs_f64() * 1_000.0;
                assert_eq!(result.rows.len(), 1_000);
                assert_eq!(
                    store.node_count_for_label(None),
                    nodes + if existing { 0 } else { 1_000 }
                );
                for (row, key) in result.rows.iter().zip(keys) {
                    assert_eq!(row["created"], Value::Bool(!existing));
                    let Value::Int(id) = row["node_id"] else {
                        panic!("node ID must be an integer")
                    };
                    assert_eq!(
                        store
                            .node_owned(NodeId(id as u64))
                            .unwrap()
                            .unwrap()
                            .properties["key"],
                        Value::Int(key as i64)
                    );
                }
                eprintln!(
                    "nodes={nodes} existing={existing} sample={sample} milliseconds={elapsed:.3}"
                );
            }
        }
    }
}
