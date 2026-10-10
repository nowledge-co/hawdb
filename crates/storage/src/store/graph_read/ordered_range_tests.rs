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
use crate::graph_engine::GraphReadEngine;

fn unique_test_dir(name: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("hawdb-{name}-{}-{nanos}", std::process::id()))
}

fn assert_ordered(store: &GraphStore, catalog: &Catalog) {
    let label = catalog.label_id("Item").unwrap();
    assert!(store.supports_ordered_node_range(label, "rank"));
    for (lower, upper) in [
        (Some((Value::Null, true)), None),
        (Some((Value::Int(0), false)), None),
        (
            Some((Value::Int(10), true)),
            Some((Value::Float(25.5), false)),
        ),
        (Some((Value::Int(10), false)), Some((Value::Int(30), true))),
        (Some((Value::Int(100), false)), None),
    ] {
        let mut expected = Vec::new();
        store
            .visit_nodes_owned(Some(label), |node| {
                if let Some(value) = node.properties.get("rank")
                    && range_bounds_match(value, lower.as_ref(), upper.as_ref())
                {
                    expected.push((value.clone(), node.id));
                }
                GraphScanControl::Continue
            })
            .unwrap();
        expected.sort();
        let mut actual = Vec::new();
        store
            .visit_nodes_by_property_range_owned(
                label,
                "rank",
                lower.as_ref(),
                upper.as_ref(),
                |node| {
                    actual.push((node.properties["rank"].clone(), node.id));
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(actual, expected);
        let mut projected = Vec::new();
        store
            .visit_projected_nodes_by_access_owned(
                label,
                &hawdb_plan_cypher::NodeProjectionAccess::PropertyRange {
                    property: "rank".into(),
                    lower: lower.clone(),
                    upper: upper.clone(),
                },
                &BTreeSet::new(),
                |node| {
                    projected.push((node.properties["rank"].clone(), node.id));
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(projected, expected);
        if !expected.is_empty() {
            let mut first = Vec::new();
            let control = store
                .visit_nodes_by_property_range_owned(
                    label,
                    "rank",
                    lower.as_ref(),
                    upper.as_ref(),
                    |node| {
                        first.push((node.properties["rank"].clone(), node.id));
                        GraphScanControl::Stop
                    },
                )
                .unwrap();
            assert_eq!(control, GraphScanControl::Stop);
            assert_eq!(first, expected[..1]);
            let mut calls = 0;
            let control = store
                .visit_projected_nodes_by_access_owned(
                    label,
                    &hawdb_plan_cypher::NodeProjectionAccess::PropertyRange {
                        property: "rank".into(),
                        lower: lower.clone(),
                        upper: upper.clone(),
                    },
                    &BTreeSet::new(),
                    |node| {
                        assert_eq!((node.properties["rank"].clone(), node.id), expected[0]);
                        calls += 1;
                        GraphScanControl::Stop
                    },
                )
                .unwrap();
            assert_eq!(control, GraphScanControl::Stop);
            assert_eq!(calls, 1);
        }
    }
}

fn insert_fixture(store: &mut GraphStore, catalog: &mut Catalog, rotation: usize) {
    let values = [
        Value::Int(30),
        Value::Int(10),
        Value::Float(15.5),
        Value::Int(20),
        Value::Null,
        Value::Int(10),
        Value::Float(30.0),
        Value::Int(-1),
    ];
    for i in 0..values.len() {
        store
            .create_node(
                catalog,
                "Item",
                BTreeMap::from([
                    (
                        "rank".to_string(),
                        values[(i + rotation) % values.len()].clone(),
                    ),
                    ("id".to_string(), Value::Int(i as i64)),
                ]),
            )
            .unwrap();
    }
    store.create_node(catalog, "Item", BTreeMap::new()).unwrap();
}

#[test]
fn ordered_range_in_memory_uses_value_order_and_keeps_unindexed_fallback() {
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    insert_fixture(&mut store, &mut catalog, 0);
    let label = catalog.label_id("Item").unwrap();
    let mut count = 0;
    store
        .visit_nodes_by_property_range_owned(label, "rank", None, None, |_| {
            count += 1;
            GraphScanControl::Continue
        })
        .unwrap();
    assert_eq!(count, 8);
    store
        .create_range_property_index(&mut catalog, "Item", "rank")
        .unwrap();
    assert_ordered(&store, &catalog);
}

#[test]
fn ordered_range_checkpoint_delta_differential_campaign() {
    for rotation in 0..8 {
        let path = unique_test_dir("ordered-range");
        let config = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
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
            .create_range_property_index(&mut catalog, "Item", "rank")
            .unwrap();
        insert_fixture(&mut store, &mut catalog, rotation);
        assert_ordered(&store, &catalog);
        store.checkpoint(&catalog).unwrap();
        assert_ordered(&store, &catalog);
        let retained = store.snapshot();
        for (id, value) in [(0, Value::Int(25)), (2, Value::Int(5)), (3, Value::Null)] {
            store
                .set_node_property(
                    &mut catalog,
                    "Item",
                    Some(&PropertyFilter::Eq {
                        property: "id".to_string(),
                        value: Value::Int(id),
                    }),
                    "rank",
                    value,
                )
                .unwrap();
        }
        store
            .delete_nodes(
                &mut catalog,
                "Item",
                Some(&PropertyFilter::Eq {
                    property: "id".to_string(),
                    value: Value::Int(1),
                }),
                false,
            )
            .unwrap();
        insert_fixture(&mut store, &mut catalog, rotation + 1);
        assert_ordered(&store, &catalog);
        assert_ordered(&retained, &catalog);
        drop(retained);
        drop(store);
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            config,
        )
        .unwrap();
        assert_ordered(&store, &catalog);
        store.checkpoint(&catalog).unwrap();
        assert_ordered(&store, &catalog);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn ordered_range_missing_projection_is_not_an_ordering_capability() {
    let path = unique_test_dir("range-fallback");
    let config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
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
    insert_fixture(&mut store, &mut catalog, 0);
    store.checkpoint(&catalog).unwrap();
    store
        .create_range_property_index(&mut catalog, "Item", "rank")
        .unwrap();
    let label = catalog.label_id("Item").unwrap();
    assert!(!store.supports_ordered_node_range(label, "rank"));
    let mut ranks = Vec::new();
    store
        .visit_nodes_by_property_range_owned(
            label,
            "rank",
            Some(&(Value::Int(0), false)),
            None,
            |node| {
                ranks.push(node.properties["rank"].clone());
                GraphScanControl::Continue
            },
        )
        .unwrap();
    assert_eq!(ranks.len(), 6);
    assert_eq!(ranks[0], Value::Int(30));
    store.checkpoint(&catalog).unwrap();
    assert_ordered(&store, &catalog);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
