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
                &hawdb_core::access::NodeProjectionAccess::PropertyRange {
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
                    &hawdb_core::access::NodeProjectionAccess::PropertyRange {
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

const COMPOSITE_COVERAGE_INDEXES: &[&[&str]] = &[
    &["kind", "rank", "id"],
    &["kind", "bucket", "rank", "id"],
    &["kind", "rank"],
    &["kind", "bucket", "rank"],
];

fn assert_composite_coverage(store: &GraphStore, catalog: &Catalog) {
    let label = catalog.label_id("Item").unwrap();
    let properties = ["kind", "bucket", "rank", "id"]
        .map(String::from)
        .into_iter()
        .collect();
    for (index, keys) in COMPOSITE_COVERAGE_INDEXES.iter().enumerate() {
        for (lower_inclusive, upper_inclusive) in
            [(true, true), (true, false), (false, true), (false, false)]
        {
            let mut equality_prefix = vec![("kind".into(), Value::String("note".into()))];
            if keys.contains(&"bucket") {
                equality_prefix.push(("bucket".into(), Value::String("primary".into())));
            }
            let seek = hawdb_core::access::CompositeRangeSeek {
                index_properties: keys.iter().map(|key| (*key).into()).collect(),
                equality_prefix,
                range_property: "rank".into(),
                lower: Some((Value::Int(2), lower_inclusive)),
                upper: Some((Value::Int(6), upper_inclusive)),
            };
            let mut expected = BTreeSet::new();
            store
                .visit_nodes_owned(Some(label), |node| {
                    if node_matches_composite_range(&node, &seek) {
                        expected.insert(node.id);
                    }
                    GraphScanControl::Continue
                })
                .unwrap();
            assert!(!expected.is_empty());
            let class = PersistentGraphIndexClass::NodeCompositeEquality;
            let before = store
                .graph_index_read_metrics
                .snapshot()
                .operation_count(class);
            let mut actual = BTreeSet::new();
            store
                .visit_nodes_by_composite_range_owned(label, &seek, |node| {
                    assert!(actual.insert(node.id));
                    GraphScanControl::Continue
                })
                .unwrap();
            assert_eq!(actual, expected, "owned {seek:?}");
            let after = store
                .graph_index_read_metrics
                .snapshot()
                .operation_count(class);
            assert_eq!(
                after > before,
                store.canonical_base.is_some() && index >= 2,
                "index work {seek:?}"
            );
            let access =
                hawdb_core::access::NodeProjectionAccess::CompositeRange { seek: seek.clone() };
            let mut projected = BTreeSet::new();
            store
                .visit_projected_nodes_by_access_owned(label, &access, &properties, |node| {
                    assert!(projected.insert(node.id));
                    GraphScanControl::Continue
                })
                .unwrap();
            assert_eq!(projected, expected, "admitted {seek:?}");
            let mut stopped = 0;
            assert_eq!(
                store
                    .visit_nodes_by_composite_range_owned(label, &seek, |node| {
                        assert!(expected.contains(&node.id));
                        stopped += 1;
                        GraphScanControl::Stop
                    })
                    .unwrap(),
                GraphScanControl::Stop
            );
            assert_eq!(stopped, 1);
            let mut deny = |_: usize| -> Result<Box<dyn GraphReadAllocation>> {
                Err(HawDBError::Execution("composite admission refused".into()))
            };
            let error = store
                .visit_projected_nodes_by_access_admitted(
                    label,
                    &access,
                    &properties,
                    &mut crate::read_view::GraphReadAllocator::new(&mut deny, None),
                    &mut |_| panic!("admission refusal must precede delivery"),
                )
                .unwrap_err();
            assert!(
                matches!(error, HawDBError::Execution(message) if message == "composite admission refused")
            );
        }
    }
}

#[test]
fn sparse_composite_ranges_preserve_owned_and_admitted_domains() {
    let path = unique_test_dir("sparse-composite-coverage");
    let config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..Default::default()
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        config,
    )
    .unwrap();
    for keys in COMPOSITE_COVERAGE_INDEXES {
        store
            .create_composite_property_index(
                &mut catalog,
                "Item",
                &keys.iter().map(|key| (*key).into()).collect::<Vec<_>>(),
            )
            .unwrap();
    }
    for rank in 0..8 {
        store
            .create_node(
                &mut catalog,
                "Item",
                BTreeMap::from([
                    ("kind".into(), Value::String("note".into())),
                    ("bucket".into(), Value::String("primary".into())),
                    ("rank".into(), Value::Int(rank)),
                    ("id".into(), Value::Int(rank)),
                ]),
            )
            .unwrap();
    }
    for (rank, suffix) in [(4, None), (5, Some(Value::Null))] {
        let mut values = BTreeMap::from([
            ("kind".into(), Value::String("note".into())),
            ("bucket".into(), Value::String("primary".into())),
            ("rank".into(), Value::Int(rank)),
        ]);
        if let Some(value) = suffix {
            values.insert("id".into(), value);
        }
        store.create_node(&mut catalog, "Item", values).unwrap();
    }
    store
        .create_node(
            &mut catalog,
            "Item",
            BTreeMap::from([
                ("kind".into(), Value::String("note".into())),
                ("rank".into(), Value::Int(4)),
                ("id".into(), Value::Int(99)),
            ]),
        )
        .unwrap();
    assert_composite_coverage(&store, &catalog);
    store.checkpoint(&catalog).unwrap();
    assert_composite_coverage(&store, &catalog);
    let retained = store.snapshot();
    store
        .set_node_property(
            &mut catalog,
            "Item",
            Some(&PropertyFilter::Eq {
                property: "rank".into(),
                value: Value::Int(3),
            }),
            "id",
            Value::Null,
        )
        .unwrap();
    store
        .delete_nodes(
            &mut catalog,
            "Item",
            Some(&PropertyFilter::Eq {
                property: "id".into(),
                value: Value::Int(2),
            }),
            false,
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Item",
            BTreeMap::from([
                ("kind".into(), Value::String("note".into())),
                ("bucket".into(), Value::String("primary".into())),
                ("rank".into(), Value::Int(4)),
            ]),
        )
        .unwrap();
    assert_composite_coverage(&store, &catalog);
    assert_composite_coverage(&retained, &catalog);
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
    assert_composite_coverage(&store, &catalog);
    store.checkpoint(&catalog).unwrap();
    assert_composite_coverage(&store, &catalog);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
