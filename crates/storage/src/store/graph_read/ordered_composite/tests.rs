// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn assert_pages(store: &GraphStore, catalog: &Catalog) {
    let label = catalog.label_id("Item").unwrap();
    let properties = vec!["kind".to_string(), "rank".to_string(), "id".to_string()];
    let required = properties.iter().cloned().collect();
    let mut reference = Vec::new();
    store
        .visit_nodes_owned(Some(label), |node| {
            if node.properties.get("kind") == Some(&Value::String("note".into())) {
                let values = properties
                    .iter()
                    .map(|property| node.properties.get(property))
                    .collect::<Option<Vec<_>>>();
                if let Some(values) = values
                    && values.iter().all(|value| !matches!(value, Value::Null))
                {
                    reference.push((values.into_iter().cloned().collect::<Vec<_>>(), node.id));
                }
            }
            GraphScanControl::Continue
        })
        .unwrap();
    reference.sort();
    assert!(!reference.is_empty());
    for descending in [false, true] {
        let mut expected = reference.clone();
        if descending {
            expected.reverse();
        }
        let mut after = None;
        let mut actual = Vec::new();
        loop {
            let seek = CompositeOrderedSeek {
                index_properties: properties.clone(),
                equality_prefix: vec![("kind".into(), Value::String("note".into()))],
                after,
                descending,
            };
            let mut page = Vec::new();
            let control = store
                .visit_projected_nodes_by_access_owned(
                    label,
                    &hawdb_core::access::NodeProjectionAccess::CompositeOrdered { seek },
                    &required,
                    |node| {
                        let key = properties
                            .iter()
                            .map(|property| node.properties[property].clone())
                            .collect::<Vec<_>>();
                        page.push((key, node.id));
                        if page.len() == 3 {
                            GraphScanControl::Stop
                        } else {
                            GraphScanControl::Continue
                        }
                    },
                )
                .unwrap();
            after = page.last().map(|(key, _)| key[1..].to_vec());
            actual.extend(page);
            assert!(actual.len() <= expected.len());
            if control == GraphScanControl::Continue {
                break;
            }
        }
        assert_eq!(actual, expected, "descending={descending}");
    }
}

#[test]
fn ordered_composite_pages_merge_base_and_delta_on_each_retained_view() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-composite-merge-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
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
    store
        .create_composite_property_index(
            &mut catalog,
            "Item",
            &["kind".into(), "rank".into(), "id".into()],
        )
        .unwrap();
    for id in 0..32 {
        store
            .create_node(
                &mut catalog,
                "Item",
                BTreeMap::from([
                    (
                        "kind".into(),
                        Value::String(if id % 7 == 0 { "archive" } else { "note" }.into()),
                    ),
                    ("rank".into(), Value::Int(id / 4)),
                    ("id".into(), Value::String(format!("item-{id:03}"))),
                ]),
            )
            .unwrap();
    }
    store
        .create_node(
            &mut catalog,
            "Item",
            BTreeMap::from([
                ("kind".into(), Value::String("note".into())),
                ("rank".into(), Value::Int(2)),
            ]),
        )
        .unwrap();
    assert_pages(&store, &catalog);
    store.checkpoint(&catalog).unwrap();
    assert_pages(&store, &catalog);
    let retained = store.snapshot();
    store
        .set_node_property(
            &mut catalog,
            "Item",
            Some(&PropertyFilter::Eq {
                property: "id".into(),
                value: Value::String("item-001".into()),
            }),
            "rank",
            Value::Int(99),
        )
        .unwrap();
    store
        .set_node_property(
            &mut catalog,
            "Item",
            Some(&PropertyFilter::Eq {
                property: "id".into(),
                value: Value::String("item-002".into()),
            }),
            "kind",
            Value::String("archive".into()),
        )
        .unwrap();
    store
        .delete_nodes(
            &mut catalog,
            "Item",
            Some(&PropertyFilter::Eq {
                property: "id".into(),
                value: Value::String("item-003".into()),
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
                ("rank".into(), Value::Int(1)),
                ("id".into(), Value::String("new-tie".into())),
            ]),
        )
        .unwrap();
    assert_pages(&store, &catalog);
    assert_pages(&retained, &catalog);
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
    assert_pages(&store, &catalog);
    store.checkpoint(&catalog).unwrap();
    assert_pages(&store, &catalog);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
