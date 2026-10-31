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
use crate::mutation::MatchedRelationshipCreate;

mod scaling;

fn id_filter(ids: &[NodeId]) -> PropertyFilter {
    PropertyFilter::IdIn {
        values: ids.iter().map(|id| Value::Int(id.0 as i64)).collect(),
    }
}

fn relationship(sources: &[NodeId], targets: &[NodeId]) -> GraphMutation {
    GraphMutation::CreateRelationshipsBetweenMatches(MatchedRelationshipCreate {
        source_label: "Memory".into(),
        source_filter: Some(id_filter(sources)),
        rel_type: "LINKS".into(),
        rel_properties: BTreeMap::new(),
        target_label: "Memory".into(),
        target_filter: Some(id_filter(targets)),
    })
}

#[test]
fn id_in_bounded_matching_agrees_with_scan_and_preserves_limits() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    let first = store
        .create_node(&mut catalog, "Memory", properties([]))
        .unwrap();
    let other = store
        .create_node(&mut catalog, "Other", properties([]))
        .unwrap();
    let last = store
        .create_node(&mut catalog, "Memory", properties([]))
        .unwrap();
    store.next_node_id = (i64::MAX as u64) + 1;
    let high = store
        .create_node(&mut catalog, "Memory", properties([]))
        .unwrap();
    let mut duplicate_values = vec![Value::Int(first.0 as i64); 4_096];
    duplicate_values.extend([Value::Int(999), Value::Int(other.0 as i64)]);
    let filters = [
        id_filter(&[]),
        id_filter(&[high, last, other, first, last, high]),
        id_filter(&[NodeId(999), NodeId(u64::MAX)]),
        PropertyFilter::IdIn {
            values: duplicate_values,
        },
        PropertyFilter::IdIn {
            values: vec![
                Value::Null,
                Value::Float(0.0),
                Value::Bool(false),
                Value::String("0".into()),
                Value::List(vec![Value::Int(0)]),
            ],
        },
        PropertyFilter::IdIn {
            values: vec![
                Value::Int(i64::MIN),
                Value::Int(-1),
                Value::Float(0.0),
                Value::Int(first.0 as i64),
                Value::Null,
                Value::Int(last.0 as i64),
            ],
        },
    ];
    for label in [
        None,
        catalog.label_id("Memory"),
        catalog.label_id("Other"),
        Some(LabelId(99)),
    ] {
        for filter in &filters {
            // The wrapper keeps the original owned-scan path as an independent oracle.
            let scan = PropertyFilter::Or(vec![filter.clone()]);
            for limit in 0..=4 {
                let expected = store
                    .matching_node_ids_bounded(label, Some(&scan), limit, "test_limit")
                    .map_err(|error| error.to_string());
                let actual = store
                    .matching_node_ids_bounded(label, Some(filter), limit, "test_limit")
                    .map_err(|error| error.to_string());
                assert_eq!(actual, expected, "label={label:?} limit={limit}");
            }
        }
    }
    assert_eq!(
        store
            .matching_node_ids_bounded(None, Some(&filters[1]), 4, "test_limit")
            .unwrap(),
        vec![first, other, last, high]
    );
}

#[test]
fn id_in_relationship_batches_preserve_cartesian_order_pending_and_recovery() {
    for residency_mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir("id_in_relationship_batch");
        let config = WalReplayConfig {
            residency_mode,
            ..WalReplayConfig::default()
        };
        let open = |catalog: &mut Catalog| {
            GraphStore::open_with_durability_and_replay_config(
                &path,
                catalog,
                DurabilityPolicy::default(),
                config,
            )
            .unwrap()
        };
        let mut catalog = Catalog::default();
        let mut store = open(&mut catalog);
        let source = store
            .create_node(&mut catalog, "Memory", properties([]))
            .unwrap();
        let deleted = store
            .create_node(&mut catalog, "Memory", properties([]))
            .unwrap();
        let updated = store
            .create_node(&mut catalog, "Memory", properties([]))
            .unwrap();
        let other = store
            .create_node(&mut catalog, "Other", properties([]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        drop(store);
        let mut store = open(&mut catalog);
        if residency_mode == StorageResidencyMode::OutOfCore {
            assert!(store.canonical_base_out_of_core);
            assert!(store.nodes.is_empty());
        }
        let snapshot = store.snapshot();
        store
            .commit_mutations(
                &mut catalog,
                vec![
                    GraphMutation::SetNodeProperty {
                        label: "Memory".into(),
                        filter: Some(id_filter(&[updated, updated, other])),
                        property: "marker".into(),
                        value: Value::Int(7),
                    },
                    GraphMutation::DeleteNode {
                        label: "Memory".into(),
                        filter: Some(id_filter(&[deleted, deleted, other])),
                        detach: false,
                    },
                ],
            )
            .unwrap();
        let resident = store
            .create_node(&mut catalog, "Memory", properties([]))
            .unwrap();
        let pending = NodeId(store.next_node_id);
        let pending_other = NodeId(pending.0 + 1);
        let mutations = || {
            let mut unknown = relationship(&[source], &[source]);
            if let GraphMutation::CreateRelationshipsBetweenMatches(request) = &mut unknown {
                request.source_label = "Unknown".into();
            }
            let mut unlabelled = relationship(&[pending_other, other, other], &[updated]);
            if let GraphMutation::CreateRelationshipsBetweenMatches(request) = &mut unlabelled {
                request.source_label.clear();
                request.target_label.clear();
            }
            vec![
                GraphMutation::CreateNode {
                    label: "Memory".into(),
                    properties: properties([]),
                },
                GraphMutation::CreateNode {
                    label: "Other".into(),
                    properties: properties([]),
                },
                unknown,
                relationship(&[], &[source]),
                relationship(&[deleted, other, pending_other, NodeId(999)], &[source]),
                relationship(
                    &[pending, updated, source, resident, updated, other, deleted],
                    &[pending, source, pending],
                ),
                unlabelled,
            ]
        };
        let before = (store.commit_epoch(), store.next_node_id, store.next_rel_id);
        for (rows, operations, limit_name) in [
            (11, 100, "max_mutation_affected_rows"),
            (12, 11, "max_mutation_operations"),
        ] {
            let limits = MutationLimits {
                max_affected_rows: NonZeroUsize::new(rows).unwrap(),
                max_operations: NonZeroUsize::new(operations).unwrap(),
                ..MutationLimits::default()
            };
            let error = store
                .commit_mutations_with_limits(&mut catalog, mutations(), limits)
                .unwrap_err();
            assert!(error.to_string().contains(limit_name), "{error}");
            assert_eq!(
                (store.commit_epoch(), store.next_node_id, store.next_rel_id),
                before
            );
            assert!(store.node_owned(pending).unwrap().is_none());
            assert_eq!(store.relationship_records_owned().count(), 0);
            assert!(catalog.rel_type_id("LINKS").is_none());
        }
        drop(store);
        store = open(&mut catalog);
        assert_eq!(
            (store.commit_epoch(), store.next_node_id, store.next_rel_id),
            before
        );
        assert!(store.node_owned(pending).unwrap().is_none());
        assert_eq!(store.relationship_records_owned().count(), 0);
        assert!(catalog.rel_type_id("LINKS").is_none());
        let limits = MutationLimits {
            max_affected_rows: NonZeroUsize::new(12).unwrap(),
            max_operations: NonZeroUsize::new(12).unwrap(),
            ..MutationLimits::default()
        };
        let result = store
            .commit_mutations_with_limits(&mut catalog, mutations(), limits)
            .unwrap();
        let expected = vec![
            (source, source),
            (source, pending),
            (updated, source),
            (updated, pending),
            (resident, source),
            (resident, pending),
            (pending, source),
            (pending, pending),
            (other, updated),
            (pending_other, updated),
        ];
        assert_eq!(result.rows.len(), 12);
        for (index, (row, (source, target))) in result.rows[2..].iter().zip(&expected).enumerate() {
            assert_eq!(row["source_node_id"], Value::Int(source.0 as i64));
            assert_eq!(row["target_node_id"], Value::Int(target.0 as i64));
            assert_eq!(row["rel_id"], Value::Int(index as i64));
        }
        assert!(snapshot.node_owned(pending).unwrap().is_none());
        assert!(snapshot.node_owned(resident).unwrap().is_none());
        assert!(snapshot.node_owned(deleted).unwrap().is_some());
        assert!(snapshot
            .node_owned(updated)
            .unwrap()
            .unwrap()
            .properties
            .is_empty());
        assert_eq!(snapshot.relationship_records_owned().count(), 0);
        drop(snapshot);
        for checkpoint in [false, true] {
            if checkpoint {
                store.checkpoint(&catalog).unwrap();
            }
            drop(store);
            store = open(&mut catalog);
            let actual: Vec<_> = store
                .relationship_records_owned()
                .map(|record| {
                    let record = record.unwrap();
                    (record.source, record.target)
                })
                .collect();
            assert_eq!(actual, expected);
            assert!(store.node_owned(deleted).unwrap().is_none());
            assert_eq!(
                store.node_owned(updated).unwrap().unwrap().properties["marker"],
                Value::Int(7)
            );
            assert!(store
                .node_owned(other)
                .unwrap()
                .unwrap()
                .properties
                .is_empty());
            assert!(store.node_owned(pending).unwrap().is_some());
            assert!(catalog.label_id("Unknown").is_none());
        }
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn id_led_endpoint_rechecks_properties_labels_and_large_fanout() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    let mut expected = Vec::new();
    let mut candidates = Vec::new();
    for index in 0..4_096 {
        let keep = index % 2 == 0;
        let id = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("keep", Value::Bool(keep))]),
            )
            .unwrap();
        candidates.push(id);
        if keep {
            expected.push(id);
        }
    }
    candidates.push(
        store
            .create_node(
                &mut catalog,
                "Other",
                properties([("keep", Value::Bool(true))]),
            )
            .unwrap(),
    );
    candidates.extend([NodeId(u64::MAX), candidates[0]]);
    let filter = PropertyFilter::And(vec![
        id_filter(&candidates),
        PropertyFilter::Eq {
            property: "keep".into(),
            value: Value::Bool(true),
        },
    ]);
    let label = catalog.label_id("Memory");
    assert_eq!(
        store
            .matching_node_ids_bounded(label, Some(&filter), expected.len(), "fanout")
            .unwrap(),
        expected
    );
    assert!(store
        .matching_node_ids_bounded(label, Some(&filter), expected.len() - 1, "fanout")
        .is_err());
    let pending = vec![
        (
            NodeId(9_000),
            label.unwrap(),
            properties([("keep", Value::Bool(true))]),
        ),
        (
            NodeId(9_001),
            label.unwrap(),
            properties([("keep", Value::Bool(false))]),
        ),
    ];
    let filter = PropertyFilter::And(vec![
        id_filter(&[NodeId(9_001), NodeId(9_000)]),
        PropertyFilter::Eq {
            property: "keep".into(),
            value: Value::Bool(true),
        },
    ]);
    assert_eq!(
        GraphStore::pending_node_ids_matching(label, Some(&filter), &pending),
        vec![NodeId(9_000)]
    );
}

#[test]
fn pending_relationship_merge_index_preserves_subsets_and_mixed_batches() {
    for mixed in [false, true] {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::default();
        let source = store
            .create_node(&mut catalog, "Memory", properties([]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties([]))
            .unwrap();
        let merge = |match_properties| {
            GraphMutation::MergeRelationshipsBetweenMatches(
                crate::mutation::MatchedRelationshipMerge {
                    source_label: "Memory".into(),
                    source_filter: Some(id_filter(&[source])),
                    target_label: "Memory".into(),
                    target_filter: Some(id_filter(&[target])),
                    rel_type: "LINK".into(),
                    rel_match_properties: match_properties,
                    on_create_properties: properties([("retained", Value::Bool(true))]),
                },
            )
        };
        let mut mutations = vec![merge(properties([("key", Value::Int(1))]))];
        if mixed {
            mutations.push(GraphMutation::CreateNode {
                label: "Unrelated".into(),
                properties: properties([]),
            });
        }
        mutations.extend([
            merge(properties([("key", Value::Int(2))])),
            merge(properties([])),
            merge(properties([("key", Value::Int(1))])),
        ]);
        let summary = store.commit_mutations(&mut catalog, mutations).unwrap();
        let rows = summary
            .rows
            .iter()
            .filter(|row| row.contains_key("rel_id"))
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows.iter()
                .map(|row| row["created"].clone())
                .collect::<Vec<_>>(),
            vec![
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false)
            ]
        );
        assert_ne!(rows[0]["rel_id"], rows[1]["rel_id"]);
        assert_eq!(rows[0]["rel_id"], rows[2]["rel_id"]);
        assert_eq!(rows[0]["rel_id"], rows[3]["rel_id"]);
    }
}
