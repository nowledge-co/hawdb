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
use crate::predicate::property_filter_matches;

fn id_filter(id: NodeId) -> PropertyFilter {
    PropertyFilter::IdEq {
        value: Value::Int(id.0 as i64),
    }
}

fn relationship(source: NodeId, target: NodeId) -> GraphMutation {
    GraphMutation::CreateRelationshipsBetweenMatches(MatchedRelationshipCreate {
        source_label: "Memory".into(),
        source_filter: Some(id_filter(source)),
        rel_type: "LINKS".into(),
        rel_properties: BTreeMap::new(),
        target_label: "Memory".into(),
        target_filter: Some(id_filter(target)),
    })
}

#[test]
fn id_eq_bounded_matching_agrees_with_scan_and_preserves_limits() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    let first = store
        .create_node(&mut catalog, "Memory", properties([]))
        .unwrap();
    store
        .create_node(&mut catalog, "Other", properties([]))
        .unwrap();
    store.next_node_id = (i64::MAX as u64) + 1;
    let high = store
        .create_node(&mut catalog, "Memory", properties([]))
        .unwrap();
    let mut filters: Vec<_> = [
        Value::Int(first.0 as i64),
        Value::Int(1),
        Value::Int(42),
        Value::Int(-1),
        Value::Int(i64::MIN),
        Value::Null,
        Value::Float(0.0),
        Value::String("0".into()),
    ]
    .into_iter()
    .map(|value| PropertyFilter::IdEq { value })
    .collect();
    filters.push(PropertyFilter::And(vec![id_filter(first)]));
    filters.push(PropertyFilter::IdIn {
        values: vec![Value::Int(0), Value::Int(1)],
    });
    for label in [
        None,
        catalog.label_id("Memory"),
        catalog.label_id("Other"),
        Some(LabelId(99)),
    ] {
        for filter in &filters {
            let mut expected = Vec::new();
            store
                .visit_nodes_owned(label, |node| {
                    if property_filter_matches(filter, node.id.0, &node.properties) {
                        expected.push(node.id);
                    }
                    GraphScanControl::Continue
                })
                .unwrap();
            for limit in [0, 1, 2] {
                let result =
                    store.matching_node_ids_bounded(label, Some(filter), limit, "test_limit");
                if expected.len() > limit {
                    assert_eq!(
                        result.unwrap_err().to_string(),
                        format!("execution error: mutation would exceed test_limit {limit}")
                    );
                } else {
                    assert_eq!(result.unwrap(), expected, "{label:?} {filter:?}");
                }
            }
        }
    }
    assert_eq!(
        store
            .matching_node_ids_bounded(None, Some(&id_filter(high)), 1, "test_limit")
            .unwrap(),
        vec![high]
    );
}

#[test]
fn id_eq_relationship_batches_preserve_pending_nodes_limits_and_recovery() {
    for residency_mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir("id_eq_relationship_batch");
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
        let wrong_label = store
            .create_node(&mut catalog, "Other", properties([]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        drop(store);
        let mut store = open(&mut catalog);
        let snapshot = store.snapshot();
        store
            .delete_nodes(&mut catalog, "Memory", Some(&id_filter(deleted)), false)
            .unwrap();
        let pending = NodeId(store.next_node_id);
        let mutations = || {
            let mut unknown = relationship(source, source);
            if let GraphMutation::CreateRelationshipsBetweenMatches(request) = &mut unknown {
                request.source_label = "Unknown".into();
            }
            let mut unlabelled = relationship(pending, source);
            if let GraphMutation::CreateRelationshipsBetweenMatches(request) = &mut unlabelled {
                request.source_label.clear();
                request.target_label.clear();
            }
            vec![
                GraphMutation::CreateNode {
                    label: "Memory".into(),
                    properties: properties([]),
                },
                relationship(source, deleted),
                relationship(wrong_label, source),
                relationship(source, NodeId(999)),
                unknown,
                relationship(source, pending),
                unlabelled,
                relationship(pending, pending),
            ]
        };
        let before = (store.commit_epoch(), store.next_node_id, store.next_rel_id);
        let limits = MutationLimits {
            max_affected_rows: NonZeroUsize::new(3).unwrap(),
            ..MutationLimits::default()
        };
        let error = store
            .commit_mutations_with_limits(&mut catalog, mutations(), limits)
            .unwrap_err();
        assert!(error.to_string().contains("max_mutation_affected_rows 0"));
        assert_eq!(
            (store.commit_epoch(), store.next_node_id, store.next_rel_id),
            before
        );
        assert!(store.node_owned(pending).unwrap().is_none());
        assert!(catalog.rel_type_id("LINKS").is_none());
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
            max_affected_rows: NonZeroUsize::new(4).unwrap(),
            ..MutationLimits::default()
        };
        let result = store
            .commit_mutations_with_limits(&mut catalog, mutations(), limits)
            .unwrap();
        assert_eq!(result.rows.len(), 4);
        assert!(snapshot.node_owned(pending).unwrap().is_none());
        assert!(snapshot.node_owned(deleted).unwrap().is_some());
        assert_eq!(snapshot.relationship_records_owned().count(), 0);
        drop(snapshot);
        let expected = vec![(source, pending), (pending, source), (pending, pending)];
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
            assert!(store.node_owned(pending).unwrap().is_some());
            assert!(catalog.label_id("Unknown").is_none());
        }
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
#[ignore = "manual release-mode relationship batch scaling measurement"]
fn id_eq_relationship_scaling_probe() {
    use std::time::Instant;

    for nodes in [8, 1_000, 16_000] {
        for sample in 0..4 {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::default();
            store
                .commit_mutations(
                    &mut catalog,
                    (0..nodes)
                        .map(|id| GraphMutation::CreateNode {
                            label: "Memory".into(),
                            properties: properties([
                                ("key", Value::Int(id as i64)),
                                ("name", Value::String(format!("n{id}"))),
                            ]),
                        })
                        .collect(),
                )
                .unwrap();
            let mutations = (0..1_000)
                .map(|id| relationship(NodeId(id % nodes), NodeId((id * 7 + 1) % nodes)))
                .collect();
            let started = Instant::now();
            let result = store.commit_mutations(&mut catalog, mutations).unwrap();
            let elapsed = started.elapsed();
            assert_eq!(result.rows.len(), 1_000);
            for (id, record) in store.relationship_records_owned().enumerate() {
                let record = record.unwrap();
                assert_eq!(record.source, NodeId(id as u64 % nodes));
                assert_eq!(record.target, NodeId((id as u64 * 7 + 1) % nodes));
            }
            assert_eq!(store.relationship_records_owned().count(), 1_000);
            eprintln!(
                "nodes={nodes} relationships=1000 batch=1000 sample={sample} elapsed_us={}",
                elapsed.as_micros()
            );
        }
    }
}
