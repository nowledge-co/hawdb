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
use std::time::Instant;

fn populate(store: &mut GraphStore, catalog: &mut Catalog, nodes: usize) {
    store
        .commit_mutations(
            catalog,
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
}

#[test]
#[ignore = "manual release-mode ID-list lookup scaling measurement"]
fn id_in_lookup_scaling_probe() {
    for cold in [false, true] {
        for nodes in [8, 1_000, 16_000] {
            let path = unique_test_dir("id_in_lookup_scaling");
            let mut catalog = Catalog::default();
            let open = |catalog: &mut Catalog| {
                GraphStore::open_with_durability_and_replay_config(
                    &path,
                    catalog,
                    DurabilityPolicy::default(),
                    WalReplayConfig {
                        residency_mode: StorageResidencyMode::OutOfCore,
                        ..WalReplayConfig::default()
                    },
                )
                .unwrap()
            };
            let mut store = if cold {
                open(&mut catalog)
            } else {
                GraphStore::default()
            };
            populate(&mut store, &mut catalog, nodes);
            if cold {
                store.checkpoint(&catalog).unwrap();
                drop(store);
                store = open(&mut catalog);
                assert!(store.canonical_base_out_of_core);
                assert!(store.nodes.is_empty());
            }
            let label = catalog.label_id("Memory");
            for width in [1, 8, 64] {
                for existing in [false, true] {
                    let ids: Vec<_> = (0..width)
                        .rev()
                        .map(|index| {
                            let position = (index * nodes / width + nodes / 2) % nodes;
                            NodeId((position + if existing { 0 } else { nodes }) as u64)
                        })
                        .collect();
                    let expected: Vec<_> = if existing {
                        ids.iter()
                            .copied()
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let filter = id_filter(&ids);
                    let scan = PropertyFilter::Or(vec![filter.clone()]);
                    for sample in 0..4 {
                        for direct in if sample % 2 == 0 {
                            [false, true]
                        } else {
                            [true, false]
                        } {
                            let selected = if direct { &filter } else { &scan };
                            let start = Instant::now();
                            let results: Vec<_> = (0..100)
                                .map(|_| {
                                    store
                                        .matching_node_ids_bounded(
                                            label,
                                            Some(selected),
                                            width,
                                            "test_limit",
                                        )
                                        .unwrap()
                                })
                                .collect();
                            let elapsed = start.elapsed();
                            assert!(results.iter().all(|actual| *actual == expected));
                            println!(
                                "id_in_lookup cold={cold} nodes={nodes} width={width} existing={existing} direct={direct} sample={sample} lookups=100 elapsed_s={:.9}",
                                elapsed.as_secs_f64()
                            );
                        }
                    }
                }
            }
            assert_eq!(store.node_count_for_label(None), nodes);
            drop(store);
            if cold {
                fs::remove_dir_all(path).unwrap();
            }
        }
    }
}

#[test]
#[ignore = "manual release-mode ID-list relationship batch scaling measurement"]
fn id_in_relationship_scaling_probe() {
    for nodes in [8, 1_000, 16_000] {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::default();
        populate(&mut store, &mut catalog, nodes);
        for width in [1, 8, 64] {
            let sources: Vec<_> = (0..width)
                .rev()
                .map(|i| NodeId((i % nodes) as u64))
                .collect();
            let ordered: Vec<_> = sources
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let expected: Vec<_> = (0..100)
                .flat_map(|index| {
                    ordered
                        .iter()
                        .map(move |source| (*source, NodeId((index % nodes) as u64)))
                })
                .collect();
            for sample in 0..4 {
                for direct in if sample % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    let mut candidate = store.snapshot();
                    let mut candidate_catalog = catalog.clone();
                    let mutations = (0..100)
                        .map(|index| {
                            let mut mutation =
                                relationship(&sources, &[NodeId((index % nodes) as u64)]);
                            if !direct {
                                let GraphMutation::CreateRelationshipsBetweenMatches(request) =
                                    &mut mutation
                                else {
                                    unreachable!();
                                };
                                request.source_filter = request
                                    .source_filter
                                    .take()
                                    .map(|filter| PropertyFilter::Or(vec![filter]));
                                request.target_filter = request
                                    .target_filter
                                    .take()
                                    .map(|filter| PropertyFilter::Or(vec![filter]));
                            }
                            mutation
                        })
                        .collect();
                    let start = Instant::now();
                    let result = candidate
                        .commit_mutations(&mut candidate_catalog, mutations)
                        .unwrap();
                    let elapsed = start.elapsed();
                    assert_eq!(result.rows.len(), expected.len());
                    for (index, (row, (source, target))) in
                        result.rows.iter().zip(&expected).enumerate()
                    {
                        assert_eq!(row["source_node_id"], Value::Int(source.0 as i64));
                        assert_eq!(row["target_node_id"], Value::Int(target.0 as i64));
                        assert_eq!(row["rel_id"], Value::Int(index as i64));
                    }
                    let actual: Vec<_> = candidate
                        .relationship_records_owned()
                        .map(|record| {
                            let record = record.unwrap();
                            (record.source, record.target)
                        })
                        .collect();
                    assert_eq!(actual, expected);
                    assert_eq!(candidate.node_count_for_label(None), nodes);
                    println!(
                        "id_in_relationship nodes={nodes} width={width} direct={direct} sample={sample} mutations=100 relationships={} elapsed_s={:.9}",
                        expected.len(), elapsed.as_secs_f64()
                    );
                }
            }
        }
    }
}
