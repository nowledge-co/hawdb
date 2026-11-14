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

#[test]
#[ignore = "manual release-mode pending endpoint scaling measurement"]
fn pending_id_relationship_scaling_probe() {
    for nodes in [8, 1_000, 16_000] {
        for width in [1, 8] {
            for sample in 0..4 {
                for direct in if sample % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    let mut catalog = Catalog::default();
                    let mut store = GraphStore::default();
                    let mut mutations: Vec<_> = (0..nodes)
                        .map(|id| GraphMutation::CreateNode {
                            label: "Memory".into(),
                            properties: properties([("key", Value::Int(id as i64))]),
                        })
                        .collect();
                    let mut expected = Vec::new();
                    for index in 0..100 {
                        let sources: Vec<_> = (0..width)
                            .map(|offset| ((index * 31 + offset) % nodes) as u64)
                            .collect();
                        let target = ((index * 7 + 1) % nodes) as u64;
                        expected.extend(
                            sources
                                .iter()
                                .copied()
                                .collect::<BTreeSet<_>>()
                                .into_iter()
                                .map(|source| (source, target)),
                        );
                        let filter = if width == 1 {
                            id_eq(sources[0])
                        } else {
                            id_in(&sources)
                        };
                        mutations.push(GraphMutation::CreateRelationshipsBetweenMatches(
                            MatchedRelationshipCreate {
                                source_label: "Memory".into(),
                                source_filter: Some(if direct {
                                    filter
                                } else {
                                    PropertyFilter::Or(vec![filter])
                                }),
                                target_label: "Memory".into(),
                                target_filter: Some(if direct {
                                    id_eq(target)
                                } else {
                                    PropertyFilter::Or(vec![id_eq(target)])
                                }),
                                rel_type: "LINKS".into(),
                                rel_properties: properties([]),
                            },
                        ));
                    }
                    let start = Instant::now();
                    let result = store.commit_mutations(&mut catalog, mutations).unwrap();
                    let elapsed = start.elapsed();
                    assert_eq!(store.node_count_for_label(None), nodes);
                    assert_eq!(result.rows.len(), nodes + expected.len());
                    for (index, (row, (source, target))) in
                        result.rows[nodes..].iter().zip(&expected).enumerate()
                    {
                        assert_eq!(row["source_node_id"], Value::Int(*source as i64));
                        assert_eq!(row["target_node_id"], Value::Int(*target as i64));
                        assert_eq!(row["rel_id"], Value::Int(index as i64));
                    }
                    let actual: Vec<_> = store
                        .relationship_records_owned()
                        .map(|record| {
                            let record = record.unwrap();
                            (record.source.0, record.target.0)
                        })
                        .collect();
                    assert_eq!(actual, expected);
                    println!(
                        "pending_id_relationship nodes={nodes} width={width} direct={direct} sample={sample} mutations=100 relationships={} elapsed_s={:.9}",
                        expected.len(), elapsed.as_secs_f64()
                    );
                }
            }
        }
    }
}
