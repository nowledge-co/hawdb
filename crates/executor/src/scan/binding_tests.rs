// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_storage::RelId;

#[test]
fn expansion_inherits_graph_payload_without_copying_and_detaches_mutations() {
    for body_bytes in [64, 16_384] {
        for rows in [20, 100, 512] {
            let seed = Binding {
                values: BTreeMap::new(),
                nodes: BTreeMap::from([(
                    "seed".into(),
                    NodeRecord {
                        id: NodeId(1),
                        labels: BTreeSet::new(),
                        properties: BTreeMap::from([(
                            "content".into(),
                            Value::String("x".repeat(body_bytes)),
                        )]),
                    },
                )])
                .into(),
                relationships: BTreeMap::from([(
                    "evidence".into(),
                    RelRecord {
                        id: RelId(1),
                        source: NodeId(1),
                        target: NodeId(2),
                        rel_type: RelTypeId(1),
                        properties: BTreeMap::from([(
                            "reason".into(),
                            Value::String("r".repeat(body_bytes)),
                        )]),
                    },
                )])
                .into(),
            };
            let mut copied_payload_bytes = 0;
            let mut expanded = Vec::with_capacity(rows);
            for index in 0..rows {
                let target = NodeRecord {
                    id: NodeId(index as u64 + 3),
                    labels: BTreeSet::new(),
                    properties: BTreeMap::new(),
                };
                let output = owned_expanded_binding(&seed, Some(("target", target)), None);
                if !std::ptr::eq(&seed.nodes["seed"], &output.nodes["seed"]) {
                    copied_payload_bytes += body_bytes;
                }
                if !std::ptr::eq(
                    &seed.relationships["evidence"],
                    &output.relationships["evidence"],
                ) {
                    copied_payload_bytes += body_bytes;
                }
                assert_eq!(output.nodes["target"].id, NodeId(index as u64 + 3));
                expanded.push(output);
            }
            assert_eq!(
                copied_payload_bytes, 0,
                "inherited graph records were copied"
            );
            eprintln!(
                "graph-binding-copy-audit-v1 rows={rows} body_bytes={body_bytes} copied_inherited_payload_bytes={copied_payload_bytes} shared_inherited_payload_bytes={}",
                2 * rows * body_bytes
            );
            // Shared payloads remain immutable to other rows and to the seed.
            expanded[0]
                .nodes
                .get_mut("seed")
                .unwrap()
                .properties
                .clear();
            expanded[0]
                .relationships
                .get_mut("evidence")
                .unwrap()
                .properties
                .clear();
            assert_eq!(seed.nodes["seed"].properties.len(), 1);
            assert_eq!(seed.relationships["evidence"].properties.len(), 1);
            assert_eq!(expanded[1].nodes["seed"].properties.len(), 1);
            assert_eq!(expanded[1].relationships["evidence"].properties.len(), 1);
        }
    }
}
