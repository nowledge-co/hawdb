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
use crate::wal::binary::{
    decode_binary_wal_record, encode_binary_wal_record, BinaryWalRecordDecode,
};
use crate::wal::WalEntry;
use hawdb_core::{PropertyType, SchemaObjectState, TableKind, Value};
use std::sync::Arc;

type Properties = BTreeMap<String, Value>;

fn node(id: u64, value: Value) -> WalOp {
    WalOp::CreateNode {
        id: NodeId(id),
        label: "Memory".into(),
        properties: BTreeMap::from([("value".into(), value)]),
    }
}

fn relationship(id: u64, source: u64, target: u64, value: Value) -> WalOp {
    WalOp::CreateRelationship {
        id: RelId(id),
        source: NodeId(source),
        target: NodeId(target),
        rel_type: "LINK".into(),
        properties: BTreeMap::from([("value".into(), value)]),
    }
}

fn set_node(id: u64, value: Value) -> WalOp {
    WalOp::SetNodeProperty {
        id: NodeId(id),
        property: "value".into(),
        value,
    }
}

fn set_relationship(id: u64, value: Value) -> WalOp {
    WalOp::SetRelationshipProperty {
        id: RelId(id),
        property: "value".into(),
        value,
    }
}

fn identity(op: &WalOp) -> Option<(bool, u64)> {
    match op {
        WalOp::CreateNode { id, .. }
        | WalOp::SetNodeProperty { id, .. }
        | WalOp::DeleteNode { id } => Some((false, id.0)),
        WalOp::CreateRelationship { id, .. }
        | WalOp::SetRelationshipProperty { id, .. }
        | WalOp::DeleteRelationship { id } => Some((true, id.0)),
        _ => None,
    }
}

fn is_create(op: &WalOp) -> bool {
    matches!(
        op,
        WalOp::CreateNode { .. } | WalOp::CreateRelationship { .. }
    )
}

fn is_delete(op: &WalOp) -> bool {
    matches!(
        op,
        WalOp::DeleteNode { .. } | WalOp::DeleteRelationship { .. }
    )
}

fn reference_compaction(ops: &[WalOp]) -> Vec<WalOp> {
    // Independent lifetime scans: no mutable ID-to-output-slot map or tombstones.
    let mut output = Vec::new();
    for (index, op) in ops.iter().enumerate() {
        let mut retained = op.clone();
        if is_create(op) {
            let properties = match &mut retained {
                WalOp::CreateNode { properties, .. }
                | WalOp::CreateRelationship { properties, .. } => properties,
                _ => unreachable!(),
            };
            let mut deleted = false;
            for future in &ops[index + 1..] {
                if identity(future) != identity(op) {
                    continue;
                }
                if is_create(future) {
                    break;
                }
                if is_delete(future) {
                    deleted = true;
                    break;
                }
                if let WalOp::SetNodeProperty {
                    property, value, ..
                }
                | WalOp::SetRelationshipProperty {
                    property, value, ..
                } = future
                {
                    properties.insert(property.clone(), value.clone());
                }
            }
            if deleted {
                continue;
            }
        } else if identity(op).is_some() {
            let previous_boundary = ops[..index].iter().rev().find(|prior| {
                identity(prior) == identity(op) && (is_create(prior) || is_delete(prior))
            });
            if previous_boundary.is_some_and(is_create) {
                continue;
            }
        }
        output.push(retained);
    }
    output
}

fn assert_ops(actual: &[WalOp], expected: &[WalOp], context: &str) {
    // WalOp deliberately has no PartialEq. Compare every variant/field and order
    // without involving the production WAL encoder as the compaction oracle.
    assert_eq!(format!("{actual:?}"), format!("{expected:?}"), "{context}");
}

#[test]
fn folds_only_same_transaction_creates_and_keeps_last_assignment() {
    let ops = vec![
        set_node(9, Value::Int(10)),
        node(1, Value::Int(1)),
        relationship(1, 1, 9, Value::Int(2)),
        set_node(1, Value::Null),
        set_relationship(1, Value::Int(3)),
        set_node(1, Value::String("last".into())),
        set_relationship(9, Value::Int(4)),
    ];
    assert_ops(
        &compact_transaction_graph_ops(ops),
        &[
            set_node(9, Value::Int(10)),
            node(1, Value::String("last".into())),
            relationship(1, 1, 9, Value::Int(3)),
            set_relationship(9, Value::Int(4)),
        ],
        "folded values and independent node/relationship IDs",
    );
}

#[test]
fn deletion_ends_a_lifetime_without_consuming_later_writes() {
    let ops = vec![
        node(u64::MAX, Value::Int(1)),
        set_node(u64::MAX, Value::Int(2)),
        WalOp::DeleteNode {
            id: NodeId(u64::MAX),
        },
        set_node(u64::MAX, Value::Int(3)),
        node(u64::MAX, Value::Int(4)),
        set_node(u64::MAX, Value::Int(5)),
        WalOp::DeleteRelationship {
            id: RelId(u64::MAX),
        },
    ];
    assert_ops(
        &compact_transaction_graph_ops(ops),
        &[
            set_node(u64::MAX, Value::Int(3)),
            node(u64::MAX, Value::Int(5)),
            WalOp::DeleteRelationship {
                id: RelId(u64::MAX),
            },
        ],
        "a deleted create must not swallow later standalone operations",
    );
}

#[test]
fn leaves_opaque_operations_and_nested_batches_in_place() {
    for opaque in opaque_ops() {
        let ops = vec![
            node(1, Value::Int(1)),
            opaque.clone(),
            set_node(1, Value::Int(2)),
        ];
        assert_ops(
            &compact_transaction_graph_ops(ops),
            &[node(1, Value::Int(2)), opaque],
            "opaque operation order",
        );
    }
}

#[test]
fn compaction_preserves_float_bits_and_nested_values() {
    for bits in [
        0_u64,
        1_u64 << 63,
        0x7ff8_0000_0000_0042,
        0xfff0_0000_0000_0000,
    ] {
        let value = Value::List(vec![Value::Float(f64::from_bits(bits))]);
        let ops = compact_transaction_graph_ops(vec![node(0, Value::Null), set_node(0, value)]);
        let WalOp::CreateNode { properties, .. } = &ops[0] else {
            panic!("create remains")
        };
        let Value::List(items) = &properties["value"] else {
            panic!("list remains")
        };
        let Value::Float(actual) = items[0] else {
            panic!("float remains")
        };
        assert_eq!(actual.to_bits(), bits);
    }
}

fn opaque_ops() -> Vec<WalOp> {
    let record: Arc<[u8]> = Arc::from([0, 1, 255]);
    vec![
        WalOp::CreateNodeLabel {
            label: "Memory".into(),
        },
        WalOp::CreateRelationshipType {
            rel_type: "LINK".into(),
        },
        WalOp::CreateNodeTable {
            name: "Memory".into(),
        },
        WalOp::CreateRelationshipTable {
            name: "LINK".into(),
        },
        WalOp::CreateProperty {
            table_kind: TableKind::Node,
            table: "Memory".into(),
            property: "value".into(),
            value_type: PropertyType::String,
            nullable: true,
        },
        WalOp::AlterTableState {
            table_kind: TableKind::Node,
            table: "Memory".into(),
            state: SchemaObjectState::Public,
        },
        WalOp::AlterPropertyState {
            table_kind: TableKind::Node,
            table: "Memory".into(),
            property: "value".into(),
            state: SchemaObjectState::Public,
        },
        WalOp::GcTableDescriptor {
            table_kind: TableKind::Node,
            table: "Memory".into(),
        },
        WalOp::GcPropertyDescriptor {
            table_kind: TableKind::Node,
            table: "Memory".into(),
            property: "value".into(),
        },
        WalOp::CreateIndex {
            label: "Memory".into(),
            property: "value".into(),
        },
        WalOp::CreateCompositeIndex {
            label: "Memory".into(),
            properties: vec!["a".into(), "b".into()],
        },
        WalOp::CreateRangeIndex {
            label: "Memory".into(),
            property: "value".into(),
        },
        WalOp::CreateFullTextIndex {
            label: "Memory".into(),
            property: "value".into(),
        },
        WalOp::CreateUniqueConstraint {
            label: "Memory".into(),
            property: "value".into(),
        },
        WalOp::CreateNodePropertyExistsConstraint {
            label: "Memory".into(),
            property: "value".into(),
        },
        WalOp::CreateRelationshipUniqueConstraint {
            rel_type: "LINK".into(),
            property: "value".into(),
        },
        WalOp::CreateRelationshipPropertyExistsConstraint {
            rel_type: "LINK".into(),
            property: "value".into(),
        },
        WalOp::ProjectGraph {
            name: "projection".into(),
            node_labels: vec!["Memory".into()],
            rel_types: vec!["LINK".into()],
        },
        WalOp::MarkInitialImportSource {
            source_fingerprint: "source-v1".into(),
        },
        WalOp::Relational {
            record: record.clone(),
        },
        WalOp::RelationalSnapshot {
            record: record.clone(),
        },
        WalOp::Append { record },
        WalOp::Batch(vec![
            node(1, Value::Int(100)),
            WalOp::Batch(vec![set_node(1, Value::Int(200))]),
        ]),
    ]
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Graph {
    nodes: BTreeMap<NodeId, (String, Properties)>,
    relationships: BTreeMap<RelId, (NodeId, NodeId, String, Properties)>,
}

impl Graph {
    fn initial() -> Self {
        let mut graph = Self::default();
        for id in 1..=3 {
            graph.apply(&node(id, Value::Int(id as i64)));
        }
        graph.apply(&relationship(1, 1, 2, Value::Int(1)));
        graph
    }

    fn apply(&mut self, op: &WalOp) {
        match op {
            WalOp::CreateNode {
                id,
                label,
                properties,
            } => {
                assert!(self
                    .nodes
                    .insert(*id, (label.clone(), properties.clone()))
                    .is_none());
            }
            WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => {
                assert!(self.nodes.contains_key(source) && self.nodes.contains_key(target));
                assert!(self
                    .relationships
                    .insert(
                        *id,
                        (*source, *target, rel_type.clone(), properties.clone())
                    )
                    .is_none());
            }
            WalOp::SetNodeProperty {
                id,
                property,
                value,
            } => {
                self.nodes
                    .get_mut(id)
                    .unwrap()
                    .1
                    .insert(property.clone(), value.clone());
            }
            WalOp::SetRelationshipProperty {
                id,
                property,
                value,
            } => {
                self.relationships
                    .get_mut(id)
                    .unwrap()
                    .3
                    .insert(property.clone(), value.clone());
            }
            WalOp::DeleteNode { id } => {
                assert!(!self
                    .relationships
                    .values()
                    .any(|(s, t, ..)| s == id || t == id));
                assert!(self.nodes.remove(id).is_some());
            }
            WalOp::DeleteRelationship { id } => {
                assert!(self.relationships.remove(id).is_some());
            }
            _ => panic!("graph replay corpus contains only admitted graph journal operations"),
        }
    }
}

struct Generator(u64);

impl Generator {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 ^ (self.0 >> 29)
    }

    fn value(&mut self) -> Value {
        match self.next() % 8 {
            0 => Value::Null,
            1 => Value::Bool(self.next() & 1 != 0),
            2 => Value::Int(i64::MIN),
            3 => Value::Int(i64::MAX),
            4 => Value::Float(-0.0),
            5 => Value::String(format!("text-\u{65e5}\u{672c}-\t\n{}", self.next() % 17)),
            6 => Value::List(vec![Value::Int(self.next() as i64), Value::Null]),
            _ => Value::Map(BTreeMap::from([(
                "nested".into(),
                Value::String("value".into()),
            )])),
        }
    }

    fn node_id(&mut self) -> u64 {
        [0, 1, 2, u64::MAX][self.index(4)]
    }

    fn index(&mut self, length: usize) -> usize {
        (self.next() % length as u64) as usize
    }
}

fn arbitrary_journal(seed: u64) -> Vec<WalOp> {
    let mut rng = Generator(seed);
    let opaque = opaque_ops();
    (0..48)
        .map(|_| {
            let id = rng.node_id();
            match rng.next() % 7 {
                0 => node(id, rng.value()),
                1 => relationship(id, rng.node_id(), rng.node_id(), rng.value()),
                2 => set_node(id, rng.value()),
                3 => set_relationship(id, rng.value()),
                4 => WalOp::DeleteNode { id: NodeId(id) },
                5 => WalOp::DeleteRelationship { id: RelId(id) },
                _ => opaque[rng.index(opaque.len())].clone(),
            }
        })
        .collect()
}

fn admitted_journal(seed: u64) -> Vec<WalOp> {
    let mut rng = Generator(seed);
    let mut graph = Graph::initial();
    let mut ops = Vec::new();
    let mut next_node = 4;
    let mut next_rel = 2;
    for _ in 0..64 {
        let op = match rng.next() % 6 {
            0 => {
                let op = node(next_node, rng.value());
                next_node += 1;
                op
            }
            1 if !graph.nodes.is_empty() => {
                let source = graph
                    .nodes
                    .keys()
                    .nth(rng.index(graph.nodes.len()))
                    .unwrap()
                    .0;
                let target = graph
                    .nodes
                    .keys()
                    .nth(rng.index(graph.nodes.len()))
                    .unwrap()
                    .0;
                let op = relationship(next_rel, source, target, rng.value());
                next_rel += 1;
                op
            }
            2 if !graph.nodes.is_empty() => {
                let id = graph
                    .nodes
                    .keys()
                    .nth(rng.index(graph.nodes.len()))
                    .unwrap()
                    .0;
                set_node(id, rng.value())
            }
            3 if !graph.relationships.is_empty() => {
                let id = graph
                    .relationships
                    .keys()
                    .nth(rng.index(graph.relationships.len()))
                    .unwrap()
                    .0;
                set_relationship(id, rng.value())
            }
            4 if !graph.nodes.is_empty() => {
                let id = *graph
                    .nodes
                    .keys()
                    .nth(rng.index(graph.nodes.len()))
                    .unwrap();
                // A successful DETACH DELETE emits relationship deletes before its node delete.
                let deletes: Vec<_> = graph
                    .relationships
                    .iter()
                    .filter(|(_, (s, t, ..))| *s == id || *t == id)
                    .map(|(id, _)| WalOp::DeleteRelationship { id: *id })
                    .collect();
                for delete in deletes {
                    graph.apply(&delete);
                    ops.push(delete);
                }
                WalOp::DeleteNode { id }
            }
            5 if !graph.relationships.is_empty() => {
                let id = *graph
                    .relationships
                    .keys()
                    .nth(rng.index(graph.relationships.len()))
                    .unwrap();
                WalOp::DeleteRelationship { id }
            }
            _ => {
                let op = node(next_node, rng.value());
                next_node += 1;
                op
            }
        };
        graph.apply(&op);
        ops.push(op);
    }
    ops
}

fn verify_sequence(ops: &[WalOp], seed: u64, prefix: usize, admitted: bool) {
    let expected = reference_compaction(ops);
    let actual = compact_transaction_graph_ops(ops.to_vec());
    let context = format!("seed={seed} prefix={prefix} admitted={admitted}");
    assert_ops(&actual, &expected, &context);
    assert!(actual.len() <= ops.len(), "{context}");
    if !admitted {
        return;
    }
    assert_ops(
        &compact_transaction_graph_ops(actual.clone()),
        &actual,
        &context,
    );
    // State equivalence covers visible records, not allocation or epoch policy.
    let mut before = Graph::initial();
    let mut after = Graph::initial();
    for op in ops {
        before.apply(op);
    }
    for op in &actual {
        after.apply(op);
    }
    assert_eq!(before, after, "{context}");
    let bytes = encode_binary_wal_record(
        &WalEntry {
            lsn: 17,
            op: WalOp::Batch(actual),
        },
        23,
    )
    .unwrap();
    let BinaryWalRecordDecode::Entry {
        entry,
        commit_epoch,
    } = decode_binary_wal_record(&bytes).unwrap()
    else {
        panic!("admitted compacted journal must decode: {context}");
    };
    assert_eq!(entry.lsn, 17);
    assert_eq!(commit_epoch, 23);
    let WalOp::Batch(decoded) = entry.op else {
        panic!("batch envelope remains")
    };
    assert_ops(&decoded, &expected, &context);
    let mut recovered = Graph::initial();
    for op in &decoded {
        recovered.apply(op);
    }
    assert_eq!(before, recovered, "{context}");
}

fn campaign(seeds: u64) {
    let mut arbitrary_prefixes = 0;
    let mut admitted_prefixes = 0;
    let mut admitted_kinds = [0_u64; 6];
    for seed in 0..seeds {
        let arbitrary = arbitrary_journal(seed);
        for prefix in 0..=arbitrary.len() {
            verify_sequence(&arbitrary[..prefix], seed, prefix, false);
            arbitrary_prefixes += 1;
        }
        let admitted = admitted_journal(seed);
        for op in &admitted {
            let kind = match op {
                WalOp::CreateNode { .. } => 0,
                WalOp::CreateRelationship { .. } => 1,
                WalOp::SetNodeProperty { .. } => 2,
                WalOp::SetRelationshipProperty { .. } => 3,
                WalOp::DeleteNode { .. } => 4,
                WalOp::DeleteRelationship { .. } => 5,
                _ => unreachable!(),
            };
            admitted_kinds[kind] += 1;
        }
        for prefix in 0..=admitted.len() {
            verify_sequence(&admitted[..prefix], seed, prefix, true);
            admitted_prefixes += 1;
        }
    }
    assert!(admitted_kinds.iter().all(|count| *count > 0));
    eprintln!("transaction-compaction-differential-v1 seeds={seeds} arbitrary_prefixes={arbitrary_prefixes} admitted_prefixes={admitted_prefixes} admitted_kinds={admitted_kinds:?}");
}

#[test]
fn transaction_compaction_differential_smoke() {
    campaign(8);
}

#[test]
#[ignore = "bounded local transaction journal compaction differential campaign"]
fn transaction_compaction_differential_campaign() {
    campaign(256);
}
