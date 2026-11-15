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

//! Executor-owned row bindings and deterministic memory accounting.

mod graph_slots;
pub use graph_slots::GraphSlots;

use hawdb_core::{LabelId, Value};
use hawdb_plan_cypher::SortDirection;
use hawdb_storage::{NodeRecord, RelRecord};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub values: BTreeMap<String, Value>,
    pub nodes: GraphSlots<NodeRecord>,
    pub relationships: GraphSlots<RelRecord>,
}

impl Binding {
    pub fn values(values: BTreeMap<String, Value>) -> Self {
        Self {
            values,
            nodes: GraphSlots::new(),
            relationships: GraphSlots::new(),
        }
    }

    pub fn scalar(name: impl Into<String>, value: Value) -> Self {
        Self::values(BTreeMap::from([(name.into(), value)]))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopNBinding {
    pub sort_values: Vec<(Value, SortDirection)>,
    pub ordinal: u64,
    pub binding: Binding,
}

impl TopNBinding {
    pub fn memory_bytes(&self) -> usize {
        binding_memory_bytes(&self.binding)
            .saturating_add(sort_values_memory_bytes(&self.sort_values))
    }
}

pub(crate) fn sort_values_memory_bytes(values: &Vec<(Value, SortDirection)>) -> usize {
    let layout = std::mem::size_of::<Vec<(Value, SortDirection)>>().saturating_add(
        values
            .capacity()
            .saturating_mul(std::mem::size_of::<(Value, SortDirection)>()),
    );
    values.iter().fold(layout, |total, (value, _)| {
        total.saturating_add(value_memory_bytes(value).saturating_sub(std::mem::size_of::<Value>()))
    })
}

impl Ord for TopNBinding {
    fn cmp(&self, other: &Self) -> Ordering {
        for ((left, direction), (right, other_direction)) in
            self.sort_values.iter().zip(&other.sort_values)
        {
            debug_assert_eq!(direction, other_direction);
            // openCypher NULL placement; see `SortDirection::compare`.
            let ordering = direction.compare(left, right);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        self.ordinal.cmp(&other.ordinal)
    }
}

impl PartialOrd for TopNBinding {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn map_payload_bytes(values: &BTreeMap<String, Value>) -> usize {
    values.iter().fold(0usize, |total, (name, value)| {
        total
            .saturating_add(name.len())
            .saturating_add(value_payload_bytes(value))
    })
}

pub fn map_memory_bytes(values: &BTreeMap<String, Value>) -> usize {
    map_memory_bytes_with_entries(values.iter().map(|(name, value)| (name.as_str(), value)))
}

fn map_memory_bytes_with_entries<'a>(values: impl Iterator<Item = (&'a str, &'a Value)>) -> usize {
    std::mem::size_of::<BTreeMap<String, Value>>().saturating_add(values.fold(
        0usize,
        |total, (name, value)| {
            total
                .saturating_add(std::mem::size_of::<(String, Value)>() * 3)
                .saturating_add(name.len())
                .saturating_add(value_memory_bytes(value))
        },
    ))
}

/// Estimate the selected owned scan input with the same rules as its final
/// node-only Binding, while all property payloads still belong to storage.
pub(crate) fn projected_node_binding_memory_bytes(
    variable: &str,
    node: &NodeRecord,
    required_properties: &BTreeSet<String>,
) -> usize {
    std::mem::size_of::<Binding>()
        .saturating_add(variable.len())
        .saturating_add(std::mem::size_of_val(&node.id))
        .saturating_add(
            node.labels
                .len()
                .saturating_mul(std::mem::size_of::<LabelId>()),
        )
        .saturating_add(map_memory_bytes_with_entries(
            required_properties.iter().filter_map(|property| {
                node.properties
                    .get(property)
                    .map(|value| (property.as_str(), value))
            }),
        ))
        .saturating_add(graph_slot_memory_bytes(1))
}

pub fn value_memory_bytes(value: &Value) -> usize {
    std::mem::size_of::<Value>().saturating_add(match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::Decimal(_) => 0,
        Value::String(value) => value.len(),
        Value::Binary(value) => value.len(),
        Value::Vector(value) => value.encoded().len(),
        Value::Uuid(_) => 16,
        // Temporal values are stored inline in the `Value` slot, except
        // durations, which are boxed to keep `Value` at 32 bytes.
        Value::Date(_) | Value::LocalTime(_) | Value::LocalDateTime(_) | Value::DateTime(_) => 0,
        Value::Duration(_) => std::mem::size_of::<hawdb_core::temporal::Duration>(),
        Value::List(values) => values
            .iter()
            .fold(std::mem::size_of::<Vec<Value>>(), |total, value| {
                total.saturating_add(value_memory_bytes(value))
            }),
        Value::Map(values) => map_memory_bytes(values),
    })
}

pub fn binding_payload_bytes(binding: &Binding) -> usize {
    map_payload_bytes(&binding.values).saturating_add(graph_binding_payload_bytes(binding))
}

fn graph_binding_payload_bytes(binding: &Binding) -> usize {
    graph_binding_bytes(binding, map_payload_bytes)
}

fn graph_binding_bytes(
    binding: &Binding,
    property_bytes: fn(&BTreeMap<String, Value>) -> usize,
) -> usize {
    graph_binding_parts_bytes(
        binding
            .nodes
            .iter()
            .map(|(name, node)| (name.as_str(), node)),
        binding
            .relationships
            .iter()
            .map(|(name, row)| (name.as_str(), row)),
        property_bytes,
    )
}

fn graph_binding_parts_bytes<'a>(
    nodes: impl Iterator<Item = (&'a str, &'a NodeRecord)>,
    relationships: impl Iterator<Item = (&'a str, &'a RelRecord)>,
    property_bytes: fn(&BTreeMap<String, Value>) -> usize,
) -> usize {
    nodes
        .fold(0usize, |total, (name, node)| {
            total
                .saturating_add(name.len())
                .saturating_add(std::mem::size_of_val(&node.id))
                .saturating_add(
                    node.labels
                        .len()
                        .saturating_mul(std::mem::size_of::<LabelId>()),
                )
                .saturating_add(property_bytes(&node.properties))
        })
        .saturating_add(relationships.fold(0usize, |total, (name, row)| {
            total
                .saturating_add(name.len())
                .saturating_add(std::mem::size_of_val(&row.id))
                .saturating_add(std::mem::size_of_val(&row.source))
                .saturating_add(std::mem::size_of_val(&row.target))
                .saturating_add(std::mem::size_of_val(&row.rel_type))
                .saturating_add(property_bytes(&row.properties))
        }))
}

pub(crate) fn binding_payload_bytes_with_parts<'a>(
    values: impl Iterator<Item = (&'a str, &'a Value)>,
    nodes: impl Iterator<Item = (&'a str, &'a NodeRecord)>,
    relationships: impl Iterator<Item = (&'a str, &'a RelRecord)>,
) -> usize {
    values
        .fold(0usize, |total, (name, value)| {
            total
                .saturating_add(name.len())
                .saturating_add(value_payload_bytes(value))
        })
        .saturating_add(graph_binding_parts_bytes(
            nodes,
            relationships,
            map_payload_bytes,
        ))
}

pub fn binding_memory_bytes(binding: &Binding) -> usize {
    binding_memory_bytes_with_values(
        binding,
        binding
            .values
            .iter()
            .map(|(name, value)| (name.as_str(), value)),
    )
}

/// Estimate an overwritten column from the final borrowed map. Resident
/// container storage differs from transport payload and must be removed in full.
pub(crate) fn binding_memory_bytes_replacing_value(
    binding: &Binding,
    name: &str,
    value: &Value,
) -> usize {
    binding_memory_bytes_with_values(
        binding,
        binding
            .values
            .iter()
            .filter(|(key, _)| key.as_str() != name)
            .map(|(key, value)| (key.as_str(), value))
            .chain(std::iter::once((name, value))),
    )
}

/// Account a projection's borrowed values with the same retained graph and
/// entry rules as its final owned Binding, before copying any of those values.
pub(crate) fn binding_memory_bytes_with_values<'a>(
    binding: &'a Binding,
    values: impl Iterator<Item = (&'a str, &'a Value)>,
) -> usize {
    binding_memory_bytes_with_parts(
        values,
        binding
            .nodes
            .iter()
            .map(|(name, node)| (name.as_str(), node)),
        binding
            .relationships
            .iter()
            .map(|(name, row)| (name.as_str(), row)),
    )
}

pub(crate) fn binding_memory_bytes_with_parts<'a>(
    values: impl Iterator<Item = (&'a str, &'a Value)>,
    nodes: impl Iterator<Item = (&'a str, &'a NodeRecord)>,
    relationships: impl Iterator<Item = (&'a str, &'a RelRecord)>,
) -> usize {
    let nodes_count = std::cell::Cell::new(0usize);
    let relationships_count = std::cell::Cell::new(0usize);
    let graph_bytes = graph_binding_parts_bytes(
        nodes.inspect(|_| nodes_count.set(nodes_count.get().saturating_add(1))),
        relationships
            .inspect(|_| relationships_count.set(relationships_count.get().saturating_add(1))),
        map_memory_bytes,
    );
    std::mem::size_of::<Binding>()
        .saturating_add(graph_bytes)
        // Charge every row as if its graph records were uniquely retained.
        // Sharing reduces allocator traffic but never weakens admission. Include
        // slot vectors, Arc headers, String/label layouts and record headers.
        .saturating_add(graph_slot_memory_bytes(nodes_count.get()))
        .saturating_add(graph_slot_memory_bytes(relationships_count.get()))
        .saturating_add(values.fold(0usize, |total, (name, value)| {
            total
                .saturating_add(name.len())
                .saturating_add(match value {
                    Value::List(_) | Value::Map(_) => {
                        value_memory_bytes(value).saturating_sub(std::mem::size_of::<Value>())
                    }
                    _ => value_payload_bytes(value),
                })
                .saturating_add(std::mem::size_of::<usize>() * 6)
        }))
}

pub(crate) fn graph_slot_memory_bytes(entries: usize) -> usize {
    if entries == 0 {
        return 0;
    }
    // A Vec grown one slot at a time has capacity at most max(4, 2*len).
    std::mem::size_of::<Vec<usize>>()
        .saturating_add(2 * std::mem::size_of::<usize>())
        .saturating_add(
            entries
                .saturating_mul(2)
                .max(4)
                .saturating_mul(2 * std::mem::size_of::<usize>()),
        )
        .saturating_add(entries.saturating_mul(10 * std::mem::size_of::<usize>()))
}

pub fn node_memory_bytes(node: &NodeRecord) -> usize {
    std::mem::size_of::<NodeRecord>()
        .saturating_add(
            node.labels
                .len()
                .saturating_mul(std::mem::size_of::<LabelId>() * 3),
        )
        .saturating_add(map_memory_bytes(&node.properties))
}

pub fn relationship_memory_bytes(relationship: &RelRecord) -> usize {
    std::mem::size_of::<RelRecord>().saturating_add(map_memory_bytes(&relationship.properties))
}

pub fn value_payload_bytes(value: &Value) -> usize {
    // Match the owned value directly: the same table as
    // `value_ref_payload_bytes`, without building a `ValueRef` per value (#1615).
    match value {
        Value::Null => 0,
        Value::Bool(_) => std::mem::size_of::<bool>(),
        Value::Int(_) => std::mem::size_of::<i64>(),
        Value::Float(_) => std::mem::size_of::<f64>(),
        Value::Decimal(_) => 17,
        Value::String(value) => value.len(),
        Value::Binary(value) => value.len(),
        Value::Vector(value) => value.encoded().len(),
        Value::Uuid(_) => 16,
        Value::Date(_) => 4,
        Value::LocalTime(_) => 8,
        Value::LocalDateTime(_) => 12,
        Value::DateTime(_) => 16,
        Value::Duration(_) => 28,
        Value::List(values) => values.iter().fold(0usize, |total, value| {
            total.saturating_add(value_payload_bytes(value))
        }),
        Value::Map(values) => map_payload_bytes(values),
    }
}

/// A row's resident bytes (`binding_memory_bytes`, the ledger charge) and
/// transport bytes (`binding_payload_bytes`, the observer statistic).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BindingSizes {
    pub(crate) memory: usize,
    pub(crate) payload: usize,
}

/// Both sizes of one row from a single walk of its values and graph records
/// (#1615). Every term and saturating sum matches the separate walks, so the
/// results are identical to `binding_memory_bytes` and `binding_payload_bytes`.
pub(crate) fn binding_sizes(binding: &Binding) -> BindingSizes {
    let mut graph_memory = 0usize;
    let mut graph_payload = 0usize;
    let mut nodes = 0usize;
    for (name, node) in binding.nodes.iter() {
        nodes = nodes.saturating_add(1);
        let fixed = |total: usize| {
            total
                .saturating_add(name.len())
                .saturating_add(std::mem::size_of_val(&node.id))
                .saturating_add(
                    node.labels
                        .len()
                        .saturating_mul(std::mem::size_of::<LabelId>()),
                )
        };
        let (memory, payload) = map_sizes(&node.properties);
        graph_memory = fixed(graph_memory).saturating_add(memory);
        graph_payload = fixed(graph_payload).saturating_add(payload);
    }
    let mut relationships = 0usize;
    for (name, row) in binding.relationships.iter() {
        relationships = relationships.saturating_add(1);
        let fixed = |total: usize| {
            total
                .saturating_add(name.len())
                .saturating_add(std::mem::size_of_val(&row.id))
                .saturating_add(std::mem::size_of_val(&row.source))
                .saturating_add(std::mem::size_of_val(&row.target))
                .saturating_add(std::mem::size_of_val(&row.rel_type))
        };
        let (memory, payload) = map_sizes(&row.properties);
        graph_memory = fixed(graph_memory).saturating_add(memory);
        graph_payload = fixed(graph_payload).saturating_add(payload);
    }
    let mut values_memory = 0usize;
    let mut values_payload = 0usize;
    for (name, value) in &binding.values {
        let (resident, payload) = match value {
            Value::List(_) | Value::Map(_) => {
                let (memory, payload) = value_sizes(value);
                (memory.saturating_sub(std::mem::size_of::<Value>()), payload)
            }
            _ => {
                let payload = value_payload_bytes(value);
                (payload, payload)
            }
        };
        values_memory = values_memory
            .saturating_add(name.len())
            .saturating_add(resident)
            .saturating_add(std::mem::size_of::<usize>() * 6);
        values_payload = values_payload
            .saturating_add(name.len())
            .saturating_add(payload);
    }
    BindingSizes {
        memory: std::mem::size_of::<Binding>()
            .saturating_add(graph_memory)
            .saturating_add(graph_slot_memory_bytes(nodes))
            .saturating_add(graph_slot_memory_bytes(relationships))
            .saturating_add(values_memory),
        payload: values_payload.saturating_add(graph_payload),
    }
}

/// `(value_memory_bytes, value_payload_bytes)` from one walk.
fn value_sizes(value: &Value) -> (usize, usize) {
    match value {
        Value::List(values) => {
            let (memory, payload) = values.iter().fold(
                (std::mem::size_of::<Vec<Value>>(), 0usize),
                |(memory, payload), value| {
                    let (value_memory, value_payload) = value_sizes(value);
                    (
                        memory.saturating_add(value_memory),
                        payload.saturating_add(value_payload),
                    )
                },
            );
            (std::mem::size_of::<Value>().saturating_add(memory), payload)
        }
        Value::Map(values) => {
            let (memory, payload) = map_sizes(values);
            (std::mem::size_of::<Value>().saturating_add(memory), payload)
        }
        // Scalars do not recurse; each walk is a constant-time match.
        scalar => (value_memory_bytes(scalar), value_payload_bytes(scalar)),
    }
}

/// `(map_memory_bytes, map_payload_bytes)` from one walk.
fn map_sizes(values: &BTreeMap<String, Value>) -> (usize, usize) {
    values.iter().fold(
        (std::mem::size_of::<BTreeMap<String, Value>>(), 0usize),
        |(memory, payload), (name, value)| {
            let (value_memory, value_payload) = value_sizes(value);
            (
                memory
                    .saturating_add(std::mem::size_of::<(String, Value)>() * 3)
                    .saturating_add(name.len())
                    .saturating_add(value_memory),
                payload
                    .saturating_add(name.len())
                    .saturating_add(value_payload),
            )
        },
    )
}

pub fn value_ref_payload_bytes(value: hawdb_core::ValueRef<'_>) -> usize {
    use hawdb_core::ValueRef;
    match value {
        ValueRef::Null => 0,
        ValueRef::Bool(_) => std::mem::size_of::<bool>(),
        ValueRef::Int(_) => std::mem::size_of::<i64>(),
        ValueRef::Float(_) => std::mem::size_of::<f64>(),
        ValueRef::Decimal(_) => 17,
        ValueRef::String(value) => value.len(),
        ValueRef::Binary(value) => value.len(),
        ValueRef::Vector(value) => value.encoded().len(),
        ValueRef::Uuid(_) => 16,
        ValueRef::Date(_) => 4,
        ValueRef::LocalTime(_) => 8,
        ValueRef::LocalDateTime(_) => 12,
        ValueRef::DateTime(_) => 16,
        ValueRef::Duration(_) => 28,
        ValueRef::List(values) => values.iter().fold(0usize, |total, value| {
            total.saturating_add(value_payload_bytes(value))
        }),
        ValueRef::Map(values) => map_payload_bytes(values),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_core::RelTypeId;
    use hawdb_storage::{NodeId, RelId};
    use std::collections::BTreeSet;

    #[test]
    fn value_only_constructors_leave_graph_bindings_empty() {
        let values = Binding::values(BTreeMap::from([("left".to_string(), Value::Int(1))]));
        assert_eq!(values.values["left"], Value::Int(1));
        assert!(values.nodes.is_empty());
        assert!(values.relationships.is_empty());

        let scalar = Binding::scalar("right", Value::Bool(true));
        assert_eq!(scalar.values["right"], Value::Bool(true));
        assert!(scalar.nodes.is_empty());
        assert!(scalar.relationships.is_empty());
    }

    #[test]
    fn binding_accounting_includes_nested_values_and_graph_records() {
        let binding = Binding {
            values: BTreeMap::from([(
                "nested".to_string(),
                Value::List(vec![Value::Int(1), Value::String("two".to_string())]),
            )]),
            nodes: BTreeMap::from([(
                "n".to_string(),
                NodeRecord {
                    id: NodeId(7),
                    labels: BTreeSet::from([LabelId(3)]),
                    properties: BTreeMap::new(),
                },
            )])
            .into(),
            relationships: BTreeMap::from([(
                "r".to_string(),
                RelRecord {
                    id: RelId(11),
                    source: NodeId(7),
                    target: NodeId(8),
                    rel_type: RelTypeId(4),
                    properties: BTreeMap::new(),
                },
            )])
            .into(),
        };

        assert!(binding_payload_bytes(&binding) > "nested".len() + "two".len());
        assert!(binding_memory_bytes(&binding) > binding_payload_bytes(&binding));
    }

    #[test]
    fn projected_node_admission_matches_graph_slot_ownership() {
        let node = NodeRecord {
            id: NodeId(3),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::from([
                ("id".into(), Value::String("chosen".into())),
                ("body".into(), Value::String("x".repeat(65536))),
            ]),
        };
        let required = BTreeSet::from(["id".to_string()]);
        let bytes = projected_node_binding_memory_bytes("n", &node, &required);
        let mut projected = node.clone();
        projected
            .properties
            .retain(|name, _| required.contains(name));
        let binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([("n".into(), projected)]).into(),
            relationships: GraphSlots::new(),
        };
        assert_eq!(bytes, binding_memory_bytes(&binding));
    }

    #[test]
    fn replacement_admission_matches_final_rows_without_mutating_input() {
        let column = hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN;
        for previous in [
            None,
            Some(Value::Null),
            Some(Value::Float(-1.0)),
            Some(Value::List(vec![Value::Map(BTreeMap::from([(
                "nested".into(),
                Value::String("x".repeat(1_024)),
            )]))])),
        ] {
            let mut input = Binding::scalar("score", Value::Float(0.5));
            if let Some(previous) = previous {
                input.values.insert(column.into(), previous);
            }
            let original = input.clone();
            let estimate =
                binding_memory_bytes_replacing_value(&input, column, &Value::Float(0.25));
            assert_eq!(input, original, "admission must not alter the candidate");
            let mut output = input;
            output.values.insert(column.into(), Value::Float(0.25));
            assert_eq!(estimate, binding_memory_bytes(&output));
        }
    }

    /// #1615: rows are charged from previews of their final shape before the
    /// values are materialized. Across a sequence of row mutations every
    /// preview must equal a full walk of the resulting row, and charging the
    /// walked size through the ledger must settle to exactly zero.
    #[test]
    fn row_mutation_previews_equal_full_walks_and_settle_the_ledger() {
        use crate::memory_ledger::{QueryMemoryClass, QueryMemoryLedger};
        use std::num::NonZeroUsize;

        let budget = NonZeroUsize::new(1 << 20).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::PipelineBatch, "rows", budget);
        let mut row = Binding {
            values: BTreeMap::from([("id".to_string(), Value::Int(1))]),
            nodes: BTreeMap::from([(
                "n".to_string(),
                NodeRecord {
                    id: NodeId(1),
                    labels: BTreeSet::from([LabelId(2)]),
                    properties: BTreeMap::from([("name".into(), Value::String("ann".into()))]),
                },
            )])
            .into(),
            relationships: GraphSlots::new(),
        };
        let mut lease = account.reserve(binding_memory_bytes(&row)).unwrap();
        let mut list = Vec::new();
        let mut map = BTreeMap::new();
        for step in 0..24usize {
            // SET-like updates: overwrite, grow a list, grow a map, null out.
            let (name, value) = match step % 4 {
                0 => ("id", Value::String("x".repeat(step))),
                1 => {
                    list.push(Value::List(vec![Value::Int(step as i64); step % 3]));
                    ("list", Value::List(list.clone()))
                }
                2 => {
                    map.insert(format!("k{step}"), Value::String("v".repeat(step)));
                    ("map", Value::Map(map.clone()))
                }
                _ => ("id", Value::Null),
            };
            let preview = binding_memory_bytes_replacing_value(&row, name, &value);
            row.values.insert(name.to_string(), value);
            let walked = binding_memory_bytes(&row);
            assert_eq!(preview, walked, "step {step}: replacement");
            // Re-charge the row to its walked size; the ledger tracks it exactly.
            match walked.checked_sub(lease.bytes()) {
                Some(growth) => lease.grow(growth).unwrap(),
                None => lease.shrink(lease.bytes() - walked),
            }
            assert_eq!(ledger.snapshot().used_bytes, walked, "step {step}");

            // Projection: keep a subset of columns plus a computed column.
            let computed = Value::Map(BTreeMap::from([(
                "wrapped".into(),
                Value::List(list.clone()),
            )]));
            let projected: Vec<(&str, &Value)> = row
                .values
                .iter()
                .filter(|(key, _)| key.as_str() != "id")
                .map(|(key, value)| (key.as_str(), value))
                .chain(std::iter::once(("computed", &computed)))
                .collect();
            let preview = binding_memory_bytes_with_values(&row, projected.iter().copied());
            let mut materialized = row.clone();
            materialized.values = projected
                .iter()
                .map(|(key, value)| (key.to_string(), (*value).clone()))
                .collect();
            assert_eq!(
                preview,
                binding_memory_bytes(&materialized),
                "step {step}: projection"
            );
        }
        // Drops: remove columns and the graph slot, then release the remainder.
        for name in ["map", "list", "id"] {
            row.values.remove(name);
            lease.shrink(lease.bytes() - binding_memory_bytes(&row));
            assert_eq!(ledger.snapshot().used_bytes, binding_memory_bytes(&row));
        }
        row.nodes.remove("n");
        assert_eq!(binding_memory_bytes(&row), std::mem::size_of::<Binding>());
        drop(lease);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    /// Every kind of value, nested in lists and maps, and the node,
    /// relationship and path values the executor builds for graph entities.
    fn sized_values() -> Vec<Value> {
        use hawdb_core::temporal;
        let scalars = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(-7),
            Value::Float(2.5),
            Value::Decimal(hawdb_core::Decimal::new(12_345, 2).unwrap()),
            Value::String(String::new()),
            Value::String("text".repeat(33)),
            Value::Binary(vec![0xa5; 19]),
            Value::Vector(hawdb_core::Vector::new(&[1.0, -2.0, 3.5]).unwrap()),
            Value::Uuid(hawdb_core::Uuid::from_bytes([0x5a; 16])),
            Value::Date(temporal::parse_date("2026-11-09").unwrap()),
            Value::LocalTime(temporal::parse_local_time("23:59:59.999999999").unwrap()),
            Value::LocalDateTime(temporal::parse_local_datetime("2026-10-30T12:00").unwrap()),
            Value::DateTime(temporal::parse_datetime("2026-10-30T12:00:00-05:30").unwrap()),
            Value::duration(temporal::parse_duration("-P1Y2M3DT4.5S").unwrap()),
        ];
        let mut catalog = hawdb_core::Catalog::default();
        let label = catalog.get_or_create_label("Post");
        let rel_type = catalog.get_or_create_rel_type("HAS_CREATOR");
        let node = NodeRecord {
            id: NodeId(41),
            labels: BTreeSet::from([label]),
            properties: BTreeMap::from([
                ("content".into(), Value::String("x".repeat(300))),
                ("tags".into(), Value::List(scalars.clone())),
                ("empty".into(), Value::Null),
            ]),
        };
        let relationship = RelRecord {
            id: RelId(9),
            source: NodeId(41),
            target: NodeId(42),
            rel_type,
            properties: BTreeMap::from([("since".into(), Value::Int(2010))]),
        };
        let node_value = crate::expression::node_value(&node, &catalog);
        let relationship_value = crate::expression::relationship_value(&relationship, &catalog);
        let path = crate::expression::path_value(
            vec![node_value.clone(), node_value.clone()],
            vec![relationship_value.clone()],
        );
        let nested_map = Value::Map(
            scalars
                .iter()
                .enumerate()
                .map(|(index, value)| (format!("k{index}"), value.clone()))
                .collect(),
        );
        let deep = Value::List(vec![
            Value::List(vec![]),
            Value::Map(BTreeMap::new()),
            Value::List(vec![nested_map.clone(), Value::List(scalars.clone())]),
            Value::Map(BTreeMap::from([("inner".into(), path.clone())])),
        ]);
        scalars
            .into_iter()
            .chain([
                Value::List(Vec::with_capacity(8)),
                nested_map,
                node_value,
                relationship_value,
                path,
                deep,
            ])
            .collect()
    }

    #[test]
    fn single_walk_sizes_equal_separate_walks_across_value_kinds() {
        let values = sized_values();
        for value in &values {
            assert_eq!(
                value_payload_bytes(value),
                value_ref_payload_bytes(value.as_ref()),
                "{value:?}"
            );
            assert_eq!(
                value_sizes(value),
                (value_memory_bytes(value), value_payload_bytes(value)),
                "{value:?}"
            );
            if let Value::Map(map) = value {
                assert_eq!(
                    map_sizes(map),
                    (map_memory_bytes(map), map_payload_bytes(map))
                );
            }
        }
        let record = |id: u64, properties: BTreeMap<String, Value>| NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::from([LabelId(1), LabelId(2)]),
            properties,
        };
        let mut rows = vec![
            Binding::values(BTreeMap::new()),
            Binding::scalar("only", Value::Null),
        ];
        for (index, value) in values.iter().enumerate() {
            let mut properties = BTreeMap::from([("p".to_string(), value.clone())]);
            properties.insert("name".into(), Value::String(format!("row-{index}")));
            rows.push(Binding {
                values: BTreeMap::from([
                    (format!("v{index}"), value.clone()),
                    ("all".to_string(), Value::List(values.clone())),
                ]),
                nodes: BTreeMap::from([
                    ("a".to_string(), record(index as u64, properties.clone())),
                    ("b".to_string(), record(7, BTreeMap::new())),
                ])
                .into(),
                relationships: BTreeMap::from([(
                    "r".to_string(),
                    RelRecord {
                        id: RelId(index as u64),
                        source: NodeId(1),
                        target: NodeId(2),
                        rel_type: RelTypeId(3),
                        properties,
                    },
                )])
                .into(),
            });
        }
        for row in &rows {
            assert_eq!(
                binding_sizes(row),
                BindingSizes {
                    memory: binding_memory_bytes(row),
                    payload: binding_payload_bytes(row),
                },
                "{row:?}"
            );
        }
    }
}
