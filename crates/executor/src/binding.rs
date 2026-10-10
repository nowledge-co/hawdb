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

use hawdb_core::{LabelId, Value};
use hawdb_plan_cypher::SortDirection;
use hawdb_storage::{NodeRecord, RelRecord};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub values: BTreeMap<String, Value>,
    pub nodes: BTreeMap<String, NodeRecord>,
    pub relationships: BTreeMap<String, RelRecord>,
}

impl Binding {
    pub fn values(values: BTreeMap<String, Value>) -> Self {
        Self {
            values,
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
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
        binding_memory_bytes(&self.binding).saturating_add(self.sort_values.iter().fold(
            std::mem::size_of::<Vec<(Value, SortDirection)>>(),
            |total, (value, _)| total.saturating_add(value_memory_bytes(value)),
        ))
    }
}

impl Ord for TopNBinding {
    fn cmp(&self, other: &Self) -> Ordering {
        for ((left, direction), (right, other_direction)) in
            self.sort_values.iter().zip(&other.sort_values)
        {
            debug_assert_eq!(direction, other_direction);
            let ordering = match direction {
                SortDirection::Asc => left.cmp(right),
                SortDirection::Desc => left.cmp(right).reverse(),
            };
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
        .saturating_add(std::mem::size_of::<usize>() * 6)
}

pub fn value_memory_bytes(value: &Value) -> usize {
    std::mem::size_of::<Value>().saturating_add(match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => 0,
        Value::String(value) => value.len(),
        Value::Binary(value) => value.len(),
        Value::Uuid(_) => 16,
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
    let entries = std::cell::Cell::new(0usize);
    let graph_bytes = graph_binding_parts_bytes(
        nodes.inspect(|_| entries.set(entries.get().saturating_add(1))),
        relationships.inspect(|_| entries.set(entries.get().saturating_add(1))),
        map_memory_bytes,
    );
    std::mem::size_of::<Binding>()
        .saturating_add(graph_bytes)
        .saturating_add(
            entries
                .get()
                .saturating_mul(std::mem::size_of::<usize>() * 6),
        )
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
    match value {
        Value::Null => 0,
        Value::Bool(_) => std::mem::size_of::<bool>(),
        Value::Int(_) => std::mem::size_of::<i64>(),
        Value::Float(_) => std::mem::size_of::<f64>(),
        Value::String(value) => value.len(),
        Value::Binary(value) => value.len(),
        Value::Uuid(_) => 16,
        Value::List(values) => values.iter().fold(0usize, |total, value| {
            total.saturating_add(value_payload_bytes(value))
        }),
        Value::Map(values) => map_payload_bytes(values),
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
            )]),
            relationships: BTreeMap::from([(
                "r".to_string(),
                RelRecord {
                    id: RelId(11),
                    source: NodeId(7),
                    target: NodeId(8),
                    rel_type: RelTypeId(4),
                    properties: BTreeMap::new(),
                },
            )]),
        };

        assert!(binding_payload_bytes(&binding) > "nested".len() + "two".len());
        assert!(binding_memory_bytes(&binding) > binding_payload_bytes(&binding));
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
}
