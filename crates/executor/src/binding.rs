//! Executor-owned row bindings and deterministic memory accounting.

use skein_core::{LabelId, Value};
use skein_plan::SortDirection;
use skein_storage::{NodeRecord, RelRecord};
use std::cmp::Ordering;
use std::collections::BTreeMap;

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
    std::mem::size_of::<BTreeMap<String, Value>>().saturating_add(values.iter().fold(
        0usize,
        |total, (name, value)| {
            total
                .saturating_add(std::mem::size_of::<(String, Value)>() * 3)
                .saturating_add(name.len())
                .saturating_add(value_memory_bytes(value))
        },
    ))
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
    map_payload_bytes(&binding.values)
        .saturating_add(binding.nodes.iter().fold(0usize, |total, (name, node)| {
            total
                .saturating_add(name.len())
                .saturating_add(std::mem::size_of_val(&node.id))
                .saturating_add(
                    node.labels
                        .len()
                        .saturating_mul(std::mem::size_of::<LabelId>()),
                )
                .saturating_add(map_payload_bytes(&node.properties))
        }))
        .saturating_add(
            binding
                .relationships
                .iter()
                .fold(0usize, |total, (name, relationship)| {
                    total
                        .saturating_add(name.len())
                        .saturating_add(std::mem::size_of_val(&relationship.id))
                        .saturating_add(std::mem::size_of_val(&relationship.source))
                        .saturating_add(std::mem::size_of_val(&relationship.target))
                        .saturating_add(std::mem::size_of_val(&relationship.rel_type))
                        .saturating_add(map_payload_bytes(&relationship.properties))
                }),
        )
}

pub fn binding_memory_bytes(binding: &Binding) -> usize {
    std::mem::size_of::<Binding>()
        .saturating_add(binding_payload_bytes(binding))
        .saturating_add(
            binding
                .values
                .len()
                .saturating_add(binding.nodes.len())
                .saturating_add(binding.relationships.len())
                .saturating_mul(std::mem::size_of::<usize>() * 6),
        )
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
    use skein_core::RelTypeId;
    use skein_storage::{NodeId, RelId};
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
}
