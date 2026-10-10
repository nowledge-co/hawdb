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

use crate::{LabelId, RelTypeId, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    pub id: NodeId,
    pub labels: BTreeSet<LabelId>,
    pub properties: BTreeMap<String, Value>,
}

/// A node identity with an explicitly selected property set.
///
/// This type is intentionally distinct from [`NodeRecord`]: an absent entry
/// means that the property was not requested, not that the canonical node is
/// missing the property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedNodeRecord {
    pub id: NodeId,
    pub labels: BTreeSet<LabelId>,
    pub properties: BTreeMap<String, Value>,
}

#[doc(hidden)]
pub fn project_node_record(
    node: NodeRecord,
    required_properties: &BTreeSet<String>,
) -> ProjectedNodeRecord {
    let properties = required_properties
        .iter()
        .filter_map(|property| {
            node.properties
                .get(property)
                .cloned()
                .map(|value| (property.clone(), value))
        })
        .collect();
    ProjectedNodeRecord {
        id: node.id,
        labels: node.labels,
        properties,
    }
}

/// Clone only selected columns from a borrowed node, without copying unrelated
/// payloads. Callers that own a query budget admit the estimate before cloning.
#[doc(hidden)]
pub fn project_node_record_ref(
    node: &NodeRecord,
    required_properties: &BTreeSet<String>,
) -> ProjectedNodeRecord {
    ProjectedNodeRecord {
        id: node.id,
        labels: node.labels.clone(),
        properties: required_properties
            .iter()
            .filter_map(|name| {
                node.properties
                    .get(name)
                    .map(|value| (name.clone(), value.clone()))
            })
            .collect(),
    }
}

/// Conservative allocation bound for the selected record, including container
/// overhead and recursively owned values, computed without cloning values.
#[doc(hidden)]
pub fn projected_node_allocation_bytes(
    node: &NodeRecord,
    required_properties: &BTreeSet<String>,
) -> usize {
    std::mem::size_of::<ProjectedNodeRecord>()
        .saturating_add(node.labels.len().saturating_mul(128))
        .saturating_add(required_properties.iter().fold(0usize, |total, name| {
            total.saturating_add(node.properties.get(name).map_or(0, |value| {
                1024usize
                    .saturating_add(name.len())
                    .saturating_add(projected_value_allocation_bytes(value))
            }))
        }))
}

fn projected_value_allocation_bytes(value: &Value) -> usize {
    std::mem::size_of::<Value>().saturating_add(match value {
        Value::String(value) => value.len(),
        Value::Binary(value) => value.len(),
        Value::List(values) => values.iter().fold(32usize, |total, value| {
            total.saturating_add(projected_value_allocation_bytes(value).saturating_mul(2))
        }),
        Value::Map(values) => values.iter().fold(32usize, |total, (key, value)| {
            total
                .saturating_add(1024)
                .saturating_add(key.len())
                .saturating_add(projected_value_allocation_bytes(value))
        }),
        _ => 0,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelRecord {
    pub id: RelId,
    pub source: NodeId,
    pub target: NodeId,
    pub rel_type: RelTypeId,
    pub properties: BTreeMap<String, Value>,
}
