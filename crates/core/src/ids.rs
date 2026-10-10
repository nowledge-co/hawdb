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
        .saturating_add(properties_allocation_bytes(
            required_properties.iter().filter_map(|name| {
                node.properties
                    .get(name)
                    .map(|value| (name.as_str(), value))
            }),
        ))
}

/// Full-record allocation bound, evaluated while all values remain borrowed.
#[doc(hidden)]
pub fn node_allocation_bytes(node: &NodeRecord) -> usize {
    std::mem::size_of::<NodeRecord>()
        .saturating_add(node.labels.len().saturating_mul(128))
        .saturating_add(properties_allocation_bytes(
            node.properties
                .iter()
                .map(|(name, value)| (name.as_str(), value)),
        ))
}

fn properties_allocation_bytes<'a>(
    properties: impl Iterator<Item = (&'a str, &'a Value)>,
) -> usize {
    let (count, payload) = properties.fold((0usize, 0usize), |(count, payload), (name, value)| {
        (
            count.saturating_add(1),
            payload
                .saturating_add(name.len())
                .saturating_add(projected_value_allocation_bytes(value)),
        )
    });
    if count == 0 {
        return 0;
    }
    payload.saturating_add(property_container_allocation_bytes(count))
}

/// Container and projection-collection bound shared with encoded preflight.
/// Values and owned key strings are charged separately, before decoding.
#[doc(hidden)]
pub fn property_container_allocation_bytes(count: usize) -> usize {
    if count == 0 {
        return 0;
    }
    // The pinned std B-tree stores up to eleven pairs per node and at least
    // five pairs per non-root node. Include internal links/header even for a
    // leaf, plus the collecting vector used by projected FromIterator reads.
    let pair_bytes = std::mem::size_of::<(String, Value)>();
    let node_bytes = pair_bytes
        .saturating_mul(11)
        .saturating_add(std::mem::size_of::<usize>().saturating_mul(12))
        .saturating_add(32);
    let nodes = if count <= 11 {
        1
    } else {
        1usize.saturating_add((count - 1) / 5)
    };
    nodes
        .saturating_mul(node_bytes)
        .saturating_add(count.saturating_mul(2).max(4).saturating_mul(pair_bytes))
}

fn projected_value_allocation_bytes(value: &Value) -> usize {
    std::mem::size_of::<Value>().saturating_add(match value {
        Value::String(value) => value.len(),
        Value::Binary(value) => value.len(),
        Value::List(values) => values.iter().fold(32usize, |total, value| {
            total.saturating_add(projected_value_allocation_bytes(value).saturating_mul(2))
        }),
        Value::Map(values) => {
            properties_allocation_bytes(values.iter().map(|(key, value)| (key.as_str(), value)))
        }
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

/// Full relationship ownership bound, measured without copying its properties.
#[doc(hidden)]
pub fn relationship_allocation_bytes(relationship: &RelRecord) -> usize {
    std::mem::size_of::<RelRecord>().saturating_add(properties_allocation_bytes(
        relationship
            .properties
            .iter()
            .map(|(name, value)| (name.as_str(), value)),
    ))
}
