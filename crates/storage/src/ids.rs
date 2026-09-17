use skein_core::{LabelId, RelTypeId, Value};
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
