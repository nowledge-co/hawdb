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

//! Index sample invalidation for graph node mutations.

use crate::NodeRecord;
use hawdb_core::{Catalog, CompositeIndexDescriptor, IndexId, IndexKind, LabelId, Value};
use std::collections::BTreeMap;

pub fn node_create_index_sample_updates(
    catalog: &Catalog,
    old_node: Option<&NodeRecord>,
    new_label_id: LabelId,
    new_properties: &BTreeMap<String, Value>,
) -> Vec<IndexId> {
    let mut affected = Vec::new();
    for index in catalog
        .property_indexes()
        .filter(|index| index.kind != IndexKind::FullText)
    {
        let old_value = old_node
            .filter(|node| node.labels.contains(&index.label_id))
            .and_then(|node| node.properties.get(&index.property));
        let new_value = (index.label_id == new_label_id)
            .then(|| new_properties.get(&index.property))
            .flatten();
        if old_value != new_value {
            affected.push(index.id);
        }
    }
    for index in catalog.composite_property_indexes() {
        if !composite_create_values_equal(old_node, new_label_id, new_properties, index) {
            affected.push(index.id);
        }
    }
    affected
}

pub fn node_property_index_sample_updates(
    catalog: &Catalog,
    node: &NodeRecord,
    property: &str,
    value: &Value,
) -> Vec<IndexId> {
    let mut affected = catalog
        .property_indexes()
        .filter(|index| {
            index.kind != IndexKind::FullText
                && index.property == property
                && node.labels.contains(&index.label_id)
                && node.properties.get(property) != Some(value)
        })
        .map(|index| index.id)
        .collect::<Vec<_>>();
    affected.extend(
        catalog
            .composite_property_indexes()
            .filter(|index| {
                node.labels.contains(&index.label_id)
                    && index
                        .properties
                        .iter()
                        .any(|candidate| candidate == property)
                    && composite_property_update_changes_key(node, property, value, index)
            })
            .map(|index| index.id),
    );
    affected
}

pub fn node_delete_index_sample_updates(catalog: &Catalog, node: &NodeRecord) -> Vec<IndexId> {
    let mut affected = catalog
        .property_indexes()
        .filter(|index| {
            index.kind != IndexKind::FullText
                && node.labels.contains(&index.label_id)
                && node.properties.contains_key(&index.property)
        })
        .map(|index| index.id)
        .collect::<Vec<_>>();
    affected.extend(
        catalog
            .composite_property_indexes()
            .filter(|index| {
                node.labels.contains(&index.label_id)
                    && index
                        .properties
                        .iter()
                        .all(|property| node.properties.contains_key(property))
            })
            .map(|index| index.id),
    );
    affected
}

fn composite_create_values_equal(
    old_node: Option<&NodeRecord>,
    new_label_id: LabelId,
    new_properties: &BTreeMap<String, Value>,
    index: &CompositeIndexDescriptor,
) -> bool {
    let old_indexed = old_node.is_some_and(|node| {
        node.labels.contains(&index.label_id)
            && index
                .properties
                .iter()
                .all(|property| node.properties.contains_key(property))
    });
    let new_indexed = new_label_id == index.label_id
        && index
            .properties
            .iter()
            .all(|property| new_properties.contains_key(property));
    match (old_indexed, new_indexed) {
        (false, false) => true,
        (true, true) => index.properties.iter().all(|property| {
            old_node.and_then(|node| node.properties.get(property)) == new_properties.get(property)
        }),
        _ => false,
    }
}

fn composite_property_update_changes_key(
    node: &NodeRecord,
    property: &str,
    value: &Value,
    index: &CompositeIndexDescriptor,
) -> bool {
    let old_indexed = index
        .properties
        .iter()
        .all(|candidate| node.properties.contains_key(candidate));
    let new_indexed = index
        .properties
        .iter()
        .all(|candidate| candidate == property || node.properties.contains_key(candidate));
    match (old_indexed, new_indexed) {
        (false, false) => false,
        (true, true) => node.properties.get(property) != Some(value),
        _ => true,
    }
}
