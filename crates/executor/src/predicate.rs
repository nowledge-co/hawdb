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

//! Storage record predicates shared by execution operators.

use hawdb_core::{Catalog, LabelId, Value};
use hawdb_plan::ComparisonOp;
pub use hawdb_storage::predicate::property_filter_matches as property_filter_matches_values;
use hawdb_storage::predicate::{comparable_value_ordering, properties_contain_all};
use hawdb_storage::{NodeRecord, PropertyFilter, RelRecord};
use std::cmp::Ordering;
use std::collections::BTreeMap;

pub fn label_ids_for_pattern(catalog: &Catalog, label: &str) -> Option<Vec<LabelId>> {
    if label.is_empty() {
        return None;
    }
    Some(
        label
            .split(':')
            .filter_map(|label| catalog.label_id(label))
            .collect(),
    )
}

pub fn node_matches_label_pattern(node: &NodeRecord, label_ids: Option<&[LabelId]>) -> bool {
    match label_ids {
        None => true,
        Some(label_ids) => label_ids
            .iter()
            .any(|label_id| node.labels.contains(label_id)),
    }
}

pub fn node_properties_match(node: &NodeRecord, properties: &BTreeMap<String, Value>) -> bool {
    properties_contain_all(&node.properties, properties)
}

pub fn node_matches_property_filter(node: &NodeRecord, filter: &PropertyFilter) -> bool {
    property_filter_matches_values(filter, node.id.0, &node.properties)
}

pub fn relationship_properties_match(
    relationship: &RelRecord,
    properties: &BTreeMap<String, Value>,
) -> bool {
    properties_contain_all(&relationship.properties, properties)
}

pub fn property_filter_from_properties(
    properties: &BTreeMap<String, Value>,
) -> Option<PropertyFilter> {
    if properties.is_empty() {
        return None;
    }
    let mut filters = properties
        .iter()
        .map(|(property, value)| PropertyFilter::Eq {
            property: property.clone(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    if filters.len() == 1 {
        filters.pop()
    } else {
        Some(PropertyFilter::And(filters))
    }
}

pub fn combine_property_filters(
    left: Option<PropertyFilter>,
    right: Option<PropertyFilter>,
) -> Option<PropertyFilter> {
    match (left, right) {
        (None, None) => None,
        (Some(filter), None) | (None, Some(filter)) => Some(filter),
        (Some(left), Some(right)) => Some(PropertyFilter::And(vec![left, right])),
    }
}

pub fn compare_property_values(actual: &Value, op: ComparisonOp, expected: &Value) -> bool {
    let Some(ordering) = comparable_value_ordering(actual, expected) else {
        return false;
    };
    match op {
        ComparisonOp::Lt => ordering == Ordering::Less,
        ComparisonOp::Lte => ordering != Ordering::Greater,
        ComparisonOp::Gt => ordering == Ordering::Greater,
        ComparisonOp::Gte => ordering != Ordering::Less,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_maps_form_deterministic_conjunctions() {
        let properties = BTreeMap::from([
            ("kind".to_string(), Value::String("note".to_string())),
            ("space".to_string(), Value::String("default".to_string())),
        ]);
        let filter = property_filter_from_properties(&properties).expect("non-empty filter");

        assert!(matches!(filter, PropertyFilter::And(filters) if filters.len() == 2));
    }
}
