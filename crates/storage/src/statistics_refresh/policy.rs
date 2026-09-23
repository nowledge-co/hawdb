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

use hawdb_core::{Catalog, LabelId, PropertyType, RelTypeId, TableKind, Value};
use std::collections::BTreeSet;

const MIN_PROPERTY_HISTOGRAM_VALUES: usize = 128;
const MID_PROPERTY_HISTOGRAM_VALUES: usize = 256;
pub const MAX_PROPERTY_HISTOGRAM_VALUES: usize = 512;
const MID_PROPERTY_HISTOGRAM_DISTINCT_VALUES: usize = 1_024;
const MAX_PROPERTY_HISTOGRAM_DISTINCT_VALUES: usize = 4_096;
pub const MAX_BOUNDED_PATH_STAT_HOPS: usize = 3;

/// Total edge traversals allowed while collecting bounded-path statistics.
/// Statistics are cardinality estimates for plan choice, so truncating a hot
/// graph's enumeration is preferable to unbounded O(degree^hops) work.
pub const MAX_BOUNDED_PATH_STAT_VISITS: usize = 100_000;

fn property_value_supports_optimizer_statistics(value: &Value) -> bool {
    match value {
        Value::Null
        | Value::Bool(_)
        | Value::Int(_)
        | Value::Float(_)
        | Value::String(_)
        | Value::Uuid(_) => true,
        Value::Binary(_) | Value::List(_) | Value::Map(_) => false,
    }
}

fn property_type_supports_optimizer_statistics(value_type: PropertyType) -> bool {
    !matches!(value_type, PropertyType::Text | PropertyType::List)
}

pub fn node_property_supports_optimizer_statistics(
    catalog: Option<&Catalog>,
    label_id: LabelId,
    property: &str,
    value: &Value,
) -> bool {
    property_supports_optimizer_statistics(
        catalog.and_then(|catalog| {
            let label = catalog.label_name(label_id)?;
            declared_property_type(catalog, TableKind::Node, label, property)
        }),
        value,
    )
}

pub fn relationship_property_supports_optimizer_statistics(
    catalog: Option<&Catalog>,
    rel_type_id: RelTypeId,
    property: &str,
    value: &Value,
) -> bool {
    property_supports_optimizer_statistics(
        catalog.and_then(|catalog| {
            let rel_type = catalog.rel_type_name(rel_type_id)?;
            declared_property_type(catalog, TableKind::Relationship, rel_type, property)
        }),
        value,
    )
}

fn declared_property_type(
    catalog: &Catalog,
    table_kind: TableKind,
    table: &str,
    property: &str,
) -> Option<PropertyType> {
    let table_id = catalog.table_id(table_kind, table)?;
    let property_id = catalog.property_descriptor_id(table_id, property)?;
    catalog
        .property_descriptor(property_id)
        .map(|descriptor| descriptor.value_type)
}

fn property_supports_optimizer_statistics(
    declared_type: Option<PropertyType>,
    value: &Value,
) -> bool {
    declared_type.is_none_or(property_type_supports_optimizer_statistics)
        && property_value_supports_optimizer_statistics(value)
}

pub fn adaptive_histogram_sample_limit(distinct_count: usize) -> usize {
    if distinct_count <= MID_PROPERTY_HISTOGRAM_DISTINCT_VALUES {
        MIN_PROPERTY_HISTOGRAM_VALUES
    } else if distinct_count <= MAX_PROPERTY_HISTOGRAM_DISTINCT_VALUES {
        MID_PROPERTY_HISTOGRAM_VALUES
    } else {
        MAX_PROPERTY_HISTOGRAM_VALUES
    }
}

pub fn sample_histogram_values(values: BTreeSet<Value>) -> Vec<Value> {
    let len = values.len();
    let sample_limit = adaptive_histogram_sample_limit(len);
    if len <= sample_limit {
        return values.into_iter().collect();
    }
    let sorted = values.into_iter().collect::<Vec<_>>();
    (0..sample_limit)
        .map(|sample_index| {
            let value_index = sample_index * (len - 1) / (sample_limit - 1);
            sorted[value_index].clone()
        })
        .collect()
}
