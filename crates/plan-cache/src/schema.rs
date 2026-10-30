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

//! Schema identity used to invalidate reusable planning state.

use hawdb_core::schema::{Catalog, IndexKind, PropertyType, TableKind};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OptimizerSchemaKey {
    labels: Vec<String>,
    unique_constraints: Vec<(hawdb_core::schema::ConstraintSubject, String)>,
    relationship_types: Vec<String>,
    equality_indexes: Vec<(String, String)>,
    range_indexes: Vec<(String, String)>,
    full_text_indexes: Vec<(String, String)>,
    composite_indexes: Vec<(String, Vec<String>)>,
    /// Declared property descriptors, because declaring or altering a
    /// property's type/nullability/state changes which properties are
    /// eligible for optimizer statistics even when labels and indexes are
    /// unchanged. Without them a descriptor DDL inside the commit-lag window
    /// would leave now-ineligible statistics cached.
    property_descriptors: Vec<SchemaPropertyDescriptor>,
}

/// Property-descriptor identity for [`OptimizerSchemaKey`]. Lifecycle states
/// (`SchemaObjectState`) are intentionally excluded: they transition as online
/// DDL machinery backfills and would churn the shared schema key without
/// changing statistics eligibility, which depends only on the declared type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SchemaPropertyDescriptor {
    table: String,
    table_kind: TableKind,
    property: String,
    value_type: PropertyType,
    nullable: bool,
}

impl OptimizerSchemaKey {
    pub fn from_catalog(catalog: &Catalog) -> Self {
        let labels = catalog.labels().map(|label| label.name.clone()).collect();
        let relationship_types = catalog
            .rel_types()
            .map(|rel_type| rel_type.name.clone())
            .collect();
        let mut equality_indexes = Vec::new();
        let mut range_indexes = Vec::new();
        let mut full_text_indexes = Vec::new();
        for index in catalog.property_indexes() {
            let Some(label) = catalog.label_name(index.label_id) else {
                continue;
            };
            let descriptor = (label.to_string(), index.property.clone());
            match index.kind {
                IndexKind::Equality => equality_indexes.push(descriptor),
                IndexKind::Range => range_indexes.push(descriptor),
                IndexKind::FullText => full_text_indexes.push(descriptor),
            }
        }
        let composite_indexes = catalog
            .composite_property_indexes()
            .filter_map(|index| {
                catalog
                    .label_name(index.label_id)
                    .map(|label| (label.to_string(), index.properties.clone()))
            })
            .collect();
        let mut property_descriptors = catalog
            .property_descriptors()
            .filter_map(|property| {
                let table = catalog.table_descriptor(property.table_id)?;
                Some(SchemaPropertyDescriptor {
                    table: table.name.clone(),
                    table_kind: table.kind,
                    property: property.name.clone(),
                    value_type: property.value_type,
                    nullable: property.nullable,
                })
            })
            .collect::<Vec<_>>();
        property_descriptors.sort();
        Self {
            unique_constraints: catalog
                .unique_constraints()
                .map(|constraint| (constraint.subject, constraint.property.clone()))
                .collect(),
            labels,
            relationship_types,
            equality_indexes,
            range_indexes,
            full_text_indexes,
            composite_indexes,
            property_descriptors,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_core::schema::SchemaObjectState;

    /// Online DDL lifecycle transitions (`DeleteOnly`→…→`Public`) must not
    /// churn the shared schema key: statistics eligibility depends on the
    /// declared type, not on how far the machinery has backfilled.
    #[test]
    fn schema_key_ignores_descriptor_lifecycle_states() {
        let mut catalog = Catalog::default();
        let table = catalog.get_or_create_table(TableKind::Node, "T");
        let property = catalog.get_or_create_property(table, "body", PropertyType::String, true);
        let baseline = OptimizerSchemaKey::from_catalog(&catalog);

        let mut churned = catalog.clone();
        churned.set_property_state(property, SchemaObjectState::Backfill);
        churned.set_table_state(table, SchemaObjectState::Validating);
        assert_eq!(baseline, OptimizerSchemaKey::from_catalog(&churned));

        let mut extended = catalog.clone();
        extended.get_or_create_property(table, "title", PropertyType::Text, false);
        assert_ne!(baseline, OptimizerSchemaKey::from_catalog(&extended));
    }
}
