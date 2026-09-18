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

use crate::{SchemaObjectState, SchemaPropertyType, SchemaTableKind};
use hawdb_core::schema::{PropertyType, TableKind};

pub const fn table_kind_to_core(kind: SchemaTableKind) -> TableKind {
    match kind {
        SchemaTableKind::Node => TableKind::Node,
        SchemaTableKind::Relationship => TableKind::Relationship,
    }
}

pub const fn property_type_to_core(value_type: SchemaPropertyType) -> PropertyType {
    match value_type {
        SchemaPropertyType::Any => PropertyType::Any,
        SchemaPropertyType::Bool => PropertyType::Bool,
        SchemaPropertyType::Int => PropertyType::Int,
        SchemaPropertyType::Float => PropertyType::Float,
        SchemaPropertyType::String => PropertyType::String,
        SchemaPropertyType::Text => PropertyType::Text,
        SchemaPropertyType::List => PropertyType::List,
    }
}

pub const fn object_state_to_core(
    state: SchemaObjectState,
) -> hawdb_core::schema::SchemaObjectState {
    match state {
        SchemaObjectState::DeleteOnly => hawdb_core::schema::SchemaObjectState::DeleteOnly,
        SchemaObjectState::WriteOnly => hawdb_core::schema::SchemaObjectState::WriteOnly,
        SchemaObjectState::Backfill => hawdb_core::schema::SchemaObjectState::Backfill,
        SchemaObjectState::Validating => hawdb_core::schema::SchemaObjectState::Validating,
        SchemaObjectState::Public => hawdb_core::schema::SchemaObjectState::Public,
        SchemaObjectState::Gc => hawdb_core::schema::SchemaObjectState::Gc,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_ddl_table_kind_to_core_schema_kind() {
        assert_eq!(table_kind_to_core(SchemaTableKind::Node), TableKind::Node);
        assert_eq!(
            table_kind_to_core(SchemaTableKind::Relationship),
            TableKind::Relationship
        );
    }

    #[test]
    fn maps_ddl_property_type_to_core_schema_type() {
        assert_eq!(
            property_type_to_core(SchemaPropertyType::Any),
            PropertyType::Any
        );
        assert_eq!(
            property_type_to_core(SchemaPropertyType::Bool),
            PropertyType::Bool
        );
        assert_eq!(
            property_type_to_core(SchemaPropertyType::Int),
            PropertyType::Int
        );
        assert_eq!(
            property_type_to_core(SchemaPropertyType::Float),
            PropertyType::Float
        );
        assert_eq!(
            property_type_to_core(SchemaPropertyType::String),
            PropertyType::String
        );
        assert_eq!(
            property_type_to_core(SchemaPropertyType::Text),
            PropertyType::Text
        );
        assert_eq!(
            property_type_to_core(SchemaPropertyType::List),
            PropertyType::List
        );
    }

    #[test]
    fn exposes_stable_state_fingerprints() {
        assert_eq!(SchemaObjectState::DeleteOnly.as_str(), "delete_only");
        assert_eq!(SchemaObjectState::Public.as_str(), "public");
        assert_eq!(SchemaObjectState::Gc.as_str(), "gc");
    }

    #[test]
    fn maps_all_ddl_object_states_to_core_schema_states() {
        use hawdb_core::schema::SchemaObjectState as CoreState;

        for (command, catalog) in [
            (SchemaObjectState::DeleteOnly, CoreState::DeleteOnly),
            (SchemaObjectState::WriteOnly, CoreState::WriteOnly),
            (SchemaObjectState::Backfill, CoreState::Backfill),
            (SchemaObjectState::Validating, CoreState::Validating),
            (SchemaObjectState::Public, CoreState::Public),
            (SchemaObjectState::Gc, CoreState::Gc),
        ] {
            assert_eq!(object_state_to_core(command), catalog);
        }
    }
}
