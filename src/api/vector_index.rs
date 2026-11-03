// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Schema resolution and Consumer ownership for explicit vector indexes.

use super::{DatabaseReadTransaction, HawDBError, Result};
use hawdb_core::{
    PropertyType, SchemaObjectState, TableKind, VectorDistance, VectorIndexBackend,
    VectorIndexDescriptor, VectorIndexTarget,
};
use hawdb_integrity::IntegrityHasher;
use hawdb_storage::relational::RelationalScalarType;

mod build;
pub use build::{VectorIndexBuildError, VectorIndexBuildOptions, VectorIndexUpdateReport};
mod language;
pub use language::{
    VectorIndexLanguageBudget, VectorIndexLanguageReport, VectorIndexSqlReport,
    VectorIndexSqlRequest,
};
mod read;
pub use read::{VectorIndexReadError, VectorIndexReadTransaction};

/// User-supplied names before resolution against a pinned canonical schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorIndexField {
    SqlColumn { table: String, column: String },
    NodeProperty { table: String, property: String },
}

impl VectorIndexField {
    fn from_target(target: &VectorIndexTarget) -> Self {
        match target {
            VectorIndexTarget::Relational { table, column, .. } => Self::SqlColumn {
                table: table.clone(),
                column: column.clone(),
            },
            VectorIndexTarget::NodeProperty {
                table, property, ..
            } => Self::NodeProperty {
                table: table.clone(),
                property: property.clone(),
            },
        }
    }
}

impl DatabaseReadTransaction {
    /// Resolve a maintenance definition, deriving dimensions from the schema.
    /// The result grants neither a source read nor publication authority.
    pub fn resolve_vector_index(
        &self,
        name: &str,
        field: &VectorIndexField,
        distance: VectorDistance,
        backend: VectorIndexBackend,
    ) -> Result<VectorIndexDescriptor> {
        self.store.ensure_usable()?;
        let (target, dimension, digest) = match field {
            VectorIndexField::SqlColumn { table, column } => {
                let state = self.store.relational_state();
                let schema = state
                    .table_schema(table)
                    .ok_or_else(|| invalid("target must be an ordinary relational table"))?;
                let (ordinal, field) = schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, field)| &field.name == column)
                    .ok_or_else(|| invalid("target column does not exist"))?;
                let RelationalScalarType::Vector(dimension) = field.scalar_type else {
                    return Err(invalid("target column must declare VECTOR(n)"));
                };
                let digest = state
                    .table_schema_digest(table)
                    .map_err(|error| HawDBError::Storage(error.to_string()))?
                    .ok_or_else(|| invalid("target schema has no digest"))?;
                (
                    VectorIndexTarget::Relational {
                        table: schema.name.clone(),
                        column: field.name.clone(),
                        column_ordinal: u32::try_from(ordinal)
                            .map_err(|_| invalid("column ordinal exceeds u32"))?,
                    },
                    dimension,
                    *digest.as_bytes(),
                )
            }
            VectorIndexField::NodeProperty { table, property } => {
                let table_id = self
                    .catalog
                    .table_id(TableKind::Node, table)
                    .ok_or_else(|| invalid("target node table does not exist"))?;
                let table_schema = self
                    .catalog
                    .table_descriptor(table_id)
                    .filter(|table| table.state == SchemaObjectState::Public)
                    .ok_or_else(|| invalid("target node table is not public"))?;
                let property_id = self
                    .catalog
                    .property_descriptor_id(table_id, property)
                    .ok_or_else(|| invalid("target property must have a schema declaration"))?;
                let field = self
                    .catalog
                    .property_descriptor(property_id)
                    .filter(|field| field.state == SchemaObjectState::Public)
                    .ok_or_else(|| invalid("target property is not public"))?;
                let PropertyType::Vector(dimension) = field.value_type else {
                    return Err(invalid("target property must declare VECTOR(n)"));
                };
                let mut hash = IntegrityHasher::new();
                hash.update(b"hawdb-vector-node-field-v1");
                hash.update(&table_id.0.to_le_bytes());
                hash.update(&property_id.0.to_le_bytes());
                for text in [&table_schema.name, &field.name] {
                    hash.update(&(text.len() as u64).to_le_bytes());
                    hash.update(text.as_bytes());
                }
                hash.update(&dimension.get().to_le_bytes());
                hash.update(&[u8::from(field.nullable)]);
                (
                    VectorIndexTarget::NodeProperty {
                        table: table_schema.name.clone(),
                        property: field.name.clone(),
                        table_id,
                        property_id,
                    },
                    dimension,
                    *hash.finish().sha256.as_bytes(),
                )
            }
        };
        VectorIndexDescriptor::new(
            name.to_owned(),
            target,
            dimension,
            distance,
            backend,
            digest,
        )
    }
}

fn invalid(message: &str) -> HawDBError {
    HawDBError::Semantic(format!("invalid vector index: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    fn backend() -> VectorIndexBackend {
        VectorIndexBackend::RaBitQ {
            bit_width: 4,
            segment_rows: 1024,
            transform_seed: 42,
        }
    }

    #[test]
    fn vector_index_resolves_both_languages_against_the_declared_schema() {
        let mut db = Database::new();
        db.query_sql("CREATE TABLE documents (id TEXT PRIMARY KEY, embedding VECTOR(3))")
            .unwrap();
        db.query("CREATE NODE TABLE Document").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Document(embedding) TYPE VECTOR(3)")
            .unwrap();
        let source = db.begin_read_transaction().unwrap();
        let sql_field = VectorIndexField::SqlColumn {
            table: "documents".into(),
            column: "embedding".into(),
        };
        let node_field = VectorIndexField::NodeProperty {
            table: "Document".into(),
            property: "embedding".into(),
        };
        let sql = source
            .resolve_vector_index("sql", &sql_field, VectorDistance::Cosine, backend())
            .unwrap();
        let node = source
            .resolve_vector_index("graph", &node_field, VectorDistance::Cosine, backend())
            .unwrap();
        assert_eq!(sql.dimension().get(), 3);
        assert_eq!(sql.dimension(), node.dimension());
        assert_ne!(sql.schema_digest(), node.schema_digest());
        assert!(matches!(
            sql.target(),
            VectorIndexTarget::Relational {
                column_ordinal: 1,
                ..
            }
        ));
        assert!(matches!(
            node.target(),
            VectorIndexTarget::NodeProperty { .. }
        ));
        let l2 = source
            .resolve_vector_index("l2", &sql_field, VectorDistance::Euclidean, backend())
            .unwrap();
        assert_eq!(l2.distance(), VectorDistance::Euclidean);
        assert_eq!(l2.backend(), backend());
        let scalar = VectorIndexField::SqlColumn {
            table: "documents".into(),
            column: "id".into(),
        };
        assert!(source
            .resolve_vector_index("scalar", &scalar, VectorDistance::Cosine, backend())
            .is_err());
        drop(source);
        db.query_sql("ALTER TABLE documents ADD COLUMN category TEXT")
            .unwrap();
        let changed = db
            .begin_read_transaction()
            .unwrap()
            .resolve_vector_index("sql", &sql_field, VectorDistance::Cosine, backend())
            .unwrap();
        assert_ne!(changed.schema_digest(), sql.schema_digest());
    }
}
