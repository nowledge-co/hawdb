// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;

/// Additional complete read domains captured at the feed's same E. Names are
/// exact canonical names; graph labels/types may be absent at E. SQL tables
/// must exist. These scopes do not certify mapping, stage coverage or C..E
/// source continuity. Callback queries retain their normal qualification.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProjectionReadScopes<'a> {
    pub node_labels: &'a [&'a str],
    pub relationship_types: &'a [&'a str],
    pub relational_tables: &'a [&'a str],
}

impl ProjectionReadScopes<'_> {
    pub(super) fn admit(self, limits: PreparedReadLimits) -> Result<()> {
        let count = self
            .node_labels
            .len()
            .checked_add(self.relationship_types.len())
            .and_then(|n| n.checked_add(self.relational_tables.len()))
            .filter(|n| *n <= limits.dependency_entries.get())
            .ok_or_else(|| {
                HawDBError::Execution("projection read scope entry admission exhausted".into())
            })?;
        let mut bytes = count
            .checked_mul(
                std::mem::size_of::<VersionKey>()
                    + std::mem::size_of::<hawdb_storage::version::VersionWrite>(),
            )
            .ok_or_else(|| {
                HawDBError::Execution("projection read scope byte admission exhausted".into())
            })?;
        // Admit the whole borrowed input before creating any owned keys.
        // Duplicate input names consume input admission even when keys coalesce.
        for name in self
            .node_labels
            .iter()
            .chain(self.relationship_types)
            .chain(self.relational_tables)
        {
            bytes = bytes
                .checked_add(name.len())
                .filter(|n| *n <= limits.dependency_bytes.get())
                .ok_or_else(|| {
                    HawDBError::Execution("projection read scope byte admission exhausted".into())
                })?;
            if name.is_empty() {
                return Err(HawDBError::Execution(
                    "projection read scope names must be nonempty".into(),
                ));
            }
        }
        if bytes > limits.dependency_bytes.get() {
            return Err(HawDBError::Execution(
                "projection read scope byte admission exhausted".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn register(self, prepared: &mut PreparedRead) -> Result<()> {
        prepared.read.store.ensure_usable()?;
        if prepared.read.projection_relational.is_some() {
            return Err(HawDBError::Execution(
                "projection read scopes require canonical storage".into(),
            ));
        }
        for table in self.relational_tables {
            if prepared
                .read
                .store
                .relational_state()
                .table_schema(table)
                .is_none()
            {
                return Err(HawDBError::Execution(
                    "projection read scope table is not canonical or does not exist".into(),
                ));
            }
        }
        let dependencies = &mut prepared.dependencies.graph;
        for label in self.node_labels {
            dependencies
                .record(VersionKey::ReadNodeDomain(Some((*label).into())))
                .map_err(|e| HawDBError::Execution(e.to_string()))?;
        }
        for kind in self.relationship_types {
            dependencies
                .record(VersionKey::ReadRelationshipDomain(Some((*kind).into())))
                .map_err(|e| HawDBError::Execution(e.to_string()))?;
        }
        for table in self.relational_tables {
            dependencies
                .record(VersionKey::ReadRelationalTableDomain((*table).into()))
                .map_err(|e| HawDBError::Execution(e.to_string()))?;
        }
        Ok(())
    }
}
