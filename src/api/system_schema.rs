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

use super::Database;
use crate::error::{HawDBError, Result};
use crate::value::Value;
use hawdb_relational::system_schema::{
    decode_applied_system_schema_migration_query_row, engine_system_schema_registry,
    is_engine_system_schema_bootstrap, read_applied_system_schema_migrations,
    state_with_engine_system_schema, validate_applied_system_schema_migrations,
    validate_engine_system_schema_registry_table, validate_engine_system_schema_state,
    validate_system_schema_registry, AppliedSystemSchemaMigration, ENGINE_SYSTEM_SCHEMA_OWNER,
    ENGINE_SYSTEM_SCHEMA_REGISTRY_INSERT_SQL, ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE,
};
pub use hawdb_relational::{
    SystemSchemaMigration, SystemSchemaRegistry, SystemSchemaUpgradeReport,
};
use hawdb_storage::RelationalState;

impl Database {
    pub(super) fn apply_engine_system_schema(&mut self) -> Result<SystemSchemaUpgradeReport> {
        self.apply_system_schema_registry_inner(&engine_system_schema_registry())
    }

    /// Applies an ordered, append-only system schema owned by the engine or an
    /// embedding application.
    ///
    /// All pending DDL and its migration records are committed in one WAL
    /// transaction. Existing records are checksum-verified before any write.
    pub fn apply_system_schema_registry(
        &mut self,
        registry: &SystemSchemaRegistry,
    ) -> Result<SystemSchemaUpgradeReport> {
        validate_system_schema_registry(registry)?;
        if registry.owner() == ENGINE_SYSTEM_SCHEMA_OWNER
            && registry != &engine_system_schema_registry()
        {
            return Err(HawDBError::Semantic(format!(
                "system schema owner {ENGINE_SYSTEM_SCHEMA_OWNER} is reserved by the engine"
            )));
        }
        if registry.owner() != ENGINE_SYSTEM_SCHEMA_OWNER {
            self.apply_system_schema_registry_inner(&engine_system_schema_registry())?;
        }
        self.apply_system_schema_registry_inner(registry)
    }

    fn apply_system_schema_registry_inner(
        &mut self,
        registry: &SystemSchemaRegistry,
    ) -> Result<SystemSchemaUpgradeReport> {
        validate_system_schema_registry(registry)?;
        let commit_epoch_before = self.commit_epoch();
        let registry_table_present = self
            .store
            .relational_state()
            .table_schema(ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE)
            .is_some();

        if registry_table_present {
            validate_engine_system_schema_registry_table(self.store.relational_state())?;
        } else if registry.owner() != ENGINE_SYSTEM_SCHEMA_OWNER {
            return Err(HawDBError::Storage(
                "system schema registry table is missing after engine bootstrap".to_string(),
            ));
        }

        let applied = if registry_table_present {
            let max_rows = registry.migrations().len().saturating_add(1);
            if self.store.relational_state().materialized_rows_resident() {
                read_applied_system_schema_migrations(
                    self.store.relational_state(),
                    registry.owner(),
                    max_rows,
                )?
            } else {
                self.read_applied_migrations_query(registry.owner(), max_rows)?
            }
        } else {
            Vec::new()
        };
        validate_applied_system_schema_migrations(registry, &applied, registry_table_present)?;

        let previous_version = applied
            .last()
            .map_or(0, AppliedSystemSchemaMigration::version);
        let pending = registry
            .migrations()
            .iter()
            .filter(|migration| migration.version() > previous_version)
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(SystemSchemaUpgradeReport {
                owner: registry.owner().to_string(),
                previous_version,
                current_version: previous_version,
                applied_versions: Vec::new(),
                commit_epoch_before,
                commit_epoch_after: commit_epoch_before,
            });
        }
        if self.config.read_only {
            return Err(HawDBError::Execution(format!(
                "system schema {} requires upgrade from version {} to {} but the database is read-only",
                registry.owner(),
                previous_version,
                registry.current_version()
            )));
        }

        let mut transaction = self.begin_transaction();
        let mut applied_versions = Vec::with_capacity(pending.len());
        for migration in pending {
            for statement in migration.statements() {
                if registry.owner() == ENGINE_SYSTEM_SCHEMA_OWNER {
                    transaction.query_system_schema_sql(statement)?;
                } else {
                    transaction.query_sql(statement)?;
                }
            }
            let version = i64::try_from(migration.version()).map_err(|_| {
                HawDBError::Semantic(format!(
                    "system schema {} migration version {} exceeds BIGINT",
                    registry.owner(),
                    migration.version()
                ))
            })?;
            transaction.query_system_schema_sql_with_params(
                ENGINE_SYSTEM_SCHEMA_REGISTRY_INSERT_SQL,
                &[
                    Value::String(format!("{}:{}", registry.owner(), migration.version())),
                    Value::String(registry.owner().to_string()),
                    Value::Int(version),
                    Value::String(migration.name().to_string()),
                    Value::String(migration.checksum(registry.owner())),
                ],
            )?;
            applied_versions.push(migration.version());
        }
        transaction.commit()?;

        Ok(SystemSchemaUpgradeReport {
            owner: registry.owner().to_string(),
            previous_version,
            current_version: registry.current_version(),
            applied_versions,
            commit_epoch_before,
            commit_epoch_after: self.commit_epoch(),
        })
    }

    fn read_applied_migrations_query(
        &mut self,
        owner: &str,
        max_rows: usize,
    ) -> Result<Vec<AppliedSystemSchemaMigration>> {
        let limit = i64::try_from(max_rows).map_err(|_| {
            HawDBError::Execution("system schema migration read limit exceeds BIGINT".to_string())
        })?;
        let output = self.query_sql_with_params_bounded(
            "SELECT version, name, checksum FROM hawdb_schema_migrations \
             WHERE owner = $1 ORDER BY version ASC LIMIT $2",
            &[Value::String(owner.to_string()), Value::Int(limit)],
            Some(max_rows),
        )?;
        output
            .rows
            .iter()
            .map(|row| decode_applied_system_schema_migration_query_row(owner, row))
            .collect()
    }

    pub(super) fn has_only_engine_system_schema_bootstrap(&self) -> Result<bool> {
        is_engine_system_schema_bootstrap(self.store.relational_state())
    }

    pub(super) fn validate_hawdb_lightning_system_schema(state: &RelationalState) -> Result<()> {
        validate_engine_system_schema_state(state)
    }

    pub(super) fn hawdb_lightning_relational_state(&self) -> Result<RelationalState> {
        state_with_engine_system_schema(self.store.relational_state())
    }
}

#[cfg(test)]
mod facade_tests {
    use super::{SystemSchemaMigration, SystemSchemaRegistry, SystemSchemaUpgradeReport};

    #[test]
    fn facade_reexports_relational_system_schema_contracts_without_conversion() {
        let _: fn(SystemSchemaMigration) -> hawdb_relational::SystemSchemaMigration = |value| value;
        let _: fn(SystemSchemaRegistry) -> hawdb_relational::SystemSchemaRegistry = |value| value;
        let _: fn(SystemSchemaUpgradeReport) -> hawdb_relational::SystemSchemaUpgradeReport =
            |value| value;
    }
}
