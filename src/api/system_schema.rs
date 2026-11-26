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

use super::{Database, DatabaseTransaction};
use crate::error::{HawDBError, Result};
use crate::value::Value;
use hawdb_relational::system_schema::{
    decode_applied_system_schema_migration_query_row, engine_system_schema_registry,
    is_engine_system_schema_bootstrap, read_applied_system_schema_migrations,
    state_with_engine_system_schema, system_schema_migration_id,
    validate_applied_system_schema_migrations, validate_engine_system_schema_registry_table,
    validate_engine_system_schema_state, validate_system_schema_registry,
    AppliedSystemSchemaMigration, SystemSchemaLedger, ENGINE_SYSTEM_SCHEMA_OWNER,
    ENGINE_SYSTEM_SCHEMA_REGISTRY_INSERT_SQL, ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE,
};
pub use hawdb_relational::{
    SystemSchemaMigration, SystemSchemaRegistry, SystemSchemaStatus, SystemSchemaUpgradeReport,
};
use hawdb_storage::relational::RelationalState;

/// A registry's pending suffix, verified before any write.
struct SystemSchemaPlan<'a> {
    registry: &'a SystemSchemaRegistry,
    previous_version: u64,
    pending: Vec<&'a SystemSchemaMigration>,
}

/// Validates a set of registries a host supplied together.
pub(crate) fn validate_system_schema_registries(registries: &[SystemSchemaRegistry]) -> Result<()> {
    let engine_registry = engine_system_schema_registry();
    let mut owners = std::collections::BTreeSet::new();
    for registry in registries {
        validate_system_schema_registry(registry)?;
        if registry.owner() == ENGINE_SYSTEM_SCHEMA_OWNER && registry != &engine_registry {
            return Err(HawDBError::Semantic(format!(
                "system schema owner {ENGINE_SYSTEM_SCHEMA_OWNER} is reserved by the engine"
            )));
        }
        if !owners.insert(registry.owner()) {
            return Err(HawDBError::Semantic(format!(
                "system schema owner {} is registered more than once",
                registry.owner()
            )));
        }
    }
    Ok(())
}

/// Writes one migration record through the privileged registry path, the only
/// one that may write the registry table.
fn insert_migration_record(
    transaction: &mut DatabaseTransaction<'_>,
    owner: &str,
    version: u64,
    name: &str,
    checksum: &str,
) -> Result<()> {
    let bound_version = i64::try_from(version).map_err(|_| {
        HawDBError::Semantic(format!(
            "system schema {owner} migration version {version} exceeds BIGINT"
        ))
    })?;
    transaction.query_system_schema_sql_with_params(
        ENGINE_SYSTEM_SCHEMA_REGISTRY_INSERT_SQL,
        &[
            Value::String(system_schema_migration_id(owner, version)),
            Value::String(owner.to_string()),
            Value::Int(bound_version),
            Value::String(name.to_string()),
            Value::String(checksum.to_string()),
        ],
    )?;
    Ok(())
}

/// The version that the engine's own system schema registry ends at in this
/// build. [`Database::system_schema_status`] compares it with the version
/// installed in an open database.
pub fn engine_system_schema_expected_version() -> u64 {
    engine_system_schema_registry().current_version()
}

impl Database {
    /// Restores the application owners' migration records of a logical dump
    /// into this fresh import target, in one transaction, and returns how many
    /// were written. The target keeps the engine records its own bootstrap
    /// wrote, and it must hold no other record.
    ///
    /// Only logical dump import calls this, with a ledger it has validated.
    /// It reaches the registry table through fixed INSERTs of typed records
    /// and executes nothing else from the dump.
    pub(crate) fn restore_system_schema_ledger(
        &mut self,
        ledger: &SystemSchemaLedger,
    ) -> Result<u64> {
        let engine = engine_system_schema_registry();
        let bootstrapped = self
            .plan_system_schema_registry(&engine)?
            .pending
            .is_empty();
        let records = self.system_schema_record_count()?;
        if !bootstrapped || records != engine.current_version() {
            return Err(HawDBError::Storage(
                "system schema records can only be restored into a fresh database".to_string(),
            ));
        }
        let mut written = 0;
        let mut transaction = self.begin_transaction()?;
        for (owner, applied) in ledger.application_owners() {
            for record in applied {
                insert_migration_record(
                    &mut transaction,
                    owner,
                    record.version(),
                    record.name(),
                    record.checksum(),
                )?;
                written += 1;
            }
        }
        if written == 0 {
            transaction.rollback();
        } else {
            transaction.commit()?;
        }
        Ok(written)
    }

    /// Requires the registry table to hold the engine's records and exactly
    /// the application records of `ledger`.
    pub(crate) fn verify_restored_system_schema_ledger(
        &mut self,
        ledger: &SystemSchemaLedger,
    ) -> Result<()> {
        let engine = engine_system_schema_registry();
        let mut expected = engine.current_version();
        let mut matches = self
            .plan_system_schema_registry(&engine)?
            .pending
            .is_empty();
        for (owner, applied) in ledger.application_owners() {
            expected += applied.len() as u64;
            matches &= self
                .read_applied_migrations_query(owner, applied.len().saturating_add(1))?
                .as_slice()
                == applied;
        }
        if !matches || self.system_schema_record_count()? != expected {
            return Err(HawDBError::StorageIntegrity(
                "restored system schema records do not match their source".to_string(),
            ));
        }
        Ok(())
    }

    fn system_schema_record_count(&mut self) -> Result<u64> {
        let output = self.query_sql("SELECT COUNT(*) AS n FROM hawdb_schema_migrations")?;
        let count = output
            .value_rows()
            .next()
            .and_then(|row| match row.first() {
                Some(Value::Int(count)) => u64::try_from(*count).ok(),
                _ => None,
            });
        count.ok_or_else(|| {
            HawDBError::StorageIntegrity(
                "COUNT(*) on the system schema registry returned no count".to_string(),
            )
        })
    }

    pub(super) fn apply_engine_system_schema(&mut self) -> Result<SystemSchemaUpgradeReport> {
        let mut reports =
            self.apply_system_schema_registries_inner(&[&engine_system_schema_registry()])?;
        Ok(reports
            .pop()
            .expect("one system schema report per registry"))
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
        let mut reports = self.apply_system_schema_registries(std::slice::from_ref(registry))?;
        Ok(reports
            .pop()
            .expect("one system schema report per registry"))
    }

    /// Applies several system schema registries, one per owner, as one
    /// upgrade.
    ///
    /// Every registry is validated and its applied records checksum-verified
    /// before any write. The pending migrations of all owners are then staged,
    /// in slice order, in one transaction and published by one WAL commit, so
    /// either every registry reaches its current version or none advances.
    /// A schema commit that rewrites existing rows runs a canonical row
    /// checkpoint before returning; one commit pays it once rather than once
    /// per registry. New empty tables and their indexes leave it to the next
    /// ordinary checkpoint, or to the first commit that writes a row to one
    /// of them (#1942).
    ///
    /// Reports are returned in slice order. The engine registry is applied
    /// first, in its own transaction, when it is pending.
    pub fn apply_system_schema_registries(
        &mut self,
        registries: &[SystemSchemaRegistry],
    ) -> Result<Vec<SystemSchemaUpgradeReport>> {
        let engine_registry = engine_system_schema_registry();
        validate_system_schema_registries(registries)?;
        // Application registries are recorded in the engine registry table,
        // so the engine schema must be current before they are planned.
        let engine_report = self
            .apply_system_schema_registries_inner(&[&engine_registry])?
            .pop()
            .expect("one system schema report per registry");
        let application = registries
            .iter()
            .filter(|registry| registry.owner() != ENGINE_SYSTEM_SCHEMA_OWNER)
            .collect::<Vec<_>>();
        let mut application_reports = self
            .apply_system_schema_registries_inner(&application)?
            .into_iter();
        Ok(registries
            .iter()
            .map(|registry| {
                if registry.owner() == ENGINE_SYSTEM_SCHEMA_OWNER {
                    engine_report.clone()
                } else {
                    application_reports
                        .next()
                        .expect("one system schema report per application registry")
                }
            })
            .collect())
    }

    /// Reports the installed and expected versions of the engine's system
    /// schema, then of every given registry in slice order, without applying
    /// anything. It is available on read-only handles.
    ///
    /// Registries are validated and applied records checksum-verified as
    /// [`Self::apply_system_schema_registries`] would, so a drifted or future
    /// schema is an error rather than a status. The engine registry is always
    /// reported, once, as the first entry.
    ///
    /// [`Database::open`] refuses a database whose engine schema is not
    /// current when it is opened read-only, with
    /// [`HawDBError::SystemSchemaUpgradeRequired`]. An engine entry is
    /// therefore pending only on an in-memory handle that recorded no schema
    /// yet.
    pub fn system_schema_status(
        &mut self,
        registries: &[SystemSchemaRegistry],
    ) -> Result<Vec<SystemSchemaStatus>> {
        let engine_registry = engine_system_schema_registry();
        validate_system_schema_registries(registries)?;
        let registry_table_present = self.system_schema_registry_table_present()?;
        std::iter::once(&engine_registry)
            .chain(
                registries
                    .iter()
                    .filter(|registry| registry.owner() != ENGINE_SYSTEM_SCHEMA_OWNER),
            )
            .map(|registry| {
                let installed_version = if registry_table_present {
                    self.plan_system_schema_registry(registry)?.previous_version
                } else {
                    0
                };
                let expected_version = registry.current_version();
                Ok(SystemSchemaStatus {
                    owner: registry.owner().to_string(),
                    installed_version,
                    expected_version,
                    pending: installed_version < expected_version,
                })
            })
            .collect()
    }

    fn system_schema_registry_table_present(&self) -> Result<bool> {
        Ok(self
            .runtime
            .get()?
            .store
            .relational_state()
            .table_schema(ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE)
            .is_some())
    }

    /// Plans every registry before any write, then commits all pending
    /// migrations in one transaction.
    fn apply_system_schema_registries_inner(
        &mut self,
        registries: &[&SystemSchemaRegistry],
    ) -> Result<Vec<SystemSchemaUpgradeReport>> {
        let commit_epoch_before = self.commit_epoch()?;
        let mut plans = Vec::with_capacity(registries.len());
        for registry in registries {
            plans.push(self.plan_system_schema_registry(registry)?);
        }
        let pending_owners = plans
            .iter()
            .filter(|plan| !plan.pending.is_empty())
            .collect::<Vec<_>>();
        if !pending_owners.is_empty() && self.config.read_only {
            let plan = pending_owners[0];
            return Err(HawDBError::SystemSchemaUpgradeRequired {
                owner: plan.registry.owner().to_string(),
                installed_version: plan.previous_version,
                expected_version: plan.registry.current_version(),
            });
        }

        let mut applied_versions = Vec::with_capacity(plans.len());
        if !pending_owners.is_empty() {
            let mut transaction = self.begin_transaction()?;
            for plan in &plans {
                let registry = plan.registry;
                let mut versions = Vec::with_capacity(plan.pending.len());
                for migration in &plan.pending {
                    for statement in migration.statements() {
                        if registry.owner() == ENGINE_SYSTEM_SCHEMA_OWNER {
                            transaction.query_system_schema_sql(statement)?;
                        } else {
                            transaction.query_sql(statement)?;
                        }
                    }
                    insert_migration_record(
                        &mut transaction,
                        registry.owner(),
                        migration.version(),
                        migration.name(),
                        &migration.checksum(registry.owner()),
                    )?;
                    versions.push(migration.version());
                }
                applied_versions.push(versions);
            }
            transaction.commit()?;
        } else {
            applied_versions.resize_with(plans.len(), Vec::new);
        }

        let commit_epoch_after = self.commit_epoch()?;
        Ok(plans
            .into_iter()
            .zip(applied_versions)
            .map(|(plan, applied_versions)| {
                let pending = !applied_versions.is_empty();
                SystemSchemaUpgradeReport {
                    owner: plan.registry.owner().to_string(),
                    previous_version: plan.previous_version,
                    current_version: if pending {
                        plan.registry.current_version()
                    } else {
                        plan.previous_version
                    },
                    applied_versions,
                    commit_epoch_before,
                    commit_epoch_after: if pending {
                        commit_epoch_after
                    } else {
                        commit_epoch_before
                    },
                }
            })
            .collect())
    }

    /// Verifies a registry against its applied records without writing.
    fn plan_system_schema_registry<'a>(
        &mut self,
        registry: &'a SystemSchemaRegistry,
    ) -> Result<SystemSchemaPlan<'a>> {
        validate_system_schema_registry(registry)?;
        let registry_table_present = self.system_schema_registry_table_present()?;

        if registry_table_present {
            validate_engine_system_schema_registry_table(
                self.runtime.get()?.store.relational_state(),
            )?;
        } else if registry.owner() != ENGINE_SYSTEM_SCHEMA_OWNER {
            return Err(HawDBError::Storage(
                "system schema registry table is missing after engine bootstrap".to_string(),
            ));
        }

        let applied = if registry_table_present {
            let max_rows = registry.migrations().len().saturating_add(1);
            if self
                .runtime
                .get()?
                .store
                .relational_state()
                .materialized_rows_resident()
            {
                read_applied_system_schema_migrations(
                    self.runtime.get()?.store.relational_state(),
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
            .collect();
        Ok(SystemSchemaPlan {
            registry,
            previous_version,
            pending,
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
        is_engine_system_schema_bootstrap(self.runtime.get()?.store.relational_state())
    }

    pub(super) fn validate_hawdb_lightning_system_schema(state: &RelationalState) -> Result<()> {
        validate_engine_system_schema_state(state)
    }

    pub(super) fn hawdb_lightning_relational_state(&self) -> Result<RelationalState> {
        state_with_engine_system_schema(self.runtime.get()?.store.relational_state())
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
