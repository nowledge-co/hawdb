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

use hawdb_core::{HawDBError, Result, Value};
use hawdb_executor::QueryRowRef;
use hawdb_integrity::IntegrityHasher;
use hawdb_sql::SqlStatement;
use hawdb_storage::{
    RelationalMutationLimits, RelationalOverflowConfig, RelationalState, RelationalTransaction,
    RelationalValue, RelationalWrite,
};

#[doc(hidden)]
pub const ENGINE_SYSTEM_SCHEMA_OWNER: &str = "hawdb.engine";
#[doc(hidden)]
pub const ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE: &str = "hawdb_schema_migrations";
#[doc(hidden)]
pub const ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE_DDL: &str = "CREATE TABLE hawdb_schema_migrations (\
    migration_id TEXT PRIMARY KEY, \
    owner TEXT NOT NULL, \
    version BIGINT NOT NULL, \
    name TEXT NOT NULL, \
    checksum TEXT NOT NULL, \
    UNIQUE (owner, version))";
#[doc(hidden)]
pub const ENGINE_SYSTEM_SCHEMA_REGISTRY_INSERT_SQL: &str = "INSERT INTO hawdb_schema_migrations \
    (migration_id, owner, version, name, checksum) VALUES ($1, $2, $3, $4, $5)";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemSchemaMigration {
    version: u64,
    name: String,
    statements: Vec<String>,
}

impl SystemSchemaMigration {
    pub fn new(
        version: u64,
        name: impl Into<String>,
        statements: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            version,
            name: name.into(),
            statements: statements.into_iter().map(Into::into).collect(),
        }
    }

    pub const fn version(&self) -> u64 {
        self.version
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn statements(&self) -> &[String] {
        &self.statements
    }

    pub fn checksum(&self, owner: &str) -> String {
        let mut hasher = IntegrityHasher::new();
        hash_component(&mut hasher, b"hawdb-system-schema-migration-v1");
        hash_component(&mut hasher, owner.as_bytes());
        hash_component(&mut hasher, &self.version.to_le_bytes());
        hash_component(&mut hasher, self.name.as_bytes());
        for statement in &self.statements {
            hash_component(&mut hasher, statement.as_bytes());
        }
        hasher.finish().sha256.to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemSchemaRegistry {
    owner: String,
    migrations: Vec<SystemSchemaMigration>,
}

impl SystemSchemaRegistry {
    pub fn new(
        owner: impl Into<String>,
        migrations: impl IntoIterator<Item = SystemSchemaMigration>,
    ) -> Self {
        Self {
            owner: owner.into(),
            migrations: migrations.into_iter().collect(),
        }
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn migrations(&self) -> &[SystemSchemaMigration] {
        &self.migrations
    }

    pub fn current_version(&self) -> u64 {
        self.migrations
            .last()
            .map_or(0, SystemSchemaMigration::version)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemSchemaUpgradeReport {
    pub owner: String,
    pub previous_version: u64,
    pub current_version: u64,
    pub applied_versions: Vec<u64>,
    pub commit_epoch_before: u64,
    pub commit_epoch_after: u64,
}

/// A durable migration record decoded from the engine registry table.
///
/// The embedded facade owns query execution; relational owns the registry
/// row format and validates it before a migration can advance.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedSystemSchemaMigration {
    version: u64,
    name: String,
    checksum: String,
}

impl AppliedSystemSchemaMigration {
    pub const fn version(&self) -> u64 {
        self.version
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn checksum(&self) -> &str {
        &self.checksum
    }
}

/// Returns the append-only registry reserved for HawDB's own relational
/// bootstrap. Hosts may use separate [`SystemSchemaRegistry`] owners.
#[doc(hidden)]
pub fn engine_system_schema_registry() -> SystemSchemaRegistry {
    SystemSchemaRegistry::new(
        ENGINE_SYSTEM_SCHEMA_OWNER,
        [SystemSchemaMigration::new(
            1,
            "create_schema_registry",
            [ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE_DDL],
        )],
    )
}

#[doc(hidden)]
pub fn validate_system_schema_registry(registry: &SystemSchemaRegistry) -> Result<()> {
    if registry.owner.is_empty()
        || registry.owner.len() > 128
        || !registry
            .owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(HawDBError::Semantic(
            "system schema owner must contain 1-128 ASCII letters, digits, '.', '_' or '-'"
                .to_string(),
        ));
    }
    if registry.migrations.is_empty() {
        return Err(HawDBError::Semantic(format!(
            "system schema {} requires at least one migration",
            registry.owner
        )));
    }
    for (index, migration) in registry.migrations.iter().enumerate() {
        let expected = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
        if migration.version != expected {
            return Err(HawDBError::Semantic(format!(
                "system schema {} migrations must be contiguous from version 1; expected {}, got {}",
                registry.owner, expected, migration.version
            )));
        }
        if migration.name.is_empty() || migration.name.len() > 128 {
            return Err(HawDBError::Semantic(format!(
                "system schema {} migration {} requires a 1-128 byte name",
                registry.owner, migration.version
            )));
        }
        if migration.statements.is_empty()
            || migration
                .statements
                .iter()
                .any(|statement| statement.trim().is_empty())
        {
            return Err(HawDBError::Semantic(format!(
                "system schema {} migration {} requires non-empty SQL statements",
                registry.owner, migration.version
            )));
        }
        i64::try_from(migration.version).map_err(|_| {
            HawDBError::Semantic(format!(
                "system schema {} migration version {} exceeds BIGINT",
                registry.owner, migration.version
            ))
        })?;
    }
    Ok(())
}

/// Identifies statements that may mutate HawDB's internal migration registry.
///
/// The embedded facade owns authorization and transaction handling; the
/// relational owner defines which SQL AST shapes target the registry.
#[doc(hidden)]
pub fn statement_writes_system_schema_registry(statement: &SqlStatement) -> bool {
    let table = match statement {
        SqlStatement::Insert(statement) => Some(&statement.table),
        SqlStatement::Update(statement) => Some(&statement.table),
        SqlStatement::Delete(statement) => Some(&statement.table),
        SqlStatement::CreateTable(statement) => Some(&statement.table),
        SqlStatement::CreateIndex(statement) => Some(&statement.table),
        SqlStatement::AlterTableAddColumn(statement) => Some(&statement.table),
        SqlStatement::Select(_) | SqlStatement::Explain(_) => None,
    };
    table.is_some_and(|table| {
        table.name == ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE
            && table
                .schema
                .as_deref()
                .is_none_or(|schema| schema == "public")
    })
}

/// Validates the durable shape of HawDB's own schema migration registry.
#[doc(hidden)]
pub fn validate_engine_system_schema_registry_table(state: &RelationalState) -> Result<()> {
    let expected = crate::compile_relational_statement_sql(
        ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE_DDL,
        &[],
        &RelationalState::default(),
    )?;
    let Some(RelationalWrite::CreateTable(expected)) = expected.writes.into_iter().next() else {
        return Err(HawDBError::Execution(
            "system schema registry DDL did not compile to CREATE TABLE".to_string(),
        ));
    };
    let actual = state
        .table_schema(ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE)
        .ok_or_else(|| {
            HawDBError::Storage("system schema registry table disappeared during open".to_string())
        })?;
    if actual != &expected {
        return Err(HawDBError::Storage(
            "system schema registry table does not match the engine definition".to_string(),
        ));
    }
    Ok(())
}

/// Decodes bounded migration records from canonical relational rows.
#[doc(hidden)]
pub fn read_applied_system_schema_migrations(
    state: &RelationalState,
    owner: &str,
    max_rows: usize,
) -> Result<Vec<AppliedSystemSchemaMigration>> {
    let mut applied = state
        .rows(ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE)
        .filter(|(_, row)| {
            matches!(row.values().get(1), Some(RelationalValue::Text(value)) if value == owner)
        })
        .take(max_rows)
        .map(|(_, row)| decode_applied_system_schema_migration_row(owner, row))
        .collect::<Result<Vec<_>>>()?;
    applied.sort_unstable_by_key(AppliedSystemSchemaMigration::version);
    Ok(applied)
}

/// Decodes one migration record returned through a bounded facade query.
#[doc(hidden)]
pub fn decode_applied_system_schema_migration_query_row(
    owner: &str,
    row: QueryRowRef<'_>,
) -> Result<AppliedSystemSchemaMigration> {
    let version = match row.get("version") {
        Some(Value::Int(version)) => u64::try_from(*version).map_err(|_| {
            HawDBError::Storage(format!(
                "system schema {owner} contains a negative migration version"
            ))
        })?,
        _ => {
            return Err(HawDBError::Storage(format!(
                "system schema {owner} contains an invalid migration version"
            )))
        }
    };
    let name = match row.get("name") {
        Some(Value::String(name)) => name.clone(),
        _ => {
            return Err(HawDBError::Storage(format!(
                "system schema {owner} contains an invalid migration name"
            )))
        }
    };
    let checksum = match row.get("checksum") {
        Some(Value::String(checksum)) => checksum.clone(),
        _ => {
            return Err(HawDBError::Storage(format!(
                "system schema {owner} contains an invalid migration checksum"
            )))
        }
    };
    Ok(AppliedSystemSchemaMigration {
        version,
        name,
        checksum,
    })
}

/// Validates the applied prefix before a system-schema write can begin.
#[doc(hidden)]
pub fn validate_applied_system_schema_migrations(
    registry: &SystemSchemaRegistry,
    applied: &[AppliedSystemSchemaMigration],
    registry_table_present: bool,
) -> Result<()> {
    if registry.owner() == ENGINE_SYSTEM_SCHEMA_OWNER
        && registry_table_present
        && applied.is_empty()
    {
        return Err(HawDBError::Storage(
            "system schema registry exists without its engine migration record".to_string(),
        ));
    }
    if applied.len() > registry.migrations().len() {
        return Err(HawDBError::Storage(format!(
            "system schema {} is at future version {}, binary supports {}",
            registry.owner(),
            applied
                .last()
                .map_or(0, AppliedSystemSchemaMigration::version),
            registry.current_version()
        )));
    }
    for (index, actual) in applied.iter().enumerate() {
        let expected = &registry.migrations()[index];
        let expected_checksum = expected.checksum(registry.owner());
        if actual.version != expected.version()
            || actual.name != expected.name()
            || actual.checksum != expected_checksum
        {
            return Err(HawDBError::Storage(format!(
                "system schema {} migration {} checksum or identity drifted",
                registry.owner(),
                actual.version
            )));
        }
    }
    Ok(())
}

/// Validates the state expected by a HawDB Lightning relational export.
#[doc(hidden)]
pub fn validate_engine_system_schema_state(state: &RelationalState) -> Result<()> {
    validate_engine_system_schema_registry_table(state)?;
    let registry = engine_system_schema_registry();
    let applied = read_applied_system_schema_migrations(state, ENGINE_SYSTEM_SCHEMA_OWNER, 2)?;
    validate_applied_system_schema_migrations(&registry, &applied, true)
}

/// Creates the HawDB registry in a materialized relational snapshot when it
/// is absent, or validates the exact existing registry when it is present.
#[doc(hidden)]
pub fn state_with_engine_system_schema(state: &RelationalState) -> Result<RelationalState> {
    state
        .require_materialized_rows("HawDB Lightning relational export")
        .map_err(|error| HawDBError::Storage(error.to_string()))?;
    if state
        .table_schema(ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE)
        .is_some()
    {
        validate_engine_system_schema_state(state)?;
        return Ok(state.clone());
    }

    let registry = engine_system_schema_registry();
    let migration = &registry.migrations()[0];
    let mut transaction = crate::compile_relational_statement_sql(
        ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE_DDL,
        &[],
        state,
    )?;
    let staged = state
        .stage_transaction(
            transaction.clone(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .map_err(|error| HawDBError::Storage(error.to_string()))?;
    let insert = crate::compile_relational_statement_sql(
        ENGINE_SYSTEM_SCHEMA_REGISTRY_INSERT_SQL,
        &[
            Value::String(format!(
                "{ENGINE_SYSTEM_SCHEMA_OWNER}:{}",
                migration.version()
            )),
            Value::String(ENGINE_SYSTEM_SCHEMA_OWNER.to_string()),
            Value::Int(i64::try_from(migration.version()).expect("engine migration fits BIGINT")),
            Value::String(migration.name().to_string()),
            Value::String(migration.checksum(ENGINE_SYSTEM_SCHEMA_OWNER)),
        ],
        &staged,
    )?;
    transaction.writes.extend(insert.writes);
    state
        .stage_transaction(
            RelationalTransaction {
                writes: transaction.writes,
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .map_err(|error| HawDBError::Storage(error.to_string()))
}

/// Returns whether the state consists only of the engine registry bootstrap.
#[doc(hidden)]
pub fn is_engine_system_schema_bootstrap(state: &RelationalState) -> Result<bool> {
    if state.table_schemas().count() != 1
        || state.row_count(ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE) != 1
        || state.total_row_count() != 1
        || state.overflow_segment_count() != 0
    {
        return Ok(false);
    }
    validate_engine_system_schema_state(state)?;
    let applied = read_applied_system_schema_migrations(state, ENGINE_SYSTEM_SCHEMA_OWNER, 2)?;
    Ok(applied.len() == 1)
}

fn decode_applied_system_schema_migration_row(
    owner: &str,
    row: &hawdb_storage::RelationalRow,
) -> Result<AppliedSystemSchemaMigration> {
    let version = match row.values().get(2) {
        Some(RelationalValue::BigInt(version)) => u64::try_from(*version).map_err(|_| {
            HawDBError::Storage(format!(
                "system schema {owner} contains a negative migration version"
            ))
        })?,
        _ => {
            return Err(HawDBError::Storage(format!(
                "system schema {owner} contains an invalid migration version"
            )))
        }
    };
    let name = match row.values().get(3) {
        Some(RelationalValue::Text(name)) => name.clone(),
        _ => {
            return Err(HawDBError::Storage(format!(
                "system schema {owner} contains an invalid migration name"
            )))
        }
    };
    let checksum = match row.values().get(4) {
        Some(RelationalValue::Text(checksum)) => checksum.clone(),
        _ => {
            return Err(HawDBError::Storage(format!(
                "system schema {owner} contains an invalid migration checksum"
            )))
        }
    };
    Ok(AppliedSystemSchemaMigration {
        version,
        name,
        checksum,
    })
}

fn hash_component(hasher: &mut IntegrityHasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::{
        engine_system_schema_registry, is_engine_system_schema_bootstrap,
        read_applied_system_schema_migrations, state_with_engine_system_schema,
        statement_writes_system_schema_registry, validate_engine_system_schema_state,
        validate_system_schema_registry, SystemSchemaMigration, SystemSchemaRegistry,
        ENGINE_SYSTEM_SCHEMA_OWNER,
    };
    use hawdb_storage::RelationalState;

    #[test]
    fn migration_checksum_binds_owner_order_and_statement_boundaries() {
        let migration = SystemSchemaMigration::new(
            1,
            "create_documents",
            [
                "CREATE TABLE documents (id TEXT PRIMARY KEY)",
                "CREATE INDEX documents_id",
            ],
        );
        let reordered = SystemSchemaMigration::new(
            1,
            "create_documents",
            [
                "CREATE INDEX documents_id",
                "CREATE TABLE documents (id TEXT PRIMARY KEY)",
            ],
        );

        assert_ne!(
            migration.checksum("nowledge.content"),
            migration.checksum("hawdb.engine")
        );
        assert_ne!(
            migration.checksum("nowledge.content"),
            reordered.checksum("nowledge.content")
        );
    }

    #[test]
    fn registry_validation_requires_a_safe_contiguous_non_empty_contract() {
        let valid = SystemSchemaRegistry::new(
            "nowledge.content",
            [
                SystemSchemaMigration::new(
                    1,
                    "create_documents",
                    ["CREATE TABLE documents (id TEXT)"],
                ),
                SystemSchemaMigration::new(
                    2,
                    "add_title",
                    ["ALTER TABLE documents ADD COLUMN title TEXT"],
                ),
            ],
        );
        assert!(validate_system_schema_registry(&valid).is_ok());

        let invalid_registries = [
            SystemSchemaRegistry::new(
                "invalid owner",
                [SystemSchemaMigration::new(
                    1,
                    "create_documents",
                    ["CREATE TABLE documents (id TEXT)"],
                )],
            ),
            SystemSchemaRegistry::new(
                "nowledge.content",
                [SystemSchemaMigration::new(
                    2,
                    "create_documents",
                    ["CREATE TABLE documents (id TEXT)"],
                )],
            ),
            SystemSchemaRegistry::new(
                "nowledge.content",
                [SystemSchemaMigration::new(1, "create_documents", ["  "])],
            ),
        ];
        for registry in invalid_registries {
            assert!(validate_system_schema_registry(&registry).is_err());
        }
    }

    #[test]
    fn registry_write_guard_is_limited_to_public_mutations() {
        let insert = hawdb_sql::parse_postgres_sql(
            "INSERT INTO hawdb_schema_migrations (version, name, checksum) VALUES (1, 'init', 'x')",
        )
        .expect("parse registry insert");
        let public_create = hawdb_sql::parse_postgres_sql(
            "CREATE TABLE public.hawdb_schema_migrations (version BIGINT)",
        )
        .expect("parse public registry create");
        let other_schema = hawdb_sql::parse_postgres_sql(
            "INSERT INTO archive.hawdb_schema_migrations (version) VALUES (1)",
        )
        .expect("parse other-schema insert");
        let read = hawdb_sql::parse_postgres_sql("SELECT version FROM hawdb_schema_migrations")
            .expect("parse registry read");

        assert!(statement_writes_system_schema_registry(&insert));
        assert!(statement_writes_system_schema_registry(&public_create));
        assert!(!statement_writes_system_schema_registry(&other_schema));
        assert!(!statement_writes_system_schema_registry(&read));
    }

    #[test]
    fn engine_bootstrap_registry_is_materialized_validated_and_idempotent() {
        let bootstrapped = state_with_engine_system_schema(&RelationalState::default()).unwrap();

        validate_engine_system_schema_state(&bootstrapped).unwrap();
        assert!(is_engine_system_schema_bootstrap(&bootstrapped).unwrap());
        let registry = engine_system_schema_registry();
        let applied =
            read_applied_system_schema_migrations(&bootstrapped, ENGINE_SYSTEM_SCHEMA_OWNER, 2)
                .unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].version(), 1);
        assert_eq!(applied[0].name(), "create_schema_registry");
        assert_eq!(
            applied[0].checksum(),
            registry.migrations()[0].checksum(ENGINE_SYSTEM_SCHEMA_OWNER)
        );

        let repeated = state_with_engine_system_schema(&bootstrapped).unwrap();
        validate_engine_system_schema_state(&repeated).unwrap();
        assert!(is_engine_system_schema_bootstrap(&repeated).unwrap());
        assert_eq!(
            read_applied_system_schema_migrations(&repeated, ENGINE_SYSTEM_SCHEMA_OWNER, 2)
                .unwrap(),
            applied
        );
    }
}
