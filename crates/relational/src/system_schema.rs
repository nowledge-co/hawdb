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

use std::collections::BTreeMap;

use hawdb_core::{HawDBError, Result, Value};
use hawdb_executor::QueryRowRef;
use hawdb_integrity::IntegrityHasher;
use hawdb_sql::SqlStatement;
use hawdb_storage::relational::{
    RelationalMutationLimits, RelationalOverflowConfig, RelationalState, RelationalTableSchema,
    RelationalTransaction, RelationalValue, RelationalWrite,
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

/// The migration records of every owner, decoded from full registry rows that
/// come from outside the database, such as a logical dump.
///
/// Such rows are data, not authority: construction checks that they have a
/// shape only a registry upgrade writes, and
/// [`SystemSchemaLedger::validate_engine`] and
/// [`SystemSchemaLedger::validate_registries`] apply the checksum rules an
/// upgrade applies to records it finds in a database.
#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SystemSchemaLedger {
    owners: BTreeMap<String, Vec<AppliedSystemSchemaMigration>>,
}

impl SystemSchemaLedger {
    /// Decodes rows in the registry table's column order. Every owner's
    /// versions must be contiguous from 1 and each record must be spelled
    /// exactly as a registry upgrade writes it.
    pub fn from_rows<'a>(rows: impl IntoIterator<Item = &'a [Value]>) -> Result<Self> {
        let mut owners = BTreeMap::<String, BTreeMap<u64, AppliedSystemSchemaMigration>>::new();
        for row in rows {
            let [Value::String(id), Value::String(owner), Value::Int(version), Value::String(name), Value::String(checksum)] =
                row
            else {
                return Err(invalid_ledger(
                    "a record does not have the columns of a migration record",
                ));
            };
            let record = u64::try_from(*version)
                .ok()
                .filter(|version| *version > 0)
                .filter(|version| *id == system_schema_migration_id(owner, *version))
                .filter(|_| valid_system_schema_owner(owner))
                .filter(|_| valid_system_schema_migration_name(name))
                .filter(|_| {
                    checksum.len() == 64
                        && checksum
                            .bytes()
                            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
                })
                .map(|version| AppliedSystemSchemaMigration {
                    version,
                    name: name.clone(),
                    checksum: checksum.clone(),
                })
                .ok_or_else(|| {
                    invalid_ledger(format!(
                        "record {id:?} is not one a system schema upgrade writes"
                    ))
                })?;
            if owners
                .entry(owner.clone())
                .or_default()
                .insert(record.version, record)
                .is_some()
            {
                return Err(invalid_ledger(format!("record {id:?} appears twice")));
            }
        }
        let owners = owners
            .into_iter()
            .map(|(owner, records)| {
                // Versions are unique and positive, so the records are the
                // contiguous prefix exactly when the last version is the count.
                if records.keys().next_back().copied() != u64::try_from(records.len()).ok() {
                    return Err(invalid_ledger(format!(
                        "versions of owner {owner} are not contiguous from 1"
                    )));
                }
                Ok((owner, records.into_values().collect()))
            })
            .collect::<Result<_>>()?;
        Ok(Self { owners })
    }

    fn applied(&self, owner: &str) -> &[AppliedSystemSchemaMigration] {
        self.owners.get(owner).map_or(&[], Vec::as_slice)
    }

    /// Records of embedding applications, by owner and ascending version.
    /// The engine's own records are excluded: only a running engine writes
    /// those.
    pub fn application_owners(
        &self,
    ) -> impl Iterator<Item = (&str, &[AppliedSystemSchemaMigration])> {
        self.owners
            .iter()
            .filter(|(owner, _)| owner.as_str() != ENGINE_SYSTEM_SCHEMA_OWNER)
            .map(|(owner, records)| (owner.as_str(), records.as_slice()))
    }

    /// Requires the engine's records to be a non-empty prefix of this build's
    /// engine registry.
    pub fn validate_engine(&self) -> Result<()> {
        validate_applied_system_schema_migrations(
            &engine_system_schema_registry(),
            self.applied(ENGINE_SYSTEM_SCHEMA_OWNER),
            true,
        )
    }

    /// Requires every application owner with records to be one of
    /// `registries` and its records to be a prefix of that registry. An owner
    /// with fewer records than its registry has pending migrations.
    pub fn validate_registries(&self, registries: &[SystemSchemaRegistry]) -> Result<()> {
        for (owner, applied) in self.application_owners() {
            let registry = registries
                .iter()
                .find(|registry| registry.owner() == owner)
                .ok_or_else(|| {
                    HawDBError::Storage(format!(
                        "system schema {owner} has migration records but no registry was supplied"
                    ))
                })?;
            validate_applied_system_schema_migrations(registry, applied, true)?;
        }
        Ok(())
    }
}

fn invalid_ledger(detail: impl std::fmt::Display) -> HawDBError {
    HawDBError::StorageIntegrity(format!("system schema migration records: {detail}"))
}

/// Primary key of one migration record.
#[doc(hidden)]
pub fn system_schema_migration_id(owner: &str, version: u64) -> String {
    format!("{owner}:{version}")
}

fn valid_system_schema_owner(owner: &str) -> bool {
    (1..=128).contains(&owner.len())
        && owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn valid_system_schema_migration_name(name: &str) -> bool {
    (1..=128).contains(&name.len())
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
    if !valid_system_schema_owner(&registry.owner) {
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
        if !valid_system_schema_migration_name(&migration.name) {
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
        SqlStatement::CreateMaterializedView(statement) => Some(&statement.name),
        SqlStatement::DropMaterializedView(statement) => Some(&statement.name),
        SqlStatement::CreateSearchIndex(statement) => {
            if statement_writes_system_schema_registry(&SqlStatement::CreateIndex(
                hawdb_sql::CreateIndexStatement {
                    name: String::new(),
                    table: statement.table.clone(),
                    columns: Vec::new(),
                    included_columns: Vec::new(),
                    unique: false,
                    if_not_exists: false,
                    predicate: None,
                },
            )) {
                return true;
            }
            Some(&statement.name)
        }
        SqlStatement::DropSearchIndex(statement) => Some(&statement.name),
        SqlStatement::Select(_)
        | SqlStatement::Query(_)
        | SqlStatement::Explain(_)
        | SqlStatement::Branch(_) => None,
    };
    table.is_some_and(|table| {
        table.name == ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE
            && table
                .schema
                .as_deref()
                .is_none_or(|schema| schema == "public")
    })
}

/// The registry table as this build defines it.
#[doc(hidden)]
pub fn engine_system_schema_registry_table() -> Result<RelationalTableSchema> {
    let compiled = crate::compile_relational_statement_sql(
        ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE_DDL,
        &[],
        &RelationalState::default(),
    )?;
    let Some(RelationalWrite::CreateTable(table)) = compiled.writes.into_iter().next() else {
        return Err(HawDBError::Execution(
            "system schema registry DDL did not compile to CREATE TABLE".to_string(),
        ));
    };
    Ok(table)
}

/// Validates the durable shape of HawDB's own schema migration registry.
#[doc(hidden)]
pub fn validate_engine_system_schema_registry_table(state: &RelationalState) -> Result<()> {
    let expected = engine_system_schema_registry_table()?;
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
    row: &hawdb_storage::relational::RelationalRow,
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
    use hawdb_storage::relational::RelationalState;

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

    #[test]
    fn external_ledger_rows_must_be_ones_a_registry_upgrade_writes() {
        use super::{system_schema_migration_id, SystemSchemaLedger};
        use hawdb_core::Value;

        let registry = SystemSchemaRegistry::new(
            "example.content",
            (1..=10).map(|version| {
                SystemSchemaMigration::new(version, "step", ["CREATE TABLE t (id TEXT)"])
            }),
        );
        let row = |owner: &str, migration: &SystemSchemaMigration| {
            vec![
                Value::String(system_schema_migration_id(owner, migration.version())),
                Value::String(owner.to_string()),
                Value::Int(migration.version() as i64),
                Value::String(migration.name().to_string()),
                Value::String(migration.checksum(owner)),
            ]
        };
        let ledger =
            |rows: &[Vec<Value>]| SystemSchemaLedger::from_rows(rows.iter().map(|row| &row[..]));
        let engine = engine_system_schema_registry();
        let engine_row = row(ENGINE_SYSTEM_SCHEMA_OWNER, &engine.migrations()[0]);
        // Any row order, such as the key order in which :10 precedes :2.
        let mut rows = registry
            .migrations()
            .iter()
            .rev()
            .map(|migration| row(registry.owner(), migration))
            .collect::<Vec<_>>();
        rows.push(engine_row.clone());
        let decoded = ledger(&rows).unwrap();
        decoded.validate_engine().unwrap();
        decoded
            .validate_registries(std::slice::from_ref(&registry))
            .unwrap();
        // The engine's own records are never offered for restoring.
        let owners = decoded.application_owners().collect::<Vec<_>>();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].0, "example.content");
        assert_eq!(
            owners[0]
                .1
                .iter()
                .map(|record| record.version())
                .collect::<Vec<_>>(),
            (1..=10).collect::<Vec<_>>()
        );
        assert!(decoded.validate_registries(&[]).is_err());
        let behind =
            SystemSchemaRegistry::new(registry.owner(), registry.migrations()[..9].iter().cloned());
        assert!(decoded.validate_registries(&[behind]).is_err());

        assert!(ledger(&[]).unwrap().validate_engine().is_err());
        assert!(ledger(&rows[1..])
            .unwrap()
            .validate_registries(&[registry])
            .is_ok());
        let mut duplicated = rows.clone();
        duplicated.push(engine_row);
        assert!(ledger(&duplicated).is_err());
        let mut gap = rows.clone();
        gap.remove(4);
        assert!(ledger(&gap).is_err());
        let mut short = rows;
        short[0].pop();
        assert!(ledger(&short).is_err());
    }
}
