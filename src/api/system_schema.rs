use super::Database;
use crate::error::{Result, SkeinError};
use crate::value::Value;
use skein_integrity::IntegrityHasher;
use skein_storage::{
    RelationalMutationLimits, RelationalOverflowConfig, RelationalState, RelationalTransaction,
    RelationalValue, RelationalWrite,
};

const REGISTRY_OWNER: &str = "skein.engine";
const REGISTRY_TABLE: &str = "skein_schema_migrations";
const REGISTRY_TABLE_DDL: &str = "CREATE TABLE skein_schema_migrations (\
    migration_id TEXT PRIMARY KEY, \
    owner TEXT NOT NULL, \
    version BIGINT NOT NULL, \
    name TEXT NOT NULL, \
    checksum TEXT NOT NULL, \
    UNIQUE (owner, version))";
const REGISTRY_INSERT_SQL: &str = "INSERT INTO skein_schema_migrations \
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

    pub fn version(&self) -> u64 {
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
        hash_component(&mut hasher, b"skein-system-schema-migration-v1");
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppliedMigration {
    version: u64,
    name: String,
    checksum: String,
}

impl Database {
    pub(super) fn apply_engine_system_schema(&mut self) -> Result<SystemSchemaUpgradeReport> {
        self.apply_system_schema_registry_inner(&engine_registry())
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
        validate_registry(registry)?;
        if registry.owner == REGISTRY_OWNER && registry != &engine_registry() {
            return Err(SkeinError::Semantic(format!(
                "system schema owner {REGISTRY_OWNER} is reserved by the engine"
            )));
        }
        if registry.owner != REGISTRY_OWNER {
            self.apply_system_schema_registry_inner(&engine_registry())?;
        }
        self.apply_system_schema_registry_inner(registry)
    }

    fn apply_system_schema_registry_inner(
        &mut self,
        registry: &SystemSchemaRegistry,
    ) -> Result<SystemSchemaUpgradeReport> {
        validate_registry(registry)?;
        let commit_epoch_before = self.commit_epoch();
        let registry_table_present = self
            .store
            .relational_state()
            .table_schema(REGISTRY_TABLE)
            .is_some();

        if registry_table_present {
            validate_registry_table(self.store.relational_state())?;
        } else if registry.owner != REGISTRY_OWNER {
            return Err(SkeinError::Storage(
                "system schema registry table is missing after engine bootstrap".to_string(),
            ));
        }

        let applied = if registry_table_present {
            read_applied_migrations(
                self.store.relational_state(),
                &registry.owner,
                registry.migrations.len().saturating_add(1),
            )?
        } else {
            Vec::new()
        };
        validate_applied_migrations(registry, &applied, registry_table_present)?;

        let previous_version = applied.last().map_or(0, |migration| migration.version);
        let pending = registry
            .migrations
            .iter()
            .filter(|migration| migration.version > previous_version)
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(SystemSchemaUpgradeReport {
                owner: registry.owner.clone(),
                previous_version,
                current_version: previous_version,
                applied_versions: Vec::new(),
                commit_epoch_before,
                commit_epoch_after: commit_epoch_before,
            });
        }
        if self.config.read_only {
            return Err(SkeinError::Execution(format!(
                "system schema {} requires upgrade from version {} to {} but the database is read-only",
                registry.owner,
                previous_version,
                registry.current_version()
            )));
        }

        let mut transaction = self.begin_transaction();
        let mut applied_versions = Vec::with_capacity(pending.len());
        for migration in pending {
            for statement in &migration.statements {
                if registry.owner == REGISTRY_OWNER {
                    transaction.query_system_schema_sql(statement)?;
                } else {
                    transaction.query_sql(statement)?;
                }
            }
            let version = i64::try_from(migration.version).map_err(|_| {
                SkeinError::Semantic(format!(
                    "system schema {} migration version {} exceeds BIGINT",
                    registry.owner, migration.version
                ))
            })?;
            transaction.query_system_schema_sql_with_params(
                REGISTRY_INSERT_SQL,
                &[
                    Value::String(format!("{}:{}", registry.owner, migration.version)),
                    Value::String(registry.owner.clone()),
                    Value::Int(version),
                    Value::String(migration.name.clone()),
                    Value::String(migration.checksum(&registry.owner)),
                ],
            )?;
            applied_versions.push(migration.version);
        }
        transaction.commit()?;

        Ok(SystemSchemaUpgradeReport {
            owner: registry.owner.clone(),
            previous_version,
            current_version: registry.current_version(),
            applied_versions,
            commit_epoch_before,
            commit_epoch_after: self.commit_epoch(),
        })
    }

    pub(super) fn has_only_engine_system_schema_bootstrap(&self) -> Result<bool> {
        let state = self.store.relational_state();
        if state.table_schemas().count() != 1
            || state.row_count(REGISTRY_TABLE) != 1
            || state.total_row_count() != 1
            || state.overflow_segment_count() != 0
        {
            return Ok(false);
        }
        validate_engine_system_schema_state(state)?;
        let applied = read_applied_migrations(state, REGISTRY_OWNER, 2)?;
        Ok(applied.len() == 1)
    }

    pub(super) fn validate_skein_lightning_system_schema(state: &RelationalState) -> Result<()> {
        validate_engine_system_schema_state(state)
    }

    pub(super) fn skein_lightning_relational_state(&self) -> Result<RelationalState> {
        state_with_engine_system_schema(self.store.relational_state())
    }
}

fn engine_registry() -> SystemSchemaRegistry {
    SystemSchemaRegistry::new(
        REGISTRY_OWNER,
        [SystemSchemaMigration::new(
            1,
            "create_schema_registry",
            [REGISTRY_TABLE_DDL],
        )],
    )
}

fn validate_registry(registry: &SystemSchemaRegistry) -> Result<()> {
    if registry.owner.is_empty()
        || registry.owner.len() > 128
        || !registry
            .owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(SkeinError::Semantic(
            "system schema owner must contain 1-128 ASCII letters, digits, '.', '_' or '-'"
                .to_string(),
        ));
    }
    if registry.migrations.is_empty() {
        return Err(SkeinError::Semantic(format!(
            "system schema {} requires at least one migration",
            registry.owner
        )));
    }
    for (index, migration) in registry.migrations.iter().enumerate() {
        let expected = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
        if migration.version != expected {
            return Err(SkeinError::Semantic(format!(
                "system schema {} migrations must be contiguous from version 1; expected {}, got {}",
                registry.owner, expected, migration.version
            )));
        }
        if migration.name.is_empty() || migration.name.len() > 128 {
            return Err(SkeinError::Semantic(format!(
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
            return Err(SkeinError::Semantic(format!(
                "system schema {} migration {} requires non-empty SQL statements",
                registry.owner, migration.version
            )));
        }
        i64::try_from(migration.version).map_err(|_| {
            SkeinError::Semantic(format!(
                "system schema {} migration version {} exceeds BIGINT",
                registry.owner, migration.version
            ))
        })?;
    }
    Ok(())
}

fn validate_registry_table(state: &RelationalState) -> Result<()> {
    let expected = crate::relational_sql::compile_relational_statement_sql(
        REGISTRY_TABLE_DDL,
        &[],
        &RelationalState::default(),
    )?;
    let Some(RelationalWrite::CreateTable(expected)) = expected.writes.into_iter().next() else {
        return Err(SkeinError::Execution(
            "system schema registry DDL did not compile to CREATE TABLE".to_string(),
        ));
    };
    let actual = state.table_schema(REGISTRY_TABLE).ok_or_else(|| {
        SkeinError::Storage("system schema registry table disappeared during open".to_string())
    })?;
    if actual != &expected {
        return Err(SkeinError::Storage(
            "system schema registry table does not match the engine definition".to_string(),
        ));
    }
    Ok(())
}

fn validate_engine_system_schema_state(state: &RelationalState) -> Result<()> {
    validate_registry_table(state)?;
    let registry = engine_registry();
    let applied = read_applied_migrations(state, REGISTRY_OWNER, 2)?;
    validate_applied_migrations(&registry, &applied, true)
}

fn state_with_engine_system_schema(state: &RelationalState) -> Result<RelationalState> {
    if state.table_schema(REGISTRY_TABLE).is_some() {
        validate_engine_system_schema_state(state)?;
        return Ok(state.clone());
    }

    let registry = engine_registry();
    let migration = &registry.migrations[0];
    let mut transaction =
        crate::relational_sql::compile_relational_statement_sql(REGISTRY_TABLE_DDL, &[], state)?;
    let staged = state
        .stage_transaction(
            transaction.clone(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
    let insert = crate::relational_sql::compile_relational_statement_sql(
        REGISTRY_INSERT_SQL,
        &[
            Value::String(format!("{REGISTRY_OWNER}:{}", migration.version)),
            Value::String(REGISTRY_OWNER.to_string()),
            Value::Int(i64::try_from(migration.version).expect("engine migration fits BIGINT")),
            Value::String(migration.name.clone()),
            Value::String(migration.checksum(REGISTRY_OWNER)),
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
        .map_err(|error| SkeinError::Storage(error.to_string()))
}

fn read_applied_migrations(
    state: &RelationalState,
    owner: &str,
    max_rows: usize,
) -> Result<Vec<AppliedMigration>> {
    let mut applied = state
        .rows(REGISTRY_TABLE)
        .filter(|(_, row)| {
            matches!(row.values().get(1), Some(RelationalValue::Text(value)) if value == owner)
        })
        .take(max_rows)
        .map(|(_, row)| applied_migration_from_row(owner, row))
        .collect::<Result<Vec<_>>>()?;
    applied.sort_unstable_by_key(|migration| migration.version);
    Ok(applied)
}

fn applied_migration_from_row(
    owner: &str,
    row: &skein_storage::RelationalRow,
) -> Result<AppliedMigration> {
    let version = match row.values().get(2) {
        Some(RelationalValue::BigInt(version)) => u64::try_from(*version).map_err(|_| {
            SkeinError::Storage(format!(
                "system schema {owner} contains a negative migration version"
            ))
        })?,
        _ => {
            return Err(SkeinError::Storage(format!(
                "system schema {owner} contains an invalid migration version"
            )))
        }
    };
    let name = match row.values().get(3) {
        Some(RelationalValue::Text(name)) => name.clone(),
        _ => {
            return Err(SkeinError::Storage(format!(
                "system schema {owner} contains an invalid migration name"
            )))
        }
    };
    let checksum = match row.values().get(4) {
        Some(RelationalValue::Text(checksum)) => checksum.clone(),
        _ => {
            return Err(SkeinError::Storage(format!(
                "system schema {owner} contains an invalid migration checksum"
            )))
        }
    };
    Ok(AppliedMigration {
        version,
        name,
        checksum,
    })
}

fn validate_applied_migrations(
    registry: &SystemSchemaRegistry,
    applied: &[AppliedMigration],
    registry_table_present: bool,
) -> Result<()> {
    if registry.owner == REGISTRY_OWNER && registry_table_present && applied.is_empty() {
        return Err(SkeinError::Storage(
            "system schema registry exists without its engine migration record".to_string(),
        ));
    }
    if applied.len() > registry.migrations.len() {
        return Err(SkeinError::Storage(format!(
            "system schema {} is at future version {}, binary supports {}",
            registry.owner,
            applied.last().map_or(0, |migration| migration.version),
            registry.current_version()
        )));
    }
    for (index, actual) in applied.iter().enumerate() {
        let expected = &registry.migrations[index];
        let expected_checksum = expected.checksum(&registry.owner);
        if actual.version != expected.version
            || actual.name != expected.name
            || actual.checksum != expected_checksum
        {
            return Err(SkeinError::Storage(format!(
                "system schema {} migration {} checksum or identity drifted",
                registry.owner, actual.version
            )));
        }
    }
    Ok(())
}

fn hash_component(hasher: &mut IntegrityHasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}
