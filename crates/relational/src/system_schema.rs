use skein_core::{Result, SkeinError};
use skein_integrity::IntegrityHasher;
use skein_sql::SqlStatement;

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

#[doc(hidden)]
pub fn validate_system_schema_registry(registry: &SystemSchemaRegistry) -> Result<()> {
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

/// Identifies statements that may mutate Skein's internal migration registry.
///
/// The embedded facade owns authorization and transaction handling; the
/// relational owner defines which SQL AST shapes target the registry.
#[doc(hidden)]
pub fn statement_writes_system_schema_registry(statement: &SqlStatement) -> bool {
    const REGISTRY_TABLE: &str = "skein_schema_migrations";

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
        table.name == REGISTRY_TABLE
            && table
                .schema
                .as_deref()
                .is_none_or(|schema| schema == "public")
    })
}

fn hash_component(hasher: &mut IntegrityHasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::{
        statement_writes_system_schema_registry, validate_system_schema_registry,
        SystemSchemaMigration, SystemSchemaRegistry,
    };

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
            migration.checksum("skein.engine")
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
        let insert = skein_sql::parse_postgres_sql(
            "INSERT INTO skein_schema_migrations (version, name, checksum) VALUES (1, 'init', 'x')",
        )
        .expect("parse registry insert");
        let public_create = skein_sql::parse_postgres_sql(
            "CREATE TABLE public.skein_schema_migrations (version BIGINT)",
        )
        .expect("parse public registry create");
        let other_schema = skein_sql::parse_postgres_sql(
            "INSERT INTO archive.skein_schema_migrations (version) VALUES (1)",
        )
        .expect("parse other-schema insert");
        let read = skein_sql::parse_postgres_sql("SELECT version FROM skein_schema_migrations")
            .expect("parse registry read");

        assert!(statement_writes_system_schema_registry(&insert));
        assert!(statement_writes_system_schema_registry(&public_create));
        assert!(!statement_writes_system_schema_registry(&other_schema));
        assert!(!statement_writes_system_schema_registry(&read));
    }
}
