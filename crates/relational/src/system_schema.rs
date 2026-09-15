use skein_core::{Result, SkeinError};
use skein_integrity::IntegrityHasher;

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

fn hash_component(hasher: &mut IntegrityHasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::{validate_system_schema_registry, SystemSchemaMigration, SystemSchemaRegistry};

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
}
