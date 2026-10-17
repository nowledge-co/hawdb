// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use hawdb::{Database, PreparedReadLimits, ProjectionPublicationError, Value};
use std::num::NonZeroUsize;
use std::path::PathBuf;

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "hawdb-sql-table-scope-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn limits() -> PreparedReadLimits {
    let n = |value| NonZeroUsize::new(value).unwrap();
    PreparedReadLimits {
        dependency_entries: n(32),
        dependency_bytes: n(8192),
        statement_rows: n(8),
        statement_payload_bytes: n(8192),
    }
}
fn fixture(cold: bool, empty: bool) -> (Root, Database) {
    let root = Root::new();
    let path = root.0.join("db");
    let mut db = Database::open(&path).unwrap();
    db.query_sql("CREATE TABLE source_rows (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    db.query_sql("CREATE TABLE operational_rows (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    db.query_sql("CREATE INDEX source_body ON source_rows (body)")
        .unwrap();
    db.query("CREATE (:Other {id:'other',value:1})").unwrap();
    if !empty {
        db.query_sql("INSERT INTO source_rows (id,body) VALUES (1,'one'),(2,'two')")
            .unwrap();
    }
    if cold {
        db.checkpoint().unwrap();
        drop(db);
        db = Database::open_with_config(
            &path,
            hawdb::DatabaseConfig {
                storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
    }
    (root, db)
}

#[test]
fn table_scope_covers_unselected_rows_and_capture_to_query_gap_hot_and_cold() {
    for cold in [false, true] {
        for mutation in [
            "INSERT INTO source_rows (id,body) VALUES (3,'three')",
            "UPDATE source_rows SET body='changed' WHERE id=1",
            "UPDATE source_rows SET body='changed' WHERE id=2",
            "DELETE FROM source_rows WHERE id=2",
            "UPDATE source_rows SET id=3 WHERE id=2",
            "INSERT INTO operational_rows (id,body) VALUES (1,'irrelevant')",
        ] {
            let (root, mut db) = fixture(cold, false);
            let mut read = db.begin_prepared_read(limits()).unwrap();
            let e = read.source_epoch();
            // Commit after snapshot registration but before its first query.
            db.query_sql(mutation).unwrap();
            db.query("MATCH (n:Other) SET n.value=2").unwrap();
            let output = read
                .query_sql_with_params("SELECT id,body FROM source_rows ORDER BY id LIMIT 1", &[])
                .unwrap();
            assert_eq!(output.rows.len(), 1);
            assert_eq!(output.rows[0]["body"], Value::String("one".into()));
            let dependencies = read.finish().unwrap();
            assert_eq!(dependencies.relational_table_domain_count(), 1);
            assert_eq!(dependencies.exact_relational_row_count(), 0);
            assert!(!dependencies.requires_unchanged_database_epoch());
            // Controlled generic candidate: this tests source validation, not
            // complete projection mapping or stage-bound coverage authority.
            let writer = hawdb::SearchOutOfCoreGenerationWriter::create(
                root.0.join("search"),
                hawdb::SearchOutOfCoreGenerationBuildOptions {
                    source_graph_commit_epoch: Some(e),
                    ..Default::default()
                },
            )
            .unwrap();
            let result =
                db.publish_prepared_search(&dependencies, writer.prepare_publication().unwrap());
            if mutation.contains("operational_rows") {
                assert_eq!(result.unwrap().source_graph_commit_epoch, Some(e));
            } else {
                let Err(ProjectionPublicationError::Dependency(error)) = result else {
                    panic!("expected source dependency refusal: cold={cold}, {mutation}");
                };
                // A primary-key move may retain the existing broad writer
                // barrier. Ordinary row changes must hit this exact table.
                if !mutation.contains("SET id=3") {
                    assert!(
                        error.to_string().contains("ReadRelationalTableDomain"),
                        "{error}"
                    );
                }
            }
        }
    }
}

#[test]
fn empty_table_scope_detects_phantom_insert_hot_and_cold() {
    for cold in [false, true] {
        let (root, mut db) = fixture(cold, true);
        let mut read = db.begin_prepared_read(limits()).unwrap();
        assert!(read
            .query_sql_with_params("SELECT id FROM source_rows LIMIT 1", &[])
            .unwrap()
            .rows
            .is_empty());
        let dependencies = read.finish().unwrap();
        assert_eq!(dependencies.relational_table_domain_count(), 1);
        db.query_sql("INSERT INTO source_rows (id,body) VALUES (1,'new')")
            .unwrap();
        let writer = hawdb::SearchOutOfCoreGenerationWriter::create(
            root.0.join("search"),
            hawdb::SearchOutOfCoreGenerationBuildOptions {
                source_graph_commit_epoch: Some(dependencies.source_epoch()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(matches!(
            db.publish_prepared_search(&dependencies, writer.prepare_publication().unwrap()),
            Err(ProjectionPublicationError::Dependency(_))
        ));
    }
}

#[test]
fn table_scope_preserves_point_prefix_precision_and_sticky_dependency_admission() {
    let (_root, db) = fixture(false, false);
    for (sql, point, prefix) in [
        ("SELECT body FROM source_rows WHERE id=1", 1, 0),
        ("SELECT id FROM source_rows WHERE body='one'", 0, 1),
    ] {
        let mut read = db.begin_prepared_read(limits()).unwrap();
        assert_eq!(read.query_sql_with_params(sql, &[]).unwrap().rows.len(), 1);
        let dependencies = read.finish().unwrap();
        assert_eq!(dependencies.relational_table_domain_count(), 0);
        assert_eq!(dependencies.exact_relational_row_count(), point);
        assert_eq!(dependencies.relational_index_prefix_count(), prefix);
    }
    let mut config = limits();
    config.dependency_entries = NonZeroUsize::new(1).unwrap();
    let mut read = db.begin_prepared_read(config).unwrap();
    read.query_sql_with_params("SELECT id FROM source_rows LIMIT 1", &[])
        .unwrap();
    assert!(read
        .query_sql_with_params("SELECT id FROM operational_rows LIMIT 1", &[])
        .is_err());
    assert!(read.finish().is_err());
    config = limits();
    config.dependency_bytes = NonZeroUsize::new(1).unwrap();
    let mut read = db.begin_prepared_read(config).unwrap();
    assert!(read
        .query_sql_with_params("SELECT id FROM source_rows LIMIT 1", &[])
        .is_err());
    assert!(read.finish().is_err());
}
