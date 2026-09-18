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

use super::*;
use crate::{SystemSchemaMigration, SystemSchemaRegistry};

fn registry_v1() -> SystemSchemaRegistry {
    SystemSchemaRegistry::new(
        "nowledge.content_store",
        [SystemSchemaMigration::new(
            1,
            "create_documents",
            ["CREATE TABLE content_documents (id TEXT PRIMARY KEY, body TEXT NOT NULL)"],
        )],
    )
}

fn registry_v2() -> SystemSchemaRegistry {
    SystemSchemaRegistry::new(
        "nowledge.content_store",
        [
            SystemSchemaMigration::new(
                1,
                "create_documents",
                ["CREATE TABLE content_documents (id TEXT PRIMARY KEY, body TEXT NOT NULL)"],
            ),
            SystemSchemaMigration::new(
                2,
                "add_document_kind",
                ["ALTER TABLE content_documents ADD COLUMN kind TEXT NOT NULL DEFAULT 'text'"],
            ),
        ],
    )
}

const SYSTEM_SCHEMA_CRASH_CHILD_ENV: &str = "HAWDB_TEST_SYSTEM_SCHEMA_CRASH_CHILD";
const SYSTEM_SCHEMA_CRASH_PATH_ENV: &str = "HAWDB_TEST_SYSTEM_SCHEMA_CRASH_PATH";

#[test]
fn system_schema_upgrade_crash_child() {
    if std::env::var_os(SYSTEM_SCHEMA_CRASH_CHILD_ENV).is_none() {
        return;
    }
    let path = std::path::PathBuf::from(
        std::env::var_os(SYSTEM_SCHEMA_CRASH_PATH_ENV)
            .expect("system schema crash test database path"),
    );
    let point =
        std::env::var("HAWDB_TEST_PROCESS_CRASH_POINT").expect("system schema crash test point");
    let mut db = Database::open(&path).unwrap();
    db.apply_system_schema_registry(&registry_v2()).unwrap();
    panic!("system schema crash failpoint {point} did not terminate the child process");
}

#[test]
fn application_system_schema_upgrade_crash_recovers_a_consistent_registry_and_schema() {
    let stages = [
        ("before_wal_append", Some(1)),
        ("after_wal_append", None),
        ("after_wal_sync", Some(2)),
        ("during_checkpoint_publication", Some(2)),
        ("after_manifest_publication", Some(2)),
    ];

    for (stage, required_version) in stages {
        let path = unique_test_dir(&format!("system_schema_upgrade_crash_{stage}"));
        {
            let mut db = Database::open(&path).unwrap();
            db.apply_system_schema_registry(&registry_v1()).unwrap();
            db.query_sql("INSERT INTO content_documents (id, body) VALUES ('doc-1', 'body')")
                .unwrap();
            db.checkpoint().unwrap();
        }

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("api::tests::system_schema_registry::system_schema_upgrade_crash_child")
            .arg("--nocapture")
            .env(SYSTEM_SCHEMA_CRASH_CHILD_ENV, "1")
            .env(SYSTEM_SCHEMA_CRASH_PATH_ENV, &path)
            .env("HAWDB_TEST_PROCESS_CRASH_POINT", stage)
            .status()
            .unwrap();
        assert_eq!(
            status.code(),
            Some(86),
            "child did not terminate at {stage}"
        );

        if stage == "after_wal_sync" {
            let mut read_only = Database::open_with_config(
                &path,
                DatabaseConfig {
                    read_only: true,
                    ..DatabaseConfig::default()
                },
            )
            .unwrap();
            assert!(matches!(
                read_only.store.relational_row_page_recovery_status(),
                crate::RelationalRowPageRecoveryStatus::Unavailable {
                    recovered_commit_epoch: 4,
                    checkpoint_required: true,
                    ..
                }
            ));
            let error = read_only
                .query_sql("SELECT kind FROM content_documents WHERE id = 'doc-1'")
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("canonical relational row reader is unavailable"));
        }

        let mut reopened = Database::open(&path).unwrap();
        let mut versions = reopened
            .query_sql(
                "SELECT version FROM hawdb_schema_migrations WHERE owner = 'nowledge.content_store'",
            )
            .unwrap()
            .rows
            .into_iter()
            .map(|row| match row.get("version") {
                Some(Value::Int(version)) => *version,
                value => panic!("expected migration version after {stage}, got {value:?}"),
            })
            .collect::<Vec<_>>();
        versions.sort_unstable();
        let current_version = versions.last().copied().expect("application migration");
        if let Some(required_version) = required_version {
            assert_eq!(current_version, required_version, "recovery at {stage}");
        }

        let kind = reopened
            .query_sql("SELECT kind FROM content_documents WHERE id = 'doc-1'")
            .map(|output| output.rows[0]["kind"].clone());
        let registry = match current_version {
            1 => {
                assert_eq!(versions, vec![1]);
                assert!(kind.is_err(), "version 1 exposed the version 2 column");
                registry_v1()
            }
            2 => {
                assert_eq!(versions, vec![1, 2]);
                assert_eq!(kind.unwrap(), Value::String("text".to_string()));
                assert!(matches!(
                    reopened.store.relational_row_page_recovery_status(),
                    crate::RelationalRowPageRecoveryStatus::CheckpointReady {
                        source_commit_epoch: 4,
                        ..
                    }
                ));
                registry_v2()
            }
            version => panic!("unexpected recovered schema version {version} after {stage}"),
        };
        let epoch_before_validation = reopened.commit_epoch();
        let validation = reopened.apply_system_schema_registry(&registry).unwrap();
        assert!(validation.applied_versions.is_empty());
        assert_eq!(reopened.commit_epoch(), epoch_before_validation);

        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn engine_system_schema_bootstraps_during_persistent_open() {
    let path = unique_test_dir("engine_system_schema_bootstrap");
    let first_epoch = {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query_sql("SELECT owner, version FROM hawdb_schema_migrations")
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["owner"], Value::String("hawdb.engine".to_string()));
        assert_eq!(rows[0]["version"], Value::Int(1));
        let changefeed = db.search_projection_changefeed_status();
        assert_eq!(changefeed.retained_mutation_count, 0);
        assert_eq!(changefeed.required_projection_commit_epoch(), 0);
        assert_eq!(changefeed.projection_commit_lag_after(0), 0);
        let delta = db
            .build_search_projection_graph_delta_request_after(0, Some(1))
            .unwrap()
            .unwrap();
        assert!(delta.upsert_node_ids.is_empty());
        assert!(delta.delete_document_ids.is_empty());
        assert_eq!(delta.complete_through_graph_commit_epoch, Some(1));
        db.commit_epoch()
    };
    let reopened = Database::open(&path).unwrap();
    assert_eq!(reopened.commit_epoch(), first_epoch);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_out_of_core_open_validates_system_schema_through_canonical_rows() {
    let path = unique_test_dir("read_only_out_of_core_system_schema");
    let body = "x".repeat(8 * 1024);
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                relational_index_mode: hawdb_storage::RelationalIndexMode::Shadow,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        db.apply_system_schema_registry(&registry_v1()).unwrap();
        db.query_sql_with_params(
            "INSERT INTO content_documents (id, body) VALUES ($1, $2)",
            &[
                Value::String("doc-1".to_string()),
                Value::String(body.clone()),
            ],
        )
        .unwrap();
        db.checkpoint().unwrap();
    }

    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            read_only: true,
            storage_residency_mode: hawdb_storage::StorageResidencyMode::OutOfCore,
            relational_index_mode: hawdb_storage::RelationalIndexMode::Authoritative,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();

    assert!(!db.store.relational_state().materialized_rows_resident());
    assert!(db.store.relational_state().canonical_row_metadata_only());
    assert_eq!(db.store.relational_state().overflow_segment_count(), 0);
    assert!(
        db.storage_residency_report()
            .relational_rows
            .overflow_extent_count
            > 0
    );
    assert_eq!(
        db.query_sql("SELECT body FROM content_documents WHERE id = 'doc-1'")
            .unwrap()
            .rows[0]["body"],
        Value::String(body)
    );
    let validation = db.apply_system_schema_registry(&registry_v1()).unwrap();
    assert!(validation.applied_versions.is_empty());

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn writable_metadata_only_transaction_checkpoints_rows_and_indexes() {
    let path = unique_test_dir("writable_metadata_only_transaction");
    let config = DatabaseConfig {
        storage_residency_mode: hawdb_storage::StorageResidencyMode::OutOfCore,
        relational_index_mode: hawdb_storage::RelationalIndexMode::Authoritative,
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                relational_index_mode: hawdb_storage::RelationalIndexMode::Shadow,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        db.apply_system_schema_registry(&registry_v1()).unwrap();
        db.query_sql("INSERT INTO content_documents (id, body) VALUES ('doc-1', 'base')")
            .unwrap();
        db.checkpoint().unwrap();
    }

    let committed_epoch = {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        assert!(db.store.relational_state().canonical_row_metadata_only());
        let mut transaction = db.begin_transaction();
        transaction
            .query_sql("INSERT INTO content_documents (id, body) VALUES ('doc-2', 'private')")
            .unwrap();
        let private = transaction
            .query_sql("SELECT body FROM content_documents WHERE id = 'doc-2'")
            .unwrap();
        assert_eq!(
            private.rows[0]["body"],
            Value::String("private".to_string())
        );
        let duplicate = transaction
            .query_sql("INSERT INTO content_documents (id, body) VALUES ('doc-2', 'rejected')")
            .unwrap_err();
        assert!(
            duplicate.to_string().contains("duplicate primary key"),
            "unexpected duplicate-key error: {duplicate}"
        );
        let after_rejection = transaction
            .query_sql("SELECT body FROM content_documents WHERE id = 'doc-2'")
            .unwrap();
        assert_eq!(after_rejection.rows, private.rows);
        transaction.commit().unwrap();
        let committed_epoch = db.commit_epoch();
        db.checkpoint().unwrap();
        committed_epoch
    };

    let incompatible = match Database::open(&path) {
        Ok(_) => panic!("metadata-only checkpoint unexpectedly reopened in materialized mode"),
        Err(error) => error,
    };
    assert!(incompatible
        .to_string()
        .contains("reopen requires OutOfCore residency with Authoritative relational indexes"));

    let mut reopened = Database::open_with_config(&path, config).unwrap();
    assert_eq!(reopened.commit_epoch(), committed_epoch);
    assert!(reopened
        .store
        .relational_state()
        .canonical_row_metadata_only());
    let rows = reopened
        .query_sql("SELECT id, body FROM content_documents ORDER BY id")
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    assert_eq!(rows.rows[0]["id"], Value::String("doc-1".to_string()));
    assert_eq!(rows.rows[1]["id"], Value::String("doc-2".to_string()));
    let validation = reopened
        .apply_system_schema_registry(&registry_v1())
        .unwrap();
    assert!(validation.applied_versions.is_empty());

    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn application_system_schema_upgrades_and_reopens_idempotently() {
    let path = unique_test_dir("application_system_schema_upgrade");
    {
        let mut db = Database::open(&path).unwrap();
        let report = db.apply_system_schema_registry(&registry_v1()).unwrap();
        assert_eq!(report.previous_version, 0);
        assert_eq!(report.current_version, 1);
        assert_eq!(report.applied_versions, vec![1]);
        db.query_sql("INSERT INTO content_documents (id, body) VALUES ('doc-1', 'body')")
            .unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let report = db.apply_system_schema_registry(&registry_v2()).unwrap();
        assert_eq!(report.previous_version, 1);
        assert_eq!(report.current_version, 2);
        assert_eq!(report.applied_versions, vec![2]);
        assert_eq!(
            db.query_sql("SELECT id FROM content_documents WHERE id = 'doc-1'")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert_eq!(
            db.query_sql("SELECT kind FROM content_documents WHERE id = 'doc-1'")
                .unwrap()
                .rows[0]["kind"],
            Value::String("text".to_string())
        );
    }
    {
        let mut db = Database::open(&path).unwrap();
        let report = db.apply_system_schema_registry(&registry_v2()).unwrap();
        assert_eq!(report.previous_version, 2);
        assert_eq!(report.current_version, 2);
        assert!(report.applied_versions.is_empty());
        assert_eq!(report.commit_epoch_before, report.commit_epoch_after);
        assert_eq!(
            db.query_sql("SELECT kind FROM content_documents WHERE id = 'doc-1'")
                .unwrap()
                .rows[0]["kind"],
            Value::String("text".to_string())
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_upgrade_publishes_a_canonical_row_checkpoint_before_returning() {
    let path = unique_test_dir("application_schema_row_checkpoint");
    let mut db = Database::open(&path).unwrap();
    db.apply_system_schema_registry(&registry_v1()).unwrap();
    db.query_sql("INSERT INTO content_documents (id, body) VALUES ('doc-1', 'body')")
        .unwrap();
    db.checkpoint().unwrap();
    let generation_before = db
        .storage_reclamation_watermark()
        .checkpoint_epoch
        .expect("checkpoint generation");

    let report = db.apply_system_schema_registry(&registry_v2()).unwrap();

    assert_eq!(report.applied_versions, vec![2]);
    assert_eq!(
        db.storage_reclamation_watermark().checkpoint_epoch,
        Some(generation_before + 1)
    );
    assert!(matches!(
        db.store.relational_row_page_recovery_status(),
        crate::RelationalRowPageRecoveryStatus::CheckpointReady {
            source_commit_epoch,
            ..
        } if *source_commit_epoch == db.commit_epoch()
    ));
    assert_eq!(
        db.query_sql("SELECT kind FROM content_documents WHERE id = 'doc-1'")
            .unwrap()
            .rows[0]["kind"],
        Value::String("text".to_string())
    );

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn application_system_schema_rejects_changed_applied_migration() {
    let path = unique_test_dir("application_system_schema_checksum_drift");
    {
        let mut db = Database::open(&path).unwrap();
        db.apply_system_schema_registry(&registry_v1()).unwrap();
    }
    let drifted = SystemSchemaRegistry::new(
        "nowledge.content_store",
        [SystemSchemaMigration::new(
            1,
            "create_documents",
            ["CREATE TABLE content_documents (id TEXT PRIMARY KEY, body BYTEA NOT NULL)"],
        )],
    );
    let mut db = Database::open(&path).unwrap();
    let error = db.apply_system_schema_registry(&drifted).unwrap_err();
    assert!(error.to_string().contains("checksum or identity drifted"));
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn application_system_schema_rejects_a_database_from_a_newer_binary() {
    let path = unique_test_dir("application_system_schema_future_version");
    {
        let mut db = Database::open(&path).unwrap();
        db.apply_system_schema_registry(&registry_v2()).unwrap();
    }
    let mut db = Database::open(&path).unwrap();
    let error = db.apply_system_schema_registry(&registry_v1()).unwrap_err();
    assert!(error.to_string().contains("binary supports 1"));
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn failed_application_system_schema_upgrade_does_not_publish_version() {
    let path = unique_test_dir("failed_application_system_schema_upgrade");
    {
        let mut db = Database::open(&path).unwrap();
        db.apply_system_schema_registry(&registry_v1()).unwrap();
    }
    let invalid_v2 = SystemSchemaRegistry::new(
        "nowledge.content_store",
        [
            registry_v1().migrations()[0].clone(),
            SystemSchemaMigration::new(
                2,
                "invalid_table",
                ["CREATE TABLE invalid_table (body TEXT NOT NULL)"],
            ),
        ],
    );
    {
        let mut db = Database::open(&path).unwrap();
        let error = db.apply_system_schema_registry(&invalid_v2).unwrap_err();
        assert!(error.to_string().contains("must declare a primary key"));
    }
    {
        let mut db = Database::open(&path).unwrap();
        let report = db.apply_system_schema_registry(&registry_v1()).unwrap();
        assert_eq!(report.previous_version, 1);
        assert!(report.applied_versions.is_empty());
        assert!(db
            .query_sql("SELECT * FROM invalid_table")
            .unwrap_err()
            .to_string()
            .contains("unknown relational table"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_database_rejects_pending_application_system_schema_upgrade() {
    let path = unique_test_dir("read_only_application_system_schema_upgrade");
    {
        let mut db = Database::open(&path).unwrap();
        db.apply_system_schema_registry(&registry_v1()).unwrap();
    }
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let error = db.apply_system_schema_registry(&registry_v2()).unwrap_err();
    assert!(error.to_string().contains("database is read-only"));
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn application_system_schema_requires_contiguous_versions() {
    let mut db = Database::new();
    let registry = SystemSchemaRegistry::new(
        "nowledge.content_store",
        [SystemSchemaMigration::new(
            2,
            "create_documents",
            ["CREATE TABLE content_documents (id TEXT PRIMARY KEY)"],
        )],
    );
    let error = db.apply_system_schema_registry(&registry).unwrap_err();
    assert!(error.to_string().contains("contiguous from version 1"));
    assert_eq!(db.commit_epoch(), 0);
}

#[test]
fn application_sql_cannot_modify_system_schema_registry() {
    let mut db = Database::new();
    db.apply_system_schema_registry(&registry_v1()).unwrap();

    let direct_error = db
        .query_sql("DELETE FROM hawdb_schema_migrations WHERE owner = 'nowledge.content_store'")
        .unwrap_err();
    assert!(direct_error
        .to_string()
        .contains("read-only outside system schema upgrade"));

    let mut transaction = db.begin_transaction();
    let transaction_error = transaction
        .query_sql(
            "UPDATE hawdb_schema_migrations SET version = 9 WHERE owner = 'nowledge.content_store'",
        )
        .unwrap_err();
    assert!(transaction_error
        .to_string()
        .contains("read-only outside system schema upgrade"));
    transaction.rollback();

    let rows = db
        .query_sql(
            "SELECT version FROM hawdb_schema_migrations WHERE owner = 'nowledge.content_store'",
        )
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["version"], Value::Int(1));
}

#[test]
fn application_migration_cannot_modify_system_schema_registry() {
    let mut db = Database::new();
    db.apply_system_schema_registry(&registry_v1()).unwrap();
    let malicious = SystemSchemaRegistry::new(
        "nowledge.content_store",
        [
            registry_v1().migrations()[0].clone(),
            SystemSchemaMigration::new(
                2,
                "rewrite_engine_registry",
                ["UPDATE hawdb_schema_migrations SET version = 9 WHERE owner = 'hawdb.engine'"],
            ),
        ],
    );

    let error = db.apply_system_schema_registry(&malicious).unwrap_err();
    assert!(error
        .to_string()
        .contains("read-only outside system schema upgrade"));
    let report = db.apply_system_schema_registry(&registry_v1()).unwrap();
    assert_eq!(report.previous_version, 1);
    assert!(report.applied_versions.is_empty());
}

#[test]
fn application_cannot_claim_the_engine_system_schema_owner() {
    let mut db = Database::new();
    let registry = SystemSchemaRegistry::new(
        "hawdb.engine",
        [SystemSchemaMigration::new(
            1,
            "untrusted_engine_schema",
            ["CREATE TABLE untrusted (id TEXT PRIMARY KEY)"],
        )],
    );

    let error = db.apply_system_schema_registry(&registry).unwrap_err();
    assert!(error.to_string().contains("reserved by the engine"));
    assert_eq!(db.commit_epoch(), 0);
}

#[test]
fn embedded_open_applies_application_schema_before_returning_the_store() {
    let path = unique_test_dir("embedded_application_schema_open");
    let options = crate::NowledgeMemOpenOptions::graph_only(
        path.clone(),
        crate::NowledgeMemGraphMode::WritableCutover,
    )
    .with_system_schema_registry(registry_v1());

    let (mut store, report) = crate::NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
    assert!(report.graph_opened);
    assert_eq!(report.system_schema_upgrades.len(), 1);
    assert_eq!(
        report.system_schema_upgrades[0].owner,
        "nowledge.content_store"
    );
    assert_eq!(report.system_schema_upgrades[0].applied_versions, vec![1]);
    store
        .graph_mut()
        .database_mut()
        .query_sql("INSERT INTO content_documents (id, body) VALUES ('doc-1', 'ready')")
        .unwrap();
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn embedded_open_rejects_duplicate_application_schema_owners_before_open() {
    let path = unique_test_dir("duplicate_application_schema_owner");
    let options = crate::NowledgeMemOpenOptions::graph_only(
        path.clone(),
        crate::NowledgeMemGraphMode::WritableCutover,
    )
    .with_system_schema_registry(registry_v1())
    .with_system_schema_registry(registry_v1());

    let error = crate::NowledgeMemEmbeddedStoreHandle::open_with_options(options).unwrap_err();
    assert!(error.to_string().contains("registered more than once"));
    assert!(!path.exists());
}
