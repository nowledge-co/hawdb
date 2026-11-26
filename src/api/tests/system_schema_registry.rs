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
                    recovery_mode: crate::RecoveryMode::Strict,
                    ..DatabaseConfig::default()
                },
            )
            .unwrap();
            assert!(matches!(
                read_only
                    .runtime
                    .get()
                    .unwrap()
                    .store
                    .relational_row_page_recovery_status(),
                crate::RelationalRowPageRecoveryStatus::Unavailable {
                    recovered_commit_epoch: 4,
                    checkpoint_required: true,
                    ..
                }
            ));
            // Resident rows recovered from the WAL serve reads while the
            // schema checkpoint is pending (#1593).
            let rows = read_only
                .query_sql("SELECT kind FROM content_documents WHERE id = 'doc-1'")
                .unwrap();
            assert_eq!(rows.rows.len(), 1);
            assert_eq!(rows.rows[0]["kind"], Value::String("text".to_string()));
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
                    reopened
                        .runtime
                        .get()
                        .unwrap()
                        .store
                        .relational_row_page_recovery_status(),
                    crate::RelationalRowPageRecoveryStatus::CheckpointReady {
                        source_commit_epoch: 4,
                        ..
                    }
                ));
                registry_v2()
            }
            version => panic!("unexpected recovered schema version {version} after {stage}"),
        };
        let epoch_before_validation = reopened.commit_epoch().unwrap();
        let validation = reopened.apply_system_schema_registry(&registry).unwrap();
        assert!(validation.applied_versions.is_empty());
        assert_eq!(reopened.commit_epoch().unwrap(), epoch_before_validation);

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
        let changefeed = db.search_projection_changefeed_status().unwrap();
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
                relational_index_mode: hawdb_storage::config::RelationalIndexMode::Shadow,
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
            recovery_mode: crate::RecoveryMode::Strict,
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
            relational_index_mode: hawdb_storage::config::RelationalIndexMode::Authoritative,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();

    assert!(!db
        .runtime
        .get()
        .unwrap()
        .store
        .relational_state()
        .materialized_rows_resident());
    assert!(db
        .runtime
        .get()
        .unwrap()
        .store
        .relational_state()
        .canonical_row_metadata_only());
    assert_eq!(
        db.runtime
            .get()
            .unwrap()
            .store
            .relational_state()
            .overflow_segment_count(),
        0
    );
    assert!(
        db.storage_residency_report()
            .unwrap()
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
        storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
        relational_index_mode: hawdb_storage::config::RelationalIndexMode::Authoritative,
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                relational_index_mode: hawdb_storage::config::RelationalIndexMode::Shadow,
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
        assert!(db
            .runtime
            .get()
            .unwrap()
            .store
            .relational_state()
            .canonical_row_metadata_only());
        let mut transaction = db.begin_transaction().unwrap();
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
        let committed_epoch = db.commit_epoch().unwrap();
        db.checkpoint().unwrap();
        committed_epoch
    };

    // Default residency rebuilds resident rows from the same row root (#1737).
    {
        let mut materialized = Database::open(&path).unwrap();
        assert_eq!(materialized.commit_epoch().unwrap(), committed_epoch);
        assert!(!materialized
            .runtime
            .get()
            .unwrap()
            .store
            .relational_state()
            .canonical_row_metadata_only());
        let rows = materialized
            .query_sql("SELECT id, body FROM content_documents ORDER BY id")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[1]["body"], Value::String("private".to_string()));
    }

    let mut reopened = Database::open_with_config(&path, config).unwrap();
    assert_eq!(reopened.commit_epoch().unwrap(), committed_epoch);
    assert!(reopened
        .runtime
        .get()
        .unwrap()
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
        .unwrap()
        .checkpoint_epoch
        .expect("checkpoint generation");

    let report = db.apply_system_schema_registry(&registry_v2()).unwrap();

    assert_eq!(report.applied_versions, vec![2]);
    assert!(
        db.storage_reclamation_watermark()
            .unwrap()
            .checkpoint_epoch
            .unwrap()
            > generation_before
    );
    assert!(matches!(
        db.runtime.get().unwrap().store.relational_row_page_recovery_status(),
        crate::RelationalRowPageRecoveryStatus::CheckpointReady {
            source_commit_epoch,
            ..
        } if *source_commit_epoch == db.commit_epoch().unwrap()
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
                ["CREATE TABLE invalid_table (body TEXT PRIMARY KEY, other TEXT PRIMARY KEY)"],
            ),
        ],
    );
    {
        let mut db = Database::open(&path).unwrap();
        let error = db.apply_system_schema_registry(&invalid_v2).unwrap_err();
        assert!(error.to_string().contains("more than one primary key"));
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
            recovery_mode: crate::RecoveryMode::Strict,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let error = db.apply_system_schema_registry(&registry_v2()).unwrap_err();
    assert_eq!(
        error,
        crate::HawDBError::SystemSchemaUpgradeRequired {
            owner: "nowledge.content_store".to_string(),
            installed_version: 1,
            expected_version: 2,
        }
    );
    // The text hosts could match before the error was typed (#1950).
    assert_eq!(
        error.to_string(),
        "execution error: system schema nowledge.content_store requires upgrade from version 1 to 2 but the database is read-only"
    );
    // A refusal does not poison the handle.
    assert!(db
        .query_sql("SELECT id FROM content_documents")
        .unwrap()
        .rows
        .is_empty());
    assert!(db
        .apply_system_schema_registry(&registry_v1())
        .unwrap()
        .applied_versions
        .is_empty());
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_open_reports_a_pending_engine_system_schema() {
    let path = unique_test_dir("read_only_engine_system_schema_upgrade");
    // A store written without the facade has no engine registry.
    {
        let mut catalog = crate::schema::Catalog::default();
        let mut store = crate::store::GraphStore::open_with_durability(
            &path,
            &mut catalog,
            crate::store::DurabilityPolicy::SyncOnEveryWrite,
        )
        .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("id".into(), Value::String("legacy".into()))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    let read_only = DatabaseConfig {
        read_only: true,
        recovery_mode: crate::RecoveryMode::Strict,
        ..DatabaseConfig::default()
    };
    let error = Database::open_with_config(&path, read_only.clone())
        .err()
        .unwrap();
    assert_eq!(
        error,
        crate::HawDBError::SystemSchemaUpgradeRequired {
            owner: "hawdb.engine".to_string(),
            installed_version: 0,
            expected_version: crate::engine_system_schema_expected_version(),
        }
    );
    // The refused open changed nothing; a writable open upgrades.
    drop(Database::open(&path).unwrap());
    let mut db = Database::open_with_config(&path, read_only).unwrap();
    assert_eq!(
        db.system_schema_status(&[]).unwrap(),
        [status("hawdb.engine", 1, 1)]
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

fn status(owner: &str, installed: u64, expected: u64) -> crate::SystemSchemaStatus {
    crate::SystemSchemaStatus {
        owner: owner.to_string(),
        installed_version: installed,
        expected_version: expected,
        pending: installed < expected,
    }
}

#[test]
fn system_schema_status_reports_every_owner_without_writing() {
    assert_eq!(crate::engine_system_schema_expected_version(), 1);
    let other = SystemSchemaRegistry::new(
        "host.other",
        [SystemSchemaMigration::new(
            1,
            "create_other",
            ["CREATE TABLE other (id TEXT PRIMARY KEY)"],
        )],
    );
    let pending = [
        status("hawdb.engine", 1, 1),
        status("nowledge.content_store", 1, 2),
        status("host.other", 0, 1),
    ];
    let path = unique_test_dir("system_schema_status");
    {
        let mut db = Database::open(&path).unwrap();
        db.apply_system_schema_registry(&registry_v1()).unwrap();
        let epoch = db.commit_epoch().unwrap();
        assert_eq!(
            db.system_schema_status(&[registry_v2(), other.clone()])
                .unwrap(),
            pending
        );
        assert_eq!(
            db.system_schema_status(&[registry_v1()]).unwrap(),
            pending[..1]
                .iter()
                .cloned()
                .chain([status("nowledge.content_store", 1, 1)])
                .collect::<Vec<_>>()
        );
        assert_eq!(db.commit_epoch().unwrap(), epoch);
    }
    let read_only = |residency| DatabaseConfig {
        read_only: true,
        recovery_mode: crate::RecoveryMode::Strict,
        storage_residency_mode: residency,
        ..DatabaseConfig::default()
    };
    for residency in [
        hawdb_storage::config::StorageResidencyMode::Materialized,
        hawdb_storage::config::StorageResidencyMode::OutOfCore,
    ] {
        let mut db = Database::open_with_config(&path, read_only(residency)).unwrap();
        assert_eq!(
            db.system_schema_status(&[registry_v2(), other.clone()])
                .unwrap(),
            pending
        );
        // The query and the upgrade refusal agree on the first pending owner.
        assert_eq!(
            db.apply_system_schema_registries(&[registry_v2(), other.clone()])
                .unwrap_err(),
            crate::HawDBError::SystemSchemaUpgradeRequired {
                owner: pending[1].owner.clone(),
                installed_version: pending[1].installed_version,
                expected_version: pending[1].expected_version,
            }
        );
    }
    {
        let mut db = Database::open(&path).unwrap();
        db.apply_system_schema_registries(&[registry_v2(), other.clone()])
            .unwrap();
        assert!(db
            .system_schema_status(&[registry_v2(), other.clone()])
            .unwrap()
            .iter()
            .all(|status| !status.pending));
        // A future schema and a duplicate owner are errors, as for upgrades.
        assert!(db
            .system_schema_status(&[registry_v1()])
            .unwrap_err()
            .to_string()
            .contains("future version 2"));
        assert!(db
            .system_schema_status(&[other.clone(), other.clone()])
            .is_err());
    }
    std::fs::remove_dir_all(path).unwrap();

    // An in-memory handle has recorded no schema yet.
    let mut memory = Database::new();
    assert_eq!(
        memory.system_schema_status(&[other]).unwrap(),
        [status("hawdb.engine", 0, 1), status("host.other", 0, 1)]
    );
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
    assert_eq!(db.commit_epoch().unwrap(), 0);
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

    let mut transaction = db.begin_transaction().unwrap();
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
    assert_eq!(db.commit_epoch().unwrap(), 0);
}

#[test]
fn embedded_open_applies_application_schema_before_returning_the_store() {
    let path = unique_test_dir("embedded_application_schema_open");
    let options = crate::EmbeddedStoreOpenOptions::graph_only(
        path.clone(),
        crate::GraphMode::WritableCutover,
    )
    .with_system_schema_registry(registry_v1());

    let (mut store, report) = crate::EmbeddedStore::open_with_options(options).unwrap();
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
    let options = crate::EmbeddedStoreOpenOptions::graph_only(
        path.clone(),
        crate::GraphMode::WritableCutover,
    )
    .with_system_schema_registry(registry_v1())
    .with_system_schema_registry(registry_v1());

    let error = crate::EmbeddedStoreHandle::open_with_options(options).unwrap_err();
    assert!(error.to_string().contains("registered more than once"));
    assert!(!path.exists());
}

/// One registry per owner, each creating an empty table and an index on it,
/// like an embedding host's first-open schema set (#1942).
fn batch_registries(count: usize) -> Vec<SystemSchemaRegistry> {
    (0..count)
        .map(|ordinal| {
            SystemSchemaRegistry::new(
                format!("batch.owner{ordinal}"),
                [SystemSchemaMigration::new(
                    1,
                    "initial",
                    [
                        format!(
                            "CREATE TABLE batch_{ordinal} (id TEXT PRIMARY KEY, kind TEXT NOT NULL, n BIGINT)"
                        ),
                        format!("CREATE INDEX batch_{ordinal}_kind ON batch_{ordinal} (kind)"),
                    ],
                )],
            )
        })
        .collect()
}

/// The canonical row-page generation. Every published checkpoint advances it
/// by the same step; tests measure that step with an explicit checkpoint.
fn checkpoint_generation(db: &Database) -> u64 {
    match db
        .runtime
        .get()
        .unwrap()
        .store
        .relational_row_page_recovery_status()
    {
        crate::RelationalRowPageRecoveryStatus::CheckpointReady { generation, .. } => *generation,
        crate::RelationalRowPageRecoveryStatus::LiveCurrent {
            base_generation, ..
        } => *base_generation,
        status => panic!("expected a current canonical row checkpoint, got {status:?}"),
    }
}

/// The generation advance of one published checkpoint.
fn checkpoint_step(db: &mut Database) -> u64 {
    let before = checkpoint_generation(db);
    db.checkpoint().unwrap();
    let step = checkpoint_generation(db) - before;
    assert!(step > 0, "a checkpoint must publish a new generation");
    step
}

fn owner_versions(db: &mut Database, owner: &str) -> Vec<i64> {
    let mut versions = db
        .query_sql_with_params(
            "SELECT version FROM hawdb_schema_migrations WHERE owner = $1",
            &[Value::String(owner.to_string())],
        )
        .unwrap()
        .rows
        .into_iter()
        .map(|row| match row.get("version") {
            Some(Value::Int(version)) => *version,
            value => panic!("expected a migration version for {owner}, got {value:?}"),
        })
        .collect::<Vec<_>>();
    versions.sort_unstable();
    versions
}

fn kind_ids(db: &mut Database, table: &str, kind: &str) -> Vec<String> {
    let mut ids = db
        .query_sql_with_params(
            &format!("SELECT id FROM {table} WHERE kind = $1"),
            &[Value::String(kind.to_string())],
        )
        .unwrap()
        .rows
        .into_iter()
        .map(|row| match row.get("id") {
            Some(Value::String(id)) => id.clone(),
            value => panic!("expected an id in {table}, got {value:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

#[test]
fn batched_system_schema_registries_publish_one_commit_and_one_checkpoint() {
    const REGISTRIES: usize = 6;
    let path = unique_test_dir("batched_system_schema_registries");
    let registries = batch_registries(REGISTRIES);
    {
        let mut db = Database::open(&path).unwrap();
        let step = checkpoint_step(&mut db);
        let generation_before = checkpoint_generation(&db);
        let epoch_before = db.commit_epoch().unwrap();

        let reports = db.apply_system_schema_registries(&registries).unwrap();

        // N new-table registries cost one WAL commit and one schema
        // checkpoint, not N of each.
        assert_eq!(db.commit_epoch().unwrap(), epoch_before + 1);
        assert_eq!(
            checkpoint_generation(&db),
            generation_before + step,
            "batched registries must publish exactly one checkpoint"
        );
        assert!(matches!(
            db.runtime.get().unwrap().store.relational_row_page_recovery_status(),
            crate::RelationalRowPageRecoveryStatus::CheckpointReady {
                source_commit_epoch,
                ..
            } if *source_commit_epoch == epoch_before + 1
        ));
        assert_eq!(reports.len(), REGISTRIES);
        for (ordinal, report) in reports.iter().enumerate() {
            assert_eq!(report.owner, format!("batch.owner{ordinal}"));
            assert_eq!(report.previous_version, 0);
            assert_eq!(report.current_version, 1);
            assert_eq!(report.applied_versions, vec![1]);
            assert_eq!(report.commit_epoch_before, epoch_before);
            assert_eq!(report.commit_epoch_after, epoch_before + 1);
        }

        for ordinal in 0..REGISTRIES {
            db.query_sql(&format!(
                "INSERT INTO batch_{ordinal} (id, kind, n) VALUES ('a', 'k', 1), ('b', 'other', 2), ('c', 'k', 3)"
            ))
            .unwrap();
            assert_eq!(
                kind_ids(&mut db, &format!("batch_{ordinal}"), "k"),
                vec!["a", "c"]
            );
        }

        // The same registries one owner at a time pay one checkpoint each.
        let generation = checkpoint_generation(&db);
        for registry in batch_registries(REGISTRIES + 2).iter().skip(REGISTRIES) {
            db.apply_system_schema_registry(registry).unwrap();
        }
        assert_eq!(checkpoint_generation(&db), generation + 2 * step);

        // A current batch neither commits nor checkpoints.
        let generation = checkpoint_generation(&db);
        let epoch = db.commit_epoch().unwrap();
        let reports = db.apply_system_schema_registries(&registries).unwrap();
        assert!(reports
            .iter()
            .all(|report| report.applied_versions.is_empty()
                && report.commit_epoch_before == report.commit_epoch_after));
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert_eq!(checkpoint_generation(&db), generation);
    }
    {
        let mut db = Database::open(&path).unwrap();
        let epoch = db.commit_epoch().unwrap();
        let reports = db.apply_system_schema_registries(&registries).unwrap();
        assert!(reports
            .iter()
            .all(|report| report.applied_versions.is_empty()));
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        for ordinal in 0..REGISTRIES {
            assert_eq!(
                kind_ids(&mut db, &format!("batch_{ordinal}"), "k"),
                vec!["a", "c"]
            );
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn batched_system_schema_registries_index_existing_rows_and_skip_current_owners() {
    let path = unique_test_dir("batched_system_schema_existing_rows");
    let mut db = Database::open(&path).unwrap();
    db.apply_system_schema_registry(&registry_v1()).unwrap();
    db.query_sql(
        "INSERT INTO content_documents (id, body) VALUES ('doc-1', 'x'), ('doc-2', 'y'), ('doc-3', 'x')",
    )
    .unwrap();
    let step = checkpoint_step(&mut db);
    let generation_before = checkpoint_generation(&db);
    let indexed_v2 = SystemSchemaRegistry::new(
        "nowledge.content_store",
        [
            registry_v1().migrations()[0].clone(),
            SystemSchemaMigration::new(
                2,
                "index_body",
                ["CREATE INDEX content_documents_body ON content_documents (body)"],
            ),
        ],
    );
    let mut registries = vec![indexed_v2];
    registries.extend(batch_registries(2));

    let reports = db.apply_system_schema_registries(&registries).unwrap();

    assert_eq!(reports[0].previous_version, 1);
    assert_eq!(reports[0].applied_versions, vec![2]);
    assert_eq!(reports[1].applied_versions, vec![1]);
    assert_eq!(reports[2].applied_versions, vec![1]);
    assert_eq!(checkpoint_generation(&db), generation_before + step);
    let mut ids = db
        .query_sql("SELECT id FROM content_documents WHERE body = 'x'")
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row["id"].clone())
        .collect::<Vec<_>>();
    ids.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(
        ids,
        vec![
            Value::String("doc-1".to_string()),
            Value::String("doc-3".to_string())
        ]
    );

    // A batch where one owner is current applies only the pending owner.
    let mut registries = batch_registries(3);
    registries.swap(0, 2);
    let epoch = db.commit_epoch().unwrap();
    let reports = db.apply_system_schema_registries(&registries).unwrap();
    assert_eq!(reports[0].owner, "batch.owner2");
    assert_eq!(reports[0].applied_versions, vec![1]);
    assert_eq!(reports[0].commit_epoch_after, epoch + 1);
    assert!(reports[1].applied_versions.is_empty());
    assert_eq!(reports[1].commit_epoch_after, epoch);
    assert!(reports[2].applied_versions.is_empty());
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn failed_batched_system_schema_registry_publishes_no_owner() {
    let path = unique_test_dir("failed_batched_system_schema_registries");
    let mut db = Database::open(&path).unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut registries = batch_registries(2);
    registries.push(SystemSchemaRegistry::new(
        "batch.invalid",
        [SystemSchemaMigration::new(
            1,
            "invalid_table",
            ["CREATE TABLE invalid_table (body TEXT PRIMARY KEY, other TEXT PRIMARY KEY)"],
        )],
    ));

    let error = db.apply_system_schema_registries(&registries).unwrap_err();

    assert!(error.to_string().contains("more than one primary key"));
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    for owner in ["batch.owner0", "batch.owner1", "batch.invalid"] {
        assert!(owner_versions(&mut db, owner).is_empty(), "{owner}");
    }
    assert!(db.query_sql("SELECT * FROM batch_0").is_err());

    let duplicate = [registry_v1(), registry_v1()];
    assert!(db
        .apply_system_schema_registries(&duplicate)
        .unwrap_err()
        .to_string()
        .contains("registered more than once"));
    let engine_claim = [
        registry_v1(),
        SystemSchemaRegistry::new(
            "hawdb.engine",
            [SystemSchemaMigration::new(
                1,
                "claim",
                ["CREATE TABLE claimed (id TEXT PRIMARY KEY)"],
            )],
        ),
    ];
    assert!(db
        .apply_system_schema_registries(&engine_claim)
        .unwrap_err()
        .to_string()
        .contains("reserved by the engine"));
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert!(owner_versions(&mut db, "nowledge.content_store").is_empty());

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn batched_system_schema_crash_child() {
    if std::env::var_os(SYSTEM_SCHEMA_CRASH_CHILD_ENV).is_none() {
        return;
    }
    let path = std::path::PathBuf::from(
        std::env::var_os(SYSTEM_SCHEMA_CRASH_PATH_ENV)
            .expect("batched system schema crash test database path"),
    );
    let point = std::env::var("HAWDB_TEST_PROCESS_CRASH_POINT")
        .expect("batched system schema crash test point");
    let mut db = Database::open(&path).unwrap();
    let mut registries = vec![registry_v2()];
    registries.extend(batch_registries(3));
    db.apply_system_schema_registries(&registries).unwrap();
    panic!("batched system schema crash failpoint {point} did not terminate the child process");
}

#[test]
fn batched_system_schema_crash_recovers_every_owner_or_none() {
    let stages = [
        ("before_wal_append", Some(false)),
        ("after_wal_append", None),
        ("after_wal_sync", Some(true)),
        ("during_checkpoint_publication", Some(true)),
        ("after_manifest_publication", Some(true)),
    ];

    for (stage, required_applied) in stages {
        let path = unique_test_dir(&format!("batched_system_schema_crash_{stage}"));
        {
            let mut db = Database::open(&path).unwrap();
            db.apply_system_schema_registry(&registry_v1()).unwrap();
            db.query_sql("INSERT INTO content_documents (id, body) VALUES ('doc-1', 'body')")
                .unwrap();
            db.checkpoint().unwrap();
        }

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("api::tests::system_schema_registry::batched_system_schema_crash_child")
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

        let mut reopened = Database::open(&path).unwrap();
        let content_versions = owner_versions(&mut reopened, "nowledge.content_store");
        let applied = content_versions == vec![1, 2];
        if let Some(required_applied) = required_applied {
            assert_eq!(applied, required_applied, "recovery at {stage}");
        }
        if !applied {
            assert_eq!(content_versions, vec![1], "recovery at {stage}");
        }
        for ordinal in 0..3 {
            let owner = format!("batch.owner{ordinal}");
            let table = format!("batch_{ordinal}");
            if applied {
                assert_eq!(owner_versions(&mut reopened, &owner), vec![1], "{stage}");
                // Recovered tables and indexes accept and serve new rows.
                reopened
                    .query_sql(&format!(
                        "INSERT INTO {table} (id, kind) VALUES ('after-{stage}', 'k')"
                    ))
                    .unwrap();
                assert_eq!(
                    kind_ids(&mut reopened, &table, "k"),
                    vec![format!("after-{stage}")]
                );
            } else {
                assert!(owner_versions(&mut reopened, &owner).is_empty(), "{stage}");
                assert!(reopened
                    .query_sql(&format!("SELECT * FROM {table}"))
                    .is_err());
            }
        }
        let kind = reopened
            .query_sql("SELECT kind FROM content_documents WHERE id = 'doc-1'")
            .map(|output| output.rows[0]["kind"].clone());
        if applied {
            assert_eq!(kind.unwrap(), Value::String("text".to_string()));
        } else {
            assert!(kind.is_err(), "version 1 exposed the version 2 column");
        }

        // Reapplying the whole batch completes an absent upgrade or is a
        // no-op, and rows inserted after recovery survive a reopen.
        let mut registries = vec![registry_v2()];
        registries.extend(batch_registries(3));
        reopened
            .apply_system_schema_registries(&registries)
            .unwrap();
        drop(reopened);
        let mut reopened = Database::open(&path).unwrap();
        let epoch = reopened.commit_epoch().unwrap();
        let reports = reopened
            .apply_system_schema_registries(&registries)
            .unwrap();
        assert!(reports
            .iter()
            .all(|report| report.applied_versions.is_empty()));
        assert_eq!(reopened.commit_epoch().unwrap(), epoch);
        if applied {
            assert_eq!(
                kind_ids(&mut reopened, "batch_0", "k"),
                vec![format!("after-{stage}")]
            );
        }

        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
}

/// Restoring a dump's records is not a second way to write the registry
/// table of a live database.
#[test]
fn ledger_restore_is_limited_to_a_fresh_database() {
    use hawdb_relational::system_schema::SystemSchemaLedger;

    let root = std::env::temp_dir().join(format!(
        "hawdb-ledger-restore-{}-{}",
        std::process::id(),
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let row = [
        Value::String("example.host:1".to_string()),
        Value::String("example.host".to_string()),
        Value::Int(1),
        Value::String("create_host".to_string()),
        Value::String("0".repeat(64)),
    ];
    let ledger = SystemSchemaLedger::from_rows([&row[..]]).unwrap();
    let mut database = Database::open(&root).unwrap();
    assert!(database
        .verify_restored_system_schema_ledger(&ledger)
        .is_err());
    database
        .verify_restored_system_schema_ledger(&SystemSchemaLedger::default())
        .unwrap();
    assert_eq!(database.restore_system_schema_ledger(&ledger).unwrap(), 1);
    database
        .verify_restored_system_schema_ledger(&ledger)
        .unwrap();
    assert!(database
        .verify_restored_system_schema_ledger(&SystemSchemaLedger::default())
        .is_err());
    let error = database.restore_system_schema_ledger(&ledger).unwrap_err();
    assert!(error.to_string().contains("fresh database"), "{error}");
    drop(database);
    std::fs::remove_dir_all(root).unwrap();
}
