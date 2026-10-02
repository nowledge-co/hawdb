//! Typed branch metadata lifecycle for the embedded database facade.
//!
//! Durable head publication and branch opening remain storage-owned. This
//! module provides the stable identity/state surface without exposing catalog
//! paths or filesystem records to callers.

use super::Database;
use crate::error::HawDBError;
use hawdb_core::Uuid;
use hawdb_storage::branch_catalog as storage;
use hawdb_storage::branch_head;
use hawdb_storage::ownership::DatabaseDirectoryLease;
use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;

const BRANCH_DIRECTORY: &str = "branches";
const BRANCH_CATALOG_FILE: &str = "catalog.hawdb";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchSelector {
    Id(Uuid),
    Name(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchCreateRequest {
    pub name: Option<String>,
    pub parent: BranchSelector,
    pub expected_source_commit_epoch: u64,
    pub owner: Option<String>,
    pub idempotency_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ConcurrentTransactionOptions;
    use hawdb_core::Value;
    use hawdb_storage::config::{DurabilityPolicy, WalReplayConfig};
    use hawdb_storage::store::{BranchAdmissionRequest, GraphStore};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{mpsc, Arc, Condvar, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_directory(name: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "hawdb-{name}-{}-{nanos}-{sequence}",
            std::process::id()
        ))
    }

    #[test]
    fn sql_branch_switch_retains_job_outcomes_and_never_reuses_ids() {
        let (path, mut database, main) = initialized_database();
        let child = database.create_branch(create_request(&main)).unwrap();
        database.query_sql("USE BRANCH main").unwrap();
        let job = database.derived_artifact_jobs.enqueue(
            "content_artifact",
            "document",
            "parse",
            BTreeMap::new(),
        );
        let claim = database
            .derived_artifact_jobs
            .claim_external_by_id(job.id)
            .unwrap();
        database
            .derived_artifact_jobs
            .complete(claim, Err(HawDBError::Execution("fixture failure".into())));
        assert!(matches!(
            database.query_sql("USE BRANCH child"),
            Err(HawDBError::BranchBusy { .. })
        ));
        assert_eq!(database.current_branch().unwrap().info.id, main.id);
        database
            .derived_artifact_jobs
            .retry_failed_external(job.id, None)
            .unwrap();
        let claim = database
            .derived_artifact_jobs
            .claim_external_by_id(job.id)
            .unwrap();
        let completed = database
            .derived_artifact_jobs
            .complete(claim, Ok(crate::QueryOutput::from_rows(Vec::new())));
        database.query_sql("USE BRANCH child").unwrap();
        assert_eq!(database.current_branch().unwrap().info.id, child.id);
        assert_eq!(database.derived_artifact_jobs.jobs(), vec![completed.job]);
        let next = database.derived_artifact_jobs.enqueue(
            "content_artifact",
            "document",
            "parse",
            BTreeMap::new(),
        );
        assert!(next.id > job.id);
        let claim = database
            .derived_artifact_jobs
            .claim_external_by_id(next.id)
            .unwrap();
        database.derived_artifact_jobs.complete(
            claim,
            Err(HawDBError::Execution("new branch failure".into())),
        );
        assert!(database
            .derived_artifact_jobs
            .retry_failed_external(job.id, None)
            .is_none());
        assert_eq!(
            database.derived_artifact_jobs.failed_external(1)[0].id,
            next.id
        );
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn sql_branch_selection_isolates_ddl_and_data_bearing_nested_forks() {
        for durability in [
            DurabilityPolicy::SyncOnEveryWrite,
            DurabilityPolicy::SyncOnCheckpoint,
        ] {
            let path = test_directory("sql-selected-branch");
            let mut database = Database::open_with_durability(&path, durability).unwrap();
            database.checkpoint().unwrap();
            database.query("CREATE (:Memory {id: 'seed'})").unwrap();
            let main = database
                .initialize_main_branch(Uuid::from_u128(11), Uuid::from_u128(12))
                .unwrap();
            database.query_sql("USE BRANCH NAME 'main'").unwrap();
            let create_sql = "CREATE BRANCH NAME $1 FROM ID $2 AT REVISION $3 REQUEST KEY $4";
            let request = [
                Value::String("dev".into()),
                Value::Uuid(main.id),
                Value::Int(database.commit_epoch() as i64),
                Value::String("sql-dev".into()),
            ];
            let created = database
                .query_sql_with_params(create_sql, &request)
                .unwrap();
            let Value::Uuid(child) = created.rows[0]["branch_id"] else {
                panic!("created UUID")
            };
            assert_eq!(
                database
                    .query_sql_with_params(create_sql, &request)
                    .unwrap(),
                created
            );
            database
                .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child)])
                .unwrap();
            let current = database.query_sql("SHOW CURRENT BRANCH").unwrap();
            assert_eq!(current.rows[0]["branch_id"], Value::Uuid(child));
            assert_eq!(
                current.rows[0]["durability"],
                Value::String(
                    match durability {
                        DurabilityPolicy::SyncOnEveryWrite => "sync_on_every_write",
                        DurabilityPolicy::SyncOnCheckpoint => "sync_on_checkpoint",
                    }
                    .into()
                )
            );
            database
                .query_sql("CREATE TABLE documents (id BIGINT PRIMARY KEY, content TEXT)")
                .unwrap();
            let child_head = database.branch_head_path(child).unwrap();
            let before_write = std::fs::read(&child_head).unwrap();
            database
                .query_sql("INSERT INTO documents (id, content) VALUES (1, 'child')")
                .unwrap();
            assert_eq!(std::fs::read(&child_head).unwrap(), before_write);
            database
                .query_sql("ALTER TABLE documents ADD COLUMN tag TEXT")
                .unwrap();
            database
                .query_sql("CREATE INDEX documents_content ON documents (content)")
                .unwrap();
            let overflow_body = "wal-overflow".repeat(4_096);
            database
                .query_sql_with_params(
                    "UPDATE documents SET content = $1 WHERE id = $2",
                    &[Value::String(overflow_body.clone()), Value::Int(1)],
                )
                .unwrap();
            let epoch = database.commit_epoch();
            let grandchild = database
                .query_sql_with_params(
                    create_sql,
                    &[
                        Value::String("grandchild".into()),
                        Value::Uuid(child),
                        Value::Int(epoch as i64),
                        Value::String("sql-grandchild".into()),
                    ],
                )
                .unwrap();
            let Value::Uuid(grandchild) = grandchild.rows[0]["branch_id"] else {
                panic!("grandchild UUID")
            };
            let prepared_write = database
                .prepare_runtime_query(
                    "CREATE (:Memory {id: 'stale-plan'})".into(),
                    &Default::default(),
                )
                .unwrap();
            database
                .query_sql("INSERT INTO documents (id, content) VALUES (2, 'later-child')")
                .unwrap();
            let snapshot = database.begin_read_transaction();
            database
                .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(grandchild)])
                .unwrap();
            assert!(database
                .query_prepared_with_params(prepared_write, &Default::default())
                .unwrap_err()
                .to_string()
                .contains("different branch"));
            let inherited = database
                .query_sql("SELECT id, content, tag FROM documents ORDER BY id")
                .unwrap();
            assert_eq!(inherited.rows.len(), 1);
            assert_eq!(inherited.rows[0]["content"], Value::String(overflow_body));
            assert_eq!(
                snapshot
                    .query_sql("SELECT id FROM documents ORDER BY id")
                    .unwrap()
                    .rows
                    .len(),
                2
            );
            assert_eq!(
                snapshot.query_sql("SHOW CURRENT BRANCH").unwrap().rows[0]["branch_id"],
                Value::Uuid(child)
            );
            assert!(matches!(
                database.query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child)]),
                Err(HawDBError::BranchBusy { .. })
            ));
            assert_eq!(
                database.query_sql("SHOW CURRENT BRANCH").unwrap().rows[0]["branch_id"],
                Value::Uuid(grandchild)
            );
            drop(snapshot);
            database
                .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child)])
                .unwrap();
            assert_eq!(
                database
                    .query_sql("SELECT id FROM documents ORDER BY id")
                    .unwrap()
                    .rows
                    .len(),
                2
            );
            assert!(database
                .query_sql_with_params(
                    "DROP BRANCH ID $1 AT REVISION $2",
                    &[Value::Uuid(child), Value::Int(2)]
                )
                .is_err());
            database.query_sql("USE BRANCH NAME 'main'").unwrap();
            assert!(database.query_sql("SELECT id FROM documents").is_err());
            drop(database);
            let mut database = Database::open_with_durability(&path, durability).unwrap();
            database
                .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(grandchild)])
                .unwrap();
            assert_eq!(
                database
                    .query_sql("SELECT id FROM documents")
                    .unwrap()
                    .rows
                    .len(),
                1
            );
            drop(database);
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn sql_branch_descriptor_rejection_preserves_source_and_releases_admission_quota() {
        let path = test_directory("sql-branch-fd-admission");
        let config = crate::DatabaseConfig {
            max_open_files: 32,
            ..Default::default()
        };
        let mut database = Database::open_with_config(&path, config).unwrap();
        database.checkpoint().unwrap();
        database.query("CREATE (:Memory {id: 'source'})").unwrap();
        let main = database
            .initialize_main_branch(Uuid::from_u128(11), Uuid::from_u128(12))
            .unwrap();
        let child = database.create_branch(create_request(&main)).unwrap();
        database.query_sql("USE BRANCH main").unwrap();
        let target_head = database.branch_head_path(child.id).unwrap();
        let head_before = std::fs::read(&target_head).unwrap();
        let runtime = database.branch_directory(child.id).unwrap().join("runtime");
        assert!(!runtime.exists());
        let mut held = Vec::new();
        while database.file_descriptor_metrics().unwrap().open < 32
            || database.file_descriptor_metrics().unwrap().cached_handles > 0
        {
            held.push(
                hawdb_storage::file_io::File::create(path.join(format!("held-{}", held.len())))
                    .unwrap(),
            );
        }
        let held_count = held.len();
        for query in ["SHOW BRANCHES LIMIT 2", "SHOW BRANCH NAME 'main'"] {
            let result = database.query_sql(query);
            assert!(
                matches!(
                    &result,
                    Err(HawDBError::FileDescriptors(
                        hawdb_core::error::FileDescriptorError::BudgetExceeded {
                            available: 0,
                            limit: 32,
                            ..
                        }
                    ))
                ),
                "unexpected result for {query}: {result:?}"
            );
        }
        assert_eq!(
            database
                .query_sql("SHOW CURRENT BRANCH")
                .unwrap()
                .rows
                .len(),
            1
        );
        for _ in 0..4 {
            drop(held.pop().unwrap());
        }
        let before = database.file_descriptor_metrics().unwrap();
        let error = database
            .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child.id)])
            .unwrap_err();
        assert!(
            matches!(
                error,
                HawDBError::FileDescriptors(
                    hawdb_core::error::FileDescriptorError::BudgetExceeded {
                        requested: 23,
                        available: 4,
                        limit: 32
                    }
                )
            ),
            "unexpected rejection: {error:?}"
        );
        let after = database.file_descriptor_metrics().unwrap();
        assert_eq!(after.open, before.open);
        assert_eq!(after.reserved, 0);
        assert_eq!(std::fs::read(&target_head).unwrap(), head_before);
        assert!(!runtime.exists());
        assert_eq!(database.current_branch().unwrap().info.id, main.id);
        database
            .query("CREATE (:Memory {id: 'after-rejection'})")
            .unwrap();
        assert_eq!(
            database
                .query("MATCH (m:Memory) RETURN m.id")
                .unwrap()
                .rows
                .len(),
            2
        );
        drop(held);
        assert!(held_count > 4);
        for _ in 0..3 {
            database.query_sql("USE BRANCH child").unwrap();
            assert_eq!(
                database
                    .query("MATCH (m:Memory) RETURN m.id")
                    .unwrap()
                    .rows
                    .len(),
                1
            );
            let metrics = database.file_descriptor_metrics().unwrap();
            assert_eq!(metrics.reserved, 0);
            assert_eq!(metrics.ownership_locks, 2);
            assert_eq!(metrics.admitted_runtimes, 1);
            database.query_sql("USE BRANCH main").unwrap();
            assert_eq!(
                database
                    .query("MATCH (m:Memory) RETURN m.id")
                    .unwrap()
                    .rows
                    .len(),
                2
            );
            let metrics = database.file_descriptor_metrics().unwrap();
            assert_eq!(metrics.reserved, 0);
            assert_eq!(metrics.ownership_locks, 2);
            assert!(metrics.high_water <= 32);
        }
        let mut snapshot = database.begin_read_transaction();
        database.query_sql("USE BRANCH child").unwrap();
        let metrics = database.file_descriptor_metrics().unwrap();
        assert_eq!(metrics.admitted_runtimes, 2);
        // The snapshot retains the UUID lease. Its source runtime directory
        // lock and mutable WAL close with the original execution context.
        assert_eq!(metrics.ownership_locks, 3);
        assert_eq!(
            snapshot
                .query("MATCH (m:Memory) RETURN m.id")
                .unwrap()
                .rows
                .len(),
            2
        );
        drop(snapshot);
        let metrics = database.file_descriptor_metrics().unwrap();
        assert_eq!(metrics.admitted_runtimes, 1);
        assert_eq!(metrics.ownership_locks, 2);
        assert_eq!(metrics.reserved, 0);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn sql_branch_failed_selection_and_transaction_rejection_preserve_source() {
        let (path, mut database, main) = initialized_database();
        let child = database.create_branch(create_request(&main)).unwrap();
        database.query_sql("USE BRANCH NAME 'main'").unwrap();
        let lease =
            DatabaseDirectoryLease::acquire(&database.branch_directory(child.id).unwrap()).unwrap();
        assert!(matches!(
            database.query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child.id)]),
            Err(HawDBError::BranchBusy { .. })
        ));
        database
            .query("CREATE (:Memory {id: 'after-busy'})")
            .unwrap();
        drop(lease);
        let head_path = database.branch_head_path(child.id).unwrap();
        let head = std::fs::read(&head_path).unwrap();
        std::fs::write(&head_path, b"corrupt").unwrap();
        assert!(database
            .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child.id)])
            .is_err());
        assert!(database.query_sql("USE BRANCH NAME 'missing'").is_err());
        assert_eq!(
            database.query_sql("SHOW CURRENT BRANCH").unwrap().rows[0]["branch_id"],
            Value::Uuid(main.id)
        );
        database
            .query_sql("CREATE TABLE source_probe (id BIGINT PRIMARY KEY)")
            .unwrap();
        let mut transaction = database.begin_transaction();
        transaction
            .query_sql("INSERT INTO source_probe (id) VALUES (1)")
            .unwrap();
        for sql in [
            "USE BRANCH NAME 'main'",
            "CREATE BRANCH NAME 'forbidden' FROM NAME 'main' AT REVISION 1 REQUEST KEY 'forbidden'",
            "DROP BRANCH ID '00000000-0000-0000-0000-000000000012' AT REVISION 2",
        ] {
            assert!(matches!(
                transaction.query_sql(sql),
                Err(HawDBError::BranchCommandUnsupported {
                    context: "explicit transaction",
                    ..
                })
            ));
        }
        assert_eq!(
            transaction
                .query_sql("SELECT id FROM source_probe")
                .unwrap()
                .rows
                .len(),
            1
        );
        transaction.commit().unwrap();
        std::fs::write(&head_path, head).unwrap();
        {
            let mut session = database.session();
            session.query("BEGIN TRANSACTION").unwrap();
            assert!(matches!(
                session.query_sql("USE BRANCH NAME 'main'"),
                Err(HawDBError::BranchCommandUnsupported { .. })
            ));
            session.query("ROLLBACK").unwrap();
        }
        let shared = database.into_concurrent();
        assert!(matches!(
            shared.clone().query_sql("USE BRANCH NAME 'main'"),
            Err(HawDBError::BranchCommandUnsupported {
                context: "shared concurrent runtime",
                ..
            })
        ));
        assert_eq!(
            shared.query_sql("SHOW CURRENT BRANCH").unwrap().rows[0]["branch_id"],
            Value::Uuid(main.id)
        );
        assert_eq!(
            shared
                .query_sql("SELECT id FROM source_probe")
                .unwrap()
                .rows
                .len(),
            1
        );
        drop(shared);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn sql_branch_budget_rejection_and_exact_delete_survive_name_reuse() {
        let (path, mut database, main) = initialized_database();
        let sql = "CREATE BRANCH NAME $1 FROM ID $2 AT REVISION $3 REQUEST KEY $4";
        let mut parameters = [
            Value::String("reusable".into()),
            Value::Uuid(main.id),
            Value::Int(main.source_commit_epoch as i64),
            Value::String("first-identity".into()),
        ];
        let catalog_path = database.branch_catalog_path().unwrap();
        let before = std::fs::read(&catalog_path).unwrap();
        for options in [
            super::super::QueryStreamOptions {
                max_rows: Some(0),
                max_payload_bytes: None,
            },
            super::super::QueryStreamOptions {
                max_rows: None,
                max_payload_bytes: Some(1),
            },
        ] {
            assert!(database
                .query_sql_with_params_options(sql, &parameters, options)
                .is_err());
            assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
        }
        let created = database.query_sql_with_params(sql, &parameters).unwrap();
        let Value::Uuid(first) = created.rows[0]["branch_id"] else {
            panic!("created UUID")
        };
        let revision = created.rows[0]["metadata_revision"].clone();
        let drop_sql = "DROP BRANCH ID $1 AT REVISION $2";
        assert!(database
            .query_sql_with_params(drop_sql, &[Value::Uuid(first), Value::Int(1)])
            .is_err());
        assert_eq!(
            database
                .describe_branch(BranchSelector::Id(first))
                .unwrap()
                .state,
            BranchLifecycleState::Ready
        );
        let deleted = database
            .query_sql_with_params(drop_sql, &[Value::Uuid(first), revision.clone()])
            .unwrap();
        assert_eq!(deleted.rows[0]["state"], Value::String("deleted".into()));
        parameters[3] = Value::String("second-identity".into());
        let recreated = database.query_sql_with_params(sql, &parameters).unwrap();
        assert_ne!(recreated.rows[0]["branch_id"], Value::Uuid(first));
        assert_eq!(
            database
                .query_sql_with_params(drop_sql, &[Value::Uuid(first), revision])
                .unwrap(),
            deleted
        );
        assert_eq!(
            database
                .describe_branch(BranchSelector::Name("reusable".into()))
                .unwrap()
                .state,
            BranchLifecycleState::Ready
        );
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn sql_branch_readonly_selection_reconstructs_private_wal_without_logical_writes() {
        let (path, mut writer, main) = initialized_database();
        let child = writer.create_branch(create_request(&main)).unwrap();
        writer
            .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child.id)])
            .unwrap();
        writer
            .query_sql("CREATE TABLE readonly_probe (id BIGINT PRIMARY KEY)")
            .unwrap();
        writer
            .query_sql("INSERT INTO readonly_probe (id) VALUES (1)")
            .unwrap();
        let head_path = writer.branch_head_path(child.id).unwrap();
        let head = std::fs::read(&head_path).unwrap();
        let active = branch_head::read_branch_head(&head_path).unwrap();
        let wal_path = writer
            .branch_wal_path(child.id, active.active_wal.generation)
            .unwrap();
        let wal = std::fs::read(&wal_path).unwrap();
        let catalog_path = writer.branch_catalog_path().unwrap();
        let catalog = std::fs::read(&catalog_path).unwrap();
        drop(writer);
        let mut reader = Database::open_with_config(
            &path,
            super::super::DatabaseConfig {
                read_only: true,
                ..Default::default()
            },
        )
        .unwrap();
        reader.query_sql("USE BRANCH NAME 'child'").unwrap();
        assert_eq!(
            reader
                .query_sql("SELECT id FROM readonly_probe")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert!(reader
            .query_sql("INSERT INTO readonly_probe (id) VALUES (2)")
            .is_err());
        assert!(reader
            .query_sql("CREATE TABLE forbidden (id BIGINT PRIMARY KEY)")
            .is_err());
        assert!(reader.checkpoint().is_err());
        let epoch = reader.commit_epoch();
        assert!(reader
            .store
            .seal_admitted_branch(epoch)
            .unwrap_err()
            .to_string()
            .contains("read-only"));
        assert_eq!(std::fs::read(&catalog_path).unwrap(), catalog);
        assert_eq!(std::fs::read(&head_path).unwrap(), head);
        assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
        reader.query_sql("USE BRANCH main").unwrap();
        assert!(reader.query_sql("SELECT id FROM readonly_probe").is_err());
        drop(reader);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn sql_branch_contexts_and_unselected_sources_keep_independent_ownership() {
        let (path, mut parent_context, main) = initialized_database();
        let child = parent_context.create_branch(create_request(&main)).unwrap();
        parent_context.query_sql("USE BRANCH main").unwrap();
        let mut child_context = Database::open(&path).unwrap();
        child_context.query_sql("USE BRANCH child").unwrap();
        assert!(matches!(
            child_context.query_sql("USE BRANCH main"),
            Err(HawDBError::BranchBusy { .. })
        ));
        let fork = "CREATE BRANCH experiment FROM ID $1 AT REVISION $2 REQUEST KEY $3";
        assert!(matches!(
            child_context.query_sql_with_params(
                fork,
                &[
                    Value::Uuid(main.id),
                    Value::Int(main.source_commit_epoch as i64),
                    Value::String("busy-source".into())
                ]
            ),
            Err(HawDBError::BranchBusy { .. })
        ));
        child_context
            .query("CREATE (:Memory {id: 'child-write'})")
            .unwrap();
        child_context
            .query_sql("CREATE TABLE fork_payload (id BIGINT PRIMARY KEY, body TEXT)")
            .unwrap();
        let body = "closed-source-data".repeat(4096);
        child_context
            .query_sql_with_params(
                "INSERT INTO fork_payload (id, body) VALUES (1, $1)",
                &[Value::String(body.clone())],
            )
            .unwrap();
        assert_eq!(
            parent_context
                .query("MATCH (m:Memory) RETURN m.id")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert_eq!(
            child_context
                .query("MATCH (m:Memory) RETURN m.id")
                .unwrap()
                .rows
                .len(),
            2
        );
        let job = child_context.schedule_derived_artifact_rebuild();
        assert!(matches!(
            child_context.query_sql("USE BRANCH main"),
            Err(HawDBError::BranchBusy {
                resource: "unfinished background work"
            })
        ));
        assert_eq!(child_context.derived_artifact_jobs(), vec![job]);
        child_context.run_next_derived_artifact_job().unwrap();
        let epoch = child_context.commit_epoch();
        drop(child_context);
        let source_directory = parent_context.branch_directory(child.id).unwrap();
        std::fs::remove_dir_all(source_directory.join("runtime")).unwrap();
        std::fs::write(
            source_directory.join("runtime"),
            b"forbid runtime admission",
        )
        .unwrap();
        let created = parent_context
            .query_sql_with_params(
                fork,
                &[
                    Value::Uuid(child.id),
                    Value::Int(epoch as i64),
                    Value::String("unselected-source".into()),
                ],
            )
            .unwrap();
        assert_eq!(
            parent_context
                .query_sql("SHOW CURRENT BRANCH")
                .unwrap()
                .rows[0]["branch_id"],
            Value::Uuid(main.id)
        );
        let Value::Uuid(experiment) = created.rows[0]["branch_id"] else {
            panic!("created UUID")
        };
        assert_eq!(
            std::fs::read(source_directory.join("runtime")).unwrap(),
            b"forbid runtime admission"
        );
        std::fs::remove_dir_all(source_directory).unwrap();
        parent_context
            .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(experiment)])
            .unwrap();
        assert_eq!(
            parent_context
                .query("MATCH (m:Memory) RETURN m.id")
                .unwrap()
                .rows
                .len(),
            2
        );
        assert_eq!(
            parent_context
                .query_sql("SELECT body FROM fork_payload WHERE id = 1")
                .unwrap()
                .rows[0]["body"],
            Value::String(body)
        );
        drop(parent_context);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn initializes_and_lists_a_typed_root_branch() {
        let path = test_directory("branch-api");
        let database = Database::open(&path).expect("open database");
        let main = database
            .initialize_branch_catalog(Uuid::from_u128(1), Uuid::from_u128(2))
            .expect("initialize branch catalog");
        assert_eq!(main.id, Uuid::from_u128(2));
        assert_eq!(main.state, BranchLifecycleState::Ready);
        assert_eq!(database.list_branches().unwrap(), vec![main.clone()]);
        assert_eq!(
            database
                .describe_branch(BranchSelector::Id(main.id))
                .unwrap(),
            main
        );
        assert!(matches!(
            database.delete_branch(BranchSelector::Id(main.id)),
            Err(BranchLifecycleError::RootBranchImmutable)
        ));
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn seals_and_publishes_the_initial_main_head_explicitly() {
        let path = test_directory("main-head");
        let mut database = Database::open(&path).expect("open database");
        database.checkpoint().expect("create initial checkpoint");
        database
            .query("CREATE (:Memory {id: 'branch-root'})")
            .expect("create initial graph state");
        let main = database
            .initialize_main_branch(Uuid::from_u128(11), Uuid::from_u128(12))
            .expect("publish initial main head");
        assert_eq!(main.name, "main");
        assert!(path
            .join("branches")
            .join(main.id.to_string())
            .join("branch.head")
            .is_file());
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn admits_a_facade_created_child_from_its_private_wal() {
        let (path, mut database, main) = initialized_database();
        let child = database
            .create_branch(create_request(&main))
            .expect("create child through facade");
        let catalog_path = database.branch_catalog_path().expect("branch catalog path");
        let child_record = database
            .read_branch_catalog()
            .expect("read child catalog record")
            .branches
            .into_iter()
            .find(|branch| branch.id.as_uuid() == child.id)
            .expect("child catalog record");
        let head_path = database
            .branch_head_path(child.id)
            .expect("child head path");
        let immutable_store_root = catalog_path
            .parent()
            .expect("branch catalog parent")
            .join("objects");

        let admitted = GraphStore::admit_branch_from_head(BranchAdmissionRequest {
            catalog_path: &catalog_path,
            branch_id: child_record.id,
            expected_metadata_revision: child_record.metadata_revision,
            head_path: &head_path,
            immutable_store_root: &immutable_store_root,
            durability: DurabilityPolicy::default(),
            replay_config: WalReplayConfig::default(),
        })
        .expect("admit child through its facade-created private WAL");

        assert_eq!(
            admitted.head().branch_id,
            *child.id.as_bytes(),
            "admission must select the child head created by the facade"
        );
        assert_eq!(admitted.store().node_count_for_label(None), 1);
        drop(admitted);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    fn initialized_database() -> (PathBuf, Database, BranchInfo) {
        let path = test_directory("branch-retry");
        let mut database = Database::open(&path).unwrap();
        database.checkpoint().unwrap();
        database.query("CREATE (:Memory {id: 'root'})").unwrap();
        let main = database
            .initialize_main_branch(Uuid::from_u128(11), Uuid::from_u128(12))
            .unwrap();
        (path, database, main)
    }

    fn create_request(main: &BranchInfo) -> BranchCreateRequest {
        BranchCreateRequest {
            name: Some("child".to_string()),
            parent: BranchSelector::Id(main.id),
            expected_source_commit_epoch: main.source_commit_epoch,
            owner: None,
            idempotency_key: "create-child".to_string(),
        }
    }

    #[test]
    fn inspects_the_durable_branch_catalog_through_sql() {
        let (path, mut database, main) = initialized_database();
        let child = database.create_branch(create_request(&main)).unwrap();

        let listed = database
            .query_sql_with_params(
                "SHOW BRANCHES LIMIT $1 OFFSET $2",
                &[Value::Int(1), Value::Int(1)],
            )
            .unwrap();
        assert_eq!(listed.rows.len(), 1);
        assert_eq!(
            listed.rows[0].get("branch_id"),
            Some(&Value::Uuid(child.id))
        );
        assert_eq!(
            listed.rows[0].get("name"),
            Some(&Value::String(child.name.clone()))
        );

        let described = database
            .query_sql_with_params("SHOW BRANCH NAME $1", &[Value::String(child.name.clone())])
            .unwrap();
        assert_eq!(described.rows.len(), 1);
        assert_eq!(
            described.rows[0].get("parent_id"),
            Some(&Value::Uuid(main.id))
        );

        let described_by_id = database
            .query_sql_with_params("SHOW BRANCH ID $1", &[Value::String(child.id.to_string())])
            .unwrap();
        assert_eq!(
            described_by_id.rows[0].get("name"),
            Some(&Value::String(child.name.clone()))
        );

        assert!(database
            .query_sql_with_params_options(
                "SHOW BRANCHES LIMIT $1",
                &[Value::Int(2)],
                super::super::QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: None,
                },
            )
            .is_err());
        let large_page = database
            .query_sql_with_params_options(
                "SHOW BRANCHES OFFSET 0 LIMIT 1_000",
                &[],
                super::super::QueryStreamOptions {
                    max_rows: Some(2),
                    max_payload_bytes: None,
                },
            )
            .unwrap();
        assert_eq!(large_page.rows.len(), 2);
        let cache_before = database.relational_plan_template_cache_stats();
        database.query_sql("SHOW BRANCHES LIMIT 2").unwrap();
        let cache_after_miss = database.relational_plan_template_cache_stats();
        database.query_sql("SHOW BRANCHES LIMIT 2").unwrap();
        let cache_after_hit = database.relational_plan_template_cache_stats();
        assert_eq!(cache_after_miss.entries, cache_before.entries + 1);
        assert_eq!(cache_after_hit.hits, cache_after_miss.hits + 1);
        assert!(database
            .query_sql_with_params_options(
                "SHOW BRANCHES LIMIT $1",
                &[Value::Int(1)],
                super::super::QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(1),
                },
            )
            .is_err());
        for value in [Value::Int(-1), Value::String("one".to_string())] {
            let error = database
                .query_sql_with_params("SHOW BRANCHES LIMIT $1", &[value])
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("PostgreSQL branch SQL parameter $1 must be a non-negative integer"));
        }
        assert!(database
            .query_sql_with_params("SHOW BRANCHES LIMIT $1 OFFSET $2", &[Value::Int(1)])
            .unwrap_err()
            .to_string()
            .contains("missing PostgreSQL parameter $2"));
        assert!(database
            .query_sql_with_params(
                "SHOW BRANCH NAME $1",
                &[Value::String("missing".to_string())]
            )
            .unwrap_err()
            .to_string()
            .contains("branch does not exist"));

        let mut transaction = database.begin_transaction();
        assert_eq!(
            transaction
                .query_sql("SHOW BRANCHES LIMIT 2")
                .unwrap()
                .rows
                .len(),
            2
        );
        assert_eq!(
            transaction
                .query_sql_with_params("SHOW BRANCH NAME $1", &[Value::String(child.name.clone())],)
                .unwrap()
                .rows[0]
                .get("branch_id"),
            Some(&Value::Uuid(child.id))
        );
        transaction.commit().unwrap();

        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn concurrent_branch_inspection_uses_a_snapshot_and_executes_in_transactions() {
        let (path, mut database, main) = initialized_database();
        let child = database.create_branch(create_request(&main)).unwrap();
        let database = database.into_concurrent();
        let (snapshot_acquired, snapshots) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        database
            .set_autocommit_read_gate(snapshot_acquired, Arc::clone(&release))
            .unwrap();
        let reader = database.clone();
        let inspection = std::thread::spawn(move || reader.query_sql("SHOW BRANCHES LIMIT 2"));
        snapshots
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("branch inspection must acquire a read snapshot");
        database.clear_autocommit_read_gate().unwrap();
        let (released, available) = &*release;
        *released.lock().unwrap() = true;
        available.notify_all();
        assert_eq!(inspection.join().unwrap().unwrap().rows.len(), 2);

        let mut transaction = database
            .begin_transaction(ConcurrentTransactionOptions::default())
            .unwrap();
        assert_eq!(
            transaction
                .query_sql_with_params("SHOW BRANCH NAME $1", &[Value::String(child.name.clone())],)
                .unwrap()
                .rows[0]
                .get("branch_id"),
            Some(&Value::Uuid(child.id))
        );
        transaction
            .query_sql("CREATE TABLE branch_lock_probe (id BIGINT PRIMARY KEY)")
            .unwrap();
        transaction.commit().unwrap();
        assert!(database
            .query_sql("SELECT id FROM branch_lock_probe")
            .unwrap()
            .rows
            .is_empty());
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn main_initialization_preserves_writes_across_reopen() {
        let (path, mut database, main) = initialized_database();
        let head_path = database.branch_head_path(main.id).unwrap();
        let head = branch_head::read_branch_head(&head_path).unwrap();
        database
            .query("CREATE (:Memory {id: 'after-bootstrap'})")
            .unwrap();
        let before = database
            .query("MATCH (m:Memory) RETURN m.id ORDER BY m.id")
            .unwrap();
        drop(database);
        let mut database = Database::open(&path).unwrap();
        let after = database
            .query("MATCH (m:Memory) RETURN m.id ORDER BY m.id")
            .unwrap();
        assert_eq!(before.rows, after.rows);
        database.checkpoint().unwrap();
        assert_eq!(branch_head::read_branch_head(&head_path).unwrap(), head);
        database.create_branch(create_request(&main)).unwrap();
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn create_rejects_changed_owner_with_the_same_key() {
        let (path, mut database, main) = initialized_database();
        let mut request = create_request(&main);
        database.create_branch(request.clone()).unwrap();
        request.owner = Some("another-owner".to_string());
        assert!(matches!(
            database.create_branch(request),
            Err(BranchLifecycleError::Transition(
                storage::CatalogTransitionError::Conflict(_)
            ))
        ));
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn main_initialization_repairs_interrupted_catalog_binding() {
        let (path, mut database, main) = initialized_database();
        let catalog_path = database.branch_catalog_path().unwrap();
        let mut catalog = database.read_branch_catalog().unwrap();
        catalog.branches[0].base_root_digest = None;
        catalog.branches[0].source_commit_epoch = 0;
        storage::write_catalog(&catalog_path, &catalog).unwrap();
        let retried = database
            .initialize_main_branch(Uuid::from_u128(11), main.id)
            .unwrap();
        assert_eq!(retried.source_commit_epoch, main.source_commit_epoch);
        database.create_branch(create_request(&main)).unwrap();
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn creates_custom_and_generated_branches_and_replays_without_parent_files() {
        let (path, mut database, main) = initialized_database();
        let custom = database.create_branch(create_request(&main)).unwrap();
        assert_eq!(
            database
                .describe_branch(BranchSelector::Name(custom.name.clone()))
                .unwrap(),
            custom
        );
        let mut request = create_request(&custom);
        request.name = None;
        request.idempotency_key = "generated-child".to_string();
        let child = database.create_branch(request.clone()).unwrap();
        assert_eq!(child.name, format!("agent/{}", child.id));
        let deleted = database
            .delete_branch(BranchSelector::Id(custom.id))
            .unwrap();
        assert_eq!(deleted.state, BranchLifecycleState::Deleted);
        std::fs::remove_file(database.branch_head_path(custom.id).unwrap()).unwrap();
        assert_eq!(database.create_branch(request).unwrap(), child);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn delete_resumes_a_durable_deleting_transition_after_restart_boundary() {
        let (path, mut database, main) = initialized_database();
        let child = database.create_branch(create_request(&main)).unwrap();
        let catalog_path = database.branch_catalog_path().unwrap();
        let child_record = database
            .read_branch_catalog()
            .unwrap()
            .branches
            .into_iter()
            .find(|branch| branch.id.as_uuid() == child.id)
            .unwrap();
        let reservation = match storage::begin_delete_file(
            &catalog_path,
            storage::DeleteRequest {
                id: child_record.id,
                expected_metadata_revision: child_record.metadata_revision,
            },
        )
        .unwrap()
        {
            storage::DeleteBeginOutcome::Deleting(reservation) => reservation,
            storage::DeleteBeginOutcome::Deleted(_) => panic!("child must begin deletion"),
        };
        assert!(matches!(
            database
                .read_branch_catalog()
                .unwrap()
                .branches
                .iter()
                .find(|branch| branch.id == child_record.id)
                .map(|branch| branch.state),
            Some(storage::BranchState::Deleting)
        ));

        drop(database);
        let database = Database::open(&path).unwrap();
        let deleted = database
            .delete_branch(BranchSelector::Id(child.id))
            .unwrap();
        assert_eq!(deleted.state, BranchLifecycleState::Deleted);
        assert_eq!(
            database
                .delete_branch(BranchSelector::Id(child.id))
                .unwrap(),
            deleted
        );
        assert_eq!(
            storage::finish_delete_file(&catalog_path, reservation)
                .unwrap()
                .state,
            storage::BranchState::Deleted
        );
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn delete_rejects_a_branch_with_a_live_writer_lease() {
        let (path, mut database, main) = initialized_database();
        let child = database.create_branch(create_request(&main)).unwrap();
        let lease =
            DatabaseDirectoryLease::acquire(&database.branch_directory(child.id).unwrap()).unwrap();
        assert!(matches!(
            database.delete_branch(BranchSelector::Id(child.id)),
            Err(BranchLifecycleError::LeaseUnavailable(_))
        ));
        drop(lease);
        assert_eq!(
            database
                .delete_branch(BranchSelector::Id(child.id))
                .unwrap()
                .state,
            BranchLifecycleState::Deleted
        );
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn delete_retry_reports_pending_while_deleting_branch_is_leased() {
        let (path, mut database, main) = initialized_database();
        let child = database.create_branch(create_request(&main)).unwrap();
        let catalog_path = database.branch_catalog_path().unwrap();
        let child_record = database
            .read_branch_catalog()
            .unwrap()
            .branches
            .into_iter()
            .find(|branch| branch.id.as_uuid() == child.id)
            .unwrap();
        let lease =
            DatabaseDirectoryLease::acquire(&database.branch_directory(child.id).unwrap()).unwrap();
        storage::begin_delete_file(
            &catalog_path,
            storage::DeleteRequest {
                id: child_record.id,
                expected_metadata_revision: child_record.metadata_revision,
            },
        )
        .unwrap();
        let before = std::fs::read(&catalog_path).unwrap();

        let pending = database
            .delete_branch(BranchSelector::Id(child.id))
            .unwrap();
        assert_eq!(pending.id, child.id);
        assert_eq!(pending.state, BranchLifecycleState::Deleting);
        assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
        assert!(matches!(
            DatabaseDirectoryLease::acquire(&database.branch_directory(child.id).unwrap()),
            Err(hawdb_storage::ownership::DatabaseDirectoryLeaseError::AlreadyOpen)
        ));
        drop(lease);
        assert_eq!(
            database
                .delete_branch(BranchSelector::Id(child.id))
                .unwrap()
                .state,
            BranchLifecycleState::Deleted
        );
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn persistent_lineage_and_receipts_survive_reopen_without_handles() {
        let (path, mut database, main) = initialized_database();
        let request = create_request(&main);
        let child = database.create_branch(request.clone()).unwrap();
        let mut descendant_request = create_request(&child);
        descendant_request.name = Some("descendant".to_string());
        descendant_request.idempotency_key = "create-descendant".to_string();
        let descendant = database.create_branch(descendant_request.clone()).unwrap();
        let catalog_path = database.branch_catalog_path().unwrap();
        let before = std::fs::read(&catalog_path).unwrap();
        drop(database);

        let mut database = Database::open(&path).unwrap();
        assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
        assert_eq!(database.create_branch(request.clone()).unwrap(), child);
        assert_eq!(
            database.create_branch(descendant_request.clone()).unwrap(),
            descendant
        );
        assert_eq!(database.list_branches().unwrap().len(), 3);
        let deleted = database
            .delete_branch(BranchSelector::Id(child.id))
            .unwrap();
        assert_eq!(deleted.state, BranchLifecycleState::Deleted);
        drop(database);

        let mut database = Database::open(&path).unwrap();
        assert_eq!(database.create_branch(request).unwrap(), deleted);
        assert_eq!(
            database.create_branch(descendant_request).unwrap(),
            descendant
        );
        assert_eq!(
            database
                .describe_branch(BranchSelector::Id(descendant.id))
                .unwrap()
                .state,
            BranchLifecycleState::Ready
        );
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn create_retry_completes_a_pending_durable_child() {
        let (path, mut database, main) = initialized_database();
        let request = create_request(&main);
        let child = database.create_branch(request.clone()).unwrap();
        let mut catalog = database.read_branch_catalog().unwrap();
        let branch = catalog
            .branches
            .iter_mut()
            .find(|branch| branch.id.as_uuid() == child.id)
            .unwrap();
        branch.state = storage::BranchState::Creating;
        branch.create_outcome = storage::CreateOutcome::Pending;
        storage::write_catalog(&database.branch_catalog_path().unwrap(), &catalog).unwrap();
        let mut completed = child;
        completed.metadata_revision += 1;
        assert_eq!(database.create_branch(request).unwrap(), completed);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn sql_pending_create_recovers_original_child_after_parent_advance_delete_and_name_reuse() {
        for durability in [
            DurabilityPolicy::SyncOnEveryWrite,
            DurabilityPolicy::SyncOnCheckpoint,
        ] {
            for delete_parent in [false, true] {
                let path = test_directory("pending-source-changes");
                let mut database = Database::open_with_durability(&path, durability).unwrap();
                database.checkpoint().unwrap();
                let main = database
                    .initialize_main_branch(Uuid::from_u128(11), Uuid::from_u128(12))
                    .unwrap();
                database.query_sql("USE BRANCH main").unwrap();
                let child = database.create_branch(create_request(&main)).unwrap();
                database.query_sql("USE BRANCH child").unwrap();
                database
                    .query_sql("CREATE TABLE receipt (id BIGINT PRIMARY KEY)")
                    .unwrap();
                database
                    .query_sql("INSERT INTO receipt (id) VALUES (7)")
                    .unwrap();
                let epoch = database.commit_epoch();
                let request = BranchCreateRequest {
                    name: Some("original-grandchild".into()),
                    parent: BranchSelector::Name("child".into()),
                    expected_source_commit_epoch: epoch,
                    owner: None,
                    idempotency_key: "pending-original-child".into(),
                };
                let grandchild = database.create_branch(request.clone()).unwrap();
                let catalog_path = database.branch_catalog_path().unwrap();
                let mut catalog = database.read_branch_catalog().unwrap();
                let reserved = catalog
                    .branches
                    .iter_mut()
                    .find(|record| record.id.as_uuid() == grandchild.id)
                    .unwrap();
                reserved.state = storage::BranchState::Creating;
                reserved.create_outcome = storage::CreateOutcome::Pending;
                storage::write_catalog(&catalog_path, &catalog).unwrap();
                database
                    .query_sql("INSERT INTO receipt (id) VALUES (8)")
                    .unwrap();
                database.query_sql("USE BRANCH main").unwrap();
                if delete_parent {
                    database
                        .delete_branch(BranchSelector::Id(child.id))
                        .unwrap();
                    std::fs::remove_dir_all(database.branch_directory(child.id).unwrap()).unwrap();
                    let mut replacement = create_request(&main);
                    replacement.idempotency_key = "replacement-parent".into();
                    assert_ne!(database.create_branch(replacement).unwrap().id, child.id);
                }
                let before = std::fs::read(&catalog_path).unwrap();
                let mut conflicting = request.clone();
                conflicting.expected_source_commit_epoch += 1;
                assert!(database.create_branch(conflicting).is_err());
                assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
                let result = database
                    .query_sql_with_params(
                        "CREATE BRANCH NAME $1 FROM NAME $2 AT REVISION $3 REQUEST KEY $4",
                        &[
                            Value::String(request.name.unwrap()),
                            Value::String("child".into()),
                            Value::Int(epoch as i64),
                            Value::String(request.idempotency_key),
                        ],
                    )
                    .unwrap();
                assert_eq!(result.rows[0]["branch_id"], Value::Uuid(grandchild.id));
                assert_eq!(result.rows[0]["parent_id"], Value::Uuid(child.id));
                database
                    .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(grandchild.id)])
                    .unwrap();
                assert_eq!(
                    database.query_sql("SELECT id FROM receipt").unwrap().rows,
                    vec![BTreeMap::from([("id".into(), Value::Int(7))])]
                );
                drop(database);
                std::fs::remove_dir_all(path).unwrap();
            }
        }
    }

    #[test]
    fn pending_create_recovery_respects_child_lease_and_aborts_only_known_missing_pairs() {
        for missing_head in [false, true] {
            let (path, mut database, main) = initialized_database();
            let request = create_request(&main);
            let child = database.create_branch(request.clone()).unwrap();
            let head_path = database.branch_head_path(child.id).unwrap();
            let head = branch_head::read_branch_head(&head_path).unwrap();
            let wal_path = database
                .branch_wal_path(child.id, head.active_wal.generation)
                .unwrap();
            let mut catalog = database.read_branch_catalog().unwrap();
            let pending = catalog
                .branches
                .iter_mut()
                .find(|branch| branch.id.as_uuid() == child.id)
                .unwrap();
            pending.state = storage::BranchState::Creating;
            pending.create_outcome = storage::CreateOutcome::Pending;
            let catalog_path = database.branch_catalog_path().unwrap();
            storage::write_catalog(&catalog_path, &catalog).unwrap();
            let before = std::fs::read(&catalog_path).unwrap();
            let lease = hawdb_storage::ownership::DatabaseDirectoryLease::acquire(
                head_path.parent().unwrap(),
            )
            .unwrap();
            assert!(matches!(
                database.create_branch(request.clone()),
                Err(BranchLifecycleError::SourceBusy("pending child creation"))
            ));
            assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
            drop(lease);
            let retained = if missing_head {
                std::fs::remove_file(&head_path).unwrap();
                &wal_path
            } else {
                std::fs::remove_file(&wal_path).unwrap();
                &head_path
            };
            let evidence = std::fs::read(retained).unwrap();
            assert!(database.create_branch(request.clone()).is_err());
            let aborted = database.read_branch_catalog().unwrap();
            let record = aborted
                .branches
                .iter()
                .find(|branch| branch.id.as_uuid() == child.id)
                .unwrap();
            assert_eq!(record.create_outcome, storage::CreateOutcome::Aborted);
            assert_eq!(record.state, storage::BranchState::Deleted);
            assert_eq!(std::fs::read(retained).unwrap(), evidence);
            let after = std::fs::read(&catalog_path).unwrap();
            assert!(database.create_branch(request).is_err());
            assert_eq!(std::fs::read(&catalog_path).unwrap(), after);
            drop(database);
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn create_retry_rejects_a_pending_child_with_corrupt_wal() {
        let (path, mut database, main) = initialized_database();
        let request = create_request(&main);
        let child = database.create_branch(request.clone()).unwrap();
        let mut catalog = database.read_branch_catalog().unwrap();
        let branch = catalog
            .branches
            .iter_mut()
            .find(|branch| branch.id.as_uuid() == child.id)
            .unwrap();
        branch.state = storage::BranchState::Creating;
        branch.create_outcome = storage::CreateOutcome::Pending;
        let catalog_path = database.branch_catalog_path().unwrap();
        storage::write_catalog(&catalog_path, &catalog).unwrap();
        let head =
            branch_head::read_branch_head(&database.branch_head_path(child.id).unwrap()).unwrap();
        std::fs::write(
            database
                .branch_wal_path(child.id, head.active_wal.generation)
                .unwrap(),
            b"corrupt",
        )
        .unwrap();
        assert!(database.create_branch(request).is_err());
        assert_eq!(storage::read_catalog(&catalog_path).unwrap(), catalog);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn readonly_branch_mutations_leave_catalog_unchanged() {
        let (path, mut database, main) = initialized_database();
        let child = database.create_branch(create_request(&main)).unwrap();
        let catalog_path = database.branch_catalog_path().unwrap();
        let before = std::fs::read(&catalog_path).unwrap();
        drop(database);
        let mut database = Database::open_with_config(
            &path,
            super::super::DatabaseConfig {
                read_only: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(database.create_branch(create_request(&main)).is_err());
        assert!(database
            .initialize_main_branch(Uuid::from_u128(11), main.id)
            .is_err());
        assert!(database
            .delete_branch(BranchSelector::Id(child.id))
            .is_err());
        assert_eq!(std::fs::read(catalog_path).unwrap(), before);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn initial_head_retry_reuses_only_an_empty_private_wal() {
        let (path, mut database, main) = initialized_database();
        let head_path = database.branch_head_path(main.id).unwrap();
        let head = branch_head::read_branch_head(&head_path).unwrap();
        let wal_path = database
            .branch_wal_path(main.id, head.active_wal.generation)
            .unwrap();
        let catalog_path = database.branch_catalog_path().unwrap();
        let mut catalog = database.read_branch_catalog().unwrap();
        catalog.branches[0].base_root_digest = None;
        catalog.branches[0].source_commit_epoch = 0;
        storage::write_catalog(&catalog_path, &catalog).unwrap();
        std::fs::remove_file(&head_path).unwrap();
        let mut rebound = main.clone();
        rebound.metadata_revision += 1;
        assert_eq!(
            database
                .initialize_main_branch(Uuid::from_u128(11), main.id)
                .unwrap(),
            rebound
        );
        assert_eq!(branch_head::read_branch_head(&head_path).unwrap(), head);
        storage::write_catalog(&catalog_path, &catalog).unwrap();
        std::fs::remove_file(&head_path).unwrap();
        std::fs::write(&wal_path, b"corrupt").unwrap();
        assert!(database
            .initialize_main_branch(Uuid::from_u128(11), main.id)
            .is_err());
        assert!(!head_path.exists());
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchLifecycleState {
    Creating,
    Ready,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchInfo {
    pub id: Uuid,
    pub name: String,
    pub parent_id: Option<Uuid>,
    pub source_commit_epoch: u64,
    pub metadata_revision: u64,
    pub state: BranchLifecycleState,
    pub owner: Option<String>,
}

#[derive(Debug)]
pub enum BranchLifecycleError {
    InMemoryDatabase,
    Storage(String),
    Runtime(HawDBError),
    CatalogIo(std::io::Error),
    Catalog(storage::CatalogError),
    Transition(storage::CatalogTransitionError),
    LeaseUnavailable(hawdb_storage::ownership::DatabaseDirectoryLeaseError),
    UnknownBranch,
    RootBranchImmutable,
    Admission(hawdb_storage::store::BranchAdmissionError),
    SourceBusy(&'static str),
}

impl Display for BranchLifecycleError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InMemoryDatabase => {
                formatter.write_str("branch lifecycle requires a durable database")
            }
            Self::Storage(message) => {
                write!(formatter, "branch storage operation failed: {message}")
            }
            Self::Runtime(error) => Display::fmt(error, formatter),
            Self::CatalogIo(error) => write!(formatter, "branch catalog I/O failed: {error}"),
            Self::Catalog(error) => write!(formatter, "branch catalog is invalid: {error}"),
            Self::Transition(error) => {
                write!(formatter, "branch lifecycle transition failed: {error}")
            }
            Self::LeaseUnavailable(error) => {
                write!(
                    formatter,
                    "branch lifecycle branch lease is unavailable: {error}"
                )
            }
            Self::UnknownBranch => formatter.write_str("branch does not exist"),
            Self::RootBranchImmutable => formatter.write_str("the root branch cannot be deleted"),
            Self::Admission(error) => Display::fmt(error, formatter),
            Self::SourceBusy(resource) => write!(formatter, "source branch is busy: {resource}"),
        }
    }
}

impl std::error::Error for BranchLifecycleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::CatalogIo(error) => Some(error),
            Self::Catalog(error) => Some(error),
            Self::Transition(error) => Some(error),
            Self::LeaseUnavailable(error) => Some(error),
            Self::Admission(error) => Some(error),
            _ => None,
        }
    }
}

impl BranchLifecycleError {
    fn storage(error: impl std::error::Error + 'static) -> Self {
        Self::Runtime(HawDBError::from_storage_error(error))
    }
}

impl From<storage::CatalogFileTransitionError> for BranchLifecycleError {
    fn from(error: storage::CatalogFileTransitionError) -> Self {
        match error {
            storage::CatalogFileTransitionError::Io(error) => Self::CatalogIo(error),
            storage::CatalogFileTransitionError::Transition(error) => Self::Transition(error),
        }
    }
}

impl From<BranchLifecycleError> for HawDBError {
    fn from(error: BranchLifecycleError) -> Self {
        if let BranchLifecycleError::Runtime(error) = error {
            return error;
        }
        if let BranchLifecycleError::SourceBusy(resource) = error {
            return HawDBError::BranchBusy { resource };
        }
        if matches!(
            error,
            BranchLifecycleError::Admission(hawdb_storage::store::BranchAdmissionError::Busy(_))
                | BranchLifecycleError::Admission(
                    hawdb_storage::store::BranchAdmissionError::Lease(
                        hawdb_storage::ownership::DatabaseDirectoryLeaseError::AlreadyOpen
                    )
                )
        ) {
            return HawDBError::BranchBusy {
                resource: "target runtime or project metadata",
            };
        }
        if let BranchLifecycleError::Admission(
            hawdb_storage::store::BranchAdmissionError::Recovery(error),
        ) = error
        {
            return error;
        }
        HawDBError::from_storage_error(error)
    }
}

fn state(state: storage::BranchState) -> BranchLifecycleState {
    match state {
        storage::BranchState::Creating => BranchLifecycleState::Creating,
        storage::BranchState::Ready => BranchLifecycleState::Ready,
        storage::BranchState::Deleting => BranchLifecycleState::Deleting,
        storage::BranchState::Deleted => BranchLifecycleState::Deleted,
    }
}

pub(super) fn branch_info(branch: &storage::BranchRecord) -> BranchInfo {
    BranchInfo {
        id: branch.id.as_uuid(),
        name: branch.name.as_str().to_string(),
        parent_id: branch.parent_id.map(storage::BranchId::as_uuid),
        source_commit_epoch: branch.source_commit_epoch,
        metadata_revision: branch.metadata_revision,
        state: state(branch.state),
        owner: branch.owner.clone(),
    }
}

#[derive(Debug, Clone)]
pub(super) struct BranchSelection {
    record: storage::BranchRecord,
}

#[derive(Debug, Clone)]
pub(super) struct CurrentBranch {
    pub info: BranchInfo,
    pub commit_epoch: u64,
    pub durability: hawdb_storage::config::DurabilityPolicy,
}

fn create_result_info(branch: &storage::BranchRecord) -> Result<BranchInfo, BranchLifecycleError> {
    let mut info = branch_info(branch);
    if branch.create_outcome == storage::CreateOutcome::Pending {
        info.state = BranchLifecycleState::Ready;
        info.metadata_revision =
            info.metadata_revision
                .checked_add(1)
                .ok_or(BranchLifecycleError::Transition(
                    storage::CatalogTransitionError::Overflow(
                        "create completion metadata revision",
                    ),
                ))?;
    }
    Ok(info)
}

fn selector_matches(branch: &storage::BranchRecord, selector: &BranchSelector) -> bool {
    match selector {
        BranchSelector::Id(id) => branch.id.as_uuid() == *id,
        BranchSelector::Name(name) => branch.name.as_str() == name,
    }
}

fn request_fingerprint(request: &BranchCreateRequest) -> [u8; 32] {
    let parent = match &request.parent {
        BranchSelector::Id(id) => ("id", id.to_string()),
        BranchSelector::Name(name) => ("name", name.clone()),
    };
    // Preserve option tags and field boundaries in the durable request identity.
    let encoded = serde_json::to_vec(&(
        "hawdb-branch-create-v2",
        &request.name,
        parent,
        request.expected_source_commit_epoch,
        &request.owner,
        &request.idempotency_key,
    ))
    .expect("branch request contains only JSON-serializable scalar fields");
    *hawdb_integrity::sha256(&encoded).as_bytes()
}

impl Database {
    fn ensure_branch_writable(&self) -> Result<(), BranchLifecycleError> {
        if self.config.read_only {
            return Err(BranchLifecycleError::Transition(
                storage::CatalogTransitionError::InvalidState(
                    "read-only storage cannot change branch metadata",
                ),
            ));
        }
        Ok(())
    }

    pub(super) fn branch_catalog_path(&self) -> Result<PathBuf, BranchLifecycleError> {
        let root = self
            .project_root_path
            .as_deref()
            .or_else(|| self.store.durable_root_path())
            .ok_or(BranchLifecycleError::InMemoryDatabase)?;
        Ok(root.join(BRANCH_DIRECTORY).join(BRANCH_CATALOG_FILE))
    }

    pub(super) fn current_branch(&self) -> Option<CurrentBranch> {
        self.branch_selection
            .as_ref()
            .map(|selection| CurrentBranch {
                info: branch_info(&selection.record),
                commit_epoch: self.store.commit_epoch(),
                durability: self.durability,
            })
    }

    fn admit_branch_record(
        &self,
        record: &storage::BranchRecord,
    ) -> Result<hawdb_storage::store::AdmittedBranchStore, BranchLifecycleError> {
        let catalog_path = self.branch_catalog_path()?;
        let head_path = self.branch_head_path(record.id.as_uuid())?;
        let objects = catalog_path
            .parent()
            .expect("catalog parent")
            .join("objects");
        hawdb_storage::store::GraphStore::admit_branch_from_head(
            hawdb_storage::store::BranchAdmissionRequest {
                catalog_path: &catalog_path,
                branch_id: record.id,
                expected_metadata_revision: record.metadata_revision,
                head_path: &head_path,
                immutable_store_root: &objects,
                durability: self.durability,
                replay_config: self.branch_replay_config(),
            },
        )
        .map_err(BranchLifecycleError::Admission)
    }
    fn branch_replay_config(&self) -> hawdb_storage::config::WalReplayConfig {
        self.config.wal_replay_config()
    }

    pub(super) fn use_branch(
        &mut self,
        selector: BranchSelector,
    ) -> Result<(), BranchLifecycleError> {
        self.store
            .ensure_usable()
            .map_err(BranchLifecycleError::storage)?;
        let catalog = self.read_branch_catalog()?;
        let record = catalog
            .branches
            .iter()
            .find(|record| selector_matches(record, &selector))
            .cloned()
            .ok_or(BranchLifecycleError::UnknownBranch)?;
        if record.state != storage::BranchState::Ready {
            return Err(BranchLifecycleError::Transition(
                storage::CatalogTransitionError::InvalidState("selection target is not ready"),
            ));
        }
        if let Some(selection) = &self.branch_selection
            && selection.record.id == record.id
        {
            if selection.record.metadata_revision != record.metadata_revision {
                return Err(BranchLifecycleError::Transition(
                    storage::CatalogTransitionError::StaleRevision {
                        expected: selection.record.metadata_revision,
                        actual: record.metadata_revision,
                    },
                ));
            }
            return Ok(());
        }
        // Until background claims own independently pinned branch runtimes,
        // keep unfinished work on its original context. A successful switch
        // must never silently drop a pending or externally claimed job.
        if self.derived_artifact_jobs.jobs().iter().any(|job| {
            matches!(
                job.status,
                hawdb_artifact::DerivedArtifactJobStatus::Pending
                    | hawdb_artifact::DerivedArtifactJobStatus::Running
                    | hawdb_artifact::DerivedArtifactJobStatus::Failed
            )
        }) {
            return Err(BranchLifecycleError::SourceBusy(
                "unfinished background work",
            ));
        }
        let _admission_resources = self
            .store
            .reserve_branch_admission_resources()
            .map_err(BranchLifecycleError::Runtime)?;
        let admitted = self.admit_branch_record(&record)?;
        let (mut store, schema) = admitted.into_parts();
        if self.config.read_only {
            store
                .make_admitted_branch_read_only()
                .map_err(BranchLifecycleError::storage)?;
        }
        // The candidate owns its lease before anything on the source is
        // replaced. Schema maintenance and validation can fail independently.
        let mut candidate = Self::new_with_config(self.config.clone());
        candidate.catalog = schema;
        candidate.store = store;
        candidate.project_root_path = self.project_root_path.clone();
        candidate.branch_selection = Some(BranchSelection { record });
        candidate.durability = self.durability;
        candidate.local_qos_scheduler = self.local_qos_scheduler.clone();
        super::configure_search_projection_changefeed(&mut candidate.store, &candidate.config);
        super::configure_relational_fast_paths(&mut candidate.store, &candidate.config);
        if let Some(governor) = &self.runtime_governor {
            candidate.set_runtime_governor(governor.clone());
        }
        if !candidate.config.read_only {
            candidate
                .complete_required_relational_row_checkpoint("branch selection")
                .map_err(BranchLifecycleError::storage)?;
        }
        candidate
            .apply_engine_system_schema()
            .map_err(BranchLifecycleError::storage)?;
        candidate.projection_consumers = super::search_projection_consumer::ConsumerRegistry::load(
            candidate.store.search_projection_registry_root(),
            candidate.store.search_projection_database_identity(),
        );
        crate::store::StoreTelemetry::set_telemetry_sink(
            &mut candidate.store,
            self.telemetry.clone(),
        );
        self.catalog = candidate.catalog;
        self.store = candidate.store;
        self.branch_selection = candidate.branch_selection;
        self.optimizer = candidate.optimizer;
        self.plan_cache = candidate.plan_cache;
        self.relational_plan_template_cache = candidate.relational_plan_template_cache;
        self.optimizer_planning_cache = candidate.optimizer_planning_cache;
        self.reader_pins = candidate.reader_pins;
        // Completed jobs and their ID allocator belong to this host handle.
        // Replacing the queue would allow stale IDs to address a new branch's
        // job. Retryable/active jobs are guarded above until they own separate
        // branch runtime pins.
        self.projection_consumers = candidate.projection_consumers;
        Ok(())
    }

    fn branch_directory(&self, id: Uuid) -> Result<PathBuf, BranchLifecycleError> {
        Ok(self
            .branch_catalog_path()?
            .parent()
            .expect("branch catalog has a parent")
            .join(id.to_string()))
    }

    fn branch_head_path(&self, id: Uuid) -> Result<PathBuf, BranchLifecycleError> {
        Ok(self.branch_directory(id)?.join("branch.head"))
    }

    fn branch_wal_path(&self, id: Uuid, generation: u64) -> Result<PathBuf, BranchLifecycleError> {
        Ok(self
            .branch_directory(id)?
            .join(hawdb_storage::artifact_files::wal_generation_file(
                generation,
            )))
    }

    fn read_branch_catalog(&self) -> Result<storage::Catalog, BranchLifecycleError> {
        storage::read_catalog(&self.branch_catalog_path()?).map_err(BranchLifecycleError::CatalogIo)
    }

    /// Initializes the durable branch catalog with an immutable root branch.
    /// Initialization is explicit so opening an existing database never
    /// invents lineage metadata or a synthetic sealed root.
    pub fn initialize_branch_catalog(
        &self,
        project_id: Uuid,
        main_branch_id: Uuid,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        Ok(branch_info(&self.initialize_branch_catalog_record(
            project_id,
            main_branch_id,
        )?))
    }

    fn initialize_branch_catalog_record(
        &self,
        project_id: Uuid,
        main_branch_id: Uuid,
    ) -> Result<storage::BranchRecord, BranchLifecycleError> {
        self.ensure_branch_writable()?;
        let path = self.branch_catalog_path()?;
        let project = storage::BranchId::new(project_id).map_err(BranchLifecycleError::Catalog)?;
        let main = storage::BranchId::new(main_branch_id).map_err(BranchLifecycleError::Catalog)?;
        storage::initialize_catalog_file(&path, project, main).map_err(Into::into)
    }

    /// Publishes the current durable state as the initial sealed `main` snapshot.
    /// Requires a checkpoint; a fully checkpoint-covered snapshot needs no
    /// sealed WAL interval. The snapshot has its own
    /// private WAL; ordinary database writes and reopen remain manifest-owned.
    /// Later ordinary writes do not advance this sealed branch source.
    pub fn initialize_main_branch(
        &mut self,
        project_id: Uuid,
        main_branch_id: Uuid,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        self.ensure_branch_writable()?;
        let main = self.initialize_branch_catalog_record(project_id, main_branch_id)?;
        let head_path = self.branch_head_path(main.id.as_uuid())?;
        let head = if hawdb_storage::file_io::try_exists(&head_path)
            .map_err(BranchLifecycleError::CatalogIo)?
        {
            let head =
                branch_head::read_branch_head(&head_path).map_err(BranchLifecycleError::storage)?;
            if head.project_id != *project_id.as_bytes()
                || head.branch_id != *main_branch_id.as_bytes()
            {
                return Err(BranchLifecycleError::Storage(
                    "existing main head identity does not match the catalog".to_string(),
                ));
            }
            head
        } else {
            if main.base_root_digest.is_some() {
                return Err(BranchLifecycleError::Storage(
                    "initialized main branch is missing its head".to_string(),
                ));
            }
            let branch_directory = head_path
                .parent()
                .expect("branch head has a parent directory");
            hawdb_storage::file_io::create_dir_all(branch_directory)
                .map_err(BranchLifecycleError::CatalogIo)?;
            let objects = self
                .branch_catalog_path()?
                .parent()
                .expect("branch catalog has a parent")
                .join("objects");
            self.store
                .initialize_immutable_root_head(
                    &objects,
                    &head_path,
                    *project_id.as_bytes(),
                    *main_branch_id.as_bytes(),
                )
                .map_err(BranchLifecycleError::storage)?
        };
        let catalog_path = self.branch_catalog_path()?;
        let objects = hawdb_storage::immutable_object::ImmutableObjectStore::open(
            catalog_path
                .parent()
                .expect("branch catalog has a parent")
                .join("objects"),
        )
        .map_err(BranchLifecycleError::storage)?;
        let root_bytes = objects
            .read(head.sealed_root)
            .map_err(BranchLifecycleError::storage)?;
        let root = hawdb_storage::sealed_root::SealedRoot::decode(&root_bytes)
            .map_err(BranchLifecycleError::storage)?;
        let wal = branch_head::active_wal_identity_from_file(
            &self.branch_wal_path(main.id.as_uuid(), head.active_wal.generation)?,
            head.active_wal.generation,
            head.active_wal.replay_start_lsn,
            head.active_wal.byte_length,
        )
        .map_err(BranchLifecycleError::storage)?;
        if root.commit_epoch != head.logical_commit_epoch || wal != head.active_wal {
            return Err(BranchLifecycleError::Storage(
                "initial main root/WAL does not match its head".to_string(),
            ));
        }
        let project = storage::BranchId::new(project_id).map_err(BranchLifecycleError::Catalog)?;
        let main = storage::BranchId::new(main_branch_id).map_err(BranchLifecycleError::Catalog)?;
        let branch = storage::bind_main_head_file(
            &catalog_path,
            project,
            main,
            *head.sealed_root.sha256.as_bytes(),
            head.logical_commit_epoch,
        )?;
        Ok(branch_info(&branch))
    }

    pub fn list_branches(&self) -> Result<Vec<BranchInfo>, BranchLifecycleError> {
        let path = self.branch_catalog_path()?;
        if !hawdb_storage::file_io::try_exists(&path).map_err(BranchLifecycleError::CatalogIo)? {
            return Ok(Vec::new());
        }
        Ok(self
            .read_branch_catalog()?
            .branches
            .iter()
            .map(branch_info)
            .collect())
    }

    pub fn describe_branch(
        &self,
        selector: BranchSelector,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        let catalog = self.read_branch_catalog()?;
        let branch = match selector {
            BranchSelector::Id(id) => catalog
                .branches
                .iter()
                .find(|branch| branch.id.as_uuid() == id),
            BranchSelector::Name(name) => catalog
                .branches
                .iter()
                .find(|branch| branch.name.as_str() == name),
        }
        .ok_or(BranchLifecycleError::UnknownBranch)?;
        Ok(branch_info(branch))
    }

    /// Publishes a durable delete transition for one resolved branch identity.
    ///
    /// The branch lease is acquired before the metadata transition, and the
    /// storage kernel revalidates the observed revision while holding the
    /// metadata lease. After `Deleting` is durable the temporary lease is
    /// released before finalization. An admitted runtime retains this same
    /// target lease, and admission must reject the durable tombstone. A crash
    /// after `Deleting` is published can be resumed by retrying this operation;
    /// the branch UUID is never reused.
    ///
    /// Retrying an already-`Deleting` branch while its lease is held returns
    /// its current pending state. Only `Deleted` confirms finalization; after
    /// the lease is released a retry can finish that same deletion.
    pub fn delete_branch(
        &self,
        selector: BranchSelector,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        self.delete_branch_with_revision(selector, None)
    }

    pub(super) fn delete_branch_at_revision(
        &self,
        id: Uuid,
        expected_metadata_revision: u64,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        self.delete_branch_with_revision(BranchSelector::Id(id), Some(expected_metadata_revision))
    }

    fn delete_branch_with_revision(
        &self,
        selector: BranchSelector,
        expected_metadata_revision: Option<u64>,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        self.ensure_branch_writable()?;
        let path = self.branch_catalog_path()?;
        let catalog = self.read_branch_catalog()?;
        let branch = catalog
            .branches
            .iter()
            .find(|branch| selector_matches(branch, &selector))
            .cloned()
            .ok_or(BranchLifecycleError::UnknownBranch)?;
        if self
            .branch_selection
            .as_ref()
            .is_some_and(|selection| selection.record.id == branch.id)
        {
            return Err(BranchLifecycleError::Transition(
                storage::CatalogTransitionError::InvalidState("selected branch cannot be deleted"),
            ));
        }
        if branch.name.as_str() == "main" {
            return Err(BranchLifecycleError::RootBranchImmutable);
        }
        if branch.state == storage::BranchState::Deleted {
            return Ok(branch_info(&branch));
        }
        if branch.state == storage::BranchState::Creating {
            return Err(BranchLifecycleError::Transition(
                storage::CatalogTransitionError::InvalidState("branch creation is not complete"),
            ));
        }

        let branch_lease =
            match DatabaseDirectoryLease::acquire(&self.branch_directory(branch.id.as_uuid())?) {
                Ok(lease) => lease,
                Err(hawdb_storage::ownership::DatabaseDirectoryLeaseError::AlreadyOpen)
                    if branch.state == storage::BranchState::Deleting =>
                {
                    // Deleting is already durable and cannot become Ready again.
                    // Report its current outcome without finalizing under an owner.
                    return self.describe_branch(BranchSelector::Id(branch.id.as_uuid()));
                }
                Err(error) => return Err(BranchLifecycleError::LeaseUnavailable(error)),
            };
        let request = storage::DeleteRequest {
            id: branch.id,
            expected_metadata_revision: expected_metadata_revision
                .unwrap_or(branch.metadata_revision),
        };
        let reservation = match storage::begin_delete_file(&path, request)? {
            storage::DeleteBeginOutcome::Deleting(reservation) => reservation,
            storage::DeleteBeginOutcome::Deleted(branch) => return Ok(branch_info(&branch)),
        };
        drop(branch_lease);
        let deleted = storage::finish_delete_file(&path, reservation)?;
        Ok(branch_info(&deleted))
    }

    /// Computes the prospective result before SQL payload admission.
    pub(super) fn preview_create_branch(
        &self,
        request: &BranchCreateRequest,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        let catalog = self.read_branch_catalog()?;
        if let Some(existing) = catalog
            .branches
            .iter()
            .find(|branch| branch.create_request_key == request.idempotency_key)
        {
            return create_result_info(existing);
        }
        let id_bytes = *hawdb_integrity::sha256(request.idempotency_key.as_bytes()).as_bytes();
        let id = Uuid::from_bytes(id_bytes[..16].try_into().expect("UUID width"));
        let branch_id = storage::BranchId::new(id).map_err(BranchLifecycleError::Catalog)?;
        let name = match &request.name {
            Some(name) => {
                storage::BranchName::new(name.clone()).map_err(BranchLifecycleError::Catalog)?
            }
            None => storage::BranchName::generated_agent(branch_id),
        };
        let parent = catalog
            .branches
            .iter()
            .find(|branch| selector_matches(branch, &request.parent))
            .ok_or(BranchLifecycleError::UnknownBranch)?;
        Ok(BranchInfo {
            id,
            name: name.as_str().into(),
            parent_id: Some(parent.id.as_uuid()),
            source_commit_epoch: request.expected_source_commit_epoch,
            metadata_revision: 2,
            state: BranchLifecycleState::Ready,
            owner: request.owner.clone(),
        })
    }

    /// Creates an isolated child branch from an exact committed source. The
    /// idempotency key is retained by the catalog, so retrying the same
    /// request returns the existing record without creating another head.
    pub fn create_branch(
        &mut self,
        request: BranchCreateRequest,
    ) -> Result<BranchInfo, BranchLifecycleError> {
        self.ensure_branch_writable()?;
        let catalog_path = self.branch_catalog_path()?;
        let catalog = self.read_branch_catalog()?;
        let fingerprint = request_fingerprint(&request);
        if let Some(existing) = catalog
            .branches
            .iter()
            .find(|branch| branch.create_request_key == request.idempotency_key)
        {
            if existing.request_fingerprint != fingerprint {
                return Err(BranchLifecycleError::Transition(
                    storage::CatalogTransitionError::Conflict(
                        "create request key has a different fingerprint",
                    ),
                ));
            }
            match existing.create_outcome {
                storage::CreateOutcome::Succeeded => return Ok(branch_info(existing)),
                storage::CreateOutcome::Aborted => {
                    return Err(BranchLifecycleError::Transition(
                        storage::CatalogTransitionError::InvalidState(
                            "branch creation was aborted",
                        ),
                    ))
                }
                storage::CreateOutcome::Pending => {
                    let completed = create_result_info(existing)?;
                    let outcome = storage::recover_create_from_head_file(
                        &catalog_path,
                        existing.id,
                        &self.branch_head_path(existing.id.as_uuid())?,
                        self.config.max_wal_replay_bytes.unwrap_or(u64::MAX),
                    )
                    .map_err(|error| match error {
                        storage::BranchCreateError::Lease(
                            hawdb_storage::ownership::DatabaseDirectoryLeaseError::AlreadyOpen,
                        ) => BranchLifecycleError::SourceBusy("pending child creation"),
                        error => BranchLifecycleError::storage(error),
                    })?;
                    return match outcome {
                        storage::CreateRecoveryOutcome::Completed => Ok(completed),
                        storage::CreateRecoveryOutcome::Aborted => {
                            Err(BranchLifecycleError::Transition(
                                storage::CatalogTransitionError::InvalidState(
                                    "branch creation was aborted",
                                ),
                            ))
                        }
                    };
                }
            }
        }
        let parent = catalog
            .branches
            .iter()
            .find(|branch| selector_matches(branch, &request.parent))
            .ok_or(BranchLifecycleError::UnknownBranch)?;
        if parent.state != storage::BranchState::Ready {
            return Err(BranchLifecycleError::Transition(
                storage::CatalogTransitionError::InvalidState("create parent is not ready"),
            ));
        }
        let parent_id = parent.id;
        let parent_head_path = self.branch_head_path(parent_id.as_uuid())?;
        let sealed_source = if self
            .store
            .admitted_branch_head()
            .is_some_and(|head| head.branch_id == *parent_id.as_uuid().as_bytes())
        {
            self.store
                .seal_admitted_branch(request.expected_source_commit_epoch)
                .map_err(BranchLifecycleError::storage)?;
            None
        } else {
            let objects = catalog_path
                .parent()
                .expect("catalog parent")
                .join("objects");
            Some(
                hawdb_storage::store::GraphStore::seal_branch_from_head(
                    hawdb_storage::store::BranchAdmissionRequest {
                        catalog_path: &catalog_path,
                        branch_id: parent.id,
                        expected_metadata_revision: parent.metadata_revision,
                        head_path: &parent_head_path,
                        immutable_store_root: &objects,
                        durability: self.durability,
                        replay_config: self.branch_replay_config(),
                    },
                    request.expected_source_commit_epoch,
                )
                .map_err(BranchLifecycleError::Admission)?,
            )
        };
        let parent_head = branch_head::read_branch_head(&parent_head_path)
            .map_err(|error| BranchLifecycleError::CatalogIo(std::io::Error::other(error)))?;
        if parent_head.logical_commit_epoch != request.expected_source_commit_epoch {
            return Err(BranchLifecycleError::Transition(
                storage::CatalogTransitionError::Conflict("create source revision is stale"),
            ));
        }
        let id_bytes = *hawdb_integrity::sha256(request.idempotency_key.as_bytes()).as_bytes();
        let id = Uuid::from_bytes(id_bytes[..16].try_into().expect("UUID width"));
        let child_id = storage::BranchId::new(id).map_err(BranchLifecycleError::Catalog)?;
        let name = match request.name {
            Some(name) => storage::BranchName::new(name).map_err(BranchLifecycleError::Catalog)?,
            None => storage::BranchName::generated_agent(child_id),
        };
        let child_head_path = self.branch_head_path(id)?;
        let generation = parent_head.active_wal.generation.checked_add(1).ok_or(
            BranchLifecycleError::Transition(storage::CatalogTransitionError::Overflow(
                "child WAL generation",
            )),
        )?;
        let child_wal_path = self.branch_wal_path(id, generation)?;
        let create_request = storage::CreateRequest {
            id: child_id,
            name,
            parent_id,
            source_commit_epoch: request.expected_source_commit_epoch,
            base_root_digest: *parent_head.sealed_root.sha256.as_bytes(),
            owner: request.owner,
            request_key: request.idempotency_key,
            request_fingerprint: fingerprint,
        };
        let mut candidate = catalog.clone();
        candidate
            .reserve_create(create_request.clone())
            .map_err(BranchLifecycleError::Transition)?;
        let reserved = candidate
            .branches
            .iter()
            .find(|branch| branch.id == child_id)
            .ok_or(BranchLifecycleError::UnknownBranch)?;
        let completed = create_result_info(reserved)?;
        // Keep the child's lease through return and report this operation's
        // completion snapshot. A later delete must not change the admitted
        // result shape or add fallible catalog IO after durable completion.
        let _publication = match &sealed_source {
            Some(source) => source.create_child(&child_head_path, &child_wal_path, create_request),
            None => self.store.create_isolated_branch_from_parent_head(
                &catalog_path,
                &parent_head_path,
                &child_head_path,
                &child_wal_path,
                create_request,
            ),
        }
        .map_err(|error| BranchLifecycleError::CatalogIo(std::io::Error::other(error)))?;
        Ok(completed)
    }
}
