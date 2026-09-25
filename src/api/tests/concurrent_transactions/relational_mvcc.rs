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

#[test]
fn relational_mvcc_explicit_keys_commit_disjoint_rows_and_reject_same_key_before_wal() {
    for mode in [
        crate::StorageResidencyMode::Materialized,
        crate::StorageResidencyMode::OutOfCore,
    ] {
        for same_key in [false, true] {
            let path = super::super::unique_test_dir("relational_mvcc_explicit");
            let config = crate::DatabaseConfig {
                storage_residency_mode: mode,
                ..crate::DatabaseConfig::default()
            };
            let mut database = Database::open_with_config(&path, config.clone()).unwrap();
            database.query_sql("CREATE TABLE records (tenant TEXT NOT NULL, id BIGINT NOT NULL, body TEXT, PRIMARY KEY (tenant, id))").unwrap();
            database
                .query_sql("CREATE INDEX records_body ON records (body)")
                .unwrap();
            database.checkpoint().unwrap();
            let db = database.into_concurrent();
            let epoch = db.commit_epoch().unwrap();
            let read = db.begin_read_transaction().unwrap();
            let options = ConcurrentTransactionOptions::optimistic();
            let mut first = db.begin_transaction(options).unwrap();
            let mut second = db.begin_transaction(options).unwrap();
            first.query_sql("INSERT INTO records (tenant, id, body) VALUES ('tenant', 1, 'shared-index-value')").unwrap();
            let second_id = if same_key { 1 } else { 2 };
            second
                .query_sql_with_params(
                    "INSERT INTO records (tenant, id, body) VALUES ($1, $2, $3)",
                    &[
                        Value::String("tenant".into()),
                        Value::Int(second_id),
                        Value::String("shared-index-value".into()),
                    ],
                )
                .unwrap();
            first.commit().unwrap();
            db.checkpoint().unwrap();
            assert!(read
                .query_sql("SELECT id FROM records")
                .unwrap()
                .rows
                .is_empty());
            let private = second.query_sql("SELECT id FROM records").unwrap().rows;
            assert_eq!(private.len(), 1);
            assert_eq!(private[0]["id"], Value::Int(second_id));
            let wal_path = super::super::active_wal_path(&path);
            let wal_before = std::fs::read(&wal_path).unwrap();
            let result = second.commit();
            if same_key {
                let error = result.unwrap_err();
                assert!(
                    matches!(error, HawDBError::TransactionConflict { ref key, .. } if key == "relational_row"),
                    "{error}"
                );
                assert!(error.is_retryable_transaction_conflict());
                assert_eq!(std::fs::read(&wal_path).unwrap(), wal_before);
            } else {
                result.unwrap();
            }
            let count = if same_key { 1 } else { 2 };
            assert_eq!(db.commit_epoch().unwrap(), epoch + count);
            assert!(read
                .query_sql("SELECT id FROM records")
                .unwrap()
                .rows
                .is_empty());
            drop(read);
            drop(db);
            let mut reopened = Database::open_with_config(&path, config).unwrap();
            let rows = reopened
                .query_sql("SELECT id FROM records WHERE body = 'shared-index-value' ORDER BY id")
                .unwrap()
                .rows;
            assert_eq!(
                rows.iter().map(|row| row["id"].clone()).collect::<Vec<_>>(),
                (1..=count)
                    .map(|id| Value::Int(id as i64))
                    .collect::<Vec<_>>()
            );
            let reopened = reopened.into_concurrent();
            let mut first = reopened.begin_transaction(options).unwrap();
            let mut stale = reopened.begin_transaction(options).unwrap();
            for tx in [&mut first, &mut stale] {
                tx.query_sql(
                    "INSERT INTO records (tenant, id, body) VALUES ('after-open', 3, 'new')",
                )
                .unwrap();
            }
            first.commit().unwrap();
            assert!(stale
                .commit()
                .unwrap_err()
                .is_retryable_transaction_conflict());
            drop(reopened);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn relational_mvcc_insert_constraints_are_precise_and_predicates_keep_barriers() {
    for setup in [
        "CREATE TABLE records (id BIGINT PRIMARY KEY, body TEXT UNIQUE)",
        "CREATE TABLE records (id BIGINT PRIMARY KEY, body TEXT)",
        "CREATE TABLE records (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES parents(id))",
        "CREATE TABLE records (id BIGINT PRIMARY KEY)",
    ] {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE parents (id BIGINT PRIMARY KEY)")
            .unwrap();
        database
            .query_sql("INSERT INTO parents (id) VALUES (1)")
            .unwrap();
        database.query_sql(setup).unwrap();
        if setup.ends_with("body TEXT)") {
            database
                .query_sql("CREATE UNIQUE INDEX records_body_unique ON records (body)")
                .unwrap();
        }
        // In the fourth case records is referenced by another table. That
        // incoming edge must keep parent deletion/cascade footprints broad.
        let incoming = !setup.contains("body") && !setup.contains("parent BIGINT");
        if incoming {
            database.query_sql("CREATE TABLE children (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES records(id) ON DELETE CASCADE)").unwrap();
        }
        let db = database.into_concurrent();
        let mut first = db
            .begin_transaction(ConcurrentTransactionOptions::optimistic())
            .unwrap();
        let mut second = db
            .begin_transaction(ConcurrentTransactionOptions::optimistic())
            .unwrap();
        for (tx, id) in [(&mut first, 1), (&mut second, 2)] {
            if setup.contains("body") {
                tx.query_sql_with_params(
                    "INSERT INTO records (id, body) VALUES ($1, $2)",
                    &[Value::Int(id), Value::String(format!("body-{id}"))],
                )
                .unwrap();
            } else if incoming {
                tx.query_sql_with_params("INSERT INTO records (id) VALUES ($1)", &[Value::Int(id)])
                    .unwrap();
            } else {
                tx.query_sql_with_params(
                    "INSERT INTO records (id, parent) VALUES ($1, 1)",
                    &[Value::Int(id)],
                )
                .unwrap();
            }
        }
        first.commit().unwrap();
        second.commit().unwrap();
        assert_eq!(
            db.query_sql("SELECT id FROM records").unwrap().rows.len(),
            2
        );
    }
    // Do not derive predicate write intents from the final net-change set.
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, value BIGINT)")
        .unwrap();
    database
        .query_sql("INSERT INTO records (id, value) VALUES (1, 0), (2, 0)")
        .unwrap();
    let db = database.into_concurrent();
    let mut predicate = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    predicate
        .query_sql("UPDATE records SET value = 9 WHERE value = 0")
        .unwrap();
    let mut newcomer = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    newcomer
        .query_sql("INSERT INTO records (id, value) VALUES (3, 0)")
        .unwrap();
    newcomer.commit().unwrap();
    assert!(predicate
        .commit()
        .unwrap_err()
        .is_retryable_transaction_conflict());
    let rows = db
        .query_sql("SELECT value FROM records ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| row["value"] == Value::Int(0)));
}

#[test]
fn relational_mvcc_constrained_inserts_validate_unique_values_without_serializing_foreign_keys() {
    use hawdb_storage::config::RelationalIndexMode;

    for (mode, indexes) in [
        (
            crate::StorageResidencyMode::Materialized,
            RelationalIndexMode::Materialized,
        ),
        (
            crate::StorageResidencyMode::OutOfCore,
            RelationalIndexMode::Materialized,
        ),
        (
            crate::StorageResidencyMode::OutOfCore,
            RelationalIndexMode::Authoritative,
        ),
    ] {
        // Distinct values, compound uniqueness, declared unique index, nullable
        // compound/index keys, and a parent-table unique collision respectively.
        for scenario in 0..5 {
            let path = super::super::unique_test_dir("relational_mvcc_content_inserts");
            let config = crate::DatabaseConfig {
                storage_residency_mode: mode,
                relational_index_mode: indexes,
                ..crate::DatabaseConfig::default()
            };
            let mut seed_config = config.clone();
            if indexes == RelationalIndexMode::Authoritative {
                seed_config.relational_index_mode = RelationalIndexMode::Shadow;
            }
            let mut database = Database::open_with_config(&path, seed_config).unwrap();
            // Contract-shaped content_documents/content_chunks: parent ownership
            // uniqueness, incoming/outgoing references and per-document ordinals.
            database.query_sql("CREATE TABLE documents (id TEXT PRIMARY KEY, owner_kind TEXT NOT NULL, owner_id TEXT NOT NULL, UNIQUE (owner_kind, owner_id))").unwrap();
            database.query_sql("CREATE TABLE chunks (id TEXT PRIMARY KEY, document TEXT NOT NULL REFERENCES documents(id), ordinal BIGINT, token TEXT, UNIQUE (document, ordinal))").unwrap();
            database
                .query_sql("CREATE UNIQUE INDEX chunks_token ON chunks (token)")
                .unwrap();
            database.query_sql("INSERT INTO documents (id, owner_kind, owner_id) VALUES ('seed', 'thread', 'seed')").unwrap();
            database.checkpoint().unwrap();
            drop(database);
            let database = Database::open_with_config(&path, config.clone()).unwrap();
            assert_eq!(
                database
                    .store
                    .relational_state()
                    .canonical_row_metadata_only(),
                indexes == RelationalIndexMode::Authoritative,
            );
            let db = database.into_concurrent();
            let old = db.begin_read_transaction().unwrap();
            let epoch = db.commit_epoch().unwrap();
            let mut first = db
                .begin_transaction(ConcurrentTransactionOptions::optimistic())
                .unwrap();
            let mut second = db
                .begin_transaction(ConcurrentTransactionOptions::optimistic())
                .unwrap();
            for (tx, writer) in [(&mut first, 1), (&mut second, 2)] {
                let owner = if scenario == 4 {
                    "same-owner".to_string()
                } else {
                    format!("owner-{writer}")
                };
                tx.query_sql_with_params(
                    "INSERT INTO documents (id, owner_kind, owner_id) VALUES ($1, 'thread', $2)",
                    &[Value::String(format!("doc-{writer}")), Value::String(owner)],
                )
                .unwrap();
                let ordinal = if scenario == 3 {
                    Value::Null
                } else {
                    Value::Int(if scenario == 1 { 1 } else { writer })
                };
                let token = if scenario == 3 {
                    Value::Null
                } else {
                    Value::String(if scenario == 2 {
                        "same-token".to_string()
                    } else {
                        format!("token-{writer}")
                    })
                };
                tx.query_sql_with_params(
                    "INSERT INTO chunks (id, document, ordinal, token) VALUES ($1, 'seed', $2, $3)",
                    &[Value::String(format!("chunk-{writer}")), ordinal, token],
                )
                .unwrap();
            }
            first.commit().unwrap();
            db.checkpoint().unwrap();
            let wal = super::super::active_wal_path(&path);
            let before = std::fs::read(&wal).unwrap();
            let success = scenario == 0 || scenario == 3;
            let result = second.commit();
            if success {
                result.unwrap();
            } else {
                let error = result.unwrap_err();
                assert!(
                    matches!(&error, HawDBError::TransactionConflict { key, .. } if key == "relational_index"),
                    "{error}"
                );
                assert!(error.is_retryable_transaction_conflict());
                assert_eq!(std::fs::read(&wal).unwrap(), before);
            }
            let committed = if success { 2 } else { 1 };
            assert_eq!(db.commit_epoch().unwrap(), epoch + committed);
            assert!(old
                .query_sql("SELECT id FROM chunks")
                .unwrap()
                .rows
                .is_empty());
            assert_eq!(
                old.query_sql("SELECT id FROM documents")
                    .unwrap()
                    .rows
                    .len(),
                1
            );
            let documents = db
                .query_sql("SELECT id FROM documents ORDER BY id")
                .unwrap()
                .rows;
            let chunks = db
                .query_sql("SELECT id, document, ordinal, token FROM chunks ORDER BY id")
                .unwrap()
                .rows;
            assert_eq!(documents.len(), 1 + committed as usize);
            assert_eq!(chunks.len(), committed as usize);
            assert!(documents
                .iter()
                .any(|row| row["id"] == Value::String("doc-1".into())));
            assert_eq!(
                documents
                    .iter()
                    .any(|row| row["id"] == Value::String("doc-2".into())),
                success
            );
            drop(old);
            drop(db);
            let mut reopened = Database::open_with_config(&path, config.clone()).unwrap();
            assert_eq!(reopened.commit_epoch(), epoch + committed);
            assert_eq!(
                reopened
                    .query_sql("SELECT id FROM documents ORDER BY id")
                    .unwrap()
                    .rows,
                documents
            );
            assert_eq!(
                reopened
                    .query_sql("SELECT id, document, ordinal, token FROM chunks ORDER BY id")
                    .unwrap()
                    .rows,
                chunks
            );
            let reopened = reopened.into_concurrent();
            let mut winner = reopened
                .begin_transaction(ConcurrentTransactionOptions::optimistic())
                .unwrap();
            let mut loser = reopened
                .begin_transaction(ConcurrentTransactionOptions::optimistic())
                .unwrap();
            for (tx, id) in [(&mut winner, "after-a"), (&mut loser, "after-b")] {
                tx.query_sql_with_params("INSERT INTO chunks (id, document, ordinal, token) VALUES ($1, 'seed', 20, NULL)", &[Value::String(id.into())]).unwrap();
            }
            winner.commit().unwrap();
            let before = std::fs::read(&wal).unwrap();
            assert!(loser
                .commit()
                .unwrap_err()
                .is_retryable_transaction_conflict());
            assert_eq!(std::fs::read(&wal).unwrap(), before);
            drop(reopened);
            let mut again = Database::open_with_config(&path, config).unwrap();
            let rows = again
                .query_sql("SELECT id FROM chunks WHERE ordinal = 20")
                .unwrap()
                .rows;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["id"], Value::String("after-a".into()));
            drop(again);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn relational_mvcc_parent_deletion_keeps_both_barrier_directions_for_child_inserts() {
    for child_first in [false, true] {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE parents (id BIGINT PRIMARY KEY)")
            .unwrap();
        database.query_sql("CREATE TABLE children (id BIGINT PRIMARY KEY, parent BIGINT NOT NULL REFERENCES parents(id) ON DELETE CASCADE)").unwrap();
        database
            .query_sql("INSERT INTO parents (id) VALUES (1)")
            .unwrap();
        let db = database.into_concurrent();
        let epoch = db.commit_epoch().unwrap();
        let mut parent = db
            .begin_transaction(ConcurrentTransactionOptions::optimistic())
            .unwrap();
        let mut child = db
            .begin_transaction(ConcurrentTransactionOptions::optimistic())
            .unwrap();
        parent
            .query_sql("DELETE FROM parents WHERE id = 1")
            .unwrap();
        child
            .query_sql("INSERT INTO children (id, parent) VALUES (1, 1)")
            .unwrap();
        let error = if child_first {
            child.commit().unwrap();
            parent.commit().unwrap_err()
        } else {
            parent.commit().unwrap();
            child.commit().unwrap_err()
        };
        assert!(error.is_retryable_transaction_conflict(), "{error}");
        assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
        assert_eq!(
            db.query_sql("SELECT id FROM parents").unwrap().rows.len(),
            usize::from(child_first)
        );
        assert_eq!(
            db.query_sql("SELECT id FROM children").unwrap().rows.len(),
            usize::from(child_first)
        );
    }
}

#[test]
fn relational_mvcc_primary_key_predicates_preserve_replay_and_absent_intents() {
    use hawdb_storage::config::RelationalIndexMode;

    for (mode, indexes) in [
        (
            crate::StorageResidencyMode::Materialized,
            RelationalIndexMode::Materialized,
        ),
        (
            crate::StorageResidencyMode::OutOfCore,
            RelationalIndexMode::Materialized,
        ),
        (
            crate::StorageResidencyMode::OutOfCore,
            RelationalIndexMode::Authoritative,
        ),
    ] {
        for scenario in 0..6 {
            let path = super::super::unique_test_dir("relational_mvcc_point_predicates");
            let config = crate::DatabaseConfig {
                storage_residency_mode: mode,
                relational_index_mode: indexes,
                ..crate::DatabaseConfig::default()
            };
            let mut seed_config = config.clone();
            if indexes == RelationalIndexMode::Authoritative {
                seed_config.relational_index_mode = RelationalIndexMode::Shadow;
            }
            let mut database = Database::open_with_config(&path, seed_config).unwrap();
            database.query_sql("CREATE TABLE records (tenant TEXT, id BIGINT, value BIGINT, PRIMARY KEY (tenant, id))").unwrap();
            database
                .query_sql(
                    "INSERT INTO records (tenant, id, value) VALUES ('a', 1, 0), ('a', 2, 0)",
                )
                .unwrap();
            database.checkpoint().unwrap();
            drop(database);
            let database = Database::open_with_config(&path, config.clone()).unwrap();
            assert_eq!(
                database
                    .store
                    .relational_state()
                    .canonical_row_metadata_only(),
                indexes == RelationalIndexMode::Authoritative
            );
            let db = database.into_concurrent();
            let old = db.begin_read_transaction().unwrap();
            let original = old
                .query_sql("SELECT * FROM records ORDER BY id")
                .unwrap()
                .rows;
            let epoch = db.commit_epoch().unwrap();
            let mut first = db
                .begin_transaction(ConcurrentTransactionOptions::optimistic())
                .unwrap();
            let mut second = db
                .begin_transaction(ConcurrentTransactionOptions::optimistic())
                .unwrap();
            let (left, right) = match scenario {
                0 => ("UPDATE records SET value = value + 11 WHERE id = 1 AND tenant = 'a'", "UPDATE records SET value = value + 12 WHERE tenant = 'a' AND id = 2"),
                1 => ("DELETE FROM records WHERE id = 1 AND tenant = 'a'", "UPDATE records SET value = 12 WHERE id = 2 AND tenant = 'a'"),
                2 => ("UPDATE records SET value = 9 WHERE id = 9 AND tenant = 'a'", "INSERT INTO records (tenant, id, value) VALUES ('a', 9, 90)"),
                3 => ("INSERT INTO records (tenant, id, value) VALUES ('a', 9, 90)", "DELETE FROM records WHERE id = 9 AND tenant = 'a'"),
                4 => ("UPDATE records SET value = 99 WHERE tenant = 'a' AND id = 1 AND value = 99", "UPDATE records SET value = 1 WHERE tenant = 'a' AND id = 1"),
                _ => ("UPDATE records SET value = 1 WHERE tenant = 'a' AND id = 1", "DELETE FROM records WHERE tenant = 'a' AND id = 1 AND (value = 1 OR value = 2)"),
            };
            first.query_sql(left).unwrap();
            second.query_sql(right).unwrap();
            first.commit().unwrap();
            db.checkpoint().unwrap();
            let wal = super::super::active_wal_path(&path);
            let before = std::fs::read(&wal).unwrap();
            if scenario < 2 {
                second.commit().unwrap();
            } else {
                let error = second.commit().unwrap_err();
                assert!(
                    matches!(&error, HawDBError::TransactionConflict { key, .. } if key == "relational_row"),
                    "{error}"
                );
                assert!(error.is_retryable_transaction_conflict());
                assert_eq!(std::fs::read(&wal).unwrap(), before);
            }
            let expected_epoch = epoch + if scenario < 2 { 2 } else { 1 };
            assert_eq!(db.commit_epoch().unwrap(), expected_epoch);
            assert_eq!(
                old.query_sql("SELECT * FROM records ORDER BY id")
                    .unwrap()
                    .rows,
                original
            );
            let rows = db
                .query_sql("SELECT * FROM records ORDER BY id")
                .unwrap()
                .rows;
            let expected = match scenario {
                0 => vec![(1, 11), (2, 12)],
                1 => vec![(2, 12)],
                2 | 4 => vec![(1, 0), (2, 0)],
                3 => vec![(1, 0), (2, 0), (9, 90)],
                _ => vec![(1, 1), (2, 0)],
            };
            assert_eq!(rows.len(), expected.len());
            for (row, (id, value)) in rows.iter().zip(expected) {
                assert_eq!(row["tenant"], Value::String("a".into()));
                assert_eq!(row["id"], Value::Int(id));
                assert_eq!(row["value"], Value::Int(value));
            }
            drop(old);
            drop(db);
            let mut reopened = Database::open_with_config(&path, config).unwrap();
            assert_eq!(reopened.commit_epoch(), expected_epoch);
            assert_eq!(
                reopened
                    .query_sql("SELECT * FROM records ORDER BY id")
                    .unwrap()
                    .rows,
                rows
            );
            drop(reopened);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
