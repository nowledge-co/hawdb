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
fn relational_mvcc_constraint_and_predicate_shapes_keep_database_barriers() {
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
        // In the third case records is referenced by another table. That
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
        let error = second.commit().unwrap_err();
        assert!(
            matches!(error, HawDBError::TransactionConflict { ref key, .. } if key == "database"),
            "{error}"
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
