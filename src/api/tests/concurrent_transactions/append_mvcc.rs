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

fn create_table(table: &str, generated: bool) -> String {
    let generated_option = if generated {
        ", generated_order = 'commit_sequence'"
    } else {
        ""
    };
    format!("CREATE TABLE {table} (stream_id TEXT NOT NULL, sequence BIGINT NOT NULL, payload TEXT NOT NULL) WITH (storage_mode = 'strict_append', partition_key = 'stream_id', order_key = 'sequence'{generated_option})")
}

fn append(
    tx: &mut crate::ConcurrentDatabaseTransaction,
    table: &str,
    generated: bool,
    partition: &str,
    value: &str,
) {
    if generated {
        tx.query_sql_with_params(
            &format!("INSERT INTO {table} (stream_id, payload) VALUES ($1, $2)"),
            &[Value::String(partition.into()), Value::String(value.into())],
        )
        .unwrap();
    } else {
        tx.query_sql_with_params(
            &format!("INSERT INTO {table} (stream_id, sequence, payload) VALUES ($1, $2, $3)"),
            &[
                Value::String(partition.into()),
                Value::Int(1),
                Value::String(value.into()),
            ],
        )
        .unwrap();
    }
}

#[test]
fn append_mvcc_table_identity_preserves_disjoint_commits_conflicts_checkpoint_and_reopen() {
    for generated in [false, true] {
        for same_table in [false, true] {
            let path = super::super::unique_test_dir("append_mvcc_tables");
            let mut database = Database::open(&path).unwrap();
            database
                .query_sql(&create_table("events_a", generated))
                .unwrap();
            database
                .query_sql(&create_table("events_b", generated))
                .unwrap();
            let db = database.into_concurrent();
            let epoch = db.commit_epoch().unwrap();
            let options = ConcurrentTransactionOptions::optimistic();
            let mut first = db.begin_transaction(options).unwrap();
            let mut second = db.begin_transaction(options).unwrap();
            let second_table = if same_table { "events_a" } else { "events_b" };
            append(&mut first, "events_a", generated, "first", "first-value");
            append(
                &mut second,
                second_table,
                generated,
                "second",
                "second-value",
            );
            first.commit().unwrap();
            db.checkpoint().unwrap(); // The old writer must retain its conflict stamp.
            let wal_path = super::super::active_wal_path(&path);
            let wal_before = std::fs::read(&wal_path).unwrap();
            let result = second.commit_with_result();
            if same_table {
                let error = result.unwrap_err();
                assert!(error.is_retryable_transaction_conflict(), "{error}");
                assert!(
                    matches!(error, HawDBError::TransactionConflict { ref key, .. } if key == "append_table")
                );
                assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
                assert_eq!(std::fs::read(&wal_path).unwrap(), wal_before);
                let mut retry = db.begin_transaction(options).unwrap();
                append(
                    &mut retry,
                    second_table,
                    generated,
                    "second",
                    "second-value",
                );
                let result = retry.commit_with_result().unwrap();
                if generated {
                    assert_eq!(
                        result.append_mutations[0].generated_order_keys,
                        vec![crate::RelationalKey(vec![RelationalValue::BigInt(2)])]
                    );
                }
            } else {
                let result = result.unwrap();
                if generated {
                    assert_eq!(
                        result.append_mutations[0].generated_order_keys,
                        vec![crate::RelationalKey(vec![RelationalValue::BigInt(1)])]
                    );
                }
            }
            assert_eq!(db.commit_epoch().unwrap(), epoch + 2);
            drop(db);
            let mut reopened = Database::open(&path).unwrap();
            assert_eq!(reopened.commit_epoch(), epoch + 2);
            for (table, partition, value, expected_sequence) in [
                ("events_a", "first", "first-value", 1),
                (
                    second_table,
                    "second",
                    "second-value",
                    if same_table && generated { 2 } else { 1 },
                ),
            ] {
                let rows = reopened.query_sql_with_params(&format!("SELECT sequence, payload FROM {table} WHERE stream_id = $1 ORDER BY sequence LIMIT 10"), &[Value::String(partition.into())]).unwrap().rows;
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0]["sequence"], Value::Int(expected_sequence));
                assert_eq!(rows[0]["payload"], Value::String(value.into()));
            }
            // Reopen discards process-local stamps, but new overlapping writes
            // must still establish first-committer-wins from that baseline.
            let reopened = reopened.into_concurrent();
            let mut winner = reopened.begin_transaction(options).unwrap();
            let mut stale = reopened.begin_transaction(options).unwrap();
            append(
                &mut winner,
                "events_a",
                generated,
                "after-open-winner",
                "winner",
            );
            append(
                &mut stale,
                "events_a",
                generated,
                "after-open-stale",
                "rejected",
            );
            winner.commit().unwrap();
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
fn append_mvcc_mixed_table_write_conflict_publishes_no_partial_rows() {
    let path = super::super::unique_test_dir("append_mvcc_mixed");
    let mut database = Database::open(&path).unwrap();
    for table in ["events_a", "events_b"] {
        database.query_sql(&create_table(table, true)).unwrap();
    }
    let db = database.into_concurrent();
    let options = ConcurrentTransactionOptions::optimistic();
    let mut mixed = db.begin_transaction(options).unwrap();
    append(&mut mixed, "events_a", true, "mixed", "must-not-appear");
    append(&mut mixed, "events_b", true, "mixed", "must-not-appear");
    let mut first = db.begin_transaction(options).unwrap();
    append(&mut first, "events_b", true, "winner", "winner");
    first.commit().unwrap();
    let epoch = db.commit_epoch().unwrap();
    let wal_path = super::super::active_wal_path(&path);
    let before = std::fs::read(&wal_path).unwrap();
    assert!(mixed
        .commit()
        .unwrap_err()
        .is_retryable_transaction_conflict());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert_eq!(std::fs::read(&wal_path).unwrap(), before);
    drop(db);
    let mut reopened = Database::open(&path).unwrap();
    for table in ["events_a", "events_b"] {
        assert!(reopened
            .query_sql(&format!(
                "SELECT payload FROM {table} WHERE stream_id = 'mixed' ORDER BY sequence LIMIT 10"
            ))
            .unwrap()
            .rows
            .is_empty());
    }
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn append_mvcc_schema_barrier_rejects_both_stale_commit_directions() {
    for schema_first in [false, true] {
        let mut database = Database::new();
        database.query_sql(&create_table("events", true)).unwrap();
        let db = database.into_concurrent();
        let mut schema = db
            .begin_transaction(ConcurrentTransactionOptions::optimistic())
            .unwrap();
        let mut rows = db
            .begin_transaction(ConcurrentTransactionOptions::optimistic())
            .unwrap();
        schema.query_sql(&create_table("new_events", true)).unwrap();
        append(&mut rows, "events", true, "first", "first");
        let (first, second) = if schema_first {
            (schema, rows)
        } else {
            (rows, schema)
        };
        first.commit().unwrap();
        assert!(second
            .commit()
            .unwrap_err()
            .is_retryable_transaction_conflict());
    }
}

#[test]
fn append_mvcc_table_write_and_graph_write_preserve_each_other_in_both_orders() {
    for append_first in [false, true] {
        let mut database = Database::new();
        database.query_sql(&create_table("events", true)).unwrap();
        database
            .query("CREATE (:Marker {id: 1, value: 0})")
            .unwrap();
        let db = database.into_concurrent();
        let mut graph = db
            .begin_transaction(ConcurrentTransactionOptions::optimistic())
            .unwrap();
        let mut rows = db
            .begin_transaction(ConcurrentTransactionOptions::optimistic())
            .unwrap();
        graph
            .query("MATCH (n:Marker) WHERE n.id = 1 SET n.value = 7")
            .unwrap();
        append(&mut rows, "events", true, "first", "retained");
        let (first, second) = if append_first {
            (rows, graph)
        } else {
            (graph, rows)
        };
        first.commit().unwrap();
        second.commit().unwrap();
        assert_eq!(
            db.query("MATCH (n:Marker) RETURN n.value AS value")
                .unwrap()
                .rows[0]["value"],
            Value::Int(7)
        );
        let rows = db.query_sql("SELECT sequence, payload FROM events WHERE stream_id = 'first' ORDER BY sequence LIMIT 10").unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["sequence"], Value::Int(1));
        assert_eq!(rows[0]["payload"], Value::String("retained".into()));
    }
}
