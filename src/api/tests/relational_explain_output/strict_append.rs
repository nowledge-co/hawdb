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
use hawdb_core::{RuntimeMemoryReservation, RuntimeTaskContext};

#[derive(Clone, Copy, Debug)]
enum FrontDoor {
    Database,
    Snapshot,
    Transaction,
}

fn fixture() -> Database {
    let mut database = Database::new();
    database
        .query_sql(
            "CREATE TABLE events (stream_id TEXT NOT NULL, sequence BIGINT NOT NULL, payload TEXT NOT NULL) \
             WITH (storage_mode = 'strict_append', partition_key = 'stream_id', order_key = 'sequence')",
        )
        .unwrap();
    database
}

fn explain_sql(analyze: bool) -> String {
    format!(
        "EXPLAIN {}SELECT payload FROM events WHERE stream_id = $1 ORDER BY sequence ASC LIMIT 1",
        if analyze { "ANALYZE " } else { "" }
    )
}

fn parameters() -> [Value; 1] {
    [Value::String("empty".into())]
}

fn query(
    database: &mut Database,
    door: FrontDoor,
    sql: &str,
    options: QueryStreamOptions,
) -> crate::Result<QueryOutput> {
    match door {
        FrontDoor::Database => database.query_sql_with_params_options(sql, &parameters(), options),
        FrontDoor::Snapshot => database
            .begin_read_transaction()?
            .query_sql_with_params_options(sql, &parameters(), options),
        FrontDoor::Transaction => {
            database.config.max_read_result_rows = options.max_rows;
            database.config.max_read_result_payload_bytes = options.max_payload_bytes;
            database
                .begin_transaction()?
                .query_sql_with_params(sql, &parameters())
        }
    }
}

fn rejects_payload(door: FrontDoor, analyze: bool) {
    let mut database = fixture();
    let sql = explain_sql(analyze);
    let report = database.query_sql_with_params(&sql, &parameters()).unwrap();
    assert_eq!(report.rows.len(), 1);
    assert!(report.payload_bytes() > 1);
    if analyze {
        assert_eq!(report.rows[0].get("actRows"), Some(&Value::Int(0)));
    }
    let error = query(
        &mut database,
        door,
        &sql,
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(1),
        },
    )
    .unwrap_err();
    assert!(matches!(error, crate::HawDBError::Execution(_)), "{error}");
    assert!(error.to_string().contains("payload"), "{error}");
}

fn rejects_result_memory(door: FrontDoor, analyze: bool) {
    let mut database = fixture();
    database.config.execution_memory.query_memory_bytes = NonZeroUsize::MIN;
    let error = query(
        &mut database,
        door,
        &explain_sql(analyze),
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: None,
        },
    )
    .unwrap_err();
    assert!(matches!(error, crate::HawDBError::Execution(_)), "{error}");
    assert!(
        error.to_string().contains("result_materialization"),
        "{error}"
    );
    assert!(error.to_string().contains("1-byte budget"), "{error}");
}

#[test]
fn database_explain_rejects_oversized_payload() {
    rejects_payload(FrontDoor::Database, false);
}

#[test]
fn database_analyze_rejects_oversized_payload() {
    rejects_payload(FrontDoor::Database, true);
}

#[test]
fn snapshot_explain_rejects_oversized_payload() {
    rejects_payload(FrontDoor::Snapshot, false);
}

#[test]
fn snapshot_analyze_rejects_oversized_payload() {
    rejects_payload(FrontDoor::Snapshot, true);
}

#[test]
fn transaction_explain_rejects_oversized_payload() {
    rejects_payload(FrontDoor::Transaction, false);
}

#[test]
fn transaction_analyze_rejects_oversized_payload() {
    rejects_payload(FrontDoor::Transaction, true);
}

#[test]
fn database_explain_rejects_oversized_result_memory() {
    rejects_result_memory(FrontDoor::Database, false);
}

#[test]
fn database_analyze_rejects_oversized_result_memory() {
    rejects_result_memory(FrontDoor::Database, true);
}

#[test]
fn snapshot_explain_rejects_oversized_result_memory() {
    rejects_result_memory(FrontDoor::Snapshot, false);
}

#[test]
fn snapshot_analyze_rejects_oversized_result_memory() {
    rejects_result_memory(FrontDoor::Snapshot, true);
}

#[test]
fn transaction_explain_rejects_oversized_result_memory() {
    rejects_result_memory(FrontDoor::Transaction, false);
}

#[test]
fn transaction_analyze_rejects_oversized_result_memory() {
    rejects_result_memory(FrontDoor::Transaction, true);
}

#[test]
fn exact_report_payload_preserves_all_front_doors() {
    for analyze in [false, true] {
        let mut database = fixture();
        let sql = explain_sql(analyze);
        let expected = database.query_sql_with_params(&sql, &parameters()).unwrap();
        for door in [
            FrontDoor::Database,
            FrontDoor::Snapshot,
            FrontDoor::Transaction,
        ] {
            let actual = query(
                &mut database,
                door,
                &sql,
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(expected.payload_bytes()),
                },
            )
            .unwrap();
            assert_eq!(actual, expected, "{door:?}, analyze={analyze}");
            let error = query(
                &mut database,
                door,
                &sql,
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(expected.payload_bytes() - 1),
                },
            )
            .unwrap_err();
            assert!(error.to_string().contains("payload"), "{error}");
        }
    }
}

#[test]
fn declared_positive_limit_still_obeys_row_cap() {
    for analyze in [false, true] {
        for door in [
            FrontDoor::Database,
            FrontDoor::Snapshot,
            FrontDoor::Transaction,
        ] {
            let mut database = fixture();
            let error = query(
                &mut database,
                door,
                &explain_sql(analyze),
                QueryStreamOptions {
                    max_rows: Some(0),
                    max_payload_bytes: None,
                },
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("LIMIT must be between 1 and 0"),
                "{error}"
            );
        }
    }
}

#[test]
fn configured_payload_limit_remains_a_hard_bound() {
    for analyze in [false, true] {
        for door in [FrontDoor::Database, FrontDoor::Snapshot] {
            let mut database = fixture();
            database.config.max_read_result_payload_bytes = Some(1);
            let error = query(
                &mut database,
                door,
                &explain_sql(analyze),
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(1024 * 1024),
                },
            )
            .unwrap_err();
            assert!(error.to_string().contains("payload"), "{error}");
        }
    }
}

#[test]
fn parameter_binding_preserves_report_contents_and_analyze_counts() {
    for analyze in [false, true] {
        for door in [
            FrontDoor::Database,
            FrontDoor::Snapshot,
            FrontDoor::Transaction,
        ] {
            let mut database = fixture();
            database
                .query_sql("INSERT INTO events (stream_id, sequence, payload) VALUES ('empty', 1, 'one'), ('other', 1, 'other')")
                .unwrap();
            let output = query(
                &mut database,
                door,
                &explain_sql(analyze),
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: None,
                },
            )
            .unwrap();
            assert_eq!(output.rows.len(), 1);
            let row = &output.rows[0];
            assert_eq!(
                row.get("id"),
                Some(&Value::String("StrictAppendPartitionScan_1".into()))
            );
            assert_eq!(
                row.get("access object"),
                Some(&Value::String("table:events".into()))
            );
            assert_eq!(row.get("estRows"), Some(&Value::Int(1)));
            assert_eq!(row.get("actRows"), analyze.then_some(&Value::Int(1)));
            if analyze {
                assert!(
                    matches!(row.get("execution info"), Some(Value::String(info)) if info.contains("rows_decoded="))
                );
            }
        }
    }
}

#[test]
fn analyze_data_payload_is_admitted_separately_from_the_report() {
    for door in [
        FrontDoor::Database,
        FrontDoor::Snapshot,
        FrontDoor::Transaction,
    ] {
        let mut database = fixture();
        let sql = explain_sql(true);
        let baseline = database.query_sql_with_params(&sql, &parameters()).unwrap();
        let options = QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(baseline.payload_bytes()),
        };
        assert_eq!(query(&mut database, door, &sql, options).unwrap(), baseline);
        database
            .query_sql_with_params(
                "INSERT INTO events (stream_id, sequence, payload) VALUES ($1, 1, $2)",
                &[
                    parameters()[0].clone(),
                    Value::String("x".repeat(baseline.payload_bytes() * 2)),
                ],
            )
            .unwrap();
        let error = query(&mut database, door, &sql, options).unwrap_err();
        assert!(error.to_string().contains("payload"), "{error}");
    }
}

#[test]
fn adequate_runtime_reservations_preserve_complete_reports() {
    for analyze in [false, true] {
        let mut database = fixture();
        let sql = explain_sql(analyze);
        let expected = database.query_sql_with_params(&sql, &parameters()).unwrap();
        let context = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(16 * 1024 * 1024, 1024 * 1024));
        let actual = database
            .begin_read_transaction_with_context(&context)
            .unwrap()
            .query_sql_with_params_options(
                &sql,
                &parameters(),
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(expected.payload_bytes()),
                },
            )
            .unwrap();
        assert_eq!(actual, expected);
        let mut transaction = database.begin_transaction_with_context(&context).unwrap();
        assert_eq!(
            transaction
                .query_sql_with_params(&sql, &parameters())
                .unwrap(),
            expected
        );
        transaction.rollback();
    }
}

#[test]
fn report_rejection_preserves_explicit_transaction_writes() {
    for analyze in [false, true] {
        let mut database = fixture();
        database.config.max_read_result_payload_bytes = Some(1);
        let mut transaction = database.begin_transaction().unwrap();
        transaction
            .query_sql(
                "INSERT INTO events (stream_id, sequence, payload) VALUES ('written', 1, 'one')",
            )
            .unwrap();
        let error = transaction
            .query_sql_with_params(&explain_sql(analyze), &parameters())
            .unwrap_err();
        assert!(error.to_string().contains("payload"), "{error}");
        transaction
            .query_sql(
                "INSERT INTO events (stream_id, sequence, payload) VALUES ('written', 2, 'two')",
            )
            .unwrap();
        transaction.commit().unwrap();
        database.config.max_read_result_payload_bytes = None;
        let rows = database
            .query_sql("SELECT payload FROM events WHERE stream_id = 'written' ORDER BY sequence ASC LIMIT 2")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0].get("payload"),
            Some(&Value::String("one".into()))
        );
        assert_eq!(
            rows.rows[1].get("payload"),
            Some(&Value::String("two".into()))
        );
    }
}

#[test]
fn snapshot_report_obeys_runtime_memory_reservations() {
    for analyze in [false, true] {
        for reservation in [
            (16 * 1024 * 1024, 1),
            (16 * 1024 * 1024, 0),
            (1, 1024 * 1024),
        ] {
            let database = fixture();
            let snapshot = database.begin_read_transaction().unwrap();
            let context = RuntimeTaskContext::default().with_memory_reservation(
                RuntimeMemoryReservation::new(reservation.0, reservation.1),
            );
            let error = snapshot
                .query_sql_with_params_options_context(
                    &explain_sql(analyze),
                    &parameters(),
                    QueryStreamOptions {
                        max_rows: Some(1),
                        max_payload_bytes: None,
                    },
                    &context,
                )
                .unwrap_err();
            assert!(matches!(error, crate::HawDBError::Execution(_)), "{error}");
            assert!(error.to_string().contains("query"), "{error}");
        }
    }
}

#[test]
fn transaction_report_obeys_runtime_result_reservation() {
    for analyze in [false, true] {
        let mut database = fixture();
        let context = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(16 * 1024 * 1024, 1));
        let mut transaction = database.begin_transaction_with_context(&context).unwrap();
        let error = transaction
            .query_sql_with_params(&explain_sql(analyze), &parameters())
            .unwrap_err();
        assert!(
            error.to_string().contains("payload")
                || error.to_string().contains("result_materialization"),
            "{error}"
        );
        transaction.rollback();
    }
}
