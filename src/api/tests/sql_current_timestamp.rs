// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `CURRENT_TIMESTAMP`, `now()`, `LOCALTIMESTAMP` and clock column defaults
//! (#1453): transaction-start stability, per-execution binding, precision,
//! index bounds and durable recorded defaults.

use super::*;
use hawdb_core::sql_timestamp;
use hawdb_core::temporal::{parse_datetime, with_statement_clock, DateTime, Utc};
use std::time::Duration;

fn instant(text: &str) -> DateTime<Utc> {
    parse_datetime(text).unwrap().with_timezone(&Utc)
}

fn tz(text: &str) -> Value {
    sql_timestamp::zoned_value(sql_timestamp::parse_timestamptz(text).unwrap()).unwrap()
}

fn ts(text: &str) -> Value {
    sql_timestamp::local_value(sql_timestamp::parse_timestamp(text).unwrap()).unwrap()
}

fn micros(value: &Value) -> i64 {
    match value {
        Value::DateTime(value) => sql_timestamp::from_datetime(*value).unwrap(),
        Value::LocalDateTime(value) => sql_timestamp::from_naive(*value).unwrap(),
        other => panic!("expected a timestamp, got {other:?}"),
    }
}

fn single(output: QueryOutput, column: &str) -> Value {
    assert_eq!(output.rows.len(), 1, "{output:?}");
    output.rows[0][column].clone()
}

fn pause() {
    std::thread::sleep(Duration::from_millis(3));
}

fn explain_uses(database: &mut Database, sql: &str, operator: &str) -> bool {
    database
        .query_sql(&format!("EXPLAIN {sql}"))
        .unwrap()
        .rows
        .iter()
        .any(|row| matches!(row.get("id"), Some(Value::String(id)) if id.contains(operator)))
}

#[test]
fn sql_clock_is_the_transaction_start_for_every_statement_and_default() {
    let mut database = Database::new();
    database
        .query_sql(
            "CREATE TABLE stamped (id BIGINT PRIMARY KEY, at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, \
             local TIMESTAMP DEFAULT LOCALTIMESTAMP, seen TIMESTAMPTZ DEFAULT now())",
        )
        .unwrap();
    let before = micros(&single(
        database.query_sql("SELECT CURRENT_TIMESTAMP AS t").unwrap(),
        "t",
    ));
    pause();
    let mut transaction = database.begin_transaction().unwrap();
    let first = single(
        transaction
            .query_sql("SELECT CURRENT_TIMESTAMP AS t")
            .unwrap(),
        "t",
    );
    pause();
    let row = transaction
        .query_sql("SELECT now() AS n, LOCALTIMESTAMP AS l, CURRENT_TIMESTAMP(3) AS m")
        .unwrap()
        .rows;
    assert_eq!(row[0]["n"], first);
    assert_eq!(micros(&row[0]["l"]), micros(&first), "UTC wall-clock time");
    assert!(matches!(row[0]["l"], Value::LocalDateTime(_)));
    assert_eq!(
        micros(&row[0]["m"]),
        sql_timestamp::TimestampPrecision::new(3)
            .unwrap()
            .round(micros(&first))
            .unwrap()
    );
    pause();
    transaction
        .query_sql("INSERT INTO stamped (id) VALUES (1), (2)")
        .unwrap();
    pause();
    transaction
        .query_sql("INSERT INTO stamped (id, seen) VALUES (3, CURRENT_TIMESTAMP)")
        .unwrap();
    let defaults = transaction
        .query_sql("SELECT at, local, seen FROM stamped ORDER BY id")
        .unwrap()
        .rows;
    for row in &defaults {
        assert_eq!(row["at"], first);
        assert_eq!(row["seen"], first);
        assert_eq!(micros(&row["local"]), micros(&first));
    }
    // Cypher keeps its own per-statement clock inside the same transaction.
    pause();
    let cypher = single(
        transaction
            .query("UNWIND [1] AS x RETURN datetime() AS d")
            .unwrap(),
        "d",
    );
    assert!(micros(&cypher) > micros(&first), "{cypher:?} {first:?}");
    transaction.commit().unwrap();
    assert!(micros(&first) > before);

    pause();
    let mut later = database.begin_transaction().unwrap();
    let second = single(later.query_sql("SELECT now() AS t").unwrap(), "t");
    later.rollback();
    assert!(micros(&second) > micros(&first));
    let read = database.begin_read_transaction().unwrap();
    let pinned = single(
        read.query_sql("SELECT CURRENT_TIMESTAMP AS t").unwrap(),
        "t",
    );
    pause();
    assert_eq!(
        single(read.query_sql("SELECT now() AS t").unwrap(), "t"),
        pinned
    );
}

#[test]
fn autocommit_statements_read_one_instant_and_cached_templates_rebind_it() {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE numbers (id BIGINT PRIMARY KEY)")
        .unwrap();
    let values = (1..=200).map(|id| format!("({id})")).collect::<Vec<_>>();
    database
        .query_sql(&format!(
            "INSERT INTO numbers (id) VALUES {}",
            values.join(",")
        ))
        .unwrap();
    let rows = database
        .query_sql("SELECT id, CURRENT_TIMESTAMP AS a, now() AS b FROM numbers")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 200);
    assert!(rows
        .iter()
        .all(|row| row["a"] == rows[0]["a"] && row["b"] == rows[0]["a"]));

    let sql = "SELECT CURRENT_TIMESTAMP AS t FROM numbers WHERE id = $1";
    let before = database.relational_plan_template_cache_stats().unwrap();
    let mut seen = Vec::new();
    for _ in 0..3 {
        seen.push(micros(&single(
            database
                .query_sql_with_params(sql, &[Value::Int(7)])
                .unwrap(),
            "t",
        )));
        pause();
    }
    let after = database.relational_plan_template_cache_stats().unwrap();
    assert!(after.hits >= before.hits + 2, "{before:?} {after:?}");
    assert!(seen.windows(2).all(|pair| pair[0] < pair[1]), "{seen:?}");
}

#[test]
fn clock_functions_round_to_precision_and_defaults_round_to_the_column() {
    let mut database = Database::new();
    database
        .query_sql(
            "CREATE TABLE rounded (id BIGINT PRIMARY KEY, whole TIMESTAMPTZ(0) DEFAULT now(), \
             millis TIMESTAMP(3) DEFAULT LOCALTIMESTAMP, full TIMESTAMP DEFAULT CURRENT_TIMESTAMP(1))",
        )
        .unwrap();
    with_statement_clock(instant("2026-11-05T12:00:00.987654Z"), || {
        let row = database
            .query_sql(
                "SELECT CURRENT_TIMESTAMP(0) AS a, LOCALTIMESTAMP(2) AS b, \
                 CURRENT_TIMESTAMP AS c, CAST(now() AS TEXT) AS d",
            )
            .unwrap()
            .rows;
        assert_eq!(row[0]["a"], tz("2026-11-05 12:00:01Z"));
        assert_eq!(row[0]["b"], ts("2026-11-05 12:00:00.99"));
        assert_eq!(row[0]["c"], tz("2026-11-05 12:00:00.987654Z"));
        assert_eq!(
            row[0]["d"],
            Value::String("2026-11-05 12:00:00.987654+00".into())
        );
        database
            .query_sql("INSERT INTO rounded (id) VALUES (1)")
            .unwrap();
    });
    let row = database
        .query_sql("SELECT whole, millis, full FROM rounded")
        .unwrap()
        .rows;
    assert_eq!(row[0]["whole"], tz("2026-11-05 12:00:01Z"));
    assert_eq!(row[0]["millis"], ts("2026-11-05 12:00:00.988"));
    // CURRENT_TIMESTAMP(1) rounds first; the TIMESTAMP column keeps 6 digits.
    assert_eq!(row[0]["full"], ts("2026-11-05 12:00:01"));
    let defaults = database
        .query_sql(
            "SELECT column_name, column_default FROM information_schema.columns \
             WHERE table_name = 'rounded' ORDER BY ordinal_position",
        )
        .unwrap()
        .rows;
    assert_eq!(
        defaults
            .iter()
            .map(|row| row["column_default"].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Null,
            Value::String("CURRENT_TIMESTAMP".into()),
            Value::String("LOCALTIMESTAMP".into()),
            Value::String("CURRENT_TIMESTAMP(1)".into()),
        ]
    );
}

#[test]
fn clock_defaults_record_the_insert_instant_across_wal_replay_and_checkpoint() {
    let path = unique_test_dir("sql-current-timestamp-replay");
    let mut database = Database::open(&path).unwrap();
    database
        .query_sql(
            "CREATE TABLE journal (id BIGINT PRIMARY KEY, at TIMESTAMPTZ NOT NULL DEFAULT now(), \
             local TIMESTAMP(0) DEFAULT LOCALTIMESTAMP(0))",
        )
        .unwrap();
    database
        .query_sql("INSERT INTO journal (id) VALUES (1)")
        .unwrap();
    pause();
    let mut transaction = database.begin_transaction().unwrap();
    transaction
        .query_sql("INSERT INTO journal (id) VALUES (2)")
        .unwrap();
    transaction.commit().unwrap();
    let recorded = database
        .query_sql("SELECT id, at, local FROM journal ORDER BY id")
        .unwrap()
        .rows;
    assert!(micros(&recorded[0]["at"]) < micros(&recorded[1]["at"]));
    drop(database);
    pause();

    let mut database = Database::open(&path).unwrap();
    assert_eq!(
        database
            .query_sql("SELECT id, at, local FROM journal ORDER BY id")
            .unwrap()
            .rows,
        recorded,
        "WAL replay must not re-evaluate the clock default"
    );
    database.checkpoint().unwrap();
    drop(database);
    let mut database = Database::open(&path).unwrap();
    assert_eq!(
        database
            .query_sql("SELECT id, at, local FROM journal ORDER BY id")
            .unwrap()
            .rows,
        recorded
    );
    drop(database);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn clock_bounds_drive_index_range_seeks() {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE deadlines (id BIGINT PRIMARY KEY, due TIMESTAMPTZ NOT NULL)")
        .unwrap();
    database
        .query_sql("CREATE INDEX deadlines_due ON deadlines (due)")
        .unwrap();
    database
        .query_sql("CREATE TABLE slots (at TIMESTAMP PRIMARY KEY, id BIGINT)")
        .unwrap();
    database
        .query_sql(
            "INSERT INTO deadlines (id, due) VALUES (1, '2000-01-01Z'), (2, '2999-01-01Z'), \
             (3, '2998-01-01Z')",
        )
        .unwrap();
    database
        .query_sql("INSERT INTO slots (at, id) VALUES ('2000-01-01', 1), ('2999-01-01', 2)")
        .unwrap();
    let secondary = "SELECT id FROM deadlines WHERE due > CURRENT_TIMESTAMP ORDER BY id";
    assert!(explain_uses(&mut database, secondary, "IndexRangeScanExec"));
    let ids = |database: &mut Database, sql: &str| {
        database
            .query_sql(sql)
            .unwrap()
            .rows
            .into_iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        ids(&mut database, secondary),
        vec![Value::Int(2), Value::Int(3)]
    );
    let primary = "SELECT id FROM slots WHERE at <= LOCALTIMESTAMP";
    assert!(explain_uses(&mut database, primary, "TableRangeScanExec"));
    assert_eq!(ids(&mut database, primary), vec![Value::Int(1)]);
    database
        .query_sql("UPDATE deadlines SET due = now() WHERE due < now()")
        .unwrap();
    assert_eq!(
        ids(
            &mut database,
            "SELECT id FROM deadlines WHERE due > '2001-01-01Z' AND due < '2100-01-01Z' ORDER BY id"
        ),
        vec![Value::Int(1)]
    );
}

#[test]
fn clock_functions_reject_invalid_shapes() {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE filled (id BIGINT PRIMARY KEY, at TIMESTAMP)")
        .unwrap();
    database
        .query_sql("INSERT INTO filled (id) VALUES (1)")
        .unwrap();
    for (sql, expected) in [
        ("SELECT CURRENT_TIMESTAMP(7)", "between 0 and 6"),
        ("SELECT CURRENT_TIMESTAMP()", "no parentheses"),
        ("SELECT now(3)", "now() takes no arguments"),
        ("SELECT LOCALTIMESTAMP(id) FROM filled", "integer precision"),
        (
            "SELECT id FROM filled WHERE at < CURRENT_TIMESTAMP",
            "incompatible",
        ),
        (
            "CREATE TABLE bad (id BIGINT PRIMARY KEY, n BIGINT DEFAULT now())",
            "not TIMESTAMP or TIMESTAMPTZ",
        ),
        (
            "CREATE TABLE bad (id BIGINT PRIMARY KEY, t TEXT DEFAULT CAST(now() AS TEXT))",
            "unsupported PostgreSQL column default expression",
        ),
        (
            "ALTER TABLE filled ADD COLUMN seen TIMESTAMPTZ DEFAULT now()",
            "clock default to a non-empty table",
        ),
    ] {
        let error = database.query_sql(sql).unwrap_err().to_string();
        assert!(error.contains(expected), "{sql}: {error}");
    }
    // A quoted identifier remains an ordinary column name.
    database
        .query_sql("CREATE TABLE quoted (\"current_timestamp\" BIGINT PRIMARY KEY)")
        .unwrap();
    database.query_sql("INSERT INTO quoted VALUES (5)").unwrap();
    assert_eq!(
        single(
            database
                .query_sql("SELECT \"current_timestamp\" FROM quoted")
                .unwrap(),
            "current_timestamp"
        ),
        Value::Int(5)
    );
}
