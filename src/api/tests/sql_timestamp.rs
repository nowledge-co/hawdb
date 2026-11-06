// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! SQL TIMESTAMP / TIMESTAMPTZ: DDL, literals, coercion, casts, ordering,
//! index access and durable recovery (#1453).

use super::*;
use crate::{RecoveryMode, StorageResidencyMode};
use hawdb_core::sql_timestamp;
use hawdb_executor::Row;
use hawdb_storage::config::RelationalIndexMode;

fn ts(text: &str) -> Value {
    sql_timestamp::local_value(sql_timestamp::parse_timestamp(text).unwrap()).unwrap()
}

fn tz(text: &str) -> Value {
    sql_timestamp::zoned_value(sql_timestamp::parse_timestamptz(text).unwrap()).unwrap()
}

fn column(database: &mut Database, sql: &str, name: &str) -> Vec<Value> {
    database
        .query_sql(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| row[name].clone())
        .collect()
}

fn explain_uses(database: &mut Database, sql: &str, operator: &str) -> bool {
    database
        .query_sql(&format!("EXPLAIN {sql}"))
        .unwrap()
        .rows
        .iter()
        .any(|row| matches!(row.get("id"), Some(Value::String(id)) if id.contains(operator)))
}

fn create_events(database: &mut Database) {
    database
        .query_sql(
            "CREATE TABLE events (id BIGINT PRIMARY KEY, at TIMESTAMP NOT NULL, \
             seen TIMESTAMP(3) WITH TIME ZONE, note TEXT)",
        )
        .unwrap();
    database
        .query_sql("CREATE INDEX events_at ON events (at)")
        .unwrap();
    database
        .query_sql("CREATE TABLE ts_keys (k TIMESTAMP(0) WITHOUT TIME ZONE PRIMARY KEY, v BIGINT)")
        .unwrap();
}

fn insert_first_batch(database: &mut Database) {
    database
        .query_sql(
            "INSERT INTO events (id, at, seen, note) VALUES \
             (1, TIMESTAMP '2026-11-05 12:00:00', TIMESTAMPTZ '2026-11-05 14:00:00+02', 'literal'), \
             (2, '2026-11-05T08:30:00.25', '2026-11-05 08:30:00.2504Z', 'text'), \
             (3, '1969-12-31 23:59:59.999999', NULL, 'before epoch')",
        )
        .unwrap();
    database
        .query_sql_with_params(
            "INSERT INTO events (id, at, seen, note) VALUES ($1, $2, $3, 'params')",
            &[
                Value::Int(4),
                ts("2026-11-06 00:00:00"),
                tz("2026-11-05 19:00:00-05:00"),
            ],
        )
        .unwrap();
    database
        .query_sql(
            "INSERT INTO ts_keys (k, v) VALUES ('2026-01-01 00:00:00.4', 1), \
             ('2026-01-01 00:00:00.5', 2), ('1999-12-31 23:59:59.5', 3)",
        )
        .unwrap();
}

fn insert_second_batch(database: &mut Database) {
    let mut transaction = database.begin_transaction().unwrap();
    transaction
        .query_sql(
            "UPDATE events SET note = 'updated', at = '2026-11-05 12:00:00.000001' WHERE id = 1",
        )
        .unwrap();
    transaction
        .query_sql("DELETE FROM events WHERE at < '1970-01-01'")
        .unwrap();
    transaction
        .query_sql(
            "INSERT INTO events (id, at, seen) VALUES (5, CAST('2026-11-07 01:02:03' AS TIMESTAMP), \
             CAST('2026-11-07 01:02:03.4567+00' AS TIMESTAMPTZ(3)))",
        )
        .unwrap();
    transaction.commit().unwrap();
}

fn assert_events(database: &mut Database, second_batch: bool) {
    let mut expected_ids = vec![Value::Int(3), Value::Int(2), Value::Int(1), Value::Int(4)];
    if second_batch {
        expected_ids = vec![Value::Int(2), Value::Int(1), Value::Int(4), Value::Int(5)];
    }
    assert_eq!(
        column(database, "SELECT id FROM events ORDER BY at, id", "id"),
        expected_ids
    );
    let first = database
        .query_sql("SELECT at, seen, note FROM events WHERE id = 1")
        .unwrap()
        .rows;
    let at = if second_batch {
        "2026-11-05 12:00:00.000001"
    } else {
        "2026-11-05 12:00:00"
    };
    assert_eq!(
        first,
        vec![Row::from([
            ("at".into(), ts(at)),
            ("seen".into(), tz("2026-11-05 12:00:00Z")),
            (
                "note".into(),
                Value::String(if second_batch { "updated" } else { "literal" }.into())
            ),
        ])]
    );
    // TIMESTAMPTZ(3) rounded the text input to milliseconds.
    assert_eq!(
        column(database, "SELECT seen FROM events WHERE id = 2", "seen"),
        vec![tz("2026-11-05 08:30:00.25Z")]
    );
    assert_eq!(
        column(database, "SELECT seen FROM events WHERE id = 4", "seen"),
        vec![tz("2026-11-06 00:00:00Z")]
    );
    if second_batch {
        assert_eq!(
            column(database, "SELECT seen FROM events WHERE id = 5", "seen"),
            vec![tz("2026-11-07 01:02:03.457Z")]
        );
    }
    // TIMESTAMP(0) keys round halfway values away from 2000-01-01.
    assert_eq!(
        column(database, "SELECT k FROM ts_keys ORDER BY k", "k"),
        vec![
            ts("1999-12-31 23:59:59"),
            ts("2026-01-01 00:00:00"),
            ts("2026-01-01 00:00:01")
        ]
    );
}

#[test]
fn sql_timestamp_wal_replay_checkpoint_and_reopen_recover_values() {
    for residency in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir("sql-timestamp-recovery");
        let config = DatabaseConfig {
            storage_residency_mode: residency,
            relational_index_mode: RelationalIndexMode::DemandPaged,
            ..Default::default()
        };
        let mut database = Database::open_with_config(&path, config.clone()).unwrap();
        create_events(&mut database);
        insert_first_batch(&mut database);
        assert_events(&mut database, false);
        drop(database);

        // WAL replay without a checkpoint.
        let mut database = Database::open_with_config(&path, config.clone()).unwrap();
        assert_events(&mut database, false);
        database.checkpoint().unwrap();
        insert_second_batch(&mut database);
        assert_events(&mut database, true);
        drop(database);

        // Checkpoint plus WAL tail.
        let mut database = Database::open_with_config(&path, config.clone()).unwrap();
        assert_events(&mut database, true);
        assert!(explain_uses(
            &mut database,
            "SELECT id FROM events WHERE at >= '2026-11-05' AND at < '2026-11-06'",
            "IndexRangeScanExec"
        ));
        assert_eq!(
            column(
                &mut database,
                "SELECT id FROM events WHERE at >= '2026-11-05' AND at < '2026-11-06' ORDER BY id",
                "id"
            ),
            vec![Value::Int(1), Value::Int(2)]
        );
        database.checkpoint().unwrap();
        drop(database);

        // Checkpoint only.
        let mut database = Database::open_with_config(&path, config).unwrap();
        assert_events(&mut database, true);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn sql_timestamp_torn_wal_tail_discards_only_the_incomplete_transaction() {
    let path = unique_test_dir("sql-timestamp-torn-tail");
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        recovery_mode: RecoveryMode::AutoRepairTornTail,
        ..Default::default()
    };
    let mut database = Database::open_with_config(&path, config.clone()).unwrap();
    create_events(&mut database);
    insert_first_batch(&mut database);
    database.checkpoint().unwrap();
    database
        .query_sql("INSERT INTO events (id, at) VALUES (10, '2030-01-01 00:00:00.000001')")
        .unwrap();
    let acknowledged = std::fs::metadata(active_wal_path(&path)).unwrap().len();
    database
        .query_sql("INSERT INTO events (id, at, seen) VALUES (11, '2031-01-01', '2031-01-01Z')")
        .unwrap();
    let complete = std::fs::metadata(active_wal_path(&path)).unwrap().len();
    drop(database);
    assert!(complete > acknowledged);
    let torn = acknowledged + (complete - acknowledged) / 2;
    std::fs::OpenOptions::new()
        .write(true)
        .open(active_wal_path(&path))
        .unwrap()
        .set_len(torn)
        .unwrap();

    let mut database = Database::open_with_config(&path, config).unwrap();
    assert_eq!(
        column(
            &mut database,
            "SELECT at FROM events WHERE id >= 10 ORDER BY id",
            "at"
        ),
        vec![ts("2030-01-01 00:00:00.000001")]
    );
    assert_events_after_tail(&mut database);
    drop(database);
    std::fs::remove_dir_all(path).unwrap();
}

fn assert_events_after_tail(database: &mut Database) {
    assert_eq!(
        column(database, "SELECT id FROM events ORDER BY at DESC", "id"),
        vec![
            Value::Int(10),
            Value::Int(4),
            Value::Int(1),
            Value::Int(2),
            Value::Int(3)
        ]
    );
}

fn seed_orders(database: &mut Database) {
    database
        .query_sql(
            "CREATE TABLE orders (o_entry_d TIMESTAMP NOT NULL, o_id BIGINT NOT NULL, \
             o_carrier_d TIMESTAMPTZ, PRIMARY KEY (o_entry_d, o_id))",
        )
        .unwrap();
    database
        .query_sql("CREATE INDEX orders_carrier ON orders (o_carrier_d)")
        .unwrap();
    database
        .query_sql("CREATE TABLE days (d TIMESTAMP PRIMARY KEY, label TEXT)")
        .unwrap();
    database
        .query_sql(
            "INSERT INTO orders (o_entry_d, o_id, o_carrier_d) VALUES \
             ('2026-01-01 10:00', 1, '2026-01-02 00:00Z'), \
             ('2026-01-01 10:00', 2, NULL), \
             ('2026-01-02 09:00', 3, '2026-01-03 00:00+01'), \
             ('1960-06-01 00:00', 4, '1960-06-02 00:00Z'), \
             ('2026-01-02 09:00', 5, '2026-01-02 00:00Z')",
        )
        .unwrap();
    database
        .query_sql(
            "INSERT INTO days (d, label) VALUES ('2026-01-01 10:00', 'first'), \
             ('2026-01-02 09:00', 'second'), ('2027-01-01', 'none')",
        )
        .unwrap();
}

#[test]
fn sql_timestamp_index_seeks_ordering_grouping_aggregates_and_joins() {
    let mut database = Database::new();
    seed_orders(&mut database);

    let pk_range = "SELECT o_id FROM orders WHERE o_entry_d >= '2026-01-01' \
                    AND o_entry_d < TIMESTAMP '2026-01-02 09:00' ORDER BY o_id";
    assert!(explain_uses(&mut database, pk_range, "TableRangeScanExec"));
    assert_eq!(
        column(&mut database, pk_range, "o_id"),
        vec![Value::Int(1), Value::Int(2)]
    );
    let point = "SELECT o_id FROM orders WHERE o_entry_d = '2026-01-02T09:00:00' AND o_id = 5";
    assert!(explain_uses(&mut database, point, "TablePointGetExec"));
    assert_eq!(column(&mut database, point, "o_id"), vec![Value::Int(5)]);
    let secondary = "SELECT o_id FROM orders WHERE o_carrier_d > '2026-01-01 23:00:00Z' \
                     AND o_carrier_d <= '2026-01-02 23:00:00Z' ORDER BY o_id";
    assert!(explain_uses(&mut database, secondary, "IndexRangeScanExec"));
    assert_eq!(
        column(&mut database, secondary, "o_id"),
        vec![Value::Int(1), Value::Int(3), Value::Int(5)]
    );
    let rows = database
        .query_sql_with_params(
            "SELECT o_id FROM orders WHERE o_carrier_d = $1 ORDER BY o_id",
            &[tz("2026-01-02 00:00:00Z")],
        )
        .unwrap()
        .rows;
    assert_eq!(
        rows.iter()
            .map(|row| row["o_id"].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(5)]
    );

    assert_eq!(
        column(
            &mut database,
            "SELECT o_id FROM orders ORDER BY o_carrier_d DESC NULLS LAST, o_id",
            "o_id"
        ),
        vec![
            Value::Int(3),
            Value::Int(1),
            Value::Int(5),
            Value::Int(4),
            Value::Int(2)
        ]
    );
    assert_eq!(
        column(
            &mut database,
            "SELECT o_id FROM orders WHERE o_carrier_d IS NULL",
            "o_id"
        ),
        vec![Value::Int(2)]
    );
    // Aggregate SELECT does not support ORDER BY; sort the groups here.
    let mut grouped = database
        .query_sql("SELECT o_entry_d, COUNT(*) AS n FROM orders GROUP BY o_entry_d")
        .unwrap()
        .rows
        .into_iter()
        .map(|row| (row["o_entry_d"].clone(), row["n"].clone()))
        .collect::<Vec<_>>();
    grouped.sort();
    assert_eq!(
        grouped,
        vec![
            (ts("1960-06-01 00:00"), Value::Int(1)),
            (ts("2026-01-01 10:00"), Value::Int(2)),
            (ts("2026-01-02 09:00"), Value::Int(2)),
        ]
    );
    let mut distinct = column(
        &mut database,
        "SELECT DISTINCT o_carrier_d FROM orders",
        "o_carrier_d",
    );
    distinct.sort_by(|left, right| match (left, right) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Greater,
        (_, Value::Null) => std::cmp::Ordering::Less,
        _ => left.cmp(right),
    });
    assert_eq!(
        distinct,
        vec![
            tz("1960-06-02 00:00Z"),
            tz("2026-01-02 00:00Z"),
            tz("2026-01-02 23:00Z"),
            Value::Null
        ]
    );
    let bounds = database
        .query_sql(
            "SELECT MIN(o_entry_d) AS first, MAX(o_entry_d) AS last, \
             MIN(o_carrier_d) AS early, MAX(o_carrier_d) AS late FROM orders",
        )
        .unwrap()
        .rows;
    assert_eq!(
        bounds,
        vec![Row::from([
            ("first".into(), ts("1960-06-01 00:00")),
            ("last".into(), ts("2026-01-02 09:00")),
            ("early".into(), tz("1960-06-02 00:00Z")),
            ("late".into(), tz("2026-01-02 23:00Z")),
        ])]
    );
    let joined = database
        .query_sql(
            "SELECT o.o_id, d.label FROM orders o JOIN days d ON o.o_entry_d = d.d \
             ORDER BY o.o_id",
        )
        .unwrap()
        .rows;
    assert_eq!(
        joined
            .iter()
            .map(|row| (row["o_id"].clone(), row["label"].clone()))
            .collect::<Vec<_>>(),
        vec![
            (Value::Int(1), Value::String("first".into())),
            (Value::Int(2), Value::String("first".into())),
            (Value::Int(3), Value::String("second".into())),
            (Value::Int(5), Value::String("second".into())),
        ]
    );
}

#[test]
fn sql_timestamp_casts_parameters_and_information_schema() {
    let mut database = Database::new();
    seed_orders(&mut database);
    let row = database
        .query_sql(
            "SELECT CAST(o_entry_d AS TEXT) AS local, o_carrier_d::text AS zoned, \
             CAST(o_carrier_d AS TIMESTAMP) AS utc, o_entry_d::timestamp(0) AS whole \
             FROM orders WHERE o_id = 3",
        )
        .unwrap()
        .rows;
    assert_eq!(
        row,
        vec![Row::from([
            ("local".into(), Value::String("2026-01-02 09:00:00".into())),
            (
                "zoned".into(),
                Value::String("2026-01-02 23:00:00+00".into())
            ),
            ("utc".into(), ts("2026-01-02 23:00:00")),
            ("whole".into(), ts("2026-01-02 09:00:00")),
        ])]
    );
    let constants = database
        .query_sql(
            "SELECT TIMESTAMP '2026-11-05 12:00:00.5' AS a, \
             CAST('2026-11-05 12:00:00.5' AS TIMESTAMP(0)) AS b, \
             TIMESTAMP WITH TIME ZONE '2026-11-05 12:00:00+05:30' AS c, \
             CAST(TIMESTAMPTZ '2026-11-05 12:00:00-01' AS TEXT) AS d",
        )
        .unwrap()
        .rows;
    assert_eq!(
        constants,
        vec![Row::from([
            ("a".into(), ts("2026-11-05 12:00:00.5")),
            ("b".into(), ts("2026-11-05 12:00:01")),
            ("c".into(), tz("2026-11-05 06:30:00Z")),
            ("d".into(), Value::String("2026-11-05 13:00:00+00".into())),
        ])]
    );
    // LocalDateTime and DateTime parameters round-trip through both types.
    database
        .query_sql("CREATE TABLE params (id BIGINT PRIMARY KEY, a TIMESTAMP, b TIMESTAMPTZ)")
        .unwrap();
    let local = ts("1901-02-03 04:05:06.789012");
    let zoned = Value::DateTime(
        hawdb_core::temporal::parse_datetime("2026-11-05T12:00:00.123456789+09:00").unwrap(),
    );
    database
        .query_sql_with_params(
            "INSERT INTO params (id, a, b) VALUES (1, $1, $2)",
            &[local.clone(), zoned],
        )
        .unwrap();
    let rows = database
        .query_sql_with_params(
            "SELECT a, b, CAST($1 AS TIMESTAMP) AS c FROM params WHERE a = $2",
            &[Value::String("2026-01-01 00:00".into()), local.clone()],
        )
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![Row::from([
            ("a".into(), local),
            ("b".into(), tz("2026-11-05 03:00:00.123457Z")),
            ("c".into(), ts("2026-01-01 00:00")),
        ])]
    );
    let Value::DateTime(b) = &rows[0]["b"] else {
        panic!("TIMESTAMPTZ is a zoned datetime");
    };
    assert_eq!(b.offset().local_minus_utc(), 0);
    // Text parameters coerce in assignment and comparison.
    database
        .query_sql_with_params(
            "INSERT INTO params (id, a) VALUES (2, $1)",
            &[Value::String("2026-02-03T04:05:06".into())],
        )
        .unwrap();
    assert_eq!(
        column(
            &mut database,
            "SELECT id FROM params WHERE a = '2026-02-03 04:05:06'",
            "id"
        ),
        vec![Value::Int(2)]
    );
    let columns = database
        .query_sql(
            "SELECT column_name, data_type, udt_name, datetime_precision \
             FROM information_schema.columns WHERE table_name = 'orders' ORDER BY ordinal_position",
        )
        .unwrap()
        .rows;
    assert_eq!(
        columns
            .iter()
            .map(|row| (
                row["data_type"].clone(),
                row["udt_name"].clone(),
                row["datetime_precision"].clone()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                Value::String("timestamp without time zone".into()),
                Value::String("timestamp".into()),
                Value::Int(6)
            ),
            (
                Value::String("bigint".into()),
                Value::String("int8".into()),
                Value::Null
            ),
            (
                Value::String("timestamp with time zone".into()),
                Value::String("timestamptz".into()),
                Value::Int(6)
            ),
        ]
    );
}

#[test]
fn sql_timestamp_rejects_invalid_literals_types_and_mixed_comparisons() {
    let mut database = Database::new();
    seed_orders(&mut database);
    for (sql, expected) in [
        (
            "SELECT TIMESTAMP '2026-01-01 00:00:00+02'",
            "must not carry a time zone offset",
        ),
        (
            "INSERT INTO days (d) VALUES ('2026-01-01T00:00:00Z')",
            "must not carry a time zone offset",
        ),
        (
            "SELECT TIMESTAMP '2026-02-30'",
            "date field value out of range",
        ),
        ("SELECT TIMESTAMP 'now'", "invalid timestamp"),
        ("SELECT TIMESTAMP 'infinity'", "invalid timestamp"),
        (
            "SELECT TIMESTAMP '+262143-01-01 00:00:00'",
            "invalid timestamp",
        ),
        (
            "SELECT DATE '2026-01-01'",
            "unsupported PostgreSQL typed literal",
        ),
        (
            "SELECT INTERVAL '1 day'",
            "unsupported PostgreSQL typed literal",
        ),
        ("CREATE TABLE bad (t TIMESTAMP(7))", "between 0 and 6"),
        (
            "CREATE TABLE bad (t TIMESTAMPTZ WITH TIME ZONE)",
            "TIMESTAMPTZ does not accept",
        ),
        (
            "SELECT CAST('2026-01-01' AS TIMESTAMP(9))",
            "between 0 and 6",
        ),
        ("SELECT CAST(1 AS TIMESTAMP)", "cannot convert"),
        (
            "SELECT o_id FROM orders WHERE o_entry_d = o_carrier_d",
            "incompatible",
        ),
        (
            "SELECT o_id FROM orders WHERE o_carrier_d = TIMESTAMP '2026-01-02 00:00'",
            "incompatible",
        ),
        (
            "SELECT o_id FROM orders WHERE o_entry_d = 'yesterday'",
            "invalid timestamp",
        ),
        ("SELECT CAST(o_id AS TEXT) FROM orders", "CAST to TEXT"),
        (
            "SELECT o_id FROM orders WHERE CAST(o_entry_d AS TEXT) = '2026-01-01 10:00:00'",
            "unsupported",
        ),
    ] {
        let error = database.query_sql(sql).unwrap_err().to_string();
        assert!(error.contains(expected), "{sql}: {error}");
    }
    let error = database
        .query_sql_with_params(
            "SELECT o_id FROM orders WHERE o_entry_d = $1",
            &[tz("2026-01-01 10:00Z")],
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("incompatible"), "{error}");
    let error = database
        .query_sql_with_params(
            "INSERT INTO days (d) VALUES ($1)",
            &[Value::Date(
                hawdb_core::temporal::parse_date("2026-01-01").unwrap(),
            )],
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("DATE, TIME and DURATION"), "{error}");
    // The maximum representable value round-trips; one microsecond more fails.
    let max = sql_timestamp::format_timestamp(sql_timestamp::max_micros()).unwrap();
    database
        .query_sql(&format!("INSERT INTO days (d) VALUES ('{max}')"))
        .unwrap();
    assert_eq!(
        column(&mut database, "SELECT MAX(d) AS m FROM days", "m"),
        vec![ts(&max)]
    );
    let error = database
        .query_sql(&format!("INSERT INTO days (d) VALUES ('{max}5')"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("out of range"), "{error}");
}
