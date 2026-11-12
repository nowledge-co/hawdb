// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Kùzu `string(x)` / `to_string(x)` through the embedded Cypher API (#1682).
//!
//! Nowledge Mem builds thread version stamps with Kùzu's cast and compares
//! stamps written before and after migration, so the text must equal Kùzu
//! 0.11.3 output. Expected strings were measured with Kùzu 0.11.3; the
//! HawDB spelling differs only where HawDB has no Kùzu constructor
//! (`timestamp(...)` returns epoch nanoseconds and there is no `interval`).

use super::*;
use hawdb_core::temporal;

/// The two halves of Mem's stamp
/// `COALESCE(string(t.updated_at), '') + '|' + COALESCE(string(t.message_count), '')`.
/// String `+` is not supported in HawDB Cypher yet (separate issue), so the
/// tests join the halves with `|` on the host; [`STAMP`] is the full Mem
/// expression, exercised by the ignored test at the end.
const STAMP_PARTS: &str = "MATCH (t:Thread) RETURN t.id AS id, \
    COALESCE(string(t.updated_at), '') AS updated, \
    COALESCE(string(t.message_count), '') AS count ORDER BY id";
const STAMP: &str = "MATCH (t:Thread) RETURN t.id AS id, \
    COALESCE(string(t.updated_at), '') + '|' + COALESCE(string(t.message_count), '') AS stamp \
    ORDER BY id";

fn stamps(db: &mut Database, query: &str) -> Vec<(Value, Value)> {
    let output = db.query(query).unwrap();
    output
        .rows
        .iter()
        .map(|row| {
            let (Value::String(updated), Value::String(count)) = (&row["updated"], &row["count"])
            else {
                panic!("stamp halves must be strings: {row:?}");
            };
            (
                row["id"].clone(),
                Value::String(format!("{updated}|{count}")),
            )
        })
        .collect()
}

fn expected_stamps() -> [(Value, Value); 5] {
    // Kùzu 0.11.3 returns the same stamps for a TIMESTAMP/INT64 Thread table.
    [
        ("t1", "2024-01-02 03:04:05|3"),
        ("t2", "2024-01-02 03:04:05.12|4"),
        ("t3", "2024-01-02 03:04:05.123456|0"),
        ("t4", "|5"),
        ("t5", "|"),
    ]
    .map(|(id, stamp)| (text(id), text(stamp)))
}

fn local_datetime(text: &str) -> Value {
    Value::LocalDateTime(temporal::parse_local_datetime(text).unwrap())
}

fn single(db: &mut Database, expression: &str) -> Value {
    let output = db
        .query(&format!("UNWIND [0] AS i RETURN {expression} AS v"))
        .unwrap_or_else(|error| panic!("{expression}: {error}"));
    assert_eq!(output.rows.len(), 1, "{expression}");
    output.rows[0]["v"].clone()
}

fn text(value: &str) -> Value {
    Value::String(value.to_owned())
}

#[test]
fn kuzu_string_matches_the_issue_table() {
    let mut db = Database::new();
    // (HawDB expression, Kùzu expression it stands for, Kùzu 0.11.3 result)
    let cases = [
        (
            "string(localdatetime('2024-01-02T03:04:05'))",
            "string(timestamp('2024-01-02 03:04:05'))",
            "2024-01-02 03:04:05",
        ),
        (
            "string(localdatetime('2024-01-02T03:04:05.120'))",
            "string(timestamp('2024-01-02 03:04:05.120'))",
            "2024-01-02 03:04:05.12",
        ),
        (
            "string(localdatetime('2024-01-02T03:04:05.123456'))",
            "string(timestamp('2024-01-02 03:04:05.123456'))",
            "2024-01-02 03:04:05.123456",
        ),
        (
            "string(datetime('2024-01-02T03:04:05Z'))",
            "string(timestamp('2024-01-02T03:04:05Z'))",
            "2024-01-02 03:04:05",
        ),
        (
            "string(datetime('2024-01-02T03:04:05+02:00'))",
            "string(timestamp('2024-01-02T03:04:05+02:00'))",
            "2024-01-02 01:04:05",
        ),
        (
            "string(date('2024-01-02'))",
            "string(date('2024-01-02'))",
            "2024-01-02",
        ),
        ("string(42)", "string(42)", "42"),
        ("string(-0.5)", "string(-0.5)", "-0.500000"),
        ("string(1.0)", "string(1.0)", "1.000000"),
        ("string(true)", "string(true)", "True"),
        ("string('abc')", "string('abc')", "abc"),
        ("string([1,2])", "string([1,2])", "[1,2]"),
        ("string({a:1})", "string({a:1})", "{a: 1}"),
        (
            "string(duration('P1DT2H'))",
            "string(interval('1 day 2 hours'))",
            "1 day 02:00:00",
        ),
        ("to_string(1)", "to_string(1)", "1"),
        ("STRING(7)", "STRING(7)", "7"),
        ("TO_STRING(true)", "TO_STRING(true)", "True"),
    ];
    for (expression, kuzu, expected) in cases {
        assert_eq!(single(&mut db, expression), text(expected), "{kuzu}");
    }
    // toString keeps its Neo4j text.
    assert_eq!(single(&mut db, "toString(1.0)"), text("1.0"));
    assert_eq!(single(&mut db, "toString(true)"), text("true"));
    assert_eq!(
        single(
            &mut db,
            "toString(localdatetime('2024-01-02T03:04:05.120'))"
        ),
        text("2024-01-02T03:04:05.120")
    );
}

#[test]
fn kuzu_string_null_composition_and_errors() {
    let mut db = Database::new();
    assert_eq!(single(&mut db, "string(null)"), Value::Null);
    assert_eq!(single(&mut db, "COALESCE(string(null), '')"), text(""));
    assert_eq!(single(&mut db, "COALESCE(string(i + 1), '')"), text("1"));
    assert_eq!(single(&mut db, "string([1, null, 'a'])"), text("[1,,a]"));
    let error = db
        .query("UNWIND [0] AS i RETURN string(localtime('01:02:03')) AS v")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("string() has no Kùzu-compatible text for a LOCAL TIME value"),
        "{error}"
    );
    // A variable named like the function still reads as a variable.
    let output = db.query("UNWIND [5] AS string RETURN string AS v").unwrap();
    assert_eq!(output.rows[0]["v"], Value::Int(5));
}

fn create_threads(db: &mut Database) {
    for (id, updated_at, count) in [
        ("t1", local_datetime("2024-01-02T03:04:05"), 3),
        ("t2", local_datetime("2024-01-02T03:04:05.120"), 4),
        (
            "t3",
            Value::DateTime(temporal::parse_datetime("2024-01-02T03:04:05.123456Z").unwrap()),
            0,
        ),
        ("t4", Value::Null, 5),
    ] {
        db.query_with_params(
            "CREATE (:Thread {id: $id, updated_at: $updated_at, message_count: $count})",
            &BTreeMap::from([
                ("id".to_owned(), text(id)),
                ("updated_at".to_owned(), updated_at),
                ("count".to_owned(), Value::Int(count)),
            ]),
        )
        .unwrap();
    }
    db.query("CREATE (:Thread {id: 't5'})").unwrap();
}

fn assert_stamps(db: &mut Database) {
    assert_eq!(stamps(db, STAMP_PARTS), expected_stamps());
}

#[test]
fn thread_version_stamp_matches_kuzu() {
    let mut db = Database::new();
    create_threads(&mut db);
    assert_stamps(&mut db);
    // Updating to a sub-second value and to NULL changes the stamp the same
    // way it does in Kùzu.
    db.query_with_params(
        "MATCH (t:Thread {id: 't1'}) SET t.updated_at = $updated_at",
        &BTreeMap::from([(
            "updated_at".to_owned(),
            local_datetime("2024-01-02T03:04:05.5"),
        )]),
    )
    .unwrap();
    db.query("MATCH (t:Thread {id: 't2'}) SET t.updated_at = NULL")
        .unwrap();
    let stamps = stamps(&mut db, STAMP_PARTS);
    assert_eq!(stamps[0], (text("t1"), text("2024-01-02 03:04:05.5|3")));
    assert_eq!(stamps[1], (text("t2"), text("|4")));
}

#[test]
fn thread_version_stamp_survives_wal_replay() {
    let path = unique_test_dir("kuzu_string_stamp_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        create_threads(&mut db);
    }
    {
        let mut db = Database::open(&path).unwrap();
        assert_stamps(&mut db);
    }
    std::fs::remove_dir_all(path).unwrap();
}

/// `string()` is type-driven. HawDB's Kùzu-compatible `timestamp(...)`,
/// `CAST(... AS TIMESTAMP)` and `CURRENT_TIMESTAMP()` return INT64 epoch
/// nanoseconds, so a property written through them renders as an integer,
/// not as Kùzu timestamp text. Kùzu-identical stamps need the property to
/// hold a native temporal value.
#[test]
fn epoch_nanosecond_timestamps_render_as_integers() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Thread {id: 't1', updated_at: timestamp('2024-01-02T03:04:05.120Z'), \
         message_count: 1})",
    )
    .unwrap();
    assert_eq!(
        stamps(&mut db, STAMP_PARTS),
        [(text("t1"), text("1704164645120000000|1"))]
    );
}

/// Mem's exact stamp expression. Blocked on string `+` concatenation, which
/// fails in HawDB Cypher with "arithmetic requires numeric operands"
/// independently of `string(x)`; tracked separately from #1682.
#[test]
#[ignore = "string + concatenation is unsupported in Cypher RETURN; tracked separately from #1682"]
fn thread_version_stamp_expression_matches_kuzu() {
    let mut db = Database::new();
    create_threads(&mut db);
    let output = db.query(STAMP).unwrap();
    let stamps = output
        .rows
        .iter()
        .map(|row| (row["id"].clone(), row["stamp"].clone()))
        .collect::<Vec<_>>();
    assert_eq!(stamps, expected_stamps());
}
