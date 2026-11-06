// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_core::sql_timestamp::{self, TimestampCast, TimestampType};
use hawdb_core::TimestampPrecision;
use hawdb_expression::sql::bind_sql_value;

fn kind(zoned: bool, precision: u8) -> SqlDataType {
    SqlDataType::Timestamp(TimestampType {
        zoned,
        precision: TimestampPrecision::new(precision).unwrap(),
    })
}

#[test]
fn timestamp_ddl_accepts_precision_and_time_zone_clauses() {
    let SqlStatement::CreateTable(create) = parse_postgres_sql(
        "CREATE TABLE t (a TIMESTAMP, b TIMESTAMP(0) WITHOUT TIME ZONE, \
         c TIMESTAMP(3) WITH TIME ZONE, d TIMESTAMPTZ, e timestamptz(2))",
    )
    .unwrap() else {
        panic!("CREATE TABLE")
    };
    assert_eq!(
        create
            .columns
            .iter()
            .map(|column| column.data_type)
            .collect::<Vec<_>>(),
        vec![
            kind(false, 6),
            kind(false, 0),
            kind(true, 3),
            kind(true, 6),
            kind(true, 2)
        ]
    );
    for sql in [
        "CREATE TABLE t (a TIMESTAMP(7))",
        "CREATE TABLE t (a TIMESTAMP(-1))",
        "CREATE TABLE t (a TIMESTAMPTZ WITHOUT TIME ZONE)",
        "CREATE TABLE t (a TIMESTAMP WITH ZONE)",
    ] {
        assert!(parse_postgres_sql(sql).is_err(), "{sql}");
    }
}

#[test]
fn timestamp_literals_and_casts_lower_to_host_values() {
    let SqlStatement::Insert(insert) = parse_postgres_sql(
        "INSERT INTO t (a, b, c, d, e) VALUES (TIMESTAMP '2026-11-05 12:00:00', \
         TIMESTAMP WITH TIME ZONE '2026-11-05 14:00:00+02', '2026-11-05'::timestamptz(0), \
         CAST($1 AS TIMESTAMP WITHOUT TIME ZONE), CAST(TIMESTAMP '2026-11-05 12:00:00.5' AS TEXT))",
    )
    .unwrap() else {
        panic!("INSERT")
    };
    let local =
        sql_timestamp::local_value(sql_timestamp::parse_timestamp("2026-11-05 12:00:00").unwrap())
            .unwrap();
    let zoned = sql_timestamp::zoned_value(
        sql_timestamp::parse_timestamptz("2026-11-05 12:00:00Z").unwrap(),
    )
    .unwrap();
    let parameters = [Value::String("2026-11-05 12:00".into())];
    let values = insert.rows[0]
        .iter()
        .map(|value| bind_sql_value(value, &parameters).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(values[0], local);
    assert_eq!(values[1], zoned);
    assert!(matches!(values[2], Value::DateTime(_)));
    assert_eq!(values[3], local);
    assert_eq!(values[4], Value::String("2026-11-05 12:00:00.5".into()));
    let SqlStatement::Select(select) =
        parse_postgres_sql("SELECT CAST(at AS TIMESTAMPTZ(3)) FROM t").unwrap()
    else {
        panic!("SELECT")
    };
    let SelectProjection::Expression { expression, .. } = &select.projection[0] else {
        panic!("expression")
    };
    assert!(matches!(
        expression.kind,
        ExprKind::TimestampCast {
            target: TimestampCast::Timestamp(TimestampType { zoned: true, .. }),
            ..
        }
    ));
    for sql in [
        "SELECT TIMESTAMP '2026-11-05 12:00:00+02'",
        "SELECT TIMESTAMPTZ WITH TIME ZONE '2026-11-05'",
        "SELECT DATE '2026-11-05'",
        "SELECT TIME '12:00'",
        "SELECT CAST('x' AS BIGINT WITH TIME ZONE)",
        "SELECT CAST('2026-11-05' AS TIMESTAMP(1, 2))",
        "SELECT CAST('2026-11-05' AS TEXT(3))",
    ] {
        assert!(parse_postgres_sql(sql).is_err(), "{sql}");
    }
}
