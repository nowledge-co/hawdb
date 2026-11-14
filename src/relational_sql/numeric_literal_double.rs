// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Numeric literals beside DOUBLE PRECISION operands (#1516). PostgreSQL types
//! `12.5` as numeric and resolves `float8 op numeric` through the implicit
//! numeric-to-float8 cast, so each literal form here must behave exactly like
//! its `CAST(... AS DOUBLE PRECISION)` spelling in every expression position.
//! DECIMAL columns, parameters and casts beside DOUBLE PRECISION keep #1429's
//! explicit-CAST rule.

use crate::{Database, Value};

fn database() -> Database {
    let mut database = Database::new();
    database
        .query_sql(
            "CREATE TABLE acct (\
                id BIGINT PRIMARY KEY, \
                n BIGINT NOT NULL, \
                balance DOUBLE PRECISION NOT NULL, \
                note DOUBLE PRECISION, \
                amount DECIMAL(12,2) NOT NULL\
            )",
        )
        .expect("create acct");
    database
        .query_sql("CREATE INDEX acct_balance ON acct (balance)")
        .expect("index balance");
    for (id, balance, note) in [
        (1, Value::Float(100.0), Value::Null),
        (2, Value::Float(12.5), Value::Float(2.0)),
        (3, Value::Float(-0.1), Value::Null),
        (4, Value::Float(13.5), Value::Float(-7.25)),
    ] {
        database
            .query_sql_with_params(
                "INSERT INTO acct VALUES ($1, $2, $3, $4, CAST($2 AS DECIMAL(12,2)))",
                &[Value::Int(id), Value::Int(id * 3), balance, note],
            )
            .expect("insert acct");
    }
    database
}

fn snapshot(database: &mut Database) -> hawdb_executor::QueryRows {
    database
        .query_sql("SELECT * FROM acct ORDER BY id")
        .expect("snapshot acct")
        .rows
}

/// Runs both spellings against identical fresh databases and compares the
/// statement result and the resulting table.
fn assert_same(literal: &str, cast: &str, parameters: &[Value]) {
    let mut literal_database = database();
    let mut cast_database = database();
    let literal_rows = literal_database
        .query_sql_with_params(literal, parameters)
        .unwrap_or_else(|error| panic!("{literal}: {error}"))
        .rows;
    let cast_rows = cast_database
        .query_sql_with_params(cast, parameters)
        .unwrap_or_else(|error| panic!("{cast}: {error}"))
        .rows;
    assert_eq!(literal_rows, cast_rows, "{literal}");
    assert_eq!(
        snapshot(&mut literal_database),
        snapshot(&mut cast_database),
        "{literal}"
    );
}

#[test]
fn numeric_literals_beside_double_match_the_explicit_cast() {
    const D: &str = "DOUBLE PRECISION";
    let mut cases = Vec::new();
    // Every arithmetic operator and both operand orders in UPDATE SET.
    for op in ["+", "-", "*", "/", "%"] {
        cases.push((
            format!("UPDATE acct SET balance = balance {op} 12.5 WHERE id >= 1"),
            format!("UPDATE acct SET balance = balance {op} CAST(12.5 AS {D}) WHERE id >= 1"),
            Vec::new(),
        ));
        cases.push((
            format!("UPDATE acct SET balance = 7.5 {op} balance WHERE id = 1"),
            format!("UPDATE acct SET balance = CAST(7.5 AS {D}) {op} balance WHERE id = 1"),
            Vec::new(),
        ));
    }
    // Literal forms: unary minus, parentheses, exponent, folded literal
    // arithmetic (exact, then converted once), and a fraction with no exact
    // binary representation.
    for (literal, cast) in [
        ("-12.5", format!("CAST(-12.5 AS {D})")),
        ("(-12.5)", format!("CAST(-12.5 AS {D})")),
        ("(12.5)", format!("CAST(12.5 AS {D})")),
        ("1e3", format!("CAST(1e3 AS {D})")),
        ("2.5E-1", format!("CAST(0.25 AS {D})")),
        ("(12.5 + 1.0)", format!("CAST(13.5 AS {D})")),
        ("(0.1 + 0.2)", format!("CAST(0.3 AS {D})")),
        ("0.1", format!("CAST(0.1 AS {D})")),
        ("5", format!("CAST(5 AS {D})")),
    ] {
        cases.push((
            format!("UPDATE acct SET balance = balance + {literal} WHERE id >= 1"),
            format!("UPDATE acct SET balance = balance + {cast} WHERE id >= 1"),
            Vec::new(),
        ));
    }
    let cases = cases.into_iter().chain(
        [
            // Chained arithmetic keeps PostgreSQL's left-to-right DOUBLE steps.
            (
                "UPDATE acct SET balance = balance + 12.5 - 0.25 WHERE id = 2",
                "UPDATE acct SET balance = balance + CAST(12.5 AS DOUBLE PRECISION) \
                 - CAST(0.25 AS DOUBLE PRECISION) WHERE id = 2",
                Vec::new(),
            ),
            // A DOUBLE parameter is a DOUBLE operand.
            (
                "UPDATE acct SET note = $1 * 0.1 WHERE id = 1",
                "UPDATE acct SET note = $1 * CAST(0.1 AS DOUBLE PRECISION) WHERE id = 1",
                vec![Value::Float(3.0)],
            ),
            // WHERE in UPDATE and DELETE, including the indexed column.
            (
                "UPDATE acct SET n = n + 1 WHERE balance > 12.5",
                "UPDATE acct SET n = n + 1 WHERE balance > CAST(12.5 AS DOUBLE PRECISION)",
                Vec::new(),
            ),
            (
                "UPDATE acct SET n = 0 WHERE balance + 0.5 >= 13.0",
                "UPDATE acct SET n = 0 WHERE balance + CAST(0.5 AS DOUBLE PRECISION) \
                 >= CAST(13.0 AS DOUBLE PRECISION)",
                Vec::new(),
            ),
            (
                "DELETE FROM acct WHERE balance = -0.1",
                "DELETE FROM acct WHERE balance = CAST(-0.1 AS DOUBLE PRECISION)",
                Vec::new(),
            ),
            (
                "DELETE FROM acct WHERE 12.5 <= balance",
                "DELETE FROM acct WHERE CAST(12.5 AS DOUBLE PRECISION) <= balance",
                Vec::new(),
            ),
            // INSERT VALUES expressions with a DOUBLE operand.
            (
                "INSERT INTO acct VALUES (9, 0, $1 + 12.5, $1 * -0.1, 0)",
                "INSERT INTO acct VALUES (9, 0, $1 + CAST(12.5 AS DOUBLE PRECISION), \
                 $1 * CAST(-0.1 AS DOUBLE PRECISION), 0)",
                vec![Value::Float(0.7)],
            ),
            // SELECT list, WHERE, IN, COALESCE, aggregates and HAVING.
            (
                "SELECT id, balance + 12.5 AS x, -0.1 * balance AS y FROM acct ORDER BY id",
                "SELECT id, balance + CAST(12.5 AS DOUBLE PRECISION) AS x, \
                 CAST(-0.1 AS DOUBLE PRECISION) * balance AS y FROM acct ORDER BY id",
                Vec::new(),
            ),
            (
                "SELECT id FROM acct WHERE balance > 12.5 ORDER BY id",
                "SELECT id FROM acct WHERE balance > CAST(12.5 AS DOUBLE PRECISION) ORDER BY id",
                Vec::new(),
            ),
            (
                "SELECT id FROM acct WHERE balance = -0.1 OR balance <> 100.0 ORDER BY id",
                "SELECT id FROM acct WHERE balance = CAST(-0.1 AS DOUBLE PRECISION) \
                 OR balance <> CAST(100.0 AS DOUBLE PRECISION) ORDER BY id",
                Vec::new(),
            ),
            (
                "SELECT id FROM acct WHERE balance IN (12.5, -0.1, 3) ORDER BY id",
                "SELECT id FROM acct WHERE balance IN (CAST(12.5 AS DOUBLE PRECISION), \
                 CAST(-0.1 AS DOUBLE PRECISION), 3) ORDER BY id",
                Vec::new(),
            ),
            (
                "SELECT id, COALESCE(note, 12.5) AS x FROM acct ORDER BY id",
                "SELECT id, COALESCE(note, CAST(12.5 AS DOUBLE PRECISION)) AS x \
                 FROM acct ORDER BY id",
                Vec::new(),
            ),
            (
                "SELECT SUM(balance) + 0.5 AS total FROM acct",
                "SELECT SUM(balance) + CAST(0.5 AS DOUBLE PRECISION) AS total FROM acct",
                Vec::new(),
            ),
            (
                "SELECT n, SUM(balance) AS total FROM acct GROUP BY n \
                 HAVING SUM(balance) > 12.5",
                "SELECT n, SUM(balance) AS total FROM acct GROUP BY n \
                 HAVING SUM(balance) > CAST(12.5 AS DOUBLE PRECISION)",
                Vec::new(),
            ),
            (
                "SELECT a.id FROM acct AS a JOIN acct AS b ON a.id = b.id \
                 WHERE a.balance + 0.5 > b.note ORDER BY a.id",
                "SELECT a.id FROM acct AS a JOIN acct AS b ON a.id = b.id \
                 WHERE a.balance + CAST(0.5 AS DOUBLE PRECISION) > b.note ORDER BY a.id",
                Vec::new(),
            ),
            // FROM-less SELECT.
            (
                "SELECT CAST($1 AS DOUBLE PRECISION) * 0.1 AS x",
                "SELECT CAST($1 AS DOUBLE PRECISION) * CAST(0.1 AS DOUBLE PRECISION) AS x",
                vec![Value::Int(3)],
            ),
        ]
        .map(|(literal, cast, parameters)| (literal.to_owned(), cast.to_owned(), parameters)),
    );
    for (literal, cast, parameters) in cases {
        assert_same(&literal, &cast, &parameters);
    }
}

#[test]
fn coerced_literals_have_double_precision_results() {
    let mut database = database();
    database
        .query_sql("UPDATE acct SET balance = balance + 12.5 WHERE id = 1")
        .expect("issue #1516 statement");
    let rows = database
        .query_sql(
            "SELECT balance, balance + 0.1 AS tenth, \
             balance * 0.30000000000000001665334536937734810635 AS long \
             FROM acct WHERE id = 1",
        )
        .unwrap()
        .rows;
    assert_eq!(rows[0]["balance"], Value::Float(112.5));
    // The literal is the nearest double, then DOUBLE arithmetic applies.
    assert_eq!(rows[0]["tenth"], Value::Float(112.5 + 0.1));
    assert_eq!(
        rows[0]["long"],
        Value::Float(
            112.5
                * "0.30000000000000001665334536937734810635"
                    .parse::<f64>()
                    .unwrap()
        )
    );
}

#[test]
fn typed_decimal_operands_still_require_cast_beside_double() {
    let mut database = database();
    let before = snapshot(&mut database);
    for (sql, parameters, expected) in [
        (
            "UPDATE acct SET balance = balance + amount WHERE id = 1",
            Vec::new(),
            "explicit CAST",
        ),
        (
            "UPDATE acct SET balance = balance + CAST(12.5 AS DECIMAL(12,2)) WHERE id = 1",
            Vec::new(),
            "explicit CAST",
        ),
        (
            "UPDATE acct SET balance = balance + $1 WHERE id = 1",
            vec![Value::Decimal("12.5".parse().unwrap())],
            "explicit CAST",
        ),
        (
            "SELECT id FROM acct WHERE balance > amount",
            Vec::new(),
            "explicit CAST",
        ),
        (
            "SELECT balance + $1 AS x FROM acct",
            vec![Value::Decimal("12.5".parse().unwrap())],
            "explicit CAST",
        ),
        (
            "SELECT id FROM acct WHERE balance > $1",
            vec![Value::Decimal("12.5".parse().unwrap())],
            "requires CAST",
        ),
        // A DOUBLE operand does not turn a DECIMAL column into DOUBLE.
        (
            "UPDATE acct SET amount = amount + $1 WHERE id = 1",
            vec![Value::Float(1.5)],
            "explicit CAST",
        ),
    ] {
        let error = database
            .query_sql_with_params(sql, &parameters)
            .expect_err("DECIMAL beside DOUBLE PRECISION needs a CAST");
        assert!(error.to_string().contains(expected), "{sql}: {error}");
    }
    assert_eq!(snapshot(&mut database), before);

    // Without a DOUBLE operand the literal stays exact DECIMAL.
    let rows = database
        .query_sql("SELECT amount + 0.1 AS a, n + 0.5 AS b FROM acct WHERE id = 1")
        .unwrap()
        .rows;
    assert_eq!(rows[0]["a"], Value::Decimal("3.10".parse().unwrap()));
    assert_eq!(rows[0]["b"], Value::Decimal("3.5".parse().unwrap()));
}

/// PostgreSQL rounds numeric to bigint on assignment (`numeric_int8` rounds
/// half away from zero). HawDB has no rounding assignment cast into integer
/// columns — DOUBLE PRECISION into BIGINT and explicit CAST to BIGINT are
/// exact too — so a fractional value is refused with the real reason.
#[test]
fn fractional_decimal_into_bigint_reports_the_fraction() {
    let mut database = database();
    let before = snapshot(&mut database);
    for (sql, expected) in [
        (
            "UPDATE acct SET n = n + 1.5 WHERE id = 1",
            "fractional part; BIGINT requires an exact integer",
        ),
        (
            "UPDATE acct SET n = 1.5 WHERE id = 1",
            "fractional part; BIGINT requires an exact integer",
        ),
        (
            "UPDATE acct SET n = CAST(amount + 0.25 AS BIGINT) WHERE id = 1",
            "fractional part; BIGINT requires an exact integer",
        ),
        (
            "INSERT INTO acct VALUES (9, 2.5, 0, NULL, 0)",
            "fractional part; BIGINT requires an exact integer",
        ),
        (
            "UPDATE acct SET n = n + 9223372036854775807.0 WHERE id = 1",
            "outside the BIGINT range",
        ),
    ] {
        let error = database
            .query_sql(sql)
            .expect_err("fractional DECIMAL into BIGINT must fail");
        assert!(error.to_string().contains(expected), "{sql}: {error}");
        assert!(
            !error.to_string().contains("38 coefficient"),
            "{sql}: {error}"
        );
    }
    assert_eq!(snapshot(&mut database), before);
    database
        .query_sql("UPDATE acct SET n = n + 2.00 WHERE id = 1")
        .expect("integral DECIMAL into BIGINT");
}
