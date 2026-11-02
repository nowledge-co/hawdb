// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_core::Decimal;
use hawdb_expression::sql::{bind_constant_numeric, bind_sql_value};

#[test]
fn numeric_frontends_bound_flat_arithmetic_and_cast_chains() {
    for expression in [
        format!("1{}", " + 1".repeat(300)),
        format!("1{}", "::DECIMAL(38,0)".repeat(300)),
    ] {
        for sql in [
            format!("SELECT {expression}"),
            format!("UPDATE amounts SET amount = {expression}"),
        ] {
            assert!(parse_postgres_sql(&sql).is_err());
        }
    }
}

#[test]
fn numeric_frontends_preserve_exact_digits_and_share_parameter_arithmetic() {
    let select = parse_postgres_sql("SELECT CAST(($1 + 0.2) * 10 / 3 AS DECIMAL(20,4))").unwrap();
    let SqlStatement::Select(select) = select else {
        panic!("SELECT")
    };
    let SelectProjection::Expression { expression, .. } = &select.projection[0] else {
        panic!("expression")
    };
    let parameters = [Value::Decimal("0.1".parse().unwrap())];
    let expected = Value::Decimal("1.0000".parse().unwrap());
    assert_eq!(
        bind_constant_numeric(expression, &parameters).unwrap(),
        expected
    );
    let insert = parse_postgres_sql(
        "INSERT INTO amounts (id, amount) VALUES (1, CAST(($1 + 0.2) * 10 / 3 AS DECIMAL(20,4)))",
    )
    .unwrap();
    let SqlStatement::Insert(insert) = insert else {
        panic!("INSERT")
    };
    assert_eq!(
        bind_sql_value(&insert.rows[0][1], &parameters).unwrap(),
        expected
    );
    let prepared = crate::prepare_postgres_sql(
        "INSERT INTO amounts (id, amount) VALUES ($1, ($2 + $3) * 0.5)",
    )
    .unwrap();
    assert_eq!(
        prepared
            .parameters
            .iter()
            .map(|parameter| parameter.position)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    let SqlStatement::Select(select) =
        parse_postgres_sql("SELECT 9007199254740993.01, -9223372036854775808, 1e-38").unwrap()
    else {
        panic!("SELECT")
    };
    let expected = [
        Value::Decimal("9007199254740993.01".parse().unwrap()),
        Value::Int(i64::MIN),
        Value::Decimal(Decimal::new(1, 38).unwrap()),
    ];
    for (projection, expected) in select.projection.iter().zip(expected) {
        let SelectProjection::Expression { expression, .. } = projection else {
            panic!("expression")
        };
        assert_eq!(bind_constant_numeric(expression, &[]).unwrap(), expected);
    }
}

#[test]
fn numeric_cast_forms_and_schema_validation_agree() {
    for sql in [
        "SELECT 1.235::DECIMAL(4,2)",
        "SELECT CAST(1.235 AS NUMERIC(4,2))",
    ] {
        let SqlStatement::Select(select) = parse_postgres_sql(sql).unwrap() else {
            panic!("SELECT")
        };
        let SelectProjection::Expression { expression, .. } = &select.projection[0] else {
            panic!("expression")
        };
        assert_eq!(
            bind_constant_numeric(expression, &[]).unwrap(),
            Value::Decimal("1.24".parse().unwrap())
        );
    }
    assert!(parse_postgres_sql(
        "CREATE TABLE amounts (id BIGINT PRIMARY KEY, amount DECIMAL(20,4))"
    )
    .is_ok());
    for specification in ["DECIMAL(0)", "DECIMAL(39)", "DECIMAL(2,3)", "DECIMAL(4,-1)"] {
        assert!(parse_postgres_sql(&format!(
            "CREATE TABLE amounts (id BIGINT PRIMARY KEY, amount {specification})"
        ))
        .is_err());
        assert!(parse_postgres_sql(&format!("SELECT 1::{specification}")).is_err());
    }
    assert!(parse_postgres_sql("SELECT CAST(1 AS DOUBLE PRECISION)").is_ok());
}
