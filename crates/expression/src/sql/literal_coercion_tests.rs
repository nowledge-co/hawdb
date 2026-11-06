// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::NumericLiteralCoercion;
use crate::sql::{Expr, ExprKind, SqlColumnRef, SqlComparisonOp, SqlFunctionArgument, SqlValue};
use hawdb_core::numeric::{NumericBinaryOp, NumericType};
use hawdb_core::{DecimalType, LogicalType, Value};

fn column(name: &str) -> Expr {
    Expr::column(SqlColumnRef {
        qualifier: None,
        name: name.into(),
    })
}

fn literal(value: Value) -> Expr {
    Expr::value(SqlValue::Literal(value))
}

fn decimal(text: &str) -> Expr {
    literal(Value::Decimal(text.parse().unwrap()))
}

fn add(left: Expr, right: Expr) -> Expr {
    Expr::unspanned(ExprKind::Arithmetic {
        left: Box::new(left),
        op: NumericBinaryOp::Add,
        right: Box::new(right),
    })
}

fn types(column: &SqlColumnRef) -> Option<LogicalType> {
    match column.name.as_str() {
        "balance" => Some(LogicalType::Float64),
        "amount" => Some(LogicalType::Decimal),
        "n" => Some(LogicalType::Int64),
        _ => None,
    }
}

fn coerce(mut expression: Expr, parameters: &[Value]) -> (bool, Expr) {
    let coercion = NumericLiteralCoercion::new(types, parameters);
    let needed = coercion.needed(&expression);
    coercion.apply(&mut expression).unwrap();
    (needed, expression)
}

fn right_operand(expression: &Expr) -> &Expr {
    match &expression.kind {
        ExprKind::Arithmetic { right, .. } | ExprKind::Compare { right, .. } => right,
        kind => panic!("not a binary expression: {kind:?}"),
    }
}

#[test]
fn decimal_literals_beside_double_become_the_nearest_double() {
    for (expression, expected) in [
        (add(column("balance"), decimal("12.5")), 12.5),
        (add(column("balance"), decimal("0.1")), 0.1),
        (add(column("balance"), decimal("1e3")), 1000.0),
        (
            add(
                column("balance"),
                Expr::unspanned(ExprKind::Negate(Box::new(decimal("12.5")))),
            ),
            -12.5,
        ),
        (
            // Literal arithmetic folds exactly, then converts once.
            add(column("balance"), add(decimal("12.5"), decimal("1.0"))),
            13.5,
        ),
        (
            add(
                column("balance"),
                Expr::value(SqlValue::Expression(Box::new(add(
                    literal(Value::Int(5)),
                    decimal("0.25"),
                )))),
            ),
            5.25,
        ),
        (
            add(
                Expr::unspanned(ExprKind::Cast {
                    expression: Box::new(literal(Value::Int(1))),
                    target: NumericType::DoublePrecision,
                }),
                decimal("12.5"),
            ),
            12.5,
        ),
        (
            // The decimal is converted exactly, not through an f64 sum.
            add(
                column("balance"),
                decimal("0.30000000000000001665334536937734810635"),
            ),
            "0.30000000000000001665334536937734810635"
                .parse::<f64>()
                .unwrap(),
        ),
    ] {
        let (needed, coerced) = coerce(expression, &[]);
        assert!(needed);
        assert_eq!(right_operand(&coerced), &literal(Value::Float(expected)));
    }

    // The literal may be on the left, beside a DOUBLE parameter, in a
    // comparison, or a COALESCE/IN member.
    let (_, coerced) = coerce(add(decimal("12.5"), column("balance")), &[]);
    assert_eq!(coerced, add(literal(Value::Float(12.5)), column("balance")));
    let parameter = Expr::value(SqlValue::Parameter(1));
    let (needed, coerced) = coerce(
        add(parameter.clone(), decimal("12.5")),
        &[Value::Float(1.0)],
    );
    assert!(needed);
    assert_eq!(coerced, add(parameter.clone(), literal(Value::Float(12.5))));
    let compare = |left, right| {
        Expr::unspanned(ExprKind::Compare {
            left: Box::new(left),
            op: SqlComparisonOp::Gt,
            right: Box::new(right),
        })
    };
    let (_, coerced) = coerce(
        compare(add(column("balance"), decimal("0.5")), decimal("13.5")),
        &[],
    );
    assert_eq!(
        coerced,
        compare(
            add(column("balance"), literal(Value::Float(0.5))),
            literal(Value::Float(13.5))
        )
    );
    let coalesce = |arguments: Vec<Expr>| {
        Expr::unspanned(ExprKind::Function {
            name: "coalesce".into(),
            arguments: arguments
                .into_iter()
                .map(SqlFunctionArgument::Expression)
                .collect(),
            distinct: false,
            filter: None,
        })
    };
    let (_, coerced) = coerce(coalesce(vec![column("balance"), decimal("12.5")]), &[]);
    assert_eq!(
        coerced,
        coalesce(vec![column("balance"), literal(Value::Float(12.5))])
    );
    let in_list = |values| {
        Expr::unspanned(ExprKind::InList {
            left: Box::new(column("balance")),
            values,
            negated: false,
        })
    };
    let (_, coerced) = coerce(in_list(vec![decimal("12.5"), literal(Value::Int(3))]), &[]);
    assert_eq!(
        coerced,
        in_list(vec![literal(Value::Float(12.5)), literal(Value::Int(3))])
    );
}

#[test]
fn typed_decimal_operands_and_non_double_contexts_stay_unchanged() {
    let cast_decimal = Expr::unspanned(ExprKind::Cast {
        expression: Box::new(decimal("12.5")),
        target: NumericType::Decimal(DecimalType::DEFAULT),
    });
    for (expression, parameters) in [
        // #1429: DECIMAL columns, parameters and casts still need a CAST.
        (add(column("balance"), column("amount")), Vec::new()),
        (add(column("balance"), cast_decimal), Vec::new()),
        (
            add(column("balance"), Expr::value(SqlValue::Parameter(1))),
            vec![Value::Decimal("12.5".parse().unwrap())],
        ),
        // A DECIMAL parameter is not a DOUBLE operand.
        (
            add(Expr::value(SqlValue::Parameter(1)), decimal("12.5")),
            vec![Value::Decimal("1".parse().unwrap())],
        ),
        // Integer literals already combine with DOUBLE PRECISION.
        (add(column("balance"), literal(Value::Int(5))), Vec::new()),
        // No DOUBLE operand: the literal keeps exact DECIMAL semantics.
        (add(column("amount"), decimal("12.5")), Vec::new()),
        (add(column("n"), decimal("12.5")), Vec::new()),
        (add(decimal("12.5"), decimal("1.0")), Vec::new()),
        (add(column("unknown"), decimal("12.5")), Vec::new()),
    ] {
        let (needed, coerced) = coerce(expression.clone(), &parameters);
        assert!(!needed, "{expression:?}");
        assert_eq!(coerced, expression);
    }
}

#[test]
fn literal_evaluation_errors_surface() {
    let divide = Expr::unspanned(ExprKind::Arithmetic {
        left: Box::new(decimal("1.0")),
        op: NumericBinaryOp::Divide,
        right: Box::new(literal(Value::Int(0))),
    });
    let mut expression = add(column("balance"), divide);
    let error = NumericLiteralCoercion::new(types, &[])
        .apply(&mut expression)
        .unwrap_err();
    assert!(error.to_string().contains("division by zero"), "{error}");
}
