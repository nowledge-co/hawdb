// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn decimal(text: &str) -> Value {
    Value::Decimal(text.parse().unwrap())
}

#[test]
fn numeric_comparison_keeps_exact_integer_and_decimal_boundaries() {
    let integer = ValueRef::Int(9_007_199_254_740_993);
    assert_eq!(
        compare_numeric(integer, ValueRef::Float(9_007_199_254_740_992.0)).unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        compare_numeric(integer, decimal("9007199254740993.01").as_ref()).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare_numeric(
            ValueRef::Int(i64::MAX),
            ValueRef::Float(9_223_372_036_854_775_808.0)
        )
        .unwrap(),
        Ordering::Less
    );
    assert_eq!(
        compare_numeric(
            ValueRef::Int(i64::MIN),
            ValueRef::Float(-9_223_372_036_854_775_808.0)
        )
        .unwrap(),
        Ordering::Equal
    );
    assert_eq!(
        compare_numeric(decimal("1.0").as_ref(), decimal("1.0000").as_ref()).unwrap(),
        Ordering::Equal
    );
    assert!(compare_numeric(decimal("1.0").as_ref(), ValueRef::Float(1.0)).is_err());
}

#[test]
fn numeric_conversion_and_arithmetic_preserve_explicit_loss_boundaries() {
    assert_eq!(
        evaluate_numeric(
            decimal("0.1").as_ref(),
            NumericBinaryOp::Add,
            decimal("0.2").as_ref()
        )
        .unwrap(),
        decimal("0.3")
    );
    assert_eq!(
        evaluate_numeric(
            decimal("0.1").as_ref(),
            NumericBinaryOp::Multiply,
            ValueRef::Int(10)
        )
        .unwrap(),
        decimal("1")
    );
    assert!(evaluate_numeric(
        decimal("0.1").as_ref(),
        NumericBinaryOp::Add,
        ValueRef::Float(0.2)
    )
    .is_err());
    assert!(evaluate_numeric(
        ValueRef::Int(i64::MIN),
        NumericBinaryOp::Divide,
        ValueRef::Int(-1)
    )
    .is_err());
    assert!(evaluate_numeric(ValueRef::Int(1), NumericBinaryOp::Divide, ValueRef::Int(0)).is_err());
    let target = NumericType::Decimal(DecimalType::new(4, 2).unwrap());
    assert_eq!(
        cast_numeric(ValueRef::String("1.235"), target).unwrap(),
        decimal("1.24")
    );
    assert_eq!(
        cast_numeric(ValueRef::Float(-1.235), target).unwrap(),
        decimal("-1.24")
    );
    assert!(cast_numeric(ValueRef::String("99.995"), target).is_err());
    assert!(cast_numeric(decimal("1.1").as_ref(), NumericType::BigInt).is_err());
    assert_eq!(
        cast_numeric(decimal("1.00").as_ref(), NumericType::BigInt).unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        evaluate_numeric(
            ValueRef::Null,
            NumericBinaryOp::Multiply,
            decimal("0.1").as_ref()
        )
        .unwrap(),
        Value::Null
    );
    assert!(evaluate_numeric(
        ValueRef::Null,
        NumericBinaryOp::Multiply,
        ValueRef::String("1")
    )
    .is_err());
}

#[test]
fn numeric_values_remain_inline_without_widening_native_value_slots() {
    assert_eq!(std::mem::size_of::<Decimal>(), 17);
    assert!(std::mem::size_of::<Value>() <= 32);
    assert!(std::mem::size_of::<ValueRef<'_>>() <= 32);
}
