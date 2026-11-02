// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Numeric semantics shared by SQL binding, row execution and mutations.

use crate::{Decimal, DecimalType, HawDBError, LogicalType, Result, Value, ValueRef};
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericBinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericType {
    BigInt,
    DoublePrecision,
    Decimal(DecimalType),
}

impl NumericType {
    pub const fn logical_type(self) -> LogicalType {
        match self {
            Self::BigInt => LogicalType::Int64,
            Self::DoublePrecision => LogicalType::Float64,
            Self::Decimal(_) => LogicalType::Decimal,
        }
    }
}

fn error(message: impl ToString) -> HawDBError {
    HawDBError::Execution(message.to_string())
}

pub fn evaluate_numeric(
    left: ValueRef<'_>,
    op: NumericBinaryOp,
    right: ValueRef<'_>,
) -> Result<Value> {
    if [left, right]
        .into_iter()
        .any(|value| value.logical_type().is_some_and(|kind| !kind.is_numeric()))
    {
        return Err(error("arithmetic requires numeric operands"));
    }
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    if matches!(left, ValueRef::Decimal(_)) || matches!(right, ValueRef::Decimal(_)) {
        let left = exact_decimal(left)?;
        let right = exact_decimal(right)?;
        return match op {
            NumericBinaryOp::Add => left.checked_add(right),
            NumericBinaryOp::Subtract => left.checked_sub(right),
            NumericBinaryOp::Multiply => left.checked_mul(right),
            NumericBinaryOp::Divide => left.checked_div(right),
            NumericBinaryOp::Remainder => left.checked_rem(right),
        }
        .map(Value::Decimal)
        .map_err(error);
    }
    if let (ValueRef::Int(left), ValueRef::Int(right)) = (left, right) {
        if matches!(op, NumericBinaryOp::Divide | NumericBinaryOp::Remainder) && right == 0 {
            return Err(error("division by zero"));
        }
        return match op {
            NumericBinaryOp::Add => left.checked_add(right),
            NumericBinaryOp::Subtract => left.checked_sub(right),
            NumericBinaryOp::Multiply => left.checked_mul(right),
            NumericBinaryOp::Divide => left.checked_div(right),
            NumericBinaryOp::Remainder => left.checked_rem(right),
        }
        .map(Value::Int)
        .ok_or_else(|| error("integer arithmetic overflow"));
    }
    let left = approximate(left)?;
    let right = approximate(right)?;
    if matches!(op, NumericBinaryOp::Divide | NumericBinaryOp::Remainder) && right == 0.0 {
        return Err(error("division by zero"));
    }
    let value = match op {
        NumericBinaryOp::Add => left + right,
        NumericBinaryOp::Subtract => left - right,
        NumericBinaryOp::Multiply => left * right,
        NumericBinaryOp::Divide => left / right,
        NumericBinaryOp::Remainder => left % right,
    };
    if !value.is_finite() {
        return Err(error("non-finite arithmetic result"));
    }
    Ok(Value::Float(value))
}

pub fn negate_numeric(value: ValueRef<'_>) -> Result<Value> {
    match value {
        ValueRef::Null => Ok(Value::Null),
        ValueRef::Decimal(value) => Ok(Value::Decimal(value.checked_neg())),
        ValueRef::Int(value) => value
            .checked_neg()
            .map(Value::Int)
            .ok_or_else(|| error("integer arithmetic overflow")),
        ValueRef::Float(value) if value.is_finite() => Ok(Value::Float(-value)),
        _ => Err(error("negation requires a finite numeric operand")),
    }
}

fn exact_decimal(value: ValueRef<'_>) -> Result<Decimal> {
    match value {
        ValueRef::Int(value) => Ok(value.into()),
        ValueRef::Decimal(value) => Ok(value),
        ValueRef::Float(_) => Err(error(
            "Decimal and DOUBLE PRECISION require an explicit CAST",
        )),
        _ => Err(error("arithmetic requires numeric operands")),
    }
}

fn approximate(value: ValueRef<'_>) -> Result<f64> {
    match value {
        ValueRef::Int(value) => Ok(value as f64),
        ValueRef::Float(value) if value.is_finite() => Ok(value),
        _ => Err(error("arithmetic requires finite numeric operands")),
    }
}

/// Numeric comparison without converting exact values through f64. NULL truth
/// semantics belong to the predicate caller; NULL is not a numeric operand.
pub fn compare_numeric(left: ValueRef<'_>, right: ValueRef<'_>) -> Result<Ordering> {
    if matches!(left, ValueRef::Decimal(_)) || matches!(right, ValueRef::Decimal(_)) {
        return Ok(exact_decimal(left)?.cmp(&exact_decimal(right)?));
    }
    match (left, right) {
        (ValueRef::Int(left), ValueRef::Int(right)) => Ok(left.cmp(&right)),
        (ValueRef::Float(left), ValueRef::Float(right)) => Ok(left.total_cmp(&right)),
        (ValueRef::Int(left), ValueRef::Float(right)) => compare_int_float(left, right),
        (ValueRef::Float(left), ValueRef::Int(right)) => {
            compare_int_float(right, left).map(Ordering::reverse)
        }
        _ => Err(error("numeric comparison requires numeric operands")),
    }
}

fn compare_int_float(integer: i64, float: f64) -> Result<Ordering> {
    if float.is_nan() {
        return Ok((integer as f64).total_cmp(&float));
    }
    if float >= 9_223_372_036_854_775_808.0 {
        return Ok(Ordering::Less);
    }
    if float < -9_223_372_036_854_775_808.0 {
        return Ok(Ordering::Greater);
    }
    let integral = float as i64;
    Ok(integer
        .cmp(&integral)
        .then_with(|| (integral as f64).partial_cmp(&float).expect("finite float")))
}

/// Explicit numeric conversion. Decimal-to-BIGINT is exact and rejects a
/// fraction; float-to-Decimal uses the float's shortest round-trip decimal
/// spelling before the declared scale's midpoint-away rounding.
pub fn cast_numeric(value: ValueRef<'_>, target: NumericType) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match target {
        NumericType::Decimal(kind) => {
            let decimal = match value {
                ValueRef::Int(value) => Decimal::from(value),
                ValueRef::Decimal(value) => value,
                ValueRef::String(value) => value.parse().map_err(error)?,
                ValueRef::Float(value) if value.is_finite() => {
                    value.to_string().parse().map_err(error)?
                }
                _ => {
                    return Err(error(
                        "CAST to DECIMAL requires a finite numeric value or decimal text",
                    ))
                }
            };
            kind.coerce(decimal).map(Value::Decimal).map_err(error)
        }
        NumericType::BigInt => match value {
            ValueRef::Int(value) => Ok(Value::Int(value)),
            ValueRef::Decimal(value) => value.to_i64().map(Value::Int).map_err(error),
            ValueRef::String(value) => value.parse::<i64>().map(Value::Int).map_err(error),
            ValueRef::Float(value)
                if value.is_finite()
                    && value.fract() == 0.0
                    && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0)
                        .contains(&value) =>
            {
                Ok(Value::Int(value as i64))
            }
            _ => Err(error("CAST to BIGINT requires an exact in-range integer")),
        },
        NumericType::DoublePrecision => {
            let value = match value {
                ValueRef::Decimal(value) => value.to_string().parse::<f64>().map_err(error)?,
                ValueRef::String(value) => value.parse::<f64>().map_err(error)?,
                value => approximate(value)?,
            };
            if !value.is_finite() {
                return Err(error("CAST produced a non-finite DOUBLE PRECISION value"));
            }
            Ok(Value::Float(value))
        }
    }
}

crate::impl_retained_heap_for_copy!(NumericBinaryOp, NumericType);

/// Infer exact arithmetic promotion before row execution, including NULL inputs.
pub fn common_numeric_type(
    left: Option<LogicalType>,
    right: Option<LogicalType>,
) -> Result<Option<LogicalType>> {
    if [left, right]
        .into_iter()
        .flatten()
        .any(|kind| !kind.is_numeric())
    {
        return Err(error("arithmetic requires numeric operands"));
    }
    if matches!(
        (left, right),
        (Some(LogicalType::Decimal), Some(LogicalType::Float64))
            | (Some(LogicalType::Float64), Some(LogicalType::Decimal))
    ) {
        return Err(error(
            "Decimal and DOUBLE PRECISION require an explicit CAST",
        ));
    }
    Ok(
        if left == Some(LogicalType::Decimal) || right == Some(LogicalType::Decimal) {
            Some(LogicalType::Decimal)
        } else if left == Some(LogicalType::Float64) || right == Some(LogicalType::Float64) {
            Some(LogicalType::Float64)
        } else {
            left.or(right)
        },
    )
}

#[cfg(test)]
mod tests;
