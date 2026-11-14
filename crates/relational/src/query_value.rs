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

//! Query parameter binding and scalar conversion without row or storage access.

use hawdb_core::{HawDBError, Result, Value};
use hawdb_sql::{Expr, ExprKind, SqlBound, SqlExpression, SqlValue};
use hawdb_storage::relational::{RelationalScalarType, RelationalValue, RelationalValueRef};

pub fn expression_name(expression: &SqlExpression) -> String {
    match expression {
        Expr {
            kind: ExprKind::Column(column),
            ..
        } => column.name.clone(),
        Expr {
            kind: ExprKind::Value(_),
            ..
        } => "value".to_string(),
        Expr {
            kind: ExprKind::Function { name, .. },
            ..
        } => name.clone(),
        _ => "expression".to_owned(),
    }
}

pub fn relational_ref_to_value(value: RelationalValueRef<'_>) -> Result<Value> {
    match value {
        RelationalValueRef::Null => Ok(Value::Null),
        RelationalValueRef::Boolean(value) => Ok(Value::Bool(value)),
        RelationalValueRef::BigInt(value) => Ok(Value::Int(value)),
        RelationalValueRef::Decimal(value) => Ok(Value::Decimal(value)),
        RelationalValueRef::DoublePrecision(value) => Ok(Value::Float(value)),
        RelationalValueRef::Text(value) => Ok(Value::String(value.to_owned())),
        RelationalValueRef::Bytea(value) => Ok(Value::Binary(value.to_vec())),
        RelationalValueRef::Uuid(value) => Ok(Value::Uuid(value)),
        RelationalValueRef::Vector(value) => Ok(Value::Vector(value.to_owned())),
        RelationalValueRef::Timestamp(value) => hawdb_core::sql_timestamp::local_value(value),
        RelationalValueRef::TimestampTz(value) => hawdb_core::sql_timestamp::zoned_value(value),
        RelationalValueRef::Overflow(_) => Err(HawDBError::Execution(
            "overflow value reached projection without hydration".to_string(),
        )),
    }
}

pub fn bind_bound(
    bound: Option<SqlBound>,
    parameters: &[Value],
    name: &str,
) -> Result<Option<u64>> {
    bound
        .map(|bound| match bound {
            SqlBound::Literal(value) => Ok(value),
            SqlBound::Parameter(position) => match parameters.get(position.saturating_sub(1)) {
                Some(Value::Int(value)) if *value >= 0 => Ok(*value as u64),
                Some(_) => Err(HawDBError::Semantic(format!(
                    "PostgreSQL {name} parameter ${position} must be a non-negative integer"
                ))),
                None => Err(HawDBError::Semantic(format!(
                    "missing PostgreSQL parameter ${position}"
                ))),
            },
        })
        .transpose()
}

pub fn bind_sql_value(value: &SqlValue, parameters: &[Value]) -> Result<Value> {
    hawdb_expression::sql::bind_sql_value(value, parameters)
}

pub fn value_to_relational(value: Value) -> Result<RelationalValue> {
    match value {
        Value::Null => Ok(RelationalValue::Null),
        Value::Bool(value) => Ok(RelationalValue::Boolean(value)),
        Value::Int(value) => Ok(RelationalValue::BigInt(value)),
        Value::Decimal(value) => Ok(RelationalValue::Decimal(value)),
        Value::Float(value) => Ok(RelationalValue::DoublePrecision(value)),
        Value::String(value) => Ok(RelationalValue::Text(value)),
        Value::Binary(value) => Ok(RelationalValue::Bytea(value)),
        Value::Uuid(value) => Ok(RelationalValue::Uuid(value)),
        Value::Vector(value) => Ok(RelationalValue::Vector(value)),
        Value::LocalDateTime(value) => {
            hawdb_core::sql_timestamp::from_naive(value).map(RelationalValue::Timestamp)
        }
        Value::DateTime(value) => {
            hawdb_core::sql_timestamp::from_datetime(value).map(RelationalValue::TimestampTz)
        }
        Value::Date(_) | Value::LocalTime(_) | Value::Duration(_) => Err(HawDBError::Semantic(
            "relational SQL supports local datetimes (TIMESTAMP) and zoned datetimes (TIMESTAMPTZ); DATE, TIME and DURATION values are not supported".to_string(),
        )),
        Value::List(_) | Value::Map(_) => Err(HawDBError::Semantic(
            "relational SQL values must be scalar".to_string(),
        )),
    }
}

pub fn value_to_relational_as(
    value: Value,
    scalar_type: RelationalScalarType,
) -> Result<RelationalValue> {
    let value = value_to_relational(value)?;
    match (scalar_type, value) {
        (RelationalScalarType::Decimal(_), RelationalValue::Decimal(value)) => {
            Ok(RelationalValue::Decimal(value))
        }
        (RelationalScalarType::Decimal(_), RelationalValue::BigInt(value)) => {
            Ok(RelationalValue::Decimal(value.into()))
        }
        (RelationalScalarType::BigInt, RelationalValue::Decimal(value)) => Ok(value
            .to_i64()
            .map(RelationalValue::BigInt)
            .unwrap_or(RelationalValue::Decimal(value))),
        (RelationalScalarType::Decimal(_), RelationalValue::DoublePrecision(_))
        | (RelationalScalarType::DoublePrecision, RelationalValue::Decimal(_)) => Err(
            HawDBError::Semantic("Decimal and DOUBLE PRECISION comparison requires CAST".into()),
        ),
        // Comparison operands keep their exact instant: text is parsed as the
        // column's type without rounding to its precision, and a TIMESTAMP
        // never silently becomes a TIMESTAMPTZ (or the reverse).
        (RelationalScalarType::Timestamp(_), RelationalValue::Text(text)) => {
            hawdb_core::sql_timestamp::parse_timestamp(&text).map(RelationalValue::Timestamp)
        }
        (RelationalScalarType::TimestampTz(_), RelationalValue::Text(text)) => {
            hawdb_core::sql_timestamp::parse_timestamptz(&text).map(RelationalValue::TimestampTz)
        }
        (RelationalScalarType::Timestamp(_) | RelationalScalarType::TimestampTz(_), value) => {
            Ok(value)
        }
        (scalar_type, value) => super::coerce_relational_value(value, scalar_type),
    }
}

pub fn relational_to_value(value: &RelationalValue) -> Result<Value> {
    match value {
        RelationalValue::Null => Ok(Value::Null),
        RelationalValue::Boolean(value) => Ok(Value::Bool(*value)),
        RelationalValue::BigInt(value) => Ok(Value::Int(*value)),
        RelationalValue::Decimal(value) => Ok(Value::Decimal(*value)),
        RelationalValue::DoublePrecision(value) => Ok(Value::Float(*value)),
        RelationalValue::Text(value) => Ok(Value::String(value.clone())),
        RelationalValue::Bytea(value) => Ok(Value::Binary(value.clone())),
        RelationalValue::Uuid(value) => Ok(Value::Uuid(*value)),
        RelationalValue::Vector(value) => Ok(Value::Vector(value.clone())),
        RelationalValue::Timestamp(value) => hawdb_core::sql_timestamp::local_value(*value),
        RelationalValue::TimestampTz(value) => hawdb_core::sql_timestamp::zoned_value(*value),
        RelationalValue::Overflow(_) => Err(HawDBError::Execution(
            "overflow value reached projection without hydration".to_string(),
        )),
    }
}

/// Exact distance over borrowed operands: candidate windows belong to explicit search plans.
pub(crate) fn evaluate_vector_distance<'a>(
    metric: hawdb_core::VectorDistance,
    arguments: &'a [hawdb_sql::SqlFunctionArgument],
    parameters: &'a [Value],
    resolve: impl Fn(&hawdb_sql::SqlColumnRef) -> Result<&'a RelationalValue>,
) -> Result<RelationalValue> {
    fn operand<'a>(
        expression: &'a SqlExpression,
        parameters: &'a [Value],
        resolve: &impl Fn(&hawdb_sql::SqlColumnRef) -> Result<&'a RelationalValue>,
    ) -> Result<hawdb_core::ValueRef<'a>> {
        match &expression.kind {
            ExprKind::Column(column) => match resolve(column)? {
                RelationalValue::Null => Ok(hawdb_core::ValueRef::Null),
                RelationalValue::Vector(vector) => {
                    Ok(hawdb_core::ValueRef::Vector(vector.as_ref()))
                }
                _ => Err(HawDBError::Semantic(
                    "distance arguments must be vectors".into(),
                )),
            },
            ExprKind::Value(SqlValue::Literal(value)) => Ok(value.as_ref()),
            ExprKind::Value(SqlValue::Parameter(position)) => parameters
                .get(position.saturating_sub(1))
                .map(Value::as_ref)
                .ok_or_else(|| {
                    HawDBError::Semantic(format!("missing PostgreSQL parameter ${position}"))
                }),
            _ => Err(HawDBError::Semantic(
                "distance arguments must be vector columns or parameters".into(),
            )),
        }
    }
    let [hawdb_sql::SqlFunctionArgument::Expression(left), hawdb_sql::SqlFunctionArgument::Expression(right)] =
        arguments
    else {
        return Err(HawDBError::Semantic(
            "distance requires two vector arguments".into(),
        ));
    };
    Ok(metric
        .evaluate(
            operand(left, parameters, &resolve)?,
            operand(right, parameters, &resolve)?,
        )?
        .map_or(RelationalValue::Null, RelationalValue::DoublePrecision))
}

pub(crate) fn core_value_ref(value: RelationalValueRef<'_>) -> Result<hawdb_core::ValueRef<'_>> {
    use hawdb_core::ValueRef;
    Ok(match value {
        RelationalValueRef::Null => ValueRef::Null,
        RelationalValueRef::Boolean(value) => ValueRef::Bool(value),
        RelationalValueRef::BigInt(value) => ValueRef::Int(value),
        RelationalValueRef::DoublePrecision(value) => ValueRef::Float(value),
        RelationalValueRef::Decimal(value) => ValueRef::Decimal(value),
        RelationalValueRef::Text(value) => ValueRef::String(value),
        RelationalValueRef::Bytea(value) => ValueRef::Binary(value),
        RelationalValueRef::Uuid(value) => ValueRef::Uuid(value),
        RelationalValueRef::Vector(value) => ValueRef::Vector(value),
        RelationalValueRef::Timestamp(value) => {
            ValueRef::LocalDateTime(hawdb_core::sql_timestamp::to_naive(value)?)
        }
        RelationalValueRef::TimestampTz(value) => {
            ValueRef::DateTime(hawdb_core::sql_timestamp::to_datetime(value)?)
        }
        RelationalValueRef::Overflow(_) => {
            return Err(HawDBError::Execution(
                "numeric expression requires overflow hydration".into(),
            ))
        }
    })
}

/// The result type of an explicit TEXT/TIMESTAMP/TIMESTAMPTZ cast whose
/// operand has `operand` type (`None` for NULL or an untyped constant).
pub(crate) fn timestamp_cast_type(
    operand: Option<RelationalScalarType>,
    target: hawdb_core::sql_timestamp::TimestampCast,
) -> Result<RelationalScalarType> {
    use hawdb_core::sql_timestamp::TimestampCast;
    if !matches!(
        operand,
        None | Some(
            RelationalScalarType::Text
                | RelationalScalarType::Timestamp(_)
                | RelationalScalarType::TimestampTz(_)
        )
    ) {
        return Err(HawDBError::Semantic(format!(
            "CAST to {target} requires a TEXT, TIMESTAMP or TIMESTAMPTZ operand"
        )));
    }
    Ok(match target {
        TimestampCast::Text => RelationalScalarType::Text,
        TimestampCast::Timestamp(kind) if kind.zoned => {
            RelationalScalarType::TimestampTz(kind.precision)
        }
        TimestampCast::Timestamp(kind) => RelationalScalarType::Timestamp(kind.precision),
    })
}

/// Evaluates an explicit TEXT/TIMESTAMP/TIMESTAMPTZ cast of one value.
pub(crate) fn evaluate_timestamp_cast(
    value: &RelationalValue,
    target: hawdb_core::sql_timestamp::TimestampCast,
) -> Result<RelationalValue> {
    value_to_relational(target.apply(&relational_to_value(value)?)?)
}

pub(crate) fn evaluate_numeric_with<'a>(
    expression: &Expr,
    evaluate: &impl Fn(&Expr) -> Result<std::borrow::Cow<'a, RelationalValue>>,
) -> Result<RelationalValue> {
    use hawdb_core::numeric::{cast_numeric, evaluate_numeric, negate_numeric};
    let value = match &expression.kind {
        ExprKind::Arithmetic { left, op, right } => {
            let left = evaluate(left)?;
            let right = evaluate(right)?;
            evaluate_numeric(
                core_value_ref(left.as_ref().as_ref())?,
                *op,
                core_value_ref(right.as_ref().as_ref())?,
            )?
        }
        ExprKind::Negate(expression) => {
            negate_numeric(core_value_ref(evaluate(expression)?.as_ref().as_ref())?)?
        }
        ExprKind::Cast { expression, target } => cast_numeric(
            core_value_ref(evaluate(expression)?.as_ref().as_ref())?,
            *target,
        )?,
        _ => return Err(HawDBError::Semantic("expected a numeric expression".into())),
    };
    value_to_relational(value)
}

pub(crate) fn infer_numeric_type(
    expression: &Expr,
    infer: &impl Fn(&Expr) -> Result<Option<RelationalScalarType>>,
) -> Result<Option<RelationalScalarType>> {
    use hawdb_core::{numeric::NumericType, LogicalType};
    let logical = match &expression.kind {
        ExprKind::Arithmetic { left, right, .. } => hawdb_core::numeric::common_numeric_type(
            infer(left)?.map(RelationalScalarType::logical_type),
            infer(right)?.map(RelationalScalarType::logical_type),
        )?,
        ExprKind::Negate(expression) => hawdb_core::numeric::common_numeric_type(
            infer(expression)?.map(RelationalScalarType::logical_type),
            None,
        )?,
        ExprKind::Cast { expression, target } => {
            if infer(expression)?.is_some_and(|kind| {
                !kind.logical_type().is_numeric() && kind != RelationalScalarType::Text
            }) {
                return Err(HawDBError::Semantic(
                    "numeric CAST requires numeric or TEXT input".into(),
                ));
            }
            return Ok(Some(match target {
                NumericType::BigInt => RelationalScalarType::BigInt,
                NumericType::DoublePrecision => RelationalScalarType::DoublePrecision,
                NumericType::Decimal(kind) => RelationalScalarType::Decimal(*kind),
            }));
        }
        _ => return Err(HawDBError::Semantic("expected a numeric expression".into())),
    };
    Ok(logical.map(|kind| match kind {
        LogicalType::Int64 => RelationalScalarType::BigInt,
        LogicalType::Float64 => RelationalScalarType::DoublePrecision,
        LogicalType::Decimal => RelationalScalarType::Decimal(hawdb_core::DecimalType::DEFAULT),
        _ => unreachable!("numeric type inference validated its operands"),
    }))
}
