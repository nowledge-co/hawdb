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

//! SQL qualification over caller-owned row values.

use hawdb_core::{HawDBError, Result, Value};
use hawdb_sql::{Expr, ExprKind, SqlColumnRef, SqlComparisonOp, SqlPredicate, SqlValue};
use hawdb_storage::relational::{RelationalScalarType, RelationalValue, RelationalValueRef};

use crate::query_value::{bind_sql_value, value_to_relational_as};

mod scalar;
mod streaming;
pub use streaming::{bind_streaming_column, BoundStreamingPredicate};

fn compare_value_refs(
    left: RelationalValueRef<'_>,
    right: RelationalValueRef<'_>,
    op: SqlComparisonOp,
) -> Result<Option<bool>> {
    if matches!(left, RelationalValueRef::Overflow(_))
        || matches!(right, RelationalValueRef::Overflow(_))
    {
        return Err(HawDBError::Execution(
            "relational filter or join requires overflow hydration before qualification"
                .to_string(),
        ));
    }
    if matches!(left, RelationalValueRef::Null) || matches!(right, RelationalValueRef::Null) {
        return Ok(None);
    }
    let numeric = left.logical_type().is_some_and(|kind| kind.is_numeric())
        && right.logical_type().is_some_and(|kind| kind.is_numeric());
    let ordering = if numeric {
        hawdb_core::numeric::compare_numeric(
            crate::query_value::core_value_ref(left)?,
            crate::query_value::core_value_ref(right)?,
        )?
    } else {
        if left.scalar_type() != right.scalar_type() {
            return Err(HawDBError::Semantic(
                "relational comparison has incompatible scalar types".into(),
            ));
        }
        left.cmp(&right)
    };
    Ok(Some(match op {
        SqlComparisonOp::Eq => ordering.is_eq(),
        SqlComparisonOp::NotEq => !ordering.is_eq(),
        SqlComparisonOp::Lt => ordering.is_lt(),
        SqlComparisonOp::Lte => !ordering.is_gt(),
        SqlComparisonOp::Gt => ordering.is_gt(),
        SqlComparisonOp::Gte => !ordering.is_lt(),
    }))
}

pub fn predicate_truth_with<'a>(
    predicate: &SqlPredicate,
    parameters: &[Value],
    resolve: &impl Fn(&SqlColumnRef) -> Result<(&'a RelationalValue, RelationalScalarType)>,
) -> Result<Option<bool>> {
    match &predicate.kind {
        ExprKind::Value(SqlValue::Literal(Value::Bool(value))) => Ok(Some(*value)),
        ExprKind::And(left, right) => match predicate_truth_with(left, parameters, resolve)? {
            Some(false) => Ok(Some(false)),
            Some(true) => predicate_truth_with(right, parameters, resolve),
            None => match predicate_truth_with(right, parameters, resolve)? {
                Some(false) => Ok(Some(false)),
                Some(true) | None => Ok(None),
            },
        },
        ExprKind::Or(left, right) => match predicate_truth_with(left, parameters, resolve)? {
            Some(true) => Ok(Some(true)),
            Some(false) => predicate_truth_with(right, parameters, resolve),
            None => match predicate_truth_with(right, parameters, resolve)? {
                Some(true) => Ok(Some(true)),
                Some(false) | None => Ok(None),
            },
        },
        ExprKind::Not(predicate) => {
            Ok(predicate_truth_with(predicate, parameters, resolve)?.map(|value| !value))
        }
        ExprKind::Compare { left, op, right } => {
            let (left_value, scalar_type) = if let Some(column) = left.as_column() {
                let (value, kind) = resolve(column)?;
                (std::borrow::Cow::Borrowed(value), Some(kind))
            } else {
                (predicate_expression(left, parameters, resolve)?, None)
            };
            let right_value = if let Some(kind) = scalar_type {
                predicate_operand(right, parameters, kind, resolve)?
            } else {
                predicate_expression(right, parameters, resolve)?
            };
            compare_values(left_value.as_ref(), right_value.as_ref(), *op)
        }
        ExprKind::InList {
            left,
            values,
            negated,
        } => {
            let (left, scalar_type) = if let Some(column) = left.as_column() {
                let (value, kind) = resolve(column)?;
                (std::borrow::Cow::Borrowed(value), Some(kind))
            } else {
                (predicate_expression(left, parameters, resolve)?, None)
            };
            let mut has_unknown = false;
            let mut matched = false;
            for value in values {
                match compare_values(
                    left.as_ref(),
                    if let Some(kind) = scalar_type {
                        predicate_operand(value, parameters, kind, resolve)?
                    } else {
                        predicate_expression(value, parameters, resolve)?
                    }
                    .as_ref(),
                    SqlComparisonOp::Eq,
                )? {
                    Some(true) => matched = true,
                    None => has_unknown = true,
                    Some(false) => {}
                }
            }
            let result = if matched {
                Some(true)
            } else if has_unknown {
                None
            } else {
                Some(false)
            };
            Ok(result.map(|value| value != *negated))
        }
        ExprKind::Like {
            left,
            pattern,
            case_insensitive,
            negated,
            escape,
        } => {
            let (left, scalar_type) = resolve(left.require_column()?)?;
            if scalar_type != RelationalScalarType::Text {
                return Err(HawDBError::Semantic(
                    "LIKE and ILIKE require a TEXT column".to_string(),
                ));
            }
            let pattern =
                predicate_operand(pattern, parameters, RelationalScalarType::Text, resolve)?;
            match (left, pattern.as_ref()) {
                (RelationalValue::Null, _) | (_, RelationalValue::Null) => Ok(None),
                (RelationalValue::Text(value), RelationalValue::Text(pattern)) => {
                    let matched =
                        hawdb_sql::sql_like_matches(value, pattern, *escape, *case_insensitive)?;
                    Ok(Some(matched != *negated))
                }
                (RelationalValue::Overflow(_), _) => Err(HawDBError::Execution(
                    "LIKE reached an overflow value without hydration".to_string(),
                )),
                _ => Err(HawDBError::Semantic(
                    "LIKE and ILIKE require TEXT values".to_string(),
                )),
            }
        }
        ExprKind::IsNull {
            expression: column,
            negated,
        } => Ok(Some(
            matches!(
                predicate_expression(column, parameters, resolve)?.as_ref(),
                RelationalValue::Null
            ) != *negated,
        )),
        _ => Err(HawDBError::Semantic(
            "unsupported relational predicate expression".to_owned(),
        )),
    }
}

fn predicate_operand<'a>(
    expression: &Expr,
    parameters: &[Value],
    scalar_type: RelationalScalarType,
    resolve: &impl Fn(&SqlColumnRef) -> Result<(&'a RelationalValue, RelationalScalarType)>,
) -> Result<std::borrow::Cow<'a, RelationalValue>> {
    match &expression.kind {
        ExprKind::Column(column) => Ok(std::borrow::Cow::Borrowed(resolve(column)?.0)),
        ExprKind::Value(value) => Ok(std::borrow::Cow::Owned(value_to_relational_as(
            bind_sql_value(value, parameters)?,
            scalar_type,
        )?)),
        _ => predicate_expression(expression, parameters, resolve),
    }
}

fn compare_values(
    left: &RelationalValue,
    right: &RelationalValue,
    op: SqlComparisonOp,
) -> Result<Option<bool>> {
    compare_value_refs(left.as_ref(), right.as_ref(), op)
}

#[cfg(test)]
mod tests;

fn predicate_expression<'a>(
    expression: &Expr,
    parameters: &[Value],
    resolve: &impl Fn(&SqlColumnRef) -> Result<(&'a RelationalValue, RelationalScalarType)>,
) -> Result<std::borrow::Cow<'a, RelationalValue>> {
    use std::borrow::Cow;
    match &expression.kind {
        ExprKind::Column(column) => Ok(Cow::Borrowed(resolve(column)?.0)),
        ExprKind::Value(value) => Ok(Cow::Owned(crate::query_value::value_to_relational(
            bind_sql_value(value, parameters)?,
        )?)),
        ExprKind::Arithmetic { .. } | ExprKind::Negate(_) | ExprKind::Cast { .. } => {
            crate::query_value::evaluate_numeric_with(expression, &|child| {
                predicate_expression(child, parameters, resolve)
            })
            .map(Cow::Owned)
        }
        _ => Err(HawDBError::Semantic("unsupported predicate operand".into())),
    }
}
