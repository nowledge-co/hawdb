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
use hawdb_storage::{RelationalScalarType, RelationalValue, RelationalValueRef};

use crate::query_value::{bind_sql_value, value_to_relational_as};

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
    if left.scalar_type() != right.scalar_type() {
        return Err(HawDBError::Semantic(
            "relational comparison has incompatible scalar types".to_string(),
        ));
    }
    Ok(Some(match op {
        SqlComparisonOp::Eq => left == right,
        SqlComparisonOp::NotEq => left != right,
        SqlComparisonOp::Lt => left < right,
        SqlComparisonOp::Lte => left <= right,
        SqlComparisonOp::Gt => left > right,
        SqlComparisonOp::Gte => left >= right,
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
            let left = left.require_column()?;
            match &right.kind {
                ExprKind::Value(right) => {
                    let (left_value, scalar_type) = resolve(left)?;
                    compare_values(
                        left_value,
                        &value_to_relational_as(bind_sql_value(right, parameters)?, scalar_type)?,
                        *op,
                    )
                }
                ExprKind::Column(right) => compare_values(resolve(left)?.0, resolve(right)?.0, *op),
                _ => Err(HawDBError::Semantic(
                    "unsupported comparison operand".to_owned(),
                )),
            }
        }
        ExprKind::InList {
            left,
            values,
            negated,
        } => {
            let (left, scalar_type) = resolve(left.require_column()?)?;
            let mut has_unknown = false;
            let mut matched = false;
            for value in values {
                match compare_values(
                    left,
                    predicate_operand(value, parameters, scalar_type, resolve)?.as_ref(),
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
            matches!(resolve(column.require_column()?)?.0, RelationalValue::Null) != *negated,
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
        _ => Err(HawDBError::Semantic("unsupported predicate operand".into())),
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
