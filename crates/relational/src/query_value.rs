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
use hawdb_storage::{RelationalScalarType, RelationalValue, RelationalValueRef};

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
        RelationalValueRef::DoublePrecision(value) => Ok(Value::Float(value)),
        RelationalValueRef::Text(value) => Ok(Value::String(value.to_owned())),
        RelationalValueRef::Bytea(value) => Ok(Value::Binary(value.to_vec())),
        RelationalValueRef::Uuid(value) => Ok(Value::Uuid(value)),
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
    match value {
        SqlValue::Literal(value) => Ok(value.clone()),
        SqlValue::Parameter(position) => parameters
            .get(position.saturating_sub(1))
            .cloned()
            .ok_or_else(|| {
                HawDBError::Semantic(format!("missing PostgreSQL parameter ${position}"))
            }),
    }
}

pub fn value_to_relational(value: Value) -> Result<RelationalValue> {
    match value {
        Value::Null => Ok(RelationalValue::Null),
        Value::Bool(value) => Ok(RelationalValue::Boolean(value)),
        Value::Int(value) => Ok(RelationalValue::BigInt(value)),
        Value::Float(value) => Ok(RelationalValue::DoublePrecision(value)),
        Value::String(value) => Ok(RelationalValue::Text(value)),
        Value::Binary(value) => Ok(RelationalValue::Bytea(value)),
        Value::Uuid(value) => Ok(RelationalValue::Uuid(value)),
        Value::List(_) | Value::Map(_) => Err(HawDBError::Semantic(
            "relational SQL values must be scalar".to_string(),
        )),
    }
}

pub fn value_to_relational_as(
    value: Value,
    scalar_type: RelationalScalarType,
) -> Result<RelationalValue> {
    super::coerce_relational_value(value_to_relational(value)?, scalar_type)
}

pub fn relational_to_value(value: &RelationalValue) -> Result<Value> {
    match value {
        RelationalValue::Null => Ok(Value::Null),
        RelationalValue::Boolean(value) => Ok(Value::Bool(*value)),
        RelationalValue::BigInt(value) => Ok(Value::Int(*value)),
        RelationalValue::DoublePrecision(value) => Ok(Value::Float(*value)),
        RelationalValue::Text(value) => Ok(Value::String(value.clone())),
        RelationalValue::Bytea(value) => Ok(Value::Binary(value.clone())),
        RelationalValue::Uuid(value) => Ok(Value::Uuid(*value)),
        RelationalValue::Overflow(_) => Err(HawDBError::Execution(
            "overflow value reached projection without hydration".to_string(),
        )),
    }
}
