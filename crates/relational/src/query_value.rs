//! Query parameter binding and scalar conversion without row or storage access.

use skein_core::{Result, SkeinError, Value};
use skein_sql::{SqlBound, SqlValue};
use skein_storage::{RelationalScalarType, RelationalValue, RelationalValueRef};

pub fn relational_ref_to_value(value: RelationalValueRef<'_>) -> Result<Value> {
    match value {
        RelationalValueRef::Null => Ok(Value::Null),
        RelationalValueRef::Boolean(value) => Ok(Value::Bool(value)),
        RelationalValueRef::BigInt(value) => Ok(Value::Int(value)),
        RelationalValueRef::DoublePrecision(value) => Ok(Value::Float(value)),
        RelationalValueRef::Text(value) => Ok(Value::String(value.to_owned())),
        RelationalValueRef::Bytea(value) => Ok(Value::Binary(value.to_vec())),
        RelationalValueRef::Uuid(value) => Ok(Value::Uuid(value)),
        RelationalValueRef::Overflow(_) => Err(SkeinError::Execution(
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
                Some(_) => Err(SkeinError::Semantic(format!(
                    "PostgreSQL {name} parameter ${position} must be a non-negative integer"
                ))),
                None => Err(SkeinError::Semantic(format!(
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
                SkeinError::Semantic(format!("missing PostgreSQL parameter ${position}"))
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
        Value::List(_) | Value::Map(_) => Err(SkeinError::Semantic(
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
        RelationalValue::Overflow(_) => Err(SkeinError::Execution(
            "overflow value reached projection without hydration".to_string(),
        )),
    }
}
