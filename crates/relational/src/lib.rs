//! Storage-neutral relational statement compilation and execution contracts.

mod append;
#[doc(hidden)]
pub mod field_plan;
mod statement;

#[doc(hidden)]
pub mod predicate;
#[doc(hidden)]
pub mod query_value;

pub use append::{
    compile_append_explain_sql, compile_append_select_sql, compile_append_statement_sql,
    format_append_explain, project_append_rows, AppendExplainPlan, AppendSelectPlan,
};

// These are internal ownership seams. Hosts continue to use the embedded facade.
#[doc(hidden)]
pub use statement::{
    compile_relational_statement_sql, compile_relational_statement_sql_with_result,
    CompiledRelationalStatement, RelationalReturningProjection,
};

use skein_core::{Result, SkeinError, Value};
use skein_sql::{SqlColumnDefault, SqlColumnDefinition, SqlDataType, SqlValue};
use skein_storage::{
    RelationalColumnDefault, RelationalColumnSchema, RelationalScalarType, RelationalValue,
};

#[doc(hidden)]
pub fn bind_relational_value(value: SqlValue, parameters: &[Value]) -> Result<RelationalValue> {
    let value = match value {
        SqlValue::Literal(value) => value,
        SqlValue::Parameter(position) => parameters
            .get(position.saturating_sub(1))
            .cloned()
            .ok_or_else(|| {
                SkeinError::Semantic(format!("missing PostgreSQL parameter ${position}"))
            })?,
    };
    match value {
        Value::Null => Ok(RelationalValue::Null),
        Value::Bool(value) => Ok(RelationalValue::Boolean(value)),
        Value::Int(value) => Ok(RelationalValue::BigInt(value)),
        Value::Float(value) => Ok(RelationalValue::DoublePrecision(value)),
        Value::String(value) => Ok(RelationalValue::Text(value)),
        Value::Binary(value) => Ok(RelationalValue::Bytea(value)),
        Value::Uuid(value) => Ok(RelationalValue::Uuid(value)),
        Value::List(_) | Value::Map(_) => Err(SkeinError::Semantic(
            "relational SQL parameters must be scalar".to_string(),
        )),
    }
}

#[doc(hidden)]
pub fn compile_column(column: SqlColumnDefinition) -> Result<RelationalColumnSchema> {
    let scalar_type = compile_data_type(column.data_type);
    Ok(RelationalColumnSchema {
        name: column.name,
        scalar_type,
        nullable: column.nullable,
        default: column
            .default
            .map(|default| match default {
                SqlColumnDefault::Literal(value) => {
                    compile_schema_value(value, scalar_type).map(RelationalColumnDefault::Literal)
                }
                SqlColumnDefault::UuidV7 => Ok(RelationalColumnDefault::UuidV7),
            })
            .transpose()?,
    })
}

fn compile_data_type(data_type: SqlDataType) -> RelationalScalarType {
    match data_type {
        SqlDataType::Boolean => RelationalScalarType::Boolean,
        SqlDataType::BigInt => RelationalScalarType::BigInt,
        SqlDataType::DoublePrecision => RelationalScalarType::DoublePrecision,
        SqlDataType::Text => RelationalScalarType::Text,
        SqlDataType::Bytea => RelationalScalarType::Bytea,
        SqlDataType::Uuid => RelationalScalarType::Uuid,
    }
}

fn compile_schema_value(
    value: SqlValue,
    scalar_type: RelationalScalarType,
) -> Result<RelationalValue> {
    let SqlValue::Literal(value) = value else {
        return Err(SkeinError::Semantic(
            "schema defaults cannot contain parameters".to_string(),
        ));
    };
    match value {
        Value::Null => Ok(RelationalValue::Null),
        Value::Bool(value) => Ok(RelationalValue::Boolean(value)),
        Value::Int(value) => Ok(RelationalValue::BigInt(value)),
        Value::Float(value) => Ok(RelationalValue::DoublePrecision(value)),
        Value::String(value) => Ok(RelationalValue::Text(value)),
        Value::Binary(value) => Ok(RelationalValue::Bytea(value)),
        Value::Uuid(value) => Ok(RelationalValue::Uuid(value)),
        Value::List(_) | Value::Map(_) => Err(SkeinError::Semantic(
            "relational schema defaults must be scalar".to_string(),
        )),
    }
    .and_then(|value| coerce_relational_value(value, scalar_type))
}

#[doc(hidden)]
pub fn coerce_relational_value(
    value: RelationalValue,
    scalar_type: RelationalScalarType,
) -> Result<RelationalValue> {
    match (scalar_type, value) {
        (RelationalScalarType::Uuid, RelationalValue::Text(value)) => {
            skein_core::Uuid::parse_str(&value)
                .map(RelationalValue::Uuid)
                .map_err(|_| SkeinError::Semantic(format!("invalid UUID value {value:?}")))
        }
        (RelationalScalarType::Uuid, RelationalValue::Uuid(value)) => {
            Ok(RelationalValue::Uuid(value))
        }
        (_, value) => Ok(value),
    }
}

#[doc(hidden)]
pub fn reject_non_public_schema(schema: Option<&str>) -> Result<()> {
    if matches!(schema, Some("system" | "information_schema" | "pg_catalog")) {
        return Err(SkeinError::Semantic(format!(
            "PostgreSQL compatibility catalog {} is read-only",
            schema.unwrap_or_default()
        )));
    }
    if schema.is_some_and(|schema| schema != "public") {
        return Err(SkeinError::Semantic(
            "relational content tables must use the public schema".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein_storage::{AppendOrderMode, AppendState, AppendTableSchema};

    #[test]
    fn append_select_compiles_from_storage_neutral_sql_ir() {
        let schema = AppendTableSchema {
            name: "events".to_string(),
            columns: vec![
                RelationalColumnSchema {
                    name: "tenant".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: false,
                    default: None,
                },
                RelationalColumnSchema {
                    name: "sequence".to_string(),
                    scalar_type: RelationalScalarType::BigInt,
                    nullable: false,
                    default: None,
                },
            ],
            partition_key: vec!["tenant".to_string()],
            order_key: vec!["sequence".to_string()],
            order_mode: AppendOrderMode::CallerProvided,
        };
        let state = AppendState::from_checkpoint(
            std::collections::BTreeMap::from([("events".to_string(), schema)]),
            std::collections::BTreeMap::new(),
        )
        .unwrap();

        let plan = compile_append_select_sql(
            "SELECT tenant, sequence FROM events WHERE tenant = 'a' ORDER BY sequence LIMIT 10",
            &[],
            &state,
            100,
        )
        .unwrap()
        .unwrap();

        assert_eq!(plan.table, "events");
        assert_eq!(plan.max_rows, 10);
        assert!(compile_append_select_sql(
            "SELECT tenant, sequence FROM events WHERE tenant = 'a' HAVING FALSE ORDER BY sequence LIMIT 10",
            &[], &state, 100,
        ).is_err());
    }
}

#[cfg(test)]
mod pgq_create_tests;
