use super::{
    bind_sql_value, projected_relational_value, projected_value, relational_to_value,
    resolve_binding, BoundRow, PlannedJoin,
};
use crate::error::{Result, SkeinError};
use crate::sql::{
    SelectProjection, SelectStatement, SqlColumnRef, SqlExpression, SqlFunctionArgument,
    SqlPredicate,
};
use crate::value::{JsonDocument, Value};
use skein_storage::{
    RelationalHydrationBudget, RelationalRow, RelationalScalarType, RelationalState,
    RelationalTableSchema, RelationalValue,
};
use std::collections::BTreeMap;

pub(super) fn validate_json_query_semantics(
    select: &SelectStatement,
    base_schema: &RelationalTableSchema,
    base_table: &str,
    base_qualifier: &str,
    joins: &[PlannedJoin<'_>],
) -> Result<()> {
    if let Some(predicate) = &select.selection {
        validate_json_predicate(predicate, base_schema, base_table, base_qualifier, joins)?;
    }
    for join in joins {
        validate_json_predicate(
            &join.join.on,
            base_schema,
            base_table,
            base_qualifier,
            joins,
        )?;
    }
    for item in &select.order_by {
        reject_json_operation(
            query_column_scalar_type(&item.column, base_schema, base_table, base_qualifier, joins)?,
            "ORDER BY",
        )?;
    }
    for column in &select.group_by {
        reject_json_operation(
            query_column_scalar_type(column, base_schema, base_table, base_qualifier, joins)?,
            "GROUP BY",
        )?;
    }
    for projection in &select.projection {
        if select.distinct {
            validate_distinct_projection(
                projection,
                base_schema,
                base_table,
                base_qualifier,
                joins,
            )?;
        }
        if let SelectProjection::Expression { expression, .. } = projection {
            validate_json_aggregate_expression(
                expression,
                base_schema,
                base_table,
                base_qualifier,
                joins,
            )?;
        }
    }
    Ok(())
}

fn validate_distinct_projection(
    projection: &SelectProjection,
    base_schema: &RelationalTableSchema,
    base_table: &str,
    base_qualifier: &str,
    joins: &[PlannedJoin<'_>],
) -> Result<()> {
    match projection {
        SelectProjection::Wildcard => {
            if base_schema
                .columns
                .iter()
                .chain(joins.iter().flat_map(|join| join.schema.columns.iter()))
                .any(|column| column.scalar_type == RelationalScalarType::Json)
            {
                return Err(SkeinError::Semantic(
                    "SELECT DISTINCT does not support JSON values".to_string(),
                ));
            }
        }
        SelectProjection::Column { name, .. } => reject_json_operation(
            query_column_scalar_type(name, base_schema, base_table, base_qualifier, joins)?,
            "SELECT DISTINCT",
        )?,
        SelectProjection::Expression { expression, .. } => {
            if let Some(scalar_type) = query_expression_scalar_type(
                expression,
                base_schema,
                base_table,
                base_qualifier,
                joins,
            )? {
                reject_json_operation(scalar_type, "SELECT DISTINCT")?;
            }
        }
    }
    Ok(())
}

fn validate_json_predicate(
    predicate: &SqlPredicate,
    base_schema: &RelationalTableSchema,
    base_table: &str,
    base_qualifier: &str,
    joins: &[PlannedJoin<'_>],
) -> Result<()> {
    let column_type =
        |column| query_column_scalar_type(column, base_schema, base_table, base_qualifier, joins);
    match predicate {
        SqlPredicate::And(left, right) | SqlPredicate::Or(left, right) => {
            validate_json_predicate(left, base_schema, base_table, base_qualifier, joins)?;
            validate_json_predicate(right, base_schema, base_table, base_qualifier, joins)
        }
        SqlPredicate::Not(predicate) => {
            validate_json_predicate(predicate, base_schema, base_table, base_qualifier, joins)
        }
        SqlPredicate::Compare { left, .. } | SqlPredicate::InList { left, .. } => {
            reject_json_operation(column_type(left)?, "comparison")
        }
        SqlPredicate::CompareColumns { left, right, .. } => {
            reject_json_operation(column_type(left)?, "comparison")?;
            reject_json_operation(column_type(right)?, "comparison")
        }
        SqlPredicate::IsNull { .. } => Ok(()),
    }
}

fn validate_json_aggregate_expression(
    expression: &SqlExpression,
    base_schema: &RelationalTableSchema,
    base_table: &str,
    base_qualifier: &str,
    joins: &[PlannedJoin<'_>],
) -> Result<()> {
    let SqlExpression::Function {
        name,
        arguments,
        distinct,
    } = expression
    else {
        return Ok(());
    };
    if name == "max" || (name == "count" && *distinct) {
        for argument in arguments {
            if let SqlFunctionArgument::Expression(argument) = argument
                && let Some(scalar_type) = query_expression_scalar_type(
                    argument,
                    base_schema,
                    base_table,
                    base_qualifier,
                    joins,
                )?
            {
                reject_json_operation(
                    scalar_type,
                    if name == "max" {
                        "MAX"
                    } else {
                        "COUNT DISTINCT"
                    },
                )?;
            }
        }
    }
    for argument in arguments {
        if let SqlFunctionArgument::Expression(argument) = argument {
            validate_json_aggregate_expression(
                argument,
                base_schema,
                base_table,
                base_qualifier,
                joins,
            )?;
        }
    }
    Ok(())
}

fn query_expression_scalar_type(
    expression: &SqlExpression,
    base_schema: &RelationalTableSchema,
    base_table: &str,
    base_qualifier: &str,
    joins: &[PlannedJoin<'_>],
) -> Result<Option<RelationalScalarType>> {
    match expression {
        SqlExpression::Column(column) => {
            query_column_scalar_type(column, base_schema, base_table, base_qualifier, joins)
                .map(Some)
        }
        SqlExpression::Value(_) => Ok(None),
        SqlExpression::Function {
            name, arguments, ..
        } => match name.as_str() {
            "json_extract" => Ok(Some(RelationalScalarType::Json)),
            "json_valid" => Ok(Some(RelationalScalarType::Boolean)),
            "octet_length" | "count" | "sum" => Ok(Some(RelationalScalarType::BigInt)),
            "max" => first_expression_scalar_type(
                arguments,
                base_schema,
                base_table,
                base_qualifier,
                joins,
            ),
            "coalesce" => {
                let mut scalar_type = None;
                for argument in arguments {
                    if let SqlFunctionArgument::Expression(expression) = argument {
                        let candidate = query_expression_scalar_type(
                            expression,
                            base_schema,
                            base_table,
                            base_qualifier,
                            joins,
                        )?;
                        if candidate == Some(RelationalScalarType::Json) {
                            return Ok(candidate);
                        }
                        scalar_type = scalar_type.or(candidate);
                    }
                }
                Ok(scalar_type)
            }
            _ => Ok(None),
        },
    }
}

fn first_expression_scalar_type(
    arguments: &[SqlFunctionArgument],
    base_schema: &RelationalTableSchema,
    base_table: &str,
    base_qualifier: &str,
    joins: &[PlannedJoin<'_>],
) -> Result<Option<RelationalScalarType>> {
    for argument in arguments {
        if let SqlFunctionArgument::Expression(expression) = argument
            && let Some(scalar_type) = query_expression_scalar_type(
                expression,
                base_schema,
                base_table,
                base_qualifier,
                joins,
            )?
        {
            return Ok(Some(scalar_type));
        }
    }
    Ok(None)
}

fn query_column_scalar_type(
    column: &SqlColumnRef,
    base_schema: &RelationalTableSchema,
    base_table: &str,
    base_qualifier: &str,
    joins: &[PlannedJoin<'_>],
) -> Result<RelationalScalarType> {
    let mut matches = Vec::new();
    if column
        .qualifier
        .as_deref()
        .is_none_or(|qualifier| qualifier == base_table || qualifier == base_qualifier)
        && let Some(position) = base_schema.column_position(&column.name)
    {
        matches.push(base_schema.columns[position].scalar_type);
    }
    for join in joins {
        if column.qualifier.as_deref().is_none_or(|qualifier| {
            qualifier == join.join.table.name || qualifier == join.qualifier
        }) && let Some(position) = join.schema.column_position(&column.name)
        {
            matches.push(join.schema.columns[position].scalar_type);
        }
    }
    match matches.as_slice() {
        [scalar_type] => Ok(*scalar_type),
        [] => Err(SkeinError::Semantic(format!(
            "unknown relational column {}",
            column.name
        ))),
        _ => Err(SkeinError::Semantic(format!(
            "ambiguous relational column {}",
            column.name
        ))),
    }
}

fn reject_json_operation(scalar_type: RelationalScalarType, operation: &str) -> Result<()> {
    if scalar_type == RelationalScalarType::Json {
        return Err(SkeinError::Semantic(format!(
            "{operation} does not support JSON values"
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn evaluate_projection_expression(
    expression: &SqlExpression,
    row: &BoundRow<'_>,
    parameters: &[Value],
    state: &RelationalState,
    hydration: &mut RelationalHydrationBudget,
    hydrated: &mut BTreeMap<usize, RelationalRow>,
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<(Value, Option<RelationalScalarType>)> {
    match expression {
        SqlExpression::Column(column) => {
            let (binding_index, binding, position) = resolve_binding(row, column)?;
            let value = projected_value(
                binding_index,
                position,
                binding,
                state,
                hydration,
                hydrated,
                task_context,
            )?;
            Ok((value, Some(binding.schema.columns[position].scalar_type)))
        }
        SqlExpression::Value(value) => Ok((bind_sql_value(value, parameters)?, None)),
        SqlExpression::Function {
            name,
            arguments,
            distinct,
        } => {
            if *distinct {
                return Err(SkeinError::Semantic(format!(
                    "{name} does not support DISTINCT"
                )));
            }
            match name.as_str() {
                "json_valid" => evaluate_json_valid(
                    arguments,
                    row,
                    parameters,
                    state,
                    hydration,
                    hydrated,
                    task_context,
                ),
                "json_extract" => evaluate_json_extract(
                    arguments,
                    row,
                    parameters,
                    state,
                    hydration,
                    hydrated,
                    task_context,
                ),
                _ => Err(SkeinError::Semantic(format!(
                    "unsupported non-aggregate relational function {name}"
                ))),
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn evaluate_json_valid(
    arguments: &[SqlFunctionArgument],
    row: &BoundRow<'_>,
    parameters: &[Value],
    state: &RelationalState,
    hydration: &mut RelationalHydrationBudget,
    hydrated: &mut BTreeMap<usize, RelationalRow>,
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<(Value, Option<RelationalScalarType>)> {
    let [SqlFunctionArgument::Expression(argument)] = arguments else {
        return Err(SkeinError::Semantic(
            "JSON_VALID requires exactly one expression".to_string(),
        ));
    };
    let valid = if let SqlExpression::Column(column) = argument {
        let (binding_index, binding, position) = resolve_binding(row, column)?;
        match projected_relational_value(
            binding_index,
            position,
            binding,
            state,
            hydration,
            hydrated,
            task_context,
        )? {
            RelationalValue::Null => {
                return Ok((Value::Null, Some(RelationalScalarType::Boolean)));
            }
            RelationalValue::Json(_) => true,
            RelationalValue::Text(value) => JsonDocument::parse(&value).is_ok(),
            value => relational_to_value(&value)
                .and_then(|value| JsonDocument::from_value(&value))
                .is_ok(),
        }
    } else {
        let (value, scalar_type) = evaluate_projection_expression(
            argument,
            row,
            parameters,
            state,
            hydration,
            hydrated,
            task_context,
        )?;
        if value == Value::Null {
            return Ok((Value::Null, Some(RelationalScalarType::Boolean)));
        }
        json_document_from_value(&value, scalar_type).is_ok()
    };
    Ok((Value::Bool(valid), Some(RelationalScalarType::Boolean)))
}

#[allow(clippy::too_many_arguments)]
fn evaluate_json_extract(
    arguments: &[SqlFunctionArgument],
    row: &BoundRow<'_>,
    parameters: &[Value],
    state: &RelationalState,
    hydration: &mut RelationalHydrationBudget,
    hydrated: &mut BTreeMap<usize, RelationalRow>,
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<(Value, Option<RelationalScalarType>)> {
    let [SqlFunctionArgument::Expression(document), SqlFunctionArgument::Expression(path)] =
        arguments
    else {
        return Err(SkeinError::Semantic(
            "JSON_EXTRACT requires a document and a path".to_string(),
        ));
    };
    let Some(document) = evaluate_json_document(
        document,
        row,
        parameters,
        state,
        hydration,
        hydrated,
        task_context,
    )?
    else {
        return Ok((Value::Null, Some(RelationalScalarType::Json)));
    };
    let (path, _) = evaluate_projection_expression(
        path,
        row,
        parameters,
        state,
        hydration,
        hydrated,
        task_context,
    )?;
    let Value::String(path) = path else {
        return Err(SkeinError::Semantic(
            "JSON_EXTRACT path must be TEXT".to_string(),
        ));
    };
    Ok((
        document.extract(&path)?.unwrap_or(Value::Null),
        Some(RelationalScalarType::Json),
    ))
}

#[allow(clippy::too_many_arguments)]
fn evaluate_json_document(
    expression: &SqlExpression,
    row: &BoundRow<'_>,
    parameters: &[Value],
    state: &RelationalState,
    hydration: &mut RelationalHydrationBudget,
    hydrated: &mut BTreeMap<usize, RelationalRow>,
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<Option<JsonDocument>> {
    if let SqlExpression::Column(column) = expression {
        let (binding_index, binding, position) = resolve_binding(row, column)?;
        return match projected_relational_value(
            binding_index,
            position,
            binding,
            state,
            hydration,
            hydrated,
            task_context,
        )? {
            RelationalValue::Null => Ok(None),
            RelationalValue::Json(document) => Ok(Some(document)),
            RelationalValue::Text(value) => JsonDocument::parse(&value).map(Some),
            value => relational_to_value(&value)
                .and_then(|value| JsonDocument::from_value(&value))
                .map(Some),
        };
    }
    let (value, scalar_type) = evaluate_projection_expression(
        expression,
        row,
        parameters,
        state,
        hydration,
        hydrated,
        task_context,
    )?;
    if value == Value::Null {
        return Ok(None);
    }
    json_document_from_value(&value, scalar_type).map(Some)
}

fn json_document_from_value(
    value: &Value,
    scalar_type: Option<RelationalScalarType>,
) -> Result<JsonDocument> {
    if scalar_type == Some(RelationalScalarType::Json) {
        JsonDocument::from_value(value)
    } else if let Value::String(value) = value {
        JsonDocument::parse(value)
    } else {
        JsonDocument::from_value(value)
    }
}
