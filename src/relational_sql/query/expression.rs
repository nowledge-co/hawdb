use super::{
    projection_contains_aggregate, Binding, BoundRow, RelationalScalarType, RelationalState,
    RelationalValue, Result, Row, SelectProjection, SelectStatement, SkeinError, SqlColumnRef,
    SqlExpression, SqlFunctionArgument, SqlPredicate, SqlValue, Value,
};
use crate::sql::{Expr, ExprKind};
use skein_relational::predicate::predicate_truth_with;
pub(super) use skein_relational::query_value::{
    bind_bound, bind_sql_value, expression_name, relational_ref_to_value, relational_to_value,
    value_to_relational, value_to_relational_as,
};

pub(super) fn projection_uses_non_aggregate_coalesce(projection: &[SelectProjection]) -> bool {
    projection.iter().any(|projection| {
        matches!(
            projection,
            SelectProjection::Expression {
                expression: Expr { kind: ExprKind::Function { name, .. }, .. },
                ..
            } if name == "coalesce" && !projection_contains_aggregate(projection)
        )
    })
}

pub(super) fn validate_non_aggregate_coalesce_projections(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<()> {
    for projection in &select.projection {
        let SelectProjection::Expression { expression, .. } = projection else {
            continue;
        };
        let Expr {
            kind: ExprKind::Function { name, .. },
            ..
        } = expression
        else {
            continue;
        };
        if name == "coalesce" && !projection_contains_aggregate(projection) {
            infer_coalesce_scalar_type(expression, select, parameters, state)?;
        }
    }
    Ok(())
}

pub(super) fn infer_coalesce_scalar_type(
    expression: &SqlExpression,
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<Option<RelationalScalarType>> {
    match expression {
        Expr {
            kind: ExprKind::Column(column),
            ..
        } => resolve_projection_column_type(select, state, column).map(Some),
        Expr {
            kind: ExprKind::Value(value),
            ..
        } => Ok(value_to_relational(bind_sql_value(value, parameters)?)?.scalar_type()),
        Expr {
            kind:
                ExprKind::Function {
                    name,
                    arguments,
                    distinct,
                    filter,
                },
            ..
        } if name == "coalesce" => {
            if *distinct {
                return Err(SkeinError::Semantic(
                    "COALESCE does not accept DISTINCT".to_string(),
                ));
            }
            if filter.is_some() {
                return Err(SkeinError::Semantic(
                    "COALESCE does not accept FILTER".to_string(),
                ));
            }
            if arguments.is_empty() {
                return Err(SkeinError::Semantic(
                    "COALESCE requires at least one argument".to_string(),
                ));
            }
            let mut scalar_type = None;
            for argument in arguments {
                let SqlFunctionArgument::Expression(expression) = argument else {
                    return Err(SkeinError::Semantic(
                        "COALESCE does not accept wildcard".to_string(),
                    ));
                };
                let candidate = infer_coalesce_scalar_type(expression, select, parameters, state)?;
                if let Some(candidate) = candidate {
                    if scalar_type.is_some_and(|scalar_type| scalar_type != candidate) {
                        return Err(SkeinError::Semantic(
                            "COALESCE arguments have incompatible scalar types".to_string(),
                        ));
                    }
                    scalar_type = Some(candidate);
                }
            }
            Ok(scalar_type)
        }
        Expr {
            kind: ExprKind::Function { name, .. },
            ..
        } => Err(SkeinError::Semantic(format!(
            "unsupported COALESCE argument function {name}"
        ))),
        _ => Err(SkeinError::Semantic(
            "unsupported scalar expression".to_owned(),
        )),
    }
}

pub(super) fn resolve_projection_column_type(
    select: &SelectStatement,
    state: &RelationalState,
    column: &SqlColumnRef,
) -> Result<RelationalScalarType> {
    let base_qualifier = select.from_alias.as_deref().unwrap_or(&select.from.name);
    let base = std::iter::once((select.from.name.as_str(), base_qualifier));
    let joins = select.joins.iter().map(|join| {
        (
            join.table.name.as_str(),
            join.alias.as_deref().unwrap_or(&join.table.name),
        )
    });
    let mut matches = base.chain(joins).filter_map(|(table, qualifier)| {
        if column
            .qualifier
            .as_deref()
            .is_some_and(|candidate| candidate != table && candidate != qualifier)
        {
            return None;
        }
        let schema = state
            .table_schema(table)
            .expect("projection relation was validated before expression binding");
        schema
            .column_position(&column.name)
            .map(|position| schema.columns[position].scalar_type)
    });
    let first = matches.next().ok_or_else(|| {
        SkeinError::Semantic(format!("column {} is unknown or ambiguous", column.name))
    })?;
    if matches.next().is_some() {
        return Err(SkeinError::Semantic(format!(
            "column {} is unknown or ambiguous",
            column.name
        )));
    }
    Ok(first)
}

pub(super) fn aggregate_filter_matches(
    filter: Option<&SqlPredicate>,
    row: &BoundRow<'_>,
    parameters: &[Value],
) -> Result<bool> {
    skein_relational::aggregate::aggregate_filter_matches(
        filter,
        &|column| resolve_column_with_type(row, column),
        parameters,
    )
}

pub(super) fn predicate_truth(
    predicate: &SqlPredicate,
    row: &BoundRow<'_>,
    parameters: &[Value],
) -> Result<Option<bool>> {
    predicate_truth_with(predicate, parameters, &|column| {
        resolve_column_with_type(row, column)
    })
}

pub(super) fn resolve_column<'a>(
    row: &'a BoundRow<'a>,
    column: &SqlColumnRef,
) -> Result<&'a RelationalValue> {
    resolve_column_with_type(row, column).map(|(value, _)| value)
}

pub(super) fn resolve_column_with_type<'a>(
    row: &'a BoundRow<'a>,
    column: &SqlColumnRef,
) -> Result<(&'a RelationalValue, RelationalScalarType)> {
    let bindings =
        row.bindings.iter().filter(|binding| {
            column.qualifier.as_deref().is_none_or(|qualifier| {
                qualifier == binding.qualifier || qualifier == binding.table
            }) && binding.schema.column_position(&column.name).is_some()
        });
    let mut bindings = bindings.collect::<Vec<_>>();
    if bindings.len() != 1 {
        return Err(SkeinError::Semantic(format!(
            "column {} is unknown or ambiguous",
            column.name
        )));
    }
    let binding = bindings.pop().expect("one binding");
    let position = binding
        .schema
        .column_position(&column.name)
        .expect("filtered binding has column");
    Ok((
        binding.value(position)?,
        binding.schema.columns[position].scalar_type,
    ))
}

pub(super) fn project_bound_row(
    row: &BoundRow<'_>,
    projection: &[SelectProjection],
    parameters: &[Value],
) -> Result<Row> {
    let mut output = Row::new();
    for item in projection {
        match item {
            SelectProjection::Wildcard => {
                for binding in &row.bindings {
                    for (position, column) in binding.schema.columns.iter().enumerate() {
                        let value = projected_value(position, binding)?;
                        insert_output(&mut output, column.name.clone(), value)?;
                    }
                }
            }
            SelectProjection::Expression {
                expression:
                    Expr {
                        kind: ExprKind::Column(name),
                        ..
                    },
                alias,
                ..
            } => {
                let (_, binding, position) = resolve_binding(row, name)?;
                let value = projected_value(position, binding)?;
                insert_output(
                    &mut output,
                    alias.clone().unwrap_or_else(|| name.name.clone()),
                    value,
                )?;
            }
            SelectProjection::Expression { expression, alias } => insert_output(
                &mut output,
                alias.clone().unwrap_or_else(|| expression_name(expression)),
                relational_to_value(&evaluate_projection_expression(
                    expression, row, parameters,
                )?)?,
            )?,
        }
    }
    Ok(output)
}

pub(super) fn evaluate_projection_expression(
    expression: &SqlExpression,
    row: &BoundRow<'_>,
    parameters: &[Value],
) -> Result<RelationalValue> {
    match expression {
        Expr {
            kind: ExprKind::Column(column),
            ..
        } => Ok(resolve_column(row, column)?.clone()),
        Expr {
            kind: ExprKind::Value(SqlValue::Literal(value)),
            ..
        } => value_to_relational(value.clone()),
        Expr {
            kind: ExprKind::Value(SqlValue::Parameter(position)),
            ..
        } => Err(SkeinError::Semantic(format!(
            "projection expression cannot bind parameter ${position}"
        ))),
        Expr {
            kind:
                ExprKind::Function {
                    name,
                    arguments,
                    distinct: false,
                    filter: None,
                },
            ..
        } if name == "uuidv7" && arguments.is_empty() => {
            Ok(RelationalValue::Uuid(skein_core::generate_uuidv7()?))
        }
        Expr {
            kind:
                ExprKind::Function {
                    name,
                    arguments,
                    distinct: false,
                    filter: None,
                },
            ..
        } if name == "coalesce" => evaluate_coalesce(arguments, row, parameters),
        Expr {
            kind: ExprKind::Function { name, .. },
            ..
        } => Err(SkeinError::Semantic(format!(
            "unsupported relational projection function {name}"
        ))),
        _ => Err(SkeinError::Semantic(
            "unsupported projection expression".to_owned(),
        )),
    }
}

pub(super) fn evaluate_coalesce(
    arguments: &[SqlFunctionArgument],
    row: &BoundRow<'_>,
    parameters: &[Value],
) -> Result<RelationalValue> {
    for argument in arguments {
        let SqlFunctionArgument::Expression(expression) = argument else {
            return Err(SkeinError::Semantic(
                "COALESCE does not accept wildcard".to_string(),
            ));
        };
        let value = match expression {
            Expr {
                kind: ExprKind::Column(column),
                ..
            } => resolve_column(row, column)?.clone(),
            Expr {
                kind: ExprKind::Value(value),
                ..
            } => value_to_relational(bind_sql_value(value, parameters)?)?,
            Expr {
                kind:
                    ExprKind::Function {
                        name,
                        arguments,
                        distinct: false,
                        filter: None,
                    },
                ..
            } if name == "coalesce" => evaluate_coalesce(arguments, row, parameters)?,
            Expr {
                kind: ExprKind::Function { name, .. },
                ..
            } => {
                return Err(SkeinError::Semantic(format!(
                    "unsupported COALESCE argument function {name}"
                )))
            }
            _ => {
                return Err(SkeinError::Semantic(
                    "unsupported COALESCE argument expression".to_owned(),
                ))
            }
        };
        if !matches!(value, RelationalValue::Null) {
            return Ok(value);
        }
    }
    Ok(RelationalValue::Null)
}

pub(super) fn resolve_binding<'a>(
    row: &'a BoundRow<'a>,
    column: &SqlColumnRef,
) -> Result<(usize, &'a Binding<'a>, usize)> {
    let mut matches = row
        .bindings
        .iter()
        .enumerate()
        .filter_map(|(index, binding)| {
            let qualifier_matches = column.qualifier.as_deref().is_none_or(|qualifier| {
                qualifier == binding.qualifier || qualifier == binding.table
            });
            qualifier_matches
                .then(|| binding.schema.column_position(&column.name))
                .flatten()
                .map(|position| (index, binding, position))
        });
    let first = matches.next().ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational column {}", column.name))
    })?;
    if matches.next().is_some() {
        return Err(SkeinError::Semantic(format!(
            "ambiguous relational column {}",
            column.name
        )));
    }
    Ok(first)
}

pub(super) fn projected_value(position: usize, binding: &Binding<'_>) -> Result<Value> {
    relational_to_value(binding.value(position)?)
}

pub(super) fn insert_output(output: &mut Row, name: String, value: Value) -> Result<()> {
    if output.insert(name.clone(), value).is_some() {
        return Err(SkeinError::Semantic(format!(
            "relational projection contains duplicate output column {name}"
        )));
    }
    Ok(())
}

pub(super) fn account_intermediate(total: &mut usize, rows: usize, limit: usize) -> Result<()> {
    *total = total.checked_add(rows).ok_or_else(|| {
        SkeinError::Execution("relational intermediate row count overflow".to_string())
    })?;
    if *total > limit {
        return Err(SkeinError::Execution(format!(
            "relational SQL exceeds max_intermediate_rows {limit}"
        )));
    }
    Ok(())
}

pub(super) fn reject_non_public_schema(schema: Option<&str>) -> Result<()> {
    if schema.is_some_and(|schema| schema != "public") {
        return Err(SkeinError::Semantic(
            "relational content tables must use the public schema".to_string(),
        ));
    }
    Ok(())
}
