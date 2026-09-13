use super::{
    expression_contains_aggregate, Binding, BoundRow, RelationalScalarType, RelationalState,
    RelationalValue, RelationalValueRef, Result, Row, SelectProjection, SelectStatement,
    SkeinError, SqlBound, SqlColumnRef, SqlComparisonOp, SqlExpression, SqlFunctionArgument,
    SqlPredicate, SqlValue, Value,
};
use crate::sql::{Expr, ExprKind};

pub(super) fn projection_contains_aggregate(projection: &SelectProjection) -> bool {
    match projection {
        SelectProjection::Expression { expression, .. } => {
            expression_contains_aggregate(expression)
        }
        SelectProjection::Wildcard => false,
    }
}

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

pub(super) fn evaluate_row_expression(
    expression: &SqlExpression,
    row: &BoundRow<'_>,
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
            "aggregate row expression cannot bind parameter ${position}"
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
        } if name == "octet_length" => {
            let [SqlFunctionArgument::Expression(Expr {
                kind: ExprKind::Column(column),
                ..
            })] = arguments.as_slice()
            else {
                return Err(SkeinError::Semantic(
                    "OCTET_LENGTH requires exactly one column".to_string(),
                ));
            };
            match resolve_column(row, column)? {
                RelationalValue::Null => Ok(RelationalValue::Null),
                RelationalValue::Text(value) => Ok(RelationalValue::BigInt(
                    i64::try_from(value.len()).unwrap_or(i64::MAX),
                )),
                RelationalValue::Bytea(value) => Ok(RelationalValue::BigInt(
                    i64::try_from(value.len()).unwrap_or(i64::MAX),
                )),
                RelationalValue::Overflow(reference) => Ok(RelationalValue::BigInt(
                    i64::try_from(reference.uncompressed_bytes).unwrap_or(i64::MAX),
                )),
                _ => Err(SkeinError::Semantic(
                    "OCTET_LENGTH requires TEXT or BYTEA input".to_string(),
                )),
            }
        }
        Expr {
            kind: ExprKind::Function { name, .. },
            ..
        } => Err(SkeinError::Semantic(format!(
            "unsupported aggregate row function {name}"
        ))),
        _ => Err(SkeinError::Semantic(
            "unsupported scalar expression".to_owned(),
        )),
    }
}

pub(super) fn aggregate_filter_matches(
    filter: Option<&SqlPredicate>,
    row: &BoundRow<'_>,
    parameters: &[Value],
) -> Result<bool> {
    match filter {
        Some(filter) => Ok(predicate_truth(filter, row, parameters)? == Some(true)),
        None => Ok(true),
    }
}

pub(super) fn compare_value_refs(
    left: RelationalValueRef<'_>,
    right: RelationalValueRef<'_>,
    op: SqlComparisonOp,
) -> Result<Option<bool>> {
    if matches!(left, RelationalValueRef::Overflow(_))
        || matches!(right, RelationalValueRef::Overflow(_))
    {
        return Err(SkeinError::Execution(
            "relational filter or join requires overflow hydration before qualification"
                .to_string(),
        ));
    }
    if matches!(left, RelationalValueRef::Null) || matches!(right, RelationalValueRef::Null) {
        return Ok(None);
    }
    if left.scalar_type() != right.scalar_type() {
        return Err(SkeinError::Semantic(
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

pub(super) fn relational_ref_to_value(value: RelationalValueRef<'_>) -> Result<Value> {
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

pub(super) fn predicate_truth(
    predicate: &SqlPredicate,
    row: &BoundRow<'_>,
    parameters: &[Value],
) -> Result<Option<bool>> {
    match &predicate.kind {
        ExprKind::And(left, right) => match predicate_truth(left, row, parameters)? {
            Some(false) => Ok(Some(false)),
            Some(true) => predicate_truth(right, row, parameters),
            None => match predicate_truth(right, row, parameters)? {
                Some(false) => Ok(Some(false)),
                Some(true) | None => Ok(None),
            },
        },
        ExprKind::Or(left, right) => match predicate_truth(left, row, parameters)? {
            Some(true) => Ok(Some(true)),
            Some(false) => predicate_truth(right, row, parameters),
            None => match predicate_truth(right, row, parameters)? {
                Some(true) => Ok(Some(true)),
                Some(false) | None => Ok(None),
            },
        },
        ExprKind::Not(predicate) => {
            Ok(predicate_truth(predicate, row, parameters)?.map(|value| !value))
        }
        ExprKind::Compare { left, op, right } => {
            let left = left.require_column()?;
            match &right.kind {
                ExprKind::Value(right) => {
                    let (left_value, scalar_type) = resolve_column_with_type(row, left)?;
                    compare_values(
                        left_value,
                        &value_to_relational_as(bind_sql_value(right, parameters)?, scalar_type)?,
                        *op,
                    )
                }
                ExprKind::Column(right) => {
                    compare_values(resolve_column(row, left)?, resolve_column(row, right)?, *op)
                }
                _ => Err(SkeinError::Semantic(
                    "unsupported comparison operand".to_owned(),
                )),
            }
        }
        ExprKind::InList {
            left,
            values,
            negated,
        } => {
            let (left, scalar_type) = resolve_column_with_type(row, left.require_column()?)?;
            let mut has_unknown = false;
            let mut matched = false;
            for value in values {
                match compare_values(
                    left,
                    &value_to_relational_as(
                        bind_sql_value(value.require_value()?, parameters)?,
                        scalar_type,
                    )?,
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
            let (left, scalar_type) = resolve_column_with_type(row, left.require_column()?)?;
            if scalar_type != RelationalScalarType::Text {
                return Err(SkeinError::Semantic(
                    "LIKE and ILIKE require a TEXT column".to_string(),
                ));
            }
            let pattern = value_to_relational_as(
                bind_sql_value(pattern.require_value()?, parameters)?,
                RelationalScalarType::Text,
            )?;
            match (left, pattern) {
                (RelationalValue::Null, _) | (_, RelationalValue::Null) => Ok(None),
                (RelationalValue::Text(value), RelationalValue::Text(pattern)) => {
                    let matched =
                        skein_sql::sql_like_matches(value, &pattern, *escape, *case_insensitive)?;
                    Ok(Some(matched != *negated))
                }
                (RelationalValue::Overflow(_), _) => Err(SkeinError::Execution(
                    "LIKE reached an overflow value without hydration".to_string(),
                )),
                _ => Err(SkeinError::Semantic(
                    "LIKE and ILIKE require TEXT values".to_string(),
                )),
            }
        }
        ExprKind::IsNull {
            expression: column,
            negated,
        } => Ok(Some(
            matches!(
                resolve_column(row, column.require_column()?)?,
                RelationalValue::Null
            ) != *negated,
        )),
        _ => Err(SkeinError::Semantic(
            "unsupported relational predicate expression".to_owned(),
        )),
    }
}

pub(super) fn compare_values(
    left: &RelationalValue,
    right: &RelationalValue,
    op: SqlComparisonOp,
) -> Result<Option<bool>> {
    compare_value_refs(left.as_ref(), right.as_ref(), op)
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

pub(super) fn bind_bound(
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

pub(super) fn bind_sql_value(value: &SqlValue, parameters: &[Value]) -> Result<Value> {
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

pub(super) fn value_to_relational(value: Value) -> Result<RelationalValue> {
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

pub(super) fn value_to_relational_as(
    value: Value,
    scalar_type: RelationalScalarType,
) -> Result<RelationalValue> {
    super::coerce_relational_value(value_to_relational(value)?, scalar_type)
}

pub(super) fn relational_to_value(value: &RelationalValue) -> Result<Value> {
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

pub(super) fn expression_name(expression: &SqlExpression) -> String {
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
