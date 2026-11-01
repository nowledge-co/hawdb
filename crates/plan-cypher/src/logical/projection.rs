// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) fn plan_comparison_op(op: CypherComparisonOp) -> ComparisonOp {
    match op {
        CypherComparisonOp::Lt => ComparisonOp::Lt,
        CypherComparisonOp::Lte => ComparisonOp::Lte,
        CypherComparisonOp::Gt => ComparisonOp::Gt,
        CypherComparisonOp::Gte => ComparisonOp::Gte,
    }
}

pub(super) fn plan_sort_items(
    scope: &BTreeSet<String>,
    projection_names: &BTreeSet<String>,
    items: &[OrderItem],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<SortItem>> {
    items
        .iter()
        .map(|item| {
            let key = match &item.expression {
                OrderExpression::Property { variable, property } => {
                    let projected_property = format!("{variable}.{property}");
                    if projection_names.contains(&projected_property) {
                        return Ok(SortItem {
                            key: SortKey::Column(projected_property),
                            direction: match item.direction {
                                CypherOrderDirection::Asc => SortDirection::Asc,
                                CypherOrderDirection::Desc => SortDirection::Desc,
                            },
                        });
                    }
                    if projection_names.contains(variable) {
                        return Ok(SortItem {
                            key: SortKey::Expression(ProjectionExpression::ColumnProperty {
                                column: variable.clone(),
                                property: property.clone(),
                            }),
                            direction: match item.direction {
                                CypherOrderDirection::Asc => SortDirection::Asc,
                                CypherOrderDirection::Desc => SortDirection::Desc,
                            },
                        });
                    }
                    if !scope.contains(variable) {
                        return Err(HawDBError::Semantic(format!(
                            "unknown variable '{variable}' in order item"
                        )));
                    }
                    SortKey::Property {
                        variable: variable.clone(),
                        property: property.clone(),
                    }
                }
                OrderExpression::Id { variable } => {
                    if !scope.contains(variable) {
                        return Err(HawDBError::Semantic(format!(
                            "unknown variable '{variable}' in order item"
                        )));
                    }
                    SortKey::Id {
                        variable: variable.clone(),
                    }
                }
                OrderExpression::Value(expression) => SortKey::Expression(
                    plan_order_value_expression(scope, projection_names, expression, parameters)?,
                ),
                OrderExpression::Projection(expression) => {
                    // `bind_projection` resolves aggregate keys before it gets
                    // here; this guards any other caller.
                    if super::arithmetic::contains_aggregate(expression) {
                        return Err(HawDBError::Semantic(
                            "aggregate ORDER BY keys require a RETURN or WITH projection"
                                .to_string(),
                        ));
                    }
                    SortKey::Expression(super::arithmetic::bind_composed_expression(
                        scope,
                        projection_names,
                        expression,
                        parameters,
                    )?)
                }
                OrderExpression::Column(name) => {
                    if !projection_names.contains(name) {
                        return Err(HawDBError::Semantic(format!(
                            "unknown column '{name}' in order item"
                        )));
                    }
                    SortKey::Column(name.clone())
                }
            };
            let direction = match item.direction {
                CypherOrderDirection::Asc => SortDirection::Asc,
                CypherOrderDirection::Desc => SortDirection::Desc,
            };
            Ok(SortItem { key, direction })
        })
        .collect()
}

/// Resolves an aggregate `ORDER BY` key to the column its projection already
/// computes: the alias of an equal projected item, else the key's default
/// name. Ordering never introduces an aggregate of its own.
pub(super) fn plan_aggregate_sort_item(
    scope: &BTreeSet<String>,
    columns: &BTreeSet<String>,
    projection: (&[ReturnItem], &BTreeSet<String>),
    item: &OrderItem,
    expression: &ReturnExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<SortItem> {
    let (items, projection_names) = projection;
    let alias = items
        .iter()
        .find(|projected| &projected.expression == expression)
        .and_then(|projected| projected.alias.clone());
    let name = match alias {
        Some(alias) => alias,
        None => {
            let returned = AstNode::synthetic(ReturnItemKind {
                expression: expression.clone(),
                alias: None,
            });
            plan_return_items_with_columns(
                scope,
                columns,
                std::slice::from_ref(&returned),
                parameters,
            )?
            .names()
            .swap_remove(0)
        }
    };
    if !projection_names.contains(&name) {
        return Err(HawDBError::Semantic(format!(
            "aggregate ORDER BY key '{name}' must also be projected by the RETURN or WITH"
        )));
    }
    Ok(SortItem {
        key: SortKey::Column(name),
        direction: match item.direction {
            CypherOrderDirection::Asc => SortDirection::Asc,
            CypherOrderDirection::Desc => SortDirection::Desc,
        },
    })
}

pub(super) fn plan_order_value_expression(
    scope: &BTreeSet<String>,
    projection_names: &BTreeSet<String>,
    expression: &ScalarExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<ProjectionExpression> {
    if let AstNode {
        kind:
            ScalarExpressionKind::CasePropertyNotNullOrEq {
                variable,
                property,
                empty,
                non_empty,
                null_or_empty,
            },
        ..
    } = expression
    {
        let projected_property = format!("{variable}.{property}");
        if projection_names.contains(&projected_property) {
            return Ok(ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
                column: projected_property,
                empty: bind_value(empty, parameters)?,
                non_empty: bind_value(non_empty, parameters)?,
                null_or_empty: bind_value(null_or_empty, parameters)?,
            });
        }
    }
    if let AstNode {
        kind:
            ScalarExpressionKind::DefaultIfNull {
                variable,
                property,
                default,
            },
        ..
    } = expression
    {
        let projected_property = format!("{variable}.{property}");
        if projection_names.contains(&projected_property) {
            return Ok(ProjectionExpression::ColumnValueDefaultIfNull {
                column: projected_property,
                default: bind_value(default, parameters)?,
            });
        }
    }
    plan_scalar_expression_with_columns(scope, projection_names, expression, parameters)
}

pub(super) fn bind_pagination_value(
    expression: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
    name: &str,
) -> Result<usize> {
    bind_non_negative_usize(expression, parameters, name)
}

pub(super) fn bind_non_negative_usize(
    expression: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
    name: &str,
) -> Result<usize> {
    match bind_value(expression, parameters)? {
        Value::Int(value) if value >= 0 => Ok(value as usize),
        value => Err(HawDBError::Semantic(format!(
            "{name} must be a non-negative integer, got {value:?}"
        ))),
    }
}

fn bind_json_value_path(
    expression: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<String> {
    let Value::String(path) = bind_value(expression, parameters)? else {
        return Err(HawDBError::Semantic(
            "JSON_VALUE path must be a string".into(),
        ));
    };
    let Some(fields) = path.strip_prefix("$.") else {
        return Err(HawDBError::Semantic(
            "JSON_VALUE path must start with $.".into(),
        ));
    };
    if path.len() > 256
        || fields.split('.').any(|field| {
            field.is_empty()
                || !field
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        })
    {
        return Err(HawDBError::Semantic(
            "JSON_VALUE path must contain bounded dotted field names".into(),
        ));
    }
    Ok(path)
}

pub(super) fn bind_optional_f64(
    expression: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
    name: &str,
) -> Result<f64> {
    match bind_value(expression, parameters)? {
        Value::Float(value) => Ok(value),
        Value::Int(value) => Ok(value as f64),
        value => Err(HawDBError::Semantic(format!(
            "{name} must be numeric, got {value:?}"
        ))),
    }
}

pub(super) fn bind_bool(
    expression: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
    name: &str,
) -> Result<bool> {
    match bind_value(expression, parameters)? {
        Value::Bool(value) => Ok(value),
        value => Err(HawDBError::Semantic(format!(
            "{name} must be a boolean, got {value:?}"
        ))),
    }
}

pub(super) enum PlannedReturns {
    Projections(Vec<Projection>),
    Aggregations {
        group_keys: Vec<Projection>,
        items: Vec<Aggregation>,
    },
    AggregateProjection {
        group_keys: Vec<Projection>,
        items: Vec<Aggregation>,
        projections: Vec<Projection>,
    },
}

impl PlannedReturns {
    /// The grouping keys of an aggregating projection.
    pub(super) fn group_keys(&self) -> Option<&[Projection]> {
        match self {
            Self::Projections(_) => None,
            Self::Aggregations { group_keys, .. }
            | Self::AggregateProjection { group_keys, .. } => Some(group_keys),
        }
    }
}

impl PlannedReturns {
    pub(super) fn names(&self) -> Vec<String> {
        match self {
            PlannedReturns::Projections(items) => {
                items.iter().map(|item| item.name.clone()).collect()
            }
            PlannedReturns::Aggregations { group_keys, items } => {
                let mut names = group_keys
                    .iter()
                    .map(|item| item.name.clone())
                    .collect::<Vec<_>>();
                names.extend(items.iter().map(|item| item.name.clone()));
                names
            }
            PlannedReturns::AggregateProjection { projections, .. } => {
                projections.iter().map(|item| item.name.clone()).collect()
            }
        }
    }

    pub(super) fn into_logical(self, input: LogicalPlan) -> LogicalPlan {
        let ranked = match &self {
            Self::Aggregations { items, .. } | Self::AggregateProjection { items, .. } => items
                .iter()
                .any(|item| matches!(item.target, AggregateTarget::Ranked { .. })),
            Self::Projections(_) => false,
        };
        let input = if ranked {
            super::pipeline::normalize_ranked_input(input)
        } else {
            input
        };
        match self {
            PlannedReturns::Projections(items) => LogicalPlan::Project {
                items,
                input: Box::new(input),
            },
            PlannedReturns::Aggregations { group_keys, items } => LogicalPlan::Aggregate {
                group_keys,
                items,
                input: Box::new(input),
            },
            PlannedReturns::AggregateProjection {
                group_keys,
                items,
                projections,
            } => LogicalPlan::Project {
                items: projections,
                input: Box::new(LogicalPlan::Aggregate {
                    group_keys,
                    items,
                    input: Box::new(input),
                }),
            },
        }
    }
}

pub(super) fn plan_set_node_properties_return_mode(
    updated_variable: &str,
    returns: &[ReturnItem],
    parameters: &BTreeMap<String, Value>,
) -> Result<SetNodePropertiesReturnMode> {
    if returns.len() == 1 {
        let item = &returns[0];
        match &item.expression.kind {
            ReturnExpressionKind::Aggregate(AggregateExpression::CountAll) => {
                return Ok(SetNodePropertiesReturnMode::Count {
                    name: item.alias.clone().unwrap_or_else(|| "count(*)".to_string()),
                });
            }
            ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
                variable,
                distinct,
            }) if !distinct => {
                if variable != updated_variable {
                    return Err(HawDBError::Semantic(format!(
                        "SET RETURN count variable '{variable}' does not match updated variable '{}'",
                        updated_variable
                    )));
                }
                return Ok(SetNodePropertiesReturnMode::Count {
                    name: item
                        .alias
                        .clone()
                        .unwrap_or_else(|| format!("count({variable})")),
                });
            }
            _ => {}
        }
    }
    let scope = BTreeSet::from([updated_variable.to_string()]);
    let projections = returns
        .iter()
        .map(|item| plan_projection(&scope, item, parameters))
        .collect::<Result<Vec<_>>>()?;
    Ok(SetNodePropertiesReturnMode::Project(projections))
}

pub(super) fn plan_return_items_with_columns(
    scope: &BTreeSet<String>,
    column_scope: &BTreeSet<String>,
    items: &[ReturnItem],
    parameters: &BTreeMap<String, Value>,
) -> Result<PlannedReturns> {
    if items.iter().any(|item| {
        matches!(
            item.expression.kind,
            ReturnExpressionKind::Arithmetic { .. } | ReturnExpressionKind::ListFunction { .. }
        )
    }) {
        return super::arithmetic::plan_composed_returns(scope, column_scope, items, parameters);
    }
    let has_aggregate = items.iter().any(|item| {
        matches!(
            item.expression,
            AstNode {
                kind: ReturnExpressionKind::Aggregate(_),
                ..
            }
        )
    });
    if has_aggregate {
        let mut group_keys = Vec::new();
        let mut aggregations = Vec::new();
        for item in items {
            match &item.expression.kind {
                ReturnExpressionKind::Value(_)
                | ReturnExpressionKind::Path(_)
                | ReturnExpressionKind::Arithmetic { .. }
                | ReturnExpressionKind::ListFunction { .. } => {
                    group_keys.push(plan_projection_with_columns(
                        scope,
                        column_scope,
                        item,
                        parameters,
                    )?);
                }
                ReturnExpressionKind::Aggregate(_) => {
                    aggregations.push(plan_aggregation_with_columns(
                        scope,
                        column_scope,
                        item,
                        parameters,
                    )?);
                }
            }
        }
        Ok(PlannedReturns::Aggregations {
            group_keys,
            items: aggregations,
        })
    } else {
        items
            .iter()
            .map(|item| plan_projection_with_columns(scope, column_scope, item, parameters))
            .collect::<Result<Vec<_>>>()
            .map(PlannedReturns::Projections)
    }
}

pub(super) fn plan_projection(
    scope: &BTreeSet<String>,
    item: &ReturnItem,
    parameters: &BTreeMap<String, Value>,
) -> Result<Projection> {
    plan_projection_with_columns(scope, &BTreeSet::new(), item, parameters)
}

pub(super) fn plan_projection_with_columns(
    scope: &BTreeSet<String>,
    column_scope: &BTreeSet<String>,
    item: &ReturnItem,
    parameters: &BTreeMap<String, Value>,
) -> Result<Projection> {
    let value = match &item.expression.kind {
        ReturnExpressionKind::Value(value) => value,
        ReturnExpressionKind::Path(path) => {
            let (expression, name) = plan_path_function(column_scope, path)?;
            return Ok(Projection {
                expression,
                name: item.alias.clone().unwrap_or(name),
            });
        }
        _ => {
            return Err(HawDBError::Semantic(
                "expected projection return item".to_string(),
            ))
        }
    };
    let (expression, default_name) = match &value.kind {
        ScalarExpressionKind::Path(path) => plan_path_function(column_scope, path)?,
        ScalarExpressionKind::Variable(variable) => {
            if column_scope.contains(variable) {
                return Ok(Projection {
                    expression: ProjectionExpression::Column(variable.clone()),
                    name: item.alias.clone().unwrap_or_else(|| variable.clone()),
                });
            }
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::Variable {
                    variable: variable.clone(),
                },
                variable.clone(),
            )
        }
        ScalarExpressionKind::Property { variable, property } => {
            if column_scope.contains(variable) {
                return Ok(Projection {
                    expression: ProjectionExpression::ColumnProperty {
                        column: variable.clone(),
                        property: property.clone(),
                    },
                    name: item
                        .alias
                        .clone()
                        .unwrap_or_else(|| format!("{variable}.{property}")),
                });
            }
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                format!("{variable}.{property}"),
            )
        }
        ScalarExpressionKind::Id(variable) => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::Id {
                    variable: variable.clone(),
                },
                format!("id({variable})"),
            )
        }
        ScalarExpressionKind::RelationshipType(variable) => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::RelationshipType {
                    variable: variable.clone(),
                },
                format!("label({variable})"),
            )
        }
        ScalarExpressionKind::Value(value) => (
            ProjectionExpression::Literal(bind_value(value, parameters)?),
            "literal".to_string(),
        ),
        ScalarExpressionKind::Coalesce(expressions) => (
            ProjectionExpression::Coalesce(
                expressions
                    .iter()
                    .map(|expression| {
                        plan_scalar_expression_with_columns(
                            scope,
                            column_scope,
                            expression,
                            parameters,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
            "coalesce".to_string(),
        ),
        ScalarExpressionKind::JsonValue { expression, path } => (
            ProjectionExpression::JsonValue {
                expression: Box::new(plan_scalar_expression_with_columns(
                    scope,
                    column_scope,
                    expression,
                    parameters,
                )?),
                path: bind_json_value_path(path, parameters)?,
            },
            "json_value".to_string(),
        ),
        ScalarExpressionKind::Left { expression, length } => (
            ProjectionExpression::Left {
                expression: Box::new(plan_scalar_expression_with_columns(
                    scope,
                    column_scope,
                    expression,
                    parameters,
                )?),
                length: bind_non_negative_usize(length, parameters, "LEFT length")?,
            },
            "left".to_string(),
        ),
        ScalarExpressionKind::VectorDistance {
            metric,
            left,
            right,
        } => (
            ProjectionExpression::VectorDistance {
                metric: *metric,
                left: Box::new(plan_scalar_expression_with_columns(
                    scope,
                    column_scope,
                    left,
                    parameters,
                )?),
                right: Box::new(plan_scalar_expression_with_columns(
                    scope,
                    column_scope,
                    right,
                    parameters,
                )?),
            },
            metric.function_name().to_owned(),
        ),
        ScalarExpressionKind::Lower(expression) => (
            ProjectionExpression::Lower(Box::new(plan_scalar_expression_with_columns(
                scope,
                column_scope,
                expression,
                parameters,
            )?)),
            "lower".to_string(),
        ),
        ScalarExpressionKind::DatePart {
            part,
            variable,
            property,
        } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::DatePart {
                    part: plan_date_part(part)?,
                    variable: variable.clone(),
                    property: property.clone(),
                },
                format!("date_part({part}, {variable}.{property})"),
            )
        }
        ScalarExpressionKind::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        } => {
            if column_scope.contains(variable) {
                return Ok(Projection {
                    expression: ProjectionExpression::ColumnDefaultIfNullOrEq {
                        column: variable.clone(),
                        property: property.clone(),
                        empty: bind_value(empty, parameters)?,
                        default: bind_value(default, parameters)?,
                    },
                    name: item.alias.clone().unwrap_or_else(|| property.clone()),
                });
            }
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::DefaultIfNullOrEq {
                    variable: variable.clone(),
                    property: property.clone(),
                    empty: bind_value(empty, parameters)?,
                    default: bind_value(default, parameters)?,
                },
                property.clone(),
            )
        }
        ScalarExpressionKind::DefaultIfNull {
            variable,
            property,
            default,
        } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::DefaultIfNull {
                    variable: variable.clone(),
                    property: property.clone(),
                    default: bind_value(default, parameters)?,
                },
                property.clone(),
            )
        }
        ScalarExpressionKind::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::CasePropertyNotNullOrEq {
                    variable: variable.clone(),
                    property: property.clone(),
                    empty: bind_value(empty, parameters)?,
                    non_empty: bind_value(non_empty, parameters)?,
                    null_or_empty: bind_value(null_or_empty, parameters)?,
                },
                "case".to_string(),
            )
        }
        ScalarExpressionKind::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::CasePropertyEqualsRank {
                    variable: variable.clone(),
                    property: property.clone(),
                    branches: bind_value_pairs(branches, parameters)?,
                    default: bind_value(default, parameters)?,
                },
                "case".to_string(),
            )
        }
        ScalarExpressionKind::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::CaseLowerPropertyDefault {
                    variable: variable.clone(),
                    property: property.clone(),
                    default: bind_value(default, parameters)?,
                },
                "case".to_string(),
            )
        }
        ScalarExpressionKind::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::CaseCoalesceDifferenceFloorZero {
                    variable: variable.clone(),
                    terms: bind_coalesce_difference_terms(terms, parameters)?,
                },
                "case".to_string(),
            )
        }
        ScalarExpressionKind::Case { .. }
        | ScalarExpressionKind::Binary { .. }
        | ScalarExpressionKind::Not(_)
        | ScalarExpressionKind::IsNull { .. } => (
            plan_case_scalar(scope, column_scope, value, parameters, true)?,
            "case".to_string(),
        ),
        ScalarExpressionKind::ListFunction { function, .. } => (
            plan_scalar_expression_with_columns(scope, column_scope, value, parameters)?,
            function.function_name().to_string(),
        ),
        ScalarExpressionKind::List(_) => (
            plan_scalar_expression_with_columns(scope, column_scope, value, parameters)?,
            "list".to_string(),
        ),
        ScalarExpressionKind::ListComprehension { .. } => (
            plan_scalar_expression_with_columns(scope, column_scope, value, parameters)?,
            "list_comprehension".to_string(),
        ),
        ScalarExpressionKind::ListIndex { .. } => (
            plan_scalar_expression_with_columns(scope, column_scope, value, parameters)?,
            "list_index".to_string(),
        ),
        ScalarExpressionKind::Conversion { function, .. } => (
            plan_scalar_expression_with_columns(scope, column_scope, value, parameters)?,
            function.function_name().to_string(),
        ),
        ScalarExpressionKind::ListSlice { .. } => (
            plan_scalar_expression_with_columns(scope, column_scope, value, parameters)?,
            "list_slice".to_string(),
        ),
        ScalarExpressionKind::TemporalFunction { function, .. } => (
            plan_scalar_expression_with_columns(scope, column_scope, value, parameters)?,
            function.function_name(),
        ),
        ScalarExpressionKind::PropertyAccess { .. } => (
            plan_scalar_expression_with_columns(scope, column_scope, value, parameters)?,
            property_access_name(value),
        ),
        ScalarExpressionKind::MapLiteral(_) => (
            plan_scalar_expression_with_columns(scope, column_scope, value, parameters)?,
            "map".to_string(),
        ),
    };
    Ok(Projection {
        expression,
        name: item.alias.clone().unwrap_or(default_name),
    })
}

pub(super) fn bind_relationship_set_value(
    value: &hawdb_cypher::SetValueExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<Value> {
    match value {
        hawdb_cypher::SetValueExpression::Value(value) => bind_value(value, parameters),
        hawdb_cypher::SetValueExpression::Property { .. } => Err(HawDBError::Semantic(
            "relationship property reference SET is only supported by relationship-copy MERGE"
                .to_string(),
        )),
        hawdb_cypher::SetValueExpression::PropertyAdd { .. }
        | hawdb_cypher::SetValueExpression::CoalesceProperty { .. }
        | hawdb_cypher::SetValueExpression::DecrementFloorZero { .. }
        | hawdb_cypher::SetValueExpression::PreserveNewerExisting { .. }
        | hawdb_cypher::SetValueExpression::CoalescePropertyAdd { .. } => {
            Err(HawDBError::Semantic(
                "relationship property increment SET is not supported".to_string(),
            ))
        }
    }
}

pub(super) fn plan_set_value(
    set: &hawdb_cypher::SetProperty,
    parameters: &BTreeMap<String, Value>,
) -> Result<SetValue> {
    match &set.value {
        hawdb_cypher::SetValueExpression::Value(value) => {
            Ok(SetValue::Value(bind_value(value, parameters)?))
        }
        hawdb_cypher::SetValueExpression::Property { .. } => Err(HawDBError::Semantic(
            "property reference SET is only supported by relationship-copy MERGE".to_string(),
        )),
        hawdb_cypher::SetValueExpression::CoalesceProperty {
            variable,
            property,
            default,
        } => {
            if variable != &set.variable || property != &set.property {
                return Err(HawDBError::Semantic(
                    "COALESCE property SET must read the same variable property it writes"
                        .to_string(),
                ));
            }
            Ok(SetValue::Coalesce {
                property: property.clone(),
                default: bind_value(default, parameters)?,
            })
        }
        hawdb_cypher::SetValueExpression::PropertyAdd {
            variable,
            property,
            value,
        } => {
            if variable != &set.variable || property != &set.property {
                return Err(HawDBError::Semantic(
                    "property increment SET must read the same variable property it writes"
                        .to_string(),
                ));
            }
            let amount = bind_value(value, parameters)?;
            let Value::Int(amount) = amount else {
                return Err(HawDBError::Semantic(
                    "property increment SET requires an integer increment".to_string(),
                ));
            };
            Ok(SetValue::AddInt {
                property: property.clone(),
                amount,
            })
        }
        hawdb_cypher::SetValueExpression::DecrementFloorZero { variable, property } => {
            if variable != &set.variable || property != &set.property {
                return Err(HawDBError::Semantic(
                    "CASE decrement SET must read the same variable property it writes".to_string(),
                ));
            }
            Ok(SetValue::DecrementFloorZero {
                property: property.clone(),
            })
        }
        hawdb_cypher::SetValueExpression::PreserveNewerExisting {
            variable,
            property,
            incoming,
            preserve,
        } => {
            if variable != &set.variable || property != &set.property {
                return Err(HawDBError::Semantic(
                    "CASE preserve SET must read the same variable property it writes".to_string(),
                ));
            }
            let preserve = bind_value(preserve, parameters)?;
            let Value::Bool(preserve) = preserve else {
                return Err(HawDBError::Semantic(
                    "CASE preserve SET requires a boolean preserve flag".to_string(),
                ));
            };
            Ok(SetValue::PreserveNewerExisting {
                property: property.clone(),
                incoming: bind_value(incoming, parameters)?,
                preserve,
            })
        }
        hawdb_cypher::SetValueExpression::CoalescePropertyAdd {
            variable,
            property,
            default,
            value,
        } => {
            if variable != &set.variable || property != &set.property {
                return Err(HawDBError::Semantic(
                    "property increment SET must read the same variable property it writes"
                        .to_string(),
                ));
            }
            let default = bind_value(default, parameters)?;
            if default != Value::Int(0) {
                return Err(HawDBError::Semantic(
                    "COALESCE property increment SET only supports integer zero defaults"
                        .to_string(),
                ));
            }
            let amount = bind_value(value, parameters)?;
            let Value::Int(amount) = amount else {
                return Err(HawDBError::Semantic(
                    "property increment SET requires an integer increment".to_string(),
                ));
            };
            Ok(SetValue::AddInt {
                property: property.clone(),
                amount,
            })
        }
    }
}

pub(super) fn plan_aggregation_with_columns(
    scope: &BTreeSet<String>,
    columns: &BTreeSet<String>,
    item: &ReturnItem,
    parameters: &BTreeMap<String, Value>,
) -> Result<Aggregation> {
    let aggregate = match &item.expression.kind {
        ReturnExpressionKind::Aggregate(aggregate) => aggregate,
        _ => return plan_aggregation(scope, item),
    };
    if let AggregateExpression::Expression {
        function,
        distinct,
        argument,
    } = aggregate
    {
        return plan_expression_aggregation(
            scope, columns, item, *function, *distinct, argument, parameters,
        );
    }
    let direct = match aggregate {
        AggregateExpression::MaxVariable { variable } => {
            if !columns.contains(variable) {
                return Err(HawDBError::Semantic(
                    "MAX without a property requires a scalar column".into(),
                ));
            }
            Some((
                AggregateFunction::Max,
                AggregateTarget::Column(variable.clone()),
            ))
        }
        AggregateExpression::CollectTop {
            document_id,
            score,
            limit,
        } => {
            if !columns.contains(document_id) || !columns.contains(score) {
                return Err(HawDBError::Semantic(
                    "collect_top requires document ID and score scalar columns".into(),
                ));
            }
            let limit = bind_non_negative_usize(limit, parameters, "collect_top evidence limit")?;
            if limit == 0 {
                return Err(HawDBError::Semantic(
                    "collect_top evidence limit must be positive".into(),
                ));
            }
            Some((
                AggregateFunction::Collect,
                AggregateTarget::Ranked {
                    document_id: document_id.clone(),
                    score: score.clone(),
                    limit,
                },
            ))
        }
        _ => None,
    };
    if let Some((function, target)) = direct {
        let name = item
            .alias
            .clone()
            .unwrap_or_else(|| default_aggregation_name(function, &target, false));
        return Ok(Aggregation {
            function,
            target,
            distinct: false,
            name,
        });
    }
    let variable = match aggregate {
        AggregateExpression::MaxVariable { .. }
        | AggregateExpression::CollectTop { .. }
        | AggregateExpression::Expression { .. } => {
            unreachable!("scalar and expression aggregates bound above")
        }
        AggregateExpression::CountAll => return plan_aggregation(scope, item),
        AggregateExpression::CountVariable { variable, .. }
        | AggregateExpression::CollectVariable { variable, .. }
        | AggregateExpression::CountProperty { variable, .. }
        | AggregateExpression::CollectProperty { variable, .. }
        | AggregateExpression::MinProperty { variable, .. }
        | AggregateExpression::MaxProperty { variable, .. }
        | AggregateExpression::AvgProperty { variable, .. } => variable,
    };
    if !columns.contains(variable) {
        return plan_aggregation(scope, item);
    }
    let mut scope = scope.clone();
    scope.insert(variable.clone());
    let mut result = plan_aggregation(&scope, item)?;
    result.target = match result.target {
        AggregateTarget::Variable(column) => AggregateTarget::Column(column),
        AggregateTarget::Property { variable, property } => AggregateTarget::ColumnProperty {
            column: variable,
            property,
        },
        other => other,
    };
    Ok(result)
}

/// Plans an aggregate over a general argument expression. Plain variable and
/// property arguments reuse the direct targets; any other aggregate-free
/// argument is evaluated per input row by the aggregate operator itself.
fn plan_expression_aggregation(
    scope: &BTreeSet<String>,
    columns: &BTreeSet<String>,
    item: &ReturnItem,
    function: AggregateFunctionName,
    distinct: bool,
    argument: &ReturnExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<Aggregation> {
    let function = match function {
        AggregateFunctionName::Count => AggregateFunction::Count,
        AggregateFunctionName::Sum => AggregateFunction::Sum,
        AggregateFunctionName::Min => AggregateFunction::Min,
        AggregateFunctionName::Max => AggregateFunction::Max,
        AggregateFunctionName::Avg => AggregateFunction::Avg,
        AggregateFunctionName::Collect => AggregateFunction::Collect,
    };
    let (expression, argument_name) =
        super::arithmetic::bind_aggregate_argument(scope, columns, argument, parameters)?;
    let argument_name = match &argument.kind {
        ReturnExpressionKind::Arithmetic { .. } => argument_name
            .strip_prefix('(')
            .and_then(|name| name.strip_suffix(')'))
            .map(str::to_string)
            .unwrap_or(argument_name),
        _ => argument_name,
    };
    let target = match expression {
        ProjectionExpression::Column(column) => AggregateTarget::Column(column),
        ProjectionExpression::ColumnProperty { column, property } => {
            AggregateTarget::ColumnProperty { column, property }
        }
        ProjectionExpression::Property { variable, property } => {
            AggregateTarget::Property { variable, property }
        }
        expression => AggregateTarget::Expression(expression),
    };
    let name = item.alias.clone().unwrap_or_else(|| {
        format!(
            "{}({}{argument_name})",
            function.as_str(),
            if distinct { "DISTINCT " } else { "" }
        )
    });
    Ok(Aggregation {
        function,
        target,
        distinct,
        name,
    })
}

pub(super) fn plan_aggregation(scope: &BTreeSet<String>, item: &ReturnItem) -> Result<Aggregation> {
    let AstNode {
        kind: ReturnExpressionKind::Aggregate(value),
        ..
    } = &item.expression
    else {
        return Err(HawDBError::Semantic(
            "expected aggregate return item".to_string(),
        ));
    };
    let (function, target, distinct) = match value {
        AggregateExpression::MaxVariable { .. }
        | AggregateExpression::CollectTop { .. }
        | AggregateExpression::Expression { .. } => {
            return Err(HawDBError::Semantic(
                "scalar aggregate requires column-aware binding".into(),
            ));
        }
        AggregateExpression::CountAll => (AggregateFunction::Count, AggregateTarget::All, false),
        AggregateExpression::CountVariable { variable, distinct } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Count,
                AggregateTarget::Variable(variable.clone()),
                *distinct,
            )
        }
        AggregateExpression::CountProperty {
            variable,
            property,
            distinct,
        } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Count,
                AggregateTarget::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                *distinct,
            )
        }
        AggregateExpression::CollectProperty {
            variable,
            property,
            distinct,
        } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Collect,
                AggregateTarget::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                *distinct,
            )
        }
        AggregateExpression::CollectVariable { variable, distinct } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Collect,
                AggregateTarget::Variable(variable.clone()),
                *distinct,
            )
        }
        AggregateExpression::MinProperty { variable, property } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Min,
                AggregateTarget::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                false,
            )
        }
        AggregateExpression::MaxProperty { variable, property } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Max,
                AggregateTarget::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                false,
            )
        }
        AggregateExpression::AvgProperty { variable, property } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Avg,
                AggregateTarget::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                false,
            )
        }
    };
    let name = item
        .alias
        .clone()
        .unwrap_or_else(|| default_aggregation_name(function, &target, distinct));
    Ok(Aggregation {
        function,
        target,
        distinct,
        name,
    })
}

pub(super) fn plan_scalar_expression(
    scope: &BTreeSet<String>,
    expression: &ScalarExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<ProjectionExpression> {
    plan_scalar_expression_with_columns(scope, &BTreeSet::new(), expression, parameters)
}

pub(super) fn plan_scalar_expression_with_columns(
    scope: &BTreeSet<String>,
    column_scope: &BTreeSet<String>,
    expression: &ScalarExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<ProjectionExpression> {
    match &expression.kind {
        ScalarExpressionKind::Variable(variable) => {
            if column_scope.contains(variable) {
                return Ok(ProjectionExpression::Column(variable.clone()));
            }
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            Ok(ProjectionExpression::Variable {
                variable: variable.clone(),
            })
        }
        ScalarExpressionKind::Property { variable, property } => {
            if column_scope.contains(variable) {
                return Ok(ProjectionExpression::ColumnProperty {
                    column: variable.clone(),
                    property: property.clone(),
                });
            }
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            Ok(ProjectionExpression::Property {
                variable: variable.clone(),
                property: property.clone(),
            })
        }
        ScalarExpressionKind::Id(variable) => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            Ok(ProjectionExpression::Id {
                variable: variable.clone(),
            })
        }
        ScalarExpressionKind::RelationshipType(variable) => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            Ok(ProjectionExpression::RelationshipType {
                variable: variable.clone(),
            })
        }
        ScalarExpressionKind::Value(value) => Ok(ProjectionExpression::Literal(bind_value(
            value, parameters,
        )?)),
        ScalarExpressionKind::Coalesce(expressions) => Ok(ProjectionExpression::Coalesce(
            expressions
                .iter()
                .map(|expression| {
                    plan_scalar_expression_with_columns(scope, column_scope, expression, parameters)
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        ScalarExpressionKind::JsonValue { expression, path } => {
            Ok(ProjectionExpression::JsonValue {
                expression: Box::new(plan_scalar_expression_with_columns(
                    scope,
                    column_scope,
                    expression,
                    parameters,
                )?),
                path: bind_json_value_path(path, parameters)?,
            })
        }
        ScalarExpressionKind::Left { expression, length } => Ok(ProjectionExpression::Left {
            expression: Box::new(plan_scalar_expression_with_columns(
                scope,
                column_scope,
                expression,
                parameters,
            )?),
            length: bind_non_negative_usize(length, parameters, "LEFT length")?,
        }),
        ScalarExpressionKind::VectorDistance {
            metric,
            left,
            right,
        } => Ok(ProjectionExpression::VectorDistance {
            metric: *metric,
            left: Box::new(plan_scalar_expression_with_columns(
                scope,
                column_scope,
                left,
                parameters,
            )?),
            right: Box::new(plan_scalar_expression_with_columns(
                scope,
                column_scope,
                right,
                parameters,
            )?),
        }),
        ScalarExpressionKind::Lower(expression) => Ok(ProjectionExpression::Lower(Box::new(
            plan_scalar_expression_with_columns(scope, column_scope, expression, parameters)?,
        ))),
        ScalarExpressionKind::Path(path) => {
            plan_path_function(column_scope, path).map(|(expression, _)| expression)
        }
        ScalarExpressionKind::DatePart {
            part,
            variable,
            property,
        } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::DatePart {
                part: plan_date_part(part)?,
                variable: variable.clone(),
                property: property.clone(),
            })
        }
        ScalarExpressionKind::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        } => {
            if column_scope.contains(variable) {
                return Ok(ProjectionExpression::ColumnDefaultIfNullOrEq {
                    column: variable.clone(),
                    property: property.clone(),
                    empty: bind_value(empty, parameters)?,
                    default: bind_value(default, parameters)?,
                });
            }
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::DefaultIfNullOrEq {
                variable: variable.clone(),
                property: property.clone(),
                empty: bind_value(empty, parameters)?,
                default: bind_value(default, parameters)?,
            })
        }
        ScalarExpressionKind::DefaultIfNull {
            variable,
            property,
            default,
        } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::DefaultIfNull {
                variable: variable.clone(),
                property: property.clone(),
                default: bind_value(default, parameters)?,
            })
        }
        ScalarExpressionKind::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::CasePropertyNotNullOrEq {
                variable: variable.clone(),
                property: property.clone(),
                empty: bind_value(empty, parameters)?,
                non_empty: bind_value(non_empty, parameters)?,
                null_or_empty: bind_value(null_or_empty, parameters)?,
            })
        }
        ScalarExpressionKind::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::CasePropertyEqualsRank {
                variable: variable.clone(),
                property: property.clone(),
                branches: bind_value_pairs(branches, parameters)?,
                default: bind_value(default, parameters)?,
            })
        }
        ScalarExpressionKind::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::CaseLowerPropertyDefault {
                variable: variable.clone(),
                property: property.clone(),
                default: bind_value(default, parameters)?,
            })
        }
        ScalarExpressionKind::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            if !scope.contains(variable) {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::CaseCoalesceDifferenceFloorZero {
                variable: variable.clone(),
                terms: bind_coalesce_difference_terms(terms, parameters)?,
            })
        }
        ScalarExpressionKind::Case { .. }
        | ScalarExpressionKind::Binary { .. }
        | ScalarExpressionKind::Not(_)
        | ScalarExpressionKind::IsNull { .. } => {
            plan_case_scalar(scope, column_scope, expression, parameters, false)
        }
        ScalarExpressionKind::List(elements) => Ok(ProjectionExpression::List(
            elements
                .iter()
                .map(|element| {
                    plan_scalar_expression_with_columns(scope, column_scope, element, parameters)
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        ScalarExpressionKind::ListFunction {
            function,
            arguments,
        } => Ok(ProjectionExpression::ListFunction {
            function: *function,
            arguments: arguments
                .iter()
                .map(|argument| {
                    plan_scalar_expression_with_columns(scope, column_scope, argument, parameters)
                })
                .collect::<Result<Vec<_>>>()?,
        }),
        ScalarExpressionKind::ListComprehension {
            variable,
            list,
            predicate,
            projection,
        } => plan_list_comprehension(
            scope,
            column_scope,
            ListComprehensionParts {
                variable,
                list,
                predicate: predicate.as_deref(),
                projection: projection.as_deref(),
            },
            parameters,
        ),
        ScalarExpressionKind::Conversion { function, argument } => {
            Ok(ProjectionExpression::Conversion {
                function: *function,
                argument: Box::new(plan_scalar_expression_with_columns(
                    scope,
                    column_scope,
                    argument,
                    parameters,
                )?),
            })
        }
        ScalarExpressionKind::ListIndex { list, index } => Ok(ProjectionExpression::ListIndex {
            list: Box::new(plan_scalar_expression_with_columns(
                scope,
                column_scope,
                list,
                parameters,
            )?),
            index: Box::new(plan_scalar_expression_with_columns(
                scope,
                column_scope,
                index,
                parameters,
            )?),
        }),
        ScalarExpressionKind::ListSlice { list, start, end } => {
            let bind = |expression: &ScalarExpression| {
                plan_scalar_expression_with_columns(scope, column_scope, expression, parameters)
                    .map(Box::new)
            };
            Ok(ProjectionExpression::ListSlice {
                list: bind(list)?,
                start: start.as_deref().map(bind).transpose()?,
                end: end.as_deref().map(bind).transpose()?,
            })
        }
        ScalarExpressionKind::TemporalFunction {
            function,
            arguments,
        } => {
            if function.reads_clock(arguments.len()) {
                // `date()`, `datetime()`, ...: one statement clock, bound now.
                return hawdb_core::temporal::evaluate(
                    *function,
                    &[],
                    hawdb_core::temporal::statement_now(),
                )
                .map(ProjectionExpression::Literal)
                .map_err(|error| HawDBError::Semantic(error.to_string()));
            }
            Ok(ProjectionExpression::TemporalFunction {
                function: *function,
                arguments: arguments
                    .iter()
                    .map(|argument| {
                        plan_scalar_expression_with_columns(
                            scope,
                            column_scope,
                            argument,
                            parameters,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        ScalarExpressionKind::PropertyAccess { expression, key } => {
            Ok(ProjectionExpression::PropertyAccess {
                expression: Box::new(plan_scalar_expression_with_columns(
                    scope,
                    column_scope,
                    expression,
                    parameters,
                )?),
                key: key.clone(),
            })
        }
        ScalarExpressionKind::MapLiteral(entries) => Ok(ProjectionExpression::MapLiteral(
            entries
                .iter()
                .map(|(key, value)| {
                    Ok((
                        key.clone(),
                        plan_scalar_expression_with_columns(
                            scope,
                            column_scope,
                            value,
                            parameters,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        )),
    }
}

/// The default column name of `a.b.c`-style access: the dotted path when
/// the base is a variable or property, otherwise the final key.
fn property_access_name(expression: &ScalarExpression) -> String {
    fn path(expression: &ScalarExpression) -> Option<String> {
        match &expression.kind {
            ScalarExpressionKind::Variable(variable) => Some(variable.clone()),
            ScalarExpressionKind::Property { variable, property } => {
                Some(format!("{variable}.{property}"))
            }
            ScalarExpressionKind::PropertyAccess { expression, key } => {
                Some(format!("{}.{key}", path(expression)?))
            }
            _ => None,
        }
    }
    path(expression).unwrap_or_else(|| match &expression.kind {
        ScalarExpressionKind::PropertyAccess { key, .. } => key.clone(),
        _ => "property".to_string(),
    })
}

struct ListComprehensionParts<'a> {
    variable: &'a str,
    list: &'a ScalarExpression,
    predicate: Option<&'a ScalarExpression>,
    projection: Option<&'a ScalarExpression>,
}

/// Plans `[variable IN list WHERE predicate | projection]`.
///
/// The element variable must not shadow a graph variable or a column that is
/// already visible; reusing a name would make `x` mean different values inside
/// and outside the brackets. Inside the body the element is planned as a
/// column and then rebound to [`ProjectionExpression::LocalVariable`], so no
/// row-column analysis ever sees it.
fn plan_list_comprehension(
    scope: &BTreeSet<String>,
    column_scope: &BTreeSet<String>,
    parts: ListComprehensionParts<'_>,
    parameters: &BTreeMap<String, Value>,
) -> Result<ProjectionExpression> {
    let variable = parts.variable;
    if scope.contains(variable) || column_scope.contains(variable) {
        return Err(HawDBError::Semantic(format!(
            "list comprehension variable '{variable}' shadows a variable that is already in scope; use a different name"
        )));
    }
    let list = plan_scalar_expression_with_columns(scope, column_scope, parts.list, parameters)?;
    let mut body_columns = column_scope.clone();
    body_columns.insert(variable.to_string());
    let bind_body = |expression: &ScalarExpression| {
        let mut planned =
            plan_scalar_expression_with_columns(scope, &body_columns, expression, parameters)?;
        bind_list_local(&mut planned, variable)?;
        Ok::<_, HawDBError>(planned)
    };
    Ok(ProjectionExpression::ListComprehension(Box::new(
        ListComprehensionProjection {
            variable: variable.to_string(),
            list,
            predicate: parts.predicate.map(bind_body).transpose()?,
            projection: parts.projection.map(bind_body).transpose()?,
        },
    )))
}

fn bind_list_local(expression: &mut ProjectionExpression, variable: &str) -> Result<()> {
    match expression {
        ProjectionExpression::Column(column) if column == variable => {
            *expression = ProjectionExpression::LocalVariable {
                name: variable.to_string(),
                property: None,
            };
            Ok(())
        }
        ProjectionExpression::ColumnProperty { column, property } if column == variable => {
            *expression = ProjectionExpression::LocalVariable {
                name: variable.to_string(),
                property: Some(property.clone()),
            };
            Ok(())
        }
        ProjectionExpression::ColumnDefaultIfNullOrEq { column, .. }
        | ProjectionExpression::ColumnValueDefaultIfNull { column, .. }
        | ProjectionExpression::ColumnValueCasePropertyNotNullOrEq { column, .. }
            if column == variable =>
        {
            Err(unsupported_list_local(variable))
        }
        ProjectionExpression::CaseColumnSearchRank(rank) if rank.column == variable => {
            Err(unsupported_list_local(variable))
        }
        _ => expression.try_for_each_child_mut(|child| bind_list_local(child, variable)),
    }
}

fn unsupported_list_local(variable: &str) -> HawDBError {
    HawDBError::Semantic(format!(
        "this CASE form is not supported over list comprehension variable '{variable}'; use a generic CASE WHEN expression"
    ))
}

/// Lowers a path function over a path column bound by a shortest-path MATCH.
/// Other path aliases are never materialized and cannot be projected.
pub(super) fn plan_path_function(
    column_scope: &BTreeSet<String>,
    path: &hawdb_cypher::ShortestPathReturnExpression,
) -> Result<(ProjectionExpression, String)> {
    use hawdb_cypher::ShortestPathReturnExpression as Path;
    let column = path.path_variable();
    if !column_scope.contains(column) {
        return Err(HawDBError::Semantic(format!(
            "path function requires a path value (bound by shortestPath or allShortestPaths, or carried through WITH), got unknown variable '{column}'"
        )));
    }
    let function = match path {
        Path::Length { .. } => PathFunction::Length,
        Path::Nodes { .. } => PathFunction::Nodes,
        Path::Relationships { .. } => PathFunction::Relationships,
        Path::NodePropertyList { property, .. } => PathFunction::NodeProperties(property.clone()),
    };
    let name = function.display(column);
    Ok((
        ProjectionExpression::Path {
            function,
            column: column.to_string(),
        },
        name,
    ))
}

pub(super) fn bind_coalesce_difference_terms(
    terms: &[hawdb_cypher::CoalesceDifferenceTerm],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<CoalesceDifferenceProjectionTerm>> {
    terms
        .iter()
        .map(|term| {
            Ok(CoalesceDifferenceProjectionTerm {
                property: term.property.clone(),
                default: bind_value(&term.default, parameters)?,
            })
        })
        .collect()
}

pub(super) fn bind_value_pairs(
    pairs: &[(ValueExpression, ValueExpression)],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<(Value, Value)>> {
    pairs
        .iter()
        .map(|(left, right)| {
            Ok((
                bind_value(left, parameters)?,
                bind_value(right, parameters)?,
            ))
        })
        .collect()
}

pub(super) fn plan_date_part(part: &str) -> Result<DatePart> {
    match part.to_ascii_lowercase().as_str() {
        "year" => Ok(DatePart::Year),
        "month" => Ok(DatePart::Month),
        _ => Err(HawDBError::Semantic(format!(
            "unsupported date_part component '{part}'"
        ))),
    }
}

pub(super) fn default_aggregation_name(
    function: AggregateFunction,
    target: &AggregateTarget,
    distinct: bool,
) -> String {
    match target {
        AggregateTarget::Ranked {
            document_id,
            score,
            limit,
        } => {
            return format!("collect_top({document_id}, {score}, {limit})");
        }
        AggregateTarget::Column(column) => {
            return default_aggregation_name(
                function,
                &AggregateTarget::Variable(column.clone()),
                distinct,
            )
        }
        AggregateTarget::ColumnProperty { column, property } => {
            return default_aggregation_name(
                function,
                &AggregateTarget::Property {
                    variable: column.clone(),
                    property: property.clone(),
                },
                distinct,
            )
        }
        AggregateTarget::Expression(_) => return format!("{}(?)", function.as_str()),
        _ => {}
    }
    match (function, target) {
        (AggregateFunction::Count, AggregateTarget::All) => "count(*)".to_string(),
        (AggregateFunction::Count, AggregateTarget::Variable(variable)) if distinct => {
            format!("count(DISTINCT {variable})")
        }
        (AggregateFunction::Count, AggregateTarget::Variable(variable)) => {
            format!("count({variable})")
        }
        (AggregateFunction::Count, AggregateTarget::Property { variable, property })
            if distinct =>
        {
            format!("count(DISTINCT {variable}.{property})")
        }
        (AggregateFunction::Count, AggregateTarget::Property { variable, property }) => {
            format!("count({variable}.{property})")
        }
        (AggregateFunction::Sum, AggregateTarget::Property { variable, property }) => {
            format!("sum({variable}.{property})")
        }
        (AggregateFunction::Sum, AggregateTarget::All | AggregateTarget::Variable(_)) => {
            "sum(?)".to_string()
        }
        (AggregateFunction::Min, AggregateTarget::Property { variable, property }) => {
            format!("min({variable}.{property})")
        }
        (AggregateFunction::Min, AggregateTarget::All | AggregateTarget::Variable(_)) => {
            "min(?)".to_string()
        }
        (AggregateFunction::Max, AggregateTarget::Property { variable, property }) => {
            format!("max({variable}.{property})")
        }
        (AggregateFunction::Max, AggregateTarget::Variable(variable)) => format!("max({variable})"),
        (AggregateFunction::Max, AggregateTarget::All) => "max(?)".into(),
        (AggregateFunction::Avg, AggregateTarget::Property { variable, property }) => {
            format!("avg({variable}.{property})")
        }
        (AggregateFunction::Avg, AggregateTarget::All | AggregateTarget::Variable(_)) => {
            "avg(?)".to_string()
        }
        (AggregateFunction::Collect, AggregateTarget::Variable(variable)) if distinct => {
            format!("collect(DISTINCT {variable})")
        }
        (AggregateFunction::Collect, AggregateTarget::Variable(variable)) => {
            format!("collect({variable})")
        }
        (AggregateFunction::Collect, AggregateTarget::Property { variable, property })
            if distinct =>
        {
            format!("collect(DISTINCT {variable}.{property})")
        }
        (AggregateFunction::Collect, AggregateTarget::Property { variable, property }) => {
            format!("collect({variable}.{property})")
        }
        (AggregateFunction::Collect, AggregateTarget::All) => "collect(*)".to_string(),
        (
            _,
            AggregateTarget::Ranked { .. }
            | AggregateTarget::Column(_)
            | AggregateTarget::ColumnProperty { .. }
            | AggregateTarget::Expression(_),
        ) => {
            unreachable!("columns normalized before naming")
        }
    }
}
