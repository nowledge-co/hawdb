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

pub(super) enum PlannedReturns {
    Projections(Vec<Projection>),
    Aggregations {
        group_keys: Vec<Projection>,
        items: Vec<Aggregation>,
    },
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
        }
    }

    pub(super) fn into_logical(self, input: LogicalPlan) -> LogicalPlan {
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
        }
    }
}

pub(super) fn planned_sort_scope<'a>(
    input: &LogicalPlan,
    scope: &'a BTreeSet<String>,
) -> &'a BTreeSet<String> {
    if matches!(input, LogicalPlan::Aggregate { .. }) {
        static EMPTY: std::sync::OnceLock<BTreeSet<String>> = std::sync::OnceLock::new();
        EMPTY.get_or_init(BTreeSet::new)
    } else {
        scope
    }
}

pub(super) fn plan_set_node_properties_return_mode(
    update: &hawdb_cypher::MatchSet,
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
                if variable != &update.variable {
                    return Err(HawDBError::Semantic(format!(
                        "SET RETURN count variable '{variable}' does not match updated variable '{}'",
                        update.variable
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
    let scope = BTreeSet::from([update.variable.clone()]);
    let projections = returns
        .iter()
        .map(|item| plan_projection(&scope, item, parameters))
        .collect::<Result<Vec<_>>>()?;
    Ok(SetNodePropertiesReturnMode::Project(projections))
}

pub(super) fn plan_return_items(
    scope: &BTreeSet<String>,
    items: &[ReturnItem],
    parameters: &BTreeMap<String, Value>,
) -> Result<PlannedReturns> {
    plan_return_items_with_columns(scope, &BTreeSet::new(), items, parameters)
}

pub(super) fn plan_return_items_with_columns(
    scope: &BTreeSet<String>,
    column_scope: &BTreeSet<String>,
    items: &[ReturnItem],
    parameters: &BTreeMap<String, Value>,
) -> Result<PlannedReturns> {
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
                ReturnExpressionKind::Value(_) => {
                    group_keys.push(plan_projection_with_columns(
                        scope,
                        column_scope,
                        item,
                        parameters,
                    )?);
                }
                ReturnExpressionKind::Aggregate(_) => {
                    aggregations.push(plan_aggregation(scope, item)?);
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

pub(super) fn returns_are_count_only(items: &[ReturnItem]) -> bool {
    !items.is_empty()
        && items.iter().all(|item| {
            matches!(
                item.expression,
                AstNode {
                    kind: ReturnExpressionKind::Aggregate(AggregateExpression::CountAll),
                    ..
                } | AstNode {
                    kind: ReturnExpressionKind::Aggregate(
                        AggregateExpression::CountVariable { .. }
                    ),
                    ..
                } | AstNode {
                    kind: ReturnExpressionKind::Aggregate(
                        AggregateExpression::CountProperty { .. }
                    ),
                    ..
                }
            )
        })
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
    let AstNode {
        kind: ReturnExpressionKind::Value(value),
        ..
    } = &item.expression
    else {
        return Err(HawDBError::Semantic(
            "expected projection return item".to_string(),
        ));
    };
    let (expression, default_name) = match &value.kind {
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
        ScalarExpressionKind::Left { expression, length } => Ok(ProjectionExpression::Left {
            expression: Box::new(plan_scalar_expression_with_columns(
                scope,
                column_scope,
                expression,
                parameters,
            )?),
            length: bind_non_negative_usize(length, parameters, "LEFT length")?,
        }),
        ScalarExpressionKind::Lower(expression) => Ok(ProjectionExpression::Lower(Box::new(
            plan_scalar_expression_with_columns(scope, column_scope, expression, parameters)?,
        ))),
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
    }
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
        (AggregateFunction::Min, AggregateTarget::Property { variable, property }) => {
            format!("min({variable}.{property})")
        }
        (AggregateFunction::Min, AggregateTarget::All | AggregateTarget::Variable(_)) => {
            "min(?)".to_string()
        }
        (AggregateFunction::Max, AggregateTarget::Property { variable, property }) => {
            format!("max({variable}.{property})")
        }
        (AggregateFunction::Max, AggregateTarget::All | AggregateTarget::Variable(_)) => {
            "max(?)".to_string()
        }
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
    }
}
