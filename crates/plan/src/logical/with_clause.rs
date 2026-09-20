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

pub(super) fn plan_shortest_path_return(
    query: &ShortestPathReturn,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    if query.direction == RelationshipDirection::Incoming {
        return Err(HawDBError::Semantic(
            "ALL SHORTEST path reads do not support incoming-only patterns".to_string(),
        ));
    }
    if !query.source_properties.is_empty() || !query.target_properties.is_empty() {
        return Err(HawDBError::Semantic(
            "ALL SHORTEST path reads require endpoint ids in WHERE predicates".to_string(),
        ));
    }
    if query.min_hops == 0 || query.max_hops == 0 || query.min_hops > query.max_hops {
        return Err(HawDBError::Semantic(
            "ALL SHORTEST path reads require a finite positive hop range".to_string(),
        ));
    }
    if query.rel_variable.is_some() && !query.rel_type.is_empty() {
        return Err(HawDBError::Semantic(
            "ALL SHORTEST path reads do not bind relationship variables".to_string(),
        ));
    }
    let scope = BTreeSet::from([query.source_variable.clone(), query.target_variable.clone()]);
    if let Some(predicate) = &query.predicate {
        validate_predicate(&scope, predicate)?;
    }
    let source_id = endpoint_id_value(
        query.predicate.as_ref(),
        &query.source_variable,
        parameters,
        "source",
    )?;
    let target_id = endpoint_id_value(
        query.predicate.as_ref(),
        &query.target_variable,
        parameters,
        "target",
    )?;
    let returns = query
        .returns
        .iter()
        .map(|item| {
            let expression = match &item.expression {
                ShortestPathReturnExpression::NodePropertyList {
                    path_variable,
                    property,
                } => {
                    if path_variable != &query.path_variable {
                        return Err(HawDBError::Semantic(
                            "shortest path projection references an unknown path".to_string(),
                        ));
                    }
                    ShortestPathProjectionExpression::NodePropertyList {
                        property: property.clone(),
                    }
                }
                ShortestPathReturnExpression::Length { path_variable } => {
                    if path_variable != &query.path_variable {
                        return Err(HawDBError::Semantic(
                            "shortest path projection references an unknown path".to_string(),
                        ));
                    }
                    ShortestPathProjectionExpression::Length
                }
            };
            Ok(ShortestPathProjection {
                expression,
                name: item.alias.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::ShortestPath {
        source_variable: query.source_variable.clone(),
        source_label: query.source_label.clone(),
        source_id,
        source_visibility_predicate: None,
        rel_type: query.rel_type.clone(),
        direction: query.direction,
        target_variable: query.target_variable.clone(),
        target_label: query.target_label.clone(),
        target_id,
        target_visibility_predicate: None,
        min_hops: query.min_hops,
        max_hops: query.max_hops,
        returns,
    })
}

pub(super) fn endpoint_id_value(
    predicate: Option<&PropertyPredicate>,
    variable: &str,
    parameters: &BTreeMap<String, Value>,
    endpoint_name: &str,
) -> Result<Value> {
    let Some(predicate) = predicate else {
        return Err(HawDBError::Semantic(format!(
            "ALL SHORTEST path reads require {endpoint_name} id predicate"
        )));
    };
    find_endpoint_id_value(predicate, variable, parameters)?.ok_or_else(|| {
        HawDBError::Semantic(format!(
            "ALL SHORTEST path reads require {endpoint_name} id equality on '{variable}.id'"
        ))
    })
}

pub(super) fn find_endpoint_id_value(
    predicate: &PropertyPredicate,
    variable: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<Value>> {
    match predicate {
        PropertyPredicate::And(predicates) => {
            for predicate in predicates {
                if let Some(value) = find_endpoint_id_value(predicate, variable, parameters)? {
                    return Ok(Some(value));
                }
            }
            Ok(None)
        }
        PropertyPredicate::Eq {
            variable: predicate_variable,
            property,
            value,
        } if predicate_variable == variable && property == "id" => {
            Ok(Some(bind_value(value, parameters)?))
        }
        _ => Ok(None),
    }
}

pub(super) fn validate_collect_with_match_return(
    query: &MatchReturn,
    collect_with: &WithCollect,
) -> Result<()> {
    let Some(expand) = &query.expand else {
        return Err(HawDBError::Semantic(
            "WITH COLLECT is supported only after a relationship MATCH".to_string(),
        ));
    };
    if query.optional_expand.is_some() || query.optional_with.is_some() {
        return Err(HawDBError::Semantic(
            "WITH COLLECT cannot be combined with OPTIONAL MATCH".to_string(),
        ));
    }
    if collect_with.group_variable != query.variable {
        return Err(HawDBError::Semantic(
            "WITH COLLECT must group by the source variable".to_string(),
        ));
    }
    if collect_with.collect_variable != expand.target_variable {
        return Err(HawDBError::Semantic(
            "WITH COLLECT must collect from the relationship target variable".to_string(),
        ));
    }
    if query.distinct
        || !query.order_by.is_empty()
        || query.offset.is_some()
        || query.limit.is_some()
    {
        return Err(HawDBError::Semantic(
            "WITH COLLECT currently supports only a direct RETURN".to_string(),
        ));
    }
    if query.returns.len() != 2 {
        return Err(HawDBError::Semantic(
            "WITH COLLECT currently supports exactly two RETURN items".to_string(),
        ));
    }
    let group_item = &query.returns[0];
    let AstNode {
        kind:
            ReturnExpressionKind::Value(AstNode {
                kind: ScalarExpressionKind::Property { variable, .. },
                ..
            }),
        ..
    } = &group_item.expression
    else {
        return Err(HawDBError::Semantic(
            "WITH COLLECT RETURN must start with the grouped variable property".to_string(),
        ));
    };
    if variable != &collect_with.group_variable {
        return Err(HawDBError::Semantic(
            "WITH COLLECT RETURN group property must use the grouped variable".to_string(),
        ));
    }
    let alias_item = &query.returns[1];
    match &alias_item.expression.kind {
        ReturnExpressionKind::Value(AstNode {
            kind: ScalarExpressionKind::Variable(variable),
            ..
        }) if variable == &collect_with.alias => Ok(()),
        _ => Err(HawDBError::Semantic(
            "WITH COLLECT RETURN must include the collected alias".to_string(),
        )),
    }
}

pub(super) fn plan_collect_with_match_return(
    input: LogicalPlan,
    query: &MatchReturn,
    collect_with: &WithCollect,
) -> Result<LogicalPlan> {
    let group_item = &query.returns[0];
    let AstNode {
        kind:
            ReturnExpressionKind::Value(AstNode {
                kind: ScalarExpressionKind::Property { variable, property },
                ..
            }),
        ..
    } = &group_item.expression
    else {
        return Err(HawDBError::Semantic(
            "WITH COLLECT RETURN must start with the grouped variable property".to_string(),
        ));
    };
    Ok(LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: variable.clone(),
                property: property.clone(),
            },
            name: group_item
                .alias
                .clone()
                .unwrap_or_else(|| format!("{variable}.{property}")),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Collect,
            target: AggregateTarget::Property {
                variable: collect_with.collect_variable.clone(),
                property: collect_with.collect_property.clone(),
            },
            distinct: collect_with.distinct,
            name: collect_with.alias.clone(),
        }],
        input: Box::new(input),
    })
}

pub(super) fn validate_with_projection_match_return(
    query: &MatchReturn,
    scope: &BTreeSet<String>,
    with_projection: &WithProjection,
) -> Result<()> {
    if query.expand.is_some()
        || query.optional_expand.is_some()
        || query.optional_with.is_some()
        || query.collect_with.is_some()
        || query.distinct_with.is_some()
        || query.aggregate_with.is_some()
    {
        return Err(HawDBError::Semantic(
            "WITH projection currently supports only a direct node MATCH".to_string(),
        ));
    }
    if with_projection.items.len() < 2 {
        return Err(HawDBError::Semantic(
            "WITH projection requires the source variable and at least one alias".to_string(),
        ));
    }
    let AstNode {
        kind:
            ReturnExpressionKind::Value(AstNode {
                kind: ScalarExpressionKind::Variable(variable),
                ..
            }),
        ..
    } = &with_projection.items[0].expression
    else {
        return Err(HawDBError::Semantic(
            "WITH projection must start with the source variable".to_string(),
        ));
    };
    if variable != &query.variable || with_projection.items[0].alias.is_some() {
        return Err(HawDBError::Semantic(
            "WITH projection must preserve the source variable without alias".to_string(),
        ));
    }
    for item in &with_projection.items[1..] {
        if item.alias.is_none() {
            return Err(HawDBError::Semantic(
                "WITH projection expressions require aliases".to_string(),
            ));
        }
        if !return_expression_is_scoped(&item.expression, scope, &BTreeSet::new()) {
            return Err(HawDBError::Semantic(
                "WITH projection expression references an unknown variable".to_string(),
            ));
        }
    }
    validate_with_alias_filter(
        query.aggregate_with_filter.as_ref(),
        scope,
        &with_projection_column_names(with_projection),
    )
}

pub(super) fn plan_with_projection(
    input: LogicalPlan,
    scope: &BTreeSet<String>,
    with_projection: &WithProjection,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let projections = with_projection
        .items
        .iter()
        .map(|item| plan_projection(scope, item, parameters))
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::Project {
        items: projections,
        input: Box::new(input),
    })
}

pub(super) fn with_projection_column_names(with_projection: &WithProjection) -> BTreeSet<String> {
    with_projection
        .items
        .iter()
        .map(|item| match &item.alias {
            Some(alias) => alias.clone(),
            None => match &item.expression.kind {
                ReturnExpressionKind::Value(AstNode {
                    kind: ScalarExpressionKind::Variable(variable),
                    ..
                }) => variable.clone(),
                ReturnExpressionKind::Value(AstNode {
                    kind: ScalarExpressionKind::Property { variable, property },
                    ..
                }) => {
                    format!("{variable}.{property}")
                }
                _ => "expression".to_string(),
            },
        })
        .collect()
}

pub(super) fn validate_distinct_with_match_return(
    query: &MatchReturn,
    distinct_with: &WithDistinctProjection,
) -> Result<()> {
    if query.expand.is_none() {
        return Err(HawDBError::Semantic(
            "WITH DISTINCT is supported only after a relationship MATCH".to_string(),
        ));
    }
    if query.optional_expand.is_some()
        || query.optional_with.is_some()
        || query.collect_with.is_some()
    {
        return Err(HawDBError::Semantic(
            "WITH DISTINCT cannot be combined with OPTIONAL MATCH or COLLECT".to_string(),
        ));
    }
    if query.distinct
        || !query.order_by.is_empty()
        || query.offset.is_some()
        || query.limit.is_some()
    {
        return Err(HawDBError::Semantic(
            "WITH DISTINCT currently supports only a direct aggregate RETURN".to_string(),
        ));
    }
    if distinct_with.items.is_empty() {
        return Err(HawDBError::Semantic(
            "WITH DISTINCT requires at least one projected item".to_string(),
        ));
    }
    for item in &distinct_with.items {
        if item.alias.is_none() {
            return Err(HawDBError::Semantic(
                "WITH DISTINCT projection items require aliases".to_string(),
            ));
        }
        if !matches!(
            item.expression,
            AstNode {
                kind: ReturnExpressionKind::Value(AstNode {
                    kind: ScalarExpressionKind::Property { .. },
                    ..
                }),
                ..
            }
        ) {
            return Err(HawDBError::Semantic(
                "WITH DISTINCT currently supports only property projections".to_string(),
            ));
        }
    }
    if query.returns.len() != 1
        || !matches!(
            query.returns[0].expression,
            AstNode {
                kind: ReturnExpressionKind::Aggregate(AggregateExpression::CountAll),
                ..
            }
        )
    {
        return Err(HawDBError::Semantic(
            "WITH DISTINCT currently supports only RETURN count(*)".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn plan_distinct_with_match_return(
    input: LogicalPlan,
    scope: &BTreeSet<String>,
    query: &MatchReturn,
    distinct_with: &WithDistinctProjection,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let projections = distinct_with
        .items
        .iter()
        .map(|item| plan_projection(scope, item, parameters))
        .collect::<Result<Vec<_>>>()?;
    let distinct = LogicalPlan::Distinct {
        input: Box::new(LogicalPlan::Project {
            items: projections,
            input: Box::new(input),
        }),
    };
    Ok(LogicalPlan::Aggregate {
        group_keys: Vec::new(),
        items: vec![plan_aggregation(scope, &query.returns[0])?],
        input: Box::new(distinct),
    })
}

pub(super) fn validate_aggregate_with_match_return(
    query: &MatchReturn,
    aggregate_with: &WithAggregateProjection,
) -> Result<()> {
    if query.optional_expand.is_some()
        || query.collect_with.is_some()
        || query.distinct_with.is_some()
    {
        return Err(HawDBError::Semantic(
            "WITH aggregate cannot be combined with OPTIONAL MATCH, COLLECT, or DISTINCT"
                .to_string(),
        ));
    }
    if query.distinct {
        return Err(HawDBError::Semantic(
            "WITH aggregate currently supports direct RETURN with optional LIMIT".to_string(),
        ));
    }
    let has_aggregate = aggregate_with
        .items
        .iter()
        .any(|item| is_aggregate_return_expression(&item.expression));
    let has_group_key = aggregate_with
        .items
        .iter()
        .any(|item| !is_aggregate_return_expression(&item.expression));
    if !has_aggregate || !has_group_key {
        return Err(HawDBError::Semantic(
            "WITH aggregate requires grouped projections and aggregate items".to_string(),
        ));
    }
    let column_names = aggregate_with_column_names(aggregate_with);
    let post_lookup_scope = query
        .post_with_match
        .as_ref()
        .map(|lookup| BTreeSet::from([lookup.variable.clone()]))
        .unwrap_or_default();
    let has_post_aggregate = query
        .returns
        .iter()
        .any(|item| is_aggregate_return_expression(&item.expression));
    if has_post_aggregate
        && (query.post_with_match.is_some()
            || !query.order_by.is_empty()
            || query.returns.len() != 1
            || !matches!(
                query.returns[0].expression,
                AstNode {
                    kind: ReturnExpressionKind::Aggregate(AggregateExpression::CountAll),
                    ..
                }
            ))
    {
        return Err(HawDBError::Semantic(
            "WITH aggregate post-aggregation supports only one COUNT(*) projection without MATCH or ORDER BY"
                .to_string(),
        ));
    }
    for item in &query.returns {
        if is_aggregate_return_expression(&item.expression) {
            continue;
        }
        if let Some(lookup) = &query.post_with_match
            && matches!(&item.expression, AstNode { kind: ReturnExpressionKind::Value(AstNode { kind: ScalarExpressionKind::Variable(variable), .. }), .. } if variable == &lookup.variable)
        {
            return Err(HawDBError::Semantic(
                "post-WITH MATCH RETURN does not support whole lookup node projection".to_string(),
            ));
        }
        if !return_expression_is_scoped(&item.expression, &post_lookup_scope, &column_names) {
            return Err(HawDBError::Semantic(format!(
                "unknown WITH aggregate return expression {:?}",
                item.expression
            )));
        }
    }
    validate_with_alias_filter(
        query.aggregate_with_filter.as_ref(),
        &post_lookup_scope,
        &column_names,
    )?;
    if let Some(lookup) = &query.post_with_match
        && !column_names.contains(&lookup.column)
    {
        return Err(HawDBError::Semantic(format!(
            "unknown post-WITH MATCH lookup column '{}'",
            lookup.column
        )));
    }
    Ok(())
}

pub(super) fn optional_with_as_aggregate(
    optional_with: &hawdb_cypher::OptionalWithAggregate,
) -> WithAggregateProjection {
    WithAggregateProjection {
        items: vec![
            AstNode::synthetic(ReturnItemKind {
                expression: AstNode::synthetic(ReturnExpressionKind::Value(AstNode::synthetic(
                    ScalarExpressionKind::Variable(optional_with.group_variable.clone()),
                ))),
                alias: None,
            }),
            AstNode::synthetic(ReturnItemKind {
                expression: AstNode::synthetic(ReturnExpressionKind::Aggregate(
                    AggregateExpression::CountVariable {
                        variable: optional_with.count_variable.clone(),
                        distinct: optional_with.distinct,
                    },
                )),
                alias: Some(optional_with.alias.clone()),
            }),
        ],
    }
}

pub(super) fn validate_with_alias_filter(
    filter: Option<&WithAliasFilter>,
    scope: &BTreeSet<String>,
    column_names: &BTreeSet<String>,
) -> Result<()> {
    let Some(filter) = filter else {
        return Ok(());
    };
    validate_with_alias_filter_node(filter, scope, column_names)
}

pub(super) fn validate_with_alias_filter_node(
    filter: &WithAliasFilter,
    scope: &BTreeSet<String>,
    column_names: &BTreeSet<String>,
) -> Result<()> {
    match filter {
        WithAliasFilter::And(filters) | WithAliasFilter::Or(filters) => {
            for filter in filters {
                validate_with_alias_filter_node(filter, scope, column_names)?;
            }
            Ok(())
        }
        WithAliasFilter::Comparison { left, right, .. } => {
            validate_with_alias_filter_expression(left, scope, column_names)?;
            validate_with_alias_filter_expression(right, scope, column_names)
        }
    }
}

pub(super) fn validate_with_alias_filter_expression(
    expression: &WithAliasFilterExpression,
    scope: &BTreeSet<String>,
    column_names: &BTreeSet<String>,
) -> Result<()> {
    match expression {
        WithAliasFilterExpression::Column(column) => {
            if column_names.contains(column) {
                Ok(())
            } else {
                Err(HawDBError::Semantic(format!(
                    "unknown WITH filter column '{column}'"
                )))
            }
        }
        WithAliasFilterExpression::Property { variable, .. } => {
            if scope.contains(variable) || column_names.contains(variable) {
                Ok(())
            } else {
                Err(HawDBError::Semantic(format!(
                    "unknown WITH filter variable '{variable}'"
                )))
            }
        }
        WithAliasFilterExpression::Value(_) => Ok(()),
    }
}

pub(super) fn return_expression_is_scoped(
    expression: &ReturnExpression,
    scope: &BTreeSet<String>,
    column_names: &BTreeSet<String>,
) -> bool {
    match &expression.kind {
        ReturnExpressionKind::Value(expression) => {
            scalar_expression_is_scoped(expression, scope, column_names)
        }
        ReturnExpressionKind::Arithmetic { first, rest } => {
            return_expression_is_scoped(first, scope, column_names)
                && rest.iter().all(|(_, expression)| {
                    return_expression_is_scoped(expression, scope, column_names)
                })
        }
        ReturnExpressionKind::Path(_) | ReturnExpressionKind::Aggregate(_) => false,
    }
}

pub(super) fn scalar_expression_is_scoped(
    expression: &ScalarExpression,
    scope: &BTreeSet<String>,
    column_names: &BTreeSet<String>,
) -> bool {
    match &expression.kind {
        ScalarExpressionKind::Variable(variable)
        | ScalarExpressionKind::Property { variable, .. }
        | ScalarExpressionKind::DefaultIfNullOrEq { variable, .. }
        | ScalarExpressionKind::DefaultIfNull { variable, .. }
        | ScalarExpressionKind::CasePropertyNotNullOrEq { variable, .. }
        | ScalarExpressionKind::CasePropertyEqualsRank { variable, .. }
        | ScalarExpressionKind::CaseLowerPropertyDefault { variable, .. }
        | ScalarExpressionKind::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            column_names.contains(variable) || scope.contains(variable)
        }
        ScalarExpressionKind::Case { .. }
        | ScalarExpressionKind::Binary { .. }
        | ScalarExpressionKind::Not(_)
        | ScalarExpressionKind::IsNull { .. } => expression
            .kind
            .all_children(|child| scalar_expression_is_scoped(child, scope, column_names)),
        ScalarExpressionKind::Value(_) => true,
        ScalarExpressionKind::Coalesce(expressions) => expressions
            .iter()
            .all(|expression| scalar_expression_is_scoped(expression, scope, column_names)),
        ScalarExpressionKind::Left { expression, .. } | ScalarExpressionKind::Lower(expression) => {
            scalar_expression_is_scoped(expression, scope, column_names)
        }
        ScalarExpressionKind::Id(_)
        | ScalarExpressionKind::RelationshipType(_)
        | ScalarExpressionKind::DatePart { .. } => false,
    }
}

pub(super) fn plan_aggregate_with_match_return(
    mut input: LogicalPlan,
    scope: &BTreeSet<String>,
    query: &MatchReturn,
    aggregate_with: &WithAggregateProjection,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let planned_with = plan_return_items(scope, &aggregate_with.items, parameters)?;
    let column_names = planned_with.names().into_iter().collect::<BTreeSet<_>>();
    input = planned_with.into_logical(input);
    if let Some(filter) = &query.aggregate_with_filter {
        input = LogicalPlan::Filter {
            predicate: plan_with_alias_filter(filter, parameters)?,
            input: Box::new(input),
        };
    }
    if query
        .returns
        .iter()
        .any(|item| is_aggregate_return_expression(&item.expression))
    {
        input = LogicalPlan::Aggregate {
            group_keys: Vec::new(),
            items: query
                .returns
                .iter()
                .map(|item| plan_aggregation(&BTreeSet::new(), item))
                .collect::<Result<Vec<_>>>()?,
            input: Box::new(input),
        };
        let offset = query
            .offset
            .as_ref()
            .map(|offset| bind_pagination_value(offset, parameters, "offset"))
            .transpose()?
            .unwrap_or(0);
        let limit = query
            .limit
            .as_ref()
            .map(|limit| bind_pagination_value(limit, parameters, "limit"))
            .transpose()?;
        if offset > 0 || limit.is_some() {
            input = LogicalPlan::Limit {
                offset,
                limit,
                input: Box::new(input),
            };
        }
        return Ok(input);
    }
    if !query.order_by.is_empty() {
        input = LogicalPlan::Sort {
            items: plan_sort_items(&BTreeSet::new(), &column_names, &query.order_by, parameters)?,
            input: Box::new(input),
        };
    }
    let offset = query
        .offset
        .as_ref()
        .map(|offset| bind_pagination_value(offset, parameters, "offset"))
        .transpose()?
        .unwrap_or(0);
    let limit = query
        .limit
        .as_ref()
        .map(|limit| bind_pagination_value(limit, parameters, "limit"))
        .transpose()?;
    if let Some(lookup) = &query.post_with_match {
        if offset > 0 || limit.is_some() {
            input = LogicalPlan::Limit {
                offset,
                limit,
                input: Box::new(input),
            };
        }
        input = plan_post_with_node_lookup(input, lookup);
        let lookup_scope = BTreeSet::from([lookup.variable.clone()]);
        let projections = query
            .returns
            .iter()
            .map(|item| {
                plan_projection_with_columns(&lookup_scope, &column_names, item, parameters)
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(LogicalPlan::Project {
            items: projections,
            input: Box::new(input),
        });
    }
    let projections = query
        .returns
        .iter()
        .map(|item| plan_projection_with_columns(&BTreeSet::new(), &column_names, item, parameters))
        .collect::<Result<Vec<_>>>()?;
    input = LogicalPlan::Project {
        items: projections,
        input: Box::new(input),
    };
    if offset > 0 || limit.is_some() {
        input = LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(input),
        };
    }
    Ok(input)
}

pub(super) fn plan_post_with_node_lookup(
    input: LogicalPlan,
    lookup: &PostWithNodeLookup,
) -> LogicalPlan {
    LogicalPlan::NodeColumnLookup {
        variable: lookup.variable.clone(),
        label: lookup.label.clone(),
        property: lookup.property.clone(),
        column: lookup.column.clone(),
        optional: lookup.optional,
        input: Box::new(input),
    }
}

pub(super) fn optional_direct_count_alias(
    query: &MatchReturn,
    optional: &hawdb_cypher::OptionalRelationshipExpand,
) -> Result<Option<String>> {
    let mut count_alias = None;
    let mut has_projection = false;
    for item in &query.returns {
        match &item.expression.kind {
            ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
                variable,
                distinct,
            }) => {
                if *distinct || count_alias.is_some() {
                    return Ok(None);
                }
                let count_matches_relationship =
                    optional.expand.variable.as_deref() == Some(variable);
                let count_matches_target = optional.expand.target_variable == *variable;
                if !count_matches_relationship && !count_matches_target {
                    return Ok(None);
                }
                count_alias = Some(
                    item.alias
                        .clone()
                        .unwrap_or_else(|| format!("count({variable})")),
                );
            }
            ReturnExpressionKind::Value(expression) => {
                if !scalar_expression_is_source_only(expression, &optional.source_variable) {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpressionKind::Aggregate(_)
            | ReturnExpressionKind::Arithmetic { .. }
            | ReturnExpressionKind::Path(_) => return Ok(None),
        }
    }
    Ok(if has_projection { count_alias } else { None })
}

pub(super) fn optional_direct_collect_alias(
    query: &MatchReturn,
    optional: &hawdb_cypher::OptionalRelationshipExpand,
) -> Result<Option<String>> {
    let mut collect_alias = None;
    let mut has_projection = false;
    for item in &query.returns {
        match &item.expression.kind {
            ReturnExpressionKind::Aggregate(AggregateExpression::CollectProperty {
                variable,
                property,
                distinct,
            }) => {
                if collect_alias.is_some() || variable != &optional.expand.target_variable {
                    return Ok(None);
                }
                collect_alias = Some(item.alias.clone().unwrap_or_else(|| {
                    if *distinct {
                        format!("collect(DISTINCT {variable}.{property})")
                    } else {
                        format!("collect({variable}.{property})")
                    }
                }));
            }
            ReturnExpressionKind::Value(expression) => {
                if !scalar_expression_is_source_only(expression, &optional.source_variable) {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpressionKind::Aggregate(_)
            | ReturnExpressionKind::Arithmetic { .. }
            | ReturnExpressionKind::Path(_) => return Ok(None),
        }
    }
    Ok(if has_projection { collect_alias } else { None })
}

pub(super) fn optional_direct_row_projection(
    query: &MatchReturn,
    optional: &hawdb_cypher::OptionalRelationshipExpand,
) -> bool {
    query.returns.iter().all(|item| {
        optional_direct_row_projection_expression(
            &item.expression,
            &optional.source_variable,
            &optional.expand.target_variable,
            optional.expand.variable.as_deref(),
        )
    })
}

pub(super) fn optional_direct_row_projection_expression(
    expression: &ReturnExpression,
    source_variable: &str,
    target_variable: &str,
    rel_variable: Option<&str>,
) -> bool {
    match &expression.kind {
        ReturnExpressionKind::Value(expression) => optional_direct_row_projection_value_expression(
            expression,
            source_variable,
            target_variable,
            rel_variable,
        ),
        ReturnExpressionKind::Aggregate(_)
        | ReturnExpressionKind::Arithmetic { .. }
        | ReturnExpressionKind::Path(_) => false,
    }
}

pub(super) fn optional_direct_row_projection_value_expression(
    expression: &ScalarExpression,
    source_variable: &str,
    target_variable: &str,
    rel_variable: Option<&str>,
) -> bool {
    match &expression.kind {
        ScalarExpressionKind::Variable(variable)
        | ScalarExpressionKind::Property { variable, .. }
        | ScalarExpressionKind::Id(variable)
        | ScalarExpressionKind::RelationshipType(variable)
        | ScalarExpressionKind::DatePart { variable, .. }
        | ScalarExpressionKind::DefaultIfNullOrEq { variable, .. }
        | ScalarExpressionKind::DefaultIfNull { variable, .. }
        | ScalarExpressionKind::CasePropertyNotNullOrEq { variable, .. }
        | ScalarExpressionKind::CasePropertyEqualsRank { variable, .. }
        | ScalarExpressionKind::CaseLowerPropertyDefault { variable, .. }
        | ScalarExpressionKind::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            optional_direct_row_projection_variable(
                variable,
                source_variable,
                target_variable,
                rel_variable,
            )
        }
        ScalarExpressionKind::Case { .. }
        | ScalarExpressionKind::Binary { .. }
        | ScalarExpressionKind::Not(_)
        | ScalarExpressionKind::IsNull { .. } => expression.kind.all_children(|child| {
            optional_direct_row_projection_value_expression(
                child,
                source_variable,
                target_variable,
                rel_variable,
            )
        }),
        ScalarExpressionKind::Value(_) => true,
        ScalarExpressionKind::Coalesce(expressions) => expressions.iter().all(|expression| {
            optional_direct_row_projection_value_expression(
                expression,
                source_variable,
                target_variable,
                rel_variable,
            )
        }),
        ScalarExpressionKind::Left { expression, .. } | ScalarExpressionKind::Lower(expression) => {
            optional_direct_row_projection_value_expression(
                expression,
                source_variable,
                target_variable,
                rel_variable,
            )
        }
    }
}

pub(super) fn optional_direct_row_projection_variable(
    variable: &str,
    source_variable: &str,
    target_variable: &str,
    rel_variable: Option<&str>,
) -> bool {
    variable == source_variable || variable == target_variable || rel_variable == Some(variable)
}

pub(super) fn plan_optional_direct_count_return(
    mut input: LogicalPlan,
    scope: &BTreeSet<String>,
    query: &MatchReturn,
    optional: &hawdb_cypher::OptionalRelationshipExpand,
    count_alias: String,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    input = LogicalPlan::OptionalDegree {
        source_variable: optional.source_variable.clone(),
        rel_type: optional.expand.rel_type.clone(),
        rel_properties: bind_properties(&optional.expand.properties, parameters)?,
        direction: optional.expand.direction,
        target_label: optional.expand.target_label.clone(),
        target_properties: bind_properties(&optional.expand.target_properties, parameters)?,
        alias: count_alias.clone(),
        input: Box::new(input),
    };
    let projections = query
        .returns
        .iter()
        .map(|item| plan_optional_direct_count_projection(scope, item, &count_alias, parameters))
        .collect::<Result<Vec<_>>>()?;
    let projection_names = projections
        .iter()
        .map(|projection| projection.name.clone())
        .collect::<BTreeSet<_>>();
    input = LogicalPlan::Project {
        items: projections,
        input: Box::new(input),
    };
    if query.distinct {
        input = LogicalPlan::Distinct {
            input: Box::new(input),
        };
    }
    if !query.order_by.is_empty() {
        input = LogicalPlan::Sort {
            items: plan_sort_items(scope, &projection_names, &query.order_by, parameters)?,
            input: Box::new(input),
        };
    }
    let offset = query
        .offset
        .as_ref()
        .map(|offset| bind_pagination_value(offset, parameters, "offset"))
        .transpose()?
        .unwrap_or(0);
    let limit = query
        .limit
        .as_ref()
        .map(|limit| bind_pagination_value(limit, parameters, "limit"))
        .transpose()?;
    if offset > 0 || limit.is_some() {
        input = LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(input),
        };
    }
    Ok(input)
}

pub(super) fn plan_optional_direct_count_projection(
    scope: &BTreeSet<String>,
    item: &ReturnItem,
    count_alias: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<Projection> {
    if matches!(
        item.expression,
        AstNode {
            kind: ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable { .. }),
            ..
        }
    ) {
        return Ok(Projection {
            expression: ProjectionExpression::Column(count_alias.to_string()),
            name: item
                .alias
                .clone()
                .unwrap_or_else(|| count_alias.to_string()),
        });
    }
    plan_projection_with_columns(scope, &BTreeSet::new(), item, parameters)
}

pub(super) fn scalar_expressions_are_source_only(
    expressions: &[ScalarExpression],
    source_variable: &str,
) -> bool {
    expressions
        .iter()
        .all(|expression| scalar_expression_is_source_only(expression, source_variable))
}

pub(super) fn scalar_expression_is_source_only(
    expression: &ScalarExpression,
    source_variable: &str,
) -> bool {
    match &expression.kind {
        ScalarExpressionKind::Variable(variable)
        | ScalarExpressionKind::Property { variable, .. }
        | ScalarExpressionKind::Id(variable)
        | ScalarExpressionKind::RelationshipType(variable)
        | ScalarExpressionKind::DatePart { variable, .. }
        | ScalarExpressionKind::DefaultIfNullOrEq { variable, .. }
        | ScalarExpressionKind::DefaultIfNull { variable, .. }
        | ScalarExpressionKind::CasePropertyNotNullOrEq { variable, .. }
        | ScalarExpressionKind::CasePropertyEqualsRank { variable, .. }
        | ScalarExpressionKind::CaseLowerPropertyDefault { variable, .. }
        | ScalarExpressionKind::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            variable == source_variable
        }
        ScalarExpressionKind::Case { .. }
        | ScalarExpressionKind::Binary { .. }
        | ScalarExpressionKind::Not(_)
        | ScalarExpressionKind::IsNull { .. } => expression
            .kind
            .all_children(|child| scalar_expression_is_source_only(child, source_variable)),
        ScalarExpressionKind::Value(_) => true,
        ScalarExpressionKind::Coalesce(expressions) => {
            scalar_expressions_are_source_only(expressions, source_variable)
        }
        ScalarExpressionKind::Left { expression, .. } | ScalarExpressionKind::Lower(expression) => {
            scalar_expression_is_source_only(expression, source_variable)
        }
    }
}

pub(super) fn aggregate_with_column_names(
    aggregate_with: &WithAggregateProjection,
) -> BTreeSet<String> {
    aggregate_with
        .items
        .iter()
        .map(|item| {
            item.alias
                .clone()
                .unwrap_or_else(|| match &item.expression.kind {
                    ReturnExpressionKind::Value(AstNode {
                        kind: ScalarExpressionKind::Variable(variable),
                        ..
                    }) => variable.clone(),
                    ReturnExpressionKind::Value(AstNode {
                        kind: ScalarExpressionKind::Property { variable, property },
                        ..
                    }) => {
                        format!("{variable}.{property}")
                    }
                    ReturnExpressionKind::Value(AstNode {
                        kind: ScalarExpressionKind::Value(_),
                        ..
                    }) => "literal".to_string(),
                    ReturnExpressionKind::Value(AstNode {
                        kind:
                            ScalarExpressionKind::DatePart {
                                part,
                                variable,
                                property,
                            },
                        ..
                    }) => format!("date_part({part}, {variable}.{property})"),
                    ReturnExpressionKind::Aggregate(AggregateExpression::CountAll) => {
                        "count(*)".to_string()
                    }
                    ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
                        variable,
                        distinct,
                    }) if *distinct => {
                        format!("count(DISTINCT {variable})")
                    }
                    ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
                        variable,
                        ..
                    }) => {
                        format!("count({variable})")
                    }
                    ReturnExpressionKind::Aggregate(AggregateExpression::CountProperty {
                        variable,
                        property,
                        distinct,
                    }) if *distinct => format!("count(DISTINCT {variable}.{property})"),
                    ReturnExpressionKind::Aggregate(AggregateExpression::CountProperty {
                        variable,
                        property,
                        ..
                    }) => format!("count({variable}.{property})"),
                    ReturnExpressionKind::Aggregate(AggregateExpression::CollectVariable {
                        variable,
                        distinct,
                    }) if *distinct => {
                        format!("collect(DISTINCT {variable})")
                    }
                    ReturnExpressionKind::Aggregate(AggregateExpression::CollectVariable {
                        variable,
                        ..
                    }) => {
                        format!("collect({variable})")
                    }
                    ReturnExpressionKind::Aggregate(AggregateExpression::CollectProperty {
                        variable,
                        property,
                        distinct,
                    }) if *distinct => format!("collect(DISTINCT {variable}.{property})"),
                    ReturnExpressionKind::Aggregate(AggregateExpression::CollectProperty {
                        variable,
                        property,
                        ..
                    }) => format!("collect({variable}.{property})"),
                    _ => String::new(),
                })
        })
        .collect()
}

pub(super) fn is_aggregate_return_expression(expression: &ReturnExpression) -> bool {
    matches!(
        expression,
        AstNode {
            kind: ReturnExpressionKind::Aggregate(_),
            ..
        }
    )
}

pub(super) fn plan_with_alias_filter(
    filter: &WithAliasFilter,
    parameters: &BTreeMap<String, Value>,
) -> Result<Predicate> {
    Ok(match filter {
        WithAliasFilter::And(filters) => Predicate::And(
            filters
                .iter()
                .map(|filter| plan_with_alias_filter(filter, parameters))
                .collect::<Result<Vec<_>>>()?,
        ),
        WithAliasFilter::Or(filters) => Predicate::Or(
            filters
                .iter()
                .map(|filter| plan_with_alias_filter(filter, parameters))
                .collect::<Result<Vec<_>>>()?,
        ),
        WithAliasFilter::Comparison { left, op, right } => {
            let expression = plan_with_alias_filter_expression(left, parameters)?;
            let value = plan_with_alias_filter_expression(right, parameters)?;
            match op {
                WithAliasFilterOp::Eq => Predicate::ExpressionEq { expression, value },
                WithAliasFilterOp::Ne => Predicate::ExpressionNotEq { expression, value },
                WithAliasFilterOp::Lt => Predicate::ExpressionCompare {
                    expression,
                    op: ComparisonOp::Lt,
                    value,
                },
                WithAliasFilterOp::Lte => Predicate::ExpressionCompare {
                    expression,
                    op: ComparisonOp::Lte,
                    value,
                },
                WithAliasFilterOp::Gt => Predicate::ExpressionCompare {
                    expression,
                    op: ComparisonOp::Gt,
                    value,
                },
                WithAliasFilterOp::Gte => Predicate::ExpressionCompare {
                    expression,
                    op: ComparisonOp::Gte,
                    value,
                },
                WithAliasFilterOp::Contains => Predicate::ExpressionContains { expression, value },
            }
        }
    })
}

pub(super) fn plan_with_alias_filter_expression(
    expression: &WithAliasFilterExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<ProjectionExpression> {
    Ok(match expression {
        WithAliasFilterExpression::Column(column) => ProjectionExpression::Column(column.clone()),
        WithAliasFilterExpression::Property { variable, property } => {
            ProjectionExpression::Property {
                variable: variable.clone(),
                property: property.clone(),
            }
        }
        WithAliasFilterExpression::Value(value) => {
            ProjectionExpression::Literal(bind_value(value, parameters)?)
        }
    })
}
