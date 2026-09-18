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

use super::stages::LOGICAL_REWRITE_STAGE;
use crate::{RuleEvent, StageStats, StageTrace};
use hawdb_core::Value;
use hawdb_plan::{LogicalPlan, Predicate, Projection, ProjectionExpression};
use std::collections::{BTreeMap, BTreeSet};

const MAX_FIXED_POINT_PASSES: usize = 16;

pub(super) struct LogicalRewriteOutput {
    plan: LogicalPlan,
    events: Vec<RuleEvent>,
    trace: StageTrace,
    warning: Option<String>,
}

impl LogicalRewriteOutput {
    pub(super) fn plan(&self) -> &LogicalPlan {
        &self.plan
    }

    pub(super) fn events(&self) -> &[RuleEvent] {
        &self.events
    }

    pub(super) fn trace(&self) -> &StageTrace {
        &self.trace
    }

    pub(super) fn warning(&self) -> Option<&str> {
        self.warning.as_deref()
    }
}

pub(super) fn rewrite_logical_plan(plan: &LogicalPlan) -> LogicalRewriteOutput {
    rewrite_logical_plan_with_pass_limit(plan, MAX_FIXED_POINT_PASSES)
}

fn rewrite_logical_plan_with_pass_limit(
    plan: &LogicalPlan,
    max_passes: usize,
) -> LogicalRewriteOutput {
    assert!(
        max_passes > 0,
        "logical rewrite pass limit must be non-zero"
    );
    let input_count = logical_node_count(plan);
    let mut current = plan.clone();
    let mut events = Vec::new();

    for pass in 1..=max_passes {
        let mut pass_events = Vec::new();
        let next = rewrite_bottom_up(current.clone(), &mut pass_events);
        if next == current {
            let applied_rules = events.len();
            return LogicalRewriteOutput {
                trace: LOGICAL_REWRITE_STAGE.trace(
                    StageStats::new(input_count, logical_node_count(&current))
                        .with_rule_counts(applied_rules, 0),
                ),
                plan: current,
                events,
                warning: None,
            };
        }
        events.extend(pass_events);
        current = next;

        if pass == max_passes {
            let warning = format!(
                "fixed_point_not_reached: logical rewrite still changed after {max_passes} passes; retained the last-pass logical plan"
            );
            let applied_rules = events.len();
            events.push(RuleEvent::skipped(
                "transformation:logical_rewrite_fixed_point",
                warning.clone(),
            ));
            return LogicalRewriteOutput {
                trace: LOGICAL_REWRITE_STAGE.trace(
                    StageStats::new(input_count, logical_node_count(&current))
                        .with_rule_counts(applied_rules, 1),
                ),
                plan: current,
                events,
                warning: Some(warning),
            };
        }
    }

    unreachable!("fixed-point loop always returns")
}

fn rewrite_bottom_up(plan: LogicalPlan, events: &mut Vec<RuleEvent>) -> LogicalPlan {
    let plan = match plan {
        LogicalPlan::NodeCartesianProduct { left, right } => LogicalPlan::NodeCartesianProduct {
            left: Box::new(rewrite_bottom_up(*left, events)),
            right: Box::new(rewrite_bottom_up(*right, events)),
        },
        LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Expand {
            source_variable,
            source_label,
            rel_variable,
            rel_type,
            rel_properties,
            direction,
            target_variable,
            target_label,
            min_hops,
            max_hops,
            optional,
            input,
        } => LogicalPlan::Expand {
            source_variable,
            source_label,
            rel_variable,
            rel_type,
            rel_properties,
            direction,
            target_variable,
            target_label,
            min_hops,
            max_hops,
            optional,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Project { items, input } => LogicalPlan::Project {
            items,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => LogicalPlan::Aggregate {
            group_keys,
            items,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Sort { items, input } => LogicalPlan::Sort {
            items,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(rewrite_bottom_up(*input, events)),
        },
        leaf => leaf,
    };

    rewrite_local(plan, events)
}

fn rewrite_local(plan: LogicalPlan, events: &mut Vec<RuleEvent>) -> LogicalPlan {
    if let Some(empty) = propagate_empty_input(&plan) {
        record(events, "propagate_empty_input");
        return empty;
    }
    match plan {
        LogicalPlan::Filter { predicate, input } => rewrite_filter(predicate, *input, events),
        LogicalPlan::Project { items, input } => rewrite_project(items, *input, events),
        LogicalPlan::Distinct { input } => match *input {
            LogicalPlan::Distinct { input } => {
                record(events, "remove_redundant_distinct");
                LogicalPlan::Distinct { input }
            }
            input => LogicalPlan::Distinct {
                input: Box::new(input),
            },
        },
        LogicalPlan::Sort { items, input } => match *input {
            LogicalPlan::Sort {
                items: inner_items,
                input,
            } if items == inner_items => {
                record(events, "remove_redundant_sort");
                LogicalPlan::Sort { items, input }
            }
            input => LogicalPlan::Sort {
                items,
                input: Box::new(input),
            },
        },
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => rewrite_limit(offset, limit, *input, events),
        plan => plan,
    }
}

fn propagate_empty_input(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let empty = match plan {
        LogicalPlan::NodeCartesianProduct { left, right } => [left.as_ref(), right.as_ref()]
            .into_iter()
            .find(|plan| is_empty_limit(plan))?,
        LogicalPlan::NodeColumnLookup { input, .. }
        | LogicalPlan::Expand { input, .. }
        | LogicalPlan::OptionalDegree { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
            if is_empty_limit(input) =>
        {
            input
        }
        LogicalPlan::Aggregate {
            group_keys, input, ..
        } if !group_keys.is_empty() && is_empty_limit(input) => input,
        _ => return None,
    };
    Some(empty.clone())
}

fn is_empty_limit(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Limit { limit: Some(0), .. })
}

fn rewrite_project(
    items: Vec<Projection>,
    input: LogicalPlan,
    events: &mut Vec<RuleEvent>,
) -> LogicalPlan {
    match input {
        LogicalPlan::Project {
            items: inner_items,
            input,
        } => {
            let Some(composed_items) = compose_projection_items(&items, &inner_items) else {
                return LogicalPlan::Project {
                    items,
                    input: Box::new(LogicalPlan::Project {
                        items: inner_items,
                        input,
                    }),
                };
            };
            record(events, "collapse_adjacent_projects");
            LogicalPlan::Project {
                items: composed_items,
                input,
            }
        }
        LogicalPlan::Aggregate {
            group_keys,
            items: aggregate_items,
            input,
        } => {
            let Some(required_columns) = projection_column_dependencies(&items) else {
                return LogicalPlan::Project {
                    items,
                    input: Box::new(LogicalPlan::Aggregate {
                        group_keys,
                        items: aggregate_items,
                        input,
                    }),
                };
            };
            let output_names = group_keys
                .iter()
                .map(|item| item.name.as_str())
                .chain(aggregate_items.iter().map(|item| item.name.as_str()))
                .collect::<Vec<_>>();
            if output_names.iter().copied().collect::<BTreeSet<_>>().len() != output_names.len() {
                return LogicalPlan::Project {
                    items,
                    input: Box::new(LogicalPlan::Aggregate {
                        group_keys,
                        items: aggregate_items,
                        input,
                    }),
                };
            }
            let original_len = aggregate_items.len();
            let aggregate_items = aggregate_items
                .into_iter()
                .filter(|item| required_columns.contains(&item.name))
                .collect::<Vec<_>>();
            if aggregate_items.len() != original_len {
                record(events, "prune_unused_aggregates");
            }
            LogicalPlan::Project {
                items,
                input: Box::new(LogicalPlan::Aggregate {
                    group_keys,
                    items: aggregate_items,
                    input,
                }),
            }
        }
        input => LogicalPlan::Project {
            items,
            input: Box::new(input),
        },
    }
}

fn compose_projection_items(
    outer_items: &[Projection],
    inner_items: &[Projection],
) -> Option<Vec<Projection>> {
    let mut inner_by_name = BTreeMap::new();
    for item in inner_items {
        if inner_by_name
            .insert(item.name.as_str(), &item.expression)
            .is_some()
        {
            return None;
        }
    }
    outer_items
        .iter()
        .cloned()
        .map(|mut item| {
            item.expression = match item.expression {
                ProjectionExpression::Column(column) => {
                    inner_by_name.get(column.as_str()).copied()?.clone()
                }
                expression if !projection_expression_references_column(&expression) => expression,
                _ => return None,
            };
            Some(item)
        })
        .collect()
}

fn projection_column_dependencies(items: &[Projection]) -> Option<BTreeSet<String>> {
    let mut columns = BTreeSet::new();
    for item in items {
        if !collect_projection_columns(&item.expression, &mut columns) {
            return None;
        }
    }
    Some(columns)
}

fn collect_projection_columns(
    expression: &ProjectionExpression,
    columns: &mut BTreeSet<String>,
) -> bool {
    match expression {
        ProjectionExpression::Case { .. }
        | ProjectionExpression::Binary { .. }
        | ProjectionExpression::Not(_)
        | ProjectionExpression::IsNull { .. } => {
            expression.all_children(|child| collect_projection_columns(child, columns))
        }
        ProjectionExpression::Column(column) => {
            columns.insert(column.clone());
            true
        }
        ProjectionExpression::Literal(_) => true,
        ProjectionExpression::Coalesce(expressions) => expressions
            .iter()
            .all(|expression| collect_projection_columns(expression, columns)),
        ProjectionExpression::Left { expression, .. } | ProjectionExpression::Lower(expression) => {
            collect_projection_columns(expression, columns)
        }
        ProjectionExpression::ColumnDefaultIfNullOrEq { column, .. }
        | ProjectionExpression::ColumnValueDefaultIfNull { column, .. }
        | ProjectionExpression::ColumnValueCasePropertyNotNullOrEq { column, .. }
        | ProjectionExpression::ColumnProperty { column, .. } => {
            columns.insert(column.clone());
            true
        }
        ProjectionExpression::CaseColumnSearchRank(rank) => {
            columns.insert(rank.column.clone());
            true
        }
        ProjectionExpression::Variable { .. }
        | ProjectionExpression::Property { .. }
        | ProjectionExpression::Id { .. }
        | ProjectionExpression::RelationshipType { .. }
        | ProjectionExpression::DatePart { .. }
        | ProjectionExpression::DefaultIfNullOrEq { .. }
        | ProjectionExpression::DefaultIfNull { .. }
        | ProjectionExpression::CasePropertyNotNullOrEq { .. }
        | ProjectionExpression::CasePropertyEqualsRank { .. }
        | ProjectionExpression::CaseLowerPropertyDefault { .. }
        | ProjectionExpression::CaseCoalesceDifferenceFloorZero { .. }
        | ProjectionExpression::CaseEntitySearchRank(_) => false,
    }
}

fn projection_expression_references_column(expression: &ProjectionExpression) -> bool {
    match expression {
        ProjectionExpression::Case { .. }
        | ProjectionExpression::Binary { .. }
        | ProjectionExpression::Not(_)
        | ProjectionExpression::IsNull { .. } => {
            !expression.all_children(|child| !projection_expression_references_column(child))
        }
        ProjectionExpression::Column(_)
        | ProjectionExpression::ColumnDefaultIfNullOrEq { .. }
        | ProjectionExpression::ColumnValueDefaultIfNull { .. }
        | ProjectionExpression::ColumnValueCasePropertyNotNullOrEq { .. }
        | ProjectionExpression::ColumnProperty { .. }
        | ProjectionExpression::CaseColumnSearchRank(_) => true,
        ProjectionExpression::Coalesce(expressions) => expressions
            .iter()
            .any(projection_expression_references_column),
        ProjectionExpression::Left { expression, .. } | ProjectionExpression::Lower(expression) => {
            projection_expression_references_column(expression)
        }
        ProjectionExpression::Variable { .. }
        | ProjectionExpression::Property { .. }
        | ProjectionExpression::Id { .. }
        | ProjectionExpression::RelationshipType { .. }
        | ProjectionExpression::Literal(_)
        | ProjectionExpression::DatePart { .. }
        | ProjectionExpression::DefaultIfNullOrEq { .. }
        | ProjectionExpression::DefaultIfNull { .. }
        | ProjectionExpression::CasePropertyNotNullOrEq { .. }
        | ProjectionExpression::CasePropertyEqualsRank { .. }
        | ProjectionExpression::CaseLowerPropertyDefault { .. }
        | ProjectionExpression::CaseCoalesceDifferenceFloorZero { .. }
        | ProjectionExpression::CaseEntitySearchRank(_) => false,
    }
}

fn rewrite_filter(
    predicate: Predicate,
    input: LogicalPlan,
    events: &mut Vec<RuleEvent>,
) -> LogicalPlan {
    let simplified = simplify_predicate(predicate.clone());
    if simplified != predicate {
        record(events, "simplify_filter_predicate");
    }
    let predicate = simplified;
    match predicate {
        Predicate::ConstantBool(true) => {
            record(events, "remove_true_filter");
            input
        }
        Predicate::ConstantBool(false) => {
            record(events, "replace_false_filter_with_empty_limit");
            canonical_empty_limit(input)
        }
        predicate if matches!(input, LogicalPlan::Expand { .. }) => {
            rewrite_filter_into_expand(predicate, input, events)
        }
        outer => match input {
            LogicalPlan::Filter {
                predicate: inner,
                input,
            } => {
                record(events, "fuse_adjacent_filters");
                rewrite_filter(Predicate::And(vec![inner, outer]), *input, events)
            }
            input => LogicalPlan::Filter {
                predicate: outer,
                input: Box::new(input),
            },
        },
    }
}

fn rewrite_filter_into_expand(
    predicate: Predicate,
    input: LogicalPlan,
    events: &mut Vec<RuleEvent>,
) -> LogicalPlan {
    let LogicalPlan::Expand {
        source_variable,
        source_label,
        rel_variable,
        rel_type,
        mut rel_properties,
        direction,
        target_variable,
        target_label,
        min_hops,
        max_hops,
        optional,
        input,
    } = input
    else {
        unreachable!("filter-into-expand requires an expand input")
    };
    let predicates = match predicate {
        Predicate::And(predicates) => predicates,
        predicate => vec![predicate],
    };
    let mut source_predicates = Vec::new();
    let mut residual_predicates = Vec::new();
    let mut embedded_relationship_predicate = false;

    for predicate in predicates {
        if !optional
            && min_hops == 1
            && max_hops == 1
            && let Some(rel_variable) = &rel_variable
            && let Predicate::PropertyEq {
                variable,
                property,
                value,
            } = &predicate
            && variable == rel_variable
        {
            if rel_properties
                .get(property)
                .is_some_and(|existing| existing != value)
            {
                record(events, "detect_conflicting_relationship_filter");
                return canonical_empty_limit(*input);
            }
            rel_properties
                .entry(property.clone())
                .or_insert_with(|| value.clone());
            embedded_relationship_predicate = true;
            continue;
        }
        if predicate_references_only_variable(&predicate, &source_variable) {
            source_predicates.push(predicate);
        } else {
            residual_predicates.push(predicate);
        }
    }

    if source_predicates.is_empty() && !embedded_relationship_predicate {
        return LogicalPlan::Filter {
            predicate: predicates_from_terms(residual_predicates),
            input: Box::new(LogicalPlan::Expand {
                source_variable,
                source_label,
                rel_variable,
                rel_type,
                rel_properties,
                direction,
                target_variable,
                target_label,
                min_hops,
                max_hops,
                optional,
                input,
            }),
        };
    }

    let input = if source_predicates.is_empty() {
        *input
    } else {
        record(events, "push_source_filter_below_expand");
        rewrite_filter(predicates_from_terms(source_predicates), *input, events)
    };
    if embedded_relationship_predicate {
        record(events, "embed_relationship_filter_into_expand");
    }
    let expand = LogicalPlan::Expand {
        source_variable,
        source_label,
        rel_variable,
        rel_type,
        rel_properties,
        direction,
        target_variable,
        target_label,
        min_hops,
        max_hops,
        optional,
        input: Box::new(input),
    };
    if residual_predicates.is_empty() {
        expand
    } else {
        LogicalPlan::Filter {
            predicate: predicates_from_terms(residual_predicates),
            input: Box::new(expand),
        }
    }
}

fn predicates_from_terms(predicates: Vec<Predicate>) -> Predicate {
    simplify_predicate(Predicate::And(predicates))
}

fn predicate_references_only_variable(predicate: &Predicate, variable: &str) -> bool {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => predicates
            .iter()
            .all(|predicate| predicate_references_only_variable(predicate, variable)),
        Predicate::Not(predicate) => predicate_references_only_variable(predicate, variable),
        Predicate::IdEq {
            variable: current, ..
        }
        | Predicate::IdNotEq {
            variable: current, ..
        }
        | Predicate::IdCompare {
            variable: current, ..
        }
        | Predicate::IdIn {
            variable: current, ..
        }
        | Predicate::PropertyEq {
            variable: current, ..
        }
        | Predicate::PropertyNotEq {
            variable: current, ..
        }
        | Predicate::PropertyCompare {
            variable: current, ..
        }
        | Predicate::PropertyListContains {
            variable: current, ..
        }
        | Predicate::PropertyListContainsLower {
            variable: current, ..
        }
        | Predicate::PropertyContains {
            variable: current, ..
        }
        | Predicate::PropertyStartsWith {
            variable: current, ..
        }
        | Predicate::PropertyEndsWith {
            variable: current, ..
        }
        | Predicate::PropertyRegexMatch {
            variable: current, ..
        }
        | Predicate::PropertyIsNull {
            variable: current, ..
        }
        | Predicate::PropertyIsNotNull {
            variable: current, ..
        }
        | Predicate::PropertyIn {
            variable: current, ..
        } => current == variable,
        Predicate::ConstantBool(_)
        | Predicate::RelationshipExists { .. }
        | Predicate::BoundRelationshipExists { .. }
        | Predicate::ExpressionEq { .. }
        | Predicate::ExpressionNotEq { .. }
        | Predicate::ExpressionCompare { .. }
        | Predicate::ExpressionContains { .. } => false,
    }
}

fn rewrite_limit(
    offset: usize,
    limit: Option<usize>,
    input: LogicalPlan,
    events: &mut Vec<RuleEvent>,
) -> LogicalPlan {
    if limit == Some(0) {
        if offset != 0 {
            record(events, "canonicalize_empty_limit");
        }
        return canonical_empty_limit(input);
    }
    if offset == 0 && limit.is_none() {
        record(events, "remove_unbounded_limit");
        return input;
    }

    let LogicalPlan::Limit {
        offset: inner_offset,
        limit: inner_limit,
        input,
    } = input
    else {
        return LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(input),
        };
    };

    record(events, "compose_adjacent_limits");
    if inner_limit.is_some_and(|inner_limit| offset >= inner_limit) {
        return canonical_empty_limit(*input);
    }

    let remaining_inner = inner_limit.map(|inner_limit| inner_limit - offset);
    let combined_limit = match (limit, remaining_inner) {
        (Some(outer), Some(inner)) => Some(outer.min(inner)),
        (Some(outer), None) => Some(outer),
        (None, Some(inner)) => Some(inner),
        (None, None) => None,
    };
    if combined_limit == Some(0) {
        return canonical_empty_limit(*input);
    }

    LogicalPlan::Limit {
        offset: inner_offset.saturating_add(offset),
        limit: combined_limit,
        input,
    }
}

fn canonical_empty_limit(input: LogicalPlan) -> LogicalPlan {
    let input = match input {
        LogicalPlan::Limit { input, .. } => input,
        input => Box::new(input),
    };
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(0),
        input,
    }
}

fn simplify_predicate(predicate: Predicate) -> Predicate {
    match predicate {
        Predicate::And(predicates) => simplify_conjunction(predicates),
        Predicate::Or(predicates) => simplify_disjunction(predicates),
        Predicate::Not(predicate) => match simplify_predicate(*predicate) {
            Predicate::ConstantBool(value) => Predicate::ConstantBool(!value),
            Predicate::Not(predicate) => *predicate,
            predicate => Predicate::Not(Box::new(predicate)),
        },
        Predicate::IdIn { variable, values } => {
            let values = deduplicate_values(values);
            match values.as_slice() {
                [] => Predicate::ConstantBool(false),
                [value] => Predicate::IdEq {
                    variable,
                    value: value.clone(),
                },
                _ => Predicate::IdIn { variable, values },
            }
        }
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => {
            let values = deduplicate_values(values);
            match values.as_slice() {
                [] => Predicate::ConstantBool(false),
                [value] => Predicate::PropertyEq {
                    variable,
                    property,
                    value: value.clone(),
                },
                _ => Predicate::PropertyIn {
                    variable,
                    property,
                    values,
                },
            }
        }
        predicate => predicate,
    }
}

fn simplify_conjunction(predicates: Vec<Predicate>) -> Predicate {
    let mut simplified = Vec::new();
    for predicate in predicates {
        match simplify_predicate(predicate) {
            Predicate::ConstantBool(false) => return Predicate::ConstantBool(false),
            Predicate::ConstantBool(true) => {}
            Predicate::And(nested) => append_unique(&mut simplified, nested),
            predicate => append_unique(&mut simplified, [predicate]),
        }
    }
    if has_conflicting_exact_conjunction(&simplified) {
        return Predicate::ConstantBool(false);
    }
    match simplified.len() {
        0 => Predicate::ConstantBool(true),
        1 => simplified.pop().expect("single predicate should exist"),
        _ => Predicate::And(simplified),
    }
}

fn has_conflicting_exact_conjunction(predicates: &[Predicate]) -> bool {
    predicates.iter().enumerate().any(|(index, predicate)| {
        predicates[index + 1..]
            .iter()
            .any(|other| exact_predicates_conflict(predicate, other))
    })
}

fn exact_predicates_conflict(left: &Predicate, right: &Predicate) -> bool {
    match (left, right) {
        (
            Predicate::IdEq {
                variable: left_variable,
                value: left_value,
            },
            Predicate::IdEq {
                variable: right_variable,
                value: right_value,
            },
        ) => left_variable == right_variable && left_value != right_value,
        (
            Predicate::PropertyEq {
                variable: left_variable,
                property: left_property,
                value: left_value,
            },
            Predicate::PropertyEq {
                variable: right_variable,
                property: right_property,
                value: right_value,
            },
        ) => {
            left_variable == right_variable
                && left_property == right_property
                && left_value != right_value
        }
        _ => false,
    }
}

fn simplify_disjunction(predicates: Vec<Predicate>) -> Predicate {
    let mut simplified = Vec::new();
    for predicate in predicates {
        match simplify_predicate(predicate) {
            Predicate::ConstantBool(true) => return Predicate::ConstantBool(true),
            Predicate::ConstantBool(false) => {}
            Predicate::Or(nested) => append_unique(&mut simplified, nested),
            predicate => append_unique(&mut simplified, [predicate]),
        }
    }
    if let Some(predicate) = collapse_exact_disjunction(&simplified) {
        return predicate;
    }
    match simplified.len() {
        0 => Predicate::ConstantBool(false),
        1 => simplified.pop().expect("single predicate should exist"),
        _ => Predicate::Or(simplified),
    }
}

fn collapse_exact_disjunction(predicates: &[Predicate]) -> Option<Predicate> {
    let mut id_variable = None;
    let mut id_values = Vec::new();
    let mut property_key = None;
    let mut property_values = Vec::new();

    for predicate in predicates {
        match predicate {
            Predicate::IdEq { variable, value } if property_key.is_none() => {
                if id_variable
                    .as_ref()
                    .is_some_and(|current| current != variable)
                {
                    return None;
                }
                id_variable.get_or_insert_with(|| variable.clone());
                push_unique_value(&mut id_values, value.clone());
            }
            Predicate::IdIn { variable, values } if property_key.is_none() => {
                if id_variable
                    .as_ref()
                    .is_some_and(|current| current != variable)
                {
                    return None;
                }
                id_variable.get_or_insert_with(|| variable.clone());
                for value in values {
                    push_unique_value(&mut id_values, value.clone());
                }
            }
            Predicate::PropertyEq {
                variable,
                property,
                value,
            } if id_variable.is_none() => {
                let key = (variable.clone(), property.clone());
                if property_key.as_ref().is_some_and(|current| current != &key) {
                    return None;
                }
                property_key.get_or_insert(key);
                push_unique_value(&mut property_values, value.clone());
            }
            Predicate::PropertyIn {
                variable,
                property,
                values,
            } if id_variable.is_none() => {
                let key = (variable.clone(), property.clone());
                if property_key.as_ref().is_some_and(|current| current != &key) {
                    return None;
                }
                property_key.get_or_insert(key);
                for value in values {
                    push_unique_value(&mut property_values, value.clone());
                }
            }
            _ => return None,
        }
    }

    if let Some(variable) = id_variable {
        return match id_values.as_slice() {
            [value] => Some(Predicate::IdEq {
                variable,
                value: value.clone(),
            }),
            _ => Some(Predicate::IdIn {
                variable,
                values: id_values,
            }),
        };
    }
    property_key.map(|(variable, property)| match property_values.as_slice() {
        [value] => Predicate::PropertyEq {
            variable,
            property,
            value: value.clone(),
        },
        _ => Predicate::PropertyIn {
            variable,
            property,
            values: property_values,
        },
    })
}

fn push_unique_value(values: &mut Vec<Value>, value: Value) {
    if !values.contains(&value) {
        values.push(value);
    }
}

fn append_unique(output: &mut Vec<Predicate>, predicates: impl IntoIterator<Item = Predicate>) {
    for predicate in predicates {
        if !output.contains(&predicate) {
            output.push(predicate);
        }
    }
}

fn deduplicate_values(values: Vec<Value>) -> Vec<Value> {
    let mut output = Vec::with_capacity(values.len());
    for value in values {
        push_unique_value(&mut output, value);
    }
    output
}

fn record(events: &mut Vec<RuleEvent>, rule: &'static str) {
    events.push(RuleEvent::applied(
        format!("transformation:{rule}"),
        "logical expression was simplified",
    ));
}

fn logical_node_count(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::NodeCartesianProduct { left, right } => {
            1 + logical_node_count(left) + logical_node_count(right)
        }
        LogicalPlan::NodeColumnLookup { input, .. }
        | LogicalPlan::Expand { input, .. }
        | LogicalPlan::OptionalDegree { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => 1 + logical_node_count(input),
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OptimizationSearchReport, RuleOutcome};
    use hawdb_cypher::RelationshipDirection;
    use hawdb_plan::{
        AggregateFunction, AggregateTarget, Aggregation, SortDirection, SortItem, SortKey,
    };

    fn scan() -> LogicalPlan {
        LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }
    }

    fn property_eq(value: i64) -> Predicate {
        Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "kind".to_string(),
            value: Value::Int(value),
        }
    }

    fn expand(optional: bool) -> LogicalPlan {
        LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: Some("r".to_string()),
            rel_type: "MENTIONS".to_string(),
            rel_properties: Default::default(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Entity".to_string(),
            min_hops: 1,
            max_hops: 1,
            optional,
            input: Box::new(scan()),
        }
    }

    #[test]
    fn rewrite_is_idempotent_and_fuses_filters() {
        let outer = Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "space_id".to_string(),
            value: Value::Int(1),
        };
        let plan = LogicalPlan::Filter {
            predicate: Predicate::And(vec![Predicate::ConstantBool(true), outer.clone()]),
            input: Box::new(LogicalPlan::Filter {
                predicate: property_eq(2),
                input: Box::new(scan()),
            }),
        };

        let first = rewrite_logical_plan(&plan);
        let second = rewrite_logical_plan(first.plan());

        assert_eq!(first.plan(), second.plan());
        assert!(matches!(
            first.plan(),
            LogicalPlan::Filter {
                predicate: Predicate::And(predicates),
                input,
            } if predicates == &vec![property_eq(2), outer]
                && matches!(input.as_ref(), LogicalPlan::NodeScan { .. })
        ));
        assert!(first
            .events()
            .iter()
            .any(|event| { event.rule() == "transformation:fuse_adjacent_filters" }));
    }

    #[test]
    fn fixed_point_limit_retains_last_rewrite_and_reports_non_convergence() {
        let plan = LogicalPlan::Filter {
            predicate: Predicate::ConstantBool(true),
            input: Box::new(scan()),
        };

        let output = rewrite_logical_plan_with_pass_limit(&plan, 1);

        assert_eq!(output.plan(), &scan());
        assert!(output.events().iter().any(|event| {
            event.rule() == "transformation:remove_true_filter"
                && event.outcome() == RuleOutcome::Applied
        }));
        assert!(output.events().iter().any(|event| {
            event.rule() == "transformation:logical_rewrite_fixed_point"
                && event.outcome() == RuleOutcome::Skipped
                && event.detail().contains("fixed_point_not_reached")
        }));
        assert!(output
            .warning()
            .is_some_and(|warning| warning.contains("fixed_point_not_reached")));
        assert_eq!(output.trace().stats().input_count, 2);
        assert_eq!(output.trace().stats().output_count, 1);
        assert_eq!(output.trace().stats().applied_rules, 1);
        assert_eq!(output.trace().stats().skipped_rules, 1);

        let mut report = OptimizationSearchReport::memo(1);
        super::super::lowering::record_logical_rewrite(&mut report, &output);
        assert!(report
            .warnings()
            .iter()
            .any(|warning| warning.contains("fixed_point_not_reached")));
    }

    #[test]
    fn false_filter_becomes_canonical_empty_limit() {
        let plan = LogicalPlan::Filter {
            predicate: Predicate::PropertyIn {
                variable: "m".to_string(),
                property: "kind".to_string(),
                values: Vec::new(),
            },
            input: Box::new(scan()),
        };

        assert!(matches!(
            rewrite_logical_plan(&plan).plan(),
            LogicalPlan::Limit {
                offset: 0,
                limit: Some(0),
                input,
            } if matches!(input.as_ref(), LogicalPlan::NodeScan { .. })
        ));
    }

    #[test]
    fn adjacent_limits_compose_without_changing_offset_semantics() {
        let plan = LogicalPlan::Limit {
            offset: 3,
            limit: Some(10),
            input: Box::new(LogicalPlan::Limit {
                offset: 5,
                limit: Some(20),
                input: Box::new(scan()),
            }),
        };

        assert!(matches!(
            rewrite_logical_plan(&plan).plan(),
            LogicalPlan::Limit {
                offset: 8,
                limit: Some(10),
                input,
            } if matches!(input.as_ref(), LogicalPlan::NodeScan { .. })
        ));
    }

    #[test]
    fn redundant_order_and_distinct_operators_are_removed() {
        let items = vec![SortItem {
            key: SortKey::Id {
                variable: "m".to_string(),
            },
            direction: SortDirection::Asc,
        }];
        let plan = LogicalPlan::Distinct {
            input: Box::new(LogicalPlan::Distinct {
                input: Box::new(LogicalPlan::Sort {
                    items: items.clone(),
                    input: Box::new(LogicalPlan::Sort {
                        items: items.clone(),
                        input: Box::new(scan()),
                    }),
                }),
            }),
        };

        assert_eq!(
            rewrite_logical_plan(&plan).plan(),
            &LogicalPlan::Distinct {
                input: Box::new(LogicalPlan::Sort {
                    items,
                    input: Box::new(scan()),
                }),
            }
        );
    }

    #[test]
    fn source_filter_moves_before_expand_and_leaves_target_filter_pushable() {
        let source = property_eq(1);
        let target = Predicate::PropertyEq {
            variable: "e".to_string(),
            property: "kind".to_string(),
            value: Value::String("person".to_string()),
        };
        let plan = LogicalPlan::Filter {
            predicate: Predicate::And(vec![source.clone(), target.clone()]),
            input: Box::new(expand(false)),
        };

        let output = rewrite_logical_plan(&plan);
        assert!(matches!(
            output.plan(),
            LogicalPlan::Filter {
                predicate,
                input,
            } if predicate == &target
                && matches!(
                    input.as_ref(),
                    LogicalPlan::Expand { input, .. }
                        if matches!(
                            input.as_ref(),
                            LogicalPlan::Filter { predicate, input }
                                if predicate == &source
                                    && matches!(input.as_ref(), LogicalPlan::NodeScan { .. })
                        )
                )
        ));
        assert!(output
            .events()
            .iter()
            .any(|event| { event.rule() == "transformation:push_source_filter_below_expand" }));
    }

    #[test]
    fn exact_relationship_filter_is_embedded_for_required_one_hop_expand() {
        let plan = LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "r".to_string(),
                property: "role".to_string(),
                value: Value::String("subject".to_string()),
            },
            input: Box::new(expand(false)),
        };

        assert!(matches!(
            rewrite_logical_plan(&plan).plan(),
            LogicalPlan::Expand { rel_properties, .. }
                if rel_properties.get("role") == Some(&Value::String("subject".to_string()))
        ));
    }

    #[test]
    fn optional_expand_keeps_post_expand_relationship_filter() {
        let predicate = Predicate::PropertyEq {
            variable: "r".to_string(),
            property: "role".to_string(),
            value: Value::String("subject".to_string()),
        };
        let plan = LogicalPlan::Filter {
            predicate: predicate.clone(),
            input: Box::new(expand(true)),
        };

        assert!(matches!(
            rewrite_logical_plan(&plan).plan(),
            LogicalPlan::Filter { predicate: actual, input }
                if actual == &predicate
                    && matches!(
                        input.as_ref(),
                        LogicalPlan::Expand { rel_properties, optional: true, .. }
                            if rel_properties.is_empty()
                    )
        ));
    }

    #[test]
    fn exact_or_predicates_collapse_to_one_multiseek_predicate() {
        let plan = LogicalPlan::Filter {
            predicate: Predicate::Or(vec![property_eq(2), property_eq(1), property_eq(2)]),
            input: Box::new(scan()),
        };

        assert!(matches!(
            rewrite_logical_plan(&plan).plan(),
            LogicalPlan::Filter {
                predicate: Predicate::PropertyIn { values, .. },
                ..
            } if values == &vec![Value::Int(2), Value::Int(1)]
        ));
    }

    #[test]
    fn conflicting_exact_predicates_become_empty() {
        let plan = LogicalPlan::Filter {
            predicate: Predicate::And(vec![property_eq(1), property_eq(2)]),
            input: Box::new(scan()),
        };

        assert!(matches!(
            rewrite_logical_plan(&plan).plan(),
            LogicalPlan::Limit { limit: Some(0), .. }
        ));
    }

    #[test]
    fn empty_input_propagates_through_row_preserving_operators() {
        let plan = LogicalPlan::Project {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "title".to_string(),
                },
                name: "title".to_string(),
            }],
            input: Box::new(LogicalPlan::Distinct {
                input: Box::new(LogicalPlan::Sort {
                    items: vec![SortItem {
                        key: SortKey::Id {
                            variable: "m".to_string(),
                        },
                        direction: SortDirection::Asc,
                    }],
                    input: Box::new(LogicalPlan::Filter {
                        predicate: Predicate::ConstantBool(false),
                        input: Box::new(scan()),
                    }),
                }),
            }),
        };

        let output = rewrite_logical_plan(&plan);

        assert!(matches!(
            output.plan(),
            LogicalPlan::Limit {
                offset: 0,
                limit: Some(0),
                ..
            }
        ));
        assert!(output
            .events()
            .iter()
            .any(|event| event.rule() == "transformation:propagate_empty_input"));
    }

    #[test]
    fn empty_input_preserves_global_aggregate_but_eliminates_grouped_aggregate() {
        let empty = LogicalPlan::Filter {
            predicate: Predicate::ConstantBool(false),
            input: Box::new(scan()),
        };
        let count = Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::All,
            distinct: false,
            name: "memory_count".to_string(),
        };
        let global = LogicalPlan::Aggregate {
            group_keys: Vec::new(),
            items: vec![count.clone()],
            input: Box::new(empty.clone()),
        };
        let grouped = LogicalPlan::Aggregate {
            group_keys: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "kind".to_string(),
                },
                name: "kind".to_string(),
            }],
            items: vec![count],
            input: Box::new(empty),
        };

        assert!(matches!(
            rewrite_logical_plan(&global).plan(),
            LogicalPlan::Aggregate { input, .. }
                if matches!(input.as_ref(), LogicalPlan::Limit { limit: Some(0), .. })
        ));
        assert!(matches!(
            rewrite_logical_plan(&grouped).plan(),
            LogicalPlan::Limit { limit: Some(0), .. }
        ));
    }

    #[test]
    fn adjacent_projects_compose_direct_column_aliases() {
        let plan = LogicalPlan::Project {
            items: vec![Projection {
                expression: ProjectionExpression::Column("projected_title".to_string()),
                name: "title".to_string(),
            }],
            input: Box::new(LogicalPlan::Project {
                items: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "m".to_string(),
                        property: "title".to_string(),
                    },
                    name: "projected_title".to_string(),
                }],
                input: Box::new(scan()),
            }),
        };

        let output = rewrite_logical_plan(&plan);

        assert!(matches!(
            output.plan(),
            LogicalPlan::Project { items, input }
                if items
                    == &vec![Projection {
                        expression: ProjectionExpression::Property {
                            variable: "m".to_string(),
                            property: "title".to_string(),
                        },
                        name: "title".to_string(),
                    }]
                    && matches!(input.as_ref(), LogicalPlan::NodeScan { .. })
        ));
        assert!(output
            .events()
            .iter()
            .any(|event| { event.rule() == "transformation:collapse_adjacent_projects" }));
    }

    #[test]
    fn outer_projection_prunes_unreferenced_aggregate_computation() {
        let plan = LogicalPlan::Project {
            items: vec![Projection {
                expression: ProjectionExpression::Column("memory_count".to_string()),
                name: "memory_count".to_string(),
            }],
            input: Box::new(LogicalPlan::Aggregate {
                group_keys: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "m".to_string(),
                        property: "kind".to_string(),
                    },
                    name: "kind".to_string(),
                }],
                items: vec![
                    Aggregation {
                        function: AggregateFunction::Count,
                        target: AggregateTarget::All,
                        distinct: false,
                        name: "memory_count".to_string(),
                    },
                    Aggregation {
                        function: AggregateFunction::Collect,
                        target: AggregateTarget::Property {
                            variable: "m".to_string(),
                            property: "content".to_string(),
                        },
                        distinct: false,
                        name: "unused_contents".to_string(),
                    },
                ],
                input: Box::new(scan()),
            }),
        };

        let output = rewrite_logical_plan(&plan);

        assert!(matches!(
            output.plan(),
            LogicalPlan::Project { input, .. }
                if matches!(
                    input.as_ref(),
                    LogicalPlan::Aggregate { items, .. }
                        if items.len() == 1 && items[0].name == "memory_count"
                )
        ));
        assert!(output
            .events()
            .iter()
            .any(|event| event.rule() == "transformation:prune_unused_aggregates"));
    }
}
