// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::cypher;

pub(super) fn match_is_deterministic(
    patterns: &[cypher::MatchPattern],
    predicate: Option<&cypher::PredicateExpression>,
) -> bool {
    patterns.iter().all(|pattern| {
        pattern
            .first
            .properties
            .values()
            .all(value_is_deterministic)
            && pattern.steps.iter().all(|step| {
                step.relationship
                    .properties
                    .values()
                    .chain(step.target.properties.values())
                    .all(value_is_deterministic)
            })
    }) && predicate.is_none_or(|predicate| predicate_is_deterministic(&predicate.kind))
}

pub(super) fn projection_is_deterministic(projection: &cypher::ProjectionClause) -> bool {
    projection
        .items
        .iter()
        .all(|item| return_is_deterministic(&item.expression))
        && projection
            .order_by
            .iter()
            .all(|item| match &item.expression {
                cypher::OrderExpression::Value(value) => scalar_is_deterministic(value),
                cypher::OrderExpression::Property { .. }
                | cypher::OrderExpression::Id { .. }
                | cypher::OrderExpression::Column(_) => true,
            })
        && projection
            .predicate
            .as_ref()
            .is_none_or(|predicate| predicate_is_deterministic(&predicate.kind))
        && projection
            .offset
            .iter()
            .chain(projection.limit.iter())
            .all(value_is_deterministic)
}

pub(super) fn graph_options_are_deterministic(options: &cypher::GraphAlgorithmOptions) -> bool {
    [
        &options.damping,
        &options.max_iterations,
        &options.max_phases,
        &options.max_levels,
        &options.tolerance,
        &options.normalize_initial,
        &options.resolution,
    ]
    .into_iter()
    .flatten()
    .all(value_is_deterministic)
}

fn value_is_deterministic(value: &cypher::ValueExpression) -> bool {
    match &value.kind {
        cypher::ValueExpressionKind::CurrentTimestamp => false,
        cypher::ValueExpressionKind::List(values) => values.iter().all(value_is_deterministic),
        cypher::ValueExpressionKind::Timestamp(value) => value_is_deterministic(value),
        cypher::ValueExpressionKind::Literal(_)
        | cypher::ValueExpressionKind::Parameter(_)
        | cypher::ValueExpressionKind::BindingProperty { .. } => true,
    }
}

fn scalar_is_deterministic(expression: &cypher::ScalarExpression) -> bool {
    use cypher::ScalarExpressionKind as Scalar;
    let values_are_deterministic = match &expression.kind {
        Scalar::Value(value) => value_is_deterministic(value),
        Scalar::Left { length, .. } => value_is_deterministic(length),
        Scalar::DefaultIfNullOrEq { empty, default, .. } => {
            value_is_deterministic(empty) && value_is_deterministic(default)
        }
        Scalar::DefaultIfNull { default, .. }
        | Scalar::CaseLowerPropertyDefault { default, .. } => value_is_deterministic(default),
        Scalar::CasePropertyNotNullOrEq {
            empty,
            non_empty,
            null_or_empty,
            ..
        } => [empty, non_empty, null_or_empty]
            .into_iter()
            .all(|value| value_is_deterministic(value)),
        Scalar::CasePropertyEqualsRank {
            branches, default, ..
        } => branches
            .iter()
            .flat_map(|(condition, result)| [condition, result])
            .chain([default])
            .all(value_is_deterministic),
        Scalar::CaseCoalesceDifferenceFloorZero { terms, .. } => terms
            .iter()
            .all(|term| value_is_deterministic(&term.default)),
        Scalar::Variable(_)
        | Scalar::Property { .. }
        | Scalar::Id(_)
        | Scalar::Properties(_)
        | Scalar::Labels(_)
        | Scalar::RelationshipType(_)
        | Scalar::Coalesce(_)
        | Scalar::Lower(_)
        | Scalar::DatePart { .. }
        | Scalar::Case { .. }
        | Scalar::Binary { .. }
        | Scalar::Not(_)
        | Scalar::IsNull { .. } => true,
    };
    values_are_deterministic && expression.kind.all_children(scalar_is_deterministic)
}

fn return_is_deterministic(expression: &cypher::ReturnExpression) -> bool {
    match &expression.kind {
        cypher::ReturnExpressionKind::Value(value) => scalar_is_deterministic(value),
        cypher::ReturnExpressionKind::Arithmetic { first, rest } => {
            return_is_deterministic(first)
                && rest.iter().all(|(_, value)| return_is_deterministic(value))
        }
        cypher::ReturnExpressionKind::Aggregate(_) | cypher::ReturnExpressionKind::Path(_) => true,
    }
}

fn predicate_is_deterministic(predicate: &cypher::PropertyPredicate) -> bool {
    use cypher::PropertyPredicate as Predicate;
    match predicate {
        Predicate::And(values) | Predicate::Or(values) => {
            values.iter().all(predicate_is_deterministic)
        }
        Predicate::Not(value) => predicate_is_deterministic(value),
        Predicate::IdEq { value, .. }
        | Predicate::IdNotEq { value, .. }
        | Predicate::IdCompare { value, .. }
        | Predicate::Eq { value, .. }
        | Predicate::NotEq { value, .. }
        | Predicate::Compare { value, .. }
        | Predicate::ListContains { value, .. }
        | Predicate::ListContainsLower { value, .. }
        | Predicate::Contains { value, .. }
        | Predicate::StartsWith { value, .. }
        | Predicate::EndsWith { value, .. }
        | Predicate::RegexMatch { pattern: value, .. }
        | Predicate::ParameterEq { right: value, .. }
        | Predicate::ParameterNotEq { right: value, .. }
        | Predicate::IdIn { values: value, .. }
        | Predicate::In { values: value, .. } => value_is_deterministic(value),
        Predicate::ExpressionEq { expression, value }
        | Predicate::ExpressionNotEq { expression, value }
        | Predicate::ExpressionCompare {
            expression, value, ..
        }
        | Predicate::ExpressionContains { expression, value } => {
            scalar_is_deterministic(expression) && scalar_is_deterministic(value)
        }
        Predicate::RelationshipExists { .. }
        | Predicate::BoundRelationshipExists { .. }
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::ParameterIsNull { .. }
        | Predicate::ParameterIsNotNull { .. } => true,
    }
}
