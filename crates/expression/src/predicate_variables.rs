// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! The row variables and columns a predicate reads.

use crate::{Predicate, ProjectionExpression};
use std::collections::BTreeSet;

/// Every row variable or column `predicate` reads: graph variables and
/// row columns, which share one namespace. Constants and parameters are
/// bound values, not names.
pub fn predicate_variables(predicate: &Predicate) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    predicate_names(predicate, &mut names);
    names
}

/// Every row variable or column a predicate reads. The predicate IR has no
/// aggregates, writes or nondeterministic functions, so its variables decide
/// alone whether it can run early. The matches are exhaustive so a new
/// construct must state what it reads.
fn predicate_names(predicate: &Predicate, names: &mut BTreeSet<String>) {
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                predicate_names(child, names);
            }
        }
        Predicate::Not(child) => predicate_names(child, names),
        Predicate::ConstantBool(_) => {}
        Predicate::RelationshipExists { variable, .. }
        | Predicate::IdEq { variable, .. }
        | Predicate::IdNotEq { variable, .. }
        | Predicate::IdCompare { variable, .. }
        | Predicate::IdIn { variable, .. }
        | Predicate::PropertyEq { variable, .. }
        | Predicate::PropertyNotEq { variable, .. }
        | Predicate::PropertyCompare { variable, .. }
        | Predicate::PropertyListContains { variable, .. }
        | Predicate::PropertyListContainsLower { variable, .. }
        | Predicate::PropertyContains { variable, .. }
        | Predicate::PropertyStartsWith { variable, .. }
        | Predicate::PropertyEndsWith { variable, .. }
        | Predicate::PropertyRegexMatch { variable, .. }
        | Predicate::PropertyIsNull { variable, .. }
        | Predicate::PropertyIsNotNull { variable, .. }
        | Predicate::PropertyIn { variable, .. } => {
            names.insert(variable.clone());
        }
        Predicate::BoundRelationshipExists {
            source_variable,
            target_variable,
            ..
        } => {
            names.insert(source_variable.clone());
            names.insert(target_variable.clone());
        }
        Predicate::ExpressionEq { expression, value }
        | Predicate::ExpressionNotEq { expression, value }
        | Predicate::ExpressionCompare {
            expression, value, ..
        }
        | Predicate::ExpressionContains { expression, value } => {
            expression_names(expression, names);
            expression_names(value, names);
        }
        Predicate::ExpressionRegexMatch { expression, .. } => {
            expression_names(expression, names);
        }
        Predicate::ExpressionIn { expression, .. } => expression_names(expression, names),
    }
}

fn expression_names(expression: &ProjectionExpression, names: &mut BTreeSet<String>) {
    match expression {
        ProjectionExpression::Variable { variable }
        | ProjectionExpression::Property { variable, .. }
        | ProjectionExpression::Id { variable }
        | ProjectionExpression::RelationshipType { variable }
        | ProjectionExpression::DatePart { variable, .. }
        | ProjectionExpression::DefaultIfNullOrEq { variable, .. }
        | ProjectionExpression::DefaultIfNull { variable, .. }
        | ProjectionExpression::CasePropertyNotNullOrEq { variable, .. }
        | ProjectionExpression::CasePropertyEqualsRank { variable, .. }
        | ProjectionExpression::CaseLowerPropertyDefault { variable, .. }
        | ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            names.insert(variable.clone());
        }
        ProjectionExpression::CaseEntitySearchRank(rank) => {
            names.insert(rank.variable.clone());
        }
        // The endpoint is read from one of the binding step's node variables.
        ProjectionExpression::RelationshipEndpoint {
            variable,
            endpoints,
            ..
        } => {
            names.insert(variable.clone());
            names.extend(endpoints.iter().flatten().cloned());
        }
        // Columns and path values live in the same row namespace as graph
        // variables; a path column is bound by its shortest-path step.
        ProjectionExpression::Column(column)
        | ProjectionExpression::ColumnProperty { column, .. }
        | ProjectionExpression::ColumnDefaultIfNullOrEq { column, .. }
        | ProjectionExpression::ColumnValueDefaultIfNull { column, .. }
        | ProjectionExpression::ColumnValueCasePropertyNotNullOrEq { column, .. }
        | ProjectionExpression::Path { column, .. } => {
            names.insert(column.clone());
        }
        ProjectionExpression::CaseColumnSearchRank(rank) => {
            names.insert(rank.column.clone());
        }
        // A comprehension's own element reads no row column.
        ProjectionExpression::LocalVariable { .. } | ProjectionExpression::Literal(_) => {}
        // The subquery reads these enclosing variables and columns; the rest
        // of its body reads only its own variables and those locals.
        ProjectionExpression::Pattern(pattern) => {
            names.extend(pattern.graph_variables.iter().cloned());
            names.extend(pattern.columns.iter().cloned());
            for argument in pattern
                .arguments
                .iter()
                .map(|(_, argument)| argument)
                .chain(pattern.seeds.iter().flat_map(|seeds| &seeds.expressions))
            {
                expression_names(argument, names);
            }
        }
        ProjectionExpression::Coalesce(_)
        | ProjectionExpression::VectorDistance { .. }
        | ProjectionExpression::JsonValue { .. }
        | ProjectionExpression::Left { .. }
        | ProjectionExpression::Lower(_)
        | ProjectionExpression::Case { .. }
        | ProjectionExpression::Binary { .. }
        | ProjectionExpression::Not(_)
        | ProjectionExpression::IsNull { .. }
        | ProjectionExpression::List(_)
        | ProjectionExpression::ListFunction { .. }
        | ProjectionExpression::StringFunction { .. }
        | ProjectionExpression::ListComprehension(_)
        | ProjectionExpression::ListIndex { .. }
        | ProjectionExpression::ListSlice { .. }
        | ProjectionExpression::Conversion { .. }
        | ProjectionExpression::NumericFunction { .. }
        | ProjectionExpression::Reduce(_)
        | ProjectionExpression::TemporalFunction { .. }
        | ProjectionExpression::PropertyAccess { .. }
        | ProjectionExpression::RelationshipValueEndpoint { .. }
        | ProjectionExpression::MapLiteral(_) => {
            expression.all_children(|child| {
                expression_names(child, names);
                true
            });
        }
    }
}
