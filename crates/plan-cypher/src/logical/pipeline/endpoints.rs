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

//! Resolves `startNode(r)`/`endNode(r)` to the node variables of the
//! single-hop pattern step that binds `r`.
//!
//! The executor reads the stored source or target from exactly those two
//! variables, never from another variable that happens to bind the same
//! node: such a variable may hold only the properties it reads.

use super::*;

/// Fills [`ProjectionExpression::RelationshipEndpoint::endpoints`] in every
/// read operator from the nearest step below it that binds the relationship.
pub(super) fn resolve(plan: &mut LogicalPlan) -> Result<()> {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            resolve(input)?;
            predicate_expressions(predicate, &mut |expression| bind(expression, input))
        }
        LogicalPlan::Project { items, input } => {
            resolve(input)?;
            for item in items {
                bind(&mut item.expression, input)?;
            }
            Ok(())
        }
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => {
            resolve(input)?;
            for key in group_keys {
                bind(&mut key.expression, input)?;
            }
            for item in items {
                if let AggregateTarget::Expression(expression) = &mut item.target {
                    bind(expression, input)?;
                }
            }
            Ok(())
        }
        LogicalPlan::Sort { items, input } => {
            resolve(input)?;
            for item in items {
                if let SortKey::Expression(expression) = &mut item.key {
                    bind(expression, input)?;
                }
            }
            Ok(())
        }
        LogicalPlan::GraphMatch { program, input } => {
            if let Some(input) = input {
                resolve(input)?;
            }
            let Some(mut predicate) = program.predicate.take() else {
                return Ok(());
            };
            let result = predicate_expressions(&mut predicate, &mut |expression| {
                bind_with(expression, &|relationship| {
                    program_endpoints(program, relationship)
                        .or_else(|| input.as_deref().and_then(|input| find(input, relationship)))
                })
            });
            program.predicate = Some(predicate);
            result
        }
        LogicalPlan::Distinct { input }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::NodeColumnLookup { input, .. }
        | LogicalPlan::Expand { input, .. }
        | LogicalPlan::OptionalDegree { input, .. } => resolve(input),
        LogicalPlan::NodeCartesianProduct { left, right } => {
            resolve(left)?;
            resolve(right)
        }
        _ => Ok(()),
    }
}

fn bind(expression: &mut ProjectionExpression, input: &LogicalPlan) -> Result<()> {
    bind_with(expression, &|relationship| find(input, relationship))
}

fn bind_with(
    expression: &mut ProjectionExpression,
    lookup: &dyn Fn(&str) -> Option<[String; 2]>,
) -> Result<()> {
    if let ProjectionExpression::RelationshipEndpoint {
        endpoint,
        variable,
        endpoints: endpoints @ None,
    } = expression
    {
        *endpoints = Some(lookup(variable).ok_or_else(|| {
            HawDBError::Semantic(format!(
                "{}({variable}) requires '{variable}' to be bound by a single-hop relationship pattern in MATCH",
                endpoint.function_name()
            ))
        })?);
        return Ok(());
    }
    if let ProjectionExpression::Pattern(pattern) = expression {
        // The subquery row holds the pattern's own steps and the enclosing
        // graph variables it copies, not the enclosing steps' endpoints. An
        // enclosing relationship's endpoints are read from the graph by id.
        let program = pattern.program.clone();
        let graph_variables = pattern.graph_variables.clone();
        let inner = |relationship: &str| program_endpoints(&program, relationship);
        for (_, argument) in &mut pattern.arguments {
            bind_with(argument, lookup)?;
        }
        for body in pattern
            .predicate
            .iter_mut()
            .chain(pattern.projection.iter_mut())
        {
            bind_body(body, &inner, &graph_variables)?;
        }
        return Ok(());
    }
    expression.try_for_each_child_mut(|child| bind_with(child, lookup))
}

/// Resolves endpoints inside a pattern expression's body: a relationship the
/// pattern binds uses its step, one copied from the enclosing row stays
/// unresolved and is read by id.
fn bind_body(
    expression: &mut ProjectionExpression,
    lookup: &dyn Fn(&str) -> Option<[String; 2]>,
    graph_variables: &[String],
) -> Result<()> {
    if let ProjectionExpression::RelationshipEndpoint {
        variable,
        endpoints: endpoints @ None,
        ..
    } = expression
        && graph_variables.contains(variable)
    {
        *endpoints = lookup(variable);
        return Ok(());
    }
    if matches!(
        expression,
        ProjectionExpression::RelationshipEndpoint { .. }
    ) {
        return bind_with(expression, lookup);
    }
    if let ProjectionExpression::Pattern(_) = expression {
        return bind_with(expression, lookup);
    }
    expression.try_for_each_child_mut(|child| bind_body(child, lookup, graph_variables))
}

/// The nearest step at or below `plan` that binds `relationship`.
fn find(plan: &LogicalPlan, relationship: &str) -> Option<[String; 2]> {
    match plan {
        LogicalPlan::GraphMatch { program, input } => program_endpoints(program, relationship)
            .or_else(|| input.as_deref().and_then(|input| find(input, relationship))),
        LogicalPlan::Expand {
            source_variable,
            rel_variable,
            target_variable,
            min_hops: 1,
            max_hops: 1,
            ..
        } if rel_variable.as_deref() == Some(relationship) => {
            Some([source_variable.clone(), target_variable.clone()])
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::NodeColumnLookup { input, .. }
        | LogicalPlan::Expand { input, .. }
        | LogicalPlan::OptionalDegree { input, .. } => find(input, relationship),
        LogicalPlan::NodeCartesianProduct { left, right } => {
            find(left, relationship).or_else(|| find(right, relationship))
        }
        _ => None,
    }
}

fn program_endpoints(program: &GraphMatchProgram, relationship: &str) -> Option<[String; 2]> {
    program.steps.iter().find_map(|step| match step {
        GraphMatchStep::Expand {
            source,
            relationship: Some(bound),
            target,
            min_hops: 1,
            max_hops: 1,
            ..
        } if bound == relationship => Some([source.clone(), target.variable.clone()]),
        _ => None,
    })
}

fn predicate_expressions(
    predicate: &mut Predicate,
    visit: &mut dyn FnMut(&mut ProjectionExpression) -> Result<()>,
) -> Result<()> {
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => {
            for predicate in predicates {
                predicate_expressions(predicate, visit)?;
            }
            Ok(())
        }
        Predicate::Not(predicate) => predicate_expressions(predicate, visit),
        Predicate::ExpressionEq { expression, value }
        | Predicate::ExpressionNotEq { expression, value }
        | Predicate::ExpressionCompare {
            expression, value, ..
        }
        | Predicate::ExpressionContains { expression, value } => {
            visit(expression)?;
            visit(value)
        }
        Predicate::ExpressionRegexMatch { expression, .. }
        | Predicate::ExpressionIn { expression, .. } => visit(expression),
        _ => Ok(()),
    }
}
