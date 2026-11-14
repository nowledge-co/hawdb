// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! SET items that are general expressions over the matched rows (#1791).
//!
//! The single-pattern SET commands evaluate only literals, parameters and a
//! few fixed read-modify-writes of the written property. Every other SET is
//! bound here: the clauses before SET become an ordinary read plan that
//! returns one column per variable in scope, and [`MatchSet`] evaluates the
//! SET items over those rows.

use super::*;
use crate::logical::{MatchSet, MatchSetAssignment, MatchSetBinding, MatchSetValue};
use hawdb_cypher::{ProjectionClause, QueryPipelineKind};

/// The input column of `variable`. A NUL byte cannot occur in a Cypher name,
/// so the column never collides with a user column.
fn binding_column(variable: &str) -> String {
    format!("\0set.{variable}")
}

/// Whether a SET item needs row evaluation: its value is a general
/// expression, or a fixed form the single-pattern commands cannot evaluate,
/// such as a property of another variable or a non-integer increment.
/// Binding errors such as a missing parameter stay with the fixed forms, so
/// they report what they always reported.
pub(super) fn needs_row_evaluation(
    set: &SetProperty,
    parameters: &BTreeMap<String, Value>,
) -> bool {
    let other = |variable: &String, property: &String| {
        *variable != set.variable || *property != set.property
    };
    // `x.p + v` is an integer increment only for an integer `v`; a value
    // read from the row, such as another variable's property, is general.
    let non_integer = |value: &ValueExpression| {
        value.kind.reads_row_binding()
            || bind_value(value, parameters).is_ok_and(|value| !matches!(value, Value::Int(_)))
    };
    match &set.value {
        SetValueExpression::Value(value) => value.kind.reads_row_binding(),
        SetValueExpression::Expression(_) | SetValueExpression::Property { .. } => true,
        SetValueExpression::CoalesceProperty {
            variable, property, ..
        }
        | SetValueExpression::DecrementFloorZero { variable, property }
        | SetValueExpression::PreserveNewerExisting {
            variable, property, ..
        }
        | SetValueExpression::PreserveOlderExisting {
            variable, property, ..
        } => other(variable, property),
        SetValueExpression::PropertyAdd {
            variable,
            property,
            value,
        } => other(variable, property) || non_integer(value),
        SetValueExpression::CoalescePropertyAdd {
            variable,
            property,
            default,
            value,
        } => {
            // The fixed form only supports a zero default.
            other(variable, property)
                || bind_value(default, parameters).is_ok_and(|default| default != Value::Int(0))
                || non_integer(value)
        }
    }
}

pub(super) fn bind(
    prefix: &[Clause],
    sets: &[SetProperty],
    returns: Option<&[ReturnItem]>,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    if sets.is_empty() {
        return Err(unsupported("SET requires at least one assignment"));
    }
    let (_, scope) = bind_read_clauses_scoped(prefix, None, Scope::default(), parameters)?;
    // Anonymous pattern nodes cannot be named by a SET item or RETURN.
    let anonymous = prefix
        .iter()
        .filter_map(|clause| match &clause.kind {
            ClauseKind::Match { patterns, .. } => Some(patterns),
            _ => None,
        })
        .flatten()
        .flat_map(|pattern| {
            std::iter::once(&pattern.first).chain(pattern.steps.iter().map(|step| &step.target))
        })
        .filter(|node| node.anonymous)
        .map(|node| node.variable.as_str())
        .collect::<BTreeSet<_>>();
    let mut graph = BTreeSet::new();
    let mut values = BTreeSet::new();
    let mut bindings = Vec::new();
    let mut items = Vec::new();
    for (variable, binding) in &scope.0 {
        let (expression, entity) = match binding {
            BindingType::Graph { kind, .. } => {
                if anonymous.contains(variable.as_str()) {
                    continue;
                }
                graph.insert(variable.clone());
                (ScalarExpressionKind::Id(variable.clone()), Some(*kind))
            }
            BindingType::Scalar | BindingType::Unwound | BindingType::Path => {
                values.insert(variable.clone());
                (ScalarExpressionKind::Variable(variable.clone()), None)
            }
            BindingType::UnmaterializedPath => continue,
        };
        let column = binding_column(variable);
        items.push(AstNode::synthetic(ReturnItemKind {
            expression: AstNode::synthetic(ReturnExpressionKind::Value(AstNode::synthetic(
                expression,
            ))),
            alias: Some(column.clone()),
        }));
        bindings.push(MatchSetBinding {
            variable: variable.clone(),
            column,
            entity,
        });
    }
    let mut assignments = Vec::with_capacity(sets.len());
    for set in sets {
        if !graph.contains(&set.variable) {
            return Err(HawDBError::Semantic(format!(
                "unknown variable '{}' in set item: SET requires a bound node or relationship",
                set.variable
            )));
        }
        let value = match &set.value {
            SetValueExpression::Expression(expression) => MatchSetValue::Expression(
                plan_scalar_expression_with_columns(&graph, &values, expression, parameters)?,
            ),
            SetValueExpression::Value(value) if value.kind.reads_row_binding() => {
                MatchSetValue::Expression(plan_scalar_expression_with_columns(
                    &graph,
                    &values,
                    &row_value_expression(value.clone())?,
                    parameters,
                )?)
            }
            value => match plan_set_value(set, parameters) {
                Ok(value) => MatchSetValue::Local(value),
                Err(error) => {
                    let expression = fixed_form_expression(value).ok_or(error)?;
                    MatchSetValue::Expression(plan_scalar_expression_with_columns(
                        &graph,
                        &values,
                        &expression,
                        parameters,
                    )?)
                }
            },
        };
        assignments.push(MatchSetAssignment {
            variable: set.variable.clone(),
            property: set.property.clone(),
            value,
        });
    }
    let returns = returns
        .map(|items| {
            let PlannedReturns::Projections(mut projections) =
                plan_return_items_with_columns(&graph, &values, items, parameters)?
            else {
                return Err(unsupported(
                    "RETURN after a row-evaluated SET does not support aggregation",
                ));
            };
            let names =
                unique_projection_names(projections.iter().map(|item| item.name.clone()).collect());
            for (projection, name) in projections.iter_mut().zip(names) {
                projection.name = name;
            }
            Ok(projections)
        })
        .transpose()?;
    let mut clauses = prefix.to_vec();
    clauses.push(AstNode::synthetic(ClauseKind::Return(ProjectionClause {
        distinct: false,
        items,
        predicate: None,
        order_by: Vec::new(),
        offset: None,
        limit: None,
    })));
    let input = plan_parsed_pipeline_query(
        &AstNode::synthetic(QueryPipelineKind { clauses }),
        parameters,
    )?;
    Ok(LogicalPlan::MatchSet(Box::new(MatchSet {
        input,
        bindings,
        assignments,
        returns,
    })))
}

/// The expression a fixed SET form denotes, for the forms row evaluation
/// computes generally, with openCypher semantics: `x.p + v` is `NULL` when
/// `x.p` is `NULL`, and concatenates strings.
fn fixed_form_expression(value: &SetValueExpression) -> Option<ScalarExpression> {
    let property = |variable: &String, property: &String| {
        AstNode::synthetic(ScalarExpressionKind::Property {
            variable: variable.clone(),
            property: property.clone(),
        })
    };
    // A row-bound operand, such as `m.x` in `n.p + m.x`, reads the row.
    let value_expression = |value: &ValueExpression| {
        row_value_expression(value.clone())
            .unwrap_or_else(|_| AstNode::synthetic(ScalarExpressionKind::Value(value.clone())))
    };
    let binary = |left, op, right| {
        AstNode::synthetic(ScalarExpressionKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    };
    let coalesce = |variable: &String, name: &String, default: &ValueExpression| {
        AstNode::synthetic(ScalarExpressionKind::Coalesce(vec![
            property(variable, name),
            value_expression(default),
        ]))
    };
    Some(match value {
        SetValueExpression::Expression(expression) => expression.clone(),
        SetValueExpression::Value(value) => value_expression(value),
        SetValueExpression::Property {
            variable,
            property: name,
        } => property(variable, name),
        SetValueExpression::CoalesceProperty {
            variable,
            property: name,
            default,
        } => coalesce(variable, name, default),
        SetValueExpression::PropertyAdd {
            variable,
            property: name,
            value,
        } => binary(
            property(variable, name),
            hawdb_cypher::ScalarBinaryOp::Add,
            value_expression(value),
        ),
        SetValueExpression::CoalescePropertyAdd {
            variable,
            property: name,
            default,
            value,
        } => binary(
            coalesce(variable, name, default),
            hawdb_cypher::ScalarBinaryOp::Add,
            value_expression(value),
        ),
        SetValueExpression::DecrementFloorZero {
            variable,
            property: name,
        } => case(
            vec![(
                binary(
                    property(variable, name),
                    hawdb_cypher::ScalarBinaryOp::Gt,
                    integer(0),
                ),
                binary(
                    property(variable, name),
                    hawdb_cypher::ScalarBinaryOp::Subtract,
                    integer(1),
                ),
            )],
            integer(0),
        ),
        SetValueExpression::PreserveNewerExisting {
            variable,
            property: name,
            incoming,
            preserve,
        }
        | SetValueExpression::PreserveOlderExisting {
            variable,
            property: name,
            incoming,
            preserve,
        } => {
            // Keep the current value when the incoming one is NULL, or when
            // `preserve` holds and the current value is newer (older).
            let current = property(variable, name);
            let incoming = value_expression(incoming);
            let keeps = if matches!(value, SetValueExpression::PreserveNewerExisting { .. }) {
                binary(
                    current.clone(),
                    hawdb_cypher::ScalarBinaryOp::Gt,
                    incoming.clone(),
                )
            } else {
                binary(
                    incoming.clone(),
                    hawdb_cypher::ScalarBinaryOp::Gt,
                    current.clone(),
                )
            };
            let present = AstNode::synthetic(ScalarExpressionKind::IsNull {
                expression: Box::new(current.clone()),
                negated: true,
            });
            let preserved = binary(
                binary(
                    value_expression(preserve),
                    hawdb_cypher::ScalarBinaryOp::And,
                    present,
                ),
                hawdb_cypher::ScalarBinaryOp::And,
                keeps,
            );
            let missing = AstNode::synthetic(ScalarExpressionKind::IsNull {
                expression: Box::new(incoming.clone()),
                negated: false,
            });
            case(
                vec![(missing, current.clone()), (preserved, current)],
                incoming,
            )
        }
    })
}

fn integer(value: i64) -> ScalarExpression {
    AstNode::synthetic(ScalarExpressionKind::Value(AstNode::synthetic(
        ValueExpressionKind::Literal(Value::Int(value)),
    )))
}

fn case(
    branches: Vec<(ScalarExpression, ScalarExpression)>,
    otherwise: ScalarExpression,
) -> ScalarExpression {
    AstNode::synthetic(ScalarExpressionKind::Case {
        operand: None,
        branches,
        otherwise: Some(Box::new(otherwise)),
    })
}
