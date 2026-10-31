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

//! Constant folding of temporal expressions in predicates, so that
//! `n.created >= datetime($start)` becomes a native property comparison that
//! storage filters, zone maps and property-index range seeks can use.

use super::*;
use hawdb_core::temporal;

/// Folds temporal function calls, temporal arithmetic and temporal component
/// reads whose inputs are all literals. Evaluation failures leave the
/// expression unchanged, so errors are still reported per row at run time
/// (and plan-cache markers are never interpreted).
pub(super) fn fold_constant(expression: ProjectionExpression) -> ProjectionExpression {
    match expression {
        ProjectionExpression::TemporalFunction {
            function,
            arguments,
        } => {
            let arguments = arguments.into_iter().map(fold_argument).collect::<Vec<_>>();
            let values = arguments
                .iter()
                .map(|argument| match argument {
                    ProjectionExpression::Literal(value) => Some(value.clone()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>();
            match values.and_then(|values| {
                temporal::evaluate(function, &values, temporal::statement_now()).ok()
            }) {
                Some(value) => ProjectionExpression::Literal(value),
                None => ProjectionExpression::TemporalFunction {
                    function,
                    arguments,
                },
            }
        }
        ProjectionExpression::Binary { left, op, right } => {
            let left = fold_constant(*left);
            let right = fold_constant(*right);
            let temporal_op = match op {
                ScalarBinaryOp::Add => Some(temporal::TemporalArithmeticOp::Add),
                ScalarBinaryOp::Subtract => Some(temporal::TemporalArithmeticOp::Subtract),
                ScalarBinaryOp::Multiply => Some(temporal::TemporalArithmeticOp::Multiply),
                ScalarBinaryOp::Divide => Some(temporal::TemporalArithmeticOp::Divide),
                _ => None,
            };
            if let (
                Some(temporal_op),
                ProjectionExpression::Literal(left_value),
                ProjectionExpression::Literal(right_value),
            ) = (temporal_op, &left, &right)
                && let Some(Ok(value)) = temporal::arithmetic(left_value, temporal_op, right_value)
            {
                return ProjectionExpression::Literal(value);
            }
            ProjectionExpression::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }
        }
        ProjectionExpression::PropertyAccess { expression, key } => {
            let expression = fold_constant(*expression);
            if let ProjectionExpression::Literal(value) = &expression
                && temporal::is_temporal(value)
                && let Ok(component) = temporal::component(value, &key)
            {
                return ProjectionExpression::Literal(component);
            }
            ProjectionExpression::PropertyAccess {
                expression: Box::new(expression),
                key,
            }
        }
        other => other,
    }
}

/// Temporal arguments additionally fold literal maps: `duration({days: 3})`.
fn fold_argument(expression: ProjectionExpression) -> ProjectionExpression {
    match expression {
        ProjectionExpression::MapLiteral(entries) => {
            let entries = entries
                .into_iter()
                .map(|(key, value)| (key, fold_argument(value)))
                .collect::<Vec<_>>();
            if entries
                .iter()
                .all(|(_, value)| matches!(value, ProjectionExpression::Literal(_)))
            {
                ProjectionExpression::Literal(Value::Map(
                    entries
                        .into_iter()
                        .map(|(key, value)| match value {
                            ProjectionExpression::Literal(value) => (key, value),
                            _ => unreachable!("checked literal entries"),
                        })
                        .collect(),
                ))
            } else {
                ProjectionExpression::MapLiteral(entries)
            }
        }
        other => fold_constant(other),
    }
}

fn flipped(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Lt => ComparisonOp::Gt,
        ComparisonOp::Lte => ComparisonOp::Gte,
        ComparisonOp::Gt => ComparisonOp::Lt,
        ComparisonOp::Gte => ComparisonOp::Lte,
    }
}

fn is_temporal_literal(expression: &ProjectionExpression) -> bool {
    matches!(expression, ProjectionExpression::Literal(value) if temporal::is_temporal(value))
}

/// Durations are not ordered (`<` is NULL), so their comparisons stay
/// expression predicates with three-valued evaluation.
fn is_ordered_temporal_literal(expression: &ProjectionExpression) -> bool {
    is_temporal_literal(expression)
        && !matches!(
            expression,
            ProjectionExpression::Literal(Value::Duration(_))
        )
}

/// `expression <op> value` after folding; a graph property compared with a
/// temporal constant becomes [`Predicate::PropertyCompare`].
pub(super) fn compare_predicate(
    expression: ProjectionExpression,
    op: ComparisonOp,
    value: ProjectionExpression,
) -> Predicate {
    let (expression, value) = (fold_constant(expression), fold_constant(value));
    match (expression, value) {
        (ProjectionExpression::Property { variable, property }, value)
            if is_ordered_temporal_literal(&value) =>
        {
            let ProjectionExpression::Literal(value) = value else {
                unreachable!("checked temporal literal")
            };
            Predicate::PropertyCompare {
                variable,
                property,
                op,
                value,
            }
        }
        (expression, ProjectionExpression::Property { variable, property })
            if is_ordered_temporal_literal(&expression) =>
        {
            let ProjectionExpression::Literal(value) = expression else {
                unreachable!("checked temporal literal")
            };
            Predicate::PropertyCompare {
                variable,
                property,
                op: flipped(op),
                value,
            }
        }
        (expression, value) => Predicate::ExpressionCompare {
            expression,
            op,
            value,
        },
    }
}

/// `expression = value` after folding; a graph property equal to a
/// temporal constant becomes [`Predicate::PropertyEq`].
pub(super) fn eq_predicate(
    expression: ProjectionExpression,
    value: ProjectionExpression,
) -> Predicate {
    let (expression, value) = (fold_constant(expression), fold_constant(value));
    match (expression, value) {
        (ProjectionExpression::Property { variable, property }, value)
        | (value, ProjectionExpression::Property { variable, property })
            if is_temporal_literal(&value) =>
        {
            let ProjectionExpression::Literal(value) = value else {
                unreachable!("checked temporal literal")
            };
            Predicate::PropertyEq {
                variable,
                property,
                value,
            }
        }
        (expression, value) => Predicate::ExpressionEq { expression, value },
    }
}
