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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use hawdb_core::{RelationshipDirection, Value};
use hawdb_plan::{ComparisonOp, LogicalPlan, Predicate, Projection, ProjectionExpression};
use hawdb_sql_syntax::{BinaryOperatorSyntax, GraphEdgeDirection, Span, UnaryOperatorSyntax};

use super::{
    BoundPgqExpression, BoundPgqExpressionKind, BoundPgqGraphTable, BoundPgqLiteral,
    BoundPgqPathPrimary, BoundPgqVariable, PgqSlotId,
};

pub type PgqLoweringParameters = BTreeMap<u32, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgqLoweringErrorCode {
    InvalidBoundPlan,
    UnsupportedPath,
    UnsupportedLabels,
    UnsupportedExpression,
    MissingParameter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgqLoweringError {
    pub code: PgqLoweringErrorCode,
    pub span: Span,
    pub message: String,
}

impl PgqLoweringError {
    fn new(code: PgqLoweringErrorCode, span: Span, message: impl Into<String>) -> Self {
        Self {
            code,
            span,
            message: message.into(),
        }
    }
}

impl fmt::Display for PgqLoweringError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "SQL/PGQ lowering error {:?} at bytes {}..{}: {}",
            self.code, self.span.start, self.span.end, self.message
        )
    }
}

impl std::error::Error for PgqLoweringError {}

/// Lowers the qualified executable SQL/PGQ subset into HawDB's shared graph
/// logical operators.
///
/// Shapes without an equivalent shared operator fail here. Syntax nodes never
/// enter the optimizer or executor, and this path never renders Cypher text.
pub fn lower_bound_pgq_graph_table(
    table: &BoundPgqGraphTable,
    parameters: &PgqLoweringParameters,
) -> Result<LogicalPlan, PgqLoweringError> {
    let path = match table.pattern.paths.as_slice() {
        [path] => path,
        _ => {
            return Err(PgqLoweringError::new(
                PgqLoweringErrorCode::InvalidBoundPlan,
                table.span,
                "the executable GRAPH_TABLE subset requires exactly one bound path",
            ));
        }
    };
    let slots = SlotBindings::new(&table.variables)?;
    let mut factors = path.factors.iter();
    let first = factors.next().ok_or_else(|| {
        PgqLoweringError::new(
            PgqLoweringErrorCode::InvalidBoundPlan,
            table.span,
            "the bound graph path is empty",
        )
    })?;
    if first.quantifier.is_some() {
        return Err(PgqLoweringError::new(
            PgqLoweringErrorCode::UnsupportedPath,
            slots.span(primary_slot(&first.primary)),
            "vertex quantifiers are not executable in the shared graph plan",
        ));
    }
    let BoundPgqPathPrimary::Vertex {
        slot: first_slot,
        predicate: first_predicate,
    } = &first.primary
    else {
        return Err(PgqLoweringError::new(
            PgqLoweringErrorCode::InvalidBoundPlan,
            slots.span(primary_slot(&first.primary)),
            "a bound graph path must start with a vertex",
        ));
    };

    let first_variable = slots.variable(*first_slot)?;
    let mut path_slots = BTreeSet::from([*first_slot]);
    let mut current_name = slots.name(*first_slot)?.to_string();
    let mut current_label = executable_label(first_variable)?;
    let mut plan = LogicalPlan::NodeScan {
        variable: current_name.clone(),
        label: current_label.clone(),
    };
    if let Some(predicate) = first_predicate {
        plan = filter_plan(plan, lower_predicate(predicate, &slots, parameters)?);
    }

    while let Some(edge_factor) = factors.next() {
        let Some(target_factor) = factors.next() else {
            return Err(PgqLoweringError::new(
                PgqLoweringErrorCode::InvalidBoundPlan,
                table.span,
                "a bound edge is missing its target vertex",
            ));
        };
        if edge_factor
            .quantifier
            .as_ref()
            .is_some_and(|range| range.min != 1 || range.max != 1)
        {
            return Err(PgqLoweringError::new(
                PgqLoweringErrorCode::UnsupportedPath,
                slots.span(primary_slot(&edge_factor.primary)),
                "quantified paths require SQL/PGQ walk-semantics qualification",
            ));
        }
        if target_factor.quantifier.is_some() {
            return Err(PgqLoweringError::new(
                PgqLoweringErrorCode::UnsupportedPath,
                slots.span(primary_slot(&target_factor.primary)),
                "vertex quantifiers are not executable in the shared graph plan",
            ));
        }
        let BoundPgqPathPrimary::Edge {
            slot: edge_slot,
            direction,
            predicate: edge_predicate,
        } = &edge_factor.primary
        else {
            return Err(PgqLoweringError::new(
                PgqLoweringErrorCode::InvalidBoundPlan,
                slots.span(primary_slot(&edge_factor.primary)),
                "bound graph paths must alternate vertex and edge factors",
            ));
        };
        let BoundPgqPathPrimary::Vertex {
            slot: target_slot,
            predicate: target_predicate,
        } = &target_factor.primary
        else {
            return Err(PgqLoweringError::new(
                PgqLoweringErrorCode::UnsupportedPath,
                slots.span(primary_slot(&target_factor.primary)),
                "parenthesized paths are not executable in the shared graph plan",
            ));
        };

        let edge = slots.variable(*edge_slot)?;
        let target = slots.variable(*target_slot)?;
        for slot in [*edge_slot, *target_slot] {
            if !path_slots.insert(slot) {
                return Err(PgqLoweringError::new(
                    PgqLoweringErrorCode::UnsupportedPath,
                    slots.span(slot),
                    "reused graph variables require an explicit identity constraint in the shared graph plan",
                ));
            }
        }
        let target_name = slots.name(*target_slot)?.to_string();
        let target_label = executable_label(target)?;
        plan = LogicalPlan::Expand {
            source_variable: current_name,
            source_label: current_label,
            rel_variable: Some(slots.name(*edge_slot)?.to_string()),
            rel_type: executable_label(edge)?,
            rel_properties: BTreeMap::new(),
            direction: lower_direction(*direction),
            target_variable: target_name.clone(),
            target_label: target_label.clone(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: Box::new(plan),
        };
        let mut predicates = Vec::new();
        if let Some(predicate) = edge_predicate {
            predicates.push(lower_predicate(predicate, &slots, parameters)?);
        }
        if let Some(predicate) = target_predicate {
            predicates.push(lower_predicate(predicate, &slots, parameters)?);
        }
        if !predicates.is_empty() {
            plan = filter_plan(plan, and_predicates(predicates));
        }
        current_name = target_name;
        current_label = target_label;
    }

    if let Some(predicate) = &table.pattern.predicate {
        plan = filter_plan(plan, lower_predicate(predicate, &slots, parameters)?);
    }
    let items = table
        .columns
        .iter()
        .map(|column| {
            Ok(Projection {
                expression: lower_projection(&column.expression, &slots, parameters)?,
                name: column.name.clone(),
            })
        })
        .collect::<Result<Vec<_>, PgqLoweringError>>()?;
    Ok(LogicalPlan::Project {
        items,
        input: Box::new(plan),
    })
}

struct SlotBindings<'a> {
    variables: BTreeMap<PgqSlotId, &'a BoundPgqVariable>,
    names: BTreeMap<PgqSlotId, String>,
}

impl<'a> SlotBindings<'a> {
    fn new(variables: &'a [BoundPgqVariable]) -> Result<Self, PgqLoweringError> {
        let mut by_slot = BTreeMap::new();
        let mut used_names = variables
            .iter()
            .filter_map(|variable| variable.name.clone())
            .collect::<BTreeSet<_>>();
        let mut names = BTreeMap::new();
        for variable in variables {
            if by_slot.insert(variable.slot, variable).is_some() {
                return Err(PgqLoweringError::new(
                    PgqLoweringErrorCode::InvalidBoundPlan,
                    variable.span,
                    format!("duplicate bound graph slot {}", variable.slot.0),
                ));
            }
            let name = variable.name.clone().unwrap_or_else(|| {
                unique_internal_name(
                    format!("__hawdb_pgq_slot_{}", variable.slot.0),
                    &mut used_names,
                )
            });
            names.insert(variable.slot, name);
        }
        Ok(Self {
            variables: by_slot,
            names,
        })
    }

    fn variable(&self, slot: PgqSlotId) -> Result<&'a BoundPgqVariable, PgqLoweringError> {
        self.variables.get(&slot).copied().ok_or_else(|| {
            PgqLoweringError::new(
                PgqLoweringErrorCode::InvalidBoundPlan,
                Span::new(0, 0),
                format!("bound graph slot {} is missing", slot.0),
            )
        })
    }

    fn name(&self, slot: PgqSlotId) -> Result<&str, PgqLoweringError> {
        self.names.get(&slot).map(String::as_str).ok_or_else(|| {
            PgqLoweringError::new(
                PgqLoweringErrorCode::InvalidBoundPlan,
                self.span(slot),
                format!("bound graph slot {} has no logical variable", slot.0),
            )
        })
    }

    fn span(&self, slot: PgqSlotId) -> Span {
        self.variables
            .get(&slot)
            .map_or(Span::new(0, 0), |variable| variable.span)
    }
}

fn unique_internal_name(base: String, used_names: &mut BTreeSet<String>) -> String {
    if used_names.insert(base.clone()) {
        return base;
    }
    let mut suffix = 1u32;
    loop {
        let candidate = format!("{base}_{suffix}");
        if used_names.insert(candidate.clone()) {
            return candidate;
        }
        suffix = suffix.saturating_add(1);
    }
}

fn primary_slot(primary: &BoundPgqPathPrimary) -> PgqSlotId {
    match primary {
        BoundPgqPathPrimary::Vertex { slot, .. } | BoundPgqPathPrimary::Edge { slot, .. } => *slot,
        BoundPgqPathPrimary::Parenthesized { path, .. } => path
            .factors
            .first()
            .map_or(PgqSlotId(u32::MAX), |factor| primary_slot(&factor.primary)),
    }
}

fn executable_label(variable: &BoundPgqVariable) -> Result<String, PgqLoweringError> {
    match variable.labels.as_slice() {
        [] => Ok(String::new()),
        [label] => Ok(label.clone()),
        labels => Err(PgqLoweringError::new(
            PgqLoweringErrorCode::UnsupportedLabels,
            variable.span,
            format!(
                "the shared graph scan supports one exact label per element, found {} labels",
                labels.len()
            ),
        )),
    }
}

fn lower_direction(direction: GraphEdgeDirection) -> RelationshipDirection {
    match direction {
        GraphEdgeDirection::Left => RelationshipDirection::Incoming,
        GraphEdgeDirection::Right => RelationshipDirection::Outgoing,
        GraphEdgeDirection::Any => RelationshipDirection::Undirected,
    }
}

fn filter_plan(input: LogicalPlan, predicate: Predicate) -> LogicalPlan {
    LogicalPlan::Filter {
        predicate,
        input: Box::new(input),
    }
}

fn and_predicates(mut predicates: Vec<Predicate>) -> Predicate {
    if predicates.len() == 1 {
        predicates.pop().expect("one predicate remains")
    } else {
        Predicate::And(predicates)
    }
}

fn lower_predicate(
    expression: &BoundPgqExpression,
    slots: &SlotBindings<'_>,
    parameters: &PgqLoweringParameters,
) -> Result<Predicate, PgqLoweringError> {
    match &expression.kind {
        BoundPgqExpressionKind::Literal(BoundPgqLiteral::Boolean(value)) => {
            Ok(Predicate::ConstantBool(*value))
        }
        BoundPgqExpressionKind::Parameter(position) => {
            match parameter(parameters, *position, expression.span)? {
                Value::Bool(value) => Ok(Predicate::ConstantBool(value)),
                value => Err(unsupported_expression(
                    expression.span,
                    format!("boolean predicate parameter ${position} resolved to {value:?}"),
                )),
            }
        }
        BoundPgqExpressionKind::Property { slot, property } => Ok(Predicate::PropertyEq {
            variable: slots.name(*slot)?.to_string(),
            property: property.clone(),
            value: Value::Bool(true),
        }),
        BoundPgqExpressionKind::Unary {
            operator: UnaryOperatorSyntax::Not,
            expression,
        } => Ok(Predicate::Not(Box::new(lower_predicate(
            expression, slots, parameters,
        )?))),
        BoundPgqExpressionKind::Binary {
            left,
            operator: BinaryOperatorSyntax::And,
            right,
        } => Ok(Predicate::And(vec![
            lower_predicate(left, slots, parameters)?,
            lower_predicate(right, slots, parameters)?,
        ])),
        BoundPgqExpressionKind::Binary {
            left,
            operator: BinaryOperatorSyntax::Or,
            right,
        } => Ok(Predicate::Or(vec![
            lower_predicate(left, slots, parameters)?,
            lower_predicate(right, slots, parameters)?,
        ])),
        BoundPgqExpressionKind::Binary {
            left,
            operator,
            right,
        } if is_comparison(*operator) => {
            lower_comparison(left, *operator, right, slots, parameters)
        }
        BoundPgqExpressionKind::IsNull {
            expression: inner,
            negated,
        } => {
            let BoundPgqExpressionKind::Property { slot, property } = &inner.kind else {
                return Err(unsupported_expression(
                    expression.span,
                    "IS NULL currently requires a graph property",
                ));
            };
            if *negated {
                Ok(Predicate::PropertyIsNotNull {
                    variable: slots.name(*slot)?.to_string(),
                    property: property.clone(),
                })
            } else {
                Ok(Predicate::PropertyIsNull {
                    variable: slots.name(*slot)?.to_string(),
                    property: property.clone(),
                })
            }
        }
        BoundPgqExpressionKind::InList {
            expression: inner,
            values,
            negated,
        } => {
            let BoundPgqExpressionKind::Property { slot, property } = &inner.kind else {
                return Err(unsupported_expression(
                    expression.span,
                    "IN currently requires a graph property on the left",
                ));
            };
            let values = values
                .iter()
                .map(|value| lower_constant(value, parameters))
                .collect::<Result<Vec<_>, _>>()?;
            let predicate = Predicate::PropertyIn {
                variable: slots.name(*slot)?.to_string(),
                property: property.clone(),
                values,
            };
            if *negated {
                Ok(Predicate::Not(Box::new(predicate)))
            } else {
                Ok(predicate)
            }
        }
        BoundPgqExpressionKind::Between {
            expression: inner,
            low,
            high,
            negated,
        } => {
            let predicate = Predicate::And(vec![
                lower_comparison(
                    inner,
                    BinaryOperatorSyntax::GreaterOrEqual,
                    low,
                    slots,
                    parameters,
                )?,
                lower_comparison(
                    inner,
                    BinaryOperatorSyntax::LessOrEqual,
                    high,
                    slots,
                    parameters,
                )?,
            ]);
            if *negated {
                Ok(Predicate::Not(Box::new(predicate)))
            } else {
                Ok(predicate)
            }
        }
        _ => Err(unsupported_expression(
            expression.span,
            "expression has no equivalent shared graph predicate",
        )),
    }
}

fn lower_comparison(
    left: &BoundPgqExpression,
    operator: BinaryOperatorSyntax,
    right: &BoundPgqExpression,
    slots: &SlotBindings<'_>,
    parameters: &PgqLoweringParameters,
) -> Result<Predicate, PgqLoweringError> {
    let comparison_span = Span::new(left.span.start, right.span.end);
    let left = lower_projection(left, slots, parameters)?;
    let right = lower_projection(right, slots, parameters)?;
    Ok(match operator {
        BinaryOperatorSyntax::Equal => Predicate::ExpressionEq {
            expression: left,
            value: right,
        },
        BinaryOperatorSyntax::NotEqual => Predicate::ExpressionNotEq {
            expression: left,
            value: right,
        },
        BinaryOperatorSyntax::Less => Predicate::ExpressionCompare {
            expression: left,
            op: ComparisonOp::Lt,
            value: right,
        },
        BinaryOperatorSyntax::LessOrEqual => Predicate::ExpressionCompare {
            expression: left,
            op: ComparisonOp::Lte,
            value: right,
        },
        BinaryOperatorSyntax::Greater => Predicate::ExpressionCompare {
            expression: left,
            op: ComparisonOp::Gt,
            value: right,
        },
        BinaryOperatorSyntax::GreaterOrEqual => Predicate::ExpressionCompare {
            expression: left,
            op: ComparisonOp::Gte,
            value: right,
        },
        _ => {
            return Err(unsupported_expression(
                comparison_span,
                "non-comparison operator reached comparison lowering",
            ));
        }
    })
}

fn lower_projection(
    expression: &BoundPgqExpression,
    slots: &SlotBindings<'_>,
    parameters: &PgqLoweringParameters,
) -> Result<ProjectionExpression, PgqLoweringError> {
    match &expression.kind {
        BoundPgqExpressionKind::Variable(slot) => Ok(ProjectionExpression::Variable {
            variable: slots.name(*slot)?.to_string(),
        }),
        BoundPgqExpressionKind::Property { slot, property } => Ok(ProjectionExpression::Property {
            variable: slots.name(*slot)?.to_string(),
            property: property.clone(),
        }),
        BoundPgqExpressionKind::Parameter(position) => Ok(ProjectionExpression::Literal(
            parameter(parameters, *position, expression.span)?,
        )),
        BoundPgqExpressionKind::Literal(_) => Ok(ProjectionExpression::Literal(lower_constant(
            expression, parameters,
        )?)),
        BoundPgqExpressionKind::Unary {
            operator: UnaryOperatorSyntax::Plus,
            expression,
        } => lower_projection(expression, slots, parameters),
        BoundPgqExpressionKind::Unary {
            operator: UnaryOperatorSyntax::Minus,
            expression: inner,
        } => match lower_constant(inner, parameters)? {
            Value::Int(value) => value
                .checked_neg()
                .map(Value::Int)
                .map(ProjectionExpression::Literal)
                .ok_or_else(|| {
                    unsupported_expression(expression.span, "integer negation overflows")
                }),
            Value::Float(value) => Ok(ProjectionExpression::Literal(Value::Float(-value))),
            _ => Err(unsupported_expression(
                expression.span,
                "unary minus currently requires a numeric constant",
            )),
        },
        BoundPgqExpressionKind::Function {
            name,
            arguments,
            distinct: false,
        } if name
            .last()
            .is_some_and(|name| name.eq_ignore_ascii_case("lower"))
            && arguments.len() == 1 =>
        {
            Ok(ProjectionExpression::Lower(Box::new(lower_projection(
                &arguments[0],
                slots,
                parameters,
            )?)))
        }
        _ => Err(unsupported_expression(
            expression.span,
            "expression has no equivalent shared graph projection",
        )),
    }
}

fn lower_constant(
    expression: &BoundPgqExpression,
    parameters: &PgqLoweringParameters,
) -> Result<Value, PgqLoweringError> {
    match &expression.kind {
        BoundPgqExpressionKind::Parameter(position) => {
            parameter(parameters, *position, expression.span)
        }
        BoundPgqExpressionKind::Literal(literal) => Ok(match literal {
            BoundPgqLiteral::Null => Value::Null,
            BoundPgqLiteral::Boolean(value) => Value::Bool(*value),
            BoundPgqLiteral::Int64(value) => Value::Int(*value),
            BoundPgqLiteral::Float64(value) => Value::Float(*value),
            BoundPgqLiteral::String(value) => Value::String(value.clone()),
            BoundPgqLiteral::TypedString { value, .. } => Value::String(value.clone()),
        }),
        _ => Err(unsupported_expression(
            expression.span,
            "a constant literal or bound parameter is required",
        )),
    }
}

fn parameter(
    parameters: &PgqLoweringParameters,
    position: u32,
    span: Span,
) -> Result<Value, PgqLoweringError> {
    parameters.get(&position).cloned().ok_or_else(|| {
        PgqLoweringError::new(
            PgqLoweringErrorCode::MissingParameter,
            span,
            format!("PostgreSQL parameter ${position} is not bound"),
        )
    })
}

fn is_comparison(operator: BinaryOperatorSyntax) -> bool {
    matches!(
        operator,
        BinaryOperatorSyntax::Equal
            | BinaryOperatorSyntax::NotEqual
            | BinaryOperatorSyntax::Less
            | BinaryOperatorSyntax::LessOrEqual
            | BinaryOperatorSyntax::Greater
            | BinaryOperatorSyntax::GreaterOrEqual
    )
}

fn unsupported_expression(span: Span, message: impl Into<String>) -> PgqLoweringError {
    PgqLoweringError::new(PgqLoweringErrorCode::UnsupportedExpression, span, message)
}

#[cfg(test)]
mod tests;
