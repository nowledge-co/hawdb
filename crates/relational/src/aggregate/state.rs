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

#[derive(Clone)]
pub struct AggregateProjectionState {
    pub(super) name: String,
    pub(super) expression: AggregateExpressionState,
}

impl AggregateProjectionState {
    pub(super) fn new(projection: &SelectProjection, parameters: &[Value]) -> Result<Self> {
        match projection {
            SelectProjection::Expression {
                expression:
                    Expr {
                        kind: ExprKind::Column(name),
                        ..
                    },
                alias,
                ..
            } => Ok(Self {
                name: alias.clone().unwrap_or_else(|| name.name.clone()),
                expression: AggregateExpressionState::First {
                    column: name.clone(),
                    value: None,
                },
            }),
            SelectProjection::Expression { expression, alias } => Ok(Self {
                name: alias.clone().unwrap_or_else(|| expression_name(expression)),
                expression: AggregateExpressionState::new(expression, parameters)?,
            }),
            SelectProjection::Wildcard => Err(HawDBError::Semantic(
                "aggregate SELECT does not support wildcard projection".to_string(),
            )),
        }
    }

    pub fn update<'a>(
        &mut self,
        row: &impl Fn(&SqlColumnRef) -> Result<(&'a RelationalValue, RelationalScalarType)>,
        parameters: &[Value],
    ) -> Result<AggregateMemoryDelta> {
        self.expression.update(row, parameters)
    }

    pub fn finish(self) -> Result<(String, Value)> {
        Ok((self.name, self.expression.finish()?))
    }
}

#[derive(Clone)]
pub(super) enum AggregateExpressionState {
    // A typed group filter shares the existing accounting without adding fields
    // to every projection or exposing a synthetic output-column name.
    Having(Box<super::having::HavingState>),
    Constant(Value),
    First {
        column: SqlColumnRef,
        value: Option<RelationalValue>,
    },
    Count {
        column: Option<SqlColumnRef>,
        filter: Option<SqlPredicate>,
        count: usize,
        distinct: Option<BTreeSet<RelationalValue>>,
    },
    Numeric {
        expression: SqlExpression,
        filter: Option<SqlPredicate>,
        aggregate: NumericAggregate,
        value: Option<RelationalValue>,
        distinct: Option<BTreeSet<RelationalValue>>,
    },
    Coalesce(Vec<AggregateExpressionState>),
    Arithmetic {
        left: Box<Self>,
        op: hawdb_core::numeric::NumericBinaryOp,
        right: Box<Self>,
    },
    Negate(Box<Self>),
    Cast {
        expression: Box<Self>,
        target: hawdb_core::numeric::NumericType,
    },
}

#[derive(Clone, Copy)]
pub(super) enum NumericAggregate {
    Sum,
    Min,
    Max,
}

#[derive(Default)]
pub struct AggregateMemoryDelta {
    pub added_bytes: usize,
    pub released_bytes: usize,
}

impl AggregateMemoryDelta {
    pub(super) fn between(previous: usize, next: usize) -> Self {
        if next >= previous {
            Self {
                added_bytes: next - previous,
                released_bytes: 0,
            }
        } else {
            Self {
                added_bytes: 0,
                released_bytes: previous - next,
            }
        }
    }

    pub(super) fn combine(&mut self, other: Self) {
        self.added_bytes = self.added_bytes.saturating_add(other.added_bytes);
        self.released_bytes = self.released_bytes.saturating_add(other.released_bytes);
    }
}

impl AggregateExpressionState {
    pub(super) fn new(expression: &SqlExpression, parameters: &[Value]) -> Result<Self> {
        match expression {
            Expr {
                kind: ExprKind::Arithmetic { left, op, right },
                ..
            } => Ok(Self::Arithmetic {
                left: Box::new(Self::new(left, parameters)?),
                op: *op,
                right: Box::new(Self::new(right, parameters)?),
            }),
            Expr {
                kind: ExprKind::Negate(expression),
                ..
            } => Ok(Self::Negate(Box::new(Self::new(expression, parameters)?))),
            Expr {
                kind: ExprKind::Cast { expression, target },
                ..
            } => Ok(Self::Cast {
                expression: Box::new(Self::new(expression, parameters)?),
                target: *target,
            }),
            Expr {
                kind: ExprKind::Value(value),
                ..
            } => Ok(Self::Constant(bind_sql_value(value, parameters)?)),
            Expr {
                kind: ExprKind::Column(column),
                ..
            } => Ok(Self::First {
                column: column.clone(),
                value: None,
            }),
            Expr {
                kind:
                    ExprKind::Function {
                        name,
                        arguments,
                        distinct,
                        filter,
                    },
                ..
            } => match name.as_str() {
                "count" => {
                    let [argument] = arguments.as_slice() else {
                        return Err(HawDBError::Semantic(
                            "COUNT requires exactly one argument".to_string(),
                        ));
                    };
                    let column = match argument {
                        SqlFunctionArgument::Wildcard => None,
                        SqlFunctionArgument::Expression(Expr {
                            kind: ExprKind::Column(column),
                            ..
                        }) => Some(column.clone()),
                        _ => {
                            return Err(HawDBError::Semantic(
                                "COUNT supports wildcard or a column argument".to_string(),
                            ))
                        }
                    };
                    if *distinct && column.is_none() {
                        return Err(HawDBError::Semantic(
                            "COUNT(DISTINCT *) is not supported".to_string(),
                        ));
                    }
                    Ok(Self::Count {
                        column,
                        filter: filter.as_deref().cloned(),
                        count: 0,
                        distinct: distinct.then(BTreeSet::new),
                    })
                }
                "sum" | "min" | "max" => {
                    let [SqlFunctionArgument::Expression(expression)] = arguments.as_slice() else {
                        return Err(HawDBError::Semantic(
                            "numeric aggregate requires exactly one expression".to_string(),
                        ));
                    };
                    Ok(Self::Numeric {
                        expression: bind_row_parameters(expression, parameters)?,
                        filter: filter.as_deref().cloned(),
                        aggregate: match name.as_str() {
                            "sum" => NumericAggregate::Sum,
                            "min" => NumericAggregate::Min,
                            _ => NumericAggregate::Max,
                        },
                        value: None,
                        distinct: distinct.then(BTreeSet::new),
                    })
                }
                "coalesce" => {
                    if *distinct {
                        return Err(HawDBError::Semantic(
                            "COALESCE does not accept DISTINCT".to_string(),
                        ));
                    }
                    let states = arguments
                        .iter()
                        .map(|argument| {
                            let SqlFunctionArgument::Expression(expression) = argument else {
                                return Err(HawDBError::Semantic(
                                    "COALESCE does not accept wildcard".to_string(),
                                ));
                            };
                            Self::new(expression, parameters)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Self::Coalesce(states))
                }
                _ => Err(HawDBError::Semantic(format!(
                    "unsupported relational aggregate function {name}"
                ))),
            },
            _ => Err(HawDBError::Semantic(
                "unsupported aggregate expression".to_owned(),
            )),
        }
    }

    pub(super) fn update<'a>(
        &mut self,
        row: &impl Fn(&SqlColumnRef) -> Result<(&'a RelationalValue, RelationalScalarType)>,
        parameters: &[Value],
    ) -> Result<AggregateMemoryDelta> {
        match self {
            Self::Arithmetic { left, right, .. } => {
                let mut delta = left.update(row, parameters)?;
                delta.combine(right.update(row, parameters)?);
                Ok(delta)
            }
            Self::Negate(expression) | Self::Cast { expression, .. } => {
                expression.update(row, parameters)
            }
            Self::Having(state) => state.update(row, parameters),
            Self::Constant(_) => Ok(AggregateMemoryDelta::default()),
            Self::First { column, value } => {
                if value.is_none() {
                    let first = resolve_column(row, column)?.clone();
                    let added_bytes = relational_value_memory_bytes(&first);
                    *value = Some(first);
                    return Ok(AggregateMemoryDelta {
                        added_bytes,
                        released_bytes: 0,
                    });
                }
                Ok(AggregateMemoryDelta::default())
            }
            Self::Count {
                column,
                filter,
                count,
                distinct,
            } => {
                if !aggregate_filter_matches(filter.as_ref(), row, parameters)? {
                    return Ok(AggregateMemoryDelta::default());
                }
                let value = column
                    .as_ref()
                    .map(|column| resolve_column(row, column).cloned())
                    .transpose()?;
                if value
                    .as_ref()
                    .is_some_and(|value| matches!(value, RelationalValue::Null))
                {
                    return Ok(AggregateMemoryDelta::default());
                }
                if let Some(distinct) = distinct {
                    let value = value.expect("DISTINCT COUNT has a column");
                    let value_bytes = relational_value_memory_bytes(&value)
                        .saturating_add(std::mem::size_of::<usize>() * 4);
                    if distinct.insert(value) {
                        *count = count.saturating_add(1);
                        return Ok(AggregateMemoryDelta {
                            added_bytes: value_bytes,
                            released_bytes: 0,
                        });
                    }
                } else {
                    *count = count.saturating_add(1);
                }
                Ok(AggregateMemoryDelta::default())
            }
            Self::Numeric {
                expression,
                filter,
                aggregate,
                value,
                distinct,
            } => {
                if !aggregate_filter_matches(filter.as_ref(), row, parameters)? {
                    return Ok(AggregateMemoryDelta::default());
                }
                let candidate = evaluate_row_expression(expression, row)?;
                if matches!(candidate, RelationalValue::Null) {
                    return Ok(AggregateMemoryDelta::default());
                }
                let mut delta = AggregateMemoryDelta::default();
                if let Some(distinct) = distinct {
                    if !distinct.insert(candidate.clone()) {
                        return Ok(delta);
                    }
                    delta.added_bytes = delta.added_bytes.saturating_add(
                        relational_value_memory_bytes(&candidate)
                            .saturating_add(std::mem::size_of::<usize>() * 4),
                    );
                }
                delta.combine(update_numeric_aggregate(value, candidate, *aggregate)?);
                Ok(delta)
            }
            Self::Coalesce(states) => {
                let mut delta = AggregateMemoryDelta::default();
                for state in states {
                    delta.combine(state.update(row, parameters)?);
                }
                Ok(delta)
            }
        }
    }

    pub(super) fn finish(self) -> Result<Value> {
        match self {
            Self::Arithmetic { left, op, right } => hawdb_core::numeric::evaluate_numeric(
                left.finish()?.as_ref(),
                op,
                right.finish()?.as_ref(),
            ),
            Self::Negate(expression) => {
                hawdb_core::numeric::negate_numeric(expression.finish()?.as_ref())
            }
            Self::Cast { expression, target } => {
                hawdb_core::numeric::cast_numeric(expression.finish()?.as_ref(), target)
            }
            Self::Having(_) => Err(HawDBError::Execution(
                "HAVING state reached output projection".into(),
            )),
            Self::Constant(value) => Ok(value),
            Self::First {
                value: Some(value), ..
            } => relational_to_value(&value),
            Self::First { value: None, .. } => Err(HawDBError::Semantic(
                "aggregate column has no input row".to_string(),
            )),
            Self::Count { count, .. } => Ok(Value::Int(i64::try_from(count).unwrap_or(i64::MAX))),
            Self::Numeric { value, .. } => {
                value.map_or(Ok(Value::Null), |value| relational_to_value(&value))
            }
            Self::Coalesce(states) => {
                for state in states {
                    let value = state.finish()?;
                    if value != Value::Null {
                        return Ok(value);
                    }
                }
                Ok(Value::Null)
            }
        }
    }
}

pub(super) fn update_numeric_aggregate(
    result: &mut Option<RelationalValue>,
    candidate: RelationalValue,
    aggregate: NumericAggregate,
) -> Result<AggregateMemoryDelta> {
    let previous_bytes = result.as_ref().map_or(0, relational_value_memory_bytes);
    match aggregate {
        NumericAggregate::Sum => {
            if !candidate
                .logical_type()
                .is_some_and(|kind| kind.is_numeric())
            {
                return Err(HawDBError::Semantic("SUM requires numeric input".into()));
            }
            let next = match result.as_ref() {
                None => candidate,
                Some(total) => value_to_relational(hawdb_core::numeric::evaluate_numeric(
                    crate::query_value::core_value_ref(total.as_ref())?,
                    hawdb_core::numeric::NumericBinaryOp::Add,
                    crate::query_value::core_value_ref(candidate.as_ref())?,
                )?)?,
            };
            *result = Some(next);
        }
        NumericAggregate::Min | NumericAggregate::Max => {
            let replace = match result.as_ref() {
                None => true,
                Some(current) => {
                    let ordering = if current.logical_type().is_some_and(|kind| kind.is_numeric())
                        && candidate
                            .logical_type()
                            .is_some_and(|kind| kind.is_numeric())
                    {
                        hawdb_core::numeric::compare_numeric(
                            crate::query_value::core_value_ref(candidate.as_ref())?,
                            crate::query_value::core_value_ref(current.as_ref())?,
                        )?
                    } else {
                        candidate.cmp(current)
                    };
                    match aggregate {
                        NumericAggregate::Min => ordering.is_lt(),
                        NumericAggregate::Max => ordering.is_gt(),
                        NumericAggregate::Sum => unreachable!(),
                    }
                }
            };
            if replace {
                *result = Some(candidate);
            }
        }
    }
    let next_bytes = result.as_ref().map_or(0, relational_value_memory_bytes);
    Ok(AggregateMemoryDelta::between(previous_bytes, next_bytes))
}

pub fn charge_aggregate_memory(bytes: usize, tracker: &mut OperatorMemoryTracker) -> Result<()> {
    ensure_operator_item_fits("RelationalAggregateExec", bytes, tracker)?;
    if tracker.would_exceed(bytes) {
        return Err(HawDBError::Execution(format!(
            "RelationalAggregateExec state exceeds blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    tracker.try_charge(bytes)?;
    Ok(())
}

pub fn aggregate_group_base_memory_bytes(
    key: &[RelationalValue],
    projections: &[AggregateProjectionState],
) -> usize {
    let key_bytes = key.iter().fold(
        std::mem::size_of::<Vec<RelationalValue>>(),
        |total, value| total.saturating_add(relational_value_memory_bytes(value)),
    );
    projections.iter().fold(
        key_bytes
            .saturating_add(std::mem::size_of::<Vec<AggregateProjectionState>>())
            .saturating_add(std::mem::size_of::<usize>() * 6),
        |total, projection| {
            total
                .saturating_add(std::mem::size_of::<AggregateProjectionState>())
                .saturating_add(projection.name.len())
                .saturating_add(aggregate_expression_base_memory_bytes(
                    &projection.expression,
                ))
        },
    )
}

pub(super) fn aggregate_expression_base_memory_bytes(state: &AggregateExpressionState) -> usize {
    let state_bytes = std::mem::size_of::<AggregateExpressionState>();
    state_bytes.saturating_add(match state {
        AggregateExpressionState::Arithmetic { left, right, .. } => {
            aggregate_expression_base_memory_bytes(left)
                .saturating_add(aggregate_expression_base_memory_bytes(right))
        }
        AggregateExpressionState::Negate(expression)
        | AggregateExpressionState::Cast { expression, .. } => {
            aggregate_expression_base_memory_bytes(expression)
        }
        AggregateExpressionState::Having(state) => state.base_memory_bytes(),
        AggregateExpressionState::Constant(value) => {
            hawdb_executor::binding::value_memory_bytes(value)
        }
        AggregateExpressionState::First { column, value } => column_ref_memory_bytes(column)
            .saturating_add(value.as_ref().map_or(0, relational_value_memory_bytes)),
        AggregateExpressionState::Count {
            column,
            filter,
            distinct,
            ..
        } => column
            .as_ref()
            .map_or(0, column_ref_memory_bytes)
            .saturating_add(filter.as_ref().map_or(0, sql_predicate_memory_bytes))
            .saturating_add(
                distinct
                    .as_ref()
                    .map_or(0, |_| std::mem::size_of::<BTreeSet<RelationalValue>>()),
            ),
        AggregateExpressionState::Numeric {
            expression,
            filter,
            value,
            distinct,
            ..
        } => sql_expression_memory_bytes(expression)
            .saturating_add(filter.as_ref().map_or(0, sql_predicate_memory_bytes))
            .saturating_add(value.as_ref().map_or(0, relational_value_memory_bytes))
            .saturating_add(
                distinct
                    .as_ref()
                    .map_or(0, |_| std::mem::size_of::<BTreeSet<RelationalValue>>()),
            ),
        AggregateExpressionState::Coalesce(states) => states.iter().fold(
            std::mem::size_of::<Vec<AggregateExpressionState>>(),
            |total, state| total.saturating_add(aggregate_expression_base_memory_bytes(state)),
        ),
    })
}

pub(super) fn sql_expression_memory_bytes(expression: &SqlExpression) -> usize {
    let children = match &expression.kind {
        ExprKind::Column(column) => column_ref_memory_bytes(column),
        ExprKind::Value(value) => sql_value_memory_bytes(value),
        ExprKind::Function {
            name,
            arguments,
            filter,
            ..
        } => arguments
            .iter()
            .fold(
                name.len()
                    .saturating_add(std::mem::size_of::<Vec<SqlFunctionArgument>>()),
                |total, argument| match argument {
                    SqlFunctionArgument::Expression(expression) => {
                        total.saturating_add(sql_expression_memory_bytes(expression))
                    }
                    SqlFunctionArgument::Wildcard => total,
                },
            )
            .saturating_add(filter.as_deref().map_or(0, sql_expression_memory_bytes)),
        ExprKind::And(left, right)
        | ExprKind::Or(left, right)
        | ExprKind::Compare { left, right, .. }
        | ExprKind::Arithmetic { left, right, .. } => {
            sql_expression_memory_bytes(left).saturating_add(sql_expression_memory_bytes(right))
        }
        ExprKind::Not(expression)
        | ExprKind::IsNull { expression, .. }
        | ExprKind::Negate(expression)
        | ExprKind::Cast { expression, .. } => sql_expression_memory_bytes(expression),
        ExprKind::InList { left, values, .. } => values.iter().fold(
            sql_expression_memory_bytes(left).saturating_add(std::mem::size_of::<Vec<Expr>>()),
            |total, value| total.saturating_add(sql_expression_memory_bytes(value)),
        ),
        ExprKind::Like { left, pattern, .. } => {
            sql_expression_memory_bytes(left).saturating_add(sql_expression_memory_bytes(pattern))
        }
    };
    std::mem::size_of::<Expr>().saturating_add(children)
}

pub(super) fn sql_predicate_memory_bytes(predicate: &SqlPredicate) -> usize {
    sql_expression_memory_bytes(predicate)
}

pub(super) fn sql_value_memory_bytes(value: &SqlValue) -> usize {
    match value {
        SqlValue::Literal(value) => hawdb_executor::binding::value_memory_bytes(value),
        SqlValue::Parameter(_) => 0,
        SqlValue::Expression(expression) => sql_expression_memory_bytes(expression),
    }
}

pub(super) fn column_ref_memory_bytes(column: &SqlColumnRef) -> usize {
    std::mem::size_of::<SqlColumnRef>()
        .saturating_add(column.name.len())
        .saturating_add(column.qualifier.as_ref().map_or(0, String::len))
}

pub(super) fn relational_value_memory_bytes(value: &RelationalValue) -> usize {
    std::mem::size_of::<RelationalValue>().saturating_add(value.estimated_payload_bytes())
}

fn bind_row_parameters(expression: &Expr, parameters: &[Value]) -> Result<Expr> {
    let mut expression = expression.clone();
    expression.try_visit_mut(&mut |node| -> Result<()> {
        if let ExprKind::Value(value) = &mut node.kind
            && !matches!(value, SqlValue::Literal(_))
        {
            *value = SqlValue::Literal(bind_sql_value(value, parameters)?);
        }
        Ok(())
    })?;
    Ok(expression)
}
