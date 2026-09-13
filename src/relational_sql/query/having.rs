use super::aggregate_state::{
    aggregate_expression_base_memory_bytes, sql_expression_memory_bytes, AggregateExpressionState,
    AggregateMemoryDelta,
};
use super::expression::predicate_truth_with;
use super::*;
use crate::sql::{Expr, ExprKind};

mod binding;
use binding::{HavingBindings, ScalarState};

#[derive(Clone)]
pub(super) struct HavingState {
    predicate: Expr,
    slots: Vec<ScalarState>,
}

pub(super) fn validate_having(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<()> {
    if select.having.is_none() {
        return Ok(());
    }
    if select.lock_strength.is_some() {
        return Err(SkeinError::Semantic(
            "HAVING does not support row locking".into(),
        ));
    }
    let bindings = HavingBindings::new(select, parameters, state)?;
    for projection in &select.projection {
        match projection {
            SelectProjection::Expression { expression, .. } => {
                bindings.scalar(expression, true)?;
            }
            SelectProjection::Wildcard => {
                return Err(SkeinError::Semantic(
                    "aggregate SELECT does not support wildcard projection".into(),
                ))
            }
        }
    }
    HavingState::new(select, parameters, state)?;
    Ok(())
}

pub(super) fn projection_template(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<Vec<AggregateProjectionState>> {
    let Some(having) = HavingState::new(select, parameters, state)? else {
        return select
            .projection
            .iter()
            .map(|projection| AggregateProjectionState::new(projection, parameters))
            .collect();
    };
    let bindings = HavingBindings::new(select, parameters, state)?;
    let mut projections = select
        .projection
        .iter()
        .map(|projection| {
            let SelectProjection::Expression { expression, alias } = projection else {
                return Err(SkeinError::Semantic(
                    "aggregate SELECT does not support wildcard projection".into(),
                ));
            };
            Ok(AggregateProjectionState {
                name: alias.clone().unwrap_or_else(|| expression_name(expression)),
                expression: bindings.scalar(expression, true)?.state,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    projections.push(AggregateProjectionState {
        name: String::new(),
        expression: AggregateExpressionState::Having(Box::new(having)),
    });
    Ok(projections)
}

pub(super) fn filter_group(
    mut projections: Vec<AggregateProjectionState>,
) -> Result<Option<Vec<AggregateProjectionState>>> {
    if let Some(projection) = projections
        .pop_if(|projection| matches!(projection.expression, AggregateExpressionState::Having(_)))
    {
        let AggregateExpressionState::Having(having) = projection.expression else {
            unreachable!("HAVING item checked")
        };
        if !having.finish()? {
            return Ok(None);
        }
    }
    Ok(Some(projections))
}

impl HavingState {
    pub(super) fn new(
        select: &SelectStatement,
        parameters: &[Value],
        state: &RelationalState,
    ) -> Result<Option<Self>> {
        let Some(predicate) = &select.having else {
            return Ok(None);
        };
        let bindings = HavingBindings::new(select, parameters, state)?;
        let mut compiler = Compiler {
            bindings: &bindings,
            slots: Vec::new(),
            grouped: true,
        };
        let predicate = compiler.predicate(predicate)?;
        Ok(Some(Self {
            predicate,
            slots: compiler.slots,
        }))
    }

    pub(super) fn base_memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(sql_expression_memory_bytes(&self.predicate))
            .saturating_add(self.slots.len().saturating_mul(
                std::mem::size_of::<ScalarState>()
                    + std::mem::size_of::<(RelationalValue, RelationalScalarType)>(),
            ))
            .saturating_add(
                self.slots
                    .iter()
                    .map(|slot| {
                        aggregate_expression_base_memory_bytes(&slot.state)
                            .saturating_add(slot.output_memory_bytes())
                    })
                    .fold(0usize, usize::saturating_add),
            )
    }

    pub(super) fn update(
        &mut self,
        row: &BoundRow<'_>,
        parameters: &[Value],
    ) -> Result<AggregateMemoryDelta> {
        let mut delta = AggregateMemoryDelta::default();
        for slot in &mut self.slots {
            let before = slot.output_memory_bytes();
            delta.combine(slot.state.update(row, parameters)?);
            delta.combine(AggregateMemoryDelta::between(
                before,
                slot.output_memory_bytes(),
            ));
        }
        Ok(delta)
    }

    pub(super) fn finish(self) -> Result<bool> {
        let mut values = Vec::with_capacity(self.slots.len());
        for slot in self.slots {
            let value = slot.state.finish()?;
            values.push((
                binding::coerce_value(value, slot.scalar_type)?,
                slot.scalar_type.unwrap_or(RelationalScalarType::Boolean),
            ));
        }
        let truth = predicate_truth_with(&self.predicate, &[], &|column| {
            let slot = column
                .name
                .parse::<usize>()
                .ok()
                .and_then(|index| values.get(index))
                .ok_or_else(|| SkeinError::Execution("invalid HAVING value slot".into()))?;
            Ok((&slot.0, slot.1))
        })?;
        Ok(truth == Some(true))
    }
}

struct Compiler<'a, 'state> {
    bindings: &'a HavingBindings<'state>,
    slots: Vec<ScalarState>,
    grouped: bool,
}

impl Compiler<'_, '_> {
    fn slot(
        &mut self,
        mut scalar: ScalarState,
        target: Option<RelationalScalarType>,
        span: crate::sql::SqlSourceSpan,
    ) -> Result<Expr> {
        scalar.coerce(target)?;
        let index = self.slots.len();
        self.slots.push(scalar);
        Ok(Expr {
            kind: ExprKind::Column(SqlColumnRef {
                qualifier: None,
                name: index.to_string(),
            }),
            span,
        })
    }

    fn predicate(&mut self, predicate: &Expr) -> Result<Expr> {
        let kind = match &predicate.kind {
            ExprKind::And(left, right) => ExprKind::And(
                Box::new(self.predicate(left)?),
                Box::new(self.predicate(right)?),
            ),
            ExprKind::Or(left, right) => ExprKind::Or(
                Box::new(self.predicate(left)?),
                Box::new(self.predicate(right)?),
            ),
            ExprKind::Not(inner) => ExprKind::Not(Box::new(self.predicate(inner)?)),
            ExprKind::Compare { left, op, right } => {
                let left_state = self.bindings.scalar(left, self.grouped)?;
                let right_state = self.bindings.scalar(right, self.grouped)?;
                let target = binding::comparison_type(&[&left_state, &right_state])?;
                ExprKind::Compare {
                    left: Box::new(self.slot(left_state, target, left.span)?),
                    op: *op,
                    right: Box::new(self.slot(right_state, target, right.span)?),
                }
            }
            ExprKind::IsNull {
                expression,
                negated,
            } => {
                let state = self.bindings.scalar(expression, self.grouped)?;
                let target = state.scalar_type;
                ExprKind::IsNull {
                    expression: Box::new(self.slot(state, target, expression.span)?),
                    negated: *negated,
                }
            }
            ExprKind::InList {
                left,
                values,
                negated,
            } => {
                let left_state = self.bindings.scalar(left, self.grouped)?;
                let states = values
                    .iter()
                    .map(|value| self.bindings.scalar(value, self.grouped))
                    .collect::<Result<Vec<_>>>()?;
                let inputs = std::iter::once(&left_state)
                    .chain(states.iter())
                    .collect::<Vec<_>>();
                let target = binding::comparison_type(&inputs)?;
                ExprKind::InList {
                    left: Box::new(self.slot(left_state, target, left.span)?),
                    values: states
                        .into_iter()
                        .zip(values)
                        .map(|(state, value)| self.slot(state, target, value.span))
                        .collect::<Result<_>>()?,
                    negated: *negated,
                }
            }
            ExprKind::Like {
                left,
                pattern,
                case_insensitive,
                negated,
                escape,
            } => {
                let left_state = self.bindings.scalar(left, self.grouped)?;
                let pattern_state = self.bindings.scalar(pattern, self.grouped)?;
                ExprKind::Like {
                    left: Box::new(self.slot(
                        left_state,
                        Some(RelationalScalarType::Text),
                        left.span,
                    )?),
                    pattern: Box::new(self.slot(
                        pattern_state,
                        Some(RelationalScalarType::Text),
                        pattern.span,
                    )?),
                    case_insensitive: *case_insensitive,
                    negated: *negated,
                    escape: *escape,
                }
            }
            _ => {
                let state = self.bindings.scalar(predicate, self.grouped)?;
                ExprKind::Compare {
                    left: Box::new(self.slot(
                        state,
                        Some(RelationalScalarType::Boolean),
                        predicate.span,
                    )?),
                    op: SqlComparisonOp::Eq,
                    right: Box::new(Expr::value(SqlValue::Literal(Value::Bool(true)))),
                }
            }
        };
        Ok(Expr {
            kind,
            span: predicate.span,
        })
    }
}
