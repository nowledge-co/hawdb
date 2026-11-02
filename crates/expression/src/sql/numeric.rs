// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded binding of constant numeric SQL expressions. Column evaluation stays
//! with the row owner; all arithmetic and conversion use the core kernel.

use super::{Expr, ExprKind, SqlValue};
use hawdb_core::numeric::{cast_numeric, evaluate_numeric, negate_numeric};
use hawdb_core::{HawDBError, Result, Value};

const MAX_DEPTH: usize = 64;
const MAX_NODES: usize = 4096;

pub fn bind_sql_value(value: &SqlValue, parameters: &[Value]) -> Result<Value> {
    Binder {
        parameters,
        remaining: MAX_NODES,
    }
    .value(value, 0)
}

pub fn bind_constant_numeric(expression: &Expr, parameters: &[Value]) -> Result<Value> {
    Binder {
        parameters,
        remaining: MAX_NODES,
    }
    .expression(expression, 0)
}

struct Binder<'a> {
    parameters: &'a [Value],
    remaining: usize,
}

impl Binder<'_> {
    fn admit(&mut self, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH || self.remaining == 0 {
            return Err(HawDBError::Semantic(
                "numeric expression exceeds 64 levels or 4096 nodes".into(),
            ));
        }
        self.remaining -= 1;
        Ok(())
    }

    fn value(&mut self, value: &SqlValue, depth: usize) -> Result<Value> {
        self.admit(depth)?;
        match value {
            SqlValue::Literal(value) => Ok(value.clone()),
            SqlValue::Parameter(position) => position
                .checked_sub(1)
                .and_then(|index| self.parameters.get(index))
                .cloned()
                .ok_or_else(|| {
                    HawDBError::Semantic(format!("missing PostgreSQL parameter ${position}"))
                }),
            SqlValue::Expression(expression) => self.expression(expression, depth + 1),
        }
    }

    fn expression(&mut self, expression: &Expr, depth: usize) -> Result<Value> {
        self.admit(depth)?;
        match &expression.kind {
            ExprKind::Value(value) => self.value(value, depth + 1),
            ExprKind::Arithmetic { left, op, right } => {
                let left = self.expression(left, depth + 1)?;
                let right = self.expression(right, depth + 1)?;
                evaluate_numeric(left.as_ref(), *op, right.as_ref())
            }
            ExprKind::Negate(expression) => {
                negate_numeric(self.expression(expression, depth + 1)?.as_ref())
            }
            ExprKind::Cast { expression, target } => {
                cast_numeric(self.expression(expression, depth + 1)?.as_ref(), *target)
            }
            _ => Err(HawDBError::Semantic(
                "expected a constant numeric expression".into(),
            )),
        }
    }
}
