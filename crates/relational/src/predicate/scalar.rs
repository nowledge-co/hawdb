// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::streaming::bind_streaming_column;
use crate::query_value::{bind_sql_value, core_value_ref, value_to_relational};
use hawdb_core::numeric::{self, NumericBinaryOp, NumericType};
use hawdb_core::{HawDBError, LogicalType, Result, Value};
use hawdb_sql::{Expr, ExprKind};
use hawdb_storage::relational::{
    RelationalScalarType, RelationalTableSchema, RelationalValue, RelationalValueRef,
};

/// Bound once; evaluation borrows leaves and keeps numeric results inline.
pub(super) enum Scalar {
    Column(usize, RelationalScalarType),
    Value(RelationalValue),
    Binary(Box<Self>, NumericBinaryOp, Box<Self>),
    Negate(Box<Self>),
    Cast(Box<Self>, NumericType),
}

impl Scalar {
    pub(super) fn bind(
        expression: &Expr,
        parameters: &[Value],
        schema: &RelationalTableSchema,
        table: &str,
        qualifier: &str,
    ) -> Result<Self> {
        Binder {
            parameters,
            schema,
            table,
            qualifier,
            remaining: 4096,
        }
        .bind(expression, 0)
    }

    pub(super) fn kind(&self) -> Result<Option<LogicalType>> {
        match self {
            Self::Column(_, kind) => Ok(Some(kind.logical_type())),
            Self::Value(value) => Ok(value.logical_type()),
            Self::Binary(left, _, right) => {
                numeric::common_numeric_type(left.kind()?, right.kind()?)
            }
            Self::Negate(value) => numeric::common_numeric_type(value.kind()?, None),
            Self::Cast(value, target) => {
                if value
                    .kind()?
                    .is_some_and(|kind| !kind.is_numeric() && !kind.is_utf8())
                {
                    return Err(HawDBError::Semantic(
                        "CAST requires a numeric or text operand".into(),
                    ));
                }
                Ok(Some(target.logical_type()))
            }
        }
    }

    pub(super) fn validate_comparison(&self, other: &Self) -> Result<()> {
        let (left, right) = (self.kind()?, other.kind()?);
        if left.is_some_and(|kind| kind.is_numeric()) && right.is_some_and(|kind| kind.is_numeric())
        {
            numeric::common_numeric_type(left, right)?;
        } else if left.is_some() && right.is_some() && left != right {
            return Err(HawDBError::Semantic(
                "relational comparison has incompatible scalar types".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn coerce_comparison_literal(&mut self, other: &Self) -> Result<()> {
        if let (Self::Value(value), Self::Column(_, target)) = (self, other)
            && target.logical_type().is_numeric()
        {
            *value = crate::query_value::value_to_relational_as(
                crate::query_value::relational_to_value(value)?,
                *target,
            )?;
        }
        Ok(())
    }

    pub(super) fn evaluate<'a>(
        &'a self,
        resolve: &impl Fn(usize) -> Result<RelationalValueRef<'a>>,
    ) -> Result<RelationalValueRef<'a>> {
        let value = match self {
            Self::Value(value) => return Ok(value.as_ref()),
            Self::Column(index, _) => return resolve(*index),
            Self::Binary(left, op, right) => numeric::evaluate_numeric(
                core_value_ref(left.evaluate(resolve)?)?,
                *op,
                core_value_ref(right.evaluate(resolve)?)?,
            )?,
            Self::Negate(value) => {
                numeric::negate_numeric(core_value_ref(value.evaluate(resolve)?)?)?
            }
            Self::Cast(value, target) => {
                numeric::cast_numeric(core_value_ref(value.evaluate(resolve)?)?, *target)?
            }
        };
        match value {
            Value::Null => Ok(RelationalValueRef::Null),
            Value::Int(value) => Ok(RelationalValueRef::BigInt(value)),
            Value::Float(value) => Ok(RelationalValueRef::DoublePrecision(value)),
            Value::Decimal(value) => Ok(RelationalValueRef::Decimal(value)),
            _ => Err(HawDBError::Execution(
                "numeric kernel returned a nonnumeric value".into(),
            )),
        }
    }
}

struct Binder<'a> {
    parameters: &'a [Value],
    schema: &'a RelationalTableSchema,
    table: &'a str,
    qualifier: &'a str,
    remaining: usize,
}

impl Binder<'_> {
    fn bind(&mut self, expression: &Expr, depth: usize) -> Result<Scalar> {
        if depth > 64 || self.remaining == 0 {
            return Err(HawDBError::Semantic(
                "numeric expression exceeds 64 levels or 4096 nodes".into(),
            ));
        }
        self.remaining -= 1;
        let value = match &expression.kind {
            ExprKind::Column(column) => {
                let index = bind_streaming_column(column, self.schema, self.table, self.qualifier)?;
                Scalar::Column(index, self.schema.columns[index].scalar_type)
            }
            ExprKind::Value(value) => Scalar::Value(value_to_relational(bind_sql_value(
                value,
                self.parameters,
            )?)?),
            ExprKind::Arithmetic { left, op, right } => Scalar::Binary(
                Box::new(self.bind(left, depth + 1)?),
                *op,
                Box::new(self.bind(right, depth + 1)?),
            ),
            ExprKind::Negate(value) => Scalar::Negate(Box::new(self.bind(value, depth + 1)?)),
            ExprKind::Cast { expression, target } => {
                Scalar::Cast(Box::new(self.bind(expression, depth + 1)?), *target)
            }
            _ => {
                return Err(HawDBError::Semantic(
                    "unsupported streaming scalar expression".into(),
                ))
            }
        };
        value.kind()?;
        Ok(value)
    }
}
