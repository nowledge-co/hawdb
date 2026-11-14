// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::super::{
    RelationalNumericExpression, MAX_NUMERIC_EXPRESSION_DEPTH, MAX_NUMERIC_EXPRESSION_NODES,
};
use super::*;
use hawdb_core::numeric::{NumericBinaryOp, NumericType};

pub(super) fn encode(
    sink: &mut impl schema_encoding::Sink,
    expression: &RelationalNumericExpression,
) -> Result<(), RelationalError> {
    expression.node_count()?;
    encode_node(sink, expression)
}

fn encode_node(
    sink: &mut impl schema_encoding::Sink,
    expression: &RelationalNumericExpression,
) -> Result<(), RelationalError> {
    match expression {
        RelationalNumericExpression::Value(value) => {
            if !matches!(
                value,
                RelationalValue::Null
                    | RelationalValue::BigInt(_)
                    | RelationalValue::DoublePrecision(_)
                    | RelationalValue::Decimal(_)
                    | RelationalValue::Text(_)
            ) {
                return Err(RelationalError::Durability(
                    "numeric expression contains an unsupported value".into(),
                ));
            }
            sink.raw(&[0])?;
            sink.value(value)
        }
        RelationalNumericExpression::Column(column) => {
            sink.raw(&[1])?;
            schema_encoding::bytes(sink, column.as_bytes())
        }
        RelationalNumericExpression::Binary { left, op, right } => {
            sink.raw(&[
                2,
                match op {
                    NumericBinaryOp::Add => 0,
                    NumericBinaryOp::Subtract => 1,
                    NumericBinaryOp::Multiply => 2,
                    NumericBinaryOp::Divide => 3,
                    NumericBinaryOp::Remainder => 4,
                },
            ])?;
            encode_node(sink, left)?;
            encode_node(sink, right)
        }
        RelationalNumericExpression::Negate(expression) => {
            sink.raw(&[3])?;
            encode_node(sink, expression)
        }
        RelationalNumericExpression::Cast { expression, target } => {
            sink.raw(&[4])?;
            match target {
                NumericType::BigInt => sink.raw(&[0])?,
                NumericType::DoublePrecision => sink.raw(&[1])?,
                NumericType::Decimal(kind) => sink.raw(&[2, kind.precision(), kind.scale()])?,
            }
            encode_node(sink, expression)
        }
    }
}

impl<I: DecodeInput> Decoder<I> {
    pub(super) fn numeric_expression(
        &mut self,
    ) -> Result<RelationalNumericExpression, RelationalError> {
        self.numeric_node(0, &mut 0)
    }

    fn boxed_numeric(
        &mut self,
        depth: usize,
        nodes: &mut usize,
    ) -> Result<Box<RelationalNumericExpression>, RelationalError> {
        if let Some(context) = self.input.checkpoint_allocation_context() {
            context
                .reserve(std::mem::size_of::<RelationalNumericExpression>())
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
        }
        Ok(Box::new(self.numeric_node(depth, nodes)?))
    }

    fn numeric_node(
        &mut self,
        depth: usize,
        nodes: &mut usize,
    ) -> Result<RelationalNumericExpression, RelationalError> {
        if depth > MAX_NUMERIC_EXPRESSION_DEPTH || *nodes == MAX_NUMERIC_EXPRESSION_NODES {
            return Err(RelationalError::Corruption(
                "numeric expression exceeds 64 levels or 4096 nodes".into(),
            ));
        }
        *nodes += 1;
        match self.u8()? {
            0 => {
                let value = self.logical_value()?;
                if !matches!(
                    value,
                    RelationalValue::Null
                        | RelationalValue::BigInt(_)
                        | RelationalValue::DoublePrecision(_)
                        | RelationalValue::Decimal(_)
                        | RelationalValue::Text(_)
                ) {
                    return Err(RelationalError::Corruption(
                        "numeric expression contains an unsupported value".into(),
                    ));
                }
                Ok(RelationalNumericExpression::Value(value))
            }
            1 => Ok(RelationalNumericExpression::Column(self.string()?)),
            2 => {
                let op = match self.u8()? {
                    0 => NumericBinaryOp::Add,
                    1 => NumericBinaryOp::Subtract,
                    2 => NumericBinaryOp::Multiply,
                    3 => NumericBinaryOp::Divide,
                    4 => NumericBinaryOp::Remainder,
                    tag => {
                        return Err(RelationalError::Corruption(format!(
                            "invalid numeric operator {tag}"
                        )))
                    }
                };
                Ok(RelationalNumericExpression::Binary {
                    left: self.boxed_numeric(depth + 1, nodes)?,
                    op,
                    right: self.boxed_numeric(depth + 1, nodes)?,
                })
            }
            3 => Ok(RelationalNumericExpression::Negate(
                self.boxed_numeric(depth + 1, nodes)?,
            )),
            4 => {
                let target = match self.u8()? {
                    0 => NumericType::BigInt,
                    1 => NumericType::DoublePrecision,
                    2 => NumericType::Decimal(
                        hawdb_core::DecimalType::new(self.u8()?, self.u8()?)
                            .map_err(|error| RelationalError::Corruption(error.to_string()))?,
                    ),
                    tag => {
                        return Err(RelationalError::Corruption(format!(
                            "invalid numeric CAST type {tag}"
                        )))
                    }
                };
                Ok(RelationalNumericExpression::Cast {
                    expression: self.boxed_numeric(depth + 1, nodes)?,
                    target,
                })
            }
            tag => Err(RelationalError::Corruption(format!(
                "invalid numeric expression tag {tag}"
            ))),
        }
    }
}
