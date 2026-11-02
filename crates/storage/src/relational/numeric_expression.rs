// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Parameter-free numeric mutation expressions. Resolve columns once and run a
//! bounded instruction stream against each original row before publishing writes.

use super::{
    RelationalError, RelationalRow, RelationalScalarType, RelationalTableSchema, RelationalValue,
    RelationalValueRef,
};
use crate::background::CheckpointDecodeContext;
use hawdb_core::numeric::{
    cast_numeric, evaluate_numeric, negate_numeric, NumericBinaryOp, NumericType,
};
use hawdb_core::{LogicalType, Value, ValueRef};

pub const MAX_NUMERIC_EXPRESSION_DEPTH: usize = 64;
pub const MAX_NUMERIC_EXPRESSION_NODES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalNumericExpression {
    Value(RelationalValue),
    Column(String),
    Binary {
        left: Box<Self>,
        op: NumericBinaryOp,
        right: Box<Self>,
    },
    Negate(Box<Self>),
    Cast {
        expression: Box<Self>,
        target: NumericType,
    },
}

impl RelationalNumericExpression {
    // Validation precedes row evaluation, bounding recursion without per-row allocation.
    pub(super) fn evaluate_row<'a>(
        &'a self,
        schema: &RelationalTableSchema,
        row: &'a RelationalRow,
    ) -> Result<ValueRef<'a>, RelationalError> {
        match self {
            Self::Value(value) => numeric_ref(value.as_ref()),
            Self::Column(name) => {
                let position = schema.column_position(name).ok_or_else(|| {
                    RelationalError::Schema(format!("unknown numeric column {name}"))
                })?;
                numeric_ref(row.values()[position].as_ref())
            }
            Self::Binary { left, op, right } => result_ref(
                evaluate_numeric(
                    left.evaluate_row(schema, row)?,
                    *op,
                    right.evaluate_row(schema, row)?,
                )
                .map_err(execution)?,
            ),
            Self::Negate(expression) => result_ref(
                negate_numeric(expression.evaluate_row(schema, row)?).map_err(execution)?,
            ),
            Self::Cast { expression, target } => result_ref(
                cast_numeric(expression.evaluate_row(schema, row)?, *target).map_err(execution)?,
            ),
        }
    }

    pub(super) fn clone_with_allocations(
        &self,
        context: &CheckpointDecodeContext,
    ) -> Result<Self, RelationalError> {
        self.node_count()?;
        self.clone_node(context)
    }

    fn clone_node(&self, context: &CheckpointDecodeContext) -> Result<Self, RelationalError> {
        let boxed = |expression: &Self| {
            context
                .reserve(std::mem::size_of::<Self>())
                .map_err(admission)?;
            expression.clone_node(context).map(Box::new)
        };
        Ok(match self {
            Self::Value(value) => {
                Self::Value(value.as_ref().to_owned_value_with_allocations(context)?)
            }
            Self::Column(name) => Self::Column(context.string(name).map_err(admission)?),
            Self::Binary { left, op, right } => Self::Binary {
                left: boxed(left)?,
                op: *op,
                right: boxed(right)?,
            },
            Self::Negate(expression) => Self::Negate(boxed(expression)?),
            Self::Cast { expression, target } => Self::Cast {
                expression: boxed(expression)?,
                target: *target,
            },
        })
    }

    pub(super) fn node_count(&self) -> Result<usize, RelationalError> {
        fn visit(
            expression: &RelationalNumericExpression,
            depth: usize,
            nodes: &mut usize,
        ) -> Result<(), RelationalError> {
            if depth > MAX_NUMERIC_EXPRESSION_DEPTH || *nodes == MAX_NUMERIC_EXPRESSION_NODES {
                return Err(RelationalError::Admission(
                    "numeric expression exceeds 64 levels or 4096 nodes".into(),
                ));
            }
            *nodes += 1;
            match expression {
                RelationalNumericExpression::Binary { left, right, .. } => {
                    visit(left, depth + 1, nodes)?;
                    visit(right, depth + 1, nodes)?;
                }
                RelationalNumericExpression::Negate(expression)
                | RelationalNumericExpression::Cast { expression, .. } => {
                    visit(expression, depth + 1, nodes)?
                }
                RelationalNumericExpression::Value(_) | RelationalNumericExpression::Column(_) => {}
            }
            Ok(())
        }
        let mut nodes = 0;
        visit(self, 0, &mut nodes)?;
        Ok(nodes)
    }

    pub(super) fn compile<'a>(
        &'a self,
        schema: &RelationalTableSchema,
        allocations: Option<&CheckpointDecodeContext>,
    ) -> Result<CompiledNumericExpression<'a>, RelationalError> {
        let nodes = self.node_count()?;
        let mut instructions = match allocations {
            Some(context) => context.vector_capacity(nodes).map_err(admission)?,
            None => Vec::with_capacity(nodes),
        };
        let result_type = compile(self, schema, &mut instructions)?;
        Ok(CompiledNumericExpression {
            instructions,
            result_type,
        })
    }
}

fn admission(error: hawdb_core::HawDBError) -> RelationalError {
    RelationalError::Admission(error.to_string())
}
fn execution(error: hawdb_core::HawDBError) -> RelationalError {
    RelationalError::Constraint(error.to_string())
}

pub(super) struct CompiledNumericExpression<'a> {
    instructions: Vec<Instruction<'a>>,
    pub(super) result_type: Option<LogicalType>,
}

enum Instruction<'a> {
    Value(ValueRef<'a>),
    Column(usize),
    Binary(NumericBinaryOp),
    Negate,
    Cast(NumericType),
}

fn compile<'a>(
    expression: &'a RelationalNumericExpression,
    schema: &RelationalTableSchema,
    instructions: &mut Vec<Instruction<'a>>,
) -> Result<Option<LogicalType>, RelationalError> {
    let (instruction, kind) = match expression {
        RelationalNumericExpression::Value(value) => {
            let value = numeric_ref(value.as_ref())?;
            (Instruction::Value(value), value.logical_type())
        }
        RelationalNumericExpression::Column(name) => {
            let index = schema.column_position(name).ok_or_else(|| {
                RelationalError::Schema(format!(
                    "numeric expression references unknown column {name}"
                ))
            })?;
            let kind = schema.columns[index].scalar_type.logical_type();
            if !kind.is_numeric() && !kind.is_utf8() {
                return Err(RelationalError::Schema(
                    "numeric expression requires numeric or CAST text operands".into(),
                ));
            }
            (Instruction::Column(index), Some(kind))
        }
        RelationalNumericExpression::Binary { left, op, right } => {
            let left = compile(left, schema, instructions)?;
            let right = compile(right, schema, instructions)?;
            (Instruction::Binary(*op), common_numeric_type(left, right)?)
        }
        RelationalNumericExpression::Negate(expression) => {
            let kind = compile(expression, schema, instructions)?;
            (Instruction::Negate, common_numeric_type(kind, None)?)
        }
        RelationalNumericExpression::Cast { expression, target } => {
            compile(expression, schema, instructions)?;
            (Instruction::Cast(*target), Some(target.logical_type()))
        }
    };
    instructions.push(instruction);
    Ok(kind)
}

fn common_numeric_type(
    left: Option<LogicalType>,
    right: Option<LogicalType>,
) -> Result<Option<LogicalType>, RelationalError> {
    hawdb_core::numeric::common_numeric_type(left, right)
        .map_err(|error| RelationalError::Schema(error.to_string()))
}

fn numeric_ref(value: RelationalValueRef<'_>) -> Result<ValueRef<'_>, RelationalError> {
    match value {
        RelationalValueRef::Null => Ok(ValueRef::Null),
        RelationalValueRef::BigInt(value) => Ok(ValueRef::Int(value)),
        RelationalValueRef::DoublePrecision(value) => Ok(ValueRef::Float(value)),
        RelationalValueRef::Decimal(value) => Ok(ValueRef::Decimal(value)),
        RelationalValueRef::Text(value) => Ok(ValueRef::String(value)),
        RelationalValueRef::Overflow(_) => Err(RelationalError::Admission(
            "numeric CAST requires hydrated text".into(),
        )),
        _ => Err(RelationalError::Schema(
            "numeric expression requires numeric or CAST text operands".into(),
        )),
    }
}

fn result_ref(value: Value) -> Result<ValueRef<'static>, RelationalError> {
    match value {
        Value::Null => Ok(ValueRef::Null),
        Value::Int(value) => Ok(ValueRef::Int(value)),
        Value::Float(value) => Ok(ValueRef::Float(value)),
        Value::Decimal(value) => Ok(ValueRef::Decimal(value)),
        _ => Err(RelationalError::Schema(
            "numeric expression must produce a numeric value".into(),
        )),
    }
}

impl CompiledNumericExpression<'_> {
    pub(super) fn evaluate<'a>(
        &'a self,
        row: &'a RelationalRow,
    ) -> Result<ValueRef<'a>, RelationalError> {
        // A binary expression of depth 64 requires at most 65 live operands.
        let mut stack = [ValueRef::Null; MAX_NUMERIC_EXPRESSION_DEPTH + 2];
        let mut len = 0;
        for instruction in &self.instructions {
            match instruction {
                Instruction::Value(value) => {
                    stack[len] = *value;
                    len += 1;
                }
                Instruction::Column(index) => {
                    stack[len] = numeric_ref(row.values()[*index].as_ref())?;
                    len += 1;
                }
                Instruction::Binary(op) => {
                    let value =
                        evaluate_numeric(stack[len - 2], *op, stack[len - 1]).map_err(execution)?;
                    len -= 1;
                    stack[len - 1] = result_ref(value)?;
                }
                Instruction::Negate => {
                    stack[len - 1] =
                        result_ref(negate_numeric(stack[len - 1]).map_err(execution)?)?;
                }
                Instruction::Cast(target) => {
                    stack[len - 1] =
                        result_ref(cast_numeric(stack[len - 1], *target).map_err(execution)?)?;
                }
            }
        }
        Ok(stack[0])
    }

    pub(super) fn assignment(
        &self,
        row: &RelationalRow,
        target: RelationalScalarType,
    ) -> Result<RelationalValueRef<'static>, RelationalError> {
        let value = self.evaluate(row)?;
        if value.is_null() {
            return Ok(RelationalValueRef::Null);
        }
        let target = match target {
            RelationalScalarType::BigInt => NumericType::BigInt,
            RelationalScalarType::DoublePrecision => NumericType::DoublePrecision,
            RelationalScalarType::Decimal(kind) => {
                if matches!(value, ValueRef::Float(_)) {
                    return Err(RelationalError::Schema(
                        "DOUBLE PRECISION assignment to Decimal requires CAST".into(),
                    ));
                }
                NumericType::Decimal(kind)
            }
            _ => {
                return Err(RelationalError::Schema(
                    "numeric assignment requires a numeric target".into(),
                ))
            }
        };
        match cast_numeric(value, target).map_err(execution)? {
            Value::Null => Ok(RelationalValueRef::Null),
            Value::Int(value) => Ok(RelationalValueRef::BigInt(value)),
            Value::Float(value) => Ok(RelationalValueRef::DoublePrecision(value)),
            Value::Decimal(value) => Ok(RelationalValueRef::Decimal(value)),
            _ => unreachable!("numeric CAST result"),
        }
    }
}

pub(super) fn validate_numeric_comparison(
    left: Option<LogicalType>,
    right: Option<LogicalType>,
) -> Result<(), RelationalError> {
    common_numeric_type(left, right).map(|_| ())
}
