// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Coerces unadorned numeric literals that meet a DOUBLE PRECISION operand
//! (#1516). PostgreSQL types `12.5` as `numeric` and resolves
//! `float8 + numeric` through the implicit numeric-to-float8 cast, so the
//! literal becomes the nearest double. DECIMAL columns, DECIMAL parameters and
//! explicit `CAST(... AS DECIMAL)` stay typed and keep #1429's explicit-CAST
//! rule beside DOUBLE PRECISION; only a constant built from numeric literals is
//! converted. Integer-only literal expressions already combine with DOUBLE
//! PRECISION through the shared numeric kernel and are left unchanged.

use super::{bind_constant_numeric, Expr, ExprKind, SqlColumnRef, SqlFunctionArgument, SqlValue};
use hawdb_core::numeric::{cast_numeric, NumericType};
use hawdb_core::{LogicalType, Result, Value};

/// Static operand types for one statement: column types from the bound schema
/// and parameter types from the supplied values. Unknown columns resolve to
/// `None`; later binding reports them.
pub struct NumericLiteralCoercion<'a, F> {
    column_type: F,
    parameters: &'a [Value],
}

impl<'a, F> NumericLiteralCoercion<'a, F>
where
    F: Fn(&SqlColumnRef) -> Option<LogicalType>,
{
    pub fn new(column_type: F, parameters: &'a [Value]) -> Self {
        Self {
            column_type,
            parameters,
        }
    }

    /// Whether `apply` would rewrite any literal; lets callers avoid cloning
    /// borrowed statement templates in the common case.
    pub fn needed(&self, expression: &Expr) -> bool {
        let mut needed = false;
        expression.visit(&mut |node| needed |= self.node_needs_coercion(node));
        needed
    }

    /// Replaces each decimal literal expression unified with a DOUBLE
    /// PRECISION operand by the nearest DOUBLE PRECISION literal. The literal
    /// expression is evaluated exactly first, so `balance + (12.5 + 1.0)` adds
    /// the double nearest 13.5, matching PostgreSQL's numeric constant folding
    /// followed by numeric-to-float8.
    pub fn apply(&self, expression: &mut Expr) -> Result<()> {
        expression.try_visit_mut(&mut |node| {
            if !self.node_needs_coercion(node) {
                return Ok(());
            }
            match &mut node.kind {
                ExprKind::Arithmetic { left, right, .. }
                | ExprKind::Compare { left, right, .. } => {
                    for operand in [left, right] {
                        coerce_literal(operand)?;
                    }
                }
                ExprKind::InList { left, values, .. } => {
                    coerce_literal(left)?;
                    for value in values {
                        coerce_literal(value)?;
                    }
                }
                ExprKind::Function { arguments, .. } => {
                    for argument in arguments {
                        if let SqlFunctionArgument::Expression(argument) = argument {
                            coerce_literal(argument)?;
                        }
                    }
                }
                _ => {}
            }
            Ok(())
        })
    }

    /// Whether a type-unifying node has a decimal literal operand beside a
    /// DOUBLE PRECISION operand.
    fn node_needs_coercion(&self, node: &Expr) -> bool {
        // Check the cheap literal shape first; most nodes have no literal.
        let mut literal = false;
        if !unified_operands(node, |operand| literal |= is_decimal_literal(operand)) || !literal {
            return false;
        }
        let mut double = false;
        unified_operands(node, |operand| {
            double |= self.static_type(operand) == Some(LogicalType::Float64)
        });
        double
    }

    /// The type an expression has after coercion. A literal beside DOUBLE
    /// PRECISION already counts as DOUBLE PRECISION, so types do not change
    /// while `apply` rewrites children before parents.
    fn static_type(&self, expression: &Expr) -> Option<LogicalType> {
        match &expression.kind {
            ExprKind::Column(column) => (self.column_type)(column),
            ExprKind::Value(SqlValue::Literal(value)) => value.logical_type(),
            ExprKind::Value(SqlValue::Parameter(position)) => position
                .checked_sub(1)
                .and_then(|index| self.parameters.get(index))
                .and_then(Value::logical_type),
            ExprKind::Value(SqlValue::Expression(expression)) | ExprKind::Negate(expression) => {
                self.static_type(expression)
            }
            ExprKind::Arithmetic { left, right, .. } => {
                let types = [self.static_type(left), self.static_type(right)];
                if types.contains(&Some(LogicalType::Float64)) {
                    Some(LogicalType::Float64)
                } else if types.contains(&Some(LogicalType::Decimal)) {
                    Some(LogicalType::Decimal)
                } else {
                    types[0].or(types[1])
                }
            }
            ExprKind::Cast { target, .. } => Some(target.logical_type()),
            ExprKind::Function {
                name, arguments, ..
            } => {
                let mut types = arguments.iter().map(|argument| match argument {
                    SqlFunctionArgument::Expression(argument) => self.static_type(argument),
                    SqlFunctionArgument::Wildcard => None,
                });
                match name.as_str() {
                    "count" => Some(LogicalType::Int64),
                    "sum" | "min" | "max" if arguments.len() == 1 => types.next().flatten(),
                    "coalesce" => {
                        let types = types.flatten().collect::<Vec<_>>();
                        if types.contains(&LogicalType::Float64) {
                            Some(LogicalType::Float64)
                        } else {
                            types.first().copied()
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

/// Visits the operands whose types an operator or function unifies:
/// arithmetic, comparison, IN lists and COALESCE. Returns false for any other
/// node.
fn unified_operands<'e>(node: &'e Expr, mut visit: impl FnMut(&'e Expr)) -> bool {
    match &node.kind {
        ExprKind::Arithmetic { left, right, .. } | ExprKind::Compare { left, right, .. } => {
            visit(left);
            visit(right);
        }
        ExprKind::InList { left, values, .. } => {
            visit(left);
            values.iter().for_each(&mut visit);
        }
        ExprKind::Function {
            name, arguments, ..
        } if name == "coalesce" => {
            for argument in arguments {
                if let SqlFunctionArgument::Expression(argument) = argument {
                    visit(argument);
                }
            }
        }
        _ => return false,
    }
    true
}

/// A constant made only of numeric literals (including unary minus,
/// parenthesized forms and literal arithmetic) whose value is DECIMAL.
fn is_decimal_literal(expression: &Expr) -> bool {
    literal_type(expression) == Some(LogicalType::Decimal)
}

fn literal_type(expression: &Expr) -> Option<LogicalType> {
    match &expression.kind {
        ExprKind::Value(SqlValue::Literal(Value::Int(_))) => Some(LogicalType::Int64),
        ExprKind::Value(SqlValue::Literal(Value::Decimal(_))) => Some(LogicalType::Decimal),
        ExprKind::Value(SqlValue::Expression(expression)) | ExprKind::Negate(expression) => {
            literal_type(expression)
        }
        ExprKind::Arithmetic { left, right, .. } => {
            match (literal_type(left)?, literal_type(right)?) {
                (LogicalType::Int64, LogicalType::Int64) => Some(LogicalType::Int64),
                _ => Some(LogicalType::Decimal),
            }
        }
        _ => None,
    }
}

fn coerce_literal(expression: &mut Expr) -> Result<()> {
    if !is_decimal_literal(expression) {
        return Ok(());
    }
    // Decimal-to-double parses the exact decimal text with Rust's correctly
    // rounded `f64` parser: the nearest double, like PostgreSQL's float8in.
    let value = bind_constant_numeric(expression, &[])?;
    let value = cast_numeric(value.as_ref(), NumericType::DoublePrecision)?;
    expression.kind = ExprKind::Value(SqlValue::Literal(value));
    Ok(())
}

#[cfg(test)]
#[path = "literal_coercion_tests.rs"]
mod tests;
