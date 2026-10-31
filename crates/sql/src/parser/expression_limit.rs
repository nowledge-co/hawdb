// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Bound lowering and subsequent visitors even when an upstream Pratt loop
/// constructs a deep, flat-looking arithmetic or cast expression.
pub(super) fn validate(expression: &Expr, depth: usize) -> Result<()> {
    if depth > 128 {
        return Err(HawDBError::Semantic(
            "SQL expression nesting limit exceeded".into(),
        ));
    }
    let child = |expression: &Expr| validate(expression, depth + 1);
    match expression {
        Expr::Nested(expression)
        | Expr::UnaryOp {
            expr: expression, ..
        }
        | Expr::Cast {
            expr: expression, ..
        }
        | Expr::IsNull(expression)
        | Expr::IsNotNull(expression) => child(expression),
        Expr::BinaryOp { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::ILike {
            expr: left,
            pattern: right,
            ..
        } => {
            child(left)?;
            child(right)
        }
        Expr::InList { expr, list, .. } => {
            child(expr)?;
            list.iter().try_for_each(child)
        }
        Expr::Function(function) => {
            if let FunctionArguments::List(arguments) = &function.args {
                for argument in &arguments.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) = argument {
                        child(expression)?;
                    }
                }
            }
            if let Some(filter) = &function.filter {
                child(filter)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
