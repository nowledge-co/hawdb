// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_sql::Expr;
use hawdb_storage::relational::{
    RelationalNumericExpression, MAX_NUMERIC_EXPRESSION_DEPTH, MAX_NUMERIC_EXPRESSION_NODES,
};

pub(super) fn compile_numeric(
    expression: &Expr,
    parameters: &[Value],
    schema: &RelationalTableSchema,
    alias: Option<&str>,
    table: &str,
) -> Result<RelationalNumericExpression> {
    let mut remaining = MAX_NUMERIC_EXPRESSION_NODES;
    compile_node(
        expression,
        parameters,
        schema,
        alias,
        table,
        0,
        &mut remaining,
    )
}

fn compile_node(
    expression: &Expr,
    parameters: &[Value],
    schema: &RelationalTableSchema,
    alias: Option<&str>,
    table: &str,
    depth: usize,
    remaining: &mut usize,
) -> Result<RelationalNumericExpression> {
    use RelationalNumericExpression as N;
    if depth > MAX_NUMERIC_EXPRESSION_DEPTH || *remaining == 0 {
        return Err(HawDBError::Semantic(
            "numeric expression exceeds 64 levels or 4096 nodes".into(),
        ));
    }
    *remaining -= 1;
    let mut child = |expression| {
        compile_node(
            expression,
            parameters,
            schema,
            alias,
            table,
            depth + 1,
            remaining,
        )
    };
    Ok(match &expression.kind {
        ExprKind::Column(column) => {
            validate_mutation_column(column, schema, alias, table)?;
            N::Column(column.name.clone())
        }
        ExprKind::Value(value) => N::Value(bind_relational_value(value.clone(), parameters)?),
        ExprKind::Arithmetic { left, op, right } => {
            let left = child(left)?;
            let right = child(right)?;
            if let (N::Value(left), N::Value(right)) = (&left, &right) {
                let left = crate::query_value::relational_to_value(left)?;
                let right = crate::query_value::relational_to_value(right)?;
                N::Value(crate::query_value::value_to_relational(
                    hawdb_core::numeric::evaluate_numeric(left.as_ref(), *op, right.as_ref())?,
                )?)
            } else {
                N::Binary {
                    left: Box::new(left),
                    op: *op,
                    right: Box::new(right),
                }
            }
        }
        ExprKind::Negate(expression) => {
            let expression = child(expression)?;
            if let N::Value(value) = &expression {
                let value = crate::query_value::relational_to_value(value)?;
                N::Value(crate::query_value::value_to_relational(
                    hawdb_core::numeric::negate_numeric(value.as_ref())?,
                )?)
            } else {
                N::Negate(Box::new(expression))
            }
        }
        ExprKind::Cast { expression, target } => {
            let expression = child(expression)?;
            if let N::Value(value) = &expression {
                let value = crate::query_value::relational_to_value(value)?;
                N::Value(crate::query_value::value_to_relational(
                    hawdb_core::numeric::cast_numeric(value.as_ref(), *target)?,
                )?)
            } else {
                N::Cast {
                    expression: Box::new(expression),
                    target: *target,
                }
            }
        }
        _ => {
            return Err(HawDBError::Semantic(
                "unsupported numeric mutation expression".into(),
            ))
        }
    })
}

pub(super) fn compile_comparison(
    left: &Expr,
    op: SqlComparisonOp,
    right: &Expr,
    parameters: &[Value],
    schema: &RelationalTableSchema,
    alias: Option<&str>,
    table: &str,
) -> Result<RelationalPredicate> {
    use RelationalNumericExpression as N;
    let mut left = compile_numeric(left, parameters, schema, alias, table)?;
    let mut right = compile_numeric(right, parameters, schema, alias, table)?;
    let mut op = compile_comparison_op(op);
    if matches!((&left, &right), (N::Value(_), N::Column(_))) {
        std::mem::swap(&mut left, &mut right);
        op = match op {
            RelationalComparisonOp::Lt => RelationalComparisonOp::Gt,
            RelationalComparisonOp::Lte => RelationalComparisonOp::Gte,
            RelationalComparisonOp::Gt => RelationalComparisonOp::Lt,
            RelationalComparisonOp::Gte => RelationalComparisonOp::Lte,
            op => op,
        };
    }
    if let (N::Column(column), N::Value(value)) = (&left, &right) {
        let kind = schema.columns[schema
            .column_position(column)
            .expect("validated numeric column")]
        .scalar_type;
        let value = crate::query_value::value_to_relational_as(
            crate::query_value::relational_to_value(value)?,
            kind,
        )?;
        if value == RelationalValue::Null || kind.same_value_domain(value.scalar_type()) {
            return Ok(RelationalPredicate::Compare {
                column: column.clone(),
                op,
                value,
            });
        }
    }
    Ok(RelationalPredicate::NumericCompare { left, op, right })
}
