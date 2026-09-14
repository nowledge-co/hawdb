//! Ordinary SQL aggregate state and HAVING compilation over borrowed row values.
//!
//! The facade retains grouping, sorting, row binding, and query lifecycle control.
//! State transitions return the same memory deltas consumed by its operator tracker.

use crate::predicate::predicate_truth_with;
use crate::query_value::{
    bind_sql_value, expression_name, relational_to_value, value_to_relational,
};
use skein_core::{Result, SkeinError, Value};
use skein_executor::kernel::{ensure_operator_item_fits, OperatorMemoryTracker};
use skein_sql::{
    Expr, ExprKind, SelectProjection, SelectStatement, SqlColumnRef, SqlComparisonOp,
    SqlExpression, SqlFunctionArgument, SqlPredicate, SqlValue,
};
use skein_storage::{
    RelationalScalarType, RelationalState, RelationalTableSchema, RelationalValue,
};
use std::collections::BTreeSet;

mod having;
mod state;

pub use having::{filter_group, projection_template, validate_having};
pub use state::{
    aggregate_group_base_memory_bytes, charge_aggregate_memory, AggregateMemoryDelta,
    AggregateProjectionState,
};

fn evaluate_row_expression<'a>(
    expression: &SqlExpression,
    row: &impl Fn(&SqlColumnRef) -> Result<(&'a RelationalValue, RelationalScalarType)>,
) -> Result<RelationalValue> {
    match expression {
        Expr {
            kind: ExprKind::Column(column),
            ..
        } => Ok(resolve_column(row, column)?.clone()),
        Expr {
            kind: ExprKind::Value(SqlValue::Literal(value)),
            ..
        } => value_to_relational(value.clone()),
        Expr {
            kind: ExprKind::Value(SqlValue::Parameter(position)),
            ..
        } => Err(SkeinError::Semantic(format!(
            "aggregate row expression cannot bind parameter ${position}"
        ))),
        Expr {
            kind:
                ExprKind::Function {
                    name,
                    arguments,
                    distinct: false,
                    filter: None,
                },
            ..
        } if name == "octet_length" => {
            let [SqlFunctionArgument::Expression(Expr {
                kind: ExprKind::Column(column),
                ..
            })] = arguments.as_slice()
            else {
                return Err(SkeinError::Semantic(
                    "OCTET_LENGTH requires exactly one column".to_string(),
                ));
            };
            match resolve_column(row, column)? {
                RelationalValue::Null => Ok(RelationalValue::Null),
                RelationalValue::Text(value) => Ok(RelationalValue::BigInt(
                    i64::try_from(value.len()).unwrap_or(i64::MAX),
                )),
                RelationalValue::Bytea(value) => Ok(RelationalValue::BigInt(
                    i64::try_from(value.len()).unwrap_or(i64::MAX),
                )),
                RelationalValue::Overflow(reference) => Ok(RelationalValue::BigInt(
                    i64::try_from(reference.uncompressed_bytes).unwrap_or(i64::MAX),
                )),
                _ => Err(SkeinError::Semantic(
                    "OCTET_LENGTH requires TEXT or BYTEA input".to_string(),
                )),
            }
        }
        Expr {
            kind: ExprKind::Function { name, .. },
            ..
        } => Err(SkeinError::Semantic(format!(
            "unsupported aggregate row function {name}"
        ))),
        _ => Err(SkeinError::Semantic(
            "unsupported scalar expression".to_owned(),
        )),
    }
}

pub fn aggregate_filter_matches<'a>(
    filter: Option<&SqlPredicate>,
    row: &impl Fn(&SqlColumnRef) -> Result<(&'a RelationalValue, RelationalScalarType)>,
    parameters: &[Value],
) -> Result<bool> {
    match filter {
        Some(filter) => Ok(predicate_truth_with(filter, parameters, row)? == Some(true)),
        None => Ok(true),
    }
}

fn resolve_column<'a>(
    row: &impl Fn(&SqlColumnRef) -> Result<(&'a RelationalValue, RelationalScalarType)>,
    column: &SqlColumnRef,
) -> Result<&'a RelationalValue> {
    row(column).map(|(value, _)| value)
}

#[cfg(test)]
mod tests;
