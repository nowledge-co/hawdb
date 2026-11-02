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

//! Prepay each expression occurrence, including repeated columns and sort keys.
//! Admission borrows input values; it must never evaluate or clone a payload.

use super::*;
use hawdb_sql::ExprKind;

pub(super) fn copy_account(ledger: &QueryMemoryLedger) -> hawdb_executor::QueryMemoryAccount {
    ledger.account(
        QueryMemoryClass::PipelineBatch,
        "relational projection copies",
        std::num::NonZeroUsize::new(ledger.snapshot().budget_bytes)
            .expect("query budget is non-zero"),
    )
}

pub(super) fn admit_distinct_value(
    value: &RelationalValue,
    account: &hawdb_executor::QueryMemoryAccount,
    row_limit: usize,
) -> Result<QueryMemoryLease> {
    let bytes = std::mem::size_of::<ExecutorBinding>()
        .saturating_add(entry_bytes("value".len(), value.estimated_payload_bytes()));
    if bytes > row_limit {
        return Err(HawDBError::Execution(format!(
            "intermediate row uses {bytes} bytes, exceeding batch_payload_bytes {row_limit}"
        )));
    }
    account.reserve(bytes.saturating_mul(2).saturating_add(512))
}

pub(super) fn admit_ordinary(
    select: &SelectStatement,
    row: &BoundRow<'_>,
    parameters: &[Value],
    account: &hawdb_executor::QueryMemoryAccount,
    row_limit: usize,
    order_keys: bool,
) -> Result<QueryMemoryLease> {
    let mut binding_bytes = std::mem::size_of::<ExecutorBinding>();
    let mut workspace = 0usize;
    for item in &select.projection {
        match item {
            SelectProjection::Wildcard => {
                for binding in &row.bindings {
                    for (ordinal, column) in binding.schema.columns.iter().enumerate() {
                        let payload = binding.value(ordinal)?.estimated_payload_bytes();
                        binding_bytes =
                            binding_bytes.saturating_add(entry_bytes(column.name.len(), payload));
                        workspace = workspace
                            .saturating_add(payload)
                            .saturating_add(column.name.len())
                            .saturating_add(512);
                    }
                }
            }
            SelectProjection::Expression { expression, alias } => {
                let name_bytes = alias.as_ref().map_or_else(
                    || match &expression.kind {
                        ExprKind::Column(column) => column.name.len(),
                        ExprKind::Function { name, .. } => name.len(),
                        ExprKind::Value(_) => "value".len(),
                        _ => "expression".len(),
                    },
                    String::len,
                );
                binding_bytes = binding_bytes.saturating_add(entry_bytes(
                    name_bytes,
                    output_payload_bytes(expression, row, parameters)?,
                ));
                workspace =
                    workspace.saturating_add(expression_bytes(expression, row, parameters)?);
            }
        }
    }
    if order_keys {
        for (ordinal, item) in select.order_by.iter().enumerate() {
            let payload = match resolve_relational_order_target(select, item)? {
                RelationalOrderTarget::InputColumn(column)
                | RelationalOrderTarget::ProjectionColumn { column, .. } => {
                    resolve_column(row, column)?.estimated_payload_bytes()
                }
                RelationalOrderTarget::ProjectionExpression { expression, .. } => {
                    output_payload_bytes(expression, row, parameters)?
                }
            };
            let name_bytes =
                projection::RELATIONAL_SORT_COLUMN_PREFIX.len() + ordinal.to_string().len();
            let sort_bytes = std::mem::size_of::<Vec<Value>>() + 2 * std::mem::size_of::<Value>();
            binding_bytes = binding_bytes
                .saturating_add(entry_bytes(name_bytes, sort_bytes.saturating_add(payload)));
            workspace = workspace.saturating_add(payload);
        }
    }
    if binding_bytes > row_limit {
        return Err(HawDBError::Execution(format!(
            "intermediate row uses {binding_bytes} bytes, exceeding batch_payload_bytes {row_limit}"
        )));
    }
    // Evaluating a scalar may retain its relational value alongside the output
    // and ordering-key copies. This shared working charge is separate from the
    // final batch's retained charge, and is acquired before any payload copy.
    account.reserve(workspace.saturating_mul(4).saturating_add(binding_bytes))
}

fn entry_bytes(name_bytes: usize, payload: usize) -> usize {
    name_bytes
        .saturating_add(payload)
        .saturating_add(6 * std::mem::size_of::<usize>())
}

pub(super) fn admit_sort_input(
    select: &SelectStatement,
    row: &BoundRow<'_>,
    account: &hawdb_executor::QueryMemoryAccount,
    row_limit: usize,
) -> Result<QueryMemoryLease> {
    let mut bytes = std::mem::size_of::<RelationalSortRecord>()
        .saturating_add(std::mem::size_of::<u64>())
        .saturating_add(std::mem::size_of::<RelationalRowSetLocator>())
        .saturating_add(
            row.bindings
                .len()
                .saturating_mul(std::mem::size_of::<Option<RelationalRowLocator>>()),
        );
    for item in &select.order_by {
        let column = match resolve_relational_order_target(select, item)? {
            RelationalOrderTarget::InputColumn(column)
            | RelationalOrderTarget::ProjectionColumn { column, .. } => column,
            RelationalOrderTarget::ProjectionExpression { .. } => {
                unreachable!("computed aliases use projected ordering")
            }
        };
        bytes = bytes
            .saturating_add(std::mem::size_of::<RelationalSortKey>())
            .saturating_add(resolve_column(row, column)?.estimated_payload_bytes());
    }
    for binding in &row.bindings {
        if let Some(row) = &binding.row {
            bytes = bytes.saturating_add(
                join::relational_key_resident_bytes(row.primary_key())
                    .saturating_sub(std::mem::size_of::<RelationalKey>()),
            );
        }
    }
    if bytes > row_limit {
        return Err(HawDBError::Execution(format!(
            "relational ordering row uses {bytes} bytes, exceeding blocking_operator_bytes {row_limit}"
        )));
    }
    account.reserve(bytes.saturating_mul(2))
}

pub(super) fn admit_projected_order_keys(
    binding: &ExecutorBinding,
    columns: &[(String, hawdb_sql::SqlOrderItem)],
    account: &hawdb_executor::QueryMemoryAccount,
    row_limit: usize,
) -> Result<QueryMemoryLease> {
    let mut growth = 0usize;
    for (ordinal, (column, _)) in columns.iter().enumerate() {
        let value = binding.values.get(column).ok_or_else(|| {
            HawDBError::Semantic(format!(
                "DISTINCT ORDER BY column {column} is not projected"
            ))
        })?;
        let payload = hawdb_executor::binding::value_memory_bytes(value)
            .saturating_sub(std::mem::size_of::<Value>());
        growth = growth.saturating_add(entry_bytes(
            projection::RELATIONAL_SORT_COLUMN_PREFIX.len() + ordinal.to_string().len(),
            std::mem::size_of::<Vec<Value>>()
                .saturating_add(2 * std::mem::size_of::<Value>())
                .saturating_add(payload),
        ));
    }
    let bytes = hawdb_executor::binding::binding_memory_bytes(binding).saturating_add(growth);
    if bytes > row_limit {
        return Err(HawDBError::Execution(format!(
            "intermediate row uses {bytes} bytes, exceeding batch_payload_bytes {row_limit}"
        )));
    }
    account.reserve(growth.saturating_mul(2))
}

fn output_payload_bytes(
    expression: &SqlExpression,
    row: &BoundRow<'_>,
    parameters: &[Value],
) -> Result<usize> {
    match &expression.kind {
        ExprKind::Column(column) => Ok(resolve_column(row, column)?.estimated_payload_bytes()),
        ExprKind::Arithmetic { .. }
        | ExprKind::Negate(_)
        | ExprKind::Cast { .. }
        | ExprKind::Value(SqlValue::Expression(_)) => Ok(17),
        ExprKind::Value(value) => {
            let value = match value {
                SqlValue::Literal(value) => Some(value),
                SqlValue::Expression(_) => unreachable!("handled above"),
                SqlValue::Parameter(position) => {
                    position.checked_sub(1).and_then(|i| parameters.get(i))
                }
            };
            value
                .map(hawdb_executor::binding::value_payload_bytes)
                .ok_or_else(|| HawDBError::Semantic("missing SQL expression parameter".into()))
        }
        ExprKind::Function {
            name, arguments, ..
        } if name == "coalesce" => arguments.iter().try_fold(0usize, |maximum, argument| {
            let SqlFunctionArgument::Expression(expression) = argument else {
                return Ok(maximum);
            };
            Ok(maximum.max(output_payload_bytes(expression, row, parameters)?))
        }),
        ExprKind::Function { name, .. } if name == "version" => {
            Ok("HawDB  (PostgreSQL-dialect SQL)".len() + env!("CARGO_PKG_VERSION").len())
        }
        ExprKind::Function { name, .. } if name == "uuidv7" => Ok(16),
        ExprKind::Function { name, .. }
            if hawdb_core::VectorDistance::from_function_name(name).is_some() =>
        {
            Ok(8)
        }
        // Unsupported shapes still fail during scalar evaluation; metadata
        // traversal here must not allocate or evaluate volatile expressions.
        _ => Ok(0),
    }
}

pub(super) fn admit_predicate(
    predicate: &SqlExpression,
    row: &BoundRow<'_>,
    parameters: &[Value],
    account: &hawdb_executor::QueryMemoryAccount,
) -> Result<QueryMemoryLease> {
    account.reserve(
        expression_bytes(predicate, row, parameters)?
            .saturating_mul(4)
            .saturating_add(1024)
            .saturating_add(predicate_scratch_bytes(predicate, row, parameters)?),
    )
}

pub(super) fn admit(
    select: &SelectStatement,
    row: &BoundRow<'_>,
    parameters: &[Value],
    account: &hawdb_executor::QueryMemoryAccount,
) -> Result<QueryMemoryLease> {
    let mut bytes = 1024usize;
    for projection in &select.projection {
        match projection {
            SelectProjection::Wildcard => {
                for binding in &row.bindings {
                    for (ordinal, column) in binding.schema.columns.iter().enumerate() {
                        bytes = bytes
                            .saturating_add(512)
                            .saturating_add(column.name.len())
                            .saturating_add(binding.value(ordinal)?.estimated_payload_bytes());
                    }
                }
            }
            SelectProjection::Expression { expression, alias } => {
                bytes = bytes
                    .saturating_add(alias.as_ref().map_or(0, String::len))
                    .saturating_add(expression_bytes(expression, row, parameters)?);
            }
        }
    }
    for item in &select.order_by {
        let payload = match resolve_relational_order_target(select, item)? {
            RelationalOrderTarget::InputColumn(column)
            | RelationalOrderTarget::ProjectionColumn { column, .. } => {
                resolve_column(row, column)?.estimated_payload_bytes()
            }
            RelationalOrderTarget::ProjectionExpression { expression, .. } => {
                expression_bytes(expression, row, parameters)?
            }
        };
        bytes = bytes.saturating_add(512).saturating_add(payload);
    }
    if let Some(predicate) = vector_index::residual_selection(select) {
        bytes = bytes.saturating_add(expression_bytes(predicate, row, parameters)?);
    }
    // Each scalar leaf may coexist with its evaluated relational value, output
    // value and sort-key copy. Map/slot/name overhead is included per occurrence.
    let scratch = vector_index::residual_selection(select).map_or(Ok(0), |predicate| {
        predicate_scratch_bytes(predicate, row, parameters)
    })?;
    account.reserve(bytes.saturating_mul(4).saturating_add(scratch))
}

fn expression_bytes(
    expression: &SqlExpression,
    row: &BoundRow<'_>,
    parameters: &[Value],
) -> Result<usize> {
    let mut bytes = Ok(0usize);
    expression.visit(&mut |expression| {
        let Ok(total) = &mut bytes else {
            return;
        };
        let payload = match &expression.kind {
            ExprKind::Column(column) => resolve_column(row, column).map(|value| {
                value
                    .estimated_payload_bytes()
                    .saturating_add(column.name.len())
            }),
            ExprKind::Value(SqlValue::Expression(_)) => Ok(17),
            ExprKind::Value(value) => {
                let value = match value {
                    SqlValue::Literal(value) => Some(value),
                    SqlValue::Expression(_) => unreachable!("handled above"),
                    SqlValue::Parameter(position) => position
                        .checked_sub(1)
                        .and_then(|index| parameters.get(index)),
                };
                value
                    .map(hawdb_executor::binding::value_memory_bytes)
                    .ok_or_else(|| HawDBError::Semantic("missing SQL expression parameter".into()))
            }
            ExprKind::Function { name, .. } => Ok(name.len()),
            ExprKind::Arithmetic { .. } | ExprKind::Negate(_) | ExprKind::Cast { .. } => Ok(1024),
            _ => Ok(0),
        };
        match payload {
            Ok(payload) => *total = total.saturating_add(512).saturating_add(payload),
            Err(error) => bytes = Err(error),
        }
    });
    bytes
}

fn predicate_scratch_bytes(
    predicate: &SqlExpression,
    row: &BoundRow<'_>,
    parameters: &[Value],
) -> Result<usize> {
    let mut bytes = Ok(0usize);
    predicate.visit(&mut |expression| {
        let Ok(total) = &mut bytes else {
            return;
        };
        if let ExprKind::Like {
            left,
            pattern,
            escape,
            case_insensitive,
            ..
        } = &expression.kind
        {
            let scratch = (|| {
                let (Some(value), Some(pattern)) = (
                    text_operand(left, row, parameters)?,
                    text_operand(pattern, row, parameters)?,
                ) else {
                    return Ok(0);
                };
                // An invalid layout also fails in the matcher before any buffer
                // allocation. Leave its diagnostic to actual predicate evaluation
                // so a short-circuited branch cannot introduce an earlier error.
                Ok(
                    hawdb_sql::sql_like_scratch_bytes(value, pattern, *escape, *case_insensitive)
                        .unwrap_or(0),
                )
            })();
            match scratch {
                Ok(scratch) => *total = total.saturating_add(scratch),
                Err(error) => bytes = Err(error),
            }
        }
    });
    bytes
}

// Borrow matcher inputs without invoking scalar evaluation or cloning a parameter.
// The predicate evaluator accepts a column on the left and Column/Value on the
// right. If it gains computed operands, this borrowing-only admission must grow
// with it. NULL has no workspace; unsupported operands retain its diagnostics.
fn text_operand<'a>(
    expression: &'a SqlExpression,
    row: &'a BoundRow<'_>,
    parameters: &'a [Value],
) -> Result<Option<&'a str>> {
    match &expression.kind {
        ExprKind::Column(column) => Ok(match resolve_column(row, column)? {
            RelationalValue::Text(value) => Some(value.as_str()),
            _ => None,
        }),
        ExprKind::Value(value) => {
            let value = match value {
                SqlValue::Literal(value) => Some(value),
                SqlValue::Expression(_) => None,
                SqlValue::Parameter(position) => position
                    .checked_sub(1)
                    .and_then(|index| parameters.get(index)),
            };
            Ok(match value {
                Some(Value::String(value)) => Some(value.as_str()),
                _ => None,
            })
        }
        _ => Ok(None),
    }
}
