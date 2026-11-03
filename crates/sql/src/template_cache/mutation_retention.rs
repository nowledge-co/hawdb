// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Requested-heap charges for schema-neutral DML templates. Unsupported literal
//! ownership refuses retention while leaving ordinary statement execution intact.

use crate::{
    Expr, ExprKind, PreparedPostgresStatement, SqlArithmeticOperand, SqlAssignment,
    SqlAssignmentValue, SqlColumnRef, SqlConflictAction, SqlFunctionArgument, SqlStatement,
    SqlTableName, SqlValue,
};
use hawdb_core::Value;
use std::mem::{align_of, size_of};

pub(super) fn heap_charge(key: &String, template: &PreparedPostgresStatement) -> Option<usize> {
    let statement = match &template.statement {
        SqlStatement::Insert(insert) => sum([
            table(&insert.table),
            vector(&insert.columns, string),
            vector(&insert.rows, |row| vector(row, value)),
            optional(&insert.on_conflict, |conflict| {
                sum([
                    vector(&conflict.columns, string),
                    match &conflict.action {
                        SqlConflictAction::DoNothing => Some(0),
                        SqlConflictAction::DoUpdate(assignments) => vector(assignments, assignment),
                    },
                ])
            }),
            vector(&insert.returning, column),
        ]),
        SqlStatement::Update(update) => sum([
            table(&update.table),
            optional(&update.alias, string),
            vector(&update.assignments, assignment),
            optional(&update.selection, expression),
        ]),
        SqlStatement::Delete(delete) => sum([
            table(&delete.table),
            optional(&delete.alias, string),
            optional(&delete.selection, expression),
        ]),
        _ => return None,
    };
    sum([
        Some(key.capacity()),
        // Arc owns its two counters and the complete inline template.
        Some(
            size_of::<PreparedPostgresStatement>()
                + 2 * size_of::<usize>()
                + align_of::<PreparedPostgresStatement>()
                - 1,
        ),
        template
            .parameters
            .capacity()
            .checked_mul(size_of::<crate::PostgresParameterMetadata>()),
        statement,
    ])
}

fn sum(parts: impl IntoIterator<Item = Option<usize>>) -> Option<usize> {
    parts
        .into_iter()
        .try_fold(0usize, |total, part| total.checked_add(part?))
}

fn string(value: &String) -> Option<usize> {
    Some(value.capacity())
}

fn optional<T>(value: &Option<T>, charge: impl FnOnce(&T) -> Option<usize>) -> Option<usize> {
    value.as_ref().map_or(Some(0), charge)
}

fn vector<T>(values: &Vec<T>, charge: impl Fn(&T) -> Option<usize>) -> Option<usize> {
    let capacity = values.capacity().checked_mul(size_of::<T>())?;
    values
        .iter()
        .try_fold(capacity, |total, item| total.checked_add(charge(item)?))
}

fn table(value: &SqlTableName) -> Option<usize> {
    sum([string(&value.name), optional(&value.schema, string)])
}

fn column(value: &SqlColumnRef) -> Option<usize> {
    sum([string(&value.name), optional(&value.qualifier, string)])
}

fn value(value: &SqlValue) -> Option<usize> {
    match value {
        SqlValue::Literal(Value::String(text)) => string(text),
        SqlValue::Literal(
            Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::Uuid(_),
        )
        | SqlValue::Parameter(_) => Some(0),
        SqlValue::Expression(expr) => boxed_expression(expr),
        SqlValue::Literal(_) => None,
    }
}

fn operand(value: &SqlArithmeticOperand) -> Option<usize> {
    match value {
        SqlArithmeticOperand::Value(item) => self::value(item),
        SqlArithmeticOperand::Column(item) => column(item),
    }
}

fn assignment(item: &SqlAssignment) -> Option<usize> {
    sum([
        string(&item.column),
        match &item.value {
            SqlAssignmentValue::Expression(expr) => expression(expr),
            SqlAssignmentValue::Value(item) => value(item),
            SqlAssignmentValue::Column(item) => column(item),
            SqlAssignmentValue::Arithmetic { left, right, .. } => {
                sum([operand(left), operand(right)])
            }
        },
    ])
}

fn boxed_expression(expr: &Expr) -> Option<usize> {
    size_of::<Expr>().checked_add(expression(expr)?)
}

fn expression(expr: &Expr) -> Option<usize> {
    match &expr.kind {
        ExprKind::Column(item) => column(item),
        ExprKind::Value(item) => value(item),
        ExprKind::Arithmetic { left, right, .. }
        | ExprKind::And(left, right)
        | ExprKind::Or(left, right)
        | ExprKind::Compare { left, right, .. } => {
            sum([boxed_expression(left), boxed_expression(right)])
        }
        ExprKind::Negate(item)
        | ExprKind::Not(item)
        | ExprKind::Cast {
            expression: item, ..
        }
        | ExprKind::IsNull {
            expression: item, ..
        } => boxed_expression(item),
        ExprKind::Function {
            name,
            arguments,
            filter,
            ..
        } => sum([
            string(name),
            vector(arguments, |argument| match argument {
                SqlFunctionArgument::Expression(expr) => expression(expr),
                SqlFunctionArgument::Wildcard => Some(0),
            }),
            optional(filter, |expr| boxed_expression(expr)),
        ]),
        ExprKind::InList { left, values, .. } => {
            sum([boxed_expression(left), vector(values, expression)])
        }
        ExprKind::Like { left, pattern, .. } => {
            sum([boxed_expression(left), boxed_expression(pattern)])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_heap_charge_includes_spare_capacity_and_nested_expression_owners() {
        let key = String::from("UPDATE public.items AS i SET body = $1 WHERE i.id IN ($2, $3)");
        let mut template = crate::prepare_postgres_sql(&key).unwrap();
        let before = heap_charge(&key, &template).unwrap();
        let SqlStatement::Update(update) = &mut template.statement else {
            panic!("UPDATE template")
        };
        update.table.name.reserve(4096);
        template.parameters.reserve(100);
        let after = heap_charge(&key, &template).unwrap();
        assert!(after >= before + 4096 + 100 * size_of::<crate::PostgresParameterMetadata>());

        let mut key_with_spare = key.clone();
        key_with_spare.reserve(8192);
        assert!(heap_charge(&key_with_spare, &template).unwrap() >= after + 8192);
        let literal = crate::prepare_postgres_sql("INSERT INTO items (id, body) VALUES ($1, 'payload') ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body RETURNING id").unwrap();
        assert!(heap_charge(&key, &literal).is_some());
        let ddl =
            crate::prepare_postgres_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)").unwrap();
        assert!(heap_charge(&key, &ddl).is_none());
    }
}
