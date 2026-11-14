// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Binds the #1516 numeric-literal coercion to the current relational schema
//! and parameters. Statement templates stay schema- and parameter-neutral;
//! SELECT rewrites its per-execution copy, and mutations clone the borrowed
//! template only when a literal actually meets a DOUBLE PRECISION operand.

use hawdb_core::{LogicalType, Result, Value};
use hawdb_expression::sql::NumericLiteralCoercion;
use hawdb_sql::{
    Expr, SelectProjection, SelectStatement, SqlAssignment, SqlAssignmentValue, SqlColumnRef,
    SqlConflictAction, SqlStatement, SqlValue,
};
use hawdb_storage::relational::{RelationalState, RelationalTableSchema};
use std::borrow::Cow;

fn column_type(schema: &RelationalTableSchema, name: &str) -> Option<LogicalType> {
    schema
        .column_position(name)
        .map(|position| schema.columns[position].scalar_type.logical_type())
}

/// Coerces every SELECT expression position: projection, WHERE, JOIN ON,
/// HAVING and ORDER BY. A column resolves by qualifier (alias or table name)
/// or, unqualified, by a unique visible relation; anything else stays untyped
/// for the binder to report.
pub(crate) fn coerce_select(
    select: &mut SelectStatement,
    state: &RelationalState,
    parameters: &[Value],
) -> Result<()> {
    let relations = select
        .from
        .iter()
        .map(|table| (select.from_alias.as_deref(), table))
        .chain(
            select
                .joins
                .iter()
                .map(|join| (join.alias.as_deref(), &join.table)),
        )
        .filter_map(|(alias, table)| {
            state
                .table_schema(&table.name)
                .map(|schema| (alias.map(str::to_owned), table.name.clone(), schema))
        })
        .collect::<Vec<_>>();
    let resolve = |column: &SqlColumnRef| {
        let mut types = relations
            .iter()
            .filter(|(alias, table, _)| {
                column.qualifier.as_deref().is_none_or(|qualifier| {
                    alias.as_deref() == Some(qualifier) || qualifier == table
                })
            })
            .filter_map(|(_, _, schema)| column_type(schema, &column.name));
        let first = types.next();
        types.next().is_none().then_some(first).flatten()
    };
    let coercion = NumericLiteralCoercion::new(resolve, parameters);
    let expressions = select
        .projection
        .iter_mut()
        .filter_map(|projection| match projection {
            SelectProjection::Expression { expression, .. } => Some(expression),
            SelectProjection::Wildcard => None,
        })
        .chain(select.joins.iter_mut().map(|join| &mut join.on))
        .chain(select.selection.as_mut())
        .chain(select.having.as_mut())
        .chain(select.order_by.iter_mut().map(|item| &mut item.expression));
    for expression in expressions {
        coercion.apply(expression)?;
    }
    Ok(())
}

/// Coerces UPDATE SET/WHERE, DELETE WHERE and INSERT VALUES/ON CONFLICT
/// expressions. Mutations name one table, so columns (including `excluded`)
/// resolve by name against it.
pub(crate) fn coerce_mutation<'a>(
    statement: &'a SqlStatement,
    state: &RelationalState,
    parameters: &[Value],
) -> Result<Cow<'a, SqlStatement>> {
    let table = match statement {
        SqlStatement::Insert(insert) => &insert.table,
        SqlStatement::Update(update) => &update.table,
        SqlStatement::Delete(delete) => &delete.table,
        _ => return Ok(Cow::Borrowed(statement)),
    };
    let schema = state.table_schema(&table.name);
    let coercion = NumericLiteralCoercion::new(
        |column: &SqlColumnRef| schema.and_then(|schema| column_type(schema, &column.name)),
        parameters,
    );
    if !mutation_expressions(statement).any(|expression| coercion.needed(expression)) {
        return Ok(Cow::Borrowed(statement));
    }
    let mut statement = statement.clone();
    for expression in mutation_expressions_mut(&mut statement) {
        coercion.apply(expression)?;
    }
    Ok(Cow::Owned(statement))
}

fn mutation_expressions(statement: &SqlStatement) -> Box<dyn Iterator<Item = &Expr> + '_> {
    fn value(value: &SqlValue) -> Option<&Expr> {
        match value {
            SqlValue::Expression(expression) => Some(expression),
            SqlValue::Literal(_) | SqlValue::Parameter(_) => None,
        }
    }
    fn assignments(assignments: &[SqlAssignment]) -> impl Iterator<Item = &Expr> {
        assignments
            .iter()
            .filter_map(|assignment| match &assignment.value {
                SqlAssignmentValue::Expression(expression) => Some(expression),
                SqlAssignmentValue::Value(literal) => value(literal),
                SqlAssignmentValue::Column(_) | SqlAssignmentValue::Arithmetic { .. } => None,
            })
    }
    match statement {
        SqlStatement::Insert(insert) => Box::new(
            insert.rows.iter().flatten().filter_map(value).chain(
                insert
                    .on_conflict
                    .iter()
                    .flat_map(|conflict| match &conflict.action {
                        SqlConflictAction::DoUpdate(update) => assignments(update).collect(),
                        SqlConflictAction::DoNothing => Vec::new(),
                    }),
            ),
        ),
        SqlStatement::Update(update) => {
            Box::new(assignments(&update.assignments).chain(update.selection.as_ref()))
        }
        SqlStatement::Delete(delete) => Box::new(delete.selection.iter()),
        _ => Box::new(std::iter::empty()),
    }
}

fn mutation_expressions_mut(statement: &mut SqlStatement) -> Vec<&mut Expr> {
    fn value(value: &mut SqlValue) -> Option<&mut Expr> {
        match value {
            SqlValue::Expression(expression) => Some(expression),
            SqlValue::Literal(_) | SqlValue::Parameter(_) => None,
        }
    }
    fn assignments(assignments: &mut [SqlAssignment]) -> impl Iterator<Item = &mut Expr> {
        assignments
            .iter_mut()
            .filter_map(|assignment| match &mut assignment.value {
                SqlAssignmentValue::Expression(expression) => Some(expression),
                SqlAssignmentValue::Value(literal) => value(literal),
                SqlAssignmentValue::Column(_) | SqlAssignmentValue::Arithmetic { .. } => None,
            })
    }
    match statement {
        SqlStatement::Insert(insert) => {
            let mut expressions = insert
                .rows
                .iter_mut()
                .flatten()
                .filter_map(value)
                .collect::<Vec<_>>();
            if let Some(SqlConflictAction::DoUpdate(update)) = insert
                .on_conflict
                .as_mut()
                .map(|conflict| &mut conflict.action)
            {
                expressions.extend(assignments(update));
            }
            expressions
        }
        SqlStatement::Update(update) => assignments(&mut update.assignments)
            .chain(update.selection.as_mut())
            .collect(),
        SqlStatement::Delete(delete) => delete.selection.iter_mut().collect(),
        _ => Vec::new(),
    }
}
