use crate::{
    DeleteStatement, InsertStatement, SelectProjection, SelectStatement, SqlAssignment,
    SqlAssignmentValue, SqlBound, SqlExpression, SqlFunctionArgument, SqlPredicate, SqlStatement,
    SqlValue, UpdateStatement,
};
use skein_core::{Result, SkeinError};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedPostgresStatement {
    pub statement: SqlStatement,
    pub parameters: Vec<PostgresParameterMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresParameterMetadata {
    pub position: usize,
}

pub fn prepare_postgres_sql(input: &str) -> Result<PreparedPostgresStatement> {
    let statement = crate::parse_postgres_sql(input)?;
    let mut positions = BTreeSet::new();
    collect_statement_parameters(&statement, &mut positions);
    if !positions.is_empty()
        && matches!(
            &statement,
            SqlStatement::CreateTable(_)
                | SqlStatement::CreateIndex(_)
                | SqlStatement::AlterTableAddColumn(_)
        )
    {
        return Err(SkeinError::Semantic(
            "PostgreSQL schema statements do not accept parameters".to_string(),
        ));
    }
    let maximum = positions.last().copied().unwrap_or(0);
    let expected = (1..=maximum).collect::<BTreeSet<_>>();
    if positions != expected {
        return Err(SkeinError::Semantic(format!(
            "PostgreSQL parameter positions must be dense from $1 through ${maximum}; found {positions:?}"
        )));
    }
    Ok(PreparedPostgresStatement {
        statement,
        parameters: positions
            .into_iter()
            .map(|position| PostgresParameterMetadata { position })
            .collect(),
    })
}

fn collect_statement_parameters(statement: &SqlStatement, positions: &mut BTreeSet<usize>) {
    match statement {
        SqlStatement::Explain(explain) => {
            collect_statement_parameters(&explain.statement, positions)
        }
        SqlStatement::Select(select) => collect_select_parameters(select, positions),
        SqlStatement::Insert(insert) => collect_insert_parameters(insert, positions),
        SqlStatement::Update(update) => collect_update_parameters(update, positions),
        SqlStatement::Delete(delete) => collect_delete_parameters(delete, positions),
        SqlStatement::CreateTable(create) => {
            for column in &create.columns {
                if let Some(value) = &column.default {
                    collect_value_parameter(value, positions);
                }
            }
        }
        SqlStatement::AlterTableAddColumn(alter) => {
            if let Some(value) = &alter.column.default {
                collect_value_parameter(value, positions);
            }
        }
        SqlStatement::CreateIndex(_) => {}
    }
}

fn collect_select_parameters(select: &SelectStatement, positions: &mut BTreeSet<usize>) {
    for projection in &select.projection {
        if let SelectProjection::Expression { expression, .. } = projection {
            collect_expression_parameters(expression, positions);
        }
    }
    for join in &select.joins {
        collect_predicate_parameters(&join.on, positions);
    }
    if let Some(predicate) = &select.selection {
        collect_predicate_parameters(predicate, positions);
    }
    collect_bound_parameter(select.limit, positions);
    collect_bound_parameter(select.offset, positions);
}

fn collect_insert_parameters(insert: &InsertStatement, positions: &mut BTreeSet<usize>) {
    for row in &insert.rows {
        for value in row {
            collect_value_parameter(value, positions);
        }
    }
    if let Some(conflict) = &insert.on_conflict
        && let crate::SqlConflictAction::DoUpdate(assignments) = &conflict.action
    {
        collect_assignment_parameters(assignments, positions);
    }
}

fn collect_update_parameters(update: &UpdateStatement, positions: &mut BTreeSet<usize>) {
    collect_assignment_parameters(&update.assignments, positions);
    if let Some(predicate) = &update.selection {
        collect_predicate_parameters(predicate, positions);
    }
}

fn collect_delete_parameters(delete: &DeleteStatement, positions: &mut BTreeSet<usize>) {
    if let Some(predicate) = &delete.selection {
        collect_predicate_parameters(predicate, positions);
    }
}

fn collect_assignment_parameters(assignments: &[SqlAssignment], positions: &mut BTreeSet<usize>) {
    for assignment in assignments {
        if let SqlAssignmentValue::Value(value) = &assignment.value {
            collect_value_parameter(value, positions);
        }
    }
}

fn collect_expression_parameters(expression: &SqlExpression, positions: &mut BTreeSet<usize>) {
    match expression {
        SqlExpression::Value(value) => collect_value_parameter(value, positions),
        SqlExpression::Function { arguments, .. } => {
            for argument in arguments {
                if let SqlFunctionArgument::Expression(expression) = argument {
                    collect_expression_parameters(expression, positions);
                }
            }
        }
        SqlExpression::Column(_) => {}
    }
}

fn collect_predicate_parameters(predicate: &SqlPredicate, positions: &mut BTreeSet<usize>) {
    match predicate {
        SqlPredicate::And(left, right) | SqlPredicate::Or(left, right) => {
            collect_predicate_parameters(left, positions);
            collect_predicate_parameters(right, positions);
        }
        SqlPredicate::Not(inner) => collect_predicate_parameters(inner, positions),
        SqlPredicate::Compare { right, .. } => collect_value_parameter(right, positions),
        SqlPredicate::InList { values, .. } => {
            for value in values {
                collect_value_parameter(value, positions);
            }
        }
        SqlPredicate::CompareColumns { .. } | SqlPredicate::IsNull { .. } => {}
    }
}

fn collect_value_parameter(value: &SqlValue, positions: &mut BTreeSet<usize>) {
    if let SqlValue::Parameter(position) = value {
        positions.insert(*position);
    }
}

fn collect_bound_parameter(bound: Option<SqlBound>, positions: &mut BTreeSet<usize>) {
    if let Some(SqlBound::Parameter(position)) = bound {
        positions.insert(position);
    }
}
