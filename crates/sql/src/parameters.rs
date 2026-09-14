use crate::{
    DeleteStatement, ExprKind, InsertStatement, SelectProjection, SelectStatement,
    SqlArithmeticOperand, SqlAssignment, SqlAssignmentValue, SqlBound, SqlColumnDefault,
    SqlExpression, SqlStatement, SqlValue, UpdateStatement,
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
                    collect_column_default_parameter(value, positions);
                }
            }
        }
        SqlStatement::AlterTableAddColumn(alter) => {
            if let Some(value) = &alter.column.default {
                collect_column_default_parameter(value, positions);
            }
        }
        SqlStatement::CreateIndex(_) => {}
    }
}

fn collect_column_default_parameter(default: &SqlColumnDefault, positions: &mut BTreeSet<usize>) {
    if let SqlColumnDefault::Literal(value) = default {
        collect_value_parameter(value, positions);
    }
}

fn collect_select_parameters(select: &SelectStatement, positions: &mut BTreeSet<usize>) {
    for projection in &select.projection {
        if let SelectProjection::Expression { expression, .. } = projection {
            collect_expression_parameters(expression, positions);
        }
    }
    for join in &select.joins {
        collect_expression_parameters(&join.on, positions);
    }
    if let Some(predicate) = &select.selection {
        collect_expression_parameters(predicate, positions);
    }
    if let Some(predicate) = &select.having {
        collect_expression_parameters(predicate, positions);
    }
    for order in &select.order_by {
        collect_expression_parameters(&order.expression, positions);
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
        collect_expression_parameters(predicate, positions);
    }
}

fn collect_delete_parameters(delete: &DeleteStatement, positions: &mut BTreeSet<usize>) {
    if let Some(predicate) = &delete.selection {
        collect_expression_parameters(predicate, positions);
    }
}

fn collect_assignment_parameters(assignments: &[SqlAssignment], positions: &mut BTreeSet<usize>) {
    for assignment in assignments {
        match &assignment.value {
            SqlAssignmentValue::Value(value) => collect_value_parameter(value, positions),
            SqlAssignmentValue::Column(_) => {}
            SqlAssignmentValue::Arithmetic { left, right, .. } => {
                collect_arithmetic_operand_parameter(left, positions);
                collect_arithmetic_operand_parameter(right, positions);
            }
        }
    }
}

fn collect_arithmetic_operand_parameter(
    operand: &SqlArithmeticOperand,
    positions: &mut BTreeSet<usize>,
) {
    if let SqlArithmeticOperand::Value(value) = operand {
        collect_value_parameter(value, positions);
    }
}

fn collect_expression_parameters(expression: &SqlExpression, positions: &mut BTreeSet<usize>) {
    expression.visit(&mut |expression| {
        if let ExprKind::Value(value) = &expression.kind {
            collect_value_parameter(value, positions);
        }
    });
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
