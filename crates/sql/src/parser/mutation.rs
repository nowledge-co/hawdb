use super::{
    lower_literal_expr, lower_predicate, lower_table_factor, lower_table_name, normalize_ident,
    object_name_parts,
};
use crate::ast::*;
use skein_core::{Result, SkeinError};
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, Expr, FromTable, OnConflictAction, OnInsert,
    SelectItem, SetExpr, TableObject,
};

pub(super) fn lower_insert_statement(insert: &sqlparser::ast::Insert) -> Result<SqlStatement> {
    super::reject_unsupported_clauses(
        "INSERT",
        &[
            ("optimizer hint", insert.optimizer_hint.is_some()),
            ("OR conflict action", insert.or.is_some()),
            ("IGNORE", insert.ignore),
            ("missing INTO", !insert.into),
            ("table alias", insert.table_alias.is_some()),
            ("OVERWRITE", insert.overwrite),
            ("SET assignments", !insert.assignments.is_empty()),
            ("PARTITION", insert.partitioned.is_some()),
            ("columns after PARTITION", !insert.after_columns.is_empty()),
            ("TABLE keyword", insert.has_table_keyword),
            ("REPLACE INTO", insert.replace_into),
            ("priority", insert.priority.is_some()),
            ("row alias", insert.insert_alias.is_some()),
            ("SETTINGS", insert.settings.is_some()),
            ("FORMAT", insert.format_clause.is_some()),
        ],
    )?;
    let TableObject::TableName(table) = &insert.table else {
        return Err(SkeinError::Semantic(
            "INSERT table functions are not supported".to_string(),
        ));
    };
    let Some(source) = &insert.source else {
        return Err(SkeinError::Semantic(
            "INSERT requires an explicit VALUES clause".to_string(),
        ));
    };
    if source.with.is_some()
        || source.order_by.is_some()
        || source.limit_clause.is_some()
        || source.fetch.is_some()
        || !source.locks.is_empty()
        || source.for_clause.is_some()
        || source.settings.is_some()
        || source.format_clause.is_some()
        || !source.pipe_operators.is_empty()
    {
        return Err(SkeinError::Semantic(
            "INSERT supports VALUES rows only".to_string(),
        ));
    }
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(SkeinError::Semantic(
            "INSERT supports VALUES rows only".to_string(),
        ));
    };
    if values.explicit_row || values.value_keyword {
        return Err(SkeinError::Semantic(
            "INSERT requires the PostgreSQL VALUES form".to_string(),
        ));
    }
    let columns = insert
        .columns
        .iter()
        .map(normalize_ident)
        .collect::<Vec<_>>();
    let rows = values
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(lower_literal_expr)
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    if rows.is_empty() || rows.iter().any(|row| row.len() != columns.len()) {
        return Err(SkeinError::Semantic(
            "INSERT VALUES rows must match the explicit column list".to_string(),
        ));
    }
    Ok(SqlStatement::Insert(InsertStatement {
        table: lower_table_name(table)?,
        columns,
        rows,
        on_conflict: insert.on.as_ref().map(lower_on_conflict).transpose()?,
        returning: insert
            .returning
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(
                    expr @ (Expr::Identifier(_) | Expr::CompoundIdentifier(_)),
                ) => super::lower_column_expr(expr),
                SelectItem::UnnamedExpr(_)
                | SelectItem::ExprWithAlias { .. }
                | SelectItem::QualifiedWildcard(_, _)
                | SelectItem::Wildcard(_) => Err(SkeinError::Semantic(
                    "INSERT RETURNING supports column references only".to_string(),
                )),
            })
            .collect::<Result<Vec<_>>>()?,
    }))
}

fn lower_on_conflict(on_insert: &OnInsert) -> Result<SqlOnConflict> {
    let OnInsert::OnConflict(conflict) = on_insert else {
        return Err(SkeinError::Semantic(
            "only PostgreSQL ON CONFLICT is supported".to_string(),
        ));
    };
    let Some(sqlparser::ast::ConflictTarget::Columns(columns)) = &conflict.conflict_target else {
        return Err(SkeinError::Semantic(
            "ON CONFLICT requires an explicit column target".to_string(),
        ));
    };
    let action = match &conflict.action {
        OnConflictAction::DoNothing => SqlConflictAction::DoNothing,
        OnConflictAction::DoUpdate(update) => {
            if update.selection.is_some() {
                return Err(SkeinError::Semantic(
                    "ON CONFLICT DO UPDATE WHERE is not supported".to_string(),
                ));
            }
            SqlConflictAction::DoUpdate(lower_assignments(&update.assignments)?)
        }
    };
    Ok(SqlOnConflict {
        columns: columns.iter().map(normalize_ident).collect(),
        action,
    })
}

pub(super) fn lower_update_statement(update: &sqlparser::ast::Update) -> Result<SqlStatement> {
    if update.optimizer_hint.is_some()
        || update.from.is_some()
        || update.returning.is_some()
        || update.or.is_some()
        || update.limit.is_some()
        || !update.table.joins.is_empty()
    {
        return Err(SkeinError::Semantic(
            "unsupported PostgreSQL UPDATE clause".to_string(),
        ));
    }
    let (table, alias) = lower_table_factor(&update.table.relation)?;
    Ok(SqlStatement::Update(UpdateStatement {
        table,
        alias,
        assignments: lower_assignments(&update.assignments)?,
        selection: update.selection.as_ref().map(lower_predicate).transpose()?,
    }))
}

fn lower_assignments(assignments: &[Assignment]) -> Result<Vec<SqlAssignment>> {
    assignments
        .iter()
        .map(|assignment| {
            let AssignmentTarget::ColumnName(name) = &assignment.target else {
                return Err(SkeinError::Semantic(
                    "tuple assignments are not supported".to_string(),
                ));
            };
            let parts = object_name_parts(name)?;
            let [column] = parts.as_slice() else {
                return Err(SkeinError::Semantic(
                    "assignment targets must be unqualified columns".to_string(),
                ));
            };
            Ok(SqlAssignment {
                column: column.clone(),
                value: lower_assignment_value(&assignment.value)?,
            })
        })
        .collect()
}

fn lower_assignment_value(expr: &Expr) -> Result<SqlAssignmentValue> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            Ok(SqlAssignmentValue::Column(super::lower_column_expr(expr)?))
        }
        Expr::BinaryOp { left, op, right }
            if matches!(op, BinaryOperator::Plus | BinaryOperator::Minus) =>
        {
            let left = lower_arithmetic_operand(left)?;
            let right = lower_arithmetic_operand(right)?;
            match (op, &left, &right) {
                (
                    BinaryOperator::Plus,
                    SqlArithmeticOperand::Column(_),
                    SqlArithmeticOperand::Value(_),
                )
                | (
                    BinaryOperator::Plus,
                    SqlArithmeticOperand::Value(_),
                    SqlArithmeticOperand::Column(_),
                )
                | (
                    BinaryOperator::Minus,
                    SqlArithmeticOperand::Column(_),
                    SqlArithmeticOperand::Value(_),
                ) => {}
                _ => {
                    return Err(SkeinError::Semantic(
                        "UPDATE arithmetic supports column + value, value + column, and column - value"
                            .to_string(),
                    ));
                }
            }
            Ok(SqlAssignmentValue::Arithmetic {
                left,
                operator: match op {
                    BinaryOperator::Plus => SqlArithmeticOperator::Add,
                    BinaryOperator::Minus => SqlArithmeticOperator::Subtract,
                    _ => unreachable!("arithmetic operator was filtered before lowering"),
                },
                right,
            })
        }
        Expr::BinaryOp { .. } => Err(SkeinError::Semantic(
            "UPDATE arithmetic supports column + value, value + column, and column - value"
                .to_string(),
        )),
        _ => lower_literal_expr(expr).map(SqlAssignmentValue::Value),
    }
}

fn lower_arithmetic_operand(expr: &Expr) -> Result<SqlArithmeticOperand> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Ok(SqlArithmeticOperand::Column(
            super::lower_column_expr(expr)?,
        )),
        _ => lower_literal_expr(expr).map(SqlArithmeticOperand::Value),
    }
}

pub(super) fn lower_delete_statement(delete: &sqlparser::ast::Delete) -> Result<SqlStatement> {
    if delete.optimizer_hint.is_some()
        || !delete.tables.is_empty()
        || delete.using.is_some()
        || delete.returning.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return Err(SkeinError::Semantic(
            "unsupported PostgreSQL DELETE clause".to_string(),
        ));
    }
    let from = match &delete.from {
        FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
    };
    let [from] = from.as_slice() else {
        return Err(SkeinError::Semantic(
            "DELETE supports exactly one base table".to_string(),
        ));
    };
    if !from.joins.is_empty() {
        return Err(SkeinError::Semantic(
            "DELETE joins are not supported".to_string(),
        ));
    }
    let (table, alias) = lower_table_factor(&from.relation)?;
    Ok(SqlStatement::Delete(DeleteStatement {
        table,
        alias,
        selection: delete.selection.as_ref().map(lower_predicate).transpose()?,
    }))
}
