//! Storage-neutral RowPage DDL/DML compilation; transaction execution stays outside.

use crate::{
    bind_relational_value, coerce_relational_value, compile_column, reject_non_public_schema,
};
use skein_core::{Result, SkeinError, Value};
use skein_sql::{
    AlterTableAddColumnStatement, CreateIndexStatement, CreateTableStatement, ExprKind,
    SqlArithmeticOperand, SqlAssignmentValue, SqlComparisonOp, SqlConflictAction, SqlPredicate,
    SqlReferentialAction, SqlStatement, SqlTableConstraint, SqlTableStorage, SqlValue,
};
use skein_storage::{
    RelationalBigIntArithmeticOperator, RelationalBigIntOperand, RelationalColumnDefault,
    RelationalColumnSchema, RelationalComparisonOp, RelationalConflictAction,
    RelationalForeignKeySchema, RelationalIndexSchema, RelationalInsertMode, RelationalPredicate,
    RelationalReferentialAction, RelationalRow, RelationalScalarType, RelationalState,
    RelationalTableSchema, RelationalTransaction, RelationalUpdateAssignment,
    RelationalUpdateValue, RelationalUpsertAssignment, RelationalUpsertValue, RelationalValue,
    RelationalWrite,
};

pub fn compile_relational_statement_sql(
    sql: &str,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<RelationalTransaction> {
    compile_relational_statement_sql_with_result(sql, parameters, state)
        .map(|compiled| compiled.transaction)
}

#[derive(Debug, Clone)]
pub struct CompiledRelationalStatement {
    pub transaction: RelationalTransaction,
    pub returning: Option<RelationalReturningProjection>,
}

#[derive(Debug, Clone)]
pub struct RelationalReturningProjection {
    pub table: String,
    pub columns: Vec<String>,
}

pub fn compile_relational_statement_sql_with_result(
    sql: &str,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<CompiledRelationalStatement> {
    let prepared = skein_sql::prepare_postgres_sql(sql)?;
    if prepared.parameters.len() != parameters.len() {
        return Err(SkeinError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    match prepared.statement {
        statement @ (SqlStatement::CreateTable(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_)) => Ok(CompiledRelationalStatement {
            transaction: RelationalTransaction {
                writes: compile_schema_statement(statement)?,
            },
            returning: None,
        }),
        statement => compile_relational_mutation(statement, parameters, state),
    }
}

fn compile_relational_mutation(
    statement: SqlStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<CompiledRelationalStatement> {
    let mut returning = None;
    let write = match statement {
        SqlStatement::Insert(insert) => {
            reject_non_public_schema(insert.table.schema.as_deref())?;
            let schema = state.table_schema(&insert.table.name).ok_or_else(|| {
                SkeinError::Semantic(format!("unknown relational table {}", insert.table.name))
            })?;
            if !insert.returning.is_empty() {
                for column in &insert.returning {
                    if column
                        .qualifier
                        .as_deref()
                        .is_some_and(|qualifier| qualifier != insert.table.name)
                    {
                        return Err(SkeinError::Semantic(format!(
                            "INSERT RETURNING has unknown qualifier {qualifier}",
                            qualifier = column.qualifier.as_deref().unwrap_or_default()
                        )));
                    }
                    if schema.column_position(&column.name).is_none() {
                        return Err(SkeinError::Semantic(format!(
                            "table {} has no column {}",
                            schema.name, column.name
                        )));
                    }
                }
                if matches!(
                    insert.on_conflict.as_ref().map(|conflict| &conflict.action),
                    Some(SqlConflictAction::DoUpdate(_))
                ) {
                    return Err(SkeinError::Semantic(
                        "INSERT RETURNING with ON CONFLICT DO UPDATE is not supported".to_string(),
                    ));
                }
                returning = Some(RelationalReturningProjection {
                    table: insert.table.name.clone(),
                    columns: insert
                        .returning
                        .iter()
                        .map(|column| column.name.clone())
                        .collect(),
                });
            }
            let mut positions = Vec::with_capacity(insert.columns.len());
            let mut unique = std::collections::BTreeSet::new();
            for column in &insert.columns {
                let position = schema.column_position(column).ok_or_else(|| {
                    SkeinError::Semantic(format!("table {} has no column {column}", schema.name))
                })?;
                if !unique.insert(position) {
                    return Err(SkeinError::Semantic(format!(
                        "INSERT column {column} is specified more than once"
                    )));
                }
                positions.push(position);
            }
            let rows = insert
                .rows
                .into_iter()
                .map(|values| {
                    let mut row = schema
                        .columns
                        .iter()
                        .map(materialize_column_default)
                        .collect::<Result<Vec<_>>>()?;
                    for ((position, value), column_name) in
                        positions.iter().zip(values).zip(insert.columns.iter())
                    {
                        row[*position] = bind_relational_value_as(
                            value,
                            parameters,
                            schema.columns[*position].scalar_type,
                        )
                        .map_err(|error| {
                            SkeinError::Semantic(format!(
                                "failed to bind INSERT column {column_name}: {error}"
                            ))
                        })?;
                    }
                    Ok(RelationalRow::new(row))
                })
                .collect::<Result<Vec<_>>>()?;
            if let Some(conflict) = insert.on_conflict {
                let action = match conflict.action {
                    SqlConflictAction::DoNothing => RelationalConflictAction::DoNothing,
                    SqlConflictAction::DoUpdate(assignments) => RelationalConflictAction::Update(
                        assignments
                            .into_iter()
                            .map(|assignment| {
                                let value = match assignment.value {
                                    SqlAssignmentValue::Column(column)
                                        if column.qualifier.as_deref() == Some("excluded") =>
                                    {
                                        RelationalUpsertValue::ExcludedColumn(column.name)
                                    }
                                    SqlAssignmentValue::Column(_) => {
                                        return Err(SkeinError::Semantic(
                                            "ON CONFLICT assignments only support EXCLUDED columns"
                                                .to_string(),
                                        ));
                                    }
                                    SqlAssignmentValue::Value(value) => {
                                        RelationalUpsertValue::Value(bind_relational_value(
                                            value, parameters,
                                        )?)
                                    }
                                    SqlAssignmentValue::Arithmetic { .. } => {
                                        return Err(SkeinError::Semantic(
                                            "ON CONFLICT assignments do not support arithmetic expressions"
                                                .to_string(),
                                        ));
                                    }
                                };
                                Ok(RelationalUpsertAssignment {
                                    column: assignment.column,
                                    value,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ),
                };
                RelationalWrite::Upsert {
                    table: insert.table.name,
                    rows,
                    conflict_columns: conflict.columns,
                    action,
                }
            } else {
                RelationalWrite::Insert {
                    table: insert.table.name,
                    rows,
                    mode: RelationalInsertMode::Error,
                }
            }
        }
        SqlStatement::Delete(delete) => {
            reject_non_public_schema(delete.table.schema.as_deref())?;
            let schema = state.table_schema(&delete.table.name).ok_or_else(|| {
                SkeinError::Semantic(format!("unknown relational table {}", delete.table.name))
            })?;
            let selection = delete.selection.ok_or_else(|| {
                SkeinError::Semantic(
                    "unbounded relational DELETE requires an explicit qualified workflow"
                        .to_string(),
                )
            })?;
            RelationalWrite::DeleteWhere {
                table: delete.table.name.clone(),
                predicate: compile_mutation_predicate(
                    selection,
                    parameters,
                    schema,
                    delete.alias.as_deref(),
                    &delete.table.name,
                )?,
            }
        }
        SqlStatement::Update(update) => {
            reject_non_public_schema(update.table.schema.as_deref())?;
            let schema = state.table_schema(&update.table.name).ok_or_else(|| {
                SkeinError::Semantic(format!("unknown relational table {}", update.table.name))
            })?;
            let selection = update.selection.ok_or_else(|| {
                SkeinError::Semantic(
                    "unbounded relational UPDATE requires an explicit qualified workflow"
                        .to_string(),
                )
            })?;
            let assignments = update
                .assignments
                .into_iter()
                .map(|assignment| {
                    if schema.column_position(&assignment.column).is_none() {
                        return Err(SkeinError::Semantic(format!(
                            "table {} has no column {}",
                            schema.name, assignment.column
                        )));
                    }
                    let target_type = schema.columns[schema
                        .column_position(&assignment.column)
                        .expect("assignment column was validated")]
                    .scalar_type;
                    let value = match assignment.value {
                        SqlAssignmentValue::Column(column) => {
                            validate_mutation_column(
                                &column,
                                schema,
                                update.alias.as_deref(),
                                &update.table.name,
                            )?;
                            RelationalUpdateValue::Column(column.name)
                        }
                        SqlAssignmentValue::Value(value) => RelationalUpdateValue::Value(
                            bind_relational_value_as(value, parameters, target_type)?,
                        ),
                        SqlAssignmentValue::Arithmetic {
                            left,
                            operator,
                            right,
                        } => {
                            if target_type != RelationalScalarType::BigInt {
                                return Err(SkeinError::Semantic(format!(
                                    "UPDATE arithmetic assignment target {} must be BIGINT",
                                    assignment.column
                                )));
                            }
                            RelationalUpdateValue::BigIntArithmetic {
                                left: compile_bigint_arithmetic_operand(
                                    left,
                                    parameters,
                                    schema,
                                    update.alias.as_deref(),
                                    &update.table.name,
                                )?,
                                operator: match operator {
                                    skein_sql::SqlArithmeticOperator::Add => {
                                        RelationalBigIntArithmeticOperator::Add
                                    }
                                    skein_sql::SqlArithmeticOperator::Subtract => {
                                        RelationalBigIntArithmeticOperator::Subtract
                                    }
                                },
                                right: compile_bigint_arithmetic_operand(
                                    right,
                                    parameters,
                                    schema,
                                    update.alias.as_deref(),
                                    &update.table.name,
                                )?,
                            }
                        }
                    };
                    Ok(RelationalUpdateAssignment {
                        column: assignment.column,
                        value,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            RelationalWrite::UpdateWhere {
                table: update.table.name.clone(),
                assignments,
                predicate: compile_mutation_predicate(
                    selection,
                    parameters,
                    schema,
                    update.alias.as_deref(),
                    &update.table.name,
                )?,
            }
        }
        SqlStatement::Select(_)
        | SqlStatement::Explain(_)
        | SqlStatement::CreateTable(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_) => {
            return Err(SkeinError::Semantic(
                "relational mutation entrypoint requires INSERT, UPDATE, or DELETE".to_string(),
            ));
        }
    };
    Ok(CompiledRelationalStatement {
        transaction: RelationalTransaction {
            writes: vec![write],
        },
        returning,
    })
}

fn compile_bigint_arithmetic_operand(
    operand: SqlArithmeticOperand,
    parameters: &[Value],
    schema: &RelationalTableSchema,
    alias: Option<&str>,
    table: &str,
) -> Result<RelationalBigIntOperand> {
    match operand {
        SqlArithmeticOperand::Column(column) => {
            validate_mutation_column(&column, schema, alias, table)?;
            let position = schema.column_position(&column.name).ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "table {} has no column {}",
                    schema.name, column.name
                ))
            })?;
            if schema.columns[position].scalar_type != RelationalScalarType::BigInt {
                return Err(SkeinError::Semantic(format!(
                    "UPDATE arithmetic source column {} must be BIGINT",
                    column.name
                )));
            }
            Ok(RelationalBigIntOperand::Column(column.name))
        }
        SqlArithmeticOperand::Value(value) => {
            let value = bind_relational_value_as(value, parameters, RelationalScalarType::BigInt)?;
            if !matches!(value, RelationalValue::BigInt(_)) {
                return Err(SkeinError::Semantic(
                    "UPDATE arithmetic values must be non-null BIGINT scalars".to_string(),
                ));
            }
            Ok(RelationalBigIntOperand::Value(value))
        }
    }
}

fn compile_mutation_predicate(
    predicate: SqlPredicate,
    parameters: &[Value],
    schema: &RelationalTableSchema,
    alias: Option<&str>,
    table: &str,
) -> Result<RelationalPredicate> {
    let compile =
        |predicate| compile_mutation_predicate(predicate, parameters, schema, alias, table);
    Ok(match predicate.kind {
        ExprKind::And(left, right) => {
            RelationalPredicate::And(Box::new(compile(*left)?), Box::new(compile(*right)?))
        }
        ExprKind::Or(left, right) => {
            RelationalPredicate::Or(Box::new(compile(*left)?), Box::new(compile(*right)?))
        }
        ExprKind::Not(predicate) => RelationalPredicate::Not(Box::new(compile(*predicate)?)),
        ExprKind::Compare { left, op, right } => {
            if right.as_column().is_some() {
                return Err(SkeinError::Semantic(
                    "single-table mutation predicates do not support column-to-column comparison"
                        .to_owned(),
                ));
            }
            let left = left.require_column()?.clone();
            let right = mutation_value_expression(*right)?;
            validate_mutation_column(&left, schema, alias, table)?;
            let scalar_type = schema.columns[schema
                .column_position(&left.name)
                .expect("mutation column was validated")]
            .scalar_type;
            RelationalPredicate::Compare {
                column: left.name,
                op: compile_comparison_op(op),
                value: bind_relational_value_as(right, parameters, scalar_type)?,
            }
        }
        ExprKind::Like { .. } => {
            return Err(SkeinError::Semantic(
                "single-table mutation predicates do not support LIKE or ILIKE".to_string(),
            ));
        }
        ExprKind::InList {
            left,
            values,
            negated,
        } => {
            let left = left.require_column()?.clone();
            validate_mutation_column(&left, schema, alias, table)?;
            let scalar_type = schema.columns[schema
                .column_position(&left.name)
                .expect("mutation column was validated")]
            .scalar_type;
            let mut predicates = values
                .into_iter()
                .map(|value| {
                    let value = mutation_value_expression(value)?;
                    Ok(RelationalPredicate::Compare {
                        column: left.name.clone(),
                        op: if negated {
                            RelationalComparisonOp::NotEq
                        } else {
                            RelationalComparisonOp::Eq
                        },
                        value: bind_relational_value_as(value, parameters, scalar_type)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter();
            let first = predicates.next().ok_or_else(|| {
                SkeinError::Semantic("mutation IN list must not be empty".to_string())
            })?;
            predicates.fold(first, |left, right| {
                if negated {
                    RelationalPredicate::And(Box::new(left), Box::new(right))
                } else {
                    RelationalPredicate::Or(Box::new(left), Box::new(right))
                }
            })
        }
        ExprKind::IsNull {
            expression,
            negated,
        } => {
            let column = expression.require_column()?.clone();
            validate_mutation_column(&column, schema, alias, table)?;
            RelationalPredicate::IsNull {
                column: column.name,
                negated,
            }
        }
        _ => {
            return Err(SkeinError::Semantic(
                "unsupported single-table mutation predicate".to_owned(),
            ))
        }
    })
}

fn mutation_value_expression(expression: skein_sql::Expr) -> Result<SqlValue> {
    match expression.kind {
        ExprKind::Value(value) => Ok(value),
        _ => Err(SkeinError::Semantic(
            "mutation predicate requires a literal or parameter".to_owned(),
        )),
    }
}

fn validate_mutation_column(
    column: &skein_sql::SqlColumnRef,
    schema: &RelationalTableSchema,
    alias: Option<&str>,
    table: &str,
) -> Result<()> {
    if column
        .qualifier
        .as_deref()
        .is_some_and(|qualifier| Some(qualifier) != alias && qualifier != table)
    {
        return Err(SkeinError::Semantic(format!(
            "mutation predicate has unknown qualifier {}",
            column.qualifier.as_deref().unwrap_or_default()
        )));
    }
    if schema.column_position(&column.name).is_none() {
        return Err(SkeinError::Semantic(format!(
            "table {} has no column {}",
            schema.name, column.name
        )));
    }
    Ok(())
}

fn compile_comparison_op(op: SqlComparisonOp) -> RelationalComparisonOp {
    match op {
        SqlComparisonOp::Eq => RelationalComparisonOp::Eq,
        SqlComparisonOp::NotEq => RelationalComparisonOp::NotEq,
        SqlComparisonOp::Lt => RelationalComparisonOp::Lt,
        SqlComparisonOp::Lte => RelationalComparisonOp::Lte,
        SqlComparisonOp::Gt => RelationalComparisonOp::Gt,
        SqlComparisonOp::Gte => RelationalComparisonOp::Gte,
    }
}

fn compile_schema_statement(statement: SqlStatement) -> Result<Vec<RelationalWrite>> {
    match statement {
        SqlStatement::CreateTable(create) => Ok(vec![RelationalWrite::CreateTable(
            compile_create_table(create)?,
        )]),
        SqlStatement::CreateIndex(create) => Ok(vec![compile_create_index(create)?]),
        SqlStatement::AlterTableAddColumn(alter) => compile_add_column(alter),
        _ => Err(SkeinError::Semantic(
            "content-store schema corpus contains a non-schema statement".to_string(),
        )),
    }
}

fn compile_create_table(create: CreateTableStatement) -> Result<RelationalTableSchema> {
    reject_non_public_schema(create.table.schema.as_deref())?;
    if !matches!(create.storage, SqlTableStorage::RowPage) {
        return Err(SkeinError::Semantic(
            "RowPage compiler does not accept a strict append table".to_string(),
        ));
    }
    if create.if_not_exists {
        return Err(SkeinError::Semantic(
            "content-store schema must not hide drift with IF NOT EXISTS".to_string(),
        ));
    }
    let mut primary_key = Vec::new();
    let mut unique_constraints = Vec::new();
    let mut foreign_keys = Vec::new();
    for column in &create.columns {
        if column.primary_key {
            if !primary_key.is_empty() {
                return Err(SkeinError::Semantic(
                    "table declares more than one primary key".to_string(),
                ));
            }
            primary_key.push(column.name.clone());
        }
        if column.unique {
            unique_constraints.push(vec![column.name.clone()]);
        }
        if let Some(reference) = &column.references {
            foreign_keys.push(RelationalForeignKeySchema {
                columns: vec![column.name.clone()],
                referenced_table: reference.table.name.clone(),
                referenced_columns: reference.columns.clone(),
                on_delete: compile_delete_referential_action(reference.on_delete)?,
                on_update: compile_update_referential_action(reference.on_update)?,
            });
        }
    }
    for constraint in create.constraints {
        match constraint {
            SqlTableConstraint::PrimaryKey(columns) => {
                if !primary_key.is_empty() {
                    return Err(SkeinError::Semantic(
                        "table declares more than one primary key".to_string(),
                    ));
                }
                primary_key = columns;
            }
            SqlTableConstraint::Unique(columns) => unique_constraints.push(columns),
            SqlTableConstraint::ForeignKey { columns, reference } => {
                foreign_keys.push(RelationalForeignKeySchema {
                    columns,
                    referenced_table: reference.table.name,
                    referenced_columns: reference.columns,
                    on_delete: compile_delete_referential_action(reference.on_delete)?,
                    on_update: compile_update_referential_action(reference.on_update)?,
                });
            }
        }
    }
    if primary_key.is_empty() {
        return Err(SkeinError::Semantic(format!(
            "relational table {} must declare a primary key",
            create.table.name
        )));
    }
    let column_names = create
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut primary_key_columns = std::collections::BTreeSet::new();
    for column in &primary_key {
        if !column_names.contains(column.as_str()) || !primary_key_columns.insert(column.clone()) {
            return Err(SkeinError::Semantic(format!(
                "primary key references unknown or duplicate column {column}"
            )));
        }
    }
    let columns = create
        .columns
        .into_iter()
        .map(|column| {
            let is_primary_key = primary_key_columns.contains(&column.name);
            let mut column = compile_column(column)?;
            if is_primary_key {
                column.nullable = false;
            }
            Ok(column)
        })
        .collect::<Result<_>>()?;
    Ok(RelationalTableSchema {
        name: create.table.name,
        columns,
        primary_key,
        unique_constraints,
        foreign_keys,
        indexes: Vec::new(),
    })
}

fn materialize_column_default(column: &RelationalColumnSchema) -> Result<RelationalValue> {
    Ok(match &column.default {
        None => RelationalValue::Null,
        Some(RelationalColumnDefault::Literal(value)) => value.clone(),
        Some(RelationalColumnDefault::UuidV7) => {
            RelationalValue::Uuid(skein_core::generate_uuidv7()?)
        }
    })
}

fn bind_relational_value_as(
    value: SqlValue,
    parameters: &[Value],
    scalar_type: RelationalScalarType,
) -> Result<RelationalValue> {
    bind_relational_value(value, parameters)
        .and_then(|value| coerce_relational_value(value, scalar_type))
}

fn compile_delete_referential_action(
    action: SqlReferentialAction,
) -> Result<RelationalReferentialAction> {
    match action {
        SqlReferentialAction::NoAction => Ok(RelationalReferentialAction::NoAction),
        SqlReferentialAction::Restrict => Ok(RelationalReferentialAction::Restrict),
        SqlReferentialAction::Cascade => Ok(RelationalReferentialAction::Cascade),
        SqlReferentialAction::SetNull => Err(SkeinError::Semantic(
            "ON DELETE SET NULL is not supported".to_string(),
        )),
    }
}

fn compile_update_referential_action(
    action: SqlReferentialAction,
) -> Result<RelationalReferentialAction> {
    match action {
        SqlReferentialAction::NoAction => Ok(RelationalReferentialAction::NoAction),
        SqlReferentialAction::Restrict => Ok(RelationalReferentialAction::Restrict),
        SqlReferentialAction::Cascade | SqlReferentialAction::SetNull => Err(SkeinError::Semantic(
            "ON UPDATE CASCADE and SET NULL are not supported".to_string(),
        )),
    }
}

fn compile_create_index(create: CreateIndexStatement) -> Result<RelationalWrite> {
    reject_non_public_schema(create.table.schema.as_deref())?;
    if create.if_not_exists {
        return Err(SkeinError::Semantic(
            "content-store indexes must not hide drift with IF NOT EXISTS".to_string(),
        ));
    }
    Ok(RelationalWrite::CreateIndex {
        table: create.table.name,
        index: RelationalIndexSchema {
            name: create.name,
            columns: create
                .columns
                .into_iter()
                .map(|column| column.column.name)
                .collect(),
            unique: create.unique,
        },
    })
}

fn compile_add_column(alter: AlterTableAddColumnStatement) -> Result<Vec<RelationalWrite>> {
    reject_non_public_schema(alter.table.schema.as_deref())?;
    if alter.if_not_exists {
        return Err(SkeinError::Semantic(
            "system schema migrations must not hide ADD COLUMN drift with IF NOT EXISTS"
                .to_string(),
        ));
    }
    if alter.column.primary_key || alter.column.unique || alter.column.references.is_some() {
        return Err(SkeinError::Semantic(
            "ALTER TABLE ADD COLUMN does not support inline key, unique, or foreign-key constraints"
                .to_string(),
        ));
    }
    Ok(vec![RelationalWrite::AddColumn {
        table: alter.table.name,
        column: compile_column(alter.column)?,
    }])
}

#[cfg(test)]
mod tests;
