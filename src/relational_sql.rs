use crate::error::{Result, SkeinError};
use crate::sql::{
    AlterTableAddColumnStatement, CreateIndexStatement, CreateTableStatement, SqlArithmeticOperand,
    SqlAssignmentValue, SqlComparisonOp, SqlConflictAction, SqlPredicate, SqlReferentialAction,
    SqlStatement, SqlTableConstraint, SqlTableStorage, SqlValue,
};
use crate::value::Value;
pub(crate) use skein_relational::{
    bind_relational_value, compile_append_explain_sql, compile_append_select_sql,
    compile_append_statement_sql, compile_column, format_append_explain, project_append_rows,
    reject_non_public_schema,
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

mod index_access;
mod query;
mod row_access;

pub use skein_optimizer::{
    RelationalJoinPlanningAttempt, RelationalJoinPlanningBudget, RelationalJoinPlanningCost,
    RelationalJoinPlanningFallbackClass, RelationalJoinPlanningOutcome,
    RelationalJoinPlanningReason, RelationalJoinPlanningStatus, RelationalJoinPlanningStrategy,
};
pub use skein_optimizer::{
    RelationalOperatorCardinalityProfile, RelationalOperatorId, RelationalOperatorKind,
};
pub use skein_sql::RelationalSqlStageTimings;

pub(crate) use index_access::RelationalIndexReadMode;
#[cfg(test)]
pub(crate) use query::execute_relational_query_sql_with_runtime;
pub(crate) use query::{
    execute_prepared_relational_query_with_resources, RelationalQueryLimits, RelationalQueryOutput,
    RelationalQueryReadModes, RelationalQueryResourceContext,
};
pub(crate) use row_access::RelationalRowReadMode;
pub(crate) use skein_sql::{PreparedRelationalSql, RelationalPlanTemplateCache};

pub(crate) fn compile_relational_statement_sql(
    sql: &str,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<RelationalTransaction> {
    compile_relational_statement_sql_with_result(sql, parameters, state)
        .map(|compiled| compiled.transaction)
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledRelationalStatement {
    pub transaction: RelationalTransaction,
    pub returning: Option<RelationalReturningProjection>,
}

#[derive(Debug, Clone)]
pub(crate) struct RelationalReturningProjection {
    pub table: String,
    pub columns: Vec<String>,
}

pub(crate) fn compile_relational_statement_sql_with_result(
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

pub(crate) fn statement_writes_system_schema_registry(statement: &SqlStatement) -> bool {
    const REGISTRY_TABLE: &str = "skein_schema_migrations";

    let table = match statement {
        SqlStatement::Insert(statement) => Some(&statement.table),
        SqlStatement::Update(statement) => Some(&statement.table),
        SqlStatement::Delete(statement) => Some(&statement.table),
        SqlStatement::CreateTable(statement) => Some(&statement.table),
        SqlStatement::CreateIndex(statement) => Some(&statement.table),
        SqlStatement::AlterTableAddColumn(statement) => Some(&statement.table),
        SqlStatement::Select(_) | SqlStatement::Explain(_) => None,
    };
    table.is_some_and(|table| {
        table.name == REGISTRY_TABLE
            && table
                .schema
                .as_deref()
                .is_none_or(|schema| schema == "public")
    })
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
                                    crate::sql::SqlArithmeticOperator::Add => {
                                        RelationalBigIntArithmeticOperator::Add
                                    }
                                    crate::sql::SqlArithmeticOperator::Subtract => {
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
    Ok(match predicate {
        SqlPredicate::And(left, right) => {
            RelationalPredicate::And(Box::new(compile(*left)?), Box::new(compile(*right)?))
        }
        SqlPredicate::Or(left, right) => {
            RelationalPredicate::Or(Box::new(compile(*left)?), Box::new(compile(*right)?))
        }
        SqlPredicate::Not(predicate) => RelationalPredicate::Not(Box::new(compile(*predicate)?)),
        SqlPredicate::Compare { left, op, right } => {
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
        SqlPredicate::CompareColumns { .. } => {
            return Err(SkeinError::Semantic(
                "single-table mutation predicates do not support column-to-column comparison"
                    .to_string(),
            ));
        }
        SqlPredicate::Like { .. } => {
            return Err(SkeinError::Semantic(
                "single-table mutation predicates do not support LIKE or ILIKE".to_string(),
            ));
        }
        SqlPredicate::InList {
            left,
            values,
            negated,
        } => {
            validate_mutation_column(&left, schema, alias, table)?;
            let scalar_type = schema.columns[schema
                .column_position(&left.name)
                .expect("mutation column was validated")]
            .scalar_type;
            let mut predicates = values
                .into_iter()
                .map(|value| {
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
        SqlPredicate::IsNull { column, negated } => {
            validate_mutation_column(&column, schema, alias, table)?;
            RelationalPredicate::IsNull {
                column: column.name,
                negated,
            }
        }
    })
}

fn validate_mutation_column(
    column: &crate::sql::SqlColumnRef,
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

fn coerce_relational_value(
    value: RelationalValue,
    scalar_type: RelationalScalarType,
) -> Result<RelationalValue> {
    match (scalar_type, value) {
        (RelationalScalarType::Uuid, RelationalValue::Text(value)) => {
            skein_core::Uuid::parse_str(&value)
                .map(RelationalValue::Uuid)
                .map_err(|_| SkeinError::Semantic(format!("invalid UUID value {value:?}")))
        }
        (RelationalScalarType::Uuid, RelationalValue::Uuid(value)) => {
            Ok(RelationalValue::Uuid(value))
        }
        (_, value) => Ok(value),
    }
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
mod tests {
    use super::*;
    use crate::{Database, DatabaseConfig};
    use skein_storage::{DurabilityPolicy, RelationalStore};
    use std::collections::BTreeMap;

    #[test]
    fn relational_uuid_scalar_preserves_typed_schema_keys_and_queries() {
        let account_id = skein_core::Uuid::parse_str("018f4e6a-7c1b-7cc8-8f4d-1234567890ab")
            .expect("parse account UUID");
        let account_external_id =
            skein_core::Uuid::parse_str("018f4e6a-7c1a-79d2-8f4d-1234567890ab")
                .expect("parse account external UUID");
        let duplicate_account_id =
            skein_core::Uuid::parse_str("018f4e6a-7c1b-79d2-8f4d-1234567890ab")
                .expect("parse duplicate account UUID");
        let first_session = skein_core::Uuid::parse_str("018f4e6a-7c1c-7b45-8f4d-1234567890ab")
            .expect("parse first session UUID");
        let second_session = skein_core::Uuid::parse_str("018f4e6a-7c1d-7b45-8f4d-1234567890ab")
            .expect("parse second session UUID");
        let mut database = Database::new();
        database
            .query_sql(
                "CREATE TABLE accounts (\
                    id UUID PRIMARY KEY, \
                    external_id UUID NOT NULL UNIQUE, \
                    name TEXT NOT NULL UNIQUE\
                )",
            )
            .expect("create UUID parent table");
        database
            .query_sql(
                "CREATE TABLE sessions (\
                    id UUID PRIMARY KEY, \
                    account_id UUID NOT NULL REFERENCES accounts(id), \
                    label TEXT NOT NULL\
                )",
            )
            .expect("create UUID child table");
        database
            .query_sql("CREATE INDEX sessions_account_id_idx ON sessions (account_id, id)")
            .expect("create UUID secondary index");
        database
            .query_sql(&format!(
                "INSERT INTO accounts (id, external_id, name) \
                 VALUES ('{account_id}', '{account_external_id}', 'primary')"
            ))
            .expect("insert UUID parent");
        database
            .query_sql(&format!(
                "INSERT INTO sessions (id, account_id, label) VALUES \
                 ('{second_session}', '{account_id}', 'second'), \
                 ('{first_session}', '{account_id}', 'first')"
            ))
            .expect("insert UUID children");

        let point = database
            .query_sql(&format!(
                "SELECT id FROM accounts WHERE id = '{account_id}'"
            ))
            .expect("read UUID primary key");
        assert_eq!(point.rows[0]["id"], Value::Uuid(account_id));
        assert_eq!(point.rows[0]["id"].to_string(), account_id.to_string());

        let unique = database
            .query_sql(&format!(
                "SELECT id FROM accounts WHERE external_id = '{account_external_id}'"
            ))
            .expect("read UUID unique constraint");
        assert_eq!(unique.rows[0]["id"], Value::Uuid(account_id));
        database
            .query_sql(&format!(
                "INSERT INTO accounts (id, external_id, name) \
                 VALUES ('{duplicate_account_id}', '{account_external_id}', 'duplicate')"
            ))
            .expect_err("duplicate UUID unique constraint must reject atomically");
        assert!(database
            .query_sql("SELECT id FROM accounts WHERE name = 'duplicate'")
            .expect("read after duplicate UUID unique constraint")
            .rows
            .is_empty());

        let parameterized = database
            .query_sql_with_params(
                "SELECT id FROM accounts WHERE id = $1",
                &[Value::String(account_id.to_string())],
            )
            .expect("bind UUID parameter as a typed scalar");
        assert_eq!(parameterized.rows[0]["id"], Value::Uuid(account_id));

        let joined = database
            .query_sql(&format!(
                "SELECT s.id FROM sessions AS s \
                 INNER JOIN accounts AS a ON a.id = s.account_id \
                 WHERE a.id = '{account_id}' \
                 ORDER BY s.id ASC LIMIT 2"
            ))
            .expect("join UUID foreign key");
        assert_eq!(
            joined
                .rows
                .iter()
                .map(|row| row["id"].clone())
                .collect::<Vec<_>>(),
            [Value::Uuid(first_session), Value::Uuid(second_session)]
        );

        let range = database
            .query_sql(&format!(
                "SELECT id FROM sessions WHERE account_id = '{account_id}' \
                 AND id > '{first_session}' ORDER BY id ASC LIMIT 1"
            ))
            .expect("range read UUID index");
        assert_eq!(range.rows[0]["id"], Value::Uuid(second_session));

        let metadata = database
            .query_sql(
                "SELECT data_type, udt_name FROM information_schema.columns \
                 WHERE table_name = 'sessions' AND column_name = 'id'",
            )
            .expect("read UUID information schema");
        assert_eq!(
            metadata.rows[0]["data_type"],
            Value::String("uuid".to_string())
        );
        assert_eq!(
            metadata.rows[0]["udt_name"],
            Value::String("uuid".to_string())
        );

        let invalid = database
            .query_sql("INSERT INTO accounts (id, name) VALUES ('invalid', 'bad')")
            .expect_err("invalid UUID must fail before mutation");
        assert!(invalid.to_string().contains("invalid UUID"));
        assert_eq!(
            database
                .query_sql("SELECT id FROM accounts WHERE name = 'bad'")
                .expect("read after invalid UUID")
                .rows
                .len(),
            0
        );
    }

    #[test]
    fn relational_uuid_scalar_survives_wal_and_checkpoint_reopen() {
        let value = skein_core::Uuid::parse_str("018f4e6a-7c1e-7d7a-8f4d-1234567890ab")
            .expect("parse durable UUID");
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-relational-uuid-{nonce}-{}",
            std::process::id()
        ));
        {
            let mut database = Database::open_with_durability(&path, DurabilityPolicy::default())
                .expect("open durable UUID database");
            database
                .query_sql("CREATE TABLE durable_ids (id UUID PRIMARY KEY, label TEXT NOT NULL)")
                .expect("create durable UUID table");
            database
                .query_sql(&format!(
                    "INSERT INTO durable_ids (id, label) VALUES ('{value}', 'durable')"
                ))
                .expect("insert durable UUID");
        }
        {
            let mut reopened = Database::open_with_durability(&path, DurabilityPolicy::default())
                .expect("reopen UUID WAL");
            let recovered = reopened
                .query_sql("SELECT id FROM durable_ids WHERE label = 'durable'")
                .expect("read UUID recovered from WAL");
            assert_eq!(recovered.rows[0]["id"], Value::Uuid(value));
            reopened.checkpoint().expect("checkpoint durable UUID");
        }
        {
            let mut reopened = Database::open_with_durability(&path, DurabilityPolicy::default())
                .expect("reopen UUID checkpoint");
            let recovered = reopened
                .query_sql(&format!("SELECT id FROM durable_ids WHERE id = '{value}'"))
                .expect("read UUID recovered from checkpoint");
            assert_eq!(recovered.rows[0]["id"], Value::Uuid(value));
        }
        std::fs::remove_dir_all(path).expect("remove durable UUID test directory");
    }

    #[test]
    fn relational_uuidv7_function_and_default_materialize_once_per_row() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE probe (id BIGINT PRIMARY KEY)")
            .expect("create uuidv7 probe");
        database
            .query_sql("INSERT INTO probe (id) VALUES (1)")
            .expect("insert uuidv7 probe row");
        let projected = database
            .query_sql("SELECT uuidv7() AS generated_id FROM probe LIMIT 1")
            .expect("project uuidv7");
        let Value::Uuid(projected_id) = projected.rows[0]["generated_id"] else {
            panic!("uuidv7 projection must return a UUID");
        };
        assert_uuidv7(projected_id);

        database
            .query_sql(
                "CREATE TABLE feeds (\
                    id UUID PRIMARY KEY DEFAULT uuidv7(), \
                    url TEXT NOT NULL UNIQUE\
                )",
            )
            .expect("create uuidv7 default table");
        let defaults = database
            .query_sql(
                "SELECT column_default FROM information_schema.columns \
                 WHERE table_name = 'feeds' AND column_name = 'id'",
            )
            .expect("read uuidv7 default metadata");
        assert_eq!(
            defaults.rows[0]["column_default"],
            Value::String("uuidv7()".to_string())
        );

        let inserted = database
            .query_sql(
                "INSERT INTO feeds (url) VALUES ('https://one.example'), ('https://two.example') \
                 RETURNING id",
            )
            .expect("insert UUID default rows");
        let ids = inserted
            .rows
            .iter()
            .map(|row| match row["id"] {
                Value::Uuid(value) => value,
                _ => panic!("uuidv7 default must return a UUID"),
            })
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
        assert!(ids[0] < ids[1]);
        ids.iter().copied().for_each(assert_uuidv7);

        let explicit = skein_core::generate_uuidv7().expect("generate explicit UUIDv7");
        let explicit_insert = database
            .query_sql(&format!(
                "INSERT INTO feeds (id, url) VALUES ('{explicit}', 'https://explicit.example') \
                 RETURNING id"
            ))
            .expect("insert explicit UUID");
        assert_eq!(explicit_insert.rows[0]["id"], Value::Uuid(explicit));

        let conflict = database
            .query_sql(
                "INSERT INTO feeds (url) VALUES ('https://one.example') \
                 ON CONFLICT (url) DO NOTHING RETURNING id",
            )
            .expect("ignore UUID default conflict");
        assert!(conflict.rows.is_empty());

        let mut transaction = database.begin_transaction();
        let staged = transaction
            .query_sql_with_result(
                "INSERT INTO feeds (url) VALUES ('https://transaction.example') RETURNING id",
            )
            .expect("stage UUID default insert");
        let staged_id = match staged
            .mutation
            .expect("staged UUID mutation")
            .rows
            .first()
            .expect("one staged UUID row")["id"]
        {
            Value::Uuid(value) => value,
            _ => panic!("staged UUID default must be typed"),
        };
        let committed = transaction
            .commit_with_result()
            .expect("commit UUID default insert");
        assert_eq!(committed.output.rows[0]["id"], Value::Uuid(staged_id));

        let mut rollback = database.begin_transaction();
        let staged = rollback
            .query_sql_with_result(
                "INSERT INTO feeds (url) VALUES ('https://rollback.example') RETURNING id",
            )
            .expect("stage rollback UUID default insert");
        assert_eq!(
            staged.mutation.expect("rollback mutation").rows.len(),
            1,
            "uuidv7 is materialized while the statement is staged"
        );
        rollback.rollback();
        assert!(database
            .query_sql("SELECT id FROM feeds WHERE url = 'https://rollback.example'")
            .expect("read rolled-back UUID default row")
            .rows
            .is_empty());

        let error = database
            .query_sql("CREATE TABLE invalid_uuid_default (id TEXT PRIMARY KEY DEFAULT uuidv7())")
            .expect_err("uuidv7 default must require UUID column");
        assert!(error
            .to_string()
            .contains("uuidv7() default but is not UUID"));
    }

    #[test]
    fn relational_uuidv7_defaults_survive_wal_and_checkpoint_without_regeneration() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-relational-uuidv7-{nonce}-{}",
            std::process::id()
        ));
        let generated;
        {
            let mut database = Database::open_with_durability(&path, DurabilityPolicy::default())
                .expect("open durable uuidv7 database");
            database
                .query_sql(
                    "CREATE TABLE durable_feeds (\
                        id UUID PRIMARY KEY DEFAULT uuidv7(), \
                        url TEXT NOT NULL UNIQUE\
                    )",
                )
                .expect("create durable uuidv7 table");
            let inserted = database
                .query_sql(
                    "INSERT INTO durable_feeds (url) VALUES ('https://durable.example') \
                     RETURNING id",
                )
                .expect("insert durable uuidv7 row");
            generated = match inserted.rows[0]["id"] {
                Value::Uuid(value) => value,
                _ => panic!("durable uuidv7 default must return a UUID"),
            };
            database
                .checkpoint()
                .expect("checkpoint durable uuidv7 row");
        }
        let mut reopened = Database::open_with_durability(&path, DurabilityPolicy::default())
            .expect("reopen durable uuidv7 database");
        let recovered = reopened
            .query_sql("SELECT id FROM durable_feeds WHERE url = 'https://durable.example'")
            .expect("read durable uuidv7 row");
        assert_eq!(recovered.rows[0]["id"], Value::Uuid(generated));
        assert_uuidv7(generated);
        drop(reopened);
        std::fs::remove_dir_all(path).expect("remove durable uuidv7 test directory");
    }

    #[test]
    fn relational_on_delete_cascade_is_transitive_and_atomic() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE parents (id BIGINT PRIMARY KEY)")
            .expect("create cascade parent table");
        database
            .query_sql(
                "CREATE TABLE children (\
                    id BIGINT PRIMARY KEY, \
                    parent_id BIGINT NOT NULL REFERENCES parents(id) ON DELETE CASCADE\
                )",
            )
            .expect("create cascade child table");
        database
            .query_sql(
                "CREATE TABLE grandchildren (\
                    id BIGINT PRIMARY KEY, \
                    child_id BIGINT NOT NULL REFERENCES children(id) ON DELETE CASCADE\
                )",
            )
            .expect("create cascade grandchild table");
        database
            .query_sql(
                "CREATE TABLE restrictors (\
                    id BIGINT PRIMARY KEY, \
                    parent_id BIGINT NOT NULL REFERENCES parents(id) ON DELETE RESTRICT\
                )",
            )
            .expect("create restrict child table");

        database
            .query_sql("INSERT INTO parents (id) VALUES (1)")
            .expect("insert cascade parent");
        database
            .query_sql("INSERT INTO children (id, parent_id) VALUES (10, 1), (11, 1)")
            .expect("insert cascade children");
        database
            .query_sql("INSERT INTO grandchildren (id, child_id) VALUES (100, 10), (101, 11)")
            .expect("insert cascade grandchildren");
        database
            .query_sql("DELETE FROM parents WHERE id = 1")
            .expect("delete cascade parent");
        for table in ["parents", "children", "grandchildren"] {
            assert!(database
                .query_sql(&format!("SELECT id FROM {table}"))
                .expect("read cascade result")
                .rows
                .is_empty());
        }

        database
            .query_sql("INSERT INTO parents (id) VALUES (2)")
            .expect("insert restricted parent");
        database
            .query_sql("INSERT INTO children (id, parent_id) VALUES (20, 2)")
            .expect("insert restricted cascade child");
        database
            .query_sql("INSERT INTO grandchildren (id, child_id) VALUES (200, 20)")
            .expect("insert restricted cascade grandchild");
        database
            .query_sql("INSERT INTO restrictors (id, parent_id) VALUES (300, 2)")
            .expect("insert restrict child");
        let rejected = database
            .query_sql("DELETE FROM parents WHERE id = 2")
            .expect_err("restrict child must reject the complete cascade");
        assert!(rejected.to_string().contains("prevents removing"));
        for (table, id) in [
            ("parents", 2),
            ("children", 20),
            ("grandchildren", 200),
            ("restrictors", 300),
        ] {
            assert_eq!(
                database
                    .query_sql(&format!("SELECT id FROM {table} WHERE id = {id}"))
                    .expect("read rejected cascade result")
                    .rows
                    .len(),
                1
            );
        }

        database
            .query_sql("INSERT INTO parents (id) VALUES (3)")
            .expect("insert rollback parent");
        database
            .query_sql("INSERT INTO children (id, parent_id) VALUES (30, 3)")
            .expect("insert rollback child");
        let mut rollback = database.begin_transaction();
        rollback
            .query_sql("DELETE FROM parents WHERE id = 3")
            .expect("stage delete cascade");
        rollback.rollback();
        assert_eq!(
            database
                .query_sql("SELECT id FROM children WHERE id = 30")
                .expect("read rolled back cascade child")
                .rows
                .len(),
            1
        );

        let unsupported = database
            .query_sql(
                "CREATE TABLE unsupported_update (\
                    id BIGINT PRIMARY KEY, \
                    parent_id BIGINT NOT NULL REFERENCES parents(id) ON UPDATE CASCADE\
                )",
            )
            .expect_err("ON UPDATE CASCADE remains unsupported");
        assert!(unsupported.to_string().contains("ON UPDATE CASCADE"));
    }

    #[test]
    fn relational_on_delete_cascade_survives_wal_and_checkpoint_reopen() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-relational-cascade-{nonce}-{}",
            std::process::id()
        ));
        {
            let mut database = Database::open_with_durability(&path, DurabilityPolicy::default())
                .expect("open durable cascade database");
            database
                .query_sql("CREATE TABLE durable_parents (id BIGINT PRIMARY KEY)")
                .expect("create durable cascade parent table");
            database
                .query_sql(
                    "CREATE TABLE durable_children (\
                        id BIGINT PRIMARY KEY, \
                        parent_id BIGINT NOT NULL REFERENCES durable_parents(id) ON DELETE CASCADE\
                    )",
                )
                .expect("create durable cascade child table");
            database
                .query_sql("INSERT INTO durable_parents (id) VALUES (1)")
                .expect("insert durable cascade parent");
            database
                .query_sql("INSERT INTO durable_children (id, parent_id) VALUES (10, 1)")
                .expect("insert durable cascade child");
            database
                .query_sql("DELETE FROM durable_parents WHERE id = 1")
                .expect("delete durable cascade parent");
        }
        {
            let mut reopened = Database::open_with_durability(&path, DurabilityPolicy::default())
                .expect("reopen durable cascade WAL");
            for table in ["durable_parents", "durable_children"] {
                assert!(reopened
                    .query_sql(&format!("SELECT id FROM {table}"))
                    .expect("read durable cascade WAL result")
                    .rows
                    .is_empty());
            }
            reopened
                .checkpoint()
                .expect("checkpoint durable cascade result");
        }
        {
            let mut reopened = Database::open_with_durability(&path, DurabilityPolicy::default())
                .expect("reopen durable cascade checkpoint");
            for table in ["durable_parents", "durable_children"] {
                assert!(reopened
                    .query_sql(&format!("SELECT id FROM {table}"))
                    .expect("read durable cascade checkpoint result")
                    .rows
                    .is_empty());
            }
        }
        std::fs::remove_dir_all(path).expect("remove durable cascade test directory");
    }

    #[test]
    fn relational_bigint_update_arithmetic_is_checked_and_transactional() {
        let mut database = Database::new();
        database
            .query_sql(
                "CREATE TABLE counters (\
                    id BIGINT PRIMARY KEY, \
                    count BIGINT NOT NULL, \
                    label TEXT NOT NULL, \
                    nullable_count BIGINT\
                )",
            )
            .expect("create counter table");
        for (id, count, label) in [
            (1, 5, "one"),
            (2, -2, "two"),
            (3, i64::MAX, "maximum"),
            (4, i64::MIN, "minimum"),
            (5, 0, "nullable"),
        ] {
            database
                .query_sql_with_params(
                    "INSERT INTO counters (id, count, label) VALUES ($1, $2, $3)",
                    &[
                        Value::Int(id),
                        Value::Int(count),
                        Value::String(label.to_string()),
                    ],
                )
                .expect("insert counter row");
        }

        database
            .query_sql("UPDATE counters SET count = count + 3 WHERE id = 1")
            .expect("increment literal counter");
        database
            .query_sql("UPDATE counters SET count = 2 + count WHERE id = 1")
            .expect("reverse increment literal counter");
        database
            .query_sql_with_params(
                "UPDATE counters SET count = count - $1 WHERE id = $2",
                &[Value::Int(-2), Value::Int(1)],
            )
            .expect("parameterized counter subtraction");
        let addition_summary = database
            .query_sql(
                "SELECT digest FROM system.statement_summary \
                 WHERE query_text = 'UPDATE counters SET count = count + 3 WHERE id = 1'",
            )
            .expect("read addition statement summary");
        let subtraction_summary = database
            .query_sql(
                "SELECT digest FROM system.statement_summary \
                 WHERE query_text = 'UPDATE counters SET count = count - $1 WHERE id = $2'",
            )
            .expect("read subtraction statement summary");
        assert_eq!(addition_summary.rows.len(), 1);
        assert_eq!(subtraction_summary.rows.len(), 1);
        assert_ne!(
            addition_summary.rows[0]["digest"], subtraction_summary.rows[0]["digest"],
            "statement summaries must retain the arithmetic expression shape"
        );
        database
            .query_sql("UPDATE counters SET count = count + 1 WHERE id <= 2")
            .expect("update multiple counter rows");
        assert_eq!(
            database
                .query_sql("SELECT count FROM counters WHERE id = 1")
                .expect("read incremented counter")
                .rows[0]["count"],
            Value::Int(13)
        );
        assert_eq!(
            database
                .query_sql("SELECT count FROM counters WHERE id = 2")
                .expect("read second incremented counter")
                .rows[0]["count"],
            Value::Int(-1)
        );

        let mut transaction = database.begin_transaction();
        transaction
            .query_sql("UPDATE counters SET count = count + 1 WHERE id = 1")
            .expect("stage first transaction-local increment");
        transaction
            .query_sql("UPDATE counters SET count = count + 1 WHERE id = 1")
            .expect("stage second transaction-local increment");
        assert_eq!(
            transaction
                .query_sql("SELECT count FROM counters WHERE id = 1")
                .expect("read transaction-local increment")
                .rows[0]["count"],
            Value::Int(15)
        );
        transaction
            .commit()
            .expect("commit transaction-local increments");

        let overflow = database
            .query_sql("UPDATE counters SET count = count + 1 WHERE id >= 3")
            .expect_err("BIGINT overflow must reject the complete UPDATE");
        assert!(overflow.to_string().contains("arithmetic overflow"));
        for (id, expected) in [(3, i64::MAX), (4, i64::MIN)] {
            assert_eq!(
                database
                    .query_sql(&format!("SELECT count FROM counters WHERE id = {id}"))
                    .expect("read after rejected arithmetic overflow")
                    .rows[0]["count"],
                Value::Int(expected)
            );
        }

        for (sql, parameters, expected) in [
            (
                "UPDATE counters SET label = label + 1 WHERE id = 1",
                Vec::new(),
                "target",
            ),
            (
                "UPDATE counters SET count = label + 1 WHERE id = 1",
                Vec::new(),
                "source",
            ),
            (
                "UPDATE counters SET count = count + $1 WHERE id = 1",
                vec![Value::String("one".to_string())],
                "BIGINT",
            ),
            (
                "UPDATE counters SET count = count + $1 WHERE id = 1",
                vec![Value::Null],
                "non-null",
            ),
        ] {
            let error = database
                .query_sql_with_params(sql, &parameters)
                .expect_err("invalid arithmetic assignment must fail before publication");
            assert!(error.to_string().contains(expected));
        }

        let null_operand = database
            .query_sql("UPDATE counters SET nullable_count = nullable_count + 1 WHERE id = 5")
            .expect_err("NULL arithmetic operand must fail");
        assert!(null_operand.to_string().contains("NULL operands"));
        database
            .query_sql("UPDATE counters SET nullable_count = count + 1 WHERE id = 5")
            .expect("non-null source can update a nullable BIGINT target");
    }

    #[test]
    fn relational_bigint_update_arithmetic_survives_wal_and_checkpoint_reopen() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-relational-arithmetic-{nonce}-{}",
            std::process::id()
        ));
        {
            let mut database = Database::open_with_durability(&path, DurabilityPolicy::default())
                .expect("open durable arithmetic database");
            database
                .query_sql(
                    "CREATE TABLE durable_counters (\
                        id BIGINT PRIMARY KEY, \
                        count BIGINT NOT NULL\
                    )",
                )
                .expect("create durable counter table");
            database
                .query_sql("INSERT INTO durable_counters (id, count) VALUES (1, 7)")
                .expect("insert durable counter");
            database
                .query_sql("UPDATE durable_counters SET count = count + 1 WHERE id = 1")
                .expect("update durable counter");
        }
        {
            let mut reopened = Database::open_with_durability(&path, DurabilityPolicy::default())
                .expect("reopen arithmetic WAL");
            assert_eq!(
                reopened
                    .query_sql("SELECT count FROM durable_counters WHERE id = 1")
                    .expect("read durable arithmetic WAL result")
                    .rows[0]["count"],
                Value::Int(8)
            );
            reopened
                .checkpoint()
                .expect("checkpoint durable arithmetic result");
        }
        {
            let mut reopened = Database::open_with_durability(&path, DurabilityPolicy::default())
                .expect("reopen arithmetic checkpoint");
            assert_eq!(
                reopened
                    .query_sql("SELECT count FROM durable_counters WHERE id = 1")
                    .expect("read durable arithmetic checkpoint result")
                    .rows[0]["count"],
                Value::Int(8)
            );
        }
        std::fs::remove_dir_all(path).expect("remove durable arithmetic test directory");
    }

    #[test]
    fn relational_aggregate_filters_preserve_grouping_distinct_and_null_semantics() {
        let mut database = Database::new();
        database
            .query_sql(
                "CREATE TABLE entries (\
                    id TEXT PRIMARY KEY, \
                    feed_id TEXT NOT NULL, \
                    is_read BOOLEAN, \
                    is_saved BOOLEAN, \
                    amount BIGINT\
                )",
            )
            .expect("create aggregate filter table");
        for (id, feed_id, is_read, is_saved, amount) in [
            (
                "entry-1",
                "feed-1",
                Value::Bool(false),
                Value::Bool(true),
                Value::Int(4),
            ),
            (
                "entry-2",
                "feed-1",
                Value::Bool(true),
                Value::Bool(false),
                Value::Int(4),
            ),
            (
                "entry-3",
                "feed-2",
                Value::Bool(false),
                Value::Bool(true),
                Value::Int(8),
            ),
            (
                "entry-4",
                "feed-2",
                Value::Null,
                Value::Bool(false),
                Value::Null,
            ),
            (
                "entry-5",
                "feed-2",
                Value::Bool(false),
                Value::Bool(true),
                Value::Int(8),
            ),
        ] {
            database
                .query_sql_with_params(
                    "INSERT INTO entries (id, feed_id, is_read, is_saved, amount) \
                     VALUES ($1, $2, $3, $4, $5)",
                    &[
                        Value::String(id.to_string()),
                        Value::String(feed_id.to_string()),
                        is_read,
                        is_saved,
                        amount,
                    ],
                )
                .expect("insert aggregate filter row");
        }

        let totals = database
            .query_sql(
                "SELECT \
                    COUNT(*) AS entry_count, \
                    COUNT(*) FILTER (WHERE is_read = FALSE) AS unread_count, \
                    COUNT(amount) FILTER (WHERE is_read = FALSE) AS unread_amount_count, \
                    SUM(amount) FILTER (WHERE is_read = FALSE) AS unread_amount_sum, \
                    SUM(amount) FILTER (WHERE is_read IS NULL) AS null_read_amount_sum \
                 FROM entries",
            )
            .expect("aggregate filters on one snapshot");
        assert_eq!(
            totals.rows,
            vec![BTreeMap::from([
                ("entry_count".to_string(), Value::Int(5)),
                ("unread_count".to_string(), Value::Int(3)),
                ("unread_amount_count".to_string(), Value::Int(3)),
                ("unread_amount_sum".to_string(), Value::Int(20)),
                ("null_read_amount_sum".to_string(), Value::Null),
            ])]
        );

        let distinct = database
            .query_sql(
                "SELECT COUNT(DISTINCT amount) FILTER (WHERE is_read = FALSE) AS amount_count \
                 FROM entries",
            )
            .expect("filtered distinct count");
        assert_eq!(distinct.rows[0]["amount_count"], Value::Int(2));

        let grouped = database
            .query_sql(
                "SELECT \
                    feed_id, \
                    COUNT(*) FILTER (WHERE is_read = FALSE) AS unread_count, \
                    SUM(amount) FILTER (WHERE is_read = FALSE) AS unread_amount_sum, \
                    COUNT(*) FILTER (WHERE is_read IS NULL) AS null_read_count \
                 FROM entries GROUP BY feed_id",
            )
            .expect("grouped aggregate filters");
        assert_eq!(
            grouped.rows,
            vec![
                BTreeMap::from([
                    ("feed_id".to_string(), Value::String("feed-1".to_string())),
                    ("unread_count".to_string(), Value::Int(1)),
                    ("unread_amount_sum".to_string(), Value::Int(4)),
                    ("null_read_count".to_string(), Value::Int(0)),
                ]),
                BTreeMap::from([
                    ("feed_id".to_string(), Value::String("feed-2".to_string())),
                    ("unread_count".to_string(), Value::Int(2)),
                    ("unread_amount_sum".to_string(), Value::Int(16)),
                    ("null_read_count".to_string(), Value::Int(1)),
                ]),
            ]
        );

        let parameterized = database
            .query_sql_with_params(
                "SELECT COUNT(*) FILTER (WHERE is_saved = $1) AS saved_count FROM entries",
                &[Value::Bool(true)],
            )
            .expect("parameterized aggregate filter");
        assert_eq!(parameterized.rows[0]["saved_count"], Value::Int(3));

        let empty = database
            .query_sql(
                "SELECT \
                    COUNT(*) FILTER (WHERE is_read = FALSE) AS unread_count, \
                    SUM(amount) FILTER (WHERE is_read = FALSE) AS unread_amount_sum \
                 FROM entries WHERE feed_id = 'missing'",
            )
            .expect("aggregate filter over empty selected input");
        assert_eq!(
            empty.rows,
            vec![BTreeMap::from([
                ("unread_count".to_string(), Value::Int(0)),
                ("unread_amount_sum".to_string(), Value::Null),
            ])]
        );

        let explain = database
            .query_sql(
                "EXPLAIN ANALYZE SELECT COUNT(*) FILTER (WHERE is_read = FALSE) \
                 AS unread_count FROM entries",
            )
            .expect("explain aggregate filter");
        assert!(
            relational_explain_operator_info(&explain, "RelationalAggregateExec")
                .contains("count(*) FILTER (is_read = false)")
        );

        let filtered_identity_sql =
            "SELECT COUNT(*) FILTER (WHERE is_read = FALSE) AS unread_count FROM entries";
        let unfiltered_identity_sql = "SELECT COUNT(*) AS entry_count FROM entries";
        database
            .query_sql(filtered_identity_sql)
            .expect("record filtered aggregate statement identity");
        database
            .query_sql(unfiltered_identity_sql)
            .expect("record unfiltered aggregate statement identity");
        let filtered_summary = database
            .query_sql(&format!(
                "SELECT digest FROM system.statement_summary WHERE query_text = '{filtered_identity_sql}'"
            ))
            .expect("read filtered aggregate statement summary");
        let unfiltered_summary = database
            .query_sql(&format!(
                "SELECT digest FROM system.statement_summary WHERE query_text = '{unfiltered_identity_sql}'"
            ))
            .expect("read unfiltered aggregate statement summary");
        assert_ne!(
            filtered_summary.rows[0]["digest"], unfiltered_summary.rows[0]["digest"],
            "statement identity must retain aggregate FILTER shape"
        );
    }

    fn assert_uuidv7(value: crate::Uuid) {
        let bytes = value.as_bytes();
        assert_eq!(bytes[6] >> 4, 7, "uuidv7 version bits");
        assert_eq!(bytes[8] & 0b1100_0000, 0b1000_0000, "RFC 9562 variant bits");
    }

    #[test]
    fn relational_keyset_pagination_uses_exclusive_forward_and_backward_index_seeks() {
        let mut database = Database::new();
        database
            .query_sql(
                "CREATE TABLE sessions (\
                    id TEXT PRIMARY KEY, \
                    org_id TEXT NOT NULL, \
                    last_seen_at BIGINT NOT NULL, \
                    payload TEXT NOT NULL\
                )",
            )
            .expect("create sessions table");
        database
            .query_sql(
                "CREATE INDEX sessions_org_last_seen_id_idx \
                 ON sessions (org_id, last_seen_at, id)",
            )
            .expect("create sessions keyset index");
        for (id, last_seen_at) in [
            ("session-a", 10),
            ("session-b", 20),
            ("session-c", 20),
            ("session-d", 30),
        ] {
            database
                .query_sql_with_params(
                    "INSERT INTO sessions (id, org_id, last_seen_at, payload) \
                     VALUES ($1, 'org-1', $2, $1)",
                    &[Value::String(id.to_string()), Value::Int(last_seen_at)],
                )
                .expect("insert session");
        }

        let ascending = database
            .query_sql_with_params(
                "SELECT id, last_seen_at FROM sessions \
                 WHERE org_id = $1 \
                   AND (last_seen_at > $2 OR (last_seen_at = $2 AND id > $3)) \
                 ORDER BY last_seen_at ASC, id ASC LIMIT $4",
                &[
                    Value::String("org-1".to_string()),
                    Value::Int(20),
                    Value::String("session-b".to_string()),
                    Value::Int(2),
                ],
            )
            .expect("read ascending keyset page");
        assert_eq!(
            ascending
                .rows
                .iter()
                .map(|row| row["id"].clone())
                .collect::<Vec<_>>(),
            [
                Value::String("session-c".to_string()),
                Value::String("session-d".to_string()),
            ]
        );
        let ascending_first = database
            .query_sql(
                "SELECT id FROM sessions WHERE org_id = 'org-1' \
                 ORDER BY last_seen_at ASC, id ASC LIMIT 2",
            )
            .expect("read first ascending keyset page");
        let walked_ascending = ascending_first
            .rows
            .iter()
            .chain(&ascending.rows)
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            walked_ascending,
            [
                Value::String("session-a".to_string()),
                Value::String("session-b".to_string()),
                Value::String("session-c".to_string()),
                Value::String("session-d".to_string()),
            ]
        );

        let descending = database
            .query_sql_with_params(
                "SELECT id, last_seen_at FROM sessions \
                 WHERE org_id = $1 \
                   AND (last_seen_at < $2 OR (last_seen_at = $2 AND id < $3)) \
                 ORDER BY last_seen_at DESC, id DESC LIMIT $4",
                &[
                    Value::String("org-1".to_string()),
                    Value::Int(20),
                    Value::String("session-c".to_string()),
                    Value::Int(2),
                ],
            )
            .expect("read descending keyset page");
        assert_eq!(
            descending
                .rows
                .iter()
                .map(|row| row["id"].clone())
                .collect::<Vec<_>>(),
            [
                Value::String("session-b".to_string()),
                Value::String("session-a".to_string()),
            ]
        );
        let descending_first = database
            .query_sql(
                "SELECT id FROM sessions WHERE org_id = 'org-1' \
                 ORDER BY last_seen_at DESC, id DESC LIMIT 2",
            )
            .expect("read first descending keyset page");
        let walked_descending = descending_first
            .rows
            .iter()
            .chain(&descending.rows)
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            walked_descending,
            [
                Value::String("session-d".to_string()),
                Value::String("session-c".to_string()),
                Value::String("session-b".to_string()),
                Value::String("session-a".to_string()),
            ]
        );

        let explained = database
            .query_sql_with_params(
                "EXPLAIN ANALYZE SELECT id FROM sessions \
                 WHERE org_id = $1 \
                   AND (last_seen_at < $2 OR (last_seen_at = $2 AND id < $3)) \
                 ORDER BY last_seen_at DESC, id DESC LIMIT $4",
                &[
                    Value::String("org-1".to_string()),
                    Value::Int(30),
                    Value::String("session-d".to_string()),
                    Value::Int(1),
                ],
            )
            .expect("explain descending keyset page");
        assert!(explained.rows.iter().all(|row| {
            !matches!(row.get("id"), Some(Value::String(id)) if id.contains("TopNExec") || id.contains("SelectionExec"))
        }));
        let info = relational_explain_operator_info(&explained, "IndexRangeScanExec");
        assert!(info.contains("order_prefix=2"));
        assert!(info.contains("exclusive_seek=true"));
        assert!(info.contains("direction=backward"));

        let mixed_direction = database
            .query_sql(
                "EXPLAIN SELECT id FROM sessions WHERE org_id = 'org-1' \
                 ORDER BY last_seen_at DESC, id ASC LIMIT 2",
            )
            .expect("explain mixed-direction fallback");
        assert!(mixed_direction.rows.iter().any(|row| {
            matches!(row.get("id"), Some(Value::String(id)) if id.contains("TopNExec"))
        }));

        database
            .query_sql(
                "CREATE TABLE nullable_sessions (\
                    id TEXT PRIMARY KEY, \
                    org_id TEXT NOT NULL, \
                    last_seen_at BIGINT\
                )",
            )
            .expect("create nullable sessions table");
        database
            .query_sql(
                "CREATE INDEX nullable_sessions_keyset_idx \
                 ON nullable_sessions (org_id, last_seen_at, id)",
            )
            .expect("create nullable sessions index");
        let nullable = database
            .query_sql(
                "EXPLAIN SELECT id FROM nullable_sessions \
                 WHERE org_id = 'org-1' \
                   AND (last_seen_at > 20 OR (last_seen_at = 20 AND id > 'session-c')) \
                 ORDER BY last_seen_at ASC NULLS FIRST, id ASC LIMIT 2",
            )
            .expect("explain nullable keyset fallback");
        assert!(nullable.rows.iter().any(|row| {
            matches!(row.get("id"), Some(Value::String(id)) if id.contains("TopNExec"))
        }));
    }

    #[test]
    fn relational_keyset_pagination_walks_every_boundary_without_gaps() {
        let mut database = Database::new();
        database
            .query_sql(
                "CREATE TABLE sessions (\
                    id TEXT PRIMARY KEY, \
                    org_id TEXT NOT NULL, \
                    last_seen_at BIGINT NOT NULL\
                )",
            )
            .expect("create sessions table");
        database
            .query_sql(
                "CREATE INDEX sessions_org_last_seen_id_idx \
                 ON sessions (org_id, last_seen_at, id)",
            )
            .expect("create sessions keyset index");
        database
            .query_sql(
                "INSERT INTO sessions (id, org_id, last_seen_at) VALUES \
                    ('session-a', 'org-1', 10), \
                    ('session-b', 'org-1', 20), \
                    ('session-c', 'org-1', 20), \
                    ('session-d', 'org-1', 30), \
                    ('session-e', 'org-1', 30), \
                    ('session-f', 'org-1', 30), \
                    ('session-g', 'org-1', 40)",
            )
            .expect("insert sessions with duplicate sort keys");
        let expected = [
            "session-a",
            "session-b",
            "session-c",
            "session-d",
            "session-e",
            "session-f",
            "session-g",
        ];

        let mut forward = Vec::new();
        let mut cursor: Option<(i64, String)> = None;
        loop {
            let page = match &cursor {
                None => database.query_sql(
                    "SELECT id, last_seen_at FROM sessions WHERE org_id = 'org-1' \
                     ORDER BY last_seen_at ASC, id ASC LIMIT 2",
                ),
                Some((last_seen_at, id)) => database.query_sql_with_params(
                    "SELECT id, last_seen_at FROM sessions \
                     WHERE org_id = 'org-1' \
                       AND (last_seen_at > $1 OR (last_seen_at = $1 AND id > $2)) \
                     ORDER BY last_seen_at ASC, id ASC LIMIT 2",
                    &[Value::Int(*last_seen_at), Value::String(id.clone())],
                ),
            }
            .expect("read forward keyset page");
            let Some(last) = page.rows.last() else {
                break;
            };
            let Value::Int(last_seen_at) = last["last_seen_at"] else {
                panic!("last_seen_at must be BIGINT");
            };
            let Value::String(id) = &last["id"] else {
                panic!("id must be TEXT");
            };
            cursor = Some((last_seen_at, id.clone()));
            forward.extend(page.rows.into_iter().map(|row| row["id"].clone()));
            assert!(forward.len() <= expected.len());
        }
        assert_eq!(
            forward,
            expected
                .iter()
                .map(|id| Value::String((*id).to_string()))
                .collect::<Vec<_>>()
        );

        let mut backward = Vec::new();
        let mut cursor: Option<(i64, String)> = None;
        loop {
            let page = match &cursor {
                None => database.query_sql(
                    "SELECT id, last_seen_at FROM sessions WHERE org_id = 'org-1' \
                     ORDER BY last_seen_at DESC, id DESC LIMIT 2",
                ),
                Some((last_seen_at, id)) => database.query_sql_with_params(
                    "SELECT id, last_seen_at FROM sessions \
                     WHERE org_id = 'org-1' \
                       AND (last_seen_at < $1 OR (last_seen_at = $1 AND id < $2)) \
                     ORDER BY last_seen_at DESC, id DESC LIMIT 2",
                    &[Value::Int(*last_seen_at), Value::String(id.clone())],
                ),
            }
            .expect("read backward keyset page");
            let Some(last) = page.rows.last() else {
                break;
            };
            let Value::Int(last_seen_at) = last["last_seen_at"] else {
                panic!("last_seen_at must be BIGINT");
            };
            let Value::String(id) = &last["id"] else {
                panic!("id must be TEXT");
            };
            cursor = Some((last_seen_at, id.clone()));
            backward.extend(page.rows.into_iter().map(|row| row["id"].clone()));
            assert!(backward.len() <= expected.len());
        }
        assert_eq!(
            backward,
            expected
                .iter()
                .rev()
                .map(|id| Value::String((*id).to_string()))
                .collect::<Vec<_>>()
        );

        for (cursor_time, cursor_id, expected_ids) in [
            (-1, "", vec!["session-a", "session-b"]),
            (20, "session-b", vec!["session-c", "session-d"]),
            (20, "session-bb", vec!["session-c", "session-d"]),
            (40, "session-g", vec![]),
        ] {
            let page = database
                .query_sql_with_params(
                    "SELECT id FROM sessions \
                     WHERE org_id = 'org-1' \
                       AND (last_seen_at > $1 OR (last_seen_at = $1 AND id > $2)) \
                     ORDER BY last_seen_at ASC, id ASC LIMIT 2",
                    &[
                        Value::Int(cursor_time),
                        Value::String(cursor_id.to_string()),
                    ],
                )
                .expect("read forward boundary page");
            assert_eq!(
                page.rows
                    .iter()
                    .map(|row| row["id"].clone())
                    .collect::<Vec<_>>(),
                expected_ids
                    .into_iter()
                    .map(|id| Value::String(id.to_string()))
                    .collect::<Vec<_>>()
            );
        }

        for (cursor_time, cursor_id, expected_ids) in [
            (50, "", vec!["session-g", "session-f"]),
            (30, "session-e", vec!["session-d", "session-c"]),
            (30, "session-ee", vec!["session-e", "session-d"]),
            (10, "session-a", vec![]),
        ] {
            let page = database
                .query_sql_with_params(
                    "SELECT id FROM sessions \
                     WHERE org_id = 'org-1' \
                       AND (last_seen_at < $1 OR (last_seen_at = $1 AND id < $2)) \
                     ORDER BY last_seen_at DESC, id DESC LIMIT 2",
                    &[
                        Value::Int(cursor_time),
                        Value::String(cursor_id.to_string()),
                    ],
                )
                .expect("read backward boundary page");
            assert_eq!(
                page.rows
                    .iter()
                    .map(|row| row["id"].clone())
                    .collect::<Vec<_>>(),
                expected_ids
                    .into_iter()
                    .map(|id| Value::String(id.to_string()))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn persistent_keyset_pagination_merges_base_live_transaction_and_recovery_entries() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-relational-keyset-{}-{nonce}",
            std::process::id()
        ));
        let config = DatabaseConfig {
            relational_index_mode: skein_storage::RelationalIndexMode::DemandPaged,
            ..DatabaseConfig::default()
        };
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                config.clone(),
            )
            .expect("open keyset database");
            database
                .query_sql(
                    "CREATE TABLE sessions (\
                        id TEXT PRIMARY KEY, \
                        org_id TEXT NOT NULL, \
                        last_seen_at BIGINT NOT NULL\
                    )",
                )
                .expect("create sessions table");
            database
                .query_sql(
                    "CREATE INDEX sessions_org_last_seen_id_idx \
                     ON sessions (org_id, last_seen_at, id)",
                )
                .expect("create sessions index");
            for (id, last_seen_at) in [("session-a", 10), ("session-b", 20), ("session-d", 30)] {
                database
                    .query_sql_with_params(
                        "INSERT INTO sessions (id, org_id, last_seen_at) \
                         VALUES ($1, 'org-1', $2)",
                        &[Value::String(id.to_string()), Value::Int(last_seen_at)],
                    )
                    .expect("insert base session");
            }
            database.checkpoint().expect("publish keyset index base");
            database
                .query_sql(
                    "INSERT INTO sessions (id, org_id, last_seen_at) \
                     VALUES ('session-c', 'org-1', 20), ('session-e', 'org-1', 40)",
                )
                .expect("insert live sessions");
            database
                .query_sql("DELETE FROM sessions WHERE id = 'session-b'")
                .expect("delete one base session");

            let live = database
                .query_sql(
                    "EXPLAIN ANALYZE SELECT id FROM sessions \
                     WHERE org_id = 'org-1' \
                       AND (last_seen_at < 40 OR (last_seen_at = 40 AND id < 'session-e')) \
                     ORDER BY last_seen_at DESC, id DESC LIMIT 2",
                )
                .expect("read backward keyset page over base and live entries");
            let info = relational_explain_operator_info(&live, "IndexRangeScanExec");
            assert!(info.contains("runtime_path=demand_paged"));
            assert!(info.contains("exclusive_seek_lookups=1"));
            assert!(info.contains("backward_lookups=1"));
            assert!(info.contains("early_stop_lookups=1"));

            let mut transaction = database.begin_transaction();
            transaction
                .query_sql(
                    "INSERT INTO sessions (id, org_id, last_seen_at) \
                     VALUES ('session-bb', 'org-1', 20)",
                )
                .expect("insert transaction-private session");
            let private = transaction
                .query_sql(
                    "SELECT id FROM sessions \
                     WHERE org_id = 'org-1' \
                       AND (last_seen_at < 30 OR (last_seen_at = 30 AND id < 'session-d')) \
                     ORDER BY last_seen_at DESC, id DESC LIMIT 2",
                )
                .expect("read transaction-private keyset page");
            assert_eq!(
                private
                    .rows
                    .iter()
                    .map(|row| row["id"].clone())
                    .collect::<Vec<_>>(),
                [
                    Value::String("session-c".to_string()),
                    Value::String("session-bb".to_string()),
                ]
            );
            transaction.rollback();
        }
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                config,
            )
            .expect("reopen keyset database");
            let recovered = database
                .query_sql(
                    "SELECT id FROM sessions \
                     WHERE org_id = 'org-1' \
                       AND (last_seen_at > 10 OR (last_seen_at = 10 AND id > 'session-a')) \
                     ORDER BY last_seen_at ASC, id ASC LIMIT 2",
                )
                .expect("read forward keyset page over recovery delta");
            assert_eq!(
                recovered
                    .rows
                    .iter()
                    .map(|row| row["id"].clone())
                    .collect::<Vec<_>>(),
                [
                    Value::String("session-c".to_string()),
                    Value::String("session-d".to_string()),
                ]
            );
        }
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                DatabaseConfig {
                    relational_index_mode: skein_storage::RelationalIndexMode::Authoritative,
                    ..DatabaseConfig::default()
                },
            )
            .expect("open authoritative keyset database");
            let authoritative = database
                .query_sql(
                    "EXPLAIN ANALYZE SELECT id FROM sessions \
                     WHERE org_id = 'org-1' \
                       AND (last_seen_at > 10 OR (last_seen_at = 10 AND id > 'session-a')) \
                     ORDER BY last_seen_at ASC, id ASC LIMIT 2",
                )
                .expect("read authoritative forward keyset page");
            let info = relational_explain_operator_info(&authoritative, "IndexRangeScanExec");
            assert!(info.contains("runtime_path=authoritative"));
            assert!(info.contains("exclusive_seek_lookups=1"));
            assert!(info.contains("early_stop_lookups=1"));
        }
        std::fs::remove_dir_all(path).expect("remove keyset database");
    }

    #[test]
    fn database_sql_entrypoint_executes_strict_append_ddl_insert_and_bounded_select() {
        let mut database = Database::new();
        database
            .query_sql(
                "CREATE TABLE public.events (\
                   stream_id TEXT NOT NULL, \
                   sequence BIGINT NOT NULL, \
                   payload BYTEA NOT NULL\
                 ) WITH (\
                   storage_mode = 'strict_append', \
                   partition_key = 'stream_id', \
                   order_key = 'sequence'\
                 )",
            )
            .expect("create strict append table");
        database
            .query_sql_with_params(
                "INSERT INTO public.events (stream_id, sequence, payload) \
                 VALUES ($1, $2, $3), ($1, $4, $5)",
                &[
                    Value::String("thread-1".to_string()),
                    Value::Int(1),
                    Value::Binary(vec![1]),
                    Value::Int(2),
                    Value::Binary(vec![2]),
                ],
            )
            .expect("append rows through SQL");

        let output = database
            .query_sql_with_params(
                "SELECT sequence, payload FROM public.events \
                 WHERE stream_id = $1 AND sequence > $2 \
                 ORDER BY sequence ASC LIMIT $3",
                &[
                    Value::String("thread-1".to_string()),
                    Value::Int(1),
                    Value::Int(10),
                ],
            )
            .expect("read strict append rows through bounded SQL");
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0],
            BTreeMap::from([
                ("payload".to_string(), Value::Binary(vec![2])),
                ("sequence".to_string(), Value::Int(2)),
            ])
        );

        let tables = database
            .query_sql(
                "SELECT table_name, storage_mode, partition_key, order_key \
                 FROM system.append_tables WHERE table_name = 'events'",
            )
            .expect("query strict append system catalog");
        assert_eq!(tables.rows.len(), 1);
        assert_eq!(
            tables.rows[0].get("storage_mode"),
            Some(&Value::String("strict_append".to_string()))
        );
        let storage = database
            .query_sql("SELECT live_rows FROM system.append_storage")
            .expect("query strict append storage residency");
        assert_eq!(storage.rows[0].get("live_rows"), Some(&Value::Int(2)));
        let explained = database
            .query_sql(
                "EXPLAIN SELECT * FROM events \
                 WHERE stream_id = 'thread-1' ORDER BY sequence ASC LIMIT 10",
            )
            .expect("explain strict append access path");
        assert_eq!(
            explained.rows[0].get("id"),
            Some(&Value::String("StrictAppendPartitionScan_1".to_string()))
        );
        let analyzed = database
            .query_sql(
                "EXPLAIN ANALYZE SELECT * FROM events \
                 WHERE stream_id = 'thread-1' ORDER BY sequence ASC LIMIT 10",
            )
            .expect("analyze strict append access path");
        assert!(matches!(
            analyzed.rows[0].get("execution info"),
            Some(Value::String(info)) if info.contains("live_rows_examined=2")
        ));

        let error = database
            .query_sql("UPDATE public.events SET payload = '\\x03' WHERE sequence = 2")
            .expect_err("strict append UPDATE must fail closed");
        assert!(error.to_string().contains("do not support UPDATE"));
    }

    #[test]
    fn database_transaction_stages_strict_append_sql_atomically() {
        let mut database = Database::new();
        let mut transaction = database.begin_transaction();
        transaction
            .query_sql(
                "CREATE TABLE events (\
                   stream_id TEXT NOT NULL, \
                   sequence BIGINT NOT NULL, \
                   payload TEXT NOT NULL\
                 ) WITH (\
                   storage_mode = 'strict_append', \
                   partition_key = 'stream_id', \
                   order_key = 'sequence'\
                 )",
            )
            .expect("stage strict append table");
        transaction
            .query_sql_with_params(
                "INSERT INTO events (stream_id, sequence, payload) VALUES ($1, $2, $3)",
                &[
                    Value::String("thread-1".to_string()),
                    Value::Int(1),
                    Value::String("first".to_string()),
                ],
            )
            .expect("stage strict append row");
        let staged = transaction
            .query_sql_with_params(
                "SELECT * FROM events WHERE stream_id = $1 ORDER BY sequence LIMIT 10",
                &[Value::String("thread-1".to_string())],
            )
            .expect("read staged strict append row");
        assert_eq!(staged.rows.len(), 1);
        let staged_catalog = transaction
            .query_sql("SELECT table_name FROM system.append_tables")
            .expect("read staged strict append catalog");
        assert_eq!(
            staged_catalog.rows[0].get("table_name"),
            Some(&Value::String("events".to_string()))
        );
        transaction
            .commit()
            .expect("commit strict append transaction");

        let committed = database
            .query_sql_with_params(
                "SELECT payload FROM events WHERE stream_id = $1 ORDER BY sequence LIMIT 10",
                &[Value::String("thread-1".to_string())],
            )
            .expect("read committed strict append row");
        assert_eq!(committed.rows.len(), 1);
        assert_eq!(
            committed.rows[0].get("payload"),
            Some(&Value::String("first".to_string()))
        );
    }

    #[test]
    fn generated_order_is_assigned_only_by_the_durable_commit_result() {
        let mut database = Database::new();
        database
            .query_sql(
                "CREATE TABLE events (\
                   stream_id TEXT NOT NULL, \
                   sequence BIGINT NOT NULL, \
                   payload TEXT NOT NULL\
                 ) WITH (\
                   storage_mode = 'strict_append', \
                   partition_key = 'stream_id', \
                   order_key = 'sequence', \
                   generated_order = 'commit_sequence'\
                 )",
            )
            .expect("create generated-order append table");
        database
            .query_sql("CREATE TABLE metadata (id BIGINT PRIMARY KEY, value TEXT NOT NULL)")
            .expect("create row-page table");
        let before_commit_epoch = database.commit_epoch();

        let mut transaction = database.begin_transaction();
        transaction
            .query("CREATE (:CommitMarker {id: 1})")
            .expect("stage graph row");
        transaction
            .query_sql("INSERT INTO metadata (id, value) VALUES (1, 'committed')")
            .expect("stage row-page row");
        transaction
            .query_sql(
                "INSERT INTO events (stream_id, payload) VALUES \
                 ('thread-1', 'first'), ('thread-2', 'second'), ('thread-1', 'third')",
            )
            .expect("stage generated rows");
        for sql in [
            "SELECT * FROM events WHERE stream_id = 'thread-1' \
             ORDER BY sequence LIMIT 10",
            "EXPLAIN SELECT * FROM events WHERE stream_id = 'thread-1' \
             ORDER BY sequence LIMIT 10",
        ] {
            let read_error = transaction
                .query_sql(sql)
                .expect_err("pending generated rows must fail closed on read");
            assert!(read_error
                .to_string()
                .contains("reads are unavailable until commit"));
        }

        let committed = transaction
            .commit_with_result()
            .expect("commit generated rows");
        assert_eq!(database.commit_epoch(), before_commit_epoch + 1);
        assert_eq!(committed.mutations.len(), 1);
        assert_eq!(committed.mutations[0].affected_rows, 1);
        assert_eq!(committed.append_mutations.len(), 1);
        assert_eq!(
            committed.append_mutations[0].generated_order_keys,
            vec![
                skein_storage::RelationalKey(vec![RelationalValue::BigInt(1)]),
                skein_storage::RelationalKey(vec![RelationalValue::BigInt(2)]),
                skein_storage::RelationalKey(vec![RelationalValue::BigInt(3)]),
            ]
        );

        let rows = database
            .query_sql(
                "SELECT sequence, payload FROM events WHERE stream_id = 'thread-1' \
                 ORDER BY sequence LIMIT 10",
            )
            .expect("read committed generated rows");
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0].get("sequence"), Some(&Value::Int(1)));
        assert_eq!(rows.rows[1].get("sequence"), Some(&Value::Int(3)));
        assert_eq!(
            database
                .query("MATCH (marker:CommitMarker) RETURN marker.id AS id")
                .expect("read committed graph row")
                .rows[0]
                .get("id"),
            Some(&Value::Int(1))
        );
        assert_eq!(
            database
                .query_sql("SELECT value FROM metadata WHERE id = 1")
                .expect("read committed row-page row")
                .rows[0]
                .get("value"),
            Some(&Value::String("committed".to_string()))
        );

        let catalog = database
            .query_sql(
                "SELECT order_mode, generated_order_watermark FROM system.append_tables \
                 WHERE table_name = 'events'",
            )
            .expect("read generated-order observability");
        assert_eq!(
            catalog.rows[0].get("order_mode"),
            Some(&Value::String("commit_sequence".to_string()))
        );
        assert_eq!(
            catalog.rows[0].get("generated_order_watermark"),
            Some(&Value::Int(3))
        );

        for sql in [
            "INSERT INTO events (stream_id, sequence, payload) \
             VALUES ('thread-1', 4, 'override')",
            "INSERT INTO events (stream_id, payload) VALUES ('thread-1', 'returning') \
             RETURNING sequence",
        ] {
            assert!(database.query_sql(sql).is_err());
        }
    }

    #[test]
    fn strict_append_sql_fails_closed_for_unbounded_and_mutating_shapes() {
        let mut database = Database::new();
        database
            .query_sql(
                "CREATE TABLE events (\
                   stream_id TEXT NOT NULL, \
                   sequence BIGINT NOT NULL, \
                   payload TEXT NOT NULL\
                 ) WITH (\
                   storage_mode = 'strict_append', \
                   partition_key = 'stream_id', \
                   order_key = 'sequence'\
                 )",
            )
            .expect("create strict append table");
        database
            .query_sql(
                "INSERT INTO events (stream_id, sequence, payload) \
                 VALUES ('thread-1', 1, 'first')",
            )
            .expect("append initial row");

        for (sql, expected) in [
            (
                "SELECT * FROM events WHERE stream_id = 'thread-1' ORDER BY sequence",
                "requires an explicit LIMIT",
            ),
            (
                "SELECT * FROM events ORDER BY sequence LIMIT 10",
                "requires an exact partition predicate",
            ),
            (
                "SELECT * FROM events WHERE stream_id = 'thread-1' LIMIT 10",
                "requires ORDER BY sequence ASC",
            ),
            (
                "SELECT * FROM events AS e WHERE events.stream_id = 'thread-1' \
                 ORDER BY e.sequence LIMIT 10",
                "unknown qualifier events",
            ),
            (
                "DELETE FROM events WHERE stream_id = 'thread-1'",
                "do not support DELETE",
            ),
            (
                "INSERT INTO events (stream_id, sequence, payload) \
                 VALUES ('thread-1', 2, 'second') \
                 ON CONFLICT (stream_id, sequence) DO NOTHING",
                "does not support ON CONFLICT",
            ),
        ] {
            let error = database.query_sql(sql).expect_err("shape must fail closed");
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, received {error}"
            );
        }

        let error = database
            .query_sql(
                "INSERT INTO events (stream_id, sequence, payload) \
                 VALUES ('thread-1', 1, 'duplicate')",
            )
            .expect_err("duplicate order key must fail");
        assert!(error.to_string().contains("order key must increase"));
    }

    #[test]
    fn database_sql_entrypoint_executes_relational_ddl_dml_and_select() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE public.messages (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
            .expect("create relational table");
        database
            .query_sql_with_params(
                "INSERT INTO public.messages (id, body) VALUES ($1, $2)",
                &[
                    Value::String("message-1".to_string()),
                    Value::String("payload".to_string()),
                ],
            )
            .expect("insert relational row");

        let output = database
            .query_sql_with_params(
                "SELECT id, body FROM public.messages WHERE id = $1",
                &[Value::String("message-1".to_string())],
            )
            .expect("query relational row");
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0],
            BTreeMap::from([
                ("body".to_string(), Value::String("payload".to_string())),
                ("id".to_string(), Value::String("message-1".to_string())),
            ])
        );

        database
            .query_sql_with_params(
                "INSERT INTO public.messages (id, body) VALUES ($1, $2)",
                &[
                    Value::String("message-2".to_string()),
                    Value::String("payload-2".to_string()),
                ],
            )
            .expect("insert second relational row");
        let count = database
            .query_sql_bounded(
                "SELECT COUNT(*) AS message_count FROM public.messages",
                Some(1),
            )
            .expect("aggregate with one output row scans the configured intermediate budget");
        assert_eq!(count.rows[0]["message_count"], Value::Int(2));

        let explain = database
            .query_sql_with_params(
                "EXPLAIN SELECT id FROM public.messages WHERE id = $1",
                &[Value::String("message-1".to_string())],
            )
            .expect("plan relational SQL through the public query entrypoint");
        assert!(explain.rows.iter().any(|row| {
            matches!(
                row.get("access object"),
                Some(Value::String(access)) if access.contains("primary_key")
            )
        }));

        let analyze = database
            .query_sql("EXPLAIN ANALYZE SELECT id FROM public.messages ORDER BY id")
            .expect("profile relational SQL through the public query entrypoint");
        assert_eq!(analyze.rows[0]["actRows"], Value::Null);
        assert!(matches!(
            analyze.rows[0].get("execution info"),
            Some(Value::String(info)) if info.contains("statement_output_rows=2")
        ));
        assert!(analyze
            .rows
            .iter()
            .any(|row| matches!(row.get("actRows"), Some(Value::Int(2)))));

        let snapshot = database.begin_read_transaction();
        let snapshot_output = snapshot
            .query_sql("SELECT id FROM public.messages ORDER BY id")
            .expect("query relational snapshot");
        assert_eq!(snapshot_output.rows.len(), 2);
        assert_eq!(
            snapshot_output.rows[0]["id"],
            Value::String("message-1".to_string())
        );
    }

    #[test]
    fn database_sql_preserves_bytea_as_binary_values() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE payloads (id TEXT PRIMARY KEY, payload BYTEA NOT NULL)")
            .expect("create binary table");
        let payload = Value::Binary(vec![0, 1, 0xfe, 0xff]);
        database
            .query_sql_with_params(
                "INSERT INTO payloads (id, payload) VALUES ($1, $2)",
                &[Value::String("payload-1".to_string()), payload.clone()],
            )
            .expect("insert binary row");

        let output = database
            .query_sql_with_params(
                "SELECT payload FROM payloads WHERE payload = $1",
                std::slice::from_ref(&payload),
            )
            .expect("query binary row");
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["payload"], payload);
    }

    #[test]
    fn database_sql_exposes_projection_schema_without_row_maps() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE documents (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
            .expect("create documents table");
        database
            .query_sql("INSERT INTO documents (id, body) VALUES ('doc-1', 'body-1')")
            .expect("insert document");

        let output = database
            .query_sql("SELECT body AS payload, id FROM documents")
            .expect("query positional result");
        assert_eq!(output.schema().columns(), ["payload", "id"]);
        assert_eq!(
            output.value_rows().collect::<Vec<_>>(),
            vec![
                &[
                    Value::String("body-1".to_string()),
                    Value::String("doc-1".to_string()),
                ][..]
            ]
        );
        assert_eq!(
            output.rows[0]["payload"],
            Value::String("body-1".to_string())
        );

        let empty = database
            .query_sql("SELECT body AS payload, id FROM documents WHERE body = 'missing'")
            .expect("query empty positional result");
        assert_eq!(empty.schema().columns(), ["payload", "id"]);
        assert!(empty.value_rows().is_empty());
    }

    #[test]
    fn borrowed_streaming_binding_matches_owned_three_valued_execution() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE logic_groups (id TEXT PRIMARY KEY)")
            .expect("create logic groups table");
        database
            .query_sql(
                "CREATE TABLE logic_rows (id TEXT PRIMARY KEY, flag BOOLEAN NOT NULL, marker BIGINT, body TEXT NOT NULL, group_id TEXT NOT NULL)",
            )
            .expect("create logic rows table");
        database
            .query_sql("INSERT INTO logic_groups (id) VALUES ('group-1')")
            .expect("insert group");
        for parameters in [
            vec![
                Value::String("row-a".to_string()),
                Value::Bool(true),
                Value::Null,
                Value::String("body-a".to_string()),
            ],
            vec![
                Value::String("row-b".to_string()),
                Value::Bool(false),
                Value::Int(7),
                Value::String("body-b".to_string()),
            ],
            vec![
                Value::String("row-c".to_string()),
                Value::Bool(false),
                Value::Null,
                Value::String("body-c".to_string()),
            ],
        ] {
            database
                .query_sql_with_params(
                    "INSERT INTO logic_rows (id, flag, marker, body, group_id) VALUES ($1, $2, $3, $4, 'group-1')",
                    &parameters,
                )
                .expect("insert logic row");
        }

        let parameters = [
            Value::Bool(false),
            Value::Int(7),
            Value::Null,
            Value::Int(7),
        ];
        let borrowed = database
            .query_sql_with_params(
                "SELECT id AS item_id, body AS payload FROM logic_rows \
                 WHERE (flag = $1 OR marker IN ($2, $3)) \
                   AND (marker IS NULL OR marker >= $4)",
                &parameters,
            )
            .expect("execute ordinal-bound borrowed scan");
        let owned = database
            .query_sql_with_params(
                "SELECT r.id AS item_id, r.body AS payload FROM logic_rows AS r \
                 INNER JOIN logic_groups AS g ON g.id = r.group_id \
                 WHERE (r.flag = $1 OR r.marker IN ($2, $3)) \
                   AND (r.marker IS NULL OR r.marker >= $4)",
                &parameters,
            )
            .expect("execute owned join oracle");

        assert_eq!(borrowed.rows, owned.rows);
        assert_eq!(borrowed.rows.len(), 2);
        assert_eq!(
            borrowed.rows[0]["item_id"],
            Value::String("row-b".to_string())
        );
        assert_eq!(
            borrowed.rows[1]["item_id"],
            Value::String("row-c".to_string())
        );
        assert!(borrowed
            .rows
            .iter()
            .all(|row| row.contains_key("payload") && !row.contains_key("body")));
    }

    #[test]
    fn relational_sql_like_and_ilike_cover_scans_joins_groups_and_nulls() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE like_feeds (id TEXT PRIMARY KEY, title TEXT NOT NULL)")
            .expect("create LIKE feeds");
        database
            .query_sql(
                "CREATE TABLE like_documents (id TEXT PRIMARY KEY, feed_id TEXT NOT NULL, title TEXT)",
            )
            .expect("create LIKE documents");
        database
            .query_sql(
                "INSERT INTO like_feeds (id, title) VALUES \
                 ('feed-1', 'Engineering Notes'), ('feed-2', 'General')",
            )
            .expect("insert LIKE feeds");
        database
            .query_sql(
                "INSERT INTO like_documents (id, feed_id, title) VALUES \
                 ('doc-1', 'feed-1', 'road_map'), \
                 ('doc-2', 'feed-1', 'roadXmap'), \
                 ('doc-3', 'feed-2', '100% complete'), \
                 ('doc-4', 'feed-2', 'ÄPFEL Guide'), \
                 ('doc-5', 'feed-2', 'misc'), \
                 ('doc-6', 'feed-2', NULL), \
                 ('doc-7', 'feed-2', 'Straße')",
            )
            .expect("insert LIKE documents");

        let escaped = database
            .query_sql("SELECT id FROM like_documents WHERE title LIKE 'road\\_map'")
            .expect("match escaped underscore through borrowed scan");
        assert_eq!(escaped.rows.len(), 1);
        assert_eq!(escaped.rows[0]["id"], text("doc-1"));

        let wildcard = database
            .query_sql("SELECT id FROM like_documents WHERE title LIKE 'road_map'")
            .expect("match single-character wildcard");
        assert_eq!(wildcard.rows.len(), 2);

        let suffix = database
            .query_sql_with_params(
                "SELECT id FROM like_documents WHERE title LIKE $1",
                &[text("%complete")],
            )
            .expect("match parameterized suffix");
        assert_eq!(suffix.rows.len(), 1);
        assert_eq!(suffix.rows[0]["id"], text("doc-3"));

        let custom_escape = database
            .query_sql("SELECT id FROM like_documents WHERE title LIKE '100!% complete' ESCAPE '!'")
            .expect("match custom escaped wildcard");
        assert_eq!(custom_escape.rows.len(), 1);
        assert_eq!(custom_escape.rows[0]["id"], text("doc-3"));

        let ilike = database
            .query_sql_with_params(
                "SELECT id FROM like_documents WHERE title ILIKE $1",
                &[text("%äpfel%")],
            )
            .expect("match Unicode ILIKE without a process locale");
        assert_eq!(ilike.rows.len(), 1);
        assert_eq!(ilike.rows[0]["id"], text("doc-4"));

        let case_folded = database
            .query_sql("SELECT id FROM like_documents WHERE title ILIKE '%STRASSE%'")
            .expect("match Unicode default case folding");
        assert_eq!(case_folded.rows.len(), 1);
        assert_eq!(case_folded.rows[0]["id"], text("doc-7"));

        let case_folded_wildcard = database
            .query_sql("SELECT id FROM like_documents WHERE title ILIKE 'Stra_e'")
            .expect("preserve wildcard cardinality across case folding");
        assert_eq!(case_folded_wildcard.rows.len(), 1);
        assert_eq!(case_folded_wildcard.rows[0]["id"], text("doc-7"));

        let bounded = database
            .query_sql(
                "SELECT id FROM like_documents WHERE title ILIKE 'road%' \
                 ORDER BY id ASC LIMIT 1 OFFSET 1",
            )
            .expect("apply ILIKE before bounded ordering");
        assert_eq!(bounded.rows.len(), 1);
        assert_eq!(bounded.rows[0]["id"], text("doc-2"));

        let null_pattern = database
            .query_sql_with_params(
                "SELECT id FROM like_documents WHERE title LIKE $1",
                &[Value::Null],
            )
            .expect("NULL LIKE pattern evaluates to unknown");
        assert!(null_pattern.rows.is_empty());
        let negated = database
            .query_sql("SELECT id FROM like_documents WHERE title NOT ILIKE 'road%'")
            .expect("NOT ILIKE retains SQL NULL semantics");
        assert_eq!(negated.rows.len(), 4);
        assert!(negated.rows.iter().all(|row| row["id"] != text("doc-6")));

        let grouped = database
            .query_sql(
                "SELECT feed_id, COUNT(*) AS document_count \
                 FROM like_documents WHERE title ILIKE '%guide%' GROUP BY feed_id",
            )
            .expect("group LIKE-filtered documents");
        assert_eq!(grouped.rows.len(), 1);
        assert_eq!(grouped.rows[0]["feed_id"], text("feed-2"));
        assert_eq!(grouped.rows[0]["document_count"], Value::Int(1));

        let joined = database
            .query_sql(
                "SELECT d.id FROM like_documents AS d \
                 INNER JOIN like_feeds AS f ON f.id = d.feed_id \
                 WHERE f.title ILIKE 'engineering%' OR d.title LIKE '%complete'",
            )
            .expect("evaluate LIKE predicates across a join");
        assert_eq!(joined.rows.len(), 3);

        let explain = database
            .query_sql("EXPLAIN SELECT id FROM like_documents WHERE title ILIKE '%guide%'")
            .expect("explain scan-based ILIKE evaluation");
        assert!(relational_explain_operator_info(&explain, "SelectionExec").contains("ILIKE"));
        assert!(explain.rows.iter().any(|row| matches!(
            row.get("id"),
            Some(Value::String(id)) if id.contains("TableFullScanExec")
        )));

        let error = database
            .query_sql("UPDATE like_documents SET title = 'changed' WHERE title LIKE 'road%'")
            .expect_err("mutation LIKE remains outside the relational write contract");
        assert!(error.to_string().contains("do not support LIKE or ILIKE"));
    }

    #[test]
    fn relational_explain_keeps_estimated_rows_non_zero_for_empty_results() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE messages (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
            .expect("create empty relational table");

        let explain = database
            .query_sql("EXPLAIN SELECT id FROM messages LIMIT 0")
            .expect("explain empty relational query");
        assert!(explain
            .rows
            .iter()
            .all(|row| { matches!(row.get("estRows"), Some(Value::Int(rows)) if *rows >= 1) }));

        let analyze = database
            .query_sql("EXPLAIN ANALYZE SELECT id FROM messages LIMIT 0")
            .expect("analyze empty relational query");
        assert_eq!(
            analyze.rows[0]["id"],
            Value::String("LimitExec_logical_limit".to_string())
        );
        assert_eq!(analyze.rows[0]["actRows"], Value::Null);
        assert!(matches!(
            analyze.rows[0].get("execution info"),
            Some(Value::String(info)) if info.contains("statement_output_rows=0")
        ));
        assert!(analyze.rows.iter().any(|row| {
            matches!(
                row.get("id"),
                Some(Value::String(id)) if id.contains("TableFullScanExec_1")
            ) && matches!(row.get("actRows"), Some(Value::Null))
        }));
        assert!(analyze
            .rows
            .iter()
            .all(|row| { matches!(row.get("estRows"), Some(Value::Int(rows)) if *rows >= 1) }));
    }

    fn explain_join_with_search_budgets(max_groups: usize, max_expressions: usize) -> String {
        let mut database = Database::new_with_config(DatabaseConfig {
            max_optimizer_groups: Some(max_groups),
            max_relational_join_expressions: Some(max_expressions),
            ..DatabaseConfig::default()
        });
        database
            .query_sql("CREATE TABLE join_left (id BIGINT PRIMARY KEY)")
            .expect("create left join table");
        database
            .query_sql("CREATE TABLE join_right (id BIGINT PRIMARY KEY, left_id BIGINT NOT NULL)")
            .expect("create right join table");

        let explain = database
            .query_sql(
                "EXPLAIN SELECT l.id FROM join_left AS l \
                 INNER JOIN join_right AS r ON r.left_id = l.id \
                 ORDER BY l.id",
            )
            .expect("explain join with configured search budgets");
        let join = explain
            .rows
            .iter()
            .find(|row| {
                matches!(
                    row.get("id"),
                    Some(Value::String(id)) if id.contains("HashJoinExec")
                )
            })
            .expect("join explain row");
        match join.get("operator info") {
            Some(Value::String(info)) => info.clone(),
            value => panic!("expected join operator info, got {value:?}"),
        }
    }

    #[test]
    fn database_config_injects_relational_join_search_budgets() {
        let group_limited = explain_join_with_search_budgets(2, 5);
        assert!(matches!(
            group_limited.as_str(),
            info if info.contains("planning_status=fallback")
                && info.contains("planning_reason=group_budget_exceeded")
                && info.contains("max_groups=2")
                && info.contains("max_expressions=5")
                && info.contains("0:csg_cmp_memo:fallback:group_budget_exceeded")
                && info.contains("1:inner_join_memo:fallback:group_budget_exceeded")
                && info.contains("2:syntax_order:selected:syntax_fallback")
        ));

        let expression_limited = explain_join_with_search_budgets(3, 1);
        assert!(matches!(
            expression_limited.as_str(),
            info if info.contains("planning_status=fallback")
                && info.contains("planning_reason=expression_budget_exceeded")
                && info.contains("max_groups=3")
                && info.contains("max_expressions=1")
                && info.contains("0:csg_cmp_memo:fallback:expression_budget_exceeded")
                && info.contains("1:inner_join_memo:fallback:expression_budget_exceeded")
                && info.contains("2:syntax_order:selected:syntax_fallback")
        ));
    }

    #[test]
    fn prepared_relational_execution_is_admitted_before_scanning() {
        let mut config = DatabaseConfig::default();
        config.execution_memory.query_memory_bytes =
            std::num::NonZeroUsize::new(1_024).expect("non-zero query memory budget");
        config.execution_memory.batch_payload_bytes =
            std::num::NonZeroUsize::new(768).expect("non-zero batch memory budget");
        config.execution_memory.blocking_operator_bytes =
            std::num::NonZeroUsize::new(512).expect("non-zero blocking memory budget");
        let mut database = Database::new_with_config(config);
        database
            .query_sql("CREATE TABLE admission_rows (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)")
            .expect("create admission table");
        database
            .query_sql("INSERT INTO admission_rows (id, value) VALUES (1, 2)")
            .expect("insert admission row");

        let streaming = database
            .query_sql("SELECT id FROM admission_rows")
            .expect("streaming descriptor fits the query memory budget");
        assert_eq!(streaming.rows.len(), 1);

        database
            .query_sql("EXPLAIN SELECT id FROM admission_rows ORDER BY value")
            .expect("plain explain does not admit execution resources");
        let error = database
            .query_sql("SELECT id FROM admission_rows ORDER BY value")
            .expect_err("blocking descriptor must be rejected before execution");
        assert!(error.to_string().contains(
            "prepared relational query requires 1280 estimated bytes, exceeding query_memory_bytes 1024"
        ));
    }

    #[test]
    fn database_sql_uses_bounded_pinned_relational_indexes_with_observable_fallback() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-relational-sql-demand-index-{}-{nonce}",
            std::process::id()
        ));
        let config = DatabaseConfig {
            segment_cache_capacity_bytes: 64 * 1024,
            relational_index_mode: skein_storage::RelationalIndexMode::DemandPaged,
            ..DatabaseConfig::default()
        };
        let published_generation;
        let published_pages;
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                config.clone(),
            )
            .expect("open demand-index database");
            database
                .query_sql(
                    "CREATE TABLE documents (id TEXT PRIMARY KEY, owner TEXT NOT NULL, body TEXT NOT NULL)",
                )
                .expect("create documents table");
            database
                .query_sql("CREATE INDEX documents_owner_id_idx ON documents (owner, id)")
                .expect("create documents owner index");
            database
                .query_sql("CREATE TABLE anchors (id TEXT PRIMARY KEY, document_id TEXT NOT NULL)")
                .expect("create anchors table");
            database
                .query_sql("CREATE INDEX anchors_document_idx ON anchors (document_id)")
                .expect("create anchor document index");
            database
                .query_sql(
                    "INSERT INTO documents (id, owner, body) VALUES ('doc-1', 'owner-1', 'body-1')",
                )
                .expect("insert base document");
            database
                .query_sql_with_params(
                    "INSERT INTO documents (id, owner, body) VALUES ('doc-large', 'owner-large', $1)",
                    &[Value::String("x".repeat(96 * 1024))],
                )
                .expect("insert large base document");
            database
                .query_sql_with_params(
                    "INSERT INTO documents (id, owner, body) VALUES ('doc-mid', 'owner-order', $1)",
                    &[Value::String("m".repeat(96 * 1024))],
                )
                .expect("insert ordered base document");
            database
                .query_sql("INSERT INTO anchors (id, document_id) VALUES ('anchor-1', 'doc-1')")
                .expect("insert base anchor");

            let fallback = database
                .query_sql("EXPLAIN ANALYZE SELECT id FROM documents WHERE owner = 'owner-1'")
                .expect("fall back before a relational index view is published");
            let fallback_info = relational_explain_operator_info(&fallback, "IndexRangeScanExec");
            assert!(fallback_info.contains("runtime_path=canonical_fallback"));
            assert!(fallback_info.contains("fallback_reasons=read_view_unavailable"));

            database
                .checkpoint()
                .expect("publish relational index base");
            let published = database
                .relational_index_shadow_checkpoint_report()
                .expect("published relational index evidence");
            published_generation = published.generation;
            published_pages = published.pages_written;
            let primary = database
                .query_sql("EXPLAIN ANALYZE SELECT id FROM documents WHERE id = 'doc-1'")
                .expect("read the demand-paged primary row");
            let primary_info = relational_explain_operator_info(&primary, "TablePointGetExec");
            assert!(primary_info.contains("row_runtime_path=snapshot_rows"));
            assert!(primary_info.contains("row_root_set_digest="));
            let id_only = database
                .query_sql("EXPLAIN ANALYZE SELECT id FROM documents WHERE id = 'doc-large'")
                .expect("project no large value from a row page");
            assert!(relational_explain_execution_info(&id_only).contains("hydrated_rows=0"));
            let lending_scan = database
                .query_sql("EXPLAIN ANALYZE SELECT id FROM documents WHERE body = 'body-1' LIMIT 1")
                .expect("filter and project from a borrowed row-page view");
            let lending_info = relational_explain_operator_info(&lending_scan, "TableFullScanExec");
            assert!(lending_info.contains("row_borrowed_rows=1"));
            assert!(lending_info.contains("row_owned_rows=0"));
            let late_hydration = database
                .query_sql(
                    "EXPLAIN ANALYZE SELECT body FROM documents ORDER BY id ASC LIMIT 1 OFFSET 1",
                )
                .expect("hydrate only the selected large result after TopN");
            assert!(relational_explain_execution_info(&late_hydration).contains("hydrated_rows=1"));
            let grouped = database
                .query_sql(
                    "SELECT document_id, COUNT(id) AS anchor_count FROM anchors GROUP BY document_id",
                )
                .expect("read aggregate inputs omitted from the final grouping key");
            assert_eq!(grouped.rows.len(), 1);
            assert_eq!(grouped.rows[0]["anchor_count"], Value::Int(1));
            {
                let mut transaction = database.begin_transaction();
                transaction
                    .query_sql(
                        "INSERT INTO documents (id, owner, body) VALUES ('doc-tx', 'owner-1', 'body-tx')",
                    )
                    .expect("insert transaction-local document");
                let own_write = transaction
                    .query_sql("SELECT id FROM documents WHERE id = 'doc-tx'")
                    .expect("read transaction-local document");
                assert_eq!(own_write.rows.len(), 1);
                let workspace = transaction
                    .query_sql("EXPLAIN ANALYZE SELECT id FROM documents WHERE owner = 'owner-1'")
                    .expect("explain transaction-workspace fallback");
                let workspace_info =
                    relational_explain_operator_info(&workspace, "IndexRangeScanExec");
                assert!(workspace_info.contains("runtime_path=canonical_fallback"));
                assert!(workspace_info.contains("fallback_reasons=transaction_workspace"));
                transaction.rollback();
            }
            database
                .query_sql(
                    "INSERT INTO documents (id, owner, body) VALUES ('doc-2', 'owner-1', 'body-2')",
                )
                .expect("insert live document");
            for (id, body) in [("doc-a", "a"), ("doc-z", "z")] {
                database
                    .query_sql_with_params(
                        "INSERT INTO documents (id, owner, body) VALUES ($1, 'owner-order', $2)",
                        &[
                            Value::String(id.to_string()),
                            Value::String(body.repeat(96 * 1024)),
                        ],
                    )
                    .expect("insert ordered live document");
            }
            database
                .query_sql("INSERT INTO anchors (id, document_id) VALUES ('anchor-2', 'doc-2')")
                .expect("insert live anchor");
            database
                .query_sql("DELETE FROM anchors WHERE id = 'anchor-1'")
                .expect("delete base anchor through live delta");
            database
                .query_sql("DELETE FROM documents WHERE id = 'doc-1'")
                .expect("delete base document through live delta");

            let rows = database
                .query_sql("SELECT id FROM documents WHERE owner = 'owner-1' ORDER BY id ASC")
                .expect("read live-merged relational index");
            assert_eq!(rows.rows.len(), 1);
            assert_eq!(rows.rows[0]["id"], Value::String("doc-2".to_string()));
            let ordered_page = database
                .query_sql(
                    "SELECT id, body FROM documents WHERE owner = 'owner-order' ORDER BY id ASC LIMIT 1 OFFSET 1",
                )
                .expect("merge live entries into index order before pagination");
            assert_eq!(ordered_page.rows.len(), 1);
            assert_eq!(
                ordered_page.rows[0]["id"],
                Value::String("doc-mid".to_string())
            );
            let live = database
                .query_sql("EXPLAIN ANALYZE SELECT id FROM documents WHERE id = 'doc-2'")
                .expect("read the live row overlay");
            let live_info = relational_explain_operator_info(&live, "TablePointGetExec");
            assert!(live_info.contains("row_runtime_path=snapshot_rows"));
            assert!(live_info.contains("row_overlay_entries=1"));

            let joined = database
                .query_sql(
                    "EXPLAIN ANALYZE SELECT d.id FROM documents AS d INNER JOIN anchors AS a ON a.document_id = d.id WHERE d.owner = 'owner-1'",
                )
                .expect("run base and join demand-index probes");
            let demand_infos = joined
                .rows
                .iter()
                .filter_map(|row| match (row.get("id"), row.get("operator info")) {
                    (Some(Value::String(id)), Some(Value::String(info)))
                        if id.contains("IndexRangeScanExec")
                            || id.contains("IndexNestedLoopJoinExec") =>
                    {
                        Some(info.as_str())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(demand_infos.len(), 2);
            assert!(demand_infos
                .iter()
                .all(|info| info.contains("runtime_path=demand_paged")));
            assert!(demand_infos
                .iter()
                .all(|info| info.contains("base_generation=") && info.contains("live_entries=")));

            let scanned_join = database
                .query_sql(
                    "SELECT d.id FROM documents AS d INNER JOIN anchors AS a ON a.document_id = d.id ORDER BY d.id ASC",
                )
                .expect("run a canonical row scan with nested point hydration");
            assert_eq!(scanned_join.rows.len(), 1);
            assert_eq!(
                scanned_join.rows[0]["id"],
                Value::String("doc-2".to_string())
            );
        }
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                config.clone(),
            )
            .expect("reopen demand-index database");
            let recovered = database
                .query_sql(
                    "EXPLAIN ANALYZE SELECT id FROM documents WHERE owner = 'owner-1' ORDER BY id ASC LIMIT 1",
                )
                .expect("read ordered recovery-delta relational index");
            let recovered_info = relational_explain_operator_info(&recovered, "IndexRangeScanExec");
            assert!(recovered_info.contains("runtime_path=demand_paged"));
            assert!(recovered_info.contains("order_prefix=1"));
            assert!(!recovered_info.contains("delta_generation=none"));
            assert!(recovered_info.contains("delta_pages_skipped="));
            assert!(recovered_info.contains("delta_entries="));
            assert!(recovered_info.contains("row_runtime_path=snapshot_rows"));
            assert!(!recovered_info.contains("row_delta_generation=none"));
            assert!(recovered.rows.iter().all(|row| {
                !matches!(row.get("id"), Some(Value::String(id)) if id.contains("TopNExec"))
            }));
            let recovered_page = database
                .query_sql(
                    "SELECT id FROM documents WHERE owner = 'owner-order' ORDER BY id ASC LIMIT 1 OFFSET 1",
                )
                .expect("preserve index order through recovery delta pagination");
            assert_eq!(
                recovered_page.rows[0]["id"],
                Value::String("doc-mid".to_string())
            );
        }
        {
            let tight_config = DatabaseConfig {
                max_read_result_payload_bytes: Some(4 * 1024),
                ..config.clone()
            };
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                tight_config,
            )
            .expect("reopen demand-index database with a tight read budget");
            let admitted = database
                .query_sql("EXPLAIN ANALYZE SELECT id FROM documents WHERE owner = 'owner-1'")
                .expect("read the index page independently of the small result budget");
            let admitted_info = relational_explain_operator_info(&admitted, "IndexRangeScanExec");
            assert!(admitted_info.contains("runtime_path=demand_paged"));
            assert!(admitted_info.contains("fallback_reasons=none"));
            let aggregate = database
                .query_sql(
                    "SELECT COALESCE(SUM(OCTET_LENGTH(body)), 0) AS body_bytes FROM documents",
                )
                .expect("scan large inputs independently of the small output payload budget");
            assert!(
                matches!(aggregate.rows[0]["body_bytes"], Value::Int(value) if value > 96 * 1024)
            );
        }
        {
            let hydration_limited_config = DatabaseConfig {
                max_read_result_payload_bytes: Some(4 * 1024),
                max_relational_hydration_bytes: std::num::NonZeroUsize::new(32 * 1024)
                    .expect("test hydration budget is non-zero"),
                ..config.clone()
            };
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                hydration_limited_config,
            )
            .expect("reopen demand-index database with a hydration budget");
            let aggregate = database
                .query_sql(
                    "SELECT COALESCE(SUM(OCTET_LENGTH(body)), 0) AS body_bytes FROM documents",
                )
                .expect("length aggregate must use overflow metadata without hydration");
            assert!(
                matches!(aggregate.rows[0]["body_bytes"], Value::Int(value) if value > 96 * 1024)
            );
            let filtered = database
                .query_sql(
                    "SELECT COALESCE(SUM(OCTET_LENGTH(body)), 0) AS body_bytes FROM documents WHERE owner = 'owner-large'",
                )
                .expect("indexed length aggregate must retain metadata-only point reads");
            assert_eq!(filtered.rows[0]["body_bytes"], Value::Int(96 * 1024));
            let analyzed = database
                .query_sql(
                    "EXPLAIN ANALYZE SELECT COALESCE(SUM(OCTET_LENGTH(body)), 0) AS body_bytes FROM documents WHERE owner = 'owner-large'",
                )
                .expect("explain metadata-only length aggregation");
            assert!(relational_explain_execution_info(&analyzed).contains("hydrated_rows=0"));
            let counted = database
                .query_sql("EXPLAIN ANALYZE SELECT COUNT(body) AS body_count FROM documents")
                .expect("COUNT only needs overflow nullability metadata");
            assert!(relational_explain_execution_info(&counted).contains("hydrated_rows=0"));
            let error = database
                .query_sql("SELECT body FROM documents WHERE owner = 'owner-large'")
                .expect_err("projecting the large value must still honor the hydration budget");
            assert!(error.to_string().contains("overflow hydration"));
            let predicate_error = database
                .query_sql_with_params(
                    "SELECT SUM(OCTET_LENGTH(body)) AS body_bytes FROM documents WHERE body = $1",
                    &[Value::String("x".repeat(96 * 1024))],
                )
                .expect_err("a value-sensitive predicate must still hydrate its operand");
            assert!(predicate_error.to_string().contains("overflow hydration"));
            let distinct_error = database
                .query_sql("SELECT COUNT(DISTINCT body) AS body_count FROM documents")
                .expect_err("COUNT DISTINCT must hydrate the compared values");
            assert!(distinct_error.to_string().contains("overflow hydration"));
        }
        {
            use std::io::{Read, Seek, SeekFrom, Write};

            let page_bytes = skein_storage::RelationalIndexShadowConfig::default()
                .page_limits
                .max_page_bytes
                .get() as u64;
            let artifact = path.join(skein_storage::relational_index_shadow_artifact_file(
                published_generation,
            ));
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&artifact)
                .expect("open relational index artifact for corruption fixture");
            for page in 0..published_pages {
                file.seek(SeekFrom::Start(page * page_bytes))
                    .expect("seek relational index page");
                let mut byte = [0u8; 1];
                file.read_exact(&mut byte)
                    .expect("read relational index page byte");
                byte[0] ^= 0xff;
                file.seek(SeekFrom::Start(page * page_bytes))
                    .expect("rewind relational index page");
                file.write_all(&byte)
                    .expect("corrupt relational index page byte");
            }
            file.sync_all().expect("sync corruption fixture");

            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                DatabaseConfig {
                    relational_index_mode: skein_storage::RelationalIndexMode::DemandPaged,
                    ..DatabaseConfig::default()
                },
            )
            .expect("open database with lazily validated corrupt index pages");
            let error = database
                .query_sql("SELECT id FROM documents WHERE owner = 'owner-1'")
                .expect_err("selected corrupt relational index must fail closed");
            assert!(error.to_string().contains("storage integrity"));
        }
        std::fs::remove_dir_all(path).expect("remove demand-index SQL fixture");
    }

    #[test]
    fn schema_change_publishes_canonical_row_checkpoint_before_sql_returns() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-relational-schema-row-checkpoint-{}-{nonce}",
            std::process::id()
        ));
        let mut database = Database::open_with_durability(&path, DurabilityPolicy::default())
            .expect("open row-reader fixture");
        database
            .query_sql("CREATE TABLE documents (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
            .expect("create documents table");
        database
            .query_sql("INSERT INTO documents (id, body) VALUES ('doc-1', 'body-1')")
            .expect("insert document");
        database.checkpoint().expect("publish canonical row root");
        database
            .query_sql("ALTER TABLE documents ADD COLUMN kind TEXT NOT NULL DEFAULT 'text'")
            .expect("commit schema change");

        let rows = database
            .query_sql("EXPLAIN ANALYZE SELECT id FROM documents")
            .expect("read through the schema-bound canonical row checkpoint");
        let scan_info = relational_explain_operator_info(&rows, "TableFullScanExec");
        assert!(scan_info.contains("row_runtime_path=snapshot_rows"));
        assert!(!database.storage_handle_poisoned());

        drop(database);
        std::fs::remove_dir_all(path).expect("remove schema row-checkpoint fixture");
    }

    #[test]
    fn blocking_row_reads_budget_the_scan_and_final_locator_projection() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-relational-blocking-row-budget-{}-{nonce}",
            std::process::id()
        ));
        let mut database = Database::open_with_durability_and_config(
            &path,
            DurabilityPolicy::default(),
            DatabaseConfig {
                max_read_result_rows: Some(2),
                ..DatabaseConfig::default()
            },
        )
        .expect("open blocking row-budget fixture");
        database
            .query_sql("CREATE TABLE documents (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
            .expect("create documents table");
        database
            .query_sql("INSERT INTO documents (id, body) VALUES ('doc-1', 'body-1')")
            .expect("insert first document");
        database
            .query_sql("INSERT INTO documents (id, body) VALUES ('doc-2', 'body-2')")
            .expect("insert second document");
        database
            .checkpoint()
            .expect("publish rows before the bounded blocking read");

        let output = database
            .query_sql("SELECT body FROM documents ORDER BY id DESC LIMIT 1")
            .expect("budget both the two-row scan and final locator read");
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["body"], Value::String("body-2".to_string()));

        drop(database);
        std::fs::remove_dir_all(path).expect("remove blocking row-budget fixture");
    }

    #[test]
    fn authoritative_sql_keeps_index_path_when_cache_rejects_pages() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-relational-sql-authoritative-index-{}-{nonce}",
            std::process::id()
        ));
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                DatabaseConfig {
                    relational_index_mode: skein_storage::RelationalIndexMode::Shadow,
                    ..DatabaseConfig::default()
                },
            )
            .expect("open authoritative SQL bootstrap");
            database
                .query_sql(
                    "CREATE TABLE documents (id TEXT PRIMARY KEY, owner TEXT NOT NULL, body TEXT NOT NULL)",
                )
                .expect("create authoritative SQL table");
            database
                .query_sql("CREATE INDEX documents_owner_idx ON documents (owner)")
                .expect("create authoritative SQL index");
            database
                .query_sql(
                    "INSERT INTO documents (id, owner, body) VALUES ('doc-1', 'owner-1', 'body-1')",
                )
                .expect("insert authoritative SQL source");
            database
                .checkpoint()
                .expect("publish authoritative SQL generation");
        }
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                DatabaseConfig {
                    relational_index_mode: skein_storage::RelationalIndexMode::Authoritative,
                    ..DatabaseConfig::default()
                },
            )
            .expect("open authoritative SQL reader");
            let output = database
                .query_sql("EXPLAIN ANALYZE SELECT body FROM documents WHERE owner = 'owner-1'")
                .expect("read authoritative SQL index");
            let info = relational_explain_operator_info(&output, "IndexRangeScanExec");
            assert!(info.contains("runtime_path=authoritative"));
            assert!(info.contains("authoritative=1"));
            assert!(info.contains("canonical_fallback=0"));
            assert!(info.contains("covering=false"));
        }
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                DatabaseConfig {
                    relational_index_mode: skein_storage::RelationalIndexMode::Authoritative,
                    segment_cache_capacity_bytes: 1,
                    ..DatabaseConfig::default()
                },
            )
            .expect("open authoritative SQL reader with an undersized cache");
            let output = database
                .query_sql("EXPLAIN ANALYZE SELECT body FROM documents WHERE owner = 'owner-1'")
                .expect("cache admission rejection should retain bounded positioned reads");
            let info = relational_explain_operator_info(&output, "IndexRangeScanExec");
            assert!(info.contains("runtime_path=authoritative"));
            assert!(info.contains("authoritative=1"));
            assert!(info.contains("canonical_fallback=0"));
            assert!(info.contains("covering=false"));
            assert!(info.contains("cache_admission_rejections="));
            assert!(!info.contains("cache_admission_rejections=0"));
        }
        std::fs::remove_dir_all(path).expect("remove authoritative SQL fixture");
    }

    #[test]
    fn authoritative_transaction_index_overlay_preserves_multi_statement_ryw() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-authoritative-transaction-index-{}-{nonce}",
            std::process::id()
        ));
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                DatabaseConfig {
                    relational_index_mode: skein_storage::RelationalIndexMode::Shadow,
                    ..DatabaseConfig::default()
                },
            )
            .expect("open authoritative transaction bootstrap");
            database
                .query_sql(
                    "CREATE TABLE parents (id TEXT PRIMARY KEY, code TEXT UNIQUE NOT NULL, body TEXT NOT NULL)",
                )
                .expect("create parent table");
            database
                .query_sql("CREATE INDEX parents_body_order ON parents (body, code, id)")
                .expect("create parent body ordering index");
            database
                .query_sql(
                    "CREATE TABLE children (id TEXT PRIMARY KEY, parent_code TEXT NOT NULL REFERENCES parents(code))",
                )
                .expect("create child table");
            database
                .query_sql(
                    "INSERT INTO parents (id, code, body) VALUES ('base', 'base-code', 'base-body')",
                )
                .expect("insert base parent");
            database
                .checkpoint()
                .expect("publish authoritative transaction generation");
        }
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                DatabaseConfig {
                    relational_index_mode: skein_storage::RelationalIndexMode::Authoritative,
                    ..DatabaseConfig::default()
                },
            )
            .expect("open authoritative transaction database");
            let mut transaction = database.begin_transaction();
            transaction
                .query_sql(
                    "INSERT INTO parents (id, code, body) VALUES ('parent-1', 'code-1', 'body-1')",
                )
                .expect("insert a transaction-local unique key");
            transaction
                .query_sql(
                    "INSERT INTO parents (id, code, body) VALUES \
                     ('page-2', 'code-2', 'body-1'), \
                     ('page-3', 'code-3', 'body-1')",
                )
                .expect("insert transaction-local keyset entries");

            let explain = transaction
                .query_sql("EXPLAIN ANALYZE SELECT id FROM parents WHERE code = 'code-1'")
                .expect("read the transaction-local unique key");
            let info = relational_explain_operator_info(&explain, "IndexRangeScanExec");
            assert!(info.contains("runtime_path=transaction_workspace"));
            assert!(info.contains("transaction_workspace=1"));
            assert!(info.contains("canonical_fallback=0"));

            let ordered = transaction
                .query_sql(
                    "EXPLAIN ANALYZE SELECT id FROM parents WHERE body = 'body-1' ORDER BY code ASC, id ASC LIMIT 1",
                )
                .expect("read ordered transaction-local index entry");
            let ordered_info = relational_explain_operator_info(&ordered, "IndexRangeScanExec");
            assert!(ordered_info.contains("runtime_path=transaction_workspace"));
            assert!(ordered_info.contains("order_prefix=2"));
            assert!(ordered.rows.iter().all(|row| {
                !matches!(row.get("id"), Some(Value::String(id)) if id.contains("TopNExec"))
            }));

            let keyset = transaction
                .query_sql(
                    "EXPLAIN ANALYZE SELECT id FROM parents \
                     WHERE body = 'body-1' \
                       AND (code > 'code-1' OR (code = 'code-1' AND id > 'parent-1')) \
                     ORDER BY code ASC, id ASC LIMIT 2",
                )
                .expect("read transaction-local keyset page");
            let keyset_info = relational_explain_operator_info(&keyset, "IndexRangeScanExec");
            assert!(keyset_info.contains("runtime_path=transaction_workspace"));
            assert!(keyset_info.contains("exclusive_seek_lookups=1"));
            assert!(keyset_info.contains("early_stop_lookups=1"));

            transaction
                .query_sql("UPDATE parents SET code = 'base-code-2' WHERE id = 'base'")
                .expect("move a base posting into the transaction overlay");
            assert!(transaction
                .query_sql("SELECT id FROM parents WHERE code = 'base-code'")
                .expect("suppress a deleted base posting")
                .rows
                .is_empty());
            transaction
                .query_sql("UPDATE parents SET code = 'base-code' WHERE id = 'base'")
                .expect("restore the base posting through a later statement");
            assert_eq!(
                transaction
                    .query_sql("SELECT id FROM parents WHERE code = 'base-code'")
                    .expect("deduplicate a restored base posting")
                    .rows
                    .len(),
                1
            );

            transaction
                .query_sql("INSERT INTO children (id, parent_code) VALUES ('child-1', 'code-1')")
                .expect("reference a parent inserted by an earlier statement");
            let duplicate = transaction
                .query_sql(
                    "INSERT INTO parents (id, code, body) VALUES ('parent-2', 'code-1', 'duplicate')",
                )
                .expect_err("reject a duplicate transaction-local unique key");
            assert!(duplicate.to_string().contains("duplicate key"));

            transaction
                .query_sql(
                    "INSERT INTO parents (id, code, body) VALUES ('unused', 'code-1', 'updated') ON CONFLICT (code) DO UPDATE SET body = EXCLUDED.body",
                )
                .expect("upsert through a transaction-local conflict target");
            let updated = transaction
                .query_sql("SELECT body FROM parents WHERE code = 'code-1'")
                .expect("read the transaction-local upsert");
            assert_eq!(updated.rows.len(), 1);
            assert_eq!(
                updated.rows[0]["body"],
                Value::String("updated".to_string())
            );

            let referenced = transaction
                .query_sql("DELETE FROM parents WHERE id = 'parent-1'")
                .expect_err("retain a parent referenced by a transaction-local child");
            assert!(referenced.to_string().contains("prevents removing"));
            assert_eq!(
                transaction
                    .query_sql("SELECT id FROM parents WHERE code = 'code-1'")
                    .expect("failed statement leaves prior workspace intact")
                    .rows
                    .len(),
                1
            );
            transaction
                .commit()
                .expect("commit the multi-statement group");

            assert_eq!(
                database
                    .query_sql("SELECT id FROM children WHERE parent_code = 'code-1'")
                    .expect("read the committed child")
                    .rows
                    .len(),
                1
            );
            assert_eq!(
                database
                    .query_sql("SELECT body FROM parents WHERE code = 'code-1'")
                    .expect("read the committed parent")
                    .rows[0]["body"],
                Value::String("updated".to_string())
            );
        }
        std::fs::remove_dir_all(path).expect("remove authoritative transaction fixture");
    }

    #[test]
    fn pinned_read_transaction_sql_propagates_cancellation_without_poisoning_service() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
            .expect("create cancellation table");
        database
            .query_sql("INSERT INTO messages (id, body) VALUES (1, 'ready')")
            .expect("insert cancellation row");

        let read = database.begin_read_transaction();
        let cancellation = skein_core::RuntimeCancellationToken::new();
        cancellation.cancel();
        let context = skein_core::RuntimeTaskContext::without_deadline(cancellation);
        let error = read
            .query_sql_with_params_options_context(
                "SELECT body FROM messages WHERE id = $1",
                &[Value::Int(1)],
                crate::QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(4096),
                },
                &context,
            )
            .expect_err("cancelled SQL read must stop");
        assert!(error.to_string().contains("cancelled"));

        let output = read
            .query_sql_with_params_options(
                "SELECT body FROM messages WHERE id = $1",
                &[Value::Int(1)],
                crate::QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(4096),
                },
            )
            .expect("cancellation must not poison the pinned reader");
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["body"], Value::String("ready".to_string()));
    }

    #[test]
    fn profiled_relational_read_returns_rows_and_accounting_from_one_execution() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
            .expect("create profiled-read table");
        database
            .query_sql("INSERT INTO messages (id, body) VALUES (1, 'ready')")
            .expect("insert profiled-read row");

        let read = database.begin_read_transaction();
        let profiled = read
            .query_sql_with_params_options_profiled(
                "SELECT body FROM messages WHERE id = $1",
                &[Value::Int(1)],
                crate::QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(4096),
                },
            )
            .expect("profile one relational read");

        assert_eq!(profiled.output.rows.len(), 1);
        assert_eq!(
            profiled.output.rows[0]["body"],
            Value::String("ready".to_string())
        );
        assert!(profiled.profile.intermediate_rows > 0);
        assert_eq!(
            profiled.profile.join_planning.strategy,
            RelationalJoinPlanningStrategy::SyntaxOrder
        );
        assert_eq!(
            profiled.profile.join_planning.status,
            RelationalJoinPlanningStatus::NotEligible
        );
        assert_eq!(
            profiled.profile.join_planning.reason,
            RelationalJoinPlanningReason::NoJoin
        );
        assert_eq!(profiled.profile.join_planning.selected_order, ["messages"]);
        assert!(profiled.profile.join_planning.cost.is_none());
        assert_eq!(profiled.profile.operator_cardinality_profiles.len(), 1);
        let scan = &profiled.profile.operator_cardinality_profiles[0];
        assert_eq!(scan.operator_id.get(), 1);
        assert_eq!(scan.operator, RelationalOperatorKind::TablePointGet);
        assert_eq!(scan.table, "messages");
        assert_eq!(scan.estimated_rows, 1);
        assert_eq!(scan.actual_rows, Some(1));
        assert!(scan.fully_consumed);
        assert_eq!(profiled.profile.row_read.runtime_path, "canonical_memory");
        assert_eq!(profiled.profile.row_read.rows_visited, 1);
        assert!(profiled.profile.stage_timings.parse_nanos > 0);
        assert!(profiled.profile.stage_timings.bind_nanos > 0);
        assert!(profiled.profile.stage_timings.plan_nanos > 0);
        assert!(profiled.profile.stage_timings.execute_nanos > 0);

        let cache_after_miss = read.relational_plan_template_cache_stats();
        let cached = read
            .query_sql_with_params_options_profiled(
                "SELECT body FROM messages WHERE id = $1",
                &[Value::Int(1)],
                crate::QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(4096),
                },
            )
            .expect("profile a relational template-cache hit");
        assert_eq!(cached.profile.stage_timings.parse_nanos, 0);
        assert!(cached.profile.stage_timings.bind_nanos > 0);
        assert!(cached.profile.stage_timings.plan_nanos > 0);
        assert!(cached.profile.stage_timings.execute_nanos > 0);
        let cache_after_hit = read.relational_plan_template_cache_stats();
        assert_eq!(cache_after_hit.hits, cache_after_miss.hits + 1);
    }

    #[test]
    fn relational_operator_cardinality_profiles_track_join_boundaries_and_early_stop() {
        const SELECT: &str = "SELECT p.id AS parent_id, c.id AS child_id \
            FROM profile_parents AS p \
            INNER JOIN profile_children AS c ON c.parent_id = p.id";

        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE profile_parents (id BIGINT PRIMARY KEY)")
            .expect("create profile parent table");
        database
            .query_sql(
                "CREATE TABLE profile_children (\
                   id BIGINT PRIMARY KEY, \
                   parent_id BIGINT NOT NULL REFERENCES profile_parents(id)\
                 )",
            )
            .expect("create profile child table");
        database
            .query_sql("INSERT INTO profile_parents (id) VALUES (1), (2)")
            .expect("insert profile parents");
        database
            .query_sql(
                "INSERT INTO profile_children (id, parent_id) VALUES (11, 1), (12, 1), (21, 2)",
            )
            .expect("insert profile children");

        let read = database.begin_read_transaction();
        let full = read
            .query_sql_with_params_options_profiled(
                SELECT,
                &[],
                crate::QueryStreamOptions::default(),
            )
            .expect("profile fully consumed join");
        assert_eq!(full.output.rows.len(), 3);
        assert_eq!(full.profile.intermediate_rows, 8);
        assert_eq!(full.profile.operator_cardinality_profiles.len(), 2);
        let base = &full.profile.operator_cardinality_profiles[0];
        assert_eq!(base.operator_id.get(), 1);
        assert_eq!(base.operator, RelationalOperatorKind::TableFullScan);
        assert_eq!(base.table, "profile_parents");
        assert_eq!(base.estimated_rows, 2);
        assert_eq!(base.actual_rows, Some(2));
        assert!(base.fully_consumed);
        let join = &full.profile.operator_cardinality_profiles[1];
        assert_eq!(join.operator_id.get(), 2);
        assert_eq!(join.operator, RelationalOperatorKind::HashJoin);
        assert_eq!(join.table, "profile_children");
        assert_eq!(
            join.access_path.kind,
            skein_optimizer::RelationalAccessPathKind::FullScan
        );
        assert_eq!(join.estimated_rows, 6);
        assert_eq!(join.actual_rows, Some(3));
        assert!(join.fully_consumed);
        assert!(full
            .profile
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "RelationalHashJoinBuild"));

        let limited = read
            .query_sql_with_params_options_profiled(
                &format!("{SELECT} LIMIT 1"),
                &[],
                crate::QueryStreamOptions::default(),
            )
            .expect("profile early-stopped join");
        assert_eq!(limited.output.rows.len(), 1);
        assert_eq!(limited.profile.intermediate_rows, 5);
        assert_eq!(
            limited.profile.operator_cardinality_profiles[0].actual_rows,
            Some(1)
        );
        assert_eq!(
            limited.profile.operator_cardinality_profiles[1].actual_rows,
            Some(1)
        );
        assert!(limited
            .profile
            .operator_cardinality_profiles
            .iter()
            .all(|profile| !profile.fully_consumed));

        let plain = read
            .query_sql_with_params(&format!("EXPLAIN {SELECT}"), &[])
            .expect("explain cardinality-profiled join");
        let analyzed = read
            .query_sql_with_params(&format!("EXPLAIN ANALYZE {SELECT}"), &[])
            .expect("analyze cardinality-profiled join");
        let analyzed_ids = analyzed
            .rows
            .iter()
            .filter_map(|row| match row.get("id") {
                Some(Value::String(id)) => Some(id.clone()),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(analyzed_ids.len(), analyzed.rows.len());
        for (table, expected_id, estimated_rows, actual_rows) in
            [("profile_parents", 1, 2, 2), ("profile_children", 2, 6, 3)]
        {
            let plain_row = relational_explain_access_row(&plain, table);
            let analyzed_row = relational_explain_access_row(&analyzed, table);
            assert_eq!(plain_row["id"], analyzed_row["id"]);
            assert!(matches!(
                analyzed_row.get("id"),
                Some(Value::String(id)) if id.ends_with(&format!("_{expected_id}"))
            ));
            assert_eq!(analyzed_row["estRows"], Value::Int(estimated_rows));
            assert_eq!(analyzed_row["actRows"], Value::Int(actual_rows));
            assert!(matches!(
                analyzed_row.get("execution info"),
                Some(Value::String(info))
                    if info.contains(&format!("operator_id={expected_id}"))
                        && info.contains("fully_consumed=true")
            ));
        }
    }

    #[test]
    fn relational_join_candidate_work_is_bounded_before_rejected_predicates() {
        let store = RelationalStore::default();
        for sql in [
            "CREATE TABLE candidate_outer (id BIGINT PRIMARY KEY)",
            "CREATE TABLE candidate_inner (id BIGINT PRIMARY KEY, outer_id BIGINT NOT NULL, accepted BOOLEAN NOT NULL)",
            "CREATE INDEX candidate_inner_outer_idx ON candidate_inner (outer_id)",
            "INSERT INTO candidate_outer (id) VALUES (1)",
            "INSERT INTO candidate_inner (id, outer_id, accepted) VALUES (1, 1, FALSE), (2, 1, FALSE), (3, 1, FALSE)",
        ] {
            commit_sql(&store, sql, &[]);
        }
        let snapshot = store.snapshot().expect("candidate work snapshot");
        let mut limits = query_limits(8, 4096);
        limits.max_intermediate_rows = 32;
        limits.max_candidate_work = 3;

        let error = execute_relational_query_sql_with_runtime(
            "SELECT o.id FROM candidate_outer AS o INNER JOIN candidate_inner AS i ON i.outer_id = o.id AND i.accepted = TRUE",
            &[],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            limits,
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect_err("rejected join candidates must consume the work budget");

        assert!(error.to_string().contains("max_candidate_work 3"));
    }

    #[test]
    fn relational_join_probe_fanout_tracks_checkpoint_epoch_and_wal_delta() {
        const PREFIX_ONE_SELECT: &str = "SELECT e.id \
            FROM probe_keys AS p \
            INNER JOIN events AS e ON e.tenant = p.tenant";
        const PREFIX_TWO_SELECT: &str = "SELECT e.id \
            FROM probe_keys AS p \
            INNER JOIN events AS e \
              ON e.tenant = p.tenant AND e.category = p.category";

        fn estimated_join_rows(database: &Database, sql: &str) -> usize {
            let read = database.begin_read_transaction();
            let profiled = read
                .query_sql_with_params_options_profiled(
                    sql,
                    &[],
                    crate::QueryStreamOptions::default(),
                )
                .expect("profile composite index join");
            assert_eq!(profiled.profile.operator_cardinality_profiles.len(), 2);
            assert_eq!(
                profiled.profile.operator_cardinality_profiles[1].operator,
                RelationalOperatorKind::BatchedIndexNestedLoopJoin
            );
            assert_eq!(
                profiled.profile.operator_cardinality_profiles[1].table,
                "events"
            );
            profiled.profile.operator_cardinality_profiles[1].estimated_rows
        }

        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-relational-probe-fanout-{}-{nonce}",
            std::process::id()
        ));
        let config = DatabaseConfig {
            relational_index_mode: skein_storage::RelationalIndexMode::Shadow,
            ..DatabaseConfig::default()
        };
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                config.clone(),
            )
            .expect("open probe-fanout database");
            database
                .query_sql(
                    "CREATE TABLE probe_keys (\
                       id TEXT PRIMARY KEY, \
                       tenant TEXT NOT NULL, \
                       category TEXT NOT NULL\
                     )",
                )
                .expect("create probe keys");
            database
                .query_sql(
                    "CREATE TABLE events (\
                       id TEXT PRIMARY KEY, \
                       tenant TEXT NOT NULL, \
                       category TEXT\
                     )",
                )
                .expect("create events");
            database
                .query_sql("CREATE INDEX events_tenant_category_idx ON events (tenant, category)")
                .expect("create composite event index");
            database
                .query_sql(
                    "INSERT INTO probe_keys (id, tenant, category) \
                     VALUES ('probe', 'A', 'x')",
                )
                .expect("insert probe key");
            database
                .query_sql(
                    "INSERT INTO events (id, tenant, category) VALUES \
                       ('row-1', 'A', 'x'), \
                       ('row-2', 'A', 'x'), \
                       ('row-3', 'A', 'x'), \
                       ('row-4', 'A', 'x'), \
                       ('row-5', 'A', 'y'), \
                       ('row-6', 'A', NULL), \
                       ('row-7', 'A', NULL), \
                       ('row-8', 'B', 'x')",
                )
                .expect("insert skewed events");
            assert_eq!(estimated_join_rows(&database, PREFIX_ONE_SELECT), 8);
            assert_eq!(estimated_join_rows(&database, PREFIX_TWO_SELECT), 8);
            database
                .checkpoint()
                .expect("publish fresh index statistics");

            assert_eq!(estimated_join_rows(&database, PREFIX_ONE_SELECT), 4);
            assert_eq!(estimated_join_rows(&database, PREFIX_TWO_SELECT), 2);

            database
                .query_sql(
                    "INSERT INTO events (id, tenant, category) \
                     VALUES ('row-9', 'A', 'x')",
                )
                .expect("append WAL delta");
            assert_eq!(estimated_join_rows(&database, PREFIX_ONE_SELECT), 9);
            assert_eq!(estimated_join_rows(&database, PREFIX_TWO_SELECT), 9);
        }

        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                config.clone(),
            )
            .expect("reopen with recovered WAL delta");
            assert_eq!(estimated_join_rows(&database, PREFIX_ONE_SELECT), 9);
            assert_eq!(estimated_join_rows(&database, PREFIX_TWO_SELECT), 9);

            database
                .checkpoint()
                .expect("refresh index statistics after WAL recovery");
            assert_eq!(estimated_join_rows(&database, PREFIX_ONE_SELECT), 5);
            assert_eq!(estimated_join_rows(&database, PREFIX_TWO_SELECT), 3);
        }

        {
            let database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                config,
            )
            .expect("reopen fresh index statistics");
            assert_eq!(estimated_join_rows(&database, PREFIX_ONE_SELECT), 5);
            assert_eq!(estimated_join_rows(&database, PREFIX_TWO_SELECT), 3);
        }

        std::fs::remove_dir_all(path).expect("remove probe-fanout fixture");
    }

    #[test]
    fn authoritative_batched_index_join_reads_right_rows_once_per_checkpoint_page() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-relational-batched-index-row-pages-{}-{nonce}",
            std::process::id()
        ));
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                DatabaseConfig {
                    relational_index_mode: skein_storage::RelationalIndexMode::Shadow,
                    ..DatabaseConfig::default()
                },
            )
            .expect("open batched-index fixture");
            database
                .query_sql("CREATE TABLE join_keys (id TEXT PRIMARY KEY, owner TEXT NOT NULL)")
                .expect("create join keys");
            database
                .query_sql(
                    "CREATE TABLE join_documents (\
                       id TEXT PRIMARY KEY, \
                       owner TEXT NOT NULL, \
                       body TEXT NOT NULL\
                     )",
                )
                .expect("create join documents");
            database
                .query_sql("CREATE INDEX join_documents_owner_idx ON join_documents (owner)")
                .expect("create join document index");
            database
                .query_sql(
                    "INSERT INTO join_keys (id, owner) VALUES \
                     ('key-1', 'owner-a'), \
                     ('key-2', 'owner-a'), \
                     ('key-3', 'owner-b')",
                )
                .expect("insert join keys");
            database
                .query_sql(
                    "INSERT INTO join_documents (id, owner, body) VALUES \
                     ('doc-1', 'owner-a', 'body-1'), \
                     ('doc-2', 'owner-a', 'body-2'), \
                     ('doc-3', 'owner-b', 'body-3')",
                )
                .expect("insert join documents");
            database
                .checkpoint()
                .expect("publish batched-index checkpoint");
        }
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                DatabaseConfig {
                    relational_index_mode: skein_storage::RelationalIndexMode::Authoritative,
                    ..DatabaseConfig::default()
                },
            )
            .expect("open authoritative batched-index reader");
            let non_covering = database
                .query_sql(
                    "EXPLAIN ANALYZE SELECT k.id AS key_id, d.body AS document_body \
                     FROM join_keys AS k \
                     INNER JOIN join_documents AS d ON d.owner = k.owner",
                )
                .expect("execute non-covering batched index join");
            let non_covering_info =
                relational_explain_operator_info(&non_covering, "BatchedIndexNestedLoopJoinExec");
            assert!(non_covering_info.contains("covering=false"));
            assert!(non_covering_info.contains("row_fetch=true"));
            assert!(non_covering_info.contains("row_logical_pages=2"));
            assert!(non_covering_info.contains("row_rows=6"));

            let covering = database
                .query_sql(
                    "EXPLAIN ANALYZE SELECT k.id AS key_id, d.id AS document_id \
                     FROM join_keys AS k \
                     INNER JOIN join_documents AS d ON d.owner = k.owner",
                )
                .expect("execute batched index join");
            let info =
                relational_explain_operator_info(&covering, "BatchedIndexNestedLoopJoinExec");
            assert!(info.contains("runtime_path=authoritative"));
            assert!(info.contains("lookups=1"));
            assert!(info.contains("covering=true"));
            assert!(info.contains("row_fetch=false"));
            assert!(info.contains("row_runtime_path=snapshot_rows"));
            assert!(info.contains("row_logical_pages=1"));
            assert!(info.contains("row_rows=3"));
            assert!(info.contains("row_index_covered_rows=3"));

            let index_only = database
                .query_sql("EXPLAIN ANALYZE SELECT id FROM join_documents WHERE owner = 'owner-a'")
                .expect("execute fully covering index scan");
            let index_only_info =
                relational_explain_operator_info(&index_only, "IndexRangeScanExec");
            assert!(index_only_info.contains("covering=true"));
            assert!(index_only_info.contains("row_fetch=false"));
            assert!(index_only_info.contains("row_runtime_path=snapshot_rows"));
            assert!(!index_only_info.contains("row_base_generation=none"));
            assert!(index_only_info.contains("row_logical_pages=0"));
            assert!(index_only_info.contains("row_index_covered_rows=2"));
            database
                .query_sql("INSERT INTO join_keys (id, owner) VALUES ('key-4', 'owner-c')")
                .expect("append live join key");
            database
                .query_sql(
                    "INSERT INTO join_documents (id, owner, body) \
                     VALUES ('doc-4', 'owner-c', 'body-4')",
                )
                .expect("append live join document");
            let live = database
                .query_sql(
                    "EXPLAIN ANALYZE SELECT k.id AS key_id, d.id AS document_id \
                     FROM join_keys AS k \
                     INNER JOIN join_documents AS d ON d.owner = k.owner",
                )
                .expect("execute live batched index join");
            let live_info =
                relational_explain_operator_info(&live, "BatchedIndexNestedLoopJoinExec");
            assert!(live_info.contains("lookups=1"));
            assert!(!live_info.contains("live_batches=0"));
        }
        {
            let mut database = Database::open_with_durability_and_config(
                &path,
                DurabilityPolicy::default(),
                DatabaseConfig {
                    relational_index_mode: skein_storage::RelationalIndexMode::Authoritative,
                    ..DatabaseConfig::default()
                },
            )
            .expect("reopen recovered batched-index reader");
            let recovered = database
                .query_sql(
                    "EXPLAIN ANALYZE SELECT k.id AS key_id, d.id AS document_id \
                     FROM join_keys AS k \
                     INNER JOIN join_documents AS d ON d.owner = k.owner",
                )
                .expect("execute recovered batched index join");
            let recovered_info =
                relational_explain_operator_info(&recovered, "BatchedIndexNestedLoopJoinExec");
            assert!(recovered_info.contains("lookups=1"));
            assert!(!recovered_info.contains("delta_generation=none"));
        }
        std::fs::remove_dir_all(path).expect("remove batched-index fixture");
    }

    fn relational_explain_access_row<'a>(
        output: &'a crate::QueryOutput,
        table: &str,
    ) -> crate::executor::QueryRowRef<'a> {
        output
            .rows
            .iter()
            .find(|row| {
                matches!(
                    row.get("access object"),
                    Some(Value::String(access)) if access.contains(&format!("table:{table}"))
                )
            })
            .unwrap_or_else(|| panic!("EXPLAIN output has no access row for {table}"))
    }

    fn relational_explain_operator_info<'a>(
        output: &'a crate::QueryOutput,
        operator: &str,
    ) -> &'a str {
        output
            .rows
            .iter()
            .find_map(|row| match (row.get("id"), row.get("operator info")) {
                (Some(Value::String(id)), Some(Value::String(info))) if id.contains(operator) => {
                    Some(info.as_str())
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("EXPLAIN output has no {operator} row: {:?}", output.rows))
    }

    fn relational_explain_execution_info(output: &crate::QueryOutput) -> &str {
        match output
            .rows
            .first()
            .and_then(|row| row.get("execution info"))
        {
            Some(Value::String(info)) => info,
            _ => panic!(
                "EXPLAIN ANALYZE output has no execution info: {:?}",
                output.rows
            ),
        }
    }

    #[test]
    fn relational_plan_template_cache_rebinds_current_parameters_state_schema_and_snapshot() {
        let mut database = Database::new_with_config(DatabaseConfig {
            max_plan_cache_entries: Some(32),
            ..DatabaseConfig::default()
        });
        database
            .query_sql(
                "CREATE TABLE template_rows (\
                 id BIGINT PRIMARY KEY, left_key TEXT NOT NULL, right_key TEXT NOT NULL)",
            )
            .expect("create template rows table");
        database
            .query_sql("CREATE INDEX idx_template_left ON template_rows (left_key)")
            .expect("create left template index");
        database
            .query_sql("CREATE INDEX idx_template_right ON template_rows (right_key)")
            .expect("create right template index");
        database
            .query_sql(
                "INSERT INTO template_rows (id, left_key, right_key) VALUES \
                 (1, 'hot', 'rare'), \
                 (2, 'hot', 'other-1'), \
                 (3, 'hot', 'other-2'), \
                 (4, 'rare', 'hot'), \
                 (5, 'other-1', 'hot'), \
                 (6, 'other-2', 'hot')",
            )
            .expect("insert parameter-sensitive template rows");

        const EXPLAIN: &str = "EXPLAIN SELECT id FROM template_rows \
                               WHERE left_key = $1 AND right_key = $2";
        let graph_cache_before = database.plan_cache_stats();
        let before = database.relational_plan_template_cache_stats();
        assert_eq!(before.entries, 0, "DDL and DML must bypass the query cache");
        let right_selective = database
            .query_sql_with_params(EXPLAIN, &[text("hot"), text("rare")])
            .expect("plan with a selective right index");
        assert_eq!(
            relational_explain_access_row(&right_selective, "template_rows").get("access object"),
            Some(&Value::String(
                "table:template_rows, index:idx_template_right".to_string()
            ))
        );
        let after_miss = database.relational_plan_template_cache_stats();
        assert_eq!(after_miss.misses, before.misses + 1);
        assert_eq!(after_miss.admissions, before.admissions + 1);
        assert_eq!(after_miss.entries, before.entries + 1);

        let left_selective = database
            .query_sql_with_params(EXPLAIN, &[text("rare"), text("hot")])
            .expect("rebind the cached template before access planning");
        assert_eq!(
            relational_explain_access_row(&left_selective, "template_rows").get("access object"),
            Some(&Value::String(
                "table:template_rows, index:idx_template_left".to_string()
            ))
        );
        let after_parameter_hit = database.relational_plan_template_cache_stats();
        assert_eq!(after_parameter_hit.hits, after_miss.hits + 1);
        assert_eq!(after_parameter_hit.misses, after_miss.misses);

        database
            .query_sql(
                "INSERT INTO template_rows (id, left_key, right_key) VALUES \
                 (7, 'other-3', 'rare'), \
                 (8, 'other-4', 'rare'), \
                 (9, 'other-5', 'rare'), \
                 (10, 'other-6', 'rare')",
            )
            .expect("change the current index cardinalities");
        let replanned = database
            .query_sql_with_params(EXPLAIN, &[text("hot"), text("rare")])
            .expect("replan the cached template against current state");
        assert_eq!(
            relational_explain_access_row(&replanned, "template_rows").get("access object"),
            Some(&Value::String(
                "table:template_rows, index:idx_template_left".to_string()
            ))
        );

        const WILDCARD: &str = "SELECT * FROM template_rows WHERE id = 1";
        let before_schema_change = database
            .query_sql(WILDCARD)
            .expect("execute wildcard template before schema change");
        assert!(!before_schema_change.rows[0].contains_key("kind"));
        database
            .query_sql("ALTER TABLE template_rows ADD COLUMN kind TEXT NOT NULL DEFAULT 'unknown'")
            .expect("extend template rows schema");
        let after_schema_change = database
            .query_sql(WILDCARD)
            .expect("rebind wildcard template after schema change");
        assert_eq!(after_schema_change.rows[0]["kind"], text("unknown"));

        const SNAPSHOT_SELECT: &str = "SELECT id FROM template_rows ORDER BY id";
        let pinned = database.begin_read_transaction();
        let pinned_before = pinned
            .query_sql_with_params_options(
                SNAPSHOT_SELECT,
                &[],
                crate::QueryStreamOptions {
                    max_rows: Some(32),
                    max_payload_bytes: Some(64 * 1024),
                },
            )
            .expect("read cached template from pinned snapshot");
        assert_eq!(pinned_before.rows.len(), 10);
        database
            .query_sql(
                "INSERT INTO template_rows (id, left_key, right_key) \
                 VALUES (11, 'new', 'new')",
            )
            .expect("advance current relational state");
        let current = database
            .query_sql(SNAPSHOT_SELECT)
            .expect("read current state with shared template");
        assert_eq!(current.rows.len(), 11);
        let pinned_after = pinned
            .query_sql_with_params_options(
                SNAPSHOT_SELECT,
                &[],
                crate::QueryStreamOptions {
                    max_rows: Some(32),
                    max_payload_bytes: Some(64 * 1024),
                },
            )
            .expect("reuse shared template without changing the pinned snapshot");
        assert_eq!(pinned_after.rows.len(), 10);

        assert_eq!(database.plan_cache_stats(), graph_cache_before);
    }

    #[test]
    fn database_sql_options_enforce_per_statement_payload_admission() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE documents (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        database
            .query_sql("INSERT INTO documents (id, body) VALUES ('doc-1', 'payload')")
            .unwrap();

        let error = database
            .query_sql_with_params_options(
                "SELECT body FROM documents WHERE id = $1",
                &[Value::String("doc-1".to_string())],
                crate::QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(1),
                },
            )
            .expect_err("per-statement payload budget must be enforced");

        assert!(error
            .to_string()
            .contains("relational SQL output exceeds max_output_payload_bytes 1"));
    }

    #[test]
    fn alter_table_add_column_materializes_defaults_and_rejects_unsafe_not_null() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE documents (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        database
            .query_sql("INSERT INTO documents (id, body) VALUES ('doc-1', 'body')")
            .unwrap();
        database
            .query_sql("ALTER TABLE documents ADD COLUMN kind TEXT NOT NULL DEFAULT 'text'")
            .unwrap();
        let row = database
            .query_sql("SELECT id, kind FROM documents")
            .unwrap();
        assert_eq!(row.rows[0]["kind"], Value::String("text".to_string()));

        let error = database
            .query_sql("ALTER TABLE documents ADD COLUMN required TEXT NOT NULL")
            .unwrap_err();
        assert!(error.to_string().contains("without a default"));
        database
            .query_sql("ALTER TABLE documents ADD COLUMN required TEXT")
            .unwrap();
        assert_eq!(
            database
                .query_sql("SELECT required FROM documents")
                .unwrap()
                .rows[0]["required"],
            Value::Null
        );
    }

    #[test]
    fn postgres_catalog_views_expose_relational_schema_and_indexes() {
        let mut database = Database::new();
        database
            .query_sql(
                "CREATE TABLE public.documents (\
                 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, external_id TEXT NOT NULL, \
                 body TEXT NOT NULL DEFAULT 'draft', score DOUBLE PRECISION, \
                 UNIQUE (tenant_id, external_id))",
            )
            .expect("create relational table");
        database
            .query_sql(
                "CREATE INDEX idx_documents_score ON public.documents (tenant_id, score, id)",
            )
            .expect("create relational index");

        let columns = database
            .query_sql_with_params(
                "SELECT column_name, ordinal_position, column_default, is_nullable, \
                 data_type, udt_name FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name = $1 \
                 ORDER BY ordinal_position",
                &[Value::String("documents".to_string())],
            )
            .expect("query information schema columns");
        assert_eq!(columns.rows.len(), 5);
        assert_eq!(
            columns.rows[0],
            BTreeMap::from([
                ("column_default".to_string(), Value::Null),
                ("column_name".to_string(), Value::String("id".to_string()),),
                ("data_type".to_string(), Value::String("text".to_string()),),
                ("is_nullable".to_string(), Value::String("NO".to_string()),),
                ("ordinal_position".to_string(), Value::Int(1)),
                ("udt_name".to_string(), Value::String("text".to_string()),),
            ])
        );
        assert_eq!(
            columns.rows[3].get("column_default"),
            Some(&Value::String("'draft'::text".to_string()))
        );
        let score_column = database
            .query_sql(
                "SELECT * FROM information_schema.columns \
                 WHERE table_name = 'documents' AND column_name = 'score'",
            )
            .expect("query complete information schema column");
        assert_eq!(score_column.rows[0].len(), 44);
        assert_eq!(
            score_column.rows[0].get("numeric_precision"),
            Some(&Value::Int(53))
        );
        assert_eq!(
            score_column.rows[0].get("numeric_precision_radix"),
            Some(&Value::Int(2))
        );

        let tables = database
            .query_sql(
                "SELECT schemaname, tablename, tableowner, hasindexes, rowsecurity \
                 FROM pg_catalog.pg_tables WHERE tablename = 'documents'",
            )
            .expect("query pg tables");
        assert_eq!(
            tables.rows,
            vec![BTreeMap::from([
                ("hasindexes".to_string(), Value::Bool(true)),
                ("rowsecurity".to_string(), Value::Bool(false)),
                (
                    "schemaname".to_string(),
                    Value::String("public".to_string()),
                ),
                (
                    "tablename".to_string(),
                    Value::String("documents".to_string()),
                ),
                ("tableowner".to_string(), Value::String("skein".to_string()),),
            ])]
        );

        let indexes = database
            .query_sql(
                "SELECT indexname, indexdef FROM pg_indexes \
                 WHERE tablename = 'documents' ORDER BY indexname",
            )
            .expect("query pg indexes");
        assert_eq!(indexes.rows.len(), 3);
        assert_eq!(
            indexes.rows[0].get("indexname"),
            Some(&Value::String("documents_pkey".to_string()))
        );
        assert_eq!(
            indexes.rows[2].get("indexdef"),
            Some(&Value::String(
                "CREATE INDEX \"idx_documents_score\" ON public.\"documents\" USING btree \
                 (\"tenant_id\", \"score\", \"id\")"
                    .to_string()
            ))
        );

        let read_transaction = database.begin_read_transaction();
        let snapshot_tables = read_transaction
            .query_sql(
                "SELECT table_name, table_type FROM information_schema.tables \
                 WHERE table_name = 'documents'",
            )
            .expect("query pinned information schema snapshot");
        assert_eq!(
            snapshot_tables.rows,
            vec![BTreeMap::from([
                (
                    "table_name".to_string(),
                    Value::String("documents".to_string()),
                ),
                (
                    "table_type".to_string(),
                    Value::String("BASE TABLE".to_string()),
                ),
            ])]
        );
        let full_table = read_transaction
            .query_sql("SELECT * FROM information_schema.tables WHERE table_name = 'documents'")
            .expect("query complete information schema table");
        assert_eq!(full_table.rows[0].len(), 12);
        assert_eq!(
            full_table.rows[0].get("is_insertable_into"),
            Some(&Value::String("YES".to_string()))
        );

        let error = database
            .query_sql_bounded("SELECT * FROM information_schema.tables", Some(0))
            .expect_err("catalog row budget must fail closed");
        assert!(error
            .to_string()
            .contains("exceeding max_read_result_rows 0"));
    }

    #[test]
    fn postgres_catalog_reads_observe_transaction_private_ddl() {
        let mut database = Database::new();
        {
            let mut transaction = database.begin_transaction();
            transaction
                .query_sql("CREATE TABLE public.pending (id BIGINT PRIMARY KEY)")
                .expect("stage relational table");
            let output = transaction
                .query_sql(
                    "SELECT table_name FROM information_schema.tables \
                     WHERE table_schema = 'public' AND table_name = 'pending'",
                )
                .expect("query transaction-private catalog");
            assert_eq!(
                output.rows,
                vec![BTreeMap::from([(
                    "table_name".to_string(),
                    Value::String("pending".to_string()),
                )])]
            );
            transaction.rollback();
        }

        let output = database
            .query_sql(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_name = 'pending'",
            )
            .expect("query committed catalog");
        assert!(output.rows.is_empty());
    }

    #[test]
    fn postgres_compatibility_catalogs_are_read_only() {
        let mut database = Database::new();
        let error = database
            .query_sql("CREATE TABLE pg_catalog.shadow (id BIGINT PRIMARY KEY)")
            .expect_err("catalog mutation must fail");
        assert!(error.to_string().contains("pg_catalog is read-only"));
    }

    #[test]
    fn database_transaction_commits_cypher_and_sql_in_one_epoch() {
        let mut database = Database::new();
        {
            let mut transaction = database.begin_transaction();
            transaction
                .query("CREATE (:Marker {id: 'graph-1'})")
                .expect("stage graph mutation");
            transaction
                .query_sql("CREATE TABLE public.messages (id TEXT PRIMARY KEY)")
                .expect("stage relational schema");
            transaction
                .query_sql_with_params(
                    "INSERT INTO public.messages (id) VALUES ($1)",
                    &[Value::String("message-1".to_string())],
                )
                .expect("stage relational row");
            let staged = transaction
                .query_sql("SELECT id FROM public.messages")
                .expect("read staged relational row");
            assert_eq!(staged.rows.len(), 1);
            let staged_graph = transaction
                .query("MATCH (m:Marker) WHERE m.id = 'graph-1' RETURN m.id AS id")
                .expect("read staged graph row");
            assert_eq!(staged_graph.rows.len(), 1);
            let staged_plan = transaction
                .query_sql("EXPLAIN SELECT id FROM public.messages")
                .expect("plan against staged relational state");
            assert!(!staged_plan.rows.is_empty());
            transaction.commit().expect("commit mixed transaction");
        }

        assert_eq!(database.commit_epoch(), 1);
        assert_eq!(
            database
                .query("MATCH (m:Marker) RETURN m.id AS id")
                .expect("read committed graph row")
                .rows
                .len(),
            1
        );
        assert_eq!(
            database
                .query_sql("SELECT id FROM public.messages")
                .expect("read committed relational row")
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn relational_ddl_requires_exactly_one_primary_key_declaration() {
        let missing = skein_sql::prepare_postgres_sql(
            "CREATE TABLE documents (id TEXT NOT NULL, payload TEXT NOT NULL)",
        )
        .expect("valid PostgreSQL syntax");
        let error = compile_schema_statement(missing.statement)
            .expect_err("Skein relational tables require a primary key");
        assert_eq!(
            error,
            SkeinError::Semantic(
                "relational table documents must declare a primary key".to_string()
            )
        );

        let duplicate = skein_sql::prepare_postgres_sql(
            "CREATE TABLE documents (tenant_id TEXT PRIMARY KEY, id TEXT PRIMARY KEY)",
        )
        .expect("parser preserves duplicate declarations for semantic validation");
        let error = compile_schema_statement(duplicate.statement)
            .expect_err("multiple primary-key declarations must be rejected");
        assert_eq!(
            error,
            SkeinError::Semantic("table declares more than one primary key".to_string())
        );
    }

    #[test]
    fn table_level_composite_primary_key_is_not_nullable() {
        let prepared = skein_sql::prepare_postgres_sql(
            "CREATE TABLE documents (tenant_id TEXT, id TEXT, payload TEXT, \
             PRIMARY KEY (tenant_id, id))",
        )
        .expect("valid composite primary key");
        let writes = compile_schema_statement(prepared.statement).expect("compiled schema");
        let [RelationalWrite::CreateTable(schema)] = writes.as_slice() else {
            panic!("expected one CREATE TABLE write");
        };

        assert_eq!(schema.primary_key, ["tenant_id", "id"]);
        assert!(!schema.columns[0].nullable);
        assert!(!schema.columns[1].nullable);
        assert!(schema.columns[2].nullable);
    }

    #[test]
    fn relational_index_join_aggregate_and_late_hydration_are_bounded() {
        let store = RelationalStore::default();
        for ddl in [
            "CREATE TABLE documents (id TEXT PRIMARY KEY, owner_kind TEXT NOT NULL, owner_id TEXT NOT NULL, UNIQUE (owner_kind, owner_id))",
            "CREATE TABLE messages (id TEXT PRIMARY KEY, document_id TEXT NOT NULL REFERENCES documents(id), stream_id TEXT NOT NULL, order_index BIGINT NOT NULL, body TEXT NOT NULL, token_count BIGINT)",
            "CREATE TABLE anchors (id TEXT PRIMARY KEY, document_id TEXT NOT NULL REFERENCES documents(id), message_id TEXT NOT NULL)",
            "CREATE INDEX idx_messages_order ON messages (stream_id, order_index, id)",
            "CREATE INDEX idx_anchors_message ON anchors (document_id, message_id)",
        ] {
            commit_sql(&store, ddl, &[]);
        }
        commit_sql(
            &store,
            "INSERT INTO documents (id, owner_kind, owner_id) VALUES ($1, $2, $3)",
            &[text("doc-1"), text("thread"), text("thread-1")],
        );
        for (id, order) in [("message-1", 1), ("message-2", 2)] {
            commit_sql(
                &store,
                "INSERT INTO messages (id, document_id, stream_id, order_index, body, token_count) VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    text(id),
                    text("doc-1"),
                    text("stream-1"),
                    Value::Int(order),
                    text(&format!("body-{order}-{}", "x".repeat(8 * 1024))),
                    Value::Int(10),
                ],
            );
        }
        commit_sql(
            &store,
            "INSERT INTO messages (id, document_id, stream_id, order_index, body, token_count) VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                text("message-3"),
                text("doc-1"),
                text("stream-1"),
                Value::Int(3),
                text("body-3"),
                Value::Null,
            ],
        );
        commit_sql(
            &store,
            "INSERT INTO anchors (id, document_id, message_id) VALUES ($1, $2, $3)",
            &[text("anchor-1"), text("doc-1"), text("message-1")],
        );

        let snapshot = store.snapshot().expect("query snapshot");
        let owner = execute_relational_query_sql_with_runtime(
            "SELECT id FROM documents WHERE owner_kind = 'thread' AND owner_id = $1",
            &[text("thread-1")],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(1, 4 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("composite unique lookup");
        assert_eq!(owner.rows[0]["id"], text("doc-1"));
        assert_eq!(owner.access_path.name, "__unique_0");
        assert_eq!(owner.access_path.equality_prefix_len, 2);
        assert!(owner.access_path.unique_point);

        let page = execute_relational_query_sql_with_runtime(
            "SELECT id, body FROM messages WHERE stream_id = $1 ORDER BY order_index ASC, id ASC LIMIT $2 OFFSET $3",
            &[text("stream-1"), Value::Int(1), Value::Int(1)],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(1, 64 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("bounded message page");
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0]["id"], text("message-2"));
        assert_eq!(page.access_path.name, "idx_messages_order");
        assert_eq!(page.access_path.equality_prefix_len, 1);
        assert_eq!(page.access_path.order_prefix_len, 2);
        assert_eq!(page.intermediate_rows, 2);
        assert!(page.blocking_operator_memory_reports.is_empty());
        assert_eq!(page.hydration.hydrated_rows, 1);
        assert!(page.hydration.decompressed_bytes > 8 * 1024);

        let locator_constrained_memory = skein_executor::ExecutionMemoryConfig {
            batch_payload_bytes: std::num::NonZeroUsize::new(128)
                .expect("non-zero locator batch budget"),
            ..skein_executor::ExecutionMemoryConfig::default()
        };
        let error = execute_relational_query_sql_with_runtime(
            "SELECT id, body FROM messages WHERE stream_id = $1 ORDER BY order_index ASC, id ASC LIMIT $2 OFFSET $3",
            &[text("stream-1"), Value::Int(1), Value::Int(1)],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(1, 64 * 1024),
            &locator_constrained_memory,
            None,
        )
        .expect_err("ordered locator batch must honor batch_payload_bytes");
        assert!(error.to_string().contains("batch_payload_bytes 128"));

        let explained_page = execute_relational_query_sql_with_runtime(
            "EXPLAIN SELECT id, body FROM messages WHERE stream_id = $1 ORDER BY order_index ASC, id ASC LIMIT $2 OFFSET $3",
            &[text("stream-1"), Value::Int(1), Value::Int(1)],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(16, 64 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("explain ordered index page");
        assert!(explained_page.rows.iter().all(|row| {
            !matches!(row.get("id"), Some(Value::String(id)) if id.contains("TopNExec"))
        }));
        let index_scan = explained_page
            .rows
            .iter()
            .find(|row| {
                matches!(row.get("id"), Some(Value::String(id)) if id.contains("IndexRangeScanExec"))
            })
            .expect("ordered index range scan in explain");
        assert!(matches!(
            index_scan.get("operator info"),
            Some(Value::String(info)) if info.contains("order_prefix=2")
        ));

        let joined = execute_relational_query_sql_with_runtime(
            "SELECT m.id FROM messages AS m INNER JOIN anchors AS a ON a.document_id = m.document_id AND a.message_id = m.id WHERE m.stream_id = $1",
            &[text("stream-1")],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(2, 4 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("indexed join");
        assert_eq!(joined.rows.len(), 1);
        assert_eq!(joined.access_path.name, "idx_messages_order");
        assert_eq!(joined.join_access_paths[0].name, "idx_anchors_message");
        assert_eq!(joined.join_access_paths[0].equality_prefix_len, 2);

        let summary_sql =
            "SELECT COUNT(*) AS message_count, SUM(token_count) AS token_count FROM messages WHERE stream_id = $1";
        let summary = execute_relational_query_sql_with_runtime(
            summary_sql,
            &[text("stream-1")],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(1, 4 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("bounded aggregate");
        assert_eq!(summary.rows[0]["message_count"], Value::Int(3));
        assert_eq!(summary.rows[0]["token_count"], Value::Int(20));
        assert_eq!(summary.hydration.hydrated_rows, 0);

        let counted_tokens = execute_relational_query_sql_with_runtime(
            "SELECT COUNT(token_count) AS counted_tokens FROM messages WHERE stream_id = $1",
            &[text("stream-1")],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(1, 4 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("columnar nullable COUNT");
        assert_eq!(counted_tokens.rows[0]["counted_tokens"], Value::Int(2));

        let row_oracle = execute_relational_query_sql_with_runtime(
            "SELECT COUNT(*) AS message_count, COALESCE(SUM(token_count), 0) AS token_count FROM messages WHERE stream_id = $1",
            &[text("stream-1")],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(1, 4 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("row aggregate oracle");
        assert_eq!(summary.rows, row_oracle.rows);

        let aggregate_constrained_memory = skein_executor::ExecutionMemoryConfig {
            batch_payload_bytes: std::num::NonZeroUsize::new(128)
                .expect("non-zero aggregate batch budget"),
            ..skein_executor::ExecutionMemoryConfig::default()
        };
        let error = execute_relational_query_sql_with_runtime(
            summary_sql,
            &[text("stream-1")],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(1, 4 * 1024),
            &aggregate_constrained_memory,
            None,
        )
        .expect_err("columnar aggregate must honor batch_payload_bytes");
        assert!(error
            .to_string()
            .contains("relational columnar aggregate cannot fit one row"));

        let constrained_memory = skein_executor::ExecutionMemoryConfig {
            blocking_operator_bytes: std::num::NonZeroUsize::new(1)
                .expect("non-zero aggregate memory budget"),
            ..skein_executor::ExecutionMemoryConfig::default()
        };
        let error = execute_relational_query_sql_with_runtime(
            summary_sql,
            &[text("stream-1")],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(1, 4 * 1024),
            &constrained_memory,
            None,
        )
        .expect_err("aggregate must honor its memory budget");
        assert!(error.to_string().contains("blocking_operator_bytes"));
    }

    #[test]
    fn distinct_sources_account_input_batch_payload() {
        let store = RelationalStore::default();
        commit_sql(
            &store,
            "CREATE TABLE distinct_values (id TEXT PRIMARY KEY, body TEXT NOT NULL)",
            &[],
        );
        commit_sql(
            &store,
            "INSERT INTO distinct_values (id, body) VALUES ($1, $2)",
            &[text("value-1"), text(&"x".repeat(256))],
        );

        let snapshot = store.snapshot().expect("distinct query snapshot");
        let constrained_memory = skein_executor::ExecutionMemoryConfig {
            batch_payload_bytes: std::num::NonZeroUsize::new(128)
                .expect("non-zero distinct batch budget"),
            ..skein_executor::ExecutionMemoryConfig::default()
        };
        for sql in [
            "SELECT DISTINCT body FROM distinct_values",
            "SELECT COUNT(DISTINCT body) AS body_count FROM distinct_values",
        ] {
            let error = execute_relational_query_sql_with_runtime(
                sql,
                &[],
                snapshot.value(),
                RelationalQueryReadModes::new(
                    RelationalIndexReadMode::Materialized,
                    RelationalRowReadMode::CanonicalMemory,
                ),
                query_limits(1, 4 * 1024),
                &constrained_memory,
                None,
            )
            .expect_err("distinct input must honor batch_payload_bytes");
            assert!(error.to_string().contains("intermediate row uses"));
            assert!(error.to_string().contains("batch_payload_bytes 128"));
        }
    }

    fn commit_sql(store: &RelationalStore, sql: &str, parameters: &[Value]) {
        let snapshot = store.snapshot().expect("SQL mutation snapshot");
        let transaction = compile_relational_statement_sql(sql, parameters, snapshot.value())
            .unwrap_or_else(|error| panic!("failed to compile SQL '{sql}': {error}"));
        store
            .commit(transaction, |_, _| Ok(()))
            .unwrap_or_else(|error| panic!("failed to commit SQL '{sql}': {error}"));
    }

    fn text(value: &str) -> Value {
        Value::String(value.to_string())
    }

    fn query_limits(
        max_output_rows: usize,
        max_output_payload_bytes: usize,
    ) -> RelationalQueryLimits {
        RelationalQueryLimits {
            max_output_rows,
            max_output_payload_bytes,
            max_intermediate_rows: 10_000,
            max_candidate_work: 10_000,
            hydration: skein_storage::RelationalHydrationBudget::default(),
            index_read: skein_storage::RelationalIndexReadLimits::default(),
            row_read: skein_storage::RelationalRowPageSnapshotReadLimits::default(),
        }
    }

    #[test]
    fn selective_content_store_join_uses_the_unique_owner_as_outer() {
        const SQL: &str = "SELECT c.chunk_id, d.owner_id AS source_id, c.chunk_index, c.text \
            FROM content_chunks AS c \
            INNER JOIN content_documents AS d ON d.content_doc_id = c.content_doc_id \
            WHERE d.owner_kind = 'source' AND d.owner_id = $1 \
            ORDER BY c.chunk_index ASC, c.chunk_id ASC LIMIT $2";

        let store = RelationalStore::default();
        for ddl in [
            "CREATE TABLE content_documents (content_doc_id TEXT PRIMARY KEY, owner_kind TEXT NOT NULL, owner_id TEXT NOT NULL, UNIQUE (owner_kind, owner_id))",
            "CREATE TABLE content_chunks (chunk_id TEXT PRIMARY KEY, content_doc_id TEXT NOT NULL REFERENCES content_documents(content_doc_id), chunk_index BIGINT NOT NULL, text TEXT NOT NULL, UNIQUE (content_doc_id, chunk_index))",
            "CREATE UNIQUE INDEX idx_content_chunks_order ON content_chunks (content_doc_id, chunk_index)",
        ] {
            commit_sql(&store, ddl, &[]);
        }
        for document in 0..4 {
            let document_id = format!("doc-{document}");
            let owner_id = format!("source-{document}");
            commit_sql(
                &store,
                "INSERT INTO content_documents (content_doc_id, owner_kind, owner_id) VALUES ($1, 'source', $2)",
                &[text(&document_id), text(&owner_id)],
            );
            for chunk in 0..3 {
                commit_sql(
                    &store,
                    "INSERT INTO content_chunks (chunk_id, content_doc_id, chunk_index, text) VALUES ($1, $2, $3, $4)",
                    &[
                        text(&format!("chunk-{document}-{chunk}")),
                        text(&document_id),
                        Value::Int(chunk),
                        text(&format!("body-{document}-{chunk}")),
                    ],
                );
            }
        }

        let snapshot = store.snapshot().expect("content store query snapshot");
        let parameters = [text("source-2"), Value::Int(8)];
        let output = execute_relational_query_sql_with_runtime(
            SQL,
            &parameters,
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(8, 64 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("select source chunks from the selective document outer");

        assert_eq!(output.rows.len(), 3);
        assert_eq!(output.intermediate_rows, 4);
        assert_eq!(output.rows[0]["chunk_id"], text("chunk-2-0"));
        assert_eq!(output.rows[2]["chunk_id"], text("chunk-2-2"));
        assert!(output.access_path.unique_point);
        assert_eq!(output.access_path.index_columns, ["owner_kind", "owner_id"]);
        assert_eq!(output.join_access_paths.len(), 1);
        assert_eq!(output.join_access_paths[0].equality_prefix_len, 1);
        assert_eq!(
            output.join_access_paths[0].index_columns[0],
            "content_doc_id"
        );
        assert_eq!(
            output.join_planning.strategy,
            RelationalJoinPlanningStrategy::CsgCmpMemo
        );
        assert_eq!(
            output.join_planning.status,
            RelationalJoinPlanningStatus::Selected
        );
        assert_eq!(
            output.join_planning.reason,
            RelationalJoinPlanningReason::CostReordered
        );
        assert_eq!(output.join_planning.selected_order, ["d", "c"]);
        assert!(output.join_planning.memo_groups.is_some());
        assert!(output.join_planning.memo_expressions.is_some());
        assert!(output.join_planning.cost.is_some());
        assert_eq!(output.join_planning.attempts.len(), 1);
        assert_eq!(
            output.join_planning.attempts[0].strategy,
            RelationalJoinPlanningStrategy::CsgCmpMemo
        );
        assert_eq!(
            output.join_planning.attempts[0].status,
            RelationalJoinPlanningStatus::Selected
        );

        let explain_sql = format!("EXPLAIN ANALYZE {SQL}");
        let explained = execute_relational_query_sql_with_runtime(
            &explain_sql,
            &parameters,
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(16, 64 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("explain the selective content store join order");
        let join = explained
            .rows
            .iter()
            .find(|row| {
                matches!(
                    row.get("id"),
                    Some(Value::String(id)) if id.contains("IndexNestedLoopJoinExec")
                )
            })
            .expect("reordered index nested-loop join in explain");
        assert!(matches!(
            join.get("access object"),
            Some(Value::String(access)) if access.contains("content_chunks")
        ));
        assert!(matches!(
            join.get("operator info"),
            Some(Value::String(info))
                if info.contains("join_order=cost_reordered")
                    && info.contains("planning_strategy=csg_cmp_memo")
                    && info.contains("planning_status=selected")
                    && info.contains("planning_reason=cost_reordered")
                    && info.contains("selected_order=[d,c]")
                    && info.contains("attempts=[0:csg_cmp_memo:selected")
                    && info.contains("parse_nanos=")
                    && info.contains("bind_nanos=")
                    && info.contains("plan_nanos=")
                    && info.contains("execute_nanos=")
                    && info.contains("plan_cost=")
        ));
    }

    #[test]
    fn three_way_inner_join_uses_memo_selected_binding_order() {
        const SQL: &str = "SELECT c.chunk_id, o.external_id \
            FROM join_chunks AS c \
            INNER JOIN join_documents AS d ON d.document_id = c.document_id \
            INNER JOIN join_owners AS o ON o.owner_id = d.owner_id \
            WHERE o.external_id = $1 \
            ORDER BY c.chunk_id ASC";

        let store = RelationalStore::default();
        for ddl in [
            "CREATE TABLE join_owners (owner_id TEXT PRIMARY KEY, external_id TEXT NOT NULL UNIQUE)",
            "CREATE TABLE join_documents (document_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL UNIQUE REFERENCES join_owners(owner_id))",
            "CREATE TABLE join_chunks (chunk_id TEXT PRIMARY KEY, document_id TEXT NOT NULL REFERENCES join_documents(document_id))",
            "CREATE INDEX idx_join_chunks_document ON join_chunks (document_id)",
        ] {
            commit_sql(&store, ddl, &[]);
        }
        for owner in 0..4 {
            let owner_id = format!("owner-{owner}");
            let external_id = format!("external-{owner}");
            let document_id = format!("document-{owner}");
            commit_sql(
                &store,
                "INSERT INTO join_owners (owner_id, external_id) VALUES ($1, $2)",
                &[text(&owner_id), text(&external_id)],
            );
            commit_sql(
                &store,
                "INSERT INTO join_documents (document_id, owner_id) VALUES ($1, $2)",
                &[text(&document_id), text(&owner_id)],
            );
            for chunk in 0..3 {
                commit_sql(
                    &store,
                    "INSERT INTO join_chunks (chunk_id, document_id) VALUES ($1, $2)",
                    &[text(&format!("chunk-{owner}-{chunk}")), text(&document_id)],
                );
            }
        }

        let snapshot = store.snapshot().expect("three-way join snapshot");
        let output = execute_relational_query_sql_with_runtime(
            SQL,
            &[text("external-2")],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(8, 64 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("execute memo-selected three-way join order");

        assert_eq!(output.rows.len(), 3);
        assert_eq!(output.rows[0]["chunk_id"], text("chunk-2-0"));
        assert_eq!(output.rows[2]["chunk_id"], text("chunk-2-2"));
        assert_eq!(output.access_path.index_columns, ["external_id"]);
        assert!(output.access_path.unique_point);
        assert_eq!(output.join_access_paths.len(), 2);
        assert!(output.join_access_paths[0].unique_point);
        assert_eq!(output.join_access_paths[0].index_columns, ["owner_id"]);
        assert_eq!(output.join_access_paths[1].index_columns, ["document_id"]);
        assert_eq!(output.operator_cardinality_profiles.len(), 3);
        assert_eq!(
            output
                .operator_cardinality_profiles
                .iter()
                .map(|profile| profile.operator_id.get())
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(
            output
                .operator_cardinality_profiles
                .iter()
                .map(|profile| profile.estimated_rows)
                .collect::<Vec<_>>(),
            [1, 1, 12]
        );
        assert_eq!(
            output
                .operator_cardinality_profiles
                .iter()
                .map(|profile| profile.actual_rows)
                .collect::<Vec<_>>(),
            [Some(1), Some(1), Some(3)]
        );
        assert!(output
            .operator_cardinality_profiles
            .iter()
            .all(|profile| profile.fully_consumed));
    }

    #[test]
    fn degenerate_inner_join_uses_csg_cmp_without_legacy_preflight() {
        const SQL: &str = "SELECT a.id AS a_id, c.id AS c_id \
            FROM planning_a AS a \
            INNER JOIN planning_b AS b ON b.a_id = a.id \
            INNER JOIN planning_c AS c ON b.a_id = a.id \
            ORDER BY a.id ASC, c.id ASC";

        let store = RelationalStore::default();
        for ddl in [
            "CREATE TABLE planning_a (id TEXT PRIMARY KEY)",
            "CREATE TABLE planning_b (id TEXT PRIMARY KEY, a_id TEXT NOT NULL REFERENCES planning_a(id))",
            "CREATE TABLE planning_c (id TEXT PRIMARY KEY)",
        ] {
            commit_sql(&store, ddl, &[]);
        }
        commit_sql(&store, "INSERT INTO planning_a (id) VALUES ('a-1')", &[]);
        commit_sql(
            &store,
            "INSERT INTO planning_b (id, a_id) VALUES ('b-1', 'a-1')",
            &[],
        );
        commit_sql(&store, "INSERT INTO planning_c (id) VALUES ('c-1')", &[]);

        let snapshot = store.snapshot().expect("join fallback snapshot");
        let output = execute_relational_query_sql_with_runtime(
            SQL,
            &[],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(8, 64 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("execute a degenerate inner join through CSG-CMP");

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.join_planning.strategy,
            RelationalJoinPlanningStrategy::CsgCmpMemo
        );
        assert_eq!(
            output.join_planning.status,
            RelationalJoinPlanningStatus::Selected
        );
        assert_eq!(
            output.join_planning.reason,
            RelationalJoinPlanningReason::SyntaxOrderOptimal
        );
        assert_eq!(output.join_planning.selected_order, ["a", "b", "c"]);
        assert!(output.join_planning.cost.is_some());
        assert_eq!(output.join_planning.attempts.len(), 1);
        assert_eq!(
            output
                .join_planning
                .attempts
                .iter()
                .map(|attempt| attempt.strategy)
                .collect::<Vec<_>>(),
            [RelationalJoinPlanningStrategy::CsgCmpMemo]
        );

        let explained = execute_relational_query_sql_with_runtime(
            &format!("EXPLAIN {SQL}"),
            &[],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(16, 64 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("explain the degenerate CSG-CMP plan");
        assert!(explained.rows.iter().any(|row| matches!(
            row.get("operator info"),
            Some(Value::String(info))
                if info.contains("join_order=syntax")
                    && info.contains("planning_status=selected")
                    && info.contains("planning_reason=syntax_order_optimal")
                    && info.contains("0:csg_cmp_memo:selected:syntax_order_optimal")
                    && info.contains("plan_cost=")
        )));
    }

    #[test]
    fn mixed_join_rewrite_preserves_left_rows_through_left_asscom() {
        const SQL: &str = "SELECT a.id AS a_id, b.id AS b_id \
            FROM rewrite_a AS a \
            LEFT JOIN rewrite_b AS b ON a.id = b.a_id \
            INNER JOIN rewrite_c AS c ON a.c_id = c.id \
            WHERE c.external_id = $1 \
            ORDER BY a.id ASC";

        let store = RelationalStore::default();
        for ddl in [
            "CREATE TABLE rewrite_c (id TEXT PRIMARY KEY, external_id TEXT NOT NULL UNIQUE)",
            "CREATE TABLE rewrite_a (id TEXT PRIMARY KEY, c_id TEXT NOT NULL REFERENCES rewrite_c(id))",
            "CREATE INDEX idx_rewrite_a_c ON rewrite_a (c_id)",
            "CREATE TABLE rewrite_b (id TEXT PRIMARY KEY, a_id TEXT NOT NULL REFERENCES rewrite_a(id), c_id TEXT NOT NULL REFERENCES rewrite_c(id))",
            "CREATE INDEX idx_rewrite_b_a ON rewrite_b (a_id)",
            "CREATE INDEX idx_rewrite_b_c ON rewrite_b (c_id)",
        ] {
            commit_sql(&store, ddl, &[]);
        }
        for (id, external_id) in [("c-target", "target"), ("c-other", "other")] {
            commit_sql(
                &store,
                "INSERT INTO rewrite_c (id, external_id) VALUES ($1, $2)",
                &[text(id), text(external_id)],
            );
        }
        for (id, c_id) in [("a-1", "c-target"), ("a-2", "c-target"), ("a-3", "c-other")] {
            commit_sql(
                &store,
                "INSERT INTO rewrite_a (id, c_id) VALUES ($1, $2)",
                &[text(id), text(c_id)],
            );
        }
        for (id, a_id, c_id) in [("b-1", "a-1", "c-target"), ("b-3", "a-3", "c-other")] {
            commit_sql(
                &store,
                "INSERT INTO rewrite_b (id, a_id, c_id) VALUES ($1, $2, $3)",
                &[text(id), text(a_id), text(c_id)],
            );
        }

        let snapshot = store.snapshot().expect("mixed join rewrite snapshot");
        let output = execute_relational_query_sql_with_runtime(
            SQL,
            &[text("target")],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(8, 64 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("execute CD-C left-asscom rewrite");

        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0]["a_id"], text("a-1"));
        assert_eq!(output.rows[0]["b_id"], text("b-1"));
        assert_eq!(output.rows[1]["a_id"], text("a-2"));
        assert_eq!(output.rows[1]["b_id"], Value::Null);
        assert_eq!(output.access_path.index_columns, ["external_id"]);
        assert_eq!(output.join_access_paths[0].index_columns, ["c_id"]);
        assert_eq!(output.join_access_paths[1].index_columns, ["a_id"]);
        assert_eq!(
            output
                .operator_cardinality_profiles
                .iter()
                .map(|profile| (
                    profile.operator,
                    profile.estimated_rows,
                    profile.actual_rows
                ))
                .collect::<Vec<_>>(),
            [
                (RelationalOperatorKind::IndexRangeScan, 1, Some(1)),
                (
                    RelationalOperatorKind::BatchedIndexNestedLoopJoin,
                    3,
                    Some(2),
                ),
                (
                    RelationalOperatorKind::BatchedIndexNestedLoopLeftJoin,
                    6,
                    Some(2),
                ),
            ]
        );
        assert!(output
            .operator_cardinality_profiles
            .iter()
            .all(|profile| profile.fully_consumed));
    }

    #[test]
    fn null_rejecting_filter_converts_left_join_before_memo_rewrite() {
        const SQL: &str = "SELECT a.id AS a_id, b.id AS b_id \
            FROM rewrite_nr_a AS a \
            LEFT JOIN rewrite_nr_b AS b ON a.id = b.a_id \
            INNER JOIN rewrite_nr_c AS c ON b.c_id = c.id \
            WHERE c.external_id = $1 AND b.id IS NOT NULL \
            ORDER BY a.id ASC";

        let store = RelationalStore::default();
        for ddl in [
            "CREATE TABLE rewrite_nr_c (id TEXT PRIMARY KEY, external_id TEXT NOT NULL UNIQUE)",
            "CREATE TABLE rewrite_nr_a (id TEXT PRIMARY KEY)",
            "CREATE TABLE rewrite_nr_b (id TEXT PRIMARY KEY, a_id TEXT NOT NULL REFERENCES rewrite_nr_a(id), c_id TEXT NOT NULL REFERENCES rewrite_nr_c(id))",
            "CREATE INDEX idx_rewrite_nr_b_a ON rewrite_nr_b (a_id)",
            "CREATE INDEX idx_rewrite_nr_b_c ON rewrite_nr_b (c_id)",
        ] {
            commit_sql(&store, ddl, &[]);
        }
        commit_sql(
            &store,
            "INSERT INTO rewrite_nr_c (id, external_id) VALUES ('c-target', 'target')",
            &[],
        );
        commit_sql(
            &store,
            "INSERT INTO rewrite_nr_c (id, external_id) VALUES ('c-other', 'other')",
            &[],
        );
        for id in ["a-1", "a-2"] {
            commit_sql(
                &store,
                "INSERT INTO rewrite_nr_a (id) VALUES ($1)",
                &[text(id)],
            );
        }
        commit_sql(
            &store,
            "INSERT INTO rewrite_nr_b (id, a_id, c_id) VALUES ('b-1', 'a-1', 'c-target')",
            &[],
        );

        let snapshot = store
            .snapshot()
            .expect("null-rejecting join rewrite snapshot");
        let output = execute_relational_query_sql_with_runtime(
            SQL,
            &[text("target")],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(8, 64 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("execute null-rejection-authorized join rewrite");

        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["a_id"], text("a-1"));
        assert_eq!(output.rows[0]["b_id"], text("b-1"));
        assert_eq!(output.access_path.name, "__full_scan");
        assert!(output.join_access_paths[0].unique_point);
        assert!(output.join_access_paths[1].unique_point);

        let explained = execute_relational_query_sql_with_runtime(
            &format!("EXPLAIN ANALYZE {SQL}"),
            &[text("target")],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            query_limits(16, 64 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("explain null-rejection-authorized join rewrite");
        let join_operators = explained
            .rows
            .iter()
            .filter_map(|row| match row.get("id") {
                Some(Value::String(id)) if id.contains("JoinExec") => Some(id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(join_operators.len(), 2);
        assert!(join_operators
            .iter()
            .all(|operator| operator.contains("IndexNestedLoopJoinExec")));
        assert!(join_operators
            .iter()
            .all(|operator| !operator.contains("LeftJoin")));
        let base = explained
            .rows
            .iter()
            .find(|row| {
                matches!(
                    row.get("id"),
                    Some(Value::String(id)) if id.contains("TableFullScanExec")
                )
            })
            .expect("rewritten base table scan in explain");
        assert!(matches!(
            base.get("access object"),
            Some(Value::String(access)) if access.contains("rewrite_nr_b")
        ));
        assert!(explained.rows.iter().any(|row| matches!(
            row.get("operator info"),
            Some(Value::String(info)) if info.contains("join_order=cost_reordered")
        )));
    }

    #[test]
    fn relational_sort_and_distinct_spill_and_pipeline_cancellation_are_bounded() {
        let store = RelationalStore::default();
        let rows = (0..256)
            .rev()
            .map(|value| {
                RelationalRow::new(vec![
                    RelationalValue::BigInt(value),
                    RelationalValue::BigInt(value % 17),
                ])
            })
            .collect();
        store
            .commit(
                RelationalTransaction {
                    writes: vec![
                        RelationalWrite::CreateTable(RelationalTableSchema {
                            name: "spill_rows".to_string(),
                            columns: vec![
                                RelationalColumnSchema {
                                    name: "id".to_string(),
                                    scalar_type: RelationalScalarType::BigInt,
                                    nullable: false,
                                    default: None,
                                },
                                RelationalColumnSchema {
                                    name: "value".to_string(),
                                    scalar_type: RelationalScalarType::BigInt,
                                    nullable: false,
                                    default: None,
                                },
                            ],
                            primary_key: vec!["id".to_string()],
                            unique_constraints: Vec::new(),
                            foreign_keys: Vec::new(),
                            indexes: Vec::new(),
                        }),
                        RelationalWrite::Insert {
                            table: "spill_rows".to_string(),
                            rows,
                            mode: RelationalInsertMode::Error,
                        },
                    ],
                },
                |_, _| Ok(()),
            )
            .expect("materialize spill fixture");
        commit_sql(
            &store,
            "CREATE TABLE spill_labels (id BIGINT PRIMARY KEY, label TEXT NOT NULL)",
            &[],
        );
        commit_sql(
            &store,
            "INSERT INTO spill_labels (id, label) VALUES ($1, $2)",
            &[Value::Int(0), text("zero")],
        );
        let snapshot = store.snapshot().expect("spill fixture snapshot");
        let limits = query_limits(256, 64 * 1024);
        let memory = skein_executor::ExecutionMemoryConfig {
            batch_rows: std::num::NonZeroUsize::new(8).expect("non-zero batch rows"),
            blocking_operator_bytes: std::num::NonZeroUsize::new(16 * 1_024)
                .expect("non-zero blocking memory"),
            min_spill_free_bytes: std::num::NonZeroU64::MIN,
            spill_directory: std::env::temp_dir().join(format!(
                "skein-relational-spill-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock")
                    .as_nanos()
            )),
            ..skein_executor::ExecutionMemoryConfig::default()
        };

        let sorted = execute_relational_query_sql_with_runtime(
            "SELECT id FROM spill_rows ORDER BY value ASC, id ASC",
            &[],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            limits,
            &memory,
            None,
        )
        .expect("spill-backed relational sort");
        assert_eq!(sorted.rows.len(), 256);
        let expected_order = (0..256)
            .map(|id| (id % 17, id))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|(_, id)| Value::Int(id))
            .collect::<Vec<_>>();
        assert_eq!(
            sorted
                .rows
                .iter()
                .map(|row| row["id"].clone())
                .collect::<Vec<_>>(),
            expected_order
        );
        assert!(sorted
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "TopNExec" && report.spill_run_count > 0));

        let joined = execute_relational_query_sql_with_runtime(
            "SELECT r.id, l.label FROM spill_rows AS r LEFT JOIN spill_labels AS l ON l.id = r.id ORDER BY r.value ASC, r.id ASC",
            &[],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            limits,
            &memory,
            None,
        )
        .expect("spill-backed relational left join sort");
        assert_eq!(joined.rows.len(), 256);
        assert_eq!(
            joined
                .rows
                .iter()
                .map(|row| row["id"].clone())
                .collect::<Vec<_>>(),
            expected_order
        );
        assert_eq!(joined.rows[0]["id"], Value::Int(0));
        assert_eq!(joined.rows[0]["label"], text("zero"));
        assert_eq!(joined.rows[1]["label"], Value::Null);
        assert!(joined
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "TopNExec" && report.spill_run_count > 0));

        let distinct = execute_relational_query_sql_with_runtime(
            "SELECT DISTINCT id FROM spill_rows ORDER BY id ASC",
            &[],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            limits,
            &memory,
            None,
        )
        .expect("spill-backed relational distinct");
        assert_eq!(distinct.rows.len(), 256);
        assert!(distinct
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "DistinctExec" && report.spill_run_count > 0));

        let distinct_count = execute_relational_query_sql_with_runtime(
            "SELECT COUNT(DISTINCT id) AS item_count FROM spill_rows",
            &[],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            limits,
            &memory,
            None,
        )
        .expect("spill-backed relational distinct aggregate");
        assert_eq!(distinct_count.rows[0]["item_count"], Value::Int(256));
        assert!(distinct_count
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "DistinctExec" && report.spill_run_count > 0));

        let grouped = execute_relational_query_sql_with_runtime(
            "SELECT value, COUNT(*) AS item_count FROM spill_rows GROUP BY value",
            &[],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            limits,
            &memory,
            None,
        )
        .expect("spill-backed grouped relational aggregate");
        assert_eq!(grouped.rows.len(), 17);
        assert_eq!(
            grouped
                .rows
                .iter()
                .map(|row| match row["item_count"] {
                    Value::Int(value) => value,
                    ref value => panic!("unexpected aggregate value {value:?}"),
                })
                .sum::<i64>(),
            256
        );
        assert!(grouped
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "SortExec" && report.spill_run_count > 0));

        let explain_cancellation = skein_core::RuntimeCancellationToken::new();
        explain_cancellation.cancel();
        let explain_context =
            skein_core::RuntimeTaskContext::without_deadline(explain_cancellation);
        let explained = execute_relational_query_sql_with_runtime(
            "EXPLAIN SELECT id FROM spill_rows WHERE id = $1",
            &[Value::Int(7)],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            limits,
            &memory,
            Some(&explain_context),
        )
        .expect("plain EXPLAIN must plan without executing the cancelled scan");
        assert!(explained.rows.iter().any(|row| {
            matches!(
                row.get("access object"),
                Some(Value::String(access)) if access.contains("primary_key")
            )
        }));
        assert!(explained
            .rows
            .iter()
            .all(|row| !row.contains_key("actRows")));

        let analyzed = execute_relational_query_sql_with_runtime(
            "EXPLAIN ANALYZE SELECT id FROM spill_rows ORDER BY value ASC, id ASC",
            &[],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            limits,
            &memory,
            None,
        )
        .expect("EXPLAIN ANALYZE must expose measured spill evidence");
        assert_eq!(analyzed.rows[0]["actRows"], Value::Null);
        assert!(matches!(
            analyzed.rows[0].get("execution info"),
            Some(Value::String(info)) if info.contains("statement_output_rows=256")
        ));
        assert!(analyzed.rows.iter().any(|row| {
            matches!(row.get("id"), Some(Value::String(id)) if id.contains("TopNExec"))
                && !matches!(row.get("disk"), None | Some(Value::Null))
        }));

        let cancellation = skein_core::RuntimeCancellationToken::new();
        cancellation.cancel();
        let task_context = skein_core::RuntimeTaskContext::without_deadline(cancellation);
        let error = execute_relational_query_sql_with_runtime(
            "SELECT id FROM spill_rows",
            &[],
            snapshot.value(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            limits,
            &memory,
            Some(&task_context),
        )
        .expect_err("cancelled relational scan must stop at a batch checkpoint");
        assert!(error
            .to_string()
            .contains("runtime task stopped: cancelled"));

        std::fs::remove_dir_all(&memory.spill_directory).expect("remove spill test directory");
    }
}
