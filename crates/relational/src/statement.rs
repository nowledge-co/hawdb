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

//! Storage-neutral RowPage DDL/DML compilation; transaction execution stays outside.

use crate::{
    bind_relational_value, coerce_relational_value, compile_column, reject_non_public_schema,
};
use hawdb_core::{HawDBError, Result, Value};
use hawdb_sql::{
    AlterTableAddColumnStatement, CreateIndexStatement, CreateTableStatement, Expr, ExprKind,
    PreparedPostgresStatement, SqlArithmeticOperand, SqlAssignmentValue, SqlCommonTableExpression,
    SqlCommonTableExpressionBody, SqlComparisonOp, SqlConflictAction, SqlPredicate,
    SqlQueryStatement, SqlReferentialAction, SqlStatement, SqlTableConstraint, SqlTableStorage,
    SqlValue,
};
use hawdb_storage::relational::{
    RelationalBigIntArithmeticOperator, RelationalBigIntOperand, RelationalColumnDefault,
    RelationalColumnSchema, RelationalComparisonOp, RelationalConflictAction,
    RelationalForeignKeySchema, RelationalIndexSchema, RelationalInsertMode,
    RelationalMutationOutcome, RelationalPredicate, RelationalReferentialAction, RelationalRow,
    RelationalScalarType, RelationalState, RelationalTableSchema, RelationalTransaction,
    RelationalUpdateAssignment, RelationalUpdateValue, RelationalUpsertAssignment,
    RelationalUpsertValue, RelationalValue, RelationalWrite,
};
use std::collections::BTreeSet;

mod numeric;

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

#[derive(Debug, Clone)]
pub struct CompiledDataModifyingCteQuery {
    pub transaction: RelationalTransaction,
    pub query: SqlQueryStatement,
    pub ctes: Vec<RelationalDataModifyingCte>,
}

#[derive(Debug, Clone)]
pub struct RelationalDataModifyingCte {
    pub name: String,
    pub columns: Vec<String>,
    pub returning: RelationalReturningProjection,
}

pub fn compile_prepared_data_modifying_cte_query(
    prepared: &PreparedPostgresStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<Option<CompiledDataModifyingCteQuery>> {
    let SqlStatement::Query(query) = &prepared.statement else {
        return Ok(None);
    };
    if !query.contains_data_modifying_cte() {
        return Ok(None);
    }
    if prepared.parameters.len() != parameters.len() {
        return Err(HawDBError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    let mut query = query.clone();
    let mut retained = Vec::new();
    let mut ctes = Vec::new();
    let mut writes = Vec::new();
    let mut names = BTreeSet::new();
    let mut mutated_tables = BTreeSet::new();
    let mut later_data_modifying_ctes = query
        .with
        .iter()
        .filter(|cte| matches!(cte.body, SqlCommonTableExpressionBody::Mutation(_)))
        .map(|cte| cte.name.clone())
        .collect::<BTreeSet<_>>();
    for cte in std::mem::take(&mut query.with) {
        if !names.insert(cte.name.clone()) {
            return Err(HawDBError::Semantic(format!(
                "common table expression {} is specified more than once",
                cte.name
            )));
        }
        match cte.body {
            SqlCommonTableExpressionBody::Query(cte_query) => {
                if cte_query.contains_data_modifying_cte() {
                    return Err(HawDBError::Semantic(
                        "data-modifying CTEs are supported only in the top-level WITH list".into(),
                    ));
                }
                if let Some(later) =
                    first_referenced_relation(&cte_query, &later_data_modifying_ctes)
                {
                    return Err(HawDBError::Semantic(format!(
                        "common table expression {} cannot reference later data-modifying CTE {later}",
                        cte.name
                    )));
                }
                retained.push(SqlCommonTableExpression {
                    name: cte.name,
                    columns: cte.columns,
                    body: SqlCommonTableExpressionBody::Query(cte_query),
                });
            }
            SqlCommonTableExpressionBody::Mutation(statement) => {
                later_data_modifying_ctes.remove(&cte.name);
                let compiled = compile_relational_mutation(&statement, parameters, state)?;
                let returning = compiled.returning.ok_or_else(|| {
                    HawDBError::Semantic(format!(
                        "data-modifying CTE {} requires a RETURNING clause",
                        cte.name
                    ))
                })?;
                if !mutated_tables.insert(returning.table.clone()) {
                    return Err(HawDBError::Semantic(format!(
                        "data-modifying CTEs cannot target table {} more than once",
                        returning.table
                    )));
                }
                if !cte.columns.is_empty() && cte.columns.len() != returning.columns.len() {
                    return Err(HawDBError::Semantic(format!(
                        "common table expression {} declares {} columns but returns {}",
                        cte.name,
                        cte.columns.len(),
                        returning.columns.len()
                    )));
                }
                let mut output_names = BTreeSet::new();
                let output_columns = if cte.columns.is_empty() {
                    &returning.columns
                } else {
                    &cte.columns
                };
                if let Some(duplicate) = output_columns
                    .iter()
                    .find(|column| !output_names.insert(column.as_str()))
                {
                    return Err(HawDBError::Semantic(format!(
                        "common table expression {} repeats output column {duplicate}",
                        cte.name
                    )));
                }
                writes.extend(compiled.transaction.writes);
                ctes.push(RelationalDataModifyingCte {
                    name: cte.name,
                    columns: cte.columns,
                    returning,
                });
            }
        }
    }
    if query.body.contains_data_modifying_cte() {
        return Err(HawDBError::Semantic(
            "data-modifying CTEs are supported only in the top-level WITH list".into(),
        ));
    }
    query.with = retained;
    Ok(Some(CompiledDataModifyingCteQuery {
        transaction: RelationalTransaction { writes },
        query,
        ctes,
    }))
}

fn first_referenced_relation(
    query: &SqlQueryStatement,
    candidates: &BTreeSet<String>,
) -> Option<String> {
    let mut referenced = None;
    query.visit_selects(&mut |select| {
        if referenced.is_some() {
            return;
        }
        referenced = select
            .from
            .iter()
            .chain(select.joins.iter().map(|join| &join.table))
            .find(|table| table.schema.is_none() && candidates.contains(&table.name))
            .map(|table| table.name.clone());
    });
    referenced
}

pub fn materialize_data_modifying_ctes(
    ctes: &[RelationalDataModifyingCte],
    outcomes: &[RelationalMutationOutcome],
    state: &RelationalState,
) -> Result<Vec<crate::query::RelationalEphemeralQueryTable>> {
    if ctes.len() != outcomes.len() {
        return Err(HawDBError::StorageIntegrity(format!(
            "data-modifying CTE plan recorded {} projections but staging produced {} outcomes",
            ctes.len(),
            outcomes.len()
        )));
    }
    ctes.iter()
        .zip(outcomes)
        .map(|(cte, outcome)| {
            if outcome.table != cte.returning.table {
                return Err(HawDBError::StorageIntegrity(format!(
                    "data-modifying CTE {} expected table {} but staging returned {}",
                    cte.name, cte.returning.table, outcome.table
                )));
            }
            let source = state.table_schema(&outcome.table).ok_or_else(|| {
                HawDBError::StorageIntegrity(format!(
                    "data-modifying CTE outcome references unknown table {}",
                    outcome.table
                ))
            })?;
            let positions = cte
                .returning
                .columns
                .iter()
                .map(|column| {
                    source.column_position(column).ok_or_else(|| {
                        HawDBError::StorageIntegrity(format!(
                            "data-modifying CTE outcome references unknown column {column}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let names = if cte.columns.is_empty() {
                cte.returning.columns.clone()
            } else {
                cte.columns.clone()
            };
            let columns = names
                .into_iter()
                .zip(
                    positions
                        .iter()
                        .map(|position| source.columns[*position].scalar_type),
                )
                .map(|(name, scalar_type)| RelationalColumnSchema {
                    name,
                    scalar_type,
                    nullable: true,
                    default: None,
                })
                .collect();
            let rows = outcome
                .rows
                .iter()
                .map(|row| {
                    RelationalRow::new(
                        positions
                            .iter()
                            .map(|position| row.values()[*position].clone())
                            .collect(),
                    )
                })
                .collect();
            Ok(crate::query::RelationalEphemeralQueryTable {
                schema: RelationalTableSchema {
                    name: cte.name.clone(),
                    columns,
                    primary_key: Vec::new(),
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    indexes: Vec::new(),
                    materialized_view: None,
                },
                rows,
            })
        })
        .collect()
}

pub fn compile_relational_statement_sql_with_result(
    sql: &str,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<CompiledRelationalStatement> {
    let prepared = hawdb_sql::prepare_postgres_sql(sql)?;
    compile_prepared_relational_statement_with_result(&prepared, parameters, state)
}

/// Binds one schema-neutral template to the current execution state and parameters.
/// The template remains borrowed; only the owned mutation data is materialized.
#[doc(hidden)]
pub fn compile_prepared_relational_statement_with_result(
    prepared: &PreparedPostgresStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<CompiledRelationalStatement> {
    if prepared.parameters.len() != parameters.len() {
        return Err(HawDBError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    match &prepared.statement {
        SqlStatement::CreateMaterializedView(create) => Ok(CompiledRelationalStatement {
            transaction: RelationalTransaction {
                writes: crate::materialized_view::create(create.clone(), state)?,
            },
            returning: None,
        }),
        SqlStatement::DropMaterializedView(drop) => Ok(CompiledRelationalStatement {
            transaction: RelationalTransaction {
                writes: crate::materialized_view::drop_view(drop.clone(), state)?,
            },
            returning: None,
        }),
        SqlStatement::CreateSearchIndex(create) => Ok(CompiledRelationalStatement {
            transaction: RelationalTransaction {
                writes: crate::materialized_view::create_search_index(create, state)?,
            },
            returning: None,
        }),
        SqlStatement::DropSearchIndex(drop) => Ok(CompiledRelationalStatement {
            transaction: RelationalTransaction {
                writes: crate::materialized_view::drop_search_index(drop, state)?,
            },
            returning: None,
        }),
        statement @ (SqlStatement::CreateTable(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_)) => Ok(CompiledRelationalStatement {
            transaction: RelationalTransaction {
                writes: compile_schema_statement(statement, state)?,
            },
            returning: None,
        }),
        statement => compile_relational_mutation(statement, parameters, state),
    }
}

fn compile_relational_mutation(
    statement: &SqlStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<CompiledRelationalStatement> {
    let statement = &*crate::literal_coercion::coerce_mutation(statement, state, parameters)?;
    let (write, returning) = match statement {
        SqlStatement::Insert(insert) => {
            reject_non_public_schema(insert.table.schema.as_deref())?;
            let schema = state.table_schema(&insert.table.name).ok_or_else(|| {
                HawDBError::Semantic(format!("unknown relational table {}", insert.table.name))
            })?;
            let returning = compile_returning(
                &insert.returning,
                &insert.table.name,
                None,
                schema,
                "INSERT",
            )?;
            let positions = crate::insert_column_positions(
                &insert.columns,
                &insert.rows,
                &schema.name,
                &schema.columns,
            )?;
            let rows = insert
                .rows
                .iter()
                .map(|values| {
                    let mut row = schema
                        .columns
                        .iter()
                        .map(materialize_column_default)
                        .collect::<Result<Vec<_>>>()?;
                    for (position, value) in positions.iter().zip(values) {
                        let column_name = &schema.columns[*position].name;
                        row[*position] = bind_relational_value_as(
                            value,
                            parameters,
                            schema.columns[*position].scalar_type,
                        )
                        .map_err(|error| {
                            HawDBError::Semantic(format!(
                                "failed to bind INSERT column {column_name}: {error}"
                            ))
                        })?;
                    }
                    Ok(RelationalRow::new(row))
                })
                .collect::<Result<Vec<_>>>()?;
            let write = if let Some(conflict) = &insert.on_conflict {
                let action = match &conflict.action {
                    SqlConflictAction::DoNothing => RelationalConflictAction::DoNothing,
                    SqlConflictAction::DoUpdate(assignments) => RelationalConflictAction::Update(
                        assignments
                            .iter()
                            .map(|assignment| {
                                let value = match &assignment.value {
                                    SqlAssignmentValue::Column(column)
                                        if column.qualifier.as_deref() == Some("excluded") =>
                                    {
                                        RelationalUpsertValue::ExcludedColumn(column.name.clone())
                                    }
                                    SqlAssignmentValue::Column(_) => {
                                        return Err(HawDBError::Semantic(
                                            "ON CONFLICT assignments only support EXCLUDED columns"
                                                .to_string(),
                                        ));
                                    }
                                    SqlAssignmentValue::Value(value) => {
                                        RelationalUpsertValue::Value(bind_relational_value(
                                            value.clone(), parameters,
                                        )?)
                                    }
                                    SqlAssignmentValue::Expression(_) | SqlAssignmentValue::Arithmetic { .. } => {
                                        return Err(HawDBError::Semantic(
                                            "ON CONFLICT assignments do not support arithmetic expressions"
                                                .to_string(),
                                        ));
                                    }
                                };
                                Ok(RelationalUpsertAssignment {
                                    column: assignment.column.clone(),
                                    value,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ),
                };
                RelationalWrite::Upsert {
                    table: insert.table.name.clone(),
                    rows,
                    conflict_columns: conflict.columns.clone(),
                    action,
                }
            } else {
                RelationalWrite::Insert {
                    table: insert.table.name.clone(),
                    rows,
                    mode: RelationalInsertMode::Error,
                }
            };
            (write, returning)
        }
        SqlStatement::Delete(delete) => {
            reject_non_public_schema(delete.table.schema.as_deref())?;
            let schema = state.table_schema(&delete.table.name).ok_or_else(|| {
                HawDBError::Semantic(format!("unknown relational table {}", delete.table.name))
            })?;
            let selection = delete.selection.as_ref().ok_or_else(|| {
                HawDBError::Semantic(
                    "unbounded relational DELETE requires an explicit qualified workflow"
                        .to_string(),
                )
            })?;
            let returning = compile_returning(
                &delete.returning,
                &delete.table.name,
                delete.alias.as_deref(),
                schema,
                "DELETE",
            )?;
            (
                RelationalWrite::DeleteWhere {
                    table: delete.table.name.clone(),
                    predicate: compile_mutation_predicate(
                        selection,
                        parameters,
                        schema,
                        delete.alias.as_deref(),
                        &delete.table.name,
                    )?,
                },
                returning,
            )
        }
        SqlStatement::Update(update) => {
            reject_non_public_schema(update.table.schema.as_deref())?;
            let schema = state.table_schema(&update.table.name).ok_or_else(|| {
                HawDBError::Semantic(format!("unknown relational table {}", update.table.name))
            })?;
            let selection = update.selection.as_ref().ok_or_else(|| {
                HawDBError::Semantic(
                    "unbounded relational UPDATE requires an explicit qualified workflow"
                        .to_string(),
                )
            })?;
            let returning = compile_returning(
                &update.returning,
                &update.table.name,
                update.alias.as_deref(),
                schema,
                "UPDATE",
            )?;
            let assignments = update
                .assignments
                .iter()
                .map(|assignment| {
                    if schema.column_position(&assignment.column).is_none() {
                        return Err(HawDBError::Semantic(format!(
                            "table {} has no column {}",
                            schema.name, assignment.column
                        )));
                    }
                    let target_type = schema.columns[schema
                        .column_position(&assignment.column)
                        .expect("assignment column was validated")]
                    .scalar_type;
                    let value = match &assignment.value {
                        SqlAssignmentValue::Expression(expression) => {
                            RelationalUpdateValue::Numeric(numeric::compile_numeric(
                                expression,
                                parameters,
                                schema,
                                update.alias.as_deref(),
                                &update.table.name,
                            )?)
                        }
                        SqlAssignmentValue::Column(column) => {
                            validate_mutation_column(
                                column,
                                schema,
                                update.alias.as_deref(),
                                &update.table.name,
                            )?;
                            if target_type.logical_type().is_numeric() {
                                RelationalUpdateValue::Numeric(
                                    hawdb_storage::relational::RelationalNumericExpression::Column(
                                        column.name.clone(),
                                    ),
                                )
                            } else {
                                RelationalUpdateValue::Column(column.name.clone())
                            }
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
                                return Err(HawDBError::Semantic(format!(
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
                                    hawdb_sql::SqlArithmeticOperator::Add => {
                                        RelationalBigIntArithmeticOperator::Add
                                    }
                                    hawdb_sql::SqlArithmeticOperator::Subtract => {
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
                        column: assignment.column.clone(),
                        value,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            (
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
                },
                returning,
            )
        }
        SqlStatement::Select(_)
        | SqlStatement::Query(_)
        | SqlStatement::CreateMaterializedView(_)
        | SqlStatement::DropMaterializedView(_)
        | SqlStatement::CreateSearchIndex(_)
        | SqlStatement::DropSearchIndex(_)
        | SqlStatement::Explain(_)
        | SqlStatement::CreateTable(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_)
        | SqlStatement::Branch(_) => {
            return Err(HawDBError::Semantic(
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

fn compile_returning(
    columns: &[hawdb_sql::SqlColumnRef],
    table: &str,
    alias: Option<&str>,
    schema: &RelationalTableSchema,
    statement: &str,
) -> Result<Option<RelationalReturningProjection>> {
    if columns.is_empty() {
        return Ok(None);
    }
    for column in columns {
        if column.qualifier.as_deref().is_some_and(|qualifier| {
            qualifier != table && alias.is_none_or(|alias| qualifier != alias)
        }) {
            return Err(HawDBError::Semantic(format!(
                "{statement} RETURNING has unknown qualifier {qualifier}",
                qualifier = column.qualifier.as_deref().unwrap_or_default()
            )));
        }
        if schema.column_position(&column.name).is_none() {
            return Err(HawDBError::Semantic(format!(
                "table {} has no column {}",
                schema.name, column.name
            )));
        }
    }
    Ok(Some(RelationalReturningProjection {
        table: table.to_string(),
        columns: columns.iter().map(|column| column.name.clone()).collect(),
    }))
}

fn compile_bigint_arithmetic_operand(
    operand: &SqlArithmeticOperand,
    parameters: &[Value],
    schema: &RelationalTableSchema,
    alias: Option<&str>,
    table: &str,
) -> Result<RelationalBigIntOperand> {
    match operand {
        SqlArithmeticOperand::Column(column) => {
            validate_mutation_column(column, schema, alias, table)?;
            let position = schema.column_position(&column.name).ok_or_else(|| {
                HawDBError::Semantic(format!(
                    "table {} has no column {}",
                    schema.name, column.name
                ))
            })?;
            if schema.columns[position].scalar_type != RelationalScalarType::BigInt {
                return Err(HawDBError::Semantic(format!(
                    "UPDATE arithmetic source column {} must be BIGINT",
                    column.name
                )));
            }
            Ok(RelationalBigIntOperand::Column(column.name.clone()))
        }
        SqlArithmeticOperand::Value(value) => {
            let value = bind_relational_value_as(value, parameters, RelationalScalarType::BigInt)?;
            if !matches!(value, RelationalValue::BigInt(_)) {
                return Err(HawDBError::Semantic(
                    "UPDATE arithmetic values must be non-null BIGINT scalars".to_string(),
                ));
            }
            Ok(RelationalBigIntOperand::Value(value))
        }
    }
}

pub(super) fn compile_mutation_predicate(
    predicate: &SqlPredicate,
    parameters: &[Value],
    schema: &RelationalTableSchema,
    alias: Option<&str>,
    table: &str,
) -> Result<RelationalPredicate> {
    let compile =
        |predicate| compile_mutation_predicate(predicate, parameters, schema, alias, table);
    Ok(match &predicate.kind {
        ExprKind::And(left, right) => {
            RelationalPredicate::And(Box::new(compile(left)?), Box::new(compile(right)?))
        }
        ExprKind::Or(left, right) => {
            RelationalPredicate::Or(Box::new(compile(left)?), Box::new(compile(right)?))
        }
        ExprKind::Not(predicate) => RelationalPredicate::Not(Box::new(compile(predicate)?)),
        ExprKind::Compare { left, op, right } => {
            numeric::compile_comparison(left, *op, right, parameters, schema, alias, table)?
        }
        ExprKind::RowCompare { left, op, right } => {
            compile_mutation_row_comparison(left, *op, right, parameters, schema, alias, table)?
        }
        ExprKind::Like { .. } => {
            return Err(HawDBError::Semantic(
                "single-table mutation predicates do not support LIKE or ILIKE".to_string(),
            ));
        }
        ExprKind::InList {
            left,
            values,
            negated,
        } => {
            let mut predicates = values
                .iter()
                .map(|right| {
                    numeric::compile_comparison(
                        left,
                        if *negated {
                            SqlComparisonOp::NotEq
                        } else {
                            SqlComparisonOp::Eq
                        },
                        right,
                        parameters,
                        schema,
                        alias,
                        table,
                    )
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter();
            let first = predicates.next().ok_or_else(|| {
                HawDBError::Semantic("mutation IN list must not be empty".to_string())
            })?;
            predicates.fold(first, |left, right| {
                if *negated {
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
            if let Some(column) = expression.as_column() {
                validate_mutation_column(column, schema, alias, table)?;
                RelationalPredicate::IsNull {
                    column: column.name.clone(),
                    negated: *negated,
                }
            } else {
                RelationalPredicate::NumericIsNull {
                    expression: numeric::compile_numeric(
                        expression, parameters, schema, alias, table,
                    )?,
                    negated: *negated,
                }
            }
        }
        _ => {
            return Err(HawDBError::Semantic(
                "unsupported single-table mutation predicate".to_owned(),
            ));
        }
    })
}

fn compile_mutation_row_comparison(
    left: &[Expr],
    op: SqlComparisonOp,
    right: &[Expr],
    parameters: &[Value],
    schema: &RelationalTableSchema,
    alias: Option<&str>,
    table: &str,
) -> Result<RelationalPredicate> {
    use hawdb_storage::relational::RelationalRowComparisonField;

    if left.len() != right.len() || left.is_empty() {
        return Err(HawDBError::Semantic(
            "row comparison requires the same non-zero number of fields on both sides".to_string(),
        ));
    }
    let compare = |index: usize, field_op| {
        numeric::compile_comparison(
            &left[index],
            field_op,
            &right[index],
            parameters,
            schema,
            alias,
            table,
        )
    };
    let strict = match op {
        SqlComparisonOp::Lt | SqlComparisonOp::Lte => Some(SqlComparisonOp::Lt),
        SqlComparisonOp::Gt | SqlComparisonOp::Gte => Some(SqlComparisonOp::Gt),
        SqlComparisonOp::Eq | SqlComparisonOp::NotEq => None,
    };
    let fields = (0..left.len())
        .map(|index| {
            Ok(RelationalRowComparisonField {
                equality: compare(index, SqlComparisonOp::Eq)?,
                ordering: strict.map(|strict| compare(index, strict)).transpose()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RelationalPredicate::RowCompare {
        fields,
        op: compile_comparison_op(op),
    })
}

pub(super) fn validate_mutation_column(
    column: &hawdb_sql::SqlColumnRef,
    schema: &RelationalTableSchema,
    alias: Option<&str>,
    table: &str,
) -> Result<()> {
    if column
        .qualifier
        .as_deref()
        .is_some_and(|qualifier| Some(qualifier) != alias && qualifier != table)
    {
        return Err(HawDBError::Semantic(format!(
            "mutation predicate has unknown qualifier {}",
            column.qualifier.as_deref().unwrap_or_default()
        )));
    }
    if schema.column_position(&column.name).is_none() {
        return Err(HawDBError::Semantic(format!(
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

fn compile_schema_statement(
    statement: &SqlStatement,
    state: &RelationalState,
) -> Result<Vec<RelationalWrite>> {
    match statement {
        SqlStatement::CreateTable(create) => {
            let mut schema = compile_create_table(create)?;
            bind_foreign_keys(&mut schema, state)?;
            Ok(vec![RelationalWrite::CreateTable(schema)])
        }
        SqlStatement::CreateIndex(create) => Ok(vec![compile_create_index(create, state)?]),
        SqlStatement::AlterTableAddColumn(alter) => compile_add_column(alter),
        _ => Err(HawDBError::Semantic(
            "content-store schema corpus contains a non-schema statement".to_string(),
        )),
    }
}

fn bind_foreign_keys(schema: &mut RelationalTableSchema, state: &RelationalState) -> Result<()> {
    let mut foreign_keys = std::mem::take(&mut schema.foreign_keys);
    for key in &mut foreign_keys {
        let parent = if key.referenced_table == schema.name {
            &*schema
        } else {
            state.table_schema(&key.referenced_table).ok_or_else(|| {
                HawDBError::Semantic(format!(
                    "foreign key references unknown table {}",
                    key.referenced_table
                ))
            })?
        };
        if key.referenced_columns.is_empty() {
            key.referenced_columns = parent.primary_key.clone();
        }
        if key.columns.len() != key.referenced_columns.len() {
            return Err(HawDBError::Semantic(
                "foreign-key column counts must match".into(),
            ));
        }
        let referenced = key
            .referenced_columns
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        let ordered = std::iter::once(&parent.primary_key)
            .chain(parent.unique_constraints.iter())
            .chain(
                parent
                    .indexes
                    .iter()
                    .filter(|index| index.unique && index.predicate.is_none())
                    .map(|index| &index.columns),
            )
            .find(|columns| {
                columns.len() == key.referenced_columns.len()
                    && referenced.len() == columns.len()
                    && columns.iter().all(|column| referenced.contains(column))
            })
            .ok_or_else(|| {
                HawDBError::Semantic(format!(
                    "foreign key must reference a primary or unique key on {}",
                    key.referenced_table
                ))
            })?;
        // The storage contract uses index order for child support indexes,
        // parent probes, cascades and locks. Reorder the pairs together once,
        // before creating a durable schema or staging any row mutations.
        key.columns = ordered
            .iter()
            .map(|column| {
                let ordinal = key
                    .referenced_columns
                    .iter()
                    .position(|candidate| candidate == column)
                    .expect("referenced columns match a unique key");
                key.columns[ordinal].clone()
            })
            .collect();
        key.referenced_columns.clone_from(ordered);
    }
    schema.foreign_keys = foreign_keys;
    Ok(())
}

fn compile_create_table(create: &CreateTableStatement) -> Result<RelationalTableSchema> {
    reject_non_public_schema(create.table.schema.as_deref())?;
    if !matches!(create.storage, SqlTableStorage::RowPage) {
        return Err(HawDBError::Semantic(
            "RowPage compiler does not accept a strict append table".to_string(),
        ));
    }
    if create.if_not_exists {
        return Err(HawDBError::Semantic(
            "content-store schema must not hide drift with IF NOT EXISTS".to_string(),
        ));
    }
    let mut primary_key = Vec::new();
    let mut unique_constraints = Vec::new();
    let mut foreign_keys = Vec::new();
    for column in &create.columns {
        if column.primary_key {
            if !primary_key.is_empty() {
                return Err(HawDBError::Semantic(
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
    for constraint in &create.constraints {
        match constraint {
            SqlTableConstraint::PrimaryKey(columns) => {
                if !primary_key.is_empty() {
                    return Err(HawDBError::Semantic(
                        "table declares more than one primary key".to_string(),
                    ));
                }
                primary_key = columns.clone();
            }
            SqlTableConstraint::Unique(columns) => unique_constraints.push(columns.clone()),
            SqlTableConstraint::ForeignKey { columns, reference } => {
                foreign_keys.push(RelationalForeignKeySchema {
                    columns: columns.clone(),
                    referenced_table: reference.table.name.clone(),
                    referenced_columns: reference.columns.clone(),
                    on_delete: compile_delete_referential_action(reference.on_delete)?,
                    on_update: compile_update_referential_action(reference.on_update)?,
                });
            }
        }
    }
    let column_names = create
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut primary_key_columns = std::collections::BTreeSet::new();
    for column in &primary_key {
        if !column_names.contains(column.as_str()) || !primary_key_columns.insert(column.clone()) {
            return Err(HawDBError::Semantic(format!(
                "primary key references unknown or duplicate column {column}"
            )));
        }
    }
    let columns = create
        .columns
        .iter()
        .map(|column| {
            let is_primary_key = primary_key_columns.contains(&column.name);
            let mut column = compile_column(column.clone())?;
            if is_primary_key {
                column.nullable = false;
            }
            Ok(column)
        })
        .collect::<Result<_>>()?;
    Ok(RelationalTableSchema {
        materialized_view: None,
        name: create.table.name.clone(),
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
            RelationalValue::Uuid(hawdb_core::generate_uuidv7()?)
        }
        // Materialized here, before the row reaches the WAL, so recovery
        // replays the recorded instant instead of reading the clock again.
        Some(RelationalColumnDefault::CurrentTimestamp(kind)) => crate::coerce_relational_value(
            crate::query_value::value_to_relational(hawdb_core::sql_timestamp::current_timestamp(
                *kind,
            )?)?,
            column.scalar_type,
        )?,
    })
}

fn bind_relational_value_as(
    value: &SqlValue,
    parameters: &[Value],
    scalar_type: RelationalScalarType,
) -> Result<RelationalValue> {
    bind_relational_value(value.clone(), parameters)
        .and_then(|value| coerce_relational_value(value, scalar_type))
}

fn compile_delete_referential_action(
    action: SqlReferentialAction,
) -> Result<RelationalReferentialAction> {
    match action {
        SqlReferentialAction::NoAction => Ok(RelationalReferentialAction::NoAction),
        SqlReferentialAction::Restrict => Ok(RelationalReferentialAction::Restrict),
        SqlReferentialAction::Cascade => Ok(RelationalReferentialAction::Cascade),
        SqlReferentialAction::SetNull => Err(HawDBError::Semantic(
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
        SqlReferentialAction::Cascade | SqlReferentialAction::SetNull => Err(HawDBError::Semantic(
            "ON UPDATE CASCADE and SET NULL are not supported".to_string(),
        )),
    }
}

fn compile_create_index(
    create: &CreateIndexStatement,
    state: &RelationalState,
) -> Result<RelationalWrite> {
    reject_non_public_schema(create.table.schema.as_deref())?;
    if create.if_not_exists {
        return Err(HawDBError::Semantic(
            "content-store indexes must not hide drift with IF NOT EXISTS".to_string(),
        ));
    }
    if create.columns.iter().any(|column| {
        column.direction != hawdb_sql::SqlOrderDirection::Asc
            || column.nulls != hawdb_sql::SqlNullOrder::DialectDefault
    }) {
        return Err(HawDBError::Semantic(
            "CREATE INDEX ordering and NULLS clauses are not supported".to_string(),
        ));
    }
    let schema = state.table_schema(&create.table.name).ok_or_else(|| {
        HawDBError::Semantic(format!("unknown relational table {}", create.table.name))
    })?;
    let predicate = create
        .predicate
        .as_ref()
        .map(|predicate| {
            compile_mutation_predicate(predicate, &[], schema, None, &create.table.name)
        })
        .transpose()?;
    if let Some(predicate) = &predicate {
        validate_compiled_index_predicate(predicate, schema)?;
    }
    Ok(RelationalWrite::CreateIndex {
        table: create.table.name.clone(),
        index: RelationalIndexSchema {
            name: create.name.clone(),
            columns: create
                .columns
                .iter()
                .map(|column| column.column.name.clone())
                .collect(),
            included_columns: create.included_columns.clone(),
            unique: create.unique,
            predicate,
        },
    })
}

fn validate_compiled_index_predicate(
    predicate: &RelationalPredicate,
    schema: &RelationalTableSchema,
) -> Result<()> {
    match predicate {
        RelationalPredicate::And(left, right) | RelationalPredicate::Or(left, right) => {
            validate_compiled_index_predicate(left, schema)?;
            validate_compiled_index_predicate(right, schema)
        }
        RelationalPredicate::Not(predicate) => {
            validate_compiled_index_predicate(predicate, schema)
        }
        RelationalPredicate::RowCompare { .. } | RelationalPredicate::NumericCompare { .. } | RelationalPredicate::NumericIsNull { .. } => {
            Err(HawDBError::Semantic(
                "partial-index predicates support column-to-literal comparisons, IS NULL, AND, OR, and NOT only"
                    .into(),
            ))
        }
        RelationalPredicate::Compare { column, value, .. } => {
            let position = schema
                .column_position(column)
                .expect("compiled partial-index column was validated");
            if matches!(
                schema.columns[position].scalar_type,
                RelationalScalarType::Bytea | RelationalScalarType::Vector(_)
            ) || matches!(value, RelationalValue::DoublePrecision(value) if !value.is_finite())
            {
                return Err(HawDBError::Semantic(format!(
                    "partial-index comparison on column {column} uses an unsupported literal type"
                )));
            }
            Ok(())
        }
        RelationalPredicate::IsNull { .. } => Ok(()),
    }
}

fn compile_add_column(alter: &AlterTableAddColumnStatement) -> Result<Vec<RelationalWrite>> {
    reject_non_public_schema(alter.table.schema.as_deref())?;
    if alter.if_not_exists {
        return Err(HawDBError::Semantic(
            "system schema migrations must not hide ADD COLUMN drift with IF NOT EXISTS"
                .to_string(),
        ));
    }
    if alter.column.primary_key || alter.column.unique || alter.column.references.is_some() {
        return Err(HawDBError::Semantic(
            "ALTER TABLE ADD COLUMN does not support inline key, unique, or foreign-key constraints"
                .to_string(),
        ));
    }
    Ok(vec![RelationalWrite::AddColumn {
        table: alter.table.name.clone(),
        column: compile_column(alter.column.clone())?,
    }])
}

#[cfg(test)]
mod tests;
