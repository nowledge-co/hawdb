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

use crate::{bind_relational_value, compile_column, reject_non_public_schema};
use hawdb_core::{HawDBError, Result, Value};
use hawdb_sql::{
    CreateTableStatement, SelectProjection, SelectStatement, SqlBound, SqlComparisonOp,
    SqlGeneratedOrder, SqlNullOrder, SqlOrderDirection, SqlPredicate, SqlStatement,
    SqlTableStorage, SqlValue,
};
use hawdb_sql::{Expr, ExprKind};
use hawdb_storage::{
    AppendGeneratedRow, AppendOrderMode, AppendState, AppendTableRow, AppendTableSchema,
    AppendTransaction, AppendWrite, RelationalColumnDefault, RelationalColumnSchema, RelationalRow,
    RelationalValue,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendSelectPlan {
    pub table: String,
    pub partition: hawdb_storage::RelationalKey,
    pub after: Option<hawdb_storage::RelationalKey>,
    pub max_rows: usize,
    projection: Vec<AppendProjection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendExplainPlan {
    pub select: AppendSelectPlan,
    pub analyze: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppendProjection {
    position: usize,
    output_name: String,
}

pub fn compile_append_statement_sql(
    sql: &str,
    parameters: &[Value],
    state: &AppendState,
) -> Result<Option<AppendTransaction>> {
    let prepared = hawdb_sql::prepare_postgres_sql(sql)?;
    if prepared.parameters.len() != parameters.len() {
        return Err(HawDBError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    match prepared.statement {
        SqlStatement::CreateTable(create)
            if matches!(create.storage, SqlTableStorage::StrictAppend { .. }) =>
        {
            Ok(Some(AppendTransaction {
                writes: vec![AppendWrite::CreateTable {
                    schema: compile_append_table(create)?,
                }],
            }))
        }
        SqlStatement::Insert(insert) if state.schema(&insert.table.name).is_some() => {
            reject_non_public_schema(insert.table.schema.as_deref())?;
            if !insert.returning.is_empty() {
                return Err(HawDBError::Semantic(
                    "strict append INSERT does not support RETURNING; use the typed Rust commit result for generated order keys"
                        .to_string(),
                ));
            }
            if insert.on_conflict.is_some() {
                return Err(HawDBError::Semantic(
                    "strict append INSERT does not support ON CONFLICT".to_string(),
                ));
            }
            let schema = state
                .schema(&insert.table.name)
                .expect("append table existence was checked");
            let write = match schema.order_mode {
                AppendOrderMode::CallerProvided => AppendWrite::Append {
                    table: insert.table.name,
                    rows: compile_insert_rows(
                        &insert.columns,
                        insert.rows,
                        parameters,
                        &schema.name,
                        &schema.columns,
                    )?,
                },
                AppendOrderMode::CommitSequence => AppendWrite::AppendGenerated {
                    table: insert.table.name,
                    rows: compile_generated_insert_rows(
                        &insert.columns,
                        insert.rows,
                        parameters,
                        schema,
                    )?,
                },
            };
            Ok(Some(AppendTransaction {
                writes: vec![write],
            }))
        }
        SqlStatement::Update(update) if state.schema(&update.table.name).is_some() => Err(
            HawDBError::Semantic("strict append tables do not support UPDATE".to_string()),
        ),
        SqlStatement::Delete(delete) if state.schema(&delete.table.name).is_some() => Err(
            HawDBError::Semantic("strict append tables do not support DELETE".to_string()),
        ),
        SqlStatement::CreateIndex(create) if state.schema(&create.table.name).is_some() => Err(
            HawDBError::Semantic("strict append tables do not support CREATE INDEX".to_string()),
        ),
        SqlStatement::AlterTableAddColumn(alter) if state.schema(&alter.table.name).is_some() => {
            Err(HawDBError::Semantic(
                "strict append tables do not support ALTER TABLE".to_string(),
            ))
        }
        _ => Ok(None),
    }
}

fn compile_append_table(create: CreateTableStatement) -> Result<AppendTableSchema> {
    reject_non_public_schema(create.table.schema.as_deref())?;
    if create.if_not_exists {
        return Err(HawDBError::Semantic(
            "strict append schema must not hide drift with IF NOT EXISTS".to_string(),
        ));
    }
    let SqlTableStorage::StrictAppend {
        partition_key,
        order_key,
        generated_order,
    } = create.storage
    else {
        return Err(HawDBError::Semantic(
            "strict append compiler requires strict_append storage".to_string(),
        ));
    };
    if partition_key.len() != 1 || order_key.len() != 1 {
        return Err(HawDBError::Semantic(
            "strict append SQL currently requires one partition key and one order key column"
                .to_string(),
        ));
    }
    if !create.constraints.is_empty()
        || create
            .columns
            .iter()
            .any(|column| column.primary_key || column.unique || column.references.is_some())
    {
        return Err(HawDBError::Semantic(
            "strict append tables do not support primary, unique, or foreign-key constraints"
                .to_string(),
        ));
    }
    Ok(AppendTableSchema {
        name: create.table.name,
        columns: create
            .columns
            .into_iter()
            .map(compile_column)
            .collect::<Result<_>>()?,
        partition_key,
        order_key,
        order_mode: match generated_order {
            SqlGeneratedOrder::CallerProvided => AppendOrderMode::CallerProvided,
            SqlGeneratedOrder::CommitSequence => AppendOrderMode::CommitSequence,
        },
    })
}

fn compile_insert_rows(
    insert_columns: &[String],
    input_rows: Vec<Vec<SqlValue>>,
    parameters: &[Value],
    table_name: &str,
    columns: &[RelationalColumnSchema],
) -> Result<Vec<RelationalRow>> {
    compile_insert_values(insert_columns, input_rows, parameters, table_name, columns)
        .map(|rows| rows.into_iter().map(RelationalRow::new).collect())
}

fn compile_generated_insert_rows(
    insert_columns: &[String],
    input_rows: Vec<Vec<SqlValue>>,
    parameters: &[Value],
    schema: &AppendTableSchema,
) -> Result<Vec<AppendGeneratedRow>> {
    let order_column = schema.order_key.first().ok_or_else(|| {
        HawDBError::Semantic(format!(
            "generated-order table {} has no order-key column",
            schema.name
        ))
    })?;
    if insert_columns.iter().any(|column| column == order_column) {
        return Err(HawDBError::Semantic(format!(
            "generated-order column {order_column} cannot be supplied by INSERT"
        )));
    }
    let caller_columns = schema
        .columns
        .iter()
        .filter(|column| column.name != *order_column)
        .cloned()
        .collect::<Vec<_>>();
    compile_insert_values(
        insert_columns,
        input_rows,
        parameters,
        &schema.name,
        &caller_columns,
    )
    .map(|rows| rows.into_iter().map(AppendGeneratedRow::new).collect())
}

fn compile_insert_values(
    insert_columns: &[String],
    input_rows: Vec<Vec<SqlValue>>,
    parameters: &[Value],
    table_name: &str,
    columns: &[RelationalColumnSchema],
) -> Result<Vec<Vec<RelationalValue>>> {
    let mut positions = Vec::with_capacity(insert_columns.len());
    let mut unique = std::collections::BTreeSet::new();
    for column in insert_columns {
        let position = columns
            .iter()
            .position(|candidate| candidate.name == *column)
            .ok_or_else(|| {
                HawDBError::Semantic(format!("table {table_name} has no column {column}"))
            })?;
        if !unique.insert(position) {
            return Err(HawDBError::Semantic(format!(
                "INSERT column {column} is specified more than once"
            )));
        }
        positions.push(position);
    }
    input_rows
        .into_iter()
        .map(|values| {
            if values.len() != insert_columns.len() {
                return Err(HawDBError::Semantic(format!(
                    "INSERT into table {table_name} names {} columns but row contains {} values",
                    insert_columns.len(),
                    values.len()
                )));
            }
            let mut row = columns
                .iter()
                .map(|column| match &column.default {
                    None => Ok(RelationalValue::Null),
                    Some(RelationalColumnDefault::Literal(value)) => Ok(value.clone()),
                    Some(RelationalColumnDefault::UuidV7) => Err(HawDBError::Semantic(format!(
                        "append table column {} does not support uuidv7() defaults",
                        column.name
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            for ((position, value), column_name) in
                positions.iter().zip(values).zip(insert_columns.iter())
            {
                row[*position] = bind_relational_value(value, parameters).map_err(|error| {
                    HawDBError::Semantic(format!(
                        "failed to bind INSERT column {column_name}: {error}"
                    ))
                })?;
            }
            Ok(row)
        })
        .collect()
}

pub fn compile_append_select_sql(
    sql: &str,
    parameters: &[Value],
    state: &AppendState,
    configured_max_rows: usize,
) -> Result<Option<AppendSelectPlan>> {
    let prepared = hawdb_sql::prepare_postgres_sql(sql)?;
    if prepared.parameters.len() != parameters.len() {
        return Err(HawDBError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    let SqlStatement::Select(select) = prepared.statement else {
        return Ok(None);
    };
    let Some(schema) = state.schema(&select.from.name) else {
        return Ok(None);
    };
    compile_append_select(select, parameters, schema, configured_max_rows).map(Some)
}

pub fn compile_append_explain_sql(
    sql: &str,
    parameters: &[Value],
    state: &AppendState,
    configured_max_rows: usize,
) -> Result<Option<AppendExplainPlan>> {
    let prepared = hawdb_sql::prepare_postgres_sql(sql)?;
    if prepared.parameters.len() != parameters.len() {
        return Err(HawDBError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    let SqlStatement::Explain(explain) = prepared.statement else {
        return Ok(None);
    };
    let SqlStatement::Select(select) = *explain.statement else {
        return Ok(None);
    };
    let Some(schema) = state.schema(&select.from.name) else {
        return Ok(None);
    };
    Ok(Some(AppendExplainPlan {
        select: compile_append_select(select, parameters, schema, configured_max_rows)?,
        analyze: explain.analyze,
    }))
}

fn compile_append_select(
    select: SelectStatement,
    parameters: &[Value],
    schema: &AppendTableSchema,
    configured_max_rows: usize,
) -> Result<AppendSelectPlan> {
    reject_non_public_schema(select.from.schema.as_deref())?;
    if select.distinct
        || !select.joins.is_empty()
        || !select.group_by.is_empty()
        || select.having.is_some()
        || select.offset.is_some()
        || select.lock_strength.is_some()
    {
        return Err(HawDBError::Semantic(
            "strict append SELECT supports only a single bounded table scan".to_string(),
        ));
    }
    if schema.partition_key.len() != 1 || schema.order_key.len() != 1 {
        return Err(HawDBError::Semantic(
            "strict append SQL SELECT currently requires single-column partition and order keys"
                .to_string(),
        ));
    }
    let expected_order = &schema.order_key[0];
    if !matches!(
        select.order_by.as_slice(),
        [order]
            if order.expression.as_column().is_some_and(|column| column.name == *expected_order
                && qualifier_matches(&column.qualifier, select.from_alias.as_deref(), &schema.name))
                && order.direction == SqlOrderDirection::Asc
                && matches!(order.nulls, SqlNullOrder::DialectDefault | SqlNullOrder::Last)
    ) {
        return Err(HawDBError::Semantic(format!(
            "strict append SELECT requires ORDER BY {expected_order} ASC"
        )));
    }
    let requested = bind_append_bound(select.limit, parameters, "LIMIT")?.ok_or_else(|| {
        HawDBError::Semantic("strict append SELECT requires an explicit LIMIT".to_string())
    })?;
    let max_rows = usize::try_from(requested)
        .unwrap_or(usize::MAX)
        .min(configured_max_rows);
    if max_rows == 0 || requested > configured_max_rows as u64 {
        return Err(HawDBError::Semantic(format!(
            "strict append SELECT LIMIT must be between 1 and {configured_max_rows}"
        )));
    }
    let mut partition = None;
    let mut after = None;
    collect_append_predicates(
        select.selection.as_ref().ok_or_else(|| {
            HawDBError::Semantic(
                "strict append SELECT requires an exact partition predicate".to_string(),
            )
        })?,
        parameters,
        schema,
        select.from_alias.as_deref(),
        &mut partition,
        &mut after,
    )?;
    let partition = partition.ok_or_else(|| {
        HawDBError::Semantic(format!(
            "strict append SELECT requires {} = <value>",
            schema.partition_key[0]
        ))
    })?;
    let projection =
        compile_append_projection(&select.projection, schema, select.from_alias.as_deref())?;
    Ok(AppendSelectPlan {
        table: schema.name.clone(),
        partition: hawdb_storage::RelationalKey(vec![partition]),
        after: after.map(|value| hawdb_storage::RelationalKey(vec![value])),
        max_rows,
        projection,
    })
}

fn collect_append_predicates(
    predicate: &SqlPredicate,
    parameters: &[Value],
    schema: &AppendTableSchema,
    alias: Option<&str>,
    partition: &mut Option<RelationalValue>,
    after: &mut Option<RelationalValue>,
) -> Result<()> {
    if let Expr {
        kind: ExprKind::And(left, right),
        ..
    } = predicate
    {
        collect_append_predicates(left, parameters, schema, alias, partition, after)?;
        return collect_append_predicates(right, parameters, schema, alias, partition, after);
    }
    let Expr {
        kind: ExprKind::Compare { left, op, right },
        ..
    } = predicate
    else {
        return Err(HawDBError::Semantic(
            "strict append SELECT predicates must be key comparisons joined by AND".to_string(),
        ));
    };
    let (Some(left), Some(right)) = (left.as_column(), right.as_value()) else {
        return Err(HawDBError::Semantic(
            "strict append SELECT predicates must be key comparisons joined by AND".to_owned(),
        ));
    };
    if !qualifier_matches(&left.qualifier, alias, &schema.name) {
        return Err(HawDBError::Semantic(format!(
            "strict append SELECT has unknown qualifier {}",
            left.qualifier.as_deref().unwrap_or_default()
        )));
    }
    let value = bind_relational_value(right.clone(), parameters)?;
    let (target, key_kind) = if left.name == schema.partition_key[0] && *op == SqlComparisonOp::Eq {
        (partition, "partition")
    } else if left.name == schema.order_key[0] && *op == SqlComparisonOp::Gt {
        (after, "order")
    } else {
        return Err(HawDBError::Semantic(format!(
            "strict append SELECT supports {} = <value> and optional {} > <value>",
            schema.partition_key[0], schema.order_key[0]
        )));
    };
    let column = schema
        .columns
        .iter()
        .find(|column| column.name == left.name)
        .expect("strict append keys reference schema columns");
    if matches!(value, RelationalValue::Null | RelationalValue::Overflow(_))
        || value.scalar_type() != Some(column.scalar_type)
    {
        return Err(HawDBError::Semantic(format!(
            "strict append {key_kind} key {} expects {:?}",
            column.name, column.scalar_type
        )));
    }
    if target.replace(value).is_some() {
        return Err(HawDBError::Semantic(format!(
            "strict append SELECT repeats predicate for column {}",
            left.name
        )));
    }
    Ok(())
}

fn qualifier_matches(qualifier: &Option<String>, alias: Option<&str>, table: &str) -> bool {
    qualifier.as_deref().is_none_or(|qualifier| match alias {
        Some(alias) => qualifier == alias,
        None => qualifier == table,
    })
}

fn compile_append_projection(
    projection: &[SelectProjection],
    schema: &AppendTableSchema,
    alias: Option<&str>,
) -> Result<Vec<AppendProjection>> {
    let mut output = Vec::new();
    let mut output_names = std::collections::BTreeSet::new();
    for item in projection {
        match item {
            SelectProjection::Wildcard => {
                for (position, column) in schema.columns.iter().enumerate() {
                    if !output_names.insert(column.name.clone()) {
                        return Err(HawDBError::Semantic(format!(
                            "strict append projection contains duplicate output column {}",
                            column.name
                        )));
                    }
                    output.push(AppendProjection {
                        position,
                        output_name: column.name.clone(),
                    });
                }
            }
            SelectProjection::Expression {
                expression:
                    Expr {
                        kind: ExprKind::Column(name),
                        ..
                    },
                alias: output_alias,
                ..
            } => {
                if !qualifier_matches(&name.qualifier, alias, &schema.name) {
                    return Err(HawDBError::Semantic(format!(
                        "strict append SELECT has unknown qualifier {}",
                        name.qualifier.as_deref().unwrap_or_default()
                    )));
                }
                let position = schema.column_position(&name.name).ok_or_else(|| {
                    HawDBError::Semantic(format!(
                        "table {} has no column {}",
                        schema.name, name.name
                    ))
                })?;
                let output_name = output_alias.clone().unwrap_or_else(|| name.name.clone());
                if !output_names.insert(output_name.clone()) {
                    return Err(HawDBError::Semantic(format!(
                        "strict append projection contains duplicate output column {output_name}"
                    )));
                }
                output.push(AppendProjection {
                    position,
                    output_name,
                });
            }
            SelectProjection::Expression { .. } => {
                return Err(HawDBError::Semantic(
                    "strict append SELECT does not support projection expressions".to_string(),
                ));
            }
        }
    }
    Ok(output)
}

pub fn project_append_rows(
    plan: &AppendSelectPlan,
    rows: &[AppendTableRow],
) -> Result<Vec<std::collections::BTreeMap<String, Value>>> {
    rows.iter()
        .map(|row| {
            plan.projection
                .iter()
                .map(|projection| {
                    let value = row.row.values().get(projection.position).ok_or_else(|| {
                        HawDBError::StorageIntegrity(
                            "strict append row does not match its table schema".to_string(),
                        )
                    })?;
                    Ok((
                        projection.output_name.clone(),
                        append_value_to_value(value)?,
                    ))
                })
                .collect()
        })
        .collect()
}

pub fn format_append_explain(
    plan: &AppendExplainPlan,
    report: Option<&hawdb_storage::AppendSegmentReadReport>,
) -> Vec<std::collections::BTreeMap<String, Value>> {
    let mut row = std::collections::BTreeMap::from([
        (
            "id".to_string(),
            Value::String("StrictAppendPartitionScan_1".to_string()),
        ),
        (
            "estRows".to_string(),
            Value::Int(i64::try_from(plan.select.max_rows).unwrap_or(i64::MAX)),
        ),
        ("task".to_string(), Value::String("root".to_string())),
        (
            "access object".to_string(),
            Value::String(format!("table:{}", plan.select.table)),
        ),
        (
            "operator info".to_string(),
            Value::String(format!(
                "partition=exact, after={}, order=ascending, limit={}",
                if plan.select.after.is_some() {
                    "exclusive"
                } else {
                    "none"
                },
                plan.select.max_rows
            )),
        ),
    ]);
    if plan.analyze {
        let report = report.copied().unwrap_or_default();
        row.insert(
            "actRows".to_string(),
            Value::Int(i64::try_from(report.rows_returned).unwrap_or(i64::MAX)),
        );
        row.insert(
            "execution info".to_string(),
            Value::String(format!(
                "segments_examined={}, segments_pruned={}, blocks_read={}, rows_decoded={}, live_batches_examined={}, live_batches_pruned={}, live_rows_examined={}, output_payload_bytes={}",
                report.segments_examined,
                report.segments_pruned,
                report.blocks_read,
                report.rows_decoded,
                report.live_batches_examined,
                report.live_batches_pruned,
                report.live_rows_examined,
                report.output_payload_bytes,
            )),
        );
        row.insert("memory".to_string(), Value::Null);
        row.insert("disk".to_string(), Value::Null);
    }
    vec![row]
}

fn bind_append_bound(
    bound: Option<SqlBound>,
    parameters: &[Value],
    name: &str,
) -> Result<Option<u64>> {
    bound
        .map(|bound| match bound {
            SqlBound::Literal(value) => Ok(value),
            SqlBound::Parameter(position) => match parameters.get(position.saturating_sub(1)) {
                Some(Value::Int(value)) if *value >= 0 => Ok(*value as u64),
                Some(_) => Err(HawDBError::Semantic(format!(
                    "PostgreSQL {name} parameter ${position} must be a non-negative integer"
                ))),
                None => Err(HawDBError::Semantic(format!(
                    "missing PostgreSQL parameter ${position}"
                ))),
            },
        })
        .transpose()
}

fn append_value_to_value(value: &RelationalValue) -> Result<Value> {
    match value {
        RelationalValue::Null => Ok(Value::Null),
        RelationalValue::Boolean(value) => Ok(Value::Bool(*value)),
        RelationalValue::BigInt(value) => Ok(Value::Int(*value)),
        RelationalValue::DoublePrecision(value) => Ok(Value::Float(*value)),
        RelationalValue::Text(value) => Ok(Value::String(value.clone())),
        RelationalValue::Bytea(value) => Ok(Value::Binary(value.clone())),
        RelationalValue::Uuid(value) => Ok(Value::Uuid(*value)),
        RelationalValue::Overflow(_) => Err(HawDBError::StorageIntegrity(
            "strict append row contains an overflow reference".to_string(),
        )),
    }
}
