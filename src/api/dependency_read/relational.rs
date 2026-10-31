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

//! Canonical SQL reads using existing row and declared-index identities.

use super::*;
use crate::sql::{
    Expr, ExprKind, SelectProjection, SelectStatement, SqlComparisonOp, SqlStatement, SqlValue,
};
use hawdb_relational::query_value::value_to_relational_as;
use hawdb_storage::relational::{RelationalKey, RelationalTableSchema, RelationalValue};

#[path = "relational/indexed.rs"]
mod indexed;
#[path = "relational/joined.rs"]
mod joined;

const MAX_INPUT_BYTES: usize = 64 * 1024;
const MAX_PARAMETERS: usize = 1024;
const MAX_TOKENS: usize = 4096;
const MAX_RECURSIVE_TOKENS: usize = 32;
const MAX_EXPRESSION_NODES: usize = 1024;
const MAX_EXPRESSION_DEPTH: usize = 64;
const MAX_KEY_COLUMNS: usize = 32;
const MAX_KEY_BYTES: usize = 4096;

pub(in crate::api) fn admit_transaction_input(text: &str, parameters: &[Value]) -> Result<()> {
    admit_input(text, parameters)?;
    crate::sql::admit_postgres_sql_syntax(text, MAX_INPUT_BYTES, MAX_TOKENS, MAX_RECURSIVE_TOKENS)
}

/// Capture from the transaction's current private relational view, without
/// re-executing the statement or replacing its ordinary query semantics.
pub(in crate::api) fn collect_transaction(
    text: &str,
    statement: &SqlStatement,
    parameters: &[Value],
    state: &hawdb_storage::relational::RelationalState,
    dependencies: &mut GraphReadDependencies,
) -> Result<()> {
    admit_input(text, parameters)?;
    let record = |dependencies: &mut GraphReadDependencies, key| {
        dependencies
            .record(key)
            .map_err(|error| HawDBError::Execution(error.to_string()))
    };
    match statement {
        SqlStatement::Select(select) => {
            qualify_expressions(select)?;
            let table = select
                .from
                .as_ref()
                .ok_or_else(|| refusal("FROM-less transaction SELECT"))?;
            transaction_schema(state, table)?;
            if system_sql::is_virtual_catalog_select(select) || select.joins.len() > MAX_KEY_COLUMNS
            {
                return Err(refusal("virtual or unbounded joined transaction SELECT"));
            }
            if !select.joins.is_empty() {
                // Private writes may change join keys. Complete table domains
                // cover both parent/child membership without querying a second
                // snapshot or guessing from the returned rows.
                for table in
                    std::iter::once(table).chain(select.joins.iter().map(|join| &join.table))
                {
                    record(
                        dependencies,
                        VersionKey::ReadRelationalTableDomain(
                            transaction_schema(state, table)?.name.clone(),
                        ),
                    )?;
                }
                return Ok(());
            }
            let schema = transaction_schema(state, table)?;
            let key = primary_key(select, schema, parameters)?;
            let key = if matches!(key, VersionKey::Database) {
                indexed::prefix_key(select, schema, parameters)?
                    .unwrap_or_else(|| VersionKey::ReadRelationalTableDomain(schema.name.clone()))
            } else {
                key
            };
            record(dependencies, key)
        }
        SqlStatement::Insert(statement) => {
            let key = VersionKey::ReadRelationalTableDomain(
                transaction_schema(state, &statement.table)?.name.clone(),
            );
            let schema = transaction_schema(state, &statement.table)?;
            if schema.columns.iter().enumerate().any(|(index, column)| {
                matches!(
                    column.default,
                    Some(hawdb_storage::relational::RelationalColumnDefault::UuidV7)
                ) && if statement.columns.is_empty() {
                    statement.rows.iter().any(|row| row.len() <= index)
                } else {
                    !statement.columns.contains(&column.name)
                }
            }) {
                return Err(refusal("unversioned generated UUID default"));
            }
            record(dependencies, key)
        }
        SqlStatement::Update(statement) => {
            if let Some(predicate) = &statement.selection {
                qualify_expression(predicate, &mut 0, 0)?;
            }
            record(
                dependencies,
                VersionKey::ReadRelationalTableDomain(
                    transaction_schema(state, &statement.table)?.name.clone(),
                ),
            )
        }
        SqlStatement::Delete(statement) => {
            if let Some(predicate) = &statement.selection {
                qualify_expression(predicate, &mut 0, 0)?;
            }
            record(
                dependencies,
                VersionKey::ReadRelationalTableDomain(
                    transaction_schema(state, &statement.table)?.name.clone(),
                ),
            )
        }
        SqlStatement::CreateTable(_)
        | SqlStatement::CreateMaterializedView(_)
        | SqlStatement::DropMaterializedView(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_) => record(dependencies, VersionKey::Database),
        SqlStatement::Explain(_) | SqlStatement::Branch(_) => {
            Err(refusal("noncanonical transaction statement"))
        }
    }
}

fn transaction_schema<'a>(
    state: &'a hawdb_storage::relational::RelationalState,
    table: &crate::sql::SqlTableName,
) -> Result<&'a RelationalTableSchema> {
    if table.name.len() > MAX_KEY_BYTES
        || table
            .schema
            .as_deref()
            .is_some_and(|schema| schema != "public")
    {
        return Err(refusal("noncanonical or unbounded transaction table"));
    }
    state
        .table_schema(&table.name)
        .filter(|schema| schema.columns.len() <= MAX_EXPRESSION_NODES)
        .ok_or_else(|| refusal("unknown or unbounded transaction schema"))
}

impl PreparedRead {
    /// Executes a bounded canonical SELECT. Necessary complete
    /// primary-key equalities under AND use existing row stamps, including
    /// absent keys. Necessary leading equalities of declared indexes record
    /// complete prefix domains, including payload and empty-page changes.
    /// One aliased INNER JOIN to a complete parent primary key collects the
    /// child scope and parent point, including absent/filtered parents. A pinned
    /// child point query may read its join fields (at most one row / 4 KiB).
    /// Other qualified single-table reads retain a complete table domain,
    /// including empty scans and rows outside a selected page. Unsupported
    /// qualified JOIN scopes retain their existing whole-epoch fallback.
    /// Other joins, functions, catalogs, projection bindings and locking reads fail
    /// closed. Metadata admission bounds SQL plus scalar parameters to 64 KiB,
    /// 1024 parameters/expression nodes and 64 nested expression levels. A lexical
    /// preflight caps 4096 non-whitespace tokens and 32 conservative recursive
    /// tokens (keywords, operators and opening delimiters) before parsing,
    /// lowering or dropping a potentially deep upstream AST.
    pub fn query_sql_with_params(
        &mut self,
        sql: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        let result = self.query_sql_inner(sql, parameters);
        self.failed |= result.is_err();
        result
    }

    fn query_sql_inner(&mut self, sql: &str, parameters: &[Value]) -> Result<QueryOutput> {
        admit_input(sql, parameters)?;
        crate::sql::admit_postgres_sql_syntax(
            sql,
            MAX_INPUT_BYTES,
            MAX_TOKENS,
            MAX_RECURSIVE_TOKENS,
        )?;
        if self.read.projection_relational.is_some() {
            return Err(refusal("projection-bound relational reads"));
        }
        let prepared = self.read.relational_plan_template_cache.prepare(sql)?;
        let SqlStatement::Select(select) = prepared.statement() else {
            return Err(refusal("non-SELECT statements"));
        };
        let Some(table) = &select.from else {
            return Err(refusal("FROM-less SELECT"));
        };
        if system_sql::is_virtual_catalog_select(select)
            || table
                .schema
                .as_deref()
                .is_some_and(|schema| schema != "public")
            || select.lock_strength.is_some()
        {
            return Err(refusal("catalog, join or locking SELECT"));
        }
        qualify_expressions(select)?;
        if !select.joins.is_empty() {
            for key in joined::keys(self, select, parameters)? {
                self.dependencies
                    .graph
                    .record(key)
                    .map_err(|error| HawDBError::Execution(error.to_string()))?;
            }
            return self
                .read
                .query_sql_with_prepared_params(sql, parameters, prepared);
        }
        let schema = self
            .read
            .store
            .relational_state()
            .table_schema(&table.name)
            .ok_or_else(|| refusal("noncanonical or unknown relational table"))?;
        let key = primary_key(select, schema, parameters)?;
        let key = if matches!(key, VersionKey::Database) {
            indexed::prefix_key(select, schema, parameters)?
                .unwrap_or_else(|| VersionKey::ReadRelationalTableDomain(schema.name.clone()))
        } else {
            key
        };
        self.dependencies
            .graph
            .record(key)
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        self.read
            .query_sql_with_prepared_params(sql, parameters, prepared)
    }
}

fn refusal(shape: &str) -> HawDBError {
    HawDBError::Execution(format!(
        "prepared SQL dependency collector does not qualify {shape}"
    ))
}

fn scalar_bytes(value: &Value) -> Result<usize> {
    match value {
        Value::String(value) => Ok(value.len()),
        Value::Binary(value) => Ok(value.len()),
        Value::Vector(value) => Ok(value.encoded().len()),
        Value::Null
        | Value::Bool(_)
        | Value::Int(_)
        | Value::Float(_)
        | Value::Uuid(_)
        | Value::Date(_)
        | Value::LocalTime(_)
        | Value::LocalDateTime(_)
        | Value::DateTime(_)
        | Value::Duration(_) => Ok(16),
        Value::List(_) | Value::Map(_) => Err(refusal("compound SQL parameter values")),
    }
}

fn admit_input(sql: &str, parameters: &[Value]) -> Result<()> {
    if sql.len() > MAX_INPUT_BYTES || parameters.len() > MAX_PARAMETERS {
        return Err(refusal("SQL input exceeding metadata admission"));
    }
    let mut bytes = sql.len().saturating_add(
        parameters
            .len()
            .saturating_mul(std::mem::size_of::<Value>()),
    );
    for parameter in parameters {
        bytes = bytes.saturating_add(scalar_bytes(parameter)?);
        if bytes > MAX_INPUT_BYTES {
            return Err(refusal("SQL input exceeding metadata admission"));
        }
    }
    Ok(())
}

fn qualify_expressions(select: &SelectStatement) -> Result<()> {
    let mut nodes = 0usize;
    for projection in &select.projection {
        if let SelectProjection::Expression { expression, .. } = projection {
            qualify_expression(expression, &mut nodes, 0)?;
        }
    }
    for expression in select.joins.iter().map(|join| &join.on) {
        qualify_expression(expression, &mut nodes, 0)?;
    }
    for expression in select
        .selection
        .iter()
        .chain(select.having.iter())
        .chain(select.order_by.iter().map(|item| &item.expression))
    {
        qualify_expression(expression, &mut nodes, 0)?;
    }
    Ok(())
}

fn qualify_expression(expression: &Expr, nodes: &mut usize, depth: usize) -> Result<()> {
    *nodes = nodes.saturating_add(1);
    if *nodes > MAX_EXPRESSION_NODES || depth > MAX_EXPRESSION_DEPTH {
        return Err(refusal("expression exceeding metadata work admission"));
    }
    match &expression.kind {
        ExprKind::Column(_) | ExprKind::Value(_) => Ok(()),
        ExprKind::And(left, right)
        | ExprKind::Or(left, right)
        | ExprKind::Compare { left, right, .. }
        | ExprKind::Like {
            left,
            pattern: right,
            ..
        } => {
            qualify_expression(left, nodes, depth + 1)?;
            qualify_expression(right, nodes, depth + 1)
        }
        ExprKind::Not(expression) | ExprKind::IsNull { expression, .. } => {
            qualify_expression(expression, nodes, depth + 1)
        }
        ExprKind::InList { left, values, .. } => {
            qualify_expression(left, nodes, depth + 1)?;
            for value in values {
                qualify_expression(value, nodes, depth + 1)?;
            }
            Ok(())
        }
        ExprKind::Function { .. } => Err(refusal("function expressions")),
    }
}

fn primary_key(
    select: &SelectStatement,
    schema: &RelationalTableSchema,
    parameters: &[Value],
) -> Result<VersionKey> {
    if schema.primary_key.is_empty() || schema.primary_key.len() > MAX_KEY_COLUMNS {
        return Ok(VersionKey::Database);
    }
    let mut bytes = std::mem::size_of::<VersionKey>()
        .saturating_add(schema.name.len())
        .saturating_add(
            schema
                .primary_key
                .len()
                .saturating_mul(std::mem::size_of::<RelationalValue>()),
        );
    let mut values = Vec::new();
    for name in &schema.primary_key {
        let Some(value) = select
            .selection
            .as_ref()
            .and_then(|predicate| necessary_equality(predicate, select, name))
        else {
            return Ok(VersionKey::Database);
        };
        let value = match value {
            SqlValue::Literal(value) => value,
            SqlValue::Parameter(position) => {
                parameters.get(position.saturating_sub(1)).ok_or_else(|| {
                    HawDBError::Semantic(format!("missing PostgreSQL parameter ${position}"))
                })?
            }
        };
        bytes = bytes.saturating_add(scalar_bytes(value)?);
        if bytes > MAX_KEY_BYTES {
            return Ok(VersionKey::Database);
        }
        let column = schema
            .column_position(name)
            .ok_or_else(|| refusal("invalid primary-key schema"))?;
        let scalar_type = schema.columns[column].scalar_type;
        let value = value_to_relational_as(value.clone(), scalar_type)?;
        if matches!(value, RelationalValue::Null) {
            return Ok(VersionKey::Database);
        }
        if value.scalar_type() != Some(scalar_type) {
            return Err(refusal(
                "primary-key comparison with incompatible scalar types",
            ));
        }
        values.push(value);
    }
    Ok(VersionKey::RelationalRow {
        table: schema.name.clone(),
        primary_key: RelationalKey(values),
    })
}

fn necessary_equality<'a>(
    expression: &'a Expr,
    select: &SelectStatement,
    name: &str,
) -> Option<&'a SqlValue> {
    match &expression.kind {
        ExprKind::And(left, right) => necessary_equality(left, select, name)
            .or_else(|| necessary_equality(right, select, name)),
        ExprKind::Compare {
            left,
            op: SqlComparisonOp::Eq,
            right,
        } => {
            let column = left.as_column()?;
            (column.name == name
                && (if select.joins.is_empty() {
                    column.qualifier.as_deref().is_none_or(|qualifier| {
                        qualifier == select.from_table().name
                            || Some(qualifier) == select.from_alias.as_deref()
                    })
                } else {
                    column.qualifier.as_deref() == select.from_alias.as_deref()
                }))
            .then(|| right.as_value())
            .flatten()
        }
        _ => None,
    }
}
