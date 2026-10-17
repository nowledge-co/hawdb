// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! One aliased INNER JOIN from a bounded child domain to a complete parent PK.
//! The original SQL still executes in the pinned runtime, including residuals.
use super::*;
use crate::sql::{SqlColumnRef, SqlJoinKind};
use hawdb_relational::query_value::relational_to_value;

pub(super) fn keys(
    read: &PreparedRead,
    select: &SelectStatement,
    parameters: &[Value],
) -> Result<Vec<VersionKey>> {
    let [join] = select.joins.as_slice() else {
        return Err(refusal("multiple joins"));
    };
    let (Some(child_alias), Some(parent_alias)) =
        (select.from_alias.as_deref(), join.alias.as_deref())
    else {
        return Err(refusal("joins without explicit aliases"));
    };
    if join.kind != SqlJoinKind::Inner
        || join.on_scope_start != 0
        || child_alias == parent_alias
        || join.table.schema.as_deref().is_some_and(|s| s != "public")
        || matches!(join.table.name.as_str(), "pg_tables" | "pg_indexes")
    {
        return Err(refusal("noncanonical or non-inner join"));
    }
    let state = read.read.store.relational_state();
    let child = state
        .table_schema(&select.from_table().name)
        .ok_or_else(|| refusal("unknown join child"))?;
    let parent = state
        .table_schema(&join.table.name)
        .ok_or_else(|| refusal("unknown join parent"))?;
    if child_alias == parent.name || parent_alias == child.name {
        return Err(refusal("join aliases colliding with another table name"));
    }
    if parent.primary_key.is_empty() || parent.primary_key.len() > MAX_KEY_COLUMNS {
        return Err(refusal("join without bounded parent primary key"));
    }
    let mut pairs = Vec::new();
    equalities(&join.on, child_alias, parent_alias, &mut pairs)?;
    if pairs.len() != parent.primary_key.len() {
        return Err(refusal("incomplete parent primary-key join"));
    }
    let mut columns = Vec::new();
    let mut compatible = true;
    for name in &parent.primary_key {
        let mut candidates = pairs.iter().filter(|(_, p)| p.name == *name);
        let Some((c, p)) = candidates.next() else {
            return Err(refusal("incomplete parent primary-key join"));
        };
        if candidates.next().is_some() {
            return Err(refusal("duplicate parent join column"));
        }
        let c = child
            .column_position(&c.name)
            .ok_or_else(|| refusal("unknown child join column"))?;
        let p = parent
            .column_position(&p.name)
            .ok_or_else(|| refusal("unknown parent join column"))?;
        compatible &= child.columns[c].scalar_type == parent.columns[p].scalar_type;
        columns.push(&child.columns[c].name);
    }
    if !compatible {
        return Ok(vec![VersionKey::Database]);
    }
    let root = primary_key(select, child, parameters)?;
    let root = if matches!(root, VersionKey::Database) {
        indexed::prefix_key(select, child, parameters)?.unwrap_or(root)
    } else {
        root
    };
    if matches!(root, VersionKey::Database) {
        return Ok(vec![root]);
    }
    if let VersionKey::RelationalRow { primary_key, .. } = &root
        && primary_key
            .0
            .iter()
            .any(|v| matches!(v, RelationalValue::DoublePrecision(_)))
    {
        return Ok(vec![VersionKey::Database]);
    }
    let mut bound = Vec::new();
    let mut bound_bytes = std::mem::size_of::<VersionKey>()
        .saturating_add(parent.name.len())
        .saturating_add(std::mem::size_of::<RelationalKey>());
    for name in &columns {
        let Some(value) = select
            .selection
            .as_ref()
            .and_then(|p| necessary_equality(p, select, name))
        else {
            break;
        };
        let value = match value {
            SqlValue::Literal(value) => value,
            SqlValue::Parameter(position) => parameters
                .get(position.saturating_sub(1))
                .ok_or_else(|| refusal("missing join parameter"))?,
        };
        bound_bytes = bound_bytes
            .saturating_add(scalar_bytes(value)?)
            .saturating_add(std::mem::size_of::<RelationalValue>());
        if bound_bytes > MAX_KEY_BYTES {
            return Ok(vec![VersionKey::Database]);
        }
        bound.push(value.clone());
    }
    if bound.len() != columns.len() {
        let VersionKey::RelationalRow { primary_key, .. } = &root else {
            return Err(refusal("join parent not fixed by a necessary child tuple"));
        };
        let output = child_key_query(read, child, &columns, primary_key)?;
        let Some(row) = output.rows.first() else {
            // An absent child cannot join; its exact absent key protects this.
            return Ok(vec![root]);
        };
        bound.clear();
        for ordinal in 0..columns.len() {
            bound.push(
                row.get(&format!("__hawdb_join_{ordinal}"))
                    .ok_or_else(|| refusal("missing internal join-key output"))?
                    .clone(),
            );
        }
    }
    let mut result = vec![root];
    match parent_key(parent, &bound)? {
        // NULL join components cannot match any parent; the child scope suffices.
        None => {}
        Some(key) => result.push(key),
    }
    Ok(result)
}

fn equalities<'a>(
    expression: &'a Expr,
    child: &str,
    parent: &str,
    pairs: &mut Vec<(&'a SqlColumnRef, &'a SqlColumnRef)>,
) -> Result<()> {
    match &expression.kind {
        ExprKind::And(left, right) => {
            equalities(left, child, parent, pairs)?;
            equalities(right, child, parent, pairs)
        }
        ExprKind::Compare {
            left,
            op: SqlComparisonOp::Eq,
            right,
        } => {
            let Some((left, right)) = left.as_column().zip(right.as_column()) else {
                return Err(refusal("non-column join equality"));
            };
            let pair = match (left.qualifier.as_deref(), right.qualifier.as_deref()) {
                (Some(l), Some(r)) if l == child && r == parent => (left, right),
                (Some(l), Some(r)) if l == parent && r == child => (right, left),
                _ => return Err(refusal("join equality outside explicit aliases")),
            };
            if pairs.len() >= MAX_KEY_COLUMNS {
                return Err(refusal("oversized join tuple"));
            }
            pairs.push(pair);
            Ok(())
        }
        _ => Err(refusal("non-conjunctive primary-key join")),
    }
}

fn parent_key(schema: &RelationalTableSchema, values: &[Value]) -> Result<Option<VersionKey>> {
    if values.len() != schema.primary_key.len() {
        return Err(refusal("incomplete inferred parent tuple"));
    }
    let mut bytes = std::mem::size_of::<VersionKey>()
        .saturating_add(schema.name.len())
        .saturating_add(std::mem::size_of::<RelationalKey>());
    let mut tuple = Vec::new();
    for (name, value) in schema.primary_key.iter().zip(values) {
        bytes = bytes
            .saturating_add(scalar_bytes(value)?)
            .saturating_add(std::mem::size_of::<RelationalValue>());
        if bytes > MAX_KEY_BYTES {
            return Ok(Some(VersionKey::Database));
        }
        let column = &schema.columns[schema
            .column_position(name)
            .ok_or_else(|| refusal("invalid parent primary key"))?];
        let value = value_to_relational_as(value.clone(), column.scalar_type)?;
        if matches!(value, RelationalValue::Null) {
            return Ok(None);
        }
        if matches!(
            value,
            RelationalValue::DoublePrecision(_) | RelationalValue::Overflow(_)
        ) {
            return Ok(Some(VersionKey::Database));
        }
        if value.scalar_type() != Some(column.scalar_type) {
            return Err(refusal("incompatible parent join key"));
        }
        tuple.push(value);
    }
    Ok(Some(VersionKey::RelationalRow {
        table: schema.name.clone(),
        primary_key: RelationalKey(tuple),
    }))
}

fn child_key_query(
    read: &PreparedRead,
    schema: &RelationalTableSchema,
    columns: &[&String],
    key: &RelationalKey,
) -> Result<QueryOutput> {
    // Bound generated SQL before identifier quoting/formatting. Values remain
    // parameters; this never evaluates the original query in host Rust.
    let mut bytes = schema.name.len().saturating_mul(2).saturating_add(128);
    for name in columns.iter().copied().chain(schema.primary_key.iter()) {
        bytes = bytes
            .saturating_add(name.len().saturating_mul(2))
            .saturating_add(64);
    }
    if bytes > MAX_INPUT_BYTES {
        return Err(refusal("join-key query metadata admission"));
    }
    let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
    let projection = columns
        .iter()
        .enumerate()
        .map(|(i, n)| format!("{} AS __hawdb_join_{i}", quote(n)))
        .collect::<Vec<_>>()
        .join(", ");
    let predicate = schema
        .primary_key
        .iter()
        .enumerate()
        .map(|(i, n)| format!("{} = ${}", quote(n), i + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "SELECT {projection} FROM public.{} WHERE {predicate}",
        quote(&schema.name)
    );
    let parameters = key
        .0
        .iter()
        .map(relational_to_value)
        .collect::<Result<Vec<_>>>()?;
    admit_input(&sql, &parameters)?;
    crate::sql::admit_postgres_sql_syntax(&sql, MAX_INPUT_BYTES, MAX_TOKENS, MAX_RECURSIVE_TOKENS)?;
    let default = hawdb_core::RuntimeTaskContext::default();
    read.read.query_sql_with_params_options_context(
        &sql,
        &parameters,
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(MAX_KEY_BYTES),
        },
        read.read.task_context.as_ref().unwrap_or(&default),
    )
}
