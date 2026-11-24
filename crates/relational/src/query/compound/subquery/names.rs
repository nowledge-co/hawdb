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

//! Bind names independently of row production or predicate short-circuiting.
//! This pass never evaluates expressions, including unused EXISTS targets.

use super::*;
use crate::subquery_scope::join_on_scope;
use hawdb_sql::{SqlColumnRef, SqlOrderItem};

type Binding = (String, String, Option<Vec<String>>);
type Scope<'a> = &'a [Binding];

pub(super) fn validate(select: &SelectStatement, state: &RelationalState) -> Result<()> {
    validate_select(select, &select.order_by, state, &CteScope::default(), &[])
}

fn column(column: &SqlColumnRef, scopes: &[Scope<'_>]) -> Result<()> {
    for scope in scopes.iter().rev() {
        let mut matches = 0;
        let mut qualified = false;
        for (table, alias, columns) in *scope {
            if let Some(qualifier) = &column.qualifier {
                if qualifier != table && qualifier != alias {
                    continue;
                }
                qualified = true;
            }
            if columns
                .as_ref()
                .is_none_or(|names| names.contains(&column.name))
            {
                matches += 1;
            }
        }
        if matches == 1 {
            return Ok(());
        }
        if matches > 1 {
            return Err(HawDBError::Semantic(format!(
                "ambiguous relational column {}",
                column.name
            )));
        }
        // A local qualifier shadows the same qualifier in every outer scope,
        // even when its relation has no column with this name.
        if qualified {
            break;
        }
    }
    Err(HawDBError::Semantic(format!(
        "unknown relational column {}",
        column.name
    )))
}

fn expression(expr: &Expr, scopes: &[Scope<'_>]) -> Result<()> {
    let mut result = Ok(());
    expr.visit(&mut |expr| {
        if result.is_ok()
            && let Some(name) = expr.as_column()
        {
            result = column(name, scopes);
        }
    });
    result
}

fn validate_select(
    select: &SelectStatement,
    order_by: &[SqlOrderItem],
    state: &RelationalState,
    ctes: &CteScope,
    outer: &[Scope<'_>],
) -> Result<()> {
    let mut local = Vec::new();
    for (table, alias) in select
        .from
        .iter()
        .map(|table| (table, select.from_alias.as_ref()))
        .chain(
            select
                .joins
                .iter()
                .map(|join| (&join.table, join.alias.as_ref())),
        )
    {
        let columns = match ctes.columns(state, &table.name) {
            RelationColumns::Table(schema) => Some(
                schema
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
            ),
            RelationColumns::Cte(names) => Some(names.to_vec()),
            RelationColumns::Unknown if ctes.contains(&table.name) => None,
            RelationColumns::Unknown => {
                return Err(HawDBError::Semantic(format!(
                    "unknown relational table {}",
                    table.name
                )))
            }
        };
        local.push((
            table.name.clone(),
            alias.unwrap_or(&table.name).clone(),
            columns,
        ));
    }
    let mut scopes = outer.to_vec();
    for (ordinal, join) in select.joins.iter().enumerate() {
        scopes.push(join_on_scope(&local, join.on_scope_start, ordinal)?);
        expression(&join.on, &scopes)?;
        scopes.pop();
    }
    scopes.push(&local);
    for projection in &select.projection {
        if let SelectProjection::Expression {
            expression: expr, ..
        } = projection
        {
            expression(expr, &scopes)?;
        }
    }
    for expr in select.selection.iter().chain(&select.having) {
        expression(expr, &scopes)?;
    }
    for name in &select.group_by {
        column(name, &scopes)?;
    }
    for item in order_by {
        order(select, item, &scopes)?;
    }
    if let Some(predicate) = &select.subquery_predicate {
        validate_predicate(predicate, state, ctes, &scopes)?;
    }
    Ok(())
}

fn order(select: &SelectStatement, item: &SqlOrderItem, scopes: &[Scope<'_>]) -> Result<()> {
    use crate::field_plan::{resolve_relational_order_target, RelationalOrderTarget};
    match resolve_relational_order_target(select, item)? {
        RelationalOrderTarget::InputColumn(name)
        | RelationalOrderTarget::ProjectionColumn { column: name, .. } => column(name, scopes),
        RelationalOrderTarget::ProjectionExpression { .. } => Ok(()),
    }
}

fn validate_predicate(
    predicate: &SqlSubqueryPredicate,
    state: &RelationalState,
    ctes: &CteScope,
    scopes: &[Scope<'_>],
) -> Result<()> {
    match predicate {
        SqlSubqueryPredicate::Scalar(expr) => expression(expr, scopes),
        SqlSubqueryPredicate::And(left, right) | SqlSubqueryPredicate::Or(left, right) => {
            validate_predicate(left, state, ctes, scopes)?;
            validate_predicate(right, state, ctes, scopes)
        }
        SqlSubqueryPredicate::Not(inner) => validate_predicate(inner, state, ctes, scopes),
        SqlSubqueryPredicate::Exists { query } => validate_query(query, state, ctes, scopes),
        SqlSubqueryPredicate::In { left, query, .. } => {
            for expr in left {
                expression(expr, scopes)?;
            }
            validate_query(query, state, ctes, scopes)
        }
    }
}

fn validate_query(
    query: &SqlQueryStatement,
    state: &RelationalState,
    ctes: &CteScope,
    scopes: &[Scope<'_>],
) -> Result<()> {
    if query.with.is_empty()
        && let SqlQueryBody::Select(select) = &query.body
    {
        return validate_select(select, &query.order_by, state, ctes, scopes);
    }
    let mut local = ctes.clone();
    for cte in &query.with {
        local.seed_recursive(query.recursive, cte, state)?;
        if let SqlCommonTableExpressionBody::Query(child) = &cte.body {
            validate_query(child, state, &local, scopes)?;
        }
        local.define(cte, state)?;
    }
    validate_body(&query.body, state, &local, scopes)?;
    if !query.order_by.is_empty()
        && let Some(columns) = local.body_columns(&query.body, state)?
    {
        let schema = hawdb_executor::QuerySchema::try_new(columns)?;
        for item in &query.order_by {
            super::super::resolve_order(item, &schema)?;
        }
    }
    Ok(())
}

fn validate_body(
    body: &SqlQueryBody,
    state: &RelationalState,
    ctes: &CteScope,
    scopes: &[Scope<'_>],
) -> Result<()> {
    match body {
        SqlQueryBody::Select(select) => {
            validate_select(select, &select.order_by, state, ctes, scopes)
        }
        SqlQueryBody::Values(rows) => {
            for expr in rows.iter().flatten() {
                expression(expr, scopes)?;
            }
            Ok(())
        }
        SqlQueryBody::Nested(query) => validate_query(query, state, ctes, scopes),
        SqlQueryBody::SetOperation { left, right, .. } => {
            validate_body(left, state, ctes, scopes)?;
            validate_body(right, state, ctes, scopes)
        }
    }
}
