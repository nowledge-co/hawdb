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

//! SQL binding for the supported incremental view algebra.

use hawdb_core::{HawDBError, Result};
use hawdb_sql::{
    CreateMaterializedViewStatement, CreateSearchIndexStatement, DropMaterializedViewStatement,
    DropSearchIndexStatement, ExprKind, SelectProjection, SqlFunctionArgument,
};
use hawdb_storage::relational::{
    MaterializedViewColumn, MaterializedViewDefinition, MaterializedViewExpression,
    RelationalState, RelationalWrite, SearchIndexAnalyzer, SearchIndexSpec,
};

pub(super) fn create(
    create: CreateMaterializedViewStatement,
    state: &RelationalState,
) -> Result<Vec<RelationalWrite>> {
    crate::reject_non_public_schema(create.name.schema.as_deref())?;
    if let Some(schema) = state.table_schema(&create.name.name) {
        return if create.if_not_exists
            && schema.materialized_view.is_some()
            && schema.search_index().is_none()
        {
            Ok(Vec::new())
        } else {
            Err(HawDBError::Semantic(format!(
                "relation {} already exists",
                create.name.name
            )))
        };
    }
    let select = *create.query;
    if select.vector_index().is_some()
        || select.text_search().is_some()
        || select.distinct
        || !select.joins.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.lock_strength.is_some()
    {
        return Err(HawDBError::Semantic("materialized views support single-table projection, WHERE, GROUP BY, COUNT, BIGINT SUM, and BIGINT/TEXT MIN/MAX; joins, DISTINCT, HAVING, ORDER BY, LIMIT, OFFSET, and locking clauses are unsupported".into()));
    }
    let from = select
        .from
        .ok_or_else(|| HawDBError::Semantic("materialized views require a base table".into()))?;
    crate::reject_non_public_schema(from.schema.as_deref())?;
    let source = state
        .table_schema(&from.name)
        .ok_or_else(|| HawDBError::Semantic(format!("unknown relational table {}", from.name)))?;
    let validate = |column: &hawdb_sql::SqlColumnRef| {
        super::statement::validate_mutation_column(
            column,
            source,
            select.from_alias.as_deref(),
            &from.name,
        )
    };
    let mut columns = Vec::new();
    for projection in select.projection {
        match projection {
            SelectProjection::Wildcard => {
                columns.extend(source.visible_columns().iter().map(|column| {
                    MaterializedViewColumn {
                        name: column.name.clone(),
                        expression: MaterializedViewExpression::Column(column.name.clone()),
                    }
                }));
            }
            SelectProjection::Expression { expression, alias } => {
                let (expression, default_name) = match expression.kind {
                    ExprKind::Column(column) => {
                        validate(&column)?;
                        (
                            MaterializedViewExpression::Column(column.name.clone()),
                            column.name,
                        )
                    }
                    ExprKind::Function {
                        name,
                        arguments,
                        distinct: false,
                        filter: None,
                    } if name.eq_ignore_ascii_case("count")
                        || name.eq_ignore_ascii_case("sum")
                        || name.eq_ignore_ascii_case("min")
                        || name.eq_ignore_ascii_case("max") =>
                    {
                        let input = match arguments.as_slice() {
                            [SqlFunctionArgument::Wildcard]
                                if name.eq_ignore_ascii_case("count") =>
                            {
                                None
                            }
                            [SqlFunctionArgument::Expression(argument)] => {
                                let column = argument.require_column()?;
                                validate(column)?;
                                Some(column.name.clone())
                            }
                            _ => {
                                return Err(HawDBError::Semantic(
                                    "materialized aggregates require one column, or COUNT(*)"
                                        .into(),
                                ))
                            }
                        };
                        let expression = if name.eq_ignore_ascii_case("count") {
                            MaterializedViewExpression::Count(input)
                        } else if name.eq_ignore_ascii_case("min") {
                            MaterializedViewExpression::Min(input.expect("MIN column"))
                        } else if name.eq_ignore_ascii_case("max") {
                            MaterializedViewExpression::Max(input.expect("MAX column"))
                        } else {
                            MaterializedViewExpression::Sum(input.expect("SUM column"))
                        };
                        (expression, name.to_ascii_lowercase())
                    }
                    _ => {
                        return Err(HawDBError::Semantic(
                            "unsupported materialized view expression; use columns, COUNT, BIGINT SUM, or BIGINT/TEXT MIN/MAX"
                                .into(),
                        ))
                    }
                };
                columns.push(MaterializedViewColumn {
                    name: alias.unwrap_or(default_name),
                    expression,
                });
            }
        }
    }
    for column in &select.group_by {
        validate(column)?;
    }
    let filter = select
        .selection
        .map(|predicate| {
            super::statement::compile_mutation_predicate(
                &predicate,
                &[],
                source,
                select.from_alias.as_deref(),
                &from.name,
            )
        })
        .transpose()?;
    let definition = MaterializedViewDefinition {
        source: from.name.clone(),
        columns,
        group_by: select
            .group_by
            .into_iter()
            .map(|column| column.name)
            .collect(),
        filter,
        search: None,
    };
    let schema = definition
        .table_schema(create.name.name, source)
        .map_err(|error| HawDBError::Semantic(error.to_string()))?;
    Ok(vec![RelationalWrite::CreateTable(schema)])
}

pub(super) fn create_search_index(
    create: &CreateSearchIndexStatement,
    state: &RelationalState,
) -> Result<Vec<RelationalWrite>> {
    crate::reject_non_public_schema(create.name.schema.as_deref())?;
    crate::reject_non_public_schema(create.table.schema.as_deref())?;
    let analyzer = match &create.analyzer {
        None => SearchIndexAnalyzer::DEFAULT,
        Some(name) => SearchIndexAnalyzer::parse(name).ok_or_else(|| {
            HawDBError::Semantic(format!(
                "unsupported search index analyzer {name}; supported: cjk_ngram"
            ))
        })?,
    };
    if let Some(existing) = state.table_schema(&create.name.name) {
        return if create.if_not_exists && existing.search_index().is_some() {
            Ok(Vec::new())
        } else {
            Err(HawDBError::Semantic(format!(
                "relation {} already exists",
                create.name.name
            )))
        };
    }
    let source = state.table_schema(&create.table.name).ok_or_else(|| {
        HawDBError::Semantic(format!("unknown relational table {}", create.table.name))
    })?;
    let definition = MaterializedViewDefinition {
        source: create.table.name.clone(),
        columns: Vec::new(),
        group_by: Vec::new(),
        filter: None,
        search: Some(SearchIndexSpec {
            column: create.column.clone(),
            analyzer,
        }),
    };
    let schema = definition
        .table_schema(create.name.name.clone(), source)
        .map_err(|error| HawDBError::Semantic(error.to_string()))?;
    Ok(vec![RelationalWrite::CreateTable(schema)])
}

pub(super) fn drop_search_index(
    drop: &DropSearchIndexStatement,
    state: &RelationalState,
) -> Result<Vec<RelationalWrite>> {
    crate::reject_non_public_schema(drop.name.schema.as_deref())?;
    match state.table_schema(&drop.name.name) {
        None if drop.if_exists => Ok(Vec::new()),
        Some(schema) if schema.search_index().is_some() => {
            Ok(vec![RelationalWrite::DropMaterializedView {
                table: drop.name.name.clone(),
            }])
        }
        _ => Err(HawDBError::Semantic(format!(
            "unknown search index {}",
            drop.name.name
        ))),
    }
}

pub(super) fn drop_view(
    drop: DropMaterializedViewStatement,
    state: &RelationalState,
) -> Result<Vec<RelationalWrite>> {
    crate::reject_non_public_schema(drop.name.schema.as_deref())?;
    match state.table_schema(&drop.name.name) {
        None if drop.if_exists => Ok(Vec::new()),
        Some(schema) if schema.search_index().is_some() => Err(HawDBError::Semantic(format!(
            "{} is a search index; use DROP SEARCH INDEX",
            drop.name.name
        ))),
        Some(schema) if schema.materialized_view.is_some() => {
            Ok(vec![RelationalWrite::DropMaterializedView {
                table: drop.name.name,
            }])
        }
        _ => Err(HawDBError::Semantic(format!(
            "unknown materialized view {}",
            drop.name.name
        ))),
    }
}

pub(super) fn prepare_select(
    select: &mut hawdb_sql::SelectStatement,
    state: &RelationalState,
) -> Result<()> {
    let relations = select
        .from
        .iter()
        .map(|table| (table, select.from_alias.as_deref()))
        .chain(
            select
                .joins
                .iter()
                .map(|join| (&join.table, join.alias.as_deref())),
        )
        .filter_map(|(table, alias)| {
            state
                .table_schema(&table.name)
                .map(|schema| (alias.unwrap_or(&table.name).to_owned(), schema))
        })
        .collect::<Vec<_>>();
    if let Some((_, schema)) = relations
        .iter()
        .find(|(_, schema)| schema.search_index().is_some())
    {
        return Err(HawDBError::Semantic(format!(
            "{} is a search index; query it with text_search",
            schema.name
        )));
    }
    if !relations
        .iter()
        .any(|(_, schema)| schema.materialized_view.is_some())
    {
        return Ok(());
    }
    let check = |column: &hawdb_sql::SqlColumnRef| -> Result<()> {
        if relations.iter().any(|(qualifier, schema)| {
            column
                .qualifier
                .as_ref()
                .is_none_or(|value| value == qualifier)
                && schema.materialized_view.is_some()
                && schema
                    .column_position(&column.name)
                    .is_some_and(|position| position >= schema.visible_columns().len())
        }) {
            return Err(HawDBError::Semantic(format!(
                "materialized view internal column {} is not queryable",
                column.name
            )));
        }
        Ok(())
    };
    let check_expression = |expression: &mut hawdb_sql::Expr| {
        expression.try_visit_mut(&mut |node| {
            if let ExprKind::Column(column) = &node.kind {
                check(column)?;
            }
            Ok::<_, HawDBError>(())
        })
    };
    for projection in &mut select.projection {
        if let SelectProjection::Expression { expression, .. } = projection {
            check_expression(expression)?;
        }
    }
    for predicate in select.selection.iter_mut().chain(select.having.iter_mut()) {
        check_expression(predicate)?;
    }
    for join in &mut select.joins {
        check_expression(&mut join.on)?;
    }
    for order in &mut select.order_by {
        check_expression(&mut order.expression)?;
    }
    for column in &select.group_by {
        check(column)?;
    }
    let mut projection = Vec::new();
    for item in std::mem::take(&mut select.projection) {
        if matches!(item, SelectProjection::Wildcard) {
            for (qualifier, schema) in &relations {
                projection.extend(schema.visible_columns().iter().map(|column| {
                    SelectProjection::Expression {
                        expression: hawdb_sql::Expr::column(hawdb_sql::SqlColumnRef {
                            qualifier: Some(qualifier.clone()),
                            name: column.name.clone(),
                        }),
                        alias: None,
                    }
                }));
            }
        } else {
            projection.push(item);
        }
    }
    select.projection = projection;
    Ok(())
}
