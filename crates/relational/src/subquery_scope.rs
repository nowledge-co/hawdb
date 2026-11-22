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

use crate::query_value::expression_name;
use hawdb_core::{HawDBError, Result};
use hawdb_sql::{
    SelectProjection, SqlColumnRef, SqlCommonTableExpression, SqlCommonTableExpressionBody,
    SqlQueryBody, SqlQueryStatement,
};
use hawdb_storage::relational::{RelationalState, RelationalTableSchema};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct CteScope(BTreeMap<String, Option<Arc<[String]>>>);

impl CteScope {
    pub(crate) fn contains(&self, table: &str) -> bool {
        self.0.contains_key(table)
    }

    pub(crate) fn columns<'a>(
        &'a self,
        state: &'a RelationalState,
        table: &str,
    ) -> RelationColumns<'a> {
        match self.0.get(table) {
            Some(Some(columns)) => RelationColumns::Cte(columns),
            None if let Some(schema) = state.table_schema(table) => RelationColumns::Table(schema),
            _ => RelationColumns::Unknown,
        }
    }

    pub(crate) fn seed_recursive(&mut self, recursive: bool, cte: &SqlCommonTableExpression) {
        if recursive {
            let names = (!cte.columns.is_empty()).then(|| Arc::from(cte.columns.as_slice()));
            self.0.insert(cte.name.clone(), names);
        }
    }

    pub(crate) fn define(
        &mut self,
        cte: &SqlCommonTableExpression,
        state: &RelationalState,
    ) -> Result<()> {
        let output = match &cte.body {
            SqlCommonTableExpressionBody::Query(query) => self.query_columns(query, state)?,
            SqlCommonTableExpressionBody::Mutation(_) => None,
        };
        if let Some(output) = &output
            && !cte.columns.is_empty()
            && cte.columns.len() != output.len()
        {
            return Err(HawDBError::Semantic(format!(
                "common table expression {} declares {} columns but returns {}",
                cte_display_name(&cte.name),
                cte.columns.len(),
                output.len()
            )));
        }
        let names = if cte.columns.is_empty() {
            output.map(Arc::from)
        } else {
            Some(Arc::from(cte.columns.as_slice()))
        };
        self.0.insert(cte.name.clone(), names);
        Ok(())
    }

    pub(crate) fn query_columns(
        &self,
        query: &SqlQueryStatement,
        state: &RelationalState,
    ) -> Result<Option<Vec<String>>> {
        if query.with.is_empty() {
            return self.body_columns(&query.body, state);
        }
        let mut local = self.clone();
        for cte in &query.with {
            local.seed_recursive(query.recursive, cte);
            local.define(cte, state)?;
        }
        local.body_columns(&query.body, state)
    }

    fn body_columns(
        &self,
        body: &SqlQueryBody,
        state: &RelationalState,
    ) -> Result<Option<Vec<String>>> {
        match body {
            SqlQueryBody::Select(select) => {
                let mut names = Vec::new();
                for projection in &select.projection {
                    match projection {
                        SelectProjection::Expression { expression, alias } => {
                            names
                                .push(alias.clone().unwrap_or_else(|| expression_name(expression)));
                        }
                        SelectProjection::Wildcard => {
                            for table in select
                                .from
                                .iter()
                                .chain(select.joins.iter().map(|join| &join.table))
                            {
                                match self.columns(state, &table.name) {
                                    RelationColumns::Table(schema) => names.extend(
                                        schema.columns.iter().map(|column| column.name.clone()),
                                    ),
                                    RelationColumns::Cte(columns) => {
                                        names.extend_from_slice(columns)
                                    }
                                    RelationColumns::Unknown => return Ok(None),
                                }
                            }
                        }
                    }
                }
                Ok(Some(names))
            }
            SqlQueryBody::Values(rows) => {
                let Some(first) = rows.first() else {
                    return Err(HawDBError::Semantic(
                        "VALUES requires at least one row".into(),
                    ));
                };
                let arity = first.len();
                if let Some((index, row)) =
                    rows.iter().enumerate().find(|(_, row)| row.len() != arity)
                {
                    return Err(HawDBError::Semantic(format!(
                        "VALUES row {} has arity {}, expected {arity}",
                        index + 1,
                        row.len(),
                    )));
                }
                Ok(Some(
                    (1..=arity).map(|index| format!("column{index}")).collect(),
                ))
            }
            SqlQueryBody::Nested(query) => self.query_columns(query, state),
            SqlQueryBody::SetOperation { left, right, .. } => {
                let left = self.body_columns(left, state)?;
                let right = self.body_columns(right, state)?;
                if let (Some(left), Some(right)) = (&left, &right)
                    && left.len() != right.len()
                {
                    return Err(HawDBError::Semantic(format!(
                        "set-operation inputs have arity {} and {}",
                        left.len(),
                        right.len(),
                    )));
                }
                // Set operations inherit output labels from the left arm.
                Ok(left)
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RelationColumns<'a> {
    Table(&'a RelationalTableSchema),
    Cte(&'a [String]),
    Unknown,
}

impl RelationColumns<'_> {
    pub(crate) fn matches(self, table: &str, alias: &str, column: &SqlColumnRef) -> bool {
        if let Some(qualifier) = &column.qualifier {
            return qualifier == table || qualifier == alias;
        }
        match self {
            Self::Table(schema) => schema.column_position(&column.name).is_some(),
            Self::Cte(columns) => columns.contains(&column.name),
            Self::Unknown => true,
        }
    }
}

pub(crate) fn cte_display_name(name: &str) -> String {
    name.strip_prefix("\0hawdb_derived_")
        .map_or_else(|| name.to_string(), |source| format!("derived@{source}"))
}
