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

//! Borrowed schema equality and cloning without whole-schema primitives.
//! Vector capacities, allocator execution and cancellation destruction still
//! require the candidate's hard resource ledger.

use super::{clone_bytes, compare, work_error, CheckpointWorkContext};
use crate::relational::{
    codec::clone_string_with_work_context, MaterializedViewDefinition, MaterializedViewExpression,
    RelationalColumnDefault, RelationalColumnSchema, RelationalForeignKeySchema,
    RelationalIndexSchema, RelationalPredicate, RelationalRowPagePublicationError,
    RelationalTableSchema, RelationalValue,
};

type Result<T> = std::result::Result<T, RelationalRowPagePublicationError>;

fn fixed<T>(work: &CheckpointWorkContext, operation: impl FnOnce() -> T) -> Result<T> {
    let unit = work.start_unit().map_err(work_error)?;
    let result = operation();
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(result)
}

fn text(value: &str, work: &CheckpointWorkContext) -> Result<String> {
    clone_string_with_work_context(value, work)
        .map_err(|error| RelationalRowPagePublicationError::Admission(error.to_string()))
}

fn same_text(left: &str, right: &str, work: &CheckpointWorkContext) -> Result<bool> {
    Ok(compare(left.as_bytes(), right.as_bytes(), work)?.is_eq())
}

fn same_list<T>(
    left: &[T],
    right: &[T],
    work: &CheckpointWorkContext,
    same: impl Fn(&T, &T, &CheckpointWorkContext) -> Result<bool>,
) -> Result<bool> {
    if !fixed(work, || left.len() == right.len())? {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        if !same(left, right, work)? {
            return Ok(false);
        }
    }
    work.checkpoint().map_err(work_error)?;
    Ok(true)
}

fn same_names(left: &[String], right: &[String], work: &CheckpointWorkContext) -> Result<bool> {
    same_list(left, right, work, |left, right, work| {
        same_text(left, right, work)
    })
}

fn same_default(
    left: &Option<RelationalColumnDefault>,
    right: &Option<RelationalColumnDefault>,
    work: &CheckpointWorkContext,
) -> Result<bool> {
    use RelationalColumnDefault::Literal;
    match (left, right) {
        (Some(Literal(left)), Some(Literal(right))) => same_value(left, right, work),
        _ => fixed(work, || left == right),
    }
}

fn same_value(
    left: &RelationalValue,
    right: &RelationalValue,
    work: &CheckpointWorkContext,
) -> Result<bool> {
    match (left, right) {
        (RelationalValue::Text(left), RelationalValue::Text(right)) => same_text(left, right, work),
        (RelationalValue::Bytea(left), RelationalValue::Bytea(right)) => {
            Ok(compare(left, right, work)?.is_eq())
        }
        (RelationalValue::Vector(left), RelationalValue::Vector(right)) => {
            Ok(compare(left.encoded(), right.encoded(), work)?.is_eq())
        }
        // Other matching payloads are fixed-size. Different variants compare
        // their discriminants first, preserving RelationalValue's total order.
        _ => fixed(work, || left == right),
    }
}

fn same_predicate(
    left: &RelationalPredicate,
    right: &RelationalPredicate,
    work: &CheckpointWorkContext,
) -> Result<bool> {
    use RelationalPredicate::*;
    work.checkpoint().map_err(work_error)?;
    match (left, right) {
        (And(a, b), And(c, d)) | (Or(a, b), Or(c, d)) => {
            Ok(same_predicate(a, c, work)? && same_predicate(b, d, work)?)
        }
        (Not(a), Not(b)) => same_predicate(a, b, work),
        (
            Compare {
                column: a,
                op: b,
                value: c,
            },
            Compare {
                column: d,
                op: e,
                value: f,
            },
        ) => Ok(b == e && same_text(a, d, work)? && same_value(c, f, work)?),
        (
            IsNull {
                column: a,
                negated: b,
            },
            IsNull {
                column: c,
                negated: d,
            },
        ) => Ok(b == d && same_text(a, c, work)?),
        _ => Ok(false),
    }
}

fn same_materialized_view(
    left: Option<&MaterializedViewDefinition>,
    right: Option<&MaterializedViewDefinition>,
    work: &CheckpointWorkContext,
) -> Result<bool> {
    let (left, right) = match (left, right) {
        (Some(left), Some(right)) => (left, right),
        (None, None) => return Ok(true),
        _ => return Ok(false),
    };
    if !same_text(&left.source, &right.source, work)?
        || !same_names(&left.group_by, &right.group_by, work)?
        || !same_list(&left.columns, &right.columns, work, |left, right, work| {
            use MaterializedViewExpression::*;
            if !same_text(&left.name, &right.name, work)? {
                return Ok(false);
            }
            match (&left.expression, &right.expression) {
                (Column(a), Column(b))
                | (Sum(a), Sum(b))
                | (Min(a), Min(b))
                | (Max(a), Max(b))
                | (Count(Some(a)), Count(Some(b))) => same_text(a, b, work),
                (Count(None), Count(None)) => Ok(true),
                _ => Ok(false),
            }
        })?
    {
        return Ok(false);
    }
    match (&left.search, &right.search) {
        (Some(left), Some(right)) => {
            if !same_text(&left.column, &right.column, work)?
                || !fixed(work, || left.analyzer == right.analyzer)?
            {
                return Ok(false);
            }
        }
        (None, None) => {}
        _ => return Ok(false),
    }
    match (&left.filter, &right.filter) {
        (Some(left), Some(right)) => same_predicate(left, right, work),
        (None, None) => Ok(true),
        _ => Ok(false),
    }
}

pub(in crate::relational::row_page::publication) fn same_schema(
    left: &RelationalTableSchema,
    right: &RelationalTableSchema,
    work: &CheckpointWorkContext,
) -> Result<bool> {
    if !same_materialized_view(
        left.materialized_view.as_deref(),
        right.materialized_view.as_deref(),
        work,
    )? || !same_text(&left.name, &right.name, work)?
        || !same_list(&left.columns, &right.columns, work, |left, right, work| {
            Ok(same_text(&left.name, &right.name, work)?
                && fixed(work, || {
                    left.scalar_type == right.scalar_type && left.nullable == right.nullable
                })?
                && same_default(&left.default, &right.default, work)?)
        })?
        || !same_names(&left.primary_key, &right.primary_key, work)?
        || !same_list(
            &left.unique_constraints,
            &right.unique_constraints,
            work,
            |left, right, work| same_names(left, right, work),
        )?
        || !same_list(
            &left.foreign_keys,
            &right.foreign_keys,
            work,
            |left, right, work| {
                Ok(same_names(&left.columns, &right.columns, work)?
                    && same_text(&left.referenced_table, &right.referenced_table, work)?
                    && same_names(&left.referenced_columns, &right.referenced_columns, work)?
                    && fixed(work, || {
                        left.on_delete == right.on_delete && left.on_update == right.on_update
                    })?)
            },
        )?
        || !same_list(&left.indexes, &right.indexes, work, |left, right, work| {
            Ok(same_text(&left.name, &right.name, work)?
                && same_names(&left.columns, &right.columns, work)?
                && fixed(work, || left.unique == right.unique)?)
        })?
    {
        return Ok(false);
    }
    work.checkpoint().map_err(work_error)?;
    Ok(true)
}

fn clone_list<T, U>(
    values: &[T],
    work: &CheckpointWorkContext,
    clone: impl Fn(&T, &CheckpointWorkContext) -> Result<U>,
) -> Result<Vec<U>> {
    let mut output = fixed(work, || Vec::with_capacity(values.len()))?;
    for value in values {
        let value = clone(value, work)?;
        fixed(work, || output.push(value))?;
    }
    work.checkpoint().map_err(work_error)?;
    Ok(output)
}

fn clone_names(values: &[String], work: &CheckpointWorkContext) -> Result<Vec<String>> {
    clone_list(values, work, |value, work| text(value, work))
}

fn clone_default(
    value: &Option<RelationalColumnDefault>,
    work: &CheckpointWorkContext,
) -> Result<Option<RelationalColumnDefault>> {
    use RelationalColumnDefault::Literal;
    let value = match value {
        Some(Literal(RelationalValue::Text(value))) => {
            Some(Literal(RelationalValue::Text(text(value, work)?)))
        }
        Some(Literal(RelationalValue::Bytea(value))) => {
            Some(Literal(RelationalValue::Bytea(clone_bytes(value, work)?)))
        }
        Some(Literal(RelationalValue::Vector(value))) => Some(Literal(RelationalValue::Vector(
            crate::background::decode_vector(clone_bytes(value.encoded(), work)?, Some(work))
                .map_err(|error| RelationalRowPagePublicationError::Admission(error.to_string()))?,
        ))),
        _ => return fixed(work, || value.clone()),
    };
    work.checkpoint().map_err(work_error)?;
    Ok(value)
}

pub(in crate::relational::row_page::publication) fn clone_schema(
    schema: &RelationalTableSchema,
    work: &CheckpointWorkContext,
) -> Result<RelationalTableSchema> {
    let name = text(&schema.name, work)?;
    let columns = clone_list(&schema.columns, work, |column, work| {
        let name = text(&column.name, work)?;
        let default = clone_default(&column.default, work)?;
        fixed(work, || RelationalColumnSchema {
            name,
            scalar_type: column.scalar_type,
            nullable: column.nullable,
            default,
        })
    })?;
    let primary_key = clone_names(&schema.primary_key, work)?;
    let unique_constraints = clone_list(&schema.unique_constraints, work, |names, work| {
        clone_names(names, work)
    })?;
    let foreign_keys = clone_list(&schema.foreign_keys, work, |foreign, work| {
        let columns = clone_names(&foreign.columns, work)?;
        let referenced_table = text(&foreign.referenced_table, work)?;
        let referenced_columns = clone_names(&foreign.referenced_columns, work)?;
        fixed(work, || RelationalForeignKeySchema {
            columns,
            referenced_table,
            referenced_columns,
            on_delete: foreign.on_delete,
            on_update: foreign.on_update,
        })
    })?;
    let indexes = clone_list(&schema.indexes, work, |index, work| {
        let name = text(&index.name, work)?;
        let columns = clone_names(&index.columns, work)?;
        fixed(work, || RelationalIndexSchema {
            name,
            columns,
            unique: index.unique,
        })
    })?;
    let context = crate::background::CheckpointDecodeContext {
        work: work.clone(),
        memory: Default::default(),
    };
    let materialized_view = schema
        .materialized_view
        .as_deref()
        .map(|view| crate::relational::materialized_view::clone_definition(view, &context))
        .transpose()
        .map_err(|error| RelationalRowPagePublicationError::Admission(error.to_string()))?;
    fixed(work, || RelationalTableSchema {
        materialized_view,
        name,
        columns,
        primary_key,
        unique_constraints,
        foreign_keys,
        indexes,
    })
}
