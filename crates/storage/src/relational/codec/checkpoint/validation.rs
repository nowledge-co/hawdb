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

//! Cooperative schema validation with borrowed name/position maps.
//! Variable-width comparisons, diagnostics and map allocations still need
//! byte/time accounting; these element boundaries are not a full mount bound.

use super::*;

fn validate_columns(
    positions: &BTreeMap<&str, usize>,
    columns: &[String],
    kind: &str,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    let unit = work.start_unit().map_err(work_error)?;
    if columns.is_empty() {
        return Err(RelationalError::Schema(format!(
            "{kind} must contain at least one column"
        )));
    }
    unit.finish();
    let mut unique = BTreeSet::new();
    for column in columns {
        let unit = work.start_unit().map_err(work_error)?;
        if !positions.contains_key(column.as_str()) || !unique.insert(column.as_str()) {
            return Err(RelationalError::Schema(format!(
                "{kind} references unknown or duplicate column {column}"
            )));
        }
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

pub(crate) fn validate_table_schema_with_work_context<'a>(
    schema: &'a RelationalTableSchema,
    work: &CheckpointWorkContext,
) -> Result<BTreeMap<&'a str, usize>, RelationalError> {
    let unit = work.start_unit().map_err(work_error)?;
    if schema.name.is_empty() || schema.columns.is_empty() {
        return Err(RelationalError::Schema(
            "table name and columns must be non-empty".into(),
        ));
    }
    if schema.primary_key.is_empty() {
        return Err(RelationalError::Schema(format!(
            "table {} must declare a primary key",
            schema.name
        )));
    }
    unit.finish();
    let mut positions = BTreeMap::new();
    for (position, column) in schema.columns.iter().enumerate() {
        let unit = work.start_unit().map_err(work_error)?;
        if column.name.is_empty() || positions.insert(column.name.as_str(), position).is_some() {
            return Err(RelationalError::Schema(format!(
                "duplicate or empty column {}",
                column.name
            )));
        }
        if let Some(default) = &column.default {
            match default {
                RelationalColumnDefault::Literal(value) => {
                    crate::relational::validate_value_type(column, value)?;
                }
                RelationalColumnDefault::UuidV7
                    if column.scalar_type == RelationalScalarType::Uuid => {}
                RelationalColumnDefault::UuidV7 => {
                    return Err(RelationalError::Schema(format!(
                        "column {} uses uuidv7() default but is not UUID",
                        column.name
                    )));
                }
            }
        }
        unit.finish();
    }
    validate_columns(&positions, &schema.primary_key, "primary key", work)?;
    for column in &schema.primary_key {
        let unit = work.start_unit().map_err(work_error)?;
        let position = positions.get(column.as_str()).expect("validated column");
        if schema.columns[*position].nullable {
            return Err(RelationalError::Schema(format!(
                "primary-key column {column} must be NOT NULL"
            )));
        }
        unit.finish();
    }
    for unique in &schema.unique_constraints {
        validate_columns(&positions, unique, "unique constraint", work)?;
    }
    for foreign in &schema.foreign_keys {
        validate_columns(&positions, &foreign.columns, "foreign key", work)?;
        let unit = work.start_unit().map_err(work_error)?;
        if foreign.columns.len() != foreign.referenced_columns.len() || foreign.columns.is_empty() {
            return Err(RelationalError::Schema(
                "foreign-key column counts must match and be non-empty".into(),
            ));
        }
        unit.finish();
    }
    let mut index_names = BTreeSet::new();
    for index in &schema.indexes {
        let unit = work.start_unit().map_err(work_error)?;
        if index.name.is_empty()
            || crate::relational::is_reserved_relational_index_name(&index.name)
            || !index_names.insert(index.name.as_str())
        {
            return Err(RelationalError::Schema(format!(
                "invalid, duplicate, or reserved index name {}",
                index.name
            )));
        }
        unit.finish();
        validate_columns(&positions, &index.columns, "index", work)?;
    }
    work.checkpoint().map_err(work_error)?;
    Ok(positions)
}

pub(crate) fn primary_key_positions_with_work_context(
    schema: &RelationalTableSchema,
    positions: &BTreeMap<&str, usize>,
    work: &CheckpointWorkContext,
) -> Result<Vec<usize>, RelationalError> {
    let mut result = Vec::new();
    for column in &schema.primary_key {
        let unit = work.start_unit().map_err(work_error)?;
        result.push(
            *positions
                .get(column.as_str())
                .expect("validated primary key"),
        );
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(result)
}

pub(crate) fn validate_row_with_work_context(
    schema: &RelationalTableSchema,
    row: &RelationalRow,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    let unit = work.start_unit().map_err(work_error)?;
    if row.values().len() != schema.columns.len() {
        return Err(RelationalError::Schema(format!(
            "table {} expects {} columns but row has {}",
            schema.name,
            schema.columns.len(),
            row.values().len()
        )));
    }
    unit.finish();
    for (column, value) in schema.columns.iter().zip(row.values()) {
        let unit = work.start_unit().map_err(work_error)?;
        crate::relational::validate_value_type(column, value)?;
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

#[cfg(test)]
mod tests;
