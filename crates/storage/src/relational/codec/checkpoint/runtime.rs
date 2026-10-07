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

//! Controlled private relational reconstruction. Owned row/posting maps move
//! into pages instead of cloning split keys. Map comparisons, allocation and
//! directory/drop costs still require a hard resource ledger.

use super::*;
use crate::relational::{
    RelationalIndexPages, RelationalIndexRole, RelationalKeySetPages, RelationalRowPages,
    RELATIONAL_INDEX_PAGE_MAX_KEYS, RELATIONAL_POSTING_PAGE_MAX_KEYS,
    RELATIONAL_ROW_PAGE_MAX_ENTRIES, RELATIONAL_ROW_PAGE_TARGET_BYTES,
};

pub(crate) fn clone_string(
    value: &str,
    work: &CheckpointWorkContext,
) -> Result<String, RelationalError> {
    let mut output = String::new();
    let mut start = 0;
    while start < value.len() {
        let mut end = start.saturating_add(64 * 1024).min(value.len());
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        let unit = work.start_unit().map_err(work_error)?;
        output.push_str(&value[start..end]);
        start = end;
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(output)
}

fn clone_value(
    value: &RelationalValue,
    work: &CheckpointWorkContext,
) -> Result<RelationalValue, RelationalError> {
    match value {
        RelationalValue::Text(text) => Ok(RelationalValue::Text(clone_string(text, work)?)),
        RelationalValue::Bytea(bytes) => {
            let mut output = Vec::new();
            for block in bytes.chunks(64 * 1024) {
                let unit = work.start_unit().map_err(work_error)?;
                output.extend_from_slice(block);
                unit.finish();
            }
            work.checkpoint().map_err(work_error)?;
            Ok(RelationalValue::Bytea(output))
        }
        _ => {
            let unit = work.start_unit().map_err(work_error)?;
            let value = value.clone();
            unit.finish();
            work.checkpoint().map_err(work_error)?;
            Ok(value)
        }
    }
}

pub(crate) fn clone_key(
    key: &RelationalKey,
    work: &CheckpointWorkContext,
) -> Result<RelationalKey, RelationalError> {
    let mut values = Vec::new();
    for value in &key.0 {
        values.push(clone_value(value, work)?);
    }
    work.checkpoint().map_err(work_error)?;
    Ok(RelationalKey(values))
}

pub(crate) fn row_key(
    row: &RelationalRow,
    positions: &[usize],
    work: &CheckpointWorkContext,
) -> Result<RelationalKey, RelationalError> {
    let mut values = Vec::new();
    for position in positions {
        values.push(clone_value(&row.values()[*position], work)?);
    }
    work.checkpoint().map_err(work_error)?;
    Ok(RelationalKey(values))
}

pub(crate) fn row_pages(
    rows: BTreeMap<RelationalKey, RelationalRow>,
    work: &CheckpointWorkContext,
) -> Result<RelationalRowPages, RelationalError> {
    let len = rows.len();
    let mut pages = Vec::new();
    let mut page = BTreeMap::new();
    let mut page_bytes = 0usize;
    for (key, row) in rows {
        let mut entry_bytes = std::mem::size_of::<RelationalKey>()
            .saturating_add(std::mem::size_of::<RelationalRow>());
        // Keep the ordinary estimator's exact summation order and page boundaries.
        for values in [&key.0[..], row.values()] {
            let mut payload_bytes = 0usize;
            for value in values {
                let unit = work.start_unit().map_err(work_error)?;
                payload_bytes += value.estimated_payload_bytes();
                unit.finish();
            }
            entry_bytes = entry_bytes.saturating_add(payload_bytes);
        }
        let unit = work.start_unit().map_err(work_error)?;
        if !page.is_empty()
            && (page.len() >= RELATIONAL_ROW_PAGE_MAX_ENTRIES
                || page_bytes.saturating_add(entry_bytes) > RELATIONAL_ROW_PAGE_TARGET_BYTES)
        {
            pages.push(Arc::new(std::mem::take(&mut page)));
            page_bytes = 0;
        }
        page_bytes = page_bytes.saturating_add(entry_bytes);
        page.insert(key, row);
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
    if !page.is_empty() {
        pages.push(Arc::new(page));
    }
    let result = RelationalRowPages {
        pages: Arc::new(pages),
        len,
    };
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(result)
}

fn column_map<'a>(
    schema: &'a RelationalTableSchema,
    work: &CheckpointWorkContext,
) -> Result<BTreeMap<&'a str, usize>, RelationalError> {
    let mut positions = BTreeMap::new();
    for (position, column) in schema.columns.iter().enumerate() {
        let unit = work.start_unit().map_err(work_error)?;
        // Match ordinary first-position lookup even for direct corrupt-state tests.
        positions.entry(column.name.as_str()).or_insert(position);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(positions)
}

fn positions(
    schema: &RelationalTableSchema,
    names: &[String],
    columns: &BTreeMap<&str, usize>,
    work: &CheckpointWorkContext,
) -> Result<Vec<usize>, RelationalError> {
    let mut result = Vec::new();
    for name in names {
        let unit = work.start_unit().map_err(work_error)?;
        result.push(*columns.get(name.as_str()).ok_or_else(|| {
            RelationalError::Schema(format!("table {} has no column {name}", schema.name))
        })?);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(result)
}

fn has_null(key: &RelationalKey, work: &CheckpointWorkContext) -> Result<bool, RelationalError> {
    for value in &key.0 {
        let unit = work.start_unit().map_err(work_error)?;
        let null = matches!(value, RelationalValue::Null);
        unit.finish();
        if null {
            work.checkpoint().map_err(work_error)?;
            return Ok(true);
        }
    }
    work.checkpoint().map_err(work_error)?;
    Ok(false)
}

fn posting_pages(
    keys: BTreeSet<RelationalKey>,
    work: &CheckpointWorkContext,
) -> Result<RelationalKeySetPages, RelationalError> {
    let len = keys.len();
    let mut pages = Vec::new();
    let mut page = BTreeSet::new();
    for key in keys {
        let unit = work.start_unit().map_err(work_error)?;
        if page.len() == RELATIONAL_POSTING_PAGE_MAX_KEYS {
            pages.push(Arc::new(std::mem::take(&mut page)));
        }
        page.insert(key);
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
    if !page.is_empty() {
        pages.push(Arc::new(page));
    }
    let result = RelationalKeySetPages {
        pages: Arc::new(pages),
        len,
    };
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(result)
}

fn index_pages(
    entries: BTreeMap<RelationalKey, BTreeSet<RelationalKey>>,
    work: &CheckpointWorkContext,
) -> Result<RelationalIndexPages, RelationalError> {
    let mut pages = Vec::new();
    let mut page = BTreeMap::new();
    for (key, postings) in entries {
        let postings = posting_pages(postings, work)?;
        let unit = work.start_unit().map_err(work_error)?;
        if page.len() == RELATIONAL_INDEX_PAGE_MAX_KEYS {
            pages.push(Arc::new(std::mem::take(&mut page)));
        }
        page.insert(key, postings);
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
    if !page.is_empty() {
        pages.push(Arc::new(page));
    }
    let result = RelationalIndexPages {
        pages: Arc::new(pages),
    };
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(result)
}

pub(crate) fn rebuild_indexes(
    schemas: &BTreeMap<String, Arc<RelationalTableSchema>>,
    segments: &mut BTreeMap<String, Arc<RelationalTableSegment>>,
    table: &str,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    let schema = schemas
        .get(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let segment = segments
        .get(table)
        .ok_or_else(|| RelationalError::Schema(format!("unknown table {table}")))?;
    let columns = column_map(schema, work)?;
    // Borrow definition columns/names and preserve ordinary unique/declared/FK order.
    let definitions = schema
        .unique_constraints
        .iter()
        .enumerate()
        .map(|(ordinal, columns)| {
            (
                std::borrow::Cow::Owned(crate::relational::relational_unique_index_name(ordinal)),
                columns.as_slice(),
                RelationalIndexRole::UniqueConstraint,
            )
        })
        .chain(schema.indexes.iter().map(|index| {
            (
                std::borrow::Cow::Borrowed(index.name.as_str()),
                index.columns.as_slice(),
                if index.unique {
                    RelationalIndexRole::DeclaredUnique
                } else {
                    RelationalIndexRole::Secondary
                },
            )
        }))
        .chain(
            schema
                .foreign_keys
                .iter()
                .enumerate()
                .map(|(ordinal, foreign)| {
                    (
                        std::borrow::Cow::Owned(
                            crate::relational::relational_foreign_key_index_name(ordinal),
                        ),
                        foreign.columns.as_slice(),
                        RelationalIndexRole::ForeignKeySupport,
                    )
                }),
        );
    let mut indexes = BTreeMap::new();
    for (name, names, role) in definitions {
        let positions = positions(schema, names, &columns, work)?;
        let mut entries: BTreeMap<RelationalKey, BTreeSet<RelationalKey>> = BTreeMap::new();
        for (primary_key, row) in segment.rows.iter() {
            let key = row_key(row, &positions, work)?;
            if role.is_unique() && has_null(&key, work)? {
                continue;
            }
            let primary_key = clone_key(primary_key, work)?;
            let unit = work.start_unit().map_err(work_error)?;
            if role.is_unique() && entries.get(&key).is_some_and(|keys| !keys.is_empty()) {
                return Err(RelationalError::Constraint(format!(
                    "unique index {name} on table {table} has a duplicate key"
                )));
            }
            entries.entry(key).or_default().insert(primary_key);
            unit.finish();
        }
        let index = index_pages(entries, work)?;
        let name = clone_string(&name, work)?;
        let unit = work.start_unit().map_err(work_error)?;
        indexes.insert(name, index);
        unit.finish();
    }
    let replacement = RelationalTableSegment {
        rows: segment.rows.clone(),
        indexes,
    };
    let name = clone_string(table, work)?;
    let unit = work.start_unit().map_err(work_error)?;
    segments.insert(name, Arc::new(replacement));
    unit.finish();
    work.checkpoint().map_err(work_error)
}

fn same_columns(
    left: &[String],
    right: &[String],
    work: &CheckpointWorkContext,
) -> Result<bool, RelationalError> {
    if left.len() != right.len() {
        work.checkpoint().map_err(work_error)?;
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        let unit = work.start_unit().map_err(work_error)?;
        let equal = left == right;
        unit.finish();
        if !equal {
            work.checkpoint().map_err(work_error)?;
            return Ok(false);
        }
    }
    work.checkpoint().map_err(work_error)?;
    Ok(true)
}

fn references_unique(
    schema: &RelationalTableSchema,
    columns: &[String],
    work: &CheckpointWorkContext,
) -> Result<bool, RelationalError> {
    if same_columns(columns, &schema.primary_key, work)? {
        return Ok(true);
    }
    for unique in &schema.unique_constraints {
        if same_columns(columns, unique, work)? {
            return Ok(true);
        }
    }
    for index in &schema.indexes {
        let unit = work.start_unit().map_err(work_error)?;
        let unique = index.unique;
        unit.finish();
        if unique && same_columns(columns, &index.columns, work)? {
            return Ok(true);
        }
    }
    work.checkpoint().map_err(work_error)?;
    Ok(false)
}

pub(crate) fn validate_foreign_keys(
    state: &RelationalState,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    for (table, schema) in &state.schemas {
        let unit = work.start_unit().map_err(work_error)?;
        let segment = state.segments.get(table);
        unit.finish();
        let Some(segment) = segment else {
            continue;
        };
        let local_columns = column_map(schema, work)?;
        for foreign in &schema.foreign_keys {
            let local_positions = positions(schema, &foreign.columns, &local_columns, work)?;
            let referenced_schema =
                state
                    .schemas
                    .get(&foreign.referenced_table)
                    .ok_or_else(|| {
                        RelationalError::Schema(format!(
                            "foreign key references unknown table {}",
                            foreign.referenced_table
                        ))
                    })?;
            let referenced_columns = column_map(referenced_schema, work)?;
            let referenced_positions = positions(
                referenced_schema,
                &foreign.referenced_columns,
                &referenced_columns,
                work,
            )?;
            if !references_unique(referenced_schema, &foreign.referenced_columns, work)? {
                return Err(RelationalError::Schema(format!(
                    "foreign key from {table} must reference a primary or unique key on {}",
                    foreign.referenced_table
                )));
            }
            for (local, referenced) in local_positions.iter().zip(&referenced_positions) {
                let unit = work.start_unit().map_err(work_error)?;
                if schema.columns[*local].scalar_type
                    != referenced_schema.columns[*referenced].scalar_type
                {
                    return Err(RelationalError::Schema(format!(
                        "foreign key from {table} to {} has incompatible column types",
                        foreign.referenced_table
                    )));
                }
                unit.finish();
            }
            let referenced_rows =
                state
                    .segments
                    .get(&foreign.referenced_table)
                    .ok_or_else(|| {
                        RelationalError::Schema(format!(
                            "missing row segment for table {}",
                            foreign.referenced_table
                        ))
                    })?;
            let mut referenced_keys = BTreeSet::new();
            for row in referenced_rows.rows.values() {
                let key = row_key(row, &referenced_positions, work)?;
                let unit = work.start_unit().map_err(work_error)?;
                referenced_keys.insert(key);
                unit.finish();
            }
            for row in segment.rows.values() {
                let key = row_key(row, &local_positions, work)?;
                if has_null(&key, work)? {
                    continue;
                }
                let unit = work.start_unit().map_err(work_error)?;
                if !referenced_keys.contains(&key) {
                    return Err(RelationalError::Constraint(format!(
                        "foreign key from {table} to {} has no visible target",
                        foreign.referenced_table
                    )));
                }
                unit.finish();
            }
        }
    }
    work.checkpoint().map_err(work_error)
}

#[cfg(test)]
mod tests;
