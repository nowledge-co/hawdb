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

pub(super) use crate::physical_plan::predicate_is_covered_by_access;
pub(super) use hawdb_optimizer::relational_sargability::collect_conjunctive_join_equalities;
use hawdb_optimizer::relational_sargability::{
    canonical_keyset_values, collect_conjunctive_equalities, predicate_is_covered_by_equalities,
};

use super::{
    bind_sql_value, relational_unique_index_name, resolve_column, select_relational_access_path,
    value_to_relational_as, BTreeMap, BTreeSet, BoundRow, HawDBError, PlannedJoin,
    RelationalAccessCandidate, RelationalAccessPathDescriptor, RelationalAccessPathKind,
    RelationalBaseAccess, RelationalIndexRangeScan, RelationalIndexReadMode,
    RelationalIndexRuntime, RelationalIndexScanDirection, RelationalJoinAccess,
    RelationalJoinAccessCandidate, RelationalKey, RelationalReadRow, RelationalRowReadMode,
    RelationalRowRuntime, RelationalState, RelationalTableSchema, RelationalValue, Result,
    SqlColumnRef, SqlNullOrder, SqlOrderDirection, SqlPredicate, Value,
};

pub(super) struct RelationalBaseAccessPlanning<'a, R> {
    pub(super) index_read_mode: RelationalIndexReadMode<'a, R>,
    pub(super) fields: &'a crate::field_plan::RelationalFieldPlan,
    pub(super) predicate: Option<&'a SqlPredicate>,
    pub(super) order_by: &'a [hawdb_sql::SqlOrderItem],
    pub(super) prefer_ordered_access: bool,
    pub(super) parameters: &'a [Value],
    pub(super) state: &'a RelationalState,
    pub(super) schema: &'a RelationalTableSchema,
    pub(super) table: &'a str,
    pub(super) qualifier: &'a str,
    pub(super) cardinality_limit: usize,
    pub(super) projection: RelationalProjectionAccessPlanning,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct RelationalProjectionAccessPlanning {
    pub(super) force_full_scan: bool,
    pub(super) row_count_override: Option<usize>,
}

pub(super) fn projection_access_planning(
    row_read_mode: RelationalRowReadMode<'_, impl crate::row_runtime::RelationalRowStoreReader>,
    table: &str,
) -> RelationalProjectionAccessPlanning {
    RelationalProjectionAccessPlanning {
        force_full_scan: row_read_mode.is_projection_table(table),
        row_count_override: row_read_mode.projection_estimated_rows(table),
    }
}

pub(super) fn choose_base_access(
    planning: RelationalBaseAccessPlanning<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
) -> Result<RelationalAccessCandidate> {
    let RelationalBaseAccessPlanning {
        fields,
        predicate,
        order_by,
        prefer_ordered_access,
        parameters,
        state,
        schema,
        table,
        qualifier,
        cardinality_limit,
        projection,
        ..
    } = planning;
    let mut equalities = Vec::new();
    if let Some(predicate) = predicate {
        collect_conjunctive_equalities(predicate, &mut equalities);
    }
    let mut bound = BTreeMap::<String, RelationalValue>::new();
    for (column, value) in equalities {
        if column
            .qualifier
            .as_deref()
            .is_some_and(|candidate| candidate != table && candidate != qualifier)
        {
            continue;
        }
        let Some(position) = schema.column_position(&column.name) else {
            continue;
        };
        let value = value_to_relational_as(
            bind_sql_value(value, parameters)?,
            schema.columns[position].scalar_type,
        )?;
        if matches!(value, RelationalValue::Null) {
            continue;
        }
        if value.scalar_type() != Some(schema.columns[position].scalar_type) {
            return Err(HawDBError::Semantic(format!(
                "relational comparison on {} has an incompatible scalar type",
                column.name
            )));
        }
        match bound.entry(column.name.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &value => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                // The residual predicate will reject the contradictory equality.
            }
        }
    }

    let row_count = projection
        .row_count_override
        .unwrap_or_else(|| state.row_count(table));
    let mut candidates = vec![RelationalAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::FullScan,
            name: "__full_scan".to_string(),
            index_columns: Vec::new(),
            access_columns: BTreeSet::new(),
            equality_prefix_len: 0,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: false,
            estimated_rows: row_count.min(cardinality_limit).max(1),
        },
        access: RelationalBaseAccess::FullScan,
    }];

    if !projection.force_full_scan {
        if let Some(key) = complete_key(&schema.primary_key, &bound) {
            let estimated_rows = usize::from(state.row(table, &key).is_some()).max(1);
            candidates.push(RelationalAccessCandidate {
                descriptor: RelationalAccessPathDescriptor {
                    kind: RelationalAccessPathKind::PrimaryKey,
                    name: "__primary_key".to_string(),
                    index_columns: schema.primary_key.clone(),
                    access_columns: schema.primary_key.iter().cloned().collect(),
                    equality_prefix_len: schema.primary_key.len(),
                    order_prefix_len: 0,
                    exclusive_range: false,
                    reverse_order: false,
                    unique_point: true,
                    covering: false,
                    requires_row_fetch: false,
                    estimated_rows,
                },
                access: RelationalBaseAccess::PrimaryKey(key),
            });
        }
        for (ordinal, columns) in schema.unique_constraints.iter().enumerate() {
            if let Some(candidate) = index_access_candidate(
                &planning,
                relational_unique_index_name(ordinal),
                columns,
                true,
                &bound,
            )? {
                candidates.push(candidate);
            }
        }
        for index in &schema.indexes {
            if let Some(candidate) = index_access_candidate(
                &planning,
                index.name.clone(),
                &index.columns,
                index.unique,
                &bound,
            )? {
                candidates.push(candidate);
            }
        }
    }

    // Coverage must participate in costing before skyline pruning and selection.
    for candidate in &mut candidates {
        fields.apply_access_coverage(&mut candidate.descriptor, table, schema)?;
    }
    let ordered_candidates = candidates
        .iter()
        .filter(|candidate| {
            prefer_ordered_access
                && !order_by.is_empty()
                && candidate.descriptor.order_prefix_len == order_by.len()
                && predicate_is_covered_by_access(
                    predicate,
                    &candidate.descriptor,
                    order_by,
                    table,
                    qualifier,
                )
        })
        .map(|candidate| candidate.descriptor.clone())
        .collect::<Vec<_>>();
    let descriptors = if ordered_candidates.is_empty() {
        candidates
            .iter()
            .map(|candidate| candidate.descriptor.clone())
            .collect()
    } else {
        ordered_candidates
    };
    let selected = select_relational_access_path(descriptors)
        .map_err(|error| HawDBError::Execution(format!("invalid relational access path: {error}")))?
        .expect("full scan is always an access-path candidate");
    let position = candidates
        .iter()
        .position(|candidate| candidate.descriptor == selected)
        .expect("selected relational access path came from the candidate set");
    Ok(candidates.swap_remove(position))
}

pub(super) fn complete_key(
    columns: &[String],
    bound: &BTreeMap<String, RelationalValue>,
) -> Option<RelationalKey> {
    columns
        .iter()
        .map(|column| bound.get(column).cloned())
        .collect::<Option<Vec<_>>>()
        .map(RelationalKey)
}

pub(super) fn index_access_candidate(
    planning: &RelationalBaseAccessPlanning<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
    name: String,
    columns: &[String],
    unique: bool,
    bound: &BTreeMap<String, RelationalValue>,
) -> Result<Option<RelationalAccessCandidate>> {
    let RelationalBaseAccessPlanning {
        order_by,
        state,
        schema,
        table,
        qualifier,
        cardinality_limit,
        ..
    } = planning;
    let prefix = columns
        .iter()
        .map_while(|column| bound.get(column).cloned())
        .collect::<Vec<_>>();
    if prefix.is_empty() {
        return Ok(None);
    }
    let prefix_len = prefix.len();
    let (mut order_prefix_len, mut direction) =
        index_order_prefix(order_by, columns, prefix_len, schema, table, qualifier);
    let key = RelationalKey(prefix);
    let access_columns = columns[..prefix_len]
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let equality_covered =
        predicate_is_covered_by_equalities(planning.predicate, &access_columns, table, qualifier);
    let exclusive_bound = if equality_covered {
        None
    } else if order_prefix_len == order_by.len()
        && prefix_len.saturating_add(order_by.len()) == columns.len()
    {
        bind_canonical_keyset_bound(planning, &key, &access_columns)?
    } else {
        None
    };
    if !equality_covered && exclusive_bound.is_none() {
        order_prefix_len = 0;
        direction = RelationalIndexScanDirection::Forward;
    }
    let estimated_rows =
        match state.index_prefix_cardinality_at_most(table, &name, &key, *cardinality_limit) {
            Some(rows) => rows,
            None if !state.materialized_index_postings_resident() => {
                if unique && prefix_len == columns.len() {
                    usize::from(state.row_count(table) != 0)
                } else {
                    // Persisted indexes may have fresh prefix NDV statistics even
                    // when their posting lists are absent from the in-memory state.
                    planning
                        .index_read_mode
                        .probe_statistics(table, &name, prefix_len)
                        .map(|statistics| {
                            usize::try_from(statistics.average_fanout()).unwrap_or(usize::MAX)
                        })
                        .unwrap_or_else(|| state.row_count(table))
                        .min(state.row_count(table))
                        .min(*cardinality_limit)
                }
            }
            None => {
                return Err(HawDBError::Execution(format!(
                    "relational index {name} on table {table} is not materialized"
                )));
            }
        }
        .max(1);
    Ok(Some(RelationalAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::Index,
            name: name.clone(),
            index_columns: columns.to_vec(),
            access_columns,
            equality_prefix_len: prefix_len,
            order_prefix_len,
            exclusive_range: exclusive_bound.is_some(),
            reverse_order: direction == RelationalIndexScanDirection::Backward,
            unique_point: unique && prefix_len == columns.len(),
            covering: false,
            requires_row_fetch: true,
            estimated_rows,
        },
        access: RelationalBaseAccess::Index {
            name,
            scan: RelationalIndexRangeScan {
                prefix: key,
                exclusive_bound,
                direction,
            },
        },
    }))
}

pub(super) fn index_order_prefix(
    order_by: &[hawdb_sql::SqlOrderItem],
    index_columns: &[String],
    equality_prefix_len: usize,
    schema: &RelationalTableSchema,
    table: &str,
    qualifier: &str,
) -> (usize, RelationalIndexScanDirection) {
    if order_by.is_empty()
        || equality_prefix_len.saturating_add(order_by.len()) > index_columns.len()
    {
        return (0, RelationalIndexScanDirection::Forward);
    }
    let direction = match order_by[0].direction {
        SqlOrderDirection::Asc => RelationalIndexScanDirection::Forward,
        SqlOrderDirection::Desc => RelationalIndexScanDirection::Backward,
    };
    for (ordinal, item) in order_by.iter().enumerate() {
        let Some(column) = item.expression.as_column() else {
            return (0, RelationalIndexScanDirection::Forward);
        };
        if item.direction != order_by[0].direction
            || column
                .qualifier
                .as_deref()
                .is_some_and(|candidate| candidate != table && candidate != qualifier)
            || column.name != index_columns[equality_prefix_len + ordinal]
        {
            return (0, RelationalIndexScanDirection::Forward);
        }
        let Some(position) = schema.column_position(&column.name) else {
            return (0, RelationalIndexScanDirection::Forward);
        };
        if schema.columns[position].nullable
            && (direction == RelationalIndexScanDirection::Backward
                || !matches!(item.nulls, SqlNullOrder::First))
        {
            return (0, RelationalIndexScanDirection::Forward);
        }
    }
    (order_by.len(), direction)
}

pub(super) fn bind_canonical_keyset_bound(
    planning: &RelationalBaseAccessPlanning<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
    prefix: &RelationalKey,
    access_columns: &BTreeSet<String>,
) -> Result<Option<RelationalKey>> {
    let RelationalBaseAccessPlanning {
        predicate,
        order_by,
        schema,
        table,
        qualifier,
        parameters,
        ..
    } = planning;
    let Some((first, second)) =
        canonical_keyset_values(*predicate, access_columns, order_by, table, qualifier)
    else {
        return Ok(None);
    };
    let mut bound = prefix.0.clone();
    for (value, item) in [(first, &order_by[0]), (second, &order_by[1])] {
        let Some(position) = schema.column_position(&item.expression.require_column()?.name) else {
            return Ok(None);
        };
        let value = value_to_relational_as(
            bind_sql_value(value, parameters)?,
            schema.columns[position].scalar_type,
        )?;
        if schema.columns[position].nullable
            || matches!(value, RelationalValue::Null)
            || value.scalar_type() != Some(schema.columns[position].scalar_type)
        {
            return Ok(None);
        }
        bound.push(value);
    }
    Ok(Some(RelationalKey(bound)))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn choose_join_access(
    predicate: &SqlPredicate,
    state: &RelationalState,
    schema: &RelationalTableSchema,
    table: &str,
    qualifier: &str,
    index_read_mode: RelationalIndexReadMode<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
    projection: RelationalProjectionAccessPlanning,
    fields: &crate::field_plan::RelationalFieldPlan,
) -> Result<RelationalJoinAccessCandidate> {
    let mut bound = BTreeMap::<String, SqlColumnRef>::new();
    collect_conjunctive_join_equalities(predicate, table, qualifier, &mut bound);
    let row_count = projection
        .row_count_override
        .unwrap_or_else(|| state.row_count(table));
    let mut candidates = vec![RelationalJoinAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::FullScan,
            name: "__full_scan".to_string(),
            index_columns: Vec::new(),
            access_columns: BTreeSet::new(),
            equality_prefix_len: 0,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: false,
            estimated_rows: row_count.max(1),
        },
        access: RelationalJoinAccess::FullScan,
    }];

    if !projection.force_full_scan {
        if let Some(columns) = complete_join_columns(&schema.primary_key, &bound) {
            candidates.push(RelationalJoinAccessCandidate {
                descriptor: RelationalAccessPathDescriptor {
                    kind: RelationalAccessPathKind::PrimaryKey,
                    name: "__primary_key".to_string(),
                    index_columns: schema.primary_key.clone(),
                    access_columns: schema.primary_key.iter().cloned().collect(),
                    equality_prefix_len: schema.primary_key.len(),
                    order_prefix_len: 0,
                    exclusive_range: false,
                    reverse_order: false,
                    unique_point: true,
                    covering: false,
                    requires_row_fetch: false,
                    estimated_rows: usize::from(row_count != 0).max(1),
                },
                access: RelationalJoinAccess::PrimaryKey(columns),
            });
        }
        for (ordinal, columns) in schema.unique_constraints.iter().enumerate() {
            if let Some(candidate) = join_index_access_candidate(
                relational_unique_index_name(ordinal),
                columns,
                true,
                &bound,
                row_count,
                table,
                index_read_mode,
            ) {
                candidates.push(candidate);
            }
        }
        for index in &schema.indexes {
            if let Some(candidate) = join_index_access_candidate(
                index.name.clone(),
                &index.columns,
                index.unique,
                &bound,
                row_count,
                table,
                index_read_mode,
            ) {
                candidates.push(candidate);
            }
        }
    }

    for candidate in &mut candidates {
        fields.apply_access_coverage(&mut candidate.descriptor, table, schema)?;
    }
    let selected = select_relational_access_path(
        candidates
            .iter()
            .map(|candidate| candidate.descriptor.clone()),
    )
    .map_err(|error| HawDBError::Execution(format!("invalid relational join access: {error}")))?
    .expect("full scan is always a join access-path candidate");
    let position = candidates
        .iter()
        .position(|candidate| candidate.descriptor == selected)
        .expect("selected relational join access path came from the candidate set");
    Ok(candidates.swap_remove(position))
}

pub(super) fn complete_join_columns(
    columns: &[String],
    bound: &BTreeMap<String, SqlColumnRef>,
) -> Option<Vec<(String, SqlColumnRef)>> {
    columns
        .iter()
        .map(|column| {
            bound
                .get(column)
                .cloned()
                .map(|outer| (column.clone(), outer))
        })
        .collect()
}

pub(super) fn join_index_access_candidate(
    name: String,
    columns: &[String],
    unique: bool,
    bound: &BTreeMap<String, SqlColumnRef>,
    row_count: usize,
    table: &str,
    index_read_mode: RelationalIndexReadMode<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
) -> Option<RelationalJoinAccessCandidate> {
    let access_columns = columns
        .iter()
        .map_while(|column| {
            bound
                .get(column)
                .cloned()
                .map(|outer| (column.clone(), outer))
        })
        .collect::<Vec<_>>();
    if access_columns.is_empty() {
        return None;
    }
    let equality_prefix_len = access_columns.len();
    let unique_point = unique && equality_prefix_len == columns.len();
    let estimated_rows = if unique_point {
        usize::from(row_count != 0)
    } else {
        index_read_mode
            .probe_statistics(table, &name, equality_prefix_len)
            .map(|statistics| {
                debug_assert!(statistics.distinct_non_null_values <= statistics.non_null_rows);
                debug_assert!(statistics.fanout <= statistics.non_null_rows);
                debug_assert!(statistics.fanout >= statistics.average_fanout());
                usize::try_from(statistics.average_fanout())
                    .unwrap_or(usize::MAX)
                    .min(row_count)
            })
            .unwrap_or(row_count)
    }
    .max(1);
    Some(RelationalJoinAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::Index,
            name: name.clone(),
            index_columns: columns.to_vec(),
            access_columns: columns[..equality_prefix_len].iter().cloned().collect(),
            equality_prefix_len,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point,
            covering: false,
            requires_row_fetch: true,
            estimated_rows,
        },
        access: RelationalJoinAccess::Index {
            name,
            columns: access_columns,
        },
    })
}

pub(super) fn bound_join_key(
    row: &BoundRow<'_>,
    schema: &RelationalTableSchema,
    columns: &[(String, SqlColumnRef)],
) -> Result<Option<RelationalKey>> {
    let mut values = Vec::with_capacity(columns.len());
    for (join_column, outer_column) in columns {
        let value = resolve_column(row, outer_column)?.clone();
        if matches!(value, RelationalValue::Null) {
            return Ok(None);
        }
        let position = schema.column_position(join_column).ok_or_else(|| {
            HawDBError::Semantic(format!(
                "relational join table {} has no column {join_column}",
                schema.name
            ))
        })?;
        if value.scalar_type() != Some(schema.columns[position].scalar_type) {
            return Err(HawDBError::Semantic(format!(
                "relational join comparison on {join_column} has an incompatible scalar type"
            )));
        }
        values.push(value);
    }
    Ok(Some(RelationalKey(values)))
}

pub(super) fn visit_join_entries<'a>(
    state: &'a RelationalState,
    index_runtime: &RelationalIndexRuntime<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
    row_runtime: &RelationalRowRuntime<'a>,
    planned: &PlannedJoin<'a>,
    row: &BoundRow<'a>,
    visit: &mut dyn FnMut(RelationalReadRow) -> Result<bool>,
) -> Result<bool> {
    let table = &planned.join.table.name;
    match &planned.access {
        RelationalJoinAccess::PrimaryKey(columns) => {
            let Some(key) = bound_join_key(row, planned.schema, columns)? else {
                return Ok(true);
            };
            match row_runtime.read_point(table, &key)? {
                Some(row) => visit(row),
                None => Ok(true),
            }
        }
        RelationalJoinAccess::Index { name, columns } => {
            let Some(prefix) = bound_join_key(row, planned.schema, columns)? else {
                return Ok(true);
            };
            index_runtime.visit_prefix(state, table, name, &prefix, |key| {
                match row_runtime.read_point(table, key)? {
                    Some(row) => visit(row),
                    None => Err(HawDBError::StorageIntegrity(format!(
                        "relational index {name} on table {table} points to missing row {key:?}"
                    ))),
                }
            })
        }
        RelationalJoinAccess::FullScan => row_runtime.visit_all(table, visit),
    }
}

pub(super) fn visit_base_entries<'a>(
    state: &'a RelationalState,
    index_runtime: &RelationalIndexRuntime<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
    row_runtime: &RelationalRowRuntime<'a>,
    table: &str,
    access: &RelationalBaseAccess,
    descriptor: Option<&RelationalAccessPathDescriptor>,
    visit: &mut dyn FnMut(RelationalReadRow) -> Result<bool>,
) -> Result<bool> {
    match access {
        RelationalBaseAccess::PrimaryKey(key) => match row_runtime.read_point(table, key)? {
            Some(row) => visit(row),
            None => Ok(true),
        },
        RelationalBaseAccess::Index { name, scan } => {
            let covered_columns = descriptor
                .filter(|descriptor| descriptor.covering)
                .map(|descriptor| descriptor.index_columns.as_slice());
            index_runtime.visit_range_entries(state, table, name, scan, |index_key, key| {
                let row = match covered_columns {
                    Some(index_columns) => row_runtime
                        .read_index_covered(table, index_columns, index_key, key)?,
                    None => row_runtime.read_point(table, key)?,
                };
                match row {
                    Some(row) => visit(row),
                    None => Err(HawDBError::StorageIntegrity(format!(
                        "relational index {name} on table {table} points to missing or non-coverable row {key:?}"
                    ))),
                }
            })
        }
        RelationalBaseAccess::FullScan => row_runtime.visit_all(table, visit),
    }
}
