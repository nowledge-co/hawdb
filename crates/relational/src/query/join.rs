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

use super::{
    bound_join_key, visit_base_entries, Binding, BlockingOperatorMemoryReport, BoundRow,
    HawDBError, QueryMemoryLedger, RefCell, RelationalEquiJoinKeys, RelationalIndexRuntime,
    RelationalJoinAccess, RelationalKey, RelationalPhysicalAccess, RelationalPhysicalJoinNode,
    RelationalPhysicalJoinPlan, RelationalPhysicalRelation, RelationalReadRow,
    RelationalRowRuntime, RelationalState, RelationalValue, Result,
};

pub(super) struct RelationalPhysicalJoinExecution<'a> {
    pub(super) tree: &'a RelationalPhysicalJoinPlan,
    pub(super) memory: &'a hawdb_executor::ExecutionMemoryConfig,
    pub(super) memory_ledger: &'a QueryMemoryLedger,
    pub(super) reports: RefCell<Vec<BlockingOperatorMemoryReport>>,
}

impl RelationalPhysicalJoinExecution<'_> {
    pub(super) fn take_reports(&self) -> Vec<BlockingOperatorMemoryReport> {
        std::mem::take(&mut *self.reports.borrow_mut())
    }
}

pub(super) fn bound_row_resident_bytes(row: &BoundRow<'_>) -> usize {
    std::mem::size_of::<BoundRow<'_>>()
        .saturating_add(
            row.bindings
                .len()
                .saturating_mul(std::mem::size_of::<Binding<'_>>()),
        )
        .saturating_add(
            row.bindings
                .iter()
                .filter_map(|binding| binding.row.as_ref())
                .map(RelationalReadRow::resident_bytes)
                .sum::<usize>(),
        )
}

pub(super) fn relational_key_resident_bytes(key: &RelationalKey) -> usize {
    std::mem::size_of::<RelationalKey>()
        .saturating_add(
            key.0
                .capacity()
                .saturating_mul(std::mem::size_of::<RelationalValue>()),
        )
        .saturating_add(
            key.0
                .iter()
                .map(|value| match value {
                    RelationalValue::Text(value) => value.capacity(),
                    RelationalValue::Bytea(value) => value.capacity(),
                    _ => 0,
                })
                .sum::<usize>(),
        )
}

pub(super) fn visit_tree_relation_entries<'a>(
    state: &'a RelationalState,
    index_runtime: &RelationalIndexRuntime<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
    row_runtime: &RelationalRowRuntime<'a>,
    relation: &'a RelationalPhysicalRelation,
    outer: Option<&BoundRow<'a>>,
    visit: &mut dyn FnMut(RelationalReadRow) -> Result<bool>,
) -> Result<bool> {
    match &relation.access {
        RelationalPhysicalAccess::Base(access) => visit_base_entries(
            state,
            index_runtime,
            row_runtime,
            &relation.table,
            &access.access,
            Some(&access.descriptor),
            visit,
        ),
        RelationalPhysicalAccess::Probe(access) => {
            let outer = outer.ok_or_else(|| {
                HawDBError::Execution(format!(
                    "physical join probe for {} has no outer row",
                    relation.qualifier
                ))
            })?;
            match &access.access {
                RelationalJoinAccess::PrimaryKey(columns) => {
                    let schema = state.table_schema(&relation.table).ok_or_else(|| {
                        HawDBError::Semantic(format!("unknown relational table {}", relation.table))
                    })?;
                    let Some(key) = bound_join_key(outer, schema, columns)? else {
                        return Ok(true);
                    };
                    match row_runtime.read_point(&relation.table, &key)? {
                        Some(row) => visit(row),
                        None => Ok(true),
                    }
                }
                RelationalJoinAccess::Index { name, columns } => {
                    let schema = state.table_schema(&relation.table).ok_or_else(|| {
                        HawDBError::Semantic(format!("unknown relational table {}", relation.table))
                    })?;
                    let Some(prefix) = bound_join_key(outer, schema, columns)? else {
                        return Ok(true);
                    };
                    let covered_columns = access
                        .descriptor
                        .covering
                        .then_some(access.descriptor.index_columns.as_slice());
                    index_runtime.visit_prefix_entries(
                        state,
                        &relation.table,
                        name,
                        &prefix,
                        |index_key, key| {
                            let row = match covered_columns {
                                Some(index_columns) => row_runtime.read_index_covered(
                                    &relation.table,
                                    index_columns,
                                    index_key,
                                    key,
                                )?,
                                None => row_runtime.read_point(&relation.table, key)?,
                            };
                            match row {
                            Some(row) => visit(row),
                            None => Err(HawDBError::StorageIntegrity(format!(
                                "relational index {name} on table {} points to missing or non-coverable row {key:?}",
                                relation.table
                            ))),
                            }
                        },
                    )
                }
                RelationalJoinAccess::FullScan => row_runtime.visit_all(&relation.table, visit),
            }
        }
    }
}

pub(super) fn null_extended_tree_row<'a>(
    node: &'a RelationalPhysicalJoinNode,
    state: &'a RelationalState,
) -> Result<BoundRow<'a>> {
    let mut bindings = Vec::new();
    let mut error = None;
    node.visit_relations(&mut |relation| {
        if error.is_some() {
            return;
        }
        match state.table_schema(&relation.table) {
            Some(schema) => bindings.push(Binding {
                binding: relation.binding,
                table: &relation.table,
                qualifier: &relation.qualifier,
                schema,
                row: None,
            }),
            None => {
                error = Some(HawDBError::Semantic(format!(
                    "unknown relational table {}",
                    relation.table
                )));
            }
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    let row = BoundRow { bindings };
    node.output_schema().ensure_matches(row.schema_bindings())?;
    Ok(row)
}

pub(super) fn batched_index_probe_key(
    state: &RelationalState,
    relation: &RelationalPhysicalRelation,
    outer: &BoundRow<'_>,
) -> Result<Option<RelationalKey>> {
    let RelationalPhysicalAccess::Probe(candidate) = &relation.access else {
        return Err(HawDBError::Execution(format!(
            "batched index join relation {} is not a probe input",
            relation.qualifier
        )));
    };
    let columns = match &candidate.access {
        RelationalJoinAccess::PrimaryKey(columns) | RelationalJoinAccess::Index { columns, .. } => {
            columns
        }
        RelationalJoinAccess::FullScan => {
            return Err(HawDBError::Execution(format!(
                "batched index join relation {} has a full-scan probe",
                relation.qualifier
            )));
        }
    };
    let schema = state.table_schema(&relation.table).ok_or_else(|| {
        HawDBError::Semantic(format!("unknown relational table {}", relation.table))
    })?;
    bound_join_key(outer, schema, columns)
}

pub(super) fn bound_relation_join_key(
    row: &BoundRow<'_>,
    relation: &RelationalPhysicalRelation,
    keys: &RelationalEquiJoinKeys,
) -> Result<Option<RelationalKey>> {
    let binding = row
        .bindings
        .iter()
        .find(|binding| binding.binding == relation.binding)
        .ok_or_else(|| {
            HawDBError::Execution(format!(
                "equi-join relation {} is missing from its row",
                relation.qualifier
            ))
        })?;
    let mut values = Vec::with_capacity(keys.columns.len());
    for (column, _) in &keys.columns {
        let position = binding.schema.column_position(column).ok_or_else(|| {
            HawDBError::Semantic(format!(
                "equi-join relation {} has no column {column}",
                relation.table
            ))
        })?;
        let value = binding.value(position)?.clone();
        if matches!(value, RelationalValue::Null) {
            return Ok(None);
        }
        values.push(value);
    }
    Ok(Some(RelationalKey(values)))
}
