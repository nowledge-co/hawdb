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

//! Canonical paged rows used to build and validate relational indexes.

use crate::{
    RelationalHydrationBudget, RelationalIndexRowSource, RelationalIndexShadowError, RelationalKey,
    RelationalRow, RelationalRowPageProjectedRange, RelationalRowPageSnapshotReadError,
    RelationalRowPageSnapshotReadLimits, RelationalRowPageSnapshotReader, RelationalState,
};
use hawdb_core::RuntimeTaskContext;
use std::{collections::BTreeMap, ops::Bound};

pub struct CanonicalRelationalIndexRowSource {
    reader: RelationalRowPageSnapshotReader,
    column_counts: BTreeMap<String, usize>,
}

impl CanonicalRelationalIndexRowSource {
    pub fn new(reader: RelationalRowPageSnapshotReader, state: &RelationalState) -> Self {
        Self {
            reader,
            column_counts: state
                .table_schemas()
                .map(|schema| (schema.name.clone(), schema.columns.len()))
                .collect(),
        }
    }
}

impl RelationalIndexRowSource for CanonicalRelationalIndexRowSource {
    fn visit_rows(
        &self,
        table: &str,
        visit: &mut dyn FnMut(
            &RelationalKey,
            &RelationalRow,
        ) -> std::result::Result<(), RelationalIndexShadowError>,
    ) -> std::result::Result<(), RelationalIndexShadowError> {
        let column_count = self.column_counts.get(table).copied().ok_or_else(|| {
            RelationalIndexShadowError::Corrupt(format!(
                "canonical index row source is missing table {table}"
            ))
        })?;
        let fields = (0..column_count).collect::<Vec<_>>();
        let limits = RelationalRowPageSnapshotReadLimits::default();
        let batch_rows = limits.demand.max_rows.get().saturating_sub(1).max(1);
        let task = RuntimeTaskContext::default();
        let mut lower_key = None;
        loop {
            let mut hydration = RelationalHydrationBudget::default();
            let mut last_key = None;
            let mut visited = 0usize;
            let mut callback_error = None;
            let report = self
                .reader
                .visit_projected_range(
                    RelationalRowPageProjectedRange {
                        table,
                        lower: lower_key.as_ref().map_or(Bound::Unbounded, Bound::Excluded),
                        upper: Bound::Unbounded,
                        requested_fields: &fields,
                    },
                    limits,
                    &mut hydration,
                    &task,
                    |projected| {
                        if projected.fields.len() != column_count
                            || projected
                                .fields
                                .iter()
                                .enumerate()
                                .any(|(ordinal, field)| field.ordinal != ordinal)
                        {
                            callback_error = Some(RelationalIndexShadowError::Corrupt(format!(
                                "canonical row scan returned an incomplete row for table {table}"
                            )));
                            return false;
                        }
                        let primary_key = projected.primary_key;
                        let row = RelationalRow::new(
                            projected
                                .fields
                                .into_iter()
                                .map(|field| field.value)
                                .collect(),
                        );
                        if let Err(error) = visit(&primary_key, &row) {
                            callback_error = Some(error);
                            return false;
                        }
                        last_key = Some(primary_key);
                        visited = visited.saturating_add(1);
                        visited < batch_rows
                    },
                )
                .map_err(map_index_row_snapshot_error)?;
            if let Some(error) = callback_error {
                return Err(error);
            }
            if !report.demand.stopped_early {
                return Ok(());
            }
            lower_key = Some(last_key.ok_or_else(|| {
                RelationalIndexShadowError::Corrupt(format!(
                    "canonical row scan for table {table} stopped without progress"
                ))
            })?);
        }
    }
}

pub fn map_index_row_snapshot_error(
    error: RelationalRowPageSnapshotReadError,
) -> RelationalIndexShadowError {
    match error {
        RelationalRowPageSnapshotReadError::Admission(message) => {
            RelationalIndexShadowError::Admission(message)
        }
        RelationalRowPageSnapshotReadError::Stopped(reason) => {
            RelationalIndexShadowError::Admission(reason.to_string())
        }
        RelationalRowPageSnapshotReadError::MissingTable(table) => {
            RelationalIndexShadowError::Corrupt(format!(
                "canonical index row source is missing table {table}"
            ))
        }
        RelationalRowPageSnapshotReadError::Corrupt(message)
        | RelationalRowPageSnapshotReadError::Durability(message) => {
            RelationalIndexShadowError::Corrupt(message)
        }
    }
}
