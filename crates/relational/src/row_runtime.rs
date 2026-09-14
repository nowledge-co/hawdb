//! Relational row execution over caller-pinned storage readers.

#[cfg(test)]
mod tests;

use crate::field_plan::RelationalFieldPlan;
use skein_core::RuntimeTaskContext;
use skein_core::{Result, SkeinError};
use skein_storage::{
    decode_projection_relational_member, encode_relational_primary_key, ProjectionGenerationError,
    ProjectionGenerationReadLimits, ProjectionGenerationReader, RelationalError,
    RelationalHydrationBudget, RelationalKey, RelationalProjectedField, RelationalProjectedRow,
    RelationalProjectedRowView, RelationalRow, RelationalRowPageDemandReadError,
    RelationalRowPageProjectedFields, RelationalRowPageProjectedRangeFields,
    RelationalRowPageReadViewIdentity, RelationalRowPageSnapshotPointReport,
    RelationalRowPageSnapshotPointsReport, RelationalRowPageSnapshotRangeReport,
    RelationalRowPageSnapshotReadError, RelationalRowPageSnapshotReadLimits,
    RelationalRowPageSnapshotReader, RelationalRowPageSnapshotRowSource, RelationalState,
    RelationalValueRef,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::sync::Arc;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationalRowExecutionEvidence {
    pub runtime_path: &'static str,
    pub base_generation: Option<u64>,
    pub delta_generation: Option<u64>,
    pub base_commit_epoch: Option<u64>,
    pub visible_commit_epoch: Option<u64>,
    pub root_set_digest: Option<String>,
    pub descriptor_reads: usize,
    pub logical_pages: usize,
    pub logical_bytes: usize,
    pub file_pages: usize,
    pub file_bytes: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_admission_rejections: usize,
    pub rows_visited: usize,
    pub borrowed_rows_visited: usize,
    pub owned_rows_visited: usize,
    pub index_covered_rows: usize,
    pub overlay_entries: usize,
    pub overlay_resident_bytes: usize,
    pub projection_generation: Option<String>,
    pub projection_source_watermark: Option<u64>,
    pub projection_version: Option<u64>,
    pub projection_publication_commit_epoch: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RelationalReadRow {
    row: Arc<RelationalProjectedRow>,
}

impl RelationalReadRow {
    pub fn as_ref(&self) -> RelationalReadRowRef<'_> {
        RelationalReadRowRef {
            row: RelationalProjectedRowView::Owned(&self.row),
        }
    }

    pub fn primary_key(&self) -> &RelationalKey {
        &self.row.primary_key
    }

    pub fn value(&self, ordinal: usize) -> Result<&skein_storage::RelationalValue> {
        let field = self
            .row
            .fields
            .binary_search_by_key(&ordinal, |field| field.ordinal)
            .ok()
            .map(|position| &self.row.fields[position])
            .ok_or_else(|| {
                SkeinError::StorageIntegrity(format!(
                    "relational row projection omitted required field {ordinal}"
                ))
            })?;
        Ok(&field.value)
    }

    pub fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(std::mem::size_of::<RelationalProjectedRow>())
            .saturating_add(
                self.row
                    .primary_key
                    .0
                    .iter()
                    .map(skein_storage::RelationalValue::estimated_payload_bytes)
                    .sum::<usize>(),
            )
            .saturating_add(
                self.row
                    .fields
                    .iter()
                    .map(|field| {
                        std::mem::size_of::<skein_storage::RelationalProjectedField>()
                            .saturating_add(field.value.estimated_payload_bytes())
                    })
                    .sum::<usize>(),
            )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RelationalReadRowRef<'a> {
    row: RelationalProjectedRowView<'a>,
}

impl<'a> RelationalReadRowRef<'a> {
    pub fn from_projected(row: &'a RelationalProjectedRow) -> Self {
        Self {
            row: RelationalProjectedRowView::Owned(row),
        }
    }

    pub fn value(self, ordinal: usize) -> Result<RelationalValueRef<'a>> {
        self.row.value(ordinal).ok_or_else(|| {
            SkeinError::StorageIntegrity(format!(
                "relational row projection omitted required field {ordinal}"
            ))
        })
    }
}

enum RelationalRowBackend {
    CanonicalMemory,
    Snapshot(RelationalRowPageSnapshotReader),
}

pub struct RelationalRowRuntime<'a> {
    state: &'a RelationalState,
    backend: RelationalRowBackend,
    fields: RelationalFieldPlan,
    limits: RelationalRowPageSnapshotReadLimits,
    hydration: RefCell<RelationalHydrationBudget>,
    evidence: RefCell<RelationalRowExecutionEvidence>,
    task: &'a RuntimeTaskContext,
    projection: Option<ProjectionRelationalRuntime<'a>>,
}

struct ProjectionRelationalRuntime<'a> {
    reader: &'a ProjectionGenerationReader,
    tables: &'a BTreeSet<String>,
}

impl<'a> RelationalRowRuntime<'a> {
    pub fn new(
        state: &'a RelationalState,
        snapshot: Option<RelationalRowPageSnapshotReader>,
        projection: Option<(&'a ProjectionGenerationReader, &'a BTreeSet<String>)>,
        fields: RelationalFieldPlan,
        limits: RelationalRowPageSnapshotReadLimits,
        hydration: RelationalHydrationBudget,
        task: &'a RuntimeTaskContext,
    ) -> Self {
        let projection = projection
            .filter(|(_, tables)| fields.uses_any_table(tables))
            .map(|(reader, tables)| ProjectionRelationalRuntime { reader, tables });
        let backend = snapshot.map_or(
            RelationalRowBackend::CanonicalMemory,
            RelationalRowBackend::Snapshot,
        );
        let runtime_path = match (&backend, &projection) {
            (_, Some(_)) => "projection_generation",
            (RelationalRowBackend::CanonicalMemory, None) => "canonical_memory",
            (RelationalRowBackend::Snapshot(_), None) => "snapshot_rows",
        };
        let projection_evidence = projection.as_ref().map(|projection| {
            (
                projection
                    .reader
                    .manifest()
                    .begin
                    .identity
                    .generation
                    .clone(),
                projection.reader.manifest().begin.source_watermark,
                projection.reader.manifest().begin.projection_version,
                projection.reader.publication_commit_epoch(),
            )
        });
        Self {
            state,
            backend,
            fields,
            limits,
            hydration: RefCell::new(hydration),
            evidence: RefCell::new(RelationalRowExecutionEvidence {
                runtime_path,
                projection_generation: projection_evidence
                    .as_ref()
                    .map(|evidence| evidence.0.clone()),
                projection_source_watermark: projection_evidence
                    .as_ref()
                    .map(|evidence| evidence.1),
                projection_version: projection_evidence.as_ref().map(|evidence| evidence.2),
                projection_publication_commit_epoch: projection_evidence
                    .as_ref()
                    .map(|evidence| evidence.3),
                ..RelationalRowExecutionEvidence::default()
            }),
            task,
            projection,
        }
    }

    pub fn hydration(&self) -> RelationalHydrationBudget {
        *self.hydration.borrow()
    }

    pub fn evidence(&self) -> RelationalRowExecutionEvidence {
        self.evidence.borrow().clone()
    }

    pub fn read_point(
        &self,
        table: &str,
        key: &RelationalKey,
    ) -> Result<Option<RelationalReadRow>> {
        let fields = self.fields.scan_fields(table)?;
        let hydration_fields = self.fields.scan_hydration_fields(table)?;
        self.read_point_with_fields(table, key, fields, hydration_fields)
    }

    pub fn read_output_point(
        &self,
        table: &str,
        key: &RelationalKey,
    ) -> Result<Option<RelationalReadRow>> {
        let fields = self.fields.output_fields(table)?;
        self.read_point_with_fields(table, key, fields, fields)
    }

    /// Reads each distinct key once while retaining the caller-owned mapping
    /// from duplicate input keys to their output rows.
    pub fn read_points(
        &self,
        table: &str,
        keys: &[RelationalKey],
    ) -> Result<BTreeMap<RelationalKey, RelationalReadRow>> {
        let fields = self.fields.scan_fields(table)?;
        let hydration_fields = self.fields.scan_hydration_fields(table)?;
        self.read_points_with_fields(table, keys, fields, hydration_fields)
    }

    /// Builds a scan projection directly from a secondary-index key and its
    /// primary-key locator. It is valid only when the query field plan is
    /// completely covered by those two key tuples.
    pub fn read_index_covered(
        &self,
        table: &str,
        index_columns: &[String],
        index_key: &RelationalKey,
        primary_key: &RelationalKey,
    ) -> Result<Option<RelationalReadRow>> {
        let schema = self
            .state
            .table_schema(table)
            .ok_or_else(|| SkeinError::Semantic(format!("unknown relational table {table}")))?;
        if !self
            .fields
            .index_covers_table(table, schema, index_columns)?
        {
            return Ok(None);
        }
        if index_key.0.len() != index_columns.len() {
            return Err(SkeinError::StorageIntegrity(format!(
                "relational index key for table {table} has {} values but its descriptor has {} columns",
                index_key.0.len(),
                index_columns.len()
            )));
        }
        if primary_key.0.len() != schema.primary_key.len() {
            return Err(SkeinError::StorageIntegrity(format!(
                "relational primary-key locator for table {table} has {} values but the schema has {} primary-key columns",
                primary_key.0.len(),
                schema.primary_key.len()
            )));
        }
        // Covering index values still belong to the pinned row snapshot.
        self.bind_snapshot_identity()?;

        let mut values = BTreeMap::new();
        for (column, value) in index_columns.iter().zip(&index_key.0) {
            let ordinal = schema.column_position(column).ok_or_else(|| {
                SkeinError::StorageIntegrity(format!(
                    "relational index coverage references unknown column {column} on table {table}"
                ))
            })?;
            values.insert(ordinal, value.clone());
        }
        for (column, value) in schema.primary_key.iter().zip(&primary_key.0) {
            let ordinal = schema.column_position(column).ok_or_else(|| {
                SkeinError::StorageIntegrity(format!(
                    "relational primary-key coverage references unknown column {column} on table {table}"
                ))
            })?;
            values.entry(ordinal).or_insert_with(|| value.clone());
        }
        let fields = self.fields.scan_fields(table)?;
        let fields = fields
            .iter()
            .map(|ordinal| {
                values
                    .get(ordinal)
                    .cloned()
                    .map(|value| RelationalProjectedField {
                        ordinal: *ordinal,
                        value,
                    })
                    .ok_or_else(|| {
                        SkeinError::StorageIntegrity(format!(
                            "relational index coverage omitted required field {ordinal} on table {table}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut projected = RelationalProjectedRow {
            primary_key: primary_key.clone(),
            fields,
        };
        let hydration_fields = self.fields.scan_hydration_fields(table)?;
        self.state
            .hydrate_projected_row_fields_with_context(
                table,
                &mut projected,
                hydration_fields,
                &mut self.hydration.borrow_mut(),
                Some(self.task),
            )
            .map_err(map_state_error)?;
        let mut evidence = self.evidence.borrow_mut();
        add_counter(
            &mut evidence.index_covered_rows,
            1,
            "relational index-covered row",
        )?;
        Ok(Some(RelationalReadRow {
            row: Arc::new(projected),
        }))
    }

    fn read_point_with_fields(
        &self,
        table: &str,
        key: &RelationalKey,
        fields: &[usize],
        hydration_fields: &[usize],
    ) -> Result<Option<RelationalReadRow>> {
        if self
            .projection
            .as_ref()
            .is_some_and(|projection| projection.tables.contains(table))
        {
            return self.read_projection_point(table, key, fields, hydration_fields);
        }
        match &self.backend {
            RelationalRowBackend::CanonicalMemory => {
                let Some((key, row)) = self.state.row_entry(table, key) else {
                    return Ok(None);
                };
                self.admit_memory_row()?;
                self.project_memory_row(table, key, row, fields, hydration_fields)
                    .map(Some)
            }
            RelationalRowBackend::Snapshot(reader) => {
                let remaining = self.remaining_limits()?;
                let mut hydration = self.hydration.borrow_mut();
                let (mut row, report) = reader
                    .point_projected_fields(
                        table,
                        key,
                        RelationalRowPageProjectedFields {
                            requested_fields: fields,
                            hydration_fields,
                        },
                        remaining,
                        &mut hydration,
                        self.task,
                    )
                    .map_err(map_snapshot_error)?;
                let requires_state_resolution = report.source
                    == RelationalRowPageSnapshotRowSource::Live
                    || (report.source == RelationalRowPageSnapshotRowSource::Recovery
                        && !reader.has_overlay_overflow_root());
                if requires_state_resolution && let Some(row) = &mut row {
                    self.state
                        .hydrate_projected_row_fields_with_context(
                            table,
                            row,
                            hydration_fields,
                            &mut hydration,
                            Some(self.task),
                        )
                        .map_err(map_state_error)?;
                }
                drop(hydration);
                self.record_point(&report)?;
                Ok(row.map(|row| RelationalReadRow { row: Arc::new(row) }))
            }
        }
    }

    fn read_points_with_fields(
        &self,
        table: &str,
        keys: &[RelationalKey],
        fields: &[usize],
        hydration_fields: &[usize],
    ) -> Result<BTreeMap<RelationalKey, RelationalReadRow>> {
        let keys = keys.iter().cloned().collect::<BTreeSet<_>>();
        if self
            .projection
            .as_ref()
            .is_some_and(|projection| projection.tables.contains(table))
        {
            let mut rows = BTreeMap::new();
            for key in keys {
                if let Some(row) =
                    self.read_projection_point(table, &key, fields, hydration_fields)?
                {
                    rows.insert(key, row);
                }
            }
            return Ok(rows);
        }
        match &self.backend {
            RelationalRowBackend::CanonicalMemory => {
                let mut rows = BTreeMap::new();
                for key in keys {
                    self.task.checkpoint().map_err(|reason| {
                        SkeinError::Execution(format!("runtime task stopped: {reason}"))
                    })?;
                    let Some((key, row)) = self.state.row_entry(table, &key) else {
                        continue;
                    };
                    self.admit_memory_row()?;
                    rows.insert(
                        key.clone(),
                        self.project_memory_row(table, key, row, fields, hydration_fields)?,
                    );
                }
                Ok(rows)
            }
            RelationalRowBackend::Snapshot(reader) => {
                let remaining = self.remaining_limits()?;
                let mut hydration = self.hydration.borrow_mut();
                let (mut rows, report) = reader
                    .points_projected_fields(
                        table,
                        &keys.into_iter().collect::<Vec<_>>(),
                        RelationalRowPageProjectedFields {
                            requested_fields: fields,
                            hydration_fields,
                        },
                        remaining,
                        &mut hydration,
                        self.task,
                    )
                    .map_err(map_snapshot_error)?;
                for key in &report.unbound_overlay_keys {
                    let row = rows.get_mut(key).ok_or_else(|| {
                        SkeinError::StorageIntegrity(
                            "snapshot multi-point resolver lost an overlay row".to_string(),
                        )
                    })?;
                    self.state
                        .hydrate_projected_row_fields_with_context(
                            table,
                            row,
                            hydration_fields,
                            &mut hydration,
                            Some(self.task),
                        )
                        .map_err(map_state_error)?;
                }
                drop(hydration);
                self.record_points(&report)?;
                Ok(rows
                    .into_iter()
                    .map(|(key, row)| (key, RelationalReadRow { row: Arc::new(row) }))
                    .collect())
            }
        }
    }

    pub fn visit_all(
        &self,
        table: &str,
        mut visit: impl FnMut(RelationalReadRow) -> Result<bool>,
    ) -> Result<bool> {
        let fields = self.fields.scan_fields(table)?;
        let hydration_fields = self.fields.scan_hydration_fields(table)?;
        if self
            .projection
            .as_ref()
            .is_some_and(|projection| projection.tables.contains(table))
        {
            return self.visit_projection_rows(table, fields, hydration_fields, &mut visit);
        }
        match &self.backend {
            RelationalRowBackend::CanonicalMemory => {
                for (key, row) in self.state.rows(table) {
                    self.task.checkpoint().map_err(|reason| {
                        SkeinError::Execution(format!("runtime task stopped: {reason}"))
                    })?;
                    self.admit_memory_row()?;
                    if !visit(self.project_memory_row(
                        table,
                        key,
                        row,
                        fields,
                        hydration_fields,
                    )?)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            RelationalRowBackend::Snapshot(reader) => {
                let remaining = self.remaining_limits()?;
                let mut hydration = *self.hydration.borrow();
                let state = self.state;
                let task = self.task;
                let mut callback_error = None;
                let read_result = reader
                    .visit_projected_range_fields_resolving(
                        RelationalRowPageProjectedRangeFields {
                            range: skein_storage::RelationalRowPageProjectedRange {
                                table,
                                lower: Bound::Unbounded,
                                upper: Bound::Unbounded,
                                requested_fields: fields,
                            },
                            hydration_fields,
                        },
                        remaining,
                        &mut hydration,
                        task,
                        |row, budget, task| {
                            state
                                .hydrate_projected_row_fields_with_context(
                                    table,
                                    row,
                                    hydration_fields,
                                    budget,
                                    Some(task),
                                )
                                .map_err(map_state_to_demand_error)
                        },
                        |row, range_hydration| {
                            self.hydration.replace(*range_hydration);
                            let keep_going = match visit(RelationalReadRow { row: Arc::new(row) }) {
                                Ok(keep_going) => keep_going,
                                Err(error) => {
                                    callback_error = Some(error);
                                    false
                                }
                            };
                            *range_hydration = *self.hydration.borrow();
                            keep_going
                        },
                    )
                    .map_err(map_snapshot_error);
                self.hydration.replace(hydration);
                let report = read_result?;
                self.record_range(&report)?;
                match callback_error {
                    Some(error) => Err(error),
                    None => Ok(!report.demand.stopped_early),
                }
            }
        }
    }

    pub fn visit_all_ref(
        &self,
        table: &str,
        mut visit: impl for<'row> FnMut(RelationalReadRowRef<'row>) -> Result<bool>,
    ) -> Result<bool> {
        let fields = self.fields.scan_fields(table)?;
        let hydration_fields = self.fields.scan_hydration_fields(table)?;
        if self
            .projection
            .as_ref()
            .is_some_and(|projection| projection.tables.contains(table))
        {
            return self.visit_projection_rows(table, fields, hydration_fields, &mut |row| {
                visit(row.as_ref())
            });
        }
        match &self.backend {
            RelationalRowBackend::CanonicalMemory => {
                for (key, row) in self.state.rows(table) {
                    self.task.checkpoint().map_err(|reason| {
                        SkeinError::Execution(format!("runtime task stopped: {reason}"))
                    })?;
                    self.admit_memory_row()?;
                    let projected =
                        self.project_memory_row(table, key, row, fields, hydration_fields)?;
                    if !visit(projected.as_ref())? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            RelationalRowBackend::Snapshot(reader) => {
                let remaining = self.remaining_limits()?;
                let mut hydration = *self.hydration.borrow();
                let state = self.state;
                let task = self.task;
                let mut callback_error = None;
                let read_result = reader
                    .visit_projected_range_fields_resolving_ref(
                        RelationalRowPageProjectedRangeFields {
                            range: skein_storage::RelationalRowPageProjectedRange {
                                table,
                                lower: Bound::Unbounded,
                                upper: Bound::Unbounded,
                                requested_fields: fields,
                            },
                            hydration_fields,
                        },
                        remaining,
                        &mut hydration,
                        task,
                        |row, budget, task| {
                            state
                                .hydrate_projected_row_fields_with_context(
                                    table,
                                    row,
                                    hydration_fields,
                                    budget,
                                    Some(task),
                                )
                                .map_err(map_state_to_demand_error)
                        },
                        |row, range_hydration| {
                            self.hydration.replace(*range_hydration);
                            let keep_going = match visit(RelationalReadRowRef { row }) {
                                Ok(keep_going) => keep_going,
                                Err(error) => {
                                    callback_error = Some(error);
                                    false
                                }
                            };
                            *range_hydration = *self.hydration.borrow();
                            keep_going
                        },
                    )
                    .map_err(map_snapshot_error);
                self.hydration.replace(hydration);
                let report = read_result?;
                self.record_range(&report)?;
                match callback_error {
                    Some(error) => Err(error),
                    None => Ok(!report.demand.stopped_early),
                }
            }
        }
    }

    fn read_projection_point(
        &self,
        table: &str,
        key: &RelationalKey,
        fields: &[usize],
        hydration_fields: &[usize],
    ) -> Result<Option<RelationalReadRow>> {
        let encoded_key = encode_relational_primary_key(key).map_err(|error| {
            SkeinError::StorageIntegrity(format!(
                "projection lookup key for {table} cannot be encoded: {error}"
            ))
        })?;
        let mut found = None;
        let cursor = self
            .projection
            .as_ref()
            .ok_or_else(|| {
                SkeinError::StorageIntegrity(
                    "projection row path was selected without a pinned generation".to_string(),
                )
            })?
            .reader
            .seek_cursor(table, &encoded_key)
            .map_err(map_projection_read_error)?;
        self.visit_projection_members_from(table, Some(cursor), &mut |member| match member
            .key
            .as_slice()
            .cmp(encoded_key.as_slice())
        {
            std::cmp::Ordering::Less => Ok(true),
            std::cmp::Ordering::Equal => {
                found = Some(self.project_projection_member(
                    table,
                    member,
                    fields,
                    hydration_fields,
                )?);
                Ok(false)
            }
            std::cmp::Ordering::Greater => Ok(false),
        })?;
        Ok(found)
    }

    fn visit_projection_rows(
        &self,
        table: &str,
        fields: &[usize],
        hydration_fields: &[usize],
        visit: &mut dyn FnMut(RelationalReadRow) -> Result<bool>,
    ) -> Result<bool> {
        self.visit_projection_members(table, &mut |member| {
            visit(self.project_projection_member(table, member, fields, hydration_fields)?)
        })
    }

    fn visit_projection_members(
        &self,
        table: &str,
        visit: &mut dyn FnMut(&skein_storage::ProjectionGenerationMember) -> Result<bool>,
    ) -> Result<bool> {
        let projection = self.projection.as_ref().ok_or_else(|| {
            SkeinError::StorageIntegrity(
                "projection row path was selected without a pinned generation".to_string(),
            )
        })?;
        let cursor = projection
            .reader
            .seek_prefix_cursor(table, &[])
            .map_err(map_projection_read_error)?;
        self.visit_projection_members_from(table, Some(cursor), visit)
    }

    fn visit_projection_members_from(
        &self,
        table: &str,
        mut cursor: Option<skein_storage::ProjectionGenerationCursor>,
        visit: &mut dyn FnMut(&skein_storage::ProjectionGenerationMember) -> Result<bool>,
    ) -> Result<bool> {
        let projection = self.projection.as_ref().ok_or_else(|| {
            SkeinError::StorageIntegrity(
                "projection row path was selected without a pinned generation".to_string(),
            )
        })?;
        loop {
            self.task.checkpoint().map_err(|reason| {
                SkeinError::Execution(format!("runtime task stopped: {reason}"))
            })?;
            let page = projection
                .reader
                .read_page(cursor.as_ref(), self.projection_page_limits()?)
                .map_err(map_projection_read_error)?;
            self.record_projection_page(&page.report)?;
            for member in &page.members {
                if !projection.tables.contains(&member.collection) {
                    return Err(SkeinError::StorageIntegrity(format!(
                        "projection generation contains unbound collection {}",
                        member.collection
                    )));
                }
                match member.collection.as_str().cmp(table) {
                    std::cmp::Ordering::Less => continue,
                    std::cmp::Ordering::Equal => {
                        if !visit(member)? {
                            return Ok(false);
                        }
                    }
                    std::cmp::Ordering::Greater => return Ok(true),
                }
            }
            let Some(next) = page.next else {
                return Ok(true);
            };
            cursor = Some(next);
        }
    }

    fn project_projection_member(
        &self,
        table: &str,
        member: &skein_storage::ProjectionGenerationMember,
        fields: &[usize],
        hydration_fields: &[usize],
    ) -> Result<RelationalReadRow> {
        let schema = self
            .state
            .table_schema(table)
            .ok_or_else(|| SkeinError::Semantic(format!("unknown relational table {table}")))?;
        let (primary_key, row) =
            decode_projection_relational_member(schema, member, member.payload.len())
                .map_err(map_projection_read_error)?;
        let mut required = fields.iter().copied().collect::<BTreeSet<_>>();
        required.extend(hydration_fields.iter().copied());
        let projected = required
            .into_iter()
            .map(|ordinal| {
                row.values()
                    .get(ordinal)
                    .cloned()
                    .map(|value| RelationalProjectedField { ordinal, value })
                    .ok_or_else(|| {
                        SkeinError::StorageIntegrity(format!(
                            "projection row field {ordinal} is outside table {table}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RelationalReadRow {
            row: Arc::new(RelationalProjectedRow {
                primary_key,
                fields: projected,
            }),
        })
    }

    fn projection_page_limits(&self) -> Result<ProjectionGenerationReadLimits> {
        let evidence = self.evidence.borrow();
        let rows = self
            .limits
            .demand
            .max_rows
            .get()
            .checked_sub(evidence.rows_visited)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                SkeinError::Execution(format!(
                    "projection generation row budget is exhausted at {}",
                    self.limits.demand.max_rows
                ))
            })?;
        let bytes = self
            .limits
            .demand
            .max_bytes
            .get()
            .checked_sub(evidence.logical_bytes)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                SkeinError::Execution(format!(
                    "projection generation payload budget is exhausted at {} bytes",
                    self.limits.demand.max_bytes
                ))
            })?;
        Ok(ProjectionGenerationReadLimits {
            max_rows: NonZeroUsize::new(rows.get().min(256)).expect("bounded rows are non-zero"),
            max_payload_bytes: bytes,
            max_record_bytes: bytes,
        })
    }

    fn record_projection_page(
        &self,
        report: &skein_storage::ProjectionGenerationReadReport,
    ) -> Result<()> {
        let mut evidence = self.evidence.borrow_mut();
        if evidence.projection_generation.as_deref() != Some(report.generation.as_str())
            || evidence.projection_source_watermark != Some(report.source_watermark)
            || evidence.projection_version != Some(report.projection_version)
            || evidence.projection_publication_commit_epoch != Some(report.publication_commit_epoch)
        {
            return Err(SkeinError::StorageIntegrity(
                "projection generation identity changed within one SQL transaction".to_string(),
            ));
        }
        add_counter(&mut evidence.logical_pages, 1, "projection page")?;
        add_counter(
            &mut evidence.logical_bytes,
            report.payload_bytes,
            "projection payload byte",
        )?;
        add_counter(
            &mut evidence.rows_visited,
            report.rows_returned,
            "projection row",
        )?;
        add_counter(
            &mut evidence.owned_rows_visited,
            report.rows_returned,
            "projection owned row",
        )?;
        Ok(())
    }

    fn project_memory_row(
        &self,
        table: &str,
        key: &RelationalKey,
        row: &RelationalRow,
        fields: &[usize],
        hydration_fields: &[usize],
    ) -> Result<RelationalReadRow> {
        let mut projected = RelationalProjectedRow {
            primary_key: key.clone(),
            fields: fields
                .iter()
                .map(|ordinal| {
                    row.values()
                        .get(*ordinal)
                        .cloned()
                        .map(|value| RelationalProjectedField {
                            ordinal: *ordinal,
                            value,
                        })
                        .ok_or_else(|| {
                            SkeinError::StorageIntegrity(format!(
                                "relational field {ordinal} is outside row shape for table {table}"
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?,
        };
        self.state
            .hydrate_projected_row_fields_with_context(
                table,
                &mut projected,
                hydration_fields,
                &mut self.hydration.borrow_mut(),
                Some(self.task),
            )
            .map_err(map_state_error)?;
        Ok(RelationalReadRow {
            row: Arc::new(projected),
        })
    }

    fn admit_memory_row(&self) -> Result<()> {
        let mut evidence = self.evidence.borrow_mut();
        let next_rows = evidence
            .rows_visited
            .checked_add(1)
            .ok_or_else(|| SkeinError::Execution("relational row count overflow".to_string()))?;
        if next_rows > self.limits.demand.max_rows.get() {
            return Err(SkeinError::Execution(format!(
                "relational row scan exceeds row limit {}",
                self.limits.demand.max_rows
            )));
        }
        let next_owned_rows = evidence.owned_rows_visited.checked_add(1).ok_or_else(|| {
            SkeinError::Execution("relational owned row count overflow".to_string())
        })?;
        evidence.rows_visited = next_rows;
        evidence.owned_rows_visited = next_owned_rows;
        Ok(())
    }

    fn remaining_limits(&self) -> Result<RelationalRowPageSnapshotReadLimits> {
        let evidence = self.evidence.borrow();
        let remaining = |limit: NonZeroUsize, used: usize, name: &str| {
            limit
                .get()
                .checked_sub(used)
                .and_then(NonZeroUsize::new)
                .ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "relational row {name} budget is exhausted at {}",
                        limit.get()
                    ))
                })
        };
        Ok(RelationalRowPageSnapshotReadLimits {
            demand: skein_storage::RelationalRowPageDemandReadLimits {
                max_pages: remaining(self.limits.demand.max_pages, evidence.logical_pages, "page")?,
                max_rows: remaining(self.limits.demand.max_rows, evidence.rows_visited, "row")?,
                max_bytes: remaining(self.limits.demand.max_bytes, evidence.logical_bytes, "byte")?,
                max_pins: self.limits.demand.max_pins,
                max_tree_height: self.limits.demand.max_tree_height,
            },
            max_overlay_entries: remaining(
                self.limits.max_overlay_entries,
                evidence.overlay_entries,
                "overlay-entry",
            )?,
            max_overlay_bytes: remaining(
                self.limits.max_overlay_bytes,
                evidence.overlay_resident_bytes,
                "overlay-byte",
            )?,
        })
    }

    fn record_point(&self, report: &RelationalRowPageSnapshotPointReport) -> Result<()> {
        let overlay_entries = usize::from(matches!(
            report.source,
            RelationalRowPageSnapshotRowSource::Recovery
                | RelationalRowPageSnapshotRowSource::Live
                | RelationalRowPageSnapshotRowSource::Deleted
        ));
        self.record(
            report.identity,
            &report.demand,
            overlay_entries,
            report.overlay_resident_bytes,
        )
    }

    fn record_range(&self, report: &RelationalRowPageSnapshotRangeReport) -> Result<()> {
        self.record(
            report.identity,
            &report.demand,
            report.overlay_entries,
            report.overlay_resident_bytes,
        )
    }

    fn record_points(&self, report: &RelationalRowPageSnapshotPointsReport) -> Result<()> {
        self.record(
            report.identity,
            &report.demand,
            report.overlay_entries,
            report.overlay_resident_bytes,
        )
    }

    fn bind_snapshot_identity(&self) -> Result<()> {
        let RelationalRowBackend::Snapshot(reader) = &self.backend else {
            return Ok(());
        };
        self.record(reader.identity(), &Default::default(), 0, 0)
    }

    fn record(
        &self,
        identity: RelationalRowPageReadViewIdentity,
        demand: &skein_storage::RelationalRowPageDemandReadReport,
        overlay_entries: usize,
        overlay_resident_bytes: usize,
    ) -> Result<()> {
        let mut evidence = self.evidence.borrow_mut();
        let observed = (
            Some(identity.base_generation),
            identity.delta_generation,
            Some(identity.base_commit_epoch),
            Some(identity.visible_commit_epoch),
            Some(identity.root_set_digest.to_string()),
        );
        let expected = (
            evidence.base_generation,
            evidence.delta_generation,
            evidence.base_commit_epoch,
            evidence.visible_commit_epoch,
            evidence.root_set_digest.clone(),
        );
        if evidence.base_generation.is_some() && expected != observed {
            return Err(SkeinError::StorageIntegrity(
                "relational row view identity changed within one SQL statement".to_string(),
            ));
        }
        evidence.base_generation = observed.0;
        evidence.delta_generation = observed.1;
        evidence.base_commit_epoch = observed.2;
        evidence.visible_commit_epoch = observed.3;
        evidence.root_set_digest = observed.4;
        add_counter(
            &mut evidence.descriptor_reads,
            demand.descriptor_reads,
            "descriptor",
        )?;
        add_counter(
            &mut evidence.logical_pages,
            demand.pages_read,
            "logical page",
        )?;
        add_counter(
            &mut evidence.logical_bytes,
            demand.bytes_read,
            "logical byte",
        )?;
        add_counter(
            &mut evidence.file_pages,
            demand.file_pages_read,
            "file page",
        )?;
        add_counter(
            &mut evidence.file_bytes,
            demand.file_bytes_read,
            "file byte",
        )?;
        add_counter(&mut evidence.cache_hits, demand.cache_hits, "cache hit")?;
        add_counter(
            &mut evidence.cache_misses,
            demand.cache_misses,
            "cache miss",
        )?;
        add_counter(
            &mut evidence.cache_admission_rejections,
            demand.cache_admission_rejections,
            "cache rejection",
        )?;
        add_counter(&mut evidence.rows_visited, demand.rows_emitted, "row")?;
        add_counter(
            &mut evidence.borrowed_rows_visited,
            demand.borrowed_rows_emitted,
            "borrowed row",
        )?;
        add_counter(
            &mut evidence.owned_rows_visited,
            demand.owned_rows_emitted,
            "owned row",
        )?;
        add_counter(
            &mut evidence.overlay_entries,
            overlay_entries,
            "overlay entry",
        )?;
        add_counter(
            &mut evidence.overlay_resident_bytes,
            overlay_resident_bytes,
            "overlay byte",
        )?;
        Ok(())
    }
}

fn map_projection_read_error(error: ProjectionGenerationError) -> SkeinError {
    match error {
        ProjectionGenerationError::Admission(message) => SkeinError::Execution(message),
        ProjectionGenerationError::Io(error) => SkeinError::StorageIntegrity(error.to_string()),
        error @ (ProjectionGenerationError::Conflict(_)
        | ProjectionGenerationError::Corruption(_)
        | ProjectionGenerationError::NotFound(_)) => {
            SkeinError::StorageIntegrity(error.to_string())
        }
    }
}

fn map_state_to_demand_error(error: RelationalError) -> RelationalRowPageDemandReadError {
    match error {
        RelationalError::Admission(message) => RelationalRowPageDemandReadError::Admission(message),
        RelationalError::Durability(message) => {
            RelationalRowPageDemandReadError::Durability(message)
        }
        RelationalError::Schema(message)
        | RelationalError::Constraint(message)
        | RelationalError::Corruption(message) => {
            RelationalRowPageDemandReadError::Corrupt(message)
        }
    }
}

fn map_state_error(error: RelationalError) -> SkeinError {
    match error {
        RelationalError::Admission(message) => SkeinError::Execution(message),
        RelationalError::Durability(message)
        | RelationalError::Schema(message)
        | RelationalError::Constraint(message)
        | RelationalError::Corruption(message) => SkeinError::StorageIntegrity(message),
    }
}

fn map_snapshot_error(error: RelationalRowPageSnapshotReadError) -> SkeinError {
    match error {
        RelationalRowPageSnapshotReadError::Admission(message) => SkeinError::Execution(message),
        RelationalRowPageSnapshotReadError::Stopped(reason) => {
            SkeinError::Execution(format!("runtime task stopped: {reason}"))
        }
        RelationalRowPageSnapshotReadError::Corrupt(message)
        | RelationalRowPageSnapshotReadError::Durability(message)
        | RelationalRowPageSnapshotReadError::MissingTable(message) => {
            SkeinError::StorageIntegrity(message)
        }
    }
}

fn add_counter(counter: &mut usize, value: usize, name: &str) -> Result<()> {
    *counter = counter.checked_add(value).ok_or_else(|| {
        SkeinError::StorageIntegrity(format!("relational row {name} counter overflow"))
    })?;
    Ok(())
}
