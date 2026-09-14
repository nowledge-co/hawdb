//! Bounded transaction row overlays and sparse live workspace hydration.
//!
//! The facade selects snapshots and policy. Storage owns row admission,
//! constraint closure, and read-your-own-writes state before WAL publication.

mod metrics;
#[cfg(test)]
mod tests;
mod transaction;
pub use metrics::RelationalMonotonicAppendMetrics;
pub use transaction::RelationalTransactionRowView;

use crate::{
    RelationalConstraintIndex, RelationalError, RelationalHydrationBudget,
    RelationalIndexChangeCaptureLimits, RelationalMonotonicAppendHydration, RelationalProjectedRow,
    RelationalReplayAccess, RelationalRow, RelationalRowChangeCaptureLimits,
    RelationalRowPageDemandReadError, RelationalRowPageProjectedRange,
    RelationalRowPageReadViewIdentity, RelationalRowPageSnapshotPointReport,
    RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError,
    RelationalRowPageSnapshotReadLimits, RelationalRowPageSnapshotReader,
    RelationalRowPageSnapshotRowSource, RelationalSparseIndexProbe,
    RelationalSparseLivePreparationStage, RelationalSparseRecoveryRow,
    RelationalSparseWorkspaceBuilder, RelationalState, RelationalTransaction,
    RELATIONAL_PRIMARY_INDEX_NAME,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    ops::Bound,
    sync::Arc,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelationalSparseLiveHydrationReport {
    pub point_reads: usize,
    pub range_reads: usize,
    pub monotonic_append_attempts: usize,
    pub monotonic_append_hits: usize,
    pub monotonic_append_fallbacks: usize,
    pub pages_read: usize,
    pub rows_decoded: usize,
    pub bytes_read: usize,
}

pub struct RelationalSparseLiveWorkspace {
    pub rows: Vec<RelationalSparseRecoveryRow>,
    pub proven_absent_primary_keys: BTreeSet<RelationalReplayAccess>,
}

pub struct RelationalProvenAbsenceConstraintIndex<'a> {
    inner: &'a dyn RelationalConstraintIndex,
    proven_absent_primary_keys: &'a BTreeSet<RelationalReplayAccess>,
}

impl<'a> RelationalProvenAbsenceConstraintIndex<'a> {
    pub fn new(
        inner: &'a dyn RelationalConstraintIndex,
        proven_absent_primary_keys: &'a BTreeSet<RelationalReplayAccess>,
    ) -> Self {
        Self {
            inner,
            proven_absent_primary_keys,
        }
    }
}

impl RelationalConstraintIndex for RelationalProvenAbsenceConstraintIndex<'_> {
    fn visit_exact_primary_keys(
        &self,
        table: &str,
        index: &str,
        key: &crate::RelationalKey,
        visit: &mut dyn FnMut(&crate::RelationalKey) -> bool,
    ) -> Result<(), RelationalError> {
        if index == RELATIONAL_PRIMARY_INDEX_NAME
            && self
                .proven_absent_primary_keys
                .contains(&RelationalReplayAccess {
                    table: table.to_string(),
                    primary_key: key.clone(),
                })
        {
            return Ok(());
        }
        self.inner
            .visit_exact_primary_keys(table, index, key, visit)
    }
}

struct RelationalSparseLiveHydrator<'a> {
    state: &'a RelationalState,
    reader: RelationalRowPageSnapshotReader,
    workspace: RelationalSparseWorkspaceBuilder,
    requested_fields: BTreeMap<String, Arc<[usize]>>,
    resolved_probes: BTreeSet<RelationalSparseIndexProbe>,
    hydration: RelationalHydrationBudget,
    task: skein_core::RuntimeTaskContext,
    limits: RelationalRowPageSnapshotReadLimits,
    pages_read: usize,
    rows_decoded: usize,
    bytes_read: usize,
    overlay_entries: usize,
    overlay_resident_bytes: usize,
    point_reads: usize,
    range_reads: usize,
    monotonic_append_attempts: usize,
    monotonic_append_hits: usize,
    monotonic_append_fallbacks: usize,
    proven_absent_primary_keys: BTreeSet<RelationalReplayAccess>,
    monotonic_append_metrics: Arc<RelationalMonotonicAppendMetrics>,
}

impl<'a> RelationalSparseLiveHydrator<'a> {
    fn new(
        state: &'a RelationalState,
        reader: RelationalRowPageSnapshotReader,
        workspace_limits: RelationalRowChangeCaptureLimits,
        monotonic_append_metrics: Arc<RelationalMonotonicAppendMetrics>,
    ) -> Self {
        let mut limits = RelationalRowPageSnapshotReadLimits::default();
        limits.demand.max_rows = nonzero_min(limits.demand.max_rows, workspace_limits.max_entries);
        limits.demand.max_bytes = nonzero_min(limits.demand.max_bytes, workspace_limits.max_bytes);
        limits.max_overlay_entries =
            nonzero_min(limits.max_overlay_entries, workspace_limits.max_entries);
        limits.max_overlay_bytes =
            nonzero_min(limits.max_overlay_bytes, workspace_limits.max_bytes);
        let max_rows = limits.demand.max_rows.get();
        let max_bytes = limits.demand.max_bytes.get();
        Self {
            state,
            reader,
            workspace: RelationalSparseWorkspaceBuilder::new(workspace_limits),
            requested_fields: BTreeMap::new(),
            resolved_probes: BTreeSet::new(),
            hydration: RelationalHydrationBudget {
                max_rows,
                max_compressed_bytes: max_bytes,
                max_decompressed_bytes: max_bytes,
                max_memory_bytes: max_bytes,
                ..RelationalHydrationBudget::default()
            },
            task: skein_core::RuntimeTaskContext::default(),
            limits,
            pages_read: 0,
            rows_decoded: 0,
            bytes_read: 0,
            overlay_entries: 0,
            overlay_resident_bytes: 0,
            point_reads: 0,
            range_reads: 0,
            monotonic_append_attempts: 0,
            monotonic_append_hits: 0,
            monotonic_append_fallbacks: 0,
            proven_absent_primary_keys: BTreeSet::new(),
            monotonic_append_metrics,
        }
    }

    fn workspace_snapshot(&self) -> Vec<RelationalSparseRecoveryRow> {
        self.workspace.snapshot()
    }

    fn fields(&mut self, table: &str) -> Result<Arc<[usize]>, RelationalError> {
        if let Some(fields) = self.requested_fields.get(table) {
            return Ok(Arc::clone(fields));
        }
        let schema = self.state.table_schema(table).ok_or_else(|| {
            RelationalError::Corruption(format!(
                "sparse relational live hydration references unknown table {table}"
            ))
        })?;
        let fields = Arc::<[usize]>::from((0..schema.columns.len()).collect::<Vec<_>>());
        self.requested_fields
            .insert(table.to_string(), Arc::clone(&fields));
        Ok(fields)
    }

    fn hydrate_point(&mut self, access: &RelationalReplayAccess) -> Result<bool, RelationalError> {
        if self.workspace.contains(access) {
            return Ok(false);
        }
        self.point_reads = self.point_reads.checked_add(1).ok_or_else(|| {
            RelationalError::Admission(
                "sparse relational live point-read counter overflow".to_string(),
            )
        })?;
        let fields = self.fields(&access.table)?;
        let limits = self.remaining_limits()?;
        let (mut projected, report) = self
            .reader
            .point_projected(
                &access.table,
                &access.primary_key,
                &fields,
                limits,
                &mut self.hydration,
                &self.task,
            )
            .map_err(map_sparse_live_snapshot_error)?;
        if projected
            .as_ref()
            .is_some_and(|row| row.primary_key != access.primary_key)
        {
            return Err(RelationalError::Corruption(format!(
                "sparse relational live point read returned the wrong primary key for table {}",
                access.table
            )));
        }
        if let Some(row) = &mut projected {
            self.state.hydrate_projected_row_with_context(
                &access.table,
                row,
                &mut self.hydration,
                Some(&self.task),
            )?;
        }
        self.record_point(&report)?;
        let row = projected
            .map(|row| complete_sparse_projected_row(&access.table, &fields, row))
            .transpose()?;
        self.workspace.insert(RelationalSparseRecoveryRow {
            table: access.table.clone(),
            primary_key: access.primary_key.clone(),
            row,
        })
    }

    fn hydrate_table(&mut self, table: &str) -> Result<(), RelationalError> {
        self.range_reads = self.range_reads.checked_add(1).ok_or_else(|| {
            RelationalError::Admission(
                "sparse relational live range-read counter overflow".to_string(),
            )
        })?;
        let fields = self.fields(table)?;
        let limits = self.remaining_limits()?;
        let state = self.state;
        let task = &self.task;
        let workspace = &mut self.workspace;
        let mut callback_error = None;
        let read_result = self.reader.visit_projected_range_resolving(
            RelationalRowPageProjectedRange {
                table,
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
                requested_fields: &fields,
            },
            limits,
            &mut self.hydration,
            task,
            |row, budget, task| {
                state
                    .hydrate_projected_row_with_context(table, row, budget, Some(task))
                    .map_err(map_sparse_live_state_to_demand_error)
            },
            |row, _| {
                let primary_key = row.primary_key.clone();
                match complete_sparse_projected_row(table, &fields, row).and_then(|row| {
                    workspace.insert(RelationalSparseRecoveryRow {
                        table: table.to_string(),
                        primary_key,
                        row: Some(row),
                    })
                }) {
                    Ok(_) => true,
                    Err(error) => {
                        callback_error = Some(error);
                        false
                    }
                }
            },
        );
        let report = read_result.map_err(map_sparse_live_snapshot_error)?;
        self.record_range(&report)?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        if report.demand.stopped_early {
            return Err(RelationalError::Corruption(format!(
                "sparse relational live hydration stopped while scanning table {table}"
            )));
        }
        Ok(())
    }

    fn hydrate_monotonic_append(
        &mut self,
        append: &RelationalMonotonicAppendHydration,
    ) -> Result<(), RelationalError> {
        self.monotonic_append_attempts =
            self.monotonic_append_attempts
                .checked_add(1)
                .ok_or_else(|| {
                    RelationalError::Admission(
                        "sparse relational monotonic-append counter overflow".to_string(),
                    )
                })?;
        let Some(first_key) = append.primary_keys.first() else {
            return Err(RelationalError::Corruption(
                "monotonic append hydration contains no primary keys".to_string(),
            ));
        };
        let proven_absent = self
            .reader
            .prove_partition_absent_at_or_after(
                &append.table,
                &append.partition_prefix,
                first_key,
                &self.task,
            )
            .map_err(map_sparse_live_snapshot_error)?;
        if !proven_absent {
            self.monotonic_append_fallbacks = self
                .monotonic_append_fallbacks
                .checked_add(1)
                .ok_or_else(|| {
                    RelationalError::Admission(
                        "sparse relational monotonic-append fallback counter overflow".to_string(),
                    )
                })?;
            self.monotonic_append_metrics.record_fallback();
            return self.hydrate_append_points(append);
        }

        self.monotonic_append_hits =
            self.monotonic_append_hits.checked_add(1).ok_or_else(|| {
                RelationalError::Admission(
                    "sparse relational monotonic-append hit counter overflow".to_string(),
                )
            })?;
        self.monotonic_append_metrics
            .record_hit(append.primary_keys.len());
        for primary_key in &append.primary_keys {
            self.proven_absent_primary_keys
                .insert(RelationalReplayAccess {
                    table: append.table.clone(),
                    primary_key: primary_key.clone(),
                });
            self.workspace.insert(RelationalSparseRecoveryRow {
                table: append.table.clone(),
                primary_key: primary_key.clone(),
                row: None,
            })?;
        }
        Ok(())
    }

    fn hydrate_append_points(
        &mut self,
        append: &RelationalMonotonicAppendHydration,
    ) -> Result<(), RelationalError> {
        for primary_key in &append.primary_keys {
            self.hydrate_point(&RelationalReplayAccess {
                table: append.table.clone(),
                primary_key: primary_key.clone(),
            })?;
        }
        Ok(())
    }

    fn report(&self) -> RelationalSparseLiveHydrationReport {
        RelationalSparseLiveHydrationReport {
            point_reads: self.point_reads,
            range_reads: self.range_reads,
            monotonic_append_attempts: self.monotonic_append_attempts,
            monotonic_append_hits: self.monotonic_append_hits,
            monotonic_append_fallbacks: self.monotonic_append_fallbacks,
            pages_read: self.pages_read,
            rows_decoded: self.rows_decoded,
            bytes_read: self.bytes_read,
        }
    }

    fn proven_absent_primary_keys(&self) -> BTreeSet<RelationalReplayAccess> {
        self.proven_absent_primary_keys.clone()
    }

    fn resolve_probe(
        &mut self,
        probe: &RelationalSparseIndexProbe,
        index: &dyn RelationalConstraintIndex,
    ) -> Result<(), RelationalError> {
        if self.resolved_probes.contains(probe) {
            return Ok(());
        }
        if probe.index == RELATIONAL_PRIMARY_INDEX_NAME
            && self
                .proven_absent_primary_keys
                .contains(&RelationalReplayAccess {
                    table: probe.table.clone(),
                    primary_key: probe.index_key.clone(),
                })
        {
            self.resolved_probes.insert(probe.clone());
            return Ok(());
        }
        let mut missing = Vec::new();
        let mut stopped_for_budget = false;
        let remaining_entries = self.workspace.remaining_entries();
        index.visit_exact_primary_keys(
            &probe.table,
            &probe.index,
            &probe.index_key,
            &mut |primary_key| {
                let access = RelationalReplayAccess {
                    table: probe.table.clone(),
                    primary_key: primary_key.clone(),
                };
                if self.workspace.contains(&access) {
                    return true;
                }
                if missing.len() >= remaining_entries {
                    stopped_for_budget = true;
                    return false;
                }
                missing.push(access);
                true
            },
        )?;
        if stopped_for_budget {
            return Err(RelationalError::Admission(format!(
                "sparse relational live index probe {}.{} exceeds the remaining {}-entry workspace budget",
                probe.table, probe.index, remaining_entries
            )));
        }
        for access in missing {
            self.hydrate_point(&access)?;
        }
        self.resolved_probes.insert(probe.clone());
        Ok(())
    }

    fn remaining_limits(&self) -> Result<RelationalRowPageSnapshotReadLimits, RelationalError> {
        Ok(RelationalRowPageSnapshotReadLimits {
            demand: crate::RelationalRowPageDemandReadLimits {
                max_pages: sparse_remaining(self.limits.demand.max_pages, self.pages_read, "page")?,
                max_rows: sparse_remaining(self.limits.demand.max_rows, self.rows_decoded, "row")?,
                max_bytes: sparse_remaining(
                    self.limits.demand.max_bytes,
                    self.bytes_read,
                    "read-byte",
                )?,
                max_pins: self.limits.demand.max_pins,
                max_tree_height: self.limits.demand.max_tree_height,
            },
            max_overlay_entries: sparse_remaining(
                self.limits.max_overlay_entries,
                self.overlay_entries,
                "overlay-entry",
            )?,
            max_overlay_bytes: sparse_remaining(
                self.limits.max_overlay_bytes,
                self.overlay_resident_bytes,
                "overlay-byte",
            )?,
        })
    }

    fn record_point(
        &mut self,
        report: &RelationalRowPageSnapshotPointReport,
    ) -> Result<(), RelationalError> {
        let overlay_entries = usize::from(matches!(
            report.source,
            RelationalRowPageSnapshotRowSource::Recovery
                | RelationalRowPageSnapshotRowSource::Live
                | RelationalRowPageSnapshotRowSource::Deleted
        ));
        self.record_snapshot_read(
            report.identity,
            &report.demand,
            report.recovery.bytes_read,
            overlay_entries,
            report.overlay_resident_bytes,
        )
    }

    fn record_range(
        &mut self,
        report: &RelationalRowPageSnapshotRangeReport,
    ) -> Result<(), RelationalError> {
        self.record_snapshot_read(
            report.identity,
            &report.demand,
            report.recovery.bytes_read,
            report.overlay_entries,
            report.overlay_resident_bytes,
        )
    }

    fn record_snapshot_read(
        &mut self,
        identity: RelationalRowPageReadViewIdentity,
        demand: &crate::RelationalRowPageDemandReadReport,
        recovery_bytes: u64,
        overlay_entries: usize,
        overlay_resident_bytes: usize,
    ) -> Result<(), RelationalError> {
        if identity != self.reader.identity() {
            return Err(RelationalError::Corruption(
                "sparse relational live row view identity changed during hydration".to_string(),
            ));
        }
        self.pages_read = sparse_add_with_limit(
            self.pages_read,
            demand.pages_read,
            self.limits.demand.max_pages.get(),
            "page",
        )?;
        self.rows_decoded = sparse_add_with_limit(
            self.rows_decoded,
            demand.rows_decoded,
            self.limits.demand.max_rows.get(),
            "row",
        )?;
        let recovery_bytes = usize::try_from(recovery_bytes).map_err(|_| {
            RelationalError::Admission(
                "sparse relational live recovery bytes exceed platform capacity".to_string(),
            )
        })?;
        let read_bytes = demand
            .bytes_read
            .checked_add(recovery_bytes)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "sparse relational live read-byte counter overflow".to_string(),
                )
            })?;
        self.bytes_read = sparse_add_with_limit(
            self.bytes_read,
            read_bytes,
            self.limits.demand.max_bytes.get(),
            "read-byte",
        )?;
        self.overlay_entries = sparse_add_with_limit(
            self.overlay_entries,
            overlay_entries,
            self.limits.max_overlay_entries.get(),
            "overlay-entry",
        )?;
        self.overlay_resident_bytes = sparse_add_with_limit(
            self.overlay_resident_bytes,
            overlay_resident_bytes,
            self.limits.max_overlay_bytes.get(),
            "overlay-byte",
        )?;
        Ok(())
    }
}

pub fn nonzero_min(left: NonZeroUsize, right: NonZeroUsize) -> NonZeroUsize {
    NonZeroUsize::new(left.get().min(right.get())).expect("minimum of non-zero limits is non-zero")
}

fn sparse_remaining(
    limit: NonZeroUsize,
    used: usize,
    name: &'static str,
) -> Result<NonZeroUsize, RelationalError> {
    limit
        .get()
        .checked_sub(used)
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| {
            RelationalError::Admission(format!(
                "sparse relational live hydration exhausted its {} {name} budget",
                limit.get()
            ))
        })
}

fn sparse_add_with_limit(
    current: usize,
    additional: usize,
    limit: usize,
    name: &'static str,
) -> Result<usize, RelationalError> {
    let next = current.checked_add(additional).ok_or_else(|| {
        RelationalError::Admission(format!(
            "sparse relational live hydration {name} counter overflow"
        ))
    })?;
    if next > limit {
        return Err(RelationalError::Admission(format!(
            "sparse relational live hydration requires {next} {name}s, exceeding limit {limit}"
        )));
    }
    Ok(next)
}

fn complete_sparse_projected_row(
    table: &str,
    fields: &[usize],
    projected: RelationalProjectedRow,
) -> Result<RelationalRow, RelationalError> {
    if projected.fields.len() != fields.len()
        || projected
            .fields
            .iter()
            .zip(fields)
            .any(|(field, expected)| field.ordinal != *expected)
    {
        return Err(RelationalError::Corruption(format!(
            "sparse relational live read returned an incomplete row for table {table}"
        )));
    }
    Ok(RelationalRow::new(
        projected
            .fields
            .into_iter()
            .map(|field| field.value)
            .collect(),
    ))
}

fn map_sparse_live_state_to_demand_error(
    error: RelationalError,
) -> RelationalRowPageDemandReadError {
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

fn map_sparse_live_snapshot_error(error: RelationalRowPageSnapshotReadError) -> RelationalError {
    match error {
        RelationalRowPageSnapshotReadError::Admission(message) => {
            RelationalError::Admission(message)
        }
        RelationalRowPageSnapshotReadError::Stopped(reason) => {
            RelationalError::Admission(reason.to_string())
        }
        RelationalRowPageSnapshotReadError::MissingTable(table) => RelationalError::Corruption(
            format!("sparse relational live snapshot is missing table {table}"),
        ),
        RelationalRowPageSnapshotReadError::Corrupt(message) => {
            RelationalError::Corruption(message)
        }
        RelationalRowPageSnapshotReadError::Durability(message) => {
            RelationalError::Durability(message)
        }
    }
}

/// Explicit facade-selected policy for one bounded hydration operation.
#[derive(Debug, Clone, Copy)]
pub struct RelationalSparseLiveHydrationOptions {
    pub mutation_limits: crate::RelationalMutationLimits,
    pub overflow_config: crate::RelationalOverflowConfig,
    pub index_capture_limits: RelationalIndexChangeCaptureLimits,
    pub row_capture_limits: RelationalRowChangeCaptureLimits,
    pub monotonic_append_fast_path_enabled: bool,
}

/// Completes the row and constraint working set against one pinned snapshot.
/// This does not select a database generation or publish a WAL mutation.
pub fn hydrate_sparse_relational_workspace(
    state: &RelationalState,
    reader: RelationalRowPageSnapshotReader,
    transaction: &RelationalTransaction,
    constraint_index: &dyn RelationalConstraintIndex,
    options: RelationalSparseLiveHydrationOptions,
    metrics: Arc<RelationalMonotonicAppendMetrics>,
) -> Result<
    (
        Vec<RelationalSparseRecoveryRow>,
        RelationalSparseLiveHydrationReport,
        BTreeSet<RelationalReplayAccess>,
    ),
    RelationalError,
> {
    if !state.canonical_row_metadata_only() {
        return Err(RelationalError::Admission(
            "sparse relational live hydration requires canonical metadata-only state".to_string(),
        ));
    }
    let plan = state.plan_sparse_transaction_hydration(transaction)?;
    let mut hydrator =
        RelationalSparseLiveHydrator::new(state, reader, options.row_capture_limits, metrics);
    for access in plan.point_access() {
        hydrator.hydrate_point(access)?;
    }
    for append in plan.monotonic_appends() {
        if options.monotonic_append_fast_path_enabled {
            hydrator.hydrate_monotonic_append(append)?;
        } else {
            hydrator.hydrate_append_points(append)?;
        }
    }
    for table in plan.scan_tables() {
        hydrator.hydrate_table(table)?;
    }
    for probe in plan.index_probes() {
        hydrator.resolve_probe(probe, constraint_index)?;
    }

    loop {
        let previous_entries = hydrator.workspace.len();
        let preparation = state.prepare_sparse_transaction_for_authoritative_live(
            RelationalSparseLivePreparationStage {
                transaction: transaction.clone(),
                hydrated_workspace: hydrator.workspace_snapshot(),
                mutation_limits: options.mutation_limits,
                overflow_config: options.overflow_config,
                index_capture_limits: options.index_capture_limits,
                row_capture_limits: options.row_capture_limits,
            },
        )?;
        for access in preparation.replay_access().entries() {
            hydrator.hydrate_point(access)?;
        }
        for probe in preparation.constraint_probes() {
            hydrator.resolve_probe(probe, constraint_index)?;
        }
        if hydrator.workspace.len() == previous_entries {
            let report = hydrator.report();
            let proven_absent_primary_keys = hydrator.proven_absent_primary_keys();
            return Ok((
                hydrator.workspace_snapshot(),
                report,
                proven_absent_primary_keys,
            ));
        }
    }
}
