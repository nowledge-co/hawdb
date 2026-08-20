//! Snapshot-correct relational reads over checkpoint, recovery, and live rows.

use super::demand::{
    RelationalRowPageOverlayCursor, RelationalRowPageOverlayPoint, RelationalRowPageOverlayRange,
    RelationalRowPageOverlayRead, RelationalRowPageProjectedOverlayValue,
};
use super::live::{RelationalRowPageOverlayRangeSources, RelationalRowPageOverlayRangeValue};
use super::{
    RelationalProjectedField, RelationalProjectedRow, RelationalProjectedRowView,
    RelationalRowDeltaError, RelationalRowDeltaReadReport, RelationalRowPageDemandReadError,
    RelationalRowPageDemandReadLimits, RelationalRowPageDemandReadReport,
    RelationalRowPageDemandReader, RelationalRowPageProjectedFields,
    RelationalRowPageProjectedRange, RelationalRowPageProjectedRangeFields,
    RelationalRowPageReadView, RelationalRowPageReadViewIdentity, RelationalRowPageRecoveredValue,
};
use crate::relational::{
    ordered_key::encode_ordered_relational_key, RelationalHydrationBudget, RelationalKey,
    RelationalOverflowRootReader, RelationalRowPageError, RelationalRowPagePublicationError,
    RelationalValue,
};
use crate::{SegmentCache, StoreId};
use skein_core::{RuntimeCancellationReason, RuntimeTaskContext};
use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::fmt;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub const DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_ENTRIES: usize = 16 * 1024;
pub const DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageSnapshotReadLimits {
    pub demand: RelationalRowPageDemandReadLimits,
    pub max_overlay_entries: NonZeroUsize,
    pub max_overlay_bytes: NonZeroUsize,
}

impl Default for RelationalRowPageSnapshotReadLimits {
    fn default() -> Self {
        Self {
            demand: RelationalRowPageDemandReadLimits::default(),
            max_overlay_entries: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_ENTRIES)
                .expect("default snapshot overlay entry limit is non-zero"),
            max_overlay_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_BYTES)
                .expect("default snapshot overlay byte limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalRowPageSnapshotRowSource {
    Checkpoint,
    Recovery,
    Live,
    Deleted,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageSnapshotPointReport {
    pub identity: RelationalRowPageReadViewIdentity,
    pub source: RelationalRowPageSnapshotRowSource,
    pub demand: RelationalRowPageDemandReadReport,
    pub recovery: RelationalRowDeltaReadReport,
    pub live_batches_examined: usize,
    pub overlay_resident_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageSnapshotRangeReport {
    pub identity: RelationalRowPageReadViewIdentity,
    pub demand: RelationalRowPageDemandReadReport,
    pub recovery: RelationalRowDeltaReadReport,
    pub live_entries_visited: usize,
    pub overlay_entries: usize,
    pub overlay_resident_bytes: usize,
    pub overlay_replacements: usize,
    pub overlay_merge_sources: usize,
    pub overlay_peak_buffered_entries: usize,
}

struct ProjectedRangeVisitContext<'a> {
    range: RelationalRowPageProjectedRange<'a>,
    limits: RelationalRowPageSnapshotReadLimits,
    hydration: &'a mut RelationalHydrationBudget,
    task: &'a RuntimeTaskContext,
    hydration_fields: Option<&'a [usize]>,
}

struct ProjectedPointReadRequest<'a> {
    table: &'a str,
    primary_key: &'a RelationalKey,
    requested_fields: &'a [usize],
    limits: RelationalRowPageSnapshotReadLimits,
    hydration: &'a mut RelationalHydrationBudget,
    task: &'a RuntimeTaskContext,
    hydration_fields: Option<&'a [usize]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalRowPageSnapshotReadError {
    Admission(String),
    Corrupt(String),
    Durability(String),
    MissingTable(String),
    Stopped(RuntimeCancellationReason),
}

impl fmt::Display for RelationalRowPageSnapshotReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(
                    formatter,
                    "relational snapshot-read admission failed: {message}"
                )
            }
            Self::Corrupt(message) => write!(formatter, "corrupt relational snapshot: {message}"),
            Self::Durability(message) => {
                write!(
                    formatter,
                    "relational snapshot-read durability failed: {message}"
                )
            }
            Self::MissingTable(table) => {
                write!(formatter, "relational snapshot has no table {table}")
            }
            Self::Stopped(reason) => {
                write!(formatter, "relational snapshot read stopped: {reason}")
            }
        }
    }
}

impl std::error::Error for RelationalRowPageSnapshotReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Stopped(reason) => Some(reason),
            _ => None,
        }
    }
}

pub struct RelationalRowPageSnapshotReader {
    view: Arc<RelationalRowPageReadView>,
    demand: RelationalRowPageDemandReader,
    overlay_overflow: Option<Arc<RelationalOverflowRootReader>>,
    poisoned: AtomicBool,
}

impl fmt::Debug for RelationalRowPageSnapshotReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelationalRowPageSnapshotReader")
            .field("identity", &self.view.identity())
            .field("has_recovery", &self.view.recovery_delta().is_some())
            .field("live_batches", &self.view.live_batch_count())
            .field("poisoned", &self.is_poisoned())
            .finish()
    }
}

impl RelationalRowPageSnapshotReader {
    pub fn new(
        view: Arc<RelationalRowPageReadView>,
        base_overflow: Arc<RelationalOverflowRootReader>,
        overlay_overflow: Option<Arc<RelationalOverflowRootReader>>,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
    ) -> Result<Self, RelationalRowPageSnapshotReadError> {
        view.validate_serving_fence().map_err(map_delta_error)?;
        match view.recovery_delta() {
            Some(delta) => delta
                .validate_overflow_root(overlay_overflow.as_deref())
                .map_err(map_delta_error)?,
            None if overlay_overflow.is_some() => {
                return Err(RelationalRowPageSnapshotReadError::Admission(
                    "an overlay overflow root requires a recovery delta".to_string(),
                ));
            }
            None => {}
        }
        let demand =
            RelationalRowPageDemandReader::new(view.pinned_base(), base_overflow, cache, store_id)
                .map_err(map_demand_error)?;
        Ok(Self {
            view,
            demand,
            overlay_overflow,
            poisoned: AtomicBool::new(false),
        })
    }

    pub fn identity(&self) -> RelationalRowPageReadViewIdentity {
        self.view.identity()
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
            || self.demand.is_poisoned()
            || self
                .view
                .recovery_delta()
                .is_some_and(|delta| delta.is_poisoned())
    }

    /// Proves that the pinned snapshot has no primary key in `partition_prefix`
    /// at or after `lower` without reading a row page.
    ///
    /// Recovery deltas and ambiguous page boundaries return `false`, which is
    /// a conservative "not proven" result rather than an absence claim.
    pub fn prove_partition_absent_at_or_after(
        &self,
        table: &str,
        partition_prefix: &RelationalKey,
        lower: &RelationalKey,
        task: &RuntimeTaskContext,
    ) -> Result<bool, RelationalRowPageSnapshotReadError> {
        self.checkpoint(task)?;
        if lower.0.len() != partition_prefix.0.len().saturating_add(1)
            || !lower.0.starts_with(partition_prefix.0.as_slice())
        {
            return Err(RelationalRowPageSnapshotReadError::Admission(
                "partition absence proof requires a primary-key prefix and one ordered suffix"
                    .to_string(),
            ));
        }
        if self.view.recovery_delta().is_some()
            || self
                .view
                .live_may_contain_prefix_at_or_after(table, partition_prefix, lower)
        {
            return Ok(false);
        }
        let encoded_lower = encode_ordered_relational_key(lower).map_err(|error| {
            RelationalRowPageSnapshotReadError::Admission(format!(
                "partition absence lower key cannot be encoded: {error}"
            ))
        })?;
        let descriptor = self
            .view
            .base()
            .find_table_page_descriptor(table, lower)
            .map_err(|error| self.map_row_publication_error(error))?;
        let Some(descriptor) = descriptor else {
            return Ok(true);
        };
        if descriptor.upper_bound.as_slice() < encoded_lower.as_slice() {
            return Ok(true);
        }
        if partition_prefix.0.is_empty() {
            return Ok(false);
        }
        let encoded_prefix = encode_ordered_relational_key(partition_prefix).map_err(|error| {
            RelationalRowPageSnapshotReadError::Admission(format!(
                "partition prefix cannot be encoded: {error}"
            ))
        })?;
        let Some(prefix_upper) = lexicographic_prefix_upper_bound(&encoded_prefix) else {
            return Ok(false);
        };
        Ok(descriptor.lower_bound.as_slice() >= prefix_upper.as_slice())
    }

    pub fn has_overlay_overflow_root(&self) -> bool {
        self.overlay_overflow.is_some()
    }

    pub fn point_projected(
        &self,
        table: &str,
        primary_key: &RelationalKey,
        requested_fields: &[usize],
        limits: RelationalRowPageSnapshotReadLimits,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
    ) -> Result<
        (
            Option<RelationalProjectedRow>,
            RelationalRowPageSnapshotPointReport,
        ),
        RelationalRowPageSnapshotReadError,
    > {
        self.point_projected_with_hydration_mode(ProjectedPointReadRequest {
            table,
            primary_key,
            requested_fields,
            limits,
            hydration,
            task,
            hydration_fields: Some(requested_fields),
        })
    }

    /// Reads one projected snapshot row while hydrating only the listed field
    /// ordinals. Retained overflow references are validated against the root
    /// bound to their snapshot source.
    pub fn point_projected_fields(
        &self,
        table: &str,
        primary_key: &RelationalKey,
        fields: RelationalRowPageProjectedFields<'_>,
        limits: RelationalRowPageSnapshotReadLimits,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
    ) -> Result<
        (
            Option<RelationalProjectedRow>,
            RelationalRowPageSnapshotPointReport,
        ),
        RelationalRowPageSnapshotReadError,
    > {
        let RelationalRowPageProjectedFields {
            requested_fields,
            hydration_fields,
        } = fields;
        self.point_projected_with_hydration_mode(ProjectedPointReadRequest {
            table,
            primary_key,
            requested_fields,
            limits,
            hydration,
            task,
            hydration_fields: Some(hydration_fields),
        })
    }

    /// Reads one projected snapshot row without loading overflow payloads.
    pub fn point_projected_unhydrated(
        &self,
        table: &str,
        primary_key: &RelationalKey,
        requested_fields: &[usize],
        limits: RelationalRowPageSnapshotReadLimits,
        task: &RuntimeTaskContext,
    ) -> Result<
        (
            Option<RelationalProjectedRow>,
            RelationalRowPageSnapshotPointReport,
        ),
        RelationalRowPageSnapshotReadError,
    > {
        let mut hydration = RelationalHydrationBudget::default();
        self.point_projected_with_hydration_mode(ProjectedPointReadRequest {
            table,
            primary_key,
            requested_fields,
            limits,
            hydration: &mut hydration,
            task,
            hydration_fields: None,
        })
    }

    fn point_projected_with_hydration_mode(
        &self,
        request: ProjectedPointReadRequest<'_>,
    ) -> Result<
        (
            Option<RelationalProjectedRow>,
            RelationalRowPageSnapshotPointReport,
        ),
        RelationalRowPageSnapshotReadError,
    > {
        let ProjectedPointReadRequest {
            table,
            primary_key,
            requested_fields,
            limits,
            hydration,
            task,
            hydration_fields,
        } = request;
        self.checkpoint(task)?;
        let (overlay, recovery, live_batches_examined, selected_live) = self
            .view
            .overlay_value_accounted(table, primary_key)
            .map_err(|error| self.map_delta_error(error))?;
        self.checkpoint(task)?;
        let (row, demand, source, overlay_resident_bytes) = match overlay {
            Some(value) => {
                let deleted = matches!(value, RelationalRowPageRecoveredValue::Deleted);
                let table_root = self
                    .view
                    .base()
                    .table_root(table)
                    .map_err(|error| self.map_row_publication_error(error))?;
                let column_count = table_root.column_count.get() as usize;
                super::validate_requested_fields(
                    requested_fields,
                    column_count,
                    self.view.base().publication_config().page_limits,
                )
                .map_err(|error| self.map_row_error(error))?;
                validate_overlay_row(&value, column_count)?;
                let value_resident_bytes =
                    projected_overlay_resident_bytes(&value, requested_fields)?;
                let overlay_resident_bytes =
                    overlay_point_resident_bytes(primary_key, value_resident_bytes)?;
                if overlay_resident_bytes > limits.max_overlay_bytes.get() {
                    return Err(RelationalRowPageSnapshotReadError::Admission(format!(
                        "overlay point requires {overlay_resident_bytes} bytes, exceeding limit {}",
                        limits.max_overlay_bytes
                    )));
                }
                let value = project_overlay_value(
                    &value,
                    requested_fields,
                    !selected_live && self.overlay_overflow.is_some(),
                );
                let (row, demand) = self
                    .demand
                    .point_projected_overlay(
                        RelationalRowPageOverlayPoint {
                            table,
                            primary_key: primary_key.clone(),
                            value,
                            overflow_root: self.overlay_overflow.as_deref(),
                        },
                        limits.demand,
                        hydration,
                        task,
                        hydration_fields,
                    )
                    .map_err(|error| self.map_demand_error(error))?;
                let source = if deleted {
                    RelationalRowPageSnapshotRowSource::Deleted
                } else if selected_live {
                    RelationalRowPageSnapshotRowSource::Live
                } else {
                    RelationalRowPageSnapshotRowSource::Recovery
                };
                (row, demand, source, overlay_resident_bytes)
            }
            None => {
                let (row, demand) = if let Some(hydration_fields) = hydration_fields {
                    self.demand.point_projected_fields(
                        table,
                        primary_key,
                        RelationalRowPageProjectedFields {
                            requested_fields,
                            hydration_fields,
                        },
                        limits.demand,
                        hydration,
                        task,
                    )
                } else {
                    self.demand.point_projected_unhydrated(
                        table,
                        primary_key,
                        requested_fields,
                        limits.demand,
                        task,
                    )
                }
                .map_err(|error| self.map_demand_error(error))?;
                let source = if row.is_some() {
                    RelationalRowPageSnapshotRowSource::Checkpoint
                } else {
                    RelationalRowPageSnapshotRowSource::Missing
                };
                (row, demand, source, 0)
            }
        };
        Ok((
            row,
            RelationalRowPageSnapshotPointReport {
                identity: self.identity(),
                source,
                demand,
                recovery,
                live_batches_examined,
                overlay_resident_bytes,
            },
        ))
    }

    pub fn visit_projected_range(
        &self,
        range: RelationalRowPageProjectedRange<'_>,
        limits: RelationalRowPageSnapshotReadLimits,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
        mut visit: impl FnMut(RelationalProjectedRow) -> bool,
    ) -> Result<RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError> {
        self.visit_projected_range_resolving(
            range,
            limits,
            hydration,
            task,
            |_, _, _| Ok(()),
            |row, _| visit(row),
        )
    }

    /// Visits a projected range and resolves any unbound overlay values before
    /// they become visible to the row callback.
    pub fn visit_projected_range_resolving(
        &self,
        range: RelationalRowPageProjectedRange<'_>,
        limits: RelationalRowPageSnapshotReadLimits,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
        resolve: impl FnMut(
            &mut RelationalProjectedRow,
            &mut RelationalHydrationBudget,
            &RuntimeTaskContext,
        ) -> Result<(), RelationalRowPageDemandReadError>,
        visit: impl FnMut(RelationalProjectedRow, &mut RelationalHydrationBudget) -> bool,
    ) -> Result<RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError> {
        self.visit_projected_range_fields_resolving(
            RelationalRowPageProjectedRangeFields {
                range,
                hydration_fields: range.requested_fields,
            },
            limits,
            hydration,
            task,
            resolve,
            visit,
        )
    }

    /// Visits a projected range while hydrating only the listed field
    /// ordinals. The resolver is invoked only for live values that are not
    /// bound to an immutable overflow root.
    pub fn visit_projected_range_fields_resolving(
        &self,
        projected: RelationalRowPageProjectedRangeFields<'_>,
        limits: RelationalRowPageSnapshotReadLimits,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
        mut resolve: impl FnMut(
            &mut RelationalProjectedRow,
            &mut RelationalHydrationBudget,
            &RuntimeTaskContext,
        ) -> Result<(), RelationalRowPageDemandReadError>,
        visit: impl FnMut(RelationalProjectedRow, &mut RelationalHydrationBudget) -> bool,
    ) -> Result<RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError> {
        let RelationalRowPageProjectedRangeFields {
            range,
            hydration_fields,
        } = projected;
        self.visit_projected_range_with_hydration_mode(
            ProjectedRangeVisitContext {
                range,
                limits,
                hydration,
                task,
                hydration_fields: Some(hydration_fields),
            },
            &mut resolve,
            visit,
        )
    }

    pub fn visit_projected_range_fields_resolving_ref(
        &self,
        projected: RelationalRowPageProjectedRangeFields<'_>,
        limits: RelationalRowPageSnapshotReadLimits,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
        mut resolve: impl FnMut(
            &mut RelationalProjectedRow,
            &mut RelationalHydrationBudget,
            &RuntimeTaskContext,
        ) -> Result<(), RelationalRowPageDemandReadError>,
        visit: impl for<'row> FnMut(
            RelationalProjectedRowView<'row>,
            &mut RelationalHydrationBudget,
        ) -> bool,
    ) -> Result<RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError> {
        let RelationalRowPageProjectedRangeFields {
            range,
            hydration_fields,
        } = projected;
        self.visit_projected_range_with_hydration_mode_ref(
            ProjectedRangeVisitContext {
                range,
                limits,
                hydration,
                task,
                hydration_fields: Some(hydration_fields),
            },
            &mut resolve,
            visit,
        )
    }

    /// Visits the exact snapshot row shape without reading overflow payloads.
    /// Overflow fields remain typed content-addressed references in both base
    /// and overlay rows. This is reserved for storage maintenance that needs
    /// physical reachability rather than user-visible values.
    pub fn visit_projected_range_unhydrated(
        &self,
        range: RelationalRowPageProjectedRange<'_>,
        limits: RelationalRowPageSnapshotReadLimits,
        task: &RuntimeTaskContext,
        mut visit: impl FnMut(RelationalProjectedRow) -> bool,
    ) -> Result<RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError> {
        let mut hydration = RelationalHydrationBudget::default();
        let mut resolve = |_: &mut RelationalProjectedRow,
                           _: &mut RelationalHydrationBudget,
                           _: &RuntimeTaskContext| { Ok(()) };
        self.visit_projected_range_with_hydration_mode(
            ProjectedRangeVisitContext {
                range,
                limits,
                hydration: &mut hydration,
                task,
                hydration_fields: None,
            },
            &mut resolve,
            |row, _| visit(row),
        )
    }

    fn visit_projected_range_with_hydration_mode(
        &self,
        context: ProjectedRangeVisitContext<'_>,
        resolve: &mut impl FnMut(
            &mut RelationalProjectedRow,
            &mut RelationalHydrationBudget,
            &RuntimeTaskContext,
        ) -> Result<(), RelationalRowPageDemandReadError>,
        mut visit: impl FnMut(RelationalProjectedRow, &mut RelationalHydrationBudget) -> bool,
    ) -> Result<RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError> {
        let mut report =
            self.visit_projected_range_with_hydration_mode_ref(context, resolve, |row, budget| {
                visit(row.to_owned_row(), budget)
            })?;
        report.demand.borrowed_rows_emitted = 0;
        report.demand.owned_rows_emitted = report.demand.rows_emitted;
        Ok(report)
    }

    fn visit_projected_range_with_hydration_mode_ref(
        &self,
        context: ProjectedRangeVisitContext<'_>,
        resolve: &mut impl FnMut(
            &mut RelationalProjectedRow,
            &mut RelationalHydrationBudget,
            &RuntimeTaskContext,
        ) -> Result<(), RelationalRowPageDemandReadError>,
        visit: impl for<'row> FnMut(
            RelationalProjectedRowView<'row>,
            &mut RelationalHydrationBudget,
        ) -> bool,
    ) -> Result<RelationalRowPageSnapshotRangeReport, RelationalRowPageSnapshotReadError> {
        let ProjectedRangeVisitContext {
            range,
            limits,
            hydration,
            task,
            hydration_fields,
        } = context;
        self.checkpoint(task)?;
        let mut overlay = StreamingOverlayCursor::new(self, range, limits, task)?;
        self.checkpoint(task)?;
        let demand = self
            .demand
            .visit_projected_range_with_overlay_ref(
                RelationalRowPageOverlayRead {
                    range,
                    limits: limits.demand,
                    overlay: RelationalRowPageOverlayRange {
                        cursor: &mut overlay,
                        overflow_root: self.overlay_overflow.as_deref(),
                    },
                },
                hydration,
                task,
                hydration_fields,
                resolve,
                visit,
            )
            .map_err(|error| self.map_demand_error(error))?;
        let overlay_report = overlay
            .report()
            .map_err(|error| self.map_delta_error(error))?;
        Ok(RelationalRowPageSnapshotRangeReport {
            identity: self.identity(),
            demand,
            recovery: overlay_report.recovery,
            live_entries_visited: overlay_report.live_entries_visited,
            overlay_entries: overlay_report.overlay_entries,
            overlay_resident_bytes: overlay_report.overlay_resident_bytes,
            overlay_replacements: overlay_report.overlay_replacements,
            overlay_merge_sources: overlay_report.overlay_merge_sources,
            overlay_peak_buffered_entries: overlay_report.overlay_peak_buffered_entries,
        })
    }

    fn checkpoint(
        &self,
        task: &RuntimeTaskContext,
    ) -> Result<(), RelationalRowPageSnapshotReadError> {
        if self.is_poisoned() {
            return Err(RelationalRowPageSnapshotReadError::Corrupt(
                "relational row snapshot reader is poisoned".to_string(),
            ));
        }
        task.checkpoint()
            .map_err(RelationalRowPageSnapshotReadError::Stopped)
    }

    fn map_delta_error(
        &self,
        error: RelationalRowDeltaError,
    ) -> RelationalRowPageSnapshotReadError {
        let mapped = map_delta_error(error);
        self.poison_if_needed(&mapped);
        mapped
    }

    fn map_demand_error(
        &self,
        error: RelationalRowPageDemandReadError,
    ) -> RelationalRowPageSnapshotReadError {
        let mapped = map_demand_error(error);
        self.poison_if_needed(&mapped);
        mapped
    }

    fn map_row_error(&self, error: RelationalRowPageError) -> RelationalRowPageSnapshotReadError {
        let mapped = map_row_error(error);
        self.poison_if_needed(&mapped);
        mapped
    }

    fn map_row_publication_error(
        &self,
        error: RelationalRowPagePublicationError,
    ) -> RelationalRowPageSnapshotReadError {
        let mapped = map_row_publication_error(error);
        self.poison_if_needed(&mapped);
        mapped
    }

    fn poison_if_needed(&self, error: &RelationalRowPageSnapshotReadError) {
        if matches!(
            error,
            RelationalRowPageSnapshotReadError::Corrupt(_)
                | RelationalRowPageSnapshotReadError::Durability(_)
        ) {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

fn lexicographic_prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    let position = upper.iter().rposition(|byte| *byte != u8::MAX)?;
    upper[position] = upper[position].saturating_add(1);
    upper.truncate(position + 1);
    Some(upper)
}

struct StreamingOverlayHead {
    key: RelationalKey,
    epoch: u64,
    value: RelationalRowPageProjectedOverlayValue,
    source: usize,
    resident_bytes: usize,
}

impl PartialEq for StreamingOverlayHead {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.epoch == other.epoch && self.source == other.source
    }
}

impl Eq for StreamingOverlayHead {}

impl PartialOrd for StreamingOverlayHead {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamingOverlayHead {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.epoch.cmp(&self.epoch))
            .then_with(|| other.source.cmp(&self.source))
    }
}

#[derive(Debug)]
struct OverlayCollectionReport {
    recovery: RelationalRowDeltaReadReport,
    live_entries_visited: usize,
    overlay_entries: usize,
    overlay_resident_bytes: usize,
    overlay_replacements: usize,
    overlay_merge_sources: usize,
    overlay_peak_buffered_entries: usize,
}

struct StreamingOverlayCursor<'a> {
    owner: &'a RelationalRowPageSnapshotReader,
    sources: RelationalRowPageOverlayRangeSources<'a>,
    heap: BinaryHeap<StreamingOverlayHead>,
    identity: RelationalRowPageReadViewIdentity,
    column_count: usize,
    requested_fields: &'a [usize],
    limits: RelationalRowPageSnapshotReadLimits,
    task: &'a RuntimeTaskContext,
    initialized: bool,
    buffered_bytes: usize,
    emitted_bytes: usize,
    peak_resident_bytes: usize,
    peak_buffered_entries: usize,
    entries: usize,
    replacements: usize,
}

impl<'a> StreamingOverlayCursor<'a> {
    fn new(
        owner: &'a RelationalRowPageSnapshotReader,
        range: RelationalRowPageProjectedRange<'a>,
        limits: RelationalRowPageSnapshotReadLimits,
        task: &'a RuntimeTaskContext,
    ) -> Result<Self, RelationalRowPageSnapshotReadError> {
        let table_root = owner
            .view
            .base()
            .table_root(range.table)
            .map_err(|error| owner.map_row_publication_error(error))?;
        let column_count = table_root.column_count.get() as usize;
        super::validate_requested_fields(
            range.requested_fields,
            column_count,
            owner.view.base().publication_config().page_limits,
        )
        .map_err(|error| owner.map_row_error(error))?;
        let sources = owner
            .view
            .overlay_range_sources(
                range.table,
                range.lower,
                range.upper,
                range.requested_fields,
                owner.overlay_overflow.is_some(),
                limits.max_overlay_entries.get(),
            )
            .map_err(|error| owner.map_delta_error(error))?;
        Ok(Self {
            owner,
            identity: owner.identity(),
            column_count,
            requested_fields: range.requested_fields,
            limits,
            task,
            heap: BinaryHeap::with_capacity(sources.len()),
            sources,
            initialized: false,
            buffered_bytes: 0,
            emitted_bytes: 0,
            peak_resident_bytes: 0,
            peak_buffered_entries: 0,
            entries: 0,
            replacements: 0,
        })
    }

    fn initialize(&mut self) -> Result<(), RelationalRowPageSnapshotReadError> {
        if self.initialized {
            return Ok(());
        }
        self.initialized = true;
        for source in 0..self.sources.len() {
            self.advance_source(source, 0)?;
        }
        Ok(())
    }

    fn advance_source(
        &mut self,
        source: usize,
        working_bytes: usize,
    ) -> Result<(), RelationalRowPageSnapshotReadError> {
        self.task
            .checkpoint()
            .map_err(RelationalRowPageSnapshotReadError::Stopped)?;
        let Some(entry) = self
            .sources
            .next(source)
            .map_err(|error| self.owner.map_delta_error(error))?
        else {
            return Ok(());
        };
        let super::live::RelationalRowPageOverlayRangeEntry { key, value, epoch } = entry;
        if epoch <= self.identity.base_commit_epoch || epoch > self.identity.visible_commit_epoch {
            return Err(RelationalRowPageSnapshotReadError::Corrupt(format!(
                "overlay row epoch {epoch} is outside ({}, {}]",
                self.identity.base_commit_epoch, self.identity.visible_commit_epoch
            )));
        }
        let (value, value_resident_bytes) = match value {
            RelationalRowPageOverlayRangeValue::Projected(value) => {
                let resident_bytes = projected_value_resident_bytes(&value)?;
                (value, resident_bytes)
            }
            RelationalRowPageOverlayRangeValue::Recovered(value) => {
                validate_overlay_row(&value, self.column_count)?;
                let resident_bytes =
                    projected_overlay_resident_bytes(&value, self.requested_fields)?;
                (
                    project_overlay_value(&value, self.requested_fields, false),
                    resident_bytes,
                )
            }
        };
        let entry_bytes = overlay_key_resident_bytes(&key)?
            .checked_add(value_resident_bytes)
            .and_then(|bytes| bytes.checked_add(size_of::<StreamingOverlayHead>()))
            .and_then(|bytes| bytes.checked_add(4 * size_of::<usize>()))
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay entry byte accounting overflow".to_string(),
                )
            })?;
        let next_bytes = self
            .buffered_bytes
            .checked_add(self.emitted_bytes)
            .and_then(|bytes| bytes.checked_add(working_bytes))
            .and_then(|bytes| bytes.checked_add(entry_bytes))
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay resident-byte accounting overflow".to_string(),
                )
            })?;
        if next_bytes > self.limits.max_overlay_bytes.get() {
            return Err(RelationalRowPageSnapshotReadError::Admission(format!(
                "overlay requires {next_bytes} bytes, exceeding limit {}",
                self.limits.max_overlay_bytes
            )));
        }
        self.heap.push(StreamingOverlayHead {
            key,
            epoch,
            value,
            source,
            resident_bytes: entry_bytes,
        });
        self.buffered_bytes = self
            .buffered_bytes
            .checked_add(entry_bytes)
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay resident-byte accounting overflow".to_string(),
                )
            })?;
        self.observe_peak(next_bytes, working_bytes)?;
        Ok(())
    }

    fn observe_peak(
        &mut self,
        resident_bytes: usize,
        working_bytes: usize,
    ) -> Result<(), RelationalRowPageSnapshotReadError> {
        self.peak_resident_bytes = self.peak_resident_bytes.max(resident_bytes);
        let entries = self
            .heap
            .len()
            .checked_add(usize::from(self.emitted_bytes != 0))
            .and_then(|entries| entries.checked_add(usize::from(working_bytes != 0)))
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay buffered-entry accounting overflow".to_string(),
                )
            })?;
        self.peak_buffered_entries = self.peak_buffered_entries.max(entries);
        Ok(())
    }

    fn pop_head(&mut self) -> Result<StreamingOverlayHead, RelationalRowPageSnapshotReadError> {
        let head = self.heap.pop().ok_or_else(|| {
            RelationalRowPageSnapshotReadError::Corrupt(
                "overlay merge heap lost a peeked row".to_string(),
            )
        })?;
        self.buffered_bytes = self
            .buffered_bytes
            .checked_sub(head.resident_bytes)
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Corrupt(
                    "overlay merge resident-byte accounting underflow".to_string(),
                )
            })?;
        Ok(head)
    }

    fn next_projected(
        &mut self,
    ) -> Result<
        Option<(RelationalKey, RelationalRowPageProjectedOverlayValue)>,
        RelationalRowPageSnapshotReadError,
    > {
        self.emitted_bytes = 0;
        self.initialize()?;
        let Some(_) = self.heap.peek() else {
            return Ok(None);
        };
        let mut selected = self.pop_head()?;
        let deferred_source = selected.source;
        while self
            .heap
            .peek()
            .is_some_and(|candidate| candidate.key == selected.key)
        {
            let candidate = self.pop_head()?;
            let candidate_source = candidate.source;
            if candidate.epoch == selected.epoch {
                return Err(RelationalRowPageSnapshotReadError::Corrupt(format!(
                    "overlay contains duplicate row version at epoch {}",
                    candidate.epoch
                )));
            }
            if candidate.epoch > selected.epoch {
                selected = candidate;
            }
            self.advance_source(candidate_source, selected.resident_bytes)?;
            self.replacements = self.replacements.checked_add(1).ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay replacement counter overflow".to_string(),
                )
            })?;
        }
        self.advance_source(deferred_source, selected.resident_bytes)?;
        self.entries = self.entries.checked_add(1).ok_or_else(|| {
            RelationalRowPageSnapshotReadError::Admission(
                "overlay entry counter overflow".to_string(),
            )
        })?;
        if self.entries > self.limits.max_overlay_entries.get() {
            return Err(RelationalRowPageSnapshotReadError::Admission(format!(
                "overlay contains {} entries, exceeding limit {}",
                self.entries, self.limits.max_overlay_entries
            )));
        }
        self.emitted_bytes = selected.resident_bytes;
        let resident_bytes = self
            .buffered_bytes
            .checked_add(self.emitted_bytes)
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay resident-byte accounting overflow".to_string(),
                )
            })?;
        if resident_bytes > self.limits.max_overlay_bytes.get() {
            return Err(RelationalRowPageSnapshotReadError::Admission(format!(
                "overlay requires {resident_bytes} bytes, exceeding limit {}",
                self.limits.max_overlay_bytes
            )));
        }
        self.observe_peak(resident_bytes, 0)?;
        Ok(Some((selected.key, selected.value)))
    }

    fn report(&self) -> Result<OverlayCollectionReport, RelationalRowDeltaError> {
        Ok(OverlayCollectionReport {
            recovery: self.sources.recovery_report()?,
            live_entries_visited: self.sources.live_entries_visited(),
            overlay_entries: self.entries,
            overlay_resident_bytes: self.peak_resident_bytes,
            overlay_replacements: self.replacements,
            overlay_merge_sources: self.sources.len(),
            overlay_peak_buffered_entries: self.peak_buffered_entries,
        })
    }

    fn map_error(
        &self,
        error: RelationalRowPageSnapshotReadError,
    ) -> RelationalRowPageDemandReadError {
        self.owner.poison_if_needed(&error);
        match error {
            RelationalRowPageSnapshotReadError::Admission(message) => {
                RelationalRowPageDemandReadError::Admission(message)
            }
            RelationalRowPageSnapshotReadError::Corrupt(message) => {
                RelationalRowPageDemandReadError::Corrupt(message)
            }
            RelationalRowPageSnapshotReadError::Durability(message) => {
                RelationalRowPageDemandReadError::Durability(message)
            }
            RelationalRowPageSnapshotReadError::MissingTable(table) => {
                RelationalRowPageDemandReadError::MissingTable(table)
            }
            RelationalRowPageSnapshotReadError::Stopped(reason) => {
                RelationalRowPageDemandReadError::Stopped(reason)
            }
        }
    }
}

impl RelationalRowPageOverlayCursor for StreamingOverlayCursor<'_> {
    fn peek_key(&mut self) -> Result<Option<&RelationalKey>, RelationalRowPageDemandReadError> {
        self.emitted_bytes = 0;
        self.initialize().map_err(|error| self.map_error(error))?;
        Ok(self.heap.peek().map(|head| &head.key))
    }

    fn next_row(
        &mut self,
    ) -> Result<
        Option<(RelationalKey, RelationalRowPageProjectedOverlayValue)>,
        RelationalRowPageDemandReadError,
    > {
        self.next_projected().map_err(|error| self.map_error(error))
    }
}

fn validate_overlay_row(
    value: &RelationalRowPageRecoveredValue,
    column_count: usize,
) -> Result<(), RelationalRowPageSnapshotReadError> {
    let RelationalRowPageRecoveredValue::Present(row) = value else {
        return Ok(());
    };
    if row.values().len() != column_count {
        return Err(RelationalRowPageSnapshotReadError::Corrupt(format!(
            "overlay row contains {} columns, expected {column_count}",
            row.values().len()
        )));
    }
    Ok(())
}

fn projected_overlay_resident_bytes(
    value: &RelationalRowPageRecoveredValue,
    requested_fields: &[usize],
) -> Result<usize, RelationalRowPageSnapshotReadError> {
    let RelationalRowPageRecoveredValue::Present(row) = value else {
        return Ok(0);
    };
    requested_fields.iter().try_fold(
        size_of::<RelationalRowPageProjectedOverlayValue>()
            .checked_add(
                requested_fields
                    .len()
                    .checked_mul(size_of::<RelationalProjectedField>())
                    .ok_or_else(|| {
                        RelationalRowPageSnapshotReadError::Admission(
                            "overlay projection allocation accounting overflow".to_string(),
                        )
                    })?,
            )
            .and_then(|bytes| bytes.checked_add(4 * size_of::<usize>()))
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay projection byte accounting overflow".to_string(),
                )
            })?,
        |bytes, ordinal| {
            bytes
                .checked_add(row.values()[*ordinal].estimated_payload_bytes())
                .ok_or_else(|| {
                    RelationalRowPageSnapshotReadError::Admission(
                        "overlay projection payload accounting overflow".to_string(),
                    )
                })
        },
    )
}

fn projected_value_resident_bytes(
    value: &RelationalRowPageProjectedOverlayValue,
) -> Result<usize, RelationalRowPageSnapshotReadError> {
    let RelationalRowPageProjectedOverlayValue::Present { fields, .. } = value else {
        return Ok(0);
    };
    fields.iter().try_fold(
        size_of::<RelationalRowPageProjectedOverlayValue>()
            .checked_add(
                fields
                    .len()
                    .checked_mul(size_of::<RelationalProjectedField>())
                    .ok_or_else(|| {
                        RelationalRowPageSnapshotReadError::Admission(
                            "overlay projection allocation accounting overflow".to_string(),
                        )
                    })?,
            )
            .and_then(|bytes| bytes.checked_add(4 * size_of::<usize>()))
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay projection byte accounting overflow".to_string(),
                )
            })?,
        |bytes, field| {
            bytes
                .checked_add(field.value.estimated_payload_bytes())
                .ok_or_else(|| {
                    RelationalRowPageSnapshotReadError::Admission(
                        "overlay projection payload accounting overflow".to_string(),
                    )
                })
        },
    )
}

fn project_overlay_value(
    value: &RelationalRowPageRecoveredValue,
    requested_fields: &[usize],
    binds_overlay_overflow: bool,
) -> RelationalRowPageProjectedOverlayValue {
    let RelationalRowPageRecoveredValue::Present(row) = value else {
        return RelationalRowPageProjectedOverlayValue::Deleted;
    };
    RelationalRowPageProjectedOverlayValue::Present {
        fields: requested_fields
            .iter()
            .map(|ordinal| RelationalProjectedField {
                ordinal: *ordinal,
                value: row.values()[*ordinal].clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        binds_overlay_overflow,
    }
}

fn overlay_point_resident_bytes(
    key: &RelationalKey,
    value_resident_bytes: usize,
) -> Result<usize, RelationalRowPageSnapshotReadError> {
    overlay_key_resident_bytes(key)?
        .checked_add(value_resident_bytes)
        .and_then(|bytes| bytes.checked_add(size_of::<RelationalProjectedRow>()))
        .and_then(|bytes| bytes.checked_add(4 * size_of::<usize>()))
        .ok_or_else(|| {
            RelationalRowPageSnapshotReadError::Admission(
                "overlay point resident-byte accounting overflow".to_string(),
            )
        })
}

fn overlay_key_resident_bytes(
    key: &RelationalKey,
) -> Result<usize, RelationalRowPageSnapshotReadError> {
    key.0.iter().try_fold(
        size_of::<RelationalKey>()
            .checked_add(
                key.0
                    .len()
                    .checked_mul(size_of::<RelationalValue>())
                    .ok_or_else(|| {
                        RelationalRowPageSnapshotReadError::Admission(
                            "overlay key allocation accounting overflow".to_string(),
                        )
                    })?,
            )
            .ok_or_else(|| {
                RelationalRowPageSnapshotReadError::Admission(
                    "overlay key byte accounting overflow".to_string(),
                )
            })?,
        |bytes, value| {
            bytes
                .checked_add(value.estimated_payload_bytes())
                .ok_or_else(|| {
                    RelationalRowPageSnapshotReadError::Admission(
                        "overlay key payload accounting overflow".to_string(),
                    )
                })
        },
    )
}

fn map_delta_error(error: RelationalRowDeltaError) -> RelationalRowPageSnapshotReadError {
    match error {
        RelationalRowDeltaError::Admission(message) => {
            RelationalRowPageSnapshotReadError::Admission(message)
        }
        RelationalRowDeltaError::Durability(message) => {
            RelationalRowPageSnapshotReadError::Durability(message)
        }
        RelationalRowDeltaError::Row(error) => map_row_error(error),
        RelationalRowDeltaError::Publication(error) => map_row_publication_error(error),
        error @ (RelationalRowDeltaError::Corrupt(_)
        | RelationalRowDeltaError::RequiresCheckpoint { .. }
        | RelationalRowDeltaError::Invalidated(_)
        | RelationalRowDeltaError::StaleGeneration { .. }
        | RelationalRowDeltaError::StaleBase { .. }) => {
            RelationalRowPageSnapshotReadError::Corrupt(error.to_string())
        }
    }
}

fn map_demand_error(error: RelationalRowPageDemandReadError) -> RelationalRowPageSnapshotReadError {
    match error {
        RelationalRowPageDemandReadError::Admission(message) => {
            RelationalRowPageSnapshotReadError::Admission(message)
        }
        RelationalRowPageDemandReadError::Corrupt(message) => {
            RelationalRowPageSnapshotReadError::Corrupt(message)
        }
        RelationalRowPageDemandReadError::Durability(message) => {
            RelationalRowPageSnapshotReadError::Durability(message)
        }
        RelationalRowPageDemandReadError::MissingTable(table) => {
            RelationalRowPageSnapshotReadError::MissingTable(table)
        }
        RelationalRowPageDemandReadError::Stopped(reason) => {
            RelationalRowPageSnapshotReadError::Stopped(reason)
        }
    }
}

fn map_row_error(error: RelationalRowPageError) -> RelationalRowPageSnapshotReadError {
    match error {
        RelationalRowPageError::Admission(message) => {
            RelationalRowPageSnapshotReadError::Admission(message)
        }
        RelationalRowPageError::Corrupt(message) => {
            RelationalRowPageSnapshotReadError::Corrupt(message)
        }
    }
}

fn map_row_publication_error(
    error: RelationalRowPagePublicationError,
) -> RelationalRowPageSnapshotReadError {
    match error {
        RelationalRowPagePublicationError::Admission(message) => {
            RelationalRowPageSnapshotReadError::Admission(message)
        }
        RelationalRowPagePublicationError::Corrupt(message) => {
            RelationalRowPageSnapshotReadError::Corrupt(message)
        }
        RelationalRowPagePublicationError::Durability(message) => {
            RelationalRowPageSnapshotReadError::Durability(message)
        }
        RelationalRowPagePublicationError::MissingTable(table) => {
            RelationalRowPageSnapshotReadError::MissingTable(table)
        }
        error @ RelationalRowPagePublicationError::StaleGeneration { .. } => {
            RelationalRowPageSnapshotReadError::Corrupt(error.to_string())
        }
    }
}

#[cfg(test)]
mod tests;
