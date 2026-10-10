// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Experimental demand-driven delivery for the materialized numeric plan.
//!
//! Native payload handoff is allocation-preserving. Pinned node-page capacity,
//! planning workspace and foreign adapters are not yet fully qualified; this is not a
//! general query, source-reuse, or whole-operation memory-bounded capability.

use super::{query_runtime, Database, DatabaseReadTransaction, QuerySystemVariables};
use crate::store::{GraphStore, MaterializedNodeReadSource};
use crate::{
    DatabaseConfig, HawDBError, RuntimeGovernor, RuntimeGovernorConfig, RuntimeResourceSnapshot,
    Value,
};
use hawdb_core::{LabelId, PropertyType, RuntimeCancellationReason, RuntimeTaskContext};
use hawdb_executor::numeric::retained::{
    NumericBatchOwner, NumericBufferProvenance, RetainedNumericBatch, RetainedNumericBuilder,
    RetainedNumericError, RetainedNumericValues,
};
use hawdb_executor::numeric::{
    try_prepare_retained_numeric_plan, NumericFragment, RetainedNumericPlan,
};
use hawdb_executor::{
    NumericLiteral, NumericPredicate, QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease,
    QueryMemoryLedger, ValidityView,
};
use hawdb_plan_cypher::ProjectionExpression;
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeAdmissionError, RuntimeRetainedResult, RuntimeRetainedResultError,
    RuntimeWorkRequest,
};
use hawdb_storage::NodeId;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetainedQueryError {
    UnsupportedPlan,
    UnsupportedLayout,
    UnsupportedType,
    CopyRequired,
    SelectionRequiresMaterialization,
    Backpressure {
        outstanding_batches: usize,
        batch_limit: usize,
    },
    WorkingUnitTooLarge,
    ResultBudget {
        rows: usize,
        payload_bytes: usize,
    },
    Admission(RuntimeAdmissionError),
    RetainedAdmission(RuntimeRetainedResultError),
    Execution(HawDBError),
    Stopped(RuntimeCancellationReason),
    Closed,
    InvalidColumn,
    SizeOverflow,
    AdapterDelivery,
}

impl RetainedQueryError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Backpressure { .. } => true,
            Self::Admission(error) => error.is_retryable(),
            Self::RetainedAdmission(error) => error.is_retryable(),
            _ => false,
        }
    }
}

impl std::fmt::Display for RetainedQueryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admission(error) => error.fmt(formatter),
            Self::RetainedAdmission(error) => error.fmt(formatter),
            Self::Execution(error) => error.fmt(formatter),
            Self::Stopped(error) => error.fmt(formatter),
            error => write!(formatter, "retained query {error:?}"),
        }
    }
}

impl std::error::Error for RetainedQueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Admission(error) => Some(error),
            Self::RetainedAdmission(error) => Some(error),
            Self::Execution(error) => Some(error),
            Self::Stopped(error) => Some(error),
            _ => None,
        }
    }
}

impl From<HawDBError> for RetainedQueryError {
    fn from(error: HawDBError) -> Self {
        Self::Execution(error)
    }
}
impl From<RuntimeAdmissionError> for RetainedQueryError {
    fn from(error: RuntimeAdmissionError) -> Self {
        Self::Admission(error)
    }
}
impl From<RuntimeRetainedResultError> for RetainedQueryError {
    fn from(error: RuntimeRetainedResultError) -> Self {
        Self::RetainedAdmission(error)
    }
}
impl From<RetainedNumericError> for RetainedQueryError {
    fn from(error: RetainedNumericError) -> Self {
        match error {
            RetainedNumericError::Admission(error) => Self::RetainedAdmission(error),
            RetainedNumericError::Execution(error) => Self::Execution(error),
        }
    }
}

type Result<T> = std::result::Result<T, RetainedQueryError>;

/// One admission binding per database, shared by all snapshots/cursors. Late
/// ordinary-governor replacement cannot multiply retained-result allowances.
#[derive(Debug, Default)]
pub(super) struct RetainedRuntime {
    configured: Mutex<Option<RuntimeGovernor>>,
    bound: OnceLock<RuntimeGovernor>,
}
impl RetainedRuntime {
    pub(super) fn configure(&self, governor: RuntimeGovernor) {
        *self
            .configured
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(governor);
    }
    fn governor(&self, minimum_handles: usize) -> Result<&RuntimeGovernor> {
        let minimum_handles = minimum_handles.max(2);
        if let Some(governor) = self.bound.get() {
            if governor.retained_result_snapshot().handle_limit < minimum_handles {
                return Err(RetainedQueryError::WorkingUnitTooLarge);
            }
            return Ok(governor);
        }
        let configured = self
            .configured
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let candidate = configured.clone().unwrap_or_else(|| {
            RuntimeGovernor::new(
                RuntimeGovernorConfig::shared_host(),
                RuntimeResourceSnapshot::detect(),
                IoConcurrencyBudget::new(2, 1),
            )
        });
        // The control owner and first batch each require a handle. Reject an
        // unusable configuration before binding so the host can correct it.
        if candidate.retained_result_snapshot().handle_limit < minimum_handles {
            return Err(RetainedQueryError::WorkingUnitTooLarge);
        }
        Ok(self.bound.get_or_init(|| candidate))
    }
}

/// Every delivered batch is provisional until the cursor completes successfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RetainedQueryStatus {
    Open,
    Completed,
    Failed,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedColumnType {
    Int64,
    Float64,
    UInt64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedColumnRole {
    Property,
    NodeIdentity,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedColumnSchema {
    pub name: String,
    pub data_type: RetainedColumnType,
    pub nullable: bool,
    pub role: RetainedColumnRole,
}
#[derive(Debug, Clone, Copy)]
pub enum RetainedColumnValues<'a> {
    Int64(&'a [i64]),
    Float64(&'a [f64]),
    UInt64(&'a [u64]),
}

#[derive(Debug, Clone, Copy)]
pub struct RetainedQueryOptions {
    /// Maximum source records inspected per pull, clamped by database configuration.
    pub batch_rows: NonZeroUsize,
    /// Maximum admitted numeric owner/view capacity per batch, also clamped.
    pub batch_bytes: NonZeroUsize,
    /// Distinct live payload owners. Retaining a view does not consume another slot.
    pub outstanding_batches: NonZeroUsize,
    pub max_result_rows: Option<usize>,
    /// Conservative selected fixed-width payload bytes, including null slots.
    pub max_result_payload_bytes: Option<usize>,
    pub require_source_reuse: bool,
    /// Additional cursor/adapter owner capacity, admitted before ownership transfer.
    pub adapter_metadata_bytes: usize,
    /// Minimum shared handles needed by the adapter's smallest usable delivery.
    /// The root control and first batch always require at least two.
    pub minimum_shared_handles: NonZeroUsize,
}
impl Default for RetainedQueryOptions {
    fn default() -> Self {
        Self {
            batch_rows: NonZeroUsize::new(1024).unwrap(),
            batch_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            outstanding_batches: NonZeroUsize::new(2).unwrap(),
            max_result_rows: None,
            max_result_payload_bytes: None,
            require_source_reuse: false,
            adapter_metadata_bytes: 0,
            minimum_shared_handles: NonZeroUsize::new(2).unwrap(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetainedQueryProfile {
    /// Original source cardinality, including unrelated labels; observed without a scan.
    pub source_snapshot_rows: usize,
    pub source_pinned_rows: usize,
    pub source_pinned_pages: usize,
    /// Shared directory capacity only; excludes node-page payload allocations.
    pub source_directory_capacity_bytes: usize,
    pub pulls: usize,
    pub visited_rows: usize,
    pub predicate_selected_rows: usize,
    pub emitted_rows: usize,
    pub output_payload_bytes: usize,
    pub source_constructed_bytes: usize,
    pub selection_bytes_generated: usize,
    pub projection_payload_copy_bytes: usize,
    pub handoff_payload_copy_bytes: usize,
    pub backpressure_events: usize,
    pub peak_outstanding_batches: usize,
    pub query_peak_bytes: usize,
}

#[derive(Debug)]
struct CursorShared {
    schema: Vec<RetainedColumnSchema>,
    outstanding: AtomicUsize,
    status: AtomicU8,
    limit: usize,
    _memory: QueryMemoryLease,
    _runtime: RuntimeRetainedResult,
}
impl CursorShared {
    fn status(&self) -> RetainedQueryStatus {
        match self.status.load(Ordering::Acquire) {
            0 => RetainedQueryStatus::Open,
            1 => RetainedQueryStatus::Completed,
            2 => RetainedQueryStatus::Failed,
            _ => RetainedQueryStatus::Closed,
        }
    }
}

#[derive(Debug)]
struct SlotGuard {
    shared: Arc<CursorShared>,
}
impl NumericBatchOwner for SlotGuard {}
impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.shared.outstanding.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Owns immutable execution buffers. It does not own a database, transaction,
/// source iterator, or page pin. Additional views must pass admission.
#[derive(Debug)]
pub struct RetainedQueryBatch {
    // The payload owner also retains this guard. Drop this wrapper reference
    // first so the last guard releases after payload, under its runtime charge.
    slot: Arc<SlotGuard>,
    numeric: RetainedNumericBatch,
    selection: Range<usize>,
}
impl RetainedQueryBatch {
    pub fn schema(&self) -> &[RetainedColumnSchema] {
        &self.slot.shared.schema
    }
    pub fn status(&self) -> RetainedQueryStatus {
        self.slot.shared.status()
    }
    pub fn physical_rows(&self) -> usize {
        self.numeric.physical_rows()
    }
    pub fn selected_rows(&self) -> &[u32] {
        &self.numeric.selected_rows().0[self.selection.clone()]
    }
    pub fn selection_provenance(&self) -> NumericBufferProvenance {
        let mut view = self.numeric.buffer_provenance()[3].expect("selection provenance");
        view.byte_offset += self.selection.start * std::mem::size_of::<u32>();
        view.byte_length = self.selection.len() * std::mem::size_of::<u32>();
        view
    }
    pub fn column(&self, index: usize) -> Result<RetainedColumnValues<'_>> {
        let column = self
            .schema()
            .get(index)
            .ok_or(RetainedQueryError::InvalidColumn)?;
        Ok(match column.role {
            RetainedColumnRole::NodeIdentity => RetainedColumnValues::UInt64(
                self.numeric.node_ids().expect("admitted identity column").0,
            ),
            RetainedColumnRole::Property => match self.numeric.values().0 {
                RetainedNumericValues::Int(values) => RetainedColumnValues::Int64(values),
                RetainedNumericValues::Float(values) => RetainedColumnValues::Float64(values),
            },
        })
    }
    pub fn column_provenance(&self, index: usize) -> Result<NumericBufferProvenance> {
        let column = self
            .schema()
            .get(index)
            .ok_or(RetainedQueryError::InvalidColumn)?;
        Ok(self.numeric.buffer_provenance()
            [usize::from(column.role == RetainedColumnRole::NodeIdentity)]
        .expect("column provenance"))
    }
    pub fn validity(&self, index: usize) -> Result<ValidityView<'_>> {
        let column = self
            .schema()
            .get(index)
            .ok_or(RetainedQueryError::InvalidColumn)?;
        Ok(if column.role == RetainedColumnRole::NodeIdentity {
            ValidityView::All {
                len: self.physical_rows(),
            }
        } else {
            self.numeric.validity()
        })
    }
    pub fn validity_provenance(&self, index: usize) -> Result<Option<NumericBufferProvenance>> {
        let column = self
            .schema()
            .get(index)
            .ok_or(RetainedQueryError::InvalidColumn)?;
        Ok(if column.role == RetainedColumnRole::NodeIdentity {
            None
        } else {
            self.numeric.buffer_provenance()[2]
        })
    }
    pub fn try_retain(&self, adapter_metadata_bytes: usize) -> Result<Self> {
        let metadata = std::mem::size_of::<Self>()
            .checked_add(adapter_metadata_bytes)
            .ok_or(RetainedQueryError::SizeOverflow)?;
        let numeric = self.numeric.try_retain(metadata)?;
        Ok(Self {
            numeric,
            selection: self.selection.clone(),
            slot: Arc::clone(&self.slot),
        })
    }
}

#[derive(Debug)]
struct OwnedFragment {
    label: String,
    property: String,
    property_type: PropertyType,
    predicate: NumericPredicate,
    expected: NumericLiteral,
}
impl OwnedFragment {
    fn borrowed(&self) -> NumericFragment<'_> {
        NumericFragment {
            label: &self.label,
            property: &self.property,
            property_type: self.property_type,
            predicate: self.predicate,
            expected: self.expected,
            fused_operators: None,
        }
    }
}

/// Serializes pulls through `&mut self`. No work is prefetched between pulls.
#[derive(Debug)]
pub struct RetainedQueryCursor {
    source: Option<RetainedSource>,
    fragment: OwnedFragment,
    label_id: LabelId,
    last_node: Option<NodeId>,
    offset: usize,
    limit: Option<usize>,
    shared: Arc<CursorShared>,
    governor: RuntimeGovernor,
    ledger: QueryMemoryLedger,
    account: QueryMemoryAccount,
    options: RetainedQueryOptions,
    profile: RetainedQueryProfile,
    terminal_error: Option<RetainedQueryError>,
    result_allowance: u64,
    needs_ids: bool,
}

#[derive(Debug)]
struct RetainedSource {
    nodes: MaterializedNodeReadSource,
    task_context: Option<RuntimeTaskContext>,
}

fn require_materialized_source(
    store: &GraphStore,
    require_source_reuse: bool,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<()> {
    store.ensure_usable()?;
    if let Some(context) = task_context {
        context.checkpoint().map_err(RetainedQueryError::Stopped)?;
    }
    if require_source_reuse
        || store
            .try_scan_materialized_nodes_after(None, None)?
            .is_none()
    {
        return Err(RetainedQueryError::CopyRequired);
    }
    Ok(())
}

fn restrictive(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    match (left, right) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) | (None, Some(a)) => Some(a),
        _ => None,
    }
}

impl Database {
    /// Experimental retained numeric delivery; other plans refuse explicitly.
    /// Configure the runtime governor before the first retained cursor. Its
    /// binding then remains shared by this database and all read snapshots.
    /// Source snapshot/planning workspace qualification is still incomplete.
    pub fn query_with_params_retained(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: RetainedQueryOptions,
    ) -> Result<RetainedQueryCursor> {
        let runtime = self.runtime.get()?;
        require_materialized_source(&runtime.store, options.require_source_reuse, None)?;
        let prepared = query_runtime::parse_runtime_execution(cypher)?;
        super::query_work_request_for_statement(&self.system_variables, &prepared.statement)?;
        let optimized = self.optimized_query_plan_with_access_control(
            cypher,
            &prepared.statement,
            parameters,
            None,
        )?;
        let plan = try_prepare_retained_numeric_plan(&optimized.physical_plan, &runtime.catalog)
            .ok_or(RetainedQueryError::UnsupportedPlan)?;
        let label_id = runtime
            .catalog
            .label_id(plan.fragment.label)
            .ok_or(RetainedQueryError::UnsupportedPlan)?;
        RetainedQueryCursor::from_plan(
            plan,
            label_id,
            &runtime.store,
            &self.config,
            &self.retained_runtime,
            None,
            options,
        )
    }
    pub fn retained_result_snapshot(&self) -> Option<hawdb_qos::RuntimeRetainedResultSnapshot> {
        self.retained_runtime
            .bound
            .get()
            .map(RuntimeGovernor::retained_result_snapshot)
    }
}

impl DatabaseReadTransaction {
    /// Consumes this read snapshot for an eligible experimental numeric cursor.
    /// The schema is fixed before pulling; batches remain provisional until EOF.
    pub fn into_retained_query(
        self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: RetainedQueryOptions,
    ) -> Result<RetainedQueryCursor> {
        require_materialized_source(
            &self.store,
            options.require_source_reuse,
            self.task_context.as_ref(),
        )?;
        let prepared = query_runtime::parse_runtime_execution(cypher)?;
        super::query_work_request_for_statement(
            &QuerySystemVariables::default(),
            &prepared.statement,
        )?;
        let optimized = self.optimized_query_plan_with_access_control(
            cypher,
            &prepared.statement,
            parameters,
            None,
        )?;
        let plan = try_prepare_retained_numeric_plan(&optimized.physical_plan, &self.catalog)
            .ok_or(RetainedQueryError::UnsupportedPlan)?;
        let label_id = self
            .catalog
            .label_id(plan.fragment.label)
            .ok_or(RetainedQueryError::UnsupportedPlan)?;
        RetainedQueryCursor::from_plan(
            plan,
            label_id,
            &self.store,
            &self.config,
            &self.retained_runtime,
            self.task_context.clone(),
            options,
        )
    }
}

impl RetainedQueryCursor {
    fn from_plan(
        plan: RetainedNumericPlan<'_>,
        label_id: LabelId,
        store: &GraphStore,
        config: &DatabaseConfig,
        runtime: &RetainedRuntime,
        task_context: Option<RuntimeTaskContext>,
        mut options: RetainedQueryOptions,
    ) -> Result<Self> {
        let governor = runtime
            .governor(options.minimum_shared_handles.get())?
            .clone();
        let result_allowance = governor.snapshot().limits.result_budget_bytes;
        let permit = governor.try_admit(
            RuntimeWorkRequest::foreground_query(0, result_allowance).with_blocking(false),
        )?;
        let ledger = QueryMemoryLedger::new(config.execution_memory.query_memory_bytes);
        let account = ledger.account(
            QueryMemoryClass::ResultMaterialization,
            "retained_query",
            config.execution_memory.query_memory_bytes,
        );
        let schema_bytes = plan
            .projections
            .iter()
            .try_fold(0usize, |bytes, item| {
                bytes
                    .checked_add(std::mem::size_of::<RetainedColumnSchema>())?
                    .checked_add(item.name.len())?
                    .checked_add(64)
            })
            .ok_or(RetainedQueryError::SizeOverflow)?;
        let metadata = schema_bytes
            .checked_add(std::mem::size_of::<CursorShared>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<RetainedQueryCursor>()))
            .and_then(|bytes| bytes.checked_add(plan.fragment.label.len()))
            .and_then(|bytes| bytes.checked_add(plan.fragment.property.len()))
            .and_then(|bytes| bytes.checked_add(options.adapter_metadata_bytes))
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or(RetainedQueryError::SizeOverflow)?;
        let runtime = permit.reserve_retained_result(metadata as u64, 0)?;
        let memory = account.reserve(
            metadata
                .checked_add(RuntimeRetainedResult::owner_overhead_bytes() as usize)
                .and_then(|bytes| bytes.checked_add(runtime.handle_bytes() as usize))
                .ok_or(RetainedQueryError::SizeOverflow)?,
        )?;
        let mut schema = Vec::with_capacity(plan.projections.len());
        for projection in plan.projections {
            let identity = matches!(projection.expression, ProjectionExpression::Id { .. });
            schema.push(RetainedColumnSchema {
                name: projection.name.clone(),
                data_type: if identity {
                    RetainedColumnType::UInt64
                } else if plan.fragment.property_type == PropertyType::Int {
                    RetainedColumnType::Int64
                } else {
                    RetainedColumnType::Float64
                },
                nullable: !identity,
                role: if identity {
                    RetainedColumnRole::NodeIdentity
                } else {
                    RetainedColumnRole::Property
                },
            });
        }
        let needs_ids = schema
            .iter()
            .any(|column| column.role == RetainedColumnRole::NodeIdentity);
        let fragment = OwnedFragment {
            label: plan.fragment.label.to_owned(),
            property: plan.fragment.property.to_owned(),
            property_type: plan.fragment.property_type,
            predicate: plan.fragment.predicate,
            expected: plan.fragment.expected,
        };
        let offset = plan.offset;
        let limit = plan.limit;
        options.batch_rows = NonZeroUsize::new(
            options
                .batch_rows
                .get()
                .min(config.execution_memory.batch_rows.get()),
        )
        .unwrap();
        options.batch_bytes = NonZeroUsize::new(
            options
                .batch_bytes
                .get()
                .min(config.execution_memory.batch_payload_bytes.get()),
        )
        .unwrap();
        options.max_result_rows = restrictive(config.max_read_result_rows, options.max_result_rows);
        options.max_result_payload_bytes = restrictive(
            config.max_read_result_payload_bytes,
            options.max_result_payload_bytes,
        );
        let source = RetainedSource {
            nodes: store
                .try_materialized_node_read_source()?
                .ok_or(RetainedQueryError::CopyRequired)?,
            task_context,
        };
        let profile = RetainedQueryProfile {
            source_snapshot_rows: source.nodes.row_count(),
            ..RetainedQueryProfile::default()
        };
        let shared = Arc::new(CursorShared {
            schema,
            outstanding: AtomicUsize::new(0),
            status: AtomicU8::new(0),
            limit: options.outstanding_batches.get(),
            _memory: memory,
            _runtime: runtime,
        });
        drop(permit);
        Ok(RetainedQueryCursor {
            source: Some(source),
            fragment,
            label_id,
            last_node: None,
            offset,
            limit,
            shared,
            governor,
            ledger,
            account,
            options,
            profile,
            terminal_error: None,
            result_allowance,
            needs_ids,
        })
    }
}

impl RetainedQueryCursor {
    pub fn schema(&self) -> &[RetainedColumnSchema] {
        &self.shared.schema
    }
    pub fn status(&self) -> RetainedQueryStatus {
        self.shared.status()
    }
    pub fn profile(&self) -> RetainedQueryProfile {
        RetainedQueryProfile {
            query_peak_bytes: self.ledger.snapshot().peak_bytes,
            source_pinned_rows: self
                .source
                .as_ref()
                .map_or(0, |source| source.nodes.row_count()),
            source_pinned_pages: self
                .source
                .as_ref()
                .map_or(0, |source| source.nodes.page_count()),
            source_directory_capacity_bytes: self
                .source
                .as_ref()
                .map_or(0, |source| source.nodes.directory_capacity_bytes()),
            ..self.profile
        }
    }
    pub fn outstanding_batches(&self) -> usize {
        self.shared.outstanding.load(Ordering::Acquire)
    }
    pub fn close(&mut self) {
        if self.status() == RetainedQueryStatus::Open {
            self.shared
                .status
                .store(RetainedQueryStatus::Closed as u8, Ordering::Release);
        }
        self.source.take();
    }
    /// Fail a foreign delivery after a native batch was produced. Earlier
    /// leases remain readable but provisional; further pulls repeat the error.
    /// Native emitted counters describe the root handoff, so adapters must
    /// report successful foreign deliveries separately.
    pub fn abort_delivery(&mut self) {
        if self.status() == RetainedQueryStatus::Open {
            self.terminal_error = Some(RetainedQueryError::AdapterDelivery);
            self.shared
                .status
                .store(RetainedQueryStatus::Failed as u8, Ordering::Release);
            self.source.take();
        }
    }
    fn complete(&mut self) {
        self.shared
            .status
            .store(RetainedQueryStatus::Completed as u8, Ordering::Release);
        self.source.take();
    }
    pub fn next_batch(&mut self) -> Result<Option<RetainedQueryBatch>> {
        self.next_batch_with_metadata(0)
    }
    /// Admit adapter owner/descriptor capacity before producing the next batch.
    /// Failure never advances the source. The charge follows the immutable owner.
    pub fn next_batch_with_metadata(
        &mut self,
        adapter_metadata_bytes: usize,
    ) -> Result<Option<RetainedQueryBatch>> {
        match self.status() {
            RetainedQueryStatus::Completed => return Ok(None),
            RetainedQueryStatus::Closed => return Err(RetainedQueryError::Closed),
            RetainedQueryStatus::Failed => {
                return Err(self.terminal_error.clone().expect("terminal evidence"))
            }
            RetainedQueryStatus::Open => {}
        }
        self.profile.pulls += 1;
        let result = self.pull(adapter_metadata_bytes);
        if let Err(error) = &result {
            if error.is_retryable() {
                self.profile.backpressure_events += 1;
            } else {
                self.terminal_error = Some(error.clone());
                self.shared
                    .status
                    .store(RetainedQueryStatus::Failed as u8, Ordering::Release);
                self.source.take();
            }
        }
        result
    }
    fn admitted_rows(&self, adapter_metadata_bytes: usize) -> Result<NonZeroUsize> {
        let snapshot = self.governor.retained_result_snapshot();
        let available = self
            .options
            .batch_bytes
            .get()
            .min(self.account.available_bytes())
            .min(
                usize::try_from(
                    snapshot
                        .budget_bytes
                        .saturating_sub(snapshot.retained_bytes),
                )
                .unwrap_or(usize::MAX),
            );
        let fixed = (std::mem::size_of::<SlotGuard>()
            + 2 * std::mem::size_of::<usize>()
            + std::mem::size_of::<RetainedQueryBatch>()
            + std::mem::size_of::<RetainedNumericBatch>()
            + RuntimeRetainedResult::owner_overhead_bytes() as usize
            + RuntimeRetainedResult::handle_overhead_bytes() as usize)
            .checked_add(adapter_metadata_bytes)
            .ok_or(RetainedQueryError::SizeOverflow)?;
        let required_one =
            RetainedNumericBuilder::required_capacity_bytes(NonZeroUsize::MIN, self.needs_ids)
                .and_then(|bytes| bytes.checked_add(fixed))
                .ok_or(RetainedQueryError::SizeOverflow)?;
        let complete_allowance = self
            .options
            .batch_bytes
            .get()
            .min(
                self.ledger
                    .snapshot()
                    .budget_bytes
                    .saturating_sub(self.shared._memory.bytes()),
            )
            .min(
                usize::try_from(snapshot.budget_bytes)
                    .unwrap_or(usize::MAX)
                    .saturating_sub(self.shared._memory.bytes()),
            );
        if required_one > complete_allowance {
            return Err(RetainedQueryError::WorkingUnitTooLarge);
        }
        let mut lower = 0;
        let mut upper = self.options.batch_rows.get().min(u32::MAX as usize);
        while lower < upper {
            let rows = lower + (upper - lower).div_ceil(2);
            let bytes = RetainedNumericBuilder::required_capacity_bytes(
                NonZeroUsize::new(rows).unwrap(),
                self.needs_ids,
            )
            .and_then(|bytes| bytes.checked_add(fixed));
            if bytes.is_some_and(|bytes| bytes <= available) {
                lower = rows;
            } else {
                upper = rows - 1;
            }
        }
        NonZeroUsize::new(lower).ok_or_else(|| RetainedQueryError::Backpressure {
            outstanding_batches: self.outstanding_batches(),
            batch_limit: self.shared.limit,
        })
    }
    fn pull(&mut self, adapter_metadata_bytes: usize) -> Result<Option<RetainedQueryBatch>> {
        let source = self.source.as_ref().expect("open source");
        if let Some(context) = &source.task_context {
            context.checkpoint().map_err(RetainedQueryError::Stopped)?;
        }
        if self.limit == Some(0) {
            self.complete();
            return Ok(None);
        }
        let count = self.outstanding_batches();
        if count >= self.shared.limit {
            return Err(RetainedQueryError::Backpressure {
                outstanding_batches: count,
                batch_limit: self.shared.limit,
            });
        }
        let rows = self.admitted_rows(adapter_metadata_bytes)?;
        let permit = self.governor.try_admit(
            RuntimeWorkRequest::foreground_query(0, self.result_allowance).with_blocking(false),
        )?;
        let owner_metadata = std::mem::size_of::<SlotGuard>() + 2 * std::mem::size_of::<usize>();
        let mut builder = RetainedNumericBuilder::new_with_metadata(
            self.fragment.borrowed(),
            rows,
            self.needs_ids,
            self.account.clone(),
            &permit,
            owner_metadata,
            std::mem::size_of::<RetainedQueryBatch>()
                .checked_add(adapter_metadata_bytes)
                .ok_or(RetainedQueryError::SizeOverflow)?,
        )?;
        self.shared.outstanding.fetch_add(1, Ordering::AcqRel);
        self.profile.peak_outstanding_batches = self
            .profile
            .peak_outstanding_batches
            .max(self.outstanding_batches());
        let slot = Arc::new(SlotGuard {
            shared: Arc::clone(&self.shared),
        });
        builder.attach_owner(Arc::clone(&slot))?;
        let mut last = self.last_node;
        let mut visited = 0usize;
        for node in source.nodes.iter_after(self.last_node)?.take(rows.get()) {
            visited += 1;
            self.profile.visited_rows += 1;
            if let Some(context) = &source.task_context {
                context.checkpoint().map_err(RetainedQueryError::Stopped)?;
            }
            // Bound inspected source records, including unrelated labels.
            // A filtered-empty batch is distinct from successful EOF.
            if node.labels.contains(&self.label_id) {
                builder.push_node(node)?;
                self.profile.source_constructed_bytes += if self.needs_ids { 16 } else { 8 };
            }
            last = Some(node.id);
        }
        if visited == 0 {
            drop((builder, slot, permit));
            self.complete();
            return Ok(None);
        }
        let numeric = builder.seal(None)?;
        let selected = numeric.selected_rows().0.len();
        self.profile.predicate_selected_rows += selected;
        self.profile.selection_bytes_generated += std::mem::size_of_val(numeric.selected_rows().0);
        let start = self.offset.min(selected);
        let count = (selected - start).min(self.limit.unwrap_or(usize::MAX));
        let emitted = self
            .profile
            .emitted_rows
            .checked_add(count)
            .ok_or(RetainedQueryError::SizeOverflow)?;
        let bytes = count
            .checked_mul(self.schema().len())
            .and_then(|bytes| bytes.checked_mul(8))
            .ok_or(RetainedQueryError::SizeOverflow)?;
        let output_bytes = self
            .profile
            .output_payload_bytes
            .checked_add(bytes)
            .ok_or(RetainedQueryError::SizeOverflow)?;
        if self
            .options
            .max_result_rows
            .is_some_and(|limit| emitted > limit)
            || self
                .options
                .max_result_payload_bytes
                .is_some_and(|limit| output_bytes > limit)
        {
            return Err(RetainedQueryError::ResultBudget {
                rows: emitted,
                payload_bytes: output_bytes,
            });
        }
        self.last_node = last;
        self.offset -= start;
        if let Some(limit) = &mut self.limit {
            *limit -= count;
        }
        self.profile.emitted_rows = emitted;
        self.profile.output_payload_bytes = output_bytes;
        Ok(Some(RetainedQueryBatch {
            numeric,
            selection: start..start + count,
            slot,
        }))
    }
}
impl Drop for RetainedQueryCursor {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests;
