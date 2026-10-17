// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Experimental single-match indexed node delivery. Native views reuse source
//! allocations; recursive node maps are not represented as Arrow arrays.
//! Planning/index-key workspace and foreign adapters remain separate gates.

use super::*;
use hawdb_core::schema::{RetainedLabelAllocation, RetainedLabelName};
use hawdb_core::Catalog;
use hawdb_plan_cypher::{PhysicalPlan, Predicate};
use hawdb_storage::read_view::GraphReadAllocation;
use hawdb_storage::store::{
    MaterializedNodePointRead, MaterializedNodePointSource, MaterializedReadHealth,
};

#[derive(Debug)]
struct PrepaidSource(usize);
impl RetainedLabelAllocation for PrepaidSource {
    fn bytes(&self) -> usize {
        self.0
    }
}
impl GraphReadAllocation for PrepaidSource {
    fn bytes(&self) -> usize {
        self.0
    }
    fn grow(&mut self, _: usize) -> crate::Result<()> {
        Err(HawDBError::Execution(
            "immutable source admission cannot grow".into(),
        ))
    }
}

struct NodeOwner {
    node: MaterializedNodePointSource<PrepaidSource>,
    labels: Vec<RetainedLabelName<PrepaidSource>>,
    column: String,
    status: AtomicU8,
    account: QueryMemoryAccount,
    ledger: QueryMemoryLedger,
    adapter_metadata_bytes: usize,
    // Complete source and owner capacity stays charged until the last view.
    _memory: QueryMemoryLease,
}

/// Borrowed logical node fields, including the ordinary synthetic fields.
#[derive(Debug, Clone, Copy)]
pub enum RetainedNodeField<'a> {
    Value(&'a Value),
    Identity(i64),
    Labels(RetainedNodeLabels<'a>),
}

#[derive(Debug, Clone, Copy)]
pub struct RetainedNodeLabels<'a> {
    labels: &'a [RetainedLabelName<PrepaidSource>],
}
impl<'a> RetainedNodeLabels<'a> {
    pub fn iter(self) -> impl ExactSizeIterator<Item = &'a str> {
        self.labels.iter().map(RetainedLabelName::name)
    }
}

/// One independently admitted native view. Borrowed fields cannot outlive it.
/// Closing its cursor does not revoke immutable payload already delivered.
pub struct RetainedNodeView {
    source: Arc<NodeOwner>,
    _view_memory: Option<QueryMemoryLease>,
    // Destroy the last payload owner before releasing the runtime result charge.
    runtime: RuntimeRetainedResult,
}
impl std::fmt::Debug for RetainedNodeView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedNodeView")
            .field("column", &self.source.column)
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}
impl RetainedNodeView {
    pub fn column_name(&self) -> &str {
        &self.source.column
    }

    pub fn status(&self) -> RetainedQueryStatus {
        match self.source.status.load(Ordering::Acquire) {
            0 => RetainedQueryStatus::Open,
            1 => RetainedQueryStatus::Completed,
            2 => RetainedQueryStatus::Failed,
            _ => RetainedQueryStatus::Closed,
        }
    }

    pub fn field(&self, name: &str) -> Result<Option<RetainedNodeField<'_>>> {
        let node = self.source.node.node()?;
        Ok(match name {
            "_id" => Some(RetainedNodeField::Identity(node.id.0 as i64)),
            "labels" => Some(RetainedNodeField::Labels(RetainedNodeLabels {
                labels: &self.source.labels,
            })),
            name => node.properties.get(name).map(RetainedNodeField::Value),
        })
    }

    /// Original identity storage. Signed logical identities preserve these
    /// bits when exposed through a native-endian signed 64-bit buffer.
    pub fn identity_storage(&self) -> Result<&u64> {
        Ok(&self.source.node.node()?.id.0)
    }

    /// Properties shadowed by the synthetic fields are deliberately omitted.
    pub fn properties(&self) -> Result<impl Iterator<Item = (&str, &Value)>> {
        Ok(self
            .source
            .node
            .node()?
            .properties
            .iter()
            .filter(|(name, _)| name.as_str() != "_id" && name.as_str() != "labels")
            .map(|(name, value)| (name.as_str(), value)))
    }

    pub fn try_retain(&self) -> Result<Self> {
        self.try_retain_with_metadata(0)
    }

    /// Admit adapter-owned descriptors before allocating them. Payload remains
    /// shared with the original row; the caller owns any extra allocation.
    pub fn try_retain_with_metadata(&self, adapter_bytes: usize) -> Result<Self> {
        self.source.node.node()?;
        let metadata = std::mem::size_of::<Self>()
            .checked_add(self.source.adapter_metadata_bytes)
            .and_then(|bytes| bytes.checked_add(adapter_bytes))
            .ok_or(RetainedQueryError::SizeOverflow)?;
        let runtime = self.runtime.try_retain(metadata as u64)?;
        let memory = self
            .source
            .account
            .reserve(runtime.handle_bytes() as usize)?;
        Ok(Self {
            source: Arc::clone(&self.source),
            _view_memory: Some(memory),
            runtime,
        })
    }
}

struct NodeObservation {
    recorder: Option<super::super::observability::StatementRecorder>,
    query: String,
    statement_kind: &'static str,
    started: hawdb_core::time::Instant,
    parse_nanos: u64,
    // Release query text/recorder ownership before their capacity reservations.
    _memory: QueryMemoryLease,
    _runtime_memory: RuntimeRetainedMemory,
}
impl std::fmt::Debug for NodeObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeObservation")
            .field("statement_kind", &self.statement_kind)
            .finish_non_exhaustive()
    }
}
impl NodeObservation {
    fn record(
        self,
        result: std::result::Result<usize, &dyn std::fmt::Display>,
        profile: &RetainedQueryProfile,
    ) {
        let Some(recorder) = self.recorder else {
            return;
        };
        recorder.record_statement_row_count(
            "cypher",
            &self.query,
            self.statement_kind,
            self.started,
            result,
            super::super::StatementExecutionContext {
                parse_nanos: self.parse_nanos,
                retained_profile: result.is_ok().then_some(profile),
                ..Default::default()
            },
        );
    }
}

/// One-row active pull, provisional until successful EOF. Multi-match seeks
/// refuse explicitly rather than silently truncating the logical query.
#[derive(Debug)]
pub struct RetainedNodeCursor {
    observation: Option<NodeObservation>,
    view: Option<RetainedNodeView>,
    health: Option<MaterializedReadHealth>,
    delivered: bool,
    status: RetainedQueryStatus,
    profile: RetainedQueryProfile,
    terminal_error: Option<RetainedQueryError>,
    payload_bytes: usize,
    // Foreign cursor wrappers may outlive observation/source completion.
    _adapter_control: Option<(QueryMemoryLease, RuntimeRetainedMemory)>,
}
/// Task-bound active pull. The context is borrowed, so this cursor neither
/// clones the cancellation ancestry nor retains a host I/O controller.
/// Exported views own their source and admission independently of this borrow.
#[derive(Debug)]
pub struct RetainedNodeTaskCursor<'a> {
    cursor: RetainedNodeCursor,
    context: &'a RuntimeTaskContext,
}
impl RetainedNodeTaskCursor<'_> {
    pub fn profile(&self) -> RetainedQueryProfile {
        self.cursor.profile()
    }
    pub fn status(&self) -> RetainedQueryStatus {
        self.cursor.status()
    }
    pub fn access_path(&self) -> &'static str {
        self.cursor.access_path()
    }
    pub fn next_node(&mut self) -> Result<Option<RetainedNodeView>> {
        self.cursor.next_node_inner(Some(self.context))
    }
    pub fn close(&mut self) {
        self.cursor.close();
    }
}

impl RetainedNodeCursor {
    pub fn profile(&self) -> RetainedQueryProfile {
        self.profile
    }
    pub fn status(&self) -> RetainedQueryStatus {
        self.status
    }
    pub fn access_path(&self) -> &'static str {
        "retained_indexed_node_view"
    }
    pub fn next_node(&mut self) -> Result<Option<RetainedNodeView>> {
        self.next_node_inner(None)
    }
    fn next_node_inner(
        &mut self,
        context: Option<&RuntimeTaskContext>,
    ) -> Result<Option<RetainedNodeView>> {
        if self.status == RetainedQueryStatus::Closed {
            return Err(RetainedQueryError::Closed);
        }
        if self.status == RetainedQueryStatus::Failed {
            return Err(self
                .terminal_error
                .clone()
                .expect("failed cursor has a cause"));
        }
        self.profile.pulls += 1;
        if self.status == RetainedQueryStatus::Completed {
            return Ok(None);
        }
        if let Some(context) = context
            && let Err(reason) = context.checkpoint()
        {
            return Err(self.fail(RetainedQueryError::Stopped(reason)));
        }
        if let Err(error) = self
            .health
            .as_ref()
            .expect("open health barrier")
            .validate_usable()
        {
            return Err(self.fail(error.into()));
        }
        if self.delivered || self.view.is_none() {
            let observation = self.observation.take();
            let completion = NodeCompletionGuard {
                cursor: self,
                armed: true,
            };
            if let Some(observation) = observation {
                observation.record(
                    Ok(completion.cursor.profile.emitted_rows),
                    &completion.cursor.profile,
                );
            }
            completion.complete();
            return Ok(None);
        }
        let result = self.view.as_ref().expect("source checked").try_retain();
        match result {
            Ok(view) => {
                self.delivered = true;
                self.profile.visited_rows = 1;
                self.profile.predicate_selected_rows = 1;
                self.profile.emitted_rows = 1;
                self.profile.peak_outstanding_batches = 1;
                self.profile.output_payload_bytes = self.payload_bytes;
                self.profile.query_peak_bytes = view.source.ledger.snapshot().peak_bytes;
                Ok(Some(view))
            }
            Err(error) if error.is_retryable() => {
                self.profile.backpressure_events += 1;
                Err(error)
            }
            Err(error) => Err(self.fail(error)),
        }
    }

    fn fail(&mut self, error: RetainedQueryError) -> RetainedQueryError {
        self.status = RetainedQueryStatus::Failed;
        self.terminal_error = Some(error.clone());
        if let Some(view) = self.view.take() {
            view.source.status.store(2, Ordering::Release);
        }
        self.health.take();
        if let Some(observation) = self.observation.take() {
            observation.record(Err(&error), &self.profile);
        }
        error
    }
    pub fn close(&mut self) {
        if self.status == RetainedQueryStatus::Open {
            if let Some(view) = self.view.take() {
                view.source.status.store(3, Ordering::Release);
            }
            self.status = RetainedQueryStatus::Closed;
            self.health.take();
            if let Some(observation) = self.observation.take() {
                observation.record(Err(&RetainedQueryError::Closed), &self.profile);
            }
        }
    }

    /// A failed foreign handoff must never complete provisional views at EOF.
    pub fn abort_delivery(&mut self) {
        if self.status == RetainedQueryStatus::Open {
            self.fail(RetainedQueryError::AdapterDelivery);
        }
    }
}
impl Drop for RetainedNodeCursor {
    fn drop(&mut self) {
        self.close();
    }
}

// Successful EOF is published only after observation callbacks return. A sink
// panic preserves unwinding but never leaves exported provisional views complete.
struct NodeCompletionGuard<'a> {
    cursor: &'a mut RetainedNodeCursor,
    armed: bool,
}
impl NodeCompletionGuard<'_> {
    fn complete(mut self) {
        self.armed = false;
        self.cursor.health.take();
        self.cursor.status = RetainedQueryStatus::Completed;
        if let Some(view) = self.cursor.view.take() {
            view.source.status.store(1, Ordering::Release);
        }
    }
}
impl Drop for NodeCompletionGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.cursor.status = RetainedQueryStatus::Failed;
            self.cursor.terminal_error = Some(RetainedQueryError::AdapterDelivery);
            self.cursor.health.take();
            if let Some(view) = self.cursor.view.take() {
                view.source.status.store(2, Ordering::Release);
            }
        }
    }
}

struct PointPlan<'a> {
    label: &'a str,
    property: &'a str,
    value: &'a Value,
    column: &'a str,
    empty: bool,
}
fn point_plan(plan: &PhysicalPlan) -> Option<PointPlan<'_>> {
    let (plan, empty) = match plan {
        PhysicalPlan::LimitExec {
            offset: 0,
            limit,
            input,
        } => (input.as_ref(), *limit == Some(0)),
        plan => (plan, false),
    };
    let PhysicalPlan::ProjectExec { items, input } = plan else {
        return None;
    };
    let [item] = items.as_slice() else {
        return None;
    };
    let ProjectionExpression::Variable { variable: returned } = &item.expression else {
        return None;
    };
    let (leaf, filter) = match input.as_ref() {
        PhysicalPlan::FilterExec { predicate, input } => (input.as_ref(), Some(predicate)),
        plan => (plan, None),
    };
    let PhysicalPlan::IndexNodeSeek {
        variable,
        label,
        property,
        value,
    } = leaf
    else {
        return None;
    };
    if variable != returned || matches!(value, Value::Null) {
        return None;
    }
    if let Some(predicate) = filter
        && !matches!(predicate, Predicate::PropertyEq { variable: v, property: p, value: x }
            if v == variable && p == property && x == value)
    {
        return None;
    }
    Some(PointPlan {
        label,
        property,
        value,
        column: &item.name,
        empty,
    })
}

impl Database {
    /// Experimental native full-node view for a single-match indexed Cypher
    /// projection. Other plans/layouts refuse; no Arrow or copied fallback.
    /// Planning and foreign workspace are not yet whole-operation qualified.
    pub fn query_with_params_retained_node(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: RetainedQueryOptions,
    ) -> Result<RetainedNodeCursor> {
        self.query_with_params_retained_node_inner(cypher, parameters, options, None)
    }

    /// Binds every open pull, including empty/post-delivery EOF, to this task.
    /// Existing task reservations cap retained query/result admission. Views
    /// can outlive the borrowed context through their independent admissions.
    pub fn query_with_params_retained_node_context<'a>(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: RetainedQueryOptions,
        context: &'a RuntimeTaskContext,
    ) -> Result<RetainedNodeTaskCursor<'a>> {
        let cursor =
            self.query_with_params_retained_node_inner(cypher, parameters, options, Some(context))?;
        Ok(RetainedNodeTaskCursor { cursor, context })
    }

    fn query_with_params_retained_node_inner(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: RetainedQueryOptions,
        context: Option<&RuntimeTaskContext>,
    ) -> Result<RetainedNodeCursor> {
        if let Some(context) = context {
            context.checkpoint().map_err(RetainedQueryError::Stopped)?;
        }
        let execution_started = hawdb_core::time::Instant::now();
        let state = self.runtime.get()?;
        state.store.ensure_usable()?;
        let prepared = query_runtime::parse_runtime_execution(cypher)?;
        let started = prepared.statement_started.unwrap_or(execution_started);
        let parse_nanos = prepared.parse_metrics.elapsed_nanos;
        let statement_kind = super::super::statement_kind(&prepared.statement);
        let result = (|| {
            if let Some(context) = context {
                context.checkpoint().map_err(RetainedQueryError::Stopped)?;
            }
            super::super::query_work_request_for_statement(
                &self.system_variables,
                &prepared.statement,
            )?;
            let optimized = self.optimized_query_plan_for_request(
                cypher,
                &prepared.statement,
                parameters,
                None,
                super::super::PlanTraceMode::Template,
                None,
            )?;
            let plan =
                point_plan(&optimized.physical_plan).ok_or(RetainedQueryError::UnsupportedPlan)?;
            RetainedNodeCursor::from_point_plan(
                plan,
                NodeCursorConstruction {
                    store: &state.store,
                    catalog: &state.catalog,
                    config: &self.config,
                    runtime: &self.retained_runtime,
                    options,
                    context,
                    cursor_metadata_bytes: 0,
                    observation: NodeObservationRequest {
                        recorder: Some(self.statement_recorder()),
                        cypher,
                        statement_kind,
                        started,
                        parse_nanos,
                    },
                },
            )
        })();
        if let Err(error) = &result {
            self.record_statement_row_count(
                "cypher",
                cypher,
                statement_kind,
                started,
                Err(error),
                super::super::StatementExecutionContext {
                    parse_nanos,
                    ..Default::default()
                },
            );
        }
        result
    }
}

/// A consumed read snapshot's single-match node cursor. Every open pull uses
/// the snapshot's owned task context; views retain only their immutable source
/// row and catalog symbols, not the read transaction or its reader pin.
#[derive(Debug)]
pub struct RetainedNodeSnapshotCursor {
    cursor: RetainedNodeCursor,
    context: Option<RuntimeTaskContext>,
}

impl RetainedNodeSnapshotCursor {
    pub fn profile(&self) -> RetainedQueryProfile {
        self.cursor.profile()
    }
    pub fn status(&self) -> RetainedQueryStatus {
        self.cursor.status()
    }
    pub fn access_path(&self) -> &'static str {
        self.cursor.access_path()
    }
    pub fn next_node(&mut self) -> Result<Option<RetainedNodeView>> {
        let result = self.cursor.next_node_inner(self.context.as_ref());
        if self.cursor.status() != RetainedQueryStatus::Open {
            self.context = None;
        }
        result
    }
    pub fn close(&mut self) {
        self.cursor.close();
        self.context = None;
    }
}

impl DatabaseReadTransaction {
    /// Consumes this snapshot for a single-match indexed full-node projection.
    /// Planning uses this snapshot's schema/indexes. Other plans and layouts
    /// refuse explicitly, and the snapshot's task remains bound through EOF.
    pub fn into_retained_node_query(
        self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: RetainedQueryOptions,
    ) -> Result<RetainedNodeSnapshotCursor> {
        let context = self.task_context.clone();
        if let Some(context) = context.as_ref() {
            context.checkpoint().map_err(RetainedQueryError::Stopped)?;
        }
        self.store.ensure_usable()?;
        let execution_started = hawdb_core::time::Instant::now();
        let prepared = query_runtime::parse_runtime_execution(cypher)?;
        super::super::query_work_request_for_statement(
            &QuerySystemVariables::default(),
            &prepared.statement,
        )?;
        let optimized = self.optimized_query_plan_for_request(
            cypher,
            &prepared.statement,
            parameters,
            None,
            super::super::PlanTraceMode::Template,
            None,
        )?;
        let plan =
            point_plan(&optimized.physical_plan).ok_or(RetainedQueryError::UnsupportedPlan)?;
        let cursor = RetainedNodeCursor::from_point_plan(
            plan,
            NodeCursorConstruction {
                store: &self.store,
                catalog: &self.catalog,
                config: &self.config,
                runtime: &self.retained_runtime,
                options,
                context: context.as_ref(),
                cursor_metadata_bytes: std::mem::size_of::<RetainedNodeSnapshotCursor>()
                    - std::mem::size_of::<RetainedNodeCursor>(),
                observation: NodeObservationRequest {
                    // Snapshot reads do not publish into the live database's
                    // statement recorder, matching ordinary snapshot queries.
                    recorder: None,
                    cypher,
                    statement_kind: super::super::statement_kind(&prepared.statement),
                    started: prepared.statement_started.unwrap_or(execution_started),
                    parse_nanos: prepared.parse_metrics.elapsed_nanos,
                },
            },
        )?;
        Ok(RetainedNodeSnapshotCursor { cursor, context })
    }
}

struct NodeObservationRequest<'a> {
    recorder: Option<super::super::observability::StatementRecorder>,
    cypher: &'a str,
    statement_kind: &'static str,
    started: hawdb_core::time::Instant,
    parse_nanos: u64,
}

struct NodeCursorConstruction<'a> {
    store: &'a GraphStore,
    catalog: &'a Catalog,
    config: &'a DatabaseConfig,
    runtime: &'a RetainedRuntime,
    options: RetainedQueryOptions,
    context: Option<&'a RuntimeTaskContext>,
    cursor_metadata_bytes: usize,
    observation: NodeObservationRequest<'a>,
}

impl RetainedNodeCursor {
    fn from_point_plan(
        plan: PointPlan<'_>,
        construction: NodeCursorConstruction<'_>,
    ) -> Result<Self> {
        let NodeCursorConstruction {
            store,
            catalog,
            config,
            runtime,
            options,
            context,
            cursor_metadata_bytes,
            observation,
        } = construction;
        let NodeObservationRequest {
            recorder,
            cypher,
            statement_kind,
            started,
            parse_nanos,
        } = observation;
        if store
            .try_scan_materialized_nodes_after(None, None)?
            .is_none()
        {
            return Err(RetainedQueryError::CopyRequired);
        }
        if let Some(context) = context {
            context.checkpoint().map_err(RetainedQueryError::Stopped)?;
        }
        let query_budget = hawdb_executor::memory::enforced_query_memory_budget(
            &config.execution_memory,
            context,
        )?
        .min(config.execution_memory.query_memory_bytes);
        let result_budget = hawdb_executor::memory::enforced_result_memory_budget(
            &config.execution_memory,
            context,
        )?;
        let ledger = QueryMemoryLedger::new(query_budget);
        let account = ledger.account(
            QueryMemoryClass::ResultMaterialization,
            "retained_node",
            result_budget.min(query_budget),
        );
        let observation_bytes = std::mem::size_of::<RetainedNodeCursor>()
            .checked_add(cursor_metadata_bytes)
            .and_then(|bytes| bytes.checked_add(cypher.len()))
            .and_then(|bytes| bytes.checked_add(MaterializedReadHealth::retained_capacity_bytes()))
            .and_then(|bytes| bytes.checked_add(options.adapter_metadata_bytes))
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or(RetainedQueryError::SizeOverflow)?;
        let adapter_control_bytes = if options.adapter_metadata_bytes == 0 {
            0
        } else {
            observation_bytes
        };
        let control_bytes = observation_bytes
            .checked_add(adapter_control_bytes)
            .ok_or(RetainedQueryError::SizeOverflow)?;
        let working_limit = options
            .batch_bytes
            .get()
            .min(config.execution_memory.batch_payload_bytes.get());
        if control_bytes > working_limit {
            return Err(RetainedQueryError::WorkingUnitTooLarge);
        }
        let governor = runtime.governor(options.minimum_shared_handles.get())?;
        let permit = governor.try_admit(
            RuntimeWorkRequest::foreground_query(control_bytes as u64, 0).with_blocking(false),
        )?;
        let memory = account.reserve(observation_bytes)?;
        let runtime_memory = permit.reserve_retained_memory(observation_bytes as u64)?;
        let adapter_control = if adapter_control_bytes == 0 {
            None
        } else {
            Some((
                account.reserve(adapter_control_bytes)?,
                permit.reserve_retained_memory(adapter_control_bytes as u64)?,
            ))
        };
        drop(permit);
        let observation = NodeObservation {
            recorder,
            query: cypher.to_owned(),
            statement_kind,
            started,
            parse_nanos,
            _memory: memory,
            _runtime_memory: runtime_memory,
        };
        let health = store.materialized_read_health()?;
        let mut profile = RetainedQueryProfile {
            source_snapshot_rows: store.node_count_for_label(None),
            query_peak_bytes: ledger.snapshot().peak_bytes,
            ..Default::default()
        };
        let view = if plan.empty {
            None
        } else {
            let label = catalog
                .label_id(plan.label)
                .ok_or(RetainedQueryError::UnsupportedPlan)?;
            let mut found = store.seek_nodes_by_property(label, plan.property, plan.value);
            match found.next() {
                None => None,
                Some(row) => {
                    if found.next().is_some() {
                        return Err(RetainedQueryError::UnsupportedPlan);
                    }
                    Some(admit_node(
                        store,
                        catalog,
                        row.id,
                        plan.column,
                        NodeAdmission {
                            config,
                            runtime,
                            options,
                            ledger: &ledger,
                            account: &account,
                            control_bytes,
                        },
                        &mut profile,
                    )?)
                }
            }
        };
        if let Some(context) = context {
            context.checkpoint().map_err(RetainedQueryError::Stopped)?;
        }
        let payload_bytes = profile.output_payload_bytes;
        profile.output_payload_bytes = 0;
        Ok(RetainedNodeCursor {
            observation: Some(observation),
            view,
            health: Some(health),
            delivered: false,
            status: RetainedQueryStatus::Open,
            profile,
            terminal_error: None,
            payload_bytes,
            _adapter_control: adapter_control,
        })
    }
}

struct NodeAdmission<'a> {
    config: &'a DatabaseConfig,
    runtime: &'a RetainedRuntime,
    options: RetainedQueryOptions,
    ledger: &'a QueryMemoryLedger,
    account: &'a QueryMemoryAccount,
    control_bytes: usize,
}

fn admit_node(
    store: &GraphStore,
    catalog: &Catalog,
    id: NodeId,
    column: &str,
    admission: NodeAdmission<'_>,
    profile: &mut RetainedQueryProfile,
) -> Result<RetainedNodeView> {
    let NodeAdmission {
        config,
        runtime,
        options,
        ledger,
        account,
        control_bytes,
    } = admission;
    let row = store.node(id).ok_or(RetainedQueryError::UnsupportedPlan)?;
    let symbol_bytes = row.labels.iter().try_fold(0usize, |total, id| {
        total
            .checked_add(
                catalog
                    .retained_label_name_capacity_bytes(*id)?
                    .unwrap_or(0),
            )
            .ok_or(RetainedQueryError::SizeOverflow)
    })?;
    let metadata = std::mem::size_of::<NodeOwner>()
        .checked_add(MaterializedReadHealth::retained_capacity_bytes())
        .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
        .and_then(|bytes| {
            bytes.checked_add(
                row.labels
                    .len()
                    .checked_mul(std::mem::size_of::<RetainedLabelName<PrepaidSource>>())?,
            )
        })
        .and_then(|bytes| bytes.checked_add(column.len()))
        .and_then(|bytes| bytes.checked_add(options.adapter_metadata_bytes))
        .ok_or(RetainedQueryError::SizeOverflow)?;
    let governor = runtime.governor(options.minimum_shared_handles.get())?;
    let allowance = governor.snapshot().limits.result_budget_bytes;
    let mut charges = None;
    let read = store.materialized_node_point_read_with_admission(id, |row_bytes| -> Result<_> {
        let capacity = row_bytes
            .checked_add(symbol_bytes)
            .and_then(|bytes| bytes.checked_add(metadata))
            .ok_or(RetainedQueryError::SizeOverflow)?;
        if capacity
            .checked_add(control_bytes)
            .ok_or(RetainedQueryError::SizeOverflow)?
            > options
                .batch_bytes
                .get()
                .min(config.execution_memory.batch_payload_bytes.get())
        {
            return Err(RetainedQueryError::WorkingUnitTooLarge);
        }
        let mut payload = column
            .len()
            .checked_add("_id".len() + 8 + "labels".len())
            .ok_or(RetainedQueryError::SizeOverflow)?;
        for (key, value) in &row.properties {
            if key != "_id" && key != "labels" {
                payload = payload
                    .checked_add(key.len())
                    .and_then(|bytes| {
                        bytes.checked_add(hawdb_executor::query_value_payload_bytes(value))
                    })
                    .ok_or(RetainedQueryError::SizeOverflow)?;
            }
        }
        for label in &row.labels {
            payload = payload
                .checked_add(catalog.label_name(*label).map_or(0, str::len))
                .ok_or(RetainedQueryError::SizeOverflow)?;
        }
        if restrictive(config.read_result_row_limit(), options.max_result_rows)
            .is_some_and(|limit| limit < 1)
            || restrictive(
                config.max_read_result_payload_bytes,
                options.max_result_payload_bytes,
            )
            .is_some_and(|limit| payload > limit)
        {
            return Err(RetainedQueryError::ResultBudget {
                rows: 1,
                payload_bytes: payload,
            });
        }
        let work_bytes =
            retained_result_work_bytes(capacity, std::mem::size_of::<RetainedNodeView>())?
                .min(allowance);
        let permit = governor
            .try_admit(RuntimeWorkRequest::foreground_query(0, work_bytes).with_blocking(false))?;
        let result = permit.reserve_retained_result(
            capacity as u64,
            std::mem::size_of::<RetainedNodeView>() as u64,
        )?;
        let memory = account.reserve(
            capacity
                .checked_add(RuntimeRetainedResult::owner_overhead_bytes() as usize)
                .and_then(|bytes| bytes.checked_add(result.handle_bytes() as usize))
                .ok_or(RetainedQueryError::SizeOverflow)?,
        )?;
        charges = Some((result, memory, permit));
        profile.source_pinned_capacity_bytes = row_bytes + symbol_bytes;
        profile.output_payload_bytes = payload;
        Ok(PrepaidSource(row_bytes))
    })?;
    let MaterializedNodePointRead::Node(node) = read else {
        return Err(RetainedQueryError::CopyRequired);
    };
    let mut labels = Vec::with_capacity(row.labels.len());
    for id in &row.labels {
        if let Some(label) = catalog
            .retain_label_name_with_admission(*id, |bytes| -> Result<_> {
                Ok(PrepaidSource(bytes))
            })?
        {
            labels.push(label);
        }
    }
    let (result, memory, _permit) = charges.expect("point source was admitted");
    profile.source_pinned_rows = 1;
    profile.source_preflight_rows = 1;
    profile.query_peak_bytes = ledger.snapshot().peak_bytes;
    Ok(RetainedNodeView {
        source: Arc::new(NodeOwner {
            node,
            labels,
            column: column.to_owned(),
            status: AtomicU8::new(0),
            account: account.clone(),
            ledger: ledger.clone(),
            adapter_metadata_bytes: options.adapter_metadata_bytes,
            _memory: memory,
        }),
        _view_memory: None,
        runtime: result,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUERY: &str = "MATCH (n:Item {score: $key}) RETURN n AS node";

    fn fixture(handles: usize) -> (Database, RuntimeGovernor) {
        let mut db = Database::new();
        db.query("CREATE NODE TABLE Item").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT")
            .unwrap();
        db.query("CREATE INDEX ON :Item(score)").unwrap();
        for key in 0..128 {
            db.query_with_params(
                "CREATE (:Item {score: $key, text: $text, _id: 'shadow', labels: 'shadow'})",
                &BTreeMap::from([
                    ("key".into(), Value::Int(key)),
                    ("text".into(), Value::String("payload".into())),
                ]),
            )
            .unwrap();
        }
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig {
                retained_result_handle_limit: NonZeroUsize::new(handles).unwrap(),
                ..RuntimeGovernorConfig::shared_host()
            },
            RuntimeResourceSnapshot::detect(),
            IoConcurrencyBudget::new(2, 1),
        );
        db.set_runtime_governor(governor.clone());
        (db, governor)
    }

    fn params() -> BTreeMap<String, Value> {
        BTreeMap::from([("key".into(), Value::Int(37))])
    }

    #[test]
    fn point_result_ceiling_does_not_reserve_unused_memory() {
        let (mut db, _) = fixture(128);
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(64 * 1024),
                result_budget_bytes: 64 * 1024,
                retained_result_handle_limit: NonZeroUsize::new(128).unwrap(),
                ..RuntimeGovernorConfig::shared_host()
            },
            RuntimeResourceSnapshot::from_parts(
                crate::RuntimeResourceBudget::from_limits(
                    NonZeroUsize::new(4).unwrap(),
                    None,
                    None,
                ),
                crate::RuntimeMemorySnapshot::from_limits(
                    Some(1 << 30),
                    Some(1 << 30),
                    None,
                    None,
                    None,
                ),
            ),
            IoConcurrencyBudget::new(2, 1),
        );
        db.set_runtime_governor(governor.clone());
        let other_work = governor
            .try_admit(RuntimeWorkRequest::foreground_query(32 * 1024, 0).with_blocking(false))
            .unwrap();
        let mut cursor = db
            .query_with_params_retained_node(QUERY, &params(), RetainedQueryOptions::default())
            .unwrap();
        let view = cursor.next_node().unwrap().unwrap();
        assert!(matches!(
            view.field("score").unwrap(),
            Some(RetainedNodeField::Value(Value::Int(37)))
        ));
        assert!(cursor.next_node().unwrap().is_none());
        assert_eq!(governor.snapshot().active_foreground_tasks, 1);
        drop((cursor, view));
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 32 * 1024);
        drop(other_work);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }

    #[test]
    fn consumed_snapshot_preserves_old_indexed_row_and_releases_the_reader_pin() {
        let (mut db, governor) = fixture(4);
        db.query_with_params(
            "CREATE (:Other {pad: $pad})",
            &BTreeMap::from([("pad".into(), Value::String("x".repeat(1024 * 1024)))]),
        )
        .unwrap();
        let pins = Arc::clone(&db.runtime.get().unwrap().reader_pins);
        let snapshot = db.begin_read_transaction().unwrap();
        let label = snapshot.catalog.label_id("Item").unwrap();
        let row = snapshot
            .store
            .seek_nodes_by_property(label, "score", &Value::Int(37))
            .next()
            .unwrap();
        let value_address = &row.properties["text"] as *const Value as usize;
        let label_address = snapshot.catalog.label_name(label).unwrap().as_ptr() as usize;
        assert_eq!(pins.lock().unwrap().active_views.len(), 1);
        db.query_with_params(
            "MATCH (n:Item {score: $key}) SET n.score = 999, n.text = 'changed'",
            &params(),
        )
        .unwrap();
        let mut cursor = snapshot
            .into_retained_node_query(QUERY, &params(), RetainedQueryOptions::default())
            .unwrap();
        assert_eq!(pins.lock().unwrap().active_views.len(), 0);
        assert!(cursor.profile().source_pinned_capacity_bytes < 1024 * 1024);
        let view = cursor.next_node().unwrap().unwrap();
        let Some(RetainedNodeField::Value(value)) = view.field("text").unwrap() else {
            panic!("original snapshot property");
        };
        assert_eq!(value as *const Value as usize, value_address);
        assert_eq!(value, &Value::String("payload".into()));
        let Some(RetainedNodeField::Labels(labels)) = view.field("labels").unwrap() else {
            panic!("snapshot catalog symbols");
        };
        assert_eq!(
            labels.iter().next().unwrap().as_ptr() as usize,
            label_address
        );
        assert!(cursor.next_node().unwrap().is_none());
        assert_eq!(view.status(), RetainedQueryStatus::Completed);
        drop((cursor, db));
        assert!(matches!(view.field("text").unwrap(),
            Some(RetainedNodeField::Value(Value::String(text))) if text == "payload"));
        assert!(governor.retained_result_snapshot().retained_bytes > 0);
        drop(view);
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
        assert_eq!(governor.retained_result_snapshot().view_handles, 0);
    }

    #[test]
    fn consumed_snapshot_keeps_task_cancellation_bound_through_empty_and_delivered_eof() {
        for (key, deliver) in [(37, false), (37, true), (999, false)] {
            let (db, governor) = fixture(4);
            let token = hawdb_core::RuntimeCancellationToken::new();
            let context = RuntimeTaskContext::without_deadline(token.clone());
            let snapshot = db.begin_read_transaction_with_context(&context).unwrap();
            drop(context);
            let mut cursor = snapshot
                .into_retained_node_query(
                    QUERY,
                    &BTreeMap::from([("key".into(), Value::Int(key))]),
                    RetainedQueryOptions::default(),
                )
                .unwrap();
            let view = deliver.then(|| cursor.next_node().unwrap().unwrap());
            token.cancel();
            let expected = RetainedQueryError::Stopped(RuntimeCancellationReason::Cancelled);
            assert_eq!(cursor.next_node().unwrap_err(), expected);
            assert_eq!(cursor.next_node().unwrap_err(), expected);
            assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
            if let Some(view) = view.as_ref() {
                assert_eq!(view.status(), RetainedQueryStatus::Failed);
                assert!(view.field("text").unwrap().is_some());
            }
            drop((cursor, view, db));
            assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
            assert_eq!(governor.retained_result_snapshot().view_handles, 0);
        }
    }

    #[test]
    fn cancelled_consumed_snapshot_refuses_before_retained_admission_and_releases_its_pin() {
        let (db, governor) = fixture(4);
        let pins = Arc::clone(&db.runtime.get().unwrap().reader_pins);
        let context = RuntimeTaskContext::default();
        let snapshot = db.begin_read_transaction_with_context(&context).unwrap();
        assert_eq!(pins.lock().unwrap().active_views.len(), 1);
        context.cancellation().cancel();
        assert_eq!(
            snapshot
                .into_retained_node_query(QUERY, &params(), RetainedQueryOptions::default())
                .unwrap_err(),
            RetainedQueryError::Stopped(RuntimeCancellationReason::Cancelled)
        );
        assert!(db.retained_result_snapshot().is_none());
        assert_eq!(pins.lock().unwrap().active_views.len(), 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }

    #[test]
    fn consumed_snapshot_never_widens_task_reservations_and_completed_eof_is_stable() {
        for (query_bytes, result_bytes) in [(0, 8192), (8192, 0), (1, 8192), (8192, 1)] {
            let (db, governor) = fixture(4);
            let context = RuntimeTaskContext::default().with_memory_reservation(
                hawdb_core::RuntimeMemoryReservation::new(query_bytes, result_bytes),
            );
            let snapshot = db.begin_read_transaction_with_context(&context).unwrap();
            assert!(matches!(
                snapshot.into_retained_node_query(
                    QUERY,
                    &params(),
                    RetainedQueryOptions::default(),
                ),
                Err(RetainedQueryError::Execution(HawDBError::Execution(_)))
            ));
            assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
        }
        let (db, governor) = fixture(4);
        let context = RuntimeTaskContext::default()
            .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(8192, 8192));
        let snapshot = db.begin_read_transaction_with_context(&context).unwrap();
        let mut cursor = snapshot
            .into_retained_node_query(QUERY, &params(), RetainedQueryOptions::default())
            .unwrap();
        let view = cursor.next_node().unwrap().unwrap();
        assert!(cursor.profile().query_peak_bytes <= 8192);
        assert!(cursor.next_node().unwrap().is_none());
        context.cancellation().cancel();
        assert!(cursor.next_node().unwrap().is_none());
        assert_eq!(view.status(), RetainedQueryStatus::Completed);
        drop((cursor, view, db));
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    }

    #[test]
    fn consumed_snapshot_uses_its_own_schema_when_live_database_adds_an_index() {
        let mut db = Database::new();
        // A different property has no declared index in the old snapshot.
        db.query("CREATE NODE TABLE Plain").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Plain(key) TYPE INT")
            .unwrap();
        db.query_with_params(
            "UNWIND $rows AS row CREATE (:Plain {key: row.key})",
            &BTreeMap::from([(
                "rows".into(),
                Value::List(
                    (0..17)
                        .map(|key| Value::Map(BTreeMap::from([("key".into(), Value::Int(key))])))
                        .collect(),
                ),
            )]),
        )
        .unwrap();
        let snapshot = db.begin_read_transaction().unwrap();
        db.query("CREATE INDEX ON :Plain(key)").unwrap();
        let query = "MATCH (n:Plain {key: $key}) RETURN n AS node";
        let parameters = BTreeMap::from([("key".into(), Value::Int(7))]);
        assert!(matches!(
            snapshot.into_retained_node_query(query, &parameters, RetainedQueryOptions::default(),),
            Err(RetainedQueryError::UnsupportedPlan)
        ));
        let mut current = db
            .begin_read_transaction()
            .unwrap()
            .into_retained_node_query(query, &parameters, RetainedQueryOptions::default())
            .unwrap();
        assert!(current.next_node().unwrap().is_some());
        assert!(current.next_node().unwrap().is_none());
    }

    #[test]
    fn indexed_full_node_reuses_original_payload_and_preserves_shadowing() {
        let (mut db, governor) = fixture(4);
        let state = db.runtime.get().unwrap();
        let label = state.catalog.label_id("Item").unwrap();
        let row = state
            .store
            .seek_nodes_by_property(label, "score", &Value::Int(37))
            .next()
            .unwrap();
        let id = row.id.0 as i64;
        let value_address = &row.properties["text"] as *const Value as usize;
        let label_address = state.catalog.label_name(label).unwrap().as_ptr() as usize;
        drop(state);
        let prepared = query_runtime::parse_runtime_execution(QUERY).unwrap();
        let optimized = db
            .optimized_query_plan_for_request(
                QUERY,
                &prepared.statement,
                &params(),
                None,
                super::super::super::PlanTraceMode::Template,
                None,
            )
            .unwrap();
        assert!(
            point_plan(&optimized.physical_plan).is_some(),
            "{:?}",
            optimized.physical_plan
        );
        let ordinary = db.query_with_params(QUERY, &params()).unwrap();
        let Some(Value::Map(expected)) = ordinary.rows[0].get("node") else {
            panic!("ordinary node map");
        };
        let mut cursor = db
            .query_with_params_retained_node(QUERY, &params(), RetainedQueryOptions::default())
            .unwrap();
        assert_eq!(cursor.profile().output_payload_bytes, 0);
        let view = cursor.next_node().unwrap().unwrap();
        assert_eq!(cursor.profile().peak_outstanding_batches, 1);
        assert_eq!(view.column_name(), "node");
        let Some(RetainedNodeField::Value(value)) = view.field("text").unwrap() else {
            panic!("borrowed text");
        };
        assert_eq!(value as *const Value as usize, value_address);
        assert_eq!(value, &expected["text"]);
        assert!(
            matches!(view.field("_id").unwrap(), Some(RetainedNodeField::Identity(found)) if found == id)
        );
        let Some(RetainedNodeField::Labels(labels)) = view.field("labels").unwrap() else {
            panic!("labels");
        };
        assert_eq!(labels.iter().collect::<Vec<_>>(), ["Item"]);
        assert_eq!(
            labels.iter().next().unwrap().as_ptr() as usize,
            label_address
        );
        assert!(view
            .properties()
            .unwrap()
            .all(|(key, _)| key != "_id" && key != "labels"));
        assert!(cursor.next_node().unwrap().is_none());
        assert_eq!(view.status(), RetainedQueryStatus::Completed);
        let second = view.try_retain().unwrap();
        db.query_with_params(
            "MATCH (n:Item {score: $key}) SET n.text = 'changed'",
            &params(),
        )
        .unwrap();
        drop(db);
        assert!(
            matches!(second.field("text").unwrap(), Some(RetainedNodeField::Value(Value::String(text))) if text == "payload")
        );
        drop((cursor, view));
        assert!(governor.retained_result_snapshot().retained_bytes > 0);
        drop(second);
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
        assert_eq!(governor.retained_result_snapshot().view_handles, 0);
    }

    #[test]
    fn poisoned_source_cannot_complete_a_delivered_or_empty_node_cursor() {
        for key in [37, 999] {
            let (db, governor) = fixture(4);
            let mut cursor = db
                .query_with_params_retained_node(
                    QUERY,
                    &BTreeMap::from([("key".into(), Value::Int(key))]),
                    RetainedQueryOptions::default(),
                )
                .unwrap();
            let delivered = if key == 37 {
                Some(cursor.next_node().unwrap().unwrap())
            } else {
                None
            };
            let payload = delivered.as_ref().map(|view| {
                let Some(RetainedNodeField::Value(Value::String(value))) =
                    view.field("text").unwrap()
                else {
                    panic!("expected original text");
                };
                value.as_bytes()
            });
            db.runtime
                .get()
                .unwrap()
                .store
                .poison_on_storage_error::<()>(&Err(HawDBError::StorageIntegrity(
                    "injected completion failure".into(),
                )));
            let expected = RetainedQueryError::Execution(
                db.runtime.get().unwrap().store.ensure_usable().unwrap_err(),
            );
            for _ in 0..2 {
                assert_eq!(cursor.next_node().unwrap_err(), expected);
                assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
            }
            if let Some(view) = delivered.as_ref() {
                assert_eq!(view.status(), RetainedQueryStatus::Failed);
                assert_eq!(payload.unwrap(), b"payload");
            }
            let digest = hawdb_query::QueryIdentity::new("cypher", QUERY);
            let summary = db
                .statement_summary
                .borrow()
                .snapshot()
                .into_iter()
                .find(|entry| entry.digest == digest.query_digest())
                .unwrap();
            assert_eq!(summary.execution_count, 1);
            assert_eq!(summary.error_count, 1);
            assert_eq!(summary.success_count, 0);
            drop((cursor, delivered, db));
            assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        }
    }

    #[test]
    fn node_view_backpressure_does_not_revoke_source_after_cursor_close() {
        let (db, governor) = fixture(2);
        let mut cursor = db
            .query_with_params_retained_node(QUERY, &params(), RetainedQueryOptions::default())
            .unwrap();
        let view = cursor.next_node().unwrap().unwrap();
        let before = governor.retained_result_snapshot().retained_bytes;
        assert!(view.try_retain().unwrap_err().is_retryable());
        assert_eq!(governor.retained_result_snapshot().retained_bytes, before);
        cursor.close();
        assert_eq!(view.status(), RetainedQueryStatus::Closed);
        let second = view.try_retain().unwrap();
        drop((db, cursor, view));
        assert!(second.field("text").unwrap().is_some());
        drop(second);
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    }

    #[test]
    fn full_node_budget_refusal_rolls_back_and_multi_match_never_truncates() {
        let (mut db, governor) = fixture(4);
        let options = RetainedQueryOptions {
            max_result_payload_bytes: Some(0),
            ..Default::default()
        };
        assert!(matches!(
            db.query_with_params_retained_node(QUERY, &params(), options),
            Err(RetainedQueryError::ResultBudget { rows: 1, .. })
        ));
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
        db.query("CREATE (:Item {score: 37})").unwrap();
        assert!(matches!(
            db.query_with_params_retained_node(QUERY, &params(), RetainedQueryOptions::default()),
            Err(RetainedQueryError::UnsupportedPlan)
        ));
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    }

    #[test]
    fn shadowed_source_capacity_is_charged_and_missing_seek_skips_capture() {
        let (mut db, governor) = fixture(4);
        db.query_with_params(
            "MATCH (n:Item {score: $key}) SET n.labels = $large",
            &BTreeMap::from([
                ("key".into(), Value::Int(37)),
                ("large".into(), Value::String("x".repeat(70 * 1024))),
            ]),
        )
        .unwrap();
        let options = RetainedQueryOptions {
            batch_bytes: NonZeroUsize::new(64 * 1024).unwrap(),
            max_result_payload_bytes: Some(1024),
            ..Default::default()
        };
        assert!(matches!(
            db.query_with_params_retained_node(QUERY, &params(), options),
            Err(RetainedQueryError::WorkingUnitTooLarge)
        ));
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
        let mut missing = db
            .query_with_params_retained_node(
                QUERY,
                &BTreeMap::from([("key".into(), Value::Int(999))]),
                RetainedQueryOptions::default(),
            )
            .unwrap();
        assert!(missing.next_node().unwrap().is_none());
        assert_eq!(missing.profile().source_pinned_rows, 0);
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    }
    #[derive(Debug, Clone)]
    struct ObservedQuery {
        digest: String,
        success: bool,
        rows: usize,
        parse_nanos: u64,
        payload_bytes: usize,
    }
    #[derive(Debug, Default)]
    struct QuerySink(std::sync::Mutex<Vec<ObservedQuery>>);
    impl crate::telemetry::TelemetrySink for QuerySink {
        fn record_query(&self, event: crate::telemetry::QueryTelemetry<'_>) {
            self.0.lock().unwrap().push(ObservedQuery {
                digest: event.query_digest.to_owned(),
                success: event.success,
                rows: event.row_count,
                parse_nanos: event.parse_nanos,
                payload_bytes: event.output_payload_bytes,
            });
        }
    }

    #[test]
    fn node_observation_records_terminal_outcomes_once_without_materializing() {
        let (mut db, governor) = fixture(2);
        db.config.slow_query_log_threshold_micros = 0;
        let sink = Arc::new(QuerySink::default());
        db.set_telemetry_sink(Some(sink.clone()));
        db.query_with_params(QUERY, &params()).unwrap();
        assert_eq!(sink.0.lock().unwrap().len(), 1);
        let mut cursor = db
            .query_with_params_retained_node(QUERY, &params(), RetainedQueryOptions::default())
            .unwrap();
        let parse_nanos = cursor.observation.as_ref().unwrap().parse_nanos;
        assert_eq!(sink.0.lock().unwrap().len(), 1);
        let view = cursor.next_node().unwrap().unwrap();
        assert_eq!(sink.0.lock().unwrap().len(), 1);
        assert!(view.try_retain().unwrap_err().is_retryable());
        assert_eq!(sink.0.lock().unwrap().len(), 1);
        assert!(cursor.next_node().unwrap().is_none());
        assert!(cursor.next_node().unwrap().is_none());
        cursor.close();
        assert_eq!(sink.0.lock().unwrap().len(), 2);
        assert!(view.field("text").unwrap().is_some());
        drop(view);
        let mut missing = db
            .query_with_params_retained_node(
                QUERY,
                &BTreeMap::from([("key".into(), Value::Int(999))]),
                RetainedQueryOptions::default(),
            )
            .unwrap();
        assert!(missing.next_node().unwrap().is_none());
        let options = RetainedQueryOptions {
            max_result_payload_bytes: Some(0),
            ..Default::default()
        };
        assert!(matches!(
            db.query_with_params_retained_node(QUERY, &params(), options),
            Err(RetainedQueryError::ResultBudget { .. })
        ));
        let mut closed = db
            .query_with_params_retained_node(QUERY, &params(), RetainedQueryOptions::default())
            .unwrap();
        closed.close();
        closed.close();
        drop((cursor, missing, closed));
        let events = sink.0.lock().unwrap().clone();
        assert_eq!(events.len(), 5);
        assert_eq!(
            events
                .iter()
                .map(|event| (event.success, event.rows))
                .collect::<Vec<_>>(),
            [(true, 1), (true, 1), (true, 0), (false, 0), (false, 0)]
        );
        assert_eq!(events[1].parse_nanos, parse_nanos);
        assert!(events[1].payload_bytes > 0);
        assert_eq!(events[1].payload_bytes, events[0].payload_bytes);
        assert!(events[2..].iter().all(|event| event.payload_bytes == 0));
        let digest = hawdb_query::QueryIdentity::new("cypher", QUERY);
        assert!(events
            .iter()
            .all(|event| event.digest == digest.query_digest()));
        let summary = db
            .statement_summary
            .borrow()
            .snapshot()
            .into_iter()
            .find(|summary| summary.digest == digest.query_digest())
            .unwrap();
        assert_eq!(summary.execution_count, 5);
        assert_eq!(summary.success_count, 3);
        assert_eq!(summary.error_count, 2);
        assert_eq!(summary.total_row_count, 2);
        assert_eq!(
            db.slow_query_log_snapshot()
                .iter()
                .filter(|record| record.query_digest == digest.query_digest())
                .count(),
            3
        );
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }

    #[test]
    fn empty_cursor_control_is_admitted_and_records_after_database_drop() {
        let (db, governor) = fixture(2);
        let summary = Arc::clone(&db.statement_summary);
        let mut cursor = db
            .query_with_params_retained_node(
                QUERY,
                &BTreeMap::from([("key".into(), Value::Int(999))]),
                RetainedQueryOptions::default(),
            )
            .unwrap();
        assert_eq!(cursor.profile().source_pinned_rows, 0);
        assert!(cursor.profile().query_peak_bytes > 0);
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
        assert!(governor.snapshot().admitted_memory_bytes > 0);
        assert_eq!(governor.snapshot().active_foreground_tasks, 0);
        drop(db);
        assert!(cursor.next_node().unwrap().is_none());
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        let digest = hawdb_query::QueryIdentity::new("cypher", QUERY);
        let record = summary
            .borrow()
            .snapshot()
            .into_iter()
            .find(|record| record.digest == digest.query_digest())
            .unwrap();
        assert_eq!(record.execution_count, 1);
        assert_eq!(record.success_count, 1);
        assert_eq!(record.total_row_count, 0);
    }
    #[test]
    fn foreign_empty_cursor_control_remains_charged_until_drop() {
        let (db, governor) = fixture(2);
        let mut cursor = db
            .query_with_params_retained_node(
                QUERY,
                &BTreeMap::from([("key".into(), Value::Int(999))]),
                RetainedQueryOptions {
                    adapter_metadata_bytes: 8192,
                    ..Default::default()
                },
            )
            .unwrap();
        let before = governor.snapshot().admitted_memory_bytes;
        assert!(before >= 2 * 8192);
        assert!(cursor.next_node().unwrap().is_none());
        let completed = governor.snapshot().admitted_memory_bytes;
        assert!(completed >= 8192 && completed < before);
        cursor.close();
        assert_eq!(governor.snapshot().admitted_memory_bytes, completed);
        drop(cursor);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }

    #[test]
    fn foreign_metadata_is_admitted_and_failed_delivery_preserves_payload() {
        let (db, governor) = fixture(4);
        let mut cursor = db
            .query_with_params_retained_node(QUERY, &params(), RetainedQueryOptions::default())
            .unwrap();
        let view = cursor.next_node().unwrap().unwrap();
        let before = governor.retained_result_snapshot();
        let retained = view.try_retain_with_metadata(8192).unwrap();
        let after = governor.retained_result_snapshot();
        assert_eq!(after.view_handles, before.view_handles + 1);
        assert!(after.retained_bytes >= before.retained_bytes + 8192);
        assert!(matches!(
            view.try_retain_with_metadata(usize::MAX),
            Err(RetainedQueryError::SizeOverflow)
        ));
        assert_eq!(
            governor.retained_result_snapshot().retained_bytes,
            after.retained_bytes
        );
        drop(retained);
        assert_eq!(
            governor.retained_result_snapshot().retained_bytes,
            before.retained_bytes
        );
        cursor.abort_delivery();
        assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
        assert_eq!(view.status(), RetainedQueryStatus::Failed);
        assert!(matches!(
            cursor.next_node(),
            Err(RetainedQueryError::AdapterDelivery)
        ));
        assert!(view.field("text").unwrap().is_some());
        cursor.abort_delivery();
        drop((db, cursor, view));
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }

    #[test]
    fn observation_panic_never_completes_provisional_views() {
        #[derive(Debug)]
        struct PanickingSink;
        impl crate::telemetry::TelemetrySink for PanickingSink {
            fn record_query(&self, _: crate::telemetry::QueryTelemetry<'_>) {
                panic!("injected query observation panic");
            }
        }
        let (mut db, governor) = fixture(2);
        db.set_telemetry_sink(Some(Arc::new(PanickingSink)));
        let mut cursor = db
            .query_with_params_retained_node(QUERY, &params(), RetainedQueryOptions::default())
            .unwrap();
        let view = cursor.next_node().unwrap().unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { cursor.next_node() }))
                .is_err()
        );
        assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
        assert_eq!(view.status(), RetainedQueryStatus::Failed);
        assert!(matches!(
            cursor.next_node(),
            Err(RetainedQueryError::AdapterDelivery)
        ));
        assert!(view.field("text").unwrap().is_some());
        drop((db, cursor, view));
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
    #[test]
    fn task_cancellation_is_sticky_before_delivery_and_at_empty_or_delivered_eof() {
        for (key, deliver) in [(37, false), (37, true), (999, false)] {
            let (db, governor) = fixture(4);
            let context = RuntimeTaskContext::default();
            let mut cursor = db
                .query_with_params_retained_node_context(
                    QUERY,
                    &BTreeMap::from([("key".into(), Value::Int(key))]),
                    RetainedQueryOptions::default(),
                    &context,
                )
                .unwrap();
            let view = deliver.then(|| cursor.next_node().unwrap().unwrap());
            let payload_address = view.as_ref().map(|view| {
                let Some(RetainedNodeField::Value(Value::String(text))) =
                    view.field("text").unwrap()
                else {
                    panic!("borrowed text");
                };
                text.as_ptr()
            });
            context.cancellation().cancel();
            let expected = RetainedQueryError::Stopped(RuntimeCancellationReason::Cancelled);
            for _ in 0..2 {
                assert_eq!(cursor.next_node().unwrap_err(), expected);
                assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
            }
            cursor.close();
            assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
            assert_eq!(cursor.profile().emitted_rows, usize::from(deliver));
            let digest = hawdb_query::QueryIdentity::new("cypher", QUERY);
            let record = db
                .statement_summary
                .borrow()
                .snapshot()
                .into_iter()
                .find(|record| record.digest == digest.query_digest())
                .unwrap();
            assert_eq!(record.execution_count, 1);
            assert_eq!(record.error_count, 1);
            assert_eq!(record.success_count, 0);
            drop(cursor);
            drop(context);
            drop(db);
            if let Some(view) = view.as_ref() {
                assert_eq!(view.status(), RetainedQueryStatus::Failed);
                let Some(RetainedNodeField::Value(Value::String(text))) =
                    view.field("text").unwrap()
                else {
                    panic!("borrowed text");
                };
                assert_eq!(text.as_ptr(), payload_address.unwrap());
                assert_eq!(text, "payload");
                assert!(governor.retained_result_snapshot().retained_bytes > 0);
            }
            drop(view);
            assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        }
    }

    #[test]
    fn expired_task_cannot_deliver_or_complete_an_open_cursor() {
        for (key, deliver) in [(37, false), (37, true), (999, false)] {
            let (db, governor) = fixture(4);
            let mut inner = db
                .query_with_params_retained_node(
                    QUERY,
                    &BTreeMap::from([("key".into(), Value::Int(key))]),
                    RetainedQueryOptions::default(),
                )
                .unwrap();
            let view = deliver.then(|| inner.next_node().unwrap().unwrap());
            // Exercise the post-capture deadline transition without wall-clock
            // sleeps or relying on constructor timing on a loaded CI host.
            let context = RuntimeTaskContext::new(
                hawdb_core::RuntimeCancellationToken::new(),
                Some(hawdb_core::time::Instant::now()),
            );
            let mut cursor = RetainedNodeTaskCursor {
                cursor: inner,
                context: &context,
            };
            let expected = RetainedQueryError::Stopped(RuntimeCancellationReason::DeadlineExceeded);
            assert_eq!(cursor.next_node().unwrap_err(), expected);
            assert_eq!(cursor.next_node().unwrap_err(), expected);
            assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
            if let Some(view) = view.as_ref() {
                assert_eq!(view.status(), RetainedQueryStatus::Failed);
                assert!(view.field("text").unwrap().is_some());
            }
            drop((cursor, view, db));
            assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        }
    }

    #[test]
    fn stopped_task_refuses_before_parsing_or_retained_admission() {
        for reason in [
            RuntimeCancellationReason::Cancelled,
            RuntimeCancellationReason::DeadlineExceeded,
        ] {
            let (db, governor) = fixture(4);
            let context = if reason == RuntimeCancellationReason::Cancelled {
                let context = RuntimeTaskContext::default();
                context.cancellation().cancel();
                context
            } else {
                RuntimeTaskContext::new(
                    hawdb_core::RuntimeCancellationToken::new(),
                    Some(hawdb_core::time::Instant::now()),
                )
            };
            let malformed = "MATCH (";
            assert!(query_runtime::parse_runtime_execution(malformed).is_err());
            for query in [QUERY, malformed] {
                let error = db
                    .query_with_params_retained_node_context(
                        query,
                        &params(),
                        RetainedQueryOptions::default(),
                        &context,
                    )
                    .unwrap_err();
                assert_eq!(error, RetainedQueryError::Stopped(reason));
                assert!(db.retained_result_snapshot().is_none());
                assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
            }
        }
    }

    #[test]
    fn task_reservations_bound_all_retained_charges_and_never_widen_zero() {
        for (query_bytes, result_bytes) in [(0, 8192), (8192, 0), (1, 8192), (8192, 1)] {
            let (db, governor) = fixture(4);
            let context = RuntimeTaskContext::default().with_memory_reservation(
                hawdb_core::RuntimeMemoryReservation::new(query_bytes, result_bytes),
            );
            let error = db
                .query_with_params_retained_node_context(
                    QUERY,
                    &params(),
                    RetainedQueryOptions::default(),
                    &context,
                )
                .unwrap_err();
            assert!(matches!(
                error,
                RetainedQueryError::Execution(HawDBError::Execution(_))
            ));
            assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        }
        let (db, governor) = fixture(4);
        let context = RuntimeTaskContext::default()
            .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(8192, 8192));
        let mut cursor = db
            .query_with_params_retained_node_context(
                QUERY,
                &params(),
                RetainedQueryOptions::default(),
                &context,
            )
            .unwrap();
        let view = cursor.next_node().unwrap().unwrap();
        assert!(cursor.profile().query_peak_bytes <= 8192);
        assert!(matches!(
            view.try_retain_with_metadata(8192),
            Err(RetainedQueryError::Execution(HawDBError::Execution(_)))
        ));
        assert!(cursor.next_node().unwrap().is_none());
        context.cancellation().cancel();
        assert!(cursor.next_node().unwrap().is_none());
        assert_eq!(cursor.status(), RetainedQueryStatus::Completed);
        assert_eq!(view.status(), RetainedQueryStatus::Completed);
        drop((cursor, view, db));
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}
