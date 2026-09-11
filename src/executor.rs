use crate::analytics::{
    LouvainOptions, PageRankOptions, ProjectedGraph, ProjectionLayout, ProjectionMemoryBudget,
};
use crate::cypher::RelationshipDirection;
use crate::error::{Result, SkeinError};
use crate::optimizer::PhysicalPlan;
use crate::planner::{
    Aggregation, GraphAlgorithmKind, PlanChildren, Predicate, Projection, RelationshipCountLeg,
    RelationshipOnCreateValue, SetNodePropertiesReturnMode, SetValue, SortItem,
};
use crate::schema::Catalog;
use crate::store::{
    ConnectedNodesCreate, GraphMutation, GraphScanControl, GraphStore,
    MatchedRelationshipCopyMerge, MatchedRelationshipCreate, MatchedRelationshipMerge,
    MatchedRelationshipRetargetMerge, MatchedRelationshipSourceRetargetMerge, MutationLimits,
    NodeId, NodeRecord, NodeSetAssignment, NodeSetValue, ProjectedGraphDefinition, PropertyFilter,
    RelationshipDeleteRequest, RelationshipOnCreatePropertyValue, RelationshipPropertiesUpdate,
    RelationshipPropertyUpdate, RelationshipSetAssignment, RelationshipTargetNodeDelete,
    ScanPruningReport,
};
use crate::value::Value;
use skein_analytics::ProjectedGraphExecution;
use skein_core::RuntimeTaskContext;
use skein_ddl::{object_state_to_core, property_type_to_core, table_kind_to_core};
use skein_executor::store::{ScanControl, SourceScanCandidateVisit, SourceScanReadLimits};
use skein_executor::ExecutionLimit;
use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};

mod batch;
mod blocking;
mod columnar;
mod entrypoint;
mod expression;
mod mutation;
mod observer;
mod read;
mod scan;
mod store_adapter;
mod traversal;
mod vector;

use batch::*;
use blocking::*;
use columnar::*;
use entrypoint::*;
use expression::*;
pub(crate) use mutation::project_staged_mutation_return_rows;
pub use mutation::{execute_mutation_with_limits, is_mutation_plan, mutation_command};
use mutation::{
    node_set_assignment, relationship_on_create_property_value,
    try_projected_graph_with_node_filter,
};
use observer::*;
use read::*;
use scan::*;
pub(crate) use skein_executor::binding::map_memory_bytes;
pub(crate) use skein_executor::binding::map_payload_bytes;
use skein_executor::binding::{binding_memory_bytes, Binding};
pub(crate) use skein_executor::external::NoExternalReadOperator;
use skein_executor::graph::GraphExpansionExecutionState;
use skein_executor::kernel::{
    collect_bounded_operator_bindings_with_account, push_bounded_operator_binding,
    OperatorMemoryTracker,
};
pub(crate) use skein_executor::memory::{
    estimated_execution_memory, estimated_mutation_memory_bytes, external_read_memory_budget,
    max_external_read_parallelism, ExecutionMemoryEstimate,
};
use skein_executor::memory::{DEFAULT_EXECUTION_BATCH_ROWS, SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES};
use skein_executor::pipeline::{
    emit_owned_binding_batches, runtime_checkpoint, AccountedBindingBatch, BatchControl,
    BindingBatch, TransformBatchBuilder,
};
use skein_executor::predicate::{
    label_ids_for_pattern, node_matches_label_pattern, node_matches_property_filter,
    node_properties_match, property_filter_from_properties,
};
use skein_executor::scan::{
    single_node_binding, source_scan_pruning_strategy, source_storage_scan_predicate,
    stream_expand_binding, AdjacencyExpandFilters, AdjacencyExpandSpec, NodeColumnLookupSpec,
    NodeProjectionScanSpec, NodeScanContext, NodeScanSpec,
};
pub use skein_executor::{ExecutionMemoryConfig, SpillPoolSnapshot};
pub use skein_executor::{
    ExternalReadOperator, ExternalReadResourceContract, ExternalReadResultBudget,
    OperatorCardinalityProfile, VectorSeedExecutionOutput, VectorSeedExecutionRequest,
    VectorSeedExecutionRow,
};
use skein_executor::{QueryMemoryClass, QueryMemoryLedger};
use traversal::*;
use vector::*;

pub type Row = skein_executor::Row;
pub type RowRef<'a> = skein_executor::RowRef<'a>;
pub type QueryRow = skein_executor::QueryRow;
pub type QueryRowRef<'a> = skein_executor::QueryRowRef<'a>;
pub type QueryRows = skein_executor::QueryRows;
pub type QueryRowsBuilder = skein_executor::QueryRowsBuilder;
pub type QueryValueRows<'a> = skein_executor::QueryValueRows<'a>;
pub type QuerySchema = skein_executor::QuerySchema;
pub type ReadExecutionProfile = skein_executor::ReadExecutionProfile<ScanPruningReport>;
pub type ProfiledQueryRows = skein_executor::ProfiledQueryRows<ScanPruningReport>;
pub type ProfiledQueryStream = skein_executor::ProfiledQueryStream<ScanPruningReport>;
pub(crate) use skein_executor::numeric::MAX_MORSEL_PARALLELISM;
const DEFAULT_MORSEL_CPU_SHARE_DIVISOR: usize = 4;
const DEFAULT_MORSEL_MIN_PARALLELISM: usize = 4;
pub(crate) const SOURCE_SEGMENT_SCAN_IO_DEPTH: usize = 2;
const SOURCE_SEGMENT_SCAN_MAX_COALESCED_BYTES: u64 = 512 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamDelivery {
    /// Keep bounded rows query-owned until every output limit is validated.
    Validated,
    /// Deliver rows as produced and let the caller surface terminal failures.
    Incremental,
}

impl StreamDelivery {
    fn consumer_memory_mode(self, bounded: bool) -> ConsumerMemoryMode {
        match (self, bounded) {
            (Self::Validated, true) => ConsumerMemoryMode::DeferredUntilValidated,
            (Self::Validated, false) | (Self::Incremental, _) => {
                ConsumerMemoryMode::ReleasedAfterCall
            }
        }
    }
}

pub(crate) fn enforced_query_memory_budget(
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<NonZeroUsize> {
    let Some(reservation) = task_context.and_then(RuntimeTaskContext::memory_reservation) else {
        return Ok(memory.query_memory_bytes);
    };
    // Never widen an undersized admission to an operator-configured floor.
    // The shared root remains the admitted reservation and the operator fails
    // closed when its first charge cannot fit.
    runtime_memory_budget("query memory", reservation.memory_bytes())
}

pub(crate) fn enforced_result_memory_budget(
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<NonZeroUsize> {
    let Some(reservation) = task_context.and_then(RuntimeTaskContext::memory_reservation) else {
        return Ok(memory.query_memory_bytes);
    };
    let admitted = runtime_memory_budget("query result", reservation.result_bytes())?;
    Ok(admitted.min(memory.query_memory_bytes))
}

fn runtime_memory_budget(owner: &str, bytes: u64) -> Result<NonZeroUsize> {
    let bytes = usize::try_from(bytes).map_err(|_| {
        SkeinError::Execution(format!(
            "runtime-admitted {owner} reservation {bytes} does not fit the executor address space"
        ))
    })?;
    NonZeroUsize::new(bytes).ok_or_else(|| {
        SkeinError::Execution(format!(
            "runtime-admitted {owner} reservation must be non-zero"
        ))
    })
}

pub(crate) fn default_morsel_cpu_ceiling(effective_cpu_slots: usize) -> usize {
    let effective_cpu_slots = effective_cpu_slots.max(1);
    effective_cpu_slots
        .div_ceil(DEFAULT_MORSEL_CPU_SHARE_DIVISOR)
        .max(DEFAULT_MORSEL_MIN_PARALLELISM)
        .min(effective_cpu_slots)
        .min(MAX_MORSEL_PARALLELISM)
}

pub(crate) fn supports_default_morsel_parallelism(plan: &PhysicalPlan, catalog: &Catalog) -> bool {
    columnar::supports_parallel_morsel_execution(plan, catalog)
}

pub(crate) fn default_morsel_parallelism(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    store: &dyn skein_executor::store::GraphExecutionRead,
    memory: &ExecutionMemoryConfig,
) -> usize {
    columnar::default_morsel_parallelism(plan, catalog, store, memory)
}

struct ExecutionContext<'a> {
    parameters: &'a BTreeMap<String, Value>,
    external: &'a mut dyn ExternalReadOperator,
    memory: &'a ExecutionMemoryConfig,
    memory_ledger: &'a QueryMemoryLedger,
    task_context: Option<&'a RuntimeTaskContext>,
    observer: &'a QueryExecutionObserver,
}

/// The execution-facing form of a physical plan.
///
/// Planning owns operator selection. This boundary performs the one-time
/// recursive capability check that decides whether a read can enter the
/// streaming batch engine, leaving mutation and schema plans on the
/// materialized path. It deliberately borrows the planner-owned tree so plan
/// cache templates remain the sole owner of physical plan structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PreparedExecutionMode {
    Batch,
    Materialized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PreparedStorageCapability {
    InMemory,
    OutOfCore,
}

#[derive(Clone, Copy)]
pub(super) struct PreparedPhysicalPlan<'a> {
    plan: &'a PhysicalPlan,
    batch_plan: Option<BatchPlanRef<'a>>,
    execution_mode: PreparedExecutionMode,
    storage_capability: PreparedStorageCapability,
    required_memory: ExecutionMemoryEstimate,
}

impl<'a> PreparedPhysicalPlan<'a> {
    pub(super) fn prepare(
        plan: &'a PhysicalPlan,
        store: &dyn skein_executor::store::GraphExecutionRead,
        memory: &ExecutionMemoryConfig,
    ) -> Self {
        let batch_plan = BatchPlanRef::try_new(plan);
        Self {
            plan,
            execution_mode: if batch_plan.is_some() {
                PreparedExecutionMode::Batch
            } else {
                PreparedExecutionMode::Materialized
            },
            batch_plan,
            storage_capability: if store.is_out_of_core() {
                PreparedStorageCapability::OutOfCore
            } else {
                PreparedStorageCapability::InMemory
            },
            required_memory: estimated_execution_memory(plan, memory),
        }
    }

    pub(in crate::executor) fn batch(self) -> Option<BatchPlanRef<'a>> {
        match self.execution_mode() {
            PreparedExecutionMode::Batch => self.batch_plan,
            PreparedExecutionMode::Materialized => None,
        }
    }

    pub(in crate::executor) fn plan(self) -> &'a PhysicalPlan {
        self.plan
    }

    pub(in crate::executor) fn execution_mode(self) -> PreparedExecutionMode {
        self.execution_mode
    }

    pub(in crate::executor) fn storage_capability(self) -> PreparedStorageCapability {
        self.storage_capability
    }

    pub(in crate::executor) fn required_memory(self) -> ExecutionMemoryEstimate {
        self.required_memory
    }
}

pub fn execute(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
) -> Result<Vec<Row>> {
    execute_with_row_limit(plan, catalog, store, None)
}

pub fn execute_with_row_limit(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    max_rows: Option<usize>,
) -> Result<Vec<Row>> {
    execute_with_row_limit_internal(plan, catalog, store, max_rows, None)
}

pub fn execute_with_row_limit_and_context(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    max_rows: Option<usize>,
    task_context: &RuntimeTaskContext,
) -> Result<Vec<Row>> {
    execute_with_row_limit_internal(plan, catalog, store, max_rows, Some(task_context))
}

fn execute_with_row_limit_internal(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    max_rows: Option<usize>,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    let parameters = BTreeMap::new();
    let mut external = NoExternalReadOperator;
    let memory = ExecutionMemoryConfig::default();
    let mut rows = Vec::new();
    execute_profiled_consumer(
        ExecutionRequest::new(plan, &parameters, &memory)
            .with_output_limits(max_rows, None)
            .with_optional_task_context(task_context),
        ExecutionResources::new(catalog, store, &mut external),
        ConsumerMemoryMode::Retained,
        &mut |row| {
            rows.push(row);
            Ok(())
        },
    )?;
    Ok(rows)
}

pub fn execute_with_row_limit_profile(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    max_rows: Option<usize>,
) -> Result<ProfiledQueryRows> {
    let mut external = NoExternalReadOperator;
    execute_with_row_limit_profile_and_external(
        plan,
        catalog,
        store,
        &BTreeMap::new(),
        &mut external,
        max_rows,
    )
}

pub fn execute_with_row_limit_profile_and_external(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
) -> Result<ProfiledQueryRows> {
    execute_with_row_limit_profile_and_external_and_memory(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        &ExecutionMemoryConfig::default(),
    )
}

pub fn execute_with_output_limits_profile_and_external(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
) -> Result<ProfiledQueryRows> {
    execute_with_output_limits_profile_and_external_and_memory(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        &ExecutionMemoryConfig::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_output_limits_profile_and_external_and_memory(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    memory: &ExecutionMemoryConfig,
) -> Result<ProfiledQueryRows> {
    execute_profiled_rows(
        ExecutionRequest::new(plan, parameters, memory)
            .with_output_limits(max_rows, max_payload_bytes),
        ExecutionResources::new(catalog, store, external),
    )
}

pub fn execute_with_row_limit_profile_and_external_and_memory(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    memory: &ExecutionMemoryConfig,
) -> Result<ProfiledQueryRows> {
    execute_profiled_rows(
        ExecutionRequest::new(plan, parameters, memory).with_output_limits(max_rows, None),
        ExecutionResources::new(catalog, store, external),
    )
}

pub fn execute_with_row_limit_profile_and_external_and_context(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    task_context: &RuntimeTaskContext,
) -> Result<ProfiledQueryRows> {
    let memory = ExecutionMemoryConfig::default();
    execute_profiled_rows(
        ExecutionRequest::new(plan, parameters, &memory)
            .with_output_limits(max_rows, None)
            .with_task_context(task_context),
        ExecutionResources::new(catalog, store, external),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_output_limits_profile_and_external_and_context(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    task_context: &RuntimeTaskContext,
) -> Result<ProfiledQueryRows> {
    execute_with_output_limits_profile_and_external_and_context_and_memory(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        task_context,
        &ExecutionMemoryConfig::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_output_limits_profile_and_external_and_context_and_memory(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    task_context: &RuntimeTaskContext,
    memory: &ExecutionMemoryConfig,
) -> Result<ProfiledQueryRows> {
    execute_profiled_rows(
        ExecutionRequest::new(plan, parameters, memory)
            .with_output_limits(max_rows, max_payload_bytes)
            .with_task_context(task_context),
        ExecutionResources::new(catalog, store, external),
    )
}

/// Executes a read plan and transfers ownership of each output row to a
/// consumer. When either output limit is present, rows remain query-owned
/// until execution validates the complete result against both limits. With
/// both limits disabled, rows are transferred as they are produced.
pub fn execute_with_row_consumer_profile(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<ProfiledQueryStream> {
    let mut external = NoExternalReadOperator;
    execute_with_row_consumer_profile_and_external(
        plan,
        catalog,
        store,
        parameters,
        &mut external,
        max_rows,
        max_payload_bytes,
        consumer,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_row_consumer_profile_and_external(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
) -> Result<ProfiledQueryStream> {
    execute_with_row_consumer_profile_and_external_and_memory(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        consumer,
        &ExecutionMemoryConfig::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_row_consumer_profile_and_external_and_memory(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
    memory: &ExecutionMemoryConfig,
) -> Result<ProfiledQueryStream> {
    execute_with_row_consumer_profile_with_delivery(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        consumer,
        None,
        memory,
        StreamDelivery::Validated,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_row_consumer_profile_and_external_and_context(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
    task_context: &RuntimeTaskContext,
) -> Result<ProfiledQueryStream> {
    execute_with_row_consumer_profile_and_external_and_context_and_memory(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        consumer,
        task_context,
        &ExecutionMemoryConfig::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_row_consumer_profile_and_external_and_context_and_memory(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
    task_context: &RuntimeTaskContext,
    memory: &ExecutionMemoryConfig,
) -> Result<ProfiledQueryStream> {
    execute_with_row_consumer_profile_with_delivery(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        consumer,
        Some(task_context),
        memory,
        StreamDelivery::Validated,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_with_row_consumer_profile_with_delivery(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
    task_context: Option<&RuntimeTaskContext>,
    memory: &ExecutionMemoryConfig,
    delivery: StreamDelivery,
) -> Result<ProfiledQueryStream> {
    let bounded = max_rows.is_some() || max_payload_bytes.is_some();
    execute_profiled_consumer(
        ExecutionRequest::new(plan, parameters, memory)
            .with_output_limits(max_rows, max_payload_bytes)
            .with_optional_task_context(task_context),
        ExecutionResources::new(catalog, store, external),
        delivery.consumer_memory_mode(bounded),
        consumer,
    )
}

pub fn read_execution_profile(
    plan: &PhysicalPlan,
    max_rows: Option<usize>,
) -> Result<ReadExecutionProfile> {
    let execution_limit = ExecutionLimit::from_user_max_rows(max_rows)?;
    Ok(ReadExecutionProfile {
        max_rows,
        detection_row_cap: execution_limit.output_rows,
        row_limit_enforced_before_output: max_rows.is_some(),
        operator_row_cap_enabled: execution_limit.output_rows.is_some(),
        operator_cardinality_profiles: Vec::new(),
        blocking_operator_kinds: blocking_operator_kinds(plan),
        scan_pruning_reports: Vec::new(),
        vector_execution_reports: Vec::new(),
        graph_expansion_reports: Vec::new(),
        blocking_operator_memory_reports: Vec::new(),
        pipeline_memory_report: skein_executor::PipelineMemoryReport::default(),
    })
}

#[cfg(test)]
#[path = "executor/tests.rs"]
mod tests;
