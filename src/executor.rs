use crate::analytics::{
    LouvainOptions, PageRankOptions, ProjectedGraph, ProjectionLayout, ProjectionMemoryBudget,
};
use crate::cypher::RelationshipDirection;
use crate::error::{Result, SkeinError};
use crate::optimizer::PhysicalPlan;
use crate::planner::{
    Aggregation, GraphAlgorithmKind, PhysicalPlanClass, PlanChildren, Predicate, Projection,
    ProjectionExpression, RelationshipCountLeg, RelationshipOnCreateValue,
    SetNodePropertiesReturnMode, SetValue, SortItem,
};
use crate::schema::Catalog;
use crate::store::{
    ConnectedNodesCreate, GraphMutation, GraphScanControl, GraphStore,
    MatchedRelationshipCopyMerge, MatchedRelationshipCreate, MatchedRelationshipMerge,
    MatchedRelationshipRetargetMerge, MatchedRelationshipSourceRetargetMerge, MutationLimits,
    NodeId, NodeRecord, NodeSetAssignment, NodeSetValue, ProjectedGraphDefinition, PropertyFilter,
    RelRecord, RelationshipDeleteRequest, RelationshipOnCreatePropertyValue,
    RelationshipPropertiesUpdate, RelationshipPropertyUpdate, RelationshipSetAssignment,
    RelationshipTargetNodeDelete, ScanPruningReport, SourceScanCandidateRead,
};
use crate::value::Value;
use skein_analytics::ProjectedGraphExecution;
use skein_core::RuntimeTaskContext;
use skein_ddl::{object_state_to_core, property_type_to_core, table_kind_to_core};
use skein_executor::ExecutionLimit;
use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};

mod batch;
mod blocking;
mod columnar;
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
#[cfg(feature = "tokio-runtime")]
pub(crate) use skein_executor::binding::map_memory_bytes;
pub(crate) use skein_executor::binding::map_payload_bytes;
use skein_executor::binding::{binding_memory_bytes, Binding};
pub(crate) use skein_executor::external::NoExternalReadOperator;
use skein_executor::graph::GraphExpansionExecutionState;
use skein_executor::kernel::{
    collect_bounded_operator_bindings, ensure_operator_item_fits, push_bounded_operator_binding,
    OperatorMemoryTracker, SpillBudgetTracker,
};
pub(crate) use skein_executor::memory::{
    estimated_execution_memory, estimated_mutation_memory_bytes,
};
use skein_executor::memory::{DEFAULT_EXECUTION_BATCH_ROWS, SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES};
use skein_executor::pipeline::{
    emit_owned_binding_batches, runtime_checkpoint, BatchControl, BindingBatch,
};
use skein_executor::predicate::{
    label_ids_for_pattern, node_matches_label_pattern, node_matches_property_filter,
    node_properties_match, property_filter_from_properties,
};
use skein_executor::scan::{
    expand_binding, single_node_binding, source_scan_pruning_strategy,
    source_storage_scan_predicate, AdjacencyExpandFilters, AdjacencyExpandSpec,
    NodeColumnLookupSpec, NodeScanContext, NodeScanSpec,
};
pub use skein_executor::{ExecutionMemoryConfig, SpillPoolSnapshot};
pub use skein_executor::{
    ExternalReadOperator, VectorSeedExecutionOutput, VectorSeedExecutionRequest,
    VectorSeedExecutionRow,
};
use traversal::*;
use vector::*;

pub type Row = skein_executor::Row;
pub type ReadExecutionProfile = skein_executor::ReadExecutionProfile<ScanPruningReport>;
pub type ProfiledQueryRows = skein_executor::ProfiledQueryRows<ScanPruningReport>;
pub type ProfiledQueryStream = skein_executor::ProfiledQueryStream<ScanPruningReport>;
pub(crate) const MAX_MORSEL_PARALLELISM: usize = 16;
const DEFAULT_MORSEL_CPU_SHARE_DIVISOR: usize = 4;
const DEFAULT_MORSEL_MIN_PARALLELISM: usize = 4;
pub(crate) const SOURCE_SEGMENT_SCAN_IO_DEPTH: usize = 2;
const SOURCE_SEGMENT_SCAN_MAX_COALESCED_BYTES: u64 = 512 * 1024;

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
    store: &GraphStore,
    memory: &ExecutionMemoryConfig,
) -> usize {
    columnar::default_morsel_parallelism(plan, catalog, store, memory)
}

struct ExecutionContext<'a> {
    parameters: &'a BTreeMap<String, Value>,
    external: &'a mut dyn ExternalReadOperator,
    memory: &'a ExecutionMemoryConfig,
    task_context: Option<&'a RuntimeTaskContext>,
    observer: &'a QueryExecutionObserver,
}

#[derive(Clone, Copy)]
struct ExecutionRuntimeControl<'a> {
    memory: &'a ExecutionMemoryConfig,
    task_context: Option<&'a RuntimeTaskContext>,
}

#[derive(Clone, Copy)]
struct ExecutionOutputLimits {
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
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
    execute_with_row_consumer_profile_internal(
        plan,
        catalog,
        store,
        &parameters,
        &mut external,
        max_rows,
        None,
        &mut |row| {
            rows.push(row);
            Ok(())
        },
        ExecutionRuntimeControl {
            memory: &memory,
            task_context,
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
    execute_with_row_limit_profile_and_external_and_memory_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        ExecutionOutputLimits {
            max_rows,
            max_payload_bytes,
        },
        ExecutionRuntimeControl {
            memory,
            task_context: None,
        },
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
    execute_with_row_limit_profile_and_external_and_memory_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        ExecutionOutputLimits {
            max_rows,
            max_payload_bytes: None,
        },
        ExecutionRuntimeControl {
            memory,
            task_context: None,
        },
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
    execute_with_row_limit_profile_and_external_and_memory_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        ExecutionOutputLimits {
            max_rows,
            max_payload_bytes: None,
        },
        ExecutionRuntimeControl {
            memory: &memory,
            task_context: Some(task_context),
        },
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
    execute_with_row_limit_profile_and_external_and_memory_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        ExecutionOutputLimits {
            max_rows,
            max_payload_bytes,
        },
        ExecutionRuntimeControl {
            memory,
            task_context: Some(task_context),
        },
    )
}

/// Executes a read plan and transfers ownership of each output row to a
/// bounded consumer. Consumer calls are provisional until this function
/// returns `Ok`: callers that cannot surface a terminal error must buffer or
/// otherwise roll back their response when a later row exceeds a budget.
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
    execute_with_row_consumer_profile_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        consumer,
        ExecutionRuntimeControl {
            memory,
            task_context: None,
        },
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
    execute_with_row_consumer_profile_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        consumer,
        ExecutionRuntimeControl {
            memory,
            task_context: Some(task_context),
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_with_row_consumer_profile_internal(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    consumer: &mut dyn FnMut(Row) -> Result<()>,
    runtime: ExecutionRuntimeControl<'_>,
) -> Result<ProfiledQueryStream> {
    store.ensure_usable()?;
    let ExecutionRuntimeControl {
        memory,
        task_context,
    } = runtime;
    let process_memory_start = skein_qos::ProcessMemorySnapshot::capture().ok();
    let execution_limit = ExecutionLimit::from_user_max_rows(max_rows)?;
    let mut profile = read_execution_profile(plan, max_rows)?;
    let batch_plan = BatchPlanRef::try_new(plan);
    let fully_streamed = batch_plan.is_some();
    let mut output_rows = 0usize;
    let mut output_payload_bytes = 0usize;
    let mut emit_binding = |binding: Binding| -> Result<()> {
        if max_rows.is_some_and(|limit| output_rows >= limit) {
            return Err(SkeinError::Execution(format!(
                "read query returned more than {} rows, exceeding max_read_result_rows {}",
                max_rows.unwrap_or_default(),
                max_rows.unwrap_or_default()
            )));
        }
        let row = binding.values;
        let row_payload_bytes = map_payload_bytes(&row);
        let next_payload_bytes = output_payload_bytes.saturating_add(row_payload_bytes);
        if max_payload_bytes.is_some_and(|limit| next_payload_bytes > limit) {
            return Err(SkeinError::Execution(format!(
                "read query payload would exceed max_payload_bytes {} (max_read_result_payload_bytes {}; next total {})",
                max_payload_bytes.unwrap_or_default(),
                max_payload_bytes.unwrap_or_default(),
                next_payload_bytes
            )));
        }
        consumer(row)?;
        output_rows = output_rows.saturating_add(1);
        output_payload_bytes = next_payload_bytes;
        Ok(())
    };
    let observer = QueryExecutionObserver::default();
    let mut context = ExecutionContext {
        parameters,
        external,
        memory,
        task_context,
        observer: &observer,
    };
    if let Some(batch_plan) = batch_plan {
        let external = BatchExternalReadAdapter::new(&mut *context.external);
        let batch_context = BatchReadContext {
            catalog,
            store,
            parameters: context.parameters,
            external: &external,
            memory,
            task_context,
            observer: context.observer,
        };
        execute_prepared_binding_batches(
            batch_plan,
            batch_context,
            execution_limit,
            &mut |batch| {
                for binding in batch {
                    emit_binding(binding)?;
                }
                Ok(BatchControl::Continue)
            },
        )?;
    } else {
        let bindings =
            execute_bindings_with_limit(plan, catalog, store, &mut context, execution_limit)?;
        for binding in bindings {
            emit_binding(binding)?;
        }
    }
    let QueryExecutionReports {
        scan_pruning,
        vector_execution,
        graph_expansion,
        blocking_memory,
        mut pipeline_memory,
    } = observer.into_reports();
    profile.scan_pruning_reports = scan_pruning;
    profile.vector_execution_reports = vector_execution;
    profile.graph_expansion_reports = graph_expansion;
    profile.blocking_operator_memory_reports = blocking_memory;
    let pipeline_memory_report = &mut pipeline_memory;
    pipeline_memory_report.output_rows = output_rows;
    pipeline_memory_report.output_payload_bytes = output_payload_bytes;
    if let Ok(process_memory_end) = skein_qos::ProcessMemorySnapshot::capture() {
        pipeline_memory_report.steady_resident_bytes = Some(process_memory_end.resident_bytes);
        pipeline_memory_report.peak_resident_bytes = Some(process_memory_end.peak_resident_bytes);
        if let Some(process_memory_start) = process_memory_start {
            let process_memory =
                skein_qos::ProcessMemoryProfile::between(process_memory_start, process_memory_end);
            pipeline_memory_report.start_resident_bytes = Some(process_memory.start_resident_bytes);
            pipeline_memory_report.start_peak_resident_bytes =
                Some(process_memory.start_peak_resident_bytes);
            pipeline_memory_report.steady_resident_growth_bytes =
                Some(process_memory.steady_resident_growth_bytes);
            pipeline_memory_report.lifetime_peak_resident_growth_bytes =
                Some(process_memory.lifetime_peak_resident_growth_bytes);
            pipeline_memory_report.total_page_faults = process_memory.total_page_faults;
            pipeline_memory_report.minor_page_faults = process_memory.minor_page_faults;
            pipeline_memory_report.major_page_faults = process_memory.major_page_faults;
        }
    }
    profile.pipeline_memory_report = pipeline_memory;
    Ok(ProfiledQueryStream {
        fully_streamed,
        profile,
    })
}

fn execute_with_row_limit_profile_and_external_and_memory_internal(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    parameters: &BTreeMap<String, Value>,
    external: &mut dyn ExternalReadOperator,
    output_limits: ExecutionOutputLimits,
    runtime: ExecutionRuntimeControl<'_>,
) -> Result<ProfiledQueryRows> {
    let ExecutionOutputLimits {
        max_rows,
        max_payload_bytes,
    } = output_limits;
    let mut rows = Vec::new();
    let streamed = execute_with_row_consumer_profile_internal(
        plan,
        catalog,
        store,
        parameters,
        external,
        max_rows,
        max_payload_bytes,
        &mut |row| {
            rows.push(row);
            Ok(())
        },
        runtime,
    )?;
    Ok(ProfiledQueryRows {
        rows,
        profile: streamed.profile,
    })
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
