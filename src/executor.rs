#[cfg(test)]
use crate::analytics::{LouvainOptions, PageRankOptions};
use crate::analytics::{ProjectionLayout, ProjectionMemoryBudget};
#[cfg(test)]
use crate::cypher::RelationshipDirection;
use crate::error::{Result, SkeinError};
use crate::optimizer::PhysicalPlan;
#[cfg(test)]
use crate::planner::GraphAlgorithmKind;
use crate::planner::{Predicate, SetNodePropertiesReturnMode, SetValue};
use crate::schema::Catalog;
use crate::store::{
    ConnectedNodesCreate, GraphScanControl, GraphStore, MatchedRelationshipCopyMerge,
    MatchedRelationshipCreate, MatchedRelationshipMerge, MatchedRelationshipRetargetMerge,
    MatchedRelationshipSourceRetargetMerge, MutationLimits, NodeSetAssignment, NodeSetValue,
    ProjectedGraphDefinition, RelationshipDeleteRequest, RelationshipPropertiesUpdate,
    RelationshipPropertyUpdate, RelationshipSetAssignment, RelationshipTargetNodeDelete,
    ScanPruningReport,
};
use crate::value::Value;
#[cfg(test)]
use skein_analytics::ProjectedGraphExecution;
use skein_core::RuntimeTaskContext;
use skein_ddl::{object_state_to_core, property_type_to_core, table_kind_to_core};
#[cfg(test)]
use skein_executor::store::ScanControl;
use skein_executor::ExecutionLimit;
use std::collections::BTreeMap;
#[cfg(test)]
use std::num::NonZeroU64;
#[cfg(test)]
use std::num::NonZeroUsize;

#[cfg(test)]
use crate::planner::{Aggregation, Projection, RelationshipCountLeg, SortItem};
#[cfg(test)]
use crate::store::{NodeId, NodeRecord};

mod batch;
mod columnar;
mod entrypoint;
mod expression;
mod mutation;
mod observer;
mod read;
mod scan;
mod store_adapter;
#[cfg(test)]
mod traversal;
mod vector;

use batch::*;
use entrypoint::*;
use expression::*;
pub(crate) use mutation::project_staged_mutation_return_rows;
pub use mutation::{execute_mutation_with_limits, is_mutation_plan, mutation_command};
use mutation::{node_set_assignment, relationship_on_create_property_value};
use observer::*;
use read::*;
#[cfg(test)]
use scan::*;
use skein_executor::analytics::try_projected_graph_with_node_filter;
#[cfg(feature = "tokio-runtime")]
pub(crate) use skein_executor::binding::map_memory_bytes;
pub(crate) use skein_executor::binding::map_payload_bytes;
use skein_executor::binding::Binding;
pub(crate) use skein_executor::external::NoExternalReadOperator;
pub(crate) use skein_executor::memory::{
    enforced_query_memory_budget, enforced_result_memory_budget, estimated_execution_memory,
    estimated_mutation_memory_bytes, max_external_read_parallelism,
};
use skein_executor::pipeline::{runtime_checkpoint, BatchControl};
use skein_executor::predicate::{
    label_ids_for_pattern, node_matches_label_pattern, node_matches_property_filter,
    property_filter_from_properties,
};
use skein_executor::QueryMemoryLedger;
#[cfg(test)]
use skein_executor::{pipeline::BindingBatch, QueryMemoryClass};
pub use skein_executor::{ExecutionMemoryConfig, SpillPoolSnapshot};
pub use skein_executor::{
    ExternalReadOperator, ExternalReadResourceContract, ExternalReadResultBudget,
    OperatorCardinalityProfile, VectorSeedExecutionOutput, VectorSeedExecutionRequest,
    VectorSeedExecutionRow,
};
#[cfg(test)]
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
pub(crate) use skein_executor::batch::SOURCE_SEGMENT_SCAN_IO_DEPTH;

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

pub use skein_executor::observer::read_execution_profile;

#[cfg(test)]
#[path = "executor/tests.rs"]
mod tests;
