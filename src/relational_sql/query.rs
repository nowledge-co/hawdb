use super::{
    RelationalJoinPlanningOutcome, RelationalJoinPlanningStatus,
    RelationalOperatorCardinalityProfile, RelationalOperatorId, RelationalOperatorKind,
};
use crate::error::{Result, SkeinError};
use crate::executor::{map_payload_bytes, Row};
use crate::relational_sql::index_access::{
    RelationalIndexExecutionEvidence, RelationalIndexReadMode, RelationalIndexRuntime,
};
use crate::relational_sql::row_access::{
    expression_contains_aggregate, plan_requested_fields, plan_scan_fields,
    plan_scan_hydration_fields, RelationalFieldPlan, RelationalReadRow,
    RelationalRowExecutionEvidence, RelationalRowReadMode, RelationalRowRuntime,
};
use crate::sql::{
    SelectProjection, SelectStatement, SqlBound, SqlColumnRef, SqlComparisonOp, SqlExpression,
    SqlFunctionArgument, SqlJoinKind, SqlLikeEscape, SqlNullOrder, SqlOrderDirection, SqlPredicate,
    SqlStatement, SqlValue,
};
use crate::value::Value;
use skein_core::Catalog;
use skein_executor::binding::Binding as ExecutorBinding;
use skein_executor::blocking::{
    stream_distinct_batches, stream_top_n_batches, BindingBatchSource, BlockingExecutionContext,
};
use skein_executor::external_order::ExternalTopN;
use skein_executor::kernel::{
    ensure_operator_item_fits, OperatorMemoryTracker, SpillBudgetTracker,
};
use skein_executor::observer::ExecutionObserver;
use skein_executor::pipeline::{AccountedBindingBatch, BatchControl, BindingBatch};
use skein_executor::spill::{SpillRun, SpillWriter};
use skein_executor::{
    BindingSchema, BlockingOperatorMemoryReport, ColumnVector, ColumnarBatch, ExecutionLimit,
    QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger, QueryRows, QueryRowsBuilder,
    RelationalRowLocator, SlotDescriptor, SlotId, SlotType,
};
use skein_expression::BindingId;
use skein_optimizer::{
    estimate_relational_access_cost, estimate_relational_join_cost,
    estimate_relational_probe_join_cost, select_relational_access_path, PlanCostBreakdown,
    RelationalAccessPathDescriptor, RelationalAccessPathKind, RelationalJoinCardinality,
    RelationalJoinEnumerationConfig, RelationalJoinPlanningDirective, RelationalJoinRightInput,
    RelationalJoinSelectivity,
};
use skein_plan::{PhysicalPlan, SortDirection, SortItem, SortKey};
use skein_storage::{
    relational_unique_index_name, RelationalHydrationBudget, RelationalIndexRangeScan,
    RelationalIndexScanDirection, RelationalKey, RelationalScalarType, RelationalState,
    RelationalTableSchema, RelationalValue, RelationalValueRef,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use super::{
    resolve_relational_order_target, PreparedRelationalSql, RelationalOrderTarget,
    RelationalSqlStageTimings,
};
use skein_sql::timing::{elapsed_nanos, measure_nanos};

mod columnar_aggregate;
mod join_order;
mod locator;
mod streaming_binding;

use self::columnar_aggregate::ColumnarAggregateExecutor;
use self::locator::{
    RelationalLocatorLayout, RelationalRowSetLocator, RelationalSortKey, RelationalSortRecord,
};
use self::streaming_binding::{BoundStreamingPredicate, BoundStreamingProjection};

use super::coerce_relational_value;

mod access;
#[cfg(test)]
use access::RelationalProjectionAccessPlanning;
use access::{
    bound_join_key, choose_base_access, choose_join_access, collect_conjunctive_join_equalities,
    predicate_is_covered_by_access, projection_access_planning, visit_base_entries,
    visit_join_entries, RelationalBaseAccessPlanning,
};

mod aggregate;
use aggregate::{execute_aggregate_select, single_count_distinct_column};

mod aggregate_state;
mod having;
use aggregate_state::{
    aggregate_group_base_memory_bytes, charge_aggregate_memory, AggregateProjectionState,
};

mod execution;
#[cfg(test)]
use execution::execute_select;
use execution::{execute_select_timed, explain_select};

mod explain;
use explain::format_relational_explain;
#[cfg(test)]
use explain::{optional_estimated_rows_explain_value, optional_usize_explain_value};

mod expression;
use expression::{
    account_intermediate, aggregate_filter_matches, bind_bound, bind_sql_value, compare_value_refs,
    evaluate_row_expression, expression_name, predicate_truth, project_bound_row,
    projection_contains_aggregate, projection_uses_non_aggregate_coalesce,
    reject_non_public_schema, relational_ref_to_value, relational_to_value, resolve_column,
    validate_non_aggregate_coalesce_projections, value_to_relational, value_to_relational_as,
};

mod join;
use join::{
    batched_index_probe_key, bound_relation_join_key, bound_row_resident_bytes,
    null_extended_tree_row, relational_key_resident_bytes, visit_tree_relation_entries,
    RelationalPhysicalJoinExecution,
};

mod join_hash;
use join_hash::visit_hash_join;

mod join_index;
use join_index::visit_batched_index_nested_loop;

mod join_merge;
use join_merge::visit_index_merge_join;

mod physical;
#[cfg(test)]
use physical::RelationalExecutionMemoryShape;
use physical::{
    planned_operator_cardinality_profiles, PreparedRelationalAccessPlan,
    PreparedRelationalExecutionDescriptor, PreparedRelationalExecutionMode,
    PreparedRelationalJoinSelection, PreparedRelationalSelect, RelationalAccessCandidate,
    RelationalBaseAccess, RelationalEquiJoinKeys, RelationalJoinAccess,
    RelationalJoinAccessCandidate, RelationalPhysicalAccess, RelationalPhysicalJoinAlgorithm,
    RelationalPhysicalJoinNode, RelationalPhysicalJoinPlan, RelationalPhysicalOutputSchema,
    RelationalPhysicalRelation,
};

mod pipeline;
use pipeline::{
    project_typed_locator, relational_locator_layout, relational_physical_join_plan_locator_layout,
    typed_row_set_locator, visit_prepared_physical_join_plan_node, visit_relational_rows,
    with_typed_locator_bound_row_for_scan, with_typed_locator_bound_row_mode,
    AccountedRelationalLocatorBatch, PlannedJoin, RelationalPipelineState,
    RELATIONAL_ROW_LOCATOR_SLOT,
};

mod preparation;
use preparation::{
    plan_relational_field_plan, prepare_relational_select, prepared_access_descriptors,
    resolved_access_order_by,
};

mod projection;
use projection::{
    execute_blocking_projection, order_by_uses_expression_alias, push_relational_output,
    relational_input_plan, DistinctAggregateValueBatchSource, RelationalBlockingObserver,
    StreamingProjectionOutput,
};

mod streaming_projection;
use streaming_projection::{execute_ordered_index_projection, execute_streaming_projection};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RelationalQueryLimits {
    pub max_output_rows: usize,
    pub max_output_payload_bytes: usize,
    pub max_intermediate_rows: usize,
    /// Maximum relation-join work units, including probe attempts and rows
    /// considered by a join predicate. This remains separate from rows emitted
    /// at relational operator boundaries.
    pub max_candidate_work: usize,
    pub hydration: RelationalHydrationBudget,
    pub index_read: skein_storage::RelationalIndexReadLimits,
    pub row_read: skein_storage::RelationalRowPageSnapshotReadLimits,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RelationalQueryResourceContext<'a> {
    join_planning: RelationalJoinPlanningContext,
    limits: RelationalQueryLimits,
    execution_memory: &'a skein_executor::ExecutionMemoryConfig,
    task_context: Option<&'a skein_core::RuntimeTaskContext>,
}

impl<'a> RelationalQueryResourceContext<'a> {
    pub(crate) const fn new(
        join_enumeration: RelationalJoinEnumerationConfig,
        limits: RelationalQueryLimits,
        execution_memory: &'a skein_executor::ExecutionMemoryConfig,
        task_context: Option<&'a skein_core::RuntimeTaskContext>,
    ) -> Self {
        Self {
            join_planning: RelationalJoinPlanningContext::new(
                join_enumeration,
                RelationalJoinPlanningDirective::Auto,
            ),
            limits,
            execution_memory,
            task_context,
        }
    }

    pub(crate) const fn with_join_planning(
        mut self,
        join_planning: RelationalJoinPlanningDirective,
    ) -> Self {
        self.join_planning.directive = join_planning;
        self
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct RelationalJoinPlanningContext {
    enumeration: RelationalJoinEnumerationConfig,
    directive: RelationalJoinPlanningDirective,
}

impl RelationalJoinPlanningContext {
    const fn new(
        enumeration: RelationalJoinEnumerationConfig,
        directive: RelationalJoinPlanningDirective,
    ) -> Self {
        Self {
            enumeration,
            directive,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelationalQueryOutput {
    pub rows: QueryRows,
    pub stage_timings: RelationalSqlStageTimings,
    pub join_planning: RelationalJoinPlanningOutcome,
    pub operator_cardinality_profiles: Vec<RelationalOperatorCardinalityProfile>,
    pub intermediate_rows: usize,
    pub hydration: RelationalHydrationBudget,
    pub access_path: RelationalAccessPathDescriptor,
    pub join_access_paths: Vec<RelationalAccessPathDescriptor>,
    pub index_execution_evidence: Vec<RelationalIndexExecutionEvidence>,
    pub row_execution_evidence: RelationalRowExecutionEvidence,
    pub blocking_operator_memory_reports: Vec<BlockingOperatorMemoryReport>,
}

struct AdmittedRelationalExecution<'state, 'runtime> {
    state: &'state RelationalState,
    index_read_mode: RelationalIndexReadMode<'state>,
    row_read_mode: RelationalRowReadMode<'state>,
    limits: RelationalQueryLimits,
    execution_memory: &'runtime skein_executor::ExecutionMemoryConfig,
    memory_ledger: QueryMemoryLedger,
    task_context: Option<&'runtime skein_core::RuntimeTaskContext>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RelationalQueryReadModes<'a> {
    index: RelationalIndexReadMode<'a>,
    row: RelationalRowReadMode<'a>,
}

impl<'a> RelationalQueryReadModes<'a> {
    pub(crate) const fn new(
        index: RelationalIndexReadMode<'a>,
        row: RelationalRowReadMode<'a>,
    ) -> Self {
        Self { index, row }
    }
}

pub(crate) fn execute_prepared_relational_query_with_resources<'a>(
    prepared_sql: PreparedRelationalSql,
    parameters: &[Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a>,
    resources: RelationalQueryResourceContext<'_>,
) -> Result<RelationalQueryOutput> {
    let bind_started = Instant::now();
    let parse_nanos = prepared_sql.parse_nanos;
    let prepared = Arc::unwrap_or_clone(prepared_sql.template);
    if prepared.parameters.len() != parameters.len() {
        return Err(SkeinError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    let initial_stage_timings = RelationalSqlStageTimings {
        parse_nanos,
        bind_nanos: elapsed_nanos(bind_started),
        ..RelationalSqlStageTimings::default()
    };
    match prepared.statement {
        SqlStatement::Select(select) => {
            let prepared = prepare_relational_select(
                select,
                parameters,
                state,
                read_modes,
                resources.limits,
                resources.join_planning,
                initial_stage_timings,
            )?;
            let execution = prepared.execution.admit(state, read_modes, resources)?;
            execute_select_timed(&prepared, parameters, execution)
        }
        SqlStatement::Explain(explain) => {
            let SqlStatement::Select(select) = *explain.statement else {
                return Err(SkeinError::Semantic(
                    "EXPLAIN only supports relational SELECT".to_string(),
                ));
            };
            let prepared = prepare_relational_select(
                select,
                parameters,
                state,
                read_modes,
                resources.limits,
                resources.join_planning,
                initial_stage_timings,
            )?;
            if !explain.analyze {
                return explain_select(&prepared, parameters, resources.limits);
            }
            let execution = prepared.execution.admit(state, read_modes, resources)?;
            let output = execute_select_timed(&prepared, parameters, execution)?;
            format_relational_explain(
                &prepared.statement,
                parameters,
                output,
                true,
                resources.limits,
            )
        }
        _ => Err(SkeinError::Semantic(
            "relational query entrypoint requires SELECT or EXPLAIN SELECT".to_string(),
        )),
    }
}

#[cfg(test)]
pub(crate) fn execute_relational_query_sql_with_runtime<'a>(
    sql: &str,
    parameters: &[Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a>,
    limits: RelationalQueryLimits,
    execution_memory: &skein_executor::ExecutionMemoryConfig,
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<RelationalQueryOutput> {
    let started = Instant::now();
    let template = Arc::new(skein_sql::prepare_postgres_sql(sql)?);
    let prepared_sql = PreparedRelationalSql {
        template,
        parse_nanos: elapsed_nanos(started),
    };
    execute_prepared_relational_query_with_resources(
        prepared_sql,
        parameters,
        state,
        read_modes,
        RelationalQueryResourceContext::new(
            RelationalJoinEnumerationConfig::default(),
            limits,
            execution_memory,
            task_context,
        ),
    )
}

#[derive(Clone)]
struct Binding<'a> {
    binding: BindingId,
    table: &'a str,
    qualifier: &'a str,
    schema: &'a RelationalTableSchema,
    row: Option<RelationalReadRow>,
}

impl Binding<'_> {
    fn primary_key(&self) -> Option<&RelationalKey> {
        self.row.as_ref().map(RelationalReadRow::primary_key)
    }

    fn value(&self, ordinal: usize) -> Result<&RelationalValue> {
        match &self.row {
            Some(row) => row.value(ordinal),
            None => Ok(&RelationalValue::Null),
        }
    }
}

#[derive(Clone, Default)]
struct BoundRow<'a> {
    bindings: Vec<Binding<'a>>,
}
