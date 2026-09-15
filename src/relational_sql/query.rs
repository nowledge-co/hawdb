use super::{
    RelationalJoinPlanningOutcome, RelationalOperatorCardinalityProfile, RelationalOperatorId,
};
use crate::error::{Result, SkeinError};
use crate::executor::{map_payload_bytes, Row};
use crate::relational_sql::index_access::{RelationalIndexReadMode, RelationalIndexRuntime};
use crate::relational_sql::row_access::{
    RelationalReadRow, RelationalRowExecutionEvidence, RelationalRowReadMode, RelationalRowRuntime,
};
use crate::sql::{
    SelectProjection, SelectStatement, SqlColumnRef, SqlExpression, SqlFunctionArgument,
    SqlJoinKind, SqlNullOrder, SqlOrderDirection, SqlPredicate, SqlStatement, SqlValue,
};
use crate::value::Value;
use skein_core::Catalog;
use skein_executor::binding::Binding as ExecutorBinding;
use skein_executor::blocking::{
    stream_distinct_batches, stream_top_n_batches, BindingBatchSource, BlockingExecutionContext,
};
use skein_executor::external_order::ExternalTopN;
use skein_executor::kernel::{OperatorMemoryTracker, SpillBudgetTracker};
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
    select_relational_access_path, RelationalAccessPathDescriptor, RelationalAccessPathKind,
    RelationalJoinEnumerationConfig, RelationalJoinPlanningDirective,
};
use skein_plan::{PhysicalPlan, SortDirection, SortItem, SortKey};
use skein_relational::field_plan::{
    order_by_uses_expression_alias, plan_relational_field_plan, projection_contains_aggregate,
};
#[cfg(test)]
use skein_storage::RelationalHydrationBudget;
use skein_storage::{
    relational_unique_index_name, RelationalIndexRangeScan, RelationalIndexScanDirection,
    RelationalKey, RelationalScalarType, RelationalState, RelationalTableSchema, RelationalValue,
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

mod join_order;
mod locator;
mod streaming_binding;

use self::locator::{
    RelationalLocatorLayout, RelationalRowSetLocator, RelationalSortKey, RelationalSortRecord,
};
use self::streaming_binding::BoundStreamingProjection;
use skein_relational::columnar_aggregate::ColumnarAggregateExecutor;
use skein_relational::predicate::BoundStreamingPredicate;

mod access;
#[cfg(test)]
use access::RelationalProjectionAccessPlanning;
use access::{
    bound_join_key, choose_base_access, choose_join_access, collect_conjunctive_join_equalities,
    predicate_is_covered_by_access, projection_access_planning, visit_base_entries,
    visit_join_entries, RelationalBaseAccessPlanning,
};

mod aggregate;
use aggregate::execute_aggregate_select;

use skein_relational::aggregate as having;
use skein_relational::aggregate::{
    aggregate_group_base_memory_bytes, charge_aggregate_memory, AggregateProjectionState,
};

mod execution;
#[cfg(test)]
use execution::execute_select;
use execution::{execute_select_timed, explain_select};

mod explain;
use explain::format_relational_explain;

mod expression;
use expression::{
    account_intermediate, aggregate_filter_matches, bind_bound, bind_sql_value, predicate_truth,
    project_bound_row, projection_uses_non_aggregate_coalesce, reject_non_public_schema,
    relational_ref_to_value, resolve_column, validate_non_aggregate_coalesce_projections,
    value_to_relational_as,
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
    RelationalBaseAccess, RelationalEquiJoinKeys, RelationalExecutionAdmission,
    RelationalJoinAccess, RelationalJoinAccessCandidate, RelationalPhysicalAccess,
    RelationalPhysicalJoinAlgorithm, RelationalPhysicalJoinNode, RelationalPhysicalJoinPlan,
    RelationalPhysicalOutputSchema, RelationalPhysicalRelation,
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
use preparation::{prepare_relational_select, prepared_access_descriptors};

mod projection;
use projection::{
    execute_blocking_projection, push_relational_output, relational_input_plan,
    DistinctAggregateValueBatchSource, RelationalBlockingObserver, StreamingProjectionOutput,
};

mod streaming_projection;
use streaming_projection::{execute_ordered_index_projection, execute_streaming_projection};

#[cfg(test)]
mod tests;

pub(crate) use skein_relational::query_output::{RelationalQueryLimits, RelationalQueryOutput};

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

impl BoundRow<'_> {
    fn schema_bindings(
        &self,
    ) -> impl ExactSizeIterator<
        Item = skein_relational::physical_plan::RelationalPhysicalOutputBindingRef<'_>,
    > {
        self.bindings.iter().map(|binding| {
            skein_relational::physical_plan::RelationalPhysicalOutputBindingRef {
                binding: binding.binding,
                table: binding.table,
                qualifier: binding.qualifier,
            }
        })
    }
}
