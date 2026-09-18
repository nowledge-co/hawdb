// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::field_plan::{
    order_by_uses_expression_alias, plan_relational_field_plan, projection_contains_aggregate,
    resolve_relational_order_target, RelationalOrderTarget,
};
use crate::index_runtime::{
    RelationalIndexReadMode, RelationalIndexRuntime, RelationalIndexStoreReader,
};
use crate::row_runtime::{
    RelationalReadRow, RelationalRowExecutionEvidence, RelationalRowReadMode, RelationalRowRuntime,
    RelationalRowStoreReader,
};
use hawdb_core::{Catalog, HawDBError, Result, Value};
use hawdb_executor::binding::map_payload_bytes;
use hawdb_executor::binding::Binding as ExecutorBinding;
use hawdb_executor::blocking::{
    stream_distinct_batches, stream_top_n_batches, BindingBatchSource, BlockingExecutionContext,
};
use hawdb_executor::external_order::ExternalTopN;
use hawdb_executor::kernel::{OperatorMemoryTracker, SpillBudgetTracker};
use hawdb_executor::observer::ExecutionObserver;
use hawdb_executor::pipeline::{AccountedBindingBatch, BatchControl, BindingBatch};
use hawdb_executor::spill::{SpillRun, SpillWriter};
use hawdb_executor::Row;
use hawdb_executor::{
    BindingSchema, BlockingOperatorMemoryReport, ColumnVector, ColumnarBatch, ExecutionLimit,
    QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger, QueryRows, QueryRowsBuilder,
    RelationalRowLocator, SlotDescriptor, SlotId, SlotType,
};
use hawdb_expression::BindingId;
use hawdb_optimizer::{
    select_relational_access_path, RelationalAccessPathDescriptor, RelationalAccessPathKind,
    RelationalJoinEnumerationConfig, RelationalJoinPlanningDirective,
};
use hawdb_plan::{PhysicalPlan, SortDirection, SortItem, SortKey};
use hawdb_sql::{
    SelectProjection, SelectStatement, SqlColumnRef, SqlExpression, SqlFunctionArgument,
    SqlJoinKind, SqlNullOrder, SqlOrderDirection, SqlPredicate, SqlStatement, SqlValue,
};
#[cfg(test)]
use hawdb_storage::RelationalHydrationBudget;
use hawdb_storage::{
    relational_unique_index_name, RelationalIndexRangeScan, RelationalIndexScanDirection,
    RelationalKey, RelationalScalarType, RelationalState, RelationalTableSchema, RelationalValue,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use hawdb_optimizer::{
    RelationalJoinPlanningOutcome, RelationalOperatorCardinalityProfile, RelationalOperatorId,
};
use hawdb_sql::timing::{elapsed_nanos, measure_nanos};
use hawdb_sql::{PreparedRelationalSql, RelationalSqlStageTimings};

mod join_order;
use crate::columnar_aggregate::ColumnarAggregateExecutor;
pub(super) use crate::locator::{
    RelationalLocatorLayout, RelationalRowSetLocator, RelationalSortKey, RelationalSortRecord,
};
use crate::predicate::BoundStreamingPredicate;
use crate::streaming_projection::BoundStreamingProjection;

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

use crate::aggregate as having;
use crate::aggregate::{
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
    resolve_column, validate_non_aggregate_coalesce_projections, value_to_relational_as,
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
    PreparedRelationalSelect, RelationalAccessCandidate, RelationalBaseAccess,
    RelationalEquiJoinKeys, RelationalExecutionAdmission, RelationalJoinAccess,
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

pub use crate::query_output::{RelationalQueryLimits, RelationalQueryOutput};

/// Storage view required by the storage-neutral relational query runtime.
///
/// The embedded facade chooses and pins the concrete reader before execution.
/// This combines the existing row and index seams solely to keep one query
/// execution bound to one coherent storage view.
#[doc(hidden)]
pub trait RelationalQueryStoreReader:
    RelationalIndexStoreReader + RelationalRowStoreReader
{
}

impl<T> RelationalQueryStoreReader for T where
    T: RelationalIndexStoreReader + RelationalRowStoreReader
{
}

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub struct RelationalQueryResourceContext<'a> {
    join_planning: RelationalJoinPlanningContext,
    limits: RelationalQueryLimits,
    execution_memory: &'a hawdb_executor::ExecutionMemoryConfig,
    task_context: Option<&'a hawdb_core::RuntimeTaskContext>,
}

impl<'a> RelationalQueryResourceContext<'a> {
    pub const fn new(
        join_enumeration: RelationalJoinEnumerationConfig,
        limits: RelationalQueryLimits,
        execution_memory: &'a hawdb_executor::ExecutionMemoryConfig,
        task_context: Option<&'a hawdb_core::RuntimeTaskContext>,
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

    pub const fn with_join_planning(
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

struct AdmittedRelationalExecution<'state, 'runtime, R: RelationalQueryStoreReader> {
    state: &'state RelationalState,
    index_read_mode: RelationalIndexReadMode<'state, R>,
    row_read_mode: RelationalRowReadMode<'state, R>,
    limits: RelationalQueryLimits,
    execution_memory: &'runtime hawdb_executor::ExecutionMemoryConfig,
    memory_ledger: QueryMemoryLedger,
    task_context: Option<&'runtime hawdb_core::RuntimeTaskContext>,
}

pub struct RelationalQueryReadModes<
    'a,
    R: RelationalQueryStoreReader = crate::RelationalMaterializedReader,
> {
    index: RelationalIndexReadMode<'a, R>,
    row: RelationalRowReadMode<'a, R>,
}

impl<R: RelationalQueryStoreReader> Copy for RelationalQueryReadModes<'_, R> {}

impl<R: RelationalQueryStoreReader> Clone for RelationalQueryReadModes<'_, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, R: RelationalQueryStoreReader> RelationalQueryReadModes<'a, R> {
    pub const fn new(
        index: RelationalIndexReadMode<'a, R>,
        row: RelationalRowReadMode<'a, R>,
    ) -> Self {
        Self { index, row }
    }
}

#[cfg(any())]
impl RelationalIndexStoreReader for crate::RelationalMaterializedReader {
    fn relational_index_probe_statistics(
        &self,
        _table: &str,
        _index: &str,
        _prefix_len: usize,
    ) -> Option<hawdb_storage::relational_index_view::RelationalIndexProbeStatistics> {
        None
    }

    fn visit_relational_index_read_view_prefix_entries(
        &self,
        _table: &str,
        _index: &str,
        _prefix: &RelationalKey,
        _limits: hawdb_storage::RelationalIndexReadLimits,
        _visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<
        std::result::Result<
            hawdb_storage::relational_index_view::RelationalIndexReadViewReport,
            hawdb_storage::RelationalIndexShadowError,
        >,
    > {
        None
    }

    fn visit_relational_index_read_view_prefix_entries_many(
        &self,
        _table: &str,
        _index: &str,
        _prefixes: &[RelationalKey],
        _limits: hawdb_storage::RelationalIndexReadLimits,
        _visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<
        std::result::Result<
            hawdb_storage::relational_index_view::RelationalIndexReadViewReport,
            hawdb_storage::RelationalIndexShadowError,
        >,
    > {
        None
    }

    fn visit_relational_index_read_view_range_entries(
        &self,
        _table: &str,
        _index: &str,
        _scan: &hawdb_storage::RelationalIndexRangeScan,
        _limits: hawdb_storage::RelationalIndexReadLimits,
        _visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<
        std::result::Result<
            hawdb_storage::relational_index_view::RelationalIndexReadViewReport,
            hawdb_storage::RelationalIndexShadowError,
        >,
    > {
        None
    }
}

#[cfg(any())]
impl RelationalRowStoreReader for crate::RelationalMaterializedReader {
    type TransactionRows = ();

    fn open_relational_row_snapshot_reader(
        &self,
    ) -> Result<Option<hawdb_storage::RelationalRowPageSnapshotReader>> {
        Ok(None)
    }

    fn open_relational_transaction_row_snapshot_reader(
        &self,
        _rows: &Self::TransactionRows,
    ) -> Result<hawdb_storage::RelationalRowPageSnapshotReader> {
        Err(HawDBError::Execution(
            "materialized relational reader does not expose transaction rows".to_string(),
        ))
    }
}

pub fn execute_prepared_relational_query_with_resources<'a, R: RelationalQueryStoreReader>(
    prepared_sql: PreparedRelationalSql,
    parameters: &[Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'_>,
) -> Result<RelationalQueryOutput> {
    let bind_started = Instant::now();
    let parse_nanos = prepared_sql.parse_nanos;
    let prepared = Arc::unwrap_or_clone(prepared_sql.template);
    if prepared.parameters.len() != parameters.len() {
        return Err(HawDBError::Semantic(format!(
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
                return Err(HawDBError::Semantic(
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
        _ => Err(HawDBError::Semantic(
            "relational query entrypoint requires SELECT or EXPLAIN SELECT".to_string(),
        )),
    }
}

#[doc(hidden)]
pub fn execute_relational_query_sql_with_runtime<'a, R: RelationalQueryStoreReader>(
    sql: &str,
    parameters: &[Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    limits: RelationalQueryLimits,
    execution_memory: &hawdb_executor::ExecutionMemoryConfig,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<RelationalQueryOutput> {
    let started = Instant::now();
    let template = Arc::new(hawdb_sql::prepare_postgres_sql(sql)?);
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
    ) -> impl ExactSizeIterator<Item = crate::physical_plan::RelationalPhysicalOutputBindingRef<'_>>
    {
        self.bindings.iter().map(|binding| {
            crate::physical_plan::RelationalPhysicalOutputBindingRef {
                binding: binding.binding,
                table: binding.table,
                qualifier: binding.qualifier,
            }
        })
    }
}
