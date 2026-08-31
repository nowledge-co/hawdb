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
    SqlFunctionArgument, SqlJoinKind, SqlNullOrder, SqlOrderDirection, SqlPredicate, SqlStatement,
    SqlValue,
};
use crate::value::Value;
use skein_core::Catalog;
use skein_executor::binding::Binding as ExecutorBinding;
use skein_executor::blocking::{
    stream_distinct_batches, stream_top_n_batches, BindingBatchSource, BlockingExecutionContext,
};
use skein_executor::external_order::ExternalTopN;
use skein_executor::kernel::{ensure_operator_item_fits, OperatorMemoryTracker};
use skein_executor::observer::ExecutionObserver;
use skein_executor::pipeline::{BatchControl, BindingBatch};
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
    RelationalJoinEnumerationConfig, RelationalJoinRightInput,
};
use skein_plan::{PhysicalPlan, SortDirection, SortItem, SortKey};
use skein_storage::{
    relational_unique_index_name, RelationalHydrationBudget, RelationalIndexRangeScan,
    RelationalIndexScanDirection, RelationalKey, RelationalState, RelationalTableSchema,
    RelationalValue, RelationalValueRef,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use super::template_cache::PreparedRelationalSql;
use super::timing::{elapsed_nanos, measure_nanos};
use super::RelationalSqlStageTimings;

mod columnar_aggregate;
mod join_order;
mod locator;
mod streaming_binding;

use self::columnar_aggregate::ColumnarAggregateExecutor;
use self::locator::{
    RelationalLocatorLayout, RelationalRowSetLocator, RelationalSortKey, RelationalSortRecord,
};
use self::streaming_binding::{BoundStreamingPredicate, BoundStreamingProjection};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RelationalQueryLimits {
    pub max_output_rows: usize,
    pub max_output_payload_bytes: usize,
    pub max_intermediate_rows: usize,
    pub hydration: RelationalHydrationBudget,
    pub index_read: skein_storage::RelationalIndexReadLimits,
    pub row_read: skein_storage::RelationalRowPageSnapshotReadLimits,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RelationalQueryResourceContext<'a> {
    join_enumeration: RelationalJoinEnumerationConfig,
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
            join_enumeration,
            limits,
            execution_memory,
            task_context,
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
                read_modes.index,
                resources.limits,
                resources.join_enumeration,
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
                read_modes.index,
                resources.limits,
                resources.join_enumeration,
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

#[derive(Debug, Clone)]
enum RelationalBaseAccess {
    PrimaryKey(RelationalKey),
    Index {
        name: String,
        scan: RelationalIndexRangeScan,
    },
    FullScan,
}

#[derive(Debug, Clone)]
enum RelationalJoinAccess {
    PrimaryKey(Vec<(String, SqlColumnRef)>),
    Index {
        name: String,
        columns: Vec<(String, SqlColumnRef)>,
    },
    FullScan,
}

#[derive(Debug, Clone)]
struct RelationalAccessCandidate {
    descriptor: RelationalAccessPathDescriptor,
    access: RelationalBaseAccess,
}

#[derive(Debug, Clone)]
struct RelationalJoinAccessCandidate {
    descriptor: RelationalAccessPathDescriptor,
    access: RelationalJoinAccess,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedRelationalJoinSelection {
    base_binding: BindingId,
    join_bindings: Vec<BindingId>,
    cost_breakdown: PlanCostBreakdown,
}

#[derive(Debug, Clone)]
enum PreparedRelationalTreeAccess {
    Base(RelationalAccessCandidate),
    Probe(RelationalJoinAccessCandidate),
}

impl PreparedRelationalTreeAccess {
    fn descriptor(&self) -> &RelationalAccessPathDescriptor {
        match self {
            Self::Base(access) => &access.descriptor,
            Self::Probe(access) => &access.descriptor,
        }
    }
}

#[derive(Debug, Clone)]
struct PreparedRelationalTreeRelation {
    binding: BindingId,
    table: String,
    qualifier: String,
    access: PreparedRelationalTreeAccess,
}

#[derive(Debug, Clone)]
enum PreparedRelationalJoinTreeNode {
    Relation(PreparedRelationalTreeRelation),
    Join {
        operator_id: RelationalOperatorId,
        kind: SqlJoinKind,
        predicates: Vec<SqlPredicate>,
        left: Box<Self>,
        right: Box<Self>,
    },
}

impl PreparedRelationalJoinTreeNode {
    fn first_relation(&self) -> &PreparedRelationalTreeRelation {
        match self {
            Self::Relation(relation) => relation,
            Self::Join { left, .. } => left.first_relation(),
        }
    }

    fn visit_relations<'a>(&'a self, visit: &mut impl FnMut(&'a PreparedRelationalTreeRelation)) {
        match self {
            Self::Relation(relation) => visit(relation),
            Self::Join { left, right, .. } => {
                left.visit_relations(visit);
                right.visit_relations(visit);
            }
        }
    }

    fn relation_count(&self) -> usize {
        let mut count = 0usize;
        self.visit_relations(&mut |_| count = count.saturating_add(1));
        count
    }

    fn visit_join_right_relations<'a>(
        &'a self,
        visit: &mut impl FnMut(&'a PreparedRelationalTreeRelation),
    ) {
        if let Self::Join { left, right, .. } = self {
            left.visit_join_right_relations(visit);
            right.visit_join_right_relations(visit);
            visit(right.first_relation());
        }
    }

    fn materialized_right_count(&self) -> usize {
        match self {
            Self::Relation(_) => 0,
            Self::Join { left, right, .. } => {
                usize::from(matches!(right.as_ref(), Self::Join { .. }))
                    .saturating_add(left.materialized_right_count())
                    .saturating_add(right.materialized_right_count())
            }
        }
    }
}

#[derive(Debug, Clone)]
struct PreparedRelationalJoinTree {
    root: PreparedRelationalJoinTreeNode,
    cost_breakdown: PlanCostBreakdown,
}

#[derive(Debug)]
struct PreparedRelationalAccessPlan {
    base_access: RelationalAccessCandidate,
    join_accesses: Vec<RelationalJoinAccessCandidate>,
    join_selection: Option<PreparedRelationalJoinSelection>,
    join_tree: Option<PreparedRelationalJoinTree>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparedRelationalExecutionMode {
    OrderedIndexProjection,
    StreamingProjection,
    BlockingProjection,
    Aggregate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RelationalExecutionMemoryShape {
    /// Maximum concurrently retained executor transfer batches.
    pipeline_batch_count: usize,
    /// Maximum concurrently retained blocking operator states.
    blocking_operator_count: usize,
}

impl RelationalExecutionMemoryShape {
    fn estimated_bytes(self, memory: &skein_executor::ExecutionMemoryConfig) -> usize {
        self.pipeline_batch_count
            .saturating_mul(memory.batch_payload_bytes.get())
            .saturating_add(
                self.blocking_operator_count
                    .saturating_mul(memory.blocking_operator_bytes.get()),
            )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedRelationalExecutionDescriptor {
    /// The execution path selected during preparation. Execution must not
    /// independently infer a different path from the SQL statement.
    mode: PreparedRelationalExecutionMode,
    /// Plan-derived peak shape resolved against runtime memory ceilings during
    /// admission.
    memory_shape: RelationalExecutionMemoryShape,
}

impl PreparedRelationalExecutionDescriptor {
    fn prepare(select: &SelectStatement, access_plan: &PreparedRelationalAccessPlan) -> Self {
        let has_aggregate = select.projection.iter().any(projection_contains_aggregate);
        let ordered_index_projection = !select.order_by.is_empty()
            && access_plan.base_access.descriptor.order_prefix_len == select.order_by.len()
            && select.joins.is_empty()
            && !select.distinct
            && !has_aggregate
            && select.group_by.is_empty()
            && predicate_is_covered_by_access(
                select.selection.as_ref(),
                &access_plan.base_access.descriptor,
                &select.order_by,
                &select.from.name,
                select.from_alias.as_deref().unwrap_or(&select.from.name),
            );
        let mode = if ordered_index_projection {
            PreparedRelationalExecutionMode::OrderedIndexProjection
        } else if has_aggregate || !select.group_by.is_empty() {
            PreparedRelationalExecutionMode::Aggregate
        } else if !select.order_by.is_empty() || select.distinct {
            PreparedRelationalExecutionMode::BlockingProjection
        } else {
            PreparedRelationalExecutionMode::StreamingProjection
        };
        let blocking_operator_count = match mode {
            PreparedRelationalExecutionMode::OrderedIndexProjection
            | PreparedRelationalExecutionMode::StreamingProjection => 0,
            PreparedRelationalExecutionMode::BlockingProjection => {
                usize::from(select.distinct) + usize::from(!select.order_by.is_empty())
            }
            PreparedRelationalExecutionMode::Aggregate => {
                if !select.group_by.is_empty() {
                    2
                } else {
                    1
                }
            }
        }
        .saturating_add(
            access_plan
                .join_tree
                .as_ref()
                .map_or(0, |tree| tree.root.materialized_right_count()),
        );
        Self {
            mode,
            memory_shape: RelationalExecutionMemoryShape {
                pipeline_batch_count: 1,
                blocking_operator_count,
            },
        }
    }

    fn admit<'state, 'runtime>(
        self,
        state: &'state RelationalState,
        read_modes: RelationalQueryReadModes<'state>,
        resources: RelationalQueryResourceContext<'runtime>,
    ) -> Result<AdmittedRelationalExecution<'state, 'runtime>> {
        skein_executor::pipeline::runtime_checkpoint(resources.task_context)?;
        let estimated_bytes = self
            .memory_shape
            .estimated_bytes(resources.execution_memory);
        if estimated_bytes > resources.execution_memory.query_memory_bytes.get() {
            return Err(SkeinError::Execution(format!(
                "prepared relational query requires {estimated_bytes} estimated bytes, exceeding query_memory_bytes {}",
                resources.execution_memory.query_memory_bytes
            )));
        }
        Ok(AdmittedRelationalExecution {
            state,
            index_read_mode: read_modes.index,
            row_read_mode: read_modes.row,
            limits: resources.limits,
            execution_memory: resources.execution_memory,
            memory_ledger: QueryMemoryLedger::new(resources.execution_memory.query_memory_bytes),
            task_context: resources.task_context,
        })
    }
}

#[derive(Debug)]
struct PreparedRelationalSelect {
    statement: SelectStatement,
    access_plan: PreparedRelationalAccessPlan,
    join_planning: RelationalJoinPlanningOutcome,
    execution: PreparedRelationalExecutionDescriptor,
    stage_timings: RelationalSqlStageTimings,
}

impl PreparedRelationalSelect {
    fn validate(&self) -> Result<()> {
        if self.statement.joins.len() != self.access_plan.join_accesses.len() {
            return Err(SkeinError::Execution(format!(
                "prepared relational SELECT has {} joins but {} join access paths",
                self.statement.joins.len(),
                self.access_plan.join_accesses.len()
            )));
        }
        if !base_access_matches_descriptor(&self.access_plan.base_access) {
            return Err(SkeinError::Execution(
                "prepared relational SELECT has an inconsistent base access path".to_string(),
            ));
        }
        if self
            .access_plan
            .join_accesses
            .iter()
            .any(|access| !join_access_matches_descriptor(access))
        {
            return Err(SkeinError::Execution(
                "prepared relational SELECT has an inconsistent join access path".to_string(),
            ));
        }
        if let Some(tree) = &self.access_plan.join_tree {
            if tree.root.relation_count() != self.statement.joins.len().saturating_add(1) {
                return Err(SkeinError::Execution(format!(
                    "prepared CSG-CMP tree has {} relations for a {}-join SELECT",
                    tree.root.relation_count(),
                    self.statement.joins.len()
                )));
            }
            let mut bindings = BTreeSet::new();
            let mut duplicate = None;
            tree.root.visit_relations(&mut |relation| {
                if !bindings.insert(relation.binding) {
                    duplicate = Some(relation.binding);
                }
            });
            if let Some(binding) = duplicate {
                return Err(SkeinError::Execution(format!(
                    "prepared CSG-CMP tree repeats binding {}",
                    binding.get()
                )));
            }
            validate_prepared_join_tree_accesses(&tree.root, true)?;
            planned_tree_operator_cardinality_profiles(tree)?;
        }
        if let Some(selection) = &self.access_plan.join_selection {
            if selection.join_bindings.len() != self.statement.joins.len() {
                return Err(SkeinError::Execution(format!(
                    "prepared relational join selection has {} bindings for {} joins",
                    selection.join_bindings.len(),
                    self.statement.joins.len()
                )));
            }
            let mut bindings = BTreeSet::from([selection.base_binding]);
            if selection
                .join_bindings
                .iter()
                .any(|binding| !bindings.insert(*binding))
            {
                return Err(SkeinError::Execution(
                    "prepared relational join selection contains duplicate bindings".to_string(),
                ));
            }
            let cost = selection.cost_breakdown;
            let component_total = cost
                .cpu
                .saturating_add(cost.random_io)
                .saturating_add(cost.sequential_io)
                .saturating_add(cost.output_rows);
            if cost.estimated_rows == 0 || cost.cost != component_total {
                return Err(SkeinError::Execution(
                    "prepared relational join selection has an invalid cost breakdown".to_string(),
                ));
            }
        }
        if self.execution
            != PreparedRelationalExecutionDescriptor::prepare(&self.statement, &self.access_plan)
        {
            return Err(SkeinError::Execution(
                "prepared relational SELECT has an inconsistent execution descriptor".to_string(),
            ));
        }
        Ok(())
    }
}

fn validate_prepared_join_tree_accesses(
    node: &PreparedRelationalJoinTreeNode,
    requires_base: bool,
) -> Result<()> {
    match node {
        PreparedRelationalJoinTreeNode::Relation(relation) => match &relation.access {
            PreparedRelationalTreeAccess::Base(access)
                if requires_base && base_access_matches_descriptor(access) =>
            {
                Ok(())
            }
            PreparedRelationalTreeAccess::Probe(access)
                if !requires_base && join_access_matches_descriptor(access) =>
            {
                Ok(())
            }
            _ => Err(SkeinError::Execution(format!(
                "prepared CSG-CMP relation {} has an invalid access role",
                relation.qualifier
            ))),
        },
        PreparedRelationalJoinTreeNode::Join {
            predicates,
            left,
            right,
            ..
        } => {
            if predicates.is_empty() {
                return Err(SkeinError::Execution(
                    "prepared CSG-CMP join has no predicate".to_string(),
                ));
            }
            validate_prepared_join_tree_accesses(left, true)?;
            validate_prepared_join_tree_accesses(
                right,
                !matches!(right.as_ref(), PreparedRelationalJoinTreeNode::Relation(_)),
            )
        }
    }
}

fn base_access_matches_descriptor(candidate: &RelationalAccessCandidate) -> bool {
    match (&candidate.descriptor.kind, &candidate.access) {
        (RelationalAccessPathKind::PrimaryKey, RelationalBaseAccess::PrimaryKey(key)) => {
            candidate.descriptor.equality_prefix_len == key.0.len()
                && candidate.descriptor.access_columns
                    == candidate.descriptor.index_columns.iter().cloned().collect()
        }
        (RelationalAccessPathKind::Index, RelationalBaseAccess::Index { name, scan }) => {
            candidate.descriptor.name == *name
                && candidate.descriptor.equality_prefix_len == scan.prefix.0.len()
        }
        (RelationalAccessPathKind::FullScan, RelationalBaseAccess::FullScan) => {
            candidate.descriptor.equality_prefix_len == 0
        }
        _ => false,
    }
}

fn join_access_matches_descriptor(candidate: &RelationalJoinAccessCandidate) -> bool {
    match (&candidate.descriptor.kind, &candidate.access) {
        (RelationalAccessPathKind::PrimaryKey, RelationalJoinAccess::PrimaryKey(columns)) => {
            candidate.descriptor.equality_prefix_len == columns.len()
                && candidate.descriptor.access_columns
                    == columns.iter().map(|(column, _)| column.clone()).collect()
        }
        (RelationalAccessPathKind::Index, RelationalJoinAccess::Index { name, columns }) => {
            candidate.descriptor.name == *name
                && candidate.descriptor.equality_prefix_len == columns.len()
                && candidate.descriptor.access_columns
                    == columns.iter().map(|(column, _)| column.clone()).collect()
        }
        (RelationalAccessPathKind::FullScan, RelationalJoinAccess::FullScan) => {
            candidate.descriptor.equality_prefix_len == 0
        }
        _ => false,
    }
}

fn planned_operator_cardinality_profiles(
    prepared: &PreparedRelationalSelect,
) -> Result<Vec<RelationalOperatorCardinalityProfile>> {
    if let Some(tree) = &prepared.access_plan.join_tree {
        return planned_tree_operator_cardinality_profiles(tree);
    }
    let mut cost =
        estimate_relational_access_cost(prepared.access_plan.base_access.descriptor.estimated_rows);
    let mut profiles = Vec::with_capacity(prepared.statement.joins.len().saturating_add(1));
    profiles.push(RelationalOperatorCardinalityProfile {
        operator_id: RelationalOperatorId::from_plan_index(0),
        operator: match prepared.access_plan.base_access.descriptor.kind {
            RelationalAccessPathKind::FullScan => RelationalOperatorKind::TableFullScan,
            RelationalAccessPathKind::PrimaryKey => RelationalOperatorKind::TablePointGet,
            RelationalAccessPathKind::Index => RelationalOperatorKind::IndexRangeScan,
        },
        table: prepared.statement.from.name.clone(),
        estimated_rows: estimated_rows_as_usize(cost.estimated_rows),
        actual_rows: None,
        fully_consumed: false,
    });
    for (index, (join, access)) in prepared
        .statement
        .joins
        .iter()
        .zip(&prepared.access_plan.join_accesses)
        .enumerate()
    {
        let cardinality = match join.kind {
            SqlJoinKind::Inner => RelationalJoinCardinality::Inner,
            SqlJoinKind::Left => RelationalJoinCardinality::PreserveLeft,
        };
        cost = estimate_relational_probe_join_cost(
            cost,
            access.descriptor.estimated_rows,
            cardinality,
        );
        profiles.push(RelationalOperatorCardinalityProfile {
            operator_id: RelationalOperatorId::from_plan_index(index.saturating_add(1)),
            operator: match join.kind {
                SqlJoinKind::Inner => RelationalOperatorKind::IndexNestedLoopJoin,
                SqlJoinKind::Left => RelationalOperatorKind::IndexNestedLoopLeftJoin,
            },
            table: join.table.name.clone(),
            estimated_rows: estimated_rows_as_usize(cost.estimated_rows),
            actual_rows: None,
            fully_consumed: false,
        });
    }
    if let Some(selection) = &prepared.access_plan.join_selection
        && cost != selection.cost_breakdown
    {
        return Err(SkeinError::Execution(
            "prepared relational operator estimates diverge from the selected join cost"
                .to_string(),
        ));
    }
    Ok(profiles)
}

fn planned_tree_operator_cardinality_profiles(
    tree: &PreparedRelationalJoinTree,
) -> Result<Vec<RelationalOperatorCardinalityProfile>> {
    fn plan_node(
        node: &PreparedRelationalJoinTreeNode,
        profiles: &mut [Option<RelationalOperatorCardinalityProfile>],
    ) -> Result<PlanCostBreakdown> {
        match node {
            PreparedRelationalJoinTreeNode::Relation(relation) => Ok(
                estimate_relational_access_cost(relation.access.descriptor().estimated_rows),
            ),
            PreparedRelationalJoinTreeNode::Join {
                operator_id,
                kind,
                left,
                right,
                ..
            } => {
                let left_cost = plan_node(left, profiles)?;
                let right_cost = plan_node(right, profiles)?;
                let cardinality = match kind {
                    SqlJoinKind::Inner => RelationalJoinCardinality::Inner,
                    SqlJoinKind::Left => RelationalJoinCardinality::PreserveLeft,
                };
                let cost = estimate_relational_join_cost(
                    left_cost,
                    right_cost,
                    cardinality,
                    if matches!(right.as_ref(), PreparedRelationalJoinTreeNode::Relation(_)) {
                        RelationalJoinRightInput::Probe
                    } else {
                        RelationalJoinRightInput::Materialized
                    },
                );
                let index = operator_id.get().checked_sub(1).ok_or_else(|| {
                    SkeinError::Execution(
                        "prepared CSG-CMP join has an invalid operator id".to_string(),
                    )
                })?;
                let slot = profiles.get_mut(index).ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "prepared CSG-CMP join operator {} is outside the plan profile",
                        operator_id.get()
                    ))
                })?;
                if slot.is_some() {
                    return Err(SkeinError::Execution(format!(
                        "prepared CSG-CMP join repeats operator {}",
                        operator_id.get()
                    )));
                }
                *slot = Some(RelationalOperatorCardinalityProfile {
                    operator_id: *operator_id,
                    operator: match kind {
                        SqlJoinKind::Inner => RelationalOperatorKind::IndexNestedLoopJoin,
                        SqlJoinKind::Left => RelationalOperatorKind::IndexNestedLoopLeftJoin,
                    },
                    table: right.first_relation().table.clone(),
                    estimated_rows: estimated_rows_as_usize(cost.estimated_rows),
                    actual_rows: None,
                    fully_consumed: false,
                });
                Ok(cost)
            }
        }
    }

    let relation_count = tree.root.relation_count();
    let mut profiles = vec![None; relation_count];
    let base = tree.root.first_relation();
    profiles[0] = Some(RelationalOperatorCardinalityProfile {
        operator_id: RelationalOperatorId::from_plan_index(0),
        operator: match base.access.descriptor().kind {
            RelationalAccessPathKind::FullScan => RelationalOperatorKind::TableFullScan,
            RelationalAccessPathKind::PrimaryKey => RelationalOperatorKind::TablePointGet,
            RelationalAccessPathKind::Index => RelationalOperatorKind::IndexRangeScan,
        },
        table: base.table.clone(),
        estimated_rows: base.access.descriptor().estimated_rows,
        actual_rows: None,
        fully_consumed: false,
    });
    let cost = plan_node(&tree.root, &mut profiles)?;
    if cost != tree.cost_breakdown {
        return Err(SkeinError::Execution(
            "prepared CSG-CMP operator estimates diverge from the selected join cost".to_string(),
        ));
    }
    profiles
        .into_iter()
        .enumerate()
        .map(|(index, profile)| {
            profile.ok_or_else(|| {
                SkeinError::Execution(format!(
                    "prepared CSG-CMP plan has no operator profile at index {index}"
                ))
            })
        })
        .collect()
}

fn estimated_rows_as_usize(rows: u64) -> usize {
    usize::try_from(rows).unwrap_or(usize::MAX)
}

struct PlannedJoin<'a> {
    join: &'a crate::sql::SqlJoin,
    schema: &'a RelationalTableSchema,
    qualifier: String,
    access: RelationalJoinAccess,
}

fn relational_locator_layout<'a>(
    base_table: &'a str,
    base_qualifier: &'a str,
    base_schema: &'a RelationalTableSchema,
    joins: &'a [PlannedJoin<'a>],
) -> Result<RelationalLocatorLayout<'a>> {
    RelationalLocatorLayout::from_bindings(
        std::iter::once((base_table, base_qualifier, base_schema)).chain(joins.iter().map(
            |join| {
                (
                    join.join.table.name.as_str(),
                    join.qualifier.as_str(),
                    join.schema,
                )
            },
        )),
    )
}

fn relational_join_tree_locator_layout<'a>(
    state: &'a RelationalState,
    tree: &'a PreparedRelationalJoinTree,
) -> Result<RelationalLocatorLayout<'a>> {
    let mut bindings = Vec::with_capacity(tree.root.relation_count());
    let mut error = None;
    tree.root.visit_relations(&mut |relation| {
        if error.is_some() {
            return;
        }
        match state.table_schema(&relation.table) {
            Some(schema) => {
                bindings.push((relation.table.as_str(), relation.qualifier.as_str(), schema))
            }
            None => {
                error = Some(SkeinError::Semantic(format!(
                    "unknown relational table {}",
                    relation.table
                )));
            }
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    RelationalLocatorLayout::from_bindings(bindings)
}

struct RelationalPipelineState<'a> {
    task_context: Option<&'a skein_core::RuntimeTaskContext>,
    batch_rows: usize,
    rows_until_checkpoint: usize,
    intermediate_rows: usize,
    max_intermediate_rows: usize,
    operator_cardinality_profiles: Vec<RelationalOperatorCardinalityProfile>,
    operator_pipeline_started: bool,
}

const RELATIONAL_ROW_LOCATOR_SLOT: SlotId = SlotId(0);

struct AccountedRelationalLocatorBatch {
    schema: Arc<BindingSchema>,
    locators: Vec<RelationalRowLocator>,
    row_limit: usize,
    byte_limit: usize,
    locator_bytes: usize,
    lease: QueryMemoryLease,
}

impl AccountedRelationalLocatorBatch {
    fn new(
        row_limit: usize,
        byte_limit: NonZeroUsize,
        memory_ledger: &QueryMemoryLedger,
    ) -> Result<Self> {
        let schema = Arc::new(BindingSchema::try_new(vec![SlotDescriptor {
            id: RELATIONAL_ROW_LOCATOR_SLOT,
            name: "row_locator".to_string(),
            slot_type: SlotType::RelationalRowLocator,
        }])?);
        let schema_bytes = std::mem::size_of::<BindingSchema>()
            .saturating_add(std::mem::size_of::<SlotDescriptor>())
            .saturating_add("row_locator".len());
        let account = memory_ledger.account(
            QueryMemoryClass::PipelineBatch,
            "RelationalOrderedIndexScan locator batch",
            byte_limit,
        );
        let lease = account.reserve(schema_bytes)?;
        Ok(Self {
            schema,
            locators: Vec::new(),
            row_limit: row_limit.max(1),
            byte_limit: byte_limit.get(),
            locator_bytes: 0,
            lease,
        })
    }

    fn push(
        &mut self,
        locator: RelationalRowLocator,
        emit: &mut dyn FnMut(ColumnarBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let bytes =
            std::mem::size_of::<RelationalRowLocator>().saturating_add(locator.allocated_bytes());
        if self
            .lease
            .bytes()
            .saturating_sub(self.locator_bytes)
            .saturating_add(bytes)
            > self.byte_limit
        {
            return Err(SkeinError::Execution(format!(
                "relational ordered locator uses {bytes} bytes, exceeding batch_payload_bytes {}",
                self.byte_limit
            )));
        }
        if !self.locators.is_empty()
            && (self.locators.len() == self.row_limit
                || self.lease.bytes().saturating_add(bytes) > self.byte_limit)
            && self.emit(emit)? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        self.lease.grow(bytes)?;
        self.locator_bytes = self.locator_bytes.saturating_add(bytes);
        self.locators.push(locator);
        if self.locators.len() == self.row_limit {
            return self.emit(emit);
        }
        Ok(BatchControl::Continue)
    }

    fn emit(
        &mut self,
        emit: &mut dyn FnMut(ColumnarBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if self.locators.is_empty() {
            return Ok(BatchControl::Continue);
        }
        let locators = std::mem::take(&mut self.locators);
        let batch = ColumnarBatch::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(ColumnVector::relational_row_locators(locators))],
        )?;
        let control = emit(batch);
        self.lease.shrink(self.locator_bytes);
        self.locator_bytes = 0;
        control
    }
}

impl<'a> RelationalPipelineState<'a> {
    fn new(
        task_context: Option<&'a skein_core::RuntimeTaskContext>,
        limits: RelationalQueryLimits,
        batch_rows: NonZeroUsize,
        operator_cardinality_profiles: Vec<RelationalOperatorCardinalityProfile>,
    ) -> Self {
        let batch_rows = batch_rows.get();
        Self {
            task_context,
            batch_rows,
            rows_until_checkpoint: batch_rows,
            intermediate_rows: 0,
            max_intermediate_rows: limits.max_intermediate_rows,
            operator_cardinality_profiles,
            operator_pipeline_started: false,
        }
    }

    fn begin_operator_pipeline(&mut self) {
        let first_invocation = !self.operator_pipeline_started;
        self.operator_pipeline_started = true;
        for profile in &mut self.operator_cardinality_profiles {
            profile.actual_rows.get_or_insert(0);
            if first_invocation {
                profile.fully_consumed = true;
            }
        }
    }

    fn account_operator_row(&mut self, operator_id: RelationalOperatorId) -> Result<()> {
        account_intermediate(&mut self.intermediate_rows, 1, self.max_intermediate_rows)?;
        let profile = self
            .operator_cardinality_profiles
            .get_mut(operator_id.get().saturating_sub(1))
            .ok_or_else(|| {
                SkeinError::Execution(format!(
                    "relational operator {} has no cardinality profile",
                    operator_id.get()
                ))
            })?;
        profile.actual_rows = Some(profile.actual_rows.unwrap_or(0).saturating_add(1));
        self.rows_until_checkpoint = self.rows_until_checkpoint.saturating_sub(1);
        if self.rows_until_checkpoint == 0 {
            skein_executor::pipeline::runtime_checkpoint(self.task_context)?;
            self.rows_until_checkpoint = self.batch_rows;
        }
        Ok(())
    }

    fn account_unprofiled_row(&mut self) -> Result<()> {
        account_intermediate(&mut self.intermediate_rows, 1, self.max_intermediate_rows)?;
        self.rows_until_checkpoint = self.rows_until_checkpoint.saturating_sub(1);
        if self.rows_until_checkpoint == 0 {
            skein_executor::pipeline::runtime_checkpoint(self.task_context)?;
            self.rows_until_checkpoint = self.batch_rows;
        }
        Ok(())
    }

    fn finish_operator_pipeline(&mut self, fully_consumed: bool) {
        if self.operator_pipeline_started {
            for profile in &mut self.operator_cardinality_profiles {
                profile.fully_consumed &= fully_consumed;
            }
        }
    }

    fn operator_cardinality_profiles(&self) -> Vec<RelationalOperatorCardinalityProfile> {
        self.operator_cardinality_profiles.clone()
    }

    fn finish(&self) -> Result<()> {
        skein_executor::pipeline::runtime_checkpoint(self.task_context)
    }
}

fn prepare_relational_select(
    select: SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    index_read_mode: RelationalIndexReadMode<'_>,
    limits: RelationalQueryLimits,
    join_enumeration: RelationalJoinEnumerationConfig,
    initial_stage_timings: RelationalSqlStageTimings,
) -> Result<PreparedRelationalSelect> {
    let prepare_started = Instant::now();
    let mut current_state_bind_nanos = 0;
    measure_nanos(&mut current_state_bind_nanos, || -> Result<()> {
        reject_non_public_schema(select.from.schema.as_deref())?;
        if state.table_schema(&select.from.name).is_none() {
            return Err(SkeinError::Semantic(format!(
                "unknown relational table {}",
                select.from.name
            )));
        }
        for join in &select.joins {
            reject_non_public_schema(join.table.schema.as_deref())?;
            if state.table_schema(&join.table.name).is_none() {
                return Err(SkeinError::Semantic(format!(
                    "unknown relational table {}",
                    join.table.name
                )));
            }
        }
        Ok(())
    })?;
    let planned = join_order::plan_select_join_order(
        select,
        parameters,
        state,
        index_read_mode,
        limits,
        join_enumeration,
        &mut current_state_bind_nanos,
    )?;
    let access_plan = match planned.access_plan {
        Some(access_plan) => access_plan,
        None => prepare_syntax_access_plan(
            &planned.statement,
            parameters,
            state,
            index_read_mode,
            limits,
        )?,
    };
    let execution =
        PreparedRelationalExecutionDescriptor::prepare(&planned.statement, &access_plan);
    let prepare_nanos = elapsed_nanos(prepare_started);
    let prepared = PreparedRelationalSelect {
        statement: planned.statement,
        access_plan,
        join_planning: planned.join_planning,
        execution,
        stage_timings: RelationalSqlStageTimings {
            parse_nanos: initial_stage_timings.parse_nanos,
            bind_nanos: initial_stage_timings
                .bind_nanos
                .saturating_add(current_state_bind_nanos),
            plan_nanos: prepare_nanos.saturating_sub(current_state_bind_nanos),
            execute_nanos: 0,
        },
    };
    prepared.validate()?;
    Ok(prepared)
}

fn prepare_syntax_access_plan(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    index_read_mode: RelationalIndexReadMode<'_>,
    limits: RelationalQueryLimits,
) -> Result<PreparedRelationalAccessPlan> {
    let base_schema = state.table_schema(&select.from.name).ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational table {}", select.from.name))
    })?;
    let base_qualifier = select
        .from_alias
        .clone()
        .unwrap_or_else(|| select.from.name.clone());
    let has_aggregate = select.projection.iter().any(projection_contains_aggregate);
    let prefer_ordered_access =
        select.joins.is_empty() && !select.distinct && !has_aggregate && select.group_by.is_empty();
    let base_access = choose_base_access(RelationalBaseAccessPlanning {
        predicate: select.selection.as_ref(),
        order_by: &select.order_by,
        prefer_ordered_access,
        parameters,
        state,
        schema: base_schema,
        table: &select.from.name,
        qualifier: &base_qualifier,
        cardinality_limit: limits.max_intermediate_rows.saturating_add(1),
    })?;
    let join_accesses = select
        .joins
        .iter()
        .map(|join| {
            let join_schema = state.table_schema(&join.table.name).ok_or_else(|| {
                SkeinError::Semantic(format!("unknown relational table {}", join.table.name))
            })?;
            let qualifier = join
                .alias
                .clone()
                .unwrap_or_else(|| join.table.name.clone());
            choose_join_access(
                &join.on,
                state,
                join_schema,
                &join.table.name,
                &qualifier,
                index_read_mode,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PreparedRelationalAccessPlan {
        base_access,
        join_accesses,
        join_selection: None,
        join_tree: None,
    })
}

fn prepared_access_descriptors(
    plan: &PreparedRelationalAccessPlan,
) -> (
    RelationalAccessPathDescriptor,
    Vec<RelationalAccessPathDescriptor>,
) {
    if let Some(tree) = &plan.join_tree {
        let base = tree.root.first_relation().access.descriptor().clone();
        let mut joins = Vec::with_capacity(tree.root.relation_count().saturating_sub(1));
        tree.root.visit_join_right_relations(&mut |relation| {
            joins.push(relation.access.descriptor().clone());
        });
        return (base, joins);
    }
    (
        plan.base_access.descriptor.clone(),
        plan.join_accesses
            .iter()
            .map(|access| access.descriptor.clone())
            .collect(),
    )
}

fn explain_select(
    prepared: &PreparedRelationalSelect,
    parameters: &[Value],
    limits: RelationalQueryLimits,
) -> Result<RelationalQueryOutput> {
    let (access_path, join_access_paths) = prepared_access_descriptors(&prepared.access_plan);
    format_relational_explain(
        &prepared.statement,
        parameters,
        RelationalQueryOutput {
            rows: QueryRows::empty(),
            stage_timings: prepared.stage_timings,
            join_planning: prepared.join_planning.clone(),
            operator_cardinality_profiles: planned_operator_cardinality_profiles(prepared)?,
            intermediate_rows: 0,
            hydration: limits.hydration,
            access_path,
            join_access_paths,
            index_execution_evidence: Vec::new(),
            row_execution_evidence: RelationalRowExecutionEvidence {
                runtime_path: "not_executed",
                ..RelationalRowExecutionEvidence::default()
            },
            blocking_operator_memory_reports: Vec::new(),
        },
        false,
        limits,
    )
}

fn execute_select_timed<'state>(
    prepared: &'state PreparedRelationalSelect,
    parameters: &[Value],
    execution: AdmittedRelationalExecution<'state, '_>,
) -> Result<RelationalQueryOutput> {
    let started = Instant::now();
    let mut output = execute_select(prepared, parameters, execution)?;
    output.stage_timings = prepared.stage_timings;
    output.stage_timings.execute_nanos = elapsed_nanos(started);
    Ok(output)
}

fn execute_select<'state>(
    prepared: &'state PreparedRelationalSelect,
    parameters: &[Value],
    execution: AdmittedRelationalExecution<'state, '_>,
) -> Result<RelationalQueryOutput> {
    let select = &prepared.statement;
    let join_planning = &prepared.join_planning;
    let operator_cardinality_profiles = planned_operator_cardinality_profiles(prepared)?;
    let AdmittedRelationalExecution {
        state,
        index_read_mode,
        row_read_mode,
        limits,
        execution_memory,
        memory_ledger,
        task_context,
    } = execution;
    let base_schema = state.table_schema(&select.from.name).ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational table {}", select.from.name))
    })?;
    let base_qualifier = select
        .from_alias
        .clone()
        .unwrap_or_else(|| select.from.name.clone());
    let has_aggregate = select.projection.iter().any(projection_contains_aggregate);
    let base_access = &prepared.access_plan.base_access;
    let (access_path, join_access_paths) = prepared_access_descriptors(&prepared.access_plan);
    let mut planned_joins = Vec::with_capacity(select.joins.len());
    for (join, join_access) in select.joins.iter().zip(&prepared.access_plan.join_accesses) {
        let join_schema = state.table_schema(&join.table.name).ok_or_else(|| {
            SkeinError::Semantic(format!("unknown relational table {}", join.table.name))
        })?;
        let qualifier = join
            .alias
            .clone()
            .unwrap_or_else(|| join.table.name.clone());
        planned_joins.push(PlannedJoin {
            join,
            schema: join_schema,
            qualifier,
            access: join_access.access.clone(),
        });
    }
    let default_task = skein_core::RuntimeTaskContext::default();
    let row_task = task_context.unwrap_or(&default_task);
    let output_fields = plan_requested_fields(select, state)?;
    let scan_fields = if (!select.order_by.is_empty() && !select.distinct && !has_aggregate)
        || has_aggregate
        || !select.group_by.is_empty()
    {
        plan_scan_fields(select, state)?
    } else {
        output_fields.clone()
    };
    let scan_hydration_fields = plan_scan_hydration_fields(select, state, &scan_fields)?;
    let row_runtime = RelationalRowRuntime::new(
        state,
        row_read_mode,
        RelationalFieldPlan::new(scan_fields, scan_hydration_fields, output_fields),
        limits.row_read,
        limits.hydration,
        row_task,
    )?;
    let mut pipeline = RelationalPipelineState::new(
        task_context,
        limits,
        execution_memory.batch_rows,
        operator_cardinality_profiles,
    );
    let index_runtime = RelationalIndexRuntime::new(index_read_mode, limits.index_read);
    let tree_execution =
        prepared
            .access_plan
            .join_tree
            .as_ref()
            .map(|tree| PreparedJoinTreeExecution {
                tree,
                memory: execution_memory,
                memory_ledger: &memory_ledger,
                reports: RefCell::new(Vec::new()),
            });
    if prepared.execution.mode == PreparedRelationalExecutionMode::OrderedIndexProjection {
        let output = execute_ordered_index_projection(
            select,
            parameters,
            state,
            base_schema,
            &base_qualifier,
            &base_access.access,
            &mut pipeline,
            &index_runtime,
            &row_runtime,
            limits,
            execution_memory,
            &memory_ledger,
        )?;
        pipeline.finish()?;
        return Ok(RelationalQueryOutput {
            rows: output.rows,
            stage_timings: RelationalSqlStageTimings::default(),
            join_planning: join_planning.clone(),
            operator_cardinality_profiles: pipeline.operator_cardinality_profiles(),
            intermediate_rows: pipeline.intermediate_rows,
            hydration: row_runtime.hydration(),
            access_path,
            join_access_paths,
            index_execution_evidence: index_runtime.evidence(),
            row_execution_evidence: row_runtime.evidence(),
            blocking_operator_memory_reports: output.blocking_operator_memory_reports,
        });
    }
    if prepared.execution.mode == PreparedRelationalExecutionMode::StreamingProjection {
        let mut output = execute_streaming_projection(
            select,
            parameters,
            state,
            base_schema,
            &base_qualifier,
            &base_access.access,
            &planned_joins,
            tree_execution.as_ref(),
            &mut pipeline,
            &index_runtime,
            &row_runtime,
            limits,
        )?;
        if let Some(execution) = &tree_execution {
            output
                .blocking_operator_memory_reports
                .extend(execution.take_reports());
        }
        pipeline.finish()?;
        return Ok(RelationalQueryOutput {
            rows: output.rows,
            stage_timings: RelationalSqlStageTimings::default(),
            join_planning: join_planning.clone(),
            operator_cardinality_profiles: pipeline.operator_cardinality_profiles(),
            intermediate_rows: pipeline.intermediate_rows,
            hydration: row_runtime.hydration(),
            access_path,
            join_access_paths,
            index_execution_evidence: index_runtime.evidence(),
            row_execution_evidence: row_runtime.evidence(),
            blocking_operator_memory_reports: output.blocking_operator_memory_reports,
        });
    }

    if prepared.execution.mode == PreparedRelationalExecutionMode::Aggregate {
        let mut output = execute_aggregate_select(
            select,
            parameters,
            state,
            base_schema,
            &base_qualifier,
            &base_access.access,
            &planned_joins,
            tree_execution.as_ref(),
            &mut pipeline,
            &index_runtime,
            &row_runtime,
            limits,
            execution_memory,
            &memory_ledger,
            access_path,
            join_access_paths,
            join_planning,
        )?;
        if let Some(execution) = &tree_execution {
            output
                .blocking_operator_memory_reports
                .extend(execution.take_reports());
        }
        return Ok(output);
    }

    let mut output = execute_blocking_projection(
        select,
        parameters,
        state,
        base_schema,
        &base_qualifier,
        &base_access.access,
        &planned_joins,
        tree_execution.as_ref(),
        &mut pipeline,
        &index_runtime,
        &row_runtime,
        limits,
        execution_memory,
        &memory_ledger,
    )?;
    if let Some(execution) = &tree_execution {
        output
            .blocking_operator_memory_reports
            .extend(execution.take_reports());
    }
    pipeline.finish()?;
    Ok(RelationalQueryOutput {
        rows: output.rows,
        stage_timings: RelationalSqlStageTimings::default(),
        join_planning: join_planning.clone(),
        operator_cardinality_profiles: pipeline.operator_cardinality_profiles(),
        intermediate_rows: pipeline.intermediate_rows,
        hydration: row_runtime.hydration(),
        access_path,
        join_access_paths,
        index_execution_evidence: index_runtime.evidence(),
        row_execution_evidence: row_runtime.evidence(),
        blocking_operator_memory_reports: output.blocking_operator_memory_reports,
    })
}

#[derive(Debug)]
struct RelationalExplainNode {
    operator: &'static str,
    operator_id: Option<RelationalOperatorId>,
    estimated_rows: Option<usize>,
    access_object: String,
    operator_info: String,
    report_operator: Option<&'static str>,
}

fn format_relational_explain(
    select: &SelectStatement,
    parameters: &[Value],
    mut output: RelationalQueryOutput,
    analyze: bool,
    limits: RelationalQueryLimits,
) -> Result<RelationalQueryOutput> {
    let actual_output_rows = output.rows.len();
    let mut nodes = Vec::new();
    let bound_limit = bind_bound(select.limit, parameters, "LIMIT")?;
    let bound_offset = bind_bound(select.offset, parameters, "OFFSET")?.unwrap_or(0);
    if select.limit.is_some() || select.offset.is_some() {
        nodes.push(RelationalExplainNode {
            operator: "LimitExec",
            operator_id: None,
            estimated_rows: bound_limit.map(|limit| usize::try_from(limit).unwrap_or(usize::MAX)),
            access_object: String::new(),
            operator_info: format!(
                "offset={}, count={}",
                bound_offset,
                bound_limit
                    .map(|limit| limit.to_string())
                    .unwrap_or_else(|| "unbounded".to_string())
            ),
            report_operator: None,
        });
    }
    if !select.order_by.is_empty() && output.access_path.order_prefix_len != select.order_by.len() {
        nodes.push(RelationalExplainNode {
            operator: "TopNExec",
            operator_id: None,
            estimated_rows: bound_limit.map(|limit| usize::try_from(limit).unwrap_or(usize::MAX)),
            access_object: String::new(),
            operator_info: format!(
                "order_by={}, offset={}",
                explain_order_by(&select.order_by),
                bound_offset
            ),
            report_operator: Some("TopNExec"),
        });
    }
    let has_aggregate = select.projection.iter().any(projection_contains_aggregate);
    if has_aggregate || !select.group_by.is_empty() {
        nodes.push(RelationalExplainNode {
            operator: "RelationalAggregateExec",
            operator_id: None,
            estimated_rows: (!select.group_by.is_empty()).then_some(
                output
                    .access_path
                    .estimated_rows
                    .min(limits.max_intermediate_rows),
            ),
            access_object: String::new(),
            operator_info: if select.group_by.is_empty() {
                "group_by=[]".to_string()
            } else {
                format!("group_by=[{}]", explain_columns(&select.group_by))
            },
            report_operator: Some("RelationalAggregateExec"),
        });
    }
    if !select.group_by.is_empty() {
        nodes.push(RelationalExplainNode {
            operator: "SortExec",
            operator_id: None,
            estimated_rows: Some(output.access_path.estimated_rows),
            access_object: String::new(),
            operator_info: format!("group_keys=[{}]", explain_columns(&select.group_by)),
            report_operator: Some("SortExec"),
        });
    }
    if select.distinct || single_count_distinct_column(select).is_some() {
        nodes.push(RelationalExplainNode {
            operator: "DistinctExec",
            operator_id: None,
            estimated_rows: Some(output.access_path.estimated_rows),
            access_object: String::new(),
            operator_info: if select.distinct {
                "scope=statement".to_string()
            } else {
                "scope=aggregate_argument".to_string()
            },
            report_operator: Some("DistinctExec"),
        });
    }
    nodes.push(RelationalExplainNode {
        operator: "ProjectionExec",
        operator_id: None,
        estimated_rows: Some(output.access_path.estimated_rows),
        access_object: String::new(),
        operator_info: format!("columns={}", select.projection.len()),
        report_operator: None,
    });
    if select.selection.is_some()
        && !predicate_is_covered_by_access(
            select.selection.as_ref(),
            &output.access_path,
            &select.order_by,
            &select.from.name,
            select.from_alias.as_deref().unwrap_or(&select.from.name),
        )
    {
        nodes.push(RelationalExplainNode {
            operator: "SelectionExec",
            operator_id: None,
            estimated_rows: Some(output.access_path.estimated_rows),
            access_object: String::new(),
            operator_info: "residual_predicate=true".to_string(),
            report_operator: None,
        });
    }
    for (cardinality, descriptor) in output
        .operator_cardinality_profiles
        .iter()
        .skip(1)
        .zip(&output.join_access_paths)
    {
        let operator_id = cardinality.operator_id;
        let table = &cardinality.table;
        let access_path = explain_access_path(
            descriptor,
            relational_index_evidence(&output, table, descriptor),
            &output.row_execution_evidence,
        );
        nodes.push(RelationalExplainNode {
            operator: cardinality.operator.as_str(),
            operator_id: Some(operator_id),
            estimated_rows: Some(cardinality.estimated_rows),
            access_object: explain_access_object(table, descriptor),
            operator_info: format!(
                "{}, {access_path}",
                explain_join_planning(&output.join_planning, output.stage_timings)
            ),
            report_operator: None,
        });
    }
    let base_operator_id = RelationalOperatorId::from_plan_index(0);
    let base_cardinality = relational_operator_cardinality_profile(&output, base_operator_id);
    let base_table = base_cardinality
        .map(|profile| profile.table.as_str())
        .unwrap_or(&select.from.name);
    nodes.push(RelationalExplainNode {
        operator: base_cardinality.map_or_else(
            || match output.access_path.kind {
                RelationalAccessPathKind::FullScan => "TableFullScanExec",
                RelationalAccessPathKind::PrimaryKey => "TablePointGetExec",
                RelationalAccessPathKind::Index => "IndexRangeScanExec",
            },
            |profile| profile.operator.as_str(),
        ),
        operator_id: Some(base_operator_id),
        estimated_rows: base_cardinality.map(|profile| profile.estimated_rows),
        access_object: explain_access_object(base_table, &output.access_path),
        operator_info: explain_access_path(
            &output.access_path,
            relational_index_evidence(&output, base_table, &output.access_path),
            &output.row_execution_evidence,
        ),
        report_operator: None,
    });

    let mut rows = Vec::with_capacity(nodes.len());
    let mut payload_bytes = 0usize;
    let mut next_unprofiled_id = output.operator_cardinality_profiles.len().saturating_add(1);
    for (index, node) in nodes.iter().enumerate() {
        let report = node.report_operator.and_then(|operator| {
            output
                .blocking_operator_memory_reports
                .iter()
                .find(|report| report.operator == operator)
        });
        let cardinality = node
            .operator_id
            .and_then(|operator_id| relational_operator_cardinality_profile(&output, operator_id));
        let display_id = node.operator_id.map_or_else(
            || {
                let display_id = next_unprofiled_id;
                next_unprofiled_id = next_unprofiled_id.saturating_add(1);
                display_id
            },
            RelationalOperatorId::get,
        );
        let mut row = Row::from([
            (
                "id".to_string(),
                Value::String(explain_tree_id(node.operator, index, display_id)),
            ),
            (
                "estRows".to_string(),
                optional_estimated_rows_explain_value(node.estimated_rows),
            ),
            ("task".to_string(), Value::String("root".to_string())),
            (
                "access object".to_string(),
                Value::String(node.access_object.clone()),
            ),
            (
                "operator info".to_string(),
                Value::String(node.operator_info.clone()),
            ),
        ]);
        if analyze {
            row.insert(
                "actRows".to_string(),
                if let Some(cardinality) = cardinality {
                    optional_usize_explain_value(cardinality.actual_rows)
                } else if index == 0 {
                    optional_usize_explain_value(Some(actual_output_rows))
                } else {
                    optional_usize_explain_value(report.map(|report| report.input_rows))
                },
            );
            row.insert(
                "execution info".to_string(),
                if let Some(cardinality) = cardinality {
                    Value::String(format!(
                        "operator_id={}, fully_consumed={}",
                        cardinality.operator_id.get(),
                        cardinality.fully_consumed
                    ))
                } else if index == 0 {
                    Value::String(format!(
                        "intermediate_rows={}, hydrated_rows={}, compressed_bytes={}, decompressed_bytes={}",
                        output.intermediate_rows,
                        output.hydration.hydrated_rows,
                        output.hydration.compressed_bytes,
                        output.hydration.decompressed_bytes,
                    ))
                } else {
                    report
                        .map(|report| {
                            Value::String(format!("input_rows={}", report.input_rows))
                        })
                        .unwrap_or(Value::Null)
                },
            );
            row.insert(
                "memory".to_string(),
                report
                    .map(|report| {
                        Value::String(format!(
                            "peak={}/budget={}",
                            report.peak_tracked_bytes, report.budget_bytes
                        ))
                    })
                    .unwrap_or(Value::Null),
            );
            row.insert(
                "disk".to_string(),
                report
                    .filter(|report| report.spill_run_count != 0)
                    .map(|report| {
                        Value::String(format!(
                            "runs={}, rows={}, bytes={}",
                            report.spill_run_count, report.spilled_rows, report.spilled_bytes
                        ))
                    })
                    .unwrap_or(Value::Null),
            );
        }
        push_relational_output(row, &mut rows, &mut payload_bytes, limits)?;
    }
    output.rows = rows.into();
    Ok(output)
}

fn explain_join_planning(
    outcome: &RelationalJoinPlanningOutcome,
    stage_timings: RelationalSqlStageTimings,
) -> String {
    let join_order = if outcome.join_order_reordered() {
        "cost_reordered"
    } else if outcome.status == RelationalJoinPlanningStatus::Fallback {
        "syntax_fallback"
    } else {
        "syntax"
    };
    let memo_groups = outcome
        .memo_groups
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unavailable".to_string());
    let memo_expressions = outcome
        .memo_expressions
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unavailable".to_string());
    let selected_order = outcome.selected_order.join(",");
    let cost = outcome.cost.map_or_else(
        || "plan_cost=unavailable".to_string(),
        |cost| {
            format!(
                "estimated_rows={}, plan_cost={}, cpu={}, random_io={}, sequential_io={}, output_rows={}",
                cost.estimated_rows,
                cost.cost,
                cost.cpu,
                cost.random_io,
                cost.sequential_io,
                cost.output_rows
            )
        },
    );
    let attempts = outcome
        .attempts
        .iter()
        .enumerate()
        .map(|(index, attempt)| {
            let fallback_class = attempt
                .fallback_class
                .map(|class| class.as_str())
                .unwrap_or("none");
            let memo_groups = attempt
                .memo_groups
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unavailable".to_string());
            let memo_expressions = attempt
                .memo_expressions
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unavailable".to_string());
            let cost = attempt
                .cost
                .map(|cost| cost.cost.to_string())
                .unwrap_or_else(|| "unavailable".to_string());
            format!(
                "{index}:{}:{}:{}:fallback_class={fallback_class}:memo_groups={memo_groups}:memo_expressions={memo_expressions}:cost={cost}",
                attempt.strategy.as_str(),
                attempt.status.as_str(),
                attempt.reason.as_str(),
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    format!(
        "join_order={join_order}, planning_strategy={}, planning_status={}, planning_reason={}, memo_groups={memo_groups}, memo_expressions={memo_expressions}, max_groups={}, max_expressions={}, selected_order=[{selected_order}], attempts=[{attempts}], parse_nanos={}, bind_nanos={}, plan_nanos={}, execute_nanos={}, {cost}",
        outcome.strategy.as_str(),
        outcome.status.as_str(),
        outcome.reason.as_str(),
        outcome.budget.max_groups,
        outcome.budget.max_expressions,
        stage_timings.parse_nanos,
        stage_timings.bind_nanos,
        stage_timings.plan_nanos,
        stage_timings.execute_nanos,
    )
}

fn explain_tree_id(operator: &str, index: usize, display_id: usize) -> String {
    if index == 0 {
        format!("{operator}_{display_id}")
    } else {
        format!("{}└─{operator}_{display_id}", "  ".repeat(index - 1))
    }
}

fn optional_usize_explain_value(value: Option<usize>) -> Value {
    value
        .map(|value| Value::Int(i64::try_from(value).unwrap_or(i64::MAX)))
        .unwrap_or(Value::Null)
}

fn optional_estimated_rows_explain_value(value: Option<usize>) -> Value {
    optional_usize_explain_value(value.map(|value| value.max(1)))
}

fn explain_access_object(table: &str, descriptor: &RelationalAccessPathDescriptor) -> String {
    match descriptor.kind {
        RelationalAccessPathKind::FullScan => format!("table:{table}"),
        RelationalAccessPathKind::PrimaryKey => format!("table:{table}, primary_key"),
        RelationalAccessPathKind::Index => {
            format!("table:{table}, index:{}", descriptor.name)
        }
    }
}

fn relational_operator_cardinality_profile(
    output: &RelationalQueryOutput,
    operator_id: RelationalOperatorId,
) -> Option<&RelationalOperatorCardinalityProfile> {
    output
        .operator_cardinality_profiles
        .iter()
        .find(|profile| profile.operator_id == operator_id)
}

fn relational_index_evidence<'a>(
    output: &'a RelationalQueryOutput,
    table: &str,
    descriptor: &RelationalAccessPathDescriptor,
) -> Option<&'a RelationalIndexExecutionEvidence> {
    let physical_index = match descriptor.kind {
        RelationalAccessPathKind::PrimaryKey => skein_storage::RELATIONAL_PRIMARY_INDEX_NAME,
        RelationalAccessPathKind::Index => descriptor.name.as_str(),
        RelationalAccessPathKind::FullScan => return None,
    };
    output
        .index_execution_evidence
        .iter()
        .find(|evidence| evidence.table == table && evidence.index == physical_index)
}

fn explain_access_path(
    descriptor: &RelationalAccessPathDescriptor,
    evidence: Option<&RelationalIndexExecutionEvidence>,
    row_evidence: &RelationalRowExecutionEvidence,
) -> String {
    let planned = format!(
        "equality_prefix={}, order_prefix={}, exclusive_seek={}, direction={}, unique_point={}, row_fetch={}",
        descriptor.equality_prefix_len,
        descriptor.order_prefix_len,
        descriptor.exclusive_range,
        if descriptor.reverse_order {
            "backward"
        } else {
            "forward"
        },
        descriptor.unique_point,
        descriptor.requires_row_fetch
    );
    let row = format!(
        "row_runtime_path={}, row_base_generation={}, row_delta_generation={}, row_base_epoch={}, row_visible_epoch={}, row_root_set_digest={}, row_descriptor_reads={}, row_logical_pages={}, row_logical_bytes={}, row_physical_pages={}, row_physical_bytes={}, row_cache_hits={}, row_cache_misses={}, row_cache_admission_rejections={}, row_overlay_entries={}, row_overlay_bytes={}, row_rows={}, row_borrowed_rows={}, row_owned_rows={}",
        row_evidence.runtime_path,
        optional_u64_text(row_evidence.base_generation),
        optional_u64_text(row_evidence.delta_generation),
        optional_u64_text(row_evidence.base_commit_epoch),
        optional_u64_text(row_evidence.visible_commit_epoch),
        row_evidence.root_set_digest.as_deref().unwrap_or("none"),
        row_evidence.descriptor_reads,
        row_evidence.logical_pages,
        row_evidence.logical_bytes,
        row_evidence.file_pages,
        row_evidence.file_bytes,
        row_evidence.cache_hits,
        row_evidence.cache_misses,
        row_evidence.cache_admission_rejections,
        row_evidence.overlay_entries,
        row_evidence.overlay_resident_bytes,
        row_evidence.rows_visited,
        row_evidence.borrowed_rows_visited,
        row_evidence.owned_rows_visited,
    );
    let Some(evidence) = evidence else {
        return format!("{planned}, {row}");
    };
    let fallback_reasons = if evidence.fallback_reasons.is_empty() {
        "none".to_string()
    } else {
        evidence
            .fallback_reasons
            .iter()
            .copied()
            .collect::<Vec<_>>()
            .join("|")
    };
    format!(
        "{planned}, runtime_path={}, lookups={}, range_lookups={}, exclusive_seek_lookups={}, backward_lookups={}, early_stop_lookups={}, demand_paged={}, authoritative={}, transaction_workspace={}, canonical_fallback={}, fallback_reasons={}, base_generation={}, delta_generation={}, base_epoch={}, visible_epoch={}, root_set_digest={}, logical_pages={}, logical_bytes={}, physical_pages={}, physical_bytes={}, cache_hits={}, cache_misses={}, cache_admission_rejections={}, delta_entries={}, live_batches={}, live_entries={}, live_matches={}, live_bytes={}, index_rows={}, {row}",
        evidence.runtime_path(),
        evidence.lookups,
        evidence.range_lookups,
        evidence.exclusive_seek_lookups,
        evidence.backward_lookups,
        evidence.early_stop_lookups,
        evidence.demand_paged_lookups,
        evidence.authoritative_lookups,
        evidence.transaction_workspace_lookups,
        evidence.canonical_fallback_lookups,
        fallback_reasons,
        optional_u64_text(evidence.base_generation),
        optional_u64_text(evidence.delta_generation),
        optional_u64_text(evidence.base_commit_epoch),
        optional_u64_text(evidence.visible_commit_epoch),
        evidence.root_set_digest.as_deref().unwrap_or("none"),
        evidence.logical_pages,
        evidence.logical_bytes,
        evidence.file_pages,
        evidence.file_bytes,
        evidence.cache_hits,
        evidence.cache_misses,
        evidence.cache_admission_rejections,
        evidence.delta_entries_visited,
        evidence.live_batches_visited,
        evidence.live_entries_visited,
        evidence.live_entries_matched,
        evidence.live_bytes_visited,
        evidence.rows_visited,
    )
}

fn optional_u64_text(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn explain_order_by(order_by: &[crate::sql::SqlOrderItem]) -> String {
    order_by
        .iter()
        .map(|item| {
            format!(
                "{} {}",
                explain_column(&item.column),
                match item.direction {
                    SqlOrderDirection::Asc => "ASC",
                    SqlOrderDirection::Desc => "DESC",
                }
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn explain_columns(columns: &[SqlColumnRef]) -> String {
    columns
        .iter()
        .map(explain_column)
        .collect::<Vec<_>>()
        .join(", ")
}

fn explain_column(column: &SqlColumnRef) -> String {
    column
        .qualifier
        .as_ref()
        .map(|qualifier| format!("{qualifier}.{}", column.name))
        .unwrap_or_else(|| column.name.clone())
}

struct RelationalBaseAccessPlanning<'a> {
    predicate: Option<&'a SqlPredicate>,
    order_by: &'a [crate::sql::SqlOrderItem],
    prefer_ordered_access: bool,
    parameters: &'a [Value],
    state: &'a RelationalState,
    schema: &'a RelationalTableSchema,
    table: &'a str,
    qualifier: &'a str,
    cardinality_limit: usize,
}

fn choose_base_access(
    planning: RelationalBaseAccessPlanning<'_>,
) -> Result<RelationalAccessCandidate> {
    let RelationalBaseAccessPlanning {
        predicate,
        order_by,
        prefer_ordered_access,
        parameters,
        state,
        schema,
        table,
        qualifier,
        cardinality_limit,
    } = planning;
    let mut equalities = Vec::new();
    if let Some(predicate) = predicate {
        collect_conjunctive_equalities(predicate, &mut equalities);
    }
    let mut bound = BTreeMap::<String, RelationalValue>::new();
    for (column, value) in equalities {
        if column
            .qualifier
            .as_deref()
            .is_some_and(|candidate| candidate != table && candidate != qualifier)
        {
            continue;
        }
        let Some(position) = schema.column_position(&column.name) else {
            continue;
        };
        let value = value_to_relational(bind_sql_value(value, parameters)?)?;
        if matches!(value, RelationalValue::Null) {
            continue;
        }
        if value.scalar_type() != Some(schema.columns[position].scalar_type) {
            return Err(SkeinError::Semantic(format!(
                "relational comparison on {} has an incompatible scalar type",
                column.name
            )));
        }
        match bound.entry(column.name.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &value => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                // The residual predicate will reject the contradictory equality.
            }
        }
    }

    let row_count = state.row_count(table);
    let mut candidates = vec![RelationalAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::FullScan,
            name: "__full_scan".to_string(),
            index_columns: Vec::new(),
            access_columns: BTreeSet::new(),
            equality_prefix_len: 0,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: false,
            estimated_rows: row_count.min(cardinality_limit).max(1),
        },
        access: RelationalBaseAccess::FullScan,
    }];

    if let Some(key) = complete_key(&schema.primary_key, &bound) {
        let estimated_rows = usize::from(state.row(table, &key).is_some()).max(1);
        candidates.push(RelationalAccessCandidate {
            descriptor: RelationalAccessPathDescriptor {
                kind: RelationalAccessPathKind::PrimaryKey,
                name: "__primary_key".to_string(),
                index_columns: schema.primary_key.clone(),
                access_columns: schema.primary_key.iter().cloned().collect(),
                equality_prefix_len: schema.primary_key.len(),
                order_prefix_len: 0,
                exclusive_range: false,
                reverse_order: false,
                unique_point: true,
                covering: false,
                requires_row_fetch: false,
                estimated_rows,
            },
            access: RelationalBaseAccess::PrimaryKey(key),
        });
    }

    for (ordinal, columns) in schema.unique_constraints.iter().enumerate() {
        if let Some(candidate) = index_access_candidate(
            &planning,
            relational_unique_index_name(ordinal),
            columns,
            true,
            &bound,
        )? {
            candidates.push(candidate);
        }
    }
    for index in &schema.indexes {
        if let Some(candidate) = index_access_candidate(
            &planning,
            index.name.clone(),
            &index.columns,
            index.unique,
            &bound,
        )? {
            candidates.push(candidate);
        }
    }

    let ordered_candidates = candidates
        .iter()
        .filter(|candidate| {
            prefer_ordered_access
                && !order_by.is_empty()
                && candidate.descriptor.order_prefix_len == order_by.len()
                && predicate_is_covered_by_access(
                    predicate,
                    &candidate.descriptor,
                    order_by,
                    table,
                    qualifier,
                )
        })
        .map(|candidate| candidate.descriptor.clone())
        .collect::<Vec<_>>();
    let descriptors = if ordered_candidates.is_empty() {
        candidates
            .iter()
            .map(|candidate| candidate.descriptor.clone())
            .collect()
    } else {
        ordered_candidates
    };
    let selected = select_relational_access_path(descriptors)
        .map_err(|error| SkeinError::Execution(format!("invalid relational access path: {error}")))?
        .expect("full scan is always an access-path candidate");
    let position = candidates
        .iter()
        .position(|candidate| candidate.descriptor == selected)
        .expect("selected relational access path came from the candidate set");
    Ok(candidates.swap_remove(position))
}

fn complete_key(
    columns: &[String],
    bound: &BTreeMap<String, RelationalValue>,
) -> Option<RelationalKey> {
    columns
        .iter()
        .map(|column| bound.get(column).cloned())
        .collect::<Option<Vec<_>>>()
        .map(RelationalKey)
}

fn index_access_candidate(
    planning: &RelationalBaseAccessPlanning<'_>,
    name: String,
    columns: &[String],
    unique: bool,
    bound: &BTreeMap<String, RelationalValue>,
) -> Result<Option<RelationalAccessCandidate>> {
    let RelationalBaseAccessPlanning {
        order_by,
        state,
        schema,
        table,
        qualifier,
        cardinality_limit,
        ..
    } = planning;
    let prefix = columns
        .iter()
        .map_while(|column| bound.get(column).cloned())
        .collect::<Vec<_>>();
    if prefix.is_empty() {
        return Ok(None);
    }
    let prefix_len = prefix.len();
    let (mut order_prefix_len, mut direction) =
        index_order_prefix(order_by, columns, prefix_len, schema, table, qualifier);
    let key = RelationalKey(prefix);
    let access_columns = columns[..prefix_len]
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let equality_covered =
        predicate_is_covered_by_equalities(planning.predicate, &access_columns, table, qualifier);
    let exclusive_bound = if equality_covered {
        None
    } else if order_prefix_len == order_by.len()
        && prefix_len.saturating_add(order_by.len()) == columns.len()
    {
        bind_canonical_keyset_bound(planning, &key, &access_columns)?
    } else {
        None
    };
    if !equality_covered && exclusive_bound.is_none() {
        order_prefix_len = 0;
        direction = RelationalIndexScanDirection::Forward;
    }
    let estimated_rows =
        match state.index_prefix_cardinality_at_most(table, &name, &key, *cardinality_limit) {
            Some(rows) => rows,
            None if !state.materialized_index_postings_resident() => {
                if unique && prefix_len == columns.len() {
                    usize::from(state.row_count(table) != 0)
                } else {
                    state.row_count(table).min(*cardinality_limit)
                }
            }
            None => {
                return Err(SkeinError::Execution(format!(
                    "relational index {name} on table {table} is not materialized"
                )));
            }
        }
        .max(1);
    Ok(Some(RelationalAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::Index,
            name: name.clone(),
            index_columns: columns.to_vec(),
            access_columns,
            equality_prefix_len: prefix_len,
            order_prefix_len,
            exclusive_range: exclusive_bound.is_some(),
            reverse_order: direction == RelationalIndexScanDirection::Backward,
            unique_point: unique && prefix_len == columns.len(),
            covering: false,
            requires_row_fetch: true,
            estimated_rows,
        },
        access: RelationalBaseAccess::Index {
            name,
            scan: RelationalIndexRangeScan {
                prefix: key,
                exclusive_bound,
                direction,
            },
        },
    }))
}

fn index_order_prefix(
    order_by: &[crate::sql::SqlOrderItem],
    index_columns: &[String],
    equality_prefix_len: usize,
    schema: &RelationalTableSchema,
    table: &str,
    qualifier: &str,
) -> (usize, RelationalIndexScanDirection) {
    if order_by.is_empty()
        || equality_prefix_len.saturating_add(order_by.len()) > index_columns.len()
    {
        return (0, RelationalIndexScanDirection::Forward);
    }
    let direction = match order_by[0].direction {
        SqlOrderDirection::Asc => RelationalIndexScanDirection::Forward,
        SqlOrderDirection::Desc => RelationalIndexScanDirection::Backward,
    };
    for (ordinal, item) in order_by.iter().enumerate() {
        if item.direction != order_by[0].direction
            || item
                .column
                .qualifier
                .as_deref()
                .is_some_and(|candidate| candidate != table && candidate != qualifier)
            || item.column.name != index_columns[equality_prefix_len + ordinal]
        {
            return (0, RelationalIndexScanDirection::Forward);
        }
        let Some(position) = schema.column_position(&item.column.name) else {
            return (0, RelationalIndexScanDirection::Forward);
        };
        if schema.columns[position].nullable
            && (direction == RelationalIndexScanDirection::Backward
                || !matches!(item.nulls, SqlNullOrder::First))
        {
            return (0, RelationalIndexScanDirection::Forward);
        }
    }
    (order_by.len(), direction)
}

fn predicate_is_covered_by_access(
    predicate: Option<&SqlPredicate>,
    access: &RelationalAccessPathDescriptor,
    order_by: &[crate::sql::SqlOrderItem],
    table: &str,
    qualifier: &str,
) -> bool {
    predicate_is_covered_by_equalities(predicate, &access.access_columns, table, qualifier)
        || (access.order_prefix_len == order_by.len()
            && canonical_keyset_values(
                predicate,
                &access.access_columns,
                order_by,
                table,
                qualifier,
            )
            .is_some())
}

fn predicate_is_covered_by_equalities(
    predicate: Option<&SqlPredicate>,
    access_columns: &BTreeSet<String>,
    table: &str,
    qualifier: &str,
) -> bool {
    fn covered(
        predicate: &SqlPredicate,
        access_columns: &BTreeSet<String>,
        table: &str,
        qualifier: &str,
        columns: &mut BTreeSet<String>,
    ) -> bool {
        match predicate {
            SqlPredicate::And(left, right) => {
                covered(left, access_columns, table, qualifier, columns)
                    && covered(right, access_columns, table, qualifier, columns)
            }
            SqlPredicate::Compare {
                left,
                op: SqlComparisonOp::Eq,
                ..
            } => {
                column_matches(left, table, qualifier)
                    && access_columns.contains(&left.name)
                    && columns.insert(left.name.clone())
            }
            _ => false,
        }
    }

    predicate.is_none_or(|predicate| {
        let mut columns = BTreeSet::new();
        covered(predicate, access_columns, table, qualifier, &mut columns)
            && columns == *access_columns
    })
}

fn bind_canonical_keyset_bound(
    planning: &RelationalBaseAccessPlanning<'_>,
    prefix: &RelationalKey,
    access_columns: &BTreeSet<String>,
) -> Result<Option<RelationalKey>> {
    let RelationalBaseAccessPlanning {
        predicate,
        order_by,
        schema,
        table,
        qualifier,
        parameters,
        ..
    } = planning;
    let Some((first, second)) =
        canonical_keyset_values(*predicate, access_columns, order_by, table, qualifier)
    else {
        return Ok(None);
    };
    let mut bound = prefix.0.clone();
    for (value, item) in [(first, &order_by[0]), (second, &order_by[1])] {
        let value = value_to_relational(bind_sql_value(value, parameters)?)?;
        let Some(position) = schema.column_position(&item.column.name) else {
            return Ok(None);
        };
        if schema.columns[position].nullable
            || matches!(value, RelationalValue::Null)
            || value.scalar_type() != Some(schema.columns[position].scalar_type)
        {
            return Ok(None);
        }
        bound.push(value);
    }
    Ok(Some(RelationalKey(bound)))
}

fn canonical_keyset_values<'a>(
    predicate: Option<&'a SqlPredicate>,
    access_columns: &BTreeSet<String>,
    order_by: &[crate::sql::SqlOrderItem],
    table: &str,
    qualifier: &str,
) -> Option<(&'a SqlValue, &'a SqlValue)> {
    let predicate = predicate?;
    if order_by.len() != 2 || order_by[0].direction != order_by[1].direction {
        return None;
    }
    let expected = match order_by[0].direction {
        SqlOrderDirection::Asc => SqlComparisonOp::Gt,
        SqlOrderDirection::Desc => SqlComparisonOp::Lt,
    };
    let mut terms = Vec::new();
    collect_conjuncts(predicate, &mut terms);
    let mut equality_columns = BTreeSet::new();
    let mut cursor = None;
    for term in terms {
        if let SqlPredicate::Compare {
            left,
            op: SqlComparisonOp::Eq,
            ..
        } = term
            && column_matches(left, table, qualifier)
            && access_columns.contains(&left.name)
        {
            if !equality_columns.insert(left.name.clone()) {
                return None;
            }
            continue;
        }
        if cursor.is_some() {
            return None;
        }
        cursor = match_keyset_or(
            term,
            &order_by[0].column,
            &order_by[1].column,
            expected,
            table,
            qualifier,
        );
        cursor?;
    }
    if equality_columns != *access_columns {
        return None;
    }
    cursor
}

fn collect_conjuncts<'a>(predicate: &'a SqlPredicate, output: &mut Vec<&'a SqlPredicate>) {
    match predicate {
        SqlPredicate::And(left, right) => {
            collect_conjuncts(left, output);
            collect_conjuncts(right, output);
        }
        predicate => output.push(predicate),
    }
}

fn match_keyset_or<'a>(
    predicate: &'a SqlPredicate,
    first_column: &SqlColumnRef,
    second_column: &SqlColumnRef,
    comparison: SqlComparisonOp,
    table: &str,
    qualifier: &str,
) -> Option<(&'a SqlValue, &'a SqlValue)> {
    let SqlPredicate::Or(left, right) = predicate else {
        return None;
    };
    match_keyset_branches(
        left,
        right,
        first_column,
        second_column,
        comparison,
        table,
        qualifier,
    )
    .or_else(|| {
        match_keyset_branches(
            right,
            left,
            first_column,
            second_column,
            comparison,
            table,
            qualifier,
        )
    })
}

fn match_keyset_branches<'a>(
    first_branch: &'a SqlPredicate,
    tie_branch: &'a SqlPredicate,
    first_column: &SqlColumnRef,
    second_column: &SqlColumnRef,
    comparison: SqlComparisonOp,
    table: &str,
    qualifier: &str,
) -> Option<(&'a SqlValue, &'a SqlValue)> {
    let first = match_column_comparison(first_branch, first_column, comparison, table, qualifier)?;
    let SqlPredicate::And(left, right) = tie_branch else {
        return None;
    };
    let tie = match_column_comparison(left, first_column, SqlComparisonOp::Eq, table, qualifier)
        .zip(match_column_comparison(
            right,
            second_column,
            comparison,
            table,
            qualifier,
        ))
        .or_else(|| {
            match_column_comparison(right, first_column, SqlComparisonOp::Eq, table, qualifier).zip(
                match_column_comparison(left, second_column, comparison, table, qualifier),
            )
        })?;
    (first == tie.0).then_some((first, tie.1))
}

fn match_column_comparison<'a>(
    predicate: &'a SqlPredicate,
    expected_column: &SqlColumnRef,
    expected_op: SqlComparisonOp,
    table: &str,
    qualifier: &str,
) -> Option<&'a SqlValue> {
    let SqlPredicate::Compare { left, op, right } = predicate else {
        return None;
    };
    (*op == expected_op
        && left.name == expected_column.name
        && column_matches(left, table, qualifier))
    .then_some(right)
}

fn column_matches(column: &SqlColumnRef, table: &str, qualifier: &str) -> bool {
    column
        .qualifier
        .as_deref()
        .is_none_or(|candidate| candidate == table || candidate == qualifier)
}

fn collect_conjunctive_equalities<'a>(
    predicate: &'a SqlPredicate,
    output: &mut Vec<(&'a SqlColumnRef, &'a SqlValue)>,
) {
    match predicate {
        SqlPredicate::And(left, right) => {
            collect_conjunctive_equalities(left, output);
            collect_conjunctive_equalities(right, output);
        }
        SqlPredicate::Compare {
            left,
            op: SqlComparisonOp::Eq,
            right,
        } => output.push((left, right)),
        _ => {}
    }
}

fn choose_join_access(
    predicate: &SqlPredicate,
    state: &RelationalState,
    schema: &RelationalTableSchema,
    table: &str,
    qualifier: &str,
    index_read_mode: RelationalIndexReadMode<'_>,
) -> Result<RelationalJoinAccessCandidate> {
    let mut bound = BTreeMap::<String, SqlColumnRef>::new();
    collect_conjunctive_join_equalities(predicate, table, qualifier, &mut bound);
    let row_count = state.row_count(table);
    let mut candidates = vec![RelationalJoinAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::FullScan,
            name: "__full_scan".to_string(),
            index_columns: Vec::new(),
            access_columns: BTreeSet::new(),
            equality_prefix_len: 0,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: false,
            estimated_rows: row_count.max(1),
        },
        access: RelationalJoinAccess::FullScan,
    }];

    if let Some(columns) = complete_join_columns(&schema.primary_key, &bound) {
        candidates.push(RelationalJoinAccessCandidate {
            descriptor: RelationalAccessPathDescriptor {
                kind: RelationalAccessPathKind::PrimaryKey,
                name: "__primary_key".to_string(),
                index_columns: schema.primary_key.clone(),
                access_columns: schema.primary_key.iter().cloned().collect(),
                equality_prefix_len: schema.primary_key.len(),
                order_prefix_len: 0,
                exclusive_range: false,
                reverse_order: false,
                unique_point: true,
                covering: false,
                requires_row_fetch: false,
                estimated_rows: usize::from(row_count != 0).max(1),
            },
            access: RelationalJoinAccess::PrimaryKey(columns),
        });
    }

    for (ordinal, columns) in schema.unique_constraints.iter().enumerate() {
        if let Some(candidate) = join_index_access_candidate(
            relational_unique_index_name(ordinal),
            columns,
            true,
            &bound,
            row_count,
            table,
            index_read_mode,
        ) {
            candidates.push(candidate);
        }
    }
    for index in &schema.indexes {
        if let Some(candidate) = join_index_access_candidate(
            index.name.clone(),
            &index.columns,
            index.unique,
            &bound,
            row_count,
            table,
            index_read_mode,
        ) {
            candidates.push(candidate);
        }
    }

    let selected = select_relational_access_path(
        candidates
            .iter()
            .map(|candidate| candidate.descriptor.clone()),
    )
    .map_err(|error| SkeinError::Execution(format!("invalid relational join access: {error}")))?
    .expect("full scan is always a join access-path candidate");
    let position = candidates
        .iter()
        .position(|candidate| candidate.descriptor == selected)
        .expect("selected relational join access path came from the candidate set");
    Ok(candidates.swap_remove(position))
}

fn collect_conjunctive_join_equalities(
    predicate: &SqlPredicate,
    table: &str,
    qualifier: &str,
    output: &mut BTreeMap<String, SqlColumnRef>,
) {
    match predicate {
        SqlPredicate::And(left, right) => {
            collect_conjunctive_join_equalities(left, table, qualifier, output);
            collect_conjunctive_join_equalities(right, table, qualifier, output);
        }
        SqlPredicate::CompareColumns {
            left,
            op: SqlComparisonOp::Eq,
            right,
        } => {
            let left_is_join = column_targets_join(left, table, qualifier);
            let right_is_join = column_targets_join(right, table, qualifier);
            match (left_is_join, right_is_join) {
                (true, false) => {
                    output
                        .entry(left.name.clone())
                        .or_insert_with(|| right.clone());
                }
                (false, true) => {
                    output
                        .entry(right.name.clone())
                        .or_insert_with(|| left.clone());
                }
                (true, true) | (false, false) => {}
            }
        }
        _ => {}
    }
}

fn column_targets_join(column: &SqlColumnRef, table: &str, qualifier: &str) -> bool {
    column
        .qualifier
        .as_deref()
        .is_some_and(|candidate| candidate == table || candidate == qualifier)
}

fn complete_join_columns(
    columns: &[String],
    bound: &BTreeMap<String, SqlColumnRef>,
) -> Option<Vec<(String, SqlColumnRef)>> {
    columns
        .iter()
        .map(|column| {
            bound
                .get(column)
                .cloned()
                .map(|outer| (column.clone(), outer))
        })
        .collect()
}

fn join_index_access_candidate(
    name: String,
    columns: &[String],
    unique: bool,
    bound: &BTreeMap<String, SqlColumnRef>,
    row_count: usize,
    table: &str,
    index_read_mode: RelationalIndexReadMode<'_>,
) -> Option<RelationalJoinAccessCandidate> {
    let access_columns = columns
        .iter()
        .map_while(|column| {
            bound
                .get(column)
                .cloned()
                .map(|outer| (column.clone(), outer))
        })
        .collect::<Vec<_>>();
    if access_columns.is_empty() {
        return None;
    }
    let equality_prefix_len = access_columns.len();
    let unique_point = unique && equality_prefix_len == columns.len();
    let estimated_rows = if unique_point {
        usize::from(row_count != 0)
    } else {
        index_read_mode
            .probe_statistics(table, &name, equality_prefix_len)
            .map(|statistics| {
                debug_assert!(statistics.distinct_non_null_values <= statistics.non_null_rows);
                debug_assert!(statistics.fanout <= statistics.non_null_rows);
                usize::try_from(statistics.fanout)
                    .unwrap_or(usize::MAX)
                    .min(row_count)
            })
            .unwrap_or(row_count)
    }
    .max(1);
    Some(RelationalJoinAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::Index,
            name: name.clone(),
            index_columns: columns.to_vec(),
            access_columns: columns[..equality_prefix_len].iter().cloned().collect(),
            equality_prefix_len,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point,
            covering: false,
            requires_row_fetch: true,
            estimated_rows,
        },
        access: RelationalJoinAccess::Index {
            name,
            columns: access_columns,
        },
    })
}

fn bound_join_key(
    row: &BoundRow<'_>,
    schema: &RelationalTableSchema,
    columns: &[(String, SqlColumnRef)],
) -> Result<Option<RelationalKey>> {
    let mut values = Vec::with_capacity(columns.len());
    for (join_column, outer_column) in columns {
        let value = resolve_column(row, outer_column)?.clone();
        if matches!(value, RelationalValue::Null) {
            return Ok(None);
        }
        let position = schema.column_position(join_column).ok_or_else(|| {
            SkeinError::Semantic(format!(
                "relational join table {} has no column {join_column}",
                schema.name
            ))
        })?;
        if value.scalar_type() != Some(schema.columns[position].scalar_type) {
            return Err(SkeinError::Semantic(format!(
                "relational join comparison on {join_column} has an incompatible scalar type"
            )));
        }
        values.push(value);
    }
    Ok(Some(RelationalKey(values)))
}

fn visit_join_entries<'a>(
    state: &'a RelationalState,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    planned: &PlannedJoin<'a>,
    row: &BoundRow<'a>,
    visit: &mut dyn FnMut(RelationalReadRow) -> Result<bool>,
) -> Result<bool> {
    let table = &planned.join.table.name;
    match &planned.access {
        RelationalJoinAccess::PrimaryKey(columns) => {
            let Some(key) = bound_join_key(row, planned.schema, columns)? else {
                return Ok(true);
            };
            match row_runtime.read_point(table, &key)? {
                Some(row) => visit(row),
                None => Ok(true),
            }
        }
        RelationalJoinAccess::Index { name, columns } => {
            let Some(prefix) = bound_join_key(row, planned.schema, columns)? else {
                return Ok(true);
            };
            index_runtime.visit_prefix(state, table, name, &prefix, |key| {
                match row_runtime.read_point(table, key)? {
                    Some(row) => visit(row),
                    None => Err(SkeinError::StorageIntegrity(format!(
                        "relational index {name} on table {table} points to missing row {key:?}"
                    ))),
                }
            })
        }
        RelationalJoinAccess::FullScan => row_runtime.visit_all(table, visit),
    }
}

fn visit_base_entries<'a>(
    state: &'a RelationalState,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    table: &str,
    access: &RelationalBaseAccess,
    visit: &mut dyn FnMut(RelationalReadRow) -> Result<bool>,
) -> Result<bool> {
    match access {
        RelationalBaseAccess::PrimaryKey(key) => match row_runtime.read_point(table, key)? {
            Some(row) => visit(row),
            None => Ok(true),
        },
        RelationalBaseAccess::Index { name, scan } => {
            index_runtime.visit_range_entries(state, table, name, scan, |_, key| match row_runtime
                .read_point(
                table, key,
            )? {
                Some(row) => visit(row),
                None => Err(SkeinError::StorageIntegrity(format!(
                    "relational index {name} on table {table} points to missing row {key:?}"
                ))),
            })
        }
        RelationalBaseAccess::FullScan => row_runtime.visit_all(table, visit),
    }
}

struct PreparedJoinTreeExecution<'a> {
    tree: &'a PreparedRelationalJoinTree,
    memory: &'a skein_executor::ExecutionMemoryConfig,
    memory_ledger: &'a QueryMemoryLedger,
    reports: RefCell<Vec<BlockingOperatorMemoryReport>>,
}

impl PreparedJoinTreeExecution<'_> {
    fn take_reports(&self) -> Vec<BlockingOperatorMemoryReport> {
        std::mem::take(&mut *self.reports.borrow_mut())
    }
}

fn bound_row_resident_bytes(row: &BoundRow<'_>) -> usize {
    std::mem::size_of::<BoundRow<'_>>()
        .saturating_add(
            row.bindings
                .len()
                .saturating_mul(std::mem::size_of::<Binding<'_>>()),
        )
        .saturating_add(
            row.bindings
                .iter()
                .filter_map(|binding| binding.row.as_ref())
                .map(RelationalReadRow::resident_bytes)
                .sum::<usize>(),
        )
}

fn visit_tree_relation_entries<'a>(
    state: &'a RelationalState,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    relation: &'a PreparedRelationalTreeRelation,
    outer: Option<&BoundRow<'a>>,
    visit: &mut dyn FnMut(RelationalReadRow) -> Result<bool>,
) -> Result<bool> {
    match &relation.access {
        PreparedRelationalTreeAccess::Base(access) => visit_base_entries(
            state,
            index_runtime,
            row_runtime,
            &relation.table,
            &access.access,
            visit,
        ),
        PreparedRelationalTreeAccess::Probe(access) => {
            let outer = outer.ok_or_else(|| {
                SkeinError::Execution(format!(
                    "prepared CSG-CMP probe for {} has no outer row",
                    relation.qualifier
                ))
            })?;
            match &access.access {
                RelationalJoinAccess::PrimaryKey(columns) => {
                    let schema = state.table_schema(&relation.table).ok_or_else(|| {
                        SkeinError::Semantic(format!("unknown relational table {}", relation.table))
                    })?;
                    let Some(key) = bound_join_key(outer, schema, columns)? else {
                        return Ok(true);
                    };
                    match row_runtime.read_point(&relation.table, &key)? {
                        Some(row) => visit(row),
                        None => Ok(true),
                    }
                }
                RelationalJoinAccess::Index { name, columns } => {
                    let schema = state.table_schema(&relation.table).ok_or_else(|| {
                        SkeinError::Semantic(format!("unknown relational table {}", relation.table))
                    })?;
                    let Some(prefix) = bound_join_key(outer, schema, columns)? else {
                        return Ok(true);
                    };
                    index_runtime.visit_prefix(state, &relation.table, name, &prefix, |key| {
                        match row_runtime.read_point(&relation.table, key)? {
                            Some(row) => visit(row),
                            None => Err(SkeinError::StorageIntegrity(format!(
                                "relational index {name} on table {} points to missing row {key:?}",
                                relation.table
                            ))),
                        }
                    })
                }
                RelationalJoinAccess::FullScan => row_runtime.visit_all(&relation.table, visit),
            }
        }
    }
}

fn null_extended_tree_row<'a>(
    node: &'a PreparedRelationalJoinTreeNode,
    state: &'a RelationalState,
) -> Result<BoundRow<'a>> {
    let mut bindings = Vec::new();
    let mut error = None;
    node.visit_relations(&mut |relation| {
        if error.is_some() {
            return;
        }
        match state.table_schema(&relation.table) {
            Some(schema) => bindings.push(Binding {
                table: &relation.table,
                qualifier: &relation.qualifier,
                schema,
                row: None,
            }),
            None => {
                error = Some(SkeinError::Semantic(format!(
                    "unknown relational table {}",
                    relation.table
                )));
            }
        }
    });
    if let Some(error) = error {
        return Err(error);
    }
    Ok(BoundRow { bindings })
}

#[allow(clippy::too_many_arguments)]
fn visit_prepared_join_tree_node<'a>(
    node: &'a PreparedRelationalJoinTreeNode,
    outer: Option<&BoundRow<'a>>,
    parameters: &[Value],
    state: &'a RelationalState,
    profiled_base_binding: BindingId,
    execution: &PreparedJoinTreeExecution<'a>,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    match node {
        PreparedRelationalJoinTreeNode::Relation(relation) => visit_tree_relation_entries(
            state,
            index_runtime,
            row_runtime,
            relation,
            outer,
            &mut |row| {
                if relation.binding == profiled_base_binding {
                    pipeline
                        .borrow_mut()
                        .account_operator_row(RelationalOperatorId::from_plan_index(0))?;
                } else if matches!(relation.access, PreparedRelationalTreeAccess::Base(_)) {
                    pipeline.borrow_mut().account_unprofiled_row()?;
                }
                let schema = state.table_schema(&relation.table).ok_or_else(|| {
                    SkeinError::Semantic(format!("unknown relational table {}", relation.table))
                })?;
                visit(BoundRow {
                    bindings: vec![Binding {
                        table: &relation.table,
                        qualifier: &relation.qualifier,
                        schema,
                        row: Some(row),
                    }],
                })
            },
        ),
        PreparedRelationalJoinTreeNode::Join {
            operator_id,
            kind,
            predicates,
            left,
            right,
        } => {
            let materialized_right =
                matches!(right.as_ref(), PreparedRelationalJoinTreeNode::Join { .. });
            let mut right_rows = Vec::new();
            let mut right_tracker = materialized_right.then(|| {
                OperatorMemoryTracker::with_account(
                    execution.memory.blocking_operator_bytes,
                    execution.memory_ledger.account(
                        QueryMemoryClass::BlockingState,
                        "RelationalBushyJoinMaterialize",
                        execution.memory.blocking_operator_bytes,
                    ),
                )
            });
            if let Some(tracker) = right_tracker.as_mut() {
                visit_prepared_join_tree_node(
                    right,
                    None,
                    parameters,
                    state,
                    profiled_base_binding,
                    execution,
                    pipeline,
                    index_runtime,
                    row_runtime,
                    &mut |row| {
                        let bytes = bound_row_resident_bytes(&row);
                        if tracker.would_exceed(bytes) {
                            return Err(SkeinError::Execution(format!(
                                "RelationalBushyJoinMaterialize state exceeds blocking_operator_bytes {}",
                                execution.memory.blocking_operator_bytes
                            )));
                        }
                        tracker.try_charge(bytes)?;
                        right_rows.push(row);
                        Ok(true)
                    },
                )?;
                execution
                    .reports
                    .borrow_mut()
                    .push(skein_executor::blocking::in_memory_report(
                        "RelationalBushyJoinMaterialize",
                        tracker,
                        tracker.peak_bytes,
                        right_rows.len(),
                        execution.memory,
                    ));
            }

            let null_right = (*kind == SqlJoinKind::Left)
                .then(|| null_extended_tree_row(right, state))
                .transpose()?;
            visit_prepared_join_tree_node(
                left,
                outer,
                parameters,
                state,
                profiled_base_binding,
                execution,
                pipeline,
                index_runtime,
                row_runtime,
                &mut |left_row| {
                    let mut matched = false;
                    let mut visit_right = |right_row: BoundRow<'a>| -> Result<bool> {
                        let mut combined = left_row.clone();
                        combined.bindings.extend(right_row.bindings);
                        for predicate in predicates {
                            if predicate_truth(predicate, &combined, parameters)? != Some(true) {
                                return Ok(true);
                            }
                        }
                        matched = true;
                        pipeline.borrow_mut().account_operator_row(*operator_id)?;
                        visit(combined)
                    };
                    let completed = if materialized_right {
                        let mut completed = true;
                        for right_row in &right_rows {
                            if !visit_right(right_row.clone())? {
                                completed = false;
                                break;
                            }
                        }
                        completed
                    } else {
                        visit_prepared_join_tree_node(
                            right,
                            Some(&left_row),
                            parameters,
                            state,
                            profiled_base_binding,
                            execution,
                            pipeline,
                            index_runtime,
                            row_runtime,
                            &mut visit_right,
                        )?
                    };
                    if !completed {
                        return Ok(false);
                    }
                    if !matched && let Some(null_right) = &null_right {
                        let mut combined = left_row;
                        combined.bindings.extend(null_right.bindings.clone());
                        pipeline.borrow_mut().account_operator_row(*operator_id)?;
                        return visit(combined);
                    }
                    Ok(true)
                },
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn visit_relational_rows<'a>(
    select: &'a SelectStatement,
    parameters: &[Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    tree_execution: Option<&PreparedJoinTreeExecution<'a>>,
    pipeline: &mut RelationalPipelineState<'_>,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    pipeline.begin_operator_pipeline();
    if let Some(execution) = tree_execution {
        let profiled_base_binding = execution.tree.root.first_relation().binding;
        let fully_consumed = {
            let tree_pipeline = RefCell::new(&mut *pipeline);
            visit_prepared_join_tree_node(
                &execution.tree.root,
                None,
                parameters,
                state,
                profiled_base_binding,
                execution,
                &tree_pipeline,
                index_runtime,
                row_runtime,
                &mut |row| {
                    if select
                        .selection
                        .as_ref()
                        .map(|selection| predicate_truth(selection, &row, parameters))
                        .transpose()?
                        .is_some_and(|truth| truth != Some(true))
                    {
                        return Ok(true);
                    }
                    visit(row)
                },
            )?
        };
        pipeline.finish_operator_pipeline(fully_consumed);
        return Ok(fully_consumed);
    }
    let fully_consumed = visit_base_entries(
        state,
        index_runtime,
        row_runtime,
        &select.from.name,
        base_access,
        &mut |row| {
            pipeline.account_operator_row(RelationalOperatorId::from_plan_index(0))?;
            visit_joined_row(
                select,
                parameters,
                state,
                joins,
                0,
                BoundRow {
                    bindings: vec![Binding {
                        table: &select.from.name,
                        qualifier: base_qualifier,
                        schema: base_schema,
                        row: Some(row),
                    }],
                },
                pipeline,
                index_runtime,
                row_runtime,
                visit,
            )
        },
    )?;
    pipeline.finish_operator_pipeline(fully_consumed);
    Ok(fully_consumed)
}

#[allow(clippy::too_many_arguments)]
fn visit_joined_row<'a>(
    select: &SelectStatement,
    parameters: &[Value],
    state: &'a RelationalState,
    joins: &'a [PlannedJoin<'a>],
    join_index: usize,
    row: BoundRow<'a>,
    pipeline: &mut RelationalPipelineState<'_>,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    let Some(planned) = joins.get(join_index) else {
        if select
            .selection
            .as_ref()
            .map(|selection| predicate_truth(selection, &row, parameters))
            .transpose()?
            .is_some_and(|truth| truth != Some(true))
        {
            return Ok(true);
        }
        return visit(row);
    };

    let mut matched = false;
    let completed = visit_join_entries(
        state,
        index_runtime,
        row_runtime,
        planned,
        &row,
        &mut |candidate| {
            let mut combined = row.clone();
            combined.bindings.push(Binding {
                table: &planned.join.table.name,
                qualifier: &planned.qualifier,
                schema: planned.schema,
                row: Some(candidate),
            });
            if predicate_truth(&planned.join.on, &combined, parameters)? != Some(true) {
                return Ok(true);
            }
            matched = true;
            pipeline.account_operator_row(RelationalOperatorId::from_plan_index(
                join_index.saturating_add(1),
            ))?;
            visit_joined_row(
                select,
                parameters,
                state,
                joins,
                join_index + 1,
                combined,
                pipeline,
                index_runtime,
                row_runtime,
                visit,
            )
        },
    )?;
    if !completed {
        return Ok(false);
    }
    if !matched && planned.join.kind == SqlJoinKind::Left {
        let mut combined = row;
        combined.bindings.push(Binding {
            table: &planned.join.table.name,
            qualifier: &planned.qualifier,
            schema: planned.schema,
            row: None,
        });
        pipeline.account_operator_row(RelationalOperatorId::from_plan_index(
            join_index.saturating_add(1),
        ))?;
        return visit_joined_row(
            select,
            parameters,
            state,
            joins,
            join_index + 1,
            combined,
            pipeline,
            index_runtime,
            row_runtime,
            visit,
        );
    }
    Ok(true)
}

struct StreamingProjectionOutput {
    rows: QueryRows,
    blocking_operator_memory_reports: Vec<BlockingOperatorMemoryReport>,
}

const RELATIONAL_SORT_COLUMN_PREFIX: &str = "__skein_relational_sort_";

#[derive(Default)]
struct RelationalBlockingObserver {
    reports: RefCell<Vec<BlockingOperatorMemoryReport>>,
}

impl ExecutionObserver for RelationalBlockingObserver {
    fn record_blocking_memory_report(&self, report: BlockingOperatorMemoryReport) {
        self.reports.borrow_mut().push(report);
    }
}

struct ProjectedBatchSource<'a, 'pipeline> {
    select: &'a SelectStatement,
    parameters: &'a [Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &'a RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    tree_execution: Option<&'pipeline PreparedJoinTreeExecution<'a>>,
    pipeline: &'pipeline mut RelationalPipelineState<'a>,
    index_runtime: &'pipeline RelationalIndexRuntime<'a>,
    row_runtime: &'pipeline RelationalRowRuntime<'a>,
    batch_rows: usize,
}

struct DistinctAggregateValueBatchSource<'a, 'pipeline> {
    select: &'a SelectStatement,
    column: &'a SqlColumnRef,
    parameters: &'a [Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &'a RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    tree_execution: Option<&'pipeline PreparedJoinTreeExecution<'a>>,
    pipeline: &'pipeline mut RelationalPipelineState<'a>,
    index_runtime: &'pipeline RelationalIndexRuntime<'a>,
    row_runtime: &'pipeline RelationalRowRuntime<'a>,
    batch_rows: usize,
}

impl BindingBatchSource for DistinctAggregateValueBatchSource<'_, '_> {
    fn execute(
        &mut self,
        _input: &PhysicalPlan,
        _execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let mut batch = Vec::with_capacity(self.batch_rows);
        let mut control = BatchControl::Continue;
        visit_relational_rows(
            self.select,
            self.parameters,
            self.state,
            self.base_schema,
            self.base_qualifier,
            self.base_access,
            self.joins,
            self.tree_execution,
            self.pipeline,
            self.index_runtime,
            self.row_runtime,
            &mut |row| {
                let value = resolve_column(&row, self.column)?;
                if matches!(value, RelationalValue::Null) {
                    return Ok(true);
                }
                batch.push(ExecutorBinding::scalar(
                    "value",
                    relational_sort_value(value)?,
                ));
                if batch.len() == self.batch_rows {
                    control = emit(std::mem::replace(
                        &mut batch,
                        Vec::with_capacity(self.batch_rows),
                    ))?;
                }
                Ok(control == BatchControl::Continue)
            },
        )?;
        if control == BatchControl::Continue && !batch.is_empty() {
            control = emit(batch)?;
        }
        Ok(control)
    }
}

impl BindingBatchSource for ProjectedBatchSource<'_, '_> {
    fn execute(
        &mut self,
        _input: &PhysicalPlan,
        _execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let mut batch = Vec::with_capacity(self.batch_rows);
        let mut control = BatchControl::Continue;
        visit_relational_rows(
            self.select,
            self.parameters,
            self.state,
            self.base_schema,
            self.base_qualifier,
            self.base_access,
            self.joins,
            self.tree_execution,
            self.pipeline,
            self.index_runtime,
            self.row_runtime,
            &mut |row| {
                let projected = project_bound_row(&row, &self.select.projection)?;
                batch.push(ExecutorBinding::values(projected));
                if batch.len() == self.batch_rows {
                    control = emit(std::mem::replace(
                        &mut batch,
                        Vec::with_capacity(self.batch_rows),
                    ))?;
                }
                Ok(control == BatchControl::Continue)
            },
        )?;
        if control == BatchControl::Continue && !batch.is_empty() {
            control = emit(batch)?;
        }
        Ok(control)
    }
}

struct DistinctBatchSource<'a> {
    input: &'a mut dyn BindingBatchSource,
    input_plan: &'a PhysicalPlan,
    catalog: &'a Catalog,
    memory: &'a skein_executor::ExecutionMemoryConfig,
    memory_ledger: &'a QueryMemoryLedger,
    task_context: Option<&'a skein_core::RuntimeTaskContext>,
    observer: &'a dyn ExecutionObserver,
}

impl BindingBatchSource for DistinctBatchSource<'_> {
    fn execute(
        &mut self,
        _input: &PhysicalPlan,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        stream_distinct_batches(
            self.input_plan,
            self.input,
            BlockingExecutionContext {
                catalog: self.catalog,
                memory: self.memory,
                memory_ledger: self.memory_ledger,
                task_context: self.task_context,
                observer: self.observer,
            },
            execution_limit,
            emit,
        )
    }
}

struct ProjectedSortKeyBatchSource<'a> {
    input: &'a mut dyn BindingBatchSource,
    order_columns: &'a [(String, crate::sql::SqlOrderItem)],
}

impl BindingBatchSource for ProjectedSortKeyBatchSource<'_> {
    fn execute(
        &mut self,
        input_plan: &PhysicalPlan,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        self.input
            .execute(input_plan, execution_limit, &mut |mut batch| {
                for binding in &mut batch {
                    for (ordinal, (column, item)) in self.order_columns.iter().enumerate() {
                        let value = binding.values.get(column).cloned().ok_or_else(|| {
                            SkeinError::Semantic(format!(
                                "DISTINCT ORDER BY column {} is not projected",
                                item.column.name
                            ))
                        })?;
                        binding.values.insert(
                            relational_sort_column(ordinal),
                            postgres_sort_key(value, item.direction, item.nulls),
                        );
                    }
                }
                emit(batch)
            })
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_blocking_projection<'a>(
    select: &'a SelectStatement,
    parameters: &'a [Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &'a RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    tree_execution: Option<&PreparedJoinTreeExecution<'a>>,
    pipeline: &mut RelationalPipelineState<'a>,
    index_runtime: &RelationalIndexRuntime<'a>,
    row_runtime: &RelationalRowRuntime<'a>,
    limits: RelationalQueryLimits,
    memory: &skein_executor::ExecutionMemoryConfig,
    memory_ledger: &QueryMemoryLedger,
) -> Result<StreamingProjectionOutput> {
    let offset = usize::try_from(bind_bound(select.offset, parameters, "OFFSET")?.unwrap_or(0))
        .map_err(|_| SkeinError::Semantic("SQL OFFSET is too large".to_string()))?;
    let requested = bind_bound(select.limit, parameters, "LIMIT")?
        .map(|value| {
            usize::try_from(value)
                .map_err(|_| SkeinError::Semantic("SQL LIMIT is too large".to_string()))
        })
        .transpose()?
        .unwrap_or(usize::MAX);
    let detection_limit = requested.min(limits.max_output_rows.saturating_add(1));
    let input_plan = relational_input_plan();
    let catalog = Catalog::default();
    let observer = RelationalBlockingObserver::default();
    let task_context = pipeline.task_context;
    let mut output = Vec::with_capacity(detection_limit.min(limits.max_output_rows));
    let mut payload_bytes = 0usize;

    if detection_limit == 0 {
        return Ok(StreamingProjectionOutput {
            rows: output.into(),
            blocking_operator_memory_reports: Vec::new(),
        });
    }

    if select.distinct {
        let mut projected = ProjectedBatchSource {
            select,
            parameters,
            state,
            base_schema,
            base_qualifier,
            base_access,
            joins,
            tree_execution,
            pipeline,
            index_runtime,
            row_runtime,
            batch_rows: memory.batch_rows.get(),
        };
        let mut distinct = DistinctBatchSource {
            input: &mut projected,
            input_plan: &input_plan,
            catalog: &catalog,
            memory,
            memory_ledger,
            task_context,
            observer: &observer,
        };
        if select.order_by.is_empty() {
            let mut skipped = 0usize;
            stream_distinct_batches(
                &input_plan,
                distinct.input,
                BlockingExecutionContext {
                    catalog: &catalog,
                    memory,
                    memory_ledger,
                    task_context,
                    observer: &observer,
                },
                ExecutionLimit {
                    output_rows: Some(offset.saturating_add(detection_limit)),
                },
                &mut |batch| {
                    let batch = batch
                        .into_iter()
                        .filter(|_| {
                            if skipped < offset {
                                skipped += 1;
                                false
                            } else {
                                true
                            }
                        })
                        .collect();
                    consume_projected_batch(
                        batch,
                        &mut output,
                        &mut payload_bytes,
                        detection_limit,
                        limits,
                    )
                },
            )?;
        } else {
            let order_columns = projected_order_columns(select)?;
            let mut keyed = ProjectedSortKeyBatchSource {
                input: &mut distinct,
                order_columns: &order_columns,
            };
            execute_relational_order(
                &input_plan,
                &mut keyed,
                select,
                offset,
                detection_limit,
                &catalog,
                memory,
                memory_ledger,
                task_context,
                &observer,
                &mut |batch| {
                    consume_projected_batch(
                        batch,
                        &mut output,
                        &mut payload_bytes,
                        detection_limit,
                        limits,
                    )
                },
            )?;
        }
    } else {
        let locator_layout = match tree_execution {
            Some(execution) => relational_join_tree_locator_layout(state, execution.tree)?,
            None => {
                relational_locator_layout(&select.from.name, base_qualifier, base_schema, joins)?
            }
        };
        let mut order = ExternalTopN::new(
            "TopNExec",
            "relational-topn",
            offset,
            detection_limit,
            memory,
            memory_ledger,
            task_context,
        );
        visit_relational_rows(
            select,
            parameters,
            state,
            base_schema,
            base_qualifier,
            base_access,
            joins,
            tree_execution,
            pipeline,
            index_runtime,
            row_runtime,
            &mut |row| {
                let sort_keys = select
                    .order_by
                    .iter()
                    .map(|item| {
                        RelationalSortKey::new(
                            resolve_column(&row, &item.column)?.clone(),
                            item.direction,
                            item.nulls,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                order.push(RelationalSortRecord::new(
                    sort_keys,
                    typed_row_set_locator(&row)?,
                ))?;
                Ok(true)
            },
        )?;
        let report = order.finish(|record| {
            let row = project_typed_locator(
                &record.into_locator(),
                &locator_layout,
                select,
                row_runtime,
            )?;
            push_relational_output(row, &mut output, &mut payload_bytes, limits)?;
            Ok(output.len() < detection_limit)
        })?;
        observer.record_blocking_memory_report(report);
    }
    Ok(StreamingProjectionOutput {
        rows: output.into(),
        blocking_operator_memory_reports: observer.reports.into_inner(),
    })
}

fn consume_projected_batch(
    batch: BindingBatch,
    output: &mut Vec<Row>,
    payload_bytes: &mut usize,
    detection_limit: usize,
    limits: RelationalQueryLimits,
) -> Result<BatchControl> {
    for mut binding in batch {
        if output.len() >= detection_limit {
            return Ok(BatchControl::Stop);
        }
        strip_relational_sort_columns(&mut binding.values);
        push_relational_output(binding.values, output, payload_bytes, limits)?;
    }
    Ok(BatchControl::Continue)
}

fn push_relational_output(
    row: Row,
    output: &mut Vec<Row>,
    payload_bytes: &mut usize,
    limits: RelationalQueryLimits,
) -> Result<()> {
    if output.len() >= limits.max_output_rows {
        return Err(SkeinError::Execution(format!(
            "relational SQL output exceeds max_output_rows {}",
            limits.max_output_rows
        )));
    }
    *payload_bytes = payload_bytes.saturating_add(map_payload_bytes(&row));
    if *payload_bytes > limits.max_output_payload_bytes {
        return Err(SkeinError::Execution(format!(
            "relational SQL output exceeds max_output_payload_bytes {}",
            limits.max_output_payload_bytes
        )));
    }
    output.push(row);
    Ok(())
}

fn relational_input_plan() -> PhysicalPlan {
    PhysicalPlan::SeqNodeScan {
        variable: "__relational_input".to_string(),
        label: String::new(),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_relational_order(
    input_plan: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    select: &SelectStatement,
    offset: usize,
    limit: usize,
    catalog: &Catalog,
    memory: &skein_executor::ExecutionMemoryConfig,
    memory_ledger: &QueryMemoryLedger,
    task_context: Option<&skein_core::RuntimeTaskContext>,
    observer: &dyn ExecutionObserver,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    if limit == 0 {
        return Ok(BatchControl::Continue);
    }
    let items = select
        .order_by
        .iter()
        .enumerate()
        .map(|(ordinal, item)| SortItem {
            key: SortKey::Column(relational_sort_column(ordinal)),
            direction: match item.direction {
                SqlOrderDirection::Asc => SortDirection::Asc,
                SqlOrderDirection::Desc => SortDirection::Desc,
            },
        })
        .collect::<Vec<_>>();
    stream_top_n_batches(
        input_plan,
        &items,
        offset,
        limit,
        source,
        BlockingExecutionContext {
            catalog,
            memory,
            memory_ledger,
            task_context,
            observer,
        },
        ExecutionLimit {
            output_rows: Some(limit),
        },
        emit,
    )
}

fn relational_sort_column(ordinal: usize) -> String {
    format!("{RELATIONAL_SORT_COLUMN_PREFIX}{ordinal}")
}

fn strip_relational_sort_columns(row: &mut Row) {
    row.retain(|name, _| !name.starts_with(RELATIONAL_SORT_COLUMN_PREFIX));
}

fn relational_sort_value(value: &RelationalValue) -> Result<Value> {
    match value {
        RelationalValue::Null => Ok(Value::Null),
        RelationalValue::Boolean(value) => Ok(Value::Bool(*value)),
        RelationalValue::BigInt(value) => Ok(Value::Int(*value)),
        RelationalValue::DoublePrecision(value) => Ok(Value::Float(*value)),
        RelationalValue::Text(value) => Ok(Value::String(value.clone())),
        RelationalValue::Bytea(value) => Ok(Value::Binary(value.clone())),
        RelationalValue::Overflow(_) => Err(SkeinError::Execution(
            "ORDER BY requires overflow hydration before qualification".to_string(),
        )),
    }
}

fn postgres_sort_key(value: Value, direction: SqlOrderDirection, nulls: SqlNullOrder) -> Value {
    let is_null = value == Value::Null;
    let nulls_first = match nulls {
        SqlNullOrder::First => true,
        SqlNullOrder::Last => false,
        SqlNullOrder::DialectDefault => direction == SqlOrderDirection::Desc,
    };
    let null_rank = match direction {
        SqlOrderDirection::Asc => usize::from(!nulls_first),
        SqlOrderDirection::Desc => usize::from(nulls_first),
    };
    let rank = if is_null { null_rank } else { 1 - null_rank };
    Value::List(vec![Value::Int(rank as i64), value])
}

fn projected_order_columns(
    select: &SelectStatement,
) -> Result<Vec<(String, crate::sql::SqlOrderItem)>> {
    select
        .order_by
        .iter()
        .map(|item| {
            let output = select
                .projection
                .iter()
                .find_map(|projection| match projection {
                    SelectProjection::Wildcard => Some(item.column.name.clone()),
                    SelectProjection::Column { name, alias }
                        if name.name == item.column.name
                            && item.column.qualifier.as_deref().is_none_or(|qualifier| {
                                name.qualifier.as_deref() == Some(qualifier)
                                    || select.from.name == qualifier
                                    || select.from_alias.as_deref() == Some(qualifier)
                            }) =>
                    {
                        Some(alias.clone().unwrap_or_else(|| name.name.clone()))
                    }
                    SelectProjection::Column { .. } | SelectProjection::Expression { .. } => None,
                });
            output.map(|output| (output, item.clone())).ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "SELECT DISTINCT requires ORDER BY column {} to appear in the projection",
                    item.column.name
                ))
            })
        })
        .collect()
}

fn typed_row_set_locator(row: &BoundRow<'_>) -> Result<RelationalRowSetLocator> {
    row.bindings
        .iter()
        .enumerate()
        .map(|(table_id, binding)| {
            binding
                .primary_key()
                .cloned()
                .map(|primary_key| {
                    u32::try_from(table_id)
                        .map(|table_id| RelationalRowLocator::new(table_id, primary_key))
                        .map_err(|_| {
                            SkeinError::Execution(
                                "typed relational locator table count exceeds u32".to_string(),
                            )
                        })
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()
        .map(RelationalRowSetLocator::new)
}

fn project_typed_locator(
    locator: &RelationalRowSetLocator,
    locator_layout: &RelationalLocatorLayout<'_>,
    select: &SelectStatement,
    row_runtime: &RelationalRowRuntime<'_>,
) -> Result<Row> {
    with_typed_locator_bound_row(locator, locator_layout, row_runtime, |bound| {
        project_bound_row(bound, &select.projection)
    })
}

fn with_typed_locator_bound_row<T>(
    locator: &RelationalRowSetLocator,
    locator_layout: &RelationalLocatorLayout<'_>,
    row_runtime: &RelationalRowRuntime<'_>,
    visit: impl FnOnce(&BoundRow<'_>) -> Result<T>,
) -> Result<T> {
    locator_layout.validate(locator)?;
    let mut bound = BoundRow {
        bindings: Vec::with_capacity(locator.rows().len()),
    };
    for (binding, locator) in locator_layout.bindings.iter().zip(locator.rows()) {
        let row = match locator {
            Some(locator) => row_runtime
                .read_output_point(binding.table, locator.primary_key())?
                .map(Some)
                .ok_or_else(|| {
                    SkeinError::StorageIntegrity(format!(
                        "typed relational locator references a missing row in table {}",
                        binding.table
                    ))
                })?,
            None => None,
        };
        bound.bindings.push(Binding {
            table: binding.table,
            qualifier: binding.qualifier,
            schema: binding.schema,
            row,
        });
    }
    visit(&bound)
}

#[allow(clippy::too_many_arguments)]
fn execute_ordered_index_projection<'a>(
    select: &'a SelectStatement,
    parameters: &[Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &RelationalBaseAccess,
    pipeline: &mut RelationalPipelineState<'_>,
    index_runtime: &RelationalIndexRuntime<'a>,
    row_runtime: &RelationalRowRuntime<'a>,
    limits: RelationalQueryLimits,
    execution_memory: &skein_executor::ExecutionMemoryConfig,
    memory_ledger: &QueryMemoryLedger,
) -> Result<StreamingProjectionOutput> {
    let RelationalBaseAccess::Index { name, scan } = base_access else {
        return Err(SkeinError::Execution(
            "ordered relational projection requires an index range access".to_string(),
        ));
    };
    let mut offset = usize::try_from(bind_bound(select.offset, parameters, "OFFSET")?.unwrap_or(0))
        .map_err(|_| SkeinError::Semantic("SQL OFFSET is too large".to_string()))?;
    let requested = bind_bound(select.limit, parameters, "LIMIT")?
        .map(|value| {
            usize::try_from(value)
                .map_err(|_| SkeinError::Semantic("SQL LIMIT is too large".to_string()))
        })
        .transpose()?
        .unwrap_or(usize::MAX);
    let mut output = Vec::with_capacity(requested.min(limits.max_output_rows));
    let mut payload_bytes = 0usize;
    let mut locator_batch = AccountedRelationalLocatorBatch::new(
        execution_memory.batch_rows.get(),
        execution_memory.batch_payload_bytes,
        memory_ledger,
    )?;
    let mut hydrate = |batch: ColumnarBatch| -> Result<BatchControl> {
        let locators = batch.column(RELATIONAL_ROW_LOCATOR_SLOT).ok_or_else(|| {
            SkeinError::Execution("ordered locator batch is missing its locator column".to_string())
        })?;
        for row_index in batch.selection().iter() {
            if output.len() >= requested {
                return Ok(BatchControl::Stop);
            }
            if output.len() >= limits.max_output_rows {
                return Err(SkeinError::Execution(format!(
                    "relational SQL output exceeds max_output_rows {}",
                    limits.max_output_rows
                )));
            }
            let locator = locators.relational_row_locator(row_index).ok_or_else(|| {
                SkeinError::Execution(format!(
                    "ordered locator batch row {row_index} is not a relational row locator"
                ))
            })?;
            let row = row_runtime
                .read_output_point(&select.from.name, locator.primary_key())?
                .ok_or_else(|| {
                    SkeinError::StorageIntegrity(format!(
                        "relational index {name} on table {} points to missing row {:?}",
                        select.from.name,
                        locator.primary_key()
                    ))
                })?;
            let bound = BoundRow {
                bindings: vec![Binding {
                    table: &select.from.name,
                    qualifier: base_qualifier,
                    schema: base_schema,
                    row: Some(row),
                }],
            };
            let projected = project_bound_row(&bound, &select.projection)?;
            payload_bytes = payload_bytes.saturating_add(map_payload_bytes(&projected));
            if payload_bytes > limits.max_output_payload_bytes {
                return Err(SkeinError::Execution(format!(
                    "relational SQL output exceeds max_output_payload_bytes {}",
                    limits.max_output_payload_bytes
                )));
            }
            output.push(projected);
        }
        Ok(if output.len() >= requested {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        })
    };
    let mut selected_rows = 0usize;
    if requested != 0 {
        pipeline.begin_operator_pipeline();
        let fully_consumed = index_runtime.visit_range_entries(
            state,
            &select.from.name,
            name,
            scan,
            |_, primary_key| {
                pipeline.account_operator_row(RelationalOperatorId::from_plan_index(0))?;
                if offset != 0 {
                    offset -= 1;
                    return Ok(true);
                }
                if selected_rows >= requested {
                    return Ok(false);
                }
                let control = locator_batch.push(
                    RelationalRowLocator::new(0, primary_key.clone()),
                    &mut hydrate,
                )?;
                selected_rows = selected_rows.saturating_add(1);
                if control == BatchControl::Stop {
                    return Ok(false);
                }
                if selected_rows >= requested {
                    return Ok(locator_batch.emit(&mut hydrate)? != BatchControl::Stop);
                }
                Ok(true)
            },
        )?;
        pipeline.finish_operator_pipeline(fully_consumed);
        locator_batch.emit(&mut hydrate)?;
    }
    Ok(StreamingProjectionOutput {
        rows: output.into(),
        blocking_operator_memory_reports: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_streaming_projection<'a>(
    select: &'a SelectStatement,
    parameters: &[Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    tree_execution: Option<&PreparedJoinTreeExecution<'a>>,
    pipeline: &mut RelationalPipelineState<'_>,
    index_runtime: &RelationalIndexRuntime<'a>,
    row_runtime: &RelationalRowRuntime<'a>,
    limits: RelationalQueryLimits,
) -> Result<StreamingProjectionOutput> {
    if tree_execution.is_none()
        && joins.is_empty()
        && matches!(base_access, RelationalBaseAccess::FullScan)
    {
        return execute_borrowed_streaming_full_scan(
            select,
            parameters,
            base_schema,
            base_qualifier,
            pipeline,
            row_runtime,
            limits,
        );
    }
    let mut offset = usize::try_from(bind_bound(select.offset, parameters, "OFFSET")?.unwrap_or(0))
        .map_err(|_| SkeinError::Semantic("SQL OFFSET is too large".to_string()))?;
    let requested = bind_bound(select.limit, parameters, "LIMIT")?
        .map(|value| {
            usize::try_from(value)
                .map_err(|_| SkeinError::Semantic("SQL LIMIT is too large".to_string()))
        })
        .transpose()?
        .unwrap_or(usize::MAX);
    let mut output = Vec::with_capacity(requested.min(limits.max_output_rows));
    let mut payload_bytes = 0usize;
    if requested != 0 {
        visit_relational_rows(
            select,
            parameters,
            state,
            base_schema,
            base_qualifier,
            base_access,
            joins,
            tree_execution,
            pipeline,
            index_runtime,
            row_runtime,
            &mut |row| {
                if offset != 0 {
                    offset -= 1;
                    return Ok(true);
                }
                if output.len() >= requested {
                    return Ok(false);
                }
                if output.len() >= limits.max_output_rows {
                    return Err(SkeinError::Execution(format!(
                        "relational SQL output exceeds max_output_rows {}",
                        limits.max_output_rows
                    )));
                }
                let projected = project_bound_row(&row, &select.projection)?;
                payload_bytes = payload_bytes.saturating_add(map_payload_bytes(&projected));
                if payload_bytes > limits.max_output_payload_bytes {
                    return Err(SkeinError::Execution(format!(
                        "relational SQL output exceeds max_output_payload_bytes {}",
                        limits.max_output_payload_bytes
                    )));
                }
                output.push(projected);
                Ok(output.len() < requested)
            },
        )?;
    }
    Ok(StreamingProjectionOutput {
        rows: output.into(),
        blocking_operator_memory_reports: Vec::new(),
    })
}

fn execute_borrowed_streaming_full_scan(
    select: &SelectStatement,
    parameters: &[Value],
    schema: &RelationalTableSchema,
    qualifier: &str,
    pipeline: &mut RelationalPipelineState<'_>,
    row_runtime: &RelationalRowRuntime<'_>,
    limits: RelationalQueryLimits,
) -> Result<StreamingProjectionOutput> {
    let predicate = select
        .selection
        .as_ref()
        .map(|predicate| {
            BoundStreamingPredicate::bind(
                predicate,
                parameters,
                schema,
                &select.from.name,
                qualifier,
            )
        })
        .transpose()?;
    let projection =
        BoundStreamingProjection::bind(&select.projection, schema, &select.from.name, qualifier)?;
    let mut offset = usize::try_from(bind_bound(select.offset, parameters, "OFFSET")?.unwrap_or(0))
        .map_err(|_| SkeinError::Semantic("SQL OFFSET is too large".to_string()))?;
    let requested = bind_bound(select.limit, parameters, "LIMIT")?
        .map(|value| {
            usize::try_from(value)
                .map_err(|_| SkeinError::Semantic("SQL LIMIT is too large".to_string()))
        })
        .transpose()?
        .unwrap_or(usize::MAX);
    let mut output = QueryRowsBuilder::with_schema(
        projection.schema().clone(),
        requested.min(limits.max_output_rows),
    );
    let mut output_rows = 0usize;
    let mut payload_bytes = 0usize;
    if requested != 0 {
        pipeline.begin_operator_pipeline();
        let fully_consumed = row_runtime.visit_all_ref(&select.from.name, |row| {
            pipeline.account_operator_row(RelationalOperatorId::from_plan_index(0))?;
            if predicate
                .as_ref()
                .map(|predicate| predicate.truth(row))
                .transpose()?
                .is_some_and(|truth| truth != Some(true))
            {
                return Ok(true);
            }
            if offset != 0 {
                offset -= 1;
                return Ok(true);
            }
            if output_rows >= requested {
                return Ok(false);
            }
            if output_rows >= limits.max_output_rows {
                return Err(SkeinError::Execution(format!(
                    "relational SQL output exceeds max_output_rows {}",
                    limits.max_output_rows
                )));
            }
            projection.project_into(
                row,
                &mut output,
                &mut payload_bytes,
                limits.max_output_payload_bytes,
            )?;
            output_rows = output_rows.saturating_add(1);
            Ok(output_rows < requested)
        })?;
        pipeline.finish_operator_pipeline(fully_consumed);
    }
    Ok(StreamingProjectionOutput {
        rows: output.finish(),
        blocking_operator_memory_reports: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_aggregate_select<'a>(
    select: &'a SelectStatement,
    parameters: &'a [Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &'a RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    tree_execution: Option<&PreparedJoinTreeExecution<'a>>,
    pipeline: &mut RelationalPipelineState<'a>,
    index_runtime: &RelationalIndexRuntime<'a>,
    row_runtime: &RelationalRowRuntime<'a>,
    limits: RelationalQueryLimits,
    execution_memory: &skein_executor::ExecutionMemoryConfig,
    memory_ledger: &QueryMemoryLedger,
    access_path: RelationalAccessPathDescriptor,
    join_access_paths: Vec<RelationalAccessPathDescriptor>,
    join_planning: &RelationalJoinPlanningOutcome,
) -> Result<RelationalQueryOutput> {
    if let Some((column, output_name)) = single_count_distinct_column(select) {
        return execute_single_count_distinct(
            select,
            column,
            output_name,
            parameters,
            state,
            base_schema,
            base_qualifier,
            base_access,
            joins,
            tree_execution,
            pipeline,
            index_runtime,
            row_runtime,
            limits,
            execution_memory,
            memory_ledger,
            access_path,
            join_access_paths,
            join_planning,
        );
    }
    if !select.group_by.is_empty() {
        return execute_grouped_aggregate(
            select,
            parameters,
            state,
            base_schema,
            base_qualifier,
            base_access,
            joins,
            tree_execution,
            pipeline,
            index_runtime,
            row_runtime,
            limits,
            execution_memory,
            memory_ledger,
            access_path,
            join_access_paths,
            join_planning,
        );
    }
    if !select.order_by.is_empty() || select.distinct {
        return Err(SkeinError::Semantic(
            "aggregate SELECT does not yet support statement DISTINCT or ORDER BY".to_string(),
        ));
    }
    if let Some(mut aggregate) = ColumnarAggregateExecutor::try_new(
        select,
        base_schema,
        &select.from.name,
        base_qualifier,
        joins.is_empty(),
        execution_memory.batch_rows.get(),
        execution_memory.batch_payload_bytes,
        memory_ledger,
    )? {
        let mut memory_tracker = OperatorMemoryTracker::with_account(
            execution_memory.blocking_operator_bytes,
            memory_ledger.account(
                QueryMemoryClass::BlockingState,
                "RelationalColumnarAggregate",
                execution_memory.blocking_operator_bytes,
            ),
        );
        charge_aggregate_memory(aggregate.blocking_state_bytes(), &mut memory_tracker)?;
        let mut aggregate_input_rows = 0usize;
        visit_relational_rows(
            select,
            parameters,
            state,
            base_schema,
            base_qualifier,
            base_access,
            joins,
            tree_execution,
            pipeline,
            index_runtime,
            row_runtime,
            &mut |row| {
                aggregate_input_rows = aggregate_input_rows.saturating_add(1);
                aggregate.push(&row)?;
                Ok(true)
            },
        )?;
        pipeline.finish()?;
        let intermediate_rows = pipeline.intermediate_rows;
        let finished = aggregate.finish()?;
        let offset = usize::try_from(bind_bound(select.offset, parameters, "OFFSET")?.unwrap_or(0))
            .map_err(|_| SkeinError::Semantic("SQL OFFSET is too large".to_string()))?;
        let requested = bind_bound(select.limit, parameters, "LIMIT")?
            .map(|value| usize::try_from(value).unwrap_or(usize::MAX))
            .unwrap_or(usize::MAX);
        let mut rows = Vec::new();
        if offset == 0 && requested != 0 {
            if limits.max_output_rows == 0 {
                return Err(SkeinError::Execution(
                    "relational SQL output exceeds max_output_rows 0".to_string(),
                ));
            }
            let row = finished.into_iter().collect::<Row>();
            if map_payload_bytes(&row) > limits.max_output_payload_bytes {
                return Err(SkeinError::Execution(format!(
                    "relational SQL output exceeds max_output_payload_bytes {}",
                    limits.max_output_payload_bytes
                )));
            }
            rows.push(row);
        }
        return Ok(RelationalQueryOutput {
            rows: rows.into(),
            stage_timings: RelationalSqlStageTimings::default(),
            join_planning: join_planning.clone(),
            operator_cardinality_profiles: pipeline.operator_cardinality_profiles(),
            intermediate_rows,
            hydration: row_runtime.hydration(),
            access_path,
            join_access_paths,
            index_execution_evidence: index_runtime.evidence(),
            row_execution_evidence: row_runtime.evidence(),
            blocking_operator_memory_reports: vec![skein_executor::blocking::in_memory_report(
                "RelationalAggregateExec",
                &memory_tracker,
                memory_tracker.peak_bytes,
                aggregate_input_rows,
                execution_memory,
            )],
        });
    }
    let projection_template = select
        .projection
        .iter()
        .map(|projection| AggregateProjectionState::new(projection, parameters))
        .collect::<Result<Vec<_>>>()?;
    let mut groups = BTreeMap::<Vec<RelationalValue>, Vec<AggregateProjectionState>>::new();
    let mut memory_tracker = OperatorMemoryTracker::with_account(
        execution_memory.blocking_operator_bytes,
        memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "RelationalAggregate",
            execution_memory.blocking_operator_bytes,
        ),
    );
    let mut aggregate_input_rows = 0usize;
    if select.group_by.is_empty() {
        charge_aggregate_memory(
            aggregate_group_base_memory_bytes(&[], &projection_template),
            &mut memory_tracker,
        )?;
        groups.insert(Vec::new(), projection_template.clone());
    }
    visit_relational_rows(
        select,
        parameters,
        state,
        base_schema,
        base_qualifier,
        base_access,
        joins,
        tree_execution,
        pipeline,
        index_runtime,
        row_runtime,
        &mut |row| {
            aggregate_input_rows = aggregate_input_rows.saturating_add(1);
            let key = if select.group_by.is_empty() {
                Vec::new()
            } else {
                select
                    .group_by
                    .iter()
                    .map(|column| resolve_column(&row, column).cloned())
                    .collect::<Result<Vec<_>>>()?
            };
            if !groups.contains_key(&key) {
                charge_aggregate_memory(
                    aggregate_group_base_memory_bytes(&key, &projection_template),
                    &mut memory_tracker,
                )?;
                groups.insert(key.clone(), projection_template.clone());
            }
            let group = groups.get_mut(&key).expect("aggregate group was inserted");
            for projection in group {
                let delta = projection.update(&row)?;
                memory_tracker.release(delta.released_bytes);
                charge_aggregate_memory(delta.added_bytes, &mut memory_tracker)?;
            }
            Ok(true)
        },
    )?;
    pipeline.finish()?;
    let intermediate_rows = pipeline.intermediate_rows;
    if groups.len() > limits.max_intermediate_rows {
        return Err(SkeinError::Execution(format!(
            "relational aggregate groups exceed max_intermediate_rows {}",
            limits.max_intermediate_rows
        )));
    }
    let offset = bind_bound(select.offset, parameters, "OFFSET")?.unwrap_or(0);
    let limit = bind_bound(select.limit, parameters, "LIMIT")?;
    let offset = usize::try_from(offset)
        .map_err(|_| SkeinError::Semantic("SQL OFFSET is too large".to_string()))?;
    let limit = limit
        .map(|value| usize::try_from(value).unwrap_or(usize::MAX))
        .unwrap_or(usize::MAX);
    let mut output = Vec::new();
    let mut payload_bytes = 0usize;
    for (_, projections) in groups
        .into_iter()
        .skip(offset)
        .take(limit.min(limits.max_output_rows.saturating_add(1)))
    {
        let mut row = Row::new();
        for projection in projections {
            let (name, value) = projection.finish()?;
            if row.insert(name.clone(), value).is_some() {
                return Err(SkeinError::Semantic(format!(
                    "relational projection contains duplicate output column {name}"
                )));
            }
        }
        payload_bytes = payload_bytes.saturating_add(map_payload_bytes(&row));
        if payload_bytes > limits.max_output_payload_bytes {
            return Err(SkeinError::Execution(format!(
                "relational SQL output exceeds max_output_payload_bytes {}",
                limits.max_output_payload_bytes
            )));
        }
        output.push(row);
    }
    if output.len() > limits.max_output_rows {
        return Err(SkeinError::Execution(format!(
            "relational SQL output exceeds max_output_rows {}",
            limits.max_output_rows
        )));
    }
    Ok(RelationalQueryOutput {
        rows: output.into(),
        stage_timings: RelationalSqlStageTimings::default(),
        join_planning: join_planning.clone(),
        operator_cardinality_profiles: pipeline.operator_cardinality_profiles(),
        intermediate_rows,
        hydration: row_runtime.hydration(),
        access_path,
        join_access_paths,
        index_execution_evidence: index_runtime.evidence(),
        row_execution_evidence: row_runtime.evidence(),
        blocking_operator_memory_reports: vec![skein_executor::blocking::in_memory_report(
            "RelationalAggregateExec",
            &memory_tracker,
            memory_tracker.peak_bytes,
            aggregate_input_rows,
            execution_memory,
        )],
    })
}

fn single_count_distinct_column(select: &SelectStatement) -> Option<(&SqlColumnRef, String)> {
    let [SelectProjection::Expression { expression, alias }] = select.projection.as_slice() else {
        return None;
    };
    let SqlExpression::Function {
        name,
        arguments,
        distinct: true,
    } = expression
    else {
        return None;
    };
    let [SqlFunctionArgument::Expression(SqlExpression::Column(column))] = arguments.as_slice()
    else {
        return None;
    };
    (name == "count" && select.group_by.is_empty() && select.order_by.is_empty())
        .then(|| (column, alias.clone().unwrap_or_else(|| "count".to_string())))
}

#[allow(clippy::too_many_arguments)]
fn execute_single_count_distinct<'a>(
    select: &'a SelectStatement,
    column: &'a SqlColumnRef,
    output_name: String,
    parameters: &'a [Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &'a RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    tree_execution: Option<&PreparedJoinTreeExecution<'a>>,
    pipeline: &mut RelationalPipelineState<'a>,
    index_runtime: &RelationalIndexRuntime<'a>,
    row_runtime: &RelationalRowRuntime<'a>,
    limits: RelationalQueryLimits,
    execution_memory: &skein_executor::ExecutionMemoryConfig,
    memory_ledger: &QueryMemoryLedger,
    access_path: RelationalAccessPathDescriptor,
    join_access_paths: Vec<RelationalAccessPathDescriptor>,
    join_planning: &RelationalJoinPlanningOutcome,
) -> Result<RelationalQueryOutput> {
    let input_plan = relational_input_plan();
    let catalog = Catalog::default();
    let observer = RelationalBlockingObserver::default();
    let task_context = pipeline.task_context;
    let mut source = DistinctAggregateValueBatchSource {
        select,
        column,
        parameters,
        state,
        base_schema,
        base_qualifier,
        base_access,
        joins,
        tree_execution,
        pipeline,
        index_runtime,
        row_runtime,
        batch_rows: execution_memory.batch_rows.get(),
    };
    let mut count = 0usize;
    stream_distinct_batches(
        &input_plan,
        &mut source,
        BlockingExecutionContext {
            catalog: &catalog,
            memory: execution_memory,
            memory_ledger,
            task_context,
            observer: &observer,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            count = count.saturating_add(batch.len());
            Ok(BatchControl::Continue)
        },
    )?;
    pipeline.finish()?;
    let row = BTreeMap::from([(
        output_name,
        Value::Int(i64::try_from(count).unwrap_or(i64::MAX)),
    )]);
    if map_payload_bytes(&row) > limits.max_output_payload_bytes {
        return Err(SkeinError::Execution(format!(
            "relational SQL output exceeds max_output_payload_bytes {}",
            limits.max_output_payload_bytes
        )));
    }
    if limits.max_output_rows == 0 {
        return Err(SkeinError::Execution(
            "relational SQL output exceeds max_output_rows 0".to_string(),
        ));
    }
    Ok(RelationalQueryOutput {
        rows: vec![row].into(),
        stage_timings: RelationalSqlStageTimings::default(),
        join_planning: join_planning.clone(),
        operator_cardinality_profiles: pipeline.operator_cardinality_profiles(),
        intermediate_rows: pipeline.intermediate_rows,
        hydration: row_runtime.hydration(),
        access_path,
        join_access_paths,
        index_execution_evidence: index_runtime.evidence(),
        row_execution_evidence: row_runtime.evidence(),
        blocking_operator_memory_reports: observer.reports.into_inner(),
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_grouped_aggregate<'a>(
    select: &'a SelectStatement,
    parameters: &'a [Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &'a RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    tree_execution: Option<&PreparedJoinTreeExecution<'a>>,
    pipeline: &mut RelationalPipelineState<'a>,
    index_runtime: &RelationalIndexRuntime<'a>,
    row_runtime: &RelationalRowRuntime<'a>,
    limits: RelationalQueryLimits,
    execution_memory: &skein_executor::ExecutionMemoryConfig,
    memory_ledger: &QueryMemoryLedger,
    access_path: RelationalAccessPathDescriptor,
    join_access_paths: Vec<RelationalAccessPathDescriptor>,
    join_planning: &RelationalJoinPlanningOutcome,
) -> Result<RelationalQueryOutput> {
    if !select.order_by.is_empty() || select.distinct {
        return Err(SkeinError::Semantic(
            "aggregate SELECT does not yet support statement DISTINCT or ORDER BY".to_string(),
        ));
    }
    let projection_template = select
        .projection
        .iter()
        .map(|projection| AggregateProjectionState::new(projection, parameters))
        .collect::<Result<Vec<_>>>()?;
    let mut offset = usize::try_from(bind_bound(select.offset, parameters, "OFFSET")?.unwrap_or(0))
        .map_err(|_| SkeinError::Semantic("SQL OFFSET is too large".to_string()))?;
    let requested = bind_bound(select.limit, parameters, "LIMIT")?
        .map(|value| {
            usize::try_from(value)
                .map_err(|_| SkeinError::Semantic("SQL LIMIT is too large".to_string()))
        })
        .transpose()?
        .unwrap_or(usize::MAX);
    let detection_limit = requested.min(limits.max_output_rows.saturating_add(1));
    let observer = RelationalBlockingObserver::default();
    let task_context = pipeline.task_context;
    let locator_layout = match tree_execution {
        Some(execution) => relational_join_tree_locator_layout(state, execution.tree)?,
        None => relational_locator_layout(&select.from.name, base_qualifier, base_schema, joins)?,
    };
    let mut order = ExternalTopN::new(
        "SortExec",
        "relational-group-sort",
        0,
        usize::MAX,
        execution_memory,
        memory_ledger,
        task_context,
    );
    let mut input_rows = 0usize;
    visit_relational_rows(
        select,
        parameters,
        state,
        base_schema,
        base_qualifier,
        base_access,
        joins,
        tree_execution,
        pipeline,
        index_runtime,
        row_runtime,
        &mut |row| {
            let sort_keys = select
                .group_by
                .iter()
                .map(|column| {
                    RelationalSortKey::new(
                        resolve_column(&row, column)?.clone(),
                        SqlOrderDirection::Asc,
                        SqlNullOrder::DialectDefault,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            order.push(RelationalSortRecord::new(
                sort_keys,
                typed_row_set_locator(&row)?,
            ))?;
            input_rows = input_rows.saturating_add(1);
            Ok(true)
        },
    )?;
    let mut current_key = None::<Vec<RelationalValue>>;
    let mut current_group = None::<Vec<AggregateProjectionState>>;
    let mut tracker = OperatorMemoryTracker::with_account(
        execution_memory.blocking_operator_bytes,
        memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "RelationalGroupedAggregate",
            execution_memory.blocking_operator_bytes,
        ),
    );
    let mut output = Vec::new();
    let mut payload_bytes = 0usize;
    let mut stopped = false;
    let sort_report = order.finish(|record| {
        let locator = record.into_locator();
        let keep_going =
            with_typed_locator_bound_row(&locator, &locator_layout, row_runtime, |row| {
                let key = select
                    .group_by
                    .iter()
                    .map(|column| resolve_column(row, column).cloned())
                    .collect::<Result<Vec<_>>>()?;
                if current_key.as_ref() != Some(&key) {
                    if let Some(group) = current_group.take()
                        && !emit_aggregate_group(
                            group,
                            &mut offset,
                            requested,
                            detection_limit,
                            limits,
                            &mut payload_bytes,
                            &mut output,
                        )?
                    {
                        return Ok(false);
                    }
                    tracker.reset();
                    charge_aggregate_memory(
                        aggregate_group_base_memory_bytes(&key, &projection_template),
                        &mut tracker,
                    )?;
                    current_key = Some(key);
                    current_group = Some(projection_template.clone());
                }
                let group = current_group
                    .as_mut()
                    .expect("grouped aggregate initialized current group");
                for projection in group {
                    let delta = projection.update(row)?;
                    tracker.release(delta.released_bytes);
                    charge_aggregate_memory(delta.added_bytes, &mut tracker)?;
                }
                Ok(true)
            })?;
        stopped = !keep_going;
        Ok(keep_going)
    })?;
    observer.record_blocking_memory_report(sort_report);
    if !stopped && let Some(group) = current_group.take() {
        emit_aggregate_group(
            group,
            &mut offset,
            requested,
            detection_limit,
            limits,
            &mut payload_bytes,
            &mut output,
        )?;
    }
    pipeline.finish()?;
    let mut reports = observer.reports.into_inner();
    reports.push(skein_executor::blocking::in_memory_report(
        "RelationalAggregateExec",
        &tracker,
        tracker.peak_bytes,
        input_rows,
        execution_memory,
    ));
    Ok(RelationalQueryOutput {
        rows: output.into(),
        stage_timings: RelationalSqlStageTimings::default(),
        join_planning: join_planning.clone(),
        operator_cardinality_profiles: pipeline.operator_cardinality_profiles(),
        intermediate_rows: pipeline.intermediate_rows,
        hydration: row_runtime.hydration(),
        access_path,
        join_access_paths,
        index_execution_evidence: index_runtime.evidence(),
        row_execution_evidence: row_runtime.evidence(),
        blocking_operator_memory_reports: reports,
    })
}

fn emit_aggregate_group(
    projections: Vec<AggregateProjectionState>,
    offset: &mut usize,
    requested: usize,
    detection_limit: usize,
    limits: RelationalQueryLimits,
    payload_bytes: &mut usize,
    output: &mut Vec<Row>,
) -> Result<bool> {
    if *offset != 0 {
        *offset -= 1;
        return Ok(true);
    }
    if output.len() >= requested || output.len() >= detection_limit {
        return Ok(false);
    }
    let mut row = Row::new();
    for projection in projections {
        let (name, value) = projection.finish()?;
        if row.insert(name.clone(), value).is_some() {
            return Err(SkeinError::Semantic(format!(
                "relational projection contains duplicate output column {name}"
            )));
        }
    }
    push_relational_output(row, output, payload_bytes, limits)?;
    Ok(output.len() < requested && output.len() < detection_limit)
}

fn projection_contains_aggregate(projection: &SelectProjection) -> bool {
    match projection {
        SelectProjection::Expression { expression, .. } => {
            expression_contains_aggregate(expression)
        }
        SelectProjection::Wildcard | SelectProjection::Column { .. } => false,
    }
}

#[derive(Clone)]
struct AggregateProjectionState {
    name: String,
    expression: AggregateExpressionState,
}

impl AggregateProjectionState {
    fn new(projection: &SelectProjection, parameters: &[Value]) -> Result<Self> {
        match projection {
            SelectProjection::Column { name, alias } => Ok(Self {
                name: alias.clone().unwrap_or_else(|| name.name.clone()),
                expression: AggregateExpressionState::First {
                    column: name.clone(),
                    value: None,
                },
            }),
            SelectProjection::Expression { expression, alias } => Ok(Self {
                name: alias.clone().unwrap_or_else(|| expression_name(expression)),
                expression: AggregateExpressionState::new(expression, parameters)?,
            }),
            SelectProjection::Wildcard => Err(SkeinError::Semantic(
                "aggregate SELECT does not support wildcard projection".to_string(),
            )),
        }
    }

    fn update(&mut self, row: &BoundRow<'_>) -> Result<AggregateMemoryDelta> {
        self.expression.update(row)
    }

    fn finish(self) -> Result<(String, Value)> {
        Ok((self.name, self.expression.finish()?))
    }
}

#[derive(Clone)]
enum AggregateExpressionState {
    Constant(Value),
    First {
        column: SqlColumnRef,
        value: Option<RelationalValue>,
    },
    Count {
        column: Option<SqlColumnRef>,
        count: usize,
        distinct: Option<BTreeSet<RelationalValue>>,
    },
    Numeric {
        expression: SqlExpression,
        aggregate: NumericAggregate,
        value: Option<RelationalValue>,
        distinct: Option<BTreeSet<RelationalValue>>,
    },
    Coalesce(Vec<AggregateExpressionState>),
}

#[derive(Clone, Copy)]
enum NumericAggregate {
    Sum,
    Max,
}

#[derive(Default)]
struct AggregateMemoryDelta {
    added_bytes: usize,
    released_bytes: usize,
}

impl AggregateMemoryDelta {
    fn between(previous: usize, next: usize) -> Self {
        if next >= previous {
            Self {
                added_bytes: next - previous,
                released_bytes: 0,
            }
        } else {
            Self {
                added_bytes: 0,
                released_bytes: previous - next,
            }
        }
    }

    fn combine(&mut self, other: Self) {
        self.added_bytes = self.added_bytes.saturating_add(other.added_bytes);
        self.released_bytes = self.released_bytes.saturating_add(other.released_bytes);
    }
}

impl AggregateExpressionState {
    fn new(expression: &SqlExpression, parameters: &[Value]) -> Result<Self> {
        match expression {
            SqlExpression::Value(value) => Ok(Self::Constant(bind_sql_value(value, parameters)?)),
            SqlExpression::Column(column) => Ok(Self::First {
                column: column.clone(),
                value: None,
            }),
            SqlExpression::Function {
                name,
                arguments,
                distinct,
            } => match name.as_str() {
                "count" => {
                    let [argument] = arguments.as_slice() else {
                        return Err(SkeinError::Semantic(
                            "COUNT requires exactly one argument".to_string(),
                        ));
                    };
                    let column = match argument {
                        SqlFunctionArgument::Wildcard => None,
                        SqlFunctionArgument::Expression(SqlExpression::Column(column)) => {
                            Some(column.clone())
                        }
                        _ => {
                            return Err(SkeinError::Semantic(
                                "COUNT supports wildcard or a column argument".to_string(),
                            ))
                        }
                    };
                    if *distinct && column.is_none() {
                        return Err(SkeinError::Semantic(
                            "COUNT(DISTINCT *) is not supported".to_string(),
                        ));
                    }
                    Ok(Self::Count {
                        column,
                        count: 0,
                        distinct: distinct.then(BTreeSet::new),
                    })
                }
                "sum" | "max" => {
                    let [SqlFunctionArgument::Expression(expression)] = arguments.as_slice() else {
                        return Err(SkeinError::Semantic(
                            "numeric aggregate requires exactly one expression".to_string(),
                        ));
                    };
                    Ok(Self::Numeric {
                        expression: expression.clone(),
                        aggregate: if name == "sum" {
                            NumericAggregate::Sum
                        } else {
                            NumericAggregate::Max
                        },
                        value: None,
                        distinct: distinct.then(BTreeSet::new),
                    })
                }
                "coalesce" => {
                    if *distinct {
                        return Err(SkeinError::Semantic(
                            "COALESCE does not accept DISTINCT".to_string(),
                        ));
                    }
                    let states = arguments
                        .iter()
                        .map(|argument| {
                            let SqlFunctionArgument::Expression(expression) = argument else {
                                return Err(SkeinError::Semantic(
                                    "COALESCE does not accept wildcard".to_string(),
                                ));
                            };
                            Self::new(expression, parameters)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Self::Coalesce(states))
                }
                _ => Err(SkeinError::Semantic(format!(
                    "unsupported relational aggregate function {name}"
                ))),
            },
        }
    }

    fn update(&mut self, row: &BoundRow<'_>) -> Result<AggregateMemoryDelta> {
        match self {
            Self::Constant(_) => Ok(AggregateMemoryDelta::default()),
            Self::First { column, value } => {
                if value.is_none() {
                    let first = resolve_column(row, column)?.clone();
                    let added_bytes = relational_value_memory_bytes(&first);
                    *value = Some(first);
                    return Ok(AggregateMemoryDelta {
                        added_bytes,
                        released_bytes: 0,
                    });
                }
                Ok(AggregateMemoryDelta::default())
            }
            Self::Count {
                column,
                count,
                distinct,
            } => {
                let value = column
                    .as_ref()
                    .map(|column| resolve_column(row, column).cloned())
                    .transpose()?;
                if value
                    .as_ref()
                    .is_some_and(|value| matches!(value, RelationalValue::Null))
                {
                    return Ok(AggregateMemoryDelta::default());
                }
                if let Some(distinct) = distinct {
                    let value = value.expect("DISTINCT COUNT has a column");
                    let value_bytes = relational_value_memory_bytes(&value)
                        .saturating_add(std::mem::size_of::<usize>() * 4);
                    if distinct.insert(value) {
                        *count = count.saturating_add(1);
                        return Ok(AggregateMemoryDelta {
                            added_bytes: value_bytes,
                            released_bytes: 0,
                        });
                    }
                } else {
                    *count = count.saturating_add(1);
                }
                Ok(AggregateMemoryDelta::default())
            }
            Self::Numeric {
                expression,
                aggregate,
                value,
                distinct,
            } => {
                let candidate = evaluate_row_expression(expression, row)?;
                if matches!(candidate, RelationalValue::Null) {
                    return Ok(AggregateMemoryDelta::default());
                }
                let mut delta = AggregateMemoryDelta::default();
                if let Some(distinct) = distinct {
                    if !distinct.insert(candidate.clone()) {
                        return Ok(delta);
                    }
                    delta.added_bytes = delta.added_bytes.saturating_add(
                        relational_value_memory_bytes(&candidate)
                            .saturating_add(std::mem::size_of::<usize>() * 4),
                    );
                }
                delta.combine(update_numeric_aggregate(value, candidate, *aggregate)?);
                Ok(delta)
            }
            Self::Coalesce(states) => {
                let mut delta = AggregateMemoryDelta::default();
                for state in states {
                    delta.combine(state.update(row)?);
                }
                Ok(delta)
            }
        }
    }

    fn finish(self) -> Result<Value> {
        match self {
            Self::Constant(value) => Ok(value),
            Self::First {
                value: Some(value), ..
            } => relational_to_value(&value),
            Self::First { value: None, .. } => Err(SkeinError::Semantic(
                "aggregate column has no input row".to_string(),
            )),
            Self::Count { count, .. } => Ok(Value::Int(i64::try_from(count).unwrap_or(i64::MAX))),
            Self::Numeric { value, .. } => {
                value.map_or(Ok(Value::Null), |value| relational_to_value(&value))
            }
            Self::Coalesce(states) => {
                for state in states {
                    let value = state.finish()?;
                    if value != Value::Null {
                        return Ok(value);
                    }
                }
                Ok(Value::Null)
            }
        }
    }
}

fn update_numeric_aggregate(
    result: &mut Option<RelationalValue>,
    candidate: RelationalValue,
    aggregate: NumericAggregate,
) -> Result<AggregateMemoryDelta> {
    let previous_bytes = result.as_ref().map_or(0, relational_value_memory_bytes);
    match (aggregate, &mut *result, candidate) {
        (NumericAggregate::Sum, result @ None, RelationalValue::BigInt(value)) => {
            *result = Some(RelationalValue::BigInt(value));
        }
        (
            NumericAggregate::Sum,
            Some(RelationalValue::BigInt(total)),
            RelationalValue::BigInt(value),
        ) => {
            *total = total
                .checked_add(value)
                .ok_or_else(|| SkeinError::Execution("BIGINT SUM overflow".to_string()))?;
        }
        (NumericAggregate::Sum, result @ None, RelationalValue::DoublePrecision(value)) => {
            *result = Some(RelationalValue::DoublePrecision(value));
        }
        (
            NumericAggregate::Sum,
            Some(RelationalValue::DoublePrecision(total)),
            RelationalValue::DoublePrecision(value),
        ) => {
            *total += value;
        }
        (NumericAggregate::Max, result @ None, value) => *result = Some(value),
        (NumericAggregate::Max, Some(current), value) if value > *current => *current = value,
        (NumericAggregate::Max, Some(_), _) => {}
        (NumericAggregate::Sum, _, _) => {
            return Err(SkeinError::Semantic(
                "SUM requires BIGINT or DOUBLE PRECISION input".to_string(),
            ))
        }
    }
    let next_bytes = result.as_ref().map_or(0, relational_value_memory_bytes);
    Ok(AggregateMemoryDelta::between(previous_bytes, next_bytes))
}

fn charge_aggregate_memory(bytes: usize, tracker: &mut OperatorMemoryTracker) -> Result<()> {
    ensure_operator_item_fits("RelationalAggregateExec", bytes, tracker)?;
    if tracker.would_exceed(bytes) {
        return Err(SkeinError::Execution(format!(
            "RelationalAggregateExec state exceeds blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    tracker.try_charge(bytes)?;
    Ok(())
}

fn aggregate_group_base_memory_bytes(
    key: &[RelationalValue],
    projections: &[AggregateProjectionState],
) -> usize {
    let key_bytes = key.iter().fold(
        std::mem::size_of::<Vec<RelationalValue>>(),
        |total, value| total.saturating_add(relational_value_memory_bytes(value)),
    );
    projections.iter().fold(
        key_bytes
            .saturating_add(std::mem::size_of::<Vec<AggregateProjectionState>>())
            .saturating_add(std::mem::size_of::<usize>() * 6),
        |total, projection| {
            total
                .saturating_add(std::mem::size_of::<AggregateProjectionState>())
                .saturating_add(projection.name.len())
                .saturating_add(aggregate_expression_base_memory_bytes(
                    &projection.expression,
                ))
        },
    )
}

fn aggregate_expression_base_memory_bytes(state: &AggregateExpressionState) -> usize {
    let state_bytes = std::mem::size_of::<AggregateExpressionState>();
    state_bytes.saturating_add(match state {
        AggregateExpressionState::Constant(value) => {
            skein_executor::binding::value_memory_bytes(value)
        }
        AggregateExpressionState::First { column, value } => column_ref_memory_bytes(column)
            .saturating_add(value.as_ref().map_or(0, relational_value_memory_bytes)),
        AggregateExpressionState::Count {
            column, distinct, ..
        } => column
            .as_ref()
            .map_or(0, column_ref_memory_bytes)
            .saturating_add(
                distinct
                    .as_ref()
                    .map_or(0, |_| std::mem::size_of::<BTreeSet<RelationalValue>>()),
            ),
        AggregateExpressionState::Numeric {
            expression,
            value,
            distinct,
            ..
        } => sql_expression_memory_bytes(expression)
            .saturating_add(value.as_ref().map_or(0, relational_value_memory_bytes))
            .saturating_add(
                distinct
                    .as_ref()
                    .map_or(0, |_| std::mem::size_of::<BTreeSet<RelationalValue>>()),
            ),
        AggregateExpressionState::Coalesce(states) => states.iter().fold(
            std::mem::size_of::<Vec<AggregateExpressionState>>(),
            |total, state| total.saturating_add(aggregate_expression_base_memory_bytes(state)),
        ),
    })
}

fn sql_expression_memory_bytes(expression: &SqlExpression) -> usize {
    std::mem::size_of::<SqlExpression>().saturating_add(match expression {
        SqlExpression::Column(column) => column_ref_memory_bytes(column),
        SqlExpression::Value(SqlValue::Literal(value)) => {
            skein_executor::binding::value_memory_bytes(value)
        }
        SqlExpression::Value(SqlValue::Parameter(_)) => 0,
        SqlExpression::Function {
            name, arguments, ..
        } => arguments.iter().fold(
            name.len()
                .saturating_add(std::mem::size_of::<Vec<SqlFunctionArgument>>()),
            |total, argument| match argument {
                SqlFunctionArgument::Expression(expression) => {
                    total.saturating_add(sql_expression_memory_bytes(expression))
                }
                SqlFunctionArgument::Wildcard => total,
            },
        ),
    })
}

fn column_ref_memory_bytes(column: &SqlColumnRef) -> usize {
    std::mem::size_of::<SqlColumnRef>()
        .saturating_add(column.name.len())
        .saturating_add(column.qualifier.as_ref().map_or(0, String::len))
}

fn relational_value_memory_bytes(value: &RelationalValue) -> usize {
    std::mem::size_of::<RelationalValue>().saturating_add(value.estimated_payload_bytes())
}

fn evaluate_row_expression(
    expression: &SqlExpression,
    row: &BoundRow<'_>,
) -> Result<RelationalValue> {
    match expression {
        SqlExpression::Column(column) => Ok(resolve_column(row, column)?.clone()),
        SqlExpression::Value(SqlValue::Literal(value)) => value_to_relational(value.clone()),
        SqlExpression::Value(SqlValue::Parameter(position)) => Err(SkeinError::Semantic(format!(
            "aggregate row expression cannot bind parameter ${position}"
        ))),
        SqlExpression::Function {
            name,
            arguments,
            distinct: false,
        } if name == "octet_length" => {
            let [SqlFunctionArgument::Expression(SqlExpression::Column(column))] =
                arguments.as_slice()
            else {
                return Err(SkeinError::Semantic(
                    "OCTET_LENGTH requires exactly one column".to_string(),
                ));
            };
            match resolve_column(row, column)? {
                RelationalValue::Null => Ok(RelationalValue::Null),
                RelationalValue::Text(value) => Ok(RelationalValue::BigInt(
                    i64::try_from(value.len()).unwrap_or(i64::MAX),
                )),
                RelationalValue::Bytea(value) => Ok(RelationalValue::BigInt(
                    i64::try_from(value.len()).unwrap_or(i64::MAX),
                )),
                RelationalValue::Overflow(reference) => Ok(RelationalValue::BigInt(
                    i64::try_from(reference.uncompressed_bytes).unwrap_or(i64::MAX),
                )),
                _ => Err(SkeinError::Semantic(
                    "OCTET_LENGTH requires TEXT or BYTEA input".to_string(),
                )),
            }
        }
        SqlExpression::Function { name, .. } => Err(SkeinError::Semantic(format!(
            "unsupported aggregate row function {name}"
        ))),
    }
}

fn compare_value_refs(
    left: RelationalValueRef<'_>,
    right: RelationalValueRef<'_>,
    op: SqlComparisonOp,
) -> Result<Option<bool>> {
    if matches!(left, RelationalValueRef::Overflow(_))
        || matches!(right, RelationalValueRef::Overflow(_))
    {
        return Err(SkeinError::Execution(
            "relational filter or join requires overflow hydration before qualification"
                .to_string(),
        ));
    }
    if matches!(left, RelationalValueRef::Null) || matches!(right, RelationalValueRef::Null) {
        return Ok(None);
    }
    if left.scalar_type() != right.scalar_type() {
        return Err(SkeinError::Semantic(
            "relational comparison has incompatible scalar types".to_string(),
        ));
    }
    Ok(Some(match op {
        SqlComparisonOp::Eq => left == right,
        SqlComparisonOp::NotEq => left != right,
        SqlComparisonOp::Lt => left < right,
        SqlComparisonOp::Lte => left <= right,
        SqlComparisonOp::Gt => left > right,
        SqlComparisonOp::Gte => left >= right,
    }))
}

fn relational_ref_to_value(value: RelationalValueRef<'_>) -> Result<Value> {
    match value {
        RelationalValueRef::Null => Ok(Value::Null),
        RelationalValueRef::Boolean(value) => Ok(Value::Bool(value)),
        RelationalValueRef::BigInt(value) => Ok(Value::Int(value)),
        RelationalValueRef::DoublePrecision(value) => Ok(Value::Float(value)),
        RelationalValueRef::Text(value) => Ok(Value::String(value.to_owned())),
        RelationalValueRef::Bytea(value) => Ok(Value::Binary(value.to_vec())),
        RelationalValueRef::Overflow(_) => Err(SkeinError::Execution(
            "overflow value reached projection without hydration".to_string(),
        )),
    }
}

fn predicate_truth(
    predicate: &SqlPredicate,
    row: &BoundRow<'_>,
    parameters: &[Value],
) -> Result<Option<bool>> {
    match predicate {
        SqlPredicate::And(left, right) => match predicate_truth(left, row, parameters)? {
            Some(false) => Ok(Some(false)),
            Some(true) => predicate_truth(right, row, parameters),
            None => match predicate_truth(right, row, parameters)? {
                Some(false) => Ok(Some(false)),
                Some(true) | None => Ok(None),
            },
        },
        SqlPredicate::Or(left, right) => match predicate_truth(left, row, parameters)? {
            Some(true) => Ok(Some(true)),
            Some(false) => predicate_truth(right, row, parameters),
            None => match predicate_truth(right, row, parameters)? {
                Some(true) => Ok(Some(true)),
                Some(false) | None => Ok(None),
            },
        },
        SqlPredicate::Not(predicate) => {
            Ok(predicate_truth(predicate, row, parameters)?.map(|value| !value))
        }
        SqlPredicate::Compare { left, op, right } => compare_values(
            resolve_column(row, left)?,
            &value_to_relational(bind_sql_value(right, parameters)?)?,
            *op,
        ),
        SqlPredicate::CompareColumns { left, op, right } => {
            compare_values(resolve_column(row, left)?, resolve_column(row, right)?, *op)
        }
        SqlPredicate::InList {
            left,
            values,
            negated,
        } => {
            let left = resolve_column(row, left)?;
            let mut has_unknown = false;
            let mut matched = false;
            for value in values {
                match compare_values(
                    left,
                    &value_to_relational(bind_sql_value(value, parameters)?)?,
                    SqlComparisonOp::Eq,
                )? {
                    Some(true) => matched = true,
                    None => has_unknown = true,
                    Some(false) => {}
                }
            }
            let result = if matched {
                Some(true)
            } else if has_unknown {
                None
            } else {
                Some(false)
            };
            Ok(result.map(|value| value != *negated))
        }
        SqlPredicate::IsNull { column, negated } => Ok(Some(
            matches!(resolve_column(row, column)?, RelationalValue::Null) != *negated,
        )),
    }
}

fn compare_values(
    left: &RelationalValue,
    right: &RelationalValue,
    op: SqlComparisonOp,
) -> Result<Option<bool>> {
    compare_value_refs(left.as_ref(), right.as_ref(), op)
}

fn resolve_column<'a>(row: &'a BoundRow<'a>, column: &SqlColumnRef) -> Result<&'a RelationalValue> {
    let bindings =
        row.bindings.iter().filter(|binding| {
            column.qualifier.as_deref().is_none_or(|qualifier| {
                qualifier == binding.qualifier || qualifier == binding.table
            }) && binding.schema.column_position(&column.name).is_some()
        });
    let mut bindings = bindings.collect::<Vec<_>>();
    if bindings.len() != 1 {
        return Err(SkeinError::Semantic(format!(
            "column {} is unknown or ambiguous",
            column.name
        )));
    }
    let binding = bindings.pop().expect("one binding");
    let position = binding
        .schema
        .column_position(&column.name)
        .expect("filtered binding has column");
    binding.value(position)
}

fn project_bound_row(row: &BoundRow<'_>, projection: &[SelectProjection]) -> Result<Row> {
    let mut output = Row::new();
    for item in projection {
        match item {
            SelectProjection::Wildcard => {
                for binding in &row.bindings {
                    for (position, column) in binding.schema.columns.iter().enumerate() {
                        let value = projected_value(position, binding)?;
                        insert_output(&mut output, column.name.clone(), value)?;
                    }
                }
            }
            SelectProjection::Column { name, alias } => {
                let (_, binding, position) = resolve_binding(row, name)?;
                let value = projected_value(position, binding)?;
                insert_output(
                    &mut output,
                    alias.clone().unwrap_or_else(|| name.name.clone()),
                    value,
                )?;
            }
            SelectProjection::Expression { .. } => {
                return Err(SkeinError::Semantic(
                    "non-aggregate relational projection expressions are not supported".to_string(),
                ));
            }
        }
    }
    Ok(output)
}

fn resolve_binding<'a>(
    row: &'a BoundRow<'a>,
    column: &SqlColumnRef,
) -> Result<(usize, &'a Binding<'a>, usize)> {
    let mut matches = row
        .bindings
        .iter()
        .enumerate()
        .filter_map(|(index, binding)| {
            let qualifier_matches = column.qualifier.as_deref().is_none_or(|qualifier| {
                qualifier == binding.qualifier || qualifier == binding.table
            });
            qualifier_matches
                .then(|| binding.schema.column_position(&column.name))
                .flatten()
                .map(|position| (index, binding, position))
        });
    let first = matches.next().ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational column {}", column.name))
    })?;
    if matches.next().is_some() {
        return Err(SkeinError::Semantic(format!(
            "ambiguous relational column {}",
            column.name
        )));
    }
    Ok(first)
}

fn projected_value(position: usize, binding: &Binding<'_>) -> Result<Value> {
    relational_to_value(binding.value(position)?)
}

fn insert_output(output: &mut Row, name: String, value: Value) -> Result<()> {
    if output.insert(name.clone(), value).is_some() {
        return Err(SkeinError::Semantic(format!(
            "relational projection contains duplicate output column {name}"
        )));
    }
    Ok(())
}

fn bind_bound(bound: Option<SqlBound>, parameters: &[Value], name: &str) -> Result<Option<u64>> {
    bound
        .map(|bound| match bound {
            SqlBound::Literal(value) => Ok(value),
            SqlBound::Parameter(position) => match parameters.get(position.saturating_sub(1)) {
                Some(Value::Int(value)) if *value >= 0 => Ok(*value as u64),
                Some(_) => Err(SkeinError::Semantic(format!(
                    "PostgreSQL {name} parameter ${position} must be a non-negative integer"
                ))),
                None => Err(SkeinError::Semantic(format!(
                    "missing PostgreSQL parameter ${position}"
                ))),
            },
        })
        .transpose()
}

fn bind_sql_value(value: &SqlValue, parameters: &[Value]) -> Result<Value> {
    match value {
        SqlValue::Literal(value) => Ok(value.clone()),
        SqlValue::Parameter(position) => parameters
            .get(position.saturating_sub(1))
            .cloned()
            .ok_or_else(|| {
                SkeinError::Semantic(format!("missing PostgreSQL parameter ${position}"))
            }),
    }
}

fn value_to_relational(value: Value) -> Result<RelationalValue> {
    match value {
        Value::Null => Ok(RelationalValue::Null),
        Value::Bool(value) => Ok(RelationalValue::Boolean(value)),
        Value::Int(value) => Ok(RelationalValue::BigInt(value)),
        Value::Float(value) => Ok(RelationalValue::DoublePrecision(value)),
        Value::String(value) => Ok(RelationalValue::Text(value)),
        Value::Binary(value) => Ok(RelationalValue::Bytea(value)),
        Value::List(_) | Value::Map(_) => Err(SkeinError::Semantic(
            "relational SQL values must be scalar".to_string(),
        )),
    }
}

fn relational_to_value(value: &RelationalValue) -> Result<Value> {
    match value {
        RelationalValue::Null => Ok(Value::Null),
        RelationalValue::Boolean(value) => Ok(Value::Bool(*value)),
        RelationalValue::BigInt(value) => Ok(Value::Int(*value)),
        RelationalValue::DoublePrecision(value) => Ok(Value::Float(*value)),
        RelationalValue::Text(value) => Ok(Value::String(value.clone())),
        RelationalValue::Bytea(value) => Ok(Value::Binary(value.clone())),
        RelationalValue::Overflow(_) => Err(SkeinError::Execution(
            "overflow value reached projection without hydration".to_string(),
        )),
    }
}

fn expression_name(expression: &SqlExpression) -> String {
    match expression {
        SqlExpression::Column(column) => column.name.clone(),
        SqlExpression::Value(_) => "value".to_string(),
        SqlExpression::Function { name, .. } => name.clone(),
    }
}

fn account_intermediate(total: &mut usize, rows: usize, limit: usize) -> Result<()> {
    *total = total.checked_add(rows).ok_or_else(|| {
        SkeinError::Execution("relational intermediate row count overflow".to_string())
    })?;
    if *total > limit {
        return Err(SkeinError::Execution(format!(
            "relational SQL exceeds max_intermediate_rows {limit}"
        )));
    }
    Ok(())
}

fn reject_non_public_schema(schema: Option<&str>) -> Result<()> {
    if schema.is_some_and(|schema| schema != "public") {
        return Err(SkeinError::Semantic(
            "relational content tables must use the public schema".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational_sql::{
        compile_relational_statement_sql, RelationalJoinPlanningAttempt,
        RelationalJoinPlanningStrategy,
    };
    use crate::Value;
    use skein_storage::{RelationalMutationLimits, RelationalOverflowConfig};

    #[test]
    fn explain_estimated_rows_never_render_zero() {
        assert_eq!(
            optional_estimated_rows_explain_value(Some(0)),
            Value::Int(1)
        );
        assert_eq!(optional_estimated_rows_explain_value(None), Value::Null);
        assert_eq!(optional_usize_explain_value(Some(0)), Value::Int(0));
    }

    #[test]
    fn prepared_bushy_join_tree_materializes_the_composite_right_input_once() {
        const SQL: &str = "SELECT a.id AS a_id, d.id AS d_id \
            FROM bushy_a AS a \
            INNER JOIN bushy_b AS b ON b.a_id = a.id \
            INNER JOIN bushy_c AS c ON c.bridge = b.bridge \
            INNER JOIN bushy_d AS d ON d.c_id = c.id \
            ORDER BY a.id ASC, d.id ASC";

        let mut state = RelationalState::default();
        for sql in [
            "CREATE TABLE bushy_a (id TEXT PRIMARY KEY)",
            "CREATE TABLE bushy_b (id TEXT PRIMARY KEY, a_id TEXT NOT NULL, bridge TEXT NOT NULL)",
            "CREATE TABLE bushy_c (id TEXT PRIMARY KEY, bridge TEXT NOT NULL)",
            "CREATE TABLE bushy_d (id TEXT PRIMARY KEY, c_id TEXT NOT NULL)",
            "INSERT INTO bushy_a (id) VALUES ('a-1'), ('a-2')",
            "INSERT INTO bushy_b (id, a_id, bridge) VALUES ('b-1', 'a-1', 'x'), ('b-2', 'a-2', 'y')",
            "INSERT INTO bushy_c (id, bridge) VALUES ('c-1', 'x'), ('c-2', 'y')",
            "INSERT INTO bushy_d (id, c_id) VALUES ('d-1', 'c-1'), ('d-2', 'c-2')",
        ] {
            let transaction = compile_relational_statement_sql(sql, &[], &state)
                .unwrap_or_else(|error| panic!("failed to compile SQL '{sql}': {error}"));
            state = state
                .stage_transaction(
                    transaction,
                    RelationalMutationLimits::default(),
                    RelationalOverflowConfig::default(),
                )
                .unwrap_or_else(|error| panic!("failed to apply SQL '{sql}': {error}"));
        }

        let prepared_sql = skein_sql::prepare_postgres_sql(SQL).expect("valid bushy SELECT");
        let SqlStatement::Select(select) = prepared_sql.statement else {
            panic!("expected SELECT statement");
        };
        let limits = RelationalQueryLimits {
            max_output_rows: 8,
            max_output_payload_bytes: 64 * 1024,
            max_intermediate_rows: 128,
            hydration: RelationalHydrationBudget::default(),
            index_read: skein_storage::RelationalIndexReadLimits::default(),
            row_read: skein_storage::RelationalRowPageSnapshotReadLimits::default(),
        };
        let syntax_plan = prepare_syntax_access_plan(
            &select,
            &[],
            &state,
            RelationalIndexReadMode::Materialized,
            limits,
        )
        .expect("prepare syntax access plan");
        let c_schema = state.table_schema("bushy_c").expect("bushy_c schema");
        let c_base = choose_base_access(RelationalBaseAccessPlanning {
            predicate: None,
            order_by: &[],
            prefer_ordered_access: false,
            parameters: &[],
            state: &state,
            schema: c_schema,
            table: "bushy_c",
            qualifier: "c",
            cardinality_limit: limits.max_intermediate_rows.saturating_add(1),
        })
        .expect("prepare bushy_c materialized base access");

        let relation =
            |binding: u32, table: &str, qualifier: &str, access: PreparedRelationalTreeAccess| {
                PreparedRelationalJoinTreeNode::Relation(PreparedRelationalTreeRelation {
                    binding: BindingId::new(binding),
                    table: table.to_string(),
                    qualifier: qualifier.to_string(),
                    access,
                })
            };
        let left = PreparedRelationalJoinTreeNode::Join {
            operator_id: RelationalOperatorId::from_plan_index(1),
            kind: SqlJoinKind::Inner,
            predicates: vec![select.joins[0].on.clone()],
            left: Box::new(relation(
                0,
                "bushy_a",
                "a",
                PreparedRelationalTreeAccess::Base(syntax_plan.base_access.clone()),
            )),
            right: Box::new(relation(
                1,
                "bushy_b",
                "b",
                PreparedRelationalTreeAccess::Probe(syntax_plan.join_accesses[0].clone()),
            )),
        };
        let right = PreparedRelationalJoinTreeNode::Join {
            operator_id: RelationalOperatorId::from_plan_index(2),
            kind: SqlJoinKind::Inner,
            predicates: vec![select.joins[2].on.clone()],
            left: Box::new(relation(
                2,
                "bushy_c",
                "c",
                PreparedRelationalTreeAccess::Base(c_base),
            )),
            right: Box::new(relation(
                3,
                "bushy_d",
                "d",
                PreparedRelationalTreeAccess::Probe(syntax_plan.join_accesses[2].clone()),
            )),
        };
        let left_cost = estimate_relational_probe_join_cost(
            estimate_relational_access_cost(syntax_plan.base_access.descriptor.estimated_rows),
            syntax_plan.join_accesses[0].descriptor.estimated_rows,
            RelationalJoinCardinality::Inner,
        );
        let right_cost = estimate_relational_probe_join_cost(
            estimate_relational_access_cost(
                right.first_relation().access.descriptor().estimated_rows,
            ),
            syntax_plan.join_accesses[2].descriptor.estimated_rows,
            RelationalJoinCardinality::Inner,
        );
        let cost = estimate_relational_join_cost(
            left_cost,
            right_cost,
            RelationalJoinCardinality::Inner,
            RelationalJoinRightInput::Materialized,
        );
        let root = PreparedRelationalJoinTreeNode::Join {
            operator_id: RelationalOperatorId::from_plan_index(3),
            kind: SqlJoinKind::Inner,
            predicates: vec![select.joins[1].on.clone()],
            left: Box::new(left),
            right: Box::new(right),
        };
        let mut access_plan = syntax_plan;
        access_plan.join_selection = None;
        access_plan.join_tree = Some(PreparedRelationalJoinTree {
            root,
            cost_breakdown: cost,
        });
        let execution = PreparedRelationalExecutionDescriptor::prepare(&select, &access_plan);
        let prepared = PreparedRelationalSelect {
            statement: select,
            access_plan,
            join_planning: RelationalJoinPlanningOutcome::selected(
                RelationalJoinPlanningAttempt::selected(
                    RelationalJoinPlanningStrategy::CsgCmpMemo,
                    true,
                    7,
                    8,
                    cost,
                ),
                vec!["a".into(), "b".into(), "c".into(), "d".into()],
                RelationalJoinEnumerationConfig::default(),
                Vec::new(),
            ),
            execution,
            stage_timings: RelationalSqlStageTimings::default(),
        };
        prepared.validate().expect("validate prepared bushy plan");

        let memory = skein_executor::ExecutionMemoryConfig::default();
        let resources = RelationalQueryResourceContext::new(
            RelationalJoinEnumerationConfig::default(),
            limits,
            &memory,
            None,
        );
        let admitted = prepared
            .execution
            .admit(
                &state,
                RelationalQueryReadModes::new(
                    RelationalIndexReadMode::Materialized,
                    RelationalRowReadMode::CanonicalMemory,
                ),
                resources,
            )
            .expect("admit prepared bushy plan");
        let output = execute_select(&prepared, &[], admitted).expect("execute prepared bushy plan");

        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0]["a_id"], Value::String("a-1".to_string()));
        assert_eq!(output.rows[1]["d_id"], Value::String("d-2".to_string()));
        assert_eq!(
            output.join_planning.strategy,
            RelationalJoinPlanningStrategy::CsgCmpMemo
        );
        assert!(output
            .blocking_operator_memory_reports
            .iter()
            .any(|report| {
                report.operator == "RelationalBushyJoinMaterialize"
                    && report.input_rows == 2
                    && report.peak_tracked_bytes > 0
            }));

        let constrained_memory = skein_executor::ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(64).expect("non-zero memory budget"),
            ..skein_executor::ExecutionMemoryConfig::default()
        };
        let constrained_resources = RelationalQueryResourceContext::new(
            RelationalJoinEnumerationConfig::default(),
            limits,
            &constrained_memory,
            None,
        );
        let constrained_execution = prepared
            .execution
            .admit(
                &state,
                RelationalQueryReadModes::new(
                    RelationalIndexReadMode::Materialized,
                    RelationalRowReadMode::CanonicalMemory,
                ),
                constrained_resources,
            )
            .expect("admit constrained bushy plan");
        let error = execute_select(&prepared, &[], constrained_execution)
            .expect_err("bushy materialization must honor its memory budget");
        assert!(error
            .to_string()
            .contains("RelationalBushyJoinMaterialize state exceeds blocking_operator_bytes"));
    }
}
