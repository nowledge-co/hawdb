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
enum RelationalPhysicalAccess {
    Base(RelationalAccessCandidate),
    Probe(RelationalJoinAccessCandidate),
}

impl RelationalPhysicalAccess {
    fn descriptor(&self) -> &RelationalAccessPathDescriptor {
        match self {
            Self::Base(access) => &access.descriptor,
            Self::Probe(access) => &access.descriptor,
        }
    }

    fn descriptor_mut(&mut self) -> &mut RelationalAccessPathDescriptor {
        match self {
            Self::Base(access) => &mut access.descriptor,
            Self::Probe(access) => &mut access.descriptor,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RelationalPhysicalOutputBinding {
    binding: BindingId,
    table: String,
    qualifier: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RelationalPhysicalOutputSchema {
    bindings: Arc<[RelationalPhysicalOutputBinding]>,
}

impl RelationalPhysicalOutputSchema {
    fn relation(binding: BindingId, table: &str, qualifier: &str) -> Self {
        Self {
            bindings: vec![RelationalPhysicalOutputBinding {
                binding,
                table: table.to_string(),
                qualifier: qualifier.to_string(),
            }]
            .into(),
        }
    }

    fn join(left: &Self, right: &Self) -> Result<Self> {
        let mut bindings =
            Vec::with_capacity(left.bindings.len().saturating_add(right.bindings.len()));
        bindings.extend(left.bindings.iter().cloned());
        bindings.extend(right.bindings.iter().cloned());
        let mut seen = BTreeSet::new();
        if let Some(duplicate) = bindings
            .iter()
            .find_map(|binding| (!seen.insert(binding.binding)).then_some(binding.binding))
        {
            return Err(SkeinError::Execution(format!(
                "physical join output schema repeats binding {}",
                duplicate.get()
            )));
        }
        Ok(Self {
            bindings: bindings.into(),
        })
    }

    fn ensure_matches(&self, row: &BoundRow<'_>) -> Result<()> {
        if self.bindings.len() != row.bindings.len() {
            return Err(SkeinError::Execution(format!(
                "physical join output schema has {} bindings but executor produced {}",
                self.bindings.len(),
                row.bindings.len()
            )));
        }
        if let Some((expected, actual)) =
            self.bindings
                .iter()
                .zip(&row.bindings)
                .find(|(expected, actual)| {
                    expected.binding != actual.binding
                        || expected.table != actual.table
                        || expected.qualifier != actual.qualifier
                })
        {
            return Err(SkeinError::Execution(format!(
                "physical join output schema binding {} is {}, but executor produced {}",
                expected.binding.get(),
                expected.qualifier,
                actual.qualifier
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelationalPhysicalJoinAlgorithm {
    Probe,
    BatchedIndex,
    Merge,
    Hash,
    Materialized,
}

#[derive(Debug, Clone)]
struct RelationalEquiJoinKeys {
    columns: Vec<(String, SqlColumnRef)>,
}

#[derive(Debug, Clone)]
struct RelationalPhysicalRelation {
    binding: BindingId,
    table: String,
    qualifier: String,
    access: RelationalPhysicalAccess,
    output_schema: RelationalPhysicalOutputSchema,
}

impl RelationalPhysicalRelation {
    fn new(
        binding: BindingId,
        table: String,
        qualifier: String,
        access: RelationalPhysicalAccess,
    ) -> Self {
        let output_schema = RelationalPhysicalOutputSchema::relation(binding, &table, &qualifier);
        Self {
            binding,
            table,
            qualifier,
            access,
            output_schema,
        }
    }

    fn supports_batched_index_probe(&self) -> bool {
        matches!(
            &self.access,
            RelationalPhysicalAccess::Probe(candidate)
                if matches!(
                    candidate.access,
                    RelationalJoinAccess::PrimaryKey(_) | RelationalJoinAccess::Index { .. }
                )
        )
    }
}

#[derive(Debug, Clone)]
enum RelationalPhysicalJoinNode {
    Relation(RelationalPhysicalRelation),
    Join {
        operator_id: RelationalOperatorId,
        kind: SqlJoinKind,
        algorithm: RelationalPhysicalJoinAlgorithm,
        equi_join_keys: Option<RelationalEquiJoinKeys>,
        selectivity: RelationalJoinSelectivity,
        predicates: Vec<SqlPredicate>,
        left: Box<Self>,
        right: Box<Self>,
        output_schema: RelationalPhysicalOutputSchema,
    },
}

impl RelationalPhysicalJoinNode {
    fn relation(
        binding: BindingId,
        table: String,
        qualifier: String,
        access: RelationalPhysicalAccess,
    ) -> Self {
        Self::Relation(RelationalPhysicalRelation::new(
            binding, table, qualifier, access,
        ))
    }

    fn apply_index_coverage(
        &mut self,
        state: &RelationalState,
        fields: &RelationalFieldPlan,
    ) -> Result<()> {
        match self {
            Self::Relation(relation) => {
                let descriptor = relation.access.descriptor_mut();
                if descriptor.kind != RelationalAccessPathKind::Index {
                    return Ok(());
                }
                let schema = state.table_schema(&relation.table).ok_or_else(|| {
                    SkeinError::Semantic(format!("unknown relational table {}", relation.table))
                })?;
                let covering = fields.index_covers_table(
                    &relation.table,
                    schema,
                    &descriptor.index_columns,
                )?;
                descriptor.covering = covering;
                descriptor.requires_row_fetch = !covering;
                Ok(())
            }
            Self::Join { left, right, .. } => {
                left.apply_index_coverage(state, fields)?;
                right.apply_index_coverage(state, fields)
            }
        }
    }

    fn join(
        operator_id: RelationalOperatorId,
        kind: SqlJoinKind,
        predicates: Vec<SqlPredicate>,
        left: Self,
        right: Self,
    ) -> Result<Self> {
        let algorithm = match &right {
            Self::Relation(relation) if relation.supports_batched_index_probe() => {
                RelationalPhysicalJoinAlgorithm::BatchedIndex
            }
            Self::Relation(_) => RelationalPhysicalJoinAlgorithm::Probe,
            Self::Join { .. } => RelationalPhysicalJoinAlgorithm::Materialized,
        };
        Self::join_with_algorithm(
            operator_id,
            kind,
            algorithm,
            None,
            RelationalJoinSelectivity::Unknown,
            predicates,
            left,
            right,
        )
    }

    fn merge_join(
        operator_id: RelationalOperatorId,
        predicates: Vec<SqlPredicate>,
        equi_join_keys: RelationalEquiJoinKeys,
        selectivity: RelationalJoinSelectivity,
        left: Self,
        right: Self,
    ) -> Result<Self> {
        Self::join_with_algorithm(
            operator_id,
            SqlJoinKind::Inner,
            RelationalPhysicalJoinAlgorithm::Merge,
            Some(equi_join_keys),
            selectivity,
            predicates,
            left,
            right,
        )
    }

    fn hash_join(
        operator_id: RelationalOperatorId,
        kind: SqlJoinKind,
        predicates: Vec<SqlPredicate>,
        equi_join_keys: RelationalEquiJoinKeys,
        selectivity: RelationalJoinSelectivity,
        left: Self,
        right: Self,
    ) -> Result<Self> {
        Self::join_with_algorithm(
            operator_id,
            kind,
            RelationalPhysicalJoinAlgorithm::Hash,
            Some(equi_join_keys),
            selectivity,
            predicates,
            left,
            right,
        )
    }

    fn join_with_algorithm(
        operator_id: RelationalOperatorId,
        kind: SqlJoinKind,
        algorithm: RelationalPhysicalJoinAlgorithm,
        equi_join_keys: Option<RelationalEquiJoinKeys>,
        selectivity: RelationalJoinSelectivity,
        predicates: Vec<SqlPredicate>,
        left: Self,
        right: Self,
    ) -> Result<Self> {
        let output_schema =
            RelationalPhysicalOutputSchema::join(left.output_schema(), right.output_schema())?;
        Ok(Self::Join {
            operator_id,
            kind,
            algorithm,
            equi_join_keys,
            selectivity,
            predicates,
            left: Box::new(left),
            right: Box::new(right),
            output_schema,
        })
    }

    fn output_schema(&self) -> &RelationalPhysicalOutputSchema {
        match self {
            Self::Relation(relation) => &relation.output_schema,
            Self::Join { output_schema, .. } => output_schema,
        }
    }

    fn first_relation(&self) -> &RelationalPhysicalRelation {
        match self {
            Self::Relation(relation) => relation,
            Self::Join { left, .. } => left.first_relation(),
        }
    }

    fn visit_relations<'a>(&'a self, visit: &mut impl FnMut(&'a RelationalPhysicalRelation)) {
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
        visit: &mut impl FnMut(&'a RelationalPhysicalRelation),
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
            Self::Join {
                algorithm,
                left,
                right,
                ..
            } => usize::from(matches!(
                algorithm,
                RelationalPhysicalJoinAlgorithm::Merge
                    | RelationalPhysicalJoinAlgorithm::Hash
                    | RelationalPhysicalJoinAlgorithm::Materialized
            ))
            .saturating_add(left.materialized_right_count())
            .saturating_add(right.materialized_right_count()),
        }
    }

    fn batched_probe_depth(&self) -> usize {
        match self {
            Self::Relation(_) => 0,
            Self::Join {
                algorithm,
                left,
                right,
                ..
            } => usize::from(*algorithm == RelationalPhysicalJoinAlgorithm::BatchedIndex)
                .saturating_add(left.batched_probe_depth())
                .saturating_add(right.batched_probe_depth()),
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::Relation(relation) => {
                let expected = RelationalPhysicalOutputSchema::relation(
                    relation.binding,
                    &relation.table,
                    &relation.qualifier,
                );
                if relation.output_schema != expected {
                    return Err(SkeinError::Execution(format!(
                        "physical relation {} has an inconsistent output schema",
                        relation.qualifier
                    )));
                }
                Ok(())
            }
            Self::Join {
                kind,
                algorithm,
                equi_join_keys,
                left,
                right,
                output_schema,
                ..
            } => {
                left.validate()?;
                right.validate()?;
                let expected_algorithm = match right.as_ref() {
                    Self::Relation(relation) if relation.supports_batched_index_probe() => {
                        RelationalPhysicalJoinAlgorithm::BatchedIndex
                    }
                    Self::Relation(_) => RelationalPhysicalJoinAlgorithm::Probe,
                    Self::Join { .. } => RelationalPhysicalJoinAlgorithm::Materialized,
                };
                match algorithm {
                    RelationalPhysicalJoinAlgorithm::Merge => {
                        if *kind != SqlJoinKind::Inner {
                            return Err(SkeinError::Execution(
                                "merge join supports inner joins only".to_string(),
                            ));
                        }
                        let (Self::Relation(left), Self::Relation(right)) =
                            (left.as_ref(), right.as_ref())
                        else {
                            return Err(SkeinError::Execution(
                                "merge join requires two relation inputs".to_string(),
                            ));
                        };
                        if !matches!(
                            left.access,
                            RelationalPhysicalAccess::Base(RelationalAccessCandidate {
                                access: RelationalBaseAccess::Index { .. },
                                ..
                            })
                        ) || !matches!(
                            right.access,
                            RelationalPhysicalAccess::Base(RelationalAccessCandidate {
                                access: RelationalBaseAccess::Index { .. },
                                ..
                            })
                        ) || equi_join_keys
                            .as_ref()
                            .is_none_or(|keys| keys.columns.is_empty())
                        {
                            return Err(SkeinError::Execution(
                                "merge join has incompatible ordered inputs".to_string(),
                            ));
                        }
                    }
                    RelationalPhysicalJoinAlgorithm::Hash => {
                        if !matches!(kind, SqlJoinKind::Inner | SqlJoinKind::Left) {
                            return Err(SkeinError::Execution(
                                "hash join supports inner and left joins only".to_string(),
                            ));
                        }
                        let (Self::Relation(left), Self::Relation(right)) =
                            (left.as_ref(), right.as_ref())
                        else {
                            return Err(SkeinError::Execution(
                                "hash join requires two relation inputs".to_string(),
                            ));
                        };
                        if !matches!(left.access, RelationalPhysicalAccess::Base(_))
                            || !matches!(
                                right.access,
                                RelationalPhysicalAccess::Base(RelationalAccessCandidate {
                                    access: RelationalBaseAccess::FullScan,
                                    ..
                                })
                            )
                            || equi_join_keys
                                .as_ref()
                                .is_none_or(|keys| keys.columns.is_empty())
                        {
                            return Err(SkeinError::Execution(
                                "hash join has incompatible build input".to_string(),
                            ));
                        }
                    }
                    _ if *algorithm != expected_algorithm || equi_join_keys.is_some() => {
                        return Err(SkeinError::Execution(
                            "physical join algorithm disagrees with its right input".to_string(),
                        ));
                    }
                    _ => {}
                }
                let expected_schema = RelationalPhysicalOutputSchema::join(
                    left.output_schema(),
                    right.output_schema(),
                )?;
                if *output_schema != expected_schema {
                    return Err(SkeinError::Execution(
                        "physical join has an inconsistent output schema".to_string(),
                    ));
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone)]
struct RelationalPhysicalJoinPlan {
    root: RelationalPhysicalJoinNode,
    cost_breakdown: PlanCostBreakdown,
    output_schema: RelationalPhysicalOutputSchema,
}

impl RelationalPhysicalJoinPlan {
    fn new(root: RelationalPhysicalJoinNode, cost_breakdown: PlanCostBreakdown) -> Self {
        let output_schema = root.output_schema().clone();
        Self {
            root,
            cost_breakdown,
            output_schema,
        }
    }

    fn validate(&self) -> Result<()> {
        self.root.validate()?;
        if self.output_schema != *self.root.output_schema() {
            return Err(SkeinError::Execution(
                "physical join plan has an inconsistent root output schema".to_string(),
            ));
        }
        Ok(())
    }

    fn apply_index_coverage(
        &mut self,
        state: &RelationalState,
        fields: &RelationalFieldPlan,
    ) -> Result<()> {
        self.root.apply_index_coverage(state, fields)
    }
}

#[derive(Debug)]
struct PreparedRelationalAccessPlan {
    base_access: RelationalAccessCandidate,
    join_accesses: Vec<RelationalJoinAccessCandidate>,
    join_selection: Option<PreparedRelationalJoinSelection>,
    physical_join_plan: Option<RelationalPhysicalJoinPlan>,
}

fn merge_join_inputs(
    state: &RelationalState,
    base: &RelationalAccessCandidate,
    right: &RelationalJoinAccessCandidate,
    right_table: &str,
) -> Option<(RelationalAccessCandidate, RelationalEquiJoinKeys)> {
    let RelationalBaseAccess::Index { scan, .. } = &base.access else {
        return None;
    };
    let RelationalJoinAccess::Index { name, columns } = &right.access else {
        return None;
    };
    if columns.is_empty()
        || base.descriptor.kind != RelationalAccessPathKind::Index
        || base.descriptor.reverse_order
        || scan.direction != RelationalIndexScanDirection::Forward
        || right.descriptor.kind != RelationalAccessPathKind::Index
        || right.descriptor.reverse_order
        || right.descriptor.index_columns.len() < columns.len()
        || right.descriptor.index_columns[..columns.len()]
            != columns
                .iter()
                .map(|(column, _)| column.clone())
                .collect::<Vec<_>>()
    {
        return None;
    }
    let left_columns = columns
        .iter()
        .map(|(_, column)| column.name.clone())
        .collect::<Vec<_>>();
    let left_order_start = base.descriptor.equality_prefix_len;
    if base.descriptor.index_columns.len() < left_order_start.saturating_add(left_columns.len())
        || base.descriptor.index_columns[left_order_start..]
            .iter()
            .take(left_columns.len())
            .ne(left_columns.iter())
    {
        return None;
    }
    let right_base = RelationalAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::Index,
            name: name.clone(),
            index_columns: right.descriptor.index_columns.clone(),
            access_columns: BTreeSet::new(),
            equality_prefix_len: 0,
            order_prefix_len: columns.len(),
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: right.descriptor.covering,
            requires_row_fetch: right.descriptor.requires_row_fetch,
            estimated_rows: state.row_count(right_table).max(1),
        },
        access: RelationalBaseAccess::Index {
            name: name.clone(),
            scan: RelationalIndexRangeScan {
                prefix: RelationalKey(Vec::new()),
                exclusive_bound: None,
                direction: RelationalIndexScanDirection::Forward,
            },
        },
    };
    Some((
        right_base,
        RelationalEquiJoinKeys {
            columns: columns.clone(),
        },
    ))
}

fn hash_join_inputs(
    predicate: &SqlPredicate,
    right: &RelationalJoinAccessCandidate,
    right_table: &str,
    right_qualifier: &str,
) -> Option<(RelationalAccessCandidate, RelationalEquiJoinKeys)> {
    if !matches!(right.access, RelationalJoinAccess::FullScan) {
        return None;
    }
    let mut columns = BTreeMap::new();
    collect_conjunctive_join_equalities(predicate, right_table, right_qualifier, &mut columns);
    if columns.is_empty() {
        return None;
    }
    Some((
        RelationalAccessCandidate {
            descriptor: right.descriptor.clone(),
            access: RelationalBaseAccess::FullScan,
        },
        RelationalEquiJoinKeys {
            columns: columns.into_iter().collect(),
        },
    ))
}

fn materialized_equi_join_selectivity(
    state: &RelationalState,
    index_read_mode: RelationalIndexReadMode<'_>,
    left: &RelationalPhysicalJoinNode,
    right: &RelationalPhysicalJoinNode,
    keys: &RelationalEquiJoinKeys,
) -> RelationalJoinSelectivity {
    let (RelationalPhysicalJoinNode::Relation(left), RelationalPhysicalJoinNode::Relation(right)) =
        (left, right)
    else {
        return RelationalJoinSelectivity::Unknown;
    };
    let left_columns = keys
        .columns
        .iter()
        .map(|(_, column)| column.name.clone())
        .collect::<Vec<_>>();
    let right_columns = keys
        .columns
        .iter()
        .map(|(column, _)| column.clone())
        .collect::<Vec<_>>();
    RelationalJoinSelectivity::equi_join(
        relational_join_distinct_values(state, index_read_mode, left, &left_columns),
        relational_join_distinct_values(state, index_read_mode, right, &right_columns),
    )
}

fn relational_join_distinct_values(
    state: &RelationalState,
    index_read_mode: RelationalIndexReadMode<'_>,
    relation: &RelationalPhysicalRelation,
    columns: &[String],
) -> Option<u64> {
    if columns.is_empty() {
        return None;
    }
    let schema = state.table_schema(&relation.table)?;
    let requested_columns = columns.iter().collect::<BTreeSet<_>>();
    // Planning must not scan relational rows to manufacture NDV. Use a fresh
    // persisted prefix statistic when available, or the exact cardinality of
    // a complete non-null unique key; otherwise retain the cost model's
    // documented fallback.
    for definition in schema.required_index_definitions() {
        if definition.columns.len() < columns.len()
            || definition.columns[..columns.len()]
                .iter()
                .collect::<BTreeSet<_>>()
                != requested_columns
        {
            continue;
        }
        if let Some(statistics) =
            index_read_mode.probe_statistics(&relation.table, &definition.name, columns.len())
        {
            return Some(statistics.distinct_non_null_values);
        }
        let complete_non_null_unique_key = definition.role.is_unique()
            && definition.columns.len() == columns.len()
            && columns.iter().all(|column| {
                schema
                    .column_position(column)
                    .is_some_and(|position| !schema.columns[position].nullable)
            });
        if complete_non_null_unique_key {
            return Some(u64::try_from(state.row_count(&relation.table)).unwrap_or(u64::MAX));
        }
    }
    None
}

impl PreparedRelationalAccessPlan {
    fn uses_specialized_materialized_join(&self) -> bool {
        self.physical_join_plan.as_ref().is_some_and(|plan| {
            matches!(
                &plan.root,
                RelationalPhysicalJoinNode::Join {
                    algorithm: RelationalPhysicalJoinAlgorithm::Merge
                        | RelationalPhysicalJoinAlgorithm::Hash,
                    ..
                }
            )
        })
    }

    fn apply_physical_index_coverage(
        &mut self,
        state: &RelationalState,
        fields: &RelationalFieldPlan,
    ) -> Result<()> {
        self.physical_join_plan
            .as_mut()
            .ok_or_else(|| {
                SkeinError::Execution(
                    "cannot apply relational index coverage before physical planning".to_string(),
                )
            })?
            .apply_index_coverage(state, fields)
    }

    fn finalize_physical_join_plan(
        &mut self,
        statement: &SelectStatement,
        state: &RelationalState,
        index_read_mode: RelationalIndexReadMode<'_>,
    ) -> Result<()> {
        if self.physical_join_plan.is_some() {
            return Ok(());
        }
        if matches!(
            index_read_mode,
            RelationalIndexReadMode::Materialized | RelationalIndexReadMode::Shadow(_)
        ) && self.join_selection.is_none()
            && statement.joins.len() == 1
            && statement.joins[0].kind == SqlJoinKind::Inner
            && let Some((right_access, merge_keys)) = merge_join_inputs(
                state,
                &self.base_access,
                &self.join_accesses[0],
                &statement.joins[0].table.name,
            )
        {
            let base_qualifier = statement
                .from_alias
                .as_deref()
                .unwrap_or(statement.from.name.as_str());
            let join = &statement.joins[0];
            let right_qualifier = join.alias.as_deref().unwrap_or(join.table.name.as_str());
            let left = RelationalPhysicalJoinNode::relation(
                BindingId::new(0),
                statement.from.name.clone(),
                base_qualifier.to_string(),
                RelationalPhysicalAccess::Base(self.base_access.clone()),
            );
            let right = RelationalPhysicalJoinNode::relation(
                BindingId::new(1),
                join.table.name.clone(),
                right_qualifier.to_string(),
                RelationalPhysicalAccess::Base(right_access.clone()),
            );
            let selectivity = materialized_equi_join_selectivity(
                state,
                index_read_mode,
                &left,
                &right,
                &merge_keys,
            );
            let cost = estimate_relational_join_cost(
                estimate_relational_access_cost(self.base_access.descriptor.estimated_rows),
                estimate_relational_access_cost(right_access.descriptor.estimated_rows),
                RelationalJoinCardinality::Inner,
                RelationalJoinRightInput::Materialized,
                selectivity,
            );
            let root = RelationalPhysicalJoinNode::merge_join(
                RelationalOperatorId::from_plan_index(1),
                vec![join.on.clone()],
                merge_keys,
                selectivity,
                left,
                right,
            )?;
            self.physical_join_plan = Some(RelationalPhysicalJoinPlan::new(root, cost));
            return Ok(());
        }
        if self.join_selection.is_none()
            && statement.joins.len() == 1
            && matches!(
                statement.joins[0].kind,
                SqlJoinKind::Inner | SqlJoinKind::Left
            )
            && let join = &statement.joins[0]
            && let Some((right_access, equi_join_keys)) = hash_join_inputs(
                &join.on,
                &self.join_accesses[0],
                &join.table.name,
                join.alias.as_deref().unwrap_or(join.table.name.as_str()),
            )
        {
            let base_qualifier = statement
                .from_alias
                .as_deref()
                .unwrap_or(statement.from.name.as_str());
            let right_qualifier = join.alias.as_deref().unwrap_or(join.table.name.as_str());
            let left = RelationalPhysicalJoinNode::relation(
                BindingId::new(0),
                statement.from.name.clone(),
                base_qualifier.to_string(),
                RelationalPhysicalAccess::Base(self.base_access.clone()),
            );
            let right = RelationalPhysicalJoinNode::relation(
                BindingId::new(1),
                join.table.name.clone(),
                right_qualifier.to_string(),
                RelationalPhysicalAccess::Base(right_access.clone()),
            );
            let selectivity = materialized_equi_join_selectivity(
                state,
                index_read_mode,
                &left,
                &right,
                &equi_join_keys,
            );
            let cost = estimate_relational_join_cost(
                estimate_relational_access_cost(self.base_access.descriptor.estimated_rows),
                estimate_relational_access_cost(right_access.descriptor.estimated_rows),
                match join.kind {
                    SqlJoinKind::Inner => RelationalJoinCardinality::Inner,
                    SqlJoinKind::Left => RelationalJoinCardinality::PreserveLeft,
                },
                RelationalJoinRightInput::Materialized,
                selectivity,
            );
            let root = RelationalPhysicalJoinNode::hash_join(
                RelationalOperatorId::from_plan_index(1),
                join.kind,
                vec![join.on.clone()],
                equi_join_keys,
                selectivity,
                left,
                right,
            )?;
            self.physical_join_plan = Some(RelationalPhysicalJoinPlan::new(root, cost));
            return Ok(());
        }
        let selection = self.join_selection.as_ref();
        let base_binding = selection.map_or(BindingId::new(0), |selection| selection.base_binding);
        let base_qualifier = statement
            .from_alias
            .as_deref()
            .unwrap_or(statement.from.name.as_str());
        let mut root = RelationalPhysicalJoinNode::relation(
            base_binding,
            statement.from.name.clone(),
            base_qualifier.to_string(),
            RelationalPhysicalAccess::Base(self.base_access.clone()),
        );
        let mut cost = estimate_relational_access_cost(self.base_access.descriptor.estimated_rows);
        for (index, (join, access)) in statement.joins.iter().zip(&self.join_accesses).enumerate() {
            let binding = if let Some(selection) = selection {
                *selection.join_bindings.get(index).ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "prepared relational join selection has no binding for join {}",
                        index.saturating_add(1)
                    ))
                })?
            } else {
                let binding = u32::try_from(index.saturating_add(1)).map_err(|_| {
                    SkeinError::Execution(
                        "relational physical join plan exceeds the binding-id range".to_string(),
                    )
                })?;
                BindingId::new(binding)
            };
            let qualifier = join.alias.as_deref().unwrap_or(join.table.name.as_str());
            let right = RelationalPhysicalJoinNode::relation(
                binding,
                join.table.name.clone(),
                qualifier.to_string(),
                RelationalPhysicalAccess::Probe(access.clone()),
            );
            root = RelationalPhysicalJoinNode::join(
                RelationalOperatorId::from_plan_index(index.saturating_add(1)),
                join.kind,
                vec![join.on.clone()],
                root,
                right,
            )?;
            cost = estimate_relational_probe_join_cost(
                cost,
                access.descriptor.estimated_rows,
                match join.kind {
                    SqlJoinKind::Inner => RelationalJoinCardinality::Inner,
                    SqlJoinKind::Left => RelationalJoinCardinality::PreserveLeft,
                },
            );
        }
        let cost_breakdown = selection.map_or(cost, |selection| selection.cost_breakdown);
        self.physical_join_plan = Some(RelationalPhysicalJoinPlan::new(root, cost_breakdown));
        Ok(())
    }

    fn physical_join_plan(&self) -> Result<&RelationalPhysicalJoinPlan> {
        self.physical_join_plan.as_ref().ok_or_else(|| {
            SkeinError::Execution(
                "prepared relational SELECT has no finalized physical join plan".to_string(),
            )
        })
    }
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
    fn prepare(
        select: &SelectStatement,
        access_plan: &PreparedRelationalAccessPlan,
    ) -> Result<Self> {
        let has_aggregate = select.projection.iter().any(projection_contains_aggregate);
        let access_order_by = resolved_access_order_by(select)?;
        let ordered_index_projection = !select.order_by.is_empty()
            && access_order_by.len() == select.order_by.len()
            && access_plan.base_access.descriptor.order_prefix_len == access_order_by.len()
            && select.joins.is_empty()
            && !select.distinct
            && !has_aggregate
            && select.group_by.is_empty()
            && predicate_is_covered_by_access(
                select.selection.as_ref(),
                &access_plan.base_access.descriptor,
                &access_order_by,
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
                .physical_join_plan
                .as_ref()
                .map_or(0, |tree| tree.root.materialized_right_count()),
        );
        Ok(Self {
            mode,
            memory_shape: RelationalExecutionMemoryShape {
                pipeline_batch_count: 1usize.saturating_add(
                    access_plan
                        .physical_join_plan
                        .as_ref()
                        .map_or(0, |tree| tree.root.batched_probe_depth()),
                ),
                blocking_operator_count,
            },
        })
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
        let physical_plan = self.access_plan.physical_join_plan()?;
        if physical_plan.root.relation_count() != self.statement.joins.len().saturating_add(1) {
            return Err(SkeinError::Execution(format!(
                "physical relational join plan has {} relations for a {}-join SELECT",
                physical_plan.root.relation_count(),
                self.statement.joins.len()
            )));
        }
        let mut bindings = BTreeSet::new();
        let mut duplicate = None;
        physical_plan.root.visit_relations(&mut |relation| {
            if !bindings.insert(relation.binding) {
                duplicate = Some(relation.binding);
            }
        });
        if let Some(binding) = duplicate {
            return Err(SkeinError::Execution(format!(
                "physical relational join plan repeats binding {}",
                binding.get()
            )));
        }
        physical_plan.validate()?;
        validate_prepared_physical_join_plan_accesses(&physical_plan.root, true)?;
        planned_tree_operator_cardinality_profiles(physical_plan)?;
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
            != PreparedRelationalExecutionDescriptor::prepare(&self.statement, &self.access_plan)?
        {
            return Err(SkeinError::Execution(
                "prepared relational SELECT has an inconsistent execution descriptor".to_string(),
            ));
        }
        Ok(())
    }
}

fn validate_prepared_physical_join_plan_accesses(
    node: &RelationalPhysicalJoinNode,
    requires_base: bool,
) -> Result<()> {
    match node {
        RelationalPhysicalJoinNode::Relation(relation) => match &relation.access {
            RelationalPhysicalAccess::Base(access)
                if requires_base && base_access_matches_descriptor(access) =>
            {
                Ok(())
            }
            RelationalPhysicalAccess::Probe(access)
                if !requires_base && join_access_matches_descriptor(access) =>
            {
                Ok(())
            }
            _ => Err(SkeinError::Execution(format!(
                "physical relation {} has an invalid access role",
                relation.qualifier
            ))),
        },
        RelationalPhysicalJoinNode::Join {
            algorithm,
            predicates,
            left,
            right,
            ..
        } => {
            if predicates.is_empty() {
                return Err(SkeinError::Execution(
                    "physical join has no predicate".to_string(),
                ));
            }
            validate_prepared_physical_join_plan_accesses(left, true)?;
            validate_prepared_physical_join_plan_accesses(
                right,
                matches!(
                    algorithm,
                    RelationalPhysicalJoinAlgorithm::Merge
                        | RelationalPhysicalJoinAlgorithm::Hash
                        | RelationalPhysicalJoinAlgorithm::Materialized
                ),
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
    planned_tree_operator_cardinality_profiles(prepared.access_plan.physical_join_plan()?)
}

fn planned_tree_operator_cardinality_profiles(
    tree: &RelationalPhysicalJoinPlan,
) -> Result<Vec<RelationalOperatorCardinalityProfile>> {
    fn plan_node(
        node: &RelationalPhysicalJoinNode,
        profiles: &mut [Option<RelationalOperatorCardinalityProfile>],
    ) -> Result<PlanCostBreakdown> {
        match node {
            RelationalPhysicalJoinNode::Relation(relation) => Ok(estimate_relational_access_cost(
                relation.access.descriptor().estimated_rows,
            )),
            RelationalPhysicalJoinNode::Join {
                operator_id,
                kind,
                algorithm,
                selectivity,
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
                    match algorithm {
                        RelationalPhysicalJoinAlgorithm::Probe
                        | RelationalPhysicalJoinAlgorithm::BatchedIndex => {
                            RelationalJoinRightInput::Probe
                        }
                        RelationalPhysicalJoinAlgorithm::Merge
                        | RelationalPhysicalJoinAlgorithm::Hash
                        | RelationalPhysicalJoinAlgorithm::Materialized => {
                            RelationalJoinRightInput::Materialized
                        }
                    },
                    *selectivity,
                );
                let index = operator_id.get().checked_sub(1).ok_or_else(|| {
                    SkeinError::Execution("physical join has an invalid operator id".to_string())
                })?;
                let slot = profiles.get_mut(index).ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "physical join operator {} is outside the plan profile",
                        operator_id.get()
                    ))
                })?;
                if slot.is_some() {
                    return Err(SkeinError::Execution(format!(
                        "physical join repeats operator {}",
                        operator_id.get()
                    )));
                }
                let access_path = right.first_relation().access.descriptor().clone();
                *slot = Some(RelationalOperatorCardinalityProfile {
                    operator_id: *operator_id,
                    operator: relational_join_operator_kind(*kind, &access_path, *algorithm),
                    table: right.first_relation().table.clone(),
                    access_path,
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
        access_path: base.access.descriptor().clone(),
        estimated_rows: base.access.descriptor().estimated_rows,
        actual_rows: None,
        fully_consumed: false,
    });
    let cost = plan_node(&tree.root, &mut profiles)?;
    if cost != tree.cost_breakdown {
        return Err(SkeinError::Execution(
            "physical join operator estimates diverge from the selected join cost".to_string(),
        ));
    }
    profiles
        .into_iter()
        .enumerate()
        .map(|(index, profile)| {
            profile.ok_or_else(|| {
                SkeinError::Execution(format!(
                    "physical join plan has no operator profile at index {index}"
                ))
            })
        })
        .collect()
}

fn relational_join_operator_kind(
    kind: SqlJoinKind,
    access_path: &RelationalAccessPathDescriptor,
    algorithm: RelationalPhysicalJoinAlgorithm,
) -> RelationalOperatorKind {
    match (kind, access_path.kind, algorithm) {
        (SqlJoinKind::Inner, _, RelationalPhysicalJoinAlgorithm::Merge) => {
            RelationalOperatorKind::MergeJoin
        }
        (SqlJoinKind::Inner, _, RelationalPhysicalJoinAlgorithm::Hash) => {
            RelationalOperatorKind::HashJoin
        }
        (SqlJoinKind::Inner, _, RelationalPhysicalJoinAlgorithm::BatchedIndex) => {
            RelationalOperatorKind::BatchedIndexNestedLoopJoin
        }
        (SqlJoinKind::Left, _, RelationalPhysicalJoinAlgorithm::BatchedIndex) => {
            RelationalOperatorKind::BatchedIndexNestedLoopLeftJoin
        }
        (SqlJoinKind::Left, _, RelationalPhysicalJoinAlgorithm::Merge) => {
            RelationalOperatorKind::NestedLoopLeftJoin
        }
        (SqlJoinKind::Left, _, RelationalPhysicalJoinAlgorithm::Hash) => {
            RelationalOperatorKind::HashJoin
        }
        (SqlJoinKind::Inner, RelationalAccessPathKind::FullScan, _)
        | (SqlJoinKind::Inner, _, RelationalPhysicalJoinAlgorithm::Materialized) => {
            RelationalOperatorKind::NestedLoopJoin
        }
        (SqlJoinKind::Left, RelationalAccessPathKind::FullScan, _)
        | (SqlJoinKind::Left, _, RelationalPhysicalJoinAlgorithm::Materialized) => {
            RelationalOperatorKind::NestedLoopLeftJoin
        }
        (SqlJoinKind::Inner, _, RelationalPhysicalJoinAlgorithm::Probe) => {
            RelationalOperatorKind::IndexNestedLoopJoin
        }
        (SqlJoinKind::Left, _, RelationalPhysicalJoinAlgorithm::Probe) => {
            RelationalOperatorKind::IndexNestedLoopLeftJoin
        }
    }
}

fn estimated_rows_as_usize(rows: u64) -> usize {
    usize::try_from(rows).unwrap_or(usize::MAX)
}

struct PlannedJoin<'a> {
    binding: BindingId,
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
        std::iter::once((BindingId::new(0), base_table, base_qualifier, base_schema)).chain(
            joins.iter().map(|join| {
                (
                    join.binding,
                    join.join.table.name.as_str(),
                    join.qualifier.as_str(),
                    join.schema,
                )
            }),
        ),
    )
}

fn relational_physical_join_plan_locator_layout<'a>(
    state: &'a RelationalState,
    tree: &'a RelationalPhysicalJoinPlan,
) -> Result<RelationalLocatorLayout<'a>> {
    let mut bindings = Vec::with_capacity(tree.root.relation_count());
    let mut error = None;
    tree.root.visit_relations(&mut |relation| {
        if error.is_some() {
            return;
        }
        match state.table_schema(&relation.table) {
            Some(schema) => bindings.push((
                relation.binding,
                relation.table.as_str(),
                relation.qualifier.as_str(),
                schema,
            )),
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
    candidate_work: usize,
    max_candidate_work: usize,
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
            candidate_work: 0,
            max_candidate_work: limits.max_candidate_work,
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
        self.checkpoint_after_work()
    }

    fn account_unprofiled_row(&mut self) -> Result<()> {
        self.account_unprofiled_work()
    }

    fn account_candidate_work(&mut self) -> Result<()> {
        self.candidate_work = self.candidate_work.checked_add(1).ok_or_else(|| {
            SkeinError::Execution("relational candidate work count overflow".to_string())
        })?;
        if self.candidate_work > self.max_candidate_work {
            return Err(SkeinError::Execution(format!(
                "relational SQL exceeds max_candidate_work {}",
                self.max_candidate_work
            )));
        }
        self.checkpoint_after_work()
    }

    fn account_unprofiled_work(&mut self) -> Result<()> {
        account_intermediate(&mut self.intermediate_rows, 1, self.max_intermediate_rows)?;
        self.checkpoint_after_work()
    }

    fn checkpoint_after_work(&mut self) -> Result<()> {
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
    read_modes: RelationalQueryReadModes<'_>,
    limits: RelationalQueryLimits,
    join_planning: RelationalJoinPlanningContext,
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
        for item in &select.order_by {
            resolve_relational_order_target(&select, item)?;
        }
        Ok(())
    })?;
    let planned = join_order::plan_select_join_order(
        select,
        parameters,
        state,
        read_modes,
        limits,
        join_planning,
        &mut current_state_bind_nanos,
    )?;
    let mut access_plan = match planned.access_plan {
        Some(access_plan) => access_plan,
        None => {
            prepare_syntax_access_plan(&planned.statement, parameters, state, read_modes, limits)?
        }
    };
    let field_plan = plan_relational_field_plan(&planned.statement, state)?;
    access_plan.finalize_physical_join_plan(&planned.statement, state, read_modes.index)?;
    access_plan.apply_physical_index_coverage(state, &field_plan)?;
    let execution =
        PreparedRelationalExecutionDescriptor::prepare(&planned.statement, &access_plan)?;
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
    read_modes: RelationalQueryReadModes<'_>,
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
    let access_order_by = resolved_access_order_by(select)?;
    let base_access = choose_base_access(RelationalBaseAccessPlanning {
        predicate: select.selection.as_ref(),
        order_by: &access_order_by,
        prefer_ordered_access,
        parameters,
        state,
        schema: base_schema,
        table: &select.from.name,
        qualifier: &base_qualifier,
        cardinality_limit: limits.max_intermediate_rows.saturating_add(1),
        projection: projection_access_planning(read_modes.row, &select.from.name),
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
                read_modes.index,
                projection_access_planning(read_modes.row, &join.table.name),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PreparedRelationalAccessPlan {
        base_access,
        join_accesses,
        join_selection: None,
        physical_join_plan: None,
    })
}

fn resolved_access_order_by(select: &SelectStatement) -> Result<Vec<crate::sql::SqlOrderItem>> {
    let mut resolved = Vec::with_capacity(select.order_by.len());
    let mut supports_ordered_access = true;
    for item in &select.order_by {
        let column = match resolve_relational_order_target(select, item)? {
            RelationalOrderTarget::InputColumn(column) => Some(column),
            RelationalOrderTarget::ProjectionColumn { column, .. } => Some(column),
            RelationalOrderTarget::ProjectionExpression { .. } => {
                supports_ordered_access = false;
                None
            }
        };
        if let Some(column) = column {
            resolved.push(crate::sql::SqlOrderItem {
                column: column.clone(),
                direction: item.direction,
                nulls: item.nulls,
            });
        }
    }
    Ok(if supports_ordered_access {
        resolved
    } else {
        Vec::new()
    })
}

fn prepared_access_descriptors(
    plan: &PreparedRelationalAccessPlan,
) -> (
    RelationalAccessPathDescriptor,
    Vec<RelationalAccessPathDescriptor>,
) {
    if let Some(tree) = &plan.physical_join_plan {
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

fn plan_relational_field_plan(
    select: &SelectStatement,
    state: &RelationalState,
) -> Result<RelationalFieldPlan> {
    let has_aggregate = select.projection.iter().any(projection_contains_aggregate);
    let output_fields = plan_requested_fields(select, state)?;
    let projects_before_order = order_by_uses_expression_alias(select)?;
    let scan_fields = if (!select.order_by.is_empty()
        && !select.distinct
        && !has_aggregate
        && !projects_before_order)
        || has_aggregate
        || !select.group_by.is_empty()
    {
        plan_scan_fields(select, state)?
    } else {
        output_fields.clone()
    };
    let scan_hydration_fields = plan_scan_hydration_fields(select, state, &scan_fields)?;
    Ok(RelationalFieldPlan::new(
        scan_fields,
        scan_hydration_fields,
        output_fields,
    ))
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
    let base_access = &prepared.access_plan.base_access;
    let (access_path, join_access_paths) = prepared_access_descriptors(&prepared.access_plan);
    let mut planned_joins = Vec::with_capacity(select.joins.len());
    for (index, (join, join_access)) in select
        .joins
        .iter()
        .zip(&prepared.access_plan.join_accesses)
        .enumerate()
    {
        let join_schema = state.table_schema(&join.table.name).ok_or_else(|| {
            SkeinError::Semantic(format!("unknown relational table {}", join.table.name))
        })?;
        let qualifier = join
            .alias
            .clone()
            .unwrap_or_else(|| join.table.name.clone());
        planned_joins.push(PlannedJoin {
            binding: BindingId::new(u32::try_from(index.saturating_add(1)).map_err(|_| {
                SkeinError::Execution(
                    "relational planned join exceeds the binding-id range".to_string(),
                )
            })?),
            join,
            schema: join_schema,
            qualifier,
            access: join_access.access.clone(),
        });
    }
    let default_task = skein_core::RuntimeTaskContext::default();
    let row_task = task_context.unwrap_or(&default_task);
    let field_plan = plan_relational_field_plan(select, state)?;
    let row_runtime = RelationalRowRuntime::new(
        state,
        row_read_mode,
        field_plan,
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
    let physical_execution = RelationalPhysicalJoinExecution {
        tree: prepared.access_plan.physical_join_plan()?,
        memory: execution_memory,
        memory_ledger: &memory_ledger,
        reports: RefCell::new(Vec::new()),
    };
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
            Some(&physical_execution),
            &mut pipeline,
            &index_runtime,
            &row_runtime,
            limits,
        )?;
        output
            .blocking_operator_memory_reports
            .extend(physical_execution.take_reports());
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
            Some(&physical_execution),
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
        output
            .blocking_operator_memory_reports
            .extend(physical_execution.take_reports());
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
        Some(&physical_execution),
        &mut pipeline,
        &index_runtime,
        &row_runtime,
        limits,
        execution_memory,
        &memory_ledger,
    )?;
    output
        .blocking_operator_memory_reports
        .extend(physical_execution.take_reports());
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
    identity: RelationalExplainNodeIdentity,
    estimated_rows: Option<usize>,
    access_object: String,
    operator_info: String,
    report_operator: Option<&'static str>,
}

#[derive(Debug, Clone, Copy)]
enum RelationalExplainNodeIdentity {
    Profile(RelationalOperatorId),
    Blocking(&'static str),
    Logical(&'static str),
}

impl RelationalExplainNodeIdentity {
    fn profile_id(self) -> Option<RelationalOperatorId> {
        match self {
            Self::Profile(operator_id) => Some(operator_id),
            Self::Blocking(_) | Self::Logical(_) => None,
        }
    }

    fn display_id(self) -> String {
        match self {
            Self::Profile(operator_id) => operator_id.get().to_string(),
            Self::Blocking(name) => format!("blocking_{name}"),
            Self::Logical(name) => format!("logical_{name}"),
        }
    }
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
            identity: RelationalExplainNodeIdentity::Logical("limit"),
            estimated_rows: bound_limit.map(|limit| usize::try_from(limit).unwrap_or(usize::MAX)),
            access_object: String::new(),
            operator_info: format!(
                "implementation=fused, offset={}, count={}",
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
            identity: RelationalExplainNodeIdentity::Blocking("top_n"),
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
            identity: RelationalExplainNodeIdentity::Blocking("aggregate"),
            estimated_rows: (!select.group_by.is_empty()).then_some(
                output
                    .access_path
                    .estimated_rows
                    .min(limits.max_intermediate_rows),
            ),
            access_object: String::new(),
            operator_info: if select.group_by.is_empty() {
                format!(
                    "group_by=[], aggregates=[{}]",
                    explain_aggregate_projections(&select.projection)
                )
            } else {
                format!(
                    "group_by=[{}], aggregates=[{}]",
                    explain_columns(&select.group_by),
                    explain_aggregate_projections(&select.projection)
                )
            },
            report_operator: Some("RelationalAggregateExec"),
        });
    }
    if !select.group_by.is_empty() {
        nodes.push(RelationalExplainNode {
            operator: "SortExec",
            identity: RelationalExplainNodeIdentity::Blocking("group_sort"),
            estimated_rows: Some(output.access_path.estimated_rows),
            access_object: String::new(),
            operator_info: format!("group_keys=[{}]", explain_columns(&select.group_by)),
            report_operator: Some("SortExec"),
        });
    }
    if select.distinct || single_count_distinct_column(select).is_some() {
        nodes.push(RelationalExplainNode {
            operator: "DistinctExec",
            identity: RelationalExplainNodeIdentity::Blocking("distinct"),
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
        identity: RelationalExplainNodeIdentity::Logical("projection"),
        estimated_rows: Some(output.access_path.estimated_rows),
        access_object: String::new(),
        operator_info: format!("implementation=fused, columns={}", select.projection.len()),
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
            identity: RelationalExplainNodeIdentity::Logical("selection"),
            estimated_rows: Some(output.access_path.estimated_rows),
            access_object: String::new(),
            operator_info: format!(
                "implementation=fused, residual_predicate={}",
                explain_predicate(select.selection.as_ref().expect("selection is present"))
            ),
            report_operator: None,
        });
    }
    for cardinality in output.operator_cardinality_profiles.iter().skip(1) {
        let operator_id = cardinality.operator_id;
        let table = &cardinality.table;
        let descriptor = &cardinality.access_path;
        let access_path = explain_access_path(
            descriptor,
            relational_index_evidence(&output, table, descriptor),
            &output.row_execution_evidence,
        );
        nodes.push(RelationalExplainNode {
            operator: cardinality.operator.as_str(),
            identity: RelationalExplainNodeIdentity::Profile(operator_id),
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
    let base_table =
        base_cardinality.map_or(select.from.name.as_str(), |profile| profile.table.as_str());
    let base_access_path =
        base_cardinality.map_or(&output.access_path, |profile| &profile.access_path);
    nodes.push(RelationalExplainNode {
        operator: base_cardinality.map_or_else(
            || match base_access_path.kind {
                RelationalAccessPathKind::FullScan => "TableFullScanExec",
                RelationalAccessPathKind::PrimaryKey => "TablePointGetExec",
                RelationalAccessPathKind::Index => "IndexRangeScanExec",
            },
            |profile| profile.operator.as_str(),
        ),
        identity: RelationalExplainNodeIdentity::Profile(base_operator_id),
        estimated_rows: base_cardinality.map(|profile| profile.estimated_rows),
        access_object: explain_access_object(base_table, base_access_path),
        operator_info: explain_access_path(
            base_access_path,
            relational_index_evidence(&output, base_table, base_access_path),
            &output.row_execution_evidence,
        ),
        report_operator: None,
    });

    let mut rows = Vec::with_capacity(nodes.len());
    let mut payload_bytes = 0usize;
    for (index, node) in nodes.iter().enumerate() {
        let report = node.report_operator.and_then(|operator| {
            output
                .blocking_operator_memory_reports
                .iter()
                .find(|report| report.operator == operator)
        });
        let cardinality = node
            .identity
            .profile_id()
            .and_then(|operator_id| relational_operator_cardinality_profile(&output, operator_id));
        let display_id = node.identity.display_id();
        let mut row = Row::from([
            (
                "id".to_string(),
                Value::String(explain_tree_id(node.operator, index, &display_id)),
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
                cardinality.map_or(Value::Null, |cardinality| {
                    optional_usize_explain_value(cardinality.actual_rows)
                }),
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
                        "statement_output_rows={actual_output_rows}, intermediate_rows={}, hydrated_rows={}, compressed_bytes={}, decompressed_bytes={}",
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

fn explain_tree_id(operator: &str, index: usize, display_id: &str) -> String {
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
        "equality_prefix={}, order_prefix={}, exclusive_seek={}, direction={}, unique_point={}, covering={}, row_fetch={}",
        descriptor.equality_prefix_len,
        descriptor.order_prefix_len,
        descriptor.exclusive_range,
        if descriptor.reverse_order {
            "backward"
        } else {
            "forward"
        },
        descriptor.unique_point,
        descriptor.covering,
        descriptor.requires_row_fetch
    );
    let row = format!(
        "row_runtime_path={}, row_projection_generation={}, row_projection_source_watermark={}, row_projection_version={}, row_projection_publication_epoch={}, row_base_generation={}, row_delta_generation={}, row_base_epoch={}, row_visible_epoch={}, row_root_set_digest={}, row_descriptor_reads={}, row_logical_pages={}, row_logical_bytes={}, row_physical_pages={}, row_physical_bytes={}, row_cache_hits={}, row_cache_misses={}, row_cache_admission_rejections={}, row_overlay_entries={}, row_overlay_bytes={}, row_rows={}, row_borrowed_rows={}, row_owned_rows={}, row_index_covered_rows={}",
        row_evidence.runtime_path,
        row_evidence
            .projection_generation
            .as_deref()
            .unwrap_or("none"),
        optional_u64_text(row_evidence.projection_source_watermark),
        optional_u64_text(row_evidence.projection_version),
        optional_u64_text(row_evidence.projection_publication_commit_epoch),
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
        row_evidence.index_covered_rows,
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
        "{planned}, runtime_path={}, lookups={}, range_lookups={}, exclusive_seek_lookups={}, backward_lookups={}, early_stop_lookups={}, demand_paged={}, authoritative={}, transaction_workspace={}, canonical_fallback={}, fallback_reasons={}, base_generation={}, delta_generation={}, base_epoch={}, visible_epoch={}, root_set_digest={}, logical_pages={}, logical_bytes={}, physical_pages={}, physical_bytes={}, cache_hits={}, cache_misses={}, cache_admission_rejections={}, delta_pages_skipped={}, delta_entries={}, live_batches={}, live_entries={}, live_matches={}, live_bytes={}, index_rows={}, {row}",
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
        evidence.delta_pages_skipped,
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

fn explain_aggregate_projections(projections: &[SelectProjection]) -> String {
    projections
        .iter()
        .filter_map(|projection| match projection {
            SelectProjection::Expression { expression, .. } => {
                explain_aggregate_expression(expression)
            }
            SelectProjection::Wildcard | SelectProjection::Column { .. } => None,
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn explain_aggregate_expression(expression: &SqlExpression) -> Option<String> {
    let SqlExpression::Function {
        name,
        arguments,
        filter,
        ..
    } = expression
    else {
        return None;
    };
    if name == "coalesce" {
        let aggregates = arguments
            .iter()
            .filter_map(|argument| match argument {
                SqlFunctionArgument::Expression(expression) => {
                    explain_aggregate_expression(expression)
                }
                SqlFunctionArgument::Wildcard => None,
            })
            .collect::<Vec<_>>();
        return (!aggregates.is_empty()).then(|| format!("coalesce({})", aggregates.join(", ")));
    }
    if !matches!(name.as_str(), "count" | "sum" | "max") {
        return None;
    }
    let arguments = arguments
        .iter()
        .map(|argument| match argument {
            SqlFunctionArgument::Wildcard => "*".to_string(),
            SqlFunctionArgument::Expression(expression) => explain_expression(expression),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut explanation = format!("{name}({arguments})");
    if let Some(filter) = filter {
        explanation.push_str(" FILTER (");
        explanation.push_str(&explain_predicate(filter));
        explanation.push(')');
    }
    Some(explanation)
}

fn explain_expression(expression: &SqlExpression) -> String {
    match expression {
        SqlExpression::Column(column) => explain_column(column),
        SqlExpression::Value(value) => explain_sql_value(value),
        SqlExpression::Function {
            name, arguments, ..
        } => format!(
            "{name}({})",
            arguments
                .iter()
                .map(|argument| match argument {
                    SqlFunctionArgument::Wildcard => "*".to_string(),
                    SqlFunctionArgument::Expression(expression) => explain_expression(expression),
                })
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn explain_predicate(predicate: &SqlPredicate) -> String {
    match predicate {
        SqlPredicate::And(left, right) => {
            format!(
                "({} AND {})",
                explain_predicate(left),
                explain_predicate(right)
            )
        }
        SqlPredicate::Or(left, right) => {
            format!(
                "({} OR {})",
                explain_predicate(left),
                explain_predicate(right)
            )
        }
        SqlPredicate::Not(predicate) => format!("NOT ({})", explain_predicate(predicate)),
        SqlPredicate::Compare { left, op, right } => format!(
            "{} {} {}",
            explain_column(left),
            explain_comparison_operator(*op),
            explain_sql_value(right)
        ),
        SqlPredicate::CompareColumns { left, op, right } => format!(
            "{} {} {}",
            explain_column(left),
            explain_comparison_operator(*op),
            explain_column(right)
        ),
        SqlPredicate::InList {
            left,
            values,
            negated,
        } => format!(
            "{} {}IN ({})",
            explain_column(left),
            if *negated { "NOT " } else { "" },
            values
                .iter()
                .map(explain_sql_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        SqlPredicate::Like {
            left,
            pattern,
            case_insensitive,
            negated,
            escape,
        } => {
            let mut explanation = format!(
                "{} {}{} {}",
                explain_column(left),
                if *negated { "NOT " } else { "" },
                if *case_insensitive { "ILIKE" } else { "LIKE" },
                explain_sql_value(pattern)
            );
            match escape {
                SqlLikeEscape::Character('\\') => {}
                SqlLikeEscape::Character(character) => {
                    explanation.push_str(&format!(" ESCAPE '{character}'"));
                }
                SqlLikeEscape::Disabled => explanation.push_str(" ESCAPE ''"),
            }
            explanation
        }
        SqlPredicate::IsNull { column, negated } => format!(
            "{} IS {}NULL",
            explain_column(column),
            if *negated { "NOT " } else { "" }
        ),
    }
}

fn explain_comparison_operator(operator: SqlComparisonOp) -> &'static str {
    match operator {
        SqlComparisonOp::Eq => "=",
        SqlComparisonOp::NotEq => "!=",
        SqlComparisonOp::Lt => "<",
        SqlComparisonOp::Lte => "<=",
        SqlComparisonOp::Gt => ">",
        SqlComparisonOp::Gte => ">=",
    }
}

fn explain_sql_value(value: &SqlValue) -> String {
    match value {
        SqlValue::Literal(value) => value.to_string(),
        SqlValue::Parameter(position) => format!("${position}"),
    }
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
    projection: RelationalProjectionAccessPlanning,
}

#[derive(Debug, Clone, Copy, Default)]
struct RelationalProjectionAccessPlanning {
    force_full_scan: bool,
    row_count_override: Option<usize>,
}

fn projection_access_planning(
    row_read_mode: RelationalRowReadMode<'_>,
    table: &str,
) -> RelationalProjectionAccessPlanning {
    RelationalProjectionAccessPlanning {
        force_full_scan: row_read_mode.is_projection_table(table),
        row_count_override: row_read_mode.projection_estimated_rows(table),
    }
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
        projection,
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
        let value = value_to_relational_as(
            bind_sql_value(value, parameters)?,
            schema.columns[position].scalar_type,
        )?;
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

    let row_count = projection
        .row_count_override
        .unwrap_or_else(|| state.row_count(table));
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

    if !projection.force_full_scan {
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
        let Some(position) = schema.column_position(&item.column.name) else {
            return Ok(None);
        };
        let value = value_to_relational_as(
            bind_sql_value(value, parameters)?,
            schema.columns[position].scalar_type,
        )?;
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
    projection: RelationalProjectionAccessPlanning,
) -> Result<RelationalJoinAccessCandidate> {
    let mut bound = BTreeMap::<String, SqlColumnRef>::new();
    collect_conjunctive_join_equalities(predicate, table, qualifier, &mut bound);
    let row_count = projection
        .row_count_override
        .unwrap_or_else(|| state.row_count(table));
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

    if !projection.force_full_scan {
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
                debug_assert!(statistics.fanout >= statistics.average_fanout());
                usize::try_from(statistics.average_fanout())
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
    descriptor: Option<&RelationalAccessPathDescriptor>,
    visit: &mut dyn FnMut(RelationalReadRow) -> Result<bool>,
) -> Result<bool> {
    match access {
        RelationalBaseAccess::PrimaryKey(key) => match row_runtime.read_point(table, key)? {
            Some(row) => visit(row),
            None => Ok(true),
        },
        RelationalBaseAccess::Index { name, scan } => {
            let covered_columns = descriptor
                .filter(|descriptor| descriptor.covering)
                .map(|descriptor| descriptor.index_columns.as_slice());
            index_runtime.visit_range_entries(state, table, name, scan, |index_key, key| {
                let row = match covered_columns {
                    Some(index_columns) => row_runtime
                        .read_index_covered(table, index_columns, index_key, key)?,
                    None => row_runtime.read_point(table, key)?,
                };
                match row {
                    Some(row) => visit(row),
                    None => Err(SkeinError::StorageIntegrity(format!(
                        "relational index {name} on table {table} points to missing or non-coverable row {key:?}"
                    ))),
                }
            })
        }
        RelationalBaseAccess::FullScan => row_runtime.visit_all(table, visit),
    }
}

struct RelationalPhysicalJoinExecution<'a> {
    tree: &'a RelationalPhysicalJoinPlan,
    memory: &'a skein_executor::ExecutionMemoryConfig,
    memory_ledger: &'a QueryMemoryLedger,
    reports: RefCell<Vec<BlockingOperatorMemoryReport>>,
}

impl RelationalPhysicalJoinExecution<'_> {
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

fn relational_key_resident_bytes(key: &RelationalKey) -> usize {
    std::mem::size_of::<RelationalKey>()
        .saturating_add(
            key.0
                .capacity()
                .saturating_mul(std::mem::size_of::<RelationalValue>()),
        )
        .saturating_add(
            key.0
                .iter()
                .map(|value| match value {
                    RelationalValue::Text(value) => value.capacity(),
                    RelationalValue::Bytea(value) => value.capacity(),
                    _ => 0,
                })
                .sum::<usize>(),
        )
}

fn visit_tree_relation_entries<'a>(
    state: &'a RelationalState,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    relation: &'a RelationalPhysicalRelation,
    outer: Option<&BoundRow<'a>>,
    visit: &mut dyn FnMut(RelationalReadRow) -> Result<bool>,
) -> Result<bool> {
    match &relation.access {
        RelationalPhysicalAccess::Base(access) => visit_base_entries(
            state,
            index_runtime,
            row_runtime,
            &relation.table,
            &access.access,
            Some(&access.descriptor),
            visit,
        ),
        RelationalPhysicalAccess::Probe(access) => {
            let outer = outer.ok_or_else(|| {
                SkeinError::Execution(format!(
                    "physical join probe for {} has no outer row",
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
                    let covered_columns = access
                        .descriptor
                        .covering
                        .then_some(access.descriptor.index_columns.as_slice());
                    index_runtime.visit_prefix_entries(
                        state,
                        &relation.table,
                        name,
                        &prefix,
                        |index_key, key| {
                            let row = match covered_columns {
                                Some(index_columns) => row_runtime.read_index_covered(
                                    &relation.table,
                                    index_columns,
                                    index_key,
                                    key,
                                )?,
                                None => row_runtime.read_point(&relation.table, key)?,
                            };
                            match row {
                            Some(row) => visit(row),
                            None => Err(SkeinError::StorageIntegrity(format!(
                                "relational index {name} on table {} points to missing or non-coverable row {key:?}",
                                relation.table
                            ))),
                            }
                        },
                    )
                }
                RelationalJoinAccess::FullScan => row_runtime.visit_all(&relation.table, visit),
            }
        }
    }
}

fn null_extended_tree_row<'a>(
    node: &'a RelationalPhysicalJoinNode,
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
                binding: relation.binding,
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
    let row = BoundRow { bindings };
    node.output_schema().ensure_matches(&row)?;
    Ok(row)
}

fn batched_index_probe_key(
    state: &RelationalState,
    relation: &RelationalPhysicalRelation,
    outer: &BoundRow<'_>,
) -> Result<Option<RelationalKey>> {
    let RelationalPhysicalAccess::Probe(candidate) = &relation.access else {
        return Err(SkeinError::Execution(format!(
            "batched index join relation {} is not a probe input",
            relation.qualifier
        )));
    };
    let columns = match &candidate.access {
        RelationalJoinAccess::PrimaryKey(columns) | RelationalJoinAccess::Index { columns, .. } => {
            columns
        }
        RelationalJoinAccess::FullScan => {
            return Err(SkeinError::Execution(format!(
                "batched index join relation {} has a full-scan probe",
                relation.qualifier
            )));
        }
    };
    let schema = state.table_schema(&relation.table).ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational table {}", relation.table))
    })?;
    bound_join_key(outer, schema, columns)
}

const BATCHED_INDEX_JOIN_CACHE_ENTRY_OVERHEAD_BYTES: usize = 128;

#[derive(Debug, Clone)]
struct BatchedIndexJoinLocator {
    primary_key: RelationalKey,
    index_key: Option<RelationalKey>,
}

#[allow(clippy::too_many_arguments)]
fn flush_batched_index_join_rows<'a>(
    batch: &[(BoundRow<'a>, Option<RelationalKey>)],
    batch_tracker: &mut OperatorMemoryTracker,
    operator_id: RelationalOperatorId,
    kind: SqlJoinKind,
    predicates: &[SqlPredicate],
    right: &'a RelationalPhysicalJoinNode,
    null_right: Option<&BoundRow<'a>>,
    output_schema: &RelationalPhysicalOutputSchema,
    parameters: &[Value],
    state: &'a RelationalState,
    execution: &RelationalPhysicalJoinExecution<'a>,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    let RelationalPhysicalJoinNode::Relation(right_relation) = right else {
        return Err(SkeinError::Execution(
            "batched index nested-loop join requires a relational probe input".to_string(),
        ));
    };
    let RelationalPhysicalAccess::Probe(access) = &right_relation.access else {
        return Err(SkeinError::Execution(format!(
            "batched index join relation {} is not a probe input",
            right_relation.qualifier
        )));
    };

    let mut locators_by_probe = BTreeMap::<RelationalKey, Vec<BatchedIndexJoinLocator>>::new();
    for (_, probe_key) in batch {
        let Some(probe_key) = probe_key else {
            continue;
        };
        if locators_by_probe.contains_key(probe_key) {
            continue;
        }
        if batch_tracker.would_exceed(BATCHED_INDEX_JOIN_CACHE_ENTRY_OVERHEAD_BYTES) {
            return Err(SkeinError::Execution(format!(
                "RelationalBatchedIndexJoin cache exceeds batch_payload_bytes {}",
                execution.memory.batch_payload_bytes
            )));
        }
        batch_tracker.try_charge(BATCHED_INDEX_JOIN_CACHE_ENTRY_OVERHEAD_BYTES)?;
        let key_bytes = relational_key_resident_bytes(probe_key);
        if batch_tracker.would_exceed(key_bytes) {
            return Err(SkeinError::Execution(format!(
                "RelationalBatchedIndexJoin cache exceeds batch_payload_bytes {}",
                execution.memory.batch_payload_bytes
            )));
        }
        batch_tracker.try_charge(key_bytes)?;

        let mut locators = Vec::new();
        match &access.access {
            RelationalJoinAccess::PrimaryKey(_) => {
                let bytes = relational_key_resident_bytes(probe_key);
                if batch_tracker.would_exceed(bytes) {
                    return Err(SkeinError::Execution(format!(
                        "RelationalBatchedIndexJoin cache exceeds batch_payload_bytes {}",
                        execution.memory.batch_payload_bytes
                    )));
                }
                batch_tracker.try_charge(bytes)?;
                locators.push(BatchedIndexJoinLocator {
                    primary_key: probe_key.clone(),
                    index_key: None,
                });
            }
            RelationalJoinAccess::Index { .. } => {}
            RelationalJoinAccess::FullScan => {
                return Err(SkeinError::Execution(format!(
                    "batched index join relation {} has a full-scan probe",
                    right_relation.qualifier
                )));
            }
        }
        locators_by_probe.insert(probe_key.clone(), locators);
    }

    if let RelationalJoinAccess::Index { name, .. } = &access.access {
        let prefixes = locators_by_probe.keys().cloned().collect::<Vec<_>>();
        index_runtime.visit_prefix_entries_many(
            state,
            &right_relation.table,
            name,
            &prefixes,
            |prefix, index_key, primary_key| {
                let bytes = relational_key_resident_bytes(index_key)
                    .saturating_add(relational_key_resident_bytes(primary_key));
                if batch_tracker.would_exceed(bytes) {
                    return Err(SkeinError::Execution(format!(
                        "RelationalBatchedIndexJoin cache exceeds batch_payload_bytes {}",
                        execution.memory.batch_payload_bytes
                    )));
                }
                batch_tracker.try_charge(bytes)?;
                let locators = locators_by_probe.get_mut(prefix).ok_or_else(|| {
                    SkeinError::StorageIntegrity(
                        "batch index reader emitted an unknown requested prefix".to_string(),
                    )
                })?;
                locators.push(BatchedIndexJoinLocator {
                    primary_key: primary_key.clone(),
                    index_key: Some(index_key.clone()),
                });
                Ok(true)
            },
        )?;
    }

    let primary_keys = locators_by_probe
        .values()
        .flatten()
        .map(|locator| locator.primary_key.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let read_rows = (!access.descriptor.covering)
        .then(|| row_runtime.read_points(&right_relation.table, &primary_keys))
        .transpose()?;
    let schema = state.table_schema(&right_relation.table).ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational table {}", right_relation.table))
    })?;
    let mut candidates = BTreeMap::<RelationalKey, Vec<BoundRow<'a>>>::new();
    for (probe_key, locators) in locators_by_probe {
        let mut rows = Vec::with_capacity(locators.len());
        for locator in locators {
            let row = match (&locator.index_key, &read_rows) {
                (Some(index_key), None) => row_runtime.read_index_covered(
                    &right_relation.table,
                    &access.descriptor.index_columns,
                    index_key,
                    &locator.primary_key,
                )?,
                (_, Some(read_rows)) => read_rows.get(&locator.primary_key).cloned(),
                (None, None) => {
                    return Err(SkeinError::StorageIntegrity(format!(
                        "relational primary-key probe on table {} cannot claim secondary-index coverage",
                        right_relation.table
                    )));
                }
            };
            let Some(row) = row else {
                if matches!(access.access, RelationalJoinAccess::PrimaryKey(_)) {
                    continue;
                }
                return Err(SkeinError::StorageIntegrity(format!(
                    "relational index probe on table {} points to missing or non-coverable row {:?}",
                    right_relation.table, locator.primary_key
                )));
            };
            let bound = BoundRow {
                bindings: vec![Binding {
                    binding: right_relation.binding,
                    table: &right_relation.table,
                    qualifier: &right_relation.qualifier,
                    schema,
                    row: Some(row),
                }],
            };
            right_relation.output_schema.ensure_matches(&bound)?;
            let bytes = bound_row_resident_bytes(&bound);
            if batch_tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "RelationalBatchedIndexJoin cache exceeds batch_payload_bytes {}",
                    execution.memory.batch_payload_bytes
                )));
            }
            batch_tracker.try_charge(bytes)?;
            rows.push(bound);
        }
        candidates.insert(probe_key, rows);
    }

    for (left_row, probe_key) in batch {
        pipeline.borrow_mut().account_candidate_work()?;
        let Some(probe_key) = probe_key else {
            if kind != SqlJoinKind::Left {
                continue;
            }
            let Some(null_right) = null_right else {
                return Err(SkeinError::Execution(
                    "left batched index join has no null extension".to_string(),
                ));
            };
            let mut combined = left_row.clone();
            combined.bindings.extend(null_right.bindings.clone());
            output_schema.ensure_matches(&combined)?;
            pipeline.borrow_mut().account_operator_row(operator_id)?;
            if !visit(combined)? {
                return Ok(false);
            }
            continue;
        };

        let mut matched = false;
        let rows = candidates.get(probe_key).ok_or_else(|| {
            SkeinError::Execution("batched index join lost a probe cache entry".to_string())
        })?;
        for right_row in rows {
            pipeline.borrow_mut().account_candidate_work()?;
            let mut combined = left_row.clone();
            combined.bindings.extend(right_row.bindings.clone());
            let mut predicates_match = true;
            for predicate in predicates {
                if predicate_truth(predicate, &combined, parameters)? != Some(true) {
                    predicates_match = false;
                    break;
                }
            }
            if !predicates_match {
                continue;
            }
            output_schema.ensure_matches(&combined)?;
            matched = true;
            pipeline.borrow_mut().account_operator_row(operator_id)?;
            if !visit(combined)? {
                return Ok(false);
            }
        }
        if !matched && kind == SqlJoinKind::Left {
            let Some(null_right) = null_right else {
                return Err(SkeinError::Execution(
                    "left batched index join has no null extension".to_string(),
                ));
            };
            let mut combined = left_row.clone();
            combined.bindings.extend(null_right.bindings.clone());
            output_schema.ensure_matches(&combined)?;
            pipeline.borrow_mut().account_operator_row(operator_id)?;
            if !visit(combined)? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn visit_batched_index_nested_loop<'a>(
    operator_id: RelationalOperatorId,
    kind: SqlJoinKind,
    predicates: &[SqlPredicate],
    left: &'a RelationalPhysicalJoinNode,
    right: &'a RelationalPhysicalJoinNode,
    output_schema: &RelationalPhysicalOutputSchema,
    outer: Option<&BoundRow<'a>>,
    parameters: &[Value],
    state: &'a RelationalState,
    profiled_base_binding: BindingId,
    execution: &RelationalPhysicalJoinExecution<'a>,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    let RelationalPhysicalJoinNode::Relation(right_relation) = right else {
        return Err(SkeinError::Execution(
            "batched index nested-loop join requires a relational probe input".to_string(),
        ));
    };
    let null_right = (kind == SqlJoinKind::Left)
        .then(|| null_extended_tree_row(right, state))
        .transpose()?;
    let mut batch = Vec::new();
    let mut batch_tracker = OperatorMemoryTracker::with_account(
        execution.memory.batch_payload_bytes,
        execution.memory_ledger.account(
            QueryMemoryClass::PipelineBatch,
            "RelationalBatchedIndexJoin input batch",
            execution.memory.batch_payload_bytes,
        ),
    );
    let mut fully_consumed = true;
    let completed = visit_prepared_physical_join_plan_node(
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
            let probe_key = batched_index_probe_key(state, right_relation, &left_row)?;
            let bytes = bound_row_resident_bytes(&left_row)
                .saturating_add(probe_key.as_ref().map_or(0, relational_key_resident_bytes))
                .saturating_add(std::mem::size_of::<(BoundRow<'_>, Option<RelationalKey>)>());
            if batch_tracker.would_exceed(bytes) {
                if batch.is_empty() {
                    return Err(SkeinError::Execution(format!(
                        "RelationalBatchedIndexJoin input row exceeds batch_payload_bytes {}",
                        execution.memory.batch_payload_bytes
                    )));
                }
                fully_consumed = flush_batched_index_join_rows(
                    &batch,
                    &mut batch_tracker,
                    operator_id,
                    kind,
                    predicates,
                    right,
                    null_right.as_ref(),
                    output_schema,
                    parameters,
                    state,
                    execution,
                    pipeline,
                    index_runtime,
                    row_runtime,
                    visit,
                )?;
                batch.clear();
                batch_tracker.reset();
                if !fully_consumed {
                    return Ok(false);
                }
            }
            if batch_tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "RelationalBatchedIndexJoin input row exceeds batch_payload_bytes {}",
                    execution.memory.batch_payload_bytes
                )));
            }
            batch_tracker.try_charge(bytes)?;
            batch.push((left_row, probe_key));
            if batch.len() == execution.memory.batch_rows.get() {
                fully_consumed = flush_batched_index_join_rows(
                    &batch,
                    &mut batch_tracker,
                    operator_id,
                    kind,
                    predicates,
                    right,
                    null_right.as_ref(),
                    output_schema,
                    parameters,
                    state,
                    execution,
                    pipeline,
                    index_runtime,
                    row_runtime,
                    visit,
                )?;
                batch.clear();
                batch_tracker.reset();
            }
            Ok(fully_consumed)
        },
    )?;
    if !completed || !fully_consumed {
        return Ok(false);
    }
    if !batch.is_empty() {
        fully_consumed = flush_batched_index_join_rows(
            &batch,
            &mut batch_tracker,
            operator_id,
            kind,
            predicates,
            right,
            null_right.as_ref(),
            output_schema,
            parameters,
            state,
            execution,
            pipeline,
            index_runtime,
            row_runtime,
            visit,
        )?;
        batch.clear();
    }
    batch_tracker.reset();
    Ok(fully_consumed)
}

fn bound_relation_join_key(
    row: &BoundRow<'_>,
    relation: &RelationalPhysicalRelation,
    keys: &RelationalEquiJoinKeys,
) -> Result<Option<RelationalKey>> {
    let binding = row
        .bindings
        .iter()
        .find(|binding| binding.binding == relation.binding)
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "equi-join relation {} is missing from its row",
                relation.qualifier
            ))
        })?;
    let mut values = Vec::with_capacity(keys.columns.len());
    for (column, _) in &keys.columns {
        let position = binding.schema.column_position(column).ok_or_else(|| {
            SkeinError::Semantic(format!(
                "equi-join relation {} has no column {column}",
                relation.table
            ))
        })?;
        let value = binding.value(position)?.clone();
        if matches!(value, RelationalValue::Null) {
            return Ok(None);
        }
        values.push(value);
    }
    Ok(Some(RelationalKey(values)))
}

#[allow(clippy::too_many_arguments)]
fn visit_index_merge_join<'a>(
    operator_id: RelationalOperatorId,
    predicates: &[SqlPredicate],
    equi_join_keys: &RelationalEquiJoinKeys,
    left: &'a RelationalPhysicalJoinNode,
    right: &'a RelationalPhysicalJoinNode,
    output_schema: &RelationalPhysicalOutputSchema,
    outer: Option<&BoundRow<'a>>,
    parameters: &[Value],
    state: &'a RelationalState,
    profiled_base_binding: BindingId,
    execution: &RelationalPhysicalJoinExecution<'a>,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    if outer.is_some() {
        return Err(SkeinError::Execution(
            "merge join cannot run below a probe input".to_string(),
        ));
    }
    let (
        RelationalPhysicalJoinNode::Relation(_left_relation),
        RelationalPhysicalJoinNode::Relation(right_relation),
    ) = (left, right)
    else {
        return Err(SkeinError::Execution(
            "merge join requires two relation inputs".to_string(),
        ));
    };
    let right_schema = state.table_schema(&right_relation.table).ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational table {}", right_relation.table))
    })?;
    let mut right_rows = Vec::new();
    let mut right_tracker = OperatorMemoryTracker::with_account(
        execution.memory.blocking_operator_bytes,
        execution.memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "RelationalMergeJoinRightInput",
            execution.memory.blocking_operator_bytes,
        ),
    );
    let mut previous_right_key = None;
    visit_prepared_physical_join_plan_node(
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
            let Some(key) = bound_relation_join_key(&row, right_relation, equi_join_keys)? else {
                return Ok(true);
            };
            if previous_right_key
                .as_ref()
                .is_some_and(|previous| key < *previous)
            {
                return Err(SkeinError::Execution(
                    "merge join right input violates its declared key order".to_string(),
                ));
            }
            previous_right_key = Some(key.clone());
            let bytes = bound_row_resident_bytes(&row)
                .saturating_add(relational_key_resident_bytes(&key))
                .saturating_add(std::mem::size_of::<(RelationalKey, BoundRow<'_>)>());
            if right_tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "RelationalMergeJoinRightInput state exceeds blocking_operator_bytes {}",
                    execution.memory.blocking_operator_bytes
                )));
            }
            right_tracker.try_charge(bytes)?;
            right_rows.push((key, row));
            Ok(true)
        },
    )?;
    execution
        .reports
        .borrow_mut()
        .push(skein_executor::blocking::in_memory_report(
            "RelationalMergeJoinRightInput",
            &right_tracker,
            right_tracker.peak_bytes,
            right_rows.len(),
            execution.memory,
        ));

    let mut previous_left_key = None;
    let mut active_right_key = None;
    let mut active_right_range = 0..0;
    let mut right_cursor = 0usize;
    visit_prepared_physical_join_plan_node(
        left,
        None,
        parameters,
        state,
        profiled_base_binding,
        execution,
        pipeline,
        index_runtime,
        row_runtime,
        &mut |left_row| {
            pipeline.borrow_mut().account_candidate_work()?;
            let Some(left_key) = bound_join_key(&left_row, right_schema, &equi_join_keys.columns)?
            else {
                return Ok(true);
            };
            if previous_left_key
                .as_ref()
                .is_some_and(|previous| left_key < *previous)
            {
                return Err(SkeinError::Execution(
                    "merge join left input violates its declared key order".to_string(),
                ));
            }
            previous_left_key = Some(left_key.clone());
            if active_right_key.as_ref() != Some(&left_key) {
                while right_cursor < right_rows.len() && right_rows[right_cursor].0 < left_key {
                    right_cursor = right_cursor.saturating_add(1);
                }
                let start = right_cursor;
                while right_cursor < right_rows.len() && right_rows[right_cursor].0 == left_key {
                    right_cursor = right_cursor.saturating_add(1);
                }
                active_right_key = Some(left_key);
                active_right_range = start..right_cursor;
            }
            for (_, right_row) in &right_rows[active_right_range.clone()] {
                pipeline.borrow_mut().account_candidate_work()?;
                let mut combined = left_row.clone();
                combined.bindings.extend(right_row.bindings.clone());
                let mut predicates_match = true;
                for predicate in predicates {
                    if predicate_truth(predicate, &combined, parameters)? != Some(true) {
                        predicates_match = false;
                        break;
                    }
                }
                if !predicates_match {
                    continue;
                }
                output_schema.ensure_matches(&combined)?;
                pipeline.borrow_mut().account_operator_row(operator_id)?;
                if !visit(combined)? {
                    return Ok(false);
                }
            }
            Ok(true)
        },
    )
}

const HASH_JOIN_MAP_ENTRY_OVERHEAD_BYTES: usize = 192;
const HASH_JOIN_GRACE_PARTITIONS: usize = 2;
const HASH_JOIN_SPILL_BINDING_NAME: &str = "__skein_relational_hash_locator";

#[derive(Debug, Clone, Copy)]
enum HashJoinSpillSide {
    Build,
    Probe,
}

struct HashJoinSpillRun {
    run: SpillRun,
    writer: Option<SpillWriter>,
}

struct HashJoinGraceSpill {
    budget: SpillBudgetTracker,
    build_runs: Vec<Option<HashJoinSpillRun>>,
    probe_runs: Vec<Option<HashJoinSpillRun>>,
    next_ordinal: u64,
    spilled_rows: usize,
}

impl HashJoinGraceSpill {
    fn new(
        memory: &skein_executor::ExecutionMemoryConfig,
        memory_ledger: &QueryMemoryLedger,
    ) -> Self {
        let staging_budget = hash_join_partition_memory_budget(memory);
        Self {
            budget: SpillBudgetTracker::with_ledger_staging_budget(
                "RelationalHashJoinGrace",
                memory,
                memory_ledger,
                staging_budget,
            ),
            build_runs: (0..HASH_JOIN_GRACE_PARTITIONS).map(|_| None).collect(),
            probe_runs: (0..HASH_JOIN_GRACE_PARTITIONS).map(|_| None).collect(),
            next_ordinal: 0,
            spilled_rows: 0,
        }
    }

    fn write(
        &mut self,
        side: HashJoinSpillSide,
        key: &RelationalKey,
        locator: RelationalRowSetLocator,
    ) -> Result<()> {
        let partition = hash_join_partition(key, HASH_JOIN_GRACE_PARTITIONS);
        let operator = match side {
            HashJoinSpillSide::Build => "RelationalHashJoinGraceBuild",
            HashJoinSpillSide::Probe => "RelationalHashJoinGraceProbe",
        };
        let Self {
            budget,
            build_runs,
            probe_runs,
            next_ordinal,
            ..
        } = self;
        let runs = match side {
            HashJoinSpillSide::Build => build_runs,
            HashJoinSpillSide::Probe => probe_runs,
        };
        write_hash_join_spill_record(runs, partition, operator, locator, budget, next_ordinal)?;
        self.spilled_rows = self.spilled_rows.saturating_add(1);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        finish_hash_join_spill_runs(&mut self.build_runs)?;
        finish_hash_join_spill_runs(&mut self.probe_runs)
    }

    fn run(&self, side: HashJoinSpillSide, partition: usize) -> Option<&HashJoinSpillRun> {
        let runs = match side {
            HashJoinSpillSide::Build => &self.build_runs,
            HashJoinSpillSide::Probe => &self.probe_runs,
        };
        runs.get(partition).and_then(Option::as_ref)
    }
}

fn hash_join_partition(key: &RelationalKey, partition_count: usize) -> usize {
    debug_assert!(partition_count > 0);
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % partition_count
}

fn write_hash_join_spill_record(
    runs: &mut [Option<HashJoinSpillRun>],
    partition: usize,
    operator: &str,
    locator: RelationalRowSetLocator,
    budget: &mut SpillBudgetTracker,
    next_ordinal: &mut u64,
) -> Result<()> {
    let binding = ExecutorBinding::scalar(
        HASH_JOIN_SPILL_BINDING_NAME,
        Value::Binary(locator.encode_hash_spill_record()?),
    );
    let slot = runs.get_mut(partition).ok_or_else(|| {
        SkeinError::Execution(format!(
            "hash join spill partition {partition} is out of bounds"
        ))
    })?;
    if slot.is_none() {
        let (run, writer) = budget.create_run(operator)?;
        *slot = Some(HashJoinSpillRun {
            run,
            writer: Some(writer),
        });
    }
    let mut run = slot
        .take()
        .expect("hash join spill run is initialized before writing");
    let result = run
        .writer
        .as_mut()
        .ok_or_else(|| {
            SkeinError::Execution("hash join spill writer is already closed".to_string())
        })?
        .write(*next_ordinal, &binding, budget);
    *slot = Some(run);
    result?;
    *next_ordinal = next_ordinal.saturating_add(1);
    Ok(())
}

fn finish_hash_join_spill_runs(runs: &mut [Option<HashJoinSpillRun>]) -> Result<()> {
    for run in runs.iter_mut().flatten() {
        if let Some(writer) = run.writer.take() {
            writer.finish()?;
        }
    }
    Ok(())
}

fn map_hash_join_spill_record<T>(
    reader: &mut skein_executor::spill::SpillReader,
    max_record_bytes: usize,
    spill_budget: &SpillBudgetTracker,
    tracker: &mut OperatorMemoryTracker,
    map: impl FnOnce(RelationalRowSetLocator) -> Result<T>,
) -> Result<Option<T>> {
    reader
        .read_binding_record(max_record_bytes, spill_budget)?
        .map(|record| {
            record.try_map(
                "RelationalHashJoinGraceReplay",
                max_record_bytes,
                tracker,
                |_ordinal, binding| map(hash_join_spill_locator(binding)?),
                |_| 0,
            )
        })
        .transpose()
}

fn hash_join_spill_locator(binding: ExecutorBinding) -> Result<RelationalRowSetLocator> {
    if !binding.nodes.is_empty() || !binding.relationships.is_empty() || binding.values.len() != 1 {
        return Err(SkeinError::StorageIntegrity(
            "hash join spill record has an invalid binding shape".to_string(),
        ));
    }
    let Some(Value::Binary(payload)) = binding.values.get(HASH_JOIN_SPILL_BINDING_NAME) else {
        return Err(SkeinError::StorageIntegrity(
            "hash join spill record has no typed relational locator".to_string(),
        ));
    };
    RelationalRowSetLocator::decode_hash_spill_record(payload)
}

fn hash_join_partition_memory_budget(
    memory: &skein_executor::ExecutionMemoryConfig,
) -> NonZeroUsize {
    // A spill handoff keeps a resident build entry and one staged record live at once.
    NonZeroUsize::new(
        memory
            .blocking_operator_bytes
            .get()
            .saturating_div(2)
            .max(1),
    )
    .expect("partition memory budget is non-zero")
}

fn try_insert_hash_join_build(
    build: &mut HashMap<RelationalKey, Vec<RelationalRowSetLocator>>,
    tracker: &mut OperatorMemoryTracker,
    key: RelationalKey,
    locator: RelationalRowSetLocator,
) -> Result<bool> {
    let row_bytes = locator.memory_bytes();
    if let Some(rows) = build.get_mut(&key) {
        if tracker.would_exceed(row_bytes) {
            return Ok(false);
        }
        tracker.try_charge(row_bytes)?;
        if let Err(error) = rows.try_reserve_exact(1) {
            tracker.release(row_bytes);
            return Err(SkeinError::Execution(format!(
                "RelationalHashJoinBuild cannot reserve build row: {error}"
            )));
        }
        rows.push(locator);
        return Ok(true);
    }

    let entry_bytes = row_bytes
        .saturating_add(relational_key_resident_bytes(&key))
        .saturating_add(HASH_JOIN_MAP_ENTRY_OVERHEAD_BYTES);
    if tracker.would_exceed(entry_bytes) {
        return Ok(false);
    }
    tracker.try_charge(entry_bytes)?;
    if let Err(error) = build.try_reserve(1) {
        tracker.release(entry_bytes);
        return Err(SkeinError::Execution(format!(
            "RelationalHashJoinBuild cannot reserve hash table: {error}"
        )));
    }
    let mut rows = Vec::new();
    if let Err(error) = rows.try_reserve_exact(1) {
        tracker.release(entry_bytes);
        return Err(SkeinError::Execution(format!(
            "RelationalHashJoinBuild cannot reserve build row: {error}"
        )));
    }
    rows.push(locator);
    build.insert(key, rows);
    Ok(true)
}

fn spill_hash_join_build(
    build: &mut HashMap<RelationalKey, Vec<RelationalRowSetLocator>>,
    tracker: &mut OperatorMemoryTracker,
    spill: &mut HashJoinGraceSpill,
) -> Result<()> {
    for (key, rows) in std::mem::take(build) {
        for locator in rows {
            spill.write(HashJoinSpillSide::Build, &key, locator)?;
        }
    }
    tracker.reset();
    Ok(())
}

fn visit_hash_join_candidate<'a>(
    operator_id: RelationalOperatorId,
    predicates: &[SqlPredicate],
    output_schema: &RelationalPhysicalOutputSchema,
    parameters: &[Value],
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    left_row: &BoundRow<'a>,
    right_row: &BoundRow<'a>,
    matched: &mut bool,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    pipeline.borrow_mut().account_candidate_work()?;
    let mut combined = left_row.clone();
    combined.bindings.extend(right_row.bindings.clone());
    for predicate in predicates {
        if predicate_truth(predicate, &combined, parameters)? != Some(true) {
            return Ok(true);
        }
    }
    *matched = true;
    output_schema.ensure_matches(&combined)?;
    pipeline.borrow_mut().account_operator_row(operator_id)?;
    visit(combined)
}

fn visit_hash_join_unmatched<'a>(
    operator_id: RelationalOperatorId,
    output_schema: &RelationalPhysicalOutputSchema,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    left_row: &BoundRow<'a>,
    null_right: &BoundRow<'a>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    let mut combined = left_row.clone();
    combined.bindings.extend(null_right.bindings.clone());
    output_schema.ensure_matches(&combined)?;
    pipeline.borrow_mut().account_operator_row(operator_id)?;
    visit(combined)
}

fn relational_physical_relation_locator_layout<'a>(
    state: &'a RelationalState,
    relation: &'a RelationalPhysicalRelation,
) -> Result<RelationalLocatorLayout<'a>> {
    let schema = state.table_schema(&relation.table).ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational table {}", relation.table))
    })?;
    RelationalLocatorLayout::from_bindings([(
        relation.binding,
        relation.table.as_str(),
        relation.qualifier.as_str(),
        schema,
    )])
}

#[allow(clippy::too_many_arguments)]
fn visit_hash_join<'a>(
    operator_id: RelationalOperatorId,
    kind: SqlJoinKind,
    predicates: &[SqlPredicate],
    equi_join_keys: &RelationalEquiJoinKeys,
    left: &'a RelationalPhysicalJoinNode,
    right: &'a RelationalPhysicalJoinNode,
    output_schema: &RelationalPhysicalOutputSchema,
    outer: Option<&BoundRow<'a>>,
    parameters: &[Value],
    state: &'a RelationalState,
    profiled_base_binding: BindingId,
    execution: &RelationalPhysicalJoinExecution<'a>,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    if outer.is_some() {
        return Err(SkeinError::Execution(
            "hash join cannot run below a probe input".to_string(),
        ));
    }
    let (
        RelationalPhysicalJoinNode::Relation(left_relation),
        RelationalPhysicalJoinNode::Relation(right_relation),
    ) = (left, right)
    else {
        return Err(SkeinError::Execution(
            "hash join requires two relation inputs".to_string(),
        ));
    };
    let right_schema = state.table_schema(&right_relation.table).ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational table {}", right_relation.table))
    })?;
    let left_locator_layout = relational_physical_relation_locator_layout(state, left_relation)?;
    let right_locator_layout = relational_physical_relation_locator_layout(state, right_relation)?;
    let mut build = HashMap::<RelationalKey, Vec<RelationalRowSetLocator>>::new();
    let mut build_rows = 0usize;
    let in_memory_budget = hash_join_partition_memory_budget(execution.memory);
    let mut build_tracker = OperatorMemoryTracker::with_account(
        in_memory_budget,
        execution.memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "RelationalHashJoinBuild",
            in_memory_budget,
        ),
    );
    let mut grace: Option<HashJoinGraceSpill> = None;
    visit_prepared_physical_join_plan_node(
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
            let Some(key) = bound_relation_join_key(&row, right_relation, equi_join_keys)? else {
                return Ok(true);
            };
            let locator = typed_row_set_locator(&row)?;
            if let Some(grace) = grace.as_mut() {
                grace.write(HashJoinSpillSide::Build, &key, locator)?;
            } else if !try_insert_hash_join_build(
                &mut build,
                &mut build_tracker,
                key.clone(),
                locator.clone(),
            )? {
                let mut new_grace =
                    HashJoinGraceSpill::new(execution.memory, execution.memory_ledger);
                spill_hash_join_build(&mut build, &mut build_tracker, &mut new_grace)?;
                new_grace.write(HashJoinSpillSide::Build, &key, locator)?;
                grace = Some(new_grace);
            }
            build_rows = build_rows.saturating_add(1);
            Ok(true)
        },
    )?;

    if let Some(mut grace) = grace {
        let (fully_consumed, hot_partitions, peak_partition_bytes) = visit_grace_hash_join(
            operator_id,
            kind,
            predicates,
            equi_join_keys,
            left,
            right,
            right_relation,
            &left_locator_layout,
            &right_locator_layout,
            output_schema,
            parameters,
            state,
            profiled_base_binding,
            execution,
            pipeline,
            index_runtime,
            row_runtime,
            &mut grace,
            visit,
        )?;
        execution
            .reports
            .borrow_mut()
            .push(skein_executor::blocking::spill_backed_report(
                "RelationalHashJoinGrace",
                &build_tracker,
                build_tracker.peak_bytes.max(peak_partition_bytes),
                build_rows,
                &grace.budget,
                grace.spilled_rows,
            ));
        if hot_partitions > 0 {
            execution
                .reports
                .borrow_mut()
                .push(skein_executor::blocking::spill_backed_report(
                    "RelationalHashJoinGraceHotPartition",
                    &build_tracker,
                    build_tracker.peak_bytes.max(peak_partition_bytes),
                    build_rows,
                    &grace.budget,
                    grace.spilled_rows,
                ));
        }
        return Ok(fully_consumed);
    }

    execution
        .reports
        .borrow_mut()
        .push(skein_executor::blocking::in_memory_report(
            "RelationalHashJoinBuild",
            &build_tracker,
            build_tracker.peak_bytes,
            build_rows,
            execution.memory,
        ));
    let null_right = (kind == SqlJoinKind::Left)
        .then(|| null_extended_tree_row(right, state))
        .transpose()?;
    visit_prepared_physical_join_plan_node(
        left,
        None,
        parameters,
        state,
        profiled_base_binding,
        execution,
        pipeline,
        index_runtime,
        row_runtime,
        &mut |left_row| {
            pipeline.borrow_mut().account_candidate_work()?;
            let mut matched = false;
            if let Some(key) = bound_join_key(&left_row, right_schema, &equi_join_keys.columns)?
                && let Some(right_rows) = build.get(&key)
            {
                for right_locator in right_rows {
                    let keep_going = with_typed_locator_bound_row_for_scan(
                        right_locator,
                        &right_locator_layout,
                        row_runtime,
                        |right_row| {
                            visit_hash_join_candidate(
                                operator_id,
                                predicates,
                                output_schema,
                                parameters,
                                pipeline,
                                &left_row,
                                right_row,
                                &mut matched,
                                visit,
                            )
                        },
                    )?;
                    if !keep_going {
                        return Ok(false);
                    }
                }
            }
            if !matched && let Some(null_right) = &null_right {
                return visit_hash_join_unmatched(
                    operator_id,
                    output_schema,
                    pipeline,
                    &left_row,
                    null_right,
                    visit,
                );
            }
            Ok(true)
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn visit_grace_hash_join<'a>(
    operator_id: RelationalOperatorId,
    kind: SqlJoinKind,
    predicates: &[SqlPredicate],
    equi_join_keys: &RelationalEquiJoinKeys,
    left: &'a RelationalPhysicalJoinNode,
    right: &'a RelationalPhysicalJoinNode,
    right_relation: &'a RelationalPhysicalRelation,
    left_locator_layout: &RelationalLocatorLayout<'a>,
    right_locator_layout: &RelationalLocatorLayout<'a>,
    output_schema: &RelationalPhysicalOutputSchema,
    parameters: &[Value],
    state: &'a RelationalState,
    profiled_base_binding: BindingId,
    execution: &RelationalPhysicalJoinExecution<'a>,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    grace: &mut HashJoinGraceSpill,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<(bool, usize, usize)> {
    let right_schema = state.table_schema(&right_relation.table).ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational table {}", right_relation.table))
    })?;
    let null_right = (kind == SqlJoinKind::Left)
        .then(|| null_extended_tree_row(right, state))
        .transpose()?;
    let mut fully_consumed = true;
    visit_prepared_physical_join_plan_node(
        left,
        None,
        parameters,
        state,
        profiled_base_binding,
        execution,
        pipeline,
        index_runtime,
        row_runtime,
        &mut |left_row| {
            pipeline.borrow_mut().account_candidate_work()?;
            let Some(key) = bound_join_key(&left_row, right_schema, &equi_join_keys.columns)?
            else {
                if let Some(null_right) = &null_right {
                    fully_consumed = visit_hash_join_unmatched(
                        operator_id,
                        output_schema,
                        pipeline,
                        &left_row,
                        null_right,
                        visit,
                    )?;
                }
                return Ok(fully_consumed);
            };
            grace.write(
                HashJoinSpillSide::Probe,
                &key,
                typed_row_set_locator(&left_row)?,
            )?;
            Ok(true)
        },
    )?;
    if !fully_consumed {
        return Ok((false, 0, 0));
    }
    grace.finish()?;

    let mut hot_partitions = 0usize;
    let mut peak_partition_bytes = 0usize;
    let partition_memory = hash_join_partition_memory_budget(execution.memory);
    let max_record_bytes = partition_memory.get();
    for partition in 0..HASH_JOIN_GRACE_PARTITIONS {
        let Some(probe_run) = grace.run(HashJoinSpillSide::Probe, partition) else {
            continue;
        };
        let Some(build_run) = grace.run(HashJoinSpillSide::Build, partition) else {
            let mut probe_reader = probe_run.run.reader()?;
            let mut replay_tracker = OperatorMemoryTracker::with_account(
                partition_memory,
                execution.memory_ledger.account(
                    QueryMemoryClass::BlockingState,
                    "RelationalHashJoinGraceReplay",
                    partition_memory,
                ),
            );
            while let Some(keep_going) = map_hash_join_spill_record(
                &mut probe_reader,
                max_record_bytes,
                &grace.budget,
                &mut replay_tracker,
                |locator| {
                    with_typed_locator_bound_row_for_scan(
                        &locator,
                        left_locator_layout,
                        row_runtime,
                        |left_row| {
                            if let Some(null_right) = &null_right {
                                visit_hash_join_unmatched(
                                    operator_id,
                                    output_schema,
                                    pipeline,
                                    left_row,
                                    null_right,
                                    visit,
                                )
                            } else {
                                Ok(true)
                            }
                        },
                    )
                },
            )? {
                fully_consumed = keep_going;
                if !fully_consumed {
                    return Ok((false, hot_partitions, peak_partition_bytes));
                }
            }
            peak_partition_bytes = peak_partition_bytes.max(replay_tracker.peak_bytes);
            continue;
        };

        let mut partition_build = HashMap::<RelationalKey, Vec<RelationalRowSetLocator>>::new();
        let mut partition_tracker = OperatorMemoryTracker::with_account(
            partition_memory,
            execution.memory_ledger.account(
                QueryMemoryClass::BlockingState,
                "RelationalHashJoinGracePartition",
                partition_memory,
            ),
        );
        let mut build_replay_tracker = OperatorMemoryTracker::with_account(
            partition_memory,
            execution.memory_ledger.account(
                QueryMemoryClass::BlockingState,
                "RelationalHashJoinGraceBuildReplay",
                partition_memory,
            ),
        );
        let mut build_reader = build_run.run.reader()?;
        let mut hot = false;
        while let Some(inserted) = map_hash_join_spill_record(
            &mut build_reader,
            max_record_bytes,
            &grace.budget,
            &mut build_replay_tracker,
            |locator| {
                let key = with_typed_locator_bound_row_for_scan(
                    &locator,
                    right_locator_layout,
                    row_runtime,
                    |right_row| {
                        bound_relation_join_key(right_row, right_relation, equi_join_keys)?
                            .ok_or_else(|| {
                                SkeinError::StorageIntegrity(
                                    "hash join spill build row has a null join key".to_string(),
                                )
                            })
                    },
                )?;
                try_insert_hash_join_build(
                    &mut partition_build,
                    &mut partition_tracker,
                    key,
                    locator,
                )
            },
        )? {
            if !inserted {
                hot = true;
                break;
            }
        }
        if hot {
            hot_partitions = hot_partitions.saturating_add(1);
            partition_build = HashMap::new();
            partition_tracker.reset();
        }
        let mut probe_replay_tracker = OperatorMemoryTracker::with_account(
            partition_memory,
            execution.memory_ledger.account(
                QueryMemoryClass::BlockingState,
                "RelationalHashJoinGraceProbeReplay",
                partition_memory,
            ),
        );
        let mut hot_replay_tracker = hot.then(|| {
            OperatorMemoryTracker::with_account(
                partition_memory,
                execution.memory_ledger.account(
                    QueryMemoryClass::BlockingState,
                    "RelationalHashJoinGraceHotReplay",
                    partition_memory,
                ),
            )
        });

        let mut probe_reader = probe_run.run.reader()?;
        while let Some(keep_going) = map_hash_join_spill_record(
            &mut probe_reader,
            max_record_bytes,
            &grace.budget,
            &mut probe_replay_tracker,
            |locator| {
                with_typed_locator_bound_row_for_scan(
                    &locator,
                    left_locator_layout,
                    row_runtime,
                    |left_row| {
                        let left_key =
                            bound_join_key(left_row, right_schema, &equi_join_keys.columns)?
                                .ok_or_else(|| {
                                    SkeinError::StorageIntegrity(
                                        "hash join spill probe row has a null join key".to_string(),
                                    )
                                })?;
                        let mut matched = false;
                        if hot {
                            let mut hot_build_reader = build_run.run.reader()?;
                            while let Some(keep_going) = map_hash_join_spill_record(
                                &mut hot_build_reader,
                                max_record_bytes,
                                &grace.budget,
                                hot_replay_tracker
                                    .as_mut()
                                    .expect("hot partition has a replay tracker"),
                                |build_locator| {
                                    with_typed_locator_bound_row_for_scan(
                                        &build_locator,
                                        right_locator_layout,
                                        row_runtime,
                                        |right_row| {
                                            let Some(right_key) = bound_relation_join_key(
                                                right_row,
                                                right_relation,
                                                equi_join_keys,
                                            )?
                                            else {
                                                return Err(SkeinError::StorageIntegrity(
                                                    "hash join spill build row has a null join key"
                                                        .to_string(),
                                                ));
                                            };
                                            if right_key != left_key {
                                                return Ok(true);
                                            }
                                            visit_hash_join_candidate(
                                                operator_id,
                                                predicates,
                                                output_schema,
                                                parameters,
                                                pipeline,
                                                left_row,
                                                right_row,
                                                &mut matched,
                                                visit,
                                            )
                                        },
                                    )
                                },
                            )? {
                                if !keep_going {
                                    return Ok(false);
                                }
                            }
                        } else if let Some(right_locators) = partition_build.get(&left_key) {
                            for right_locator in right_locators {
                                let keep_going = with_typed_locator_bound_row_for_scan(
                                    right_locator,
                                    right_locator_layout,
                                    row_runtime,
                                    |right_row| {
                                        visit_hash_join_candidate(
                                            operator_id,
                                            predicates,
                                            output_schema,
                                            parameters,
                                            pipeline,
                                            left_row,
                                            right_row,
                                            &mut matched,
                                            visit,
                                        )
                                    },
                                )?;
                                if !keep_going {
                                    return Ok(false);
                                }
                            }
                        }
                        if !matched && let Some(null_right) = &null_right {
                            return visit_hash_join_unmatched(
                                operator_id,
                                output_schema,
                                pipeline,
                                left_row,
                                null_right,
                                visit,
                            );
                        }
                        Ok(true)
                    },
                )
            },
        )? {
            fully_consumed = keep_going;
            if !fully_consumed {
                return Ok((false, hot_partitions, peak_partition_bytes));
            }
        }
        peak_partition_bytes = peak_partition_bytes.max(partition_tracker.peak_bytes);
        peak_partition_bytes = peak_partition_bytes.max(build_replay_tracker.peak_bytes);
        peak_partition_bytes = peak_partition_bytes.max(probe_replay_tracker.peak_bytes);
        peak_partition_bytes = peak_partition_bytes.max(
            hot_replay_tracker
                .as_ref()
                .map_or(0, |tracker| tracker.peak_bytes),
        );
    }
    Ok((true, hot_partitions, peak_partition_bytes))
}

#[allow(clippy::too_many_arguments)]
fn visit_prepared_physical_join_plan_node<'a>(
    node: &'a RelationalPhysicalJoinNode,
    outer: Option<&BoundRow<'a>>,
    parameters: &[Value],
    state: &'a RelationalState,
    profiled_base_binding: BindingId,
    execution: &RelationalPhysicalJoinExecution<'a>,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    match node {
        RelationalPhysicalJoinNode::Relation(relation) => visit_tree_relation_entries(
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
                } else if matches!(relation.access, RelationalPhysicalAccess::Base(_)) {
                    pipeline.borrow_mut().account_unprofiled_row()?;
                }
                let schema = state.table_schema(&relation.table).ok_or_else(|| {
                    SkeinError::Semantic(format!("unknown relational table {}", relation.table))
                })?;
                let bound = BoundRow {
                    bindings: vec![Binding {
                        binding: relation.binding,
                        table: &relation.table,
                        qualifier: &relation.qualifier,
                        schema,
                        row: Some(row),
                    }],
                };
                relation.output_schema.ensure_matches(&bound)?;
                visit(bound)
            },
        ),
        RelationalPhysicalJoinNode::Join {
            operator_id,
            kind,
            algorithm,
            equi_join_keys,
            predicates,
            left,
            right,
            output_schema,
            ..
        } => {
            if *algorithm == RelationalPhysicalJoinAlgorithm::Merge {
                let equi_join_keys = equi_join_keys.as_ref().ok_or_else(|| {
                    SkeinError::Execution("merge join has no key contract".to_string())
                })?;
                return visit_index_merge_join(
                    *operator_id,
                    predicates,
                    equi_join_keys,
                    left,
                    right,
                    output_schema,
                    outer,
                    parameters,
                    state,
                    profiled_base_binding,
                    execution,
                    pipeline,
                    index_runtime,
                    row_runtime,
                    visit,
                );
            }
            if *algorithm == RelationalPhysicalJoinAlgorithm::Hash {
                let equi_join_keys = equi_join_keys.as_ref().ok_or_else(|| {
                    SkeinError::Execution("hash join has no key contract".to_string())
                })?;
                return visit_hash_join(
                    *operator_id,
                    *kind,
                    predicates,
                    equi_join_keys,
                    left,
                    right,
                    output_schema,
                    outer,
                    parameters,
                    state,
                    profiled_base_binding,
                    execution,
                    pipeline,
                    index_runtime,
                    row_runtime,
                    visit,
                );
            }
            if *algorithm == RelationalPhysicalJoinAlgorithm::BatchedIndex {
                return visit_batched_index_nested_loop(
                    *operator_id,
                    *kind,
                    predicates,
                    left,
                    right,
                    output_schema,
                    outer,
                    parameters,
                    state,
                    profiled_base_binding,
                    execution,
                    pipeline,
                    index_runtime,
                    row_runtime,
                    visit,
                );
            }
            let materialized_right = *algorithm == RelationalPhysicalJoinAlgorithm::Materialized;
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
                visit_prepared_physical_join_plan_node(
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
            visit_prepared_physical_join_plan_node(
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
                        pipeline.borrow_mut().account_candidate_work()?;
                        let mut combined = left_row.clone();
                        combined.bindings.extend(right_row.bindings);
                        for predicate in predicates {
                            if predicate_truth(predicate, &combined, parameters)? != Some(true) {
                                return Ok(true);
                            }
                        }
                        output_schema.ensure_matches(&combined)?;
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
                        pipeline.borrow_mut().account_candidate_work()?;
                        visit_prepared_physical_join_plan_node(
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
                        output_schema.ensure_matches(&combined)?;
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
    tree_execution: Option<&RelationalPhysicalJoinExecution<'a>>,
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
            visit_prepared_physical_join_plan_node(
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
        None,
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
                        binding: BindingId::new(0),
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
    pipeline.account_candidate_work()?;
    let completed = visit_join_entries(
        state,
        index_runtime,
        row_runtime,
        planned,
        &row,
        &mut |candidate| {
            pipeline.account_candidate_work()?;
            let mut combined = row.clone();
            combined.bindings.push(Binding {
                binding: planned.binding,
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
            binding: planned.binding,
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
    tree_execution: Option<&'pipeline RelationalPhysicalJoinExecution<'a>>,
    pipeline: &'pipeline mut RelationalPipelineState<'a>,
    index_runtime: &'pipeline RelationalIndexRuntime<'a>,
    row_runtime: &'pipeline RelationalRowRuntime<'a>,
    batch_rows: usize,
    batch_memory: NonZeroUsize,
    memory_ledger: &'pipeline QueryMemoryLedger,
    add_order_keys: bool,
}

struct DistinctAggregateValueBatchSource<'a, 'pipeline> {
    select: &'a SelectStatement,
    column: &'a SqlColumnRef,
    filter: Option<&'a SqlPredicate>,
    parameters: &'a [Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &'a RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    tree_execution: Option<&'pipeline RelationalPhysicalJoinExecution<'a>>,
    pipeline: &'pipeline mut RelationalPipelineState<'a>,
    index_runtime: &'pipeline RelationalIndexRuntime<'a>,
    row_runtime: &'pipeline RelationalRowRuntime<'a>,
    batch_rows: usize,
    batch_memory: NonZeroUsize,
    memory_ledger: &'pipeline QueryMemoryLedger,
}

impl BindingBatchSource for DistinctAggregateValueBatchSource<'_, '_> {
    fn execute(
        &mut self,
        _input: &PhysicalPlan,
        _execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let mut batch = AccountedBindingBatch::with_ledger(
            "RelationalCountDistinctExec input",
            self.batch_rows,
            self.batch_memory,
            self.memory_ledger,
        );
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
                if !aggregate_filter_matches(self.filter, &row, self.parameters)? {
                    return Ok(true);
                }
                let value = resolve_column(&row, self.column)?;
                if matches!(value, RelationalValue::Null) {
                    return Ok(true);
                }
                control = batch.push(
                    ExecutorBinding::scalar("value", relational_sort_value(value)?),
                    emit,
                )?;
                if control == BatchControl::Continue && batch.is_full() {
                    control = batch.emit(emit)?;
                }
                Ok(control == BatchControl::Continue)
            },
        )?;
        if control == BatchControl::Continue && !batch.is_empty() {
            control = batch.emit(emit)?;
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
        let mut batch = AccountedBindingBatch::with_ledger(
            "RelationalDistinctProjectionExec input",
            self.batch_rows,
            self.batch_memory,
            self.memory_ledger,
        );
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
                let mut projected = project_bound_row(&row, &self.select.projection)?;
                if self.add_order_keys {
                    add_relational_order_keys(self.select, &row, &mut projected)?;
                }
                control = batch.push(ExecutorBinding::values(projected), emit)?;
                if control == BatchControl::Continue && batch.is_full() {
                    control = batch.emit(emit)?;
                }
                Ok(control == BatchControl::Continue)
            },
        )?;
        if control == BatchControl::Continue && !batch.is_empty() {
            control = batch.emit(emit)?;
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
    tree_execution: Option<&RelationalPhysicalJoinExecution<'a>>,
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
            batch_memory: memory.batch_payload_bytes,
            memory_ledger,
            add_order_keys: false,
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
    } else if order_by_uses_expression_alias(select)? {
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
            batch_memory: memory.batch_payload_bytes,
            memory_ledger,
            add_order_keys: true,
        };
        execute_relational_order(
            &input_plan,
            &mut projected,
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
    } else {
        let locator_layout = match tree_execution {
            Some(execution) => relational_physical_join_plan_locator_layout(state, execution.tree)?,
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
                        let column = match resolve_relational_order_target(select, item)? {
                            RelationalOrderTarget::InputColumn(column) => column,
                            RelationalOrderTarget::ProjectionColumn { column, .. } => column,
                            RelationalOrderTarget::ProjectionExpression { .. } => unreachable!(
                                "expression ORDER BY aliases use projected batch sorting"
                            ),
                        };
                        RelationalSortKey::new(
                            resolve_column(&row, column)?.clone(),
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

fn order_by_uses_expression_alias(select: &SelectStatement) -> Result<bool> {
    select.order_by.iter().try_fold(false, |found, item| {
        Ok(found
            || matches!(
                resolve_relational_order_target(select, item)?,
                RelationalOrderTarget::ProjectionExpression { .. }
            ))
    })
}

fn add_relational_order_keys(
    select: &SelectStatement,
    row: &BoundRow<'_>,
    projected: &mut Row,
) -> Result<()> {
    for (ordinal, item) in select.order_by.iter().enumerate() {
        let value = match resolve_relational_order_target(select, item)? {
            RelationalOrderTarget::InputColumn(column) => {
                relational_sort_value(resolve_column(row, column)?)?
            }
            RelationalOrderTarget::ProjectionColumn { alias, .. }
            | RelationalOrderTarget::ProjectionExpression { alias, .. } => {
                projected.get(alias).cloned().ok_or_else(|| {
                    SkeinError::Semantic(format!(
                        "relational ORDER BY alias {alias} is not projected"
                    ))
                })?
            }
        };
        projected.insert(
            relational_sort_column(ordinal),
            postgres_sort_key(value, item.direction, item.nulls),
        );
    }
    Ok(())
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
        RelationalValue::Uuid(value) => Ok(Value::Uuid(*value)),
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
            let output =
                match resolve_relational_order_target(select, item)? {
                    RelationalOrderTarget::ProjectionColumn { alias, .. }
                    | RelationalOrderTarget::ProjectionExpression { alias, .. } => {
                        Some(alias.to_string())
                    }
                    RelationalOrderTarget::InputColumn(column) => select
                        .projection
                        .iter()
                        .find_map(|projection| match projection {
                            SelectProjection::Wildcard => Some(column.name.clone()),
                            SelectProjection::Column { name, alias }
                                if name.name == column.name
                                    && column.qualifier.as_deref().is_none_or(|qualifier| {
                                        name.qualifier.as_deref() == Some(qualifier)
                                            || select.from.name == qualifier
                                            || select.from_alias.as_deref() == Some(qualifier)
                                    }) =>
                            {
                                Some(alias.clone().unwrap_or_else(|| name.name.clone()))
                            }
                            SelectProjection::Column { .. }
                            | SelectProjection::Expression { .. } => None,
                        }),
                };
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

fn with_typed_locator_bound_row<'a, T>(
    locator: &RelationalRowSetLocator,
    locator_layout: &RelationalLocatorLayout<'a>,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: impl FnOnce(&BoundRow<'a>) -> Result<T>,
) -> Result<T> {
    with_typed_locator_bound_row_mode(locator, locator_layout, row_runtime, true, visit)
}

fn with_typed_locator_bound_row_for_scan<'a, T>(
    locator: &RelationalRowSetLocator,
    locator_layout: &RelationalLocatorLayout<'a>,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: impl FnOnce(&BoundRow<'a>) -> Result<T>,
) -> Result<T> {
    with_typed_locator_bound_row_mode(locator, locator_layout, row_runtime, false, visit)
}

fn with_typed_locator_bound_row_mode<'a, T>(
    locator: &RelationalRowSetLocator,
    locator_layout: &RelationalLocatorLayout<'a>,
    row_runtime: &RelationalRowRuntime<'a>,
    output_fields: bool,
    visit: impl FnOnce(&BoundRow<'a>) -> Result<T>,
) -> Result<T> {
    locator_layout.validate(locator)?;
    let mut bound = BoundRow {
        bindings: Vec::with_capacity(locator.rows().len()),
    };
    for (binding, locator) in locator_layout.bindings.iter().zip(locator.rows()) {
        let row = match locator {
            Some(locator) => (if output_fields {
                row_runtime.read_output_point(binding.table, locator.primary_key())?
            } else {
                row_runtime.read_point(binding.table, locator.primary_key())?
            })
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
            binding: binding.binding,
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
                    binding: BindingId::new(0),
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
    tree_execution: Option<&RelationalPhysicalJoinExecution<'a>>,
    pipeline: &mut RelationalPipelineState<'_>,
    index_runtime: &RelationalIndexRuntime<'a>,
    row_runtime: &RelationalRowRuntime<'a>,
    limits: RelationalQueryLimits,
) -> Result<StreamingProjectionOutput> {
    if tree_execution.is_none_or(|execution| execution.tree.root.relation_count() == 1)
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
    tree_execution: Option<&RelationalPhysicalJoinExecution<'a>>,
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
    if let Some((column, output_name, filter)) = single_count_distinct_column(select) {
        return execute_single_count_distinct(
            select,
            column,
            output_name,
            filter,
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
                let delta = projection.update(&row, parameters)?;
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

fn single_count_distinct_column(
    select: &SelectStatement,
) -> Option<(&SqlColumnRef, String, Option<&SqlPredicate>)> {
    let [SelectProjection::Expression { expression, alias }] = select.projection.as_slice() else {
        return None;
    };
    let SqlExpression::Function {
        name,
        arguments,
        distinct: true,
        filter,
    } = expression
    else {
        return None;
    };
    let [SqlFunctionArgument::Expression(SqlExpression::Column(column))] = arguments.as_slice()
    else {
        return None;
    };
    (name == "count" && select.group_by.is_empty() && select.order_by.is_empty()).then(|| {
        (
            column,
            alias.clone().unwrap_or_else(|| "count".to_string()),
            filter.as_ref(),
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_single_count_distinct<'a>(
    select: &'a SelectStatement,
    column: &'a SqlColumnRef,
    output_name: String,
    filter: Option<&'a SqlPredicate>,
    parameters: &'a [Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &'a RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    tree_execution: Option<&RelationalPhysicalJoinExecution<'a>>,
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
        filter,
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
        batch_memory: execution_memory.batch_payload_bytes,
        memory_ledger,
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
    tree_execution: Option<&RelationalPhysicalJoinExecution<'a>>,
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
        Some(execution) => relational_physical_join_plan_locator_layout(state, execution.tree)?,
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
                    let delta = projection.update(row, parameters)?;
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

    fn update(&mut self, row: &BoundRow<'_>, parameters: &[Value]) -> Result<AggregateMemoryDelta> {
        self.expression.update(row, parameters)
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
        filter: Option<SqlPredicate>,
        count: usize,
        distinct: Option<BTreeSet<RelationalValue>>,
    },
    Numeric {
        expression: SqlExpression,
        filter: Option<SqlPredicate>,
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
                filter,
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
                        filter: filter.clone(),
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
                        filter: filter.clone(),
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

    fn update(&mut self, row: &BoundRow<'_>, parameters: &[Value]) -> Result<AggregateMemoryDelta> {
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
                filter,
                count,
                distinct,
            } => {
                if !aggregate_filter_matches(filter.as_ref(), row, parameters)? {
                    return Ok(AggregateMemoryDelta::default());
                }
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
                filter,
                aggregate,
                value,
                distinct,
            } => {
                if !aggregate_filter_matches(filter.as_ref(), row, parameters)? {
                    return Ok(AggregateMemoryDelta::default());
                }
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
                    delta.combine(state.update(row, parameters)?);
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
            column,
            filter,
            distinct,
            ..
        } => column
            .as_ref()
            .map_or(0, column_ref_memory_bytes)
            .saturating_add(filter.as_ref().map_or(0, sql_predicate_memory_bytes))
            .saturating_add(
                distinct
                    .as_ref()
                    .map_or(0, |_| std::mem::size_of::<BTreeSet<RelationalValue>>()),
            ),
        AggregateExpressionState::Numeric {
            expression,
            filter,
            value,
            distinct,
            ..
        } => sql_expression_memory_bytes(expression)
            .saturating_add(filter.as_ref().map_or(0, sql_predicate_memory_bytes))
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
            name,
            arguments,
            filter,
            ..
        } => arguments
            .iter()
            .fold(
                name.len()
                    .saturating_add(std::mem::size_of::<Vec<SqlFunctionArgument>>()),
                |total, argument| match argument {
                    SqlFunctionArgument::Expression(expression) => {
                        total.saturating_add(sql_expression_memory_bytes(expression))
                    }
                    SqlFunctionArgument::Wildcard => total,
                },
            )
            .saturating_add(filter.as_ref().map_or(0, sql_predicate_memory_bytes)),
    })
}

fn sql_predicate_memory_bytes(predicate: &SqlPredicate) -> usize {
    std::mem::size_of::<SqlPredicate>().saturating_add(match predicate {
        SqlPredicate::And(left, right) | SqlPredicate::Or(left, right) => {
            sql_predicate_memory_bytes(left).saturating_add(sql_predicate_memory_bytes(right))
        }
        SqlPredicate::Not(predicate) => sql_predicate_memory_bytes(predicate),
        SqlPredicate::Compare { left, right, .. } => {
            column_ref_memory_bytes(left).saturating_add(sql_value_memory_bytes(right))
        }
        SqlPredicate::CompareColumns { left, right, .. } => {
            column_ref_memory_bytes(left).saturating_add(column_ref_memory_bytes(right))
        }
        SqlPredicate::InList { left, values, .. } => values.iter().fold(
            column_ref_memory_bytes(left).saturating_add(std::mem::size_of::<Vec<SqlValue>>()),
            |total, value| total.saturating_add(sql_value_memory_bytes(value)),
        ),
        SqlPredicate::Like { left, pattern, .. } => {
            column_ref_memory_bytes(left).saturating_add(sql_value_memory_bytes(pattern))
        }
        SqlPredicate::IsNull { column, .. } => column_ref_memory_bytes(column),
    })
}

fn sql_value_memory_bytes(value: &SqlValue) -> usize {
    match value {
        SqlValue::Literal(value) => skein_executor::binding::value_memory_bytes(value),
        SqlValue::Parameter(_) => 0,
    }
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
            filter: None,
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

fn aggregate_filter_matches(
    filter: Option<&SqlPredicate>,
    row: &BoundRow<'_>,
    parameters: &[Value],
) -> Result<bool> {
    match filter {
        Some(filter) => Ok(predicate_truth(filter, row, parameters)? == Some(true)),
        None => Ok(true),
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
        RelationalValueRef::Uuid(value) => Ok(Value::Uuid(value)),
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
        SqlPredicate::Compare { left, op, right } => {
            let (left_value, scalar_type) = resolve_column_with_type(row, left)?;
            compare_values(
                left_value,
                &value_to_relational_as(bind_sql_value(right, parameters)?, scalar_type)?,
                *op,
            )
        }
        SqlPredicate::CompareColumns { left, op, right } => {
            compare_values(resolve_column(row, left)?, resolve_column(row, right)?, *op)
        }
        SqlPredicate::InList {
            left,
            values,
            negated,
        } => {
            let (left, scalar_type) = resolve_column_with_type(row, left)?;
            let mut has_unknown = false;
            let mut matched = false;
            for value in values {
                match compare_values(
                    left,
                    &value_to_relational_as(bind_sql_value(value, parameters)?, scalar_type)?,
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
        SqlPredicate::Like {
            left,
            pattern,
            case_insensitive,
            negated,
            escape,
        } => {
            let (left, scalar_type) = resolve_column_with_type(row, left)?;
            if scalar_type != RelationalScalarType::Text {
                return Err(SkeinError::Semantic(
                    "LIKE and ILIKE require a TEXT column".to_string(),
                ));
            }
            let pattern = value_to_relational_as(
                bind_sql_value(pattern, parameters)?,
                RelationalScalarType::Text,
            )?;
            match (left, pattern) {
                (RelationalValue::Null, _) | (_, RelationalValue::Null) => Ok(None),
                (RelationalValue::Text(value), RelationalValue::Text(pattern)) => {
                    let matched =
                        skein_sql::sql_like_matches(value, &pattern, *escape, *case_insensitive)?;
                    Ok(Some(matched != *negated))
                }
                (RelationalValue::Overflow(_), _) => Err(SkeinError::Execution(
                    "LIKE reached an overflow value without hydration".to_string(),
                )),
                _ => Err(SkeinError::Semantic(
                    "LIKE and ILIKE require TEXT values".to_string(),
                )),
            }
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
    resolve_column_with_type(row, column).map(|(value, _)| value)
}

fn resolve_column_with_type<'a>(
    row: &'a BoundRow<'a>,
    column: &SqlColumnRef,
) -> Result<(&'a RelationalValue, RelationalScalarType)> {
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
    Ok((
        binding.value(position)?,
        binding.schema.columns[position].scalar_type,
    ))
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
            SelectProjection::Expression { expression, alias } => insert_output(
                &mut output,
                alias.clone().unwrap_or_else(|| expression_name(expression)),
                relational_to_value(&evaluate_projection_expression(expression, row)?)?,
            )?,
        }
    }
    Ok(output)
}

fn evaluate_projection_expression(
    expression: &SqlExpression,
    row: &BoundRow<'_>,
) -> Result<RelationalValue> {
    match expression {
        SqlExpression::Column(column) => Ok(resolve_column(row, column)?.clone()),
        SqlExpression::Value(SqlValue::Literal(value)) => value_to_relational(value.clone()),
        SqlExpression::Value(SqlValue::Parameter(position)) => Err(SkeinError::Semantic(format!(
            "projection expression cannot bind parameter ${position}"
        ))),
        SqlExpression::Function {
            name,
            arguments,
            distinct: false,
            filter: None,
        } if name == "uuidv7" && arguments.is_empty() => {
            Ok(RelationalValue::Uuid(skein_core::generate_uuidv7()?))
        }
        SqlExpression::Function { name, .. } => Err(SkeinError::Semantic(format!(
            "unsupported relational projection function {name}"
        ))),
    }
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
        Value::Uuid(value) => Ok(RelationalValue::Uuid(value)),
        Value::List(_) | Value::Map(_) => Err(SkeinError::Semantic(
            "relational SQL values must be scalar".to_string(),
        )),
    }
}

fn value_to_relational_as(
    value: Value,
    scalar_type: RelationalScalarType,
) -> Result<RelationalValue> {
    super::coerce_relational_value(value_to_relational(value)?, scalar_type)
}

fn relational_to_value(value: &RelationalValue) -> Result<Value> {
    match value {
        RelationalValue::Null => Ok(Value::Null),
        RelationalValue::Boolean(value) => Ok(Value::Bool(*value)),
        RelationalValue::BigInt(value) => Ok(Value::Int(*value)),
        RelationalValue::DoublePrecision(value) => Ok(Value::Float(*value)),
        RelationalValue::Text(value) => Ok(Value::String(value.clone())),
        RelationalValue::Bytea(value) => Ok(Value::Binary(value.clone())),
        RelationalValue::Uuid(value) => Ok(Value::Uuid(*value)),
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
    use std::num::{NonZeroU64, NonZeroUsize};

    #[test]
    fn candidate_work_has_an_independent_budget_and_checkpoint() {
        let limits = RelationalQueryLimits {
            max_output_rows: 1,
            max_output_payload_bytes: 1,
            max_intermediate_rows: 1,
            max_candidate_work: 1,
            hydration: RelationalHydrationBudget::default(),
            index_read: skein_storage::RelationalIndexReadLimits::default(),
            row_read: skein_storage::RelationalRowPageSnapshotReadLimits::default(),
        };
        let cancellation = skein_core::RuntimeCancellationToken::new();
        let task_context = skein_core::RuntimeTaskContext::without_deadline(cancellation.clone());
        let mut pipeline = RelationalPipelineState::new(
            Some(&task_context),
            limits,
            NonZeroUsize::MIN,
            Vec::new(),
        );

        pipeline
            .account_candidate_work()
            .expect("first candidate fits");
        let error = pipeline
            .account_candidate_work()
            .expect_err("second candidate exceeds its separate budget");
        assert!(error.to_string().contains("max_candidate_work 1"));

        let mut cancellable = RelationalPipelineState::new(
            Some(&task_context),
            RelationalQueryLimits {
                max_candidate_work: 2,
                ..limits
            },
            NonZeroUsize::MIN,
            Vec::new(),
        );
        cancellation.cancel();
        let error = cancellable
            .account_candidate_work()
            .expect_err("candidate work must reach a runtime checkpoint");
        assert!(error
            .to_string()
            .contains("runtime task stopped: cancelled"));
    }

    fn batched_index_join_state() -> RelationalState {
        let mut state = RelationalState::default();
        for sql in [
            "CREATE TABLE batch_outer (id TEXT PRIMARY KEY, join_key TEXT)",
            "CREATE TABLE batch_inner (id TEXT PRIMARY KEY, join_key TEXT NOT NULL, value TEXT NOT NULL)",
            "CREATE INDEX idx_batch_inner_join_key ON batch_inner (join_key)",
            "INSERT INTO batch_outer (id, join_key) VALUES ('outer-1', 'shared'), ('outer-2', 'shared'), ('outer-3', 'solo'), ('outer-4', NULL)",
            "INSERT INTO batch_inner (id, join_key, value) VALUES ('inner-1', 'shared', 'first'), ('inner-2', 'shared', 'second'), ('inner-3', 'solo', 'only')",
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
        state
    }

    fn batched_index_join_limits() -> RelationalQueryLimits {
        RelationalQueryLimits {
            max_output_rows: 16,
            max_output_payload_bytes: 64 * 1024,
            max_intermediate_rows: 128,
            max_candidate_work: 128,
            hydration: RelationalHydrationBudget::default(),
            index_read: skein_storage::RelationalIndexReadLimits::default(),
            row_read: skein_storage::RelationalRowPageSnapshotReadLimits::default(),
        }
    }

    fn prepare_batched_index_join(state: &RelationalState) -> PreparedRelationalSelect {
        let prepared_sql = skein_sql::prepare_postgres_sql(
            "SELECT o.id AS outer_id, i.id AS inner_id \
             FROM batch_outer AS o \
             LEFT JOIN batch_inner AS i \
             ON i.join_key = o.join_key AND o.id <> 'outer-2'",
        )
        .expect("valid batched index join SELECT");
        let SqlStatement::Select(select) = prepared_sql.statement else {
            panic!("expected SELECT statement");
        };
        prepare_relational_select(
            select,
            &[],
            state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            batched_index_join_limits(),
            RelationalJoinPlanningContext::default(),
            RelationalSqlStageTimings::default(),
        )
        .expect("prepare batched index join")
    }

    fn merge_join_state() -> RelationalState {
        let mut state = RelationalState::default();
        for sql in [
            "CREATE TABLE merge_left (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, join_key TEXT NOT NULL)",
            "CREATE TABLE merge_right (id TEXT PRIMARY KEY, join_key TEXT NOT NULL, value TEXT NOT NULL)",
            "CREATE INDEX idx_merge_left_tenant_key ON merge_left (tenant, join_key)",
            "CREATE INDEX idx_merge_right_key ON merge_right (join_key)",
            "INSERT INTO merge_left (id, tenant, join_key) VALUES ('left-b', 'tenant-1', 'b'), ('left-a', 'tenant-1', 'a'), ('left-other', 'tenant-2', 'a')",
            "INSERT INTO merge_right (id, join_key, value) VALUES ('right-a', 'a', 'first'), ('right-b-1', 'b', 'second'), ('right-b-2', 'b', 'third')",
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
        state
    }

    fn prepare_merge_join(state: &RelationalState) -> PreparedRelationalSelect {
        prepare_merge_join_with_index_read_mode(state, RelationalIndexReadMode::Materialized)
    }

    fn prepare_merge_join_with_index_read_mode(
        state: &RelationalState,
        index_read_mode: RelationalIndexReadMode<'_>,
    ) -> PreparedRelationalSelect {
        let prepared_sql = skein_sql::prepare_postgres_sql(
            "SELECT l.id AS left_id, r.id AS right_id \
             FROM merge_left AS l \
             INNER JOIN merge_right AS r ON r.join_key = l.join_key \
             WHERE l.tenant = 'tenant-1'",
        )
        .expect("valid merge join SELECT");
        let SqlStatement::Select(select) = prepared_sql.statement else {
            panic!("expected SELECT statement");
        };
        prepare_relational_select(
            select,
            &[],
            state,
            RelationalQueryReadModes::new(index_read_mode, RelationalRowReadMode::CanonicalMemory),
            batched_index_join_limits(),
            RelationalJoinPlanningContext::new(
                RelationalJoinEnumerationConfig::default(),
                RelationalJoinPlanningDirective::SyntaxOrder,
            ),
            RelationalSqlStageTimings::default(),
        )
        .expect("prepare merge join")
    }

    fn hash_join_state() -> RelationalState {
        let mut state = RelationalState::default();
        for sql in [
            "CREATE TABLE hash_left (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, join_key TEXT, tag TEXT NOT NULL)",
            "CREATE TABLE hash_right (id TEXT PRIMARY KEY, join_key TEXT, tag TEXT NOT NULL, value TEXT NOT NULL)",
            "CREATE INDEX idx_hash_left_tenant ON hash_left (tenant)",
            "INSERT INTO hash_left (id, tenant, join_key, tag) VALUES ('left-b', 'tenant-1', 'b', 'gold'), ('left-a', 'tenant-1', 'a', 'silver'), ('left-null', 'tenant-1', NULL, 'silver'), ('left-other', 'tenant-2', 'a', 'silver')",
            "INSERT INTO hash_right (id, join_key, tag, value) VALUES ('right-a', 'a', 'silver', 'keep'), ('right-b-1', 'b', 'gold', 'keep'), ('right-b-2', 'b', 'gold', 'keep'), ('right-b-skip', 'b', 'gold', 'skip'), ('right-null', NULL, 'silver', 'keep')",
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
        state
    }

    fn prepare_hash_join(state: &RelationalState) -> PreparedRelationalSelect {
        let prepared_sql = skein_sql::prepare_postgres_sql(
            "SELECT l.id AS left_id, r.id AS right_id \
             FROM hash_left AS l \
             INNER JOIN hash_right AS r \
             ON r.join_key = l.join_key AND r.tag = l.tag AND r.value = 'keep' \
             WHERE l.tenant = 'tenant-1'",
        )
        .expect("valid hash join SELECT");
        let SqlStatement::Select(select) = prepared_sql.statement else {
            panic!("expected SELECT statement");
        };
        prepare_relational_select(
            select,
            &[],
            state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            batched_index_join_limits(),
            RelationalJoinPlanningContext::default(),
            RelationalSqlStageTimings::default(),
        )
        .expect("prepare hash join")
    }

    fn prepare_hash_left_join(state: &RelationalState) -> PreparedRelationalSelect {
        let prepared_sql = skein_sql::prepare_postgres_sql(
            "SELECT l.id AS left_id, r.id AS right_id \
             FROM hash_left AS l \
             LEFT JOIN hash_right AS r \
             ON r.join_key = l.join_key AND r.tag = l.tag AND r.value = 'keep' \
             WHERE l.tenant = 'tenant-1'",
        )
        .expect("valid hash left join SELECT");
        let SqlStatement::Select(select) = prepared_sql.statement else {
            panic!("expected SELECT statement");
        };
        prepare_relational_select(
            select,
            &[],
            state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            batched_index_join_limits(),
            RelationalJoinPlanningContext::default(),
            RelationalSqlStageTimings::default(),
        )
        .expect("prepare hash left join")
    }

    fn constrained_hash_join_memory() -> skein_executor::ExecutionMemoryConfig {
        skein_executor::ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(512).expect("non-zero blocking budget"),
            max_spill_bytes: NonZeroU64::new(64 * 1024).expect("non-zero spill budget"),
            max_spill_runs: NonZeroUsize::new(4).expect("non-zero spill run budget"),
            min_spill_free_bytes: NonZeroU64::MIN,
            spill_directory: std::env::temp_dir().join(format!(
                "skein-hash-join-spill-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock")
                    .as_nanos()
            )),
            ..skein_executor::ExecutionMemoryConfig::default()
        }
    }

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
    fn physical_join_plan_rejects_output_schema_drift() {
        let full_scan = || RelationalAccessPathDescriptor {
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
            estimated_rows: 1,
        };
        let join_predicate = match skein_sql::prepare_postgres_sql(
            "SELECT left_table.id FROM left_table INNER JOIN right_table ON left_table.id = right_table.id",
        )
        .expect("parse join predicate")
        .statement
        {
            SqlStatement::Select(select) => select
                .joins
                .into_iter()
                .next()
                .expect("join")
                .on,
            _ => unreachable!("join test must parse as a SELECT"),
        };
        let left = RelationalPhysicalJoinNode::relation(
            BindingId::new(0),
            "left_table".to_string(),
            "left_table".to_string(),
            RelationalPhysicalAccess::Base(RelationalAccessCandidate {
                descriptor: full_scan(),
                access: RelationalBaseAccess::FullScan,
            }),
        );
        let right = RelationalPhysicalJoinNode::relation(
            BindingId::new(1),
            "right_table".to_string(),
            "right_table".to_string(),
            RelationalPhysicalAccess::Probe(RelationalJoinAccessCandidate {
                descriptor: full_scan(),
                access: RelationalJoinAccess::FullScan,
            }),
        );
        let mut plan = RelationalPhysicalJoinPlan::new(
            RelationalPhysicalJoinNode::join(
                RelationalOperatorId::from_plan_index(1),
                SqlJoinKind::Inner,
                vec![join_predicate],
                left,
                right,
            )
            .expect("build physical join"),
            estimate_relational_access_cost(1),
        );
        let RelationalPhysicalJoinNode::Join { output_schema, .. } = &mut plan.root else {
            panic!("expected physical join root");
        };
        *output_schema =
            RelationalPhysicalOutputSchema::relation(BindingId::new(0), "left_table", "left_table");

        let error = plan
            .validate()
            .expect_err("schema drift must fail closed before execution");
        assert!(error.to_string().contains("inconsistent output schema"));
    }

    #[test]
    fn prepared_index_join_uses_batched_physical_operator_and_profile() {
        let state = batched_index_join_state();
        let prepared = prepare_batched_index_join(&state);
        let RelationalPhysicalJoinNode::Join { algorithm, .. } = &prepared
            .access_plan
            .physical_join_plan()
            .expect("physical join plan")
            .root
        else {
            panic!("expected physical join root");
        };
        assert_eq!(*algorithm, RelationalPhysicalJoinAlgorithm::BatchedIndex);
        let profiles =
            planned_operator_cardinality_profiles(&prepared).expect("physical operator profiles");
        assert_eq!(
            profiles[1].operator,
            RelationalOperatorKind::BatchedIndexNestedLoopLeftJoin
        );
    }

    #[test]
    fn batched_index_join_preserves_duplicate_probe_keys_and_left_join_nulls() {
        let state = batched_index_join_state();
        let prepared = prepare_batched_index_join(&state);
        let memory = skein_executor::ExecutionMemoryConfig::default();
        let execution = prepared
            .execution
            .admit(
                &state,
                RelationalQueryReadModes::new(
                    RelationalIndexReadMode::Materialized,
                    RelationalRowReadMode::CanonicalMemory,
                ),
                RelationalQueryResourceContext::new(
                    RelationalJoinEnumerationConfig::default(),
                    batched_index_join_limits(),
                    &memory,
                    None,
                ),
            )
            .expect("admit batched index join");
        let output = execute_select(&prepared, &[], execution).expect("execute batched index join");

        assert_eq!(output.rows.len(), 5);
        assert_eq!(
            output.rows[0]["outer_id"],
            Value::String("outer-1".to_string())
        );
        assert_eq!(
            output.rows[0]["inner_id"],
            Value::String("inner-1".to_string())
        );
        assert_eq!(
            output.rows[1]["outer_id"],
            Value::String("outer-1".to_string())
        );
        assert_eq!(
            output.rows[1]["inner_id"],
            Value::String("inner-2".to_string())
        );
        assert_eq!(
            output.rows[2]["outer_id"],
            Value::String("outer-2".to_string())
        );
        assert_eq!(output.rows[2]["inner_id"], Value::Null);
        assert_eq!(
            output.rows[3]["outer_id"],
            Value::String("outer-3".to_string())
        );
        assert_eq!(
            output.rows[3]["inner_id"],
            Value::String("inner-3".to_string())
        );
        assert_eq!(
            output.rows[4]["outer_id"],
            Value::String("outer-4".to_string())
        );
        assert_eq!(output.rows[4]["inner_id"], Value::Null);
        assert_eq!(
            output.operator_cardinality_profiles[1].operator,
            RelationalOperatorKind::BatchedIndexNestedLoopLeftJoin
        );
        assert_eq!(output.operator_cardinality_profiles[1].actual_rows, Some(5));
    }

    #[test]
    fn batched_index_join_rejects_an_input_row_larger_than_its_batch_budget() {
        let state = batched_index_join_state();
        let prepared = prepare_batched_index_join(&state);
        let memory = skein_executor::ExecutionMemoryConfig {
            batch_payload_bytes: NonZeroUsize::new(1).expect("non-zero batch budget"),
            ..skein_executor::ExecutionMemoryConfig::default()
        };
        let execution = prepared
            .execution
            .admit(
                &state,
                RelationalQueryReadModes::new(
                    RelationalIndexReadMode::Materialized,
                    RelationalRowReadMode::CanonicalMemory,
                ),
                RelationalQueryResourceContext::new(
                    RelationalJoinEnumerationConfig::default(),
                    batched_index_join_limits(),
                    &memory,
                    None,
                ),
            )
            .expect("admit constrained batched index join");
        let error = execute_select(&prepared, &[], execution)
            .expect_err("batched index join must enforce its batch budget");
        assert!(error
            .to_string()
            .contains("RelationalBatchedIndexJoin input row exceeds batch_payload_bytes 1"));
    }

    #[test]
    fn prepared_index_ordered_join_uses_merge_operator_and_profile() {
        let state = merge_join_state();
        let prepared = prepare_merge_join(&state);
        let RelationalPhysicalJoinNode::Join {
            algorithm,
            equi_join_keys,
            ..
        } = &prepared
            .access_plan
            .physical_join_plan()
            .expect("physical join plan")
            .root
        else {
            panic!("expected physical join root");
        };
        assert_eq!(*algorithm, RelationalPhysicalJoinAlgorithm::Merge);
        assert_eq!(
            equi_join_keys
                .as_ref()
                .expect("merge key contract")
                .columns
                .iter()
                .map(|(right, left)| (right.as_str(), left.name.as_str()))
                .collect::<Vec<_>>(),
            [("join_key", "join_key")]
        );
        let profiles =
            planned_operator_cardinality_profiles(&prepared).expect("physical operator profiles");
        assert_eq!(profiles[1].operator, RelationalOperatorKind::MergeJoin);
    }

    #[test]
    fn transaction_workspace_join_keeps_the_batched_index_probe_plan() {
        let state = merge_join_state();
        let prepared = prepare_merge_join_with_index_read_mode(
            &state,
            RelationalIndexReadMode::TransactionWorkspace,
        );
        let RelationalPhysicalJoinNode::Join { algorithm, .. } = &prepared
            .access_plan
            .physical_join_plan()
            .expect("physical join plan")
            .root
        else {
            panic!("expected physical join root");
        };
        assert_eq!(*algorithm, RelationalPhysicalJoinAlgorithm::BatchedIndex);
    }

    #[test]
    fn merge_join_reuses_right_key_groups_and_preserves_left_index_order() {
        let state = merge_join_state();
        let prepared = prepare_merge_join(&state);
        let memory = skein_executor::ExecutionMemoryConfig::default();
        let execution = prepared
            .execution
            .admit(
                &state,
                RelationalQueryReadModes::new(
                    RelationalIndexReadMode::Materialized,
                    RelationalRowReadMode::CanonicalMemory,
                ),
                RelationalQueryResourceContext::new(
                    RelationalJoinEnumerationConfig::default(),
                    batched_index_join_limits(),
                    &memory,
                    None,
                ),
            )
            .expect("admit merge join");
        let output = execute_select(&prepared, &[], execution).expect("execute merge join");

        assert_eq!(output.rows.len(), 3);
        assert_eq!(
            output.rows[0]["left_id"],
            Value::String("left-a".to_string())
        );
        assert_eq!(
            output.rows[0]["right_id"],
            Value::String("right-a".to_string())
        );
        assert_eq!(
            output.rows[1]["left_id"],
            Value::String("left-b".to_string())
        );
        assert_eq!(
            output.rows[1]["right_id"],
            Value::String("right-b-1".to_string())
        );
        assert_eq!(
            output.rows[2]["left_id"],
            Value::String("left-b".to_string())
        );
        assert_eq!(
            output.rows[2]["right_id"],
            Value::String("right-b-2".to_string())
        );
        assert_eq!(
            output.operator_cardinality_profiles[1].operator,
            RelationalOperatorKind::MergeJoin
        );
        assert_eq!(output.operator_cardinality_profiles[1].actual_rows, Some(3));
        assert!(output
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "RelationalMergeJoinRightInput"));
    }

    #[test]
    fn merge_join_rejects_right_input_that_exceeds_its_blocking_budget() {
        let state = merge_join_state();
        let prepared = prepare_merge_join(&state);
        let memory = skein_executor::ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(1).expect("non-zero blocking budget"),
            ..skein_executor::ExecutionMemoryConfig::default()
        };
        let execution = prepared
            .execution
            .admit(
                &state,
                RelationalQueryReadModes::new(
                    RelationalIndexReadMode::Materialized,
                    RelationalRowReadMode::CanonicalMemory,
                ),
                RelationalQueryResourceContext::new(
                    RelationalJoinEnumerationConfig::default(),
                    batched_index_join_limits(),
                    &memory,
                    None,
                ),
            )
            .expect("admit constrained merge join");
        let error = execute_select(&prepared, &[], execution)
            .expect_err("merge join must enforce its blocking budget");
        assert!(error
            .to_string()
            .contains("RelationalMergeJoinRightInput state exceeds blocking_operator_bytes 1"));
    }

    #[test]
    fn prepared_full_scan_equi_join_uses_hash_operator_and_profile() {
        let state = hash_join_state();
        let prepared = prepare_hash_join(&state);
        let RelationalPhysicalJoinNode::Join {
            algorithm,
            equi_join_keys,
            ..
        } = &prepared
            .access_plan
            .physical_join_plan()
            .expect("physical join plan")
            .root
        else {
            panic!("expected physical join root");
        };
        assert_eq!(*algorithm, RelationalPhysicalJoinAlgorithm::Hash);
        assert_eq!(
            equi_join_keys
                .as_ref()
                .expect("hash key contract")
                .columns
                .iter()
                .map(|(right, left)| (right.as_str(), left.name.as_str()))
                .collect::<Vec<_>>(),
            [("join_key", "join_key"), ("tag", "tag")]
        );
        let profiles =
            planned_operator_cardinality_profiles(&prepared).expect("physical operator profiles");
        assert_eq!(profiles[1].operator, RelationalOperatorKind::HashJoin);
        assert_eq!(
            profiles[1].access_path.kind,
            RelationalAccessPathKind::FullScan
        );
    }

    #[test]
    fn hash_join_preserves_duplicate_build_rows_and_evaluates_full_on_predicates() {
        let state = hash_join_state();
        let prepared = prepare_hash_join(&state);
        let memory = skein_executor::ExecutionMemoryConfig::default();
        let execution = prepared
            .execution
            .admit(
                &state,
                RelationalQueryReadModes::new(
                    RelationalIndexReadMode::Materialized,
                    RelationalRowReadMode::CanonicalMemory,
                ),
                RelationalQueryResourceContext::new(
                    RelationalJoinEnumerationConfig::default(),
                    batched_index_join_limits(),
                    &memory,
                    None,
                ),
            )
            .expect("admit hash join");
        let output = execute_select(&prepared, &[], execution).expect("execute hash join");

        assert_eq!(output.rows.len(), 3);
        assert_eq!(
            output.rows[0]["left_id"],
            Value::String("left-a".to_string())
        );
        assert_eq!(
            output.rows[0]["right_id"],
            Value::String("right-a".to_string())
        );
        assert_eq!(
            output.rows[1]["left_id"],
            Value::String("left-b".to_string())
        );
        assert_eq!(
            output.rows[1]["right_id"],
            Value::String("right-b-1".to_string())
        );
        assert_eq!(
            output.rows[2]["right_id"],
            Value::String("right-b-2".to_string())
        );
        assert_eq!(
            output.operator_cardinality_profiles[1].operator,
            RelationalOperatorKind::HashJoin
        );
        assert_eq!(output.operator_cardinality_profiles[1].actual_rows, Some(3));
        assert!(output
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "RelationalHashJoinBuild"));
    }

    #[test]
    fn hash_join_spills_and_falls_back_for_a_hot_partition() {
        let state = hash_join_state();
        let prepared = prepare_hash_join(&state);
        let memory = constrained_hash_join_memory();
        let execution = prepared
            .execution
            .admit(
                &state,
                RelationalQueryReadModes::new(
                    RelationalIndexReadMode::Materialized,
                    RelationalRowReadMode::CanonicalMemory,
                ),
                RelationalQueryResourceContext::new(
                    RelationalJoinEnumerationConfig::default(),
                    batched_index_join_limits(),
                    &memory,
                    None,
                ),
            )
            .expect("admit constrained hash join");
        let output = execute_select(&prepared, &[], execution).expect("spill-backed hash join");
        let mut rows = output
            .rows
            .iter()
            .map(|row| (row["left_id"].clone(), row["right_id"].clone()))
            .collect::<Vec<_>>();
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (
                    Value::String("left-a".to_string()),
                    Value::String("right-a".to_string()),
                ),
                (
                    Value::String("left-b".to_string()),
                    Value::String("right-b-1".to_string()),
                ),
                (
                    Value::String("left-b".to_string()),
                    Value::String("right-b-2".to_string()),
                ),
            ]
        );
        assert!(output
            .blocking_operator_memory_reports
            .iter()
            .any(|report| {
                report.operator == "RelationalHashJoinGrace"
                    && report.spilled_rows > 0
                    && report.spill_run_count > 0
            }));
        assert!(output
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "RelationalHashJoinGraceHotPartition"));
        std::fs::remove_dir_all(&memory.spill_directory).expect("remove hash join spill fixture");
    }

    #[test]
    fn hash_left_join_null_extends_unmatched_and_null_keys() {
        let state = hash_join_state();
        let prepared = prepare_hash_left_join(&state);
        let RelationalPhysicalJoinNode::Join {
            algorithm, kind, ..
        } = &prepared
            .access_plan
            .physical_join_plan()
            .expect("physical join plan")
            .root
        else {
            panic!("expected physical join root");
        };
        assert_eq!(*algorithm, RelationalPhysicalJoinAlgorithm::Hash);
        assert_eq!(*kind, SqlJoinKind::Left);

        let memory = skein_executor::ExecutionMemoryConfig::default();
        let execution = prepared
            .execution
            .admit(
                &state,
                RelationalQueryReadModes::new(
                    RelationalIndexReadMode::Materialized,
                    RelationalRowReadMode::CanonicalMemory,
                ),
                RelationalQueryResourceContext::new(
                    RelationalJoinEnumerationConfig::default(),
                    batched_index_join_limits(),
                    &memory,
                    None,
                ),
            )
            .expect("admit hash left join");
        let output = execute_select(&prepared, &[], execution).expect("execute hash left join");
        let mut rows = output
            .rows
            .iter()
            .map(|row| (row["left_id"].clone(), row["right_id"].clone()))
            .collect::<Vec<_>>();
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (
                    Value::String("left-a".to_string()),
                    Value::String("right-a".to_string()),
                ),
                (
                    Value::String("left-b".to_string()),
                    Value::String("right-b-1".to_string()),
                ),
                (
                    Value::String("left-b".to_string()),
                    Value::String("right-b-2".to_string()),
                ),
                (Value::String("left-null".to_string()), Value::Null),
            ]
        );
        assert_eq!(
            output.operator_cardinality_profiles[1].operator,
            RelationalOperatorKind::HashJoin
        );
    }

    #[test]
    fn hash_join_observes_cancellation_after_admission() {
        let state = hash_join_state();
        let prepared = prepare_hash_join(&state);
        let memory = skein_executor::ExecutionMemoryConfig {
            batch_rows: NonZeroUsize::MIN,
            ..skein_executor::ExecutionMemoryConfig::default()
        };
        let cancellation = skein_core::RuntimeCancellationToken::new();
        let task_context = skein_core::RuntimeTaskContext::without_deadline(cancellation.clone());
        let execution = prepared
            .execution
            .admit(
                &state,
                RelationalQueryReadModes::new(
                    RelationalIndexReadMode::Materialized,
                    RelationalRowReadMode::CanonicalMemory,
                ),
                RelationalQueryResourceContext::new(
                    RelationalJoinEnumerationConfig::default(),
                    batched_index_join_limits(),
                    &memory,
                    Some(&task_context),
                ),
            )
            .expect("admit hash join before cancellation");
        assert!(cancellation.cancel());
        let error = execute_select(&prepared, &[], execution)
            .expect_err("cancelled hash join must stop at a bounded checkpoint");
        assert!(error
            .to_string()
            .contains("runtime task stopped: cancelled"));
    }

    #[test]
    fn prepared_bushy_physical_join_plan_materializes_the_composite_right_input_once() {
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
            max_candidate_work: 128,
            hydration: RelationalHydrationBudget::default(),
            index_read: skein_storage::RelationalIndexReadLimits::default(),
            row_read: skein_storage::RelationalRowPageSnapshotReadLimits::default(),
        };
        let mut binding_nanos = 0;
        let planned = join_order::plan_select_join_order(
            select.clone(),
            &[],
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            limits,
            RelationalJoinPlanningContext::default(),
            &mut binding_nanos,
        )
        .expect("plan bushy candidate with probe-only CSG-CMP policy");
        let access_plan = planned
            .access_plan
            .expect("eligible bushy candidate retains an access plan");
        assert_eq!(
            access_plan
                .physical_join_plan()
                .expect("eligible bushy candidate has a physical join plan")
                .root
                .materialized_right_count(),
            0
        );

        let mut syntax_plan = prepare_syntax_access_plan(
            &select,
            &[],
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::CanonicalMemory,
            ),
            limits,
        )
        .expect("prepare syntax access plan");
        syntax_plan
            .finalize_physical_join_plan(&select, &state, RelationalIndexReadMode::Materialized)
            .expect("finalize syntax physical join plan");
        let syntax_physical_plan = syntax_plan
            .physical_join_plan()
            .expect("syntax physical join plan");
        assert_eq!(syntax_physical_plan.root.materialized_right_count(), 0);
        assert!(matches!(
            syntax_physical_plan.root,
            RelationalPhysicalJoinNode::Join {
                algorithm: RelationalPhysicalJoinAlgorithm::Probe,
                ..
            }
        ));
        assert_eq!(
            syntax_physical_plan
                .output_schema
                .bindings
                .iter()
                .map(|binding| (
                    binding.binding.get(),
                    binding.table.as_str(),
                    binding.qualifier.as_str()
                ))
                .collect::<Vec<_>>(),
            [
                (0, "bushy_a", "a"),
                (1, "bushy_b", "b"),
                (2, "bushy_c", "c"),
                (3, "bushy_d", "d"),
            ]
        );
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
            projection: RelationalProjectionAccessPlanning::default(),
        })
        .expect("prepare bushy_c materialized base access");

        let relation =
            |binding: u32, table: &str, qualifier: &str, access: RelationalPhysicalAccess| {
                RelationalPhysicalJoinNode::relation(
                    BindingId::new(binding),
                    table.to_string(),
                    qualifier.to_string(),
                    access,
                )
            };
        let left = RelationalPhysicalJoinNode::join(
            RelationalOperatorId::from_plan_index(1),
            SqlJoinKind::Inner,
            vec![select.joins[0].on.clone()],
            relation(
                0,
                "bushy_a",
                "a",
                RelationalPhysicalAccess::Base(syntax_plan.base_access.clone()),
            ),
            relation(
                1,
                "bushy_b",
                "b",
                RelationalPhysicalAccess::Probe(syntax_plan.join_accesses[0].clone()),
            ),
        )
        .expect("build left physical join");
        let right = RelationalPhysicalJoinNode::join(
            RelationalOperatorId::from_plan_index(2),
            SqlJoinKind::Inner,
            vec![select.joins[2].on.clone()],
            relation(2, "bushy_c", "c", RelationalPhysicalAccess::Base(c_base)),
            relation(
                3,
                "bushy_d",
                "d",
                RelationalPhysicalAccess::Probe(syntax_plan.join_accesses[2].clone()),
            ),
        )
        .expect("build right physical join");
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
            RelationalJoinSelectivity::Unknown,
        );
        let root = RelationalPhysicalJoinNode::join(
            RelationalOperatorId::from_plan_index(3),
            SqlJoinKind::Inner,
            vec![select.joins[1].on.clone()],
            left,
            right,
        )
        .expect("build materialized physical join");
        let mut access_plan = syntax_plan;
        access_plan.join_selection = None;
        access_plan.physical_join_plan = Some(RelationalPhysicalJoinPlan::new(root, cost));
        let execution = PreparedRelationalExecutionDescriptor::prepare(&select, &access_plan)
            .expect("prepare relational execution descriptor");
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
        let physical_plan = prepared
            .access_plan
            .physical_join_plan()
            .expect("materialized physical join plan");
        assert_eq!(physical_plan.root.materialized_right_count(), 1);
        assert!(matches!(
            physical_plan.root,
            RelationalPhysicalJoinNode::Join {
                algorithm: RelationalPhysicalJoinAlgorithm::Materialized,
                ..
            }
        ));
        assert_eq!(
            physical_plan
                .output_schema
                .bindings
                .iter()
                .map(|binding| (binding.binding.get(), binding.qualifier.as_str()))
                .collect::<Vec<_>>(),
            [(0, "a"), (1, "b"), (2, "c"), (3, "d")]
        );

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
