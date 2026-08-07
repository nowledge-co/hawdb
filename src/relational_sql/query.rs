use crate::error::{Result, SkeinError};
use crate::executor::{map_payload_bytes, Row};
use crate::sql::{
    SelectProjection, SelectStatement, SqlBound, SqlColumnRef, SqlComparisonOp, SqlExpression,
    SqlFunctionArgument, SqlJoinKind, SqlNullOrder, SqlOrderDirection, SqlPredicate, SqlStatement,
    SqlValue,
};
use crate::value::Value;
use skein_core::Catalog;
use skein_executor::binding::Binding as ExecutorBinding;
use skein_executor::blocking::{
    stream_distinct_batches, stream_sort_batches, stream_top_n_batches, BindingBatchSource,
    BlockingExecutionContext,
};
use skein_executor::kernel::{ensure_operator_item_fits, OperatorMemoryTracker};
use skein_executor::observer::ExecutionObserver;
use skein_executor::pipeline::{BatchControl, BindingBatch};
use skein_executor::{BlockingOperatorMemoryReport, ExecutionLimit};
use skein_optimizer::{
    select_relational_access_path, RelationalAccessPathDescriptor, RelationalAccessPathKind,
};
use skein_plan::{PhysicalPlan, SortDirection, SortItem, SortKey};
use skein_storage::{
    RelationalHydrationBudget, RelationalKey, RelationalRow, RelationalScalarType, RelationalState,
    RelationalTableSchema, RelationalValue,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RelationalQueryLimits {
    pub max_output_rows: usize,
    pub max_output_payload_bytes: usize,
    pub max_intermediate_rows: usize,
    pub batch_rows: NonZeroUsize,
    pub blocking_operator_bytes: NonZeroUsize,
    pub hydration: RelationalHydrationBudget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelationalQueryOutput {
    pub rows: Vec<Row>,
    pub intermediate_rows: usize,
    pub hydration: RelationalHydrationBudget,
    pub access_path: RelationalAccessPathDescriptor,
    pub join_access_paths: Vec<RelationalAccessPathDescriptor>,
    pub blocking_operator_memory_reports: Vec<BlockingOperatorMemoryReport>,
}

pub(crate) fn execute_relational_query_sql_with_runtime(
    sql: &str,
    parameters: &[Value],
    state: &RelationalState,
    limits: RelationalQueryLimits,
    execution_memory: &skein_executor::ExecutionMemoryConfig,
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<RelationalQueryOutput> {
    let prepared = skein_sql::prepare_postgres_sql(sql)?;
    if prepared.parameters.len() != parameters.len() {
        return Err(SkeinError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    match prepared.statement {
        SqlStatement::Select(select) => execute_select(
            &select,
            parameters,
            state,
            limits,
            execution_memory,
            task_context,
            false,
        ),
        SqlStatement::Explain(explain) => {
            let SqlStatement::Select(select) = *explain.statement else {
                return Err(SkeinError::Semantic(
                    "EXPLAIN only supports relational SELECT".to_string(),
                ));
            };
            let output = execute_select(
                &select,
                parameters,
                state,
                limits,
                execution_memory,
                task_context,
                !explain.analyze,
            )?;
            if explain.analyze {
                format_relational_explain(&select, parameters, output, true, limits)
            } else {
                Ok(output)
            }
        }
        _ => Err(SkeinError::Semantic(
            "relational query entrypoint requires SELECT or EXPLAIN SELECT".to_string(),
        )),
    }
}

#[derive(Clone)]
struct Binding<'a> {
    table: &'a str,
    qualifier: &'a str,
    schema: &'a RelationalTableSchema,
    key: Option<&'a RelationalKey>,
    row: Option<&'a RelationalRow>,
}

#[derive(Clone, Default)]
struct BoundRow<'a> {
    bindings: Vec<Binding<'a>>,
}

#[derive(Debug)]
enum RelationalBaseAccess {
    PrimaryKey(RelationalKey),
    Index { name: String, prefix: RelationalKey },
    FullScan,
}

#[derive(Debug)]
enum RelationalJoinAccess {
    PrimaryKey(Vec<(String, SqlColumnRef)>),
    Index {
        name: String,
        columns: Vec<(String, SqlColumnRef)>,
    },
    FullScan,
}

#[derive(Debug)]
struct RelationalAccessCandidate {
    descriptor: RelationalAccessPathDescriptor,
    access: RelationalBaseAccess,
}

#[derive(Debug)]
struct RelationalJoinAccessCandidate {
    descriptor: RelationalAccessPathDescriptor,
    access: RelationalJoinAccess,
}

struct PlannedJoin<'a> {
    join: &'a crate::sql::SqlJoin,
    schema: &'a RelationalTableSchema,
    qualifier: String,
    access: RelationalJoinAccess,
}

struct RelationalPipelineState<'a> {
    task_context: Option<&'a skein_core::RuntimeTaskContext>,
    batch_rows: usize,
    rows_until_checkpoint: usize,
    intermediate_rows: usize,
    max_intermediate_rows: usize,
}

impl<'a> RelationalPipelineState<'a> {
    fn new(
        task_context: Option<&'a skein_core::RuntimeTaskContext>,
        limits: RelationalQueryLimits,
    ) -> Self {
        let batch_rows = limits.batch_rows.get();
        Self {
            task_context,
            batch_rows,
            rows_until_checkpoint: batch_rows,
            intermediate_rows: 0,
            max_intermediate_rows: limits.max_intermediate_rows,
        }
    }

    fn account_row(&mut self) -> Result<()> {
        account_intermediate(&mut self.intermediate_rows, 1, self.max_intermediate_rows)?;
        self.rows_until_checkpoint = self.rows_until_checkpoint.saturating_sub(1);
        if self.rows_until_checkpoint == 0 {
            skein_executor::pipeline::runtime_checkpoint(self.task_context)?;
            self.rows_until_checkpoint = self.batch_rows;
        }
        Ok(())
    }

    fn finish(&self) -> Result<()> {
        skein_executor::pipeline::runtime_checkpoint(self.task_context)
    }
}

fn execute_select(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    limits: RelationalQueryLimits,
    execution_memory: &skein_executor::ExecutionMemoryConfig,
    task_context: Option<&skein_core::RuntimeTaskContext>,
    explain_only: bool,
) -> Result<RelationalQueryOutput> {
    reject_non_public_schema(select.from.schema.as_deref())?;
    let base_schema = state.table_schema(&select.from.name).ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational table {}", select.from.name))
    })?;
    let base_qualifier = select
        .from_alias
        .clone()
        .unwrap_or_else(|| select.from.name.clone());
    let base_access = choose_base_access(
        select.selection.as_ref(),
        parameters,
        state,
        base_schema,
        &select.from.name,
        &base_qualifier,
        limits.max_intermediate_rows.saturating_add(1),
    )?;
    let access_path = base_access.descriptor.clone();
    let mut planned_joins = Vec::with_capacity(select.joins.len());
    let mut join_access_paths = Vec::with_capacity(select.joins.len());
    for join in &select.joins {
        reject_non_public_schema(join.table.schema.as_deref())?;
        let join_schema = state.table_schema(&join.table.name).ok_or_else(|| {
            SkeinError::Semantic(format!("unknown relational table {}", join.table.name))
        })?;
        let qualifier = join
            .alias
            .clone()
            .unwrap_or_else(|| join.table.name.clone());
        let join_access =
            choose_join_access(&join.on, state, join_schema, &join.table.name, &qualifier)?;
        join_access_paths.push(join_access.descriptor.clone());
        planned_joins.push(PlannedJoin {
            join,
            schema: join_schema,
            qualifier,
            access: join_access.access,
        });
    }

    let has_aggregate = select.projection.iter().any(projection_contains_aggregate);
    let has_blocking_operator = has_aggregate
        || !select.group_by.is_empty()
        || !select.order_by.is_empty()
        || select.distinct;
    if explain_only {
        return format_relational_explain(
            select,
            parameters,
            RelationalQueryOutput {
                rows: Vec::new(),
                intermediate_rows: 0,
                hydration: limits.hydration,
                access_path,
                join_access_paths,
                blocking_operator_memory_reports: Vec::new(),
            },
            false,
            limits,
        );
    }
    let mut pipeline = RelationalPipelineState::new(task_context, limits);
    if !has_blocking_operator {
        let output = execute_streaming_projection(
            select,
            parameters,
            state,
            base_schema,
            &base_qualifier,
            &base_access.access,
            &planned_joins,
            &mut pipeline,
            limits,
        )?;
        pipeline.finish()?;
        return Ok(RelationalQueryOutput {
            rows: output.rows,
            intermediate_rows: pipeline.intermediate_rows,
            hydration: output.hydration,
            access_path,
            join_access_paths,
            blocking_operator_memory_reports: output.blocking_operator_memory_reports,
        });
    }

    if has_aggregate || !select.group_by.is_empty() {
        return execute_aggregate_select(
            select,
            parameters,
            state,
            base_schema,
            &base_qualifier,
            &base_access.access,
            &planned_joins,
            &mut pipeline,
            limits,
            execution_memory,
            access_path,
            join_access_paths,
        );
    }

    let output = execute_blocking_projection(
        select,
        parameters,
        state,
        base_schema,
        &base_qualifier,
        &base_access.access,
        &planned_joins,
        &mut pipeline,
        limits,
        execution_memory,
    )?;
    pipeline.finish()?;
    Ok(RelationalQueryOutput {
        rows: output.rows,
        intermediate_rows: pipeline.intermediate_rows,
        hydration: output.hydration,
        access_path,
        join_access_paths,
        blocking_operator_memory_reports: output.blocking_operator_memory_reports,
    })
}

#[derive(Debug)]
struct RelationalExplainNode {
    operator: &'static str,
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
    if !select.order_by.is_empty() {
        nodes.push(RelationalExplainNode {
            operator: "TopNExec",
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
            estimated_rows: Some(output.access_path.estimated_rows),
            access_object: String::new(),
            operator_info: format!("group_keys=[{}]", explain_columns(&select.group_by)),
            report_operator: Some("SortExec"),
        });
    }
    if select.distinct || single_count_distinct_column(select).is_some() {
        nodes.push(RelationalExplainNode {
            operator: "DistinctExec",
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
        estimated_rows: Some(output.access_path.estimated_rows),
        access_object: String::new(),
        operator_info: format!("columns={}", select.projection.len()),
        report_operator: None,
    });
    if select.selection.is_some() {
        nodes.push(RelationalExplainNode {
            operator: "SelectionExec",
            estimated_rows: Some(output.access_path.estimated_rows),
            access_object: String::new(),
            operator_info: "residual_predicate=true".to_string(),
            report_operator: None,
        });
    }
    for (join, descriptor) in select.joins.iter().zip(&output.join_access_paths) {
        nodes.push(RelationalExplainNode {
            operator: match join.kind {
                SqlJoinKind::Inner => "IndexNestedLoopJoinExec",
                SqlJoinKind::Left => "IndexNestedLoopLeftJoinExec",
            },
            estimated_rows: Some(descriptor.estimated_rows),
            access_object: explain_access_object(&join.table.name, descriptor),
            operator_info: explain_access_path(descriptor),
            report_operator: None,
        });
    }
    nodes.push(RelationalExplainNode {
        operator: match output.access_path.kind {
            RelationalAccessPathKind::FullScan => "TableFullScanExec",
            RelationalAccessPathKind::PrimaryKey => "TablePointGetExec",
            RelationalAccessPathKind::Index => "IndexRangeScanExec",
        },
        estimated_rows: Some(output.access_path.estimated_rows),
        access_object: explain_access_object(&select.from.name, &output.access_path),
        operator_info: explain_access_path(&output.access_path),
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
        let mut row = Row::from([
            (
                "id".to_string(),
                Value::String(explain_tree_id(node.operator, index)),
            ),
            (
                "estRows".to_string(),
                optional_usize_explain_value(node.estimated_rows),
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
                if index == 0 {
                    optional_usize_explain_value(Some(actual_output_rows))
                } else {
                    optional_usize_explain_value(report.map(|report| report.input_rows))
                },
            );
            row.insert(
                "execution info".to_string(),
                if index == 0 {
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
    output.rows = rows;
    Ok(output)
}

fn explain_tree_id(operator: &str, index: usize) -> String {
    if index == 0 {
        format!("{operator}_1")
    } else {
        format!("{}└─{operator}_{}", "  ".repeat(index - 1), index + 1)
    }
}

fn optional_usize_explain_value(value: Option<usize>) -> Value {
    value
        .map(|value| Value::Int(i64::try_from(value).unwrap_or(i64::MAX)))
        .unwrap_or(Value::Null)
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

fn explain_access_path(descriptor: &RelationalAccessPathDescriptor) -> String {
    format!(
        "equality_prefix={}, unique_point={}, row_fetch={}",
        descriptor.equality_prefix_len, descriptor.unique_point, descriptor.requires_row_fetch
    )
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

fn choose_base_access(
    predicate: Option<&SqlPredicate>,
    parameters: &[Value],
    state: &RelationalState,
    schema: &RelationalTableSchema,
    table: &str,
    qualifier: &str,
    cardinality_limit: usize,
) -> Result<RelationalAccessCandidate> {
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
            unique_point: false,
            covering: false,
            requires_row_fetch: false,
            estimated_rows: row_count.min(cardinality_limit),
        },
        access: RelationalBaseAccess::FullScan,
    }];

    if let Some(key) = complete_key(&schema.primary_key, &bound) {
        let estimated_rows = usize::from(state.row(table, &key).is_some());
        candidates.push(RelationalAccessCandidate {
            descriptor: RelationalAccessPathDescriptor {
                kind: RelationalAccessPathKind::PrimaryKey,
                name: "__primary_key".to_string(),
                index_columns: schema.primary_key.clone(),
                access_columns: schema.primary_key.iter().cloned().collect(),
                equality_prefix_len: schema.primary_key.len(),
                order_prefix_len: 0,
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
            state,
            table,
            format!("__unique_{ordinal}"),
            columns,
            true,
            &bound,
            cardinality_limit,
        )? {
            candidates.push(candidate);
        }
    }
    for index in &schema.indexes {
        if let Some(candidate) = index_access_candidate(
            state,
            table,
            index.name.clone(),
            &index.columns,
            index.unique,
            &bound,
            cardinality_limit,
        )? {
            candidates.push(candidate);
        }
    }

    let selected = select_relational_access_path(
        candidates
            .iter()
            .map(|candidate| candidate.descriptor.clone()),
    )
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
    state: &RelationalState,
    table: &str,
    name: String,
    columns: &[String],
    unique: bool,
    bound: &BTreeMap<String, RelationalValue>,
    cardinality_limit: usize,
) -> Result<Option<RelationalAccessCandidate>> {
    let prefix = columns
        .iter()
        .map_while(|column| bound.get(column).cloned())
        .collect::<Vec<_>>();
    if prefix.is_empty() {
        return Ok(None);
    }
    let prefix_len = prefix.len();
    let key = RelationalKey(prefix);
    let estimated_rows = state
        .index_prefix_cardinality_at_most(table, &name, &key, cardinality_limit)
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "relational index {name} on table {table} is not materialized"
            ))
        })?;
    Ok(Some(RelationalAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::Index,
            name: name.clone(),
            index_columns: columns.to_vec(),
            access_columns: columns[..prefix_len].iter().cloned().collect(),
            equality_prefix_len: prefix_len,
            order_prefix_len: 0,
            unique_point: unique && prefix_len == columns.len(),
            covering: false,
            requires_row_fetch: true,
            estimated_rows,
        },
        access: RelationalBaseAccess::Index { name, prefix: key },
    }))
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
            unique_point: false,
            covering: false,
            requires_row_fetch: false,
            estimated_rows: row_count,
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
                unique_point: true,
                covering: false,
                requires_row_fetch: false,
                estimated_rows: usize::from(row_count != 0),
            },
            access: RelationalJoinAccess::PrimaryKey(columns),
        });
    }

    for (ordinal, columns) in schema.unique_constraints.iter().enumerate() {
        if let Some(candidate) = join_index_access_candidate(
            format!("__unique_{ordinal}"),
            columns,
            true,
            &bound,
            row_count,
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
    Some(RelationalJoinAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::Index,
            name: name.clone(),
            index_columns: columns.to_vec(),
            access_columns: columns[..equality_prefix_len].iter().cloned().collect(),
            equality_prefix_len,
            order_prefix_len: 0,
            unique_point,
            covering: false,
            requires_row_fetch: true,
            estimated_rows: if unique_point {
                usize::from(row_count != 0)
            } else {
                row_count
            },
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
    table: &str,
    schema: &RelationalTableSchema,
    row: &BoundRow<'a>,
    access: &RelationalJoinAccess,
    visit: &mut dyn FnMut(&'a RelationalKey, &'a RelationalRow) -> Result<bool>,
) -> Result<bool> {
    match access {
        RelationalJoinAccess::PrimaryKey(columns) => {
            let Some(key) = bound_join_key(row, schema, columns)? else {
                return Ok(true);
            };
            if let Some((key, row)) = state.row_entry(table, &key) {
                return visit(key, row);
            }
        }
        RelationalJoinAccess::Index { name, columns } => {
            let Some(prefix) = bound_join_key(row, schema, columns)? else {
                return Ok(true);
            };
            let mut error = None;
            state
                .visit_index_prefix_rows(table, name, &prefix, |key, candidate| {
                    match visit(key, candidate) {
                        Ok(keep_going) => keep_going,
                        Err(candidate_error) => {
                            error = Some(candidate_error);
                            false
                        }
                    }
                })
                .ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "relational index {name} on table {table} is not materialized"
                    ))
                })?;
            if let Some(error) = error {
                return Err(error);
            }
        }
        RelationalJoinAccess::FullScan => {
            for (key, candidate) in state.rows(table) {
                if !visit(key, candidate)? {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

fn visit_base_entries<'a>(
    state: &'a RelationalState,
    table: &str,
    access: &RelationalBaseAccess,
    visit: &mut dyn FnMut(&'a RelationalKey, &'a RelationalRow) -> Result<bool>,
) -> Result<bool> {
    match access {
        RelationalBaseAccess::PrimaryKey(key) => match state.row_entry(table, key) {
            Some((key, row)) => visit(key, row),
            None => Ok(true),
        },
        RelationalBaseAccess::Index { name, prefix } => {
            let mut error = None;
            let mut keep_going = true;
            state
                .visit_index_prefix_rows(table, name, prefix, |key, row| match visit(key, row) {
                    Ok(continue_scan) => {
                        keep_going = continue_scan;
                        continue_scan
                    }
                    Err(candidate_error) => {
                        error = Some(candidate_error);
                        false
                    }
                })
                .ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "relational index {name} on table {table} is not materialized"
                    ))
                })?;
            match error {
                Some(error) => Err(error),
                None => Ok(keep_going),
            }
        }
        RelationalBaseAccess::FullScan => {
            for (key, row) in state.rows(table) {
                if !visit(key, row)? {
                    return Ok(false);
                }
            }
            Ok(true)
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
    pipeline: &mut RelationalPipelineState<'_>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    visit_base_entries(state, &select.from.name, base_access, &mut |key, row| {
        pipeline.account_row()?;
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
                    key: Some(key),
                    row: Some(row),
                }],
            },
            pipeline,
            visit,
        )
    })
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
        &planned.join.table.name,
        planned.schema,
        &row,
        &planned.access,
        &mut |key, candidate| {
            let mut combined = row.clone();
            combined.bindings.push(Binding {
                table: &planned.join.table.name,
                qualifier: &planned.qualifier,
                schema: planned.schema,
                key: Some(key),
                row: Some(candidate),
            });
            if predicate_truth(&planned.join.on, &combined, parameters)? != Some(true) {
                return Ok(true);
            }
            matched = true;
            pipeline.account_row()?;
            visit_joined_row(
                select,
                parameters,
                state,
                joins,
                join_index + 1,
                combined,
                pipeline,
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
            key: None,
            row: None,
        });
        pipeline.account_row()?;
        return visit_joined_row(
            select,
            parameters,
            state,
            joins,
            join_index + 1,
            combined,
            pipeline,
            visit,
        );
    }
    Ok(true)
}

struct StreamingProjectionOutput {
    rows: Vec<Row>,
    hydration: RelationalHydrationBudget,
    blocking_operator_memory_reports: Vec<BlockingOperatorMemoryReport>,
}

const RELATIONAL_LOCATOR_COLUMN: &str = "__skein_relational_locator";
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

struct LocatorBatchSource<'a, 'pipeline> {
    select: &'a SelectStatement,
    parameters: &'a [Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &'a RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    pipeline: &'pipeline mut RelationalPipelineState<'a>,
    batch_rows: usize,
}

struct GroupLocatorBatchSource<'a, 'pipeline> {
    select: &'a SelectStatement,
    parameters: &'a [Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &'a RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    pipeline: &'pipeline mut RelationalPipelineState<'a>,
    batch_rows: usize,
}

impl BindingBatchSource for GroupLocatorBatchSource<'_, '_> {
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
            self.pipeline,
            &mut |row| {
                let mut values =
                    BTreeMap::from([(RELATIONAL_LOCATOR_COLUMN.to_string(), locator_value(&row))]);
                for (ordinal, column) in self.select.group_by.iter().enumerate() {
                    let value = relational_sort_value(resolve_column(&row, column)?)?;
                    values.insert(
                        relational_sort_column(ordinal),
                        postgres_sort_key(
                            value,
                            SqlOrderDirection::Asc,
                            SqlNullOrder::DialectDefault,
                        ),
                    );
                }
                batch.push(ExecutorBinding::values(values));
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

impl BindingBatchSource for LocatorBatchSource<'_, '_> {
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
            self.pipeline,
            &mut |row| {
                batch.push(locator_sort_binding(&row, &self.select.order_by)?);
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

struct ProjectedBatchSource<'a, 'pipeline, 'hydration> {
    select: &'a SelectStatement,
    parameters: &'a [Value],
    state: &'a RelationalState,
    base_schema: &'a RelationalTableSchema,
    base_qualifier: &'a str,
    base_access: &'a RelationalBaseAccess,
    joins: &'a [PlannedJoin<'a>],
    pipeline: &'pipeline mut RelationalPipelineState<'a>,
    hydration: &'hydration mut RelationalHydrationBudget,
    task_context: Option<&'a skein_core::RuntimeTaskContext>,
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
    pipeline: &'pipeline mut RelationalPipelineState<'a>,
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
            self.pipeline,
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

impl BindingBatchSource for ProjectedBatchSource<'_, '_, '_> {
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
            self.pipeline,
            &mut |row| {
                let projected = project_bound_row(
                    &row,
                    &self.select.projection,
                    self.state,
                    self.hydration,
                    self.task_context,
                )?;
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
    pipeline: &mut RelationalPipelineState<'a>,
    limits: RelationalQueryLimits,
    memory: &skein_executor::ExecutionMemoryConfig,
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
    let mut hydration = limits.hydration;
    let mut output = Vec::with_capacity(detection_limit.min(limits.max_output_rows));
    let mut payload_bytes = 0usize;

    if select.distinct {
        let mut projected = ProjectedBatchSource {
            select,
            parameters,
            state,
            base_schema,
            base_qualifier,
            base_access,
            joins,
            pipeline,
            hydration: &mut hydration,
            task_context,
            batch_rows: limits.batch_rows.get(),
        };
        let mut distinct = DistinctBatchSource {
            input: &mut projected,
            input_plan: &input_plan,
            catalog: &catalog,
            memory,
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
        let mut locators = LocatorBatchSource {
            select,
            parameters,
            state,
            base_schema,
            base_qualifier,
            base_access,
            joins,
            pipeline,
            batch_rows: limits.batch_rows.get(),
        };
        execute_relational_order(
            &input_plan,
            &mut locators,
            select,
            offset,
            detection_limit,
            &catalog,
            memory,
            task_context,
            &observer,
            &mut |batch| {
                consume_locator_batch(
                    batch,
                    select,
                    state,
                    &mut hydration,
                    task_context,
                    &mut output,
                    &mut payload_bytes,
                    detection_limit,
                    limits,
                )
            },
        )?;
    }
    Ok(StreamingProjectionOutput {
        rows: output,
        hydration,
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

#[allow(clippy::too_many_arguments)]
fn consume_locator_batch(
    batch: BindingBatch,
    select: &SelectStatement,
    state: &RelationalState,
    hydration: &mut RelationalHydrationBudget,
    task_context: Option<&skein_core::RuntimeTaskContext>,
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
        let locator = binding
            .values
            .remove(RELATIONAL_LOCATOR_COLUMN)
            .ok_or_else(|| {
                SkeinError::Execution("relational spill row is missing its locator".to_string())
            })?;
        let row = project_locator(&locator, select, state, hydration, task_context)?;
        push_relational_output(row, output, payload_bytes, limits)?;
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

fn locator_sort_binding(
    row: &BoundRow<'_>,
    order_by: &[crate::sql::SqlOrderItem],
) -> Result<ExecutorBinding> {
    let mut values = BTreeMap::from([(RELATIONAL_LOCATOR_COLUMN.to_string(), locator_value(row))]);
    for (ordinal, item) in order_by.iter().enumerate() {
        let value = relational_sort_value(resolve_column(row, &item.column)?)?;
        values.insert(
            relational_sort_column(ordinal),
            postgres_sort_key(value, item.direction, item.nulls),
        );
    }
    Ok(ExecutorBinding::values(values))
}

fn relational_sort_value(value: &RelationalValue) -> Result<Value> {
    match value {
        RelationalValue::Null => Ok(Value::Null),
        RelationalValue::Boolean(value) => Ok(Value::Bool(*value)),
        RelationalValue::BigInt(value) => Ok(Value::Int(*value)),
        RelationalValue::DoublePrecision(value) => Ok(Value::Float(*value)),
        RelationalValue::Text(value) => Ok(Value::String(value.clone())),
        RelationalValue::Bytea(value) => Ok(Value::String(format!("bytea:{}", hex_encode(value)))),
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

fn locator_value(row: &BoundRow<'_>) -> Value {
    Value::List(
        row.bindings
            .iter()
            .map(|binding| {
                Value::Map(BTreeMap::from([
                    (
                        "table".to_string(),
                        Value::String(binding.table.to_string()),
                    ),
                    (
                        "qualifier".to_string(),
                        Value::String(binding.qualifier.to_string()),
                    ),
                    (
                        "key".to_string(),
                        binding.key.map_or(Value::Null, |key| {
                            Value::List(key.0.iter().map(encode_locator_value).collect())
                        }),
                    ),
                ]))
            })
            .collect(),
    )
}

fn encode_locator_value(value: &RelationalValue) -> Value {
    let (kind, value) = match value {
        RelationalValue::Null => ("null", Value::Null),
        RelationalValue::Boolean(value) => ("bool", Value::Bool(*value)),
        RelationalValue::BigInt(value) => ("int", Value::Int(*value)),
        RelationalValue::DoublePrecision(value) => ("float", Value::Float(*value)),
        RelationalValue::Text(value) => ("text", Value::String(value.clone())),
        RelationalValue::Bytea(value) => ("bytea", Value::String(hex_encode(value))),
        RelationalValue::Overflow(reference) => (
            "overflow",
            Value::Map(BTreeMap::from([
                (
                    "digest".to_string(),
                    Value::String(reference.digest.clone()),
                ),
                (
                    "scalar_type".to_string(),
                    Value::String(
                        match reference.scalar_type {
                            RelationalScalarType::Text => "text",
                            RelationalScalarType::Bytea => "bytea",
                            RelationalScalarType::Boolean
                            | RelationalScalarType::BigInt
                            | RelationalScalarType::DoublePrecision => "invalid",
                        }
                        .to_string(),
                    ),
                ),
                (
                    "compressed_bytes".to_string(),
                    Value::Int(reference.compressed_bytes as i64),
                ),
                (
                    "uncompressed_bytes".to_string(),
                    Value::Int(reference.uncompressed_bytes as i64),
                ),
            ])),
        ),
    };
    Value::Map(BTreeMap::from([
        ("kind".to_string(), Value::String(kind.to_string())),
        ("value".to_string(), value),
    ]))
}

struct OwnedLocatorBinding {
    table: String,
    qualifier: String,
    key: Option<RelationalKey>,
}

fn project_locator(
    locator: &Value,
    select: &SelectStatement,
    state: &RelationalState,
    hydration: &mut RelationalHydrationBudget,
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<Row> {
    with_locator_bound_row(locator, state, |bound| {
        project_bound_row(bound, &select.projection, state, hydration, task_context)
    })
}

fn with_locator_bound_row<T>(
    locator: &Value,
    state: &RelationalState,
    visit: impl FnOnce(&BoundRow<'_>) -> Result<T>,
) -> Result<T> {
    let bindings = decode_locator(locator)?;
    let mut bound = BoundRow {
        bindings: Vec::with_capacity(bindings.len()),
    };
    for binding in &bindings {
        let schema = state.table_schema(&binding.table).ok_or_else(|| {
            SkeinError::Storage(format!(
                "relational spill locator references unknown table {}",
                binding.table
            ))
        })?;
        let (key, row) = match &binding.key {
            Some(key) => state
                .row_entry(&binding.table, key)
                .map(|(key, row)| (Some(key), Some(row)))
                .ok_or_else(|| {
                    SkeinError::Storage(format!(
                        "relational spill locator references a missing row in table {}",
                        binding.table
                    ))
                })?,
            None => (None, None),
        };
        bound.bindings.push(Binding {
            table: &binding.table,
            qualifier: &binding.qualifier,
            schema,
            key,
            row,
        });
    }
    visit(&bound)
}

fn decode_locator(value: &Value) -> Result<Vec<OwnedLocatorBinding>> {
    let Value::List(bindings) = value else {
        return Err(SkeinError::Execution(
            "relational spill locator is not a list".to_string(),
        ));
    };
    bindings
        .iter()
        .map(|binding| {
            let Value::Map(binding) = binding else {
                return Err(SkeinError::Execution(
                    "relational spill binding locator is not a map".to_string(),
                ));
            };
            let table = locator_string(binding, "table")?;
            let qualifier = locator_string(binding, "qualifier")?;
            let key = match binding.get("key") {
                Some(Value::Null) => None,
                Some(Value::List(values)) => Some(RelationalKey(
                    values
                        .iter()
                        .map(decode_locator_value)
                        .collect::<Result<Vec<_>>>()?,
                )),
                _ => {
                    return Err(SkeinError::Execution(
                        "relational spill locator has an invalid key".to_string(),
                    ))
                }
            };
            Ok(OwnedLocatorBinding {
                table,
                qualifier,
                key,
            })
        })
        .collect()
}

fn locator_string(values: &BTreeMap<String, Value>, name: &str) -> Result<String> {
    match values.get(name) {
        Some(Value::String(value)) => Ok(value.clone()),
        _ => Err(SkeinError::Execution(format!(
            "relational spill locator is missing string field {name}"
        ))),
    }
}

fn decode_locator_value(value: &Value) -> Result<RelationalValue> {
    let Value::Map(fields) = value else {
        return Err(SkeinError::Execution(
            "relational spill key value is not a map".to_string(),
        ));
    };
    let kind = locator_string(fields, "kind")?;
    let value = fields.get("value").ok_or_else(|| {
        SkeinError::Execution("relational spill key value is missing payload".to_string())
    })?;
    match (kind.as_str(), value) {
        ("null", Value::Null) => Ok(RelationalValue::Null),
        ("bool", Value::Bool(value)) => Ok(RelationalValue::Boolean(*value)),
        ("int", Value::Int(value)) => Ok(RelationalValue::BigInt(*value)),
        ("float", Value::Float(value)) => Ok(RelationalValue::DoublePrecision(*value)),
        ("text", Value::String(value)) => Ok(RelationalValue::Text(value.clone())),
        ("bytea", Value::String(value)) => Ok(RelationalValue::Bytea(hex_decode(value)?)),
        _ => Err(SkeinError::Execution(format!(
            "relational spill key has unsupported value kind {kind}"
        ))),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(SkeinError::Execution(
            "relational spill byte string has an odd length".to_string(),
        ));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(SkeinError::Execution(
            "relational spill byte string contains invalid hex".to_string(),
        )),
    }
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
    pipeline: &mut RelationalPipelineState<'_>,
    limits: RelationalQueryLimits,
) -> Result<StreamingProjectionOutput> {
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
    let mut hydration = limits.hydration;
    let task_context = pipeline.task_context;
    if requested != 0 {
        visit_relational_rows(
            select,
            parameters,
            state,
            base_schema,
            base_qualifier,
            base_access,
            joins,
            pipeline,
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
                let projected = project_bound_row(
                    &row,
                    &select.projection,
                    state,
                    &mut hydration,
                    task_context,
                )?;
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
        rows: output,
        hydration,
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
    pipeline: &mut RelationalPipelineState<'a>,
    limits: RelationalQueryLimits,
    execution_memory: &skein_executor::ExecutionMemoryConfig,
    access_path: RelationalAccessPathDescriptor,
    join_access_paths: Vec<RelationalAccessPathDescriptor>,
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
            pipeline,
            limits,
            execution_memory,
            access_path,
            join_access_paths,
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
            pipeline,
            limits,
            execution_memory,
            access_path,
            join_access_paths,
        );
    }
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
    let mut groups = BTreeMap::<Vec<RelationalValue>, Vec<AggregateProjectionState>>::new();
    let mut memory_tracker = OperatorMemoryTracker::new(limits.blocking_operator_bytes);
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
        pipeline,
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
        rows: output,
        intermediate_rows,
        hydration: limits.hydration,
        access_path,
        join_access_paths,
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
    pipeline: &mut RelationalPipelineState<'a>,
    limits: RelationalQueryLimits,
    execution_memory: &skein_executor::ExecutionMemoryConfig,
    access_path: RelationalAccessPathDescriptor,
    join_access_paths: Vec<RelationalAccessPathDescriptor>,
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
        pipeline,
        batch_rows: limits.batch_rows.get(),
    };
    let mut count = 0usize;
    stream_distinct_batches(
        &input_plan,
        &mut source,
        BlockingExecutionContext {
            catalog: &catalog,
            memory: execution_memory,
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
        rows: vec![row],
        intermediate_rows: pipeline.intermediate_rows,
        hydration: limits.hydration,
        access_path,
        join_access_paths,
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
    pipeline: &mut RelationalPipelineState<'a>,
    limits: RelationalQueryLimits,
    execution_memory: &skein_executor::ExecutionMemoryConfig,
    access_path: RelationalAccessPathDescriptor,
    join_access_paths: Vec<RelationalAccessPathDescriptor>,
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
    let input_plan = relational_input_plan();
    let items = select
        .group_by
        .iter()
        .enumerate()
        .map(|(ordinal, _)| SortItem {
            key: SortKey::Column(relational_sort_column(ordinal)),
            direction: SortDirection::Asc,
        })
        .collect::<Vec<_>>();
    let catalog = Catalog::default();
    let observer = RelationalBlockingObserver::default();
    let task_context = pipeline.task_context;
    let mut source = GroupLocatorBatchSource {
        select,
        parameters,
        state,
        base_schema,
        base_qualifier,
        base_access,
        joins,
        pipeline,
        batch_rows: limits.batch_rows.get(),
    };
    let mut current_key = None::<Vec<RelationalValue>>;
    let mut current_group = None::<Vec<AggregateProjectionState>>;
    let mut tracker = OperatorMemoryTracker::new(limits.blocking_operator_bytes);
    let mut input_rows = 0usize;
    let mut output = Vec::new();
    let mut payload_bytes = 0usize;
    let mut stopped = false;
    stream_sort_batches(
        &input_plan,
        &items,
        &mut source,
        BlockingExecutionContext {
            catalog: &catalog,
            memory: execution_memory,
            task_context,
            observer: &observer,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            for mut binding in batch {
                input_rows = input_rows.saturating_add(1);
                let locator = binding
                    .values
                    .remove(RELATIONAL_LOCATOR_COLUMN)
                    .ok_or_else(|| {
                        SkeinError::Execution(
                            "relational aggregate spill row is missing its locator".to_string(),
                        )
                    })?;
                let keep_going = with_locator_bound_row(&locator, state, |row| {
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
                if !keep_going {
                    stopped = true;
                    return Ok(BatchControl::Stop);
                }
            }
            Ok(BatchControl::Continue)
        },
    )?;
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
        rows: output,
        intermediate_rows: pipeline.intermediate_rows,
        hydration: limits.hydration,
        access_path,
        join_access_paths,
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

fn expression_contains_aggregate(expression: &SqlExpression) -> bool {
    match expression {
        SqlExpression::Function {
            name, arguments, ..
        } => {
            matches!(name.as_str(), "count" | "sum" | "max")
                || arguments.iter().any(|argument| match argument {
                    SqlFunctionArgument::Expression(expression) => {
                        expression_contains_aggregate(expression)
                    }
                    SqlFunctionArgument::Wildcard => false,
                })
        }
        SqlExpression::Column(_) | SqlExpression::Value(_) => false,
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
    tracker.charge(bytes);
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

fn predicate_truth(
    predicate: &SqlPredicate,
    row: &BoundRow<'_>,
    parameters: &[Value],
) -> Result<Option<bool>> {
    match predicate {
        SqlPredicate::And(left, right) => match (
            predicate_truth(left, row, parameters)?,
            predicate_truth(right, row, parameters)?,
        ) {
            (Some(false), _) | (_, Some(false)) => Ok(Some(false)),
            (Some(true), Some(true)) => Ok(Some(true)),
            _ => Ok(None),
        },
        SqlPredicate::Or(left, right) => match (
            predicate_truth(left, row, parameters)?,
            predicate_truth(right, row, parameters)?,
        ) {
            (Some(true), _) | (_, Some(true)) => Ok(Some(true)),
            (Some(false), Some(false)) => Ok(Some(false)),
            _ => Ok(None),
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
    if matches!(left, RelationalValue::Overflow(_)) || matches!(right, RelationalValue::Overflow(_))
    {
        return Err(SkeinError::Execution(
            "relational filter or join requires overflow hydration before qualification"
                .to_string(),
        ));
    }
    if matches!(left, RelationalValue::Null) || matches!(right, RelationalValue::Null) {
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
    Ok(binding
        .row
        .map_or(&RelationalValue::Null, |row| &row.values()[position]))
}

fn project_bound_row(
    row: &BoundRow<'_>,
    projection: &[SelectProjection],
    state: &RelationalState,
    hydration: &mut RelationalHydrationBudget,
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<Row> {
    let mut output = Row::new();
    let mut hydrated = BTreeMap::<usize, RelationalRow>::new();
    for item in projection {
        match item {
            SelectProjection::Wildcard => {
                for (binding_index, binding) in row.bindings.iter().enumerate() {
                    for (position, column) in binding.schema.columns.iter().enumerate() {
                        let value = projected_value(
                            binding_index,
                            position,
                            binding,
                            state,
                            hydration,
                            &mut hydrated,
                            task_context,
                        )?;
                        insert_output(&mut output, column.name.clone(), value)?;
                    }
                }
            }
            SelectProjection::Column { name, alias } => {
                let (binding_index, binding, position) = resolve_binding(row, name)?;
                let value = projected_value(
                    binding_index,
                    position,
                    binding,
                    state,
                    hydration,
                    &mut hydrated,
                    task_context,
                )?;
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

fn projected_value(
    binding_index: usize,
    position: usize,
    binding: &Binding<'_>,
    state: &RelationalState,
    hydration: &mut RelationalHydrationBudget,
    hydrated: &mut BTreeMap<usize, RelationalRow>,
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<Value> {
    let Some(row) = binding.row else {
        return Ok(Value::Null);
    };
    let value = &row.values()[position];
    if !matches!(value, RelationalValue::Overflow(_)) {
        return relational_to_value(value);
    }
    if let std::collections::btree_map::Entry::Vacant(entry) = hydrated.entry(binding_index) {
        let key = binding.key.expect("present row has a primary key");
        let row = state
            .hydrate_row_with_context(binding.table, key, hydration, task_context)
            .map_err(|error| SkeinError::Execution(error.to_string()))?
            .ok_or_else(|| {
                SkeinError::Storage(format!(
                    "relational row disappeared from pinned table {}",
                    binding.table
                ))
            })?;
        entry.insert(row);
    }
    relational_to_value(&hydrated[&binding_index].values()[position])
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
        RelationalValue::Bytea(_) => Err(SkeinError::Semantic(
            "BYTEA result conversion requires a binary Value variant".to_string(),
        )),
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
