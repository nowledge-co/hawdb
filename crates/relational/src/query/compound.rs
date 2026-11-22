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

use super::expression::{evaluate_projection_expression, relational_to_value};
use super::{
    account_intermediate, bind_bound, execute_select_statement,
    prepare_relational_select_with_snapshot, push_relational_output, select_output_schema,
    validate_scalar_expressions, HawDBError, OperatorMemoryTracker, QueryMemoryLedger,
    RelationalAccessPathDescriptor, RelationalAccessPathKind, RelationalJoinEnumerationConfig,
    RelationalJoinPlanningOutcome, RelationalJoinPlanningReason, RelationalQueryOutput,
    RelationalQueryReadModes, RelationalQueryResourceContext, RelationalQueryStoreReader,
    RelationalRowExecutionEvidence, RelationalSqlStageTimings, RelationalState, Result, Value,
};
use crate::index_runtime::RelationalIndexExecutionEvidence;
use hawdb_executor::{QueryMemoryClass, QueryRows, QuerySchema, Row};
use hawdb_sql::{
    Expr, ExprKind, SelectProjection, SelectStatement, SqlBound, SqlCommonTableExpression,
    SqlCommonTableExpressionBody, SqlFunctionArgument, SqlNullOrder, SqlOrderDirection,
    SqlOrderItem, SqlQueryBody, SqlQueryStatement, SqlSetOperator, SqlSubqueryPredicate, SqlValue,
};
use hawdb_storage::relational::{
    RelationalColumnSchema, RelationalIndexReadLimits, RelationalRow,
    RelationalRowPageSnapshotReadLimits, RelationalScalarType, RelationalTableSchema,
    RelationalValue,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::num::NonZeroUsize;

mod subquery;

const MAX_RECURSIVE_CTE_ITERATIONS: usize = 1_024;

pub(super) fn execute_query<'a, R: RelationalQueryStoreReader>(
    query: SqlQueryStatement,
    parameters: &[Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<RelationalQueryOutput> {
    execute_query_typed(
        query,
        parameters,
        state,
        read_modes,
        resources,
        stage_timings,
    )
    .map(|output| output.output)
}

pub(super) fn explain_query<'a, R: RelationalQueryStoreReader>(
    query: SqlQueryStatement,
    parameters: &[Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
    analyze: bool,
) -> Result<RelationalQueryOutput> {
    let summary = query_plan_summary(&query);
    let mut output = if analyze {
        execute_query(
            query,
            parameters,
            state,
            read_modes,
            resources,
            stage_timings,
        )?
    } else {
        let plan_started = hawdb_core::time::Instant::now();
        let planning_resources = RelationalQueryResourceContext {
            task_context: None,
            ..resources
        };
        plan_query_typed(
            &query,
            parameters,
            state,
            read_modes,
            planning_resources,
            stage_timings,
        )?;
        let mut explain_timings = stage_timings;
        explain_timings.plan_nanos = explain_timings
            .plan_nanos
            .saturating_add(hawdb_sql::timing::elapsed_nanos(plan_started));
        compound_plan_output(summary, resources, explain_timings)?
    };
    let actual_rows = output.rows.len();
    let schema = crate::explain::sql_explain_schema(analyze);
    let mut row = Row::new();
    row.insert("id".into(), Value::String(summary.operator.into()));
    row.insert(
        "estRows".into(),
        Value::Int(i64::try_from(output.access_path.estimated_rows.max(1)).unwrap_or(i64::MAX)),
    );
    row.insert("task".into(), Value::String("root".into()));
    row.insert(
        "access object".into(),
        Value::String(output.access_path.name.clone()),
    );
    let mut operator_info = format!(
        "implementation=bounded, ctes={}, recursive_ctes={}, set_operations={}, values={}, materialization=statement_scoped",
        summary.ctes, summary.recursive_ctes, summary.set_operations, summary.values
    );
    if summary.recursive_ctes > 0 {
        operator_info.push_str(&format!(
            ", recursive_strategy=working_table, recursive_iteration_limit={MAX_RECURSIVE_CTE_ITERATIONS}"
        ));
    }
    if summary.subqueries > 0 {
        operator_info.push_str(&format!(
            ", subqueries={}, subquery_strategy=budgeted_nested_loop, scalar_in=hash_if_eligible",
            summary.subqueries
        ));
    }
    row.insert("operator info".into(), Value::String(operator_info));
    if analyze {
        let peak_memory = output
            .blocking_operator_memory_reports
            .iter()
            .map(|report| report.peak_tracked_bytes)
            .max()
            .unwrap_or_default();
        let memory_budget = output
            .blocking_operator_memory_reports
            .iter()
            .map(|report| report.budget_bytes)
            .min()
            .unwrap_or(resources.execution_memory.blocking_operator_bytes.get());
        let spilled_bytes = output
            .blocking_operator_memory_reports
            .iter()
            .fold(0u64, |total, report| {
                total.saturating_add(report.spilled_bytes)
            });
        row.insert(
            "actRows".into(),
            Value::Int(i64::try_from(actual_rows).unwrap_or(i64::MAX)),
        );
        let execution_info = if summary.recursive_ctes == 0 {
            "executed=true".into()
        } else {
            format!(
                "executed=true, recursive_iterations={}, recursive_working_rows_peak={}, recursive_rows_emitted={}, recursive_work={}",
                output.row_execution_evidence.recursive_iterations,
                output.row_execution_evidence.recursive_working_rows_peak,
                output.row_execution_evidence.recursive_rows_emitted,
                output.row_execution_evidence.recursive_work,
            )
        };
        row.insert("execution info".into(), Value::String(execution_info));
        row.insert(
            "memory".into(),
            Value::String(format!(
                "peak={peak_memory}/budget={memory_budget}, operators={}",
                output.blocking_operator_memory_reports.len()
            )),
        );
        row.insert(
            "disk".into(),
            Value::String(format!("{spilled_bytes} Bytes")),
        );
    }
    let mut rows = Vec::new();
    let mut payload_bytes = 0;
    super::push_relational_output(row, &mut rows, &mut payload_bytes, resources.limits)?;
    output.rows = QueryRows::try_from_named_rows(schema, rows)?;
    Ok(output)
}

struct PlannedOutput {
    schema: QuerySchema,
    types: Vec<Option<RelationalScalarType>>,
}

fn plan_query_typed<'a, R: RelationalQueryStoreReader>(
    query: &SqlQueryStatement,
    parameters: &[Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<PlannedOutput> {
    if query.contains_locking_clause() {
        return Err(HawDBError::Semantic(
            "locking clauses are not supported on CTE or set-operation queries".into(),
        ));
    }
    let mut scoped_state = (!query.with.is_empty()).then(|| state.clone());
    let mut cte_tables = BTreeSet::new();
    if let Some(scoped_state) = &mut scoped_state {
        for cte in &query.with {
            if cte_tables.contains(&cte.name) {
                return Err(HawDBError::Semantic(format!(
                    "common table expression {} is specified more than once",
                    cte.name
                )));
            }
            let planned = if recursive_cte_parts(query.recursive, cte)?.is_some() {
                let cte_read_modes = read_modes.with_memory_overlay(&cte_tables);
                plan_recursive_cte(
                    cte,
                    query.recursive,
                    parameters,
                    scoped_state,
                    cte_read_modes,
                    resources,
                    stage_timings,
                )?
            } else {
                let SqlCommonTableExpressionBody::Query(query) = &cte.body else {
                    return Err(HawDBError::Semantic(
                        "data-modifying CTEs require the mutation statement executor".into(),
                    ));
                };
                let cte_read_modes = read_modes.with_memory_overlay(&cte_tables);
                plan_query_typed(
                    query,
                    parameters,
                    scoped_state,
                    cte_read_modes,
                    resources,
                    stage_timings,
                )?
            };
            let schema = cte_table_schema(cte, &planned.schema, &planned.types)?;
            scoped_state
                .attach_ephemeral_query_table(schema, Vec::new())
                .map_err(HawDBError::from_execution_error)?;
            cte_tables.insert(cte.name.clone());
        }
    }
    let (state, read_modes) = match &scoped_state {
        Some(scoped_state) => (scoped_state, read_modes.with_memory_overlay(&cte_tables)),
        None => (state, read_modes),
    };
    let planned = plan_body_typed(
        &query.body,
        parameters,
        state,
        read_modes,
        resources,
        stage_timings,
    )?;
    for item in &query.order_by {
        resolve_order(item, &planned.schema)?;
    }
    bind_bound(query.limit, parameters, "LIMIT")?;
    bind_bound(query.offset, parameters, "OFFSET")?;
    Ok(planned)
}

fn plan_recursive_cte<'a, R: RelationalQueryStoreReader>(
    cte: &SqlCommonTableExpression,
    with_recursive: bool,
    parameters: &[Value],
    state: &mut RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<PlannedOutput> {
    let Some((seed, recursive, _)) = recursive_cte_parts(with_recursive, cte)? else {
        return Err(HawDBError::Execution(
            "recursive CTE planner received a non-recursive CTE".into(),
        ));
    };
    let seed = plan_body_typed(
        seed,
        parameters,
        state,
        read_modes,
        resources,
        stage_timings,
    )?;
    let seed_schema = cte_table_schema(cte, &seed.schema, &seed.types)?;
    state
        .attach_ephemeral_query_table(seed_schema, Vec::new())
        .map_err(HawDBError::from_execution_error)?;
    let mut recursive_tables = BTreeSet::new();
    recursive_tables.insert(cte.name.clone());
    let recursive_read_modes = read_modes.with_memory_overlay(&recursive_tables);
    let recursive = plan_body_typed(
        recursive,
        parameters,
        state,
        recursive_read_modes,
        resources,
        RelationalSqlStageTimings::default(),
    )?;
    if seed.schema.len() != recursive.schema.len() {
        return Err(HawDBError::Semantic(format!(
            "recursive common table expression {} has seed arity {} and recursive-term arity {}",
            cte_display_name(&cte.name),
            seed.schema.len(),
            recursive.schema.len()
        )));
    }
    let types = seed
        .types
        .iter()
        .copied()
        .zip(recursive.types.iter().copied())
        .enumerate()
        .map(|(column, (left, right))| common_type(left, right, column + 1))
        .collect::<Result<Vec<_>>>()?;
    let schema = QuerySchema::try_new(if cte.columns.is_empty() {
        seed.schema.columns().to_vec()
    } else {
        cte.columns.clone()
    })?;
    let final_schema = cte_table_schema(cte, &schema, &types)?;
    state
        .attach_ephemeral_query_table(final_schema, Vec::new())
        .map_err(HawDBError::from_execution_error)?;
    Ok(PlannedOutput { schema, types })
}

fn plan_body_typed<'a, R: RelationalQueryStoreReader>(
    body: &SqlQueryBody,
    parameters: &[Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<PlannedOutput> {
    match body {
        SqlQueryBody::Select(select) => {
            let mut select = (**select).clone();
            crate::literal_coercion::coerce_select(&mut select, state, parameters)?;
            if select.text_search().is_some() {
                super::text_search::explain_text_search_select(
                    &select,
                    parameters,
                    state,
                    read_modes,
                    resources,
                    stage_timings,
                    false,
                )?;
            } else if select.from.is_some() {
                prepare_relational_select_with_snapshot(
                    select.clone(),
                    parameters,
                    state,
                    read_modes,
                    resources,
                    stage_timings,
                )?;
            } else {
                validate_scalar_expressions(&select, parameters, state)?;
                let mut invalid_column = None;
                for projection in &select.projection {
                    let SelectProjection::Expression { expression, .. } = projection else {
                        continue;
                    };
                    expression.visit(&mut |expression| {
                        if invalid_column.is_none()
                            && let ExprKind::Column(column) = &expression.kind
                        {
                            invalid_column = Some(column.name.clone());
                        }
                    });
                }
                if let Some(column) = invalid_column {
                    return Err(HawDBError::Semantic(format!(
                        "unknown relational column {column}"
                    )));
                }
            }
            let schema = select_output_schema(&select, state)?;
            let types = select_output_types(&select, parameters, state)?;
            Ok(PlannedOutput { schema, types })
        }
        SqlQueryBody::Values(rows) => plan_values(rows, parameters, state),
        SqlQueryBody::Nested(query) => plan_query_typed(
            query,
            parameters,
            state,
            read_modes,
            resources,
            stage_timings,
        ),
        SqlQueryBody::SetOperation { left, right, .. } => {
            let left = plan_body_typed(
                left,
                parameters,
                state,
                read_modes,
                resources,
                stage_timings,
            )?;
            let right = plan_body_typed(
                right,
                parameters,
                state,
                read_modes,
                resources,
                RelationalSqlStageTimings::default(),
            )?;
            if left.schema.len() != right.schema.len() {
                return Err(HawDBError::Semantic(format!(
                    "set-operation inputs have arity {} and {}",
                    left.schema.len(),
                    right.schema.len()
                )));
            }
            let types = left
                .types
                .iter()
                .copied()
                .zip(right.types.iter().copied())
                .enumerate()
                .map(|(column, (left, right))| common_type(left, right, column + 1))
                .collect::<Result<Vec<_>>>()?;
            Ok(PlannedOutput {
                schema: left.schema,
                types,
            })
        }
    }
}

fn values_select(row: &[hawdb_sql::SqlExpression]) -> SelectStatement {
    SelectStatement {
        table_function: None,
        projection: row
            .iter()
            .enumerate()
            .map(|(index, expression)| SelectProjection::Expression {
                expression: expression.clone(),
                alias: Some(format!("column{}", index + 1)),
            })
            .collect(),
        distinct: false,
        from: None,
        from_alias: None,
        joins: Vec::new(),
        selection: None,
        subquery_predicate: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        limit: None,
        offset: None,
        lock_strength: None,
    }
}

fn plan_values(
    rows: &[Vec<hawdb_sql::SqlExpression>],
    parameters: &[Value],
    state: &RelationalState,
) -> Result<PlannedOutput> {
    let Some(first) = rows.first() else {
        return Err(HawDBError::Semantic(
            "VALUES requires at least one row".into(),
        ));
    };
    let arity = first.len();
    if arity == 0 {
        return Err(HawDBError::Semantic(
            "VALUES rows require at least one expression".into(),
        ));
    }
    let schema = QuerySchema::try_new(
        (1..=arity)
            .map(|index| format!("column{index}"))
            .collect::<Vec<_>>(),
    )?;
    let mut types = vec![None; arity];
    for (row_index, row) in rows.iter().enumerate() {
        if row.len() != arity {
            return Err(HawDBError::Semantic(format!(
                "VALUES row {} has arity {}, expected {arity}",
                row_index + 1,
                row.len()
            )));
        }
        let select = values_select(row);
        validate_scalar_expressions(&select, parameters, state)?;
        let row_types = select_output_types(&select, parameters, state)?;
        for (column, candidate) in row_types.into_iter().enumerate() {
            types[column] = common_type(types[column], candidate, column + 1)?;
        }
    }
    Ok(PlannedOutput { schema, types })
}

fn execute_values(
    expressions: Vec<Vec<hawdb_sql::SqlExpression>>,
    parameters: &[Value],
    state: &RelationalState,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<TypedOutput> {
    let planned = plan_values(&expressions, parameters, state)?;
    if expressions.len() > resources.limits.max_candidate_work {
        return Err(super::expression::work_budget_exceeded(
            "max_candidate_work",
            0,
            expressions.len(),
            resources.limits.max_candidate_work,
        ));
    }
    if expressions.len() > resources.limits.max_intermediate_rows {
        return Err(super::expression::work_budget_exceeded(
            "max_intermediate_rows",
            0,
            expressions.len(),
            resources.limits.max_intermediate_rows,
        ));
    }
    let ledger = QueryMemoryLedger::for_query(resources.execution_memory, resources.task_context)?;
    let mut memory_tracker = OperatorMemoryTracker::with_account(
        resources.execution_memory.blocking_operator_bytes,
        ledger.account(
            QueryMemoryClass::BlockingState,
            "relational VALUES materialization",
            resources.execution_memory.blocking_operator_bytes,
        ),
    );
    let empty = super::BoundRow::default();
    let mut rows = Vec::with_capacity(expressions.len());
    let mut payload_bytes = 0usize;
    for expressions in &expressions {
        let mut row = Row::new();
        for (index, expression) in expressions.iter().enumerate() {
            let value = relational_to_value(&evaluate_projection_expression(
                expression, &empty, parameters,
            )?)?;
            row.insert(format!("column{}", index + 1), value);
        }
        let retained =
            size_of::<Row>().saturating_add(hawdb_executor::binding::map_payload_bytes(&row));
        memory_tracker.try_charge(retained)?;
        push_relational_output(row, &mut rows, &mut payload_bytes, resources.limits)?;
    }
    let rows = QueryRows::try_from_named_rows(planned.schema, rows)?;
    let rows = coerce_rows(&rows, &planned.types)?;
    let mut output = compound_plan_output(
        QueryPlanSummary {
            operator: "ValuesExec",
            ctes: 0,
            recursive_ctes: 0,
            set_operations: 0,
            values: 1,
            subqueries: 0,
        },
        resources,
        stage_timings,
    )?;
    output.intermediate_rows = rows.len();
    output.rows = rows;
    output.row_execution_evidence.runtime_path = "values";
    output
        .blocking_operator_memory_reports
        .push(hawdb_executor::blocking::in_memory_report(
            "ValuesExec",
            &memory_tracker,
            memory_tracker.peak_bytes,
            expressions.len(),
            resources.execution_memory,
        ));
    Ok(TypedOutput {
        output,
        types: planned.types,
    })
}

fn compound_plan_output(
    summary: QueryPlanSummary,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<RelationalQueryOutput> {
    let access_path = RelationalAccessPathDescriptor {
        kind: RelationalAccessPathKind::FullScan,
        name: summary.access_name(),
        index_columns: Vec::new(),
        access_columns: BTreeSet::new(),
        equality_prefix_len: 0,
        order_prefix_len: 0,
        exclusive_range: false,
        range: None,
        reverse_order: false,
        unique_point: false,
        covering: false,
        requires_row_fetch: false,
        // Keep EXPLAIN's established non-zero estimate contract when a
        // compound node has no single base-table estimate to inherit.
        estimated_rows: 1,
    };
    Ok(RelationalQueryOutput {
        rows: QueryRows::try_from_named_rows(hawdb_executor::QuerySchema::empty(), Vec::new())?,
        stage_timings,
        join_planning: RelationalJoinPlanningOutcome::not_eligible(
            RelationalJoinPlanningReason::NoJoin,
            Vec::new(),
            RelationalJoinEnumerationConfig::default(),
        ),
        operator_cardinality_profiles: Vec::new(),
        intermediate_rows: 0,
        hydration: resources.limits.hydration,
        access_path,
        join_access_paths: Vec::new(),
        index_execution_evidence: Vec::new(),
        row_execution_evidence: RelationalRowExecutionEvidence {
            runtime_path: "not_executed",
            ..RelationalRowExecutionEvidence::default()
        },
        blocking_operator_memory_reports: Vec::new(),
        search_index_scan: None,
    })
}

#[derive(Clone, Copy)]
struct QueryPlanSummary {
    operator: &'static str,
    ctes: usize,
    recursive_ctes: usize,
    set_operations: usize,
    values: usize,
    subqueries: usize,
}

impl QueryPlanSummary {
    fn access_name(self) -> String {
        let base = match (
            self.ctes,
            self.recursive_ctes,
            self.set_operations,
            self.values,
        ) {
            (0, 0, 0, 0) => "Query".into(),
            (0, 0, 0, values) => format!("Values({values})"),
            (0, 0, set_operations, _) => format!("SetOperation({set_operations})"),
            (ctes, 0, 0, _) => format!("CTE Materialize({ctes})"),
            (ctes, 0, set_operations, _) => {
                format!("CTE Materialize({ctes}) -> SetOperation({set_operations})")
            }
            (ctes, recursive, 0, _) => {
                format!("RecursiveUnion({recursive}) -> CTE Materialize({ctes})")
            }
            (ctes, recursive, set_operations, _) => format!(
                "RecursiveUnion({recursive}) -> CTE Materialize({ctes}) -> SetOperation({set_operations})"
            ),
        };
        if self.subqueries == 0 {
            base
        } else {
            format!("SubqueryPredicate({}) -> {base}", self.subqueries)
        }
    }
}

fn query_plan_summary(query: &SqlQueryStatement) -> QueryPlanSummary {
    fn summarize_body(body: &SqlQueryBody) -> (usize, usize, usize, usize, usize) {
        match body {
            SqlQueryBody::Select(select) => {
                let mut subqueries = 0usize;
                if let Some(predicate) = &select.subquery_predicate {
                    predicate.visit_queries(&mut |query| {
                        let summary = query_plan_summary(query);
                        subqueries = subqueries
                            .saturating_add(1)
                            .saturating_add(summary.subqueries);
                    });
                }
                (0, 0, 0, 0, subqueries)
            }
            SqlQueryBody::Values(_) => (0, 0, 0, 1, 0),
            SqlQueryBody::Nested(query) => {
                let summary = query_plan_summary(query);
                (
                    summary.ctes,
                    summary.recursive_ctes,
                    summary.set_operations,
                    summary.values,
                    summary.subqueries,
                )
            }
            SqlQueryBody::SetOperation { left, right, .. } => {
                let left = summarize_body(left);
                let right = summarize_body(right);
                (
                    left.0.saturating_add(right.0),
                    left.1.saturating_add(right.1),
                    1usize.saturating_add(left.2).saturating_add(right.2),
                    left.3.saturating_add(right.3),
                    left.4.saturating_add(right.4),
                )
            }
        }
    }
    let live_ctes = live_cte_names(query);
    let nested = query
        .with
        .iter()
        .filter(|cte| live_ctes.contains(&cte.name))
        .filter_map(|cte| match &cte.body {
            SqlCommonTableExpressionBody::Query(query) => Some(query_plan_summary(query)),
            SqlCommonTableExpressionBody::Mutation(_) => None,
        })
        .fold(
            (0usize, 0usize, 0usize, 0usize, 0usize),
            |(ctes, recursive, sets, values, subqueries), summary| {
                (
                    ctes.saturating_add(summary.ctes),
                    recursive.saturating_add(summary.recursive_ctes),
                    sets.saturating_add(summary.set_operations),
                    values.saturating_add(summary.values),
                    subqueries.saturating_add(summary.subqueries),
                )
            },
        );
    let body = summarize_body(&query.body);
    let recursive_ctes = query
        .with
        .iter()
        .filter(|cte| live_ctes.contains(&cte.name) && cte_is_recursive(query.recursive, cte))
        .count()
        .saturating_add(nested.1)
        .saturating_add(body.1);
    let set_operations = nested.2.saturating_add(body.2);
    let values = nested.3.saturating_add(body.3);
    let subqueries = nested.4.saturating_add(body.4);
    let ctes = live_ctes
        .len()
        .saturating_add(nested.0)
        .saturating_add(body.0);
    QueryPlanSummary {
        operator: match (ctes, set_operations, values, subqueries) {
            _ if recursive_ctes > 0 => "RecursiveUnionExec",
            (0, 0, 0, 0) => "QueryExec",
            (0, 0, 0, _) => "SubqueryPredicateExec",
            (0, 0, _, _) => "ValuesExec",
            (0, _, _, _) => "SetOperationExec",
            (_, _, _, _) => "CteMaterializeExec",
        },
        ctes,
        recursive_ctes,
        set_operations,
        values,
        subqueries,
    }
}

fn live_cte_names(query: &SqlQueryStatement) -> BTreeSet<String> {
    let names = query
        .with
        .iter()
        .map(|cte| cte.name.as_str())
        .collect::<Vec<_>>();
    let mut needed = body_external_relations(&query.body);
    let mut live = BTreeSet::new();
    for (index, cte) in query.with.iter().enumerate().rev() {
        if !needed.contains(&cte.name) {
            continue;
        }
        live.insert(cte.name.clone());
        let dependencies = match &cte.body {
            SqlCommonTableExpressionBody::Query(query) => query_external_relations(query),
            SqlCommonTableExpressionBody::Mutation(_) => BTreeSet::new(),
        };
        for name in &names[..index] {
            if dependencies.contains(*name) {
                needed.insert((*name).to_owned());
            }
        }
    }
    live
}

fn cte_is_recursive(with_recursive: bool, cte: &SqlCommonTableExpression) -> bool {
    if !with_recursive {
        return false;
    }
    match &cte.body {
        SqlCommonTableExpressionBody::Query(query) => {
            query_external_relations(query).contains(&cte.name)
        }
        SqlCommonTableExpressionBody::Mutation(_) => false,
    }
}

fn recursive_cte_parts(
    with_recursive: bool,
    cte: &SqlCommonTableExpression,
) -> Result<Option<(&SqlQueryBody, &SqlQueryBody, bool)>> {
    let SqlCommonTableExpressionBody::Query(query) = &cte.body else {
        return Ok(None);
    };
    if !cte_is_recursive(with_recursive, cte) {
        return Ok(None);
    }
    if !query.with.is_empty()
        || !query.order_by.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
        || query.lock_strength.is_some()
    {
        return Err(HawDBError::Semantic(format!(
            "recursive common table expression {} requires a top-level seed UNION recursive term without a query tail",
            cte_display_name(&cte.name)
        )));
    }
    let SqlQueryBody::SetOperation {
        left,
        operator: SqlSetOperator::Union,
        all,
        right,
    } = &query.body
    else {
        return Err(HawDBError::Semantic(format!(
            "recursive common table expression {} requires seed UNION [ALL] recursive term",
            cte_display_name(&cte.name)
        )));
    };
    if body_external_relations(left).contains(&cte.name) {
        return Err(HawDBError::Semantic(format!(
            "recursive reference to {} is not allowed in the non-recursive seed",
            cte_display_name(&cte.name)
        )));
    }
    if !body_external_relations(right).contains(&cte.name) {
        return Err(HawDBError::Semantic(format!(
            "recursive common table expression {} has no recursive-term reference",
            cte_display_name(&cte.name)
        )));
    }
    Ok(Some((left, right, *all)))
}

fn query_external_relations(query: &SqlQueryStatement) -> BTreeSet<String> {
    let mut external = BTreeSet::new();
    let mut local = BTreeSet::new();
    let with_recursive = query.recursive;
    for cte in &query.with {
        if let SqlCommonTableExpressionBody::Query(cte_query) = &cte.body {
            for relation in query_external_relations(cte_query) {
                if with_recursive && relation == cte.name {
                    continue;
                }
                if !local.contains(&relation) {
                    external.insert(relation);
                }
            }
        }
        local.insert(cte.name.clone());
    }
    for relation in body_external_relations(&query.body) {
        if !local.contains(&relation) {
            external.insert(relation);
        }
    }
    external
}

fn body_external_relations(body: &SqlQueryBody) -> BTreeSet<String> {
    match body {
        SqlQueryBody::Select(select) => {
            let mut relations = select
                .from
                .iter()
                .chain(select.joins.iter().map(|join| &join.table))
                .filter(|table| table.schema.is_none())
                .map(|table| table.name.clone())
                .collect::<BTreeSet<_>>();
            if let Some(predicate) = &select.subquery_predicate {
                predicate.visit_queries(&mut |query| {
                    relations.extend(query_external_relations(query));
                });
            }
            relations
        }
        SqlQueryBody::Values(_) => BTreeSet::new(),
        SqlQueryBody::Nested(query) => query_external_relations(query),
        SqlQueryBody::SetOperation { left, right, .. } => {
            let mut relations = body_external_relations(left);
            relations.extend(body_external_relations(right));
            relations
        }
    }
}

struct TypedOutput {
    output: RelationalQueryOutput,
    types: Vec<Option<RelationalScalarType>>,
}

fn execute_query_typed<'a, R: RelationalQueryStoreReader>(
    query: SqlQueryStatement,
    parameters: &[Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<TypedOutput> {
    if query.with.is_empty() && matches!(query.body, SqlQueryBody::Select(_)) {
        let select = query
            .into_simple_select()
            .expect("simple query shape was checked");
        return execute_body(
            SqlQueryBody::Select(Box::new(select)),
            parameters,
            state,
            read_modes,
            resources,
            stage_timings,
        );
    }
    let live_ctes = live_cte_names(&query);
    let SqlQueryStatement {
        recursive,
        with,
        body,
        order_by,
        limit,
        offset,
        lock_strength,
    } = query;
    if lock_strength.is_some() {
        return Err(HawDBError::Semantic(
            "locking clauses are not supported on CTE or set-operation queries".into(),
        ));
    }
    let mut scoped_resources = resources;
    // CTEs, set-operation branches, and the final body are intermediate until
    // this query level applies its ORDER/OFFSET/LIMIT tail. Do not let the
    // caller's final output-row budget reject rows that the tail will discard,
    // but retain an explicit byte bound for every resident intermediate.
    scoped_resources.limits.max_output_rows = resources.limits.max_intermediate_rows;
    scoped_resources.limits.max_output_payload_bytes =
        resources.execution_memory.query_memory_bytes.get();
    let mut stage_timings = stage_timings;
    let mut cte_stage_timings = RelationalSqlStageTimings::default();
    let mut cte_rows = 0usize;
    let mut recursive_iterations = 0usize;
    let mut recursive_working_rows_peak = 0usize;
    let mut recursive_rows_emitted = 0usize;
    let mut recursive_work = 0usize;
    let mut recursive_memory_reports = Vec::new();
    let mut scoped_state = (!with.is_empty()).then(|| state.clone());
    let mut cte_tables = BTreeSet::new();
    let mut materialized_ctes = BTreeSet::new();
    let mut cte_memory_tracker = None;
    if let Some(scoped_state) = &mut scoped_state {
        let ledger =
            QueryMemoryLedger::for_query(resources.execution_memory, resources.task_context)?;
        cte_memory_tracker = Some(OperatorMemoryTracker::with_account(
            resources.execution_memory.blocking_operator_bytes,
            ledger.account(
                QueryMemoryClass::BlockingState,
                "relational CTE materialization",
                resources.execution_memory.blocking_operator_bytes,
            ),
        ));
        let mut unique_ctes = BTreeSet::new();
        for cte in &with {
            if !unique_ctes.insert(cte.name.as_str()) {
                return Err(HawDBError::Semantic(format!(
                    "common table expression {} is specified more than once",
                    cte.name
                )));
            }
        }
        for cte in with {
            if cte_tables.contains(&cte.name) {
                return Err(HawDBError::Semantic(format!(
                    "common table expression {} is specified more than once",
                    cte.name
                )));
            }
            if !live_ctes.contains(&cte.name) {
                let SqlCommonTableExpressionBody::Query(query) = &cte.body else {
                    return Err(HawDBError::Semantic(
                        "data-modifying CTEs require the mutation statement executor".into(),
                    ));
                };
                let cte_read_modes = read_modes.with_memory_overlay(&cte_tables);
                let planned = if recursive_cte_parts(recursive, &cte)?.is_some() {
                    plan_recursive_cte(
                        &cte,
                        recursive,
                        parameters,
                        scoped_state,
                        cte_read_modes,
                        scoped_resources,
                        stage_timings,
                    )?
                } else {
                    plan_query_typed(
                        query,
                        parameters,
                        scoped_state,
                        cte_read_modes,
                        scoped_resources,
                        stage_timings,
                    )?
                };
                let schema = cte_table_schema(&cte, &planned.schema, &planned.types)?;
                scoped_state
                    .attach_ephemeral_query_table(schema, Vec::new())
                    .map_err(HawDBError::from_execution_error)?;
                cte_tables.insert(cte.name);
                continue;
            }
            let SqlCommonTableExpressionBody::Query(query) = &cte.body else {
                return Err(HawDBError::Semantic(
                    "data-modifying CTEs require the mutation statement executor".into(),
                ));
            };
            let cte_read_modes = read_modes.with_memory_overlay(&cte_tables);
            let cte_output = if recursive_cte_parts(recursive, &cte)?.is_some() {
                execute_recursive_cte(
                    &cte,
                    recursive,
                    parameters,
                    scoped_state,
                    cte_read_modes,
                    scoped_resources,
                    stage_timings,
                )?
            } else {
                execute_query_typed(
                    *query.clone(),
                    parameters,
                    scoped_state,
                    cte_read_modes,
                    scoped_resources,
                    stage_timings,
                )?
            };
            recursive_iterations = recursive_iterations.saturating_add(
                cte_output
                    .output
                    .row_execution_evidence
                    .recursive_iterations,
            );
            recursive_working_rows_peak = recursive_working_rows_peak.max(
                cte_output
                    .output
                    .row_execution_evidence
                    .recursive_working_rows_peak,
            );
            recursive_rows_emitted = recursive_rows_emitted.saturating_add(
                cte_output
                    .output
                    .row_execution_evidence
                    .recursive_rows_emitted,
            );
            recursive_work = recursive_work
                .saturating_add(cte_output.output.row_execution_evidence.recursive_work);
            recursive_memory_reports.extend(
                cte_output
                    .output
                    .blocking_operator_memory_reports
                    .iter()
                    .filter(|report| report.operator == "RecursiveUnionExec")
                    .cloned(),
            );
            add_stage_timings(&mut cte_stage_timings, cte_output.output.stage_timings);
            stage_timings = RelationalSqlStageTimings::default();
            scoped_resources.limits.hydration = cte_output.output.hydration;
            account_intermediate(
                &mut cte_rows,
                cte_output.output.rows.len(),
                resources.limits.max_intermediate_rows,
            )?;
            let retained_bytes = retained_bytes(&cte_output.output.rows)
                .checked_mul(2)
                .ok_or_else(|| HawDBError::Execution("CTE retained bytes overflow".into()))?;
            cte_memory_tracker
                .as_mut()
                .expect("CTE memory tracker was initialized")
                .try_charge(retained_bytes)?;
            materialize_cte(scoped_state, &cte, &cte_output)?;
            materialized_ctes.insert(cte.name.clone());
            cte_tables.insert(cte.name);
        }
    }
    let mut output = match &scoped_state {
        Some(scoped_state) => {
            let scoped_read_modes = read_modes.with_memory_overlay(&cte_tables);
            execute_body(
                body,
                parameters,
                scoped_state,
                scoped_read_modes,
                scoped_resources,
                stage_timings,
            )?
        }
        None => execute_body(
            body,
            parameters,
            state,
            read_modes,
            scoped_resources,
            stage_timings,
        )?,
    };
    apply_query_tail(
        &order_by,
        limit,
        offset,
        parameters,
        resources,
        &mut output.output,
    )?;
    add_stage_timings(&mut output.output.stage_timings, cte_stage_timings);
    output.output.row_execution_evidence.recursive_iterations = output
        .output
        .row_execution_evidence
        .recursive_iterations
        .saturating_add(recursive_iterations);
    output
        .output
        .row_execution_evidence
        .recursive_working_rows_peak = output
        .output
        .row_execution_evidence
        .recursive_working_rows_peak
        .max(recursive_working_rows_peak);
    output.output.row_execution_evidence.recursive_rows_emitted = output
        .output
        .row_execution_evidence
        .recursive_rows_emitted
        .saturating_add(recursive_rows_emitted);
    output.output.row_execution_evidence.recursive_work = output
        .output
        .row_execution_evidence
        .recursive_work
        .saturating_add(recursive_work);
    output
        .output
        .blocking_operator_memory_reports
        .extend(recursive_memory_reports);
    if !materialized_ctes.is_empty() {
        output.output.access_path.name = format!(
            "CTE Materialize({}) -> {}",
            materialized_ctes
                .iter()
                .map(|name| cte_display_name(name))
                .collect::<Vec<_>>()
                .join(", "),
            output.output.access_path.name
        );
        let tracker = cte_memory_tracker
            .as_ref()
            .expect("materialized CTEs have a memory tracker");
        output.output.blocking_operator_memory_reports.push(
            hawdb_executor::blocking::in_memory_report(
                "CteMaterializeExec",
                tracker,
                tracker.peak_bytes,
                cte_rows,
                resources.execution_memory,
            ),
        );
    }
    Ok(output)
}

fn execute_recursive_cte<'a, R: RelationalQueryStoreReader>(
    cte: &SqlCommonTableExpression,
    with_recursive: bool,
    parameters: &[Value],
    state: &mut RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<TypedOutput> {
    let Some((seed_body, recursive_body, union_all)) = recursive_cte_parts(with_recursive, cte)?
    else {
        return Err(HawDBError::Execution(
            "recursive CTE executor received a non-recursive CTE".into(),
        ));
    };
    let planned = plan_recursive_cte(
        cte,
        with_recursive,
        parameters,
        state,
        read_modes,
        resources,
        stage_timings,
    )?;
    let mut seed = execute_body(
        seed_body.clone(),
        parameters,
        state,
        read_modes,
        resources,
        stage_timings,
    )?;
    if seed.output.rows.schema().len() != planned.schema.len() {
        return Err(HawDBError::Semantic(format!(
            "recursive common table expression {} seed has arity {}, expected {}",
            cte_display_name(&cte.name),
            seed.output.rows.schema().len(),
            planned.schema.len()
        )));
    }
    seed.output.rows = coerce_rows(&seed.output.rows, &planned.types)?;
    let mut result = owned_values(&seed.output.rows);
    if !union_all {
        let mut unique = BTreeSet::new();
        result.retain(|row| unique.insert(row.clone()));
    }
    let mut seen = if union_all {
        BTreeSet::new()
    } else {
        result.iter().cloned().collect()
    };
    let mut working = result.clone();
    validate_recursive_working_rows(cte, working.len(), resources)?;
    validate_recursive_result(cte, &result, resources)?;

    let ledger = QueryMemoryLedger::for_query(resources.execution_memory, resources.task_context)?;
    let mut memory_tracker = OperatorMemoryTracker::with_account(
        resources.execution_memory.blocking_operator_bytes,
        ledger.account(
            QueryMemoryClass::BlockingState,
            "relational recursive union",
            resources.execution_memory.blocking_operator_bytes,
        ),
    );
    resize_recursive_memory(
        &mut memory_tracker,
        recursive_state_bytes(&result, &working, !union_all),
    )?;

    let mut iterations = 0usize;
    let mut working_rows_peak = working.len();
    let mut total_work = recursive_output_work(&seed.output);
    validate_recursive_total_work(cte, 0, total_work, resources)?;
    validate_recursive_read_limits(cte, &seed.output, resources)?;
    let mut recursive_resources = resources;
    recursive_resources.limits.hydration = seed.output.hydration;
    let mut recursive_tables = BTreeSet::new();
    recursive_tables.insert(cte.name.clone());
    while !working.is_empty() {
        if iterations >= MAX_RECURSIVE_CTE_ITERATIONS {
            return Err(HawDBError::Execution(format!(
                "recursive common table expression {} exceeds recursive_iteration_limit {}",
                cte_display_name(&cte.name),
                MAX_RECURSIVE_CTE_ITERATIONS
            )));
        }
        let table_schema = cte_table_schema(cte, &planned.schema, &planned.types)?;
        attach_recursive_rows(state, table_schema, &working)?;
        let recursive_read_modes = read_modes.with_memory_overlay(&recursive_tables);
        recursive_resources.limits.index_read = remaining_recursive_index_limits(
            resources.limits.index_read,
            &seed.output.index_execution_evidence,
        );
        recursive_resources.limits.row_read = remaining_recursive_row_limits(
            resources.limits.row_read,
            &seed.output.row_execution_evidence,
        );
        let mut next = execute_body(
            recursive_body.clone(),
            parameters,
            state,
            recursive_read_modes,
            recursive_resources,
            RelationalSqlStageTimings::default(),
        )?;
        if next.output.rows.schema().len() != planned.schema.len() {
            return Err(HawDBError::Semantic(format!(
                "recursive common table expression {} recursive term has arity {}, expected {}",
                cte_display_name(&cte.name),
                next.output.rows.schema().len(),
                planned.schema.len()
            )));
        }
        next.output.rows = coerce_rows(&next.output.rows, &planned.types)?;
        iterations = iterations.saturating_add(1);
        let next_work = recursive_output_work(&next.output);
        validate_recursive_total_work(cte, total_work, next_work, resources)?;
        total_work = total_work.saturating_add(next_work);
        add_stage_timings(&mut seed.output.stage_timings, next.output.stage_timings);
        seed.output.hydration = next.output.hydration;
        recursive_resources.limits.hydration = next.output.hydration;
        merge_recursive_row_evidence(
            &mut seed.output.row_execution_evidence,
            &next.output.row_execution_evidence,
        )?;
        merge_recursive_index_evidence(
            &mut seed.output.index_execution_evidence,
            next.output.index_execution_evidence,
        )?;
        validate_recursive_read_limits(cte, &seed.output, resources)?;
        let mut next_values = owned_values(&next.output.rows);
        if !union_all {
            next_values.retain(|row| seen.insert(row.clone()));
        }
        validate_recursive_working_rows(cte, next_values.len(), resources)?;
        result.extend(next_values.iter().cloned());
        validate_recursive_result(cte, &result, resources)?;
        working = next_values;
        working_rows_peak = working_rows_peak.max(working.len());
        resize_recursive_memory(
            &mut memory_tracker,
            recursive_state_bytes(&result, &working, !union_all),
        )?;
    }

    seed.output.rows = rows_from_values(&planned.schema, result)?;
    seed.output.intermediate_rows = seed.output.intermediate_rows.max(seed.output.rows.len());
    seed.output.access_path.name = format!(
        "RecursiveUnion({}, {})",
        cte_display_name(&cte.name),
        if union_all { "UNION ALL" } else { "UNION" }
    );
    seed.output.row_execution_evidence.runtime_path = "recursive_union";
    seed.output.row_execution_evidence.recursive_iterations = seed
        .output
        .row_execution_evidence
        .recursive_iterations
        .saturating_add(iterations);
    seed.output
        .row_execution_evidence
        .recursive_working_rows_peak = seed
        .output
        .row_execution_evidence
        .recursive_working_rows_peak
        .max(working_rows_peak);
    seed.output.row_execution_evidence.recursive_rows_emitted = seed
        .output
        .row_execution_evidence
        .recursive_rows_emitted
        .saturating_add(seed.output.rows.len());
    seed.output.row_execution_evidence.recursive_work = seed
        .output
        .row_execution_evidence
        .recursive_work
        .saturating_add(total_work);
    seed.output
        .blocking_operator_memory_reports
        .push(hawdb_executor::blocking::in_memory_report(
            "RecursiveUnionExec",
            &memory_tracker,
            memory_tracker.peak_bytes,
            total_work,
            resources.execution_memory,
        ));
    seed.types = planned.types;
    Ok(seed)
}

fn remaining_nonzero(limit: NonZeroUsize, used: usize) -> NonZeroUsize {
    NonZeroUsize::new(limit.get().saturating_sub(used).max(1))
        .expect("remaining recursive read envelope has a floor of one")
}

fn remaining_recursive_row_limits(
    limits: RelationalRowPageSnapshotReadLimits,
    evidence: &RelationalRowExecutionEvidence,
) -> RelationalRowPageSnapshotReadLimits {
    RelationalRowPageSnapshotReadLimits {
        demand: hawdb_storage::relational::RelationalRowPageDemandReadLimits {
            max_pages: remaining_nonzero(limits.demand.max_pages, evidence.logical_pages),
            max_rows: remaining_nonzero(limits.demand.max_rows, evidence.rows_visited),
            max_bytes: remaining_nonzero(limits.demand.max_bytes, evidence.logical_bytes),
            max_pins: limits.demand.max_pins,
            max_tree_height: limits.demand.max_tree_height,
        },
        max_overlay_entries: remaining_nonzero(
            limits.max_overlay_entries,
            evidence.overlay_entries,
        ),
        max_overlay_bytes: remaining_nonzero(
            limits.max_overlay_bytes,
            evidence.overlay_resident_bytes,
        ),
    }
}

fn remaining_recursive_index_limits(
    limits: RelationalIndexReadLimits,
    evidence: &[RelationalIndexExecutionEvidence],
) -> RelationalIndexReadLimits {
    let totals = recursive_index_read_totals(evidence);
    RelationalIndexReadLimits {
        max_pages: remaining_nonzero(limits.max_pages, totals[0]),
        max_rows: remaining_nonzero(limits.max_rows, totals[1]),
        max_bytes: remaining_nonzero(limits.max_bytes, totals[2]),
        max_file_bytes: limits.max_file_bytes.saturating_sub(totals[3]),
        max_tree_height: limits.max_tree_height,
    }
}

fn recursive_index_read_totals(evidence: &[RelationalIndexExecutionEvidence]) -> [usize; 4] {
    evidence.iter().fold([0; 4], |mut totals, item| {
        for (total, value) in totals.iter_mut().zip([
            item.logical_pages,
            item.rows_visited,
            item.logical_bytes,
            item.file_bytes,
        ]) {
            *total = total.saturating_add(value);
        }
        totals
    })
}

fn validate_recursive_read_limits(
    cte: &SqlCommonTableExpression,
    output: &RelationalQueryOutput,
    resources: RelationalQueryResourceContext<'_>,
) -> Result<()> {
    let row_limits = resources.limits.row_read;
    for (name, used, limit) in [
        (
            "row pages",
            output.row_execution_evidence.logical_pages,
            row_limits.demand.max_pages.get(),
        ),
        (
            "row rows",
            output.row_execution_evidence.rows_visited,
            row_limits.demand.max_rows.get(),
        ),
        (
            "row bytes",
            output.row_execution_evidence.logical_bytes,
            row_limits.demand.max_bytes.get(),
        ),
        (
            "row overlay entries",
            output.row_execution_evidence.overlay_entries,
            row_limits.max_overlay_entries.get(),
        ),
        (
            "row overlay bytes",
            output.row_execution_evidence.overlay_resident_bytes,
            row_limits.max_overlay_bytes.get(),
        ),
    ] {
        if used > limit {
            return Err(HawDBError::Execution(format!(
                "recursive common table expression {} exceeds cumulative {name} read limit {limit}",
                cte_display_name(&cte.name)
            )));
        }
    }
    let index_limits = resources.limits.index_read;
    let totals = recursive_index_read_totals(&output.index_execution_evidence);
    for (name, used, limit) in [
        ("index pages", totals[0], index_limits.max_pages.get()),
        ("index rows", totals[1], index_limits.max_rows.get()),
        ("index bytes", totals[2], index_limits.max_bytes.get()),
        ("index file bytes", totals[3], index_limits.max_file_bytes),
    ] {
        if used > limit {
            return Err(HawDBError::Execution(format!(
                "recursive common table expression {} exceeds cumulative {name} read limit {limit}",
                cte_display_name(&cte.name)
            )));
        }
    }
    Ok(())
}

fn merge_recursive_index_evidence(
    target: &mut Vec<RelationalIndexExecutionEvidence>,
    source: Vec<RelationalIndexExecutionEvidence>,
) -> Result<()> {
    for source in source {
        let Some(target) = target
            .iter_mut()
            .find(|target| target.table == source.table && target.index == source.index)
        else {
            target.push(source);
            continue;
        };
        merge_recursive_identity(
            &mut target.base_generation,
            &source.base_generation,
            "index base generation",
        )?;
        merge_recursive_identity(
            &mut target.delta_generation,
            &source.delta_generation,
            "index delta generation",
        )?;
        merge_recursive_identity(
            &mut target.base_commit_epoch,
            &source.base_commit_epoch,
            "index base commit epoch",
        )?;
        merge_recursive_identity(
            &mut target.visible_commit_epoch,
            &source.visible_commit_epoch,
            "index visible commit epoch",
        )?;
        merge_recursive_identity(
            &mut target.root_set_digest,
            &source.root_set_digest,
            "index root set digest",
        )?;
        macro_rules! add_counter {
            ($field:ident) => {
                target.$field = target.$field.saturating_add(source.$field);
            };
        }
        add_counter!(lookups);
        add_counter!(metadata_count_lookups);
        add_counter!(demand_paged_lookups);
        add_counter!(authoritative_lookups);
        add_counter!(transaction_workspace_lookups);
        add_counter!(canonical_fallback_lookups);
        target.fallback_reasons.extend(source.fallback_reasons);
        add_counter!(logical_pages);
        add_counter!(logical_bytes);
        add_counter!(file_pages);
        add_counter!(file_bytes);
        add_counter!(cache_hits);
        add_counter!(cache_misses);
        add_counter!(cache_admission_rejections);
        add_counter!(delta_pages_skipped);
        add_counter!(delta_entries_visited);
        add_counter!(live_batches_visited);
        add_counter!(live_entries_visited);
        add_counter!(live_entries_matched);
        add_counter!(live_bytes_visited);
        add_counter!(rows_visited);
        add_counter!(range_lookups);
        add_counter!(exclusive_seek_lookups);
        add_counter!(backward_lookups);
        add_counter!(early_stop_lookups);
    }
    Ok(())
}

fn merge_recursive_identity<T: Clone + Eq>(
    target: &mut Option<T>,
    source: &Option<T>,
    name: &str,
) -> Result<()> {
    match (&*target, source) {
        (Some(left), Some(right)) if left != right => Err(HawDBError::Execution(format!(
            "recursive query observed inconsistent {name} identities"
        ))),
        (None, Some(value)) => {
            *target = Some(value.clone());
            Ok(())
        }
        _ => Ok(()),
    }
}

fn merge_recursive_row_evidence(
    target: &mut RelationalRowExecutionEvidence,
    source: &RelationalRowExecutionEvidence,
) -> Result<()> {
    merge_recursive_identity(
        &mut target.base_generation,
        &source.base_generation,
        "base generation",
    )?;
    merge_recursive_identity(
        &mut target.delta_generation,
        &source.delta_generation,
        "delta generation",
    )?;
    merge_recursive_identity(
        &mut target.base_commit_epoch,
        &source.base_commit_epoch,
        "base commit epoch",
    )?;
    merge_recursive_identity(
        &mut target.visible_commit_epoch,
        &source.visible_commit_epoch,
        "visible commit epoch",
    )?;
    merge_recursive_identity(
        &mut target.root_set_digest,
        &source.root_set_digest,
        "root set digest",
    )?;
    merge_recursive_identity(
        &mut target.projection_generation,
        &source.projection_generation,
        "projection generation",
    )?;
    merge_recursive_identity(
        &mut target.projection_source_watermark,
        &source.projection_source_watermark,
        "projection source watermark",
    )?;
    merge_recursive_identity(
        &mut target.projection_version,
        &source.projection_version,
        "projection version",
    )?;
    merge_recursive_identity(
        &mut target.projection_publication_commit_epoch,
        &source.projection_publication_commit_epoch,
        "projection publication commit epoch",
    )?;
    macro_rules! add_counter {
        ($field:ident) => {
            target.$field = target.$field.saturating_add(source.$field);
        };
    }
    add_counter!(descriptor_reads);
    add_counter!(logical_pages);
    add_counter!(logical_bytes);
    add_counter!(file_pages);
    add_counter!(file_bytes);
    add_counter!(cache_hits);
    add_counter!(cache_misses);
    add_counter!(cache_admission_rejections);
    add_counter!(rows_visited);
    add_counter!(borrowed_rows_visited);
    add_counter!(owned_rows_visited);
    add_counter!(index_covered_rows);
    add_counter!(postings_visited);
    add_counter!(posting_blocks_read);
    add_counter!(posting_blocks_skipped);
    add_counter!(overlay_entries);
    add_counter!(overlay_resident_bytes);
    add_counter!(recursive_iterations);
    target.recursive_working_rows_peak = target
        .recursive_working_rows_peak
        .max(source.recursive_working_rows_peak);
    add_counter!(recursive_rows_emitted);
    add_counter!(recursive_work);
    Ok(())
}

fn recursive_output_work(output: &RelationalQueryOutput) -> usize {
    output
        .rows
        .len()
        .saturating_add(output.intermediate_rows)
        .saturating_add(output.row_execution_evidence.rows_visited)
}

fn validate_recursive_total_work(
    _cte: &SqlCommonTableExpression,
    current: usize,
    additional: usize,
    resources: RelationalQueryResourceContext<'_>,
) -> Result<()> {
    if current
        .checked_add(additional)
        .is_none_or(|total| total > resources.limits.max_candidate_work)
    {
        return Err(super::expression::work_budget_exceeded(
            "recursive_total_work(max_candidate_work)",
            current,
            additional,
            resources.limits.max_candidate_work,
        ));
    }
    Ok(())
}

fn validate_recursive_working_rows(
    _cte: &SqlCommonTableExpression,
    rows: usize,
    resources: RelationalQueryResourceContext<'_>,
) -> Result<()> {
    if rows > resources.limits.max_intermediate_rows {
        return Err(super::expression::work_budget_exceeded(
            "recursive_working_rows(max_intermediate_rows)",
            0,
            rows,
            resources.limits.max_intermediate_rows,
        ));
    }
    Ok(())
}

fn validate_recursive_result(
    _cte: &SqlCommonTableExpression,
    rows: &[Vec<Value>],
    resources: RelationalQueryResourceContext<'_>,
) -> Result<()> {
    if rows.len() > resources.limits.max_intermediate_rows {
        return Err(super::expression::work_budget_exceeded(
            "recursive_output_rows(max_intermediate_rows)",
            0,
            rows.len(),
            resources.limits.max_intermediate_rows,
        ));
    }
    let payload_bytes = value_rows_payload_bytes(rows);
    if payload_bytes > resources.limits.max_output_payload_bytes {
        return Err(super::expression::work_budget_exceeded(
            "recursive_output_payload_bytes(max_output_payload_bytes)",
            0,
            payload_bytes,
            resources.limits.max_output_payload_bytes,
        ));
    }
    Ok(())
}

fn value_rows_payload_bytes(rows: &[Vec<Value>]) -> usize {
    rows.iter().flatten().fold(0usize, |bytes, value| {
        bytes.saturating_add(hawdb_executor::query_value_payload_bytes(value))
    })
}

fn value_rows_retained_bytes(rows: &[Vec<Value>]) -> usize {
    rows.iter().fold(0usize, |bytes, row| {
        bytes
            .saturating_add(size_of::<Vec<Value>>())
            .saturating_add(row.capacity().saturating_mul(size_of::<Value>()))
            .saturating_add(row.iter().fold(0usize, |payload, value| {
                payload.saturating_add(hawdb_executor::query_value_payload_bytes(value))
            }))
    })
}

fn recursive_state_bytes(
    result: &[Vec<Value>],
    working: &[Vec<Value>],
    retains_seen: bool,
) -> usize {
    let result_bytes = value_rows_retained_bytes(result);
    result_bytes
        .saturating_add(value_rows_retained_bytes(working))
        .saturating_add(if retains_seen {
            result_bytes.saturating_add(
                result
                    .len()
                    .saturating_mul(size_of::<Vec<Value>>().saturating_add(128)),
            )
        } else {
            0
        })
}

fn resize_recursive_memory(tracker: &mut OperatorMemoryTracker, target_bytes: usize) -> Result<()> {
    if target_bytes > tracker.used_bytes {
        tracker.try_charge(target_bytes - tracker.used_bytes)
    } else {
        tracker.release(tracker.used_bytes - target_bytes);
        Ok(())
    }
}

fn attach_recursive_rows(
    state: &mut RelationalState,
    schema: RelationalTableSchema,
    rows: &[Vec<Value>],
) -> Result<()> {
    let types = schema
        .columns
        .iter()
        .map(|column| column.scalar_type)
        .collect::<Vec<_>>();
    let rows = rows
        .iter()
        .map(|row| {
            row.iter()
                .cloned()
                .zip(types.iter().copied())
                .map(|(value, scalar_type)| value_to_type(value, scalar_type))
                .collect::<Result<Vec<_>>>()
                .map(RelationalRow::new)
        })
        .collect::<Result<Vec<_>>>()?;
    state
        .attach_ephemeral_query_table(schema, rows)
        .map_err(HawDBError::from_execution_error)
}

fn execute_body<'a, R: RelationalQueryStoreReader>(
    body: SqlQueryBody,
    parameters: &[Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<TypedOutput> {
    match body {
        SqlQueryBody::Select(select) => {
            validate_select_relations(&select, state)?;
            let type_hints = select_output_types(&select, parameters, state)?;
            let output = if select.subquery_predicate.is_some() {
                subquery::execute_select_with_subqueries(
                    *select,
                    parameters,
                    state,
                    read_modes,
                    resources,
                    stage_timings,
                )?
            } else {
                execute_select_statement(
                    *select,
                    parameters,
                    state,
                    read_modes,
                    resources,
                    stage_timings,
                )?
            };
            let types = observed_output_types(&output.rows, type_hints)?;
            Ok(TypedOutput { output, types })
        }
        SqlQueryBody::Values(rows) => {
            execute_values(rows, parameters, state, resources, stage_timings)
        }
        SqlQueryBody::Nested(query) => execute_query_typed(
            *query,
            parameters,
            state,
            read_modes,
            resources,
            stage_timings,
        ),
        SqlQueryBody::SetOperation {
            left,
            operator,
            all,
            right,
        } => {
            let mut branch_resources = resources;
            branch_resources.limits.max_output_rows = resources.limits.max_intermediate_rows;
            let left = execute_body(
                *left,
                parameters,
                state,
                read_modes,
                branch_resources,
                stage_timings,
            )?;
            branch_resources.limits.hydration = left.output.hydration;
            let right = execute_body(
                *right,
                parameters,
                state,
                read_modes,
                branch_resources,
                RelationalSqlStageTimings::default(),
            )?;
            combine(operator, all, left, right, resources)
        }
    }
}

fn validate_select_relations(select: &SelectStatement, state: &RelationalState) -> Result<()> {
    for table in select
        .from
        .iter()
        .chain(select.joins.iter().map(|join| &join.table))
    {
        if state.table_schema(&table.name).is_none() {
            return Err(HawDBError::Semantic(format!(
                "unknown relational table {}",
                table.name
            )));
        }
    }
    Ok(())
}

fn combine(
    operator: SqlSetOperator,
    all: bool,
    mut left: TypedOutput,
    mut right: TypedOutput,
    resources: RelationalQueryResourceContext<'_>,
) -> Result<TypedOutput> {
    if left.output.rows.schema().len() != right.output.rows.schema().len() {
        return Err(HawDBError::Semantic(format!(
            "set-operation inputs have arity {} and {}",
            left.output.rows.schema().len(),
            right.output.rows.schema().len()
        )));
    }
    let types = left
        .types
        .iter()
        .copied()
        .zip(right.types.iter().copied())
        .enumerate()
        .map(|(column, (left, right))| common_type(left, right, column + 1))
        .collect::<Result<Vec<_>>>()?;
    left.output.rows = coerce_rows(&left.output.rows, &types)?;
    right.output.rows = coerce_rows(&right.output.rows, &types)?;

    let mut intermediate_rows = left.output.intermediate_rows;
    account_intermediate(
        &mut intermediate_rows,
        left.output.rows.len(),
        resources.limits.max_intermediate_rows,
    )?;
    account_intermediate(
        &mut intermediate_rows,
        right.output.rows.len(),
        resources.limits.max_intermediate_rows,
    )?;
    let set_work = left
        .output
        .rows
        .len()
        .checked_add(right.output.rows.len())
        .ok_or_else(|| HawDBError::Execution("set-operation work count overflow".into()))?;
    if set_work > resources.limits.max_candidate_work {
        return Err(super::expression::work_budget_exceeded(
            "max_candidate_work",
            0,
            set_work,
            resources.limits.max_candidate_work,
        ));
    }

    let retained_bytes = retained_bytes(&left.output.rows)
        .checked_add(retained_bytes(&right.output.rows))
        .and_then(|bytes| bytes.checked_mul(3))
        .ok_or_else(|| HawDBError::Execution("set-operation retained bytes overflow".into()))?;
    let ledger = QueryMemoryLedger::for_query(resources.execution_memory, resources.task_context)?;
    let mut memory_tracker = OperatorMemoryTracker::with_account(
        resources.execution_memory.blocking_operator_bytes,
        ledger.account(
            QueryMemoryClass::BlockingState,
            "relational set operation",
            resources.execution_memory.blocking_operator_bytes,
        ),
    );
    memory_tracker.try_charge(retained_bytes)?;

    let schema = left.output.rows.schema().clone();
    let left_values = owned_values(&left.output.rows);
    let right_values = owned_values(&right.output.rows);
    let values = set_values(operator, all, left_values, right_values);
    let rows = rows_from_values(&schema, values)?;

    left.output.rows = rows;
    left.output.intermediate_rows = intermediate_rows
        .checked_add(right.output.intermediate_rows)
        .ok_or_else(|| HawDBError::Execution("set-operation intermediate count overflow".into()))?;
    if left.output.intermediate_rows > resources.limits.max_intermediate_rows {
        return Err(super::expression::work_budget_exceeded(
            "max_intermediate_rows",
            intermediate_rows,
            right.output.intermediate_rows,
            resources.limits.max_intermediate_rows,
        ));
    }
    left.output.hydration = right.output.hydration;
    left.output
        .operator_cardinality_profiles
        .extend(right.output.operator_cardinality_profiles);
    left.output
        .join_access_paths
        .extend(right.output.join_access_paths);
    left.output
        .index_execution_evidence
        .extend(right.output.index_execution_evidence);
    left.output
        .blocking_operator_memory_reports
        .extend(right.output.blocking_operator_memory_reports);
    left.output
        .blocking_operator_memory_reports
        .push(hawdb_executor::blocking::in_memory_report(
            "SetOperationExec",
            &memory_tracker,
            memory_tracker.peak_bytes,
            set_work,
            resources.execution_memory,
        ));
    add_stage_timings(&mut left.output.stage_timings, right.output.stage_timings);
    left.output.access_path.name = match (operator, all) {
        (SqlSetOperator::Union, false) => "SetOperation(UNION)",
        (SqlSetOperator::Union, true) => "SetOperation(UNION ALL)",
        (SqlSetOperator::Intersect, false) => "SetOperation(INTERSECT)",
        (SqlSetOperator::Intersect, true) => "SetOperation(INTERSECT ALL)",
        (SqlSetOperator::Except, false) => "SetOperation(EXCEPT)",
        (SqlSetOperator::Except, true) => "SetOperation(EXCEPT ALL)",
    }
    .into();
    left.types = types;
    Ok(left)
}

fn add_stage_timings(
    target: &mut RelationalSqlStageTimings,
    additional: RelationalSqlStageTimings,
) {
    target.parse_nanos = target.parse_nanos.saturating_add(additional.parse_nanos);
    target.bind_nanos = target.bind_nanos.saturating_add(additional.bind_nanos);
    target.plan_nanos = target.plan_nanos.saturating_add(additional.plan_nanos);
    target.execute_nanos = target
        .execute_nanos
        .saturating_add(additional.execute_nanos);
}

fn materialize_cte(
    state: &mut RelationalState,
    cte: &SqlCommonTableExpression,
    output: &TypedOutput,
) -> Result<()> {
    let schema = cte_table_schema(cte, output.output.rows.schema(), &output.types)?;
    let types = schema
        .columns
        .iter()
        .map(|column| column.scalar_type)
        .collect::<Vec<_>>();
    let rows = output
        .output
        .rows
        .value_rows()
        .map(|row| {
            row.iter()
                .cloned()
                .zip(types.iter().copied())
                .map(|(value, scalar_type)| value_to_type(value, scalar_type))
                .collect::<Result<Vec<_>>>()
                .map(RelationalRow::new)
        })
        .collect::<Result<Vec<_>>>()?;
    state
        .attach_ephemeral_query_table(schema, rows)
        .map_err(HawDBError::from_execution_error)
}

fn cte_table_schema(
    cte: &SqlCommonTableExpression,
    output_schema: &QuerySchema,
    output_types: &[Option<RelationalScalarType>],
) -> Result<RelationalTableSchema> {
    if output_schema.len() != output_types.len() {
        return Err(HawDBError::Execution(
            "CTE output type shape does not match its schema".into(),
        ));
    }
    let names = if cte.columns.is_empty() {
        output_schema.columns().to_vec()
    } else {
        if cte.columns.len() != output_schema.len() {
            return Err(HawDBError::Semantic(format!(
                "common table expression {} declares {} columns but returns {}",
                cte_display_name(&cte.name),
                cte.columns.len(),
                output_schema.len()
            )));
        }
        cte.columns.clone()
    };
    let mut unique = BTreeSet::new();
    if let Some(duplicate) = names.iter().find(|name| !unique.insert(name.as_str())) {
        return Err(HawDBError::Semantic(format!(
            "common table expression {} repeats output column {duplicate}",
            cte_display_name(&cte.name)
        )));
    }
    let types = output_types
        .iter()
        .copied()
        .map(|scalar_type| scalar_type.unwrap_or(RelationalScalarType::Text))
        .collect::<Vec<_>>();
    Ok(RelationalTableSchema {
        name: cte.name.clone(),
        columns: names
            .into_iter()
            .zip(types.iter().copied())
            .map(|(name, scalar_type)| RelationalColumnSchema {
                name,
                scalar_type,
                nullable: true,
                default: None,
            })
            .collect(),
        primary_key: Vec::new(),
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        materialized_view: None,
    })
}

fn cte_display_name(name: &str) -> String {
    name.strip_prefix("\0hawdb_derived_")
        .map_or_else(|| name.to_string(), |source| format!("derived@{source}"))
}

fn select_output_types(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<Vec<Option<RelationalScalarType>>> {
    let mut types = Vec::new();
    for projection in &select.projection {
        match projection {
            SelectProjection::Wildcard => {
                for table in select
                    .from
                    .iter()
                    .chain(select.joins.iter().map(|join| &join.table))
                {
                    let schema = state.table_schema(&table.name).ok_or_else(|| {
                        HawDBError::Semantic(format!("unknown relational table {}", table.name))
                    })?;
                    types.extend(schema.columns.iter().map(|column| Some(column.scalar_type)));
                }
            }
            SelectProjection::Expression { expression, .. } => {
                let scalar_type = match &expression.kind {
                    ExprKind::And(_, _)
                    | ExprKind::Or(_, _)
                    | ExprKind::Not(_)
                    | ExprKind::Compare { .. }
                    | ExprKind::InList { .. }
                    | ExprKind::Like { .. }
                    | ExprKind::IsNull { .. } => Some(RelationalScalarType::Boolean),
                    ExprKind::CurrentTimestamp(kind) if kind.zoned => {
                        Some(RelationalScalarType::TimestampTz(kind.precision))
                    }
                    ExprKind::CurrentTimestamp(kind) => {
                        Some(RelationalScalarType::Timestamp(kind.precision))
                    }
                    ExprKind::Function { name, .. } if name == "count" => {
                        Some(RelationalScalarType::BigInt)
                    }
                    ExprKind::Function { name, .. } if name == "uuidv7" => {
                        Some(RelationalScalarType::Uuid)
                    }
                    ExprKind::Function { name, .. } if name == "version" => {
                        Some(RelationalScalarType::Text)
                    }
                    ExprKind::Function {
                        name, arguments, ..
                    } if matches!(name.as_str(), "sum" | "min" | "max") => arguments
                        .first()
                        .and_then(|argument| match argument {
                            SqlFunctionArgument::Expression(expression) => Some(expression),
                            SqlFunctionArgument::Wildcard => None,
                        })
                        .map(|expression| {
                            super::expression::infer_coalesce_scalar_type(
                                expression, select, parameters, state,
                            )
                        })
                        .transpose()?
                        .flatten(),
                    _ => super::expression::infer_coalesce_scalar_type(
                        expression, select, parameters, state,
                    )
                    .ok()
                    .flatten(),
                };
                types.push(scalar_type);
            }
        }
    }
    Ok(types)
}

fn observed_output_types(
    rows: &QueryRows,
    mut types: Vec<Option<RelationalScalarType>>,
) -> Result<Vec<Option<RelationalScalarType>>> {
    if rows.schema().len() != types.len() {
        return Err(HawDBError::Execution(
            "query output type shape does not match its schema".into(),
        ));
    }
    for row in rows.value_rows() {
        for (column, value) in row.iter().enumerate() {
            let candidate = crate::query_value::value_to_relational(value.clone())?.scalar_type();
            types[column] = common_type(types[column], candidate, column + 1)?;
        }
    }
    Ok(types)
}

fn common_type(
    left: Option<RelationalScalarType>,
    right: Option<RelationalScalarType>,
    column: usize,
) -> Result<Option<RelationalScalarType>> {
    use RelationalScalarType::{BigInt, Decimal, DoublePrecision, Timestamp, TimestampTz};
    let scalar_type = match (left, right) {
        (None, right) => return Ok(right),
        (left, None) => return Ok(left),
        (Some(left), Some(right)) if left == right => left,
        (Some(Decimal(left)), Some(Decimal(right))) => Decimal(
            hawdb_core::DecimalType::new(38, left.scale().max(right.scale()))
                .expect("decimal scale is bounded by 38"),
        ),
        (Some(Timestamp(left)), Some(Timestamp(right))) => Timestamp(
            hawdb_core::TimestampPrecision::new(left.get().max(right.get()))
                .expect("timestamp precision is bounded"),
        ),
        (Some(TimestampTz(left)), Some(TimestampTz(right))) => TimestampTz(
            hawdb_core::TimestampPrecision::new(left.get().max(right.get()))
                .expect("timestamp precision is bounded"),
        ),
        (Some(DoublePrecision), Some(BigInt | Decimal(_)))
        | (Some(BigInt | Decimal(_)), Some(DoublePrecision)) => DoublePrecision,
        (Some(Decimal(kind)), Some(BigInt)) | (Some(BigInt), Some(Decimal(kind))) => Decimal(
            hawdb_core::DecimalType::new(38, kind.scale()).expect("decimal scale is bounded"),
        ),
        (Some(left), Some(right)) => {
            return Err(HawDBError::Semantic(format!(
                "set-operation column {column} has incompatible types {left:?} and {right:?}"
            )));
        }
    };
    Ok(Some(scalar_type))
}

fn coerce_rows(rows: &QueryRows, types: &[Option<RelationalScalarType>]) -> Result<QueryRows> {
    let values = rows
        .value_rows()
        .map(|row| {
            row.iter()
                .cloned()
                .zip(types.iter().copied())
                .map(|(value, scalar_type)| match scalar_type {
                    Some(scalar_type) => value_to_type(value, scalar_type)
                        .and_then(|value| crate::query_value::relational_to_value(&value)),
                    None => Ok(value),
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    rows_from_values(rows.schema(), values)
}

fn value_to_type(value: Value, scalar_type: RelationalScalarType) -> Result<RelationalValue> {
    let value = crate::query_value::value_to_relational(value)?;
    let value = match (scalar_type, value) {
        (RelationalScalarType::DoublePrecision, RelationalValue::BigInt(value)) => {
            RelationalValue::DoublePrecision(value as f64)
        }
        (_, value) => crate::coerce_relational_value(value, scalar_type)?,
    };
    if !scalar_type.accepts(value.as_ref()) {
        return Err(HawDBError::Semantic(format!(
            "set-operation value has type {:?}, which cannot be coerced to {scalar_type:?}",
            value.scalar_type()
        )));
    }
    Ok(value)
}

fn retained_bytes(rows: &QueryRows) -> usize {
    rows.payload_bytes().saturating_add(
        rows.len()
            .saturating_mul(rows.schema().len())
            .saturating_mul(size_of::<Value>()),
    )
}

fn owned_values(rows: &QueryRows) -> Vec<Vec<Value>> {
    rows.value_rows().map(<[Value]>::to_vec).collect()
}

fn set_values(
    operator: SqlSetOperator,
    all: bool,
    left: Vec<Vec<Value>>,
    right: Vec<Vec<Value>>,
) -> Vec<Vec<Value>> {
    match (operator, all) {
        (SqlSetOperator::Union, true) => left.into_iter().chain(right).collect(),
        (SqlSetOperator::Union, false) => {
            let mut seen = BTreeSet::new();
            left.into_iter()
                .chain(right)
                .filter(|row| seen.insert(row.clone()))
                .collect()
        }
        (SqlSetOperator::Intersect, false) => {
            let right = right.into_iter().collect::<BTreeSet<_>>();
            let mut emitted = BTreeSet::new();
            left.into_iter()
                .filter(|row| right.contains(row) && emitted.insert(row.clone()))
                .collect()
        }
        (SqlSetOperator::Except, false) => {
            let right = right.into_iter().collect::<BTreeSet<_>>();
            let mut emitted = BTreeSet::new();
            left.into_iter()
                .filter(|row| !right.contains(row) && emitted.insert(row.clone()))
                .collect()
        }
        (SqlSetOperator::Intersect, true) => {
            let mut right_counts = multiplicities(right);
            left.into_iter()
                .filter(|row| take_one(&mut right_counts, row))
                .collect()
        }
        (SqlSetOperator::Except, true) => {
            let mut right_counts = multiplicities(right);
            left.into_iter()
                .filter(|row| !take_one(&mut right_counts, row))
                .collect()
        }
    }
}

fn multiplicities(rows: Vec<Vec<Value>>) -> BTreeMap<Vec<Value>, usize> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(row).or_default() += 1;
    }
    counts
}

fn take_one(counts: &mut BTreeMap<Vec<Value>, usize>, row: &[Value]) -> bool {
    let Some(count) = counts.get_mut(row) else {
        return false;
    };
    if *count == 0 {
        return false;
    }
    *count -= 1;
    true
}

fn rows_from_values(
    schema: &hawdb_executor::QuerySchema,
    rows: Vec<Vec<Value>>,
) -> Result<QueryRows> {
    let rows = rows
        .into_iter()
        .map(|values| {
            schema
                .columns()
                .iter()
                .cloned()
                .zip(values)
                .collect::<Row>()
        })
        .collect();
    QueryRows::try_from_named_rows(schema.clone(), rows)
}

fn apply_query_tail(
    order_by: &[SqlOrderItem],
    limit: Option<hawdb_sql::SqlBound>,
    offset: Option<hawdb_sql::SqlBound>,
    parameters: &[Value],
    resources: RelationalQueryResourceContext<'_>,
    output: &mut RelationalQueryOutput,
) -> Result<()> {
    let schema = output.rows.schema().clone();
    let offset = usize::try_from(bind_bound(offset, parameters, "OFFSET")?.unwrap_or(0))
        .unwrap_or(usize::MAX);
    let limit = bind_bound(limit, parameters, "LIMIT")?
        .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX))
        .unwrap_or(usize::MAX);
    let mut payload_bytes = 0usize;
    let mut rows = Vec::new();
    if order_by.is_empty() {
        for values in output.rows.value_rows().skip(offset).take(limit) {
            let row = schema
                .columns()
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .collect::<Row>();
            super::push_relational_output(row, &mut rows, &mut payload_bytes, resources.limits)?;
        }
    } else {
        let retained_bytes = retained_bytes(&output.rows)
            .checked_mul(3)
            .ok_or_else(|| HawDBError::Execution("set-operation sort bytes overflow".into()))?;
        let ledger =
            QueryMemoryLedger::for_query(resources.execution_memory, resources.task_context)?;
        let mut memory_tracker = OperatorMemoryTracker::with_account(
            resources.execution_memory.blocking_operator_bytes,
            ledger.account(
                QueryMemoryClass::BlockingState,
                "relational set operation sort",
                resources.execution_memory.blocking_operator_bytes,
            ),
        );
        memory_tracker.try_charge(retained_bytes)?;
        let order = order_by
            .iter()
            .map(|item| resolve_order(item, &schema))
            .collect::<Result<Vec<_>>>()?;
        let mut values = owned_values(&output.rows);
        values.sort_by(|left, right| compare_rows(left, right, &order));
        for values in values.into_iter().skip(offset).take(limit) {
            let row = schema
                .columns()
                .iter()
                .cloned()
                .zip(values)
                .collect::<Row>();
            super::push_relational_output(row, &mut rows, &mut payload_bytes, resources.limits)?;
        }
        output
            .blocking_operator_memory_reports
            .push(hawdb_executor::blocking::in_memory_report(
                "SetOperationSortExec",
                &memory_tracker,
                memory_tracker.peak_bytes,
                output.rows.len(),
                resources.execution_memory,
            ));
    }
    output.rows = QueryRows::try_from_named_rows(schema, rows)?;
    Ok(())
}

#[derive(Clone, Copy)]
struct BoundOrder {
    column: usize,
    direction: SqlOrderDirection,
    nulls: SqlNullOrder,
}

fn resolve_order(item: &SqlOrderItem, schema: &hawdb_executor::QuerySchema) -> Result<BoundOrder> {
    let column = match &item.expression.kind {
        ExprKind::Column(column) if column.qualifier.is_none() => {
            schema.position(&column.name).ok_or_else(|| {
                HawDBError::Semantic(format!(
                    "unknown set-operation ORDER BY column {}",
                    column.name
                ))
            })?
        }
        ExprKind::Value(SqlValue::Literal(Value::Int(position))) if *position > 0 => {
            usize::try_from(*position - 1)
                .ok()
                .filter(|position| *position < schema.len())
                .ok_or_else(|| {
                    HawDBError::Semantic(format!(
                        "set-operation ORDER BY position {position} is out of range"
                    ))
                })?
        }
        _ => {
            return Err(HawDBError::Semantic(
                "set-operation ORDER BY supports output column names or positions".into(),
            ));
        }
    };
    Ok(BoundOrder {
        column,
        direction: item.direction,
        nulls: item.nulls,
    })
}

fn compare_rows(left: &[Value], right: &[Value], order: &[BoundOrder]) -> Ordering {
    for item in order {
        let left = &left[item.column];
        let right = &right[item.column];
        let nulls_first = match item.nulls {
            SqlNullOrder::First => true,
            SqlNullOrder::Last => false,
            SqlNullOrder::DialectDefault => item.direction == SqlOrderDirection::Desc,
        };
        let ordering = match (left, right) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (_, Value::Null) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            _ => left.cmp(right),
        };
        let ordering = if item.direction == SqlOrderDirection::Desc
            && !matches!((left, right), (Value::Null, _) | (_, Value::Null))
        {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}
