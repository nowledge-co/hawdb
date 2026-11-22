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
use hawdb_executor::{QueryMemoryClass, QueryRows, QuerySchema, Row};
use hawdb_sql::{
    Expr, ExprKind, SelectProjection, SelectStatement, SqlBound, SqlCommonTableExpression,
    SqlCommonTableExpressionBody, SqlFunctionArgument, SqlNullOrder, SqlOrderDirection,
    SqlOrderItem, SqlQueryBody, SqlQueryStatement, SqlSetOperator, SqlSubqueryPredicate, SqlValue,
};
use hawdb_storage::relational::{
    RelationalColumnSchema, RelationalRow, RelationalScalarType, RelationalTableSchema,
    RelationalValue,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;

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
        "implementation=bounded, ctes={}, set_operations={}, values={}, materialization=statement_scoped",
        summary.ctes, summary.set_operations, summary.values
    );
    if summary.subqueries > 0 {
        operator_info.push_str(&format!(
            ", subqueries={}, subquery_strategy=budgeted_nested_loop",
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
        row.insert(
            "execution info".into(),
            Value::String("executed=true".into()),
        );
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
            let planned = {
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
    set_operations: usize,
    values: usize,
    subqueries: usize,
}

impl QueryPlanSummary {
    fn access_name(self) -> String {
        let base = match (self.ctes, self.set_operations, self.values) {
            (0, 0, 0) => "Query".into(),
            (0, 0, values) => format!("Values({values})"),
            (0, set_operations, _) => format!("SetOperation({set_operations})"),
            (ctes, 0, _) => format!("CTE Materialize({ctes})"),
            (ctes, set_operations, _) => {
                format!("CTE Materialize({ctes}) -> SetOperation({set_operations})")
            }
        };
        if self.subqueries == 0 {
            base
        } else {
            format!("SubqueryPredicate({}) -> {base}", self.subqueries)
        }
    }
}

fn query_plan_summary(query: &SqlQueryStatement) -> QueryPlanSummary {
    fn summarize_body(body: &SqlQueryBody) -> (usize, usize, usize, usize) {
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
                (0, 0, 0, subqueries)
            }
            SqlQueryBody::Values(_) => (0, 0, 1, 0),
            SqlQueryBody::Nested(query) => {
                let summary = query_plan_summary(query);
                (
                    summary.ctes,
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
                    1usize.saturating_add(left.1).saturating_add(right.1),
                    left.2.saturating_add(right.2),
                    left.3.saturating_add(right.3),
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
            (0usize, 0usize, 0usize, 0usize),
            |(ctes, sets, values, subqueries), summary| {
                (
                    ctes.saturating_add(summary.ctes),
                    sets.saturating_add(summary.set_operations),
                    values.saturating_add(summary.values),
                    subqueries.saturating_add(summary.subqueries),
                )
            },
        );
    let body = summarize_body(&query.body);
    let set_operations = nested.1.saturating_add(body.1);
    let values = nested.2.saturating_add(body.2);
    let subqueries = nested.3.saturating_add(body.3);
    let ctes = live_ctes
        .len()
        .saturating_add(nested.0)
        .saturating_add(body.0);
    QueryPlanSummary {
        operator: match (ctes, set_operations, values, subqueries) {
            (0, 0, 0, 0) => "QueryExec",
            (0, 0, 0, _) => "SubqueryPredicateExec",
            (0, 0, _, _) => "ValuesExec",
            (0, _, _, _) => "SetOperationExec",
            (_, _, _, _) => "CteMaterializeExec",
        },
        ctes,
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

fn query_external_relations(query: &SqlQueryStatement) -> BTreeSet<String> {
    let mut external = BTreeSet::new();
    let mut local = BTreeSet::new();
    for cte in &query.with {
        if let SqlCommonTableExpressionBody::Query(query) = &cte.body {
            for relation in query_external_relations(query) {
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
                let planned = plan_query_typed(
                    query,
                    parameters,
                    scoped_state,
                    cte_read_modes,
                    scoped_resources,
                    stage_timings,
                )?;
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
            let cte_output = execute_query_typed(
                *query.clone(),
                parameters,
                scoped_state,
                cte_read_modes,
                scoped_resources,
                stage_timings,
            )?;
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
                execute_select_with_subqueries(
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

struct SubqueryEvaluator<'a, R: RelationalQueryStoreReader> {
    parameters: &'a [Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'a>,
    work: usize,
}

impl<'a, R: RelationalQueryStoreReader> SubqueryEvaluator<'a, R> {
    fn evaluate(
        &mut self,
        predicate: &SqlSubqueryPredicate,
        row: &super::BoundRow<'_>,
    ) -> Result<Option<bool>> {
        match predicate {
            SqlSubqueryPredicate::Scalar(predicate) => {
                super::predicate_truth(predicate, row, self.parameters)
            }
            SqlSubqueryPredicate::And(left, right) => match self.evaluate(left, row)? {
                Some(false) => Ok(Some(false)),
                Some(true) => self.evaluate(right, row),
                None => match self.evaluate(right, row)? {
                    Some(false) => Ok(Some(false)),
                    Some(true) | None => Ok(None),
                },
            },
            SqlSubqueryPredicate::Or(left, right) => match self.evaluate(left, row)? {
                Some(true) => Ok(Some(true)),
                Some(false) => self.evaluate(right, row),
                None => match self.evaluate(right, row)? {
                    Some(true) => Ok(Some(true)),
                    Some(false) | None => Ok(None),
                },
            },
            SqlSubqueryPredicate::Not(predicate) => {
                Ok(self.evaluate(predicate, row)?.map(|value| !value))
            }
            SqlSubqueryPredicate::Exists { query } => self
                .execute(query, row, true)
                .map(|output| Some(!output.output.rows.is_empty())),
            SqlSubqueryPredicate::In {
                left,
                query,
                negated,
            } => {
                let output = self.execute(query, row, false)?;
                if output.output.rows.schema().len() != left.len() {
                    return Err(HawDBError::Semantic(format!(
                        "IN subquery returns {} columns but the left row has arity {}",
                        output.output.rows.schema().len(),
                        left.len()
                    )));
                }
                let mut matched = false;
                let mut unknown = false;
                for candidate in output.output.rows.value_rows() {
                    let right = candidate
                        .iter()
                        .cloned()
                        .map(|value| Expr::value(SqlValue::Literal(value)))
                        .collect::<Vec<_>>();
                    let comparison = if left.len() == 1 {
                        Expr::unspanned(ExprKind::Compare {
                            left: Box::new(left[0].clone()),
                            op: hawdb_sql::SqlComparisonOp::Eq,
                            right: Box::new(right[0].clone()),
                        })
                    } else {
                        Expr::unspanned(ExprKind::RowCompare {
                            left: left.clone(),
                            op: hawdb_sql::SqlComparisonOp::Eq,
                            right,
                        })
                    };
                    match super::predicate_truth(&comparison, row, self.parameters)? {
                        Some(true) => {
                            matched = true;
                            break;
                        }
                        None => unknown = true,
                        Some(false) => {}
                    }
                }
                let result = if matched {
                    Some(true)
                } else if unknown {
                    None
                } else {
                    Some(false)
                };
                Ok(result.map(|value| value != *negated))
            }
        }
    }

    fn execute(
        &mut self,
        query: &SqlQueryStatement,
        row: &super::BoundRow<'_>,
        exists: bool,
    ) -> Result<TypedOutput> {
        self.charge_work(1)?;
        let mut query = query.clone();
        bind_outer_query_references(&mut query, row, self.state)?;
        if exists {
            match query.limit {
                None | Some(SqlBound::Literal(2..)) => query.limit = Some(SqlBound::Literal(1)),
                Some(SqlBound::Literal(0..=1) | SqlBound::Parameter(_)) => {}
            }
        }
        let mut resources = self.resources;
        resources.limits.max_output_rows = if exists {
            1
        } else {
            resources.limits.max_intermediate_rows
        };
        resources.limits.max_output_payload_bytes =
            resources.execution_memory.query_memory_bytes.get();
        resources.limits.max_candidate_work = resources
            .limits
            .max_candidate_work
            .saturating_sub(self.work)
            .max(1);
        let output = execute_query_typed(
            query,
            self.parameters,
            self.state,
            self.read_modes,
            resources,
            RelationalSqlStageTimings::default(),
        )?;
        self.charge_work(
            output
                .output
                .intermediate_rows
                .max(output.output.rows.len()),
        )?;
        Ok(output)
    }

    fn charge_work(&mut self, requested: usize) -> Result<()> {
        let next = self
            .work
            .checked_add(requested)
            .ok_or_else(|| HawDBError::Execution("subquery work count overflow".into()))?;
        if next > self.resources.limits.max_candidate_work {
            return Err(super::expression::work_budget_exceeded(
                "max_candidate_work",
                self.work,
                requested,
                self.resources.limits.max_candidate_work,
            ));
        }
        self.work = next;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_select_with_subqueries<'a, R: RelationalQueryStoreReader>(
    select: SelectStatement,
    parameters: &'a [Value],
    state: &'a RelationalState,
    read_modes: RelationalQueryReadModes<'a, R>,
    resources: RelationalQueryResourceContext<'a>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<RelationalQueryOutput> {
    let predicate = select
        .subquery_predicate
        .clone()
        .expect("subquery execution requires a subquery predicate");
    validate_subquery_shapes(&predicate, &select, parameters, state)?;
    let ledger = QueryMemoryLedger::for_query(resources.execution_memory, resources.task_context)?;
    let tracker = OperatorMemoryTracker::with_account(
        resources.execution_memory.blocking_operator_bytes,
        ledger.account(
            QueryMemoryClass::BlockingState,
            "relational subquery predicate",
            resources.execution_memory.blocking_operator_bytes,
        ),
    );
    let mut evaluator = SubqueryEvaluator {
        parameters,
        state,
        read_modes,
        resources,
        work: 0,
    };
    let mut output = {
        let mut evaluate = |row: &super::BoundRow<'_>| evaluator.evaluate(&predicate, row);
        let filter = super::RelationalSubqueryFilter::new(&mut evaluate);
        super::execute_select_statement_with_filter(
            select,
            parameters,
            state,
            read_modes,
            resources,
            stage_timings,
            Some(&filter),
        )?
    };
    output.access_path.name = format!("SubqueryPredicateExec -> {}", output.access_path.name);
    output
        .blocking_operator_memory_reports
        .push(hawdb_executor::blocking::in_memory_report(
            "SubqueryPredicateExec",
            &tracker,
            tracker.peak_bytes,
            evaluator.work,
            resources.execution_memory,
        ));
    Ok(output)
}

fn validate_subquery_shapes(
    predicate: &SqlSubqueryPredicate,
    owner: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<()> {
    match predicate {
        SqlSubqueryPredicate::Scalar(_) => Ok(()),
        SqlSubqueryPredicate::And(left, right) | SqlSubqueryPredicate::Or(left, right) => {
            validate_subquery_shapes(left, owner, parameters, state)?;
            validate_subquery_shapes(right, owner, parameters, state)
        }
        SqlSubqueryPredicate::Not(predicate) => {
            validate_subquery_shapes(predicate, owner, parameters, state)
        }
        SqlSubqueryPredicate::Exists { query } => {
            validate_nested_subquery_shapes(query, parameters, state)
        }
        SqlSubqueryPredicate::In { left, query, .. } => {
            if let Some(arity) = static_query_arity(query, state)?
                && arity != left.len()
            {
                return Err(HawDBError::Semantic(format!(
                    "IN subquery returns {arity} columns but the left row has arity {}",
                    left.len()
                )));
            }
            if let Some(right_types) = static_query_types(query, parameters, state)? {
                for (column, (left, right)) in left.iter().zip(right_types).enumerate() {
                    let left = super::expression::infer_coalesce_scalar_type(
                        left, owner, parameters, state,
                    )?;
                    if let (Some(left), Some(right)) = (left, right)
                        && !subquery_types_comparable(left, right)
                    {
                        return Err(HawDBError::Semantic(format!(
                            "IN subquery column {} has incompatible types {left:?} and {right:?}",
                            column + 1
                        )));
                    }
                }
            }
            validate_nested_subquery_shapes(query, parameters, state)
        }
    }
}

fn validate_nested_subquery_shapes(
    query: &SqlQueryStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<()> {
    let mut result = Ok(());
    query.visit_selects(&mut |select| {
        if result.is_ok()
            && let Some(predicate) = &select.subquery_predicate
        {
            result = validate_subquery_shapes(predicate, select, parameters, state);
        }
    });
    result
}

fn static_query_types(
    query: &SqlQueryStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<Option<Vec<Option<RelationalScalarType>>>> {
    if !query.with.is_empty() {
        return Ok(None);
    }
    static_body_types(&query.body, parameters, state)
}

fn static_body_types(
    body: &SqlQueryBody,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<Option<Vec<Option<RelationalScalarType>>>> {
    match body {
        SqlQueryBody::Select(select) => {
            if select
                .from
                .iter()
                .chain(select.joins.iter().map(|join| &join.table))
                .any(|table| state.table_schema(&table.name).is_none())
            {
                return Ok(None);
            }
            select_output_types(select, parameters, state).map(Some)
        }
        SqlQueryBody::Values(rows) => {
            plan_values(rows, parameters, state).map(|plan| Some(plan.types))
        }
        SqlQueryBody::Nested(query) => static_query_types(query, parameters, state),
        SqlQueryBody::SetOperation { left, right, .. } => {
            let Some(left) = static_body_types(left, parameters, state)? else {
                return Ok(None);
            };
            let Some(right) = static_body_types(right, parameters, state)? else {
                return Ok(None);
            };
            if left.len() != right.len() {
                return Err(HawDBError::Semantic(format!(
                    "set-operation inputs have arity {} and {}",
                    left.len(),
                    right.len()
                )));
            }
            left.into_iter()
                .zip(right)
                .enumerate()
                .map(|(column, (left, right))| common_type(left, right, column + 1))
                .collect::<Result<Vec<_>>>()
                .map(Some)
        }
    }
}

fn subquery_types_comparable(left: RelationalScalarType, right: RelationalScalarType) -> bool {
    use RelationalScalarType::{BigInt, Decimal, DoublePrecision, Timestamp, TimestampTz};
    left == right
        || matches!(
            (left, right),
            (
                BigInt | Decimal(_) | DoublePrecision,
                BigInt | Decimal(_) | DoublePrecision
            ) | (Timestamp(_), Timestamp(_))
                | (TimestampTz(_), TimestampTz(_))
        )
}

fn static_query_arity(query: &SqlQueryStatement, state: &RelationalState) -> Result<Option<usize>> {
    let mut local = BTreeMap::new();
    for cte in &query.with {
        let arity = match &cte.body {
            SqlCommonTableExpressionBody::Query(query) => static_query_arity(query, state)?,
            SqlCommonTableExpressionBody::Mutation(_) => None,
        };
        if let Some(arity) = arity {
            if !cte.columns.is_empty() && cte.columns.len() != arity {
                return Err(HawDBError::Semantic(format!(
                    "common table expression {} declares {} columns but returns {arity}",
                    cte_display_name(&cte.name),
                    cte.columns.len()
                )));
            }
            local.insert(cte.name.clone(), arity);
        }
    }
    static_body_arity(&query.body, state, &local)
}

fn static_body_arity(
    body: &SqlQueryBody,
    state: &RelationalState,
    local: &BTreeMap<String, usize>,
) -> Result<Option<usize>> {
    match body {
        SqlQueryBody::Select(select) => {
            let mut arity = 0usize;
            for projection in &select.projection {
                match projection {
                    SelectProjection::Expression { .. } => arity = arity.saturating_add(1),
                    SelectProjection::Wildcard => {
                        for table in select
                            .from
                            .iter()
                            .chain(select.joins.iter().map(|join| &join.table))
                        {
                            let width = state
                                .table_schema(&table.name)
                                .map(|schema| schema.columns.len())
                                .or_else(|| local.get(&table.name).copied());
                            let Some(width) = width else {
                                return Ok(None);
                            };
                            arity = arity.checked_add(width).ok_or_else(|| {
                                HawDBError::Semantic("subquery output arity overflow".into())
                            })?;
                        }
                    }
                }
            }
            Ok(Some(arity))
        }
        SqlQueryBody::Values(rows) => {
            let Some(first) = rows.first() else {
                return Err(HawDBError::Semantic(
                    "VALUES requires at least one row".into(),
                ));
            };
            let arity = first.len();
            if let Some((index, row)) = rows.iter().enumerate().find(|(_, row)| row.len() != arity)
            {
                return Err(HawDBError::Semantic(format!(
                    "VALUES row {} has arity {}, expected {arity}",
                    index + 1,
                    row.len()
                )));
            }
            Ok(Some(arity))
        }
        SqlQueryBody::Nested(query) => static_query_arity(query, state),
        SqlQueryBody::SetOperation { left, right, .. } => {
            let left = static_body_arity(left, state, local)?;
            let right = static_body_arity(right, state, local)?;
            match (left, right) {
                (Some(left), Some(right)) if left != right => Err(HawDBError::Semantic(format!(
                    "set-operation inputs have arity {left} and {right}"
                ))),
                (Some(arity), Some(_)) => Ok(Some(arity)),
                _ => Ok(None),
            }
        }
    }
}

fn bind_outer_query_references(
    query: &mut SqlQueryStatement,
    row: &super::BoundRow<'_>,
    state: &RelationalState,
) -> Result<()> {
    bind_outer_query_references_scoped(query, row, state, &[])
}

fn bind_outer_query_references_scoped(
    query: &mut SqlQueryStatement,
    row: &super::BoundRow<'_>,
    state: &RelationalState,
    shadowing: &[LocalRelation<'_>],
) -> Result<()> {
    for cte in &mut query.with {
        if let SqlCommonTableExpressionBody::Query(query) = &mut cte.body {
            bind_outer_query_references_scoped(query, row, state, shadowing)?;
        }
    }
    bind_outer_body_references(&mut query.body, row, state, shadowing)
}

fn bind_outer_body_references(
    body: &mut SqlQueryBody,
    row: &super::BoundRow<'_>,
    state: &RelationalState,
    shadowing: &[LocalRelation<'_>],
) -> Result<()> {
    match body {
        SqlQueryBody::Select(select) => bind_outer_select_references(select, row, state, shadowing),
        SqlQueryBody::Values(rows) => {
            for expression in rows.iter_mut().flatten() {
                bind_outer_expression(expression, shadowing, row)?;
            }
            Ok(())
        }
        SqlQueryBody::Nested(query) => {
            bind_outer_query_references_scoped(query, row, state, shadowing)
        }
        SqlQueryBody::SetOperation { left, right, .. } => {
            bind_outer_body_references(left, row, state, shadowing)?;
            bind_outer_body_references(right, row, state, shadowing)
        }
    }
}

fn bind_outer_select_references(
    select: &mut SelectStatement,
    row: &super::BoundRow<'_>,
    state: &RelationalState,
    shadowing: &[LocalRelation<'_>],
) -> Result<()> {
    let current = select
        .from
        .iter()
        .map(|table| {
            (
                table.name.clone(),
                select
                    .from_alias
                    .clone()
                    .unwrap_or_else(|| table.name.clone()),
                state.table_schema(&table.name),
            )
        })
        .chain(select.joins.iter().map(|join| {
            (
                join.table.name.clone(),
                join.alias
                    .clone()
                    .unwrap_or_else(|| join.table.name.clone()),
                state.table_schema(&join.table.name),
            )
        }))
        .collect::<Vec<_>>();
    let mut local = shadowing.to_vec();
    local.extend(current);
    for projection in &mut select.projection {
        if let SelectProjection::Expression { expression, .. } = projection {
            bind_outer_expression(expression, &local, row)?;
        }
    }
    for join in &mut select.joins {
        bind_outer_expression(&mut join.on, &local, row)?;
    }
    if let Some(selection) = &mut select.selection {
        bind_outer_expression(selection, &local, row)?;
    }
    if let Some(predicate) = &mut select.subquery_predicate {
        bind_outer_predicate_expressions(predicate, &local, row, state)?;
    }
    if let Some(having) = &mut select.having {
        bind_outer_expression(having, &local, row)?;
    }
    for order in &mut select.order_by {
        bind_outer_expression(&mut order.expression, &local, row)?;
    }
    Ok(())
}

type LocalRelation<'a> = (String, String, Option<&'a RelationalTableSchema>);

fn bind_outer_predicate_expressions(
    predicate: &mut SqlSubqueryPredicate,
    local: &[LocalRelation<'_>],
    row: &super::BoundRow<'_>,
    state: &RelationalState,
) -> Result<()> {
    match predicate {
        SqlSubqueryPredicate::Scalar(expression) => bind_outer_expression(expression, local, row),
        SqlSubqueryPredicate::And(left, right) | SqlSubqueryPredicate::Or(left, right) => {
            bind_outer_predicate_expressions(left, local, row, state)?;
            bind_outer_predicate_expressions(right, local, row, state)
        }
        SqlSubqueryPredicate::Not(predicate) => {
            bind_outer_predicate_expressions(predicate, local, row, state)
        }
        SqlSubqueryPredicate::Exists { query } => {
            bind_outer_query_references_scoped(query, row, state, local)
        }
        SqlSubqueryPredicate::In { left, query, .. } => {
            for expression in left {
                bind_outer_expression(expression, local, row)?;
            }
            bind_outer_query_references_scoped(query, row, state, local)
        }
    }
}

fn bind_outer_expression(
    expression: &mut Expr,
    local: &[LocalRelation<'_>],
    row: &super::BoundRow<'_>,
) -> Result<()> {
    expression.try_visit_mut(&mut |expression| {
        let ExprKind::Column(column) = &expression.kind else {
            return Ok(());
        };
        let local_match = match column.qualifier.as_deref() {
            Some(qualifier) => local
                .iter()
                .any(|(table, alias, _)| qualifier == table || qualifier == alias),
            None => local.iter().any(|(_, _, schema)| {
                schema.is_none_or(|schema| schema.column_position(&column.name).is_some())
            }),
        };
        if local_match {
            return Ok(());
        }
        let bindings = row
            .bindings
            .iter()
            .filter(|binding| {
                column.qualifier.as_deref().is_none_or(|qualifier| {
                    qualifier == binding.qualifier || qualifier == binding.table
                }) && binding.schema.column_position(&column.name).is_some()
            })
            .collect::<Vec<_>>();
        match bindings.as_slice() {
            [] => Ok(()),
            [binding] => {
                let position = binding
                    .schema
                    .column_position(&column.name)
                    .expect("matched outer column has an ordinal");
                let value = relational_to_value(binding.value(position)?)?;
                expression.kind = ExprKind::Value(SqlValue::Literal(value));
                Ok(())
            }
            _ => Err(HawDBError::Semantic(format!(
                "column {} is unknown or ambiguous",
                column.name
            ))),
        }
    })
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
