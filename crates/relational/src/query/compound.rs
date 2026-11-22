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

use super::{
    account_intermediate, bind_bound, execute_select_statement,
    prepare_relational_select_with_snapshot, select_output_schema, validate_scalar_expressions,
    HawDBError, OperatorMemoryTracker, QueryMemoryLedger, RelationalAccessPathDescriptor,
    RelationalAccessPathKind, RelationalJoinEnumerationConfig, RelationalJoinPlanningOutcome,
    RelationalJoinPlanningReason, RelationalQueryOutput, RelationalQueryReadModes,
    RelationalQueryResourceContext, RelationalQueryStoreReader, RelationalRowExecutionEvidence,
    RelationalSqlStageTimings, RelationalState, Result, Value,
};
use hawdb_executor::{QueryMemoryClass, QueryRows, QuerySchema, Row};
use hawdb_sql::{
    ExprKind, SelectProjection, SelectStatement, SqlCommonTableExpression,
    SqlCommonTableExpressionBody, SqlFunctionArgument, SqlNullOrder, SqlOrderDirection,
    SqlOrderItem, SqlQueryBody, SqlQueryStatement, SqlSetOperator, SqlValue,
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
    row.insert(
        "operator info".into(),
        Value::String(format!(
            "implementation=bounded, ctes={}, set_operations={}, materialization=statement_scoped",
            summary.ctes, summary.set_operations
        )),
    );
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
}

impl QueryPlanSummary {
    fn access_name(self) -> String {
        match (self.ctes, self.set_operations) {
            (0, 0) => "Query".into(),
            (0, set_operations) => format!("SetOperation({set_operations})"),
            (ctes, 0) => format!("CTE Materialize({ctes})"),
            (ctes, set_operations) => {
                format!("CTE Materialize({ctes}) -> SetOperation({set_operations})")
            }
        }
    }
}

fn query_plan_summary(query: &SqlQueryStatement) -> QueryPlanSummary {
    fn summarize_body(body: &SqlQueryBody) -> (usize, usize) {
        match body {
            SqlQueryBody::Select(_) => (0, 0),
            SqlQueryBody::Nested(query) => {
                let summary = query_plan_summary(query);
                (summary.ctes, summary.set_operations)
            }
            SqlQueryBody::SetOperation { left, right, .. } => {
                let left = summarize_body(left);
                let right = summarize_body(right);
                (
                    left.0.saturating_add(right.0),
                    1usize.saturating_add(left.1).saturating_add(right.1),
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
        .fold((0usize, 0usize), |(ctes, sets), summary| {
            (
                ctes.saturating_add(summary.ctes),
                sets.saturating_add(summary.set_operations),
            )
        });
    let body = summarize_body(&query.body);
    let set_operations = nested.1.saturating_add(body.1);
    let ctes = live_ctes
        .len()
        .saturating_add(nested.0)
        .saturating_add(body.0);
    QueryPlanSummary {
        operator: match (ctes, set_operations) {
            (0, 0) => "QueryExec",
            (0, _) => "SetOperationExec",
            (_, _) => "CteMaterializeExec",
        },
        ctes,
        set_operations,
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
        SqlQueryBody::Select(select) => select
            .from
            .iter()
            .chain(select.joins.iter().map(|join| &join.table))
            .filter(|table| table.schema.is_none())
            .map(|table| table.name.clone())
            .collect(),
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
                .cloned()
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
            let output = execute_select_statement(
                *select,
                parameters,
                state,
                read_modes,
                resources,
                stage_timings,
            )?;
            let types = observed_output_types(&output.rows, type_hints)?;
            Ok(TypedOutput { output, types })
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
                cte.name,
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
            cte.name
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
