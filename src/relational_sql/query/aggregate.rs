use super::{
    aggregate_group_base_memory_bytes, bind_bound, charge_aggregate_memory, map_payload_bytes,
    push_relational_output, relational_input_plan, relational_locator_layout,
    relational_physical_join_plan_locator_layout, resolve_column, stream_distinct_batches,
    typed_row_set_locator, visit_relational_rows, with_typed_locator_bound_row,
    AggregateProjectionState, BTreeMap, BatchControl, BlockingExecutionContext, Catalog,
    ColumnarAggregateExecutor, DistinctAggregateValueBatchSource, ExecutionLimit, ExternalTopN,
    OperatorMemoryTracker, PlannedJoin, QueryMemoryClass, QueryMemoryLedger,
    RelationalAccessPathDescriptor, RelationalBaseAccess, RelationalBlockingObserver,
    RelationalIndexRuntime, RelationalJoinPlanningOutcome, RelationalPhysicalJoinExecution,
    RelationalPipelineState, RelationalQueryLimits, RelationalQueryOutput, RelationalRowRuntime,
    RelationalSortKey, RelationalSortRecord, RelationalSqlStageTimings, RelationalState,
    RelationalTableSchema, RelationalValue, Result, Row, SelectProjection, SelectStatement,
    SkeinError, SqlColumnRef, SqlExpression, SqlFunctionArgument, SqlNullOrder, SqlOrderDirection,
    SqlPredicate, Value,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_aggregate_select<'a>(
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

pub(super) fn single_count_distinct_column(
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
pub(super) fn execute_single_count_distinct<'a>(
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
pub(super) fn execute_grouped_aggregate<'a>(
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

pub(super) fn emit_aggregate_group(
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
use skein_executor::observer::ExecutionObserver;
