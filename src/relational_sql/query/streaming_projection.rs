use super::{
    bind_bound, map_payload_bytes, project_bound_row, projection_uses_non_aggregate_coalesce,
    visit_relational_rows, AccountedRelationalLocatorBatch, BatchControl, Binding, BindingId,
    BoundRow, BoundStreamingPredicate, BoundStreamingProjection, ColumnarBatch, PlannedJoin,
    QueryMemoryLedger, QueryRowsBuilder, RelationalBaseAccess, RelationalIndexRuntime,
    RelationalOperatorId, RelationalPhysicalJoinExecution, RelationalPipelineState,
    RelationalQueryLimits, RelationalRowLocator, RelationalRowRuntime, RelationalState,
    RelationalTableSchema, Result, SelectStatement, SkeinError, StreamingProjectionOutput, Value,
    RELATIONAL_ROW_LOCATOR_SLOT,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_ordered_index_projection<'a>(
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
            let projected = project_bound_row(&bound, &select.projection, parameters)?;
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
pub(super) fn execute_streaming_projection<'a>(
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
        && !projection_uses_non_aggregate_coalesce(&select.projection)
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
                let projected = project_bound_row(&row, &select.projection, parameters)?;
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

pub(super) fn execute_borrowed_streaming_full_scan(
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
                .map(|predicate| predicate.truth_with(&|ordinal| row.value(ordinal)))
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
