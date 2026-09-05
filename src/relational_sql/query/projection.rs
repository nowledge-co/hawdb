use super::{
    aggregate_filter_matches, bind_bound, map_payload_bytes, project_bound_row,
    project_typed_locator, relational_locator_layout, relational_physical_join_plan_locator_layout,
    resolve_column, resolve_relational_order_target, stream_distinct_batches, stream_top_n_batches,
    typed_row_set_locator, visit_relational_rows, AccountedBindingBatch, BatchControl,
    BindingBatch, BindingBatchSource, BlockingExecutionContext, BlockingOperatorMemoryReport,
    BoundRow, Catalog, ExecutionLimit, ExecutionObserver, ExecutorBinding, ExternalTopN,
    NonZeroUsize, PhysicalPlan, PlannedJoin, QueryMemoryLedger, QueryRows, RefCell,
    RelationalBaseAccess, RelationalIndexRuntime, RelationalOrderTarget,
    RelationalPhysicalJoinExecution, RelationalPipelineState, RelationalQueryLimits,
    RelationalRowRuntime, RelationalSortKey, RelationalSortRecord, RelationalState,
    RelationalTableSchema, RelationalValue, Result, Row, SelectProjection, SelectStatement,
    SkeinError, SortDirection, SortItem, SortKey, SqlColumnRef, SqlNullOrder, SqlOrderDirection,
    SqlPredicate, Value,
};

pub(super) struct StreamingProjectionOutput {
    pub(super) rows: QueryRows,
    pub(super) blocking_operator_memory_reports: Vec<BlockingOperatorMemoryReport>,
}

pub(super) const RELATIONAL_SORT_COLUMN_PREFIX: &str = "__skein_relational_sort_";

#[derive(Default)]
pub(super) struct RelationalBlockingObserver {
    pub(super) reports: RefCell<Vec<BlockingOperatorMemoryReport>>,
}

impl ExecutionObserver for RelationalBlockingObserver {
    fn record_blocking_memory_report(&self, report: BlockingOperatorMemoryReport) {
        self.reports.borrow_mut().push(report);
    }
}

pub(super) struct ProjectedBatchSource<'a, 'pipeline> {
    pub(super) select: &'a SelectStatement,
    pub(super) parameters: &'a [Value],
    pub(super) state: &'a RelationalState,
    pub(super) base_schema: &'a RelationalTableSchema,
    pub(super) base_qualifier: &'a str,
    pub(super) base_access: &'a RelationalBaseAccess,
    pub(super) joins: &'a [PlannedJoin<'a>],
    pub(super) tree_execution: Option<&'pipeline RelationalPhysicalJoinExecution<'a>>,
    pub(super) pipeline: &'pipeline mut RelationalPipelineState<'a>,
    pub(super) index_runtime: &'pipeline RelationalIndexRuntime<'a>,
    pub(super) row_runtime: &'pipeline RelationalRowRuntime<'a>,
    pub(super) batch_rows: usize,
    pub(super) batch_memory: NonZeroUsize,
    pub(super) memory_ledger: &'pipeline QueryMemoryLedger,
    pub(super) add_order_keys: bool,
}

pub(super) struct DistinctAggregateValueBatchSource<'a, 'pipeline> {
    pub(super) select: &'a SelectStatement,
    pub(super) column: &'a SqlColumnRef,
    pub(super) filter: Option<&'a SqlPredicate>,
    pub(super) parameters: &'a [Value],
    pub(super) state: &'a RelationalState,
    pub(super) base_schema: &'a RelationalTableSchema,
    pub(super) base_qualifier: &'a str,
    pub(super) base_access: &'a RelationalBaseAccess,
    pub(super) joins: &'a [PlannedJoin<'a>],
    pub(super) tree_execution: Option<&'pipeline RelationalPhysicalJoinExecution<'a>>,
    pub(super) pipeline: &'pipeline mut RelationalPipelineState<'a>,
    pub(super) index_runtime: &'pipeline RelationalIndexRuntime<'a>,
    pub(super) row_runtime: &'pipeline RelationalRowRuntime<'a>,
    pub(super) batch_rows: usize,
    pub(super) batch_memory: NonZeroUsize,
    pub(super) memory_ledger: &'pipeline QueryMemoryLedger,
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
                let mut projected =
                    project_bound_row(&row, &self.select.projection, self.parameters)?;
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

pub(super) struct DistinctBatchSource<'a> {
    pub(super) input: &'a mut dyn BindingBatchSource,
    pub(super) input_plan: &'a PhysicalPlan,
    pub(super) catalog: &'a Catalog,
    pub(super) memory: &'a skein_executor::ExecutionMemoryConfig,
    pub(super) memory_ledger: &'a QueryMemoryLedger,
    pub(super) task_context: Option<&'a skein_core::RuntimeTaskContext>,
    pub(super) observer: &'a dyn ExecutionObserver,
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

pub(super) struct ProjectedSortKeyBatchSource<'a> {
    pub(super) input: &'a mut dyn BindingBatchSource,
    pub(super) order_columns: &'a [(String, crate::sql::SqlOrderItem)],
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
pub(super) fn execute_blocking_projection<'a>(
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
                parameters,
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

pub(super) fn consume_projected_batch(
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

pub(super) fn push_relational_output(
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

pub(super) fn relational_input_plan() -> PhysicalPlan {
    PhysicalPlan::SeqNodeScan {
        variable: "__relational_input".to_string(),
        label: String::new(),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute_relational_order(
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

pub(super) fn relational_sort_column(ordinal: usize) -> String {
    format!("{RELATIONAL_SORT_COLUMN_PREFIX}{ordinal}")
}

pub(super) fn order_by_uses_expression_alias(select: &SelectStatement) -> Result<bool> {
    select.order_by.iter().try_fold(false, |found, item| {
        Ok(found
            || matches!(
                resolve_relational_order_target(select, item)?,
                RelationalOrderTarget::ProjectionExpression { .. }
            ))
    })
}

pub(super) fn add_relational_order_keys(
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

pub(super) fn strip_relational_sort_columns(row: &mut Row) {
    row.retain(|name, _| !name.starts_with(RELATIONAL_SORT_COLUMN_PREFIX));
}

pub(super) fn relational_sort_value(value: &RelationalValue) -> Result<Value> {
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

pub(super) fn postgres_sort_key(
    value: Value,
    direction: SqlOrderDirection,
    nulls: SqlNullOrder,
) -> Value {
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

pub(super) fn projected_order_columns(
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
