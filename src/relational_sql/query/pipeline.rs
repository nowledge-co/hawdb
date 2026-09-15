use super::{
    account_intermediate, bound_row_resident_bytes, null_extended_tree_row, predicate_truth,
    project_bound_row, visit_base_entries, visit_batched_index_nested_loop, visit_hash_join,
    visit_index_merge_join, visit_join_entries, visit_tree_relation_entries, Arc, BatchControl,
    Binding, BindingId, BindingSchema, BoundRow, ColumnVector, ColumnarBatch, NonZeroUsize,
    OperatorMemoryTracker, QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger, RefCell,
    RelationalBaseAccess, RelationalIndexRuntime, RelationalJoinAccess, RelationalLocatorLayout,
    RelationalOperatorCardinalityProfile, RelationalOperatorId, RelationalPhysicalAccess,
    RelationalPhysicalJoinAlgorithm, RelationalPhysicalJoinExecution, RelationalPhysicalJoinNode,
    RelationalPhysicalJoinPlan, RelationalQueryLimits, RelationalRowLocator, RelationalRowRuntime,
    RelationalRowSetLocator, RelationalState, RelationalTableSchema, Result, Row, SelectStatement,
    SkeinError, SlotDescriptor, SlotId, SlotType, SqlJoinKind, Value,
};

pub(super) struct PlannedJoin<'a> {
    pub(super) binding: BindingId,
    pub(super) join: &'a crate::sql::SqlJoin,
    pub(super) schema: &'a RelationalTableSchema,
    pub(super) qualifier: String,
    pub(super) access: RelationalJoinAccess,
}

pub(super) fn relational_locator_layout<'a>(
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

pub(super) fn relational_physical_join_plan_locator_layout<'a>(
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

pub(super) struct RelationalPipelineState<'a> {
    pub(super) task_context: Option<&'a skein_core::RuntimeTaskContext>,
    pub(super) batch_rows: usize,
    pub(super) rows_until_checkpoint: usize,
    pub(super) intermediate_rows: usize,
    pub(super) max_intermediate_rows: usize,
    pub(super) candidate_work: usize,
    pub(super) max_candidate_work: usize,
    pub(super) operator_cardinality_profiles: Vec<RelationalOperatorCardinalityProfile>,
    pub(super) operator_pipeline_started: bool,
}

pub(super) const RELATIONAL_ROW_LOCATOR_SLOT: SlotId = SlotId(0);

pub(super) struct AccountedRelationalLocatorBatch {
    pub(super) schema: Arc<BindingSchema>,
    pub(super) locators: Vec<RelationalRowLocator>,
    pub(super) row_limit: usize,
    pub(super) byte_limit: usize,
    pub(super) locator_bytes: usize,
    pub(super) lease: QueryMemoryLease,
}

impl AccountedRelationalLocatorBatch {
    pub(super) fn new(
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

    pub(super) fn push(
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

    pub(super) fn emit(
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
    pub(super) fn new(
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

    pub(super) fn begin_operator_pipeline(&mut self) {
        let first_invocation = !self.operator_pipeline_started;
        self.operator_pipeline_started = true;
        for profile in &mut self.operator_cardinality_profiles {
            profile.actual_rows.get_or_insert(0);
            if first_invocation {
                profile.fully_consumed = true;
            }
        }
    }

    pub(super) fn account_operator_row(&mut self, operator_id: RelationalOperatorId) -> Result<()> {
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

    pub(super) fn account_unprofiled_row(&mut self) -> Result<()> {
        self.account_unprofiled_work()
    }

    pub(super) fn account_candidate_work(&mut self) -> Result<()> {
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

    pub(super) fn account_unprofiled_work(&mut self) -> Result<()> {
        account_intermediate(&mut self.intermediate_rows, 1, self.max_intermediate_rows)?;
        self.checkpoint_after_work()
    }

    pub(super) fn checkpoint_after_work(&mut self) -> Result<()> {
        self.rows_until_checkpoint = self.rows_until_checkpoint.saturating_sub(1);
        if self.rows_until_checkpoint == 0 {
            skein_executor::pipeline::runtime_checkpoint(self.task_context)?;
            self.rows_until_checkpoint = self.batch_rows;
        }
        Ok(())
    }

    pub(super) fn finish_operator_pipeline(&mut self, fully_consumed: bool) {
        if self.operator_pipeline_started {
            for profile in &mut self.operator_cardinality_profiles {
                profile.fully_consumed &= fully_consumed;
            }
        }
    }

    pub(super) fn operator_cardinality_profiles(
        &self,
    ) -> Vec<RelationalOperatorCardinalityProfile> {
        self.operator_cardinality_profiles.clone()
    }

    pub(super) fn finish(&self) -> Result<()> {
        skein_executor::pipeline::runtime_checkpoint(self.task_context)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn visit_prepared_physical_join_plan_node<'a>(
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
                relation
                    .output_schema
                    .ensure_matches(bound.schema_bindings())?;
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
                        output_schema.ensure_matches(combined.schema_bindings())?;
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
                        output_schema.ensure_matches(combined.schema_bindings())?;
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
pub(super) fn visit_relational_rows<'a>(
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
pub(super) fn visit_joined_row<'a>(
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

pub(super) fn typed_row_set_locator(row: &BoundRow<'_>) -> Result<RelationalRowSetLocator> {
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

pub(super) fn project_typed_locator(
    locator: &RelationalRowSetLocator,
    locator_layout: &RelationalLocatorLayout<'_>,
    select: &SelectStatement,
    parameters: &[Value],
    row_runtime: &RelationalRowRuntime<'_>,
) -> Result<Row> {
    with_typed_locator_bound_row(locator, locator_layout, row_runtime, |bound| {
        project_bound_row(bound, &select.projection, parameters)
    })
}

pub(super) fn with_typed_locator_bound_row<'a, T>(
    locator: &RelationalRowSetLocator,
    locator_layout: &RelationalLocatorLayout<'a>,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: impl FnOnce(&BoundRow<'a>) -> Result<T>,
) -> Result<T> {
    with_typed_locator_bound_row_mode(locator, locator_layout, row_runtime, true, visit)
}

pub(super) fn with_typed_locator_bound_row_for_scan<'a, T>(
    locator: &RelationalRowSetLocator,
    locator_layout: &RelationalLocatorLayout<'a>,
    row_runtime: &RelationalRowRuntime<'a>,
    visit: impl FnOnce(&BoundRow<'a>) -> Result<T>,
) -> Result<T> {
    with_typed_locator_bound_row_mode(locator, locator_layout, row_runtime, false, visit)
}

pub(super) fn with_typed_locator_bound_row_mode<'a, T>(
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
