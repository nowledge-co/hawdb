use super::{
    bound_join_key, bound_relation_join_key, null_extended_tree_row, predicate_truth,
    relational_key_resident_bytes, typed_row_set_locator, visit_prepared_physical_join_plan_node,
    with_typed_locator_bound_row_for_scan, BindingId, BoundRow, DefaultHasher, ExecutorBinding,
    Hash, HashMap, Hasher, NonZeroUsize, OperatorMemoryTracker, QueryMemoryClass,
    QueryMemoryLedger, RefCell, RelationalEquiJoinKeys, RelationalIndexRuntime, RelationalKey,
    RelationalLocatorLayout, RelationalOperatorId, RelationalPhysicalJoinExecution,
    RelationalPhysicalJoinNode, RelationalPhysicalOutputSchema, RelationalPhysicalRelation,
    RelationalPipelineState, RelationalRowRuntime, RelationalRowSetLocator, RelationalState,
    Result, SkeinError, SpillBudgetTracker, SpillRun, SpillWriter, SqlJoinKind, SqlPredicate,
    Value,
};

pub(super) const HASH_JOIN_MAP_ENTRY_OVERHEAD_BYTES: usize = 192;
pub(super) const HASH_JOIN_GRACE_PARTITIONS: usize = 2;
pub(super) const HASH_JOIN_SPILL_BINDING_NAME: &str = "__skein_relational_hash_locator";

#[derive(Debug, Clone, Copy)]
pub(super) enum HashJoinSpillSide {
    Build,
    Probe,
}

pub(super) struct HashJoinSpillRun {
    pub(super) run: SpillRun,
    pub(super) writer: Option<SpillWriter>,
}

pub(super) struct HashJoinGraceSpill {
    pub(super) budget: SpillBudgetTracker,
    pub(super) build_runs: Vec<Option<HashJoinSpillRun>>,
    pub(super) probe_runs: Vec<Option<HashJoinSpillRun>>,
    pub(super) next_ordinal: u64,
    pub(super) spilled_rows: usize,
}

impl HashJoinGraceSpill {
    pub(super) fn new(
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

    pub(super) fn write(
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

    pub(super) fn finish(&mut self) -> Result<()> {
        finish_hash_join_spill_runs(&mut self.build_runs)?;
        finish_hash_join_spill_runs(&mut self.probe_runs)
    }

    pub(super) fn run(
        &self,
        side: HashJoinSpillSide,
        partition: usize,
    ) -> Option<&HashJoinSpillRun> {
        let runs = match side {
            HashJoinSpillSide::Build => &self.build_runs,
            HashJoinSpillSide::Probe => &self.probe_runs,
        };
        runs.get(partition).and_then(Option::as_ref)
    }
}

pub(super) fn hash_join_partition(key: &RelationalKey, partition_count: usize) -> usize {
    debug_assert!(partition_count > 0);
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % partition_count
}

pub(super) fn write_hash_join_spill_record(
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

pub(super) fn finish_hash_join_spill_runs(runs: &mut [Option<HashJoinSpillRun>]) -> Result<()> {
    for run in runs.iter_mut().flatten() {
        if let Some(writer) = run.writer.take() {
            writer.finish()?;
        }
    }
    Ok(())
}

pub(super) fn map_hash_join_spill_record<T>(
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

pub(super) fn hash_join_spill_locator(binding: ExecutorBinding) -> Result<RelationalRowSetLocator> {
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

pub(super) fn hash_join_partition_memory_budget(
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

pub(super) fn try_insert_hash_join_build(
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

pub(super) fn spill_hash_join_build(
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

pub(super) fn visit_hash_join_candidate<'a>(
    context: &HashJoinCandidateContext<'_>,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    left_row: &BoundRow<'a>,
    right_row: &BoundRow<'a>,
    matched: &mut bool,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    pipeline.borrow_mut().account_candidate_work()?;
    let mut combined = left_row.clone();
    combined.bindings.extend(right_row.bindings.clone());
    for predicate in context.predicates {
        if predicate_truth(predicate, &combined, context.parameters)? != Some(true) {
            return Ok(true);
        }
    }
    *matched = true;
    context
        .output_schema
        .ensure_matches(combined.schema_bindings())?;
    pipeline
        .borrow_mut()
        .account_operator_row(context.operator_id)?;
    visit(combined)
}

pub(super) struct HashJoinCandidateContext<'a> {
    pub(super) operator_id: RelationalOperatorId,
    pub(super) predicates: &'a [SqlPredicate],
    pub(super) output_schema: &'a RelationalPhysicalOutputSchema,
    pub(super) parameters: &'a [Value],
}

pub(super) fn visit_hash_join_unmatched<'a>(
    operator_id: RelationalOperatorId,
    output_schema: &RelationalPhysicalOutputSchema,
    pipeline: &RefCell<&mut RelationalPipelineState<'_>>,
    left_row: &BoundRow<'a>,
    null_right: &BoundRow<'a>,
    visit: &mut dyn FnMut(BoundRow<'a>) -> Result<bool>,
) -> Result<bool> {
    let mut combined = left_row.clone();
    combined.bindings.extend(null_right.bindings.clone());
    output_schema.ensure_matches(combined.schema_bindings())?;
    pipeline.borrow_mut().account_operator_row(operator_id)?;
    visit(combined)
}

pub(super) fn relational_physical_relation_locator_layout<'a>(
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
pub(super) fn visit_hash_join<'a>(
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
    let candidate_context = HashJoinCandidateContext {
        operator_id,
        predicates,
        output_schema,
        parameters,
    };
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
                                &candidate_context,
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
pub(super) fn visit_grace_hash_join<'a>(
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
    let candidate_context = HashJoinCandidateContext {
        operator_id,
        predicates,
        output_schema,
        parameters,
    };
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
                                                &candidate_context,
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
                                            &candidate_context,
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
