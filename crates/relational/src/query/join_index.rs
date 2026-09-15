use super::{
    batched_index_probe_key, bound_row_resident_bytes, null_extended_tree_row, predicate_truth,
    relational_key_resident_bytes, visit_prepared_physical_join_plan_node, BTreeMap, BTreeSet,
    Binding, BindingId, BoundRow, OperatorMemoryTracker, QueryMemoryClass, RefCell,
    RelationalIndexRuntime, RelationalJoinAccess, RelationalKey, RelationalOperatorId,
    RelationalPhysicalAccess, RelationalPhysicalJoinExecution, RelationalPhysicalJoinNode,
    RelationalPhysicalOutputSchema, RelationalPipelineState, RelationalRowRuntime, RelationalState,
    Result, SkeinError, SqlJoinKind, SqlPredicate, Value,
};

pub(super) const BATCHED_INDEX_JOIN_CACHE_ENTRY_OVERHEAD_BYTES: usize = 128;

#[derive(Debug, Clone)]
pub(super) struct BatchedIndexJoinLocator {
    pub(super) primary_key: RelationalKey,
    pub(super) index_key: Option<RelationalKey>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn flush_batched_index_join_rows<'a>(
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
    index_runtime: &RelationalIndexRuntime<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
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
            right_relation
                .output_schema
                .ensure_matches(bound.schema_bindings())?;
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
            output_schema.ensure_matches(combined.schema_bindings())?;
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
            output_schema.ensure_matches(combined.schema_bindings())?;
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
            output_schema.ensure_matches(combined.schema_bindings())?;
            pipeline.borrow_mut().account_operator_row(operator_id)?;
            if !visit(combined)? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn visit_batched_index_nested_loop<'a>(
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
    index_runtime: &RelationalIndexRuntime<
        '_,
        impl crate::index_runtime::RelationalIndexStoreReader,
    >,
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
