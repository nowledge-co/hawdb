use super::*;

const PARTIAL_AGGREGATE_VALUE: &str = "__skein_partial_aggregate";

fn partial_state_width_mismatch_error() -> SkeinError {
    SkeinError::Execution("AggregateExec partial state width mismatch".to_string())
}

struct PartialGroup {
    ordinal: u64,
    states: Vec<AggregateState>,
}

impl PartialGroup {
    fn memory_bytes(&self, key: &[Value]) -> usize {
        partial_group_memory_bytes(key, &self.states)
    }
}

struct PartialRunRow {
    key: Vec<Value>,
    ordinal: u64,
    states: Vec<AggregateState>,
}

impl PartialRunRow {
    fn memory_bytes(&self) -> usize {
        partial_group_memory_bytes(&self.key, &self.states)
    }
}

pub(super) fn partial_aggregation_is_mergeable(item: &Aggregation) -> bool {
    !item.distinct
        && matches!(
            item.function,
            AggregateFunction::Count
                | AggregateFunction::Min
                | AggregateFunction::Max
                | AggregateFunction::Avg
        )
}

fn partial_group_memory_bytes(key: &[Value], states: &[AggregateState]) -> usize {
    partial_group_base_memory_bytes(key).saturating_add(
        states.iter().fold(0usize, |total, state| {
            total.saturating_add(state.partial_memory_bytes())
        }),
    )
}

fn partial_group_base_memory_bytes(key: &[Value]) -> usize {
    std::mem::size_of::<PartialGroup>()
        .saturating_add(
            key.iter()
                .fold(std::mem::size_of::<Vec<Value>>(), |total, value| {
                    total.saturating_add(value_memory_bytes(value))
                }),
        )
        .saturating_add(std::mem::size_of::<Vec<AggregateState>>())
        .saturating_add(std::mem::size_of::<usize>() * 6)
}

fn partial_group_memory_bytes_after_merge(
    key: &[Value],
    states: &[AggregateState],
    incoming: &[AggregateState],
) -> Result<usize> {
    if states.len() != incoming.len() {
        return Err(partial_state_width_mismatch_error());
    }
    states.iter().zip(incoming).try_fold(
        partial_group_base_memory_bytes(key),
        |total, (state, incoming)| {
            Ok(total.saturating_add(state.partial_memory_bytes_after_merge(incoming)?))
        },
    )
}

fn partial_states_for_binding(
    items: &[Aggregation],
    catalog: &Catalog,
    binding: &Binding,
) -> Vec<AggregateState> {
    items
        .iter()
        .map(|item| {
            let mut state = AggregateState::new(item);
            state.update(item, catalog, binding);
            state
        })
        .collect()
}

fn merge_partial_states(
    states: &mut [AggregateState],
    incoming: Vec<AggregateState>,
) -> Result<()> {
    if states.len() != incoming.len() {
        return Err(partial_state_width_mismatch_error());
    }
    for (state, incoming) in states.iter_mut().zip(incoming) {
        state.merge_partial(incoming)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn stream_partial_aggregate_batches(
    input: &PhysicalPlan,
    group_keys: &[Projection],
    items: &[Aggregation],
    source: &mut dyn BindingBatchSource,
    context: AggregateExecutionContext<'_>,
    memory: &ExecutionMemoryConfig,
    observer: &dyn ExecutionObserver,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let blocking_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "AggregateExec partial state",
        memory.blocking_operator_bytes,
    );
    let mut tracker = OperatorMemoryTracker::with_account(
        memory.blocking_operator_bytes,
        blocking_account.clone(),
    );
    let mut spill_budget =
        SpillBudgetTracker::with_ledger("AggregateExec", memory, context.memory_ledger);
    let mut groups = BTreeMap::<Vec<Value>, PartialGroup>::new();
    let mut runs = Vec::<spill::SpillRun>::new();
    let mut ordinal = 0u64;
    let partial_item_limit = memory
        .blocking_operator_bytes
        .get()
        .saturating_div(3)
        .max(1);

    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(context.task_context)?;
        for binding in batch {
            let key = group_keys
                .iter()
                .map(|item| group_key_value(item, context.catalog, &binding))
                .collect::<Vec<_>>();
            let incoming = partial_states_for_binding(items, context.catalog, &binding);

            if let Some(group) = groups.get(&key) {
                let previous_bytes = group.memory_bytes(&key);
                let next_bytes =
                    partial_group_memory_bytes_after_merge(&key, &group.states, &incoming)?;
                if next_bytes > partial_item_limit {
                    return Err(partial_item_limit_error(next_bytes, partial_item_limit));
                }
                let delta = MemoryDelta::between(previous_bytes, next_bytes);
                if tracker.would_exceed(delta.added_bytes) {
                    runs.push(spill_partial_group_run(
                        &mut groups,
                        &mut spill_budget,
                        context.task_context,
                    )?);
                    tracker.reset();
                    insert_partial_group(
                        &mut groups,
                        key,
                        ordinal,
                        incoming,
                        &mut tracker,
                        partial_item_limit,
                    )?;
                } else {
                    // Reserve growth before replacing retained Min/Max values;
                    // release shrinkage only after the in-place merge.
                    tracker.try_charge(delta.added_bytes)?;
                    merge_partial_states(
                        &mut groups.get_mut(&key).expect("partial group exists").states,
                        incoming,
                    )?;
                    tracker.release(delta.released_bytes);
                }
            } else {
                let bytes = partial_group_memory_bytes(&key, &incoming);
                if bytes > partial_item_limit {
                    return Err(partial_item_limit_error(bytes, partial_item_limit));
                }
                if tracker.would_exceed(bytes) && !groups.is_empty() {
                    runs.push(spill_partial_group_run(
                        &mut groups,
                        &mut spill_budget,
                        context.task_context,
                    )?);
                    tracker.reset();
                }
                insert_partial_group(
                    &mut groups,
                    key,
                    ordinal,
                    incoming,
                    &mut tracker,
                    partial_item_limit,
                )?;
            }
            ordinal = ordinal.saturating_add(1);
        }
        Ok(BatchControl::Continue)
    })?;

    if runs.is_empty() {
        observer.record_blocking_memory_report(in_memory_report(
            "AggregateExec",
            &tracker,
            tracker.peak_bytes,
            ordinal as usize,
            memory,
        ));
        return emit_partial_groups(groups, tracker, context, emit);
    }
    if !groups.is_empty() {
        runs.push(spill_partial_group_run(
            &mut groups,
            &mut spill_budget,
            context.task_context,
        )?);
        tracker.reset();
    }
    runs = compact_partial_runs(
        runs,
        items,
        memory,
        &mut spill_budget,
        &blocking_account,
        context.task_context,
    )?;
    observer.record_blocking_memory_report(spill_backed_report(
        "AggregateExec",
        &tracker,
        tracker.peak_bytes,
        ordinal as usize,
        &spill_budget,
        ordinal as usize,
    ));
    merge_partial_runs(&runs, &spill_budget, &blocking_account, context, emit)
}

fn partial_item_limit_error(bytes: usize, limit: usize) -> SkeinError {
    SkeinError::Execution(format!(
        "AggregateExec partial state uses {bytes} bytes, exceeding its bounded merge allowance {limit}"
    ))
}

fn insert_partial_group(
    groups: &mut BTreeMap<Vec<Value>, PartialGroup>,
    key: Vec<Value>,
    ordinal: u64,
    states: Vec<AggregateState>,
    tracker: &mut OperatorMemoryTracker,
    partial_item_limit: usize,
) -> Result<()> {
    let bytes = partial_group_memory_bytes(&key, &states);
    if bytes > partial_item_limit {
        return Err(partial_item_limit_error(bytes, partial_item_limit));
    }
    if tracker.would_exceed(bytes) {
        return Err(SkeinError::Execution(format!(
            "AggregateExec partial state exceeds blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    tracker.try_charge(bytes)?;
    groups.insert(key, PartialGroup { ordinal, states });
    Ok(())
}

fn finish_partial_group(
    key: Vec<Value>,
    states: Vec<AggregateState>,
    group_keys: &[Projection],
    items: &[Aggregation],
) -> Binding {
    let mut values = BTreeMap::new();
    for (item, value) in group_keys.iter().zip(key) {
        insert_projected_value(&mut values, &item.name, value);
    }
    for (item, state) in items.iter().zip(states) {
        insert_projected_value(&mut values, &item.name, state.finish());
    }
    Binding::values(values)
}

fn emit_partial_groups(
    groups: BTreeMap<Vec<Value>, PartialGroup>,
    mut group_tracker: OperatorMemoryTracker,
    context: AggregateExecutionContext<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut output = AccountedBindingBatch::with_ledger(
        "AggregateExec",
        context.batch_rows,
        context.output_memory_budget,
        context.memory_ledger,
    );
    let mut emitted = 0usize;
    for (key, group) in groups {
        runtime_checkpoint(context.task_context)?;
        let group_bytes = group.memory_bytes(&key);
        let binding = finish_partial_group(key, group.states, context.group_keys, context.items);
        if output.transfer_from(&mut group_tracker, group_bytes, binding, emit)?
            == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        emitted = emitted.saturating_add(1);
        if flush_aggregate_batch(&mut output, emitted, context.execution_limit, emit)?
            == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
    }
    if !output.is_empty() && output.emit(emit)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

fn encode_partial_binding(key: Vec<Value>, states: Vec<AggregateState>) -> Result<Binding> {
    let mut encoded = Vec::with_capacity(states.len().saturating_add(1));
    encoded.push(Value::List(key));
    for state in states {
        let value = match state {
            AggregateState::Count {
                count,
                distinct: None,
            } => Value::Int(count as i64),
            AggregateState::Min(value) | AggregateState::Max(value) => value.unwrap_or(Value::Null),
            AggregateState::Avg { sum, count } => {
                Value::List(vec![Value::Float(sum), Value::Int(count as i64)])
            }
            _ => {
                return Err(SkeinError::Execution(
                    "AggregateExec cannot encode an unmergeable partial state".to_string(),
                ));
            }
        };
        encoded.push(value);
    }
    Ok(Binding::scalar(
        PARTIAL_AGGREGATE_VALUE,
        Value::List(encoded),
    ))
}

fn decode_partial_binding(
    ordinal: u64,
    mut binding: Binding,
    items: &[Aggregation],
) -> Result<PartialRunRow> {
    if !binding.nodes.is_empty() || !binding.relationships.is_empty() || binding.values.len() != 1 {
        return Err(SkeinError::Execution(
            "AggregateExec partial spill record has an invalid shape".to_string(),
        ));
    }
    let Some(Value::List(encoded)) = binding.values.remove(PARTIAL_AGGREGATE_VALUE) else {
        return Err(SkeinError::Execution(
            "AggregateExec partial spill record is missing its state".to_string(),
        ));
    };
    let mut encoded = encoded.into_iter();
    let Some(Value::List(key)) = encoded.next() else {
        return Err(SkeinError::Execution(
            "AggregateExec partial spill record is missing its group key".to_string(),
        ));
    };
    let mut states = Vec::with_capacity(items.len());
    for item in items {
        let Some(value) = encoded.next() else {
            return Err(SkeinError::Execution(
                "AggregateExec partial spill record has too few states".to_string(),
            ));
        };
        let state = match (&item.function, value) {
            (AggregateFunction::Count, Value::Int(count)) if count >= 0 && !item.distinct => {
                AggregateState::Count {
                    count: usize::try_from(count).map_err(|_| {
                        SkeinError::Execution(
                            "AggregateExec partial count does not fit in memory".to_string(),
                        )
                    })?,
                    distinct: None,
                }
            }
            (AggregateFunction::Min, Value::Null) => AggregateState::Min(None),
            (AggregateFunction::Min, value) => AggregateState::Min(Some(value)),
            (AggregateFunction::Max, Value::Null) => AggregateState::Max(None),
            (AggregateFunction::Max, value) => AggregateState::Max(Some(value)),
            (AggregateFunction::Avg, Value::List(values)) if values.len() == 2 => {
                let mut values = values.into_iter();
                let (Some(Value::Float(sum)), Some(Value::Int(count))) =
                    (values.next(), values.next())
                else {
                    return Err(SkeinError::Execution(
                        "AggregateExec partial average has an invalid state".to_string(),
                    ));
                };
                if count < 0 {
                    return Err(SkeinError::Execution(
                        "AggregateExec partial average has a negative count".to_string(),
                    ));
                }
                AggregateState::Avg {
                    sum,
                    count: usize::try_from(count).map_err(|_| {
                        SkeinError::Execution(
                            "AggregateExec partial average count does not fit in memory"
                                .to_string(),
                        )
                    })?,
                }
            }
            _ => {
                return Err(SkeinError::Execution(
                    "AggregateExec partial spill record has an incompatible state".to_string(),
                ));
            }
        };
        states.push(state);
    }
    if encoded.next().is_some() {
        return Err(SkeinError::Execution(
            "AggregateExec partial spill record has too many states".to_string(),
        ));
    }
    Ok(PartialRunRow {
        key,
        ordinal,
        states,
    })
}

fn spill_partial_group_run(
    groups: &mut BTreeMap<Vec<Value>, PartialGroup>,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let (run, mut writer) = spill_budget.create_run("aggregate-partial")?;
    for (key, group) in std::mem::take(groups) {
        runtime_checkpoint(task_context)?;
        let binding = encode_partial_binding(key, group.states)?;
        writer.write(group.ordinal, &binding, spill_budget)?;
    }
    writer.finish()?;
    Ok(run)
}

fn read_partial_row(
    reader: &mut spill::SpillReader,
    items: &[Aggregation],
    memory_budget: NonZeroUsize,
    spill_budget: &SpillBudgetTracker,
    tracker: &mut OperatorMemoryTracker,
) -> Result<Option<PartialRunRow>> {
    let limit = memory_budget.get().saturating_div(3).max(1);
    reader
        .read_binding_record(memory_budget.get(), spill_budget)?
        .map(|record| {
            record.try_map(
                "AggregateExec partial merge",
                limit,
                tracker,
                |ordinal, binding| decode_partial_binding(ordinal, binding, items),
                PartialRunRow::memory_bytes,
            )
        })
        .transpose()
}

fn compact_partial_runs(
    mut runs: Vec<spill::SpillRun>,
    items: &[Aggregation],
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    blocking_account: &QueryMemoryAccount,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<spill::SpillRun>> {
    while runs.len() > 2 {
        runtime_checkpoint(task_context)?;
        let mut compacted = Vec::with_capacity(runs.len().div_ceil(2));
        let mut pending = runs.into_iter();
        while let Some(left) = pending.next() {
            let Some(right) = pending.next() else {
                compacted.push(left);
                break;
            };
            compacted.push(merge_partial_run_pair(
                &left,
                &right,
                items,
                memory.blocking_operator_bytes,
                spill_budget,
                blocking_account,
                task_context,
            )?);
        }
        runs = compacted;
    }
    Ok(runs)
}

fn merge_partial_run_pair(
    left: &spill::SpillRun,
    right: &spill::SpillRun,
    items: &[Aggregation],
    memory_budget: NonZeroUsize,
    spill_budget: &mut SpillBudgetTracker,
    blocking_account: &QueryMemoryAccount,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    let mut left_reader = left.reader()?;
    let mut right_reader = right.reader()?;
    let mut tracker = OperatorMemoryTracker::with_account(memory_budget, blocking_account.clone());
    let mut left_row = read_partial_row(
        &mut left_reader,
        items,
        memory_budget,
        spill_budget,
        &mut tracker,
    )?;
    let mut right_row = read_partial_row(
        &mut right_reader,
        items,
        memory_budget,
        spill_budget,
        &mut tracker,
    )?;
    let (run, mut writer) = spill_budget.create_run("aggregate-partial-merge")?;
    while left_row.is_some() || right_row.is_some() {
        runtime_checkpoint(task_context)?;
        let Some(selection) = take_next_partial_row(&mut left_row, &mut right_row)? else {
            break;
        };
        let PartialRowSelection {
            row,
            advance_left,
            advance_right,
            released_bytes,
        } = selection;
        let binding = encode_partial_binding(row.key, row.states)?;
        writer.write(row.ordinal, &binding, spill_budget)?;
        tracker.release(released_bytes);
        if advance_left {
            left_row = read_partial_row(
                &mut left_reader,
                items,
                memory_budget,
                spill_budget,
                &mut tracker,
            )?;
        }
        if advance_right {
            right_row = read_partial_row(
                &mut right_reader,
                items,
                memory_budget,
                spill_budget,
                &mut tracker,
            )?;
        }
    }
    writer.finish()?;
    Ok(run)
}

fn merge_partial_runs(
    runs: &[spill::SpillRun],
    spill_budget: &SpillBudgetTracker,
    blocking_account: &QueryMemoryAccount,
    context: AggregateExecutionContext<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut readers = runs
        .iter()
        .map(spill::SpillRun::reader)
        .collect::<Result<Vec<_>>>()?;
    if readers.len() > 2 {
        return Err(SkeinError::Execution(
            "AggregateExec partial merge fan-in exceeds two runs".to_string(),
        ));
    }
    let mut tracker =
        OperatorMemoryTracker::with_account(context.memory_budget, blocking_account.clone());
    let mut left = if let Some(reader) = readers.get_mut(0) {
        read_partial_row(
            reader,
            context.items,
            context.memory_budget,
            spill_budget,
            &mut tracker,
        )?
    } else {
        None
    };
    let mut right = if let Some(reader) = readers.get_mut(1) {
        read_partial_row(
            reader,
            context.items,
            context.memory_budget,
            spill_budget,
            &mut tracker,
        )?
    } else {
        None
    };
    let mut output = AccountedBindingBatch::with_ledger(
        "AggregateExec",
        context.batch_rows,
        context.output_memory_budget,
        context.memory_ledger,
    );
    let mut emitted = 0usize;
    while left.is_some() || right.is_some() {
        runtime_checkpoint(context.task_context)?;
        let Some(selection) = take_next_partial_row(&mut left, &mut right)? else {
            break;
        };
        let PartialRowSelection {
            row,
            advance_left,
            advance_right,
            released_bytes,
        } = selection;
        let binding = finish_partial_group(row.key, row.states, context.group_keys, context.items);
        if output.transfer_from(&mut tracker, released_bytes, binding, emit)? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        emitted = emitted.saturating_add(1);
        if flush_aggregate_batch(&mut output, emitted, context.execution_limit, emit)?
            == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        if advance_left {
            left = read_partial_row(
                &mut readers[0],
                context.items,
                context.memory_budget,
                spill_budget,
                &mut tracker,
            )?;
        }
        if advance_right {
            right = read_partial_row(
                &mut readers[1],
                context.items,
                context.memory_budget,
                spill_budget,
                &mut tracker,
            )?;
        }
    }
    if !output.is_empty() && output.emit(emit)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

struct PartialRowSelection {
    row: PartialRunRow,
    advance_left: bool,
    advance_right: bool,
    released_bytes: usize,
}

fn take_next_partial_row(
    left: &mut Option<PartialRunRow>,
    right: &mut Option<PartialRunRow>,
) -> Result<Option<PartialRowSelection>> {
    let ordering = match (left.as_ref(), right.as_ref()) {
        (None, None) => return Ok(None),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (Some(left), Some(right)) => left.key.cmp(&right.key),
    };
    let selection = match ordering {
        Ordering::Less => {
            let row = left.take().expect("left partial row exists");
            let released_bytes = row.memory_bytes();
            PartialRowSelection {
                row,
                advance_left: true,
                advance_right: false,
                released_bytes,
            }
        }
        Ordering::Greater => {
            let row = right.take().expect("right partial row exists");
            let released_bytes = row.memory_bytes();
            PartialRowSelection {
                row,
                advance_left: false,
                advance_right: true,
                released_bytes,
            }
        }
        Ordering::Equal => {
            let mut row = left.take().expect("left partial row exists");
            let other = right.take().expect("right partial row exists");
            let released_bytes = row.memory_bytes().saturating_add(other.memory_bytes());
            row.ordinal = row.ordinal.min(other.ordinal);
            merge_partial_states(&mut row.states, other.states)?;
            PartialRowSelection {
                row,
                advance_left: true,
                advance_right: true,
                released_bytes,
            }
        }
    };
    Ok(Some(selection))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_merge_memory_preflight_matches_materialized_merge() {
        let key = vec![Value::String("group".to_string())];
        let states = vec![
            AggregateState::Count {
                count: 3,
                distinct: None,
            },
            AggregateState::Min(Some(Value::String("z".repeat(8)))),
            AggregateState::Max(Some(Value::String("a".repeat(128)))),
            AggregateState::Avg {
                sum: 12.0,
                count: 3,
            },
        ];
        let incoming = vec![
            AggregateState::Count {
                count: 2,
                distinct: None,
            },
            AggregateState::Min(Some(Value::String("a".repeat(128)))),
            AggregateState::Max(Some(Value::String("z".repeat(8)))),
            AggregateState::Avg {
                sum: 20.0,
                count: 2,
            },
        ];

        let expected = partial_group_memory_bytes_after_merge(&key, &states, &incoming).unwrap();
        let mut materialized = states.clone();
        merge_partial_states(&mut materialized, incoming).unwrap();

        assert_eq!(expected, partial_group_memory_bytes(&key, &materialized));
    }

    #[test]
    fn partial_merge_memory_preflight_preserves_budget_boundary() {
        let key = vec![Value::Int(1)];
        let states = vec![AggregateState::Min(Some(Value::String("z".repeat(8))))];
        let incoming = vec![AggregateState::Min(Some(Value::String("a".repeat(128))))];
        let previous_bytes = partial_group_memory_bytes(&key, &states);
        let next_bytes = partial_group_memory_bytes_after_merge(&key, &states, &incoming).unwrap();
        let delta = MemoryDelta::between(previous_bytes, next_bytes);
        assert!(next_bytes > previous_bytes);

        let mut exact = OperatorMemoryTracker::new(NonZeroUsize::new(next_bytes).unwrap());
        exact.try_charge(previous_bytes).unwrap();
        exact.try_charge(delta.added_bytes).unwrap();
        exact.release(delta.released_bytes);
        assert_eq!(exact.used_bytes, next_bytes);

        let mut insufficient =
            OperatorMemoryTracker::new(NonZeroUsize::new(next_bytes - 1).unwrap());
        insufficient.try_charge(previous_bytes).unwrap();
        let error = insufficient.try_charge(delta.added_bytes).unwrap_err();
        assert!(error.to_string().contains("exceeding its"));
    }

    #[test]
    fn partial_merge_memory_preflight_rejects_incompatible_states() {
        let states = vec![AggregateState::Min(None)];
        let incoming = vec![AggregateState::Max(None)];

        let error = partial_group_memory_bytes_after_merge(&[], &states, &incoming).unwrap_err();

        assert!(error.to_string().contains("incompatible partial states"));
    }
}
