use super::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum AggregateDistinctValue {
    Identity(u8, u64),
    Value(Value),
}

enum AggregateInput {
    Missing,
    Present,
    Identity(u8, u64),
    Value(Value),
}

#[derive(Clone)]
enum AggregateState {
    Count {
        count: usize,
        distinct: Option<HashSet<AggregateDistinctValue>>,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    Avg {
        sum: f64,
        count: usize,
    },
    Collect {
        values: Vec<Value>,
        distinct: Option<HashSet<Value>>,
    },
}

fn incompatible_partial_states_error() -> SkeinError {
    SkeinError::Execution("AggregateExec encountered incompatible partial states".to_string())
}

#[derive(Default)]
struct MemoryDelta {
    added_bytes: usize,
    released_bytes: usize,
}

impl MemoryDelta {
    fn between(previous: usize, next: usize) -> Self {
        if next >= previous {
            Self {
                added_bytes: next - previous,
                released_bytes: 0,
            }
        } else {
            Self {
                added_bytes: 0,
                released_bytes: previous - next,
            }
        }
    }

    fn combine(&mut self, other: Self) {
        self.added_bytes = self.added_bytes.saturating_add(other.added_bytes);
        self.released_bytes = self.released_bytes.saturating_add(other.released_bytes);
    }
}

impl AggregateState {
    fn new(item: &Aggregation) -> Self {
        match item.function {
            AggregateFunction::Count => Self::Count {
                count: 0,
                distinct: item.distinct.then(HashSet::new),
            },
            AggregateFunction::Min => Self::Min(None),
            AggregateFunction::Max => Self::Max(None),
            AggregateFunction::Avg => Self::Avg { sum: 0.0, count: 0 },
            AggregateFunction::Collect => Self::Collect {
                values: Vec::new(),
                distinct: item.distinct.then(HashSet::new),
            },
        }
    }

    fn update(&mut self, item: &Aggregation, catalog: &Catalog, binding: &Binding) -> MemoryDelta {
        self.update_input(aggregate_input(item, catalog, binding))
    }

    fn update_input(&mut self, input: AggregateInput) -> MemoryDelta {
        match self {
            Self::Count { count, distinct } => {
                if distinct.is_none() {
                    if !matches!(input, AggregateInput::Missing) {
                        *count = count.saturating_add(1);
                    }
                    return MemoryDelta::default();
                }
                let value = match input {
                    AggregateInput::Present => {
                        *count = count.saturating_add(1);
                        return MemoryDelta::default();
                    }
                    AggregateInput::Identity(kind, id) => {
                        Some(AggregateDistinctValue::Identity(kind, id))
                    }
                    AggregateInput::Value(value) => Some(AggregateDistinctValue::Value(value)),
                    AggregateInput::Missing => None,
                };
                let Some(value) = value else {
                    return MemoryDelta::default();
                };
                let value_bytes = aggregate_distinct_value_memory_bytes(&value)
                    .saturating_sub(std::mem::size_of::<AggregateDistinctValue>());
                if let Some(distinct) = distinct {
                    let previous =
                        hash_set_capacity_bytes::<AggregateDistinctValue>(distinct.capacity());
                    if distinct.insert(value) {
                        *count = count.saturating_add(1);
                        return MemoryDelta {
                            added_bytes: value_bytes.saturating_add(
                                hash_set_capacity_bytes::<AggregateDistinctValue>(
                                    distinct.capacity(),
                                )
                                .saturating_sub(previous),
                            ),
                            released_bytes: 0,
                        };
                    }
                } else {
                    *count = count.saturating_add(1);
                }
                MemoryDelta::default()
            }
            Self::Min(current) => {
                if let AggregateInput::Value(value) = input
                    && current.as_ref().is_none_or(|current| value < *current)
                {
                    let previous = current.as_ref().map_or(0, value_memory_bytes);
                    let next = value_memory_bytes(&value);
                    *current = Some(value);
                    return MemoryDelta::between(previous, next);
                }
                MemoryDelta::default()
            }
            Self::Max(current) => {
                if let AggregateInput::Value(value) = input
                    && current.as_ref().is_none_or(|current| value > *current)
                {
                    let previous = current.as_ref().map_or(0, value_memory_bytes);
                    let next = value_memory_bytes(&value);
                    *current = Some(value);
                    return MemoryDelta::between(previous, next);
                }
                MemoryDelta::default()
            }
            Self::Avg { sum, count } => {
                if let AggregateInput::Value(value) = input {
                    match value {
                        Value::Int(value) => {
                            *sum += value as f64;
                            *count = count.saturating_add(1);
                        }
                        Value::Float(value) if value.is_finite() => {
                            *sum += value;
                            *count = count.saturating_add(1);
                        }
                        _ => {}
                    }
                }
                MemoryDelta::default()
            }
            Self::Collect { values, distinct } => {
                let AggregateInput::Value(value) = input else {
                    return MemoryDelta::default();
                };
                let value_bytes = value_memory_bytes(&value);
                if let Some(distinct) = distinct {
                    let previous = hash_set_capacity_bytes::<Value>(distinct.capacity());
                    if distinct.insert(value) {
                        return MemoryDelta {
                            // The inline value charge also reserves the sorted
                            // COLLECT(DISTINCT) output vector before finish.
                            added_bytes: value_bytes.saturating_add(
                                hash_set_capacity_bytes::<Value>(distinct.capacity())
                                    .saturating_sub(previous),
                            ),
                            released_bytes: 0,
                        };
                    }
                } else {
                    values.push(value);
                    return MemoryDelta {
                        added_bytes: value_bytes.saturating_add(std::mem::size_of::<usize>() * 4),
                        released_bytes: 0,
                    };
                }
                MemoryDelta::default()
            }
        }
    }

    fn finish(self) -> Value {
        match self {
            Self::Count { count, .. } => Value::Int(count as i64),
            Self::Min(value) | Self::Max(value) => value.unwrap_or(Value::Null),
            Self::Avg { sum, count } if count > 0 => Value::Float(sum / count as f64),
            Self::Avg { .. } => Value::Null,
            Self::Collect {
                values,
                distinct: None,
            } => Value::List(values),
            Self::Collect {
                distinct: Some(values),
                ..
            } => {
                let mut values = values.into_iter().collect::<Vec<_>>();
                values.sort_unstable();
                Value::List(values)
            }
        }
    }

    fn merge_partial(&mut self, other: Self) -> Result<MemoryDelta> {
        match (self, other) {
            (
                Self::Count {
                    count,
                    distinct: None,
                },
                Self::Count {
                    count: other,
                    distinct: None,
                },
            ) => {
                *count = count.saturating_add(other);
                Ok(MemoryDelta::default())
            }
            (Self::Min(current), Self::Min(other)) => {
                let previous = current.as_ref().map_or(0, value_memory_bytes);
                if let Some(other) = other
                    && current.as_ref().is_none_or(|current| other < *current)
                {
                    *current = Some(other);
                }
                let next = current.as_ref().map_or(0, value_memory_bytes);
                Ok(MemoryDelta::between(previous, next))
            }
            (Self::Max(current), Self::Max(other)) => {
                let previous = current.as_ref().map_or(0, value_memory_bytes);
                if let Some(other) = other
                    && current.as_ref().is_none_or(|current| other > *current)
                {
                    *current = Some(other);
                }
                let next = current.as_ref().map_or(0, value_memory_bytes);
                Ok(MemoryDelta::between(previous, next))
            }
            (
                Self::Avg { sum, count },
                Self::Avg {
                    sum: other_sum,
                    count: other_count,
                },
            ) => {
                *sum += other_sum;
                *count = count.saturating_add(other_count);
                Ok(MemoryDelta::default())
            }
            _ => Err(incompatible_partial_states_error()),
        }
    }

    fn partial_memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(match self {
            Self::Min(Some(value)) | Self::Max(Some(value)) => value_memory_bytes(value),
            Self::Count { .. }
            | Self::Min(None)
            | Self::Max(None)
            | Self::Avg { .. }
            | Self::Collect { .. } => 0,
        })
    }

    fn partial_memory_bytes_after_merge(&self, other: &Self) -> Result<usize> {
        let retained_value = match (self, other) {
            (Self::Count { distinct: None, .. }, Self::Count { distinct: None, .. })
            | (Self::Avg { .. }, Self::Avg { .. }) => return Ok(self.partial_memory_bytes()),
            (Self::Min(current), Self::Min(incoming)) => match (current, incoming) {
                (Some(current), Some(incoming)) if incoming < current => Some(incoming),
                (Some(current), _) => Some(current),
                (None, incoming) => incoming.as_ref(),
            },
            (Self::Max(current), Self::Max(incoming)) => match (current, incoming) {
                (Some(current), Some(incoming)) if incoming > current => Some(incoming),
                (Some(current), _) => Some(current),
                (None, incoming) => incoming.as_ref(),
            },
            _ => return Err(incompatible_partial_states_error()),
        };
        Ok(
            std::mem::size_of::<Self>()
                .saturating_add(retained_value.map_or(0, value_memory_bytes)),
        )
    }
}

fn aggregate_distinct_value_memory_bytes(value: &AggregateDistinctValue) -> usize {
    std::mem::size_of::<AggregateDistinctValue>().saturating_add(match value {
        AggregateDistinctValue::Identity(_, _) => 0,
        AggregateDistinctValue::Value(value) => value_memory_bytes(value),
    })
}

fn aggregate_property_value(target: &AggregateTarget, binding: &Binding) -> Option<Value> {
    let AggregateTarget::Property { variable, property } = target else {
        return None;
    };
    binding_property(binding, variable, property)
        .filter(|value| *value != &Value::Null)
        .cloned()
}

fn aggregate_input(item: &Aggregation, catalog: &Catalog, binding: &Binding) -> AggregateInput {
    match &item.target {
        AggregateTarget::All => AggregateInput::Present,
        AggregateTarget::Variable(variable) => match item.function {
            AggregateFunction::Count if item.distinct => binding_identity_key(binding, variable)
                .map_or(AggregateInput::Missing, |(kind, id)| {
                    AggregateInput::Identity(kind, id)
                }),
            AggregateFunction::Count => {
                if binding_has_variable(binding, variable) {
                    AggregateInput::Present
                } else {
                    AggregateInput::Missing
                }
            }
            AggregateFunction::Collect => binding_value(binding, catalog, variable)
                .filter(|value| value != &Value::Null)
                .map_or(AggregateInput::Missing, AggregateInput::Value),
            AggregateFunction::Min | AggregateFunction::Max | AggregateFunction::Avg => {
                AggregateInput::Missing
            }
        },
        AggregateTarget::Property { .. } => aggregate_property_value(&item.target, binding)
            .map_or(AggregateInput::Missing, AggregateInput::Value),
    }
}

fn aggregate_input_memory_bytes(input: &AggregateInput) -> usize {
    std::mem::size_of::<AggregateInput>().saturating_add(match input {
        AggregateInput::Value(value) => {
            value_memory_bytes(value).saturating_sub(std::mem::size_of::<Value>())
        }
        AggregateInput::Missing | AggregateInput::Present | AggregateInput::Identity(_, _) => 0,
    })
}

struct GroupAccumulator<'a> {
    key: Vec<Value>,
    group_keys: &'a [Projection],
    items: &'a [Aggregation],
    states: Vec<AggregateState>,
}

impl<'a> GroupAccumulator<'a> {
    fn new(key: Vec<Value>, group_keys: &'a [Projection], items: &'a [Aggregation]) -> Self {
        Self {
            key,
            group_keys,
            items,
            states: items.iter().map(AggregateState::new).collect(),
        }
    }

    fn base_memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.key.iter().fold(0usize, |total, value| {
                total.saturating_add(value_memory_bytes(value))
            }))
            .saturating_add(
                self.states
                    .len()
                    .saturating_mul(std::mem::size_of::<AggregateState>()),
            )
    }

    fn update(&mut self, catalog: &Catalog, binding: &Binding) -> MemoryDelta {
        let mut delta = MemoryDelta::default();
        for (state, item) in self.states.iter_mut().zip(self.items) {
            delta.combine(state.update(item, catalog, binding));
        }
        delta
    }

    fn update_inputs(&mut self, inputs: Vec<AggregateInput>) -> Result<MemoryDelta> {
        if inputs.len() != self.states.len() {
            return Err(SkeinError::Execution(
                "AggregateExec compact input width mismatch".to_string(),
            ));
        }
        let mut delta = MemoryDelta::default();
        for (state, input) in self.states.iter_mut().zip(inputs) {
            delta.combine(state.update_input(input));
        }
        Ok(delta)
    }

    fn finish(self) -> Binding {
        let mut values = BTreeMap::new();
        for (item, value) in self.group_keys.iter().zip(self.key) {
            insert_projected_value(&mut values, &item.name, value);
        }
        for (item, state) in self.items.iter().zip(self.states) {
            insert_projected_value(&mut values, &item.name, state.finish());
        }
        Binding {
            values,
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }
    }
}

struct GroupRunRow {
    key: Vec<Value>,
    ordinal: u64,
    inputs: Vec<AggregateInput>,
}

struct BufferedGroupRow {
    ordinal: u64,
    inputs: Vec<AggregateInput>,
}

impl BufferedGroupRow {
    fn payload_bytes(&self) -> usize {
        self.inputs.iter().fold(0usize, |total, input| {
            total.saturating_add(aggregate_input_memory_bytes(input))
        })
    }
}

type BufferedGroups = HashGroups<Vec<Value>, Vec<BufferedGroupRow>>;

fn buffered_key_bytes(key: &[Value]) -> usize {
    key.iter().fold(0usize, |total, value| {
        total.saturating_add(value_memory_bytes(value))
    })
}

fn buffered_row_insertion_bytes(rows: &Vec<BufferedGroupRow>, payload_bytes: usize) -> usize {
    if rows.len() == rows.capacity() {
        payload_bytes.saturating_add(
            rows.capacity()
                .saturating_mul(2)
                .max(1)
                .saturating_mul(std::mem::size_of::<BufferedGroupRow>()),
        )
    } else {
        payload_bytes
    }
}

fn push_buffered_row(
    rows: &mut Vec<BufferedGroupRow>,
    row: BufferedGroupRow,
    tracker: &mut OperatorMemoryTracker,
) -> Result<()> {
    tracker.try_charge(buffered_row_insertion_bytes(rows, row.payload_bytes()))?;
    if rows.len() == rows.capacity() {
        let old_bytes = rows
            .capacity()
            .saturating_mul(std::mem::size_of::<BufferedGroupRow>());
        let mut next = Vec::with_capacity(rows.capacity().saturating_mul(2).max(1));
        next.append(rows);
        *rows = next;
        tracker.release(old_bytes);
    }
    rows.push(row);
    Ok(())
}

impl GroupRunRow {
    fn cmp_key(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }

    fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.key.iter().fold(0usize, |total, value| {
                total.saturating_add(value_memory_bytes(value))
            }))
            .saturating_add(self.inputs.iter().fold(0usize, |total, input| {
                total.saturating_add(aggregate_input_memory_bytes(input))
            }))
    }
}

struct GroupMergeEntry {
    row: GroupRunRow,
    run_index: usize,
}

mod compact;
mod partial;

use compact::{decode_compact_group_binding, encode_compact_group_binding};
use partial::{partial_aggregation_is_mergeable, stream_partial_aggregate_batches};

#[derive(Clone, Copy)]
struct AggregateExecutionContext<'a> {
    group_keys: &'a [Projection],
    items: &'a [Aggregation],
    catalog: &'a Catalog,
    batch_rows: usize,
    memory_budget: NonZeroUsize,
    output_memory_budget: NonZeroUsize,
    memory_ledger: &'a QueryMemoryLedger,
    execution_limit: ExecutionLimit,
    task_context: Option<&'a RuntimeTaskContext>,
}

impl PartialEq for GroupMergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.row.cmp_key(&other.row) == Ordering::Equal && self.run_index == other.run_index
    }
}

impl Eq for GroupMergeEntry {}

impl Ord for GroupMergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .row
            .cmp_key(&self.row)
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl PartialOrd for GroupMergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn stream_aggregate_batches(
    input: &PhysicalPlan,
    group_keys: &[Projection],
    items: &[Aggregation],
    source: &mut dyn BindingBatchSource,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let primary_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "AggregateExec",
        context.memory.blocking_operator_bytes,
    );
    let BlockingExecutionContext {
        catalog,
        memory,
        memory_ledger,
        task_context,
        observer,
    } = context;
    runtime_checkpoint(task_context)?;
    if group_keys.is_empty() {
        let mut accumulator = GroupAccumulator::new(Vec::new(), group_keys, items);
        let mut tracker =
            OperatorMemoryTracker::with_account(memory.blocking_operator_bytes, primary_account);
        let base_bytes = accumulator.base_memory_bytes();
        ensure_operator_item_fits("AggregateExec", base_bytes, &tracker)?;
        tracker.try_charge(base_bytes)?;
        let mut input_rows = 0usize;
        source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
            runtime_checkpoint(task_context)?;
            for binding in &batch {
                update_group_accumulator(&mut accumulator, catalog, binding, &mut tracker)?;
                input_rows = input_rows.saturating_add(1);
            }
            Ok(BatchControl::Continue)
        })?;
        observer.record_blocking_memory_report(in_memory_report(
            "AggregateExec",
            &tracker,
            tracker.peak_bytes,
            input_rows,
            memory,
        ));
        let mut output = AccountedBindingBatch::with_ledger(
            "AggregateExec",
            memory.batch_rows.get(),
            memory.batch_payload_bytes,
            memory_ledger,
        );
        let binding = accumulator.finish();
        let source_bytes = tracker.used_bytes;
        if output.transfer_from(&mut tracker, source_bytes, binding, emit)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        return output.emit(emit);
    }

    if items.iter().all(partial_aggregation_is_mergeable) {
        return stream_partial_aggregate_batches(
            input,
            group_keys,
            items,
            source,
            AggregateExecutionContext {
                group_keys,
                items,
                catalog,
                batch_rows: memory.batch_rows.get(),
                memory_budget: memory.blocking_operator_bytes,
                output_memory_budget: memory.batch_payload_bytes,
                memory_ledger,
                execution_limit,
                task_context,
            },
            memory,
            observer,
            emit,
        );
    }

    let mut tracker = OperatorMemoryTracker::with_account(
        memory.blocking_operator_bytes,
        primary_account.clone(),
    );
    let mut spill_budget = SpillBudgetTracker::with_ledger("AggregateExec", memory, memory_ledger);
    let mut groups = BufferedGroups::default();
    let key_hasher = RandomState::new();
    let mut runs = Vec::<spill::SpillRun>::new();
    let mut ordinal = 0u64;
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(task_context)?;
        for binding in batch {
            let key = group_keys
                .iter()
                .map(|item| group_key_value(item, catalog, &binding))
                .collect::<Vec<_>>();
            let inputs = items
                .iter()
                .map(|item| aggregate_input(item, catalog, &binding))
                .collect::<Vec<_>>();
            let row = BufferedGroupRow { ordinal, inputs };
            let key_bytes = buffered_key_bytes(&key);
            let bytes = std::mem::size_of::<GroupRunRow>()
                .saturating_add(key_bytes)
                .saturating_add(row.payload_bytes());
            ensure_operator_item_fits("AggregateExec", bytes, &tracker)?;
            let key = HashedKey::new(key, &key_hasher);
            let new_group_payload = key_bytes
                .saturating_add(std::mem::size_of::<BufferedGroupRow>())
                .saturating_add(row.payload_bytes());
            let insertion_bytes = if let Some(rows) = groups.get(&key) {
                buffered_row_insertion_bytes(rows, row.payload_bytes())
            } else {
                groups.insertion_bytes(new_group_payload)
            };
            if tracker.would_exceed(insertion_bytes) && !groups.is_empty() {
                runs.push(spill_group_run(
                    &mut groups,
                    &mut spill_budget,
                    task_context,
                )?);
                tracker.reset();
            }
            if let Some(rows) = groups.get_mut(&key) {
                push_buffered_row(rows, row, &mut tracker)?;
            } else {
                groups.insert(key, vec![row], new_group_payload, &mut tracker)?;
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
        let aggregate_context = AggregateExecutionContext {
            group_keys,
            items,
            catalog,
            batch_rows: memory.batch_rows.get(),
            memory_budget: memory.blocking_operator_bytes,
            output_memory_budget: memory.batch_payload_bytes,
            memory_ledger,
            execution_limit,
            task_context,
        };
        return aggregate_buffered_groups(groups, tracker, aggregate_context, emit);
    }
    if !groups.is_empty() {
        runs.push(spill_group_run(
            &mut groups,
            &mut spill_budget,
            task_context,
        )?);
        tracker.reset();
    }
    runs = compact_group_runs(
        runs,
        items.len(),
        memory,
        &mut spill_budget,
        &primary_account,
        task_context,
    )?;
    observer.record_blocking_memory_report(spill_backed_report(
        "AggregateExec",
        &tracker,
        tracker.peak_bytes,
        ordinal as usize,
        &spill_budget,
        ordinal as usize,
    ));
    let aggregate_context = AggregateExecutionContext {
        group_keys,
        items,
        catalog,
        batch_rows: memory.batch_rows.get(),
        memory_budget: memory.blocking_operator_bytes,
        output_memory_budget: memory.batch_payload_bytes,
        memory_ledger,
        execution_limit,
        task_context,
    };
    merge_group_runs(&runs, &spill_budget, aggregate_context, emit)
}

fn spill_group_run(
    groups: &mut BufferedGroups,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let (run, mut writer) = spill_budget.create_run("aggregate")?;
    let (groups, _) = std::mem::take(groups).into_sorted();
    for (mut key, rows) in groups {
        let mut rows = rows.into_iter();
        while let Some(row) = rows.next() {
            runtime_checkpoint(task_context)?;
            let row_key = if rows.len() == 0 {
                std::mem::take(&mut key)
            } else {
                key.clone()
            };
            let binding = encode_compact_group_binding(row_key, row.inputs);
            writer.write(row.ordinal, &binding, spill_budget)?;
        }
    }
    runtime_checkpoint(task_context)?;
    writer.finish()?;
    Ok(run)
}

fn compact_group_runs(
    mut runs: Vec<spill::SpillRun>,
    aggregate_input_count: usize,
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
            compacted.push(merge_group_run_pair(
                &left,
                &right,
                aggregate_input_count,
                memory,
                spill_budget,
                blocking_account,
                task_context,
            )?);
        }
        runs = compacted;
    }
    Ok(runs)
}

#[allow(clippy::too_many_arguments)]
fn merge_group_run_pair(
    left: &spill::SpillRun,
    right: &spill::SpillRun,
    aggregate_input_count: usize,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    blocking_account: &QueryMemoryAccount,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let mut readers = [left.reader()?, right.reader()?];
    let mut heap = BinaryHeap::new();
    let mut tracker = OperatorMemoryTracker::with_account(
        memory.blocking_operator_bytes,
        blocking_account.clone(),
    );
    let per_row_budget = memory.blocking_operator_bytes.get() / 2;
    for (run_index, reader) in readers.iter_mut().enumerate() {
        if let Some(entry) = read_group_merge_entry(
            reader,
            run_index,
            aggregate_input_count,
            memory.blocking_operator_bytes.get(),
            per_row_budget,
            spill_budget,
            &mut tracker,
        )? {
            heap.push(entry);
        }
    }
    let (run, mut writer) = spill_budget.create_run("aggregate-merge")?;
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        let run_index = entry.run_index;
        let entry_bytes = entry.row.memory_bytes();
        let binding = encode_compact_group_binding(entry.row.key, entry.row.inputs);
        writer.write(entry.row.ordinal, &binding, spill_budget)?;
        tracker.release(entry_bytes);
        if let Some(next) = read_group_merge_entry(
            &mut readers[run_index],
            run_index,
            aggregate_input_count,
            memory.blocking_operator_bytes.get(),
            per_row_budget,
            spill_budget,
            &mut tracker,
        )? {
            heap.push(next);
        }
    }
    writer.finish()?;
    Ok(run)
}

fn aggregate_buffered_groups(
    groups: BufferedGroups,
    mut input_tracker: OperatorMemoryTracker,
    context: AggregateExecutionContext<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let AggregateExecutionContext {
        group_keys,
        items,
        catalog: _,
        batch_rows,
        memory_budget,
        output_memory_budget,
        memory_ledger,
        execution_limit,
        task_context,
    } = context;
    runtime_checkpoint(task_context)?;
    let mut tracker = OperatorMemoryTracker::with_account(
        memory_budget,
        memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "AggregateExec group state",
            memory_budget,
        ),
    );
    let mut output = AccountedBindingBatch::with_ledger(
        "AggregateExec",
        batch_rows,
        output_memory_budget,
        memory_ledger,
    );
    let (groups, released_bytes) = groups.into_sorted();
    input_tracker.release(released_bytes);
    let mut emitted = 0usize;
    for (key, rows) in groups {
        runtime_checkpoint(task_context)?;
        let key_bytes = buffered_key_bytes(&key);
        let row_array_bytes = rows
            .capacity()
            .saturating_mul(std::mem::size_of::<BufferedGroupRow>());
        let mut accumulator = GroupAccumulator::new(key, group_keys, items);
        let base_bytes = accumulator.base_memory_bytes();
        ensure_operator_item_fits("AggregateExec group state", base_bytes, &tracker)?;
        input_tracker.transfer_to(key_bytes, &mut tracker, base_bytes)?;
        // Hash grouping retains input order within each group for COLLECT.
        for row in rows {
            runtime_checkpoint(task_context)?;
            let payload_bytes = row.payload_bytes();
            update_group_accumulator_inputs(&mut accumulator, row.inputs, &mut tracker)?;
            input_tracker.release(payload_bytes);
        }
        input_tracker.release(row_array_bytes);
        let binding = accumulator.finish();
        let source_bytes = tracker.used_bytes;
        if output.transfer_from(&mut tracker, source_bytes, binding, emit)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        emitted = emitted.saturating_add(1);
        if flush_aggregate_batch(&mut output, emitted, execution_limit, emit)? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
    }
    runtime_checkpoint(task_context)?;
    if !output.is_empty() && output.emit(emit)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

fn merge_group_runs(
    runs: &[spill::SpillRun],
    spill_budget: &SpillBudgetTracker,
    context: AggregateExecutionContext<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let AggregateExecutionContext {
        group_keys,
        items,
        catalog: _,
        batch_rows,
        memory_budget,
        output_memory_budget,
        memory_ledger,
        execution_limit,
        task_context,
    } = context;
    runtime_checkpoint(task_context)?;
    let mut accumulator_tracker = OperatorMemoryTracker::with_account(
        memory_budget,
        memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "AggregateExec accumulator",
            memory_budget,
        ),
    );
    let mut readers = runs
        .iter()
        .map(spill::SpillRun::reader)
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    let mut merge_tracker = OperatorMemoryTracker::with_account(
        memory_budget,
        memory_ledger.account(
            QueryMemoryClass::BlockingState,
            "AggregateExec merge",
            memory_budget,
        ),
    );
    for (run_index, reader) in readers.iter_mut().enumerate() {
        runtime_checkpoint(task_context)?;
        if let Some(entry) = read_group_merge_entry(
            reader,
            run_index,
            items.len(),
            memory_budget.get(),
            memory_budget.get(),
            spill_budget,
            &mut merge_tracker,
        )? {
            heap.push(entry);
        }
    }
    let mut output = AccountedBindingBatch::with_ledger(
        "AggregateExec",
        batch_rows,
        output_memory_budget,
        memory_ledger,
    );
    let mut accumulator: Option<GroupAccumulator<'_>> = None;
    let mut emitted = 0usize;
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        let row_bytes = entry.row.memory_bytes();
        let run_index = entry.run_index;
        let row = entry.row;
        if accumulator
            .as_ref()
            .is_some_and(|accumulator| accumulator.key != row.key)
        {
            let binding = accumulator.take().expect("group exists").finish();
            let source_bytes = accumulator_tracker.used_bytes;
            if output.transfer_from(&mut accumulator_tracker, source_bytes, binding, emit)?
                == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
            emitted = emitted.saturating_add(1);
            if flush_aggregate_batch(&mut output, emitted, execution_limit, emit)?
                == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
        }
        if accumulator.is_none() {
            let next = GroupAccumulator::new(row.key.clone(), group_keys, items);
            let base_bytes = next.base_memory_bytes();
            ensure_operator_item_fits(
                "AggregateExec group state",
                base_bytes,
                &accumulator_tracker,
            )?;
            accumulator_tracker.try_charge(base_bytes)?;
            accumulator = Some(next);
        }
        update_group_accumulator_inputs(
            accumulator.as_mut().expect("group exists"),
            row.inputs,
            &mut accumulator_tracker,
        )?;
        merge_tracker.release(row_bytes);
        if let Some(next) = read_group_merge_entry(
            &mut readers[run_index],
            run_index,
            items.len(),
            memory_budget.get(),
            memory_budget.get(),
            spill_budget,
            &mut merge_tracker,
        )? {
            heap.push(next);
        }
    }
    runtime_checkpoint(task_context)?;
    if let Some(accumulator) = accumulator {
        let binding = accumulator.finish();
        let source_bytes = accumulator_tracker.used_bytes;
        if output.transfer_from(&mut accumulator_tracker, source_bytes, binding, emit)?
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

#[allow(clippy::too_many_arguments)]
fn read_group_merge_entry(
    reader: &mut spill::SpillReader,
    run_index: usize,
    aggregate_input_count: usize,
    max_record_bytes: usize,
    max_item_bytes: usize,
    spill_budget: &SpillBudgetTracker,
    tracker: &mut OperatorMemoryTracker,
) -> Result<Option<GroupMergeEntry>> {
    reader
        .read_binding_record(max_record_bytes, spill_budget)?
        .map(|record| {
            record.try_map(
                "AggregateExec merge",
                max_item_bytes,
                tracker,
                |ordinal, binding| {
                    Ok(GroupMergeEntry {
                        row: decode_compact_group_binding(ordinal, binding, aggregate_input_count)?,
                        run_index,
                    })
                },
                |entry| entry.row.memory_bytes(),
            )
        })
        .transpose()
}

fn update_group_accumulator(
    accumulator: &mut GroupAccumulator<'_>,
    catalog: &Catalog,
    binding: &Binding,
    tracker: &mut OperatorMemoryTracker,
) -> Result<()> {
    let delta = accumulator.update(catalog, binding);
    tracker.release(delta.released_bytes);
    if tracker.would_exceed(delta.added_bytes) {
        return Err(SkeinError::Execution(format!(
            "AggregateExec state exceeds blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    tracker.try_charge(delta.added_bytes)?;
    Ok(())
}

fn update_group_accumulator_inputs(
    accumulator: &mut GroupAccumulator<'_>,
    inputs: Vec<AggregateInput>,
    tracker: &mut OperatorMemoryTracker,
) -> Result<()> {
    let delta = accumulator.update_inputs(inputs)?;
    tracker.release(delta.released_bytes);
    if tracker.would_exceed(delta.added_bytes) {
        return Err(SkeinError::Execution(format!(
            "AggregateExec state exceeds blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    tracker.try_charge(delta.added_bytes)?;
    Ok(())
}

fn flush_aggregate_batch(
    batch: &mut AccountedBindingBatch,
    emitted: usize,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    if (batch.is_full() || execution_limit.is_reached(emitted))
        && (batch.emit(emit)? == BatchControl::Stop || execution_limit.is_reached(emitted))
    {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}
