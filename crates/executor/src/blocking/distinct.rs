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

use super::*;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct DistinctKey {
    schema_id: usize,
    values: Vec<Value>,
}

#[derive(Debug, Default)]
struct DistinctSchemaInterner {
    schemas: Vec<Vec<String>>,
    memory_bytes: usize,
}

impl DistinctSchemaInterner {
    fn find(&self, binding: &Binding) -> Option<usize> {
        self.schemas
            .iter()
            .position(|schema| schema.iter().eq(binding.values.keys()))
    }

    fn next_schema_memory_bytes(binding: &Binding) -> usize {
        binding
            .values
            .keys()
            .fold(std::mem::size_of::<Vec<String>>(), |total, name| {
                total
                    .saturating_add(std::mem::size_of::<String>())
                    .saturating_add(name.len())
            })
    }

    fn insert(&mut self, binding: &Binding, memory_bytes: usize) -> usize {
        let schema_id = self.schemas.len();
        self.schemas.push(binding.values.keys().cloned().collect());
        self.memory_bytes = self.memory_bytes.saturating_add(memory_bytes);
        schema_id
    }
}

struct DistinctOperator<'a> {
    memory: &'a ExecutionMemoryConfig,
    task_context: Option<&'a RuntimeTaskContext>,
    observer: &'a dyn ExecutionObserver,
    blocking_account: QueryMemoryAccount,
    tracker: OperatorMemoryTracker,
    spill_budget: SpillBudgetTracker,
    schemas: DistinctSchemaInterner,
    distinct: HashMap<HashedKey<DistinctKey>, (u64, Binding)>,
    key_hasher: RandomState,
    runs: Vec<spill::SpillRun>,
    input_rows: u64,
}

pub fn stream_distinct_batches<P>(
    input: &P,
    source: &mut dyn BindingBatchSource<P>,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut operator = DistinctOperator::new(context);
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        for binding in batch {
            operator.push(binding)?;
        }
        Ok(BatchControl::Continue)
    })?;
    operator.finish(execution_limit, emit)
}

impl<'a> DistinctOperator<'a> {
    fn new(context: BlockingExecutionContext<'a>) -> Self {
        let blocking_account = context.operator_account("DistinctExec");
        let tracker = OperatorMemoryTracker::with_account(
            context.memory.blocking_operator_bytes,
            blocking_account.clone(),
        );
        Self {
            memory: context.memory,
            task_context: context.task_context,
            observer: context.observer,
            blocking_account,
            tracker,
            spill_budget: SpillBudgetTracker::with_ledger(
                "DistinctExec",
                context.memory,
                context.memory_ledger,
            ),
            schemas: DistinctSchemaInterner::default(),
            distinct: HashMap::new(),
            key_hasher: RandomState::new(),
            runs: Vec::new(),
            input_rows: 0,
        }
    }

    fn push(&mut self, binding: Binding) -> Result<()> {
        runtime_checkpoint(self.task_context)?;
        let existing_schema_id = self.schemas.find(&binding);
        let schema_id = existing_schema_id.unwrap_or(self.schemas.schemas.len());
        let schema_bytes = if existing_schema_id.is_none() {
            DistinctSchemaInterner::next_schema_memory_bytes(&binding)
        } else {
            0
        };
        let key = HashedKey::new(distinct_binding_key(&binding, schema_id), &self.key_hasher);
        let entry_bytes = binding_memory_bytes(&binding)
            .saturating_add(distinct_key_memory_bytes(&key.key))
            .saturating_add(hash_entry_overhead::<DistinctKey, (u64, Binding)>());
        let admitted_bytes = schema_bytes.saturating_add(entry_bytes);
        ensure_operator_item_fits("DistinctExec", admitted_bytes, &self.tracker)?;
        if !self.distinct.contains_key(&key) {
            if self.tracker.would_exceed(admitted_bytes) && !self.distinct.is_empty() {
                self.spill_current_run()?;
            }
            if existing_schema_id.is_none() {
                self.tracker.try_charge(schema_bytes)?;
                let inserted_schema_id = self.schemas.insert(&binding, schema_bytes);
                debug_assert_eq!(inserted_schema_id, schema_id);
            }
            self.tracker.try_charge(entry_bytes)?;
            self.distinct.insert(key, (self.input_rows, binding));
        }
        self.input_rows = self.input_rows.saturating_add(1);
        Ok(())
    }

    fn spill_current_run(&mut self) -> Result<()> {
        self.runs.push(spill_distinct_run(
            &mut self.distinct,
            &mut self.spill_budget,
            self.task_context,
        )?);
        self.tracker.release(
            self.tracker
                .used_bytes
                .saturating_sub(self.schemas.memory_bytes),
        );
        debug_assert_eq!(self.tracker.used_bytes, self.schemas.memory_bytes);
        Ok(())
    }

    fn finish(
        mut self,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        runtime_checkpoint(self.task_context)?;
        if self.runs.is_empty() {
            self.record_memory_report(0, self.tracker.peak_bytes);
            let mut selected = self.distinct.into_values().collect::<Vec<_>>();
            selected.sort_by_key(|(ordinal, _)| *ordinal);
            return emit_binding_iterator(
                selected
                    .into_iter()
                    .take(execution_limit.output_rows.unwrap_or(usize::MAX))
                    .map(|(_, binding)| binding),
                self.memory.batch_rows.get(),
                emit,
            );
        }
        if !self.distinct.is_empty() {
            self.spill_current_run()?;
        }
        let mut peak_tracked_bytes = self.tracker.peak_bytes;
        self.runs = compact_distinct_runs(
            self.runs,
            self.memory,
            &mut self.spill_budget,
            &self.blocking_account,
            &self.schemas,
            self.task_context,
            &mut peak_tracked_bytes,
        )?;
        let schemas = std::mem::take(&mut self.schemas);
        let schema_bytes = schemas.memory_bytes;
        drop(schemas);
        self.tracker.release(schema_bytes);
        self.record_memory_report(self.input_rows as usize, peak_tracked_bytes);
        emit_distinct_run(
            self.runs
                .first()
                .expect("compaction retains one distinct run"),
            DistinctRunExecutionContext {
                memory_budget: self.memory.blocking_operator_bytes,
                spill_budget: &self.spill_budget,
                blocking_account: &self.blocking_account,
                batch_rows: self.memory.batch_rows.get(),
                execution_limit,
                task_context: self.task_context,
            },
            emit,
        )
    }

    fn record_memory_report(&self, spilled_rows: usize, peak_tracked_bytes: usize) {
        self.observer
            .record_blocking_memory_report(spill_backed_report(
                "DistinctExec",
                &self.tracker,
                peak_tracked_bytes,
                self.input_rows as usize,
                &self.spill_budget,
                spilled_rows,
            ));
    }
}

fn distinct_binding_key(binding: &Binding, schema_id: usize) -> DistinctKey {
    DistinctKey {
        schema_id,
        values: binding.values.values().cloned().collect(),
    }
}

fn spill_distinct_run(
    distinct: &mut HashMap<HashedKey<DistinctKey>, (u64, Binding)>,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let (run, mut writer) = spill_budget.create_run("distinct")?;
    let mut sorted = std::mem::take(distinct).into_iter().collect::<Vec<_>>();
    sorted.sort_unstable_by(|(left, _), (right, _)| left.key.cmp(&right.key));
    for (_, (ordinal, binding)) in sorted {
        runtime_checkpoint(task_context)?;
        writer.write(ordinal, &binding, spill_budget)?;
    }
    writer.finish()?;
    Ok(run)
}

fn compact_distinct_runs(
    runs: Vec<spill::SpillRun>,
    memory: &ExecutionMemoryConfig,
    spill_budget: &mut SpillBudgetTracker,
    blocking_account: &QueryMemoryAccount,
    schemas: &DistinctSchemaInterner,
    task_context: Option<&RuntimeTaskContext>,
    peak_tracked_bytes: &mut usize,
) -> Result<Vec<spill::SpillRun>> {
    spill::compact_runs(runs, NonZeroUsize::MIN, task_context, |left, right| {
        merge_distinct_run_pair(
            left,
            right,
            spill_budget,
            DistinctMergeContext {
                memory,
                blocking_account,
                schemas,
                task_context,
            },
            peak_tracked_bytes,
        )
    })
}

struct DistinctRunRow {
    schema_id: usize,
    ordinal: u64,
    binding: Binding,
    memory_bytes: usize,
}

impl DistinctRunRow {
    fn cmp_key(&self, other: &Self) -> Ordering {
        self.schema_id.cmp(&other.schema_id).then_with(|| {
            self.binding
                .values
                .values()
                .cmp(other.binding.values.values())
        })
    }
}

#[derive(Clone, Copy)]
struct DistinctMergeContext<'a> {
    memory: &'a ExecutionMemoryConfig,
    blocking_account: &'a QueryMemoryAccount,
    schemas: &'a DistinctSchemaInterner,
    task_context: Option<&'a RuntimeTaskContext>,
}

fn read_distinct_run_row(
    reader: &mut spill::SpillReader,
    memory_limit: usize,
    spill_budget: &SpillBudgetTracker,
    tracker: &mut OperatorMemoryTracker,
    schemas: &DistinctSchemaInterner,
) -> Result<Option<DistinctRunRow>> {
    reader
        .read_binding_record(memory_limit, spill_budget)?
        .map(|record| {
            record.try_map(
                "DistinctExec merge",
                memory_limit,
                tracker,
                |ordinal, binding| {
                    let schema_id = schemas.find(&binding).ok_or_else(|| {
                        HawDBError::Execution(
                            "DistinctExec spill record has an unknown schema".to_string(),
                        )
                    })?;
                    let memory_bytes = binding_memory_bytes(&binding);
                    Ok(DistinctRunRow {
                        schema_id,
                        ordinal,
                        binding,
                        memory_bytes,
                    })
                },
                |row| row.memory_bytes,
            )
        })
        .transpose()
}

fn merge_distinct_run_pair(
    left: &spill::SpillRun,
    right: &spill::SpillRun,
    spill_budget: &mut SpillBudgetTracker,
    context: DistinctMergeContext<'_>,
    peak_tracked_bytes: &mut usize,
) -> Result<spill::SpillRun> {
    let DistinctMergeContext {
        memory,
        blocking_account,
        schemas,
        task_context,
    } = context;
    runtime_checkpoint(task_context)?;
    let merge_memory = memory
        .blocking_operator_bytes
        .get()
        .saturating_sub(schemas.memory_bytes);
    let per_row_memory = merge_memory / 2;
    if per_row_memory == 0 {
        return Err(HawDBError::Execution(
            "DistinctExec spill merge requires at least two bytes of blocking memory after schema interning"
                .to_string(),
        ));
    }
    let mut left_reader = left.reader()?;
    let mut right_reader = right.reader()?;
    let mut tracker = OperatorMemoryTracker::with_account(
        memory.blocking_operator_bytes,
        blocking_account.clone(),
    );
    let mut left_row = read_distinct_run_row(
        &mut left_reader,
        per_row_memory,
        spill_budget,
        &mut tracker,
        schemas,
    )?;
    let mut right_row = read_distinct_run_row(
        &mut right_reader,
        per_row_memory,
        spill_budget,
        &mut tracker,
        schemas,
    )?;
    let (run, mut writer) = spill_budget.create_run("distinct-merge")?;
    loop {
        runtime_checkpoint(task_context)?;
        *peak_tracked_bytes = (*peak_tracked_bytes).max(
            schemas
                .memory_bytes
                .saturating_add(left_row.as_ref().map_or(0, |row| row.memory_bytes))
                .saturating_add(right_row.as_ref().map_or(0, |row| row.memory_bytes)),
        );
        let selection = match (&left_row, &right_row) {
            (None, None) => break,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(left), Some(right)) => left.cmp_key(right),
        };
        let (selected, released_bytes) = match selection {
            Ordering::Less => {
                let released_bytes = left_row.as_ref().expect("left row exists").memory_bytes;
                (left_row.take(), released_bytes)
            }
            Ordering::Greater => {
                let released_bytes = right_row.as_ref().expect("right row exists").memory_bytes;
                (right_row.take(), released_bytes)
            }
            Ordering::Equal => {
                let released_bytes = left_row
                    .as_ref()
                    .expect("left row exists")
                    .memory_bytes
                    .saturating_add(right_row.as_ref().expect("right row exists").memory_bytes);
                let left = left_row.take().expect("left row exists");
                let right = right_row.take().expect("right row exists");
                (
                    Some(if left.ordinal <= right.ordinal {
                        left
                    } else {
                        right
                    }),
                    released_bytes,
                )
            }
        };
        let selected = selected.expect("distinct merge selected one row");
        writer.write(selected.ordinal, &selected.binding, spill_budget)?;
        tracker.release(released_bytes);
        if left_row.is_none() {
            left_row = read_distinct_run_row(
                &mut left_reader,
                per_row_memory,
                spill_budget,
                &mut tracker,
                schemas,
            )?;
        }
        if right_row.is_none() {
            right_row = read_distinct_run_row(
                &mut right_reader,
                per_row_memory,
                spill_budget,
                &mut tracker,
                schemas,
            )?;
        }
    }
    writer.finish()?;
    Ok(run)
}

struct DistinctRunExecutionContext<'a> {
    memory_budget: NonZeroUsize,
    spill_budget: &'a SpillBudgetTracker,
    blocking_account: &'a QueryMemoryAccount,
    batch_rows: usize,
    execution_limit: ExecutionLimit,
    task_context: Option<&'a RuntimeTaskContext>,
}

fn emit_distinct_run(
    run: &spill::SpillRun,
    context: DistinctRunExecutionContext<'_>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let DistinctRunExecutionContext {
        memory_budget,
        spill_budget,
        blocking_account,
        batch_rows,
        execution_limit,
        task_context,
    } = context;
    let mut reader = run.reader()?;
    let mut output = Vec::with_capacity(batch_rows);
    let mut tracker = OperatorMemoryTracker::with_account(memory_budget, blocking_account.clone());
    let mut emitted = 0usize;
    while let Some(record) = reader.read_binding_record(memory_budget.get(), spill_budget)? {
        runtime_checkpoint(task_context)?;
        if !output.is_empty()
            && tracker.would_exceed(record.decoded_binding_bytes())
            && emit_accounted_distinct_batch(&mut output, &mut tracker, batch_rows, emit)?
                == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        let binding = record.try_map(
            "DistinctExec output",
            memory_budget.get(),
            &mut tracker,
            |_, binding| Ok(binding),
            binding_memory_bytes,
        )?;
        output.push(binding);
        emitted = emitted.saturating_add(1);
        if output.len() == batch_rows
            && emit_accounted_distinct_batch(&mut output, &mut tracker, batch_rows, emit)?
                == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        if execution_limit.is_reached(emitted) {
            break;
        }
    }
    if !output.is_empty()
        && emit_accounted_distinct_batch(&mut output, &mut tracker, batch_rows, emit)?
            == BatchControl::Stop
    {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

fn emit_accounted_distinct_batch(
    batch: &mut BindingBatch,
    tracker: &mut OperatorMemoryTracker,
    batch_rows: usize,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let outgoing = std::mem::replace(batch, Vec::with_capacity(batch_rows));
    tracker.reset();
    emit(outgoing)
}

fn distinct_key_memory_bytes(key: &DistinctKey) -> usize {
    std::mem::size_of::<DistinctKey>().saturating_add(
        key.values.iter().fold(0usize, |total, value| {
            total.saturating_add(value_memory_bytes(value))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_interner_reuses_names_and_distinguishes_value_schemas() {
        let first = Binding::values(BTreeMap::from([
            ("left".to_string(), Value::Int(1)),
            ("right".to_string(), Value::Null),
        ]));
        let same_schema = Binding::values(BTreeMap::from([
            ("left".to_string(), Value::Int(2)),
            ("right".to_string(), Value::Null),
        ]));
        let different_schema = Binding::scalar("other", Value::Int(1));
        let mut interner = DistinctSchemaInterner::default();

        let first_bytes = DistinctSchemaInterner::next_schema_memory_bytes(&first);
        let first_id = interner.insert(&first, first_bytes);
        assert_eq!(interner.find(&same_schema), Some(first_id));
        assert_eq!(interner.memory_bytes, first_bytes);

        let other_bytes = DistinctSchemaInterner::next_schema_memory_bytes(&different_schema);
        let other_id = interner.insert(&different_schema, other_bytes);
        assert_ne!(other_id, first_id);
        assert_eq!(interner.memory_bytes, first_bytes + other_bytes);

        let first_key = distinct_binding_key(&first, first_id);
        let other_key = distinct_binding_key(&different_schema, other_id);
        assert_ne!(first_key, other_key);
    }
}
