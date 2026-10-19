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

struct SortRunRow {
    sort_values: Vec<(Value, SortDirection)>,
    ordinal: u64,
    binding: Binding,
}

struct SortOperator<'plan, 'runtime> {
    items: &'plan [SortItem],
    catalog: &'runtime Catalog,
    memory: &'runtime ExecutionMemoryConfig,
    memory_ledger: &'runtime QueryMemoryLedger,
    task_context: Option<&'runtime RuntimeTaskContext>,
    observer: &'runtime dyn ExecutionObserver,
    blocking_account: QueryMemoryAccount,
    key_memory: KeyMemory,
    spill_budget: SpillBudgetTracker,
    rows: Vec<SortRunRow>,
    tracker: OperatorMemoryTracker,
    runs: Vec<spill::SpillRun>,
    input_rows: u64,
}

impl SortRunRow {
    fn new(
        catalog: &Catalog,
        items: &[SortItem],
        ordinal: u64,
        binding: Binding,
        key_memory: &KeyMemory,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<PreparedKey<Self>> {
        let PreparedKey {
            allocation,
            values: sort_values,
        } = key_memory.sort(items, catalog, &binding, task_context)?;
        Ok(PreparedKey {
            values: Self {
                sort_values,
                ordinal,
                binding,
            },
            allocation,
        })
    }

    fn cmp_key(&self, other: &Self) -> Ordering {
        compare_sort_values(&self.sort_values, &other.sort_values)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }

    fn memory_bytes(&self) -> usize {
        binding_memory_bytes(&self.binding)
            .saturating_add(crate::binding::sort_values_memory_bytes(&self.sort_values))
    }
}

fn compare_sort_values(
    left: &[(Value, SortDirection)],
    right: &[(Value, SortDirection)],
) -> Ordering {
    for ((left, direction), (right, other_direction)) in left.iter().zip(right) {
        debug_assert_eq!(direction, other_direction);
        let ordering = match direction {
            SortDirection::Asc => left.cmp(right),
            SortDirection::Desc => left.cmp(right).reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

struct SortMergeEntry {
    row: SortRunRow,
    run_index: usize,
}

impl PartialEq for SortMergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.row.cmp_key(&other.row) == Ordering::Equal && self.run_index == other.run_index
    }
}

impl Eq for SortMergeEntry {}

impl Ord for SortMergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .row
            .cmp_key(&self.row)
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl PartialOrd for SortMergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn stream_sort_batches(
    input: &PhysicalPlan,
    items: &[SortItem],
    source: &mut dyn BindingBatchSource,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut operator = SortOperator::new(items, context);
    runtime_checkpoint(operator.task_context)?;
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(operator.task_context)?;
        for binding in batch {
            operator.push(binding)?;
        }
        Ok(BatchControl::Continue)
    })?;
    operator.finish(execution_limit, emit)
}

impl<'plan, 'runtime> SortOperator<'plan, 'runtime> {
    fn new(items: &'plan [SortItem], context: BlockingExecutionContext<'runtime>) -> Self {
        let blocking_account = context.operator_account("SortExec");
        let key_memory = KeyMemory::new(
            &blocking_account,
            context.memory.query_memory_bytes,
            "ExecutionMemoryConfig.query_memory_bytes",
            "SortExec expressions",
            "SortExec keys",
        );
        let tracker = OperatorMemoryTracker::with_blocking_account(
            context.memory.blocking_operator_bytes,
            blocking_account.clone(),
        );
        Self {
            items,
            catalog: context.catalog,
            memory: context.memory,
            memory_ledger: context.memory_ledger,
            task_context: context.task_context,
            observer: context.observer,
            blocking_account,
            key_memory,
            tracker,
            spill_budget: SpillBudgetTracker::with_ledger(
                "SortExec",
                context.memory,
                context.memory_ledger,
            ),
            rows: Vec::new(),
            runs: Vec::new(),
            input_rows: 0,
        }
    }

    fn push(&mut self, binding: Binding) -> Result<()> {
        let PreparedKey {
            mut allocation,
            values: row,
        } = SortRunRow::new(
            self.catalog,
            self.items,
            self.input_rows,
            binding,
            &self.key_memory,
            self.task_context,
        )?;
        let bytes = row.memory_bytes();
        ensure_operator_item_fits("SortExec", bytes, &self.tracker)?;
        if self.tracker.would_exceed(bytes) {
            self.runs.push(spill_sort_run(
                &mut self.rows,
                &mut self.spill_budget,
                self.task_context,
            )?);
            self.tracker.reset();
        }
        self.tracker.try_charge_from(&mut allocation, bytes)?;
        self.rows.push(row);
        self.input_rows = self.input_rows.saturating_add(1);
        Ok(())
    }

    fn finish(
        mut self,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if self.runs.is_empty() {
            runtime_checkpoint(self.task_context)?;
            self.record_memory_report(0);
            self.rows.sort_by(SortRunRow::cmp_key);
            return emit_sorted_output(
                self.rows.into_iter().map(|row| {
                    let bytes = row.memory_bytes();
                    (bytes, row.binding)
                }),
                &mut self.tracker,
                AccountedBindingBatch::with_ledger(
                    "SortExec",
                    self.memory.batch_rows.get(),
                    self.memory.batch_payload_bytes,
                    self.memory_ledger,
                ),
                0,
                execution_limit.output_rows.unwrap_or(usize::MAX),
                self.task_context,
                emit,
            );
        }
        if !self.rows.is_empty() {
            self.runs.push(spill_sort_run(
                &mut self.rows,
                &mut self.spill_budget,
                self.task_context,
            )?);
            self.tracker.reset();
        }
        self.runs = compact_sort_runs(
            self.runs,
            self.items,
            self.catalog,
            self.memory,
            &self.spill_budget,
            &self.blocking_account,
            self.task_context,
        )?;
        self.record_memory_report(self.input_rows as usize);
        merge_sort_runs_with_output(
            &self.runs,
            self.items,
            self.catalog,
            OperatorMemoryTracker::with_blocking_account(
                self.memory.blocking_operator_bytes,
                self.blocking_account.clone(),
            ),
            &self.spill_budget,
            &self.key_memory,
            AccountedBindingBatch::with_ledger(
                "SortExec",
                self.memory.batch_rows.get(),
                self.memory.batch_payload_bytes,
                self.memory_ledger,
            ),
            0,
            execution_limit.output_rows.unwrap_or(usize::MAX),
            self.task_context,
            emit,
        )
    }

    fn record_memory_report(&self, spilled_rows: usize) {
        self.observer
            .record_blocking_memory_report(spill_backed_report(
                "SortExec",
                &self.tracker,
                self.tracker.peak_bytes,
                self.input_rows as usize,
                &self.spill_budget,
                spilled_rows,
            ));
    }
}

#[allow(clippy::too_many_arguments)]
pub fn stream_top_n_batches<P>(
    input: &P,
    items: &[SortItem],
    offset: usize,
    limit: usize,
    source: &mut dyn BindingBatchSource<P>,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let retained = offset.saturating_add(limit);
    if retained == 0 {
        return Ok(BatchControl::Continue);
    }
    let mut operator = TopNOperator::new(items, offset, limit, context);
    runtime_checkpoint(operator.task_context)?;
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(operator.task_context)?;
        for binding in batch {
            operator.push(binding)?;
        }
        Ok(BatchControl::Continue)
    })?;
    operator.finish(execution_limit, emit)
}

struct TopNOperator<'plan, 'runtime> {
    items: &'plan [SortItem],
    offset: usize,
    limit: usize,
    retained: usize,
    catalog: &'runtime Catalog,
    memory: &'runtime ExecutionMemoryConfig,
    memory_ledger: &'runtime QueryMemoryLedger,
    task_context: Option<&'runtime RuntimeTaskContext>,
    observer: &'runtime dyn ExecutionObserver,
    blocking_account: QueryMemoryAccount,
    key_memory: KeyMemory,
    spill_budget: SpillBudgetTracker,
    runs: Vec<spill::SpillRun>,
    heap: BinaryHeap<TopNBinding>,
    tracker: OperatorMemoryTracker,
    input_rows: u64,
    spilled_rows: usize,
}

impl<'plan, 'runtime> TopNOperator<'plan, 'runtime> {
    fn new(
        items: &'plan [SortItem],
        offset: usize,
        limit: usize,
        context: BlockingExecutionContext<'runtime>,
    ) -> Self {
        let blocking_account = context.operator_account("TopNExec");
        let key_memory = KeyMemory::new(
            &blocking_account,
            context.memory.query_memory_bytes,
            "ExecutionMemoryConfig.query_memory_bytes",
            "TopNExec expressions",
            "TopNExec keys",
        );
        let tracker = OperatorMemoryTracker::with_blocking_account(
            context.memory.blocking_operator_bytes,
            blocking_account.clone(),
        );
        Self {
            items,
            offset,
            limit,
            retained: offset.saturating_add(limit),
            catalog: context.catalog,
            memory: context.memory,
            memory_ledger: context.memory_ledger,
            task_context: context.task_context,
            observer: context.observer,
            blocking_account,
            key_memory,
            tracker,
            spill_budget: SpillBudgetTracker::with_ledger(
                "TopNExec",
                context.memory,
                context.memory_ledger,
            ),
            runs: Vec::new(),
            heap: BinaryHeap::new(),
            input_rows: 0,
            spilled_rows: 0,
        }
    }

    fn push(&mut self, binding: Binding) -> Result<()> {
        let PreparedKey {
            mut allocation,
            values: sort_values,
        } = self
            .key_memory
            .sort(self.items, self.catalog, &binding, self.task_context)?;
        let candidate = TopNBinding {
            sort_values,
            ordinal: self.input_rows,
            binding,
        };
        self.input_rows = self.input_rows.saturating_add(1);
        let bytes = candidate.memory_bytes();
        ensure_operator_item_fits("TopNExec", bytes, &self.tracker)?;
        if self.heap.len() < self.retained {
            if self.tracker.would_exceed(bytes) {
                self.spill_heap()?;
            }
            self.tracker.try_charge_from(&mut allocation, bytes)?;
            self.heap.push(candidate);
        } else if self.heap.peek().is_some_and(|worst| candidate < *worst) {
            let worst_bytes = self.heap.peek().map(TopNBinding::memory_bytes).unwrap_or(0);
            if self
                .tracker
                .used_bytes
                .saturating_sub(worst_bytes)
                .saturating_add(bytes)
                > self.tracker.budget_bytes
            {
                self.spill_heap()?;
            } else {
                self.heap.pop();
                self.tracker.release(worst_bytes);
            }
            self.tracker.try_charge_from(&mut allocation, bytes)?;
            self.heap.push(candidate);
        }
        Ok(())
    }

    fn spill_heap(&mut self) -> Result<()> {
        self.spilled_rows = self.spilled_rows.saturating_add(self.heap.len());
        self.runs.push(spill_top_n_run(
            &mut self.heap,
            &mut self.spill_budget,
            self.task_context,
        )?);
        self.tracker.reset();
        Ok(())
    }

    fn finish(
        mut self,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if !self.runs.is_empty() {
            if !self.heap.is_empty() {
                self.spill_heap()?;
            }
            self.runs = compact_sort_runs(
                self.runs,
                self.items,
                self.catalog,
                self.memory,
                &self.spill_budget,
                &self.blocking_account,
                self.task_context,
            )?;
            self.record_memory_report();
            return merge_sort_runs_with_output(
                &self.runs,
                self.items,
                self.catalog,
                OperatorMemoryTracker::with_blocking_account(
                    self.memory.blocking_operator_bytes,
                    self.blocking_account.clone(),
                ),
                &self.spill_budget,
                &self.key_memory,
                AccountedBindingBatch::with_ledger(
                    "TopNExec",
                    self.memory.batch_rows.get(),
                    self.memory.batch_payload_bytes,
                    self.memory_ledger,
                ),
                self.offset,
                self.limit
                    .min(execution_limit.output_rows.unwrap_or(usize::MAX)),
                self.task_context,
                emit,
            );
        }
        self.record_memory_report();
        let mut selected = self.heap.into_vec();
        selected.sort();
        emit_sorted_output(
            selected.into_iter().map(|entry| {
                let bytes = entry.memory_bytes();
                (bytes, entry.binding)
            }),
            &mut self.tracker,
            AccountedBindingBatch::with_ledger(
                "TopNExec",
                self.memory.batch_rows.get(),
                self.memory.batch_payload_bytes,
                self.memory_ledger,
            ),
            self.offset,
            self.limit
                .min(execution_limit.output_rows.unwrap_or(usize::MAX)),
            self.task_context,
            emit,
        )
    }

    fn record_memory_report(&self) {
        self.observer
            .record_blocking_memory_report(spill_backed_report(
                "TopNExec",
                &self.tracker,
                self.tracker.peak_bytes,
                self.input_rows as usize,
                &self.spill_budget,
                self.spilled_rows,
            ));
    }
}

fn emit_sorted_output(
    rows: impl IntoIterator<Item = (usize, Binding)>,
    tracker: &mut OperatorMemoryTracker,
    mut output: AccountedBindingBatch,
    offset: usize,
    limit: usize,
    task_context: Option<&RuntimeTaskContext>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut emitted = 0usize;
    for (ordinal, (source_bytes, binding)) in rows.into_iter().enumerate() {
        runtime_checkpoint(task_context)?;
        if ordinal < offset {
            drop(binding);
            tracker.release(source_bytes);
            continue;
        }
        if emitted == limit {
            break;
        }
        if output.transfer_from(tracker, source_bytes, binding, emit)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        emitted += 1;
        if emitted == limit {
            break;
        }
        if output.is_full() && output.emit(emit)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
    }
    // The for-loop has destroyed its iterator, including rows beyond the
    // result cap. Release their old retention charges before the final batch
    // asks a parent transform to admit its output.
    tracker.reset();
    runtime_checkpoint(task_context)?;
    output.emit(emit)
}

fn spill_sort_run(
    rows: &mut Vec<SortRunRow>,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    rows.sort_by(SortRunRow::cmp_key);
    let (run, mut writer) = spill_budget.create_run("sort")?;
    for row in rows.drain(..) {
        runtime_checkpoint(task_context)?;
        writer.note_merge_record_bytes(row.memory_bytes());
        writer.write(row.ordinal, &row.binding, spill_budget)?;
    }
    runtime_checkpoint(task_context)?;
    writer.finish()?;
    Ok(run)
}

pub fn spill_top_n_run(
    heap: &mut BinaryHeap<TopNBinding>,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let mut rows = std::mem::take(heap).into_vec();
    rows.sort();
    let (run, mut writer) = spill_budget.create_run("topn")?;
    for row in rows {
        runtime_checkpoint(task_context)?;
        writer.note_merge_record_bytes(row.memory_bytes());
        writer.write(row.ordinal, &row.binding, spill_budget)?;
    }
    runtime_checkpoint(task_context)?;
    writer.finish()?;
    Ok(run)
}

pub fn compact_sort_runs(
    runs: Vec<spill::SpillRun>,
    items: &[SortItem],
    catalog: &Catalog,
    memory: &ExecutionMemoryConfig,
    spill_budget: &SpillBudgetTracker,
    blocking_account: &QueryMemoryAccount,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<spill::SpillRun>> {
    if items
        .iter()
        .any(|item| matches!(item.key, hawdb_plan_cypher::SortKey::Expression(_)))
    {
        // Record-size child allowances cannot bound expression scratch space.
        // Share the query root serially until merges can reserve both separately.
        return spill::compact_runs_serial(
            runs,
            NonZeroUsize::new(2).unwrap(),
            task_context,
            |left, right| {
                merge_sort_run_pair(
                    left,
                    right,
                    items,
                    catalog,
                    memory,
                    spill_budget,
                    blocking_account,
                    task_context,
                )
            },
        );
    }
    spill::compact_runs_with_memory(
        runs,
        NonZeroUsize::new(2).expect("two-way merge fan-in"),
        spill::CompactionMemory {
            blocking: blocking_account,
            spill: spill_budget,
            worker_limit: spill::default_compaction_worker_limit(),
        },
        task_context,
        |left, right, blocking_account, spill_budget| {
            merge_sort_run_pair(
                left,
                right,
                items,
                catalog,
                memory,
                spill_budget,
                blocking_account,
                task_context,
            )
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn merge_sort_run_pair(
    left: &spill::SpillRun,
    right: &spill::SpillRun,
    items: &[SortItem],
    catalog: &Catalog,
    memory: &ExecutionMemoryConfig,
    spill_budget: &SpillBudgetTracker,
    blocking_account: &QueryMemoryAccount,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let mut readers = [left.reader()?, right.reader()?];
    let mut tracker = OperatorMemoryTracker::with_blocking_account(
        memory.blocking_operator_bytes,
        blocking_account.clone(),
    );
    let mut heap = BinaryHeap::new();
    let key_memory = KeyMemory::new(
        blocking_account,
        memory.query_memory_bytes,
        "ExecutionMemoryConfig.query_memory_bytes",
        "SortExec merge expressions",
        "SortExec merge keys",
    );
    let per_row_budget = memory.blocking_operator_bytes.get() / 2;
    for (run_index, reader) in readers.iter_mut().enumerate() {
        if let Some(entry) = read_sort_merge_entry(
            reader,
            run_index,
            items,
            catalog,
            memory.blocking_operator_bytes.get(),
            per_row_budget,
            spill_budget,
            &mut tracker,
            &key_memory,
            task_context,
        )? {
            heap.push(entry);
        }
    }
    let (run, mut writer) = spill_budget.create_run("sort-merge")?;
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        let run_index = entry.run_index;
        writer.note_merge_record_bytes(entry.row.memory_bytes());
        writer.write(entry.row.ordinal, &entry.row.binding, spill_budget)?;
        let bytes = entry.row.memory_bytes();
        drop(entry);
        tracker.release(bytes);
        if let Some(next) = read_sort_merge_entry(
            &mut readers[run_index],
            run_index,
            items,
            catalog,
            memory.blocking_operator_bytes.get(),
            per_row_budget,
            spill_budget,
            &mut tracker,
            &key_memory,
            task_context,
        )? {
            heap.push(next);
        }
    }
    writer.finish()?;
    Ok(run)
}

#[allow(clippy::too_many_arguments)]
pub fn merge_sort_runs(
    runs: &[spill::SpillRun],
    items: &[SortItem],
    catalog: &Catalog,
    memory_budget: NonZeroUsize,
    spill_budget: &SpillBudgetTracker,
    blocking_account: &QueryMemoryAccount,
    batch_rows: usize,
    skip_rows: usize,
    output_rows: usize,
    task_context: Option<&RuntimeTaskContext>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    // Preserve the existing entrypoint's single budget parameter while keeping
    // output separate from merge state and charged to the caller's query root.
    runtime_checkpoint(task_context)?;
    let output = AccountedBindingBatch::with_account(
        "SortExec",
        batch_rows,
        memory_budget,
        blocking_account.sibling(
            QueryMemoryClass::PipelineBatch,
            "SortExec output batch",
            memory_budget,
        ),
    );
    let key_memory = KeyMemory::new(
        blocking_account,
        memory_budget,
        "merge_sort_runs(memory_budget)",
        "SortExec merge expressions",
        "SortExec merge keys",
    );
    merge_sort_runs_with_output(
        runs,
        items,
        catalog,
        OperatorMemoryTracker::with_account(memory_budget, blocking_account.clone()),
        spill_budget,
        &key_memory,
        output,
        skip_rows,
        output_rows,
        task_context,
        emit,
    )
}

#[allow(clippy::too_many_arguments)]
fn merge_sort_runs_with_output(
    runs: &[spill::SpillRun],
    items: &[SortItem],
    catalog: &Catalog,
    mut tracker: OperatorMemoryTracker,
    spill_budget: &SpillBudgetTracker,
    key_memory: &KeyMemory,
    mut output: AccountedBindingBatch,
    skip_rows: usize,
    output_rows: usize,
    task_context: Option<&RuntimeTaskContext>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(task_context)?;
    let mut readers = runs
        .iter()
        .map(spill::SpillRun::reader)
        .collect::<Result<Vec<_>>>()?;
    let memory_budget = tracker.budget_bytes;
    let mut heap = BinaryHeap::new();
    for (run_index, reader) in readers.iter_mut().enumerate() {
        runtime_checkpoint(task_context)?;
        if let Some(entry) = read_sort_merge_entry(
            reader,
            run_index,
            items,
            catalog,
            memory_budget,
            memory_budget,
            spill_budget,
            &mut tracker,
            key_memory,
            task_context,
        )? {
            heap.push(entry);
        }
    }
    if output_rows == 0 {
        return Ok(BatchControl::Continue);
    }
    let mut skipped = 0usize;
    let mut emitted = 0usize;
    while let Some(entry) = heap.pop() {
        runtime_checkpoint(task_context)?;
        let run_index = entry.run_index;
        let bytes = entry.row.memory_bytes();
        if skipped < skip_rows {
            drop(entry);
            tracker.release(bytes);
            skipped = skipped.saturating_add(1);
        } else {
            let SortRunRow {
                sort_values,
                binding,
                ..
            } = entry.row;
            drop(sort_values);
            if output.transfer_from(&mut tracker, bytes, binding, emit)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            emitted = emitted.saturating_add(1);
        }
        if let Some(next) = read_sort_merge_entry(
            &mut readers[run_index],
            run_index,
            items,
            catalog,
            memory_budget,
            memory_budget,
            spill_budget,
            &mut tracker,
            key_memory,
            task_context,
        )? {
            heap.push(next);
        }
        if (output.is_full() || emitted == output_rows) && output.emit(emit)? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        if emitted == output_rows {
            return Ok(BatchControl::Stop);
        }
    }
    runtime_checkpoint(task_context)?;
    if !output.is_empty() && output.emit(emit)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(BatchControl::Continue)
}

#[allow(clippy::too_many_arguments)]
fn read_sort_merge_entry(
    reader: &mut spill::SpillReader,
    run_index: usize,
    items: &[SortItem],
    catalog: &Catalog,
    max_record_bytes: usize,
    max_item_bytes: usize,
    spill_budget: &SpillBudgetTracker,
    tracker: &mut OperatorMemoryTracker,
    key_memory: &KeyMemory,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Option<SortMergeEntry>> {
    reader
        .read_binding_record(max_record_bytes, spill_budget)?
        .map(|record| {
            record.try_map_with_key_memory(
                "SortExec merge",
                max_item_bytes,
                tracker,
                |ordinal, binding| {
                    let PreparedKey {
                        allocation,
                        values: row,
                    } = SortRunRow::new(
                        catalog,
                        items,
                        ordinal,
                        binding,
                        key_memory,
                        task_context,
                    )?;
                    Ok((SortMergeEntry { row, run_index }, Some(allocation)))
                },
                |entry| entry.row.memory_bytes(),
            )
        })
        .transpose()
}

#[cfg(test)]
mod tests;
