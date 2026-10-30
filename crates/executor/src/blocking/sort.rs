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

mod encoded;

fn has_expression_key(items: &[SortItem]) -> bool {
    items
        .iter()
        .any(|item| matches!(item.key, hawdb_plan_cypher::SortKey::Expression(_)))
}

impl SortKeyValues {
    fn cmp_key(&self, other: &Self) -> Ordering {
        match (self, other) {
            #[cfg(target_pointer_width = "64")]
            (Self::Integer(left, direction), Self::Integer(right, other_direction)) => {
                debug_assert_eq!(direction, other_direction);
                match direction {
                    SortDirection::Asc => left.cmp(right),
                    SortDirection::Desc => left.cmp(right).reverse(),
                }
            }
            #[cfg(target_pointer_width = "64")]
            (Self::Integer(value, direction), Self::Owned(right)) => {
                compare_sort_values(&[(Value::Int(*value), *direction)], right)
            }
            #[cfg(target_pointer_width = "64")]
            (Self::Owned(left), Self::Integer(value, direction)) => {
                compare_sort_values(left, &[(Value::Int(*value), *direction)])
            }
            (Self::Owned(left), Self::Owned(right)) => compare_sort_values(left, right),
        }
    }

    fn memory_bytes(&self) -> usize {
        let layout = std::mem::size_of::<Vec<(Value, SortDirection)>>();
        match self {
            #[cfg(target_pointer_width = "64")]
            Self::Integer(..) => {
                layout.saturating_add(std::mem::size_of::<(Value, SortDirection)>())
            }
            Self::Owned(values) => values.iter().fold(
                layout.saturating_add(std::mem::size_of_val(values.as_ref())),
                |total, (value, _)| {
                    total.saturating_add(
                        value_memory_bytes(value).saturating_sub(std::mem::size_of::<Value>()),
                    )
                },
            ),
        }
    }
}

// Keep run keys within the old Vec header without a per-row discriminant.
// An empty owned slice represents an inline integer only for a single-column
// run. The run's mode owns that distinction (and its common direction).
struct RunKeyValues {
    owned: Box<[(Value, SortDirection)]>,
    #[cfg(target_pointer_width = "64")]
    integer: i64,
}

impl From<SortKeyValues> for RunKeyValues {
    fn from(values: SortKeyValues) -> Self {
        match values {
            #[cfg(target_pointer_width = "64")]
            SortKeyValues::Integer(integer, _) => Self {
                owned: Box::default(),
                integer,
            },
            SortKeyValues::Owned(owned) => Self {
                owned,
                #[cfg(target_pointer_width = "64")]
                integer: 0,
            },
        }
    }
}

impl From<Vec<(Value, SortDirection)>> for RunKeyValues {
    fn from(values: Vec<(Value, SortDirection)>) -> Self {
        SortKeyValues::from(values).into()
    }
}

#[derive(Clone, Copy, Debug)]
enum RunKeyMode {
    Empty,
    General,
    #[cfg(target_pointer_width = "64")]
    Integers(SortDirection),
    #[cfg(target_pointer_width = "64")]
    Mixed(SortDirection),
}

impl RunKeyMode {
    fn for_row(values: &RunKeyValues, items: &[SortItem]) -> Self {
        #[cfg(target_pointer_width = "64")]
        if values.owned.is_empty()
            && let [SortItem {
                key: hawdb_plan_cypher::SortKey::Column(_),
                direction,
            }] = items
        {
            return Self::Integers(*direction);
        }
        let _ = (values, items);
        Self::General
    }

    fn include(self, next: Self) -> Self {
        match (self, next) {
            (Self::Empty, mode) | (mode, Self::Empty) => mode,
            (Self::General, Self::General) => Self::General,
            #[cfg(target_pointer_width = "64")]
            (Self::Integers(left), Self::Integers(right)) => {
                debug_assert_eq!(left, right);
                Self::Integers(left)
            }
            #[cfg(target_pointer_width = "64")]
            (Self::Integers(direction), Self::General)
            | (Self::General, Self::Integers(direction))
            | (Self::Mixed(direction), _)
            | (_, Self::Mixed(direction)) => Self::Mixed(direction),
        }
    }

    fn is_inline(self, values: &RunKeyValues) -> bool {
        match self {
            #[cfg(target_pointer_width = "64")]
            Self::Integers(_) | Self::Mixed(_) => values.owned.is_empty(),
            _ => {
                let _ = values;
                false
            }
        }
    }

    fn sort(self, rows: &mut [SortRunRow]) {
        // Ordinals order equal keys, so in-place sorting preserves output ties.
        // Select once per run. Pure fallback comparisons have exactly the
        // original slice comparator, without checking both keys' layouts.
        match self {
            Self::Empty => debug_assert!(rows.is_empty()),
            Self::General => rows.sort_unstable_by(|left, right| {
                compare_sort_values(&left.sort_values.owned, &right.sort_values.owned)
                    .then_with(|| left.ordinal.cmp(&right.ordinal))
            }),
            #[cfg(target_pointer_width = "64")]
            Self::Integers(SortDirection::Asc) => rows.sort_unstable_by(|left, right| {
                left.sort_values
                    .integer
                    .cmp(&right.sort_values.integer)
                    .then_with(|| left.ordinal.cmp(&right.ordinal))
            }),
            #[cfg(target_pointer_width = "64")]
            Self::Integers(SortDirection::Desc) => rows.sort_unstable_by(|left, right| {
                right
                    .sort_values
                    .integer
                    .cmp(&left.sort_values.integer)
                    .then_with(|| left.ordinal.cmp(&right.ordinal))
            }),
            #[cfg(target_pointer_width = "64")]
            Self::Mixed(direction) => {
                rows.sort_unstable_by(|left, right| left.cmp_mixed(right, direction))
            }
        }
    }
}

struct SortRow<K> {
    sort_values: K,
    ordinal: u64,
    binding: Binding,
}

type SortRunRow = SortRow<RunKeyValues>;
// Spill records still contain bindings and ordinals. Reconstructed merge keys
// retain the original tagged representation and comparator.
type SortMergeRow = SortRow<SortKeyValues>;

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
    key_mode: RunKeyMode,
    tracker: OperatorMemoryTracker,
    runs: Vec<spill::SpillRun>,
    input_rows: u64,
}

impl<K: From<SortKeyValues>> SortRow<K> {
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
        } = key_memory.sort_run(items, catalog, &binding, task_context)?;
        Ok(PreparedKey {
            values: Self {
                sort_values: sort_values.into(),
                ordinal,
                binding,
            },
            allocation,
        })
    }
}

impl SortMergeRow {
    fn cmp_key(&self, other: &Self) -> Ordering {
        self.sort_values
            .cmp_key(&other.sort_values)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }

    fn memory_bytes(&self) -> usize {
        binding_memory_bytes(&self.binding).saturating_add(self.sort_values.memory_bytes())
    }
}

impl SortRunRow {
    fn memory_bytes(&self, mode: RunKeyMode) -> usize {
        binding_memory_bytes(&self.binding).saturating_add(self.sort_key_memory_bytes(mode))
    }

    fn sort_key_memory_bytes(&self, mode: RunKeyMode) -> usize {
        if mode.is_inline(&self.sort_values) {
            std::mem::size_of::<Vec<(Value, SortDirection)>>()
                .saturating_add(std::mem::size_of::<(Value, SortDirection)>())
        } else {
            self.sort_values.owned.iter().fold(
                std::mem::size_of::<Vec<(Value, SortDirection)>>()
                    .saturating_add(std::mem::size_of_val(self.sort_values.owned.as_ref())),
                |total, (value, _)| {
                    total.saturating_add(
                        value_memory_bytes(value).saturating_sub(std::mem::size_of::<Value>()),
                    )
                },
            )
        }
    }

    #[cfg(target_pointer_width = "64")]
    fn cmp_mixed(&self, other: &Self, direction: SortDirection) -> Ordering {
        let ordering = match (
            self.sort_values.owned.as_ref(),
            other.sort_values.owned.as_ref(),
        ) {
            ([], []) => match direction {
                SortDirection::Asc => self.sort_values.integer.cmp(&other.sort_values.integer),
                SortDirection::Desc => other.sort_values.integer.cmp(&self.sort_values.integer),
            },
            ([], right) => {
                compare_sort_values(&[(Value::Int(self.sort_values.integer), direction)], right)
            }
            (left, []) => {
                compare_sort_values(left, &[(Value::Int(other.sort_values.integer), direction)])
            }
            (left, right) => compare_sort_values(left, right),
        };
        ordering.then_with(|| self.ordinal.cmp(&other.ordinal))
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
    row: SortMergeRow,
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
            key_mode: RunKeyMode::Empty,
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
        let row_mode = RunKeyMode::for_row(&row.sort_values, self.items);
        let bytes = row.memory_bytes(row_mode);
        ensure_operator_item_fits("SortExec", bytes, &self.tracker)?;
        if self.tracker.should_spill(bytes, self.task_context) {
            self.runs.push(spill_sort_run(
                &mut self.rows,
                self.key_mode,
                &mut self.spill_budget,
                self.task_context,
            )?);
            self.tracker.reset();
            self.key_mode = RunKeyMode::Empty;
            // A pressure spill must release the row-vector backing as well as
            // its payloads; ordinary capacity spills can reuse that allocation.
            if self.task_context.is_some_and(|task| {
                task.working_memory_target(self.tracker.budget_bytes as u64)
                    < self.tracker.budget_bytes as u64
            }) {
                self.rows = Vec::new();
            }
        }
        self.tracker.try_charge_from(&mut allocation, bytes)?;
        self.key_mode = self.key_mode.include(row_mode);
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
            self.key_mode.sort(&mut self.rows);
            return emit_sorted_output(
                self.rows.into_iter().map(|row| {
                    let bytes = row.memory_bytes(self.key_mode);
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
                self.key_mode,
                &mut self.spill_budget,
                self.task_context,
            )?);
            self.tracker.reset();
        }
        // Run generation reuses this allocation; merging no longer needs it.
        self.rows = Vec::new();
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
        // TopN already bounds its state by retained. Soft-pressure spills would
        // discard its cutoff and write otherwise rejected input repeatedly.
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
        selected.sort_unstable();
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
    key_mode: RunKeyMode,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    key_mode.sort(rows);
    let (run, mut writer) = spill_budget.create_run("sort")?;
    for row in rows.drain(..) {
        runtime_checkpoint(task_context)?;
        writer.note_merge_record_bytes(row.memory_bytes(key_mode));
        writer.note_sort_key_bytes(row.sort_key_memory_bytes(key_mode));
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
    rows.sort_unstable();
    let (run, mut writer) = spill_budget.create_run("topn")?;
    for row in rows {
        runtime_checkpoint(task_context)?;
        writer.note_merge_record_bytes(row.memory_bytes());
        writer.note_sort_key_bytes(crate::binding::sort_values_memory_bytes(&row.sort_values));
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
    if has_expression_key(items) {
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
    encoded::Merge::new(items, spill_budget, task_context).compact_levels(
        runs,
        memory.blocking_operator_bytes,
        blocking_account,
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
    if !has_expression_key(items) {
        return encoded::Merge::new(items, spill_budget, task_context).compact(
            &[left, right],
            OperatorMemoryTracker::with_blocking_account(
                memory.blocking_operator_bytes,
                blocking_account.clone(),
            ),
        );
    }
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
    if !has_expression_key(items) {
        return encoded::Merge::new(items, spill_budget, task_context).emit(
            runs,
            tracker,
            output,
            skip_rows,
            output_rows,
            emit,
        );
    }
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
            let SortRow {
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
                    } = SortMergeRow::new(
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
