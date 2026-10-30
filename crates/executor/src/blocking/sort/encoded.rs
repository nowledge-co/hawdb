// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Non-expression merges retain encoded rows and own only their ordering keys.

use super::*;

mod compaction;
mod workspace;
use workspace::{Buffers, Workspace};

struct Entry {
    record: spill::SpillBindingRecord,
    keys: Vec<(Value, SortDirection)>,
    ordinal: u64,
    run_index: usize,
    key_bytes: usize,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_sort_values(&other.keys, &self.keys)
            .then_with(|| other.ordinal.cmp(&self.ordinal))
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

pub(super) struct Merge<'a> {
    items: &'a [SortItem],
    spill: &'a SpillBudgetTracker,
    task: Option<&'a RuntimeTaskContext>,
    io_account: Option<QueryMemoryAccount>,
}

impl<'a> Merge<'a> {
    pub(super) fn new(
        items: &'a [SortItem],
        spill: &'a SpillBudgetTracker,
        task: Option<&'a RuntimeTaskContext>,
    ) -> Self {
        Self {
            items,
            spill,
            task,
            io_account: spill.merge_io_account(),
        }
    }

    fn next(
        &self,
        reader: &mut spill::SpillReader,
        run_index: usize,
        tracker: &mut OperatorMemoryTracker,
    ) -> Result<Option<Entry>> {
        runtime_checkpoint(self.task)?;
        let Some(record) =
            reader.read_binding_record(self.spill.staging_budget_bytes(), self.spill)?
        else {
            return Ok(None);
        };
        let ordinal = record.ordinal()?;
        let before = tracker.used_bytes;
        let keys = record.sort_keys(self.items, tracker, self.task)?;
        Ok(Some(Entry {
            record,
            keys,
            ordinal,
            run_index,
            key_bytes: tracker.used_bytes - before,
        }))
    }

    fn heads(
        &self,
        readers: &mut [spill::SpillReader],
        tracker: &mut OperatorMemoryTracker,
    ) -> Result<BinaryHeap<Entry>> {
        let mut heap = BinaryHeap::with_capacity(readers.len());
        for (index, reader) in readers.iter_mut().enumerate() {
            if let Some(entry) = self.next(reader, index, tracker)? {
                heap.push(entry);
            }
        }
        Ok(heap)
    }

    pub(super) fn compact(
        &self,
        runs: &[&spill::SpillRun],
        mut tracker: OperatorMemoryTracker,
    ) -> Result<spill::SpillRun> {
        let headroom = runs.iter().fold(0usize, |bytes, run| {
            bytes
                .saturating_add(run.staging_bytes())
                .saturating_add(run.sort_key_bytes())
        });
        let workspace = Workspace::new(
            self.spill,
            self.io_account.as_ref(),
            runs.len(),
            headroom,
            true,
            None,
        )?;
        self.compact_admitted(runs, &mut tracker, &workspace)
    }

    fn compact_admitted(
        &self,
        runs: &[&spill::SpillRun],
        tracker: &mut OperatorMemoryTracker,
        workspace: &Buffers,
    ) -> Result<spill::SpillRun> {
        let mut readers = runs
            .iter()
            .map(|run| run.reader_with_context(workspace.buffer, &workspace.files))
            .collect::<Result<Vec<_>>>()?;
        let mut heap = self.heads(&mut readers, tracker)?;
        let (run, mut writer) = self.spill.create_run_with_context(
            "sort-merge",
            workspace.buffer,
            Some(&workspace.files),
        )?;
        while let Some(entry) = heap.pop() {
            runtime_checkpoint(self.task)?;
            let index = entry.run_index;
            writer.note_merge_record_bytes(
                entry
                    .record
                    .decoded_binding_bytes()
                    .saturating_add(entry.key_bytes),
            );
            writer.note_sort_key_bytes(entry.key_bytes);
            writer.write_record_payload(entry.record.payload(), self.spill)?;
            let bytes = entry.key_bytes;
            drop(entry);
            tracker.release(bytes);
            if let Some(next) = self.next(&mut readers[index], index, tracker)? {
                heap.push(next);
            }
        }
        writer.finish()?;
        Ok(run)
    }

    pub(super) fn compact_levels(
        &self,
        runs: Vec<spill::SpillRun>,
        budget: NonZeroUsize,
        account: &QueryMemoryAccount,
    ) -> Result<Vec<spill::SpillRun>> {
        compaction::compact(self, runs, budget, account)
    }

    pub(super) fn emit(
        &self,
        runs: &[spill::SpillRun],
        mut tracker: OperatorMemoryTracker,
        mut output: AccountedBindingBatch,
        skip: usize,
        limit: usize,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if limit == 0 {
            return Ok(BatchControl::Continue);
        }
        let retained = runs.iter().fold(0usize, |bytes, run| {
            bytes
                .saturating_add(run.staging_bytes())
                .saturating_add(run.sort_key_bytes())
        });
        let output_headroom = runs
            .iter()
            .map(spill::SpillRun::merge_record_bytes)
            .max()
            .unwrap_or(0);
        let workspace = Workspace::new(
            self.spill,
            self.io_account.as_ref(),
            runs.len(),
            retained.saturating_add(output_headroom.saturating_mul(2)),
            false,
            None,
        )?;
        let mut readers = runs
            .iter()
            .map(|run| run.reader_with_context(workspace.buffer, &workspace.files))
            .collect::<Result<Vec<_>>>()?;
        let mut heap = self.heads(&mut readers, &mut tracker)?;
        let mut skipped = 0;
        let mut emitted = 0;
        while let Some(entry) = heap.pop() {
            runtime_checkpoint(self.task)?;
            let Entry {
                record,
                keys,
                key_bytes,
                run_index,
                ..
            } = entry;
            drop(keys);
            tracker.release(key_bytes);
            if skipped < skip {
                drop(record);
                skipped += 1;
            } else {
                // Only the winning row is materialized. All other merge heads
                // keep their encoded payloads, including rows skipped by LIMIT.
                let binding = record.try_map(
                    "SortExec output",
                    tracker.budget_bytes,
                    &mut tracker,
                    |_, binding| Ok(binding),
                    binding_memory_bytes,
                )?;
                let bytes = binding_memory_bytes(&binding);
                if output.transfer_from(&mut tracker, bytes, binding, emit)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
                emitted += 1;
                if emitted == limit {
                    // Release navigation state before admitting downstream output.
                    drop(heap);
                    drop(readers);
                    drop(workspace);
                    tracker.reset();
                    output.emit(emit)?;
                    return Ok(BatchControl::Stop);
                }
                if output.is_full() && output.emit(emit)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
            }
            if let Some(next) = self.next(&mut readers[run_index], run_index, &mut tracker)? {
                heap.push(next);
            }
        }
        runtime_checkpoint(self.task)?;
        output.emit(emit)
    }
}
