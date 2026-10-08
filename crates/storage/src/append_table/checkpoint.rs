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

//! Cooperative live-row capture for private checkpoint preparation.

use super::{compare_rows, AppendState, AppendTableError, AppendTableRow};
use crate::background::{CheckpointAllocationOwner, CheckpointWorkContext, CheckpointWorkError};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

mod memory;
use memory::CheckpointAppendRows;

const SORT_ROWS_PER_UNIT: usize = 1024;

pub(super) enum CompactionFailure {
    Work(CheckpointWorkError),
    Data(AppendTableError),
}

impl From<CheckpointWorkError> for CompactionFailure {
    fn from(error: CheckpointWorkError) -> Self {
        Self::Work(error)
    }
}

impl From<AppendTableError> for CompactionFailure {
    fn from(error: AppendTableError) -> Self {
        Self::Data(error)
    }
}

impl CompactionFailure {
    pub(super) fn into_append(self) -> AppendTableError {
        match self {
            Self::Work(error) => work_error(error),
            Self::Data(error) => error,
        }
    }
}

pub(super) fn work_error(error: CheckpointWorkError) -> AppendTableError {
    AppendTableError::Admission(error.to_string())
}

impl AppendState {
    pub(crate) fn checkpoint_rows_with_work_context(
        &self,
        max_rows: usize,
        work: &CheckpointWorkContext,
    ) -> Result<CheckpointAppendRows, AppendTableError> {
        work.checkpoint().map_err(work_error)?;
        if self.live_rows > max_rows {
            return Err(AppendTableError::Admission(format!(
                "append checkpoint contains {} live rows, exceeding limit {max_rows}",
                self.live_rows
            )));
        }
        let mut rows = CheckpointAppendRows::new(self.live_rows, work)?;
        let mut batch = self.live_head.as_deref();
        while let Some(current) = batch {
            let unit = work.start_unit().map_err(work_error)?;
            let input = current.rows.iter();
            batch = current.previous.as_deref();
            unit.finish();
            for row in input {
                rows.push_clone(row, work)?;
            }
        }
        let rows = sort_captured_rows(rows, work, true)?;
        if rows.len() != self.live_rows {
            return Err(inconsistent_batches());
        }
        for pair in rows.windows(2) {
            let unit = work.start_unit().map_err(work_error)?;
            let ordered = pair[0].table != pair[1].table
                || pair[0].partition_key != pair[1].partition_key
                || pair[0].order_key < pair[1].order_key;
            unit.finish();
            if !ordered {
                return Err(inconsistent_batches());
            }
        }
        work.checkpoint().map_err(work_error)?;
        Ok(rows)
    }
}

fn inconsistent_batches() -> AppendTableError {
    AppendTableError::Corruption(
        "append live batches contain inconsistent counts or duplicate order keys".to_string(),
    )
}

pub(super) fn sort_rows_with_work_context(
    rows: Vec<AppendTableRow>,
    work: &CheckpointWorkContext,
) -> Result<Vec<AppendTableRow>, AppendTableError> {
    // The existing compaction API still returns an unleased Vec. Live capture
    // retains its typed owner directly; compaction ownership remains separate.
    Ok(sort_captured_rows(CheckpointAppendRows::unadmitted(rows), work, false)?.rows)
}

fn scratch_capacity<T>(
    capacity: usize,
    allocations: &mut CheckpointAllocationOwner,
    work: &CheckpointWorkContext,
    admitted: bool,
) -> Result<Vec<T>, AppendTableError> {
    if admitted {
        allocations
            .reserve(memory::capacity_bytes::<T>(capacity)?, work)
            .map_err(work_error)?;
    }
    work.checkpoint().map_err(work_error)?;
    memory::allocate_capacity(capacity, work)
}

fn sort_captured_rows(
    mut rows: CheckpointAppendRows,
    work: &CheckpointWorkContext,
    admitted: bool,
) -> Result<CheckpointAppendRows, AppendTableError> {
    let row_count = rows.len();
    let run_count = row_count.div_ceil(SORT_ROWS_PER_UNIT);
    // Leases outlive all scratch buffers, also on a cancellation/error unwind.
    let mut scratch = CheckpointAllocationOwner::default();
    let mut input = std::mem::take(&mut rows.rows).into_iter();
    let mut runs = Vec::new();
    while !input.as_slice().is_empty() {
        let unit = work.start_unit().map_err(work_error)?;
        if runs.is_empty() {
            runs = scratch_capacity(run_count, &mut scratch, work, admitted)?;
        }
        let capacity = input.as_slice().len().min(SORT_ROWS_PER_UNIT);
        let mut run = scratch_capacity(capacity, &mut scratch, work, admitted)?;
        run.extend(input.by_ref().take(capacity));
        run.sort_unstable_by(compare_rows);
        runs.push(run.into_iter());
        unit.finish();
    }
    let mut heap = BinaryHeap::new();
    for (run, rows) in runs.iter_mut().enumerate() {
        let unit = work.start_unit().map_err(work_error)?;
        if run == 0 {
            heap = BinaryHeap::from(scratch_capacity(run_count, &mut scratch, work, admitted)?);
        }
        if let Some(row) = rows.next() {
            heap.push(Reverse(MergeHead { row, run }));
        }
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
    let memory = if admitted {
        memory::reserve_capacity::<AppendTableRow>(row_count, work)?
    } else {
        None
    };
    let mut sorted = memory::allocate_capacity(row_count, work)?;
    unit.finish();
    while !heap.is_empty() {
        let unit = work.start_unit().map_err(work_error)?;
        let Reverse(head) = heap.pop().expect("heap was checked nonempty");
        sorted.push(head.row);
        if let Some(row) = runs[head.run].next() {
            heap.push(Reverse(MergeHead { row, run: head.run }));
        }
        unit.finish();
    }
    drop(heap);
    drop(runs);
    drop(input);
    drop(scratch);
    rows.rows = sorted;
    rows.memory = memory;
    work.checkpoint().map_err(work_error)?;
    Ok(rows)
}

struct MergeHead {
    row: AppendTableRow,
    run: usize,
}

impl Ord for MergeHead {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_rows(&self.row, &other.row).then_with(|| self.run.cmp(&other.run))
    }
}

impl PartialOrd for MergeHead {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MergeHead {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for MergeHead {}

#[cfg(test)]
mod tests;
