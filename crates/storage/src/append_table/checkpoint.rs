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
use crate::background::{CheckpointWorkContext, CheckpointWorkError};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};

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
    ) -> Result<Vec<AppendTableRow>, AppendTableError> {
        work.checkpoint().map_err(work_error)?;
        if self.live_rows > max_rows {
            return Err(AppendTableError::Admission(format!(
                "append checkpoint contains {} live rows, exceeding limit {max_rows}",
                self.live_rows
            )));
        }
        let unit = work.start_unit().map_err(work_error)?;
        let mut rows = Vec::with_capacity(self.live_rows);
        let mut batch = self.live_head.as_deref();
        unit.finish();
        while let Some(current) = batch {
            let unit = work.start_unit().map_err(work_error)?;
            let input = current.rows.iter();
            batch = current.previous.as_deref();
            unit.finish();
            for row in input {
                let unit = work.start_unit().map_err(work_error)?;
                rows.push(row.clone());
                unit.finish();
            }
        }
        let rows = sort_rows_with_work_context(rows, work)?;
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
    let row_count = rows.len();
    let mut input = rows.into_iter();
    let mut runs = Vec::new();
    while !input.as_slice().is_empty() {
        let unit = work.start_unit().map_err(work_error)?;
        let mut run = input.by_ref().take(SORT_ROWS_PER_UNIT).collect::<Vec<_>>();
        run.sort_unstable_by(compare_rows);
        runs.push(VecDeque::from(run));
        unit.finish();
    }
    let mut heap = BinaryHeap::new();
    for (run, rows) in runs.iter_mut().enumerate() {
        let unit = work.start_unit().map_err(work_error)?;
        if let Some(row) = rows.pop_front() {
            heap.push(Reverse(MergeHead { row, run }));
        }
        unit.finish();
    }
    let unit = work.start_unit().map_err(work_error)?;
    let mut sorted = Vec::with_capacity(row_count);
    unit.finish();
    while !heap.is_empty() {
        let unit = work.start_unit().map_err(work_error)?;
        let Reverse(head) = heap.pop().expect("heap was checked nonempty");
        sorted.push(head.row);
        if let Some(row) = runs[head.run].pop_front() {
            heap.push(Reverse(MergeHead { row, run: head.run }));
        }
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(sorted)
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
