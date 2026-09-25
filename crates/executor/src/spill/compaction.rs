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

//! Shared run-level scheduling; row codecs and reduction remain with callers.

use super::SpillRun;
use crate::concurrent::BoundedExecutor;
use crate::pipeline::runtime_checkpoint;
use hawdb_core::{HawDBError, Result, RuntimeTaskContext};
use std::num::NonZeroUsize;

/// Keep concurrency bounded independently of spill-run count. Admission below
/// reduces this further according to the retained records and query budget.
pub(crate) fn default_compaction_worker_limit() -> NonZeroUsize {
    NonZeroUsize::new(4).unwrap()
}

pub(crate) struct CompactionMemory<'a> {
    pub blocking: &'a crate::QueryMemoryAccount,
    pub spill: &'a crate::kernel::SpillBudgetTracker,
    pub worker_limit: NonZeroUsize,
}

/// Reserve an independently sized blocking and staging allowance for every
/// merge before dispatch. Root/parent reservations survive until all child
/// accounts and leases drop; low-memory queries run smaller waves, not smaller
/// records. Completed waves are owned locally until the whole level succeeds.
pub(crate) fn compact_runs_with_memory(
    runs: Vec<SpillRun>,
    final_run_count: NonZeroUsize,
    memory: CompactionMemory<'_>,
    task_context: Option<&RuntimeTaskContext>,
    merge_pair: impl Fn(
            &SpillRun,
            &SpillRun,
            &crate::QueryMemoryAccount,
            &crate::kernel::SpillBudgetTracker,
        ) -> Result<SpillRun>
        + Sync,
) -> Result<Vec<SpillRun>> {
    compact_levels(runs, final_run_count, task_context, |pairs| {
        // Existing retained state (e.g. DISTINCT schemas) keeps its parent
        // charge. Bounds exceeding a whole serial allowance retain that
        // allowance, so conservative run maxima never tighten item admission.
        let serial_budget = memory.blocking.available_bytes().max(1);
        let mut outputs = Vec::with_capacity(pairs.len());
        let mut offset = 0;
        while offset < pairs.len() {
            runtime_checkpoint(task_context)?;
            let mut wave = Vec::new();
            for (index, (left, right)) in pairs
                .iter()
                .enumerate()
                .skip(offset)
                .take(memory.worker_limit.get())
            {
                let opaque = left.merge_record_bytes() == 0 || right.merge_record_bytes() == 0;
                let record_bytes = if opaque {
                    // Opaque external codecs do not promise a decoded-size
                    // bound. Preserve the full serial allowance for them.
                    serial_budget
                } else {
                    left.merge_record_bytes()
                        .saturating_add(right.merge_record_bytes())
                        .min(serial_budget)
                };
                let staging_bytes = if opaque {
                    memory.spill.staging_budget_bytes()
                } else {
                    left.staging_bytes()
                        .saturating_add(right.staging_bytes())
                        .min(memory.spill.staging_budget_bytes())
                        .max(1)
                };
                let admitted = (|| {
                    let blocking = memory
                        .blocking
                        .sub_account(NonZeroUsize::new(record_bytes.max(1)).unwrap())?;
                    let spill = memory
                        .spill
                        .for_merge(NonZeroUsize::new(staging_bytes).unwrap())?;
                    Ok::<_, HawDBError>((index, blocking, spill))
                })();
                match admitted {
                    Ok(merge) => wave.push(merge),
                    Err(_) => break, // Release this wave before retrying admission.
                }
            }
            if wave.is_empty() {
                // Run maxima can occur at different positions. Their sum may
                // not fit even though the serial working set does. With no
                // in-flight children, retain the original shared-account path
                // and let actual allocations enforce both parent/root limits.
                let (left, right) = &pairs[offset];
                outputs.push(merge_pair(left, right, memory.blocking, memory.spill)?);
                offset += 1;
                continue;
            }
            let executor = BoundedExecutor::new(memory.worker_limit);
            let execute = |(index, blocking, spill): &(
                usize,
                crate::QueryMemoryAccount,
                crate::kernel::SpillBudgetTracker,
            )| {
                let (left, right) = &pairs[*index];
                merge_pair(left, right, blocking, spill)
            };
            let results: Vec<Result<SpillRun>> = match task_context {
                Some(context) => executor
                    .map_ordered_with_context(&wave, context, execute)
                    .map_err(|reason| {
                        HawDBError::Execution(format!("runtime task stopped: {reason}"))
                    })?,
                None => executor.map_ordered(&wave, execute),
            };
            offset += wave.len();
            outputs.extend(results.into_iter().collect::<Result<Vec<_>>>()?);
        }
        Ok(outputs)
    })
}

fn compact_levels(
    mut runs: Vec<SpillRun>,
    final_run_count: NonZeroUsize,
    task_context: Option<&RuntimeTaskContext>,
    merge: impl Fn(&[(SpillRun, SpillRun)]) -> Result<Vec<SpillRun>>,
) -> Result<Vec<SpillRun>> {
    while runs.len() > final_run_count.get() {
        runtime_checkpoint(task_context)?;
        let mut pending = runs.into_iter();
        let mut pairs = Vec::with_capacity(pending.len() / 2);
        let mut leftover = None;
        while let Some(left) = pending.next() {
            match pending.next() {
                Some(right) => pairs.push((left, right)),
                None => leftover = Some(left),
            }
        }
        // Keep every input alive until its replacement has been flushed. On
        // error or unwind, `pairs` and `leftover` still own the not-yet- (or
        // never-to-be-) merged inputs and the merge closure's collection
        // releases any sibling outputs it already produced; no partial level
        // escapes the caller. Already dispatched siblings finish before an
        // error is reported; subsequent waves are not started after failure.
        let mut compacted = merge(&pairs)?;
        compacted.extend(leftover);
        runs = compacted;
    }
    Ok(runs)
}

#[cfg(test)]
mod tests;
