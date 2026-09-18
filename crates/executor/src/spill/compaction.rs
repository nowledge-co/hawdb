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
use crate::pipeline::runtime_checkpoint;
use hawdb_core::{Result, RuntimeTaskContext};
use std::num::NonZeroUsize;

pub(crate) fn compact_runs(
    mut runs: Vec<SpillRun>,
    final_run_count: NonZeroUsize,
    task_context: Option<&RuntimeTaskContext>,
    mut merge_pair: impl FnMut(&SpillRun, &SpillRun) -> Result<SpillRun>,
) -> Result<Vec<SpillRun>> {
    while runs.len() > final_run_count.get() {
        runtime_checkpoint(task_context)?;
        let mut compacted = Vec::with_capacity(runs.len().div_ceil(2));
        let mut pending = runs.into_iter();
        while let Some(left) = pending.next() {
            let Some(right) = pending.next() else {
                compacted.push(left);
                break;
            };
            // Keep both inputs alive until the replacement has been flushed.
            // On error or unwind, this scope also releases earlier outputs and
            // the still-pending inputs; no partial level escapes the caller.
            compacted.push(merge_pair(&left, &right)?);
        }
        runs = compacted;
    }
    Ok(runs)
}

#[cfg(test)]
mod tests;
