//! Shared run-level scheduling; row codecs and reduction remain with callers.

use super::SpillRun;
use crate::pipeline::runtime_checkpoint;
use skein_core::{Result, RuntimeTaskContext};
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
