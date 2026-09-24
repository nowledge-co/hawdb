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

/// Pairs within one compaction level are independent and `merge_level` below
/// is able to run them concurrently on the shared executor pool, but every
/// `merge_*_run_pair` caller sizes its `OperatorMemoryTracker` from
/// `ExecutionMemoryConfig::blocking_operator_bytes` assuming exactly one
/// merge is charging the operator's shared `QueryMemoryAccount` at a time.
/// Concurrent merges cloning that same account each stay within their own
/// tracker's view of the budget while collectively exceeding the account's
/// real ceiling — dividing a tracker's *local* budget by the worker count
/// does not fix this correctly (it under-admits single legitimately-sized
/// items instead), and the real fix (giving concurrent merges their own
/// correctly-sized share of the account, not just a smaller number to
/// compare against) is not done. Default to a single worker until that
/// lands, so every production call site stays exactly as correct as the
/// pre-parallel implementation; tests pass a larger explicit limit to
/// exercise and verify the concurrent path pairs are still dispatched over.
pub(crate) fn default_compaction_worker_limit() -> NonZeroUsize {
    NonZeroUsize::MIN
}

pub(crate) fn compact_runs(
    mut runs: Vec<SpillRun>,
    final_run_count: NonZeroUsize,
    worker_limit: NonZeroUsize,
    task_context: Option<&RuntimeTaskContext>,
    merge_pair: impl Fn(&SpillRun, &SpillRun) -> Result<SpillRun> + Sync,
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
        // never-to-be-) merged inputs and `merge_level`'s own collection
        // releases any sibling outputs it already produced; no partial level
        // escapes the caller. Note that every pair in the level is still
        // attempted even after one fails (see `merge_level`) — this is a
        // deliberate tradeoff of running pairs concurrently, not a bug: the
        // first-encountered failure is still what's reported, and nothing
        // that failed partway leaks, but a level does not stop early the way
        // the old strictly-serial loop did.
        let mut compacted = merge_level(&pairs, worker_limit, task_context, &merge_pair)?;
        compacted.extend(leftover);
        runs = compacted;
    }
    Ok(runs)
}

/// Runs every pair in one compaction level concurrently on the shared pool
/// (falling back to genuinely serial execution only when the pool itself is
/// unavailable, e.g. on `wasm32-unknown-unknown`), preserving input order in
/// the result regardless of completion order. A `merge_pair` failure does
/// not stop sibling pairs already dispatched in the same level — an output
/// that is itself an `Err` does not short-circuit the other workers, by
/// `BoundedExecutor`'s own contract — so every pair in the level is still
/// attempted; `outputs.into_iter().collect()` below is what turns the first
/// failure into this call's `Err` and releases every successfully produced
/// sibling output. This holds even at `worker_limit = NonZeroUsize::MIN`:
/// one worker still drains the whole level's queue before this function's
/// caller can observe an error, it just does so on a single thread.
fn merge_level(
    pairs: &[(SpillRun, SpillRun)],
    worker_limit: NonZeroUsize,
    task_context: Option<&RuntimeTaskContext>,
    merge_pair: &(impl Fn(&SpillRun, &SpillRun) -> Result<SpillRun> + Sync),
) -> Result<Vec<SpillRun>> {
    let executor = BoundedExecutor::new(worker_limit);
    let outputs: Vec<Result<SpillRun>> = match task_context {
        Some(context) => executor
            .map_ordered_with_context(pairs, context, |(left, right)| merge_pair(left, right))
            .map_err(|reason| HawDBError::Execution(format!("runtime task stopped: {reason}")))?,
        None => executor.map_ordered(pairs, |(left, right)| merge_pair(left, right)),
    };
    outputs.into_iter().collect()
}

#[cfg(test)]
mod tests;
