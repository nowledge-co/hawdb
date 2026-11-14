// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::concurrent::BoundedExecutor;

struct Group {
    start: usize,
    count: usize,
    keys: QueryMemoryAccount,
    payloads: SpillBudgetTracker,
    buffers: Buffers,
    working_bytes: usize,
}

pub(super) fn compact(
    merge: &Merge<'_>,
    mut runs: Vec<spill::SpillRun>,
    budget: NonZeroUsize,
    account: &QueryMemoryAccount,
) -> Result<Vec<spill::SpillRun>> {
    let workers = spill::default_compaction_worker_limit();
    while runs.len() > 2 {
        let mut pending = runs.into_iter();
        let mut merged = Vec::new();
        while pending.len() > 1 {
            runtime_checkpoint(merge.task)?;
            let mut wave = Vec::with_capacity(workers.get());
            // Descriptor guards stay on the admitting thread. Each worker gets
            // its own captured inventory; all guards outlive the joined wave.
            let mut descriptors = Vec::with_capacity(workers.get());
            let mut offset = 0;
            let mut wave_bytes = 0usize;
            'wave: while pending.len() - offset > 1 && wave.len() < workers.get() {
                let mut admitted = None;
                for count in [8, 4, 2] {
                    if pending.len() - offset < count {
                        continue;
                    }
                    let inputs = &pending.as_slice()[offset..offset + count];
                    if inputs.iter().any(|run| run.sort_key_bytes() == 0) {
                        continue;
                    }
                    let key_bytes = inputs
                        .iter()
                        .fold(0usize, |sum, run| sum.saturating_add(run.sort_key_bytes()));
                    let payload_bytes = inputs
                        .iter()
                        .fold(0usize, |sum, run| sum.saturating_add(run.staging_bytes()));
                    let head_bytes = key_bytes.saturating_add(payload_bytes);
                    let io_limit = budget
                        .get()
                        .saturating_sub(wave_bytes)
                        .saturating_sub(head_bytes);
                    if io_limit < Workspace::minimum_bytes(count) {
                        if !wave.is_empty() {
                            break 'wave;
                        }
                        continue;
                    }
                    let admission = (|| {
                        let keys = account.sub_account(NonZeroUsize::new(key_bytes).unwrap())?;
                        let payloads = merge
                            .spill
                            .for_merge(NonZeroUsize::new(payload_bytes.max(1)).unwrap())?;
                        let workspace = Workspace::new(
                            merge.spill,
                            merge.io_account.as_ref(),
                            count,
                            0,
                            true,
                            Some(io_limit),
                        )?;
                        let (buffers, descriptors) = workspace.into_parts();
                        Ok((
                            Group {
                                start: offset,
                                count,
                                keys,
                                payloads,
                                working_bytes: head_bytes.saturating_add(buffers.bytes),
                                buffers,
                            },
                            descriptors,
                        ))
                    })();
                    match admission {
                        Ok(group) => {
                            admitted = Some(group);
                            break;
                        }
                        Err(error) if workspace::is_capacity_error(&error) => {
                            // Prefer another full fan-in wave over extra merge
                            // passes caused only by currently in-flight groups.
                            if !wave.is_empty() {
                                break 'wave;
                            }
                        }
                        Err(error) => return Err(error),
                    }
                }
                let Some((group, guard)) = admitted else {
                    break;
                };
                offset += group.count;
                wave_bytes = wave_bytes.saturating_add(group.working_bytes);
                descriptors.push(guard);
                wave.push(group);
            }
            if wave.is_empty() {
                // Maxima can occur at different positions, or a caller may not
                // provide key bounds. Retain two-way actual-allocation admission.
                let pair = &pending.as_slice()[..2];
                let output = merge.compact(
                    &[&pair[0], &pair[1]],
                    OperatorMemoryTracker::with_blocking_account(budget, account.clone()),
                )?;
                for input in pending.by_ref().take(2) {
                    drop(input);
                }
                merged.push(output);
                continue;
            }
            let execute = |group: &Group| {
                let mut tracker = OperatorMemoryTracker::with_blocking_account(
                    group.keys.budget_bytes(),
                    group.keys.clone(),
                );
                let refs: Vec<_> = pending.as_slice()[group.start..group.start + group.count]
                    .iter()
                    .collect();
                Merge::new(merge.items, &group.payloads, merge.task).compact_admitted(
                    &refs,
                    &mut tracker,
                    &group.buffers,
                )
            };
            let executor = BoundedExecutor::new(workers);
            let outputs: Vec<Result<spill::SpillRun>> = match merge.task {
                Some(task) => executor
                    .map_ordered_with_context(&wave, task, execute)
                    .map_err(|reason| {
                        HawDBError::Execution(format!("runtime task stopped: {reason}"))
                    })?,
                None => executor.map_ordered(&wave, execute),
            };
            let outputs = outputs.into_iter().collect::<Result<Vec<_>>>()?;
            // Every dispatched sibling has joined and flushed before releasing
            // its reservation or replacing an input. Errors publish no output.
            drop(wave);
            drop(descriptors);
            for input in pending.by_ref().take(offset) {
                drop(input);
            }
            merged.extend(outputs);
        }
        merged.extend(pending);
        runs = merged;
    }
    Ok(runs)
}
