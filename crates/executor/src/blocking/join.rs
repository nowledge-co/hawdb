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

//! Bounded graph equi-join over owned bindings and the shared spill pool.

use super::*;
use crate::spill::{SpillReader, SpillRun, SpillWriter, SPILL_IO_BUFFER_BYTES};
use crate::QueryMemoryLease;
use hawdb_plan::HashJoinKey;
use std::hash::BuildHasher;

const OPERATOR: &str = "HashJoinExec";

#[derive(Default, Debug)]
struct JoinWork {
    input_rows: usize,
    candidates: usize,
    replay_rows: usize,
    repartitions: usize,
    spilled_rows: usize,
}

#[derive(Clone, Copy)]
enum JoinSide {
    Build,
    Probe,
}

struct JoinRun {
    side: JoinSide,
    run: SpillRun,
    hash_or: u64,
    hash_and: u64,
    rows: usize,
    _metadata: QueryMemoryLease,
}

struct JoinWriter {
    // Release the writer's shared run handle before its metadata admission.
    writer: SpillWriter,
    run: JoinRun,
    _buffer: QueryMemoryLease,
}

impl JoinWriter {
    fn write(&mut self, hash: u64, binding: &Binding, state: &mut JoinState<'_>) -> Result<()> {
        runtime_checkpoint(state.context.task_context)?;
        self.writer.write(hash, binding, &mut state.spill)?;
        self.run.hash_or |= hash;
        self.run.hash_and &= hash;
        self.run.rows = self.run.rows.saturating_add(1);
        state.work.spilled_rows = state.work.spilled_rows.saturating_add(1);
        Ok(())
    }

    fn finish(self) -> Result<JoinRun> {
        self.writer.finish()?;
        Ok(self.run)
    }
}

struct JoinReader {
    side: JoinSide,
    reader: SpillReader,
    _buffer: QueryMemoryLease,
}

struct JoinState<'a> {
    keys: (&'a HashJoinKey, &'a HashJoinKey),
    hash_state: RandomState,
    // Bindings own their snapshot data. Spill replay never rehydrates IDs from
    // a later storage view, and keys borrow the owned properties without cloning.
    table: HashGroups<usize, Binding>,
    tracker: OperatorMemoryTracker,
    context: BlockingExecutionContext<'a>,
    account: QueryMemoryAccount,
    spill: SpillBudgetTracker,
    work: JoinWork,
}

impl<'a> JoinState<'a> {
    fn new(
        context: BlockingExecutionContext<'a>,
        keys: (&'a HashJoinKey, &'a HashJoinKey),
    ) -> Self {
        let budget = context.memory.blocking_operator_bytes.get();
        let staging = NonZeroUsize::new((budget / 4).max(1)).unwrap();
        let account = context.memory_ledger.account(
            QueryMemoryClass::BlockingState,
            OPERATOR,
            NonZeroUsize::new(budget.saturating_sub(staging.get()).max(1)).unwrap(),
        );
        Self {
            keys,
            hash_state: RandomState::new(),
            table: HashGroups::default(),
            tracker: OperatorMemoryTracker::with_account(
                NonZeroUsize::new((budget / 2).max(1)).unwrap(),
                account.clone(),
            ),
            context,
            account,
            spill: SpillBudgetTracker::with_ledger_staging_budget(
                OPERATOR,
                context.memory,
                context.memory_ledger,
                staging,
            ),
            work: JoinWork::default(),
        }
    }

    fn replay_tracker(&self) -> OperatorMemoryTracker {
        OperatorMemoryTracker::with_account(
            NonZeroUsize::new((self.context.memory.blocking_operator_bytes.get() / 4).max(1))
                .unwrap(),
            self.account.clone(),
        )
    }

    fn writer(&mut self, side: JoinSide) -> Result<JoinWriter> {
        runtime_checkpoint(self.context.task_context)?;
        let buffer = self.account.reserve(SPILL_IO_BUFFER_BYTES)?;
        let metadata = self.account.reserve(
            self.context
                .memory
                .spill_directory
                .as_os_str()
                .len()
                .saturating_mul(2)
                .saturating_add(1024),
        )?;
        let (run, writer) = self.spill.create_run("hash-join")?;
        Ok(JoinWriter {
            run: JoinRun {
                side,
                run,
                hash_or: 0,
                hash_and: u64::MAX,
                rows: 0,
                _metadata: metadata,
            },
            writer,
            _buffer: buffer,
        })
    }

    fn reader(&self, run: &JoinRun) -> Result<JoinReader> {
        runtime_checkpoint(self.context.task_context)?;
        let buffer = self.account.reserve(SPILL_IO_BUFFER_BYTES)?;
        Ok(JoinReader {
            side: run.side,
            reader: run.run.reader()?,
            _buffer: buffer,
        })
    }

    fn read(
        &mut self,
        reader: &mut JoinReader,
        tracker: &mut OperatorMemoryTracker,
    ) -> Result<Option<(u64, Binding)>> {
        runtime_checkpoint(self.context.task_context)?;
        let Some(record) = reader
            .reader
            .read_binding_record(tracker.budget_bytes, &self.spill)?
        else {
            return Ok(None);
        };
        self.work.replay_rows = self.work.replay_rows.saturating_add(1);
        record
            .try_map(
                OPERATOR,
                tracker.budget_bytes,
                tracker,
                |hash, binding| {
                    let key = match reader.side {
                        JoinSide::Build => self.keys.1,
                        JoinSide::Probe => self.keys.0,
                    };
                    if join_key(&binding, key).map(|value| self.hash_state.hash_one(value))
                        != Some(hash)
                    {
                        return Err(HawDBError::StorageIntegrity(
                            "HashJoinExec spill key does not match its recorded hash".into(),
                        ));
                    }
                    Ok((hash, binding))
                },
                |(_, binding)| binding_memory_bytes(binding),
            )
            .map(Some)
    }

    fn can_insert(&self, binding: &Binding) -> bool {
        !self
            .tracker
            .would_exceed(self.table.insertion_bytes(binding_memory_bytes(binding)))
    }

    fn insert(&mut self, hash: u64, binding: Binding) -> Result<()> {
        let bytes = binding_memory_bytes(&binding);
        self.table.insert(
            HashedKey::with_hash(self.table.len(), hash),
            binding,
            bytes,
            &mut self.tracker,
        )
    }

    fn clear(&mut self) {
        self.table = HashGroups::default();
        self.tracker.reset();
    }

    fn spill_table(&mut self, writer: &mut JoinWriter) -> Result<()> {
        // Keep array and payload admission until the consuming iterator is
        // dropped, including when a write, cancellation or unwind interrupts it.
        for (hash, binding) in std::mem::take(&mut self.table).into_values() {
            writer.write(hash, &binding, self)?;
        }
        self.tracker.reset();
        Ok(())
    }

    fn probe(
        &mut self,
        hash: u64,
        left: &Binding,
        left_key: &HashJoinKey,
        right_key: &HashJoinKey,
        output: &mut CartesianOutput,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Some(key) = join_key(left, left_key) else {
            return Ok(BatchControl::Continue);
        };
        for right in self.table.hashed_values(hash) {
            runtime_checkpoint(self.context.task_context)?;
            self.work.candidates = self.work.candidates.saturating_add(1);
            if join_key(right, right_key) == Some(key)
                && output.push(left, right, emit)? == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    }

    fn probe_run(
        &mut self,
        probe: &JoinRun,
        keys: (&HashJoinKey, &HashJoinKey),
        output: &mut CartesianOutput,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let mut reader = self.reader(probe)?;
        let mut tracker = self.replay_tracker();
        while let Some((hash, binding)) = self.read(&mut reader, &mut tracker)? {
            let control = self.probe(hash, &binding, keys.0, keys.1, output, emit)?;
            drop(binding);
            tracker.reset();
            if control == BatchControl::Stop {
                return Ok(control);
            }
        }
        Ok(BatchControl::Continue)
    }

    fn split(&mut self, run: JoinRun, bit: u32) -> Result<[JoinRun; 2]> {
        let mut reader = self.reader(&run)?;
        let mut writers = [self.writer(run.side)?, self.writer(run.side)?];
        let mut tracker = self.replay_tracker();
        while let Some((hash, binding)) = self.read(&mut reader, &mut tracker)? {
            writers[((hash >> bit) & 1) as usize].write(hash, &binding, self)?;
            drop(binding);
            tracker.reset();
        }
        let [left, right] = writers;
        Ok([left.finish()?, right.finish()?])
    }

    fn partition(
        &mut self,
        build: JoinRun,
        probe: JoinRun,
        keys: (&HashJoinKey, &HashJoinKey),
        output: &mut CartesianOutput,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if build.rows == 0 || probe.rows == 0 {
            return Ok(BatchControl::Continue);
        }
        let mut reader = self.reader(&build)?;
        let mut replay = self.replay_tracker();
        while let Some((hash, binding)) = self.read(&mut reader, &mut replay)? {
            if !self.can_insert(&binding) {
                if self.table.is_empty() {
                    return Err(HawDBError::Execution(
                        "HashJoinExec build row exceeds its admitted table budget".to_string(),
                    ));
                }
                let differing_bits = build.hash_or ^ build.hash_and;
                if differing_bits != 0 {
                    // Every chosen bit varies in this build run and is uniform
                    // in each child. At most 64 successful split levels exist.
                    drop(binding);
                    replay.reset();
                    drop(reader);
                    self.clear();
                    self.work.repartitions = self.work.repartitions.saturating_add(1);
                    let bit = differing_bits.trailing_zeros();
                    let [build_left, build_right] = self.split(build, bit)?;
                    let [probe_left, probe_right] = self.split(probe, bit)?;
                    if self.partition(build_left, probe_left, keys, output, emit)?
                        == BatchControl::Stop
                    {
                        return Ok(BatchControl::Stop);
                    }
                    return self.partition(build_right, probe_right, keys, output, emit);
                }
                // A single hash cannot be repartitioned. Replay probes once
                // per bounded build chunk; full equality still rejects collisions.
                if self.probe_run(&probe, keys, output, emit)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
                self.clear();
            }
            self.insert(hash, binding)?;
            replay.reset();
        }
        drop(reader);
        let control = self.probe_run(&probe, keys, output, emit)?;
        self.clear();
        Ok(control)
    }

    fn report(&self) {
        self.context
            .observer
            .record_blocking_memory_report(BlockingOperatorMemoryReport {
                operator: OPERATOR.to_string(),
                budget_bytes: self.context.memory.blocking_operator_bytes.get(),
                peak_tracked_bytes: self
                    .account
                    .peak_bytes()
                    .saturating_add(self.spill.staging_peak_bytes()),
                input_rows: self.work.input_rows,
                max_spill_bytes: self.spill.max_bytes,
                max_spill_runs: self.spill.max_runs,
                spilled_bytes: self.spill.used_bytes,
                spill_run_count: self.spill.run_count,
                spilled_rows: self.work.spilled_rows,
            });
    }
}

fn join_key<'a>(binding: &'a Binding, key: &HashJoinKey) -> Option<&'a Value> {
    binding_property(binding, &key.variable, &key.property).filter(|value| **value != Value::Null)
}

pub fn stream_hash_join_batches(
    plan: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    execute_hash_join(plan, source, context, execution_limit, emit).map(|(control, _)| control)
}

fn execute_hash_join(
    plan: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(BatchControl, JoinWork)> {
    let PhysicalPlan::HashJoinExec {
        left_key,
        right_key,
        left,
        right,
    } = plan
    else {
        return Err(HawDBError::Execution(
            "expected HashJoinExec plan".to_string(),
        ));
    };
    if execution_limit.is_reached(0) {
        return Ok((BatchControl::Stop, JoinWork::default()));
    }
    let mut state = JoinState::new(context, (left_key, right_key));
    let mut build_spill: Option<JoinWriter> = None;
    source.execute(right, ExecutionLimit::unlimited(), &mut |batch| {
        for binding in batch {
            runtime_checkpoint(context.task_context)?;
            state.work.input_rows = state.work.input_rows.saturating_add(1);
            let Some(key) = join_key(&binding, right_key) else {
                continue;
            };
            let hash = state.hash_state.hash_one(key);
            if build_spill.is_none() && !state.can_insert(&binding) {
                let mut writer = state.writer(JoinSide::Build)?;
                state.spill_table(&mut writer)?;
                build_spill = Some(writer);
            }
            if let Some(writer) = &mut build_spill {
                writer.write(hash, &binding, &mut state)?;
            } else {
                state.insert(hash, binding)?;
            }
        }
        Ok(BatchControl::Continue)
    })?;
    let output_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "HashJoinExec output",
        context.memory.batch_payload_bytes,
    );
    let mut output = CartesianOutput::new(
        "HashJoinExec output",
        context.memory.batch_rows.get(),
        context.memory.batch_payload_bytes,
        output_account,
        execution_limit,
    );
    let control = if let Some(build) = build_spill {
        let build = build.finish()?;
        let mut probe = state.writer(JoinSide::Probe)?;
        source.execute(left, ExecutionLimit::unlimited(), &mut |batch| {
            for binding in batch {
                runtime_checkpoint(context.task_context)?;
                state.work.input_rows = state.work.input_rows.saturating_add(1);
                if let Some(key) = join_key(&binding, left_key) {
                    probe.write(state.hash_state.hash_one(key), &binding, &mut state)?;
                }
            }
            Ok(BatchControl::Continue)
        })?;
        state.partition(
            build,
            probe.finish()?,
            (left_key, right_key),
            &mut output,
            emit,
        )?
    } else if state.table.is_empty() {
        BatchControl::Continue
    } else {
        source.execute(left, ExecutionLimit::unlimited(), &mut |batch| {
            for binding in batch {
                runtime_checkpoint(context.task_context)?;
                state.work.input_rows = state.work.input_rows.saturating_add(1);
                if let Some(key) = join_key(&binding, left_key)
                    && state.probe(
                        state.hash_state.hash_one(key),
                        &binding,
                        left_key,
                        right_key,
                        &mut output,
                        emit,
                    )? == BatchControl::Stop
                {
                    return Ok(BatchControl::Stop);
                }
            }
            Ok(BatchControl::Continue)
        })?
    };
    state.report();
    let control = if output.finish(emit)? == BatchControl::Stop {
        BatchControl::Stop
    } else {
        control
    };
    Ok((control, std::mem::take(&mut state.work)))
}

#[cfg(test)]
mod tests;
