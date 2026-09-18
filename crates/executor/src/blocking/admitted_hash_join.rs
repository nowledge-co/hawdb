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

//! Shared admitted hash-join build, spill, repartition, and replay lifecycle.
//!
//! This module deliberately carries opaque executor bindings. Callers retain
//! their key semantics, row hydration, residual predicates, and output shape in
//! an adapter, while both graph and relational joins use one bounded lifecycle.

use super::{HashGroups, HashedKey};
use crate::binding::{binding_memory_bytes, Binding};
use crate::kernel::{OperatorMemoryTracker, SpillBudgetTracker};
use crate::pipeline::runtime_checkpoint;
use crate::spill::{SpillReader, SpillRun, SpillWriter, SPILL_IO_BUFFER_BYTES};
use crate::{
    BlockingOperatorMemoryReport, ExecutionMemoryConfig, QueryMemoryAccount, QueryMemoryClass,
    QueryMemoryLease, QueryMemoryLedger,
};
use hawdb_core::{HawDBError, Result, RuntimeTaskContext};
use std::num::NonZeroUsize;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AdmittedHashJoinWork {
    pub input_rows: usize,
    pub candidate_rows: usize,
    pub replay_rows: usize,
    pub repartitions: usize,
    pub spilled_rows: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmittedHashJoinControl {
    Continue,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmittedHashJoinSide {
    Build,
    Probe,
}

pub struct AdmittedHashJoinRecord {
    pub hash: u64,
    pub binding: Binding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmittedHashJoinCandidate {
    pub matched: bool,
    pub control: AdmittedHashJoinControl,
}

impl AdmittedHashJoinCandidate {
    pub const fn rejected() -> Self {
        Self {
            matched: false,
            control: AdmittedHashJoinControl::Continue,
        }
    }

    pub const fn matched(control: AdmittedHashJoinControl) -> Self {
        Self {
            matched: true,
            control,
        }
    }
}

/// Domain-specific semantics kept outside the executor lifecycle.
pub trait AdmittedHashJoinAdapter {
    /// Validate that a replay record still belongs to its recorded hash.
    fn validate_spill_record(
        &mut self,
        side: AdmittedHashJoinSide,
        hash: u64,
        binding: &Binding,
    ) -> Result<()>;

    /// Evaluate one equal-hash candidate and report whether it was accepted.
    fn visit_candidate(
        &mut self,
        probe: &Binding,
        build: &Binding,
    ) -> Result<AdmittedHashJoinCandidate>;

    /// Emit an unmatched probe after all build chunks rejected it.
    fn visit_unmatched(&mut self, _probe: &Binding) -> Result<AdmittedHashJoinControl> {
        Ok(AdmittedHashJoinControl::Continue)
    }

    /// LEFT joins need a complete candidate result before emitting an unmatched
    /// row. The bounded hot-key fallback scans build chunks per probe only for
    /// this case, avoiding per-probe match state in the shared table.
    fn requires_complete_probe_match(&self) -> bool {
        false
    }
}

struct JoinRun {
    side: AdmittedHashJoinSide,
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

struct JoinReader {
    side: AdmittedHashJoinSide,
    reader: SpillReader,
    _buffer: QueryMemoryLease,
}

struct AdmittedHashJoinMemoryLayout {
    staging_budget: NonZeroUsize,
    state_budget: NonZeroUsize,
    table_budget: NonZeroUsize,
    spill_io_buffer_bytes: NonZeroUsize,
}

impl AdmittedHashJoinMemoryLayout {
    fn new(blocking_operator_bytes: NonZeroUsize) -> Self {
        let total = blocking_operator_bytes.get();
        let staging_budget =
            NonZeroUsize::new((total / 4).max(1)).expect("staging budget is non-zero");
        let state_budget = NonZeroUsize::new(total.saturating_sub(staging_budget.get()).max(1))
            .expect("state budget is non-zero");
        // Repartitioning can keep table state and three buffered handles live
        // while replay records are decoded. Preserve the table share that
        // bounds spill-run fanout, and leave three quarters of the state
        // account for table state, metadata, and replay by scaling each buffer.
        let table_budget = NonZeroUsize::new((total / 2).max(1)).expect("table budget is non-zero");
        let spill_io_buffer_bytes = NonZeroUsize::new(
            state_budget
                .get()
                .saturating_div(12)
                .clamp(1, SPILL_IO_BUFFER_BYTES),
        )
        .expect("spill buffer is non-zero");
        Self {
            staging_budget,
            state_budget,
            table_budget,
            spill_io_buffer_bytes,
        }
    }
}

pub struct AdmittedHashJoin<'a> {
    operator: &'static str,
    memory: &'a ExecutionMemoryConfig,
    task_context: Option<&'a RuntimeTaskContext>,
    table: HashGroups<usize, Binding>,
    tracker: OperatorMemoryTracker,
    account: QueryMemoryAccount,
    spill_io_buffer_bytes: NonZeroUsize,
    spill: SpillBudgetTracker,
    build_writer: Option<JoinWriter>,
    build_run: Option<JoinRun>,
    probe_writer: Option<JoinWriter>,
    build_finished: bool,
    work: AdmittedHashJoinWork,
}

impl<'a> AdmittedHashJoin<'a> {
    pub fn new(
        operator: &'static str,
        memory: &'a ExecutionMemoryConfig,
        memory_ledger: &'a QueryMemoryLedger,
        task_context: Option<&'a RuntimeTaskContext>,
    ) -> Self {
        let layout = AdmittedHashJoinMemoryLayout::new(memory.blocking_operator_bytes);
        let account = memory_ledger.account(
            QueryMemoryClass::BlockingState,
            operator,
            layout.state_budget,
        );
        Self {
            operator,
            memory,
            task_context,
            table: HashGroups::default(),
            tracker: OperatorMemoryTracker::with_account(layout.table_budget, account.clone()),
            account,
            spill_io_buffer_bytes: layout.spill_io_buffer_bytes,
            spill: SpillBudgetTracker::with_ledger_staging_budget(
                operator,
                memory,
                memory_ledger,
                layout.staging_budget,
            ),
            build_writer: None,
            build_run: None,
            probe_writer: None,
            build_finished: false,
            work: AdmittedHashJoinWork::default(),
        }
    }

    pub fn push_build(&mut self, record: AdmittedHashJoinRecord) -> Result<()> {
        if self.build_finished {
            return Err(HawDBError::Execution(
                "cannot add a hash-join build record after finishing the build".to_string(),
            ));
        }
        runtime_checkpoint(self.task_context)?;
        self.work.input_rows = self.work.input_rows.saturating_add(1);
        if self.build_writer.is_none() && !self.can_insert(&record.binding) {
            let mut writer = self.writer(AdmittedHashJoinSide::Build)?;
            self.spill_table(&mut writer)?;
            self.build_writer = Some(writer);
        }
        if let Some(mut writer) = self.build_writer.take() {
            let result = writer.write(record.hash, &record.binding, self);
            self.build_writer = Some(writer);
            result?;
        } else {
            self.insert(record.hash, record.binding)?;
        }
        Ok(())
    }

    pub fn finish_build(&mut self) -> Result<()> {
        if self.build_finished {
            return Ok(());
        }
        self.build_run = self
            .build_writer
            .take()
            .map(JoinWriter::finish)
            .transpose()?;
        self.build_finished = true;
        Ok(())
    }

    pub fn push_probe<A: AdmittedHashJoinAdapter>(
        &mut self,
        record: AdmittedHashJoinRecord,
        adapter: &mut A,
    ) -> Result<AdmittedHashJoinControl> {
        if !self.build_finished {
            return Err(HawDBError::Execution(
                "cannot add a hash-join probe record before finishing the build".to_string(),
            ));
        }
        runtime_checkpoint(self.task_context)?;
        self.work.input_rows = self.work.input_rows.saturating_add(1);
        if self.build_run.is_some() {
            if self.probe_writer.is_none() {
                self.probe_writer = Some(self.writer(AdmittedHashJoinSide::Probe)?);
            }
            let mut writer = self
                .probe_writer
                .take()
                .expect("probe writer is initialized before use");
            let result = writer.write(record.hash, &record.binding, self);
            self.probe_writer = Some(writer);
            result?;
            return Ok(AdmittedHashJoinControl::Continue);
        }
        self.probe_table(record.hash, &record.binding, adapter)
    }

    pub fn finish<A: AdmittedHashJoinAdapter>(
        &mut self,
        adapter: &mut A,
    ) -> Result<AdmittedHashJoinControl> {
        if !self.build_finished {
            return Err(HawDBError::Execution(
                "cannot finish a hash join before its build input".to_string(),
            ));
        }
        let Some(build) = self.build_run.take() else {
            return Ok(AdmittedHashJoinControl::Continue);
        };
        let Some(probe) = self
            .probe_writer
            .take()
            .map(JoinWriter::finish)
            .transpose()?
        else {
            return Ok(AdmittedHashJoinControl::Continue);
        };
        self.partition(build, probe, adapter)
    }

    pub fn work(&self) -> AdmittedHashJoinWork {
        self.work
    }

    pub fn report(&self) -> BlockingOperatorMemoryReport {
        BlockingOperatorMemoryReport {
            operator: self.operator.to_string(),
            budget_bytes: self.memory.blocking_operator_bytes.get(),
            peak_tracked_bytes: self
                .account
                .peak_bytes()
                .saturating_add(self.spill.staging_peak_bytes()),
            input_rows: self.work.input_rows,
            candidate_rows: self.work.candidate_rows,
            replay_rows: self.work.replay_rows,
            repartitions: self.work.repartitions,
            max_spill_bytes: self.spill.max_bytes,
            max_spill_runs: self.spill.max_runs,
            spilled_bytes: self.spill.used_bytes,
            spill_run_count: self.spill.run_count,
            spilled_rows: self.work.spilled_rows,
        }
    }

    fn replay_tracker(&self) -> OperatorMemoryTracker {
        OperatorMemoryTracker::with_account(
            NonZeroUsize::new((self.memory.blocking_operator_bytes.get() / 4).max(1))
                .expect("replay budget is non-zero"),
            self.account.clone(),
        )
    }

    fn writer(&mut self, side: AdmittedHashJoinSide) -> Result<JoinWriter> {
        runtime_checkpoint(self.task_context)?;
        let buffer = self
            .account
            .reserve(self.spill_io_buffer_bytes.get())
            .map_err(|error| {
                HawDBError::Execution(format!(
                    "{} cannot admit a {}-byte spill writer buffer: {error}",
                    self.operator,
                    self.spill_io_buffer_bytes.get(),
                ))
            })?;
        let metadata = self.account.reserve(
            self.memory
                .spill_directory
                .as_os_str()
                .len()
                .saturating_mul(2)
                .saturating_add(1024),
        )?;
        let (run, writer) = self
            .spill
            .create_run_with_buffer_bytes("hash-join", self.spill_io_buffer_bytes)?;
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
        runtime_checkpoint(self.task_context)?;
        let buffer = self
            .account
            .reserve(self.spill_io_buffer_bytes.get())
            .map_err(|error| {
                HawDBError::Execution(format!(
                    "{} cannot admit a {}-byte spill reader buffer: {error}",
                    self.operator,
                    self.spill_io_buffer_bytes.get(),
                ))
            })?;
        Ok(JoinReader {
            side: run.side,
            reader: run
                .run
                .reader_with_buffer_bytes(self.spill_io_buffer_bytes)?,
            _buffer: buffer,
        })
    }

    fn read<A: AdmittedHashJoinAdapter>(
        &mut self,
        reader: &mut JoinReader,
        tracker: &mut OperatorMemoryTracker,
        adapter: &mut A,
    ) -> Result<Option<AdmittedHashJoinRecord>> {
        runtime_checkpoint(self.task_context)?;
        let Some(record) = reader
            .reader
            .read_binding_record(tracker.budget_bytes, &self.spill)?
        else {
            return Ok(None);
        };
        self.work.replay_rows = self.work.replay_rows.saturating_add(1);
        record
            .try_map(
                self.operator,
                tracker.budget_bytes,
                tracker,
                |hash, binding| {
                    adapter.validate_spill_record(reader.side, hash, &binding)?;
                    Ok(AdmittedHashJoinRecord { hash, binding })
                },
                |record| binding_memory_bytes(&record.binding),
            )
            .map(Some)
    }

    fn can_insert(&self, binding: &Binding) -> bool {
        let insertion_bytes = self.table.insertion_bytes(binding_memory_bytes(binding));
        !self.tracker.would_exceed(insertion_bytes) && self.account.can_reserve(insertion_bytes)
    }

    fn insert(&mut self, hash: u64, binding: Binding) -> Result<()> {
        let bytes = binding_memory_bytes(&binding);
        self.table
            .insert(
                HashedKey::with_hash(self.table.len(), hash),
                binding,
                bytes,
                &mut self.tracker,
            )
            .map_err(|error| {
                HawDBError::Execution(format!(
                    "{} cannot admit a hash table entry: {error}",
                    self.operator,
                ))
            })
    }

    fn clear(&mut self) {
        self.table = HashGroups::default();
        self.tracker.reset();
    }

    fn spill_table(&mut self, writer: &mut JoinWriter) -> Result<()> {
        // Keep array and payload admission until the consuming iterator is
        // dropped, including when a write, cancellation, or unwind interrupts it.
        for (hash, binding) in std::mem::take(&mut self.table).into_values() {
            writer.write(hash, &binding, self)?;
        }
        self.tracker.reset();
        Ok(())
    }

    fn match_table<A: AdmittedHashJoinAdapter>(
        &mut self,
        hash: u64,
        probe: &Binding,
        adapter: &mut A,
    ) -> Result<(AdmittedHashJoinControl, bool)> {
        let mut matched = false;
        for build in self.table.hashed_values(hash) {
            runtime_checkpoint(self.task_context)?;
            self.work.candidate_rows = self.work.candidate_rows.saturating_add(1);
            let candidate = adapter.visit_candidate(probe, build)?;
            matched |= candidate.matched;
            if candidate.control == AdmittedHashJoinControl::Stop {
                return Ok((candidate.control, matched));
            }
        }
        Ok((AdmittedHashJoinControl::Continue, matched))
    }

    fn probe_table<A: AdmittedHashJoinAdapter>(
        &mut self,
        hash: u64,
        probe: &Binding,
        adapter: &mut A,
    ) -> Result<AdmittedHashJoinControl> {
        let (control, matched) = self.match_table(hash, probe, adapter)?;
        if control == AdmittedHashJoinControl::Stop || matched {
            return Ok(control);
        }
        adapter.visit_unmatched(probe)
    }

    fn probe_run<A: AdmittedHashJoinAdapter>(
        &mut self,
        probe: &JoinRun,
        adapter: &mut A,
    ) -> Result<AdmittedHashJoinControl> {
        let mut reader = self.reader(probe)?;
        let mut tracker = self.replay_tracker();
        while let Some(record) = self.read(&mut reader, &mut tracker, adapter)? {
            let control = self.probe_table(record.hash, &record.binding, adapter)?;
            drop(record);
            tracker.reset();
            if control == AdmittedHashJoinControl::Stop {
                return Ok(control);
            }
        }
        Ok(AdmittedHashJoinControl::Continue)
    }

    fn split<A: AdmittedHashJoinAdapter>(
        &mut self,
        run: JoinRun,
        bit: u32,
        adapter: &mut A,
    ) -> Result<[JoinRun; 2]> {
        let mut reader = self.reader(&run)?;
        let mut writers = [self.writer(run.side)?, self.writer(run.side)?];
        let mut tracker = self.replay_tracker();
        while let Some(record) = self.read(&mut reader, &mut tracker, adapter)? {
            writers[((record.hash >> bit) & 1) as usize].write(
                record.hash,
                &record.binding,
                self,
            )?;
            drop(record);
            tracker.reset();
        }
        let [left, right] = writers;
        Ok([left.finish()?, right.finish()?])
    }

    fn probe_hot<A: AdmittedHashJoinAdapter>(
        &mut self,
        build: &JoinRun,
        probe: &JoinRun,
        adapter: &mut A,
    ) -> Result<AdmittedHashJoinControl> {
        let mut probe_reader = self.reader(probe)?;
        let mut probe_tracker = self.replay_tracker();
        while let Some(probe_record) = self.read(&mut probe_reader, &mut probe_tracker, adapter)? {
            let mut matched = false;
            let mut build_reader = self.reader(build)?;
            let mut build_tracker = self.replay_tracker();
            while let Some(build_record) =
                self.read(&mut build_reader, &mut build_tracker, adapter)?
            {
                if !self.can_insert(&build_record.binding) {
                    if self.table.is_empty() {
                        return Err(HawDBError::Execution(format!(
                            "{} build row exceeds its admitted table budget",
                            self.operator
                        )));
                    }
                    let (control, accepted) =
                        self.match_table(probe_record.hash, &probe_record.binding, adapter)?;
                    matched |= accepted;
                    self.clear();
                    if control == AdmittedHashJoinControl::Stop {
                        return Ok(control);
                    }
                }
                self.insert(build_record.hash, build_record.binding)?;
                build_tracker.reset();
            }
            if !self.table.is_empty() {
                let (control, accepted) =
                    self.match_table(probe_record.hash, &probe_record.binding, adapter)?;
                matched |= accepted;
                self.clear();
                if control == AdmittedHashJoinControl::Stop {
                    return Ok(control);
                }
            }
            if !matched {
                let control = adapter.visit_unmatched(&probe_record.binding)?;
                if control == AdmittedHashJoinControl::Stop {
                    return Ok(control);
                }
            }
            drop(probe_record);
            probe_tracker.reset();
        }
        Ok(AdmittedHashJoinControl::Continue)
    }

    fn partition<A: AdmittedHashJoinAdapter>(
        &mut self,
        build: JoinRun,
        probe: JoinRun,
        adapter: &mut A,
    ) -> Result<AdmittedHashJoinControl> {
        if probe.rows == 0 {
            return Ok(AdmittedHashJoinControl::Continue);
        }
        if build.rows == 0 {
            return self.probe_run(&probe, adapter);
        }
        let mut reader = self.reader(&build)?;
        let mut replay = self.replay_tracker();
        while let Some(record) = self.read(&mut reader, &mut replay, adapter)? {
            if !self.can_insert(&record.binding) {
                if self.table.is_empty() {
                    return Err(HawDBError::Execution(format!(
                        "{} build row exceeds its admitted table budget",
                        self.operator
                    )));
                }
                let differing_bits = build.hash_or ^ build.hash_and;
                if differing_bits != 0 {
                    // Every chosen bit varies in this build run and is uniform
                    // in each child. At most 64 successful split levels exist.
                    drop(record);
                    replay.reset();
                    drop(reader);
                    self.clear();
                    self.work.repartitions = self.work.repartitions.saturating_add(1);
                    let bit = differing_bits.trailing_zeros();
                    let [build_left, build_right] = self.split(build, bit, adapter)?;
                    let [probe_left, probe_right] = self.split(probe, bit, adapter)?;
                    if self.partition(build_left, probe_left, adapter)?
                        == AdmittedHashJoinControl::Stop
                    {
                        return Ok(AdmittedHashJoinControl::Stop);
                    }
                    return self.partition(build_right, probe_right, adapter);
                }
                if adapter.requires_complete_probe_match() {
                    drop(record);
                    replay.reset();
                    drop(reader);
                    self.clear();
                    return self.probe_hot(&build, &probe, adapter);
                }
                if self.probe_run(&probe, adapter)? == AdmittedHashJoinControl::Stop {
                    return Ok(AdmittedHashJoinControl::Stop);
                }
                self.clear();
            }
            self.insert(record.hash, record.binding)?;
            replay.reset();
        }
        drop(reader);
        let control = self.probe_run(&probe, adapter)?;
        self.clear();
        Ok(control)
    }
}

impl JoinWriter {
    fn write(
        &mut self,
        hash: u64,
        binding: &Binding,
        state: &mut AdmittedHashJoin<'_>,
    ) -> Result<()> {
        runtime_checkpoint(state.task_context)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    struct EqualityAdapter {
        matches: Vec<i64>,
    }

    impl EqualityAdapter {
        fn value(binding: &Binding) -> Result<i64> {
            match binding.values.get("key") {
                Some(hawdb_core::Value::Int(value)) => Ok(*value),
                _ => Err(HawDBError::StorageIntegrity(
                    "test hash-join record has no integer key".to_string(),
                )),
            }
        }

        fn binding(value: i64, padding: usize) -> Binding {
            Binding {
                values: BTreeMap::from([
                    ("key".to_string(), hawdb_core::Value::Int(value)),
                    (
                        "padding".to_string(),
                        hawdb_core::Value::String("x".repeat(padding)),
                    ),
                ]),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            }
        }
    }

    impl AdmittedHashJoinAdapter for EqualityAdapter {
        fn validate_spill_record(
            &mut self,
            _side: AdmittedHashJoinSide,
            hash: u64,
            binding: &Binding,
        ) -> Result<()> {
            if hash != Self::value(binding)? as u64 {
                return Err(HawDBError::StorageIntegrity(
                    "test hash-join record has an inconsistent hash".to_string(),
                ));
            }
            Ok(())
        }

        fn visit_candidate(
            &mut self,
            probe: &Binding,
            build: &Binding,
        ) -> Result<AdmittedHashJoinCandidate> {
            let probe = Self::value(probe)?;
            if probe != Self::value(build)? {
                return Ok(AdmittedHashJoinCandidate::rejected());
            }
            self.matches.push(probe);
            Ok(AdmittedHashJoinCandidate::matched(
                AdmittedHashJoinControl::Continue,
            ))
        }
    }

    fn spill_memory(name: &str) -> ExecutionMemoryConfig {
        spill_memory_with_blocking_budget(name, 128 * 1024)
    }

    fn spill_memory_with_blocking_budget(
        name: &str,
        blocking_operator_bytes: usize,
    ) -> ExecutionMemoryConfig {
        ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(blocking_operator_bytes)
                .expect("non-zero budget"),
            max_spill_bytes: std::num::NonZeroU64::new(2 * 1024 * 1024)
                .expect("non-zero spill budget"),
            max_spill_runs: NonZeroUsize::new(16).expect("non-zero spill run budget"),
            min_spill_free_bytes: std::num::NonZeroU64::MIN,
            spill_directory: std::env::temp_dir().join(format!(
                "hawdb-admitted-hash-join-{name}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock")
                    .as_nanos()
            )),
            ..ExecutionMemoryConfig::default()
        }
    }

    #[test]
    fn memory_layout_scales_spill_buffers_inside_the_state_budget() {
        let layout = AdmittedHashJoinMemoryLayout::new(
            NonZeroUsize::new(40 * 1024).expect("non-zero blocking budget"),
        );

        assert_eq!(layout.staging_budget.get(), 10 * 1024);
        assert_eq!(layout.state_budget.get(), 30 * 1024);
        assert_eq!(layout.table_budget.get(), 20 * 1024);
        assert_eq!(layout.spill_io_buffer_bytes.get(), 2_560);
        assert!(layout.spill_io_buffer_bytes.get().saturating_mul(3) < layout.state_budget.get());
    }

    #[test]
    fn equal_hash_candidates_still_require_adapter_equality() {
        let memory = ExecutionMemoryConfig::default();
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let mut join = AdmittedHashJoin::new("test", &memory, &ledger, None);
        let mut adapter = EqualityAdapter {
            matches: Vec::new(),
        };
        for value in [1, 2] {
            join.push_build(AdmittedHashJoinRecord {
                hash: 0,
                binding: EqualityAdapter::binding(value, 0),
            })
            .expect("admit build record");
        }
        join.finish_build().expect("finish build");
        for value in [1, 2, 3] {
            join.push_probe(
                AdmittedHashJoinRecord {
                    hash: 0,
                    binding: EqualityAdapter::binding(value, 0),
                },
                &mut adapter,
            )
            .expect("probe equal-hash records");
        }
        join.finish(&mut adapter).expect("finish in-memory join");
        assert_eq!(adapter.matches, [1, 2]);
        assert_eq!(join.work().candidate_rows, 6);
        drop(join);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn replay_rejects_an_inconsistent_recorded_hash_and_releases_leases() {
        let memory = spill_memory("inconsistent-hash");
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let mut join = AdmittedHashJoin::new("test", &memory, &ledger, None);
        let mut adapter = EqualityAdapter {
            matches: Vec::new(),
        };
        for value in 0..16 {
            join.push_build(AdmittedHashJoinRecord {
                hash: if value == 0 { 1 } else { value as u64 },
                binding: EqualityAdapter::binding(value, 8 * 1024),
            })
            .expect("admit spill-backed build record");
        }
        join.finish_build().expect("finish spill-backed build");
        join.push_probe(
            AdmittedHashJoinRecord {
                hash: 1,
                binding: EqualityAdapter::binding(1, 0),
            },
            &mut adapter,
        )
        .expect("admit spill-backed probe record");
        let error = join
            .finish(&mut adapter)
            .expect_err("replay must validate the recorded hash");
        assert!(matches!(error, HawDBError::StorageIntegrity(_)));
        drop(join);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        std::fs::remove_dir_all(&memory.spill_directory)
            .expect("remove inconsistent-hash spill fixture");
    }

    #[test]
    fn constrained_hot_partition_preflights_live_io_against_table_capacity() {
        let memory = spill_memory_with_blocking_budget("constrained-hot-partition", 40 * 1024);
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let mut join = AdmittedHashJoin::new("test", &memory, &ledger, None);
        let mut adapter = EqualityAdapter {
            matches: Vec::new(),
        };

        for _ in 0..64 {
            join.push_build(AdmittedHashJoinRecord {
                hash: 0,
                binding: EqualityAdapter::binding(0, 1_024),
            })
            .expect("admit constrained spill-backed build record");
        }
        join.finish_build().expect("finish constrained build");
        for _ in 0..64 {
            join.push_probe(
                AdmittedHashJoinRecord {
                    hash: 0,
                    binding: EqualityAdapter::binding(0, 0),
                },
                &mut adapter,
            )
            .expect("admit constrained spill-backed probe record");
        }
        join.finish(&mut adapter)
            .expect("complete constrained hot partition");

        assert_eq!(adapter.matches.len(), 64 * 64);
        assert!(adapter.matches.iter().all(|value| *value == 0));
        assert!(join.report().spilled_rows > 0);
        drop(join);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        std::fs::remove_dir_all(&memory.spill_directory)
            .expect("remove constrained hot-partition fixture");
    }
}
