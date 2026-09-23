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

use super::*;
use crate::binding::Binding;
use crate::kernel::SpillBudgetTracker;
use crate::{ExecutionMemoryConfig, QueryMemoryLedger};
use hawdb_core::{HawDBError, RuntimeCancellationToken, Value};
use std::num::NonZeroU64;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture {
    memory: ExecutionMemoryConfig,
    ledger: QueryMemoryLedger,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = loop {
            let path = std::env::temp_dir().join(format!(
                "hawdb-compaction-oracle-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => break path,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create compaction fixture: {error}"),
            }
        };
        let memory = ExecutionMemoryConfig {
            spill_directory: directory,
            min_spill_free_bytes: NonZeroU64::MIN,
            ..ExecutionMemoryConfig::default()
        };
        Self {
            ledger: QueryMemoryLedger::new(memory.query_memory_bytes),
            memory,
        }
    }

    fn budget(&self) -> SpillBudgetTracker {
        SpillBudgetTracker::with_ledger("CompactionOracle", &self.memory, &self.ledger)
    }

    fn assert_released(&self) {
        assert_eq!(self.ledger.snapshot().used_bytes, 0);
        let pool = self.memory.spill_pool_snapshot().unwrap();
        assert_eq!(pool.active_runs, 0);
        assert_eq!(pool.active_bytes, 0);
        assert_eq!(pool.pending_write_bytes, 0);
        assert_eq!(
            std::fs::read_dir(&self.memory.spill_directory)
                .unwrap()
                .count(),
            0
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Do not hide leaked files with recursive fixture cleanup.
        std::fs::remove_dir(&self.memory.spill_directory).unwrap();
    }
}

fn write_run(values: &[u64], budget: &mut SpillBudgetTracker) -> Result<SpillRun> {
    let (run, mut writer) = budget.create_run("compaction-oracle")?;
    for &value in values {
        writer.write(
            value,
            &Binding::scalar("value", Value::Int(value as i64)),
            budget,
        )?;
    }
    writer.finish()?;
    Ok(run)
}

fn read_run(run: &SpillRun) -> Result<Vec<u64>> {
    let mut reader = run.reader()?;
    let mut values = Vec::new();
    while let Some((ordinal, binding)) = reader.read(1024)? {
        assert_eq!(
            binding,
            Binding::scalar("value", Value::Int(ordinal as i64))
        );
        values.push(ordinal);
    }
    Ok(values)
}

type Pair = (Vec<u64>, Vec<u64>);

fn reference(mut runs: Vec<Vec<u64>>, final_count: usize) -> (Vec<Vec<u64>>, Vec<Pair>) {
    let mut pairs = Vec::new();
    while runs.len() > final_count {
        runs = runs
            .chunks(2)
            .map(|chunk| {
                if chunk.len() == 2 {
                    pairs.push((chunk[0].clone(), chunk[1].clone()));
                }
                chunk.concat()
            })
            .collect();
    }
    (runs, pairs)
}

#[derive(Clone, Copy)]
enum Exit {
    Complete,
    Error(usize),
    Panic(usize),
    RunBudget(usize),
    ByteBudget,
    Cancel,
}

fn check_case(values: Vec<Vec<u64>>, final_count: usize, exit: Exit) {
    let fixture = Fixture::new();
    let mut budget = fixture.budget();
    let runs = values
        .iter()
        .map(|values| write_run(values, &mut budget).unwrap())
        .collect();
    let seeded_runs = budget.run_count();
    let (expected, expected_pairs) = reference(values, final_count);
    if let Exit::RunBudget(allowed) = exit {
        budget.max_runs = seeded_runs + allowed;
    }
    if matches!(exit, Exit::ByteBudget) {
        budget.max_bytes = budget.used_bytes();
    }
    let token = RuntimeCancellationToken::new();
    let context = RuntimeTaskContext::without_deadline(token.clone());
    let mut observed = Vec::new();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        compact_runs(
            runs,
            NonZeroUsize::new(final_count).unwrap(),
            Some(&context),
            |left, right| {
                runtime_checkpoint(Some(&context))?;
                let pair = (read_run(left)?, read_run(right)?);
                let index = observed.len();
                assert_eq!(pair, expected_pairs[index]);
                observed.push(pair.clone());
                let (output, mut writer) = budget.create_run("compaction-oracle-merge")?;
                for value in pair.0.into_iter().chain(pair.1) {
                    writer.write(
                        value,
                        &Binding::scalar("value", Value::Int(value as i64)),
                        &budget,
                    )?;
                    match exit {
                        Exit::Error(at) if index == at => {
                            return Err(HawDBError::Execution("injected merge failure".into()))
                        }
                        Exit::Panic(at) if index == at => panic!("injected merge panic"),
                        _ => {}
                    }
                }
                writer.finish()?;
                let pool = fixture.memory.spill_pool_snapshot().unwrap();
                assert!(
                    pool.active_runs <= seeded_runs + 1,
                    "completed input pairs must be released promptly"
                );
                if matches!(exit, Exit::Cancel) {
                    token.cancel();
                }
                Ok(output)
            },
        )
    }));
    match exit {
        Exit::Complete => {
            let runs = outcome.unwrap().unwrap();
            assert_eq!(
                runs.iter()
                    .map(|run| read_run(run).unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(observed, expected_pairs);
            assert_eq!(budget.run_count(), seeded_runs + observed.len());
            assert_eq!(
                fixture.memory.spill_pool_snapshot().unwrap().active_runs,
                runs.len()
            );
            drop(runs);
        }
        Exit::Panic(at) => {
            let payload = outcome.err().expect("merge must unwind");
            assert_eq!(
                payload.downcast_ref::<&str>(),
                Some(&"injected merge panic")
            );
            assert_eq!(observed.len(), at + 1);
        }
        _ => {
            let error = outcome
                .unwrap()
                .err()
                .expect("compaction must fail closed")
                .to_string();
            let expected_error = match exit {
                Exit::Error(at) => {
                    assert_eq!(observed.len(), at + 1);
                    "injected merge failure"
                }
                Exit::RunBudget(allowed) => {
                    assert_eq!(budget.run_count(), seeded_runs + allowed);
                    assert_eq!(observed.len(), allowed + 1);
                    "max_spill_runs"
                }
                Exit::ByteBudget => "max_spill_bytes",
                Exit::Cancel => "cancel",
                _ => unreachable!(),
            };
            assert!(
                error.contains(expected_error),
                "unexpected failure: {error}"
            );
        }
    }
    fixture.assert_released();
}

fn values(count: usize, seed: u64) -> Vec<Vec<u64>> {
    (0..count)
        .map(|index| vec![seed + index as u64, index as u64 % 3])
        .collect()
}

#[test]
fn real_runs_match_pairwise_reference_at_both_final_fanins() {
    for count in 0..=17 {
        for final_count in [1, 2] {
            check_case(values(count, 100), final_count, Exit::Complete);
            check_case(vec![Vec::new(); count], final_count, Exit::Complete);
        }
    }
}

#[test]
fn every_merge_failure_and_unwind_releases_partial_outputs_and_pending_inputs() {
    for final_count in [1, 2] {
        let input = values(9, 200);
        let (_, pairs) = reference(input.clone(), final_count);
        for at in 0..pairs.len() {
            check_case(input.clone(), final_count, Exit::Error(at));
            check_case(input.clone(), final_count, Exit::Panic(at));
            check_case(input.clone(), final_count, Exit::RunBudget(at));
        }
        check_case(input.clone(), final_count, Exit::ByteBudget);
        check_case(input, final_count, Exit::Cancel);
    }
}

#[test]
fn cancelled_levels_release_runs_without_calling_the_merger() {
    let fixture = Fixture::new();
    let mut budget = fixture.budget();
    let runs = (0..5)
        .map(|index| write_run(&[index], &mut budget).unwrap())
        .collect();
    let token = RuntimeCancellationToken::new();
    token.cancel();
    let context = RuntimeTaskContext::without_deadline(token);
    let result = compact_runs(runs, NonZeroUsize::MIN, Some(&context), |_, _| {
        panic!("a cancelled level must not invoke its merger")
    });
    assert!(result.is_err());
    fixture.assert_released();
}

#[test]
#[ignore = "local deterministic spill-file compaction campaign"]
fn compaction_differential_campaign() {
    let mut state = 220u64;
    let mut failures = 0;
    for _ in 0..128 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let count = (state >> 32) as usize % 33;
        for final_count in [1, 2] {
            let input = values(count, state % 10_000);
            check_case(input.clone(), final_count, Exit::Complete);
            let (_, pairs) = reference(input.clone(), final_count);
            if !pairs.is_empty() {
                let at = state as usize % pairs.len();
                for exit in [Exit::Error(at), Exit::RunBudget(at), Exit::ByteBudget] {
                    check_case(input.clone(), final_count, exit);
                    failures += 1;
                }
            }
            // Cancellation must precede another checkpoint, not the final return.
            if pairs.len() > 1 {
                check_case(input, final_count, Exit::Cancel);
                failures += 1;
            }
        }
    }
    println!(
        "Spill compaction differential: 256 successful and {failures} failing real-file fixtures, both final fan-ins"
    );
}
