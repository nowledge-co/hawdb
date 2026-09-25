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
use std::sync::Mutex;

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

/// Runs one compaction case at a given worker limit. The merge closure below
/// must satisfy `Fn + Sync` regardless of `worker_limit`, so bookkeeping
/// (`observed`, injection matching) uses thread-safe primitives even for a
/// forced-serial (`worker_limit = NonZeroUsize::MIN`) run, where exactly one
/// worker ever calls it and the extra synchronization is a no-op in practice.
///
/// Call-order-exact assertions (`observed.len() == at + 1`) only hold when
/// `worker_limit` forces serial execution: with more than one worker, a
/// pair claimed after the injected failure can still finish concurrently
/// before the failure propagates, so those checks are skipped and replaced
/// with the weaker (but still meaningful under concurrency) "the injected
/// pair was in fact observed" check.
fn check_case(values: Vec<Vec<u64>>, final_count: usize, worker_limit: NonZeroUsize, exit: Exit) {
    let serial = worker_limit == NonZeroUsize::MIN;
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
    let observed = Mutex::new(Vec::new());
    let blocking = fixture.ledger.account(
        crate::QueryMemoryClass::BlockingState,
        "compaction oracle",
        fixture.memory.blocking_operator_bytes,
    );
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        compact_runs_with_memory(
            runs,
            NonZeroUsize::new(final_count).unwrap(),
            CompactionMemory {
                blocking: &blocking,
                spill: &budget,
                worker_limit,
            },
            Some(&context),
            |left, right, child, budget| {
                let _lease = child.reserve(child.budget_bytes().get())?;
                runtime_checkpoint(Some(&context))?;
                let pair = (read_run(left)?, read_run(right)?);
                let injected_at = match exit {
                    Exit::Error(at) | Exit::Panic(at) if pair == expected_pairs[at] => Some(at),
                    _ => None,
                };
                observed
                    .lock()
                    .expect("compaction oracle observed lock should not be poisoned")
                    .push(pair.clone());
                let (output, mut writer) = budget.create_run("compaction-oracle-merge")?;
                for value in pair.0.into_iter().chain(pair.1) {
                    writer.write(
                        value,
                        &Binding::scalar("value", Value::Int(value as i64)),
                        budget,
                    )?;
                    if injected_at.is_some() {
                        match exit {
                            Exit::Error(_) => {
                                return Err(HawDBError::Execution("injected merge failure".into()))
                            }
                            Exit::Panic(_) => panic!("injected merge panic"),
                            _ => unreachable!("injected_at only set for Error/Panic"),
                        }
                    }
                }
                writer.finish()?;
                if matches!(exit, Exit::Cancel) {
                    token.cancel();
                }
                Ok(output)
            },
        )
    }));
    let observed = observed
        .into_inner()
        .expect("compaction oracle observed lock should not be poisoned");
    match exit {
        Exit::Complete => {
            let runs = outcome.unwrap().unwrap();
            assert_eq!(
                runs.iter()
                    .map(|run| read_run(run).unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
            // Concurrent dispatch preserves the *set* of merges a level
            // performs (the pairing itself is still formed up front,
            // sequentially); only the order in which they complete and get
            // recorded here is no longer guaranteed to match `expected_pairs`.
            let mut observed_sorted = observed.clone();
            observed_sorted.sort();
            let mut expected_sorted = expected_pairs.clone();
            expected_sorted.sort();
            assert_eq!(observed_sorted, expected_sorted);
            if serial {
                assert_eq!(observed, expected_pairs);
            }
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
            // Dispatched siblings may finish, but later waves never start
            // after failure. The injected pair must have been attempted.
            assert!(observed.contains(&expected_pairs[at]));
        }
        _ => {
            let error = outcome
                .unwrap()
                .err()
                .expect("compaction must fail closed")
                .to_string();
            let expected_error = match exit {
                Exit::Error(at) => {
                    assert!(observed.contains(&expected_pairs[at]));
                    "injected merge failure"
                }
                Exit::RunBudget(allowed) => {
                    // #730's CAS-based admission is a hard upper bound
                    // regardless of how many pairs race to create a run
                    // concurrently; it is not a promise that exactly
                    // `allowed` extra runs get created before every other
                    // in-flight pair also observes the cap and fails.
                    assert!(budget.run_count() <= seeded_runs + allowed);
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

/// A handful of workers, deliberately larger than any test's level width, so
/// every pair in a level is dispatched concurrently rather than degenerating
/// back to sequential admission.
fn concurrent_worker_limit() -> NonZeroUsize {
    NonZeroUsize::new(8).unwrap()
}

fn copy_pair(left: &SpillRun, right: &SpillRun, budget: &SpillBudgetTracker) -> Result<SpillRun> {
    let (output, mut writer) = budget.create_run("accounted-merge")?;
    for input in [left, right] {
        writer.note_merge_record_bytes(input.merge_record_bytes());
        for value in read_run(input)? {
            writer.write(
                value,
                &Binding::scalar("value", Value::Int(value as i64)),
                budget,
            )?;
        }
    }
    writer.finish()?;
    Ok(output)
}

#[test]
fn independent_merge_allowances_admit_real_concurrency_and_preserve_large_items() {
    use crate::concurrent::SharedExecutorPool;
    use std::sync::Condvar;
    use std::time::Duration;

    for tight in [false, true] {
        let fixture = Fixture::new();
        let mut budget = fixture.budget();
        let runs: Vec<_> = (0..4)
            .map(|i| write_run(&[i], &mut budget).unwrap())
            .collect();
        let pair_bytes = runs[0].merge_record_bytes() * 2;
        let parent_bytes = if tight {
            pair_bytes + 1
        } else {
            pair_bytes * 2 + 1
        };
        let blocking = fixture.ledger.account(
            crate::QueryMemoryClass::BlockingState,
            "concurrent merges",
            NonZeroUsize::new(parent_bytes).unwrap(),
        );
        // Models DISTINCT's schema state, retained throughout compaction.
        let retained = blocking.reserve(1).unwrap();
        let worker_limit = NonZeroUsize::new(2).unwrap();
        let can_overlap = !tight
            && SharedExecutorPool::shared_bounded(worker_limit)
                .is_ok_and(|pool| pool.worker_count() >= 2);
        let arrivals = Mutex::new(0usize);
        let ready = Condvar::new();
        let outputs = compact_runs_with_memory(
            runs,
            NonZeroUsize::new(2).unwrap(),
            CompactionMemory {
                blocking: &blocking,
                spill: &budget,
                worker_limit,
            },
            None,
            |left, right, child, spill| {
                assert_eq!(child.budget_bytes().get(), pair_bytes);
                // A complete merge fits even when its records exceed a naive
                // parent_budget / worker_count / fan_in item allowance.
                let _lease = child.reserve(pair_bytes)?;
                assert!(child.reserve(1).is_err());
                assert!(blocking.peak_bytes() <= parent_bytes);
                let mut count = arrivals.lock().unwrap();
                *count += 1;
                ready.notify_all();
                if can_overlap {
                    let (count, timeout) = ready
                        .wait_timeout_while(count, Duration::from_secs(10), |count| *count < 2)
                        .unwrap();
                    assert!(!timeout.timed_out(), "two admitted merges must overlap");
                    drop(count);
                } else {
                    drop(count);
                }
                copy_pair(left, right, spill)
            },
        )
        .unwrap();
        assert_eq!(*arrivals.lock().unwrap(), 2);
        assert_eq!(
            outputs
                .iter()
                .map(|run| read_run(run).unwrap())
                .collect::<Vec<_>>(),
            vec![vec![0, 1], vec![2, 3]]
        );
        assert_eq!(blocking.peak_bytes(), parent_bytes);
        assert_eq!(fixture.ledger.snapshot().used_bytes, 1);
        assert_eq!(budget.run_count(), 6);
        drop((outputs, retained));
        fixture.assert_released();
    }
}

#[test]
fn conservative_root_bounds_fall_back_without_rejecting_a_serial_working_set() {
    let mut fixture = Fixture::new();
    let row_bytes = crate::binding::binding_memory_bytes(&Binding::scalar("value", Value::Int(0)));
    let staging_bytes =
        crate::spill::binding_record_encoded_len(&Binding::scalar("value", Value::Int(0))).unwrap();
    // Both row heads and one staging payload fit. Reserving maxima for two
    // payloads does not; the original serial query must still succeed.
    fixture.ledger =
        QueryMemoryLedger::new(NonZeroUsize::new(row_bytes * 2 + staging_bytes).unwrap());
    let mut budget = fixture.budget();
    let runs = (0..4)
        .map(|i| write_run(&[i], &mut budget).unwrap())
        .collect();
    let blocking = fixture.ledger.account(
        crate::QueryMemoryClass::BlockingState,
        "tight root",
        fixture.memory.blocking_operator_bytes,
    );
    let output = compact_runs_with_memory(
        runs,
        NonZeroUsize::new(2).unwrap(),
        CompactionMemory {
            blocking: &blocking,
            spill: &budget,
            worker_limit: default_compaction_worker_limit(),
        },
        None,
        |left, right, child, spill| {
            assert_eq!(child.budget_bytes(), blocking.budget_bytes());
            let _rows = child.reserve(row_bytes * 2)?;
            copy_pair(left, right, spill)
        },
    )
    .unwrap();
    assert_eq!(
        fixture.ledger.snapshot().peak_bytes,
        row_bytes * 2 + staging_bytes
    );
    assert_eq!(budget.run_count(), 6);
    drop(output);
    fixture.assert_released();
}

#[test]
fn unequal_run_bounds_survive_multiple_levels_and_shared_staging_limits() {
    for staging_cap in [64, 1024] {
        let fixture = Fixture::new();
        let budget = SpillBudgetTracker::with_ledger_staging_budget(
            "compaction",
            &fixture.memory,
            &fixture.ledger,
            NonZeroUsize::new(staging_cap).unwrap(),
        );
        let runs = (0..9)
            .map(|i| {
                let (run, mut writer) = budget.create_run("unequal").unwrap();
                writer.note_merge_record_bytes(544 + i * 32);
                writer
                    .write(
                        i as u64,
                        &Binding::scalar("value", Value::Int(i as i64)),
                        &budget,
                    )
                    .unwrap();
                writer.finish().unwrap();
                run
            })
            .collect();
        let blocking = fixture.ledger.account(
            crate::QueryMemoryClass::BlockingState,
            "unequal",
            NonZeroUsize::new(4096).unwrap(),
        );
        let output = compact_runs_with_memory(
            runs,
            NonZeroUsize::MIN,
            CompactionMemory {
                blocking: &blocking,
                spill: &budget,
                worker_limit: default_compaction_worker_limit(),
            },
            None,
            |left, right, child, spill| {
                let expected = left.merge_record_bytes() + right.merge_record_bytes();
                assert_eq!(child.budget_bytes().get(), expected);
                let _rows = child.reserve(expected)?;
                let _staging = spill.reserve_staging(1)?;
                copy_pair(left, right, spill)
            },
        )
        .unwrap();
        assert_eq!(read_run(&output[0]).unwrap(), (0..9).collect::<Vec<_>>());
        assert_eq!(budget.run_count(), 17);
        assert!(budget.staging_peak_bytes() <= staging_cap);
        drop(output);
        fixture.assert_released();
    }
}

#[test]
fn real_runs_match_pairwise_reference_at_both_final_fanins() {
    for count in 0..=17 {
        for final_count in [1, 2] {
            for worker_limit in [NonZeroUsize::MIN, concurrent_worker_limit()] {
                check_case(
                    values(count, 100),
                    final_count,
                    worker_limit,
                    Exit::Complete,
                );
                check_case(
                    vec![Vec::new(); count],
                    final_count,
                    worker_limit,
                    Exit::Complete,
                );
            }
        }
    }
}

/// The acceptance criterion this issue was opened for: a forced-serial run
/// and a forced-concurrent run over the same input produce byte-identical
/// compacted output, not just each independently matching the reference.
#[test]
fn concurrent_compaction_matches_serial_byte_for_byte() {
    for count in 0..=17 {
        for final_count in [1, 2] {
            let input = values(count, 100);
            let fixture = Fixture::new();
            let mut budget = fixture.budget();
            let blocking = fixture.ledger.account(
                crate::QueryMemoryClass::BlockingState,
                "parity",
                fixture.memory.blocking_operator_bytes,
            );
            let mut run_case = |worker_limit| {
                let runs = input
                    .iter()
                    .map(|values| write_run(values, &mut budget).unwrap())
                    .collect();
                let output = compact_runs_with_memory(
                    runs,
                    NonZeroUsize::new(final_count).unwrap(),
                    CompactionMemory {
                        blocking: &blocking,
                        spill: &budget,
                        worker_limit,
                    },
                    None,
                    |left, right, _, spill| copy_pair(left, right, spill),
                )
                .unwrap();
                output
                    .iter()
                    .map(|run| std::fs::read(run.lease.path()).unwrap())
                    .collect::<Vec<_>>()
            };
            let serial_values = run_case(NonZeroUsize::MIN);
            let concurrent_values = run_case(concurrent_worker_limit());
            fixture.assert_released();

            assert_eq!(
                serial_values, concurrent_values,
                "concurrent compaction must match serial byte-for-byte (count={count}, final_count={final_count})"
            );
        }
    }
}

#[test]
fn every_merge_failure_and_unwind_releases_partial_outputs_and_pending_inputs() {
    for final_count in [1, 2] {
        let input = values(9, 200);
        let (_, pairs) = reference(input.clone(), final_count);
        for at in 0..pairs.len() {
            for worker_limit in [NonZeroUsize::MIN, concurrent_worker_limit()] {
                check_case(input.clone(), final_count, worker_limit, Exit::Error(at));
                check_case(input.clone(), final_count, worker_limit, Exit::Panic(at));
                check_case(
                    input.clone(),
                    final_count,
                    worker_limit,
                    Exit::RunBudget(at),
                );
            }
        }
        for worker_limit in [NonZeroUsize::MIN, concurrent_worker_limit()] {
            check_case(input.clone(), final_count, worker_limit, Exit::ByteBudget);
            check_case(input.clone(), final_count, worker_limit, Exit::Cancel);
        }
    }
}

#[test]
fn cancelled_levels_release_runs_without_calling_the_merger() {
    for worker_limit in [NonZeroUsize::MIN, concurrent_worker_limit()] {
        let fixture = Fixture::new();
        let mut budget = fixture.budget();
        let runs = (0..5)
            .map(|index| write_run(&[index], &mut budget).unwrap())
            .collect();
        let token = RuntimeCancellationToken::new();
        token.cancel();
        let context = RuntimeTaskContext::without_deadline(token);
        let blocking = fixture.ledger.account(
            crate::QueryMemoryClass::BlockingState,
            "cancelled",
            fixture.memory.blocking_operator_bytes,
        );
        let result = compact_runs_with_memory(
            runs,
            NonZeroUsize::MIN,
            CompactionMemory {
                blocking: &blocking,
                spill: &budget,
                worker_limit,
            },
            Some(&context),
            |_, _, _, _| panic!("a cancelled level must not invoke its merger"),
        );
        assert!(result.is_err());
        fixture.assert_released();
    }
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
            for worker_limit in [NonZeroUsize::MIN, concurrent_worker_limit()] {
                check_case(input.clone(), final_count, worker_limit, Exit::Complete);
                let (_, pairs) = reference(input.clone(), final_count);
                if !pairs.is_empty() {
                    let at = state as usize % pairs.len();
                    for exit in [Exit::Error(at), Exit::RunBudget(at), Exit::ByteBudget] {
                        check_case(input.clone(), final_count, worker_limit, exit);
                        failures += 1;
                    }
                }
                // Cancellation must precede another checkpoint, not the final return.
                if pairs.len() > 1 {
                    check_case(input.clone(), final_count, worker_limit, Exit::Cancel);
                    failures += 1;
                }
            }
        }
    }
    println!(
        "Spill compaction differential: 512 successful and {failures} failing real-file fixtures, both final fan-ins and worker limits"
    );
}
