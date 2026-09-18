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
use crate::observer::NoopExecutionObserver;
use hawdb_plan::SortKey;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

struct Rows(Vec<Binding>);

impl BindingBatchSource for Rows {
    fn execute(
        &mut self,
        _: &PhysicalPlan,
        _: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        emit(std::mem::take(&mut self.0))
    }
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        loop {
            let path = std::env::temp_dir().join(format!(
                "hawdb-sort-merge-accounts-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, AtomicOrdering::Relaxed),
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create sort fixture directory: {error}"),
            }
        }
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn assert_spilled_output_has_its_own_account(top_n: bool) {
    let directory = Directory::new();
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(512).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(512).unwrap(),
        batch_rows: NonZeroUsize::new(4).unwrap(),
        query_memory_bytes: NonZeroUsize::new(4096).unwrap(),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_directory: directory.0.clone(),
        ..ExecutionMemoryConfig::default()
    };
    let catalog = Catalog::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let input = PhysicalPlan::SeqNodeScan {
        variable: "n".to_string(),
        label: String::new(),
    };
    let binding = |value| Binding::scalar("value", Value::Int(value));
    let mut source = Rows((0..12).rev().map(binding).collect());
    let items = [SortItem {
        key: SortKey::Column("value".to_string()),
        direction: SortDirection::Asc,
    }];
    let context = BlockingExecutionContext {
        catalog: &catalog,
        memory: &memory,
        memory_ledger: &ledger,
        task_context: None,
        observer: &NoopExecutionObserver,
    };
    let mut output = Vec::new();
    let mut emit = |batch: BindingBatch| {
        assert!(batch.len() <= memory.batch_rows.get());
        assert!(
            batch.iter().map(binding_memory_bytes).sum::<usize>()
                <= memory.batch_payload_bytes.get()
        );
        output.extend(batch);
        Ok(BatchControl::Continue)
    };
    let result = if top_n {
        stream_top_n_batches(
            &input,
            &items,
            2,
            6,
            &mut source,
            context,
            ExecutionLimit {
                output_rows: Some(4),
            },
            &mut emit,
        )
    } else {
        stream_sort_batches(
            &input,
            &items,
            &mut source,
            context,
            ExecutionLimit::unlimited(),
            &mut emit,
        )
    };
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert!(snapshot.peak_bytes <= memory.query_memory_bytes.get());
    assert!(snapshot
        .classes
        .iter()
        .any(|class| { class.class == QueryMemoryClass::SpillStaging && class.peak_bytes > 0 }));
    assert_eq!(memory.spill_pool_snapshot().unwrap().active_runs, 0);
    result.expect("spilled merge must admit heap and output through separate accounts");
    let expected: Vec<_> = if top_n { 2..6 } else { 0..12 }.map(binding).collect();
    assert_eq!(output, expected);
    assert!(snapshot
        .classes
        .iter()
        .any(|class| { class.class == QueryMemoryClass::PipelineBatch && class.peak_bytes > 0 }));
}

#[test]
fn spilled_sort_output_does_not_compete_with_merge_heap_account() {
    assert_spilled_output_has_its_own_account(false);
}

#[test]
fn spilled_top_n_output_does_not_compete_with_merge_heap_account() {
    assert_spilled_output_has_its_own_account(true);
}

#[derive(Clone, Copy, Default)]
enum Exit {
    #[default]
    Complete,
    Stop,
    Error,
    Cancel,
}

#[derive(Clone, Copy)]
struct Case {
    top_n: Option<(usize, usize)>,
    parent_cap: Option<usize>,
    direction: SortDirection,
    output_bytes: usize,
    root_bytes: usize,
    exit: Exit,
}

impl Default for Case {
    fn default() -> Self {
        Self {
            top_n: None,
            parent_cap: None,
            direction: SortDirection::Asc,
            output_bytes: 512,
            root_bytes: 4096,
            exit: Exit::Complete,
        }
    }
}

fn tied_rows(seed: u64) -> Vec<Binding> {
    let mut state = seed + 1;
    (0..24)
        .map(|position| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            Binding::values(BTreeMap::from([
                ("value".to_string(), Value::Int(((state >> 32) % 5) as i64)),
                ("position".to_string(), Value::Int(position)),
            ]))
        })
        .collect()
}

fn expected_rows(rows: &[Binding], case: Case) -> Vec<Binding> {
    let mut expected = rows.to_vec();
    // Vec::sort_by is stable; the position payload makes tie order observable.
    expected.sort_by(|left, right| {
        let comparison = left.values["value"].cmp(&right.values["value"]);
        match case.direction {
            SortDirection::Asc => comparison,
            SortDirection::Desc => comparison.reverse(),
        }
    });
    let (offset, limit) = case.top_n.unwrap_or((0, usize::MAX));
    expected
        .into_iter()
        .skip(offset)
        .take(limit)
        .take(case.parent_cap.unwrap_or(usize::MAX))
        .collect()
}

fn run_case(
    rows: &[Binding],
    case: Case,
) -> (
    Result<BatchControl>,
    Vec<Binding>,
    crate::QueryMemoryLedgerSnapshot,
) {
    let directory = Directory::new();
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(512).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(case.output_bytes).unwrap(),
        batch_rows: NonZeroUsize::new(4).unwrap(),
        query_memory_bytes: NonZeroUsize::new(case.root_bytes).unwrap(),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_directory: directory.0.clone(),
        ..ExecutionMemoryConfig::default()
    };
    let catalog = Catalog::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let token = hawdb_core::RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    let context = BlockingExecutionContext {
        catalog: &catalog,
        memory: &memory,
        memory_ledger: &ledger,
        task_context: Some(&task),
        observer: &NoopExecutionObserver,
    };
    let input = PhysicalPlan::SeqNodeScan {
        variable: "n".to_string(),
        label: String::new(),
    };
    let items = [SortItem {
        key: SortKey::Column("value".to_string()),
        direction: case.direction,
    }];
    let mut source = Rows(rows.to_vec());
    let mut output = Vec::new();
    let mut emit = |batch: BindingBatch| {
        assert!(!batch.is_empty());
        assert!(batch.len() <= memory.batch_rows.get());
        assert!(batch.iter().map(binding_memory_bytes).sum::<usize>() <= case.output_bytes);
        output.extend(batch);
        match case.exit {
            Exit::Complete => Ok(BatchControl::Continue),
            Exit::Stop => Ok(BatchControl::Stop),
            Exit::Error => Err(HawDBError::Execution(
                "injected merge callback error".to_string(),
            )),
            Exit::Cancel => {
                token.cancel();
                Ok(BatchControl::Continue)
            }
        }
    };
    let limit = ExecutionLimit {
        output_rows: case.parent_cap,
    };
    let result = if let Some((offset, count)) = case.top_n {
        stream_top_n_batches(
            &input,
            &items,
            offset,
            count,
            &mut source,
            context,
            limit,
            &mut emit,
        )
    } else {
        stream_sort_batches(&input, &items, &mut source, context, limit, &mut emit)
    };
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert!(snapshot.peak_bytes <= case.root_bytes);
    let spill = memory.spill_pool_snapshot().unwrap();
    assert_eq!(spill.active_runs, 0);
    assert_eq!(spill.active_bytes, 0);
    (result, output, snapshot)
}

#[test]
fn spilled_merges_enforce_pipeline_payload_and_cleanup_on_early_exit() {
    let rows = tied_rows(7);
    let row_bytes = binding_memory_bytes(&rows[0]);
    for top_n in [None, Some((2, 12))] {
        for exit in [Exit::Stop, Exit::Error, Exit::Cancel] {
            let case = Case {
                top_n,
                output_bytes: row_bytes,
                exit,
                ..Case::default()
            };
            let (result, output, _) = run_case(&rows, case);
            assert_eq!(output, expected_rows(&rows, case)[..1]);
            match exit {
                Exit::Stop => assert_eq!(result.unwrap(), BatchControl::Stop),
                Exit::Error => assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("injected merge callback error")),
                Exit::Cancel => assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("runtime task stopped")),
                Exit::Complete => unreachable!(),
            }
        }
        let case = Case {
            top_n,
            output_bytes: row_bytes - 1,
            ..Case::default()
        };
        let (result, output, _) = run_case(&rows, case);
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("exceeding batch_payload_bytes"));
        assert!(output.is_empty());
    }
}

#[test]
fn empty_merge_results_emit_no_batch_and_release_all_resources() {
    let rows = tied_rows(29);
    for (top_n, parent_cap) in [
        (None, Some(0)),
        (Some((0, 0)), None),
        (Some((3, 0)), None),
        (Some((2, 12)), Some(0)),
        (Some((rows.len(), 4)), None),
    ] {
        let case = Case {
            top_n,
            parent_cap,
            ..Case::default()
        };
        for input in [&rows[..], &[]] {
            let (result, output, snapshot) = run_case(input, case);
            assert_eq!(result.unwrap(), BatchControl::Continue);
            assert!(output.is_empty());
            assert!(snapshot
                .classes
                .iter()
                .filter(|class| class.class == QueryMemoryClass::PipelineBatch)
                .all(|class| class.peak_bytes == 0));
        }
    }
}

#[test]
fn spilled_merges_keep_exact_query_root_admission() {
    let rows = tied_rows(19);
    for top_n in [None, Some((2, 12))] {
        let case = Case {
            top_n,
            ..Case::default()
        };
        let (result, expected, snapshot) = run_case(&rows, case);
        result.unwrap();
        let peak = snapshot.peak_bytes;
        let (result, output, _) = run_case(
            &rows,
            Case {
                root_bytes: peak,
                ..case
            },
        );
        result.expect("the observed exact peak must be sufficient");
        assert_eq!(output, expected);
        let (result, _, _) = run_case(
            &rows,
            Case {
                root_bytes: peak - 1,
                ..case
            },
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("query_memory_bytes"));
    }
}

#[test]
fn legacy_merge_entrypoint_keeps_pipeline_accounting_and_signature() {
    let directory = Directory::new();
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(512).unwrap(),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_directory: directory.0.clone(),
        ..ExecutionMemoryConfig::default()
    };
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(4096).unwrap());
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "legacy merge",
        memory.blocking_operator_bytes,
    );
    let mut spill = SpillBudgetTracker::with_ledger("legacy merge", &memory, &ledger);
    let catalog = Catalog::default();
    let items = [SortItem {
        key: SortKey::Column("value".to_string()),
        direction: SortDirection::Asc,
    }];
    let mut runs = Vec::new();
    for parity in 0..2 {
        let mut rows: Vec<_> = (0..6)
            .filter(|value| value % 2 == parity)
            .map(|value| {
                SortRunRow::new(
                    &catalog,
                    &items,
                    value as u64,
                    Binding::scalar("value", Value::Int(value)),
                )
            })
            .collect();
        runs.push(spill_sort_run(&mut rows, &mut spill, None).unwrap());
    }
    let mut output = Vec::new();
    merge_sort_runs(
        &runs,
        &items,
        &catalog,
        memory.blocking_operator_bytes,
        &spill,
        &account,
        4,
        0,
        6,
        None,
        &mut |batch| {
            assert!(batch.iter().map(binding_memory_bytes).sum::<usize>() <= 512);
            output.extend(batch);
            Ok(BatchControl::Continue)
        },
    )
    .unwrap();
    assert_eq!(
        output,
        (0..6)
            .map(|value| Binding::scalar("value", Value::Int(value)))
            .collect::<Vec<_>>()
    );
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert!(snapshot
        .classes
        .iter()
        .any(|class| class.class == QueryMemoryClass::PipelineBatch && class.peak_bytes > 0));
}

#[test]
#[ignore = "deterministic local sort merge differential campaign"]
fn sort_merge_account_differential_campaign() {
    let mut cases = 0;
    for seed in 0..3 {
        let rows = tied_rows(seed);
        let row_bytes = binding_memory_bytes(&rows[0]);
        for direction in [SortDirection::Asc, SortDirection::Desc] {
            for output_bytes in [row_bytes, row_bytes * 2, row_bytes * 4] {
                for (top_n, parent_cap) in [
                    (None, None),
                    (None, Some(5)),
                    (Some((2, 9)), None),
                    (Some((3, 7)), Some(3)),
                ] {
                    let case = Case {
                        top_n,
                        parent_cap,
                        direction,
                        output_bytes,
                        ..Case::default()
                    };
                    let (result, output, snapshot) = run_case(&rows, case);
                    result.unwrap();
                    assert_eq!(output, expected_rows(&rows, case));
                    assert!(snapshot
                        .classes
                        .iter()
                        .any(|class| class.class == QueryMemoryClass::SpillStaging
                            && class.peak_bytes > 0));
                    assert!(snapshot
                        .classes
                        .iter()
                        .any(|class| class.class == QueryMemoryClass::PipelineBatch
                            && class.peak_bytes > 0));
                    cases += 1;
                }
            }
        }
    }
    assert_eq!(cases, 72);
    eprintln!("sort merge differential campaign: {cases} cases, 1728 input rows");
}
