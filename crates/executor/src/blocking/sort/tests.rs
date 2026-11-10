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
use hawdb_plan_cypher::SortKey;
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
        graph: None,
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

/// Sorts `value` (NULL for every third row) by itself, then `id`, through the
/// complete sort or top-k operator, resident or spilled with encoded or
/// re-evaluated expression keys.
fn null_ordered_ids(
    top_n: bool,
    spill: bool,
    expression: bool,
    direction: SortDirection,
) -> Vec<Value> {
    let directory = Directory::new();
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(if spill { 1024 } else { 1 << 20 }).unwrap(),
        batch_rows: NonZeroUsize::new(4).unwrap(),
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
    let rows = (0..12)
        .rev()
        .map(|id| {
            let mut binding = Binding::scalar("id", Value::Int(id));
            let value = if id % 3 == 0 {
                Value::Null
            } else {
                Value::Int(id % 4)
            };
            binding.values.insert("value".to_string(), value);
            binding
        })
        .collect();
    let value = if expression {
        SortKey::Expression(hawdb_plan_cypher::ProjectionExpression::Binary {
            left: Box::new(hawdb_plan_cypher::ProjectionExpression::Column(
                "value".to_string(),
            )),
            op: hawdb_plan_cypher::ScalarBinaryOp::Add,
            right: Box::new(hawdb_plan_cypher::ProjectionExpression::Literal(
                Value::Int(0),
            )),
        })
    } else {
        SortKey::Column("value".to_string())
    };
    let items = [
        SortItem {
            key: value,
            direction,
        },
        SortItem {
            key: SortKey::Column("id".to_string()),
            direction: SortDirection::Asc,
        },
    ];
    let context = BlockingExecutionContext {
        catalog: &catalog,
        memory: &memory,
        memory_ledger: &ledger,
        task_context: None,
        observer: &NoopExecutionObserver,
        graph: None,
    };
    let mut output = Vec::new();
    let mut emit = |batch: BindingBatch| {
        output.extend(
            batch
                .into_iter()
                .map(|binding| binding.values["id"].clone()),
        );
        Ok(BatchControl::Continue)
    };
    let mut source = Rows(rows);
    if top_n {
        stream_top_n_batches(
            &input,
            &items,
            1,
            10,
            &mut source,
            context,
            ExecutionLimit::unlimited(),
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
    }
    .unwrap();
    assert_eq!(memory.spill_pool_snapshot().unwrap().active_runs, 0);
    let spilled = ledger
        .snapshot()
        .classes
        .iter()
        .any(|class| class.class == QueryMemoryClass::SpillStaging && class.peak_bytes > 0);
    assert_eq!(spilled, spill);
    output
}

#[test]
fn sort_and_top_n_place_null_last_ascending_and_first_descending() {
    // Non-null values by id: 1→1, 2→2, 4→0, 5→1, 7→3, 8→0, 10→2, 11→3.
    let ascending = [4, 8, 1, 5, 2, 10, 7, 11, 0, 3, 6, 9].map(Value::Int);
    let descending = [0, 3, 6, 9, 7, 11, 2, 10, 1, 5, 4, 8].map(Value::Int);
    for top_n in [false, true] {
        for spill in [false, true] {
            for expression in [false, true] {
                for (direction, expected) in [
                    (SortDirection::Asc, &ascending),
                    (SortDirection::Desc, &descending),
                ] {
                    let expected = if top_n {
                        &expected[1..11]
                    } else {
                        &expected[..]
                    };
                    assert_eq!(
                        null_ordered_ids(top_n, spill, expression, direction),
                        expected,
                        "top_n={top_n} spill={spill} expression={expression} {direction:?}"
                    );
                }
            }
        }
    }
}

fn check_in_memory_output_accounting(top_n: bool) {
    for exit in [Exit::Complete, Exit::Stop, Exit::Error, Exit::Cancel] {
        check_in_memory_output_exit(top_n, exit);
    }
}

fn check_in_memory_output_exit(top_n: bool, exit: Exit) {
    let token = hawdb_core::RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(2048).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(200).unwrap(),
        batch_rows: NonZeroUsize::new(4).unwrap(),
        query_memory_bytes: NonZeroUsize::new(2048).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let catalog = Catalog::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut source = Rows(
        (0..4)
            .rev()
            .map(|value| Binding::scalar("value", Value::Int(value)))
            .collect(),
    );
    let items = [SortItem {
        key: SortKey::Column("value".into()),
        direction: SortDirection::Asc,
    }];
    let context = BlockingExecutionContext {
        catalog: &catalog,
        memory: &memory,
        memory_ledger: &ledger,
        task_context: Some(&task),
        observer: &NoopExecutionObserver,
        graph: None,
    };
    let mut output = Vec::new();
    let mut emit = |batch: BindingBatch| {
        assert!(batch.len() <= memory.batch_rows.get());
        assert!(
            batch.iter().map(binding_memory_bytes).sum::<usize>()
                <= memory.batch_payload_bytes.get(),
            "sorted output must respect the payload cap"
        );
        output.extend(batch);
        match exit {
            Exit::Complete => Ok(BatchControl::Continue),
            Exit::Stop => Ok(BatchControl::Stop),
            Exit::Error => Err(HawDBError::Execution("consumer failure".into())),
            Exit::Cancel => {
                token.cancel();
                Ok(BatchControl::Continue)
            }
        }
    };
    let result = if top_n {
        stream_top_n_batches(
            &PhysicalPlan::EmptyExec,
            &items,
            1,
            2,
            &mut source,
            context,
            ExecutionLimit::unlimited(),
            &mut emit,
        )
    } else {
        stream_sort_batches(
            &PhysicalPlan::EmptyExec,
            &items,
            &mut source,
            context,
            ExecutionLimit::unlimited(),
            &mut emit,
        )
    };
    match exit {
        Exit::Complete => assert_eq!(result.unwrap(), BatchControl::Continue),
        Exit::Stop => assert_eq!(result.unwrap(), BatchControl::Stop),
        Exit::Error => assert!(result.unwrap_err().to_string().contains("consumer failure")),
        Exit::Cancel => assert!(result
            .unwrap_err()
            .to_string()
            .contains("runtime task stopped")),
    }
    let mut expected: Vec<_> = if top_n { 1..3 } else { 0..4 }
        .map(|value| Binding::scalar("value", Value::Int(value)))
        .collect();
    if !matches!(exit, Exit::Complete) {
        expected.truncate(1);
    }
    assert_eq!(output, expected);
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert!(snapshot.peak_bytes <= memory.query_memory_bytes.get());
    assert!(
        snapshot
            .classes
            .iter()
            .any(|class| class.class == QueryMemoryClass::PipelineBatch && class.peak_bytes > 0),
        "sorted output must acquire a pipeline account"
    );
    assert!(
        snapshot
            .classes
            .iter()
            .all(|class| class.class != QueryMemoryClass::SpillStaging || class.peak_bytes == 0),
        "this guard must exercise the in-memory path"
    );
}

#[test]
fn in_memory_sort_obeys_output_payload_and_transfers_memory_ownership() {
    check_in_memory_output_accounting(false);
}

#[test]
fn in_memory_top_n_obeys_output_payload_and_transfers_memory_ownership() {
    check_in_memory_output_accounting(true);
}

#[test]
fn capped_resident_sort_and_top_n_release_discarded_rows_before_parent_admission() {
    for (batch_rows, result_rows, query_bytes) in [(4, 1, 1024), (1, 1, 1024), (2, 2, 800)] {
        for top_n in [false, true] {
            let memory = ExecutionMemoryConfig {
                blocking_operator_bytes: NonZeroUsize::new(1024).unwrap(),
                batch_payload_bytes: NonZeroUsize::new(512).unwrap(),
                batch_rows: NonZeroUsize::new(batch_rows).unwrap(),
                query_memory_bytes: NonZeroUsize::new(query_bytes).unwrap(),
                ..ExecutionMemoryConfig::default()
            };
            let catalog = Catalog::default();
            let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
            let mut source = Rows(
                (0..4)
                    .rev()
                    .map(|value| Binding::scalar("value", Value::Int(value)))
                    .collect(),
            );
            let items = [SortItem {
                key: SortKey::Column("value".into()),
                direction: SortDirection::Asc,
            }];
            let context = BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: None,
                observer: &NoopExecutionObserver,
                graph: None,
            };
            let mut output = Vec::new();
            let mut emit = |batch: BindingBatch| {
                let _parent = crate::pipeline::TransformBatchBuilder::new(
                    "ProjectExec",
                    memory.batch_rows.get(),
                    memory.batch_payload_bytes,
                    &ledger,
                )?;
                output.extend(batch);
                Ok(BatchControl::Continue)
            };
            let cap = ExecutionLimit {
                output_rows: Some(result_rows),
            };
            let result = if top_n {
                stream_top_n_batches(
                    &PhysicalPlan::EmptyExec,
                    &items,
                    0,
                    4,
                    &mut source,
                    context,
                    cap,
                    &mut emit,
                )
            } else {
                stream_sort_batches(
                    &PhysicalPlan::EmptyExec,
                    &items,
                    &mut source,
                    context,
                    cap,
                    &mut emit,
                )
            };
            result.expect("discarded retention must not reject parent output admission");
            assert_eq!(
                output,
                (0..result_rows)
                    .map(|value| Binding::scalar("value", Value::Int(value as i64)))
                    .collect::<Vec<_>>()
            );
            assert_eq!(ledger.snapshot().used_bytes, 0);
            assert!(ledger.snapshot().classes.iter().all(|class| class.class
                != QueryMemoryClass::SpillStaging
                || class.peak_bytes == 0));
        }
    }
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
    blocking_bytes: Option<usize>,
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
            blocking_bytes: None,
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
    // Two merge heads must fit, including each key's direction and Vec storage.
    let merge_row_bytes = rows
        .iter()
        .map(|binding| {
            binding_memory_bytes(binding)
                + std::mem::size_of::<Vec<(Value, SortDirection)>>()
                + std::mem::size_of::<(Value, SortDirection)>()
        })
        .max()
        .unwrap_or(256);
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(
            case.blocking_bytes.unwrap_or(merge_row_bytes * 2),
        )
        .unwrap(),
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
        graph: None,
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
fn in_place_sort_preserves_tie_order_with_and_without_spilling() {
    let rows = tied_rows(43);
    for blocking_bytes in [None, Some(16 * 1024)] {
        for top_n in [None, Some((2, 12))] {
            for direction in [SortDirection::Asc, SortDirection::Desc] {
                let case = Case {
                    blocking_bytes,
                    top_n,
                    direction,
                    output_bytes: 2048,
                    root_bytes: 32 * 1024,
                    ..Case::default()
                };
                let (result, output, snapshot) = run_case(&rows, case);
                result.unwrap();
                assert_eq!(output, expected_rows(&rows, case));
                let spilled = snapshot.classes.iter().any(|class| {
                    class.class == QueryMemoryClass::SpillStaging && class.peak_bytes > 0
                });
                assert_eq!(spilled, blocking_bytes.is_none());
            }
        }
    }
}

#[test]
fn spilled_sort_and_top_n_preserve_wide_rows() {
    let mut rows = tied_rows(47);
    for (index, row) in rows.iter_mut().enumerate() {
        row.values.insert(
            "payload".to_string(),
            Value::Binary(vec![index as u8; 16 * 1024]),
        );
    }
    for top_n in [None, Some((2, 12))] {
        let case = Case {
            top_n,
            blocking_bytes: Some(64 * 1024),
            output_bytes: 64 * 1024,
            root_bytes: 256 * 1024,
            ..Case::default()
        };
        let (result, output, snapshot) = run_case(&rows, case);
        result.unwrap();
        assert_eq!(output, expected_rows(&rows, case));
        assert!(snapshot.classes.iter().any(|class| {
            class.class == QueryMemoryClass::SpillStaging && class.peak_bytes > 0
        }));
    }
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
fn spilled_merges_adapt_without_exceeding_query_root_admission() {
    let rows = tied_rows(19);
    let mut adapted_below_observed_peak = false;
    for top_n in [None, Some((2, 12))] {
        let case = Case {
            top_n,
            ..Case::default()
        };
        let (result, expected, snapshot) = run_case(&rows, case);
        result.unwrap();
        let peak = snapshot.peak_bytes;
        for root_bytes in [peak, peak - 1, peak * 3 / 4] {
            let (result, output, snapshot) = run_case(&rows, Case { root_bytes, ..case });
            assert!(snapshot.peak_bytes <= root_bytes);
            match result {
                Ok(_) => {
                    assert_eq!(output, expected);
                    adapted_below_observed_peak |= root_bytes < peak;
                }
                Err(HawDBError::ResourceBudgetExceeded(cause)) => {
                    assert!(root_bytes < peak, "the observed peak must be sufficient");
                    assert_eq!(cause.scope, hawdb_core::ResourceBudgetScope::Query);
                    assert_eq!(cause.budget, root_bytes as u64);
                    assert!(cause.used + cause.requested > cause.budget);
                }
                Err(error) => panic!("unexpected merge error: {error}"),
            }
        }
        // Flexible IO buffers cannot make an owned output row fit below its
        // irreducible allocation. Keep a definite hard-refusal boundary too.
        let root_bytes = binding_memory_bytes(&rows[0]) - 1;
        let (result, output, snapshot) = run_case(&rows, Case { root_bytes, ..case });
        let HawDBError::ResourceBudgetExceeded(cause) = result.unwrap_err() else {
            panic!("an insufficient query budget must fail explicitly");
        };
        assert_eq!(cause.resource, hawdb_core::ResourceBudgetKind::MemoryBytes);
        assert_eq!(cause.scope, hawdb_core::ResourceBudgetScope::Query);
        assert_eq!(cause.budget, root_bytes as u64);
        assert!(cause.used + cause.requested > cause.budget);
        assert!(snapshot.peak_bytes <= root_bytes);
        assert!(output.is_empty());
    }
    assert!(adapted_below_observed_peak);
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
            .map(|value| SortRunRow {
                sort_values: vec![(Value::Int(value), SortDirection::Asc)],
                ordinal: value as u64,
                binding: Binding::scalar("value", Value::Int(value)),
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
fn legacy_merge_refusal_preserves_explicit_budget_origin_and_releases_state() {
    let directory = Directory::new();
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(4096).unwrap(),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_directory: directory.0.clone(),
        ..ExecutionMemoryConfig::default()
    };
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(8192).unwrap());
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "legacy merge",
        memory.blocking_operator_bytes,
    );
    let mut spill = SpillBudgetTracker::with_ledger("legacy merge", &memory, &ledger);
    let rows: Vec<_> = (0..2)
        .map(|value| SortRunRow {
            sort_values: Vec::new(),
            ordinal: value as u64,
            binding: Binding::scalar("value", Value::Int(value)),
        })
        .collect();
    let item_bytes = rows[0].memory_bytes();
    let key_bytes = std::mem::size_of::<Vec<(Value, SortDirection)>>();
    let mut runs = Vec::new();
    for row in rows {
        runs.push(spill_sort_run(&mut vec![row], &mut spill, None).unwrap());
    }
    for (budget, should_pass) in [(key_bytes, false), (item_bytes * 2, true)] {
        let mut output = Vec::new();
        let result = merge_sort_runs(
            &runs,
            &[],
            &Catalog::default(),
            NonZeroUsize::new(budget).unwrap(),
            &spill,
            &account,
            1,
            0,
            2,
            None,
            &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        );
        if should_pass {
            result.unwrap();
            assert_eq!(
                output,
                (0..2)
                    .map(|value| Binding::scalar("value", Value::Int(value)))
                    .collect::<Vec<_>>()
            );
        } else {
            let error = result.unwrap_err();
            let HawDBError::ResourceBudgetExceeded(cause) = error else {
                panic!("expected a typed merge-state refusal: {error}");
            };
            assert_eq!(cause.resource, hawdb_core::ResourceBudgetKind::MemoryBytes);
            assert_eq!(cause.scope, hawdb_core::ResourceBudgetScope::Operator);
            assert_eq!(cause.owner, "legacy merge");
            assert_eq!(cause.memory_class, Some("blocking_state"));
            assert_eq!(cause.budget, budget as u64);
            assert_eq!(cause.used, key_bytes as u64);
            assert_eq!(cause.requested, key_bytes as u64);
            assert_eq!(
                cause.budget_option,
                "OperatorMemoryTracker::with_account(budget_bytes)"
            );
            assert!(output.is_empty());
        }
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn spilled_json_sort_keys_use_query_working_memory_during_merge_and_compaction() {
    use hawdb_core::ResourceBudgetScope;
    use hawdb_plan_cypher::ProjectionExpression;

    for query_bytes in [8192, 65536] {
        for compact in [false, true] {
            let directory = Directory::new();
            let memory = ExecutionMemoryConfig {
                blocking_operator_bytes: NonZeroUsize::new(4096).unwrap(),
                batch_payload_bytes: NonZeroUsize::new(2048).unwrap(),
                batch_rows: NonZeroUsize::MIN,
                query_memory_bytes: NonZeroUsize::new(query_bytes).unwrap(),
                min_spill_free_bytes: NonZeroU64::MIN,
                spill_directory: directory.0.clone(),
                ..Default::default()
            };
            let catalog = Catalog::default();
            let ledger = QueryMemoryLedger::for_query(&memory, None).unwrap();
            let account = ledger.account(
                QueryMemoryClass::BlockingState,
                "sort merge",
                memory.blocking_operator_bytes,
            );
            let key_memory = KeyMemory::new(
                &account,
                memory.query_memory_bytes,
                "ExecutionMemoryConfig.query_memory_bytes",
                "merge expressions",
                "merge keys",
            );
            let mut spill = SpillBudgetTracker::with_ledger("sort merge", &memory, &ledger);
            let items = [SortItem {
                key: SortKey::Expression(ProjectionExpression::JsonValue {
                    expression: Box::new(ProjectionExpression::Column("metadata".into())),
                    path: "$.rank".into(),
                    null_on_non_scalar: false,
                }),
                direction: SortDirection::Asc,
            }];
            let mut runs = Vec::new();
            for rank in (0..4).rev() {
                let metadata = format!(r#"{{"rank":{rank},"padding":"{}"}}"#, "x".repeat(256));
                let binding = Binding::values(BTreeMap::from([
                    ("rank".into(), Value::Int(rank)),
                    ("metadata".into(), Value::String(metadata)),
                ]));
                // Prebuilt keys isolate reconstruction from initial expression evaluation.
                let mut rows = vec![SortRunRow {
                    sort_values: vec![(Value::Int(rank), SortDirection::Asc)],
                    ordinal: rank as u64,
                    binding,
                }];
                runs.push(spill_sort_run(&mut rows, &mut spill, None).unwrap());
            }
            let mut output = Vec::new();
            let result = (|| {
                let context = RuntimeTaskContext::default();
                context.cancellation().cancel();
                let mut reader = runs[0].reader()?;
                let mut tracker = OperatorMemoryTracker::with_account(
                    memory.blocking_operator_bytes,
                    account.clone(),
                );
                let error = read_sort_merge_entry(
                    &mut reader,
                    0,
                    &items,
                    &catalog,
                    memory.blocking_operator_bytes.get(),
                    memory.blocking_operator_bytes.get(),
                    &spill,
                    &mut tracker,
                    &key_memory,
                    Some(&context),
                )
                .err()
                .expect("cancelled reconstruction must fail");
                assert!(
                    error.to_string().contains("runtime task stopped"),
                    "{error}"
                );
                assert_eq!(tracker.used_bytes, 0);
                assert_eq!(ledger.snapshot().used_bytes, 0);
                if compact {
                    runs =
                        compact_sort_runs(runs, &items, &catalog, &memory, &spill, &account, None)?;
                    assert_eq!(runs.len(), 2);
                }
                merge_sort_runs_with_output(
                    &runs,
                    &items,
                    &catalog,
                    OperatorMemoryTracker::with_blocking_account(
                        memory.blocking_operator_bytes,
                        account.clone(),
                    ),
                    &spill,
                    &key_memory,
                    AccountedBindingBatch::with_ledger(
                        "sort merge",
                        1,
                        memory.batch_payload_bytes,
                        &ledger,
                    ),
                    0,
                    4,
                    None,
                    &mut |batch| {
                        output.extend(batch);
                        Ok(BatchControl::Continue)
                    },
                )
            })();
            if query_bytes == 8192 {
                let HawDBError::ResourceBudgetExceeded(cause) = result.unwrap_err() else {
                    panic!("expected a typed expression working-memory refusal");
                };
                assert_eq!(cause.scope, ResourceBudgetScope::Account);
                assert_eq!(cause.budget, 8192);
                assert!(cause.requested > 8192);
                assert_eq!(
                    cause.budget_option,
                    "ExecutionMemoryConfig.query_memory_bytes"
                );
                assert_eq!(cause.memory_class, Some("blocking_state"));
                assert!(output.is_empty());
            } else {
                result.unwrap();
                assert_eq!(
                    output
                        .iter()
                        .map(|binding| binding.values["rank"].clone())
                        .collect::<Vec<_>>(),
                    (0..4).map(Value::Int).collect::<Vec<_>>()
                );
            }
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
}

#[test]
#[ignore = "deterministic local sort merge differential campaign"]
fn sort_merge_account_differential_campaign() {
    spilled_merges_adapt_without_exceeding_query_root_admission();
    active_sort_releases_rows_and_capacity_under_host_pressure();
    top_n_keeps_its_cutoff_under_sustained_pressure();
    encoded_merge_copies_wide_payloads_with_a_key_sized_working_budget();
    adaptive_merge_fan_in_respects_memory_and_descriptor_capacity();
    in_place_sort_preserves_tie_order_with_and_without_spilling();
    spilled_sort_and_top_n_preserve_wide_rows();
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

#[test]
fn active_sort_releases_rows_and_capacity_under_host_pressure() {
    use hawdb_qos::{
        IoConcurrencyBudget, ProcessMemoryCapabilities, ProcessMemoryPolicy,
        ProcessMemoryPolicyConfig, ProcessMemorySnapshot, RuntimeGovernor, RuntimeGovernorConfig,
        RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
    };
    let directory = Directory::new();
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(128 * 1024).unwrap(),
        query_memory_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_directory: directory.0.clone(),
        ..ExecutionMemoryConfig::default()
    };
    let policy = ProcessMemoryPolicy::new(ProcessMemoryPolicyConfig::new(
        NonZeroU64::new(16 * 1024 * 1024).unwrap(),
    ));
    let snapshot = |resident_bytes| ProcessMemorySnapshot {
        capabilities: ProcessMemoryCapabilities {
            resident_memory: true,
            ..Default::default()
        },
        resident_bytes,
        peak_resident_bytes: resident_bytes,
        total_page_faults: None,
        minor_page_faults: None,
        major_page_faults: None,
    };
    policy.update(snapshot(1024 * 1024));
    let governor = RuntimeGovernor::new_with_process_memory_policy(
        RuntimeGovernorConfig::shared_host(),
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(4, 1),
        policy.clone(),
    );
    let permit = governor
        .try_admit(RuntimeWorkRequest::foreground_query(1024 * 1024, 0))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let ledger = QueryMemoryLedger::for_query(&memory, Some(&task)).unwrap();
    let catalog = Catalog::default();
    let items = [SortItem {
        key: SortKey::Column("key".into()),
        direction: SortDirection::Asc,
    }];
    let mut sort = SortOperator::new(
        &items,
        BlockingExecutionContext {
            catalog: &catalog,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: Some(&task),
            observer: &NoopExecutionObserver,
            graph: None,
        },
    );
    let binding = |key| {
        Binding::values(BTreeMap::from([
            ("key".into(), Value::Int(key)),
            ("body".into(), Value::String("x".repeat(1024))),
        ]))
    };
    for key in (0..48).rev() {
        sort.push(binding(key)).unwrap();
    }
    assert!(sort.runs.is_empty());
    let retained = sort.tracker.used_bytes;
    let capacity = sort.rows.capacity();
    policy.update(snapshot(16 * 1024 * 1024));
    // Feedback is sampled within 64 safe points, before the hard limit.
    for key in 48..65 {
        sort.push(binding(key)).unwrap();
    }
    assert_eq!(sort.runs.len(), 1);
    assert!(sort.tracker.used_bytes < retained / 4);
    assert!(sort.rows.capacity() < capacity);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 1024 * 1024);
    assert_eq!(
        task.memory_reservation().unwrap().memory_bytes(),
        1024 * 1024
    );
    // The progress floor permits a useful batch even under sustained pressure.
    for key in 65..72 {
        sort.push(binding(key)).unwrap();
    }
    assert_eq!(sort.runs.len(), 1);
    let mut actual = Vec::new();
    sort.finish(ExecutionLimit::unlimited(), &mut |batch| {
        actual.extend(batch);
        Ok(BatchControl::Continue)
    })
    .unwrap();
    assert_eq!(actual, (0..72).map(binding).collect::<Vec<_>>());
    assert_eq!(ledger.snapshot().used_bytes, 0);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn encoded_merge_copies_wide_payloads_with_a_key_sized_working_budget() {
    let directory = Directory::new();
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(256 * 1024).unwrap(),
        query_memory_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_directory: directory.0.clone(),
        ..ExecutionMemoryConfig::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "encoded merge",
        NonZeroUsize::new(256).unwrap(),
    );
    let spill = SpillBudgetTracker::with_ledger("encoded merge", &memory, &ledger);
    let items = [SortItem {
        key: SortKey::Column("key".into()),
        direction: SortDirection::Asc,
    }];
    let binding = |key| {
        Binding::values(BTreeMap::from([
            ("key".into(), Value::Int(key)),
            (
                "payload".into(),
                Value::List(vec![Value::Map(BTreeMap::from([(
                    "body".into(),
                    Value::String("x".repeat(64 * 1024)),
                )]))]),
            ),
        ]))
    };
    let mut runs = Vec::new();
    for key in [1, 0] {
        let (run, mut writer) = spill.create_run("wide").unwrap();
        writer.write(key as u64, &binding(key), &spill).unwrap();
        writer.finish().unwrap();
        runs.push(run);
    }
    let tracker =
        OperatorMemoryTracker::with_account(NonZeroUsize::new(256).unwrap(), account.clone());
    let merged = encoded::Merge::new(&items, &spill, None)
        .compact(&[&runs[0], &runs[1]], tracker)
        .unwrap();
    assert!(
        account.peak_bytes() <= 256,
        "wide non-key payload must remain encoded"
    );
    assert_eq!(ledger.snapshot().used_bytes, 0);
    let mut reader = merged.reader().unwrap();
    assert_eq!(reader.read(usize::MAX).unwrap(), Some((0, binding(0))));
    assert_eq!(reader.read(usize::MAX).unwrap(), Some((1, binding(1))));
    assert_eq!(reader.read(usize::MAX).unwrap(), None);
}

#[test]
fn adaptive_merge_fan_in_respects_memory_and_descriptor_capacity() {
    for (query_bytes, descriptor_limit, expected_passes, blocking_bytes, width) in [
        (2 * 1024 * 1024, 32, 2, 1024 * 1024, 64 * 1024),
        (300 * 1024, 32, 3, 1024 * 1024, 64 * 1024),
        (150 * 1024, 32, 5, 1024 * 1024, 64 * 1024),
        (2 * 1024 * 1024, 5, 3, 1024 * 1024, 64 * 1024),
        (2 * 1024 * 1024, 3, 5, 1024 * 1024, 64 * 1024),
        (16 * 1024 * 1024, 32, 2, 128 * 1024, 4096),
    ] {
        let directory = Directory::new();
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(blocking_bytes).unwrap(),
            query_memory_bytes: NonZeroUsize::new(query_bytes).unwrap(),
            min_spill_free_bytes: NonZeroU64::MIN,
            spill_directory: directory.0.join("spill"),
            ..ExecutionMemoryConfig::default()
        };
        let files =
            hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire_with_capacity_limit(
                &directory.0,
                descriptor_limit,
                true,
            )
            .unwrap();
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes)
            .with_file_descriptors(Some(files.io_context()));
        let account = ledger.account(
            QueryMemoryClass::BlockingState,
            "adaptive merge",
            memory.blocking_operator_bytes,
        );
        let mut spill = SpillBudgetTracker::with_ledger("adaptive merge", &memory, &ledger);
        let items = [SortItem {
            key: SortKey::Column("key".into()),
            direction: SortDirection::Asc,
        }];
        let mut runs = Vec::new();
        for key in (0..64).rev() {
            let binding = Binding::values(BTreeMap::from([
                ("key".into(), Value::Int(key)),
                ("payload".into(), Value::String("x".repeat(width))),
            ]));
            runs.push(
                spill_sort_run(
                    &mut vec![SortRunRow {
                        sort_values: vec![(Value::Int(key), SortDirection::Asc)],
                        ordinal: key as u64,
                        binding,
                    }],
                    &mut spill,
                    None,
                )
                .unwrap(),
            );
        }
        let original_bytes = spill.used_bytes();
        let runs = compact_sort_runs(
            runs,
            &items,
            &Catalog::default(),
            &memory,
            &spill,
            &account,
            None,
        )
        .unwrap();
        assert_eq!(
            spill.used_bytes() - original_bytes,
            original_bytes * expected_passes
        );
        assert_eq!(runs.len(), if expected_passes == 5 { 2 } else { 1 });
        assert!(files.metrics().high_water <= descriptor_limit);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert!(ledger.snapshot().peak_bytes <= query_bytes);
        assert!(
            ledger.snapshot().peak_bytes <= blocking_bytes,
            "parallel buffers must stay bounded by the work area, not the query root"
        );
        let mut keys = Vec::new();
        for run in &runs {
            let mut reader = run.reader().unwrap();
            while let Some((_, binding)) = reader.read(usize::MAX).unwrap() {
                keys.push(binding.values["key"].clone());
            }
        }
        keys.sort();
        assert_eq!(keys, (0..64).map(Value::Int).collect::<Vec<_>>());
        drop(runs);
        assert_eq!(memory.spill_pool_snapshot().unwrap().active_bytes, 0);
    }
}

#[test]
fn top_n_keeps_its_cutoff_under_sustained_pressure() {
    #[derive(Debug)]
    struct CriticalPressure;
    impl hawdb_core::RuntimeMemoryTarget for CriticalPressure {
        fn target_bytes(&self, hard: u64) -> u64 {
            hard / 4
        }
    }
    let task =
        RuntimeTaskContext::default().with_memory_target(std::sync::Arc::new(CriticalPressure));
    let binding = |key| {
        Binding::values(BTreeMap::from([
            ("key".into(), Value::Int(key)),
            ("body".into(), Value::String("x".repeat(1024))),
        ]))
    };
    // Exercise both rejected candidates and continual replacement of the cutoff.
    for reverse in [false, true] {
        let directory = Directory::new();
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(128 * 1024).unwrap(),
            query_memory_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            max_spill_runs: NonZeroUsize::MIN,
            max_total_spill_runs: NonZeroUsize::MIN,
            min_spill_free_bytes: NonZeroU64::MIN,
            spill_directory: directory.0.clone(),
            ..ExecutionMemoryConfig::default()
        };
        let ledger = QueryMemoryLedger::for_query(&memory, Some(&task)).unwrap();
        let catalog = Catalog::default();
        let items = [SortItem {
            key: SortKey::Column("key".into()),
            direction: SortDirection::Asc,
        }];
        let mut top_n = TopNOperator::new(
            &items,
            3,
            45,
            BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: Some(&task),
                observer: &NoopExecutionObserver,
                graph: None,
            },
        );
        for index in 0..4096 {
            top_n
                .push(binding(if reverse { 4095 - index } else { index }))
                .unwrap();
            assert!(top_n.runs.is_empty());
            assert!(top_n.heap.len() <= 48);
            assert!(top_n.tracker.used_bytes <= memory.blocking_operator_bytes.get());
        }
        assert!(top_n.tracker.used_bytes > memory.blocking_operator_bytes.get() / 4);
        let mut actual = Vec::new();
        top_n
            .finish(ExecutionLimit::unlimited(), &mut |batch| {
                actual.extend(batch);
                Ok(BatchControl::Continue)
            })
            .unwrap();
        assert_eq!(actual, (3..48).map(binding).collect::<Vec<_>>());
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(memory.spill_pool_snapshot().unwrap().active_runs, 0);
    }
}
