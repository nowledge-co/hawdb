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
use crate::observer::ExecutionObserver;
use std::cell::RefCell;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

struct Rows(Vec<Binding>);

impl BindingBatchSource<()> for Rows {
    fn execute(
        &mut self,
        _: &(),
        _: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        emit(std::mem::take(&mut self.0))
    }
}

#[derive(Default)]
struct Reports(RefCell<Vec<BlockingOperatorMemoryReport>>);

impl ExecutionObserver for Reports {
    fn record_blocking_memory_report(&self, report: BlockingOperatorMemoryReport) {
        self.0.borrow_mut().push(report);
    }
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        loop {
            let path = std::env::temp_dir().join(format!(
                "hawdb-distinct-output-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, AtomicOrdering::Relaxed)
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create distinct fixture: {error}"),
            }
        }
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn binding(value: i64) -> Binding {
    Binding::scalar("value", Value::Int(value))
}

fn memory(directory: &Directory, spilled: bool) -> ExecutionMemoryConfig {
    ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(if spilled { 1024 } else { 4096 }).unwrap(),
        query_memory_bytes: NonZeroUsize::new(8192).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(200).unwrap(),
        batch_rows: NonZeroUsize::new(4).unwrap(),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_directory: directory.0.clone(),
        ..ExecutionMemoryConfig::default()
    }
}

fn assert_output_payload_is_bounded(spilled: bool) {
    let directory = Directory::new();
    let memory = memory(&directory, spilled);
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let reports = Reports::default();
    let catalog = Catalog::default();
    let expected = (0..4).map(binding).collect::<Vec<_>>();
    let mut source = Rows([expected.clone(), expected.clone()].concat());
    let mut actual = Vec::new();
    stream_distinct_batches(
        &(),
        &mut source,
        BlockingExecutionContext {
            catalog: &catalog,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &reports,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            let bytes = batch.iter().map(binding_memory_bytes).sum::<usize>();
            assert!(
                bytes <= memory.batch_payload_bytes.get(),
                "batch uses {bytes} bytes"
            );
            assert!(batch.len() <= memory.batch_rows.get());
            actual.extend(batch);
            Ok(BatchControl::Continue)
        },
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(reports.0.borrow()[0].spill_run_count > 0, spilled);
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert!(snapshot
        .classes
        .iter()
        .any(|class| { class.class == QueryMemoryClass::PipelineBatch && class.peak_bytes > 0 }));
}

#[test]
fn resident_output_obeys_the_transfer_payload_budget() {
    assert_output_payload_is_bounded(false);
}

#[test]
fn spilled_output_obeys_the_transfer_payload_budget() {
    assert_output_payload_is_bounded(true);
}

#[test]
fn output_preserves_mixed_schema_order_and_batches_variable_widths() {
    let expected = vec![
        Binding::scalar("x", Value::Int(3)),
        Binding::scalar("y", Value::Null),
        Binding::scalar("x", Value::String("h".repeat(8))),
        Binding::scalar("x", Value::Int(1)),
        Binding::scalar("y", Value::String("h".repeat(90))),
    ];
    let mut schemas = DistinctSchemaInterner::default();
    for binding in &expected {
        if schemas.find(binding).is_none() {
            schemas.insert(
                binding,
                DistinctSchemaInterner::next_schema_memory_bytes(binding),
            );
        }
    }
    for spilled in [false, true] {
        let directory = Directory::new();
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(if spilled { 2048 } else { 8192 }).unwrap(),
            batch_rows: NonZeroUsize::new(3).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(300).unwrap(),
            ..memory(&directory, spilled)
        };
        let catalog = Catalog::default();
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let reports = Reports::default();
        let mut source = Rows([expected.clone(), expected.clone()].concat());
        let mut actual = Vec::new();
        stream_distinct_batches(
            &(),
            &mut source,
            BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: None,
                observer: &reports,
            },
            ExecutionLimit::unlimited(),
            &mut |batch| {
                assert!(batch.len() <= 3);
                assert!(batch.iter().map(binding_memory_bytes).sum::<usize>() <= 300);
                actual.extend(batch);
                Ok(BatchControl::Continue)
            },
        )
        .unwrap();
        let mut expected = expected.clone();
        if spilled {
            expected.sort_by_key(|binding| {
                distinct_binding_key(binding, schemas.find(binding).unwrap())
            });
        }
        assert_eq!(actual, expected);
        assert_eq!(reports.0.borrow()[0].spill_run_count > 0, spilled);
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.used_bytes, 0);
        assert!(snapshot.peak_bytes <= memory.query_memory_bytes.get());
        assert!(snapshot.classes.iter().any(|class| {
            class.class == QueryMemoryClass::PipelineBatch
                && class.peak_bytes > 0
                && class.peak_bytes <= 300
        }));
        assert_runs_released(&directory);
    }
}

#[test]
fn capped_resident_output_releases_discarded_state_before_parent_admission() {
    assert_capped_output_releases_state(false);
}

#[test]
fn capped_spilled_output_releases_discarded_state_before_parent_admission() {
    assert_capped_output_releases_state(true);
}

fn assert_capped_output_releases_state(spilled: bool) {
    for batch_rows in [1, 2, 4] {
        for cap in [0, 1, 2, 4] {
            let directory = Directory::new();
            let memory = ExecutionMemoryConfig {
                batch_rows: NonZeroUsize::new(batch_rows).unwrap(),
                batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
                ..memory(&directory, spilled)
            };
            let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
            let parent = ledger.account(
                QueryMemoryClass::PipelineBatch,
                "parent output",
                memory.query_memory_bytes,
            );
            let catalog = Catalog::default();
            let reports = Reports::default();
            let mut source = Rows((0..4).map(binding).collect());
            let mut count = 0;
            stream_distinct_batches(
                &(),
                &mut source,
                BlockingExecutionContext {
                    catalog: &catalog,
                    memory: &memory,
                    memory_ledger: &ledger,
                    task_context: None,
                    observer: &reports,
                },
                ExecutionLimit {
                    output_rows: Some(cap),
                },
                &mut |batch| {
                    count += batch.len();
                    if count == cap {
                        assert_eq!(ledger.snapshot().used_bytes, 0);
                        let reservation = parent.reserve(memory.query_memory_bytes.get() - 1)?;
                        drop(reservation);
                    }
                    Ok(BatchControl::Continue)
                },
            )
            .unwrap();
            assert_eq!(count, cap);
            assert_eq!(ledger.snapshot().used_bytes, 0);
            assert_eq!(reports.0.borrow()[0].spill_run_count > 0, spilled);
            assert_runs_released(&directory);
        }
    }
}

fn assert_runs_released(directory: &Directory) {
    assert!(std::fs::read_dir(&directory.0).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".spill")
    }));
}

#[test]
fn empty_distinct_does_not_require_output_admission() {
    let directory = Directory::new();
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::MIN,
        ..memory(&directory, false)
    };
    let catalog = Catalog::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let reports = Reports::default();
    let mut source = Rows(Vec::new());
    let control = stream_distinct_batches(
        &(),
        &mut source,
        BlockingExecutionContext {
            catalog: &catalog,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &reports,
        },
        ExecutionLimit::unlimited(),
        &mut |_| panic!("empty distinct must not emit"),
    )
    .unwrap();
    assert_eq!(control, BatchControl::Continue);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(ledger.snapshot().peak_bytes, 0);
}

#[test]
fn distinct_output_releases_state_and_runs_on_stop_error_and_cancellation() {
    for spilled in [false, true] {
        for exit in 0..3 {
            let directory = Directory::new();
            let memory = memory(&directory, spilled);
            let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
            let catalog = Catalog::default();
            let reports = Reports::default();
            let cancellation = hawdb_core::RuntimeCancellationToken::new();
            let task = RuntimeTaskContext::without_deadline(cancellation.clone());
            let mut source = Rows((0..4).map(binding).collect());
            let mut calls = 0;
            let result = stream_distinct_batches(
                &(),
                &mut source,
                BlockingExecutionContext {
                    catalog: &catalog,
                    memory: &memory,
                    memory_ledger: &ledger,
                    task_context: Some(&task),
                    observer: &reports,
                },
                ExecutionLimit::unlimited(),
                &mut |batch| {
                    calls += 1;
                    assert_eq!(batch.len(), 1);
                    match exit {
                        0 => Ok(BatchControl::Stop),
                        1 => Err(HawDBError::Execution("distinct consumer failure".into())),
                        _ => {
                            cancellation.cancel();
                            Ok(BatchControl::Continue)
                        }
                    }
                },
            );
            match exit {
                0 => assert_eq!(result.unwrap(), BatchControl::Stop),
                1 => assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("distinct consumer failure")),
                _ => assert!(result.unwrap_err().to_string().contains("cancelled")),
            }
            assert_eq!(calls, 1);
            assert_eq!(ledger.snapshot().used_bytes, 0);
            assert_eq!(reports.0.borrow()[0].spill_run_count > 0, spilled);
            assert_runs_released(&directory);
        }
    }
}

#[test]
fn distinct_rejects_an_oversized_transfer_row_and_releases_all_state() {
    for spilled in [false, true] {
        let directory = Directory::new();
        let memory = memory(&directory, spilled);
        let catalog = Catalog::default();
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let reports = Reports::default();
        let mut source = Rows(
            (0..if spilled { 4 } else { 1 })
                .map(|index| {
                    Binding::scalar(
                        "value",
                        Value::String(format!("{index}{}", "x".repeat(128))),
                    )
                })
                .collect(),
        );
        let error = stream_distinct_batches(
            &(),
            &mut source,
            BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: None,
                observer: &reports,
            },
            ExecutionLimit::unlimited(),
            &mut |_| panic!("oversized output must not emit"),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("batch_payload_bytes 200"),
            "{error}"
        );
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(reports.0.borrow()[0].spill_run_count > 0, spilled);
        assert_runs_released(&directory);
    }
}
