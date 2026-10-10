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
use crate::scoring::{HostScorer, HostScorerBatch, HostScorerDescriptor};
use hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN;
use std::num::NonZeroU64;

#[path = "evidence_bands.rs"]
mod evidence_bands;

#[derive(Default)]
struct CohortScorer {
    calls: usize,
    expected_rows: usize,
    cancel: Option<RuntimeCancellationToken>,
    fail: bool,
}

impl HostScorer for CohortScorer {
    fn descriptor(&self) -> HostScorerDescriptor<'_> {
        HostScorerDescriptor::new("cohort", "v1", NonZeroU64::MIN).unwrap()
    }

    fn score_batch(&mut self, request: HostScorerBatch<'_>, scores: &mut [f64]) -> Result<()> {
        self.calls += 1;
        assert_eq!(request.features.len(), self.expected_rows);
        assert_eq!(request.reference_time_millis, 1234);
        let _scratch = request.scratch_account.reserve(8)?;
        let max = request
            .features
            .iter()
            .map(|row| row.numeric_property("authority").unwrap())
            .fold(0.0f64, f64::max);
        for (features, score) in request.features.iter().zip(scores) {
            request.checkpoint()?;
            *score = features.search_score().unwrap()
                * features.numeric_property("authority").unwrap()
                / max;
        }
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
        if self.fail {
            return Err(HawDBError::Execution("host scoring failed".into()));
        }
        Ok(())
    }
}

fn candidates() -> Vec<Binding> {
    (0..24)
        .map(|ordinal| {
            Binding::values(BTreeMap::from([
                ("score".into(), Value::Float(1.0)),
                ("ordinal".into(), Value::Int(ordinal)),
                (
                    "authority".into(),
                    Value::Float(if ordinal == 23 {
                        100.0
                    } else {
                        (ordinal % 7 + 1) as f64
                    }),
                ),
            ]))
        })
        .collect()
}

fn options(cap: usize, limit: usize) -> HostScoringOptions<'static> {
    HostScoringOptions {
        score_column: "score",
        max_candidate_rows: NonZeroUsize::new(cap).unwrap(),
        reference_time_millis: 1234,
        limit,
        expected_identity: None,
        rank_policy: &hawdb_plan_cypher::HostScoringRankPolicy::ScoreDescending,
    }
}

#[test]
fn host_cohort_freezes_identity_before_candidate_staging() {
    use std::cell::Cell;
    use std::rc::Rc;

    type Identity = (&'static str, &'static str, NonZeroU64);
    struct SharedScorer {
        identity: Rc<Cell<Identity>>,
        calls: usize,
    }
    impl HostScorer for SharedScorer {
        fn descriptor(&self) -> HostScorerDescriptor<'_> {
            let (name, version, cpu) = self.identity.get();
            HostScorerDescriptor::new(name, version, cpu).unwrap()
        }
        fn score_batch(&mut self, _: HostScorerBatch<'_>, scores: &mut [f64]) -> Result<()> {
            self.calls += 1;
            scores.fill(1.0);
            Ok(())
        }
    }
    struct ChangingSource {
        source: Source<'static>,
        identity: Rc<Cell<Identity>>,
        next: Identity,
    }
    impl BindingBatchSource for ChangingSource {
        fn execute(
            &mut self,
            input: &PhysicalPlan,
            limit: ExecutionLimit,
            emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
        ) -> Result<BatchControl> {
            self.identity.set(self.next);
            self.source.execute(input, limit, emit)
        }
    }
    for next in [
        ("changed", "v1", NonZeroU64::MIN),
        ("cohort", "v2", NonZeroU64::MIN),
        ("cohort", "v1", NonZeroU64::new(2).unwrap()),
    ] {
        let identity = Rc::new(Cell::new(("cohort", "v1", NonZeroU64::MIN)));
        let mut scorer = SharedScorer {
            identity: Rc::clone(&identity),
            calls: 0,
        };
        let mut source = ChangingSource {
            source: Source::new(candidates(), 7),
            identity,
            next,
        };
        let mut delivered = 0;
        let error = with_context(2, 131072, |context| {
            stream_host_scoring_batches(
                &PhysicalPlan::EmptyExec,
                options(24, 1),
                &mut scorer,
                &mut source,
                context,
                ExecutionLimit::unlimited(),
                &mut |batch| {
                    delivered += batch.len();
                    Ok(BatchControl::Continue)
                },
            )
            .unwrap_err()
        });
        assert!(error.to_string().contains("identity"), "{error}");
        assert_eq!(scorer.calls, 0);
        assert_eq!(delivered, 0);
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[test]
fn host_cohort_uses_late_maximum_once_and_preserves_stable_top_n_across_batches_and_spill() {
    use crate::observer::ExecutionObserver;
    use crate::BlockingOperatorMemoryReport;
    use std::cell::RefCell;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Reports(RefCell<Vec<BlockingOperatorMemoryReport>>);
    impl ExecutionObserver for Reports {
        fn record_blocking_memory_report(&self, report: BlockingOperatorMemoryReport) {
            self.0.borrow_mut().push(report);
        }
    }
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "hawdb-host-cohort-{}-{}",
        std::process::id(),
        NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let directory = Directory(path);
    let originals = candidates();
    let mut expected = originals.clone();
    for row in &mut expected {
        let Value::Float(authority) = row.values["authority"] else {
            unreachable!()
        };
        row.values.insert(
            SCORING_RERANK_SCORE_COLUMN.into(),
            Value::Float(authority / 100.0),
        );
    }
    expected.sort_by(|left, right| {
        right.values[SCORING_RERANK_SCORE_COLUMN].cmp(&left.values[SCORING_RERANK_SCORE_COLUMN])
    });
    expected.truncate(8);
    assert_eq!(expected[0].values["ordinal"], Value::Int(23));
    let cohort_bytes = originals
        .iter()
        .map(crate::binding::binding_memory_bytes)
        .sum::<usize>()
        + originals.len() * std::mem::size_of::<f64>();
    // The callback simultaneously borrows both concrete features and trait
    // views; 24 rows need 1152 bytes before its identity/output/scratch buffers.
    // Leave room for that real footprint while still forcing TopN spill.
    for (source_rows, top_n_bytes) in [(1, 65536), (7, 1536)] {
        let catalog = Catalog::default();
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(cohort_bytes + top_n_bytes).unwrap(),
            query_memory_bytes: NonZeroUsize::new(131072).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
            batch_rows: NonZeroUsize::new(2).unwrap(),
            min_spill_free_bytes: NonZeroU64::MIN,
            spill_directory: directory.0.clone(),
            ..ExecutionMemoryConfig::default()
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let reports = Reports::default();
        let mut source = Source::new(originals.clone(), source_rows);
        let mut scorer = CohortScorer {
            expected_rows: 24,
            ..CohortScorer::default()
        };
        let mut output = Vec::new();
        stream_host_scoring_batches(
            &PhysicalPlan::EmptyExec,
            options(24, 8),
            &mut scorer,
            &mut source,
            BatchExecutionContext {
                catalog: &catalog,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: None,
                observer: &reports,
            },
            ExecutionLimit {
                output_rows: Some(8),
            },
            &mut |batch| {
                assert!(batch.len() <= memory.batch_rows.get());
                assert!(
                    batch
                        .iter()
                        .map(crate::binding::binding_memory_bytes)
                        .sum::<usize>()
                        <= memory.batch_payload_bytes.get()
                );
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        )
        .unwrap();
        assert_eq!(output, expected);
        assert_eq!(source.requested, vec![ExecutionLimit::unlimited()]);
        assert_eq!(source.calls, 24usize.div_ceil(source_rows));
        assert_eq!(scorer.calls, 1);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert!(ledger.snapshot().peak_bytes <= memory.query_memory_bytes.get());
        let reports = reports.0.borrow();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].operator, "HostScoringExec");
        assert_eq!(reports[0].input_rows, 24);
        assert_eq!(reports[0].budget_bytes, top_n_bytes);
        assert_eq!(reports[0].spill_run_count > 0, top_n_bytes == 1536);
        assert_eq!(memory.spill_pool_snapshot().unwrap().active_runs, 0);
    }
}

#[test]
fn host_cohort_row_memory_and_source_failures_emit_nothing_and_never_call_host() {
    for failure in ["rows", "memory", "source"] {
        let catalog = Catalog::default();
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(if failure == "memory" { 1 } else { 65536 })
                .unwrap(),
            query_memory_bytes: NonZeroUsize::new(131072).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
            batch_rows: NonZeroUsize::new(2).unwrap(),
            ..ExecutionMemoryConfig::default()
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let mut source = Source::new(candidates(), 1);
        source.fail_at = (failure == "source").then_some(1);
        let mut scorer = CohortScorer {
            expected_rows: 24,
            ..CohortScorer::default()
        };
        let mut emitted = 0;
        let result = stream_host_scoring_batches(
            &PhysicalPlan::EmptyExec,
            options(if failure == "rows" { 23 } else { 24 }, 8),
            &mut scorer,
            &mut source,
            BatchExecutionContext {
                catalog: &catalog,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: None,
                observer: &NoopExecutionObserver,
            },
            ExecutionLimit::unlimited(),
            &mut |batch| {
                emitted += batch.len();
                Ok(BatchControl::Continue)
            },
        );
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains(match failure {
                "rows" => "candidate limit",
                "memory" => "budget",
                _ => "source failure",
            }),
            "{message}"
        );
        assert_eq!(scorer.calls, 0);
        assert_eq!(emitted, 0);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        if failure == "rows" {
            let admitted = candidates();
            let maximum_staged_bytes = admitted[..23]
                .iter()
                .map(crate::binding::binding_memory_bytes)
                .sum::<usize>();
            assert!(
                ledger.snapshot().peak_bytes <= maximum_staged_bytes,
                "candidate admission must stop before retaining overflow or building callback features"
            );
        }
    }
}

#[test]
fn host_cohort_callback_error_and_cancellation_release_all_staged_rows_before_delivery() {
    for mode in ["pre-cancel", "callback-cancel", "callback-error"] {
        let catalog = Catalog::default();
        let memory = ExecutionMemoryConfig {
            batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
            ..ExecutionMemoryConfig::default()
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let token = RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(token.clone());
        if mode == "pre-cancel" {
            token.cancel();
        }
        let mut source = Source::new(candidates(), 5);
        let mut scorer = CohortScorer {
            expected_rows: 24,
            cancel: (mode == "callback-cancel").then_some(token),
            fail: mode == "callback-error",
            ..CohortScorer::default()
        };
        let mut emitted = 0;
        let result = stream_host_scoring_batches(
            &PhysicalPlan::EmptyExec,
            options(24, 8),
            &mut scorer,
            &mut source,
            BatchExecutionContext {
                catalog: &catalog,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: Some(&task),
                observer: &NoopExecutionObserver,
            },
            ExecutionLimit::unlimited(),
            &mut |batch| {
                emitted += batch.len();
                Ok(BatchControl::Continue)
            },
        );
        assert!(result.is_err());
        assert_eq!(scorer.calls, usize::from(mode != "pre-cancel"));
        assert_eq!(emitted, 0);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn host_cohort_consumer_stop_and_error_release_the_full_candidate_window() {
    for fail in [false, true] {
        let mut source = Source::new(candidates(), 3);
        let mut scorer = CohortScorer {
            expected_rows: 24,
            ..CohortScorer::default()
        };
        let mut calls = 0;
        let result = with_context(1, 131072, |context| {
            stream_host_scoring_batches(
                &PhysicalPlan::EmptyExec,
                options(24, 8),
                &mut scorer,
                &mut source,
                context,
                ExecutionLimit::unlimited(),
                &mut |batch| {
                    calls += 1;
                    assert_eq!(batch[0].values["ordinal"], Value::Int(23));
                    if fail {
                        Err(HawDBError::Execution("consumer failed".into()))
                    } else {
                        Ok(BatchControl::Stop)
                    }
                },
            )
        });
        if fail {
            assert!(result.unwrap_err().to_string().contains("consumer failed"));
        } else {
            assert_eq!(result.unwrap(), BatchControl::Stop);
        }
        assert_eq!(calls, 1);
        assert_eq!(scorer.calls, 1);
        assert_eq!(source.calls, 8);
    }
}
