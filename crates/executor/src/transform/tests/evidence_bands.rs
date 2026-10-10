// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use hawdb_plan_cypher::HostScoringRankPolicy;

#[derive(Default)]
struct SecondaryScorer {
    calls: usize,
    rows: usize,
    cancel: Option<RuntimeCancellationToken>,
}

impl HostScorer for SecondaryScorer {
    fn descriptor(&self) -> HostScorerDescriptor<'_> {
        HostScorerDescriptor::new("secondary", "v1", NonZeroU64::MIN).unwrap()
    }

    fn score_batch(&mut self, batch: HostScorerBatch<'_>, scores: &mut [f64]) -> Result<()> {
        self.calls += 1;
        self.rows = batch.features.len();
        for (row, score) in batch.features.iter().zip(scores) {
            batch.checkpoint()?;
            *score = row.numeric_property("secondary").unwrap();
        }
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
        Ok(())
    }
}

fn cohort() -> Vec<Binding> {
    [
        ("a", 2.0, "direct", 1.0),
        ("b", 2.0, "direct", 3.0),
        ("c", 1.0, "direct", 900.0),
        ("d", 1.0, "other", 999.0),
        ("e", 2.0, "direct", 1000.0),
        ("f", 0.0, "zero", -0.0),
        ("g", 0.0, "zero", 0.0),
        ("h", -0.0, "zero", 999.0),
        ("i", 0.0, "stable", 5.0),
        ("j", 0.0, "stable", 5.0),
    ]
    .into_iter()
    .map(|(id, score, reason, secondary)| {
        Binding::values(BTreeMap::from([
            ("id".into(), Value::String(id.into())),
            ("score".into(), Value::Float(score)),
            ("reason".into(), Value::String(reason.into())),
            ("secondary".into(), Value::Float(secondary)),
        ]))
    })
    .collect()
}

fn policy() -> HostScoringRankPolicy {
    HostScoringRankPolicy::ContiguousEvidenceTies {
        reason_column: "reason".into(),
    }
}

#[test]
fn evidence_bands_validate_declared_columns_before_zero_window() {
    for reason in ["", "score", SCORING_RERANK_SCORE_COLUMN, "\0private"] {
        let policy = HostScoringRankPolicy::ContiguousEvidenceTies {
            reason_column: reason.into(),
        };
        let mut source = Source::new(cohort(), 3);
        let mut scorer = SecondaryScorer::default();
        let result = with_context(2, 131072, |context| {
            stream_host_scoring_batches(
                &PhysicalPlan::EmptyExec,
                HostScoringOptions {
                    rank_policy: &policy,
                    ..options(10, 0)
                },
                &mut scorer,
                &mut source,
                context,
                ExecutionLimit::unlimited(),
                &mut |_| panic!("invalid policy reached consumer"),
            )
        });
        assert!(matches!(result, Err(HawDBError::Semantic(_))));
        assert_eq!(source.calls, 0);
        assert_eq!(scorer.calls, 0);
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[test]
fn evidence_bands_keep_exact_order_and_private_metadata_through_top_n_spill() {
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
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let directory = Directory(std::env::temp_dir().join(format!(
        "hawdb-evidence-bands-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    std::fs::create_dir(&directory.0).unwrap();
    let rows = cohort();
    let cohort_bytes = rows
        .iter()
        .map(crate::binding::binding_memory_bytes)
        .sum::<usize>()
        + rows.len() * 16;
    let catalog = Catalog::default();
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(cohort_bytes + 1024 + 1536).unwrap(),
        query_memory_bytes: NonZeroUsize::new(131072).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(4096).unwrap(),
        batch_rows: NonZeroUsize::new(2).unwrap(),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_directory: directory.0.clone(),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let reports = Reports::default();
    let mut source = Source::new(rows, 3);
    let mut scorer = SecondaryScorer::default();
    let policy = policy();
    let mut output = Vec::new();
    stream_host_scoring_batches(
        &PhysicalPlan::EmptyExec,
        HostScoringOptions {
            rank_policy: &policy,
            ..options(10, 8)
        },
        &mut scorer,
        &mut source,
        BatchExecutionContext {
            catalog: &catalog,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &reports,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            output.extend(batch);
            Ok(BatchControl::Continue)
        },
    )
    .unwrap();
    assert_eq!(
        output
            .iter()
            .map(|row| row.values["id"].clone())
            .collect::<Vec<_>>(),
        ["b", "a", "c", "d", "e", "g", "f", "h"].map(|id| Value::String(id.into()))
    );
    assert!(output
        .iter()
        .all(|row| row.values.keys().all(|key| !key.contains('\0'))));
    assert_eq!(scorer.rows, 10);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(reports
        .0
        .borrow()
        .iter()
        .any(|report| report.spill_run_count > 0));
    assert_eq!(memory.spill_pool_snapshot().unwrap().active_runs, 0);
}

#[test]
fn evidence_bands_preserve_direct_order_exact_bits_and_stable_ties_before_top_n() {
    let original = cohort();
    let expected = ["b", "a", "c", "d", "e", "g", "f", "h", "i", "j"];
    let policy = policy();
    for source_rows in [1, 3, 10] {
        for limit in [1, 4, 10] {
            let mut source = Source::new(original.clone(), source_rows);
            let mut scorer = SecondaryScorer::default();
            let mut output = Vec::new();
            with_context(2, 131072, |context| {
                stream_host_scoring_batches(
                    &PhysicalPlan::EmptyExec,
                    HostScoringOptions {
                        rank_policy: &policy,
                        ..options(10, limit)
                    },
                    &mut scorer,
                    &mut source,
                    context,
                    ExecutionLimit::unlimited(),
                    &mut |batch| {
                        output.extend(batch);
                        Ok(BatchControl::Continue)
                    },
                )
                .unwrap()
            });
            let ids: Vec<_> = output.iter().map(|row| row.values["id"].clone()).collect();
            assert_eq!(
                ids,
                expected[..limit]
                    .iter()
                    .map(|id| Value::String((*id).into()))
                    .collect::<Vec<_>>()
            );
            for row in output {
                let original = original
                    .iter()
                    .find(|input| input.values["id"] == row.values["id"])
                    .unwrap();
                assert_eq!(row.values["score"], original.values["score"]);
                assert_eq!(row.values["reason"], original.values["reason"]);
                assert_eq!(
                    row.values[SCORING_RERANK_SCORE_COLUMN],
                    original.values["secondary"]
                );
                assert!(row.values.keys().all(|key| !key.contains('\0')));
            }
            assert_eq!(scorer.calls, 1);
            assert_eq!(scorer.rows, 10);
            assert_eq!(source.requested, vec![ExecutionLimit::unlimited()]);
        }
    }
}

#[test]
fn evidence_bands_reject_invalid_late_metadata_before_host_or_output() {
    let policy = policy();
    for invalid in ["missing", "reason", "nan", "infinity", "reserved"] {
        let mut rows = cohort();
        let last = rows.last_mut().unwrap();
        match invalid {
            "missing" => {
                last.values.remove("reason");
            }
            "reason" => {
                last.values.insert("reason".into(), Value::Null);
            }
            "nan" => {
                last.values.insert("score".into(), Value::Float(f64::NAN));
            }
            "infinity" => {
                last.values
                    .insert("score".into(), Value::Float(f64::INFINITY));
            }
            "reserved" => {
                last.values
                    .insert("\0hawdb.scoring.evidence_band".into(), Value::Int(0));
            }
            _ => unreachable!(),
        }
        let mut source = Source::new(rows, 3);
        let mut scorer = SecondaryScorer::default();
        let mut delivered = 0;
        let error = with_context(2, 131072, |context| {
            stream_host_scoring_batches(
                &PhysicalPlan::EmptyExec,
                HostScoringOptions {
                    rank_policy: &policy,
                    ..options(10, 1)
                },
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
        assert!(matches!(error, HawDBError::Execution(_)));
        assert_eq!(scorer.calls, 0);
        assert_eq!(delivered, 0);
    }
}

#[test]
fn evidence_bands_release_cohort_scores_and_band_ownership_on_stop_error_and_cancel() {
    let policy = policy();
    for terminal in ["stop", "error", "cancel"] {
        let catalog = Catalog::default();
        let memory = ExecutionMemoryConfig {
            batch_rows: NonZeroUsize::MIN,
            ..Default::default()
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let token = RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(token.clone());
        let mut source = Source::new(cohort(), 3);
        let mut scorer = SecondaryScorer {
            cancel: (terminal == "cancel").then_some(token),
            ..Default::default()
        };
        let mut delivered = 0;
        let result = stream_host_scoring_batches(
            &PhysicalPlan::EmptyExec,
            HostScoringOptions {
                rank_policy: &policy,
                ..options(10, 8)
            },
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
                delivered += batch.len();
                assert_eq!(batch[0].values["id"], Value::String("b".into()));
                assert!(batch[0].values.keys().all(|key| !key.contains('\0')));
                if terminal == "error" {
                    Err(HawDBError::Execution("consumer failed".into()))
                } else {
                    Ok(BatchControl::Stop)
                }
            },
        );
        if terminal == "stop" {
            assert_eq!(result.unwrap(), BatchControl::Stop);
        } else {
            assert!(result.is_err());
        }
        assert_eq!(scorer.calls, 1);
        assert_eq!(scorer.rows, 10);
        assert_eq!(delivered, usize::from(terminal != "cancel"));
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
