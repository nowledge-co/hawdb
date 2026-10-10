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
use hawdb_core::graph_rag::{ScoreFeature, ScoringSpec, ScoringTerm};

fn spec() -> ScoringSpec {
    ScoringSpec {
        terms: vec![ScoringTerm {
            weight: 1.0,
            feature: ScoreFeature::SearchScore,
        }],
        decay: Vec::new(),
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[test]
fn scoring_program_product_uses_fixed_time_and_matches_resident_and_spilled_output() {
    use crate::observer::ExecutionObserver;
    use crate::{BlockingOperatorMemoryReport, QueryMemoryClass};
    use hawdb_core::graph_rag::{
        DecayTerm, MissingScoringFeature, ScoringCombination, ScoringProgram,
    };
    use std::cell::RefCell;
    use std::num::NonZeroU64;

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
    let path = std::env::temp_dir().join(format!(
        "hawdb-scoring-program-spill-{}",
        std::process::id()
    ));
    std::fs::create_dir(&path).unwrap();
    let directory = Directory(path);
    let catalog = Catalog::default();
    let program = ScoringProgram::new(
        ScoringCombination::WeightedProduct,
        MissingScoringFeature::Reject,
        ScoringSpec {
            terms: vec![
                ScoringTerm {
                    feature: ScoreFeature::SearchScore,
                    weight: 2.0,
                },
                ScoringTerm {
                    feature: ScoreFeature::NodeProperty("importance".into()),
                    weight: 1.0,
                },
            ],
            decay: vec![DecayTerm {
                feature: ScoreFeature::TimestampProperty("created".into()),
                half_life: 1.0,
                min_factor: 0.0,
            }],
        },
    )
    .unwrap();
    let originals: Vec<_> = (0..24)
        .map(|ordinal| {
            Binding::values(BTreeMap::from([
                ("ordinal".into(), Value::Int(ordinal)),
                (
                    "score".into(),
                    Value::Float(((ordinal * 5) % 13 + 1) as f64 / 13.0),
                ),
                (
                    "importance".into(),
                    Value::Float(((ordinal * 7) % 17 + 1) as f64 / 17.0),
                ),
                (
                    "created".into(),
                    Value::Int(if ordinal % 2 == 0 { 2_000 } else { 1_000 }),
                ),
            ]))
        })
        .collect();
    let column = hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN;
    let mut expected = originals.clone();
    for row in &mut expected {
        let (Value::Float(seed), Value::Float(importance), Value::Int(created)) = (
            &row.values["score"],
            &row.values["importance"],
            &row.values["created"],
        ) else {
            unreachable!()
        };
        let independent =
            seed.powf(2.0) * importance * 0.5f64.powf((2_000 - created) as f64 / 1_000.0);
        row.values.insert(column.into(), Value::Float(independent));
    }
    expected.sort_by(|left, right| {
        let (Value::Float(left), Value::Float(right)) =
            (&left.values[column], &right.values[column])
        else {
            unreachable!()
        };
        right.partial_cmp(left).unwrap()
    });
    expected.truncate(8);
    for blocking_bytes in [65_536, 1_024] {
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(blocking_bytes).unwrap(),
            query_memory_bytes: NonZeroUsize::new(131_072).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(1_024).unwrap(),
            batch_rows: NonZeroUsize::new(2).unwrap(),
            min_spill_free_bytes: NonZeroU64::MIN,
            spill_directory: directory.0.clone(),
            ..ExecutionMemoryConfig::default()
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let reports = Reports::default();
        let context = BatchExecutionContext {
            catalog: &catalog,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &reports,
        };
        let mut source = Source::new(originals.clone(), 1);
        let mut output = Vec::new();
        stream_scoring_program_batches(
            &PhysicalPlan::EmptyExec,
            "score",
            &program,
            2_000,
            8,
            &mut source,
            context,
            ExecutionLimit::unlimited(),
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
        for (actual, expected) in output.iter().zip(&expected) {
            let (Value::Float(actual), Value::Float(expected)) =
                (&actual.values[column], &expected.values[column])
            else {
                unreachable!()
            };
            assert_eq!(actual.to_bits(), expected.to_bits());
        }
        assert_eq!(source.calls, 24, "evaluate the whole candidate stream");
        assert_eq!(source.requested, vec![ExecutionLimit::unlimited()]);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        let report = reports.0.borrow();
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].operator, "ScoringProgramExec");
        assert_eq!(report[0].input_rows, 24);
        assert_eq!(report[0].spill_run_count > 0, blocking_bytes == 1_024);
        assert_eq!(
            ledger
                .snapshot()
                .classes
                .iter()
                .any(|class| class.class == QueryMemoryClass::SpillStaging && class.peak_bytes > 0),
            blocking_bytes == 1_024
        );
        assert_eq!(memory.spill_pool_snapshot().unwrap().active_runs, 0);
    }
}

fn score_rows() -> Vec<Binding> {
    [1, 4, 4, 2]
        .into_iter()
        .enumerate()
        .map(|(ordinal, score)| {
            Binding::values(BTreeMap::from([
                ("score".into(), Value::Int(score)),
                ("ordinal".into(), Value::Int(ordinal as i64)),
            ]))
        })
        .collect()
}

#[test]
fn scoring_rejects_a_row_that_exceeds_blocking_admission() {
    with_context(4, 32_768, |context| {
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(1).unwrap(),
            ..context.memory.clone()
        };
        let context = BatchExecutionContext {
            memory: &memory,
            ..context
        };
        let mut source = Source::new(score_rows(), 4);
        let mut output = Vec::new();
        let result = stream_scoring_rerank_batches(
            &PhysicalPlan::EmptyExec,
            "score",
            &spec(),
            2,
            &mut source,
            context,
            ExecutionLimit::unlimited(),
            &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        );
        assert!(result.is_err(), "scored retention must obey its memory cap");
        assert!(output.is_empty(), "admission failure must not emit results");
    });
}

#[test]
fn scoring_checks_cancellation_even_when_the_source_does_not() {
    with_context(4, 32_768, |context| {
        let token = RuntimeCancellationToken::new();
        token.cancel();
        let task = RuntimeTaskContext::without_deadline(token);
        let context = BatchExecutionContext {
            task_context: Some(&task),
            ..context
        };
        let mut source = Source::new(score_rows(), 4);
        let mut output = Vec::new();
        let result = stream_scoring_rerank_batches(
            &PhysicalPlan::EmptyExec,
            "score",
            &spec(),
            2,
            &mut source,
            context,
            ExecutionLimit::unlimited(),
            &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        );
        assert!(result.is_err(), "the scorer must own cancellation checks");
        assert!(output.is_empty());
        assert_eq!(source.calls, 0);
    });
}

#[test]
fn scoring_with_zero_results_does_not_execute_candidates() {
    with_context(4, 32_768, |context| {
        for (limit, cap) in [(0, None), (2, Some(0))] {
            let mut source = Source::new(score_rows(), 4);
            stream_scoring_rerank_batches(
                &PhysicalPlan::EmptyExec,
                "score",
                &spec(),
                limit,
                &mut source,
                context,
                ExecutionLimit { output_rows: cap },
                &mut |_| panic!("zero result window must not emit"),
            )
            .unwrap();
            assert_eq!(source.calls, 0);
            assert!(source.requested.is_empty());
        }
    });
}

#[test]
fn scoring_consumes_the_complete_stream_and_preserves_exact_ties() {
    for batch_rows in [1, 2, 4] {
        with_context(4, 32_768, |context| {
            let mut source = Source::new(score_rows(), batch_rows);
            let mut output = Vec::new();
            stream_scoring_rerank_batches(
                &PhysicalPlan::EmptyExec,
                "score",
                &spec(),
                3,
                &mut source,
                context,
                ExecutionLimit {
                    output_rows: Some(2),
                },
                &mut |batch| {
                    output.extend(batch);
                    Ok(BatchControl::Continue)
                },
            )
            .unwrap();
            assert_eq!(source.requested, vec![ExecutionLimit::unlimited()]);
            assert_eq!(
                output
                    .iter()
                    .map(|row| row.values["ordinal"].clone())
                    .collect::<Vec<_>>(),
                vec![Value::Int(1), Value::Int(2)],
            );
            assert!(output.iter().all(|row| row.values
                [hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN]
                == Value::Float(4.0)));
        });
    }
}

#[test]
fn scoring_preserves_signed_zero_ties_and_reported_score_bits() {
    use hawdb_core::graph_rag::DecayTerm;

    let mut scoring = spec();
    scoring.decay.push(DecayTerm {
        feature: ScoreFeature::TimestampProperty("timestamp".into()),
        half_life: 1.0,
        min_factor: 0.0,
    });
    let originals: Vec<_> = [-1.0, 1.0]
        .into_iter()
        .enumerate()
        .map(|(ordinal, score)| {
            Binding::values(BTreeMap::from([
                ("score".into(), Value::Float(score)),
                ("timestamp".into(), Value::Int(0)),
                ("ordinal".into(), Value::Int(ordinal as i64)),
            ]))
        })
        .collect();
    for batch_rows in [1, 2] {
        for limit in [1, 2] {
            with_context(4, 32_768, |context| {
                let mut source = Source::new(originals.clone(), batch_rows);
                let mut output = Vec::new();
                stream_scoring_rerank_batches(
                    &PhysicalPlan::EmptyExec,
                    "score",
                    &scoring,
                    limit,
                    &mut source,
                    context,
                    ExecutionLimit::unlimited(),
                    &mut |batch| {
                        output.extend(batch);
                        Ok(BatchControl::Continue)
                    },
                )
                .unwrap();
                assert_eq!(output.len(), limit);
                for (ordinal, row) in output.iter().enumerate() {
                    assert_eq!(row.values["ordinal"], Value::Int(ordinal as i64));
                    let Value::Float(score) =
                        row.values[hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN]
                    else {
                        panic!("scoring must report the combined float");
                    };
                    let expected: f64 = if ordinal == 0 { -0.0 } else { 0.0 };
                    assert_eq!(score.to_bits(), expected.to_bits());
                }
            });
        }
    }
}

#[test]
fn scoring_uses_combined_signals_before_truncating_candidates() {
    with_context(4, 32_768, |context| {
        let mut source = Source::new(
            [0.9, 0.8, 0.7, 0.01]
                .into_iter()
                .enumerate()
                .map(|(ordinal, raw)| {
                    Binding::values(BTreeMap::from([
                        ("score".into(), Value::Float(raw)),
                        (
                            "pagerank".into(),
                            Value::Float(if ordinal == 3 { 100.0 } else { 0.0 }),
                        ),
                        ("ordinal".into(), Value::Int(ordinal as i64)),
                    ]))
                })
                .collect(),
            1,
        );
        let spec = ScoringSpec {
            terms: vec![
                ScoringTerm {
                    weight: 1.0,
                    feature: ScoreFeature::SearchScore,
                },
                ScoringTerm {
                    weight: 1.0,
                    feature: ScoreFeature::NodeProperty("pagerank".into()),
                },
            ],
            decay: Vec::new(),
        };
        let mut output = Vec::new();
        stream_scoring_rerank_batches(
            &PhysicalPlan::EmptyExec,
            "score",
            &spec,
            1,
            &mut source,
            context,
            ExecutionLimit::unlimited(),
            &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        )
        .unwrap();
        assert_eq!(source.calls, 4);
        assert_eq!(source.requested, vec![ExecutionLimit::unlimited()]);
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].values["ordinal"], Value::Int(3));
        assert_eq!(
            output[0].values[hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN],
            Value::Float(100.01)
        );
    });
}

#[test]
fn scoring_rejects_invalid_specs_and_arithmetic_overflow_without_output() {
    with_context(4, 32_768, |context| {
        for weight in [f64::NAN, -1.0, f64::MAX] {
            let spec = ScoringSpec {
                terms: vec![ScoringTerm {
                    weight,
                    feature: ScoreFeature::SearchScore,
                }],
                decay: Vec::new(),
            };
            let mut source = Source::new(score_rows(), 1);
            let mut output = Vec::new();
            let result = stream_scoring_rerank_batches(
                &PhysicalPlan::EmptyExec,
                "score",
                &spec,
                2,
                &mut source,
                context,
                ExecutionLimit::unlimited(),
                &mut |batch| {
                    output.extend(batch);
                    Ok(BatchControl::Continue)
                },
            );
            assert!(result.is_err());
            assert!(output.is_empty());
            if !weight.is_finite() || weight < 0.0 {
                assert_eq!(
                    source.calls, 0,
                    "invalid specifications must fail before reading"
                );
            }
        }
    });
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[test]
fn scoring_spills_without_changing_ranking_or_leaking_admissions() {
    use crate::observer::ExecutionObserver;
    use crate::{BlockingOperatorMemoryReport, QueryMemoryClass};
    use std::cell::RefCell;
    use std::num::NonZeroU64;

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
    let path = std::env::temp_dir().join(format!("hawdb-scoring-spill-{}", std::process::id()));
    // This test owns a new directory. An existing path is never reused or removed.
    std::fs::create_dir(&path).unwrap();
    let directory = Directory(path);
    let catalog = Catalog::default();
    for signed_zeros in [false, true] {
        let originals: Vec<_> = (0..24)
            .map(|ordinal| {
                let score = if signed_zeros {
                    Value::Float(if ordinal % 2 == 0 { -1.0 } else { 1.0 })
                } else {
                    Value::Int((ordinal * 7) % 11)
                };
                Binding::values(BTreeMap::from([
                    ("score".into(), score),
                    ("ordinal".into(), Value::Int(ordinal)),
                    ("timestamp".into(), Value::Int(0)),
                ]))
            })
            .collect();
        let mut scoring = spec();
        if signed_zeros {
            scoring.decay.push(hawdb_core::graph_rag::DecayTerm {
                feature: ScoreFeature::TimestampProperty("timestamp".into()),
                half_life: 1.0,
                min_factor: 0.0,
            });
        }
        let mut expected = originals.clone();
        if !signed_zeros {
            expected.sort_by_key(|row| std::cmp::Reverse(row.values["score"].clone()));
        }
        expected.truncate(8);
        for row in &mut expected {
            let score = match row.values["score"] {
                Value::Int(score) => score as f64,
                Value::Float(score) => score * 0.0,
                _ => unreachable!(),
            };
            row.values.insert(
                hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN.into(),
                Value::Float(score),
            );
        }
        for blocking_bytes in [65_536, 1024] {
            let memory = ExecutionMemoryConfig {
                blocking_operator_bytes: NonZeroUsize::new(blocking_bytes).unwrap(),
                query_memory_bytes: NonZeroUsize::new(131_072).unwrap(),
                batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
                batch_rows: NonZeroUsize::new(4).unwrap(),
                min_spill_free_bytes: NonZeroU64::MIN,
                spill_directory: directory.0.clone(),
                ..ExecutionMemoryConfig::default()
            };
            let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
            let reports = Reports::default();
            let context = BatchExecutionContext {
                catalog: &catalog,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: None,
                observer: &reports,
            };
            let mut source = Source::new(originals.clone(), 1);
            let mut output = Vec::new();
            stream_scoring_rerank_batches(
                &PhysicalPlan::EmptyExec,
                "score",
                &scoring,
                8,
                &mut source,
                context,
                ExecutionLimit::unlimited(),
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
            for (row, expected_row) in output.iter().zip(&expected) {
                let column = hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN;
                let (Value::Float(score), Value::Float(expected_score)) =
                    (&row.values[column], &expected_row.values[column])
                else {
                    panic!("combined scores must remain floats");
                };
                assert_eq!(score.to_bits(), expected_score.to_bits());
            }
            let snapshot = ledger.snapshot();
            assert_eq!(snapshot.used_bytes, 0);
            assert!(snapshot.peak_bytes <= memory.query_memory_bytes.get());
            let report = reports.0.borrow();
            assert_eq!(report.len(), 1);
            assert_eq!(report[0].operator, "ScoringRerankExec");
            assert_eq!(report[0].input_rows, originals.len());
            assert_eq!(
                report[0].spill_run_count > 0,
                blocking_bytes == 1024,
                "both spill and resident paths must actually execute"
            );
            assert_eq!(
                snapshot
                    .classes
                    .iter()
                    .any(|class| class.class == QueryMemoryClass::SpillStaging
                        && class.peak_bytes > 0),
                blocking_bytes == 1024
            );
            assert_eq!(memory.spill_pool_snapshot().unwrap().active_runs, 0);
        }
    }
}

#[test]
fn scoring_overwrite_uses_final_resident_values_at_the_candidate_cap() {
    use hawdb_core::graph_rag::{MissingScoringFeature, ScoringCombination, ScoringProgram};
    use hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN;
    for previous in [
        Value::List(vec![Value::Null; 64]),
        Value::Map(BTreeMap::from([(
            "nested".into(),
            Value::List(vec![Value::Null; 64]),
        )])),
        Value::String("existing result text".into()),
        Value::Float(3.0),
    ] {
        let candidate = Binding::values(BTreeMap::from([
            ("score".into(), Value::Float(0.25)),
            ("authority".into(), Value::Float(1.0)),
            (SCORING_RERANK_SCORE_COLUMN.into(), previous),
        ]));
        let bytes = crate::binding::binding_memory_bytes(&candidate);
        let memory = ExecutionMemoryConfig {
            query_memory_bytes: NonZeroUsize::new(2 * 1024 * 1024).unwrap(),
            blocking_operator_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(bytes).unwrap(),
            batch_rows: NonZeroUsize::MIN,
            ..Default::default()
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let catalog = Catalog::default();
        let context = BatchExecutionContext {
            catalog: &catalog,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &crate::observer::NoopExecutionObserver,
        };
        for combination in [
            ScoringCombination::WeightedSum,
            ScoringCombination::WeightedProduct,
        ] {
            let mut source = Source::new(vec![candidate.clone()], 1);
            let mut output = Vec::new();
            let program =
                ScoringProgram::new(combination, MissingScoringFeature::Reject, spec()).unwrap();
            stream_scoring_program_batches(
                &PhysicalPlan::EmptyExec,
                "score",
                &program,
                1234,
                1,
                &mut source,
                context,
                ExecutionLimit::unlimited(),
                &mut |batch| {
                    assert!(
                        batch
                            .iter()
                            .map(crate::binding::binding_memory_bytes)
                            .sum::<usize>()
                            <= bytes
                    );
                    output.extend(batch);
                    Ok(BatchControl::Continue)
                },
            )
            .unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(
                output[0].values[SCORING_RERANK_SCORE_COLUMN],
                Value::Float(0.25)
            );
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
}
