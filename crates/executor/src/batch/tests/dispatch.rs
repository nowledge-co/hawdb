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

use super::fixtures;
use super::store::ReadFixture;
use crate::batch::*;
use crate::external::NoExternalReadOperator;
use hawdb_plan::{ComparisonOp, PhysicalPlanKind, ProjectionExpression, SortDirection, SortKey};
use std::collections::BTreeSet;

fn wrap(plan: PhysicalPlan, shape: usize) -> PhysicalPlan {
    match shape {
        0 => plan,
        1 => PhysicalPlan::FilterExec {
            predicate: Predicate::ConstantBool(true),
            input: Box::new(plan),
        },
        2 => PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(plan),
            right: Box::new(PhysicalPlan::EmptyExec),
        },
        3 => PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(PhysicalPlan::EmptyExec),
            right: Box::new(plan),
        },
        _ => panic!("unknown fixture shape"),
    }
}

#[test]
fn admission_covers_every_physical_operator_and_descendant_position() {
    let fixtures = fixtures::operators();
    let kinds: BTreeSet<_> = fixtures.iter().map(|(plan, _)| plan.kind()).collect();
    assert_eq!(kinds.len(), fixtures.len(), "duplicate operator fixture");
    assert_eq!(kinds, PhysicalPlanKind::all().iter().copied().collect());
    assert_eq!(
        fixtures.iter().filter(|(_, supported)| *supported).count(),
        32
    );
    for (plan, expected) in fixtures {
        for shape in 0..4 {
            let nested = wrap(wrap(plan.clone(), shape), 1);
            assert_eq!(
                BatchPlanRef::try_new(&nested).is_some(),
                expected,
                "operator {:?}, shape {shape}",
                plan.kind(),
            );
            assert_eq!(BatchPlanRef::try_new(&plan).is_some(), expected);
        }
    }
}

fn with_context<T>(
    values: &[i64],
    batch_rows: usize,
    payload_bytes: usize,
    task_context: Option<&RuntimeTaskContext>,
    run: impl FnOnce(BatchReadContext<'_>) -> T,
) -> T {
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Item");
    let store = ReadFixture {
        nodes: values
            .iter()
            .enumerate()
            .map(|(id, value)| NodeRecord {
                id: NodeId(id as u64),
                labels: [label].into_iter().collect(),
                properties: BTreeMap::from([("score".to_string(), Value::Int(*value))]),
            })
            .collect(),
        ..ReadFixture::default()
    };
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(batch_rows).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(payload_bytes).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let memory_ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let parameters = BTreeMap::new();
    let mut external_operator = NoExternalReadOperator;
    let external = BatchExternalReadAdapter::new(&mut external_operator);
    let observer = QueryExecutionObserver::default();
    let output = run(BatchReadContext {
        catalog: &catalog,
        store: &store,
        parameters: &parameters,
        external: &external,
        memory: &memory,
        memory_ledger: &memory_ledger,
        task_context,
        observer: &observer,
    });
    assert_eq!(memory_ledger.snapshot().used_bytes, 0);
    assert!(memory_ledger.snapshot().peak_bytes <= memory.query_memory_bytes.get());
    output
}

#[test]
fn graph_hash_join_executes_residual_before_ordered_offset_limit() {
    let scan = |variable: &str| {
        Box::new(PhysicalPlan::SeqNodeScan {
            variable: variable.into(),
            label: "Item".into(),
        })
    };
    let key = |variable: &str| hawdb_plan::HashJoinKey {
        variable: variable.into(),
        property: "score".into(),
    };
    let plan = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            name: "score".into(),
            expression: ProjectionExpression::Property {
                variable: "a".into(),
                property: "score".into(),
            },
        }],
        input: Box::new(PhysicalPlan::TopNExec {
            items: vec![SortItem {
                key: SortKey::Property {
                    variable: "a".into(),
                    property: "score".into(),
                },
                direction: SortDirection::Desc,
            }],
            offset: 1,
            limit: 3,
            input: Box::new(PhysicalPlan::FilterExec {
                predicate: Predicate::PropertyNotEq {
                    variable: "a".into(),
                    property: "score".into(),
                    value: Value::Int(9),
                },
                input: Box::new(PhysicalPlan::HashJoinExec {
                    left_key: key("a"),
                    right_key: key("b"),
                    left: scan("a"),
                    right: scan("b"),
                }),
            }),
        }),
    };
    for batch_rows in [1, 3, 16] {
        let mut output = Vec::new();
        with_context(
            &[7, 1, 9, 3, 5, 2, 8],
            batch_rows,
            16 * 1024,
            None,
            |context| {
                execute_binding_batches(
                    &plan,
                    context,
                    ExecutionLimit::unlimited(),
                    &mut |batch| {
                        output.extend(
                            batch
                                .into_iter()
                                .map(|binding| binding.values["score"].clone()),
                        );
                        Ok(BatchControl::Continue)
                    },
                )
                .unwrap();
            },
        );
        assert_eq!(output, vec![Value::Int(7), Value::Int(5), Value::Int(3)]);
    }
}

#[test]
fn unsupported_trees_fail_at_batch_entry_before_any_output_or_storage_mutation() {
    with_context(&[7, 11], 2, 64 * 1024, None, |context| {
        let records = || {
            let mut records = Vec::new();
            context
                .store
                .visit_nodes_owned(None, &mut |node| {
                    records.push(node);
                    Ok(ScanControl::Continue)
                })
                .unwrap();
            records
        };
        let before = records();
        for (plan, supported) in fixtures::operators() {
            if supported {
                continue;
            }
            for shape in 0..4 {
                let plan = wrap(plan.clone(), shape);
                for output_rows in [None, Some(0), Some(1)] {
                    let error = execute_binding_batches(
                        &plan,
                        context,
                        ExecutionLimit { output_rows },
                        &mut |_| panic!("unsupported plan emitted a batch"),
                    )
                    .unwrap_err();
                    assert_eq!(
                        error.to_string(),
                        format!(
                            "execution error: physical operator '{}' does not support batch execution",
                            plan.kind().as_str(),
                        ),
                    );
                    assert_eq!(records(), before);
                    assert_eq!(context.memory_ledger.snapshot().used_bytes, 0);
                }
            }
        }
    });
}

#[test]
#[ignore = "local batch admission and dispatch benchmark"]
fn batch_dispatch_benchmark() {
    const ITERATIONS: usize = 20_000;
    for depth in [1, 8, 32] {
        let mut plan = PhysicalPlan::EmptyExec;
        for _ in 0..depth {
            plan = wrap(plan, 1);
        }
        let start = std::time::Instant::now();
        for _ in 0..ITERATIONS {
            assert!(
                std::hint::black_box(BatchPlanRef::try_new(std::hint::black_box(&plan))).is_some()
            );
        }
        println!(
            "batch_admission depth={depth} iterations={ITERATIONS} elapsed_ns={}",
            start.elapsed().as_nanos()
        );
    }
    let plan = read_plan(0, 0, 0, 8);
    with_context(&[1, 2, 3, 4, 5, 6, 7, 8], 4, 64 * 1024, None, |context| {
        let start = std::time::Instant::now();
        let mut rows = 0;
        for _ in 0..ITERATIONS {
            execute_binding_batches(
                std::hint::black_box(&plan),
                context,
                ExecutionLimit::unlimited(),
                &mut |batch| {
                    rows += batch.len();
                    std::hint::black_box(batch);
                    Ok(BatchControl::Continue)
                },
            )
            .unwrap();
        }
        assert_eq!(rows, ITERATIONS * 8);
        println!(
            "batch_execution iterations={ITERATIONS} rows={rows} elapsed_ns={}",
            start.elapsed().as_nanos()
        );
    });
}

fn read_plan(shape: usize, threshold: i64, offset: usize, limit: usize) -> PhysicalPlan {
    let scan = PhysicalPlan::SeqNodeScan {
        variable: "n".to_string(),
        label: "Item".to_string(),
    };
    let mut input = PhysicalPlan::FilterExec {
        predicate: Predicate::PropertyCompare {
            variable: "n".to_string(),
            property: "score".to_string(),
            op: ComparisonOp::Gte,
            value: Value::Int(threshold),
        },
        input: Box::new(scan),
    };
    let items = vec![SortItem {
        key: SortKey::Property {
            variable: "n".to_string(),
            property: "score".to_string(),
        },
        direction: SortDirection::Desc,
    }];
    match shape {
        0 => {}
        1 => {
            input = PhysicalPlan::SortExec {
                items,
                input: Box::new(input),
            }
        }
        2 => {
            input = PhysicalPlan::TopNExec {
                items,
                offset,
                limit,
                input: Box::new(input),
            }
        }
        _ => panic!("unknown read shape"),
    }
    let projected = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            name: "score".to_string(),
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "score".to_string(),
            },
        }],
        input: Box::new(input),
    };
    if shape == 2 {
        projected
    } else {
        PhysicalPlan::LimitExec {
            offset,
            limit: Some(limit),
            input: Box::new(projected),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Exit {
    Complete,
    Stop,
    Error,
}

fn check_read_case(seed: usize, batch_rows: usize, shape: usize, exit: Exit) {
    let values: Vec<_> = (0..seed % 17)
        .map(|index| ((index * 7 + seed) % 13) as i64 - 6)
        .collect();
    let threshold = (seed % 9) as i64 - 4;
    let offset = seed % 4;
    let limit = seed % 11;
    let output_limit = seed % 7;
    let output_rows = Some(output_limit);
    let mut expected: Vec<_> = values
        .iter()
        .copied()
        .filter(|&value| value >= threshold)
        .collect();
    if shape != 0 {
        expected.sort_by(|a, b| b.cmp(a));
    }
    let expected: Vec<_> = expected
        .into_iter()
        .skip(offset)
        .take(limit)
        .take(output_limit)
        .collect();
    let plan = read_plan(shape, threshold, offset, limit);
    assert!(BatchPlanRef::try_new(&plan).is_some());
    let mut actual = Vec::new();
    let mut calls = 0;
    let result = with_context(&values, batch_rows, 64 * 1024, None, |context| {
        execute_binding_batches(
            &plan,
            context,
            ExecutionLimit { output_rows },
            &mut |batch| {
                calls += 1;
                assert!(!batch.is_empty());
                assert!(batch.len() <= batch_rows);
                for binding in batch {
                    let Value::Int(value) = binding.values["score"] else {
                        panic!("non-integer score")
                    };
                    actual.push(value);
                }
                match exit {
                    Exit::Complete => Ok(BatchControl::Continue),
                    Exit::Stop => Ok(BatchControl::Stop),
                    Exit::Error => Err(HawDBError::Execution(
                        "dispatch consumer failure".to_string(),
                    )),
                }
            },
        )
    });
    match exit {
        Exit::Complete => {
            result.unwrap();
            assert_eq!(
                actual, expected,
                "seed {seed}, batch {batch_rows}, shape {shape}"
            );
        }
        Exit::Stop | Exit::Error => {
            assert!(calls <= 1, "consumer called again after {exit:?}");
            assert_eq!(
                actual,
                expected[..actual.len()],
                "seed {seed}, shape {shape}"
            );
            if calls == 1 && matches!(exit, Exit::Error) {
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("dispatch consumer failure"));
            } else {
                result.unwrap();
                assert!(expected.is_empty() || !actual.is_empty());
            }
        }
    }
}

#[test]
fn batch_dispatch_preserves_rows_limits_and_consumer_control() {
    for seed in [0, 1, 6, 9, 12, 16, 23, 31] {
        for batch_rows in [1, 3, 8] {
            for shape in 0..3 {
                for exit in [Exit::Complete, Exit::Stop, Exit::Error] {
                    check_read_case(seed, batch_rows, shape, exit);
                }
            }
        }
    }
}

#[test]
fn batch_dispatch_preserves_cancellation_and_byte_budget_errors() {
    let plan = read_plan(0, 0, 0, 3);
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    assert!(cancellation.cancel());
    let task = RuntimeTaskContext::without_deadline(cancellation);
    let error = with_context(&[1, 2, 3], 2, 64 * 1024, Some(&task), |context| {
        execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |_| {
            panic!("cancelled output")
        })
    })
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("runtime task stopped: cancelled"));
    let error = with_context(&[1, 2, 3], 2, 1, None, |context| {
        execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |_| {
            panic!("over-budget output")
        })
    })
    .unwrap_err();
    assert!(error.to_string().contains("batch_payload_bytes"), "{error}");
}

#[test]
fn generic_transform_adapters_preserve_graph_bindings_and_consumer_control() {
    // The intermediate projection prevents scan/columnar filter fusion, so
    // every transform must cross the recursive owner-kernel adapter.
    let input = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            name: "score".into(),
            expression: ProjectionExpression::Property {
                variable: "n".into(),
                property: "score".into(),
            },
        }],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".into(),
            label: "Item".into(),
        }),
    };
    let input = PhysicalPlan::FilterExec {
        predicate: Predicate::PropertyCompare {
            variable: "n".into(),
            property: "score".into(),
            op: ComparisonOp::Gte,
            value: Value::Int(0),
        },
        input: Box::new(input),
    };
    let plan = PhysicalPlan::LimitExec {
        offset: 1,
        limit: Some(2),
        input: Box::new(PhysicalPlan::ProjectExec {
            items: vec![Projection {
                name: "result".into(),
                expression: ProjectionExpression::Column("score".into()),
            }],
            input: Box::new(input),
        }),
    };
    for batch_rows in [1, 3, 8] {
        for output_rows in [None, Some(0), Some(1), Some(8)] {
            for exit in [Exit::Complete, Exit::Stop, Exit::Error] {
                let expected: Vec<_> = [Value::Int(2), Value::Int(3)]
                    .into_iter()
                    .take(output_rows.unwrap_or(usize::MAX))
                    .collect();
                let mut actual = Vec::new();
                let mut calls = 0;
                let result = with_context(
                    &[-1, 0, -2, 2, 3, 4],
                    batch_rows,
                    64 * 1024,
                    None,
                    |context| {
                        execute_binding_batches(
                            &plan,
                            context,
                            ExecutionLimit { output_rows },
                            &mut |batch| {
                                calls += 1;
                                for binding in batch {
                                    assert!(binding.nodes.contains_key("n"));
                                    assert_eq!(binding.values.len(), 1);
                                    actual.push(binding.values["result"].clone());
                                }
                                match exit {
                                    Exit::Complete => Ok(BatchControl::Continue),
                                    Exit::Stop => Ok(BatchControl::Stop),
                                    Exit::Error => Err(HawDBError::Execution(
                                        "transform consumer failure".into(),
                                    )),
                                }
                            },
                        )
                    },
                );
                if matches!(exit, Exit::Complete) {
                    result.unwrap();
                    assert_eq!(actual, expected);
                } else {
                    assert!(calls <= 1);
                    assert_eq!(actual, expected[..actual.len()]);
                    assert_eq!(calls == 0, expected.is_empty());
                    if calls == 1 && matches!(exit, Exit::Error) {
                        assert!(result
                            .unwrap_err()
                            .to_string()
                            .contains("transform consumer failure"));
                    } else {
                        result.unwrap();
                    }
                }
            }
        }
    }
}

#[test]
#[ignore = "deterministic local batch dispatch campaign"]
fn batch_dispatch_campaign() {
    let operators = fixtures::operators();
    for seed in 0..96 {
        let (operator, supported) = &operators[seed % operators.len()];
        for shape in 0..4 {
            let mut plan = wrap(operator.clone(), shape);
            for level in 0..seed % 8 {
                plan = wrap(plan, (seed + level) % 4);
            }
            assert_eq!(
                BatchPlanRef::try_new(&plan).is_some(),
                *supported,
                "seed {seed}, shape {shape}"
            );
            if !supported {
                let error = with_context(&[1, 2], 1, 64 * 1024, None, |context| {
                    execute_binding_batches(
                        &plan,
                        context,
                        ExecutionLimit::unlimited(),
                        &mut |_| panic!("unsupported campaign output"),
                    )
                })
                .unwrap_err();
                assert!(error
                    .to_string()
                    .contains("does not support batch execution"));
            }
        }
        for batch_rows in [1, 2, 5, 16] {
            for shape in 0..3 {
                for exit in [Exit::Complete, Exit::Stop, Exit::Error] {
                    check_read_case(seed, batch_rows, shape, exit);
                }
            }
        }
    }
}
