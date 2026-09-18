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
use hawdb_core::PropertyType;
use hawdb_plan::ComparisonOp;
use hawdb_storage::NodeId;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug)]
enum Path {
    Rows,
    Lending,
    OwnedTyped,
    ColumnarMorsel,
}

#[derive(Clone, Copy, Debug)]
enum Exit {
    Complete,
    Stop,
    Error,
}

fn check_case(seed: usize, batch_rows: usize, path: Path, exit: Exit) {
    let floating = seed.is_multiple_of(2);
    let threshold = (seed % 7) as i64 - 3;
    let predicate = [
        NumericPredicate::Eq,
        NumericPredicate::Compare(ComparisonOp::Lt),
        NumericPredicate::Compare(ComparisonOp::Lte),
        NumericPredicate::Compare(ComparisonOp::Gt),
        NumericPredicate::Compare(ComparisonOp::Gte),
    ][seed % 5];
    let limit = ExecutionLimit {
        output_rows: [None, Some(0), Some(1), Some(9), Some(usize::MAX)][seed / 5 % 5],
    };
    let fragment = NumericFragment {
        label: "Item",
        property: "score",
        property_type: if floating {
            PropertyType::Float
        } else {
            PropertyType::Int
        },
        predicate,
        expected: if seed.is_multiple_of(17) {
            NumericLiteral::Float(-0.0)
        } else if seed.is_multiple_of(19) {
            NumericLiteral::Float(f64::from_bits(0x7ff8_0000_0000_0042))
        } else if seed.is_multiple_of(3) {
            NumericLiteral::Float(threshold as f64)
        } else {
            NumericLiteral::Int(threshold)
        },
        fused_operators: None,
    };
    let items = [
        Projection {
            name: "value".into(),
            expression: ProjectionExpression::Property {
                variable: "n".into(),
                property: "score".into(),
            },
        },
        Projection {
            name: "identity".into(),
            expression: ProjectionExpression::Id {
                variable: "n".into(),
            },
        },
        Projection {
            name: "literal".into(),
            expression: ProjectionExpression::Literal(Value::Null),
        },
    ];
    let nodes: Vec<_> = (0..seed % 67)
        .map(|index| {
            let score = ((index * 11 + seed) % 17) as i64 - 8;
            let value = if floating {
                Value::Float(match (seed + index) % 11 {
                    0 => -0.0,
                    1 => f64::INFINITY,
                    2 => f64::NEG_INFINITY,
                    3 => f64::from_bits(0x7ff8_0000_0000_0042),
                    _ => score as f64 / 2.0,
                })
            } else {
                Value::Int(score)
            };
            let properties = match (seed + index) % 7 {
                0 => BTreeMap::new(),
                1 => BTreeMap::from([("score".into(), Value::Null)]),
                _ => BTreeMap::from([("score".into(), value)]),
            };
            NodeRecord {
                id: NodeId(index as u64),
                labels: BTreeSet::new(),
                properties,
            }
        })
        .collect();
    // This scalar oracle does not call numeric selection or projection helpers.
    let expected: Vec<_> = nodes
        .iter()
        .filter_map(|node| {
            let value = node.properties.get("score")?;
            // Equality is type-strict; ordering permits numeric coercion and
            // uses the established total order for signed zero and NaN.
            let (same_type, order) = match (value, fragment.expected) {
                (Value::Int(actual), NumericLiteral::Int(expected)) => {
                    (true, actual.cmp(&expected))
                }
                (Value::Float(actual), NumericLiteral::Float(expected)) => {
                    (true, actual.total_cmp(&expected))
                }
                (Value::Int(actual), NumericLiteral::Float(expected)) => {
                    (false, (*actual as f64).total_cmp(&expected))
                }
                (Value::Float(actual), NumericLiteral::Int(expected)) => {
                    (false, actual.total_cmp(&(expected as f64)))
                }
                _ => return None,
            };
            let selected = match predicate {
                NumericPredicate::Eq => same_type && order.is_eq(),
                NumericPredicate::Compare(ComparisonOp::Lt) => order.is_lt(),
                NumericPredicate::Compare(ComparisonOp::Lte) => !order.is_gt(),
                NumericPredicate::Compare(ComparisonOp::Gt) => order.is_gt(),
                NumericPredicate::Compare(ComparisonOp::Gte) => !order.is_lt(),
            };
            selected.then(|| {
                Binding::values(BTreeMap::from([
                    ("identity".into(), Value::Int(node.id.0 as i64)),
                    ("value".into(), value.clone()),
                    ("literal".into(), Value::Null),
                ]))
            })
        })
        .take(limit.output_rows.unwrap_or(usize::MAX))
        .collect();
    let observer = QueryExecutionObserver::default();
    let mut actual = Vec::new();
    let mut calls = 0;
    let mut emit = |batch: BindingBatch| {
        assert!(!batch.is_empty());
        assert!(batch.len() <= batch_rows);
        calls += 1;
        actual.extend(batch);
        match exit {
            Exit::Complete => Ok(BatchControl::Continue),
            Exit::Stop => Ok(BatchControl::Stop),
            Exit::Error => Err(HawDBError::Execution("numeric consumer failure".into())),
        }
    };
    let mut emitter = NumericBatchEmitter::new(fragment, &items, limit, None, &observer, &mut emit);
    let mut input_rows = 0;
    let result = (|| -> Result<BatchControl> {
        let schema = numeric_columnar_schema(fragment, true)?;
        let mut owned = OwnedNumericBatchBuffer::new(fragment, batch_rows, true);
        for batch in nodes.chunks(batch_rows) {
            input_rows += batch.len();
            let control = match path {
                Path::Rows => emitter.emit_nodes(batch)?,
                Path::Lending => {
                    let mut cursor =
                        NumericNodeBatchCursor::new(batch.iter(), fragment, batch_rows, true);
                    emitter.emit_typed(cursor.next_batch()?.unwrap())?
                }
                Path::OwnedTyped => {
                    for node in batch {
                        owned.push_owned(node.clone())?;
                    }
                    let control = emitter.emit_typed(owned.take_batch())?;
                    owned.clear();
                    control
                }
                Path::ColumnarMorsel => {
                    let rows = batch.iter().collect::<Vec<_>>();
                    let memory = numeric_morsel_memory(rows.len(), batch_rows, true, &schema);
                    let prepared = prepare_lending_numeric_morsel(
                        fragment,
                        &rows,
                        LendingNumericScan {
                            batch_rows,
                            needs_node_ids: true,
                        },
                        memory.output_reservation_bytes.get(),
                        &schema,
                        None,
                    )?;
                    assert_eq!(prepared.batches.len(), 1);
                    assert!(prepared.resident_bytes() <= memory.output_reservation_bytes.get());
                    emitter.emit_columnar(prepared.batches.into_iter().next().unwrap())?
                }
            };
            if control == BatchControl::Stop {
                return Ok(control);
            }
        }
        Ok(BatchControl::Continue)
    })();
    let reports = observer.into_reports();
    assert_eq!(reports.pipeline_memory.columnar_input_rows, input_rows);
    match exit {
        Exit::Complete => {
            result.unwrap();
            assert_eq!(
                actual, expected,
                "seed {seed}, batch {batch_rows}, path {path:?}"
            );
        }
        Exit::Stop | Exit::Error => {
            assert!(calls <= 1);
            assert_eq!(actual, expected[..actual.len()]);
            assert_eq!(calls == 0, expected.is_empty());
            if calls > 0 && matches!(exit, Exit::Error) {
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("numeric consumer failure"));
            } else {
                result.unwrap();
            }
        }
    }
}

fn campaign(seeds: usize) {
    for seed in 0..seeds {
        for batch_rows in [1, 3, 8] {
            for path in [
                Path::Rows,
                Path::Lending,
                Path::OwnedTyped,
                Path::ColumnarMorsel,
            ] {
                for exit in [Exit::Complete, Exit::Stop, Exit::Error] {
                    check_case(seed, batch_rows, path, exit);
                }
            }
        }
    }
}

#[test]
fn numeric_paths_match_scalar_oracle() {
    campaign(16);
}

#[test]
#[ignore = "deterministic local numeric execution campaign"]
fn numeric_execution_differential_campaign() {
    campaign(256);
    eprintln!("numeric execution campaign: 256 seeds, 9216 path/batch/exit cases");
}
