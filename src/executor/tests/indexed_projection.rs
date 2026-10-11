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
use hawdb_plan_cypher::NodeProjectionAccess;

fn point_plan(value: Value, indexed: bool) -> PhysicalPlan {
    PhysicalPlan::NodeProjectionScanExec {
        variable: "n".into(),
        label: "Item".into(),
        access: if indexed {
            NodeProjectionAccess::PropertyValues {
                property: "key".into(),
                values: vec![value.clone()],
            }
        } else {
            NodeProjectionAccess::LabelScan
        },
        required_properties: vec!["key".into(), "rank".into(), "title".into()],
        predicate: Some(Predicate::And(vec![
            Predicate::PropertyEq {
                variable: "n".into(),
                property: "key".into(),
                value,
            },
            Predicate::PropertyCompare {
                variable: "n".into(),
                property: "rank".into(),
                op: crate::planner::ComparisonOp::Gte,
                value: Value::Int(1),
            },
        ])),
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".into(),
                property: "title".into(),
            },
            name: "title".into(),
        }],
    }
}

#[test]
fn indexed_projection_matches_scan_before_and_after_backfill_and_mutations() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let keys = [
        Value::Int(1),
        Value::Int(1),
        Value::Float(1.0),
        Value::String("1\0one".into()),
        Value::Null,
    ];
    for (rank, key) in keys.into_iter().enumerate() {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([
                    ("key", key),
                    ("rank", Value::Int(rank as i64)),
                    ("title", Value::String(format!("row-{rank}"))),
                    ("unrequested", Value::String("x".repeat(128 * 1024))),
                ]),
            )
            .unwrap();
    }
    store
        .create_node(
            &mut catalog,
            "Other",
            properties([("key", Value::Int(1)), ("rank", Value::Int(9))]),
        )
        .unwrap();
    let memory = ExecutionMemoryConfig {
        batch_payload_bytes: NonZeroUsize::new(4096).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(4096).unwrap(),
        ..Default::default()
    };
    for phase in 0..4 {
        match phase {
            1 => {
                store
                    .create_property_index(&mut catalog, "Item", "key")
                    .unwrap();
            }
            2 => {
                store
                    .set_node_property(
                        &mut catalog,
                        "Item",
                        Some(&hawdb_storage::mutation::PropertyFilter::Eq {
                            property: "rank".into(),
                            value: Value::Int(2),
                        }),
                        "key",
                        Value::Int(1),
                    )
                    .unwrap();
            }
            3 => {
                store
                    .delete_nodes(
                        &mut catalog,
                        "Item",
                        Some(&hawdb_storage::mutation::PropertyFilter::Eq {
                            property: "rank".into(),
                            value: Value::Int(1),
                        }),
                        false,
                    )
                    .unwrap();
            }
            _ => {}
        }
        for value in [
            Value::Int(1),
            Value::Float(1.0),
            Value::String("1\0one".into()),
            Value::Null,
            Value::Int(999),
        ] {
            let mut external = NoExternalReadOperator;
            let expected = execute_with_request(
                ExecutionRequest::new(&point_plan(value.clone(), false), &BTreeMap::new(), &memory),
                ExecutionResources::new(&mut catalog, &mut store, &mut external),
            )
            .unwrap();
            let actual = execute_with_request(
                ExecutionRequest::new(&point_plan(value, true), &BTreeMap::new(), &memory),
                ExecutionResources::new(&mut catalog, &mut store, &mut external),
            )
            .unwrap();
            assert_eq!(actual.rows, expected.rows, "phase {phase}");
        }
    }
}

#[test]
fn indexed_projection_admits_selected_source_before_residual_evaluation() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Item",
            properties([
                ("key", Value::Int(1)),
                ("rank", Value::Int(0)),
                ("title", Value::String("x".repeat(128 * 1024))),
            ]),
        )
        .unwrap();
    store
        .create_property_index(&mut catalog, "Item", "key")
        .unwrap();
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(4096).unwrap(),
        ..Default::default()
    };
    let mut external = NoExternalReadOperator;
    let error = execute_with_request(
        ExecutionRequest::new(&point_plan(Value::Int(1), true), &BTreeMap::new(), &memory),
        ExecutionResources::new(&mut catalog, &mut store, &mut external),
    )
    .unwrap_err();
    // The residual rejects rank=0, but source allocation must be admitted first.
    assert!(error.to_string().contains("4096-byte budget"), "{error}");
    let output = execute_with_request(
        ExecutionRequest::new(
            &point_plan(Value::Int(999), true),
            &BTreeMap::new(),
            &memory,
        ),
        ExecutionResources::new(&mut catalog, &mut store, &mut external),
    )
    .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn indexed_projection_stop_error_limit_and_cancellation_release_source_leases() {
    use hawdb_executor::observer::NoopExecutionObserver;
    use hawdb_executor::pipeline::BatchControl;
    use hawdb_executor::scan::{
        stream_node_projection_scan_batches, NodeProjectionScanSpec, NodeScanContext,
    };
    use hawdb_executor::{ExecutionLimit, QueryMemoryClass, QueryMemoryLedger};

    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for title in ["first".into(), "x".repeat(128 * 1024)] {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("key", Value::Int(1)), ("title", Value::String(title))]),
            )
            .unwrap();
    }
    store
        .create_property_index(&mut catalog, "Item", "key")
        .unwrap();
    let access = NodeProjectionAccess::PropertyValues {
        property: "key".into(),
        values: vec![Value::Int(1)],
    };
    let required_properties = vec!["title".into()];
    let items = vec![Projection {
        expression: ProjectionExpression::Property {
            variable: "n".into(),
            property: "title".into(),
        },
        name: "title".into(),
    }];
    for case in ["stop", "error", "limit", "cancel"] {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(16 * 1024).unwrap());
        let budget = NonZeroUsize::new(4096).unwrap();
        let source = ledger.account(QueryMemoryClass::BlockingState, "source", budget);
        let output = ledger.account(QueryMemoryClass::PipelineBatch, "batch", budget);
        let token = hawdb_core::RuntimeCancellationToken::new();
        if case == "cancel" {
            token.cancel();
        }
        let task = RuntimeTaskContext::without_deadline(token);
        let mut calls = 0;
        let result = stream_node_projection_scan_batches(
            NodeProjectionScanSpec {
                variable: "n",
                label: "Item",
                access: &access,
                required_properties: &required_properties,
                predicate: None,
                items: &items,
            },
            NodeScanContext {
                catalog: &catalog,
                store: &store,
                execution_limit: ExecutionLimit::unlimited()
                    .child_for_limit(0, (case == "limit").then_some(1)),
                memory_budget: budget,
                memory_account: &source,
                batch_memory_budget: budget,
                batch_memory_account: &output,
                batch_rows: 1,
                task_context: Some(&task),
            },
            &NoopExecutionObserver,
            &mut |batch| {
                calls += 1;
                assert_eq!(batch.len(), 1);
                match case {
                    "error" => Err(HawDBError::Execution("consumer failure".into())),
                    "stop" => Ok(BatchControl::Stop),
                    _ => Ok(BatchControl::Continue),
                }
            },
        );
        match case {
            "error" => assert!(result.unwrap_err().to_string().contains("consumer failure")),
            "cancel" => assert!(result.unwrap_err().to_string().contains("cancel")),
            _ => assert_eq!(result.unwrap(), BatchControl::Stop),
        }
        // Fetching the next candidate would reject its selected 128 KiB field.
        assert_eq!(calls, usize::from(case != "cancel"));
        assert_eq!(ledger.snapshot().used_bytes, 0, "{case}");
    }
}
