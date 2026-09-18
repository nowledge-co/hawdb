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
use crate::planner::{ComparisonOp, ProjectionExpression, SetAssignment};
use crate::schema::PropertyType;
use crate::store::{DurabilityPolicy, StorageResidencyMode, WalReplayConfig};
use hawdb_executor::columnar::{NumericLiteral, NumericPredicate};
use hawdb_executor::numeric::{
    stream_owned_numeric_nodes, stream_owned_typed_numeric_nodes, LendingNumericScan,
    NumericFragment,
};
use hawdb_storage::artifact_files::canonical_adjacency_artifact_generation_file;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        loop {
            let path = std::env::temp_dir().join(format!(
                "hawdb-scan-errors-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create scan fixture directory: {error}"),
            }
        }
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    store: GraphStore,
    catalog: Catalog,
    values: Vec<Value>,
    nodes: Vec<NodeId>,
    _directory: Directory,
}

impl Fixture {
    fn new(count: usize, out_of_core: bool) -> Self {
        Self::with_relationship(count, out_of_core, false)
    }

    fn with_relationship(count: usize, out_of_core: bool, relationship: bool) -> Self {
        let directory = Directory::new();
        let mut catalog = Catalog::default();
        let table = catalog.get_or_create_table(crate::schema::TableKind::Node, "Item");
        catalog.get_or_create_property(table, "score", PropertyType::Int, false);
        let replay = WalReplayConfig {
            residency_mode: if out_of_core {
                StorageResidencyMode::OutOfCore
            } else {
                StorageResidencyMode::Materialized
            },
            ..WalReplayConfig::default()
        };
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        let values: Vec<_> = (0..count).map(|index| Value::Int(index as i64)).collect();
        let nodes: Vec<_> = values
            .iter()
            .map(|value| {
                store
                    .create_node(
                        &mut catalog,
                        "Item",
                        BTreeMap::from([("score".to_string(), value.clone())]),
                    )
                    .unwrap()
            })
            .collect();
        if relationship {
            store
                .create_relationship(&mut catalog, nodes[1], nodes[2], "LINK", BTreeMap::new())
                .unwrap();
        }
        store.checkpoint(&catalog).unwrap();
        drop(store);
        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability_and_replay_config(
            &directory.0,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .unwrap();
        assert_eq!(store.is_out_of_core(), out_of_core);
        Self {
            store,
            catalog,
            values,
            nodes,
            _directory: directory,
        }
    }

    fn records(&self) -> Vec<NodeRecord> {
        self.nodes
            .iter()
            .map(|&id| self.store.node_owned(id).unwrap().unwrap())
            .collect()
    }

    fn durable_frontier(&self) -> BTreeMap<PathBuf, Vec<u8>> {
        let files: BTreeMap<_, _> = std::fs::read_dir(&self._directory.0)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| {
                let name = entry.file_name();
                name == "manifest.hawdb" || name.to_string_lossy().starts_with("wal.")
            })
            .map(|entry| {
                (
                    PathBuf::from(entry.file_name()),
                    std::fs::read(entry.path()).unwrap(),
                )
            })
            .collect();
        assert!(files.contains_key(&PathBuf::from("manifest.hawdb")));
        assert!(files.len() >= 2, "snapshot must include a WAL generation");
        files
    }
}

#[derive(Debug, Clone, Copy)]
enum Path {
    Visited,
    Owned,
    Typed,
}

#[derive(Debug, Clone, Copy)]
enum Exit {
    Complete,
    Stop,
    Error,
    Cancel,
}

fn sentinel() -> HawDBError {
    HawDBError::Semantic("scan callback sentinel".to_string())
}

fn with_context<T>(
    fixture: &Fixture,
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
    run: impl FnOnce(BatchReadContext<'_>) -> T,
) -> T {
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut external = NoExternalReadOperator;
    let external = BatchExternalReadAdapter::new(&mut external);
    let parameters = BTreeMap::new();
    let observer = QueryExecutionObserver::default();
    let context = BatchReadContext {
        catalog: &fixture.catalog,
        store: &fixture.store,
        parameters: &parameters,
        external: &external,
        memory,
        memory_ledger: &ledger,
        task_context,
        observer: &observer,
    };
    let result = run(context);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(ledger.snapshot().peak_bytes <= memory.query_memory_bytes.get());
    result
}

fn scan(
    path: Path,
    context: BatchReadContext<'_>,
    property_type: PropertyType,
    limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let fragment = NumericFragment {
        label: "Item",
        property: "score",
        property_type,
        predicate: NumericPredicate::Compare(ComparisonOp::Gte),
        expected: match property_type {
            PropertyType::Float => NumericLiteral::Float(0.0),
            _ => NumericLiteral::Int(0),
        },
        fused_operators: None,
    };
    let items = [Projection {
        expression: ProjectionExpression::Property {
            variable: "n".to_string(),
            property: "score".to_string(),
        },
        name: "score".to_string(),
    }];
    let label = context.catalog.label_id("Item").unwrap();
    match path {
        Path::Visited => stream_visited_node_batches("n", context, limit, emit, |consumer| {
            hawdb_executor::store::GraphExecutionRead::visit_nodes_owned(
                context.store,
                Some(label),
                consumer,
            )
        }),
        Path::Owned => stream_owned_numeric_nodes(
            fragment,
            &items,
            label,
            context.numeric_context(),
            limit,
            emit,
        )
        .map(|(_, stopped)| {
            if stopped {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            }
        }),
        Path::Typed => stream_owned_typed_numeric_nodes(
            fragment,
            &items,
            label,
            LendingNumericScan {
                batch_rows: context.memory.batch_rows.get(),
                needs_node_ids: false,
            },
            context.numeric_context(),
            limit,
            emit,
        )
        .map(|(_, stopped)| {
            if stopped {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            }
        }),
    }
}

fn exercise(fixture: &Fixture, path: Path, batch_rows: usize, exit: Exit, exit_at: usize) {
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(batch_rows).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(32 * 1024).unwrap(),
        query_memory_bytes: NonZeroUsize::new(128 * 1024).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let token = hawdb_core::RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    let mut calls = 0;
    let mut values = Vec::new();
    let result = with_context(fixture, &memory, Some(&task), |context| {
        scan(
            path,
            context,
            PropertyType::Int,
            ExecutionLimit::unlimited(),
            &mut |batch| {
                calls += 1;
                assert!(!batch.is_empty() && batch.len() <= batch_rows);
                values.extend(batch.iter().map(|binding| match path {
                    Path::Visited => binding.nodes["n"].properties["score"].clone(),
                    Path::Owned | Path::Typed => binding.values["score"].clone(),
                }));
                if calls == exit_at {
                    match exit {
                        Exit::Complete => {}
                        Exit::Stop => return Ok(BatchControl::Stop),
                        Exit::Error => return Err(sentinel()),
                        Exit::Cancel => {
                            token.cancel();
                        }
                    }
                }
                Ok(BatchControl::Continue)
            },
        )
    });
    match exit {
        Exit::Complete => {
            assert_eq!(result.unwrap(), BatchControl::Continue);
            assert_eq!(values, fixture.values);
        }
        Exit::Stop => assert_eq!(result.unwrap(), BatchControl::Stop),
        Exit::Error => assert_eq!(result.unwrap_err(), sentinel()),
        Exit::Cancel => assert!(result
            .unwrap_err()
            .to_string()
            .contains("runtime task stopped")),
    }
    if !matches!(exit, Exit::Complete) {
        assert_eq!(calls, exit_at, "no callbacks after {exit:?} on {path:?}");
        assert_eq!(values, fixture.values[..exit_at * batch_rows]);
    }
}

#[test]
fn owned_scan_callbacks_preserve_errors_stop_and_cancellation() {
    for out_of_core in [false, true] {
        let fixture = Fixture::new(9, out_of_core);
        for path in [Path::Visited, Path::Owned, Path::Typed] {
            for exit in [Exit::Complete, Exit::Stop, Exit::Error, Exit::Cancel] {
                exercise(&fixture, path, 3, exit, 1);
            }
        }
    }
}

#[test]
fn scan_failures_release_accounts_without_emitting_rows() {
    for out_of_core in [false, true] {
        let fixture = Fixture::new(3, out_of_core);
        for path in [Path::Owned, Path::Typed] {
            let error = with_context(
                &fixture,
                &ExecutionMemoryConfig::default(),
                None,
                |context| {
                    scan(
                        path,
                        context,
                        PropertyType::Float,
                        ExecutionLimit::unlimited(),
                        &mut |_| panic!("a mismatched numeric schema must not emit rows"),
                    )
                },
            )
            .unwrap_err();
            assert!(error.to_string().contains("schema"), "{error}");
        }
        for payload_limit in [false, true] {
            let memory = ExecutionMemoryConfig {
                query_memory_bytes: NonZeroUsize::new(if payload_limit { 128 * 1024 } else { 1 })
                    .unwrap(),
                batch_payload_bytes: NonZeroUsize::new(if payload_limit { 1 } else { 32 * 1024 })
                    .unwrap(),
                ..ExecutionMemoryConfig::default()
            };
            let error = with_context(&fixture, &memory, None, |context| {
                scan(
                    Path::Visited,
                    context,
                    PropertyType::Int,
                    ExecutionLimit::unlimited(),
                    &mut |_| panic!("a rejected node must not emit rows"),
                )
            })
            .unwrap_err();
            let boundary = if payload_limit {
                "batch_payload_bytes"
            } else {
                "query_memory_bytes"
            };
            assert!(error.to_string().contains(boundary), "{error}");
        }
    }
}

#[test]
fn owned_scan_limits_preserve_exact_prefixes_and_partial_batches() {
    for out_of_core in [false, true] {
        let fixture = Fixture::new(9, out_of_core);
        let memory = ExecutionMemoryConfig {
            batch_rows: NonZeroUsize::new(3).unwrap(),
            ..ExecutionMemoryConfig::default()
        };
        for path in [Path::Visited, Path::Owned, Path::Typed] {
            for limit in [1, 2, 3, 4, 8, 9, 10] {
                let mut values = Vec::new();
                with_context(&fixture, &memory, None, |context| {
                    scan(
                        path,
                        context,
                        PropertyType::Int,
                        ExecutionLimit {
                            output_rows: Some(limit),
                        },
                        &mut |batch| {
                            values.extend(batch.iter().map(|binding| match path {
                                Path::Visited => binding.nodes["n"].properties["score"].clone(),
                                Path::Owned | Path::Typed => binding.values["score"].clone(),
                            }));
                            Ok(BatchControl::Continue)
                        },
                    )
                })
                .unwrap();
                assert_eq!(values, fixture.values[..limit.min(fixture.values.len())]);
            }
        }
    }
}

#[test]
fn optional_count_sum_rejects_leg_errors_without_partial_output() {
    for out_of_core in [false, true] {
        let fixture = Fixture::with_relationship(3, out_of_core, true);
        let plan = PhysicalPlan::OptionalRelationshipCountSumExec {
            variable: "n".to_string(),
            label: "Item".to_string(),
            properties: BTreeMap::new(),
            legs: vec![RelationshipCountLeg {
                rel_type: String::new(),
                direction: RelationshipDirection::Outgoing,
                distinct: false,
                filter: None,
            }],
            output: "count".to_string(),
        };
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(1).unwrap(),
            ..ExecutionMemoryConfig::default()
        };
        let error = with_context(&fixture, &memory, None, |context| {
            execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |_| {
                panic!("a failed count leg must not emit a partial aggregate")
            })
        })
        .unwrap_err();
        assert!(
            error.to_string().contains("budget")
                || error.to_string().contains("blocking_operator_bytes"),
            "{error}"
        );
        with_context(
            &fixture,
            &ExecutionMemoryConfig::default(),
            None,
            |context| {
                execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |batch| {
                    assert_eq!(batch.len(), 1);
                    assert_eq!(batch[0].values["count"], Value::Int(1));
                    Err(sentinel())
                })
            },
        )
        .map_or_else(
            |error| assert_eq!(error, sentinel()),
            |_| panic!("output error was lost"),
        );
    }
}

fn failing_predicate() -> Predicate {
    Predicate::Or(vec![
        Predicate::PropertyEq {
            variable: "n".to_string(),
            property: "score".to_string(),
            value: Value::Int(0),
        },
        Predicate::RelationshipExists {
            variable: "n".to_string(),
            rel_type: "LINK".to_string(),
            direction: RelationshipDirection::Outgoing,
            target_label: "Item".to_string(),
        },
    ])
}

fn mutation_plans(predicate: Predicate) -> [PhysicalPlan; 3] {
    let assignments = vec![SetAssignment {
        property: "score".to_string(),
        value: SetValue::Value(Value::Int(99)),
    }];
    [
        PhysicalPlan::SetNodeProperties {
            variable: "n".to_string(),
            label: "Item".to_string(),
            predicate: Some(predicate.clone()),
            assignments: assignments.clone(),
        },
        PhysicalPlan::SetNodePropertiesReturn {
            variable: "n".to_string(),
            label: "Item".to_string(),
            predicate: Some(predicate.clone()),
            assignments,
            returns: SetNodePropertiesReturnMode::Project(vec![Projection {
                name: "score".to_string(),
                expression: ProjectionExpression::Property {
                    variable: "n".to_string(),
                    property: "score".to_string(),
                },
            }]),
        },
        PhysicalPlan::DeleteNode {
            variable: "n".to_string(),
            label: "Item".to_string(),
            predicate: Some(predicate),
            detach: true,
        },
    ]
}

#[test]
fn mutation_scan_predicate_errors_do_not_commit_partial_writes() {
    assert!(property_filter_from_predicate(&failing_predicate()).is_err());
    {
        // Materialized adjacency does not read its checkpoint artifact. Exercise
        // the real fallible storage boundary through the out-of-core reader.
        let mut fixture = Fixture::with_relationship(3, true, true);
        let adjacency = fixture
            ._directory
            .0
            .join(canonical_adjacency_artifact_generation_file(1));
        std::fs::OpenOptions::new()
            .write(true)
            .open(adjacency)
            .unwrap()
            .set_len(0)
            .unwrap();
        let binding = |node| Binding {
            nodes: BTreeMap::from([("n".to_string(), node)]),
            values: BTreeMap::new(),
            relationships: BTreeMap::new(),
        };
        let records = fixture.records();
        assert!(evaluate_predicate(
            &failing_predicate(),
            &fixture.catalog,
            &fixture.store,
            &binding(records[0].clone()),
        )
        .unwrap());
        let expected_error = evaluate_predicate(
            &failing_predicate(),
            &fixture.catalog,
            &fixture.store,
            &binding(records[1].clone()),
        )
        .unwrap_err();
        assert!(matches!(expected_error, HawDBError::StorageIntegrity(_)));
        // The first physical failure poisons the reader. Compare callback
        // propagation with the error from the same subsequent reader state.
        let expected_error = evaluate_predicate(
            &failing_predicate(),
            &fixture.catalog,
            &fixture.store,
            &binding(records[1].clone()),
        )
        .unwrap_err();
        let plans = mutation_plans(failing_predicate());
        let before = fixture.records();
        let durable_before = fixture.durable_frontier();
        for plan in plans {
            let memory = ExecutionMemoryConfig::default();
            let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
            let parameters = BTreeMap::new();
            let observer = QueryExecutionObserver::default();
            let mut external = NoExternalReadOperator;
            let mut context = ExecutionContext {
                parameters: &parameters,
                external: &mut external,
                memory: &memory,
                memory_ledger: &ledger,
                task_context: None,
                observer: &observer,
            };
            // The legacy plain SET path accepts storage predicates only and
            // does not use the callback being refactored here.
            if !matches!(plan, PhysicalPlan::SetNodeProperties { .. }) {
                let error = execute_bindings_with_limit(
                    &plan,
                    &mut fixture.catalog,
                    &mut fixture.store,
                    &mut context,
                    ExecutionLimit::unlimited(),
                )
                .unwrap_err();
                assert_eq!(error, expected_error);
            }
            assert_eq!(fixture.records(), before);
            assert_eq!(fixture.durable_frontier(), durable_before);
            let limited = execute_mutation_with_limits(
                &plan,
                &mut fixture.catalog,
                &mut fixture.store,
                MutationLimits::default(),
                None,
            );
            let error = limited.unwrap_err();
            assert_eq!(error, expected_error);
            assert_eq!(fixture.records(), before);
            assert_eq!(fixture.durable_frontier(), durable_before);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
}

#[test]
#[ignore = "deterministic local scan callback differential campaign"]
fn scan_callback_differential_campaign() {
    let mut cases = 0;
    for seed in 0..4 {
        for out_of_core in [false, true] {
            let fixture = Fixture::new(9 + 8 * seed, out_of_core);
            for path in [Path::Visited, Path::Owned, Path::Typed] {
                for batch_rows in [1, 3, 4] {
                    for exit in [Exit::Complete, Exit::Stop, Exit::Error, Exit::Cancel] {
                        exercise(&fixture, path, batch_rows, exit, 1 + seed % 2);
                        cases += 1;
                    }
                }
            }
        }
    }
    assert_eq!(cases, 288);
    eprintln!("scan callback differential campaign: {cases} cases");
}

fn assert_mutation_rejected(
    fixture: &mut Fixture,
    plan: &PhysicalPlan,
    limits: MutationLimits,
    task: Option<&RuntimeTaskContext>,
    expected: &str,
) {
    let records = fixture.records();
    let frontier = fixture.durable_frontier();
    let error =
        execute_mutation_with_limits(plan, &mut fixture.catalog, &mut fixture.store, limits, task)
            .unwrap_err();
    assert!(error.to_string().contains(expected), "{error}");
    assert_eq!(fixture.records(), records);
    assert_eq!(fixture.durable_frontier(), frontier);
}

#[test]
fn mutation_preflight_limits_reject_before_wal_changes() {
    let limits = [
        (
            "max_mutation_affected_rows",
            MutationLimits {
                max_affected_rows: NonZeroUsize::new(1).unwrap(),
                ..MutationLimits::default()
            },
        ),
        (
            "max_mutation_result_rows",
            MutationLimits {
                max_result_rows: NonZeroUsize::new(1).unwrap(),
                ..MutationLimits::default()
            },
        ),
        (
            "max_mutation_result_payload_bytes",
            MutationLimits {
                max_result_payload_bytes: NonZeroUsize::new(16).unwrap(),
                ..MutationLimits::default()
            },
        ),
        (
            "max_mutation_operations",
            MutationLimits {
                max_operations: NonZeroUsize::new(1).unwrap(),
                ..MutationLimits::default()
            },
        ),
    ];
    for out_of_core in [false, true] {
        for plan in mutation_plans(Predicate::ConstantBool(true)) {
            // Force the executable-predicate fallback, not the storage-filter
            // fast path, so the refactored callback collects the candidate IDs.
            assert!(mutation_command(&plan).is_err());
            let mut fixture = Fixture::new(3, out_of_core);
            for (boundary, limits) in limits {
                assert_mutation_rejected(&mut fixture, &plan, limits, None, boundary);
            }
            let token = hawdb_core::RuntimeCancellationToken::new();
            token.cancel();
            let task = RuntimeTaskContext::without_deadline(token);
            assert_mutation_rejected(
                &mut fixture,
                &plan,
                MutationLimits::default(),
                Some(&task),
                "runtime task stopped",
            );

            let before = fixture.durable_frontier();
            let rows = execute_mutation_with_limits(
                &plan,
                &mut fixture.catalog,
                &mut fixture.store,
                MutationLimits::default(),
                None,
            )
            .unwrap();
            assert_eq!(rows.len(), 3);
            assert_ne!(fixture.durable_frontier(), before);
            if matches!(&plan, PhysicalPlan::DeleteNode { .. }) {
                assert!(fixture.nodes.iter().all(|&id| fixture
                    .store
                    .node_owned(id)
                    .unwrap()
                    .is_none()));
            } else {
                assert!(fixture
                    .records()
                    .iter()
                    .all(|node| node.properties["score"] == Value::Int(99)));
            }
        }
    }
}

#[test]
fn mutation_late_assignment_and_return_errors_do_not_append_wal() {
    for out_of_core in [false, true] {
        let mut fixture = Fixture::new(3, out_of_core);
        let mut plan = mutation_plans(Predicate::ConstantBool(true))[1].clone();
        let PhysicalPlan::SetNodePropertiesReturn { assignments, .. } = &mut plan else {
            unreachable!("fixture selects SET RETURN")
        };
        assignments[0].value = SetValue::AddInt {
            property: "score".to_string(),
            amount: i64::MAX,
        };
        // Node zero's update is representable; node one's update overflows.
        assert_mutation_rejected(
            &mut fixture,
            &plan,
            MutationLimits::default(),
            None,
            "overflow",
        );

        let PhysicalPlan::SetNodePropertiesReturn {
            assignments,
            returns,
            ..
        } = &mut plan
        else {
            unreachable!("fixture selects SET RETURN")
        };
        assignments[0] = SetAssignment {
            property: "updated".to_string(),
            value: SetValue::Value(Value::Int(99)),
        };
        let expression =
            ProjectionExpression::Lower(Box::new(ProjectionExpression::CasePropertyEqualsRank {
                variable: "n".to_string(),
                property: "score".to_string(),
                branches: vec![(Value::Int(0), Value::String("valid".to_string()))],
                default: Value::Int(1),
            }));
        *returns = SetNodePropertiesReturnMode::Project(vec![Projection {
            name: "result".to_string(),
            expression,
        }]);
        // The first projected row succeeds; the second requires LOWER(integer).
        assert_mutation_rejected(
            &mut fixture,
            &plan,
            MutationLimits::default(),
            None,
            "LOWER expression requires a string",
        );
    }
}
