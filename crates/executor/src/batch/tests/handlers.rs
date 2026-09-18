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

use super::dispatch::Exit;
use super::store::ReadFixture;
use crate::analytics::try_projected_graph_with_node_filter;
use crate::batch::*;
use crate::external::NoExternalReadOperator;
use crate::observer::QueryExecutionReports;
use crate::Row;
use hawdb_analytics::{
    LouvainOptions, PageRankOptions, ProjectedGraphExecution, ProjectionLayout,
    ProjectionMemoryBudget,
};
use hawdb_plan::GraphAlgorithmKind;

fn graph_algorithm_fixture() -> (Catalog, ReadFixture) {
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Memory");
    let rel_type = catalog.get_or_create_rel_type("MENTIONS");
    let store = ReadFixture {
        nodes: (0..2)
            .map(|id| NodeRecord {
                id: NodeId(id),
                labels: [label].into_iter().collect(),
                properties: BTreeMap::from([("id".to_string(), Value::Int(id as i64 + 1))]),
            })
            .collect(),
        relationships: vec![hawdb_storage::RelRecord {
            id: hawdb_storage::RelId(0),
            source: NodeId(0),
            target: NodeId(1),
            rel_type,
            properties: BTreeMap::new(),
        }],
        definition: Some(hawdb_storage::ProjectedGraphDefinition {
            node_labels: vec!["Memory".to_string()],
            rel_types: vec!["MENTIONS".to_string()],
        }),
        ..ReadFixture::default()
    };
    (catalog, store)
}

fn graph_algorithm_plan(algorithm: GraphAlgorithmKind) -> PhysicalPlan {
    PhysicalPlan::GraphAlgorithm {
        algorithm,
        graph_name: "MemoryGraph".to_string(),
        options: hawdb_plan::GraphAlgorithmOptions {
            damping: None,
            max_iterations: Some(2),
            max_levels: Some(1),
        },
        score_column: "score".to_string(),
        node_visibility_predicate: None,
    }
}

fn with_graph_context<T>(
    plan: &PhysicalPlan,
    batch_rows: usize,
    blocking_bytes: usize,
    task_context: Option<&RuntimeTaskContext>,
    run: impl FnOnce(BatchReadContext<'_>) -> T,
) -> (T, QueryExecutionReports) {
    let (catalog, store) = graph_algorithm_fixture();
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(batch_rows).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(blocking_bytes).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let memory_ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let parameters = BTreeMap::new();
    let mut external_operator = NoExternalReadOperator;
    let external = BatchExternalReadAdapter::new(&mut external_operator);
    let observer = QueryExecutionObserver::new(plan);
    let result = run(BatchReadContext {
        catalog: &catalog,
        store: &store,
        parameters: &parameters,
        external: &external,
        memory: &memory,
        memory_ledger: &memory_ledger,
        task_context,
        observer: &observer,
    });
    let snapshot = memory_ledger.snapshot();
    assert_eq!(
        snapshot.used_bytes, 0,
        "handler leaked a query memory lease"
    );
    assert!(snapshot.peak_bytes <= memory.query_memory_bytes.get());
    (result, observer.into_reports())
}

fn graph_row_oracle(algorithm: GraphAlgorithmKind, context: BatchReadContext<'_>) -> Vec<Row> {
    // Exercise the algorithm directly, independently of dispatcher output mapping.
    let graph = try_projected_graph_with_node_filter(
        context.catalog,
        context.store,
        &["Memory".to_string()],
        &["MENTIONS".to_string()],
        |_| true,
        match algorithm {
            GraphAlgorithmKind::PageRank => ProjectionLayout::Outgoing,
            GraphAlgorithmKind::Louvain => ProjectionLayout::Undirected,
        },
        ProjectionMemoryBudget::new(context.memory.blocking_operator_bytes),
    )
    .unwrap();
    match algorithm {
        GraphAlgorithmKind::PageRank => graph
            .page_rank_with_context(
                PageRankOptions {
                    iterations: 2,
                    ..PageRankOptions::default()
                },
                None,
            )
            .unwrap()
            .into_iter()
            .map(|score| {
                BTreeMap::from([
                    ("node".to_string(), Value::Int(score.node.0 as i64)),
                    ("score".to_string(), Value::Float(score.score)),
                ])
            })
            .collect(),
        GraphAlgorithmKind::Louvain => graph
            .hierarchical_louvain_communities_with_context(
                LouvainOptions {
                    max_iterations: 2,
                    max_levels: 1,
                },
                None,
            )
            .unwrap()
            .into_iter()
            .map(|assignment| {
                BTreeMap::from([
                    ("node".to_string(), Value::Int(assignment.node.0 as i64)),
                    ("level".to_string(), Value::Int(assignment.level as i64)),
                    (
                        "louvain_id".to_string(),
                        Value::Int(assignment.community.0 as i64),
                    ),
                ])
            })
            .collect(),
    }
}

#[test]
fn graph_handlers_preserve_rows_limits_consumer_control_and_reports() {
    for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
        let plan = graph_algorithm_plan(algorithm);
        for batch_rows in [1, 2, 8] {
            for output_rows in [None, Some(0), Some(1), Some(2), Some(3)] {
                for exit in [Exit::Complete, Exit::Stop, Exit::Error] {
                    let mut calls = 0;
                    let mut actual = Vec::new();
                    let (_, reports) =
                        with_graph_context(&plan, batch_rows, 4096, None, |context| {
                            let expected: Vec<_> = graph_row_oracle(algorithm, context)
                                .into_iter()
                                .take(output_rows.unwrap_or(usize::MAX))
                                .collect();
                            let consumer_error = HawDBError::StorageIntegrity(
                                "graph handler consumer sentinel".to_string(),
                            );
                            let result = execute_binding_batches(
                                &plan,
                                context,
                                ExecutionLimit { output_rows },
                                &mut |batch| {
                                    calls += 1;
                                    assert!(!batch.is_empty());
                                    assert!(batch.len() <= batch_rows);
                                    actual.extend(batch.into_iter().map(|binding| binding.values));
                                    match exit {
                                        Exit::Complete => Ok(BatchControl::Continue),
                                        Exit::Stop => Ok(BatchControl::Stop),
                                        Exit::Error => Err(consumer_error.clone()),
                                    }
                                },
                            );
                            match exit {
                                Exit::Complete => {
                                    result.unwrap();
                                    assert_eq!(actual, expected);
                                }
                                Exit::Stop | Exit::Error => {
                                    assert_eq!(calls, usize::from(!expected.is_empty()));
                                    assert_eq!(actual, expected[..expected.len().min(batch_rows)]);
                                    if calls > 0 && matches!(exit, Exit::Error) {
                                        assert_eq!(result.unwrap_err(), consumer_error);
                                    } else {
                                        result.unwrap();
                                    }
                                }
                            }
                        });
                    if output_rows == Some(0) {
                        assert!(reports.blocking_memory.is_empty());
                    } else {
                        assert_eq!(reports.blocking_memory.len(), 1);
                        let report = &reports.blocking_memory[0];
                        assert_eq!(report.operator, "GraphAlgorithm");
                        assert_eq!(report.input_rows, 2);
                        assert_eq!(report.budget_bytes, 4096);
                        assert!(report.peak_tracked_bytes > 0);
                        assert!(report.peak_tracked_bytes <= report.budget_bytes);
                        assert_eq!(report.spilled_bytes, 0);
                        assert_eq!(
                            reports.operator_cardinality[0].actual_rows,
                            Some(actual.len())
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn graph_handler_errors_release_memory_without_emitting_partial_results() {
    let mut missing = graph_algorithm_plan(GraphAlgorithmKind::PageRank);
    if let PhysicalPlan::GraphAlgorithm { graph_name, .. } = &mut missing {
        *graph_name = "MissingGraph".to_string();
    }
    let scratch = graph_algorithm_plan(GraphAlgorithmKind::PageRank);
    for (plan, budget, expected) in [
        (
            missing,
            4096,
            "projected graph 'MissingGraph' does not exist",
        ),
        (
            scratch,
            150,
            "GraphAlgorithm PageRank scratch and result state",
        ),
    ] {
        let (result, reports) = with_graph_context(&plan, 1, budget, None, |context| {
            execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |_| {
                panic!("failed graph handler emitted a partial result")
            })
        });
        assert!(result.unwrap_err().to_string().contains(expected));
        if budget == 150 {
            assert_eq!(reports.blocking_memory.len(), 1);
            assert!(reports.blocking_memory[0].peak_tracked_bytes > 0);
        }
    }
}

#[test]
fn graph_handler_cancellation_from_consumer_releases_memory() {
    for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
        let plan = graph_algorithm_plan(algorithm);
        let cancellation = hawdb_core::RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(cancellation.clone());
        let mut calls = 0;
        let (result, _) = with_graph_context(&plan, 1, 4096, Some(&task), |context| {
            execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |batch| {
                calls += 1;
                assert_eq!(batch.len(), 1);
                assert!(cancellation.cancel());
                Ok(BatchControl::Continue)
            })
        });
        assert_eq!(calls, 1);
        assert_eq!(
            result.unwrap_err(),
            HawDBError::Execution("runtime task stopped: cancelled".to_string())
        );
    }
}
