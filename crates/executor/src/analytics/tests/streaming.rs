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

pub(super) fn run_external(
    fixture: &Fixture,
    options: &RunOptions,
    task: Option<&RuntimeTaskContext>,
) -> Outcome {
    let ledger = QueryMemoryLedger::new(options.memory.query_memory_bytes);
    let observer = QueryExecutionObserver::default();
    let mut batches = Vec::new();
    let mut live_bytes = Vec::new();
    let result = GraphAlgorithmSpec {
        algorithm: &options.algorithm,
        graph_name: "graph",
        options: &options.options,
        score_column: &options.score_column,
        node_visibility_predicate: &options.predicate,
    }
    .stream_external(
        GraphAlgorithmContext {
            catalog: &fixture.catalog,
            store: fixture,
            memory: &options.memory,
            memory_ledger: &ledger,
            task_context: task,
            observer: &observer,
        },
        ExecutionLimit {
            output_rows: options.output_rows,
        },
        &mut |batch| {
            live_bytes.push(ledger.snapshot().used_bytes);
            batches.push(batch);
            match options.exit {
                Exit::Complete => Ok(BatchControl::Continue),
                Exit::Stop => Ok(BatchControl::Stop),
                Exit::Error => Err(HawDBError::StorageIntegrity("consumer sentinel".into())),
            }
        },
    );
    let memory = ledger.snapshot();
    assert_eq!(memory.used_bytes, 0);
    assert!(memory.classes.iter().all(|class| class.used_bytes == 0));
    Outcome {
        result,
        batches,
        live_bytes,
        reports: observer.into_reports(),
        peak_bytes: memory.peak_bytes,
    }
}

fn dense_fixture() -> Fixture {
    let mut fixture = Fixture::new();
    fixture.nodes = (0..128)
        .map(|id| NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::from([LabelId(0)]),
            properties: BTreeMap::new(),
        })
        .collect();
    fixture.relationships.clear();
    for source in 0..128 {
        for target in 0..128 {
            if source != target {
                fixture.relationships.push(RelRecord {
                    id: RelId(fixture.relationships.len() as u64),
                    source: NodeId(source),
                    target: NodeId(target),
                    rel_type: RelTypeId(0),
                    properties: BTreeMap::new(),
                });
            }
        }
    }
    fixture
}

#[test]
fn both_algorithms_complete_when_edges_exceed_the_blocking_budget() {
    let fixture = dense_fixture();
    let budget = 96 * 1024;
    assert!(fixture.relationships.len() * std::mem::size_of::<usize>() > budget);
    for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
        let mut options = RunOptions {
            algorithm,
            ..RunOptions::default()
        };
        options.options.max_iterations = Some(3);
        options.options.max_levels = Some(2);
        options.memory.blocking_operator_bytes = nz(budget);
        options.memory.batch_rows = nz(7);
        let layout = if algorithm == GraphAlgorithmKind::PageRank {
            ProjectionLayout::Outgoing
        } else {
            ProjectionLayout::Undirected
        };
        assert!(try_projected_graph_with_node_filter(
            &fixture.catalog,
            &fixture,
            &[],
            &[],
            |_| true,
            layout,
            ProjectionMemoryBudget::new(nz(budget))
        )
        .is_err());
        let output = run(&fixture, &options, None);
        assert_eq!(output.result.unwrap(), BatchControl::Continue);
        let rows: Vec<_> = output.batches.into_iter().flatten().collect();
        assert_eq!(
            rows.len(),
            if algorithm == GraphAlgorithmKind::PageRank {
                128
            } else {
                256
            }
        );
        for row in rows {
            if algorithm == GraphAlgorithmKind::PageRank {
                let Value::Float(score) = row.values[&options.score_column] else {
                    panic!("missing score")
                };
                assert!((score - 1.0 / 128.0).abs() < 1e-14);
            } else {
                assert_eq!(row.values["louvain_id"], Value::Int(0));
            }
        }
        assert!(fixture.adjacency_visits.get() > fixture.relationships.len());
        let report = &output.reports.blocking_memory[0];
        assert_eq!(report.operator, "GraphAlgorithmStreaming");
        assert!(report.peak_tracked_bytes <= budget);
        assert!(output.peak_bytes <= budget);
        assert_eq!(report.spilled_bytes, 0);
    }
}

#[test]
fn cancellation_and_storage_failure_emit_no_partial_algorithm_results() {
    for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
        for fail_at in [0, 127, 2048] {
            let mut fixture = dense_fixture();
            let mut options = RunOptions {
                algorithm,
                ..RunOptions::default()
            };
            options.memory.blocking_operator_bytes = nz(96 * 1024);
            fixture.fail_adjacency_at = Some(fail_at);
            let output = run_external(&fixture, &options, None);
            assert!(output
                .result
                .unwrap_err()
                .to_string()
                .contains("adjacency scan sentinel"));
            assert!(output.batches.is_empty());
            let token = RuntimeCancellationToken::new();
            let task = RuntimeTaskContext::without_deadline(token.clone());
            fixture.fail_adjacency_at = None;
            fixture.adjacency_visits.set(0);
            fixture.cancel_adjacency_at = Some((fail_at, token));
            let output = run_external(&fixture, &options, Some(&task));
            assert!(output
                .result
                .unwrap_err()
                .to_string()
                .contains("runtime task stopped: cancelled"));
            assert!(output.batches.is_empty());
        }
    }
}

#[test]
fn node_state_and_oversized_records_remain_fail_closed() {
    let mut fixture = Fixture::new();
    let mut options = RunOptions::default();
    options.memory.blocking_operator_bytes = nz(512);
    let output = run_external(&fixture, &options, None);
    assert!(output
        .result
        .unwrap_err()
        .to_string()
        .contains("streaming node scan"));
    assert!(output.batches.is_empty());
    options.memory.blocking_operator_bytes = nz(4096);
    fixture.relationships[0]
        .properties
        .insert("large".into(), Value::String("x".repeat(8192)));
    let output = run_external(&fixture, &options, None);
    assert!(output
        .result
        .unwrap_err()
        .to_string()
        .contains("adjacency record"));
    assert!(output.batches.is_empty());
}
