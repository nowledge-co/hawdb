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
        return_node_identity: options.return_node_identity,
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

fn predicate_fixture(prefilter: bool) -> Fixture {
    use hawdb_storage::projection::ProjectedRelationshipPredicate as Filter;
    let mut fixture = dense_fixture();
    for node in &mut fixture.nodes {
        node.properties
            .insert("id".into(), Value::String(format!("memory-{}", node.id.0)));
    }
    for relationship in &mut fixture.relationships {
        let active = !relationship.target.0.is_multiple_of(13);
        let confidence =
            if relationship.source.0.is_multiple_of(7) && relationship.target.0.is_multiple_of(3) {
                0.1
            } else {
                0.9
            };
        relationship.properties = BTreeMap::from([
            ("active".into(), Value::Bool(active)),
            ("confidence".into(), Value::Float(confidence)),
        ]);
    }
    if prefilter {
        fixture.relationships.retain(|relationship| {
            !relationship.target.0.is_multiple_of(13)
                && !(relationship.source.0.is_multiple_of(7)
                    && relationship.target.0.is_multiple_of(3))
        });
    } else {
        fixture.definition.as_mut().unwrap().relationship_predicates = BTreeMap::from([(
            "LINK".into(),
            Filter::And(vec![
                Filter::Eq {
                    property: "active".into(),
                    value: Value::Bool(true),
                },
                Filter::Gte {
                    property: "confidence".into(),
                    value: Value::Float(0.5),
                },
            ]),
        )]);
    }
    fixture
}

#[test]
fn resident_and_streaming_preserve_predicates_options_and_complete_identities() {
    let fixture = predicate_fixture(false);
    let control = predicate_fixture(true);
    assert!(control.relationships.len() < fixture.relationships.len());
    for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
        let mut options = RunOptions {
            algorithm,
            return_node_identity: true,
            ..RunOptions::default()
        };
        options.options.tolerance = Some(0.2);
        options.options.normalize_initial = Some(false);
        options.options.resolution = Some(0.5);
        options.memory.batch_rows = nz(7);
        options.memory.blocking_operator_bytes = nz(16 * 1024 * 1024);
        options.memory.query_memory_bytes = nz(32 * 1024 * 1024);
        let resident = run(&fixture, &options, None);
        assert_eq!(resident.result.unwrap(), BatchControl::Continue);
        assert_eq!(
            resident.reports.blocking_memory[0].operator,
            "GraphAlgorithm"
        );
        let expected = run(&control, &options, None);
        assert_eq!(expected.result.unwrap(), BatchControl::Continue);
        let resident: Vec<_> = resident.batches.into_iter().flatten().collect();
        let expected: Vec<_> = expected.batches.into_iter().flatten().collect();
        assert_eq!(resident, expected);
        options.memory.blocking_operator_bytes = nz(96 * 1024);
        let external = run(&fixture, &options, None);
        assert_eq!(external.result.unwrap(), BatchControl::Continue);
        assert_eq!(
            external.reports.blocking_memory[0].operator,
            "GraphAlgorithmStreaming"
        );
        assert!(external.reports.blocking_memory[0].peak_tracked_bytes <= 96 * 1024);
        let external: Vec<_> = external.batches.into_iter().flatten().collect();
        assert_eq!(external, resident);
        assert_eq!(
            external.len(),
            128,
            "stationary Louvain phases must not duplicate assignments"
        );
        for row in external {
            let Value::Int(node) = row.values["node"] else {
                panic!("missing node")
            };
            assert_eq!(
                row.values["node_id"],
                Value::String(format!("memory-{node}"))
            );
            assert_eq!(row.values["node_label"], Value::String("Memory".into()));
        }
    }
}

#[test]
fn late_streaming_identity_failure_emits_no_prefix() {
    let mut fixture = predicate_fixture(false);
    fixture.fail_identity_node = Some(NodeId(127));
    for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
        let mut options = RunOptions {
            algorithm,
            return_node_identity: true,
            ..RunOptions::default()
        };
        options.memory.blocking_operator_bytes = nz(96 * 1024);
        let output = run(&fixture, &options, None);
        assert!(output
            .result
            .unwrap_err()
            .to_string()
            .contains("identity lookup sentinel"));
        assert!(output.batches.is_empty());
        assert_eq!(
            output.reports.blocking_memory[0].operator,
            "GraphAlgorithmStreaming"
        );
    }
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
            128,
            "stationary Louvain phases must not duplicate assignments"
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
fn source_memory_streaming_fallback_identity_uses_live_query_allowance() {
    let mut fixture = dense_fixture();
    let large_id = "x".repeat(1024 * 1024);
    for node in &mut fixture.nodes {
        node.properties.insert(
            "id".into(),
            Value::String(if node.id == NodeId(0) {
                large_id.clone()
            } else {
                format!("memory-{}", node.id.0)
            }),
        );
    }
    for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
        let mut options = RunOptions {
            algorithm,
            return_node_identity: true,
            ..RunOptions::default()
        };
        options.options.max_iterations = Some(1);
        options.options.max_levels = Some(1);
        options.memory.query_memory_bytes = nz(32 * 1024 * 1024);
        options.memory.blocking_operator_bytes = nz(96 * 1024);
        options.memory.batch_payload_bytes = nz(2 * 1024 * 1024);
        options.memory.batch_rows = nz(1);
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
            ProjectionMemoryBudget::new(options.memory.blocking_operator_bytes),
        )
        .is_err());
        // Exercise the automatic resident-to-streaming fallback, rather than
        // calling the streaming implementation directly.
        let output = run(&fixture, &options, None);
        assert_eq!(output.result.unwrap(), BatchControl::Continue);
        let report = &output.reports.blocking_memory[0];
        assert_eq!(report.operator, "GraphAlgorithmStreaming");
        assert!(report.peak_tracked_bytes <= 96 * 1024);
        assert!(output.peak_bytes > large_id.len());
        assert!(output
            .live_bytes
            .iter()
            .all(|bytes| *bytes >= large_id.len()));
        let rows: Vec<_> = output.batches.into_iter().flatten().collect();
        assert_eq!(rows.len(), 128);
        for row in rows {
            let Value::Int(node) = row.values["node"] else {
                panic!("missing node")
            };
            assert_eq!(
                row.values["node_id"],
                Value::String(if node == 0 {
                    large_id.clone()
                } else {
                    format!("memory-{node}")
                }),
            );
            assert_eq!(row.values["node_label"], Value::String("Memory".into()));
        }
        options.memory.batch_payload_bytes = nz(64 * 1024);
        let output = run(&fixture, &options, None);
        let error = output.result.unwrap_err();
        assert!(error.to_string().contains("batch_payload_bytes"), "{error}");
        assert!(output.batches.is_empty());
        assert_eq!(
            output.reports.blocking_memory[0].operator,
            "GraphAlgorithmStreaming"
        );
        options.memory.batch_payload_bytes = nz(2 * 1024 * 1024);
        fixture.fail_identity_node = Some(NodeId(127));
        let output = run(&fixture, &options, None);
        let error = output.result.unwrap_err();
        assert!(
            error.to_string().contains("identity lookup sentinel"),
            "{error}"
        );
        assert!(output.batches.is_empty());
        fixture.fail_identity_node = None;
    }
}

#[test]
fn source_memory_streaming_identity_grants_release_on_consumer_exit() {
    let mut fixture = dense_fixture();
    fixture.nodes[0]
        .properties
        .insert("id".into(), Value::String("x".repeat(1024 * 1024)));
    for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
        for exit in [Exit::Stop, Exit::Error] {
            let mut options = RunOptions {
                algorithm,
                return_node_identity: true,
                exit,
                ..RunOptions::default()
            };
            options.options.max_iterations = Some(1);
            options.options.max_levels = Some(1);
            options.memory.query_memory_bytes = nz(32 * 1024 * 1024);
            options.memory.blocking_operator_bytes = nz(96 * 1024);
            options.memory.batch_payload_bytes = nz(2 * 1024 * 1024);
            options.memory.batch_rows = nz(1);
            let output = run(&fixture, &options, None);
            match exit {
                Exit::Stop => assert_eq!(output.result.unwrap(), BatchControl::Stop),
                Exit::Error => {
                    let error = output.result.unwrap_err();
                    assert!(error.to_string().contains("consumer sentinel"), "{error}");
                }
                Exit::Complete => unreachable!(),
            }
            assert_eq!(output.batches.len(), 1);
            assert_eq!(output.batches[0].len(), 1);
            assert!(output.live_bytes[0] >= 1024 * 1024);
            assert_eq!(
                output.reports.blocking_memory[0].operator,
                "GraphAlgorithmStreaming"
            );
            assert!(output.reports.blocking_memory[0].peak_tracked_bytes <= 96 * 1024);
            // run() independently requires every query class to return to zero.
        }
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
    assert!(output.result.unwrap_err().to_string().contains("exceeding"));
    assert!(output.batches.is_empty());
    options.memory.blocking_operator_bytes = nz(4096);
    fixture.relationships[0]
        .properties
        .insert("large".into(), Value::String("x".repeat(8192)));
    let output = run_external(&fixture, &options, None);
    assert!(
        output.result.is_ok(),
        "source record fits shared root: {:?}",
        output.result
    );
    assert!(!output.batches.is_empty());
    assert!(output
        .reports
        .blocking_memory
        .iter()
        .all(|report| report.peak_tracked_bytes <= 4096));
    options.memory.query_memory_bytes = nz(8192);
    let output = run_external(&fixture, &options, None);
    assert!(output.result.unwrap_err().to_string().contains("exceeding"));
    assert!(output.batches.is_empty());
}

#[test]
fn streaming_visibility_admits_filtered_node_before_copy() {
    let mut fixture = Fixture::new();
    fixture.nodes[0]
        .properties
        .insert("body".into(), Value::String("X".repeat(1024 * 1024)));
    fixture.nodes[0]
        .properties
        .insert("visible".into(), Value::Bool(false));
    let mut options = RunOptions {
        predicate: Some(visibility()),
        ..RunOptions::default()
    };
    options.memory.blocking_operator_bytes = nz(4096);
    let output = run_external(&fixture, &options, None);
    assert!(output.result.is_err());
    assert_eq!(
        fixture.node_visits.get(),
        0,
        "filtered node copied before admission"
    );
    assert!(output.batches.is_empty());
}

#[test]
fn streaming_adjacency_admits_rejected_record_before_copy() {
    let mut fixture = Fixture::new();
    fixture.relationships[0]
        .properties
        .insert("body".into(), Value::String("X".repeat(1024 * 1024)));
    fixture
        .definition
        .as_mut()
        .unwrap()
        .relationship_predicates
        .insert(
            "LINK".into(),
            hawdb_storage::projection::ProjectedRelationshipPredicate::Eq {
                property: "active".into(),
                value: Value::Bool(true),
            },
        );
    let output = run_external(&fixture, &RunOptions::default(), None);
    assert!(output.result.is_err());
    assert_eq!(
        fixture.adjacency_visits.get(),
        0,
        "relationship copied before admission"
    );
    assert!(output.batches.is_empty());
}

#[test]
fn streaming_node_cancellation_stops_before_next_source_copy() {
    let mut fixture = Fixture::new();
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    fixture.cancel_node_at = Some((1, token));
    let output = run_external(
        &fixture,
        &RunOptions {
            predicate: Some(visibility()),
            ..RunOptions::default()
        },
        Some(&task),
    );
    assert!(
        matches!(output.result, Err(HawDBError::Execution(ref message)) if message.contains("runtime task stopped: cancelled"))
    );
    assert_eq!(
        fixture.node_visits.get(),
        1,
        "cancelled source copied more records"
    );
    assert!(output.batches.is_empty());
}
