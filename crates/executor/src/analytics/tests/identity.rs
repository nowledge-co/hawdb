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

fn all_identity() -> GraphAlgorithmIdentity {
    GraphAlgorithmIdentity {
        node_id: true,
        node_label: true,
        node_labels: true,
    }
}

#[test]
fn identity_898_complete_sorted_labels_preserve_typed_ids_in_both_algorithms_and_paths() {
    let mut fixture = Fixture::new();
    let alpha = fixture.catalog.get_or_create_label("Alpha");
    fixture.nodes[1].labels.insert(alpha);
    fixture.definition.as_mut().unwrap().node_labels = vec!["Memory".into(), "Source".into()];
    fixture.nodes[1]
        .properties
        .insert("id".into(), Value::Int(42));
    fixture.nodes[2].properties.remove("id");
    for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
        let options = RunOptions {
            algorithm,
            identity: Some(all_identity()),
            ..RunOptions::default()
        };
        let mut reference = None;
        for output in [
            run(&fixture, &options, None),
            streaming::run_external(&fixture, &options, None),
        ] {
            assert_eq!(output.result.unwrap(), BatchControl::Continue);
            let rows: Vec<_> = output.batches.into_iter().flatten().collect();
            assert!(!rows.is_empty());
            for row in &rows {
                match row.values["node"] {
                    Value::Int(0) => {
                        assert_eq!(row.values["node_id"], Value::String("node-0".into()));
                        assert_eq!(
                            row.values["node_labels"],
                            Value::List(vec![Value::String("Memory".into())])
                        );
                    }
                    Value::Int(7) => {
                        assert_eq!(row.values["node_id"], Value::Int(42));
                        assert_eq!(row.values["node_label"], Value::String("Memory".into()));
                        assert_eq!(
                            row.values["node_labels"],
                            Value::List(
                                ["Alpha", "Memory", "Source"]
                                    .map(|s| Value::String(s.into()))
                                    .to_vec()
                            )
                        );
                    }
                    Value::Int(-1) => assert_eq!(row.values["node_id"], Value::Null),
                    _ => panic!("unexpected algorithm node"),
                }
            }
            if let Some(expected) = &reference {
                assert_eq!(&rows, expected);
            } else {
                reference = Some(rows);
            }
        }
    }
}

#[test]
fn identity_898_labels_only_never_hydrates_large_ids() {
    let mut fixture = Fixture::new();
    for node in &mut fixture.nodes {
        node.properties
            .insert("id".into(), Value::String("x".repeat(2 * 1024 * 1024)));
    }
    let mut options = RunOptions {
        identity: Some(GraphAlgorithmIdentity {
            node_labels: true,
            ..GraphAlgorithmIdentity::default()
        }),
        ..RunOptions::default()
    };
    for output in [
        run(&fixture, &options, None),
        streaming::run_external(&fixture, &options, None),
    ] {
        assert_eq!(output.result.unwrap(), BatchControl::Continue);
        for row in output.batches.into_iter().flatten() {
            assert!(row.values.contains_key("node_labels"));
            assert!(!row.values.contains_key("node_id"));
            assert!(!row.values.contains_key("node_label"));
        }
    }
    options.identity = Some(all_identity());
    for output in [
        run(&fixture, &options, None),
        streaming::run_external(&fixture, &options, None),
    ] {
        assert!(output.result.is_err());
        assert!(output.batches.is_empty());
    }
}

#[test]
fn identity_898_late_label_payload_and_admission_fail_before_output() {
    let mut fixture = Fixture::new();
    fixture.relationships.clear();
    let wide = fixture.catalog.get_or_create_label(&"Wide".repeat(1024));
    fixture.nodes[2].labels.insert(wide);
    let mut options = RunOptions {
        identity: Some(all_identity()),
        ..RunOptions::default()
    };
    options.memory.batch_rows = nz(1);
    options.memory.batch_payload_bytes = nz(1024);
    for output in [
        run(&fixture, &options, None),
        streaming::run_external(&fixture, &options, None),
    ] {
        let HawDBError::ResourceBudgetExceeded(error) = output.result.unwrap_err() else {
            panic!("expected a typed per-row payload refusal")
        };
        assert_eq!(error.resource, ResourceBudgetKind::MemoryBytes);
        assert_eq!(error.scope, ResourceBudgetScope::Operator);
        assert_eq!(error.used, 0);
        assert_eq!(error.budget, 1024);
        assert!(error.requested > error.budget);
        assert_eq!(
            error.budget_option,
            "ExecutionMemoryConfig.batch_payload_bytes"
        );
        assert_eq!(
            error.memory_class,
            Some(QueryMemoryClass::ResultMaterialization.as_str())
        );
        assert!(error.message.contains("per-row"));
        assert!(output.batches.is_empty());
    }
    options.memory.batch_payload_bytes = nz(64 * 1024);
    options.memory.query_memory_bytes = nz(4096);
    for output in [
        run(&fixture, &options, None),
        streaming::run_external(&fixture, &options, None),
    ] {
        assert!(output.result.is_err());
        assert!(output.batches.is_empty());
    }
}

#[test]
fn identity_898_missing_node_and_cancellation_emit_no_identity_rows() {
    let mut fixture = Fixture::new();
    let options = RunOptions {
        identity: Some(all_identity()),
        ..RunOptions::default()
    };
    fixture.fail_identity_node = Some(NodeId(u64::MAX));
    for output in [
        run(&fixture, &options, None),
        streaming::run_external(&fixture, &options, None),
    ] {
        assert!(output
            .result
            .unwrap_err()
            .to_string()
            .contains("identity lookup sentinel"));
        assert!(output.batches.is_empty());
    }
    fixture.fail_identity_node = None;
    let cancellation = RuntimeCancellationToken::new();
    cancellation.cancel();
    let task = RuntimeTaskContext::without_deadline(cancellation);
    for output in [
        run(&fixture, &options, Some(&task)),
        streaming::run_external(&fixture, &options, Some(&task)),
    ] {
        assert!(output.result.is_err());
        assert!(output.batches.is_empty());
    }
}
