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
use crate::external::NoExternalReadOperator;
use hawdb_plan::GraphAlgorithmKind;
use std::cell::Cell;

mod boundaries;
mod dispatch;
mod fixtures;
mod graph_match;
mod handlers;
mod store;

fn with_context<T>(
    task_context: Option<&RuntimeTaskContext>,
    run: impl FnOnce(BatchReadContext<'_>) -> T,
) -> T {
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Memory");
    let rel_type = catalog.get_or_create_rel_type("MENTIONS");
    let store = store::ReadFixture {
        nodes: (0..2)
            .map(|id| NodeRecord {
                id: NodeId(id),
                labels: [label].into_iter().collect(),
                properties: BTreeMap::from([("id".into(), Value::Int(id as i64 + 1))]),
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
            node_labels: vec!["Memory".into()],
            rel_types: vec!["MENTIONS".into()],
        }),
        out_of_core: false,
        ..store::ReadFixture::default()
    };
    let memory = ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let parameters = BTreeMap::new();
    let mut external = NoExternalReadOperator;
    let external = BatchExternalReadAdapter::new(&mut external);
    let observer = QueryExecutionObserver::default();
    let result = run(BatchReadContext {
        catalog: &catalog,
        store: &store,
        memory: &memory,
        memory_ledger: &ledger,
        parameters: &parameters,
        external: &external,
        observer: &observer,
        task_context,
    });
    assert_eq!(ledger.snapshot().used_bytes, 0);
    result
}

#[test]
fn support_probe_does_not_execute_or_capture_runtime_state() {
    let called = Cell::new(false);
    assert!(BatchSupport.supported(|_, _, _| {
        called.set(true);
        panic!("a capability probe must not execute its handler")
    }));
    assert!(!called.get());
    for (plan, supported) in fixtures::operators() {
        assert_eq!(dispatch_batch_operator(&plan, BatchSupport), supported);
    }
}

#[test]
fn unsupported_operator_dispatch_returns_an_error_instead_of_panicking() {
    let plan = PhysicalPlan::CreateNode {
        label: "Item".to_string(),
        properties: BTreeMap::new(),
    };
    let error = with_context(None, |context| {
        // Deliberately bypass the proof constructor to exercise defensive dispatch.
        execute_binding_batches_inner(
            BatchPlanRef(&plan),
            context,
            ExecutionLimit::unlimited(),
            &mut |_| panic!("unsupported output"),
        )
    })
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("physical operator 'CreateNode' does not support batch execution"));
}

#[test]
fn graph_handler_checks_cancellation_inside_its_execution_boundary() {
    for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
        let plan = PhysicalPlan::GraphAlgorithm {
            algorithm,
            graph_name: "MemoryGraph".into(),
            options: hawdb_plan::GraphAlgorithmOptions {
                damping: None,
                max_iterations: Some(2),
                max_levels: Some(1),
            },
            score_column: "score".into(),
            node_visibility_predicate: None,
        };
        let cancellation = hawdb_core::RuntimeCancellationToken::new();
        assert!(cancellation.cancel());
        let task = RuntimeTaskContext::without_deadline(cancellation);
        let result = with_context(Some(&task), |context| {
            // Bypass entrypoint checkpoints to pin the extracted handler's own boundary.
            dispatch_batch_operator(
                &plan,
                BatchExecution {
                    context,
                    execution_limit: ExecutionLimit::unlimited(),
                    emit: &mut |_| panic!("cancelled graph handler emitted a row"),
                },
            )
        });
        assert_eq!(
            result.unwrap_err(),
            HawDBError::Execution("runtime task stopped: cancelled".to_string())
        );
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    #[test]
    fn optional_relationship_count_checks_cancellation_without_matching_nodes() {
        let mut catalog = Catalog::default();
        let other = catalog.get_or_create_label("Other");
        let store = store::ReadFixture {
            nodes: vec![NodeRecord {
                id: NodeId(0),
                labels: [other].into_iter().collect(),
                properties: BTreeMap::new(),
            }],
            ..store::ReadFixture::default()
        };
        let plan = PhysicalPlan::OptionalRelationshipCountSumExec {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            properties: BTreeMap::new(),
            legs: vec![RelationshipCountLeg {
                rel_type: "HAS_MEMORY".to_string(),
                direction: RelationshipDirection::Outgoing,
                distinct: false,
                filter: None,
            }],
            output: "count".to_string(),
        };
        let memory = ExecutionMemoryConfig {
            batch_rows: NonZeroUsize::new(1).unwrap(),
            ..ExecutionMemoryConfig::default()
        };
        let memory_ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let parameters = BTreeMap::new();
        let mut external_operator = NoExternalReadOperator;
        let external = BatchExternalReadAdapter::new(&mut external_operator);
        let observer = QueryExecutionObserver::default();
        let cancellation = hawdb_core::RuntimeCancellationToken::new();
        let task_context = RuntimeTaskContext::without_deadline(cancellation.clone());
        assert!(cancellation.cancel());
        let context = BatchReadContext {
            catalog: &catalog,
            store: &store,
            parameters: &parameters,
            external: &external,
            memory: &memory,
            memory_ledger: &memory_ledger,
            task_context: Some(&task_context),
            observer: &observer,
        };

        // Bypass both pipeline checkpoints to isolate the operator's scan loop.
        let error = dispatch_batch_operator(
            &plan,
            BatchExecution {
                context,
                execution_limit: ExecutionLimit::unlimited(),
                emit: &mut |_| Ok(BatchControl::Continue),
            },
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("runtime task stopped: cancelled"));
    }
}

#[cfg(test)]
mod byte_bounded_batch_tests {
    use super::*;

    #[test]
    fn within_budget_batch_keeps_its_allocation() {
        let batch = vec![Binding {
            values: BTreeMap::from([("value".to_string(), Value::Int(1))]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }];
        let allocation = batch.as_ptr();
        let observer = QueryExecutionObserver::default();
        let memory = ExecutionMemoryConfig::default();
        let memory_ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let memory_account = memory_ledger.account(
            QueryMemoryClass::PipelineBatch,
            "test batch",
            memory.query_memory_bytes,
        );
        let mut emitted_allocation = None;

        let control = emit_byte_bounded_batches(
            batch,
            usize::MAX,
            &memory_account,
            &observer,
            &mut |emitted| {
                emitted_allocation = Some(emitted.as_ptr());
                Ok(BatchControl::Continue)
            },
        )
        .unwrap();

        assert_eq!(control, BatchControl::Continue);
        assert_eq!(emitted_allocation, Some(allocation));
        assert_eq!(observer.into_reports().pipeline_memory.intermediate_rows, 1);
    }
}
