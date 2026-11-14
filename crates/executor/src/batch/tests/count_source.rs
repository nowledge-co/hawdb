// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_core::RuntimeCancellationToken;
use hawdb_storage::{RelId, RelRecord};

#[test]
fn relationship_count_source_releases_admission_on_every_exit() {
    for mode in [
        "complete",
        "stop",
        "error",
        "cancel",
        "mid_cancel",
        "refuse",
    ] {
        let cancellation = RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(cancellation.clone());
        let mut catalog = Catalog::default();
        let label = catalog.get_or_create_label("Entity");
        let rel_type = catalog.get_or_create_rel_type("RELATES_TO");
        let key = Value::String("k".repeat(8192));
        let store = store::ReadFixture {
            nodes: (0..2)
                .map(|id| NodeRecord {
                    id: NodeId(id),
                    labels: [label].into_iter().collect(),
                    properties: BTreeMap::from([
                        ("id".into(), key.clone()),
                        ("body".into(), Value::String("x".repeat(32_768))),
                    ]),
                })
                .collect(),
            relationships: vec![RelRecord {
                id: RelId(0),
                source: NodeId(0),
                target: NodeId(1),
                rel_type,
                properties: BTreeMap::new(),
            }],
            adjacency_cancellation: (mode == "mid_cancel").then(|| cancellation.clone()),
            ..store::ReadFixture::default()
        };
        let plan = PhysicalPlan::OptionalRelationshipCountSumExec {
            variable: "e".into(),
            label: "Entity".into(),
            properties: BTreeMap::from([("id".into(), key)]),
            legs: vec![RelationshipCountLeg {
                rel_type: "RELATES_TO".into(),
                direction: RelationshipDirection::Outgoing,
                distinct: true,
                filter: None,
            }],
            output: "count".into(),
        };
        let memory = ExecutionMemoryConfig {
            query_memory_bytes: NonZeroUsize::new(if mode == "refuse" { 4096 } else { 65_536 })
                .unwrap(),
            batch_payload_bytes: NonZeroUsize::new(4096).unwrap(),
            batch_rows: NonZeroUsize::new(1).unwrap(),
            ..ExecutionMemoryConfig::default()
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        if mode == "cancel" {
            cancellation.cancel();
        }
        let mut emitted = 0;
        let result = with_context(None, |context| {
            dispatch_batch_operator(
                &plan,
                BatchExecution {
                    context: BatchReadContext {
                        catalog: &catalog,
                        store: &store,
                        memory: &memory,
                        memory_ledger: &ledger,
                        task_context: Some(&task),
                        ..context
                    },
                    execution_limit: ExecutionLimit::unlimited(),
                    emit: &mut |batch| {
                        emitted += batch.len();
                        assert_eq!(batch[0].values["count"], Value::Int(1));
                        match mode {
                            "stop" => Ok(BatchControl::Stop),
                            "error" => Err(HawDBError::Execution("consumer refused".into())),
                            _ => Ok(BatchControl::Continue),
                        }
                    },
                },
            )
        });
        match mode {
            "complete" => {
                assert_eq!(result.unwrap(), BatchControl::Continue);
                assert_eq!(emitted, 1);
            }
            "stop" => {
                assert_eq!(result.unwrap(), BatchControl::Stop);
                assert_eq!(emitted, 1);
            }
            "error" => {
                assert!(result.is_err());
                assert_eq!(emitted, 1);
            }
            "cancel" | "mid_cancel" => {
                assert!(
                    result.unwrap_err().to_string().contains("cancelled"),
                    "{mode}"
                );
                assert_eq!(emitted, 0);
            }
            "refuse" => {
                let Err(HawDBError::ResourceBudgetExceeded(error)) = result else {
                    panic!("expected source refusal, got {result:?}");
                };
                assert_eq!(error.owner, "OptionalRelationshipCountSumExec");
                assert_eq!(emitted, 0);
            }
            _ => unreachable!(),
        }
        assert_eq!(ledger.snapshot().used_bytes, 0, "{mode}");
    }
}
