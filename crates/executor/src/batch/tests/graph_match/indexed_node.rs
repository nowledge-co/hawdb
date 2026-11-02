// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_core::{PropertyType, RuntimeCancellationToken, TableKind};
use hawdb_storage::store::GraphStore;

#[test]
fn indexed_node_lookup_releases_admission_on_stop_error_cancellation_and_refusal() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    for label in ["Key", "Item"] {
        store.create_node_table(&mut catalog, label).unwrap();
        store
            .create_property_descriptor(
                &mut catalog,
                TableKind::Node,
                label,
                "id",
                PropertyType::String,
                false,
            )
            .unwrap();
    }
    store
        .create_property_descriptor(
            &mut catalog,
            TableKind::Node,
            "Item",
            "active",
            PropertyType::Bool,
            false,
        )
        .unwrap();
    store
        .create_property_descriptor(
            &mut catalog,
            TableKind::Node,
            "Item",
            "body",
            PropertyType::String,
            false,
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Key",
            BTreeMap::from([("id".into(), Value::String("key".into()))]),
        )
        .unwrap();
    for _ in 0..4 {
        store
            .create_node(
                &mut catalog,
                "Item",
                BTreeMap::from([
                    ("id".into(), Value::String("key".into())),
                    ("active".into(), Value::Bool(true)),
                    ("body".into(), Value::String("x".repeat(32_768))),
                ]),
            )
            .unwrap();
    }
    store
        .create_property_index(&mut catalog, "Item", "id")
        .unwrap();
    let plan = lower(hawdb_plan_cypher::plan_pipeline_query(
        "MATCH (source:Key) OPTIONAL MATCH (n:Item {id: source.id, active: true}) RETURN n.id AS id",
        &BTreeMap::new(),
    ).unwrap());
    for mode in ["complete", "stop", "error", "cancel", "refuse"] {
        let token = RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(token.clone());
        let memory = ExecutionMemoryConfig {
            batch_rows: NonZeroUsize::new(1).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(65_536).unwrap(),
            query_memory_bytes: NonZeroUsize::new(if mode == "refuse" {
                16_384
            } else {
                2_097_152
            })
            .unwrap(),
            ..ExecutionMemoryConfig::default()
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let mut emitted = 0;
        let result = with_context(None, |context| {
            execute_binding_batches(
                &plan,
                BatchReadContext {
                    catalog: &catalog,
                    store: &store,
                    memory: &memory,
                    memory_ledger: &ledger,
                    task_context: Some(&task),
                    ..context
                },
                ExecutionLimit::unlimited(),
                &mut |batch| {
                    emitted += batch.len();
                    match mode {
                        "stop" => Ok(BatchControl::Stop),
                        "error" => Err(HawDBError::Execution("consumer refused".into())),
                        "cancel" => {
                            token.cancel();
                            Ok(BatchControl::Continue)
                        }
                        _ => Ok(BatchControl::Continue),
                    }
                },
            )
        });
        match mode {
            "complete" => {
                result.unwrap();
                assert_eq!(emitted, 4);
            }
            "stop" => {
                assert_eq!(result.unwrap(), BatchControl::Stop);
                assert_eq!(emitted, 1);
            }
            "error" | "cancel" => {
                assert!(result.is_err(), "{mode}");
                assert_eq!(emitted, 1);
            }
            "refuse" => {
                let Err(HawDBError::ResourceBudgetExceeded(error)) = result else {
                    panic!("expected source budget refusal, got {result:?}");
                };
                assert_eq!(error.owner, "GraphMatchExec source");
                assert_eq!(emitted, 0);
            }
            _ => unreachable!(),
        }
        assert_eq!(ledger.snapshot().used_bytes, 0, "{mode}");
    }
}
