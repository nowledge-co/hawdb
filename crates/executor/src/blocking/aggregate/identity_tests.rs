// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Identity group keys at the operator boundary (#1615): shared records,
//! exact ledger settlement, divergent records of one entity and the spill
//! key codec.

use super::*;
use crate::expression::with_legacy_map_keys;
use crate::observer::NoopExecutionObserver;
use hawdb_core::{LabelId, RelTypeId};
use hawdb_plan_cypher::ProjectionExpression;
use hawdb_storage::{NodeId, NodeRecord, RelId, RelRecord};
use std::sync::Arc;

struct Rows(Vec<BindingBatch>);

impl BindingBatchSource for Rows {
    fn execute(
        &mut self,
        _input: &PhysicalPlan,
        _execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        for batch in std::mem::take(&mut self.0) {
            if emit(batch)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    }
}

fn catalog() -> (Catalog, LabelId) {
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Item");
    (catalog, label)
}

fn node(id: u64, value: i64) -> NodeRecord {
    NodeRecord {
        id: NodeId(id),
        labels: [catalog().1].into_iter().collect(),
        properties: BTreeMap::from([
            ("v".to_string(), Value::Int(value)),
            ("text".to_string(), Value::String("t".repeat(64))),
        ]),
    }
}

fn row(node: NodeRecord) -> Binding {
    let mut binding = Binding::values(BTreeMap::new());
    binding.nodes.insert("n".to_string(), node);
    binding
}

fn group_by_n(function: AggregateFunction) -> (Vec<Projection>, Vec<Aggregation>) {
    (
        vec![Projection {
            expression: ProjectionExpression::Variable {
                variable: "n".to_string(),
            },
            name: "n".to_string(),
        }],
        vec![Aggregation {
            function,
            target: match function {
                AggregateFunction::Collect => AggregateTarget::Property {
                    variable: "n".to_string(),
                    property: "v".to_string(),
                },
                _ => AggregateTarget::All,
            },
            distinct: false,
            name: "out".to_string(),
        }],
    )
}

struct Run {
    rows: Result<Vec<Binding>>,
    used_bytes: usize,
    peak_bytes: usize,
}

fn run(
    rows: Vec<Binding>,
    function: AggregateFunction,
    graph_keys: GraphGroupKeys,
    memory: &ExecutionMemoryConfig,
) -> Run {
    let (group_keys, items) = group_by_n(function);
    let (catalog, _) = catalog();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut output = Vec::new();
    let result = stream_grouped_aggregate_batches(
        &PhysicalPlan::EmptyExec,
        &group_keys,
        &items,
        graph_keys,
        &mut Rows(rows.chunks(4).map(<[Binding]>::to_vec).collect()),
        BlockingExecutionContext {
            catalog: &catalog,
            memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &NoopExecutionObserver,
            graph: None,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            output.extend(batch);
            Ok(BatchControl::Continue)
        },
    );
    let snapshot = ledger.snapshot();
    Run {
        rows: result.map(|_| output),
        used_bytes: snapshot.used_bytes,
        peak_bytes: snapshot.peak_bytes,
    }
}

#[test]
fn shared_records_leave_uncopied_and_charge_each_group_once() {
    let memory = ExecutionMemoryConfig::default();
    let first = row(node(7, 1));
    for function in [AggregateFunction::Count, AggregateFunction::Collect] {
        for graph_keys in [GraphGroupKeys::Values, GraphGroupKeys::Slots] {
            let context = format!("{function:?} {graph_keys:?}");
            // Every row shares the first row's record, as a match does.
            let single = run(vec![first.clone()], function, graph_keys, &memory);
            let many = run(vec![first.clone(); 64], function, graph_keys, &memory);
            assert_eq!((single.used_bytes, many.used_bytes), (0, 0), "{context}");
            let rows = many.rows.unwrap();
            assert_eq!(rows.len(), 1, "{context}");
            if graph_keys == GraphGroupKeys::Slots {
                let record = rows[0].nodes.get_shared("n").expect("slot output");
                assert!(
                    Arc::ptr_eq(record, first.nodes.get_shared("n").unwrap()),
                    "{context}"
                );
                assert!(!rows[0].values.contains_key("n"), "{context}");
            } else {
                assert!(rows[0].nodes.is_empty(), "{context}");
                assert_eq!(
                    rows[0].values["n"],
                    crate::expression::node_value(first.nodes.get("n").unwrap(), &catalog().0),
                    "{context}"
                );
            }
            // The group state charges the record once, however many rows
            // share it: COUNT state does not grow with the row count.
            if function == AggregateFunction::Count {
                assert_eq!(single.peak_bytes, many.peak_bytes, "{context}");
            }
            // The observed peak is an exact admission boundary.
            let exact = ExecutionMemoryConfig {
                query_memory_bytes: NonZeroUsize::new(many.peak_bytes).unwrap(),
                ..memory.clone()
            };
            let fitted = run(vec![first.clone(); 64], function, graph_keys, &exact);
            assert_eq!(fitted.rows.unwrap(), rows, "{context}");
            assert_eq!((fitted.used_bytes, fitted.peak_bytes), (0, many.peak_bytes));
            let below = ExecutionMemoryConfig {
                query_memory_bytes: NonZeroUsize::new(many.peak_bytes - 1).unwrap(),
                ..memory.clone()
            };
            let refused = run(vec![first.clone(); 64], function, graph_keys, &below);
            // The output batch reports a refused ownership transfer as an
            // execution error that names the query budget.
            let error = refused.rows.unwrap_err();
            assert!(
                matches!(error, HawDBError::ResourceBudgetExceeded(_))
                    || error.to_string().contains(&format!(
                        "exceeding query_memory_bytes {}",
                        many.peak_bytes - 1
                    )),
                "{context}: {error:?}"
            );
            assert_eq!(refused.used_bytes, 0, "{context}");
        }
    }
}

/// Inside one statement, rows of the same entity can carry different records
/// when a write lands between them. Cypher groups by the entity, so these
/// rows form one group that keeps the first row's record; the map key used
/// before #1615 split them into one group per distinct record. The current
/// mutation binder admits no aggregation after a write, so this pins the
/// operator contract that such a plan would inherit.
#[test]
fn divergent_records_of_one_entity_form_one_group_with_the_first_record() {
    let memory = ExecutionMemoryConfig::default();
    let rows = vec![
        row(node(7, 1)),
        row(node(7, 2)),
        row(node(8, 1)),
        row(node(7, 3)),
    ];
    for graph_keys in [GraphGroupKeys::Values, GraphGroupKeys::Slots] {
        let output = run(rows.clone(), AggregateFunction::Count, graph_keys, &memory)
            .rows
            .unwrap();
        let groups = output
            .iter()
            .map(|binding| {
                let record = match graph_keys {
                    GraphGroupKeys::Values => {
                        let Value::Map(values) = &binding.values["n"] else {
                            panic!("expected a node map");
                        };
                        (values["_id"].clone(), values["v"].clone())
                    }
                    _ => {
                        let node = binding.nodes.get("n").unwrap();
                        (Value::Int(node.id.0 as i64), node.properties["v"].clone())
                    }
                };
                (record, binding.values["out"].clone())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            groups,
            vec![
                ((Value::Int(7), Value::Int(1)), Value::Int(3)),
                ((Value::Int(8), Value::Int(1)), Value::Int(1)),
            ],
            "{graph_keys:?}"
        );
    }
    let legacy = with_legacy_map_keys(|| {
        run(
            rows,
            AggregateFunction::Count,
            GraphGroupKeys::Values,
            &memory,
        )
    });
    assert_eq!(legacy.rows.unwrap().len(), 4);
}

#[test]
fn spill_key_codec_round_trips_records_and_refuses_misplaced_slots() {
    let node = Arc::new(node(3, 9));
    let relationship = Arc::new(RelRecord {
        id: RelId(u64::MAX),
        source: NodeId(1),
        target: NodeId(2),
        rel_type: RelTypeId(4),
        properties: BTreeMap::from([("w".to_string(), Value::Float(0.5))]),
    });
    let key = vec![
        GroupKeyValue::Value(Value::Null),
        GroupKeyValue::Node(Arc::clone(&node)),
        GroupKeyValue::Value(Value::List(vec![Value::Null])),
        GroupKeyValue::Relationship(Arc::clone(&relationship)),
    ];
    let encoded = compact::encode_compact_group_binding(key.clone(), vec![AggregateInput::Present]);
    // Through the spill file codec, as a run writes and reads it.
    let encoded = crate::spill::round_trip_binding_for_tests(&encoded).unwrap();
    let decoded = compact::decode_compact_group_binding(5, encoded.clone(), 1).unwrap();
    assert_eq!(decoded.key, key);
    let GroupKeyValue::Node(decoded_node) = &decoded.key[1] else {
        panic!("expected a node key");
    };
    assert_eq!(decoded_node.as_ref(), node.as_ref());
    let GroupKeyValue::Relationship(decoded_relationship) = &decoded.key[3] else {
        panic!("expected a relationship key");
    };
    assert_eq!(decoded_relationship.as_ref(), relationship.as_ref());
    assert_eq!(decoded.key[0], GroupKeyValue::Value(Value::Null));

    for (name, reason) in [
        ("4", "outside its key"),
        ("2", "non-null key value"),
        ("01", "outside"),
    ] {
        let mut invalid = encoded.clone();
        invalid
            .nodes
            .insert(name.to_string(), node.as_ref().clone());
        let error = compact::decode_compact_group_binding(5, invalid, 1)
            .err()
            .expect("misplaced slot");
        assert!(error.to_string().contains(reason), "{name}: {error}");
    }
    let mut collision = encoded;
    collision
        .nodes
        .insert("3".to_string(), node.as_ref().clone());
    assert!(compact::decode_compact_group_binding(5, collision, 1).is_err());
}
