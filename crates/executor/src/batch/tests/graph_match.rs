use super::*;
use hawdb_plan::LogicalPlan;

// The oracle lowers the generic operators directly, independently of optimizer fast paths.
fn lower(plan: LogicalPlan) -> PhysicalPlan {
    match plan {
        LogicalPlan::GraphMatch { program, input } => PhysicalPlan::GraphMatchExec {
            program,
            input: input.map(|input| Box::new(lower(*input))),
        },
        LogicalPlan::Project { items, input } => PhysicalPlan::ProjectExec {
            items,
            input: Box::new(lower(*input)),
        },
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => PhysicalPlan::AggregateExec {
            group_keys,
            items,
            input: Box::new(lower(*input)),
        },
        LogicalPlan::Filter { predicate, input } => PhysicalPlan::FilterExec {
            predicate,
            input: Box::new(lower(*input)),
        },
        LogicalPlan::Sort { items, input } => PhysicalPlan::SortExec {
            items,
            input: Box::new(lower(*input)),
        },
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => PhysicalPlan::LimitExec {
            offset,
            limit,
            input: Box::new(lower(*input)),
        },
        LogicalPlan::Distinct { input } => PhysicalPlan::DistinctExec {
            input: Box::new(lower(*input)),
        },
        _ => panic!("unexpected generic plan: {plan:?}"),
    }
}

fn execute(query: &str) -> Result<Vec<Binding>> {
    let logical = hawdb_plan::plan_pipeline_query(query, &BTreeMap::new())?;
    execute_logical(logical)
}

fn execute_logical(logical: LogicalPlan) -> Result<Vec<Binding>> {
    let plan = lower(logical);
    with_context(None, |context| {
        let mut rows = Vec::new();
        execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |batch| {
            rows.extend(batch);
            Ok(BatchControl::Continue)
        })?;
        Ok(rows)
    })
}

#[test]
fn repeated_with_preserves_aliases_graph_identity_and_scalar_shadowing() {
    let rows = execute("MATCH (n:Memory) WITH n AS renamed, n.id AS score WITH renamed, score AS n MATCH (renamed)-[:MENTIONS]->(m:Memory) RETURN id(renamed) AS identity, n, m.id AS target").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values,
        BTreeMap::from([
            ("identity".into(), Value::Int(0)),
            ("n".into(), Value::Int(1)),
            ("target".into(), Value::Int(2))
        ])
    );
    for query in [
        "MATCH (n:Memory) WITH n.id AS value RETURN n",
        "MATCH (n:Memory) WITH n.id AS n MATCH (n) RETURN n",
    ] {
        assert!(execute(query).is_err(), "{query}");
    }
}

#[test]
fn grouped_nodes_are_restored_before_later_matches() {
    let rows = execute("MATCH (n:Memory) WITH n AS anchor, COUNT(n) AS seen OPTIONAL MATCH (anchor)-[:MENTIONS]->(m:Memory) WITH anchor, seen, COUNT(m) AS links RETURN anchor.id AS id, seen, links ORDER BY id").unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.values["links"].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(0)]
    );
    assert_eq!(rows[0].values["id"], Value::Int(1));
    assert_eq!(rows[1].values["id"], Value::Int(2));
    assert!(rows.iter().all(|row| row.values["seen"] == Value::Int(1)));
}

#[test]
fn optional_where_covers_the_complete_pattern_and_preserves_null_entities() {
    let rows = execute("MATCH (n:Memory) OPTIONAL MATCH (n)-[r:MENTIONS]->(m:Memory) WHERE m.id = 99 RETURN n.id AS id, m, id(m) AS identity, type(r) AS kind ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        for key in ["m", "identity", "kind"] {
            assert_eq!(row.values[key], Value::Null);
        }
    }
    let rows = execute("MATCH (n:Memory) OPTIONAL MATCH (n)-[:MENTIONS]->(m:Memory), (x:Missing) RETURN n.id AS id, m, x ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .all(|row| row.values["m"] == Value::Null && row.values["x"] == Value::Null));
}

#[test]
fn optional_node_correlation_is_not_a_query_template() {
    let rows = execute("MATCH (left:Memory) OPTIONAL MATCH (right:Memory) WHERE right.id = left.id WITH left AS kept, COUNT(right) AS same OPTIONAL MATCH (kept)-[:MENTIONS]->(linked:Memory) WITH kept, same, COUNT(linked) AS linked_count OPTIONAL MATCH (kept)-[:ABSENT]->(missing:Memory) RETURN kept.id AS id, same, linked_count, COUNT(missing) AS absent ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .all(|row| row.values["same"] == Value::Int(1) && row.values["absent"] == Value::Int(0)));
    assert_eq!(rows[0].values["linked_count"], Value::Int(1));
    assert_eq!(rows[1].values["linked_count"], Value::Int(0));
}

#[test]
fn scalar_columns_aggregate_without_stale_graph_bindings() {
    let rows = execute("MATCH (n:Memory) WITH n.id AS n RETURN COUNT(n) + COUNT(DISTINCT n) AS total, COLLECT(n) AS values").unwrap();
    assert_eq!(rows[0].values["total"], Value::Int(4));
    assert_eq!(
        rows[0].values["values"],
        Value::List(vec![Value::Int(1), Value::Int(2)])
    );
    let rows = execute("MATCH (n:Missing) RETURN COUNT(n) + 1 AS total").unwrap();
    assert_eq!(rows[0].values["total"], Value::Int(1));
}

#[test]
fn relationship_aliases_are_restored_after_grouping() {
    let rows = execute("MATCH (n:Memory)-[r:MENTIONS]->(m:Memory) WITH r AS edge, COUNT(m) AS seen RETURN id(edge) AS id, type(edge) AS kind, seen").unwrap();
    assert_eq!(rows[0].values["id"], Value::Int(0));
    assert_eq!(rows[0].values["kind"], Value::String("MENTIONS".into()));
    assert_eq!(rows[0].values["seen"], Value::Int(1));
}

#[test]
fn optional_null_import_never_matches_real_node_zero() {
    let rows = execute("MATCH (n:Memory) OPTIONAL MATCH (n)-[:ABSENT]->(m:Memory) WITH m AS kept MATCH (kept) RETURN kept").unwrap();
    assert!(rows.is_empty());
    let rows = execute("MATCH (n:Memory) OPTIONAL MATCH (n)-[:ABSENT]->(m:Memory) WITH m AS kept OPTIONAL MATCH (kept)-[:MENTIONS]->(target:Memory) RETURN COUNT(target) AS count").unwrap();
    assert_eq!(rows[0].values["count"], Value::Int(0));
}

#[test]
fn relationship_uniqueness_is_local_to_each_match_clause() {
    let rows = execute(
        "MATCH (a:Memory)-[:MENTIONS]-(b:Memory)-[:MENTIONS]-(c:Memory) RETURN COUNT(a) AS count",
    )
    .unwrap();
    assert_eq!(rows[0].values["count"], Value::Int(0));
    let rows = execute("MATCH (a:Memory)-[:MENTIONS]-(b:Memory) MATCH (b)-[:MENTIONS]-(c:Memory) RETURN COUNT(a) AS count").unwrap();
    assert_eq!(rows[0].values["count"], Value::Int(2));
}

#[test]
fn scalar_property_predicates_do_not_read_shadowed_nodes() {
    let parameters = BTreeMap::from([(
        "replacement".into(),
        Value::Map(BTreeMap::from([("id".into(), Value::Int(99))])),
    )]);
    let plan = lower(
        hawdb_plan::plan_pipeline_query(
            "MATCH (n:Memory) WITH $replacement AS n WHERE n.id = 99 RETURN n.id AS id",
            &parameters,
        )
        .unwrap(),
    );
    let rows = with_context(None, |context| {
        let mut rows = Vec::new();
        execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |batch| {
            rows.extend(batch);
            Ok(BatchControl::Continue)
        })
        .unwrap();
        rows
    });
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.values["id"] == Value::Int(99)));
}

#[test]
fn match_propagates_errors_cancellation_and_downstream_stop() {
    let query = "MATCH (n:Memory) OPTIONAL MATCH (m:Memory) WHERE lower(m.id) = 'x' RETURN m";
    let error = execute(query).unwrap_err();
    assert!(error
        .to_string()
        .contains("LOWER expression requires a string"));
    let plan = lower(
        hawdb_plan::plan_pipeline_query("MATCH (n:Memory) RETURN n", &BTreeMap::new()).unwrap(),
    );
    let token = hawdb_core::RuntimeCancellationToken::new();
    token.cancel();
    let task = RuntimeTaskContext::without_deadline(token);
    with_context(Some(&task), |context| {
        let result =
            execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |_| {
                panic!("cancelled query emitted")
            });
        assert!(result.unwrap_err().to_string().contains("cancelled"));
    });
    with_context(None, |context| {
        let memory = ExecutionMemoryConfig {
            batch_rows: std::num::NonZeroUsize::new(1).unwrap(),
            ..context.memory.clone()
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let context = BatchReadContext {
            memory: &memory,
            memory_ledger: &ledger,
            ..context
        };
        for fail in [false, true] {
            let mut calls = 0;
            let result =
                execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |_| {
                    calls += 1;
                    if fail {
                        Err(HawDBError::Execution("consumer failure".into()))
                    } else {
                        Ok(BatchControl::Stop)
                    }
                });
            assert_eq!(calls, 1);
            assert_eq!(result.is_err(), fail);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    });
}

#[test]
fn match_respects_state_memory_admission() {
    let plan = lower(
        hawdb_plan::plan_pipeline_query("MATCH (n:Memory) RETURN n", &BTreeMap::new()).unwrap(),
    );
    with_context(None, |context| {
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: std::num::NonZeroUsize::new(1).unwrap(),
            ..context.memory.clone()
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let context = BatchReadContext {
            memory: &memory,
            memory_ledger: &ledger,
            ..context
        };
        let result =
            execute_binding_batches(&plan, context, ExecutionLimit::unlimited(), &mut |_| {
                panic!("over-budget query emitted")
            });
        assert!(result.is_err());
        assert_eq!(ledger.snapshot().used_bytes, 0);
    });
}

#[test]
fn visibility_filters_inside_optional_matching_and_survives_with() {
    let query = "MATCH (n:Memory) WITH n AS anchor OPTIONAL MATCH (anchor)-[:MENTIONS]->(m:Memory) RETURN anchor.id AS id, m";
    let logical = hawdb_plan::plan_pipeline_query(query, &BTreeMap::new()).unwrap();
    let visible =
        hawdb_plan::apply_node_visibility_predicates(logical, &|variable| Predicate::PropertyIn {
            variable: variable.to_string(),
            property: "id".to_string(),
            values: vec![Value::Int(1)],
        });
    let rows = execute_logical(visible).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["id"], Value::Int(1));
    assert_eq!(rows[0].values["m"], Value::Null);
}

#[test]
fn ordering_can_read_unreturned_input_columns_without_exposing_them() {
    for query in [
        "MATCH (n:Memory) RETURN n.id + 1 AS result ORDER BY n.id DESC",
        "MATCH (n:Memory) WITH n.id + 1 AS score WITH score AS result ORDER BY score DESC RETURN result",
    ] {
        let rows = execute(query).unwrap();
        assert_eq!(rows.iter().map(|row| row.values["result"].clone()).collect::<Vec<_>>(), vec![Value::Int(3), Value::Int(2)]);
        assert!(rows.iter().all(|row| row.values.len() == 1));
    }
    let rows = execute("MATCH (n:Memory) RETURN n.id AS id, COUNT(n) AS count ORDER BY CASE WHEN n.id = 1 THEN 3 ELSE 0 END DESC").unwrap();
    assert_eq!(rows[0].values["id"], Value::Int(1));
    assert!(rows
        .iter()
        .all(|row| row.values.len() == 2 && row.values["count"] == Value::Int(1)));
    assert!(execute("MATCH (n:Memory) RETURN DISTINCT 1 AS result ORDER BY n.id").is_err());
}

#[test]
fn repeated_default_projection_names_preserve_each_result_value() {
    let rows =
        execute("MATCH (n:Memory) RETURN COALESCE(n.id, 0), COALESCE(n.absent, 0) ORDER BY n.id")
            .unwrap();
    assert_eq!(rows.len(), 2);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.values.len(), 2);
        assert_eq!(row.values["coalesce"], Value::Int(index as i64 + 1));
        assert_eq!(row.values["coalesce#2"], Value::Int(0));
    }
}

#[test]
fn match_expansion_checks_cancellation_before_rejecting_target_labels() {
    use crate::traversal::{visit_one_hop_relationships_with_context, OneHopRelationshipSpec};
    use hawdb_storage::{AdjacencyDirection, RelRecord};
    with_context(None, |context| {
        let token = hawdb_core::RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(token.clone());
        let mut relationships: Vec<RelRecord> = Vec::new();
        context
            .store
            .visit_adjacent_relationships_owned(
                NodeId(0),
                None,
                AdjacencyDirection::Outgoing,
                &mut |edge| {
                    relationships.push(edge);
                    Ok(ScanControl::Continue)
                },
            )
            .unwrap();
        let store = super::store::ReadFixture {
            nodes: vec![context.store.node_owned(NodeId(1)).unwrap().unwrap()],
            relationships,
            adjacency_cancellation: Some(token),
            ..super::store::ReadFixture::default()
        };
        let result = visit_one_hop_relationships_with_context(
            &store,
            OneHopRelationshipSpec {
                source: NodeId(0),
                rel_type_id: None,
                target_label_ids: Some(&[]),
                rel_properties: &BTreeMap::new(),
                relationship_scan_filter: None,
                direction: RelationshipDirection::Outgoing,
            },
            crate::store::AdjacencyReadMemory {
                budget_bytes: 4096,
                account: None,
            },
            context.observer,
            Some(&task),
            &mut |_, _| panic!("a filtered target must never emit"),
        );
        assert!(result.unwrap_err().to_string().contains("cancelled"));
    });
}

#[test]
fn consecutive_optional_clauses_preserve_cartesian_multiplicity() {
    with_context(None, |context| {
        let mut catalog = Catalog::default();
        let label = catalog.get_or_create_label("Item");
        let outgoing = catalog.get_or_create_rel_type("OUT");
        let incoming = catalog.get_or_create_rel_type("IN");
        let store = super::store::ReadFixture {
            nodes: (0..6)
                .map(|id| NodeRecord {
                    id: NodeId(id),
                    labels: [label].into_iter().collect(),
                    properties: BTreeMap::from([("id".into(), Value::Int(id as i64))]),
                })
                .collect(),
            relationships: (1..6)
                .map(|id| hawdb_storage::RelRecord {
                    id: hawdb_storage::RelId(id),
                    source: NodeId(if id <= 2 { 0 } else { id }),
                    target: NodeId(if id <= 2 { id } else { 0 }),
                    rel_type: if id <= 2 { outgoing } else { incoming },
                    properties: BTreeMap::new(),
                })
                .collect(),
            ..super::store::ReadFixture::default()
        };
        let context = BatchReadContext {
            catalog: &catalog,
            store: &store,
            ..context
        };
        let run = |plan: &PhysicalPlan| {
            let mut rows = Vec::new();
            execute_binding_batches(plan, context, ExecutionLimit::unlimited(), &mut |batch| {
                rows.extend(batch);
                Ok(BatchControl::Continue)
            })
            .unwrap();
            assert_eq!(rows.len(), 1);
            rows[0].values["total"].clone()
        };
        for (distinct, expected) in [("", 12), ("DISTINCT ", 5)] {
            let query = format!("MATCH (n:Item {{id: 0}}) OPTIONAL MATCH (n)-[left:OUT]->() OPTIONAL MATCH (n)<-[right:IN]-() RETURN COUNT({distinct}left) + COUNT({distinct}right) AS total");
            let plan = lower(hawdb_plan::plan_pipeline_query(&query, &BTreeMap::new()).unwrap());
            assert_eq!(run(&plan), Value::Int(expected));
        }
    });
}
