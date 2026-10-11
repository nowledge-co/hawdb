// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;

fn nz(bytes: usize) -> NonZeroUsize {
    NonZeroUsize::new(bytes).unwrap()
}

fn fixture(count: usize) -> (Catalog, store::ReadFixture) {
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Memory");
    let store = store::ReadFixture {
        nodes: (0..count)
            .map(|id| NodeRecord {
                id: NodeId(id as u64),
                labels: [label].into_iter().collect(),
                properties: BTreeMap::from([
                    ("id".into(), Value::Int(id as i64)),
                    ("body".into(), Value::String("x".repeat(8192))),
                ]),
            })
            .collect(),
        ..Default::default()
    };
    (catalog, store)
}

fn run(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    store: &dyn crate::store::GraphExecutionRead,
    memory: &ExecutionMemoryConfig,
    ledger: &QueryMemoryLedger,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let parameters = BTreeMap::from([("query".into(), Value::String("needle".into()))]);
    let mut external = NoExternalReadOperator;
    let external = BatchExternalReadAdapter::new(&mut external);
    execute_binding_batches(
        plan,
        BatchReadContext {
            catalog,
            store,
            memory,
            memory_ledger: ledger,
            parameters: &parameters,
            external: &external,
            observer: &QueryExecutionObserver::new(plan),
            host_scorer: None,
            task_context: None,
        },
        ExecutionLimit::unlimited(),
        emit,
    )
}

fn memory() -> ExecutionMemoryConfig {
    ExecutionMemoryConfig {
        query_memory_bytes: nz(128 * 1024),
        blocking_operator_bytes: nz(512),
        batch_payload_bytes: nz(16 * 1024),
        batch_rows: nz(1),
        ..Default::default()
    }
}

fn source_plan(indexed: bool) -> PhysicalPlan {
    if indexed {
        PhysicalPlan::IndexNodeSeek {
            variable: "n".into(),
            label: "Memory".into(),
            property: "id".into(),
            value: Value::Int(0),
        }
    } else {
        PhysicalPlan::SeqNodeScan {
            variable: "n".into(),
            label: "Memory".into(),
        }
    }
}

fn assert_read_above_blocking_cap(indexed: bool) {
    let (catalog, store) = fixture(1);
    let memory = memory();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut rows = 0;
    run(
        &source_plan(indexed),
        &catalog,
        &store,
        &memory,
        &ledger,
        &mut |batch| {
            rows += batch.len();
            assert_eq!(
                batch[0].nodes["n"].properties["body"],
                Value::String("x".repeat(8192))
            );
            Ok(BatchControl::Continue)
        },
    )
    .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(ledger.snapshot().peak_bytes > memory.blocking_operator_bytes.get());
}

#[test]
fn source_memory_node_scan_reads_above_blocking_cap() {
    assert_read_above_blocking_cap(false);
}

#[test]
fn source_memory_index_seek_reads_above_blocking_cap() {
    assert_read_above_blocking_cap(true);
}

#[test]
fn source_memory_root_refusal_recovers_without_cumulative_read_charge() {
    let (catalog, store) = fixture(32);
    let memory = memory();
    let ledger = QueryMemoryLedger::new(nz(2048));
    let error = run(
        &source_plan(false),
        &catalog,
        &store,
        &memory,
        &ledger,
        &mut |_| panic!("root-refused row must not be emitted"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("query memory ledger"), "{error}");
    assert_eq!(ledger.snapshot().used_bytes, 0);
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut rows = 0;
    run(
        &source_plan(false),
        &catalog,
        &store,
        &memory,
        &ledger,
        &mut |batch| {
            rows += batch.len();
            Ok(BatchControl::Continue)
        },
    )
    .unwrap();
    assert_eq!(rows, 32);
    assert!(rows * 8192 > memory.query_memory_bytes.get());
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(ledger.snapshot().peak_bytes < memory.query_memory_bytes.get());
}

#[test]
fn source_memory_fallback_filter_releases_rejected_source_before_consumer_returns() {
    let (catalog, mut store) = fixture(2);
    store.nodes[0]
        .properties
        .insert("body".into(), Value::String("x".repeat(20 * 1024)));
    store.nodes[1]
        .properties
        .insert("body".into(), Value::String("ok".into()));
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: nz(64 * 1024),
        batch_payload_bytes: nz(64 * 1024),
        batch_rows: nz(256),
        ..memory()
    };
    let plan = PhysicalPlan::FilterExec {
        predicate: Predicate::PropertyCompare {
            variable: "n".into(),
            property: "id".into(),
            op: hawdb_plan_cypher::ComparisonOp::Gte,
            value: Value::Int(1),
        },
        input: Box::new(PhysicalPlan::IndexNodeMultiSeek {
            variable: "n".into(),
            label: "Memory".into(),
            property: "id".into(),
            values: vec![Value::Int(0), Value::Int(1)],
        }),
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let reuse = ledger.account(
        QueryMemoryClass::ExternalRead,
        "post-filter reuse",
        memory.query_memory_bytes,
    );
    let mut rows = 0;
    run(&plan, &catalog, &store, &memory, &ledger, &mut |batch| {
        rows += batch.len();
        assert_eq!(batch[0].nodes["n"].id, NodeId(1));
        assert!(
            ledger.snapshot().used_bytes < 12 * 1024,
            "already rejected 20KiB payload still charged inside active source callback: {:?}",
            ledger.snapshot()
        );
        let reused = reuse
            .reserve(memory.query_memory_bytes.get() - 12 * 1024)
            .unwrap();
        drop(reused);
        Ok(BatchControl::Continue)
    })
    .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(store.node_admissions.get(), 2);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

fn fallback_filter_batching(input_kind: &str) {
    for count in [17usize, 257] {
        let (catalog, mut store) = fixture(count);
        for node in &mut store.nodes {
            node.properties
                .insert("body".into(), Value::String("small".into()));
        }
        let memory = ExecutionMemoryConfig {
            query_memory_bytes: nz(4 * 1024 * 1024),
            blocking_operator_bytes: nz(1024 * 1024),
            batch_payload_bytes: nz(64 * 1024),
            batch_rows: nz(16),
            ..memory()
        };
        let leaf = PhysicalPlan::IndexNodeMultiSeek {
            variable: "n".into(),
            label: "Memory".into(),
            property: "id".into(),
            values: (0..count).map(|id| Value::Int(id as i64)).collect(),
        };
        let input = match input_kind {
            "index" => leaf,
            "projection" => PhysicalPlan::ProjectExec {
                items: vec![Projection {
                    name: "body".into(),
                    expression: hawdb_plan_cypher::ProjectionExpression::Property {
                        variable: "n".into(),
                        property: "body".into(),
                    },
                }],
                input: Box::new(leaf),
            },
            "sort" => PhysicalPlan::SortExec {
                items: Vec::new(),
                input: Box::new(leaf),
            },
            _ => unreachable!(),
        };
        let plan = PhysicalPlan::FilterExec {
            predicate: Predicate::ConstantBool(true),
            input: Box::new(input),
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let mut batches = Vec::new();
        let mut ids = Vec::new();
        run(&plan, &catalog, &store, &memory, &ledger, &mut |batch| {
            batches.push(batch.len());
            ids.extend(batch.iter().map(|row| row.nodes["n"].id));
            Ok(BatchControl::Continue)
        })
        .unwrap();
        let snapshot = ledger.snapshot();
        eprintln!(
            "fallback filter {input_kind}: rows={count}, callbacks={}, accounts={}, peak_bytes={}",
            batches.len(),
            snapshot.account_count,
            snapshot.peak_bytes
        );
        let expected_batches: Vec<_> = (0..count)
            .collect::<Vec<_>>()
            .chunks(memory.batch_rows.get())
            .map(<[usize]>::len)
            .collect();
        assert_eq!(batches, expected_batches, "{input_kind}, rows={count}");
        assert_eq!(
            ids,
            (0..count).map(|id| NodeId(id as u64)).collect::<Vec<_>>()
        );
        assert!(
            snapshot.account_count <= 24 + 3 * count.div_ceil(memory.batch_rows.get()),
            "per-row ledger account growth: {snapshot:?}"
        );
        assert_eq!(snapshot.used_bytes, 0);
    }
}

#[test]
fn source_memory_fallback_filter_batches_index_rows() {
    fallback_filter_batching("index");
}

#[test]
fn source_memory_fallback_filter_batches_projection_rows() {
    fallback_filter_batching("projection");
}

#[test]
fn source_memory_fallback_filter_batches_sort_rows() {
    fallback_filter_batching("sort");
}

#[test]
fn source_memory_graph_seed_rejected_read_does_not_consume_topk_cap() {
    let (catalog, mut store) = fixture(2);
    store.nodes[0]
        .properties
        .insert("body".into(), Value::String("x".repeat(20 * 1024)));
    store.nodes[1]
        .properties
        .insert("body".into(), Value::String("needle".into()));
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: nz(8192),
        ..memory()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let plan = PhysicalPlan::GraphSeedScan {
        query_parameter: "query".into(),
        label: "Memory".into(),
        variable: "n".into(),
        score_column: "score".into(),
        top_k: 1,
        node_visibility_predicate: None,
    };
    let mut ids = Vec::new();
    run(&plan, &catalog, &store, &memory, &ledger, &mut |batch| {
        ids.extend(batch.into_iter().map(|binding| binding.nodes["n"].id));
        Ok(BatchControl::Continue)
    })
    .unwrap();
    assert_eq!(ids, vec![NodeId(1)]);
    assert_eq!(store.node_admissions.get(), 2);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    let retained_peak = ledger
        .snapshot()
        .classes
        .iter()
        .find(|class| class.class == QueryMemoryClass::BlockingState)
        .unwrap()
        .peak_bytes;
    assert!(retained_peak <= memory.blocking_operator_bytes.get());
}

#[test]
fn source_memory_graph_seed_replacement_fits_the_existing_one_candidate_cap() {
    let (catalog, mut store) = fixture(1);
    store.nodes[0]
        .properties
        .insert("id".into(), Value::String("prefix".into()));
    store.nodes[0]
        .properties
        .insert("body".into(), Value::String("NEEDLE".into()));
    let plan = PhysicalPlan::GraphSeedScan {
        query_parameter: "query".into(),
        label: "Memory".into(),
        variable: "n".into(),
        score_column: "score".into(),
        top_k: 1,
        node_visibility_predicate: None,
    };
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: nz(8192),
        ..memory()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    run(&plan, &catalog, &store, &memory, &ledger, &mut |_| {
        Ok(BatchControl::Continue)
    })
    .unwrap();
    let cap = ledger
        .snapshot()
        .classes
        .iter()
        .find(|class| class.class == QueryMemoryClass::BlockingState)
        .unwrap()
        .peak_bytes;
    assert!(cap > 4096 && cap < 8192);
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: nz(cap),
        ..memory
    };
    let mut second = store.nodes[0].clone();
    second.id = NodeId(1);
    second
        .properties
        .insert("id".into(), Value::String("needle".into()));
    second
        .properties
        .insert("body".into(), Value::String("needle".into()));
    store.nodes.push(second);
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut ids = Vec::new();
    run(&plan, &catalog, &store, &memory, &ledger, &mut |batch| {
        ids.extend(batch.into_iter().map(|binding| binding.nodes["n"].id));
        Ok(BatchControl::Continue)
    })
    .unwrap();
    assert_eq!(
        ids,
        vec![NodeId(1)],
        "later exact match must replace the retained weaker candidate"
    );
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn source_memory_retained_transfer_preserves_root_and_refuses_before_mutation() {
    use crate::store::admit_graph_read;
    let ledger = QueryMemoryLedger::new(nz(1024));
    let source = ledger.source_account("retained transfer", nz(1024), nz(900));
    let mut grant = admit_graph_read(&source, None, 900).unwrap();
    let before = ledger.snapshot();
    grant.retain_state().unwrap();
    let after = ledger.snapshot();
    assert_eq!(after.used_bytes, before.used_bytes);
    assert_eq!(after.peak_bytes, before.peak_bytes);
    assert_eq!(
        after
            .classes
            .iter()
            .find(|c| c.class == QueryMemoryClass::BlockingState)
            .unwrap()
            .used_bytes,
        900
    );
    assert_eq!(
        after
            .classes
            .iter()
            .find(|c| c.class == QueryMemoryClass::ExternalRead)
            .unwrap()
            .used_bytes,
        0
    );
    grant.retain_state().unwrap();
    assert_eq!(
        ledger.snapshot(),
        after,
        "already transferred grant must not transfer twice"
    );
    assert!(grant.grow(1).is_err());
    assert_eq!(ledger.snapshot(), after);
    drop(grant);
    assert_eq!(ledger.snapshot().used_bytes, 0);

    let source = ledger.source_account("refused transfer", nz(1024), nz(899));
    let mut grant = admit_graph_read(&source, None, 900).unwrap();
    let before = ledger.snapshot();
    assert!(grant.retain_state().is_err());
    assert_eq!(grant.bytes(), 900);
    assert_eq!(
        ledger.snapshot(),
        before,
        "refusal must preserve the original source ownership"
    );
    drop(grant);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn source_memory_graph_seed_selected_candidate_keeps_retained_cap() {
    let (catalog, mut store) = fixture(1);
    store.nodes[0].properties.insert(
        "body".into(),
        Value::String(format!("needle{}", "x".repeat(20 * 1024))),
    );
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: nz(8192),
        ..memory()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let plan = PhysicalPlan::GraphSeedScan {
        query_parameter: "query".into(),
        label: "Memory".into(),
        variable: "n".into(),
        score_column: "score".into(),
        top_k: 1,
        node_visibility_predicate: None,
    };
    let error = run(&plan, &catalog, &store, &memory, &ledger, &mut |_| {
        panic!("unadmitted retained candidate reached consumer")
    })
    .unwrap_err();
    assert!(
        error.to_string().contains("GraphSeedScan (blocking_state)"),
        "{error}"
    );
    assert_eq!(store.node_admissions.get(), 1);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn source_memory_lookup_pending_rows_keep_their_source_grants() {
    let (catalog, mut store) = fixture(3);
    for node in &mut store.nodes {
        node.properties.insert("group".into(), Value::Int(7));
    }
    let plan = PhysicalPlan::NodeColumnLookupExec {
        variable: "found".into(),
        label: "Memory".into(),
        property: "group".into(),
        column: "key".into(),
        optional: false,
        node_visibility_predicate: None,
        input: Box::new(PhysicalPlan::ProjectExec {
            items: vec![Projection {
                name: "key".into(),
                expression: hawdb_plan_cypher::ProjectionExpression::Literal(Value::Int(7)),
            }],
            input: Box::new(source_plan(false)),
        }),
    };
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nz(256 * 1024),
        batch_payload_bytes: nz(32 * 1024),
        blocking_operator_bytes: nz(8192),
        ..memory()
    };
    for terminal in 0..3 {
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let reuse = ledger.account(
            QueryMemoryClass::ExternalRead,
            "lookup root reuse",
            memory.query_memory_bytes,
        );
        let mut calls = 0;
        let mut rows = 0;
        let result = run(&plan, &catalog, &store, &memory, &ledger, &mut |batch| {
            calls += 1;
            rows += batch.len();
            if calls == 1 {
                let pending = ledger
                    .test_owner_used_bytes(QueryMemoryClass::ExternalRead, "NodeColumnLookupExec");
                assert!(
                    pending >= 2 * 8192,
                    "hydrated pending rows lost source grants: {pending}"
                );
                let other = ledger.snapshot().used_bytes - pending;
                assert!(reuse
                    .reserve(memory.query_memory_bytes.get() - other - pending + 1)
                    .is_err());
            }
            match terminal {
                1 => Ok(BatchControl::Stop),
                2 => Err(HawDBError::Execution("lookup sink sentinel".into())),
                _ => Ok(BatchControl::Continue),
            }
        });
        match terminal {
            0 => {
                assert_eq!(result.unwrap(), BatchControl::Continue);
                assert_eq!(rows, 9);
            }
            1 => {
                assert_eq!(result.unwrap(), BatchControl::Stop);
                assert_eq!(calls, 1);
            }
            _ => {
                assert_eq!(
                    result.unwrap_err(),
                    HawDBError::Execution("lookup sink sentinel".into())
                );
                assert_eq!(calls, 1);
            }
        }
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn source_memory_source_fallback_releases_unused_wave_reservation() {
    let mut catalog = Catalog::default();
    let mut store = hawdb_storage::store::GraphStore::default();
    store
        .create_node(
            &mut catalog,
            "Source",
            BTreeMap::from([("body".into(), Value::String("x".repeat(20 * 1024)))]),
        )
        .unwrap();
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nz(64 * 1024),
        batch_payload_bytes: nz(32 * 1024),
        ..memory()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let parameters = BTreeMap::new();
    let mut external = NoExternalReadOperator;
    let external = BatchExternalReadAdapter::new(&mut external);
    let mut rows = 0;
    scan::stream_source_segment_scan_batches(
        "s",
        &Predicate::PropertyEq {
            variable: "s".into(),
            property: "version".into(),
            value: Value::Int(1),
        },
        BatchReadContext {
            catalog: &catalog,
            store: &store,
            parameters: &parameters,
            external: &external,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &QueryExecutionObserver::default(),
            host_scorer: None,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            rows += batch.len();
            assert_eq!(
                ledger.test_owner_used_bytes(
                    QueryMemoryClass::ExternalRead,
                    "SourceSegmentScan candidate wave reservation"
                ),
                0
            );
            Ok(BatchControl::Continue)
        },
    )
    .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn source_memory_ordered_adjacency_keys_keep_the_retained_cap() {
    use hawdb_storage::{
        adjacency::AdjacencyDirection,
        store::{GraphScanControl, GraphStore},
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    let source = store
        .create_node(&mut catalog, "Memory", BTreeMap::new())
        .unwrap();
    for _ in 0..10 {
        let target = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "LINK", BTreeMap::new())
            .unwrap();
    }
    for cap in [8, 512] {
        let ledger = QueryMemoryLedger::new(nz(64 * 1024));
        let source_account = ledger.source_account("ordered adjacency", nz(64 * 1024), nz(cap));
        let mut rows = 0;
        let result = store.visit_ordered_adjacent_relationships_with_allocation(
            source,
            None,
            AdjacencyDirection::Outgoing,
            64 * 1024,
            &mut |bytes| crate::store::admit_graph_read(&source_account, None, bytes).map(Some),
            |_| {
                rows += 1;
                Ok(GraphScanControl::Continue)
            },
        );
        if cap == 8 {
            assert!(result.unwrap_err().to_string().contains("8-byte budget"));
            assert_eq!(rows, 0);
        } else {
            assert_eq!(result.unwrap(), GraphScanControl::Continue);
            assert_eq!(rows, 10);
        }
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn source_memory_peer_lease_refuses_before_copy_and_same_ledger_recovers() {
    let (catalog, mut store) = fixture(1);
    store.out_of_core = true;
    let memory = memory();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let peer = ledger.account(
        QueryMemoryClass::ExternalRead,
        "live peer",
        memory.query_memory_bytes,
    );
    let node_bytes = hawdb_core::ids::node_allocation_bytes(&store.nodes[0]);
    let peer = peer
        .reserve(memory.query_memory_bytes.get() - node_bytes + 1)
        .unwrap();
    let error = run(
        &source_plan(false),
        &catalog,
        &store,
        &memory,
        &ledger,
        &mut |_| panic!("unadmitted source row reached consumer"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("query memory ledger"), "{error}");
    assert_eq!(store.node_admissions.get(), 0);
    assert_eq!(store.node_copies.get(), 0);
    assert_eq!(ledger.snapshot().used_bytes, peer.bytes());
    drop(peer);
    let mut rows = 0;
    run(
        &source_plan(false),
        &catalog,
        &store,
        &memory,
        &ledger,
        &mut |batch| {
            rows += batch.len();
            Ok(BatchControl::Continue)
        },
    )
    .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(store.node_admissions.get(), 1);
    assert_eq!(store.node_copies.get(), 1);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn source_memory_shortest_path_output_uses_root_without_expanding_state_cap() {
    let (mut catalog, mut store) = fixture(2);
    let rel_type = catalog.get_or_create_rel_type("LINK");
    store.relationships.push(hawdb_storage::RelRecord {
        id: hawdb_storage::RelId(0),
        source: NodeId(0),
        target: NodeId(1),
        rel_type,
        properties: BTreeMap::new(),
    });
    let plan = PhysicalPlan::ShortestPathExec {
        source_variable: "s".into(),
        source_label: "Memory".into(),
        source_id: Value::Int(0),
        source_visibility_predicate: None,
        rel_type: "LINK".into(),
        direction: hawdb_core::RelationshipDirection::Outgoing,
        target_variable: "t".into(),
        target_label: "Memory".into(),
        target_id: Value::Int(1),
        target_visibility_predicate: None,
        min_hops: 1,
        max_hops: 1,
        returns: vec![hawdb_plan_cypher::ShortestPathProjection {
            name: "bodies".into(),
            expression: hawdb_plan_cypher::ShortestPathProjectionExpression::NodePropertyList {
                property: "body".into(),
            },
        }],
    };
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: nz(4096),
        batch_payload_bytes: nz(32 * 1024),
        ..memory()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut rows = 0;
    run(&plan, &catalog, &store, &memory, &ledger, &mut |batch| {
        rows += batch.len();
        assert_eq!(
            batch[0].values["bodies"],
            Value::List(vec![Value::String("x".repeat(8192)); 2])
        );
        Ok(BatchControl::Continue)
    })
    .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(
        ledger
            .snapshot()
            .classes
            .iter()
            .find(|class| class.class == QueryMemoryClass::ResultMaterialization)
            .unwrap()
            .peak_bytes
            >= 16 * 1024
    );
    assert!(
        ledger
            .snapshot()
            .classes
            .iter()
            .find(|class| class.class == QueryMemoryClass::BlockingState)
            .unwrap()
            .peak_bytes
            <= 4096
    );
}

#[test]
fn source_memory_graph_algorithm_visibility_reads_above_retained_cap() {
    let (catalog, mut store) = fixture(2);
    store.definition = Some(hawdb_storage::projection::ProjectedGraphDefinition {
        node_labels: vec!["Memory".into()],
        rel_types: vec![],
        relationship_predicates: BTreeMap::new(),
    });
    let plan = PhysicalPlan::GraphAlgorithm {
        algorithm: GraphAlgorithmKind::PageRank,
        graph_name: "MemoryGraph".into(),
        options: Default::default(),
        score_column: "score".into(),
        return_node_identity: false,
        node_visibility_predicate: Some(Predicate::PropertyIn {
            variable: "n".into(),
            property: "id".into(),
            values: vec![Value::Int(0), Value::Int(1)],
        }),
    };
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: nz(4096),
        ..memory()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let mut rows = 0;
    run(&plan, &catalog, &store, &memory, &ledger, &mut |batch| {
        rows += batch.len();
        Ok(BatchControl::Continue)
    })
    .unwrap();
    assert_eq!(rows, 2);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(
        ledger
            .snapshot()
            .classes
            .iter()
            .find(|class| class.class == QueryMemoryClass::ExternalRead)
            .unwrap()
            .peak_bytes
            > 4096
    );
    assert!(
        ledger
            .snapshot()
            .classes
            .iter()
            .find(|class| class.class == QueryMemoryClass::BlockingState)
            .unwrap()
            .peak_bytes
            <= 4096
    );
}
