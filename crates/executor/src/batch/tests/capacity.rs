use super::*;
use crate::external::{
    TextSeedExecutionOutput, TextSeedExecutionRequest, VectorSeedExecutionOutput,
    VectorSeedExecutionRequest,
};
use hawdb_plan_cypher::{
    GraphMatchNode, GraphMatchProgram, GraphMatchStep, ProjectionExpression,
    VectorExecutionResourceProfile,
};

struct TextSource {
    calls: Cell<usize>,
}

impl BatchExternalRead for TextSource {
    fn execute_vector_seed(
        &self,
        _: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        panic!("unexpected vector source")
    }

    fn execute_text_seed(
        &self,
        request: TextSeedExecutionRequest<'_>,
    ) -> Result<TextSeedExecutionOutput> {
        self.calls.set(self.calls.get() + 1);
        let mut output =
            TextSeedExecutionOutput::new(request.result_account, request.resources.result)?;
        for _ in 0..request.resources.result.max_rows {
            output.push("document:a", Some("a"), 1.0)?;
        }
        Ok(output)
    }
}

fn seed(window: usize) -> PhysicalPlan {
    PhysicalPlan::TextSeedScan {
        query_parameter: "text".into(),
        top_k: window,
        output_external_id: true,
        metadata_filters: BTreeMap::new(),
        resource_profile: VectorExecutionResourceProfile {
            priority: 1,
            max_parallelism: 1,
            max_working_memory_bytes: Some(16 * 1024),
        },
    }
}

fn lookup(window: usize) -> PhysicalPlan {
    PhysicalPlan::NodeColumnLookupExec {
        variable: "seed".into(),
        label: String::new(),
        property: "id".into(),
        column: "external_id".into(),
        optional: false,
        node_visibility_predicate: None,
        input: Box::new(seed(window)),
    }
}

#[derive(Clone, Copy, Debug)]
enum Parent {
    Lookup,
    Project,
    Filter,
    Limit,
    Expand,
    Match,
    Cartesian,
}

fn plan(parent: Parent, window: usize) -> PhysicalPlan {
    match parent {
        Parent::Lookup => lookup(window),
        Parent::Project => PhysicalPlan::ProjectExec {
            items: vec![Projection {
                name: "id".into(),
                expression: ProjectionExpression::Column("external_id".into()),
            }],
            input: Box::new(seed(window)),
        },
        Parent::Filter => PhysicalPlan::FilterExec {
            predicate: Predicate::ConstantBool(true),
            input: Box::new(seed(window)),
        },
        Parent::Limit => PhysicalPlan::LimitExec {
            offset: 0,
            limit: None,
            input: Box::new(seed(window)),
        },
        Parent::Expand => PhysicalPlan::AdjacencyExpandExec {
            source_variable: "seed".into(),
            source_label: "Memory".into(),
            rel_variable: None,
            rel_type: "MENTIONS".into(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "target".into(),
            target_label: "Memory".into(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            graph_budget: None,
            input: Box::new(lookup(window)),
        },
        Parent::Match => PhysicalPlan::GraphMatchExec {
            program: GraphMatchProgram {
                imports: vec![],
                introduced: vec!["target".into()],
                predicate: None,
                optional: false,
                steps: vec![GraphMatchStep::Expand {
                    source: "seed".into(),
                    relationship: None,
                    rel_type: "MENTIONS".into(),
                    properties: BTreeMap::new(),
                    direction: RelationshipDirection::Outgoing,
                    min_hops: 1,
                    max_hops: 1,
                    target: GraphMatchNode {
                        variable: "target".into(),
                        label: "Memory".into(),
                        properties: BTreeMap::new(),
                    },
                }],
            },
            input: Some(Box::new(lookup(window))),
        },
        Parent::Cartesian => PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(seed(window)),
            right: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "target".into(),
                label: "Memory".into(),
            }),
        },
    }
}

fn check(parent: Parent) {
    let nz = |n| NonZeroUsize::new(n).unwrap();
    for (window, batch_rows, terminal) in [
        (1, 8192, 0),
        (1, 8192, 1),
        (1, 8192, 2),
        (3, 2, 0),
        (3, 2, 1),
        (3, 2, 2),
        (0, 8192, 0),
    ] {
        let mut catalog = Catalog::default();
        let label = catalog.get_or_create_label("Memory");
        let rel_type = catalog.get_or_create_rel_type("MENTIONS");
        let store = store::ReadFixture {
            nodes: ["a", "b"]
                .into_iter()
                .enumerate()
                .map(|(id, external_id)| NodeRecord {
                    id: NodeId(id as u64),
                    labels: [label].into_iter().collect(),
                    properties: BTreeMap::from([("id".into(), Value::String(external_id.into()))]),
                })
                .collect(),
            relationships: vec![hawdb_storage::RelRecord {
                id: hawdb_storage::RelId(0),
                source: NodeId(0),
                target: NodeId(1),
                rel_type,
                properties: BTreeMap::new(),
            }],
            ..store::ReadFixture::default()
        };
        let memory = ExecutionMemoryConfig {
            query_memory_bytes: nz(64 * 1024),
            blocking_operator_bytes: nz(16 * 1024),
            batch_payload_bytes: nz(16 * 1024),
            batch_rows: nz(batch_rows),
            ..Default::default()
        };
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let parameters = BTreeMap::from([("text".into(), Value::String("graph".into()))]);
        let source = TextSource {
            calls: Cell::new(0),
        };
        let plan = plan(parent, window);
        let observer = QueryExecutionObserver::new(&plan);
        let context = BatchReadContext {
            catalog: &catalog,
            store: &store,
            parameters: &parameters,
            external: &source,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: None,
            observer: &observer,
            host_scorer: None,
        };
        let mut emitted = 0;
        let result = execute_binding_batches(
            &plan,
            context,
            ExecutionLimit::unlimited(),
            &mut |batch| {
                assert!(!batch.is_empty());
                assert!(batch.len() <= batch_rows);
                let slots = batch
                    .capacity()
                    .checked_mul(std::mem::size_of::<Binding>())
                    .unwrap();
                assert!(
                    slots <= memory.batch_payload_bytes.get(),
                    "{parent:?}: actual output capacity {slots} exceeds admitted batch budget {}",
                    memory.batch_payload_bytes
                );
                assert!(
                    slots <= memory.query_memory_bytes.get(),
                    "{parent:?}: actual output capacity exceeds root"
                );
                if matches!(parent, Parent::Lookup) {
                    let owner_bytes = ledger.test_owner_used_bytes(
                        crate::QueryMemoryClass::PipelineBatch,
                        "NodeColumnLookupExec output",
                    );
                    assert!(
                        slots <= owner_bytes,
                        "lookup output capacity {slots} exceeds its retained owner charge {owner_bytes}"
                    );
                    assert!(
                        slots <= ledger.snapshot().used_bytes,
                        "lookup output capacity {slots} exceeds retained query charge {}",
                        ledger.snapshot().used_bytes
                    );
                }
                for binding in &batch {
                    match parent {
                        Parent::Project => {
                            assert_eq!(binding.values["id"], Value::String("a".into()))
                        }
                        Parent::Lookup => assert_eq!(binding.nodes["seed"].id, NodeId(0)),
                        Parent::Expand | Parent::Match => {
                            assert_eq!(binding.nodes["seed"].id, NodeId(0));
                            assert_eq!(binding.nodes["target"].id, NodeId(1));
                        }
                        Parent::Cartesian => assert!(binding.nodes["target"].id.0 <= 1),
                        Parent::Filter | Parent::Limit => {
                            assert_eq!(binding.values["id"], Value::String("document:a".into()))
                        }
                    }
                }
                emitted += batch.len();
                match terminal {
                    1 => Ok(BatchControl::Stop),
                    2 => Err(HawDBError::Execution("capacity consumer refused".into())),
                    _ => Ok(BatchControl::Continue),
                }
            },
        );
        match terminal {
            1 => assert_eq!(result.unwrap(), BatchControl::Stop),
            2 => assert_eq!(
                result.unwrap_err(),
                HawDBError::Execution("capacity consumer refused".into())
            ),
            _ => {
                result.unwrap();
            }
        }
        let available = window
            * if matches!(parent, Parent::Cartesian) {
                2
            } else {
                1
            };
        assert_eq!(
            emitted,
            if terminal == 0 {
                available
            } else {
                available.min(batch_rows)
            },
            "{parent:?}"
        );
        assert_eq!(source.calls.get(), usize::from(window > 0));
        assert_eq!(
            ledger.snapshot().used_bytes,
            0,
            "{parent:?} leaked after terminal {terminal}"
        );
        assert!(ledger.snapshot().peak_bytes <= memory.query_memory_bytes.get());
    }
}

#[test]
fn text_lookup_output_capacity_is_backed_through_every_terminal() {
    check(Parent::Lookup);
}
#[test]
fn text_projection_output_capacity_fits_the_query_budget() {
    check(Parent::Project);
}
#[test]
fn text_filter_output_capacity_fits_the_query_budget() {
    check(Parent::Filter);
}
#[test]
fn text_limit_output_capacity_fits_the_query_budget() {
    check(Parent::Limit);
}
#[test]
fn text_native_expand_output_capacity_fits_the_query_budget() {
    check(Parent::Expand);
}
#[test]
fn text_generic_match_output_capacity_fits_the_query_budget() {
    check(Parent::Match);
}
#[test]
fn text_cartesian_output_capacity_fits_the_query_budget() {
    check(Parent::Cartesian);
}

fn run_batch_lookup_source_cancellation(indexed: bool) {
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Memory");
    let token = hawdb_core::RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    let store = store::ReadFixture {
        nodes: (0..4)
            .map(|id| NodeRecord {
                id: NodeId(id),
                labels: [label].into_iter().collect(),
                properties: BTreeMap::from([("id".into(), Value::String("a".into()))]),
            })
            .collect(),
        node_cancellation: Some(token.clone()),
        ..Default::default()
    };
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::new(64 * 1024).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(16 * 1024).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(16 * 1024).unwrap(),
        batch_rows: NonZeroUsize::new(8192).unwrap(),
        ..Default::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let parameters = BTreeMap::from([("text".into(), Value::String("graph".into()))]);
    let external = TextSource {
        calls: Cell::new(0),
    };
    let plan = PhysicalPlan::NodeColumnLookupExec {
        variable: "seed".into(),
        label: if indexed {
            "Memory".into()
        } else {
            String::new()
        },
        property: "id".into(),
        column: "external_id".into(),
        optional: false,
        node_visibility_predicate: None,
        input: Box::new(seed(1)),
    };
    let observer = QueryExecutionObserver::new(&plan);
    let emitted = Cell::new(0);
    let result = execute_binding_batches(
        &plan,
        BatchReadContext {
            catalog: &catalog,
            store: &store,
            parameters: &parameters,
            external: &external,
            memory: &memory,
            memory_ledger: &ledger,
            task_context: Some(&task),
            observer: &observer,
            host_scorer: None,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            emitted.set(emitted.get() + batch.len());
            Ok(BatchControl::Continue)
        },
    );
    assert_eq!(
        store.node_copies.get(),
        1,
        "lookup copied a node after source cancellation"
    );
    assert_eq!(
        store.node_admissions.get(),
        1,
        "lookup admitted a node after source cancellation"
    );
    assert!(token.is_cancelled());
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(external.calls.get(), 1);
    assert_eq!(emitted.get(), 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn indexed_batch_lookup_keeps_cancellation_before_the_next_owned_source_copy() {
    run_batch_lookup_source_cancellation(true);
}

#[test]
fn unindexed_batch_lookup_keeps_cancellation_before_the_next_owned_source_copy() {
    run_batch_lookup_source_cancellation(false);
}
