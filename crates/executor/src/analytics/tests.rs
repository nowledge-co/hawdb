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
use crate::binding::binding_memory_bytes;
use crate::observer::QueryExecutionReports;
use hawdb_analytics::{ProjectionScanControl, ProjectionSource};
use hawdb_core::{LabelId, RelTypeId, RuntimeCancellationToken};
use hawdb_plan_cypher::GraphAlgorithmOptions;
use hawdb_storage::{
    mutation::PropertyFilter, projection::ProjectedGraphDefinition, NodeId, RelId,
};
use std::cell::Cell;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

mod differential;
mod store;
mod streaming;

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

struct Fixture {
    catalog: Catalog,
    nodes: Vec<NodeRecord>,
    relationships: Vec<RelRecord>,
    definition: Option<ProjectedGraphDefinition>,
    node_scans: Cell<usize>,
    node_visits: Cell<usize>,
    rel_scans: Cell<usize>,
    rel_visits: Cell<usize>,
    fail_node_at: Option<usize>,
    fail_rel_scan: Option<usize>,
    cancel_after_nodes: Option<RuntimeCancellationToken>,
    cancel_node_at: Option<(usize, RuntimeCancellationToken)>,
    adjacency_visits: Cell<usize>,
    fail_adjacency_at: Option<usize>,
    fail_identity_node: Option<NodeId>,
    cancel_adjacency_at: Option<(usize, RuntimeCancellationToken)>,
}

impl Fixture {
    fn new() -> Self {
        let mut catalog = Catalog::default();
        let memory = catalog.get_or_create_label("Memory");
        let source = catalog.get_or_create_label("Source");
        let link = catalog.get_or_create_rel_type("LINK");
        let alt = catalog.get_or_create_rel_type("ALT");
        Self {
            catalog,
            nodes: [
                (0, vec![memory], true),
                (7, vec![memory, source], false),
                (u64::MAX, vec![source], true),
            ]
            .into_iter()
            .map(|(id, labels, visible)| NodeRecord {
                id: NodeId(id),
                labels: labels.into_iter().collect(),
                properties: BTreeMap::from([
                    ("id".to_string(), Value::String(format!("node-{id}"))),
                    ("visible".to_string(), Value::Bool(visible)),
                ]),
            })
            .collect(),
            relationships: [
                (0, 7, link),
                (7, u64::MAX, alt),
                (0, 7, link),
                (u64::MAX, u64::MAX, link),
                (0, 999, link),
            ]
            .into_iter()
            .enumerate()
            .map(|(id, (source, target, rel_type))| RelRecord {
                id: RelId(id as u64),
                source: NodeId(source),
                target: NodeId(target),
                rel_type,
                properties: BTreeMap::new(),
            })
            .collect(),
            definition: Some(ProjectedGraphDefinition {
                node_labels: vec![],
                rel_types: vec![],
                relationship_predicates: BTreeMap::new(),
            }),
            node_scans: Cell::new(0),
            node_visits: Cell::new(0),
            rel_scans: Cell::new(0),
            rel_visits: Cell::new(0),
            fail_node_at: None,
            fail_rel_scan: None,
            cancel_after_nodes: None,
            cancel_node_at: None,
            adjacency_visits: Cell::new(0),
            fail_adjacency_at: None,
            fail_identity_node: None,
            cancel_adjacency_at: None,
        }
    }

    fn reset_visits(&self) {
        self.node_scans.set(0);
        self.node_visits.set(0);
        self.rel_scans.set(0);
        self.rel_visits.set(0);
    }
}

fn visible(node: &NodeRecord) -> bool {
    node.properties.get("visible") == Some(&Value::Bool(true))
}

fn visibility() -> Predicate {
    Predicate::PropertyEq {
        variable: "n".into(),
        property: "visible".into(),
        value: Value::Bool(true),
    }
}

struct ReferenceSource {
    nodes: Vec<NodeRecord>,
    relationships: Vec<RelRecord>,
}

impl ProjectionSource for ReferenceSource {
    fn visit_projection_nodes(
        &self,
        visitor: &mut dyn FnMut(NodeRecord) -> ProjectionScanControl,
    ) -> std::result::Result<ProjectionScanControl, String> {
        for node in &self.nodes {
            if visitor(node.clone()) == ProjectionScanControl::Stop {
                return Ok(ProjectionScanControl::Stop);
            }
        }
        Ok(ProjectionScanControl::Continue)
    }
    fn visit_projection_relationships(
        &self,
        visitor: &mut dyn FnMut(RelRecord) -> ProjectionScanControl,
    ) -> std::result::Result<ProjectionScanControl, String> {
        for relationship in &self.relationships {
            if visitor(relationship.clone()) == ProjectionScanControl::Stop {
                return Ok(ProjectionScanControl::Stop);
            }
        }
        Ok(ProjectionScanControl::Continue)
    }
}

fn selected_source(fixture: &Fixture, only_visible: bool) -> ReferenceSource {
    let definition = fixture.definition.as_ref().unwrap();
    // Match the names on each record directly. Do not lower the requested names
    // into the production helper's label/type vectors or its empty-list branches.
    let nodes: Vec<_> = fixture
        .nodes
        .iter()
        .filter(|node| {
            (!only_visible || visible(node))
                && (definition.node_labels.is_empty()
                    || node.labels.iter().any(|id| {
                        fixture.catalog.label_name(*id).is_some_and(|name| {
                            definition.node_labels.iter().any(|label| label == name)
                        })
                    }))
        })
        .cloned()
        .collect();
    let selected: BTreeSet<_> = nodes.iter().map(|node| node.id).collect();
    let relationships = fixture
        .relationships
        .iter()
        .filter(|relationship| {
            selected.contains(&relationship.source)
                && selected.contains(&relationship.target)
                && (definition.rel_types.is_empty()
                    || fixture
                        .catalog
                        .rel_type_name(relationship.rel_type)
                        .is_some_and(|name| definition.rel_types.iter().any(|kind| kind == name)))
        })
        .cloned()
        .collect();
    ReferenceSource {
        nodes,
        relationships,
    }
}

fn reference_graph(
    fixture: &Fixture,
    only_visible: bool,
    layout: ProjectionLayout,
) -> ProjectedGraph {
    // The analytics builder/algorithms are unchanged by this migration. Use
    // independently selected records, bypassing both moved projection adapters.
    ProjectedGraph::try_from_store_with_node_filter_and_layout(
        &selected_source(fixture, only_visible),
        None,
        |_| true,
        layout,
        ProjectionMemoryBudget::unlimited(),
    )
    .unwrap()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Exit {
    Complete,
    Stop,
    Error,
}

struct RunOptions {
    algorithm: GraphAlgorithmKind,
    options: GraphAlgorithmOptions,
    predicate: Option<Predicate>,
    memory: ExecutionMemoryConfig,
    output_rows: Option<usize>,
    score_column: String,
    return_node_identity: bool,
    exit: Exit,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            algorithm: GraphAlgorithmKind::PageRank,
            options: GraphAlgorithmOptions {
                damping: Some(0.75),
                max_iterations: Some(3),
                max_levels: Some(2),
                ..GraphAlgorithmOptions::default()
            },
            predicate: None,
            memory: ExecutionMemoryConfig {
                query_memory_bytes: nz(1024 * 1024),
                blocking_operator_bytes: nz(64 * 1024),
                batch_rows: nz(2),
                ..ExecutionMemoryConfig::default()
            },
            output_rows: None,
            score_column: "score".to_string(),
            return_node_identity: false,
            exit: Exit::Complete,
        }
    }
}

struct Outcome {
    result: Result<BatchControl>,
    batches: Vec<Vec<Binding>>,
    live_bytes: Vec<usize>,
    reports: QueryExecutionReports,
    peak_bytes: usize,
}

fn run(fixture: &Fixture, options: &RunOptions, task: Option<&RuntimeTaskContext>) -> Outcome {
    let ledger = QueryMemoryLedger::new(options.memory.query_memory_bytes);
    let observer = QueryExecutionObserver::default();
    let mut batches = vec![];
    let mut live_bytes = vec![];
    let result = GraphAlgorithmSpec {
        algorithm: &options.algorithm,
        graph_name: "graph",
        options: &options.options,
        score_column: &options.score_column,
        return_node_identity: options.return_node_identity,
        node_visibility_predicate: &options.predicate,
    }
    .stream(
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
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert!(snapshot.classes.iter().all(|class| class.used_bytes == 0));
    Outcome {
        result,
        batches,
        live_bytes,
        reports: observer.into_reports(),
        peak_bytes: snapshot.peak_bytes,
    }
}

#[test]
fn unknown_projection_names_remain_empty_instead_of_becoming_wildcards() {
    for labels in [
        vec![],
        vec!["Missing"],
        vec!["Memory"],
        vec!["Missing", "Source"],
    ] {
        for kinds in [
            vec![],
            vec!["Missing"],
            vec!["LINK"],
            vec!["Missing", "ALT"],
        ] {
            let mut fixture = Fixture::new();
            fixture.definition = Some(ProjectedGraphDefinition {
                node_labels: labels.iter().map(|name| name.to_string()).collect(),
                rel_types: kinds.iter().map(|name| name.to_string()).collect(),
                relationship_predicates: BTreeMap::new(),
            });
            let definition = fixture.definition.as_ref().unwrap();
            for layout in [
                ProjectionLayout::Outgoing,
                ProjectionLayout::Incoming,
                ProjectionLayout::Bidirectional,
                ProjectionLayout::Undirected,
            ] {
                let actual = try_projected_graph_with_node_filter(
                    &fixture.catalog,
                    &fixture,
                    &definition.node_labels,
                    &definition.rel_types,
                    visible,
                    layout,
                    ProjectionMemoryBudget::unlimited(),
                )
                .unwrap();
                let expected = reference_graph(&fixture, true, layout);
                assert_eq!(
                    actual.nodes(),
                    expected.nodes(),
                    "{labels:?} {kinds:?} {layout:?}"
                );
                assert_eq!(actual.csr_offsets(), expected.csr_offsets());
                assert_eq!(actual.csr_targets(), expected.csr_targets());
                assert_eq!(actual.csc_offsets(), expected.csc_offsets());
                assert_eq!(actual.csc_sources(), expected.csc_sources());
                assert_eq!(actual.edge_count(), expected.edge_count());
                assert_eq!(actual.memory_estimate(), expected.memory_estimate());
            }
        }
    }
}

#[test]
fn projection_budget_is_inclusive_and_stops_before_more_node_reads() {
    let fixture = Fixture::new();
    for layout in [
        ProjectionLayout::Outgoing,
        ProjectionLayout::Incoming,
        ProjectionLayout::Bidirectional,
        ProjectionLayout::Undirected,
    ] {
        let expected = reference_graph(&fixture, false, layout);
        let bytes = expected.memory_estimate().estimated_bytes;
        let actual = try_projected_graph_with_node_filter(
            &fixture.catalog,
            &fixture,
            &[],
            &[],
            |_| true,
            layout,
            ProjectionMemoryBudget::new(nz(bytes)),
        )
        .unwrap();
        assert_eq!(actual.memory_estimate(), expected.memory_estimate());
        let error = try_projected_graph_with_node_filter(
            &fixture.catalog,
            &fixture,
            &[],
            &[],
            |_| true,
            layout,
            ProjectionMemoryBudget::new(nz(bytes - 1)),
        )
        .unwrap_err();
        assert!(error.to_string().contains("exceeding"));
        fixture.reset_visits();
        assert!(try_projected_graph_with_node_filter(
            &fixture.catalog,
            &fixture,
            &[],
            &[],
            |_| true,
            layout,
            ProjectionMemoryBudget::new(nz(1)),
        )
        .is_err());
        assert_eq!(fixture.node_visits.get(), 1);
        assert_eq!(fixture.rel_scans.get(), 0);
    }
}

#[test]
fn storage_scan_failure_and_early_stop_preserve_adapter_control() {
    let mut fixture = Fixture::new();
    let source = GraphExecutionProjectionSource(
        &fixture,
        None,
        None,
        QueryMemoryLedger::new(nz(1024 * 1024)).account(
            QueryMemoryClass::BlockingState,
            "projection source test",
            nz(1024 * 1024),
        ),
        None,
    );
    assert_eq!(
        source
            .visit_projection_nodes(&mut |_| ProjectionScanControl::Stop)
            .unwrap(),
        ProjectionScanControl::Stop
    );
    assert_eq!(fixture.node_visits.get(), 1);
    let mut rel_visits = 0;
    assert_eq!(
        source
            .visit_projection_relationships(&mut |_| {
                rel_visits += 1;
                ProjectionScanControl::Stop
            })
            .unwrap(),
        ProjectionScanControl::Stop
    );
    assert_eq!(rel_visits, 1);
    assert_eq!(fixture.rel_visits.get(), 1);
    for fail_node in [true, false] {
        for pass in [1, 2] {
            fixture.reset_visits();
            fixture.fail_node_at = fail_node.then_some(pass - 1);
            fixture.fail_rel_scan = (!fail_node).then_some(pass);
            let output = run(&fixture, &RunOptions::default(), None);
            let error = output.result.unwrap_err().to_string();
            assert!(
                error.contains("analytics projection storage scan failed"),
                "{error}"
            );
            assert!(error.contains(if fail_node {
                "node scan sentinel"
            } else {
                "relationship scan sentinel"
            }));
            assert!(output.batches.is_empty() && output.reports.blocking_memory.is_empty());
            assert!(output.peak_bytes <= 1024 * 1024);
        }
    }
}

#[test]
fn definition_predicate_and_cancellation_keep_existing_precedence() {
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    token.cancel();
    let mut fixture = Fixture::new();
    fixture.definition = None;
    let options = RunOptions {
        predicate: Some(Predicate::ConstantBool(false)),
        ..RunOptions::default()
    };
    let output = run(&fixture, &options, Some(&task));
    assert!(output
        .result
        .unwrap_err()
        .to_string()
        .contains("projected graph 'graph' does not exist"));
    fixture.definition = Some(ProjectedGraphDefinition {
        node_labels: vec![],
        rel_types: vec![],
        relationship_predicates: BTreeMap::new(),
    });
    let output = run(&fixture, &options, Some(&task));
    assert!(output
        .result
        .unwrap_err()
        .to_string()
        .contains("expression predicates are not supported"));
    assert_eq!(fixture.node_scans.get(), 0);
    for cancel_before in [false, true] {
        fixture.reset_visits();
        let cancellation = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(cancellation.clone());
        if cancel_before {
            cancellation.cancel();
        }
        fixture.cancel_after_nodes = Some(cancellation);
        let output = run(&fixture, &RunOptions::default(), Some(&context));
        assert!(output
            .result
            .unwrap_err()
            .to_string()
            .contains("runtime task stopped: cancelled"));
        assert!(fixture.node_scans.get() > 0);
        assert_eq!(fixture.rel_scans.get(), usize::from(!cancel_before));
        assert!(output.batches.is_empty() && output.reports.blocking_memory.is_empty());
    }
}

#[test]
fn source_and_projection_admission_fail_without_partial_rows() {
    let fixture = Fixture::new();
    let graph = reference_graph(&fixture, false, ProjectionLayout::Outgoing);
    let projection = graph.memory_estimate().estimated_bytes;
    let scratch = graph.page_rank_memory_estimate().algorithm_peak_bytes;
    // These graph-only estimates cannot bypass a larger live source lease.
    // Small operator caps now refuse before publishing algorithm reports.
    for (block, query, fragment, reports) in [
        (1, 1024 * 1024, "exceeding its 1-byte budget", 0),
        (64 * 1024, projection - 1, "exceeding", 0),
        (projection + scratch - 1, 1024 * 1024, "exceeding", 0),
        (projection + scratch, 1024 * 1024, "exceeding", 0),
    ] {
        let mut options = RunOptions::default();
        options.memory.blocking_operator_bytes = nz(block);
        options.memory.query_memory_bytes = nz(query);
        let output = run(&fixture, &options, None);
        let error = output.result.unwrap_err().to_string();
        assert!(error.contains(fragment), "{error}");
        assert!(output.batches.is_empty());
        assert_eq!(output.reports.blocking_memory.len(), reports);
    }
}

#[test]
fn algorithm_options_fail_closed_and_identity_columns_are_bounded() {
    let mut fixture = Fixture::new();
    for (algorithm, options, expected) in [
        (
            GraphAlgorithmKind::PageRank,
            GraphAlgorithmOptions {
                damping: Some(1.0),
                ..GraphAlgorithmOptions::default()
            },
            "PageRank damping must be finite and in [0, 1)",
        ),
        (
            GraphAlgorithmKind::PageRank,
            GraphAlgorithmOptions {
                tolerance: Some(-1.0),
                ..GraphAlgorithmOptions::default()
            },
            "PageRank tolerance must be finite and non-negative",
        ),
        (
            GraphAlgorithmKind::Louvain,
            GraphAlgorithmOptions {
                resolution: Some(0.0),
                ..GraphAlgorithmOptions::default()
            },
            "Louvain resolution must be finite and greater than 0",
        ),
    ] {
        let options = RunOptions {
            algorithm,
            options,
            ..RunOptions::default()
        };
        for output in [
            run(&fixture, &options, None),
            streaming::run_external(&fixture, &options, None),
        ] {
            assert!(output.result.unwrap_err().to_string().contains(expected));
            assert!(output.batches.is_empty());
        }
    }

    let output = run(
        &fixture,
        &RunOptions {
            return_node_identity: true,
            output_rows: Some(2),
            ..RunOptions::default()
        },
        None,
    );
    assert_eq!(output.result.unwrap(), BatchControl::Continue);
    let rows = output.batches.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| {
        matches!(row.values["node_id"], Value::String(_))
            && matches!(row.values["node_label"], Value::String(_))
    }));

    fixture.nodes[0]
        .properties
        .insert("payload".to_string(), Value::String("x".repeat(16 * 1024)));
    let baseline_options = RunOptions {
        output_rows: Some(1),
        ..RunOptions::default()
    };
    let baseline = run(&fixture, &baseline_options, None);
    assert_eq!(baseline.result.unwrap(), BatchControl::Continue);
    let baseline_peak = baseline.reports.blocking_memory[0].peak_tracked_bytes;
    let bounded = run(
        &fixture,
        &RunOptions {
            memory: ExecutionMemoryConfig {
                blocking_operator_bytes: nz(baseline_peak),
                ..RunOptions::default().memory
            },
            output_rows: Some(1),
            return_node_identity: true,
            ..RunOptions::default()
        },
        None,
    );
    let error = bounded.result.unwrap_err().to_string();
    assert!(error.contains("node identity hydration"), "{error}");
    assert!(bounded.batches.is_empty());
}

#[test]
fn identity_hydration_admits_selected_columns_before_cloning() {
    let mut fixture = Fixture::new();
    fixture.nodes[0].properties.insert(
        "content".into(),
        Value::String("unrelated".repeat(256 * 1024)),
    );
    let ledger = QueryMemoryLedger::new(nz(4096));
    let mut tracker = OperatorMemoryTracker::with_account(
        nz(4096),
        ledger.account(QueryMemoryClass::BlockingState, "identity test", nz(4096)),
    );
    let mut row = BTreeMap::new();
    let bytes = append_node_identity(
        &mut row,
        &fixture.catalog,
        &fixture,
        NodeId(0),
        &["Memory".into()],
        "PageRank",
        &mut tracker,
    )
    .unwrap();
    assert!(bytes < 4096, "unrequested content was retained");
    assert_eq!(row["node_id"], Value::String("node-0".into()));
    tracker.release(bytes);
    fixture.nodes[0]
        .properties
        .insert("id".into(), Value::String("oversized".repeat(1024)));
    let mut row = BTreeMap::new();
    let error = append_node_identity(
        &mut row,
        &fixture.catalog,
        &fixture,
        NodeId(0),
        &[],
        "PageRank",
        &mut tracker,
    )
    .unwrap_err();
    assert!(error.to_string().contains("blocking_operator_bytes"));
    assert!(row.is_empty());
    drop(tracker);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn mem_pagerank_reference_values_match_resident_and_forced_streaming() {
    let mut fixture = Fixture::new();
    fixture.nodes.truncate(2);
    fixture.relationships.truncate(1);
    for normalize in [true, false] {
        let initial = if normalize { 0.5 } else { 1.0 };
        let mut expected = [initial; 2];
        for iterations in 0..=3 {
            if iterations > 1 {
                expected = [0.15 * initial, 0.15 * initial + 0.85 * expected[0]];
            }
            let options = RunOptions {
                options: GraphAlgorithmOptions {
                    max_iterations: Some(iterations),
                    damping: Some(0.85),
                    tolerance: Some(0.0),
                    normalize_initial: Some(normalize),
                    ..GraphAlgorithmOptions::default()
                },
                ..RunOptions::default()
            };
            for outcome in [
                run(&fixture, &options, None),
                streaming::run_external(&fixture, &options, None),
            ] {
                assert_eq!(outcome.result.unwrap(), BatchControl::Continue);
                let rows: Vec<_> = outcome.batches.into_iter().flatten().collect();
                assert_eq!(rows.len(), 2);
                for row in rows {
                    let index = if row.values["node"] == Value::Int(0) {
                        0
                    } else {
                        1
                    };
                    let Value::Float(score) = row.values["score"] else {
                        panic!("missing score")
                    };
                    assert!(
                        (score - expected[index]).abs() < 1e-12,
                        "cap={iterations}, normalize={normalize}, node={index}"
                    );
                }
            }
        }
    }
}

#[test]
fn resident_projection_admits_root_before_accumulating_node_buffers() {
    let mut fixture = Fixture::new();
    fixture.nodes = (0..1024)
        .map(|id| NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::new(),
            properties: BTreeMap::new(),
        })
        .collect();
    fixture.relationships.clear();
    let mut options = RunOptions::default();
    options.memory.query_memory_bytes = nz(4096);
    options.memory.blocking_operator_bytes = nz(1024 * 1024);
    let output = run(&fixture, &options, None);
    assert!(output.result.is_err());
    assert!(
        fixture.node_visits.get() < 128,
        "projection owned the full graph before root admission: {}",
        fixture.node_visits.get()
    );
    assert!(output.batches.is_empty());
    assert!(output.peak_bytes <= 4096);
}

#[test]
fn resident_projection_admits_root_before_allocating_adjacency_buffers() {
    let mut fixture = Fixture::new();
    fixture.relationships = (0..1024)
        .map(|id| RelRecord {
            id: RelId(id),
            source: NodeId(0),
            target: NodeId(7),
            rel_type: fixture.relationships[0].rel_type,
            properties: BTreeMap::new(),
        })
        .collect();
    let mut options = RunOptions::default();
    options.memory.query_memory_bytes = nz(4096);
    options.memory.blocking_operator_bytes = nz(1024 * 1024);
    let output = run(&fixture, &options, None);
    assert!(output.result.is_err());
    assert_eq!(
        fixture.rel_scans.get(),
        1,
        "graph allocated adjacency before root admission"
    );
    assert!(output.batches.is_empty());
    assert!(output.peak_bytes <= 4096);
}

fn assert_empty_projection_offsets_root(unknown_label: bool, algorithm: GraphAlgorithmKind) {
    let mut fixture = Fixture::new();
    fixture.nodes.clear();
    fixture.relationships.clear();
    fixture.definition = Some(ProjectedGraphDefinition {
        node_labels: if unknown_label {
            vec!["Unknown".into()]
        } else {
            vec![]
        },
        rel_types: if unknown_label {
            vec![]
        } else {
            vec!["Unknown".into()]
        },
        relationship_predicates: BTreeMap::new(),
    });
    let footprint = std::mem::size_of::<usize>();
    let mut options = RunOptions {
        algorithm,
        ..RunOptions::default()
    };
    options.memory.blocking_operator_bytes = nz(1024 * 1024);
    options.memory.query_memory_bytes = nz(footprint - 1);
    let output = run(&fixture, &options, None);
    assert!(output.batches.is_empty());
    assert!(
        output.result.is_err(),
        "empty offsets escaped the query root"
    );
    assert!(output.reports.blocking_memory.is_empty());
    assert!(output.peak_bytes < footprint);

    // Isolate the projection owner's exact inclusive footprint from Louvain's
    // separate nonzero empty-graph algorithm scratch reservation.
    let ledger = QueryMemoryLedger::new(nz(footprint));
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "GraphAlgorithm",
        nz(1024 * 1024),
    );
    let no_properties = BTreeSet::new();
    let source = GraphExecutionProjectionSource(
        &fixture,
        None,
        Some(&no_properties),
        account.clone(),
        Some(std::cell::RefCell::new(
            OperatorMemoryTracker::with_account(nz(1024 * 1024), account),
        )),
    );
    let definition = fixture.definition.as_ref().unwrap();
    let layout = match algorithm {
        GraphAlgorithmKind::PageRank => ProjectionLayout::Outgoing,
        GraphAlgorithmKind::Louvain => ProjectionLayout::Undirected,
    };
    let graph = try_projected_graph_with_filters_admitted(
        &fixture.catalog,
        &source,
        ProjectedGraphFilters {
            node_labels: &definition.node_labels,
            rel_types: &definition.rel_types,
            relationship_predicates: &definition.relationship_predicates,
        },
        |_| true,
        layout,
        ProjectionMemoryBudget::new(nz(1024 * 1024)),
    )
    .unwrap();
    assert_eq!(graph.csr_offsets(), &[0]);
    assert_eq!(graph.memory_estimate().estimated_bytes, footprint);
    assert_eq!(source.4.as_ref().unwrap().borrow().used_bytes, footprint);
    assert_eq!(ledger.snapshot().used_bytes, footprint);
    drop(graph);
    drop(source);
    assert_eq!(ledger.snapshot().used_bytes, 0);

    options.memory.query_memory_bytes = nz(64);
    let output = run(&fixture, &options, None);
    assert_eq!(output.result.unwrap(), BatchControl::Continue);
    assert!(output.batches.is_empty());
    assert_eq!(output.reports.blocking_memory.len(), 1);
    assert!(output.peak_bytes >= footprint && output.peak_bytes <= 64);
}

#[test]
fn empty_unknown_label_pagerank_offsets_obey_root() {
    assert_empty_projection_offsets_root(true, GraphAlgorithmKind::PageRank);
}
#[test]
fn empty_unknown_type_pagerank_offsets_obey_root() {
    assert_empty_projection_offsets_root(false, GraphAlgorithmKind::PageRank);
}
#[test]
fn empty_unknown_label_louvain_offsets_obey_root() {
    assert_empty_projection_offsets_root(true, GraphAlgorithmKind::Louvain);
}
#[test]
fn empty_unknown_type_louvain_offsets_obey_root() {
    assert_empty_projection_offsets_root(false, GraphAlgorithmKind::Louvain);
}
