use super::*;
use crate::binding::binding_memory_bytes;
use crate::observer::QueryExecutionReports;
use skein_analytics::{ProjectionScanControl, ProjectionSource};
use skein_core::{LabelId, RelTypeId, RuntimeCancellationToken};
use skein_plan::GraphAlgorithmOptions;
use skein_storage::{NodeId, ProjectedGraphDefinition, PropertyFilter, RelId};
use std::cell::Cell;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

mod differential;
mod store;

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
                properties: BTreeMap::from([("visible".to_string(), Value::Bool(visible))]),
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
            }),
            node_scans: Cell::new(0),
            node_visits: Cell::new(0),
            rel_scans: Cell::new(0),
            rel_visits: Cell::new(0),
            fail_node_at: None,
            fail_rel_scan: None,
            cancel_after_nodes: None,
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
                Exit::Error => Err(SkeinError::StorageIntegrity("consumer sentinel".into())),
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
    let source = GraphExecutionProjectionSource(&fixture);
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
            assert_eq!(output.peak_bytes, 0);
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
        assert!(fixture.node_scans.get() > 0 && fixture.rel_scans.get() > 0);
        assert!(output.batches.is_empty() && output.reports.blocking_memory.is_empty());
    }
}

#[test]
fn projection_scratch_and_binding_admission_fail_without_partial_rows() {
    let fixture = Fixture::new();
    let graph = reference_graph(&fixture, false, ProjectionLayout::Outgoing);
    let projection = graph.memory_estimate().estimated_bytes;
    let scratch = graph.page_rank_memory_estimate().algorithm_peak_bytes;
    for (block, query, fragment, reports) in [
        (1, 1024 * 1024, "analytics projection layout", 0),
        (64 * 1024, projection - 1, "exceeding query_memory_bytes", 0),
        (
            projection + scratch - 1,
            1024 * 1024,
            "scratch and result state",
            1,
        ),
        (
            projection + scratch,
            1024 * 1024,
            "blocking_operator_bytes",
            1,
        ),
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
