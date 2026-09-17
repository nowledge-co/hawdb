use super::*;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn index(&mut self, count: usize) -> usize {
        (self.next() % count as u64) as usize
    }
}

fn generated_fixture(rng: &mut Rng, ordinal: usize) -> Fixture {
    let mut fixture = Fixture::new();
    let count = rng.index(7) + 2;
    fixture.nodes = (0..count)
        .map(|index| NodeRecord {
            id: NodeId(if index + 1 == count {
                u64::MAX
            } else {
                index as u64 * 7
            }),
            labels: [LabelId(0), LabelId(1)]
                .into_iter()
                .filter(|_| rng.index(2) == 0)
                .collect(),
            properties: BTreeMap::from([("visible".to_string(), Value::Bool(rng.index(2) == 0))]),
        })
        .collect();
    fixture.relationships = (0..rng.index(24))
        .map(|index| RelRecord {
            id: RelId(index as u64),
            source: fixture.nodes[rng.index(count)].id,
            target: if rng.index(8) == 0 {
                NodeId(999)
            } else {
                fixture.nodes[rng.index(count)].id
            },
            rel_type: RelTypeId(rng.index(2) as u32),
            properties: BTreeMap::new(),
        })
        .collect();
    let names = [
        vec![],
        vec!["Missing"],
        vec!["Memory"],
        vec!["Missing", "Source"],
        vec!["Source", "Memory", "Memory"],
    ];
    let kinds = [
        vec![],
        vec!["Missing"],
        vec!["LINK"],
        vec!["Missing", "ALT"],
        vec!["ALT", "LINK", "LINK"],
    ];
    fixture.definition = Some(ProjectedGraphDefinition {
        node_labels: names[ordinal % names.len()]
            .iter()
            .map(|name| name.to_string())
            .collect(),
        rel_types: kinds[rng.index(kinds.len())]
            .iter()
            .map(|name| name.to_string())
            .collect(),
    });
    fixture
}

fn check_projection(
    fixture: &Fixture,
    only_visible: bool,
    layout: ProjectionLayout,
    identity: &str,
) {
    let definition = fixture.definition.as_ref().unwrap();
    let actual = try_projected_graph_with_node_filter(
        &fixture.catalog,
        fixture,
        &definition.node_labels,
        &definition.rel_types,
        |node| !only_visible || visible(node),
        layout,
        ProjectionMemoryBudget::unlimited(),
    )
    .unwrap();
    let selected = selected_source(fixture, only_visible);
    let ids: Vec<_> = selected.nodes.iter().map(|node| node.id).collect();
    assert_eq!(actual.nodes(), ids, "{identity}");
    assert_eq!(actual.layout(), layout, "{identity}");
    // A set-of-neighbors oracle independently covers direction, duplicate
    // edges, self loops, missing endpoints, and compressed offsets.
    let mut outgoing = vec![BTreeSet::new(); ids.len()];
    let mut incoming = vec![BTreeSet::new(); ids.len()];
    for relationship in &selected.relationships {
        let source = ids
            .iter()
            .position(|id| *id == relationship.source)
            .unwrap();
        let target = ids
            .iter()
            .position(|id| *id == relationship.target)
            .unwrap();
        if layout != ProjectionLayout::Incoming {
            outgoing[source].insert(target);
            if layout == ProjectionLayout::Undirected {
                outgoing[target].insert(source);
            }
        }
        if matches!(
            layout,
            ProjectionLayout::Incoming | ProjectionLayout::Bidirectional
        ) {
            incoming[target].insert(source);
        }
    }
    fn compress(neighbors: &[BTreeSet<usize>]) -> (Vec<usize>, Vec<usize>) {
        let mut offsets = vec![0];
        let mut items = vec![];
        for row in neighbors {
            items.extend(row);
            offsets.push(items.len());
        }
        (offsets, items)
    }
    let (offsets, targets) = compress(&outgoing);
    assert_eq!(actual.csr_offsets(), offsets, "{identity}");
    assert_eq!(actual.csr_targets(), targets, "{identity}");
    if matches!(
        layout,
        ProjectionLayout::Incoming | ProjectionLayout::Bidirectional
    ) {
        let (offsets, sources) = compress(&incoming);
        assert_eq!(actual.csc_offsets(), offsets, "{identity}");
        assert_eq!(actual.csc_sources(), sources, "{identity}");
    } else {
        assert!(
            actual.csc_offsets().is_empty() && actual.csc_sources().is_empty(),
            "{identity}"
        );
    }
    let edges: usize = match layout {
        ProjectionLayout::Incoming => incoming.iter().map(BTreeSet::len).sum(),
        ProjectionLayout::Undirected => outgoing
            .iter()
            .enumerate()
            .map(|(source, targets)| targets.range(source..).count())
            .sum(),
        _ => outgoing.iter().map(BTreeSet::len).sum(),
    };
    assert_eq!(actual.edge_count(), edges, "{identity}");
}

fn expected_rows(graph: &ProjectedGraph, options: &RunOptions) -> (Vec<Binding>, usize, usize) {
    let (rows, result_bytes, scratch) = match options.algorithm {
        GraphAlgorithmKind::PageRank => {
            let settings = PageRankOptions {
                damping: options
                    .options
                    .damping
                    .unwrap_or(PageRankOptions::default().damping),
                iterations: options
                    .options
                    .max_iterations
                    .unwrap_or(PageRankOptions::default().iterations),
            };
            let scores = graph.page_rank_with_context(settings, None).unwrap();
            let bytes = scores.len() * std::mem::size_of::<skein_analytics::PageRankScore>() * 2;
            let rows = scores
                .into_iter()
                .map(|score| {
                    let mut row = Binding::scalar("node", Value::Int(score.node.0 as i64));
                    row.values
                        .insert(options.score_column.clone(), Value::Float(score.score));
                    row
                })
                .collect::<Vec<_>>();
            (
                rows,
                bytes,
                graph.page_rank_memory_estimate().algorithm_peak_bytes,
            )
        }
        GraphAlgorithmKind::Louvain => {
            let settings = LouvainOptions {
                max_iterations: options
                    .options
                    .max_iterations
                    .unwrap_or(LouvainOptions::default().max_iterations),
                max_levels: options
                    .options
                    .max_levels
                    .unwrap_or(LouvainOptions::default().max_levels),
            };
            let assignments = graph
                .hierarchical_louvain_communities_with_context(settings, None)
                .unwrap();
            let bytes = assignments.len()
                * std::mem::size_of::<skein_analytics::HierarchicalCommunityAssignment>()
                * 2;
            let rows = assignments
                .into_iter()
                .map(|assignment| {
                    Binding::values(BTreeMap::from([
                        ("node".to_string(), Value::Int(assignment.node.0 as i64)),
                        ("level".to_string(), Value::Int(assignment.level as i64)),
                        (
                            "louvain_id".to_string(),
                            Value::Int(assignment.community.0 as i64),
                        ),
                    ]))
                })
                .collect();
            (
                rows,
                bytes,
                graph.louvain_memory_estimate(settings).algorithm_peak_bytes,
            )
        }
    };
    (rows, result_bytes, scratch)
}

fn check_stream(fixture: &Fixture, options: &RunOptions, only_visible: bool, identity: &str) {
    let layout = match options.algorithm {
        GraphAlgorithmKind::PageRank => ProjectionLayout::Outgoing,
        GraphAlgorithmKind::Louvain => ProjectionLayout::Undirected,
    };
    let graph = reference_graph(fixture, only_visible, layout);
    let (mut expected, result_bytes, scratch) = expected_rows(&graph, options);
    expected.truncate(options.output_rows.unwrap_or(usize::MAX));
    let binding_bytes: usize = expected.iter().map(binding_memory_bytes).sum();
    let projection = graph.memory_estimate().estimated_bytes;
    let peak = projection + scratch.max(result_bytes + binding_bytes);
    let expected_report = crate::BlockingOperatorMemoryReport {
        operator: "GraphAlgorithm".to_string(),
        budget_bytes: options.memory.blocking_operator_bytes.get(),
        peak_tracked_bytes: peak,
        input_rows: graph.node_count(),
        max_spill_bytes: options.memory.max_spill_bytes.get(),
        max_spill_runs: options.memory.max_spill_runs.get(),
        spilled_bytes: 0,
        spill_run_count: 0,
        spilled_rows: 0,
    };
    let output = run(fixture, options, None);
    assert_eq!(
        output.reports.blocking_memory,
        vec![expected_report],
        "{identity}"
    );
    assert_eq!(output.peak_bytes, peak, "{identity}");
    assert!(output.reports.scan_pruning.is_empty() && output.reports.vector_execution.is_empty());
    assert!(output.reports.graph_expansion.is_empty());
    if !expected.is_empty() && options.exit == Exit::Error {
        assert!(
            matches!(output.result, Err(SkeinError::StorageIntegrity(ref message)) if message == "consumer sentinel"),
            "{identity}"
        );
    } else {
        assert_eq!(
            output.result.unwrap(),
            if !expected.is_empty() && options.exit == Exit::Stop {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            },
            "{identity}"
        );
    }
    if options.exit != Exit::Complete {
        expected.truncate(options.memory.batch_rows.get());
    }
    let batches: Vec<_> = expected
        .chunks(options.memory.batch_rows.get())
        .map(|chunk| chunk.to_vec())
        .collect();
    assert_eq!(output.batches, batches, "{identity}");
    assert_eq!(
        output.live_bytes,
        vec![projection + binding_bytes; batches.len()],
        "{identity}"
    );
}

fn campaign(seeds: u64) {
    let mut projections = 0;
    let mut executions = 0;
    for seed in 0..seeds {
        let mut rng = Rng(seed);
        for ordinal in 0..10 {
            let fixture = generated_fixture(&mut rng, ordinal);
            for only_visible in [false, true] {
                for layout in [
                    ProjectionLayout::Outgoing,
                    ProjectionLayout::Incoming,
                    ProjectionLayout::Bidirectional,
                    ProjectionLayout::Undirected,
                ] {
                    let identity = format!(
                        "seed={seed} graph={ordinal} visible={only_visible} layout={layout:?}"
                    );
                    check_projection(&fixture, only_visible, layout, &identity);
                    projections += 1;
                }
            }
            let only_visible = rng.index(2) == 0;
            for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
                for limit in [None, Some(0), Some(1), Some(16)] {
                    for exit in [Exit::Complete, Exit::Stop, Exit::Error] {
                        let mut options = RunOptions {
                            algorithm,
                            output_rows: limit,
                            exit,
                            predicate: only_visible.then(visibility),
                            score_column: ["score", "rank", "node"][rng.index(3)].to_string(),
                            ..RunOptions::default()
                        };
                        options.memory.batch_rows = nz(rng.index(4) + 1);
                        options.options.damping =
                            [None, Some(0.0), Some(0.85), Some(1.0)][rng.index(4)];
                        options.options.max_iterations =
                            [None, Some(0), Some(1), Some(3)][rng.index(4)];
                        options.options.max_levels = [None, Some(1), Some(2)][rng.index(3)];
                        let identity = format!("seed={seed} graph={ordinal} algorithm={algorithm:?} limit={limit:?} exit={exit:?}");
                        check_stream(&fixture, &options, only_visible, &identity);
                        executions += 1;
                    }
                }
            }
        }
    }
    eprintln!("graph-algorithm-differential-v1 seeds={seeds} projection_cases={projections} execution_cases={executions}");
}

#[test]
fn graph_algorithm_differential_smoke() {
    campaign(4);
}

#[test]
#[ignore = "bounded local graph algorithm differential campaign"]
fn graph_algorithm_differential_campaign() {
    campaign(128);
}
