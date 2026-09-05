use super::*;
use std::collections::VecDeque;

struct Fixture {
    store: GraphStore,
    catalog: Catalog,
    nodes: Vec<NodeId>,
    edges: Vec<(usize, usize, bool)>,
}

impl Fixture {
    fn new(count: usize, edges: Vec<(usize, usize, bool)>) -> Self {
        let mut store = GraphStore::in_memory();
        let mut catalog = Catalog::default();
        let nodes = (0..count)
            .map(|index| {
                store
                    .create_node(
                        &mut catalog,
                        "Node",
                        properties([
                            ("id", Value::Int(index as i64)),
                            ("visible", Value::Bool(index % 3 != 1)),
                        ]),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        for &(source, target, link) in &edges {
            store
                .create_relationship(
                    &mut catalog,
                    nodes[source],
                    nodes[target],
                    if link { "LINK" } else { "OTHER" },
                    BTreeMap::new(),
                )
                .unwrap();
        }
        Self {
            store,
            catalog,
            nodes,
            edges,
        }
    }

    fn reference(&self, search: &ShortestPathSearch<'_>, limit: usize) -> Vec<Vec<NodeId>> {
        // Independent full-path BFS over the fixture's edge list. Do not use
        // production adjacency helpers or the compact discovery implementation.
        let mut queue = VecDeque::from([vec![search.source]]);
        let mut paths = Vec::new();
        let mut found = None;
        while let Some(path) = queue.pop_front() {
            let depth = path.len() - 1;
            if found.is_some_and(|found| depth >= found) || depth >= search.max_hops {
                continue;
            }
            let current = *path.last().unwrap();
            let mut adjacent = Vec::new();
            for (ordinal, &(source, target, link)) in self.edges.iter().enumerate() {
                let kind = self
                    .catalog
                    .rel_type_id(if link { "LINK" } else { "OTHER" });
                if search.rel_type_id.is_some() && search.rel_type_id != kind {
                    continue;
                }
                if search.direction != RelationshipDirection::Incoming
                    && self.nodes[source] == current
                {
                    adjacent.push((0, self.nodes[target], ordinal));
                }
                if search.direction != RelationshipDirection::Outgoing
                    && self.nodes[target] == current
                    && source != target
                {
                    adjacent.push((1, self.nodes[source], ordinal));
                }
            }
            adjacent.sort_unstable();
            for (_, next, _) in adjacent {
                let index = self.nodes.iter().position(|&id| id == next).unwrap();
                if path.contains(&next)
                    || (search.path_node_visibility_filter.is_some() && index % 3 == 1)
                {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.push(next);
                if next == search.target && depth + 1 >= search.min_hops {
                    found = Some(depth + 1);
                    paths.push(next_path);
                    if paths.len() == limit {
                        return paths;
                    }
                } else if found.is_none() && depth + 1 < search.max_hops {
                    queue.push_back(next_path);
                }
            }
        }
        paths
    }
}

#[test]
fn seeded_shortest_paths_match_full_path_bfs_including_order_and_multiplicity() {
    let mut random = 222_u64;
    let mut next = || {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        random
    };
    let visible = skein_storage::PropertyFilter::Eq {
        property: "visible".to_string(),
        value: Value::Bool(true),
    };
    let mut comparisons = 0;
    let mut nonempty_cases = 0;
    let mut duplicate_cases = 0;
    for case in 0..96 {
        let count = 4 + (next() as usize % 4);
        let mut edges = Vec::new();
        for source in 0..count {
            for target in 0..count {
                if next() % 4 == 0 {
                    edges.push((source, target, next() % 3 != 0));
                    if next() % 5 == 0 {
                        edges.push((source, target, true));
                    }
                }
            }
        }
        let fixture = Fixture::new(count, edges);
        for direction in [
            RelationshipDirection::Outgoing,
            RelationshipDirection::Incoming,
            RelationshipDirection::Undirected,
        ] {
            for min_hops in [1, 2, 3] {
                for filtered in [false, true] {
                    for limit in [1, 7, usize::MAX] {
                        let search = ShortestPathSearch {
                            source: fixture.nodes[0],
                            target: fixture.nodes[count - 1],
                            rel_type_id: if case % 2 == 0 {
                                fixture.catalog.rel_type_id("LINK")
                            } else {
                                None
                            },
                            direction,
                            min_hops,
                            max_hops: if case % 3 == 0 { 3 } else { count },
                            path_node_visibility_filter: filtered.then_some(&visible),
                        };
                        let expected = fixture.reference(&search, limit);
                        comparisons += 1;
                        nonempty_cases += usize::from(!expected.is_empty());
                        duplicate_cases +=
                            usize::from(expected.windows(2).any(|pair| pair[0] == pair[1]));
                        let (actual, _, _) = all_shortest_paths(
                            &fixture.store,
                            search,
                            NonZeroUsize::new(2 * 1024 * 1024).unwrap(),
                            limit,
                            None,
                        )
                        .unwrap();
                        assert_eq!(actual, expected, "case {case}, direction {direction:?}, min {min_hops}, filtered {filtered}, limit {limit}");
                    }
                }
            }
        }
    }
    assert_eq!(comparisons, 5184);
    assert!(nonempty_cases > 100);
    assert!(duplicate_cases > 10);
    eprintln!("shortest-path oracle: comparisons={comparisons}, nonempty={nonempty_cases}, duplicate_paths={duplicate_cases}");
}

#[test]
fn minimum_hops_preserves_longer_simple_paths_and_rejects_cyclic_walks() {
    // The three-hop candidate 0 -> 1 -> 1 -> 5 is only a walk. The valid
    // four-hop route must survive both the direct shortcut and that candidate.
    let fixture = Fixture::new(
        6,
        vec![
            (0, 5, true),
            (0, 1, true),
            (1, 1, true),
            (1, 5, true),
            (0, 2, true),
            (2, 3, true),
            (3, 4, true),
            (4, 5, true),
        ],
    );
    let (paths, _, _) = all_shortest_paths(
        &fixture.store,
        ShortestPathSearch {
            source: fixture.nodes[0],
            target: fixture.nodes[5],
            rel_type_id: None,
            direction: RelationshipDirection::Outgoing,
            min_hops: 3,
            max_hops: usize::MAX,
            path_node_visibility_filter: None,
        },
        NonZeroUsize::new(64 * 1024).unwrap(),
        10,
        None,
    )
    .unwrap();
    assert_eq!(
        paths,
        vec![vec![
            fixture.nodes[0],
            fixture.nodes[2],
            fixture.nodes[3],
            fixture.nodes[4],
            fixture.nodes[5]
        ]]
    );
}

#[test]
fn shortest_path_empty_boundaries_do_not_allocate_discovery_state() {
    let fixture = Fixture::new(2, vec![(0, 1, true), (1, 0, true)]);
    for (target, min_hops, max_hops, limit) in
        [(1, 1, 1, 0), (0, 1, 10, 10), (1, 1, 0, 10), (1, 3, 2, 10)]
    {
        let (paths, peak, visited) = all_shortest_paths(
            &fixture.store,
            ShortestPathSearch {
                source: fixture.nodes[0],
                target: fixture.nodes[target],
                rel_type_id: None,
                direction: RelationshipDirection::Outgoing,
                min_hops,
                max_hops,
                path_node_visibility_filter: None,
            },
            NonZeroUsize::new(1).unwrap(),
            limit,
            None,
        )
        .unwrap();
        assert!(paths.is_empty());
        assert_eq!((peak, visited), (0, 0));
    }
}

#[test]
fn shortest_path_failures_release_query_memory() {
    let fixture = Fixture::new(
        6,
        vec![
            (0, 1, true),
            (0, 2, true),
            (1, 3, true),
            (2, 3, true),
            (3, 4, true),
            (4, 5, true),
        ],
    );
    for (root_budget, operator_budget, cancelled) in [
        (512, 64 * 1024, false),
        (64 * 1024, 512, false),
        (64 * 1024, 64 * 1024, true),
    ] {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(root_budget).unwrap());
        let budget = NonZeroUsize::new(operator_budget).unwrap();
        let account = ledger.account(
            QueryMemoryClass::BlockingState,
            "ShortestPathExec test",
            budget,
        );
        let context = RuntimeTaskContext::default();
        if cancelled {
            context.cancellation().cancel();
        }
        let result = skein_executor::traversal::all_shortest_paths(
            &fixture.store,
            ShortestPathSearch {
                source: fixture.nodes[0],
                target: fixture.nodes[5],
                rel_type_id: None,
                direction: RelationshipDirection::Outgoing,
                min_hops: 1,
                max_hops: 5,
                path_node_visibility_filter: None,
            },
            budget,
            10,
            account,
            Some(&context),
            &skein_executor::observer::NoopExecutionObserver,
        );
        assert!(result.is_err());
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert!(ledger.snapshot().peak_bytes <= root_budget);
    }
}

#[test]
fn layered_shortest_paths_fit_a_linear_discovery_budget() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Node", BTreeMap::new())
        .unwrap();
    let target = store
        .create_node(&mut catalog, "Node", BTreeMap::new())
        .unwrap();
    let mut layers = Vec::new();
    let mut previous = vec![source];
    for _ in 0..9 {
        let mut layer = Vec::new();
        for _ in 0..4 {
            layer.push(
                store
                    .create_node(&mut catalog, "Node", BTreeMap::new())
                    .unwrap(),
            );
        }
        for &left in &previous {
            for &right in &layer {
                store
                    .create_relationship(&mut catalog, left, right, "LINK", BTreeMap::new())
                    .unwrap();
            }
        }
        previous = layer.clone();
        layers.push(layer);
    }
    for left in previous {
        store
            .create_relationship(&mut catalog, left, target, "LINK", BTreeMap::new())
            .unwrap();
    }
    let (paths, peak, visited) = all_shortest_paths(
        &store,
        ShortestPathSearch {
            source,
            target,
            rel_type_id: catalog.rel_type_id("LINK"),
            direction: RelationshipDirection::Outgoing,
            min_hops: 1,
            max_hops: 10,
            path_node_visibility_filter: None,
        },
        NonZeroUsize::new(64 * 1024).unwrap(),
        7,
        None,
    )
    .unwrap();
    let expected = (0..7)
        .map(|rank| {
            let mut path = vec![source];
            for (depth, layer) in layers.iter().enumerate() {
                let shift = 2 * (layers.len() - depth - 1);
                path.push(layer[(rank >> shift) & 3]);
            }
            path.push(target);
            path
        })
        .collect::<Vec<_>>();
    assert_eq!(paths, expected);
    assert!(peak <= 64 * 1024);
    assert!(
        visited <= 38,
        "discovery revisited path prefixes: {visited}"
    );
    eprintln!("layered shortest paths: peak_tracked_bytes={peak}, expanded_states={visited}, emitted_paths={}", paths.len());
}
