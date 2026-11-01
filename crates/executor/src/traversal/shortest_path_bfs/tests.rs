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
use std::num::NonZeroUsize;

/// `(relationship, source, target)` in relationship-id adjacency order.
struct Graph(Vec<(u64, u64, u64)>);

impl Graph {
    fn adjacency(
        &self,
        node: NodeId,
        direction: AdjacencyDirection,
        visit: &mut dyn FnMut(RelId, NodeId) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        for &(relationship, source, target) in &self.0 {
            let neighbor = match direction {
                AdjacencyDirection::Outgoing if source == node.0 => target,
                AdjacencyDirection::Incoming if target == node.0 => source,
                _ => continue,
            };
            if visit(RelId(relationship), NodeId(neighbor))? == ScanControl::Stop {
                return Ok(ScanControl::Stop);
            }
        }
        Ok(ScanControl::Continue)
    }
}

struct Search {
    direction: RelationshipDirection,
    min_hops: usize,
    max_hops: usize,
    bounded: bool,
    all: bool,
    excluded: BTreeSet<RelId>,
    budget: usize,
}

impl Default for Search {
    fn default() -> Self {
        Self {
            direction: RelationshipDirection::Outgoing,
            min_hops: 1,
            max_hops: 16,
            bounded: true,
            all: false,
            excluded: BTreeSet::new(),
            budget: 1 << 20,
        }
    }
}

type Paths = Vec<(Vec<u64>, Vec<u64>)>;

impl Search {
    fn run(&self, graph: &Graph, source: u64, target: u64) -> Result<Option<Paths>> {
        self.run_limited(graph, source, target, usize::MAX)
    }

    fn run_limited(
        &self,
        graph: &Graph,
        source: u64,
        target: u64,
        limit: usize,
    ) -> Result<Option<Paths>> {
        let spec = ShortestPathBfsSpec {
            source: NodeId(source),
            target: NodeId(target),
            direction: self.direction,
            min_hops: self.min_hops,
            max_hops: self.max_hops,
            bounded: self.bounded,
            all: self.all,
            excluded: &self.excluded,
        };
        let mut tracker = OperatorMemoryTracker::new(NonZeroUsize::new(self.budget).unwrap());
        let mut checkpoint = || Ok(());
        let mut adjacency =
            |node: NodeId,
             direction: AdjacencyDirection,
             visit: &mut dyn FnMut(RelId, NodeId) -> Result<ScanControl>| {
                graph.adjacency(node, direction, visit)
            };
        let paths =
            match ShortestPathBfs::search(&spec, &mut adjacency, &mut tracker, &mut checkpoint)? {
                ShortestPathBfsOutcome::NotFound => None,
                ShortestPathBfsOutcome::ZeroLength => Some(vec![(vec![source], Vec::new())]),
                ShortestPathBfsOutcome::Found(search) => {
                    let mut paths = Vec::new();
                    search.visit_paths(
                        &mut tracker,
                        &mut checkpoint,
                        &mut |nodes, relationships| {
                            paths.push((
                                nodes.iter().map(|node| node.0).collect(),
                                relationships
                                    .iter()
                                    .map(|relationship| relationship.0)
                                    .collect(),
                            ));
                            Ok(if paths.len() == limit {
                                ScanControl::Stop
                            } else {
                                ScanControl::Continue
                            })
                        },
                    )?;
                    (*search).release(&mut tracker);
                    Some(paths)
                }
            };
        assert_eq!(tracker.used_bytes, 0, "search state must be released");
        Ok(paths)
    }
}

fn chain(length: u64) -> Graph {
    Graph((0..length).map(|index| (index, index, index + 1)).collect())
}

/// 1 -> {2, 3} -> 4, plus a longer detour 1 -> 5 -> 6 -> 4.
fn diamond() -> Graph {
    Graph(vec![
        (10, 1, 2),
        (11, 1, 3),
        (12, 2, 4),
        (13, 3, 4),
        (14, 1, 5),
        (15, 5, 6),
        (16, 6, 4),
    ])
}

#[test]
fn finds_directed_paths_in_both_orientations() {
    let graph = chain(4);
    let outgoing = Search::default().run(&graph, 0, 4).unwrap().unwrap();
    assert_eq!(outgoing, vec![(vec![0, 1, 2, 3, 4], vec![0, 1, 2, 3])]);
    assert_eq!(Search::default().run(&graph, 4, 0).unwrap(), None);
    let incoming = Search {
        direction: RelationshipDirection::Incoming,
        ..Search::default()
    }
    .run(&graph, 4, 0)
    .unwrap()
    .unwrap();
    assert_eq!(incoming, vec![(vec![4, 3, 2, 1, 0], vec![3, 2, 1, 0])]);
    let undirected = Search {
        direction: RelationshipDirection::Undirected,
        ..Search::default()
    }
    .run(&graph, 4, 1)
    .unwrap()
    .unwrap();
    assert_eq!(undirected, vec![(vec![4, 3, 2, 1], vec![3, 2, 1])]);
}

#[test]
fn shortest_path_selects_one_deterministic_path_and_all_selects_every_path() {
    let graph = diamond();
    let any = Search::default().run(&graph, 1, 4).unwrap().unwrap();
    assert_eq!(any, vec![(vec![1, 2, 4], vec![10, 12])]);
    let all = Search {
        all: true,
        ..Search::default()
    }
    .run(&graph, 1, 4)
    .unwrap()
    .unwrap();
    assert_eq!(
        all,
        vec![(vec![1, 2, 4], vec![10, 12]), (vec![1, 3, 4], vec![11, 13])]
    );
    let limited = Search {
        all: true,
        ..Search::default()
    }
    .run_limited(&graph, 1, 4, 1)
    .unwrap()
    .unwrap();
    assert_eq!(limited, vec![(vec![1, 2, 4], vec![10, 12])]);
}

#[test]
fn all_shortest_paths_keep_parallel_relationships_and_wide_layers() {
    // Two parallel first hops and a 2x2 layered middle: 2 * 2 * 2 paths.
    let graph = Graph(vec![
        (1, 0, 1),
        (2, 0, 1),
        (3, 1, 2),
        (4, 1, 3),
        (5, 2, 4),
        (6, 3, 4),
        (7, 4, 5),
        (8, 4, 5),
    ]);
    let all = Search {
        all: true,
        ..Search::default()
    }
    .run(&graph, 0, 5)
    .unwrap()
    .unwrap();
    assert_eq!(all.len(), 8);
    let unique = all.iter().cloned().collect::<BTreeSet<_>>();
    assert_eq!(unique.len(), 8);
    assert!(all.iter().all(|(nodes, relationships)| {
        nodes.len() == 5 && relationships.len() == 4 && nodes[0] == 0 && nodes[4] == 5
    }));
}

#[test]
fn equal_endpoints_depend_on_the_minimum_hop_count() {
    let graph = Graph(vec![(1, 0, 0), (2, 0, 1), (3, 1, 0)]);
    assert_eq!(
        Search {
            min_hops: 0,
            ..Search::default()
        }
        .run(&graph, 0, 0)
        .unwrap(),
        Some(vec![(vec![0], vec![])])
    );
    assert_eq!(Search::default().run(&graph, 0, 0).unwrap(), None);
    // A zero minimum does not change paths between distinct endpoints.
    assert_eq!(
        Search {
            min_hops: 0,
            ..Search::default()
        }
        .run(&graph, 0, 1)
        .unwrap(),
        Some(vec![(vec![0, 1], vec![2])])
    );
}

#[test]
fn bounds_and_unbounded_hop_cap_fail_closed() {
    let graph = chain(6);
    let bounded = Search {
        max_hops: 3,
        ..Search::default()
    };
    assert_eq!(bounded.run(&graph, 0, 6).unwrap(), None);
    assert!(bounded.run(&graph, 0, 3).unwrap().is_some());
    let capped = Search {
        max_hops: 3,
        bounded: false,
        ..Search::default()
    };
    let error = capped.run(&graph, 0, 6).unwrap_err().to_string();
    assert!(error.contains("3-hop limit"), "{error}");
    // An exhausted component proves absence even for an unbounded pattern.
    let disconnected = Graph(vec![(1, 0, 1), (2, 1, 2), (3, 7, 8)]);
    assert_eq!(capped.run(&disconnected, 0, 8).unwrap(), None);
}

#[test]
fn excluded_relationships_are_not_traversed() {
    let search = Search {
        all: true,
        excluded: BTreeSet::from([RelId(10)]),
        ..Search::default()
    };
    assert_eq!(
        search.run(&diamond(), 1, 4).unwrap().unwrap(),
        vec![(vec![1, 3, 4], vec![11, 13])]
    );
}

#[test]
fn search_state_is_charged_to_the_operator_budget() {
    let graph = Graph((0..512).map(|index| (index, 0, index + 1)).collect());
    let error = Search {
        budget: 4096,
        ..Search::default()
    }
    .run(&graph, 0, 10_000)
    .unwrap_err()
    .to_string();
    assert!(error.contains("blocking_operator_bytes"), "{error}");
}

#[test]
fn checkpoints_interrupt_discovery_and_enumeration() {
    let graph = diamond();
    let spec = ShortestPathBfsSpec {
        source: NodeId(1),
        target: NodeId(4),
        direction: RelationshipDirection::Outgoing,
        min_hops: 1,
        max_hops: 8,
        bounded: true,
        all: true,
        excluded: &BTreeSet::new(),
    };
    let mut adjacency =
        |node: NodeId,
         direction: AdjacencyDirection,
         visit: &mut dyn FnMut(RelId, NodeId) -> Result<ScanControl>| {
            graph.adjacency(node, direction, visit)
        };
    for allowed in 0..4 {
        let mut tracker = OperatorMemoryTracker::new(NonZeroUsize::new(1 << 20).unwrap());
        let mut calls = 0;
        let mut checkpoint = || {
            calls += 1;
            if calls > allowed {
                Err(HawDBError::Execution("cancelled".into()))
            } else {
                Ok(())
            }
        };
        let error =
            match ShortestPathBfs::search(&spec, &mut adjacency, &mut tracker, &mut checkpoint) {
                Err(error) => error,
                Ok(ShortestPathBfsOutcome::Found(search)) => {
                    let result = search.visit_paths(&mut tracker, &mut checkpoint, &mut |_, _| {
                        Ok(ScanControl::Continue)
                    });
                    (*search).release(&mut tracker);
                    result.unwrap_err()
                }
                Ok(_) => panic!("diamond endpoints are connected"),
            };
        assert!(error.to_string().contains("cancelled"));
    }
}
