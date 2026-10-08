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

//! Repeated, bounded adjacency reads. Edges remain in the pinned source; a
//! contraction retains only original-node membership, never another edge set.

use super::*;
use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::sync::Arc;

/// Internal seam for an immutable, repeatable adjacency view. Each row contains
/// sorted, unique indexes into `nodes()`. Implementations must not retain edges
/// between calls. The caller admits a row buffer of at most `nodes().len()`.
#[doc(hidden)]
pub trait GraphAdjacency {
    fn nodes(&self) -> &[NodeId];
    fn read_neighbors(&self, index: usize, neighbors: &mut Vec<usize>) -> Result<()>;

    /// Weighted undirected row. Self-loop weights include both degree ends.
    fn read_weighted_neighbors(
        &self,
        index: usize,
        neighbors: &mut Vec<usize>,
        weights: &mut Vec<f64>,
    ) -> Result<()> {
        self.read_neighbors(index, neighbors)?;
        weights.clear();
        weights.extend(
            neighbors
                .iter()
                .map(|&target| if target == index { 2.0 } else { 1.0 }),
        );
        Ok(())
    }
}

/// A pinned storage reader supplies endpoints one at a time, including incoming
/// endpoints for an undirected request. No complete adjacency list is required.
#[doc(hidden)]
pub trait AnalyticsEdgeSource {
    fn visit_neighbors(
        &self,
        node: NodeId,
        undirected: bool,
        visitor: &mut dyn FnMut(NodeId) -> Result<()>,
    ) -> Result<()>;
}

/// Node-resident analytics over a repeatable external edge source.
#[doc(hidden)]
pub struct StreamingGraph<'a, S: AnalyticsEdgeSource + ?Sized> {
    source: &'a S,
    originals: Arc<[NodeId]>,
    nodes: Vec<NodeId>,
    groups: Vec<Vec<usize>>,
    original_to_current: Vec<usize>,
    undirected: bool,
    contracted: bool,
    marks: RefCell<Vec<usize>>,
    generation: Cell<usize>,
    original_marks: RefCell<Vec<usize>>,
    original_generation: Cell<usize>,
    accumulated_weights: RefCell<Vec<f64>>,
}

impl<'a, S: AnalyticsEdgeSource + ?Sized> StreamingGraph<'a, S> {
    /// IDs must be strictly increasing, as in the canonical graph scan.
    pub fn new(source: &'a S, nodes: Vec<NodeId>, undirected: bool) -> Result<Self> {
        if nodes.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(HawDBError::Execution(
                "streaming analytics requires strictly ordered unique node IDs".into(),
            ));
        }
        Ok(Self {
            source,
            originals: Arc::from(nodes.as_slice()),
            groups: (0..nodes.len()).map(|index| vec![index]).collect(),
            original_to_current: (0..nodes.len()).collect(),
            original_marks: RefCell::new(vec![0; nodes.len()]),
            original_generation: Cell::new(0),
            accumulated_weights: RefCell::new(vec![0.0; nodes.len()]),
            marks: RefCell::new(vec![0; nodes.len()]),
            nodes,
            undirected,
            contracted: false,
            generation: Cell::new(0),
        })
    }

    pub fn page_rank(
        &self,
        options: PageRankOptions,
        context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<PageRankScore>> {
        page_rank(self, PageRankConfig::legacy(options), context)
    }

    pub fn page_rank_procedure(
        &self,
        options: PageRankProcedureOptions,
        context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<PageRankScore>> {
        options.validate()?;
        page_rank(self, PageRankConfig::procedure(options), context)
    }

    pub fn hierarchical_louvain(
        self,
        options: LouvainOptions,
        context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<HierarchicalCommunityAssignment>> {
        self.louvain(options.into(), false, context)
    }

    pub fn louvain_procedure(
        self,
        options: LouvainProcedureOptions,
        context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<HierarchicalCommunityAssignment>> {
        options.validate()?;
        self.louvain(options, true, context)
    }

    fn louvain(
        self,
        options: LouvainProcedureOptions,
        weighted: bool,
        context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<HierarchicalCommunityAssignment>> {
        algorithm_checkpoint(context)?;
        let mut graph = self;
        let mut output: Vec<HierarchicalCommunityAssignment> = Vec::new();
        for level in 0..options.max_levels.max(1) {
            let assignments = single_level_louvain_internal(&graph, options, weighted, context)?;
            if assignments.is_empty() {
                break;
            }
            // A stationary phase adds no information to the hierarchy.
            if weighted
                && level != 0
                && graph
                    .original_to_current
                    .iter()
                    .enumerate()
                    .all(|(original, current)| {
                        output[output.len() - graph.originals.len() + original].community
                            == assignments[*current].community
                    })
            {
                break;
            }
            if !options.hierarchy {
                output.clear();
            }
            for (original, current) in graph.original_to_current.iter().copied().enumerate() {
                algorithm_checkpoint_periodically(context, original)?;
                output.push(HierarchicalCommunityAssignment {
                    level,
                    node: graph.originals[original],
                    community: assignments[current].community,
                });
            }
            if level + 1 == options.max_levels.max(1) {
                break;
            }
            let next = graph.contract(&assignments, context)?;
            if next.nodes.len() == graph.nodes.len() {
                break;
            }
            graph = next;
        }
        algorithm_checkpoint(context)?;
        Ok(output)
    }

    fn contract(
        &self,
        assignments: &[CommunityAssignment],
        context: Option<&RuntimeTaskContext>,
    ) -> Result<Self> {
        let mut nodes: Vec<_> = assignments.iter().map(|row| row.community).collect();
        nodes.sort_unstable();
        nodes.dedup();
        let mut groups = vec![Vec::new(); nodes.len()];
        let mut original_to_current = vec![0; self.originals.len()];
        for (original, current) in self.original_to_current.iter().copied().enumerate() {
            algorithm_checkpoint_periodically(context, original)?;
            let target = nodes
                .binary_search(&assignments[current].community)
                .map_err(|_| {
                    HawDBError::Execution("analytics contraction lost community identity".into())
                })?;
            groups[target].push(original);
            original_to_current[original] = target;
        }
        Ok(Self {
            source: self.source,
            originals: Arc::clone(&self.originals),
            original_marks: RefCell::new(vec![0; self.originals.len()]),
            original_generation: Cell::new(0),
            accumulated_weights: RefCell::new(vec![0.0; nodes.len()]),
            marks: RefCell::new(vec![0; nodes.len()]),
            generation: Cell::new(0),
            nodes,
            groups,
            original_to_current,
            undirected: self.undirected,
            contracted: true,
        })
    }
}

impl<S: AnalyticsEdgeSource + ?Sized> GraphAdjacency for StreamingGraph<'_, S> {
    fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    fn read_neighbors(&self, index: usize, neighbors: &mut Vec<usize>) -> Result<()> {
        // Generation marks bound parallel relationships without clearing or
        // scanning all nodes on every row (which would make sparse graphs O(N²)).
        neighbors.clear();
        let mut marks = self.marks.borrow_mut();
        let generation = match self.generation.get().checked_add(1) {
            Some(generation) => generation,
            None => {
                marks.fill(0);
                1
            }
        };
        self.generation.set(generation);
        for original in &self.groups[index] {
            self.source.visit_neighbors(
                self.originals[*original],
                self.undirected,
                &mut |neighbor| {
                    if let Ok(original) = self.originals.binary_search(&neighbor) {
                        let target = self.original_to_current[original];
                        if (!self.contracted || index != target) && marks[target] != generation {
                            marks[target] = generation;
                            neighbors.push(target);
                        }
                    }
                    Ok(())
                },
            )?;
        }
        neighbors.sort_unstable();
        Ok(())
    }
    fn read_weighted_neighbors(
        &self,
        index: usize,
        neighbors: &mut Vec<usize>,
        weights: &mut Vec<f64>,
    ) -> Result<()> {
        neighbors.clear();
        weights.clear();
        let mut original_marks = self.original_marks.borrow_mut();
        let mut accumulated = self.accumulated_weights.borrow_mut();
        for &original in &self.groups[index] {
            let generation = match self.original_generation.get().checked_add(1) {
                Some(next) => next,
                None => {
                    original_marks.fill(0);
                    1
                }
            };
            self.original_generation.set(generation);
            let read =
                self.source
                    .visit_neighbors(self.originals[original], true, &mut |neighbor| {
                        if let Ok(target_original) = self.originals.binary_search(&neighbor)
                            && original_marks[target_original] != generation
                        {
                            original_marks[target_original] = generation;
                            let target = self.original_to_current[target_original];
                            if accumulated[target] == 0.0 {
                                neighbors.push(target);
                            }
                            accumulated[target] += if target_original == original {
                                2.0
                            } else {
                                1.0
                            };
                        }
                        Ok(())
                    });
            if let Err(error) = read {
                for &target in neighbors.iter() {
                    accumulated[target] = 0.0;
                }
                neighbors.clear();
                return Err(error);
            }
        }
        neighbors.sort_unstable();
        for &target in neighbors.iter() {
            weights.push(accumulated[target]);
            accumulated[target] = 0.0;
        }
        Ok(())
    }
}

pub(crate) struct PageRankConfig {
    options: PageRankProcedureOptions,
    redistribute_dangling: bool,
}

impl PageRankConfig {
    pub(crate) fn legacy(options: PageRankOptions) -> Self {
        Self {
            options: PageRankProcedureOptions {
                iterations: options.iterations,
                damping: options.damping,
                tolerance: 0.0,
                normalize_initial: true,
            },
            redistribute_dangling: true,
        }
    }
    pub(crate) fn procedure(mut options: PageRankProcedureOptions) -> Self {
        options.iterations = options.iterations.saturating_sub(1);
        Self {
            options,
            redistribute_dangling: false,
        }
    }
}

pub(crate) fn page_rank(
    graph: &impl GraphAdjacency,
    config: PageRankConfig,
    context: Option<&RuntimeTaskContext>,
) -> Result<Vec<PageRankScore>> {
    let options = config.options;
    algorithm_checkpoint(context)?;
    let count = graph.nodes().len();
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut neighbors = Vec::with_capacity(count);
    let mut degrees = Vec::with_capacity(count);
    for index in 0..count {
        algorithm_checkpoint_periodically(context, index)?;
        graph.read_neighbors(index, &mut neighbors)?;
        degrees.push(neighbors.len());
    }
    let damping = options.damping.clamp(0.0, 1.0);
    let initial_rank = if options.normalize_initial {
        1.0 / count as f64
    } else {
        1.0
    };
    let mut ranks = vec![initial_rank; count];
    for _ in 0..options.iterations {
        algorithm_checkpoint(context)?;
        let mut dangling = 0.0;
        for (index, rank) in ranks.iter().enumerate() {
            algorithm_checkpoint_periodically(context, index)?;
            if config.redistribute_dangling && degrees[index] == 0 {
                dangling += rank;
            }
        }
        let mut next = vec![(1.0 - damping) * initial_rank; count];
        let share = damping * dangling / count as f64;
        for score in &mut next {
            *score += share;
        }
        for (source, rank) in ranks.iter().enumerate() {
            algorithm_checkpoint_periodically(context, source)?;
            if degrees[source] == 0 {
                continue;
            }
            let contribution = damping * rank / degrees[source] as f64;
            graph.read_neighbors(source, &mut neighbors)?;
            for (ordinal, target) in neighbors.iter().copied().enumerate() {
                algorithm_checkpoint_periodically(context, ordinal)?;
                next[target] += contribution;
            }
        }
        let difference = ranks
            .iter()
            .zip(&next)
            .map(|(current, next)| (current - next).abs())
            .sum::<f64>();
        ranks = next;
        if difference < options.tolerance {
            break;
        }
    }
    let mut scores: Vec<_> = graph
        .nodes()
        .iter()
        .copied()
        .zip(ranks)
        .map(|(node, score)| PageRankScore { node, score })
        .collect();
    scores.sort_unstable_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.node.cmp(&right.node))
    });
    algorithm_checkpoint(context)?;
    Ok(scores)
}

pub(crate) fn single_level_louvain(
    graph: &impl GraphAdjacency,
    options: LouvainProcedureOptions,
    context: Option<&RuntimeTaskContext>,
) -> Result<Vec<CommunityAssignment>> {
    single_level_louvain_internal(graph, options, false, context)
}

fn single_level_louvain_internal(
    graph: &impl GraphAdjacency,
    options: LouvainProcedureOptions,
    weighted: bool,
    context: Option<&RuntimeTaskContext>,
) -> Result<Vec<CommunityAssignment>> {
    algorithm_checkpoint(context)?;
    let count = graph.nodes().len();
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut neighbors = Vec::with_capacity(count);
    let mut weights = Vec::with_capacity(count);
    let mut degrees = Vec::with_capacity(count);
    for index in 0..count {
        algorithm_checkpoint_periodically(context, index)?;
        if weighted {
            graph.read_weighted_neighbors(index, &mut neighbors, &mut weights)?;
            degrees.push(weights.iter().sum());
        } else {
            graph.read_neighbors(index, &mut neighbors)?;
            degrees.push(neighbors.len() as f64);
        }
    }
    let total = degrees.iter().sum::<f64>();
    let mut communities: Vec<_> = (0..count).collect();
    let mut community_degrees = degrees.clone();
    let mut links = vec![0.0; count];
    let mut candidates = Vec::with_capacity(count);
    let mut members: Vec<_> = graph
        .nodes()
        .iter()
        .copied()
        .map(|node| BTreeSet::from([node]))
        .collect();
    if total != 0.0 {
        for _ in 0..options.max_iterations {
            algorithm_checkpoint(context)?;
            let mut changed = false;
            for node in 0..count {
                algorithm_checkpoint_periodically(context, node)?;
                let current = communities[node];
                let degree = degrees[node];
                community_degrees[current] -= degree;
                if weighted {
                    graph.read_weighted_neighbors(node, &mut neighbors, &mut weights)?;
                } else {
                    graph.read_neighbors(node, &mut neighbors)?;
                }
                for candidate in &candidates {
                    links[*candidate] = 0.0;
                }
                candidates.clear();
                candidates.push(current);
                for (ordinal, neighbor) in neighbors.iter().copied().enumerate() {
                    algorithm_checkpoint_periodically(context, ordinal)?;
                    if weighted && neighbor == node {
                        continue;
                    }
                    let community = communities[neighbor];
                    if links[community] == 0.0 && community != current {
                        candidates.push(community);
                    }
                    links[community] += if weighted { weights[ordinal] } else { 1.0 };
                }
                candidates.sort_unstable();
                let mut best = current;
                let mut best_gain = 0.0;
                for (ordinal, candidate) in candidates.iter().copied().enumerate() {
                    algorithm_checkpoint_periodically(context, ordinal)?;
                    let gain = links[candidate]
                        - options.resolution * (degree * community_degrees[candidate] / total);
                    match gain.total_cmp(&best_gain) {
                        Ordering::Greater => {
                            best = candidate;
                            best_gain = gain;
                        }
                        Ordering::Equal => {
                            let candidate_id = members[candidate]
                                .first()
                                .copied()
                                .unwrap_or(graph.nodes()[candidate]);
                            let best_id = members[best]
                                .first()
                                .copied()
                                .unwrap_or(graph.nodes()[best]);
                            if candidate_id < best_id {
                                best = candidate;
                                best_gain = gain;
                            }
                        }
                        _ => {}
                    }
                }
                community_degrees[best] += degree;
                if best != current {
                    members[current].remove(&graph.nodes()[node]);
                    members[best].insert(graph.nodes()[node]);
                    communities[node] = best;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }
    let mut representatives = vec![NodeId(u64::MAX); count];
    for (index, community) in communities.iter().copied().enumerate() {
        algorithm_checkpoint_periodically(context, index)?;
        representatives[community] = representatives[community].min(graph.nodes()[index]);
    }
    let output = graph
        .nodes()
        .iter()
        .copied()
        .enumerate()
        .map(|(index, node)| CommunityAssignment {
            node,
            community: representatives[communities[index]],
        })
        .collect();
    algorithm_checkpoint(context)?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Edges(Vec<(u64, u64)>);
    impl AnalyticsEdgeSource for Edges {
        fn visit_neighbors(
            &self,
            node: NodeId,
            undirected: bool,
            visitor: &mut dyn FnMut(NodeId) -> Result<()>,
        ) -> Result<()> {
            for &(from, to) in &self.0 {
                if from == node.0 {
                    visitor(NodeId(to))?;
                }
                if undirected && to == node.0 {
                    visitor(NodeId(from))?;
                }
            }
            Ok(())
        }
    }

    #[test]
    fn weighted_contraction_preserves_internal_edges_and_total_degree() {
        let source = Edges(vec![
            (0, 1),
            (1, 2),
            (2, 0),
            (3, 4),
            (4, 5),
            (5, 3),
            (6, 7),
            (7, 8),
            (8, 6),
            (2, 3),
            (5, 6),
        ]);
        let graph = StreamingGraph::new(&source, (0..9).map(NodeId).collect(), true).unwrap();
        let assignments: Vec<_> = (0..9)
            .map(|node| CommunityAssignment {
                node: NodeId(node),
                community: NodeId(node / 3 * 3),
            })
            .collect();
        let contracted = graph.contract(&assignments, None).unwrap();
        let mut neighbors = Vec::new();
        let mut weights = Vec::new();
        for (index, expected) in [
            (0, vec![(0, 6.0), (1, 1.0)]),
            (1, vec![(0, 1.0), (1, 6.0), (2, 1.0)]),
            (2, vec![(1, 1.0), (2, 6.0)]),
        ] {
            contracted
                .read_weighted_neighbors(index, &mut neighbors, &mut weights)
                .unwrap();
            assert_eq!(
                neighbors
                    .iter()
                    .copied()
                    .zip(weights.iter().copied())
                    .collect::<Vec<_>>(),
                expected
            );
        }
        let joined = contracted
            .contract(
                &[
                    CommunityAssignment {
                        node: NodeId(0),
                        community: NodeId(0),
                    },
                    CommunityAssignment {
                        node: NodeId(3),
                        community: NodeId(0),
                    },
                    CommunityAssignment {
                        node: NodeId(6),
                        community: NodeId(0),
                    },
                ],
                None,
            )
            .unwrap();
        joined
            .read_weighted_neighbors(0, &mut neighbors, &mut weights)
            .unwrap();
        assert_eq!(neighbors, [0]);
        assert_eq!(weights, [22.0]); // Exactly twice the 11 original edges.
    }

    #[test]
    fn weighted_louvain_keeps_three_triangles_at_mem_resolution() {
        let source = Edges(vec![
            (0, 1),
            (1, 2),
            (2, 0),
            (3, 4),
            (4, 5),
            (5, 3),
            (6, 7),
            (7, 8),
            (8, 6),
            (2, 3),
            (5, 6),
        ]);
        let run = |hierarchy| {
            StreamingGraph::new(&source, (0..9).map(NodeId).collect(), true)
                .unwrap()
                .louvain_procedure(
                    LouvainProcedureOptions {
                        resolution: 0.8,
                        hierarchy,
                        ..LouvainProcedureOptions::default()
                    },
                    None,
                )
                .unwrap()
        };
        let rows = run(false);
        assert_eq!(rows.len(), 9);
        for row in &rows {
            assert_eq!(row.community, NodeId(row.node.0 / 3 * 3));
        }
        // Once contracted, the stationary partition is not repeated as another
        // hierarchy level. The partition's independent Q_gamma is positive.
        assert_eq!(run(true), rows);
        let modularity = 9.0 / 11.0
            - 0.8 * (7.0_f64.powi(2) + 8.0_f64.powi(2) + 7.0_f64.powi(2)) / 22.0_f64.powi(2);
        assert!(
            modularity > 1.0 - 0.8,
            "collapsing all nodes decreases modularity"
        );
    }
}
