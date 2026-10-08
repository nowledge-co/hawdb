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
        page_rank(self, options, context)
    }

    pub fn hierarchical_louvain(
        self,
        options: LouvainOptions,
        context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<HierarchicalCommunityAssignment>> {
        algorithm_checkpoint(context)?;
        let mut graph = self;
        let mut output = Vec::new();
        for level in 0..options.max_levels.max(1) {
            let assignments = single_level_louvain(&graph, options, context)?;
            if assignments.is_empty() {
                break;
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
}

pub(crate) fn page_rank(
    graph: &impl GraphAdjacency,
    options: PageRankOptions,
    context: Option<&RuntimeTaskContext>,
) -> Result<Vec<PageRankScore>> {
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
    let mut ranks = vec![1.0 / count as f64; count];
    for _ in 0..options.iterations {
        algorithm_checkpoint(context)?;
        let mut dangling = 0.0;
        for (index, rank) in ranks.iter().enumerate() {
            algorithm_checkpoint_periodically(context, index)?;
            if degrees[index] == 0 {
                dangling += rank;
            }
        }
        let mut next = vec![(1.0 - damping) / count as f64; count];
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
        ranks = next;
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
    options: LouvainOptions,
    context: Option<&RuntimeTaskContext>,
) -> Result<Vec<CommunityAssignment>> {
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
        degrees.push(neighbors.len() as f64);
    }
    let total = degrees.iter().sum::<f64>();
    let mut communities: Vec<_> = (0..count).collect();
    let mut community_degrees = degrees.clone();
    let mut links = vec![0usize; count];
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
                graph.read_neighbors(node, &mut neighbors)?;
                for candidate in &candidates {
                    links[*candidate] = 0;
                }
                candidates.clear();
                candidates.push(current);
                for (ordinal, neighbor) in neighbors.iter().copied().enumerate() {
                    algorithm_checkpoint_periodically(context, ordinal)?;
                    let community = communities[neighbor];
                    if links[community] == 0 && community != current {
                        candidates.push(community);
                    }
                    links[community] += 1;
                }
                candidates.sort_unstable();
                let mut best = current;
                let mut best_gain = 0.0;
                for (ordinal, candidate) in candidates.iter().copied().enumerate() {
                    algorithm_checkpoint_periodically(context, ordinal)?;
                    let gain =
                        links[candidate] as f64 - degree * community_degrees[candidate] / total;
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
