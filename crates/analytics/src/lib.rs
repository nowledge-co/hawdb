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

use hawdb_core::{HawDBError, LabelId, RelTypeId, Result, RuntimeTaskContext};
use hawdb_storage::{NodeId, NodeRecord, RelRecord};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;

const ALGORITHM_CHECKPOINT_INTERVAL: usize = 1024;
const LOUVAIN_NODE_STATE_BYTES: usize = 384;
const BTREE_ENTRY_ESTIMATED_BYTES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionScanControl {
    Continue,
    Stop,
}

/// Storage-neutral source consumed while building an immutable analytics
/// projection. Implementations retain ownership of scan and recovery details.
pub trait ProjectionSource {
    fn visit_projection_nodes(
        &self,
        visitor: &mut dyn FnMut(NodeRecord) -> ProjectionScanControl,
    ) -> std::result::Result<ProjectionScanControl, String>;

    fn visit_projection_relationships(
        &self,
        visitor: &mut dyn FnMut(RelRecord) -> ProjectionScanControl,
    ) -> std::result::Result<ProjectionScanControl, String>;
}

/// Internal execution seam used by the embedded executor to attach
/// cancellation without moving runtime orchestration into this crate.
pub trait ProjectedGraphExecution {
    fn page_rank_with_context(
        &self,
        options: PageRankOptions,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<PageRankScore>>;

    fn hierarchical_louvain_communities_with_context(
        &self,
        options: LouvainOptions,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<HierarchicalCommunityAssignment>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionLayout {
    Outgoing,
    Incoming,
    Bidirectional,
    Undirected,
}

impl ProjectionLayout {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Outgoing => "outgoing",
            Self::Incoming => "incoming",
            Self::Bidirectional => "bidirectional",
            Self::Undirected => "undirected",
        }
    }

    fn stores_outgoing(self) -> bool {
        matches!(
            self,
            Self::Outgoing | Self::Bidirectional | Self::Undirected
        )
    }

    fn stores_incoming(self) -> bool {
        matches!(self, Self::Incoming | Self::Bidirectional)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionMemoryBudget {
    max_bytes: Option<NonZeroUsize>,
}

impl ProjectionMemoryBudget {
    pub const fn unlimited() -> Self {
        Self { max_bytes: None }
    }

    pub const fn new(max_bytes: NonZeroUsize) -> Self {
        Self {
            max_bytes: Some(max_bytes),
        }
    }

    pub fn max_bytes(self) -> Option<usize> {
        self.max_bytes.map(NonZeroUsize::get)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionMemoryEstimate {
    pub layout: ProjectionLayout,
    pub node_count: usize,
    pub relationship_count: usize,
    pub projected_edge_count: usize,
    pub estimated_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphAlgorithmMemoryEstimate {
    pub projection_bytes: usize,
    pub algorithm_peak_bytes: usize,
    pub result_bytes: usize,
    pub total_peak_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionMemoryAdmissionError {
    pub estimate: ProjectionMemoryEstimate,
    pub budget_bytes: usize,
    pub storage_error: Option<String>,
}

impl Display for ProjectionMemoryAdmissionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(error) = &self.storage_error {
            return write!(
                formatter,
                "analytics projection storage scan failed: {error}"
            );
        }
        write!(
            formatter,
            "analytics projection layout '{}' requires an estimated {} bytes for {} nodes and {} relationships, exceeding the {} byte budget",
            self.estimate.layout.as_str(),
            self.estimate.estimated_bytes,
            self.estimate.node_count,
            self.estimate.relationship_count,
            self.budget_bytes,
        )
    }
}

impl std::error::Error for ProjectionMemoryAdmissionError {}

impl ProjectionMemoryAdmissionError {
    fn storage(error: impl Display) -> Self {
        Self {
            estimate: ProjectionMemoryEstimate {
                layout: ProjectionLayout::Bidirectional,
                node_count: 0,
                relationship_count: 0,
                projected_edge_count: 0,
                estimated_bytes: 0,
            },
            budget_bytes: 0,
            storage_error: Some(error.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedGraph {
    nodes: Vec<NodeId>,
    offsets: Vec<usize>,
    targets: Vec<usize>,
    incoming_offsets: Option<Vec<usize>>,
    incoming_sources: Option<Vec<usize>>,
    layout: ProjectionLayout,
    edge_count: usize,
    memory_estimate: ProjectionMemoryEstimate,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageRankOptions {
    pub iterations: usize,
    pub damping: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageRankScore {
    pub node: NodeId,
    pub score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LouvainOptions {
    pub max_iterations: usize,
    pub max_levels: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommunityAssignment {
    pub node: NodeId,
    pub community: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HierarchicalCommunityAssignment {
    pub level: usize,
    pub node: NodeId,
    pub community: NodeId,
}

enum UndirectedNeighborIndexes<'a> {
    Projected(std::iter::Copied<std::slice::Iter<'a, usize>>),
    Materialized(std::iter::Copied<std::collections::btree_set::Iter<'a, usize>>),
}

impl Iterator for UndirectedNeighborIndexes<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Projected(iter) => iter.next(),
            Self::Materialized(iter) => iter.next(),
        }
    }
}

impl Default for PageRankOptions {
    fn default() -> Self {
        Self {
            iterations: 20,
            damping: 0.85,
        }
    }
}

impl Default for LouvainOptions {
    fn default() -> Self {
        Self {
            max_iterations: 20,
            max_levels: 1,
        }
    }
}

impl ProjectedGraph {
    pub fn empty() -> Self {
        Self::from_nodes_without_edges(Vec::new(), ProjectionLayout::Bidirectional)
    }

    pub fn from_parts(
        nodes: Vec<NodeId>,
        offsets: Vec<usize>,
        targets: Vec<usize>,
        incoming_offsets: Vec<usize>,
        incoming_sources: Vec<usize>,
    ) -> std::result::Result<Self, String> {
        validate_offsets("csr_offsets", nodes.len(), &offsets, targets.len())?;
        validate_offsets(
            "csc_offsets",
            nodes.len(),
            &incoming_offsets,
            incoming_sources.len(),
        )?;
        validate_indexes("csr_targets", nodes.len(), &targets)?;
        validate_indexes("csc_sources", nodes.len(), &incoming_sources)?;
        let relationship_count = targets.len();
        let memory_estimate = projection_memory_estimate(
            ProjectionLayout::Bidirectional,
            nodes.len(),
            relationship_count,
        );
        Ok(Self {
            nodes,
            offsets,
            targets,
            incoming_offsets: Some(incoming_offsets),
            incoming_sources: Some(incoming_sources),
            layout: ProjectionLayout::Bidirectional,
            edge_count: relationship_count,
            memory_estimate,
        })
    }

    pub fn from_store<S>(store: &S, rel_type: Option<RelTypeId>) -> Self
    where
        S: ProjectionSource + ?Sized,
    {
        Self::from_store_with_node_filter(store, rel_type, |_| true)
    }

    pub fn from_store_with_node_filter<S>(
        store: &S,
        rel_type: Option<RelTypeId>,
        include_node: impl Fn(&NodeRecord) -> bool,
    ) -> Self
    where
        S: ProjectionSource + ?Sized,
    {
        Self::try_from_store_with_node_filter_and_layout(
            store,
            rel_type,
            include_node,
            ProjectionLayout::Bidirectional,
            ProjectionMemoryBudget::unlimited(),
        )
        .expect("unlimited analytics projection is admitted")
    }

    pub fn try_from_store_with_node_filter_and_layout<S>(
        store: &S,
        rel_type: Option<RelTypeId>,
        include_node: impl Fn(&NodeRecord) -> bool,
        layout: ProjectionLayout,
        budget: ProjectionMemoryBudget,
    ) -> std::result::Result<Self, ProjectionMemoryAdmissionError>
    where
        S: ProjectionSource + ?Sized,
    {
        let nodes = collect_projected_node_ids(store, layout, budget, include_node)?;
        Self::try_from_nodes_and_relationships(
            store,
            nodes,
            move |relationship| {
                rel_type
                    .map(|rel_type| relationship.rel_type == rel_type)
                    .unwrap_or(true)
            },
            layout,
            budget,
        )
    }

    pub fn from_store_labels_and_rel_types<S>(
        store: &S,
        labels: &[LabelId],
        rel_types: &[RelTypeId],
    ) -> Self
    where
        S: ProjectionSource + ?Sized,
    {
        Self::from_store_labels_and_rel_types_with_node_filter(store, labels, rel_types, |_| true)
    }

    pub fn from_store_labels_and_rel_types_with_node_filter<S>(
        store: &S,
        labels: &[LabelId],
        rel_types: &[RelTypeId],
        include_node: impl Fn(&NodeRecord) -> bool,
    ) -> Self
    where
        S: ProjectionSource + ?Sized,
    {
        Self::try_from_store_labels_and_rel_types_with_node_filter_and_layout(
            store,
            labels,
            rel_types,
            include_node,
            ProjectionLayout::Bidirectional,
            ProjectionMemoryBudget::unlimited(),
        )
        .expect("unlimited analytics projection is admitted")
    }

    pub fn try_from_store_labels_and_rel_types_with_node_filter_and_layout<S>(
        store: &S,
        labels: &[LabelId],
        rel_types: &[RelTypeId],
        include_node: impl Fn(&NodeRecord) -> bool,
        layout: ProjectionLayout,
        budget: ProjectionMemoryBudget,
    ) -> std::result::Result<Self, ProjectionMemoryAdmissionError>
    where
        S: ProjectionSource + ?Sized,
    {
        let labels = labels.iter().copied().collect::<BTreeSet<_>>();
        let nodes = collect_projected_node_ids(store, layout, budget, |node| {
            (labels.is_empty() || node.labels.iter().any(|label| labels.contains(label)))
                && include_node(node)
        })?;
        let rel_types = rel_types.iter().copied().collect::<BTreeSet<_>>();
        Self::try_from_nodes_and_relationships(
            store,
            nodes,
            move |relationship| rel_types.is_empty() || rel_types.contains(&relationship.rel_type),
            layout,
            budget,
        )
    }

    pub fn from_store_without_edges<S>(store: &S) -> Self
    where
        S: ProjectionSource + ?Sized,
    {
        Self::from_store_without_edges_with_node_filter(store, |_| true)
    }

    pub fn from_store_without_edges_with_node_filter<S>(
        store: &S,
        include_node: impl Fn(&NodeRecord) -> bool,
    ) -> Self
    where
        S: ProjectionSource + ?Sized,
    {
        Self::try_from_store_without_edges_with_node_filter_and_layout(
            store,
            include_node,
            ProjectionLayout::Bidirectional,
            ProjectionMemoryBudget::unlimited(),
        )
        .expect("unlimited analytics projection is admitted")
    }

    pub fn try_from_store_without_edges_with_node_filter_and_layout<S>(
        store: &S,
        include_node: impl Fn(&NodeRecord) -> bool,
        layout: ProjectionLayout,
        budget: ProjectionMemoryBudget,
    ) -> std::result::Result<Self, ProjectionMemoryAdmissionError>
    where
        S: ProjectionSource + ?Sized,
    {
        let nodes = collect_projected_node_ids(store, layout, budget, include_node)?;
        Self::try_from_nodes_without_edges(nodes, layout, budget)
    }

    pub fn from_store_labels_without_edges<S>(store: &S, labels: &[LabelId]) -> Self
    where
        S: ProjectionSource + ?Sized,
    {
        Self::from_store_labels_without_edges_with_node_filter(store, labels, |_| true)
    }

    pub fn from_store_labels_without_edges_with_node_filter<S>(
        store: &S,
        labels: &[LabelId],
        include_node: impl Fn(&NodeRecord) -> bool,
    ) -> Self
    where
        S: ProjectionSource + ?Sized,
    {
        Self::try_from_store_labels_without_edges_with_node_filter_and_layout(
            store,
            labels,
            include_node,
            ProjectionLayout::Bidirectional,
            ProjectionMemoryBudget::unlimited(),
        )
        .expect("unlimited analytics projection is admitted")
    }

    pub fn try_from_store_labels_without_edges_with_node_filter_and_layout<S>(
        store: &S,
        labels: &[LabelId],
        include_node: impl Fn(&NodeRecord) -> bool,
        layout: ProjectionLayout,
        budget: ProjectionMemoryBudget,
    ) -> std::result::Result<Self, ProjectionMemoryAdmissionError>
    where
        S: ProjectionSource + ?Sized,
    {
        let labels = labels.iter().copied().collect::<BTreeSet<_>>();
        let nodes = collect_projected_node_ids(store, layout, budget, |node| {
            node.labels.iter().any(|label| labels.contains(label)) && include_node(node)
        })?;
        Self::try_from_nodes_without_edges(nodes, layout, budget)
    }

    fn from_nodes_without_edges(nodes: Vec<NodeId>, layout: ProjectionLayout) -> Self {
        Self::try_from_nodes_without_edges(nodes, layout, ProjectionMemoryBudget::unlimited())
            .expect("unlimited analytics projection is admitted")
    }

    fn try_from_nodes_without_edges(
        nodes: Vec<NodeId>,
        layout: ProjectionLayout,
        budget: ProjectionMemoryBudget,
    ) -> std::result::Result<Self, ProjectionMemoryAdmissionError> {
        let memory_estimate = projection_memory_estimate(layout, nodes.len(), 0);
        admit_projection(memory_estimate, budget)?;
        let offsets = vec![0; nodes.len() + 1];
        let incoming_offsets = layout.stores_incoming().then(|| offsets.clone());
        Ok(Self {
            nodes,
            offsets,
            targets: Vec::new(),
            incoming_offsets,
            incoming_sources: layout.stores_incoming().then(Vec::new),
            layout,
            edge_count: 0,
            memory_estimate,
        })
    }

    fn try_from_nodes_and_relationships<S>(
        store: &S,
        nodes: Vec<NodeId>,
        include_relationship: impl Fn(&RelRecord) -> bool,
        layout: ProjectionLayout,
        budget: ProjectionMemoryBudget,
    ) -> std::result::Result<Self, ProjectionMemoryAdmissionError>
    where
        S: ProjectionSource + ?Sized,
    {
        let mut relationship_count = 0usize;
        store
            .visit_projection_relationships(&mut |relationship| {
                if include_relationship(&relationship)
                    && nodes.binary_search(&relationship.source).is_ok()
                    && nodes.binary_search(&relationship.target).is_ok()
                {
                    relationship_count = relationship_count.saturating_add(1);
                }
                ProjectionScanControl::Continue
            })
            .map_err(ProjectionMemoryAdmissionError::storage)?;
        let memory_estimate = projection_memory_estimate(layout, nodes.len(), relationship_count);
        admit_projection(memory_estimate, budget)?;

        let mut adjacency = layout
            .stores_outgoing()
            .then(|| vec![Vec::new(); nodes.len()]);
        let mut incoming = layout
            .stores_incoming()
            .then(|| vec![Vec::new(); nodes.len()]);

        store
            .visit_projection_relationships(&mut |relationship| {
                if !include_relationship(&relationship) {
                    return ProjectionScanControl::Continue;
                }
                let Ok(source) = nodes.binary_search(&relationship.source) else {
                    return ProjectionScanControl::Continue;
                };
                let Ok(target) = nodes.binary_search(&relationship.target) else {
                    return ProjectionScanControl::Continue;
                };
                if let Some(adjacency) = adjacency.as_mut() {
                    adjacency[source].push(target);
                    if layout == ProjectionLayout::Undirected && source != target {
                        adjacency[target].push(source);
                    }
                }
                if let Some(incoming) = incoming.as_mut() {
                    incoming[target].push(source);
                }
                ProjectionScanControl::Continue
            })
            .map_err(ProjectionMemoryAdmissionError::storage)?;

        let (offsets, targets) = adjacency
            .map(build_compressed_adjacency)
            .unwrap_or_else(|| (vec![0; nodes.len() + 1], Vec::new()));
        let (incoming_offsets, incoming_sources) = incoming
            .map(build_compressed_adjacency)
            .map(|(offsets, sources)| (Some(offsets), Some(sources)))
            .unwrap_or((None, None));
        let edge_count = match layout {
            ProjectionLayout::Incoming => incoming_sources
                .as_deref()
                .map_or(0, |sources| sources.len()),
            ProjectionLayout::Undirected => (0..nodes.len())
                .map(|source| {
                    targets[offsets[source]..offsets[source + 1]]
                        .iter()
                        .filter(|target| source <= **target)
                        .count()
                })
                .sum(),
            ProjectionLayout::Outgoing | ProjectionLayout::Bidirectional => targets.len(),
        };

        Ok(Self {
            nodes,
            offsets,
            targets,
            incoming_offsets,
            incoming_sources,
            layout,
            edge_count,
            memory_estimate,
        })
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edge_count
    }

    pub fn layout(&self) -> ProjectionLayout {
        self.layout
    }

    pub fn memory_estimate(&self) -> ProjectionMemoryEstimate {
        self.memory_estimate
    }

    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    pub fn csr_offsets(&self) -> &[usize] {
        &self.offsets
    }

    pub fn csr_targets(&self) -> &[usize] {
        &self.targets
    }

    pub fn csc_offsets(&self) -> &[usize] {
        self.incoming_offsets.as_deref().unwrap_or(&[])
    }

    pub fn csc_sources(&self) -> &[usize] {
        self.incoming_sources.as_deref().unwrap_or(&[])
    }

    pub fn outgoing_targets(&self, node: NodeId) -> Option<impl Iterator<Item = NodeId> + '_> {
        if !self.layout.stores_outgoing() {
            return None;
        }
        let index = self.nodes.iter().position(|candidate| *candidate == node)?;
        Some(
            self.outgoing_target_indexes(index)
                .map(|target| self.nodes[target]),
        )
    }

    pub fn incoming_sources(&self, node: NodeId) -> Option<impl Iterator<Item = NodeId> + '_> {
        if !self.layout.stores_incoming() {
            return None;
        }
        let index = self.nodes.iter().position(|candidate| *candidate == node)?;
        Some(
            self.incoming_source_indexes(index)
                .map(|source| self.nodes[source]),
        )
    }

    pub fn page_rank_memory_estimate(&self) -> GraphAlgorithmMemoryEstimate {
        let node_count = self.nodes.len();
        let rank_bytes = node_count.saturating_mul(std::mem::size_of::<f64>());
        let result_bytes = estimated_vec_bytes::<PageRankScore>(node_count);
        let algorithm_peak_bytes = rank_bytes.saturating_add(result_bytes);
        GraphAlgorithmMemoryEstimate {
            projection_bytes: self.memory_estimate.estimated_bytes,
            algorithm_peak_bytes,
            result_bytes,
            total_peak_bytes: self
                .memory_estimate
                .estimated_bytes
                .saturating_add(algorithm_peak_bytes),
        }
    }

    pub fn louvain_memory_estimate(&self, options: LouvainOptions) -> GraphAlgorithmMemoryEstimate {
        let node_count = self.nodes.len();
        let max_levels = options.max_levels.max(1);
        let result_count = node_count.saturating_mul(max_levels);
        let result_bytes = estimated_vec_bytes::<HierarchicalCommunityAssignment>(result_count);
        let contracted_graph_bytes = self.memory_estimate.estimated_bytes.saturating_mul(2);
        let node_state_bytes = node_count.saturating_mul(LOUVAIN_NODE_STATE_BYTES);
        let candidate_count =
            node_count.min(self.memory_estimate.projected_edge_count.saturating_add(1));
        let candidate_bytes = candidate_count.saturating_mul(BTREE_ENTRY_ESTIMATED_BYTES);
        let materialized_undirected_bytes = if self.layout != ProjectionLayout::Undirected {
            self.memory_estimate
                .projected_edge_count
                .saturating_mul(2)
                .saturating_mul(BTREE_ENTRY_ESTIMATED_BYTES)
                .saturating_add(node_count.saturating_mul(std::mem::size_of::<BTreeSet<usize>>()))
        } else {
            0
        };
        let algorithm_peak_bytes = contracted_graph_bytes
            .saturating_add(node_state_bytes)
            .saturating_add(candidate_bytes)
            .saturating_add(materialized_undirected_bytes)
            .saturating_add(result_bytes);
        GraphAlgorithmMemoryEstimate {
            projection_bytes: self.memory_estimate.estimated_bytes,
            algorithm_peak_bytes,
            result_bytes,
            total_peak_bytes: self
                .memory_estimate
                .estimated_bytes
                .saturating_add(algorithm_peak_bytes),
        }
    }

    pub fn page_rank(&self, options: PageRankOptions) -> Vec<PageRankScore> {
        self.page_rank_with_context_internal(options, None)
            .expect("page rank without runtime cancellation cannot fail")
    }

    fn page_rank_with_context_internal(
        &self,
        options: PageRankOptions,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<PageRankScore>> {
        algorithm_checkpoint(task_context)?;
        if !self.layout.stores_outgoing() {
            return Ok(Vec::new());
        }
        let node_count = self.nodes.len();
        if node_count == 0 {
            return Ok(Vec::new());
        }

        let damping = options.damping.clamp(0.0, 1.0);
        let mut ranks = vec![1.0 / node_count as f64; node_count];
        for _ in 0..options.iterations {
            algorithm_checkpoint(task_context)?;
            let mut dangling = 0.0;
            for (index, rank) in ranks.iter().enumerate() {
                algorithm_checkpoint_periodically(task_context, index)?;
                if self.out_degree(index) == 0 {
                    dangling += rank;
                }
            }
            let mut next = vec![(1.0 - damping) / node_count as f64; node_count];
            let dangling_share = damping * dangling / node_count as f64;
            for score in &mut next {
                *score += dangling_share;
            }

            for (source, rank) in ranks.iter().enumerate() {
                algorithm_checkpoint_periodically(task_context, source)?;
                let out_degree = self.out_degree(source);
                if out_degree == 0 {
                    continue;
                }
                let contribution = damping * rank / out_degree as f64;
                for (edge_ordinal, target) in self.outgoing_target_indexes(source).enumerate() {
                    algorithm_checkpoint_periodically(task_context, edge_ordinal)?;
                    next[target] += contribution;
                }
            }
            ranks = next;
        }

        let mut scores = self
            .nodes
            .iter()
            .copied()
            .zip(ranks)
            .map(|(node, score)| PageRankScore { node, score })
            .collect::<Vec<_>>();
        scores.sort_unstable_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.node.cmp(&right.node))
        });
        algorithm_checkpoint(task_context)?;
        Ok(scores)
    }

    pub fn louvain_communities(&self, options: LouvainOptions) -> Vec<CommunityAssignment> {
        self.single_level_louvain(options, None)
            .expect("Louvain without runtime cancellation cannot fail")
    }

    pub fn hierarchical_louvain_communities(
        &self,
        options: LouvainOptions,
    ) -> Vec<HierarchicalCommunityAssignment> {
        self.hierarchical_louvain_communities_with_context_internal(options, None)
            .expect("Louvain without runtime cancellation cannot fail")
    }

    fn hierarchical_louvain_communities_with_context_internal(
        &self,
        options: LouvainOptions,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<HierarchicalCommunityAssignment>> {
        algorithm_checkpoint(task_context)?;
        let max_levels = options.max_levels.max(1);
        let mut graph = self.clone();
        let mut original_to_current = (0..self.nodes.len()).collect::<Vec<_>>();
        let mut output = Vec::new();

        for level in 0..max_levels {
            algorithm_checkpoint(task_context)?;
            let assignments = graph.single_level_louvain(
                LouvainOptions {
                    max_levels: 1,
                    ..options
                },
                task_context,
            )?;
            if assignments.is_empty() {
                break;
            }
            for (original_index, current_index) in original_to_current.iter().copied().enumerate() {
                algorithm_checkpoint_periodically(task_context, original_index)?;
                let community = assignments[current_index].community;
                output.push(HierarchicalCommunityAssignment {
                    level,
                    node: self.nodes[original_index],
                    community,
                });
            }
            if level + 1 == max_levels {
                break;
            }
            let contracted = graph.contract_by_communities(&assignments, task_context)?;
            if contracted.nodes.len() == graph.nodes.len() {
                break;
            }
            let contracted_positions = contracted
                .nodes
                .iter()
                .enumerate()
                .map(|(index, node)| (*node, index))
                .collect::<BTreeMap<_, _>>();
            let mut next_original_to_current = Vec::with_capacity(original_to_current.len());
            for (ordinal, current_index) in original_to_current.into_iter().enumerate() {
                algorithm_checkpoint_periodically(task_context, ordinal)?;
                let community = assignments[current_index].community;
                next_original_to_current.push(contracted_positions[&community]);
            }
            original_to_current = next_original_to_current;
            graph = contracted;
        }

        algorithm_checkpoint(task_context)?;
        Ok(output)
    }

    fn single_level_louvain(
        &self,
        options: LouvainOptions,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<CommunityAssignment>> {
        algorithm_checkpoint(task_context)?;
        let node_count = self.nodes.len();
        if node_count == 0 {
            return Ok(Vec::new());
        }

        let materialized_adjacency = if self.layout != ProjectionLayout::Undirected {
            Some(self.undirected_adjacency(task_context)?)
        } else {
            None
        };
        let mut degrees = Vec::with_capacity(node_count);
        for node in 0..node_count {
            algorithm_checkpoint_periodically(task_context, node)?;
            let mut degree = 0usize;
            for (edge_ordinal, _) in self
                .undirected_neighbor_indexes(node, materialized_adjacency.as_deref())
                .enumerate()
            {
                algorithm_checkpoint_periodically(task_context, edge_ordinal)?;
                degree = degree.saturating_add(1);
            }
            degrees.push(degree as f64);
        }
        let total_degree = degrees.iter().sum::<f64>();
        if total_degree == 0.0 {
            return Ok(self
                .nodes
                .iter()
                .copied()
                .map(|node| CommunityAssignment {
                    node,
                    community: node,
                })
                .collect());
        }

        let mut communities = (0..node_count).collect::<Vec<_>>();
        let mut community_degrees = degrees.clone();
        for _ in 0..options.max_iterations {
            algorithm_checkpoint(task_context)?;
            let mut changed = false;
            for node in 0..node_count {
                algorithm_checkpoint_periodically(task_context, node)?;
                let current = communities[node];
                let node_degree = degrees[node];
                community_degrees[current] -= node_degree;

                let mut candidates = BTreeSet::from([current]);
                for (edge_ordinal, neighbor) in self
                    .undirected_neighbor_indexes(node, materialized_adjacency.as_deref())
                    .enumerate()
                {
                    algorithm_checkpoint_periodically(task_context, edge_ordinal)?;
                    candidates.insert(communities[neighbor]);
                }

                let mut best = current;
                let mut best_gain = 0.0;
                for (candidate_ordinal, candidate) in candidates.into_iter().enumerate() {
                    algorithm_checkpoint_periodically(task_context, candidate_ordinal)?;
                    let mut links_to_candidate = 0usize;
                    for (edge_ordinal, neighbor) in self
                        .undirected_neighbor_indexes(node, materialized_adjacency.as_deref())
                        .enumerate()
                    {
                        algorithm_checkpoint_periodically(task_context, edge_ordinal)?;
                        links_to_candidate = links_to_candidate
                            .saturating_add(usize::from(communities[neighbor] == candidate));
                    }
                    let links_to_candidate = links_to_candidate as f64;
                    let gain = links_to_candidate
                        - (node_degree * community_degrees[candidate] / total_degree);
                    match gain.total_cmp(&best_gain) {
                        Ordering::Greater => {
                            best = candidate;
                            best_gain = gain;
                        }
                        Ordering::Equal => {
                            let candidate_representative = community_representative(
                                candidate,
                                &communities,
                                &self.nodes,
                                task_context,
                            )?;
                            let best_representative = community_representative(
                                best,
                                &communities,
                                &self.nodes,
                                task_context,
                            )?;
                            if candidate_representative < best_representative {
                                best = candidate;
                                best_gain = gain;
                            }
                        }
                        _ => {}
                    }
                }

                community_degrees[best] += node_degree;
                if best != current {
                    communities[node] = best;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        let mut representatives = BTreeMap::<usize, NodeId>::new();
        for (index, community) in communities.iter().copied().enumerate() {
            algorithm_checkpoint_periodically(task_context, index)?;
            representatives
                .entry(community)
                .and_modify(|node| *node = (*node).min(self.nodes[index]))
                .or_insert(self.nodes[index]);
        }

        let output = self
            .nodes
            .iter()
            .copied()
            .enumerate()
            .map(|(index, node)| CommunityAssignment {
                node,
                community: representatives[&communities[index]],
            })
            .collect();
        algorithm_checkpoint(task_context)?;
        Ok(output)
    }

    fn contract_by_communities(
        &self,
        assignments: &[CommunityAssignment],
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Self> {
        algorithm_checkpoint(task_context)?;
        let nodes = assignments
            .iter()
            .map(|assignment| assignment.community)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let node_positions = nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (*node, index))
            .collect::<BTreeMap<_, _>>();
        let original_positions = self
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (*node, index))
            .collect::<BTreeMap<_, _>>();
        let mut adjacency = vec![Vec::new(); nodes.len()];
        let mut incoming = self
            .layout
            .stores_incoming()
            .then(|| vec![Vec::new(); nodes.len()]);

        let mut add_edge = |source: usize, target: usize| {
            let source_community = assignments[source].community;
            let target_community = assignments[target].community;
            if source_community == target_community {
                return;
            }
            let source_position = node_positions[&source_community];
            let target_position = node_positions[&target_community];
            adjacency[source_position].push(target_position);
            if let Some(incoming) = incoming.as_mut() {
                incoming[target_position].push(source_position);
            }
        };
        if self.layout == ProjectionLayout::Incoming {
            for target in 0..self.nodes.len() {
                algorithm_checkpoint_periodically(task_context, target)?;
                for (edge_ordinal, source) in self.incoming_source_indexes(target).enumerate() {
                    algorithm_checkpoint_periodically(task_context, edge_ordinal)?;
                    add_edge(source, target);
                }
            }
        } else {
            for source in 0..self.nodes.len() {
                algorithm_checkpoint_periodically(task_context, source)?;
                for (edge_ordinal, target) in self.outgoing_target_indexes(source).enumerate() {
                    algorithm_checkpoint_periodically(task_context, edge_ordinal)?;
                    add_edge(source, target);
                }
            }
        }
        for assignment in assignments {
            debug_assert!(original_positions.contains_key(&assignment.node));
        }
        let (offsets, targets) = build_compressed_adjacency(adjacency);
        let (incoming_offsets, incoming_sources) = incoming
            .map(build_compressed_adjacency)
            .map(|(offsets, sources)| (Some(offsets), Some(sources)))
            .unwrap_or((None, None));
        let edge_count = if self.layout == ProjectionLayout::Undirected {
            let mut edge_count = 0usize;
            for source in 0..nodes.len() {
                algorithm_checkpoint_periodically(task_context, source)?;
                for (edge_ordinal, target) in targets[offsets[source]..offsets[source + 1]]
                    .iter()
                    .enumerate()
                {
                    algorithm_checkpoint_periodically(task_context, edge_ordinal)?;
                    edge_count = edge_count.saturating_add(usize::from(source <= *target));
                }
            }
            edge_count
        } else {
            targets.len()
        };
        let memory_estimate = projection_memory_estimate(self.layout, nodes.len(), edge_count);
        algorithm_checkpoint(task_context)?;
        Ok(Self {
            nodes,
            offsets,
            targets,
            incoming_offsets,
            incoming_sources,
            layout: self.layout,
            edge_count,
            memory_estimate,
        })
    }

    fn out_degree(&self, index: usize) -> usize {
        self.offsets[index + 1] - self.offsets[index]
    }

    fn outgoing_target_indexes(&self, index: usize) -> impl Iterator<Item = usize> + '_ {
        self.targets[self.offsets[index]..self.offsets[index + 1]]
            .iter()
            .copied()
    }

    fn incoming_source_indexes(&self, index: usize) -> impl Iterator<Item = usize> + '_ {
        let offsets = self
            .incoming_offsets
            .as_deref()
            .expect("incoming indexes require an incoming projection");
        self.incoming_sources
            .as_deref()
            .expect("incoming indexes require an incoming projection")
            [offsets[index]..offsets[index + 1]]
            .iter()
            .copied()
    }

    fn undirected_adjacency(
        &self,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<BTreeSet<usize>>> {
        let mut adjacency = vec![BTreeSet::new(); self.nodes.len()];
        if self.layout == ProjectionLayout::Incoming {
            for target in 0..self.nodes.len() {
                algorithm_checkpoint_periodically(task_context, target)?;
                for (edge_ordinal, source) in self.incoming_source_indexes(target).enumerate() {
                    algorithm_checkpoint_periodically(task_context, edge_ordinal)?;
                    if source == target {
                        continue;
                    }
                    adjacency[source].insert(target);
                    adjacency[target].insert(source);
                }
            }
            return Ok(adjacency);
        }
        for source in 0..self.nodes.len() {
            algorithm_checkpoint_periodically(task_context, source)?;
            for (edge_ordinal, target) in self.outgoing_target_indexes(source).enumerate() {
                algorithm_checkpoint_periodically(task_context, edge_ordinal)?;
                if source == target {
                    continue;
                }
                adjacency[source].insert(target);
                adjacency[target].insert(source);
            }
        }
        Ok(adjacency)
    }

    fn undirected_neighbor_indexes<'a>(
        &'a self,
        index: usize,
        materialized: Option<&'a [BTreeSet<usize>]>,
    ) -> UndirectedNeighborIndexes<'a> {
        if self.layout == ProjectionLayout::Undirected {
            return UndirectedNeighborIndexes::Projected(
                self.targets[self.offsets[index]..self.offsets[index + 1]]
                    .iter()
                    .copied(),
            );
        }
        UndirectedNeighborIndexes::Materialized(
            materialized.expect("directed projection requires an undirected view")[index]
                .iter()
                .copied(),
        )
    }
}

impl ProjectedGraphExecution for ProjectedGraph {
    fn page_rank_with_context(
        &self,
        options: PageRankOptions,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<PageRankScore>> {
        self.page_rank_with_context_internal(options, task_context)
    }

    fn hierarchical_louvain_communities_with_context(
        &self,
        options: LouvainOptions,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<HierarchicalCommunityAssignment>> {
        self.hierarchical_louvain_communities_with_context_internal(options, task_context)
    }
}

fn community_representative(
    community: usize,
    assignments: &[usize],
    nodes: &[NodeId],
    task_context: Option<&RuntimeTaskContext>,
) -> Result<NodeId> {
    let mut representative = None;
    for (index, assigned) in assignments.iter().enumerate() {
        algorithm_checkpoint_periodically(task_context, index)?;
        if *assigned == community {
            representative =
                Some(representative.map_or(nodes[index], |node: NodeId| node.min(nodes[index])));
        }
    }
    Ok(representative.unwrap_or(nodes[community]))
}

fn estimated_vec_bytes<T>(item_count: usize) -> usize {
    item_count
        .saturating_mul(std::mem::size_of::<T>())
        .saturating_mul(2)
}

fn algorithm_checkpoint(task_context: Option<&RuntimeTaskContext>) -> Result<()> {
    match task_context {
        Some(task_context) => task_context
            .checkpoint()
            .map_err(|reason| HawDBError::Execution(format!("runtime task stopped: {reason}"))),
        None => Ok(()),
    }
}

#[inline]
fn algorithm_checkpoint_periodically(
    task_context: Option<&RuntimeTaskContext>,
    ordinal: usize,
) -> Result<()> {
    if let Some(task_context) = task_context
        && ordinal.is_multiple_of(ALGORITHM_CHECKPOINT_INTERVAL)
    {
        task_context
            .checkpoint()
            .map_err(|reason| HawDBError::Execution(format!("runtime task stopped: {reason}")))?;
    }
    Ok(())
}

fn collect_projected_node_ids<S>(
    store: &S,
    layout: ProjectionLayout,
    budget: ProjectionMemoryBudget,
    include_node: impl Fn(&NodeRecord) -> bool,
) -> std::result::Result<Vec<NodeId>, ProjectionMemoryAdmissionError>
where
    S: ProjectionSource + ?Sized,
{
    let mut nodes = Vec::new();
    let mut admission_error = None;
    store
        .visit_projection_nodes(&mut |node| {
            if !include_node(&node) {
                return ProjectionScanControl::Continue;
            }
            let estimate = projection_memory_estimate(layout, nodes.len().saturating_add(1), 0);
            if let Err(error) = admit_projection(estimate, budget) {
                admission_error = Some(error);
                return ProjectionScanControl::Stop;
            }
            nodes.push(node.id);
            ProjectionScanControl::Continue
        })
        .map_err(ProjectionMemoryAdmissionError::storage)?;
    if let Some(error) = admission_error {
        return Err(error);
    }
    Ok(nodes)
}

fn projection_memory_estimate(
    layout: ProjectionLayout,
    node_count: usize,
    relationship_count: usize,
) -> ProjectionMemoryEstimate {
    let outgoing_edge_count = match layout {
        ProjectionLayout::Incoming => 0,
        ProjectionLayout::Undirected => relationship_count.saturating_mul(2),
        ProjectionLayout::Outgoing | ProjectionLayout::Bidirectional => relationship_count,
    };
    let incoming_edge_count = if layout.stores_incoming() {
        relationship_count
    } else {
        0
    };
    let projected_edge_count = outgoing_edge_count.saturating_add(incoming_edge_count);
    let direction_count =
        usize::from(layout.stores_outgoing()).saturating_add(usize::from(layout.stores_incoming()));
    let offset_direction_count = 1usize.saturating_add(usize::from(layout.stores_incoming()));
    let node_bytes = node_count.saturating_mul(std::mem::size_of::<NodeId>());
    let outer_adjacency_bytes = node_count
        .saturating_mul(std::mem::size_of::<Vec<usize>>())
        .saturating_mul(direction_count);
    let offset_bytes = node_count
        .saturating_add(1)
        .saturating_mul(std::mem::size_of::<usize>())
        .saturating_mul(offset_direction_count);
    // Building compressed adjacency temporarily overlaps the per-node vectors
    // and their final compressed neighbor array, so account for both copies.
    let edge_bytes = projected_edge_count
        .saturating_mul(std::mem::size_of::<usize>())
        .saturating_mul(2);
    ProjectionMemoryEstimate {
        layout,
        node_count,
        relationship_count,
        projected_edge_count,
        estimated_bytes: node_bytes
            .saturating_add(outer_adjacency_bytes)
            .saturating_add(offset_bytes)
            .saturating_add(edge_bytes),
    }
}

fn admit_projection(
    estimate: ProjectionMemoryEstimate,
    budget: ProjectionMemoryBudget,
) -> std::result::Result<(), ProjectionMemoryAdmissionError> {
    if let Some(budget_bytes) = budget.max_bytes()
        && estimate.estimated_bytes > budget_bytes
    {
        return Err(ProjectionMemoryAdmissionError {
            estimate,
            budget_bytes,
            storage_error: None,
        });
    }
    Ok(())
}

fn build_compressed_adjacency(mut adjacency: Vec<Vec<usize>>) -> (Vec<usize>, Vec<usize>) {
    let mut offsets = Vec::with_capacity(adjacency.len() + 1);
    let mut neighbors = Vec::new();
    offsets.push(0);
    for neighbors_for_node in &mut adjacency {
        neighbors_for_node.sort_unstable();
        neighbors_for_node.dedup();
        neighbors.extend(neighbors_for_node.iter().copied());
        offsets.push(neighbors.len());
    }
    (offsets, neighbors)
}

fn validate_offsets(
    name: &str,
    node_count: usize,
    offsets: &[usize],
    edge_count: usize,
) -> std::result::Result<(), String> {
    if offsets.len() != node_count + 1 {
        return Err(format!(
            "{name} length {} does not match node count {node_count}",
            offsets.len()
        ));
    }
    if offsets.first() != Some(&0) {
        return Err(format!("{name} must start at 0"));
    }
    if offsets.last() != Some(&edge_count) {
        return Err(format!("{name} must end at edge count {edge_count}"));
    }
    if offsets.windows(2).any(|window| window[0] > window[1]) {
        return Err(format!("{name} must be monotonic"));
    }
    Ok(())
}

fn validate_indexes(
    name: &str,
    node_count: usize,
    indexes: &[usize],
) -> std::result::Result<(), String> {
    if let Some(index) = indexes.iter().find(|index| **index >= node_count) {
        return Err(format!("{name} contains out-of-range index {index}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        projection_memory_estimate, LouvainOptions, PageRankOptions, ProjectedGraph,
        ProjectedGraphExecution, ProjectionLayout, ProjectionMemoryBudget, ProjectionScanControl,
        ProjectionSource,
    };
    use hawdb_core::{Catalog, Value};
    use hawdb_storage::{NodeId, NodeRecord, RelId, RelRecord};
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;

    #[derive(Default)]
    struct GraphStore {
        nodes: Vec<NodeRecord>,
        relationships: Vec<RelRecord>,
    }

    impl GraphStore {
        fn in_memory() -> Self {
            Self::default()
        }

        fn create_node(
            &mut self,
            catalog: &mut Catalog,
            label: &str,
            properties: BTreeMap<String, Value>,
        ) -> Result<NodeId, String> {
            let id = NodeId(self.nodes.len() as u64 + 1);
            self.nodes.push(NodeRecord {
                id,
                labels: BTreeSet::from([catalog.get_or_create_label(label)]),
                properties,
            });
            Ok(id)
        }

        fn create_relationship(
            &mut self,
            catalog: &mut Catalog,
            source: NodeId,
            target: NodeId,
            rel_type: &str,
            properties: BTreeMap<String, Value>,
        ) -> Result<RelId, String> {
            let id = RelId(self.relationships.len() as u64 + 1);
            self.relationships.push(RelRecord {
                id,
                source,
                target,
                rel_type: catalog.get_or_create_rel_type(rel_type),
                properties,
            });
            Ok(id)
        }
    }

    impl ProjectionSource for GraphStore {
        fn visit_projection_nodes(
            &self,
            visitor: &mut dyn FnMut(NodeRecord) -> ProjectionScanControl,
        ) -> Result<ProjectionScanControl, String> {
            for node in self.nodes.iter().cloned() {
                if visitor(node) == ProjectionScanControl::Stop {
                    return Ok(ProjectionScanControl::Stop);
                }
            }
            Ok(ProjectionScanControl::Continue)
        }

        fn visit_projection_relationships(
            &self,
            visitor: &mut dyn FnMut(RelRecord) -> ProjectionScanControl,
        ) -> Result<ProjectionScanControl, String> {
            for relationship in self.relationships.iter().cloned() {
                if visitor(relationship) == ProjectionScanControl::Stop {
                    return Ok(ProjectionScanControl::Stop);
                }
            }
            Ok(ProjectionScanControl::Continue)
        }
    }

    #[test]
    fn algorithm_layouts_only_materialize_required_directions() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();
        let rel_type = catalog.rel_type_id("MENTIONS");

        let page_rank = ProjectedGraph::try_from_store_with_node_filter_and_layout(
            &store,
            rel_type,
            |_| true,
            ProjectionLayout::Outgoing,
            ProjectionMemoryBudget::unlimited(),
        )
        .unwrap();
        assert_eq!(page_rank.layout(), ProjectionLayout::Outgoing);
        assert_eq!(page_rank.edge_count(), 1);
        assert_eq!(page_rank.csr_targets().len(), 1);
        assert!(page_rank.csc_offsets().is_empty());
        assert!(page_rank.incoming_sources(target).is_none());

        let louvain = ProjectedGraph::try_from_store_with_node_filter_and_layout(
            &store,
            rel_type,
            |_| true,
            ProjectionLayout::Undirected,
            ProjectionMemoryBudget::unlimited(),
        )
        .unwrap();
        assert_eq!(louvain.layout(), ProjectionLayout::Undirected);
        assert_eq!(louvain.edge_count(), 1);
        assert_eq!(louvain.csr_targets().len(), 2);
        assert!(louvain.csc_offsets().is_empty());
        assert_eq!(
            louvain.louvain_communities(LouvainOptions::default()).len(),
            2
        );

        let bidirectional = ProjectedGraph::from_store(&store, rel_type);
        assert!(
            page_rank.memory_estimate().estimated_bytes
                < bidirectional.memory_estimate().estimated_bytes
        );
        assert!(
            louvain.memory_estimate().estimated_bytes
                < bidirectional.memory_estimate().estimated_bytes
        );
    }

    #[test]
    fn projection_memory_admission_fails_before_adjacency_allocation() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();

        let error = ProjectedGraph::try_from_store_with_node_filter_and_layout(
            &store,
            catalog.rel_type_id("MENTIONS"),
            |_| true,
            ProjectionLayout::Outgoing,
            ProjectionMemoryBudget::new(
                NonZeroUsize::new(
                    projection_memory_estimate(ProjectionLayout::Outgoing, 2, 0).estimated_bytes,
                )
                .unwrap(),
            ),
        )
        .unwrap_err();

        assert_eq!(error.estimate.layout, ProjectionLayout::Outgoing);
        assert_eq!(error.estimate.node_count, 2);
        assert_eq!(error.estimate.relationship_count, 1);
        assert!(error.estimate.estimated_bytes > error.budget_bytes);
    }

    #[test]
    fn projection_memory_admission_stops_node_scan_at_the_budget_boundary() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for id in 0..100 {
            store
                .create_node(&mut catalog, "Memory", properties(&[("id", id)]))
                .unwrap();
        }
        let visited = std::cell::Cell::new(0usize);
        let error = ProjectedGraph::try_from_store_with_node_filter_and_layout(
            &store,
            None,
            |_| {
                visited.set(visited.get().saturating_add(1));
                true
            },
            ProjectionLayout::Outgoing,
            ProjectionMemoryBudget::new(NonZeroUsize::new(1).unwrap()),
        )
        .unwrap_err();

        assert_eq!(visited.get(), 1);
        assert_eq!(error.estimate.node_count, 1);
        assert_eq!(error.estimate.relationship_count, 0);
    }

    #[test]
    fn projects_relationships_into_csr() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, target, source, "RELATED", BTreeMap::new())
            .unwrap();

        let mentions = ProjectedGraph::from_store(&store, catalog.rel_type_id("MENTIONS"));

        assert_eq!(mentions.node_count(), 2);
        assert_eq!(mentions.edge_count(), 1);
        assert_eq!(
            mentions
                .outgoing_targets(source)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![target]
        );
        assert_eq!(
            mentions
                .incoming_sources(target)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![source]
        );
        assert!(mentions
            .incoming_sources(source)
            .unwrap()
            .collect::<Vec<_>>()
            .is_empty());
    }

    #[test]
    fn page_rank_orders_sink_higher_than_source() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let a = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let b = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();
        let c = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 3)]))
            .unwrap();
        store
            .create_relationship(&mut catalog, a, b, "MENTIONS", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, a, c, "MENTIONS", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, b, c, "MENTIONS", BTreeMap::new())
            .unwrap();

        let graph = ProjectedGraph::from_store(&store, catalog.rel_type_id("MENTIONS"));
        let scores = graph.page_rank(PageRankOptions::default());
        let score_for = |node: NodeId| {
            scores
                .iter()
                .find(|score| score.node == node)
                .map(|score| score.score)
                .unwrap()
        };

        assert!(score_for(c) > score_for(b));
        assert!(score_for(b) > score_for(a));
    }

    #[test]
    fn louvain_groups_disconnected_pairs_deterministically() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let a = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let b = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();
        let c = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 3)]))
            .unwrap();
        let d = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 4)]))
            .unwrap();
        store
            .create_relationship(&mut catalog, a, b, "MENTIONS", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, c, d, "MENTIONS", BTreeMap::new())
            .unwrap();

        let graph = ProjectedGraph::from_store(&store, catalog.rel_type_id("MENTIONS"));
        let communities = graph
            .louvain_communities(LouvainOptions::default())
            .into_iter()
            .map(|assignment| (assignment.node, assignment.community))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(communities[&a], a);
        assert_eq!(communities[&b], a);
        assert_eq!(communities[&c], c);
        assert_eq!(communities[&d], c);
        assert_ne!(communities[&a], communities[&c]);
    }

    #[test]
    fn louvain_keeps_edgeless_nodes_in_singleton_communities() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let a = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let b = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();

        let graph = ProjectedGraph::from_store_without_edges(&store);
        let communities = graph.louvain_communities(LouvainOptions::default());

        assert_eq!(
            communities,
            vec![
                super::CommunityAssignment {
                    node: a,
                    community: a,
                },
                super::CommunityAssignment {
                    node: b,
                    community: b,
                },
            ]
        );
    }

    #[test]
    fn hierarchical_louvain_emits_stable_levels() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let a = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 1)]))
            .unwrap();
        let b = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 2)]))
            .unwrap();
        let c = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 3)]))
            .unwrap();
        let d = store
            .create_node(&mut catalog, "Memory", properties(&[("id", 4)]))
            .unwrap();
        store
            .create_relationship(&mut catalog, a, b, "MENTIONS", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, c, d, "MENTIONS", BTreeMap::new())
            .unwrap();

        let graph = ProjectedGraph::from_store(&store, catalog.rel_type_id("MENTIONS"));
        let assignments = graph.hierarchical_louvain_communities(LouvainOptions {
            max_iterations: 20,
            max_levels: 2,
        });

        assert_eq!(assignments.len(), 8);
        assert!(assignments.iter().any(|assignment| assignment.level == 0
            && assignment.node == b
            && assignment.community == a));
        assert!(assignments.iter().any(|assignment| assignment.level == 1
            && assignment.node == d
            && assignment.community == c));
    }

    #[test]
    fn graph_algorithm_memory_estimates_include_projection_scratch_and_results() {
        let graph = ProjectedGraph::from_parts(
            vec![NodeId(1), NodeId(2)],
            vec![0, 1, 1],
            vec![1],
            vec![0, 0, 1],
            vec![0],
        )
        .unwrap();

        let page_rank = graph.page_rank_memory_estimate();
        let louvain_one_level = graph.louvain_memory_estimate(LouvainOptions {
            max_iterations: 2,
            max_levels: 1,
        });
        let louvain_two_levels = graph.louvain_memory_estimate(LouvainOptions {
            max_iterations: 2,
            max_levels: 2,
        });

        assert_eq!(
            page_rank.projection_bytes,
            graph.memory_estimate().estimated_bytes
        );
        assert!(page_rank.result_bytes > 0);
        assert_eq!(
            page_rank.total_peak_bytes,
            page_rank
                .projection_bytes
                .saturating_add(page_rank.algorithm_peak_bytes)
        );
        assert!(louvain_one_level.algorithm_peak_bytes > page_rank.algorithm_peak_bytes);
        assert!(louvain_two_levels.result_bytes > louvain_one_level.result_bytes);
        assert!(louvain_two_levels.total_peak_bytes > louvain_one_level.total_peak_bytes);
    }

    #[test]
    fn graph_algorithms_observe_runtime_cancellation() {
        let graph = ProjectedGraph::from_parts(
            vec![NodeId(1)],
            vec![0, 0],
            Vec::new(),
            vec![0, 0],
            Vec::new(),
        )
        .unwrap();
        let cancellation = hawdb_core::RuntimeCancellationToken::new();
        cancellation.cancel();
        let context = hawdb_core::RuntimeTaskContext::without_deadline(cancellation);

        let page_rank_error = graph
            .page_rank_with_context(PageRankOptions::default(), Some(&context))
            .unwrap_err();
        let louvain_error = graph
            .hierarchical_louvain_communities_with_context(
                LouvainOptions::default(),
                Some(&context),
            )
            .unwrap_err();

        assert!(page_rank_error
            .to_string()
            .contains("runtime task stopped: cancelled"));
        assert!(louvain_error
            .to_string()
            .contains("runtime task stopped: cancelled"));
    }

    fn properties(values: &[(&str, i64)]) -> BTreeMap<String, Value> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), Value::Int(*value)))
            .collect()
    }
}
