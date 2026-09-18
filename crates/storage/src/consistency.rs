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

//! Graph statistics consistency and adjacency consolidation contracts.

use crate::{
    AdjacencyDirection, AdjacencyGroupConsistencyMismatch, AdjacencyGroupKey, AdjacencyLayout,
    AdjacencyPostingList, CowSegment, CowSegmentedMap, NodeId, NodeRecord, RelId, RelRecord,
};
use hawdb_core::{BasicGraphStatistics, LabelId, RelTypeId, Value};
use std::collections::{BTreeMap, BTreeSet};

pub const DENSE_ADJACENCY_DEGREE_THRESHOLD: usize = 64;
const MAX_ADJACENCY_CONSISTENCY_SAMPLES: usize = 32;

type AdjacencyGroups = BTreeMap<AdjacencyGroupKey, BTreeSet<RelId>>;
type NodePropertyIndex = CowSegmentedMap<(LabelId, String, Value), CowSegment<BTreeSet<NodeId>>>;
type RelationshipPropertyIndex =
    CowSegmentedMap<(RelTypeId, String, Value), CowSegment<BTreeSet<RelId>>>;

pub fn adjacency_layout_for_degree(degree: usize) -> AdjacencyLayout {
    if degree >= DENSE_ADJACENCY_DEGREE_THRESHOLD {
        AdjacencyLayout::Dense
    } else {
        AdjacencyLayout::Sparse
    }
}

pub fn adjacency_consolidation_plan(
    candidates: &[AdjacencyConsolidationCandidate],
) -> AdjacencyConsolidationPlan {
    candidates.iter().fold(
        AdjacencyConsolidationPlan::default(),
        |mut plan, candidate| {
            plan.group_count = plan.group_count.saturating_add(1);
            plan.delta_entry_count = plan
                .delta_entry_count
                .saturating_add(candidate.delta_entry_count);
            plan.estimated_entries = plan
                .estimated_entries
                .saturating_add(candidate.estimated_entries);
            plan
        },
    )
}

pub fn maintained_adjacency_groups(
    outgoing: &CowSegmentedMap<(NodeId, RelTypeId), AdjacencyPostingList>,
    incoming: &CowSegmentedMap<(NodeId, RelTypeId), AdjacencyPostingList>,
) -> AdjacencyGroups {
    let mut groups = AdjacencyGroups::new();
    for ((node_id, rel_type), rel_ids) in outgoing.iter() {
        groups.insert(
            AdjacencyGroupKey {
                node_id: *node_id,
                rel_type: *rel_type,
                direction: AdjacencyDirection::Outgoing,
            },
            rel_ids
                .iter_copied()
                .map(|entry| entry.relationship_id)
                .collect(),
        );
    }
    for ((node_id, rel_type), rel_ids) in incoming.iter() {
        groups.insert(
            AdjacencyGroupKey {
                node_id: *node_id,
                rel_type: *rel_type,
                direction: AdjacencyDirection::Incoming,
            },
            rel_ids
                .iter_copied()
                .map(|entry| entry.relationship_id)
                .collect(),
        );
    }
    groups
}

pub fn recompute_adjacency_groups(
    relationships: &CowSegmentedMap<RelId, RelRecord>,
) -> AdjacencyGroups {
    let mut groups = AdjacencyGroups::new();
    for relationship in relationships.values() {
        groups
            .entry(AdjacencyGroupKey {
                node_id: relationship.source,
                rel_type: relationship.rel_type,
                direction: AdjacencyDirection::Outgoing,
            })
            .or_default()
            .insert(relationship.id);
        groups
            .entry(AdjacencyGroupKey {
                node_id: relationship.target,
                rel_type: relationship.rel_type,
                direction: AdjacencyDirection::Incoming,
            })
            .or_default()
            .insert(relationship.id);
    }
    groups
}

pub fn compute_degree_statistics_from_adjacency(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    outgoing: &CowSegmentedMap<(NodeId, RelTypeId), AdjacencyPostingList>,
    incoming: &CowSegmentedMap<(NodeId, RelTypeId), AdjacencyPostingList>,
) -> BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry> {
    compute_degree_statistics_from_groups(nodes, maintained_adjacency_groups(outgoing, incoming))
}

pub fn compute_degree_statistics_from_relationships(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
) -> BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry> {
    compute_degree_statistics_from_groups(nodes, recompute_adjacency_groups(relationships))
}

fn compute_degree_statistics_from_groups(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    groups: AdjacencyGroups,
) -> BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry> {
    let rel_types = groups
        .keys()
        .map(|key| key.rel_type)
        .collect::<BTreeSet<_>>();
    let label_counts = label_counts_for_degree_statistics(nodes);
    let mut statistics = BTreeMap::<DegreeStatisticsKey, DegreeStatisticsEntry>::new();
    for (label_id, node_count) in &label_counts {
        for rel_type in &rel_types {
            for direction in [AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming] {
                statistics.insert(
                    DegreeStatisticsKey {
                        label_id: *label_id,
                        rel_type: *rel_type,
                        direction,
                    },
                    DegreeStatisticsEntry {
                        node_count: *node_count,
                        non_zero_node_count: 0,
                        relationship_count: 0,
                        max_degree: 0,
                        dense_node_count: 0,
                    },
                );
            }
        }
    }
    for (group, rel_ids) in groups {
        let Some(node) = nodes.get(&group.node_id) else {
            continue;
        };
        let degree = rel_ids.len() as u64;
        for label_id in &node.labels {
            let entry = statistics
                .entry(DegreeStatisticsKey {
                    label_id: *label_id,
                    rel_type: group.rel_type,
                    direction: group.direction,
                })
                .or_insert(DegreeStatisticsEntry {
                    node_count: label_counts.get(label_id).copied().unwrap_or_default(),
                    non_zero_node_count: 0,
                    relationship_count: 0,
                    max_degree: 0,
                    dense_node_count: 0,
                });
            entry.non_zero_node_count += 1;
            entry.relationship_count += degree;
            entry.max_degree = entry.max_degree.max(degree);
            if rel_ids.len() >= DENSE_ADJACENCY_DEGREE_THRESHOLD {
                entry.dense_node_count += 1;
            }
        }
    }
    statistics
}

fn label_counts_for_degree_statistics(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
) -> BTreeMap<LabelId, u64> {
    let mut label_counts = BTreeMap::new();
    for node in nodes.values() {
        for label_id in &node.labels {
            *label_counts.entry(*label_id).or_default() += 1;
        }
    }
    label_counts
}

pub fn adjacency_direction_sort_key(direction: AdjacencyDirection) -> u8 {
    match direction {
        AdjacencyDirection::Outgoing => 0,
        AdjacencyDirection::Incoming => 1,
    }
}

fn sample_relationship_ids(rel_ids: &BTreeSet<RelId>) -> Vec<RelId> {
    rel_ids
        .iter()
        .copied()
        .take(MAX_ADJACENCY_CONSISTENCY_SAMPLES)
        .collect()
}

fn node_property_index_reference_count(index: &NodePropertyIndex) -> usize {
    index.values().map(|node_ids| node_ids.len()).sum()
}

fn relationship_property_index_reference_count(index: &RelationshipPropertyIndex) -> usize {
    index.values().map(|rel_ids| rel_ids.len()).sum()
}

fn property_index_mismatch_summary(
    maintained: &NodePropertyIndex,
    recomputed: &NodePropertyIndex,
) -> (usize, usize, usize, Vec<(LabelId, String, Value)>) {
    let mut missing_key_count = 0usize;
    let mut extra_key_count = 0usize;
    let mut mismatched_key_count = 0usize;
    let mut mismatched_keys = Vec::new();
    for key in maintained
        .keys()
        .chain(recomputed.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
    {
        match (maintained.get(&key), recomputed.get(&key)) {
            (Some(left), Some(right)) if left == right => {}
            (Some(_), Some(_)) => mismatched_key_count += 1,
            (Some(_), None) => extra_key_count += 1,
            (None, Some(_)) => missing_key_count += 1,
            (None, None) => {}
        }
        if maintained.get(&key) != recomputed.get(&key)
            && mismatched_keys.len() < MAX_ADJACENCY_CONSISTENCY_SAMPLES
        {
            mismatched_keys.push(key);
        }
    }
    (
        missing_key_count,
        extra_key_count,
        mismatched_key_count,
        mismatched_keys,
    )
}

fn relationship_property_index_mismatch_summary(
    maintained: &RelationshipPropertyIndex,
    recomputed: &RelationshipPropertyIndex,
) -> (usize, usize, usize, Vec<(RelTypeId, String, Value)>) {
    let mut missing_key_count = 0usize;
    let mut extra_key_count = 0usize;
    let mut mismatched_key_count = 0usize;
    let mut mismatched_keys = Vec::new();
    for key in maintained
        .keys()
        .chain(recomputed.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
    {
        match (maintained.get(&key), recomputed.get(&key)) {
            (Some(left), Some(right)) if left == right => {}
            (Some(_), Some(_)) => mismatched_key_count += 1,
            (Some(_), None) => extra_key_count += 1,
            (None, Some(_)) => missing_key_count += 1,
            (None, None) => {}
        }
        if maintained.get(&key) != recomputed.get(&key)
            && mismatched_keys.len() < MAX_ADJACENCY_CONSISTENCY_SAMPLES
        {
            mismatched_keys.push(key);
        }
    }
    (
        missing_key_count,
        extra_key_count,
        mismatched_key_count,
        mismatched_keys,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjacencyConsistencyReport {
    pub ready: bool,
    pub computed_at_commit_epoch: u64,
    pub relationship_count: usize,
    pub maintained_group_count: usize,
    pub recomputed_group_count: usize,
    pub dense_group_count: usize,
    pub missing_group_count: usize,
    pub extra_group_count: usize,
    pub mismatched_group_count: usize,
    pub dangling_relationship_count: usize,
    pub mismatches: Vec<AdjacencyGroupConsistencyMismatch>,
    pub dangling_relationship_ids: Vec<RelId>,
}

impl AdjacencyConsistencyReport {
    pub fn new(
        computed_at_commit_epoch: u64,
        relationship_count: usize,
        maintained: AdjacencyGroups,
        recomputed: AdjacencyGroups,
        relationships: &CowSegmentedMap<RelId, RelRecord>,
    ) -> Self {
        let mut missing_group_count = 0;
        let mut extra_group_count = 0;
        let mut mismatched_group_count = 0;
        let mut mismatches = Vec::new();
        let keys = maintained
            .keys()
            .chain(recomputed.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        for key in keys {
            let maintained_ids = maintained.get(&key);
            let recomputed_ids = recomputed.get(&key);
            if maintained_ids == recomputed_ids {
                continue;
            }
            match (maintained_ids, recomputed_ids) {
                (None, Some(_)) => missing_group_count += 1,
                (Some(_), None) => extra_group_count += 1,
                (Some(_), Some(_)) => mismatched_group_count += 1,
                (None, None) => {}
            }
            if mismatches.len() < MAX_ADJACENCY_CONSISTENCY_SAMPLES {
                mismatches.push(AdjacencyGroupConsistencyMismatch {
                    key,
                    maintained_relationship_ids: maintained_ids
                        .map(sample_relationship_ids)
                        .unwrap_or_default(),
                    recomputed_relationship_ids: recomputed_ids
                        .map(sample_relationship_ids)
                        .unwrap_or_default(),
                });
            }
        }
        let dangling_relationships = maintained
            .values()
            .flat_map(|rel_ids| rel_ids.iter().copied())
            .filter(|rel_id| !relationships.contains_key(rel_id))
            .collect::<BTreeSet<_>>();
        let dangling_relationship_ids = dangling_relationships
            .iter()
            .copied()
            .take(MAX_ADJACENCY_CONSISTENCY_SAMPLES)
            .collect::<Vec<_>>();
        let dangling_relationship_count = dangling_relationships.len();
        let ready = missing_group_count == 0
            && extra_group_count == 0
            && mismatched_group_count == 0
            && dangling_relationship_count == 0;
        Self {
            ready,
            computed_at_commit_epoch,
            relationship_count,
            maintained_group_count: maintained.len(),
            recomputed_group_count: recomputed.len(),
            dense_group_count: maintained
                .values()
                .filter(|rel_ids| rel_ids.len() >= DENSE_ADJACENCY_DEGREE_THRESHOLD)
                .count(),
            missing_group_count,
            extra_group_count,
            mismatched_group_count,
            dangling_relationship_count,
            mismatches,
            dangling_relationship_ids,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DegreeStatisticsKey {
    pub label_id: LabelId,
    pub rel_type: RelTypeId,
    pub direction: AdjacencyDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DegreeStatisticsEntry {
    pub node_count: u64,
    pub non_zero_node_count: u64,
    pub relationship_count: u64,
    pub max_degree: u64,
    pub dense_node_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DegreeStatisticsConsistencyReport {
    pub ready: bool,
    pub computed_at_commit_epoch: u64,
    pub maintained: BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry>,
    pub recomputed: BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry>,
    pub mismatched_keys: Vec<DegreeStatisticsKey>,
}

impl DegreeStatisticsConsistencyReport {
    pub fn new(
        computed_at_commit_epoch: u64,
        maintained: BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry>,
        recomputed: BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry>,
    ) -> Self {
        let mismatched_keys = maintained
            .keys()
            .chain(recomputed.keys())
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|key| maintained.get(key) != recomputed.get(key))
            .take(MAX_ADJACENCY_CONSISTENCY_SAMPLES)
            .collect::<Vec<_>>();
        Self {
            ready: mismatched_keys.is_empty(),
            computed_at_commit_epoch,
            maintained,
            recomputed,
            mismatched_keys,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistinctValueStatisticsConsistencyReport {
    pub ready: bool,
    pub computed_at_commit_epoch: u64,
    pub maintained_property_distinct_counts: BTreeMap<(LabelId, String), u64>,
    pub recomputed_property_distinct_counts: BTreeMap<(LabelId, String), u64>,
    pub maintained_rel_property_distinct_counts: BTreeMap<(RelTypeId, String), u64>,
    pub recomputed_rel_property_distinct_counts: BTreeMap<(RelTypeId, String), u64>,
    pub mismatched_property_keys: Vec<(LabelId, String)>,
    pub mismatched_rel_property_keys: Vec<(RelTypeId, String)>,
}

impl DistinctValueStatisticsConsistencyReport {
    pub fn new(
        computed_at_commit_epoch: u64,
        maintained_property_distinct_counts: BTreeMap<(LabelId, String), u64>,
        recomputed_property_distinct_counts: BTreeMap<(LabelId, String), u64>,
        maintained_rel_property_distinct_counts: BTreeMap<(RelTypeId, String), u64>,
        recomputed_rel_property_distinct_counts: BTreeMap<(RelTypeId, String), u64>,
    ) -> Self {
        let mismatched_property_keys = maintained_property_distinct_counts
            .keys()
            .chain(recomputed_property_distinct_counts.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|key| {
                maintained_property_distinct_counts.get(key)
                    != recomputed_property_distinct_counts.get(key)
            })
            .take(MAX_ADJACENCY_CONSISTENCY_SAMPLES)
            .collect::<Vec<_>>();
        let mismatched_rel_property_keys = maintained_rel_property_distinct_counts
            .keys()
            .chain(recomputed_rel_property_distinct_counts.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|key| {
                maintained_rel_property_distinct_counts.get(key)
                    != recomputed_rel_property_distinct_counts.get(key)
            })
            .take(MAX_ADJACENCY_CONSISTENCY_SAMPLES)
            .collect::<Vec<_>>();
        Self {
            ready: mismatched_property_keys.is_empty() && mismatched_rel_property_keys.is_empty(),
            computed_at_commit_epoch,
            maintained_property_distinct_counts,
            recomputed_property_distinct_counts,
            maintained_rel_property_distinct_counts,
            recomputed_rel_property_distinct_counts,
            mismatched_property_keys,
            mismatched_rel_property_keys,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyIndexConsistencyReport {
    pub ready: bool,
    pub computed_at_commit_epoch: u64,
    pub node_index_entry_count: usize,
    pub recomputed_node_index_entry_count: usize,
    pub node_index_reference_count: usize,
    pub recomputed_node_index_reference_count: usize,
    pub missing_node_key_count: usize,
    pub extra_node_key_count: usize,
    pub mismatched_node_key_count: usize,
    pub relationship_index_entry_count: usize,
    pub recomputed_relationship_index_entry_count: usize,
    pub relationship_index_reference_count: usize,
    pub recomputed_relationship_index_reference_count: usize,
    pub missing_relationship_key_count: usize,
    pub extra_relationship_key_count: usize,
    pub mismatched_relationship_key_count: usize,
    pub mismatched_node_keys: Vec<(LabelId, String, Value)>,
    pub mismatched_relationship_keys: Vec<(RelTypeId, String, Value)>,
}

impl PropertyIndexConsistencyReport {
    pub fn new(
        computed_at_commit_epoch: u64,
        maintained_node_index: &NodePropertyIndex,
        recomputed_node_index: &NodePropertyIndex,
        maintained_relationship_index: &RelationshipPropertyIndex,
        recomputed_relationship_index: &RelationshipPropertyIndex,
    ) -> Self {
        let (
            missing_node_key_count,
            extra_node_key_count,
            mismatched_node_key_count,
            mismatched_node_keys,
        ) = property_index_mismatch_summary(maintained_node_index, recomputed_node_index);
        let (
            missing_relationship_key_count,
            extra_relationship_key_count,
            mismatched_relationship_key_count,
            mismatched_relationship_keys,
        ) = relationship_property_index_mismatch_summary(
            maintained_relationship_index,
            recomputed_relationship_index,
        );
        Self {
            ready: missing_node_key_count == 0
                && extra_node_key_count == 0
                && mismatched_node_key_count == 0
                && missing_relationship_key_count == 0
                && extra_relationship_key_count == 0
                && mismatched_relationship_key_count == 0,
            computed_at_commit_epoch,
            node_index_entry_count: maintained_node_index.len(),
            recomputed_node_index_entry_count: recomputed_node_index.len(),
            node_index_reference_count: node_property_index_reference_count(maintained_node_index),
            recomputed_node_index_reference_count: node_property_index_reference_count(
                recomputed_node_index,
            ),
            missing_node_key_count,
            extra_node_key_count,
            mismatched_node_key_count,
            relationship_index_entry_count: maintained_relationship_index.len(),
            recomputed_relationship_index_entry_count: recomputed_relationship_index.len(),
            relationship_index_reference_count: relationship_property_index_reference_count(
                maintained_relationship_index,
            ),
            recomputed_relationship_index_reference_count:
                relationship_property_index_reference_count(recomputed_relationship_index),
            missing_relationship_key_count,
            extra_relationship_key_count,
            mismatched_relationship_key_count,
            mismatched_node_keys,
            mismatched_relationship_keys,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicStatisticsConsistencyReport {
    pub ready: bool,
    pub computed_at_commit_epoch: u64,
    pub incremental: BasicGraphStatistics,
    pub recomputed: BasicGraphStatistics,
    pub mismatched_fields: Vec<String>,
}

impl BasicStatisticsConsistencyReport {
    pub fn new(incremental: BasicGraphStatistics, recomputed: BasicGraphStatistics) -> Self {
        let mut mismatched_fields = Vec::new();
        if incremental.computed_at_commit_epoch != recomputed.computed_at_commit_epoch {
            mismatched_fields.push("computed_at_commit_epoch".to_string());
        }
        if incremental.node_count != recomputed.node_count {
            mismatched_fields.push("node_count".to_string());
        }
        if incremental.relationship_count != recomputed.relationship_count {
            mismatched_fields.push("relationship_count".to_string());
        }
        if incremental.label_counts != recomputed.label_counts {
            mismatched_fields.push("label_counts".to_string());
        }
        if incremental.rel_type_counts != recomputed.rel_type_counts {
            mismatched_fields.push("rel_type_counts".to_string());
        }
        Self {
            ready: mismatched_fields.is_empty(),
            computed_at_commit_epoch: incremental.computed_at_commit_epoch,
            incremental,
            recomputed,
            mismatched_fields,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdjacencyConsolidationPlan {
    pub group_count: usize,
    pub delta_entry_count: usize,
    pub estimated_entries: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdjacencyConsolidationReport {
    pub planned: AdjacencyConsolidationPlan,
    pub consolidated_group_count: usize,
    pub consolidated_delta_entry_count: usize,
    pub consolidated_estimated_entries: usize,
    pub remaining: AdjacencyConsolidationPlan,
}

#[derive(Debug, Clone, Copy)]
pub struct AdjacencyConsolidationCandidate {
    pub direction: AdjacencyDirection,
    pub key: (NodeId, RelTypeId),
    pub delta_entry_count: usize,
    pub estimated_entries: usize,
}
