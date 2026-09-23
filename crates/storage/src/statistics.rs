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

//! Graph statistics sampling, computation, and full-text tokenization.

#[doc(hidden)]
pub mod node_index_updates;

use crate::graph_index::{CompositePropertyIndex, NodePropertyIndex, RelationshipPropertyIndex};
use crate::statistics_refresh::{
    adaptive_histogram_sample_limit, node_property_supports_optimizer_statistics,
    relationship_property_supports_optimizer_statistics, sample_histogram_values,
    MAX_BOUNDED_PATH_STAT_HOPS, MAX_BOUNDED_PATH_STAT_VISITS, MAX_PROPERTY_HISTOGRAM_VALUES,
};
use crate::{CowSegmentedMap, NodeId, NodeRecord, RelId, RelRecord};
use hawdb_core::{
    BasicGraphStatistics, Catalog, GraphStatistics, IndexId, IndexKind, IndexStatisticsSample,
    LabelId, RelTypeId, Value,
};
use std::collections::{BTreeMap, BTreeSet};

pub fn composite_property_index_key(
    node: &NodeRecord,
    properties: &[String],
) -> Option<Vec<(String, Value)>> {
    properties
        .iter()
        .map(|property| {
            node.properties
                .get(property)
                .cloned()
                .map(|value| (property.clone(), value))
        })
        .collect()
}

pub fn scalar_property_index_cardinality(
    index: &NodePropertyIndex,
    label_id: LabelId,
    property: &str,
) -> (u64, u64) {
    index
        .iter()
        .filter(|((candidate_label, candidate_property, _), _)| {
            *candidate_label == label_id && candidate_property == property
        })
        .fold((0_u64, 0_u64), |(size, unique), (_, node_ids)| {
            (
                size.saturating_add(node_ids.len() as u64),
                unique.saturating_add(1),
            )
        })
}

pub fn composite_property_index_unique_values(
    index: &CompositePropertyIndex,
    label_id: LabelId,
    properties: &[String],
) -> u64 {
    index
        .keys()
        .filter(|(candidate_label, key)| {
            *candidate_label == label_id
                && key
                    .iter()
                    .map(|(property, _)| property)
                    .eq(properties.iter())
        })
        .count() as u64
}

pub fn compute_index_statistics_samples(
    catalog: &Catalog,
    property_index: &NodePropertyIndex,
    composite_property_index: &CompositePropertyIndex,
) -> BTreeMap<IndexId, IndexStatisticsSample> {
    let mut samples = BTreeMap::new();
    for index in catalog
        .property_indexes()
        .filter(|index| index.kind != IndexKind::FullText)
    {
        let (index_size, unique_values) =
            scalar_property_index_cardinality(property_index, index.label_id, &index.property);
        samples.insert(
            index.id,
            IndexStatisticsSample::exact(index_size, unique_values),
        );
    }
    for index in catalog.composite_property_indexes() {
        let index_size = composite_property_index
            .iter()
            .filter(|((candidate_label, key), _)| {
                *candidate_label == index.label_id
                    && key
                        .iter()
                        .map(|(property, _)| property)
                        .eq(index.properties.iter())
            })
            .fold(0_u64, |size, (_, node_ids)| {
                size.saturating_add(node_ids.len() as u64)
            });
        let unique_values = composite_property_index_unique_values(
            composite_property_index,
            index.label_id,
            &index.properties,
        );
        samples.insert(
            index.id,
            IndexStatisticsSample::exact(index_size, unique_values),
        );
    }
    samples
}

pub fn full_text_index_tokens(value: &str) -> BTreeSet<String> {
    let normalized = value.to_lowercase();
    let chars = normalized.chars().collect::<Vec<_>>();
    let mut tokens = BTreeSet::new();
    for start in 0..chars.len() {
        for width in 1..=3 {
            let end = start + width;
            if end > chars.len() {
                break;
            }
            let token = chars[start..end].iter().collect::<String>();
            if !token.chars().all(char::is_whitespace) {
                tokens.insert(token);
            }
        }
    }
    tokens
}

pub fn full_text_query_tokens(query: &str) -> Vec<String> {
    full_text_index_tokens(query).into_iter().collect()
}

pub fn compute_statistics(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    computed_at_commit_epoch: u64,
) -> GraphStatistics {
    compute_statistics_with_basic(
        nodes,
        relationships,
        None,
        compute_basic_statistics(nodes, relationships, computed_at_commit_epoch),
    )
}

pub fn compute_statistics_for_catalog(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    catalog: &Catalog,
    basic_statistics: BasicGraphStatistics,
) -> GraphStatistics {
    compute_statistics_with_basic(nodes, relationships, Some(catalog), basic_statistics)
}

pub fn graph_statistics_from_basic(
    basic_statistics: BasicGraphStatistics,
    advanced_statistics_complete: bool,
) -> GraphStatistics {
    GraphStatistics {
        computed_at_commit_epoch: basic_statistics.computed_at_commit_epoch,
        advanced_statistics_complete,
        histogram_sample_limit: MAX_PROPERTY_HISTOGRAM_VALUES,
        node_count: basic_statistics.node_count,
        relationship_count: basic_statistics.relationship_count,
        label_counts: basic_statistics.label_counts,
        rel_type_counts: basic_statistics.rel_type_counts,
        ..GraphStatistics::default()
    }
}

pub fn compute_statistics_with_basic(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    catalog: Option<&Catalog>,
    basic_statistics: BasicGraphStatistics,
) -> GraphStatistics {
    let mut statistics = graph_statistics_from_basic(basic_statistics, true);
    let mut property_values = BTreeMap::<(LabelId, String), BTreeSet<Value>>::new();
    let mut rel_property_values = BTreeMap::<(RelTypeId, String), BTreeSet<Value>>::new();
    let mut excluded_property_groups = BTreeSet::<(LabelId, String)>::new();
    let mut excluded_rel_property_groups = BTreeSet::<(RelTypeId, String)>::new();
    let mut rel_type_sources = BTreeMap::<RelTypeId, BTreeSet<NodeId>>::new();
    let mut rel_type_targets = BTreeMap::<RelTypeId, BTreeSet<NodeId>>::new();
    let mut path_sources = BTreeMap::<(LabelId, RelTypeId, LabelId), BTreeSet<NodeId>>::new();
    let mut path_targets = BTreeMap::<(LabelId, RelTypeId, LabelId), BTreeSet<NodeId>>::new();
    let mut outgoing_by_source_type = BTreeMap::<(NodeId, RelTypeId), Vec<NodeId>>::new();

    for node in nodes.values() {
        for label_id in &node.labels {
            for (property, value) in &node.properties {
                let key = (*label_id, property.clone());
                collect_property_statistic_value(
                    &mut property_values,
                    &mut excluded_property_groups,
                    key,
                    value,
                    node_property_supports_optimizer_statistics(
                        catalog, *label_id, property, value,
                    ),
                );
            }
        }
    }
    for relationship in relationships.values() {
        rel_type_sources
            .entry(relationship.rel_type)
            .or_default()
            .insert(relationship.source);
        rel_type_targets
            .entry(relationship.rel_type)
            .or_default()
            .insert(relationship.target);
        outgoing_by_source_type
            .entry((relationship.source, relationship.rel_type))
            .or_default()
            .push(relationship.target);
        for (property, value) in &relationship.properties {
            let key = (relationship.rel_type, property.clone());
            collect_property_statistic_value(
                &mut rel_property_values,
                &mut excluded_rel_property_groups,
                key,
                value,
                relationship_property_supports_optimizer_statistics(
                    catalog,
                    relationship.rel_type,
                    property,
                    value,
                ),
            );
        }
        if let (Some(source), Some(target)) = (
            nodes.get(&relationship.source),
            nodes.get(&relationship.target),
        ) {
            for source_label in &source.labels {
                for target_label in &target.labels {
                    let path_key = (*source_label, relationship.rel_type, *target_label);
                    *statistics.path_counts.entry(path_key).or_default() += 1;
                    path_sources
                        .entry(path_key)
                        .or_default()
                        .insert(relationship.source);
                    path_targets
                        .entry(path_key)
                        .or_default()
                        .insert(relationship.target);
                }
            }
        }
    }
    statistics.rel_type_source_counts = rel_type_sources
        .into_iter()
        .map(|(rel_type, sources)| (rel_type, sources.len() as u64))
        .collect();
    statistics.rel_type_target_counts = rel_type_targets
        .into_iter()
        .map(|(rel_type, targets)| (rel_type, targets.len() as u64))
        .collect();
    statistics.path_source_distinct_counts = path_sources
        .into_iter()
        .map(|(path, sources)| (path, sources.len() as u64))
        .collect();
    statistics.path_target_distinct_counts = path_targets
        .into_iter()
        .map(|(path, targets)| (path, targets.len() as u64))
        .collect();
    for (key, values) in property_values {
        let histogram_sample_limit = adaptive_histogram_sample_limit(values.len());
        let is_sampled = values.len() > histogram_sample_limit;
        statistics
            .property_distinct_counts
            .insert(key.clone(), values.len() as u64);
        statistics
            .property_histograms
            .insert(key.clone(), sample_histogram_values(values));
        statistics
            .sampled_property_histograms
            .insert(key, is_sampled);
    }
    for (key, values) in rel_property_values {
        let histogram_sample_limit = adaptive_histogram_sample_limit(values.len());
        let is_sampled = values.len() > histogram_sample_limit;
        statistics
            .rel_property_distinct_counts
            .insert(key.clone(), values.len() as u64);
        statistics
            .rel_property_histograms
            .insert(key.clone(), sample_histogram_values(values));
        statistics
            .sampled_rel_property_histograms
            .insert(key, is_sampled);
    }
    let bounded_path_statistics = compute_bounded_path_statistics(
        nodes,
        &outgoing_by_source_type,
        MAX_BOUNDED_PATH_STAT_HOPS,
    );
    statistics.bounded_path_counts = bounded_path_statistics.counts;
    statistics.bounded_path_source_distinct_counts = bounded_path_statistics.source_distinct_counts;
    statistics.bounded_path_target_distinct_counts = bounded_path_statistics.target_distinct_counts;
    statistics
}

pub fn collect_property_statistic_value<K: Ord>(
    values: &mut BTreeMap<K, BTreeSet<Value>>,
    excluded: &mut BTreeSet<K>,
    key: K,
    value: &Value,
    eligible: bool,
) {
    if !eligible {
        values.remove(&key);
        excluded.insert(key);
    } else if !excluded.contains(&key) {
        values.entry(key).or_default().insert(value.clone());
    }
}

pub fn compute_node_property_distinct_counts_from_index(
    property_index: &NodePropertyIndex,
    catalog: &Catalog,
) -> BTreeMap<(LabelId, String), u64> {
    compute_supported_property_distinct_counts(
        property_index
            .keys()
            .map(|(label, property, value)| ((*label, property.clone()), value)),
        |(label, property), value| {
            node_property_supports_optimizer_statistics(Some(catalog), *label, property, value)
        },
    )
}

pub fn compute_relationship_property_distinct_counts_from_index(
    relationship_property_index: &RelationshipPropertyIndex,
    catalog: &Catalog,
) -> BTreeMap<(RelTypeId, String), u64> {
    compute_supported_property_distinct_counts(
        relationship_property_index
            .keys()
            .map(|(rel_type, property, value)| ((*rel_type, property.clone()), value)),
        |(rel_type, property), value| {
            relationship_property_supports_optimizer_statistics(
                Some(catalog),
                *rel_type,
                property,
                value,
            )
        },
    )
}

pub fn compute_supported_property_distinct_counts<'a, K: Ord>(
    entries: impl Iterator<Item = (K, &'a Value)>,
    mut supports: impl FnMut(&K, &Value) -> bool,
) -> BTreeMap<K, u64> {
    let mut counts = BTreeMap::<K, Option<u64>>::new();
    for (key, value) in entries {
        let eligible = supports(&key, value);
        let count = counts.entry(key).or_insert(Some(0));
        if eligible {
            if let Some(count) = count {
                *count = count.saturating_add(1);
            }
        } else {
            *count = None;
        }
    }
    counts
        .into_iter()
        .filter_map(|(key, count)| count.map(|count| (key, count)))
        .collect()
}

/// Recomputes the node property index the way the write path maintains it:
/// declared properties only. Recomputing every property would report the
/// undeclared ones as permanently missing, which is the design, not a defect.
pub fn recompute_node_property_index(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    catalog: &Catalog,
) -> NodePropertyIndex {
    let mut index = NodePropertyIndex::default();
    for node in nodes.values() {
        for label_id in &node.labels {
            for (property, value) in &node.properties {
                if !catalog.has_scalar_property_index(*label_id, property) {
                    continue;
                }
                index
                    .entry_or_default((*label_id, property.clone(), value.clone()))
                    .insert(node.id);
            }
        }
    }
    index
}

pub fn recompute_relationship_property_index(
    relationships: &CowSegmentedMap<RelId, RelRecord>,
) -> RelationshipPropertyIndex {
    let mut index = RelationshipPropertyIndex::default();
    for relationship in relationships.values() {
        for (property, value) in &relationship.properties {
            index
                .entry_or_default((relationship.rel_type, property.clone(), value.clone()))
                .insert(relationship.id);
        }
    }
    index
}

pub fn compute_basic_statistics(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    computed_at_commit_epoch: u64,
) -> BasicGraphStatistics {
    let mut statistics = BasicGraphStatistics {
        computed_at_commit_epoch,
        node_count: nodes.len() as u64,
        relationship_count: relationships.len() as u64,
        ..BasicGraphStatistics::default()
    };
    for node in nodes.values() {
        for label_id in &node.labels {
            *statistics.label_counts.entry(*label_id).or_default() += 1;
        }
    }
    for relationship in relationships.values() {
        *statistics
            .rel_type_counts
            .entry(relationship.rel_type)
            .or_default() += 1;
    }
    statistics
}

pub fn decrement_counter<K>(counts: &mut BTreeMap<K, u64>, key: &K)
where
    K: Ord,
{
    let Some(count) = counts.get_mut(key) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        counts.remove(key);
    }
}

#[derive(Debug, Default)]
pub struct BoundedPathStatistics {
    pub counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
    pub source_distinct_counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
    pub target_distinct_counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
    /// True when the global visit budget stopped enumeration early. Truncated
    /// runs publish no path statistics: partial prefixes would be mistaken for
    /// exact counts by consumers, so the maps are left empty and callers fall
    /// back to heuristic estimates.
    pub truncated: bool,
}

pub fn compute_bounded_path_statistics(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    outgoing_by_source_type: &BTreeMap<(NodeId, RelTypeId), Vec<NodeId>>,
    max_hops: usize,
) -> BoundedPathStatistics {
    let mut accumulator = BoundedPathStatAccumulator::default();
    let context = BoundedPathStatContext {
        nodes,
        outgoing_by_source_type,
        max_hops,
    };
    let rel_types = outgoing_by_source_type
        .keys()
        .map(|(_, rel_type)| *rel_type)
        .collect::<BTreeSet<_>>();
    'sources: for source in nodes.values() {
        for source_label in &source.labels {
            for rel_type in &rel_types {
                if accumulator.exhausted {
                    break 'sources;
                }
                context.collect(
                    source.id,
                    source.id,
                    *source_label,
                    *rel_type,
                    1,
                    &mut accumulator,
                );
            }
        }
    }
    if accumulator.exhausted {
        return BoundedPathStatistics {
            truncated: true,
            ..BoundedPathStatistics::default()
        };
    }
    BoundedPathStatistics {
        counts: accumulator.counts,
        source_distinct_counts: accumulator
            .sources
            .into_iter()
            .map(|(path, sources)| (path, sources.len() as u64))
            .collect(),
        target_distinct_counts: accumulator
            .targets
            .into_iter()
            .map(|(path, targets)| (path, targets.len() as u64))
            .collect(),
        truncated: false,
    }
}

struct BoundedPathStatContext<'a> {
    nodes: &'a CowSegmentedMap<NodeId, NodeRecord>,
    outgoing_by_source_type: &'a BTreeMap<(NodeId, RelTypeId), Vec<NodeId>>,
    max_hops: usize,
}

#[derive(Debug, Default)]
struct BoundedPathStatAccumulator {
    counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
    sources: BTreeMap<(LabelId, RelTypeId, LabelId, usize), BTreeSet<NodeId>>,
    targets: BTreeMap<(LabelId, RelTypeId, LabelId, usize), BTreeSet<NodeId>>,
    visits: usize,
    /// Set once the global visit budget is spent; stops every remaining
    /// enumeration, including the outer source x label x rel_type loops.
    exhausted: bool,
}

impl BoundedPathStatContext<'_> {
    fn collect(
        &self,
        root_source: NodeId,
        current: NodeId,
        source_label: LabelId,
        rel_type: RelTypeId,
        hop: usize,
        accumulator: &mut BoundedPathStatAccumulator,
    ) {
        if hop > self.max_hops || accumulator.exhausted {
            return;
        }
        let Some(targets) = self.outgoing_by_source_type.get(&(current, rel_type)) else {
            return;
        };
        for target_id in targets {
            if accumulator.visits >= MAX_BOUNDED_PATH_STAT_VISITS {
                accumulator.exhausted = true;
                return;
            }
            accumulator.visits += 1;
            let Some(target) = self.nodes.get(target_id) else {
                continue;
            };
            for target_label in &target.labels {
                let path_key = (source_label, rel_type, *target_label, hop);
                *accumulator.counts.entry(path_key).or_default() += 1;
                accumulator
                    .sources
                    .entry(path_key)
                    .or_default()
                    .insert(root_source);
                accumulator
                    .targets
                    .entry(path_key)
                    .or_default()
                    .insert(*target_id);
            }
            self.collect(
                root_source,
                *target_id,
                source_label,
                rel_type,
                hop + 1,
                accumulator,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u64) -> NodeRecord {
        NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::from([LabelId(0)]),
            properties: BTreeMap::new(),
        }
    }

    /// A complete directed graph of 30 vertices needs 30+900+27_000 = 27,930
    /// visits per source — under the budget — but 30 * 27,930 = 837,900 across
    /// all sources. Only a *global* budget truncates this; a per-source cap
    /// would not. Truncated runs publish no partial path statistics.
    #[test]
    fn bounded_path_statistics_respect_global_visit_budget() {
        const SIZE: u64 = 30;
        let rel_type = RelTypeId(0);

        let mut nodes = CowSegmentedMap::default();
        let mut outgoing: BTreeMap<(NodeId, RelTypeId), Vec<NodeId>> = BTreeMap::new();
        let all: Vec<NodeId> = (0..SIZE).map(NodeId).collect();
        for id in 0..SIZE {
            nodes.insert(NodeId(id), node(id));
            outgoing.insert((NodeId(id), rel_type), all.clone());
        }

        let stats = compute_bounded_path_statistics(
            &nodes,
            &outgoing,
            MAX_BOUNDED_PATH_STAT_HOPS,
        );
        assert!(stats.truncated);
        assert!(stats.counts.is_empty());
        assert!(stats.source_distinct_counts.is_empty());
        assert!(stats.target_distinct_counts.is_empty());
    }

    /// Below the budget the pass must still publish exact counts and distinct
    /// source/target cardinalities. Chain A -> B -> C -> D gives per-hop
    /// counts 3, 2, 1 with three sources and three targets at hop 1.
    #[test]
    fn bounded_path_statistics_below_budget_stay_exact() {
        let rel_type = RelTypeId(0);
        let label = LabelId(0);

        let mut nodes = CowSegmentedMap::default();
        let mut outgoing: BTreeMap<(NodeId, RelTypeId), Vec<NodeId>> = BTreeMap::new();
        for id in 0..4u64 {
            nodes.insert(NodeId(id), node(id));
        }
        for (from, to) in [(0u64, 1u64), (1, 2), (2, 3)] {
            outgoing.insert((NodeId(from), rel_type), vec![NodeId(to)]);
        }

        let stats = compute_bounded_path_statistics(
            &nodes,
            &outgoing,
            MAX_BOUNDED_PATH_STAT_HOPS,
        );
        assert!(!stats.truncated);
        assert_eq!(stats.counts[&(label, rel_type, label, 1)], 3);
        assert_eq!(stats.counts[&(label, rel_type, label, 2)], 2);
        assert_eq!(stats.counts[&(label, rel_type, label, 3)], 1);
        assert_eq!(stats.source_distinct_counts[&(label, rel_type, label, 1)], 3);
        assert_eq!(stats.target_distinct_counts[&(label, rel_type, label, 1)], 3);
        assert_eq!(stats.source_distinct_counts[&(label, rel_type, label, 3)], 1);
        assert_eq!(stats.target_distinct_counts[&(label, rel_type, label, 3)], 1);
    }
}
