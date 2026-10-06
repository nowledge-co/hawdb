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

//! Cooperative checkpoint statistics; the ordinary statistics APIs stay unchanged.
//!
//! Units cover records, properties, index entries, path visits and histogram
//! chunks. Complete fact sets, single-value cloning, allocations and cancellation
//! destruction still require the owner's retained-memory/resource accounting.

use super::*;
use crate::background::{CheckpointWorkContext, CheckpointWorkError};

pub fn compute_with_work_context(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    catalog: Option<&Catalog>,
    basic_statistics: BasicGraphStatistics,
    work: &CheckpointWorkContext,
) -> Result<GraphStatistics, CheckpointWorkError> {
    work.checkpoint()?;
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
        let unit = work.start_unit()?;
        unit.finish();
        for label_id in &node.labels {
            let unit = work.start_unit()?;
            unit.finish();
            for (property, value) in &node.properties {
                let unit = work.start_unit()?;
                let key = (*label_id, property.clone());
                let eligible = node_property_supports_optimizer_statistics(
                    catalog, *label_id, property, value,
                );
                unit.finish();
                collect_value_with_work_context(
                    &mut property_values,
                    &mut excluded_property_groups,
                    key,
                    value,
                    eligible,
                    work,
                )?;
            }
        }
    }
    for relationship in relationships.values() {
        let unit = work.start_unit()?;
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
        unit.finish();
        for (property, value) in &relationship.properties {
            let unit = work.start_unit()?;
            let key = (relationship.rel_type, property.clone());
            let eligible = relationship_property_supports_optimizer_statistics(
                catalog,
                relationship.rel_type,
                property,
                value,
            );
            unit.finish();
            collect_value_with_work_context(
                &mut rel_property_values,
                &mut excluded_rel_property_groups,
                key,
                value,
                eligible,
                work,
            )?;
        }
        if let (Some(source), Some(target)) = (
            nodes.get(&relationship.source),
            nodes.get(&relationship.target),
        ) {
            for source_label in &source.labels {
                for target_label in &target.labels {
                    let unit = work.start_unit()?;
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
                    unit.finish();
                }
            }
        }
    }
    statistics.rel_type_source_counts = distinct_counts(rel_type_sources, work)?;
    statistics.rel_type_target_counts = distinct_counts(rel_type_targets, work)?;
    statistics.path_source_distinct_counts = distinct_counts(path_sources, work)?;
    statistics.path_target_distinct_counts = distinct_counts(path_targets, work)?;
    let (counts, histograms, sampled) = histograms_with_work_context(property_values, work)?;
    statistics.property_distinct_counts = counts;
    statistics.property_histograms = histograms;
    statistics.sampled_property_histograms = sampled;
    let (counts, histograms, sampled) = histograms_with_work_context(rel_property_values, work)?;
    statistics.rel_property_distinct_counts = counts;
    statistics.rel_property_histograms = histograms;
    statistics.sampled_rel_property_histograms = sampled;
    let bounded_path_statistics = bounded_paths_with_work_context(
        nodes,
        &outgoing_by_source_type,
        MAX_BOUNDED_PATH_STAT_HOPS,
        work,
    )?;
    statistics.bounded_path_counts = bounded_path_statistics.counts;
    statistics.bounded_path_source_distinct_counts = bounded_path_statistics.source_distinct_counts;
    statistics.bounded_path_target_distinct_counts = bounded_path_statistics.target_distinct_counts;
    work.checkpoint()?;
    Ok(statistics)
}

fn collect_value_with_work_context<K: Ord>(
    values: &mut BTreeMap<K, BTreeSet<Value>>,
    excluded: &mut BTreeSet<K>,
    key: K,
    value: &Value,
    eligible: bool,
    work: &CheckpointWorkContext,
) -> Result<(), CheckpointWorkError> {
    let unit = work.start_unit()?;
    let retired = if !eligible {
        let retired = values.remove(&key);
        excluded.insert(key);
        retired
    } else {
        if !excluded.contains(&key) {
            values.entry(key).or_default().insert(value.clone());
        }
        None
    };
    unit.finish();
    if let Some(retired) = retired {
        drain_values(retired, work)?;
    }
    Ok(())
}

fn drain_values<T: Ord>(
    mut values: BTreeSet<T>,
    work: &CheckpointWorkContext,
) -> Result<(), CheckpointWorkError> {
    while !values.is_empty() {
        let unit = work.start_unit()?;
        drop(values.pop_first());
        unit.finish();
    }
    work.checkpoint()
}

fn distinct_counts<K: Ord>(
    sets: BTreeMap<K, BTreeSet<NodeId>>,
    work: &CheckpointWorkContext,
) -> Result<BTreeMap<K, u64>, CheckpointWorkError> {
    let mut counts = BTreeMap::new();
    for (key, values) in sets {
        let unit = work.start_unit()?;
        counts.insert(key, values.len() as u64);
        unit.finish();
        drain_values(values, work)?;
    }
    work.checkpoint()?;
    Ok(counts)
}

type PropertyHistograms<K> = (BTreeMap<K, u64>, BTreeMap<K, Vec<Value>>, BTreeMap<K, bool>);

fn histograms_with_work_context<K: Ord + Clone>(
    groups: BTreeMap<K, BTreeSet<Value>>,
    work: &CheckpointWorkContext,
) -> Result<PropertyHistograms<K>, CheckpointWorkError> {
    let mut counts = BTreeMap::new();
    let mut histograms = BTreeMap::new();
    let mut sampled = BTreeMap::new();
    for (key, values) in groups {
        let unit = work.start_unit()?;
        let len = values.len();
        let sample_limit = adaptive_histogram_sample_limit(len);
        counts.insert(key.clone(), len as u64);
        sampled.insert(key.clone(), len > sample_limit);
        unit.finish();
        histograms.insert(key, sample_values_with_work_context(values, work)?);
    }
    work.checkpoint()?;
    Ok((counts, histograms, sampled))
}

fn sample_values_with_work_context(
    values: BTreeSet<Value>,
    work: &CheckpointWorkContext,
) -> Result<Vec<Value>, CheckpointWorkError> {
    let len = values.len();
    let limit = adaptive_histogram_sample_limit(len);
    let mut samples = Vec::new();
    let mut input = values.into_iter();
    let mut offset = 0;
    // Preserve the old endpoint-inclusive quantile indices, but consume the
    // set directly rather than retaining a second complete sorted vector.
    while offset < len {
        let unit = work.start_unit()?;
        let end = offset.saturating_add(1024).min(len);
        for index in offset..end {
            let value = input.next().expect("set length is stable");
            if len <= limit || index == samples.len() * (len - 1) / (limit - 1) {
                samples.push(value);
            }
        }
        offset = end;
        unit.finish();
    }
    work.checkpoint()?;
    Ok(samples)
}

pub fn index_samples_with_work_context(
    catalog: &Catalog,
    property_index: &NodePropertyIndex,
    composite_property_index: &CompositePropertyIndex,
    work: &CheckpointWorkContext,
) -> Result<BTreeMap<IndexId, IndexStatisticsSample>, CheckpointWorkError> {
    let mut samples = BTreeMap::new();
    for index in catalog.property_indexes() {
        let unit = work.start_unit()?;
        let full_text = index.kind == IndexKind::FullText;
        unit.finish();
        if full_text {
            continue;
        }
        let mut size = 0_u64;
        let mut unique = 0_u64;
        for ((label, property, _), ids) in property_index.iter() {
            let unit = work.start_unit()?;
            if *label == index.label_id && *property == index.property {
                size = size.saturating_add(ids.len() as u64);
                unique = unique.saturating_add(1);
            }
            unit.finish();
        }
        let unit = work.start_unit()?;
        samples.insert(index.id, IndexStatisticsSample::exact(size, unique));
        unit.finish();
    }
    for index in catalog.composite_property_indexes() {
        let unit = work.start_unit()?;
        unit.finish();
        let mut size = 0_u64;
        let mut unique = 0_u64;
        for ((label, key), ids) in composite_property_index.iter() {
            let unit = work.start_unit()?;
            let label_matches = *label == index.label_id;
            let length_matches = key.len() == index.properties.len();
            unit.finish();
            let mut matches = label_matches && length_matches;
            if matches {
                for ((property, _), expected) in key.iter().zip(&index.properties) {
                    let unit = work.start_unit()?;
                    matches = property == expected;
                    unit.finish();
                    if !matches {
                        break;
                    }
                }
            }
            if matches {
                let unit = work.start_unit()?;
                size = size.saturating_add(ids.len() as u64);
                unique = unique.saturating_add(1);
                unit.finish();
            }
        }
        let unit = work.start_unit()?;
        samples.insert(index.id, IndexStatisticsSample::exact(size, unique));
        unit.finish();
    }
    work.checkpoint()?;
    Ok(samples)
}

fn bounded_paths_with_work_context(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    outgoing_by_source_type: &BTreeMap<(NodeId, RelTypeId), Vec<NodeId>>,
    max_hops: usize,
    work: &CheckpointWorkContext,
) -> Result<BoundedPathStatistics, CheckpointWorkError> {
    let mut accumulator = BoundedPathStatAccumulator::default();
    let context = CheckpointPathContext {
        nodes,
        outgoing_by_source_type,
        max_hops,
        work,
    };
    let mut rel_types = BTreeSet::new();
    for (_, rel_type) in outgoing_by_source_type.keys() {
        let unit = work.start_unit()?;
        rel_types.insert(*rel_type);
        unit.finish();
    }
    'sources: for source in nodes.values() {
        let unit = work.start_unit()?;
        unit.finish();
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
                )?;
            }
        }
    }
    work.checkpoint()?;
    if accumulator.exhausted {
        // A visit-budget truncation has exactly the existing no-partial-stats
        // contract. Cancellation is an error rather than another truncation.
        for values in accumulator
            .sources
            .into_values()
            .chain(accumulator.targets.into_values())
        {
            drain_values(values, work)?;
        }
        return Ok(BoundedPathStatistics {
            truncated: true,
            ..BoundedPathStatistics::default()
        });
    }
    Ok(BoundedPathStatistics {
        counts: accumulator.counts,
        source_distinct_counts: distinct_counts(accumulator.sources, work)?,
        target_distinct_counts: distinct_counts(accumulator.targets, work)?,
        truncated: false,
    })
}

struct CheckpointPathContext<'a> {
    nodes: &'a CowSegmentedMap<NodeId, NodeRecord>,
    outgoing_by_source_type: &'a BTreeMap<(NodeId, RelTypeId), Vec<NodeId>>,
    max_hops: usize,
    work: &'a CheckpointWorkContext,
}

impl CheckpointPathContext<'_> {
    fn collect(
        &self,
        root_source: NodeId,
        current: NodeId,
        source_label: LabelId,
        rel_type: RelTypeId,
        hop: usize,
        accumulator: &mut BoundedPathStatAccumulator,
    ) -> Result<(), CheckpointWorkError> {
        let unit = self.work.start_unit()?;
        if hop > self.max_hops || accumulator.exhausted {
            unit.finish();
            return Ok(());
        }
        let targets = self.outgoing_by_source_type.get(&(current, rel_type));
        unit.finish();
        let Some(targets) = targets else {
            return Ok(());
        };
        for target_id in targets {
            let unit = self.work.start_unit()?;
            if accumulator.visits >= MAX_BOUNDED_PATH_STAT_VISITS {
                accumulator.exhausted = true;
                unit.finish();
                return Ok(());
            }
            accumulator.visits += 1;
            let target = self.nodes.get(target_id);
            unit.finish();
            let Some(target) = target else { continue };
            for target_label in &target.labels {
                let unit = self.work.start_unit()?;
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
                unit.finish();
            }
            self.collect(
                root_source,
                *target_id,
                source_label,
                rel_type,
                hop + 1,
                accumulator,
            )?;
        }
        self.work.checkpoint()
    }
}

pub fn clone_basic_with_work_context(
    basic: &BasicGraphStatistics,
    commit_epoch: u64,
    work: &CheckpointWorkContext,
) -> Result<BasicGraphStatistics, CheckpointWorkError> {
    Ok(BasicGraphStatistics {
        computed_at_commit_epoch: commit_epoch,
        node_count: basic.node_count,
        relationship_count: basic.relationship_count,
        label_counts: clone_map_with_work_context(&basic.label_counts, work)?,
        rel_type_counts: clone_map_with_work_context(&basic.rel_type_counts, work)?,
    })
}

fn clone_map_with_work_context<K: Ord + Clone, V: Clone>(
    map: &BTreeMap<K, V>,
    work: &CheckpointWorkContext,
) -> Result<BTreeMap<K, V>, CheckpointWorkError> {
    let mut copy = BTreeMap::new();
    for (key, value) in map {
        let unit = work.start_unit()?;
        copy.insert(key.clone(), value.clone());
        unit.finish();
    }
    work.checkpoint()?;
    Ok(copy)
}

pub fn clone_retained_with_work_context(
    source: &GraphStatistics,
    catalog: &Catalog,
    basic: BasicGraphStatistics,
    work: &CheckpointWorkContext,
) -> Result<GraphStatistics, CheckpointWorkError> {
    let mut copy = graph_statistics_from_basic(basic, source.advanced_statistics_complete);
    copy.computed_at_commit_epoch = source.computed_at_commit_epoch;
    copy.histogram_sample_limit = source.histogram_sample_limit;
    copy.rel_type_source_counts =
        clone_map_with_work_context(&source.rel_type_source_counts, work)?;
    copy.rel_type_target_counts =
        clone_map_with_work_context(&source.rel_type_target_counts, work)?;
    copy.path_counts = clone_map_with_work_context(&source.path_counts, work)?;
    copy.path_source_distinct_counts =
        clone_map_with_work_context(&source.path_source_distinct_counts, work)?;
    copy.path_target_distinct_counts =
        clone_map_with_work_context(&source.path_target_distinct_counts, work)?;
    copy.bounded_path_counts = clone_map_with_work_context(&source.bounded_path_counts, work)?;
    copy.bounded_path_source_distinct_counts =
        clone_map_with_work_context(&source.bounded_path_source_distinct_counts, work)?;
    copy.bounded_path_target_distinct_counts =
        clone_map_with_work_context(&source.bounded_path_target_distinct_counts, work)?;
    for (id, sample) in &source.index_samples {
        let unit = work.start_unit()?;
        if catalog.supports_index_statistics(*id) && sample.is_valid() {
            copy.index_samples.insert(*id, *sample);
        }
        unit.finish();
    }
    let (counts, histograms, sampled) = clone_supported_histograms(
        &source.property_distinct_counts,
        &source.property_histograms,
        &source.sampled_property_histograms,
        |(label, property), value| {
            node_property_supports_optimizer_statistics(Some(catalog), *label, property, value)
        },
        work,
    )?;
    copy.property_distinct_counts = counts;
    copy.property_histograms = histograms;
    copy.sampled_property_histograms = sampled;
    let (counts, histograms, sampled) = clone_supported_histograms(
        &source.rel_property_distinct_counts,
        &source.rel_property_histograms,
        &source.sampled_rel_property_histograms,
        |(rel_type, property), value| {
            relationship_property_supports_optimizer_statistics(
                Some(catalog),
                *rel_type,
                property,
                value,
            )
        },
        work,
    )?;
    copy.rel_property_distinct_counts = counts;
    copy.rel_property_histograms = histograms;
    copy.sampled_rel_property_histograms = sampled;
    work.checkpoint()?;
    Ok(copy)
}

fn clone_supported_histograms<K: Ord + Clone>(
    counts: &BTreeMap<K, u64>,
    histograms: &BTreeMap<K, Vec<Value>>,
    sampled: &BTreeMap<K, bool>,
    mut supports: impl FnMut(&K, &Value) -> bool,
    work: &CheckpointWorkContext,
) -> Result<PropertyHistograms<K>, CheckpointWorkError> {
    let mut copied_counts = BTreeMap::new();
    let mut copied_histograms = BTreeMap::new();
    let mut copied_sampled = BTreeMap::new();
    for (key, values) in histograms {
        let unit = work.start_unit()?;
        let flags = counts.get(key).zip(sampled.get(key));
        unit.finish();
        let Some((count, is_sampled)) = flags.filter(|_| !values.is_empty()) else {
            continue;
        };
        let mut copied_values = Vec::new();
        let mut eligible = true;
        for value in values {
            let unit = work.start_unit()?;
            eligible = supports(key, value);
            if eligible {
                copied_values.push(value.clone());
            }
            unit.finish();
            if !eligible {
                break;
            }
        }
        if eligible {
            let unit = work.start_unit()?;
            copied_counts.insert(key.clone(), *count);
            copied_sampled.insert(key.clone(), *is_sampled);
            copied_histograms.insert(key.clone(), copied_values);
            unit.finish();
        }
    }
    work.checkpoint()?;
    Ok((copied_counts, copied_histograms, copied_sampled))
}

#[cfg(test)]
mod tests;
