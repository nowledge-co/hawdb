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
use crate::background::CheckpointWorkProbe;
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    })
}

fn chain(
    size: u64,
) -> (
    CowSegmentedMap<NodeId, NodeRecord>,
    CowSegmentedMap<RelId, RelRecord>,
) {
    let mut nodes = CowSegmentedMap::default();
    let mut relationships = CowSegmentedMap::default();
    for id in 0..size {
        nodes.insert(
            NodeId(id),
            NodeRecord {
                id: NodeId(id),
                labels: BTreeSet::from([LabelId(0)]),
                properties: BTreeMap::from([
                    ("rank".into(), Value::Int(id as i64)),
                    (
                        "late".into(),
                        if id + 1 == size {
                            Value::List(vec![])
                        } else {
                            Value::Int(id as i64)
                        },
                    ),
                ]),
            },
        );
        if id > 0 {
            relationships.insert(
                RelId(id),
                RelRecord {
                    id: RelId(id),
                    source: NodeId(id - 1),
                    target: NodeId(id),
                    rel_type: RelTypeId(0),
                    properties: BTreeMap::from([("rank".into(), Value::Int(id as i64))]),
                },
            );
        }
    }
    (nodes, relationships)
}

fn assert_cancelled<T: std::fmt::Debug>(result: Result<T, CheckpointWorkError>) {
    assert!(
        matches!(result, Err(CheckpointWorkError::Stopped(_))),
        "{result:?}"
    );
}

#[test]
fn checkpoint_units_statistics_preserve_complete_counts_paths_and_late_exclusion() {
    let (nodes, relationships) = chain(4);
    let basic = compute_basic_statistics(&nodes, &relationships, 73);
    let expected = compute_statistics_with_basic(&nodes, &relationships, None, basic.clone());
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = compute_with_work_context(
        &nodes,
        &relationships,
        None,
        basic,
        &probe.context(scheduler.clone()),
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.node_count, 4);
    assert_eq!(actual.relationship_count, 3);
    assert_eq!(actual.computed_at_commit_epoch, 73);
    assert_eq!(
        actual.property_histograms[&(LabelId(0), "rank".into())],
        (0..4).map(Value::Int).collect::<Vec<_>>()
    );
    assert!(!actual
        .property_distinct_counts
        .contains_key(&(LabelId(0), "late".into())));
    assert!(!actual
        .property_histograms
        .contains_key(&(LabelId(0), "late".into())));
    assert_eq!(actual.rel_type_source_counts[&RelTypeId(0)], 3);
    assert_eq!(actual.rel_type_target_counts[&RelTypeId(0)], 3);
    for hop in 1..=3 {
        let key = (LabelId(0), RelTypeId(0), LabelId(0), hop);
        assert_eq!(actual.bounded_path_counts[&key], (4 - hop) as u64);
        assert_eq!(
            actual.bounded_path_source_distinct_counts[&key],
            (4 - hop) as u64
        );
        assert_eq!(
            actual.bounded_path_target_distinct_counts[&key],
            (4 - hop) as u64
        );
    }
    assert!(probe.completed.load(Ordering::SeqCst) > 50);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    probe.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_statistics_histograms_keep_legacy_quantiles_without_full_vector() {
    for len in [0usize, 1, 128, 129, 1024, 1025, 4096, 4097, 8193] {
        let values = (0..len)
            .map(|value| Value::Int(value as i64))
            .collect::<BTreeSet<_>>();
        let expected = sample_histogram_values(values.clone());
        let scheduler = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual =
            sample_values_with_work_context(values, &probe.context(scheduler.clone())).unwrap();
        assert_eq!(actual, expected, "distinct={len}");
        assert_eq!(probe.completed.load(Ordering::SeqCst), len.div_ceil(1024));
        probe.assert_released(&scheduler);
    }
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(2, Ordering::SeqCst);
    assert_cancelled(sample_values_with_work_context(
        (0..8193).map(Value::Int).collect(),
        &probe.context(scheduler.clone()),
    ));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 2);
    probe.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_statistics_cancel_records_exclusion_and_path_visits_then_retry() {
    let (nodes, relationships) = chain(64);
    let basic = compute_basic_statistics(&nodes, &relationships, 41);
    let expected = compute_statistics_with_basic(&nodes, &relationships, None, basic.clone());
    let scheduler = scheduler();
    for limit in [1, 17, 127, 257, 511] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        assert_cancelled(compute_with_work_context(
            &nodes,
            &relationships,
            None,
            basic.clone(),
            &probe.context(scheduler.clone()),
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), limit);
        probe.assert_released(&scheduler);
    }
    // Exercise cancellation specifically while retiring a previously eligible
    // large group and during a path expansion, independent of earlier stages.
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(17, Ordering::SeqCst);
    let mut values = BTreeMap::from([(7, (0..2048).map(Value::Int).collect())]);
    let mut excluded = BTreeSet::new();
    assert_cancelled(collect_value_with_work_context(
        &mut values,
        &mut excluded,
        7,
        &Value::List(vec![]),
        false,
        &probe.context(scheduler.clone()),
    ));
    assert!(excluded.contains(&7));
    assert!(!values.contains_key(&7));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 17);
    probe.assert_released(&scheduler);
    let outgoing = relationships
        .values()
        .map(|r| ((r.source, r.rel_type), vec![r.target]))
        .collect();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(80, Ordering::SeqCst);
    assert_cancelled(bounded_paths_with_work_context(
        &nodes,
        &outgoing,
        3,
        &probe.context(scheduler.clone()),
    ));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 80);
    probe.assert_released(&scheduler);
    let retry = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        compute_with_work_context(
            &nodes,
            &relationships,
            None,
            basic,
            &retry.context(scheduler.clone())
        )
        .unwrap(),
        expected
    );
    retry.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_statistics_global_path_budget_does_not_publish_partial_counts() {
    let (nodes, _) = chain(30);
    let targets = (0..30).map(NodeId).collect::<Vec<_>>();
    let outgoing = (0..30)
        .map(|id| ((NodeId(id), RelTypeId(0)), targets.clone()))
        .collect();
    let expected = compute_bounded_path_statistics(&nodes, &outgoing, 3);
    let actual =
        bounded_paths_with_work_context(&nodes, &outgoing, 3, &Default::default()).unwrap();
    assert!(expected.truncated && actual.truncated);
    assert!(actual.counts.is_empty());
    assert!(actual.source_distinct_counts.is_empty());
    assert!(actual.target_distinct_counts.is_empty());
}

#[test]
fn checkpoint_units_statistics_index_samples_cover_scalar_composite_and_skipped_entries() {
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Memory");
    let other = catalog.get_or_create_label("Other");
    let scalar_id = catalog.get_or_create_property_index(label, "rank");
    let fulltext_id =
        catalog.get_or_create_property_index_with_kind(label, "rank", IndexKind::FullText);
    let composite_id =
        catalog.get_or_create_composite_property_index(label, &["rank".into(), "scope".into()]);
    let mut scalar = NodePropertyIndex::default();
    let mut composite = CompositePropertyIndex::default();
    for id in 0..2048u64 {
        scalar
            .entry_or_default((label, "rank".into(), Value::Int((id % 512) as i64)))
            .insert(NodeId(id));
        scalar
            .entry_or_default((other, "rank".into(), Value::Int(id as i64)))
            .insert(NodeId(id));
        composite
            .entry_or_default((
                label,
                vec![
                    ("rank".into(), Value::Int((id % 512) as i64)),
                    ("scope".into(), Value::String("a".into())),
                ],
            ))
            .insert(NodeId(id));
        composite
            .entry_or_default((
                label,
                vec![
                    ("scope".into(), Value::String("a".into())),
                    ("rank".into(), Value::Int(id as i64)),
                ],
            ))
            .insert(NodeId(id));
    }
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = index_samples_with_work_context(
        &catalog,
        &scalar,
        &composite,
        &probe.context(scheduler.clone()),
    )
    .unwrap();
    assert_eq!(
        actual,
        compute_index_statistics_samples(&catalog, &scalar, &composite)
    );
    assert_eq!(actual[&scalar_id], IndexStatisticsSample::exact(2048, 512));
    assert_eq!(
        actual[&composite_id],
        IndexStatisticsSample::exact(2048, 512)
    );
    assert!(!actual.contains_key(&fulltext_id));
    probe.assert_released(&scheduler);
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(17, Ordering::SeqCst);
    assert_cancelled(index_samples_with_work_context(
        &catalog,
        &scalar,
        &composite,
        &probe.context(scheduler.clone()),
    ));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 17);
    probe.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_statistics_retained_clone_matches_supported_groups_and_stale_epoch() {
    let (nodes, relationships) = chain(4);
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Memory");
    let index = catalog.get_or_create_property_index(label, "rank");
    let mut source = compute_statistics(&nodes, &relationships, 10);
    source.histogram_sample_limit = 256;
    source
        .index_samples
        .insert(index, IndexStatisticsSample::exact(4, 4));
    source
        .index_samples
        .insert(IndexId(999), IndexStatisticsSample::exact(4, 4));
    for (name, histogram) in [
        ("empty", vec![]),
        ("bad", vec![Value::Binary(vec![1])]),
        ("incomplete", vec![Value::Int(1)]),
    ] {
        let key = (label, name.into());
        source.property_distinct_counts.insert(key.clone(), 1);
        source.property_histograms.insert(key.clone(), histogram);
        if name != "incomplete" {
            source.sampled_property_histograms.insert(key, false);
        }
    }
    let basic = BasicGraphStatistics {
        computed_at_commit_epoch: 99,
        node_count: 7,
        relationship_count: 8,
        label_counts: BTreeMap::from([(label, 7)]),
        rel_type_counts: BTreeMap::from([(RelTypeId(0), 8)]),
    };
    let mut expected = source.clone();
    crate::statistics_refresh::retain_supported_property_statistics(&mut expected, Some(&catalog));
    crate::statistics_refresh::retain_valid_index_statistics_samples(&mut expected, &catalog);
    expected.node_count = basic.node_count;
    expected.relationship_count = basic.relationship_count;
    expected.label_counts = basic.label_counts.clone();
    expected.rel_type_counts = basic.rel_type_counts.clone();
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        clone_retained_with_work_context(
            &source,
            &catalog,
            basic.clone(),
            &probe.context(scheduler.clone())
        )
        .unwrap(),
        expected
    );
    probe.assert_released(&scheduler);
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(17, Ordering::SeqCst);
    assert_cancelled(clone_retained_with_work_context(
        &source,
        &catalog,
        basic,
        &probe.context(scheduler.clone()),
    ));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 17);
    probe.assert_released(&scheduler);
}
