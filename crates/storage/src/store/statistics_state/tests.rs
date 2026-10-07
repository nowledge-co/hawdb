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

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    })
}

fn populated(store: &mut GraphStore, catalog: &mut Catalog) -> IndexId {
    let mut previous = None;
    for id in 0..64 {
        let node = store
            .create_node(
                catalog,
                "Memory",
                BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("score".into(), Value::Int(id % 7)),
                ]),
            )
            .unwrap();
        if let Some(source) = previous {
            store
                .create_relationship(catalog, source, node, "NEXT", BTreeMap::new())
                .unwrap();
        }
        previous = Some(node);
    }
    store
        .create_property_index(catalog, "Memory", "score")
        .unwrap()
}

#[test]
fn checkpoint_units_statistics_state_preserves_complete_wide_dtos_and_cancellation() {
    let basic = BasicGraphStatistics {
        computed_at_commit_epoch: 37,
        node_count: 1025,
        relationship_count: 1025,
        label_counts: (0..1025).map(|id| (LabelId(id), 1)).collect(),
        rel_type_counts: (0..1025).map(|id| (RelTypeId(id), 1)).collect(),
    };
    let state = BasicStatisticsState::from(basic.clone());
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = state
        .materialize_with_work_context(37, &probe.context(scheduler.clone()))
        .unwrap();
    assert_eq!(actual, basic);
    assert_eq!(probe.completed.load(Ordering::SeqCst), 2050);
    probe.assert_released(&scheduler);
    for limit in [1, 512, 1025, 2050] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        assert!(matches!(
            state.materialize_with_work_context(37, &probe.context(scheduler.clone())),
            Err(CheckpointWorkError::Stopped(_))
        ));
        assert_eq!(state.materialize(37), basic);
        probe.assert_released(&scheduler);
    }

    let mut advanced = GraphStatistics {
        computed_at_commit_epoch: 37,
        advanced_statistics_complete: true,
        node_count: 1025,
        relationship_count: 1025,
        label_counts: basic.label_counts,
        rel_type_counts: basic.rel_type_counts,
        index_samples: (0..1025)
            .map(|id| (IndexId(id), IndexStatisticsSample::exact(257, 129)))
            .collect(),
        ..GraphStatistics::default()
    };
    for id in 0..1025 {
        let key = (LabelId(id), format!("值-{id}-{}", "界".repeat(128)));
        advanced.property_distinct_counts.insert(key.clone(), 512);
        advanced
            .property_histograms
            .insert(key.clone(), (0..512).map(Value::Int).collect());
        advanced.sampled_property_histograms.insert(key, false);
        advanced
            .bounded_path_counts
            .insert((LabelId(id), RelTypeId(id), LabelId(id), 3), 19);
    }
    let probe = Arc::new(CheckpointWorkProbe::default());
    let state = CheckpointStatisticsState::with_work_context(
        advanced.clone(),
        &probe.context(scheduler.clone()),
    )
    .unwrap();
    assert_eq!(state.materialize(), advanced);
    assert_eq!(probe.completed.load(Ordering::SeqCst), 1026);
    probe.assert_released(&scheduler);
    let source = state.clone();
    let mut candidate = state.clone();
    candidate
        .index_samples
        .get_mut(&IndexId(513))
        .unwrap()
        .updates_since_sample += 1;
    assert!(source.advanced.shares_storage_with(&candidate.advanced));
    assert_eq!(source.materialize(), advanced);
    let mut expected = advanced.clone();
    expected
        .index_samples
        .get_mut(&IndexId(513))
        .unwrap()
        .updates_since_sample += 1;
    assert_eq!(candidate.materialize(), expected);
    candidate
        .property_distinct_counts
        .insert((LabelId(0), "candidate".into()), 3);
    expected
        .property_distinct_counts
        .insert((LabelId(0), "candidate".into()), 3);
    assert_eq!(candidate.materialize(), expected);
    assert_eq!(source.materialize(), advanced);
    for limit in [1, 512, 1025, 1026] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        assert!(matches!(
            CheckpointStatisticsState::with_work_context(
                advanced.clone(),
                &probe.context(scheduler.clone())
            ),
            Err(CheckpointWorkError::Stopped(_))
        ));
        assert_eq!(source.materialize(), advanced);
        probe.assert_released(&scheduler);
    }
}

#[test]
fn statistics_state_snapshot_keeps_samples_counts_and_graph_independent_after_writes() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let index = populated(&mut store, &mut catalog);
    let original = store.statistics(&catalog);
    store.restore_checkpoint_statistics(original.clone(), AdvancedStatisticsDirtyState::default());
    let mut source = store.snapshot();
    let source_basic = source.basic_statistics();

    store
        .set_node_property(
            &mut catalog,
            "Memory",
            Some(&PropertyFilter::Eq {
                property: "id".into(),
                value: Value::Int(7),
            }),
            "score",
            Value::Int(999),
        )
        .unwrap();
    let mut expected = original.clone();
    expected
        .index_samples
        .get_mut(&index)
        .unwrap()
        .updates_since_sample += 1;
    assert_eq!(store.checkpoint_statistics_snapshot(), expected);
    assert_eq!(source.checkpoint_statistics_snapshot(), original);
    assert!(source
        .checkpoint_statistics
        .advanced
        .shares_storage_with(&store.checkpoint_statistics.advanced));

    source
        .create_node(&mut catalog, "Entity", BTreeMap::new())
        .unwrap();
    assert_eq!(source.basic_statistics().node_count, 65);
    assert_eq!(store.basic_statistics().node_count, source_basic.node_count);
    assert!(source.basic_statistics_consistency_report().ready);
    assert!(store.basic_statistics_consistency_report().ready);
    store
        .delete_nodes(&mut catalog, "Memory", None, true)
        .unwrap();
    assert_eq!(store.basic_statistics().node_count, 0);
    assert!(store.basic_statistics().label_counts.is_empty());
    assert!(store.basic_statistics().rel_type_counts.is_empty());
    assert!(store.basic_statistics_consistency_report().ready);
    assert_eq!(source.basic_statistics().node_count, 65);
    assert_eq!(source.basic_statistics().relationship_count, 63);
    assert_eq!(source.checkpoint_statistics_snapshot(), original);
}

#[test]
fn checkpoint_units_statistics_state_retains_valid_samples_and_defers_without_mutation() {
    use hawdb_qos::{WorkClass, WorkRequest, WORK_CLASS_COUNT};

    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Memory");
    let mut original = GraphStatistics::default();
    for id in 0..1025 {
        let index = catalog.get_or_create_property_index(label, &format!("value-{id}"));
        original
            .index_samples
            .insert(index, IndexStatisticsSample::exact(31, 7));
    }
    let invalid = catalog.get_or_create_property_index(label, "invalid");
    original.index_samples.insert(
        invalid,
        IndexStatisticsSample {
            index_size: 1,
            unique_values: 2,
            sample_size: 2,
            updates_since_sample: 0,
        },
    );
    original
        .index_samples
        .insert(IndexId(u32::MAX), IndexStatisticsSample::exact(3, 2));
    let basic = BasicGraphStatistics {
        computed_at_commit_epoch: 13,
        ..BasicGraphStatistics::default()
    };
    let state = CheckpointStatisticsState::from(original.clone());
    let mut expected = original.clone();
    retain_valid_index_statistics_samples(&mut expected, &catalog);
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = crate::statistics::checkpoint::clone_retained_with_index_samples_and_work_context(
        &state,
        state.index_samples.iter(),
        &catalog,
        basic.clone(),
        &probe.context(scheduler.clone()),
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.index_samples.len(), 1025);
    assert_eq!(state.materialize(), original);
    probe.assert_released(&scheduler);
    let mut recovered = state.clone();
    recovered.retain_valid_index_samples(&catalog);
    assert_eq!(recovered.materialize(), expected);
    assert_eq!(state.materialize(), original);

    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        max_background_operations_by_class: [Some(1); WORK_CLASS_COUNT],
        ..LocalQosPolicy::default()
    });
    let blocker = scheduler
        .try_start(WorkRequest::background(WorkClass::Mutation, 1))
        .unwrap();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    assert!(matches!(
        CheckpointStatisticsState::with_work_context(original.clone(), &work),
        Err(CheckpointWorkError::Admission(_))
    ));
    let basic_state = BasicStatisticsState::from(BasicGraphStatistics {
        label_counts: BTreeMap::from([(label, 1)]),
        ..basic
    });
    assert!(matches!(
        basic_state.materialize_with_work_context(13, &work),
        Err(CheckpointWorkError::Admission(_))
    ));
    assert_eq!(state.materialize(), original);
    assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
    scheduler.set_telemetry_sink(None);
    drop(blocker);
    probe.assert_released(&scheduler);
    assert_eq!(
        basic_state
            .materialize_with_work_context(13, &probe.context(scheduler.clone()))
            .unwrap(),
        basic_state.materialize(13)
    );
    probe.assert_released(&scheduler);
}

struct TestDirectory(std::path::PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self(std::env::temp_dir().join(format!(
            "hawdb-statistics-state-{}-{nonce}",
            std::process::id()
        )))
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn statistics_state_checkpoint_reopen_and_retained_copy_preserve_samples_and_freshness() {
    let directory = TestDirectory::new();
    let config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &directory.0,
        &mut catalog,
        DurabilityPolicy::default(),
        config,
    )
    .unwrap();
    let index = populated(&mut store, &mut catalog);
    let original = store.statistics(&catalog);
    let mut initial = graph_statistics_from_basic(store.basic_statistics(), false);
    initial.index_samples = original.index_samples.clone();
    store.checkpoint(&catalog).unwrap();
    // The first transition to an out-of-core base deliberately retains basic
    // counts and index samples without computing advanced facts.
    assert_eq!(store.checkpoint_statistics_snapshot(), initial);
    drop(store);
    let mut reopened_catalog = Catalog::default();
    let mut reopened = GraphStore::open_with_durability_and_replay_config(
        &directory.0,
        &mut reopened_catalog,
        DurabilityPolicy::default(),
        config,
    )
    .unwrap();
    assert_eq!(reopened.statistics(&reopened_catalog), initial);
    assert_eq!(reopened.checkpoint_statistics_snapshot(), initial);
    reopened
        .restore_checkpoint_statistics(original.clone(), AdvancedStatisticsDirtyState::default());
    reopened.checkpoint(&reopened_catalog).unwrap();
    assert_eq!(reopened.statistics(&reopened_catalog), original);
    let source = reopened.checkpoint_source();
    reopened
        .set_node_property(
            &mut reopened_catalog,
            "Memory",
            Some(&PropertyFilter::Eq {
                property: "id".into(),
                value: Value::Int(13),
            }),
            "score",
            Value::Int(997),
        )
        .unwrap();
    let mut expected = original.clone();
    expected
        .index_samples
        .get_mut(&index)
        .unwrap()
        .updates_since_sample += 1;
    assert_eq!(reopened.checkpoint_statistics_snapshot(), expected);
    assert_eq!(source.checkpoint_statistics_snapshot(), original);
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        reopened
            .checkpoint_statistics_with_work_context(
                &reopened_catalog,
                &probe.context(scheduler.clone()),
            )
            .unwrap(),
        reopened.statistics(&reopened_catalog)
    );
    probe.assert_released(&scheduler);
    reopened.checkpoint(&reopened_catalog).unwrap();
    drop(source);
    let persisted = reopened.statistics(&reopened_catalog);
    assert_eq!(
        persisted.index_samples[&index],
        expected.index_samples[&index]
    );
    drop(reopened);
    let mut final_catalog = Catalog::default();
    let final_store = GraphStore::open_with_durability_and_replay_config(
        &directory.0,
        &mut final_catalog,
        DurabilityPolicy::default(),
        config,
    )
    .unwrap();
    assert_eq!(final_store.statistics(&final_catalog), persisted);
    assert_eq!(final_store.basic_statistics().node_count, 64);
    assert_eq!(final_store.basic_statistics().relationship_count, 63);
}
