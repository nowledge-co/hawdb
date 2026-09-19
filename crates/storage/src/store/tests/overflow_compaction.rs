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

#[test]
fn exact_overflow_compaction_rewrites_reachable_closure_without_hydration() {
    let default_config = super::RelationalOverflowCompactionConfig::default();
    assert_eq!(default_config.admission_bytes().unwrap(), 154 * 1024 * 1024);
    assert!(default_config.admission_bytes().unwrap() < 512 * 1024 * 1024);
    let path = unique_test_dir("exact-overflow-compaction");
    let seed_replay = WalReplayConfig {
        relational_index_mode: RelationalIndexMode::Shadow,
        ..WalReplayConfig::default()
    };
    let replay = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        relational_index_mode: RelationalIndexMode::Authoritative,
        ..WalReplayConfig::default()
    };
    let retained_body = format!("retained-{}", "a".repeat(8 * 1024));
    let removed_body = format!("removed-{}", "b".repeat(8 * 1024));
    let introduced_body = format!("introduced-{}", "c".repeat(8 * 1024));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        seed_replay,
    )
    .unwrap();
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(schema()),
                    RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![row(1, &retained_body), row(2, &removed_body)],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    drop(store);
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay,
    )
    .unwrap();
    assert!(store.relational_state.canonical_row_metadata_only());
    let generation_one = store
        .durable
        .as_ref()
        .unwrap()
        .open_bound_relational_overflow()
        .unwrap();
    assert_eq!(generation_one.manifest().extent_count, 2);

    let rejected_config = super::RelationalOverflowCompactionConfig {
        max_scan_rows: NonZeroUsize::new(1).unwrap(),
        ..super::RelationalOverflowCompactionConfig::default()
    };
    let rejected = store
        .compact_relational_overflow(
            &catalog,
            None,
            rejected_config,
            &RuntimeTaskContext::default(),
        )
        .unwrap_err();
    assert!(rejected.to_string().contains("exceeding limit 1"));
    assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 1);
    assert!(!path
        .join(relational_overflow_manifest_generation_file(2))
        .exists());
    let rewrite_rejected_config = super::RelationalOverflowCompactionConfig {
        max_rewrite_bytes: NonZeroU64::new(1).unwrap(),
        ..super::RelationalOverflowCompactionConfig::default()
    };
    let rewrite_rejected = store
        .compact_relational_overflow(
            &catalog,
            None,
            rewrite_rejected_config,
            &RuntimeTaskContext::default(),
        )
        .unwrap_err();
    assert!(rewrite_rejected
        .to_string()
        .contains("rewritten extent bytes"));
    assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 1);
    assert!(!path
        .join(relational_overflow_manifest_generation_file(2))
        .exists());
    let cancellation = RuntimeCancellationToken::new();
    let cancelled_task = RuntimeTaskContext::without_deadline(cancellation.clone());
    cancellation.cancel();
    let cancelled = store
        .compact_relational_overflow(
            &catalog,
            None,
            super::RelationalOverflowCompactionConfig::default(),
            &cancelled_task,
        )
        .unwrap_err();
    assert!(cancelled.to_string().contains("compaction stopped"));
    assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 1);

    let pinned = store.snapshot();
    let pinned_reader = pinned
        .open_relational_row_snapshot_reader()
        .unwrap()
        .expect("generation-one row reader");
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::DeleteByPrimaryKey {
                    table: "documents".to_string(),
                    keys: vec![key(2)],
                }],
            },
        )
        .unwrap();
    store
        .checkpoint_with_reader_epoch(&catalog, Some(1))
        .unwrap();
    let retained_base = store
        .durable
        .as_ref()
        .unwrap()
        .open_bound_relational_overflow()
        .unwrap();
    assert_eq!(retained_base.manifest().generation, 2);
    assert_eq!(retained_base.manifest().extent_count, 2);
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![row(3, &introduced_body)],
                    mode: RelationalInsertMode::Error,
                }],
            },
        )
        .unwrap();

    let report = store
        .compact_relational_overflow(
            &catalog,
            Some(1),
            default_config,
            &RuntimeTaskContext::default(),
        )
        .unwrap();
    assert_eq!(report.published_generation, 3);
    assert_eq!(report.rows_scanned, 2);
    assert_eq!(report.hydrated_values, 0);
    assert_eq!(report.reference_occurrences, 2);
    assert_eq!(report.unique_references, 2);
    assert_eq!(report.previous_extent_count, 2);
    assert_eq!(report.published_extent_count, 2);
    assert_eq!(report.reclaimable_base_extent_count, 1);
    assert_eq!(report.copied_base_extent_count, 1);
    assert_eq!(report.introduced_extent_count, 1);
    assert_eq!(report.new_extent_count, 2);
    assert_eq!(report.reused_extent_count, 0);
    let compacted = store
        .durable
        .as_ref()
        .unwrap()
        .open_bound_relational_overflow()
        .unwrap();
    assert_eq!(compacted.manifest().generation, 3);
    assert_eq!(compacted.manifest().extent_count, 2);
    let mut physical_generations = Vec::new();
    compacted
        .visit_descriptors(|descriptor| {
            physical_generations.push(descriptor.physical_generation);
            Ok(())
        })
        .unwrap();
    assert_eq!(physical_generations, vec![3, 3]);

    let mut hydration = RelationalHydrationBudget::default();
    let (old_row, old_report) = pinned_reader
        .point_projected(
            "documents",
            &key(2),
            &[1],
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        )
        .unwrap();
    assert_eq!(
        old_row.unwrap().fields[0].value,
        RelationalValue::Text(removed_body)
    );
    assert_eq!(old_report.identity.base_generation, 1);
    assert!(path.join(relational_overflow_extent_file(1)).exists());
    drop(pinned_reader);
    drop(pinned);

    store
        .create_node(&mut catalog, "CheckpointMarker", BTreeMap::new())
        .unwrap();
    store.checkpoint_with_reader_epoch(&catalog, None).unwrap();
    assert!(!path.join(relational_overflow_extent_file(1)).exists());
    drop(store);

    let mut reopened_catalog = Catalog::default();
    let mut reopened = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut reopened_catalog,
        DurabilityPolicy::default(),
        replay,
    )
    .unwrap();
    assert_eq!(reopened.relational_state.row_count("documents"), 2);
    let current = reopened
        .open_relational_row_snapshot_reader()
        .unwrap()
        .expect("compacted row reader");
    let mut hydration = RelationalHydrationBudget::default();
    let (row, _) = current
        .point_projected(
            "documents",
            &key(1),
            &[1],
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        )
        .unwrap();
    assert_eq!(
        row.unwrap().fields[0].value,
        RelationalValue::Text(retained_body)
    );
    let (introduced, _) = current
        .point_projected(
            "documents",
            &key(3),
            &[1],
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        )
        .unwrap();
    assert_eq!(
        introduced.unwrap().fields[0].value,
        RelationalValue::Text(introduced_body)
    );
    drop(current);

    let extent = path.join(relational_overflow_extent_file(3));
    let mut corrupted = std::fs::read(&extent).unwrap();
    *corrupted.last_mut().expect("non-empty overflow extent") ^= 1;
    std::fs::write(&extent, corrupted).unwrap();
    let corruption = reopened
        .compact_relational_overflow(
            &reopened_catalog,
            None,
            default_config,
            &RuntimeTaskContext::default(),
        )
        .unwrap_err();
    assert!(matches!(
        corruption,
        crate::error::HawDBError::StorageIntegrity(_)
    ));
    assert!(!path
        .join(relational_overflow_manifest_generation_file(5))
        .exists());
    assert!(reopened.storage_handle_poisoned());
    let poisoned = reopened.ensure_usable().unwrap_err();
    assert!(poisoned.to_string().contains("poisoned"));
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}
