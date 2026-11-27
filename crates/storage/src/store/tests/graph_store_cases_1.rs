// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

#[test]
fn derived_repair_borrows_existing_custom_descriptor_budget() {
    let path = unique_test_dir("derived_repair_custom_descriptors");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            max_open_files: 64,
            strict_capacity_limit: true,
            ..WalReplayConfig::default()
        },
    )
    .unwrap();
    store.checkpoint(&catalog).unwrap();
    let domain =
        crate::file_descriptors::ProjectFileDescriptors::acquire_existing(&path, 64).unwrap();
    drop(store);
    let (repaired, _) = GraphStore::open_for_derived_repair(
        &path,
        WalReplayConfig {
            recovery_mode: crate::config::RecoveryMode::Strict,
            ..WalReplayConfig::default()
        },
    )
    .unwrap();
    assert_eq!(repaired.file_descriptor_metrics().unwrap().limit, 64);
    assert!(matches!(
        crate::file_descriptors::ProjectFileDescriptors::acquire_existing(&path, 256),
        Err(HawDBError::FileDescriptors(
            hawdb_core::error::FileDescriptorError::ConfigurationConflict {
                configured: 64,
                requested: 256
            }
        ))
    ));
    drop(repaired);
    assert_eq!(domain.metrics().open, 0);
    drop(domain);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn current_epoch_version_baseline_bounds_history_and_preserves_older_sources() {
    use crate::cow::CowPageWeight;
    use crate::version::{VersionIndex, VersionKey, VersionStamp};
    for durable in [false, true] {
        let path = unique_test_dir("current_epoch_version_baseline");
        let mut catalog = Catalog::default();
        let mut store = if durable {
            GraphStore::open(&path, &mut catalog).unwrap()
        } else {
            GraphStore::default()
        };
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("value", Value::Int(0))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let reserve = VersionIndex::default().estimated_bytes();
        let weight =
            VersionKey::GraphNode(NodeId(0)).cow_page_bytes() + std::mem::size_of::<VersionStamp>();
        for id in 1..=64 {
            let before = store.commit_epoch();
            store
                .commit_mutations(
                    &mut catalog,
                    vec![GraphMutation::CreateNode {
                        label: "Memory".into(),
                        properties: properties([("value", Value::Int(0))]),
                    }],
                )
                .unwrap();
            assert_eq!(store.version_index.len(), 2);
            assert_eq!(store.version_index.estimated_bytes(), reserve + weight);
            assert_eq!(store.version_index.retained_history_bytes(), weight);
            assert_eq!(
                store
                    .version_index
                    .stamp(&VersionKey::Database)
                    .unwrap()
                    .commit_epoch,
                before
            );
            assert_eq!(
                store
                    .version_index
                    .stamp(&VersionKey::GraphNode(NodeId(id)))
                    .unwrap()
                    .commit_epoch,
                before + 1
            );
        }
        let update = |id, value| GraphMutation::SetNodeProperty {
            label: "Memory".into(),
            filter: Some(PropertyFilter::IdEq {
                value: Value::Int(id),
            }),
            property: "value".into(),
            value: Value::Int(value),
        };
        if durable {
            let before = store.snapshot(); // equality at the tip permits baseline preparation
            let wal = fs::read(active_wal_path(&path)).unwrap();
            store
                .durable
                .as_mut()
                .unwrap()
                .set_wal_available_space_override(0);
            assert!(store
                .commit_mutations(&mut catalog, vec![update(0, 99)])
                .is_err());
            assert_eq!(store.commit_epoch(), before.commit_epoch());
            assert_eq!(
                store.version_index.estimated_bytes(),
                before.version_index.estimated_bytes()
            );
            assert_eq!(store.version_index.retained_history_bytes(), weight);
            assert!(store
                .version_index
                .shares_storage_with(&before.version_index));
            assert_eq!(
                store.version_index.stamp(&VersionKey::Database),
                before.version_index.stamp(&VersionKey::Database)
            );
            assert_eq!(fs::read(active_wal_path(&path)).unwrap(), wal);
            store
                .durable
                .as_mut()
                .unwrap()
                .set_wal_available_space_override(u64::MAX);
        }
        let source = store.snapshot();
        let source_epoch = source.commit_epoch();
        store
            .commit_mutations(&mut catalog, vec![update(1, 10)])
            .unwrap();
        store
            .commit_mutations(&mut catalog, vec![update(2, 20)])
            .unwrap();
        // A retained source can create writers after intervening commits.
        // The unchanged key must remain admissible, the changed key must not.
        let mut disjoint = source.begin_mutation_transaction(&catalog);
        let mut overlapping = source.begin_mutation_transaction(&catalog);
        disjoint
            .stage_mutation_with_limits(update(0, 30), MutationLimits::default())
            .unwrap();
        overlapping
            .stage_mutation_with_limits(update(1, 99), MutationLimits::default())
            .unwrap();
        store
            .commit_mutation_transaction_and_relational(
                &mut catalog,
                disjoint,
                RelationalTransaction::default(),
                MutationLimits::default(),
            )
            .unwrap();
        assert_eq!(
            store
                .version_index
                .stamp(&VersionKey::Database)
                .unwrap()
                .commit_epoch,
            source_epoch
        );
        let epoch = store.commit_epoch();
        let wal = durable.then(|| fs::read(active_wal_path(&path)).unwrap());
        assert!(store
            .commit_mutation_transaction_and_relational(
                &mut catalog,
                overlapping,
                RelationalTransaction::default(),
                MutationLimits::default()
            )
            .unwrap_err()
            .is_retryable_transaction_conflict());
        assert_eq!(store.commit_epoch(), epoch);
        if let Some(wal) = wal {
            assert_eq!(fs::read(active_wal_path(&path)).unwrap(), wal);
        }
        let descendant = source.snapshot();
        drop(source);
        store
            .commit_mutations(&mut catalog, vec![update(2, 21)])
            .unwrap();
        assert_eq!(
            store
                .version_index
                .stamp(&VersionKey::Database)
                .unwrap()
                .commit_epoch,
            source_epoch
        );
        for id in 0..3 {
            assert_eq!(
                descendant
                    .node_owned(NodeId(id))
                    .unwrap()
                    .unwrap()
                    .properties["value"],
                Value::Int(0)
            );
        }
        drop(descendant);
        let before = store.commit_epoch();
        store
            .commit_mutations(&mut catalog, vec![update(0, 31)])
            .unwrap();
        assert_eq!(store.version_index.len(), 2);
        assert_eq!(store.version_index.retained_history_bytes(), weight);
        assert_eq!(
            store
                .version_index
                .stamp(&VersionKey::Database)
                .unwrap()
                .commit_epoch,
            before
        );
        let epoch = store.commit_epoch();
        if durable {
            drop(store);
            catalog = Catalog::default();
            store = GraphStore::open(&path, &mut catalog).unwrap();
        }
        assert_eq!(store.commit_epoch(), epoch);
        assert_eq!(store.node_count_for_label(None), 65);
        for (id, value) in [(0, 31), (1, 10), (2, 21)] {
            assert_eq!(
                store.node_owned(NodeId(id)).unwrap().unwrap().properties["value"],
                Value::Int(value)
            );
        }
        drop(store);
        if durable {
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn current_epoch_version_baseline_keeps_pinned_pressure_reclamation_available() {
    use crate::cow::CowPageWeight;
    use crate::version::{VersionIndex, VersionKey, VersionStamp};
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    let reserve = VersionIndex::default().estimated_bytes();
    let weight =
        VersionKey::GraphNode(NodeId(0)).cow_page_bytes() + std::mem::size_of::<VersionStamp>();
    store
        .create_node(&mut catalog, "Memory", properties([]))
        .unwrap();
    store.version_index = VersionIndex::with_byte_limit(reserve + weight);
    let insert = || GraphMutation::CreateNode {
        label: "Memory".into(),
        properties: properties([]),
    };
    store
        .commit_mutations(&mut catalog, vec![insert()])
        .unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([]))
        .unwrap();
    let reader = store.snapshot();
    // This direct commit leaves an obsolete narrow stamp below the pin,
    // while advancing the tip so whole-index baseline preparation is unsafe.
    store
        .create_node(&mut catalog, "Memory", properties([]))
        .unwrap();
    assert_eq!(store.version_index.estimated_bytes(), reserve + weight);
    store
        .commit_mutations(&mut catalog, vec![insert()])
        .unwrap();
    assert_eq!(store.version_index.estimated_bytes(), reserve + weight);
    assert_eq!(store.node_count_for_label(None), 5);
    assert_eq!(reader.node_count_for_label(None), 3);
    assert!(store
        .version_index
        .stamp(&VersionKey::GraphNode(NodeId(1)))
        .is_none());
    assert!(store
        .version_index
        .stamp(&VersionKey::GraphNode(NodeId(4)))
        .is_some());
}

#[test]
fn retained_history_budget_refunds_wal_refusal_and_recovers_after_snapshot_drop() {
    use crate::cow::CowPageWeight;
    use crate::version::{VersionIndex, VersionKey, VersionStamp};
    for durable in [false, true] {
        let path = unique_test_dir("retained_history_budget");
        let mut catalog = Catalog::default();
        let mut store = if durable {
            GraphStore::open(&path, &mut catalog).unwrap()
        } else {
            GraphStore::default()
        };
        store
            .create_node(&mut catalog, "Memory", properties([]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let reserve = VersionIndex::default().estimated_bytes();
        let weight =
            VersionKey::GraphNode(NodeId(0)).cow_page_bytes() + std::mem::size_of::<VersionStamp>();
        store.version_index = VersionIndex::with_history_limit(reserve + 4 * weight, 4 * weight);
        let insert = || GraphMutation::CreateNode {
            label: "Memory".into(),
            properties: properties([]),
        };
        let protection = store.snapshot();
        store
            .commit_mutations(&mut catalog, vec![insert()])
            .unwrap();
        assert_eq!(store.version_index.retained_history_bytes(), weight);
        let old = store.snapshot();
        if durable {
            let epoch = store.commit_epoch();
            let wal = fs::read(active_wal_path(&path)).unwrap();
            store
                .durable
                .as_mut()
                .unwrap()
                .set_wal_available_space_override(0);
            assert!(store
                .commit_mutations(&mut catalog, vec![insert()])
                .is_err());
            assert_eq!(store.version_index.retained_history_bytes(), weight);
            assert_eq!(store.commit_epoch(), epoch);
            assert_eq!(fs::read(active_wal_path(&path)).unwrap(), wal);
            store
                .durable
                .as_mut()
                .unwrap()
                .set_wal_available_space_override(u64::MAX);
        }
        store
            .commit_mutations(&mut catalog, vec![insert()])
            .unwrap();
        assert_eq!(store.version_index.retained_history_bytes(), 3 * weight);
        let epoch = store.commit_epoch();
        let wal = durable.then(|| fs::read(active_wal_path(&path)).unwrap());
        let error = store
            .commit_mutations(&mut catalog, vec![insert()])
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("retained version history budget exhausted"),
            "{error}"
        );
        assert_eq!(store.commit_epoch(), epoch);
        assert_eq!(store.node_count_for_label(None), 3);
        assert_eq!(old.node_count_for_label(None), 2);
        assert_eq!(store.version_index.retained_history_bytes(), 3 * weight);
        if let Some(wal) = wal {
            assert_eq!(fs::read(active_wal_path(&path)).unwrap(), wal);
        }
        drop(old);
        drop(protection);
        store.checkpoint(&catalog).unwrap();
        assert_eq!(store.version_index.retained_history_bytes(), 0);
        store
            .commit_mutations(&mut catalog, vec![insert()])
            .unwrap();
        let epoch = store.commit_epoch();
        assert_eq!(store.node_count_for_label(None), 4);
        drop(store);
        if durable {
            let mut reopened_catalog = Catalog::default();
            let reopened = GraphStore::open(&path, &mut reopened_catalog).unwrap();
            assert_eq!(reopened.commit_epoch(), epoch);
            assert_eq!(reopened.node_count_for_label(None), 4);
            assert_eq!(reopened.version_index.retained_history_bytes(), 0);
            drop(reopened);
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn version_history_budget_rejects_growth_before_wal_and_recovers_after_unpin() {
    use crate::cow::CowPageWeight;
    use crate::version::{VersionIndex, VersionKey, VersionStamp};
    for durable in [false, true] {
        let path = unique_test_dir("version_history_budget");
        let mut catalog = Catalog::default();
        let mut store = if durable {
            GraphStore::open(&path, &mut catalog).unwrap()
        } else {
            GraphStore::default()
        };
        store
            .create_node(&mut catalog, "Memory", properties([]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let reserve = VersionIndex::default().estimated_bytes();
        let weight =
            VersionKey::GraphNode(NodeId(0)).cow_page_bytes() + std::mem::size_of::<VersionStamp>();
        let limit = reserve + 2 * weight;
        store.version_index = VersionIndex::with_byte_limit(limit);
        let reader = store.snapshot();
        let insert = || GraphMutation::CreateNode {
            label: "Memory".into(),
            properties: properties([]),
        };
        for _ in 0..2 {
            store
                .commit_mutations(&mut catalog, vec![insert()])
                .unwrap();
        }
        assert_eq!(store.version_index.estimated_bytes(), limit);
        let epoch = store.commit_epoch();
        let wal = durable.then(|| fs::read(active_wal_path(&path)).unwrap());
        let error = store
            .commit_mutations(&mut catalog, vec![insert()])
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("version index estimated payload budget exhausted"));
        assert_eq!(store.commit_epoch(), epoch);
        assert_eq!(store.node_count_for_label(None), 3);
        assert_eq!(reader.node_count_for_label(None), 1);
        assert_eq!(store.version_index.estimated_bytes(), limit);
        if let Some(wal) = wal {
            assert_eq!(fs::read(active_wal_path(&path)).unwrap(), wal);
        }
        // A legacy direct commit uses its prepaid Database identity even
        // while the current index is full and an old reader remains pinned.
        store
            .create_node(&mut catalog, "Memory", properties([]))
            .unwrap();
        assert_eq!(store.version_index.estimated_bytes(), limit);
        assert!(store.version_index.stamp(&VersionKey::Database).is_some());
        drop(reader);
        store.checkpoint(&catalog).unwrap();
        assert_eq!(store.version_index.estimated_bytes(), reserve);
        store
            .commit_mutations(&mut catalog, vec![insert()])
            .unwrap();
        assert_eq!(store.node_count_for_label(None), 5);
        // Without an old external pin, the prepared current-epoch baseline
        // bounds history without requiring a checkpoint per commit.
        for _ in 0..8 {
            store
                .commit_mutations(&mut catalog, vec![insert()])
                .unwrap();
            assert!(store.version_index.estimated_bytes() <= limit);
        }
        let epoch = store.commit_epoch();
        drop(store);
        if durable {
            let mut recovered_catalog = Catalog::default();
            let recovered = GraphStore::open(&path, &mut recovered_catalog).unwrap();
            assert_eq!(recovered.node_count_for_label(None), 13);
            assert_eq!(recovered.commit_epoch(), epoch);
            drop(recovered);
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn checkpoint_reclaims_obsolete_live_version_history_without_losing_rows_or_conflicts() {
    use crate::version::VersionKey;
    for durable in [false, true] {
        let path = unique_test_dir("live_version_history");
        let mut catalog = Catalog::default();
        let mut store = if durable {
            GraphStore::open(&path, &mut catalog).unwrap()
        } else {
            GraphStore::default()
        };
        let insert_batch = |store: &mut GraphStore, catalog: &mut Catalog| {
            store
                .commit_mutations(
                    catalog,
                    (0..8)
                        .map(|_| GraphMutation::CreateNode {
                            label: "Memory".into(),
                            properties: properties([("value", Value::Int(0))]),
                        })
                        .collect(),
                )
                .unwrap();
        };
        let protection = store.snapshot();
        insert_batch(&mut store, &mut catalog);
        insert_batch(&mut store, &mut catalog);
        let older = store.snapshot();
        insert_batch(&mut store, &mut catalog);
        let newer = store.snapshot();
        drop(protection);
        store.checkpoint(&catalog).unwrap();
        assert_eq!(store.version_index.len(), 16);
        assert_eq!(
            store.version_index.stamp(&VersionKey::GraphNode(NodeId(0))),
            None
        );
        assert!(store
            .version_index
            .stamp(&VersionKey::GraphNode(NodeId(8)))
            .is_some());
        assert!(older
            .version_index
            .stamp(&VersionKey::GraphNode(NodeId(0)))
            .is_some());
        assert_eq!(older.node_count_for_label(None), 16);
        drop(older);
        store.checkpoint(&catalog).unwrap();
        assert_eq!(store.version_index.len(), 8);
        assert_eq!(newer.node_count_for_label(None), 24);
        drop(newer);
        store.checkpoint(&catalog).unwrap();
        assert!(store.version_index.is_empty());
        assert_eq!(store.node_count_for_label(None), 24);

        // After old history is discarded, new mutations must still stamp
        // conflicts for transactions that start from the retained data.
        let mut stale = store.begin_mutation_transaction(&catalog);
        let update = |value| GraphMutation::SetNodeProperty {
            label: "Memory".into(),
            filter: None,
            property: "value".into(),
            value: Value::Int(value),
        };
        stale
            .stage_mutation_with_limits(update(1), MutationLimits::default())
            .unwrap();
        store
            .commit_mutations(&mut catalog, vec![update(2)])
            .unwrap();
        let epoch = store.commit_epoch;
        assert!(store
            .commit_mutation_transaction_and_relational(
                &mut catalog,
                stale,
                RelationalTransaction::default(),
                MutationLimits::default()
            )
            .unwrap_err()
            .is_retryable_transaction_conflict());
        assert_eq!(store.commit_epoch, epoch);
        store.checkpoint(&catalog).unwrap();
        assert!(store.version_index.is_empty());
        if durable {
            drop(store);
            catalog = Catalog::default();
            store = GraphStore::open(&path, &mut catalog).unwrap();
        }
        assert_eq!(store.node_count_for_label(None), 24);
        for id in 0..24 {
            assert_eq!(
                store.node_owned(NodeId(id)).unwrap().unwrap().properties["value"],
                Value::Int(2)
            );
        }
        drop(store);
        if durable {
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn direct_legacy_commits_invalidate_stale_optimistic_workspaces() {
    for durable in [false, true] {
        for mutation in ["property", "schema", "delete"] {
            let path = unique_test_dir("legacy_version_barrier");
            let mut catalog = Catalog::default();
            let mut store = if durable {
                GraphStore::open(&path, &mut catalog).unwrap()
            } else {
                GraphStore::default()
            };
            let id = store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("value", Value::Int(0))]),
                )
                .unwrap();
            let read_epoch = store.commit_epoch;
            let mut stale = store.begin_mutation_transaction(&catalog);
            stale
                .stage_mutation_with_limits(
                    GraphMutation::SetNodeProperty {
                        label: "Memory".into(),
                        filter: None,
                        property: "value".into(),
                        value: Value::Int(1),
                    },
                    MutationLimits::default(),
                )
                .unwrap();
            match mutation {
                "property" => {
                    store
                        .set_node_property(&mut catalog, "Memory", None, "value", Value::Int(2))
                        .unwrap();
                }
                "schema" => {
                    store.create_node_label(&mut catalog, "Other").unwrap();
                }
                "delete" => {
                    store
                        .delete_nodes(&mut catalog, "Memory", None, false)
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let wal = durable.then(|| fs::read(active_wal_path(&path)).unwrap());
            let error = store
                .commit_mutation_transaction_and_relational(
                    &mut catalog,
                    stale,
                    RelationalTransaction::default(),
                    MutationLimits::default(),
                )
                .unwrap_err();
            assert!(
                error.is_retryable_transaction_conflict(),
                "{mutation}: {error}"
            );
            assert!(matches!(error, HawDBError::TransactionConflict {
                    read_epoch: actual_read, committed_epoch, ref key,
                } if actual_read == read_epoch && committed_epoch == read_epoch + 1 && key == "database"));
            assert_eq!(store.commit_epoch, read_epoch + 1);
            if let Some(wal) = wal {
                assert_eq!(fs::read(active_wal_path(&path)).unwrap(), wal);
                drop(store);
                catalog = Catalog::default();
                store = GraphStore::open(&path, &mut catalog).unwrap();
                assert_eq!(store.commit_epoch, read_epoch + 1);
            }
            if mutation == "delete" {
                assert!(store.node_owned(id).unwrap().is_none());
            } else {
                let node = store.node_owned(id).unwrap().unwrap();
                assert_eq!(
                    node.properties["value"],
                    Value::Int(if mutation == "property" { 2 } else { 0 })
                );
            }
            assert_eq!(catalog.label_id("Other").is_some(), mutation == "schema");
            drop(store);
            if durable {
                fs::remove_dir_all(path).unwrap();
            }
        }
    }
}

#[test]
fn rejected_and_noop_legacy_mutations_do_not_invalidate_a_transaction() {
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    let id = store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("value", Value::Int(i64::MAX))]),
        )
        .unwrap();
    let epoch = store.commit_epoch;
    let mut transaction = store.begin_mutation_transaction(&catalog);
    transaction
        .stage_mutation_with_limits(
            GraphMutation::SetNodeProperty {
                label: "Memory".into(),
                filter: None,
                property: "value".into(),
                value: Value::Int(7),
            },
            MutationLimits::default(),
        )
        .unwrap();
    assert!(store
        .add_int_node_property(&mut catalog, "Memory", None, "value", 1)
        .is_err());
    assert!(store
        .set_node_property(&mut catalog, "Absent", None, "value", Value::Int(1))
        .unwrap()
        .is_empty());
    assert_eq!(store.commit_epoch, epoch);
    store
        .commit_mutation_transaction_and_relational(
            &mut catalog,
            transaction,
            RelationalTransaction::default(),
            MutationLimits::default(),
        )
        .unwrap();
    assert_eq!(
        store.node_owned(id).unwrap().unwrap().properties["value"],
        Value::Int(7)
    );
}

#[test]
fn version_tombstone_checkpoint_waits_for_storage_snapshots_and_transactions() {
    use crate::version::{VersionDisposition, VersionKey};
    for durable in [false, true] {
        let path = unique_test_dir("version_tombstone_pins");
        let mut catalog = Catalog::default();
        let mut store = if durable {
            GraphStore::open(&path, &mut catalog).unwrap()
        } else {
            GraphStore::default()
        };
        let id = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("value", Value::Int(0))]),
            )
            .unwrap();
        let key = VersionKey::GraphNode(id);
        let source = store.snapshot();
        let mut stale = source.begin_mutation_transaction(&catalog);
        stale
            .stage_mutation_with_limits(
                GraphMutation::SetNodeProperty {
                    label: "Memory".into(),
                    filter: None,
                    property: "value".into(),
                    value: Value::Int(1),
                },
                MutationLimits::default(),
            )
            .unwrap();
        store
            .commit_mutations(
                &mut catalog,
                vec![GraphMutation::DeleteNode {
                    label: "Memory".into(),
                    filter: None,
                    detach: false,
                }],
            )
            .unwrap();
        let deleted = store.version_index.stamp(&key).unwrap();
        assert_eq!(deleted.disposition, VersionDisposition::Tombstone);
        store.checkpoint(&catalog).unwrap();
        assert_eq!(store.version_index.stamp(&key), Some(deleted));
        let error = store
            .commit_mutation_transaction_and_relational(
                &mut catalog,
                stale,
                RelationalTransaction::default(),
                MutationLimits::default(),
            )
            .unwrap_err();
        assert!(error.is_retryable_transaction_conflict());
        // A source snapshot can start a transaction later. It must retain
        // protection even after the original transaction has retired.
        let later = source.begin_mutation_transaction(&catalog);
        drop(source);
        store.checkpoint(&catalog).unwrap();
        assert_eq!(store.version_index.stamp(&key), Some(deleted));
        drop(later);
        let equal_epoch = store.snapshot();
        store.checkpoint(&catalog).unwrap();
        assert_eq!(store.version_index.stamp(&key), Some(deleted));
        let next_id = NodeId(store.next_node_id);
        store
            .commit_mutations(
                &mut catalog,
                vec![GraphMutation::CreateNode {
                    label: "Memory".into(),
                    properties: properties([("value", Value::Int(2))]),
                }],
            )
            .unwrap();
        let newer = store.snapshot();
        let live_key = VersionKey::GraphNode(next_id);
        let live = store.version_index.stamp(&live_key).unwrap();
        store.checkpoint(&catalog).unwrap();
        assert_eq!(store.version_index.stamp(&key), None);
        assert_eq!(store.version_index.stamp(&live_key), Some(live));
        // The next commit can compact a tip-pinned baseline, while the
        // precise source captured before that commit remains unchanged.
        assert_eq!(newer.version_index.stamp(&key), None);
        assert_eq!(equal_epoch.version_index.stamp(&key), Some(deleted));
        drop(equal_epoch);
        drop(newer);
        drop(store);
        if durable {
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn version_tombstone_checkpoint_preserves_base_epoch_after_savepoint_restore() {
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    let id = store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("value", Value::Int(0))]),
        )
        .unwrap();
    let key = crate::version::VersionKey::GraphNode(id);
    let mut transaction = store.begin_mutation_transaction(&catalog);
    for value in 1..=2 {
        transaction
            .stage_mutation_with_limits(
                GraphMutation::SetNodeProperty {
                    label: "Memory".into(),
                    filter: None,
                    property: "value".into(),
                    value: Value::Int(value),
                },
                MutationLimits::default(),
            )
            .unwrap();
    }
    let savepoint = transaction.savepoint();
    transaction
        .stage_mutation_with_limits(
            GraphMutation::SetNodeProperty {
                label: "Memory".into(),
                filter: None,
                property: "value".into(),
                value: Value::Int(3),
            },
            MutationLimits::default(),
        )
        .unwrap();
    transaction.restore(savepoint);
    store
        .commit_mutations(
            &mut catalog,
            vec![GraphMutation::DeleteNode {
                label: "Memory".into(),
                filter: None,
                detach: false,
            }],
        )
        .unwrap();
    let deleted = store.version_index.stamp(&key).unwrap();
    store.checkpoint(&catalog).unwrap();
    assert_eq!(store.version_index.stamp(&key), Some(deleted));
    assert!(store
        .commit_mutation_transaction_and_relational(
            &mut catalog,
            transaction,
            RelationalTransaction::default(),
            MutationLimits::default(),
        )
        .unwrap_err()
        .is_retryable_transaction_conflict());
}

#[test]
fn version_tombstone_checkpoint_bounds_unpinned_delete_churn() {
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    for value in 0..32 {
        store
            .commit_mutations(
                &mut catalog,
                vec![GraphMutation::CreateNode {
                    label: "Memory".into(),
                    properties: properties([("value", Value::Int(value))]),
                }],
            )
            .unwrap();
        store
            .commit_mutations(
                &mut catalog,
                vec![GraphMutation::DeleteNode {
                    label: "Memory".into(),
                    filter: None,
                    detach: false,
                }],
            )
            .unwrap();
        assert!(store
            .version_index
            .stamp(&crate::version::VersionKey::GraphNode(NodeId(value as u64)))
            .is_some());
        store.checkpoint(&catalog).unwrap();
        // Only the schema barrier may remain after all nodes were deleted.
        assert!(store.version_index.len() <= 1);
    }
}

#[test]
fn user_snapshot_observes_fatal_single_and_batch_wal_errors_but_allows_recoverable_append() {
    use super::{set_wal_append_failpoint, WalAppendFailure};
    for batch in [false, true] {
        for failure in [
            WalAppendFailure::PartialWrite,
            WalAppendFailure::Rollback,
            WalAppendFailure::Sync,
        ] {
            // Given a snapshot captured before an append to a real durable store.
            let path = unique_test_dir("snapshot_wal_failure");
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let snapshot = store.snapshot();
            let op = WalOp::CreateNode {
                id: NodeId(0),
                label: "Record".into(),
                properties: BTreeMap::new(),
            };
            // When an append reports either a reversible or an uncertain durable outcome.
            set_wal_append_failpoint(failure);
            let result = if batch {
                store.append_durable_wal_batch(std::slice::from_ref(&op))
            } else {
                store.append_durable_wal_single(op.clone())
            };
            assert!(result.is_err());
            let fatal = failure != WalAppendFailure::PartialWrite;
            assert_eq!(
                matches!(result, Err(HawDBError::StorageIntegrity(_))),
                fatal
            );
            // Then old and current views share the fatal state without requiring publication.
            assert_eq!(snapshot.ensure_usable().is_err(), fatal);
            assert_eq!(store.ensure_usable().is_err(), fatal);
            if !fatal {
                store.append_durable_wal_single(op).unwrap();
            }
            drop(snapshot);
            drop(store);
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn residency_facade_preserves_storage_type_identity() {
    assert_eq!(
        TypeId::of::<StorageResidencyReport>(),
        TypeId::of::<hawdb_storage::residency::StorageResidencyReport>()
    );
    assert_eq!(
        TypeId::of::<RelationalRowStorageResidencyReport>(),
        TypeId::of::<hawdb_storage::residency::RelationalRowStorageResidencyReport>()
    );
    assert_eq!(
        TypeId::of::<RelationalIndexStorageResidencyReport>(),
        TypeId::of::<hawdb_storage::relational_index_view::RelationalIndexStorageResidencyReport>()
    );
}

#[test]
fn owned_graph_overlay_preserves_snapshot_checkpoint_and_reopen() {
    let path = unique_test_dir("owned_graph_overlay");
    let open = |catalog: &mut Catalog| {
        GraphStore::open_with_durability_and_replay_config(
            &path,
            catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                residency_mode: StorageResidencyMode::OutOfCore,
                ..WalReplayConfig::default()
            },
        )
        .unwrap()
    };
    let mut catalog = Catalog::default();
    let mut store = open(&mut catalog);
    let mut ids = Vec::new();
    for id in 0..3 {
        ids.push(
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
                .unwrap(),
        );
    }
    let first_rel = store
        .create_relationship(&mut catalog, ids[0], ids[1], "LINKS", BTreeMap::new())
        .unwrap();
    store
        .create_relationship(&mut catalog, ids[0], ids[2], "LINKS", BTreeMap::new())
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    assert!(store.nodes.is_empty());
    assert!(store.relationships.is_empty());

    store
        .set_node_property(&mut catalog, "Memory", None, "revision", Value::Int(2))
        .unwrap();
    store
        .commit_mutations(
            &mut catalog,
            vec![GraphMutation::SetRelationshipProperty {
                source_label: "Memory".to_string(),
                filter: None,
                rel_type: "LINKS".to_string(),
                target_label: "Memory".to_string(),
                target_filter: None,
                rel_filter: None,
                property: "revision".to_string(),
                value: Value::Int(2),
            }],
        )
        .unwrap();
    let filter = |id| PropertyFilter::Eq {
        property: "id".to_string(),
        value: Value::Int(id),
    };
    store
        .delete_nodes(&mut catalog, "Memory", Some(&filter(2)), true)
        .unwrap();
    let added = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
        .unwrap();
    let added_rel = store
        .create_relationship(&mut catalog, ids[0], added, "LINKS", BTreeMap::new())
        .unwrap();
    let expected_nodes = [ids[0], ids[1], added]
        .into_iter()
        .map(|id| store.node_owned(id).unwrap().unwrap())
        .collect::<Vec<_>>();
    let expected_rels = [first_rel, added_rel]
        .into_iter()
        .map(|id| store.relationship_owned(id).unwrap().unwrap())
        .collect::<Vec<_>>();
    let nodes: super::GraphNodeIterator = store.node_records_owned();
    let nodes: hawdb_storage::graph_overlay::GraphNodeIterator = nodes;
    let relationships: super::GraphRelationshipIterator = store.relationship_records_owned();
    let relationships: hawdb_storage::graph_overlay::GraphRelationshipIterator = relationships;

    store
        .set_node_property(
            &mut catalog,
            "Memory",
            Some(&filter(0)),
            "revision",
            Value::Int(3),
        )
        .unwrap();
    store
        .delete_nodes(&mut catalog, "Memory", Some(&filter(1)), true)
        .unwrap();
    assert_eq!(
        nodes.collect::<crate::Result<Vec<_>>>().unwrap(),
        expected_nodes
    );
    assert_eq!(
        relationships.collect::<crate::Result<Vec<_>>>().unwrap(),
        expected_rels
    );

    let expected_nodes = [ids[0], added]
        .into_iter()
        .map(|id| store.node_owned(id).unwrap().unwrap())
        .collect::<Vec<_>>();
    let expected_rels = vec![store.relationship_owned(added_rel).unwrap().unwrap()];
    assert_eq!(
        store
            .node_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected_nodes
    );
    assert_eq!(
        store
            .relationship_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected_rels
    );
    store.checkpoint(&catalog).unwrap();
    drop(store);
    let mut catalog = Catalog::default();
    let reopened = open(&mut catalog);
    assert_eq!(
        reopened
            .node_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected_nodes
    );
    assert_eq!(
        reopened
            .relationship_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected_rels
    );
    drop(reopened);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn background_storage_permit_retains_governor_resources_until_drop_and_unwind() {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot,
    };
    use hawdb_storage::background::{BackgroundWorkAdmission, BackgroundWorkRequest};

    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(1024),
            result_budget_bytes: 0,
            ..RuntimeGovernorConfig::default()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(8192), Some(8192), None, None, None),
        ),
        IoConcurrencyBudget::new(4, 1),
    );
    let admission = super::RuntimeGovernorBackgroundAdmission(governor.clone());
    let request = BackgroundWorkRequest {
        cpu_slots: 1,
        memory_bytes: 512,
        io_slots: 1,
    };
    for unwind in [false, true] {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let permit = admission.try_admit(request).unwrap();
            let snapshot = governor.snapshot();
            assert_eq!(snapshot.active_background_tasks, 1);
            assert_eq!(snapshot.active_cpu_slots, 1);
            assert_eq!(snapshot.active_background_io_slots, 1);
            assert_eq!(snapshot.admitted_memory_bytes, 512);
            assert!(admission.try_admit(request).is_err());
            if unwind {
                panic!("injected background work panic");
            }
            drop(permit);
        }));
        assert_eq!(result.is_err(), unwind);
        let snapshot = governor.snapshot();
        assert_eq!(snapshot.active_background_tasks, 0);
        assert_eq!(snapshot.active_cpu_slots, 0);
        assert_eq!(snapshot.active_background_io_slots, 0);
        assert_eq!(snapshot.admitted_memory_bytes, 0);
    }
    drop(admission.try_admit(request).unwrap());
    assert_eq!(governor.snapshot().completions, 3);
}

#[test]
fn read_snapshot_baseline_releases_history_without_losing_data_or_pins() {
    use crate::version::VersionKey;
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("value", Value::Int(0))]),
        )
        .unwrap();
    let update = |value| GraphMutation::SetNodeProperty {
        label: "Memory".into(),
        filter: None,
        property: "value".into(),
        value: Value::Int(value),
    };
    let mut old_read_target = store.begin_mutation_transaction(&catalog);
    let mut old_precise_target = store.begin_mutation_transaction(&catalog);
    old_read_target
        .stage_mutation_with_limits(update(1), MutationLimits::default())
        .unwrap();
    old_precise_target
        .stage_mutation_with_limits(update(1), MutationLimits::default())
        .unwrap();
    store
        .commit_mutations(
            &mut catalog,
            (0..8)
                .map(|_| GraphMutation::CreateNode {
                    label: "Memory".into(),
                    properties: properties([("value", Value::Int(0))]),
                })
                .collect(),
        )
        .unwrap();
    assert!(store.version_index.len() >= 8);
    let mut precise = store.snapshot();
    assert!(precise
        .version_index
        .shares_storage_with(&store.version_index));
    let mut read = store.snapshot_for_read();
    assert_eq!(read.version_index.len(), 1);
    assert!(!read.version_index.shares_storage_with(&store.version_index));
    assert!(read.nodes.shares_storage_with(&store.nodes));
    let epoch = read.commit_epoch();
    assert_eq!(
        read.version_index
            .stamp(&VersionKey::Database)
            .unwrap()
            .commit_epoch,
        epoch
    );
    let mut read_catalog = catalog.clone();
    let error = read
        .commit_mutation_transaction_and_relational(
            &mut read_catalog,
            old_read_target,
            RelationalTransaction::default(),
            MutationLimits::default(),
        )
        .unwrap_err();
    assert!(error.is_retryable_transaction_conflict());
    assert_eq!(read.commit_epoch(), epoch);
    // The normal workspace still admits the disjoint old writer: only the
    // explicit read-optimized fork sacrifices historical write precision.
    precise
        .commit_mutation_transaction_and_relational(
            &mut catalog.clone(),
            old_precise_target,
            RelationalTransaction::default(),
            MutationLimits::default(),
        )
        .unwrap();
    drop(precise);
    let descendant = read.snapshot_for_read();
    drop(read);
    store
        .commit_mutations(&mut catalog, vec![update(2)])
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    assert!(store.version_index.is_empty());
    assert_eq!(descendant.commit_epoch(), epoch);
    assert_eq!(descendant.version_index.len(), 1);
    for id in 0..9 {
        assert_eq!(
            descendant
                .node_owned(NodeId(id))
                .unwrap()
                .unwrap()
                .properties["value"],
            Value::Int(0)
        );
        assert_eq!(
            store.node_owned(NodeId(id)).unwrap().unwrap().properties["value"],
            Value::Int(2)
        );
    }
    drop(descendant);
    store.checkpoint(&catalog).unwrap();
    assert!(store.version_index.is_empty());
    assert!(GraphStore::default()
        .snapshot_for_read()
        .version_index
        .is_empty());
}

#[test]
fn snapshot_shares_segments_until_the_live_store_mutates_them() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("id", Value::String("memory-a".to_string()))]),
        )
        .unwrap();

    let snapshot = store.snapshot();
    assert!(store.nodes.shares_storage_with(&snapshot.nodes));
    assert!(store
        .relationships
        .shares_storage_with(&snapshot.relationships));

    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("id", Value::String("memory-b".to_string()))]),
        )
        .unwrap();

    assert_eq!(snapshot.nodes.len(), 1);
    assert_eq!(store.nodes.len(), 2);
    assert!(!store.nodes.shares_storage_with(&snapshot.nodes));
    assert!(store
        .relationships
        .shares_storage_with(&snapshot.relationships));
}

#[test]
fn wal_append_handle_is_reused_and_rotated_with_its_generation() {
    let path = unique_test_dir("wal_append_handle_cache");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    let durable = store.durable.as_ref().unwrap();
    assert!(durable.wal_append_file.is_none());
    assert_eq!(durable.wal_append_open_count, 0);

    for id in 1..=2 {
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
            .unwrap();
    }
    let durable = store.durable.as_ref().unwrap();
    assert!(durable.wal_append_file.is_some());
    assert_eq!(durable.wal_append_open_count, 1);
    let checkpoint_source = store.checkpoint_source();
    assert!(checkpoint_source
        .durable
        .as_ref()
        .unwrap()
        .wal_append_file
        .is_none());
    drop(checkpoint_source);

    store.checkpoint(&catalog).unwrap();
    assert!(store.durable.as_ref().unwrap().wal_append_file.is_none());
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
        .unwrap();
    let durable = store.durable.as_ref().unwrap();
    assert!(durable.wal_append_file.is_some());
    assert_eq!(durable.wal_append_open_count, 2);
    drop(store);

    let mut reopened = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(reopened.durable.as_ref().unwrap().wal_append_open_count, 0);
    reopened
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(4))]))
        .unwrap();
    assert_eq!(reopened.durable.as_ref().unwrap().wal_append_open_count, 1);
    drop(reopened);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_free_space_pressure_rejects_before_opening_or_writing_the_wal() {
    let path = unique_test_dir("wal_free_space_pressure");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .durable
        .as_mut()
        .unwrap()
        .set_wal_available_space_override(0);

    let error = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap_err();

    assert!(error.to_string().contains("reasons=free_space_reserve"));
    let durable = store.durable.as_ref().unwrap();
    assert_eq!(durable.wal_bytes, 0);
    assert!(durable.wal_append_file.is_none());
    assert!(!durable.wal_path.exists());
    drop(store);

    let mut reopened_catalog = Catalog::default();
    let reopened = GraphStore::open(&path, &mut reopened_catalog).unwrap();
    assert_eq!(reopened.scan_nodes(None).count(), 0);
    drop(reopened);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_free_space_probe_is_amortized_across_small_appends() {
    let path = unique_test_dir("wal_free_space_probe_watermark");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .durable
        .as_mut()
        .unwrap()
        .set_wal_available_space_override(u64::MAX);

    for id in 1..=2 {
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
            .unwrap();
    }

    assert_eq!(
        store.durable.as_ref().unwrap().wal_free_space_probe_count(),
        1
    );
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_delta_pressure_matches_full_scan_through_mutations_and_reopen() {
    fn verify(store: &GraphStore) {
        assert_eq!(
            store.estimated_delta_resident_bytes(),
            store.estimated_delta_resident_bytes_reference()
        );
    }
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir("checkpoint_delta_pressure");
        let config = WalReplayConfig {
            residency_mode: mode,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            config,
        )
        .unwrap();
        store
            .create_property_index(&mut catalog, "Memory", "id")
            .unwrap();
        store
            .create_composite_property_index(&mut catalog, "Memory", &["id".into(), "body".into()])
            .unwrap();
        store
            .create_full_text_property_index(&mut catalog, "Memory", "body")
            .unwrap();
        let mut ids = Vec::new();
        for id in 0..1057 {
            ids.push(
                store
                    .create_node(
                        &mut catalog,
                        "Memory",
                        properties([
                            ("id", Value::Int(id)),
                            ("body", Value::String(format!("alpha beta {id}"))),
                            (
                                "detail",
                                Value::Map(BTreeMap::from([(
                                    "flags".into(),
                                    Value::List(vec![
                                        Value::Null,
                                        Value::Bool(true),
                                        Value::List(vec![Value::String("雪".repeat(8))]),
                                    ]),
                                )])),
                            ),
                        ]),
                    )
                    .unwrap(),
            );
            verify(&store);
        }
        for target in ids.iter().take(130).skip(1) {
            store
                .create_relationship(
                    &mut catalog,
                    ids[0],
                    *target,
                    "LINK",
                    properties([("rank", Value::Int(1))]),
                )
                .unwrap();
            verify(&store);
        }
        let old = store.snapshot();
        let old_pressure = old.estimated_delta_resident_bytes();
        for id in 0..17 {
            store
                .set_node_property(
                    &mut catalog,
                    "Memory",
                    Some(&PropertyFilter::Eq {
                        property: "id".into(),
                        value: Value::Int(id),
                    }),
                    "body",
                    Value::String(format!("gamma beta {id}")),
                )
                .unwrap();
            verify(&store);
            assert_eq!(old.estimated_delta_resident_bytes(), old_pressure);
        }
        store
            .set_relationship_property(
                &mut catalog,
                RelationshipPropertyUpdate {
                    source_label: "Memory".into(),
                    filter: None,
                    rel_type: "LINK".into(),
                    target_label: "Memory".into(),
                    target_filter: None,
                    rel_filter: None,
                    property: "rank".into(),
                    value: Value::Int(2),
                },
            )
            .unwrap();
        verify(&store);
        store
            .delete_nodes(
                &mut catalog,
                "Memory",
                Some(&PropertyFilter::Eq {
                    property: "id".into(),
                    value: Value::Int(1),
                }),
                true,
            )
            .unwrap();
        verify(&store);
        store.checkpoint(&catalog).unwrap();
        verify(&store);
        if mode == StorageResidencyMode::OutOfCore {
            assert_eq!(store.checkpoint_debt_snapshot().unwrap().delta_bytes, 0);
        }
        store
            .set_node_property(
                &mut catalog,
                "Memory",
                Some(&PropertyFilter::Eq {
                    property: "id".into(),
                    value: Value::Int(2),
                }),
                "body",
                Value::String("suffix gamma".into()),
            )
            .unwrap();
        verify(&store);
        let expected_nodes = store
            .node_records_owned()
            .collect::<hawdb_core::Result<Vec<_>>>()
            .unwrap();
        let expected_relationships = store
            .relationship_records_owned()
            .collect::<hawdb_core::Result<Vec<_>>>()
            .unwrap();
        drop(old);
        drop(store);
        let reopened = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            config,
        )
        .unwrap();
        verify(&reopened);
        assert_eq!(
            reopened
                .node_records_owned()
                .collect::<hawdb_core::Result<Vec<_>>>()
                .unwrap(),
            expected_nodes
        );
        assert_eq!(
            reopened
                .relationship_records_owned()
                .collect::<hawdb_core::Result<Vec<_>>>()
                .unwrap(),
            expected_relationships
        );
        if mode == StorageResidencyMode::OutOfCore {
            let debt = reopened.checkpoint_debt_snapshot().unwrap();
            assert_eq!(
                debt.delta_bytes,
                reopened.estimated_delta_resident_bytes_reference()
            );
            assert!(debt.delta_bytes > 0);
        }
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn durable_append_evaluates_delta_and_integrity_pressure_before_wal_write() {
    let path = unique_test_dir("wal_full_pressure_signals");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            max_out_of_core_delta_bytes: None,
            ..WalReplayConfig::default()
        },
    )
    .unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    store.max_out_of_core_delta_bytes = Some(1);
    let wal_bytes = store.durable.as_ref().unwrap().wal_bytes;

    let rejected_op = WalOp::CreateNode {
        id: NodeId(1),
        label: "Memory".to_string(),
        properties: properties([("id", Value::Int(2))]),
    };
    let error = store
        .append_durable_wal_single(rejected_op.clone())
        .unwrap_err();
    assert!(
        error.to_string().contains("reasons=delta_hard_limit"),
        "{error}"
    );
    assert_eq!(store.durable.as_ref().unwrap().wal_bytes, wal_bytes);

    store.max_out_of_core_delta_bytes = None;
    store
        .post_wal_apply_poisoned
        .store(true, std::sync::atomic::Ordering::Release);
    let error = store.append_durable_wal_single(rejected_op).unwrap_err();
    assert!(
        error.to_string().contains("reasons=integrity_poisoned"),
        "{error}"
    );
    assert_eq!(store.durable.as_ref().unwrap().wal_bytes, wal_bytes);

    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn graph_and_relational_state_share_wal_epoch_and_checkpoint_publication() {
    let path = unique_test_dir("unified_relational_commit");
    let backup = unique_test_dir("unified_relational_backup");
    let restored = unique_test_dir("unified_relational_restored");
    let table = RelationalTableSchema {
        materialized_view: None,
        name: "messages".to_string(),
        columns: vec![
            RelationalColumnSchema {
                name: "id".to_string(),
                scalar_type: RelationalScalarType::Text,
                nullable: false,
                default: None,
            },
            RelationalColumnSchema {
                name: "body".to_string(),
                scalar_type: RelationalScalarType::Text,
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    };

    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).expect("open durable store");
        store
            .commit_mutations_and_relational(
                &mut catalog,
                vec![GraphMutation::CreateNode {
                    label: "Marker".to_string(),
                    properties: properties([("id", Value::String("graph-1".to_string()))]),
                }],
                RelationalTransaction {
                    writes: vec![RelationalWrite::CreateTable(table)],
                },
                MutationLimits::default(),
            )
            .expect("commit graph and relational schema");
        assert_eq!(store.commit_epoch(), 1);
        assert_eq!(store.nodes.len(), 1);
        assert!(store.relational_state().table_schema("messages").is_some());
    }

    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).expect("replay unified WAL");
        assert_eq!(store.commit_epoch(), 1);
        assert_eq!(store.nodes.len(), 1);
        assert!(store.relational_state().table_schema("messages").is_some());

        store
            .commit_mutations_and_relational(
                &mut catalog,
                vec![GraphMutation::CreateNode {
                    label: "Marker".to_string(),
                    properties: properties([("id", Value::String("graph-2".to_string()))]),
                }],
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "messages".to_string(),
                        rows: vec![RelationalRow::new(vec![
                            RelationalValue::Text("message-1".to_string()),
                            RelationalValue::Text("payload".repeat(1_024)),
                        ])],
                        mode: RelationalInsertMode::Error,
                    }],
                },
                MutationLimits::default(),
            )
            .expect("commit graph and relational row");
        assert_eq!(store.commit_epoch(), 2);
        store.checkpoint(&catalog).expect("publish checkpoint");
        // Row-page and overflow roots carry rows without a full-row file (#1737).
        assert!(!path.join("relational.1.hawdb").exists());
        assert_eq!(
            store
                .relational_state()
                .file_backed_overflow_segment_count(),
            1
        );
        store
            .backup_to(&catalog, &backup)
            .expect("back up relational checkpoint");
        assert!(!backup.join("relational.2.hawdb").exists());
    }

    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).expect("load unified checkpoint");
        assert_eq!(store.commit_epoch(), 2);
        assert_eq!(store.nodes.len(), 2);
        assert_eq!(store.relational_state().row_count("messages"), 1);
        assert_eq!(
            store
                .relational_state()
                .file_backed_overflow_segment_count(),
            1
        );
        let hydrated = store
            .relational_state()
            .hydrate_row(
                "messages",
                &RelationalKey(vec![RelationalValue::Text("message-1".to_string())]),
                &mut RelationalHydrationBudget::default(),
            )
            .expect("hydrate file-backed row")
            .expect("message row");
        assert_eq!(
            hydrated.values()[1],
            RelationalValue::Text("payload".repeat(1_024))
        );
    }

    restore_storage_backup(&backup, &restored).expect("restore unified backup");
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&restored, &mut catalog).expect("open restored checkpoint");
        assert_eq!(store.commit_epoch(), 2);
        assert_eq!(store.nodes.len(), 2);
        assert_eq!(store.relational_state().row_count("messages"), 1);
        assert_eq!(
            store
                .relational_state()
                .file_backed_overflow_segment_count(),
            1
        );
    }

    fs::remove_dir_all(path).expect("remove test store");
    fs::remove_dir_all(backup).expect("remove backup");
    fs::remove_dir_all(restored).expect("remove restored store");
}

#[test]
fn mixed_commit_recovers_both_states_after_post_wal_apply_failure() {
    let path = unique_test_dir("unified_relational_apply_failure");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).expect("open durable store");
        set_wal_apply_failpoint(Some(0));
        let error = store
            .commit_mutations_and_relational(
                &mut catalog,
                vec![GraphMutation::CreateNode {
                    label: "Marker".to_string(),
                    properties: properties([("id", Value::String("graph-1".to_string()))]),
                }],
                RelationalTransaction {
                    writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                        materialized_view: None,
                        name: "messages".to_string(),
                        columns: vec![RelationalColumnSchema {
                            name: "id".to_string(),
                            scalar_type: RelationalScalarType::Text,
                            nullable: false,
                            default: None,
                        }],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: Vec::new(),
                    })],
                },
                MutationLimits::default(),
            )
            .expect_err("injected apply failure must poison the handle");
        set_wal_apply_failpoint(None);
        assert!(error.to_string().contains("injected failure"));
        assert!(store.post_wal_apply_poisoned());
        assert_eq!(store.nodes.len(), 0);
        assert!(store.relational_state().is_empty());
    }

    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).expect("recover canonical WAL batch");
        assert_eq!(store.commit_epoch(), 1);
        assert_eq!(store.nodes.len(), 1);
        assert!(store.relational_state().table_schema("messages").is_some());
    }

    fs::remove_dir_all(path).expect("remove test store");
}

#[test]
fn relational_checkpoint_corruption_fails_scrub_and_reopen() {
    let path = unique_test_dir("relational_checkpoint_corruption");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).expect("open durable store");
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                        materialized_view: None,
                        name: "messages".to_string(),
                        columns: vec![RelationalColumnSchema {
                            name: "id".to_string(),
                            scalar_type: RelationalScalarType::Text,
                            nullable: false,
                            default: None,
                        }],
                        primary_key: vec!["id".to_string()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: Vec::new(),
                    })],
                },
            )
            .expect("create relational table");
        // Exercise verification of the explicit legacy downgrade file.
        store.set_legacy_relational_checkpoint_file_enabled(true);
        store.checkpoint(&catalog).expect("publish checkpoint");

        let relational_path = path.join("relational.1.hawdb");
        let mut bytes = fs::read(&relational_path).expect("read relational checkpoint");
        *bytes.last_mut().expect("relational checkpoint payload") ^= 0xff;
        fs::write(&relational_path, bytes).expect("corrupt relational checkpoint");

        let error = store
            .scrub_storage()
            .expect_err("scrub must reject relational corruption");
        assert!(error.to_string().contains("relational checkpoint"));
    }

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog)
        .expect_err("reopen must reject relational corruption");
    assert!(error.to_string().contains("relational checkpoint"));
    fs::remove_dir_all(path).expect("remove test store");
}

#[test]
fn active_snapshot_detaches_only_the_mutated_adjacency_posting_list() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source_a = store
        .create_node(&mut catalog, "Source", BTreeMap::new())
        .unwrap();
    let source_b = store
        .create_node(&mut catalog, "Source", BTreeMap::new())
        .unwrap();
    let target_a = store
        .create_node(&mut catalog, "Target", BTreeMap::new())
        .unwrap();
    let target_b = store
        .create_node(&mut catalog, "Target", BTreeMap::new())
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source_a,
            target_a,
            "LINKS_TO",
            BTreeMap::new(),
        )
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source_b,
            target_a,
            "LINKS_TO",
            BTreeMap::new(),
        )
        .unwrap();
    let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
    let snapshot = store.snapshot();

    assert!(store
        .outgoing
        .get(&(source_a, rel_type))
        .unwrap()
        .shares_pivot_with(snapshot.outgoing.get(&(source_a, rel_type)).unwrap()));
    assert!(store
        .outgoing
        .get(&(source_b, rel_type))
        .unwrap()
        .shares_pivot_with(snapshot.outgoing.get(&(source_b, rel_type)).unwrap()));

    store
        .create_relationship(
            &mut catalog,
            source_a,
            target_b,
            "LINKS_TO",
            BTreeMap::new(),
        )
        .unwrap();

    assert!(!store
        .outgoing
        .get(&(source_a, rel_type))
        .unwrap()
        .shares_pivot_with(snapshot.outgoing.get(&(source_a, rel_type)).unwrap()));
    assert!(store
        .outgoing
        .get(&(source_b, rel_type))
        .unwrap()
        .shares_pivot_with(snapshot.outgoing.get(&(source_b, rel_type)).unwrap()));
    assert_eq!(store.outgoing_relationships(source_a, rel_type).count(), 2);
    assert_eq!(
        snapshot.outgoing_relationships(source_a, rel_type).count(),
        1
    );
}

#[test]
fn active_snapshot_buffers_dense_adjacency_mutation_in_a_mini_delta() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Source", BTreeMap::new())
        .unwrap();
    let mut targets = Vec::new();
    for _ in 0..=DENSE_ADJACENCY_DEGREE_THRESHOLD {
        targets.push(
            store
                .create_node(&mut catalog, "Target", BTreeMap::new())
                .unwrap(),
        );
    }
    for target in targets.iter().take(DENSE_ADJACENCY_DEGREE_THRESHOLD) {
        store
            .create_relationship(&mut catalog, source, *target, "LINKS_TO", BTreeMap::new())
            .unwrap();
    }
    let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
    let snapshot = store.snapshot();

    store
        .create_relationship(
            &mut catalog,
            source,
            targets[DENSE_ADJACENCY_DEGREE_THRESHOLD],
            "LINKS_TO",
            BTreeMap::new(),
        )
        .unwrap();

    let live_posting = store.outgoing.get(&(source, rel_type)).unwrap();
    let snapshot_posting = snapshot.outgoing.get(&(source, rel_type)).unwrap();
    assert!(live_posting.shares_pivot_with(snapshot_posting));
    assert_eq!(live_posting.mini_delta_len(), 1);
    assert_eq!(snapshot_posting.mini_delta_len(), 0);
    assert_eq!(
        store.outgoing_relationships(source, rel_type).count(),
        DENSE_ADJACENCY_DEGREE_THRESHOLD + 1
    );
    assert_eq!(
        snapshot.outgoing_relationships(source, rel_type).count(),
        DENSE_ADJACENCY_DEGREE_THRESHOLD
    );
}

#[test]
fn bounded_adjacency_consolidation_preserves_snapshot_and_graph_epoch() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Source", BTreeMap::new())
        .unwrap();
    let target_count = DENSE_ADJACENCY_DEGREE_THRESHOLD
        + hawdb_storage::adjacency::ADJACENCY_DELTA_CONSOLIDATION_ENTRIES;
    let targets = (0..target_count)
        .map(|_| {
            store
                .create_node(&mut catalog, "Target", BTreeMap::new())
                .unwrap()
        })
        .collect::<Vec<_>>();
    for target in targets.iter().take(DENSE_ADJACENCY_DEGREE_THRESHOLD) {
        store
            .create_relationship(&mut catalog, source, *target, "LINKS_TO", BTreeMap::new())
            .unwrap();
    }
    let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
    let snapshot = store.snapshot();
    for target in targets.iter().skip(DENSE_ADJACENCY_DEGREE_THRESHOLD) {
        store
            .create_relationship(&mut catalog, source, *target, "LINKS_TO", BTreeMap::new())
            .unwrap();
    }
    let commit_epoch = store.commit_epoch();
    let plan = store.adjacency_consolidation_plan();
    assert_eq!(plan.group_count, 1);
    assert_eq!(
        plan.delta_entry_count,
        hawdb_storage::adjacency::ADJACENCY_DELTA_CONSOLIDATION_ENTRIES
    );

    let deferred =
        store.consolidate_bounded_adjacency_deltas(plan.estimated_entries.saturating_sub(1));
    assert_eq!(deferred.consolidated_group_count, 0);
    assert_eq!(deferred.remaining, plan);

    let report = store.consolidate_bounded_adjacency_deltas(plan.estimated_entries);
    assert_eq!(report.planned, plan);
    assert_eq!(report.consolidated_group_count, 1);
    assert_eq!(
        report.consolidated_delta_entry_count,
        plan.delta_entry_count
    );
    assert_eq!(
        report.consolidated_estimated_entries,
        plan.estimated_entries
    );
    assert_eq!(report.remaining, AdjacencyConsolidationPlan::default());
    assert_eq!(store.commit_epoch(), commit_epoch);
    assert_eq!(
        store.outgoing_relationships(source, rel_type).count(),
        target_count
    );
    assert_eq!(
        snapshot.outgoing_relationships(source, rel_type).count(),
        DENSE_ADJACENCY_DEGREE_THRESHOLD
    );
}

#[test]
fn active_snapshot_detaches_only_the_mutated_property_posting_list() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_property_index(&mut catalog, "Memory", "topic")
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("topic", Value::String("storage".to_string()))]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("topic", Value::String("retrieval".to_string()))]),
        )
        .unwrap();
    let label_id = catalog.label_id("Memory").unwrap();
    let storage_key = (
        label_id,
        "topic".to_string(),
        Value::String("storage".to_string()),
    );
    let retrieval_key = (
        label_id,
        "topic".to_string(),
        Value::String("retrieval".to_string()),
    );
    let snapshot = store.snapshot();

    assert!(store
        .property_index
        .get(&storage_key)
        .unwrap()
        .shares_storage_with(snapshot.property_index.get(&storage_key).unwrap()));
    assert!(store
        .property_index
        .get(&retrieval_key)
        .unwrap()
        .shares_storage_with(snapshot.property_index.get(&retrieval_key).unwrap()));

    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("topic", Value::String("storage".to_string()))]),
        )
        .unwrap();

    assert!(!store
        .property_index
        .get(&storage_key)
        .unwrap()
        .shares_storage_with(snapshot.property_index.get(&storage_key).unwrap()));
    assert!(store
        .property_index
        .get(&retrieval_key)
        .unwrap()
        .shares_storage_with(snapshot.property_index.get(&retrieval_key).unwrap()));
    assert_eq!(
        store
            .seek_nodes_by_property(label_id, "topic", &Value::String("storage".to_string()),)
            .count(),
        2
    );
    assert_eq!(
        snapshot
            .seek_nodes_by_property(label_id, "topic", &Value::String("storage".to_string()),)
            .count(),
        1
    );
}

#[test]
fn active_snapshot_detaches_only_the_mutated_map_page() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for id in 0..1_100 {
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
            .unwrap();
    }
    let snapshot = store.snapshot();
    let segment_count = store.nodes.segment_count();
    assert!(segment_count >= 3);
    assert_eq!(
        store.nodes.shared_segment_count_with(&snapshot.nodes),
        segment_count
    );

    store.apply_set_node_property(
        &catalog,
        NodeId(0),
        "title".to_string(),
        Value::String("updated".to_string()),
    );

    assert_eq!(store.nodes.segment_count(), segment_count);
    assert_eq!(
        store.nodes.shared_segment_count_with(&snapshot.nodes),
        segment_count - 1
    );
    assert_eq!(
        snapshot
            .nodes
            .get(&NodeId(0))
            .unwrap()
            .properties
            .get("title"),
        None
    );
    assert_eq!(
        store.nodes.get(&NodeId(0)).unwrap().properties.get("title"),
        Some(&Value::String("updated".to_string()))
    );
}

#[test]
fn large_records_split_cow_pages_by_estimated_bytes() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for id in 0..8 {
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
            .unwrap();
    }
    let snapshot = store.snapshot();
    assert_eq!(store.nodes.segment_count(), 1);

    store.apply_set_node_property(
        &catalog,
        NodeId(0),
        "body".to_string(),
        Value::String("x".repeat(COW_MAP_TARGET_SEGMENT_BYTES * 2)),
    );

    assert_eq!(snapshot.nodes.segment_count(), 1);
    assert!(store.nodes.segment_count() >= 2);
    assert_eq!(store.nodes.shared_segment_count_with(&snapshot.nodes), 0);
    assert!(!snapshot
        .nodes
        .get(&NodeId(0))
        .unwrap()
        .properties
        .contains_key("body"));
}

#[test]
fn replays_relationships_from_wal_and_rebuilds_adjacency() {
    let path = unique_test_dir("rel_wal");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "RELATES_TO",
                properties([("weight", Value::Int(7))]),
            )
            .unwrap();
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let rel_type = catalog.rel_type_id("RELATES_TO").unwrap();
        let rels = store
            .outgoing_relationships(NodeId(0), rel_type)
            .collect::<Vec<_>>();
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].target, NodeId(1));
        assert_eq!(rels[0].properties.get("weight"), Some(&Value::Int(7)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn backup_restore_publishes_one_verified_generation() {
    let path = unique_test_dir("backup_source");
    let backup = unique_test_dir("backup_image");
    let restored = unique_test_dir("backup_restored");
    let content = "backup-property-spill-".repeat(8_192);
    let report = {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let source = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("id", Value::String("memory-1".to_string())),
                    ("content", Value::String(content.clone())),
                ]),
            )
            .unwrap();
        let target = store
            .create_node(
                &mut catalog,
                "Entity",
                properties([("id", Value::String("entity-1".to_string()))]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "MENTIONS",
                properties([("weight", Value::Int(7))]),
            )
            .unwrap();
        store.backup_to(&catalog, &backup).unwrap()
    };
    assert!(report.generation > 0);
    assert_eq!(report.file_count, 26);
    assert!(backup
        .join(hawdb_storage::append_table::append_generation_manifest_file(report.generation))
        .exists());
    assert!(backup
        .join(hawdb_storage::canonical::canonical_segment_descriptor_page_file(report.generation))
        .exists());
    assert!(backup
        .join(hawdb_storage::canonical::canonical_segment_descriptor_root_file(report.generation))
        .exists());
    assert!(backup
        .join(
            hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(
                report.generation
            )
        )
        .exists());
    assert!(backup
        .join(
            hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(
                report.generation
            )
        )
        .exists());
    assert!(backup
        .join(
            hawdb_storage::property_projection::property_projection_descriptor_page_file(
                report.generation
            )
        )
        .exists());
    assert!(backup
        .join(
            hawdb_storage::property_projection::property_projection_descriptor_root_file(
                report.generation
            )
        )
        .exists());
    assert!(backup
        .join(hawdb_storage::property_spill::property_spill_descriptor_page_file(report.generation))
        .exists());
    assert!(backup
        .join(hawdb_storage::property_spill::property_spill_descriptor_root_file(report.generation))
        .exists());

    let restore = restore_storage_backup(&backup, &restored).unwrap();
    assert_eq!(restore.generation, report.generation);
    assert_eq!(restore.manifest_checksum, report.manifest_checksum);
    let mut catalog = Catalog::default();
    let store = GraphStore::open(&restored, &mut catalog).unwrap();
    assert_eq!(store.scan_nodes(None).count(), 2);
    assert_eq!(store.scan_relationships(None).count(), 1);
    assert_eq!(
        store.node(NodeId(0)).unwrap().properties["content"],
        Value::String(content)
    );
    assert_eq!(
        store.relationship(RelId(0)).unwrap().properties["weight"],
        Value::Int(7)
    );

    std::fs::remove_dir_all(path).unwrap();
    std::fs::remove_dir_all(backup).unwrap();
    drop(store);
    std::fs::remove_dir_all(restored).unwrap();
}

#[test]
fn restore_rejects_corrupt_backup_before_destination_publication() {
    let path = unique_test_dir("backup_corrupt_source");
    let backup = unique_test_dir("backup_corrupt_image");
    let restored = unique_test_dir("backup_corrupt_restored");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("id", Value::String("memory-1".to_string()))]),
            )
            .unwrap();
        store.backup_to(&catalog, &backup).unwrap();
    }
    let checkpoint = active_checkpoint_path(&backup);
    let mut bytes = std::fs::read(&checkpoint).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(checkpoint, bytes).unwrap();

    let error = restore_storage_backup(&backup, &restored).unwrap_err();
    assert!(error.to_string().contains("verification failed"));
    assert!(!restored.exists());

    std::fs::remove_dir_all(path).unwrap();
    std::fs::remove_dir_all(backup).unwrap();
}

#[test]
fn backup_and_restore_never_replace_existing_destinations() {
    let path = unique_test_dir("backup_existing_source");
    let backup = unique_test_dir("backup_existing_image");
    let restored = unique_test_dir("backup_existing_restored");
    std::fs::create_dir_all(&backup).unwrap();
    std::fs::create_dir_all(&restored).unwrap();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    let backup_error = store.backup_to(&catalog, &backup).unwrap_err();
    assert!(
        backup_error.to_string().contains("already exists"),
        "unexpected backup error: {backup_error}"
    );

    std::fs::remove_dir_all(&backup).unwrap();
    store.backup_to(&catalog, &backup).unwrap();
    let restore_error = restore_storage_backup(&backup, &restored).unwrap_err();
    assert!(restore_error.to_string().contains("already exists"));

    drop(store);
    std::fs::remove_dir_all(path).unwrap();
    std::fs::remove_dir_all(backup).unwrap();
    std::fs::remove_dir_all(restored).unwrap();
}

#[test]
fn checkpoint_segment_scan_rejects_a_manifest_behind_the_graph_snapshot() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let manifest = ScanSegmentManifest::new(0, Vec::new()).unwrap();

    assert!(matches!(
        store.plan_checkpoint_segment_scan(Some(&manifest), &ScanPredicate::True),
        ScanSegmentAccessPlan::Read(_)
    ));
    store
        .create_node(
            &mut catalog,
            "Source",
            properties([("id", Value::String("source-1".to_string()))]),
        )
        .unwrap();

    assert!(matches!(
        store.plan_checkpoint_segment_scan(Some(&manifest), &ScanPredicate::True),
        ScanSegmentAccessPlan::Fallback(ScanSegmentFallback::SnapshotEpochMismatch {
            reader_epoch: 1,
            manifest_epoch: 0,
        })
    ));
    assert!(matches!(
        store.plan_checkpoint_segment_scan(None, &ScanPredicate::True),
        ScanSegmentAccessPlan::Fallback(ScanSegmentFallback::NoManifest)
    ));
}

#[test]
fn checkpoint_publishes_source_scan_and_wal_mutation_invalidates_it() {
    let path = unique_test_dir("source_scan_checkpoint");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Source",
                properties([
                    ("id", Value::String("source-a".to_string())),
                    ("space_id", Value::String("alpha".to_string())),
                ]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        assert!(matches!(
            store.plan_published_source_scan(&ScanPredicate::Eq {
                property: "space_id".to_string(),
                value: Value::String("alpha".to_string()),
            }),
            ScanSegmentAccessPlan::Read(_)
        ));
        let SourceScanCandidateRead::Rows { rows, report, .. } = store
            .read_published_source_scan_candidates(
                &ScanPredicate::Eq {
                    property: "space_id".to_string(),
                    value: Value::String("alpha".to_string()),
                },
                NonZeroUsize::new(2).unwrap(),
                NonZeroU64::new(1024).unwrap(),
                NonZeroU64::new(1024).unwrap(),
            )
            .unwrap()
        else {
            panic!("expected source scan payload read");
        };
        assert_eq!(report.range_count, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].properties["id"],
            Value::String("source-a".to_string())
        );
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("id", Value::String("memory-a".to_string()))]),
            )
            .unwrap();
        assert!(matches!(
            store.plan_published_source_scan(&ScanPredicate::True),
            ScanSegmentAccessPlan::Fallback(ScanSegmentFallback::SnapshotEpochMismatch { .. })
        ));
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        assert!(matches!(
            store.plan_published_source_scan(&ScanPredicate::True),
            ScanSegmentAccessPlan::Fallback(ScanSegmentFallback::SnapshotEpochMismatch { .. })
        ));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_publishes_digest_bound_canonical_segments() {
    let path = unique_test_dir("canonical_segment_checkpoint");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let source = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("id", Value::String("memory-1".to_string()))]),
            )
            .unwrap();
        let target = store
            .create_node(
                &mut catalog,
                "Entity",
                properties([("id", Value::String("entity-1".to_string()))]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "MENTIONS",
                properties([("weight", Value::Int(5))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let manifest = store.canonical_segment_manifest().unwrap();
        assert_eq!(manifest.node_count, 2);
        assert_eq!(manifest.relationship_count, 1);
        assert_eq!(
            store.canonical_node_from_segments(source).unwrap(),
            store.node(source).cloned()
        );
        assert_eq!(
            store
                .canonical_relationship_from_segments(RelId(0))
                .unwrap(),
            store.relationship(RelId(0)).cloned()
        );
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        assert_eq!(store.canonical_segment_manifest().unwrap().node_count, 2);
        assert_eq!(
            store.canonical_node_from_segments(NodeId(1)).unwrap(),
            store.node(NodeId(1)).cloned()
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn detached_snapshots_preserve_live_cache_and_captured_manifest_observations() {
    let path = unique_test_dir("snapshot_residency_observation");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("body", Value::String("snapshot payload".into()))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                residency_mode: StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
        let before = store.storage_residency_report();
        let snapshot = store.snapshot_for_read();
        let descendant = snapshot.snapshot_for_read();
        assert!(snapshot.durable.is_none());
        assert!(descendant.durable.is_none());
        assert!(descendant.storage_recovery_report().durable);
        for read in [&snapshot, &descendant] {
            let residency = read.storage_residency_report();
            assert_eq!(
                residency.segment_cache_capacity_bytes,
                before.segment_cache_capacity_bytes
            );
            assert_eq!(
                residency.graph_manifest_open_budget_bytes,
                before.graph_manifest_open_budget_bytes
            );
            assert_eq!(
                residency.graph_manifest_encoded_bytes,
                before.graph_manifest_encoded_bytes
            );
            assert_eq!(residency.canonical_generation, before.canonical_generation);
        }
        let cache_before = descendant.segment_cache_snapshot().unwrap();
        assert!(cache_before.capacity_bytes > 0);
        drop(store);
        drop(snapshot);
        assert!(descendant.node_owned(NodeId(0)).unwrap().is_some());
        let after = descendant.segment_cache_snapshot().unwrap();
        assert!(
            after.hit_count + after.miss_count > cache_before.hit_count + cache_before.miss_count
        );
        assert_eq!(
            descendant
                .storage_residency_report()
                .segment_cache_hit_count,
            after.hit_count
        );
        assert_eq!(
            descendant
                .storage_residency_report()
                .segment_cache_miss_count,
            after.miss_count
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn graph_manifest_open_budget_is_aggregate_across_published_roots() {
    let path = unique_test_dir("graph_manifest_open_budget");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let source = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Entity", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }

    let canonical_manifest_bytes = fs::metadata(path.join(canonical_manifest_generation_file(1)))
        .unwrap()
        .len();
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let residency = store.storage_residency_report();
        assert_eq!(
            residency.graph_manifest_open_budget_bytes,
            hawdb_storage::config::DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES
        );
        assert!(residency.graph_manifest_encoded_bytes > canonical_manifest_bytes);
    }
    let replay_config = WalReplayConfig {
        max_graph_manifest_open_bytes: canonical_manifest_bytes,
        ..WalReplayConfig::default()
    };
    let mut catalog = Catalog::default();
    let error = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay_config,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("aggregate encoded graph manifest bytes during open"),
        "unexpected graph manifest admission error: {error}"
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn zero_graph_manifest_open_budget_is_rejected_before_initialization() {
    let path = unique_test_dir("zero_graph_manifest_open_budget");
    let replay_config = WalReplayConfig {
        max_graph_manifest_open_bytes: 0,
        ..WalReplayConfig::default()
    };
    let mut catalog = Catalog::default();
    let error = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay_config,
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("max_graph_manifest_open_bytes must be non-zero"));
    assert!(!path.exists());
}

#[test]
fn checkpoint_uses_compact_adjacency_root_and_enforces_manifest_budget_before_selection() {
    let path = unique_test_dir("checkpoint_graph_manifest_budget");
    let replay_config = WalReplayConfig {
        max_graph_manifest_open_bytes: 64 * 1024,
        ..WalReplayConfig::default()
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay_config,
    )
    .unwrap();
    store
        .commit_mutations(
            &mut catalog,
            vec![GraphMutation::MergeConnectedNodes(ConnectedNodesCreate {
                source_label: "Memory".to_string(),
                source_properties: properties([("id", Value::String("source-0".to_string()))]),
                rel_type: "MENTIONS".to_string(),
                rel_properties: BTreeMap::new(),
                target_label: "Entity".to_string(),
                target_properties: properties([("id", Value::String("target-0".to_string()))]),
            })],
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 1);

    let mutations = (1..=512)
        .map(|ordinal| {
            GraphMutation::MergeConnectedNodes(ConnectedNodesCreate {
                source_label: "Memory".to_string(),
                source_properties: properties([("id", Value::String(format!("source-{ordinal}")))]),
                rel_type: "MENTIONS".to_string(),
                rel_properties: BTreeMap::new(),
                target_label: "Entity".to_string(),
                target_properties: properties([("id", Value::String(format!("target-{ordinal}")))]),
            })
        })
        .collect();
    store.commit_mutations(&mut catalog, mutations).unwrap();
    store.checkpoint(&catalog).unwrap();
    assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 2);
    assert!(
        store
            .durable
            .as_ref()
            .unwrap()
            .graph_manifest_encoded_bytes()
            <= replay_config.max_graph_manifest_open_bytes
    );

    store
        .commit_mutations(
            &mut catalog,
            vec![GraphMutation::MergeConnectedNodes(ConnectedNodesCreate {
                source_label: "Memory".to_string(),
                source_properties: properties([("id", Value::String("source-513".to_string()))]),
                rel_type: "MENTIONS".to_string(),
                rel_properties: BTreeMap::new(),
                target_label: "Entity".to_string(),
                target_properties: properties([("id", Value::String("target-513".to_string()))]),
            })],
        )
        .unwrap();
    store
        .durable
        .as_mut()
        .unwrap()
        .set_graph_manifest_open_budget_bytes(1);
    let error = store.checkpoint(&catalog).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("aggregate encoded graph manifest bytes during open"),
        "unexpected graph manifest checkpoint admission error: {error}"
    );
    assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 2);
    let selected_manifest = DurableManifest::load(&path.join(MANIFEST_FILE)).unwrap();
    assert_eq!(selected_manifest.checkpoint_epoch, 2);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn graph_manifest_growth_is_rejected_before_unbounded_read() {
    let path = unique_test_dir("graph_manifest_growth");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }

    let manifest_path = path.join(canonical_manifest_generation_file(1));
    let original_len = fs::metadata(&manifest_path).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&manifest_path)
        .unwrap()
        .set_len(original_len.saturating_add(1024 * 1024))
        .unwrap();

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert!(
        error.to_string().contains("exceeding its admitted bound"),
        "unexpected graph manifest growth error: {error}"
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_checkpoint_uses_generation_bound_dense_adjacency_and_fails_closed() {
    use std::io::{Seek, SeekFrom};

    let path = unique_test_dir("canonical_adjacency_checkpoint");
    let replay_config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    let source;
    let first_relationship;
    let corrupt_offset;
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        source = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        first_relationship = RelId(0);
        for index in 0..70 {
            let target = store
                .create_node(
                    &mut catalog,
                    "Entity",
                    properties([("ordinal", Value::Int(index))]),
                )
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
                .unwrap();
        }
        store.checkpoint(&catalog).unwrap();
        let adjacency = store
            .durable
            .as_ref()
            .and_then(|durable| durable.canonical_adjacency.as_ref())
            .unwrap();
        let cache_before_scrub = store.segment_cache_snapshot().unwrap();
        let scrub = adjacency.deep_scrub().unwrap();
        assert_eq!(store.segment_cache_snapshot().unwrap(), cache_before_scrub);
        assert!(!path.join("adjacency.1.manifest.hawdb").exists());
        let descriptor_reader = hawdb_storage::graph_descriptor_tree::GraphDescriptorTreeRootReader::open(
                hawdb_storage::graph_descriptor_tree::GraphDescriptorTreePaths::new(
                    path.join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(1)),
                    path.join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(1)),
                ),
                hawdb_storage::graph_descriptor_tree::GraphDescriptorTreeBuildConfig::default(),
            )
            .unwrap();
        assert_eq!(
            descriptor_reader.root().descriptor_count,
            scrub.descriptors_checked
        );
        assert_eq!(
            descriptor_reader.root().source_commit_epoch,
            store.commit_epoch
        );
        assert_eq!(descriptor_reader.report().page_payload_bytes_read, 0);
        let mention_type = catalog.rel_type_id("MENTIONS").unwrap();
        corrupt_offset = 24 + 48;

        let mut relationship_ids = Vec::new();
        store
            .visit_adjacent_relationships_owned(
                source,
                Some(mention_type),
                AdjacencyDirection::Outgoing,
                |relationship| {
                    relationship_ids.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(relationship_ids.len(), 70);
        let mut ordered_prefix = Vec::new();
        let control = store
            .try_visit_ordered_adjacent_relationships_owned(
                source,
                Some(mention_type),
                AdjacencyDirection::Outgoing,
                1024 * 1024,
                |relationship| {
                    ordered_prefix.push(relationship.id);
                    Ok(if ordered_prefix.len() == 5 {
                        GraphScanControl::Stop
                    } else {
                        GraphScanControl::Continue
                    })
                },
            )
            .unwrap();
        assert_eq!(control, GraphScanControl::Stop);
        assert_eq!(ordered_prefix, (0..5).map(RelId).collect::<Vec<_>>());
        let cold = store.storage_residency_report().graph_index_reads;
        assert!(cold.adjacency_dense_blocks_read > 0);
        assert!(cold.adjacency_descriptor_pages_visited > 0);
        assert!(cold.adjacency_descriptor_page_bytes_decoded > 0);
        assert!(cold.adjacency_descriptor_storage_bytes_read > 0);
        assert!(cold.adjacency_descriptor_cache_misses > 0);

        let target = store
            .create_node(&mut catalog, "Entity", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();
        relationship_ids.clear();
        store
            .visit_adjacent_relationships_owned(
                source,
                Some(mention_type),
                AdjacencyDirection::Outgoing,
                |relationship| {
                    relationship_ids.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(relationship_ids.len(), 71);
        assert!(relationship_ids.contains(&first_relationship));
        let warm = store
            .storage_residency_report()
            .graph_index_reads
            .delta_since(cold);
        assert!(warm.adjacency_descriptor_cache_hits > 0);
        assert!(warm.adjacency_descriptor_page_bytes_decoded > 0);
        assert_eq!(warm.adjacency_descriptor_storage_bytes_read, 0);
    }
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join(canonical_adjacency_artifact_generation_file(1)))
            .unwrap();
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        let mention_type = catalog.rel_type_id("MENTIONS").unwrap();
        let error = store
            .visit_adjacent_relationships_owned(
                source,
                Some(mention_type),
                AdjacencyDirection::Outgoing,
                |_| GraphScanControl::Continue,
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("content digest verification"),
            "unexpected adjacency corruption error: {error}"
        );
        let poisoned = store
            .visit_adjacent_relationships_owned(
                source,
                Some(mention_type),
                AdjacencyDirection::Outgoing,
                |_| GraphScanControl::Continue,
            )
            .unwrap_err();
        assert!(poisoned.to_string().contains("poisoned"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_checkpoint_requires_adjacency_generation_binding() {
    let path = unique_test_dir("canonical_adjacency_required");
    let replay_config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        let source = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Entity", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }

    let manifest_path = path.join(MANIFEST_FILE);
    let manifest = fs::read_to_string(&manifest_path).unwrap();
    let mut body = manifest
        .lines()
        .filter(|line| !line.starts_with("checksum\t"))
        .map(|line| {
            if line.starts_with("canonical_adjacency_") {
                let (field, _) = line.split_once('\t').unwrap();
                format!("{field}\tnone")
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    body.push('\n');
    fs::write(
        &manifest_path,
        format!("{body}checksum\t{}\n", checksum_bytes(body.as_bytes())),
    )
    .unwrap();
    fs::remove_file(path.join(canonical_adjacency_artifact_generation_file(1))).unwrap();
    fs::remove_file(
        path.join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(1)),
    )
    .unwrap();
    fs::remove_file(
        path.join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(1)),
    )
    .unwrap();

    let mut catalog = Catalog::default();
    let error = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay_config,
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("canonical segments require canonical adjacency"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_property_spills_round_trip_and_fail_closed() {
    use std::io::{Seek, SeekFrom};

    let path = unique_test_dir("property_spill_checkpoint");
    let replay_config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    let content = "property-spill-value-".repeat(8_192);
    let node_id;
    let corrupt_offset;
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        node_id = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("content", Value::String(content.clone()))]),
            )
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Entity", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                node_id,
                target,
                "MENTIONS",
                properties([("context", Value::String(content.clone()))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        store.scrub_storage().unwrap();
        let spill_manifest = store.property_spill_manifest().unwrap();
        assert_eq!(spill_manifest.value_count, 2);
        assert!(spill_manifest.value_bytes > 64 * 1024);
        assert_eq!(spill_manifest.source_commit_epoch, store.commit_epoch());
        let descriptor_root =
            hawdb_storage::graph_descriptor_tree::GraphDescriptorTreeRootReader::open_bound(
                hawdb_storage::graph_descriptor_tree::GraphDescriptorTreePaths::new(
                    path.join(
                        hawdb_storage::property_spill::property_spill_descriptor_page_file(1),
                    ),
                    path.join(
                        hawdb_storage::property_spill::property_spill_descriptor_root_file(1),
                    ),
                ),
                spill_manifest.descriptor_generation_artifacts(),
                hawdb_storage::graph_descriptor_tree::GraphDescriptorTreeBuildConfig::default(),
            )
            .unwrap();
        assert_eq!(
            descriptor_root.root().descriptor_count,
            spill_manifest.block_count
        );
        corrupt_offset = 40;
        let canonical_manifest = store.canonical_segment_manifest().unwrap();
        assert!(canonical_manifest.artifact_len < 16 * 1024);
        assert_eq!(canonical_manifest.node_segment_count, 1);
        assert_eq!(
            store.node_owned(node_id).unwrap().unwrap().properties["content"],
            Value::String(content.clone())
        );
        let mention_type = catalog.rel_type_id("MENTIONS").unwrap();
        let mut contexts = Vec::new();
        store
            .visit_adjacent_relationships_owned(
                node_id,
                Some(mention_type),
                AdjacencyDirection::Outgoing,
                |relationship| {
                    contexts.push(relationship.properties["context"].clone());
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(contexts, vec![Value::String(content.clone())]);
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        assert_eq!(
            store.node_owned(node_id).unwrap().unwrap().properties["content"],
            Value::String(content)
        );
    }
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join(property_spill_artifact_generation_file(1)))
            .unwrap();
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();
    }
    {
        let mut catalog = Catalog::default();
        let error = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("content digest verification"),
            "unexpected property spill corruption error: {error}"
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_property_spill_requires_the_selected_descriptor_root() {
    let path = unique_test_dir("property_spill_descriptor_root");
    let replay_config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    let descriptor_path =
        path.join(hawdb_storage::property_spill::property_spill_descriptor_root_file(1));
    let descriptor_bytes = fs::read(&descriptor_path).unwrap();
    fs::remove_file(&descriptor_path).unwrap();
    // I/O error text is platform/localization dependent. Capture this
    // filesystem's NotFound error instead of assuming Unix wording.
    let missing = fs::File::open(&descriptor_path).unwrap_err();
    assert_eq!(missing.kind(), std::io::ErrorKind::NotFound);
    let mut catalog = Catalog::default();
    let error = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay_config,
    )
    .unwrap_err();
    assert_eq!(error, HawDBError::Storage(missing.to_string()));
    // Restoring only the selected root must make the same store usable.
    fs::write(&descriptor_path, descriptor_bytes).unwrap();
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        assert_eq!(store.node_count_for_label(None), 1);
    }
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_property_projections_merge_wal_delta_and_fail_closed() {
    use hawdb_storage::property_projection::PersistentPropertyProjectionKind;
    use std::io::{Seek, SeekFrom};

    let path = unique_test_dir("property_projection_checkpoint");
    let replay_config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    let first_id;
    let second_id;
    let delta_id;
    let corrupt_offset;
    let composite_properties = vec!["key".to_string(), "rank".to_string()];
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        store
            .create_range_property_index(&mut catalog, "Memory", "rank")
            .unwrap();
        store
            .create_property_index(&mut catalog, "Memory", "key")
            .unwrap();
        store
            .create_full_text_property_index(&mut catalog, "Memory", "content")
            .unwrap();
        store
            .create_composite_property_index(&mut catalog, "Memory", &composite_properties)
            .unwrap();
        first_id = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("key", Value::String("first".to_string())),
                    ("rank", Value::Int(10)),
                    ("content", Value::String("alpha beta".to_string())),
                ]),
            )
            .unwrap();
        second_id = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("key", Value::String("second".to_string())),
                    ("rank", Value::Int(20)),
                    ("content", Value::String("beta gamma".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("key", Value::String("third".to_string())),
                    ("rank", Value::Int(30)),
                    ("content", Value::String("delta".to_string())),
                ]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();

        let label_id = catalog.label_id("Memory").unwrap();
        let manifest = store.persistent_property_projection_manifest().unwrap();
        assert!(manifest.supports(label_id, "key", PersistentPropertyProjectionKind::Equality));
        assert!(manifest.supports(label_id, "rank", PersistentPropertyProjectionKind::Range));
        assert!(manifest.supports(
            label_id,
            "content",
            PersistentPropertyProjectionKind::FullText
        ));
        assert!(manifest.supports_composite_equality(label_id, &composite_properties));
        let graph_index_reads_before = store.storage_residency_report().graph_index_reads;
        let read_snapshot = store.snapshot();
        corrupt_offset = property_projection_block_corrupt_offset(
            &path.join(property_projection_artifact_generation_file(1)),
            4,
        );

        let lower = (Value::Int(15), true);
        let upper = (Value::Int(25), true);
        let mut range_ids = Vec::new();
        store
            .visit_nodes_by_property_range_owned(
                label_id,
                "rank",
                Some(&lower),
                Some(&upper),
                |node| {
                    range_ids.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(range_ids, vec![second_id]);
        let mut equality_ids = Vec::new();
        read_snapshot
            .visit_nodes_by_property_owned(
                label_id,
                "key",
                &[Value::String("second".to_string())],
                |node| {
                    equality_ids.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(equality_ids, vec![second_id]);
        let mut full_text_ids = Vec::new();
        store
            .visit_nodes_by_full_text_property_owned(label_id, "content", "beta", |node| {
                full_text_ids.push(node.id);
                GraphScanControl::Continue
            })
            .unwrap();
        assert_eq!(full_text_ids, vec![first_id, second_id]);
        let mut composite_ids = Vec::new();
        store
            .visit_nodes_by_composite_property_owned(
                label_id,
                &[
                    ("key".to_string(), Value::String("second".to_string())),
                    ("rank".to_string(), Value::Int(20)),
                ],
                |node| {
                    composite_ids.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(composite_ids, vec![second_id]);
        let graph_index_reads = store
            .storage_residency_report()
            .graph_index_reads
            .delta_since(graph_index_reads_before);
        assert_eq!(
            graph_index_reads.operation_count(PersistentGraphIndexClass::NodeEquality),
            1
        );
        assert_eq!(
            graph_index_reads.operation_count(PersistentGraphIndexClass::NodeRange),
            1
        );
        assert_eq!(
            graph_index_reads.operation_count(PersistentGraphIndexClass::NodeFullText),
            1
        );
        assert_eq!(
            graph_index_reads.operation_count(PersistentGraphIndexClass::NodeCompositeEquality),
            1
        );
        assert!(graph_index_reads.property_blocks_read >= 4);
        assert!(graph_index_reads.property_bytes_read > 0);

        let second_filter = PropertyFilter::IdEq {
            value: Value::Int(second_id.0 as i64),
        };
        store
            .set_node_property(
                &mut catalog,
                "Memory",
                Some(&second_filter),
                "key",
                Value::String("updated".to_string()),
            )
            .unwrap();
        store
            .set_node_property(
                &mut catalog,
                "Memory",
                Some(&second_filter),
                "rank",
                Value::Int(40),
            )
            .unwrap();
        store
            .set_node_property(
                &mut catalog,
                "Memory",
                Some(&second_filter),
                "content",
                Value::String("omega".to_string()),
            )
            .unwrap();
        let first_filter = PropertyFilter::IdEq {
            value: Value::Int(first_id.0 as i64),
        };
        store
            .delete_nodes(&mut catalog, "Memory", Some(&first_filter), false)
            .unwrap();
        delta_id = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("key", Value::String("second".to_string())),
                    ("rank", Value::Int(18)),
                    ("content", Value::String("beta epsilon".to_string())),
                ]),
            )
            .unwrap();

        range_ids.clear();
        store
            .visit_nodes_by_property_range_owned(
                label_id,
                "rank",
                Some(&lower),
                Some(&upper),
                |node| {
                    range_ids.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(range_ids, vec![delta_id]);
        equality_ids.clear();
        store
            .visit_nodes_by_property_owned(
                label_id,
                "key",
                &[Value::String("second".to_string())],
                |node| {
                    equality_ids.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(equality_ids, vec![delta_id]);
        full_text_ids.clear();
        store
            .visit_nodes_by_full_text_property_owned(label_id, "content", "beta", |node| {
                full_text_ids.push(node.id);
                GraphScanControl::Continue
            })
            .unwrap();
        assert_eq!(full_text_ids, vec![delta_id]);
        composite_ids.clear();
        store
            .visit_nodes_by_composite_property_owned(
                label_id,
                &[
                    ("key".to_string(), Value::String("second".to_string())),
                    ("rank".to_string(), Value::Int(18)),
                ],
                |node| {
                    composite_ids.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(composite_ids, vec![delta_id]);
        composite_ids.clear();
        store
            .visit_nodes_by_composite_property_owned(
                label_id,
                &[
                    ("key".to_string(), Value::String("updated".to_string())),
                    ("rank".to_string(), Value::Int(40)),
                ],
                |node| {
                    composite_ids.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(composite_ids, vec![second_id]);
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        let label_id = catalog.label_id("Memory").unwrap();
        let lower = (Value::Int(15), true);
        let upper = (Value::Int(25), true);
        let mut range_ids = Vec::new();
        store
            .visit_nodes_by_property_range_owned(
                label_id,
                "rank",
                Some(&lower),
                Some(&upper),
                |node| {
                    range_ids.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(range_ids, vec![delta_id]);
        let mut equality_ids = Vec::new();
        store
            .visit_nodes_by_property_owned(
                label_id,
                "key",
                &[Value::String("second".to_string())],
                |node| {
                    equality_ids.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(equality_ids, vec![delta_id]);
        let mut composite_ids = Vec::new();
        store
            .visit_nodes_by_composite_property_owned(
                label_id,
                &[
                    ("key".to_string(), Value::String("second".to_string())),
                    ("rank".to_string(), Value::Int(18)),
                ],
                |node| {
                    composite_ids.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(composite_ids, vec![delta_id]);
    }
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join(property_projection_artifact_generation_file(1)))
            .unwrap();
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        let label_id = catalog.label_id("Memory").unwrap();
        let error = store
            .visit_nodes_by_composite_property_owned(
                label_id,
                &[
                    ("key".to_string(), Value::String("second".to_string())),
                    ("rank".to_string(), Value::Int(18)),
                ],
                |_| GraphScanControl::Continue,
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("content digest verification"),
            "unexpected property projection corruption error: {error}"
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn out_of_core_relationship_property_projection_prunes_and_merges_wal_delta() {
    use hawdb_storage::property_projection::PersistentPropertyProjectionKind;
    use std::io::{Seek, SeekFrom};

    let path = unique_test_dir("relationship_property_projection_checkpoint");
    let replay_config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    let source;
    let second_relationship;
    let delta_relationship;
    let corrupt_offset;
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        source = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("id", Value::String("source".to_string()))]),
            )
            .unwrap();
        let targets = (0..3)
            .map(|id| {
                store
                    .create_node(&mut catalog, "Entity", properties([("id", Value::Int(id))]))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let relationship_ids = [10_i64, 20, 30]
            .into_iter()
            .zip(&targets)
            .map(|(rank, target)| {
                store
                    .create_relationship(
                        &mut catalog,
                        source,
                        *target,
                        "LINKS_TO",
                        properties([("rank", Value::Int(rank))]),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        second_relationship = relationship_ids[1];
        store.checkpoint(&catalog).unwrap();

        let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
        let manifest = store.persistent_property_projection_manifest().unwrap();
        assert!(manifest.supports_relationship(
            rel_type,
            "rank",
            PersistentPropertyProjectionKind::RelationshipEquality,
        ));
        assert!(manifest.supports_relationship(
            rel_type,
            "rank",
            PersistentPropertyProjectionKind::RelationshipRange,
        ));
        let graph_index_reads_before = store.storage_residency_report().graph_index_reads;
        corrupt_offset = property_projection_block_corrupt_offset(
            &path.join(property_projection_artifact_generation_file(1)),
            5,
        );

        let mut ids = Vec::new();
        let (_, report) = store
            .visit_adjacent_relationships_with_filter_owned(
                source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                &PropertyFilter::Eq {
                    property: "rank".to_string(),
                    value: Value::Int(20),
                },
                |relationship| {
                    ids.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(ids, vec![second_relationship]);
        assert!(report.is_some_and(|report| {
            report.pruned
                && matches!(
                    report.strategy,
                    ScanPruningStrategy::PropertyEq { ref property }
                        if property == "rank"
                )
        }));
        let mut outgoing_ids = Vec::new();
        store
            .visit_adjacent_relationships_owned(
                source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                |relationship| {
                    outgoing_ids.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(outgoing_ids.len(), 3);
        let mut incoming_ids = Vec::new();
        store
            .visit_adjacent_relationships_owned(
                targets[1],
                Some(rel_type),
                AdjacencyDirection::Incoming,
                |relationship| {
                    incoming_ids.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(incoming_ids, vec![second_relationship]);

        store
            .set_relationship_property(
                &mut catalog,
                RelationshipPropertyUpdate {
                    source_label: "Memory".to_string(),
                    filter: Some(PropertyFilter::IdEq {
                        value: Value::Int(source.0 as i64),
                    }),
                    rel_type: "LINKS_TO".to_string(),
                    target_label: "Entity".to_string(),
                    target_filter: None,
                    rel_filter: Some(PropertyFilter::IdEq {
                        value: Value::Int(second_relationship.0 as i64),
                    }),
                    property: "rank".to_string(),
                    value: Value::Int(40),
                },
            )
            .unwrap();
        delta_relationship = store
            .create_relationship(
                &mut catalog,
                source,
                targets[1],
                "LINKS_TO",
                properties([("rank", Value::Int(20))]),
            )
            .unwrap();

        ids.clear();
        store
            .visit_adjacent_relationships_with_filter_owned(
                source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                &PropertyFilter::Eq {
                    property: "rank".to_string(),
                    value: Value::Int(20),
                },
                |relationship| {
                    ids.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(ids, vec![delta_relationship]);
        ids.clear();
        store
            .visit_adjacent_relationships_with_filter_owned(
                source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                &PropertyFilter::Range {
                    property: "rank".to_string(),
                    lower: Some((Value::Int(35), true)),
                    upper: Some((Value::Int(45), true)),
                },
                |relationship| {
                    ids.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(ids, vec![second_relationship]);
        let graph_index_reads = store
            .storage_residency_report()
            .graph_index_reads
            .delta_since(graph_index_reads_before);
        assert_eq!(
            graph_index_reads.operation_count(PersistentGraphIndexClass::RelationshipEquality),
            2
        );
        assert_eq!(
            graph_index_reads.operation_count(PersistentGraphIndexClass::RelationshipRange),
            1
        );
        assert_eq!(
            graph_index_reads.operation_count(PersistentGraphIndexClass::ForwardAdjacency),
            1
        );
        assert_eq!(
            graph_index_reads.operation_count(PersistentGraphIndexClass::ReverseAdjacency),
            1
        );
        assert!(graph_index_reads.property_blocks_read >= 2);
        assert!(graph_index_reads.property_bytes_read > 0);
        assert!(graph_index_reads.adjacency_blocks_read >= 2);
        assert!(graph_index_reads.adjacency_bytes_read > 0);
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
        let mut ids = Vec::new();
        store
            .visit_adjacent_relationships_with_filter_owned(
                source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                &PropertyFilter::Eq {
                    property: "rank".to_string(),
                    value: Value::Int(20),
                },
                |relationship| {
                    ids.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(ids, vec![delta_relationship]);
    }
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join(property_projection_artifact_generation_file(1)))
            .unwrap();
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
        let error = store
            .visit_adjacent_relationships_with_filter_owned(
                source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                &PropertyFilter::Eq {
                    property: "rank".to_string(),
                    value: Value::Int(20),
                },
                |_| GraphScanControl::Continue,
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("content digest verification"),
            "unexpected relationship property projection corruption error: {error}"
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn persistent_graph_indexes_match_canonical_fallbacks_and_report_each_class() {
    let path = unique_test_dir("persistent_graph_index_differential");
    let replay_config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay_config,
    )
    .unwrap();
    store
        .create_property_index(&mut catalog, "Memory", "key")
        .unwrap();
    store
        .create_range_property_index(&mut catalog, "Memory", "rank")
        .unwrap();
    store
        .create_full_text_property_index(&mut catalog, "Memory", "content")
        .unwrap();
    store
        .create_composite_property_index(
            &mut catalog,
            "Memory",
            &["key".to_string(), "rank".to_string()],
        )
        .unwrap();
    let source = store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("key", Value::String("first".to_string())),
                ("rank", Value::Int(10)),
                ("content", Value::String("alpha beta".to_string())),
            ]),
        )
        .unwrap();
    let second = store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("key", Value::String("second".to_string())),
                ("rank", Value::Int(20)),
                ("content", Value::String("beta gamma".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("key", Value::String("third".to_string())),
                ("rank", Value::Int(30)),
                ("content", Value::String("delta".to_string())),
            ]),
        )
        .unwrap();
    let targets = (0..3)
        .map(|ordinal| {
            store
                .create_node(
                    &mut catalog,
                    "Entity",
                    properties([("ordinal", Value::Int(ordinal))]),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let relationships = [10_i64, 20, 30]
        .into_iter()
        .zip(&targets)
        .map(|(weight, target)| {
            store
                .create_relationship(
                    &mut catalog,
                    source,
                    *target,
                    "LINKS_TO",
                    properties([("weight", Value::Int(weight))]),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    store.checkpoint(&catalog).unwrap();

    let mut oracle = store.snapshot();
    oracle.persistent_property_projection = None;
    oracle.canonical_adjacency = None;
    let label_id = catalog.label_id("Memory").unwrap();
    let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
    let lower = (Value::Int(15), true);
    let upper = (Value::Int(25), true);
    let before = store.storage_residency_report().graph_index_reads;

    let mut indexed_equality = Vec::new();
    store
        .visit_nodes_by_property_owned(
            label_id,
            "key",
            &[Value::String("second".to_string())],
            |node| {
                indexed_equality.push(node.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    let mut oracle_equality = Vec::new();
    oracle
        .visit_nodes_by_property_owned(
            label_id,
            "key",
            &[Value::String("second".to_string())],
            |node| {
                oracle_equality.push(node.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    assert_eq!(indexed_equality, oracle_equality);
    assert_eq!(indexed_equality, vec![second]);

    let mut indexed_range = Vec::new();
    store
        .visit_nodes_by_property_range_owned(label_id, "rank", Some(&lower), Some(&upper), |node| {
            indexed_range.push(node.id);
            GraphScanControl::Continue
        })
        .unwrap();
    let mut oracle_range = Vec::new();
    oracle
        .visit_nodes_by_property_range_owned(label_id, "rank", Some(&lower), Some(&upper), |node| {
            oracle_range.push(node.id);
            GraphScanControl::Continue
        })
        .unwrap();
    assert_eq!(indexed_range, oracle_range);

    let mut indexed_full_text = Vec::new();
    store
        .visit_nodes_by_full_text_property_owned(label_id, "content", "beta", |node| {
            indexed_full_text.push(node.id);
            GraphScanControl::Continue
        })
        .unwrap();
    let mut oracle_full_text = Vec::new();
    oracle
        .visit_nodes_by_full_text_property_owned(label_id, "content", "beta", |node| {
            oracle_full_text.push(node.id);
            GraphScanControl::Continue
        })
        .unwrap();
    assert_eq!(indexed_full_text, oracle_full_text);

    let predicates = [
        ("key".to_string(), Value::String("second".to_string())),
        ("rank".to_string(), Value::Int(20)),
    ];
    let mut indexed_composite = Vec::new();
    store
        .visit_nodes_by_composite_property_owned(label_id, &predicates, |node| {
            indexed_composite.push(node.id);
            GraphScanControl::Continue
        })
        .unwrap();
    let mut oracle_composite = Vec::new();
    oracle
        .visit_nodes_by_composite_property_owned(label_id, &predicates, |node| {
            oracle_composite.push(node.id);
            GraphScanControl::Continue
        })
        .unwrap();
    assert_eq!(indexed_composite, oracle_composite);

    let equality_filter = PropertyFilter::Eq {
        property: "weight".to_string(),
        value: Value::Int(20),
    };
    let range_filter = PropertyFilter::Range {
        property: "weight".to_string(),
        lower: Some(lower.clone()),
        upper: Some(upper.clone()),
    };
    let mut indexed_relationship_equality = Vec::new();
    store
        .visit_adjacent_relationships_with_filter_owned(
            source,
            Some(rel_type),
            AdjacencyDirection::Outgoing,
            &equality_filter,
            |relationship| {
                indexed_relationship_equality.push(relationship.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    let mut oracle_relationship_equality = Vec::new();
    oracle
        .visit_adjacent_relationships_with_filter_owned(
            source,
            Some(rel_type),
            AdjacencyDirection::Outgoing,
            &equality_filter,
            |relationship| {
                oracle_relationship_equality.push(relationship.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    assert_eq!(indexed_relationship_equality, oracle_relationship_equality);
    assert_eq!(indexed_relationship_equality, vec![relationships[1]]);

    let mut indexed_relationship_range = Vec::new();
    store
        .visit_adjacent_relationships_with_filter_owned(
            source,
            Some(rel_type),
            AdjacencyDirection::Outgoing,
            &range_filter,
            |relationship| {
                indexed_relationship_range.push(relationship.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    let mut oracle_relationship_range = Vec::new();
    oracle
        .visit_adjacent_relationships_with_filter_owned(
            source,
            Some(rel_type),
            AdjacencyDirection::Outgoing,
            &range_filter,
            |relationship| {
                oracle_relationship_range.push(relationship.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    assert_eq!(indexed_relationship_range, oracle_relationship_range);

    let mut indexed_forward = Vec::new();
    store
        .visit_adjacent_relationships_owned(
            source,
            Some(rel_type),
            AdjacencyDirection::Outgoing,
            |relationship| {
                indexed_forward.push(relationship.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    let mut oracle_forward = Vec::new();
    oracle
        .visit_adjacent_relationships_owned(
            source,
            Some(rel_type),
            AdjacencyDirection::Outgoing,
            |relationship| {
                oracle_forward.push(relationship.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    assert_eq!(indexed_forward, oracle_forward);

    let mut indexed_reverse = Vec::new();
    store
        .visit_adjacent_relationships_owned(
            targets[1],
            Some(rel_type),
            AdjacencyDirection::Incoming,
            |relationship| {
                indexed_reverse.push(relationship.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    let mut oracle_reverse = Vec::new();
    oracle
        .visit_adjacent_relationships_owned(
            targets[1],
            Some(rel_type),
            AdjacencyDirection::Incoming,
            |relationship| {
                oracle_reverse.push(relationship.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    assert_eq!(indexed_reverse, oracle_reverse);

    let reads = store
        .storage_residency_report()
        .graph_index_reads
        .delta_since(before);
    for class in PersistentGraphIndexClass::ALL {
        assert_eq!(reads.operation_count(class), 1, "{}", class.as_str());
        assert!(reads.blocks_read(class) > 0, "{}", class.as_str());
        assert!(reads.bytes_read(class) > 0, "{}", class.as_str());
    }
    assert!(reads.property_blocks_read >= 6);
    assert!(reads.property_bytes_read > 0);
    assert!(reads.adjacency_blocks_read >= 2);
    assert!(reads.adjacency_bytes_read > 0);

    drop(oracle);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relationship_projection_estimate_fallback_reports_descriptor_io() {
    let path = unique_test_dir("relationship_projection_estimate_fallback");
    let replay_config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay_config,
    )
    .unwrap();
    let narrow_source = store
        .create_node(&mut catalog, "Entity", BTreeMap::new())
        .unwrap();
    let wide_source = store
        .create_node(&mut catalog, "Entity", BTreeMap::new())
        .unwrap();
    let targets = (0..5)
        .map(|_| {
            store
                .create_node(&mut catalog, "Entity", BTreeMap::new())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let expected = store
        .create_relationship(
            &mut catalog,
            narrow_source,
            targets[0],
            "LINKS_TO",
            properties([("weight", Value::Int(20))]),
        )
        .unwrap();
    for target in &targets[1..] {
        store
            .create_relationship(
                &mut catalog,
                wide_source,
                *target,
                "LINKS_TO",
                properties([("weight", Value::Int(20))]),
            )
            .unwrap();
    }
    store.checkpoint(&catalog).unwrap();

    let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
    let before = store.storage_residency_report().graph_index_reads;
    let mut relationships = Vec::new();
    store
        .visit_adjacent_relationships_with_filter_owned(
            narrow_source,
            Some(rel_type),
            AdjacencyDirection::Outgoing,
            &PropertyFilter::Eq {
                property: "weight".to_string(),
                value: Value::Int(20),
            },
            |relationship| {
                relationships.push(relationship.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    assert_eq!(relationships, vec![expected]);

    let reads = store
        .storage_residency_report()
        .graph_index_reads
        .delta_since(before);
    assert_eq!(
        reads.operation_count(PersistentGraphIndexClass::RelationshipEquality),
        1
    );
    assert_eq!(
        reads.blocks_read(PersistentGraphIndexClass::RelationshipEquality),
        0
    );
    assert!(reads.property_descriptor_pages_visited > 0);
    assert!(reads.property_descriptor_storage_bytes_read > 0);
    assert_eq!(reads.property_blocks_read, 0);
    assert_eq!(
        reads.operation_count(PersistentGraphIndexClass::ForwardAdjacency),
        1
    );
    assert!(reads.blocks_read(PersistentGraphIndexClass::ForwardAdjacency) > 0);

    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn corrupted_source_scan_artifact_never_blocks_canonical_graph_recovery() {
    let path = unique_test_dir("source_scan_corruption");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Source",
                properties([("id", Value::String("source-a".to_string()))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    std::fs::write(path.join(source_scan::SOURCE_SCAN_PAYLOAD_FILE), b"corrupt").unwrap();
    let mut catalog = Catalog::default();
    let store = GraphStore::open(&path, &mut catalog).unwrap();
    assert!(store.node(NodeId(0)).is_some());
    assert!(matches!(
        store.plan_published_source_scan(&ScanPredicate::True),
        ScanSegmentAccessPlan::Fallback(ScanSegmentFallback::NoManifest)
    ));
    assert!(!path.join(source_scan::SOURCE_SCAN_PAYLOAD_FILE).exists());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn source_scan_reader_caches_independently_verified_segment_ranges() {
    let path = unique_test_dir("source_scan_coalesced_ranges");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    for id in 0..129 {
        store
            .create_node(
                &mut catalog,
                "Source",
                properties([("id", Value::String(format!("source-{id}")))]),
            )
            .unwrap();
    }
    store.checkpoint(&catalog).unwrap();
    let SourceScanCandidateRead::Rows { rows, report, .. } = store
        .read_published_source_scan_candidates(
            &ScanPredicate::True,
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .unwrap()
    else {
        panic!("expected source scan payload read");
    };
    assert_eq!(rows.len(), 129);
    assert_eq!(report.range_count, 2);
    assert_eq!(report.wave_count, 1);
    let first_cache = store.segment_cache_snapshot().unwrap();
    assert_eq!(first_cache.entry_count, 2);
    assert_eq!(first_cache.miss_count, 2);

    let SourceScanCandidateRead::Rows { rows, .. } = store
        .read_published_source_scan_candidates(
            &ScanPredicate::True,
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .unwrap()
    else {
        panic!("expected cached source scan payload read");
    };
    assert_eq!(rows.len(), 129);
    let second_cache = store.segment_cache_snapshot().unwrap();
    assert_eq!(second_cache.hit_count, 2);
    assert_eq!(second_cache.resident_bytes, first_cache.resident_bytes);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn source_scan_candidate_budget_tracks_one_live_segment() {
    let path = unique_test_dir("source_scan_live_segment_budget");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    for id in 0..=source_scan::SOURCE_SCAN_TARGET_ROWS {
        store
            .create_node(
                &mut catalog,
                "Source",
                properties([("id", Value::String(format!("source-{id}")))]),
            )
            .unwrap();
    }
    store.checkpoint(&catalog).unwrap();
    let SourceScanCandidateRead::Rows { rows, .. } = store
        .read_published_source_scan_candidates(
            &ScanPredicate::True,
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
        )
        .unwrap()
    else {
        panic!("expected source scan payload read");
    };
    let row_bytes = |row: &SourceScanRow| {
        std::mem::size_of::<SourceScanRow>().saturating_add(
            usize::try_from(estimated_properties_bytes(&row.properties)).unwrap_or(usize::MAX),
        )
    };
    let first_segment_bytes = rows
        .iter()
        .take(source_scan::SOURCE_SCAN_TARGET_ROWS)
        .fold(0usize, |total, row| total.saturating_add(row_bytes(row)));
    let all_candidate_bytes = rows
        .iter()
        .fold(0usize, |total, row| total.saturating_add(row_bytes(row)));
    assert!(all_candidate_bytes > first_segment_bytes);

    let mut visited = 0usize;
    let visit = store
        .visit_published_source_scan_candidates_bounded(
            &ScanPredicate::True,
            SourceScanCandidateLimits::bounded(
                NonZeroUsize::new(2).unwrap(),
                NonZeroU64::new(1024 * 1024).unwrap(),
                NonZeroU64::new(1024 * 1024).unwrap(),
                NonZeroUsize::new(first_segment_bytes).unwrap(),
            ),
            None,
            &mut |_| {
                visited = visited.saturating_add(1);
                Ok(GraphScanControl::Continue)
            },
        )
        .unwrap();
    let SourceScanCandidateVisit::Rows {
        candidate_count, ..
    } = visit
    else {
        panic!("expected bounded source scan visit");
    };
    assert_eq!(visited, source_scan::SOURCE_SCAN_TARGET_ROWS + 1);
    assert_eq!(candidate_count, visited);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn source_scan_corruption_after_stream_admission_fails_closed() {
    let path = unique_test_dir("source_scan_runtime_corruption");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Source",
            properties([("id", Value::String("source-a".to_string()))]),
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();

    let payload_path = path.join(source_scan::SOURCE_SCAN_PAYLOAD_FILE);
    let mut payload = fs::read(&payload_path).unwrap();
    let corrupt_at = payload.len() / 2;
    payload[corrupt_at] ^= 0xff;
    fs::write(&payload_path, payload).unwrap();

    let error = store
        .read_published_source_scan_candidates(
            &ScanPredicate::True,
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(1024).unwrap(),
            NonZeroU64::new(1024).unwrap(),
        )
        .expect_err("an admitted streaming read must fail closed on corruption");
    assert!(matches!(error, HawDBError::StorageIntegrity(_)));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn ordered_adjacency_entries_sort_by_neighbor_then_relationship_id() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
        .unwrap();
    let target_one = store
        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
        .unwrap();
    let target_two = store
        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
        .unwrap();
    let target_three = store
        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(3))]))
        .unwrap();

    let rel_three = store
        .create_relationship(
            &mut catalog,
            source,
            target_three,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();
    let rel_one = store
        .create_relationship(
            &mut catalog,
            source,
            target_one,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();
    let rel_two = store
        .create_relationship(
            &mut catalog,
            source,
            target_two,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();
    let rel_type = catalog.rel_type_id("MENTIONS").unwrap();

    let outgoing = store.ordered_adjacency_entries(source, rel_type, AdjacencyDirection::Outgoing);
    let incoming =
        store.ordered_adjacency_entries(target_one, rel_type, AdjacencyDirection::Incoming);

    assert_eq!(
        outgoing,
        vec![
            OrderedAdjacencyEntry {
                relationship_id: rel_one,
                neighbor_id: target_one,
            },
            OrderedAdjacencyEntry {
                relationship_id: rel_two,
                neighbor_id: target_two,
            },
            OrderedAdjacencyEntry {
                relationship_id: rel_three,
                neighbor_id: target_three,
            },
        ]
    );
    assert_eq!(
        incoming,
        vec![OrderedAdjacencyEntry {
            relationship_id: rel_one,
            neighbor_id: source,
        }]
    );
}

#[test]
fn ordered_adjacency_visitor_streams_without_degree_sized_sort_keys() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", BTreeMap::new())
        .unwrap();
    for _ in 0..3 {
        let target = store
            .create_node(&mut catalog, "Entity", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();
    }
    let mention_type = catalog.rel_type_id("MENTIONS").unwrap();
    let mut visited = Vec::new();
    let control = store
        .try_visit_ordered_adjacent_relationships_owned(
            source,
            Some(mention_type),
            AdjacencyDirection::Outgoing,
            1,
            |relationship| {
                visited.push(relationship.id);
                Ok(if visited.len() == 2 {
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                })
            },
        )
        .unwrap();
    assert_eq!(control, GraphScanControl::Stop);
    assert_eq!(visited, vec![RelId(0), RelId(1)]);
}

#[test]
fn ordered_adjacency_entries_for_node_sort_across_relationship_types() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
        .unwrap();
    let target_one = store
        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
        .unwrap();
    let target_two = store
        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
        .unwrap();

    let rel_late_neighbor = store
        .create_relationship(
            &mut catalog,
            source,
            target_two,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();
    let rel_early_neighbor = store
        .create_relationship(
            &mut catalog,
            source,
            target_one,
            "RELATES_TO",
            BTreeMap::new(),
        )
        .unwrap();

    let outgoing = store.ordered_adjacency_entries_for_node(source, AdjacencyDirection::Outgoing);
    let incoming =
        store.ordered_adjacency_entries_for_node(target_one, AdjacencyDirection::Incoming);

    assert_eq!(
        outgoing,
        vec![
            OrderedAdjacencyEntry {
                relationship_id: rel_early_neighbor,
                neighbor_id: target_one,
            },
            OrderedAdjacencyEntry {
                relationship_id: rel_late_neighbor,
                neighbor_id: target_two,
            },
        ]
    );
    assert_eq!(
        incoming,
        vec![OrderedAdjacencyEntry {
            relationship_id: rel_early_neighbor,
            neighbor_id: source,
        }]
    );
}

#[test]
fn adjacency_group_stats_classify_sparse_and_dense_groups() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
        .unwrap();

    for target_index in 0..DENSE_ADJACENCY_DEGREE_THRESHOLD {
        let target = store
            .create_node(
                &mut catalog,
                "Entity",
                properties([("id", Value::Int(target_index as i64))]),
            )
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();
    }
    let rel_type = catalog.rel_type_id("MENTIONS").unwrap();

    let dense_stats = store.adjacency_group_stats(source, rel_type, AdjacencyDirection::Outgoing);
    let sparse_stats =
        store.adjacency_group_stats(NodeId(1), rel_type, AdjacencyDirection::Incoming);

    assert_eq!(dense_stats.degree, DENSE_ADJACENCY_DEGREE_THRESHOLD);
    assert_eq!(dense_stats.layout, AdjacencyLayout::Dense);
    assert_eq!(sparse_stats.degree, 1);
    assert_eq!(sparse_stats.layout, AdjacencyLayout::Sparse);
}

#[test]
fn adjacency_group_stats_for_node_reports_each_relationship_type() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
        .unwrap();
    let mentions_target = store
        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
        .unwrap();
    let relates_target = store
        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source,
            mentions_target,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source,
            relates_target,
            "RELATES_TO",
            BTreeMap::new(),
        )
        .unwrap();
    let mentions = catalog.rel_type_id("MENTIONS").unwrap();
    let relates_to = catalog.rel_type_id("RELATES_TO").unwrap();

    let stats = store.adjacency_group_stats_for_node(source, AdjacencyDirection::Outgoing);

    assert_eq!(
        stats,
        vec![
            AdjacencyGroupStats {
                node_id: source,
                rel_type: mentions,
                direction: AdjacencyDirection::Outgoing,
                degree: 1,
                layout: AdjacencyLayout::Sparse,
            },
            AdjacencyGroupStats {
                node_id: source,
                rel_type: relates_to,
                direction: AdjacencyDirection::Outgoing,
                degree: 1,
                layout: AdjacencyLayout::Sparse,
            },
        ]
    );
}

#[test]
fn adjacency_consistency_report_matches_full_recompute_after_relationship_mutations() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
        .unwrap();
    let target_one = store
        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
        .unwrap();
    let target_two = store
        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source,
            target_one,
            "MENTIONS",
            properties([("confidence", Value::Float(0.8))]),
        )
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source,
            target_two,
            "MENTIONS",
            properties([("confidence", Value::Float(0.9))]),
        )
        .unwrap();

    let report = store.adjacency_consistency_report();
    assert!(report.ready);
    assert_eq!(report.relationship_count, 2);
    assert_eq!(report.maintained_group_count, 3);
    assert_eq!(report.recomputed_group_count, 3);
    assert_eq!(report.missing_group_count, 0);
    assert_eq!(report.extra_group_count, 0);
    assert_eq!(report.mismatched_group_count, 0);
    assert_eq!(report.dangling_relationship_count, 0);
    assert!(report.mismatches.is_empty());

    store
        .delete_relationships(
            &mut catalog,
            RelationshipDeleteRequest {
                source_label: "Memory".to_string(),
                filter: Some(PropertyFilter::Eq {
                    property: "id".to_string(),
                    value: Value::Int(0),
                }),
                rel_type: "MENTIONS".to_string(),
                target_label: "Entity".to_string(),
                target_filter: Some(PropertyFilter::Eq {
                    property: "id".to_string(),
                    value: Value::Int(1),
                }),
                rel_filter: None,
            },
        )
        .unwrap();
    let report = store.adjacency_consistency_report();
    assert!(report.ready);
    assert_eq!(report.relationship_count, 1);
    assert_eq!(report.maintained_group_count, 2);
    assert_eq!(report.recomputed_group_count, 2);
    assert!(report.mismatches.is_empty());
}

#[test]
fn adjacency_consistency_report_matches_full_recompute_after_detach_delete() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
        .unwrap();
    let target = store
        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
        .unwrap();
    store
        .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
        .unwrap();

    store
        .delete_nodes(
            &mut catalog,
            "Memory",
            Some(&PropertyFilter::Eq {
                property: "id".to_string(),
                value: Value::Int(0),
            }),
            true,
        )
        .unwrap();
    let report = store.adjacency_consistency_report();
    assert!(report.ready);
    assert_eq!(report.relationship_count, 0);
    assert_eq!(report.maintained_group_count, 0);
    assert_eq!(report.recomputed_group_count, 0);
    assert_eq!(report.dangling_relationship_count, 0);
    assert!(report.mismatches.is_empty());
}

#[test]
fn checkpoints_relationships_and_truncates_wal() {
    let path = unique_test_dir("rel_checkpoint");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "RELATES_TO", BTreeMap::new())
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    assert_eq!(read_test_wal(&path).unwrap(), "");
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let rel_type = catalog.rel_type_id("RELATES_TO").unwrap();
        let rels = store
            .outgoing_relationships(NodeId(0), rel_type)
            .collect::<Vec<_>>();
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].source, NodeId(0));
        assert_eq!(rels[0].target, NodeId(1));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn search_projection_snapshots_preserve_payloads_across_append_trim_and_reopen() {
    use crate::relational::RelationalPrimaryKeyChangeCaptureLimits;

    let path = unique_test_dir("shared_search_projection_changes");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                    materialized_view: None,
                    name: "messages".to_string(),
                    columns: vec![RelationalColumnSchema {
                        name: "id".to_string(),
                        scalar_type: RelationalScalarType::Text,
                        nullable: false,
                        default: None,
                    }],
                    primary_key: vec!["id".to_string()],
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    indexes: Vec::new(),
                })],
            },
        )
        .unwrap();
    for id in 10..140 {
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
            .unwrap();
    }
    for batch in 0..2 {
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "messages".to_string(),
                        mode: RelationalInsertMode::Error,
                        rows: (0..128)
                            .map(|key| {
                                RelationalRow::new(vec![RelationalValue::Text(format!(
                                    "message-{batch}-{key:04}"
                                ))])
                            })
                            .collect(),
                    }],
                },
            )
            .unwrap();
    }
    let before_checkpoint = store.search_projection_changes_after(0);
    store.checkpoint(&catalog).unwrap();
    drop(store);
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(store.search_projection_changes_after(0), before_checkpoint);
    let snapshot = store.snapshot_for_read();
    let before = snapshot.search_projection_changes_after(0);
    let before_status = snapshot.search_projection_changefeed_status();
    assert_eq!(
        before
            .last()
            .unwrap()
            .relational_primary_key_changes
            .operation_count(),
        128
    );
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    assert_eq!(
        &store.search_projection_changes_after(0)[..before.len()],
        before
    );

    store.set_search_projection_primary_key_capture_limits(
        RelationalPrimaryKeyChangeCaptureLimits {
            max_entries: NonZeroUsize::new(1).unwrap(),
            ..RelationalPrimaryKeyChangeCaptureLimits::default()
        },
    );
    assert!(store
        .search_projection_changes_after(0)
        .iter()
        .find(|change| change.commit_epoch == snapshot.commit_epoch())
        .unwrap()
        .relational_primary_key_changes
        .requires_rebuild());
    assert_eq!(snapshot.search_projection_changes_after(0), before);
    assert_eq!(
        snapshot.search_projection_changefeed_status(),
        before_status
    );
    store.set_max_search_projection_change_log_entries(Some(1));
    let retained = store.search_projection_changes_after(0);
    let floor = store.search_projection_change_log_start_epoch();
    assert_eq!(retained.len(), 1);
    assert_eq!(floor, snapshot.commit_epoch());
    assert_eq!(snapshot.search_projection_changes_after(0), before);
    store.checkpoint(&catalog).unwrap();
    drop(store);

    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(store.search_projection_changes_after(0), retained);
    assert_eq!(store.search_projection_change_log_start_epoch(), floor);
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
        .unwrap();
    let with_wal = store.search_projection_changes_after(0);
    drop(store);
    let store = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(store.search_projection_changes_after(0), with_wal);
    assert_eq!(snapshot.search_projection_changes_after(0), before);
    drop(store);
    drop(snapshot);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoints_search_projection_changes_for_restart_safe_catch_up() {
    let path = unique_test_dir("search_projection_change_checkpoint");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("id", Value::String("deleted-memory".to_string()))]),
            )
            .unwrap();
        store
            .delete_nodes(
                &mut catalog,
                "Memory",
                Some(&PropertyFilter::Eq {
                    property: "id".to_string(),
                    value: Value::String("deleted-memory".to_string()),
                }),
                true,
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }

    let mut catalog = Catalog::default();
    let store = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(store.commit_epoch(), 2);
    assert_eq!(store.search_projection_change_log_start_epoch(), 0);
    assert!(store.node_owned(NodeId(0)).unwrap().is_none());
    assert_eq!(
        store.search_projection_graph_changes_after(0),
        vec![
            SearchProjectionGraphChange {
                commit_epoch: 1,
                upsert_node_ids: vec![0],
                delete_document_ids: Vec::new(),
                relational_primary_key_changes:
                    hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::Captured {
                        tables: Vec::new(),
                        encoded_bytes: 0,
                    },
            },
            SearchProjectionGraphChange {
                commit_epoch: 2,
                upsert_node_ids: vec![0],
                delete_document_ids: vec!["memory:deleted-memory".to_string()],
                relational_primary_key_changes:
                    hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::Captured {
                        tables: Vec::new(),
                        encoded_bytes: 0,
                    },
            },
        ]
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_requires_projection_change_boundary() {
    let path = unique_test_dir("search_projection_checkpoint_boundary");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("id", Value::String("memory-1".to_string()))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    let checkpoint_path = active_checkpoint_path(&path);
    rewrite_checksummed_file(
        &checkpoint_path,
        "search_projection_change_log_start_epoch\t0\n",
        "",
        "checkpoint",
    );
    rewrite_checksummed_file(
        &checkpoint_path,
        "search_projection_change\t1\t0\t\n",
        "",
        "checkpoint",
    );

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert!(error
        .to_string()
        .contains("checkpoint search projection changes are missing their start epoch"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn rejects_checkpoint_relationship_with_missing_endpoint() {
    let path = unique_test_dir("rel_checkpoint_missing_endpoint");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "RELATES_TO", BTreeMap::new())
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }

    let properties_one = crate::store::encode_properties(&properties([("id", Value::Int(1))]));
    let properties_two = crate::store::encode_properties(&properties([("id", Value::Int(2))]));
    let corrupt_records =
        format!("node\t0\t0\t{properties_one}\nnode\t1\t0\t{properties_two}\nrel\t0\t0\t99\t0\t");
    rewrite_checksummed_file(
        &active_checkpoint_path(&path),
        "canonical_records\ttrue",
        &corrupt_records,
        "checkpoint",
    );

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert!(error
        .to_string()
        .contains("relationship 0 references missing target node 99"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_publishes_manifest_with_epoch_boundary() {
    let path = unique_test_dir("manifest_checkpoint");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }

    let checkpoint_bytes = std::fs::read(active_checkpoint_path(&path)).unwrap();
    assert!(checkpoint_bytes.starts_with(DURABLE_COMPRESSION_HEADER.as_bytes()));
    let header_end = checkpoint_bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .unwrap();
    let header = std::str::from_utf8(&checkpoint_bytes[..header_end]).unwrap();
    assert!(header.contains("codec\tzstd\n"));
    let checkpoint = read_durable_text(&active_checkpoint_path(&path), "checkpoint").unwrap();
    assert!(checkpoint.contains("commit_epoch\t2\n"));
    let manifest = std::fs::read_to_string(path.join("manifest.hawdb")).unwrap();
    assert!(manifest.contains("HAWDB_MANIFEST_V1\n"));
    assert!(manifest.contains("checkpoint_generation\t1\n"));
    assert!(manifest.contains("wal_generation\t1\n"));
    assert!(manifest.contains("checkpoint_epoch\t1\n"));
    assert!(manifest.contains("checkpoint_commit_epoch\t2\n"));
    assert!(manifest.contains("oldest_reader_commit_epoch\tnone\n"));
    assert!(manifest.contains("safe_reclaim_commit_epoch\t2\n"));
    assert!(manifest.contains("wal_replay_start_lsn\t3\n"));
    assert!(manifest.contains("next_lsn\t3\n"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn prepared_checkpoint_rejects_stale_source_without_rotating_wal() {
    let path = unique_test_dir("prepared_checkpoint_stale_source");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();

    let source = store.checkpoint_source();
    let prepared = source.prepare_checkpoint(&catalog).unwrap().unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
        .unwrap();
    let error = store
        .publish_prepared_checkpoint(prepared, None)
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("checkpoint source changed before publication"));
    assert!(!path.join("checkpoint.1.hawdb").exists());
    assert!(!path.join("wal.1.hawdb").exists());
    assert!(!path
        .join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(1))
        .exists());
    assert!(!path
        .join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(1))
        .exists());
    assert!(!path.join(".checkpoint.1.prepare").exists());
    drop(source);
    drop(store);

    let mut recovered_catalog = Catalog::default();
    let recovered = GraphStore::open(&path, &mut recovered_catalog).unwrap();
    assert_eq!(recovered.commit_epoch(), 2);
    assert_eq!(
        recovered.storage_recovery_report().checkpoint_commit_epoch,
        None
    );
    let memory = recovered_catalog.label_id("Memory").unwrap();
    assert_eq!(recovered.scan_nodes(Some(memory)).count(), 2);
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_wal_tail_preserves_every_post_snapshot_transaction() {
    use super::{wal_generation_file, WalCursorEvent, WalOpenOutcome, WalRecordCursor};

    let path = unique_test_dir("checkpoint_wal_tail");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let source = store.checkpoint_source();
    let prepared = source.prepare_checkpoint(&catalog).unwrap().unwrap();
    let authoritative = store.durable.as_ref().unwrap().wal_path.clone();
    for id in 2..=5 {
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("id", Value::Int(id)),
                    ("body", Value::String("x".repeat(48 * 1024))),
                ]),
            )
            .unwrap();
    }
    let captured = store.checkpoint_source();
    let before = std::fs::read(&authoritative).unwrap();
    let receipt = captured.prepare_checkpoint_wal_tail(&prepared).unwrap();
    assert_eq!(receipt.entries, 4);
    assert_eq!(receipt.captured_commit_epoch, 5);
    assert_eq!(std::fs::read(&authoritative).unwrap(), before);
    assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 0);
    let manifest = store
        .durable
        .as_ref()
        .unwrap()
        .checkpoint_manifest(
            prepared.generation,
            prepared.manifest_artifacts,
            prepared.source_commit_epoch,
            None,
            prepared.source_scan_publication,
            super::durable::CheckpointReplayBoundary {
                start_lsn: prepared.source_next_lsn,
                next_lsn: receipt.captured_next_lsn,
                commit_epoch: receipt.captured_commit_epoch,
            },
        )
        .unwrap();
    assert_eq!(manifest.checkpoint_commit_epoch, 1);
    assert_eq!(manifest.wal_replay_start_lsn, 2);
    assert_eq!(manifest.next_lsn, 6);
    let candidate = path.join(wal_generation_file(prepared.generation));
    let WalOpenOutcome::Cursor(mut cursor) = WalRecordCursor::open(&candidate, None).unwrap()
    else {
        panic!("candidate suffix has no valid header");
    };
    assert_eq!(cursor.generation(), prepared.generation);
    assert_eq!(cursor.start_lsn(), prepared.source_next_lsn);
    for lsn in prepared.source_next_lsn..receipt.captured_next_lsn {
        let WalCursorEvent::Entry { entry, .. } = cursor.next().unwrap() else {
            panic!("candidate lost a complete transaction");
        };
        assert_eq!(entry.lsn, lsn);
    }
    assert!(matches!(cursor.next().unwrap(), WalCursorEvent::Eof));
    drop(cursor);
    // Exercise the real v1 reopen path for checkpoint S plus its suffix
    // to C. Production publication still requires candidate rebasing and
    // writer revalidation; this controlled fixture ends all writes here.
    store
        .durable
        .as_ref()
        .unwrap()
        .publish_checkpoint_sidecars(
            &prepared.staging_path,
            prepared.publish_projected_graph_artifacts,
            prepared.source_scan_publication,
        )
        .unwrap();
    manifest.write(&path.join(super::MANIFEST_FILE)).unwrap();
    drop(captured);
    drop(prepared);
    drop(source);
    drop(store);
    let recovered = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(recovered.commit_epoch(), 5);
    assert_eq!(
        recovered.storage_recovery_report().checkpoint_commit_epoch,
        Some(1)
    );
    assert_eq!(recovered.scan_nodes(None).count(), 5);
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_candidate_private_recovery_keeps_existing_selectors_until_publication() {
    use crate::config::RelationalIndexMode;
    use crate::relational::{
        RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE, RELATIONAL_ROW_DELTA_MANIFEST_FILE,
    };

    let path = unique_test_dir("checkpoint_candidate_private_selectors");
    let replay = WalReplayConfig {
        relational_index_mode: RelationalIndexMode::Shadow,
        ..WalReplayConfig::default()
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay,
    )
    .unwrap();
    let row = |id: usize| {
        RelationalRow::new(vec![
            RelationalValue::Text(id.to_string()),
            RelationalValue::Text(format!("complete row {id}: 雪")),
        ])
    };
    let insert = |id| RelationalTransaction {
        writes: vec![RelationalWrite::Insert {
            table: "documents".into(),
            mode: RelationalInsertMode::Error,
            rows: vec![row(id)],
        }],
    };
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        materialized_view: None,
                        name: "documents".into(),
                        columns: ["id", "body"]
                            .into_iter()
                            .map(|name| RelationalColumnSchema {
                                name: name.into(),
                                scalar_type: RelationalScalarType::Text,
                                nullable: false,
                                default: None,
                            })
                            .collect(),
                        primary_key: vec!["id".into()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: Vec::new(),
                    }),
                    insert(0).writes.pop().unwrap(),
                ],
            },
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    store
        .commit_relational_transaction(&mut catalog, insert(1))
        .unwrap();
    drop(store);
    // Ordinary writable recovery establishes exact source selectors before
    // any private future generation exists.
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay,
    )
    .unwrap();
    let source = store.checkpoint_source();
    let mut candidate = source
        .prepare_checkpoint_candidate(&catalog)
        .unwrap()
        .unwrap();
    drop(source);
    store
        .commit_relational_transaction(&mut catalog, insert(2))
        .unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(7))]))
        .unwrap();
    let before_manifest = fs::read(path.join(MANIFEST_FILE)).unwrap();
    let before_wal = fs::read(active_wal_path(&path)).unwrap();
    let selectors = [
        RELATIONAL_ROW_DELTA_MANIFEST_FILE,
        RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE,
    ]
    .map(|name| (name, fs::read(path.join(name)).unwrap()));
    let captured = store.checkpoint_source();
    candidate.catch_up(&captured).unwrap();
    drop(captured);
    candidate.finish_catch_up().unwrap();
    assert_eq!(fs::read(path.join(MANIFEST_FILE)).unwrap(), before_manifest);
    assert_eq!(fs::read(active_wal_path(&path)).unwrap(), before_wal);
    for (name, expected) in selectors {
        assert_eq!(
            fs::read(path.join(name)).unwrap(),
            expected,
            "private {name}"
        );
    }
    drop(candidate);
    let expected = (0..3).map(row).collect::<Vec<_>>();
    assert_eq!(
        store
            .relational_state()
            .rows("documents")
            .map(|(_, row)| row.clone())
            .collect::<Vec<_>>(),
        expected
    );
    drop(store);
    let recovered = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay,
    )
    .unwrap();
    assert_eq!(
        recovered
            .relational_state()
            .rows("documents")
            .map(|(_, row)| row.clone())
            .collect::<Vec<_>>(),
        expected
    );
    drop(recovered);
    let read_only = GraphStore::open_read_only_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            recovery_mode: crate::config::RecoveryMode::Strict,
            ..replay
        },
    )
    .unwrap();
    assert_eq!(
        read_only
            .relational_state()
            .rows("documents")
            .map(|(_, row)| row.clone())
            .collect::<Vec<_>>(),
        expected
    );
    drop(read_only);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_candidate_seals_multiple_prefixes_on_the_same_base() {
    use crate::config::RelationalIndexMode;
    use crate::relational::{
        RelationalHydrationBudget, RelationalKey, RelationalRowPageSnapshotReadLimits,
    };
    for mode in [
        RelationalIndexMode::Shadow,
        RelationalIndexMode::Authoritative,
    ] {
        for durability in [
            DurabilityPolicy::SyncOnEveryWrite,
            DurabilityPolicy::SyncOnCheckpoint,
        ] {
            let path = unique_test_dir("checkpoint_candidate_multiple_prefixes");
            let replay = WalReplayConfig {
                residency_mode: StorageResidencyMode::OutOfCore,
                relational_index_mode: mode,
                ..WalReplayConfig::default()
            };
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                durability,
                WalReplayConfig {
                    relational_index_mode: RelationalIndexMode::Shadow,
                    ..replay
                },
            )
            .unwrap();
            let row = |id: usize| {
                RelationalRow::new(vec![
                    RelationalValue::Text(id.to_string()),
                    RelationalValue::Text(format!("sealed complete row {id}: 雪")),
                ])
            };
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                            materialized_view: None,
                            name: "documents".into(),
                            columns: ["id", "body"]
                                .into_iter()
                                .map(|name| RelationalColumnSchema {
                                    name: name.into(),
                                    scalar_type: RelationalScalarType::Text,
                                    nullable: false,
                                    default: None,
                                })
                                .collect(),
                            primary_key: vec!["id".into()],
                            unique_constraints: Vec::new(),
                            foreign_keys: Vec::new(),
                            indexes: Vec::new(),
                        })],
                    },
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
            drop(store);
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                durability,
                replay,
            )
            .unwrap();
            let source = store.checkpoint_source();
            let base_epoch = source.commit_epoch();
            let generation = source
                .durable
                .as_ref()
                .unwrap()
                .next_checkpoint_generation()
                .unwrap();
            let mut candidate = source
                .prepare_checkpoint_candidate(&catalog)
                .unwrap()
                .unwrap();
            drop(source);
            let base_path = path.join(super::checkpoint_generation_file(generation));
            let base_bytes = fs::read(&base_path).unwrap();
            let manifest_before = fs::read(path.join(MANIFEST_FILE)).unwrap();
            let mut previous_wal = Vec::new();
            let check = |store: &GraphStore, count: usize| {
                assert_eq!(store.commit_epoch(), base_epoch + 2 * count as u64);
                assert_eq!(store.node_count_for_label(None), count);
                store
                    .validate_authoritative_relational_index_open()
                    .unwrap();
                let reader = store
                    .open_relational_row_snapshot_reader()
                    .unwrap()
                    .unwrap();
                let mut hydration = RelationalHydrationBudget::default();
                for id in 0..count {
                    assert_eq!(
                        store
                            .node_owned(NodeId(id as u64))
                            .unwrap()
                            .unwrap()
                            .properties
                            .get("id"),
                        Some(&Value::Int(id as i64))
                    );
                    let key = RelationalKey(vec![RelationalValue::Text(id.to_string())]);
                    let (actual, report) = reader
                        .point_projected(
                            "documents",
                            &key,
                            &[0, 1],
                            RelationalRowPageSnapshotReadLimits::default(),
                            &mut hydration,
                            &hawdb_core::RuntimeTaskContext::default(),
                        )
                        .unwrap();
                    let actual = actual
                        .unwrap()
                        .fields
                        .into_iter()
                        .map(|field| field.value)
                        .collect::<Vec<_>>();
                    assert_eq!(actual.as_slice(), row(id).values());
                    assert_eq!(report.identity.visible_commit_epoch, store.commit_epoch());
                    let mut postings = Vec::new();
                    store
                        .visit_relational_index_read_view_prefix(
                            "documents",
                            crate::relational::RELATIONAL_PRIMARY_INDEX_NAME,
                            &key,
                            crate::relational::RelationalIndexReadLimits::default(),
                            |posting| {
                                postings.push(posting.clone());
                                true
                            },
                        )
                        .unwrap()
                        .unwrap();
                    assert_eq!(postings, vec![key]);
                }
            };
            for id in 0..4 {
                store
                    .commit_relational_transaction(
                        &mut catalog,
                        RelationalTransaction {
                            writes: vec![RelationalWrite::Insert {
                                table: "documents".into(),
                                mode: RelationalInsertMode::Error,
                                rows: vec![row(id)],
                            }],
                        },
                    )
                    .unwrap();
                store
                    .create_node(
                        &mut catalog,
                        "Memory",
                        properties([("id", Value::Int(id as i64))]),
                    )
                    .unwrap();
                let captured = store.checkpoint_source();
                let tail = candidate.catch_up(&captured).unwrap();
                assert_eq!(tail.entries, 2 * (id + 1) as u64);
                candidate.finish_catch_up().unwrap();
                candidate.finish_catch_up().unwrap();
                assert_eq!(candidate.catch_up(&captured).unwrap(), tail);
                drop(captured);
                assert_eq!(candidate.commit_epoch(), base_epoch + tail.entries);
                assert_eq!(fs::read(&base_path).unwrap(), base_bytes);
                assert_eq!(fs::read(path.join(MANIFEST_FILE)).unwrap(), manifest_before);
                let wal = fs::read(path.join(super::wal_generation_file(generation))).unwrap();
                assert!(wal.starts_with(&previous_wal));
                assert!(wal.len() > previous_wal.len());
                previous_wal = wal;
            }
            store
                .publish_checkpoint_candidate_deferred_reclamation(&mut candidate, None)
                .unwrap();
            assert!(store.relational_row_pages.recovery_builder.is_none());
            assert!(store.relational_index_shadow.recovery_builder.is_none());
            check(&store, 4);
            drop(candidate);
            drop(store);
            let read_only = GraphStore::open_read_only_with_durability_and_replay_config(
                &path,
                &mut catalog,
                durability,
                WalReplayConfig {
                    recovery_mode: crate::config::RecoveryMode::Strict,
                    ..replay
                },
            )
            .unwrap();
            check(&read_only, 4);
            drop(read_only);
            let recovered = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                durability,
                replay,
            )
            .unwrap();
            check(&recovered, 4);
            drop(recovered);
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn checkpoint_candidate_private_recovery_publishes_complete_selectors() {
    for mode in [
        crate::config::RelationalIndexMode::Shadow,
        crate::config::RelationalIndexMode::Authoritative,
    ] {
        for durability in [
            DurabilityPolicy::SyncOnEveryWrite,
            DurabilityPolicy::SyncOnCheckpoint,
        ] {
            private_checkpoint_recovery_publication(mode, durability, false);
        }
    }
}

#[test]
fn checkpoint_candidate_private_recovery_uncertain_publication_retains_recovery() {
    for mode in [
        crate::config::RelationalIndexMode::Shadow,
        crate::config::RelationalIndexMode::Authoritative,
    ] {
        for durability in [
            DurabilityPolicy::SyncOnEveryWrite,
            DurabilityPolicy::SyncOnCheckpoint,
        ] {
            private_checkpoint_recovery_publication(mode, durability, true);
        }
    }
}

#[test]
fn checkpoint_candidate_advances_across_captured_suffixes_without_rebuilding() {
    for residency in [
        StorageResidencyMode::Auto,
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir("checkpoint_candidate_progress");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                residency_mode: residency,
                ..Default::default()
            },
        )
        .unwrap();
        for id in 1..=3 {
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
                .unwrap();
        }
        let reader = store.snapshot();
        let source = store.checkpoint_source();
        let mut candidate = source
            .prepare_checkpoint_candidate(&catalog)
            .unwrap()
            .unwrap();
        drop(source);
        let before = std::fs::read(path.join(super::MANIFEST_FILE)).unwrap();
        for id in 4..=5 {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("id", Value::Int(id)),
                        ("body", Value::String("x".repeat(48 * 1024))),
                    ]),
                )
                .unwrap();
        }
        // Stale publication preserves this candidate for incremental retry.
        assert!(store
            .publish_checkpoint_candidate(&mut candidate, Some(3), &Default::default())
            .is_err());
        assert_eq!(
            std::fs::read(path.join(super::MANIFEST_FILE)).unwrap(),
            before
        );
        let captured = store.checkpoint_source();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(6))]))
            .unwrap();
        let receipt = candidate.catch_up(&captured).unwrap();
        assert_eq!(receipt.entries, 2);
        assert_eq!(candidate.commit_epoch(), 5);
        drop(captured);
        assert!(store
            .publish_checkpoint_candidate(&mut candidate, Some(3), &Default::default())
            .is_err());
        for id in 7..=9 {
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
                .unwrap();
            let captured = store.checkpoint_source();
            let receipt = candidate.catch_up(&captured).unwrap();
            assert_eq!(receipt.entries, id as u64 - 3);
            assert_eq!(candidate.commit_epoch(), id as u64);
        }
        let last_key = hawdb_storage::version::VersionKey::Database;
        let last_stamp = store.version_index.stamp(&last_key);
        assert!(last_stamp.is_some());
        catalog = store
            .publish_checkpoint_candidate(&mut candidate, Some(3), &Default::default())
            .unwrap();
        assert_eq!(store.commit_epoch(), 9);
        assert_eq!(store.version_index.stamp(&last_key), last_stamp);
        assert_eq!(
            store
                .node_records_owned()
                .collect::<crate::error::Result<Vec<_>>>()
                .unwrap()
                .len(),
            9
        );
        assert_eq!(
            reader
                .node_records_owned()
                .collect::<crate::error::Result<Vec<_>>>()
                .unwrap()
                .len(),
            3
        );
        for id in 0..9 {
            assert_eq!(
                store
                    .node_owned(NodeId(id))
                    .unwrap()
                    .unwrap()
                    .properties
                    .get("id"),
                Some(&Value::Int(id as i64 + 1))
            );
        }
        assert_eq!(reader.commit_epoch(), 3);
        reader.ensure_usable().unwrap();
        assert_eq!(
            store
                .storage_reclamation_watermark(Some(3))
                .checkpoint_commit_epoch,
            Some(3)
        );
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(10))]))
            .unwrap();
        drop(candidate);
        drop(reader);
        drop(store);
        let recovered = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                residency_mode: residency,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(recovered.commit_epoch(), 10);
        assert_eq!(
            recovered
                .node_records_owned()
                .collect::<crate::error::Result<Vec<_>>>()
                .unwrap()
                .len(),
            10
        );
        drop(recovered);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn checkpoint_candidate_cancellation_between_replay_records_preserves_authority() {
    use hawdb_core::{
        RuntimeCancellationToken, RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit,
        RuntimeTaskContext,
    };
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    #[derive(Debug)]
    struct WaveLease(Arc<AtomicUsize>);

    impl Drop for WaveLease {
        fn drop(&mut self) {
            self.0.fetch_sub(1, AtomicOrdering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct CancelDuringReplay {
        acquisitions: AtomicUsize,
        in_flight: Arc<AtomicUsize>,
        cancellation: RuntimeCancellationToken,
    }

    impl RuntimeIoWaveController for CancelDuringReplay {
        fn try_acquire(
            &self,
            slots: NonZeroUsize,
            task: &RuntimeTaskContext,
        ) -> std::result::Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
            self.acquire(slots, task).map(Some)
        }

        fn acquire(
            &self,
            slots: NonZeroUsize,
            _task: &RuntimeTaskContext,
        ) -> std::result::Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
            assert_eq!(slots.get(), 1);
            // For this single-block suffix: candidate/source open, source
            // seek/read and the first candidate write precede the second
            // write. Cancel there after exactly one transaction is applied.
            // All authority/partial-replay/cleanup/reopen assertions remain.
            if self.acquisitions.fetch_add(1, AtomicOrdering::SeqCst) + 1 == 6 {
                self.cancellation.cancel();
            }
            assert_eq!(self.in_flight.fetch_add(1, AtomicOrdering::SeqCst), 0);
            Ok(Box::new(WaveLease(Arc::clone(&self.in_flight))))
        }
    }

    let path = unique_test_dir("checkpoint_candidate_cancel_replay");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let generation = store
        .durable
        .as_ref()
        .unwrap()
        .next_checkpoint_generation()
        .unwrap();
    let source = store.checkpoint_source();
    let mut candidate = source
        .prepare_checkpoint_candidate(&catalog)
        .unwrap()
        .unwrap();
    drop(source);
    for id in 2..=4 {
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
            .unwrap();
    }
    let captured = store.checkpoint_source();
    let manifest = std::fs::read(path.join(super::MANIFEST_FILE)).unwrap();
    let original_wal = store.durable.as_ref().unwrap().wal_path.clone();
    let original_bytes = std::fs::read(&original_wal).unwrap();
    let cancellation = RuntimeCancellationToken::new();
    let controller = Arc::new(CancelDuringReplay {
        acquisitions: AtomicUsize::new(0),
        in_flight: Arc::new(AtomicUsize::new(0)),
        cancellation: cancellation.clone(),
    });
    let task = RuntimeTaskContext::without_deadline(cancellation)
        .with_io_wave_controller(controller.clone());
    let result = candidate.catch_up_with_task_context(&captured, &task);
    assert!(matches!(result, Err(HawDBError::RuntimeTaskStopped(_))));
    assert_eq!(candidate.commit_epoch(), 2);
    assert_eq!(controller.acquisitions.load(AtomicOrdering::SeqCst), 6);
    assert_eq!(controller.in_flight.load(AtomicOrdering::SeqCst), 0);
    assert!(store
        .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
        .is_err());
    store.ensure_usable().unwrap();
    assert_eq!(std::fs::read(&original_wal).unwrap(), original_bytes);
    assert_eq!(
        std::fs::read(path.join(super::MANIFEST_FILE)).unwrap(),
        manifest
    );
    let candidate_wal = path.join(super::wal_generation_file(generation));
    assert!(
        std::fs::metadata(&candidate_wal).unwrap().len()
            > super::WAL_BINARY_FILE_HEADER_BYTES as u64
    );
    drop(candidate);
    drop(captured);
    assert!(!candidate_wal.exists());
    assert!(!path
        .join(super::checkpoint_generation_file(generation))
        .exists());
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(5))]))
        .unwrap();
    drop(store);
    let recovered = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(recovered.commit_epoch(), 5);
    for id in 0..5 {
        assert_eq!(
            recovered
                .node_owned(NodeId(id))
                .unwrap()
                .unwrap()
                .properties
                .get("id"),
            Some(&Value::Int(id as i64 + 1)),
        );
    }
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_units_projected_build_matches_independent_edges_and_analytics_filters() {
    use crate::background::CheckpointWorkProbe;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    use std::sync::Arc;
    let (catalog, store, edges) = checkpoint_projection_fixture();
    let cases = [
        (vec![], vec![]),
        (vec!["Memory"], vec![]),
        (vec!["Source", "Missing"], vec!["BACK", "Missing"]),
        (vec!["Memory", "Memory", "Missing"], vec!["LINKS", "OTHER"]),
        (vec!["Missing"], vec![]),
        (vec![], vec!["Missing"]),
        (vec!["Memory"], vec!["Missing"]),
    ];
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    });
    for (labels, rel_types) in cases {
        let definition = ProjectedGraphDefinition {
            node_labels: labels.iter().map(|label| (*label).to_string()).collect(),
            rel_types: rel_types
                .iter()
                .map(|rel_type| (*rel_type).to_string())
                .collect(),
            relationship_predicates: Default::default(),
        };
        let ids = (0..1025)
            .filter(|id| {
                labels.is_empty() || labels.contains(&if id % 2 == 0 { "Memory" } else { "Source" })
            })
            .map(NodeId)
            .collect::<Vec<_>>();
        let indices = ids
            .iter()
            .enumerate()
            .map(|(index, id)| (*id, index))
            .collect::<BTreeMap<_, _>>();
        let pairs = edges
            .iter()
            .filter(|(_, _, kind)| rel_types.is_empty() || rel_types.contains(&kind.as_str()))
            .filter_map(|(source, target, _)| Some((*indices.get(source)?, *indices.get(target)?)))
            .collect::<BTreeSet<_>>();
        let mut outgoing = Vec::new();
        let mut incoming = Vec::new();
        let mut outgoing_offsets = vec![0];
        let mut incoming_offsets = vec![0];
        for node in 0..ids.len() {
            outgoing.extend(
                pairs
                    .iter()
                    .filter(|(source, _)| *source == node)
                    .map(|(_, target)| *target),
            );
            incoming.extend(
                pairs
                    .iter()
                    .filter(|(_, target)| *target == node)
                    .map(|(source, _)| *source),
            );
            outgoing_offsets.push(outgoing.len());
            incoming_offsets.push(incoming.len());
        }
        let expected = super::ProjectedGraphArtifactData::new(
            ids,
            outgoing_offsets,
            outgoing,
            incoming_offsets,
            incoming,
        )
        .unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = super::checkpoint_projected_graph_from_definition(
            &catalog,
            &store,
            &definition,
            &probe.context(scheduler.clone()),
        )
        .unwrap();
        assert_eq!(actual, expected, "definition={definition:?}");
        let analytics = super::projected_graph_from_definition(&catalog, &store, &definition);
        assert_eq!(actual.nodes, analytics.nodes());
        assert_eq!(actual.csr_offsets, analytics.csr_offsets());
        assert_eq!(actual.csr_targets, analytics.csr_targets());
        assert_eq!(actual.csc_offsets, analytics.csc_offsets());
        assert_eq!(actual.csc_sources, analytics.csc_sources());
        probe.assert_released(&scheduler);
    }
}

#[test]
fn checkpoint_units_projected_build_applies_predicates_and_preserves_all_adjacency_arrays() {
    use super::ProjectedGraphArtifactData;
    use crate::background::CheckpointWorkProbe;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    let nodes = (0..4)
        .map(|_| {
            store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap()
        })
        .collect::<Vec<_>>();
    for (source, target, kind, confidence, tag) in [
        (0, 1, "LINK", 0.7, "yes"),
        (0, 2, "LINK", 0.69, "yes"),
        (1, 3, "LINK", 0.8, "no"),
        (3, 0, "BACK", 0.1, "no"),
        (3, 0, "LINK", 0.9, "yes"),
    ] {
        store
            .create_relationship(
                &mut catalog,
                nodes[source],
                nodes[target],
                kind,
                BTreeMap::from([
                    ("confidence".into(), Value::Float(confidence)),
                    ("tag".into(), Value::String(tag.into())),
                ]),
            )
            .unwrap();
    }
    let identity = store.checkpoint_source_identity();
    let local = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    });
    for (labels, kinds) in [
        (vec!["Memory"], vec!["LINK", "BACK"]),
        (vec![], vec!["LINK"]),
        (vec!["Memory"], vec![]),
        (vec![], vec![]),
    ] {
        let definition = ProjectedGraphDefinition {
            node_labels: labels.iter().map(|name| (*name).into()).collect(),
            rel_types: kinds.iter().map(|name| (*name).into()).collect(),
            relationship_predicates: BTreeMap::from([
                (
                    "LINK".into(),
                    ProjectedRelationshipPredicate::And(vec![
                        ProjectedRelationshipPredicate::Gte {
                            property: "confidence".into(),
                            value: Value::Float(0.7),
                        },
                        ProjectedRelationshipPredicate::Eq {
                            property: "tag".into(),
                            value: Value::String("yes".into()),
                        },
                    ]),
                ),
                (
                    "Missing".into(),
                    ProjectedRelationshipPredicate::Eq {
                        property: "missing".into(),
                        value: Value::Null,
                    },
                ),
            ]),
        };
        let expected = if labels.is_empty() && kinds.is_empty() {
            // The ordinary all-label/all-type fast path ignores predicates.
            ProjectedGraphArtifactData::new(
                nodes.clone(),
                vec![0, 2, 3, 3, 4],
                vec![1, 2, 3, 0],
                vec![0, 1, 2, 3, 4],
                vec![3, 0, 0, 1],
            )
            .unwrap()
        } else {
            ProjectedGraphArtifactData::new(
                nodes.clone(),
                vec![0, 1, 1, 1, 2],
                vec![1, 0],
                vec![0, 1, 2, 2, 2],
                vec![3, 0],
            )
            .unwrap()
        };
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = super::checkpoint_projected_graph_from_definition(
            &catalog,
            &store,
            &definition,
            &probe.context(local.clone()),
        )
        .unwrap();
        assert_eq!(actual, expected);
        let ordinary = super::projected_graph_from_definition(&catalog, &store, &definition);
        assert_eq!(actual.nodes, ordinary.nodes());
        assert_eq!(actual.csr_offsets, ordinary.csr_offsets());
        assert_eq!(actual.csr_targets, ordinary.csr_targets());
        assert_eq!(actual.csc_offsets, ordinary.csc_offsets());
        assert_eq!(actual.csc_sources, ordinary.csc_sources());
        let total = probe.completed.load(Ordering::SeqCst);
        probe.assert_released(&local);
        for cut in 1..=total {
            let cancelled = Arc::new(CheckpointWorkProbe::default());
            cancelled.cancel_after.store(cut, Ordering::SeqCst);
            let error = super::checkpoint_projected_graph_from_definition(
                &catalog,
                &store,
                &definition,
                &cancelled.context(local.clone()),
            )
            .unwrap_err();
            assert!(error.to_string().contains("runtime task stopped"));
            assert_eq!(cancelled.completed.load(Ordering::SeqCst), cut);
            cancelled.assert_released(&local);
            assert_eq!(store.checkpoint_source_identity(), identity);
        }
        let retry = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            super::checkpoint_projected_graph_from_definition(
                &catalog,
                &store,
                &definition,
                &retry.context(local.clone()),
            )
            .unwrap(),
            expected
        );
        retry.assert_released(&local);
    }
}

#[test]
fn checkpoint_units_projected_build_cancels_capture_hydration_and_flatten_then_retries() {
    use crate::background::CheckpointWorkProbe;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    let (catalog, store, _) = checkpoint_projection_fixture();
    let nodes = store.nodes.len();
    let relationships = store.relationships.len();
    let definition = ProjectedGraphDefinition {
        node_labels: Vec::new(),
        rel_types: Vec::new(),
        relationship_predicates: Default::default(),
    };
    let identity = store.checkpoint_source_identity();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    });
    for cancel_after in [
        17,
        nodes + 17,
        2 * nodes + 17,
        3 * nodes + 1 + 17,
        3 * nodes + relationships + 1 + 17,
        3 * nodes + 2 * relationships + 2 + 17,
    ] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cancel_after, Ordering::SeqCst);
        let error = super::checkpoint_projected_graph_from_definition(
            &catalog,
            &store,
            &definition,
            &probe.context(scheduler.clone()),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("runtime task stopped"),
            "cancel_after={cancel_after}: {error}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), cancel_after);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&scheduler);
        assert_eq!(store.checkpoint_source_identity(), identity);
        store.ensure_usable().unwrap();
    }
    let retry = Arc::new(CheckpointWorkProbe::default());
    let actual = super::checkpoint_projected_graph_from_definition(
        &catalog,
        &store,
        &definition,
        &retry.context(scheduler.clone()),
    )
    .unwrap();
    let reference = super::projected_graph_from_definition(&catalog, &store, &definition);
    assert_eq!(actual.nodes, reference.nodes());
    assert_eq!(actual.csr_offsets, reference.csr_offsets());
    assert_eq!(actual.csr_targets, reference.csr_targets());
    assert_eq!(actual.csc_offsets, reference.csc_offsets());
    assert_eq!(actual.csc_sources, reference.csc_sources());
    retry.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_cancel_native_overlay_capture_before_io_and_retry_complete_graph() {
    use crate::background::CheckpointWorkProbe;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let path = unique_test_dir("checkpoint_overlay_capture_cancel");
    let open = |catalog: &mut Catalog| {
        GraphStore::open_with_durability_and_replay_config(
            &path,
            catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                residency_mode: StorageResidencyMode::OutOfCore,
                ..WalReplayConfig::default()
            },
        )
        .unwrap()
    };
    let mut catalog = Catalog::default();
    let mut store = open(&mut catalog);
    let mut nodes = Vec::new();
    let mut relationships = Vec::new();
    for id in 0..192 {
        nodes.push(
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
                .unwrap(),
        );
        if id > 0 {
            relationships.push(
                store
                    .create_relationship(
                        &mut catalog,
                        nodes[id as usize - 1],
                        nodes[id as usize],
                        "LINKS",
                        properties([("id", Value::Int(id))]),
                    )
                    .unwrap(),
            );
        }
        if id == 63 {
            store.checkpoint(&catalog).unwrap();
            assert!(store.canonical_base.is_some());
            assert!(store.nodes.is_empty());
            assert!(store.relationships.is_empty());
        }
    }
    store
        .set_node_properties_by_ids(
            &mut catalog,
            &[nodes[0]],
            &[NodeSetAssignment {
                property: "revision".to_string(),
                value: super::NodeSetValue::Value(Value::Int(2)),
            }],
        )
        .unwrap();
    store
        .delete_node_ids_with_limits(
            &mut catalog,
            &[nodes[20], nodes[150]],
            true,
            MutationLimits::default(),
        )
        .unwrap();
    assert!(!store.node_tombstones.is_empty());
    assert!(!store.relationship_tombstones.is_empty());
    assert!(store.nodes.len() > 17);
    assert!(store.relationships.len() > 17);
    let expected_nodes = nodes
        .into_iter()
        .filter_map(|id| store.node_owned(id).unwrap())
        .collect::<Vec<_>>();
    let expected_relationships = relationships
        .into_iter()
        .filter_map(|id| store.relationship_owned(id).unwrap())
        .collect::<Vec<_>>();
    let identity = store.checkpoint_source_identity();
    let durable = store.durable.as_ref().unwrap();
    let wal_path = durable.wal_path.clone();
    let manifest_path = durable.manifest_path().to_path_buf();
    let wal = std::fs::read(&wal_path).unwrap();
    let manifest = std::fs::read(&manifest_path).unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    });

    // Actual candidate preparation must stop during capture, before any
    // builder can hydrate base records or create an artifact.
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(17, Ordering::SeqCst);
    let source = store.checkpoint_source();
    let error = source
        .prepare_checkpoint_candidate_with_work_context(&catalog, &probe.context(scheduler.clone()))
        .unwrap_err();
    assert!(error.to_string().contains("runtime task stopped"));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 17);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    probe.assert_released(&scheduler);
    source.ensure_usable().unwrap();
    drop(source);

    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(17, Ordering::SeqCst);
    let error = store
        .checkpoint_relationship_records_owned(&probe.context(scheduler.clone()))
        .err()
        .expect("relationship capture must also stop before collecting its delta");
    assert!(error.to_string().contains("runtime task stopped"));
    assert_eq!(probe.completed.load(Ordering::SeqCst), 17);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    probe.assert_released(&scheduler);
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
    assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
    store.ensure_usable().unwrap();

    let retry = Arc::new(CheckpointWorkProbe::default());
    let work = retry.context(scheduler.clone());
    assert_eq!(
        store
            .checkpoint_node_records_owned(&work)
            .unwrap()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected_nodes,
    );
    assert_eq!(
        store
            .checkpoint_relationship_records_owned(&work)
            .unwrap()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected_relationships,
    );
    let source = store.checkpoint_source();
    let mut candidate = source
        .prepare_checkpoint_candidate_with_work_context(&catalog, &work)
        .unwrap()
        .unwrap();
    drop(source);
    let captured = store.checkpoint_source();
    candidate.catch_up(&captured).unwrap();
    drop(captured);
    store
        .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
        .unwrap();
    drop(candidate);
    retry.assert_released(&scheduler);
    drop(store);
    let recovered = open(&mut catalog);
    assert_eq!(
        recovered
            .node_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected_nodes
    );
    assert_eq!(
        recovered
            .relationship_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected_relationships
    );
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_units_statistics_candidate_cancellation_preserves_authority_and_reopens() {
    use crate::background::CheckpointWorkProbe;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    for residency in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir("checkpoint_statistics_cancel");
        let open = |catalog: &mut Catalog| {
            GraphStore::open_with_durability_and_replay_config(
                &path,
                catalog,
                DurabilityPolicy::default(),
                WalReplayConfig {
                    residency_mode: residency,
                    ..WalReplayConfig::default()
                },
            )
            .unwrap()
        };
        let mut catalog = Catalog::default();
        let mut store = open(&mut catalog);
        let index = store
            .create_property_index(&mut catalog, "Memory", "rank")
            .unwrap();
        let mut ids = Vec::new();
        for rank in 0..97i64 {
            let node = store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("rank", Value::Int(rank))]),
                )
                .unwrap();
            if let Some(previous) = ids.last() {
                store
                    .create_relationship(
                        &mut catalog,
                        *previous,
                        node,
                        "LINKS",
                        properties([("rank", Value::Int(rank))]),
                    )
                    .unwrap();
            }
            ids.push(node);
        }
        let expected_nodes = store
            .node_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();
        let expected_relationships = store
            .relationship_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();
        let expected_statistics = store.statistics(&catalog);
        let identity = store.checkpoint_source_identity();
        let durable = store.durable.as_ref().unwrap();
        let wal_path = durable.wal_path.clone();
        let manifest_path = durable.manifest_path().to_path_buf();
        let wal = std::fs::read(&wal_path).unwrap();
        let manifest = std::fs::read(&manifest_path).unwrap();
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(1),
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        });
        // Direct materialized statistics cancellation cannot be mistaken
        // for stopping an earlier projection phase. The first out-of-core
        // checkpoint instead admits basic counts and declared-index scans.
        if residency == StorageResidencyMode::Materialized {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(17, Ordering::SeqCst);
            let error = store
                .checkpoint_statistics_with_work_context(
                    &catalog,
                    &probe.context(scheduler.clone()),
                )
                .unwrap_err();
            assert!(error.to_string().contains("runtime task stopped"));
            assert_eq!(probe.completed.load(Ordering::SeqCst), 17);
            probe.assert_released(&scheduler);
        }
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(17, Ordering::SeqCst);
        let source = store.checkpoint_source();
        let error = source
            .prepare_checkpoint_candidate_with_work_context(
                &catalog,
                &probe.context(scheduler.clone()),
            )
            .unwrap_err();
        assert!(error.to_string().contains("runtime task stopped"));
        assert_eq!(probe.completed.load(Ordering::SeqCst), 17);
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
        probe.assert_released(&scheduler);
        source.ensure_usable().unwrap();
        drop(source);
        assert_eq!(store.checkpoint_source_identity(), identity);
        assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
        assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
        store.ensure_usable().unwrap();
        let retry = Arc::new(CheckpointWorkProbe::default());
        let source = store.checkpoint_source();
        let mut candidate = source
            .prepare_checkpoint_candidate_with_work_context(
                &catalog,
                &retry.context(scheduler.clone()),
            )
            .unwrap()
            .unwrap();
        drop(source);
        let captured = store.checkpoint_source();
        candidate.catch_up(&captured).unwrap();
        drop(captured);
        store
            .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
            .unwrap();
        drop(candidate);
        retry.assert_released(&scheduler);
        drop(store);
        let recovered = open(&mut catalog);
        assert_eq!(
            recovered
                .node_records_owned()
                .collect::<crate::Result<Vec<_>>>()
                .unwrap(),
            expected_nodes
        );
        assert_eq!(
            recovered
                .relationship_records_owned()
                .collect::<crate::Result<Vec<_>>>()
                .unwrap(),
            expected_relationships
        );
        let statistics = recovered.statistics(&catalog);
        assert_eq!(statistics.node_count, 97);
        assert_eq!(statistics.relationship_count, 96);
        assert_eq!(
            statistics.index_samples[&index],
            hawdb_core::IndexStatisticsSample::exact(97, 97)
        );
        if residency == StorageResidencyMode::Materialized {
            assert_eq!(statistics, expected_statistics);
        } else {
            assert!(!statistics.advanced_statistics_complete);
        }
        let retry = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            recovered
                .checkpoint_statistics_with_work_context(
                    &catalog,
                    &retry.context(scheduler.clone())
                )
                .unwrap(),
            statistics
        );
        retry.assert_released(&scheduler);
        drop(recovered);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn checkpoint_units_cancel_base_encoding_and_retry_without_changing_authority() {
    use crate::background::{CheckpointWorkContext, CheckpointWorkProbe};
    use hawdb_core::RuntimeTaskContext;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let path = unique_test_dir("checkpoint_base_unit_cancel");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    for id in 0..64i64 {
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
            .unwrap();
    }
    store.checkpoint(&catalog).unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(64))]))
        .unwrap();
    let identity = store.checkpoint_source_identity();
    let generation = store
        .durable
        .as_ref()
        .unwrap()
        .next_checkpoint_generation()
        .unwrap();
    let wal_path = store.durable.as_ref().unwrap().wal_path.clone();
    let manifest_path = store
        .durable
        .as_ref()
        .unwrap()
        .manifest_path()
        .to_path_buf();
    let wal = std::fs::read(&wal_path).unwrap();
    let manifest = std::fs::read(&manifest_path).unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    let private = path.join(super::canonical_artifact_generation_file(generation));
    let observer = Arc::new(CancelCheckpointAtFile {
        probe: probe.clone(),
        path: private.with_extension("hawdb.tmp"),
        observed_file: Default::default(),
        completed_after_file: Default::default(),
        cancel_after_file: 10,
        file_bytes_at_cancellation: Default::default(),
    });
    scheduler.set_telemetry_sink(Some(observer.clone()));
    let task = RuntimeTaskContext::without_deadline(probe.cancellation.clone())
        .with_io_wave_controller(probe.clone());
    let work = CheckpointWorkContext::new(task).with_scheduler(scheduler.clone());
    let source = store.checkpoint_source();
    let error = source
        .prepare_checkpoint_candidate_with_work_context(&catalog, &work)
        .unwrap_err();
    assert!(error.to_string().contains("runtime task stopped"));
    assert!(observer.observed_file.load(Ordering::SeqCst));
    assert_eq!(observer.completed_after_file.load(Ordering::SeqCst), 10);
    probe.assert_released(&scheduler);
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
    assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
    source.ensure_usable().unwrap();
    store.ensure_usable().unwrap();
    assert!(!private.exists());
    assert!(!private.with_extension("hawdb.tmp").exists());
    assert!(!path
        .join(format!(".checkpoint.{generation}.prepare"))
        .exists());
    drop(source);

    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(65))]))
        .unwrap();
    let retry = Arc::new(CheckpointWorkProbe::default());
    let work = retry.context(scheduler.clone());
    let source = store.checkpoint_source();
    let mut candidate = source
        .prepare_checkpoint_candidate_with_work_context(&catalog, &work)
        .unwrap()
        .unwrap();
    drop(source);
    let captured = store.checkpoint_source();
    candidate.catch_up(&captured).unwrap();
    drop(captured);
    store
        .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
        .unwrap();
    drop(candidate);
    retry.assert_released(&scheduler);
    drop(store);
    let recovered = GraphStore::open(&path, &mut catalog).unwrap();
    let mut ids = recovered
        .scan_nodes(None)
        .map(|node| match node.properties["id"] {
            Value::Int(id) => id,
            _ => panic!("wrong recovered id"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, (0..66i64).collect::<Vec<_>>());
    assert_eq!(recovered.commit_epoch(), 66);
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_units_append_capture_candidate_stops_before_publication_and_reopens_all_rows() {
    use crate::append_table::{
        append_generation_manifest_file, append_segment_file, AppendOrderMode, AppendTableRow,
        AppendTableSchema, AppendTransaction, AppendWrite,
    };
    use crate::background::{CheckpointWorkContext, CheckpointWorkProbe};
    use hawdb_core::RuntimeTaskContext;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    let path = unique_test_dir("checkpoint_append_capture_cancel");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    let schema = AppendTableSchema {
        name: "events".into(),
        columns: [
            ("stream", RelationalScalarType::BigInt),
            ("sequence", RelationalScalarType::BigInt),
            ("payload", RelationalScalarType::Bytea),
        ]
        .into_iter()
        .map(|(name, scalar_type)| RelationalColumnSchema {
            name: name.into(),
            scalar_type,
            nullable: false,
            default: None,
        })
        .collect(),
        partition_key: vec!["stream".into()],
        order_key: vec!["sequence".into()],
        order_mode: AppendOrderMode::CallerProvided,
    };
    let row = |sequence| {
        RelationalRow::new(vec![
            RelationalValue::BigInt(sequence % 7),
            RelationalValue::BigInt(sequence),
            RelationalValue::Bytea(vec![sequence as u8; 17]),
        ])
    };
    store
        .append_transaction(AppendTransaction {
            writes: vec![
                AppendWrite::CreateTable {
                    schema: schema.clone(),
                },
                AppendWrite::Append {
                    table: "events".into(),
                    rows: (0..64).map(row).collect(),
                },
            ],
        })
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    store
        .append_transaction(AppendTransaction {
            writes: vec![AppendWrite::Append {
                table: "events".into(),
                rows: (64..128).map(row).collect(),
            }],
        })
        .unwrap();
    assert_eq!(store.append_state.live_rows(), 64);
    let identity = store.checkpoint_source_identity();
    let durable = store.durable.as_ref().unwrap();
    let generation = durable.next_checkpoint_generation().unwrap();
    let staging = path.join(format!(".checkpoint.{generation}.prepare"));
    let wal_path = durable.wal_path.clone();
    let manifest_path = durable.manifest_path().to_path_buf();
    let wal = std::fs::read(&wal_path).unwrap();
    let manifest = std::fs::read(&manifest_path).unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    let observer = Arc::new(CancelCheckpointAtFile {
        probe: probe.clone(),
        path: staging.clone(),
        observed_file: Default::default(),
        completed_after_file: Default::default(),
        cancel_after_file: 17,
        file_bytes_at_cancellation: Default::default(),
    });
    scheduler.set_telemetry_sink(Some(observer.clone()));
    let work = CheckpointWorkContext::new(
        RuntimeTaskContext::without_deadline(probe.cancellation.clone())
            .with_io_wave_controller(probe.clone()),
    )
    .with_scheduler(scheduler.clone());
    let source = store.checkpoint_source();
    let error = source
        .prepare_checkpoint_candidate_with_work_context(&catalog, &work)
        .unwrap_err();
    assert!(error.to_string().contains("runtime task stopped"));
    assert!(observer.observed_file.load(Ordering::SeqCst));
    assert_eq!(observer.completed_after_file.load(Ordering::SeqCst), 17);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    probe.assert_released(&scheduler);
    source.ensure_usable().unwrap();
    drop(source);
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
    assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
    assert!(!staging.exists());
    assert!(!path.join(append_segment_file(generation)).exists());
    assert!(!path
        .join(append_generation_manifest_file(generation))
        .exists());
    store.ensure_usable().unwrap();
    let retry = Arc::new(CheckpointWorkProbe::default());
    let source = store.checkpoint_source();
    let mut candidate = source
        .prepare_checkpoint_candidate_with_work_context(&catalog, &retry.context(scheduler.clone()))
        .unwrap()
        .unwrap();
    drop(source);
    let captured = store.checkpoint_source();
    candidate.catch_up(&captured).unwrap();
    drop(captured);
    store
        .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
        .unwrap();
    drop(candidate);
    retry.assert_released(&scheduler);
    drop(store);
    let recovered = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(recovered.append_state.schema("events"), Some(&schema));
    let mut count = 0;
    for stream in 0..7i64 {
        let partition = RelationalKey(vec![RelationalValue::BigInt(stream)]);
        let actual = recovered
            .read_append_partition("events", &partition, None, 128)
            .unwrap();
        let expected = (0..128)
            .filter(|sequence| sequence % 7 == stream)
            .map(|sequence| AppendTableRow {
                table: "events".into(),
                partition_key: partition.clone(),
                order_key: RelationalKey(vec![RelationalValue::BigInt(sequence)]),
                row: row(sequence),
            })
            .collect::<Vec<_>>();
        assert_eq!(actual.rows, expected);
        count += actual.rows.len();
    }
    assert_eq!(count, 128);
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_units_cancel_native_tombstone_run_before_survivor_and_retry() {
    use crate::background::{CheckpointWorkContext, CheckpointWorkProbe};
    use hawdb_core::RuntimeTaskContext;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    let path = unique_test_dir("checkpoint_tombstone_run_cancel");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..WalReplayConfig::default()
        },
    )
    .unwrap();
    let mut ids = Vec::new();
    for id in 0..512 {
        ids.push(
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
                .unwrap(),
        );
    }
    store.checkpoint(&catalog).unwrap();
    store
        .delete_node_ids_with_limits(&mut catalog, &ids[..511], true, MutationLimits::default())
        .unwrap();
    assert!(store.nodes.is_empty());
    assert_eq!(store.node_tombstones.len(), 511);
    let expected = store.node_owned(ids[511]).unwrap().unwrap();
    let identity = store.checkpoint_source_identity();
    let durable = store.durable.as_ref().unwrap();
    let generation = durable.next_checkpoint_generation().unwrap();
    let wal_path = durable.wal_path.clone();
    let manifest_path = durable.manifest_path().to_path_buf();
    let wal = std::fs::read(&wal_path).unwrap();
    let manifest = std::fs::read(&manifest_path).unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    let private = path.join(super::canonical_artifact_generation_file(generation));
    let observer = Arc::new(CancelCheckpointAtFile {
        probe: probe.clone(),
        path: private.with_extension("hawdb.tmp"),
        observed_file: Default::default(),
        completed_after_file: Default::default(),
        cancel_after_file: 17,
        file_bytes_at_cancellation: Default::default(),
    });
    scheduler.set_telemetry_sink(Some(observer.clone()));
    let task = RuntimeTaskContext::without_deadline(probe.cancellation.clone())
        .with_io_wave_controller(probe.clone());
    let work = CheckpointWorkContext::new(task).with_scheduler(scheduler.clone());
    let source = store.checkpoint_source();
    let error = source
        .prepare_checkpoint_candidate_with_work_context(&catalog, &work)
        .unwrap_err();
    assert!(error.to_string().contains("runtime task stopped"));
    assert!(observer.observed_file.load(Ordering::SeqCst));
    assert_eq!(observer.completed_after_file.load(Ordering::SeqCst), 17);
    // No surviving record has been encoded: only the immutable 16-byte
    // canonical header and 8-byte generation were written before stopping.
    assert_eq!(
        observer.file_bytes_at_cancellation.load(Ordering::SeqCst),
        24
    );
    probe.assert_released(&scheduler);
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
    assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
    assert!(!private.exists());
    assert!(!observer.path.exists());
    source.ensure_usable().unwrap();
    store.ensure_usable().unwrap();
    drop(source);
    let retry = Arc::new(CheckpointWorkProbe::default());
    let source = store.checkpoint_source();
    let mut candidate = source
        .prepare_checkpoint_candidate_with_work_context(&catalog, &retry.context(scheduler.clone()))
        .unwrap()
        .unwrap();
    drop(source);
    let captured = store.checkpoint_source();
    candidate.catch_up(&captured).unwrap();
    drop(captured);
    store
        .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
        .unwrap();
    drop(candidate);
    retry.assert_released(&scheduler);
    drop(store);
    let recovered = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(
        recovered
            .node_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        vec![expected]
    );
    assert_eq!(recovered.relationship_records_owned().count(), 0);
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_units_cancel_adjacency_and_retry_with_complete_relationships() {
    assert_cancelled_derived_artifact_unit_preserves_authority("adjacency");
}

#[test]
fn checkpoint_units_cancel_property_projection_and_retry_with_complete_relationships() {
    assert_cancelled_derived_artifact_unit_preserves_authority("property-index");
}

#[test]
fn checkpoint_units_cancel_source_scan_and_retry_with_complete_graph() {
    assert_cancelled_derived_artifact_unit_preserves_authority("source-scan");
}

#[test]
fn checkpoint_units_cancel_projected_graph_and_retry_with_complete_arrays() {
    assert_cancelled_derived_artifact_unit_preserves_authority("projected-graph");
}

#[test]
fn checkpoint_candidate_rejects_lost_private_tail_and_cleans_owned_artifacts() {
    let path = unique_test_dir("checkpoint_candidate_lost_tail");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let source = store.checkpoint_source();
    let mut candidate = source
        .prepare_checkpoint_candidate(&catalog)
        .unwrap()
        .unwrap();
    drop(source);
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
        .unwrap();
    let captured = store.checkpoint_source();
    candidate.catch_up(&captured).unwrap();
    drop(captured);
    let old_manifest = std::fs::read(path.join(super::MANIFEST_FILE)).unwrap();
    let old_wal = store.durable.as_ref().unwrap().wal_path.clone();
    let old_bytes = std::fs::read(&old_wal).unwrap();
    // Locate this candidate, rather than allocate another generation.
    let generation = candidate.prepared_generation_for_test();
    let candidate_path = path.join(super::wal_generation_file(generation));
    std::fs::OpenOptions::new()
        .write(true)
        .open(&candidate_path)
        .unwrap()
        .set_len(super::WAL_BINARY_FILE_HEADER_BYTES as u64)
        .unwrap();
    assert!(matches!(
        store.publish_checkpoint_candidate(&mut candidate, None, &Default::default()),
        Err(HawDBError::StorageIntegrity(_))
    ));
    store.ensure_usable().unwrap();
    assert_eq!(
        std::fs::read(path.join(super::MANIFEST_FILE)).unwrap(),
        old_manifest
    );
    assert_eq!(std::fs::read(&old_wal).unwrap(), old_bytes);
    drop(candidate);
    assert!(!candidate_path.exists());
    assert!(!path
        .join(super::checkpoint_generation_file(generation))
        .exists());
    drop(store);
    let recovered = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 2);
    assert_eq!(recovered.commit_epoch(), 2);
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_candidate_uncertain_manifest_retains_complete_recovery_evidence() {
    let path = unique_test_dir("checkpoint_candidate_uncertain_manifest");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let reader = store.snapshot();
    let source = store.checkpoint_source();
    let mut candidate = source
        .prepare_checkpoint_candidate(&catalog)
        .unwrap()
        .unwrap();
    drop(source);
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
        .unwrap();
    let captured = store.checkpoint_source();
    candidate.catch_up(&captured).unwrap();
    drop(captured);
    // Locate this candidate, rather than allocate another generation.
    let generation = candidate.prepared_generation_for_test();
    super::set_checkpoint_failpoint(Some(super::CheckpointPublishStage::ManifestPublished));
    let publication =
        store.publish_checkpoint_candidate(&mut candidate, Some(1), &Default::default());
    super::set_checkpoint_failpoint(None);
    assert!(matches!(publication, Err(HawDBError::StorageIntegrity(_))));
    assert!(store.ensure_usable().is_err());
    assert!(reader.ensure_usable().is_err());
    drop(candidate);
    assert!(path.join(super::wal_generation_file(generation)).exists());
    assert!(path
        .join(super::checkpoint_generation_file(generation))
        .exists());
    drop(reader);
    drop(store);
    let recovered = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(recovered.commit_epoch(), 2);
    assert_eq!(recovered.node_count_for_label(None), 2);
    assert_eq!(
        recovered.storage_recovery_report().checkpoint_commit_epoch,
        Some(1)
    );
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn selected_checkpoint_handoff_preserves_live_ownership_and_releases_snapshot_floor() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let path = unique_test_dir("selected_checkpoint_handoff");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability(&path, &mut catalog, durability).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        let reader = store.snapshot();
        let base = store.checkpoint_source();
        let mut candidate = base
            .prepare_checkpoint_candidate(&catalog)
            .unwrap()
            .unwrap();
        drop(base);
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
        let expected = store.checkpoint_source_identity().unwrap();
        let mut worker = store.checkpoint_source();
        candidate.catch_up(&worker).unwrap();
        assert!(worker
            .publish_checkpoint_candidate_deferred_reclamation(&mut candidate, Some(1))
            .unwrap_err()
            .to_string()
            .contains("finalized before publication"));
        assert_eq!(worker.durable.as_ref().unwrap().checkpoint_epoch, 0);
        candidate.finish_catch_up().unwrap();
        candidate.finish_catch_up().unwrap();
        let sealed_tail = candidate.catch_up(&worker).unwrap();
        assert_eq!(sealed_tail.captured_commit_epoch, worker.commit_epoch());
        assert_eq!(sealed_tail.entries, 1);
        candidate.finish_catch_up().unwrap();
        worker
            .publish_checkpoint_candidate_deferred_reclamation(&mut candidate, Some(1))
            .unwrap();
        candidate
            .reclaim_published_generations(&mut worker, &Default::default())
            .unwrap();
        assert!(worker.version_snapshot_pin.is_some());
        assert!(store.version_snapshot_pin.is_none());
        let retired = store.adopt_selected_checkpoint(worker, expected).unwrap();
        assert!(store.version_snapshot_pin.is_none());
        assert_eq!(store.commit_epoch(), 2);
        assert_eq!(store.node_count_for_label(None), 2);
        assert_eq!(reader.commit_epoch(), 1);
        assert_eq!(reader.node_count_for_label(None), 1);
        assert!(store
            .version_index
            .shares_storage_with(&retired.version_index));
        drop(retired);
        drop(candidate);
        drop(reader);
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
            .unwrap();
        store.reclaim_version_history();
        // The worker's capture at epoch 2 must not become a permanent live
        // writer pin. No snapshots remain, so every old stamp can retire.
        assert_eq!(store.version_snapshot_pins.oldest_epoch(), None);
        assert_eq!(store.version_index.retained_history_bytes(), 0);
        assert_eq!(store.version_index.len(), 0);
        drop(store);
        let recovered = GraphStore::open(&path, &mut catalog).unwrap();
        assert_eq!(recovered.commit_epoch(), 3);
        assert_eq!(recovered.node_count_for_label(None), 3);
        drop(recovered);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn selected_checkpoint_handoff_rejects_an_equal_epoch_from_another_store() {
    let left_path = unique_test_dir("selected_checkpoint_identity_left");
    let right_path = unique_test_dir("selected_checkpoint_identity_right");
    let mut left_catalog = Catalog::default();
    let mut right_catalog = Catalog::default();
    let mut left = GraphStore::open(&left_path, &mut left_catalog).unwrap();
    let mut right = GraphStore::open(&right_path, &mut right_catalog).unwrap();
    left.create_node(
        &mut left_catalog,
        "Memory",
        properties([("id", Value::Int(1))]),
    )
    .unwrap();
    right
        .create_node(
            &mut right_catalog,
            "Memory",
            properties([("id", Value::Int(2))]),
        )
        .unwrap();
    let reader = left.snapshot();
    let expected = left.checkpoint_source_identity().unwrap();
    right.checkpoint(&right_catalog).unwrap();
    let error = left.adopt_selected_checkpoint(right, expected).unwrap_err();
    assert!(matches!(error, HawDBError::StorageIntegrity(_)));
    assert!(left.ensure_usable().is_err());
    assert!(reader.ensure_usable().is_err());
    drop(reader);
    drop(left);
    let recovered = GraphStore::open(&left_path, &mut left_catalog).unwrap();
    let id = left_catalog.label_id("Memory").unwrap();
    let row = recovered.scan_nodes(Some(id)).next().unwrap();
    assert_eq!(row.properties.get("id"), Some(&Value::Int(1)));
    assert_eq!(recovered.commit_epoch(), 1);
    drop(recovered);
    std::fs::remove_dir_all(left_path).unwrap();
    std::fs::remove_dir_all(right_path).unwrap();
}

#[test]
fn checkpoint_candidate_rejects_lost_authoritative_tail_and_poisons_live_owners() {
    let path = unique_test_dir("checkpoint_candidate_source_tail_loss");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let base = store.checkpoint_source();
    let mut candidate = base
        .prepare_checkpoint_candidate(&catalog)
        .unwrap()
        .unwrap();
    drop(base);
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
        .unwrap();
    let reader = store.snapshot();
    let source = store.checkpoint_source();
    let wal_path = store.durable.as_ref().unwrap().wal_path.clone();
    let complete_wal = std::fs::read(&wal_path).unwrap();
    let manifest_path = store
        .durable
        .as_ref()
        .unwrap()
        .manifest_path()
        .to_path_buf();
    let manifest = std::fs::read(&manifest_path).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&wal_path)
        .unwrap()
        .set_len(complete_wal.len() as u64 - 1)
        .unwrap();
    let error = candidate.catch_up(&source).unwrap_err();
    assert!(matches!(error, HawDBError::StorageIntegrity(_)));
    assert!(store.ensure_usable().is_err());
    assert!(reader.ensure_usable().is_err());
    assert!(source.ensure_usable().is_err());
    assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
    assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 0);
    drop(candidate);
    drop(source);
    drop(reader);
    drop(store);
    // Restore only this fixture's known complete bytes, then use ordinary
    // recovery. Live handles stay poisoned until they are closed.
    std::fs::write(&wal_path, &complete_wal).unwrap();
    let recovered = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(recovered.commit_epoch(), 2);
    assert_eq!(recovered.node_count_for_label(None), 2);
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_candidate_keeps_only_post_snapshot_debt_age() {
    let path = unique_test_dir("checkpoint_candidate_suffix_age");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let old_debt = std::time::Instant::now() - std::time::Duration::from_secs(60);
    store.durable.as_mut().unwrap().wal_uncheckpointed_since = Some(old_debt);
    let source = store.checkpoint_source();
    let capture_floor = source.checkpoint_capture_started.unwrap();
    let mut candidate = source
        .prepare_checkpoint_candidate(&catalog)
        .unwrap()
        .unwrap();
    drop(source);
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
        .unwrap();
    let source = store.checkpoint_source();
    candidate.catch_up(&source).unwrap();
    drop(source);
    store
        .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
        .unwrap();
    assert_eq!(
        store.durable.as_ref().unwrap().wal_uncheckpointed_since,
        Some(capture_floor)
    );
    assert_ne!(
        store.durable.as_ref().unwrap().wal_uncheckpointed_since,
        Some(old_debt)
    );
    drop(candidate);
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
        .unwrap();
    assert_eq!(
        store.durable.as_ref().unwrap().wal_uncheckpointed_since,
        Some(capture_floor)
    );
    let source = store.checkpoint_source();
    let mut candidate = source
        .prepare_checkpoint_candidate(&catalog)
        .unwrap()
        .unwrap();
    drop(source);
    candidate.finish_catch_up().unwrap();
    store
        .publish_checkpoint_candidate_deferred_reclamation(&mut candidate, None)
        .unwrap();
    assert!(store
        .durable
        .as_ref()
        .unwrap()
        .wal_uncheckpointed_since
        .is_none());
    assert_eq!(store.checkpoint_debt_snapshot().unwrap().wal_age_millis, 0);
    drop(candidate);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_checkpoint_debt_age_does_not_reset_on_continuous_writes() {
    let path = unique_test_dir("wal_checkpoint_debt_age");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let started = std::time::Instant::now() - std::time::Duration::from_secs(60);
    store.durable.as_mut().unwrap().wal_uncheckpointed_since = Some(started);
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
        .unwrap();
    assert!(store.storage_pressure_snapshot(None).wal_age_millis >= 60_000);
    assert_eq!(
        store.durable.as_ref().unwrap().wal_uncheckpointed_since,
        Some(started)
    );
    store.checkpoint(&catalog).unwrap();
    assert_eq!(store.storage_pressure_snapshot(None).wal_age_millis, 0);
    assert!(store
        .durable
        .as_ref()
        .unwrap()
        .wal_uncheckpointed_since
        .is_none());
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_wal_tail_rejects_corruption_without_selecting_a_candidate() {
    let path = unique_test_dir("checkpoint_wal_tail_corruption");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let source = store.checkpoint_source();
    let prepared = source.prepare_checkpoint(&catalog).unwrap().unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
        .unwrap();
    let durable = store.durable.as_ref().unwrap();
    let manifest_path = path.join(super::MANIFEST_FILE);
    let manifest = std::fs::read(&manifest_path).unwrap();
    let mut bytes = std::fs::read(&durable.wal_path).unwrap();
    // The complete second record remains the same length but its payload
    // no longer matches the fragment checksum.
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&durable.wal_path, &bytes).unwrap();
    let error = store.prepare_checkpoint_wal_tail(&prepared).unwrap_err();
    assert!(matches!(error, HawDBError::StorageIntegrity(_)));
    assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
    assert_eq!(std::fs::read(&durable.wal_path).unwrap(), bytes);
    assert!(!path
        .join(format!("wal.{}.hawdb.tail.tmp", prepared.generation))
        .exists());
    drop(prepared);
    drop(source);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn writable_open_reclaims_abandoned_future_checkpoint_generation() {
    let path = unique_test_dir("abandoned_prepared_checkpoint");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let source = store.checkpoint_source();
    let prepared = source.prepare_checkpoint(&catalog).unwrap().unwrap();
    assert!(path.join("checkpoint.1.hawdb").exists());
    assert!(path.join("wal.1.hawdb").exists());
    assert!(path.join(".checkpoint.1.prepare").exists());
    drop(prepared);
    drop(source);
    drop(store);

    let mut recovered_catalog = Catalog::default();
    let recovered = GraphStore::open(&path, &mut recovered_catalog).unwrap();
    assert_eq!(recovered.commit_epoch(), 1);
    assert!(!path.join("checkpoint.1.hawdb").exists());
    assert!(!path.join("wal.1.hawdb").exists());
    assert!(!path.join(".checkpoint.1.prepare").exists());
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

/// Range, composite and full-text index reads on an out-of-core store return
/// canonical nodes moved into the delta by a write to an unindexed property
/// (#2019, #2040).
#[test]
fn out_of_core_index_reads_find_materialized_base_nodes() {
    let path = unique_test_dir("index_reads_materialized_base_nodes");
    let replay_config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    let composite_properties = vec!["key".to_string(), "rank".to_string()];
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay_config,
    )
    .unwrap();
    store
        .create_range_property_index(&mut catalog, "Memory", "rank")
        .unwrap();
    store
        .create_full_text_property_index(&mut catalog, "Memory", "content")
        .unwrap();
    store
        .create_composite_property_index(&mut catalog, "Memory", &composite_properties)
        .unwrap();
    let mut ids = Vec::new();
    for (key, rank, content) in [
        ("first", 10, "alpha beta"),
        ("second", 20, "beta gamma"),
        ("third", 30, "delta"),
    ] {
        ids.push(
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("key", Value::String(key.to_string())),
                        ("rank", Value::Int(rank)),
                        ("content", Value::String(content.to_string())),
                    ]),
                )
                .unwrap(),
        );
    }
    store.checkpoint(&catalog).unwrap();
    assert!(store.is_out_of_core());
    let label_id = catalog.label_id("Memory").unwrap();
    // Moves every canonical node into the delta without touching an index key.
    store
        .set_node_property(&mut catalog, "Memory", None, "other", Value::Int(1))
        .unwrap();

    let mut range_ids = Vec::new();
    store
        .visit_nodes_by_property_range_owned(
            label_id,
            "rank",
            Some(&(Value::Int(15), true)),
            Some(&(Value::Int(25), true)),
            |node| {
                range_ids.push(node.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    assert_eq!(range_ids, vec![ids[1]], "range");

    let mut composite_range_ids = Vec::new();
    store
        .visit_nodes_by_composite_range_owned(
            label_id,
            &hawdb_core::access::CompositeRangeSeek {
                index_properties: composite_properties.clone(),
                equality_prefix: vec![("key".to_string(), Value::String("second".to_string()))],
                range_property: "rank".to_string(),
                lower: Some((Value::Int(0), true)),
                upper: None,
            },
            |node| {
                composite_range_ids.push(node.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    assert_eq!(composite_range_ids, vec![ids[1]], "composite range");

    let mut composite_ids = Vec::new();
    store
        .visit_nodes_by_composite_property_owned(
            label_id,
            &[
                ("key".to_string(), Value::String("second".to_string())),
                ("rank".to_string(), Value::Int(20)),
            ],
            |node| {
                composite_ids.push(node.id);
                GraphScanControl::Continue
            },
        )
        .unwrap();
    assert_eq!(composite_ids, vec![ids[1]], "composite equality");

    let mut full_text_ids = Vec::new();
    store
        .visit_nodes_by_full_text_property_owned(label_id, "content", "beta", |node| {
            full_text_ids.push(node.id);
            GraphScanControl::Continue
        })
        .unwrap();
    full_text_ids.sort();
    assert_eq!(full_text_ids, vec![ids[0], ids[1]], "full text");
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
