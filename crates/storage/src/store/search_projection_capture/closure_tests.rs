// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::num::NonZeroUsize;

fn limits() -> ProjectionFeedLimits {
    ProjectionFeedLimits {
        work: NonZeroUsize::new(10000).unwrap(),
        items: NonZeroUsize::new(100).unwrap(),
        bytes: NonZeroUsize::new(65536).unwrap(),
    }
}
fn seed(path: &Path, mode: StorageResidencyMode) -> (GraphStore, Catalog, NodeId) {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        path,
        &mut catalog,
        Default::default(),
        WalReplayConfig {
            residency_mode: mode,
            ..Default::default()
        },
    )
    .unwrap();
    store.create_node_table(&mut catalog, "Memory").unwrap();
    let id = store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("id".into(), Value::String("a".into()))]),
        )
        .unwrap();
    (store, catalog, id)
}
fn reopen(path: &Path, mode: StorageResidencyMode) -> (GraphStore, Catalog) {
    let mut catalog = Catalog::default();
    let store = GraphStore::open_with_durability_and_replay_config(
        path,
        &mut catalog,
        Default::default(),
        WalReplayConfig {
            residency_mode: mode,
            ..Default::default()
        },
    )
    .unwrap();
    (store, catalog)
}
fn assert_gap(store: &GraphStore, cursor: u64, floor: u64) {
    assert_eq!(store.search_projection_change_log_start_epoch(), floor);
    assert!(matches!(
        store.capture_projection_feed(cursor, limits()),
        Err(ProjectionFeedError::BehindFloor { .. })
    ));
}

#[test]
fn schema_gap_survives_live_wal_checkpoint_and_cold_reopen_without_changing_old_feeds() {
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = super::tests::directory();
        let (mut store, catalog, id) = seed(&path, mode);
        store.checkpoint(&catalog).unwrap();
        drop(store);
        let (mut store, mut catalog) = reopen(&path, mode);
        assert_eq!(
            store.is_out_of_core(),
            mode == StorageResidencyMode::OutOfCore
        );
        let before = store.commit_epoch();
        let mut old = store.capture_projection_feed(before - 1, limits()).unwrap();
        store
            .create_property_descriptor(
                &mut catalog,
                TableKind::Node,
                "Memory",
                "new_field",
                PropertyType::String,
                true,
            )
            .unwrap();
        let floor = store.commit_epoch();
        assert!(floor > before);
        assert_gap(&store, before, floor);
        store
            .set_node_property(
                &mut catalog,
                "Memory",
                None,
                "new_field",
                Value::String("updated".into()),
            )
            .unwrap();
        let end = store.commit_epoch();
        let mut old_nodes = Vec::new();
        let mut old_end = None;
        while !old.is_complete() {
            old.visit_next(limits(), |item| {
                match item {
                    ProjectionFeedItem::GraphNode { node_id, .. } => old_nodes.push(node_id),
                    ProjectionFeedItem::End { source_epoch } => old_end = Some(source_epoch),
                    _ => panic!("unexpected old source event"),
                };
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(old_nodes, [id.0]);
        assert_eq!(old_end, Some(before));
        drop(old);
        // The same retained log/floor must hold before checkpoint and after a
        // checkpoint replaces the WAL. Check ordinary open in both residencies.
        for checkpoint in [false, true] {
            if checkpoint {
                store.checkpoint(&catalog).unwrap();
            }
            drop(store);
            (store, catalog) = reopen(&path, mode);
            assert_gap(&store, before, floor);
            assert_eq!(store.commit_epoch(), end);
            assert_eq!(
                store.node_owned(id).unwrap().unwrap().properties["new_field"],
                Value::String("updated".into())
            );
            let mut cursor = store.capture_projection_feed(floor, limits()).unwrap();
            let mut nodes = Vec::new();
            let mut ends = Vec::new();
            while !cursor.is_complete() {
                cursor
                    .visit_next(limits(), |item| {
                        match item {
                            ProjectionFeedItem::GraphNode { node_id, .. } => nodes.push(node_id),
                            ProjectionFeedItem::End { source_epoch } => ends.push(source_epoch),
                            _ => panic!("unexpected point event after rebuild floor"),
                        };
                        Ok(())
                    })
                    .unwrap();
            }
            assert_eq!(nodes, [id.0]);
            assert_eq!(ends, [end]);
        }
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn direct_schema_wal_failures_never_publish_a_live_gap_or_partial_catalog() {
    for failure in [
        WalAppendFailure::PartialWrite,
        WalAppendFailure::Rollback,
        WalAppendFailure::Sync,
    ] {
        let path = super::tests::directory();
        let (mut store, mut catalog, _) = seed(&path, StorageResidencyMode::Materialized);
        let epoch = store.commit_epoch();
        let floor = store.search_projection_change_log_start_epoch();
        let changes = store.search_projection_changes_after(0);
        set_wal_append_failpoint(failure);
        assert!(store
            .create_property_descriptor(
                &mut catalog,
                TableKind::Node,
                "Memory",
                "new_field",
                PropertyType::String,
                true
            )
            .is_err());
        let table = catalog.table_id(TableKind::Node, "Memory").unwrap();
        assert!(catalog.property_descriptor_id(table, "new_field").is_none());
        assert_eq!(store.commit_epoch(), epoch);
        assert_eq!(store.search_projection_change_log_start_epoch(), floor);
        assert_eq!(store.search_projection_changes_after(0), changes);
        drop(store);
        let mut catalog = Catalog::default();
        match GraphStore::open(&path, &mut catalog) {
            Ok(recovered) => {
                let table = catalog.table_id(TableKind::Node, "Memory").unwrap();
                if catalog.property_descriptor_id(table, "new_field").is_some() {
                    assert!(recovered.commit_epoch() > epoch);
                    assert_gap(&recovered, epoch, recovered.commit_epoch());
                } else {
                    assert_eq!(recovered.commit_epoch(), epoch);
                    assert_eq!(recovered.search_projection_change_log_start_epoch(), floor);
                }
            }
            Err(error) => {
                assert!(failure == WalAppendFailure::Rollback);
                assert!(error
                    .to_string()
                    .contains("strict WAL recovery rejected torn tail"));
                assert!(std::fs::read_dir(&path).unwrap().next().is_some());
            }
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn direct_relationship_path_captures_before_ack_and_on_replay() {
    assert_direct_capture(false);
}
#[test]
fn connected_node_path_captures_before_ack_and_on_replay() {
    assert_direct_capture(true);
}
fn assert_direct_capture(connected: bool) {
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = super::tests::directory();
        let (mut store, mut catalog, id) = seed(&path, mode);
        let label = store
            .create_node(
                &mut catalog,
                "Label",
                BTreeMap::from([("name".into(), Value::String("tag".into()))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        drop(store);
        (store, catalog) = reopen(&path, mode);
        assert_eq!(
            store.is_out_of_core(),
            mode == StorageResidencyMode::OutOfCore
        );
        let start = store.commit_epoch();
        let expected = if connected {
            let (a, _, b) = store
                .create_connected_nodes(
                    &mut catalog,
                    ConnectedNodesCreate {
                        source_label: "Memory".into(),
                        source_properties: BTreeMap::from([(
                            "id".into(),
                            Value::String("new".into()),
                        )]),
                        rel_type: "HAS_LABEL".into(),
                        rel_properties: Default::default(),
                        target_label: "Label".into(),
                        target_properties: BTreeMap::from([(
                            "name".into(),
                            Value::String("new tag".into()),
                        )]),
                    },
                )
                .unwrap();
            vec![a.0, b.0]
        } else {
            store
                .create_relationship(&mut catalog, id, label, "HAS_LABEL", Default::default())
                .unwrap();
            vec![id.0]
        };
        let changes = store.search_projection_changes_after(start);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].upsert_node_ids, expected);
        let end = store.commit_epoch();
        drop(store);
        let (recovered, _) = reopen(&path, StorageResidencyMode::OutOfCore);
        assert_eq!(recovered.commit_epoch(), end);
        assert_eq!(recovered.search_projection_changes_after(start), changes);
        assert!(recovered.capture_projection_feed(start, limits()).is_ok());
        drop(recovered);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn existing_schema_noop_does_not_advance_a_floor_or_epoch() {
    let path = super::tests::directory();
    let (mut store, mut catalog, _) = seed(&path, StorageResidencyMode::Materialized);
    let epoch = store.commit_epoch();
    let floor = store.search_projection_change_log_start_epoch();
    let changes = store.search_projection_changes_after(0);
    store.create_node_label(&mut catalog, "Memory").unwrap();
    store.create_node_table(&mut catalog, "Memory").unwrap();
    assert_eq!(store.commit_epoch(), epoch);
    assert_eq!(store.search_projection_change_log_start_epoch(), floor);
    assert_eq!(store.search_projection_changes_after(0), changes);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn nested_schema_and_metadata_exhaustion_stage_a_gap_without_mutating_the_source() {
    let path = super::tests::directory();
    let (store, catalog, id) = seed(&path, StorageResidencyMode::Materialized);
    let epoch = store.commit_epoch();
    let floor = store.search_projection_change_log_start_epoch();
    let changes = store.search_projection_changes_after(0);
    let ops = [
        WalOp::SetNodeProperty {
            id,
            property: "id".into(),
            value: Value::String("uncommitted".into()),
        },
        WalOp::Batch(vec![WalOp::CreateProperty {
            table_kind: TableKind::Node,
            table: "Memory".into(),
            property: "new_field".into(),
            value_type: PropertyType::String,
            nullable: true,
        }]),
    ];
    assert!(
        matches!(store.prepare_search_projection_changes_for_ops(&catalog,epoch+1,&ops,None).unwrap(),PreparedSearchProjectionChange::Gap(next) if next==epoch+1)
    );
    assert_eq!(store.commit_epoch(), epoch);
    assert_eq!(store.search_projection_change_log_start_epoch(), floor);
    assert_eq!(store.search_projection_changes_after(0), changes);
    assert_eq!(
        store.node_owned(id).unwrap().unwrap().properties["id"],
        Value::String("a".into())
    );
    // Even empty groups consume metadata work, and deep batches cannot evade
    // classification merely because their eventual leaf is a narrow operation.
    let empties = vec![WalOp::Batch(vec![]); MAX_WORK];
    assert!(!projection_wal_requires_rebuild(&empties));
    let mut over = empties;
    over.push(WalOp::Batch(vec![]));
    assert!(projection_wal_requires_rebuild(&over));
    let mut deep = WalOp::DeleteNode { id };
    assert!(!projection_wal_requires_rebuild(std::slice::from_ref(
        &deep
    )));
    for _ in 0..=MAX_DEPTH {
        deep = WalOp::Batch(vec![deep]);
    }
    assert!(projection_wal_requires_rebuild(&[deep]));
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn memory_only_and_direct_single_schema_commits_obey_the_same_gap_boundary() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    store.create_node_table(&mut catalog, "Memory").unwrap();
    assert_eq!(
        store.search_projection_change_log_start_epoch(),
        store.commit_epoch()
    );
    let before = store.commit_epoch();
    let op = WalOp::CreateNodeLabel {
        label: "New".into(),
    };
    store.append_durable_wal_single(op.clone()).unwrap();
    store.apply_wal_op(&mut catalog, op).unwrap();
    store.finish_non_relational_commit();
    assert_gap(&store, before, store.commit_epoch());
}

#[test]
fn checkpoint_replay_preserves_cold_preimages_and_schema_gaps() {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(16 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(
            RuntimeWorkRequest::background_maintenance(16 * 1024 * 1024).with_io_wave_slots(1),
        )
        .unwrap();
    let work = crate::background::CheckpointWorkContext::new(
        permit.bind_task_context(RuntimeTaskContext::default()),
    );
    let path = super::tests::directory();
    let (mut store, catalog, id) = seed(&path, StorageResidencyMode::OutOfCore);
    store.checkpoint(&catalog).unwrap();
    drop(store);
    let (mut store, mut catalog) = reopen(&path, StorageResidencyMode::OutOfCore);
    assert!(!store.nodes.contains_key(&id));
    let before = store.commit_epoch();
    store
        .apply_replayed_checkpoint_wal_transaction(
            &mut catalog,
            WalOp::SetNodeProperty {
                id,
                property: "id".into(),
                value: Value::String("renamed".into()),
            },
            &work,
        )
        .unwrap();
    let changes = store.search_projection_changes_after(before);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].upsert_node_ids, [id.0]);
    assert_eq!(changes[0].delete_document_ids, ["memory:a"]);
    let before_schema = store.commit_epoch();
    store
        .apply_replayed_checkpoint_wal_transaction(
            &mut catalog,
            WalOp::CreateNodeLabel {
                label: "NewLabel".into(),
            },
            &work,
        )
        .unwrap();
    assert_gap(&store, before_schema, store.commit_epoch());
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
