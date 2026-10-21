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
fn nested_schema_and_depth_exhaustion_stage_a_gap_without_mutating_the_source() {
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
    // Operation count alone does not invalidate a complete capture. Depth still
    // protects recursive WAL traversal, including otherwise narrow leaves.
    let empties = vec![WalOp::Batch(vec![]); 65_537];
    assert!(!projection_wal_requires_rebuild(&empties));
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
fn checkpoint_capture_observes_cancellation_within_one_work_group() {
    use crate::background::CheckpointWorkProbe;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    use std::sync::atomic::Ordering;

    for cancel_after in [1, 513, 1024] {
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(1),
            max_total_background_operations: Some(1),
            ..Default::default()
        });
        let probe = Arc::new(CheckpointWorkProbe::default());
        let mut budget = Budget {
            checkpoint: Some(probe.context(scheduler.clone())),
            until_checkpoint: 0,
            bytes: MAX_BYTES,
        };
        for _ in 0..cancel_after {
            assert!(budget.work().is_ok());
        }
        probe.cancellation.cancel();
        for _ in cancel_after..1024 {
            assert!(budget.work().is_ok());
        }
        assert!(matches!(
            budget.work(),
            Err(CaptureError::Storage(HawDBError::Storage(message)))
                if message == "checkpoint build stopped: cancelled"
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), 1);
        probe.assert_released(&scheduler);
    }
}

#[test]
fn canonical_preimage_refusals_do_not_poison_the_store() {
    let store = GraphStore::in_memory();
    let snapshot = store.snapshot();
    for error in crate::reader_failure::tests::refusals() {
        let error = canonical_segment_error(CanonicalSegmentError::Io(error));
        assert!(matches!(
            error,
            HawDBError::Storage(_)
                | HawDBError::FileDescriptors(_)
                | HawDBError::ResourceBudgetExceeded(_)
        ));
        store.poison_on_storage_error::<()>(&Err(error));
        store.ensure_usable().unwrap();
        snapshot.ensure_usable().unwrap();
    }
    assert!(matches!(
        canonical_segment_error(CanonicalSegmentError::Corrupt("damaged preimage".into())),
        HawDBError::StorageIntegrity(_)
    ));
}

#[test]
fn cold_capture_descriptor_exhaustion_preserves_the_writer_and_allows_retry() {
    for kind in [
        "node",
        "adjacency",
        "adjacency_payload",
        "relationship",
        "property_spill",
    ] {
        let path = super::tests::directory();
        let (mut store, mut catalog, id) = seed(&path, StorageResidencyMode::Materialized);
        if kind == "property_spill" {
            store
                .set_node_property(
                    &mut catalog,
                    "Memory",
                    None,
                    "id",
                    Value::String("a".repeat(2 * MAX_PREIMAGE_BYTES)),
                )
                .unwrap();
        }
        let label = store
            .create_node(
                &mut catalog,
                "Label",
                BTreeMap::from([("name".into(), Value::String("tag".into()))]),
            )
            .unwrap();
        let relationship = store
            .create_relationship(&mut catalog, id, label, "HAS_LABEL", BTreeMap::new())
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        drop(store);
        (store, catalog) = reopen(&path, StorageResidencyMode::OutOfCore);
        let op = match kind {
            "node" | "property_spill" => {
                if kind == "property_spill" {
                    // Warm only the encoded node and its descriptor path. The
                    // selected ID's spill remains cold for allocation preflight.
                    assert_eq!(
                        store
                            .canonical_base
                            .as_ref()
                            .unwrap()
                            .with_node_input(id, |_| Ok(()))
                            .unwrap(),
                        Some(())
                    );
                }
                WalOp::SetNodeProperty {
                    id,
                    property: "id".into(),
                    value: Value::String("new".into()),
                }
            }
            "adjacency" | "adjacency_payload" => {
                // Keep the label lookup hot so the refusal occurs in adjacency.
                let node = store.node_owned(label).unwrap().unwrap();
                store.nodes.insert(label, node);
                if kind == "adjacency_payload" {
                    // Separate descriptor-page refusal from payload refusal.
                    let reader = store.canonical_adjacency.as_ref().unwrap();
                    let kind = catalog.rel_type_id("HAS_LABEL").unwrap();
                    for direction in [AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming] {
                        reader
                            .estimate_endpoint_entries(label, direction, Some(kind))
                            .unwrap();
                    }
                }
                WalOp::SetNodeProperty {
                    id: label,
                    property: "name".into(),
                    value: Value::String("new".into()),
                }
            }
            "relationship" => WalOp::DeleteRelationship { id: relationship },
            _ => unreachable!(),
        };
        let snapshot = store.snapshot();
        let project = store
            .durable
            .as_ref()
            .unwrap()
            .project_file_descriptors()
            .clone();
        let metrics = project.metrics();
        // Reserving cached capacity evicts idle handles as well as exhausting
        // fresh-open capacity, so warm caches cannot bypass this negative control.
        let available = metrics.limit - metrics.open - metrics.reserved + metrics.cached_handles;
        let competitor = project.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let held = std::thread::spawn(move || {
            // Reservations belong to their thread. A same-thread reservation
            // would lend its capacity to the read instead of competing with it.
            let _reservation = competitor.reserve(available).unwrap();
            ready_tx.send(()).unwrap();
            let _ = release_rx.recv();
        });
        ready_rx.recv().unwrap();
        let lsn = store.durable.as_ref().unwrap().next_lsn;
        let epoch = store.commit_epoch();
        let floor = store.search_projection_change_log_start_epoch();
        let changes = store.search_projection_changes_after(0);
        let error = store
            .append_graph_wal_and_capture(&catalog, std::slice::from_ref(&op), false)
            .expect_err(kind);
        assert!(
            matches!(
                error,
                HawDBError::FileDescriptors(
                    hawdb_core::error::FileDescriptorError::BudgetExceeded { .. }
                )
            ),
            "{kind}: {error}"
        );
        assert_eq!(store.durable.as_ref().unwrap().next_lsn, lsn);
        assert_eq!(store.commit_epoch(), epoch);
        assert_eq!(store.search_projection_change_log_start_epoch(), floor);
        assert_eq!(store.search_projection_changes_after(0), changes);
        store.ensure_usable().unwrap();
        snapshot.ensure_usable().unwrap();
        assert!(
            !store.canonical_base.as_ref().unwrap().is_poisoned(),
            "{kind}"
        );
        assert!(
            !store.canonical_adjacency.as_ref().unwrap().is_poisoned(),
            "{kind}"
        );
        release_tx.send(()).unwrap();
        held.join().unwrap();
        store
            .append_graph_wal_and_capture(&catalog, std::slice::from_ref(&op), false)
            .unwrap();
        store.apply_wal_op(&mut catalog, op).unwrap();
        store.finish_non_relational_commit();
        assert_eq!(store.commit_epoch(), epoch + 1);
        let expected = store.search_projection_changes_after(epoch);
        if kind == "property_spill" {
            // Once I/O can proceed, the oversized preimage independently
            // requires a complete rebuild; the source commit must still retry.
            assert_gap(&store, epoch, epoch + 1);
            assert!(expected.is_empty());
        } else {
            assert_eq!(store.search_projection_change_log_start_epoch(), floor);
            assert_eq!(expected.len(), 1);
        }
        drop(snapshot);
        drop(store);
        drop(project);
        let (recovered, _) = reopen(&path, StorageResidencyMode::OutOfCore);
        assert_eq!(recovered.commit_epoch(), epoch + 1);
        assert_eq!(recovered.search_projection_changes_after(epoch), expected);
        if kind == "property_spill" {
            assert_gap(&recovered, epoch, epoch + 1);
        }
        drop(recovered);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn bulk_create_capture_has_no_operation_count_cap_and_survives_replay() {
    let path = super::tests::directory();
    let (mut store, mut catalog, _) = seed(&path, StorageResidencyMode::Materialized);
    let epoch = store.commit_epoch();
    let floor = store.search_projection_change_log_start_epoch();
    let count = 65_537;
    store
        .commit_mutations(
            &mut catalog,
            (0..count)
                .map(|i| GraphMutation::CreateNode {
                    label: "Memory".into(),
                    properties: BTreeMap::from([("id".into(), Value::String(format!("bulk-{i}")))]),
                })
                .collect(),
        )
        .unwrap();
    assert_eq!(store.commit_epoch(), epoch + 1);
    assert_eq!(store.search_projection_change_log_start_epoch(), floor);
    let changes = store.search_projection_changes_after(epoch);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].upsert_node_ids.len(), count);
    assert_eq!(changes[0].upsert_node_ids.first(), Some(&1));
    assert_eq!(changes[0].upsert_node_ids.last(), Some(&(count as u64)));
    drop(store);
    let (recovered, _) = reopen(&path, StorageResidencyMode::OutOfCore);
    assert_eq!(recovered.commit_epoch(), epoch + 1);
    assert_eq!(recovered.search_projection_change_log_start_epoch(), floor);
    assert_eq!(recovered.search_projection_changes_after(epoch), changes);
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn oversized_id_preimage_commits_an_explicit_gap_instead_of_partial_capture() {
    let path = super::tests::directory();
    let (mut store, mut catalog, _) = seed(&path, StorageResidencyMode::Materialized);
    store
        .set_node_property(
            &mut catalog,
            "Memory",
            None,
            "id",
            Value::String("a".repeat(MAX_PREIMAGE_BYTES)),
        )
        .unwrap();
    let before = store.commit_epoch();
    store
        .set_node_property(
            &mut catalog,
            "Memory",
            None,
            "id",
            Value::String("small".into()),
        )
        .unwrap();
    let floor = store.commit_epoch();
    assert_gap(&store, before, floor);
    assert!(store.search_projection_changes_after(0).is_empty());
    for checkpoint in [false, true] {
        if checkpoint {
            store.checkpoint(&catalog).unwrap();
        }
        drop(store);
        (store, catalog) = reopen(&path, StorageResidencyMode::OutOfCore);
        assert_gap(&store, before, floor);
        assert_eq!(
            store.node_owned(NodeId(0)).unwrap().unwrap().properties["id"],
            Value::String("small".into())
        );
    }
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
