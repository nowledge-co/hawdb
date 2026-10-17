// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;

fn owner_record(store: &GraphStore) -> &CapturedProjectionChange {
    store.search_projection_graph_changes.last().unwrap()
}
fn expected(id: NodeId, label: LabelId, before_id: OwnerIdV1) -> Vec<OwnerEffectV1> {
    vec![OwnerEffectV1 {
        node_id: id.0,
        before: OwnerBeforeV1::Present {
            labels: vec![label.0],
            id: before_id,
        },
    }]
}

#[test]
fn owner_only_thread_delete_survives_wal_checkpoint_and_both_residencies() {
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        for checkpoint_deleted in [false, true] {
            let path = super::tests::directory();
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                Default::default(),
                WalReplayConfig {
                    residency_mode: mode,
                    ..Default::default()
                },
            )
            .unwrap();
            store.create_node_table(&mut catalog, "Thread").unwrap();
            let id = store
                .create_node(
                    &mut catalog,
                    "Thread",
                    BTreeMap::from([("id".into(), Value::String("old-thread".into()))]),
                )
                .unwrap();
            let label = catalog.label_id("Thread").unwrap();
            store.checkpoint(&catalog).unwrap();
            drop(store);
            store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                Default::default(),
                WalReplayConfig {
                    residency_mode: mode,
                    ..Default::default()
                },
            )
            .unwrap();
            let c = store.snapshot();
            let cursor = store.commit_epoch();
            store
                .delete_nodes(&mut catalog, "Thread", None, false)
                .unwrap();
            let epoch = store.commit_epoch();
            let effects = expected(id, label, OwnerIdV1::String("old-thread".into()));
            assert_eq!(owner_record(&store).owners_v1.as_ref().unwrap(), &effects);
            assert_eq!(owner_record(&store).commit_epoch, epoch);
            assert!(owner_record(&store).upsert_node_ids.is_empty());
            assert!(owner_record(&store).delete_document_ids.is_empty());
            assert!(store.search_projection_changes_after(cursor).is_empty());
            assert_eq!(c.commit_epoch(), cursor);
            assert_ne!(owner_record(&c).owners_v1, owner_record(&store).owners_v1);
            if checkpoint_deleted {
                store.checkpoint(&catalog).unwrap();
            }
            let floor = store.search_projection_graph_changes.owner_floor();
            drop(c);
            drop(store);
            let recovered = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                Default::default(),
                WalReplayConfig {
                    residency_mode: mode,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(recovered.commit_epoch(), epoch);
            assert_eq!(
                recovered.search_projection_graph_changes.owner_floor(),
                floor
            );
            assert_eq!(
                owner_record(&recovered).owners_v1.as_ref().unwrap(),
                &effects
            );
            assert!(recovered.search_projection_changes_after(cursor).is_empty());
            drop(recovered);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn owner_identity_is_transaction_preimage_not_an_intermediate_rename() {
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let id = store
        .create_node(
            &mut catalog,
            "Thread",
            BTreeMap::from([("id".into(), Value::String("old".into()))]),
        )
        .unwrap();
    let label = catalog.label_id("Thread").unwrap();
    for ops in [
        vec![
            WalOp::SetNodeProperty {
                id,
                property: "id".into(),
                value: Value::String("middle".into()),
            },
            WalOp::DeleteNode { id },
        ],
        vec![
            WalOp::SetNodeProperty {
                id,
                property: "body".into(),
                value: Value::String("body".into()),
            },
            WalOp::SetNodeProperty {
                id,
                property: "id".into(),
                value: Value::String("final".into()),
            },
        ],
    ] {
        let PreparedSearchProjectionChange::Complete(Some(change)) = store
            .prepare_search_projection_changes_for_ops(
                &catalog,
                store.commit_epoch() + 1,
                &ops,
                None,
            )
            .unwrap()
        else {
            panic!("expected complete capture");
        };
        assert_eq!(
            change.owners_v1.as_ref().unwrap(),
            &expected(id, label, OwnerIdV1::String("old".into()))
        );
        assert_eq!(change.upsert_node_ids, [id.0]);
    }
    store
        .set_node_property(
            &mut catalog,
            "Thread",
            None,
            "body",
            Value::String("x".into()),
        )
        .unwrap();
    assert_eq!(
        owner_record(&store).owners_v1.as_ref().unwrap()[0].before,
        OwnerBeforeV1::NotRecorded
    );
}

#[test]
fn owner_created_deleted_and_unsupported_ids_are_explicit() {
    for value in [
        None,
        Some(Value::Int(7)),
        Some(Value::String("x\t\nλ".into())),
    ] {
        let mut store = GraphStore::in_memory();
        let mut catalog = Catalog::default();
        let props = value
            .clone()
            .map(|value| BTreeMap::from([("id".into(), value)]))
            .unwrap_or_default();
        let id = store.create_node(&mut catalog, "Thread", props).unwrap();
        assert_eq!(
            owner_record(&store).owners_v1.as_ref().unwrap()[0].before,
            OwnerBeforeV1::Absent
        );
        store
            .delete_nodes(&mut catalog, "Thread", None, false)
            .unwrap();
        let before_id = match value {
            None => OwnerIdV1::Absent,
            Some(Value::String(id)) => OwnerIdV1::String(id),
            _ => OwnerIdV1::Unsupported,
        };
        assert_eq!(
            owner_record(&store).owners_v1.as_ref().unwrap(),
            &expected(id, catalog.label_id("Thread").unwrap(), before_id)
        );
    }
}

#[test]
fn owner_floors_advance_on_gap_trim_and_do_not_follow_live_mutation() {
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    store
        .create_node(&mut catalog, "Thread", BTreeMap::new())
        .unwrap();
    let snapshot = store.snapshot();
    let c = store.commit_epoch();
    let floor = snapshot.search_projection_graph_changes.owner_floor();
    store.set_max_search_projection_change_log_entries(Some(0));
    assert!(store.search_projection_graph_changes.owner_floor() >= c);
    assert_eq!(
        snapshot.search_projection_graph_changes.owner_floor(),
        floor
    );
    store.install_search_projection_change(PreparedSearchProjectionChange::Gap(c + 1));
    assert_eq!(store.search_projection_graph_changes.owner_floor(), c + 1);
    assert_eq!(
        snapshot.search_projection_graph_changes.owner_floor(),
        floor
    );
}

#[test]
fn all_relationship_effects_capture_endpoint_owners_without_legacy_upserts() {
    use crate::mutation::{GraphMutation, MutationLimits};
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = super::tests::directory();
        let mut catalog = Catalog::default();
        let config = WalReplayConfig {
            residency_mode: mode,
            ..Default::default()
        };
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            Default::default(),
            config,
        )
        .unwrap();
        let a = store
            .create_node(&mut catalog, "Parent", BTreeMap::new())
            .unwrap();
        let b = store
            .create_node(&mut catalog, "Child", BTreeMap::new())
            .unwrap();
        let c = store.commit_epoch();
        store
            .create_relationship(
                &mut catalog,
                a,
                b,
                "DEPENDS_ON",
                BTreeMap::from([("body".into(), Value::String("x".repeat(1024 * 1024)))]),
            )
            .unwrap();
        let first = store.commit_epoch();
        let assert_endpoints = |record: &CapturedProjectionChange| {
            let owners = record.owners_v1.as_ref().unwrap();
            assert_eq!(
                owners.iter().map(|owner| owner.node_id).collect::<Vec<_>>(),
                vec![a.0, b.0]
            );
            assert!(owners
                .iter()
                .all(|owner| owner.before == OwnerBeforeV1::NotRecorded));
            assert!(record.upsert_node_ids.is_empty());
            assert!(record.delete_document_ids.is_empty());
            assert!(record.estimated_retained_bytes() < 4096);
        };
        assert_endpoints(owner_record(&store));
        store.checkpoint(&catalog).unwrap();
        drop(store);
        store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            Default::default(),
            config,
        )
        .unwrap();
        if mode == StorageResidencyMode::OutOfCore {
            assert!(store.relationships.is_empty());
        }
        store
            .commit_mutations_with_limits(
                &mut catalog,
                vec![GraphMutation::SetRelationshipProperty {
                    source_label: "Parent".into(),
                    filter: None,
                    rel_type: "DEPENDS_ON".into(),
                    target_label: "Child".into(),
                    target_filter: None,
                    rel_filter: None,
                    property: "weight".into(),
                    value: Value::Int(2),
                }],
                MutationLimits::default(),
            )
            .unwrap();
        let second = store.commit_epoch();
        assert_endpoints(owner_record(&store));
        store
            .commit_mutations_with_limits(
                &mut catalog,
                vec![GraphMutation::DeleteRelationship {
                    source_label: "Parent".into(),
                    filter: None,
                    rel_type: "DEPENDS_ON".into(),
                    target_label: "Child".into(),
                    target_filter: None,
                    rel_filter: None,
                }],
                MutationLimits::default(),
            )
            .unwrap();
        let third = store.commit_epoch();
        assert_endpoints(owner_record(&store));
        store.checkpoint(&catalog).unwrap();
        drop(store);
        let store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            Default::default(),
            config,
        )
        .unwrap();
        assert_eq!(store.commit_epoch(), third);
        let expected = [first, second, third];
        let changes = store
            .search_projection_graph_changes
            .iter()
            .filter(|record| record.commit_epoch > c)
            .collect::<Vec<_>>();
        assert_eq!(
            changes
                .iter()
                .map(|record| record.commit_epoch)
                .collect::<Vec<_>>(),
            expected
        );
        for record in changes {
            assert_endpoints(record);
        }
        assert!(store.search_projection_changes_after(c).is_empty());
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn nested_new_endpoints_keep_birth_facts_and_unknown_relationship_requires_rebuild() {
    let store = GraphStore::in_memory();
    let catalog = Catalog::default();
    let ops = vec![WalOp::Batch(vec![
        WalOp::CreateNode {
            id: NodeId(3),
            label: "Parent".into(),
            properties: BTreeMap::new(),
        },
        WalOp::CreateNode {
            id: NodeId(4),
            label: "Child".into(),
            properties: BTreeMap::new(),
        },
        WalOp::CreateRelationship {
            id: RelId(1),
            source: NodeId(3),
            target: NodeId(4),
            rel_type: "ANY_TYPE".into(),
            properties: BTreeMap::new(),
        },
        WalOp::SetRelationshipProperty {
            id: RelId(1),
            property: "weight".into(),
            value: Value::Int(1),
        },
        WalOp::DeleteRelationship { id: RelId(1) },
    ])];
    let PreparedSearchProjectionChange::Complete(Some(record)) = store
        .prepare_search_projection_changes_for_ops(&catalog, 1, &ops, None)
        .unwrap()
    else {
        panic!("new endpoints must be captured")
    };
    assert_eq!(
        record
            .owners_v1
            .as_ref()
            .unwrap()
            .iter()
            .map(|owner| (owner.node_id, &owner.before))
            .collect::<Vec<_>>(),
        vec![(3, &OwnerBeforeV1::Absent), (4, &OwnerBeforeV1::Absent)]
    );
    let record = store
        .prepare_search_projection_changes_for_ops(
            &catalog,
            1,
            &[WalOp::SetRelationshipProperty {
                id: RelId(99),
                property: "weight".into(),
                value: Value::Int(1),
            }],
            None,
        )
        .unwrap();
    assert!(matches!(record, PreparedSearchProjectionChange::Gap(1)));
}

#[test]
fn label_relationship_property_capture_keeps_the_legacy_projection_unchanged() {
    use crate::mutation::{GraphMutation, MutationLimits};
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let a = store
        .create_node(&mut catalog, "Memory", BTreeMap::new())
        .unwrap();
    let b = store
        .create_node(&mut catalog, "Label", BTreeMap::new())
        .unwrap();
    store
        .create_relationship(&mut catalog, a, b, "HAS_LABEL", BTreeMap::new())
        .unwrap();
    let c = store.commit_epoch();
    store
        .commit_mutations_with_limits(
            &mut catalog,
            vec![GraphMutation::SetRelationshipProperty {
                source_label: "Memory".into(),
                filter: None,
                rel_type: "HAS_LABEL".into(),
                target_label: "Label".into(),
                target_filter: None,
                rel_filter: None,
                property: "weight".into(),
                value: Value::Int(1),
            }],
            MutationLimits::default(),
        )
        .unwrap();
    let record = owner_record(&store);
    assert_eq!(
        record
            .owners_v1
            .as_ref()
            .unwrap()
            .iter()
            .map(|owner| owner.node_id)
            .collect::<Vec<_>>(),
        vec![a.0, b.0]
    );
    assert!(record.upsert_node_ids.is_empty());
    assert!(record.delete_document_ids.is_empty());
    assert!(store.search_projection_changes_after(c).is_empty());
}

#[test]
fn repeated_relationship_owner_work_exhaustion_requires_rebuild() {
    let store = GraphStore::in_memory();
    let catalog = Catalog::default();
    let mut ops = vec![WalOp::CreateRelationship {
        id: RelId(1),
        source: NodeId(2),
        target: NodeId(3),
        rel_type: "ANY_TYPE".into(),
        properties: BTreeMap::new(),
    }];
    ops.extend((0..MAX_WORK / 4).map(|_| WalOp::SetRelationshipProperty {
        id: RelId(1),
        property: "weight".into(),
        value: Value::Int(1),
    }));
    let result = store
        .prepare_search_projection_changes_for_ops(&catalog, 1, &ops, None)
        .unwrap();
    assert!(matches!(result, PreparedSearchProjectionChange::Gap(1)));
}
