// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::relational::{
    RelationalColumnSchema, RelationalInsertMode, RelationalRow, RelationalScalarType,
    RelationalTableSchema, RelationalTransaction, RelationalValue, RelationalWrite,
};

fn graph_files(root: &Path) -> BTreeSet<String> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| crate::artifact_files::parse_graph_generation_file(name).is_some())
        .collect()
}

#[test]
fn relational_only_checkpoint_carries_graph_files_across_generations_and_reopen() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-graph-bundle-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    for id in 0..100 {
        store
            .create_node(
                &mut catalog,
                "Note",
                BTreeMap::from([("id".into(), Value::Int(id))]),
            )
            .unwrap();
    }
    store.checkpoint(&catalog).unwrap();
    let baseline = graph_files(&path);
    assert!(!baseline.is_empty());
    let graph_epoch = store.graph_change_commit_epoch().unwrap();
    let baseline_bytes = baseline
        .iter()
        .map(|name| (name.clone(), fs::read(path.join(name)).unwrap()))
        .collect::<BTreeMap<_, _>>();
    #[cfg(unix)]
    let baseline_inodes = {
        use std::os::unix::fs::MetadataExt;
        baseline
            .iter()
            .map(|name| (name.clone(), fs::metadata(path.join(name)).unwrap().ino()))
            .collect::<BTreeMap<_, _>>()
    };
    let table = RelationalTableSchema {
        materialized_view: None,
        name: "messages".into(),
        columns: vec![RelationalColumnSchema {
            name: "body".into(),
            scalar_type: RelationalScalarType::Text,
            nullable: false,
            default: None,
        }],
        primary_key: vec!["body".into()],
        unique_constraints: vec![],
        foreign_keys: vec![],
        indexes: vec![],
    };
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(table)],
            },
        )
        .unwrap();
    for round in 0..4 {
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "messages".into(),
                        rows: vec![RelationalRow::new(vec![RelationalValue::Text(format!(
                            "body-{round}"
                        ))])],
                        mode: RelationalInsertMode::Error,
                    }],
                },
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        assert_eq!(store.graph_change_commit_epoch().unwrap(), graph_epoch);
        let manifest =
            crate::durable_manifest::DurableManifest::load(&path.join("manifest.hawdb")).unwrap();
        assert_eq!(manifest.graph_generation(), Some(1));
        assert_eq!(manifest.graph_source_commit_epoch(), graph_epoch);
        assert!(manifest.checkpoint_epoch > manifest.graph_generation().unwrap());
        for (name, bytes) in &baseline_bytes {
            assert_eq!(fs::read(path.join(name)).unwrap(), *bytes);
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                assert_eq!(
                    fs::metadata(path.join(name)).unwrap().ino(),
                    baseline_inodes[name]
                );
            }
        }
        assert_eq!(
            graph_files(&path),
            baseline,
            "relational-only round{round} must not create or replace graph generations"
        );
    }
    store.scrub_storage().unwrap();
    let backup = path.with_extension("backup");
    let restored = path.with_extension("restore");
    store.backup_to(&catalog, &backup).unwrap();
    crate::store::backup::restore_storage_backup(&backup, &restored).unwrap();
    let mut restored_catalog = Catalog::default();
    let restored_store = GraphStore::open(&restored, &mut restored_catalog).unwrap();
    assert_eq!(restored_store.node_count_for_label(None), 100);
    assert_eq!(restored_store.relational_state().row_count("messages"), 4);
    drop(restored_store);
    fs::remove_dir_all(backup).unwrap();
    fs::remove_dir_all(restored).unwrap();
    drop(store);
    let mut reopened = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(reopened.node_count_for_label(None), 100);
    assert_eq!(reopened.relational_state().row_count("messages"), 4);
    reopened
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".into(),
                    rows: vec![RelationalRow::new(vec![RelationalValue::Text(
                        "after-reopen".into(),
                    )])],
                    mode: RelationalInsertMode::Error,
                }],
            },
        )
        .unwrap();
    reopened.checkpoint(&catalog).unwrap();
    assert_eq!(reopened.relational_state().row_count("messages"), 5);
    assert_eq!(graph_files(&path), baseline);
    for (name, bytes) in &baseline_bytes {
        assert_eq!(fs::read(path.join(name)).unwrap(), *bytes);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                fs::metadata(path.join(name)).unwrap().ino(),
                baseline_inodes[name]
            );
        }
    }
    drop(reopened);
    fs::remove_dir_all(path).unwrap();
}

fn assert_projection_debt(store: &GraphStore, has_debt: bool) {
    let pressure = store.storage_pressure_snapshot(None);
    assert_eq!(
        pressure.projection_debt_operations > 0,
        has_debt,
        "{pressure:?}"
    );
    assert_eq!(
        pressure
            .reason_codes
            .contains(&crate::pressure::StoragePressureReasonCode::ProjectionDebt),
        has_debt
    );
}

#[test]
fn carried_projection_debt_tracks_graph_changes_in_live_and_detached_views() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-graph-debt-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::SyncOnEveryWrite,
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..Default::default()
        },
    )
    .unwrap();
    store
        .create_node(&mut catalog, "Note", BTreeMap::new())
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                    materialized_view: None,
                    name: "debt_rows".into(),
                    columns: vec![RelationalColumnSchema {
                        name: "id".into(),
                        scalar_type: RelationalScalarType::BigInt,
                        nullable: false,
                        default: None,
                    }],
                    primary_key: vec!["id".into()],
                    unique_constraints: vec![],
                    foreign_keys: vec![],
                    indexes: vec![],
                })],
            },
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    assert!(store.is_out_of_core());
    assert!(store.persistent_property_projection.is_some());
    let manifest =
        crate::durable_manifest::DurableManifest::load(&path.join("manifest.hawdb")).unwrap();
    assert!(manifest.graph_generation().unwrap() < manifest.checkpoint_epoch);
    assert_projection_debt(&store, false);
    assert_projection_debt(&store.snapshot(), false);
    drop(store);
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::SyncOnEveryWrite,
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..Default::default()
        },
    )
    .unwrap();
    assert_projection_debt(&store, false);
    assert_projection_debt(&store.snapshot(), false);
    store
        .create_node(&mut catalog, "Note", BTreeMap::new())
        .unwrap();
    assert_projection_debt(&store, true);
    let dirty_snapshot = store.snapshot();
    assert_projection_debt(&dirty_snapshot, true);
    store.checkpoint(&catalog).unwrap();
    assert_projection_debt(&store, false);
    assert_projection_debt(&dirty_snapshot, true);
    drop(dirty_snapshot);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn graph_wal_replay_and_non_wal_catalog_changes_force_a_new_bundle() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-graph-fences-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Note",
            BTreeMap::from([("id".into(), Value::Int(1))]),
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    let original = store.canonical_segment_manifest().unwrap().generation;
    store.checkpoint(&catalog).unwrap();
    assert_eq!(
        store.canonical_segment_manifest().unwrap().generation,
        original
    );
    // Direct Catalog edits do not advance the WAL graph fence.
    let fence = store.graph_change_commit_epoch().unwrap();
    catalog.get_or_create_label("NewSchema");
    store.checkpoint(&catalog).unwrap();
    let schema_generation = store.canonical_segment_manifest().unwrap().generation;
    assert_ne!(schema_generation, original);
    assert_eq!(store.graph_change_commit_epoch().unwrap(), fence);
    store
        .create_node(
            &mut catalog,
            "Note",
            BTreeMap::from([("id".into(), Value::Int(2))]),
        )
        .unwrap();
    drop(store); // replayed graph changes must also disable carry
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store.checkpoint(&catalog).unwrap();
    assert_ne!(
        store.canonical_segment_manifest().unwrap().generation,
        schema_generation
    );
    assert_eq!(store.node_count_for_label(None), 2);
    assert!(catalog.label_id("NewSchema").is_some());
    store.scrub_storage().unwrap();
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn carried_candidate_cancellation_and_graph_catch_up_keep_the_base_complete() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-graph-candidate-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Note",
            BTreeMap::from([("id".into(), Value::Int(1))]),
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    store.checkpoint(&catalog).unwrap();
    let base_files = graph_files(&path);
    let source = store.checkpoint_source();
    let candidate = source
        .prepare_checkpoint_candidate(&catalog)
        .unwrap()
        .unwrap();
    drop(candidate); // private checkpoint cleanup must not remove carried G
    assert_eq!(graph_files(&path), base_files);
    let mut candidate = source
        .prepare_checkpoint_candidate(&catalog)
        .unwrap()
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Note",
            BTreeMap::from([("id".into(), Value::Int(2))]),
        )
        .unwrap();
    candidate.catch_up(&store).unwrap();
    store
        .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
        .unwrap();
    drop(candidate);
    assert_eq!(store.node_count_for_label(None), 2);
    // The candidate's canonical base is still old G; new graph data lives in
    // its replay suffix and must force the following checkpoint to rebuild.
    assert_eq!(store.canonical_segment_manifest().unwrap().generation.0, 1);
    drop(source);
    store.checkpoint(&catalog).unwrap();
    assert!(store.canonical_segment_manifest().unwrap().generation.0 > 1);
    drop(store);
    let reopened = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(reopened.node_count_for_label(None), 2);
    drop(reopened);
    fs::remove_dir_all(path).unwrap();
}

fn adjacency_fixture(
    mode: StorageResidencyMode,
) -> (PathBuf, Catalog, GraphStore, NodeId, NodeId, RelId) {
    let path = std::env::temp_dir().join(format!(
        "hawdb-adjacency-carry-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::SyncOnEveryWrite,
        WalReplayConfig {
            residency_mode: mode,
            ..Default::default()
        },
    )
    .unwrap();
    let first = store
        .create_node(
            &mut catalog,
            "Note",
            BTreeMap::from([("value".into(), Value::Int(0))]),
        )
        .unwrap();
    let second = store
        .create_node(
            &mut catalog,
            "Note",
            BTreeMap::from([("value".into(), Value::Int(0))]),
        )
        .unwrap();
    let relationship = store
        .create_relationship(
            &mut catalog,
            first,
            second,
            "LINK",
            BTreeMap::from([("weight".into(), Value::Int(0))]),
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    (path, catalog, store, first, second, relationship)
}

fn adjacency_binding(
    path: &Path,
) -> hawdb_storage::canonical_adjacency::CanonicalAdjacencyGenerationArtifacts {
    crate::durable_manifest::DurableManifest::load(&path.join("manifest.hawdb"))
        .unwrap()
        .canonical_adjacency_generation_artifacts
        .unwrap()
}

fn change_relationship_property(store: &mut GraphStore, catalog: &mut Catalog, weight: i64) {
    store
        .set_relationship_property(
            catalog,
            crate::mutation::RelationshipPropertyUpdate {
                source_label: "Note".into(),
                filter: None,
                rel_type: "LINK".into(),
                target_label: "Note".into(),
                target_filter: None,
                rel_filter: None,
                property: "weight".into(),
                value: Value::Int(weight),
            },
        )
        .unwrap();
}

#[test]
fn node_property_checkpoint_carries_adjacency_across_reopen_backup_and_scrub() {
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let (path, mut catalog, mut store, first, _, relationship) = adjacency_fixture(mode);
        let binding = adjacency_binding(&path);
        let files = graph_files(&path)
            .into_iter()
            .filter(|name| crate::artifact_files::parse_adjacency_generation_file(name).is_some())
            .map(|name| {
                let bytes = fs::read(path.join(&name)).unwrap();
                (name, bytes)
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(files.len(), 3);
        for round in 1..5 {
            store
                .set_node_property(&mut catalog, "Note", None, "value", Value::Int(round))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
            assert_eq!(adjacency_binding(&path), binding);
            assert_eq!(
                store.node_owned(first).unwrap().unwrap().properties["value"],
                Value::Int(round)
            );
            assert_eq!(
                store
                    .relationship_owned(relationship)
                    .unwrap()
                    .unwrap()
                    .properties["weight"],
                Value::Int(0)
            );
            for (name, bytes) in &files {
                assert_eq!(fs::read(path.join(name)).unwrap(), *bytes);
            }
            store.scrub_storage().unwrap();
            drop(store);
            store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::SyncOnEveryWrite,
                WalReplayConfig {
                    residency_mode: mode,
                    ..Default::default()
                },
            )
            .unwrap();
        }
        let backup = path.with_extension("backup");
        let restored = path.with_extension("restored");
        store.backup_to(&catalog, &backup).unwrap();
        crate::store::backup::restore_storage_backup(&backup, &restored).unwrap();
        let mut restored_catalog = Catalog::default();
        let mut restored_store = GraphStore::open(&restored, &mut restored_catalog).unwrap();
        assert_eq!(
            restored_store
                .node_owned(first)
                .unwrap()
                .unwrap()
                .properties["value"],
            Value::Int(4)
        );
        restored_store.scrub_storage().unwrap();
        drop(restored_store);
        // Inline relationship properties must invalidate even without topology changes.
        change_relationship_property(&mut store, &mut catalog, 9);
        store.checkpoint(&catalog).unwrap();
        assert_ne!(adjacency_binding(&path).generation, binding.generation);
        assert_eq!(
            store
                .relationship_owned(relationship)
                .unwrap()
                .unwrap()
                .properties["weight"],
            Value::Int(9)
        );
        drop(store);
        fs::remove_dir_all(path).unwrap();
        fs::remove_dir_all(backup).unwrap();
        fs::remove_dir_all(restored).unwrap();
    }
}

#[test]
fn carried_adjacency_reader_pin_survives_rebuilds_then_reclaims_only_its_family() {
    let (path, mut catalog, mut store, _, _, _) =
        adjacency_fixture(StorageResidencyMode::OutOfCore);
    store
        .set_node_property(&mut catalog, "Note", None, "value", Value::Int(1))
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    let view = store.published_read_view();
    let adjacency = view.adjacency_generation().unwrap().0;
    let canonical = view.graph_generation().unwrap().0;
    assert_ne!(adjacency, canonical);
    let pins = crate::read_view::PhysicalGenerationPins::from_views([view]);
    for weight in 1..5 {
        change_relationship_property(&mut store, &mut catalog, weight);
        let prepared = store.prepare_checkpoint(&catalog).unwrap().unwrap();
        store
            .publish_prepared_checkpoint_with_reader_generations(
                prepared,
                Some(view.visible_commit_epoch()),
                &pins,
                None,
            )
            .unwrap();
        assert!(path
            .join(canonical_adjacency_artifact_generation_file(adjacency))
            .exists());
    }
    // Retaining adjacency A never retains unrelated canonical records at A.
    assert!(!path
        .join(canonical_artifact_generation_file(adjacency))
        .exists());
    let prepared = store.prepare_checkpoint(&catalog).unwrap().unwrap();
    store
        .publish_prepared_checkpoint_with_reader_generations(
            prepared,
            None,
            &Default::default(),
            None,
        )
        .unwrap();
    assert!(!path
        .join(canonical_adjacency_artifact_generation_file(adjacency))
        .exists());
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn candidate_adjacency_carry_preserves_relationship_suffix_invalidation() {
    let (path, mut catalog, mut store, first, _, relationship) =
        adjacency_fixture(StorageResidencyMode::OutOfCore);
    let binding = adjacency_binding(&path);
    store
        .set_node_property(&mut catalog, "Note", None, "value", Value::Int(1))
        .unwrap();
    let source = store.checkpoint_source();
    let mut candidate = source
        .prepare_checkpoint_candidate(&catalog)
        .unwrap()
        .unwrap();
    change_relationship_property(&mut store, &mut catalog, 8);
    candidate.catch_up(&store).unwrap();
    store
        .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
        .unwrap();
    assert_eq!(adjacency_binding(&path), binding);
    assert_eq!(
        store.node_owned(first).unwrap().unwrap().properties["value"],
        Value::Int(1)
    );
    assert_eq!(
        store
            .relationship_owned(relationship)
            .unwrap()
            .unwrap()
            .properties["weight"],
        Value::Int(8)
    );
    drop(candidate);
    drop(source);
    drop(store);
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(
        store
            .relationship_owned(relationship)
            .unwrap()
            .unwrap()
            .properties["weight"],
        Value::Int(8)
    );
    store.checkpoint(&catalog).unwrap();
    assert_ne!(adjacency_binding(&path).generation, binding.generation);
    store.scrub_storage().unwrap();
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn adjacency_wal_classification_recurses_and_rejects_inline_relationship_updates() {
    let node = WalOp::SetNodeProperty {
        id: NodeId(1),
        property: "value".into(),
        value: Value::Int(2),
    };
    assert!(node.changes_graph());
    assert!(!node.changes_adjacency());
    let graph_only = WalOp::Batch(vec![
        WalOp::Batch(vec![node]),
        WalOp::MarkInitialImportSource {
            source_fingerprint: "fixture".into(),
        },
    ]);
    assert!(!graph_only.changes_adjacency());
    let relationship = WalOp::SetRelationshipProperty {
        id: RelId(1),
        property: "weight".into(),
        value: Value::Int(3),
    };
    assert!(relationship.changes_adjacency());
    assert!(WalOp::Batch(vec![graph_only, WalOp::Batch(vec![relationship])]).changes_adjacency());
}

#[test]
fn adjacency_carry_refuses_catalog_and_nondefault_build_configuration() {
    let (path, mut catalog, mut store, _, _, _) =
        adjacency_fixture(StorageResidencyMode::OutOfCore);
    let original = adjacency_binding(&path);
    store
        .set_node_property(&mut catalog, "Note", None, "value", Value::Int(1))
        .unwrap();
    catalog.get_or_create_label("DirectSchemaEdit");
    store.checkpoint(&catalog).unwrap();
    let after_catalog = adjacency_binding(&path);
    assert_ne!(after_catalog.generation, original.generation);
    let mut config = DerivedArtifactBuildConfig::default();
    config.adjacency.target_block_bytes = std::num::NonZeroU64::new(32 * 1024).unwrap();
    store
        .checkpoint_with_reader_epoch_and_build_config(&catalog, None, config)
        .unwrap();
    assert_ne!(
        adjacency_binding(&path).generation,
        after_catalog.generation
    );
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn mixed_node_property_and_relational_wal_replays_before_adjacency_carry() {
    let (path, mut catalog, mut store, first, _, _) =
        adjacency_fixture(StorageResidencyMode::OutOfCore);
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                    materialized_view: None,
                    name: "mixed_rows".into(),
                    columns: vec![RelationalColumnSchema {
                        name: "id".into(),
                        scalar_type: RelationalScalarType::BigInt,
                        nullable: false,
                        default: None,
                    }],
                    primary_key: vec!["id".into()],
                    unique_constraints: vec![],
                    foreign_keys: vec![],
                    indexes: vec![],
                })],
            },
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    let binding = adjacency_binding(&path);
    let epoch = store.commit_epoch();
    store
        .commit_mutations_and_relational(
            &mut catalog,
            vec![GraphMutation::SetNodeProperty {
                label: "Note".into(),
                filter: None,
                property: "value".into(),
                value: Value::Int(7),
            }],
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "mixed_rows".into(),
                    rows: vec![RelationalRow::new(vec![RelationalValue::BigInt(7)])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            MutationLimits::default(),
        )
        .unwrap();
    assert_eq!(store.commit_epoch(), epoch + 1);
    drop(store);
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(store.commit_epoch(), epoch + 1);
    assert_eq!(
        store.node_owned(first).unwrap().unwrap().properties["value"],
        Value::Int(7)
    );
    assert_eq!(store.relational_state().row_count("mixed_rows"), 1);
    store.checkpoint(&catalog).unwrap();
    assert_eq!(adjacency_binding(&path), binding);
    store.scrub_storage().unwrap();
    drop(store);
    fs::remove_dir_all(path).unwrap();
}
