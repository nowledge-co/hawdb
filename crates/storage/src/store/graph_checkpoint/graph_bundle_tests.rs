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
    let reopened = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(reopened.node_count_for_label(None), 100);
    assert_eq!(reopened.relational_state().row_count("messages"), 4);
    drop(reopened);
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
