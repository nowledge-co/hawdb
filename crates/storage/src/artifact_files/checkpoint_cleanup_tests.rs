// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn directory() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-temporaries-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

#[test]
fn reopen_cleanup_removes_only_known_checkpoint_temporary_files() {
    let root = directory();
    let owner = crate::ownership::DatabaseDirectoryLease::acquire(&root).unwrap();
    let families = [
        "checkpoint.{g}.hawdb",
        "relational.{g}.hawdb",
        "canonical.{g}.hawdb",
        "canonical.{g}.manifest.hawdb",
        "properties.{g}.hawdb",
        "properties.{g}.manifest.hawdb",
        "property-index.{g}.manifest.hawdb",
        "canonical-segment-descriptors-{g}.root.hawdb",
        "append-{g}.segment.hawdb",
        "append-{g}.manifest.hawdb",
    ];
    for family in families {
        for generation in [0, 6, 7, 8] {
            let name = family.replace("{g}", &generation.to_string());
            std::fs::write(root.join(format!("{name}.tmp")), b"interrupted write").unwrap();
            if generation <= 7 {
                std::fs::write(root.join(name), b"published data").unwrap();
            }
        }
    }
    let retained = [
        "manifest.hawdb",
        "manifest.hawdb.tmp",
        "unrelated.hawdb.tmp",
        "checkpoint.invalid.hawdb.tmp",
        "checkpoint.8.hawdb.tmp.backup",
        "wal.7.hawdb",
        "wal.7.hawdb.sync",
    ];
    for name in retained {
        std::fs::write(root.join(name), b"retained evidence").unwrap();
    }
    let unexpected_directory = root.join("checkpoint.9.hawdb.tmp");
    std::fs::create_dir(&unexpected_directory).unwrap();
    std::fs::write(unexpected_directory.join("evidence"), b"keep directory").unwrap();

    cleanup_abandoned_checkpoint_preparations(&root, 7).unwrap();
    // A second admission must be harmless, including after the first sweep
    // removed all abandoned candidates.
    cleanup_abandoned_checkpoint_preparations(&root, 7).unwrap();
    for family in families {
        for generation in [0, 6, 7, 8] {
            let name = family.replace("{g}", &generation.to_string());
            assert!(!root.join(format!("{name}.tmp")).exists(), "{name}");
            if generation <= 7 {
                assert_eq!(std::fs::read(root.join(name)).unwrap(), b"published data");
            }
        }
    }
    for name in retained {
        assert_eq!(
            std::fs::read(root.join(name)).unwrap(),
            b"retained evidence"
        );
    }
    assert_eq!(
        std::fs::read(unexpected_directory.join("evidence")).unwrap(),
        b"keep directory"
    );
    drop(owner);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn reopen_cleanup_preserves_temporary_symlinks_and_their_targets() {
    let root = directory();
    let target = root.join("published.hawdb");
    std::fs::write(&target, b"published data").unwrap();
    let link = root.join("checkpoint.8.hawdb.tmp");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    cleanup_abandoned_checkpoint_preparations(&root, 7).unwrap();
    assert!(std::fs::symlink_metadata(&link).unwrap().is_symlink());
    assert_eq!(std::fs::read(target).unwrap(), b"published data");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn legacy_store_cleans_crash_temporaries_only_on_writable_reopen() {
    use crate::config::{DurabilityPolicy, RecoveryMode, WalReplayConfig};
    use crate::schema::Catalog;
    use crate::store::GraphStore;
    use crate::Value;
    use std::collections::BTreeMap;

    let root = directory();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&root, &mut catalog).unwrap();
    store
        .create_node(&mut catalog, "Memory", BTreeMap::new())
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("id".into(), Value::Int(2))]),
        )
        .unwrap();
    let epoch = store.commit_epoch();
    drop(store);
    let manifest =
        crate::durable_manifest::DurableManifest::load(&root.join(MANIFEST_FILE)).unwrap();
    let generation = manifest.checkpoint_epoch.max(manifest.wal_generation) + 1;
    let names = [
        format!("checkpoint.{generation}.hawdb.tmp"),
        format!("append-{generation}.manifest.hawdb.tmp"),
        format!("properties.{generation}.manifest.hawdb.tmp"),
    ];
    for name in &names {
        std::fs::write(root.join(name), b"interrupted checkpoint").unwrap();
    }
    let published = manifest.checkpoint_path(&root);
    let before = std::fs::read(&published).unwrap();
    let read_only = GraphStore::open_read_only_with_durability_and_replay_config(
        &root,
        &mut catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            recovery_mode: RecoveryMode::Strict,
            ..WalReplayConfig::default()
        },
    )
    .unwrap();
    assert_eq!(read_only.commit_epoch(), epoch);
    assert_eq!(read_only.node_count_for_label(None), 2);
    for name in &names {
        assert_eq!(
            std::fs::read(root.join(name)).unwrap(),
            b"interrupted checkpoint"
        );
    }
    drop(read_only);
    let mut reopened = GraphStore::open(&root, &mut catalog).unwrap();
    assert_eq!(std::fs::read(&published).unwrap(), before);
    assert_eq!(reopened.node_count_for_label(None), 2);
    reopened.checkpoint(&catalog).unwrap();
    for name in names {
        assert!(!root.join(name).exists());
    }
    drop(reopened);
    let reopened = GraphStore::open(&root, &mut catalog).unwrap();
    assert_eq!(reopened.commit_epoch(), epoch);
    assert_eq!(reopened.node_count_for_label(None), 2);
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}
