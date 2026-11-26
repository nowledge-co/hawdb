// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Process-crash images at real temporary-file IO boundaries (#1974).
//! Persisting every observed operation models SIGKILL with surviving page
//! cache, not a power failure or a new durability guarantee.

use super::*;
use crate::{
    EmbeddedGraph, EmbeddedStore, EmbeddedStoreHandle, EmbeddedStoreOpenOptions, GraphMode,
    QueryOutput, StoragePressureSnapshot,
};
use hawdb_storage::durable_manifest::DurableManifest;
use std::time::{Duration, Instant};

enum Api {
    Database(Box<Database>),
    Handle(EmbeddedStoreHandle),
}

impl Api {
    fn from_database(database: Database, handle: bool) -> Self {
        if handle {
            let graph = EmbeddedGraph::from_database(database, GraphMode::WritableCutover);
            Self::Handle(EmbeddedStoreHandle::new(EmbeddedStore::new(graph, None)).unwrap())
        } else {
            Self::Database(Box::new(database))
        }
    }

    fn reopen(root: &Path, handle: bool, automatic: bool) -> Self {
        let config = config(automatic);
        if handle {
            Self::Handle(
                EmbeddedStoreHandle::open_with_options(
                    EmbeddedStoreOpenOptions::graph_only(root, GraphMode::WritableCutover)
                        .with_database_config(config),
                )
                .unwrap()
                .0,
            )
        } else {
            Self::Database(Box::new(Database::open_with_config(root, config).unwrap()))
        }
    }

    fn query(&mut self, query: &str) -> QueryOutput {
        match self {
            Self::Database(database) => database.query(query).unwrap(),
            Self::Handle(handle) => handle.query_with_report(query).unwrap().output,
        }
    }

    fn checkpoint(&mut self) {
        match self {
            Self::Database(database) => database.checkpoint().unwrap(),
            Self::Handle(handle) => handle.checkpoint().unwrap(),
        }
    }

    fn pressure(&self) -> StoragePressureSnapshot {
        match self {
            Self::Database(database) => database.storage_pressure_snapshot().unwrap(),
            Self::Handle(handle) => handle.storage_pressure_snapshot().unwrap(),
        }
    }
}

fn config(automatic: bool) -> DatabaseConfig {
    let mut config = DatabaseConfig {
        max_open_files: 32,
        strict_capacity_limit: true,
        automatic_checkpoint_max_age: Duration::from_millis(20),
        ..DatabaseConfig::default()
    };
    config.runtime_capabilities.background_maintenance = automatic;
    config
}

fn assert_rows(api: &mut Api, rows: i64) {
    let output = api.query("MATCH (n:CrashTemporary) RETURN n.id AS id ORDER BY id");
    assert_eq!(output.rows.len(), rows as usize);
    for id in 0..rows {
        assert_eq!(output.rows[id as usize]["id"], Value::Int(id));
    }
}

fn recover_and_checkpoint(root: &Path, handle: bool, automatic: bool, expected_epoch: u64) {
    let mut api = Api::reopen(root, handle, automatic);
    assert_rows(&mut api, 2);
    assert_eq!(api.pressure().current_commit_epoch, expected_epoch);
    // The crash precedes checkpoint selection. A new write must still work,
    // and successful checkpointing must cover both recovered and new data.
    api.query("CREATE (:CrashTemporary {id: 2})");
    let target = api.pressure().current_commit_epoch;
    if automatic {
        let deadline = Instant::now() + Duration::from_secs(30);
        while api.pressure().checkpoint_commit_epoch < target {
            assert!(
                Instant::now() < deadline,
                "checkpoint stalled: {:?}",
                api.pressure()
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    } else {
        api.checkpoint();
        assert_eq!(api.pressure().checkpoint_commit_epoch, target);
    }
    assert_rows(&mut api, 3);
    drop(api);
    let mut reopened = Api::reopen(root, handle, false);
    assert_rows(&mut reopened, 3);
    assert_eq!(reopened.pressure().checkpoint_commit_epoch, target);
}

fn qualify(family: &str) {
    for handle in [false, true] {
        for initial_checkpoint in [true, false] {
            for event in [IoEvent::CreateFile, IoEvent::Write, IoEvent::FileSync] {
                let mut fixture = Fixture::new();
                let mut database =
                    Database::open_with_config(&fixture.root, config(false)).unwrap();
                let main = database
                    .describe_branch(BranchSelector::Name("main".into()))
                    .unwrap()
                    .id;
                database.query("CREATE (:CrashTemporary {id: 0})").unwrap();
                let mut api = Api::from_database(database, handle);
                if !initial_checkpoint {
                    api.checkpoint();
                }
                api.query("CREATE (:CrashTemporary {id: 1})");
                let epoch = api.pressure().current_commit_epoch;
                let runtime = PathBuf::from("branches")
                    .join(main.to_string())
                    .join("runtime");
                let manifest =
                    DurableManifest::load(&fixture.root.join(&runtime).join("manifest.hawdb"))
                        .unwrap();
                let head = hawdb_storage::branch_head::read_branch_head(
                    &fixture
                        .root
                        .join("branches")
                        .join(main.to_string())
                        .join("branch.head"),
                )
                .unwrap();
                let generation = manifest.checkpoint_epoch.max(head.active_wal.generation) + 1;
                let temporary = PathBuf::from("branches")
                    .join(main.to_string())
                    .join("data")
                    .join(family.replace("{g}", &generation.to_string()));
                fixture
                    .model
                    .observe(ObservationPoint {
                        event,
                        relative_path: temporary.clone(),
                        boundary: ObservationBoundary::After,
                        skip_matches: 0,
                        include_descendants: false,
                        keep_last: false,
                    })
                    .unwrap();
                api.checkpoint();
                fixture.model.capture().unwrap();
                let snapshot = fixture
                    .model
                    .take_observation()
                    .unwrap()
                    .unwrap_or_else(|| panic!("missing {event:?} at {}", temporary.display()));
                // Retain the image before destructors or later publication can
                // clean the interrupted temporary file.
                drop(api);
                for automatic in [false, true] {
                    if automatic && !cfg!(feature = "background-maintenance") {
                        continue;
                    }
                    let root = fixture.image(&snapshot, &snapshot.persist_all_plan());
                    assert!(root.join(&temporary).is_file());
                    recover_and_checkpoint(&root, handle, automatic, epoch);
                    assert!(!root.join(&temporary).exists());
                }
            }
        }
    }
}

#[test]
fn checkpoint_temporary_crashes_allow_reopen_and_checkpoint() {
    qualify("checkpoint.{g}.hawdb.tmp");
}

#[test]
fn append_manifest_temporary_crashes_allow_reopen_and_checkpoint() {
    qualify("append-{g}.manifest.hawdb.tmp");
}

#[test]
fn property_manifest_temporary_crashes_allow_reopen_and_checkpoint() {
    qualify("properties.{g}.manifest.hawdb.tmp");
}

#[test]
fn canonical_manifest_temporary_crashes_allow_reopen_and_checkpoint() {
    qualify("canonical.{g}.manifest.hawdb.tmp");
}
