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

#![cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]

use hawdb::{
    BranchCreateRequest, BranchSelector, Database, DatabaseConfig, DurabilityPolicy, HawDBError,
    Value,
};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

#[path = "branch_project_open/name_reuse.rs"]
mod name_reuse;

#[path = "branch_project_open/qualification.rs"]
mod qualification;

struct Project(PathBuf);
impl Project {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(std::env::temp_dir().join(format!(
            "hawdb-default-project-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        )))
    }
}
impl Drop for Project {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn main_id(database: &Database) -> hawdb::Uuid {
    database
        .describe_branch(BranchSelector::Name("main".into()))
        .unwrap()
        .id
}
fn assert_no_runtime(database: &Database) {
    let metrics = database.file_descriptor_metrics().unwrap();
    assert_eq!(metrics.admitted_runtimes, 0);
    assert_eq!(metrics.ownership_locks, 0);
    assert_eq!(metrics.mutable_wals, 0);
    assert_eq!(metrics.reserved, 0);
}
fn values(database: &mut Database) -> Vec<BTreeMap<String, Value>> {
    database
        .query("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
        .unwrap()
        .rows
        .into_rows()
}

#[test]
fn live_writer_reclamation_retains_readers_descendants_and_budget_retries() {
    use hawdb::BranchReclamationLimits;
    use hawdb_storage::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};

    let project = Project::new();
    let mut database = Database::open_with_config(
        &project.0,
        DatabaseConfig {
            max_open_files: 32,
            ..Default::default()
        },
    )
    .unwrap();
    database.query("CREATE (:Memory {id: 'main'})").unwrap();
    database.checkpoint().unwrap();
    let revision = database.commit_epoch().unwrap();
    database
        .query_sql(&format!(
            "CREATE BRANCH parent FROM main AT REVISION {revision} REQUEST KEY 'reclaim-parent'"
        ))
        .unwrap();
    database
        .query_sql(&format!(
            "CREATE BRANCH sibling FROM main AT REVISION {revision} REQUEST KEY 'reclaim-sibling'"
        ))
        .unwrap();
    database.query_sql("USE BRANCH parent").unwrap();
    database.query("CREATE (:Memory {id: 'parent'})").unwrap();
    database
        .query_sql("CREATE TABLE inherited (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    database
        .query_sql("INSERT INTO inherited (id, body) VALUES (7, 'parent schema and data')")
        .unwrap();
    database.checkpoint().unwrap();
    let revision = database.commit_epoch().unwrap();
    database.query_sql(&format!(
        "CREATE BRANCH descendant FROM parent AT REVISION {revision} REQUEST KEY 'reclaim-descendant'"
    )).unwrap();
    let parent = database
        .describe_branch(BranchSelector::Name("parent".into()))
        .unwrap();
    let descendant = database
        .describe_branch(BranchSelector::Name("descendant".into()))
        .unwrap();
    let catalog =
        hawdb_storage::branch_catalog::read_catalog(&project.0.join("branches/catalog.hawdb"))
            .unwrap();
    let baseline = catalog
        .branches
        .iter()
        .find(|record| record.id.as_uuid() == descendant.id)
        .unwrap()
        .base_root_digest
        .unwrap();
    let initial_head = hawdb_storage::branch_head::read_branch_head(
        &project
            .0
            .join("branches")
            .join(descendant.id.to_string())
            .join("branch.head"),
    )
    .unwrap();
    assert_eq!(initial_head.sealed_root.sha256.as_bytes(), &baseline);
    let mut objects = ImmutableObjectStore::open(project.0.join("branches/objects")).unwrap();
    let baseline_path = objects.object_path(initial_head.sealed_root);
    database.query_sql("USE BRANCH descendant").unwrap();
    database
        .query("CREATE (:Memory {id: 'descendant'})")
        .unwrap();
    database.checkpoint().unwrap();
    database.query_sql("USE BRANCH main").unwrap();
    database
        .query_sql(&format!(
            "DROP BRANCH ID '{}' AT REVISION {}",
            parent.id, parent.metadata_revision
        ))
        .unwrap();
    let parent_directory = project.0.join("branches").join(parent.id.to_string());
    assert!(parent_directory.exists());
    let orphan = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, b"unpublished");
    objects.publish(orphan, b"unpublished").unwrap();
    let orphan_path = objects.object_path(orphan);
    let staging = project
        .0
        .join("branches/objects/objects/.staging/999999-1.stage");
    std::fs::write(&staging, b"interrupted publisher").unwrap();
    let snapshot = database.begin_read_transaction().unwrap();
    let deferred = database
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert!(deferred.deferred_for_active_leases);
    assert_eq!(deferred.reclaimed_objects, 0);
    assert!(orphan_path.exists());
    assert!(parent_directory.exists());
    drop(snapshot);
    assert!(database
        .reclaim_branch_storage(BranchReclamationLimits {
            max_objects: 1,
            ..Default::default()
        })
        .is_err());
    assert!(orphan_path.exists());
    assert!(parent_directory.exists());
    assert!(staging.exists());
    let report = database
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert!(!report.deferred_for_active_leases, "{report:?}");
    assert!(report.reclaimed_objects > 0);
    assert!(report.reclaimed_bytes >= orphan.byte_length);
    assert_eq!(report.reclaimed_branch_directories, 1);
    assert_eq!(report.reclaimed_staging_files, 1);
    assert!(!staging.exists());
    assert!(!orphan_path.exists());
    assert!(!parent_directory.exists());
    assert!(
        baseline_path.exists(),
        "a catalog creation baseline remains a GC root after head advancement"
    );
    assert_eq!(values(&mut database)[0]["id"], Value::String("main".into()));
    database
        .query("CREATE (:Memory {id: 'still-writable'})")
        .unwrap();
    assert!(
        !database
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .unwrap()
            .deferred_for_active_leases
    );
    assert!(database.file_descriptor_metrics().unwrap().high_water <= 32);
    drop(database);

    let mut reopened = Database::open(&project.0).unwrap();
    reopened.query_sql("USE BRANCH descendant").unwrap();
    assert_eq!(values(&mut reopened).len(), 3);
    assert_eq!(
        reopened
            .query_sql("SELECT body FROM inherited WHERE id = 7")
            .unwrap()
            .rows[0]["body"],
        Value::String("parent schema and data".into())
    );
    reopened.query_sql("USE BRANCH sibling").unwrap();
    assert_eq!(values(&mut reopened).len(), 1);
    reopened.query_sql("USE BRANCH main").unwrap();
    assert_eq!(values(&mut reopened).len(), 2);
    assert!(reopened.query_sql("USE BRANCH parent").is_err());
}

#[test]
fn concurrent_reclamation_retires_only_idle_publication_pins() {
    use hawdb::BranchReclamationLimits;
    use hawdb_storage::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};

    let project = Project::new();
    let mut database = Database::open(&project.0).unwrap();
    database.query("CREATE (:Memory {id: 'before'})").unwrap();
    database.checkpoint().unwrap();
    let database = database.into_concurrent();
    let mut reader = database.begin_read_transaction().unwrap();
    database.query("CREATE (:Memory {id: 'after'})").unwrap();
    database.checkpoint().unwrap();
    let mut objects = ImmutableObjectStore::open(project.0.join("branches/objects")).unwrap();
    let orphan =
        ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, b"concurrent-orphan");
    objects.publish(orphan, b"concurrent-orphan").unwrap();
    assert!(
        database
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .unwrap()
            .deferred_for_active_leases
    );
    assert!(objects.object_path(orphan).exists());
    assert_eq!(
        reader
            .query("MATCH (m:Memory) RETURN m.id")
            .unwrap()
            .rows
            .len(),
        1
    );
    drop(reader);
    let report = database
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert!(!report.deferred_for_active_leases, "{report:?}");
    assert!(!objects.object_path(orphan).exists());
    assert_eq!(
        database
            .query("MATCH (m:Memory) RETURN m.id")
            .unwrap()
            .rows
            .len(),
        2
    );
    database
        .query("CREATE (:Memory {id: 'after-maintenance'})")
        .unwrap();
    assert_eq!(
        database
            .query("MATCH (m:Memory) RETURN m.id")
            .unwrap()
            .rows
            .len(),
        3
    );
}

#[test]
fn reclamation_rejects_corrupt_inventory_and_unrelated_writer_then_retries() {
    use hawdb::BranchReclamationLimits;
    use hawdb_storage::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};

    let project = Project::new();
    let mut database = Database::open(&project.0).unwrap();
    database.query("CREATE (:Memory {id: 'retained'})").unwrap();
    database.checkpoint().unwrap();
    let revision = database.commit_epoch().unwrap();
    database
        .query_sql(&format!(
            "CREATE BRANCH other FROM main AT REVISION {revision} REQUEST KEY 'gc-other'"
        ))
        .unwrap();
    let mut other = Database::open(&project.0).unwrap();
    other.query_sql("USE BRANCH other").unwrap();
    let mut objects = ImmutableObjectStore::open(project.0.join("branches/objects")).unwrap();
    let orphan = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, b"retained-orphan");
    objects.publish(orphan, b"retained-orphan").unwrap();
    let path = objects.object_path(orphan);
    assert!(
        database
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .unwrap()
            .deferred_for_active_leases
    );
    assert!(path.exists());
    drop(other);
    std::fs::write(&path, b"damaged-orphan").unwrap();
    assert!(database
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"damaged-orphan");
    assert_eq!(values(&mut database).len(), 1);
    std::fs::write(&path, b"retained-orphan").unwrap();
    let report = database
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert!(!report.deferred_for_active_leases);
    assert!(!path.exists());
    database.query_sql("USE BRANCH other").unwrap();
    assert_eq!(values(&mut database).len(), 1);
}

#[test]
fn storage_inventory_tracks_selection_and_retries_budget_exhaustion() {
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;
    use hawdb_storage::file_io::OpenOptions;

    let project = Project::new();
    let mut database = Database::open_with_config(
        &project.0,
        DatabaseConfig {
            max_open_files: 32,
            ..Default::default()
        },
    )
    .unwrap();
    database.query("CREATE (:Memory {id: 'main'})").unwrap();
    database.checkpoint().unwrap();
    let revision = database.commit_epoch().unwrap();
    database
        .query_sql(&format!(
            "CREATE BRANCH inventory_child FROM main AT REVISION {revision} REQUEST KEY 'inventory-child'"
        ))
        .unwrap();
    let main = main_id(&database);
    let child = database
        .describe_branch(BranchSelector::Name("inventory_child".into()))
        .unwrap()
        .id;
    let expected_files = |id: hawdb::Uuid| {
        std::fs::read_dir(project.0.join("branches").join(id.to_string()).join("data"))
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter_map(|entry| {
                let metadata = entry.metadata().unwrap();
                metadata
                    .is_file()
                    .then(|| (entry.file_name().into_string().unwrap(), metadata.len()))
            })
            .collect::<BTreeMap<_, _>>()
    };
    let main_files = expected_files(main);
    assert!(!main_files.is_empty());
    assert_eq!(database.storage_artifact_file_sizes().unwrap(), main_files);
    database.query_sql("USE BRANCH inventory_child").unwrap();
    database.query("CREATE (:Memory {id: 'child'})").unwrap();
    database.checkpoint().unwrap();
    assert_eq!(
        database.storage_artifact_file_sizes().unwrap(),
        expected_files(child)
    );
    assert_eq!(expected_files(main), main_files);

    let files = ProjectFileDescriptors::acquire_existing(&project.0, 32).unwrap();
    let mut options = OpenOptions::new();
    options.read(true);
    let mut held = Vec::new();
    loop {
        match files
            .io_context()
            .open(&options, &project.0.join("manifest.hawdb"))
        {
            Ok(file) => held.push(file),
            Err(error) => {
                assert!(matches!(
                    HawDBError::from(error),
                    HawDBError::FileDescriptors(_)
                ));
                break;
            }
        }
    }
    assert_eq!(files.metrics().open, 32);
    let before = files.metrics();
    assert!(matches!(
        database.storage_artifact_file_sizes(),
        Err(HawDBError::FileDescriptors(
            hawdb::FileDescriptorError::BudgetExceeded {
                requested: 1,
                available: 0,
                limit: 32,
            }
        ))
    ));
    assert_eq!(files.metrics().open, before.open);
    assert_eq!(files.metrics().reserved, before.reserved);
    assert!(!database.storage_handle_poisoned().unwrap());
    drop(held);
    assert_eq!(
        database.storage_artifact_file_sizes().unwrap(),
        expected_files(child)
    );
    assert_eq!(values(&mut database).len(), 2);
    drop(database);
    assert_eq!(files.metrics().open, 0);
}

#[test]
fn slow_query_export_uses_source_budget_without_admitting_deferred_runtime() {
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;
    use hawdb_storage::file_io::OpenOptions;

    let project = Project::new();
    let database = Database::open_with_config(
        &project.0,
        DatabaseConfig {
            max_open_files: 32,
            ..Default::default()
        },
    )
    .unwrap();
    let files = ProjectFileDescriptors::acquire_existing(&project.0, 32).unwrap();
    let external = Project::new();
    // A distinct destination domain must not allow export to bypass the source
    // domain, even when it has spare capacity and the source runtime is cold.
    let _destination_files = ProjectFileDescriptors::acquire(&external.0, 64).unwrap();
    let destination = external.0.join("slow-query.jsonl");
    std::fs::write(&destination, b"retain on admission failure").unwrap();
    let mut options = OpenOptions::new();
    options.read(true);
    let held = (0..32)
        .map(|_| {
            files
                .io_context()
                .open(&options, &project.0.join("manifest.hawdb"))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let before = files.metrics();
    assert_eq!(before.open, 32);
    assert!(matches!(
        database.write_slow_query_log_jsonl(&destination),
        Err(HawDBError::FileDescriptors(
            hawdb::FileDescriptorError::BudgetExceeded {
                requested: 1,
                available: 0,
                limit: 32,
            }
        ))
    ));
    let mut rejected = before;
    rejected.budget_rejections += 1;
    assert_eq!(files.metrics(), rejected);
    assert_no_runtime(&database);
    assert_eq!(
        std::fs::read(&destination).unwrap(),
        b"retain on admission failure"
    );
    drop(held);
    database.write_slow_query_log_jsonl(&destination).unwrap();
    assert_eq!(
        std::fs::read_to_string(&destination).unwrap(),
        database.slow_query_log_jsonl().unwrap()
    );
    assert_eq!(files.metrics().open, 0);
    assert_no_runtime(&database);
}

#[derive(Debug, Default)]
struct ReentrantRecoverySink {
    database: Mutex<Weak<Database>>,
    recoveries: AtomicU64,
    epoch: AtomicU64,
}

impl hawdb::TelemetrySink for ReentrantRecoverySink {
    fn record_query(&self, _: hawdb::QueryTelemetry<'_>) {}

    fn record_kernel(&self, event: hawdb::KernelTelemetry) {
        if event.operation != hawdb::KernelTelemetryOperation::Recovery {
            return;
        }
        let database = self.database.lock().unwrap().upgrade().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            sender.send(database.commit_epoch()).unwrap();
        });
        // A callback before publication would wait for the admission lock
        // held by its caller. Bound the wait so that regression fails cleanly.
        let epoch = receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("recovery callback must observe the published runtime")
            .unwrap();
        reader.join().unwrap();
        self.epoch.store(epoch, Ordering::Relaxed);
        self.recoveries.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn deferred_recovery_callback_can_read_the_published_database() {
    let project = Project::new();
    let mut writer = Database::open(&project.0).unwrap();
    writer.query("CREATE (:Memory {id: 'telemetry'})").unwrap();
    let epoch = writer.commit_epoch().unwrap();
    drop(writer);
    let mut database = Database::open(&project.0).unwrap();
    let sink = Arc::new(ReentrantRecoverySink::default());
    database.set_telemetry_sink(Some(sink.clone()));
    assert_no_runtime(&database);
    assert_eq!(sink.recoveries.load(Ordering::Relaxed), 0);
    let database = Arc::new(database);
    *sink.database.lock().unwrap() = Arc::downgrade(&database);
    assert_eq!(database.commit_epoch().unwrap(), epoch);
    assert_eq!(database.commit_epoch().unwrap(), epoch);
    assert_eq!(sink.epoch.load(Ordering::Relaxed), epoch);
    assert_eq!(sink.recoveries.load(Ordering::Relaxed), 1);
    drop(database);
}

#[test]
fn ordinary_open_bootstraps_main_and_reopens_its_private_wal() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let project = Project::new();
        let mut database = Database::open_with_durability(&project.0, durability).unwrap();
        assert_no_runtime(&database);
        let main = main_id(&database);
        let selector = std::fs::read(project.0.join("manifest.hawdb")).unwrap();
        assert!(selector.starts_with(b"HAWDB_BRANCH_PROJECT_V1\n"));
        let current = database.query_sql("SHOW CURRENT BRANCH").unwrap();
        assert_eq!(current.rows[0]["branch_id"], Value::Uuid(main));
        database
            .query("CREATE (:Memory {id: 'checkpoint'})")
            .unwrap();
        database.checkpoint().unwrap();
        database
            .query("CREATE (:Memory {id: 'private-wal'})")
            .unwrap();
        let epoch = database.commit_epoch().unwrap();
        drop(database);
        let mut reopened = Database::open_with_durability(&project.0, durability).unwrap();
        assert_no_runtime(&reopened);
        assert_eq!(main_id(&reopened), main);
        assert_eq!(
            std::fs::read(project.0.join("manifest.hawdb")).unwrap(),
            selector
        );
        assert_eq!(reopened.commit_epoch().unwrap(), epoch);
        assert_eq!(
            values(&mut reopened),
            vec![
                BTreeMap::from([("id".into(), Value::String("checkpoint".into()))]),
                BTreeMap::from([("id".into(), Value::String("private-wal".into()))]),
            ]
        );
    }
}

#[test]
fn read_only_admission_and_use_preserve_unpublished_checkpoint_evidence() {
    let project = Project::new();
    let mut writer = Database::open(&project.0).unwrap();
    writer.query("CREATE (:Memory {id: 'retained'})").unwrap();
    writer.checkpoint().unwrap();
    let main = main_id(&writer);
    let child = writer
        .create_branch(BranchCreateRequest {
            name: Some("readonly-child".into()),
            parent: BranchSelector::Id(main),
            expected_source_commit_epoch: writer.commit_epoch().unwrap(),
            idempotency_key: "readonly-child".into(),
            owner: None,
        })
        .unwrap();
    drop(writer);
    let directories = [main, child.id].map(|id| project.0.join("branches").join(id.to_string()));
    for directory in &directories {
        let data = directory.join("data");
        let projections = data.join("projection-generations");
        if projections.exists() {
            std::fs::remove_dir_all(&projections).unwrap();
        }
        let preparation = data.join(".checkpoint.999.prepare");
        std::fs::create_dir_all(&preparation).unwrap();
        std::fs::write(preparation.join("evidence"), b"unpublished-checkpoint").unwrap();
    }
    let mut reader = Database::open_with_config(
        &project.0,
        DatabaseConfig {
            read_only: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_no_runtime(&reader);
    for (index, directory) in directories.iter().enumerate() {
        if index == 1 {
            reader
                .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child.id)])
                .unwrap();
        }
        let head = std::fs::read(directory.join("branch.head")).unwrap();
        assert_eq!(values(&mut reader).len(), 1);
        assert_eq!(std::fs::read(directory.join("branch.head")).unwrap(), head);
        assert_eq!(
            std::fs::read(directory.join("data/.checkpoint.999.prepare/evidence")).unwrap(),
            b"unpublished-checkpoint"
        );
        assert!(!directory.join("data/projection-generations").exists());
        assert!(reader.projection_generation_store().is_err());
    }
}

#[test]
fn metadata_open_and_show_survive_busy_main_and_data_admission_can_retry() {
    let project = Project::new();
    let mut writer = Database::open(&project.0).unwrap();
    writer.query("CREATE (:Memory {id: 'retained'})").unwrap();
    let before = writer.file_descriptor_metrics().unwrap();
    let mut metadata = Database::open(&project.0).unwrap();
    assert_eq!(
        metadata.file_descriptor_metrics().unwrap().open,
        before.open
    );
    assert_eq!(
        metadata
            .query_sql("SHOW BRANCHES LIMIT 10")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(matches!(
        metadata.query("MATCH (m:Memory) RETURN m.id"),
        Err(HawDBError::BranchBusy { .. })
    ));
    assert!(matches!(
        metadata.begin_transaction(),
        Err(HawDBError::BranchBusy { .. })
    ));
    assert!(matches!(
        metadata.begin_read_transaction(),
        Err(HawDBError::BranchBusy { .. })
    ));
    assert!(matches!(
        metadata.commit_epoch(),
        Err(HawDBError::BranchBusy { .. })
    ));
    assert_eq!(
        metadata
            .file_descriptor_metrics()
            .unwrap()
            .admitted_runtimes,
        1
    );
    drop(writer);
    assert_eq!(
        values(&mut metadata),
        vec![BTreeMap::from([(
            "id".into(),
            Value::String("retained".into())
        )])]
    );
    assert_eq!(
        metadata
            .file_descriptor_metrics()
            .unwrap()
            .admitted_runtimes,
        1
    );
}

#[test]
fn cold_sql_drop_does_not_admit_busy_main_and_root_delete_is_immutable() {
    let project = Project::new();
    let mut writer = Database::open(&project.0).unwrap();
    let source = writer
        .describe_branch(BranchSelector::Name("main".into()))
        .unwrap();
    let child = writer
        .create_branch(BranchCreateRequest {
            name: Some("disposable".into()),
            parent: BranchSelector::Name("main".into()),
            expected_source_commit_epoch: source.source_commit_epoch,
            owner: None,
            idempotency_key: "metadata-delete".into(),
        })
        .unwrap();
    writer.query("CREATE (:Memory {id: 'writer'})").unwrap();
    let mut metadata = Database::open(&project.0).unwrap();
    let before = metadata.file_descriptor_metrics().unwrap();
    assert!(matches!(
        metadata.delete_branch(BranchSelector::Name("main".into())),
        Err(hawdb::BranchLifecycleError::RootBranchImmutable)
    ));
    metadata
        .query_sql_with_params(
            "DROP BRANCH ID $1 AT REVISION $2",
            &[
                Value::Uuid(child.id),
                Value::Int(child.metadata_revision as i64),
            ],
        )
        .unwrap();
    assert_eq!(
        metadata
            .describe_branch(BranchSelector::Id(child.id))
            .unwrap()
            .state,
        hawdb::BranchLifecycleState::Deleted
    );
    let after = metadata.file_descriptor_metrics().unwrap();
    assert_eq!(after.admitted_runtimes, before.admitted_runtimes);
    assert_eq!(after.ownership_locks, before.ownership_locks);
    assert_eq!(after.mutable_wals, before.mutable_wals);
    assert_eq!(after.reserved, 0);
    assert!(matches!(
        metadata.commit_epoch(),
        Err(HawDBError::BranchBusy { .. })
    ));
    assert_eq!(values(&mut writer).len(), 1);
    drop(metadata);
    drop(writer);
}

#[test]
fn pending_main_can_select_another_branch_while_main_is_busy() {
    let project = Project::new();
    let mut writer = Database::open(&project.0).unwrap();
    writer.query("CREATE (:Memory {id: 'source'})").unwrap();
    let child = writer
        .create_branch(BranchCreateRequest {
            name: Some("child".into()),
            parent: BranchSelector::Name("main".into()),
            expected_source_commit_epoch: writer.commit_epoch().unwrap(),
            owner: None,
            idempotency_key: "default-project-child".into(),
        })
        .unwrap();
    let mut metadata = Database::open(&project.0).unwrap();
    metadata.query_sql("USE BRANCH child").unwrap();
    assert_eq!(
        metadata.query_sql("SHOW CURRENT BRANCH").unwrap().rows[0]["branch_id"],
        Value::Uuid(child.id)
    );
    assert_eq!(
        values(&mut metadata),
        vec![BTreeMap::from([(
            "id".into(),
            Value::String("source".into())
        )])]
    );
    assert!(matches!(
        metadata.query_sql("USE BRANCH main"),
        Err(HawDBError::BranchBusy { .. })
    ));
    metadata
        .query("CREATE (:Memory {id: 'child-write'})")
        .unwrap();
    assert_eq!(values(&mut writer).len(), 1);
}

#[test]
fn published_metadata_can_open_damaged_main_without_recreating_it() {
    let project = Project::new();
    let database = Database::open(&project.0).unwrap();
    let main = main_id(&database);
    drop(database);
    let head = project
        .0
        .join("branches")
        .join(main.to_string())
        .join("branch.head");
    let original = std::fs::read(&head).unwrap();
    std::fs::write(&head, b"torn-head").unwrap();
    let mut metadata = Database::open(&project.0).unwrap();
    assert_no_runtime(&metadata);
    assert_eq!(
        metadata
            .query_sql("SHOW BRANCHES LIMIT 10")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(metadata.query("MATCH (m:Memory) RETURN m.id").is_err());
    assert!(metadata.commit_epoch().is_err());
    assert_no_runtime(&metadata);
    assert_eq!(std::fs::read(&head).unwrap(), b"torn-head");
    std::fs::write(&head, original).unwrap();
    assert!(values(&mut metadata).is_empty());
}

#[test]
fn pending_main_repair_blocks_only_its_data_admission() {
    let project = Project::new();
    let mut database = Database::open(&project.0).unwrap();
    database.query("CREATE (:Memory {id: 'retained'})").unwrap();
    let main = main_id(&database);
    let child = database
        .create_branch(BranchCreateRequest {
            name: Some("healthy".into()),
            parent: BranchSelector::Id(main),
            expected_source_commit_epoch: database.commit_epoch().unwrap(),
            owner: None,
            idempotency_key: "pending-repair-healthy".into(),
        })
        .unwrap();
    drop(database);
    let directory = project
        .0
        .join("branches")
        .join(main.to_string())
        .join("doctor");
    std::fs::create_dir_all(&directory).unwrap();
    let evidence = directory.join("fixture.derived-repair.pending.json");
    std::fs::write(&evidence, b"retained interrupted repair evidence").unwrap();
    let mut metadata = Database::open(&project.0).unwrap();
    assert_no_runtime(&metadata);
    assert_eq!(
        metadata
            .query_sql("SHOW BRANCHES LIMIT 10")
            .unwrap()
            .rows
            .len(),
        2
    );
    assert!(metadata
        .commit_epoch()
        .unwrap_err()
        .to_string()
        .contains("interrupted derived artifact repair"));
    assert_no_runtime(&metadata);
    metadata.query_sql("USE BRANCH healthy").unwrap();
    assert_eq!(
        metadata.query_sql("SHOW CURRENT BRANCH").unwrap().rows[0]["branch_id"],
        Value::Uuid(child.id)
    );
    assert_eq!(values(&mut metadata).len(), 1);
    assert_eq!(
        std::fs::read(&evidence).unwrap(),
        b"retained interrupted repair evidence"
    );
}

#[test]
fn metadata_open_obeys_small_fd_limit_without_admitting_main() {
    let project = Project::new();
    drop(Database::open(&project.0).unwrap());
    let mut metadata = Database::open_with_config(
        &project.0,
        DatabaseConfig {
            max_open_files: 4,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    assert_no_runtime(&metadata);
    assert_eq!(
        metadata
            .query_sql("SHOW BRANCHES LIMIT 10")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(matches!(
        metadata.query("MATCH (m:Memory) RETURN m.id"),
        Err(HawDBError::FileDescriptors(_))
    ));
    assert_no_runtime(&metadata);
    assert!(
        metadata
            .file_descriptor_metrics()
            .unwrap()
            .budget_rejections
            > 0
    );
    assert!(metadata.file_descriptor_metrics().unwrap().high_water <= 4);
    drop(metadata);
    assert!(values(&mut Database::open(&project.0).unwrap()).is_empty());
}

#[test]
fn ordinary_open_adopts_legacy_acknowledged_rows_without_copying_directory() {
    let project = Project::new();
    let mut catalog = hawdb::schema::Catalog::default();
    let mut legacy = hawdb::store::GraphStore::open(&project.0, &mut catalog).unwrap();
    legacy
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("id".into(), Value::String("legacy-wal".into()))]),
        )
        .unwrap();
    drop(legacy);
    let mut database = Database::open(&project.0).unwrap();
    assert_no_runtime(&database);
    assert_eq!(
        values(&mut database),
        vec![BTreeMap::from([(
            "id".into(),
            Value::String("legacy-wal".into())
        )])]
    );
    let main = main_id(&database);
    drop(database);
    let mut reopened = Database::open(&project.0).unwrap();
    assert_eq!(main_id(&reopened), main);
    assert_eq!(values(&mut reopened).len(), 1);
}

#[test]
fn shared_runtime_rejects_use_before_main_admission_and_can_show_catalog() {
    let project = Project::new();
    let mut writer = Database::open(&project.0).unwrap();
    writer.query("CREATE (:Memory {id: 'busy-main'})").unwrap();
    let shared = Database::open(&project.0).unwrap().into_concurrent();
    assert!(matches!(
        shared.query_sql("USE BRANCH main"),
        Err(HawDBError::BranchCommandUnsupported { .. })
    ));
    assert_eq!(
        shared
            .query_sql("SHOW BRANCHES LIMIT 10")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(matches!(
        shared.query("MATCH (m:Memory) RETURN m.id"),
        Err(HawDBError::BranchBusy { .. })
    ));
    drop(writer);
    let rows = shared.query("MATCH (m:Memory) RETURN m.id AS id").unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0]["id"], Value::String("busy-main".into()));
    assert!(shared.commit_epoch().unwrap() > 0);
}

#[cfg(unix)]
#[test]
fn unopened_catalog_scale_keeps_native_and_project_descriptors_bounded() {
    const TEST: &str = "unopened_catalog_scale_keeps_native_and_project_descriptors_bounded";
    const CHILD: &str = "HAWDB_TEST_BRANCH_DESCRIPTOR_CHILD";
    if std::env::var(CHILD).as_deref() != Ok(TEST) {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
            .env(CHILD, TEST)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "isolated branch FD qualification failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
        return;
    }
    let native_count = || {
        let directory = if cfg!(target_os = "linux") {
            "/proc/self/fd"
        } else {
            "/dev/fd"
        };
        std::fs::read_dir(directory)
            .unwrap()
            .try_fold(0, |count, entry| entry.map(|_| count + 1))
            .unwrap()
    };
    let project = Project::new();
    let limited = DatabaseConfig {
        max_open_files: 12,
        ..Default::default()
    };
    let before = native_count();
    let mut rejected = Database::open_with_config(&project.0, limited).unwrap();
    let main = rejected
        .describe_branch(BranchSelector::Name("main".into()))
        .unwrap();
    let sql = "CREATE BRANCH NAME $1 FROM ID $2 AT REVISION $3 REQUEST KEY $4";
    let parameters = [
        Value::String("descriptor-denied".into()),
        Value::Uuid(main.id),
        Value::Int(i64::try_from(main.source_commit_epoch).unwrap()),
        Value::String("descriptor-denied".into()),
    ];
    assert!(matches!(
        rejected.query_sql_with_params(sql, &parameters),
        Err(HawDBError::FileDescriptors(_))
    ));
    assert_no_runtime(&rejected);
    assert_eq!(
        rejected
            .query_sql("SHOW BRANCHES LIMIT 65")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(native_count(), before);
    drop(rejected);
    // Cold creation explicitly reserves 23 descriptors for its recovery and
    // sealing path. A finite limit of 32 admits that path; 12 above proves
    // exhaustion fails before catalog mutation and releases the reservation.
    let config = DatabaseConfig {
        max_open_files: 32,
        ..Default::default()
    };
    let mut database = Database::open_with_config(&project.0, config.clone()).unwrap();
    assert_no_runtime(&database);
    let main = database
        .describe_branch(BranchSelector::Name("main".into()))
        .unwrap();
    let sql = "CREATE BRANCH NAME $1 FROM ID $2 AT REVISION $3 REQUEST KEY $4";
    for id in 0..64 {
        let parameters = [
            Value::String(format!("unopened-{id}")),
            Value::Uuid(main.id),
            Value::Int(i64::try_from(main.source_commit_epoch).unwrap()),
            Value::String(format!("descriptor-create-{id}")),
        ];
        let first = database.query_sql_with_params(sql, &parameters).unwrap();
        assert_eq!(
            database
                .query_sql_with_params(sql, &parameters)
                .unwrap()
                .rows,
            first.rows
        );
        let mut conflicting = parameters;
        conflicting[0] = Value::String(format!("conflicting-{id}"));
        assert!(database.query_sql_with_params(sql, &conflicting).is_err());
        assert_no_runtime(&database);
        let metrics = database.file_descriptor_metrics().unwrap();
        assert_eq!(metrics.open, 0);
        assert_eq!(metrics.reserved, 0);
        assert!(metrics.high_water <= 32);
        assert_eq!(native_count(), before, "branch {id} retained a native FD");
    }
    for _ in 0..8 {
        assert_eq!(
            database
                .query_sql("SHOW BRANCHES LIMIT 65")
                .unwrap()
                .rows
                .len(),
            65
        );
        assert_eq!(native_count(), before);
        assert_no_runtime(&database);
    }
    drop(database);
    let mut reopened = Database::open_with_config(&project.0, config).unwrap();
    assert_eq!(
        reopened
            .query_sql("SHOW BRANCHES LIMIT 65")
            .unwrap()
            .rows
            .len(),
        65
    );
    assert_no_runtime(&reopened);
    assert_eq!(native_count(), before);
}

#[test]
fn sql_create_from_cold_main_and_bootstrap_retry_keep_data_runtime_closed() {
    let project = Project::new();
    let mut database = Database::open(&project.0).unwrap();
    let selector = std::fs::read_to_string(project.0.join("manifest.hawdb")).unwrap();
    let project_id = selector
        .lines()
        .find_map(|line| line.strip_prefix("project_id\t"))
        .unwrap()
        .parse()
        .unwrap();
    let main = database
        .describe_branch(BranchSelector::Name("main".into()))
        .unwrap();
    let head = project
        .0
        .join("branches")
        .join(main.id.to_string())
        .join("branch.head");
    let original = std::fs::read(&head).unwrap();
    assert_eq!(
        database
            .initialize_main_branch(project_id, main.id)
            .unwrap(),
        main
    );
    assert_eq!(std::fs::read(&head).unwrap(), original);
    assert_no_runtime(&database);
    let sql = "CREATE BRANCH NAME $1 FROM ID $2 AT REVISION $3 REQUEST KEY $4";
    let parameters = [
        Value::String("cold-child".into()),
        Value::Uuid(main.id),
        Value::Int(i64::try_from(main.source_commit_epoch).unwrap()),
        Value::String("cold-main-child".into()),
    ];
    let child = database.query_sql_with_params(sql, &parameters).unwrap();
    assert_no_runtime(&database);
    assert_eq!(
        database
            .query_sql_with_params(sql, &parameters)
            .unwrap()
            .rows,
        child.rows
    );
    assert_no_runtime(&database);
    database.query_sql("USE BRANCH NAME 'cold-child'").unwrap();
    database
        .query_sql("CREATE TABLE child_rows (id BIGINT PRIMARY KEY, value TEXT)")
        .unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    transaction.query("CREATE (:Memory {id: 'child'})").unwrap();
    transaction
        .query_sql("INSERT INTO child_rows (id, value) VALUES (1, 'atomic')")
        .unwrap();
    transaction.commit().unwrap();
    database.checkpoint().unwrap();
    let grandchild = database
        .create_branch(BranchCreateRequest {
            name: Some("grandchild".into()),
            parent: BranchSelector::Name("cold-child".into()),
            expected_source_commit_epoch: database.commit_epoch().unwrap(),
            owner: None,
            idempotency_key: "cold-child-grandchild".into(),
        })
        .unwrap();
    drop(database);
    let mut reopened = Database::open(&project.0).unwrap();
    assert_no_runtime(&reopened);
    assert!(values(&mut reopened).is_empty());
    assert!(reopened.query_sql("SELECT value FROM child_rows").is_err());
    reopened.query_sql("USE BRANCH grandchild").unwrap();
    assert_eq!(
        reopened.query_sql("SHOW CURRENT BRANCH").unwrap().rows[0]["branch_id"],
        Value::Uuid(grandchild.id)
    );
    assert_eq!(values(&mut reopened).len(), 1);
    assert_eq!(
        reopened
            .query_sql("SELECT value FROM child_rows")
            .unwrap()
            .rows[0]["value"],
        Value::String("atomic".into())
    );
}

#[test]
fn published_project_never_falls_back_after_selector_or_catalog_damage() {
    let project = Project::new();
    drop(Database::open(&project.0).unwrap());
    let manifest = project.0.join("manifest.hawdb");
    let selector = std::fs::read(&manifest).unwrap();
    let catalog = project.0.join("branches/catalog.hawdb");
    let catalog_bytes = std::fs::read(&catalog).unwrap();
    std::fs::write(&manifest, &selector[..selector.len() - 1]).unwrap();
    assert!(Database::open(&project.0).is_err());
    assert_eq!(
        std::fs::read(&manifest).unwrap(),
        &selector[..selector.len() - 1]
    );
    assert_eq!(std::fs::read(&catalog).unwrap(), catalog_bytes);
    std::fs::write(&manifest, &selector).unwrap();
    std::fs::write(&catalog, b"damaged catalog").unwrap();
    assert!(Database::open(&project.0).is_err());
    assert_eq!(std::fs::read(&manifest).unwrap(), selector);
    assert_eq!(std::fs::read(&catalog).unwrap(), b"damaged catalog");
}

fn pending_children(
    root: &std::path::Path,
    database: &mut Database,
    count: usize,
) -> Vec<hawdb::Uuid> {
    use hawdb_storage::branch_catalog::{self, BranchState, CreateOutcome};

    let epoch = database.commit_epoch().unwrap();
    let ids: Vec<_> = (0..count)
        .map(|index| {
            database
                .create_branch(BranchCreateRequest {
                    name: Some(format!("pending-{index}")),
                    parent: BranchSelector::Name("main".into()),
                    expected_source_commit_epoch: epoch,
                    owner: None,
                    idempotency_key: format!("recover-pending-{index}"),
                })
                .unwrap()
                .id
        })
        .collect();
    let path = root.join("branches/catalog.hawdb");
    let mut catalog = branch_catalog::read_catalog(&path).unwrap();
    for branch in &mut catalog.branches {
        if ids.contains(&branch.id.as_uuid()) {
            branch.state = BranchState::Creating;
            branch.create_outcome = CreateOutcome::Pending;
        }
    }
    branch_catalog::write_catalog(&path, &catalog).unwrap();
    ids
}

#[test]
fn reclamation_defers_for_a_live_pending_creator_without_a_head() {
    use hawdb::{BranchCreateRecoveryLimits, BranchCreateRecoveryStatus, BranchReclamationLimits};
    use hawdb_storage::ownership::DatabaseDirectoryLease;

    let project = Project::new();
    let mut writer = Database::open(&project.0).unwrap();
    writer.query("CREATE (:Memory {id: 'parent'})").unwrap();
    let child = pending_children(&project.0, &mut writer, 1)[0];
    let directory = project.0.join("branches").join(child.to_string());
    let lease = DatabaseDirectoryLease::acquire(&directory).unwrap();
    std::fs::remove_file(directory.join("branch.head")).unwrap();
    let catalog_path = project.0.join("branches/catalog.hawdb");
    let before = std::fs::read(&catalog_path).unwrap();

    let report = writer
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert!(report.deferred_for_active_leases);
    assert_eq!(report.reclaimed_objects, 0);
    assert_eq!(report.reclaimed_branch_directories, 0);
    assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
    assert!(directory.exists());

    drop(lease);
    let recovery = writer
        .recover_pending_branch_creates(BranchCreateRecoveryLimits::default())
        .unwrap();
    assert_eq!(recovery.entries[0].branch_id, child);
    assert_eq!(
        recovery.entries[0].status,
        BranchCreateRecoveryStatus::Aborted
    );
    let report = writer
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert!(!report.deferred_for_active_leases);
    assert_eq!(report.reclaimed_branch_directories, 1);
    assert!(!directory.exists());
    assert_eq!(values(&mut writer).len(), 1);
}

#[test]
fn retained_pending_create_blocks_reclamation_without_deleting_evidence() {
    use hawdb::{BranchCreateRecoveryLimits, BranchCreateRecoveryStatus, BranchReclamationLimits};
    use hawdb_storage::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};

    let project = Project::new();
    let mut writer = Database::open(&project.0).unwrap();
    writer.query("CREATE (:Memory {id: 'parent'})").unwrap();
    let child = pending_children(&project.0, &mut writer, 1)[0];
    let directory = project.0.join("branches").join(child.to_string());
    let head_path = directory.join("branch.head");
    let mut damaged_head = std::fs::read(&head_path).unwrap();
    damaged_head[0] ^= 0xff;
    std::fs::write(&head_path, &damaged_head).unwrap();
    let catalog_path = project.0.join("branches/catalog.hawdb");
    let before = std::fs::read(&catalog_path).unwrap();
    let mut objects = ImmutableObjectStore::open(project.0.join("branches/objects")).unwrap();
    let orphan = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, b"orphan");
    objects.publish(orphan, b"orphan").unwrap();

    for _ in 0..2 {
        let recovery = writer
            .recover_pending_branch_creates(BranchCreateRecoveryLimits::default())
            .unwrap();
        assert_eq!(recovery.entries[0].branch_id, child);
        assert_eq!(
            recovery.entries[0].status,
            BranchCreateRecoveryStatus::Retained
        );
        assert!(recovery.entries[0].error.is_some());
        assert!(writer
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .is_err());
        assert!(writer.delete_branch(BranchSelector::Id(child)).is_err());
        assert_eq!(std::fs::read(&head_path).unwrap(), damaged_head);
        assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
        assert_eq!(objects.read(orphan).unwrap(), b"orphan");
        assert_eq!(values(&mut writer).len(), 1);
    }
}

#[test]
fn pending_recovery_preserves_live_creators_and_never_admits_busy_main() {
    use hawdb::{BranchCreateRecoveryLimits, BranchCreateRecoveryStatus};
    use hawdb_storage::ownership::DatabaseDirectoryLease;

    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let project = Project::new();
        let mut writer = Database::open_with_durability(&project.0, durability).unwrap();
        writer.query("CREATE (:Memory {id: 'inherited'})").unwrap();
        let child = pending_children(&project.0, &mut writer, 1)[0];
        writer
            .query("CREATE (:Memory {id: 'parent-only'})")
            .unwrap();
        let catalog_path = project.0.join("branches/catalog.hawdb");
        let before = std::fs::read(&catalog_path).unwrap();
        let lease =
            DatabaseDirectoryLease::acquire(&project.0.join("branches").join(child.to_string()))
                .unwrap();
        let mut metadata = Database::open(&project.0).unwrap();
        let report = metadata.branch_create_recovery_report().unwrap();
        assert_eq!(report.pending_at_start, 1);
        assert_eq!(report.entries[0].branch_id, child);
        assert_eq!(report.entries[0].status, BranchCreateRecoveryStatus::Busy);
        assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
        assert_eq!(
            metadata
                .file_descriptor_metrics()
                .unwrap()
                .admitted_runtimes,
            1
        );
        drop(lease);

        let report = metadata
            .recover_pending_branch_creates(BranchCreateRecoveryLimits::default())
            .unwrap();
        assert_eq!(
            report.entries[0].status,
            BranchCreateRecoveryStatus::Completed
        );
        assert_eq!(
            metadata
                .file_descriptor_metrics()
                .unwrap()
                .admitted_runtimes,
            1
        );
        assert!(matches!(
            metadata.commit_epoch(),
            Err(HawDBError::BranchBusy { .. })
        ));
        metadata
            .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child)])
            .unwrap();
        assert_eq!(
            values(&mut metadata),
            vec![BTreeMap::from([(
                "id".into(),
                Value::String("inherited".into()),
            )])]
        );
        assert_eq!(values(&mut writer).len(), 2);
    }
}

#[test]
fn writable_open_recovers_pending_children_with_aggregate_limits_and_no_runtime() {
    use hawdb::{BranchCreateRecoveryLimits, BranchCreateRecoveryStatus};

    for limits in [
        BranchCreateRecoveryLimits {
            max_creates: 1,
            ..Default::default()
        },
        BranchCreateRecoveryLimits {
            max_files: 1,
            ..Default::default()
        },
        BranchCreateRecoveryLimits {
            max_bytes: 1,
            ..Default::default()
        },
    ] {
        let project = Project::new();
        let mut writer = Database::open(&project.0).unwrap();
        let children = pending_children(&project.0, &mut writer, 2);
        drop(writer);
        let catalog_path = project.0.join("branches/catalog.hawdb");
        let before = std::fs::read(&catalog_path).unwrap();
        let mut metadata = Database::open_with_config(
            &project.0,
            DatabaseConfig {
                max_open_files: 4,
                branch_create_recovery_limits: limits,
                ..Default::default()
            },
        )
        .unwrap();
        assert_no_runtime(&metadata);
        let report = metadata.branch_create_recovery_report().unwrap();
        assert_eq!(report.pending_at_start, 2);
        assert!(report.limit_exceeded);
        assert!(report.admitted_files <= limits.max_files);
        assert!(report.admitted_bytes <= limits.max_bytes);
        assert_eq!(report.entries.len(), 1);
        assert_eq!(report.unattempted, 1);
        if limits.max_creates == 1 {
            assert_eq!(
                report.entries[0].status,
                BranchCreateRecoveryStatus::Completed
            );
        } else {
            assert_eq!(
                report.entries[0].status,
                BranchCreateRecoveryStatus::Retained
            );
            assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
        }
        let retry = metadata
            .recover_pending_branch_creates(BranchCreateRecoveryLimits::default())
            .unwrap();
        assert!(!retry.limit_exceeded);
        assert!(retry
            .entries
            .iter()
            .all(|entry| entry.status == BranchCreateRecoveryStatus::Completed));
        for id in children {
            assert_eq!(
                metadata
                    .describe_branch(BranchSelector::Id(id))
                    .unwrap()
                    .state,
                hawdb::BranchLifecycleState::Ready
            );
        }
        assert_no_runtime(&metadata);
        assert!(metadata.file_descriptor_metrics().unwrap().high_water <= 4);
        assert!(metadata
            .recover_pending_branch_creates(BranchCreateRecoveryLimits::default())
            .unwrap()
            .entries
            .is_empty());
    }
}

#[test]
fn pending_recovery_retains_corruption_and_aborts_only_known_missing_head_or_wal() {
    use hawdb::BranchCreateRecoveryStatus;
    use hawdb_storage::immutable_object::ImmutableObjectStore;
    use hawdb_storage::sealed_root::SealedRoot;
    use hawdb_storage::{artifact_files::wal_generation_file, branch_head::read_branch_head};

    for damage in [
        "head",
        "wal",
        "root",
        "checkpoint",
        "absent-head",
        "absent-wal",
    ] {
        let project = Project::new();
        let mut writer = Database::open(&project.0).unwrap();
        writer.query("CREATE (:Memory {id: 'retained'})").unwrap();
        let child = pending_children(&project.0, &mut writer, 1)[0];
        drop(writer);
        let directory = project.0.join("branches").join(child.to_string());
        let head_path = directory.join("branch.head");
        let head = read_branch_head(&head_path).unwrap();
        let wal_path = directory.join(wal_generation_file(head.active_wal.generation));
        let objects = ImmutableObjectStore::open(project.0.join("branches/objects")).unwrap();
        let path = match damage {
            "head" | "absent-head" => head_path.clone(),
            "wal" | "absent-wal" => wal_path.clone(),
            "root" => objects.object_path(head.sealed_root),
            "checkpoint" => {
                let root = SealedRoot::decode(&objects.read(head.sealed_root).unwrap()).unwrap();
                objects.object_path(root.checkpoint_references[0])
            }
            _ => unreachable!(),
        };
        let missing_pair = damage.starts_with("absent-");
        if missing_pair {
            std::fs::remove_file(&path).unwrap();
        } else {
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[0] ^= 0xff;
            std::fs::write(&path, bytes).unwrap();
        }
        let retained = if path == head_path {
            &wal_path
        } else {
            &head_path
        };
        let evidence = std::fs::read(retained).unwrap();
        let catalog_path = project.0.join("branches/catalog.hawdb");
        let before = std::fs::read(&catalog_path).unwrap();
        let mut database = Database::open(&project.0).unwrap();
        assert_no_runtime(&database);
        let report = database.branch_create_recovery_report().unwrap();
        assert_eq!(report.entries.len(), 1);
        assert_eq!(report.entries[0].branch_id, child);
        assert_eq!(std::fs::read(retained).unwrap(), evidence);
        if missing_pair {
            assert_eq!(
                report.entries[0].status,
                BranchCreateRecoveryStatus::Aborted
            );
            assert!(!path.exists());
        } else {
            assert_eq!(
                report.entries[0].status,
                BranchCreateRecoveryStatus::Retained
            );
            assert!(report.entries[0].error.is_some());
            assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
            assert!(database
                .create_branch(BranchCreateRequest {
                    name: Some("pending-0".into()),
                    parent: BranchSelector::Name("main".into()),
                    expected_source_commit_epoch: head.logical_commit_epoch,
                    owner: None,
                    idempotency_key: "recover-pending-0".into(),
                })
                .is_err());
            assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
        }
    }
}

#[test]
fn readonly_pending_open_does_not_recreate_missing_directories_or_repair_receipts() {
    use hawdb::BranchCreateRecoveryLimits;

    let project = Project::new();
    let mut writer = Database::open(&project.0).unwrap();
    let child = pending_children(&project.0, &mut writer, 1)[0];
    drop(writer);
    let directory = project.0.join("branches").join(child.to_string());
    std::fs::remove_dir_all(&directory).unwrap();
    let catalog_path = project.0.join("branches/catalog.hawdb");
    let before = std::fs::read(&catalog_path).unwrap();
    let mut reader = Database::open_with_config(
        &project.0,
        DatabaseConfig {
            read_only: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(reader.branch_create_recovery_report().is_none());
    assert!(reader
        .recover_pending_branch_creates(BranchCreateRecoveryLimits::default())
        .is_err());
    assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
    assert!(!directory.exists());
    assert_no_runtime(&reader);
}

#[test]
fn concurrent_pending_reports_and_retries_preserve_deferred_main_admission() {
    use hawdb::{BranchCreateRecoveryLimits, BranchCreateRecoveryStatus, ConcurrentDatabase};
    use hawdb_storage::ownership::DatabaseDirectoryLease;

    let project = Project::new();
    let mut writer = Database::open(&project.0).unwrap();
    let child = pending_children(&project.0, &mut writer, 1)[0];
    let lease =
        DatabaseDirectoryLease::acquire(&project.0.join("branches").join(child.to_string()))
            .unwrap();
    let shared = ConcurrentDatabase::open(&project.0).unwrap();
    let report = shared.branch_create_recovery_report().unwrap().unwrap();
    assert_eq!(report.entries[0].status, BranchCreateRecoveryStatus::Busy);
    drop(lease);
    let report = shared
        .recover_pending_branch_creates(BranchCreateRecoveryLimits::default())
        .unwrap();
    assert_eq!(report.entries[0].branch_id, child);
    assert_eq!(
        report.entries[0].status,
        BranchCreateRecoveryStatus::Completed
    );
    assert_eq!(
        shared.branch_create_recovery_report().unwrap(),
        Some(report)
    );
    assert_eq!(
        writer.file_descriptor_metrics().unwrap().admitted_runtimes,
        1
    );
    assert!(matches!(
        shared.commit_epoch(),
        Err(HawDBError::BranchBusy { .. })
    ));
}

#[test]
fn stale_pending_recovery_does_not_recreate_a_reclaimed_successful_child() {
    use hawdb::{BranchCreateRecoveryLimits, BranchCreateRecoveryStatus, BranchReclamationLimits};
    use hawdb_storage::branch_project::ProjectMetadata;

    let project = Project::new();
    let mut writer = Database::open(&project.0).unwrap();
    let child = pending_children(&project.0, &mut writer, 1)[0];
    let stale = ProjectMetadata::open(&project.0, writer.config().max_open_files).unwrap();
    writer
        .recover_pending_branch_creates(BranchCreateRecoveryLimits::default())
        .unwrap();
    writer.delete_branch(BranchSelector::Id(child)).unwrap();
    let report = writer
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert_eq!(report.reclaimed_branch_directories, 1);
    let directory = project.0.join("branches").join(child.to_string());
    assert!(!directory.exists());
    let catalog_path = project.0.join("branches/catalog.hawdb");
    let before = std::fs::read(&catalog_path).unwrap();
    let report = stale
        .recover_pending_creates(BranchCreateRecoveryLimits::default())
        .unwrap();
    assert_eq!(
        report.entries[0].status,
        BranchCreateRecoveryStatus::Completed
    );
    assert!(!directory.exists());
    assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
}
