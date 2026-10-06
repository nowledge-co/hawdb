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

use super::{Database, DatabaseConfig, Value};
use crate::{BranchSelector, DurabilityPolicy, HawDBError, Uuid};
use hawdb_storage::artifact_files::wal_generation_file;
use hawdb_storage::file_descriptors::ProjectFileDescriptors;
use hawdb_storage::power_loss::image::{CrashPlan, ImageLimits, PersistOperation};
use hawdb_storage::power_loss::{
    IoEvent, ObservationBoundary, ObservationPoint, PowerLossModel, PowerLossSnapshot,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "full-text-search")]
mod search_projection;

struct Fixture {
    root: PathBuf,
    model: PowerLossModel,
    next_image: usize,
}

// Each fault schedule owns its image only until that schedule's reopened
// handles have closed. Keeping every image until the fixture ends makes the
// disk requirement grow with the number of generated fault plans.
struct MaterializedImage(PathBuf);

impl std::ops::Deref for MaterializedImage {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Path> for MaterializedImage {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for MaterializedImage {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0)
            && !std::thread::panicking()
        {
            panic!("remove crash image {}: {error}", self.0.display());
        }
    }
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "hawdb-power-branch-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let project = ProjectFileDescriptors::acquire(&root, 32).unwrap();
        let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
        Self {
            root,
            model,
            next_image: 0,
        }
    }

    fn image(&mut self, snapshot: &PowerLossSnapshot, plan: &CrashPlan) -> MaterializedImage {
        let root = MaterializedImage(
            self.root
                .with_extension(format!("crash-{}", self.next_image)),
        );
        self.next_image += 1;
        snapshot
            .crash(plan)
            .unwrap()
            .materialize(&root)
            .unwrap_or_else(|error| panic!("materialize crash image {}: {error}", root.display()));
        root
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn open(root: &Path, durability: DurabilityPolicy) -> Database {
    Database::open_with_durability_and_config(
        root,
        durability,
        DatabaseConfig {
            max_open_files: 32,
            ..Default::default()
        },
    )
    .unwrap()
}

fn commit_mixed(database: &mut Database) {
    let mut transaction = database.begin_transaction().unwrap();
    transaction
        .query_sql("CREATE TABLE atomic_tail (id BIGINT PRIMARY KEY, payload TEXT)")
        .unwrap();
    transaction
        .query_sql("INSERT INTO atomic_tail (id, payload) VALUES (7, 'whole')")
        .unwrap();
    transaction
        .query("CREATE (:Memory {id: 'atomic-tail'})")
        .unwrap();
    transaction.commit().unwrap();
}

/// Every successful admission must expose the entire mixed transaction or
/// none of its schema/relational/graph changes, even for a torn WAL image.
fn assert_atomic(database: &mut Database, main: Uuid) -> bool {
    assert_eq!(
        database
            .describe_branch(BranchSelector::Name("main".into()))
            .unwrap()
            .id,
        main
    );
    let columns = database.query_sql("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'atomic_tail' ORDER BY ordinal_position").unwrap();
    let graph = database
        .query("MATCH (m:Memory {id: 'atomic-tail'}) RETURN m.id AS id")
        .unwrap();
    if columns.rows.is_empty() {
        assert!(
            graph.rows.is_empty(),
            "graph changes survived without transaction schema"
        );
        false
    } else {
        assert_eq!(columns.rows.len(), 2);
        assert_eq!(columns.rows[0]["column_name"], Value::String("id".into()));
        assert_eq!(
            columns.rows[1]["column_name"],
            Value::String("payload".into())
        );
        let rows = database
            .query_sql("SELECT id, payload FROM atomic_tail")
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0]["id"], Value::Int(7));
        assert_eq!(rows.rows[0]["payload"], Value::String("whole".into()));
        assert_eq!(graph.rows.len(), 1);
        true
    }
}

#[test]
fn synchronized_mixed_commit_survives_loss_of_every_uncovered_write() {
    let mut fixture = Fixture::new();
    let mut database = open(&fixture.root, DurabilityPolicy::SyncOnEveryWrite);
    let main = database
        .describe_branch(BranchSelector::Name("main".into()))
        .unwrap()
        .id;
    database.checkpoint().unwrap();
    commit_mixed(&mut database);
    let epoch = database.commit_epoch().unwrap();
    let snapshot = fixture.model.capture().unwrap();
    drop(database);
    let root = fixture.image(&snapshot, &CrashPlan::default());
    let mut recovered = open(&root, DurabilityPolicy::SyncOnEveryWrite);
    assert_eq!(recovered.commit_epoch().unwrap(), epoch);
    assert!(assert_atomic(&mut recovered, main));
}

#[test]
fn relaxed_mixed_commit_checkpoint_preserves_the_complete_transaction() {
    let mut fixture = Fixture::new();
    let mut database = open(&fixture.root, DurabilityPolicy::SyncOnCheckpoint);
    let main = database
        .describe_branch(BranchSelector::Name("main".into()))
        .unwrap()
        .id;
    database.checkpoint().unwrap();
    commit_mixed(&mut database);
    let epoch = database.commit_epoch().unwrap();
    let snapshot = fixture.model.capture().unwrap();
    drop(database);
    let root = fixture.image(&snapshot, &CrashPlan::default());
    let mut recovered = open(&root, DurabilityPolicy::SyncOnCheckpoint);
    assert_eq!(recovered.commit_epoch().unwrap(), epoch);
    assert!(assert_atomic(&mut recovered, main));
}

fn assert_graph_schema_atomic(database: &mut Database) -> bool {
    let schema_present = database
        .runtime
        .get()
        .unwrap()
        .catalog
        .label_id("UnsyncedTail")
        .is_some();
    let rows = database
        .query("MATCH (m:UnsyncedTail) RETURN m.id AS id")
        .unwrap();
    if schema_present {
        assert_eq!(rows.rows.len(), 1);
        assert!(
            matches!(&rows.rows[0]["id"], Value::String(value) if value == "whole" || value == "second")
        );
    } else {
        assert!(rows.rows.is_empty());
    }
    schema_present
}

#[test]
fn relaxed_graph_schema_commit_is_whole_lost_or_fail_closed_under_physical_wal_faults() {
    let mut fixture = Fixture::new();
    let mut database = open(&fixture.root, DurabilityPolicy::SyncOnCheckpoint);
    let main = database
        .describe_branch(BranchSelector::Name("main".into()))
        .unwrap()
        .id;
    database.checkpoint().unwrap();
    let source_epoch = database.commit_epoch().unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    transaction
        .query("CREATE (:UnsyncedTail {id: 'whole'})")
        .unwrap();
    transaction.commit().unwrap();
    database
        .query("MATCH (m:UnsyncedTail) SET m.id = 'second' RETURN m.id AS id")
        .unwrap();
    let snapshot = fixture.model.capture().unwrap();
    let directory = PathBuf::from("branches").join(main.to_string());
    let head = hawdb_storage::branch_head::read_branch_head(
        &fixture.root.join(&directory).join("branch.head"),
    )
    .unwrap();
    let wal = directory.join(wal_generation_file(head.active_wal.generation));
    let writes = snapshot.uncovered_writes(&wal).unwrap();
    assert!(
        writes.len() >= 2,
        "physical reorder must include writes from both unsynchronized acknowledged transactions"
    );
    drop(database);
    let lost = fixture.image(&snapshot, &CrashPlan::default());
    let mut recovered = open(&lost, DurabilityPolicy::SyncOnCheckpoint);
    assert_eq!(recovered.commit_epoch().unwrap(), source_epoch);
    assert!(!assert_graph_schema_atomic(&mut recovered));
    drop(recovered);
    let whole = fixture.image(&snapshot, &snapshot.persist_all_plan());
    assert!(assert_graph_schema_atomic(&mut open(
        &whole,
        DurabilityPolicy::SyncOnCheckpoint
    )));
    let mut faults: Vec<_> = writes
        .iter()
        .filter(|write| write.length > 1)
        .map(|write| CrashPlan {
            persistence: vec![PersistOperation::TornWrite {
                operation: write.operation,
                bytes: 0..write.length / 2,
            }],
        })
        .collect();
    faults.push(CrashPlan {
        persistence: writes
            .iter()
            .rev()
            .map(|write| PersistOperation::Whole(write.operation))
            .collect(),
    });
    for write in &writes {
        faults.push(CrashPlan {
            persistence: vec![PersistOperation::Whole(write.operation)],
        });
    }
    for fault in faults {
        let root = fixture.image(&snapshot, &fault);
        let evidence = recovery_evidence(&root);
        let result = Database::open_with_durability_and_config(
            &root,
            DurabilityPolicy::SyncOnCheckpoint,
            DatabaseConfig {
                max_open_files: 32,
                ..Default::default()
            },
        );
        match result {
            Ok(mut recovered) => match recovered.commit_epoch() {
                Ok(_) => {
                    assert_graph_schema_atomic(&mut recovered);
                }
                Err(error) => assert_recovery_rejection(&root, &evidence, error),
            },
            Err(error) => assert_recovery_rejection(&root, &evidence, error),
        }
    }
}

fn assert_recovery_rejection(root: &Path, before: &BTreeMap<PathBuf, Vec<u8>>, error: HawDBError) {
    assert!(
        matches!(&error, HawDBError::StorageIntegrity(_))
            || matches!(&error, HawDBError::Storage(message) if message.starts_with("strict WAL recovery rejected torn tail:")),
        "unexpected recovery error: {error}"
    );
    let after = recovery_evidence(root);
    let changed: Vec<_> = before
        .keys()
        .chain(after.keys())
        .filter(|path| before.get(*path) != after.get(*path))
        .take(16)
        .collect();
    assert!(
        after == *before,
        "failed admission changed publication/WAL/object evidence at {changed:?}"
    );
}

fn recovery_evidence(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = BTreeMap::new();
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                pending.push(path);
                continue;
            }
            let relative = path.strip_prefix(root).unwrap();
            if relative.starts_with("objects")
                || relative.starts_with("branches/objects")
                || relative == Path::new("manifest.hawdb")
                || relative == Path::new("branches/catalog.hawdb")
                || path.file_name().unwrap() == "branch.head"
                // UUID-private WAL files are authoritative. Unpublished data
                // and runtime scratch WALs may be removed after failed mount.
                || relative.starts_with("branches") && relative.components().count() == 3
                    && path
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with("wal.")
            {
                files.insert(relative.to_path_buf(), std::fs::read(path).unwrap());
            }
        }
    }
    files
}

#[test]
fn checkpoint_head_replacement_cuts_recover_complete_schema_and_data() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
            let mut fixture = Fixture::new();
            let mut database = open(&fixture.root, durability);
            commit_mixed(&mut database);
            let covered_epoch = database.commit_epoch().unwrap();
            database
                .query("CREATE (:Memory {id: 'checkpoint-tail'})")
                .unwrap();
            let main = database
                .describe_branch(BranchSelector::Name("main".into()))
                .unwrap()
                .id;
            let epoch = database.commit_epoch().unwrap();
            fixture
                .model
                .observe(ObservationPoint {
                    event: IoEvent::Rename,
                    relative_path: PathBuf::from("branches")
                        .join(main.to_string())
                        .join("branch.head"),
                    boundary,
                    skip_matches: 0,
                    include_descendants: false,
                    keep_last: false,
                })
                .unwrap();
            database.checkpoint().unwrap();
            fixture.model.capture().unwrap();
            let snapshot = fixture
                .model
                .take_observation()
                .unwrap()
                .expect("actual checkpoint head replacement must be observed");
            drop(database);
            for plan in publication_fault_plans(&snapshot) {
                let root = fixture.image(&snapshot, &plan);
                let mut recovered = open(&root, durability);
                let recovered_epoch = recovered.commit_epoch().unwrap();
                if durability == DurabilityPolicy::SyncOnEveryWrite
                    || plan == snapshot.persist_all_plan()
                {
                    assert_eq!(
                        recovered_epoch, epoch,
                        "{durability:?} {boundary:?} {plan:?}"
                    );
                } else {
                    // The observed checkpoint has not completed its head
                    // barrier yet. A relaxed tail may still be lost, while the
                    // preceding completed mixed checkpoint remains covered.
                    assert!(recovered_epoch == covered_epoch || recovered_epoch == epoch);
                }
                assert!(assert_atomic(&mut recovered, main));
                assert_eq!(
                    recovered
                        .query("MATCH (m:Memory {id: 'checkpoint-tail'}) RETURN m.id AS id")
                        .unwrap()
                        .rows
                        .len(),
                    usize::from(recovered_epoch == epoch)
                );
            }
        }
    }
}

// Namespace publications can survive independently of other uncovered names.
// Retain file/directory barriers while reversing or isolating the actual pending
// operations, rather than assuming that every in-flight publication survives.
fn publication_fault_plans(snapshot: &PowerLossSnapshot) -> Vec<CrashPlan> {
    let complete = snapshot.persist_all_plan();
    let mut plans = vec![CrashPlan::default(), complete.clone()];
    let reversed = CrashPlan {
        persistence: complete.persistence.iter().rev().cloned().collect(),
    };
    if !plans.contains(&reversed) {
        plans.push(reversed);
    }
    for operation in &complete.persistence {
        let isolated = CrashPlan {
            persistence: vec![operation.clone()],
        };
        if !plans.contains(&isolated) {
            plans.push(isolated);
        }
    }
    eprintln!(
        "branch-power-publication-v1 path={:?} uncovered={} plans={}",
        snapshot.observed_path(),
        complete.persistence.len(),
        plans.len()
    );
    plans
}

const CREATE_BRANCH: &str = "CREATE BRANCH NAME $1 FROM ID $2 AT REVISION $3 REQUEST KEY $4";

fn fork(database: &mut Database, parent: Uuid, name: &str) -> Uuid {
    let request = [
        Value::String(name.into()),
        Value::Uuid(parent),
        Value::Int(database.commit_epoch().unwrap() as i64),
        Value::String(format!("physical-{name}")),
    ];
    let created = database
        .query_sql_with_params(CREATE_BRANCH, &request)
        .unwrap();
    let Value::Uuid(id) = created.rows[0]["branch_id"] else {
        panic!("branch UUID")
    };
    id
}

#[test]
fn catalog_publication_cuts_preserve_creation_identity_after_a_lost_response() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        for phase in 0..2 {
            for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
                let mut fixture = Fixture::new();
                let mut database = open(&fixture.root, durability);
                commit_mixed(&mut database);
                let main = database
                    .describe_branch(BranchSelector::Name("main".into()))
                    .unwrap()
                    .id;
                let request = [
                    Value::String("lost-response".into()),
                    Value::Uuid(main),
                    Value::Int(database.commit_epoch().unwrap() as i64),
                    Value::String("same-create-outcome".into()),
                ];
                fixture
                    .model
                    .observe(ObservationPoint {
                        event: IoEvent::Rename,
                        relative_path: PathBuf::from("branches/catalog.hawdb"),
                        boundary,
                        // Capture either the Creating reservation or Ready completion,
                        // before its directory barrier and before acknowledgement.
                        skip_matches: phase,
                        include_descendants: false,
                        keep_last: false,
                    })
                    .unwrap();
                let created = database
                    .query_sql_with_params(CREATE_BRANCH, &request)
                    .unwrap();
                let child = created.rows[0]["branch_id"].clone();
                let acknowledged = fixture.model.capture().unwrap();
                let snapshot = fixture
                    .model
                    .take_observation()
                    .unwrap()
                    .expect("actual catalog completion must be observed");
                drop(database);
                let plans = publication_fault_plans(&snapshot)
                    .into_iter()
                    .map(|plan| (&snapshot, plan, true))
                    .chain(std::iter::once((
                        &acknowledged,
                        CrashPlan::default(),
                        false,
                    )));
                for (snapshot, plan, allow_aborted_reservation) in plans {
                    let root = fixture.image(snapshot, &plan);
                    let catalog = hawdb_storage::branch_catalog::read_catalog(
                        &root.join("branches/catalog.hawdb"),
                    )
                    .unwrap();
                    let reserved = catalog
                        .branches
                        .iter()
                        .any(|record| Value::Uuid(record.id.as_uuid()) == child);
                    let mut recovered = open(&root, durability);
                    if let Some(pending) = catalog.branches.iter().find(|record| {
                        Value::Uuid(record.id.as_uuid()) == child
                            && record.create_outcome
                                == hawdb_storage::branch_catalog::CreateOutcome::Pending
                    }) {
                        let entry = recovered
                            .branch_create_recovery_report()
                            .unwrap()
                            .entries
                            .iter()
                            .find(|entry| entry.branch_id == pending.id.as_uuid())
                            .expect("ordinary open must attempt the interrupted receipt");
                        assert!(matches!(
                            entry.status,
                            crate::BranchCreateRecoveryStatus::Completed
                                | crate::BranchCreateRecoveryStatus::Aborted
                        ));
                    }
                    let retried = match recovered.query_sql_with_params(CREATE_BRANCH, &request) {
                        Ok(retried) => retried,
                        Err(HawDBError::Storage(message))
                            if allow_aborted_reservation
                                && message.contains("branch creation was aborted") =>
                        {
                            // An in-flight catalog rename is not an acknowledgement.
                            // Losing its unsynced UUID directory can leave the durable
                            // Creating reservation with a known absent head. Recovery
                            // must persist that same UUID's aborted outcome, not reuse
                            // the request key to invent another live or empty branch.
                            let catalog_path = root.join("branches/catalog.hawdb");
                            let catalog =
                                hawdb_storage::branch_catalog::read_catalog(&catalog_path).unwrap();
                            let Value::Uuid(child_id) = child else {
                                panic!("branch UUID")
                            };
                            let records: Vec<_> = catalog
                                .branches
                                .iter()
                                .filter(|record| record.id.as_uuid() == child_id)
                                .collect();
                            assert_eq!(records.len(), 1);
                            assert_eq!(
                                records[0].create_outcome,
                                hawdb_storage::branch_catalog::CreateOutcome::Aborted
                            );
                            let catalog_before = std::fs::read(&catalog_path).unwrap();
                            assert!(
                                matches!(recovered.query_sql_with_params(CREATE_BRANCH, &request),
                        Err(HawDBError::Storage(message)) if message.contains("branch creation was aborted"))
                            );
                            assert!(std::fs::read(catalog_path).unwrap() == catalog_before);
                            assert!(assert_atomic(&mut recovered, main));
                            continue;
                        }
                        Err(error) => panic!("unexpected creation retry error: {error}"),
                    };
                    let recovered_child = retried.rows[0]["branch_id"].clone();
                    if reserved || !allow_aborted_reservation {
                        assert_eq!(recovered_child, child);
                    }
                    // A reservation that never became durable has no recorded outcome.
                    // The new attempt still owns one durable, idempotent identity.
                    assert_eq!(
                        recovered
                            .query_sql_with_params(CREATE_BRANCH, &request)
                            .unwrap()
                            .rows[0]["branch_id"],
                        recovered_child,
                    );
                    assert!(assert_atomic(&mut recovered, main));
                    recovered
                        .query_sql_with_params(
                            "USE BRANCH ID $1",
                            std::slice::from_ref(&recovered_child),
                        )
                        .unwrap();
                    let Value::Uuid(child_id) = recovered_child else {
                        panic!("branch UUID")
                    };
                    assert_eq!(
                        recovered.query_sql("SHOW CURRENT BRANCH").unwrap().rows[0]["branch_id"],
                        Value::Uuid(child_id)
                    );
                    assert_eq!(
                        recovered
                            .query_sql("SELECT payload FROM atomic_tail WHERE id = 7")
                            .unwrap()
                            .rows[0]["payload"],
                        Value::String("whole".into())
                    );
                }
            }
        }
    }
}

#[test]
fn pending_recovery_publication_cuts_reopen_the_same_complete_child() {
    use hawdb_storage::branch_catalog::{self, BranchState, CreateOutcome};

    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
            let mut fixture = Fixture::new();
            let mut database = open(&fixture.root, durability);
            commit_mixed(&mut database);
            let main = database
                .describe_branch(BranchSelector::Name("main".into()))
                .unwrap()
                .id;
            let child = fork(&mut database, main, "recovering-child");
            // The creation-cut matrix above supplies actual interrupted
            // receipts. Here a durable pending fixture isolates a second
            // interruption while the ordinary opener publishes recovery.
            let catalog_path = fixture.root.join("branches/catalog.hawdb");
            let mut catalog = branch_catalog::read_catalog(&catalog_path).unwrap();
            let pending = catalog
                .branches
                .iter_mut()
                .find(|branch| branch.id.as_uuid() == child)
                .unwrap();
            pending.state = BranchState::Creating;
            pending.create_outcome = CreateOutcome::Pending;
            branch_catalog::write_catalog(&catalog_path, &catalog).unwrap();
            drop(database);
            fixture
                .model
                .observe(ObservationPoint {
                    event: IoEvent::Rename,
                    relative_path: PathBuf::from("branches/catalog.hawdb"),
                    boundary,
                    skip_matches: 0,
                    include_descendants: false,
                    keep_last: false,
                })
                .unwrap();
            let recovered = open(&fixture.root, durability);
            let report = recovered.branch_create_recovery_report().unwrap();
            assert_eq!(report.entries.len(), 1);
            assert_eq!(report.entries[0].branch_id, child);
            assert_eq!(
                report.entries[0].status,
                crate::BranchCreateRecoveryStatus::Completed
            );
            assert_eq!(
                recovered
                    .file_descriptor_metrics()
                    .unwrap()
                    .admitted_runtimes,
                0
            );
            let acknowledged = fixture.model.capture().unwrap();
            let cut = fixture
                .model
                .take_observation()
                .unwrap()
                .expect("actual recovery publication");
            drop(recovered);
            let plans = publication_fault_plans(&cut)
                .into_iter()
                .map(|plan| (&cut, plan))
                .chain(std::iter::once((&acknowledged, CrashPlan::default())));
            for (snapshot, plan) in plans {
                let root = fixture.image(snapshot, &plan);
                let mut recovered = open(&root, durability);
                assert_eq!(
                    recovered
                        .describe_branch(BranchSelector::Id(child))
                        .unwrap()
                        .state,
                    crate::BranchLifecycleState::Ready
                );
                let catalog_path = root.join("branches/catalog.hawdb");
                let before = std::fs::read(&catalog_path).unwrap();
                assert!(recovered
                    .recover_pending_branch_creates(crate::BranchCreateRecoveryLimits::default())
                    .unwrap()
                    .entries
                    .is_empty());
                assert_eq!(std::fs::read(&catalog_path).unwrap(), before);
                assert!(assert_atomic(&mut recovered, main));
                recovered
                    .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child)])
                    .unwrap();
                assert!(assert_atomic(&mut recovered, main));
            }
        }
    }
}

fn assert_gc_survivors(database: &mut Database, main: Uuid, deleted: Uuid, nested: Uuid) {
    assert_eq!(
        database
            .describe_branch(BranchSelector::Id(deleted))
            .unwrap()
            .state,
        crate::BranchLifecycleState::Deleted
    );
    assert!(database
        .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(deleted)])
        .is_err());
    database
        .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(main)])
        .unwrap();
    assert!(assert_atomic(database, main));
    database
        .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(nested)])
        .unwrap();
    let rows = database
        .query_sql("SELECT id, payload, tag FROM atomic_tail")
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0]["id"], Value::Int(7));
    assert_eq!(rows.rows[0]["payload"], Value::String("child".into()));
    assert_eq!(rows.rows[0]["tag"], Value::String("inherited".into()));
    assert_eq!(
        database
            .query("MATCH (m:Memory {id: 'nested-inherited'}) RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[derive(Debug, Clone, Copy)]
enum GcCut {
    OrphanUnlink,
    DirectoryRetirement,
    FirstRetiredUnlink,
    RetiredDirectoryUnlink,
}

#[test]
fn deletion_and_gc_cuts_keep_an_unleased_nested_branch_recoverable() {
    use hawdb_storage::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};

    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        for gc_cut in [
            GcCut::OrphanUnlink,
            GcCut::DirectoryRetirement,
            GcCut::FirstRetiredUnlink,
            GcCut::RetiredDirectoryUnlink,
        ] {
            let mut fixture = Fixture::new();
            let mut database = open(&fixture.root, durability);
            commit_mixed(&mut database);
            let main = database
                .describe_branch(BranchSelector::Name("main".into()))
                .unwrap()
                .id;
            let child = fork(&mut database, main, "parent-to-delete");
            database
                .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child)])
                .unwrap();
            database
                .query_sql("ALTER TABLE atomic_tail ADD COLUMN tag TEXT")
                .unwrap();
            database
                .query_sql(
                    "UPDATE atomic_tail SET payload = 'child', tag = 'inherited' WHERE id = 7",
                )
                .unwrap();
            database
                .query("CREATE (:Memory {id: 'nested-inherited'})")
                .unwrap();
            let nested = fork(&mut database, child, "surviving-descendant");
            database
                .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(main)])
                .unwrap();
            let revision = database
                .describe_branch(BranchSelector::Id(child))
                .unwrap()
                .metadata_revision;
            database
                .query_sql_with_params(
                    "DROP BRANCH ID $1 AT REVISION $2",
                    &[Value::Uuid(child), Value::Int(revision as i64)],
                )
                .unwrap();
            let deletion = fixture.model.capture().unwrap();
            let catalog_path = fixture.root.join("branches/catalog.hawdb");
            let mut objects =
                ImmutableObjectStore::open(catalog_path.parent().unwrap().join("objects")).unwrap();
            let orphan = ObjectReference::for_bytes(
                ObjectKind::Checkpoint,
                1,
                b"unreachable physical qualification object",
            );
            objects
                .publish(orphan, b"unreachable physical qualification object")
                .unwrap();
            let orphan_path = objects.object_path(orphan);
            let deleted_directory = PathBuf::from("branches").join(child.to_string());
            let retired_directory = PathBuf::from("branches").join(format!(".reclaim-{child}"));
            let (event, relative_path, include_descendants) = match gc_cut {
                GcCut::OrphanUnlink => (
                    IoEvent::Remove,
                    orphan_path
                        .strip_prefix(&fixture.root)
                        .unwrap()
                        .to_path_buf(),
                    false,
                ),
                GcCut::DirectoryRetirement => (IoEvent::Rename, retired_directory.clone(), false),
                GcCut::FirstRetiredUnlink => (IoEvent::Remove, retired_directory.clone(), true),
                GcCut::RetiredDirectoryUnlink => {
                    (IoEvent::Remove, retired_directory.clone(), false)
                }
            };
            fixture
                .model
                .observe(ObservationPoint {
                    event,
                    relative_path,
                    boundary: ObservationBoundary::After,
                    skip_matches: 0,
                    include_descendants,
                    keep_last: false,
                })
                .unwrap();
            let report = database
                .reclaim_branch_storage(crate::BranchReclamationLimits::default())
                .unwrap();
            assert!(report.reclaimed_objects >= 1);
            assert_eq!(report.reclaimed_branch_directories, 1);
            assert!(!report.deferred_for_active_leases);
            assert!(!fixture.root.join(&deleted_directory).exists());
            assert!(!fixture.root.join(&retired_directory).exists());
            let swept = fixture.model.capture().unwrap();
            let cut = fixture
                .model
                .take_observation()
                .unwrap()
                .expect("actual GC boundary must be observed");
            if matches!(gc_cut, GcCut::FirstRetiredUnlink) {
                assert_ne!(cut.observed_path(), Some(retired_directory.as_path()));
            }
            assert!(fixture.model.project().metrics().high_water <= 32);
            drop(database);
            assert_eq!(fixture.model.project().metrics().admitted_runtimes, 0);

            eprintln!("branch-power-gc-v1 durability={durability:?} cut={gc_cut:?}");
            let plans = publication_fault_plans(&cut)
                .into_iter()
                .map(|plan| (&cut, plan))
                .chain([
                    (&deletion, CrashPlan::default()),
                    (&swept, CrashPlan::default()),
                ]);
            for (snapshot, plan) in plans {
                let root = fixture.image(snapshot, &plan);
                if snapshot.observed_path().is_some()
                    && matches!(
                        gc_cut,
                        GcCut::FirstRetiredUnlink | GcCut::RetiredDirectoryUnlink
                    )
                {
                    // Destructive cleanup starts only after the retired name is
                    // synchronized, so no crash may resurrect an admissible UUID.
                    assert!(!root.join(&deleted_directory).exists());
                }
                let mut recovered = open(&root, durability);
                assert_gc_survivors(&mut recovered, main, child, nested);
                let retry = recovered
                    .reclaim_branch_storage(crate::BranchReclamationLimits::default())
                    .unwrap();
                assert!(!retry.deferred_for_active_leases);
                assert!(!root.join(&deleted_directory).exists());
                assert!(!root.join(&retired_directory).exists());
                let repeated = recovered
                    .reclaim_branch_storage(crate::BranchReclamationLimits::default())
                    .unwrap();
                assert!(!repeated.deferred_for_active_leases);
                assert_eq!(repeated.reclaimed_branch_directories, 0);
                assert_eq!(repeated.reclaimed_objects, 0);
                drop(recovered);
                assert_gc_survivors(&mut open(&root, durability), main, child, nested);
            }
        }
    }
}

#[test]
fn interrupted_legacy_bootstrap_keeps_committed_data_and_reserved_identity() {
    use hawdb_storage::branch_catalog::BranchId;
    use hawdb_storage::branch_project::ProjectSelector;
    use hawdb_storage::config::WalReplayConfig;
    use hawdb_storage::store::GraphStore;
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let mut fixture = Fixture::new();
        let mut catalog = crate::schema::Catalog::default();
        let mut legacy = GraphStore::open_with_durability_and_replay_config(
            &fixture.root,
            &mut catalog,
            DurabilityPolicy::SyncOnEveryWrite,
            WalReplayConfig {
                max_open_files: 32,
                ..Default::default()
            },
        )
        .unwrap();
        legacy
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("id".into(), Value::String("legacy-covered".into()))]),
            )
            .unwrap();
        legacy.checkpoint(&catalog).unwrap();
        let selector = ProjectSelector::new(
            BranchId::new(Uuid::from_u128(2011)).unwrap(),
            BranchId::new(Uuid::from_u128(2012)).unwrap(),
        )
        .unwrap();
        legacy.reserve_branch_project_identity(selector).unwrap();
        drop(legacy);
        fixture
            .model
            .observe(ObservationPoint {
                event: IoEvent::Rename,
                relative_path: PathBuf::from("manifest.hawdb"),
                boundary,
                skip_matches: 0,
                include_descendants: false,
                keep_last: true,
            })
            .unwrap();
        let database = open(&fixture.root, DurabilityPolicy::SyncOnEveryWrite);
        fixture.model.capture().unwrap();
        let cut = fixture
            .model
            .take_observation()
            .unwrap()
            .expect("bootstrap selector rename");
        assert_eq!(cut.observed_path(), Some(Path::new("manifest.hawdb")));
        drop(database);
        for plan in [CrashPlan::default(), cut.persist_all_plan()] {
            let root = fixture.image(&cut, &plan);
            let mut recovered = open(&root, DurabilityPolicy::SyncOnEveryWrite);
            assert_eq!(
                recovered
                    .describe_branch(BranchSelector::Name("main".into()))
                    .unwrap()
                    .id,
                selector.main_branch_id().as_uuid()
            );
            let rows = recovered
                .query("MATCH (m:Memory) RETURN m.id AS id")
                .unwrap();
            assert_eq!(rows.rows.len(), 1);
            assert_eq!(rows.rows[0]["id"], Value::String("legacy-covered".into()));
        }
    }
}

#[test]
fn source_seal_and_private_rotation_cuts_preserve_complete_recovery_prefixes() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        for case in 0..7 {
            for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
                let mut fixture = Fixture::new();
                let mut database = open(&fixture.root, durability);
                commit_mixed(&mut database);
                let main = database
                    .describe_branch(BranchSelector::Name("main".into()))
                    .unwrap()
                    .id;
                let covered_epoch = database.commit_epoch().unwrap();
                database
                    .query("CREATE (:Memory {id: 'source-tail'})")
                    .unwrap();
                let epoch = database.commit_epoch().unwrap();
                let directory = PathBuf::from("branches").join(main.to_string());
                let head_path = directory.join("branch.head");
                let head =
                    hawdb_storage::branch_head::read_branch_head(&fixture.root.join(&head_path))
                        .unwrap();
                let successor = directory.join(wal_generation_file(head.active_wal.generation + 1));
                let (event, path, descendants, skip) = match case {
                    0 => (IoEvent::CreateFile, successor.clone(), false, 0),
                    1 => (IoEvent::Write, successor.clone(), false, 0),
                    2 => (IoEvent::FileSync, successor, false, 0),
                    3 => (
                        IoEvent::HardLink,
                        PathBuf::from("branches/objects/objects/sealed-wal"),
                        true,
                        0,
                    ),
                    4 => (
                        IoEvent::HardLink,
                        PathBuf::from("branches/objects/objects/sealed-root"),
                        true,
                        0,
                    ),
                    5 => (IoEvent::Rename, head_path, false, 0),
                    // The successor name is synchronized first; the second
                    // directory barrier completes the source head replacement.
                    6 => (IoEvent::DirectorySync, directory, false, 1),
                    _ => unreachable!(),
                };
                fixture
                    .model
                    .observe(ObservationPoint {
                        event,
                        relative_path: path,
                        boundary,
                        skip_matches: skip,
                        include_descendants: descendants,
                        keep_last: false,
                    })
                    .unwrap();
                fork(&mut database, main, "seal-cut-child");
                let acknowledged = fixture.model.capture().unwrap();
                let cut = fixture
                    .model
                    .take_observation()
                    .unwrap()
                    .expect("actual source seal/rotation cut");
                eprintln!("branch-power-seal-v1 policy={durability:?} case={case} boundary={boundary:?} path={:?}", cut.observed_path().unwrap());
                drop(database);
                for (snapshot, plan, require_tail) in [
                    (
                        &cut,
                        CrashPlan::default(),
                        durability == DurabilityPolicy::SyncOnEveryWrite
                            || case == 6 && boundary == ObservationBoundary::After,
                    ),
                    (&cut, cut.persist_all_plan(), true),
                    (&acknowledged, CrashPlan::default(), true),
                ] {
                    let root = fixture.image(snapshot, &plan);
                    let mut recovered = open(&root, durability);
                    assert!(assert_atomic(&mut recovered, main));
                    let tail = recovered
                        .query("MATCH (m:Memory {id: 'source-tail'}) RETURN m.id AS id")
                        .unwrap();
                    assert!(tail.rows.len() <= 1);
                    if require_tail {
                        assert_eq!(tail.rows.len(), 1);
                    }
                    assert_eq!(
                        recovered.commit_epoch().unwrap(),
                        if tail.rows.is_empty() {
                            covered_epoch
                        } else {
                            epoch
                        }
                    );
                }
            }
        }
    }
}

#[test]
fn interrupted_admission_alias_mount_keeps_authoritative_schema_and_data() {
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let mut fixture = Fixture::new();
        let mut database = open(&fixture.root, DurabilityPolicy::SyncOnEveryWrite);
        commit_mixed(&mut database);
        let main = database
            .describe_branch(BranchSelector::Name("main".into()))
            .unwrap()
            .id;
        let epoch = database.commit_epoch().unwrap();
        drop(database);
        fixture
            .model
            .observe(ObservationPoint {
                event: IoEvent::HardLink,
                relative_path: PathBuf::from("branches")
                    .join(main.to_string())
                    .join("data"),
                boundary,
                skip_matches: 0,
                include_descendants: true,
                keep_last: false,
            })
            .unwrap();
        let database = open(&fixture.root, DurabilityPolicy::SyncOnEveryWrite);
        assert_eq!(database.commit_epoch().unwrap(), epoch);
        fixture.model.capture().unwrap();
        let cut = fixture
            .model
            .take_observation()
            .unwrap()
            .expect("actual checkpoint alias mount");
        drop(database);
        for plan in [CrashPlan::default(), cut.persist_all_plan()] {
            let root = fixture.image(&cut, &plan);
            let mut recovered = open(&root, DurabilityPolicy::SyncOnEveryWrite);
            assert_eq!(recovered.commit_epoch().unwrap(), epoch);
            assert!(assert_atomic(&mut recovered, main));
        }
    }
}

#[test]
fn schema_indexes_and_nested_data_survive_power_loss_in_configured_residencies() {
    use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};
    for mode in [
        StorageResidencyMode::Auto,
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        for indexes in [
            RelationalIndexMode::Materialized,
            RelationalIndexMode::Shadow,
            RelationalIndexMode::DemandPaged,
            RelationalIndexMode::Authoritative,
        ] {
            let mut fixture = Fixture::new();
            let config = DatabaseConfig {
                max_open_files: 32,
                storage_residency_mode: mode,
                relational_index_mode: indexes,
                ..Default::default()
            };
            // Authoritative indexes intentionally require a published canonical
            // binding. Bootstrap with Shadow, as existing authoritative callers
            // do, then exercise the branch lifecycle in the selected mode.
            let mut seed_config = config.clone();
            if indexes == RelationalIndexMode::Authoritative {
                seed_config.relational_index_mode = RelationalIndexMode::Shadow;
            }
            let mut database = Database::open_with_config(&fixture.root, seed_config).unwrap();
            commit_mixed(&mut database);
            database
                .query_sql("CREATE INDEX atomic_payload ON atomic_tail (payload)")
                .unwrap();
            database.checkpoint().unwrap();
            drop(database);
            let mut database = Database::open_with_config(&fixture.root, config.clone()).unwrap();
            let main = database
                .describe_branch(BranchSelector::Name("main".into()))
                .unwrap()
                .id;
            let child = fork(&mut database, main, "mode-child");
            database
                .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child)])
                .unwrap();
            if indexes == RelationalIndexMode::Authoritative {
                let epoch = database.commit_epoch().unwrap();
                let error = database
                    .query_sql("ALTER TABLE atomic_tail ADD COLUMN tag TEXT")
                    .unwrap_err();
                assert!(error.to_string().contains("schema-changing"), "{error}");
                assert_eq!(database.commit_epoch().unwrap(), epoch);
                drop(database);
                let mut schema_config = config.clone();
                schema_config.relational_index_mode = RelationalIndexMode::Shadow;
                database = Database::open_with_config(&fixture.root, schema_config).unwrap();
                database
                    .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child)])
                    .unwrap();
            }
            database
                .query_sql("ALTER TABLE atomic_tail ADD COLUMN tag TEXT")
                .unwrap();
            if indexes == RelationalIndexMode::Authoritative {
                database.checkpoint().unwrap();
                drop(database);
                database = Database::open_with_config(&fixture.root, config.clone()).unwrap();
                database
                    .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child)])
                    .unwrap();
            }
            database
                .query_sql("UPDATE atomic_tail SET payload = 'child' WHERE id = 7")
                .unwrap();
            database.checkpoint().unwrap();
            let descendant = fork(&mut database, child, "mode-descendant");
            database
                .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(main)])
                .unwrap();
            assert_eq!(
                database
                    .query_sql("SELECT payload FROM atomic_tail WHERE payload = 'whole'")
                    .unwrap()
                    .rows[0]["payload"],
                Value::String("whole".into())
            );
            database.delete_branch(BranchSelector::Id(child)).unwrap();
            let catalog_path = fixture.root.join("branches/catalog.hawdb");
            let catalog = hawdb_storage::branch_catalog::read_catalog(&catalog_path).unwrap();
            let paths: Vec<_> = catalog
                .branches
                .iter()
                .map(|record| {
                    let directory = fixture
                        .root
                        .join("branches")
                        .join(record.id.as_uuid().to_string());
                    hawdb_storage::branch_catalog::BranchReclamationPath {
                        id: record.id,
                        head_path: directory.join("branch.head"),
                        directory,
                    }
                })
                .collect();
            let mut objects = hawdb_storage::immutable_object::ImmutableObjectStore::open(
                catalog_path.parent().unwrap().join("objects"),
            )
            .unwrap();
            hawdb_storage::branch_catalog::reclaim_catalog_branches(
                &catalog_path,
                &mut objects,
                &[],
                &paths,
            )
            .unwrap();
            let snapshot = fixture.model.capture().unwrap();
            drop(database);
            let root = fixture.image(&snapshot, &CrashPlan::default());
            let mut recovered = Database::open_with_config(&root, config).unwrap();
            recovered
                .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(descendant)])
                .unwrap();
            assert_eq!(
                recovered
                    .query_sql("SELECT payload, tag FROM atomic_tail WHERE payload = 'child'")
                    .unwrap()
                    .rows
                    .len(),
                1
            );
            assert_eq!(
                recovered
                    .query_sql("SELECT payload FROM atomic_tail WHERE id = 7")
                    .unwrap()
                    .rows[0]["payload"],
                Value::String("child".into())
            );
            assert_eq!(
                recovered
                    .query("MATCH (m:Memory {id: 'atomic-tail'}) RETURN m.id")
                    .unwrap()
                    .rows
                    .len(),
                1
            );
            let metrics = recovered.file_descriptor_metrics().unwrap();
            assert!(metrics.high_water <= 32);
            assert_eq!(metrics.reserved, 0);
            eprintln!(
                "branch-power-mode-v1 residency={mode:?} indexes={indexes:?} high_water={}",
                metrics.high_water
            );
        }
    }
}

#[test]
fn logical_deletion_publication_cuts_are_atomic_and_retry_the_same_uuid() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        for phase in 0..2 {
            for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
                let mut fixture = Fixture::new();
                let mut database = open(&fixture.root, durability);
                commit_mixed(&mut database);
                let main = database
                    .describe_branch(BranchSelector::Name("main".into()))
                    .unwrap()
                    .id;
                let child = fork(&mut database, main, "deleting-parent");
                database
                    .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(child)])
                    .unwrap();
                let nested = fork(&mut database, child, "delete-survivor");
                database
                    .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(main)])
                    .unwrap();
                let revision = database
                    .describe_branch(BranchSelector::Id(child))
                    .unwrap()
                    .metadata_revision;
                let request = [Value::Uuid(child), Value::Int(revision as i64)];
                fixture
                    .model
                    .observe(ObservationPoint {
                        event: IoEvent::Rename,
                        relative_path: PathBuf::from("branches/catalog.hawdb"),
                        boundary,
                        skip_matches: phase,
                        include_descendants: false,
                        keep_last: false,
                    })
                    .unwrap();
                database
                    .query_sql_with_params("DROP BRANCH ID $1 AT REVISION $2", &request)
                    .unwrap();
                let acknowledged = fixture.model.capture().unwrap();
                let cut = fixture
                    .model
                    .take_observation()
                    .unwrap()
                    .expect("actual delete catalog publication");
                drop(database);
                let plans = publication_fault_plans(&cut)
                    .into_iter()
                    .map(|plan| (&cut, plan))
                    .chain(std::iter::once((&acknowledged, CrashPlan::default())));
                for (snapshot, plan) in plans {
                    let root = fixture.image(snapshot, &plan);
                    let mut recovered = open(&root, durability);
                    recovered
                        .query_sql_with_params("DROP BRANCH ID $1 AT REVISION $2", &request)
                        .unwrap();
                    let first = std::fs::read(root.join("branches/catalog.hawdb")).unwrap();
                    recovered
                        .query_sql_with_params("DROP BRANCH ID $1 AT REVISION $2", &request)
                        .unwrap();
                    assert_eq!(
                        std::fs::read(root.join("branches/catalog.hawdb")).unwrap(),
                        first
                    );
                    let tombstone = recovered
                        .describe_branch(BranchSelector::Id(child))
                        .unwrap();
                    assert_eq!(tombstone.id, child);
                    assert_eq!(tombstone.state, crate::BranchLifecycleState::Deleted);
                    assert!(assert_atomic(&mut recovered, main));
                    recovered
                        .query_sql_with_params("USE BRANCH ID $1", &[Value::Uuid(nested)])
                        .unwrap();
                    assert_eq!(
                        recovered
                            .query_sql("SELECT payload FROM atomic_tail WHERE id = 7")
                            .unwrap()
                            .rows[0]["payload"],
                        Value::String("whole".into())
                    );
                    assert_eq!(
                        recovered
                            .query("MATCH (m:Memory {id: 'atomic-tail'}) RETURN m.id")
                            .unwrap()
                            .rows
                            .len(),
                        1
                    );
                }
            }
        }
    }
}

#[test]
fn initial_project_installation_preserves_ancestors_after_acknowledgement() {
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let mut fixture = Fixture::new();
        let relative = PathBuf::from("new-parent/project");
        fixture
            .model
            .observe(ObservationPoint {
                event: IoEvent::CreateDirectory,
                relative_path: relative.clone(),
                boundary,
                skip_matches: 0,
                include_descendants: false,
                keep_last: false,
            })
            .unwrap();
        let mut database = open(
            &fixture.root.join(&relative),
            DurabilityPolicy::SyncOnEveryWrite,
        );
        commit_mixed(&mut database);
        let main = database
            .describe_branch(BranchSelector::Name("main".into()))
            .unwrap()
            .id;
        let epoch = database.commit_epoch().unwrap();
        let acknowledged = fixture.model.capture().unwrap();
        let cut = fixture
            .model
            .take_observation()
            .unwrap()
            .expect("actual initial project directory creation");
        drop(database);
        for plan in [CrashPlan::default(), cut.persist_all_plan()] {
            let anchor = fixture.image(&cut, &plan);
            let root = anchor.join(&relative);
            let mut interrupted = open(&root, DurabilityPolicy::SyncOnEveryWrite);
            // This cut precedes every schema/data transaction and identity
            // reservation. Reopening can initialize the uncommitted project.
            assert!(interrupted.query_sql("SELECT column_name FROM information_schema.columns WHERE table_name = 'atomic_tail'").unwrap().rows.is_empty());
            assert!(interrupted
                .query("MATCH (m:Memory {id: 'atomic-tail'}) RETURN m.id")
                .unwrap()
                .rows
                .is_empty());
        }
        let anchor = fixture.image(&acknowledged, &CrashPlan::default());
        let mut recovered = open(&anchor.join(&relative), DurabilityPolicy::SyncOnEveryWrite);
        assert_eq!(recovered.commit_epoch().unwrap(), epoch);
        assert!(assert_atomic(&mut recovered, main));
    }
}
