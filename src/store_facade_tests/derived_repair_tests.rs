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

use crate::{Database, Value};
use hawdb_core::error::HawDBError;
use hawdb_storage::artifact_files::*;
use hawdb_storage::derived_repair::*;
use hawdb_storage::durable_manifest::*;
use hawdb_storage::store::derived_repair::audit::*;
use hawdb_storage::store::doctor::*;
#[cfg(feature = "test-support")]
use hawdb_storage::store::{set_checkpoint_failpoint, CheckpointPublishStage};
use std::any::TypeId;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn root_facade_preserves_storage_repair_contract_type_identity() {
    assert_eq!(
        TypeId::of::<crate::DerivedArtifactBranchSource>(),
        TypeId::of::<hawdb_storage::derived_repair::DerivedArtifactBranchSource>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactKind>(),
        TypeId::of::<hawdb_storage::derived_repair::DerivedArtifactKind>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactHealthState>(),
        TypeId::of::<hawdb_storage::derived_repair::DerivedArtifactHealthState>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactHealth>(),
        TypeId::of::<hawdb_storage::derived_repair::DerivedArtifactHealth>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactHealthReport>(),
        TypeId::of::<hawdb_storage::derived_repair::DerivedArtifactHealthReport>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactRepairPlan>(),
        TypeId::of::<hawdb_storage::derived_repair::DerivedArtifactRepairPlan>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactRebuildOptions>(),
        TypeId::of::<hawdb_storage::derived_repair::DerivedArtifactRebuildOptions>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactRepairReport>(),
        TypeId::of::<hawdb_storage::derived_repair::DerivedArtifactRepairReport>()
    );
    assert_eq!(
        DerivedArtifactRebuildOptions::default(),
        hawdb_storage::derived_repair::DerivedArtifactRebuildOptions::default()
    );
}

#[test]
fn branch_repair_recovers_mixed_schema_and_data_wal_under_the_host_descriptor_limit() {
    let path = checkpointed_database("private_wal_plan");
    let metadata = hawdb_storage::branch_project::ProjectMetadata::open(&path, 32).unwrap();
    let mut database = Database::open_with_config(
        &path,
        crate::DatabaseConfig {
            max_open_files: 32,
            ..Default::default()
        },
    )
    .unwrap();
    let mut transaction = database.begin_transaction().unwrap();
    transaction
        .query_sql("CREATE TABLE repair_tail (id BIGINT PRIMARY KEY, payload TEXT)")
        .unwrap();
    transaction
        .query_sql("INSERT INTO repair_tail (id, payload) VALUES (9, 'retained')")
        .unwrap();
    transaction
        .query("CREATE (:Memory {id: 3, score: 11})")
        .unwrap();
    transaction.commit().unwrap();
    let expected_epoch = database.commit_epoch().unwrap();
    drop(database);
    let head_path = branch_directory(&path).join("branch.head");
    let before = fs::read(&head_path).unwrap();
    let manifest = load_active_manifest(&path);
    corrupt_checkpoint_artifact(
        &path,
        &canonical_adjacency_artifact_generation_file(manifest.checkpoint_epoch),
    );
    let plan = DatabaseDoctor::plan_derived_artifact_rebuild(
        &path,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap();
    let source = plan
        .branch
        .as_ref()
        .expect("plan must bind the branch head");
    assert_eq!(source.branch_id, metadata.main().id.as_uuid());
    assert_eq!(
        source.project_id,
        metadata.selector().project_id().as_uuid()
    );
    assert_eq!(plan.source_commit_epoch, expected_epoch);
    assert_eq!(plan.source_node_count, 3);
    assert_eq!(plan.source_relationship_count, 1);
    assert!(plan.target_generation > plan.source_generation + 1);
    assert_eq!(fs::read(&head_path).unwrap(), before);
    assert!(pending_record_paths(&path).unwrap().is_empty());
    DatabaseDoctor::apply_derived_artifact_rebuild(&path, &plan).unwrap();
    let mut reopened = Database::open_with_config(
        &path,
        crate::DatabaseConfig {
            max_open_files: 32,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        reopened
            .query_sql("SELECT payload FROM repair_tail WHERE id = 9")
            .unwrap()
            .rows[0]["payload"],
        Value::String("retained".into())
    );
    assert_eq!(
        reopened
            .query("MATCH (m:Memory {id: 3}) RETURN m.score AS score")
            .unwrap()
            .rows[0]["score"],
        Value::Int(11)
    );
    drop(reopened);
    let metrics = metadata.file_descriptors().metrics();
    assert_eq!(metrics.limit, 32);
    assert_eq!(metrics.admitted_runtimes, 0);
    assert!(metrics.high_water <= 32);
    drop(metadata);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn branch_repair_plan_rejects_an_incomplete_private_transaction_without_audit() {
    let path = checkpointed_database("invalid_private_wal_plan");
    let directory = branch_directory(&path);
    let head =
        hawdb_storage::branch_head::read_branch_head(&directory.join("branch.head")).unwrap();
    let wal_path = directory.join(wal_generation_file(head.active_wal.generation));
    let mut wal = fs::read(&wal_path).unwrap();
    wal.push(0);
    fs::write(&wal_path, &wal).unwrap();
    let head_bytes = fs::read(directory.join("branch.head")).unwrap();
    let error = DatabaseDoctor::plan_derived_artifact_rebuild(
        &path,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("WAL"), "{error}");
    assert_eq!(fs::read(&wal_path).unwrap(), wal);
    assert_eq!(fs::read(directory.join("branch.head")).unwrap(), head_bytes);
    assert!(pending_record_paths(&path).unwrap().is_empty());
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn branch_repair_keeps_a_live_sibling_snapshot_and_its_immutable_history() {
    let path = checkpointed_database("sibling_snapshot");
    let mut database = Database::open(&path).unwrap();
    let epoch = database.commit_epoch().unwrap();
    database
        .query_sql_with_params(
            "CREATE BRANCH child FROM NAME 'main' AT REVISION $1 REQUEST KEY 'repair-child'",
            &[Value::Int(epoch as i64)],
        )
        .unwrap();
    database.query_sql("USE BRANCH child").unwrap();
    let snapshot = database.read_snapshot().unwrap();
    drop(database);
    let metadata = hawdb_storage::branch_project::ProjectMetadata::open(
        &path,
        crate::DatabaseConfig::default().max_open_files,
    )
    .unwrap();
    let child = metadata
        .catalog()
        .branches
        .iter()
        .find(|record| record.name.as_str() == "child")
        .unwrap();
    let child_directory = path.join("branches").join(child.id.as_uuid().to_string());
    let child_head_path = child_directory.join("branch.head");
    let child_head = fs::read(&child_head_path).unwrap();
    let manifest = load_active_manifest(&path);
    let artifact = canonical_adjacency_artifact_generation_file(manifest.checkpoint_epoch);
    let (objects, root) = checkpoint_root(&path);
    let reference = root
        .checkpoint_bindings
        .iter()
        .find(|binding| binding.relative_path == artifact)
        .unwrap()
        .reference;
    corrupt_checkpoint_artifact(&path, &artifact);
    let damaged = fs::read(objects.object_path(reference)).unwrap();
    let plan = DatabaseDoctor::plan_derived_artifact_rebuild(
        &path,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap();
    DatabaseDoctor::apply_derived_artifact_rebuild(&path, &plan).unwrap();
    let (_, rebuilt) = checkpoint_root(&path);
    let rebuilt_reference = rebuilt
        .checkpoint_bindings
        .iter()
        .find(|binding| {
            binding.relative_path
                == canonical_adjacency_artifact_generation_file(plan.target_generation)
        })
        .unwrap()
        .reference;
    assert_ne!(rebuilt_reference, reference);
    assert_eq!(fs::read(objects.object_path(reference)).unwrap(), damaged);
    assert_eq!(fs::read(&child_head_path).unwrap(), child_head);
    let evidence = quarantine_directory(&path, &plan).join(&artifact);
    assert_eq!(fs::read(evidence).unwrap(), damaged);
    let busy = DatabaseDoctor::plan_derived_artifact_rebuild(
        &child_directory,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap_err();
    assert!(busy.to_string().contains("already open"), "{busy}");
    assert!(pending_record_paths(&child_directory).unwrap().is_empty());
    drop(snapshot);
    let mut reopened = Database::open(&path).unwrap();
    assert!(reopened.query_sql("USE BRANCH child").is_err());
    assert_eq!(
        reopened
            .query("MATCH (:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS name")
            .unwrap()
            .rows[0]["name"],
        Value::String("Rust".into())
    );
    drop(reopened);
    let main_head = fs::read(branch_directory(&path).join("branch.head")).unwrap();
    let child_plan = DatabaseDoctor::plan_derived_artifact_rebuild(
        &child_directory,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap();
    assert_eq!(
        child_plan.branch.as_ref().unwrap().branch_id,
        child.id.as_uuid()
    );
    DatabaseDoctor::apply_derived_artifact_rebuild(&child_directory, &child_plan).unwrap();
    assert_eq!(
        fs::read(branch_directory(&path).join("branch.head")).unwrap(),
        main_head
    );
    let mut reopened = Database::open(&path).unwrap();
    reopened.query_sql("USE BRANCH child").unwrap();
    assert_eq!(
        reopened
            .query("MATCH (:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS name")
            .unwrap()
            .rows[0]["name"],
        Value::String("Rust".into())
    );
    drop(reopened);
    assert_eq!(metadata.file_descriptors().metrics().admitted_runtimes, 0);
    drop(metadata);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn branch_repair_does_not_open_closed_siblings_under_the_project_descriptor_limit() {
    let path = checkpointed_database("repair_lease_budget");
    let metadata = hawdb_storage::branch_project::ProjectMetadata::open(&path, 32).unwrap();
    let mut database = Database::open_with_config(
        &path,
        crate::DatabaseConfig {
            max_open_files: 32,
            ..Default::default()
        },
    )
    .unwrap();
    let epoch = database.commit_epoch().unwrap();
    for index in 0..32 {
        database
            .query_sql_with_params(
                "CREATE BRANCH NAME $1 FROM NAME 'main' AT REVISION $2 REQUEST KEY $3",
                &[
                    Value::String(format!("child-{index}")),
                    Value::Int(epoch as i64),
                    Value::String(format!("repair-budget-{index}")),
                ],
            )
            .unwrap();
    }
    drop(database);
    let manifest = load_active_manifest(&path);
    corrupt_checkpoint_artifact(
        &path,
        &canonical_adjacency_artifact_generation_file(manifest.checkpoint_epoch),
    );
    let plan = DatabaseDoctor::plan_derived_artifact_rebuild(
        &path,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap();
    let head_path = branch_directory(&path).join("branch.head");
    let before = fs::read(&head_path).unwrap();
    DatabaseDoctor::apply_derived_artifact_rebuild(&path, &plan).unwrap();
    assert!(pending_record_paths(&path).unwrap().is_empty());
    assert_ne!(fs::read(head_path).unwrap(), before);
    let metrics = metadata.file_descriptors().metrics();
    assert_eq!(metrics.limit, 32);
    assert_eq!(metrics.admitted_runtimes, 0);
    assert!(metrics.high_water <= 32);
    drop(metadata);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn branch_repair_rejects_a_plan_after_the_private_wal_changes_without_audit() {
    let path = checkpointed_database("stale_private_wal");
    let directory = branch_directory(&path);
    let head =
        hawdb_storage::branch_head::read_branch_head(&directory.join("branch.head")).unwrap();
    let wal_path = directory.join(wal_generation_file(head.active_wal.generation));
    let manifest = load_active_manifest(&path);
    corrupt_checkpoint_artifact(
        &path,
        &canonical_adjacency_artifact_generation_file(manifest.checkpoint_epoch),
    );
    let plan = DatabaseDoctor::plan_derived_artifact_rebuild(
        &path,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap();
    let mut changed = fs::read(&wal_path).unwrap();
    changed.push(0);
    fs::write(&wal_path, &changed).unwrap();
    let before = fs::read(directory.join("branch.head")).unwrap();
    assert!(DatabaseDoctor::apply_derived_artifact_rebuild(&path, &plan).is_err());
    assert!(pending_record_paths(&path).unwrap().is_empty());
    assert_eq!(fs::read(wal_path).unwrap(), changed);
    assert_eq!(fs::read(directory.join("branch.head")).unwrap(), before);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn rebuilds_corrupt_adjacency_from_verified_canonical_source() {
    let path = checkpointed_database("adjacency");
    let manifest = load_active_manifest(&path);
    corrupt_checkpoint_artifact(
        &path,
        &canonical_adjacency_artifact_generation_file(manifest.checkpoint_epoch),
    );

    let health =
        DatabaseDoctor::derived_artifact_health(&path, DerivedArtifactRebuildOptions::default())
            .unwrap();
    assert!(health.repair_required);
    assert_eq!(
        health
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == DerivedArtifactKind::CanonicalAdjacency)
            .unwrap()
            .state,
        DerivedArtifactHealthState::RepairRequired
    );

    let plan = DatabaseDoctor::plan_derived_artifact_rebuild(
        &path,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap();
    assert_eq!(plan.targets, vec![DerivedArtifactKind::CanonicalAdjacency]);
    let report = DatabaseDoctor::apply_derived_artifact_rebuild(&path, &plan).unwrap();
    assert_eq!(report.published_generation, plan.target_generation);
    assert!(!report.resumed_interrupted_repair);
    assert!(report
        .quarantined_files
        .iter()
        .any(|file| file.starts_with("adjacency.")));

    let mut db = Database::open(&path).unwrap();
    let output = db
        .query("MATCH (:Memory {id: 1})-[:MENTIONS]->(e:Entity) RETURN e.name AS name")
        .unwrap();
    assert_eq!(
        output.rows[0].get("name"),
        Some(&Value::String("Rust".to_string()))
    );
    drop(db);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn rebuilds_corrupt_property_projection_with_bounded_options() {
    let path = checkpointed_database("property_projection");
    let manifest = load_active_manifest(&path);
    corrupt_checkpoint_artifact(
        &path,
        &property_projection_artifact_generation_file(manifest.checkpoint_epoch),
    );

    let options = DerivedArtifactRebuildOptions {
        build_memory_bytes: 1024 * 1024,
        max_temporary_bytes: 256 * 1024 * 1024,
        ..DerivedArtifactRebuildOptions::default()
    };
    let plan = DatabaseDoctor::plan_derived_artifact_rebuild(&path, options).unwrap();
    assert_eq!(
        plan.targets,
        vec![DerivedArtifactKind::PersistentPropertyProjection]
    );
    DatabaseDoctor::apply_derived_artifact_rebuild(&path, &plan).unwrap();

    let health = DatabaseDoctor::derived_artifact_health(&path, options).unwrap();
    assert!(!health.repair_required);
    assert!(health
        .artifacts
        .iter()
        .all(|artifact| artifact.state == DerivedArtifactHealthState::Healthy));
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn canonical_corruption_fails_closed_before_repair_is_planned() {
    let path = checkpointed_database("canonical_corruption");
    let manifest = load_active_manifest(&path);
    corrupt_checkpoint_artifact(
        &path,
        &canonical_artifact_generation_file(manifest.checkpoint_epoch),
    );

    let error = DatabaseDoctor::plan_derived_artifact_rebuild(
        &path,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap_err();
    assert!(matches!(error, HawDBError::StorageIntegrity(_)));
    assert!(pending_record_paths(&path).unwrap().is_empty());
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn source_budget_rejects_before_repair_audit_or_publication() {
    let path = checkpointed_database("budget");
    let manifest = load_active_manifest(&path);
    corrupt_checkpoint_artifact(
        &path,
        &canonical_adjacency_artifact_generation_file(manifest.checkpoint_epoch),
    );
    let options = DerivedArtifactRebuildOptions {
        max_source_records: 1,
        ..DerivedArtifactRebuildOptions::default()
    };

    let error = DatabaseDoctor::plan_derived_artifact_rebuild(&path, options).unwrap_err();
    assert!(error.to_string().contains("source record admission"));
    assert!(pending_record_paths(&path).unwrap().is_empty());
    assert_eq!(
        load_active_manifest(&path).checkpoint_epoch,
        manifest.checkpoint_epoch
    );
    fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "test-support")]
#[test]
fn local_manifest_interruption_keeps_the_original_head_and_resumes_rebuild() {
    interrupted_branch_repair(CheckpointPublishStage::ManifestPublished, false);
}

#[cfg(feature = "test-support")]
#[test]
fn head_published_interruption_blocks_data_admission_and_finalizes_idempotently() {
    interrupted_branch_repair(CheckpointPublishStage::BranchHeadPublished, true);
}

#[cfg(feature = "test-support")]
fn interrupted_branch_repair(stage: CheckpointPublishStage, head_published: bool) {
    let path = checkpointed_database("interrupted");
    let manifest = load_active_manifest(&path);
    corrupt_checkpoint_artifact(
        &path,
        &canonical_adjacency_artifact_generation_file(manifest.checkpoint_epoch),
    );
    let plan = DatabaseDoctor::plan_derived_artifact_rebuild(
        &path,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap();

    let head_path = branch_directory(&path).join("branch.head");
    let source_head = fs::read(&head_path).unwrap();
    set_checkpoint_failpoint(Some(stage));
    let error = DatabaseDoctor::apply_derived_artifact_rebuild(&path, &plan).unwrap_err();
    set_checkpoint_failpoint(None);
    assert!(error.to_string().contains("injected checkpoint failure"));
    assert_eq!(
        load_active_manifest(&path).checkpoint_epoch,
        if head_published {
            plan.target_generation
        } else {
            plan.source_generation
        }
    );
    if !head_published {
        assert_eq!(fs::read(&head_path).unwrap(), source_head);
    }
    let mut metadata = Database::open(&path).unwrap();
    assert_eq!(
        metadata
            .file_descriptor_metrics()
            .unwrap()
            .admitted_runtimes,
        0
    );
    assert_eq!(
        metadata
            .query_sql("SHOW BRANCHES LIMIT 10")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(metadata
        .commit_epoch()
        .unwrap_err()
        .to_string()
        .contains("interrupted derived artifact repair"));
    assert_eq!(
        metadata
            .file_descriptor_metrics()
            .unwrap()
            .admitted_runtimes,
        0
    );
    drop(metadata);

    let quarantined_manifest =
        quarantine_directory(&path, &plan).join(hawdb_storage::store::MANIFEST_FILE);
    let quarantined_manifest_bytes = fs::read(&quarantined_manifest).unwrap();
    corrupt_file(&quarantined_manifest);
    let error = DatabaseDoctor::apply_derived_artifact_rebuild(&path, &plan).unwrap_err();
    assert!(error.to_string().contains("quarantine identity changed"));
    fs::write(&quarantined_manifest, quarantined_manifest_bytes).unwrap();

    if head_published {
        let pending = pending_record_paths(&path).unwrap();
        assert_eq!(pending.len(), 1);
        let original = fs::read(&pending[0]).unwrap();
        let mut audit: serde_json::Value = serde_json::from_slice(&original).unwrap();
        audit["target_head"]["sha256"] = serde_json::json!("0".repeat(64));
        fs::write(&pending[0], serde_json::to_vec(&audit).unwrap()).unwrap();
        let error = DatabaseDoctor::apply_derived_artifact_rebuild(&path, &plan).unwrap_err();
        assert!(
            error.to_string().contains("target head identity"),
            "{error}"
        );
        assert!(Database::open(&path).unwrap().commit_epoch().is_err());
        fs::write(&pending[0], original).unwrap();
    }

    let resumed = DatabaseDoctor::plan_derived_artifact_rebuild(
        &path,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap();
    assert_eq!(resumed, plan);
    let before_retry = fs::read(&head_path).unwrap();
    let report = DatabaseDoctor::apply_derived_artifact_rebuild(&path, &resumed).unwrap();
    assert!(report.resumed_interrupted_repair);
    if head_published {
        assert_eq!(fs::read(&head_path).unwrap(), before_retry);
    }
    Database::open(&path).unwrap();
    fs::remove_dir_all(path).unwrap();
}

fn checkpointed_database(name: &str) -> PathBuf {
    let path = unique_test_dir(name);
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE RANGE INDEX ON :Memory(score)").unwrap();
        db.query("CREATE (:Memory {id: 1, score: 7})-[:MENTIONS]->(:Entity {id: 2, name: 'Rust'})")
            .unwrap();
        db.checkpoint().unwrap();
    }
    path
}

fn branch_directory(path: &Path) -> PathBuf {
    let hawdb_storage::branch_project::ProjectManifest::Branch(selector) =
        hawdb_storage::branch_project::inspect_project_manifest(path).unwrap()
    else {
        panic!("repair fixture must publish a branch project");
    };
    path.join("branches")
        .join(selector.main_branch_id().as_uuid().to_string())
}

fn checkpoint_root(
    path: &Path,
) -> (
    hawdb_storage::immutable_object::ImmutableObjectStore,
    hawdb_storage::sealed_root::SealedRoot,
) {
    let head =
        hawdb_storage::branch_head::read_branch_head(&branch_directory(path).join("branch.head"))
            .unwrap();
    let objects = hawdb_storage::immutable_object::ImmutableObjectStore::open(
        path.join("branches").join("objects"),
    )
    .unwrap();
    let root =
        hawdb_storage::sealed_root::SealedRoot::decode(&objects.read(head.sealed_root).unwrap())
            .unwrap();
    (objects, root)
}

fn load_active_manifest(path: &Path) -> DurableManifest {
    let (objects, root) = checkpoint_root(path);
    objects.read(root.durable_manifest).unwrap();
    DurableManifest::load(&objects.object_path(root.durable_manifest)).unwrap()
}

fn corrupt_checkpoint_artifact(path: &Path, name: &str) {
    let (objects, root) = checkpoint_root(path);
    let binding = root
        .checkpoint_bindings
        .iter()
        .find(|binding| binding.relative_path == name)
        .expect("corruption fixture artifact must belong to the authoritative checkpoint closure");
    corrupt_file(&objects.object_path(binding.reference));
}

fn corrupt_file(path: &Path) {
    let mut bytes = fs::read(path).unwrap();
    let offset = bytes.len().saturating_sub(1).max(24);
    bytes[offset] ^= 0xff;
    fs::write(path, bytes).unwrap();
}

fn unique_test_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "hawdb-derived-repair-{name}-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}
