use super::*;
use crate::store::{
    canonical_adjacency_artifact_generation_file, canonical_artifact_generation_file,
    property_projection_artifact_generation_file, set_checkpoint_failpoint, CheckpointPublishStage,
};
use crate::{Database, Value};
use std::any::TypeId;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn root_facade_preserves_storage_repair_contract_type_identity() {
    assert_eq!(
        TypeId::of::<DerivedArtifactKind>(),
        TypeId::of::<skein_storage::DerivedArtifactKind>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactHealthState>(),
        TypeId::of::<skein_storage::DerivedArtifactHealthState>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactHealth>(),
        TypeId::of::<skein_storage::DerivedArtifactHealth>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactHealthReport>(),
        TypeId::of::<skein_storage::DerivedArtifactHealthReport>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactRepairPlan>(),
        TypeId::of::<skein_storage::DerivedArtifactRepairPlan>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactRebuildOptions>(),
        TypeId::of::<skein_storage::DerivedArtifactRebuildOptions>()
    );
    assert_eq!(
        TypeId::of::<DerivedArtifactRepairReport>(),
        TypeId::of::<skein_storage::DerivedArtifactRepairReport>()
    );
    assert_eq!(
        DerivedArtifactRebuildOptions::default(),
        skein_storage::DerivedArtifactRebuildOptions::default()
    );
}

#[test]
fn rebuilds_corrupt_adjacency_from_verified_canonical_source() {
    let path = checkpointed_database("adjacency");
    let manifest = DurableManifest::load(&path.join(MANIFEST_FILE)).unwrap();
    corrupt_file(&path.join(canonical_adjacency_artifact_generation_file(
        manifest.checkpoint_epoch,
    )));

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
    let manifest = DurableManifest::load(&path.join(MANIFEST_FILE)).unwrap();
    corrupt_file(&path.join(property_projection_artifact_generation_file(
        manifest.checkpoint_epoch,
    )));

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
    let manifest = DurableManifest::load(&path.join(MANIFEST_FILE)).unwrap();
    corrupt_file(&path.join(canonical_artifact_generation_file(
        manifest.checkpoint_epoch,
    )));

    let error = DatabaseDoctor::plan_derived_artifact_rebuild(
        &path,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap_err();
    assert!(matches!(error, SkeinError::StorageIntegrity(_)));
    assert!(pending_record_paths(&path).unwrap().is_empty());
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn source_budget_rejects_before_repair_audit_or_publication() {
    let path = checkpointed_database("budget");
    let manifest = DurableManifest::load(&path.join(MANIFEST_FILE)).unwrap();
    corrupt_file(&path.join(canonical_adjacency_artifact_generation_file(
        manifest.checkpoint_epoch,
    )));
    let options = DerivedArtifactRebuildOptions {
        max_source_records: 1,
        ..DerivedArtifactRebuildOptions::default()
    };

    let error = DatabaseDoctor::plan_derived_artifact_rebuild(&path, options).unwrap_err();
    assert!(error.to_string().contains("source record admission"));
    assert!(pending_record_paths(&path).unwrap().is_empty());
    assert_eq!(
        DurableManifest::load(&path.join(MANIFEST_FILE))
            .unwrap()
            .checkpoint_epoch,
        manifest.checkpoint_epoch
    );
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn manifest_published_interruption_blocks_open_and_finalizes_idempotently() {
    let path = checkpointed_database("interrupted");
    let manifest = DurableManifest::load(&path.join(MANIFEST_FILE)).unwrap();
    corrupt_file(&path.join(canonical_adjacency_artifact_generation_file(
        manifest.checkpoint_epoch,
    )));
    let plan = DatabaseDoctor::plan_derived_artifact_rebuild(
        &path,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap();

    set_checkpoint_failpoint(Some(CheckpointPublishStage::ManifestPublished));
    let error = DatabaseDoctor::apply_derived_artifact_rebuild(&path, &plan).unwrap_err();
    set_checkpoint_failpoint(None);
    assert!(error.to_string().contains("injected checkpoint failure"));
    assert_eq!(
        DurableManifest::load(&path.join(MANIFEST_FILE))
            .unwrap()
            .checkpoint_epoch,
        plan.target_generation
    );
    assert!(Database::open(&path)
        .unwrap_err()
        .to_string()
        .contains("interrupted derived artifact repair"));

    let quarantined_manifest = quarantine_directory(&path, &plan).join(MANIFEST_FILE);
    let quarantined_manifest_bytes = fs::read(&quarantined_manifest).unwrap();
    corrupt_file(&quarantined_manifest);
    let error = DatabaseDoctor::apply_derived_artifact_rebuild(&path, &plan).unwrap_err();
    assert!(error.to_string().contains("quarantine identity changed"));
    fs::write(&quarantined_manifest, quarantined_manifest_bytes).unwrap();

    let resumed = DatabaseDoctor::plan_derived_artifact_rebuild(
        &path,
        DerivedArtifactRebuildOptions::default(),
    )
    .unwrap();
    assert_eq!(resumed, plan);
    let report = DatabaseDoctor::apply_derived_artifact_rebuild(&path, &resumed).unwrap();
    assert!(report.resumed_interrupted_repair);
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
        "skein-derived-repair-{name}-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}
