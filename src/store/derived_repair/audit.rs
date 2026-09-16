use super::{
    plan_identity, DerivedArtifactKind, DerivedArtifactRepairPlan, DerivedArtifactRepairReport,
    DERIVED_ARTIFACT_REPAIR_PROTOCOL,
};
use crate::error::{Result, SkeinError};
use crate::store::{
    canonical_adjacency_artifact_generation_file, file_checksum,
    property_projection_artifact_generation_file, property_projection_manifest_generation_file,
    sync_parent_dir, DurableManifest, MANIFEST_FILE,
};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const DOCTOR_DIRECTORY: &str = "doctor";
const DERIVED_QUARANTINE_DIRECTORY: &str = "derived-quarantine";
const MAX_REPAIR_AUDIT_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DerivedRepairAuditState {
    Prepared,
    Applied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct DerivedRepairAuditRecord {
    protocol: String,
    state: DerivedRepairAuditState,
    pub(super) plan: DerivedArtifactRepairPlan,
    quarantined_files: Vec<QuarantinedFileIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct QuarantinedFileIdentity {
    name: String,
    len: u64,
    crc32c: u64,
    sha256: String,
}

pub(super) fn prepare_repair(
    path: &Path,
    plan: &DerivedArtifactRepairPlan,
) -> Result<DerivedRepairAuditRecord> {
    super::validate_source_identity(path, plan)?;
    let quarantine = quarantine_directory(path, plan);
    fs::create_dir_all(&quarantine)?;
    sync_parent_dir(&quarantine)?;
    let mut files = vec![MANIFEST_FILE.to_string()];
    for target in &plan.targets {
        files.extend(target_files(*target, plan.source_generation));
    }
    files.sort();
    files.dedup();
    let mut quarantined_files = Vec::new();
    for name in files {
        let source = path.join(&name);
        if !source.exists() {
            continue;
        }
        let destination = quarantine.join(&name);
        if destination.exists() {
            let source_identity = file_checksum(&source)?;
            let destination_identity = file_checksum(&destination)?;
            if source_identity != destination_identity {
                return Err(SkeinError::Storage(format!(
                    "existing derived repair quarantine file has the wrong identity: {name}"
                )));
            }
        } else {
            super::super::copy_file_with_checksum(&source, &destination)?;
            sync_parent_dir(&destination)?;
        }
        let identity = file_checksum(&destination)?;
        quarantined_files.push(QuarantinedFileIdentity {
            name,
            len: identity.0,
            crc32c: identity.1,
            sha256: identity.2.to_string(),
        });
    }
    let record = DerivedRepairAuditRecord {
        protocol: DERIVED_ARTIFACT_REPAIR_PROTOCOL.to_string(),
        state: DerivedRepairAuditState::Prepared,
        plan: plan.clone(),
        quarantined_files,
    };
    validate_quarantine(path, &record)?;
    write_audit_record(&pending_record_path(path, plan), &record)?;
    Ok(record)
}

pub(super) fn finalize_repair(
    path: &Path,
    mut record: DerivedRepairAuditRecord,
    resumed_interrupted_repair: bool,
) -> Result<DerivedArtifactRepairReport> {
    validate_pending_record(path, &record)?;
    record.state = DerivedRepairAuditState::Applied;
    let applied = applied_record_path(path, &record.plan);
    write_audit_record(&applied, &record)?;
    let pending = pending_record_path(path, &record.plan);
    if pending.exists() {
        fs::remove_file(&pending)?;
        sync_parent_dir(&pending)?;
    }
    Ok(DerivedArtifactRepairReport {
        protocol: DERIVED_ARTIFACT_REPAIR_PROTOCOL.to_string(),
        plan_id: record.plan.plan_id.clone(),
        source_generation: record.plan.source_generation,
        published_generation: record.plan.target_generation,
        source_commit_epoch: record.plan.source_commit_epoch,
        targets: record.plan.targets.clone(),
        quarantined_files: record
            .quarantined_files
            .into_iter()
            .map(|file| file.name)
            .collect(),
        repair_record_file: file_name(&applied)?,
        resumed_interrupted_repair,
    })
}

pub(super) fn validate_pending_record(
    path: &Path,
    record: &DerivedRepairAuditRecord,
) -> Result<()> {
    super::validate_plan(&record.plan)?;
    validate_quarantine(path, record)?;
    let manifest = DurableManifest::load(&path.join(MANIFEST_FILE))?;
    if manifest.checkpoint_epoch != record.plan.source_generation
        && manifest.checkpoint_epoch != record.plan.target_generation
    {
        return Err(SkeinError::Storage(
            "pending derived repair does not match the published generation".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn pending_record_paths(path: &Path) -> Result<Vec<PathBuf>> {
    let doctor = doctor_directory(path);
    if !doctor.exists() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    for entry in fs::read_dir(doctor)? {
        let path = entry?.path();
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".derived-repair.pending.json"))
        {
            records.push(path);
        }
    }
    records.sort();
    Ok(records)
}

pub(super) fn load_single_pending_record(path: &Path) -> Result<Option<DerivedRepairAuditRecord>> {
    match pending_record_paths(path)?.as_slice() {
        [] => Ok(None),
        [record] => load_audit_record(record).map(Some),
        _ => Err(SkeinError::Storage(
            "database has multiple pending derived artifact repair records".to_string(),
        )),
    }
}

pub(super) fn load_matching_pending_record(
    path: &Path,
    plan_id: &str,
) -> Result<Option<DerivedRepairAuditRecord>> {
    let Some(record) = load_single_pending_record(path)? else {
        return Ok(None);
    };
    if record.plan.plan_id != plan_id {
        return Err(SkeinError::Storage(
            "database has a pending derived repair for a different plan".to_string(),
        ));
    }
    Ok(Some(record))
}

pub(super) fn quarantine_directory(path: &Path, plan: &DerivedArtifactRepairPlan) -> PathBuf {
    doctor_directory(path)
        .join(DERIVED_QUARANTINE_DIRECTORY)
        .join(&plan.plan_id)
}

fn validate_quarantine(path: &Path, record: &DerivedRepairAuditRecord) -> Result<()> {
    validate_quarantined_file_names(record)?;
    let quarantine = quarantine_directory(path, &record.plan);
    for expected in &record.quarantined_files {
        let actual = file_checksum(&quarantine.join(&expected.name))?;
        if actual.0 != expected.len
            || actual.1 != expected.crc32c
            || actual.2.to_string() != expected.sha256
        {
            return Err(SkeinError::Storage(format!(
                "derived repair quarantine identity changed: {}",
                expected.name
            )));
        }
        if expected.name == MANIFEST_FILE
            && (actual.0 != record.plan.manifest_len
                || actual.1 != record.plan.manifest_crc32c
                || actual.2.to_string() != record.plan.manifest_sha256)
        {
            return Err(SkeinError::Storage(
                "derived repair quarantine manifest does not match the planned source".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_quarantined_file_names(record: &DerivedRepairAuditRecord) -> Result<()> {
    let mut allowed = vec![MANIFEST_FILE.to_string()];
    for target in &record.plan.targets {
        allowed.extend(target_files(*target, record.plan.source_generation));
    }
    allowed.sort();
    allowed.dedup();

    let mut actual = record
        .quarantined_files
        .iter()
        .map(|file| file.name.clone())
        .collect::<Vec<_>>();
    let manifest_count = actual
        .iter()
        .filter(|name| name.as_str() == MANIFEST_FILE)
        .count();
    actual.sort();
    let unique_len = actual.len();
    actual.dedup();
    if manifest_count != 1
        || actual.len() != unique_len
        || actual.iter().any(|name| !allowed.contains(name))
    {
        return Err(SkeinError::Storage(
            "derived repair quarantine file set is invalid".to_string(),
        ));
    }
    Ok(())
}

fn target_files(kind: DerivedArtifactKind, generation: u64) -> Vec<String> {
    match kind {
        DerivedArtifactKind::CanonicalAdjacency => vec![
            canonical_adjacency_artifact_generation_file(generation),
            skein_storage::canonical_adjacency_descriptor_page_file(generation),
            skein_storage::canonical_adjacency_descriptor_root_file(generation),
        ],
        DerivedArtifactKind::PersistentPropertyProjection => vec![
            property_projection_artifact_generation_file(generation),
            property_projection_manifest_generation_file(generation),
            skein_storage::property_projection_descriptor_page_file(generation),
            skein_storage::property_projection_descriptor_root_file(generation),
        ],
    }
}

fn write_audit_record(path: &Path, record: &DerivedRepairAuditRecord) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        SkeinError::Storage("derived repair audit path has no parent".to_string())
    })?;
    fs::create_dir_all(parent)?;
    sync_parent_dir(parent)?;
    let encoded = serde_json::to_vec_pretty(record).map_err(|error| {
        SkeinError::Storage(format!("failed to encode derived repair audit: {error}"))
    })?;
    let temporary = path.with_extension("json.tmp");
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
    }
    skein_storage::durable_replace_file(&temporary, path)
        .map_err(|error| SkeinError::Storage(error.to_string()))
}

fn load_audit_record(path: &Path) -> Result<DerivedRepairAuditRecord> {
    if fs::metadata(path)?.len() > MAX_REPAIR_AUDIT_BYTES {
        return Err(SkeinError::Storage(format!(
            "derived repair audit exceeds the {MAX_REPAIR_AUDIT_BYTES} byte limit"
        )));
    }
    let record =
        serde_json::from_slice::<DerivedRepairAuditRecord>(&fs::read(path)?).map_err(|error| {
            SkeinError::Storage(format!("invalid derived repair audit record: {error}"))
        })?;
    if record.protocol != DERIVED_ARTIFACT_REPAIR_PROTOCOL
        || record.plan.protocol != DERIVED_ARTIFACT_REPAIR_PROTOCOL
        || record.plan.plan_id != plan_identity(&record.plan)
    {
        return Err(SkeinError::Storage(
            "derived repair audit identity is invalid".to_string(),
        ));
    }
    validate_quarantined_file_names(&record)?;
    Ok(record)
}

fn doctor_directory(path: &Path) -> PathBuf {
    path.join(DOCTOR_DIRECTORY)
}

fn pending_record_path(path: &Path, plan: &DerivedArtifactRepairPlan) -> PathBuf {
    doctor_directory(path).join(format!("{}.derived-repair.pending.json", plan.plan_id))
}

fn applied_record_path(path: &Path, plan: &DerivedArtifactRepairPlan) -> PathBuf {
    doctor_directory(path).join(format!("{}.derived-repair.applied.json", plan.plan_id))
}

fn file_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string)
        .ok_or_else(|| SkeinError::Storage("derived repair path has no file name".to_string()))
}
