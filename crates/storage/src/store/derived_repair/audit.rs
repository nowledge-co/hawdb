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

use super::{
    plan_identity, DerivedArtifactKind, DerivedArtifactRepairPlan, DerivedArtifactRepairReport,
    DERIVED_ARTIFACT_REPAIR_PROTOCOL,
};
use crate::error::{HawDBError, Result};
use crate::file_io::{self as fs, OpenOptions};
use crate::store::{
    canonical_adjacency_artifact_generation_file, file_checksum,
    property_projection_artifact_generation_file, property_projection_manifest_generation_file,
    sync_parent_dir, DurableManifest, MANIFEST_FILE,
};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};

const DOCTOR_DIRECTORY: &str = "doctor";
const DERIVED_QUARANTINE_DIRECTORY: &str = "derived-quarantine";
const MAX_REPAIR_AUDIT_BYTES: u64 = 1024 * 1024;
const BRANCH_ROOT_FILE: &str = "branch-root.hawdb";
const BRANCH_PRIVATE_WAL_FILE: &str = "branch-private-wal.hawdb";
const BRANCH_HEAD_FILE: &str = "branch.head";

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
    target_head: Option<RepairTargetHead>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RepairTargetHead {
    byte_length: u64,
    sha256: String,
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
    let files = repair_source_files(path, plan)?;
    // Admission precedes creation of persistent audit/quarantine artifacts.
    let mut total_bytes = 0_u64;
    for (_, source) in &files {
        match fs::metadata(source) {
            Ok(metadata) => {
                total_bytes = total_bytes.checked_add(metadata.len()).ok_or_else(|| {
                    HawDBError::Storage("derived repair quarantine size overflow".into())
                })?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if total_bytes > plan.options.max_temporary_bytes {
        return Err(HawDBError::Storage(
            "derived repair quarantine exceeds the temporary byte limit".into(),
        ));
    }
    let quarantine = quarantine_directory(path, plan);
    fs::create_dir_all(&quarantine)?;
    sync_parent_dir(&quarantine)?;
    let mut quarantined_files = Vec::new();
    for (name, source) in files {
        if !source.exists() {
            continue;
        }
        let destination = quarantine.join(&name);
        if destination.exists() {
            let source_identity = file_checksum(&source)?;
            let destination_identity = file_checksum(&destination)?;
            if source_identity != destination_identity {
                return Err(HawDBError::Storage(format!(
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
        target_head: None,
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
    if record.state != DerivedRepairAuditState::Prepared {
        return Err(HawDBError::Storage(
            "pending derived repair record is not prepared".into(),
        ));
    }
    validate_quarantine(path, record)?;
    let manifest = match &record.plan.branch {
        Some(source) => {
            let published = super::published_branch(path, source)?;
            let manifest = DurableManifest::load(&published.manifest_path)?;
            if manifest.checkpoint_epoch == record.plan.source_generation {
                super::validate_source_identity(path, &record.plan)?;
            } else if manifest.checkpoint_epoch == record.plan.target_generation {
                let identity = file_checksum(&path.join(BRANCH_HEAD_FILE))?;
                if manifest.checkpoint_commit_epoch != record.plan.source_commit_epoch
                    || published.head.logical_commit_epoch != record.plan.source_commit_epoch
                    || record.target_head.as_ref().is_none_or(|target| {
                        target.byte_length != identity.0 || target.sha256 != identity.2.to_string()
                    })
                {
                    return Err(HawDBError::Storage("pending derived repair target head identity does not match the prepared publication".into()));
                }
            }
            manifest
        }
        None => DurableManifest::load(&path.join(MANIFEST_FILE))?,
    };
    if manifest.checkpoint_epoch != record.plan.source_generation
        && manifest.checkpoint_epoch != record.plan.target_generation
    {
        return Err(HawDBError::Storage(
            "pending derived repair does not match the published generation".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn record_target_head(
    path: &Path,
    record: &mut DerivedRepairAuditRecord,
    target: &crate::branch_head::BranchHead,
) -> Result<()> {
    validate_pending_record(path, record)?;
    super::validate_source_identity(path, &record.plan)?;
    let source =
        record.plan.branch.as_ref().ok_or_else(|| {
            HawDBError::Storage("target head intent requires a branch repair".into())
        })?;
    if target.project_id != *source.project_id.as_bytes()
        || target.branch_id != *source.branch_id.as_bytes()
        || target.logical_commit_epoch != record.plan.source_commit_epoch
    {
        return Err(HawDBError::StorageIntegrity(
            "repair publication candidate has the wrong branch identity".into(),
        ));
    }
    let encoded = target.encode().map_err(HawDBError::from_storage_error)?;
    record.target_head = Some(RepairTargetHead {
        byte_length: encoded.len() as u64,
        sha256: hawdb_integrity::integrity_digest(&encoded)
            .sha256
            .to_string(),
    });
    write_audit_record(&pending_record_path(path, &record.plan), record)
}

#[doc(hidden)]
pub fn pending_record_paths(path: &Path) -> Result<Vec<PathBuf>> {
    let directory = super::super::doctor::repair_directory(path)?;
    pending_record_paths_at_directory(&directory)
}

/// Admission already selected its exact storage directory. Namespace checks
/// must not reinterpret a malformed legacy manifest as a project selector;
/// the authoritative manifest decoder owns that corruption diagnosis.
pub(super) fn pending_record_paths_at_directory(path: &Path) -> Result<Vec<PathBuf>> {
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
        _ => Err(HawDBError::Storage(
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
        return Err(HawDBError::Storage(
            "database has a pending derived repair for a different plan".to_string(),
        ));
    }
    Ok(Some(record))
}

#[doc(hidden)]
pub fn quarantine_directory(path: &Path, plan: &DerivedArtifactRepairPlan) -> PathBuf {
    repair_doctor_directory(path, plan)
        .join(DERIVED_QUARANTINE_DIRECTORY)
        .join(&plan.plan_id)
}

pub(super) fn validate_quarantine(path: &Path, record: &DerivedRepairAuditRecord) -> Result<()> {
    validate_quarantined_file_names(record)?;
    let quarantine = quarantine_directory(path, &record.plan);
    for expected in &record.quarantined_files {
        let actual = file_checksum(&quarantine.join(&expected.name))?;
        if actual.0 != expected.len
            || actual.1 != expected.crc32c
            || actual.2.to_string() != expected.sha256
        {
            return Err(HawDBError::Storage(format!(
                "derived repair quarantine identity changed: {}",
                expected.name
            )));
        }
        if expected.name == MANIFEST_FILE
            && (actual.0 != record.plan.manifest_len
                || actual.1 != record.plan.manifest_crc32c
                || actual.2.to_string() != record.plan.manifest_sha256)
        {
            return Err(HawDBError::Storage(
                "derived repair quarantine manifest does not match the planned source".to_string(),
            ));
        }
        if let Some(source) = &record.plan.branch {
            let planned = match expected.name.as_str() {
                BRANCH_HEAD_FILE => Some((
                    source.head_len,
                    source.head_crc32c,
                    source.head_sha256.as_str(),
                )),
                BRANCH_PRIVATE_WAL_FILE => Some((
                    record.plan.wal_len,
                    record.plan.wal_crc32c,
                    record.plan.wal_sha256.as_str(),
                )),
                _ => None,
            };
            if planned.is_some_and(|planned| {
                actual.0 != planned.0 || actual.1 != planned.1 || actual.2.to_string() != planned.2
            }) {
                return Err(HawDBError::Storage(
                    "derived repair quarantine does not match the planned branch source".into(),
                ));
            }
        }
    }
    if record.plan.branch.is_some() {
        let head = crate::branch_head::read_branch_head(&quarantine.join(BRANCH_HEAD_FILE))
            .map_err(HawDBError::from_storage_error)?;
        let root_bytes = fs::read(quarantine.join(BRANCH_ROOT_FILE))?;
        if crate::immutable_object::ObjectReference::for_bytes(
            crate::immutable_object::ObjectKind::SealedRoot,
            head.sealed_root.format_version,
            &root_bytes,
        ) != head.sealed_root
        {
            return Err(HawDBError::Storage(
                "derived repair quarantine root does not match its head".into(),
            ));
        }
        let root = crate::sealed_root::SealedRoot::decode(&root_bytes)
            .map_err(HawDBError::from_storage_error)?;
        let manifest_bytes = fs::read(quarantine.join(MANIFEST_FILE))?;
        if crate::immutable_object::ObjectReference::for_bytes(
            crate::immutable_object::ObjectKind::DurableManifest,
            root.durable_manifest.format_version,
            &manifest_bytes,
        ) != root.durable_manifest
        {
            return Err(HawDBError::Storage(
                "derived repair quarantine manifest does not match its root".into(),
            ));
        }
    }
    Ok(())
}

fn validate_quarantined_file_names(record: &DerivedRepairAuditRecord) -> Result<()> {
    let mut allowed = vec![MANIFEST_FILE.to_string()];
    let required = if record.plan.branch.is_some() {
        allowed.extend(
            [BRANCH_HEAD_FILE, BRANCH_ROOT_FILE, BRANCH_PRIVATE_WAL_FILE].map(str::to_string),
        );
        vec![
            MANIFEST_FILE,
            BRANCH_HEAD_FILE,
            BRANCH_ROOT_FILE,
            BRANCH_PRIVATE_WAL_FILE,
        ]
    } else {
        vec![MANIFEST_FILE]
    };
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
    actual.sort();
    let unique_len = actual.len();
    actual.dedup();
    if required
        .iter()
        .any(|name| !actual.iter().any(|actual| actual == name))
        || actual.len() != unique_len
        || actual.iter().any(|name| !allowed.contains(name))
    {
        return Err(HawDBError::Storage(
            "derived repair quarantine file set is invalid".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn target_files(kind: DerivedArtifactKind, generation: u64) -> Vec<String> {
    match kind {
        DerivedArtifactKind::CanonicalAdjacency => vec![
            canonical_adjacency_artifact_generation_file(generation),
            hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(
                generation,
            ),
            hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(
                generation,
            ),
        ],
        DerivedArtifactKind::PersistentPropertyProjection => vec![
            property_projection_artifact_generation_file(generation),
            property_projection_manifest_generation_file(generation),
            hawdb_storage::property_projection::property_projection_descriptor_page_file(
                generation,
            ),
            hawdb_storage::property_projection::property_projection_descriptor_root_file(
                generation,
            ),
        ],
    }
}

/// Retain an interrupted candidate without mistaking the local data directory
/// for the published branch. A validated source head proves that these exact
/// target-generation aliases were never committed; immutable objects and the
/// UUID's private WAL remain untouched.
pub(super) fn archive_unpublished_checkpoint(
    path: &Path,
    record: &DerivedRepairAuditRecord,
    data: &Path,
) -> Result<()> {
    validate_pending_record(path, record)?;
    super::validate_source_identity(path, &record.plan)?;
    let source =
        record.plan.branch.as_ref().ok_or_else(|| {
            HawDBError::Storage("candidate archival requires a branch repair".into())
        })?;
    let published = super::published_branch(path, source)?;
    let archive = quarantine_directory(path, &record.plan).join(format!(
        "unpublished-checkpoint-{}",
        record.plan.target_generation
    ));
    let mut candidates = Vec::new();
    for entry in fs::read_dir(data)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if crate::artifact_files::storage_generation_for_file(name)
            != Some(record.plan.target_generation)
        {
            continue;
        }
        if !entry.file_type()?.is_file()
            || published
                .root
                .checkpoint_bindings
                .iter()
                .any(|binding| binding.relative_path == name)
        {
            return Err(HawDBError::StorageIntegrity(
                "unpublished checkpoint candidate overlaps a published dependency".into(),
            ));
        }
        candidates.push((name.to_string(), entry.path()));
    }
    if candidates.is_empty() {
        return Ok(());
    }
    fs::create_dir_all(&archive)?;
    sync_parent_dir(&archive)?;
    for (name, candidate) in candidates {
        let identity = file_checksum(&candidate)?;
        let destination = archive.join(format!("{name}.{}", identity.2));
        if destination.exists() {
            if file_checksum(&destination)? != identity {
                return Err(HawDBError::StorageIntegrity(
                    "unpublished checkpoint archive identity changed".into(),
                ));
            }
            fs::remove_file(&candidate)?;
            sync_parent_dir(&candidate)?;
        } else {
            // Sync the candidate before moving its namespace entry into the
            // durable evidence directory, including a lost-response retry.
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&candidate)?
                .sync_all()?;
            crate::durability::durable_replace_file(&candidate, &destination)
                .map_err(HawDBError::from_storage_error)?;
        }
    }
    Ok(())
}

fn write_audit_record(path: &Path, record: &DerivedRepairAuditRecord) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        HawDBError::Storage("derived repair audit path has no parent".to_string())
    })?;
    fs::create_dir_all(parent)?;
    sync_parent_dir(parent)?;
    let encoded = serde_json::to_vec_pretty(record).map_err(|error| {
        HawDBError::Storage(format!("failed to encode derived repair audit: {error}"))
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
    hawdb_storage::durability::durable_replace_file(&temporary, path)
        .map_err(HawDBError::from_storage_error)
}

fn load_audit_record(path: &Path) -> Result<DerivedRepairAuditRecord> {
    if fs::metadata(path)?.len() > MAX_REPAIR_AUDIT_BYTES {
        return Err(HawDBError::Storage(format!(
            "derived repair audit exceeds the {MAX_REPAIR_AUDIT_BYTES} byte limit"
        )));
    }
    let record =
        serde_json::from_slice::<DerivedRepairAuditRecord>(&fs::read(path)?).map_err(|error| {
            HawDBError::Storage(format!("invalid derived repair audit record: {error}"))
        })?;
    if record.protocol != DERIVED_ARTIFACT_REPAIR_PROTOCOL
        || record.plan.protocol != DERIVED_ARTIFACT_REPAIR_PROTOCOL
        || record.plan.plan_id != plan_identity(&record.plan)
        || record.target_head.as_ref().is_some_and(|target| {
            record.plan.branch.is_none()
                || target.byte_length == 0
                || target
                    .sha256
                    .parse::<hawdb_integrity::Sha256Digest>()
                    .is_err()
        })
    {
        return Err(HawDBError::Storage(
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
    repair_doctor_directory(path, plan)
        .join(format!("{}.derived-repair.pending.json", plan.plan_id))
}

fn applied_record_path(path: &Path, plan: &DerivedArtifactRepairPlan) -> PathBuf {
    repair_doctor_directory(path, plan)
        .join(format!("{}.derived-repair.applied.json", plan.plan_id))
}

fn repair_doctor_directory(path: &Path, plan: &DerivedArtifactRepairPlan) -> PathBuf {
    match &plan.branch {
        Some(source)
            if path
                .file_name()
                .is_some_and(|name| name == source.branch_id.to_string().as_str())
                && path.parent().is_some_and(|parent| {
                    parent.file_name().is_some_and(|name| name == "branches")
                }) =>
        {
            doctor_directory(path)
        }
        Some(source) => doctor_directory(&path.join("branches").join(source.branch_id.to_string())),
        None => doctor_directory(path),
    }
}

fn repair_source_files(
    path: &Path,
    plan: &DerivedArtifactRepairPlan,
) -> Result<Vec<(String, PathBuf)>> {
    let mut files = match &plan.branch {
        Some(source) => {
            let published = super::published_branch(path, source)?;
            let mut files = vec![
                (MANIFEST_FILE.to_string(), published.manifest_path),
                (BRANCH_HEAD_FILE.to_string(), path.join(BRANCH_HEAD_FILE)),
                (
                    BRANCH_ROOT_FILE.to_string(),
                    published.objects.object_path(published.head.sealed_root),
                ),
                (BRANCH_PRIVATE_WAL_FILE.to_string(), published.wal_path),
            ];
            for binding in published.root.checkpoint_bindings {
                if plan.targets.iter().any(|target| {
                    target_files(*target, plan.source_generation).contains(&binding.relative_path)
                }) {
                    files.push((
                        binding.relative_path,
                        published.objects.object_path(binding.reference),
                    ));
                }
            }
            files
        }
        None => {
            let mut names = vec![MANIFEST_FILE.to_string()];
            for target in &plan.targets {
                names.extend(target_files(*target, plan.source_generation));
            }
            names
                .into_iter()
                .map(|name| {
                    let source = path.join(&name);
                    (name, source)
                })
                .collect()
        }
    };
    files.sort();
    files.dedup();
    Ok(files)
}

fn file_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string)
        .ok_or_else(|| HawDBError::Storage("derived repair path has no file name".to_string()))
}
