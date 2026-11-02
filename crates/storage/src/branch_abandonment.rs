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

//! Explicit abandonment of a pending create, with durable evidence ownership.
//!
//! Quarantine is deliberately outside the ordinary object and UUID sweeps. A
//! full namespace snapshot is used because a damaged root can hide dependencies.

use crate::branch_catalog::{
    self, BranchId, BranchRecord, BranchState, Catalog, CatalogMetadataLease, CreateOutcome,
};
use crate::branch_project::ProjectMetadata;
use crate::durability;
use crate::file_io::{self as fs, File, OpenOptions};
use crate::ownership::{DatabaseDirectoryLease, DATABASE_DIRECTORY_LOCK_FILE};
use hawdb_core::{HawDBError, Result, Uuid};
use hawdb_integrity::{sha256, IntegrityHasher};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};

const VERSION: u32 = 1;
const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
const BUFFER_BYTES: usize = 64 * 1024;

/// Bounds include private files, immutable objects, staging, and the catalog.
/// Directory entries consume the file-count budget; encoded metadata has an
/// additional fixed 16 MiB ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BranchAbandonmentLimits {
    pub max_files: usize,
    pub max_bytes: u64,
}

impl Default for BranchAbandonmentLimits {
    fn default() -> Self {
        Self {
            max_files: 100_000,
            max_bytes: 1024 * 1024 * 1024,
        }
    }
}

/// Inspection does not authorize a transition. The complete immutable namespace
/// is retained, including unrelated objects; quarantine has no automatic expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchAbandonmentPlan {
    catalog_revision: u64,
    manifest: Manifest,
}

impl BranchAbandonmentPlan {
    pub fn branch_id(&self) -> Uuid {
        self.manifest.branch_id().as_uuid()
    }
    pub fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }
    pub fn metadata_revision(&self) -> u64 {
        self.manifest.metadata_revision
    }
    pub fn evidence_files(&self) -> usize {
        self.manifest.files.len() + 1
    }
    pub fn evidence_bytes(&self) -> u64 {
        self.manifest
            .files
            .iter()
            .map(|file| file.bytes)
            .sum::<u64>()
            + self.manifest.catalog.len() as u64
    }

    /// Explicitly authorize abandonment and indefinite retention of a complete
    /// immutable namespace snapshot, even when some dependencies are missing.
    pub fn acknowledge_potential_data_loss(&self) -> BranchAbandonmentAcknowledgement {
        BranchAbandonmentAcknowledgement { plan: self.clone() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchAbandonmentAcknowledgement {
    plan: BranchAbandonmentPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchAbandonmentReport {
    pub branch_id: Uuid,
    pub catalog_revision: u64,
    pub metadata_revision: u64,
    pub evidence_files: usize,
    pub evidence_bytes: u64,
    pub quarantine_directory: PathBuf,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceFile {
    path: String,
    bytes: u64,
    digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    branch: String,
    metadata_revision: u64,
    catalog: Vec<u8>,
    inventory_entries: usize,
    files: Vec<EvidenceFile>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<T = Manifest> {
    manifest: T,
    digest: [u8; 32],
}

impl Manifest {
    fn branch_id(&self) -> BranchId {
        BranchId::parse(&self.branch).expect("validated abandonment branch identity")
    }

    fn digest(&self) -> Result<[u8; 32]> {
        Ok(*sha256(&bounded_json(self)?).as_bytes())
    }

    fn encode_intent(&self) -> Result<Vec<u8>> {
        bounded_json(&Envelope {
            manifest: self,
            digest: self.digest()?,
        })
    }

    fn original(&self) -> Result<(Catalog, BranchRecord)> {
        let catalog = Catalog::decode(&self.catalog).map_err(invalid)?;
        let branch = BranchId::parse(&self.branch).map_err(invalid)?;
        let record = catalog
            .branches
            .iter()
            .find(|r| r.id == branch)
            .ok_or_else(|| invalid("quarantine branch is absent"))?
            .clone();
        if self.version != VERSION
            || branch.as_uuid().to_string() != self.branch
            || record.state != BranchState::Creating
            || record.create_outcome != CreateOutcome::Pending
            || record.metadata_revision != self.metadata_revision
        {
            return Err(invalid("quarantine does not identify a pending create"));
        }
        if self.inventory_entries < self.files.len() || self.inventory_entries == 0 {
            return Err(invalid("invalid quarantine inventory entry count"));
        }
        let mut previous: Option<&str> = None;
        for file in &self.files {
            let path = Path::new(&file.path);
            let private_prefix = format!("{}/", self.branch);
            if file.path.len() > 4096
                || file.path.contains('\\')
                || !path
                    .components()
                    .all(|part| matches!(part, Component::Normal(_)))
                || !(file.path.starts_with("objects/") || file.path.starts_with(&private_prefix))
                || previous.is_some_and(|last| last >= file.path.as_str())
            {
                return Err(invalid("invalid quarantine source path or order"));
            }
            previous = Some(&file.path);
        }
        Ok((catalog, record))
    }

    fn validate_limits(&self, limits: BranchAbandonmentLimits) -> Result<()> {
        let bytes = self
            .files
            .iter()
            .try_fold(self.catalog.len() as u64, |sum, file| {
                sum.checked_add(file.bytes)
            })
            .ok_or_else(|| invalid("quarantine byte count overflow"))?;
        let files = self
            .inventory_entries
            .checked_add(1)
            .ok_or_else(|| invalid("quarantine file count overflow"))?;
        if files > limits.max_files {
            return Err(HawDBError::BranchEvidenceBudgetExceeded {
                resource: "files",
                requested: files as u64,
                limit: limits.max_files as u64,
            });
        }
        if bytes > limits.max_bytes {
            return Err(HawDBError::BranchEvidenceBudgetExceeded {
                resource: "bytes",
                requested: bytes,
                limit: limits.max_bytes,
            });
        }
        Ok(())
    }

    fn matches(&self, catalog: &Catalog, terminal: bool) -> Result<()> {
        let (original, mut expected) = self.original()?;
        if terminal {
            expected.state = BranchState::Deleted;
            expected.create_outcome = CreateOutcome::Abandoned;
            expected.metadata_revision = expected
                .metadata_revision
                .checked_add(1)
                .ok_or_else(|| invalid("branch revision overflow"))?;
        }
        if catalog.project_id != original.project_id || !catalog.branches.contains(&expected) {
            return Err(invalid("branch abandonment identity or state changed"));
        }
        Ok(())
    }
}

impl ProjectMetadata {
    /// Inspect an unowned pending branch without admitting main. Close data
    /// runtimes before this maintenance operation; any active owner rejects it.
    pub fn inspect_pending_branch_abandonment(
        &self,
        branch: Uuid,
        limits: BranchAbandonmentLimits,
    ) -> Result<BranchAbandonmentPlan> {
        let branch = BranchId::new(branch).map_err(invalid)?;
        let directory = self.file_descriptors().root().join("branches");
        let _metadata = CatalogMetadataLease::acquire_blocking(&directory)?;
        let catalog = branch_catalog::read_catalog(&directory.join("catalog.hawdb"))?;
        if catalog.project_id != self.selector().project_id() {
            return Err(invalid("project identity changed"));
        }
        let _lease = quiesce(&directory, &catalog, branch)?;
        let archive = archive_directory(&directory, branch);
        let manifest = if let Some(manifest) = load_intent(&archive)? {
            let terminal = catalog.branches.iter().any(|record| {
                record.id == branch && record.create_outcome == CreateOutcome::Abandoned
            });
            manifest.matches(&catalog, terminal)?;
            manifest.validate_limits(limits)?;
            manifest
        } else {
            let record = catalog
                .branches
                .iter()
                .find(|record| record.id == branch)
                .ok_or_else(|| invalid("pending branch is absent"))?;
            if record.state != BranchState::Creating
                || record.create_outcome != CreateOutcome::Pending
            {
                return Err(invalid("abandonment requires a pending create"));
            }
            let catalog_bytes = catalog.encode().map_err(invalid)?;
            let (files, inventory_entries) =
                inventory_sources(&directory, branch, limits, catalog_bytes.len() as u64)?;
            let manifest = Manifest {
                version: VERSION,
                branch: branch.as_uuid().to_string(),
                metadata_revision: record.metadata_revision,
                catalog: catalog_bytes,
                inventory_entries,
                files,
            };
            manifest.validate_limits(limits)?;
            manifest
        };
        // An acknowledged plan must fit its durable encoding before any
        // archive directory can reserve automatic recovery for this branch.
        drop(manifest.encode_intent()?);
        Ok(BranchAbandonmentPlan {
            catalog_revision: catalog.revision,
            manifest,
        })
    }

    /// Preserve every planned byte before publishing Abandoned. A lost response
    /// is replayable with the same acknowledgement, or by inspecting again.
    pub fn abandon_pending_branch(
        &self,
        acknowledgement: BranchAbandonmentAcknowledgement,
    ) -> Result<BranchAbandonmentReport> {
        let plan = acknowledgement.plan;
        let branch = plan.manifest.branch_id();
        let directory = self.file_descriptors().root().join("branches");
        let catalog_path = directory.join("catalog.hawdb");
        let _metadata = CatalogMetadataLease::acquire_blocking(&directory)?;
        let mut catalog = branch_catalog::read_catalog(&catalog_path)?;
        if catalog.project_id != self.selector().project_id() {
            return Err(invalid("project identity changed"));
        }
        let _lease = quiesce(&directory, &catalog, branch)?;
        let archive = archive_directory(&directory, branch);
        let existing = load_intent(&archive)?;
        if existing
            .as_ref()
            .is_some_and(|manifest| *manifest != plan.manifest)
        {
            return Err(invalid("quarantine intent differs from acknowledgement"));
        }
        let replayed = catalog
            .branches
            .iter()
            .any(|record| record.id == branch && record.create_outcome == CreateOutcome::Abandoned);
        plan.manifest.matches(&catalog, replayed)?;
        if replayed && existing.is_none() {
            return Err(invalid("abandoned branch has no matching operator intent"));
        }
        if !replayed {
            if catalog.revision != plan.catalog_revision {
                return Err(HawDBError::Semantic(
                    "stale branch abandonment catalog revision".into(),
                ));
            }
            verify_sources(&directory, &plan.manifest)?;
            if existing.is_none() {
                let encoded = plan.manifest.encode_intent()?;
                fs::create_dir_all(&archive)?;
                durability::sync_directory_ancestors(&archive)?;
                publish_file(&archive.join("intent.json"), &encoded)?;
            }
            sync_file(&archive.join("intent.json"))?;
            durability::sync_directory_ancestors(&archive)?;
            copy_evidence(&directory, &archive, &plan.manifest)?;
            catalog
                .abandon_create(branch, plan.manifest.metadata_revision)
                .map_err(invalid)?;
            branch_catalog::write_catalog_locked(&catalog_path, &catalog)?;
        } else {
            verify_archive(&archive, &plan.manifest)?;
            // The previous catalog rename may have been visible without its
            // directory barrier completing. Reestablish durability before GC.
            sync_file(&catalog_path)?;
            durability::sync_parent_directory(&catalog_path)?;
        }
        publish_file(&archive.join("committed"), &plan.manifest.digest()?)?;
        let record = catalog
            .branches
            .iter()
            .find(|record| record.id == branch)
            .expect("validated terminal branch");
        Ok(BranchAbandonmentReport {
            branch_id: branch.as_uuid(),
            catalog_revision: catalog.revision,
            metadata_revision: record.metadata_revision,
            evidence_files: plan.evidence_files(),
            evidence_bytes: plan.evidence_bytes(),
            quarantine_directory: archive,
            replayed,
        })
    }
}

fn archive_directory(directory: &Path, branch: BranchId) -> PathBuf {
    directory
        .join("quarantine")
        .join(branch.as_uuid().to_string())
}

pub(crate) fn has_abandonment(catalog_path: &Path, branch: BranchId) -> io::Result<bool> {
    let directory = catalog_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| io::Error::other("catalog has no parent"))?;
    // Even an interrupted intent publication reserves the operation. Letting
    // recovery finish a healthy child here would strand its incomplete archive
    // and permanently block GC without a resumable pending receipt.
    match fs::symlink_metadata(archive_directory(directory, branch)) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn quiesce(
    directory: &Path,
    catalog: &Catalog,
    target: BranchId,
) -> Result<Option<DatabaseDirectoryLease>> {
    let mut target_lease = None;
    for record in &catalog.branches {
        let path = directory.join(record.id.as_uuid().to_string());
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => return Err(invalid("invalid branch lease directory")),
        }
        let lease =
            DatabaseDirectoryLease::acquire(&path).map_err(HawDBError::from_storage_error)?;
        if record.id == target {
            target_lease = Some(lease);
        }
    }
    Ok(target_lease)
}

fn inventory_sources(
    directory: &Path,
    branch: BranchId,
    limits: BranchAbandonmentLimits,
    catalog_bytes: u64,
) -> Result<(Vec<EvidenceFile>, usize)> {
    let (paths, entries) = source_paths(
        directory,
        &["objects".to_owned(), branch.as_uuid().to_string()],
        limits.max_files,
    )?;
    let mut bytes = catalog_bytes;
    let mut files = Vec::new();
    for path in paths {
        let length = fs::symlink_metadata(directory.join(&path))?.len();
        bytes = bytes
            .checked_add(length)
            .filter(|sum| *sum <= limits.max_bytes)
            .ok_or_else(|| invalid("branch abandonment byte limit exceeded"))?;
        files.push(EvidenceFile {
            digest: digest_file(&directory.join(&path), length)?,
            path,
            bytes: length,
        });
    }
    Ok((files, entries))
}

fn source_paths(directory: &Path, roots: &[String], limit: usize) -> Result<(Vec<String>, usize)> {
    let mut pending: BTreeSet<_> = roots.iter().cloned().collect();
    let mut files = BTreeSet::new();
    let mut visited = 0usize;
    while let Some(relative) = pending.pop_first() {
        visited = visited
            .checked_add(1)
            .filter(|count| *count <= limit)
            .ok_or_else(|| invalid("branch abandonment entry limit exceeded"))?;
        let path = directory.join(&relative);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound && roots.contains(&relative) => {
                continue
            }
            Err(error) => return Err(error.into()),
        };
        if metadata.is_file() {
            files.insert(relative);
            continue;
        }
        if !metadata.is_dir() {
            return Err(invalid("nonregular branch abandonment evidence"));
        }
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            if !relative.contains('/')
                && relative != "objects"
                && entry.file_name() == DATABASE_DIRECTORY_LOCK_FILE
            {
                continue;
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| invalid("non-UTF8 evidence path"))?;
            if name.contains('\\') {
                return Err(invalid("noncanonical evidence path"));
            }
            let child = format!("{relative}/{name}");
            if child.len() > 4096 || pending.len().saturating_add(visited) >= limit {
                return Err(invalid("branch abandonment entry limit exceeded"));
            }
            pending.insert(child);
        }
    }
    Ok((files.into_iter().collect(), visited))
}

fn verify_sources(directory: &Path, manifest: &Manifest) -> Result<()> {
    let expected: Vec<_> = manifest
        .files
        .iter()
        .filter(|file| file.path.starts_with(&format!("{}/", manifest.branch)))
        .map(|file| file.path.clone())
        .collect();
    let (actual, _) = source_paths(
        directory,
        std::slice::from_ref(&manifest.branch),
        manifest.inventory_entries,
    )?;
    if actual != expected {
        return Err(invalid("pending branch evidence changed after inspection"));
    }
    for file in &manifest.files {
        if digest_file(&directory.join(&file.path), file.bytes)? != file.digest {
            return Err(invalid("abandonment source bytes changed after inspection"));
        }
    }
    Ok(())
}

fn digest_file(path: &Path, length: u64) -> Result<[u8; 32]> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() != length {
        return Err(invalid("quarantine file type or length differs"));
    }
    let mut file = File::open(path)?;
    let mut hasher = IntegrityHasher::new();
    let mut remaining = length;
    let mut buffer = [0u8; BUFFER_BYTES];
    while remaining > 0 {
        let amount = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..amount])?;
        hasher.update(&buffer[..amount]);
        remaining -= amount as u64;
    }
    if file.read(&mut buffer[..1])? != 0 {
        return Err(invalid("quarantine source grew during inspection"));
    }
    Ok(*hasher.finish().sha256.as_bytes())
}

fn load_intent(archive: &Path) -> Result<Option<Manifest>> {
    for path in [archive.parent().expect("quarantine parent"), archive] {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_dir() => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
            Ok(_) => return Err(invalid("invalid quarantine directory")),
        }
    }
    let path = archive.join("intent.json");
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    let bytes = read_bounded(&path, MAX_MANIFEST_BYTES)?;
    let envelope: Envelope = serde_json::from_slice(&bytes).map_err(invalid)?;
    if envelope.digest != envelope.manifest.digest()? {
        return Err(invalid("quarantine intent checksum mismatch"));
    }
    envelope.manifest.original()?;
    Ok(Some(envelope.manifest))
}

fn bounded_json(value: &impl Serialize) -> Result<Vec<u8>> {
    struct Buffer(Vec<u8>);
    impl Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() as u64 > MAX_MANIFEST_BYTES.saturating_sub(self.0.len() as u64) {
                return Err(io::Error::other(
                    "quarantine JSON exceeds its encoding limit",
                ));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut output = Buffer(Vec::new());
    serde_json::to_writer(&mut output, value).map_err(invalid)?;
    Ok(output.0)
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err(invalid("invalid quarantine record type or length"));
    }
    let mut bytes = Vec::new();
    File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(invalid("quarantine record exceeds limit"));
    }
    Ok(bytes)
}

fn sync_file(path: &Path) -> Result<()> {
    OpenOptions::new()
        .read(true)
        .write(cfg!(windows))
        .open(path)?
        .sync_all()?;
    Ok(())
}

fn regular_file_present(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err(invalid("quarantine destination is not a regular file")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn publish_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let candidate = path.with_extension("partial");
    if regular_file_present(path)? {
        if read_bounded(path, MAX_MANIFEST_BYTES)? != bytes {
            return Err(invalid(
                "quarantine publication conflicts with existing evidence",
            ));
        }
        sync_file(path)?;
        durability::sync_parent_directory(path)?;
        return Ok(());
    }
    write_candidate(&candidate, |file| {
        file.write_all(bytes)?;
        Ok(())
    })?;
    durability::durable_replace_file(&candidate, path)?;
    Ok(())
}

fn write_candidate(path: &Path, write: impl FnOnce(&mut File) -> Result<()>) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
        Ok(_) => return Err(invalid("nonregular quarantine candidate")),
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    write(&mut file)?;
    file.sync_all()?;
    Ok(())
}

fn copy_evidence(directory: &Path, archive: &Path, manifest: &Manifest) -> Result<()> {
    for (index, evidence) in manifest.files.iter().enumerate() {
        let destination = archive.join(format!("file-{index}"));
        if regular_file_present(&destination)? {
            if digest_file(&destination, evidence.bytes)? != evidence.digest {
                return Err(invalid("existing quarantine evidence differs"));
            }
            sync_file(&destination)?;
            durability::sync_parent_directory(&destination)?;
            continue;
        }
        let candidate = destination.with_extension("partial");
        write_candidate(&candidate, |output| {
            let mut input = File::open(directory.join(&evidence.path))?;
            let mut remaining = evidence.bytes;
            let mut buffer = [0u8; BUFFER_BYTES];
            let mut digest = IntegrityHasher::new();
            while remaining > 0 {
                let amount = remaining.min(buffer.len() as u64) as usize;
                input.read_exact(&mut buffer[..amount])?;
                output.write_all(&buffer[..amount])?;
                digest.update(&buffer[..amount]);
                remaining -= amount as u64;
            }
            if input.read(&mut buffer[..1])? != 0
                || digest.finish().sha256.as_bytes() != &evidence.digest
            {
                return Err(invalid("quarantine source changed while copying"));
            }
            Ok(())
        })?;
        durability::durable_replace_file(&candidate, &destination)?;
    }
    verify_archive(archive, manifest)?;
    Ok(())
}

fn verify_archive(archive: &Path, manifest: &Manifest) -> Result<()> {
    for (index, evidence) in manifest.files.iter().enumerate() {
        if digest_file(&archive.join(format!("file-{index}")), evidence.bytes)? != evidence.digest {
            return Err(invalid("quarantine evidence checksum mismatch"));
        }
    }
    Ok(())
}

/// Only this module constructs the evidence map after validating every archive.
/// Live-root verification still runs before these proofs can permit a sweep.
#[derive(Default)]
pub(crate) struct QuarantinedObjects(BTreeMap<PathBuf, (u64, [u8; 32])>);

impl QuarantinedObjects {
    pub(crate) fn preserves(&self, path: &Path) -> Result<bool> {
        let Some((length, digest)) = self.0.get(path) else {
            return Ok(false);
        };
        Ok(digest_file(path, *length)? == *digest)
    }
}

pub(crate) struct ReclamationSnapshot {
    pub catalog: Catalog,
    pub proofs: QuarantinedObjects,
    pub metadata: CatalogMetadataLease,
}

pub(crate) fn prepare_reclamation(
    catalog_path: &Path,
    limits: BranchAbandonmentLimits,
) -> Result<ReclamationSnapshot> {
    prepare_reclamation_with_verifier(catalog_path, |directory, catalog| {
        reclamation_evidence(directory, catalog, limits)
    })
}

fn prepare_reclamation_with_verifier(
    catalog_path: &Path,
    verify: impl FnOnce(&Path, &Catalog) -> Result<QuarantinedObjects>,
) -> Result<ReclamationSnapshot> {
    let directory = catalog_path
        .parent()
        .ok_or_else(|| invalid("branch catalog has no project directory"))?;
    let catalog = {
        let _metadata = CatalogMetadataLease::acquire_blocking(directory)?;
        branch_catalog::read_catalog(catalog_path)?
    };
    // Committed quarantine archives are immutable and never swept. Retain
    // full byte verification on every call, without holding up publishers.
    // A concurrent abandonment or catalog transition invalidates this view.
    let proofs = verify(directory, &catalog);
    let metadata = CatalogMetadataLease::acquire_blocking(directory)?;
    if branch_catalog::read_catalog(catalog_path)? != catalog {
        return Err(HawDBError::BranchBusy {
            resource: "catalog changed during quarantine verification; retry reclamation",
        });
    }
    Ok(ReclamationSnapshot {
        catalog,
        proofs: proofs?,
        metadata,
    })
}

pub(crate) fn reclamation_evidence(
    directory: &Path,
    catalog: &Catalog,
    limits: BranchAbandonmentLimits,
) -> Result<QuarantinedObjects> {
    let mut proofs = QuarantinedObjects::default();
    let mut remaining = limits;
    for record in &catalog.branches {
        let archive = archive_directory(directory, record.id);
        let Some(manifest) = load_intent(&archive)? else {
            if record.create_outcome == CreateOutcome::Abandoned || fs::try_exists(&archive)? {
                return Err(invalid("quarantine intent is missing"));
            }
            continue;
        };
        manifest.validate_limits(remaining)?;
        remaining.max_files -= manifest.inventory_entries + 1;
        remaining.max_bytes -= manifest.files.iter().map(|file| file.bytes).sum::<u64>()
            + manifest.catalog.len() as u64;
        manifest.matches(catalog, true)?;
        if read_bounded(&archive.join("committed"), 32)? != manifest.digest()? {
            return Err(invalid("branch quarantine publication is incomplete"));
        }
        verify_archive(&archive, &manifest)?;
        verify_private_remnants(directory, &manifest)?;
        for evidence in &manifest.files {
            if evidence.path.starts_with("objects/") {
                let path = directory.join(&evidence.path);
                if proofs
                    .0
                    .insert(path, (evidence.bytes, evidence.digest))
                    .is_some_and(|old| old != (evidence.bytes, evidence.digest))
                {
                    return Err(invalid("conflicting branch quarantine evidence"));
                }
            }
        }
    }
    Ok(proofs)
}

// A prior sweep may have removed a prefix or retired the UUID directory.
// Remaining private bytes must still belong to this archived generation.
fn verify_private_remnants(directory: &Path, manifest: &Manifest) -> Result<()> {
    let expected: BTreeMap<_, _> = manifest
        .files
        .iter()
        .filter_map(|file| {
            file.path
                .strip_prefix(&format!("{}/", manifest.branch))
                .map(|path| (path, file))
        })
        .collect();
    for name in [
        manifest.branch.clone(),
        format!(".reclaim-{}", manifest.branch),
    ] {
        let (files, _) = source_paths(
            directory,
            std::slice::from_ref(&name),
            manifest.inventory_entries,
        )?;
        for path in files {
            let relative = path
                .strip_prefix(&format!("{name}/"))
                .expect("inventoried private file");
            let file = expected
                .get(relative)
                .ok_or_else(|| invalid("unarchived private branch evidence"))?;
            if digest_file(&directory.join(path), file.bytes)? != file.digest {
                return Err(invalid("private branch evidence changed after quarantine"));
            }
        }
    }
    Ok(())
}

fn invalid(error: impl std::fmt::Display) -> HawDBError {
    HawDBError::StorageIntegrity(error.to_string())
}

#[cfg(test)]
mod tests;
