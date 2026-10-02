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

//! Project identity and metadata admission, independent of a data runtime.
//!
//! The selector is immutable after bootstrap. It binds a project UUID and its
//! default `main` UUID, not a mutable catalog revision or checkpoint generation.
//! Its canonical v1 encoding is five ordered, newline-terminated ASCII lines:
//! header, `project_id`, `main_branch_id`, `catalog_format`, and `checksum`.
//! Values use a single tab separator; UUIDs use lowercase hyphenated encoding,
//! catalog format is `2`, and checksum is decimal CRC32C of the first four lines.
//! Unknown, duplicate, reordered, noncanonical, or incomplete fields fail closed.
//!
//! This storage seam does not yet replace the facade's ordinary opener. Final
//! bootstrap must publish the selector only after the complete main closure,
//! private WAL, head, and catalog are durable. No selector publisher is exposed
//! here: reserving an identity alone is not evidence of completed bootstrap.

use crate::branch_catalog::{self, BranchId, BranchRecord, BranchState, CatalogMetadataLease};
use crate::durability;
use crate::file_descriptors::ProjectFileDescriptors;
use crate::file_io::{self as fs, File, OpenOptions};
use hawdb_core::{HawDBError, Result};
use hawdb_integrity::crc32c;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub const PROJECT_HEADER: &str = "HAWDB_BRANCH_PROJECT_V1";
const BOOTSTRAP_HEADER: &str = "HAWDB_BRANCH_BOOTSTRAP_V1";
const LEGACY_HEADER: &[u8] = b"HAWDB_MANIFEST_V1\n";
const MANIFEST_FILE: &str = "manifest.hawdb";
const BRANCH_DIRECTORY: &str = "branches";
const CATALOG_FILE: &str = "catalog.hawdb";
const BOOTSTRAP_FILE: &str = "branch-bootstrap.hawdb";
const MAX_SELECTOR_BYTES: usize = 512;
static CANDIDATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Stable project/default-branch identities. Catalog and head publication use
/// these exact UUIDs after a lost response or interrupted bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectSelector {
    project_id: BranchId,
    main_branch_id: BranchId,
}

impl ProjectSelector {
    pub fn new(project_id: BranchId, main_branch_id: BranchId) -> Result<Self> {
        if project_id == main_branch_id {
            return Err(integrity("project and main UUIDs must be distinct"));
        }
        Ok(Self {
            project_id,
            main_branch_id,
        })
    }

    pub fn project_id(self) -> BranchId {
        self.project_id
    }

    pub fn main_branch_id(self) -> BranchId {
        self.main_branch_id
    }

    pub fn encode(self) -> Vec<u8> {
        self.encode_with_header(PROJECT_HEADER)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self> {
        Self::decode_with_header(encoded, PROJECT_HEADER)
    }

    fn encode_with_header(self, header: &str) -> Vec<u8> {
        let body = format!(
            "{header}\nproject_id\t{}\nmain_branch_id\t{}\ncatalog_format\t2\n",
            self.project_id.as_uuid(),
            self.main_branch_id.as_uuid(),
        );
        let checksum = crc32c(body.as_bytes());
        format!("{body}checksum\t{checksum}\n").into_bytes()
    }

    fn decode_with_header(encoded: &[u8], header: &str) -> Result<Self> {
        if encoded.len() > MAX_SELECTOR_BYTES {
            return Err(integrity("project identity exceeds its byte limit"));
        }
        let text =
            std::str::from_utf8(encoded).map_err(|_| integrity("project identity is not UTF-8"))?;
        let mut lines = text.split_terminator('\n');
        if lines.next() != Some(header) {
            return Err(integrity("unknown or incomplete project identity header"));
        }
        let project_id = decode_id(lines.next(), "project_id")?;
        let main_branch_id = decode_id(lines.next(), "main_branch_id")?;
        if lines.next() != Some("catalog_format\t2") {
            return Err(integrity("project identity does not select catalog v2"));
        }
        let checksum = lines
            .next()
            .and_then(|line| line.strip_prefix("checksum\t"))
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or_else(|| integrity("project identity checksum is missing or invalid"))?;
        if lines.next().is_some() {
            return Err(integrity("project identity has trailing fields"));
        }
        let selector = Self::new(project_id, main_branch_id)?;
        // Canonical equality also checks the checksum, final newline, field
        // order, UUID representation, and decimal representation of the CRC.
        let canonical = selector.encode_with_header(header);
        if canonical != encoded {
            return Err(integrity(format!(
                "project identity checksum or canonical encoding differs (recorded {checksum})"
            )));
        }
        Ok(selector)
    }
}

fn decode_id(line: Option<&str>, field: &str) -> Result<BranchId> {
    let value = line
        .and_then(|line| line.split_once('\t'))
        .filter(|(key, _)| *key == field)
        .map(|(_, value)| value)
        .ok_or_else(|| integrity(format!("project identity field {field} is missing")))?;
    BranchId::parse(value).map_err(|error| integrity(error.to_string()))
}

/// An absent selector is distinct from a recognized legacy manifest. Unknown
/// or damaged bytes are errors and must never be treated as either alternative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectManifest {
    Missing,
    Legacy,
    Branch(ProjectSelector),
}

/// Inspect only a bounded prefix. Legacy recovery must still validate the
/// complete legacy manifest before opening its WAL or performing reclamation.
pub fn inspect_project_manifest(root: &Path) -> Result<ProjectManifest> {
    let encoded = match read_bounded(&root.join(MANIFEST_FILE)) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ProjectManifest::Missing);
        }
        Err(error) => return Err(error.into()),
    };
    if encoded.starts_with(LEGACY_HEADER) {
        return Ok(ProjectManifest::Legacy);
    }
    ProjectSelector::decode(&encoded).map(ProjectManifest::Branch)
}

/// A validated metadata snapshot with a shared, finite project FD domain.
/// It owns no branch writer lock, WAL, recovery cache, or runtime lease.
#[derive(Debug)]
pub struct ProjectMetadata {
    files: ProjectFileDescriptors,
    selector: ProjectSelector,
    catalog: branch_catalog::Catalog,
    main_index: usize,
}

impl ProjectMetadata {
    pub fn open(root: &Path, max_open_files: usize) -> Result<Self> {
        let files = ProjectFileDescriptors::acquire_existing(root, max_open_files)?;
        Self::from_files(files)
    }

    pub fn from_files(files: ProjectFileDescriptors) -> Result<Self> {
        let selector = match inspect_project_manifest(files.root())? {
            ProjectManifest::Branch(selector) => selector,
            _ => return Err(integrity("project selector has not been published")),
        };
        let catalog = branch_catalog::read_catalog(&catalog_path(files.root()))?;
        let main_index = validate_catalog_identity(selector, &catalog)?;
        let main = &catalog.branches[main_index];
        if main.state != BranchState::Ready || main.base_root_digest.is_none() {
            return Err(integrity("project selector references an incomplete main"));
        }
        Ok(Self {
            files,
            selector,
            catalog,
            main_index,
        })
    }

    pub fn selector(&self) -> ProjectSelector {
        self.selector
    }

    pub fn catalog(&self) -> &branch_catalog::Catalog {
        &self.catalog
    }

    pub fn main(&self) -> &BranchRecord {
        &self.catalog.branches[self.main_index]
    }

    pub fn file_descriptors(&self) -> &ProjectFileDescriptors {
        &self.files
    }

    /// Read a new atomic catalog image without acquiring a data-branch lock.
    /// A changed project selector is corruption, not a request to retarget.
    pub fn refresh(&self) -> Result<Self> {
        let refreshed = Self::from_files(self.files.clone())?;
        if refreshed.selector != self.selector {
            return Err(integrity("published project identity changed"));
        }
        Ok(refreshed)
    }
}

/// Durably choose bootstrap UUIDs before publishing any catalog/head state.
/// An existing intent or development catalog wins over a newly proposed pair;
/// retries therefore cannot accidentally manufacture a second main incarnation.
///
/// The caller retains its legacy directory lease while adopting an existing
/// database. This operation only reserves identity and does not make a project
/// ready, open a data runtime, or publish the top-level selector.
pub fn reserve_bootstrap_identity(
    files: &ProjectFileDescriptors,
    proposed: ProjectSelector,
) -> Result<ProjectSelector> {
    let root = files.root();
    let metadata_directory = root.join(BRANCH_DIRECTORY);
    fs::create_dir_all(&metadata_directory)?;
    durability::sync_parent_directory(&metadata_directory)?;
    let _metadata_lease = CatalogMetadataLease::acquire(&metadata_directory)?;
    let manifest = inspect_project_manifest(root)?;
    let intent_path = root.join(BOOTSTRAP_FILE);
    let intent = match read_bounded(&intent_path) {
        Ok(encoded) => Some(ProjectSelector::decode_with_header(
            &encoded,
            BOOTSTRAP_HEADER,
        )?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if let ProjectManifest::Branch(selector) = manifest {
        let published = ProjectMetadata::from_files(files.clone())?;
        if intent.is_some_and(|intent| intent != published.selector) {
            return Err(integrity("bootstrap intent differs from published project"));
        }
        return Ok(selector);
    }
    let catalog = match branch_catalog::read_catalog(&catalog_path(root)) {
        Ok(catalog) => Some(catalog),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let selector = match (intent, &catalog) {
        (Some(selector), _) => selector,
        (None, Some(catalog)) => {
            let main = catalog
                .branches
                .iter()
                .find(|branch| branch.name.as_str() == "main")
                .ok_or_else(|| integrity("development catalog has no main"))?;
            ProjectSelector::new(catalog.project_id, main.id)?
        }
        (None, None) => proposed,
    };
    if let Some(catalog) = &catalog {
        validate_catalog_identity(selector, catalog)?;
    }
    if intent.is_none() {
        publish_bootstrap_intent(&intent_path, selector)?;
    }
    Ok(selector)
}

fn validate_catalog_identity(
    selector: ProjectSelector,
    catalog: &branch_catalog::Catalog,
) -> Result<usize> {
    if catalog.project_id != selector.project_id {
        return Err(integrity("catalog project UUID differs from selector"));
    }
    catalog
        .branches
        .iter()
        .position(|branch| {
            branch.id == selector.main_branch_id
                && branch.name.as_str() == "main"
                && branch.parent_id.is_none()
        })
        .ok_or_else(|| integrity("catalog main UUID differs from selector"))
}

pub fn catalog_path(root: &Path) -> PathBuf {
    root.join(BRANCH_DIRECTORY).join(CATALOG_FILE)
}

fn read_bounded(path: &Path) -> io::Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(MAX_SELECTOR_BYTES);
    File::open(path)?
        .take((MAX_SELECTOR_BYTES + 1) as u64)
        .read_to_end(&mut encoded)?;
    Ok(encoded)
}

fn publish_bootstrap_intent(path: &Path, selector: ProjectSelector) -> Result<()> {
    let (candidate, mut file) = loop {
        let sequence = CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = path.with_file_name(format!(
            ".{BOOTSTRAP_FILE}.candidate-{}-{sequence}",
            std::process::id(),
        ));
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&candidate)
        {
            Ok(file) => break (candidate, file),
            // A previous process can leave a candidate and later reuse its
            // PID. Preserve the old evidence and choose another private name.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    };
    {
        file.write_all(&selector.encode_with_header(BOOTSTRAP_HEADER))?;
        file.sync_all()?;
    }
    drop(file);
    // Retain candidates on failure. A failed directory durability barrier may
    // already have installed the complete intent; reopening resolves identity.
    durability::durable_replace_file(&candidate, path)?;
    Ok(())
}

fn integrity(message: impl Into<String>) -> HawDBError {
    HawDBError::StorageIntegrity(message.into())
}

#[cfg(test)]
mod tests;
