//! Bounded, deterministic identity and catalog records for project branches.
//!
//! This module is the storage-owned codec seam for the branching contract.  It
//! owns durable lifecycle publication without opening the logical dataset.
//! Catalog v2 removes development-only expiry metadata; v1 is rejected rather
//! than migrated. State tag 2 remains invalid instead of being reinterpreted.
//! The wire format is versioned and checksummed so publication can reject an
//! incomplete or ambiguous catalog before changing any durable selector.

use crate::branch_head::{
    create_child_branch_head_from_parent, read_branch_head, BranchHead, BranchHeadError,
    ChildBranchHeadRequest, ChildBranchSourceExpectation,
};
use crate::durability;
use crate::file_io::{self as fs, File, OpenOptions, TryLockError};
use crate::immutable_object::{
    BranchReclamationEntry, BranchReclamationInventory, ImmutableObjectError, ImmutableObjectStore,
    ObjectReference, ReclamationReport,
};
use crate::ownership::{DatabaseDirectoryLease, DatabaseDirectoryLeaseError};
use hawdb_core::Uuid;
use hawdb_integrity::crc32c;
use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const MAGIC: &[u8; 8] = b"HBCATV2\0";
const VERSION: u16 = 2;
const MAX_CATALOG_BYTES: usize = 16 * 1024 * 1024;
const MAX_BRANCHES: u32 = 100_000;
const MAX_NAME_BYTES: usize = 128;
const MAX_OWNER_BYTES: usize = 256;
const MAX_REQUEST_KEY_BYTES: usize = 256;
const DIGEST_BYTES: usize = 32;
const METADATA_LOCK_FILE: &str = "metadata.hawdb.lock";
static CANDIDATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Stable project-scoped branch identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchId(Uuid);

impl BranchId {
    pub fn new(value: Uuid) -> Result<Self, CatalogError> {
        if value.is_nil() {
            return Err(CatalogError::InvalidIdentity("branch UUID must not be nil"));
        }
        Ok(Self(value))
    }

    pub fn parse(value: &str) -> Result<Self, CatalogError> {
        let uuid = value
            .parse::<Uuid>()
            .map_err(|_| CatalogError::InvalidIdentity("branch UUID is not valid"))?;
        Self::new(uuid)
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Case-sensitive, validated catalog name.  Names are never used as paths.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchName(String);

impl BranchName {
    /// The canonical root branch name. It is reserved for catalog records and
    /// cannot be supplied as a user-created branch name.
    pub fn main() -> Self {
        Self("main".to_string())
    }

    /// Returns the canonical engine-generated agent name for a branch UUID.
    pub fn generated_agent(id: BranchId) -> Self {
        Self(format!("agent/{}", id.as_uuid()))
    }

    pub fn new(value: impl Into<String>) -> Result<Self, CatalogError> {
        let value = value.into();
        validate_name(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn from_encoded(value: String) -> Result<Self, CatalogError> {
        validate_catalog_name(&value)?;
        Ok(Self(value))
    }
}

/// Explicit selector alternatives prevent UUID-looking names from being
/// silently reinterpreted by an open operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchSelector<'a> {
    Id(BranchId),
    Name(&'a BranchName),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BranchState {
    Creating,
    Ready,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    Pending,
    Succeeded,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchRecord {
    pub id: BranchId,
    pub name: BranchName,
    pub parent_id: Option<BranchId>,
    pub source_commit_epoch: u64,
    pub base_root_digest: Option<[u8; DIGEST_BYTES]>,
    pub metadata_revision: u64,
    pub state: BranchState,
    pub owner: Option<String>,
    pub create_request_key: String,
    pub request_fingerprint: [u8; DIGEST_BYTES],
    pub create_outcome: CreateOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    pub project_id: BranchId,
    pub revision: u64,
    pub branches: Vec<BranchRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRequest {
    pub id: BranchId,
    pub name: BranchName,
    pub parent_id: BranchId,
    pub source_commit_epoch: u64,
    pub base_root_digest: [u8; DIGEST_BYTES],
    pub owner: Option<String>,
    pub request_key: String,
    pub request_fingerprint: [u8; DIGEST_BYTES],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogTransitionError {
    Conflict(&'static str),
    InvalidState(&'static str),
    MissingBranch,
    MissingParent,
    StaleRevision { expected: u64, actual: u64 },
    Overflow(&'static str),
    Validation(CatalogError),
}

impl Display for CatalogTransitionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict(message) => write!(formatter, "branch catalog conflict: {message}"),
            Self::InvalidState(message) => {
                write!(formatter, "invalid branch catalog state: {message}")
            }
            Self::MissingBranch => formatter.write_str("branch catalog branch is missing"),
            Self::MissingParent => formatter.write_str("branch catalog parent is missing"),
            Self::StaleRevision { expected, actual } => write!(
                formatter,
                "branch catalog revision mismatch: expected {expected}, found {actual}"
            ),
            Self::Overflow(field) => write!(formatter, "branch catalog {field} overflow"),
            Self::Validation(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CatalogTransitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Validation(error) => Some(error),
            _ => None,
        }
    }
}

impl Catalog {
    /// Creates a new catalog with an empty, non-deletable root branch.
    pub fn bootstrap(project_id: BranchId, main_id: BranchId) -> Result<Self, CatalogError> {
        if project_id == main_id {
            return Err(CatalogError::InvalidIdentity(
                "project UUID and branch UUID must be distinct",
            ));
        }
        let catalog = Self {
            project_id,
            revision: 1,
            branches: vec![BranchRecord {
                id: main_id,
                name: BranchName::main(),
                parent_id: None,
                source_commit_epoch: 0,
                base_root_digest: None,
                metadata_revision: 1,
                state: BranchState::Ready,
                owner: None,
                create_request_key: "bootstrap".to_string(),
                request_fingerprint: [0; DIGEST_BYTES],
                create_outcome: CreateOutcome::Succeeded,
            }],
        };
        catalog.validate()?;
        Ok(catalog)
    }

    pub fn validate(&self) -> Result<(), CatalogError> {
        if self.branches.len() > MAX_BRANCHES as usize {
            return Err(CatalogError::Limit("branch count"));
        }
        let mut ordered = self.branches.clone();
        ordered.sort_by_key(|branch| branch.id);
        for pair in ordered.windows(2) {
            if pair[0].id == pair[1].id {
                return Err(CatalogError::Duplicate("branch UUID"));
            }
        }
        let mut names = BTreeSet::new();
        let mut main_count = 0;
        for branch in &self.branches {
            if branch.state != BranchState::Deleted && !names.insert(branch.name.clone()) {
                return Err(CatalogError::Duplicate("branch name"));
            }
            if branch.id == self.project_id {
                return Err(CatalogError::InvalidIdentity(
                    "project UUID and branch UUID must be distinct",
                ));
            }
            validate_catalog_name(branch.name.as_str())?;
            if branch.name.as_str() == "main" {
                main_count += 1;
                if branch.parent_id.is_some() {
                    return Err(CatalogError::InvalidState(
                        "main branch cannot have a parent",
                    ));
                }
                if matches!(branch.state, BranchState::Deleting | BranchState::Deleted) {
                    return Err(CatalogError::InvalidState("main branch cannot be deleted"));
                }
            }
            if branch.parent_id == Some(branch.id) {
                return Err(CatalogError::InvalidState(
                    "branch cannot be its own parent",
                ));
            }
            if let Some(parent_id) = branch.parent_id
                && !self
                    .branches
                    .iter()
                    .any(|candidate| candidate.id == parent_id)
            {
                return Err(CatalogError::InvalidIdentity(
                    "branch parent UUID is not present in the catalog",
                ));
            }
            if branch.name.as_str().starts_with("agent/")
                && branch.name.as_str() != format!("agent/{}", branch.id.as_uuid())
            {
                return Err(CatalogError::InvalidName);
            }
            validate_bounded_string(
                &branch.create_request_key,
                MAX_REQUEST_KEY_BYTES,
                "create request key",
            )?;
            if let Some(owner) = &branch.owner {
                validate_bounded_string(owner, MAX_OWNER_BYTES, "owner")?;
            }
            if branch.state == BranchState::Deleted
                && branch.create_outcome == CreateOutcome::Pending
            {
                return Err(CatalogError::InvalidState(
                    "deleted branch cannot have a pending create outcome",
                ));
            }
        }
        if main_count > 1 {
            return Err(CatalogError::Duplicate("main branch"));
        }
        Ok(())
    }

    /// Reserve a branch identity and name before any child files are created.
    /// Replaying the same request key and fingerprint returns the original ID;
    /// a reused key with different input is always a conflict.
    /// The parent's source epoch records creation lineage, not its current
    /// revision. Callers must bind the requested revision to the live head;
    /// this metadata-only transition can reject only revisions before birth.
    pub fn reserve_create(
        &mut self,
        request: CreateRequest,
    ) -> Result<BranchId, CatalogTransitionError> {
        if let Some(existing) = self
            .branches
            .iter()
            .find(|branch| branch.create_request_key == request.request_key)
        {
            if existing.request_fingerprint == request.request_fingerprint {
                return Ok(existing.id);
            }
            return Err(CatalogTransitionError::Conflict(
                "create request key has a different fingerprint",
            ));
        }
        let parent = self
            .branches
            .iter()
            .find(|branch| branch.id == request.parent_id)
            .ok_or(CatalogTransitionError::MissingParent)?;
        if parent.state != BranchState::Ready {
            return Err(CatalogTransitionError::InvalidState(
                "create parent is not ready",
            ));
        }
        if request.source_commit_epoch < parent.source_commit_epoch {
            return Err(CatalogTransitionError::Conflict(
                "create source revision is stale",
            ));
        }
        if self.branches.iter().any(|branch| branch.id == request.id) {
            return Err(CatalogTransitionError::Conflict(
                "branch UUID is already used",
            ));
        }
        if self
            .branches
            .iter()
            .any(|branch| branch.name == request.name && branch.state != BranchState::Deleted)
        {
            return Err(CatalogTransitionError::Conflict("branch name is reserved"));
        }
        let catalog_revision = self.next_revision()?;
        let mut candidate = self.clone();
        candidate.revision = catalog_revision;
        candidate.branches.push(BranchRecord {
            id: request.id,
            name: request.name,
            parent_id: Some(request.parent_id),
            source_commit_epoch: request.source_commit_epoch,
            base_root_digest: Some(request.base_root_digest),
            metadata_revision: 1,
            state: BranchState::Creating,
            owner: request.owner,
            create_request_key: request.request_key,
            request_fingerprint: request.request_fingerprint,
            create_outcome: CreateOutcome::Pending,
        });
        candidate
            .validate()
            .map_err(CatalogTransitionError::Validation)?;
        *self = candidate;
        Ok(request.id)
    }

    pub fn complete_create(
        &mut self,
        id: BranchId,
        expected_metadata_revision: u64,
    ) -> Result<(), CatalogTransitionError> {
        self.transition_create(id, expected_metadata_revision, CreateOutcome::Succeeded)
    }

    pub fn abort_create(
        &mut self,
        id: BranchId,
        expected_metadata_revision: u64,
    ) -> Result<(), CatalogTransitionError> {
        self.transition_create(id, expected_metadata_revision, CreateOutcome::Aborted)
    }

    fn transition_create(
        &mut self,
        id: BranchId,
        expected_metadata_revision: u64,
        outcome: CreateOutcome,
    ) -> Result<(), CatalogTransitionError> {
        let index = self
            .branches
            .iter()
            .position(|branch| branch.id == id)
            .ok_or(CatalogTransitionError::MissingBranch)?;
        let branch = &self.branches[index];
        if branch.metadata_revision != expected_metadata_revision {
            return Err(CatalogTransitionError::StaleRevision {
                expected: expected_metadata_revision,
                actual: branch.metadata_revision,
            });
        }
        if branch.state != BranchState::Creating || branch.create_outcome != CreateOutcome::Pending
        {
            return Err(CatalogTransitionError::InvalidState(
                "create transition requires a pending reservation",
            ));
        }
        let mut candidate = self.clone();
        let catalog_revision = candidate.next_revision()?;
        let metadata_revision = candidate.next_metadata_revision(index)?;
        candidate.revision = catalog_revision;
        candidate.branches[index].metadata_revision = metadata_revision;
        candidate.branches[index].create_outcome = outcome;
        candidate.branches[index].state = match outcome {
            CreateOutcome::Succeeded => BranchState::Ready,
            CreateOutcome::Aborted => BranchState::Deleted,
            CreateOutcome::Pending => unreachable!("pending is rejected above"),
        };
        candidate
            .validate()
            .map_err(CatalogTransitionError::Validation)?;
        *self = candidate;
        Ok(())
    }

    pub fn rename(
        &mut self,
        id: BranchId,
        expected_metadata_revision: u64,
        new_name: BranchName,
    ) -> Result<(), CatalogTransitionError> {
        if new_name.as_str() == "main" || new_name.as_str().starts_with("agent/") {
            return Err(CatalogTransitionError::Conflict("name is reserved"));
        }
        let index = self.index_at_revision(id, expected_metadata_revision)?;
        if self.branches[index].state != BranchState::Ready {
            return Err(CatalogTransitionError::InvalidState(
                "only a ready branch can be renamed",
            ));
        }
        if self.branches.iter().any(|branch| {
            branch.id != id && branch.name == new_name && branch.state != BranchState::Deleted
        }) {
            return Err(CatalogTransitionError::Conflict("branch name is reserved"));
        }
        let mut candidate = self.clone();
        candidate.revision = candidate.next_revision()?;
        candidate.branches[index].metadata_revision = candidate.next_metadata_revision(index)?;
        candidate.branches[index].name = new_name;
        candidate
            .validate()
            .map_err(CatalogTransitionError::Validation)?;
        *self = candidate;
        Ok(())
    }

    pub fn begin_delete(
        &mut self,
        id: BranchId,
        expected_metadata_revision: u64,
    ) -> Result<(), CatalogTransitionError> {
        let index = self.index_at_revision(id, expected_metadata_revision)?;
        let branch = &self.branches[index];
        if branch.name.as_str() == "main" {
            return Err(CatalogTransitionError::InvalidState(
                "main branch is protected",
            ));
        }
        if branch.state != BranchState::Ready {
            return Err(CatalogTransitionError::InvalidState(
                "branch is not deletable",
            ));
        }
        self.transition_state(index, BranchState::Deleting)
    }

    pub fn finish_delete(
        &mut self,
        id: BranchId,
        expected_metadata_revision: u64,
    ) -> Result<(), CatalogTransitionError> {
        let index = self.index_at_revision(id, expected_metadata_revision)?;
        if self.branches[index].state != BranchState::Deleting {
            return Err(CatalogTransitionError::InvalidState(
                "delete finalization requires a deleting branch",
            ));
        }
        self.transition_state(index, BranchState::Deleted)
    }

    fn index_at_revision(
        &self,
        id: BranchId,
        expected_metadata_revision: u64,
    ) -> Result<usize, CatalogTransitionError> {
        let index = self
            .branches
            .iter()
            .position(|branch| branch.id == id)
            .ok_or(CatalogTransitionError::MissingBranch)?;
        let actual = self.branches[index].metadata_revision;
        if actual != expected_metadata_revision {
            return Err(CatalogTransitionError::StaleRevision {
                expected: expected_metadata_revision,
                actual,
            });
        }
        Ok(index)
    }

    fn transition_state(
        &mut self,
        index: usize,
        state: BranchState,
    ) -> Result<(), CatalogTransitionError> {
        let mut candidate = self.clone();
        candidate.revision = candidate.next_revision()?;
        candidate.branches[index].metadata_revision = candidate.next_metadata_revision(index)?;
        candidate.branches[index].state = state;
        candidate
            .validate()
            .map_err(CatalogTransitionError::Validation)?;
        *self = candidate;
        Ok(())
    }

    fn next_revision(&self) -> Result<u64, CatalogTransitionError> {
        self.revision
            .checked_add(1)
            .ok_or(CatalogTransitionError::Overflow("catalog revision"))
    }

    fn next_metadata_revision(&self, index: usize) -> Result<u64, CatalogTransitionError> {
        self.branches[index]
            .metadata_revision
            .checked_add(1)
            .ok_or(CatalogTransitionError::Overflow("branch metadata revision"))
    }

    /// Encode in UUID order.  Sorting is part of the codec contract, so two
    /// equivalent catalogs have byte-identical representations.
    pub fn encode(&self) -> Result<Vec<u8>, CatalogError> {
        self.validate()?;
        let mut branches = self.branches.clone();
        branches.sort_by_key(|branch| branch.id);
        let mut bytes = Vec::with_capacity(128);
        bytes.extend_from_slice(MAGIC);
        put_u16(&mut bytes, VERSION);
        bytes.extend_from_slice(self.project_id.as_uuid().as_bytes());
        put_u64(&mut bytes, self.revision);
        put_u32(&mut bytes, branches.len() as u32);
        for branch in branches {
            encode_branch(&mut bytes, &branch)?;
        }
        let checksum = crc32c(&bytes).get();
        put_u32(&mut bytes, checksum);
        if bytes.len() > MAX_CATALOG_BYTES {
            return Err(CatalogError::Limit("catalog bytes"));
        }
        Ok(bytes)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, CatalogError> {
        if encoded.len() > MAX_CATALOG_BYTES {
            return Err(CatalogError::Limit("catalog bytes"));
        }
        if encoded.len() < MAGIC.len() + 2 + 16 + 8 + 4 + 4 {
            return Err(CatalogError::Truncated);
        }
        let checksum_offset = encoded.len() - 4;
        let expected = u32::from_le_bytes(
            encoded[checksum_offset..]
                .try_into()
                .map_err(|_| CatalogError::Truncated)?,
        );
        let actual = crc32c(&encoded[..checksum_offset]).get();
        if actual != expected {
            return Err(CatalogError::Checksum);
        }
        let mut reader = Reader::new(&encoded[..checksum_offset]);
        if reader.take(MAGIC.len())? != MAGIC {
            return Err(CatalogError::Version);
        }
        if reader.u16()? != VERSION {
            return Err(CatalogError::Version);
        }
        let project_id = BranchId::new(Uuid::from_bytes(reader.array()?))?;
        let revision = reader.u64()?;
        let count = reader.u32()?;
        if count > MAX_BRANCHES {
            return Err(CatalogError::Limit("branch count"));
        }
        let mut branches = Vec::with_capacity(count as usize);
        for _ in 0..count {
            branches.push(decode_branch(&mut reader)?);
        }
        if !reader.is_empty() {
            return Err(CatalogError::TrailingBytes);
        }
        let catalog = Self {
            project_id,
            revision,
            branches,
        };
        catalog.validate()?;
        Ok(catalog)
    }
}

/// Read a published catalog after applying the same byte bound as the decoder.
pub fn read_catalog(path: &Path) -> io::Result<Catalog> {
    let length = fs::metadata(path)?.len();
    if length > MAX_CATALOG_BYTES as u64 {
        return Err(invalid_data("branch catalog exceeds its byte limit"));
    }
    let file = fs::File::open(path)?;
    let mut encoded = Vec::with_capacity(length as usize);
    file.take((MAX_CATALOG_BYTES + 1) as u64)
        .read_to_end(&mut encoded)?;
    Catalog::decode(&encoded).map_err(|error| invalid_data(error.to_string()))
}

/// Publish a catalog with candidate-file sync followed by atomic replacement.
///
/// The destination is never opened for writing.  A failed write or sync removes
/// only its private candidate; a failed replacement is returned without retry,
/// because the caller cannot infer whether the directory operation reached the
/// filesystem.  The caller must reopen before attempting another publication.
pub fn write_catalog(path: &Path, catalog: &Catalog) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| invalid_data("branch catalog destination has no parent"))?;
    let _metadata_lock = CatalogMetadataLease::acquire_blocking(parent)?;
    write_catalog_locked(path, catalog)
}

/// Creates the root catalog once, or reopens the same root identity, while
/// holding the metadata lease across the existence check and publication.
///
/// A caller must not infer that a missing catalog is still missing after a
/// separate read: another initializer can publish it in that interval. This
/// helper returns the one validated `main` record observed under the lease.
pub fn initialize_catalog_file(
    path: &Path,
    project_id: BranchId,
    main_id: BranchId,
) -> Result<BranchRecord, CatalogFileTransitionError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            CatalogFileTransitionError::Io(invalid_data("branch catalog destination has no parent"))
        })?;
    fs::create_dir_all(parent).map_err(CatalogFileTransitionError::Io)?;
    let _metadata_lock =
        CatalogMetadataLease::acquire_blocking(parent).map_err(CatalogFileTransitionError::Io)?;
    match read_catalog(path) {
        Ok(catalog) => main_record(&catalog, project_id, main_id),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let catalog = Catalog::bootstrap(project_id, main_id)
                .map_err(|error| CatalogFileTransitionError::Io(invalid_data(error.to_string())))?;
            let main = catalog.branches[0].clone();
            write_catalog_locked(path, &catalog).map_err(CatalogFileTransitionError::Io)?;
            Ok(main)
        }
        Err(error) => Err(CatalogFileTransitionError::Io(error)),
    }
}

/// Binds the root catalog record to the exact immutable root selected by its
/// initial branch head. The catalog read, validation, and publication occur
/// under one metadata lease, so a concurrent lifecycle transition cannot be
/// overwritten by a stale in-memory catalog image.
pub fn bind_main_head_file(
    path: &Path,
    project_id: BranchId,
    main_id: BranchId,
    sealed_root_digest: [u8; DIGEST_BYTES],
    source_commit_epoch: u64,
) -> Result<BranchRecord, CatalogFileTransitionError> {
    mutate_catalog_file(path, |catalog| {
        let index = main_record_index(catalog, project_id, main_id)?;
        let branch = &catalog.branches[index];
        if let Some(digest) = branch.base_root_digest {
            if digest != sealed_root_digest || branch.source_commit_epoch != source_commit_epoch {
                return Err(CatalogTransitionError::Conflict(
                    "main catalog binding does not match its sealed head",
                ));
            }
            return Ok(CatalogMutation {
                value: branch.clone(),
                changed: false,
            });
        }
        if branch.state != BranchState::Ready {
            return Err(CatalogTransitionError::InvalidState(
                "main branch is not ready for head binding",
            ));
        }
        let next_catalog_revision = catalog.next_revision()?;
        let next_metadata_revision = catalog.next_metadata_revision(index)?;
        let branch = &mut catalog.branches[index];
        branch.base_root_digest = Some(sealed_root_digest);
        branch.source_commit_epoch = source_commit_epoch;
        branch.metadata_revision = next_metadata_revision;
        catalog.revision = next_catalog_revision;
        catalog
            .validate()
            .map_err(CatalogTransitionError::Validation)?;
        Ok(CatalogMutation {
            value: catalog.branches[index].clone(),
            changed: true,
        })
    })
}

/// Publishes a catalog while the caller owns the project metadata lease.
/// Keeping the lock acquisition outside the read/modify/write sequence lets
/// catalog transitions serialize their read and publication as one operation.
fn write_catalog_locked(path: &Path, catalog: &Catalog) -> io::Result<()> {
    let encoded = catalog
        .encode()
        .map_err(|error| invalid_data(error.to_string()))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| invalid_data("branch catalog destination has no parent"))?;
    let sequence = CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let candidate = parent.join(format!(
        ".{}.candidate-{}-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("catalog"),
        std::process::id(),
        sequence
    ));
    let result = (|| {
        {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&candidate)?;
            file.write_all(&encoded)?;
            file.sync_all()?;
        }
        durability::durable_replace_file(&candidate, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&candidate);
    }
    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateReservation {
    pub id: BranchId,
    pub metadata_revision: u64,
    pub replayed: bool,
}

/// Exact identity and revision required to start a durable delete transition.
///
/// A name is intentionally absent. Name resolution belongs to the caller's
/// admission protocol; the storage transition only acts on the resolved,
/// immutable branch identity so a delayed request cannot affect a later name
/// incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteRequest {
    pub id: BranchId,
    pub expected_metadata_revision: u64,
}

/// Durable evidence that a branch has stopped accepting new admission while
/// its deletion is completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteReservation {
    pub id: BranchId,
    pub metadata_revision: u64,
}

/// Result of beginning or resuming a deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteBeginOutcome {
    Deleting(DeleteReservation),
    Deleted(BranchRecord),
}

#[derive(Debug)]
pub enum CatalogFileTransitionError {
    Io(io::Error),
    Transition(CatalogTransitionError),
}

impl Display for CatalogFileTransitionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => Display::fmt(error, formatter),
            Self::Transition(error) => Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for CatalogFileTransitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Transition(error) => Some(error),
        }
    }
}

struct CatalogMutation<T> {
    value: T,
    changed: bool,
}

/// Runs one catalog read/modify/validate/publish cycle while owning the stable
/// project metadata lease. A transition that discovers an already-published
/// idempotent outcome can return `changed = false` and avoid replacing the
/// catalog bytes again.
fn mutate_catalog_file<T>(
    path: &Path,
    transition: impl FnOnce(&mut Catalog) -> Result<CatalogMutation<T>, CatalogTransitionError>,
) -> Result<T, CatalogFileTransitionError> {
    mutate_catalog_file_with_io(path, |catalog| {
        transition(catalog).map_err(CatalogFileTransitionError::Transition)
    })
}

fn mutate_catalog_file_with_io<T>(
    path: &Path,
    transition: impl FnOnce(&mut Catalog) -> Result<CatalogMutation<T>, CatalogFileTransitionError>,
) -> Result<T, CatalogFileTransitionError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            CatalogFileTransitionError::Io(invalid_data("branch catalog destination has no parent"))
        })?;
    let _metadata_lock =
        CatalogMetadataLease::acquire_blocking(parent).map_err(CatalogFileTransitionError::Io)?;
    let mut catalog = read_catalog(path).map_err(CatalogFileTransitionError::Io)?;
    let mutation = transition(&mut catalog)?;
    if mutation.changed {
        write_catalog_locked(path, &catalog).map_err(CatalogFileTransitionError::Io)?;
    }
    Ok(mutation.value)
}

fn main_record(
    catalog: &Catalog,
    project_id: BranchId,
    main_id: BranchId,
) -> Result<BranchRecord, CatalogFileTransitionError> {
    let index = main_record_index(catalog, project_id, main_id)
        .map_err(CatalogFileTransitionError::Transition)?;
    Ok(catalog.branches[index].clone())
}

fn main_record_index(
    catalog: &Catalog,
    project_id: BranchId,
    main_id: BranchId,
) -> Result<usize, CatalogTransitionError> {
    if catalog.project_id != project_id {
        return Err(CatalogTransitionError::Conflict(
            "catalog project identity does not match root initialization",
        ));
    }
    let index = catalog
        .branches
        .iter()
        .position(|branch| branch.id == main_id)
        .ok_or(CatalogTransitionError::MissingBranch)?;
    if catalog.branches[index].name.as_str() != "main" {
        return Err(CatalogTransitionError::Conflict(
            "root branch identity does not name main",
        ));
    }
    Ok(index)
}

// Isolated catalog-transition fixture helper. Production callers must use
// create_branch_from_parent, which validates the live source under this same
// metadata lease before reserving the child.
#[cfg(test)]
fn reserve_create_file(
    path: &Path,
    request: CreateRequest,
) -> Result<CreateReservation, CatalogFileTransitionError> {
    reserve_create_file_with_source(path, request, |_| Ok(()))
}

fn reserve_create_file_with_source(
    path: &Path,
    request: CreateRequest,
    validate_source: impl FnOnce(&Catalog) -> Result<(), CatalogFileTransitionError>,
) -> Result<CreateReservation, CatalogFileTransitionError> {
    mutate_catalog_file_with_io(path, move |catalog| {
        let replayed = catalog
            .branches
            .iter()
            .any(|branch| branch.create_request_key == request.request_key);
        // An idempotent retry resolves the original outcome even if its
        // source has advanced since publication of the child.
        if !replayed {
            validate_source(catalog)?;
        }
        let revision = catalog.revision;
        let id = catalog
            .reserve_create(request)
            .map_err(CatalogFileTransitionError::Transition)?;
        let metadata_revision = catalog
            .branches
            .iter()
            .find(|branch| branch.id == id)
            .map(|branch| branch.metadata_revision)
            .ok_or(CatalogFileTransitionError::Transition(
                CatalogTransitionError::MissingBranch,
            ))?;
        Ok(CatalogMutation {
            value: CreateReservation {
                id,
                metadata_revision,
                replayed,
            },
            changed: catalog.revision != revision,
        })
    })
}

/// Marks a previously reserved child ready after its head and WAL are durable.
pub fn complete_create_file(
    path: &Path,
    reservation: CreateReservation,
) -> Result<(), CatalogFileTransitionError> {
    mutate_catalog_file(path, |catalog| {
        catalog.complete_create(reservation.id, reservation.metadata_revision)?;
        Ok(CatalogMutation {
            value: (),
            changed: true,
        })
    })
}

/// Aborts a reserved child create after a known pre-publication failure.
pub fn abort_create_file(
    path: &Path,
    reservation: CreateReservation,
) -> Result<(), CatalogFileTransitionError> {
    mutate_catalog_file(path, |catalog| {
        catalog.abort_create(reservation.id, reservation.metadata_revision)?;
        Ok(CatalogMutation {
            value: (),
            changed: true,
        })
    })
}

/// Publishes `Ready -> Deleting` under one metadata lease, or resumes an
/// interrupted deletion of the same durable branch identity. The reservation
/// is intentionally published before physical cleanup so new branch admission
/// can reject the target after a crash or lost response.
///
/// Revision CAS applies to the first `Ready -> Deleting` transition. For an
/// already-`Deleting` or `Deleted` UUID, return its current outcome without
/// another mutation: a lost response leaves the caller's original revision
/// stale. The immutable UUID, rather than a reusable name, binds that replay.
pub fn begin_delete_file(
    path: &Path,
    request: DeleteRequest,
) -> Result<DeleteBeginOutcome, CatalogFileTransitionError> {
    mutate_catalog_file(path, |catalog| {
        let branch = catalog
            .branches
            .iter()
            .find(|branch| branch.id == request.id)
            .cloned()
            .ok_or(CatalogTransitionError::MissingBranch)?;
        match branch.state {
            BranchState::Ready => {
                catalog.begin_delete(request.id, request.expected_metadata_revision)?;
                let metadata_revision = catalog
                    .branches
                    .iter()
                    .find(|branch| branch.id == request.id)
                    .map(|branch| branch.metadata_revision)
                    .ok_or(CatalogTransitionError::MissingBranch)?;
                Ok(CatalogMutation {
                    value: DeleteBeginOutcome::Deleting(DeleteReservation {
                        id: request.id,
                        metadata_revision,
                    }),
                    changed: true,
                })
            }
            BranchState::Deleting => Ok(CatalogMutation {
                value: DeleteBeginOutcome::Deleting(DeleteReservation {
                    id: branch.id,
                    metadata_revision: branch.metadata_revision,
                }),
                changed: false,
            }),
            BranchState::Deleted => Ok(CatalogMutation {
                value: DeleteBeginOutcome::Deleted(branch),
                changed: false,
            }),
            BranchState::Creating => Err(CatalogTransitionError::InvalidState(
                "branch creation is not complete",
            )),
        }
    })
}

/// Publishes `Deleting -> Deleted` under one metadata lease. Retrying after a
/// completed publication returns the same durable tombstone without creating a
/// new branch identity.
/// The reservation revision is checked while changing `Deleting` to `Deleted`;
/// after publication, the same UUID's tombstone is an idempotent read even
/// though completing that transition advanced its revision.
pub fn finish_delete_file(
    path: &Path,
    reservation: DeleteReservation,
) -> Result<BranchRecord, CatalogFileTransitionError> {
    mutate_catalog_file(path, |catalog| {
        let branch = catalog
            .branches
            .iter()
            .find(|branch| branch.id == reservation.id)
            .cloned()
            .ok_or(CatalogTransitionError::MissingBranch)?;
        match branch.state {
            BranchState::Deleting => {
                catalog.finish_delete(reservation.id, reservation.metadata_revision)?;
                let branch = catalog
                    .branches
                    .iter()
                    .find(|branch| branch.id == reservation.id)
                    .cloned()
                    .ok_or(CatalogTransitionError::MissingBranch)?;
                Ok(CatalogMutation {
                    value: branch,
                    changed: true,
                })
            }
            BranchState::Deleted => Ok(CatalogMutation {
                value: branch,
                changed: false,
            }),
            _ => Err(CatalogTransitionError::InvalidState(
                "delete finalization requires a deleting branch",
            )),
        }
    })
}

#[derive(Debug)]
pub enum BranchCreateError {
    Catalog(CatalogFileTransitionError),
    Head(BranchHeadError),
    Lease(DatabaseDirectoryLeaseError),
    InconsistentRequest(&'static str),
}

impl Display for BranchCreateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => Display::fmt(error, formatter),
            Self::Head(error) => Display::fmt(error, formatter),
            Self::Lease(error) => Display::fmt(error, formatter),
            Self::InconsistentRequest(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for BranchCreateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Catalog(error) => Some(error),
            Self::Head(error) => Some(error),
            Self::Lease(error) => Some(error),
            Self::InconsistentRequest(_) => None,
        }
    }
}

/// Acquires the child lease, reserves its catalog record, creates the isolated
/// head/WAL, and completes the record only after both files are durable.
/// Known child-file failures abort the reservation. Failed abort/completion
/// publication preserves the receipt for explicit recovery or request retry.
pub fn create_branch_from_parent(
    catalog_path: &Path,
    parent_head_path: &Path,
    child_head_request: ChildBranchHeadRequest,
    expected_parent: ChildBranchSourceExpectation,
    max_active_wal_bytes: u64,
    request: CreateRequest,
) -> Result<BranchCreateResult, BranchCreateError> {
    if request.id.as_uuid().as_bytes() != &child_head_request.branch_id {
        return Err(BranchCreateError::InconsistentRequest(
            "catalog and child head branch IDs differ",
        ));
    }
    if request.base_root_digest != *child_head_request.sealed_root.sha256.as_bytes() {
        return Err(BranchCreateError::InconsistentRequest(
            "catalog and child head sealed-root digests differ",
        ));
    }
    if request.parent_id.as_uuid().as_bytes() != &expected_parent.branch_id {
        return Err(BranchCreateError::InconsistentRequest(
            "catalog parent and selected parent head differ",
        ));
    }
    if request.source_commit_epoch != expected_parent.logical_commit_epoch
        || request.source_commit_epoch != child_head_request.logical_commit_epoch
        || child_head_request.sealed_root != expected_parent.sealed_root
    {
        return Err(BranchCreateError::InconsistentRequest(
            "catalog source revision and selected parent head differ",
        ));
    }
    let child_head_path = child_head_request.head_path.clone();
    let child_directory = child_head_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    // Own the child before its pending receipt is visible. Otherwise a retry
    // can acquire the lease and abort the still-active creator's reservation.
    // The directory/lock are coordination state; no head/WAL exists yet.
    fs::create_dir_all(child_directory).map_err(|source| {
        BranchCreateError::Head(BranchHeadError::Io {
            operation: "create child branch directory",
            source,
        })
    })?;
    let lease =
        DatabaseDirectoryLease::acquire(child_directory).map_err(BranchCreateError::Lease)?;
    let reservation = reserve_create_file_with_source(catalog_path, request, |catalog| {
        let parent = crate::branch_head::read_branch_head(parent_head_path)
            .map_err(|error| CatalogFileTransitionError::Io(invalid_data(error.to_string())))?;
        if parent.project_id != *catalog.project_id.as_uuid().as_bytes()
            || parent.branch_id != expected_parent.branch_id
            || parent.physical_generation != expected_parent.physical_generation
            || parent.logical_commit_epoch != expected_parent.logical_commit_epoch
            || parent.sealed_root != expected_parent.sealed_root
        {
            return Err(CatalogFileTransitionError::Transition(
                CatalogTransitionError::Conflict("create source revision is stale"),
            ));
        }
        Ok(())
    })
    .map_err(BranchCreateError::Catalog)?;
    if reservation.replayed {
        if reservation.id.as_uuid().as_bytes() != &child_head_request.branch_id {
            return Err(BranchCreateError::InconsistentRequest(
                "replayed branch identity does not match the requested child paths",
            ));
        }
        let catalog = read_catalog(catalog_path)
            .map_err(|error| BranchCreateError::Catalog(CatalogFileTransitionError::Io(error)))?;
        let branch = catalog
            .branches
            .iter()
            .find(|branch| branch.id == reservation.id)
            .ok_or(BranchCreateError::InconsistentRequest(
                "replayed branch is missing",
            ))?;
        match (branch.state, branch.create_outcome) {
            (BranchState::Creating, CreateOutcome::Pending) => {
                if recover_create_file(
                    catalog_path,
                    reservation.id,
                    &child_head_path,
                    &child_head_request.wal_path,
                    max_active_wal_bytes,
                )? == CreateRecoveryOutcome::Aborted
                {
                    return Err(BranchCreateError::InconsistentRequest(
                        "replayed branch creation was aborted",
                    ));
                }
            }
            (BranchState::Ready, CreateOutcome::Succeeded) => {}
            _ => {
                return Err(BranchCreateError::InconsistentRequest(
                    "replayed branch is not ready",
                ))
            }
        }
        let head = crate::branch_head::read_branch_head(&child_head_path)
            .map_err(BranchCreateError::Head)?;
        if head.project_id != *catalog.project_id.as_uuid().as_bytes()
            || head.branch_id != *reservation.id.as_uuid().as_bytes()
        {
            return Err(BranchCreateError::InconsistentRequest(
                "replayed branch head identity mismatch",
            ));
        }
        let wal = crate::branch_head::active_wal_identity_from_file(
            &child_head_request.wal_path,
            head.active_wal.generation,
            head.active_wal.replay_start_lsn,
            max_active_wal_bytes,
        )
        .map_err(BranchCreateError::Head)?;
        if wal != head.active_wal {
            return Err(BranchCreateError::Head(BranchHeadError::InvalidWalIdentity));
        }
        return Ok(BranchCreateResult { head, lease });
    }
    match create_child_branch_head_from_parent(
        parent_head_path,
        &child_head_path,
        child_head_request,
        expected_parent,
        max_active_wal_bytes,
    ) {
        Ok(head) => {
            complete_create_file(catalog_path, reservation).map_err(BranchCreateError::Catalog)?;
            Ok(BranchCreateResult { head, lease })
        }
        Err(error) => {
            abort_create_file(catalog_path, reservation).map_err(BranchCreateError::Catalog)?;
            Err(BranchCreateError::Head(error))
        }
    }
}

#[derive(Debug)]
pub struct BranchCreateResult {
    pub head: BranchHead,
    pub lease: DatabaseDirectoryLease,
}

/// Filesystem locations needed to bind one catalog record to its durable head.
/// Paths are supplied by the branch owner; the catalog never derives paths
/// from human names.
#[derive(Debug, Clone)]
pub struct BranchReclamationPath {
    pub id: BranchId,
    pub directory: PathBuf,
    pub head_path: PathBuf,
}

#[derive(Debug)]
pub enum BranchReclamationError {
    Catalog(io::Error),
    MissingPath(BranchId),
    Head(BranchHeadError),
    Lease(io::Error),
    Objects(ImmutableObjectError),
}

impl Display for BranchReclamationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => {
                write!(formatter, "read branch catalog for reclamation: {error}")
            }
            Self::MissingPath(id) => write!(
                formatter,
                "missing reclamation path for branch {}",
                id.as_uuid()
            ),
            Self::Head(error) => write!(formatter, "read branch head for reclamation: {error}"),
            Self::Lease(error) => {
                write!(formatter, "inspect branch lease for reclamation: {error}")
            }
            Self::Objects(error) => write!(formatter, "reclaim branch objects: {error}"),
        }
    }
}

impl std::error::Error for BranchReclamationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Catalog(error) | Self::Lease(error) => Some(error),
            Self::Head(error) => Some(error),
            Self::Objects(error) => Some(error),
            Self::MissingPath(_) => None,
        }
    }
}

/// Builds a conservative branch-aware sweep from one durable catalog. A
/// catalog read, head read, or lease inspection failure prevents reclamation.
pub fn reclaim_catalog_branches(
    catalog_path: &Path,
    object_store: &mut ImmutableObjectStore,
    objects: &[ObjectReference],
    paths: &[BranchReclamationPath],
) -> Result<ReclamationReport, BranchReclamationError> {
    let project_directory = catalog_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| BranchReclamationError::Catalog(invalid_data("catalog has no parent")))?;
    // Admission validates metadata before exposing a runtime. Holding this
    // lease through sweep prevents a new owner from publishing candidates
    // after the lease inventory was inspected.
    let _metadata_lease = CatalogMetadataLease::acquire_blocking(project_directory)
        .map_err(BranchReclamationError::Catalog)?;
    let catalog = read_catalog(catalog_path).map_err(BranchReclamationError::Catalog)?;
    let mut branches = Vec::with_capacity(catalog.branches.len());
    for record in &catalog.branches {
        let path = paths
            .iter()
            .find(|candidate| candidate.id == record.id)
            .ok_or(BranchReclamationError::MissingPath(record.id))?;
        let active_lease =
            if matches!(record.state, BranchState::Deleted) && !path.directory.exists() {
                false
            } else {
                match DatabaseDirectoryLease::acquire(&path.directory) {
                    Ok(lease) => {
                        drop(lease);
                        false
                    }
                    Err(DatabaseDirectoryLeaseError::AlreadyOpen) => true,
                    Err(DatabaseDirectoryLeaseError::Canonicalize(error))
                    | Err(DatabaseDirectoryLeaseError::OpenLockFile(error))
                    | Err(DatabaseDirectoryLeaseError::Lock(error)) => {
                        return Err(BranchReclamationError::Lease(error));
                    }
                }
            };
        let sealed_root = if matches!(record.state, BranchState::Deleted) {
            None
        } else {
            Some(
                read_branch_head(&path.head_path)
                    .map_err(BranchReclamationError::Head)?
                    .sealed_root,
            )
        };
        branches.push(BranchReclamationEntry {
            state: record.state,
            sealed_root,
            directory: path.directory.clone(),
            active_lease,
        });
    }
    object_store
        .reclaim_branches(&BranchReclamationInventory {
            objects: objects.to_vec(),
            branches,
        })
        .map_err(BranchReclamationError::Objects)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateRecoveryOutcome {
    Completed,
    Aborted,
}

/// Recovers a pending child creation after process interruption.  A complete
/// and valid child head/WAL pair is promoted to `Ready`; a known absent pair
/// is aborted.  Any other filesystem or integrity error leaves `Creating`
/// untouched so a later open can retry conservatively.
pub fn recover_create_file(
    catalog_path: &Path,
    branch_id: BranchId,
    child_head_path: &Path,
    child_wal_path: &Path,
    max_active_wal_bytes: u64,
) -> Result<CreateRecoveryOutcome, BranchCreateError> {
    recover_create_file_inner(
        catalog_path,
        branch_id,
        child_head_path,
        Some(child_wal_path),
        max_active_wal_bytes,
    )
}

/// Resumes a reserved child without consulting its parent's current state.
/// The child's UUID lease protects validation and catalog completion; the WAL
/// identity comes from its durable head, never a later parent generation.
pub fn recover_create_from_head_file(
    catalog_path: &Path,
    branch_id: BranchId,
    child_head_path: &Path,
    max_active_wal_bytes: u64,
) -> Result<CreateRecoveryOutcome, BranchCreateError> {
    let directory = child_head_path
        .parent()
        .ok_or(BranchCreateError::InconsistentRequest(
            "child head has no branch directory",
        ))?;
    fs::create_dir_all(directory).map_err(|source| {
        BranchCreateError::Head(BranchHeadError::Io {
            operation: "create pending child lease directory",
            source,
        })
    })?;
    let _lease = DatabaseDirectoryLease::acquire(directory).map_err(BranchCreateError::Lease)?;
    recover_create_file_inner(
        catalog_path,
        branch_id,
        child_head_path,
        None,
        max_active_wal_bytes,
    )
}

fn recover_create_file_inner(
    catalog_path: &Path,
    branch_id: BranchId,
    child_head_path: &Path,
    child_wal_path: Option<&Path>,
    max_active_wal_bytes: u64,
) -> Result<CreateRecoveryOutcome, BranchCreateError> {
    let catalog = read_catalog(catalog_path)
        .map_err(|error| BranchCreateError::Catalog(CatalogFileTransitionError::Io(error)))?;
    let branch = catalog
        .branches
        .iter()
        .find(|branch| branch.id == branch_id)
        .ok_or(BranchCreateError::Catalog(
            CatalogFileTransitionError::Transition(CatalogTransitionError::MissingBranch),
        ))?;
    if branch.state != BranchState::Creating || branch.create_outcome != CreateOutcome::Pending {
        return Err(BranchCreateError::Catalog(
            CatalogFileTransitionError::Transition(CatalogTransitionError::InvalidState(
                "recovery requires a pending child create",
            )),
        ));
    }
    let reservation = CreateReservation {
        id: branch_id,
        metadata_revision: branch.metadata_revision,
        replayed: false,
    };
    let head = match crate::branch_head::read_branch_head(child_head_path) {
        Ok(head) => head,
        Err(BranchHeadError::Io { source, .. }) if source.kind() == io::ErrorKind::NotFound => {
            abort_create_file(catalog_path, reservation).map_err(BranchCreateError::Catalog)?;
            return Ok(CreateRecoveryOutcome::Aborted);
        }
        Err(error) => return Err(BranchCreateError::Head(error)),
    };
    if head.project_id != *catalog.project_id.as_uuid().as_bytes()
        || head.branch_id != *branch_id.as_uuid().as_bytes()
        || branch.base_root_digest != Some(*head.sealed_root.sha256.as_bytes())
        || branch.source_commit_epoch != head.logical_commit_epoch
    {
        return Err(BranchCreateError::InconsistentRequest(
            "pending child metadata does not match its head",
        ));
    }
    let derived_wal_path = child_head_path.with_file_name(
        crate::artifact_files::wal_generation_file(head.active_wal.generation),
    );
    let child_wal_path = child_wal_path.unwrap_or(&derived_wal_path);
    match fs::metadata(child_wal_path) {
        Ok(_) => {}
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            abort_create_file(catalog_path, reservation).map_err(BranchCreateError::Catalog)?;
            return Ok(CreateRecoveryOutcome::Aborted);
        }
        Err(source) => {
            return Err(BranchCreateError::Head(BranchHeadError::Io {
                operation: "read pending child WAL metadata",
                source,
            }));
        }
    }
    let wal = crate::branch_head::active_wal_identity_from_file(
        child_wal_path,
        head.active_wal.generation,
        head.active_wal.replay_start_lsn,
        max_active_wal_bytes,
    )
    .map_err(BranchCreateError::Head)?;
    if wal != head.active_wal {
        return Err(BranchCreateError::Head(BranchHeadError::InvalidWalIdentity));
    }
    complete_create_file(catalog_path, reservation).map_err(BranchCreateError::Catalog)?;
    Ok(CreateRecoveryOutcome::Completed)
}

/// Stable project metadata lock.  It is separate from branch writer leases so
/// independent branch handles can write their own WALs while catalog updates
/// remain serialized.
#[derive(Debug)]
pub struct CatalogMetadataLease {
    file: File,
}

impl CatalogMetadataLease {
    pub fn acquire(project_directory: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(project_directory.join(METADATA_LOCK_FILE))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { file }),
            Err(TryLockError::WouldBlock) => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "branch catalog metadata lock is held",
            )),
            Err(TryLockError::Error(error)) => Err(error),
        }
    }

    pub fn acquire_blocking(project_directory: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(project_directory.join(METADATA_LOCK_FILE))?;
        file.lock()?;
        Ok(Self { file })
    }
}

impl Drop for CatalogMetadataLease {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn validate_name(value: &str) -> Result<(), CatalogError> {
    validate_bounded_string(value, MAX_NAME_BYTES, "branch name")?;
    if value == "main" || value.starts_with("agent/") {
        return Err(CatalogError::ReservedName);
    }
    validate_catalog_name(value)
}

fn validate_catalog_name(value: &str) -> Result<(), CatalogError> {
    validate_bounded_string(value, MAX_NAME_BYTES, "branch name")?;
    if value.starts_with('/') || value.ends_with('/') || value.contains('\\') {
        return Err(CatalogError::InvalidName);
    }
    for component in value.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(CatalogError::InvalidName);
        }
        if !component
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(CatalogError::InvalidName);
        }
        if component.bytes().any(|byte| byte.is_ascii_whitespace()) {
            return Err(CatalogError::InvalidName);
        }
    }
    Ok(())
}

fn validate_bounded_string(
    value: &str,
    maximum: usize,
    field: &'static str,
) -> Result<(), CatalogError> {
    if value.is_empty() || value.len() > maximum || !value.is_ascii() {
        return Err(CatalogError::Limit(field));
    }
    if value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(CatalogError::InvalidName);
    }
    Ok(())
}

fn encode_branch(output: &mut Vec<u8>, branch: &BranchRecord) -> Result<(), CatalogError> {
    output.extend_from_slice(branch.id.as_uuid().as_bytes());
    put_string(output, branch.name.as_str(), MAX_NAME_BYTES, "branch name")?;
    put_optional_uuid(output, branch.parent_id);
    put_u64(output, branch.source_commit_epoch);
    put_optional_bytes(output, branch.base_root_digest);
    put_u64(output, branch.metadata_revision);
    output.push(state_byte(branch.state));
    put_optional_string(output, branch.owner.as_deref(), MAX_OWNER_BYTES, "owner")?;
    put_string(
        output,
        &branch.create_request_key,
        MAX_REQUEST_KEY_BYTES,
        "create request key",
    )?;
    output.extend_from_slice(&branch.request_fingerprint);
    output.push(outcome_byte(branch.create_outcome));
    Ok(())
}

fn decode_branch(reader: &mut Reader<'_>) -> Result<BranchRecord, CatalogError> {
    let id = BranchId::new(Uuid::from_bytes(reader.array()?))?;
    let name = BranchName::from_encoded(reader.string(MAX_NAME_BYTES, "branch name")?)?;
    let parent_id = reader.optional_uuid()?;
    let source_commit_epoch = reader.u64()?;
    let base_root_digest = reader.optional_array()?;
    let metadata_revision = reader.u64()?;
    let state = parse_state(reader.byte()?)?;
    let owner = reader.optional_string(MAX_OWNER_BYTES, "owner")?;
    let create_request_key = reader.string(MAX_REQUEST_KEY_BYTES, "create request key")?;
    let request_fingerprint = reader.array()?;
    let create_outcome = parse_outcome(reader.byte()?)?;
    Ok(BranchRecord {
        id,
        name,
        parent_id,
        source_commit_epoch,
        base_root_digest,
        metadata_revision,
        state,
        owner,
        create_request_key,
        request_fingerprint,
        create_outcome,
    })
}

fn state_byte(value: BranchState) -> u8 {
    match value {
        BranchState::Creating => 0,
        BranchState::Ready => 1,
        BranchState::Deleting => 3,
        BranchState::Deleted => 4,
    }
}

fn parse_state(value: u8) -> Result<BranchState, CatalogError> {
    match value {
        0 => Ok(BranchState::Creating),
        1 => Ok(BranchState::Ready),
        3 => Ok(BranchState::Deleting),
        4 => Ok(BranchState::Deleted),
        _ => Err(CatalogError::InvalidEnum("branch state")),
    }
}

fn outcome_byte(value: CreateOutcome) -> u8 {
    match value {
        CreateOutcome::Pending => 0,
        CreateOutcome::Succeeded => 1,
        CreateOutcome::Aborted => 2,
    }
}

fn parse_outcome(value: u8) -> Result<CreateOutcome, CatalogError> {
    match value {
        0 => Ok(CreateOutcome::Pending),
        1 => Ok(CreateOutcome::Succeeded),
        2 => Ok(CreateOutcome::Aborted),
        _ => Err(CatalogError::InvalidEnum("create outcome")),
    }
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_string(
    output: &mut Vec<u8>,
    value: &str,
    maximum: usize,
    field: &'static str,
) -> Result<(), CatalogError> {
    validate_bounded_string(value, maximum, field)?;
    let length = u16::try_from(value.len()).map_err(|_| CatalogError::Limit(field))?;
    put_u16(output, length);
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_optional_string(
    output: &mut Vec<u8>,
    value: Option<&str>,
    maximum: usize,
    field: &'static str,
) -> Result<(), CatalogError> {
    match value {
        Some(value) => {
            output.push(1);
            put_string(output, value, maximum, field)?;
        }
        None => output.push(0),
    }
    Ok(())
}

fn put_optional_uuid(output: &mut Vec<u8>, value: Option<BranchId>) {
    match value {
        Some(value) => {
            output.push(1);
            output.extend_from_slice(value.as_uuid().as_bytes());
        }
        None => output.push(0),
    }
}

fn put_optional_bytes(output: &mut Vec<u8>, value: Option<[u8; DIGEST_BYTES]>) {
    match value {
        Some(value) => {
            output.push(1);
            output.extend_from_slice(&value);
        }
        None => output.push(0),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], CatalogError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(CatalogError::Truncated)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(CatalogError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, CatalogError> {
        Ok(*self.take(1)?.first().ok_or(CatalogError::Truncated)?)
    }

    fn u16(&mut self) -> Result<u16, CatalogError> {
        Ok(u16::from_le_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| CatalogError::Truncated)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, CatalogError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| CatalogError::Truncated)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, CatalogError> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| CatalogError::Truncated)?,
        ))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CatalogError> {
        self.take(N)?
            .try_into()
            .map_err(|_| CatalogError::Truncated)
    }

    fn optional_uuid(&mut self) -> Result<Option<BranchId>, CatalogError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(BranchId::new(Uuid::from_bytes(self.array()?))?)),
            _ => Err(CatalogError::InvalidEnum("optional UUID marker")),
        }
    }

    fn optional_array(&mut self) -> Result<Option<[u8; DIGEST_BYTES]>, CatalogError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.array()?)),
            _ => Err(CatalogError::InvalidEnum("optional digest marker")),
        }
    }

    fn string(&mut self, maximum: usize, field: &'static str) -> Result<String, CatalogError> {
        let length = self.u16()? as usize;
        if length == 0 || length > maximum {
            return Err(CatalogError::Limit(field));
        }
        let value = std::str::from_utf8(self.take(length)?)
            .map_err(|_| CatalogError::InvalidUtf8(field))?;
        validate_bounded_string(value, maximum, field)?;
        Ok(value.to_owned())
    }

    fn optional_string(
        &mut self,
        maximum: usize,
        field: &'static str,
    ) -> Result<Option<String>, CatalogError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.string(maximum, field)?)),
            _ => Err(CatalogError::InvalidEnum("optional string marker")),
        }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    Checksum,
    Duplicate(&'static str),
    InvalidEnum(&'static str),
    InvalidIdentity(&'static str),
    InvalidName,
    InvalidState(&'static str),
    InvalidUtf8(&'static str),
    Limit(&'static str),
    ReservedName,
    TrailingBytes,
    Truncated,
    Version,
}

impl Display for CatalogError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Checksum => formatter.write_str("branch catalog checksum mismatch"),
            Self::Duplicate(field) => write!(formatter, "duplicate branch catalog {field}"),
            Self::InvalidEnum(field) => write!(formatter, "invalid branch catalog {field}"),
            Self::InvalidIdentity(message) => formatter.write_str(message),
            Self::InvalidName => formatter.write_str("invalid branch catalog name"),
            Self::InvalidState(message) => formatter.write_str(message),
            Self::InvalidUtf8(field) => write!(formatter, "branch catalog {field} is not UTF-8"),
            Self::Limit(field) => write!(formatter, "branch catalog {field} exceeds its limit"),
            Self::ReservedName => formatter.write_str("branch catalog name is reserved"),
            Self::TrailingBytes => formatter.write_str("branch catalog has trailing bytes"),
            Self::Truncated => formatter.write_str("branch catalog is truncated"),
            Self::Version => formatter.write_str("unsupported branch catalog version"),
        }
    }
}

impl std::error::Error for CatalogError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::immutable_object::ObjectKind;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;

    static DIRECTORY_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

    fn id(byte: u8) -> BranchId {
        BranchId::new(Uuid::from_bytes([byte; 16])).unwrap()
    }

    fn record(byte: u8, name: &str) -> BranchRecord {
        BranchRecord {
            id: id(byte),
            name: BranchName::new(name).unwrap(),
            parent_id: None,
            source_commit_epoch: 7,
            base_root_digest: Some([byte; DIGEST_BYTES]),
            metadata_revision: 3,
            state: BranchState::Ready,
            owner: Some("agent-1".to_string()),
            create_request_key: format!("request-{byte}"),
            request_fingerprint: [byte.wrapping_add(1); DIGEST_BYTES],
            create_outcome: CreateOutcome::Succeeded,
        }
    }

    fn catalog() -> Catalog {
        Catalog {
            project_id: id(99),
            revision: 11,
            branches: vec![record(2, "second"), record(1, "first")],
        }
    }

    fn create_request() -> CreateRequest {
        CreateRequest {
            id: id(3),
            name: BranchName::new("third").unwrap(),
            parent_id: id(1),
            source_commit_epoch: 7,
            base_root_digest: [3; DIGEST_BYTES],
            owner: Some("worker-1".to_string()),
            request_key: "create-third".to_string(),
            request_fingerprint: [9; DIGEST_BYTES],
        }
    }

    fn temporary_catalog_path() -> (PathBuf, PathBuf) {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-branch-catalog-{}-{}",
            std::process::id(),
            DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("catalog.hawdb");
        (directory, path)
    }

    #[test]
    fn names_enforce_the_catalog_path_independent_contract() {
        for value in [
            "", "main", "agent/x", "/x", "x/", "x//y", "x/../y", "x/y\\z", "x y",
        ] {
            assert!(BranchName::new(value).is_err(), "accepted {value:?}");
        }
        for value in ["Foo", "foo", "agentx", "a/b.c_2"] {
            assert!(BranchName::new(value).is_ok(), "rejected {value:?}");
        }
        assert!(BranchName::new("agent/generated").is_err());
    }

    #[test]
    fn catalog_codec_accepts_reserved_engine_names_only_in_catalog_records() {
        let mut main = record(1, "ordinary-main");
        main.name = BranchName::from_encoded("main".to_string()).unwrap();
        main.parent_id = None;
        let mut generated = record(2, "generated");
        generated.name =
            BranchName::from_encoded(format!("agent/{}", generated.id.as_uuid())).unwrap();
        generated.parent_id = Some(main.id);
        let catalog = Catalog {
            project_id: id(99),
            revision: 1,
            branches: vec![main, generated],
        };
        let encoded = catalog.encode().unwrap();
        assert_eq!(Catalog::decode(&encoded).unwrap().branches.len(), 2);
    }

    #[test]
    fn codec_is_deterministic_and_round_trips_unsorted_records() {
        let catalog = catalog();
        let mut reversed = catalog.clone();
        reversed.branches.reverse();
        let encoded = catalog.encode().unwrap();
        assert_eq!(encoded, reversed.encode().unwrap());
        let mut expected = catalog.clone();
        expected.branches.sort_by_key(|branch| branch.id);
        assert_eq!(Catalog::decode(&encoded).unwrap(), expected);
    }

    #[test]
    fn file_publication_syncs_and_reopens_the_canonical_catalog() {
        let (directory, path) = temporary_catalog_path();
        let catalog = catalog();
        write_catalog(&path, &catalog).unwrap();
        assert_eq!(read_catalog(&path).unwrap().revision, catalog.revision);
        assert!(path.is_file());
        assert!(directory.join(METADATA_LOCK_FILE).is_file());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn file_create_reservation_and_completion_are_restart_visible() {
        let (directory, path) = temporary_catalog_path();
        write_catalog(&path, &catalog()).unwrap();
        let reservation = reserve_create_file(&path, create_request()).unwrap();
        let pending = read_catalog(&path).unwrap();
        assert_eq!(pending.revision, 12);
        assert_eq!(pending.branches.len(), 3);
        assert_eq!(
            pending
                .branches
                .iter()
                .find(|branch| branch.id == reservation.id)
                .unwrap()
                .state,
            BranchState::Creating
        );
        complete_create_file(&path, reservation).unwrap();
        let ready = read_catalog(&path).unwrap();
        assert_eq!(
            ready
                .branches
                .iter()
                .find(|branch| branch.id == reservation.id)
                .unwrap()
                .state,
            BranchState::Ready
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn delete_transition_is_published_resumable_and_idempotent() {
        let (directory, path) = temporary_catalog_path();
        write_catalog(&path, &catalog()).unwrap();
        let request = DeleteRequest {
            id: id(1),
            expected_metadata_revision: 3,
        };

        let reservation = match begin_delete_file(&path, request).unwrap() {
            DeleteBeginOutcome::Deleting(reservation) => reservation,
            DeleteBeginOutcome::Deleted(_) => panic!("ready branch must begin deletion"),
        };
        assert_eq!(reservation.metadata_revision, 4);
        let deleting = read_catalog(&path).unwrap();
        let branch = deleting
            .branches
            .iter()
            .find(|branch| branch.id == request.id)
            .unwrap();
        assert_eq!(branch.state, BranchState::Deleting);
        assert_eq!(branch.metadata_revision, reservation.metadata_revision);

        assert_eq!(
            begin_delete_file(&path, request).unwrap(),
            DeleteBeginOutcome::Deleting(reservation)
        );
        let deleted = finish_delete_file(&path, reservation).unwrap();
        assert_eq!(deleted.state, BranchState::Deleted);
        assert_eq!(deleted.metadata_revision, 5);
        assert_eq!(finish_delete_file(&path, reservation).unwrap(), deleted);
        assert_eq!(
            begin_delete_file(&path, request).unwrap(),
            DeleteBeginOutcome::Deleted(deleted)
        );

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn delete_revision_cas_applies_before_transition_and_replays_keep_uuid_identity() {
        let (directory, path) = temporary_catalog_path();
        let mut initial = catalog();
        initial
            .rename(id(1), 3, BranchName::new("reusable").unwrap())
            .unwrap();
        write_catalog(&path, &initial).unwrap();
        let before = fs::read(&path).unwrap();
        let request = DeleteRequest {
            id: id(1),
            expected_metadata_revision: 3,
        };
        assert!(matches!(
            begin_delete_file(&path, request),
            Err(CatalogFileTransitionError::Transition(
                CatalogTransitionError::StaleRevision {
                    expected: 3,
                    actual: 4,
                }
            ))
        ));
        assert_eq!(fs::read(&path).unwrap(), before);
        let reservation = match begin_delete_file(
            &path,
            DeleteRequest {
                expected_metadata_revision: 4,
                ..request
            },
        )
        .unwrap()
        {
            DeleteBeginOutcome::Deleting(reservation) => reservation,
            DeleteBeginOutcome::Deleted(_) => panic!("ready branch must begin deletion"),
        };
        assert!(matches!(
            finish_delete_file(
                &path,
                DeleteReservation {
                    metadata_revision: 4,
                    ..reservation
                }
            ),
            Err(CatalogFileTransitionError::Transition(
                CatalogTransitionError::StaleRevision {
                    expected: 4,
                    actual: 5,
                }
            ))
        ));
        let deleted = finish_delete_file(&path, reservation).unwrap();
        let mut reused = read_catalog(&path).unwrap();
        reused.branches.push(record(9, "reusable"));
        write_catalog(&path, &reused).unwrap();
        let before_replay = fs::read(&path).unwrap();
        assert_eq!(
            begin_delete_file(&path, request).unwrap(),
            DeleteBeginOutcome::Deleted(deleted.clone())
        );
        assert_eq!(finish_delete_file(&path, reservation).unwrap(), deleted);
        assert_eq!(fs::read(&path).unwrap(), before_replay);
        assert_eq!(
            read_catalog(&path)
                .unwrap()
                .branches
                .iter()
                .find(|branch| branch.id == id(9))
                .unwrap()
                .state,
            BranchState::Ready
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn concurrent_create_and_delete_keep_both_catalog_transitions() {
        let (directory, path) = temporary_catalog_path();
        write_catalog(&path, &catalog()).unwrap();
        let start = std::sync::Arc::new(std::sync::Barrier::new(3));

        let delete_path = path.clone();
        let delete_start = std::sync::Arc::clone(&start);
        let delete = std::thread::spawn(move || {
            delete_start.wait();
            begin_delete_file(
                &delete_path,
                DeleteRequest {
                    id: id(1),
                    expected_metadata_revision: 3,
                },
            )
        });

        let create_path = path.clone();
        let create_start = std::sync::Arc::clone(&start);
        let create = std::thread::spawn(move || {
            create_start.wait();
            reserve_create_file(
                &create_path,
                CreateRequest {
                    id: id(3),
                    name: BranchName::new("third").unwrap(),
                    parent_id: id(2),
                    source_commit_epoch: 7,
                    base_root_digest: [3; DIGEST_BYTES],
                    owner: None,
                    request_key: "concurrent-create".to_string(),
                    request_fingerprint: [3; DIGEST_BYTES],
                },
            )
        });

        start.wait();
        let reservation = match delete.join().unwrap().unwrap() {
            DeleteBeginOutcome::Deleting(reservation) => reservation,
            DeleteBeginOutcome::Deleted(_) => panic!("branch must begin deletion once"),
        };
        let created = create.join().unwrap().unwrap();
        let published = read_catalog(&path).unwrap();
        assert_eq!(published.revision, 13);
        assert_eq!(
            published
                .branches
                .iter()
                .find(|branch| branch.id == id(1))
                .unwrap()
                .state,
            BranchState::Deleting
        );
        assert_eq!(
            published
                .branches
                .iter()
                .find(|branch| branch.id == created.id)
                .unwrap()
                .state,
            BranchState::Creating
        );

        let deleted = finish_delete_file(&path, reservation).unwrap();
        assert_eq!(deleted.state, BranchState::Deleted);
        assert_eq!(read_catalog(&path).unwrap().revision, 14);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn concurrent_create_reservations_serialize_the_read_modify_publish_cycle() {
        let (directory, path) = temporary_catalog_path();
        write_catalog(&path, &catalog()).unwrap();
        let first_path = path.clone();
        let first =
            std::thread::spawn(move || reserve_with_retry(&first_path, create_request_for(3)));
        let second_path = path.clone();
        let second =
            std::thread::spawn(move || reserve_with_retry(&second_path, create_request_for(4)));
        let first = first.join().unwrap().unwrap();
        let second = second.join().unwrap().unwrap();
        assert_ne!(first.id, second.id);
        let published = read_catalog(&path).unwrap();
        assert!(published
            .branches
            .iter()
            .any(|branch| branch.id == first.id));
        assert!(published
            .branches
            .iter()
            .any(|branch| branch.id == second.id));
        assert_eq!(published.revision, 13);
        fs::remove_dir_all(directory).unwrap();
    }

    fn create_request_for(byte: u8) -> CreateRequest {
        CreateRequest {
            id: id(byte),
            name: BranchName::new(format!("branch-{byte}")).unwrap(),
            parent_id: id(1),
            source_commit_epoch: 7,
            base_root_digest: [byte; DIGEST_BYTES],
            owner: Some("worker-1".to_string()),
            request_key: format!("create-{byte}"),
            request_fingerprint: [byte.wrapping_add(10); DIGEST_BYTES],
        }
    }

    fn reserve_with_retry(
        path: &Path,
        request: CreateRequest,
    ) -> Result<CreateReservation, CatalogFileTransitionError> {
        for _ in 0..100 {
            match reserve_create_file(path, request.clone()) {
                Err(CatalogFileTransitionError::Io(error))
                    if error.kind() == io::ErrorKind::WouldBlock =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                result => return result,
            }
        }
        panic!("catalog metadata lock did not become available");
    }

    #[test]
    fn interrupted_create_without_child_files_is_aborted_on_recovery() {
        let (directory, path) = temporary_catalog_path();
        write_catalog(&path, &catalog()).unwrap();
        let reservation = reserve_create_file(&path, create_request()).unwrap();
        let outcome = recover_create_file(
            &path,
            reservation.id,
            &directory.join("missing.head"),
            &directory.join("missing.wal"),
            1024,
        )
        .unwrap();
        assert_eq!(outcome, CreateRecoveryOutcome::Aborted);
        let recovered = read_catalog(&path).unwrap();
        let branch = recovered
            .branches
            .iter()
            .find(|branch| branch.id == reservation.id)
            .unwrap();
        assert_eq!(branch.state, BranchState::Deleted);
        assert_eq!(branch.create_outcome, CreateOutcome::Aborted);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn recovery_completes_a_pending_create_when_files_are_already_durable() {
        let (directory, catalog_path) = temporary_catalog_path();
        write_catalog(&catalog_path, &catalog()).unwrap();
        let parent_head_path = directory.join("parent.head");
        let child_directory = directory.join("child");
        let child_head_path = child_directory.join("child.head");
        let child_wal_path = child_directory.join("child.wal");
        fs::create_dir_all(&child_directory).unwrap();
        let root = crate::immutable_object::ObjectReference::for_bytes(
            crate::immutable_object::ObjectKind::SealedRoot,
            1,
            b"parent-root",
        );
        let parent = BranchHead {
            project_id: *catalog().project_id.as_uuid().as_bytes(),
            branch_id: *id(1).as_uuid().as_bytes(),
            physical_generation: 4,
            sealed_root: root,
            logical_commit_epoch: 7,
            active_wal: crate::branch_head::ActiveWalIdentity {
                generation: 5,
                replay_start_lsn: 20,
                byte_length: 1,
                sha256: hawdb_integrity::sha256(b"x"),
            },
        };
        fs::write(&parent_head_path, parent.encode().unwrap()).unwrap();
        let mut request = create_request();
        request.base_root_digest = *root.sha256.as_bytes();
        let reservation = reserve_create_file(&catalog_path, request.clone()).unwrap();
        let child_request = ChildBranchHeadRequest {
            project_id: parent.project_id,
            branch_id: *request.id.as_uuid().as_bytes(),
            sealed_root: root,
            logical_commit_epoch: 7,
            active_wal_generation: 1,
            replay_start_lsn: 42,
            head_path: child_head_path.clone(),
            wal_path: child_wal_path.clone(),
        };
        create_child_branch_head_from_parent(
            &parent_head_path,
            &child_head_path,
            child_request,
            ChildBranchSourceExpectation {
                branch_id: parent.branch_id,
                physical_generation: 4,
                logical_commit_epoch: 7,
                sealed_root: root,
            },
            1024,
        )
        .unwrap();
        assert_eq!(
            recover_create_file(
                &catalog_path,
                reservation.id,
                &child_head_path,
                &child_wal_path,
                1024,
            )
            .unwrap(),
            CreateRecoveryOutcome::Completed
        );
        assert_eq!(
            read_catalog(&catalog_path)
                .unwrap()
                .branches
                .iter()
                .find(|branch| branch.id == reservation.id)
                .unwrap()
                .state,
            BranchState::Ready
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn recovery_keeps_creating_for_corrupt_child_head() {
        let (directory, path) = temporary_catalog_path();
        write_catalog(&path, &catalog()).unwrap();
        let reservation = reserve_create_file(&path, create_request()).unwrap();
        let head_path = directory.join("corrupt.head");
        let wal_path = directory.join("corrupt.wal");
        fs::write(&head_path, b"corrupt").unwrap();
        let error =
            recover_create_file(&path, reservation.id, &head_path, &wal_path, 1024).unwrap_err();
        assert!(matches!(error, BranchCreateError::Head(_)));
        assert_eq!(
            read_catalog(&path)
                .unwrap()
                .branches
                .iter()
                .find(|branch| branch.id == reservation.id)
                .unwrap()
                .state,
            BranchState::Creating
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[derive(Clone)]
    struct CreateFixture {
        directory: PathBuf,
        catalog_path: PathBuf,
        parent_head_path: PathBuf,
        child_request: ChildBranchHeadRequest,
        expected_parent: ChildBranchSourceExpectation,
        request: CreateRequest,
    }

    impl CreateFixture {
        fn new() -> Self {
            let (directory, catalog_path) = temporary_catalog_path();
            write_catalog(&catalog_path, &catalog()).unwrap();
            let parent_head_path = directory.join("parent.head");
            let child_directory = directory.join("child");
            let child_head_path = child_directory.join("child.head");
            let child_wal_path = child_directory.join("child.wal");
            let root = crate::immutable_object::ObjectReference::for_bytes(
                crate::immutable_object::ObjectKind::SealedRoot,
                1,
                b"parent-root",
            );
            let parent = BranchHead {
                project_id: *catalog().project_id.as_uuid().as_bytes(),
                branch_id: *id(1).as_uuid().as_bytes(),
                physical_generation: 4,
                sealed_root: root,
                logical_commit_epoch: 7,
                active_wal: crate::branch_head::ActiveWalIdentity {
                    generation: 5,
                    replay_start_lsn: 20,
                    byte_length: 1,
                    sha256: hawdb_integrity::sha256(b"x"),
                },
            };
            fs::write(&parent_head_path, parent.encode().unwrap()).unwrap();
            let mut request = create_request();
            request.base_root_digest = *root.sha256.as_bytes();
            Self {
                directory,
                catalog_path,
                parent_head_path,
                child_request: ChildBranchHeadRequest {
                    project_id: parent.project_id,
                    branch_id: *request.id.as_uuid().as_bytes(),
                    sealed_root: root,
                    logical_commit_epoch: 7,
                    active_wal_generation: 1,
                    replay_start_lsn: 42,
                    head_path: child_head_path.clone(),
                    wal_path: child_wal_path.clone(),
                },
                expected_parent: ChildBranchSourceExpectation {
                    branch_id: parent.branch_id,
                    physical_generation: 4,
                    logical_commit_epoch: 7,
                    sealed_root: root,
                },
                request,
            }
        }

        fn child_directory(&self) -> &Path {
            self.child_request.head_path.parent().unwrap()
        }

        fn create(&self) -> Result<BranchCreateResult, BranchCreateError> {
            create_branch_from_parent(
                &self.catalog_path,
                &self.parent_head_path,
                self.child_request.clone(),
                self.expected_parent,
                1024,
                self.request.clone(),
            )
        }
    }

    #[test]
    fn create_branch_from_parent_completes_catalog_after_child_files() {
        let fixture = CreateFixture::new();
        let child = fixture.create().unwrap();
        assert_eq!(child.head.sealed_root, fixture.child_request.sealed_root);
        assert_eq!(
            read_catalog(&fixture.catalog_path)
                .unwrap()
                .branches
                .last()
                .unwrap()
                .state,
            BranchState::Ready
        );
        assert!(fixture.child_request.head_path.is_file());
        assert!(fixture.child_request.wal_path.is_file());
        assert!(matches!(
            DatabaseDirectoryLease::acquire(fixture.child_directory()),
            Err(DatabaseDirectoryLeaseError::AlreadyOpen)
        ));
        let first_head = child.head;
        drop(child);
        let retried = fixture.create().unwrap();
        assert_eq!(retried.head, first_head);
        drop(retried);
        let reopened =
            crate::branch_head::read_branch_head(&fixture.child_request.head_path).unwrap();
        assert_eq!(reopened.sealed_root, fixture.child_request.sealed_root);
        assert_eq!(
            read_catalog(&fixture.catalog_path)
                .unwrap()
                .branches
                .iter()
                .find(|branch| branch.id == fixture.request.id)
                .unwrap()
                .state,
            BranchState::Ready
        );
        let reopened_lease = DatabaseDirectoryLease::acquire(fixture.child_directory()).unwrap();
        drop(reopened_lease);
        fs::remove_dir_all(fixture.directory).unwrap();
    }

    #[test]
    fn create_branch_directory_and_lease_failures_leave_no_pending_receipt() {
        let fixture = CreateFixture::new();
        let before = fs::read(&fixture.catalog_path).unwrap();
        fs::write(fixture.child_directory(), b"not a directory").unwrap();
        assert!(matches!(fixture.create(), Err(BranchCreateError::Head(_))));
        assert_eq!(fs::read(&fixture.catalog_path).unwrap(), before);
        fs::remove_file(fixture.child_directory()).unwrap();

        fs::create_dir(fixture.child_directory()).unwrap();
        let lease = DatabaseDirectoryLease::acquire(fixture.child_directory()).unwrap();
        assert!(matches!(
            fixture.create(),
            Err(BranchCreateError::Lease(
                DatabaseDirectoryLeaseError::AlreadyOpen
            ))
        ));
        assert_eq!(fs::read(&fixture.catalog_path).unwrap(), before);
        drop(lease);
        let child = fixture.create().unwrap();
        assert_eq!(
            child.head.branch_id,
            *fixture.request.id.as_uuid().as_bytes()
        );
        drop(child);
        fs::remove_dir_all(fixture.directory).unwrap();
    }

    #[test]
    fn create_branch_owns_child_before_waiting_for_catalog_reservation() {
        let fixture = CreateFixture::new();
        let before = fs::read(&fixture.catalog_path).unwrap();
        let metadata = CatalogMetadataLease::acquire(&fixture.directory).unwrap();
        let creator_fixture = fixture.clone();
        let creator = std::thread::spawn(move || creator_fixture.create());
        let child_directory = fs::canonicalize(&fixture.directory).unwrap().join("child");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let owns_child = loop {
            if crate::ownership::has_process_lease(&child_directory) {
                break true;
            }
            if creator.is_finished() || std::time::Instant::now() >= deadline {
                break false;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        };
        let waiting_catalog = fs::read(&fixture.catalog_path).unwrap();
        // Release the blocked worker even if the ordering assertion will fail.
        drop(metadata);
        let child = creator.join().unwrap().unwrap();
        assert!(
            owns_child,
            "creation must own the child before publishing a pending receipt"
        );
        assert_eq!(waiting_catalog, before);
        drop(child);
        fs::remove_dir_all(fixture.directory).unwrap();
    }

    #[test]
    fn metadata_lock_is_stable_and_serializes_catalog_writers() {
        let (directory, _path) = temporary_catalog_path();
        let first = CatalogMetadataLease::acquire(&directory).unwrap();
        assert_eq!(
            CatalogMetadataLease::acquire(&directory)
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(first);
        CatalogMetadataLease::acquire(&directory).unwrap();
        assert!(directory.join(METADATA_LOCK_FILE).is_file());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn root_initialization_and_head_binding_share_one_catalog_transition() {
        let (directory, path) = temporary_catalog_path();
        let start = std::sync::Arc::new(std::sync::Barrier::new(2));
        let first_path = path.clone();
        let first_start = std::sync::Arc::clone(&start);
        let first = std::thread::spawn(move || {
            first_start.wait();
            initialize_catalog_file(&first_path, id(90), id(91)).unwrap()
        });
        start.wait();
        let second = initialize_catalog_file(&path, id(90), id(91)).unwrap();
        let first = first.join().unwrap();
        assert_eq!(first, second);
        assert_eq!(read_catalog(&path).unwrap().branches, vec![first.clone()]);

        let bound = bind_main_head_file(&path, id(90), id(91), [7; DIGEST_BYTES], 12).unwrap();
        assert_eq!(bound.base_root_digest, Some([7; DIGEST_BYTES]));
        assert_eq!(bound.source_commit_epoch, 12);
        assert_eq!(bound.metadata_revision, first.metadata_revision + 1);
        assert_eq!(read_catalog(&path).unwrap().revision, 2);
        assert_eq!(
            bind_main_head_file(&path, id(90), id(91), [7; DIGEST_BYTES], 12).unwrap(),
            bound
        );
        assert!(matches!(
            bind_main_head_file(&path, id(90), id(91), [8; DIGEST_BYTES], 12),
            Err(CatalogFileTransitionError::Transition(
                CatalogTransitionError::Conflict(_)
            ))
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn file_publication_failure_keeps_previous_bytes_and_cleans_candidate() {
        let (directory, path) = temporary_catalog_path();
        let first = catalog();
        write_catalog(&path, &first).unwrap();
        let before = fs::read(&path).unwrap();
        let _failure = durability::fail_durable_replace_for_destination("catalog.hawdb");
        assert!(write_catalog(
            &path,
            &Catalog {
                revision: 12,
                ..first
            }
        )
        .is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        assert!(path.is_file());
        assert!(directory.join(METADATA_LOCK_FILE).is_file());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn catalog_reclamation_binds_heads_leases_and_deleted_directories() {
        let (directory, catalog_path) = temporary_catalog_path();
        let object_root = directory.join("objects");
        let mut objects = ImmutableObjectStore::open(&object_root).unwrap();
        let checkpoint = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, b"checkpoint");
        let wal = ObjectReference::for_bytes(ObjectKind::SealedWal, 1, b"wal");
        let manifest = ObjectReference::for_bytes(ObjectKind::DurableManifest, 1, b"manifest");
        objects.publish(checkpoint, b"checkpoint").unwrap();
        objects.publish(wal, b"wal").unwrap();
        objects.publish(manifest, b"manifest").unwrap();
        let sealed_root = crate::sealed_root::SealedRoot {
            checkpoint_epoch: 1,
            commit_epoch: 1,
            wal_replay_start_lsn: 0,
            durable_manifest: manifest,
            checkpoint_references: vec![checkpoint],
            checkpoint_bindings: vec![crate::sealed_root::CheckpointArtifactBinding {
                relative_path: "checkpoint.hawdb".to_string(),
                reference: checkpoint,
            }],
            sealed_wals: vec![crate::sealed_root::SealedWalReference {
                start_lsn: 0,
                end_lsn: 3,
                object: wal,
            }],
        };
        let sealed_root_bytes = sealed_root.encode().unwrap();
        let root_reference =
            ObjectReference::for_bytes(ObjectKind::SealedRoot, 2, &sealed_root_bytes);
        let orphan = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, b"orphan");
        objects.publish(root_reference, &sealed_root_bytes).unwrap();
        objects.publish(orphan, b"orphan").unwrap();

        let parent_directory = directory.join("parent");
        let deleted_directory = directory.join("deleted");
        fs::create_dir_all(&parent_directory).unwrap();
        fs::create_dir_all(&deleted_directory).unwrap();
        fs::write(deleted_directory.join("head"), b"old").unwrap();
        let parent_head_path = parent_directory.join("branch.head");
        fs::write(
            &parent_head_path,
            BranchHead {
                project_id: *id(99).as_uuid().as_bytes(),
                branch_id: *id(1).as_uuid().as_bytes(),
                physical_generation: 1,
                sealed_root: root_reference,
                logical_commit_epoch: 1,
                active_wal: crate::branch_head::ActiveWalIdentity {
                    generation: 1,
                    replay_start_lsn: 3,
                    byte_length: 3,
                    sha256: hawdb_integrity::sha256(b"wal"),
                },
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
        let mut deleted = record(2, "deleted");
        deleted.parent_id = Some(id(1));
        deleted.state = BranchState::Deleted;
        deleted.create_outcome = CreateOutcome::Succeeded;
        write_catalog(
            &catalog_path,
            &Catalog {
                project_id: id(99),
                revision: 1,
                branches: vec![record(1, "parent"), deleted],
            },
        )
        .unwrap();

        let report = reclaim_catalog_branches(
            &catalog_path,
            &mut objects,
            &[root_reference, manifest, checkpoint, wal, orphan],
            &[
                BranchReclamationPath {
                    id: id(1),
                    directory: parent_directory,
                    head_path: parent_head_path,
                },
                BranchReclamationPath {
                    id: id(2),
                    directory: deleted_directory.clone(),
                    head_path: deleted_directory.join("branch.head"),
                },
            ],
        )
        .unwrap();
        assert_eq!(report.retained_objects, 4);
        assert_eq!(report.reclaimed_objects, 1);
        assert!(!deleted_directory.exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn read_rejects_an_oversized_catalog_before_loading_bytes() {
        let (directory, path) = temporary_catalog_path();
        let file = fs::File::create(&path).unwrap();
        file.set_len((MAX_CATALOG_BYTES + 1) as u64).unwrap();
        let error = read_catalog(&path).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn codec_rejects_tampering_versions_duplicates_and_trailing_bytes() {
        let catalog = Catalog {
            project_id: id(99),
            revision: 1,
            branches: vec![record(1, "one")],
        };
        let encoded = catalog.encode().unwrap();
        let mut tampered = encoded.clone();
        tampered[10] ^= 1;
        assert_eq!(Catalog::decode(&tampered), Err(CatalogError::Checksum));

        let mut trailing = encoded.clone();
        trailing.insert(trailing.len() - 4, 0);
        let checksum = crc32c(&trailing[..trailing.len() - 4]).get();
        let checksum_offset = trailing.len() - 4;
        trailing[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
        assert_eq!(Catalog::decode(&trailing), Err(CatalogError::TrailingBytes));

        let duplicate = Catalog {
            project_id: id(99),
            revision: 1,
            branches: vec![record(1, "one"), record(1, "two")],
        };
        assert_eq!(
            duplicate.encode(),
            Err(CatalogError::Duplicate("branch UUID"))
        );
    }

    #[test]
    fn codec_rejects_old_headers_versions_and_retired_state_tag() {
        let encoded = catalog().encode().unwrap();
        assert_eq!(&encoded[..8], b"HBCATV2\0");
        for (offset, replacement) in [(0, b"HBCATV1\0".as_slice()), (8, &[1, 0])] {
            let mut old = encoded.clone();
            old[offset..offset + replacement.len()].copy_from_slice(replacement);
            let checksum_offset = old.len() - 4;
            let checksum = crc32c(&old[..checksum_offset]).get();
            old[checksum_offset..].copy_from_slice(&checksum.to_le_bytes());
            assert_eq!(Catalog::decode(&old), Err(CatalogError::Version));
        }

        // Build a record with the retired state tag and a valid envelope checksum.
        let branch = record(1, "first");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        put_u16(&mut bytes, VERSION);
        bytes.extend_from_slice(id(99).as_uuid().as_bytes());
        put_u64(&mut bytes, 1);
        put_u32(&mut bytes, 1);
        let record_start = bytes.len();
        encode_branch(&mut bytes, &branch).unwrap();
        let mut reader = Reader::new(&bytes[record_start..]);
        let _ = reader.array::<16>().unwrap();
        let _ = reader.string(MAX_NAME_BYTES, "branch name").unwrap();
        let _ = reader.optional_uuid().unwrap();
        let _ = reader.u64().unwrap();
        let _ = reader.optional_array().unwrap();
        let _ = reader.u64().unwrap();
        let state_offset = reader.offset;
        bytes[record_start + state_offset] = 2;
        let checksum = crc32c(&bytes).get();
        put_u32(&mut bytes, checksum);
        assert_eq!(
            Catalog::decode(&bytes),
            Err(CatalogError::InvalidEnum("branch state"))
        );
    }

    #[test]
    fn lifecycle_transitions_are_idempotent_and_revision_bound() {
        let mut catalog = catalog();
        let request = create_request();
        let before = catalog.encode().unwrap();
        assert_eq!(catalog.reserve_create(request.clone()).unwrap(), id(3));
        assert_eq!(catalog.branches.len(), 3);
        assert_eq!(catalog.branches[2].state, BranchState::Creating);
        assert_eq!(catalog.reserve_create(request).unwrap(), id(3));
        assert_eq!(catalog.encode().unwrap(), catalog.encode().unwrap());
        assert_ne!(catalog.encode().unwrap(), before);

        assert_eq!(
            catalog.complete_create(id(3), 99),
            Err(CatalogTransitionError::StaleRevision {
                expected: 99,
                actual: 1
            })
        );
        catalog.complete_create(id(3), 1).unwrap();
        assert_eq!(catalog.branches[2].state, BranchState::Ready);
        catalog
            .rename(id(3), 2, BranchName::new("renamed").unwrap())
            .unwrap();
        catalog.begin_delete(id(3), 3).unwrap();
        catalog.finish_delete(id(3), 4).unwrap();
        assert_eq!(catalog.branches[2].state, BranchState::Deleted);

        let mut reused = create_request();
        reused.id = id(4);
        reused.name = BranchName::new("renamed").unwrap();
        reused.request_key = "create-reused-name".to_string();
        reused.request_fingerprint = [10; DIGEST_BYTES];
        assert_eq!(catalog.reserve_create(reused).unwrap(), id(4));
    }

    #[test]
    fn failed_reservation_and_cas_leave_catalog_bytes_unchanged() {
        let mut catalog = catalog();
        let mut request = create_request();
        request.source_commit_epoch = 6;
        let before = catalog.encode().unwrap();
        assert_eq!(
            catalog.reserve_create(request),
            Err(CatalogTransitionError::Conflict(
                "create source revision is stale"
            ))
        );
        assert_eq!(catalog.encode().unwrap(), before);

        assert_eq!(
            catalog.rename(id(1), 99, BranchName::new("renamed").unwrap()),
            Err(CatalogTransitionError::StaleRevision {
                expected: 99,
                actual: 3
            })
        );
        assert_eq!(catalog.encode().unwrap(), before);
    }

    #[test]
    fn invalid_uuid_and_deleted_pending_state_fail_closed() {
        assert!(BranchId::new(Uuid::nil()).is_err());
        assert!(BranchId::parse("not-a-uuid").is_err());
        let mut branch = record(1, "one");
        branch.state = BranchState::Deleted;
        branch.create_outcome = CreateOutcome::Pending;
        let catalog = Catalog {
            project_id: id(99),
            revision: 1,
            branches: vec![branch],
        };
        assert_eq!(
            catalog.encode(),
            Err(CatalogError::InvalidState(
                "deleted branch cannot have a pending create outcome"
            ))
        );
    }
}
