//! Bounded, deterministic identity and catalog records for project branches.
//!
//! This module is the storage-owned codec seam for the branching contract.  It
//! deliberately does not open branches or publish files yet; those operations
//! will build on these validated records.  The wire format is versioned and
//! checksummed so a future publisher can reject an incomplete or ambiguous
//! catalog before changing any durable selector.

use crate::branch_head::{
    create_child_branch_head_from_parent, read_branch_head, BranchHead, BranchHeadError,
    ChildBranchHeadRequest, ChildBranchSourceExpectation,
};
use crate::durability;
use crate::immutable_object::{
    BranchReclamationEntry, BranchReclamationInventory, ImmutableObjectError, ImmutableObjectStore,
    ObjectReference, ReclamationReport,
};
use crate::ownership::{DatabaseDirectoryLease, DatabaseDirectoryLeaseError};
use hawdb_core::Uuid;
use hawdb_integrity::crc32c;
use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const MAGIC: &[u8; 8] = b"HBCATV1\0";
const VERSION: u16 = 1;
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
    Expired,
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
    pub expires_at_unix_seconds: Option<i64>,
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
    pub expires_at_unix_seconds: Option<i64>,
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
                expires_at_unix_seconds: None,
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
        if parent.source_commit_epoch != request.source_commit_epoch {
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
            expires_at_unix_seconds: request.expires_at_unix_seconds,
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
        if !matches!(
            self.branches[index].state,
            BranchState::Ready | BranchState::Expired
        ) {
            return Err(CatalogTransitionError::InvalidState(
                "only a ready or expired branch can be renamed",
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

    pub fn expire(
        &mut self,
        id: BranchId,
        expected_metadata_revision: u64,
        now_unix_seconds: i64,
    ) -> Result<(), CatalogTransitionError> {
        let index = self.index_at_revision(id, expected_metadata_revision)?;
        let branch = &self.branches[index];
        if branch.name.as_str() == "main" {
            return Err(CatalogTransitionError::InvalidState(
                "main branch cannot expire",
            ));
        }
        if branch.state != BranchState::Ready {
            return Err(CatalogTransitionError::InvalidState(
                "only a ready branch can expire",
            ));
        }
        if branch
            .expires_at_unix_seconds
            .is_none_or(|expires_at| expires_at > now_unix_seconds)
        {
            return Err(CatalogTransitionError::InvalidState(
                "branch expiry is not due",
            ));
        }
        self.transition_state(index, BranchState::Expired)
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
        if !matches!(branch.state, BranchState::Ready | BranchState::Expired) {
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
    let mut file = fs::File::open(path)?;
    let mut encoded = Vec::with_capacity(length as usize);
    file.read_to_end(&mut encoded)?;
    Catalog::decode(&encoded).map_err(|error| invalid_data(error.to_string()))
}

/// Publish a catalog with candidate-file sync followed by atomic replacement.
///
/// The destination is never opened for writing.  A failed write or sync removes
/// only its private candidate; a failed replacement is returned without retry,
/// because the caller cannot infer whether the directory operation reached the
/// filesystem.  The caller must reopen before attempting another publication.
pub fn write_catalog(path: &Path, catalog: &Catalog) -> io::Result<()> {
    let encoded = catalog
        .encode()
        .map_err(|error| invalid_data(error.to_string()))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| invalid_data("branch catalog destination has no parent"))?;
    let _metadata_lock = CatalogMetadataLease::acquire(parent)?;
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
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&candidate)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        drop(file);
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

impl std::error::Error for CatalogFileTransitionError {}

/// Reserves a child branch in the durable catalog before child files are made.
/// The returned metadata revision binds the later completion transition.
pub fn reserve_create_file(
    path: &Path,
    request: CreateRequest,
) -> Result<CreateReservation, CatalogFileTransitionError> {
    let mut catalog = read_catalog(path).map_err(CatalogFileTransitionError::Io)?;
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
    write_catalog(path, &catalog).map_err(CatalogFileTransitionError::Io)?;
    Ok(CreateReservation {
        id,
        metadata_revision,
    })
}

/// Marks a previously reserved child ready after its head and WAL are durable.
pub fn complete_create_file(
    path: &Path,
    reservation: CreateReservation,
) -> Result<(), CatalogFileTransitionError> {
    let mut catalog = read_catalog(path).map_err(CatalogFileTransitionError::Io)?;
    catalog
        .complete_create(reservation.id, reservation.metadata_revision)
        .map_err(CatalogFileTransitionError::Transition)?;
    write_catalog(path, &catalog).map_err(CatalogFileTransitionError::Io)
}

/// Aborts a reserved child create after a known pre-publication failure.
pub fn abort_create_file(
    path: &Path,
    reservation: CreateReservation,
) -> Result<(), CatalogFileTransitionError> {
    let mut catalog = read_catalog(path).map_err(CatalogFileTransitionError::Io)?;
    catalog
        .abort_create(reservation.id, reservation.metadata_revision)
        .map_err(CatalogFileTransitionError::Transition)?;
    write_catalog(path, &catalog).map_err(CatalogFileTransitionError::Io)
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

impl std::error::Error for BranchCreateError {}

/// Reserves the catalog record, creates the isolated child head/WAL, and
/// completes the record only after both files are durable. Known child-file
/// failures abort the reservation; an uncertain catalog completion leaves the
/// `Creating` record for deterministic recovery on the next open.
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
    let reservation =
        reserve_create_file(catalog_path, request).map_err(BranchCreateError::Catalog)?;
    let child_head_path = child_head_request.head_path.clone();
    let child_directory = child_head_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if let Err(error) = fs::create_dir_all(child_directory) {
        let _ = abort_create_file(catalog_path, reservation);
        return Err(BranchCreateError::Head(BranchHeadError::Io {
            operation: "create child branch directory",
            source: error,
        }));
    }
    let lease = match DatabaseDirectoryLease::acquire(child_directory) {
        Ok(lease) => lease,
        Err(error) => {
            let _ = abort_create_file(catalog_path, reservation);
            return Err(BranchCreateError::Lease(error));
        }
    };
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
            let _ = abort_create_file(catalog_path, reservation);
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
    match branch.expires_at_unix_seconds {
        Some(value) => {
            output.push(1);
            put_i64(output, value);
        }
        None => output.push(0),
    }
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
    let expires_at_unix_seconds = if reader.byte()? == 1 {
        Some(reader.i64()?)
    } else {
        None
    };
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
        expires_at_unix_seconds,
        create_request_key,
        request_fingerprint,
        create_outcome,
    })
}

fn state_byte(value: BranchState) -> u8 {
    match value {
        BranchState::Creating => 0,
        BranchState::Ready => 1,
        BranchState::Expired => 2,
        BranchState::Deleting => 3,
        BranchState::Deleted => 4,
    }
}

fn parse_state(value: u8) -> Result<BranchState, CatalogError> {
    match value {
        0 => Ok(BranchState::Creating),
        1 => Ok(BranchState::Ready),
        2 => Ok(BranchState::Expired),
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

fn put_i64(output: &mut Vec<u8>, value: i64) {
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

    fn i64(&mut self) -> Result<i64, CatalogError> {
        Ok(i64::from_le_bytes(
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
            expires_at_unix_seconds: Some(42),
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
            expires_at_unix_seconds: Some(100),
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

    #[test]
    fn create_branch_from_parent_completes_catalog_after_child_files() {
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
        let child_id = request.id;
        let child = create_branch_from_parent(
            &catalog_path,
            &parent_head_path,
            ChildBranchHeadRequest {
                project_id: parent.project_id,
                branch_id: *request.id.as_uuid().as_bytes(),
                sealed_root: root,
                logical_commit_epoch: 7,
                active_wal_generation: 1,
                replay_start_lsn: 42,
                head_path: child_head_path.clone(),
                wal_path: child_wal_path.clone(),
            },
            ChildBranchSourceExpectation {
                branch_id: parent.branch_id,
                physical_generation: 4,
                logical_commit_epoch: 7,
                sealed_root: root,
            },
            1024,
            request,
        )
        .unwrap();
        assert_eq!(child.head.sealed_root, root);
        assert_eq!(
            read_catalog(&catalog_path)
                .unwrap()
                .branches
                .last()
                .unwrap()
                .state,
            BranchState::Ready
        );
        assert!(child_head_path.is_file());
        assert!(child_wal_path.is_file());
        assert!(matches!(
            DatabaseDirectoryLease::acquire(&child_directory),
            Err(DatabaseDirectoryLeaseError::AlreadyOpen)
        ));
        drop(child);
        let reopened = crate::branch_head::read_branch_head(&child_head_path).unwrap();
        assert_eq!(reopened.sealed_root, root);
        assert_eq!(
            read_catalog(&catalog_path)
                .unwrap()
                .branches
                .iter()
                .find(|branch| branch.id == child_id)
                .unwrap()
                .state,
            BranchState::Ready
        );
        let reopened_lease = DatabaseDirectoryLease::acquire(&child_directory).unwrap();
        drop(reopened_lease);
        fs::remove_dir_all(directory).unwrap();
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
        objects.publish(checkpoint, b"checkpoint").unwrap();
        objects.publish(wal, b"wal").unwrap();
        let sealed_root = crate::sealed_root::SealedRoot {
            checkpoint_epoch: 1,
            commit_epoch: 1,
            wal_replay_start_lsn: 0,
            checkpoint_references: vec![checkpoint],
            sealed_wals: vec![crate::sealed_root::SealedWalReference {
                start_lsn: 0,
                end_lsn: 3,
                object: wal,
            }],
        };
        let sealed_root_bytes = sealed_root.encode().unwrap();
        let root_reference =
            ObjectReference::for_bytes(ObjectKind::SealedRoot, 1, &sealed_root_bytes);
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
            &[root_reference, checkpoint, wal, orphan],
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
        assert_eq!(report.retained_objects, 3);
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
        catalog.expire(id(3), 3, 100).unwrap();
        catalog.begin_delete(id(3), 4).unwrap();
        catalog.finish_delete(id(3), 5).unwrap();
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
        request.source_commit_epoch = 8;
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
