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

//! Versioned branch-head selector and generation-checked publication.

use crate::durability::durable_replace_file;
use crate::immutable_object::{
    ImmutableObjectError, ImmutableObjectStore, ObjectKind, ObjectReference,
};
use crate::sealed_root::{SealedRoot, SealedRootError};
use crate::sealed_wal::PreparedWalRotation;
use hawdb_integrity::{crc32c, IntegrityHasher, Sha256Digest};
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const MAGIC: &[u8; 12] = b"HAWDBHEADV1\0";
const ENCODED_BYTES: usize = 12 + 16 + 16 + 8 + (1 + 2 + 8 + 32) + 8 + 8 + 8 + 8 + 32 + 4;
const MAX_HEAD_BYTES: u64 = 1024 * 1024;
static CANDIDATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveWalIdentity {
    pub generation: u64,
    pub replay_start_lsn: u64,
    pub byte_length: u64,
    pub sha256: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BranchHead {
    pub project_id: [u8; 16],
    pub branch_id: [u8; 16],
    pub physical_generation: u64,
    pub sealed_root: ObjectReference,
    pub logical_commit_epoch: u64,
    pub active_wal: ActiveWalIdentity,
}

#[derive(Debug, Clone, Copy)]
pub struct PreparedWalRotationHeadRequest<'a> {
    pub expected_current_generation: u64,
    pub project_id: [u8; 16],
    pub branch_id: [u8; 16],
    pub sealed_root: ObjectReference,
    pub logical_commit_epoch: u64,
    pub prepared: &'a PreparedWalRotation,
    pub max_active_wal_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct PreparedWalRootPublicationRequest<'a> {
    pub expected_current_generation: u64,
    pub project_id: [u8; 16],
    pub branch_id: [u8; 16],
    pub logical_commit_epoch: u64,
    pub prepared: &'a PreparedWalRotation,
    pub max_active_wal_bytes: u64,
}

/// Errors returned by the complete sealed-root and WAL handoff.
#[derive(Debug)]
pub enum WalRotationPublicationError {
    Root(SealedRootError),
    Immutable(ImmutableObjectError),
    Head(BranchHeadError),
    SealedWalMissing,
}

impl Display for WalRotationPublicationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Root(error) => Display::fmt(error, formatter),
            Self::Immutable(error) => Display::fmt(error, formatter),
            Self::Head(error) => Display::fmt(error, formatter),
            Self::SealedWalMissing => {
                formatter.write_str("sealed root does not reference the prepared WAL")
            }
        }
    }
}

impl std::error::Error for WalRotationPublicationError {}

/// Publishes a sealed root and adopts the prepared successor WAL atomically
/// from the selector's point of view.
///
/// The caller must hold the branch publication barrier.  The root object is
/// published first, then the head selector is switched last.  If head
/// publication fails, the old head remains authoritative and the new root is
/// harmlessly unreachable for later reclamation.  The root must reference the
/// exact sealed WAL produced by `prepared`; accepting a root that omits it
/// would make the head's replay boundary unverifiable.
pub fn publish_prepared_wal_rotation_with_root(
    path: &Path,
    request: PreparedWalRootPublicationRequest<'_>,
    root: &SealedRoot,
    objects: &mut ImmutableObjectStore,
) -> Result<BranchHead, WalRotationPublicationError> {
    root.validate().map_err(WalRotationPublicationError::Root)?;
    if !root.sealed_wals.iter().any(|wal| {
        wal.start_lsn == request.prepared.sealed.start_lsn
            && wal.end_lsn == request.prepared.sealed.end_lsn
            && wal.object == request.prepared.sealed.object
    }) {
        return Err(WalRotationPublicationError::SealedWalMissing);
    }
    let encoded_root = root.encode().map_err(WalRotationPublicationError::Root)?;
    let root_reference = root
        .object_reference()
        .map_err(WalRotationPublicationError::Root)?;
    objects
        .publish(root_reference, &encoded_root)
        .map_err(WalRotationPublicationError::Immutable)?;
    publish_prepared_wal_rotation(
        path,
        PreparedWalRotationHeadRequest {
            expected_current_generation: request.expected_current_generation,
            project_id: request.project_id,
            branch_id: request.branch_id,
            sealed_root: root_reference,
            logical_commit_epoch: request.logical_commit_epoch,
            prepared: request.prepared,
            max_active_wal_bytes: request.max_active_wal_bytes,
        },
    )
    .map_err(WalRotationPublicationError::Head)
}

/// Inputs for creating a child branch's independent writable head.
#[derive(Debug, Clone)]
pub struct ChildBranchHeadRequest {
    pub project_id: [u8; 16],
    pub branch_id: [u8; 16],
    pub sealed_root: ObjectReference,
    pub logical_commit_epoch: u64,
    pub active_wal_generation: u64,
    pub replay_start_lsn: u64,
    pub head_path: PathBuf,
    pub wal_path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub struct ChildBranchSourceExpectation {
    pub branch_id: [u8; 16],
    pub physical_generation: u64,
    pub logical_commit_epoch: u64,
    pub sealed_root: ObjectReference,
}

/// Creates the child branch's private empty WAL and selector.
///
/// The sealed root is shared by reference; no parent data is copied. Both
/// files use exclusive creation and are synchronized before the function
/// returns, so an interrupted create cannot expose a partially written head.
pub fn create_child_branch_head(
    head_path: &Path,
    request: ChildBranchHeadRequest,
    max_active_wal_bytes: u64,
) -> Result<BranchHead, BranchHeadError> {
    if request.active_wal_generation == 0 {
        return Err(BranchHeadError::InvalidWalIdentity);
    }
    let header = crate::wal::frame::encode_binary_wal_header(
        request.active_wal_generation,
        request.replay_start_lsn,
    );
    let mut wal = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&request.wal_path)
        .map_err(|source| BranchHeadError::Io {
            operation: "create child active WAL",
            source,
        })?;
    if let Err(source) = wal.write_all(&header).and_then(|_| wal.sync_all()) {
        let _ = fs::remove_file(&request.wal_path);
        return Err(BranchHeadError::Io {
            operation: "write and sync child active WAL",
            source,
        });
    }
    if let Err(source) = crate::durability::sync_parent_directory(&request.wal_path) {
        let _ = fs::remove_file(&request.wal_path);
        return Err(BranchHeadError::Io {
            operation: "sync child active WAL directory",
            source,
        });
    }
    let active_wal = active_wal_identity_from_file(
        &request.wal_path,
        request.active_wal_generation,
        request.replay_start_lsn,
        max_active_wal_bytes,
    )?;
    let head = BranchHead {
        project_id: request.project_id,
        branch_id: request.branch_id,
        physical_generation: 1,
        sealed_root: request.sealed_root,
        logical_commit_epoch: request.logical_commit_epoch,
        active_wal,
    };
    let encoded = head.encode()?;
    let mut selector = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(head_path)
    {
        Ok(file) => file,
        Err(source) => {
            let _ = fs::remove_file(&request.wal_path);
            return Err(BranchHeadError::Io {
                operation: "create child branch head",
                source,
            });
        }
    };
    if let Err(source) = selector
        .write_all(&encoded)
        .and_then(|_| selector.sync_all())
    {
        let _ = fs::remove_file(head_path);
        let _ = fs::remove_file(&request.wal_path);
        return Err(BranchHeadError::Io {
            operation: "write and sync child branch head",
            source,
        });
    }
    if let Err(source) = crate::durability::sync_parent_directory(head_path) {
        let _ = fs::remove_file(head_path);
        let _ = fs::remove_file(&request.wal_path);
        return Err(BranchHeadError::Io {
            operation: "sync child branch head directory",
            source,
        });
    }
    Ok(head)
}

/// Creates the first durable selector for a branch after its immutable root
/// and successor WAL have already been published. The selector is exclusive;
/// an existing path is never replaced or adopted implicitly.
#[derive(Debug, Clone, Copy)]
pub struct InitialBranchHeadRequest<'a> {
    pub path: &'a Path,
    pub project_id: [u8; 16],
    pub branch_id: [u8; 16],
    pub sealed_root: ObjectReference,
    pub logical_commit_epoch: u64,
    pub wal_path: &'a Path,
    pub wal_generation: u64,
    pub replay_start_lsn: u64,
    pub max_active_wal_bytes: u64,
}

pub fn create_initial_branch_head(
    request: InitialBranchHeadRequest<'_>,
) -> Result<BranchHead, BranchHeadError> {
    let active_wal = active_wal_identity_from_file(
        request.wal_path,
        request.wal_generation,
        request.replay_start_lsn,
        request.max_active_wal_bytes,
    )?;
    let head = BranchHead {
        project_id: request.project_id,
        branch_id: request.branch_id,
        physical_generation: 1,
        sealed_root: request.sealed_root,
        logical_commit_epoch: request.logical_commit_epoch,
        active_wal,
    };
    let encoded = head.encode()?;
    let mut selector = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(request.path)
        .map_err(|source| BranchHeadError::Io {
            operation: "create initial branch head",
            source,
        })?;
    if let Err(source) = selector
        .write_all(&encoded)
        .and_then(|_| selector.sync_all())
        .and_then(|_| crate::durability::sync_parent_directory(request.path))
    {
        let _ = fs::remove_file(request.path);
        return Err(BranchHeadError::Io {
            operation: "sync initial branch head",
            source,
        });
    }
    Ok(head)
}

/// Creates a child head only when the selected parent head still names the
/// expected sealed source revision.  The parent selector is read but never
/// modified, so a stale request cannot alter parent state.
pub fn create_child_branch_head_from_parent(
    parent_head_path: &Path,
    child_head_path: &Path,
    request: ChildBranchHeadRequest,
    expected_parent: ChildBranchSourceExpectation,
    max_active_wal_bytes: u64,
) -> Result<BranchHead, BranchHeadError> {
    let parent = read_branch_head(parent_head_path)?;
    if parent.project_id != request.project_id {
        return Err(BranchHeadError::BranchIdentityMismatch);
    }
    if parent.branch_id != expected_parent.branch_id {
        return Err(BranchHeadError::BranchIdentityMismatch);
    }
    if parent.physical_generation != expected_parent.physical_generation {
        return Err(BranchHeadError::StaleGeneration {
            expected: expected_parent.physical_generation,
            actual: parent.physical_generation,
        });
    }
    if parent.logical_commit_epoch != expected_parent.logical_commit_epoch
        || parent.sealed_root != expected_parent.sealed_root
    {
        return Err(BranchHeadError::ParentSourceMismatch);
    }
    if request.sealed_root != parent.sealed_root
        || request.logical_commit_epoch != parent.logical_commit_epoch
    {
        return Err(BranchHeadError::ParentSourceMismatch);
    }
    create_child_branch_head(child_head_path, request, max_active_wal_bytes)
}

#[derive(Debug)]
pub enum BranchHeadError {
    Io {
        operation: &'static str,
        source: io::Error,
    },
    TooLarge,
    InvalidMagic,
    Truncated,
    ChecksumMismatch,
    TrailingBytes,
    InvalidIdentity,
    InvalidGeneration,
    InvalidRootReference,
    InvalidWalIdentity,
    StaleGeneration {
        expected: u64,
        actual: u64,
    },
    BranchIdentityMismatch,
    ParentSourceMismatch,
    CandidatePublicationUncertain {
        source: io::Error,
    },
}

impl Display for BranchHeadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { operation, source } => write!(formatter, "{operation}: {source}"),
            Self::TooLarge => formatter.write_str("branch head exceeds its byte limit"),
            Self::InvalidMagic => formatter.write_str("branch head has an unknown format"),
            Self::Truncated => formatter.write_str("branch head is truncated"),
            Self::ChecksumMismatch => formatter.write_str("branch head checksum mismatch"),
            Self::TrailingBytes => formatter.write_str("branch head has trailing bytes"),
            Self::InvalidIdentity => formatter.write_str("branch head has an all-zero identity"),
            Self::InvalidGeneration => formatter.write_str("branch head generation is zero"),
            Self::InvalidRootReference => {
                formatter.write_str("branch head sealed-root reference is invalid")
            }
            Self::InvalidWalIdentity => {
                formatter.write_str("branch head active-WAL identity is invalid")
            }
            Self::StaleGeneration { expected, actual } => {
                write!(
                    formatter,
                    "branch head generation is stale: expected {expected}, got {actual}"
                )
            }
            Self::BranchIdentityMismatch => {
                formatter.write_str("branch head identity does not match the selected branch")
            }
            Self::ParentSourceMismatch => {
                formatter.write_str("parent branch source revision does not match")
            }
            Self::CandidatePublicationUncertain { source } => {
                write!(formatter, "branch head publication is uncertain: {source}")
            }
        }
    }
}

impl std::error::Error for BranchHeadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } | Self::CandidatePublicationUncertain { source } => {
                Some(source)
            }
            _ => None,
        }
    }
}

impl BranchHead {
    pub fn validate(&self) -> Result<(), BranchHeadError> {
        if self.project_id == [0; 16] || self.branch_id == [0; 16] {
            return Err(BranchHeadError::InvalidIdentity);
        }
        if self.physical_generation == 0 {
            return Err(BranchHeadError::InvalidGeneration);
        }
        if self.sealed_root.kind != ObjectKind::SealedRoot
            || self.sealed_root.format_version == 0
            || self.sealed_root.sha256 == Sha256Digest::from_bytes([0; 32])
        {
            return Err(BranchHeadError::InvalidRootReference);
        }
        if self.active_wal.generation == 0
            || self.active_wal.sha256 == Sha256Digest::from_bytes([0; 32])
        {
            return Err(BranchHeadError::InvalidWalIdentity);
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, BranchHeadError> {
        self.validate()?;
        let mut encoded = Vec::with_capacity(ENCODED_BYTES);
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&self.project_id);
        encoded.extend_from_slice(&self.branch_id);
        encoded.extend_from_slice(&self.physical_generation.to_le_bytes());
        encode_reference(&mut encoded, self.sealed_root);
        encoded.extend_from_slice(&self.logical_commit_epoch.to_le_bytes());
        encoded.extend_from_slice(&self.active_wal.generation.to_le_bytes());
        encoded.extend_from_slice(&self.active_wal.replay_start_lsn.to_le_bytes());
        encoded.extend_from_slice(&self.active_wal.byte_length.to_le_bytes());
        encoded.extend_from_slice(self.active_wal.sha256.as_bytes());
        encoded.extend_from_slice(&crc32c(&encoded).get().to_le_bytes());
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BranchHeadError> {
        if bytes.len() > MAX_HEAD_BYTES as usize {
            return Err(BranchHeadError::TooLarge);
        }
        if bytes.len() < MAGIC.len() + 4 {
            return Err(BranchHeadError::Truncated);
        }
        if &bytes[..MAGIC.len()] != MAGIC {
            return Err(BranchHeadError::InvalidMagic);
        }
        let checksum_offset = bytes.len() - 4;
        let expected = u32::from_le_bytes(
            bytes[checksum_offset..]
                .try_into()
                .map_err(|_| BranchHeadError::Truncated)?,
        );
        if crc32c(&bytes[..checksum_offset]).get() != expected {
            return Err(BranchHeadError::ChecksumMismatch);
        }
        let mut reader = Reader::new(&bytes[MAGIC.len()..checksum_offset]);
        let mut project_id = [0; 16];
        project_id.copy_from_slice(reader.bytes(16)?);
        let mut branch_id = [0; 16];
        branch_id.copy_from_slice(reader.bytes(16)?);
        let physical_generation = reader.u64()?;
        let sealed_root = decode_reference(&mut reader)?;
        let logical_commit_epoch = reader.u64()?;
        let active_wal = ActiveWalIdentity {
            generation: reader.u64()?,
            replay_start_lsn: reader.u64()?,
            byte_length: reader.u64()?,
            sha256: reader.digest()?,
        };
        if !reader.is_empty() {
            return Err(BranchHeadError::TrailingBytes);
        }
        let head = Self {
            project_id,
            branch_id,
            physical_generation,
            sealed_root,
            logical_commit_epoch,
            active_wal,
        };
        head.validate()?;
        Ok(head)
    }
}

/// Reads a bounded, checksummed branch head.
pub fn read_branch_head(path: &Path) -> Result<BranchHead, BranchHeadError> {
    let length = map_io("read branch head metadata", fs::metadata(path))?.len();
    if length > MAX_HEAD_BYTES {
        return Err(BranchHeadError::TooLarge);
    }
    let mut file = map_io("open branch head", File::open(path))?;
    let mut bytes = Vec::with_capacity(usize::try_from(length).unwrap_or(usize::MAX));
    map_io("read branch head", file.read_to_end(&mut bytes))?;
    BranchHead::decode(&bytes)
}

/// Computes the identity a branch head records for its active WAL file.
///
/// The caller must have completed the WAL rotation preparation and hold the
/// source publication barrier.  The file is read in full and bounded before
/// its identity is returned; a path name or metadata length alone is never
/// accepted as an active-WAL identity.
pub fn active_wal_identity_from_file(
    path: &Path,
    generation: u64,
    replay_start_lsn: u64,
    max_bytes: u64,
) -> Result<ActiveWalIdentity, BranchHeadError> {
    if generation == 0 {
        return Err(BranchHeadError::InvalidWalIdentity);
    }
    let length = map_io("read active WAL metadata", fs::metadata(path))?.len();
    if length > max_bytes {
        return Err(BranchHeadError::InvalidWalIdentity);
    }
    let mut file = map_io("open active WAL", File::open(path))?;
    let mut bytes = Vec::with_capacity(usize::try_from(length).unwrap_or(usize::MAX));
    map_io("read active WAL", file.read_to_end(&mut bytes))?;
    if bytes.len() as u64 != length {
        return Err(BranchHeadError::InvalidWalIdentity);
    }
    let mut hasher = IntegrityHasher::new();
    hasher.update(&bytes);
    Ok(ActiveWalIdentity {
        generation,
        replay_start_lsn,
        byte_length: length,
        sha256: hasher.finish().sha256,
    })
}

/// Switches a branch head after [`PreparedWalRotation`] has made the
/// successor WAL header durable.  The caller must hold the source publication
/// barrier and must have published the sealed root before calling this
/// function; this function performs only the final selector adoption.
pub fn publish_prepared_wal_rotation(
    path: &Path,
    request: PreparedWalRotationHeadRequest<'_>,
) -> Result<BranchHead, BranchHeadError> {
    let current = read_branch_head(path)?;
    if current.project_id != request.project_id || current.branch_id != request.branch_id {
        return Err(BranchHeadError::BranchIdentityMismatch);
    }
    if current.physical_generation != request.expected_current_generation {
        return Err(BranchHeadError::StaleGeneration {
            expected: request.expected_current_generation,
            actual: current.physical_generation,
        });
    }
    let physical_generation = current
        .physical_generation
        .checked_add(1)
        .ok_or(BranchHeadError::InvalidGeneration)?;
    let active_wal = active_wal_identity_from_file(
        &request.prepared.next_wal_path,
        request.prepared.next_generation,
        request.prepared.next_start_lsn,
        request.max_active_wal_bytes,
    )?;
    let next = BranchHead {
        project_id: request.project_id,
        branch_id: request.branch_id,
        physical_generation,
        sealed_root: request.sealed_root,
        logical_commit_epoch: request.logical_commit_epoch,
        active_wal,
    };
    publish_branch_head(path, request.expected_current_generation, next)?;
    Ok(next)
}

/// Publishes a newer selector after an exact generation and identity check.
pub fn publish_branch_head(
    path: &Path,
    expected_current_generation: u64,
    next: BranchHead,
) -> Result<(), BranchHeadError> {
    next.validate()?;
    let current = read_branch_head(path)?;
    if current.project_id != next.project_id || current.branch_id != next.branch_id {
        return Err(BranchHeadError::BranchIdentityMismatch);
    }
    if current.physical_generation != expected_current_generation {
        return Err(BranchHeadError::StaleGeneration {
            expected: expected_current_generation,
            actual: current.physical_generation,
        });
    }
    if next.physical_generation <= current.physical_generation {
        return Err(BranchHeadError::StaleGeneration {
            expected: current.physical_generation.saturating_add(1),
            actual: next.physical_generation,
        });
    }
    let bytes = next.encode()?;
    let sequence = CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let candidate = path.with_extension(format!("hawdb.head.{sequence}.tmp"));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
            .map_err(|source| BranchHeadError::Io {
                operation: "create branch head candidate",
                source,
            })?;
        file.write_all(&bytes)
            .map_err(|source| BranchHeadError::Io {
                operation: "write branch head candidate",
                source,
            })?;
        file.sync_all().map_err(|source| BranchHeadError::Io {
            operation: "sync branch head candidate",
            source,
        })?;
        durable_replace_file(&candidate, path)
            .map_err(|source| BranchHeadError::CandidatePublicationUncertain { source })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&candidate);
    }
    result
}

fn encode_reference(encoded: &mut Vec<u8>, reference: ObjectReference) {
    encoded.push(reference.kind as u8);
    encoded.extend_from_slice(&reference.format_version.to_le_bytes());
    encoded.extend_from_slice(&reference.byte_length.to_le_bytes());
    encoded.extend_from_slice(reference.sha256.as_bytes());
}

fn decode_reference(reader: &mut Reader<'_>) -> Result<ObjectReference, BranchHeadError> {
    let kind = match reader.u8()? {
        1 => ObjectKind::Checkpoint,
        2 => ObjectKind::SealedWal,
        3 => ObjectKind::SealedRoot,
        _ => return Err(BranchHeadError::InvalidRootReference),
    };
    let reference = ObjectReference {
        kind,
        format_version: reader.u16()?,
        byte_length: reader.u64()?,
        sha256: reader.digest()?,
    };
    Ok(reference)
}

fn map_io<T>(operation: &'static str, result: io::Result<T>) -> Result<T, BranchHeadError> {
    result.map_err(|source| BranchHeadError::Io { operation, source })
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], BranchHeadError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(BranchHeadError::Truncated)?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(BranchHeadError::Truncated)?;
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, BranchHeadError> {
        Ok(*self.bytes(1)?.first().ok_or(BranchHeadError::Truncated)?)
    }

    fn u16(&mut self) -> Result<u16, BranchHeadError> {
        Ok(u16::from_le_bytes(
            self.bytes(2)?
                .try_into()
                .map_err(|_| BranchHeadError::Truncated)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, BranchHeadError> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?
                .try_into()
                .map_err(|_| BranchHeadError::Truncated)?,
        ))
    }

    fn digest(&mut self) -> Result<Sha256Digest, BranchHeadError> {
        let mut digest = [0; 32];
        digest.copy_from_slice(self.bytes(32)?);
        Ok(Sha256Digest::from_bytes(digest))
    }

    const fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::fail_durable_replace_for_destination;
    use crate::immutable_object::ImmutableObjectStore;
    use crate::immutable_object::ObjectKind;
    use crate::immutable_object::PublishOutcome;
    use crate::sealed_root::{CheckpointArtifactBinding, SealedRoot, SealedWalReference};
    use crate::sealed_wal::SealedWalPublication;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn sample() -> BranchHead {
        BranchHead {
            project_id: [1; 16],
            branch_id: [2; 16],
            physical_generation: 4,
            sealed_root: ObjectReference::for_bytes(ObjectKind::SealedRoot, 1, b"root"),
            logical_commit_epoch: 9,
            active_wal: ActiveWalIdentity {
                generation: 5,
                replay_start_lsn: 20,
                byte_length: 28,
                sha256: hawdb_integrity::sha256(b"active-header"),
            },
        }
    }

    fn path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("hawdb-head-{label}-{nanos}.hawdb"))
    }

    fn directory(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("hawdb-head-{label}-{nanos}"))
    }

    #[test]
    fn deterministic_round_trip_and_identity_validation() {
        let head = sample();
        let encoded = head.encode().unwrap();
        assert_eq!(BranchHead::decode(&encoded).unwrap(), head);
        assert_eq!(head.encode().unwrap(), encoded);
        let mut invalid = head;
        invalid.branch_id = [0; 16];
        assert!(matches!(
            invalid.validate(),
            Err(BranchHeadError::InvalidIdentity)
        ));
    }

    #[test]
    fn decode_rejects_checksum_truncation_and_trailing_bytes() {
        let encoded = sample().encode().unwrap();
        let mut tampered = encoded.clone();
        tampered[20] ^= 1;
        assert!(matches!(
            BranchHead::decode(&tampered),
            Err(BranchHeadError::ChecksumMismatch)
        ));
        assert!(matches!(
            BranchHead::decode(&encoded[..encoded.len() - 1]),
            Err(BranchHeadError::ChecksumMismatch)
        ));
        let mut trailing = encoded;
        trailing.insert(trailing.len() - 4, 0);
        let checksum_offset = trailing.len() - 4;
        let checksum = crc32c(&trailing[..checksum_offset]).get().to_le_bytes();
        trailing[checksum_offset..].copy_from_slice(&checksum);
        assert!(matches!(
            BranchHead::decode(&trailing),
            Err(BranchHeadError::TrailingBytes)
        ));
    }

    #[test]
    fn generation_cas_and_identity_reject_stale_publication() {
        let file = path("cas");
        fs::write(&file, sample().encode().unwrap()).unwrap();
        let mut next = sample();
        next.physical_generation = 5;
        assert!(matches!(
            publish_branch_head(&file, 3, next),
            Err(BranchHeadError::StaleGeneration { .. })
        ));
        next.project_id = [3; 16];
        assert!(matches!(
            publish_branch_head(&file, 4, next),
            Err(BranchHeadError::BranchIdentityMismatch)
        ));
        fs::remove_file(file).unwrap();
    }

    #[test]
    fn publication_replaces_one_complete_head_and_failure_preserves_old_bytes() {
        let file = path("publish");
        let old = sample();
        fs::write(&file, old.encode().unwrap()).unwrap();
        let mut next = old;
        next.physical_generation = 5;
        next.logical_commit_epoch = 10;
        publish_branch_head(&file, 4, next).unwrap();
        assert_eq!(read_branch_head(&file).unwrap(), next);
        let old_bytes = fs::read(&file).unwrap();
        let mut later = next;
        later.physical_generation = 6;
        let _guard = fail_durable_replace_for_destination(file.file_name().unwrap());
        assert!(matches!(
            publish_branch_head(&file, 5, later),
            Err(BranchHeadError::CandidatePublicationUncertain { .. })
        ));
        assert_eq!(fs::read(&file).unwrap(), old_bytes);
        fs::remove_file(file).unwrap();
    }

    #[test]
    fn active_wal_identity_binds_complete_successor_bytes() {
        let file = path("active-wal");
        let bytes = b"successor-wal-header-and-record";
        fs::write(&file, bytes).unwrap();
        let identity = active_wal_identity_from_file(&file, 7, 42, 1024).unwrap();
        assert_eq!(identity.generation, 7);
        assert_eq!(identity.replay_start_lsn, 42);
        assert_eq!(identity.byte_length, bytes.len() as u64);
        assert_eq!(identity.sha256, hawdb_integrity::sha256(bytes));
        assert!(matches!(
            active_wal_identity_from_file(&file, 7, 42, 1),
            Err(BranchHeadError::InvalidWalIdentity)
        ));
        fs::remove_file(file).unwrap();
    }

    #[test]
    fn child_head_creation_shares_root_and_creates_private_wal() {
        let head_path = path("child-head");
        let wal_path = path("child-wal");
        let root = ObjectReference::for_bytes(ObjectKind::SealedRoot, 1, b"parent-root");
        let child = create_child_branch_head(
            &head_path,
            ChildBranchHeadRequest {
                project_id: [3; 16],
                branch_id: [4; 16],
                sealed_root: root,
                logical_commit_epoch: 12,
                active_wal_generation: 1,
                replay_start_lsn: 99,
                head_path: head_path.clone(),
                wal_path: wal_path.clone(),
            },
            1024,
        )
        .unwrap();
        assert_eq!(child.physical_generation, 1);
        assert_eq!(child.sealed_root, root);
        assert_eq!(read_branch_head(&head_path).unwrap(), child);
        assert!(wal_path.exists());
        assert!(matches!(
            create_child_branch_head(
                &path("child-head-existing"),
                ChildBranchHeadRequest {
                    project_id: [3; 16],
                    branch_id: [4; 16],
                    sealed_root: root,
                    logical_commit_epoch: 12,
                    active_wal_generation: 1,
                    replay_start_lsn: 99,
                    head_path: path("child-head-existing"),
                    wal_path: wal_path.clone(),
                },
                1024,
            ),
            Err(BranchHeadError::Io { .. })
        ));
        fs::remove_file(head_path).unwrap();
        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn child_creation_rejects_stale_parent_source_before_writing() {
        let parent_path = path("parent-source");
        let child_path = path("stale-child-head");
        let wal_path = path("stale-child-wal");
        let parent = sample();
        fs::write(&parent_path, parent.encode().unwrap()).unwrap();
        let request = ChildBranchHeadRequest {
            project_id: parent.project_id,
            branch_id: [7; 16],
            sealed_root: parent.sealed_root,
            logical_commit_epoch: parent.logical_commit_epoch,
            active_wal_generation: 1,
            replay_start_lsn: 99,
            head_path: child_path.clone(),
            wal_path: wal_path.clone(),
        };
        let mut stale = ChildBranchSourceExpectation {
            branch_id: parent.branch_id,
            physical_generation: parent.physical_generation,
            logical_commit_epoch: parent.logical_commit_epoch,
            sealed_root: parent.sealed_root,
        };
        stale.logical_commit_epoch += 1;
        assert!(matches!(
            create_child_branch_head_from_parent(
                &parent_path,
                &child_path,
                request.clone(),
                stale,
                1024,
            ),
            Err(BranchHeadError::ParentSourceMismatch)
        ));
        stale.logical_commit_epoch = parent.logical_commit_epoch;
        stale.branch_id = [8; 16];
        assert!(matches!(
            create_child_branch_head_from_parent(&parent_path, &child_path, request, stale, 1024,),
            Err(BranchHeadError::BranchIdentityMismatch)
        ));
        assert!(!child_path.exists());
        assert!(!wal_path.exists());
        assert_eq!(read_branch_head(&parent_path).unwrap(), parent);
        fs::remove_file(parent_path).unwrap();
    }

    #[test]
    fn prepared_rotation_switches_head_only_after_successor_identity_is_read() {
        let head_path = path("rotation-head");
        let next_path = path("rotation-next");
        let old = sample();
        fs::write(&head_path, old.encode().unwrap()).unwrap();
        let successor = b"durable-successor-header";
        fs::write(&next_path, successor).unwrap();
        let prepared = PreparedWalRotation {
            sealed: SealedWalPublication {
                generation: old.active_wal.generation,
                start_lsn: old.active_wal.replay_start_lsn,
                end_lsn: 42,
                object: ObjectReference::for_bytes(ObjectKind::SealedWal, 1, b"sealed"),
                outcome: PublishOutcome::Published,
            },
            next_generation: old.active_wal.generation + 1,
            next_start_lsn: 42,
            next_wal_path: next_path.clone(),
        };
        let next = publish_prepared_wal_rotation(
            &head_path,
            PreparedWalRotationHeadRequest {
                expected_current_generation: old.physical_generation,
                project_id: old.project_id,
                branch_id: old.branch_id,
                sealed_root: old.sealed_root,
                logical_commit_epoch: old.logical_commit_epoch + 1,
                prepared: &prepared,
                max_active_wal_bytes: 1024,
            },
        )
        .unwrap();
        assert_eq!(next.physical_generation, old.physical_generation + 1);
        assert_eq!(next.active_wal.sha256, hawdb_integrity::sha256(successor));
        assert_eq!(read_branch_head(&head_path).unwrap(), next);
        fs::remove_file(head_path).unwrap();
        fs::remove_file(next_path).unwrap();
    }

    #[test]
    fn complete_rotation_publishes_root_before_switching_head() {
        let head_path = path("complete-rotation-head");
        let next_path = path("complete-rotation-next");
        let object_root = directory("complete-rotation-objects");
        let old = sample();
        fs::write(&head_path, old.encode().unwrap()).unwrap();
        let successor = b"durable-successor-header";
        fs::write(&next_path, successor).unwrap();
        let mut objects = ImmutableObjectStore::open(&object_root).unwrap();
        let checkpoint = b"checkpoint";
        let checkpoint_ref = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, checkpoint);
        objects.publish(checkpoint_ref, checkpoint).unwrap();
        let sealed = ObjectReference::for_bytes(ObjectKind::SealedWal, 1, b"sealed");
        let manifest = ObjectReference::for_bytes(ObjectKind::DurableManifest, 1, b"manifest");
        let prepared = PreparedWalRotation {
            sealed: SealedWalPublication {
                generation: old.active_wal.generation,
                start_lsn: old.active_wal.replay_start_lsn,
                end_lsn: 42,
                object: sealed,
                outcome: PublishOutcome::Published,
            },
            next_generation: old.active_wal.generation + 1,
            next_start_lsn: 42,
            next_wal_path: next_path.clone(),
        };
        let root = SealedRoot {
            checkpoint_epoch: 9,
            commit_epoch: 10,
            wal_replay_start_lsn: 20,
            durable_manifest: manifest,
            checkpoint_references: vec![checkpoint_ref],
            checkpoint_bindings: vec![CheckpointArtifactBinding {
                relative_path: "checkpoint.hawdb".to_string(),
                reference: checkpoint_ref,
            }],
            sealed_wals: vec![SealedWalReference {
                start_lsn: 20,
                end_lsn: 42,
                object: sealed,
            }],
        };
        let next = publish_prepared_wal_rotation_with_root(
            &head_path,
            PreparedWalRootPublicationRequest {
                expected_current_generation: old.physical_generation,
                project_id: old.project_id,
                branch_id: old.branch_id,
                logical_commit_epoch: 10,
                prepared: &prepared,
                max_active_wal_bytes: 1024,
            },
            &root,
            &mut objects,
        )
        .unwrap();
        assert_eq!(next.sealed_root, root.object_reference().unwrap());
        assert_eq!(read_branch_head(&head_path).unwrap(), next);
        assert!(objects.object_path(next.sealed_root).exists());
        fs::remove_file(head_path).unwrap();
        fs::remove_file(next_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn complete_rotation_rejects_root_omitting_prepared_wal() {
        let head_path = path("missing-wal-head");
        let next_path = path("missing-wal-next");
        let object_root = directory("missing-wal-objects");
        let old = sample();
        fs::write(&head_path, old.encode().unwrap()).unwrap();
        fs::write(&next_path, b"successor").unwrap();
        let mut objects = ImmutableObjectStore::open(&object_root).unwrap();
        let checkpoint_ref = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, b"checkpoint");
        let manifest = ObjectReference::for_bytes(ObjectKind::DurableManifest, 1, b"manifest");
        objects.publish(checkpoint_ref, b"checkpoint").unwrap();
        let prepared = PreparedWalRotation {
            sealed: SealedWalPublication {
                generation: old.active_wal.generation,
                start_lsn: old.active_wal.replay_start_lsn,
                end_lsn: 42,
                object: ObjectReference::for_bytes(ObjectKind::SealedWal, 1, b"sealed"),
                outcome: PublishOutcome::Published,
            },
            next_generation: old.active_wal.generation + 1,
            next_start_lsn: 42,
            next_wal_path: next_path.clone(),
        };
        let root = SealedRoot {
            checkpoint_epoch: 9,
            commit_epoch: 10,
            wal_replay_start_lsn: 20,
            durable_manifest: manifest,
            checkpoint_references: vec![checkpoint_ref],
            checkpoint_bindings: vec![CheckpointArtifactBinding {
                relative_path: "checkpoint.hawdb".to_string(),
                reference: checkpoint_ref,
            }],
            sealed_wals: Vec::new(),
        };
        assert!(matches!(
            publish_prepared_wal_rotation_with_root(
                &head_path,
                PreparedWalRootPublicationRequest {
                    expected_current_generation: old.physical_generation,
                    project_id: old.project_id,
                    branch_id: old.branch_id,
                    logical_commit_epoch: 10,
                    prepared: &prepared,
                    max_active_wal_bytes: 1024,
                },
                &root,
                &mut objects,
            ),
            Err(WalRotationPublicationError::SealedWalMissing)
        ));
        assert_eq!(read_branch_head(&head_path).unwrap(), old);
        assert!(!object_root.join("objects").join("sealed-root").exists());
        fs::remove_file(head_path).unwrap();
        fs::remove_file(next_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }
}
