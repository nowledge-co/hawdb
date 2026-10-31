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

//! Content-addressed immutable object publication.
//!
//! Objects are first written to an operation-owned staging file.  The file is
//! synced and read back before an exclusive hard-link installation.  Existing
//! objects are accepted only when their complete bytes match the requested
//! reference; an immutable path is never replaced.

use crate::file_io::{self as fs, File, OpenOptions};
use hawdb_integrity::{IntegrityHasher, Sha256Digest};
use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const OBJECTS_DIRECTORY: &str = "objects";
const STAGING_DIRECTORY: &str = ".staging";
const IDENTITY_DOMAIN: &[u8] = b"HAWDB-IMMUTABLE-OBJECT-V1\0";
const DEFAULT_MAX_OBJECT_BYTES: u64 = 128 * 1024 * 1024;
/// Checkpoint data and artifacts are only ever streamed (published, verified
/// and mounted in bounded buffers), so their size is bounded by the
/// checkpoint encoding limit rather than by the in-memory object bound.
const DEFAULT_MAX_STREAMED_OBJECT_BYTES: u64 = crate::config::DEFAULT_MAX_CHECKPOINT_ENCODED_BYTES;
const STREAM_BUFFER_BYTES: usize = 64 * 1024;

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ObjectKind {
    Checkpoint = 1,
    SealedWal = 2,
    SealedRoot = 3,
    CheckpointArtifact = 4,
    DurableManifest = 5,
}

impl ObjectKind {
    pub(crate) const fn current_format_version(self) -> u16 {
        match self {
            Self::SealedRoot => 2,
            Self::Checkpoint
            | Self::SealedWal
            | Self::CheckpointArtifact
            | Self::DurableManifest => 1,
        }
    }

    const fn directory(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::SealedWal => "sealed-wal",
            Self::SealedRoot => "sealed-root",
            Self::CheckpointArtifact => "checkpoint-artifact",
            Self::DurableManifest => "durable-manifest",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectReference {
    pub kind: ObjectKind,
    pub format_version: u16,
    pub byte_length: u64,
    pub sha256: Sha256Digest,
}

impl ObjectReference {
    pub fn for_bytes(kind: ObjectKind, format_version: u16, bytes: &[u8]) -> Self {
        Self {
            kind,
            format_version,
            byte_length: bytes.len() as u64,
            sha256: identity_digest(kind, format_version, bytes),
        }
    }

    fn path_component(self) -> String {
        self.sha256.to_string()
    }
}

/// Streaming kind/version/length-separated identity, with no payload buffer.
/// A description only: callers own memory/I/O admission, immutable source
/// authority, synchronization and publication. `flush` performs no I/O.
pub struct ObjectReferenceWriter {
    kind: ObjectKind,
    format_version: u16,
    byte_length: u64,
    written: u64,
    hasher: IntegrityHasher,
}
impl ObjectReferenceWriter {
    pub fn new(
        kind: ObjectKind,
        format_version: u16,
        byte_length: u64,
    ) -> Result<Self, ImmutableObjectError> {
        if format_version == 0 {
            return Err(ImmutableObjectError::InvalidFormatVersion);
        }
        Ok(Self {
            kind,
            format_version,
            byte_length,
            written: 0,
            hasher: identity_hasher(kind, format_version, byte_length),
        })
    }
    pub fn finish(self) -> Result<ObjectReference, ImmutableObjectError> {
        if self.written != self.byte_length {
            return Err(ImmutableObjectError::ReferenceMismatch);
        }
        Ok(ObjectReference {
            kind: self.kind,
            format_version: self.format_version,
            byte_length: self.byte_length,
            sha256: self.hasher.finish().sha256,
        })
    }
}
impl Write for ObjectReferenceWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .written
            .checked_add(bytes.len() as u64)
            .filter(|next| *next <= self.byte_length)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "object exceeds declared identity length",
                )
            })?;
        self.hasher.update(bytes);
        self.written = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    Published,
    Reused,
}

#[derive(Debug)]
pub enum ImmutableObjectError {
    InvalidFormatVersion,
    ObjectTooLarge {
        length: u64,
        limit: u64,
    },
    PublisherPoisoned,
    ReferenceMismatch,
    ExistingObjectCorrupt {
        path: PathBuf,
    },
    Io {
        operation: &'static str,
        source: io::Error,
    },
    PublicationUncertain {
        source: io::Error,
    },
    InvalidSealedRoot {
        source: crate::sealed_root::SealedRootError,
    },
    ReclamationRoot {
        root: ObjectReference,
        source: Box<ImmutableObjectError>,
    },
    BranchMetadataIncomplete(&'static str),
}

impl Display for ImmutableObjectError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormatVersion => {
                formatter.write_str("immutable object format version is zero")
            }
            Self::ObjectTooLarge { length, limit } => {
                write!(
                    formatter,
                    "immutable object length {length} exceeds limit {limit}"
                )
            }
            Self::PublisherPoisoned => {
                formatter.write_str("immutable object publisher is poisoned; reopen it")
            }
            Self::ReferenceMismatch => {
                formatter.write_str("object reference does not match payload")
            }
            Self::ExistingObjectCorrupt { path } => {
                write!(
                    formatter,
                    "existing immutable object is corrupt: {}",
                    path.display()
                )
            }
            Self::Io { operation, source } => write!(formatter, "{operation}: {source}"),
            Self::PublicationUncertain { source } => {
                write!(
                    formatter,
                    "immutable object publication is uncertain: {source}"
                )
            }
            Self::InvalidSealedRoot { source } => {
                write!(
                    formatter,
                    "sealed root is invalid for reachability: {source}"
                )
            }
            Self::ReclamationRoot { root, source } => write!(
                formatter,
                "reclamation root {} cannot be verified: {source}",
                root.sha256
            ),
            Self::BranchMetadataIncomplete(message) => {
                write!(
                    formatter,
                    "branch reclamation metadata is incomplete: {message}"
                )
            }
        }
    }
}

impl std::error::Error for ImmutableObjectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } | Self::PublicationUncertain { source } => Some(source),
            Self::InvalidSealedRoot { source } => Some(source),
            Self::ReclamationRoot { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

/// The result of one conservative immutable-object sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclamationReport {
    pub retained_objects: u64,
    pub reclaimed_objects: u64,
    pub reclaimed_bytes: u64,
    pub reclaimed_branch_directories: u64,
    /// Active owners may hold unpublished roots or reader generations.
    pub deferred_for_active_leases: bool,
}

/// Durable branch metadata supplied by the catalog/lease owner to a global
/// reclamation pass. The collector never infers branch liveness from files.
#[derive(Debug, Clone)]
pub struct BranchReclamationEntry {
    pub state: crate::branch_catalog::BranchState,
    pub sealed_root: Option<ObjectReference>,
    pub directory: PathBuf,
    pub active_lease: bool,
}

/// Explicit inventory for a branch-aware immutable-object sweep.
#[derive(Debug, Clone, Default)]
pub struct BranchReclamationInventory {
    pub objects: Vec<ObjectReference>,
    pub branches: Vec<BranchReclamationEntry>,
}

pub(crate) struct ImmutableObjectInventory {
    pub objects: Vec<ObjectReference>,
    pub staging: Vec<(PathBuf, u64)>,
}

/// Owns the immutable-object namespace for one project.
#[derive(Debug)]
pub struct ImmutableObjectStore {
    root: PathBuf,
    /// Bound for objects that may be read whole into memory.
    max_object_bytes: u64,
    /// Bound for checkpoint objects, which are only streamed.
    max_streamed_object_bytes: u64,
    poisoned: bool,
    synchronized_kinds: BTreeSet<ObjectKind>,
    namespace_synchronized: bool,
}

impl ImmutableObjectStore {
    pub(crate) fn open_existing(root: PathBuf, max_object_bytes: u64) -> Self {
        Self {
            root,
            max_object_bytes,
            max_streamed_object_bytes: max_object_bytes.max(DEFAULT_MAX_STREAMED_OBJECT_BYTES),
            poisoned: false,
            synchronized_kinds: BTreeSet::new(),
            namespace_synchronized: false,
        }
    }

    pub fn open(root: impl Into<PathBuf>) -> Result<Self, ImmutableObjectError> {
        Self::open_with_limits(
            root,
            DEFAULT_MAX_OBJECT_BYTES,
            DEFAULT_MAX_STREAMED_OBJECT_BYTES,
        )
    }

    /// Opens with one bound for every object kind.
    pub fn open_with_limit(
        root: impl Into<PathBuf>,
        max_object_bytes: u64,
    ) -> Result<Self, ImmutableObjectError> {
        Self::open_with_limits(root, max_object_bytes, max_object_bytes)
    }

    /// Opens with separate bounds for objects that may be read into memory
    /// and for checkpoint objects, which are only streamed.
    pub fn open_with_limits(
        root: impl Into<PathBuf>,
        max_object_bytes: u64,
        max_streamed_object_bytes: u64,
    ) -> Result<Self, ImmutableObjectError> {
        if max_object_bytes == 0 || max_streamed_object_bytes == 0 {
            return Err(ImmutableObjectError::ObjectTooLarge {
                length: 1,
                limit: max_object_bytes,
            });
        }
        let root = root.into();
        let objects = root.join(OBJECTS_DIRECTORY);
        map_io(
            "create immutable object directories",
            fs::create_dir_all(&objects),
        )?;
        map_io(
            "create immutable object staging directory",
            fs::create_dir_all(objects.join(STAGING_DIRECTORY)),
        )?;
        Ok(Self {
            root,
            max_object_bytes,
            max_streamed_object_bytes,
            poisoned: false,
            synchronized_kinds: BTreeSet::new(),
            namespace_synchronized: false,
        })
    }

    /// The size bound for an object of `kind` handled through streaming
    /// paths. Buffered APIs (`publish`, `read`) always use the in-memory bound.
    fn streamed_limit(&self, kind: ObjectKind) -> u64 {
        match kind {
            ObjectKind::Checkpoint | ObjectKind::CheckpointArtifact => {
                self.max_streamed_object_bytes
            }
            ObjectKind::SealedWal | ObjectKind::SealedRoot | ObjectKind::DurableManifest => {
                self.max_object_bytes
            }
        }
    }

    /// Buffered payloads keep the in-memory bound; file-backed (streamed)
    /// publication uses the per-kind streamed bound.
    fn payload_limit(&self, reference: ObjectReference, payload: Option<&[u8]>) -> u64 {
        match payload {
            Some(_) => self.max_object_bytes,
            None => self.streamed_limit(reference.kind),
        }
    }

    /// Verifies a stored object by streaming its identity, without reading it
    /// into memory.
    pub fn verify_stored(&self, reference: ObjectReference) -> Result<(), ImmutableObjectError> {
        self.verify(reference)
    }

    pub fn object_path(&self, reference: ObjectReference) -> PathBuf {
        self.root
            .join(OBJECTS_DIRECTORY)
            .join(reference.kind.directory())
            .join(reference.path_component())
    }

    /// Inventory only the currently supported immutable namespace. Unknown
    /// versions, names and non-files fail closed before any sweep can start.
    pub(crate) fn inventory(
        &self,
        max_objects: usize,
        max_bytes: u64,
    ) -> io::Result<ImmutableObjectInventory> {
        let mut objects = Vec::new();
        let mut staging = Vec::new();
        let mut total_bytes = 0u64;
        let directories = [
            ObjectKind::Checkpoint,
            ObjectKind::SealedWal,
            ObjectKind::SealedRoot,
            ObjectKind::CheckpointArtifact,
            ObjectKind::DurableManifest,
        ]
        .into_iter()
        .map(|kind| (kind.directory(), Some(kind)))
        .chain([(STAGING_DIRECTORY, None)]);
        for (name, kind) in directories {
            let directory = self.root.join(OBJECTS_DIRECTORY).join(name);
            match fs::symlink_metadata(&directory) {
                Ok(metadata) if metadata.is_dir() => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
                Ok(_) => return Err(io::Error::other("invalid immutable object directory")),
            }
            for entry in fs::read_dir(&directory)? {
                let entry = entry?;
                if objects.len() + staging.len() >= max_objects {
                    return Err(io::Error::other("branch reclamation object limit exceeded"));
                }
                let metadata = fs::symlink_metadata(entry.path())?;
                if !metadata.is_file() {
                    return Err(io::Error::other("invalid immutable object inventory entry"));
                }
                total_bytes = total_bytes
                    .checked_add(metadata.len())
                    .filter(|bytes| *bytes <= max_bytes)
                    .ok_or_else(|| io::Error::other("branch reclamation byte limit exceeded"))?;
                let name = entry.file_name();
                let name = name.to_str().ok_or_else(|| {
                    io::Error::other("immutable object name is not a canonical digest")
                })?;
                let Some(kind) = kind else {
                    let valid = name
                        .strip_suffix(".stage")
                        .and_then(|name| name.split_once('-'))
                        .is_some_and(|(pid, sequence)| {
                            !pid.is_empty()
                                && !sequence.is_empty()
                                && pid.bytes().all(|byte| byte.is_ascii_digit())
                                && sequence.bytes().all(|byte| byte.is_ascii_digit())
                        });
                    if !valid {
                        return Err(io::Error::other("unrecognized immutable staging entry"));
                    }
                    staging.push((entry.path(), metadata.len()));
                    continue;
                };
                let sha256: Sha256Digest = name.parse().map_err(io::Error::other)?;
                if sha256.to_string() != name {
                    return Err(io::Error::other("immutable object digest is not canonical"));
                }
                objects.push(ObjectReference {
                    kind,
                    format_version: kind.current_format_version(),
                    byte_length: metadata.len(),
                    sha256,
                });
            }
        }
        Ok(ImmutableObjectInventory { objects, staging })
    }

    /// Reads an immutable object after validating its complete reference.
    /// Reopen paths use this instead of trusting an object filename or length.
    pub fn read(&self, reference: ObjectReference) -> Result<Vec<u8>, ImmutableObjectError> {
        let path = self.object_path(reference);
        let metadata = map_io(
            "read immutable object metadata",
            fs::symlink_metadata(&path),
        )?;
        if !metadata.file_type().is_file()
            || metadata.len() != reference.byte_length
            || metadata.len() > self.max_object_bytes
        {
            return Err(ImmutableObjectError::ExistingObjectCorrupt { path });
        }
        let capacity = usize::try_from(metadata.len())
            .map_err(|_| ImmutableObjectError::ExistingObjectCorrupt { path: path.clone() })?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut file = map_io("open immutable object", File::open(&path))?;
        file.read_to_end(&mut bytes)
            .map_err(|source| ImmutableObjectError::Io {
                operation: "read immutable object",
                source,
            })?;
        if bytes.len() as u64 != metadata.len()
            || validate_reference(reference, &bytes, self.max_object_bytes).is_err()
        {
            return Err(ImmutableObjectError::ExistingObjectCorrupt { path });
        }
        Ok(bytes)
    }

    /// Authenticate a recovery dependency with bounded memory before making
    /// its bytes and namespace durable. The caller holds its branch lease.
    pub(crate) fn verify_and_sync(
        &self,
        reference: ObjectReference,
    ) -> Result<(), ImmutableObjectError> {
        self.verify_object(reference, true)
    }

    fn verify(&self, reference: ObjectReference) -> Result<(), ImmutableObjectError> {
        self.verify_object(reference, false)
    }

    fn verify_object(
        &self,
        reference: ObjectReference,
        synchronize: bool,
    ) -> Result<(), ImmutableObjectError> {
        let path = self.object_path(reference);
        let metadata = map_io(
            "inspect immutable recovery dependency",
            fs::symlink_metadata(&path),
        )?;
        if !metadata.is_file()
            || metadata.len() != reference.byte_length
            || metadata.len() > self.streamed_limit(reference.kind)
            || reference.format_version == 0
        {
            return Err(ImmutableObjectError::ExistingObjectCorrupt { path });
        }
        // Windows requires write access for FlushFileBuffers. Never create or
        // truncate a missing dependency while recovering an existing receipt.
        let mut file = map_io(
            "open immutable recovery dependency",
            OpenOptions::new()
                .read(true)
                .write(synchronize && cfg!(windows))
                .open(&path),
        )?;
        let mut hasher = identity_hasher(
            reference.kind,
            reference.format_version,
            reference.byte_length,
        );
        let mut remaining = reference.byte_length;
        let mut buffer = [0; 64 * 1024];
        while remaining != 0 {
            let length = usize::try_from(remaining.min(buffer.len() as u64)).expect("buffer width");
            map_io(
                "read immutable recovery dependency",
                file.read_exact(&mut buffer[..length]),
            )?;
            hasher.update(&buffer[..length]);
            remaining -= length as u64;
        }
        if hasher.finish().sha256 != reference.sha256
            || map_io("recheck immutable recovery dependency", file.metadata())?.len()
                != reference.byte_length
        {
            return Err(ImmutableObjectError::ExistingObjectCorrupt { path });
        }
        if synchronize {
            map_io("sync immutable recovery dependency", file.sync_all())?;
            drop(file);
            map_io(
                "sync immutable recovery namespace",
                crate::durability::sync_directory_ancestors(
                    path.parent().expect("object kind directory"),
                ),
            )?;
        }
        Ok(())
    }

    /// Reclaims only explicitly inventoried objects that are unreachable from
    /// the supplied sealed roots.
    ///
    /// The inventory is an ownership boundary supplied by the catalog/lease
    /// layer. Unknown files are never scanned or deleted. Every inventoried
    /// object is fully verified before the first unlink, and sealed roots are
    /// decoded to mark their checkpoint and WAL closure. Any unreadable or
    /// malformed input fails the operation before sweeping begins, so an
    /// incomplete catalog or interrupted mark phase retains all candidates.
    pub fn reclaim_unreachable(
        &mut self,
        inventory: &[ObjectReference],
        roots: &[ObjectReference],
    ) -> Result<ReclamationReport, ImmutableObjectError> {
        self.reclaim_unreachable_with_evidence(inventory, roots, &Default::default())
    }

    fn reclaim_unreachable_with_evidence(
        &mut self,
        inventory: &[ObjectReference],
        roots: &[ObjectReference],
        evidence: &crate::branch_abandonment::QuarantinedObjects,
    ) -> Result<ReclamationReport, ImmutableObjectError> {
        if self.poisoned {
            return Err(ImmutableObjectError::PublisherPoisoned);
        }

        let inventory: BTreeSet<_> = inventory.iter().copied().collect();
        let mut reachable = BTreeSet::new();
        // Keep the originating root for a failed dependency without changing
        // the complete-mark-before-sweep boundary or duplicating validation.
        let mut pending: Vec<_> = roots.iter().copied().enumerate().collect();
        while let Some((root_index, reference)) = pending.pop() {
            if !reachable.insert(reference) {
                continue;
            }
            let validation = (|| {
                if reference.kind == ObjectKind::SealedRoot {
                    let bytes = self.read(reference)?;
                    let sealed = crate::sealed_root::SealedRoot::decode(&bytes)
                        .map_err(|source| ImmutableObjectError::InvalidSealedRoot { source })?;
                    pending.push((root_index, sealed.durable_manifest));
                    pending.extend(
                        sealed
                            .checkpoint_references
                            .into_iter()
                            .map(|r| (root_index, r)),
                    );
                    pending.extend(
                        sealed
                            .sealed_wals
                            .into_iter()
                            .map(|wal| (root_index, wal.object)),
                    );
                } else {
                    self.verify(reference)?;
                }
                Ok(())
            })();
            validation.map_err(|source| ImmutableObjectError::ReclamationRoot {
                root: roots[root_index],
                source: Box::new(source),
            })?;
        }

        // Verify the complete caller-owned inventory before deleting anything.
        // A missing unreachable candidate is an idempotent remnant of an
        // earlier sweep; a missing reachable object remains fatal.
        for reference in &inventory {
            match self.verify(*reference) {
                Ok(()) => {}
                Err(ImmutableObjectError::Io { source, .. })
                    if source.kind() == io::ErrorKind::NotFound
                        && !reachable.contains(reference) => {}
                Err(error @ ImmutableObjectError::ExistingObjectCorrupt { .. })
                | Err(error @ ImmutableObjectError::ReferenceMismatch)
                    if !reachable.contains(reference) =>
                {
                    let preserved =
                        evidence
                            .preserves(&self.object_path(*reference))
                            .map_err(|source| ImmutableObjectError::Io {
                                operation: "verify quarantined source before reclamation",
                                source: io::Error::other(source),
                            })?;
                    if !preserved {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }

        let mut report = ReclamationReport {
            retained_objects: inventory.intersection(&reachable).count() as u64,
            reclaimed_objects: 0,
            reclaimed_bytes: 0,
            reclaimed_branch_directories: 0,
            deferred_for_active_leases: false,
        };
        for reference in inventory.difference(&reachable) {
            let path = self.object_path(*reference);
            let context = map_io(
                "admit unreachable object cache retirement",
                crate::file_descriptors::context_for_path(&path),
            )?;
            if let Some(handles) = context.state.existing_immutable_handles() {
                map_io(
                    "retire unreachable object cache",
                    handles.retire_unreachable(*reference),
                )?;
            }
            match fs::remove_file(&path) {
                Ok(()) => {
                    crate::durability::sync_parent_directory(&path).map_err(|source| {
                        ImmutableObjectError::Io {
                            operation: "sync immutable object directory after reclamation",
                            source,
                        }
                    })?;
                }
                Err(source) if source.kind() == io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(ImmutableObjectError::Io {
                        operation: "remove unreachable immutable object",
                        source,
                    });
                }
            }
            report.reclaimed_objects += 1;
            report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(reference.byte_length);
        }
        Ok(report)
    }

    /// Reclaims immutable objects and completed branch directories using a
    /// catalog/lease-owned inventory. Any branch whose root cannot be proven
    /// is retained conservatively and prevents the sweep.
    pub fn reclaim_branches(
        &mut self,
        inventory: &BranchReclamationInventory,
    ) -> Result<ReclamationReport, ImmutableObjectError> {
        self.reclaim_branches_with_roots(inventory, &[])
    }

    pub(crate) fn reclaim_branches_with_roots(
        &mut self,
        inventory: &BranchReclamationInventory,
        retained_roots: &[ObjectReference],
    ) -> Result<ReclamationReport, ImmutableObjectError> {
        self.reclaim_branches_with_evidence(inventory, retained_roots, &Default::default())
    }

    pub(crate) fn reclaim_branches_with_evidence(
        &mut self,
        inventory: &BranchReclamationInventory,
        retained_roots: &[ObjectReference],
        evidence: &crate::branch_abandonment::QuarantinedObjects,
    ) -> Result<ReclamationReport, ImmutableObjectError> {
        let mut roots = retained_roots.to_vec();
        for branch in &inventory.branches {
            // A leased creator can still be preparing its first head. The
            // sweep below defers without trusting that incomplete metadata.
            if branch.active_lease {
                continue;
            }
            let removable = matches!(branch.state, crate::branch_catalog::BranchState::Deleted);
            if !removable {
                roots.push(branch.sealed_root.ok_or(
                    ImmutableObjectError::BranchMetadataIncomplete(
                        "live or recovery-pending branch has no sealed root",
                    ),
                )?);
            }
        }
        if inventory.branches.iter().any(|branch| branch.active_lease) {
            // A head cannot describe unpublished candidates or old reader
            // pins, so retain them while any owner is active. Unleased
            // records above still require complete root metadata.
            // Registered Ready owners supply their exact publication and
            // historical-reader roots separately. An unregistered owner still
            // requires conservative deferral; its head alone is insufficient.
            return Ok(ReclamationReport {
                retained_objects: inventory.objects.len() as u64,
                reclaimed_objects: 0,
                reclaimed_bytes: 0,
                reclaimed_branch_directories: 0,
                deferred_for_active_leases: true,
            });
        }
        let mut report =
            self.reclaim_unreachable_with_evidence(&inventory.objects, &roots, evidence)?;
        for branch in &inventory.branches {
            if matches!(branch.state, crate::branch_catalog::BranchState::Deleted)
                && !branch.active_lease
            {
                // A reclamation owner can remove namespace paths. Revalidate
                // kind names before any subsequent publication by this store.
                self.synchronized_kinds.clear();
                self.namespace_synchronized = false;
                if reclaim_deleted_directory(&branch.directory)? {
                    report.reclaimed_branch_directories += 1;
                }
            }
        }
        Ok(report)
    }

    pub fn publish(
        &mut self,
        reference: ObjectReference,
        payload: &[u8],
    ) -> Result<PublishOutcome, ImmutableObjectError> {
        if self.poisoned {
            return Err(ImmutableObjectError::PublisherPoisoned);
        }
        validate_reference(reference, payload, self.max_object_bytes)?;
        let destination = self.prepare_kind_directory(reference)?;
        if destination.exists() {
            return self.validate_existing(reference, Some(payload), &destination);
        }

        let staging = self.stage_payload(payload)?;
        let install_result = self.install_staged(reference, Some(payload), &staging, &destination);
        if install_result.is_err() {
            let _ = fs::remove_file(&staging);
        }
        install_result
    }

    /// Publishes a file-backed payload without buffering it. The source is
    /// streamed into staging while its identity is recomputed, and both the
    /// staged read-back and reuse of an existing object are authenticated by
    /// streaming identity checks, so memory stays bounded by one I/O buffer
    /// regardless of the object size.
    pub fn publish_file(
        &mut self,
        reference: ObjectReference,
        source: &Path,
    ) -> Result<PublishOutcome, ImmutableObjectError> {
        if self.poisoned {
            return Err(ImmutableObjectError::PublisherPoisoned);
        }
        if reference.format_version == 0 {
            return Err(ImmutableObjectError::InvalidFormatVersion);
        }
        if reference.byte_length > self.streamed_limit(reference.kind) {
            return Err(ImmutableObjectError::ObjectTooLarge {
                length: reference.byte_length,
                limit: self.streamed_limit(reference.kind),
            });
        }
        let destination = self.prepare_kind_directory(reference)?;
        if destination.exists() {
            return self.validate_existing(reference, None, &destination);
        }

        let staging = self.stage_file(reference, source)?;
        let install_result = self.install_staged(reference, None, &staging, &destination);
        if install_result.is_err() {
            let _ = fs::remove_file(&staging);
        }
        install_result
    }

    /// Establishes the object namespace and kind directory name barriers and
    /// returns the content-addressed destination path.
    fn prepare_kind_directory(
        &mut self,
        reference: ObjectReference,
    ) -> Result<PathBuf, ImmutableObjectError> {
        if !self.namespace_synchronized {
            // Opening a reader is not a publication boundary. Establish all
            // directory names before the first acknowledged publication, and
            // retry the whole ancestry if a previous barrier was uncertain.
            map_io(
                "sync immutable object directory ancestry",
                crate::durability::sync_directory_ancestors(
                    &self.root.join(OBJECTS_DIRECTORY).join(STAGING_DIRECTORY),
                ),
            )?;
            self.namespace_synchronized = true;
        }
        let destination = self.object_path(reference);
        let object_directory = destination
            .parent()
            .expect("object path always has an object-kind parent");
        if !self.synchronized_kinds.contains(&reference.kind) {
            map_io(
                "create immutable object kind directory",
                fs::create_dir_all(object_directory),
            )?;
            map_io(
                "sync immutable object kind directory name",
                crate::durability::sync_parent_directory(object_directory),
            )?;
            // Only cache a completed name barrier. Known directories are never
            // recreated here: an unexpectedly missing directory fails closed.
            // The set has at most the five supported object kinds. Each object's
            // bytes and final entry still get their own publication barriers.
            self.synchronized_kinds.insert(reference.kind);
        }
        Ok(destination)
    }

    fn stage_payload(&self, payload: &[u8]) -> Result<PathBuf, ImmutableObjectError> {
        let staging_directory = self.root.join(OBJECTS_DIRECTORY).join(STAGING_DIRECTORY);
        let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = staging_directory.join(format!("{}-{sequence}.stage", std::process::id()));
        let mut file = map_io(
            "create immutable object staging file",
            OpenOptions::new().write(true).create_new(true).open(&path),
        )?;
        if let Err(error) = file.write_all(payload).and_then(|_| file.sync_all()) {
            let _ = fs::remove_file(&path);
            return Err(ImmutableObjectError::Io {
                operation: "write and sync immutable object staging file",
                source: error,
            });
        }
        Ok(path)
    }

    fn stage_file(
        &self,
        reference: ObjectReference,
        source: &Path,
    ) -> Result<PathBuf, ImmutableObjectError> {
        let mut input = map_io("open immutable object source", File::open(source))?;
        let staging_directory = self.root.join(OBJECTS_DIRECTORY).join(STAGING_DIRECTORY);
        let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = staging_directory.join(format!("{}-{sequence}.stage", std::process::id()));
        let mut file = map_io(
            "create immutable object staging file",
            OpenOptions::new().write(true).create_new(true).open(&path),
        )?;
        let copied = (|| -> Result<(), ImmutableObjectError> {
            let mut identity = ObjectReferenceWriter::new(
                reference.kind,
                reference.format_version,
                reference.byte_length,
            )?;
            let mut buffer = vec![0; STREAM_BUFFER_BYTES];
            loop {
                let length = map_io("read immutable object source", input.read(&mut buffer))?;
                if length == 0 {
                    break;
                }
                // A source longer than its reference fails here instead of
                // growing the staged object past the admitted length.
                identity
                    .write_all(&buffer[..length])
                    .map_err(|_| ImmutableObjectError::ReferenceMismatch)?;
                map_io(
                    "write immutable object staging file",
                    file.write_all(&buffer[..length]),
                )?;
            }
            if identity.finish()? != reference {
                return Err(ImmutableObjectError::ReferenceMismatch);
            }
            map_io("sync immutable object staging file", file.sync_all())
        })();
        if let Err(error) = copied {
            drop(file);
            let _ = fs::remove_file(&path);
            return Err(error);
        }
        Ok(path)
    }

    fn install_staged(
        &mut self,
        reference: ObjectReference,
        payload: Option<&[u8]>,
        staging: &Path,
        destination: &Path,
    ) -> Result<PublishOutcome, ImmutableObjectError> {
        validate_file(
            staging,
            reference,
            payload,
            self.payload_limit(reference, payload),
        )?;
        match fs::hard_link(staging, destination) {
            Ok(()) => {
                if let Err(error) = fs::remove_file(staging) {
                    self.poisoned = true;
                    return Err(ImmutableObjectError::PublicationUncertain { source: error });
                }
                if let Err(error) = sync_publication_directories(destination, staging.parent()) {
                    self.poisoned = true;
                    return Err(ImmutableObjectError::PublicationUncertain { source: error });
                }
                Ok(PublishOutcome::Published)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                self.validate_existing(reference, payload, destination)
            }
            Err(error) => Err(ImmutableObjectError::Io {
                operation: "install immutable object exclusively",
                source: error,
            }),
        }
    }

    fn validate_existing(
        &mut self,
        reference: ObjectReference,
        payload: Option<&[u8]>,
        path: &Path,
    ) -> Result<PublishOutcome, ImmutableObjectError> {
        let metadata = map_io("read immutable object metadata", fs::symlink_metadata(path))?;
        if !metadata.file_type().is_file() {
            return Err(ImmutableObjectError::ExistingObjectCorrupt {
                path: path.to_path_buf(),
            });
        }
        validate_file(
            path,
            reference,
            payload,
            self.payload_limit(reference, payload),
        )?;
        // A previous publisher may have installed complete bytes but failed its
        // final namespace barrier. Acknowledging reuse must close that window.
        if let Err(source) = sync_publication_directories(path, None) {
            self.poisoned = true;
            return Err(ImmutableObjectError::PublicationUncertain { source });
        }
        Ok(PublishOutcome::Reused)
    }
}

fn reclaim_deleted_directory(directory: &Path) -> Result<bool, ImmutableObjectError> {
    use crate::ownership::DatabaseDirectoryLease;

    let name = directory.file_name().and_then(|name| name.to_str()).ok_or(
        ImmutableObjectError::BranchMetadataIncomplete(
            "deleted branch directory has no valid name",
        ),
    )?;
    let retired = directory.with_file_name(format!(".reclaim-{name}"));
    let present = map_io(
        "inspect deleted branch directory",
        fs::try_exists(directory),
    )?;
    let pending = map_io("inspect retired branch directory", fs::try_exists(&retired))?;
    if present && pending {
        return Err(ImmutableObjectError::BranchMetadataIncomplete(
            "both original and retired branch directories exist",
        ));
    }
    if present || pending {
        let source = if present { directory } else { &retired };
        let metadata = map_io(
            "inspect cleanup directory kind",
            fs::symlink_metadata(source),
        )?;
        if !metadata.is_dir() {
            return Err(ImmutableObjectError::BranchMetadataIncomplete(
                "branch cleanup path is not a directory",
            ));
        }
        let lease =
            DatabaseDirectoryLease::acquire(source).map_err(|error| ImmutableObjectError::Io {
                operation: "lease deleted branch before cleanup",
                source: io::Error::other(error),
            })?;
        let _lease = if present {
            retire_deleted_directory(directory, &retired, lease)?
        } else {
            lease
        };
        // Repeat this barrier on retry: seeing the retired name after a process
        // interruption does not prove the original UUID is durably absent.
        map_io(
            "sync retired branch directory name",
            crate::durability::sync_parent_directory(&retired),
        )?;
        map_io(
            "remove retired branch directory",
            fs::remove_dir_all(&retired),
        )?;
    }
    // Also cover a prior successful unlink whose final barrier failed.
    map_io(
        "sync branch directory parent after reclamation",
        crate::durability::sync_parent_directory(directory),
    )?;
    Ok(present || pending)
}

fn retire_deleted_directory(
    directory: &Path,
    retired: &Path,
    lease: crate::ownership::DatabaseDirectoryLease,
) -> Result<crate::ownership::DatabaseDirectoryLease, ImmutableObjectError> {
    // Windows refuses directory renames while any descendant has an open
    // handle. Close our own lock handle; a racing opener's handle makes the
    // rename fail atomically, retaining the original directory. Catalog
    // serialization remains held, and a successful rename removes the only
    // admissible UUID path before we reacquire cleanup ownership.
    #[cfg(windows)]
    drop(lease);
    // Unix can rename an owned directory, so retain its original lock inode
    // throughout retirement and cleanup to exclude delayed openers.
    map_io(
        "retire deleted branch directory",
        fs::rename(directory, retired),
    )?;
    #[cfg(windows)]
    let lease = crate::ownership::DatabaseDirectoryLease::acquire(retired).map_err(|error| {
        ImmutableObjectError::Io {
            operation: "lease retired branch before cleanup",
            source: io::Error::other(error),
        }
    })?;
    Ok(lease)
}

fn validate_reference(
    reference: ObjectReference,
    payload: &[u8],
    max_object_bytes: u64,
) -> Result<(), ImmutableObjectError> {
    if reference.format_version == 0 {
        return Err(ImmutableObjectError::InvalidFormatVersion);
    }
    let length = payload.len() as u64;
    if length > max_object_bytes {
        return Err(ImmutableObjectError::ObjectTooLarge {
            length,
            limit: max_object_bytes,
        });
    }
    if reference.byte_length != length
        || reference.sha256 != identity_digest(reference.kind, reference.format_version, payload)
    {
        return Err(ImmutableObjectError::ReferenceMismatch);
    }
    Ok(())
}

/// Streams a stored object once, checking its length and identity and, when
/// the publisher still holds the payload, its exact bytes.
fn validate_file(
    path: &Path,
    reference: ObjectReference,
    expected: Option<&[u8]>,
    max_object_bytes: u64,
) -> Result<(), ImmutableObjectError> {
    let corrupt = || ImmutableObjectError::ExistingObjectCorrupt {
        path: path.to_path_buf(),
    };
    let metadata = map_io("read immutable object length", fs::metadata(path))?;
    if metadata.len() != reference.byte_length
        || metadata.len() > max_object_bytes
        || expected.is_some_and(|expected| expected.len() as u64 != reference.byte_length)
    {
        return Err(corrupt());
    }
    let mut file = map_io("read immutable object", File::open(path))?;
    let mut hasher = identity_hasher(
        reference.kind,
        reference.format_version,
        reference.byte_length,
    );
    let mut buffer = vec![0; STREAM_BUFFER_BYTES];
    let mut offset = 0_usize;
    loop {
        let length = map_io("read immutable object", file.read(&mut buffer))?;
        if length == 0 {
            break;
        }
        let end = offset.checked_add(length).ok_or_else(corrupt)?;
        if end as u64 > reference.byte_length
            || expected.is_some_and(|expected| expected[offset..end] != buffer[..length])
        {
            return Err(corrupt());
        }
        hasher.update(&buffer[..length]);
        offset = end;
    }
    if offset as u64 != reference.byte_length || hasher.finish().sha256 != reference.sha256 {
        return Err(corrupt());
    }
    Ok(())
}

fn identity_digest(kind: ObjectKind, format_version: u16, bytes: &[u8]) -> Sha256Digest {
    let mut hasher = identity_hasher(kind, format_version, bytes.len() as u64);
    hasher.update(bytes);
    hasher.finish().sha256
}

pub(crate) fn identity_hasher(
    kind: ObjectKind,
    format_version: u16,
    byte_length: u64,
) -> IntegrityHasher {
    let mut hasher = IntegrityHasher::new();
    hasher.update(IDENTITY_DOMAIN);
    hasher.update(&[kind as u8]);
    hasher.update(&format_version.to_le_bytes());
    hasher.update(&byte_length.to_le_bytes());
    hasher
}

fn map_io<T>(operation: &'static str, result: io::Result<T>) -> Result<T, ImmutableObjectError> {
    result.map_err(|source| ImmutableObjectError::Io { operation, source })
}

fn sync_publication_directories(
    destination: &Path,
    staging_directory: Option<&Path>,
) -> io::Result<()> {
    #[cfg(test)]
    if FAIL_NEXT_PUBLICATION_SYNC.with(|failure| failure.replace(false)) {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "injected immutable object directory sync failure",
        ));
    }
    crate::durability::sync_parent_directory(destination)?;
    if let Some(staging_directory) = staging_directory {
        crate::durability::sync_directory(staging_directory)?;
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_PUBLICATION_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
struct PublicationSyncFailureGuard;

#[cfg(test)]
impl Drop for PublicationSyncFailureGuard {
    fn drop(&mut self) {
        FAIL_NEXT_PUBLICATION_SYNC.with(|failure| failure.set(false));
    }
}

#[cfg(test)]
fn fail_next_publication_sync() -> PublicationSyncFailureGuard {
    FAIL_NEXT_PUBLICATION_SYNC.with(|failure| {
        assert!(!failure.replace(true));
    });
    PublicationSyncFailureGuard
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sealed_root::{CheckpointArtifactBinding, SealedRoot, SealedWalReference};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn streaming_reference_matches_complete_bytes_for_all_chunk_boundaries() {
        let bytes = vec![17; 2 * 8192 + 3];
        for kind in [
            ObjectKind::CheckpointArtifact,
            ObjectKind::Checkpoint,
            ObjectKind::SealedWal,
        ] {
            for version in [1, 2, u16::MAX] {
                for chunk in [1, 8192, bytes.len()] {
                    let mut writer =
                        ObjectReferenceWriter::new(kind, version, bytes.len() as u64).unwrap();
                    for bytes in bytes.chunks(chunk) {
                        writer.write_all(bytes).unwrap();
                    }
                    assert_eq!(
                        writer.finish().unwrap(),
                        ObjectReference::for_bytes(kind, version, &bytes)
                    );
                }
            }
        }
        assert!(matches!(
            ObjectReferenceWriter::new(ObjectKind::CheckpointArtifact, 0, 0),
            Err(ImmutableObjectError::InvalidFormatVersion)
        ));
        let empty = ObjectReferenceWriter::new(ObjectKind::CheckpointArtifact, 1, 0).unwrap();
        assert_eq!(
            empty.finish().unwrap(),
            ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &[])
        );
        let mut short = ObjectReferenceWriter::new(ObjectKind::CheckpointArtifact, 1, 2).unwrap();
        short.write_all(&[0]).unwrap();
        assert!(short.write_all(&[1, 2]).is_err());
        assert!(matches!(
            short.finish(),
            Err(ImmutableObjectError::ReferenceMismatch)
        ));
    }

    fn test_root(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("hawdb-immutable-{label}-{nanos}"))
    }

    fn reference(payload: &[u8]) -> ObjectReference {
        ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, payload)
    }

    fn staging_entries(root: &Path) -> usize {
        fs::read_dir(root.join(OBJECTS_DIRECTORY).join(STAGING_DIRECTORY))
            .unwrap()
            .count()
    }

    #[test]
    fn identity_is_domain_separated_by_kind_and_version() {
        let payload = b"same bytes";
        assert_ne!(
            ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, payload).sha256,
            ObjectReference::for_bytes(ObjectKind::SealedWal, 1, payload).sha256
        );
        assert_ne!(
            ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, payload).sha256,
            ObjectReference::for_bytes(ObjectKind::Checkpoint, 2, payload).sha256
        );
    }

    #[test]
    fn publishes_then_reuses_only_exact_bytes() {
        let root = test_root("dedup");
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        let payload = b"checkpoint bytes";
        let reference = reference(payload);
        assert_eq!(
            store.publish(reference, payload).unwrap(),
            PublishOutcome::Published
        );
        assert_eq!(
            store.publish(reference, payload).unwrap(),
            PublishOutcome::Reused
        );
        assert_eq!(fs::read(store.object_path(reference)).unwrap(), payload);
        drop(store);
        let mut reopened = ImmutableObjectStore::open(&root).unwrap();
        assert_eq!(
            reopened.publish(reference, payload).unwrap(),
            PublishOutcome::Reused
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publish_file_streams_across_buffers_and_matches_byte_publication() {
        let root = test_root("publish-file");
        let source = root.join("source.bin");
        fs::create_dir_all(&root).unwrap();
        // Spans several stream buffers and ends mid-buffer.
        let payload = (0..(3 * STREAM_BUFFER_BYTES + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        fs::write(&source, &payload).unwrap();
        let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &payload);
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        assert_eq!(
            store.publish_file(reference, &source).unwrap(),
            PublishOutcome::Published
        );
        assert_eq!(fs::read(store.object_path(reference)).unwrap(), payload);
        assert_eq!(
            store.publish_file(reference, &source).unwrap(),
            PublishOutcome::Reused
        );
        assert_eq!(
            store.publish(reference, &payload).unwrap(),
            PublishOutcome::Reused
        );
        assert_eq!(staging_entries(&root), 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publish_file_rejects_changed_sources_and_limits_without_staging_residue() {
        let root = test_root("publish-file-reject");
        fs::create_dir_all(&root).unwrap();
        let source = root.join("source.bin");
        fs::write(&source, b"payload").unwrap();
        let mut store = ImmutableObjectStore::open_with_limit(&root, 16).unwrap();
        let expected = reference(b"PAYLOAD");
        assert!(matches!(
            store.publish_file(expected, &source),
            Err(ImmutableObjectError::ReferenceMismatch)
        ));
        let mut shorter = reference(b"payload");
        shorter.byte_length = 3;
        assert!(matches!(
            store.publish_file(shorter, &source),
            Err(ImmutableObjectError::ReferenceMismatch)
        ));
        fs::write(&source, b"this payload is too long").unwrap();
        assert!(matches!(
            store.publish_file(reference(b"this payload is too long"), &source),
            Err(ImmutableObjectError::ObjectTooLarge { .. })
        ));
        assert!(matches!(
            store.publish_file(reference(b"missing"), &root.join("missing.bin")),
            Err(ImmutableObjectError::Io {
                operation: "open immutable object source",
                ..
            })
        ));
        assert!(!store.object_path(expected).exists());
        assert_eq!(staging_entries(&root), 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn streamed_checkpoint_objects_use_the_streamed_bound_only() {
        let root = test_root("streamed-bound");
        fs::create_dir_all(&root).unwrap();
        let source = root.join("artifact.bin");
        let payload = vec![7u8; 64];
        fs::write(&source, &payload).unwrap();
        // 16 bytes may be read into memory; checkpoint objects may stream 1 KiB.
        let mut store = ImmutableObjectStore::open_with_limits(&root, 16, 1024).unwrap();
        for kind in [ObjectKind::Checkpoint, ObjectKind::CheckpointArtifact] {
            let reference = ObjectReference::for_bytes(kind, 1, &payload);
            assert_eq!(
                store.publish_file(reference, &source).unwrap(),
                PublishOutcome::Published
            );
            store.verify_stored(reference).unwrap();
            // Buffered APIs keep the in-memory bound.
            assert!(matches!(
                store.publish(reference, &payload),
                Err(ImmutableObjectError::ObjectTooLarge { .. })
            ));
            assert!(store.read(reference).is_err());
        }
        // Kinds that are read whole into memory keep the in-memory bound even
        // when published from a file.
        for kind in [
            ObjectKind::SealedWal,
            ObjectKind::SealedRoot,
            ObjectKind::DurableManifest,
        ] {
            let reference = ObjectReference::for_bytes(kind, 1, &payload);
            assert!(matches!(
                store.publish_file(reference, &source),
                Err(ImmutableObjectError::ObjectTooLarge { limit: 16, .. })
            ));
        }
        // The streamed bound still applies.
        let large = vec![1u8; 2048];
        fs::write(&source, &large).unwrap();
        let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &large);
        assert!(matches!(
            store.publish_file(reference, &source),
            Err(ImmutableObjectError::ObjectTooLarge { limit: 1024, .. })
        ));
        assert_eq!(staging_entries(&root), 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reopened_store_reads_only_the_exact_immutable_reference() {
        let root = test_root("reopen-read");
        let payload = b"sealed root bytes";
        let reference = ObjectReference::for_bytes(ObjectKind::SealedRoot, 1, payload);
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        store.publish(reference, payload).unwrap();
        drop(store);

        let reopened = ImmutableObjectStore::open(&root).unwrap();
        assert_eq!(reopened.read(reference).unwrap(), payload);
        let corrupt = reopened.object_path(reference);
        fs::write(corrupt, b"different bytes").unwrap();
        assert!(matches!(
            reopened.read(reference),
            Err(ImmutableObjectError::ExistingObjectCorrupt { .. })
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_reference_mismatch_and_resource_limits() {
        let root = test_root("limits");
        let mut store = ImmutableObjectStore::open_with_limit(&root, 16).unwrap();
        assert!(matches!(
            store.publish(
                reference(b"this payload is too long"),
                b"this payload is too long"
            ),
            Err(ImmutableObjectError::ObjectTooLarge { .. })
        ));
        let mut wrong = reference(b"payload");
        wrong.byte_length = 1;
        assert!(matches!(
            store.publish(wrong, b"payload"),
            Err(ImmutableObjectError::ReferenceMismatch)
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn detects_existing_collision_or_corruption_without_replacement() {
        let root = test_root("collision");
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        let payload = b"original";
        let reference = reference(payload);
        store.publish(reference, payload).unwrap();
        fs::write(store.object_path(reference), b"tampered").unwrap();
        assert!(matches!(
            store.publish(reference, payload),
            Err(ImmutableObjectError::ExistingObjectCorrupt { .. })
        ));
        assert_eq!(fs::read(store.object_path(reference)).unwrap(), b"tampered");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn uncertain_directory_publication_poisons_until_reopen() {
        let root = test_root("poison");
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        let payload = b"durability boundary";
        let reference = reference(payload);
        let _guard = fail_next_publication_sync();
        assert!(matches!(
            store.publish(reference, payload),
            Err(ImmutableObjectError::PublicationUncertain { .. })
        ));
        assert!(matches!(
            store.publish(reference, payload),
            Err(ImmutableObjectError::PublisherPoisoned)
        ));
        drop(store);
        let mut reopened = ImmutableObjectStore::open(&root).unwrap();
        assert_eq!(
            reopened.publish(reference, payload).unwrap(),
            PublishOutcome::Reused
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_publishers_share_one_complete_object() {
        let root = test_root("concurrent");
        let payload = b"concurrent object";
        let reference = reference(payload);
        let root_a = root.clone();
        let root_b = root.clone();
        let first = std::thread::spawn(move || {
            let mut store = ImmutableObjectStore::open(root_a).unwrap();
            store.publish(reference, payload).unwrap()
        });
        let second = std::thread::spawn(move || {
            let mut store = ImmutableObjectStore::open(root_b).unwrap();
            store.publish(reference, payload).unwrap()
        });
        let outcomes = [first.join().unwrap(), second.join().unwrap()];
        assert!(outcomes.contains(&PublishOutcome::Published));
        assert!(outcomes.contains(&PublishOutcome::Reused));
        let store = ImmutableObjectStore::open(&root).unwrap();
        assert_eq!(fs::read(store.object_path(reference)).unwrap(), payload);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reclamation_marks_sealed_root_closure_and_removes_only_orphans() {
        let root = test_root("reclamation");
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        let checkpoint = b"checkpoint bytes";
        let wal = b"sealed wal bytes";
        let orphan = b"orphan bytes";
        let checkpoint_reference =
            ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, checkpoint);
        let wal_reference = ObjectReference::for_bytes(ObjectKind::SealedWal, 1, wal);
        let manifest = b"manifest bytes";
        let manifest_reference =
            ObjectReference::for_bytes(ObjectKind::DurableManifest, 1, manifest);
        let orphan_reference =
            ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, orphan);
        let sealed_root = SealedRoot {
            checkpoint_epoch: 1,
            commit_epoch: 1,
            wal_replay_start_lsn: 0,
            durable_manifest: manifest_reference,
            checkpoint_references: vec![checkpoint_reference],
            checkpoint_bindings: vec![CheckpointArtifactBinding {
                relative_path: "checkpoint.hawdb".to_string(),
                reference: checkpoint_reference,
            }],
            sealed_wals: vec![SealedWalReference {
                start_lsn: 0,
                end_lsn: 1,
                object: wal_reference,
            }],
        };
        let sealed_root_bytes = sealed_root.encode().unwrap();
        let sealed_root_reference =
            ObjectReference::for_bytes(ObjectKind::SealedRoot, 2, &sealed_root_bytes);

        store.publish(checkpoint_reference, checkpoint).unwrap();
        store.publish(wal_reference, wal).unwrap();
        store.publish(manifest_reference, manifest).unwrap();
        store.publish(orphan_reference, orphan).unwrap();
        store
            .publish(sealed_root_reference, &sealed_root_bytes)
            .unwrap();

        let report = store
            .reclaim_unreachable(
                &[
                    sealed_root_reference,
                    manifest_reference,
                    checkpoint_reference,
                    wal_reference,
                    orphan_reference,
                ],
                &[sealed_root_reference],
            )
            .unwrap();
        assert_eq!(report.retained_objects, 4);
        assert_eq!(report.reclaimed_objects, 1);
        assert_eq!(report.reclaimed_bytes, orphan.len() as u64);
        assert!(store.read(sealed_root_reference).is_ok());
        assert!(store.read(manifest_reference).is_ok());
        assert!(store.read(checkpoint_reference).is_ok());
        assert!(store.read(wal_reference).is_ok());
        assert!(matches!(
            store.read(orphan_reference),
            Err(ImmutableObjectError::Io { .. })
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reclamation_retains_unknown_files_and_aborts_before_sweep_on_corruption() {
        let root = test_root("reclamation-retention");
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        let mut orphan = vec![0x5a; 2 * 64 * 1024 + 1];
        let orphan_reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, &orphan);
        store.publish(orphan_reference, &orphan).unwrap();
        let unknown = root.join("objects").join("checkpoint").join("unknown-file");
        fs::write(&unknown, b"unlisted object").unwrap();
        let second = b"second orphan";
        let second_reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, second);
        store.publish(second_reference, second).unwrap();
        // Keep the length unchanged and corrupt a byte beyond two hash
        // buffers: a partial verification must not allow any deletion.
        *orphan.last_mut().unwrap() ^= 0xff;
        fs::write(store.object_path(orphan_reference), &orphan).unwrap();

        assert!(matches!(
            store.reclaim_unreachable(&[orphan_reference, second_reference], &[]),
            Err(ImmutableObjectError::ExistingObjectCorrupt { .. })
        ));
        assert!(store.object_path(second_reference).exists());
        assert!(unknown.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reclamation_retry_is_idempotent_after_a_prior_unlink() {
        let root = test_root("reclamation-retry");
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        let first = b"first orphan";
        let second = b"second orphan";
        let first_reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, first);
        let second_reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, second);
        store.publish(first_reference, first).unwrap();
        store.publish(second_reference, second).unwrap();
        fs::remove_file(store.object_path(first_reference)).unwrap();

        let report = store
            .reclaim_unreachable(&[first_reference, second_reference], &[])
            .unwrap();
        assert_eq!(report.reclaimed_objects, 2);
        assert_eq!(report.reclaimed_bytes, (first.len() + second.len()) as u64);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn recovery_authenticates_and_syncs_a_read_only_immutable_object() {
        let root = test_root("read-only-recovery-dependency");
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        let payload = vec![0x5a; 2 * 64 * 1024 + 1];
        let reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, &payload);
        store.publish(reference, &payload).unwrap();
        let path = store.object_path(reference);
        let original = fs::metadata(&path).unwrap().permissions();
        let mut read_only = original.clone();
        read_only.set_readonly(true);
        std::fs::set_permissions(&path, read_only).unwrap();

        let result = store.verify_and_sync(reference);
        std::fs::set_permissions(&path, original).unwrap();
        result.unwrap();
        assert_eq!(store.read(reference).unwrap(), payload);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn deleted_directory_cleanup_revalidates_lease_and_resumes_retirement() {
        use crate::ownership::DatabaseDirectoryLease;

        let root = test_root("reclamation-retirement");
        let directory = root.join("deleted");
        fs::create_dir_all(directory.join("data")).unwrap();
        fs::write(directory.join("data/payload"), b"retained until unpinned").unwrap();
        let lease = DatabaseDirectoryLease::acquire(&directory).unwrap();
        assert!(super::reclaim_deleted_directory(&directory).is_err());
        assert_eq!(
            fs::read(directory.join("data/payload")).unwrap(),
            b"retained until unpinned"
        );
        drop(lease);

        assert!(super::reclaim_deleted_directory(&directory).unwrap());
        assert!(!directory.exists());
        fs::create_dir_all(directory.join("data")).unwrap();
        fs::write(directory.join("data/payload"), b"interrupted retirement").unwrap();

        let retired = root.join(".reclaim-deleted");
        fs::rename(&directory, &retired).unwrap();
        assert!(DatabaseDirectoryLease::acquire(&directory).is_err());
        assert!(super::reclaim_deleted_directory(&directory).unwrap());
        assert!(!retired.exists());
        assert!(!super::reclaim_deleted_directory(&directory).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn windows_retirement_requires_all_descendant_handles_to_close() {
        use crate::ownership::DatabaseDirectoryLease;

        let root = test_root("retirement-open-descendant");
        let directory = root.join("deleted");
        let retired = root.join(".reclaim-deleted");
        fs::create_dir_all(&directory).unwrap();
        let lease = DatabaseDirectoryLease::acquire(&directory).unwrap();
        assert!(fs::rename(&directory, &retired).is_err());
        assert!(directory.exists());
        assert!(!retired.exists());
        drop(lease);

        fs::write(directory.join("payload"), b"open descendant").unwrap();
        let reader = fs::File::open(directory.join("payload")).unwrap();
        assert!(super::reclaim_deleted_directory(&directory).is_err());
        assert_eq!(
            fs::read(directory.join("payload")).unwrap(),
            b"open descendant"
        );
        assert!(!retired.exists());
        drop(reader);

        assert!(super::reclaim_deleted_directory(&directory).unwrap());
        assert!(!directory.exists());
        assert!(!retired.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reclamation_rejects_active_cached_reads_and_releases_idle_descriptors() {
        use crate::file_descriptors::ProjectFileDescriptors;
        use crate::immutable_files::ImmutableFileBinding;

        let root = test_root("reclamation-cache");
        let project = ProjectFileDescriptors::acquire_with_capacity_limit(&root, 8, true).unwrap();
        let context = project.io_context();
        let handles = context.state.immutable_handles();
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        let object =
            ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, b"cached orphan");
        store.publish(object, b"cached orphan").unwrap();
        let binding = ImmutableFileBinding {
            reference: object,
            object_path: store.object_path(object),
        };
        let reader = handles.get(&binding, &context).unwrap();
        assert_eq!(project.metrics().cached_handles, 1);
        assert!(store.reclaim_unreachable(&[object], &[]).is_err());
        assert!(store.object_path(object).exists());
        drop(reader);
        let report = store.reclaim_unreachable(&[object], &[]).unwrap();
        assert_eq!(report.reclaimed_objects, 1);
        assert_eq!(project.metrics().cached_handles, 0);
        assert!(!store.object_path(object).exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn branch_reclamation_keeps_live_roots_and_removes_deleted_branch_directory() {
        let root = test_root("branch-reclamation");
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        let checkpoint_reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, b"base");
        let wal_reference = ObjectReference::for_bytes(ObjectKind::SealedWal, 1, b"wal");
        let manifest_reference =
            ObjectReference::for_bytes(ObjectKind::DurableManifest, 1, b"manifest");
        store.publish(checkpoint_reference, b"base").unwrap();
        store.publish(wal_reference, b"wal").unwrap();
        store.publish(manifest_reference, b"manifest").unwrap();
        let live_root = SealedRoot {
            checkpoint_epoch: 1,
            commit_epoch: 1,
            wal_replay_start_lsn: 0,
            durable_manifest: manifest_reference,
            checkpoint_references: vec![checkpoint_reference],
            checkpoint_bindings: vec![CheckpointArtifactBinding {
                relative_path: "checkpoint.hawdb".to_string(),
                reference: checkpoint_reference,
            }],
            sealed_wals: vec![SealedWalReference {
                start_lsn: 0,
                end_lsn: 3,
                object: wal_reference,
            }],
        };
        let live_root_bytes = live_root.encode().unwrap();
        let live_root_reference =
            ObjectReference::for_bytes(ObjectKind::SealedRoot, 2, &live_root_bytes);
        let orphan_reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, b"orphan");
        store
            .publish(live_root_reference, &live_root_bytes)
            .unwrap();
        store.publish(orphan_reference, b"orphan").unwrap();
        let deleted_directory = root.join("branches").join("deleted");
        fs::create_dir_all(&deleted_directory).unwrap();
        fs::write(deleted_directory.join("head"), b"old").unwrap();

        let error = store
            .reclaim_branches(&BranchReclamationInventory {
                objects: vec![live_root_reference, orphan_reference],
                branches: vec![
                    BranchReclamationEntry {
                        state: crate::branch_catalog::BranchState::Ready,
                        sealed_root: Some(live_root_reference),
                        directory: root.join("branches").join("live"),
                        active_lease: true,
                    },
                    BranchReclamationEntry {
                        state: crate::branch_catalog::BranchState::Ready,
                        sealed_root: None,
                        directory: root.join("branches").join("corrupt-child"),
                        active_lease: false,
                    },
                ],
            })
            .unwrap_err();
        assert!(matches!(
            error,
            ImmutableObjectError::BranchMetadataIncomplete(_)
        ));
        assert!(store.object_path(orphan_reference).exists());
        assert!(deleted_directory.exists());

        let report = store
            .reclaim_branches(&BranchReclamationInventory {
                objects: vec![
                    live_root_reference,
                    manifest_reference,
                    checkpoint_reference,
                    wal_reference,
                    orphan_reference,
                ],
                branches: vec![
                    BranchReclamationEntry {
                        state: crate::branch_catalog::BranchState::Ready,
                        sealed_root: Some(live_root_reference),
                        directory: root.join("branches").join("live"),
                        active_lease: false,
                    },
                    BranchReclamationEntry {
                        state: crate::branch_catalog::BranchState::Deleted,
                        sealed_root: None,
                        directory: deleted_directory.clone(),
                        active_lease: false,
                    },
                ],
            })
            .unwrap();
        assert_eq!(report.retained_objects, 4);
        assert_eq!(report.reclaimed_objects, 1);
        assert!(!deleted_directory.exists());
        assert!(store.read(live_root_reference).is_ok());
        assert!(matches!(
            store.read(orphan_reference),
            Err(ImmutableObjectError::Io { .. })
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn branch_reclamation_requires_a_root_only_without_an_active_lease() {
        let root = test_root("branch-reclamation-incomplete");
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        let orphan_reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, b"orphan");
        store.publish(orphan_reference, b"orphan").unwrap();
        let mut inventory = BranchReclamationInventory {
            objects: vec![orphan_reference],
            branches: vec![BranchReclamationEntry {
                state: crate::branch_catalog::BranchState::Creating,
                sealed_root: None,
                directory: root.join("branches").join("creating"),
                active_lease: true,
            }],
        };
        let report = store.reclaim_branches(&inventory).unwrap();
        assert!(report.deferred_for_active_leases);
        assert_eq!(report.reclaimed_objects, 0);
        assert_eq!(report.retained_objects, 1);
        assert!(store.object_path(orphan_reference).exists());

        inventory.branches[0].active_lease = false;
        let error = store.reclaim_branches(&inventory).unwrap_err();
        assert!(matches!(
            error,
            ImmutableObjectError::BranchMetadataIncomplete(_)
        ));
        assert!(store.object_path(orphan_reference).exists());
        fs::remove_dir_all(root).unwrap();
    }
}
