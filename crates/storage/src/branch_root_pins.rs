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

//! Ephemeral reachability declarations owned by a live UUID lease.

use crate::branch_catalog::CatalogMetadataLease;
use crate::file_io::{self as fs, File};
use crate::immutable_object::{ObjectKind, ObjectReference};
use crate::ownership::{DatabaseDirectoryLease, DatabaseDirectoryLeaseError};
use hawdb_integrity::{integrity_digest, Sha256Digest};
use std::collections::BTreeMap;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const DIRECTORY: &str = "root-pins";
const RECORD: &str = "roots.hawdb";
const PENDING: &str = "roots.pending";
const MAGIC: &[u8; 8] = b"HAWPIN01";
const HEADER_BYTES: usize = 48;
const ENTRY_BYTES: usize = 50;
const CHECKSUM_BYTES: usize = 32;
const MAX_ROOTS: usize = 100_000;
const MAX_RECORD_BYTES: usize = HEADER_BYTES + ENTRY_BYTES * MAX_ROOTS + CHECKSUM_BYTES;

#[derive(Debug, Default)]
struct RootSet {
    next_id: u64,
    roots: BTreeMap<u64, ObjectReference>,
}

#[derive(Debug)]
pub(crate) struct RootRegistry {
    project_directory: PathBuf,
    directory: PathBuf,
    project_id: [u8; 16],
    branch_id: [u8; 16],
    roots: Mutex<RootSet>,
    _registration: DatabaseDirectoryLease,
    _owner: Arc<DatabaseDirectoryLease>,
}

/// Cloning a guard performs no IO and keeps its exact historical root alive.
#[derive(Debug)]
pub struct RootPin {
    registry: Arc<RootRegistry>,
    id: u64,
    root: ObjectReference,
}

pub(crate) struct RootPublication<'a> {
    registry: &'a Arc<RootRegistry>,
    _metadata: CatalogMetadataLease,
}

impl RootRegistry {
    pub(crate) fn open(
        project_directory: &Path,
        branch_directory: &Path,
        project_id: [u8; 16],
        branch_id: [u8; 16],
        owner: Arc<DatabaseDirectoryLease>,
        root: ObjectReference,
    ) -> io::Result<Arc<RootPin>> {
        let metadata = CatalogMetadataLease::acquire_blocking(project_directory)?;
        Self::open_with_metadata(
            project_directory,
            branch_directory,
            project_id,
            branch_id,
            owner,
            root,
            &metadata,
        )
    }

    /// Registers a closed-source publication while its caller still excludes
    /// GC from reading the old head and sweeping newly published objects.
    pub(crate) fn open_with_metadata(
        project_directory: &Path,
        branch_directory: &Path,
        project_id: [u8; 16],
        branch_id: [u8; 16],
        owner: Arc<DatabaseDirectoryLease>,
        root: ObjectReference,
        _metadata: &CatalogMetadataLease,
    ) -> io::Result<Arc<RootPin>> {
        if !owner.owns_directory(branch_directory)? {
            return Err(invalid("root registry does not own its UUID directory"));
        }
        let directory = branch_directory.join(DIRECTORY);
        match fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        if !fs::symlink_metadata(&directory)?.is_dir() {
            return Err(invalid("root registry must be a regular directory"));
        }
        let registration = DatabaseDirectoryLease::acquire(&directory).map_err(lease_error)?;
        let registry = Arc::new(Self {
            project_directory: project_directory.to_path_buf(),
            directory,
            project_id,
            branch_id,
            roots: Mutex::new(RootSet::default()),
            _registration: registration,
            _owner: owner,
        });
        // The UUID and registration leases exclude every earlier owner. The
        // metadata barrier excludes GC until this complete replacement exists.
        registry.pin_locked(root)
    }

    pub(crate) fn publication(self: &Arc<Self>) -> io::Result<RootPublication<'_>> {
        Ok(RootPublication {
            registry: self,
            _metadata: CatalogMetadataLease::acquire_blocking(&self.project_directory)?,
        })
    }

    fn pin_locked(self: &Arc<Self>, root: ObjectReference) -> io::Result<Arc<RootPin>> {
        if root.kind != ObjectKind::SealedRoot || root.format_version == 0 {
            return Err(invalid("root pin requires a versioned sealed root"));
        }
        let mut state = self
            .roots
            .lock()
            .map_err(|_| invalid("root registry poisoned"))?;
        if state.roots.len() == MAX_ROOTS {
            return Err(invalid("root registry entry limit exceeded"));
        }
        let id = state
            .next_id
            .checked_add(1)
            .ok_or_else(|| invalid("root pin ID overflow"))?;
        state.roots.insert(id, root);
        if let Err(error) = self.publish(&state.roots) {
            state.roots.remove(&id);
            return Err(error);
        }
        state.next_id = id;
        Ok(Arc::new(RootPin {
            registry: Arc::clone(self),
            id,
            root,
        }))
    }

    fn publish(&self, roots: &BTreeMap<u64, ObjectReference>) -> io::Result<()> {
        let _descriptor_operation =
            crate::file_descriptors::FileOpenContext::for_path(&self.directory)?
                .internal_operation()?;
        let target = self.directory.join(RECORD);
        let pending = self.directory.join(PENDING);
        for path in [&target, &pending] {
            match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.is_file() => {}
                Ok(_) => return Err(invalid("root registry destination is not a regular file")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        fs::write(&pending, encode(self.project_id, self.branch_id, roots))?;
        // This is a live-process declaration, not durable branch metadata.
        // Atomic visibility suffices: after a crash, the registration lease
        // is gone and these bytes cannot authorize sweeping an active owner.
        fs::rename(pending, target)
    }
}

impl RootPublication<'_> {
    pub(crate) fn metadata(&self) -> &CatalogMetadataLease {
        &self._metadata
    }

    pub(crate) fn check_capacity(&self) -> io::Result<()> {
        let state = self
            .registry
            .roots
            .lock()
            .map_err(|_| invalid("root registry poisoned"))?;
        if state.roots.len() >= MAX_ROOTS {
            return Err(invalid("root registry entry limit exceeded"));
        }
        Ok(())
    }

    pub(crate) fn pin(&self, root: ObjectReference) -> io::Result<Arc<RootPin>> {
        self.registry.pin_locked(root)
    }
}

impl RootPin {
    pub(crate) fn root(&self) -> ObjectReference {
        self.root
    }

    pub(crate) fn registry(&self) -> &Arc<RootRegistry> {
        &self.registry
    }
}

impl Drop for RootPin {
    fn drop(&mut self) {
        let Ok(mut state) = self.registry.roots.lock() else {
            return;
        };
        state.roots.remove(&self.id);
        // Removal only shrinks reachability. A concurrent GC can safely read
        // either complete set; publication failure retains surplus roots.
        let _ = self.registry.publish(&state.roots);
    }
}

/// The caller owns metadata serialization and has observed an active UUID
/// lease. None means the owner is not registered, so GC must still defer.
pub(crate) fn active_roots(
    branch_directory: &Path,
    project_id: [u8; 16],
    branch_id: [u8; 16],
    max_roots: usize,
    _metadata: &CatalogMetadataLease,
) -> io::Result<Option<Vec<ObjectReference>>> {
    let directory = branch_directory.join(DIRECTORY);
    match fs::symlink_metadata(&directory) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Err(invalid("root registry must be a regular directory")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    match DatabaseDirectoryLease::acquire(&directory) {
        Ok(_inactive) => return Ok(None),
        Err(DatabaseDirectoryLeaseError::AlreadyOpen) => {}
        Err(error) => return Err(lease_error(error)),
    }
    let path = directory.join(RECORD);
    if !fs::symlink_metadata(&path)?.is_file() {
        return Err(invalid("root registry record is not a regular file"));
    }
    let file = File::open(path)?;
    let limit = HEADER_BYTES + ENTRY_BYTES * max_roots.min(MAX_ROOTS) + CHECKSUM_BYTES;
    if file.metadata()?.len() > limit as u64 {
        return Err(invalid("root registry record size limit exceeded"));
    }
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    Ok(Some(decode(&bytes, project_id, branch_id, max_roots)?))
}

fn encode(project: [u8; 16], branch: [u8; 16], roots: &BTreeMap<u64, ObjectReference>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEADER_BYTES + roots.len() * ENTRY_BYTES + CHECKSUM_BYTES);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&project);
    bytes.extend_from_slice(&branch);
    bytes.extend_from_slice(&(roots.len() as u64).to_le_bytes());
    for (id, root) in roots {
        bytes.extend_from_slice(&id.to_le_bytes());
        bytes.extend_from_slice(&root.format_version.to_le_bytes());
        bytes.extend_from_slice(&root.byte_length.to_le_bytes());
        bytes.extend_from_slice(root.sha256.as_bytes());
    }
    let checksum = integrity_digest(&bytes).sha256;
    bytes.extend_from_slice(checksum.as_bytes());
    bytes
}

fn decode(
    bytes: &[u8],
    project: [u8; 16],
    branch: [u8; 16],
    limit: usize,
) -> io::Result<Vec<ObjectReference>> {
    if bytes.len() < HEADER_BYTES + CHECKSUM_BYTES || bytes.len() > MAX_RECORD_BYTES {
        return Err(invalid("invalid root registry length"));
    }
    let (payload, checksum) = bytes.split_at(bytes.len() - CHECKSUM_BYTES);
    if &payload[..8] != MAGIC
        || payload[8..24] != project
        || payload[24..40] != branch
        || integrity_digest(payload).sha256.as_bytes() != checksum
    {
        return Err(invalid("root registry identity or checksum mismatch"));
    }
    let count = u64::from_le_bytes(payload[40..48].try_into().unwrap());
    if count > limit.min(MAX_ROOTS) as u64
        || payload.len() != HEADER_BYTES + count as usize * ENTRY_BYTES
    {
        return Err(invalid("root registry entry limit or length mismatch"));
    }
    let mut roots = Vec::with_capacity(count as usize);
    let mut previous = 0;
    for entry in payload[HEADER_BYTES..].chunks_exact(ENTRY_BYTES) {
        let id = u64::from_le_bytes(entry[..8].try_into().unwrap());
        let format_version = u16::from_le_bytes(entry[8..10].try_into().unwrap());
        if id <= previous || format_version == 0 {
            return Err(invalid("root registry pin order or version is invalid"));
        }
        previous = id;
        roots.push(ObjectReference {
            kind: ObjectKind::SealedRoot,
            format_version,
            byte_length: u64::from_le_bytes(entry[10..18].try_into().unwrap()),
            sha256: Sha256Digest::from_bytes(entry[18..50].try_into().unwrap()),
        });
    }
    Ok(roots)
}

fn lease_error(error: DatabaseDirectoryLeaseError) -> io::Error {
    match error {
        DatabaseDirectoryLeaseError::AlreadyOpen => {
            io::Error::new(io::ErrorKind::WouldBlock, error)
        }
        DatabaseDirectoryLeaseError::Canonicalize(error)
        | DatabaseDirectoryLeaseError::OpenLockFile(error)
        | DatabaseDirectoryLeaseError::Lock(error) => error,
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
