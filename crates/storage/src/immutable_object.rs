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

use hawdb_integrity::{IntegrityHasher, Sha256Digest};
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const OBJECTS_DIRECTORY: &str = "objects";
const STAGING_DIRECTORY: &str = ".staging";
const IDENTITY_DOMAIN: &[u8] = b"HAWDB-IMMUTABLE-OBJECT-V1\0";
const DEFAULT_MAX_OBJECT_BYTES: u64 = 128 * 1024 * 1024;

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ObjectKind {
    Checkpoint = 1,
    SealedWal = 2,
    SealedRoot = 3,
    CheckpointArtifact = 4,
}

impl ObjectKind {
    const fn directory(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::SealedWal => "sealed-wal",
            Self::SealedRoot => "sealed-root",
            Self::CheckpointArtifact => "checkpoint-artifact",
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
        }
    }
}

impl std::error::Error for ImmutableObjectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } | Self::PublicationUncertain { source } => Some(source),
            _ => None,
        }
    }
}

/// Owns the immutable-object namespace for one project.
#[derive(Debug)]
pub struct ImmutableObjectStore {
    root: PathBuf,
    max_object_bytes: u64,
    poisoned: bool,
}

impl ImmutableObjectStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, ImmutableObjectError> {
        Self::open_with_limit(root, DEFAULT_MAX_OBJECT_BYTES)
    }

    pub fn open_with_limit(
        root: impl Into<PathBuf>,
        max_object_bytes: u64,
    ) -> Result<Self, ImmutableObjectError> {
        if max_object_bytes == 0 {
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
            poisoned: false,
        })
    }

    pub fn object_path(&self, reference: ObjectReference) -> PathBuf {
        self.root
            .join(OBJECTS_DIRECTORY)
            .join(reference.kind.directory())
            .join(reference.path_component())
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

        let destination = self.object_path(reference);
        let object_directory = destination
            .parent()
            .expect("object path always has an object-kind parent");
        map_io(
            "create immutable object kind directory",
            fs::create_dir_all(object_directory),
        )?;

        if destination.exists() {
            return self.validate_existing(reference, payload, &destination);
        }

        let staging = self.stage_payload(payload)?;
        let install_result = self.install_staged(reference, payload, &staging, &destination);
        if install_result.is_err() {
            let _ = fs::remove_file(&staging);
        }
        install_result
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

    fn install_staged(
        &mut self,
        reference: ObjectReference,
        payload: &[u8],
        staging: &Path,
        destination: &Path,
    ) -> Result<PublishOutcome, ImmutableObjectError> {
        validate_file(staging, reference, payload, self.max_object_bytes)?;
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
        &self,
        reference: ObjectReference,
        payload: &[u8],
        path: &Path,
    ) -> Result<PublishOutcome, ImmutableObjectError> {
        let metadata = map_io("read immutable object metadata", fs::symlink_metadata(path))?;
        if !metadata.file_type().is_file() {
            return Err(ImmutableObjectError::ExistingObjectCorrupt {
                path: path.to_path_buf(),
            });
        }
        validate_file(path, reference, payload, self.max_object_bytes)?;
        Ok(PublishOutcome::Reused)
    }
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

fn validate_file(
    path: &Path,
    reference: ObjectReference,
    expected: &[u8],
    max_object_bytes: u64,
) -> Result<(), ImmutableObjectError> {
    let metadata = map_io("read immutable object length", fs::metadata(path))?;
    if metadata.len() != reference.byte_length || metadata.len() > max_object_bytes {
        return Err(ImmutableObjectError::ExistingObjectCorrupt {
            path: path.to_path_buf(),
        });
    }
    let capacity = usize::try_from(metadata.len()).map_err(|_| {
        ImmutableObjectError::ExistingObjectCorrupt {
            path: path.to_path_buf(),
        }
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    let mut file = map_io("read immutable object", File::open(path))?;
    file.read_to_end(&mut bytes)
        .map_err(|error| ImmutableObjectError::Io {
            operation: "read immutable object",
            source: error,
        })?;
    if bytes != expected
        || identity_digest(reference.kind, reference.format_version, &bytes) != reference.sha256
    {
        return Err(ImmutableObjectError::ExistingObjectCorrupt {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn identity_digest(kind: ObjectKind, format_version: u16, bytes: &[u8]) -> Sha256Digest {
    let mut hasher = IntegrityHasher::new();
    hasher.update(IDENTITY_DOMAIN);
    hasher.update(&[kind as u8]);
    hasher.update(&format_version.to_le_bytes());
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
    hasher.finish().sha256
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
    use std::time::{SystemTime, UNIX_EPOCH};

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
}
