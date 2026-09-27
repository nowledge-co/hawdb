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
use crate::immutable_object::{ObjectKind, ObjectReference};
use hawdb_integrity::{crc32c, IntegrityHasher, Sha256Digest};
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
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
    use crate::immutable_object::ObjectKind;
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
}
