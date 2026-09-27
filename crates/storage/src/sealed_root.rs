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

//! Deterministic sealed-root metadata and WAL interval validation.
//!
//! This codec contains references only.  It never names a mutable path and it
//! does not publish any selector; the object publisher owns that boundary.

use crate::immutable_object::{ObjectKind, ObjectReference};
use hawdb_integrity::{crc32c, Sha256Digest};
use std::fmt::{self, Display, Formatter};

const MAGIC: &[u8; 12] = b"HAWDBROOTV1\0";
const REFERENCE_BYTES: usize = 1 + 2 + 8 + 32;
const MAX_CHECKPOINT_REFERENCES: usize = 4096;
const MAX_WAL_REFERENCES: usize = 1_000_000;
const MAX_ENCODED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedWalReference {
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub object: ObjectReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedRoot {
    pub checkpoint_epoch: u64,
    pub commit_epoch: u64,
    pub wal_replay_start_lsn: u64,
    pub checkpoint_references: Vec<ObjectReference>,
    pub sealed_wals: Vec<SealedWalReference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealedRootError {
    InvalidMagic,
    Truncated,
    ChecksumMismatch,
    TrailingBytes,
    TooManyCheckpointReferences,
    TooManyWalReferences,
    EncodedTooLarge,
    InvalidEpochs,
    InvalidObjectKind,
    InvalidObjectVersion,
    DuplicateCheckpointReference,
    UnorderedCheckpointReferences,
    InvalidWalInterval,
    UnorderedWalIntervals,
    NonContiguousWalIntervals,
    DuplicateWalObject,
    InvalidReferenceDigest,
}

impl Display for SealedRootError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidMagic => "sealed root has an unknown format",
            Self::Truncated => "sealed root is truncated",
            Self::ChecksumMismatch => "sealed root checksum mismatch",
            Self::TrailingBytes => "sealed root has trailing bytes",
            Self::TooManyCheckpointReferences => "sealed root has too many checkpoint references",
            Self::TooManyWalReferences => "sealed root has too many sealed WAL references",
            Self::EncodedTooLarge => "sealed root encoding exceeds its byte limit",
            Self::InvalidEpochs => "sealed root epochs are inconsistent",
            Self::InvalidObjectKind => "sealed root contains an object of the wrong kind",
            Self::InvalidObjectVersion => "sealed root contains an object with version zero",
            Self::DuplicateCheckpointReference => "sealed root repeats a checkpoint reference",
            Self::UnorderedCheckpointReferences => {
                "sealed root checkpoint references are not canonical"
            }
            Self::InvalidWalInterval => "sealed root contains an empty or overflowing WAL interval",
            Self::UnorderedWalIntervals => "sealed root WAL intervals are not ordered",
            Self::NonContiguousWalIntervals => "sealed root WAL intervals have a gap",
            Self::DuplicateWalObject => "sealed root repeats a sealed WAL object",
            Self::InvalidReferenceDigest => "sealed root contains an invalid object digest",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for SealedRootError {}

impl SealedRoot {
    pub fn validate(&self) -> Result<(), SealedRootError> {
        if self.checkpoint_epoch > self.commit_epoch {
            return Err(SealedRootError::InvalidEpochs);
        }
        if self.checkpoint_references.is_empty()
            || self.checkpoint_references.len() > MAX_CHECKPOINT_REFERENCES
        {
            return Err(SealedRootError::TooManyCheckpointReferences);
        }
        if self.sealed_wals.len() > MAX_WAL_REFERENCES {
            return Err(SealedRootError::TooManyWalReferences);
        }

        let mut previous_checkpoint: Option<ObjectReference> = None;
        for reference in &self.checkpoint_references {
            if !matches!(
                reference.kind,
                ObjectKind::Checkpoint | ObjectKind::CheckpointArtifact
            ) {
                return Err(SealedRootError::InvalidObjectKind);
            }
            if previous_checkpoint.is_some_and(|previous| previous >= *reference) {
                return if previous_checkpoint == Some(*reference) {
                    Err(SealedRootError::DuplicateCheckpointReference)
                } else {
                    Err(SealedRootError::UnorderedCheckpointReferences)
                };
            }
            previous_checkpoint = Some(*reference);
        }

        let mut expected_start = self.wal_replay_start_lsn;
        let mut previous_object: Option<ObjectReference> = None;
        for wal in &self.sealed_wals {
            validate_reference_kind(wal.object, ObjectKind::SealedWal)?;
            if wal.start_lsn != expected_start || wal.start_lsn >= wal.end_lsn {
                return if wal.start_lsn < expected_start {
                    Err(SealedRootError::UnorderedWalIntervals)
                } else if wal.start_lsn > expected_start {
                    Err(SealedRootError::NonContiguousWalIntervals)
                } else {
                    Err(SealedRootError::InvalidWalInterval)
                };
            }
            if previous_object == Some(wal.object) {
                return Err(SealedRootError::DuplicateWalObject);
            }
            expected_start = wal.end_lsn;
            previous_object = Some(wal.object);
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, SealedRootError> {
        self.validate()?;
        let checkpoint_count = u32::try_from(self.checkpoint_references.len())
            .map_err(|_| SealedRootError::TooManyCheckpointReferences)?;
        let wal_count = u32::try_from(self.sealed_wals.len())
            .map_err(|_| SealedRootError::TooManyWalReferences)?;
        let record_bytes = self
            .checkpoint_references
            .len()
            .saturating_mul(REFERENCE_BYTES)
            .saturating_add(self.sealed_wals.len().saturating_mul(16 + REFERENCE_BYTES));
        let total = MAGIC
            .len()
            .saturating_add(8 * 3)
            .saturating_add(4 * 2)
            .saturating_add(record_bytes)
            .saturating_add(4);
        if total > MAX_ENCODED_BYTES {
            return Err(SealedRootError::EncodedTooLarge);
        }
        let mut encoded = Vec::with_capacity(total);
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&self.checkpoint_epoch.to_le_bytes());
        encoded.extend_from_slice(&self.commit_epoch.to_le_bytes());
        encoded.extend_from_slice(&self.wal_replay_start_lsn.to_le_bytes());
        encoded.extend_from_slice(&checkpoint_count.to_le_bytes());
        encoded.extend_from_slice(&wal_count.to_le_bytes());
        for reference in &self.checkpoint_references {
            encode_reference(&mut encoded, *reference);
        }
        for wal in &self.sealed_wals {
            encoded.extend_from_slice(&wal.start_lsn.to_le_bytes());
            encoded.extend_from_slice(&wal.end_lsn.to_le_bytes());
            encode_reference(&mut encoded, wal.object);
        }
        encoded.extend_from_slice(&crc32c(&encoded).get().to_le_bytes());
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, SealedRootError> {
        if encoded.len() > MAX_ENCODED_BYTES {
            return Err(SealedRootError::EncodedTooLarge);
        }
        if encoded.len() < MAGIC.len() + 4 {
            return Err(SealedRootError::Truncated);
        }
        if &encoded[..MAGIC.len()] != MAGIC {
            return Err(SealedRootError::InvalidMagic);
        }
        let checksum_offset = encoded.len() - 4;
        let expected = u32::from_le_bytes(
            encoded[checksum_offset..]
                .try_into()
                .map_err(|_| SealedRootError::Truncated)?,
        );
        if crc32c(&encoded[..checksum_offset]).get() != expected {
            return Err(SealedRootError::ChecksumMismatch);
        }
        let mut reader = Reader::new(&encoded[MAGIC.len()..checksum_offset]);
        let checkpoint_epoch = reader.u64()?;
        let commit_epoch = reader.u64()?;
        let wal_replay_start_lsn = reader.u64()?;
        let checkpoint_count = reader.u32()? as usize;
        let wal_count = reader.u32()? as usize;
        if checkpoint_count == 0 || checkpoint_count > MAX_CHECKPOINT_REFERENCES {
            return Err(SealedRootError::TooManyCheckpointReferences);
        }
        if wal_count > MAX_WAL_REFERENCES {
            return Err(SealedRootError::TooManyWalReferences);
        }
        let mut checkpoint_references = Vec::with_capacity(checkpoint_count);
        for _ in 0..checkpoint_count {
            checkpoint_references.push(decode_reference(&mut reader)?);
        }
        let mut sealed_wals = Vec::with_capacity(wal_count);
        for _ in 0..wal_count {
            sealed_wals.push(SealedWalReference {
                start_lsn: reader.u64()?,
                end_lsn: reader.u64()?,
                object: decode_reference(&mut reader)?,
            });
        }
        if !reader.is_empty() {
            return Err(SealedRootError::TrailingBytes);
        }
        let root = Self {
            checkpoint_epoch,
            commit_epoch,
            wal_replay_start_lsn,
            checkpoint_references,
            sealed_wals,
        };
        root.validate()?;
        Ok(root)
    }

    pub fn object_reference(&self) -> Result<ObjectReference, SealedRootError> {
        let encoded = self.encode()?;
        Ok(ObjectReference::for_bytes(
            ObjectKind::SealedRoot,
            1,
            &encoded,
        ))
    }
}

fn validate_reference_kind(
    reference: ObjectReference,
    expected_kind: ObjectKind,
) -> Result<(), SealedRootError> {
    if reference.kind != expected_kind {
        return Err(SealedRootError::InvalidObjectKind);
    }
    if reference.format_version == 0 {
        return Err(SealedRootError::InvalidObjectVersion);
    }
    if reference.sha256 == Sha256Digest::from_bytes([0; 32]) {
        return Err(SealedRootError::InvalidReferenceDigest);
    }
    Ok(())
}

fn encode_reference(encoded: &mut Vec<u8>, reference: ObjectReference) {
    encoded.push(reference.kind as u8);
    encoded.extend_from_slice(&reference.format_version.to_le_bytes());
    encoded.extend_from_slice(&reference.byte_length.to_le_bytes());
    encoded.extend_from_slice(reference.sha256.as_bytes());
}

fn decode_reference(reader: &mut Reader<'_>) -> Result<ObjectReference, SealedRootError> {
    let kind = match reader.u8()? {
        1 => ObjectKind::Checkpoint,
        2 => ObjectKind::SealedWal,
        3 => ObjectKind::SealedRoot,
        4 => ObjectKind::CheckpointArtifact,
        _ => return Err(SealedRootError::InvalidObjectKind),
    };
    let format_version = reader.u16()?;
    let byte_length = reader.u64()?;
    let mut digest = [0; 32];
    digest.copy_from_slice(reader.bytes(32)?);
    let reference = ObjectReference {
        kind,
        format_version,
        byte_length,
        sha256: Sha256Digest::from_bytes(digest),
    };
    if reference.format_version == 0 {
        return Err(SealedRootError::InvalidObjectVersion);
    }
    if reference.sha256 == Sha256Digest::from_bytes([0; 32]) {
        return Err(SealedRootError::InvalidReferenceDigest);
    }
    Ok(reference)
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], SealedRootError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(SealedRootError::Truncated)?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(SealedRootError::Truncated)?;
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, SealedRootError> {
        Ok(*self.bytes(1)?.first().ok_or(SealedRootError::Truncated)?)
    }

    fn u16(&mut self) -> Result<u16, SealedRootError> {
        Ok(u16::from_le_bytes(
            self.bytes(2)?
                .try_into()
                .map_err(|_| SealedRootError::Truncated)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, SealedRootError> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?
                .try_into()
                .map_err(|_| SealedRootError::Truncated)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, SealedRootError> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?
                .try_into()
                .map_err(|_| SealedRootError::Truncated)?,
        ))
    }

    const fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(kind: ObjectKind, seed: u8) -> ObjectReference {
        ObjectReference::for_bytes(kind, 1, &[seed, seed.wrapping_add(1)])
    }

    fn sample() -> SealedRoot {
        SealedRoot {
            checkpoint_epoch: 7,
            commit_epoch: 9,
            wal_replay_start_lsn: 40,
            checkpoint_references: vec![reference(ObjectKind::Checkpoint, 1)],
            sealed_wals: vec![
                SealedWalReference {
                    start_lsn: 40,
                    end_lsn: 50,
                    object: reference(ObjectKind::SealedWal, 2),
                },
                SealedWalReference {
                    start_lsn: 50,
                    end_lsn: 70,
                    object: reference(ObjectKind::SealedWal, 3),
                },
            ],
        }
    }

    #[test]
    fn deterministic_round_trip_and_root_identity() {
        let root = sample();
        let encoded = root.encode().unwrap();
        assert_eq!(SealedRoot::decode(&encoded).unwrap(), root);
        assert_eq!(root.encode().unwrap(), encoded);
        assert_eq!(
            root.object_reference().unwrap().kind,
            ObjectKind::SealedRoot
        );
    }

    #[test]
    fn rejects_gaps_and_overlapping_wal_intervals() {
        let mut root = sample();
        root.sealed_wals[1].start_lsn = 51;
        assert_eq!(
            root.validate(),
            Err(SealedRootError::NonContiguousWalIntervals)
        );
        root.sealed_wals[1].start_lsn = 49;
        assert_eq!(root.validate(), Err(SealedRootError::UnorderedWalIntervals));
    }

    #[test]
    fn rejects_wrong_kinds_duplicate_refs_and_epoch_drift() {
        let mut root = sample();
        root.checkpoint_references[0] = reference(ObjectKind::SealedWal, 1);
        assert_eq!(root.validate(), Err(SealedRootError::InvalidObjectKind));
        root = sample();
        root.checkpoint_references
            .push(root.checkpoint_references[0]);
        assert_eq!(
            root.validate(),
            Err(SealedRootError::DuplicateCheckpointReference)
        );
        root = sample();
        root.commit_epoch = 6;
        assert_eq!(root.validate(), Err(SealedRootError::InvalidEpochs));
    }

    #[test]
    fn checksum_truncation_and_trailing_bytes_fail_closed() {
        let encoded = sample().encode().unwrap();
        let mut tampered = encoded.clone();
        tampered[20] ^= 1;
        assert_eq!(
            SealedRoot::decode(&tampered),
            Err(SealedRootError::ChecksumMismatch)
        );
        assert_eq!(
            SealedRoot::decode(&encoded[..encoded.len() - 1]),
            Err(SealedRootError::ChecksumMismatch)
        );
        let mut trailing = encoded;
        trailing.insert(trailing.len() - 4, 0);
        let checksum_offset = trailing.len() - 4;
        let checksum = crc32c(&trailing[..checksum_offset]).get().to_le_bytes();
        trailing[checksum_offset..].copy_from_slice(&checksum);
        assert_eq!(
            SealedRoot::decode(&trailing),
            Err(SealedRootError::TrailingBytes)
        );
    }

    #[test]
    fn canonical_checkpoint_order_is_required() {
        let mut root = sample();
        let first = reference(ObjectKind::Checkpoint, 0);
        let second = reference(ObjectKind::Checkpoint, 1);
        let (high, low) = if first > second {
            (first, second)
        } else {
            (second, first)
        };
        root.checkpoint_references = vec![high, low];
        assert_eq!(
            root.validate(),
            Err(SealedRootError::UnorderedCheckpointReferences)
        );
    }
}
