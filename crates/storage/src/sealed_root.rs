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
use std::path::{Component, Path};

const MAGIC: &[u8; 12] = b"HAWDBROOTV2\0";
const REFERENCE_BYTES: usize = 1 + 2 + 8 + 32;
const MAX_CHECKPOINT_REFERENCES: usize = 4096;
const MAX_CHECKPOINT_BINDINGS: usize = 4096;
const MAX_WAL_REFERENCES: usize = 1_000_000;
const MAX_ENCODED_BYTES: usize = 64 * 1024 * 1024;
const MAX_ARTIFACT_PATH_BYTES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedWalReference {
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub object: ObjectReference,
}

/// Binds a root-closure object to the exact path named by its durable
/// manifest.  Paths are relative to the database directory and have a
/// canonical portable representation so a root can be materialized without
/// consulting the source database's current manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointArtifactBinding {
    pub relative_path: String,
    pub reference: ObjectReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedRoot {
    pub checkpoint_epoch: u64,
    pub commit_epoch: u64,
    pub wal_replay_start_lsn: u64,
    pub durable_manifest: ObjectReference,
    pub checkpoint_references: Vec<ObjectReference>,
    pub checkpoint_bindings: Vec<CheckpointArtifactBinding>,
    pub sealed_wals: Vec<SealedWalReference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealedRootError {
    InvalidMagic,
    Truncated,
    ChecksumMismatch,
    TrailingBytes,
    TooManyCheckpointReferences,
    TooManyCheckpointBindings,
    TooManyWalReferences,
    EncodedTooLarge,
    InvalidEpochs,
    InvalidObjectKind,
    InvalidObjectVersion,
    DuplicateCheckpointReference,
    UnorderedCheckpointReferences,
    DuplicateArtifactPath,
    UnorderedArtifactBindings,
    InvalidArtifactPath,
    BindingReferenceNotInClosure,
    ClosureReferenceWithoutBinding,
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
            Self::TooManyCheckpointBindings => "sealed root has too many checkpoint bindings",
            Self::TooManyWalReferences => "sealed root has too many sealed WAL references",
            Self::EncodedTooLarge => "sealed root encoding exceeds its byte limit",
            Self::InvalidEpochs => "sealed root epochs are inconsistent",
            Self::InvalidObjectKind => "sealed root contains an object of the wrong kind",
            Self::InvalidObjectVersion => "sealed root contains an object with version zero",
            Self::DuplicateCheckpointReference => "sealed root repeats a checkpoint reference",
            Self::UnorderedCheckpointReferences => {
                "sealed root checkpoint references are not canonical"
            }
            Self::DuplicateArtifactPath => "sealed root repeats an artifact path",
            Self::UnorderedArtifactBindings => "sealed root artifact bindings are not canonical",
            Self::InvalidArtifactPath => "sealed root contains an invalid artifact path",
            Self::BindingReferenceNotInClosure => {
                "sealed root artifact binding is absent from the checkpoint closure"
            }
            Self::ClosureReferenceWithoutBinding => {
                "sealed root checkpoint closure has no artifact path binding"
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

        validate_reference_kind(self.durable_manifest, ObjectKind::DurableManifest)?;

        let mut previous_checkpoint: Option<ObjectReference> = None;
        for reference in &self.checkpoint_references {
            validate_reference(*reference)?;
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

        if self.checkpoint_bindings.len() != self.checkpoint_references.len()
            || self.checkpoint_bindings.len() > MAX_CHECKPOINT_BINDINGS
        {
            return Err(SealedRootError::TooManyCheckpointBindings);
        }

        let mut previous_path: Option<&str> = None;
        for binding in &self.checkpoint_bindings {
            validate_artifact_path(&binding.relative_path)?;
            if previous_path.is_some_and(|previous| previous >= binding.relative_path.as_str()) {
                return if previous_path == Some(binding.relative_path.as_str()) {
                    Err(SealedRootError::DuplicateArtifactPath)
                } else {
                    Err(SealedRootError::UnorderedArtifactBindings)
                };
            }
            if self
                .checkpoint_references
                .binary_search(&binding.reference)
                .is_err()
            {
                return Err(SealedRootError::BindingReferenceNotInClosure);
            }
            previous_path = Some(&binding.relative_path);
        }
        for reference in &self.checkpoint_references {
            if !self
                .checkpoint_bindings
                .iter()
                .any(|binding| binding.reference == *reference)
            {
                return Err(SealedRootError::ClosureReferenceWithoutBinding);
            }
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
        let binding_count = u32::try_from(self.checkpoint_bindings.len())
            .map_err(|_| SealedRootError::TooManyCheckpointBindings)?;
        let wal_count = u32::try_from(self.sealed_wals.len())
            .map_err(|_| SealedRootError::TooManyWalReferences)?;
        let record_bytes = self
            .checkpoint_references
            .len()
            .saturating_mul(REFERENCE_BYTES)
            .saturating_add(
                self.checkpoint_bindings
                    .iter()
                    .map(|binding| 4usize.saturating_add(binding.relative_path.len()))
                    .sum::<usize>()
                    .saturating_add(
                        self.checkpoint_bindings
                            .len()
                            .saturating_mul(REFERENCE_BYTES),
                    ),
            )
            .saturating_add(self.sealed_wals.len().saturating_mul(16 + REFERENCE_BYTES));
        let total = MAGIC
            .len()
            .saturating_add(8 * 3)
            .saturating_add(REFERENCE_BYTES)
            .saturating_add(4 * 3)
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
        encode_reference(&mut encoded, self.durable_manifest);
        encoded.extend_from_slice(&checkpoint_count.to_le_bytes());
        encoded.extend_from_slice(&binding_count.to_le_bytes());
        encoded.extend_from_slice(&wal_count.to_le_bytes());
        for reference in &self.checkpoint_references {
            encode_reference(&mut encoded, *reference);
        }
        for binding in &self.checkpoint_bindings {
            let path = binding.relative_path.as_bytes();
            encoded.extend_from_slice(&(path.len() as u32).to_le_bytes());
            encoded.extend_from_slice(path);
            encode_reference(&mut encoded, binding.reference);
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
        let durable_manifest = decode_reference(&mut reader)?;
        let checkpoint_count = reader.u32()? as usize;
        let binding_count = reader.u32()? as usize;
        let wal_count = reader.u32()? as usize;
        if checkpoint_count == 0 || checkpoint_count > MAX_CHECKPOINT_REFERENCES {
            return Err(SealedRootError::TooManyCheckpointReferences);
        }
        if wal_count > MAX_WAL_REFERENCES {
            return Err(SealedRootError::TooManyWalReferences);
        }
        if binding_count != checkpoint_count || binding_count > MAX_CHECKPOINT_BINDINGS {
            return Err(SealedRootError::TooManyCheckpointBindings);
        }
        let mut checkpoint_references = Vec::with_capacity(checkpoint_count);
        for _ in 0..checkpoint_count {
            checkpoint_references.push(decode_reference(&mut reader)?);
        }
        let mut checkpoint_bindings = Vec::with_capacity(binding_count);
        for _ in 0..binding_count {
            let path_length = reader.u32()? as usize;
            if path_length == 0 || path_length > MAX_ARTIFACT_PATH_BYTES {
                return Err(SealedRootError::InvalidArtifactPath);
            }
            let path = std::str::from_utf8(reader.bytes(path_length)?)
                .map_err(|_| SealedRootError::InvalidArtifactPath)?
                .to_owned();
            checkpoint_bindings.push(CheckpointArtifactBinding {
                relative_path: path,
                reference: decode_reference(&mut reader)?,
            });
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
            durable_manifest,
            checkpoint_references,
            checkpoint_bindings,
            sealed_wals,
        };
        root.validate()?;
        Ok(root)
    }

    pub fn object_reference(&self) -> Result<ObjectReference, SealedRootError> {
        let encoded = self.encode()?;
        Ok(ObjectReference::for_bytes(
            ObjectKind::SealedRoot,
            2,
            &encoded,
        ))
    }
}

fn validate_artifact_path(path: &str) -> Result<(), SealedRootError> {
    if path.is_empty()
        || path.len() > MAX_ARTIFACT_PATH_BYTES
        || path.as_bytes().contains(&0)
        || path.contains('\\')
        || path
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(SealedRootError::InvalidArtifactPath);
    }
    let path = Path::new(path);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(SealedRootError::InvalidArtifactPath);
    }
    Ok(())
}

fn validate_reference_kind(
    reference: ObjectReference,
    expected_kind: ObjectKind,
) -> Result<(), SealedRootError> {
    if reference.kind != expected_kind {
        return Err(SealedRootError::InvalidObjectKind);
    }
    validate_reference(reference)
}

fn validate_reference(reference: ObjectReference) -> Result<(), SealedRootError> {
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
        5 => ObjectKind::DurableManifest,
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
        let checkpoint = reference(ObjectKind::Checkpoint, 1);
        SealedRoot {
            checkpoint_epoch: 7,
            commit_epoch: 9,
            wal_replay_start_lsn: 40,
            durable_manifest: reference(ObjectKind::DurableManifest, 4),
            checkpoint_references: vec![checkpoint],
            checkpoint_bindings: vec![CheckpointArtifactBinding {
                relative_path: "checkpoint.7.hawdb".to_string(),
                reference: checkpoint,
            }],
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
        assert_eq!(root.object_reference().unwrap().format_version, 2);
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
        root.durable_manifest = reference(ObjectKind::Checkpoint, 4);
        assert_eq!(root.validate(), Err(SealedRootError::InvalidObjectKind));
        root = sample();
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
    fn rejects_the_retired_v1_root_codec() {
        let mut encoded = sample().encode().unwrap();
        encoded[..MAGIC.len()].copy_from_slice(b"HAWDBROOTV1\0");
        let checksum_offset = encoded.len() - 4;
        let checksum = crc32c(&encoded[..checksum_offset]).get().to_le_bytes();
        encoded[checksum_offset..].copy_from_slice(&checksum);
        assert_eq!(
            SealedRoot::decode(&encoded),
            Err(SealedRootError::InvalidMagic)
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

    #[test]
    fn artifact_bindings_must_be_complete_canonical_and_safe() {
        let mut root = sample();
        root.checkpoint_bindings[0].relative_path = "../checkpoint".to_string();
        assert_eq!(root.validate(), Err(SealedRootError::InvalidArtifactPath));

        root = sample();
        root.checkpoint_bindings[0].reference = reference(ObjectKind::CheckpointArtifact, 9);
        assert_eq!(
            root.validate(),
            Err(SealedRootError::BindingReferenceNotInClosure)
        );

        root = sample();
        root.checkpoint_bindings.clear();
        assert_eq!(
            root.validate(),
            Err(SealedRootError::TooManyCheckpointBindings)
        );
    }
}
