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

//! Immutable content-addressed blocks for block-level COW checkpoints.
//!
//! This codec is intentionally independent of a generation manifest and the
//! checkpoint writer. See `docs/specs/BLOCK_COW_CHECKPOINT_SPEC.md`.

use crate::immutable_object::{ObjectKind, ObjectReference};
use hawdb_integrity::{Crc32cHasher, IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::fmt::{self, Display, Formatter};

pub const BLOCK_MAGIC: &[u8; 8] = b"SKNBLK01";
pub const BLOCK_FORMAT_VERSION: u16 = 1;
pub const BLOCK_HEADER_BYTES: usize = 80;
pub const BLOCK_DIGEST_BYTES: usize = SHA256_BYTES;
pub const DEFAULT_CHECKPOINT_BLOCK_TARGET_BYTES: u64 = 2 * 1024 * 1024;
/// Smallest encodable block: header, digest and one payload byte.
pub const MIN_CHECKPOINT_BLOCK_BYTES: u64 = (BLOCK_HEADER_BYTES + BLOCK_DIGEST_BYTES + 1) as u64;
pub const MAX_CHECKPOINT_BLOCK_BYTES: u64 = 16 * 1024 * 1024;

const FLAG_SPILL_NAMESPACE: u8 = 1;
const KNOWN_FLAGS: u8 = FLAG_SPILL_NAMESPACE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CheckpointBlockKind {
    CanonicalNodes = 1,
    CanonicalRelationships = 2,
    Adjacency = 3,
    PropertyProjection = 4,
}

impl CheckpointBlockKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CanonicalNodes => "canonical nodes",
            Self::CanonicalRelationships => "canonical relationships",
            Self::Adjacency => "adjacency",
            Self::PropertyProjection => "property projection",
        }
    }
}

impl TryFrom<u8> for CheckpointBlockKind {
    type Error = CheckpointBlockError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::CanonicalNodes),
            2 => Ok(Self::CanonicalRelationships),
            3 => Ok(Self::Adjacency),
            4 => Ok(Self::PropertyProjection),
            other => Err(CheckpointBlockError::UnsupportedKind(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointBlockHeader {
    pub format_version: u16,
    pub kind: CheckpointBlockKind,
    /// Inclusive first key of the stable bucket.
    pub bucket_start: u64,
    /// Inclusive last key of the stable bucket. May be `u64::MAX`.
    pub bucket_end: u64,
    pub part: u32,
    pub record_count: u32,
    pub payload_len: u64,
    pub spill_namespace: Option<Sha256Digest>,
    pub payload_crc32c: u32,
}

pub struct CheckpointBlockSpec<'a> {
    pub kind: CheckpointBlockKind,
    pub bucket_start: u64,
    pub bucket_end: u64,
    pub part: u32,
    pub record_count: u32,
    pub spill_namespace: Option<Sha256Digest>,
    pub payload: &'a [u8],
}

pub struct CheckpointBlock<'a> {
    encoded: &'a [u8],
    header: CheckpointBlockHeader,
    payload: &'a [u8],
}

impl<'a> CheckpointBlock<'a> {
    pub fn header(&self) -> CheckpointBlockHeader {
        self.header
    }

    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }

    pub fn encoded(&self) -> &'a [u8] {
        self.encoded
    }

    pub fn object_reference(&self) -> ObjectReference {
        ObjectReference::for_bytes(
            ObjectKind::CheckpointBlock,
            self.header.format_version,
            self.encoded,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointBlockError {
    UnsupportedVersion { found: u16, supported: u16 },
    UnsupportedKind(u8),
    Corrupt(&'static str),
    TooLarge { bytes: u64, max: u64 },
}

impl Display for CheckpointBlockError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { found, supported } => write!(
                formatter,
                "checkpoint block format version {found} is unsupported; supported version is {supported}"
            ),
            Self::UnsupportedKind(kind) => {
                write!(formatter, "checkpoint block kind {kind} is unsupported")
            }
            Self::Corrupt(message) => formatter.write_str(message),
            Self::TooLarge { bytes, max } => write!(
                formatter,
                "checkpoint block uses {bytes} bytes, exceeding its {max}-byte limit"
            ),
        }
    }
}

impl std::error::Error for CheckpointBlockError {}

pub fn encode(spec: CheckpointBlockSpec<'_>) -> Result<Vec<u8>, CheckpointBlockError> {
    if spec.bucket_start > spec.bucket_end {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block bucket range endpoints are inverted",
        ));
    }
    if spec.record_count == 0 {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block contains no records",
        ));
    }
    if spec.payload.is_empty() {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block payload is empty",
        ));
    }
    if spec
        .spill_namespace
        .is_some_and(|digest| digest.as_bytes() == &[0; SHA256_BYTES])
    {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block has an empty spill namespace",
        ));
    }
    if spec.payload.len() as u64 > MAX_CHECKPOINT_BLOCK_BYTES {
        return Err(CheckpointBlockError::TooLarge {
            bytes: spec.payload.len() as u64,
            max: MAX_CHECKPOINT_BLOCK_BYTES,
        });
    }
    let total = BLOCK_HEADER_BYTES
        .checked_add(spec.payload.len())
        .and_then(|bytes| bytes.checked_add(BLOCK_DIGEST_BYTES))
        .ok_or(CheckpointBlockError::TooLarge {
            bytes: u64::MAX,
            max: MAX_CHECKPOINT_BLOCK_BYTES,
        })?;
    if total as u64 > MAX_CHECKPOINT_BLOCK_BYTES {
        return Err(CheckpointBlockError::TooLarge {
            bytes: total as u64,
            max: MAX_CHECKPOINT_BLOCK_BYTES,
        });
    }

    let mut payload_hasher = Crc32cHasher::new();
    payload_hasher.update(spec.payload);
    let payload_crc32c = payload_hasher.finish_u32();
    let spill_namespace = spec.spill_namespace.map(|digest| *digest.as_bytes());

    let mut encoded = Vec::with_capacity(total);
    encoded.extend_from_slice(BLOCK_MAGIC);
    encoded.extend_from_slice(&BLOCK_FORMAT_VERSION.to_le_bytes());
    encoded.push(spec.kind as u8);
    encoded.push(u8::from(spill_namespace.is_some()) * FLAG_SPILL_NAMESPACE);
    encoded.extend_from_slice(&spec.bucket_start.to_le_bytes());
    encoded.extend_from_slice(&spec.bucket_end.to_le_bytes());
    encoded.extend_from_slice(&spec.part.to_le_bytes());
    encoded.extend_from_slice(&spec.record_count.to_le_bytes());
    encoded.extend_from_slice(&(spec.payload.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&spill_namespace.unwrap_or([0; SHA256_BYTES]));
    encoded.extend_from_slice(&payload_crc32c.to_le_bytes());
    debug_assert_eq!(encoded.len(), BLOCK_HEADER_BYTES);
    encoded.extend_from_slice(spec.payload);
    let mut digest_hasher = IntegrityHasher::new();
    digest_hasher.update(&encoded);
    let digest = digest_hasher.finish().sha256;
    encoded.extend_from_slice(digest.as_bytes());
    Ok(encoded)
}

pub fn decode(encoded: &[u8]) -> Result<CheckpointBlock<'_>, CheckpointBlockError> {
    if encoded.len() < BLOCK_HEADER_BYTES + BLOCK_DIGEST_BYTES {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block is shorter than its header and digest",
        ));
    }
    if &encoded[..8] != BLOCK_MAGIC {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block magic is invalid",
        ));
    }
    let format_version = u16::from_le_bytes(encoded[8..10].try_into().expect("fixed version"));
    if format_version != BLOCK_FORMAT_VERSION {
        return Err(CheckpointBlockError::UnsupportedVersion {
            found: format_version,
            supported: BLOCK_FORMAT_VERSION,
        });
    }
    let kind = CheckpointBlockKind::try_from(encoded[10])?;
    let flags = encoded[11];
    if flags & !KNOWN_FLAGS != 0 {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block has reserved flags set",
        ));
    }
    let bucket_start = u64::from_le_bytes(encoded[12..20].try_into().expect("fixed bucket start"));
    let bucket_end = u64::from_le_bytes(encoded[20..28].try_into().expect("fixed bucket end"));
    if bucket_start > bucket_end {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block bucket range endpoints are inverted",
        ));
    }
    let part = u32::from_le_bytes(encoded[28..32].try_into().expect("fixed part"));
    let record_count = u32::from_le_bytes(encoded[32..36].try_into().expect("fixed record count"));
    if record_count == 0 {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block contains no records",
        ));
    }
    let payload_len = u64::from_le_bytes(encoded[36..44].try_into().expect("fixed payload length"));
    if payload_len == 0 {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block payload is empty",
        ));
    }
    let payload_len = usize::try_from(payload_len).map_err(|_| {
        CheckpointBlockError::Corrupt("checkpoint block payload length overflows usize")
    })?;
    if payload_len > MAX_CHECKPOINT_BLOCK_BYTES as usize {
        return Err(CheckpointBlockError::TooLarge {
            bytes: payload_len as u64,
            max: MAX_CHECKPOINT_BLOCK_BYTES,
        });
    }
    let spill_namespace = if flags & FLAG_SPILL_NAMESPACE != 0 {
        let digest = Sha256Digest::from_bytes(
            encoded[44..76]
                .try_into()
                .expect("fixed spill namespace digest"),
        );
        if digest.as_bytes() == &[0; SHA256_BYTES] {
            return Err(CheckpointBlockError::Corrupt(
                "checkpoint block has an empty spill namespace",
            ));
        }
        Some(digest)
    } else {
        if encoded[44..76].iter().any(|byte| *byte != 0) {
            return Err(CheckpointBlockError::Corrupt(
                "checkpoint block has a spill namespace without its flag",
            ));
        }
        None
    };
    let payload_crc32c =
        u32::from_le_bytes(encoded[76..80].try_into().expect("fixed payload checksum"));
    let total = BLOCK_HEADER_BYTES
        .checked_add(payload_len)
        .and_then(|bytes| bytes.checked_add(BLOCK_DIGEST_BYTES))
        .ok_or(CheckpointBlockError::Corrupt(
            "checkpoint block length overflows usize",
        ))?;
    if total != encoded.len() {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block length does not match its payload",
        ));
    }
    if total as u64 > MAX_CHECKPOINT_BLOCK_BYTES {
        return Err(CheckpointBlockError::TooLarge {
            bytes: total as u64,
            max: MAX_CHECKPOINT_BLOCK_BYTES,
        });
    }
    let payload = &encoded[BLOCK_HEADER_BYTES..BLOCK_HEADER_BYTES + payload_len];
    let mut payload_hasher = Crc32cHasher::new();
    payload_hasher.update(payload);
    if payload_hasher.finish_u32() != payload_crc32c {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block payload checksum mismatch",
        ));
    }
    let content_len = BLOCK_HEADER_BYTES + payload_len;
    let mut digest_hasher = IntegrityHasher::new();
    digest_hasher.update(&encoded[..content_len]);
    let expected = digest_hasher.finish().sha256;
    let actual = Sha256Digest::from_bytes(
        encoded[content_len..]
            .try_into()
            .expect("fixed block digest length"),
    );
    if expected != actual {
        return Err(CheckpointBlockError::Corrupt(
            "checkpoint block digest mismatch",
        ));
    }
    Ok(CheckpointBlock {
        encoded,
        header: CheckpointBlockHeader {
            format_version,
            kind,
            bucket_start,
            bucket_end,
            part,
            record_count,
            payload_len: payload_len as u64,
            spill_namespace,
            payload_crc32c,
        },
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: u8) -> Sha256Digest {
        Sha256Digest::from_bytes([byte; SHA256_BYTES])
    }

    fn spec<'a>(kind: CheckpointBlockKind, payload: &'a [u8]) -> CheckpointBlockSpec<'a> {
        CheckpointBlockSpec {
            kind,
            bucket_start: 10,
            bucket_end: 20,
            part: 0,
            record_count: 3,
            spill_namespace: None,
            payload,
        }
    }

    #[test]
    fn round_trips_every_kind_and_spill_namespace() {
        for kind in [
            CheckpointBlockKind::CanonicalNodes,
            CheckpointBlockKind::CanonicalRelationships,
            CheckpointBlockKind::Adjacency,
            CheckpointBlockKind::PropertyProjection,
        ] {
            for spill_namespace in [None, Some(digest(7))] {
                let mut block = spec(kind, b"encoded records");
                block.spill_namespace = spill_namespace;
                let encoded = encode(block).unwrap();
                let decoded = decode(&encoded).unwrap();
                let header = decoded.header();
                assert_eq!(header.format_version, BLOCK_FORMAT_VERSION);
                assert_eq!(header.kind, kind);
                assert_eq!(header.bucket_start, 10);
                assert_eq!(header.bucket_end, 20);
                assert_eq!(header.part, 0);
                assert_eq!(header.record_count, 3);
                assert_eq!(header.payload_len, 15);
                assert_eq!(header.spill_namespace, spill_namespace);
                assert_eq!(decoded.payload(), b"encoded records");
                let reference = decoded.object_reference();
                assert_eq!(reference.kind, ObjectKind::CheckpointBlock);
                assert_eq!(reference.format_version, BLOCK_FORMAT_VERSION);
                assert_eq!(reference.byte_length, encoded.len() as u64);
            }
        }
    }

    #[test]
    fn identity_separates_kind_bucket_part_and_spill_namespace() {
        let payload = b"same payload";
        let base = encode(spec(CheckpointBlockKind::CanonicalNodes, payload)).unwrap();
        let mut variants = Vec::new();
        let mut changed = spec(CheckpointBlockKind::CanonicalRelationships, payload);
        variants.push(encode(changed).unwrap());
        changed = spec(CheckpointBlockKind::CanonicalNodes, payload);
        changed.bucket_start = 11;
        variants.push(encode(changed).unwrap());
        changed = spec(CheckpointBlockKind::CanonicalNodes, payload);
        changed.part = 1;
        variants.push(encode(changed).unwrap());
        changed = spec(CheckpointBlockKind::CanonicalNodes, payload);
        changed.spill_namespace = Some(digest(9));
        variants.push(encode(changed).unwrap());
        let base_digest = decode(&base).unwrap().object_reference().sha256;
        for variant in variants {
            assert_ne!(
                base_digest,
                decode(&variant).unwrap().object_reference().sha256
            );
        }
    }

    #[test]
    fn accepts_inclusive_bucket_endpoints_and_upper_ids() {
        let mut block = spec(CheckpointBlockKind::CanonicalNodes, b"x");
        block.bucket_start = 5;
        block.bucket_end = 5;
        block.part = u32::MAX;
        block.record_count = u32::MAX;
        let encoded = encode(block).unwrap();
        let header = decode(&encoded).unwrap().header();
        assert_eq!(header.bucket_start, 5);
        assert_eq!(header.bucket_end, 5);
        assert_eq!(header.part, u32::MAX);
        assert_eq!(header.record_count, u32::MAX);

        let mut block = spec(CheckpointBlockKind::CanonicalRelationships, b"x");
        block.bucket_start = u64::MAX;
        block.bucket_end = u64::MAX;
        let encoded = encode(block).unwrap();
        let header = decode(&encoded).unwrap().header();
        assert_eq!(header.bucket_start, u64::MAX);
        assert_eq!(header.bucket_end, u64::MAX);
    }

    #[test]
    fn enforces_the_exact_maximum_encoded_length() {
        let max_payload =
            MAX_CHECKPOINT_BLOCK_BYTES as usize - BLOCK_HEADER_BYTES - BLOCK_DIGEST_BYTES;
        let payload = vec![0x5a; max_payload];
        let encoded = encode(spec(CheckpointBlockKind::Adjacency, &payload)).unwrap();
        assert_eq!(encoded.len() as u64, MAX_CHECKPOINT_BLOCK_BYTES);
        assert_eq!(decode(&encoded).unwrap().payload(), payload);

        let too_large = vec![0x5a; max_payload + 1];
        assert!(matches!(
            encode(spec(CheckpointBlockKind::Adjacency, &too_large)),
            Err(CheckpointBlockError::TooLarge { .. })
        ));
    }

    #[test]
    fn rejects_invalid_specs() {
        let mut invalid = spec(CheckpointBlockKind::CanonicalNodes, b"x");
        invalid.record_count = 0;
        assert!(encode(invalid).is_err());
        let mut invalid = spec(CheckpointBlockKind::CanonicalNodes, b"");
        invalid.record_count = 1;
        assert!(encode(invalid).is_err());
        let mut invalid = spec(CheckpointBlockKind::CanonicalNodes, b"x");
        invalid.bucket_end = invalid.bucket_start - 1;
        assert!(encode(invalid).is_err());
        let mut invalid = spec(CheckpointBlockKind::CanonicalNodes, b"x");
        invalid.spill_namespace = Some(Sha256Digest::from_bytes([0; SHA256_BYTES]));
        assert!(encode(invalid).is_err());
    }

    #[test]
    fn rejects_corrupt_headers_payload_and_digest() {
        let encoded = encode(spec(CheckpointBlockKind::CanonicalNodes, b"payload")).unwrap();
        let mut damaged = encoded.clone();
        damaged[0] ^= 1;
        assert!(matches!(
            decode(&damaged),
            Err(CheckpointBlockError::Corrupt(_))
        ));
        let mut damaged = encoded.clone();
        damaged[8..10].copy_from_slice(&(BLOCK_FORMAT_VERSION + 1).to_le_bytes());
        assert!(matches!(
            decode(&damaged),
            Err(CheckpointBlockError::UnsupportedVersion { .. })
        ));
        let mut damaged = encoded.clone();
        damaged[10] = 0;
        assert!(matches!(
            decode(&damaged),
            Err(CheckpointBlockError::UnsupportedKind(0))
        ));
        let mut damaged = encoded.clone();
        damaged[11] = 2;
        assert!(matches!(
            decode(&damaged),
            Err(CheckpointBlockError::Corrupt(_))
        ));
        let mut damaged = encoded.clone();
        damaged[76] ^= 1;
        assert!(matches!(
            decode(&damaged),
            Err(CheckpointBlockError::Corrupt(_))
        ));
        let mut damaged = encoded.clone();
        let last = damaged.len() - 1;
        damaged[last] ^= 1;
        assert!(matches!(
            decode(&damaged),
            Err(CheckpointBlockError::Corrupt(_))
        ));
        assert!(decode(&encoded[..encoded.len() - 1]).is_err());
        assert!(decode(&[encoded.as_slice(), b"x"].concat()).is_err());
    }
}
