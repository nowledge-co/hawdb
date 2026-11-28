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

use super::*;
use hawdb_integrity::IntegrityHasher;

fn digest(byte: u8) -> Sha256Digest {
    Sha256Digest::from_bytes([byte; SHA256_BYTES])
}

fn block(byte: u8) -> ObjectReference {
    ObjectReference {
        kind: ObjectKind::CheckpointBlock,
        format_version: BLOCK_FORMAT_VERSION,
        byte_length: 128,
        sha256: digest(byte),
    }
}

fn entry(
    kind: CheckpointBlockKind,
    bucket_start: u64,
    bucket_end: u64,
    part: u32,
    byte: u8,
) -> CheckpointBlockEntry {
    CheckpointBlockEntry {
        kind,
        bucket_start,
        bucket_end,
        part,
        record_count: 2,
        block: block(byte),
        spill_namespace: None,
    }
}

#[test]
fn entry_limit_matches_the_encoded_byte_limit() {
    let maximum_bytes = MAX_CHECKPOINT_BLOCK_TABLE_ENTRIES * BLOCK_TABLE_ENTRY_BYTES
        + BLOCK_TABLE_HEADER_BYTES
        + BLOCK_TABLE_DIGEST_BYTES;
    assert!(maximum_bytes <= MAX_CHECKPOINT_BLOCK_TABLE_BYTES as usize);
    assert!(maximum_bytes + BLOCK_TABLE_ENTRY_BYTES > MAX_CHECKPOINT_BLOCK_TABLE_BYTES as usize);
    assert!(validate_capacity(MAX_CHECKPOINT_BLOCK_TABLE_ENTRIES).is_ok());
    assert!(matches!(
        validate_capacity(MAX_CHECKPOINT_BLOCK_TABLE_ENTRIES + 1),
        Err(CheckpointBlockTableError::TooManyEntries { .. })
    ));
}

#[test]
fn round_trips_empty_and_ordered_tables() {
    let empty = CheckpointBlockTable::new(Vec::new()).unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    let encoded = encode(&empty).unwrap();
    assert_eq!(decode(&encoded).unwrap(), empty);
    assert_eq!(
        empty.object_reference().unwrap().kind,
        ObjectKind::CheckpointBlockTable
    );

    let table = CheckpointBlockTable::new(vec![
        entry(CheckpointBlockKind::CanonicalNodes, 0, 99, 0, 1),
        entry(CheckpointBlockKind::CanonicalNodes, 0, 99, 1, 2),
        entry(CheckpointBlockKind::CanonicalNodes, 100, u64::MAX, 0, 3),
        entry(CheckpointBlockKind::CanonicalRelationships, 0, 10, 0, 4),
        entry(CheckpointBlockKind::Adjacency, 7, 7, 0, 5),
    ])
    .unwrap();
    let encoded = encode(&table).unwrap();
    let decoded = decode(&encoded).unwrap();
    assert_eq!(decoded, table);
    assert_eq!(decoded.entries()[2].bucket_end, u64::MAX);
}

#[test]
fn preserves_spill_namespace_binding() {
    let mut first = entry(CheckpointBlockKind::CanonicalNodes, 0, 10, 0, 1);
    first.spill_namespace = Some(digest(9));
    let second = entry(CheckpointBlockKind::CanonicalNodes, 11, 20, 0, 2);
    let table = CheckpointBlockTable::new(vec![first, second]).unwrap();
    let decoded = decode(&encode(&table).unwrap()).unwrap();
    assert_eq!(decoded.entries()[0].spill_namespace, Some(digest(9)));
    assert_eq!(decoded.entries()[1].spill_namespace, None);
}

#[test]
fn rejects_block_lengths_outside_the_codec_bounds() {
    let minimum = MIN_CHECKPOINT_BLOCK_BYTES;
    let maximum = MAX_CHECKPOINT_BLOCK_BYTES;
    let mut first = entry(CheckpointBlockKind::CanonicalNodes, 0, 0, 0, 1);
    first.block.byte_length = minimum;
    let mut second = entry(CheckpointBlockKind::CanonicalNodes, 1, 1, 0, 2);
    second.block.byte_length = maximum;
    assert!(CheckpointBlockTable::new(vec![first, second]).is_ok());

    let mut too_short = entry(CheckpointBlockKind::CanonicalNodes, 0, 0, 0, 1);
    too_short.block.byte_length = minimum - 1;
    assert!(CheckpointBlockTable::new(vec![too_short]).is_err());

    let mut too_long = entry(CheckpointBlockKind::CanonicalNodes, 0, 0, 0, 1);
    too_long.block.byte_length = maximum + 1;
    assert!(CheckpointBlockTable::new(vec![too_long]).is_err());
}

#[test]
fn decode_validates_block_lengths_after_a_valid_table_digest() {
    let table =
        CheckpointBlockTable::new(vec![entry(CheckpointBlockKind::CanonicalNodes, 0, 0, 0, 1)])
            .unwrap();
    let mut encoded = encode(&table).unwrap();
    let length_offset = BLOCK_TABLE_HEADER_BYTES + 28;
    encoded[length_offset..length_offset + 8]
        .copy_from_slice(&(MIN_CHECKPOINT_BLOCK_BYTES - 1).to_le_bytes());
    let content_len = encoded.len() - BLOCK_TABLE_DIGEST_BYTES;
    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded[..content_len]);
    encoded[content_len..].copy_from_slice(hasher.finish().sha256.as_bytes());
    assert!(matches!(
        decode(&encoded),
        Err(CheckpointBlockTableError::Corrupt(_))
    ));
}

#[test]
fn rejects_overlapping_and_unordered_entries() {
    assert!(CheckpointBlockTable::new(vec![
        entry(CheckpointBlockKind::CanonicalNodes, 0, 10, 0, 1),
        entry(CheckpointBlockKind::CanonicalNodes, 10, 20, 0, 2),
    ])
    .is_err());
    assert!(CheckpointBlockTable::new(vec![
        entry(CheckpointBlockKind::CanonicalNodes, 0, 10, 0, 1),
        entry(CheckpointBlockKind::CanonicalNodes, 0, 10, 0, 2),
    ])
    .is_err());
    assert!(CheckpointBlockTable::new(vec![
        entry(CheckpointBlockKind::CanonicalNodes, 0, 10, 0, 1),
        entry(CheckpointBlockKind::CanonicalNodes, 0, 10, 2, 2),
    ])
    .is_err());
    assert!(CheckpointBlockTable::new(vec![
        entry(CheckpointBlockKind::CanonicalRelationships, 0, 10, 0, 1),
        entry(CheckpointBlockKind::CanonicalNodes, 11, 20, 0, 2),
    ])
    .is_err());
    assert!(CheckpointBlockTable::new(vec![entry(
        CheckpointBlockKind::CanonicalNodes,
        10,
        9,
        0,
        1,
    )])
    .is_err());
}

#[test]
fn rejects_invalid_block_references() {
    let mut wrong_kind = entry(CheckpointBlockKind::CanonicalNodes, 0, 10, 0, 1);
    wrong_kind.block.kind = ObjectKind::CheckpointArtifact;
    assert!(CheckpointBlockTable::new(vec![wrong_kind]).is_err());

    let mut wrong_version = entry(CheckpointBlockKind::CanonicalNodes, 0, 10, 0, 1);
    wrong_version.block.format_version += 1;
    assert!(CheckpointBlockTable::new(vec![wrong_version]).is_err());

    let mut empty_block = entry(CheckpointBlockKind::CanonicalNodes, 0, 10, 0, 1);
    empty_block.block.byte_length = 0;
    assert!(CheckpointBlockTable::new(vec![empty_block]).is_err());

    let mut zero_digest = entry(CheckpointBlockKind::CanonicalNodes, 0, 10, 0, 1);
    zero_digest.block.sha256 = digest(0);
    assert!(CheckpointBlockTable::new(vec![zero_digest]).is_err());

    let mut zero_spill = entry(CheckpointBlockKind::CanonicalNodes, 0, 10, 0, 1);
    zero_spill.spill_namespace = Some(digest(0));
    assert!(CheckpointBlockTable::new(vec![zero_spill]).is_err());
}

#[test]
fn rejects_corrupt_table_headers_and_digests() {
    let table = CheckpointBlockTable::new(vec![entry(
        CheckpointBlockKind::CanonicalNodes,
        0,
        10,
        0,
        1,
    )])
    .unwrap();
    let encoded = encode(&table).unwrap();

    let mut damaged = encoded.clone();
    damaged[0] ^= 1;
    assert!(matches!(
        decode(&damaged),
        Err(CheckpointBlockTableError::Corrupt(_))
    ));

    let mut damaged = encoded.clone();
    damaged[8..10].copy_from_slice(&(BLOCK_TABLE_FORMAT_VERSION + 1).to_le_bytes());
    assert!(matches!(
        decode(&damaged),
        Err(CheckpointBlockTableError::UnsupportedVersion { .. })
    ));

    let mut damaged = encoded.clone();
    damaged[14] = 1;
    assert!(matches!(
        decode(&damaged),
        Err(CheckpointBlockTableError::Corrupt(_))
    ));

    let mut damaged = encoded.clone();
    damaged[10..14].copy_from_slice(&0u32.to_le_bytes());
    assert!(matches!(
        decode(&damaged),
        Err(CheckpointBlockTableError::Corrupt(_))
    ));

    let mut damaged = encoded.clone();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    assert!(matches!(
        decode(&damaged),
        Err(CheckpointBlockTableError::Corrupt(_))
    ));

    assert!(decode(&encoded[..encoded.len() - 1]).is_err());
    assert!(decode(&[encoded.as_slice(), b"x"].concat()).is_err());
}
