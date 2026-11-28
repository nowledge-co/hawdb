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

//! Ordered block table for block-level COW checkpoint generations.
//!
//! The table is an immutable content-addressed artifact. A later generation
//! manifest binds one table and maps descriptor entries to these blocks.

use crate::checkpoint_block::{
    CheckpointBlockKind, BLOCK_FORMAT_VERSION, MAX_CHECKPOINT_BLOCK_BYTES,
    MIN_CHECKPOINT_BLOCK_BYTES,
};
use crate::immutable_object::{ObjectKind, ObjectReference};
use hawdb_integrity::{IntegrityHasher, Sha256Digest, SHA256_BYTES};
use std::fmt::{self, Display, Formatter};

pub const BLOCK_TABLE_MAGIC: &[u8; 8] = b"SKNBLT01";
pub const BLOCK_TABLE_FORMAT_VERSION: u16 = 1;
pub const BLOCK_TABLE_HEADER_BYTES: usize = 16;
pub const BLOCK_TABLE_ENTRY_BYTES: usize = 100;
pub const BLOCK_TABLE_DIGEST_BYTES: usize = SHA256_BYTES;
pub const MAX_CHECKPOINT_BLOCK_TABLE_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_CHECKPOINT_BLOCK_TABLE_ENTRIES: usize = (MAX_CHECKPOINT_BLOCK_TABLE_BYTES as usize
    - BLOCK_TABLE_HEADER_BYTES
    - BLOCK_TABLE_DIGEST_BYTES)
    / BLOCK_TABLE_ENTRY_BYTES;

const FLAG_SPILL_NAMESPACE: u8 = 1;
const KNOWN_FLAGS: u8 = FLAG_SPILL_NAMESPACE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointBlockEntry {
    pub kind: CheckpointBlockKind,
    /// Inclusive first key of the stable bucket.
    pub bucket_start: u64,
    /// Inclusive last key of the stable bucket. May be `u64::MAX`.
    pub bucket_end: u64,
    pub part: u32,
    pub record_count: u32,
    pub block: ObjectReference,
    pub spill_namespace: Option<Sha256Digest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointBlockTable {
    entries: Vec<CheckpointBlockEntry>,
}

impl CheckpointBlockTable {
    pub fn new(entries: Vec<CheckpointBlockEntry>) -> Result<Self, CheckpointBlockTableError> {
        let table = Self { entries };
        table.validate()?;
        Ok(table)
    }

    pub fn entries(&self) -> &[CheckpointBlockEntry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn validate(&self) -> Result<(), CheckpointBlockTableError> {
        validate_capacity(self.entries.len())?;
        let mut previous: Option<&CheckpointBlockEntry> = None;
        for entry in &self.entries {
            validate_entry(entry)?;
            if let Some(previous) = previous {
                validate_order(previous, entry)?;
            } else if entry.part != 0 {
                return Err(CheckpointBlockTableError::Corrupt(
                    "checkpoint block table first part is not zero",
                ));
            }
            previous = Some(entry);
        }
        Ok(())
    }
}

fn validate_entry(entry: &CheckpointBlockEntry) -> Result<(), CheckpointBlockTableError> {
    if entry.bucket_start > entry.bucket_end {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table bucket range endpoints are inverted",
        ));
    }
    if entry.record_count == 0 {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table entry contains no records",
        ));
    }
    if entry.block.kind != ObjectKind::CheckpointBlock {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table entry does not reference a checkpoint block",
        ));
    }
    if entry.block.format_version != BLOCK_FORMAT_VERSION {
        return Err(CheckpointBlockTableError::UnsupportedVersion {
            found: entry.block.format_version,
            supported: BLOCK_FORMAT_VERSION,
        });
    }
    if !(MIN_CHECKPOINT_BLOCK_BYTES..=MAX_CHECKPOINT_BLOCK_BYTES).contains(&entry.block.byte_length)
    {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table entry block length is outside the block codec bounds",
        ));
    }
    if entry.block.sha256.as_bytes() == &[0; SHA256_BYTES] {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table entry has an empty block digest",
        ));
    }
    if entry
        .spill_namespace
        .is_some_and(|digest| digest.as_bytes() == &[0; SHA256_BYTES])
    {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table entry has an empty spill namespace",
        ));
    }
    Ok(())
}

fn validate_order(
    previous: &CheckpointBlockEntry,
    current: &CheckpointBlockEntry,
) -> Result<(), CheckpointBlockTableError> {
    let previous_kind = previous.kind as u8;
    let current_kind = current.kind as u8;
    if current_kind < previous_kind {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table kinds are not ordered",
        ));
    }
    if current_kind > previous_kind {
        if current.part != 0 {
            return Err(CheckpointBlockTableError::Corrupt(
                "checkpoint block table first part is not zero",
            ));
        }
        return Ok(());
    }
    if current.bucket_start > previous.bucket_end {
        if current.part != 0 {
            return Err(CheckpointBlockTableError::Corrupt(
                "checkpoint block table first part is not zero",
            ));
        }
        return Ok(());
    }
    if current.bucket_start != previous.bucket_start || current.bucket_end != previous.bucket_end {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table buckets overlap or are not ordered",
        ));
    }
    if current.part != previous.part.saturating_add(1) {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table parts are not contiguous",
        ));
    }
    Ok(())
}

fn validate_capacity(entry_count: usize) -> Result<(), CheckpointBlockTableError> {
    if entry_count > MAX_CHECKPOINT_BLOCK_TABLE_ENTRIES {
        return Err(CheckpointBlockTableError::TooManyEntries {
            count: entry_count,
            max: MAX_CHECKPOINT_BLOCK_TABLE_ENTRIES,
        });
    }
    let total = BLOCK_TABLE_HEADER_BYTES
        .checked_add(entry_count.checked_mul(BLOCK_TABLE_ENTRY_BYTES).ok_or(
            CheckpointBlockTableError::TooLarge {
                bytes: u64::MAX,
                max: MAX_CHECKPOINT_BLOCK_TABLE_BYTES,
            },
        )?)
        .and_then(|bytes| bytes.checked_add(BLOCK_TABLE_DIGEST_BYTES))
        .ok_or(CheckpointBlockTableError::TooLarge {
            bytes: u64::MAX,
            max: MAX_CHECKPOINT_BLOCK_TABLE_BYTES,
        })?;
    if total as u64 > MAX_CHECKPOINT_BLOCK_TABLE_BYTES {
        return Err(CheckpointBlockTableError::TooLarge {
            bytes: total as u64,
            max: MAX_CHECKPOINT_BLOCK_TABLE_BYTES,
        });
    }
    Ok(())
}

pub fn encode(table: &CheckpointBlockTable) -> Result<Vec<u8>, CheckpointBlockTableError> {
    table.validate()?;
    let entry_count = u32::try_from(table.entries.len()).map_err(|_| {
        CheckpointBlockTableError::TooManyEntries {
            count: table.entries.len(),
            max: u32::MAX as usize,
        }
    })?;
    let total = BLOCK_TABLE_HEADER_BYTES
        .checked_add(
            table
                .entries
                .len()
                .checked_mul(BLOCK_TABLE_ENTRY_BYTES)
                .ok_or(CheckpointBlockTableError::TooLarge {
                    bytes: u64::MAX,
                    max: MAX_CHECKPOINT_BLOCK_TABLE_BYTES,
                })?,
        )
        .and_then(|bytes| bytes.checked_add(BLOCK_TABLE_DIGEST_BYTES))
        .ok_or(CheckpointBlockTableError::TooLarge {
            bytes: u64::MAX,
            max: MAX_CHECKPOINT_BLOCK_TABLE_BYTES,
        })?;
    if total as u64 > MAX_CHECKPOINT_BLOCK_TABLE_BYTES {
        return Err(CheckpointBlockTableError::TooLarge {
            bytes: total as u64,
            max: MAX_CHECKPOINT_BLOCK_TABLE_BYTES,
        });
    }

    let mut encoded = Vec::with_capacity(total);
    encoded.extend_from_slice(BLOCK_TABLE_MAGIC);
    encoded.extend_from_slice(&BLOCK_TABLE_FORMAT_VERSION.to_le_bytes());
    encoded.extend_from_slice(&entry_count.to_le_bytes());
    encoded.extend_from_slice(&0u16.to_le_bytes());
    for entry in &table.entries {
        encoded.push(entry.kind as u8);
        encoded.push(u8::from(entry.spill_namespace.is_some()) * FLAG_SPILL_NAMESPACE);
        encoded.extend_from_slice(&entry.bucket_start.to_le_bytes());
        encoded.extend_from_slice(&entry.bucket_end.to_le_bytes());
        encoded.extend_from_slice(&entry.part.to_le_bytes());
        encoded.extend_from_slice(&entry.record_count.to_le_bytes());
        encoded.extend_from_slice(&entry.block.format_version.to_le_bytes());
        encoded.extend_from_slice(&entry.block.byte_length.to_le_bytes());
        encoded.extend_from_slice(entry.block.sha256.as_bytes());
        encoded.extend_from_slice(
            &entry
                .spill_namespace
                .map_or([0; SHA256_BYTES], |digest| *digest.as_bytes()),
        );
    }
    debug_assert_eq!(
        encoded.len(),
        BLOCK_TABLE_HEADER_BYTES + table.entries.len() * BLOCK_TABLE_ENTRY_BYTES
    );
    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded);
    encoded.extend_from_slice(hasher.finish().sha256.as_bytes());
    Ok(encoded)
}

pub fn decode(encoded: &[u8]) -> Result<CheckpointBlockTable, CheckpointBlockTableError> {
    if encoded.len() < BLOCK_TABLE_HEADER_BYTES + BLOCK_TABLE_DIGEST_BYTES {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table is shorter than its header and digest",
        ));
    }
    if &encoded[..8] != BLOCK_TABLE_MAGIC {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table magic is invalid",
        ));
    }
    let format_version =
        u16::from_le_bytes(encoded[8..10].try_into().expect("fixed table version"));
    if format_version != BLOCK_TABLE_FORMAT_VERSION {
        return Err(CheckpointBlockTableError::UnsupportedVersion {
            found: format_version,
            supported: BLOCK_TABLE_FORMAT_VERSION,
        });
    }
    let entry_count =
        u32::from_le_bytes(encoded[10..14].try_into().expect("fixed table entry count"));
    let reserved = u16::from_le_bytes(encoded[14..16].try_into().expect("fixed table reserved"));
    if reserved != 0 {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table has reserved header bytes set",
        ));
    }
    let entry_count = usize::try_from(entry_count).map_err(|_| {
        CheckpointBlockTableError::Corrupt("checkpoint block table entry count overflows usize")
    })?;
    if entry_count > MAX_CHECKPOINT_BLOCK_TABLE_ENTRIES {
        return Err(CheckpointBlockTableError::TooManyEntries {
            count: entry_count,
            max: MAX_CHECKPOINT_BLOCK_TABLE_ENTRIES,
        });
    }
    let total = BLOCK_TABLE_HEADER_BYTES
        .checked_add(entry_count.checked_mul(BLOCK_TABLE_ENTRY_BYTES).ok_or(
            CheckpointBlockTableError::Corrupt("checkpoint block table length overflows usize"),
        )?)
        .and_then(|bytes| bytes.checked_add(BLOCK_TABLE_DIGEST_BYTES))
        .ok_or(CheckpointBlockTableError::Corrupt(
            "checkpoint block table length overflows usize",
        ))?;
    if total != encoded.len() {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table length does not match its entry count",
        ));
    }
    if total as u64 > MAX_CHECKPOINT_BLOCK_TABLE_BYTES {
        return Err(CheckpointBlockTableError::TooLarge {
            bytes: total as u64,
            max: MAX_CHECKPOINT_BLOCK_TABLE_BYTES,
        });
    }
    let content_len = total - BLOCK_TABLE_DIGEST_BYTES;
    let mut hasher = IntegrityHasher::new();
    hasher.update(&encoded[..content_len]);
    let expected = hasher.finish().sha256;
    let actual = Sha256Digest::from_bytes(
        encoded[content_len..]
            .try_into()
            .expect("fixed table digest length"),
    );
    if expected != actual {
        return Err(CheckpointBlockTableError::Corrupt(
            "checkpoint block table digest mismatch",
        ));
    }

    let mut entries = Vec::with_capacity(entry_count);
    let mut offset = BLOCK_TABLE_HEADER_BYTES;
    for _ in 0..entry_count {
        let end = offset + BLOCK_TABLE_ENTRY_BYTES;
        let entry = &encoded[offset..end];
        let kind = CheckpointBlockKind::try_from(entry[0]).map_err(|_| {
            CheckpointBlockTableError::Corrupt("checkpoint block table kind is invalid")
        })?;
        let flags = entry[1];
        if flags & !KNOWN_FLAGS != 0 {
            return Err(CheckpointBlockTableError::Corrupt(
                "checkpoint block table entry has reserved flags set",
            ));
        }
        let bucket_start = u64::from_le_bytes(entry[2..10].try_into().expect("fixed bucket start"));
        let bucket_end = u64::from_le_bytes(entry[10..18].try_into().expect("fixed bucket end"));
        let part = u32::from_le_bytes(entry[18..22].try_into().expect("fixed part"));
        let record_count =
            u32::from_le_bytes(entry[22..26].try_into().expect("fixed record count"));
        let block_format_version =
            u16::from_le_bytes(entry[26..28].try_into().expect("fixed block version"));
        let byte_length = u64::from_le_bytes(entry[28..36].try_into().expect("fixed block length"));
        let digest =
            Sha256Digest::from_bytes(entry[36..68].try_into().expect("fixed block digest"));
        let spill_namespace = if flags & FLAG_SPILL_NAMESPACE != 0 {
            let digest =
                Sha256Digest::from_bytes(entry[68..100].try_into().expect("fixed spill namespace"));
            Some(digest)
        } else {
            if entry[68..100].iter().any(|byte| *byte != 0) {
                return Err(CheckpointBlockTableError::Corrupt(
                    "checkpoint block table has a spill namespace without its flag",
                ));
            }
            None
        };
        entries.push(CheckpointBlockEntry {
            kind,
            bucket_start,
            bucket_end,
            part,
            record_count,
            block: ObjectReference {
                kind: ObjectKind::CheckpointBlock,
                format_version: block_format_version,
                byte_length,
                sha256: digest,
            },
            spill_namespace,
        });
        offset = end;
    }
    CheckpointBlockTable::new(entries)
}

impl CheckpointBlockTable {
    pub fn object_reference(&self) -> Result<ObjectReference, CheckpointBlockTableError> {
        let encoded = encode(self)?;
        Ok(ObjectReference::for_bytes(
            ObjectKind::CheckpointBlockTable,
            BLOCK_TABLE_FORMAT_VERSION,
            &encoded,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointBlockTableError {
    UnsupportedVersion { found: u16, supported: u16 },
    Corrupt(&'static str),
    TooManyEntries { count: usize, max: usize },
    TooLarge { bytes: u64, max: u64 },
}

impl Display for CheckpointBlockTableError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { found, supported } => write!(
                formatter,
                "checkpoint block table format version {found} is unsupported; supported version is {supported}"
            ),
            Self::Corrupt(message) => formatter.write_str(message),
            Self::TooManyEntries { count, max } => write!(
                formatter,
                "checkpoint block table has {count} entries, exceeding the {max}-entry limit"
            ),
            Self::TooLarge { bytes, max } => write!(
                formatter,
                "checkpoint block table uses {bytes} bytes, exceeding its {max}-byte limit"
            ),
        }
    }
}

impl std::error::Error for CheckpointBlockTableError {}

#[cfg(test)]
mod tests;
