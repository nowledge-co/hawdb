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

//! Validation and publication of one complete immutable WAL generation.
//!
//! The caller must hold the source branch publication barrier while invoking
//! [`seal_wal_file`].  This module validates a stable prefix and publishes its
//! bytes, but deliberately does not rotate the active writer or switch a head.

use crate::immutable_object::{
    ImmutableObjectError, ImmutableObjectStore, ObjectKind, ObjectReference, PublishOutcome,
};
use crate::wal::{WalCursorEvent, WalOpenOutcome, WalRecordCursor};
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::io::Read;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedWalPublication {
    pub generation: u64,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub object: ObjectReference,
    pub outcome: PublishOutcome,
}

#[derive(Debug)]
pub enum SealedWalError {
    Io {
        operation: &'static str,
        source: std::io::Error,
    },
    TooLarge {
        length: u64,
        limit: u64,
    },
    MissingHeader,
    HeaderTorn {
        reason: String,
    },
    HeaderCorrupt {
        reason: String,
    },
    GenerationMismatch {
        expected: u64,
        actual: u64,
    },
    StartLsnMismatch {
        expected: u64,
        actual: u64,
    },
    Corrupt {
        offset: u64,
        reason: String,
    },
    TornTail {
        valid_prefix_len: u64,
        reason: String,
    },
    LsnGap {
        expected: u64,
        actual: u64,
    },
    LsnOverflow,
    ChangedDuringRead,
    Immutable(ImmutableObjectError),
}

impl Display for SealedWalError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { operation, source } => write!(formatter, "{operation}: {source}"),
            Self::TooLarge { length, limit } => {
                write!(
                    formatter,
                    "sealed WAL length {length} exceeds limit {limit}"
                )
            }
            Self::MissingHeader => formatter.write_str("sealed WAL has no binary header"),
            Self::HeaderTorn { reason } => write!(formatter, "sealed WAL header is torn: {reason}"),
            Self::HeaderCorrupt { reason } => {
                write!(formatter, "sealed WAL header is corrupt: {reason}")
            }
            Self::GenerationMismatch { expected, actual } => {
                write!(
                    formatter,
                    "sealed WAL generation mismatch: expected {expected}, got {actual}"
                )
            }
            Self::StartLsnMismatch { expected, actual } => {
                write!(
                    formatter,
                    "sealed WAL start LSN mismatch: expected {expected}, got {actual}"
                )
            }
            Self::Corrupt { offset, reason } => {
                write!(
                    formatter,
                    "sealed WAL is corrupt at offset {offset}: {reason}"
                )
            }
            Self::TornTail {
                valid_prefix_len,
                reason,
            } => write!(
                formatter,
                "sealed WAL has torn tail at valid prefix {valid_prefix_len}: {reason}"
            ),
            Self::LsnGap { expected, actual } => {
                write!(
                    formatter,
                    "sealed WAL LSN is not contiguous: expected {expected}, got {actual}"
                )
            }
            Self::LsnOverflow => formatter.write_str("sealed WAL LSN overflow"),
            Self::ChangedDuringRead => formatter.write_str("sealed WAL changed while being sealed"),
            Self::Immutable(error) => Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for SealedWalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Immutable(error) => Some(error),
            _ => None,
        }
    }
}

/// Validates and publishes one complete WAL generation as an immutable object.
pub fn seal_wal_file(
    path: &Path,
    expected_generation: u64,
    expected_start_lsn: u64,
    max_bytes: u64,
    store: &mut ImmutableObjectStore,
) -> Result<SealedWalPublication, SealedWalError> {
    let metadata = map_io("read sealed WAL metadata", fs::metadata(path))?;
    if metadata.len() > max_bytes {
        return Err(SealedWalError::TooLarge {
            length: metadata.len(),
            limit: max_bytes,
        });
    }
    let mut cursor =
        match WalRecordCursor::open(path, Some(max_record_limit(max_bytes))).map_err(|error| {
            SealedWalError::Io {
                operation: "open sealed WAL",
                source: std::io::Error::other(error.to_string()),
            }
        })? {
            WalOpenOutcome::Cursor(cursor) => cursor,
            WalOpenOutcome::MissingHeader => return Err(SealedWalError::MissingHeader),
            WalOpenOutcome::HeaderTorn { reason } => {
                return Err(SealedWalError::HeaderTorn { reason })
            }
            WalOpenOutcome::HeaderCorrupt { reason } => {
                return Err(SealedWalError::HeaderCorrupt { reason });
            }
        };
    if cursor.generation() != expected_generation {
        return Err(SealedWalError::GenerationMismatch {
            expected: expected_generation,
            actual: cursor.generation(),
        });
    }
    if cursor.start_lsn() != expected_start_lsn {
        return Err(SealedWalError::StartLsnMismatch {
            expected: expected_start_lsn,
            actual: cursor.start_lsn(),
        });
    }

    let mut expected_lsn = expected_start_lsn;
    loop {
        match cursor.next().map_err(|error| SealedWalError::Io {
            operation: "read sealed WAL record",
            source: std::io::Error::other(error.to_string()),
        })? {
            WalCursorEvent::Entry { entry, .. } => {
                if entry.lsn != expected_lsn {
                    return Err(SealedWalError::LsnGap {
                        expected: expected_lsn,
                        actual: entry.lsn,
                    });
                }
                expected_lsn = expected_lsn
                    .checked_add(1)
                    .ok_or(SealedWalError::LsnOverflow)?;
            }
            WalCursorEvent::Corrupt { offset, reason } => {
                return Err(SealedWalError::Corrupt { offset, reason });
            }
            WalCursorEvent::TornTail {
                valid_prefix_len,
                reason,
            } => {
                return Err(SealedWalError::TornTail {
                    valid_prefix_len,
                    reason,
                })
            }
            WalCursorEvent::Eof => break,
        }
    }

    let bytes = read_stable_bytes(path, metadata.len(), max_bytes)?;
    let reference = ObjectReference::for_bytes(ObjectKind::SealedWal, 1, &bytes);
    let outcome = store
        .publish(reference, &bytes)
        .map_err(SealedWalError::Immutable)?;
    Ok(SealedWalPublication {
        generation: expected_generation,
        start_lsn: expected_start_lsn,
        end_lsn: expected_lsn,
        object: reference,
        outcome,
    })
}

fn max_record_limit(max_bytes: u64) -> usize {
    usize::try_from(max_bytes.min(usize::MAX as u64)).unwrap_or(usize::MAX)
}

fn read_stable_bytes(
    path: &Path,
    expected_len: u64,
    max_bytes: u64,
) -> Result<Vec<u8>, SealedWalError> {
    let mut file = map_io("open sealed WAL bytes", fs::File::open(path))?;
    let capacity = usize::try_from(expected_len).map_err(|_| SealedWalError::TooLarge {
        length: expected_len,
        limit: max_bytes,
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes)
        .map_err(|source| SealedWalError::Io {
            operation: "read sealed WAL bytes",
            source,
        })?;
    let actual_len = bytes.len() as u64;
    if actual_len != expected_len {
        return Err(SealedWalError::ChangedDuringRead);
    }
    if actual_len > max_bytes {
        return Err(SealedWalError::TooLarge {
            length: actual_len,
            limit: max_bytes,
        });
    }
    Ok(bytes)
}

fn map_io<T>(operation: &'static str, result: std::io::Result<T>) -> Result<T, SealedWalError> {
    result.map_err(|source| SealedWalError::Io { operation, source })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{binary::encode_binary_wal_record, frame::*, WalEntry, WalOp};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn root(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("hawdb-sealed-wal-{label}-{nanos}"))
    }

    fn wal_bytes(generation: u64, start_lsn: u64, lsns: &[u64]) -> Vec<u8> {
        let mut bytes = encode_binary_wal_header(generation, start_lsn);
        let mut position = 0;
        for (index, lsn) in lsns.iter().copied().enumerate() {
            let entry = WalEntry {
                lsn,
                op: WalOp::CreateNodeLabel {
                    label: format!("label-{index}"),
                },
            };
            let payload = encode_binary_wal_record(&entry, lsn + 1).unwrap();
            let framed = frame_binary_wal_record(generation, &payload, position);
            position += framed.len() as u64;
            bytes.extend_from_slice(&framed);
        }
        bytes
    }

    #[test]
    fn validates_and_publishes_complete_contiguous_wal() {
        let directory = root("complete");
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("active.wal");
        fs::write(&path, wal_bytes(4, 10, &[10, 11, 12])).unwrap();
        let mut objects = ImmutableObjectStore::open(&directory).unwrap();
        let publication = seal_wal_file(&path, 4, 10, 1 << 20, &mut objects).unwrap();
        assert_eq!((publication.start_lsn, publication.end_lsn), (10, 13));
        assert_eq!(
            fs::read(objects.object_path(publication.object)).unwrap(),
            fs::read(&path).unwrap()
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_generation_lsn_gaps_torn_tail_and_limits() {
        let directory = root("reject");
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("active.wal");
        fs::write(&path, wal_bytes(4, 10, &[10, 12])).unwrap();
        let mut objects = ImmutableObjectStore::open(&directory).unwrap();
        assert!(matches!(
            seal_wal_file(&path, 4, 10, 1 << 20, &mut objects),
            Err(SealedWalError::LsnGap { .. })
        ));
        fs::write(&path, wal_bytes(4, 10, &[10, 11])).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes.pop();
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            seal_wal_file(&path, 4, 10, 1 << 20, &mut objects),
            Err(SealedWalError::TornTail { .. })
        ));
        fs::write(&path, wal_bytes(4, 10, &[10])).unwrap();
        assert!(matches!(
            seal_wal_file(&path, 4, 10, 1, &mut objects),
            Err(SealedWalError::TooLarge { .. })
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_header_identity_mismatch_without_publishing() {
        let directory = root("identity");
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("active.wal");
        fs::write(&path, wal_bytes(4, 10, &[10])).unwrap();
        let mut objects = ImmutableObjectStore::open(&directory).unwrap();
        assert!(matches!(
            seal_wal_file(&path, 5, 10, 1 << 20, &mut objects),
            Err(SealedWalError::GenerationMismatch { .. })
        ));
        assert!(!directory.join("objects/sealed-wal").exists());
        fs::remove_dir_all(directory).unwrap();
    }
}
