// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A conservative protected-prefix fence for generations admitting relaxed writes.
//!
//! Publish the fence durably BEFORE synchronizing a WAL. A completed barrier
//! cannot then be recovered with an older discard boundary. An interrupted
//! barrier may protect more than actually persisted; recovery fails closed.

use crate::error::{HawDBError, Result};
use crate::file_io::{File, SourceDirectory};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"HAWFENC1";
const ENCODED_LEN: usize = 72;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WalRecoveryFence {
    pub generation: u64,
    pub start_lsn: u64,
    pub protected_bytes: u64,
    pub next_lsn: u64,
}

pub(super) fn fence_path(wal: &Path) -> PathBuf {
    wal.with_extension("hawdb.sync")
}

impl WalRecoveryFence {
    pub fn load(wal: &Path, generation: u64, start_lsn: u64) -> Result<Option<Self>> {
        let mut file = match File::open(fence_path(wal)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let mut bytes = [0; ENCODED_LEN];
        if file.metadata()?.len() != ENCODED_LEN as u64 {
            return Err(invalid_fence());
        }
        file.read_exact(&mut bytes)?;
        if &bytes[..8] != MAGIC || hawdb_integrity::sha256(&bytes[..40]).as_bytes() != &bytes[40..]
        {
            return Err(invalid_fence());
        }
        let field =
            |offset| u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fence field"));
        let fence = Self {
            generation: field(8),
            start_lsn: field(16),
            protected_bytes: field(24),
            next_lsn: field(32),
        };
        if fence.generation != generation
            || fence.start_lsn != start_lsn
            || fence.protected_bytes < super::WAL_BINARY_FILE_HEADER_BYTES as u64
            || fence.next_lsn < start_lsn
        {
            return Err(invalid_fence());
        }
        Ok(Some(fence))
    }

    pub fn publish(self, wal: &Path, directory: &SourceDirectory) -> Result<()> {
        let name = fence_path(wal);
        let name = name
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(invalid_fence)?;
        let context = directory.operation_context();
        let candidate_name = format!(".wal-fence-{}.tmp", hawdb_core::generate_uuidv7()?);
        let mut candidate = directory.create_control_candidate(&context, &candidate_name)?;
        let mut bytes = Vec::with_capacity(ENCODED_LEN);
        bytes.extend_from_slice(MAGIC);
        for value in [
            self.generation,
            self.start_lsn,
            self.protected_bytes,
            self.next_lsn,
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        let digest = hawdb_integrity::sha256(&bytes);
        bytes.extend_from_slice(digest.as_bytes());
        candidate.write_all(&bytes)?;
        candidate.seal()?;
        super::process_crash_failpoint("after_wal_fence_candidate_sync");
        candidate.replace_head(&context, name)?;
        super::process_crash_failpoint("after_wal_fence_publish");
        Ok(())
    }

    pub fn validate_retained(self, bytes: u64, next_lsn: u64) -> Result<()> {
        if bytes < self.protected_bytes || next_lsn < self.next_lsn {
            return Err(HawDBError::StorageIntegrity(
                "WAL damage intersects a protected synchronization prefix".into(),
            ));
        }
        Ok(())
    }
}

fn invalid_fence() -> HawDBError {
    HawDBError::StorageIntegrity(
        "invalid or mismatched WAL synchronization fence; retain recovery evidence".into(),
    )
}
