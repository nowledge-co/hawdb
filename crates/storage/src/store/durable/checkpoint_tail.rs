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

//! Bounded, generation-reframed post-snapshot WAL preparation.

use super::{DurableStore, PreparedCheckpoint};
use crate::durability::durable_replace_file;
use crate::error::{HawDBError, Result};
use crate::file_io::{self as fs, File};
use crate::store::{
    encode_binary_wal_header, encode_binary_wal_record, frame_binary_wal_record,
    wal_generation_file, WalCursorEvent, WalRecordCursor, WAL_BINARY_FILE_HEADER_BYTES,
};
use std::io::Write;

/// Receipt for a private candidate; not evidence of checkpoint publication.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointWalTail {
    pub captured_commit_epoch: u64,
    pub captured_next_lsn: u64,
    pub captured_wal_generation: u64,
    pub captured_wal_bytes: u64,
    pub candidate_wal_bytes: u64,
    pub entries: u64,
}

impl DurableStore {
    pub(in crate::store) fn checkpoint_tail_record_limit(&self) -> Option<usize> {
        self.max_record_bytes
    }

    pub(in crate::store) fn prepare_checkpoint_wal_tail(
        &self,
        prepared: &PreparedCheckpoint,
        commit_epoch: u64,
    ) -> Result<CheckpointWalTail> {
        if self.read_only || self.wal_sync_group_active() {
            return Err(HawDBError::Storage(
                "checkpoint suffix capture requires a writable, fully flushed commit boundary"
                    .into(),
            ));
        }
        if self.checkpoint_epoch != prepared.source_checkpoint_epoch
            || self.wal_generation != prepared.source_wal_generation
            || self.wal_bytes < prepared.source_wal_bytes
            || self.next_lsn < prepared.source_next_lsn
            || commit_epoch < prepared.source_commit_epoch
            || self.next_lsn - prepared.source_next_lsn
                != commit_epoch - prepared.source_commit_epoch
            || prepared.generation <= self.wal_generation
        {
            return Err(HawDBError::Storage(
                "checkpoint suffix source identity changed; retain the authoritative WAL".into(),
            ));
        }
        let path = self
            .root_path
            .join(wal_generation_file(prepared.generation));
        let temporary = path.with_extension("hawdb.tail.tmp");
        let result = (|| {
            let header = encode_binary_wal_header(prepared.generation, prepared.source_next_lsn);
            let mut output = File::create(&temporary)?;
            output.write_all(&header)?;
            let mut bytes = header.len() as u64;
            let mut expected_lsn = prepared.source_next_lsn;
            let mut epoch = prepared.source_commit_epoch;
            if expected_lsn < self.next_lsn {
                let mut cursor = WalRecordCursor::open_range(
                    &self.wal_path,
                    self.max_record_bytes,
                    self.wal_generation,
                    self.wal_replay_start_lsn,
                    prepared
                        .source_wal_bytes
                        .max(WAL_BINARY_FILE_HEADER_BYTES as u64),
                    self.wal_bytes,
                )?;
                loop {
                    let entry = match cursor.next()? {
                        WalCursorEvent::Entry { entry, .. } => entry,
                        WalCursorEvent::Eof => break,
                        WalCursorEvent::Corrupt { reason, .. }
                        | WalCursorEvent::TornTail { reason, .. } => {
                            return Err(HawDBError::StorageIntegrity(format!(
                                "checkpoint suffix is not a complete committed interval: {reason}"
                            )));
                        }
                    };
                    if entry.lsn != expected_lsn || expected_lsn >= self.next_lsn {
                        return Err(HawDBError::StorageIntegrity(
                            "checkpoint suffix has a noncontiguous or unexpected LSN".into(),
                        ));
                    }
                    epoch = epoch.checked_add(1).ok_or_else(|| {
                        HawDBError::StorageIntegrity("checkpoint suffix epoch overflow".into())
                    })?;
                    let payload = encode_binary_wal_record(&entry, epoch)?;
                    let framed = frame_binary_wal_record(
                        prepared.generation,
                        &payload,
                        bytes - WAL_BINARY_FILE_HEADER_BYTES as u64,
                    );
                    output.write_all(&framed)?;
                    bytes = bytes.checked_add(framed.len() as u64).ok_or_else(|| {
                        HawDBError::Storage("checkpoint suffix byte count overflow".into())
                    })?;
                    if self.max_wal_bytes.is_some_and(|limit| bytes > limit) {
                        return Err(HawDBError::Storage(
                            "reframed checkpoint suffix exceeds the configured WAL budget".into(),
                        ));
                    }
                    expected_lsn = expected_lsn.checked_add(1).ok_or_else(|| {
                        HawDBError::StorageIntegrity("checkpoint suffix LSN overflow".into())
                    })?;
                }
            }
            if expected_lsn != self.next_lsn || epoch != commit_epoch {
                return Err(HawDBError::StorageIntegrity(
                    "checkpoint suffix does not cover every captured commit".into(),
                ));
            }
            // A completed prefix with unexpected bytes after the final LSN is
            // rejected by the bounded reader/count checks above. All captured
            // suffix bytes become durable before any future selector change.
            output.sync_all()?;
            drop(output);
            durable_replace_file(&temporary, &path)?;
            Ok(CheckpointWalTail {
                captured_commit_epoch: commit_epoch,
                captured_next_lsn: self.next_lsn,
                captured_wal_generation: self.wal_generation,
                captured_wal_bytes: self.wal_bytes,
                candidate_wal_bytes: bytes,
                entries: self.next_lsn - prepared.source_next_lsn,
            })
        })();
        if result.is_err() {
            // This temporary file is owned by the serialized checkpoint job.
            // Leave the already prepared generation and authoritative WAL intact.
            match fs::remove_file(&temporary) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        result
    }
}
