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

//! WAL admission, append rollback, group durability and generation preparation.

use super::{DurableStore, WalFreeSpaceProbeState, WAL_FREE_SPACE_PROBE_INTERVAL_BYTES};
use crate::error::{HawDBError, Result};
use crate::file_io::{self as fs, File, OpenOptions};
use crate::store::{
    elapsed_micros, encode_binary_wal_header, encode_binary_wal_record, frame_binary_wal_record,
    process_crash_failpoint, sync_parent_dir, wal_generation_file, wal_group_sync_failpoint,
    WalEntry, WalOp, CHECKPOINT_TEMPORARY_SPACE_MULTIPLIER, MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES,
    WAL_BINARY_FILE_HEADER_BYTES,
};
use hawdb_storage::{
    config::DurabilityPolicy,
    durability::{
        durable_replace_file, WalSyncGroupFlush, WalSyncGroupProgress, WalSyncGroupState,
    },
    pressure::{
        available_storage_space, StorageDebtController, StoragePressureSignals,
        StoragePressureSnapshot,
    },
    telemetry::WalAppendTelemetry,
};
use std::io::Write;
use std::sync::Arc;

/// Keep the identity cache with the exact WAL descriptor it describes. Rotation
/// replaces both together, and cloned checkpoint sources retain the same pair.
/// Ordinary files and WAL read cursors do not pay for an append-only cache.
#[derive(Debug)]
pub(in crate::store) struct WalAppendFile {
    file: File,
    #[cfg(unix)]
    identity: std::sync::OnceLock<crate::native_file_identity::NativeIdentity>,
}

impl WalAppendFile {
    pub(in crate::store) fn new(file: File) -> Self {
        Self {
            file,
            #[cfg(unix)]
            identity: std::sync::OnceLock::new(),
        }
    }

    pub(in crate::store) fn file(&self) -> &File {
        &self.file
    }

    /// Follow symlinks to compare the target object's identity with the held file.
    #[cfg(unix)]
    fn matches_native_path_in(
        &self,
        path: &std::path::Path,
        context: &crate::file_descriptors::FileOpenContext,
    ) -> std::io::Result<bool> {
        let identity = self.file.with_native(|native| {
            if let Some(identity) = self.identity.get() {
                return Ok(*identity);
            }
            let identity = crate::native_file_identity::of_file(native)?;
            // Concurrent first checks see the same pinned descriptor. Failed
            // observations are never cached; recorder I/O guards still run.
            let _ = self.identity.set(identity);
            Ok(identity)
        })?;
        // Only the held identity is stable. Always admit and observe the named
        // file again through the original operation's descriptor domain.
        Ok(identity == crate::native_file_identity::of_metadata(&context.metadata(path)?))
    }
}

fn wal_admission_refusal(
    mut signals: StoragePressureSignals,
    generation_bytes: impl FnOnce() -> u64,
) -> Option<StoragePressureSnapshot> {
    // More retained debt cannot make admission less restrictive. If even its
    // upper bound permits a write, inventory I/O cannot change that decision.
    // Refusals and public pressure reports retain the actual measured debt.
    signals.obsolete_generation_bytes = u64::MAX;
    if StorageDebtController
        .evaluate(signals)
        .state
        .admits_mutation()
    {
        return None;
    }
    signals.obsolete_generation_bytes = generation_bytes();
    let pressure = StorageDebtController.evaluate(signals);
    (!pressure.state.admits_mutation()).then_some(pressure)
}

impl DurableStore {
    /// The cache is scoped to the exact generation path, including rotations
    /// and replay of another branch's private WAL.
    fn load_wal_recovery_fence(
        &mut self,
    ) -> Result<Option<super::super::wal_fence::WalRecoveryFence>> {
        if let Some((path, fence)) = &self.wal_recovery_fence
            && path == &self.wal_path
        {
            return Ok(*fence);
        }
        // Fence discovery belongs to the same internal WAL admission scope.
        // A cold lookup must not borrow an exhausted foreground FD budget.
        let _operation = self.file_descriptor_context().internal_operation()?;
        let fence = super::super::wal_fence::WalRecoveryFence::load(
            &self.wal_path,
            self.wal_generation,
            self.wal_replay_start_lsn,
        )?;
        self.wal_recovery_fence = Some((self.wal_path.clone(), fence));
        Ok(fence)
    }

    fn publish_wal_recovery_fence(&mut self, bytes: u64, next_lsn: u64) -> Result<()> {
        let fence = super::super::wal_fence::WalRecoveryFence {
            generation: self.wal_generation,
            start_lsn: self.wal_replay_start_lsn,
            protected_bytes: bytes.max(WAL_BINARY_FILE_HEADER_BYTES as u64),
            next_lsn,
        };
        if let Some(previous) = super::super::wal_fence::WalRecoveryFence::load(
            &self.wal_path,
            self.wal_generation,
            self.wal_replay_start_lsn,
        )? {
            previous.validate_retained(fence.protected_bytes, fence.next_lsn)?;
        }
        let captured;
        let directory = match &self.wal_append_owner {
            Some(owner) => owner.captured_directory(),
            None => {
                captured = crate::file_io::SourceDirectory::capture_for_path(
                    self.wal_path
                        .parent()
                        .ok_or_else(|| HawDBError::StorageIntegrity("WAL has no parent".into()))?,
                )?;
                &captured
            }
        };
        fence.publish(&self.wal_path, directory)?;
        self.wal_recovery_fence = Some((self.wal_path.clone(), Some(fence)));
        Ok(())
    }

    fn begin_relaxed_wal_interval(&mut self) -> Result<()> {
        if self.durability == DurabilityPolicy::SyncOnCheckpoint
            && self.load_wal_recovery_fence()?.is_none()
        {
            // Never retroactively classify an existing prefix as disposable.
            self.publish_wal_recovery_fence(self.wal_bytes, self.next_lsn)?;
        }
        Ok(())
    }

    fn protect_wal_sync(&mut self, bytes: u64, next_lsn: u64) -> Result<()> {
        if let Some(fence) = self.load_wal_recovery_fence()? {
            if bytes < fence.protected_bytes || next_lsn < fence.next_lsn {
                return Err(HawDBError::StorageIntegrity(
                    "WAL synchronization fence moved backwards".into(),
                ));
            }
            // A visible fence can be an interrupted rename whose directory
            // barrier was never acknowledged. Re-publish even equal bounds.
            self.publish_wal_recovery_fence(bytes, next_lsn)?;
        }
        Ok(())
    }

    pub(in crate::store) fn begin_wal_sync_group(&mut self) -> Result<bool> {
        if self.durability != DurabilityPolicy::SyncOnEveryWrite {
            return Ok(false);
        }
        if self.wal_sync_group.is_some() {
            return Err(HawDBError::Storage(
                "nested WAL sync groups are not allowed".to_string(),
            ));
        }
        self.wal_sync_group = Some(WalSyncGroupState::default());
        Ok(true)
    }

    pub(in crate::store) fn wal_sync_group_progress(&self) -> WalSyncGroupProgress {
        self.wal_sync_group
            .map_or_else(WalSyncGroupProgress::default, WalSyncGroupState::progress)
    }

    pub(in crate::store) const fn wal_sync_group_active(&self) -> bool {
        self.wal_sync_group.is_some()
    }

    pub(in crate::store) fn finish_wal_sync_group(&mut self) -> Result<WalSyncGroupFlush> {
        let Some(group) = self.wal_sync_group.take() else {
            return Ok(WalSyncGroupFlush::default());
        };
        if group.is_empty() {
            return Ok(WalSyncGroupFlush::default());
        }
        wal_group_sync_failpoint()?;
        let started = std::time::Instant::now();
        self.protect_wal_sync(self.wal_bytes, self.next_lsn)?;
        let file = self.wal_append_file.as_ref().ok_or_else(|| {
            HawDBError::Storage(
                "WAL sync group has entries without an open append handle".to_string(),
            )
        })?;
        self.require_captured_wal_append(file)?;
        file.file().sync_data()?;
        if group.requires_parent_sync() || !self.wal_namespace_known_durable {
            #[cfg(test)]
            crate::store::read_dependencies::source_sync_failpoint(
                crate::store::read_dependencies::SourceSyncFailure::Directory,
            )?;
            self.sync_wal_namespace()?;
            self.wal_namespace_known_durable = true;
        }
        self.require_captured_wal_append(file)?;
        self.known_durable_commit_epoch = self.wal_commit_epoch;
        process_crash_failpoint("after_wal_sync");
        Ok(group.into_flush(elapsed_micros(started)))
    }

    pub(in crate::store) fn wal_age_millis(&self) -> Option<u64> {
        let elapsed = self.wal_uncheckpointed_since?.elapsed();
        Some(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
    }

    pub(in crate::store) fn append_single(
        &mut self,
        op: WalOp,
        pressure_signals: StoragePressureSignals,
    ) -> Result<()> {
        self.append_entry(op, 1, pressure_signals)
    }

    pub(in crate::store) fn append_batch(
        &mut self,
        ops: Vec<WalOp>,
        pressure_signals: StoragePressureSignals,
    ) -> Result<()> {
        let operation_count = ops.len();
        if self
            .max_batch_operations
            .is_some_and(|limit| operation_count > limit)
        {
            return Err(HawDBError::Storage(format!(
                "WAL batch operation limit exceeded before append: max_wal_batch_operations={}",
                self.max_batch_operations.unwrap_or_default()
            )));
        }
        self.append_entry(WalOp::Batch(ops), operation_count, pressure_signals)
    }

    pub(in crate::store) fn append_prepared_batch(
        &mut self,
        prepared: crate::wal::binary::PreparedWalBatch,
        pressure_signals: StoragePressureSignals,
    ) -> Result<()> {
        let operation_count = prepared.operation_count();
        if self
            .max_batch_operations
            .is_some_and(|limit| operation_count > limit)
        {
            return Err(HawDBError::Storage(format!(
                "WAL batch operation limit exceeded before append: max_wal_batch_operations={}",
                self.max_batch_operations.unwrap_or_default()
            )));
        }
        let payload = prepared.bind(self.next_lsn, self.wal_commit_epoch.saturating_add(1));
        self.append_encoded_entry(payload, operation_count, pressure_signals)
    }

    fn append_entry(
        &mut self,
        op: WalOp,
        operation_count: usize,
        pressure_signals: StoragePressureSignals,
    ) -> Result<()> {
        let entry = WalEntry {
            lsn: self.next_lsn,
            op,
        };
        let payload = encode_binary_wal_record(&entry, self.wal_commit_epoch.saturating_add(1))?;
        self.append_encoded_entry(payload, operation_count, pressure_signals)
    }

    fn append_encoded_entry(
        &mut self,
        payload: Vec<u8>,
        operation_count: usize,
        pressure_signals: StoragePressureSignals,
    ) -> Result<()> {
        if self
            .max_record_bytes
            .is_some_and(|limit| payload.len() > limit)
        {
            return Err(HawDBError::Storage(format!(
                "WAL record byte limit exceeded before append: max_wal_record_bytes={}",
                self.max_record_bytes.unwrap_or_default()
            )));
        }
        let header_bytes = encode_binary_wal_header(self.wal_generation, self.wal_replay_start_lsn);
        let position = self
            .wal_bytes
            .saturating_sub(WAL_BINARY_FILE_HEADER_BYTES as u64);
        let record_bytes = frame_binary_wal_record(self.wal_generation, &payload, position);
        let started = std::time::Instant::now();
        let mut byte_count = record_bytes.len() as u64;
        if self.wal_bytes == 0 {
            byte_count = byte_count.saturating_add(header_bytes.len() as u64);
        }
        self.ensure_wal_admission(
            self.wal_bytes.saturating_add(byte_count),
            byte_count,
            pressure_signals,
        )?;
        // Native ownership checks and namespace barriers may open temporary
        // handles. Fund those before any record bytes can reach the WAL.
        let _descriptors = self.reserve_wal_operation_resources()?;
        self.begin_relaxed_wal_interval()?;
        process_crash_failpoint("before_wal_append");
        let sync_deferred = self.wal_sync_group.is_some();
        let result = match self.take_wal_append() {
            Err(error) => Err(error),
            Ok((file, created)) => {
                #[cfg(test)]
                if self.wal_append_owner.is_some() {
                    super::AFTER_CAPTURED_WAL_APPEND_OPEN.with(|hook| {
                        if let Some(hook) = hook.borrow_mut().take() {
                            hook();
                        }
                    });
                }
                let write_result: Result<()> = (|| {
                    let mut writer = file.file();
                    if self.wal_bytes == 0 {
                        writer.write_all(&header_bytes)?;
                    }
                    {
                        if std::env::var(crate::store::PROCESS_CRASH_POINT_ENV).as_deref()
                            == Ok("during_wal_append")
                        {
                            let cut = std::env::var("HAWDB_TEST_WAL_APPEND_CUT")
                                .ok()
                                .and_then(|value| value.parse::<usize>().ok())
                                .unwrap_or(record_bytes.len() / 2)
                                .min(record_bytes.len());
                            writer.write_all(&record_bytes[..cut])?;
                            process_crash_failpoint("during_wal_append");
                        }
                        #[cfg(any(test, feature = "test-support"))]
                        {
                            if matches!(
                                crate::store::WAL_APPEND_FAILURE.get(),
                                Some(
                                    crate::store::WalAppendFailure::PartialWrite
                                        | crate::store::WalAppendFailure::Rollback
                                )
                            ) {
                                writer.write_all(&record_bytes[..record_bytes.len() / 2])?;
                                if crate::store::WAL_APPEND_FAILURE.get()
                                    == Some(crate::store::WalAppendFailure::PartialWrite)
                                {
                                    crate::store::WAL_APPEND_FAILURE.take();
                                }
                                return Err(
                                    std::io::Error::from(std::io::ErrorKind::StorageFull).into()
                                );
                            }
                        }
                    }
                    writer.write_all(&record_bytes)?;
                    Ok(())
                })();
                let append_result = match write_result {
                    Err(error) => match self.rollback_failed_wal_write(file.file()) {
                        Ok(()) => {
                            // A group may still need this handle to acknowledge
                            // earlier entries even when no later write retries.
                            self.wal_append_file = Some(Arc::clone(&file));
                            Err(HawDBError::Storage(format!(
                                "WAL write failed and was rolled back to byte {}: {error}",
                                self.wal_bytes
                            )))
                        }
                        Err(rollback_error) => Err(HawDBError::StorageIntegrity(format!(
                            "WAL write failed: {error}; rollback to byte {} failed: {rollback_error}; close and recover the database",
                            self.wal_bytes
                        ))),
                    },
                    Ok(()) => {
                        process_crash_failpoint("after_wal_append");
                        let sync_result = self.finish_wal_append(file.as_ref(), created);
                        if sync_result.is_ok() && !sync_deferred {
                            process_crash_failpoint("after_wal_sync");
                        }
                        sync_result.map_err(|error| HawDBError::StorageIntegrity(format!(
                            "WAL append outcome is uncertain after writing the complete record: {error}"
                        )))
                    }
                };
                match append_result {
                    Ok(fsync_micros) => {
                        self.wal_append_file = Some(file);
                        Ok(fsync_micros)
                    }
                    Err(error) => {
                        // A poisoned store still owns the actual uncertain file.
                        // Keep its native evidence until the store closes; a
                        // directory lease cannot substitute for this object.
                        if self.wal_append_owner.is_some() {
                            self.wal_append_file = Some(file);
                        }
                        Err(error)
                    }
                }
            }
        };
        if let Some(telemetry) = &self.telemetry {
            telemetry.record_wal_append(WalAppendTelemetry {
                success: result.is_ok(),
                elapsed_micros: elapsed_micros(started),
                operation_count,
                byte_count,
                fsync_micros: result.as_ref().copied().unwrap_or_default(),
                generation: self.wal_generation,
            });
        }
        if result.is_ok() {
            self.wal_uncheckpointed_since
                .get_or_insert_with(std::time::Instant::now);
            self.next_lsn += 1;
            self.wal_commit_epoch = self.wal_commit_epoch.saturating_add(1);
            if self.durability == DurabilityPolicy::SyncOnEveryWrite && !sync_deferred {
                self.known_durable_commit_epoch = self.wal_commit_epoch;
            }
            self.wal_bytes = self.wal_bytes.saturating_add(byte_count);
            self.wal_free_space_probe.wal_bytes_since_probe = self
                .wal_free_space_probe
                .wal_bytes_since_probe
                .saturating_add(byte_count);
            if let Some(group) = &mut self.wal_sync_group {
                group.record_entry(byte_count);
            }
        }
        result.map(|_| ())
    }

    /// Synchronizes the complete currently applied WAL prefix without changing
    /// the policy for subsequent commits. The checkpoint closure is already
    /// durable; canonical WAL records contain the complete transaction payload.
    pub(in crate::store) fn synchronize_source_prefix(&mut self) -> Result<u64> {
        if self.wal_sync_group.is_some() {
            return Err(HawDBError::Execution(
                "cannot synchronize a projection source inside a WAL sync group".into(),
            ));
        }
        if self.known_durable_commit_epoch >= self.wal_commit_epoch {
            return Ok(self.known_durable_commit_epoch);
        }
        if self.read_only {
            return Err(HawDBError::Execution(
                "cannot synchronize a read-only source".into(),
            ));
        }
        let _descriptors = self.reserve_wal_operation_resources()?;
        // Recovered databases need not have an append handle yet. Never create
        // an empty WAL to replace missing recovery evidence.
        let file = if let Some(file) = &self.wal_append_file {
            Arc::clone(file)
        } else {
            if self.wal_append_owner.is_some() {
                return Err(HawDBError::StorageIntegrity(
                    "captured WAL append handle was lost".into(),
                ));
            }
            Arc::new(WalAppendFile::new(
                OpenOptions::new()
                    .descriptor_kind(crate::file_descriptors::DescriptorKind::MutableWal)
                    .append(true)
                    .open(&self.wal_path)?,
            ))
        };
        self.require_captured_wal_append(&file)?;
        if file.file().metadata()?.len() != self.wal_bytes
            || (self.wal_append_owner.is_none()
                && fs::metadata(&self.wal_path)?.len() != self.wal_bytes)
        {
            return Err(HawDBError::StorageIntegrity(
                "projection source WAL length changed outside the committed boundary".into(),
            ));
        }
        self.protect_wal_sync(self.wal_bytes, self.next_lsn)?;
        let synchronization: Result<()> = (|| {
            #[cfg(test)]
            crate::store::read_dependencies::source_sync_failpoint(
                crate::store::read_dependencies::SourceSyncFailure::Wal,
            )?;
            file.file().sync_data()?;
            // Relaxed creation can leave the WAL name unsynchronized even
            // though the complete transaction was flushed to the OS.
            if !self.wal_namespace_known_durable {
                #[cfg(test)]
                crate::store::read_dependencies::source_sync_failpoint(
                    crate::store::read_dependencies::SourceSyncFailure::Directory,
                )?;
                self.sync_wal_namespace()?;
                self.wal_namespace_known_durable = true;
            }
            self.require_captured_wal_append(&file)
        })();
        synchronization.map_err(|error| {
            HawDBError::StorageIntegrity(format!(
                "projection source synchronization outcome is uncertain: {error}"
            ))
        })?;
        self.known_durable_commit_epoch = self.wal_commit_epoch;
        Ok(self.known_durable_commit_epoch)
    }

    pub(in crate::store) const fn known_durable_commit_epoch(&self) -> u64 {
        self.known_durable_commit_epoch
    }

    fn rollback_failed_wal_write(&mut self, file: &File) -> Result<()> {
        #[cfg(test)]
        BEFORE_WAL_ROLLBACK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        #[cfg(any(test, feature = "test-support"))]
        if crate::store::WAL_APPEND_FAILURE.take() == Some(crate::store::WalAppendFailure::Rollback)
        {
            return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into());
        }
        self.protect_wal_sync(
            self.wal_bytes.max(WAL_BINARY_FILE_HEADER_BYTES as u64),
            self.next_lsn,
        )?;
        let empty_header = encode_binary_wal_header(self.wal_generation, self.wal_replay_start_lsn);
        self.wal_bytes = file.rollback_wal_append(self.wal_bytes, &empty_header)?;
        // Keep even a newly created WAL's valid empty header. Removing its saved pathname
        // could unlink a replacement. A retry reuses this same append handle;
        // its eventual commit still synchronizes the unsynchronized namespace.
        // This observation refuses reuse after replacement; it does not grant
        // authority for any later pathname mutation.
        if !file.matches_native_path(&self.wal_path)? {
            return Err(HawDBError::StorageIntegrity(
                "WAL name changed during append rollback".into(),
            ));
        }
        self.wal_free_space_probe = WalFreeSpaceProbeState::default();
        Ok(())
    }

    fn ensure_wal_admission(
        &mut self,
        projected_wal_bytes: u64,
        pending_wal_bytes: u64,
        mut signals: StoragePressureSignals,
    ) -> Result<()> {
        let available_free_space_bytes = self
            .available_space_for_wal_admission()?
            .saturating_sub(pending_wal_bytes);
        let reclamation = self.generation_reclamation_debt();
        signals.wal_bytes = projected_wal_bytes;
        signals.max_wal_bytes = self.max_wal_bytes;
        signals.generation_reclamation_retry_required = reclamation.retry_required;
        signals.generation_reclamation_pending_files = reclamation.pending_file_count;
        signals.generation_reclamation_pending_bytes = reclamation.pending_bytes;
        signals.oldest_reader_commit_epoch = self.oldest_reader_commit_epoch;
        signals.estimated_checkpoint_temporary_bytes = signals
            .estimated_checkpoint_temporary_bytes
            .saturating_add(pending_wal_bytes.saturating_mul(CHECKPOINT_TEMPORARY_SPACE_MULTIPLIER))
            .max(MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES);
        signals.available_free_space_bytes = Some(available_free_space_bytes);
        let Some(pressure) = wal_admission_refusal(signals, || {
            self.obsolete_generation_bytes(self.oldest_reader_commit_epoch)
        }) else {
            return Ok(());
        };
        let reasons = pressure
            .reason_codes
            .iter()
            .map(|reason| reason.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let recovery = if pressure
            .reason_codes
            .contains(&hawdb_storage::pressure::StoragePressureReasonCode::IntegrityPoisoned)
        {
            "close and reopen the database before retrying"
        } else if pressure
            .reason_codes
            .contains(&hawdb_storage::pressure::StoragePressureReasonCode::FreeSpaceReserve)
        {
            "free storage space before retrying"
        } else {
            "checkpoint the database before retrying"
        };
        Err(HawDBError::Storage(format!(
            "WAL append rejected by storage pressure: state={}, projected_wal_bytes={projected_wal_bytes}, max_wal_bytes={}, available_free_space_bytes={}, estimated_checkpoint_temporary_bytes={}, reasons={reasons}; {recovery}",
            pressure.state.as_str(),
            self.max_wal_bytes.unwrap_or_default(),
            pressure.available_free_space_bytes.unwrap_or_default(),
            pressure.estimated_checkpoint_temporary_bytes,
        )))
    }

    fn available_space_for_wal_admission(&mut self) -> Result<u64> {
        let probe_required = self.wal_free_space_probe.last_available_bytes.is_none()
            || self.wal_free_space_probe.wal_bytes_since_probe
                >= WAL_FREE_SPACE_PROBE_INTERVAL_BYTES;
        if probe_required {
            #[cfg(test)]
            let available = self
                .wal_free_space_probe
                .available_bytes_override
                .or_else(|| available_storage_space(&self.root_path));
            #[cfg(not(test))]
            let available = available_storage_space(&self.root_path);
            let available = available.ok_or_else(|| {
                HawDBError::Storage(
                    "WAL append rejected because filesystem free space could not be inspected"
                        .to_string(),
                )
            })?;
            self.wal_free_space_probe.last_available_bytes = Some(available);
            self.wal_free_space_probe.wal_bytes_since_probe = 0;
            #[cfg(test)]
            {
                self.wal_free_space_probe.probe_count =
                    self.wal_free_space_probe.probe_count.saturating_add(1);
            }
        }
        Ok(self
            .wal_free_space_probe
            .last_available_bytes
            .unwrap_or_default()
            .saturating_sub(self.wal_free_space_probe.wal_bytes_since_probe))
    }

    #[cfg(test)]
    pub(in crate::store) fn set_wal_available_space_override(&mut self, available_bytes: u64) {
        self.wal_free_space_probe.available_bytes_override = Some(available_bytes);
        self.wal_free_space_probe.last_available_bytes = None;
        self.wal_free_space_probe.wal_bytes_since_probe = 0;
    }

    #[cfg(test)]
    pub(in crate::store) fn wal_free_space_probe_count(&self) -> u64 {
        self.wal_free_space_probe.probe_count
    }

    fn reserve_wal_operation_resources(
        &mut self,
    ) -> Result<Option<crate::file_descriptors::DescriptorReservation>> {
        let has_fence = self.load_wal_recovery_fence()?.is_some();
        let needed = if self.wal_append_owner.is_some() {
            // Includes Windows directory/identity reopen and rollback checks.
            4
        } else {
            usize::from(self.wal_append_file.is_none())
                + usize::from(!self.wal_namespace_known_durable)
                + if self.durability == DurabilityPolicy::SyncOnCheckpoint || has_fence {
                    3
                } else {
                    0
                }
        };
        (needed != 0)
            .then(|| self.project_files.reserve_admission(needed))
            .transpose()
            .map_err(Into::into)
    }

    fn require_captured_wal_append(&self, file: &WalAppendFile) -> Result<()> {
        if let Some(owner) = &self.wal_append_owner {
            let parent = self.wal_path.parent().ok_or_else(|| {
                HawDBError::StorageIntegrity("captured WAL has no directory".into())
            })?;
            let matches = owner.owns_directory(parent)? && {
                #[cfg(unix)]
                {
                    file.matches_native_path_in(
                        &self.wal_path,
                        &owner.captured_directory().operation_context(),
                    )?
                }
                #[cfg(not(unix))]
                file.file().matches_native_path(&self.wal_path)?
            };
            if !matches {
                return Err(HawDBError::StorageIntegrity(
                    "captured active WAL writer was replaced".into(),
                ));
            }
        }
        Ok(())
    }

    fn sync_wal_namespace(&self) -> Result<()> {
        if let Some(owner) = &self.wal_append_owner {
            let directory = owner.captured_directory();
            directory
                .sync(&directory.operation_context())
                .map_err(Into::into)
        } else {
            sync_parent_dir(&self.wal_path)
        }
    }

    fn take_wal_append(&mut self) -> Result<(Arc<WalAppendFile>, bool)> {
        if let Some(file) = &self.wal_append_file {
            self.require_captured_wal_append(file)?;
        } else if self.wal_append_owner.is_some() {
            return Err(HawDBError::StorageIntegrity(
                "captured WAL append handle was lost".into(),
            ));
        }
        if let Some(file) = self.wal_append_file.take() {
            return Ok((file, false));
        }
        let open_existing = || {
            OpenOptions::new()
                .descriptor_kind(crate::file_descriptors::DescriptorKind::MutableWal)
                .append(true)
                .open(&self.wal_path)
        };
        let (file, created) = if self.wal_bytes == 0 {
            match OpenOptions::new()
                .descriptor_kind(crate::file_descriptors::DescriptorKind::MutableWal)
                .append(true)
                .create_new(true)
                .open(&self.wal_path)
            {
                Ok(file) => (file, true),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    (open_existing()?, false)
                }
                Err(error) => return Err(error.into()),
            }
        } else {
            (open_existing()?, false)
        };
        #[cfg(test)]
        {
            self.wal_append_open_count = self.wal_append_open_count.saturating_add(1);
        }
        Ok((Arc::new(WalAppendFile::new(file)), created))
    }

    fn finish_wal_append(&mut self, file: &WalAppendFile, created: bool) -> Result<u64> {
        #[cfg(any(test, feature = "test-support"))]
        if crate::store::WAL_APPEND_FAILURE.take() == Some(crate::store::WalAppendFailure::Sync) {
            return Err(std::io::Error::other("injected WAL sync failure").into());
        }
        let mut writer = file.file();
        writer.flush()?;
        self.require_captured_wal_append(file)?;
        if let Some(group) = &mut self.wal_sync_group {
            if created {
                group.record_wal_created();
            }
            return Ok(0);
        }
        let mut fsync_micros = 0;
        if self.durability == DurabilityPolicy::SyncOnEveryWrite {
            let started = std::time::Instant::now();
            self.protect_wal_sync(file.file().metadata()?.len(), self.next_lsn + 1)?;
            file.file().sync_data()?;
            if created || !self.wal_namespace_known_durable {
                #[cfg(test)]
                crate::store::read_dependencies::source_sync_failpoint(
                    crate::store::read_dependencies::SourceSyncFailure::Directory,
                )?;
                self.sync_wal_namespace()?;
                self.wal_namespace_known_durable = true;
            }
            fsync_micros = elapsed_micros(started);
        }
        self.require_captured_wal_append(file)?;
        Ok(fsync_micros)
    }

    pub(in crate::store) fn prepare_wal_generation(&self, generation: u64) -> Result<()> {
        // New WAL generations always use the binary format; an existing
        // text database therefore upgrades at its next checkpoint.
        let wal_path = self.root_path.join(wal_generation_file(generation));
        let tmp_path = wal_path.with_extension("hawdb.tmp");
        let header = encode_binary_wal_header(generation, self.next_lsn);
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&header)?;
            file.sync_all()?;
        }
        durable_replace_file(&tmp_path, &wal_path)?;
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    static BEFORE_WAL_ROLLBACK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const {
        std::cell::RefCell::new(None)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{set_wal_append_failpoint, GraphStore, WalAppendFailure};
    use hawdb_core::{schema::Catalog, Value};
    use std::collections::BTreeMap;

    #[test]
    fn admitted_wal_pressure_does_not_observe_generation_inventory() {
        let signals = StoragePressureSignals {
            current_commit_epoch: 10,
            checkpoint_commit_epoch: 2,
            oldest_reader_commit_epoch: Some(1),
            ..Default::default()
        };
        assert!(wal_admission_refusal(signals, || {
            panic!("admitted WAL must not enumerate generation files")
        })
        .is_none());
    }

    #[test]
    fn lazy_wal_pressure_matches_complete_refusals_and_diagnostics() {
        for mask in 0..128 {
            for oldest in [None, Some(0), Some(10)] {
                for obsolete in [0, 1, 1 << 40, u64::MAX] {
                    let signals = StoragePressureSignals {
                        current_commit_epoch: 10,
                        checkpoint_commit_epoch: 2,
                        oldest_reader_commit_epoch: oldest,
                        obsolete_generation_bytes: obsolete,
                        wal_bytes: if mask & 1 != 0 {
                            100
                        } else if mask & 2 != 0 {
                            95
                        } else {
                            10
                        },
                        max_wal_bytes: Some(100),
                        delta_bytes: if mask & 4 != 0 { 95 } else { 10 },
                        max_delta_bytes: Some(100),
                        integrity_poisoned: mask & 8 != 0,
                        estimated_checkpoint_temporary_bytes: 128,
                        available_free_space_bytes: Some(if mask & 16 != 0 { 64 } else { 256 }),
                        generation_reclamation_retry_required: mask & 32 != 0,
                        cache_capacity_bytes: 100,
                        cache_resident_bytes: 100,
                        cache_pinned_bytes: if mask & 64 != 0 { 95 } else { 0 },
                        ..Default::default()
                    };
                    let expected = StorageDebtController.evaluate(signals);
                    let expected = (!expected.state.admits_mutation()).then_some(expected);
                    let observed = std::cell::Cell::new(false);
                    let actual = wal_admission_refusal(signals, || {
                        observed.set(true);
                        obsolete
                    });
                    assert_eq!(
                        actual, expected,
                        "mask={mask}, oldest={oldest:?}, obsolete={obsolete}"
                    );
                    if let Some(refusal) = actual {
                        assert!(observed.get());
                        assert_eq!(refusal.obsolete_generation_bytes, obsolete);
                    }
                }
            }
        }
    }

    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "hawdb-held-wal-rollback-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ))
    }

    #[cfg(unix)]
    #[test]
    fn captured_wal_identity_cache_observes_namespace_replacement_and_rotation() {
        let root = path();
        let files = crate::file_descriptors::ProjectFileDescriptors::acquire(&root, 16).unwrap();
        let context = files.io_context();
        let wal = root.join("active.wal");
        let moved = root.join("old.wal");
        let alias = root.join("alias.wal");
        std::fs::write(&wal, b"original").unwrap();
        std::os::unix::fs::symlink(&wal, &alias).unwrap();
        let original = Arc::new(WalAppendFile::new(File::open(&wal).unwrap()));
        let pinned = Arc::clone(&original);
        assert!(original.matches_native_path_in(&alias, &context).unwrap());
        std::fs::rename(&wal, &moved).unwrap();
        std::fs::write(&wal, b"original").unwrap();
        assert!(!pinned.matches_native_path_in(&alias, &context).unwrap());
        assert!(pinned.matches_native_path_in(&moved, &context).unwrap());
        let rotated = WalAppendFile::new(File::open(&wal).unwrap());
        assert!(rotated.matches_native_path_in(&wal, &context).unwrap());
        assert!(!rotated.matches_native_path_in(&moved, &context).unwrap());
        std::fs::remove_file(&alias).unwrap();
        std::os::unix::fs::symlink(&moved, &alias).unwrap();
        assert!(pinned.matches_native_path_in(&alias, &context).unwrap());
        std::fs::remove_file(&moved).unwrap();
        assert_eq!(
            pinned
                .matches_native_path_in(&alias, &context)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        std::fs::write(&moved, b"original").unwrap();
        assert!(!pinned.matches_native_path_in(&alias, &context).unwrap());
        drop((original, pinned, rotated));
        assert_eq!(files.metrics().open, 0);
        assert_eq!(files.metrics().reserved, 0);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn captured_wal_append_prepays_post_write_descriptor_checks() {
        let root = path();
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&root, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Record", BTreeMap::new())
            .unwrap();
        let durable = store.durable.as_mut().unwrap();
        durable.wal_append_owner = Some(Arc::clone(&durable._directory_lease));
        let files = durable.project_file_descriptors().clone();
        let baseline = files.metrics();
        let competitor = files.clone();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = Arc::new(std::sync::Mutex::new(None));
        let captured_worker = Arc::clone(&worker);
        super::super::AFTER_CAPTURED_WAL_APPEND_OPEN.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let metrics = competitor.metrics();
                let available =
                    metrics.limit - metrics.open - metrics.reserved + metrics.cached_handles;
                let (ready_tx, ready_rx) = std::sync::mpsc::channel();
                let held = std::thread::spawn(move || {
                    let _reservation = competitor.reserve(available).unwrap();
                    ready_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                });
                ready_rx.recv().unwrap();
                *captured_worker.lock().unwrap() = Some(held);
            }));
        });
        // A competing owner takes every unreserved descriptor after the append
        // handle has been acquired. Post-write checks must use prepaid capacity.
        let result = store.create_node(&mut catalog, "Record", BTreeMap::new());
        release_tx.send(()).unwrap();
        worker.lock().unwrap().take().unwrap().join().unwrap();
        result.unwrap();
        store.ensure_usable().unwrap();
        assert_eq!(files.metrics().reserved, baseline.reserved);
        assert_eq!(files.metrics().open, baseline.open);
        store
            .create_node(&mut catalog, "Record", BTreeMap::new())
            .unwrap();
        drop(store);
        let mut catalog = Catalog::default();
        let reopened = GraphStore::open(&root, &mut catalog).unwrap();
        assert_eq!(reopened.scan_nodes(None).count(), 3);
        drop(reopened);
        drop(files);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn captured_wal_namespace_barrier_persists_the_original_directory_after_replacement() {
        use crate::power_loss::{
            image::{CrashPlan, ImageLimits},
            PowerLossModel,
        };
        let root = path();
        let files = crate::file_descriptors::ProjectFileDescriptors::acquire(
            &root,
            crate::config::WalReplayConfig::default().max_open_files,
        )
        .unwrap();
        let model = PowerLossModel::attach(&files, ImageLimits::default()).unwrap();
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&root, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Seed", BTreeMap::new())
            .unwrap();
        let durable = store.durable.as_mut().unwrap();
        durable.wal_append_owner = Some(Arc::clone(&durable._directory_lease));
        let owner = durable.wal_append_owner.as_ref().unwrap();
        let directory = owner.captured_directory();
        let mut pending = directory
            .create_child_new(&directory.operation_context(), "pending.1.hawdb")
            .unwrap();
        pending.write_all(b"original namespace barrier").unwrap();
        pending.sync_all().unwrap();
        drop(pending);
        let before = model.capture().unwrap();
        assert!(before
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(std::path::Path::new("pending.1.hawdb"))
            .is_none());
        let moved = root.with_extension("original");
        std::fs::rename(&root, &moved).unwrap();
        std::fs::create_dir(&root).unwrap();
        durable.sync_wal_namespace().unwrap();
        std::fs::remove_dir(&root).unwrap();
        std::fs::rename(moved, &root).unwrap();
        let after = model.capture().unwrap();
        assert_eq!(
            after
                .crash(&CrashPlan::default())
                .unwrap()
                .bytes(std::path::Path::new("pending.1.hawdb"))
                .unwrap(),
            b"original namespace barrier"
        );
        drop(store);
        drop(model);
        drop(files);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn held_wal_rollback_repairs_original_and_preserves_replacement() {
        for seeded in [false, true] {
            let root = path();
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&root, &mut catalog).unwrap();
            if seeded {
                store
                    .create_node(&mut catalog, "Record", BTreeMap::new())
                    .unwrap();
            }
            let durable = store.durable.as_ref().unwrap();
            let wal_path = durable.wal_path.clone();
            let original = wal_path.with_extension("original");
            let before = if seeded {
                std::fs::read(&wal_path).unwrap()
            } else {
                encode_binary_wal_header(durable.wal_generation, durable.wal_replay_start_lsn)
            };
            let foreign = b"foreign WAL evidence must never be truncated or unlinked";
            let replace_path = wal_path.clone();
            let saved = original.clone();
            BEFORE_WAL_ROLLBACK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    std::fs::rename(&replace_path, saved).unwrap();
                    std::fs::write(replace_path, foreign).unwrap();
                }))
            });
            set_wal_append_failpoint(WalAppendFailure::PartialWrite);
            let result = store.create_node(&mut catalog, "Record", BTreeMap::new());
            assert!(matches!(result, Err(HawDBError::StorageIntegrity(message))
                if message.contains("WAL name changed during append rollback")));
            assert!(store.ensure_usable().is_err());
            assert_eq!(std::fs::read(&original).unwrap(), before);
            assert_eq!(std::fs::read(&wal_path).unwrap(), foreign);
            drop(store);
            std::fs::remove_file(&wal_path).unwrap();
            std::fs::rename(original, &wal_path).unwrap();
            let mut catalog = Catalog::default();
            let recovered = GraphStore::open(&root, &mut catalog).unwrap();
            assert_eq!(recovered.scan_nodes(None).count(), usize::from(seeded));
            drop(recovered);
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn held_wal_rollback_keeps_empty_header_for_reopen_and_same_handle_retry() {
        for reopen in [false, true] {
            let root = path();
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&root, &mut catalog).unwrap();
            set_wal_append_failpoint(WalAppendFailure::PartialWrite);
            assert!(matches!(
                store.create_node(&mut catalog, "Record", BTreeMap::new()),
                Err(HawDBError::Storage(_))
            ));
            store.ensure_usable().unwrap();
            let durable = store.durable.as_ref().unwrap();
            assert_eq!(
                std::fs::read(&durable.wal_path).unwrap(),
                encode_binary_wal_header(durable.wal_generation, durable.wal_replay_start_lsn)
            );
            assert!(durable.wal_append_file.is_some());
            assert!(!durable.wal_namespace_known_durable);
            assert_eq!(durable.wal_append_open_count, 1);
            if reopen {
                drop(store);
                catalog = Catalog::default();
                store = GraphStore::open(&root, &mut catalog).unwrap();
                assert_eq!(store.scan_nodes(None).count(), 0);
            }
            store
                .create_node(
                    &mut catalog,
                    "Record",
                    BTreeMap::from([("id".into(), Value::Int(1))]),
                )
                .unwrap();
            assert_eq!(store.durable.as_ref().unwrap().wal_append_open_count, 1);
            assert!(store.durable.as_ref().unwrap().wal_namespace_known_durable);
            drop(store);
            let mut catalog = Catalog::default();
            let recovered = GraphStore::open(&root, &mut catalog).unwrap();
            assert_eq!(recovered.scan_nodes(None).count(), 1);
            drop(recovered);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
