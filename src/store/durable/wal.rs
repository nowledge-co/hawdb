//! WAL admission, append rollback, group durability and generation preparation.

use super::{DurableStore, WalFreeSpaceProbeState, WAL_FREE_SPACE_PROBE_INTERVAL_BYTES};
use crate::error::{Result, SkeinError};
use crate::store::{
    elapsed_micros, encode_binary_wal_header, encode_binary_wal_record, frame_binary_wal_record,
    process_crash_failpoint, sync_parent_dir, wal_generation_file, wal_group_sync_failpoint,
    WalEntry, WalOp, CHECKPOINT_TEMPORARY_SPACE_MULTIPLIER, MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES,
    WAL_BINARY_FILE_HEADER_BYTES,
};
use skein_storage::{
    available_storage_space, durable_replace_file, DurabilityPolicy, StorageDebtController,
    StoragePressureSignals, WalAppendTelemetry, WalSyncGroupFlush, WalSyncGroupProgress,
    WalSyncGroupState,
};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::sync::Arc;

impl DurableStore {
    pub(in crate::store) fn begin_wal_sync_group(&mut self) -> Result<bool> {
        if self.durability != DurabilityPolicy::SyncOnEveryWrite {
            return Ok(false);
        }
        if self.wal_sync_group.is_some() {
            return Err(SkeinError::Storage(
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
        let file = self.wal_append_file.as_ref().ok_or_else(|| {
            SkeinError::Storage(
                "WAL sync group has entries without an open append handle".to_string(),
            )
        })?;
        file.sync_data()?;
        if group.requires_parent_sync() {
            sync_parent_dir(&self.wal_path)?;
        }
        process_crash_failpoint("after_wal_sync");
        Ok(group.into_flush(elapsed_micros(started)))
    }

    pub(in crate::store) fn wal_age_millis(&self) -> Option<u64> {
        let modified = fs::metadata(&self.wal_path).ok()?.modified().ok()?;
        let elapsed = std::time::SystemTime::now().duration_since(modified).ok()?;
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
            return Err(SkeinError::Storage(format!(
                "WAL batch operation limit exceeded before append: max_wal_batch_operations={}",
                self.max_batch_operations.unwrap_or_default()
            )));
        }
        self.append_entry(WalOp::Batch(ops), operation_count, pressure_signals)
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
        if self
            .max_record_bytes
            .is_some_and(|limit| payload.len() > limit)
        {
            return Err(SkeinError::Storage(format!(
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
        process_crash_failpoint("before_wal_append");
        let sync_deferred = self.wal_sync_group.is_some();
        let result = match self.take_wal_append() {
            Err(error) => Err(error),
            Ok((file, created)) => {
                let write_result: Result<()> = (|| {
                    let mut writer = file.as_ref();
                    if self.wal_bytes == 0 {
                        writer.write_all(&header_bytes)?;
                    }
                    #[cfg(test)]
                    {
                        if std::env::var(crate::store::PROCESS_CRASH_POINT_ENV).as_deref()
                            == Ok("during_wal_append")
                        {
                            writer.write_all(&record_bytes[..record_bytes.len() / 2])?;
                            process_crash_failpoint("during_wal_append");
                        }
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
                    writer.write_all(&record_bytes)?;
                    Ok(())
                })();
                let append_result = match write_result {
                    Err(error) => match self.rollback_failed_wal_write(created) {
                        Ok(()) => {
                            // A group may still need this handle to acknowledge
                            // earlier entries even when no later write retries.
                            if self.wal_bytes > 0 {
                                self.wal_append_file = Some(Arc::clone(&file));
                            }
                            Err(SkeinError::Storage(format!(
                                "WAL write failed and was rolled back to byte {}: {error}",
                                self.wal_bytes
                            )))
                        }
                        Err(rollback_error) => Err(SkeinError::StorageIntegrity(format!(
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
                        sync_result.map_err(|error| SkeinError::StorageIntegrity(format!(
                            "WAL append outcome is uncertain after writing the complete record: {error}"
                        )))
                    }
                };
                match append_result {
                    Ok(fsync_micros) => {
                        self.wal_append_file = Some(file);
                        Ok(fsync_micros)
                    }
                    Err(error) => Err(error),
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
            self.next_lsn += 1;
            self.wal_commit_epoch = self.wal_commit_epoch.saturating_add(1);
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

    fn rollback_failed_wal_write(&mut self, created: bool) -> Result<()> {
        #[cfg(test)]
        if crate::store::WAL_APPEND_FAILURE.take() == Some(crate::store::WalAppendFailure::Rollback)
        {
            return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into());
        }
        // Windows append-only handles do not grant the access required to resize.
        let file = OpenOptions::new().write(true).open(&self.wal_path)?;
        if file.metadata()?.len() < self.wal_bytes {
            return Err(SkeinError::Storage(
                "WAL lost previously appended bytes before rollback".to_string(),
            ));
        }
        file.set_len(self.wal_bytes)?;
        file.sync_all()?;
        if created {
            if self.wal_bytes == 0 {
                drop(file);
                fs::remove_file(&self.wal_path)?;
            }
            sync_parent_dir(&self.wal_path)?;
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
        signals.obsolete_generation_bytes =
            self.obsolete_generation_bytes(self.oldest_reader_commit_epoch);
        signals.estimated_checkpoint_temporary_bytes = signals
            .estimated_checkpoint_temporary_bytes
            .saturating_add(pending_wal_bytes.saturating_mul(CHECKPOINT_TEMPORARY_SPACE_MULTIPLIER))
            .max(MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES);
        signals.available_free_space_bytes = Some(available_free_space_bytes);
        let pressure = StorageDebtController.evaluate(signals);
        if pressure.state.admits_mutation() {
            return Ok(());
        }
        let reasons = pressure
            .reason_codes
            .iter()
            .map(|reason| reason.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let recovery = if pressure
            .reason_codes
            .contains(&skein_storage::StoragePressureReasonCode::IntegrityPoisoned)
        {
            "close and reopen the database before retrying"
        } else if pressure
            .reason_codes
            .contains(&skein_storage::StoragePressureReasonCode::FreeSpaceReserve)
        {
            "free storage space before retrying"
        } else {
            "checkpoint the database before retrying"
        };
        Err(SkeinError::Storage(format!(
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
                SkeinError::Storage(
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

    fn take_wal_append(&mut self) -> Result<(Arc<File>, bool)> {
        if let Some(file) = self.wal_append_file.take() {
            return Ok((file, false));
        }
        let open_existing = || OpenOptions::new().append(true).open(&self.wal_path);
        let (file, created) = if self.wal_bytes == 0 {
            match OpenOptions::new()
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
        Ok((Arc::new(file), created))
    }

    fn finish_wal_append(&mut self, file: &File, created: bool) -> Result<u64> {
        #[cfg(test)]
        if crate::store::WAL_APPEND_FAILURE.take() == Some(crate::store::WalAppendFailure::Sync) {
            return Err(std::io::Error::other("injected WAL sync failure").into());
        }
        let mut writer = file;
        writer.flush()?;
        if let Some(group) = &mut self.wal_sync_group {
            if created {
                group.record_wal_created();
            }
            return Ok(0);
        }
        let mut fsync_micros = 0;
        if self.durability == DurabilityPolicy::SyncOnEveryWrite {
            let started = std::time::Instant::now();
            file.sync_data()?;
            if created {
                sync_parent_dir(&self.wal_path)?;
            }
            fsync_micros = elapsed_micros(started);
        }
        Ok(fsync_micros)
    }

    pub(in crate::store) fn prepare_wal_generation(&self, generation: u64) -> Result<()> {
        // New WAL generations always use the binary format; an existing
        // text database therefore upgrades at its next checkpoint.
        let wal_path = self.root_path.join(wal_generation_file(generation));
        let tmp_path = wal_path.with_extension("skein.tmp");
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
