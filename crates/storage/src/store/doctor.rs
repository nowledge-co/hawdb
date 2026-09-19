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

use super::{
    file_checksum, sync_parent_dir, DurableManifest, WalCursorEvent, WalOpenOutcome,
    WalRecordCursor, MANIFEST_FILE,
};
use crate::error::{HawDBError, Result};
use hawdb_integrity::IntegrityHasher;
pub use hawdb_storage::{
    DatabaseDirectoryLease, WalDoctorOptions, WalRepairAcknowledgement, WalTailRepairPlan,
    WalTailRepairReason, WalTailRepairReport, WAL_DOCTOR_REPAIR_PROTOCOL,
};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const DOCTOR_DIRECTORY: &str = "doctor";
const DOCTOR_QUARANTINE_DIRECTORY: &str = "quarantine";
const MAX_DOCTOR_AUDIT_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[doc(hidden)]
pub struct WalRepairAuditRecord {
    protocol: String,
    state: WalRepairAuditState,
    plan: WalTailRepairPlan,
    quarantine_file: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WalRepairAuditState {
    Prepared,
    Applied,
}

pub struct DatabaseDoctor;

impl DatabaseDoctor {
    pub fn plan_wal_tail_repair(
        path: impl AsRef<Path>,
        options: WalDoctorOptions,
    ) -> Result<WalTailRepairPlan> {
        let path = path.as_ref();
        validate_existing_database_directory(path)?;
        let _lease = DatabaseDirectoryLease::acquire(path)
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        inspect_wal_tail_locked(path, options)
    }

    pub fn apply_wal_tail_repair(
        path: impl AsRef<Path>,
        plan: &WalTailRepairPlan,
        acknowledgement: WalRepairAcknowledgement,
        options: WalDoctorOptions,
    ) -> Result<WalTailRepairReport> {
        validate_acknowledgement(plan, &acknowledgement)?;
        let path = path.as_ref();
        validate_existing_database_directory(path)?;
        let _lease = DatabaseDirectoryLease::acquire(path)
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        apply_wal_tail_repair_locked(path, plan, options)
    }
}

pub(super) fn reject_pending_wal_doctor_repair(path: &Path) -> Result<()> {
    let pending = pending_repair_records(path)?;
    if pending.is_empty() {
        return Ok(());
    }
    Err(HawDBError::Storage(format!(
        "database has {} interrupted WAL doctor repair record(s); finish the repair with DatabaseDoctor before opening the database",
        pending.len()
    )))
}

// Callers hold the same directory lease used by explicit doctor operations.
pub(super) fn resume_automatic_wal_tail_repair_locked(
    path: &Path,
    config: hawdb_storage::WalReplayConfig,
) -> Result<Option<WalTailRepairReport>> {
    if pending_repair_records(path)?.is_empty() {
        return Ok(None);
    }
    automatic_wal_tail_repair_locked(path, config).map(Some)
}

pub(super) fn automatic_wal_tail_repair_locked(
    path: &Path,
    config: hawdb_storage::WalReplayConfig,
) -> Result<WalTailRepairReport> {
    let options = WalDoctorOptions {
        max_wal_bytes: config.max_bytes,
        max_record_bytes: config.max_record_bytes,
        max_batch_operations: config.max_batch_operations,
    };
    let plan = inspect_wal_tail_locked(path, options)?;
    if config
        .max_bytes
        .is_some_and(|limit| plan.original_wal_len > limit)
    {
        return Err(HawDBError::Storage(
            "automatic WAL repair exceeds the configured replay byte limit".to_string(),
        ));
    }
    let directory = doctor_directory(path).join(DOCTOR_QUARANTINE_DIRECTORY);
    let mut retained_bytes = 0u64;
    if directory.exists() {
        for entry in fs::read_dir(&directory)? {
            retained_bytes = retained_bytes
                .checked_add(entry?.metadata()?.len())
                .ok_or_else(|| {
                    HawDBError::Storage("doctor quarantine size overflow".to_string())
                })?;
        }
    }
    if !directory.join(quarantine_file_name(&plan)).exists() {
        retained_bytes = retained_bytes
            .checked_add(plan.original_wal_len)
            .ok_or_else(|| HawDBError::Storage("doctor quarantine size overflow".to_string()))?;
    }
    // Automatic recovery must preserve prior audit evidence, even when the
    // quarantine budget is exhausted. Explicit doctor repair remains available.
    if retained_bytes > config.max_quarantine_bytes {
        return Err(HawDBError::Storage(format!(
            "automatic WAL repair quarantine byte limit exceeded: required={retained_bytes}, max_wal_quarantine_bytes={}",
            config.max_quarantine_bytes
        )));
    }
    apply_wal_tail_repair_locked(path, &plan, options)
}

fn validate_existing_database_directory(path: &Path) -> Result<()> {
    if !path.exists() {
        return Err(HawDBError::Storage(format!(
            "database doctor path does not exist: {}",
            path.display()
        )));
    }
    if !path.is_dir() {
        return Err(HawDBError::Storage(format!(
            "database doctor path is not a directory: {}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_acknowledgement(
    plan: &WalTailRepairPlan,
    acknowledgement: &WalRepairAcknowledgement,
) -> Result<()> {
    if !acknowledgement.accepts(plan) {
        return Err(HawDBError::Storage(
            "WAL doctor repair requires explicit acknowledgement of the exact plan and potential data loss"
                .to_string(),
        ));
    }
    Ok(())
}

fn inspect_wal_tail_locked(path: &Path, options: WalDoctorOptions) -> Result<WalTailRepairPlan> {
    let manifest_path = path.join(MANIFEST_FILE);
    let manifest = DurableManifest::load(&manifest_path)?;
    manifest.validate()?;
    validate_checkpoint_boundary(path, manifest)?;
    let wal_path = manifest.wal_path(path);
    let wal_len = fs::metadata(&wal_path)
        .map_err(|error| {
            HawDBError::Storage(format!(
                "failed to inspect WAL generation {}: {error}",
                manifest.wal_generation
            ))
        })?
        .len();
    if options.max_wal_bytes.is_some_and(|limit| wal_len > limit) {
        return Err(HawDBError::Storage(format!(
            "WAL doctor byte limit exceeded: max_wal_bytes={}",
            options.max_wal_bytes.unwrap_or_default()
        )));
    }

    let mut cursor = match WalRecordCursor::open(&wal_path, options.max_record_bytes)? {
        WalOpenOutcome::Cursor(cursor) => cursor,
        WalOpenOutcome::MissingHeader => {
            return Err(HawDBError::Storage(format!(
                "WAL generation {} is missing its header",
                manifest.wal_generation
            )));
        }
        WalOpenOutcome::HeaderTorn { .. } => {
            return Err(HawDBError::Storage(
                "WAL doctor rejected an incomplete WAL header".to_string(),
            ));
        }
        WalOpenOutcome::HeaderCorrupt { reason } => {
            return Err(HawDBError::Storage(format!(
                "WAL doctor rejected corruption at byte offset 0: {reason}"
            )));
        }
    };
    if cursor.generation() != manifest.wal_generation
        || cursor.start_lsn() != manifest.wal_replay_start_lsn
    {
        return Err(HawDBError::Storage(format!(
            "WAL header generation/start ({}, {}) does not match manifest ({}, {})",
            cursor.generation(),
            cursor.start_lsn(),
            manifest.wal_generation,
            manifest.wal_replay_start_lsn
        )));
    }
    let mut expected_lsn = manifest.wal_replay_start_lsn;
    loop {
        let (entry, record_start) = match cursor.next()? {
            WalCursorEvent::Eof => break,
            WalCursorEvent::TornTail {
                valid_prefix_len, ..
            } => {
                let plan = build_plan(
                    &manifest_path,
                    &wal_path,
                    manifest,
                    expected_lsn,
                    valid_prefix_len,
                    wal_len,
                )?;
                if let Some(pending) = load_matching_pending_record(path, &plan.plan_id)?
                    && pending.plan != plan
                {
                    return Err(HawDBError::Storage(
                        "pending WAL doctor repair record does not match the current repair plan"
                            .to_string(),
                    ));
                }
                return Ok(plan);
            }
            WalCursorEvent::Corrupt { offset, reason } => {
                return Err(HawDBError::Storage(format!(
                    "WAL doctor rejected corruption at byte offset {offset}: {reason}"
                )));
            }
            WalCursorEvent::Entry {
                entry,
                start_offset,
                ..
            } => (entry, start_offset),
        };
        if entry.lsn != expected_lsn {
            return Err(HawDBError::Storage(format!(
                "WAL doctor rejected LSN sequence mismatch at byte offset {record_start}: expected {expected_lsn}, got {}",
                entry.lsn
            )));
        }
        if let super::WalOp::Batch(operations) = &entry.op
            && options
                .max_batch_operations
                .is_some_and(|limit| operations.len() > limit)
        {
            return Err(HawDBError::Storage(format!(
                "WAL doctor batch operation limit exceeded: max_batch_operations={}",
                options.max_batch_operations.unwrap_or_default()
            )));
        }
        expected_lsn = expected_lsn.checked_add(1).ok_or_else(|| {
            HawDBError::Storage("WAL LSN overflow during doctor scan".to_string())
        })?;
    }

    if let Some(record) = load_single_pending_record(path)? {
        validate_pending_truncated_wal(path, &record)?;
        return Ok(record.plan);
    }
    Err(HawDBError::Storage(
        "WAL doctor found no repairable incomplete final record".to_string(),
    ))
}

fn build_plan(
    manifest_path: &Path,
    wal_path: &Path,
    manifest: DurableManifest,
    next_lsn_after_repair: u64,
    retained_wal_len: u64,
    original_wal_len: u64,
) -> Result<WalTailRepairPlan> {
    let (manifest_len, manifest_crc32c, manifest_sha256) = file_checksum(manifest_path)?;
    let (actual_wal_len, original_wal_crc32c, original_wal_sha256) = file_checksum(wal_path)?;
    if actual_wal_len != original_wal_len {
        return Err(HawDBError::Storage(
            "WAL changed while the doctor repair plan was being generated".to_string(),
        ));
    }
    let (retained_len, retained_wal_crc32c, retained_wal_sha256) =
        file_prefix_checksum(wal_path, retained_wal_len)?;
    let mut plan = WalTailRepairPlan {
        protocol: WAL_DOCTOR_REPAIR_PROTOCOL.to_string(),
        plan_id: String::new(),
        wal_generation: manifest.wal_generation,
        wal_replay_start_lsn: manifest.wal_replay_start_lsn,
        next_lsn_after_repair,
        manifest_len,
        manifest_crc32c,
        manifest_sha256: manifest_sha256.to_string(),
        original_wal_len,
        original_wal_crc32c,
        original_wal_sha256: original_wal_sha256.to_string(),
        retained_wal_len: retained_len,
        retained_wal_crc32c,
        retained_wal_sha256: retained_wal_sha256.to_string(),
        discarded_wal_tail_bytes: original_wal_len.saturating_sub(retained_len),
        reason: WalTailRepairReason::IncompleteFinalRecord,
        data_loss_possible: true,
    };
    plan.plan_id = plan_identity(&plan);
    Ok(plan)
}

fn plan_identity(plan: &WalTailRepairPlan) -> String {
    let identity = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{:?}\n{}",
        plan.protocol,
        plan.wal_generation,
        plan.wal_replay_start_lsn,
        plan.next_lsn_after_repair,
        plan.manifest_len,
        plan.manifest_crc32c,
        plan.manifest_sha256,
        plan.original_wal_len,
        plan.original_wal_crc32c,
        plan.original_wal_sha256,
        plan.retained_wal_len,
        plan.retained_wal_crc32c,
        plan.retained_wal_sha256,
        plan.discarded_wal_tail_bytes,
        plan.reason,
        plan.data_loss_possible
    );
    hawdb_integrity::integrity_digest(identity.as_bytes())
        .sha256
        .to_string()
}

fn apply_wal_tail_repair_locked(
    path: &Path,
    requested_plan: &WalTailRepairPlan,
    options: WalDoctorOptions,
) -> Result<WalTailRepairReport> {
    if requested_plan.protocol != WAL_DOCTOR_REPAIR_PROTOCOL
        || requested_plan.plan_id != plan_identity(requested_plan)
        || !requested_plan.data_loss_possible
        || requested_plan.discarded_wal_tail_bytes == 0
    {
        return Err(HawDBError::Storage(
            "WAL doctor repair plan identity is invalid".to_string(),
        ));
    }

    let manifest = DurableManifest::load(&path.join(MANIFEST_FILE))?;
    manifest.validate()?;
    if manifest.wal_generation != requested_plan.wal_generation {
        return Err(HawDBError::Storage(
            "WAL generation changed after the doctor repair plan was created".to_string(),
        ));
    }
    let wal_path = manifest.wal_path(path);
    let pending = load_matching_pending_record(path, &requested_plan.plan_id)?;
    let current = file_checksum(&wal_path)?;
    let original_matches = file_identity_matches(
        current,
        requested_plan.original_wal_len,
        requested_plan.original_wal_crc32c,
        &requested_plan.original_wal_sha256,
    );
    let retained_matches = file_identity_matches(
        current,
        requested_plan.retained_wal_len,
        requested_plan.retained_wal_crc32c,
        &requested_plan.retained_wal_sha256,
    );
    if retained_matches {
        let pending = pending.ok_or_else(|| {
            HawDBError::Storage(
                "WAL already matches the retained prefix without a pending doctor audit record"
                    .to_string(),
            )
        })?;
        if pending.plan != *requested_plan {
            return Err(HawDBError::Storage(
                "pending WAL doctor repair record does not match the requested plan".to_string(),
            ));
        }
        return finalize_repair(path, pending, true);
    }
    if !original_matches {
        return Err(HawDBError::Storage(
            "WAL changed after the doctor repair plan was created; no files were modified"
                .to_string(),
        ));
    }

    let current_plan = inspect_wal_tail_locked(path, options)?;
    if current_plan != *requested_plan {
        return Err(HawDBError::Storage(
            "WAL doctor repair plan no longer matches the current database state".to_string(),
        ));
    }
    validate_manifest_identity(path, requested_plan)?;

    let prepared = match pending {
        Some(record) => {
            if record.plan != *requested_plan {
                return Err(HawDBError::Storage(
                    "pending WAL doctor repair record does not match the requested plan"
                        .to_string(),
                ));
            }
            validate_quarantine(path, &record)?;
            record
        }
        None => prepare_repair(path, &wal_path, requested_plan)?,
    };
    validate_quarantine(path, &prepared)?;
    super::process_crash_failpoint("after_wal_repair_prepare");

    validate_manifest_identity(path, requested_plan)?;
    let before_truncate = file_checksum(&wal_path)?;
    if !file_identity_matches(
        before_truncate,
        requested_plan.original_wal_len,
        requested_plan.original_wal_crc32c,
        &requested_plan.original_wal_sha256,
    ) {
        return Err(HawDBError::Storage(
            "WAL changed after the doctor repair was prepared; pending audit was retained"
                .to_string(),
        ));
    }
    let wal = OpenOptions::new().read(true).write(true).open(&wal_path)?;
    wal.set_len(requested_plan.retained_wal_len)?;
    wal.sync_all()?;
    sync_parent_dir(&wal_path)?;
    super::process_crash_failpoint("after_wal_repair_truncate");
    let after_truncate = file_checksum(&wal_path)?;
    if !file_identity_matches(
        after_truncate,
        requested_plan.retained_wal_len,
        requested_plan.retained_wal_crc32c,
        &requested_plan.retained_wal_sha256,
    ) {
        return Err(HawDBError::Storage(
            "WAL doctor repair produced an unexpected retained WAL identity; pending audit was retained"
                .to_string(),
        ));
    }
    finalize_repair(path, prepared, false)
}

#[doc(hidden)]
pub fn prepare_repair(
    path: &Path,
    wal_path: &Path,
    plan: &WalTailRepairPlan,
) -> Result<WalRepairAuditRecord> {
    let doctor_dir = doctor_directory(path);
    let quarantine_dir = doctor_dir.join(DOCTOR_QUARANTINE_DIRECTORY);
    fs::create_dir_all(&quarantine_dir)?;
    sync_parent_dir(&doctor_dir)?;
    sync_parent_dir(&quarantine_dir)?;
    let quarantine_file = quarantine_file_name(plan);
    let quarantine_path = quarantine_dir.join(&quarantine_file);
    if quarantine_path.exists() {
        let identity = file_checksum(&quarantine_path)?;
        if !file_identity_matches(
            identity,
            plan.original_wal_len,
            plan.original_wal_crc32c,
            &plan.original_wal_sha256,
        ) {
            return Err(HawDBError::Storage(
                "existing WAL doctor quarantine file has the wrong identity".to_string(),
            ));
        }
    } else {
        super::copy_file_with_checksum(wal_path, &quarantine_path)?;
        sync_parent_dir(&quarantine_path)?;
    }
    let record = WalRepairAuditRecord {
        protocol: WAL_DOCTOR_REPAIR_PROTOCOL.to_string(),
        state: WalRepairAuditState::Prepared,
        plan: plan.clone(),
        quarantine_file,
    };
    write_audit_record(&pending_record_path(path, plan), &record)?;
    Ok(record)
}

fn finalize_repair(
    path: &Path,
    mut record: WalRepairAuditRecord,
    resumed_interrupted_repair: bool,
) -> Result<WalTailRepairReport> {
    validate_quarantine(path, &record)?;
    record.state = WalRepairAuditState::Applied;
    let applied_path = applied_record_path(path, &record.plan);
    write_audit_record(&applied_path, &record)?;
    let pending_path = pending_record_path(path, &record.plan);
    if pending_path.exists() {
        fs::remove_file(&pending_path)?;
        sync_parent_dir(&pending_path)?;
    }
    Ok(WalTailRepairReport {
        protocol: WAL_DOCTOR_REPAIR_PROTOCOL.to_string(),
        plan_id: record.plan.plan_id.clone(),
        wal_generation: record.plan.wal_generation,
        retained_wal_len: record.plan.retained_wal_len,
        discarded_wal_tail_bytes: record.plan.discarded_wal_tail_bytes,
        next_lsn_after_repair: record.plan.next_lsn_after_repair,
        quarantine_file: record.quarantine_file,
        repair_record_file: file_name(&applied_path)?,
        resumed_interrupted_repair,
    })
}

fn validate_manifest_identity(path: &Path, plan: &WalTailRepairPlan) -> Result<()> {
    let identity = file_checksum(&path.join(MANIFEST_FILE))?;
    if file_identity_matches(
        identity,
        plan.manifest_len,
        plan.manifest_crc32c,
        &plan.manifest_sha256,
    ) {
        Ok(())
    } else {
        Err(HawDBError::Storage(
            "durable manifest changed after the WAL doctor repair plan was created".to_string(),
        ))
    }
}

fn validate_checkpoint_boundary(path: &Path, manifest: DurableManifest) -> Result<()> {
    let Some(generation) = manifest.checkpoint_generation else {
        return Ok(());
    };
    let expected_len = manifest
        .checkpoint_encoded_len
        .ok_or_else(|| HawDBError::Storage("manifest checkpoint length is missing".to_string()))?;
    let expected_crc32c = manifest
        .checkpoint_encoded_checksum
        .ok_or_else(|| HawDBError::Storage("manifest checkpoint CRC32C is missing".to_string()))?;
    let expected_sha256 = manifest
        .checkpoint_encoded_sha256
        .ok_or_else(|| HawDBError::Storage("manifest checkpoint SHA-256 is missing".to_string()))?;
    let identity = file_checksum(&manifest.checkpoint_path(path))?;
    if identity.0 != expected_len || identity.1 != expected_crc32c || identity.2 != expected_sha256
    {
        return Err(HawDBError::Storage(format!(
            "WAL doctor rejected checkpoint generation {generation} because its published identity does not match the manifest"
        )));
    }
    Ok(())
}

fn validate_pending_truncated_wal(path: &Path, record: &WalRepairAuditRecord) -> Result<()> {
    if record.state != WalRepairAuditState::Prepared {
        return Err(HawDBError::Storage(
            "pending WAL doctor record has an invalid state".to_string(),
        ));
    }
    validate_manifest_identity(path, &record.plan)?;
    let manifest = DurableManifest::load(&path.join(MANIFEST_FILE))?;
    let identity = file_checksum(&manifest.wal_path(path))?;
    if !file_identity_matches(
        identity,
        record.plan.retained_wal_len,
        record.plan.retained_wal_crc32c,
        &record.plan.retained_wal_sha256,
    ) {
        return Err(HawDBError::Storage(
            "pending WAL doctor repair does not match the current WAL identity".to_string(),
        ));
    }
    validate_quarantine(path, record)
}

fn validate_quarantine(path: &Path, record: &WalRepairAuditRecord) -> Result<()> {
    let identity = file_checksum(
        &doctor_directory(path)
            .join(DOCTOR_QUARANTINE_DIRECTORY)
            .join(&record.quarantine_file),
    )?;
    if file_identity_matches(
        identity,
        record.plan.original_wal_len,
        record.plan.original_wal_crc32c,
        &record.plan.original_wal_sha256,
    ) {
        Ok(())
    } else {
        Err(HawDBError::Storage(
            "WAL doctor quarantine file does not match the original WAL identity".to_string(),
        ))
    }
}

fn file_prefix_checksum(
    path: &Path,
    limit: u64,
) -> Result<(u64, u64, hawdb_integrity::Sha256Digest)> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(0))?;
    let mut integrity = IntegrityHasher::new();
    let mut total = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    while total < limit {
        let remaining = limit.saturating_sub(total).min(buffer.len() as u64) as usize;
        let read = file.read(&mut buffer[..remaining])?;
        if read == 0 {
            break;
        }
        integrity.update(&buffer[..read]);
        total = total.saturating_add(read as u64);
    }
    if total != limit {
        return Err(HawDBError::Storage(
            "WAL ended before the planned retained prefix".to_string(),
        ));
    }
    let digest = integrity.finish();
    Ok((total, digest.crc32c.as_u64(), digest.sha256))
}

fn file_identity_matches(
    identity: (u64, u64, hawdb_integrity::Sha256Digest),
    expected_len: u64,
    expected_crc32c: u64,
    expected_sha256: &str,
) -> bool {
    identity.0 == expected_len
        && identity.1 == expected_crc32c
        && identity.2.to_string() == expected_sha256
}

fn write_audit_record(path: &Path, record: &WalRepairAuditRecord) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        HawDBError::Storage("WAL doctor audit path has no parent directory".to_string())
    })?;
    fs::create_dir_all(parent)?;
    let encoded = serde_json::to_vec_pretty(record).map_err(|error| {
        HawDBError::Storage(format!("failed to encode WAL doctor audit record: {error}"))
    })?;
    let temp_path = path.with_extension("json.tmp");
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;
        std::io::Write::write_all(&mut file, &encoded)?;
        file.sync_all()?;
    }
    hawdb_storage::durable_replace_file(&temp_path, path)
        .map_err(|error| HawDBError::Storage(error.to_string()))
}

#[doc(hidden)]
pub fn pending_repair_records(path: &Path) -> Result<Vec<PathBuf>> {
    let doctor_dir = doctor_directory(path);
    if !doctor_dir.exists() {
        return Ok(Vec::new());
    }
    let mut pending = Vec::new();
    for entry in fs::read_dir(&doctor_dir)? {
        let path = entry?.path();
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".repair.pending.json"))
        {
            pending.push(path);
        }
    }
    pending.sort();
    Ok(pending)
}

fn load_single_pending_record(path: &Path) -> Result<Option<WalRepairAuditRecord>> {
    let records = pending_repair_records(path)?;
    match records.as_slice() {
        [] => Ok(None),
        [record] => load_audit_record(record).map(Some),
        _ => Err(HawDBError::Storage(
            "database has multiple pending WAL doctor repair records".to_string(),
        )),
    }
}

fn load_matching_pending_record(
    path: &Path,
    plan_id: &str,
) -> Result<Option<WalRepairAuditRecord>> {
    let Some(record) = load_single_pending_record(path)? else {
        return Ok(None);
    };
    if record.plan.plan_id == plan_id {
        Ok(Some(record))
    } else {
        Err(HawDBError::Storage(
            "database has a pending WAL doctor repair for a different plan".to_string(),
        ))
    }
}

fn load_audit_record(path: &Path) -> Result<WalRepairAuditRecord> {
    let encoded_len = fs::metadata(path)?.len();
    if encoded_len > MAX_DOCTOR_AUDIT_BYTES {
        return Err(HawDBError::Storage(format!(
            "WAL doctor audit record exceeds the {MAX_DOCTOR_AUDIT_BYTES} byte limit"
        )));
    }
    let encoded = fs::read(path)?;
    let record = serde_json::from_slice::<WalRepairAuditRecord>(&encoded).map_err(|error| {
        HawDBError::Storage(format!("invalid WAL doctor audit record: {error}"))
    })?;
    if record.protocol != WAL_DOCTOR_REPAIR_PROTOCOL
        || record.plan.protocol != WAL_DOCTOR_REPAIR_PROTOCOL
        || record.plan.plan_id != plan_identity(&record.plan)
        || record.quarantine_file != quarantine_file_name(&record.plan)
    {
        return Err(HawDBError::Storage(
            "WAL doctor audit record identity is invalid".to_string(),
        ));
    }
    Ok(record)
}

#[doc(hidden)]
pub fn doctor_directory(path: &Path) -> PathBuf {
    path.join(DOCTOR_DIRECTORY)
}

#[doc(hidden)]
pub fn pending_record_path(path: &Path, plan: &WalTailRepairPlan) -> PathBuf {
    doctor_directory(path).join(format!(
        "wal.{}.{}.repair.pending.json",
        plan.wal_generation, plan.plan_id
    ))
}

#[doc(hidden)]
pub fn applied_record_path(path: &Path, plan: &WalTailRepairPlan) -> PathBuf {
    doctor_directory(path).join(format!(
        "wal.{}.{}.repair.applied.json",
        plan.wal_generation, plan.plan_id
    ))
}

fn quarantine_file_name(plan: &WalTailRepairPlan) -> String {
    format!(
        "wal.{}.{}.before-repair.hawdb",
        plan.wal_generation, plan.plan_id
    )
}

fn file_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string)
        .ok_or_else(|| HawDBError::Storage("WAL doctor path has no valid file name".to_string()))
}

