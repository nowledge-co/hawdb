// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Backup, restore and scrub over the library maintenance API
//! (`Database::backup_to`, `Database::restore_backup`,
//! `Database::scrub_storage`), each returning a JSON report.

use std::ffi::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;

use hawdb::{
    Database, HawDBEmbedded, StorageBackupReport, StorageRestoreReport, StorageScrubReport,
};
use serde_json::json;

use super::HawdbDatabase;
use super::{clear_buffer, read_str, set_buffer, set_error, FfiError, FfiStr, HawdbBuffer};

fn backup_report_json(report: &StorageBackupReport) -> serde_json::Value {
    json!({
        "generation": report.generation,
        "checkpoint_commit_epoch": report.checkpoint_commit_epoch,
        "file_count": report.file_count,
        "total_bytes": report.total_bytes,
        "manifest_checksum": report.manifest_checksum,
    })
}

fn restore_report_json(report: &StorageRestoreReport) -> serde_json::Value {
    json!({
        "generation": report.generation,
        "checkpoint_commit_epoch": report.checkpoint_commit_epoch,
        "file_count": report.file_count,
        "total_bytes": report.total_bytes,
        "manifest_checksum": report.manifest_checksum,
    })
}

fn scrub_report_json(report: &StorageScrubReport) -> serde_json::Value {
    json!({
        "generation": report.generation,
        "checked_file_count": report.checked_file_count,
        "checked_bytes": report.checked_bytes,
        "sha256_verified_file_count": report.sha256_verified_file_count,
        "wal_record_count": report.wal_record_count,
        "wal_bytes": report.wal_bytes,
    })
}

/// Maintenance needs a consistent published state; an open user
/// transaction on the handle must be finished first.
fn ensure_no_transaction(database: &HawDBEmbedded) -> Result<(), FfiError> {
    if database.transaction_active() {
        return Err(FfiError {
            kind: "transaction_open".to_string(),
            message: "hawdb: commit or roll back the open transaction before maintenance"
                .to_string(),
            resource_budget: None,
        });
    }
    Ok(())
}

/// Run `action` with the shared panic, report and error conventions.
unsafe fn report_call(
    report_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
    action: impl FnOnce() -> Result<serde_json::Value, FfiError>,
) -> bool {
    unsafe {
        clear_buffer(report_out);
        clear_buffer(err_out);
    }
    match catch_unwind(AssertUnwindSafe(action)) {
        Ok(Ok(report)) => {
            unsafe { set_buffer(report_out, report) };
            true
        }
        Ok(Err(error)) => {
            unsafe { set_error(err_out, error) };
            false
        }
        Err(_) => {
            unsafe { set_error(err_out, FfiError::panic()) };
            false
        }
    }
}

unsafe fn live<'a>(db: *mut HawdbDatabase) -> Result<&'a HawdbDatabase, FfiError> {
    if db.is_null() {
        return Err(FfiError::invalid("hawdb: database handle is NULL"));
    }
    Ok(unsafe { &*db })
}

fn lock(database: &HawdbDatabase) -> Result<std::sync::MutexGuard<'_, HawDBEmbedded>, FfiError> {
    database
        .inner
        .lock()
        .map_err(|_| FfiError::invalid("hawdb: database mutex poisoned"))
}

/// Write a consistent, self-verifying backup of an open database to
/// `dest_path`, a directory that must not exist yet (its parent must).
///
/// The database first checkpoints, so the backup holds every transaction
/// committed before the call; the handle stays open and usable. Writes are
/// paused for the duration of the call. Requires a writable persistent
/// handle: a read-only or in-memory handle, an open transaction
/// (`transaction_open`) or an existing destination fail without writing a
/// backup.
///
/// On success returns true and `report_out` receives
/// `{"generation", "checkpoint_commit_epoch", "file_count", "total_bytes",
/// "manifest_checksum"}` (unsigned 64-bit integers; preserve their
/// precision). On failure returns false and fills `err_out`.
///
/// # Safety
/// `db` must be a live [`super::hawdb_open`] handle; `dest_path` must be
/// readable for `dest_len` bytes or NULL; `report_out` and `err_out` must be
/// NULL or writable [`HawdbBuffer`] storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_backup(
    db: *mut HawdbDatabase,
    dest_path: *const c_char,
    dest_len: usize,
    report_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        report_call(report_out, err_out, || {
            let database = live(db)?;
            let destination = read_str(
                FfiStr {
                    ptr: dest_path,
                    len: dest_len,
                },
                "dest_path",
            )?;
            let mut inner = lock(database)?;
            ensure_no_transaction(&inner)?;
            inner
                .database_mut()
                .backup_to(Path::new(destination))
                .map(|report| backup_report_json(&report))
                .map_err(|error| FfiError::open(&error))
        })
    }
}

/// Restore the backup directory at `src_path` into `dest_path`, a new
/// directory that must not exist yet. Every backup file is verified against
/// the backup manifest (length, CRC32C and SHA-256) before anything is
/// written; the result is an ordinary database to open with
/// [`super::hawdb_open`]. No database handle is needed, and the backup is
/// never modified.
///
/// On success returns true and `report_out` receives
/// `{"generation", "checkpoint_commit_epoch", "file_count", "total_bytes",
/// "manifest_checksum"}`, matching the [`hawdb_backup`] report of the same
/// backup. A damaged or incomplete backup fails without creating
/// `dest_path`.
///
/// # Safety
/// `src_path` and `dest_path` must be readable for `src_len` / `dest_len`
/// bytes or NULL; `report_out` and `err_out` must be NULL or writable
/// [`HawdbBuffer`] storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_restore(
    src_path: *const c_char,
    src_len: usize,
    dest_path: *const c_char,
    dest_len: usize,
    report_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        report_call(report_out, err_out, || {
            let source = read_str(
                FfiStr {
                    ptr: src_path,
                    len: src_len,
                },
                "src_path",
            )?;
            let destination = read_str(
                FfiStr {
                    ptr: dest_path,
                    len: dest_len,
                },
                "dest_path",
            )?;
            Database::restore_backup(Path::new(source), Path::new(destination))
                .map(|report| restore_report_json(&report))
                .map_err(|error| FfiError::open(&error))
        })
    }
}

/// Verify every durable file of the open database's published generation
/// and its WAL against their recorded checksums (CRC32C, and SHA-256 where
/// recorded), streaming without loading the database.
///
/// On success returns true and `report_out` receives
/// `{"generation", "checked_file_count", "checked_bytes",
/// "sha256_verified_file_count", "wal_record_count", "wal_bytes"}`.
/// Damage fails with the engine's storage error kind and a message naming
/// the mismatch; the handle is then poisoned so that later statements fail
/// until it is closed and the database reopened (or restored from a
/// backup). An in-memory handle or an open transaction (`transaction_open`)
/// fails without scrubbing. Read-only handles can be scrubbed.
///
/// # Safety
/// `db` must be a live [`super::hawdb_open`] handle; `report_out` and
/// `err_out` must be NULL or writable [`HawdbBuffer`] storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_scrub(
    db: *mut HawdbDatabase,
    report_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        report_call(report_out, err_out, || {
            let mut inner = lock(live(db)?)?;
            ensure_no_transaction(&inner)?;
            inner
                .database_mut()
                .scrub_storage()
                .map(|report| scrub_report_json(&report))
                .map_err(|error| FfiError::open(&error))
        })
    }
}
