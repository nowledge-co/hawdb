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

//! Logical export/import and the persistent format version
//! (docs/LOGICAL_DUMP.md). Thin wrappers over the typed Rust API; reports are
//! the Rust report structs serialized field for field.

use std::ffi::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};

use hawdb::{HawDBEmbedded, LogicalDumpExportReport, LogicalDumpImportReport};
use serde_json::json;

use crate::{
    clear_buffer, read_str, set_buffer, set_error, FfiError, FfiStr, HawdbBuffer, HawdbDatabase,
};

/// The engine-wide persistent format version of this library. Two library
/// builds with the same value open each other's databases. Before replacing
/// the library with one whose value differs, export each database with
/// [`hawdb_export`] using the old library and import it with
/// [`hawdb_import`] using the new one.
#[unsafe(no_mangle)]
pub extern "C" fn hawdb_format_version() -> u32 {
    hawdb::persistent_format_version()
}

/// The newest logical dump format version this library writes and reads.
/// [`hawdb_import`] accepts every dump version up to this one.
#[unsafe(no_mangle)]
pub extern "C" fn hawdb_dump_format_version() -> u32 {
    u32::from(hawdb::LOGICAL_DUMP_FORMAT_VERSION)
}

fn export_report_json(report: &LogicalDumpExportReport) -> serde_json::Value {
    json!({
        "dump_format_version": report.dump_format_version,
        "persistent_format_version": report.persistent_format_version,
        "hawdb_version": report.hawdb_version,
        "branch": report.branch,
        "commit_epoch": report.commit_epoch,
        "table_count": report.table_count,
        "table_row_count": report.table_row_count,
        "node_count": report.node_count,
        "relationship_count": report.relationship_count,
        "file_count": report.file_count,
        "total_bytes": report.total_bytes,
        "manifest_sha256": report.manifest_sha256,
    })
}

fn import_report_json(report: &LogicalDumpImportReport) -> serde_json::Value {
    json!({
        "dump_format_version": report.dump_format_version,
        "source_hawdb_version": report.source_hawdb_version,
        "source_persistent_format_version": report.source_persistent_format_version,
        "persistent_format_version": report.persistent_format_version,
        "table_count": report.table_count,
        "table_row_count": report.table_row_count,
        "node_count": report.node_count,
        "relationship_count": report.relationship_count,
        "file_count": report.file_count,
        "total_bytes": report.total_bytes,
        "manifest_sha256": report.manifest_sha256,
    })
}

/// Export the handle's current snapshot as a logical dump directory at
/// `dest_path` (`dest_len` UTF-8 bytes), which must not exist; its parent
/// must. The dump is written beside the destination and renamed onto it only
/// when complete, so a failed export leaves nothing at `dest_path`. The call
/// holds the handle for its duration, so it reflects one consistent snapshot.
///
/// On success returns true and `report_out` receives
/// `{"dump_format_version", "persistent_format_version", "hawdb_version",
/// "branch", "commit_epoch", "table_count", "table_row_count", "node_count",
/// "relationship_count", "file_count", "total_bytes", "manifest_sha256"}`.
/// On failure returns false and `err_out` receives `{"kind", "message"}`:
/// `semantic` when the database holds a kind of data the dump format cannot
/// represent yet (nothing is written), `transaction_open` while the handle has
/// an open transaction, `invalid_argument` for a NULL or non-UTF-8 argument.
///
/// # Safety
/// `db` must be a live [`crate::hawdb_open`] handle or NULL; `dest_path` must
/// be readable for `dest_len` bytes or NULL; `report_out` and `err_out` must
/// be NULL or writable [`HawdbBuffer`] storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_export(
    db: *mut HawdbDatabase,
    dest_path: *const c_char,
    dest_len: usize,
    report_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        clear_buffer(report_out);
        clear_buffer(err_out);
    }
    if db.is_null() {
        unsafe { set_error(err_out, FfiError::invalid("hawdb: database handle is NULL")) };
        return false;
    }
    let database = unsafe { &*db };
    let result = catch_unwind(AssertUnwindSafe(|| {
        let destination = unsafe {
            read_str(
                FfiStr {
                    ptr: dest_path,
                    len: dest_len,
                },
                "dest_path",
            )
        }?;
        let inner = database
            .inner
            .lock()
            .map_err(|_| FfiError::invalid("hawdb: database mutex poisoned"))?;
        if inner.transaction_active() {
            // Built inline, like the maintenance exports, so the transaction
            // and maintenance changes need no shared constructor.
            return Err(FfiError {
                kind: "transaction_open".to_string(),
                message: "hawdb: export is refused while a transaction is open on this handle"
                    .to_string(),
                resource_budget: None,
            });
        }
        inner
            .export_logical_dump(destination)
            .map(|report| export_report_json(&report))
            .map_err(|error| FfiError::open(&error))
    }));
    unsafe { finish(result, report_out, err_out) }
}

/// Build a new database at `dest_path` from the logical dump at `src_path`.
/// `dest_path` must not exist; its parent must. The database is built in an
/// unpublished directory beside the destination, every file is verified
/// against the dump's SHA-256 and record counts, and the result is renamed
/// onto `dest_path` only on complete success: a failed or interrupted import
/// never leaves a database at `dest_path`. Needs no open handle; open the
/// result with [`crate::hawdb_open`].
///
/// On success returns true and `report_out` receives
/// `{"dump_format_version", "source_hawdb_version",
/// "source_persistent_format_version", "persistent_format_version",
/// "table_count", "table_row_count", "node_count", "relationship_count",
/// "file_count", "total_bytes", "manifest_sha256"}`. A dump written by a
/// newer HawDB fails with `unsupported_storage_format`; a damaged or
/// incomplete dump fails with `storage_integrity`.
///
/// # Safety
/// `src_path` and `dest_path` must be readable for `src_len` / `dest_len`
/// bytes or NULL; `report_out` and `err_out` must be NULL or writable
/// [`HawdbBuffer`] storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_import(
    src_path: *const c_char,
    src_len: usize,
    dest_path: *const c_char,
    dest_len: usize,
    report_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        clear_buffer(report_out);
        clear_buffer(err_out);
    }
    let result = catch_unwind(AssertUnwindSafe(|| {
        let source = unsafe {
            read_str(
                FfiStr {
                    ptr: src_path,
                    len: src_len,
                },
                "src_path",
            )
        }?;
        let destination = unsafe {
            read_str(
                FfiStr {
                    ptr: dest_path,
                    len: dest_len,
                },
                "dest_path",
            )
        }?;
        HawDBEmbedded::import_logical_dump(source, destination)
            .map(|report| import_report_json(&report))
            .map_err(|error| FfiError::open(&error))
    }));
    unsafe { finish(result, report_out, err_out) }
}

/// # Safety
/// `report_out` and `err_out` must be NULL or writable [`HawdbBuffer`] storage.
unsafe fn finish(
    result: std::thread::Result<Result<serde_json::Value, FfiError>>,
    report_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    match result {
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
