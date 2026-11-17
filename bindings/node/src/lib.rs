// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Node-API ownership over the existing, panic-contained embedded C ABI.
//! No native pointer or foreign allocation is exposed to JavaScript.

use std::ffi::c_char;
use std::ptr::NonNull;

use hawdb_ffi as ffi;
use napi::{Error, Result, Status};
use napi_derive::napi;

struct Buffer(ffi::HawdbBuffer);

impl Buffer {
    fn new() -> Self {
        Self(ffi::HawdbBuffer {
            ptr: std::ptr::null_mut(),
            len: 0,
        })
    }

    fn text(&self) -> Result<String> {
        if self.0.ptr.is_null() {
            return Ok(String::new());
        }
        // The C ABI owns this allocation until Buffer::drop, and len excludes NUL.
        let bytes = unsafe { std::slice::from_raw_parts(self.0.ptr.cast(), self.0.len) };
        String::from_utf8(bytes.to_vec())
            .map_err(|error| Error::new(Status::GenericFailure, error.to_string()))
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        // This owner releases precisely the buffer returned by the C ABI.
        unsafe { ffi::hawdb_buffer_free(&mut self.0) };
    }
}

fn call(
    operation: impl FnOnce(*mut ffi::HawdbBuffer, *mut ffi::HawdbBuffer) -> bool,
) -> Result<String> {
    let mut output = Buffer::new();
    let mut error = Buffer::new();
    if operation(&mut output.0, &mut error.0) {
        output.text()
    } else {
        Err(Error::new(Status::GenericFailure, error.text()?))
    }
}

type PathReport = unsafe extern "C" fn(
    *mut ffi::HawdbDatabase,
    *const c_char,
    usize,
    *mut ffi::HawdbBuffer,
    *mut ffi::HawdbBuffer,
) -> bool;

/// A native object owns one handle. Calls are synchronous; no handle is shared
/// between JavaScript environments or handed to an asynchronous worker.
#[napi]
pub struct NativeDatabase {
    handle: Option<NonNull<ffi::HawdbDatabase>>,
}

impl NativeDatabase {
    fn from_handle(handle: *mut ffi::HawdbDatabase, error: &Buffer) -> Result<Self> {
        NonNull::new(handle)
            .map(|handle| Self {
                handle: Some(handle),
            })
            .ok_or_else(|| {
                Error::new(
                    Status::GenericFailure,
                    error.text().unwrap_or_else(|error| error.to_string()),
                )
            })
    }

    fn handle(&self) -> Result<*mut ffi::HawdbDatabase> {
        self.handle.map(NonNull::as_ptr).ok_or_else(|| {
            Error::new(
                Status::GenericFailure,
                r#"{"kind":"closed","message":"HawDB database is closed"}"#,
            )
        })
    }

    fn path_report(&self, path: &str, operation: PathReport) -> Result<String> {
        let handle = self.handle()?;
        // The owned string and handle remain live for this synchronous call.
        call(|output, error| unsafe {
            operation(handle, path.as_ptr().cast(), path.len(), output, error)
        })
    }
}

#[napi]
impl NativeDatabase {
    #[napi(factory)]
    pub fn open(path: String, options: String) -> Result<Self> {
        let mut error = Buffer::new();
        // Strings and output storage live until the C ABI has copied them.
        let handle = unsafe {
            ffi::hawdb_open(
                path.as_ptr().cast(),
                path.len(),
                options.as_ptr().cast(),
                options.len(),
                &mut error.0,
            )
        };
        Self::from_handle(handle, &error)
    }

    #[napi(factory)]
    pub fn open_in_memory() -> Result<Self> {
        let mut error = Buffer::new();
        let handle = unsafe { ffi::hawdb_open_in_memory(&mut error.0) };
        Self::from_handle(handle, &error)
    }

    #[napi]
    pub fn query(
        &self,
        statement: String,
        parameters: String,
        sql: bool,
        transaction: bool,
    ) -> Result<String> {
        let handle = self.handle()?;
        let operation = match (sql, transaction) {
            (false, false) => ffi::hawdb_query,
            (true, false) => ffi::hawdb_query_sql,
            (false, true) => ffi::hawdb_transaction_query,
            (true, true) => ffi::hawdb_transaction_query_sql,
        };
        // The C ABI validates parameters and returns owned JSON, copied by call.
        call(|output, error| unsafe {
            operation(
                handle,
                statement.as_ptr().cast(),
                statement.len(),
                parameters.as_ptr().cast(),
                parameters.len(),
                output,
                error,
            )
        })
    }

    #[napi]
    pub fn begin(&self) -> Result<()> {
        let handle = self.handle()?;
        call(|_, error| unsafe { ffi::hawdb_begin_transaction(handle, error) }).map(|_| ())
    }

    #[napi]
    pub fn commit(&self) -> Result<()> {
        let handle = self.handle()?;
        call(|output, error| unsafe { ffi::hawdb_commit_transaction(handle, output, error) })
            .map(|_| ())
    }

    #[napi]
    pub fn rollback(&self) -> Result<()> {
        let handle = self.handle()?;
        call(|_, error| unsafe { ffi::hawdb_rollback_transaction(handle, error) }).map(|_| ())
    }

    #[napi]
    pub fn backup(&self, path: String) -> Result<String> {
        self.path_report(&path, ffi::hawdb_backup)
    }

    #[napi]
    pub fn export(&self, path: String) -> Result<String> {
        self.path_report(&path, ffi::hawdb_export)
    }

    #[napi]
    pub fn scrub(&self) -> Result<String> {
        let handle = self.handle()?;
        call(|output, error| unsafe { ffi::hawdb_scrub(handle, output, error) })
    }

    #[napi]
    pub fn close(&mut self) {
        if let Some(handle) = self.handle.take() {
            // Taking the unique owner makes explicit close and finalization idempotent.
            unsafe { ffi::hawdb_close(handle.as_ptr()) };
        }
    }
}

impl Drop for NativeDatabase {
    fn drop(&mut self) {
        self.close();
    }
}

#[napi]
pub fn version() -> Result<String> {
    let mut output = Buffer::new();
    unsafe { ffi::hawdb_version(&mut output.0) };
    output.text()
}

#[napi]
pub fn format_version() -> u32 {
    ffi::hawdb_format_version()
}

#[napi]
pub fn dump_format_version() -> u32 {
    ffi::hawdb_dump_format_version()
}

#[napi]
pub fn restore(source: String, destination: String, logical: bool) -> Result<String> {
    let operation = if logical {
        ffi::hawdb_import
    } else {
        ffi::hawdb_restore
    };
    call(|output, error| unsafe {
        operation(
            source.as_ptr().cast(),
            source.len(),
            destination.as_ptr().cast(),
            destination.len(),
            output,
            error,
        )
    })
}

#[napi]
pub fn verify_dump(path: String) -> Result<String> {
    call(|output, error| unsafe {
        ffi::hawdb_verify_dump(path.as_ptr().cast(), path.len(), output, error)
    })
}

#[napi]
pub fn confirm_dump_publication(path: String) -> Result<()> {
    call(|_, error| unsafe {
        ffi::hawdb_confirm_dump_publication(path.as_ptr().cast(), path.len(), error)
    })
    .map(|_| ())
}
