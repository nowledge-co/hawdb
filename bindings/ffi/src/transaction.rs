// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Explicit user transactions over the embedded facade's single transaction
//! slot (`HawDBEmbedded::begin_transaction` and friends).
//!
//! A transaction belongs to its database handle rather than to a second
//! opaque pointer, so there is no transaction handle that can outlive or
//! dangle past its database: every misuse is a reported error, never
//! undefined behaviour. One handle has at most one open transaction.
//! While it is open, autocommit statements on that handle are refused with
//! `transaction_open`; transaction calls without one fail with
//! `no_transaction`. Closing the handle rolls an open transaction back.

use std::ffi::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};

use hawdb::HawDBEmbedded;

use super::{
    clear_buffer, output_to_json, query_impl, set_buffer, set_error, FfiError, FfiStr, HawdbBuffer,
    HawdbDatabase,
};

pub(crate) fn ensure_open(database: &HawDBEmbedded) -> Result<(), FfiError> {
    if database.transaction_active() {
        Ok(())
    } else {
        Err(FfiError::no_transaction(
            "hawdb: no transaction is open on this database; call hawdb_begin_transaction first",
        ))
    }
}

pub(crate) fn ensure_none(database: &HawDBEmbedded) -> Result<(), FfiError> {
    if database.transaction_active() {
        Err(FfiError::transaction_open(
            "hawdb: a transaction is open on this database; run statements with \
             hawdb_transaction_query or hawdb_transaction_query_sql, then commit or roll back",
        ))
    } else {
        Ok(())
    }
}

/// Run `action` against a live handle with the shared panic and error
/// conventions; on success `result_out` (when given) receives its JSON.
unsafe fn with_database(
    db: *mut HawdbDatabase,
    result_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
    action: impl FnOnce(&mut HawDBEmbedded) -> Result<Option<serde_json::Value>, FfiError>,
) -> bool {
    unsafe {
        clear_buffer(result_out);
        clear_buffer(err_out);
    }
    if db.is_null() {
        unsafe { set_error(err_out, FfiError::invalid("hawdb: database handle is NULL")) };
        return false;
    }
    let database = unsafe { &*db };
    let result = catch_unwind(AssertUnwindSafe(|| action(&mut *database.lock()?)));
    match result {
        Ok(Ok(output)) => {
            if let Some(output) = output {
                unsafe { set_buffer(result_out, output) };
            }
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

/// Begin a user transaction on `db`.
///
/// Statements then run through [`hawdb_transaction_query`] and
/// [`hawdb_transaction_query_sql`], see their own writes, and become durable
/// and visible to other readers only together, at
/// [`hawdb_commit_transaction`]. [`hawdb_rollback_transaction`] discards
/// them. While the transaction is open, [`hawdb_query`] and
/// [`hawdb_query_sql`] on this handle fail with `transaction_open`.
///
/// Returns true on success. Fails with `transaction_open` if a transaction
/// is already open on the handle (there is no nesting) and with the engine's
/// own error kind if the database cannot start one (for example a read-only
/// open).
///
/// # Safety
/// `db` must be a live [`super::hawdb_open`] handle; `err_out` must be NULL
/// or writable [`HawdbBuffer`] storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_begin_transaction(
    db: *mut HawdbDatabase,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        with_database(db, std::ptr::null_mut(), err_out, |database| {
            if database.transaction_active() {
                return Err(FfiError::transaction_open(
                    "hawdb: a transaction is already open on this database; \
                     nested transactions are not supported",
                ));
            }
            database
                .begin_transaction()
                .map_err(|error| FfiError::open(&error))?;
            Ok(None)
        })
    }
}

/// Run one Cypher statement inside the open transaction.
///
/// Arguments, value encoding and result/error conventions match
/// [`super::hawdb_query`]. Fails with `no_transaction` when no transaction is
/// open. A failed statement leaves the transaction open; roll it back (a
/// later commit may also refuse it).
///
/// # Safety
/// Same contract as [`super::hawdb_query`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_transaction_query(
    db: *mut HawdbDatabase,
    cypher: *const c_char,
    cypher_len: usize,
    params_json: *const c_char,
    params_len: usize,
    result_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        query_impl(
            db,
            FfiStr {
                ptr: cypher,
                len: cypher_len,
            },
            FfiStr {
                ptr: params_json,
                len: params_len,
            },
            false,
            true,
            result_out,
            err_out,
        )
    }
}

/// Run one SQL statement inside the open transaction.
///
/// Arguments, value encoding and result/error conventions match
/// [`super::hawdb_query_sql`]; otherwise behaves like
/// [`hawdb_transaction_query`].
///
/// # Safety
/// Same contract as [`super::hawdb_query`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_transaction_query_sql(
    db: *mut HawdbDatabase,
    sql: *const c_char,
    sql_len: usize,
    params_json: *const c_char,
    params_len: usize,
    result_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        query_impl(
            db,
            FfiStr {
                ptr: sql,
                len: sql_len,
            },
            FfiStr {
                ptr: params_json,
                len: params_len,
            },
            true,
            true,
            result_out,
            err_out,
        )
    }
}

/// Commit the open transaction as one durable commit.
///
/// On success returns true and `result_out` receives the commit's result
/// envelope (`{"columns": [...], "rows": [...]}`), and every statement is
/// committed, durable under the handle's `durability` option. The
/// transaction is closed after this call whether or not it succeeds. The
/// commit is all or nothing, never partial: a commit refused before
/// publication (for example a write conflict or a budget refusal)
/// publishes none of its statements, but an error raised while publishing,
/// or a crash before this call returns, leaves the outcome unknown, and
/// recovery may show the whole transaction committed. Reconcile or retry
/// idempotently; a missing acknowledgment is not proof of rollback.
/// Fails with `no_transaction` when no transaction is open.
///
/// # Safety
/// `db` must be a live [`super::hawdb_open`] handle; `result_out` and
/// `err_out` must be NULL or writable [`HawdbBuffer`] storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_commit_transaction(
    db: *mut HawdbDatabase,
    result_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        with_database(db, result_out, err_out, |database| {
            ensure_open(database)?;
            database
                .commit_transaction()
                .map(|output| Some(output_to_json(&output)))
                .map_err(|error| FfiError::open(&error))
        })
    }
}

/// Roll back the open transaction, discarding all of its statements.
/// Fails with `no_transaction` when no transaction is open.
///
/// # Safety
/// `db` must be a live [`super::hawdb_open`] handle; `err_out` must be NULL
/// or writable [`HawdbBuffer`] storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_rollback_transaction(
    db: *mut HawdbDatabase,
    err_out: *mut HawdbBuffer,
) -> bool {
    unsafe {
        with_database(db, std::ptr::null_mut(), err_out, |database| {
            ensure_open(database)?;
            database
                .rollback_transaction()
                .map_err(|error| FfiError::open(&error))?;
            Ok(None)
        })
    }
}
