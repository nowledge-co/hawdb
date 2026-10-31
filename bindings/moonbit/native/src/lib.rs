// Copyright 2026 Nowledge contributors
// SPDX-License-Identifier: Apache-2.0
//
// First-class MoonBit native adapter. Exports the C calling convention
// directly from Rust and depends only on the HawDB engine crate.
// MoonBit allocations use the native runtime contract in moonbit.h.

use std::collections::BTreeMap;
use std::ffi::{c_int, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::ptr;
use std::slice;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use base64::Engine;
use hawdb::{
    DatabaseReadTransaction, EmbeddedQueryError, HawDBEmbedded, HawDBEmbeddedOpenOptions,
    HawDBError, QueryOutput, RecoveryMode, Uuid, Value,
};
use serde_json::{json, Value as Json};

unsafe extern "C" {
    fn moonbit_make_bytes(length: i32, init: c_int) -> *mut u8;
    fn moonbit_make_external_object(
        finalize: Option<extern "C" fn(*mut c_void)>,
        payload_size: u32,
    ) -> *mut c_void;
}

// The Rust mutex protects each externally owned MoonBit handle's state.
// A borrowed FFI parameter pins the payload through the native call.
struct SharedDatabase {
    engine: Mutex<HawDBEmbedded>,
    closed: AtomicBool,
}

struct HandleState {
    db: Option<Arc<SharedDatabase>>,
    error: String,
}

#[repr(C)]
pub struct Handle {
    // Keep alignment-sensitive Mutex off the MoonBit GC payload.
    state: *mut Mutex<HandleState>,
}

fn owned_database(engine: HawDBEmbedded) -> Arc<SharedDatabase> {
    Arc::new(SharedDatabase {
        engine: Mutex::new(engine),
        closed: AtomicBool::new(false),
    })
}

fn clone_database(handle: *mut Handle) -> Option<Arc<SharedDatabase>> {
    if handle.is_null() {
        return None;
    }
    let state = unsafe { &*(*handle).state }
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.db.clone()
}

type Failure = (String, String);

fn fail(kind: &str, message: impl Into<String>) -> Failure {
    (kind.to_owned(), message.into())
}

fn variant_name(value: &impl std::fmt::Debug) -> String {
    let value = format!("{value:?}");
    let end = value.find(['(', ' ', '{']).unwrap_or(value.len());
    let mut result = String::new();
    for (i, ch) in value[..end].chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_ascii_lowercase());
        } else {
            result.push(ch);
        }
    }
    result
}

fn engine_error(e: &HawDBError) -> Failure {
    fail(&variant_name(e), e.to_string())
}

fn query_error(e: &EmbeddedQueryError) -> Failure {
    let kind = match e {
        EmbeddedQueryError::Database(inner) => variant_name(inner),
        EmbeddedQueryError::Admission(_) => "admission".to_owned(),
        EmbeddedQueryError::Stopped(_) => "stopped".to_owned(),
    };
    (kind, e.to_string())
}

fn error_json((kind, message): Failure) -> String {
    json!({"kind":kind,"message":message}).to_string()
}

fn clear_error(handle: *mut Handle) {
    if !handle.is_null() {
        let mut state = unsafe { &*(*handle).state }
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.error.clear();
    }
}

fn save_error(handle: *mut Handle, message: String) {
    if !handle.is_null() {
        let mut state = unsafe { &*(*handle).state }
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.error = message;
    }
}

fn close_db(handle: *mut Handle) {
    if handle.is_null() {
        return;
    }
    let mut state = unsafe { &*(*handle).state }
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(shared) = state.db.take() {
        // Wait for in-flight engine work before marking the handle closed.
        let _engine = shared
            .engine
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        shared.closed.store(true, Ordering::Release);
    }
}

extern "C" fn finalize(payload: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let handle = payload.cast::<Handle>();
        close_db(handle);
        // External payload memory belongs to MoonBit; Rust must drop fields.
        unsafe {
            let state = (*handle).state;
            (*handle).state = ptr::null_mut();
            if !state.is_null() {
                drop(Box::from_raw(state));
            }
        }
    }));
}

fn allocate_handle() -> *mut Handle {
    let ptr = unsafe {
        moonbit_make_external_object(Some(finalize), std::mem::size_of::<Handle>() as u32)
    }
    .cast::<Handle>();
    if !ptr.is_null() {
        unsafe {
            ptr::write(
                ptr,
                Handle {
                    state: Box::into_raw(Box::new(Mutex::new(HandleState {
                        db: None,
                        error: String::new(),
                    }))),
                },
            );
        };
    }
    ptr
}

fn to_moonbit_bytes(data: &[u8]) -> *mut u8 {
    let Ok(len) = i32::try_from(data.len()) else {
        return unsafe { moonbit_make_bytes(0, 0) };
    };
    let out = unsafe { moonbit_make_bytes(len, 0) };
    if !out.is_null() && len > 0 {
        unsafe { ptr::copy_nonoverlapping(data.as_ptr(), out, data.len()) };
    }
    out
}

unsafe fn input<'a>(p: *const u8, len: i32, what: &str) -> Result<&'a str, Failure> {
    if len < 0 || p.is_null() {
        return Err(fail(
            "invalid_argument",
            format!("{what} has an invalid pointer or length"),
        ));
    }
    let bytes = unsafe { slice::from_raw_parts(p, len as usize) };
    std::str::from_utf8(bytes)
        .map_err(|_| fail("invalid_argument", format!("{what} must be valid UTF-8")))
}

fn decode_value(value: &Json) -> Result<Value, Failure> {
    Ok(match value {
        Json::Null => Value::Null,
        Json::Bool(v) => Value::Bool(*v),
        Json::Number(v) => {
            if let Some(i) = v.as_i64() {
                Value::Int(i)
            } else if let Some(f) = v.as_f64() {
                Value::Float(f)
            } else {
                return Err(fail("invalid_argument", "number out of range"));
            }
        }
        Json::String(v) => Value::String(v.clone()),
        Json::Array(values) => {
            Value::List(values.iter().map(decode_value).collect::<Result<_, _>>()?)
        }
        Json::Object(values) => {
            if values.len() == 1 {
                if let Some(v) = values.get("$map") {
                    let entries = v
                        .as_array()
                        .ok_or_else(|| fail("invalid_argument", "$map must be an array"))?;
                    let mut map = BTreeMap::new();
                    for entry in entries {
                        let pair = entry
                            .as_array()
                            .filter(|p| p.len() == 2)
                            .ok_or_else(|| fail("invalid_argument", "$map entry must be a pair"))?;
                        let key = pair[0]
                            .as_str()
                            .ok_or_else(|| fail("invalid_argument", "$map key must be a string"))?;
                        if map
                            .insert(key.to_owned(), decode_value(&pair[1])?)
                            .is_some()
                        {
                            return Err(fail("invalid_argument", "duplicate $map key"));
                        }
                    }
                    return Ok(Value::Map(map));
                }
                if let Some(v) = values.get("$vector") {
                    let numbers = v
                        .as_array()
                        .ok_or_else(|| fail("invalid_argument", "$vector must be an array"))?
                        .iter()
                        .map(|v| {
                            let x = v.as_f64().ok_or_else(|| {
                                fail("invalid_argument", "$vector requires numbers")
                            })?;
                            let coordinate = x as f32;
                            if !coordinate.is_finite() {
                                return Err(fail(
                                    "invalid_argument",
                                    "$vector coordinate is outside finite f32 range",
                                ));
                            }
                            Ok(coordinate)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    return hawdb::Vector::new(&numbers)
                        .map(Value::Vector)
                        .map_err(|e| fail("invalid_argument", e.to_string()));
                }
                if let Some(v) = values.get("$binary") {
                    let bytes = v
                        .as_str()
                        .ok_or_else(|| fail("invalid_argument", "$binary must be a string"))?;
                    return base64::engine::general_purpose::STANDARD
                        .decode(bytes)
                        .map(Value::Binary)
                        .map_err(|e| fail("invalid_argument", e.to_string()));
                }
                if let Some(v) = values.get("$uuid") {
                    let uuid = v
                        .as_str()
                        .ok_or_else(|| fail("invalid_argument", "$uuid must be a string"))?;
                    return uuid
                        .parse::<Uuid>()
                        .map(Value::Uuid)
                        .map_err(|e| fail("invalid_argument", e.to_string()));
                }
            }
            Value::Map(
                values
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), decode_value(value)?)))
                    .collect::<Result<BTreeMap<_, _>, Failure>>()?,
            )
        }
    })
}

fn encode_value(value: &Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Bool(v) => json!(v),
        Value::Int(v) => json!(v),
        Value::Float(v) if v.is_finite() => json!(v),
        Value::Float(_) => Json::Null,
        Value::String(v) => json!(v),
        Value::Binary(v) => json!({"$binary":base64::engine::general_purpose::STANDARD.encode(v)}),
        Value::Uuid(v) => json!({"$uuid":v.to_string()}),
        Value::Vector(v) => json!({"$vector":v.as_ref().coordinates().collect::<Vec<_>>()}),
        Value::List(v) => Json::Array(v.iter().map(encode_value).collect()),
        Value::Map(v) => {
            let reserved = v.len() == 1
                && v.keys().next().is_some_and(|key| {
                    matches!(key.as_str(), "$binary" | "$uuid" | "$vector" | "$map")
                });
            if reserved {
                let pairs: Vec<Json> = v
                    .iter()
                    .map(|(key, value)| {
                        Json::Array(vec![Json::String(key.clone()), encode_value(value)])
                    })
                    .collect();
                json!({"$map": pairs})
            } else {
                Json::Object(
                    v.iter()
                        .map(|(key, value)| (key.clone(), encode_value(value)))
                        .collect(),
                )
            }
        }
    }
}

fn encode_result(result: &QueryOutput) -> String {
    let rows: Vec<_> = result
        .value_rows()
        .map(|row| Json::Array(row.iter().map(encode_value).collect()))
        .collect();
    json!({"columns":result.schema().columns(),"rows":rows}).to_string()
}

fn named_params(data: &str) -> Result<BTreeMap<String, Value>, Failure> {
    let parsed: Json =
        serde_json::from_str(data).map_err(|e| fail("invalid_argument", e.to_string()))?;
    let obj = parsed
        .as_object()
        .ok_or_else(|| fail("invalid_argument", "Cypher params must be an object"))?;
    obj.iter()
        .map(|(key, val)| Ok((key.clone(), decode_value(val)?)))
        .collect()
}

fn sql_params(data: &str) -> Result<Vec<Value>, Failure> {
    let parsed: Json =
        serde_json::from_str(data).map_err(|e| fail("invalid_argument", e.to_string()))?;
    let arr = parsed
        .as_array()
        .ok_or_else(|| fail("invalid_argument", "SQL params must be an array"))?;
    arr.iter().map(decode_value).collect()
}

// Opening read-only must select strict WAL replay. HawDB explicitly
// rejects AutoRepairTornTail for read-only open, even on a healthy database.
fn open_options(path: PathBuf, read_only: bool) -> HawDBEmbeddedOpenOptions {
    let mut options = HawDBEmbeddedOpenOptions::new(path);
    options.config.read_only = read_only;
    if read_only {
        options.config.recovery_mode = RecoveryMode::Strict;
    }
    options
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_open(path: *const u8, len: i32, read_only: i32) -> *mut Handle {
    let handle = allocate_handle();
    if handle.is_null() {
        return handle;
    }
    let opened = catch_unwind(AssertUnwindSafe(|| {
        let path = unsafe { input(path, len, "path") }?;
        let options = open_options(PathBuf::from(path), read_only != 0);
        HawDBEmbedded::open_with_options(options).map_err(|e| engine_error(&e))
    }));
    match opened {
        Ok(Ok(db)) => {
            let mut state = unsafe { &*(*handle).state }
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.db = Some(owned_database(db));
        }
        Ok(Err(e)) => save_error(handle, error_json(e)),
        Err(_) => save_error(handle, error_json(fail("panic", "panic during open"))),
    }
    handle
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_open_in_memory() -> *mut Handle {
    let handle = allocate_handle();
    if handle.is_null() {
        return handle;
    }
    match catch_unwind(AssertUnwindSafe(HawDBEmbedded::open_in_memory)) {
        Ok(db) => {
            let mut state = unsafe { &*(*handle).state }
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.db = Some(owned_database(db));
        }
        Err(_) => save_error(
            handle,
            error_json(fail("panic", "panic during open_in_memory")),
        ),
    }
    handle
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_is_open(handle: *mut Handle) -> i32 {
    i32::from(clone_database(handle).is_some_and(|db| !db.closed.load(Ordering::Acquire)))
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_close(handle: *mut Handle) {
    let _ = catch_unwind(AssertUnwindSafe(|| close_db(handle)));
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_execute(
    handle: *mut Handle,
    text: *const u8,
    text_len: i32,
    params: *const u8,
    params_len: i32,
    sql: i32,
) -> *mut u8 {
    let Some(db) = clone_database(handle) else {
        return to_moonbit_bytes(&[]);
    };
    clear_error(handle);
    let result = catch_unwind(AssertUnwindSafe(|| {
        let statement = unsafe { input(text, text_len, "statement") }?;
        let params = unsafe { input(params, params_len, "params") }?;
        let mut locked = db
            .engine
            .lock()
            .map_err(|_| fail("execution", "database mutex poisoned"))?;
        if db.closed.load(Ordering::Acquire) {
            return Err(fail("closed", "database is closed"));
        }
        if sql != 0 {
            let args = sql_params(params)?;
            let answer = locked
                .database_mut()
                .query_sql_with_params(statement, &args)
                .map_err(|e| engine_error(&e))?;
            Ok::<_, Failure>(encode_result(&answer))
        } else {
            let args = named_params(params)?;
            let answer = locked
                .query_with_params_admitted(statement, &args)
                .map_err(|e| query_error(&e))?;
            Ok::<_, Failure>(encode_result(&answer))
        }
    }));
    match result {
        Ok(Ok(text)) => to_moonbit_bytes(text.as_bytes()),
        Ok(Err(e)) => {
            save_error(handle, error_json(e));
            to_moonbit_bytes(&[])
        }
        Err(_) => {
            save_error(handle, error_json(fail("panic", "panic during execute")));
            to_moonbit_bytes(&[])
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_error(handle: *mut Handle) -> *mut u8 {
    let message = if handle.is_null() {
        error_json(fail("closed", "HawDB handle is closed"))
    } else {
        let state = unsafe { &*(*handle).state }
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.error.is_empty() {
            error_json(fail("closed", "HawDB handle is closed"))
        } else {
            state.error.clone()
        }
    };
    to_moonbit_bytes(message.as_bytes())
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_version() -> *mut u8 {
    to_moonbit_bytes(concat!("hawdb ", env!("CARGO_PKG_VERSION")).as_bytes())
}

// Per-transaction mutex serializes mutations and finalization.
struct TransactionState {
    db: Option<Arc<SharedDatabase>>,
    read: Option<DatabaseReadTransaction>,
    active: bool,
    read_only: bool,
    error: String,
}

#[repr(C)]
pub struct TransactionHandle {
    state: *mut Mutex<TransactionState>,
}

fn tx_set_error(tx: *mut TransactionHandle, value: Failure) {
    if !tx.is_null() {
        let mut state = unsafe { &*(*tx).state }
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.error = error_json(value);
    }
}

fn tx_abandon_state(state: &mut TransactionState) -> Result<(), Failure> {
    let active = state.active;
    state.active = false;
    state.read.take();
    let db = state.db.take();
    if active && !state.read_only {
        if let Some(shared) = db {
            let mut engine = shared
                .engine
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if engine.transaction_active() {
                engine
                    .rollback_transaction()
                    .map_err(|e| engine_error(&e))?;
            }
        }
    }
    Ok(())
}

fn tx_abandon(tx: *mut TransactionHandle) -> Result<(), Failure> {
    if tx.is_null() {
        return Ok(());
    }
    let mut state = unsafe { &*(*tx).state }
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    tx_abandon_state(&mut state)
}

extern "C" fn tx_finalize(payload: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let tx = payload.cast::<TransactionHandle>();
        let _ = tx_abandon(tx);
        unsafe {
            let state = (*tx).state;
            (*tx).state = ptr::null_mut();
            if !state.is_null() {
                drop(Box::from_raw(state));
            }
        }
    }));
}

fn tx_alloc(read_only: bool) -> *mut TransactionHandle {
    let tx = unsafe {
        moonbit_make_external_object(
            Some(tx_finalize),
            std::mem::size_of::<TransactionHandle>() as u32,
        )
    }
    .cast::<TransactionHandle>();
    if !tx.is_null() {
        unsafe {
            ptr::write(
                tx,
                TransactionHandle {
                    state: Box::into_raw(Box::new(Mutex::new(TransactionState {
                        db: None,
                        read: None,
                        active: false,
                        read_only,
                        error: String::new(),
                    }))),
                },
            );
        }
    }
    tx
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_tx_begin(
    parent: *mut Handle,
    read_only: i32,
) -> *mut TransactionHandle {
    let tx = tx_alloc(read_only != 0);
    if tx.is_null() {
        return tx;
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let shared = clone_database(parent).ok_or_else(|| fail("closed", "database is closed"))?;
        let mut locked = shared
            .engine
            .lock()
            .map_err(|_| fail("execution", "database mutex poisoned"))?;
        if shared.closed.load(Ordering::Acquire) {
            return Err(fail("closed", "database is closed"));
        }
        let read = if read_only != 0 {
            Some(
                locked
                    .begin_read_transaction()
                    .map_err(|e| engine_error(&e))?,
            )
        } else {
            locked.begin_transaction().map_err(|e| engine_error(&e))?;
            None
        };
        let mut state = unsafe { &*(*tx).state }
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.db = Some(shared.clone());
        state.read = read;
        state.active = true;
        Ok(())
    }));
    match outcome {
        Ok(Ok(())) => (),
        Ok(Err(err)) => tx_set_error(tx, err),
        Err(_) => tx_set_error(tx, fail("panic", "panic during transaction begin")),
    }
    tx
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_tx_is_open(tx: *mut TransactionHandle) -> i32 {
    if tx.is_null() {
        return 0;
    }
    let state = unsafe { &*(*tx).state }
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    i32::from(
        state.active
            && state
                .db
                .as_ref()
                .is_some_and(|db| !db.closed.load(Ordering::Acquire)),
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_tx_error(tx: *mut TransactionHandle) -> *mut u8 {
    let message = if tx.is_null() {
        error_json(fail("closed", "transaction is closed"))
    } else {
        let state = unsafe { &*(*tx).state }
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.error.is_empty() {
            error_json(fail("closed", "transaction is closed"))
        } else {
            state.error.clone()
        }
    };
    to_moonbit_bytes(message.as_bytes())
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_tx_query(
    tx: *mut TransactionHandle,
    statement: *const u8,
    statement_len: i32,
    params: *const u8,
    params_len: i32,
    is_sql: i32,
) -> *mut u8 {
    if tx.is_null() {
        return to_moonbit_bytes(&[]);
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let stmt = unsafe { input(statement, statement_len, "statement") }?;
        let args = unsafe { input(params, params_len, "params") }?;
        let mut state = unsafe { &*(*tx).state }
            .lock()
            .map_err(|_| fail("execution", "transaction mutex poisoned"))?;
        state.error.clear();
        if !state.active {
            return Err(fail("closed", "transaction is closed"));
        }
        let shared = state
            .db
            .as_ref()
            .cloned()
            .ok_or_else(|| fail("closed", "transaction is closed"))?;
        if shared.closed.load(Ordering::Acquire) {
            return Err(fail("closed", "database is closed"));
        }
        if state.read_only {
            let read = state
                .read
                .as_mut()
                .ok_or_else(|| fail("closed", "read transaction is closed"))?;
            let result = if is_sql != 0 {
                read.query_sql_with_params(stmt, &sql_params(args)?)
            } else {
                read.query_with_params(stmt, &named_params(args)?)
            }
            .map_err(|e| engine_error(&e))?;
            Ok::<String, Failure>(encode_result(&result))
        } else {
            let mut engine = shared
                .engine
                .lock()
                .map_err(|_| fail("execution", "database mutex poisoned"))?;
            if shared.closed.load(Ordering::Acquire) {
                return Err(fail("closed", "database is closed"));
            }
            let result = if is_sql != 0 {
                engine.transaction_query_sql_with_params(stmt, &sql_params(args)?)
            } else {
                engine.transaction_query_with_params(stmt, &named_params(args)?)
            }
            .map_err(|e| engine_error(&e))?;
            Ok::<String, Failure>(encode_result(&result))
        }
    }));
    match outcome {
        Ok(Ok(json)) => to_moonbit_bytes(json.as_bytes()),
        Ok(Err(err)) => {
            tx_set_error(tx, err);
            to_moonbit_bytes(&[])
        }
        Err(_) => {
            tx_set_error(tx, fail("panic", "panic during transaction query"));
            to_moonbit_bytes(&[])
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_tx_finish(tx: *mut TransactionHandle, commit: i32) -> *mut u8 {
    if tx.is_null() {
        return to_moonbit_bytes(&[]);
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let mut state = unsafe { &*(*tx).state }
            .lock()
            .map_err(|_| fail("execution", "transaction mutex poisoned"))?;
        state.error.clear();
        if !state.active {
            return Err(fail("closed", "transaction is closed"));
        }
        let shared = state
            .db
            .as_ref()
            .cloned()
            .ok_or_else(|| fail("closed", "transaction is closed"))?;
        if shared.closed.load(Ordering::Acquire) {
            tx_abandon_state(&mut state)?;
            return Err(fail("closed", "database is closed"));
        }
        if state.read_only || commit == 0 {
            tx_abandon_state(&mut state)?;
            return Ok::<String, Failure>(r#"{"columns":[],"rows":[]}"#.to_owned());
        }
        let mut engine = shared
            .engine
            .lock()
            .map_err(|_| fail("execution", "database mutex poisoned"))?;
        if shared.closed.load(Ordering::Acquire) {
            if engine.transaction_active() {
                engine
                    .rollback_transaction()
                    .map_err(|e| engine_error(&e))?;
            }
            state.active = false;
            state.db.take();
            return Err(fail("closed", "database is closed"));
        }
        let result = engine.commit_transaction().map_err(|e| engine_error(&e));
        state.active = false;
        state.db.take();
        result.map(|value| encode_result(&value))
    }));
    match outcome {
        Ok(Ok(json)) => to_moonbit_bytes(json.as_bytes()),
        Ok(Err(err)) => {
            tx_set_error(tx, err);
            to_moonbit_bytes(&[])
        }
        Err(_) => {
            let _ = tx_abandon(tx);
            tx_set_error(tx, fail("panic", "panic during transaction finish"));
            to_moonbit_bytes(&[])
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_tx_close(tx: *mut TransactionHandle) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Err(err) = tx_abandon(tx) {
            tx_set_error(tx, err);
        }
    }));
}

// The following tests exercise the Rust-side locking without calling any
// MoonBit allocation function. The MoonBit runtime owns external payloads,
// while these test-owned payloads remain alive until all workers have joined.
#[cfg(test)]
mod lifecycle_tests {
    // The unit-test executable does not link the MoonBit runtime. These
    // deliberate test-only stubs satisfy native references which are never
    // invoked by the pure-Rust lifecycle tests.
    #[unsafe(no_mangle)]
    extern "C" fn moonbit_make_bytes(_: i32, _: std::ffi::c_int) -> *mut u8 {
        panic!("MoonBit allocation is unavailable in a Rust-only test")
    }

    #[unsafe(no_mangle)]
    extern "C" fn moonbit_make_external_object(
        _: Option<extern "C" fn(*mut std::ffi::c_void)>,
        _: u32,
    ) -> *mut std::ffi::c_void {
        panic!("MoonBit external objects are unavailable in a Rust-only test")
    }

    use super::*;

    #[test]
    fn readonly_open_requests_strict_wal_recovery() {
        let options = open_options(PathBuf::from("readonly-fixture"), true);
        assert!(options.config.read_only);
        assert_eq!(options.config.recovery_mode, RecoveryMode::Strict);

        let writable = open_options(PathBuf::from("writable-fixture"), false);
        assert!(!writable.config.read_only);
        assert_eq!(
            writable.config.recovery_mode,
            RecoveryMode::AutoRepairTornTail
        );
    }

    #[test]
    fn concurrent_close_and_clone_keep_owned_engine_alive() {
        let handle = Box::new(Handle {
            state: Box::into_raw(Box::new(Mutex::new(HandleState {
                db: Some(owned_database(HawDBEmbedded::open_in_memory())),
                error: String::new(),
            }))),
        });
        let address = Box::into_raw(handle) as usize;
        std::thread::scope(|scope| {
            for worker in 0..12 {
                scope.spawn(move || {
                    let handle = address as *mut Handle;
                    for iteration in 0..100 {
                        if worker == 0 && iteration == 50 {
                            close_db(handle);
                        } else if let Some(shared) = clone_database(handle) {
                            let _guard = shared
                                .engine
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            // The clone prevents destruction across close.
                            let _ = shared.closed.load(Ordering::Acquire);
                        }
                    }
                });
            }
        });
        let handle = unsafe { Box::from_raw(address as *mut Handle) };
        assert!(unsafe { &*handle.state }.lock().unwrap().db.is_none());
        unsafe { drop(Box::from_raw(handle.state)) };
    }

    #[test]
    fn dropped_write_scope_rolls_back_once() {
        let shared = owned_database(HawDBEmbedded::open_in_memory());
        shared.engine.lock().unwrap().begin_transaction().unwrap();
        let mut state = TransactionState {
            db: Some(Arc::clone(&shared)),
            read: None,
            active: true,
            read_only: false,
            error: String::new(),
        };
        tx_abandon_state(&mut state).unwrap();
        tx_abandon_state(&mut state).unwrap();
        assert!(!state.active);
        assert!(state.db.is_none());
        assert!(!shared.engine.lock().unwrap().transaction_active());
    }
}
