// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

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
    HawDBError, QueryOutput, RecoveryMode, SqlQueryRequest, Uuid, Value,
};
use serde::ser::{Serialize, SerializeMap, Serializer};
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
    // `None` once closed. Read snapshots live beside the engine, so closing
    // releases every engine resource at once instead of waiting for MoonBit
    // to finalize outstanding transaction handles. MoonBit handles are not
    // shared across threads, so serializing snapshot reads costs nothing.
    engine: Mutex<Option<OpenEngine>>,
    closed: AtomicBool,
}

struct OpenEngine {
    db: HawDBEmbedded,
    readers: BTreeMap<u64, DatabaseReadTransaction>,
    next_reader: u64,
}

impl SharedDatabase {
    fn with_engine<T>(
        &self,
        run: impl FnOnce(&mut OpenEngine) -> Result<T, Failure>,
    ) -> Result<T, Failure> {
        let mut engine = self
            .engine
            .lock()
            .map_err(|_| fail("execution", "database mutex poisoned"))?;
        run(engine
            .as_mut()
            .ok_or_else(|| fail("closed", "database is closed"))?)
    }

    /// Rolls back an open write transaction, drops every read snapshot and
    /// the engine, and so releases the database for a later open. Waits for
    /// in-flight engine work first.
    fn close(&self) {
        let mut engine = self
            .engine
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut engine) = engine.take() {
            engine.readers.clear();
            if engine.db.transaction_active() {
                // Dropping the engine discards the transaction regardless.
                let _ = engine.db.rollback_transaction();
            }
        }
        self.closed.store(true, Ordering::Release);
    }
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

fn owned_database(db: HawDBEmbedded) -> Arc<SharedDatabase> {
    Arc::new(SharedDatabase {
        engine: Mutex::new(Some(OpenEngine {
            db,
            readers: BTreeMap::new(),
            next_reader: 0,
        })),
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
        shared.close();
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
                if let Some(v) = values.get("$float") {
                    return v
                        .as_str()
                        .and_then(non_finite_value)
                        .map(Value::Float)
                        .ok_or_else(|| {
                            fail(
                                "invalid_argument",
                                "$float must be \"NaN\", \"Infinity\" or \"-Infinity\"",
                            )
                        });
                }
                if let Some((tag, v)) = values.iter().next()
                    && hawdb::temporal::JSON_TAGS.contains(&tag.as_str())
                {
                    let text = v.as_str().ok_or_else(|| {
                        fail(
                            "invalid_argument",
                            format!("{tag} must be an ISO 8601 string"),
                        )
                    })?;
                    return hawdb::temporal::from_json_tag(tag, text)
                        .expect("checked temporal tag")
                        .map_err(|e| fail("invalid_argument", format!("invalid {tag}: {e}")));
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

/// Singleton-map keys that the transport reads as typed values. A user map
/// with exactly one such key travels as `{"$map": [[key, value]]}`.
fn is_reserved_tag(key: &str) -> bool {
    matches!(key, "$binary" | "$uuid" | "$vector" | "$map" | "$float")
        || hawdb::temporal::JSON_TAGS.contains(&key)
}

/// JSON has no NaN or infinity, so non-finite floats travel as
/// `{"$float": "NaN" | "Infinity" | "-Infinity"}` instead of as NULL.
fn non_finite_text(value: f64) -> &'static str {
    if value.is_nan() {
        "NaN"
    } else if value > 0.0 {
        "Infinity"
    } else {
        "-Infinity"
    }
}

fn non_finite_value(text: &str) -> Option<f64> {
    match text {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

/// Serializes one engine value in the tagged transport, streaming it
/// straight into the output buffer instead of building a JSON tree.
struct Tagged<'a>(&'a Value);

fn tagged_entry<S: Serializer>(
    serializer: S,
    tag: &str,
    payload: &(impl Serialize + ?Sized),
) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(1))?;
    map.serialize_entry(tag, payload)?;
    map.end()
}

struct Coordinates<'a>(&'a hawdb::Vector);

impl Serialize for Coordinates<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.as_ref().coordinates())
    }
}

struct EscapedPairs<'a>(&'a BTreeMap<String, Value>);

impl Serialize for EscapedPairs<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(|(key, value)| (key, Tagged(value))))
    }
}

impl Serialize for Tagged<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(v) => serializer.serialize_bool(*v),
            Value::Int(v) => serializer.serialize_i64(*v),
            Value::Float(v) if v.is_finite() => serializer.serialize_f64(*v),
            Value::Float(v) => tagged_entry(serializer, "$float", non_finite_text(*v)),
            Value::String(v) => serializer.serialize_str(v),
            Value::Binary(v) => tagged_entry(
                serializer,
                "$binary",
                &base64::engine::general_purpose::STANDARD.encode(v),
            ),
            Value::Uuid(v) => tagged_entry(serializer, "$uuid", &v.to_string()),
            Value::Vector(v) => tagged_entry(serializer, "$vector", &Coordinates(v)),
            value @ (Value::Date(_)
            | Value::LocalTime(_)
            | Value::LocalDateTime(_)
            | Value::DateTime(_)
            | Value::Duration(_)) => tagged_entry(
                serializer,
                hawdb::temporal::json_tag(value).expect("temporal value"),
                &value.to_string(),
            ),
            Value::List(values) => serializer.collect_seq(values.iter().map(Tagged)),
            Value::Map(values)
                if values.len() == 1
                    && values.keys().next().is_some_and(|key| is_reserved_tag(key)) =>
            {
                tagged_entry(serializer, "$map", &EscapedPairs(values))
            }
            Value::Map(values) => {
                serializer.collect_map(values.iter().map(|(key, value)| (key, Tagged(value))))
            }
        }
    }
}

struct Row<'a>(&'a [Value]);

impl Serialize for Row<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(Tagged))
    }
}

struct Rows<'a>(&'a QueryOutput);

impl Serialize for Rows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.value_rows().map(Row))
    }
}

/// The `{"columns": [...], "rows": [[...], ...]}` result envelope.
struct ResultEnvelope<'a>(&'a QueryOutput);

impl Serialize for ResultEnvelope<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("columns", self.0.schema().columns())?;
        map.serialize_entry("rows", &Rows(self.0))?;
        map.end()
    }
}

/// Encodes a result in one pass into the buffer that is then copied once
/// into MoonBit-owned bytes.
fn encode_result(result: &QueryOutput) -> Result<Vec<u8>, Failure> {
    serde_json::to_vec(&ResultEnvelope(result)).map_err(|e| fail("execution", e.to_string()))
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

/// # Safety
///
/// `path` must be readable for `len` bytes. MoonBit borrows it for the
/// duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moonbit_hawdb_open(
    path: *const u8,
    len: i32,
    read_only: i32,
) -> *mut Handle {
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

/// # Safety
///
/// `handle` must be null or a live database handle from this adapter.
/// MoonBit borrows it for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moonbit_hawdb_is_open(handle: *mut Handle) -> i32 {
    i32::from(clone_database(handle).is_some_and(|db| !db.closed.load(Ordering::Acquire)))
}

/// # Safety
///
/// `handle` must be null or a live database handle from this adapter.
/// MoonBit borrows it for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moonbit_hawdb_close(handle: *mut Handle) {
    let _ = catch_unwind(AssertUnwindSafe(|| close_db(handle)));
}

/// Runs one autocommit statement through the admitted facade. Both languages
/// refuse to run beside an open user transaction; the raw database's SQL
/// entry point would instead autocommit outside it.
fn execute_statement(
    engine: &mut HawDBEmbedded,
    statement: &str,
    params: &str,
    sql: bool,
) -> Result<Vec<u8>, Failure> {
    let answer = if sql {
        let args = sql_params(params)?;
        engine.query_sql_request_admitted(SqlQueryRequest::new(statement).with_params(&args))
    } else {
        engine.query_with_params_admitted(statement, &named_params(params)?)
    }
    .map_err(|e| query_error(&e))?;
    encode_result(&answer)
}

/// # Safety
///
/// `handle` must be null or a live database handle from this adapter, and
/// each byte pointer must be readable for its length. MoonBit borrows all
/// arguments for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moonbit_hawdb_execute(
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
        db.with_engine(|engine| execute_statement(&mut engine.db, statement, params, sql != 0))
    }));
    match result {
        Ok(Ok(bytes)) => to_moonbit_bytes(&bytes),
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

/// # Safety
///
/// `handle` must be null or a live database handle from this adapter.
/// MoonBit borrows it for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moonbit_hawdb_error(handle: *mut Handle) -> *mut u8 {
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

/// The adapter is versioned in lockstep with the workspace (Cargo.toml and
/// the Bazel `version` attribute), so this is also the engine's version.
#[unsafe(no_mangle)]
pub extern "C" fn moonbit_hawdb_version() -> *mut u8 {
    to_moonbit_bytes(concat!("hawdb ", env!("CARGO_PKG_VERSION")).as_bytes())
}

// Per-transaction mutex serializes mutations and finalization.
struct TransactionState {
    db: Option<Arc<SharedDatabase>>,
    // The read snapshot's key in `OpenEngine::readers`.
    reader: Option<u64>,
    active: bool,
    read_only: bool,
    error: String,
}

impl TransactionState {
    fn new(read_only: bool) -> Self {
        Self {
            db: None,
            reader: None,
            active: false,
            read_only,
            error: String::new(),
        }
    }
}

fn tx_begin_state(
    state: &mut TransactionState,
    shared: Arc<SharedDatabase>,
) -> Result<(), Failure> {
    let read_only = state.read_only;
    let reader = shared.with_engine(|engine| {
        if read_only {
            let snapshot = engine
                .db
                .begin_read_transaction()
                .map_err(|e| engine_error(&e))?;
            let id = engine.next_reader;
            engine.next_reader += 1;
            engine.readers.insert(id, snapshot);
            Ok(Some(id))
        } else {
            engine
                .db
                .begin_transaction()
                .map_err(|e| engine_error(&e))?;
            Ok(None)
        }
    })?;
    state.db = Some(shared);
    state.reader = reader;
    state.active = true;
    Ok(())
}

fn tx_query_state(
    state: &TransactionState,
    statement: &str,
    params: &str,
    sql: bool,
) -> Result<Vec<u8>, Failure> {
    if !state.active {
        return Err(fail("closed", "transaction is closed"));
    }
    let shared = state
        .db
        .as_ref()
        .ok_or_else(|| fail("closed", "transaction is closed"))?;
    let (positional, named) = if sql {
        (sql_params(params)?, BTreeMap::new())
    } else {
        (Vec::new(), named_params(params)?)
    };
    let output = shared.with_engine(|engine| {
        let output = match state.reader {
            Some(id) => {
                let read = engine
                    .readers
                    .get_mut(&id)
                    .ok_or_else(|| fail("closed", "read transaction is closed"))?;
                if sql {
                    read.query_sql_with_params(statement, &positional)
                } else {
                    read.query_with_params(statement, &named)
                }
            }
            None if sql => engine
                .db
                .transaction_query_sql_with_params(statement, &positional),
            None => engine.db.transaction_query_with_params(statement, &named),
        };
        output.map_err(|e| engine_error(&e))
    })?;
    encode_result(&output)
}

fn tx_finish_state(state: &mut TransactionState, commit: bool) -> Result<Vec<u8>, Failure> {
    if !state.active {
        return Err(fail("closed", "transaction is closed"));
    }
    let shared = state
        .db
        .clone()
        .ok_or_else(|| fail("closed", "transaction is closed"))?;
    if shared.closed.load(Ordering::Acquire) {
        tx_abandon_state(state)?;
        return Err(fail("closed", "database is closed"));
    }
    if state.read_only || !commit {
        tx_abandon_state(state)?;
        return Ok(br#"{"columns":[],"rows":[]}"#.to_vec());
    }
    state.active = false;
    state.db = None;
    let output = shared
        .with_engine(|engine| engine.db.commit_transaction().map_err(|e| engine_error(&e)))?;
    encode_result(&output)
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
    let active = std::mem::take(&mut state.active);
    let reader = state.reader.take();
    let Some(shared) = state.db.take() else {
        return Ok(());
    };
    let mut engine = shared
        .engine
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // A closed database already released the snapshot and the transaction.
    let Some(engine) = engine.as_mut() else {
        return Ok(());
    };
    if let Some(id) = reader {
        engine.readers.remove(&id);
    }
    if active && !state.read_only && engine.db.transaction_active() {
        engine
            .db
            .rollback_transaction()
            .map_err(|e| engine_error(&e))?;
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
                    state: Box::into_raw(Box::new(Mutex::new(TransactionState::new(read_only)))),
                },
            );
        }
    }
    tx
}

/// # Safety
///
/// `parent` must be null or a live database handle from this adapter.
/// MoonBit borrows it for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moonbit_hawdb_tx_begin(
    parent: *mut Handle,
    read_only: i32,
) -> *mut TransactionHandle {
    let tx = tx_alloc(read_only != 0);
    if tx.is_null() {
        return tx;
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let shared = clone_database(parent).ok_or_else(|| fail("closed", "database is closed"))?;
        let mut state = unsafe { &*(*tx).state }
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        tx_begin_state(&mut state, shared)
    }));
    match outcome {
        Ok(Ok(())) => (),
        Ok(Err(err)) => tx_set_error(tx, err),
        Err(_) => tx_set_error(tx, fail("panic", "panic during transaction begin")),
    }
    tx
}

/// # Safety
///
/// `tx` must be null or a live transaction handle from this adapter.
/// MoonBit borrows it for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moonbit_hawdb_tx_is_open(tx: *mut TransactionHandle) -> i32 {
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

/// # Safety
///
/// `tx` must be null or a live transaction handle from this adapter.
/// MoonBit borrows it for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moonbit_hawdb_tx_error(tx: *mut TransactionHandle) -> *mut u8 {
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

/// # Safety
///
/// `tx` must be null or a live transaction handle from this adapter, and
/// each byte pointer must be readable for its length. MoonBit borrows all
/// arguments for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moonbit_hawdb_tx_query(
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
        tx_query_state(&state, stmt, args, is_sql != 0)
    }));
    match outcome {
        Ok(Ok(bytes)) => to_moonbit_bytes(&bytes),
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

/// # Safety
///
/// `tx` must be null or a live transaction handle from this adapter.
/// MoonBit borrows it for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moonbit_hawdb_tx_finish(
    tx: *mut TransactionHandle,
    commit: i32,
) -> *mut u8 {
    if tx.is_null() {
        return to_moonbit_bytes(&[]);
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let mut state = unsafe { &*(*tx).state }
            .lock()
            .map_err(|_| fail("execution", "transaction mutex poisoned"))?;
        state.error.clear();
        tx_finish_state(&mut state, commit != 0)
    }));
    match outcome {
        Ok(Ok(bytes)) => to_moonbit_bytes(&bytes),
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

/// # Safety
///
/// `tx` must be null or a live transaction handle from this adapter.
/// MoonBit borrows it for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn moonbit_hawdb_tx_close(tx: *mut TransactionHandle) {
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
        let mut state = TransactionState::new(false);
        tx_begin_state(&mut state, Arc::clone(&shared)).unwrap();
        tx_abandon_state(&mut state).unwrap();
        tx_abandon_state(&mut state).unwrap();
        assert!(!state.active);
        assert!(state.db.is_none());
        assert!(!shared
            .with_engine(|engine| Ok(engine.db.transaction_active()))
            .unwrap());
    }

    fn encoded(value: &Value) -> Json {
        serde_json::to_value(Tagged(value)).unwrap()
    }

    fn rows(bytes: Vec<u8>) -> String {
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn autocommit_statements_refuse_to_run_beside_an_open_transaction() {
        let mut engine = HawDBEmbedded::open_in_memory();
        execute_statement(
            &mut engine,
            "CREATE TABLE items (id BIGINT PRIMARY KEY)",
            "[]",
            true,
        )
        .unwrap();
        engine.begin_transaction().unwrap();
        for (statement, params, sql) in [
            ("INSERT INTO items VALUES ($1)", "[1]", true),
            ("CREATE (:Item {id: $id})", r#"{"id":1}"#, false),
        ] {
            let (_, message) = execute_statement(&mut engine, statement, params, sql).unwrap_err();
            assert!(message.contains("transaction is open"), "{message}");
        }
        engine.rollback_transaction().unwrap();
        let output = execute_statement(&mut engine, "SELECT id FROM items", "[]", true).unwrap();
        assert_eq!(rows(output), r#"{"columns":["id"],"rows":[]}"#);
    }

    #[test]
    fn close_releases_the_database_and_its_open_transactions() {
        let path = std::env::temp_dir().join(format!(
            "hawdb-moonbit-close-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let open = || HawDBEmbedded::open_with_options(open_options(path.clone(), false));
        let shared = owned_database(open().unwrap());
        shared
            .with_engine(|engine| {
                execute_statement(
                    &mut engine.db,
                    "CREATE TABLE items (id BIGINT PRIMARY KEY)",
                    "[]",
                    true,
                )
            })
            .unwrap();
        let mut reader = TransactionState::new(true);
        tx_begin_state(&mut reader, Arc::clone(&shared)).unwrap();
        let mut writer = TransactionState::new(false);
        tx_begin_state(&mut writer, Arc::clone(&shared)).unwrap();
        tx_query_state(&writer, "INSERT INTO items (id) VALUES ($1)", "[1]", true).unwrap();

        // Both transaction handles stay reachable, as unfinalized MoonBit
        // objects would; closing must not wait for them.
        shared.close();
        for state in [&reader, &writer] {
            let (kind, _) = tx_query_state(state, "SELECT id FROM items", "[]", true).unwrap_err();
            assert_eq!(kind, "closed");
        }
        let reopened = owned_database(open().unwrap());
        let output = reopened
            .with_engine(|engine| {
                execute_statement(&mut engine.db, "SELECT id FROM items", "[]", true)
            })
            .unwrap();
        assert_eq!(rows(output), r#"{"columns":["id"],"rows":[]}"#);

        let (kind, _) = tx_finish_state(&mut writer, true).unwrap_err();
        assert_eq!(kind, "closed");
        tx_abandon_state(&mut reader).unwrap();
        assert!(!writer.active && !reader.active);
        reopened.close();
        std::fs::remove_dir_all(&path).unwrap();
    }

    #[test]
    fn read_snapshots_are_released_when_their_transaction_finishes() {
        let shared = owned_database(HawDBEmbedded::open_in_memory());
        let mut reader = TransactionState::new(true);
        tx_begin_state(&mut reader, Arc::clone(&shared)).unwrap();
        let output = tx_query_state(&reader, "UNWIND [1] AS one RETURN one", "{}", false).unwrap();
        assert_eq!(rows(output), r#"{"columns":["one"],"rows":[[1]]}"#);
        tx_finish_state(&mut reader, false).unwrap();
        let readers = shared
            .with_engine(|engine| Ok(engine.readers.len()))
            .unwrap();
        assert_eq!(readers, 0);
    }

    #[test]
    fn non_finite_floats_are_tagged_instead_of_null() {
        for (value, text) in [
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            let json = encoded(&Value::Float(value));
            assert_eq!(json, json!({ "$float": text }));
            let Value::Float(decoded) = decode_value(&json).unwrap() else {
                panic!("{text} did not decode as a float");
            };
            assert_eq!(decoded.to_bits(), value.to_bits(), "{text}");
        }
        assert_eq!(encoded(&Value::Float(1.5)), json!(1.5));
        let map = Value::Map(BTreeMap::from([(
            "$float".to_owned(),
            Value::String("NaN".to_owned()),
        )]));
        assert_eq!(decode_value(&encoded(&map)).unwrap(), map);
        let (kind, _) = decode_value(&json!({"$float": "1.5"})).unwrap_err();
        assert_eq!(kind, "invalid_argument");
    }

    #[test]
    fn streamed_results_keep_the_tagged_transport() {
        let mut engine = HawDBEmbedded::open_in_memory();
        let output = execute_statement(
            &mut engine,
            "UNWIND [1] AS row RETURN $binary AS binary, $uuid AS uuid, $vector AS vector, $nested AS nested",
            r#"{
                "binary": {"$binary": "AP8="},
                "uuid": {"$uuid": "550e8400-e29b-41d4-a716-446655440000"},
                "vector": {"$vector": [1.5, -0.25]},
                "nested": {"$map": [["$uuid", [1, "x", null, true]]]}
            }"#,
            false,
        )
        .unwrap();
        assert_eq!(
            rows(output),
            concat!(
                r#"{"columns":["binary","uuid","vector","nested"],"rows":[["#,
                r#"{"$binary":"AP8="},"#,
                r#"{"$uuid":"550e8400-e29b-41d4-a716-446655440000"},"#,
                r#"{"$vector":[1.5,-0.25]},"#,
                r#"{"$map":[["$uuid",[1,"x",null,true]]]}]]}"#
            )
        );
    }

    #[test]
    fn temporal_values_use_the_shared_tagged_json() {
        for (tagged, text) in [
            ("$date", "2026-10-30"),
            ("$localtime", "12:30:00.500"),
            ("$localdatetime", "2026-10-30T12:30:00"),
            ("$datetime", "2026-10-30T12:00:00+02:00"),
            ("$duration", "P1M2DT3.5S"),
        ] {
            let json = json!({ tagged: text });
            let value = decode_value(&json).unwrap();
            assert_eq!(hawdb::temporal::json_tag(&value), Some(tagged));
            assert_eq!(encoded(&value), json);
            // A plain map with a temporal key must not turn into a value.
            let map = Value::Map(BTreeMap::from([(
                tagged.to_owned(),
                Value::String(text.to_owned()),
            )]));
            assert_eq!(decode_value(&encoded(&map)).unwrap(), map);
        }
        for invalid in [
            json!({"$date": "2026-02-30"}),
            json!({"$datetime": 7}),
            json!({"$duration": "P"}),
        ] {
            let (kind, _) = decode_value(&invalid).unwrap_err();
            assert_eq!(kind, "invalid_argument", "{invalid}");
        }
    }
}
