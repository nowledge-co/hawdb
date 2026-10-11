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

//! C ABI bindings for the HawDB embedded database.
//!
//! The surface is deliberately small: open a database, run one Cypher or SQL
//! statement per call, close. All strings cross the boundary as UTF-8
//! pointer-plus-length pairs; there is no NUL-termination contract, though
//! returned buffers are NUL-terminated as a courtesy for C callers.
//! Statement parameters and results are exchanged as JSON, so hosts never
//! need to know HawDB's internal value representation.
//!
//! Output convention: results and errors are delivered through caller-owned
//! [`HawdbBuffer`] out parameters, never a thread-local — callers such as Go
//! may hop OS threads between calls. Every buffer the library writes is
//! heap-allocated and must be released exactly once with
//! [`hawdb_buffer_free`]. On failure a query fills `err_out` with a JSON
//! object `{"kind": "<engine error kind>", "message": "<text>"}` carrying
//! the same error kind the engine reports; a failed statement leaves the
//! database handle usable. Passing NULL for an out parameter discards that
//! output.

use std::collections::BTreeMap;
use std::ffi::{c_char, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::ptr;
use std::sync::Mutex;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use hawdb::{
    EmbeddedQueryError, HawDBEmbedded, HawDBEmbeddedOpenOptions, HawDBError, QueryOutput, Uuid,
    Value,
};
use serde_json::json;

mod retained;
pub use retained::*;

#[cfg(feature = "boundary-profiling")]
#[path = "../../benchmarks/native_profile.rs"]
mod boundary_profile;
#[cfg(feature = "boundary-profiling")]
pub use boundary_profile::BoundaryProfileSnapshot;

/// Developer profiling build only. This observation is not an admission ledger
/// or an allocator-capacity bound. NULL output is a no-op; no buffer is allocated.
///
/// # Safety
/// `out` must be NULL or point to writable `BoundaryProfileSnapshot` storage.
#[cfg(feature = "boundary-profiling")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_boundary_profile_snapshot(out: *mut BoundaryProfileSnapshot) {
    if !out.is_null() {
        unsafe { out.write(boundary_profile::snapshot()) };
    }
}

/// An open HawDB database handle. Opaque to C callers.
pub struct HawdbDatabase {
    inner: Mutex<HawDBEmbedded>,
}

/// A byte buffer the library hands to the caller.
///
/// `ptr` is `len` bytes of UTF-8 text; it is NUL-terminated as a courtesy
/// but `len` is authoritative and the content may not contain NUL bytes
/// anyway (JSON escapes them). The caller owns every non-NULL `ptr` and
/// releases it with [`hawdb_buffer_free`]. The library never retains or
/// reuses a buffer it has returned.
#[repr(C)]
pub struct HawdbBuffer {
    pub ptr: *mut c_char,
    pub len: usize,
}

/// An error reported by this crate: an engine or contract `kind` plus a
/// human-readable `message`.
struct FfiError {
    kind: String,
    message: String,
}

impl FfiError {
    /// A caller-side contract violation: NULL pointer, non-UTF-8 bytes,
    /// malformed JSON, wrong parameter container.
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: "invalid_argument".to_string(),
            message: message.into(),
        }
    }

    fn panic() -> Self {
        Self {
            kind: "panic".to_string(),
            message: "hawdb: panic while executing statement".to_string(),
        }
    }

    /// A query error carrying the engine's own kind: the `HawDBError`
    /// variant for database failures, `admission` or `stopped` otherwise.
    fn query(error: &EmbeddedQueryError) -> Self {
        let kind = match error {
            EmbeddedQueryError::Database(inner) => variant_name(inner),
            EmbeddedQueryError::Admission(_) => "admission".to_string(),
            EmbeddedQueryError::Stopped(_) => "stopped".to_string(),
        };
        Self {
            kind,
            message: error.to_string(),
        }
    }

    fn open(error: &HawDBError) -> Self {
        Self {
            kind: variant_name(error),
            message: error.to_string(),
        }
    }
}

/// Extract the enum variant name of an error as snake_case, e.g.
/// `CapabilityUnavailable` -> `capability_unavailable`. Derived from the
/// `Debug` representation so it stays in sync with the engine's taxonomy.
fn variant_name(error: &impl std::fmt::Debug) -> String {
    let text = format!("{error:?}");
    let end = text.find(['(', ' ', '{']).unwrap_or(text.len());
    let mut snake = String::with_capacity(end + 4);
    for (index, ch) in text[..end].chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                snake.push('_');
            }
            snake.push(ch.to_ascii_lowercase());
        } else {
            snake.push(ch);
        }
    }
    snake
}

/// Write `text` into `*out` as a heap-allocated buffer. A NULL `out`
/// discards the output. Interior NUL bytes are replaced so the buffer is
/// always NUL-terminated; `len` stays authoritative.
///
/// # Safety
/// `out` must be NULL or point to writable [`HawdbBuffer`] storage.
unsafe fn set_buffer(out: *mut HawdbBuffer, text: impl std::fmt::Display) {
    if out.is_null() {
        return;
    }
    let mut text = text.to_string();
    if text.contains('\0') {
        text = text.replace('\0', " ");
    }
    let len = text.len();
    match CString::new(text) {
        Ok(text) => {
            let buffer = unsafe { &mut *out };
            buffer.ptr = text.into_raw();
            buffer.len = len;
        }
        Err(_) => unsafe { clear_buffer(out) },
    }
}

/// Zero an out buffer so callers that read it unconditionally see {NULL, 0}.
///
/// # Safety
/// `out` must be NULL or point to writable [`HawdbBuffer`] storage.
unsafe fn clear_buffer(out: *mut HawdbBuffer) {
    if !out.is_null() {
        unsafe {
            (*out).ptr = ptr::null_mut();
            (*out).len = 0;
        }
    }
}

/// Deliver `error` through `err_out` as `{"kind", "message"}` JSON.
/// A NULL `err_out` discards it.
///
/// # Safety
/// `err_out` must be NULL or point to writable [`HawdbBuffer`] storage.
unsafe fn set_error(err_out: *mut HawdbBuffer, error: FfiError) {
    let payload = json!({ "kind": error.kind, "message": error.message });
    unsafe { set_buffer(err_out, payload) };
}

/// A borrowed `(ptr, len)` string argument at the FFI boundary.
/// A NULL `ptr` is only meaningful where the callee treats it as absent.
#[derive(Clone, Copy)]
struct FfiStr {
    ptr: *const c_char,
    len: usize,
}

/// Read a required UTF-8 string argument.
///
/// # Safety
/// `text.ptr` must be readable for `text.len` bytes, or NULL.
unsafe fn read_str<'a>(text: FfiStr, name: &str) -> Result<&'a str, FfiError> {
    if text.ptr.is_null() {
        return Err(FfiError::invalid(format!("hawdb: {name} must not be NULL")));
    }
    let bytes = unsafe { std::slice::from_raw_parts(text.ptr.cast::<u8>(), text.len) };
    std::str::from_utf8(bytes)
        .map_err(|_| FfiError::invalid(format!("hawdb: {name} is not valid UTF-8")))
}

/// Read an optional UTF-8 argument: a NULL pointer means absent.
///
/// # Safety
/// `text.ptr` must be readable for `text.len` bytes, or NULL.
unsafe fn read_opt_str<'a>(text: FfiStr, name: &str) -> Result<Option<&'a str>, FfiError> {
    if text.ptr.is_null() {
        return Ok(None);
    }
    unsafe { read_str(text, name) }.map(Some)
}

/// Convert a HawDB [`Value`] into `serde_json::Value`.
///
/// `Binary` becomes `{"$binary": "<base64>"}` and `Uuid` becomes
/// `{"$uuid": "<canonical>"}` so both stay distinguishable from plain
/// strings.
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(item) => json!(item),
        Value::Int(item) => json!(item),
        Value::Float(item) if item.is_finite() => json!(item),
        Value::Float(_) => serde_json::Value::Null,
        Value::String(item) => json!(item),
        Value::Binary(item) => json!({ "$binary": BASE64.encode(item) }),
        Value::Uuid(item) => json!({ "$uuid": item.to_string() }),
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Map(items) => serde_json::Value::Object(
            items
                .iter()
                .map(|(key, item)| (key.clone(), value_to_json(item)))
                .collect(),
        ),
    }
}

/// Convert a `serde_json::Value` into a HawDB [`Value`] for parameters.
///
/// This is the inverse of [`value_to_json`]: the single-key objects
/// `{"$binary": "<base64>"}` and `{"$uuid": "<canonical>"}` decode to
/// `Binary` and `Uuid`; every other object becomes a `Map`.
fn json_to_value(value: &serde_json::Value) -> Result<Value, String> {
    Ok(match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(item) => Value::Bool(*item),
        serde_json::Value::Number(item) => {
            if let Some(item) = item.as_i64() {
                Value::Int(item)
            } else if let Some(item) = item.as_f64() {
                Value::Float(item)
            } else {
                return Err("number is out of range".to_string());
            }
        }
        serde_json::Value::String(item) => Value::String(item.clone()),
        serde_json::Value::Array(items) => {
            Value::List(items.iter().map(json_to_value).collect::<Result<_, _>>()?)
        }
        serde_json::Value::Object(items) => {
            if items.len() == 1 {
                if let Some(encoded) = items.get("$binary").and_then(|v| v.as_str()) {
                    return BASE64
                        .decode(encoded)
                        .map(Value::Binary)
                        .map_err(|error| format!("invalid $binary parameter: {error}"));
                }
                if let Some(encoded) = items.get("$uuid").and_then(|v| v.as_str()) {
                    return encoded
                        .parse::<Uuid>()
                        .map(Value::Uuid)
                        .map_err(|error| format!("invalid $uuid parameter: {error}"));
                }
            }
            Value::Map(
                items
                    .iter()
                    .map(|(key, item)| Ok((key.clone(), json_to_value(item)?)))
                    .collect::<Result<BTreeMap<_, _>, String>>()?,
            )
        }
    })
}

/// Materialize a [`QueryOutput`] into the result envelope:
/// `{"columns": [...], "rows": [[...], ...]}`.
fn output_to_json(output: &QueryOutput) -> serde_json::Value {
    let columns = output.schema().columns();
    let rows: Vec<serde_json::Value> = output
        .value_rows()
        .map(|row| serde_json::Value::Array(row.iter().map(value_to_json).collect()))
        .collect();
    json!({ "columns": columns, "rows": rows })
}

/// Open a HawDB database at `path`, creating it if needed.
///
/// `path` is `path_len` UTF-8 bytes and must not be NULL. `options_json`
/// may be NULL or a JSON object; currently supported keys:
/// `{"read_only": bool}`.
///
/// Returns a handle to pass to the other functions, or NULL on failure
/// with `err_out` filled. Close the handle with [`hawdb_close`].
///
/// # Safety
/// `path` and `options_json` must be readable for `path_len` /
/// `options_len` bytes or NULL; `err_out` must be NULL or writable
/// [`HawdbBuffer`] storage. The returned handle is safe to use from
/// multiple threads; calls are serialized internally.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_open(
    path: *const c_char,
    path_len: usize,
    options_json: *const c_char,
    options_len: usize,
    err_out: *mut HawdbBuffer,
) -> *mut HawdbDatabase {
    unsafe { clear_buffer(err_out) };
    let result = catch_unwind(AssertUnwindSafe(|| unsafe {
        hawdb_open_inner(
            FfiStr {
                ptr: path,
                len: path_len,
            },
            FfiStr {
                ptr: options_json,
                len: options_len,
            },
        )
    }));
    match result {
        Ok(Ok(db)) => db,
        Ok(Err(error)) => {
            unsafe { set_error(err_out, error) };
            ptr::null_mut()
        }
        Err(_) => {
            unsafe { set_error(err_out, FfiError::panic()) };
            ptr::null_mut()
        }
    }
}

unsafe fn hawdb_open_inner(
    path: FfiStr,
    options_json: FfiStr,
) -> Result<*mut HawdbDatabase, FfiError> {
    let path = unsafe { read_str(path, "path") }?;
    let mut options = HawDBEmbeddedOpenOptions::new(PathBuf::from(path));
    if let Some(options_text) = unsafe { read_opt_str(options_json, "options_json") }? {
        let parsed: serde_json::Value = serde_json::from_str(options_text).map_err(|error| {
            FfiError::invalid(format!("hawdb: options_json is not valid JSON: {error}"))
        })?;
        let object = parsed
            .as_object()
            .ok_or_else(|| FfiError::invalid("hawdb: options_json must be a JSON object"))?;
        if let Some(read_only) = object.get("read_only") {
            let read_only = read_only
                .as_bool()
                .ok_or_else(|| FfiError::invalid("hawdb: \"read_only\" must be a bool"))?;
            options.config.read_only = read_only;
        }
    }
    let database =
        HawDBEmbedded::open_with_options(options).map_err(|error| FfiError::open(&error))?;
    Ok(Box::into_raw(Box::new(HawdbDatabase {
        inner: Mutex::new(database),
    })))
}

/// Open an independent in-memory database using the embedded facade defaults.
/// Close the returned handle with [`hawdb_close`]. Errors use the same output
/// convention as [`hawdb_open`]; this does not create a temporary on-disk store.
///
/// # Safety
/// `err_out` must be NULL or point to writable [`HawdbBuffer`] storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_open_in_memory(err_out: *mut HawdbBuffer) -> *mut HawdbDatabase {
    unsafe { clear_buffer(err_out) };
    match catch_unwind(AssertUnwindSafe(HawDBEmbedded::open_in_memory)) {
        Ok(database) => Box::into_raw(Box::new(HawdbDatabase {
            inner: Mutex::new(database),
        })),
        Err(_) => {
            unsafe { set_error(err_out, FfiError::panic()) };
            ptr::null_mut()
        }
    }
}

/// Close a database handle and release its resources.
///
/// # Safety
/// `db` must be a handle returned by [`hawdb_open`] and not already
/// closed. Passing NULL is a no-op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_close(db: *mut HawdbDatabase) {
    if db.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| drop(unsafe { Box::from_raw(db) })));
}

/// Run one Cypher statement.
///
/// `cypher` is `cypher_len` UTF-8 bytes and must not be NULL.
/// `params_json` may be NULL or a JSON object mapping `$name` parameter
/// names to values (`null`, bool, number, string, array, object, or the
/// tagged forms `{"$binary": "<base64>"}` and `{"$uuid": "<text>"}`).
///
/// On success returns true and `result_out` receives the result envelope
/// `{"columns": [...], "rows": [[...], ...]}`; on failure returns false
/// and `err_out` receives `{"kind", "message"}`.
///
/// # Safety
/// `db` must be a live [`hawdb_open`] handle; `cypher` and `params_json`
/// must be readable for `cypher_len` / `params_len` bytes or NULL;
/// `result_out` and `err_out` must be NULL or writable [`HawdbBuffer`]
/// storage. A false return leaves the handle usable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_query(
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
            result_out,
            err_out,
        )
    }
}

/// Run one SQL statement.
///
/// `params_json` may be NULL or a JSON array of positional `$1`
/// parameters with the same value encoding as [`hawdb_query`]. Result
/// and error conventions match [`hawdb_query`].
///
/// # Safety
/// Same contract as [`hawdb_query`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_query_sql(
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
            result_out,
            err_out,
        )
    }
}

unsafe fn query_impl(
    db: *mut HawdbDatabase,
    text: FfiStr,
    params_json: FfiStr,
    sql: bool,
    result_out: *mut HawdbBuffer,
    err_out: *mut HawdbBuffer,
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
    let result = catch_unwind(AssertUnwindSafe(|| unsafe {
        let text = read_str(text, "statement")?;
        if sql {
            let params = parse_sql_params(params_json)?;
            #[cfg(feature = "boundary-profiling")]
            let _engine = boundary_profile::PhaseTimer::start(boundary_profile::Phase::Engine);
            database
                .inner
                .lock()
                .map_err(|_| FfiError::invalid("hawdb: database mutex poisoned"))?
                .database_mut()
                .query_sql_with_params(text, &params)
                .map_err(|error| FfiError {
                    kind: variant_name(&error),
                    message: error.to_string(),
                })
        } else {
            let params = parse_cypher_params(params_json)?;
            #[cfg(feature = "boundary-profiling")]
            let _engine = boundary_profile::PhaseTimer::start(boundary_profile::Phase::Engine);
            database
                .inner
                .lock()
                .map_err(|_| FfiError::invalid("hawdb: database mutex poisoned"))?
                .query_with_params_admitted(text, &params)
                .map_err(|error| FfiError::query(&error))
        }
    }));
    match result {
        Ok(Ok(output)) => {
            #[cfg(feature = "boundary-profiling")]
            let _conversion = boundary_profile::PhaseTimer::start(boundary_profile::Phase::Results);
            unsafe { set_buffer(result_out, output_to_json(&output)) };
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

unsafe fn parse_cypher_params(params_json: FfiStr) -> Result<BTreeMap<String, Value>, FfiError> {
    #[cfg(feature = "boundary-profiling")]
    let _conversion = boundary_profile::PhaseTimer::start(boundary_profile::Phase::Parameters);
    let Some(text) = unsafe { read_opt_str(params_json, "params_json") }? else {
        return Ok(BTreeMap::new());
    };
    parsed_params(text, "cypher params must be a JSON object")?
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, item)| {
            json_to_value(item)
                .map(|value| (key.clone(), value))
                .map_err(FfiError::invalid)
        })
        .collect()
}

unsafe fn parse_sql_params(params_json: FfiStr) -> Result<Vec<Value>, FfiError> {
    #[cfg(feature = "boundary-profiling")]
    let _conversion = boundary_profile::PhaseTimer::start(boundary_profile::Phase::Parameters);
    let Some(text) = unsafe { read_opt_str(params_json, "params_json") }? else {
        return Ok(Vec::new());
    };
    parsed_params(text, "sql params must be a JSON array")?
        .as_array()
        .unwrap()
        .iter()
        .map(|item| json_to_value(item).map_err(FfiError::invalid))
        .collect()
}

fn parsed_params(text: &str, requirement: &str) -> Result<serde_json::Value, FfiError> {
    let parsed: serde_json::Value = serde_json::from_str(text).map_err(|error| {
        FfiError::invalid(format!("hawdb: params_json is not valid JSON: {error}"))
    })?;
    let matches = if requirement.contains("object") {
        parsed.is_object()
    } else {
        parsed.is_array()
    };
    if !matches {
        return Err(FfiError::invalid(format!("hawdb: {requirement}")));
    }
    Ok(parsed)
}

/// Free a buffer returned by this library and zero the caller's
/// [`HawdbBuffer`] so a second call is a safe no-op. Passing NULL for
/// `buffer`, or a buffer whose `ptr` is NULL, is a no-op.
///
/// # Safety
/// `buffer` must be NULL or point to a [`HawdbBuffer`] whose `ptr` was
/// allocated by this library and not already freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_buffer_free(buffer: *mut HawdbBuffer) {
    if buffer.is_null() {
        return;
    }
    let buffer = unsafe { &mut *buffer };
    if buffer.ptr.is_null() {
        return;
    }
    drop(unsafe { CString::from_raw(buffer.ptr) });
    buffer.ptr = ptr::null_mut();
    buffer.len = 0;
}

/// Copy the HawDB version string into `out`. The buffer is caller-owned
/// like every other [`HawdbBuffer`]: release it with
/// [`hawdb_buffer_free`].
///
/// # Safety
/// `out` must be NULL or point to writable [`HawdbBuffer`] storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hawdb_version(out: *mut HawdbBuffer) {
    unsafe { set_buffer(out, concat!("hawdb ", env!("CARGO_PKG_VERSION"))) };
}
