///|
/// Native HawDB handle backed by a MoonBit external object. The Rust finalizer closes the embedded engine; explicit close is idempotent.
type NativeHandle

///|
#borrow(path)
extern "C" fn ffi_open(path : Bytes, length : Int, read_only : Int) -> NativeHandle = "moonbit_hawdb_open"

///|
extern "C" fn ffi_open_in_memory() -> NativeHandle = "moonbit_hawdb_open_in_memory"

///|
#borrow(handle)
extern "C" fn ffi_is_open(handle : NativeHandle) -> Int = "moonbit_hawdb_is_open"

///|
#borrow(handle)
extern "C" fn ffi_close(handle : NativeHandle) -> Unit = "moonbit_hawdb_close"

///|
#borrow(handle, statement, params)
extern "C" fn ffi_execute(
  handle : NativeHandle,
  statement : Bytes,
  statement_length : Int,
  params : Bytes,
  params_length : Int,
  sql : Int,
) -> Bytes = "moonbit_hawdb_execute"

///|
#borrow(handle)
extern "C" fn ffi_error(handle : NativeHandle) -> Bytes = "moonbit_hawdb_error"

///|
extern "C" fn ffi_version() -> Bytes = "moonbit_hawdb_version"

///|
/// One in-process HawDB database. There is no server and no HTTP connection.
/// The GC finalizer is a backstop, not a substitute for explicit close.
pub struct Database {
  handle : NativeHandle
}

///|
/// Open an isolated in-memory HawDB engine.
pub fn open_in_memory() -> Result[Database, HawdbError] {
  let handle = ffi_open_in_memory()
  if ffi_is_open(handle) == 1 {
    Ok({ handle, })
  } else {
    Err(native_error(@utf8.decode_lossy(ffi_error(handle))))
  }
}

///|
/// Open a persisted HawDB project without write permission.
pub fn open_read_only(path : String) -> Result[Database, HawdbError] {
  open_with_mode(path, 1)
}

///|
/// Create or reopen a durable HawDB project directory.
pub fn open(path : String) -> Result[Database, HawdbError] {
  open_with_mode(path, 0)
}

///|
fn open_with_mode(path : String, read_only : Int) -> Result[Database, HawdbError] {
  let encoded = @utf8.encode(path)
  let handle = ffi_open(encoded, encoded.length(), read_only)
  if ffi_is_open(handle) == 1 {
    Ok({ handle, })
  } else {
    Err(native_error(@utf8.decode_lossy(ffi_error(handle))))
  }
}

///|
pub fn Database::is_open(self : Database) -> Bool {
  ffi_is_open(self.handle) == 1
}

///|
/// Advanced raw JSON Cypher transport; prefer typed query() for business code.
pub fn Database::query_raw(
  self : Database,
  cypher : String,
  params_json : String,
) -> Result[String, String] {
  self.run(cypher, params_json, 0)
}

///|
/// Advanced raw JSON SQL transport; prefer typed query_sql().
pub fn Database::query_sql_raw(
  self : Database,
  sql : String,
  params_json : String,
) -> Result[String, String] {
  self.run(sql, params_json, 1)
}

///|
fn Database::run(
  self : Database,
  statement : String,
  params_json : String,
  sql : Int,
) -> Result[String, String] {
  let statement_bytes = @utf8.encode(statement)
  let params_bytes = @utf8.encode(params_json)
  let result = ffi_execute(
    self.handle,
    statement_bytes,
    statement_bytes.length(),
    params_bytes,
    params_bytes.length(),
    sql,
  )
  if result.length() == 0 {
    Err(@utf8.decode_lossy(ffi_error(self.handle)))
  } else {
    Ok(@utf8.decode_lossy(result))
  }
}

///|
/// Idempotent at the MoonBit boundary; the Rust adapter invalidates the
/// native pointer before dropping the embedded engine. A subsequent execute reports a closed-handle error.
pub fn Database::close(self : Database) -> Unit {
  ffi_close(self.handle)
}

///|
pub fn version() -> String {
  @utf8.decode_lossy(ffi_version())
}
