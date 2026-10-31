///|
/// A native transaction keeps the parent database reachable in MoonBit.
/// Rust rolls back uncommitted writes when the transaction is finalized.
type NativeTransaction

///|
#borrow(handle)
extern "C" fn ffi_tx_begin(handle : NativeHandle, read_only : Int) -> NativeTransaction = "moonbit_hawdb_tx_begin"

///|
#borrow(tx)
extern "C" fn ffi_tx_is_open(tx : NativeTransaction) -> Int = "moonbit_hawdb_tx_is_open"

///|
#borrow(tx)
extern "C" fn ffi_tx_error(tx : NativeTransaction) -> Bytes = "moonbit_hawdb_tx_error"

///|
#borrow(tx, statement, params)
extern "C" fn ffi_tx_query(
  tx : NativeTransaction,
  statement : Bytes,
  statement_len : Int,
  params : Bytes,
  params_len : Int,
  sql : Int,
) -> Bytes = "moonbit_hawdb_tx_query"

///|
#borrow(tx)
extern "C" fn ffi_tx_finish(tx : NativeTransaction, commit : Int) -> Bytes = "moonbit_hawdb_tx_finish"

///|
#borrow(tx)
extern "C" fn ffi_tx_close(tx : NativeTransaction) -> Unit = "moonbit_hawdb_tx_close"

///|
/// One explicit scope. Do not copy or use after finish. close() rolls back a
/// pending write scope or releases a read snapshot; it is idempotent.
pub struct Transaction {
  native : NativeTransaction
  owner : Database
  read_only : Bool
}

///|
pub fn Database::begin(self : Database) -> Result[Transaction, HawdbError] {
  self.begin_tx(false)
}

///|
pub fn Database::begin_read(self : Database) -> Result[Transaction, HawdbError] {
  self.begin_tx(true)
}

///|
fn Database::begin_tx(self : Database, read_only : Bool) -> Result[Transaction, HawdbError] {
  let native = ffi_tx_begin(self.handle, if read_only { 1 } else { 0 })
  if ffi_tx_is_open(native) == 1 {
    Ok({ native, owner: self, read_only })
  } else {
    Err(native_error(@utf8.decode_lossy(ffi_tx_error(native))))
  }
}

///|
pub fn Transaction::is_open(self : Transaction) -> Bool {
  ffi_tx_is_open(self.native) == 1
}

///|
pub fn Transaction::query(
  self : Transaction,
  statement : String,
  params : Map[String, Value],
) -> Result[QueryResult, HawdbError] {
  if !self.is_open() {
    return Err(HawdbError::Closed)
  }
  let json = match encode_named_params(params) {
    Ok(json) => json
    Err(error) => return Err(error)
  }
  self.run(statement, json, 0)
}

///|
pub fn Transaction::query_sql(
  self : Transaction,
  statement : String,
  params : Array[Value],
) -> Result[QueryResult, HawdbError] {
  if !self.is_open() {
    return Err(HawdbError::Closed)
  }
  let json = match encode_sql_params(params) {
    Ok(json) => json
    Err(error) => return Err(error)
  }
  self.run(statement, json, 1)
}

///|
fn Transaction::run(
  self : Transaction, statement : String, json : String, sql : Int,
) -> Result[QueryResult, HawdbError] {
  let query_bytes = @utf8.encode(statement)
  let json_bytes = @utf8.encode(json)
  let bytes = ffi_tx_query(
    self.native, query_bytes, query_bytes.length(),
    json_bytes, json_bytes.length(), sql,
  )
  if bytes.length() == 0 {
    Err(native_error(@utf8.decode_lossy(ffi_tx_error(self.native))))
  } else {
    result_from_json(@utf8.decode_lossy(bytes))
  }
}

///|
/// Commit staged writes as one engine transaction; read scopes simply release
/// their pinned snapshots.
pub fn Transaction::commit(self : Transaction) -> Result[QueryResult, HawdbError] {
  self.finish(1)
}

///|
/// Rollback and close (or release a read-only snapshot).
pub fn Transaction::rollback(self : Transaction) -> Result[Unit, HawdbError] {
  match self.finish(0) {
    Ok(_) => Ok(())
    Err(err) => Err(err)
  }
}

///|
fn Transaction::finish(self : Transaction, commit : Int) -> Result[QueryResult, HawdbError] {
  let result = ffi_tx_finish(self.native, commit)
  if result.length() == 0 {
    Err(native_error(@utf8.decode_lossy(ffi_tx_error(self.native))))
  } else {
    result_from_json(@utf8.decode_lossy(result))
  }
}

///|
pub fn Transaction::close(self : Transaction) -> Unit {
  ffi_tx_close(self.native)
}

///|
/// A MoonBit-first transaction scope. Commit on Ok, rollback on Err.
/// For imperative callers manual begin/commit/rollback remains available.
/// Returning a failure never publishes a prefix of writes.
pub fn[T] Database::with_transaction(
  self : Database,
  body : (Transaction) -> Result[T, HawdbError],
) -> Result[T, HawdbError] {
  let tx = match self.begin() {
    Ok(tx) => tx
    Err(err) => return Err(err)
  }
  // Deterministic rollback on any non-local exit; harmless after commit/rollback.
  defer tx.close()
  match body(tx) {
    Ok(value) =>
      match tx.commit() {
        Ok(_) => Ok(value)
        Err(err) => Err(err)
      }
    Err(err) =>
      match tx.rollback() {
        Ok(_) => Err(err)
        Err(rollback_error) => Err(rollback_error)
      }
  }
}

///|
/// A pinned read scope with deterministic release even if the callback
/// returns an error. Snapshot reads are not mutually exclusive with writer
/// transactions on another connection.
pub fn[T] Database::with_read_transaction(
  self : Database,
  body : (Transaction) -> Result[T, HawdbError],
) -> Result[T, HawdbError] {
  let tx = match self.begin_read() {
    Ok(tx) => tx
    Err(err) => return Err(err)
  }
  defer tx.close()
  body(tx)
}
