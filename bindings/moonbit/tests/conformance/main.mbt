///|
/// Real end-to-end MoonBit -> HawDB business contract checks.
/// An error suppresses the final success marker.

///|
fn failed(label : String, detail : String) -> Bool {
  println("CASE_FAIL " + label + ": " + detail)
  false
}

///|
fn cypher(db : @hawdb.Database, label : String, statement : String, params : String) -> Bool {
  match db.query_raw(statement, params) {
    Ok(_) => true
    Err(err) => failed(label, err)
  }
}

///|
fn sql(db : @hawdb.Database, label : String, statement : String, params : String) -> Bool {
  match db.query_sql_raw(statement, params) {
    Ok(_) => true
    Err(err) => failed(label, err)
  }
}

///|
/// Compare structured rows instead of searching an output string.
fn expect_rows(label : String, response : Result[String, String], expected : String) -> Bool {
  let data = match response {
    Ok(data) => data
    Err(err) => return failed(label, err)
  }
  let parsed : Json = @json.parse(data) catch {
    _ => return failed(label, "invalid result JSON")
  }
  let expected_json : Json = @json.parse(expected) catch {
    _ => return failed(label, "invalid expected JSON fixture")
  }
  match parsed {
    Json::Object(fields) =>
      match fields.get("rows") {
        Some(Json::Array(rows)) => {
          let got = Json::array(rows).stringify()
          let want = expected_json.stringify()
          if got != want {
            return failed(label, "rows=" + got + " expected=" + want)
          }
          println("CASE_PASS " + label)
          true
        }
        _ => failed(label, "missing rows array")
      }
    _ => failed(label, "missing result object")
  }
}

///|
/// Match the actual C ABI column names to each positional row value.
/// Go Result.Dicts() and Python fetchall() expose the same name/value
/// contract. We must not assume the engine returns columns in a particular
/// order or change the expected values to mask a row mismatch.
fn expect_named_rows(
  label : String,
  response : Result[String, String],
  expected_columns : Array[String],
  expected : String,
) -> Bool {
  let data = match response {
    Ok(data) => data
    Err(err) => return failed(label, err)
  }
  let parsed : Json = @json.parse(data) catch {
    _ => return failed(label, "invalid result JSON")
  }
  let expected_json : Json = @json.parse(expected) catch {
    _ => return failed(label, "invalid expected JSON fixture")
  }
  match parsed {
    Json::Object(fields) =>
      match (fields.get("columns"), fields.get("rows")) {
        (Some(Json::Array(columns)), Some(Json::Array(rows))) => {
          if columns.length() != expected_columns.length() {
            return failed(label, "column count mismatch: " + data)
          }
          let indices : Array[Int] = []
          for expected_name in expected_columns {
            let mut index = -1
            let mut count = 0
            for i = 0; i < columns.length(); i = i + 1 {
              match columns[i] {
                Json::String(actual_name) =>
                  if actual_name == expected_name {
                    index = i
                    count = count + 1
                  }
                _ => return failed(label, "column name must be a string: " + data)
              }
            }
            if count != 1 {
              return failed(
                label,
                "expected exactly one column named " + expected_name + ": " + data,
              )
            }
            indices.push(index)
          }
          let projected : Array[Json] = []
          for row in rows {
            match row {
              Json::Array(values) => {
                if values.length() != columns.length() {
                  return failed(label, "row width does not match columns: " + data)
                }
                let ordered : Array[Json] = []
                for index in indices {
                  ordered.push(values[index])
                }
                projected.push(Json::array(ordered))
              }
              _ => return failed(label, "row must be an array: " + data)
            }
          }
          let got = Json::array(projected).stringify()
          let want = expected_json.stringify()
          if got != want {
            return failed(label, "columns=" + Json::array(columns).stringify() +
              " projected rows=" + got + " expected=" + want)
          }
          println("CASE_PASS " + label)
          true
        }
        _ => failed(label, "result missing columns or rows: " + data)
      }
    _ => failed(label, "result must be a JSON object")
  }
}

///|
/// Equivalent to the Go two-hop friend-of-friend test.
fn graph_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(db) => db
    Err(err) => return failed("graph.open", err.describe())
  }
  for statement in [
    "CREATE (:Person {name: 'alice'})",
    "CREATE (:Person {name: 'bob'})",
    "CREATE (:Person {name: 'carol'})",
    "MATCH (a:Person {name: 'alice'}), (b:Person {name: 'bob'}) CREATE (a)-[:KNOWS]->(b)",
    "MATCH (b:Person {name: 'bob'}), (c:Person {name: 'carol'}) CREATE (b)-[:KNOWS]->(c)",
  ] {
    if !cypher(db, "graph.setup", statement, "{}") {
      db.close()
      return false
    }
  }
  let ok = expect_rows(
    "graph.social_two_hops",
    db.query_raw("MATCH (:Person {name: 'alice'})-[:KNOWS]->()-[:KNOWS]->(fof) RETURN fof.name AS name", "{}"),
    "[[\"carol\"]]",
  )
  db.close()
  ok
}

///|
/// Small TPC-C-shaped order record; SQL DDL, inserts and deterministic reads.
fn sql_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(db) => db
    Err(err) => return failed("sql.open", err.describe())
  }
  if !sql(db, "sql.schema", "CREATE TABLE business_orders (id BIGINT PRIMARY KEY, amount BIGINT)", "[]") ||
    !sql(db, "sql.insert1", "INSERT INTO business_orders (id, amount) VALUES ($1, $2)", "[1,12]") ||
    !sql(db, "sql.insert2", "INSERT INTO business_orders (id, amount) VALUES ($1, $2)", "[2,34]") {
    db.close()
    return false
  }
  let ok = expect_named_rows(
    "sql.orders",
    db.query_sql_raw("SELECT id, amount FROM business_orders ORDER BY id", "[]"),
    ["id", "amount"],
    "[[1,12],[2,34]]",
  )
  db.close()
  ok
}

///|
/// Exactly computed L2 distance, not ANN index/search recall.
fn vector_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(db) => db
    Err(err) => return failed("vector.open", err.describe())
  }
  if !sql(db, "vector.schema", "CREATE TABLE business_vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))", "[]") ||
    !sql(db, "vector.insert", "INSERT INTO business_vectors (id, embedding) VALUES ($1, $2)", "[1,{\"$vector\":[1.0,-0.0]}]") {
    db.close()
    return false
  }
  let ok = expect_rows(
    "vector.exact_l2",
    db.query_sql_raw(
      "SELECT l2_distance(embedding, $1) AS distance FROM business_vectors",
      "[{\"$vector\":[1.0,-0.0]}]",
    ),
    "[[0.0]]",
  )
  db.close()
  ok
}

///|
/// Minimal persisted Posoco-shaped session: metadata and ordered messages.
/// Does not claim to implement the full SessionStore trait.
fn session_case() -> Bool {
  let path = "./tests/conformance-db"
  let db = match @hawdb.open(path) {
    Ok(db) => db
    Err(err) => return failed("session.open", err.describe())
  }
  // HawDB's bound mutation planner requires two independent MATCH
  // endpoints and undecorated CREATE references. Creating a message node
  // and attaching it are separate statements until we expose transactions;
  // this fixture checks persistence/order, not multi-statement atomicity.
  if !cypher(db, "session.create", "CREATE (:SuiteSession {id: $id, title: $title})", "{\"id\":\"s1\",\"title\":\"MoonBit Session\"}") ||
    !cypher(db, "session.user.create", "CREATE (:SuiteMessage {sid: $sid, idx: $idx, role: $role, body: $body})", "{\"sid\":\"s1\",\"idx\":0,\"role\":\"user\",\"body\":\"你好，MoonBit\"}") ||
    !cypher(db, "session.user.attach", "MATCH (s:SuiteSession {id: $sid}), (m:SuiteMessage {sid: $sid, idx: $idx}) CREATE (s)-[:HAS_MESSAGE]->(m)", "{\"sid\":\"s1\",\"idx\":0}") ||
    !cypher(db, "session.assistant.create", "CREATE (:SuiteMessage {sid: $sid, idx: $idx, role: $role, body: $body})", "{\"sid\":\"s1\",\"idx\":1,\"role\":\"assistant\",\"body\":\"HawDB 已保存\"}") ||
    !cypher(db, "session.assistant.attach", "MATCH (s:SuiteSession {id: $sid}), (m:SuiteMessage {sid: $sid, idx: $idx}) CREATE (s)-[:HAS_MESSAGE]->(m)", "{\"sid\":\"s1\",\"idx\":1}") {
    db.close()
    return false
  }
  db.close()
  let reopened = match @hawdb.open(path) {
    Ok(db) => db
    Err(err) => return failed("session.reopen", err.describe())
  }
  let ok = expect_named_rows(
    "session.ordered_messages_after_reopen",
    reopened.query_raw(
      "MATCH (s:SuiteSession {id: $sid})-[:HAS_MESSAGE]->(m:SuiteMessage) RETURN m.idx AS idx, m.role AS role, m.body AS body ORDER BY m.idx",
      "{\"sid\":\"s1\"}",
    ),
    ["idx", "role", "body"],
    "[[0,\"user\",\"你好，MoonBit\"],[1,\"assistant\",\"HawDB 已保存\"]]",
  )
  reopened.close()
  ok
}

///|
/// Malformed query must not poison handle, close must be idempotent.
fn error_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(db) => db
    Err(err) => return failed("error.open", err.describe())
  }
  if !cypher(db, "errors.setup", "CREATE (:RecoveryProbe {n: 1})", "{}") {
    db.close()
    return false
  }
  let typed = match db.query_raw("NOT VALID CYPHER AT ALL", "{}") {
    Err(err) => err.contains("kind") && err.contains("message")
    Ok(_) => false
  }
  if !typed {
    db.close()
    return failed("errors.typed_envelope", "invalid error reporting")
  }
  let ok = expect_named_rows(
    "errors.recover",
    db.query_raw("MATCH (n:RecoveryProbe) RETURN n.n AS answer", "{}"),
    ["answer"],
    "[[1]]",
  )
  db.close()
  db.close()
  let closed = match db.query_raw("MATCH (n:RecoveryProbe) RETURN n.n AS answer", "{}") {
    Err(_) => true
    Ok(_) => false
  }
  if !closed {
    return failed("errors.closed", "query after close succeeded")
  }
  ok
}

///|
/// A typed, real-engine contract. This uses ADT values, not handwritten JSON.
fn typed_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(db) => db
    Err(err) => return failed("typed.open", err.describe())
  }
  let max = 9223372036854775807L
  let min = -9223372036854775807L - 1L
  let p : Map[String, @hawdb.Value] = {
    "id": @hawdb.Value::Text("typed-one"),
    "hi": @hawdb.Value::Int(max),
    "lo": @hawdb.Value::Int(min),
    "body": @hawdb.Value::Text("MoonBit 中文 🤣"),
    "nested": @hawdb.Value::List([
      @hawdb.Value::Bool(true),
      @hawdb.Value::Null,
      @hawdb.Value::Map({ "k": @hawdb.Value::Text("v") }),
    ]),
  }
  let created = db.query(
    "CREATE (:TypedRecord {id: $id, hi: $hi, lo: $lo, body: $body, nested: $nested})",
    p,
  )
  match created {
    Err(err) => {
      db.close()
      return failed("typed.create", err.describe())
    }
    Ok(_) => ()
  }
  let selected = db.query(
    "MATCH (n:TypedRecord {id: $id}) RETURN n.hi AS hi, n.lo AS lo, n.body AS body, n.nested AS nested",
    { "id": @hawdb.Value::Text("typed-one") },
  )
  let ok = match selected {
    Err(err) => failed("typed.read", err.describe())
    Ok(result) =>
      if result.rows.length() != 1 {
        failed("typed.read", "row count mismatch")
      } else {
        match (result.get(0, "hi"), result.get(0, "lo"), result.get(0, "body"), result.get(0, "nested")) {
          (
            Some(@hawdb.Value::Int(hi)),
            Some(@hawdb.Value::Int(lo)),
            Some(@hawdb.Value::Text(body)),
            Some(@hawdb.Value::List(nested)),
          ) => {
            if hi != max || lo != min || body != "MoonBit 中文 🤣" || nested.length() != 3 {
              failed("typed.read", "typed value mismatch")
            } else {
              println("CASE_PASS typed.cypher_int64_nested")
              true
            }
          }
          _ => failed("typed.read", "incorrect ADT variants")
        }
      }
  }
  if !ok {
    db.close()
    return false
  }

  let sql_ok = match db.query_sql("SELECT 1", []) {
    Ok(result) =>
      if result.rows.length() == 1 {
        println("CASE_PASS typed.sql")
        true
      } else {
        failed("typed.sql", "expected one row")
      }
    Err(err) => failed("typed.sql", err.describe())
  }
  if !sql_ok {
    db.close()
    return false
  }

  let failed_query = match db.query("NOT A VALID CYPHER QUERY", {}) {
    Err(@hawdb.HawdbError::Engine(kind, _)) =>
      if kind == "parse" {
        println("CASE_PASS typed.error_kind")
        true
      } else {
        failed("typed.error_kind", "unexpected engine error category: " + kind)
      }
    Err(err) => failed("typed.error_kind", err.describe())
    Ok(_) => failed("typed.error_kind", "invalid query accepted")
  }
  db.close()
  failed_query
}

///|
/// Typed values must survive native traversal and result ownership,
/// including JSON-tagged binary, UUID and VECTOR values.
fn typed_value_roundtrip() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(db) => db
    Err(err) => return failed("typed.values.open", err.describe())
  }
  let p : Map[String, @hawdb.Value] = {
    "id": @hawdb.Value::Text("value-roundtrip"),
    "binary": @hawdb.Value::Binary(b"\x00\xff\x01\x7f"),
    "empty_binary": @hawdb.Value::Binary(b""),
    "uuid": @hawdb.Value::Uuid("550e8400-e29b-41d4-a716-446655440000"),
    "nested": @hawdb.Value::Map({
      "empty": @hawdb.Value::List([]),
      "nullable": @hawdb.Value::Null,
      "flag": @hawdb.Value::Bool(true),
      "child": @hawdb.Value::List([@hawdb.Value::Int(-42L), @hawdb.Value::Text("💾数据库")]),
    }),
  }
  let created = db.query(
    "CREATE (:ValueRoundtrip {id: $id, binary: $binary, empty_binary: $empty_binary, uuid: $uuid, nested: $nested})",
    p,
  )
  match created {
    Err(err) => {
      db.close()
      return failed("typed.values.create", err.describe())
    }
    Ok(_) => ()
  }
  let retrieved = db.query(
    "MATCH (n:ValueRoundtrip {id: $id}) RETURN n.binary AS binary, n.empty_binary AS empty_binary, n.uuid AS uuid, n.nested AS nested",
    { "id": @hawdb.Value::Text("value-roundtrip") },
  )
  let passed = match retrieved {
    Err(err) => failed("typed.values.read", err.describe())
    Ok(result) =>
      match (
        result.get(0, "binary"),
        result.get(0, "empty_binary"),
        result.get(0, "uuid"),
        result.get(0, "nested"),
      ) {
        (
          Some(@hawdb.Value::Binary(binary)),
          Some(@hawdb.Value::Binary(empty)),
          Some(@hawdb.Value::Uuid(uuid)),
          Some(@hawdb.Value::Map(nested)),
        ) =>
          if binary.length() == 4 &&
            binary[0] == 0x00 &&
            binary[1] == 0xff &&
            binary[2] == 0x01 &&
            binary[3] == 0x7f &&
            empty.length() == 0 &&
            uuid == "550e8400-e29b-41d4-a716-446655440000" {
            match (nested.get("nullable"), nested.get("flag"), nested.get("child"), nested.get("empty")) {
              (Some(@hawdb.Value::Null), Some(@hawdb.Value::Bool(true)), Some(@hawdb.Value::List(child)), Some(@hawdb.Value::List(empty_list))) =>
                if child.length() == 2 && empty_list.length() == 0 {
                  println("CASE_PASS typed.binary_uuid_nested")
                  true
                } else {
                  failed("typed.binary_uuid_nested", "incorrect nested value lengths")
                }
              _ => failed("typed.binary_uuid_nested", "incorrect nested ADT shape")
            }
          } else {
            failed("typed.binary_uuid_nested", "wrong binary bytes or UUID")
          }
        _ => failed("typed.binary_uuid_nested", "incorrect result variants")
      }
  }
  // Materialized rows must remain readable after closing the connection.
  db.close()
  passed
}

///|
/// Typed SQL vector parameter -> engine -> numeric distance -> typed result.
fn typed_vector_roundtrip() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(db) => db
    Err(err) => return failed("typed.vector.open", err.describe())
  }
  match db.query_sql("CREATE TABLE typed_vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))", []) {
    Err(err) => {
      db.close()
      return failed("typed.vector.schema", err.describe())
    }
    Ok(_) => ()
  }
  match db.query_sql(
    "INSERT INTO typed_vectors (id, embedding) VALUES ($1, $2)",
    [@hawdb.Value::Int(1L), @hawdb.Value::Vector([1.0, -0.0])],
  ) {
    Err(err) => {
      db.close()
      return failed("typed.vector.insert", err.describe())
    }
    Ok(_) => ()
  }
  let data = db.query_sql(
    "SELECT embedding, l2_distance(embedding, $1) AS distance FROM typed_vectors",
    [@hawdb.Value::Vector([1.0, -0.0])],
  )
  let ok = match data {
    Err(err) => failed("typed.vector.read", err.describe())
    Ok(result) =>
      match (result.get(0, "embedding"), result.get(0, "distance")) {
        (Some(@hawdb.Value::Vector(coords)), Some(@hawdb.Value::Float(distance))) =>
          if coords.length() == 2 && coords[0] == 1.0 && coords[1] == 0.0 && distance == 0.0 {
            println("CASE_PASS typed.vector_roundtrip")
            true
          } else {
            failed("typed.vector_roundtrip", "vector coordinates or distance differ")
          }
        _ => failed("typed.vector_roundtrip", "wrong result ADT")
      }
  }
  db.close()
  ok
}

///|
/// Invalid typed vector and closed-handle diagnostics are explicit.
fn typed_rejections() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(db) => db
    Err(err) => return failed("typed.reject.open", err.describe())
  }
  let rejected = match db.query(
    "CREATE (:Reject {embedding: $v})",
    { "v": @hawdb.Value::Vector([]) },
  ) {
    Err(@hawdb.HawdbError::Codec(_)) => true
    Err(err) => return failed("typed.invalid_vector", "wrong error: " + err.describe())
    Ok(_) => return failed("typed.invalid_vector", "empty vector accepted")
  }
  if rejected {
    println("CASE_PASS typed.invalid_vector")
  }
  db.close()
  let closed = match db.query("MATCH (n:Reject) RETURN n.embedding AS v", {}) {
    Err(@hawdb.HawdbError::Closed) => true
    Err(err) => return failed("typed.closed", "wrong error: " + err.describe())
    Ok(_) => return failed("typed.closed", "query after close was accepted")
  }
  if closed {
    println("CASE_PASS typed.closed")
  }
  rejected && closed
}

///|
fn transaction_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(v) => v
    Err(err) => return failed("transaction.open", err.describe())
  }
  let tx = match db.begin() {
    Ok(v) => v
    Err(err) => return failed("transaction.begin", err.describe())
  }
  for id in [1L, 2L] {
    match tx.query("CREATE (:TxItem {id: $id})", {"id": @hawdb.Value::Int(id)}) {
      Ok(_) => ()
      Err(err) => return failed("transaction.write", err.describe())
    }
  }
  // Parent autocommit must be blocked during a write transaction.
  let blocked = match db.query("MATCH (n:TxItem) RETURN n.id AS id", {}) {
    Err(_) => true
    Ok(_) => false
  }
  if !blocked { return failed("transaction.autocommit", "parent query was allowed") }
  match tx.commit() {
    Ok(_) => ()
    Err(err) => return failed("transaction.commit", err.describe())
  }
  let values = match db.query("MATCH (n:TxItem) RETURN n.id AS id ORDER BY n.id", {}) {
    Ok(v) => v
    Err(err) => return failed("transaction.select", err.describe())
  }
  if values.rows.length() != 2 { return failed("transaction.commit", "both writes not visible") }
  let rollback = match db.begin() {
    Ok(v) => v
    Err(err) => return failed("transaction.rollback.begin", err.describe())
  }
  match rollback.query("CREATE (:TxItem {id: 3})", {}) {
    Ok(_) => ()
    Err(err) => return failed("transaction.rollback.write", err.describe())
  }
  match rollback.rollback() {
    Ok(_) => ()
    Err(err) => return failed("transaction.rollback", err.describe())
  }
  let remaining = match db.query("MATCH (n:TxItem) RETURN n.id AS id", {}) {
    Ok(v) => v
    Err(err) => return failed("transaction.rollback.query", err.describe())
  }
  if remaining.rows.length() != 2 { return failed("transaction.rollback", "rolled-back node still present") }
  println("CASE_PASS transaction.commit_rollback")
  let snapshot = match db.begin_read() {
    Ok(v) => v
    Err(err) => return failed("read_snapshot.begin", err.describe())
  }
  match db.query("CREATE (:TxItem {id: 4})", {}) {
    Ok(_) => ()
    Err(err) => return failed("read_snapshot.external_write", err.describe())
  }
  let old = match snapshot.query("MATCH (n:TxItem) RETURN n.id AS id", {}) {
    Ok(v) => v
    Err(err) => return failed("read_snapshot.query", err.describe())
  }
  if old.rows.length() != 2 { return failed("read_snapshot.stability", "read snapshot saw later write") }
  let denied = match snapshot.query("CREATE (:TxItem {id: 5})", {}) {
    Err(_) => true
    Ok(_) => false
  }
  if !denied { return failed("read_snapshot.write", "read scope permitted write") }
  snapshot.close()
  snapshot.close()
  db.close()
  println("CASE_PASS transaction.snapshot_read_only")
  true
}

///|
/// A read-only file-backed connection must reject mutations while retaining
/// the real persisted session data, not just an in-memory mock.
fn readonly_case() -> Bool {
  // A read-only open must use strict replay without mutating WAL or
  // requiring a prior writable repair/checkpoint operation.
  let db = match @hawdb.open_read_only("./tests/conformance-db") {
    Ok(v) => v
    Err(e) => return failed("readonly.open", e.describe())
  }
  let result = match db.query(
    "MATCH (s:SuiteSession {id: $id}) RETURN s.title AS title",
    {"id": @hawdb.Value::Text("s1")},
  ) {
    Ok(v) => v
    Err(e) => return failed("readonly.query", e.describe())
  }
  let found = match result.get(0, "title") {
    Some(@hawdb.Value::Text(title)) => title == "MoonBit Session"
    _ => false
  }
  if !found {
    db.close()
    return failed("readonly.data", "read-only connection lost stored session")
  }
  let blocked = match db.query("CREATE (:ReadonlyViolation {id: 1})", {}) {
    Ok(_) => false
    Err(_) => true
  }
  db.close()
  if blocked {
    println("CASE_PASS readonly.reopen_write_refusal")
    true
  } else {
    failed("readonly.refusal", "read-only database accepted mutation")
  }
}

///|
/// A second write transaction must be rejected, and abandoning an open
/// transaction must release the parent without committing its writes.
fn transaction_boundary_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(v) => v
    Err(e) => return failed("transaction.boundary.open", e.describe())
  }
  let tx = match db.begin() {
    Ok(v) => v
    Err(e) => return failed("transaction.boundary.begin", e.describe())
  }
  let nested = match db.begin() {
    Ok(other) => {
      other.close()
      false
    }
    Err(_) => true
  }
  if !nested {
    tx.close()
    db.close()
    return failed("transaction.nested", "nested transaction was allowed")
  }
  match tx.query("CREATE (:Abandoned {id: 77})", {}) {
    Ok(_) => ()
    Err(e) => return failed("transaction.abandoned.write", e.describe())
  }
  tx.close()
  tx.close()
  let result = match db.query("MATCH (n:Abandoned) RETURN n.id AS id", {}) {
    Ok(v) => v
    Err(e) => return failed("transaction.abandoned.query", e.describe())
  }
  if result.rows.length() != 0 {
    db.close()
    return failed("transaction.abandoned", "an uncommitted write was published")
  }
  db.close()
  println("CASE_PASS transaction.nested_and_abandoned")
  true
}

///|
/// A callback-scoped transaction is part of MoonBit's idiomatic API: Ok
/// commits, Err rolls back, without requiring the caller to remember close.
fn scoped_transaction_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(v) => v
    Err(e) => return failed("scoped.open", e.describe())
  }
  let committed : Result[String, @hawdb.HawdbError] = db.with_transaction(fn(tx) {
    match tx.query("CREATE (:Scoped {id: 1})", {}) {
      Ok(_) => Ok("done")
      Err(e) => Err(e)
    }
  })
  match committed {
    Ok("done") => ()
    Ok(_) => return failed("scoped.commit", "unexpected return value")
    Err(e) => return failed("scoped.commit", e.describe())
  }
  let aborted : Result[String, @hawdb.HawdbError] = db.with_transaction(fn(tx) {
    match tx.query("CREATE (:Scoped {id: 2})", {}) {
      Ok(_) => Err(@hawdb.HawdbError::Codec("intentional callback failure"))
      Err(e) => Err(e)
    }
  })
  match aborted {
    Err(@hawdb.HawdbError::Codec("intentional callback failure")) => ()
    _ => return failed("scoped.rollback", "callback error did not propagate")
  }
  let count : Result[Int, @hawdb.HawdbError] = db.with_read_transaction(fn(read) {
    match read.query("MATCH (n:Scoped) RETURN n.id AS id", {}) {
      Ok(output) => Ok(output.rows.length())
      Err(e) => Err(e)
    }
  })
  let success = match count {
    Ok(1) => true
    Ok(_) => failed("scoped.atomicity", "callback failure published a write")
    Err(e) => failed("scoped.read", e.describe())
  }
  db.close()
  if success { println("CASE_PASS transaction.scoped_callbacks") }
  success
}

///|
/// SQL operations must share the same engine transaction as Cypher.
fn transaction_sql_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(v) => v
    Err(e) => return failed("transaction.sql.open", e.describe())
  }
  match db.query_sql("CREATE TABLE tx_sql (id BIGINT PRIMARY KEY)", []) {
    Err(e) => return failed("transaction.sql.schema", e.describe())
    Ok(_) => ()
  }
  let tx = match db.begin() {
    Ok(v) => v
    Err(e) => return failed("transaction.sql.begin", e.describe())
  }
  for id in [1L, 2L] {
    match tx.query_sql("INSERT INTO tx_sql (id) VALUES ($1)", [@hawdb.Value::Int(id)]) {
      Err(e) => return failed("transaction.sql.insert", e.describe())
      Ok(_) => ()
    }
  }
  match tx.commit() {
    Err(e) => return failed("transaction.sql.commit", e.describe())
    Ok(_) => ()
  }
  let result = match db.query_sql("SELECT id FROM tx_sql ORDER BY id", []) {
    Ok(v) => v
    Err(e) => return failed("transaction.sql.read", e.describe())
  }
  db.close()
  if result.rows.length() == 2 {
    println("CASE_PASS transaction.sql_batch_commit")
    true
  } else {
    failed("transaction.sql_batch_commit", "committed rows missing")
  }
}

///|
/// Match Python's strict nested parameter rejection (64 levels), rejecting
/// excess depth in MoonBit before touching the embedded engine.
fn nested_depth_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(v) => v
    Err(e) => return failed("typed.depth.open", e.describe())
  }
  let mut deep = @hawdb.Value::Null
  for i = 0; i < 70; i = i + 1 {
    deep = @hawdb.Value::List([deep])
  }
  let answer = db.query("CREATE (:Deep {nested: $v})", {"v": deep})
  let rejected = match answer {
    Err(@hawdb.HawdbError::Codec(message)) => message.contains("64")
    _ => false
  }
  // A nested Map must consume the same depth budget as a nested List.
  // Previously Map entries restarted depth at zero, bypassing the limit.
  let mut map_deep = @hawdb.Value::Null
  for i = 0; i < 70; i = i + 1 {
    map_deep = @hawdb.Value::Map({"child": map_deep})
  }
  let map_answer = db.query("CREATE (:DeepMap {nested: $v})", {"v": map_deep})
  let map_rejected = match map_answer {
    Err(@hawdb.HawdbError::Codec(message)) => message.contains("64")
    _ => false
  }
  if !map_rejected {
    db.close()
    return failed("typed.map_max_nesting", "over-deep maps bypassed codec depth limit")
  }
  db.close()
  println("CASE_PASS typed.map_max_nesting")
  if rejected {
    println("CASE_PASS typed.max_nesting")
    true
  } else {
    failed("typed.max_nesting", "over-deep parameters were not rejected by codec")
  }
}

///|
/// A singleton map with a reserved ABI tag must survive a roundtrip as a
/// Map, not turn into Uuid/Binary/Vector (nor lose a literal "$map" key).
fn typed_marker_map_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(v) => v
    Err(e) => return failed("typed.marker_map.open", e.describe())
  }
  let params : Map[String, @hawdb.Value] = {
    "id": @hawdb.Value::Text("marker-map"),
    "uuid_map": @hawdb.Value::Map({"$uuid": @hawdb.Value::Text("ordinary-text")}),
    "binary_map": @hawdb.Value::Map({"$binary": @hawdb.Value::Text("not-base64")}),
    "vector_map": @hawdb.Value::Map({"$vector": @hawdb.Value::Text("not-a-vector")}),
    "escape_map": @hawdb.Value::Map({"$map": @hawdb.Value::Text("literal-key")}),
  }
  match db.query(
    "CREATE (:MarkerMap {id: $id, uuid_map: $uuid_map, binary_map: $binary_map, vector_map: $vector_map, escape_map: $escape_map})",
    params,
  ) {
    Err(e) => {
      db.close()
      return failed("typed.marker_map.create", e.describe())
    }
    Ok(_) => ()
  }
  let rows = db.query(
    "MATCH (n:MarkerMap {id: $id}) RETURN n.uuid_map AS a, n.binary_map AS b, n.vector_map AS c, n.escape_map AS d",
    {"id": @hawdb.Value::Text("marker-map")},
  )
  let ok = match rows {
    Err(e) => failed("typed.marker_map.read", e.describe())
    Ok(result) =>
      match (result.get(0, "a"), result.get(0, "b"), result.get(0, "c"), result.get(0, "d")) {
        (
          Some(@hawdb.Value::Map(a)),
          Some(@hawdb.Value::Map(b)),
          Some(@hawdb.Value::Map(c)),
          Some(@hawdb.Value::Map(d)),
        ) =>
          match (a.get("$uuid"), b.get("$binary"), c.get("$vector"), d.get("$map")) {
            (
              Some(@hawdb.Value::Text("ordinary-text")),
              Some(@hawdb.Value::Text("not-base64")),
              Some(@hawdb.Value::Text("not-a-vector")),
              Some(@hawdb.Value::Text("literal-key")),
            ) => true
            _ => failed("typed.marker_map", "escaped map value changed")
          }
        _ => failed("typed.marker_map", "singleton map decoded as tagged value")
      }
  }
  db.close()
  if ok { println("CASE_PASS typed.marker_map") }
  ok
}

///|
/// Finite Double coordinates can still overflow HawDB's f32 vector storage.
fn typed_vector_range_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(v) => v
    Err(e) => return failed("typed.vector_range.open", e.describe())
  }
  let invalid = db.query_sql("SELECT 1", [@hawdb.Value::Vector([1.0e100])])
  let rejected = match invalid {
    Err(@hawdb.HawdbError::Engine(kind, _)) => kind == "invalid_argument"
    _ => false
  }
  db.close()
  if rejected {
    println("CASE_PASS typed.vector_range")
    true
  } else {
    failed("typed.vector_range", "f32-overflow coordinate was accepted")
  }
}

///|
/// A parent close invalidates a transaction without freeing its engine
/// before rollback or read snapshot cleanup.
fn parent_close_transaction_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(v) => v
    Err(e) => return failed("transaction.parent_close.open", e.describe())
  }
  let tx = match db.begin() {
    Ok(v) => v
    Err(e) => {
      db.close()
      return failed("transaction.parent_close.begin", e.describe())
    }
  }
  match tx.query("CREATE (:ParentClose {id: 1})", {}) {
    Ok(_) => ()
    Err(e) => {
      tx.close()
      db.close()
      return failed("transaction.parent_close.write", e.describe())
    }
  }
  db.close()
  if tx.is_open() {
    tx.close()
    return failed("transaction.parent_close.write", "write transaction remained open")
  }
  match tx.query("CREATE (:ParentClose {id: 2})", {}) {
    Err(@hawdb.HawdbError::Closed) => ()
    _ => {
      tx.close()
      return failed("transaction.parent_close.write", "query after parent close was not Closed")
    }
  }
  match tx.commit() {
    Err(@hawdb.HawdbError::Closed) => ()
    _ => {
      tx.close()
      return failed("transaction.parent_close.write", "commit after parent close was not Closed")
    }
  }
  tx.close()
  tx.close()
  println("CASE_PASS transaction.parent_close_write")

  let reader_db = match @hawdb.open_in_memory() {
    Ok(v) => v
    Err(e) => return failed("transaction.parent_close.read_open", e.describe())
  }
  let read = match reader_db.begin_read() {
    Ok(v) => v
    Err(e) => {
      reader_db.close()
      return failed("transaction.parent_close.read_begin", e.describe())
    }
  }
  reader_db.close()
  if read.is_open() {
    read.close()
    return failed("transaction.parent_close.read", "read scope remained open")
  }
  match read.query_sql("SELECT 1", []) {
    Err(@hawdb.HawdbError::Closed) => ()
    _ => {
      read.close()
      return failed("transaction.parent_close.read", "read after parent close was not Closed")
    }
  }
  read.close()
  read.close()
  println("CASE_PASS transaction.parent_close_read")
  true
}

///|
/// Public typed API: missing columns must not be confused with database NULL,
/// and the raise adapters must report the same structured error variants.
fn api_error_contract_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(v) => v
    Err(e) => return failed("api.error.open", e.describe())
  }
  let null_case = match db.query_sql("SELECT NULL AS present", []) {
    Ok(result) =>
      match (result.get(0, "present"), result.get(0, "missing"), result.get(10, "present")) {
        (Some(@hawdb.Value::Null), None, None) => true
        _ => false
      }
    Err(_) => false
  }
  if !null_case {
    db.close()
    return failed("api.null_absent", "NULL, missing column, or missing row semantics changed")
  }
  println("CASE_PASS api.null_absent")

  let cypher_error : Result[@hawdb.QueryResult, @hawdb.HawdbError] =
    Ok(db.query_or_raise("NOT A VALID CYPHER QUERY", {})) catch {
      error => Err(error)
    }
  match cypher_error {
    Err(@hawdb.HawdbError::Engine("parse", _)) => ()
    _ => {
      db.close()
      return failed("api.raise_cypher", "raise adapter changed engine error")
    }
  }
  let sql_error : Result[@hawdb.QueryResult, @hawdb.HawdbError] =
    Ok(db.query_sql_or_raise("NOT A VALID SQL QUERY", [])) catch {
      error => Err(error)
    }
  match sql_error {
    Err(@hawdb.HawdbError::Engine(_, _)) => ()
    _ => {
      db.close()
      return failed("api.raise_sql", "SQL raise adapter lost structured engine error")
    }
  }
  println("CASE_PASS api.raise_bridge")

  db.close()
  let deep = @hawdb.Value::Vector([])
  match db.query("CREATE (:Never {v: $v})", {"v": deep}) {
    Err(@hawdb.HawdbError::Closed) => ()
    _ => return failed("api.closed_precedence", "closed database must be checked before encoding")
  }
  println("CASE_PASS api.closed_precedence")
  true
}

///|
/// Calling code may finish a scope manually; the deferred cleanup must be
/// idempotent and a later transaction on the same database must still work.
fn scoped_cleanup_case() -> Bool {
  let db = match @hawdb.open_in_memory() {
    Ok(v) => v
    Err(e) => return failed("transaction.cleanup.open", e.describe())
  }
  let first : Result[String, @hawdb.HawdbError] = db.with_transaction(fn(tx) {
    match tx.query("CREATE (:ScopedCleanup {id: 1})", {}) {
      Err(e) => Err(e)
      Ok(_) => {
        tx.close()
        Ok("closed")
      }
    }
  })
  match first {
    Err(@hawdb.HawdbError::Closed) => ()
    _ => {
      db.close()
      return failed("transaction.cleanup.closed_scope", "commit after scope close was allowed")
    }
  }
  let after = match db.query("MATCH (n:ScopedCleanup) RETURN n.id AS id", {}) {
    Ok(rows) => rows.rows.length() == 0
    Err(_) => false
  }
  if !after {
    db.close()
    return failed("transaction.cleanup.rollback", "abandoned writes escaped cleanup")
  }
  let committed : Result[String, @hawdb.HawdbError] = db.with_transaction(fn(tx) {
    match tx.query("CREATE (:ScopedCleanup {id: 2})", {}) {
      Ok(_) => Ok("ok")
      Err(e) => Err(e)
    }
  })
  match committed {
    Ok("ok") => ()
    _ => {
      db.close()
      return failed("transaction.cleanup.reuse", "subsequent transaction failed")
    }
  }
  let read : Result[Int, @hawdb.HawdbError] = db.with_read_transaction(fn(tx) {
    match tx.query("MATCH (n:ScopedCleanup) RETURN n.id AS id", {}) {
      Ok(result) => {
        tx.close()
        Ok(result.rows.length())
      }
      Err(e) => Err(e)
    }
  })
  db.close()
  match read {
    Ok(1) => {
      println("CASE_PASS transaction.scoped_cleanup")
      true
    }
    _ => failed("transaction.scoped_cleanup", "read-scope cleanup was not idempotent")
  }
}

///|
/// Repeated open/close and abandoned transactions must release engine state
/// without leaking visible writes or breaking the next open.
fn lifecycle_stress_case() -> Bool {
  for i = 0; i < 80; i = i + 1 {
    let db = match @hawdb.open_in_memory() {
      Ok(db) => db
      Err(e) => return failed("lifecycle.stress.open", e.describe())
    }
    let tx = match db.begin() {
      Ok(v) => v
      Err(e) => return failed("lifecycle.stress.begin", e.describe())
    }
    match tx.query("CREATE (:Transient {id: 1})", {}) {
      Ok(_) => ()
      Err(e) => return failed("lifecycle.stress.write", e.describe())
    }
    if i % 2 == 0 {
      db.close()
      tx.close()
    } else {
      tx.close()
      db.close()
    }
    tx.close()
    db.close()
  }
  println("CASE_PASS lifecycle.repeated_open_close")
  true
}

///|
fn main {
  if !graph_case() { return }
  if !sql_case() { return }
  if !vector_case() { return }
  if !session_case() { return }
  if !error_case() { return }
  if !typed_case() { return }
  if !api_error_contract_case() { return }
  if !typed_value_roundtrip() { return }
  if !typed_vector_roundtrip() { return }
  if !typed_marker_map_case() { return }
  if !typed_vector_range_case() { return }
  if !typed_rejections() { return }
  if !transaction_case() { return }
  if !parent_close_transaction_case() { return }
  if !lifecycle_stress_case() { return }
  if !transaction_boundary_case() { return }
  if !readonly_case() { return }
  if !scoped_transaction_case() { return }
  if !scoped_cleanup_case() { return }
  if !transaction_sql_case() { return }
  if !nested_depth_case() { return }
  println("HAWDB_MOONBIT_BUSINESS_SUITE_PASS")
}
