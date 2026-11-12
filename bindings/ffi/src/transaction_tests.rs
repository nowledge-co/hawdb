// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "hawdb-ffi-transaction-{name}-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }

    fn database(&self) -> PathBuf {
        self.0.join("database")
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

type Outcome = Result<serde_json::Value, serde_json::Value>;

fn empty_buffer() -> HawdbBuffer {
    HawdbBuffer {
        ptr: ptr::null_mut(),
        len: 0,
    }
}

unsafe fn take(buffer: &mut HawdbBuffer) -> Option<serde_json::Value> {
    if buffer.ptr.is_null() {
        return None;
    }
    let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr.cast::<u8>(), buffer.len) };
    let value = serde_json::from_slice(bytes).unwrap();
    unsafe { hawdb_buffer_free(buffer) };
    Some(value)
}

fn finish(ok: bool, mut result: HawdbBuffer, mut error: HawdbBuffer) -> Outcome {
    let (result, error) = unsafe { (take(&mut result), take(&mut error)) };
    if ok {
        assert!(error.is_none(), "successful call reported {error:?}");
        Ok(result.unwrap_or(serde_json::Value::Null))
    } else {
        assert!(result.is_none(), "failed call returned {result:?}");
        Err(error.expect("a failed call reports an error"))
    }
}

fn open(path: &Path, options: &str) -> *mut HawdbDatabase {
    let path = path.to_str().unwrap();
    let mut error = empty_buffer();
    let db = unsafe {
        hawdb_open(
            path.as_ptr().cast(),
            path.len(),
            options.as_ptr().cast(),
            options.len(),
            &mut error,
        )
    };
    let error = unsafe { take(&mut error) };
    assert!(!db.is_null(), "open failed: {error:?}");
    db
}

type QueryFn = unsafe extern "C" fn(
    *mut HawdbDatabase,
    *const c_char,
    usize,
    *const c_char,
    usize,
    *mut HawdbBuffer,
    *mut HawdbBuffer,
) -> bool;

fn run(call: QueryFn, db: *mut HawdbDatabase, text: &str, params: serde_json::Value) -> Outcome {
    let params = params.to_string();
    let mut result = empty_buffer();
    let mut error = empty_buffer();
    let ok = unsafe {
        call(
            db,
            text.as_ptr().cast(),
            text.len(),
            params.as_ptr().cast(),
            params.len(),
            &mut result,
            &mut error,
        )
    };
    finish(ok, result, error)
}

fn sql(db: *mut HawdbDatabase, text: &str) -> Outcome {
    run(hawdb_query_sql, db, text, json!([]))
}

fn cypher(db: *mut HawdbDatabase, text: &str) -> Outcome {
    run(hawdb_query, db, text, json!({}))
}

fn tx_sql(db: *mut HawdbDatabase, text: &str, params: serde_json::Value) -> Outcome {
    run(hawdb_transaction_query_sql, db, text, params)
}

fn tx_cypher(db: *mut HawdbDatabase, text: &str) -> Outcome {
    run(hawdb_transaction_query, db, text, json!({}))
}

fn begin(db: *mut HawdbDatabase) -> Outcome {
    let mut error = empty_buffer();
    let ok = unsafe { hawdb_begin_transaction(db, &mut error) };
    finish(ok, empty_buffer(), error)
}

fn commit(db: *mut HawdbDatabase) -> Outcome {
    let mut result = empty_buffer();
    let mut error = empty_buffer();
    let ok = unsafe { hawdb_commit_transaction(db, &mut result, &mut error) };
    finish(ok, result, error)
}

fn rollback(db: *mut HawdbDatabase) -> Outcome {
    let mut error = empty_buffer();
    let ok = unsafe { hawdb_rollback_transaction(db, &mut error) };
    finish(ok, empty_buffer(), error)
}

fn kind(outcome: Outcome) -> String {
    outcome.expect_err("call should fail")["kind"]
        .as_str()
        .unwrap()
        .to_string()
}

fn rows(outcome: Outcome) -> serde_json::Value {
    outcome.unwrap()["rows"].clone()
}

const SCHEMA: [&str; 2] = [
    "CREATE TABLE events (id BIGINT PRIMARY KEY, kind TEXT)",
    "CREATE TABLE projection (id BIGINT PRIMARY KEY, total BIGINT)",
];

fn with_schema(path: &Path) -> *mut HawdbDatabase {
    let db = open(path, "{}");
    for statement in SCHEMA {
        sql(db, statement).unwrap();
    }
    sql(db, "INSERT INTO projection (id, total) VALUES (1, 0)").unwrap();
    db
}

#[test]
fn sql_and_cypher_statements_commit_together_and_survive_reopen() {
    let directory = TestDirectory::new("commit");
    let db = with_schema(&directory.database());
    begin(db).unwrap();
    tx_sql(
        db,
        "INSERT INTO events (id, kind) VALUES ($1, $2)",
        json!([1, "deposit"]),
    )
    .unwrap();
    tx_sql(
        db,
        "UPDATE projection SET total = total + 5 WHERE id = 1",
        json!([]),
    )
    .unwrap();
    tx_cypher(db, "CREATE (:Audit {event: 1})").unwrap();
    // Statements inside the transaction see its own writes.
    assert_eq!(
        rows(tx_sql(
            db,
            "SELECT total FROM projection WHERE id = 1",
            json!([])
        )),
        json!([[5]])
    );
    assert_eq!(
        rows(tx_cypher(db, "MATCH (a:Audit) RETURN a.event AS event")),
        json!([[1]])
    );
    commit(db).unwrap();
    assert_eq!(
        rows(sql(db, "SELECT id, kind FROM events")),
        json!([[1, "deposit"]])
    );
    unsafe { hawdb_close(db) };

    let db = open(&directory.database(), r#"{"read_only":true}"#);
    assert_eq!(
        rows(sql(db, "SELECT id, kind FROM events")),
        json!([[1, "deposit"]])
    );
    assert_eq!(
        rows(sql(db, "SELECT total FROM projection WHERE id = 1")),
        json!([[5]])
    );
    assert_eq!(
        rows(cypher(db, "MATCH (a:Audit) RETURN a.event AS event")),
        json!([[1]])
    );
    unsafe { hawdb_close(db) };
}

#[test]
fn rollback_discards_every_statement_and_closes_the_transaction() {
    let directory = TestDirectory::new("rollback");
    let db = with_schema(&directory.database());
    begin(db).unwrap();
    tx_sql(
        db,
        "INSERT INTO events (id, kind) VALUES (1, 'lost')",
        json!([]),
    )
    .unwrap();
    tx_sql(
        db,
        "UPDATE projection SET total = 99 WHERE id = 1",
        json!([]),
    )
    .unwrap();
    rollback(db).unwrap();
    assert_eq!(rows(sql(db, "SELECT COUNT(*) FROM events")), json!([[0]]));
    assert_eq!(
        rows(sql(db, "SELECT total FROM projection WHERE id = 1")),
        json!([[0]])
    );

    // Use after rollback or commit is a reported error, never a stale write.
    for outcome in [
        tx_sql(
            db,
            "INSERT INTO events (id, kind) VALUES (2, 'late')",
            json!([]),
        ),
        tx_cypher(db, "CREATE (:Late)"),
        commit(db),
        rollback(db),
    ] {
        assert_eq!(kind(outcome), "no_transaction");
    }
    begin(db).unwrap();
    commit(db).unwrap();
    assert_eq!(kind(commit(db)), "no_transaction");
    assert_eq!(rows(sql(db, "SELECT COUNT(*) FROM events")), json!([[0]]));
    unsafe { hawdb_close(db) };
}

#[test]
fn misuse_reports_clear_errors_and_keeps_the_handle_usable() {
    let directory = TestDirectory::new("misuse");
    let db = with_schema(&directory.database());
    begin(db).unwrap();
    assert_eq!(kind(begin(db)), "transaction_open");
    // Autocommit statements cannot run beside an open transaction.
    assert_eq!(
        kind(sql(db, "SELECT COUNT(*) FROM events")),
        "transaction_open"
    );
    assert_eq!(kind(cypher(db, "RETURN 1 AS one")), "transaction_open");
    // SQL transaction control stays rejected on the handle.
    assert!(sql(db, "BEGIN").is_err());
    tx_sql(
        db,
        "INSERT INTO events (id, kind) VALUES (1, 'kept')",
        json!([]),
    )
    .unwrap();
    commit(db).unwrap();
    assert!(sql(db, "BEGIN").is_err());
    assert!(sql(db, "COMMIT").is_err());

    let mut error = empty_buffer();
    assert!(!unsafe { hawdb_begin_transaction(ptr::null_mut(), &mut error) });
    assert_eq!(
        unsafe { take(&mut error) }.unwrap()["kind"],
        "invalid_argument"
    );
    assert!(!unsafe { hawdb_rollback_transaction(ptr::null_mut(), ptr::null_mut()) });
    assert_eq!(rows(sql(db, "SELECT id FROM events")), json!([[1]]));
    unsafe { hawdb_close(db) };
}

#[test]
fn a_failed_statement_leaves_the_transaction_open_for_rollback() {
    let directory = TestDirectory::new("failed-statement");
    let db = with_schema(&directory.database());
    sql(db, "INSERT INTO events (id, kind) VALUES (1, 'original')").unwrap();
    begin(db).unwrap();
    tx_sql(
        db,
        "INSERT INTO events (id, kind) VALUES (2, 'staged')",
        json!([]),
    )
    .unwrap();
    assert!(tx_sql(
        db,
        "INSERT INTO events (id, kind) VALUES (1, 'duplicate')",
        json!([])
    )
    .is_err());
    assert!(tx_cypher(db, "NOT VALID CYPHER").is_err());
    assert_eq!(kind(sql(db, "SELECT 1")), "transaction_open");
    rollback(db).unwrap();
    assert_eq!(
        rows(sql(db, "SELECT id, kind FROM events ORDER BY id")),
        json!([[1, "original"]])
    );
    unsafe { hawdb_close(db) };
}

#[test]
fn closing_with_an_open_transaction_rolls_it_back() {
    let directory = TestDirectory::new("close-open");
    let db = with_schema(&directory.database());
    begin(db).unwrap();
    tx_sql(
        db,
        "INSERT INTO events (id, kind) VALUES (1, 'uncommitted')",
        json!([]),
    )
    .unwrap();
    tx_sql(
        db,
        "UPDATE projection SET total = 7 WHERE id = 1",
        json!([]),
    )
    .unwrap();
    unsafe { hawdb_close(db) };

    let db = open(&directory.database(), "{}");
    assert_eq!(rows(sql(db, "SELECT COUNT(*) FROM events")), json!([[0]]));
    assert_eq!(
        rows(sql(db, "SELECT total FROM projection WHERE id = 1")),
        json!([[0]])
    );
    // The reopened handle starts without a transaction.
    assert_eq!(kind(commit(db)), "no_transaction");
    begin(db).unwrap();
    rollback(db).unwrap();
    unsafe { hawdb_close(db) };
}

#[test]
fn a_read_only_handle_cannot_commit_writes() {
    let directory = TestDirectory::new("read-only");
    unsafe { hawdb_close(with_schema(&directory.database())) };
    let db = open(&directory.database(), r#"{"read_only":true}"#);
    let refused = begin(db).and_then(|_| {
        tx_sql(
            db,
            "INSERT INTO events (id, kind) VALUES (1, 'no')",
            json!([]),
        )
        .and_then(|_| commit(db))
    });
    assert!(refused.is_err(), "read-only transaction wrote: {refused:?}");
    let _ = rollback(db);
    assert_eq!(rows(sql(db, "SELECT COUNT(*) FROM events")), json!([[0]]));
    unsafe { hawdb_close(db) };
}
