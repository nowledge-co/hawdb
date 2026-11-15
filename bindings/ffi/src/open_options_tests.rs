// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb::{DatabaseConfig, DurabilityPolicy};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "hawdb-ffi-open-options-{name}-{}-{}",
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

fn open(path: &Path, options: &str) -> Result<*mut HawdbDatabase, serde_json::Value> {
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
    if db.is_null() {
        Err(error.expect("a failed open reports an error"))
    } else {
        assert!(error.is_none(), "successful open reported {error:?}");
        Ok(db)
    }
}

fn sql(
    db: *mut HawdbDatabase,
    text: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, serde_json::Value> {
    let params = params.to_string();
    let mut result = empty_buffer();
    let mut error = empty_buffer();
    let ok = unsafe {
        hawdb_query_sql(
            db,
            text.as_ptr().cast(),
            text.len(),
            params.as_ptr().cast(),
            params.len(),
            &mut result,
            &mut error,
        )
    };
    let (result, error) = unsafe { (take(&mut result), take(&mut error)) };
    if ok {
        Ok(result.unwrap())
    } else {
        Err(error.unwrap())
    }
}

fn config(db: *mut HawdbDatabase) -> DatabaseConfig {
    unsafe { &*db }
        .inner
        .lock()
        .unwrap()
        .database()
        .config()
        .clone()
}

fn applied(options: serde_json::Value) -> Result<HawDBEmbeddedOpenOptions, FfiError> {
    let mut open = HawDBEmbeddedOpenOptions::new("unused");
    open_options::apply_open_options(&mut open, options.as_object().unwrap())?;
    Ok(open)
}

#[test]
fn absent_options_keep_power_loss_safe_defaults() {
    let defaults = HawDBEmbeddedOpenOptions::new("unused");
    let open = applied(json!({})).unwrap();
    assert_eq!(open.durability, DurabilityPolicy::SyncOnEveryWrite);
    assert_eq!(open.durability, defaults.durability);
    assert_eq!(open.config, defaults.config);
    assert!(!open.config.relational_monotonic_append_fast_path);
    assert_eq!(open.config.max_read_result_rows, None);
    assert_eq!(open.config.max_read_intermediate_rows, None);
    assert_eq!(
        open.config.max_read_result_payload_bytes,
        Some(hawdb::DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES)
    );
}

#[test]
fn every_option_maps_to_its_typed_field() {
    let open = applied(json!({
        "read_only": false,
        "durability": "sync_on_checkpoint",
        "max_read_result_rows": 10_000,
        "max_read_intermediate_rows": 250_000,
        "max_read_result_payload_bytes": 8_388_608,
        "relational_monotonic_append_fast_path": true,
    }))
    .unwrap();
    assert_eq!(open.durability, DurabilityPolicy::SyncOnCheckpoint);
    assert!(!open.config.read_only);
    assert_eq!(open.config.max_read_result_rows, Some(10_000));
    assert_eq!(open.config.max_read_intermediate_rows, Some(250_000));
    assert_eq!(open.config.read_intermediate_row_limit(), Some(250_000));
    assert_eq!(open.config.max_read_result_payload_bytes, Some(8_388_608));
    assert!(open.config.relational_monotonic_append_fast_path);

    let explicit_default = applied(json!({"durability": "sync_on_every_write"})).unwrap();
    assert_eq!(
        explicit_default.durability,
        DurabilityPolicy::SyncOnEveryWrite
    );

    let read_only = applied(json!({"read_only": true})).unwrap();
    assert!(read_only.config.read_only);
    assert_eq!(read_only.config.recovery_mode, hawdb::RecoveryMode::Strict);

    for unlimited in [json!(null), json!(0)] {
        let open = applied(json!({"max_read_result_rows": unlimited})).unwrap();
        assert_eq!(open.config.read_result_row_limit(), None, "{unlimited}");
    }
    let inherited = applied(json!({
        "max_read_result_rows": 100,
        "max_read_intermediate_rows": null,
    }))
    .unwrap();
    assert_eq!(inherited.config.read_intermediate_row_limit(), Some(100));
    let unbounded = applied(json!({
        "max_read_result_rows": 100,
        "max_read_intermediate_rows": 0,
    }))
    .unwrap();
    assert_eq!(unbounded.config.read_result_row_limit(), Some(100));
    assert_eq!(unbounded.config.read_intermediate_row_limit(), None);
}

#[test]
fn unknown_keys_and_invalid_values_are_rejected() {
    for (options, needle) in [
        (
            json!({"durability": "SyncOnCheckpoint"}),
            "\"durability\" must be",
        ),
        (json!({"durability": "relaxed"}), "\"durability\" must be"),
        (json!({"durability": null}), "\"durability\" must be"),
        (json!({"read_only": 1}), "\"read_only\" must be a bool"),
        (
            json!({"relational_monotonic_append_fast_path": "yes"}),
            "\"relational_monotonic_append_fast_path\" must be a bool",
        ),
        (
            json!({"max_read_result_rows": -1}),
            "\"max_read_result_rows\" must be",
        ),
        (
            json!({"max_read_result_rows": 1.5}),
            "\"max_read_result_rows\" must be",
        ),
        (
            json!({"max_read_result_rows": "10"}),
            "\"max_read_result_rows\" must be",
        ),
        (
            json!({"max_read_intermediate_rows": -1}),
            "\"max_read_intermediate_rows\" must be",
        ),
        (
            json!({"max_read_intermediate_rows": "10"}),
            "\"max_read_intermediate_rows\" must be",
        ),
        (
            json!({"max_read_result_payload_bytes": 0}),
            "\"max_read_result_payload_bytes\" must be a positive integer",
        ),
        (
            json!({"max_read_result_payload_bytes": null}),
            "\"max_read_result_payload_bytes\" must be a positive integer",
        ),
        (
            json!({"monotonic_append_fast_path": true}),
            "unknown open option \"monotonic_append_fast_path\"",
        ),
        (
            json!({"group_commit": true}),
            "unknown open option \"group_commit\"",
        ),
        (
            json!({"readOnly": true}),
            "unknown open option \"readOnly\"",
        ),
    ] {
        let error = applied(options.clone()).err().unwrap_or_else(|| {
            panic!("{options} should be rejected");
        });
        assert_eq!(error.kind, "invalid_argument", "{options}");
        assert!(
            error.message.contains(needle),
            "{options}: {}",
            error.message
        );
    }
}

#[test]
fn rejected_options_fail_the_open_before_touching_the_path() {
    let directory = TestDirectory::new("rejected");
    for options in [
        r#"{"durability":"none"}"#,
        r#"{"unknown":true}"#,
        r#"{"max_read_result_payload_bytes":0}"#,
        r#"[]"#,
    ] {
        let error = open(&directory.database(), options).unwrap_err();
        assert_eq!(error["kind"], "invalid_argument", "{options}: {error}");
        assert!(!directory.0.exists(), "{options} created the database path");
    }
}

#[test]
fn open_options_reach_the_opened_database() {
    let directory = TestDirectory::new("applied");
    let db = open(
        &directory.database(),
        r#"{"durability":"sync_on_checkpoint","max_read_result_rows":2,"max_read_result_payload_bytes":8388608,"relational_monotonic_append_fast_path":true}"#,
    )
    .unwrap();
    let applied = config(db);
    assert_eq!(applied.max_read_result_rows, Some(2));
    assert_eq!(applied.max_read_result_payload_bytes, Some(8_388_608));
    assert!(applied.relational_monotonic_append_fast_path);
    let branch = sql(db, "SHOW CURRENT BRANCH", json!([])).unwrap();
    let durability = branch["columns"]
        .as_array()
        .unwrap()
        .iter()
        .position(|column| column == "durability")
        .unwrap();
    assert_eq!(branch["rows"][0][durability], "sync_on_checkpoint");
    unsafe { hawdb_close(db) };

    // Options are per open: a reopen without options is power-loss safe again.
    let db = open(&directory.database(), "{}").unwrap();
    let branch = sql(db, "SHOW CURRENT BRANCH", json!([])).unwrap();
    assert_eq!(branch["rows"][0][durability], "sync_on_every_write");
    let defaults = config(db);
    assert_eq!(defaults.max_read_result_rows, None);
    assert!(!defaults.relational_monotonic_append_fast_path);
    unsafe { hawdb_close(db) };
}

#[test]
fn read_row_and_payload_budgets_refuse_instead_of_truncating() {
    let directory = TestDirectory::new("budgets");
    let db = open(&directory.database(), "{}").unwrap();
    sql(
        db,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, body TEXT)",
        json!([]),
    )
    .unwrap();
    for id in 1..=3 {
        sql(
            db,
            "INSERT INTO events (id, body) VALUES ($1, $2)",
            json!([id, "x".repeat(4096)]),
        )
        .unwrap();
    }
    unsafe { hawdb_close(db) };

    let db = open(&directory.database(), r#"{"max_read_result_rows":2}"#).unwrap();
    assert_eq!(
        sql(
            db,
            "SELECT id FROM events WHERE id <= 2 ORDER BY id",
            json!([])
        )
        .unwrap()["rows"],
        json!([[1], [2]])
    );
    let error = sql(db, "SELECT id FROM events ORDER BY id", json!([])).unwrap_err();
    assert_eq!(error["kind"], "resource_budget_exceeded", "{error}");
    assert_eq!(error["resource_budget"]["budget"], 2, "{error}");
    unsafe { hawdb_close(db) };

    let db = open(
        &directory.database(),
        r#"{"max_read_result_payload_bytes":1024}"#,
    )
    .unwrap();
    assert_eq!(
        sql(db, "SELECT id FROM events ORDER BY id", json!([])).unwrap()["rows"],
        json!([[1], [2], [3]])
    );
    let error = sql(db, "SELECT body FROM events ORDER BY id", json!([])).unwrap_err();
    assert_eq!(error["kind"], "read_budget_exceeded", "{error}");
    unsafe { hawdb_close(db) };
}

#[test]
fn monotonic_append_fast_path_and_relaxed_durability_commits_survive_reopen() {
    let directory = TestDirectory::new("append");
    let db = open(
        &directory.database(),
        r#"{"durability":"sync_on_checkpoint","relational_monotonic_append_fast_path":true}"#,
    )
    .unwrap();
    sql(
        db,
        "CREATE TABLE log (id BIGINT PRIMARY KEY, body TEXT)",
        json!([]),
    )
    .unwrap();
    for id in 1..=64 {
        sql(
            db,
            "INSERT INTO log (id, body) VALUES ($1, $2)",
            json!([id, format!("event {id}")]),
        )
        .unwrap();
    }
    // Out-of-order and duplicate keys take the ordinary checked path.
    sql(
        db,
        "INSERT INTO log (id, body) VALUES (0, 'early')",
        json!([]),
    )
    .unwrap();
    let duplicate = sql(
        db,
        "INSERT INTO log (id, body) VALUES (7, 'again')",
        json!([]),
    );
    assert!(duplicate.is_err(), "duplicate primary key was accepted");
    unsafe { hawdb_close(db) };

    let db = open(&directory.database(), r#"{"read_only":true}"#).unwrap();
    assert_eq!(
        sql(
            db,
            "SELECT COUNT(*) AS n, MIN(id) AS lo, MAX(id) AS hi FROM log",
            json!([])
        )
        .unwrap()["rows"],
        json!([[65, 0, 64]])
    );
    unsafe { hawdb_close(db) };
}
