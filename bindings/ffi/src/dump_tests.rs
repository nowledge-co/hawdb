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
            "hawdb-ffi-dump-{name}-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
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

fn open(path: &Path) -> *mut HawdbDatabase {
    let path = path.to_str().unwrap();
    let mut error = empty_buffer();
    let db = unsafe { hawdb_open(path.as_ptr().cast(), path.len(), ptr::null(), 0, &mut error) };
    assert!(!db.is_null(), "{:?}", unsafe { take(&mut error) });
    db
}

fn sql(db: *mut HawdbDatabase, text: &str, params: serde_json::Value) -> serde_json::Value {
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
    assert!(ok, "{text}: {error:?}");
    result.unwrap()
}

fn export(
    db: *mut HawdbDatabase,
    destination: &Path,
) -> Result<serde_json::Value, serde_json::Value> {
    let destination = destination.to_str().unwrap();
    let mut report = empty_buffer();
    let mut error = empty_buffer();
    let ok = unsafe {
        hawdb_export(
            db,
            destination.as_ptr().cast(),
            destination.len(),
            &mut report,
            &mut error,
        )
    };
    let (report, error) = unsafe { (take(&mut report), take(&mut error)) };
    if ok {
        assert!(error.is_none());
        Ok(report.unwrap())
    } else {
        assert!(report.is_none());
        Err(error.unwrap())
    }
}

fn import(source: &Path, destination: &Path) -> Result<serde_json::Value, serde_json::Value> {
    let (source, destination) = (source.to_str().unwrap(), destination.to_str().unwrap());
    let mut report = empty_buffer();
    let mut error = empty_buffer();
    let ok = unsafe {
        hawdb_import(
            source.as_ptr().cast(),
            source.len(),
            destination.as_ptr().cast(),
            destination.len(),
            &mut report,
            &mut error,
        )
    };
    let (report, error) = unsafe { (take(&mut report), take(&mut error)) };
    if ok {
        assert!(error.is_none());
        Ok(report.unwrap())
    } else {
        assert!(report.is_none());
        Err(error.unwrap())
    }
}

#[test]
fn format_versions_match_the_engine() {
    assert_eq!(hawdb_format_version(), hawdb::PERSISTENT_FORMAT_VERSION);
    assert_eq!(
        hawdb_dump_format_version(),
        u32::from(hawdb::LOGICAL_DUMP_FORMAT_VERSION)
    );
}

#[test]
fn export_and_import_round_trip_with_json_reports() {
    let directory = TestDirectory::new("round-trip");
    let db = open(&directory.0.join("source"));
    sql(
        db,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT, blob BYTEA)",
        serde_json::json!([]),
    );
    sql(
        db,
        "INSERT INTO t (id, body, blob) VALUES ($1, $2, $3), ($4, $5, $6)",
        serde_json::json!([
            9223372036854775807i64, "最大", {"$binary": "AP8="},
            -9223372036854775808i64, "最小", null
        ]),
    );
    let dump = directory.0.join("dump");
    let exported = export(db, &dump).unwrap();
    assert_eq!(exported["table_count"], 2);
    assert_eq!(exported["table_row_count"], 3);
    assert_eq!(exported["dump_format_version"], 1);
    assert_eq!(
        exported["persistent_format_version"],
        hawdb_format_version()
    );
    assert_eq!(exported["branch"], "main");
    assert!(exported["manifest_sha256"].as_str().unwrap().len() == 64);
    unsafe { hawdb_close(db) };

    let imported_path = directory.0.join("imported");
    let imported = import(&dump, &imported_path).unwrap();
    assert_eq!(imported["table_row_count"], 3);
    assert_eq!(imported["manifest_sha256"], exported["manifest_sha256"]);
    assert_eq!(imported["source_hawdb_version"], exported["hawdb_version"]);
    let db = open(&imported_path);
    assert_eq!(
        sql(
            db,
            "SELECT id, body, blob FROM t ORDER BY id",
            serde_json::json!([])
        )["rows"],
        serde_json::json!([
            [-9223372036854775808i64, "最小", null],
            [9223372036854775807i64, "最大", {"$binary": "AP8="}]
        ])
    );

    // The destination already exists now.
    let error = import(&dump, &imported_path).unwrap_err();
    assert_eq!(error["kind"], "storage");
    let error = export(db, &dump).unwrap_err();
    assert_eq!(error["kind"], "storage");
    unsafe { hawdb_close(db) };
}

#[test]
fn errors_use_the_engine_kinds() {
    let directory = TestDirectory::new("errors");
    let db = open(&directory.0.join("source"));
    sql(
        db,
        "CREATE TABLE ev (s TEXT NOT NULL, n BIGINT NOT NULL) \
         WITH (storage_mode = 'strict_append', partition_key = 's', order_key = 'n')",
        serde_json::json!([]),
    );
    let destination = directory.0.join("refused");
    let error = export(db, &destination).unwrap_err();
    assert_eq!(error["kind"], "semantic", "{error}");
    assert!(!destination.exists());

    let mut report = empty_buffer();
    let mut error = empty_buffer();
    let ok = unsafe { hawdb_export(ptr::null_mut(), ptr::null(), 0, &mut report, &mut error) };
    assert!(!ok);
    assert_eq!(
        unsafe { take(&mut error) }.unwrap()["kind"],
        "invalid_argument"
    );
    let ok = unsafe { hawdb_export(db, ptr::null(), 0, &mut report, &mut error) };
    assert!(!ok);
    assert_eq!(
        unsafe { take(&mut error) }.unwrap()["kind"],
        "invalid_argument"
    );
    let invalid = [0xffu8, 0xfe];
    let ok = unsafe {
        hawdb_import(
            invalid.as_ptr().cast(),
            invalid.len(),
            invalid.as_ptr().cast(),
            invalid.len(),
            &mut report,
            &mut error,
        )
    };
    assert!(!ok);
    assert_eq!(
        unsafe { take(&mut error) }.unwrap()["kind"],
        "invalid_argument"
    );

    // A dump from a newer HawDB is a typed compatibility refusal.
    let fresh = open(&directory.0.join("fresh"));
    let dump = directory.0.join("dump");
    export(fresh, &dump).unwrap();
    unsafe { hawdb_close(fresh) };
    let manifest = dump.join("manifest.json");
    let text = std::fs::read_to_string(&manifest)
        .unwrap()
        .replace("\"dump_format_version\": 1", "\"dump_format_version\": 999");
    std::fs::write(&manifest, text).unwrap();
    let error = import(&dump, &directory.0.join("newer")).unwrap_err();
    assert_eq!(error["kind"], "unsupported_storage_format", "{error}");
    assert!(error["message"].as_str().unwrap().contains("999"));
    assert!(!directory.0.join("newer").exists());

    // A damaged dump is an integrity failure.
    std::fs::write(&manifest, b"{\"format\": \"something else\"}").unwrap();
    let error = import(&dump, &directory.0.join("damaged")).unwrap_err();
    assert_eq!(error["kind"], "storage_integrity", "{error}");
    unsafe { hawdb_close(db) };
}
