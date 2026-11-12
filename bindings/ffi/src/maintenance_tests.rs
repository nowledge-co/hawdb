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
            "hawdb-ffi-maintenance-{name}-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
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
        Ok(result.expect("a successful call returns a report"))
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

fn sql(db: *mut HawdbDatabase, text: &str) -> Outcome {
    let mut result = empty_buffer();
    let mut error = empty_buffer();
    let ok = unsafe {
        hawdb_query_sql(
            db,
            text.as_ptr().cast(),
            text.len(),
            ptr::null(),
            0,
            &mut result,
            &mut error,
        )
    };
    finish(ok, result, error)
}

fn backup(db: *mut HawdbDatabase, destination: &Path) -> Outcome {
    let destination = destination.to_str().unwrap();
    let mut report = empty_buffer();
    let mut error = empty_buffer();
    let ok = unsafe {
        hawdb_backup(
            db,
            destination.as_ptr().cast(),
            destination.len(),
            &mut report,
            &mut error,
        )
    };
    finish(ok, report, error)
}

fn restore(source: &Path, destination: &Path) -> Outcome {
    let (source, destination) = (source.to_str().unwrap(), destination.to_str().unwrap());
    let mut report = empty_buffer();
    let mut error = empty_buffer();
    let ok = unsafe {
        hawdb_restore(
            source.as_ptr().cast(),
            source.len(),
            destination.as_ptr().cast(),
            destination.len(),
            &mut report,
            &mut error,
        )
    };
    finish(ok, report, error)
}

fn scrub(db: *mut HawdbDatabase) -> Outcome {
    let mut report = empty_buffer();
    let mut error = empty_buffer();
    let ok = unsafe { hawdb_scrub(db, &mut report, &mut error) };
    finish(ok, report, error)
}

fn ids(db: *mut HawdbDatabase) -> serde_json::Value {
    sql(db, "SELECT id FROM events ORDER BY id").unwrap()["rows"].clone()
}

/// A populated database whose handle stays open.
fn seeded(path: &Path) -> *mut HawdbDatabase {
    let db = open(path, "{}");
    sql(db, "CREATE TABLE events (id BIGINT PRIMARY KEY, body TEXT)").unwrap();
    let cypher = "CREATE (:Memory {id: 1, title: 'Scrub me'})";
    let mut result = empty_buffer();
    let mut error = empty_buffer();
    let ok = unsafe {
        hawdb_query(
            db,
            cypher.as_ptr().cast(),
            cypher.len(),
            ptr::null(),
            0,
            &mut result,
            &mut error,
        )
    };
    finish(ok, result, error).unwrap();
    for id in 1..=3 {
        sql(
            db,
            &format!("INSERT INTO events (id, body) VALUES ({id}, 'event {id}')"),
        )
        .unwrap();
    }
    db
}

/// Every file below `root`.
fn files(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found
}

/// The canonical graph checkpoint file of the published generation that a
/// handle reads. A backup keeps it beside its storage manifest. An opened
/// project serves it from `branches/<branch>/data/`; opening a restored
/// backup adopts its root files into that layout, leaving the root copies
/// unread, so prefer the branch data copy.
fn canonical_checkpoint(root: &Path) -> PathBuf {
    let files = files(root);
    let generation = files
        .iter()
        .filter(|path| path.file_name().unwrap() == "manifest.hawdb")
        .find_map(|path| {
            let manifest = std::fs::read_to_string(path).unwrap();
            manifest.lines().find_map(|line| {
                let (field, value) = line.split_once('\t')?;
                (field == "checkpoint_generation" && value != "none").then(|| value.to_string())
            })
        })
        .unwrap_or_else(|| panic!("no published generation below {}", root.display()));
    let name = format!("canonical.{generation}.hawdb");
    let mut candidates: Vec<_> = files
        .into_iter()
        .filter(|path| path.file_name().unwrap() == name.as_str())
        .collect();
    candidates.sort_by_key(|path| !path.parent().unwrap().ends_with("data"));
    candidates
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{name} is missing below {}", root.display()))
}

/// Flip one byte in the middle of `path`.
fn flip_byte(path: &Path) {
    let mut bytes = std::fs::read(path).unwrap();
    let offset = bytes.len() / 2;
    bytes[offset] ^= 0xff;
    std::fs::write(path, bytes).unwrap();
}

#[test]
fn backup_while_open_restores_into_a_fresh_directory_and_reopens() {
    let directory = TestDirectory::new("round-trip");
    let db = seeded(&directory.join("database"));

    let backed_up = backup(db, &directory.join("backup")).unwrap();
    for field in ["file_count", "total_bytes"] {
        assert!(backed_up[field].as_u64().unwrap() > 0, "{backed_up}");
    }
    for field in ["generation", "checkpoint_commit_epoch", "manifest_checksum"] {
        assert!(backed_up[field].is_u64(), "{backed_up}");
    }
    // The handle stays usable; later writes are not part of the backup.
    sql(
        db,
        "INSERT INTO events (id, body) VALUES (4, 'after backup')",
    )
    .unwrap();
    assert_eq!(ids(db), json!([[1], [2], [3], [4]]));
    assert!(scrub(db).unwrap()["checked_file_count"].as_u64().unwrap() > 0);

    let restored = restore(&directory.join("backup"), &directory.join("restored")).unwrap();
    assert_eq!(restored, backed_up);
    let copy = open(&directory.join("restored"), "{}");
    assert_eq!(ids(copy), json!([[1], [2], [3]]));
    let healthy = scrub(copy).unwrap();
    assert_eq!(healthy["generation"], backed_up["generation"]);
    assert!(healthy["checked_bytes"].as_u64().unwrap() > 0, "{healthy}");
    // The restored copy is an ordinary writable database.
    sql(
        copy,
        "INSERT INTO events (id, body) VALUES (5, 'in the copy')",
    )
    .unwrap();
    unsafe { hawdb_close(copy) };
    unsafe { hawdb_close(db) };

    let db = open(&directory.join("database"), r#"{"read_only":true}"#);
    assert_eq!(ids(db), json!([[1], [2], [3], [4]]));
    assert!(scrub(db).is_ok(), "read-only handles can be scrubbed");
    unsafe { hawdb_close(db) };
}

#[test]
fn scrub_reports_damage_and_poisons_only_the_damaged_copy() {
    let directory = TestDirectory::new("scrub-damage");
    let db = seeded(&directory.join("database"));
    backup(db, &directory.join("backup")).unwrap();
    unsafe { hawdb_close(db) };
    restore(&directory.join("backup"), &directory.join("damaged")).unwrap();

    let copy = open(&directory.join("damaged"), "{}");
    assert!(scrub(copy).is_ok());
    flip_byte(&canonical_checkpoint(&directory.join("damaged")));
    let error = scrub(copy).unwrap_err();
    let message = error["message"].as_str().unwrap();
    assert!(
        message.contains("CRC32C mismatch during scrub")
            || message.contains("SHA-256 mismatch during scrub"),
        "{error}"
    );
    assert_ne!(error["kind"], "invalid_argument", "{error}");
    let poisoned = sql(copy, "SELECT id FROM events").unwrap_err();
    assert!(
        poisoned["message"]
            .as_str()
            .unwrap()
            .contains("close and reopen"),
        "{poisoned}"
    );
    unsafe { hawdb_close(copy) };

    // The original is untouched and still verifies.
    let db = open(&directory.join("database"), "{}");
    assert!(scrub(db).is_ok());
    assert_eq!(ids(db), json!([[1], [2], [3]]));
    unsafe { hawdb_close(db) };
}

#[test]
fn restore_verifies_the_backup_before_creating_the_destination() {
    let directory = TestDirectory::new("restore-damage");
    let db = seeded(&directory.join("database"));
    backup(db, &directory.join("backup")).unwrap();
    unsafe { hawdb_close(db) };

    flip_byte(&canonical_checkpoint(&directory.join("backup")));
    let error = restore(&directory.join("backup"), &directory.join("restored")).unwrap_err();
    assert_ne!(error["kind"], "invalid_argument", "{error}");
    assert!(!directory.join("restored").exists());

    let missing = restore(
        &directory.join("no-such-backup"),
        &directory.join("restored"),
    );
    assert!(missing.is_err());
    assert!(!directory.join("restored").exists());
}

#[test]
fn maintenance_refusals_leave_state_unchanged() {
    let directory = TestDirectory::new("refusals");
    let db = seeded(&directory.join("database"));
    std::fs::create_dir(directory.join("occupied")).unwrap();
    let error = backup(db, &directory.join("occupied")).unwrap_err();
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("already exists"),
        "{error}"
    );
    backup(db, &directory.join("backup")).unwrap();
    let error = restore(&directory.join("backup"), &directory.join("occupied")).unwrap_err();
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("already exists"),
        "{error}"
    );
    assert_eq!(
        std::fs::read_dir(directory.join("occupied"))
            .unwrap()
            .count(),
        0
    );
    unsafe { hawdb_close(db) };

    let reader = open(&directory.join("database"), r#"{"read_only":true}"#);
    assert!(backup(reader, &directory.join("from-reader")).is_err());
    assert!(!directory.join("from-reader").exists());
    unsafe { hawdb_close(reader) };

    let memory = unsafe { hawdb_open_in_memory(ptr::null_mut()) };
    assert!(backup(memory, &directory.join("from-memory")).is_err());
    assert!(!directory.join("from-memory").exists());
    assert!(scrub(memory).is_err());
    unsafe { hawdb_close(memory) };

    for outcome in [
        scrub(ptr::null_mut()),
        backup(ptr::null_mut(), &directory.join("null")),
    ] {
        assert_eq!(outcome.unwrap_err()["kind"], "invalid_argument");
    }
    let mut error = empty_buffer();
    let db = open(&directory.join("database"), "{}");
    assert!(!unsafe { hawdb_backup(db, ptr::null(), 0, ptr::null_mut(), &mut error) });
    assert_eq!(
        unsafe { take(&mut error) }.unwrap()["kind"],
        "invalid_argument"
    );
    assert!(!unsafe { hawdb_restore(ptr::null(), 0, ptr::null(), 0, ptr::null_mut(), &mut error) });
    assert_eq!(
        unsafe { take(&mut error) }.unwrap()["kind"],
        "invalid_argument"
    );
    assert_eq!(ids(db), json!([[1], [2], [3]]));
    unsafe { hawdb_close(db) };
}
