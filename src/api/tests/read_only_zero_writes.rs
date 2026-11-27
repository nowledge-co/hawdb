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

//! A `read_only` open and its reads cause zero filesystem mutations under the
//! database root (#2029).

use super::*;
use std::path::{Path, PathBuf};

/// Recursive file list with sizes and modification times in nanoseconds.
fn snapshot(root: &Path) -> BTreeMap<PathBuf, (bool, u64, i128)> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, (bool, u64, i128)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            let modified = metadata
                .modified()
                .unwrap()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(-1, |duration| duration.as_nanos() as i128);
            out.insert(
                path.strip_prefix(root).unwrap().to_path_buf(),
                (metadata.is_dir(), metadata.len(), modified),
            );
            if metadata.is_dir() {
                walk(root, &path, out);
            }
        }
    }
    let mut out = BTreeMap::new();
    let root_metadata = std::fs::metadata(root).unwrap();
    out.insert(
        PathBuf::new(),
        (
            true,
            0,
            root_metadata
                .modified()
                .unwrap()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i128,
        ),
    );
    walk(root, root, &mut out);
    out
}

fn diff(
    before: &BTreeMap<PathBuf, (bool, u64, i128)>,
    after: &BTreeMap<PathBuf, (bool, u64, i128)>,
) -> Vec<String> {
    let mut changes = Vec::new();
    for (path, value) in before {
        match after.get(path) {
            None => changes.push(format!("deleted {}", path.display())),
            Some(next) if next != value => changes.push(format!(
                "changed {} {:?} -> {:?}",
                path.display(),
                value,
                next
            )),
            Some(_) => {}
        }
    }
    for path in after.keys() {
        if !before.contains_key(path) {
            changes.push(format!("created {}", path.display()));
        }
    }
    changes
}

fn config(mode: crate::StorageResidencyMode, read_only: bool) -> DatabaseConfig {
    DatabaseConfig {
        storage_residency_mode: mode,
        read_only,
        recovery_mode: if read_only {
            crate::RecoveryMode::Strict
        } else {
            crate::RecoveryMode::default()
        },
        ..DatabaseConfig::default()
    }
}

/// Writes graph and relational data. With `wal_tail`, the last writes stay
/// only in the WAL: the handle is dropped without a checkpoint.
fn prepare(path: &Path, mode: crate::StorageResidencyMode, wal_tail: bool) {
    let mut db = Database::open_with_config(path, config(mode, false)).unwrap();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.query_sql("CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    for id in 0..20 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, title: 'memory {id}'}})-[:MENTIONS]->(:Entity {{id: {id}}})"
        ))
        .unwrap();
        db.query_sql(&format!("INSERT INTO notes VALUES ({id}, 'note {id}')"))
            .unwrap();
    }
    db.checkpoint().unwrap();
    if wal_tail {
        for id in 20..30 {
            db.query(&format!(
                "CREATE (:Memory {{id: {id}, title: 'tail {id}'}})"
            ))
            .unwrap();
            db.query_sql(&format!("INSERT INTO notes VALUES ({id}, 'tail {id}')"))
                .unwrap();
        }
    }
    drop(db);
}

/// Graph reads, `information_schema.tables` and a relational `COUNT(*)`.
fn read_everything(db: &mut Database) -> (Value, Vec<String>, Result<Value>) {
    let memories = db
        .query("MATCH (m:Memory) RETURN count(m) AS c")
        .unwrap()
        .rows[0]["c"]
        .clone();
    assert_eq!(
        db.query("MATCH (m:Memory {id: 3})-[:MENTIONS]->(e:Entity) RETURN e.id AS id")
            .unwrap()
            .rows[0]["id"],
        Value::Int(3)
    );
    let tables = db
        .query_sql("SELECT table_name FROM information_schema.tables ORDER BY table_name")
        .unwrap()
        .rows
        .into_iter()
        .map(|row| format!("{:?}", row["table_name"]))
        .collect::<Vec<_>>();
    let notes = db
        .query_sql("SELECT COUNT(*) AS c FROM notes")
        .map(|output| output.rows[0]["c"].clone());
    (memories, tables, notes)
}

fn assert_unchanged(case: &str, before: &BTreeMap<PathBuf, (bool, u64, i128)>, root: &Path) {
    let changes = diff(before, &snapshot(root));
    assert!(changes.is_empty(), "{case}: {changes:#?}");
}

#[test]
fn read_only_open_and_reads_do_not_mutate_the_root() {
    for mode in [
        crate::StorageResidencyMode::Materialized,
        crate::StorageResidencyMode::OutOfCore,
    ] {
        for wal_tail in [false, true] {
            let case = format!("{mode:?} wal_tail={wal_tail}");
            let path = unique_test_dir("read_only_zero_writes");
            prepare(&path, mode, wal_tail);
            let before = snapshot(&path);
            {
                let mut db = Database::open_with_config(&path, config(mode, true)).unwrap();
                let (memories, tables, notes) = read_everything(&mut db);
                // The WAL tail replays in memory for graph and relational reads.
                let expected = Value::Int(if wal_tail { 30 } else { 20 });
                assert_eq!(memories, expected, "{case}");
                assert!(tables.iter().any(|table| table.contains("notes")), "{case}");
                assert_eq!(notes.unwrap(), expected, "{case}");
                // Writes are refused before touching storage.
                assert!(db.query("CREATE (:Memory {id: 99})").is_err(), "{case}");
            }
            assert_unchanged(&case, &before, &path);
            // A second read-only open of the same state stays identical.
            drop(Database::open_with_config(&path, config(mode, true)).unwrap());
            assert_unchanged(&case, &before, &path);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn read_only_open_refuses_automatic_tail_repair_with_a_typed_error() {
    let path = unique_test_dir("read_only_auto_repair");
    prepare(&path, crate::StorageResidencyMode::Materialized, true);
    let before = snapshot(&path);
    let config = DatabaseConfig {
        read_only: true,
        recovery_mode: crate::RecoveryMode::AutoRepairTornTail,
        ..DatabaseConfig::default()
    };
    let error = Database::open_with_config(&path, config)
        .and_then(|mut db| db.query("MATCH (m:Memory) RETURN count(m) AS c"))
        .unwrap_err();
    assert!(
        matches!(error, HawDBError::ReadOnly { operation } if operation.contains("WAL tail repair")),
        "{error:?}"
    );
    assert_unchanged("auto repair", &before, &path);
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(unix)]
#[test]
fn read_only_open_serves_reads_from_a_write_protected_copy() {
    use std::os::unix::fs::PermissionsExt;
    let euid = std::process::Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|text| text.trim().parse::<u32>().ok());
    if euid == Some(0) {
        // Root bypasses permission bits, so the copy would not be read-only.
        return;
    }
    fn set_writable(path: &Path, writable: bool) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                set_writable(&entry.path(), writable);
            }
            let mut permissions = std::fs::metadata(entry.path()).unwrap().permissions();
            let mode = permissions.mode();
            permissions.set_mode(if writable {
                mode | 0o200
            } else {
                mode & !0o222
            });
            std::fs::set_permissions(entry.path(), permissions).unwrap();
        }
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        let mode = permissions.mode();
        permissions.set_mode(if writable {
            mode | 0o200
        } else {
            mode & !0o222
        });
        std::fs::set_permissions(path, permissions).unwrap();
    }
    for mode in [
        crate::StorageResidencyMode::Materialized,
        crate::StorageResidencyMode::OutOfCore,
    ] {
        for wal_tail in [false, true] {
            let path = unique_test_dir("read_only_write_protected");
            prepare(&path, mode, wal_tail);
            set_writable(&path, false);
            let before = snapshot(&path);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut db = Database::open_with_config(&path, config(mode, true)).unwrap();
                let (memories, _tables, notes) = read_everything(&mut db);
                let expected = Value::Int(if wal_tail { 30 } else { 20 });
                assert_eq!(memories, expected);
                assert_eq!(notes.unwrap(), expected);
            }));
            let after = snapshot(&path);
            set_writable(&path, true);
            if let Err(panic) = result {
                std::fs::remove_dir_all(&path).unwrap();
                std::panic::resume_unwind(panic);
            }
            assert_eq!(
                diff(&before, &after),
                Vec::<String>::new(),
                "{mode:?} wal_tail={wal_tail}"
            );
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn shadow_read_only_embedded_graph_does_not_mutate_the_root() {
    let path = unique_test_dir("read_only_embedded_graph");
    prepare(&path, crate::StorageResidencyMode::Auto, true);
    let before = snapshot(&path);
    {
        let mut graph =
            crate::EmbeddedGraph::open(&path, crate::GraphMode::ShadowReadOnly).unwrap();
        assert_eq!(
            graph
                .query("MATCH (m:Memory) RETURN count(m) AS c")
                .unwrap()
                .rows[0]["c"],
            Value::Int(30)
        );
        graph
            .database_mut()
            .query_sql("SELECT table_name FROM information_schema.tables")
            .unwrap();
        assert_eq!(
            graph
                .database_mut()
                .query_sql("SELECT COUNT(*) AS c FROM notes")
                .unwrap()
                .rows[0]["c"],
            Value::Int(30)
        );
    }
    assert_unchanged("embedded shadow read-only", &before, &path);
    std::fs::remove_dir_all(path).unwrap();
}
