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
    db.query_sql("CREATE INDEX notes_body ON notes (body)")
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
fn set_writable(path: &Path, writable: bool) {
    use std::os::unix::fs::PermissionsExt;
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

#[cfg(unix)]
fn running_as_root() -> bool {
    std::process::Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|text| text.trim().parse::<u32>().ok())
        == Some(0)
}

#[cfg(unix)]
#[test]
fn read_only_open_serves_reads_from_a_write_protected_copy() {
    if running_as_root() {
        // Root bypasses permission bits, so the copy would not be read-only.
        return;
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

#[test]
fn discarded_read_only_row_replay_refuses_instead_of_serving_checkpoint_rows() {
    for mode in [
        crate::StorageResidencyMode::Materialized,
        crate::StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir("read_only_discarded_row_replay");
        prepare(&path, mode, true);
        let before = snapshot(&path);
        {
            let _discard =
                hawdb_storage::store::relational_row_pages::DiscardReadOnlyRowReplay::arm();
            let mut db = Database::open_with_config(&path, config(mode, true)).unwrap();
            // Graph WAL replay does not depend on the relational row view.
            assert_eq!(
                db.query("MATCH (m:Memory) RETURN count(m) AS c")
                    .unwrap()
                    .rows[0]["c"],
                Value::Int(30),
                "{mode:?}"
            );
            assert_eq!(
                db.query("MATCH (m:Memory {id: 25}) RETURN m.title AS t")
                    .unwrap()
                    .rows[0]["t"],
                Value::String("tail 25".into()),
                "{mode:?}"
            );
            for sql in [
                "SELECT COUNT(*) AS c FROM notes",
                "SELECT id, body FROM notes ORDER BY id",
                "SELECT body FROM notes WHERE id = 25",
                "SELECT body FROM notes WHERE id = 3",
                "SELECT id FROM notes WHERE body = 'tail 25'",
                "SELECT COUNT(*) AS c FROM notes WHERE body = 'note 3'",
            ] {
                let result = db.query_sql(sql);
                assert!(
                    matches!(result, Err(HawDBError::ReadOnly { .. })),
                    "{mode:?} {sql}: {result:?}"
                );
            }
            assert!(db
                .query_sql("SELECT table_name FROM information_schema.tables")
                .is_ok());
        }
        assert_unchanged(&format!("{mode:?} discarded replay"), &before, &path);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn read_only_open_serves_a_missing_artifact_directory_without_creating_it() {
    let path = unique_test_dir("read_only_missing_artifact_directory");
    prepare(&path, crate::StorageResidencyMode::Materialized, false);
    let data = std::fs::read_dir(path.join("branches"))
        .unwrap()
        .map(|entry| entry.unwrap().path().join("data"))
        .find(|data| data.is_dir())
        .expect("branch artifact directory");
    // A copied or never-admitted branch: its artifact directory is absent.
    std::fs::remove_dir_all(&data).unwrap();
    let before = snapshot(&path);
    {
        let mut db = Database::open_with_config(
            &path,
            config(crate::StorageResidencyMode::Materialized, true),
        )
        .unwrap();
        // Checkpoint artifacts are read from the immutable objects.
        assert_eq!(
            db.query("MATCH (m:Memory) RETURN count(m) AS c")
                .unwrap()
                .rows[0]["c"],
            Value::Int(20)
        );
        assert_eq!(
            db.query_sql("SELECT COUNT(*) AS c FROM notes")
                .unwrap()
                .rows[0]["c"],
            Value::Int(20)
        );
    }
    assert!(!data.exists());
    assert_unchanged("missing artifact directory", &before, &path);
    std::fs::remove_dir_all(path).unwrap();
}

/// A persistent search projection beside the graph, as Mem lays them out.
fn prepare_search_projection(path: &Path) {
    let mut index = crate::SearchIndex::open(path).unwrap();
    for id in 0..12_i64 {
        index
            .upsert_projection_row(crate::SearchProjectionRow {
                kind: crate::SearchProjectionKind::Memory,
                external_id: format!("memory-{id}"),
                title: format!("memory {id}"),
                body: format!("shared searchable body {id}"),
                embedding: Some(vec![1.0, id as f32 / 12.0]),
                source_id: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
    }
    index.checkpoint().unwrap();
}

#[test]
fn shadow_read_only_search_and_vector_reads_do_not_mutate_the_root() {
    for mode in [
        crate::StorageResidencyMode::Materialized,
        crate::StorageResidencyMode::OutOfCore,
    ] {
        for wal_tail in [false, true] {
            let case = format!("{mode:?} wal_tail={wal_tail}");
            let parent = unique_test_dir("read_only_search_reads");
            let graph_path = parent.join("graph");
            let search_path = parent.join("search");
            std::fs::create_dir_all(&parent).unwrap();
            prepare(&graph_path, mode, wal_tail);
            prepare_search_projection(&search_path);
            let before = snapshot(&parent);
            {
                let mut options = crate::EmbeddedStoreOpenOptions::with_search_projection(
                    &graph_path,
                    &search_path,
                    crate::GraphMode::ShadowReadOnly,
                );
                options.database_config = Some(config(mode, true));
                let (store, _) = crate::EmbeddedStore::open_with_options(options).unwrap();
                let handle = crate::EmbeddedStoreHandle::new(store).unwrap();
                for request in [
                    crate::SearchCandidateRequest::text("searchable", 3),
                    crate::SearchCandidateRequest::vector(vec![1.0, 0.5], 3),
                    crate::SearchCandidateRequest::hybrid("searchable", vec![1.0, 0.5], 3),
                ] {
                    let output = handle.search_candidates(&request).unwrap();
                    assert!(
                        !output.accept_bounded().hits.is_empty(),
                        "{case}: {request:?}"
                    );
                }
                let rows = handle
                    .with_bounded_read_snapshot(
                        crate::EmbeddedReadSnapshotBudget {
                            max_rows: 16,
                            max_payload_bytes: 64 * 1024,
                        },
                        |snapshot| {
                            let text = snapshot.query_cypher(
                                "CALL text_search($text, topK := 3) YIELD id, score RETURN id",
                                &BTreeMap::from([(
                                    "text".into(),
                                    Value::String("searchable".into()),
                                )]),
                                3,
                            )?;
                            let vector = snapshot.query_cypher(
                                "CALL vector_search($embedding, topK := 3) RETURN id, score",
                                &BTreeMap::from([(
                                    "embedding".into(),
                                    Value::List(vec![Value::Float(1.0), Value::Float(0.5)]),
                                )]),
                                3,
                            )?;
                            let graph = snapshot.query_cypher(
                                "MATCH (m:Memory) RETURN count(m) AS c",
                                &BTreeMap::new(),
                                1,
                            )?;
                            Ok((text.rows.len(), vector.rows.len(), graph.rows))
                        },
                    )
                    .unwrap();
                assert_eq!((rows.0, rows.1), (3, 3), "{case}");
                assert_eq!(
                    rows.2[0]["c"],
                    Value::Int(if wal_tail { 30 } else { 20 }),
                    "{case}"
                );
            }
            assert_unchanged(&case, &before, &parent);
            std::fs::remove_dir_all(parent).unwrap();
        }
    }
}

#[cfg(unix)]
#[test]
fn shadow_read_only_search_reads_work_on_a_write_protected_copy() {
    if running_as_root() {
        return;
    }
    let parent = unique_test_dir("read_only_search_write_protected");
    let graph_path = parent.join("graph");
    let search_path = parent.join("search");
    std::fs::create_dir_all(&parent).unwrap();
    prepare(&graph_path, crate::StorageResidencyMode::Materialized, true);
    prepare_search_projection(&search_path);
    set_writable(&parent, false);
    let before = snapshot(&parent);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut options = crate::EmbeddedStoreOpenOptions::with_search_projection(
            &graph_path,
            &search_path,
            crate::GraphMode::ShadowReadOnly,
        );
        options.database_config = Some(config(crate::StorageResidencyMode::Materialized, true));
        let (store, _) = crate::EmbeddedStore::open_with_options(options).unwrap();
        let handle = crate::EmbeddedStoreHandle::new(store).unwrap();
        for request in [
            crate::SearchCandidateRequest::text("searchable", 3),
            crate::SearchCandidateRequest::vector(vec![1.0, 0.5], 3),
        ] {
            assert!(!handle
                .search_candidates(&request)
                .unwrap()
                .accept_bounded()
                .hits
                .is_empty());
        }
    }));
    let after = snapshot(&parent);
    set_writable(&parent, true);
    if let Err(panic) = result {
        std::fs::remove_dir_all(&parent).unwrap();
        std::panic::resume_unwind(panic);
    }
    assert_eq!(diff(&before, &after), Vec::<String>::new());
    std::fs::remove_dir_all(parent).unwrap();
}

#[test]
fn read_only_branch_with_sealed_wal_intervals_does_not_mutate_the_root() {
    let path = unique_test_dir("read_only_sealed_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query_sql("CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        db.checkpoint().unwrap();
        // Uncheckpointed writes become the child's sealed WAL interval.
        for id in 0..5 {
            db.query(&format!("CREATE (:Memory {{id: {id}}})")).unwrap();
            db.query_sql(&format!("INSERT INTO notes VALUES ({id}, 'note {id}')"))
                .unwrap();
        }
        let main = db
            .describe_branch(crate::BranchSelector::Name("main".into()))
            .unwrap();
        let current = db.current_branch().unwrap().unwrap();
        let child = db
            .create_branch(crate::BranchCreateRequest {
                name: Some("child".into()),
                parent: crate::BranchSelector::Id(main.id),
                expected_source_commit_epoch: current.commit_epoch,
                owner: None,
                idempotency_key: "read-only-sealed-wal".into(),
            })
            .unwrap();
        let head = hawdb_storage::branch_head::read_branch_head(
            &path
                .join("branches")
                .join(child.id.to_string())
                .join("branch.head"),
        )
        .unwrap();
        let objects = hawdb_storage::immutable_object::ImmutableObjectStore::open(
            path.join("branches/objects"),
        )
        .unwrap();
        let root = hawdb_storage::sealed_root::SealedRoot::decode(
            &objects.read(head.sealed_root).unwrap(),
        )
        .unwrap();
        assert!(
            !root.sealed_wals.is_empty(),
            "fixture needs sealed WAL intervals"
        );
    }
    // The child was never admitted writable: it has no artifact directory,
    // and its root carries the parent's tail as sealed WAL intervals.
    let before = snapshot(&path);
    {
        let mut db =
            Database::open_with_config(&path, config(crate::StorageResidencyMode::Auto, true))
                .unwrap();
        db.query_sql("USE BRANCH NAME 'child'").unwrap();
        assert_eq!(
            db.query("MATCH (m:Memory) RETURN count(m) AS c")
                .unwrap()
                .rows[0]["c"],
            Value::Int(5)
        );
        assert_eq!(
            db.query_sql("SELECT COUNT(*) AS c FROM notes")
                .unwrap()
                .rows[0]["c"],
            Value::Int(5)
        );
    }
    assert_unchanged("sealed WAL branch", &before, &path);
    std::fs::remove_dir_all(path).unwrap();
}
