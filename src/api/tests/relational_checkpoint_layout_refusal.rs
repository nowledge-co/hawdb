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

//! A newer relational checkpoint layout (#1737: rows only in the canonical
//! row-page root, no full-row `relational.N.hawdb`) is a typed
//! `UnsupportedStorageFormat` refusal in every configuration this reader
//! cannot serve, never `Storage`, and opening it changes no byte.

use super::*;
use crate::{StorageFormatError, RELATIONAL_CHECKPOINT_LAYOUT_FORMAT};
use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};

/// Writes the current row-page format while preserving this test's original
/// layout distinction: the initial Shadow checkpoint carries the full-row
/// artifact, while an optional successor Authoritative checkpoint publishes
/// only canonical row pages plus its persistent index generation.
fn write_current_row_root(path: &std::path::Path, canonical_only: bool) {
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        relational_index_mode: RelationalIndexMode::Shadow,
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(path, config).unwrap();
        db.query_sql("CREATE TABLE docs (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        for id in 0..20i64 {
            db.query_sql_with_params(
                "INSERT INTO docs (id, body) VALUES ($1, $2)",
                &[Value::Int(id), Value::String(format!("body {id}"))],
            )
            .unwrap();
        }
        db.checkpoint().unwrap();
    }
    if canonical_only {
        let mut db = Database::open_with_config(
            path,
            DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::OutOfCore,
                relational_index_mode: RelationalIndexMode::Authoritative,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        db.checkpoint().unwrap();
    }
}

/// Every durable file below `root`. Lock files and each branch's disposable
/// `runtime/` mount, which writable admission rebuilds from the sealed head
/// before it loads the checkpoint, are not database content.
fn database_bytes(root: &std::path::Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().is_none_or(|name| name != "runtime") {
                    pending.push(path);
                }
            } else if path.extension().is_none_or(|extension| extension != "lock") {
                let bytes = std::fs::read(&path).unwrap();
                files.insert(path.strip_prefix(root).unwrap().to_path_buf(), bytes);
            }
        }
    }
    files
}

fn restore_database_bytes(root: &std::path::Path, files: &BTreeMap<std::path::PathBuf, Vec<u8>>) {
    for (relative, bytes) in files {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().expect("database file has a parent")).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
}

#[test]
fn newer_row_root_layout_is_typed_or_served_by_authoritative_out_of_core() {
    use RelationalIndexMode as Index;
    use StorageResidencyMode as Residency;
    let fixture_path = unique_test_dir("relational_checkpoint_layout_source");
    write_current_row_root(&fixture_path, true);
    let fixture = database_bytes(&fixture_path);
    std::fs::remove_dir_all(fixture_path).unwrap();
    let mut checked = 0;
    for residency in [
        Residency::Auto,
        Residency::Materialized,
        Residency::OutOfCore,
    ] {
        for index in [
            Index::Materialized,
            Index::Shadow,
            Index::DemandPaged,
            Index::Authoritative,
        ] {
            for read_only in [false, true] {
                let path = unique_test_dir("relational_checkpoint_layout_refusal");
                restore_database_bytes(&path, &fixture);
                let before = database_bytes(&path);
                let config = DatabaseConfig {
                    storage_residency_mode: residency,
                    relational_index_mode: index,
                    read_only,
                    recovery_mode: if read_only {
                        crate::RecoveryMode::Strict
                    } else {
                        crate::RecoveryMode::default()
                    },
                    ..DatabaseConfig::default()
                };
                let result = Database::open_with_config(&path, config)
                    .and_then(|mut db| db.query_sql("SELECT COUNT(*) AS n FROM docs"));
                if residency == Residency::OutOfCore && index == Index::Authoritative {
                    assert_eq!(result.unwrap().rows[0]["n"], Value::Int(20));
                    assert_eq!(database_bytes(&path), before);
                    std::fs::remove_dir_all(&path).unwrap();
                    checked += 1;
                    continue;
                }
                let error = result.unwrap_err();
                let context = format!("{residency:?}/{index:?}/read_only={read_only}: {error:?}");
                let HawDBError::UnsupportedStorageFormat(format) = &error else {
                    panic!("expected a typed refusal: {context}");
                };
                assert_eq!(
                    format.format(),
                    RELATIONAL_CHECKPOINT_LAYOUT_FORMAT,
                    "{context}"
                );
                let StorageFormatError::UnsupportedFeature { feature, .. } = format else {
                    panic!("expected an unsupported feature: {context}");
                };
                let expected = "without a full-row relational checkpoint file";
                assert!(feature.contains(expected), "{context}");
                let after = database_bytes(&path);
                let changed = before
                    .keys()
                    .chain(after.keys())
                    .filter(|file| before.get(*file) != after.get(*file))
                    .collect::<std::collections::BTreeSet<_>>();
                assert!(
                    changed.is_empty(),
                    "the refused open changed database bytes {changed:?}: {context}"
                );
                std::fs::remove_dir_all(&path).unwrap();
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 24);
}

/// The one configuration this reader serves for the newer layout:
/// `OutOfCore` residency with `Authoritative` relational indexes, on a
/// database that published a relational index generation.
#[test]
fn newer_row_root_layout_with_index_generation_opens_out_of_core_authoritative() {
    let config = |read_only: bool| DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        relational_index_mode: RelationalIndexMode::Authoritative,
        read_only,
        recovery_mode: if read_only {
            crate::RecoveryMode::Strict
        } else {
            crate::RecoveryMode::default()
        },
        ..DatabaseConfig::default()
    };
    let rows = |db: &mut Database| {
        db.query_sql("SELECT id, body FROM docs ORDER BY id")
            .unwrap()
            .rows
            .iter()
            .map(|row| (row["id"].clone(), row["body"].clone()))
            .collect::<Vec<_>>()
    };
    let expected = (0..20i64)
        .map(|id| (Value::Int(id), Value::String(format!("body {id}"))))
        .collect::<Vec<_>>();

    let path = unique_test_dir("relational_checkpoint_layout_shadow_index");
    write_current_row_root(&path, true);
    {
        let mut db = Database::open_with_config(&path, config(true)).unwrap();
        assert_eq!(rows(&mut db), expected);
    }
    {
        let mut db = Database::open_with_config(&path, config(false)).unwrap();
        assert_eq!(rows(&mut db), expected);
        assert!(db
            .query_sql("INSERT INTO docs (id, body) VALUES (3, 'duplicate')")
            .is_err());
        db.query_sql("INSERT INTO docs (id, body) VALUES (20, 'body 20')")
            .unwrap();
        db.checkpoint().unwrap();
    }
    let mut expected = expected;
    expected.push((Value::Int(20), Value::String("body 20".into())));
    {
        let mut db = Database::open_with_config(&path, config(false)).unwrap();
        assert_eq!(rows(&mut db), expected);
    }
    // Every other configuration still refuses with the typed error.
    let error = Database::open(&path)
        .and_then(|mut db| db.query_sql("SELECT COUNT(*) FROM docs"))
        .unwrap_err();
    assert!(
        matches!(&error, HawDBError::UnsupportedStorageFormat(format)
            if format.format() == RELATIONAL_CHECKPOINT_LAYOUT_FORMAT),
        "{error:?}"
    );
    std::fs::remove_dir_all(&path).unwrap();
}
