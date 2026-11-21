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

//! Row-root-only relational checkpoints rebuild resident rows (#1737).
//! Authoritative indexes still require a published index generation. Every
//! successful or refused open leaves durable database bytes unchanged.

use super::*;
use crate::{StorageFormatError, RELATIONAL_CHECKPOINT_LAYOUT_FORMAT};
use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};

/// Written by the #1737 change (nowledge-co/hawdb#1778, head `17499379f`)
/// with the default configuration: one `docs` table with 20 rows and one
/// checkpoint, so no full-row file and no relational index generation.
/// Packed in `property_defaults::unpack_fixture`'s bundle format.
const ROW_ROOT_FIXTURE: &[u8] =
    include_bytes!("../../../tests/fixtures/row_root_relational_checkpoint.bundle");

/// The same layout written with `Shadow` relational indexes (#1778, head
/// `17499379f`, 20 `docs` rows, one checkpoint), so it binds a persistent
/// relational index generation. Its index pages are mostly zero padding, so
/// the bundle is zstd-compressed.
const SHADOW_INDEX_FIXTURE: &[u8] =
    include_bytes!("../../../tests/fixtures/row_root_shadow_index_checkpoint.bundle.zst");

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

#[test]
fn row_root_layout_preserves_all_configuration_and_refusal_boundaries() {
    use RelationalIndexMode as Index;
    use StorageResidencyMode as Residency;
    let mut checked = 0;
    let mut opened = 0;
    let mut refused = 0;
    let expected = (0..20i64)
        .map(|id| (Value::Int(id), Value::String(format!("body {id}"))))
        .collect::<Vec<_>>();
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
                super::property_defaults::unpack_fixture(ROW_ROOT_FIXTURE, &path);
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
                let context = format!("{residency:?}/{index:?}/read_only={read_only}");
                let result = Database::open_with_config(&path, config)
                    .and_then(|mut db| db.query_sql("SELECT id, body FROM docs ORDER BY id"));
                if index == Index::Authoritative {
                    let error = result.unwrap_err();
                    let HawDBError::UnsupportedStorageFormat(format) = &error else {
                        panic!("expected a typed refusal: {context}: {error:?}");
                    };
                    assert_eq!(
                        format.format(),
                        RELATIONAL_CHECKPOINT_LAYOUT_FORMAT,
                        "{context}"
                    );
                    let StorageFormatError::UnsupportedFeature { feature, .. } = format else {
                        panic!("expected an unsupported feature: {context}");
                    };
                    assert!(
                        feature.contains("without a persistent relational index generation"),
                        "{context}: {error:?}"
                    );
                    refused += 1;
                } else {
                    let rows = result.unwrap_or_else(|error| panic!("{context}: {error:?}"));
                    let actual = rows
                        .rows
                        .iter()
                        .map(|row| (row["id"].clone(), row["body"].clone()))
                        .collect::<Vec<_>>();
                    assert_eq!(actual, expected, "{context}");
                    opened += 1;
                }
                let after = database_bytes(&path);
                let changed = before
                    .keys()
                    .chain(after.keys())
                    .filter(|file| before.get(*file) != after.get(*file))
                    .collect::<std::collections::BTreeSet<_>>();
                assert!(
                    changed.is_empty(),
                    "opening changed database bytes {changed:?}: {context}"
                );
                std::fs::remove_dir_all(&path).unwrap();
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 24);
    assert_eq!(opened, 18);
    assert_eq!(refused, 6);
}

/// Authoritative reads and default resident reconstruction share the same
/// checkpoint after an authoritative writer publishes another generation.
#[test]
fn newer_row_root_layout_with_index_generation_opens_out_of_core_authoritative() {
    let bundle = zstd::decode_all(SHADOW_INDEX_FIXTURE).unwrap();
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
    super::property_defaults::unpack_fixture(&bundle, &path);
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
    {
        let mut db = Database::open(&path).unwrap();
        assert_eq!(rows(&mut db), expected);
    }
    std::fs::remove_dir_all(&path).unwrap();
}
