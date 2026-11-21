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

/// Written by the #1737 change (nowledge-co/hawdb#1778, head `17499379f`)
/// with the default configuration: one `docs` table with 20 rows and one
/// checkpoint, so no full-row file and no relational index generation. This
/// v2 row-page fixture also exercises legacy schema decoding after v3 added
/// partial-index predicates.
/// Packed in `property_defaults::unpack_fixture`'s bundle format.
const ROW_ROOT_FIXTURE: &[u8] =
    include_bytes!("../../../tests/fixtures/row_root_relational_checkpoint.bundle");

/// The same v2 layout written with `Shadow` relational indexes (#1778, head
/// `17499379f`, 20 `docs` rows, one checkpoint), so it binds a persistent
/// relational index generation. Its index pages are mostly zero padding, so
/// the bundle is zstd-compressed.
const SHADOW_INDEX_FIXTURE: &[u8] =
    include_bytes!("../../../tests/fixtures/row_root_shadow_index_checkpoint.bundle.zst");

/// Written by the same v2 writer at `17499379f`, but with the declared
/// secondary index `docs_body_idx`. This pins the legacy full-index schema
/// digest shared by the row root and its bound index generation.
const NAMED_SHADOW_INDEX_FIXTURE: &[u8] =
    include_bytes!("../../../tests/fixtures/row_root_shadow_named_index_v2.bundle.zst");

/// The v3 relational writer at `761d5b3ed` checkpointed the `docs` rows
/// before committing a separate `CREATE INDEX docs_body_idx` transaction.
const STANDALONE_CREATE_INDEX_WAL_V3: &[u8] =
    include_bytes!("../../../tests/fixtures/standalone_create_index_wal_v3.bundle.zst");

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
fn newer_row_root_layout_is_a_typed_refusal_in_every_configuration() {
    use RelationalIndexMode as Index;
    use StorageResidencyMode as Residency;
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
                let error = Database::open_with_config(&path, config)
                    .and_then(|mut db| db.query_sql("SELECT COUNT(*) AS n FROM docs"))
                    .unwrap_err();
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
                let expected = if residency == Residency::OutOfCore && index == Index::Authoritative
                {
                    "without a persistent relational index generation"
                } else {
                    "without a full-row relational checkpoint file"
                };
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

#[test]
fn v2_row_root_with_named_index_reopens_read_only_and_writable() {
    let bundle = zstd::decode_all(NAMED_SHADOW_INDEX_FIXTURE).unwrap();
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
    let path = unique_test_dir("relational_checkpoint_v2_named_index");
    super::property_defaults::unpack_fixture(&bundle, &path);

    {
        let mut db = Database::open_with_config(&path, config(true)).unwrap();
        let schema = db
            .runtime
            .get()
            .unwrap()
            .store
            .relational_state()
            .table_schema("docs")
            .unwrap()
            .clone();
        assert!(schema.indexes.iter().any(|index| {
            index.name == "docs_body_idx" && index.columns == ["body"] && index.predicate.is_none()
        }));
        let rows = db
            .query_sql("SELECT id FROM docs WHERE body = 'body 7'")
            .unwrap();
        assert_eq!(rows.rows[0]["id"], Value::Int(7));
    }
    {
        let mut db = Database::open_with_config(&path, config(false)).unwrap();
        db.query_sql("INSERT INTO docs (id, body) VALUES (20, 'body 20')")
            .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let mut db = Database::open_with_config(&path, config(false)).unwrap();
        let rows = db
            .query_sql("SELECT id FROM docs WHERE body = 'body 20'")
            .unwrap();
        assert_eq!(rows.rows[0]["id"], Value::Int(20));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn v3_wal_replays_standalone_create_index_after_checkpoint() {
    let bundle = zstd::decode_all(STANDALONE_CREATE_INDEX_WAL_V3).unwrap();
    let path = unique_test_dir("standalone-create-index-wal-v3");
    super::property_defaults::unpack_fixture(&bundle, &path);
    use hawdb_storage::relational::{RelationalKey, RelationalValue};
    use hawdb_storage::schema::Catalog;
    use hawdb_storage::store::GraphStore;

    let store = GraphStore::open(&path, &mut Catalog::default()).unwrap();
    let report = store.storage_recovery_report();
    assert_eq!(report.replayed_wal_entries, 1);
    assert_eq!(report.checkpoint_commit_epoch, Some(1));
    assert_eq!(report.recovered_commit_epoch, 2);
    let state = store.relational_state();
    let schema = state.table_schema("docs").unwrap();
    assert!(schema.indexes.iter().any(|index| {
        index.name == "docs_body_idx" && index.columns == ["body"] && index.predicate.is_none()
    }));
    assert_eq!(state.row_count("docs"), 20);
    let lookup = RelationalKey(vec![RelationalValue::Text("body 7".into())]);
    let postings = state
        .index_lookup("docs", "docs_body_idx", &lookup)
        .unwrap();
    assert_eq!(
        postings.iter().cloned().collect::<Vec<_>>(),
        vec![RelationalKey(vec![RelationalValue::BigInt(7)])],
    );
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
