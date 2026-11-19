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

//! Checkpoints whose relational rows are carried by row-page and overflow
//! roots write only the changed generations, not a full-row file (#1737).

use super::*;
use crate::store::{set_checkpoint_failpoint, CheckpointPublishStage};

const TABLE: &str = "accounts";

fn noise(seed: i64, len: usize) -> String {
    let mut state = (seed as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}

/// Every 50th row carries an incompressible overflow value.
fn account(id: i64, round: u64) -> RelationalRow {
    let payload = if id % 50 == 0 {
        format!(
            "large-{round}-{}",
            noise(id + round as i64 * 7919, 6 * 1024)
        )
    } else {
        format!("payload-{id}-{round}-{}", "p".repeat(160))
    };
    upsert_row(id, &format!("handle-{id}"), &payload)
}

fn open_with(path: &std::path::Path, replay: WalReplayConfig) -> (GraphStore, Catalog) {
    let mut catalog = Catalog::default();
    let store = GraphStore::open_with_durability_and_replay_config(
        path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay,
    )
    .unwrap();
    (store, catalog)
}

fn insert(store: &mut GraphStore, catalog: &mut Catalog, ids: std::ops::Range<i64>) {
    store
        .commit_relational_transaction(
            catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: TABLE.to_string(),
                    rows: ids.map(|id| account(id, 0)).collect(),
                    mode: RelationalInsertMode::Error,
                }],
            },
        )
        .unwrap();
}

fn update(store: &mut GraphStore, catalog: &mut Catalog, ids: &[i64], round: u64) {
    store
        .commit_relational_transaction(
            catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::Upsert {
                    table: TABLE.to_string(),
                    rows: ids.iter().map(|id| account(*id, round)).collect(),
                    conflict_columns: vec!["id".to_string()],
                    action: RelationalConflictAction::Update(vec![RelationalUpsertAssignment {
                        column: "payload".to_string(),
                        value: RelationalUpsertValue::ExcludedColumn("payload".to_string()),
                    }]),
                }],
            },
        )
        .unwrap();
}

/// Bytes of the relational artifacts that one checkpoint generation created.
fn relational_generation_bytes(root: &std::path::Path, generation: u64) -> u64 {
    let mut bytes = 0;
    for entry in std::fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        let owned = [
            format!("relational-row-pages-{generation}."),
            format!("relational-row-root-{generation}."),
            format!("relational-overflow-{generation}."),
            format!("relational-overflow-root-{generation}."),
            format!("relational.{generation}.hawdb"),
        ];
        if owned.iter().any(|prefix| name.starts_with(prefix.as_str())) {
            bytes += entry.metadata().unwrap().len();
        }
    }
    bytes
}

fn legacy_relational_files(root: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| {
            name.strip_prefix("relational.")
                .and_then(|rest| rest.strip_suffix(".hawdb"))
                .is_some_and(|generation| generation.parse::<u64>().is_ok())
        })
        .collect()
}

fn checkpoint_generation(store: &GraphStore) -> u64 {
    store.durable.as_ref().unwrap().checkpoint_epoch
}

fn assert_accounts(store: &GraphStore, expected: &BTreeMap<i64, RelationalRow>) {
    assert_eq!(store.relational_state().row_count(TABLE), expected.len());
    for (id, row) in expected {
        let actual = store
            .relational_state()
            .hydrate_row(TABLE, &key(*id), &mut RelationalHydrationBudget::default())
            .unwrap()
            .unwrap_or_else(|| panic!("row {id} is missing"));
        assert!(actual == *row, "row {id} differs");
    }
}

fn expected_rows(ids: std::ops::Range<i64>) -> BTreeMap<i64, RelationalRow> {
    ids.map(|id| (id, account(id, 0))).collect()
}

fn seed(path: &std::path::Path, rows: i64) -> (GraphStore, Catalog) {
    let (mut store, mut catalog) = open_with(path, WalReplayConfig::default());
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(upsert_schema())],
            },
        )
        .unwrap();
    insert(&mut store, &mut catalog, 0..rows);
    store.checkpoint(&catalog).unwrap();
    (store, catalog)
}

#[test]
fn row_root_decode_enforces_checkpoint_value_limits() {
    let path = unique_test_dir("checkpoint-writes-row-root-decode-limits");
    let (mut store, mut catalog) = open_with(&path, WalReplayConfig::default());
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(schema())],
            },
        )
        .unwrap();
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![row(1, "short"), row(2, &"x".repeat(128))],
                    mode: RelationalInsertMode::Error,
                }],
            },
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();

    let durable = store.durable.as_ref().unwrap();
    let overflow = durable.open_bound_relational_overflow().unwrap();
    let rows = durable.open_bound_relational_row_pages(&overflow).unwrap();
    let decode = |limits| {
        hawdb_storage::relational::decode_relational_checkpoint_from_row_root(
            durable.root_path(),
            &rows,
            &overflow,
            limits,
            hawdb_storage::relational::RelationalCheckpointIndexLoad::MaterializedPostings,
        )
    };

    let mut limits = hawdb_storage::relational::RelationalDecodeLimits::checkpoint();
    limits.max_values = 2;
    assert!(matches!(decode(limits), Err(RelationalError::Admission(_))));

    limits.max_values = 4;
    decode(limits).expect("four row values fit the exact limit");

    limits.max_value_bytes = 64;
    assert!(matches!(
        decode(limits),
        Err(RelationalError::Admission(message)) if message.contains("bytes")
    ));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn small_change_checkpoint_writes_relational_bytes_proportional_to_the_change() {
    let path = unique_test_dir("checkpoint-writes-small-change");
    let (mut store, mut catalog) = seed(&path, 4_000);
    let root = store.durable.as_ref().unwrap().root_path().to_path_buf();
    let full = relational_generation_bytes(&root, checkpoint_generation(&store));
    assert!(legacy_relational_files(&root).is_empty());

    let mut small = Vec::new();
    for round in 1..=3 {
        update(&mut store, &mut catalog, &[17, 18, 19], round);
        store.checkpoint(&catalog).unwrap();
        assert!(legacy_relational_files(&root).is_empty());
        small.push(relational_generation_bytes(
            &root,
            checkpoint_generation(&store),
        ));
    }
    // Doubling the table must not grow the cost of the same small change by
    // more than its page-directory metadata.
    insert(&mut store, &mut catalog, 4_000..8_000);
    store.checkpoint(&catalog).unwrap();
    let doubled = relational_generation_bytes(&root, checkpoint_generation(&store));
    update(&mut store, &mut catalog, &[17, 18, 19], 4);
    store.checkpoint(&catalog).unwrap();
    let small_after_doubling = relational_generation_bytes(&root, checkpoint_generation(&store));
    eprintln!(
        "relational checkpoint bytes: full={full} small={small:?} doubled_insert={doubled} small_after_doubling={small_after_doubling}"
    );
    for bytes in &small {
        assert!(
            *bytes * 10 < full,
            "a three-row checkpoint wrote {bytes} relational bytes; the full state is {full}"
        );
    }
    assert!(
        small_after_doubling * 10 < full * 2,
        "a three-row checkpoint after doubling wrote {small_after_doubling} bytes"
    );
    assert!(
        small_after_doubling < small[2] + full / 10,
        "small-change cost grew with table size: {} -> {small_after_doubling}",
        small[2]
    );

    let mut expected = expected_rows(0..8_000);
    for id in [17, 18, 19] {
        expected.insert(id, account(id, 4));
    }
    assert_accounts(&store, &expected);
    drop(store);
    for replay in [
        WalReplayConfig::default(),
        WalReplayConfig {
            residency_mode: StorageResidencyMode::Materialized,
            ..WalReplayConfig::default()
        },
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            relational_index_mode: RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        },
    ] {
        let (store, _) = open_with(&path, replay);
        assert!(store.relational_state().materialized_rows_resident());
        assert!(!store.relational_state().canonical_row_metadata_only());
        assert_eq!(
            store
                .relational_state()
                .file_backed_overflow_segment_count(),
            8_000 / 50
        );
        assert_accounts(&store, &expected);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn reopened_row_root_state_rebuilds_indexes_and_accepts_writes() {
    let path = unique_test_dir("checkpoint-writes-reopen-indexes");
    let (store, _) = seed(&path, 300);
    drop(store);
    let (mut store, mut catalog) = open_with(&path, WalReplayConfig::default());
    assert!(store
        .relational_state()
        .materialized_index_postings_resident());
    // The unique handle index was rebuilt from the row root.
    let duplicate = store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: TABLE.to_string(),
                    rows: vec![upsert_row(10_000, "handle-7", "duplicate handle")],
                    mode: RelationalInsertMode::Error,
                }],
            },
        )
        .unwrap_err();
    assert!(duplicate.to_string().contains("unique"), "{duplicate}");
    update(&mut store, &mut catalog, &[50, 100], 9);
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::DeleteByPrimaryKey {
                    table: TABLE.to_string(),
                    keys: vec![key(150)],
                }],
            },
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    drop(store);
    let (store, _) = open_with(&path, WalReplayConfig::default());
    let mut expected = expected_rows(0..300);
    expected.insert(50, account(50, 9));
    expected.insert(100, account(100, 9));
    expected.remove(&150);
    assert_accounts(&store, &expected);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn interrupted_incremental_relational_checkpoint_recovers_every_committed_row() {
    for stage in [
        CheckpointPublishStage::CheckpointPersisted,
        CheckpointPublishStage::WalPrepared,
        CheckpointPublishStage::ManifestPublished,
    ] {
        let path = unique_test_dir(&format!("checkpoint-writes-interrupted-{stage:?}"));
        let (mut store, mut catalog) = seed(&path, 500);
        update(&mut store, &mut catalog, &[0, 1, 250], 3);
        insert(&mut store, &mut catalog, 500..520);
        set_checkpoint_failpoint(Some(stage));
        let error = store.checkpoint(&catalog).unwrap_err();
        set_checkpoint_failpoint(None);
        assert!(error.to_string().contains("injected checkpoint failure"));
        drop(store);

        let mut expected = expected_rows(0..520);
        for id in [0, 1, 250] {
            expected.insert(id, account(id, 3));
        }
        let (mut store, mut catalog) = open_with(&path, WalReplayConfig::default());
        assert_accounts(&store, &expected);
        // The recovered handle publishes the next incremental generation.
        update(&mut store, &mut catalog, &[2], 4);
        expected.insert(2, account(2, 4));
        store.checkpoint(&catalog).unwrap();
        let root = store.durable.as_ref().unwrap().root_path().to_path_buf();
        assert!(legacy_relational_files(&root).is_empty(), "{stage:?}");
        drop(store);
        let (store, _) = open_with(&path, WalReplayConfig::default());
        assert_accounts(&store, &expected);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn legacy_full_row_checkpoint_reopens_and_moves_to_incremental_checkpoints() {
    let path = unique_test_dir("checkpoint-writes-legacy-upgrade");
    {
        // The explicit downgrade option writes exactly the earlier format.
        let (mut store, mut catalog) = open_with(&path, WalReplayConfig::default());
        store.set_legacy_relational_checkpoint_file_enabled(true);
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::CreateTable(upsert_schema())],
                },
            )
            .unwrap();
        insert(&mut store, &mut catalog, 0..400);
        store.checkpoint(&catalog).unwrap();
        let root = store.durable.as_ref().unwrap().root_path().to_path_buf();
        assert_eq!(
            legacy_relational_files(&root),
            vec![format!(
                "relational.{}.hawdb",
                checkpoint_generation(&store)
            )]
        );
        // Earlier releases leave a WAL tail after their full-row checkpoint.
        update(&mut store, &mut catalog, &[50, 51], 1);
    }
    let mut expected = expected_rows(0..400);
    expected.insert(50, account(50, 1));
    expected.insert(51, account(51, 1));
    let (mut store, mut catalog) = open_with(&path, WalReplayConfig::default());
    let root = store.durable.as_ref().unwrap().root_path().to_path_buf();
    assert_eq!(
        store
            .relational_state()
            .file_backed_overflow_segment_count(),
        400 / 50 - 1
    );
    assert_accounts(&store, &expected);
    // The first incremental checkpoint stops writing the full-row file and
    // rebinds resident overflow values to the published overflow extents.
    let legacy = legacy_relational_files(&root);
    update(&mut store, &mut catalog, &[100], 2);
    expected.insert(100, account(100, 2));
    store.checkpoint(&catalog).unwrap();
    assert_eq!(
        legacy_relational_files(&root),
        legacy,
        "no new full-row file"
    );
    assert_eq!(
        store
            .relational_state()
            .file_backed_overflow_segment_count(),
        400 / 50
    );
    assert_accounts(&store, &expected);
    // Once generation reclamation removes the old full-row file, the same
    // session still reads every overflow value.
    update(&mut store, &mut catalog, &[150], 3);
    expected.insert(150, account(150, 3));
    store.checkpoint(&catalog).unwrap();
    assert!(legacy_relational_files(&root).is_empty());
    assert_accounts(&store, &expected);
    drop(store);
    let (store, _) = open_with(&path, WalReplayConfig::default());
    assert_accounts(&store, &expected);
    std::fs::remove_dir_all(path).unwrap();
}
