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

//! Durable relational commits size checkpoints from maintained row byte
//! totals instead of walking every resident row (nowledge-co/hawdb#1519).

use super::*;
use crate::relational::row_entry_byte_probe;

const TABLE: &str = "accounts";
/// Rows sized by one commit may include one row page split check
/// (`RELATIONAL_ROW_PAGE_MAX_ENTRIES` = 256 entries) plus the changed rows.
/// The resident tables below are many times larger than this bound.
const MAX_ROW_ENTRIES_SIZED_PER_SINGLE_ROW_COMMIT: u64 = 2 * 256 + 64;

fn accounts_schema() -> RelationalTableSchema {
    let column = |name: &str, scalar_type| RelationalColumnSchema {
        name: name.to_string(),
        scalar_type,
        nullable: false,
        default: None,
    };
    RelationalTableSchema {
        materialized_view: None,
        name: TABLE.to_string(),
        columns: vec![
            column("id", RelationalScalarType::BigInt),
            column("balance", RelationalScalarType::BigInt),
            column("note", RelationalScalarType::Text),
        ],
        primary_key: vec!["id".to_string()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    }
}

fn account(id: i64, balance: i64) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::BigInt(id),
        RelationalValue::BigInt(balance),
        RelationalValue::Text(format!("note-{}", "n".repeat((id % 40) as usize))),
    ])
}

fn open(path: &std::path::Path, catalog: &mut Catalog) -> GraphStore {
    GraphStore::open_with_durability_and_replay_config(
        path,
        catalog,
        DurabilityPolicy::SyncOnEveryWrite,
        WalReplayConfig::default(),
    )
    .unwrap()
}

fn commit(store: &mut GraphStore, catalog: &mut Catalog, writes: Vec<RelationalWrite>) {
    store
        .commit_relational_transaction(catalog, RelationalTransaction { writes })
        .unwrap();
}

fn insert_accounts(store: &mut GraphStore, catalog: &mut Catalog, ids: std::ops::Range<i64>) {
    commit(
        store,
        catalog,
        vec![RelationalWrite::Insert {
            table: TABLE.to_string(),
            rows: ids.map(|id| account(id, id)).collect(),
            mode: RelationalInsertMode::Error,
        }],
    );
}

fn update_balance(id: i64) -> RelationalWrite {
    RelationalWrite::UpdateWhere {
        table: TABLE.to_string(),
        assignments: vec![RelationalUpdateAssignment {
            column: "balance".to_string(),
            value: RelationalUpdateValue::Value(RelationalValue::BigInt(-id)),
        }],
        predicate: RelationalPredicate::Compare {
            column: "id".to_string(),
            op: RelationalComparisonOp::Eq,
            value: RelationalValue::BigInt(id),
        },
    }
}

fn assert_row_bytes_exact(store: &GraphStore) -> u64 {
    let state = store.relational_state();
    let estimated = state.estimated_materialized_row_bytes();
    assert_eq!(estimated, state.recomputed_materialized_row_bytes());
    estimated
}

/// Row entries sized on this thread while committing one single-row UPDATE.
fn single_row_update_visits(store: &mut GraphStore, catalog: &mut Catalog, id: i64) -> u64 {
    let before = row_entry_byte_probe::visits();
    commit(store, catalog, vec![update_balance(id)]);
    row_entry_byte_probe::visits() - before
}

#[test]
fn durable_single_row_commit_does_not_walk_resident_rows() {
    let path = unique_test_dir("row-bytes-commit-work");
    let mut catalog = Catalog::default();
    let mut store = open(&path, &mut catalog);
    commit(
        &mut store,
        &mut catalog,
        vec![RelationalWrite::CreateTable(accounts_schema())],
    );
    assert!(store.durable.is_some());
    assert!(store.relational_state().materialized_rows_resident());

    insert_accounts(&mut store, &mut catalog, 0..1_000);
    let small = single_row_update_visits(&mut store, &mut catalog, 500);
    for chunk in 1..8 {
        insert_accounts(&mut store, &mut catalog, chunk * 1_000..(chunk + 1) * 1_000);
    }
    let large = single_row_update_visits(&mut store, &mut catalog, 4_500);
    assert_eq!(store.relational_state().total_row_count(), 8_000);

    // Before #1519 each commit sized every resident row (at least twice:
    // storage pressure and WAL admission), i.e. >= 16,000 visits here.
    assert!(
        small <= MAX_ROW_ENTRIES_SIZED_PER_SINGLE_ROW_COMMIT
            && large <= MAX_ROW_ENTRIES_SIZED_PER_SINGLE_ROW_COMMIT,
        "single-row commit sized {small} row entries at 1,000 rows and {large} at 8,000 rows"
    );

    // Positive control: a resident-row walk on this thread is observable.
    let before = row_entry_byte_probe::visits();
    store
        .relational_state()
        .estimated_checkpoint_admission_bytes_with_visit(&mut || {
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
    assert!(row_entry_byte_probe::visits() - before >= 8_000);
    assert_row_bytes_exact(&store);

    drop(store);
    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn durable_row_bytes_stay_exact_across_rollback_checkpoint_and_reopen() {
    let path = unique_test_dir("row-bytes-recovery");
    let mut catalog = Catalog::default();
    let mut store = open(&path, &mut catalog);
    commit(
        &mut store,
        &mut catalog,
        vec![RelationalWrite::CreateTable(accounts_schema())],
    );
    insert_accounts(&mut store, &mut catalog, 0..700);
    commit(
        &mut store,
        &mut catalog,
        vec![
            update_balance(3),
            RelationalWrite::Upsert {
                table: TABLE.to_string(),
                rows: vec![account(4, 44), account(10_000, 1)],
                conflict_columns: vec!["id".to_string()],
                action: RelationalConflictAction::Update(vec![RelationalUpsertAssignment {
                    column: "note".to_string(),
                    value: RelationalUpsertValue::Value(RelationalValue::Text("u".repeat(2_000))),
                }]),
            },
            RelationalWrite::DeleteByPrimaryKey {
                table: TABLE.to_string(),
                keys: (100..300)
                    .map(|id| RelationalKey(vec![RelationalValue::BigInt(id)]))
                    .collect(),
            },
        ],
    );
    let committed = assert_row_bytes_exact(&store);

    // A failed commit rolls back every write in the transaction.
    assert!(store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![
                    update_balance(5),
                    RelationalWrite::Insert {
                        table: TABLE.to_string(),
                        rows: vec![account(6, 0)],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
        )
        .is_err());
    assert_eq!(assert_row_bytes_exact(&store), committed);

    // WAL replay rebuilds the same total.
    drop(store);
    let mut catalog = Catalog::default();
    let mut store = open(&path, &mut catalog);
    assert!(store.relational_state().materialized_rows_resident());
    assert_eq!(assert_row_bytes_exact(&store), committed);

    // Checkpoint, more WAL, then recovery from checkpoint plus WAL.
    store.checkpoint(&catalog).unwrap();
    assert_row_bytes_exact(&store);
    commit(
        &mut store,
        &mut catalog,
        vec![
            update_balance(7),
            RelationalWrite::DeleteByPrimaryKey {
                table: TABLE.to_string(),
                keys: vec![RelationalKey(vec![RelationalValue::BigInt(8)])],
            },
        ],
    );
    let after_checkpoint = assert_row_bytes_exact(&store);
    let checkpoint_bytes = store.relational_state().estimated_checkpoint_bytes();
    drop(store);
    let mut catalog = Catalog::default();
    let store = open(&path, &mut catalog);
    assert!(store.relational_state().materialized_rows_resident());
    assert_eq!(assert_row_bytes_exact(&store), after_checkpoint);
    assert_eq!(
        store.relational_state().estimated_checkpoint_bytes(),
        checkpoint_bytes
    );

    drop(store);
    let _ = std::fs::remove_dir_all(path);
}
