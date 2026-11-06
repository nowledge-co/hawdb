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

//! Row-page dirty budget and required schema checkpoints (#1593).
//!
//! The dirty budget charges encoded page bytes, not the 1 MiB v1 slot, and a
//! resource or budget refusal of a required schema checkpoint leaves it
//! pending instead of poisoning the handle. Budgets are forced small through
//! the test-only publication override so the cases stay a few thousand rows;
//! the 131k/300k-row production cases are measured with the release harness.

use super::*;
use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};
use hawdb_storage::relational::RelationalRowPagePublicationConfig;
use hawdb_storage::store::relational_row_pages::set_relational_row_publication_config_override_for_test;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

/// Twelve 256-row pages: 12 MiB of v1 slots, about 0.7 MiB encoded.
const ROWS: i64 = 3_000;
const BODY_BYTES: usize = 180;

fn config(residency: StorageResidencyMode, index: RelationalIndexMode) -> DatabaseConfig {
    DatabaseConfig {
        storage_residency_mode: residency,
        relational_index_mode: index,
        automatic_checkpoint_max_age: std::time::Duration::from_secs(3600),
        ..Default::default()
    }
}

fn resident() -> DatabaseConfig {
    config(
        StorageResidencyMode::Materialized,
        RelationalIndexMode::Materialized,
    )
}

/// Resident rows that also publish persistent index generations, which an
/// out-of-core reopen with authoritative indexes requires.
fn resident_shadow() -> DatabaseConfig {
    config(
        StorageResidencyMode::Materialized,
        RelationalIndexMode::Shadow,
    )
}

fn out_of_core() -> DatabaseConfig {
    config(
        StorageResidencyMode::OutOfCore,
        RelationalIndexMode::Authoritative,
    )
}

/// Restricts the canonical checkpoint of `path` to `bytes` encoded dirty
/// bytes for the lifetime of the guard.
struct BudgetOverride(PathBuf);

impl BudgetOverride {
    fn new(path: &Path, bytes: u64) -> Self {
        let config = RelationalRowPagePublicationConfig {
            max_dirty_bytes: NonZeroU64::new(bytes).unwrap(),
            ..Default::default()
        };
        set_relational_row_publication_config_override_for_test(path, Some(config));
        Self(path.to_path_buf())
    }
}

impl Drop for BudgetOverride {
    fn drop(&mut self) {
        set_relational_row_publication_config_override_for_test(&self.0, None);
    }
}

fn load(db: &mut Database, table: &str, rows: i64) {
    db.query_sql(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, body TEXT)"
    ))
    .unwrap();
    for start in (0..rows).step_by(500) {
        let values = (start..(start + 500).min(rows))
            .map(|id| format!("({id}, '{}')", "x".repeat(BODY_BYTES)))
            .collect::<Vec<_>>()
            .join(", ");
        db.query_sql(&format!("INSERT INTO {table} (id, body) VALUES {values}"))
            .unwrap();
    }
}

fn int(db: &mut Database, sql: &str) -> i64 {
    match *db.query_sql(sql).unwrap().rows[0].values().next().unwrap() {
        Value::Int(value) => value,
        ref other => panic!("{sql}: {other:?}"),
    }
}

fn required(db: &Database) -> bool {
    db.runtime
        .get()
        .unwrap()
        .store
        .relational_row_schema_checkpoint_required()
}

fn deferred(db: &Database) -> Option<String> {
    db.runtime
        .get()
        .unwrap()
        .store
        .relational_row_schema_checkpoint_deferred()
        .map(str::to_string)
}

/// Reads and writes through an ordinary handle, then checks the totals.
fn exercise(db: &mut Database, expected_t0: i64) {
    assert_eq!(int(db, "SELECT count(*) FROM t0"), expected_t0);
    db.query_sql("INSERT INTO t1 (id) VALUES (1)").unwrap();
    db.query_sql("UPDATE t0 SET body = 'updated' WHERE id = 7")
        .unwrap();
    db.query_sql(&format!(
        "INSERT INTO t0 (id, body) VALUES ({}, 'new')",
        expected_t0 + 1_000_000
    ))
    .unwrap();
    assert_eq!(int(db, "SELECT count(*) FROM t0"), expected_t0 + 1);
    assert_eq!(int(db, "SELECT count(*) FROM t1 WHERE id = 1"), 1);
    db.query_sql(&format!(
        "DELETE FROM t0 WHERE id = {}",
        expected_t0 + 1_000_000
    ))
    .unwrap();
    db.query_sql("DELETE FROM t1 WHERE id = 1").unwrap();
}

#[test]
fn schema_rebuild_charges_encoded_page_bytes() {
    let path = unique_test_dir("row_budget_rebuild");
    // Twelve full v1 slots exceed 2 MiB; their encoded pages do not.
    let _budget = BudgetOverride::new(&path, 2 * 1024 * 1024);
    let mut db = Database::open_with_config(&path, resident()).unwrap();
    load(&mut db, "t0", ROWS);
    db.query_sql("CREATE TABLE t1 (id BIGINT PRIMARY KEY)")
        .unwrap();
    // CREATE TABLE writes no existing page; ALTER rewrites all of t0.
    db.query_sql("ALTER TABLE t0 ADD COLUMN extra TEXT")
        .unwrap();
    assert!(!required(&db), "the schema checkpoint must complete");
    exercise(&mut db, ROWS);
    drop(db);

    let mut db = Database::open_with_config(&path, resident()).unwrap();
    assert!(!required(&db));
    exercise(&mut db, ROWS);
}

#[test]
fn spread_update_checkpoint_charges_encoded_page_bytes() {
    let path = unique_test_dir("row_budget_spread_update");
    let _budget = BudgetOverride::new(&path, 2 * 1024 * 1024);
    let mut db = Database::open_with_config(&path, resident()).unwrap();
    load(&mut db, "t0", ROWS);
    db.checkpoint().unwrap();
    // One row in each of the twelve pages, in one transaction.
    let mut tx = db.begin_transaction().unwrap();
    for id in (0..ROWS).step_by(256) {
        tx.query_sql(&format!("UPDATE t0 SET body = 'z' WHERE id = {id}"))
            .unwrap();
    }
    tx.commit().unwrap();
    db.checkpoint().unwrap();
    let updated = (0..ROWS).step_by(256).count() as i64;
    assert_eq!(
        int(&mut db, "SELECT count(*) FROM t0 WHERE body = 'z'"),
        updated
    );
    drop(db);

    let mut db = Database::open_with_config(&path, resident()).unwrap();
    assert_eq!(
        int(&mut db, "SELECT count(*) FROM t0 WHERE body = 'z'"),
        updated
    );
    assert_eq!(int(&mut db, "SELECT count(*) FROM t0"), ROWS);
}

#[test]
fn budget_refusal_defers_the_schema_checkpoint_without_poisoning() {
    let path = unique_test_dir("row_budget_deferred");
    let budget = BudgetOverride::new(&path, 64 * 1024);
    let mut db = Database::open_with_config(&path, resident()).unwrap();
    load(&mut db, "t0", ROWS);

    // The DDL commits; its schema checkpoint does not fit and stays pending.
    db.query_sql("CREATE TABLE t1 (id BIGINT PRIMARY KEY)")
        .unwrap();
    // CREATE TABLE writes no existing page; ALTER rewrites all of t0.
    db.query_sql("ALTER TABLE t0 ADD COLUMN extra TEXT")
        .unwrap();
    assert!(required(&db));
    let reason = deferred(&db).expect("the refused checkpoint is recorded");
    assert!(reason.contains("exceeding limit"), "{reason}");

    // Resident reads and writes keep working, and later commits do not
    // retry or poison.
    exercise(&mut db, ROWS);
    db.query_sql("INSERT INTO t0 (id, body) VALUES (-1, 'after')")
        .unwrap();
    assert_eq!(deferred(&db).as_deref(), Some(reason.as_str()));

    // An explicit checkpoint retries and reports the refusal without
    // poisoning.
    let error = db.checkpoint().unwrap_err();
    assert!(
        !matches!(error, HawDBError::StorageIntegrity(_)),
        "{error:?}"
    );
    assert_eq!(int(&mut db, "SELECT count(*) FROM t0"), ROWS + 1);
    drop(db);

    // Reopen attempts the checkpoint once, still cannot fit it, and opens
    // with the checkpoint pending rather than refusing.
    let mut db = Database::open_with_config(&path, resident()).unwrap();
    assert!(required(&db));
    assert!(deferred(&db).is_some());
    exercise(&mut db, ROWS + 1);
    drop(db);

    // This on-disk state is the one a poisoned handle leaves on main: the
    // last checkpoint precedes the DDL and the WAL holds it. With the
    // default budget, recovery completes the checkpoint.
    drop(budget);
    let mut db = Database::open_with_config(&path, resident()).unwrap();
    assert!(!required(&db), "recovery must complete the checkpoint");
    assert!(deferred(&db).is_none());
    exercise(&mut db, ROWS + 1);
    assert_eq!(int(&mut db, "SELECT count(*) FROM t0 WHERE id = -1"), 1);
    drop(db);

    let mut db = Database::open_with_config(&path, resident()).unwrap();
    assert!(!required(&db));
    exercise(&mut db, ROWS + 1);
}

#[test]
fn out_of_core_reopen_of_a_pending_schema_checkpoint_heals_after_a_resident_reopen() {
    let path = unique_test_dir("row_budget_out_of_core");
    {
        let mut db = Database::open_with_config(&path, resident_shadow()).unwrap();
        load(&mut db, "t0", ROWS);
        db.checkpoint().unwrap();
    }
    // Out-of-core handles reject schema-changing transactions before the
    // WAL, so a pending schema checkpoint comes from a resident handle.
    let budget = BudgetOverride::new(&path, 64 * 1024);
    {
        let mut db = Database::open_with_config(&path, resident_shadow()).unwrap();
        db.query_sql("CREATE TABLE t1 (id BIGINT PRIMARY KEY)")
            .unwrap();
        // CREATE TABLE writes no existing page; ALTER rewrites all of t0.
        db.query_sql("ALTER TABLE t0 ADD COLUMN extra TEXT")
            .unwrap();
        assert!(required(&db));
        assert_eq!(int(&mut db, "SELECT count(*) FROM t0"), ROWS);
    }

    // Out-of-core recovery replays only canonical checkpoints plus
    // authoritative WAL, so it refuses the uncheckpointed schema change with
    // a recovery error, not an integrity failure.
    let error = Database::open_with_config(&path, out_of_core())
        .and_then(|mut db| db.query_sql("SELECT count(*) FROM t0"))
        .unwrap_err();
    assert!(
        !matches!(error, HawDBError::StorageIntegrity(_)),
        "{error:?}"
    );

    // A resident reopen with the default budget completes the checkpoint;
    // out-of-core handles then serve the new schema.
    drop(budget);
    let mut db = Database::open_with_config(&path, resident_shadow()).unwrap();
    assert!(!required(&db));
    exercise(&mut db, ROWS);
    drop(db);
    let mut db = Database::open_with_config(&path, out_of_core()).unwrap();
    assert!(!required(&db));
    assert_eq!(int(&mut db, "SELECT count(*) FROM t0"), ROWS);
    assert_eq!(int(&mut db, "SELECT count(*) FROM t1"), 0);
}
