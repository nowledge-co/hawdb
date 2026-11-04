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

//! Scans merge any number of uncheckpointed rows (#1480). Each merged entry
//! is charged to the statement's row budget, not to a fixed entry count.

use super::*;
use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};

/// Rows inserted after the checkpoint: above the former 16,384-entry limit.
const ROWS: i64 = 20_000;
const BATCH: i64 = 4_000;

fn modes() -> [(StorageResidencyMode, RelationalIndexMode); 2] {
    [
        (
            StorageResidencyMode::Materialized,
            RelationalIndexMode::Materialized,
        ),
        (
            StorageResidencyMode::OutOfCore,
            RelationalIndexMode::DemandPaged,
        ),
    ]
}

/// One checkpointed row (`id = 0`), then `ROWS` uncheckpointed rows with
/// `v = id` in commits of `BATCH` rows.
fn overlay_database(
    name: &str,
    residency: StorageResidencyMode,
    index: RelationalIndexMode,
    max_read_result_rows: Option<usize>,
) -> Database {
    let config = DatabaseConfig {
        storage_residency_mode: residency,
        relational_index_mode: index,
        automatic_checkpoint_max_age: std::time::Duration::from_secs(3600),
        max_read_result_rows,
        ..Default::default()
    };
    let mut db = Database::open_with_config(unique_test_dir(name), config).unwrap();
    db.query_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .unwrap();
    db.query_sql("INSERT INTO t VALUES (0, 0)").unwrap();
    db.checkpoint().unwrap();
    for start in (1..=ROWS).step_by(BATCH as usize) {
        let values = (start..start + BATCH)
            .map(|id| format!("({id}, {id})"))
            .collect::<Vec<_>>()
            .join(", ");
        db.query_sql(&format!("INSERT INTO t VALUES {values}"))
            .unwrap();
    }
    db
}

fn int(db: &mut Database, sql: &str) -> i64 {
    match *db.query_sql(sql).unwrap().rows[0].values().next().unwrap() {
        Value::Int(value) => value,
        ref other => panic!("{sql}: {other:?}"),
    }
}

fn ids(db: &mut Database, sql: &str) -> Vec<i64> {
    db.query_sql(sql)
        .unwrap()
        .rows
        .iter()
        .map(|row| match *row.values().next().unwrap() {
            Value::Int(value) => value,
            ref other => panic!("{sql}: {other:?}"),
        })
        .collect()
}

#[test]
fn scans_merge_more_uncheckpointed_rows_than_the_former_entry_limit() {
    for (residency, index) in modes() {
        let mut db = overlay_database("overlay_scans", residency, index, None);
        let sum = ROWS * (ROWS + 1) / 2;
        assert_eq!(
            int(&mut db, "SELECT COUNT(*) FROM t"),
            ROWS + 1,
            "{residency:?}"
        );
        assert_eq!(int(&mut db, "SELECT SUM(v) FROM t"), sum);
        assert_eq!(
            ids(&mut db, "SELECT id FROM t WHERE v >= 0 ORDER BY id LIMIT 3"),
            vec![0, 1, 2]
        );
        assert_eq!(
            ids(&mut db, "SELECT id FROM t ORDER BY v DESC LIMIT 2"),
            vec![ROWS, ROWS - 1]
        );

        // Replacements and deletions in the overlay merge exactly.
        db.query_sql("UPDATE t SET v = v + 1 WHERE id > 0 AND id <= 5000")
            .unwrap();
        db.query_sql("DELETE FROM t WHERE id > 19000 AND id <= 20000")
            .unwrap();
        assert_eq!(int(&mut db, "SELECT COUNT(*) FROM t"), ROWS + 1 - 1000);
        assert_eq!(
            int(&mut db, "SELECT SUM(v) FROM t"),
            sum + 5000 - (19_001..=20_000).sum::<i64>()
        );

        // A transaction scans the same merged view.
        let mut transaction = db.begin_transaction().unwrap();
        let count = transaction
            .query_sql("SELECT COUNT(*) FROM t")
            .unwrap()
            .rows[0]
            .values()
            .next()
            .unwrap()
            .clone();
        assert_eq!(count, Value::Int(ROWS + 1 - 1000));
        transaction.rollback();

        // The checkpointed result is the same.
        db.checkpoint().unwrap();
        assert_eq!(int(&mut db, "SELECT COUNT(*) FROM t"), ROWS + 1 - 1000);
    }
}

/// The merge is still bounded: a statement row limit caps merged entries.
#[test]
fn a_statement_row_limit_still_bounds_merged_uncheckpointed_rows() {
    for (residency, index) in modes() {
        let mut db = overlay_database("overlay_row_limit", residency, index, Some(100));
        let error = db
            .query_sql("SELECT COUNT(*) FROM t")
            .unwrap_err()
            .to_string();
        // The statement stops at its row bound, by the executor's
        // intermediate-row check or by the overlay's share of the row budget.
        assert!(
            error.contains("max_intermediate_rows 100") || error.contains("overlay contains"),
            "{residency:?}: {error}"
        );
        // A bounded read within the limit still succeeds.
        assert_eq!(ids(&mut db, "SELECT id FROM t WHERE id = 7"), vec![7]);
    }
}
