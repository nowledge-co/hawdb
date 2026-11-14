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

//! `max_read_intermediate_rows` bounds relational scan work separately from
//! the rows a statement returns (#1679). Unset, it inherits the result-row
//! ceiling, so existing hosts keep the coupled bound.

use super::super::relational_query_limits_with_payload;
use super::*;
use crate::Row;
use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};

/// Checkpointed rows, then uncheckpointed rows, so scans read row pages and
/// the overlay merge.
const CHECKPOINTED_ROWS: i64 = 2_000;
const ROWS: i64 = 3_000;
const BATCH: i64 = 1_000;
const RESULT_ROWS: usize = 100;
const AGGREGATE: &str =
    "SELECT COUNT(*) AS row_count, SUM(OCTET_LENGTH(content)) AS content_bytes FROM content_rows";

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

fn content(id: i64) -> String {
    "m".repeat(16 + (id % 23) as usize)
}

fn content_database(
    name: &str,
    (residency, index): (StorageResidencyMode, RelationalIndexMode),
    max_read_result_rows: Option<usize>,
    max_read_intermediate_rows: Option<usize>,
) -> Database {
    let config = DatabaseConfig {
        storage_residency_mode: residency,
        relational_index_mode: index,
        automatic_checkpoint_max_age: std::time::Duration::from_secs(3600),
        max_read_result_rows,
        max_read_intermediate_rows,
        ..Default::default()
    };
    let mut db = Database::open_with_config(unique_test_dir(name), config).unwrap();
    db.query_sql("CREATE TABLE content_rows (id BIGINT PRIMARY KEY, content TEXT NOT NULL)")
        .unwrap();
    for start in (0..ROWS).step_by(BATCH as usize) {
        if start == CHECKPOINTED_ROWS {
            db.checkpoint().unwrap();
        }
        let values = (start..start + BATCH)
            .map(|id| format!("({id}, '{}')", content(id)))
            .collect::<Vec<_>>()
            .join(", ");
        db.query_sql(&format!(
            "INSERT INTO content_rows (id, content) VALUES {values}"
        ))
        .unwrap();
    }
    db
}

fn expected_aggregate() -> Vec<Row> {
    vec![Row::from([
        ("row_count".into(), Value::Int(ROWS)),
        (
            "content_bytes".into(),
            Value::Int((0..ROWS).map(|id| content(id).len() as i64).sum()),
        ),
    ])]
}

fn assert_scan_refusal(error: &HawDBError, limit: usize, mode: impl std::fmt::Debug) {
    // As in #1480, the scan stops at its intermediate-row bound or at the
    // overlay's share of the row budget derived from it.
    let message = error.to_string();
    assert!(
        message.contains(&format!("max_intermediate_rows {limit}"))
            || message.contains("overlay contains"),
        "{mode:?}: {message}"
    );
}

fn assert_output_refusal(error: &HawDBError, mode: impl std::fmt::Debug) {
    assert!(
        matches!(error, HawDBError::ReadBudgetExceeded(cause)
            if cause.resource == crate::ReadBudgetResource::Rows && cause.limit == RESULT_ROWS),
        "{mode:?}: {error}"
    );
}

#[test]
fn default_intermediate_limit_inherits_the_result_limit() {
    // The regression guard for unchanged hosts: only the result ceiling is
    // set, and a one-row aggregate over more rows is still refused.
    for mode in modes() {
        let mut db = content_database("intermediate_default", mode, Some(RESULT_ROWS), None);
        assert_eq!(db.config().max_read_intermediate_rows, None);
        assert_eq!(db.config().read_intermediate_row_limit(), Some(RESULT_ROWS));
        assert_scan_refusal(&db.query_sql(AGGREGATE).unwrap_err(), RESULT_ROWS, mode);
        let mut session = db.sql_session();
        session.set_max_result_rows(0);
        assert_scan_refusal(
            &session.query(AGGREGATE, &[]).unwrap_err(),
            RESULT_ROWS,
            mode,
        );
        assert_eq!(
            db.query_sql("SELECT id FROM content_rows WHERE id = 7")
                .unwrap()
                .rows,
            vec![Row::from([("id".into(), Value::Int(7))])]
        );
    }
}

#[test]
fn raised_intermediate_limit_admits_the_aggregate_but_output_stays_capped() {
    for mode in modes() {
        let mut db = content_database("intermediate_raised", mode, Some(RESULT_ROWS), Some(10_000));
        assert_eq!(db.query_sql(AGGREGATE).unwrap().rows, expected_aggregate());
        // A session ceiling lowers returned rows, never the scan bound.
        {
            let mut session = db.sql_session();
            session.set_max_result_rows(1);
            assert_eq!(
                session.query(AGGREGATE, &[]).unwrap().output.rows,
                expected_aggregate()
            );
        }
        let mut transaction = db.begin_transaction().unwrap();
        assert_eq!(
            transaction.query_sql(AGGREGATE).unwrap().rows,
            expected_aggregate()
        );
        transaction.rollback();

        // Returning more rows than the result ceiling is still refused, for
        // streaming and blocking (ordered) projections alike.
        for sql in [
            "SELECT id FROM content_rows",
            "SELECT id FROM content_rows ORDER BY content DESC, id",
        ] {
            assert_output_refusal(&db.query_sql(sql).unwrap_err(), mode);
        }
        assert_eq!(
            db.query_sql(&format!("SELECT id FROM content_rows LIMIT {RESULT_ROWS}"))
                .unwrap()
                .rows
                .len(),
            RESULT_ROWS
        );
    }
}

#[test]
fn explicit_intermediate_limit_bounds_scans_without_a_result_limit() {
    for mode in modes() {
        let mut db = content_database("intermediate_explicit", mode, None, Some(500));
        assert_scan_refusal(&db.query_sql(AGGREGATE).unwrap_err(), 500, mode);
        // Zero removes only the intermediate ceiling.
        let mut unbounded = content_database("intermediate_zero", mode, Some(1), Some(0));
        assert_eq!(unbounded.config().read_intermediate_row_limit(), None);
        assert_eq!(
            unbounded.query_sql(AGGREGATE).unwrap().rows,
            expected_aggregate()
        );
    }
}

#[test]
fn intermediate_budgets_derive_from_the_intermediate_limit() {
    let derived = |result: Option<usize>, intermediate: Option<usize>, request: Option<usize>| {
        relational_query_limits_with_payload(
            &DatabaseConfig {
                max_read_result_rows: result,
                max_read_intermediate_rows: intermediate,
                ..DatabaseConfig::default()
            },
            request,
            None,
        )
    };
    // Unset reproduces the coupled limits exactly, including zero/None.
    for result in [None, Some(0), Some(1), Some(4_097)] {
        for request in [None, Some(0), Some(1), Some(64)] {
            let coupled = derived(result, None, request);
            let explicit = derived(
                result,
                Some(result.filter(|rows| *rows != 0).unwrap_or(0)),
                request,
            );
            assert_eq!(
                coupled.max_intermediate_rows,
                result.filter(|rows| *rows != 0).unwrap_or(usize::MAX)
            );
            assert_eq!(coupled, explicit);
        }
    }

    let limits = derived(Some(100), Some(5_000), Some(10));
    // Output takes the minimum of host and request; scans use their own bound.
    assert_eq!(limits.max_output_rows, 10);
    assert_eq!(limits.max_intermediate_rows, 5_000);
    assert_eq!(limits.max_candidate_work, 5_000);
    assert_eq!(limits.index_read.max_rows.get(), 5_000);
    assert_eq!(
        limits.index_read.max_pages.get(),
        5_000 * hawdb_storage::relational::DEFAULT_RELATIONAL_INDEX_READ_TREE_HEIGHT as usize
    );
    assert_eq!(limits.hydration.max_rows, 5_010);
    assert_eq!(limits.row_read.demand.max_rows.get(), 5_010);
    assert_eq!(limits.row_read.max_overlay_entries.get(), 5_010);
    assert_eq!(limits.row_read.demand.max_pages.get(), 5_011);
    assert_eq!(
        limits.row_read.demand.max_bytes.get(),
        5_011 * hawdb_storage::relational::DEFAULT_RELATIONAL_ROW_PAGE_BYTES
    );

    // A lower intermediate ceiling is not raised by a larger result ceiling.
    let lower = derived(Some(5_000), Some(100), None);
    assert_eq!(lower.max_output_rows, 5_000);
    assert_eq!(lower.max_intermediate_rows, 100);
}
