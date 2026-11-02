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

//! A single commit larger than the free live-overlay headroom drains first
//! (#1479).

use super::*;

/// One py-tpcc district: 3,000 orders of ten lines, about 45% of the default
/// resident-byte envelope at 997 bytes per line.
const DISTRICT_LINES: i64 = 30_000;
/// About 59% of the envelope: two of them never fit together, so every one
/// needs a drain first.
const LARGE_DISTRICT_LINES: i64 = 40_000;
/// Large district commits in the repeated-drain test.
const DISTRICTS: i64 = 2;
/// Small commits that fill the overlay to about 67%, just under the 70%
/// trigger, so no drain is running when the district commit arrives.
const FILL_COMMITS: i64 = 18;

fn order_lines_sql(lines: std::ops::Range<i64>) -> String {
    let values = lines.map(order_line).collect::<Vec<_>>().join(", ");
    format!(
        "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, \
         ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES {values}"
    )
}

fn insert_line_range(db: &mut Database, lines: std::ops::Range<i64>) -> Result<()> {
    let mut transaction = db.begin_transaction()?;
    transaction.query_sql(&order_lines_sql(lines))?;
    transaction.commit().map(|_| ())
}

fn completed(db: &Database) -> u64 {
    db.automatic_checkpoint_report()
        .unwrap()
        .unwrap()
        .completed_checkpoints
}

fn row_byte_ratio(db: &Database) -> f64 {
    let live = debt(db).relational_live;
    live.pairs()[1].0 as f64 / DEFAULT_MAX_RELATIONAL_ROW_CHANGE_BYTES as f64
}

/// Fills the serving overlay to just under the drain trigger.
fn fill_below_trigger(db: &mut Database) -> i64 {
    create_order_line(db);
    fill_order_lines(db)
}

/// Schema changes fold the live overlay, so they precede the fill.
fn fill_order_lines(db: &mut Database) -> i64 {
    for commit in 0..FILL_COMMITS {
        insert_order_lines(db, commit).unwrap();
    }
    FILL_COMMITS * ORDER_LINES_PER_COMMIT
}

/// Commits `lines`, retrying only the bounded-wait `BranchBusy` that applies
/// nothing (an unoptimized build can exceed it between builder work units).
fn commit_with_busy_retries(
    db: &mut Database,
    lines: std::ops::Range<i64>,
    retries: &mut usize,
) -> Result<()> {
    loop {
        match insert_line_range(db, lines.clone()) {
            Err(HawDBError::BranchBusy { .. }) if *retries < 100 => *retries += 1,
            result => return result,
        }
    }
}

#[test]
fn district_sized_commits_above_the_free_headroom_drain_first_and_succeed() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    let mut lines = fill_below_trigger(&mut db);
    let initial = completed(&db);
    let filled = row_byte_ratio(&db);
    assert!(
        (0.6..0.7).contains(&filled),
        "fill must stop just below the trigger: {filled}"
    );
    let mut busy_retries = 0;
    // Each commit needs about 59% of the envelope, below the 70% trigger.
    // The first meets the fill; the second meets the first, so both need a
    // drain before they fit. Two commits cover the repeated case; more only
    // add full checkpoints of a growing table to the CI partition. On main
    // the first one (commit 22) is rejected before WAL append with the
    // live-overlay admission error.
    for district in 0..DISTRICTS {
        let before = completed(&db);
        commit_with_busy_retries(
            &mut db,
            lines..lines + LARGE_DISTRICT_LINES,
            &mut busy_retries,
        )
        .unwrap_or_else(|error| {
            panic!(
                "district {district} failed after {busy_retries} busy retries: {error}; \
                     {:?}; {:?}",
                debt(&db).relational_live,
                db.automatic_checkpoint_report()
            )
        });
        lines += LARGE_DISTRICT_LINES;
        assert!(
            completed(&db) > before,
            "district {district} must be admitted after a drain"
        );
        for (retained, limit) in debt(&db).relational_live.admission_pairs() {
            assert!(limit.is_none_or(|limit| retained <= limit));
        }
    }
    let report = db.automatic_checkpoint_report().unwrap().unwrap();
    assert_eq!(report.failed_attempts, 0, "{report:?}");
    assert!(
        report.completed_checkpoints >= initial + DISTRICTS as u64,
        "{report:?}"
    );
    assert_order_lines(&mut db, lines);
    drop(db);
    let mut reopened = Database::open_with_config(&fixture.0, config()).unwrap();
    assert_order_lines(&mut reopened, lines);
    reopened.checkpoint().unwrap();
    assert_order_line_totals(&mut reopened, lines);
}

const WIDE_COLUMNS: usize = 40;

/// Rows of one primary key and forty BIGINT columns: about 2.7 KB each.
fn wide_rows_sql(rows: std::ops::Range<i64>) -> String {
    let columns = (0..WIDE_COLUMNS)
        .map(|column| format!("c{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let values = rows
        .map(|id| {
            let row = (0..WIDE_COLUMNS as i64)
                .map(|column| (id * 31 + column).to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!("({id}, {row})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO wide (id, {columns}) VALUES {values}")
}

#[test]
fn a_commit_larger_than_the_whole_envelope_is_rejected_with_its_size_and_writes_nothing() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    create_order_line(&mut db);
    let columns = (0..WIDE_COLUMNS)
        .map(|column| format!("c{column} BIGINT"))
        .collect::<Vec<_>>()
        .join(", ");
    db.query_sql(&format!(
        "CREATE TABLE wide (id BIGINT PRIMARY KEY, {columns})"
    ))
    .unwrap();
    let lines = fill_order_lines(&mut db);
    let epoch = db.commit_epoch().unwrap();
    let checkpoints = completed(&db);
    // 30,000 wide rows need about 80 MB: more than the 64 MiB envelope even
    // when the overlay is empty, so no drain can make room for them.
    let sql = wide_rows_sql(0..30_000);
    for explicit in [true, false] {
        let error = if explicit {
            let mut transaction = db.begin_transaction().unwrap();
            transaction.query_sql(&sql).unwrap();
            transaction.commit().unwrap_err()
        } else {
            db.query_sql(&sql).unwrap_err()
        };
        let message = error.to_string();
        // The limit and the commit size are both named.
        assert!(
            message.contains("before WAL append")
                && message.contains(&format!(
                    "max_entries={DEFAULT_MAX_RELATIONAL_ROW_CHANGES}, \
                     max_bytes={DEFAULT_MAX_RELATIONAL_ROW_CHANGE_BYTES}"
                ))
                && message.contains("a single commit changes at least")
                && message.contains("split it into smaller commits"),
            "{message}"
        );
        assert!(!db.storage_handle_poisoned().unwrap());
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        // A commit that can never fit does not wait for or force a drain.
        assert_eq!(completed(&db), checkpoints);
    }
    let count = db
        .query_sql("SELECT COUNT(*) AS n FROM wide")
        .unwrap()
        .rows
        .into_rows();
    assert_eq!(count[0].get("n"), Some(&Value::Int(0)));
    assert_order_lines(&mut db, lines);
    // The handle keeps admitting commits that fit.
    insert_order_lines(&mut db, FILL_COMMITS).unwrap();
    let lines = lines + ORDER_LINES_PER_COMMIT;
    drop(db);
    let mut reopened = Database::open_with_config(&fixture.0, config()).unwrap();
    assert_order_lines(&mut reopened, lines);
    reopened.checkpoint().unwrap();
    assert_order_line_totals(&mut reopened, lines);
}

#[test]
fn autocommit_statement_above_the_free_headroom_drains_first() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    let lines = fill_below_trigger(&mut db);
    let before = completed(&db);
    db.query_sql(&order_lines_sql(lines..lines + DISTRICT_LINES))
        .unwrap();
    assert!(completed(&db) > before);
    assert_order_lines(&mut db, lines + DISTRICT_LINES);
}

/// Another writer and a reader keep running while a district commit waits
/// for its drain on a shared concurrent handle.
#[test]
fn concurrent_writer_and_reader_progress_around_a_pre_drained_commit() {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    create_order_line(&mut db);
    db.query_sql("CREATE TABLE ticks (id BIGINT PRIMARY KEY, payload TEXT)")
        .unwrap();
    let lines = fill_order_lines(&mut db);
    let before = completed(&db);
    let db = db.into_concurrent();
    let district_done = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicUsize::new(0));
    let reads_during_district = Arc::new(AtomicUsize::new(0));
    let ticks = 200;
    std::thread::scope(|scope| {
        let district = scope.spawn(|| {
            // Stops the reader even when this commit fails (as on main).
            struct Done<'a>(&'a AtomicBool);
            impl Drop for Done<'_> {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::Release);
                }
            }
            let _done = Done(&district_done);
            let mut retries = 0;
            loop {
                let mut transaction = db
                    .begin_transaction(crate::ConcurrentTransactionOptions::default())
                    .unwrap();
                transaction
                    .query_sql(&order_lines_sql(lines..lines + DISTRICT_LINES))
                    .unwrap();
                match transaction.commit() {
                    Ok(_) => break,
                    Err(HawDBError::BranchBusy { .. }) if retries < 100 => retries += 1,
                    Err(error) => panic!("district commit failed: {error}"),
                }
            }
        });
        let writer = scope.spawn(|| {
            let mut retries = 0;
            let mut id = 0;
            while id < ticks {
                match db.query_sql_with_params(
                    "INSERT INTO ticks (id, payload) VALUES ($1, $2)",
                    &[Value::Int(id), Value::String(format!("tick-{id}"))],
                ) {
                    Ok(_) => id += 1,
                    Err(HawDBError::BranchBusy { .. }) if retries < 100 => retries += 1,
                    Err(error) => panic!("tick {id} failed: {error}"),
                }
            }
        });
        let reader = scope.spawn(|| {
            while !district_done.load(Ordering::Acquire) {
                let rows = db
                    .query_sql(
                        "SELECT ol_i_id FROM order_line WHERE ol_w_id = 1 AND ol_d_id = 1 \
                         AND ol_o_id = 0 AND ol_number = 0",
                    )
                    .unwrap()
                    .rows
                    .into_rows();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("ol_i_id"), Some(&Value::Int(0)));
                reads.fetch_add(1, Ordering::Relaxed);
                if !district_done.load(Ordering::Acquire) {
                    reads_during_district.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        let district = district.join();
        writer.join().unwrap();
        reader.join().unwrap();
        district.unwrap();
    });
    assert!(reads_during_district.load(Ordering::Relaxed) > 0);
    let report = db.automatic_checkpoint_report().unwrap().unwrap();
    assert!(report.completed_checkpoints > before, "{report:?}");
    assert_eq!(report.failed_attempts, 0, "{report:?}");
    let count = db
        .query_sql("SELECT COUNT(*) AS n FROM ticks")
        .unwrap()
        .rows
        .into_rows();
    assert_eq!(count[0].get("n"), Some(&Value::Int(ticks)));
    drop(db);
    let mut reopened = Database::open_with_config(&fixture.0, config()).unwrap();
    let lines = lines + DISTRICT_LINES;
    assert_order_lines(&mut reopened, lines);
    reopened.checkpoint().unwrap();
    assert_order_line_totals(&mut reopened, lines);
}

const PREDRAIN_CHILD_ENV: &str = "HAWDB_TEST_LIVE_OVERLAY_PREDRAIN_CHILD";

#[test]
fn pre_drain_crash_child() {
    if std::env::var_os(PREDRAIN_CHILD_ENV).is_none() {
        return;
    }
    let root = PathBuf::from(std::env::var_os(CRASH_PATH_ENV).expect("crash database path"));
    let point = std::env::var(CRASH_POINT_ENV).expect("crash point");
    let mut db = Database::open_with_config(&root, config()).unwrap();
    // The parent's schema checkpoint left no debt. The fill stays below the
    // trigger, so the first checkpoint this child can publish is the drain a
    // district commit requests before it applies.
    let mut lines = fill_order_lines(&mut db);
    let filled = row_byte_ratio(&db);
    assert!((0.6..0.7).contains(&filled), "{filled}");
    assert_eq!(debt(&db).relational_live.row_recovery_delta_entries, 0);
    std::fs::write(acknowledged_path(&root), lines.to_string()).unwrap();
    for _ in 0..5 {
        insert_line_range(&mut db, lines..lines + DISTRICT_LINES).unwrap();
        lines += DISTRICT_LINES;
        std::fs::write(acknowledged_path(&root), lines.to_string()).unwrap();
    }
    panic!("crash failpoint {point} was not reached by a pre-commit drain");
}

#[test]
fn crash_during_a_pre_commit_drain_recovers_every_acknowledged_commit() {
    for point in [
        "during_checkpoint_publication",
        "after_manifest_publication",
    ] {
        let fixture = Fixture::new();
        {
            let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
            create_order_line(&mut db);
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(
                "api::automatic_checkpoint::tests::relational_live_overlay::large_commit::pre_drain_crash_child",
            )
            .arg("--nocapture")
            .env(PREDRAIN_CHILD_ENV, "1")
            .env(CRASH_PATH_ENV, &fixture.0)
            .env(CRASH_POINT_ENV, point)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "child did not stop at {point}");
        let acknowledged: i64 = std::fs::read_to_string(acknowledged_path(&fixture.0))
            .unwrap()
            .parse()
            .unwrap();
        let _ = std::fs::remove_file(acknowledged_path(&fixture.0));
        // The district waiting for the drain never reached the WAL, so the
        // recovered table holds exactly the acknowledged commits.
        assert_eq!(
            acknowledged,
            FILL_COMMITS * ORDER_LINES_PER_COMMIT,
            "{point}"
        );
        let mut reopened = Database::open_with_config(&fixture.0, config()).unwrap();
        assert_order_lines(&mut reopened, acknowledged);
        // The recovered handle admits the district, draining first when
        // recovery replayed the fill into the live overlay.
        insert_line_range(&mut reopened, acknowledged..acknowledged + DISTRICT_LINES)
            .unwrap_or_else(|error| panic!("district after {point} recovery: {error}"));
        let lines = acknowledged + DISTRICT_LINES;
        assert_order_lines(&mut reopened, lines);
        reopened.checkpoint().unwrap();
        assert_order_line_totals(&mut reopened, lines);
    }
}
