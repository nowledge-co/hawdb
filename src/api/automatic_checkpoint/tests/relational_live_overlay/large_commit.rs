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

mod interrupted;

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

/// Lines per INSERT statement. One statement of a whole district exceeds the
/// SQL parser's memo limit, and only the commit's size matters to live
/// admission, so a large commit is staged as several statements.
const LINES_PER_STATEMENT: i64 = 5_000;

/// The INSERT statements that stage `lines` in one transaction.
fn order_line_statements(lines: std::ops::Range<i64>) -> impl Iterator<Item = String> {
    let end = lines.end;
    lines
        .step_by(LINES_PER_STATEMENT as usize)
        .map(move |start| order_lines_sql(start..(start + LINES_PER_STATEMENT).min(end)))
}

fn insert_line_range(db: &mut Database, lines: std::ops::Range<i64>) -> Result<()> {
    let mut transaction = db.begin_transaction()?;
    for sql in order_line_statements(lines) {
        transaction.query_sql(&sql)?;
    }
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

/// Runs one autocommit statement, retrying only the bounded-wait
/// `BranchBusy` that applies nothing (an unoptimized build can exceed it
/// between builder work units).
fn query_with_busy_retries(db: &mut Database, sql: &str) -> Result<()> {
    for _ in 0..100 {
        match db.query_sql(sql) {
            Err(HawDBError::BranchBusy { .. }) => continue,
            result => return result.map(|_| ()),
        }
    }
    db.query_sql(sql).map(|_| ())
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

/// A district commit above the free headroom whose drain keeps failing is
/// rejected with the retryable `BranchBusy`, never the live-view admission
/// error, and its retries re-arm the stopped circuit (#1618). On main the
/// first retry after one failed drain returns the non-retryable error.
#[test]
fn a_district_commit_retries_while_its_drain_fails_and_succeeds_once_the_fault_clears() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    let lines = fill_below_trigger(&mut db);
    let initial = completed(&db);
    let epoch = db.commit_epoch().unwrap();
    let obstruction = obstruct_next_checkpoint(&mut db);
    let mut busy_retries = 0;
    let mut cleared = false;
    loop {
        match insert_line_range(&mut db, lines..lines + LARGE_DISTRICT_LINES) {
            Ok(()) => break,
            Err(HawDBError::BranchBusy {
                resource: "automatic checkpoint capacity",
            }) if busy_retries < 100 => busy_retries += 1,
            Err(error) => panic!(
                "district failed after {busy_retries} busy retries: {error}; {:?}; {:?}",
                debt(&db).relational_live,
                db.automatic_checkpoint_report()
            ),
        }
        // Every rejected attempt applied nothing.
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        let report = db.automatic_checkpoint_report().unwrap().unwrap();
        assert_eq!(report.completed_checkpoints, initial, "{report:?}");
        if !cleared && report.operation_failures > u64::from(MAX_OPERATION_FAILURES) {
            // The circuit stopped and a retry re-armed it at least once.
            assert!(report.operation_retry_rearms >= 1, "{report:?}");
            std::fs::remove_file(&obstruction).unwrap();
            cleared = true;
        }
    }
    let report = db.automatic_checkpoint_report().unwrap().unwrap();
    eprintln!("district after fault: busy_retries={busy_retries}, report={report:?}");
    assert!(cleared, "{report:?}");
    assert!(report.completed_checkpoints > initial, "{report:?}");
    assert!(!report.operation_retry_exhausted, "{report:?}");
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let lines = lines + LARGE_DISTRICT_LINES;
    assert_order_lines(&mut db, lines);
    drop(db);
    let mut reopened = Database::open_with_config(&fixture.0, config()).unwrap();
    assert_order_lines(&mut reopened, lines);
    reopened.checkpoint().unwrap();
    assert_order_line_totals(&mut reopened, lines);
}

const WIDE_COLUMNS: usize = 40;

/// Wide rows per INSERT statement, within the SQL parser's memo limit.
const WIDE_ROWS_PER_STATEMENT: i64 = 1_000;

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
    let mut transaction = db.begin_transaction().unwrap();
    for start in (0..WIDE_ROWS).step_by(WIDE_ROWS_PER_STATEMENT as usize) {
        transaction
            .query_sql(&wide_rows_sql(start..start + WIDE_ROWS_PER_STATEMENT))
            .unwrap();
    }
    let error = transaction.commit().unwrap_err();
    assert_never_fits(&db, error, epoch, checkpoints);
    let count = db
        .query_sql("SELECT COUNT(*) AS n FROM wide")
        .unwrap()
        .rows
        .into_rows();
    assert_eq!(count[0].get("n"), Some(&Value::Int(0)));
    assert_order_lines(&mut db, lines);
    // The same rows committed in envelope-sized pieces and checkpointed; one
    // autocommit UPDATE of all of them is again larger than the envelope.
    for start in (0..WIDE_ROWS).step_by(WIDE_ROWS_PER_STATEMENT as usize) {
        query_with_busy_retries(
            &mut db,
            &wide_rows_sql(start..start + WIDE_ROWS_PER_STATEMENT),
        )
        .unwrap();
    }
    db.checkpoint().unwrap();
    let wide_sum = |db: &mut Database| {
        db.query_sql("SELECT COUNT(*) AS n, SUM(c0) AS c0 FROM wide")
            .unwrap()
            .rows
            .into_rows()
    };
    let staged = wide_sum(&mut db);
    assert_eq!(staged[0].get("n"), Some(&Value::Int(WIDE_ROWS)));
    let (epoch, checkpoints) = (db.commit_epoch().unwrap(), completed(&db));
    let error = db
        .query_sql(&format!(
            "UPDATE wide SET c0 = c0 + 1 WHERE id >= 0 AND id < {WIDE_ROWS}"
        ))
        .unwrap_err();
    assert_never_fits(&db, error, epoch, checkpoints);
    assert_eq!(wide_sum(&mut db), staged);
    // The handle keeps admitting commits that fit.
    insert_order_lines(&mut db, FILL_COMMITS).unwrap();
    let lines = lines + ORDER_LINES_PER_COMMIT;
    drop(db);
    let mut reopened = Database::open_with_config(&fixture.0, config()).unwrap();
    assert_order_lines(&mut reopened, lines);
    reopened.checkpoint().unwrap();
    assert_order_line_totals(&mut reopened, lines);
}

/// Asserts that `error` is the live-overlay rejection of a commit larger than
/// the whole envelope, and that it applied nothing and forced no drain.
fn assert_never_fits(db: &Database, error: HawDBError, epoch: u64, checkpoints: u64) {
    {
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
        assert_eq!(completed(db), checkpoints);
    }
}

const WIDE_ROWS: i64 = 30_000;

/// First line of a district staged and checkpointed before the fill: lines
/// 60,000..90,000 are exactly district 3, which the fill never reaches.
const STAGED_DISTRICT_START: i64 = 60_000;

/// One autocommit statement larger than the free headroom: an UPDATE of a
/// whole checkpointed district. The statement text stays small, so the
/// commit size, not the parser, is what the statement exercises.
#[test]
fn autocommit_statement_above_the_free_headroom_drains_first() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    create_order_line(&mut db);
    let staged = STAGED_DISTRICT_START..STAGED_DISTRICT_START + DISTRICT_LINES;
    insert_line_range(&mut db, staged.clone()).unwrap();
    db.checkpoint().unwrap();
    let lines = fill_order_lines(&mut db);
    let before = completed(&db);
    query_with_busy_retries(
        &mut db,
        "UPDATE order_line SET ol_quantity = ol_quantity + 1 WHERE ol_w_id = 1 AND ol_d_id = 3",
    )
    .unwrap();
    assert!(completed(&db) > before);
    // A district-wide read merges at most 16,384 live entries (#1480); fold
    // the updated district before reading it back.
    db.checkpoint().unwrap();
    let rows = db
        .query_sql(
            "SELECT COUNT(*) AS n, SUM(ol_quantity) AS quantity FROM order_line \
             WHERE ol_w_id = 1 AND ol_d_id = 3",
        )
        .unwrap()
        .rows
        .into_rows();
    assert_eq!(rows[0].get("n"), Some(&Value::Int(DISTRICT_LINES)));
    assert_eq!(
        rows[0].get("quantity"),
        Some(&Value::Int(DISTRICT_LINES * 6))
    );
    assert_order_lines(&mut db, lines);
}

/// Holds the idle checkpoint owner just before it starts the drain that a
/// waiting commit requested, and reports when that commit starts waiting.
/// Readers are not gated while the owner is idle; writers are.
struct RequestedDrainPause {
    control: Arc<Control>,
    owner_paused: std::sync::mpsc::Receiver<()>,
    resume_owner: std::sync::mpsc::Sender<()>,
    writer_waiting: std::sync::mpsc::Receiver<Phase>,
}

impl RequestedDrainPause {
    /// Install after the fill: below the trigger and the age limit nothing
    /// is due, so the first drain the owner starts is the one the large
    /// commit's declared size requests.
    fn install(db: &Database) -> Self {
        let control = db.runtime.checkpoint_control_for_test();
        // Freeze the frontend policy clock so the writer's bounded drain
        // waits cannot expire into `BranchBusy` while the owner is held.
        control.set_frontend_time(Instant::now());
        let (paused, owner_paused) = std::sync::mpsc::channel();
        let (resume_owner, continuation) = std::sync::mpsc::channel();
        let (waiting, writer_waiting) = std::sync::mpsc::channel();
        {
            let mut state = control.lock().unwrap();
            assert_eq!(state.phase, Phase::Idle);
            state.idle_start_probe = Some(Arc::new(OwnerPauseProbe {
                paused,
                resume: Mutex::new(continuation),
            }));
            state.frontend_wait_probe = Some(waiting);
        }
        Self {
            control,
            owner_paused,
            resume_owner,
            writer_waiting,
        }
    }

    /// Returns once the writer, having passed its task check, waits for the
    /// drain it requested and the owner holds that drain before it starts.
    /// The paused owner gives up after 15 s, so callers release promptly.
    fn wait_until_drain_is_held(&self) {
        // Bounds only a broken run; neither receive is a timing assumption.
        let bound = Duration::from_secs(600);
        self.writer_waiting
            .recv_timeout(bound)
            .expect("the large commit must wait for its drain");
        self.owner_paused
            .recv_timeout(bound)
            .expect("the large commit must make a drain due");
        assert_eq!(self.control.lock().unwrap().phase, Phase::Idle);
    }

    fn release(self) {
        self.resume_owner.send(()).unwrap();
        self.control.lock().unwrap().frontend_wait_probe = None;
        *self.control.frontend_clock.lock().unwrap() = None;
    }
}

fn assert_line_absent(rows: crate::executor::QueryRows, line: i64) {
    assert!(rows.is_empty(), "line {line} must not be committed");
}

fn line_query(line: i64) -> String {
    let (order, number) = (line / 10, line % 10);
    format!(
        "SELECT ol_i_id FROM order_line WHERE ol_w_id = 1 AND ol_d_id = {} \
         AND ol_o_id = {} AND ol_number = {number}",
        order / 3_000 + 1,
        order % 3_000,
    )
}

/// Stages one district commit in a concurrent transaction, as statements of
/// at most `LINES_PER_STATEMENT` lines.
fn stage_district(
    transaction: &mut crate::ConcurrentDatabaseTransaction,
    lines: std::ops::Range<i64>,
) {
    for sql in order_line_statements(lines) {
        transaction.query_sql(&sql).unwrap();
    }
}

/// Ticks another writer commits on the concurrent handle around the district.
const TICKS: i64 = 20;

/// While a district commit on a concurrent handle waits for the drain it
/// requested, readers keep their published view and another writer queues
/// behind the commit sequencer; both finish once the drain publishes. The
/// owner holds that drain before publication, so the reads provably happen
/// during the wait, without free-running threads competing with the drain.
#[test]
fn concurrent_writer_and_reader_progress_around_a_pre_drained_commit() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    create_order_line(&mut db);
    db.query_sql("CREATE TABLE ticks (id BIGINT PRIMARY KEY, payload TEXT)")
        .unwrap();
    let lines = fill_order_lines(&mut db);
    let before = completed(&db);
    let pause = RequestedDrainPause::install(&db);
    let db = db.into_concurrent();
    // Readers use the published view; the first read publishes it.
    assert_eq!(db.query_sql(&line_query(0)).unwrap().rows.len(), 1);
    std::thread::scope(|scope| {
        let district = scope.spawn(|| {
            let mut transaction = db
                .begin_transaction(crate::ConcurrentTransactionOptions::default())
                .unwrap();
            stage_district(&mut transaction, lines..lines + DISTRICT_LINES);
            transaction.commit().map(|_| ())
        });
        pause.wait_until_drain_is_held();
        for line in [0, lines - 1] {
            let rows = db.query_sql(&line_query(line)).unwrap().rows.into_rows();
            assert_eq!(rows.len(), 1, "line {line}");
            assert_eq!(
                rows[0].get("ol_i_id"),
                Some(&Value::Int(line * 7_919 % 100_000))
            );
        }
        assert_line_absent(db.query_sql(&line_query(lines)).unwrap().rows, lines);
        let writer = scope.spawn(|| {
            for id in 0..TICKS {
                db.query_sql_with_params(
                    "INSERT INTO ticks (id, payload) VALUES ($1, $2)",
                    &[Value::Int(id), Value::String(format!("tick-{id}"))],
                )
                .unwrap_or_else(|error| panic!("tick {id} failed: {error}"));
            }
        });
        pause.release();
        district
            .join()
            .unwrap()
            .unwrap_or_else(|error| panic!("district commit failed: {error}"));
        writer.join().unwrap();
    });
    let report = db.automatic_checkpoint_report().unwrap().unwrap();
    assert!(report.completed_checkpoints > before, "{report:?}");
    assert_eq!(report.failed_attempts, 0, "{report:?}");
    let count = db
        .query_sql("SELECT COUNT(*) AS n FROM ticks")
        .unwrap()
        .rows
        .into_rows();
    assert_eq!(count[0].get("n"), Some(&Value::Int(TICKS)));
    drop(db);
    let mut reopened = Database::open_with_config(&fixture.0, config()).unwrap();
    let lines = lines + DISTRICT_LINES;
    assert_order_lines(&mut reopened, lines);
    reopened.checkpoint().unwrap();
    assert_order_line_totals(&mut reopened, lines);
}
