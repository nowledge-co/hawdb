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

//! Relational live row-overlay pressure drives automatic checkpoints (#1438).

use super::*;
use crate::Value;
use hawdb_storage::relational::{
    DEFAULT_MAX_RELATIONAL_ROW_CHANGES, DEFAULT_MAX_RELATIONAL_ROW_CHANGE_BYTES,
};

const ROWS: i64 = 512;
const BODY_BYTES: usize = 1024;

fn body(round: i64) -> String {
    format!("{round:08}").repeat(BODY_BYTES / 8)
}

fn config() -> DatabaseConfig {
    DatabaseConfig {
        // Neither age nor WAL bytes can trigger a checkpoint in this fixture.
        automatic_checkpoint_max_age: Duration::from_secs(3600),
        ..DatabaseConfig::default()
    }
}

fn load(db: &mut Database) {
    db.query_sql("CREATE TABLE hot (id BIGINT PRIMARY KEY, round BIGINT, body TEXT)")
        .unwrap();
    for id in 0..ROWS {
        db.query_sql_with_params(
            "INSERT INTO hot (id, round, body) VALUES ($1, $2, $3)",
            &[Value::Int(id), Value::Int(-1), Value::String(body(-1))],
        )
        .unwrap();
    }
    // Establish the canonical row base that the live overlay layers over.
    db.checkpoint().unwrap();
}

fn update_round(db: &mut Database, round: i64) -> Result<()> {
    let mut transaction = db.begin_transaction()?;
    transaction.query_sql_with_params(
        "UPDATE hot SET round = $1, body = $2 WHERE id >= 0",
        &[Value::Int(round), Value::String(body(round))],
    )?;
    transaction.commit().map(|_| ())
}

fn assert_round(rows: crate::executor::QueryRows, round: i64) {
    let rows = rows.into_rows();
    assert_eq!(rows.len(), ROWS as usize);
    for (id, row) in rows.iter().enumerate() {
        assert_eq!(row.get("id"), Some(&Value::Int(id as i64)));
        assert_eq!(row.get("round"), Some(&Value::Int(round)));
        assert_eq!(row.get("body"), Some(&Value::String(body(round))));
    }
}

fn live_rows(db: &Database) -> (usize, usize) {
    let report = db.storage_residency_report().unwrap().relational_rows;
    (report.live_entries, report.live_resident_bytes)
}

#[test]
fn steady_relational_updates_on_one_handle_never_exhaust_the_live_overlay() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    load(&mut db);
    let initial = db
        .automatic_checkpoint_report()
        .unwrap()
        .unwrap()
        .completed_checkpoints;
    let mut admitted_entries = 0usize;
    let mut peak = (0usize, 0usize);
    let mut previous_entries = 0usize;
    let mut drains = 0usize;
    // Each round rewrites every row. Without a drain, main rejects round 97
    // with the #1438 live-overlay admission error.
    let rounds = 400;
    for round in 0..rounds {
        update_round(&mut db, round).unwrap_or_else(|error| {
            panic!(
                "round {round} failed after {admitted_entries} admitted entries: {error}; {:?}",
                db.automatic_checkpoint_report()
            )
        });
        admitted_entries += ROWS as usize;
        let live = live_rows(&db);
        peak = (peak.0.max(live.0), peak.1.max(live.1));
        // Adopting a published drain empties the serving overlay.
        drains += usize::from(live.0 < previous_entries);
        previous_entries = live.0;
    }
    // Payload bytes alone, before per-row allocation overhead, exceed the
    // default resident-byte envelope three times over.
    assert!(
        admitted_entries * BODY_BYTES > 3 * DEFAULT_MAX_RELATIONAL_ROW_CHANGE_BYTES,
        "fixture must exceed the live envelope several times"
    );
    assert!(peak.0 <= DEFAULT_MAX_RELATIONAL_ROW_CHANGES, "{peak:?}");
    assert!(
        peak.1 <= DEFAULT_MAX_RELATIONAL_ROW_CHANGE_BYTES,
        "{peak:?}"
    );
    let report = db.automatic_checkpoint_report().unwrap().unwrap();
    assert!(drains >= 2, "serving overlay must be folded: {report:?}");
    assert!(
        report.completed_checkpoints >= initial + 2,
        "live-overlay pressure must rotate checkpoints: {report:?}"
    );
    let query = "SELECT id, round, body FROM hot ORDER BY id";
    assert_round(db.query_sql(query).unwrap().rows, rounds - 1);
    drop(db);
    let mut reopened = Database::open(&fixture.0).unwrap();
    assert_round(reopened.query_sql(query).unwrap().rows, rounds - 1);
}

fn debt(db: &Database) -> CheckpointDebtSnapshot {
    db.runtime
        .peek()
        .unwrap()
        .store
        .checkpoint_debt_snapshot()
        .unwrap()
}

#[test]
fn checkpoint_debt_reports_the_live_overlay_envelope_and_checkpoint_folds_it() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    load(&mut db);
    assert_eq!(debt(&db).relational_live.row_entries, 0);
    for round in 0..3 {
        update_round(&mut db, round).unwrap();
    }
    let live = debt(&db).relational_live;
    let (entries, resident_bytes) = live_rows(&db);
    assert_eq!(live.row_entries, 3 * ROWS as u64);
    assert_eq!(live.row_entries, entries as u64);
    assert_eq!(live.row_resident_bytes, resident_bytes as u64);
    assert_eq!(
        live.max_row_entries,
        Some(DEFAULT_MAX_RELATIONAL_ROW_CHANGES as u64)
    );
    assert_eq!(
        live.max_row_resident_bytes,
        Some(DEFAULT_MAX_RELATIONAL_ROW_CHANGE_BYTES as u64)
    );
    db.checkpoint().unwrap();
    let folded = debt(&db).relational_live;
    assert_eq!((folded.row_entries, folded.row_resident_bytes), (0, 0));
    assert_eq!(live_rows(&db), (0, 0));
    assert_round(
        db.query_sql("SELECT id, round, body FROM hot ORDER BY id")
            .unwrap()
            .rows,
        2,
    );
}

#[test]
fn live_overlay_pressure_uses_the_shared_soft_and_capacity_boundaries() {
    const LIMIT: u64 = 100_000;
    let soft = LIMIT * u64::from(hawdb_storage::pressure::STORAGE_PRESSURE_SOFT_RATIO_PER_MILLION)
        / 1_000_000;
    let defer = LIMIT
        * u64::from(hawdb_storage::pressure::STORAGE_PRESSURE_DEFER_RATIO_PER_MILLION)
        / 1_000_000;
    let capacity = defer - LIMIT / 10;
    let with = |pair: usize, retained: u64, limit: Option<u64>| {
        let mut live = hawdb_storage::store::RelationalLiveOverlayDebt::default();
        let slot = match pair {
            0 => (&mut live.row_entries, &mut live.max_row_entries),
            1 => (
                &mut live.row_resident_bytes,
                &mut live.max_row_resident_bytes,
            ),
            2 => (&mut live.index_entries, &mut live.max_index_entries),
            _ => (
                &mut live.index_encoded_bytes,
                &mut live.max_index_encoded_bytes,
            ),
        };
        *slot.0 = retained;
        *slot.1 = limit;
        State {
            debt: Some(CheckpointDebtSnapshot {
                commit_epoch: 2,
                checkpoint_commit_epoch: 1,
                wal_generation: 1,
                wal_bytes: 0,
                wal_age_millis: 0,
                max_wal_bytes: Some(u64::MAX),
                max_wal_record_bytes: Some(1),
                delta_bytes: 0,
                max_delta_bytes: None,
                read_only: false,
                wal_sync_group_active: false,
                relational_live: live,
            }),
            ..State::default()
        }
    };
    for pair in 0..4 {
        let below = with(pair, soft - 1, Some(LIMIT));
        assert!(!soft_pressure(below.debt.unwrap()), "pair {pair}");
        let at = with(pair, soft, Some(LIMIT));
        assert!(soft_pressure(at.debt.unwrap()), "pair {pair}");
        assert!(!needs_headroom(&at) && !preparation_needs_capacity(&at));
        let below = with(pair, capacity - 1, Some(LIMIT));
        assert!(!needs_headroom(&below) && !preparation_needs_capacity(&below));
        let at = with(pair, capacity, Some(LIMIT));
        assert!(
            needs_headroom(&at) && preparation_needs_capacity(&at),
            "pair {pair}"
        );
        // No serving live view has no live-overlay envelope to exhaust.
        let unbounded = with(pair, LIMIT, None);
        assert!(!soft_pressure(unbounded.debt.unwrap()));
        assert!(!needs_headroom(&unbounded) && !preparation_needs_capacity(&unbounded));
    }
    // The recovery delta that the next drain captures with the live overlay
    // counts toward both row pairs (#1464).
    for pair in 0..2 {
        let split = |delta: u64| {
            let mut state = with(pair, soft / 2, Some(LIMIT));
            let live = &mut state.debt.as_mut().unwrap().relational_live;
            if pair == 0 {
                live.row_recovery_delta_entries = delta;
            } else {
                live.row_recovery_delta_resident_bytes = delta;
            }
            state
        };
        assert!(!soft_pressure(split(soft - soft / 2 - 1).debt.unwrap()));
        assert!(soft_pressure(split(soft - soft / 2).debt.unwrap()));
        // Writers fence for an idle owner to start that drain. A running drain
        // already captured the delta, so its capacity wait counts live rows.
        let at = split(capacity - soft / 2);
        assert!(
            needs_headroom(&at) && !preparation_needs_capacity(&at),
            "pair {pair}"
        );
    }
}

#[test]
fn pinned_reader_survives_pressure_triggered_live_overlay_drains() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    load(&mut db);
    update_round(&mut db, 0).unwrap();
    let pinned = db.begin_read_transaction().unwrap();
    let initial = db
        .automatic_checkpoint_report()
        .unwrap()
        .unwrap()
        .completed_checkpoints;
    let query = "SELECT id, round, body FROM hot ORDER BY id";
    let mut round = 1;
    while db
        .automatic_checkpoint_report()
        .unwrap()
        .unwrap()
        .completed_checkpoints
        < initial + 2
    {
        assert!(round < 1_000, "live-overlay pressure must drain twice");
        update_round(&mut db, round).unwrap();
        round += 1;
    }
    assert_round(pinned.query_sql(query).unwrap().rows, 0);
    assert_round(db.query_sql(query).unwrap().rows, round - 1);
    drop(pinned);
    update_round(&mut db, round).unwrap();
    assert_round(db.query_sql(query).unwrap().rows, round);
}

const CRASH_CHILD_ENV: &str = "HAWDB_TEST_LIVE_OVERLAY_CRASH_CHILD";
const CRASH_PATH_ENV: &str = "HAWDB_TEST_LIVE_OVERLAY_CRASH_PATH";
const CRASH_POINT_ENV: &str = "HAWDB_TEST_PROCESS_CRASH_POINT";

fn acknowledged_path(root: &std::path::Path) -> PathBuf {
    root.with_extension("acknowledged")
}

#[test]
fn pressure_drain_crash_child() {
    if std::env::var_os(CRASH_CHILD_ENV).is_none() {
        return;
    }
    let root = PathBuf::from(std::env::var_os(CRASH_PATH_ENV).expect("crash database path"));
    let point = std::env::var(CRASH_POINT_ENV).expect("crash point");
    let mut db = Database::open_with_config(&root, config()).unwrap();
    // The only checkpoint this child can publish is the pressure-triggered
    // one; its publication failpoint terminates the process mid-drain.
    for round in 0..1_000 {
        update_round(&mut db, round).unwrap();
        std::fs::write(acknowledged_path(&root), round.to_string()).unwrap();
    }
    panic!("crash failpoint {point} was not reached by a pressure-triggered drain");
}

#[test]
fn crash_during_pressure_triggered_drain_recovers_whole_acknowledged_rounds() {
    for point in [
        "during_checkpoint_publication",
        "after_manifest_publication",
    ] {
        let fixture = Fixture::new();
        {
            let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
            load(&mut db);
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(
                "api::automatic_checkpoint::tests::relational_live_overlay::pressure_drain_crash_child",
            )
            .arg("--nocapture")
            .env(CRASH_CHILD_ENV, "1")
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
        assert!(
            acknowledged as usize * ROWS as usize * BODY_BYTES
                > DEFAULT_MAX_RELATIONAL_ROW_CHANGE_BYTES / 2,
            "the drain must start under live-overlay pressure ({acknowledged} rounds at {point})"
        );
        let mut reopened = Database::open_with_config(&fixture.0, config()).unwrap();
        let rows = reopened
            .query_sql("SELECT id, round, body FROM hot ORDER BY id")
            .unwrap()
            .rows;
        // Every acknowledged round is durable; at most the one in flight when
        // the process stopped may also be present, and never partially.
        let recovered = match rows.get(0, "round") {
            Some(hawdb_core::ValueRef::Int(round)) => round,
            value => panic!("unexpected recovered round {value:?} after {point}"),
        };
        assert!(
            (acknowledged..=acknowledged + 1).contains(&recovered),
            "recovered round {recovered}, acknowledged {acknowledged} at {point}"
        );
        assert_round(rows, recovered);
        // The recovered handle keeps draining under the same pressure.
        let next = recovered + 1;
        for round in next..next + 160 {
            update_round(&mut reopened, round).unwrap();
        }
        assert_round(
            reopened
                .query_sql("SELECT id, round, body FROM hot ORDER BY id")
                .unwrap()
                .rows,
            next + 159,
        );
    }
}

const ORDER_LINES_PER_COMMIT: i64 = 2_500;

fn create_order_line(db: &mut Database) {
    db.query_sql(
        "CREATE TABLE order_line (ol_o_id BIGINT NOT NULL, ol_d_id BIGINT NOT NULL, \
         ol_w_id BIGINT NOT NULL, ol_number BIGINT NOT NULL, ol_i_id BIGINT, \
         ol_supply_w_id BIGINT, ol_delivery_d TEXT, ol_quantity BIGINT, \
         ol_amount DOUBLE PRECISION, ol_dist_info TEXT, \
         PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
    )
    .unwrap();
    db.query_sql("CREATE INDEX idx_order_line_order ON order_line (ol_w_id, ol_d_id, ol_o_id)")
        .unwrap();
}

/// TPC-C ORDER_LINE-shaped row `line` (about 120 encoded bytes): ten lines
/// per order, 3,000 orders per district.
fn order_line(line: i64) -> String {
    let (order, number) = (line / 10, line % 10);
    format!(
        "({}, {}, 1, {number}, {}, 1, '2026-11-01 00:00:00', 5, 12.5, '{}')",
        order % 3_000,
        order / 3_000 + 1,
        line * 7_919 % 100_000,
        "x".repeat(24),
    )
}

fn insert_order_lines(db: &mut Database, commit: i64) -> Result<()> {
    let lines = (commit * ORDER_LINES_PER_COMMIT..(commit + 1) * ORDER_LINES_PER_COMMIT)
        .map(order_line)
        .collect::<Vec<_>>()
        .join(", ");
    let mut transaction = db.begin_transaction()?;
    transaction.query_sql(&format!(
        "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, \
         ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES {lines}"
    ))?;
    transaction.commit().map(|_| ())
}

/// Exact totals by full scan. Call it on a folded base: one full scan merges
/// at most the row snapshot's per-scan overlay-entry bound.
fn assert_order_line_totals(db: &mut Database, lines: i64) {
    let rows = db
        .query_sql(
            "SELECT COUNT(*) AS n, SUM(ol_i_id) AS items, MAX(ol_d_id) AS districts \
             FROM order_line",
        )
        .unwrap()
        .rows
        .into_rows();
    assert_eq!(rows[0].get("n"), Some(&Value::Int(lines)));
    assert_eq!(
        rows[0].get("items"),
        Some(&Value::Int(
            (0..lines).map(|line| line * 7_919 % 100_000).sum::<i64>()
        ))
    );
    assert_eq!(
        rows[0].get("districts"),
        Some(&Value::Int((lines - 1) / 10 / 3_000 + 1))
    );
}

/// Point and secondary-index reads through the serving overlay.
fn assert_order_lines(db: &mut Database, lines: i64) {
    for line in [0, lines / 2, lines - 1] {
        let (order, number) = (line / 10, line % 10);
        let (district, order) = (order / 3_000 + 1, order % 3_000);
        let point = db
            .query_sql(&format!(
                "SELECT ol_i_id FROM order_line WHERE ol_w_id = 1 AND ol_d_id = {district} \
                 AND ol_o_id = {order} AND ol_number = {number}"
            ))
            .unwrap()
            .rows
            .into_rows();
        assert_eq!(point.len(), 1, "line {line}");
        assert_eq!(
            point[0].get("ol_i_id"),
            Some(&Value::Int(line * 7_919 % 100_000))
        );
        let order_lines = db
            .query_sql(&format!(
                "SELECT ol_number FROM order_line WHERE ol_w_id = 1 AND ol_d_id = {district} \
                 AND ol_o_id = {order}"
            ))
            .unwrap()
            .rows
            .into_rows();
        assert_eq!(order_lines.len(), 10, "order of line {line}");
    }
}

#[test]
fn steady_inserts_into_a_growing_table_keep_draining_the_live_overlay() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    create_order_line(&mut db);
    let initial = db
        .automatic_checkpoint_report()
        .unwrap()
        .unwrap()
        .completed_checkpoints;
    let mut commits = 0;
    let mut peak_live = (0usize, 0usize);
    let mut peak_live_debt = [0u64; 4];
    let mut max_delta_entries = 0;
    let mut busy_retries = 0;
    // Each drain folds about 47,000 lines (70% of the resident-byte envelope).
    // On c8d767cd the second drain's capture of the recovery delta plus the
    // live overlay is rejected three times, which stops automatic retries;
    // commit 48 (epoch 52) then fails with the #1464 admission error.
    while db
        .automatic_checkpoint_report()
        .unwrap()
        .unwrap()
        .completed_checkpoints
        < initial + 5
    {
        assert!(commits < 240, "live-overlay pressure must drain five times");
        match insert_order_lines(&mut db, commits) {
            Ok(()) => commits += 1,
            // Near the envelope a writer waits for the running drain. When
            // that drain makes no builder progress for the bounded wait (an
            // unoptimized build can take that long between work units), the
            // commit is rejected before WAL append with nothing applied, and
            // the writer retries it. A live-overlay admission error never is.
            Err(HawDBError::BranchBusy {
                resource: "automatic checkpoint capacity",
            }) if busy_retries < 100 => busy_retries += 1,
            Err(error) => panic!(
                "commit {commits} failed after {busy_retries} busy retries: {error}; {:?}; {:?}",
                debt(&db).relational_live,
                db.automatic_checkpoint_report()
            ),
        }
        let live = live_rows(&db);
        peak_live = (peak_live.0.max(live.0), peak_live.1.max(live.1));
        let overlay = debt(&db).relational_live;
        // The previous drain's catch-up suffix is base-relative debt for the
        // next one, charged at the live-overlay rate of 997 bytes per line.
        assert_eq!(
            overlay.row_recovery_delta_resident_bytes,
            overlay.row_recovery_delta_entries * 997
        );
        max_delta_entries = max_delta_entries.max(overlay.row_recovery_delta_entries);
        // Live occupancy never reaches the admission envelope. While a drain
        // runs, the delta it folds still counts in `pairs()`; that drain's
        // capture was admitted when it started (no failed attempt below).
        for (peak, (retained, limit)) in peak_live_debt.iter_mut().zip(overlay.admission_pairs()) {
            *peak = (*peak).max(retained);
            assert!(
                limit.is_none_or(|limit| retained < limit),
                "{retained} {limit:?}"
            );
        }
    }
    let report = db.automatic_checkpoint_report().unwrap().unwrap();
    assert!(max_delta_entries > 0, "drains must overlap later commits");
    // Every rejected attempt left no trace: the table holds exactly the
    // acknowledged commits (checked below and after reopen).
    assert_eq!(report.failed_attempts, 0, "{report:?}");
    assert!(!report.operation_retry_exhausted, "{report:?}");
    assert!(
        peak_live.0 <= DEFAULT_MAX_RELATIONAL_ROW_CHANGES,
        "{peak_live:?}"
    );
    assert!(
        peak_live.1 <= DEFAULT_MAX_RELATIONAL_ROW_CHANGE_BYTES,
        "{peak_live:?}"
    );
    let lines = commits * ORDER_LINES_PER_COMMIT;
    assert!(
        lines as usize > 2 * DEFAULT_MAX_RELATIONAL_ROW_CHANGES,
        "the table must outgrow the live envelope several times: {lines}, {peak_live_debt:?}"
    );
    assert_order_lines(&mut db, lines);
    drop(db);
    let mut reopened = Database::open_with_config(&fixture.0, config()).unwrap();
    assert_order_lines(&mut reopened, lines);
    // The reopened handle keeps admitting inserts; folding everything into
    // one base then gives exact totals.
    insert_order_lines(&mut reopened, commits).unwrap();
    let lines = lines + ORDER_LINES_PER_COMMIT;
    assert_order_lines(&mut reopened, lines);
    reopened.checkpoint().unwrap();
    assert_order_line_totals(&mut reopened, lines);
}
