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
