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

//! A drain-first large commit interrupted while it waits: a cancelled task,
//! an expired deadline, or a process stop during the requested drain (#1479).
//! These run in their own partition, apart from the drain-first successes.

use super::*;

/// A concurrent-handle commit whose task is cancelled while it waits for the
/// drain it requested applies nothing once that drain is adopted: no WAL
/// append or new epoch, no rows, and its admission, transaction locks and
/// the commit sequencer are released.
#[test]
fn a_commit_cancelled_while_waiting_for_its_drain_applies_nothing_and_releases_admission() {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkPriority, RuntimeWorkRequest,
    };
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    let lines = fill_below_trigger(&mut db);
    let epoch = db.commit_epoch().unwrap();
    let before = completed(&db);
    let pause = RequestedDrainPause::install(&db);
    // A host governor that admits the transaction; the checkpoint owner keeps
    // its own resources, so the permit cannot starve the drain.
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(1024 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(std::num::NonZeroUsize::new(4).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 31), Some(1 << 31), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let permit = governor
        .try_admit(
            RuntimeWorkRequest::mutation(RuntimeWorkPriority::Foreground, 256 * 1024 * 1024)
                .with_result_bytes(1024 * 1024),
        )
        .unwrap();
    let context = RuntimeTaskContext::default();
    let cancellation = context.cancellation().clone();
    let db = db.into_concurrent();
    let error = std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut transaction = db
                .begin_admitted_transaction(
                    crate::ConcurrentTransactionOptions::default(),
                    permit,
                    context,
                )
                .unwrap();
            stage_district(&mut transaction, lines..lines + DISTRICT_LINES);
            transaction.commit()
        });
        pause.wait_until_drain_is_held();
        assert_eq!(governor.snapshot().active_foreground_tasks, 1);
        cancellation.cancel();
        pause.release();
        writer.join().unwrap().unwrap_err()
    });
    assert!(
        error
            .to_string()
            .contains("runtime task stopped: cancelled"),
        "{error}"
    );
    // The drain completed and was adopted; the commit did not follow it.
    assert!(
        db.automatic_checkpoint_report()
            .unwrap()
            .unwrap()
            .completed_checkpoints
            > before
    );
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert_line_absent(db.query_sql(&line_query(lines)).unwrap().rows, lines);
    assert_eq!(governor.snapshot().active_foreground_tasks, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    // The sequencer and the district's keys are free: a commit to the same
    // keys succeeds and would conflict had the district been applied.
    db.query_sql(&order_lines_sql(lines..lines + ORDER_LINES_PER_COMMIT))
        .unwrap();
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    drop(db);
    let mut reopened = Database::open_with_config(&fixture.0, config()).unwrap();
    let lines = lines + ORDER_LINES_PER_COMMIT;
    assert_order_lines(&mut reopened, lines);
    assert_line_absent(reopened.query_sql(&line_query(lines)).unwrap().rows, lines);
}

/// A single-handle commit whose deadline passes while it waits for the drain
/// it requested applies nothing and leaves the handle writable.
#[test]
fn a_commit_whose_deadline_expires_while_waiting_for_its_drain_applies_nothing() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(&fixture.0, config()).unwrap();
    let lines = fill_below_trigger(&mut db);
    let epoch = db.commit_epoch().unwrap();
    let before = completed(&db);
    let pause = RequestedDrainPause::install(&db);
    let (deadline_sender, deadline_receiver) = std::sync::mpsc::channel();
    let (mut db, result) = std::thread::scope(|scope| {
        let writer = scope.spawn(move || {
            let result = {
                let mut transaction = db.begin_transaction().unwrap();
                transaction
                    .query_sql(&order_lines_sql(lines..lines + DISTRICT_LINES))
                    .unwrap();
                // The deadline covers only the commit: its first attempt and
                // the task check before the wait finish well inside it, and
                // the test lets it pass while the drain is held.
                let context = RuntimeTaskContext::with_timeout(Duration::from_secs(8));
                deadline_sender.send(context.deadline().unwrap()).unwrap();
                transaction.task_context = Some(context);
                transaction.commit().map(|_| ())
            };
            (db, result)
        });
        pause.wait_until_drain_is_held();
        let deadline = deadline_receiver.recv().unwrap();
        // Waiting for the clock is the condition under test, not a handoff.
        std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
        assert!(Instant::now() >= deadline);
        pause.release();
        writer.join().unwrap()
    });
    let error = result.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("runtime task stopped: deadline_exceeded"),
        "{error}"
    );
    assert!(completed(&db) > before);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert!(!db.storage_handle_poisoned().unwrap());
    assert_line_absent(db.query_sql(&line_query(lines)).unwrap().rows, lines);
    // The drain folded the fill and nothing followed it: exact totals.
    assert_order_line_totals(&mut db, lines);
    // The runtime and publication barrier were released: the same district
    // now commits without another drain.
    let checkpoints = completed(&db);
    insert_line_range(&mut db, lines..lines + DISTRICT_LINES).unwrap();
    assert_eq!(completed(&db), checkpoints);
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let lines = lines + DISTRICT_LINES;
    drop(db);
    let mut reopened = Database::open_with_config(&fixture.0, config()).unwrap();
    assert_order_lines(&mut reopened, lines);
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
                "api::automatic_checkpoint::tests::relational_live_overlay::large_commit::interrupted::pre_drain_crash_child",
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
