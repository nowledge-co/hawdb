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

use super::*;
use crate::{
    IoConcurrencyBudget, RuntimeAdmissionCode, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeTaskContext,
    RuntimeWorkPriority, RuntimeWorkRequest,
};

fn governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            cpu_slot_limit: NonZeroUsize::new(2),
            foreground_task_limit: NonZeroUsize::new(2),
            blocking_task_limit: NonZeroUsize::new(2),
            memory_budget_bytes: Some(64 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(
                Some(1024 * 1024 * 1024),
                Some(1024 * 1024 * 1024),
                None,
                None,
                None,
            ),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

fn request() -> RuntimeWorkRequest {
    RuntimeWorkRequest::mutation(RuntimeWorkPriority::Foreground, 16 * 1024 * 1024)
        .with_result_bytes(1024 * 1024)
}

#[test]
fn admitted_transaction_reuses_one_permit_and_refunds_every_retirement() {
    let db = ConcurrentDatabase::new(Database::new());
    let governor = governor();
    db.query_sql("CREATE TABLE admitted (id BIGINT PRIMARY KEY)")
        .unwrap();
    db.query_sql("INSERT INTO admitted (id) VALUES (1)")
        .unwrap();
    for mode in [
        ConcurrentTransactionOptions::optimistic(),
        ConcurrentTransactionOptions::pessimistic(Duration::ZERO),
    ] {
        for retirement in 0..3 {
            let admissions = governor.snapshot().admissions;
            let mut tx = db
                .begin_admitted_transaction(
                    mode,
                    governor.try_admit(request()).unwrap(),
                    RuntimeTaskContext::default(),
                )
                .unwrap();
            tx.query("CREATE (:Admitted {id: 1})").unwrap();
            assert_eq!(
                tx.query("MATCH (n:Admitted) RETURN n.id")
                    .unwrap()
                    .rows
                    .len(),
                1
            );
            assert_eq!(
                tx.query_sql("SELECT id FROM admitted").unwrap().rows.len(),
                1
            );
            assert_eq!(governor.snapshot().admissions, admissions + 1);
            assert_eq!(governor.snapshot().active_foreground_tasks, 1);
            match retirement {
                0 => tx.rollback(),
                1 => drop(tx),
                _ => {
                    tx.commit().unwrap();
                }
            }
            assert_eq!(governor.snapshot().active_foreground_tasks, 0);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
            db.query("MATCH (n:Admitted) DELETE n").unwrap();
        }
    }
}

#[test]
fn admitted_transaction_rejects_wrong_kind_and_cancelled_context_without_leaking() {
    let db = ConcurrentDatabase::new(Database::new());
    let governor = governor();
    let context = RuntimeTaskContext::default();
    context.cancellation().cancel();
    let result = db.begin_admitted_transaction(
        ConcurrentTransactionOptions::optimistic(),
        governor.try_admit(request()).unwrap(),
        context,
    );
    assert!(result.err().unwrap().to_string().contains("cancelled"));
    let result = db.begin_admitted_transaction(
        ConcurrentTransactionOptions::optimistic(),
        governor
            .try_admit(RuntimeWorkRequest::foreground_query(1024, 1024))
            .unwrap(),
        RuntimeTaskContext::default(),
    );
    assert!(result
        .err()
        .unwrap()
        .to_string()
        .contains("mutation permit"));
    assert_eq!(governor.snapshot().active_foreground_tasks, 0);
}

#[test]
fn admitted_transaction_large_waiter_keeps_priority_until_both_transactions_retire() {
    let db = ConcurrentDatabase::new(Database::new());
    let governor = governor();
    let begin = |permit| {
        db.begin_admitted_transaction(
            ConcurrentTransactionOptions::optimistic(),
            permit,
            RuntimeTaskContext::default(),
        )
        .unwrap()
    };
    let first = begin(governor.try_admit(request()).unwrap());
    let second = begin(governor.try_admit(request()).unwrap());
    let large = governor.admission_waiter(RuntimeWorkPriority::Foreground);
    let large_request = request().with_cpu_slots(2);
    assert!(governor.try_admit_waiter(&large, large_request).is_err());
    first.rollback();
    for _ in 0..32 {
        assert_eq!(
            governor.try_admit(request()).unwrap_err().code,
            RuntimeAdmissionCode::QueuedAhead
        );
    }
    assert!(governor.try_admit_waiter(&large, large_request).is_err());
    second.rollback();
    let mut transaction = begin(governor.try_admit_waiter(&large, large_request).unwrap());
    transaction.query("CREATE (:LargeAdmitted)").unwrap();
    transaction.commit().unwrap();
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
}

#[test]
fn admitted_large_transaction_completes_under_recurring_small_transactions() {
    const WORKERS: usize = 4;
    const SMALL_COMMITS: usize = 32;
    const LARGE_ROWS: usize = 64;

    for (durable, priority) in [
        (false, RuntimeWorkPriority::Foreground),
        (true, RuntimeWorkPriority::Foreground),
        (false, RuntimeWorkPriority::Background),
        (true, RuntimeWorkPriority::Background),
    ] {
        let path = super::super::unique_test_dir("admitted_recurring_small_writers");
        let mut database = if durable {
            Database::open(&path).unwrap()
        } else {
            Database::new()
        };
        database
            .query_sql("CREATE TABLE progress (id BIGINT PRIMARY KEY)")
            .unwrap();
        let db = ConcurrentDatabase::new_with_wal_group_commit(
            database,
            WalGroupCommitConfig::benchmark_candidate(
                NonZeroUsize::new(4).unwrap(),
                NonZeroU64::new(1024 * 1024).unwrap(),
                Duration::from_micros(100),
            )
            .unwrap(),
        );
        let governor = governor();
        let begin = |permit| {
            db.begin_admitted_transaction(
                ConcurrentTransactionOptions::optimistic(),
                permit,
                RuntimeTaskContext::default(),
            )
            .unwrap()
        };
        let epoch = db.commit_epoch().unwrap();
        let mut first = begin(governor.try_admit(request()).unwrap());
        let mut second = begin(governor.try_admit(request()).unwrap());
        first
            .query_sql("INSERT INTO progress (id) VALUES (-1)")
            .unwrap();
        second
            .query_sql("INSERT INTO progress (id) VALUES (-2)")
            .unwrap();
        let large = governor.admission_waiter(priority);
        let mut large_request = request().with_cpu_slots(2);
        large_request.priority = priority;
        assert!(governor.try_admit_waiter(&large, large_request).is_err());
        // Hold both slots until the public aging deadline has passed. This
        // checks the actual clock-based boundary without depending on how fast
        // the setup runs or exposing a test-only governor control plane.
        if priority == RuntimeWorkPriority::Background {
            while let Some(deadline) = large.next_priority_change_at() {
                std::thread::sleep(deadline.saturating_duration_since(std::time::Instant::now()));
            }
        }
        first.commit().unwrap();

        // One slot is available, but younger one-slot requests must not consume
        // it while the older two-slot transaction waits for the other owner.
        // This includes direct callers that do not create a queued waiter.
        assert_eq!(
            governor.try_admit(request()).unwrap_err().code,
            RuntimeAdmissionCode::QueuedAhead
        );
        let (attempted, attempts) = mpsc::channel();
        let handles = (0..WORKERS)
            .map(|worker| {
                let db = db.clone();
                let governor = governor.clone();
                let attempted = attempted.clone();
                std::thread::spawn(move || {
                    let deadline = std::time::Instant::now() + Duration::from_secs(60);
                    for iteration in 0..SMALL_COMMITS {
                        let waiter = governor.admission_waiter(RuntimeWorkPriority::Foreground);
                        if iteration == 0 {
                            let probe = governor.try_admit_waiter(&waiter, request());
                            attempted
                                .send(matches!(&probe, Err(error) if error.code == RuntimeAdmissionCode::QueuedAhead))
                                .unwrap();
                            drop(probe);
                        }
                        let permit = loop {
                            match governor.try_admit_waiter(&waiter, request()) {
                                Ok(permit) => break permit,
                                Err(error) => {
                                    assert!(error.is_retryable(), "{error}");
                                    assert!(
                                        std::time::Instant::now() < deadline,
                                        "small writer {worker} did not make progress: {error}"
                                    );
                                    std::thread::sleep(Duration::from_millis(1));
                                }
                            }
                        };
                        let mut tx = db
                            .begin_admitted_transaction(
                                ConcurrentTransactionOptions::optimistic(),
                                permit,
                                RuntimeTaskContext::default(),
                            )
                            .unwrap();
                        // Admission occurs before snapshot capture. Even the
                        // earliest younger writer must see the entire large
                        // transaction, not a prefix of its 64 statements.
                        let rows = tx
                            .query_sql("SELECT id FROM progress WHERE id >= 0 AND id < 64")
                            .unwrap()
                            .rows;
                        assert_eq!(rows.len(), LARGE_ROWS);
                        let ids = rows.iter().map(|row| match row["id"] {
                            Value::Int(id) => id,
                            _ => panic!("expected integer primary key"),
                        }).collect::<std::collections::BTreeSet<_>>();
                        assert_eq!(ids, (0..LARGE_ROWS as i64).collect());
                        let id = 1000 + worker * SMALL_COMMITS + iteration;
                        tx.query_sql(&format!("INSERT INTO progress (id) VALUES ({id})"))
                            .unwrap();
                        tx.commit().unwrap();
                    }
                    SMALL_COMMITS
                })
            })
            .collect::<Vec<_>>();
        drop(attempted);
        let probes = (0..WORKERS)
            .map(|_| attempts.recv_timeout(Duration::from_secs(20)).unwrap())
            .collect::<Vec<_>>();
        second.commit().unwrap();
        let mut tx = begin(governor.try_admit_waiter(&large, large_request).unwrap());
        drop(large);
        for id in 0..LARGE_ROWS {
            tx.query_sql(&format!("INSERT INTO progress (id) VALUES ({id})"))
                .unwrap();
        }
        tx.commit().unwrap();
        let small_commits = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .sum::<usize>();
        assert!(probes.into_iter().all(|queued| queued));
        assert_eq!(small_commits, WORKERS * SMALL_COMMITS);
        assert_eq!(db.commit_epoch().unwrap(), epoch + 3 + small_commits as u64);
        let resources = governor.snapshot();
        assert_eq!(resources.active_cpu_slots, 0);
        assert_eq!(resources.active_foreground_tasks, 0);
        assert_eq!(resources.active_background_tasks, 0);
        assert_eq!(resources.admitted_memory_bytes, 0);
        let expected_ids = [-2, -1]
            .into_iter()
            .chain(0..LARGE_ROWS as i64)
            .chain(1000..1000 + small_commits as i64)
            .collect::<Vec<_>>();
        let query = "SELECT id FROM progress ORDER BY id";
        let rows = db.query_sql(query).unwrap().rows;
        assert_eq!(rows.len(), expected_ids.len());
        for (row, id) in rows.iter().zip(&expected_ids) {
            assert_eq!(row["id"], Value::Int(*id));
        }
        drop(db);
        if durable {
            let mut reopened = Database::open(&path).unwrap();
            assert_eq!(reopened.commit_epoch(), epoch + 3 + small_commits as u64);
            assert_eq!(reopened.query_sql(query).unwrap().rows, rows);
            drop(reopened);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn admitted_transaction_cancellation_while_queued_prevents_publication_and_refunds() {
    let path = super::super::unique_test_dir("admitted_cancel_queued");
    let db = ConcurrentDatabase::new_with_wal_group_commit(
        Database::open(&path).unwrap(),
        WalGroupCommitConfig::benchmark_candidate(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            Duration::ZERO,
        )
        .unwrap(),
    );
    let epoch = db.commit_epoch().unwrap();
    let governor = governor();
    let context = RuntimeTaskContext::default();
    let mut tx = db
        .begin_admitted_transaction(
            ConcurrentTransactionOptions::optimistic(),
            governor.try_admit(request()).unwrap(),
            context.clone(),
        )
        .unwrap();
    tx.query("CREATE (:CancelledAdmitted)").unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    db.set_group_commit_enqueue_gate(gate.clone()).unwrap();
    let handle = std::thread::spawn(move || tx.commit());
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while db.wal_group_commit_snapshot().unwrap().submitted_commits == 0
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(1));
    }
    let queued = db.wal_group_commit_snapshot().unwrap().submitted_commits;
    let active = governor.snapshot().active_foreground_tasks;
    context.cancellation().cancel();
    release_autocommit_reads(&gate);
    let result = handle.join().unwrap();
    assert_eq!(queued, 1);
    assert_eq!(active, 1);
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(governor.snapshot().active_foreground_tasks, 0);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    drop(db);
    let mut reopened = Database::open(&path).unwrap();
    assert!(reopened
        .query("MATCH (n:CancelledAdmitted) RETURN n")
        .unwrap()
        .rows
        .is_empty());
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn admitted_transaction_context_survives_refresh_and_stops_statements() {
    let db = ConcurrentDatabase::new(Database::new());
    let governor = governor();
    for options in [
        ConcurrentTransactionOptions::optimistic(),
        ConcurrentTransactionOptions::pessimistic(Duration::ZERO),
    ] {
        let context = RuntimeTaskContext::default();
        let mut tx = db
            .begin_admitted_transaction(
                options,
                governor.try_admit(request()).unwrap(),
                context.clone(),
            )
            .unwrap();
        tx.query("CREATE (:NeverPublished)").unwrap();
        context.cancellation().cancel();
        assert!(tx
            .query("RETURN 1")
            .unwrap_err()
            .to_string()
            .contains("cancelled"));
        assert!(tx
            .query_sql("SELECT 1")
            .unwrap_err()
            .to_string()
            .contains("cancelled"));
        assert!(tx.commit().unwrap_err().to_string().contains("cancelled"));
        assert_eq!(governor.snapshot().active_foreground_tasks, 0);
    }
    assert!(db
        .query("MATCH (n:NeverPublished) RETURN n")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn admitted_transaction_read_result_budget_is_propagated_to_both_languages() {
    let db = ConcurrentDatabase::new(Database::new());
    let governor = governor();
    db.query("CREATE (:Budgeted {body: 'a deliberately nonempty payload'})")
        .unwrap();
    db.query_sql("CREATE TABLE budgeted (body TEXT PRIMARY KEY)")
        .unwrap();
    db.query_sql("INSERT INTO budgeted (body) VALUES ('a deliberately nonempty payload')")
        .unwrap();
    for sql in [false, true] {
        let mut tx = db
            .begin_admitted_transaction(
                ConcurrentTransactionOptions::optimistic(),
                governor.try_admit(request().with_result_bytes(1)).unwrap(),
                RuntimeTaskContext::default(),
            )
            .unwrap();
        let result = if sql {
            tx.query_sql("SELECT body FROM budgeted")
        } else {
            tx.query("MATCH (n:Budgeted) RETURN n.body")
        };
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("budget") || message.contains("max_output_payload_bytes 1"),
            "{message}"
        );
        tx.rollback();
        assert_eq!(governor.snapshot().active_foreground_tasks, 0);
    }
}

#[test]
fn admitted_group_failure_preserves_conflicts_and_recovers_only_complete_serial_prefixes() {
    use hawdb_storage::wal::{WalCursorEvent, WalOp, WalOpenOutcome, WalRecordCursor};

    for overlap in [false, true] {
        let path = super::super::unique_test_dir("admitted_group_failure_prefix");
        let mut database = Database::open(&path).unwrap();
        for id in 1..=4 {
            database
                .query(&format!("CREATE (:RecoveryPair {{id: {id}, value: 0}})"))
                .unwrap();
        }
        database.checkpoint().unwrap();
        database
            .query("CREATE (:AcknowledgedBeforeFailure {value: 99})")
            .unwrap();
        let acknowledged_bytes = std::fs::metadata(super::super::active_wal_path(&path))
            .unwrap()
            .len();
        let db = ConcurrentDatabase::new_with_wal_group_commit(
            database,
            WalGroupCommitConfig::benchmark_candidate(
                NonZeroUsize::new(2).unwrap(),
                NonZeroU64::new(1024 * 1024).unwrap(),
                Duration::ZERO,
            )
            .unwrap(),
        );
        let epoch = db.commit_epoch().unwrap();
        let governor = governor();
        let query = "MATCH (n:RecoveryPair) RETURN n.id AS id, n.value AS value ORDER BY id";
        let mut old_reader = db.begin_read_transaction().unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        db.set_group_commit_enqueue_gate(gate.clone()).unwrap();
        // Stage both workspaces before either can publish. Each transaction
        // mutates two records, so recovery must not expose half of a pair.
        let transactions = (1..=2).map(|writer| {
            let mut tx = db.begin_admitted_transaction(ConcurrentTransactionOptions::optimistic(), governor.try_admit(request()).unwrap(), RuntimeTaskContext::default()).unwrap();
            let first = if overlap { 1 } else { 2 * writer - 1 };
            tx.query_with_params("MATCH (n:RecoveryPair) WHERE n.id = $first OR n.id = $second SET n.value = $value", &BTreeMap::from([
                ("first".into(), Value::Int(first)), ("second".into(), Value::Int(first + 1)), ("value".into(), Value::Int(writer)),
            ])).unwrap();
            tx
        }).collect::<Vec<_>>();
        let handles = transactions
            .into_iter()
            .map(|tx| {
                std::thread::spawn(move || {
                    // The failpoint is thread-local. Arm each possible group leader.
                    crate::store::set_wal_group_sync_failpoint(true);
                    let result = tx.commit();
                    crate::store::set_wal_group_sync_failpoint(false);
                    result
                })
            })
            .collect::<Vec<_>>();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while db.wal_group_commit_snapshot().unwrap().submitted_commits < 2
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        let queued = db.wal_group_commit_snapshot().unwrap().submitted_commits;
        let reserved = governor.snapshot().active_foreground_tasks;
        release_autocommit_reads(&gate);
        let errors = handles
            .into_iter()
            .map(|h| h.join().unwrap().unwrap_err())
            .collect::<Vec<_>>();
        assert_eq!(queued, 2);
        assert_eq!(reserved, 2);
        let accepted = if overlap { 1 } else { 2 };
        assert_eq!(
            errors
                .iter()
                .filter(|e| matches!(e, HawDBError::StorageIntegrity(_)))
                .count(),
            accepted
        );
        assert_eq!(
            errors
                .iter()
                .filter(|e| e.is_retryable_transaction_conflict())
                .count(),
            2 - accepted
        );
        for error in errors
            .iter()
            .filter(|e| matches!(e, HawDBError::StorageIntegrity(_)))
        {
            assert!(error
                .to_string()
                .contains("WAL group durability barrier failed"));
        }
        assert_eq!(governor.snapshot().active_foreground_tasks, 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        let metrics = db.wal_group_commit_snapshot().unwrap();
        assert_eq!(metrics.group_count, 1);
        assert_eq!(metrics.completed_commits, 0);
        assert_eq!(metrics.shared_sync_count, 0);
        let wal_path = super::super::active_wal_path(&path);
        let full_wal = std::fs::read(&wal_path).unwrap();
        assert!(db
            .query(query)
            .unwrap_err()
            .to_string()
            .contains("close and reopen"));
        assert!(old_reader
            .query(query)
            .unwrap_err()
            .to_string()
            .contains("close and reopen"));
        assert!(db
            .query("CREATE (:MustNotPublish)")
            .unwrap_err()
            .to_string()
            .contains("close and reopen"));
        assert!(db
            .checkpoint()
            .unwrap_err()
            .to_string()
            .contains("close and reopen"));
        assert_eq!(std::fs::read(&wal_path).unwrap(), full_wal);
        drop(old_reader);
        drop(db);

        let WalOpenOutcome::Cursor(mut cursor) = WalRecordCursor::open(&wal_path, None).unwrap()
        else {
            panic!("expected a valid WAL header")
        };
        let mut records = Vec::new();
        let mut record_writers = Vec::new();
        let mut acknowledged_records = 0;
        loop {
            match cursor.next().unwrap() {
                WalCursorEvent::Entry {
                    start_offset,
                    encoded_len,
                    entry,
                    ..
                } => {
                    if start_offset < acknowledged_bytes {
                        acknowledged_records += 1;
                        continue;
                    }
                    let WalOp::Batch(ops) = entry.op else {
                        panic!("expected atomic mutation batch")
                    };
                    let values = ops
                        .iter()
                        .filter_map(|op| match op {
                            WalOp::SetNodeProperty {
                                property,
                                value: Value::Int(writer),
                                ..
                            } if property == "value" => Some(*writer),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(values.len(), 2);
                    assert_eq!(values[0], values[1]);
                    assert!((1..=2).contains(&values[0]));
                    record_writers.push(values[0]);
                    records.push((start_offset as usize, encoded_len as usize));
                }
                WalCursorEvent::Eof => break,
                _ => panic!("failed sync must retain complete written frames in this fixture"),
            }
        }
        drop(cursor);
        assert_eq!(acknowledged_records, 1);
        assert_eq!(records.len(), accepted);
        assert_eq!(records[0].0 as u64, acknowledged_bytes);
        if overlap {
            let winner = errors
                .iter()
                .position(|e| matches!(e, HawDBError::StorageIntegrity(_)))
                .unwrap() as i64
                + 1;
            assert_eq!(record_writers, vec![winner]);
        }
        // Enumerate every readable surviving prefix of the uncertain group.
        // These explicit cuts simulate persisted bytes, not a power-loss test.
        for retained in 0..=records.len() {
            let end = if retained == 0 {
                records[0].0
            } else {
                let (start, len) = records[retained - 1];
                start + len
            };
            std::fs::write(&wal_path, &full_wal[..end]).unwrap();
            let mut reopened = Database::open(&path).unwrap();
            assert_eq!(reopened.commit_epoch(), epoch + retained as u64);
            let rows = reopened.query(query).unwrap().rows;
            assert_eq!(rows.len(), 4);
            assert_eq!(rows[0]["value"], rows[1]["value"]);
            assert_eq!(rows[2]["value"], rows[3]["value"]);
            assert_eq!(
                rows.iter()
                    .filter(|row| row["value"] != Value::Int(0))
                    .count(),
                2 * retained
            );
            let mut expected = vec![Value::Int(0); 4];
            for writer in &record_writers[..retained] {
                let index = if overlap {
                    0
                } else {
                    2 * (*writer as usize - 1)
                };
                expected[index] = Value::Int(*writer);
                expected[index + 1] = Value::Int(*writer);
            }
            assert_eq!(
                rows.iter()
                    .map(|row| row["value"].clone())
                    .collect::<Vec<_>>(),
                expected
            );
            let acknowledged = reopened
                .query("MATCH (n:AcknowledgedBeforeFailure) RETURN n.value AS value")
                .unwrap();
            assert_eq!(acknowledged.rows.len(), 1);
            assert_eq!(acknowledged.rows[0]["value"], Value::Int(99));
            drop(reopened);
        }
        // A partially surviving frame must fail closed without automatic repair.
        for (start, len) in &records {
            let torn = &full_wal[..start + len / 2];
            std::fs::write(&wal_path, torn).unwrap();
            assert!(Database::open(&path)
                .unwrap_err()
                .to_string()
                .contains("strict WAL recovery rejected torn tail"));
            assert_eq!(std::fs::read(&wal_path).unwrap(), torn);
        }
        if records.len() == 2 {
            // Valid individual frames in the wrong order are not a serial
            // prefix. Recovery must reject their LSN order without repair.
            let (first_start, first_len) = records[0];
            let (second_start, second_len) = records[1];
            let mut reordered = full_wal[..first_start].to_vec();
            reordered.extend_from_slice(&full_wal[second_start..second_start + second_len]);
            reordered.extend_from_slice(&full_wal[first_start..first_start + first_len]);
            std::fs::write(&wal_path, &reordered).unwrap();
            let error = Database::open(&path).unwrap_err().to_string();
            assert!(error.contains("LSN") || error.contains("lsn"), "{error}");
            assert_eq!(std::fs::read(&wal_path).unwrap(), reordered);
        }
        std::fs::write(&wal_path, &full_wal).unwrap();
        let mut reopened = Database::open(&path).unwrap();
        reopened.query("CREATE (:AfterReopen)").unwrap();
        assert_eq!(reopened.commit_epoch(), epoch + accepted as u64 + 1);
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn admitted_conflict_retry_escalates_before_recurring_hot_writers() {
    const WORKERS: usize = 4;
    const SMALL_COMMITS: usize = 16;
    const LARGE_ROWS: usize = 64;
    for durable in [false, true] {
        let path = super::super::unique_test_dir("admitted_conflict_escalation");
        let mut database = if durable {
            Database::open(&path).unwrap()
        } else {
            Database::new()
        };
        database
            .query_sql("CREATE TABLE retry_progress (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)")
            .unwrap();
        database
            .query_sql("INSERT INTO retry_progress (id, value) VALUES (0, 0)")
            .unwrap();
        let db = ConcurrentDatabase::new_with_wal_group_commit(
            database,
            WalGroupCommitConfig::benchmark_candidate(
                NonZeroUsize::new(4).unwrap(),
                NonZeroU64::new(1024 * 1024).unwrap(),
                Duration::from_micros(100),
            )
            .unwrap(),
        );
        let governor = governor();
        let begin = |permit| {
            db.begin_admitted_transaction(
                ConcurrentTransactionOptions::optimistic(),
                permit,
                RuntimeTaskContext::default(),
            )
            .unwrap()
        };
        let large_work = |tx: &mut crate::ConcurrentDatabaseTransaction| {
            tx.query_sql("UPDATE retry_progress SET value = value + 100 WHERE id = 0")
                .unwrap();
            for id in 1..=LARGE_ROWS {
                tx.query_sql_with_params(
                    "INSERT INTO retry_progress (id, value) VALUES ($1, $1)",
                    &[Value::Int(id as i64)],
                )
                .unwrap();
            }
        };
        let epoch = db.commit_epoch().unwrap();
        let mut first_attempt = begin(governor.try_admit(request()).unwrap());
        large_work(&mut first_attempt);
        let mut winner = begin(governor.try_admit(request()).unwrap());
        winner
            .query_sql("UPDATE retry_progress SET value = value + 1 WHERE id = 0")
            .unwrap();
        winner.commit().unwrap();
        // One owner survives the conflict, so escalation must wait without
        // allowing recurring one-slot work to consume the other free slot.
        let mut older = begin(governor.try_admit(request()).unwrap());
        older
            .query_sql("UPDATE retry_progress SET value = value + 1 WHERE id = 0")
            .unwrap();
        let wal_before =
            durable.then(|| std::fs::read(super::super::active_wal_path(&path)).unwrap());
        let error = first_attempt.commit().unwrap_err();
        assert!(
            matches!(&error, HawDBError::TransactionConflict { key, .. } if key == "relational_row"),
            "{error}"
        );
        assert!(error.is_retryable_transaction_conflict());
        if let Some(before) = wal_before {
            assert_eq!(
                std::fs::read(super::super::active_wal_path(&path)).unwrap(),
                before
            );
        }
        let rejected = db
            .query_sql("SELECT id, value FROM retry_progress")
            .unwrap()
            .rows;
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0]["value"], Value::Int(1));
        assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
        assert_eq!(governor.snapshot().active_cpu_slots, 1);

        // This is an explicit host policy: only a known pre-publication
        // transaction conflict permits replay; uncertain I/O failures do not.
        let retry = governor.admission_waiter(RuntimeWorkPriority::Foreground);
        let retry_request = request().with_cpu_slots(2);
        assert_eq!(
            governor
                .try_admit_waiter(&retry, retry_request)
                .unwrap_err()
                .code,
            RuntimeAdmissionCode::CpuSaturated
        );
        assert_eq!(
            governor.try_admit(request()).unwrap_err().code,
            RuntimeAdmissionCode::QueuedAhead
        );
        let (sent, received) = mpsc::channel();
        let handles = (0..WORKERS)
            .map(|worker| {
                let db = db.clone();
                let governor = governor.clone();
                let sent = sent.clone();
                std::thread::spawn(move || {
                    let deadline = std::time::Instant::now() + Duration::from_secs(60);
                    let mut sent = Some(sent);
                    for iteration in 0..SMALL_COMMITS {
                        loop {
                            let waiter = governor.admission_waiter(RuntimeWorkPriority::Foreground);
                            if iteration == 0 {
                                // All initial requests arrive before the older
                                // owner retires and before the large retry runs.
                                if let Some(sent) = sent.take() {
                                    assert_eq!(
                                        governor
                                            .try_admit_waiter(&waiter, request())
                                            .unwrap_err()
                                            .code,
                                        RuntimeAdmissionCode::QueuedAhead
                                    );
                                    sent.send(()).unwrap();
                                }
                            }
                            let permit = loop {
                                match governor.try_admit_waiter(&waiter, request()) {
                                    Ok(permit) => break permit,
                                    Err(error) => {
                                        assert!(error.is_retryable(), "{error}");
                                        assert!(
                                            std::time::Instant::now() < deadline,
                                            "admission timed out: {error}"
                                        );
                                        std::thread::sleep(Duration::from_millis(1));
                                    }
                                }
                            };
                            let mut tx = db
                                .begin_admitted_transaction(
                                    ConcurrentTransactionOptions::optimistic(),
                                    permit,
                                    RuntimeTaskContext::default(),
                                )
                                .unwrap();
                            let rows = tx
                                .query_sql(
                                    "SELECT id FROM retry_progress WHERE id >= 1 AND id <= 64",
                                )
                                .unwrap()
                                .rows;
                            assert_eq!(rows.len(), LARGE_ROWS);
                            tx.query_sql(
                                "UPDATE retry_progress SET value = value + 1 WHERE id = 0",
                            )
                            .unwrap();
                            let id = 1000 + worker * SMALL_COMMITS + iteration;
                            tx.query_sql_with_params(
                                "INSERT INTO retry_progress (id, value) VALUES ($1, $1)",
                                &[Value::Int(id as i64)],
                            )
                            .unwrap();
                            match tx.commit() {
                                Ok(_) => break,
                                Err(error) => {
                                    assert!(error.is_retryable_transaction_conflict(), "{error}");
                                    assert!(
                                        std::time::Instant::now() < deadline,
                                        "small writer retry timed out"
                                    );
                                }
                            }
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        drop(sent);
        for _ in 0..WORKERS {
            received.recv_timeout(Duration::from_secs(20)).unwrap();
        }
        older.commit().unwrap();
        let mut second_attempt = begin(governor.try_admit_waiter(&retry, retry_request).unwrap());
        drop(retry);
        assert_eq!(
            second_attempt
                .query_sql("SELECT value FROM retry_progress WHERE id = 0")
                .unwrap()
                .rows[0]["value"],
            Value::Int(2)
        );
        large_work(&mut second_attempt);
        second_attempt.commit().unwrap();
        for handle in handles {
            handle.join().unwrap();
        }
        let rows = db
            .query_sql("SELECT id, value FROM retry_progress ORDER BY id")
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1 + LARGE_ROWS + WORKERS * SMALL_COMMITS);
        assert_eq!(
            rows[0]["value"],
            Value::Int((102 + WORKERS * SMALL_COMMITS) as i64)
        );
        let ids = (1..=LARGE_ROWS).chain(1000..1000 + WORKERS * SMALL_COMMITS);
        for (row, id) in rows.iter().skip(1).zip(ids) {
            assert_eq!(row["id"], Value::Int(id as i64));
            assert_eq!(row["value"], Value::Int(id as i64));
        }
        let final_epoch = epoch + 3 + (WORKERS * SMALL_COMMITS) as u64;
        assert_eq!(db.commit_epoch().unwrap(), final_epoch);
        let resources = governor.snapshot();
        assert_eq!(resources.active_cpu_slots, 0);
        assert_eq!(resources.active_foreground_tasks, 0);
        assert_eq!(resources.admitted_memory_bytes, 0);
        assert_eq!(resources.queued_admission_waiters, 0);
        drop(db);
        if durable {
            let mut reopened = Database::open(&path).unwrap();
            assert_eq!(reopened.commit_epoch(), final_epoch);
            assert_eq!(
                reopened
                    .query_sql("SELECT id, value FROM retry_progress ORDER BY id")
                    .unwrap()
                    .rows,
                rows
            );
            drop(reopened);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
