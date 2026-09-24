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
