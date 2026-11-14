// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn transaction(db: &ConcurrentDatabase) -> crate::ConcurrentDatabaseTransaction {
    db.enable_concurrency_metrics();
    db.begin_transaction(ConcurrentTransactionOptions::pessimistic(Duration::ZERO))
        .unwrap()
}

fn update(id: i64, value: i64) -> String {
    format!("MATCH (n:Memory) WHERE n.id = {id} SET n.value = {value} RETURN n.value AS value")
}

#[test]
fn covered_graph_mutations_reuse_staging_across_disjoint_commits_and_reopen() {
    let path = super::super::unique_test_dir("covered_graph_reuse");
    let db = ConcurrentDatabase::open(&path).unwrap();
    for id in 1..=3 {
        db.query(&format!("CREATE (:Memory {{id: {id}, value: 0}})"))
            .unwrap();
    }
    let before = db.concurrency_metrics();
    let mut owner = transaction(&db);
    owner.query(&update(1, 1)).unwrap();
    let mut other = transaction(&db);
    other.query(&update(2, 2)).unwrap();
    other.commit().unwrap();

    let before_reuse = db.concurrency_metrics();
    let output = owner.query(&update(1, 3)).unwrap();
    assert_eq!(output.rows[0]["value"], Value::Int(3));
    let after = db.concurrency_metrics();
    assert_eq!(
        after.sequencer_wait.count, before_reuse.sequencer_wait.count,
        "a covered mutation does not reacquire the sequencer"
    );
    assert_eq!(
        after.graph_statement_executions - before.graph_statement_executions,
        3
    );
    assert_eq!(
        after.graph_statement_replays - before.graph_statement_replays,
        2
    );
    assert_eq!(
        after.graph_statement_reuses - before.graph_statement_reuses,
        1
    );
    owner.commit().unwrap();
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    let rows = reopened
        .query("MATCH (n:Memory) RETURN n.value AS value ORDER BY n.id")
        .unwrap()
        .rows;
    assert_eq!(
        rows.iter()
            .map(|row| row["value"].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(3), Value::Int(2), Value::Int(0)]
    );
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn new_graph_targets_still_replay_and_stale_snapshots_still_abort() {
    let db = Database::new().into_concurrent();
    for id in 1..=3 {
        db.query(&format!("CREATE (:Memory {{id: {id}, value: 0}})"))
            .unwrap();
    }
    let mut owner = transaction(&db);
    owner.query(&update(1, 1)).unwrap();
    owner.query(&update(2, 2)).unwrap();
    assert_eq!(db.concurrency_metrics().graph_statement_replays, 2);
    assert_eq!(db.concurrency_metrics().graph_statement_reuses, 0);
    let mut other = transaction(&db);
    other.query(&update(3, 3)).unwrap();
    other.commit().unwrap();
    // A new entity requires acquisition and the existing epoch fence. It must
    // not accidentally inherit reuse from the locks on the first two nodes.
    assert!(owner
        .query(&update(3, 4))
        .unwrap_err()
        .to_string()
        .contains("cannot acquire a new lock after its snapshot changed"));
    assert!(owner.commit().is_err());
    let rows = db
        .query("MATCH (n:Memory) RETURN n.value AS value ORDER BY n.id")
        .unwrap()
        .rows;
    assert_eq!(
        rows.iter()
            .map(|row| row["value"].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(0), Value::Int(0), Value::Int(3)]
    );
}

#[test]
fn covered_graph_reuse_preserves_rollback_and_competing_lock_timeout() {
    let db = Database::new().into_concurrent();
    db.query("CREATE (:Memory {id: 1, value: 0})").unwrap();
    let mut owner = transaction(&db);
    owner.query(&update(1, 1)).unwrap();
    owner.query(&update(1, 2)).unwrap();
    let mut waiter = transaction(&db);
    assert!(waiter
        .query(&update(1, 3))
        .unwrap_err()
        .to_string()
        .contains("lock wait timed out"));
    owner.rollback();
    waiter.query(&update(1, 4)).unwrap();
    waiter.commit().unwrap();
    assert_eq!(
        db.query("MATCH (n:Memory) RETURN n.value AS value")
            .unwrap()
            .rows[0]["value"],
        Value::Int(4)
    );
}

#[test]
fn covered_graph_reuse_keeps_failed_statement_atomic_and_replays_lock_upgrade() {
    let db = Database::new().into_concurrent();
    db.query("CREATE (:Memory {id: 1, value: 0})").unwrap();
    db.query("CREATE (:Memory {id: 2, value: 0})").unwrap();
    db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
        .unwrap();
    let mut owner = transaction(&db);
    owner.query(&update(1, 1)).unwrap();
    owner.query(&update(1, 2)).unwrap();
    assert!(owner
        .query("MATCH (n:Memory) WHERE n.id = 1 SET n.id = 2")
        .unwrap_err()
        .to_string()
        .contains("unique constraint violation"));
    owner
        .query("MATCH (n:Memory) WHERE n.id = 1 SET n.id = 3")
        .unwrap();
    let metrics = db.concurrency_metrics();
    assert_eq!(
        metrics.graph_statement_replays, 2,
        "new label lock mode requires replay"
    );
    assert_eq!(metrics.graph_statement_reuses, 1);
    owner.commit().unwrap();
    assert_eq!(
        db.query("MATCH (n:Memory) WHERE n.id = 3 RETURN n.value AS value")
            .unwrap()
            .rows[0]["value"],
        Value::Int(2)
    );
}
