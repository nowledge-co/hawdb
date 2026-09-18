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

use crate::{
    AppendGeneratedRow, AppendTransaction, AppendWrite, ConcurrentDatabase,
    ConcurrentTransactionOptions, Database, HawDBError, RelationalValue, Value,
    WalGroupCommitActivation, WalGroupCommitAdaptiveColdStartEvidence,
    WalGroupCommitAdaptivePolicyEvidence, WalGroupCommitAdaptiveSteadyStateEvidence,
    WalGroupCommitConfig, WalGroupCommitDelayPolicy, WalGroupCommitEvidence,
    WalGroupCommitTailLatencyEvidence, WalGroupCommitWaitDecision,
};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{mpsc, Arc, Barrier, Condvar, Mutex};
use std::time::Duration;

mod read_observation;

fn release_autocommit_reads(release: &Arc<(Mutex<bool>, Condvar)>) {
    let (released, available) = &**release;
    *released.lock().unwrap() = true;
    available.notify_all();
}

#[test]
fn concurrent_database_checkpoint_publishes_an_immutable_cut() {
    let path = super::unique_test_dir("concurrent_checkpoint_immutable_cut");
    let db = Database::open(&path).unwrap().into_concurrent();
    db.query("CREATE (:Memory {id: 1})").unwrap();
    db.checkpoint().unwrap();
    assert_eq!(db.commit_epoch().unwrap(), 2);
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    let rows = reopened
        .query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0].get("id"), Some(&Value::Int(1)));
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_autocommit_statements_overlap_after_snapshot_acquisition() {
    let db = Database::new().into_concurrent();
    db.query("CREATE (:Memory {id: 1, title: 'graph'})")
        .unwrap();
    db.query_sql("CREATE TABLE messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    db.query_sql("INSERT INTO messages (id, body) VALUES (1, 'relational')")
        .unwrap();

    let (snapshot_acquired, snapshots) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    db.set_autocommit_read_gate(snapshot_acquired, Arc::clone(&release))
        .unwrap();

    let graph_db = db.clone();
    let graph = std::thread::spawn(move || {
        graph_db.query("MATCH (m:Memory {id: 1}) RETURN m.title AS title")
    });
    let relational_db = db.clone();
    let relational = std::thread::spawn(move || {
        relational_db.query_sql("SELECT body FROM messages WHERE id = 1")
    });

    for _ in 0..2 {
        snapshots
            .recv_timeout(Duration::from_secs(5))
            .expect("both read-only statements must acquire snapshots");
    }
    db.clear_autocommit_read_gate().unwrap();
    release_autocommit_reads(&release);

    let graph = graph.join().unwrap().unwrap();
    assert_eq!(
        graph.rows[0].get("title"),
        Some(&Value::String("graph".to_string()))
    );
    let relational = relational.join().unwrap().unwrap();
    assert_eq!(
        relational.rows[0].get("body"),
        Some(&Value::String("relational".to_string()))
    );
}

#[test]
fn snapshot_autocommit_read_does_not_hold_the_writer_gate() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    db.query_sql("INSERT INTO messages (id, body) VALUES (1, 'before')")
        .unwrap();

    let (snapshot_acquired, snapshots) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    db.set_autocommit_read_gate(snapshot_acquired, Arc::clone(&release))
        .unwrap();

    let reader_db = db.clone();
    let reader =
        std::thread::spawn(move || reader_db.query_sql("SELECT body FROM messages WHERE id = 1"));
    snapshots
        .recv_timeout(Duration::from_secs(5))
        .expect("read-only statement must acquire its snapshot");

    let (writer_done, writer_result) = mpsc::channel();
    let writer_db = db.clone();
    let writer = std::thread::spawn(move || {
        let result = writer_db.query_sql("UPDATE messages SET body = 'after' WHERE id = 1");
        writer_done.send(result).unwrap();
    });
    let write = match writer_result.recv_timeout(Duration::from_secs(5)) {
        Ok(write) => write,
        Err(error) => {
            db.clear_autocommit_read_gate().unwrap();
            release_autocommit_reads(&release);
            let _ = reader.join();
            panic!("writer remained blocked after the read snapshot was acquired: {error}");
        }
    };
    write.unwrap();

    db.clear_autocommit_read_gate().unwrap();
    release_autocommit_reads(&release);
    writer.join().unwrap();
    let pinned = reader.join().unwrap().unwrap();
    assert_eq!(
        pinned.rows[0].get("body"),
        Some(&Value::String("before".to_string()))
    );
    let current = db
        .query_sql("SELECT body FROM messages WHERE id = 1")
        .unwrap();
    assert_eq!(
        current.rows[0].get("body"),
        Some(&Value::String("after".to_string()))
    );
}

#[test]
fn snapshot_autocommit_reads_remain_observable() {
    let db = Database::new_with_config(crate::DatabaseConfig {
        slow_query_log_threshold_micros: 0,
        ..crate::DatabaseConfig::default()
    })
    .into_concurrent();
    db.query("CREATE (:Memory {id: 1})").unwrap();
    db.query("MATCH (m:Memory {id: 1}) RETURN m.id AS id")
        .unwrap();

    let slow = db
        .query_sql(
            "SELECT query_language, statement_kind, row_count FROM system.slow_queries \
             WHERE query_language = 'cypher' AND statement_kind = 'match_return'",
        )
        .unwrap();
    assert_eq!(slow.rows.len(), 1);
    assert_eq!(slow.rows[0].get("row_count"), Some(&Value::Int(1)));

    let summary = db
        .query_sql(
            "SELECT query_language, statement_kind, execution_count, success_count \
             FROM system.statement_summary \
             WHERE query_language = 'cypher' AND statement_kind = 'match_return'",
        )
        .unwrap();
    assert_eq!(summary.rows.len(), 1);
    assert_eq!(summary.rows[0].get("execution_count"), Some(&Value::Int(1)));
    assert_eq!(summary.rows[0].get("success_count"), Some(&Value::Int(1)));
}

#[test]
fn autocommit_explain_of_a_mutation_remains_non_mutating() {
    let db = Database::new().into_concurrent();
    let explain = db.query("EXPLAIN CREATE (:Memory {id: 1})").unwrap();
    assert_eq!(explain.rows.len(), 1);

    let rows = db
        .query("MATCH (m:Memory {id: 1}) RETURN m.id AS id")
        .unwrap();
    assert!(rows.rows.is_empty());
}

#[test]
fn optimistic_transactions_prepare_in_parallel_and_reject_the_stale_committer() {
    let db = Database::new().into_concurrent();
    let barrier = Arc::new(Barrier::new(2));
    let handles = (1..=2)
        .map(|id| {
            let db = db.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut tx = db
                    .begin_transaction(ConcurrentTransactionOptions::optimistic())
                    .unwrap();
                tx.query(&format!("CREATE (:Memory {{id: {id}}})")).unwrap();
                barrier.wait();
                tx.commit()
            })
        })
        .collect::<Vec<_>>();

    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let conflict = results
        .iter()
        .find_map(|result| result.as_ref().err())
        .expect("one optimistic transaction must conflict");
    assert!(matches!(conflict, HawDBError::Execution(_)));
    assert!(conflict
        .to_string()
        .contains("optimistic transaction conflict"));

    let output = db
        .query("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(db.commit_epoch().unwrap(), 1);
}

#[test]
fn optimistic_conflict_noop_commits_converge_to_inserted_and_conflict_results() {
    let db = Database::new().into_concurrent();
    db.query_sql(
        "CREATE TABLE public.raw_turns (\
            raw_turn_id TEXT PRIMARY KEY, \
            org_id TEXT NOT NULL, \
            request_id TEXT NOT NULL, \
            UNIQUE (org_id, request_id)\
        )",
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let handles = (1..=2)
        .map(|id| {
            let db = db.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut tx = db
                    .begin_transaction(ConcurrentTransactionOptions::optimistic())
                    .unwrap();
                let staged = tx
                    .query_sql_with_result(&format!(
                        "INSERT INTO public.raw_turns (raw_turn_id, org_id, request_id) \
                         VALUES ('turn-{id}', 'org-1', 'request-1') \
                         ON CONFLICT (org_id, request_id) DO NOTHING \
                         RETURNING raw_turn_id"
                    ))
                    .unwrap();
                assert_eq!(staged.mutation.unwrap().affected_rows, 1);
                barrier.wait();
                tx.commit_with_result()
            })
        })
        .collect::<Vec<_>>();

    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        results
            .iter()
            .filter(|result| result.mutations[0].affected_rows == 1)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| result.mutations[0].conflict_rows == 1)
            .count(),
        1
    );
    assert_eq!(
        db.query_sql("SELECT raw_turn_id FROM public.raw_turns")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn pessimistic_group_commit_preserves_inserted_and_conflict_results() {
    const WRITERS: usize = 2;
    let path = super::unique_test_dir("idempotent_group_commit_results");
    let mut database = Database::open(&path).unwrap();
    database
        .query_sql(
            "CREATE TABLE public.raw_turns (\
                raw_turn_id TEXT PRIMARY KEY, \
                request_id TEXT UNIQUE NOT NULL\
            )",
        )
        .unwrap();
    database
        .query_sql(
            "INSERT INTO public.raw_turns (raw_turn_id, request_id) \
             VALUES ('turn-1', 'request-1')",
        )
        .unwrap();
    let group_commit = WalGroupCommitConfig::benchmark_candidate(
        NonZeroUsize::new(WRITERS).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::from_millis(5),
    )
    .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(database, group_commit);
    db.set_group_commit_post_enqueue_barrier(Arc::new(Barrier::new(WRITERS)))
        .unwrap();

    let writers = [("turn-duplicate", "request-1"), ("turn-2", "request-2")]
        .into_iter()
        .map(|(raw_turn_id, request_id)| {
            let db = db.clone();
            std::thread::spawn(move || {
                let mut transaction = db
                    .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                        Duration::from_secs(1),
                    ))
                    .unwrap();
                transaction
                    .query_sql_with_result(&format!(
                        "INSERT INTO public.raw_turns (raw_turn_id, request_id) \
                         VALUES ('{raw_turn_id}', '{request_id}') \
                         ON CONFLICT (request_id) DO NOTHING \
                         RETURNING raw_turn_id"
                    ))
                    .unwrap();
                transaction.commit_with_result().unwrap()
            })
        })
        .collect::<Vec<_>>();
    let results = writers
        .into_iter()
        .map(|writer| writer.join().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        results
            .iter()
            .filter(|result| result.mutations[0].affected_rows == 1)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| result.mutations[0].conflict_rows == 1)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .flat_map(|result| result.output.rows.iter())
            .filter(|row| row.get("raw_turn_id") == Some(&Value::String("turn-2".to_string())))
            .count(),
        1
    );
    let snapshot = db.wal_group_commit_snapshot().unwrap();
    assert_eq!(snapshot.submitted_commits, WRITERS as u64);
    assert_eq!(snapshot.completed_commits, WRITERS as u64);
    assert_eq!(snapshot.shared_sync_count, 1);
    assert_eq!(snapshot.grouped_wal_entries, WRITERS as u64);
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    let rows = reopened
        .query_sql("SELECT raw_turn_id FROM public.raw_turns ORDER BY raw_turn_id")
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn optimistic_transaction_reads_its_private_workspace() {
    let db = Database::new().into_concurrent();
    let mut tx = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    assert_eq!(tx.base_commit_epoch(), 0);
    tx.query("CREATE (:Memory {id: 1, title: 'private'})")
        .unwrap();

    let staged = tx
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        staged.rows[0].get("title"),
        Some(&Value::String("private".to_string()))
    );
    let mut outside = db.begin_read_transaction().unwrap();
    assert!(outside
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());

    tx.commit().unwrap();
    let mut committed = db.begin_read_transaction().unwrap();
    assert_eq!(
        committed
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn pessimistic_transaction_blocks_other_writers_with_a_bounded_wait() {
    let db = Database::new().into_concurrent();
    let mut owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    owner.query("CREATE (:Memory {id: 1})").unwrap();

    let contender = {
        let db = db.clone();
        std::thread::spawn(move || {
            let mut contender = db
                .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                    Duration::from_millis(25),
                ))
                .unwrap();
            contender.query("CREATE (:Memory {id: 2})")
        })
    };
    let error = contender.join().unwrap().unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));

    owner.rollback();
    let next_owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    next_owner.rollback();
}

#[test]
fn disjoint_graph_node_updates_can_stage_concurrently() {
    let db = Database::new().into_concurrent();
    db.query("CREATE (:Memory {id: 1, state: 'before'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, state: 'before'})")
        .unwrap();
    let mut first = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    let mut second = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();

    first
        .query("MATCH (m:Memory) WHERE id(m) = 0 SET m.state = 'first'")
        .unwrap();
    second
        .query("MATCH (m:Memory) WHERE id(m) = 1 SET m.other_state = 'second'")
        .unwrap();
    first.commit().unwrap();
    second.commit().unwrap();

    let rows = db
        .query("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 2);
}

#[test]
fn graph_create_allocation_lock_prevents_duplicate_physical_ids() {
    let db = Database::new().into_concurrent();
    let mut owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    owner.query("CREATE (:Memory {id: 1})").unwrap();

    let mut waiter = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    let error = waiter.query("CREATE (:Memory {id: 2})").unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    waiter.rollback();
    owner.commit().unwrap();
}

#[test]
fn graph_unique_property_updates_serialize_by_constraint_subject() {
    let db = Database::new().into_concurrent();
    db.query("CREATE CONSTRAINT ON :Memory(slug) ASSERT UNIQUE")
        .unwrap();
    db.query("CREATE (:Memory {slug: 'first'})").unwrap();
    db.query("CREATE (:Memory {slug: 'second'})").unwrap();
    let mut owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    owner
        .query("MATCH (m:Memory) WHERE id(m) = 0 SET m.slug = 'shared'")
        .unwrap();

    let mut waiter = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    let error = waiter
        .query("MATCH (m:Memory) WHERE id(m) = 1 SET m.slug = 'shared'")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    owner.rollback();
}

#[test]
fn graph_lock_derivation_rejects_a_changed_snapshot() {
    let db = Database::new().into_concurrent();
    db.query("CREATE (:Memory {id: 1, state: 'before'})")
        .unwrap();
    let mut stale = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();

    db.query("CREATE (:Entity {id: 2})").unwrap();

    let error = stale
        .query("MATCH (m:Memory) WHERE m.id = 1 SET m.state = 'after'")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("cannot acquire a graph lock after its snapshot changed"));
}

#[test]
fn relationship_creation_conflicts_with_endpoint_delete_guard() {
    let db = Database::new().into_concurrent();
    db.query("CREATE (:Memory {id: 'source'})").unwrap();
    db.query("CREATE (:Entity {id: 'target'})").unwrap();
    let mut deleter = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    deleter
        .query("MATCH (m:Memory {id: 'source'}) DETACH DELETE m")
        .unwrap();

    let mut creator = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    let error = creator
        .query("MATCH (m:Memory {id: 'source'}), (e:Entity {id: 'target'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    deleter.rollback();
}

#[test]
fn failed_graph_statement_restores_workspace_and_statement_locks() {
    let db = Database::new().into_concurrent();
    db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
        .unwrap();
    db.query("CREATE (:Memory {id: 'existing'})").unwrap();
    let mut transaction = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    transaction
        .query("CREATE (:Entity {id: 'retained'})")
        .unwrap();

    let error = transaction
        .query("CREATE (:Memory {id: 'existing'})")
        .unwrap_err();
    assert!(error.to_string().contains("unique constraint violation"));
    assert_eq!(
        transaction
            .query("MATCH (e:Entity {id: 'retained'}) RETURN e.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
    transaction.rollback();
}

#[test]
fn graph_lock_failure_restores_the_failed_statement_only() {
    let db = Database::new().into_concurrent();
    db.query("CREATE (:Memory {id: 'one', state: 'before'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'two', state: 'before'})")
        .unwrap();
    let mut owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    owner
        .query("MATCH (m:Memory) WHERE id(m) = 0 SET m.state = 'owner'")
        .unwrap();

    let mut waiter = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    waiter
        .query("MATCH (m:Memory) WHERE id(m) = 1 SET m.state = 'retained'")
        .unwrap();
    let error = waiter
        .query("MATCH (m:Memory) WHERE id(m) = 0 SET m.state = 'discarded'")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    waiter.commit().unwrap();

    owner.rollback();
    let rows = db
        .query("MATCH (m:Memory) RETURN m.state AS state ORDER BY m.id")
        .unwrap()
        .rows;
    assert_eq!(rows[0].get("state"), Some(&Value::String("before".into())));
    assert_eq!(
        rows[1].get("state"),
        Some(&Value::String("retained".into()))
    );
}

#[test]
fn disjoint_primary_key_point_locks_allow_both_pessimistic_writers_to_commit() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    let mut first = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    let mut second = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();

    first
        .query_sql("INSERT INTO public.messages (id, body) VALUES (1, 'first')")
        .unwrap();
    second
        .query_sql("INSERT INTO public.messages (id, body) VALUES (2, 'second')")
        .unwrap();
    first.commit().unwrap();
    second.commit().unwrap();

    let rows = db
        .query_sql("SELECT id FROM public.messages ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(rows[1].get("id"), Some(&Value::Int(2)));
}

#[test]
fn disjoint_update_upserts_use_point_locks_when_the_conflict_targets_are_absent() {
    let db = Database::new().into_concurrent();
    db.query_sql(
        "CREATE TABLE public.documents (\
            id BIGINT PRIMARY KEY, \
            owner TEXT NOT NULL UNIQUE, \
            body TEXT NOT NULL\
        )",
    )
    .unwrap();
    let mut first = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    let mut second = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();

    first
        .query_sql(
            "INSERT INTO public.documents (id, owner, body) VALUES (1, 'first', 'first') \
             ON CONFLICT (owner) DO UPDATE SET body = EXCLUDED.body",
        )
        .unwrap();
    second
        .query_sql(
            "INSERT INTO public.documents (id, owner, body) VALUES (2, 'second', 'second') \
             ON CONFLICT (owner) DO UPDATE SET body = EXCLUDED.body",
        )
        .unwrap();
    first.commit().unwrap();
    second.commit().unwrap();

    let rows = db
        .query_sql("SELECT id FROM public.documents ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(rows[1].get("id"), Some(&Value::Int(2)));
}

#[test]
fn same_key_update_upserts_remain_bounded_by_the_conflict_target_lock() {
    let db = Database::new().into_concurrent();
    db.query_sql(
        "CREATE TABLE public.documents (\
            id BIGINT PRIMARY KEY, \
            owner TEXT NOT NULL UNIQUE, \
            body TEXT NOT NULL\
        )",
    )
    .unwrap();
    let mut owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    owner
        .query_sql(
            "INSERT INTO public.documents (id, owner, body) VALUES (1, 'shared', 'owner') \
             ON CONFLICT (owner) DO UPDATE SET body = EXCLUDED.body",
        )
        .unwrap();
    let mut waiter = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    let error = waiter
        .query_sql(
            "INSERT INTO public.documents (id, owner, body) VALUES (2, 'shared', 'waiter') \
             ON CONFLICT (owner) DO UPDATE SET body = EXCLUDED.body",
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    owner.rollback();
}

#[test]
fn wal_group_commit_requires_performance_and_recovery_evidence() {
    let bounds = (
        NonZeroUsize::new(8).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::from_millis(1),
    );
    let accepted_evidence = WalGroupCommitEvidence {
        measurement_rounds: 9,
        commit_count: 8,
        baseline_elapsed_micros: 200,
        baseline_fsync_count: 8,
        grouped_elapsed_micros: 100,
        grouped_fsync_count: 1,
        concurrent_tail_latency: WalGroupCommitTailLatencyEvidence {
            commit_count: 8,
            paired_p95_regression_micros: 5,
            paired_p95_mad_micros: 1,
            max_accepted_p95_regression_micros: 10,
        },
        single_writer_tail_latency: WalGroupCommitTailLatencyEvidence {
            commit_count: 16,
            paired_p95_regression_micros: 5,
            paired_p95_mad_micros: 1,
            max_accepted_p95_regression_micros: 10,
        },
        single_writer_max_coalescing_wait_count: 0,
        single_writer_max_observed_group_entries: 1,
        adaptive_cold_start_behavior: Some(WalGroupCommitAdaptiveColdStartEvidence {
            commit_count: 8,
            min_fallback_delay_count: 1,
            min_coalescing_wait_count: 1,
            min_observed_group_entries: 2,
        }),
        adaptive_steady_state_behavior: Some(WalGroupCommitAdaptiveSteadyStateEvidence {
            commit_count: 8,
            max_fallback_delay_count: 0,
            min_fsync_baseline_sample_count: 8,
            min_coalescing_wait_count: 1,
            min_observed_group_entries: 2,
            safety_net: WalGroupCommitAdaptivePolicyEvidence {
                paired_elapsed_regression_micros: -10,
                paired_elapsed_mad_micros: 1,
                max_accepted_elapsed_regression_micros: 10,
                tail_latency: WalGroupCommitTailLatencyEvidence {
                    commit_count: 8,
                    paired_p95_regression_micros: -5,
                    paired_p95_mad_micros: 1,
                    max_accepted_p95_regression_micros: 10,
                },
            },
        }),
        strict_recovery_verified: true,
        wal_order_verified: true,
    };
    let steady_state_safety_net = accepted_evidence
        .adaptive_steady_state_behavior
        .unwrap()
        .safety_net;
    let rejected = WalGroupCommitConfig::enabled_after_evidence(
        WalGroupCommitEvidence {
            baseline_elapsed_micros: 100,
            grouped_elapsed_micros: 100,
            grouped_fsync_count: 8,
            strict_recovery_verified: false,
            wal_order_verified: false,
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(rejected
        .to_string()
        .contains("throughput_improvement_not_proven"));
    assert!(rejected
        .to_string()
        .contains("strict_recovery_not_verified"));

    let low_concurrency_rejected = WalGroupCommitConfig::enabled_after_evidence(
        WalGroupCommitEvidence {
            single_writer_tail_latency: WalGroupCommitTailLatencyEvidence {
                paired_p95_regression_micros: 11,
                ..accepted_evidence.single_writer_tail_latency
            },
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(low_concurrency_rejected
        .to_string()
        .contains("single_writer_tail_latency_budget_exceeded"));

    let single_round_rejected = WalGroupCommitConfig::enabled_after_evidence(
        WalGroupCommitEvidence {
            measurement_rounds: 1,
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(single_round_rejected
        .to_string()
        .contains("insufficient_measurement_rounds"));

    let missing_concurrent_tail = WalGroupCommitConfig::enabled_after_evidence(
        WalGroupCommitEvidence {
            concurrent_tail_latency: WalGroupCommitTailLatencyEvidence {
                commit_count: 0,
                ..accepted_evidence.concurrent_tail_latency
            },
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(missing_concurrent_tail
        .to_string()
        .contains("concurrent_tail_latency_evidence_missing"));

    let structural_rejected = WalGroupCommitConfig::enabled_after_evidence(
        WalGroupCommitEvidence {
            single_writer_max_coalescing_wait_count: 1,
            single_writer_max_observed_group_entries: 2,
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(structural_rejected
        .to_string()
        .contains("single_writer_coalescing_wait_observed"));
    assert!(structural_rejected
        .to_string()
        .contains("single_writer_grouping_observed"));

    let admitted = WalGroupCommitConfig::enabled_after_evidence(
        accepted_evidence,
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap();
    assert_eq!(
        admitted.activation(),
        WalGroupCommitActivation::EvidenceValidated
    );
    let adaptive = WalGroupCommitConfig::adaptive_enabled_after_evidence(
        accepted_evidence,
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap();
    assert_eq!(
        adaptive.delay_policy(),
        WalGroupCommitDelayPolicy::AdaptiveFsync
    );

    let missing_policy_comparison = WalGroupCommitConfig::adaptive_enabled_after_evidence(
        WalGroupCommitEvidence {
            adaptive_cold_start_behavior: None,
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(missing_policy_comparison
        .to_string()
        .contains("cold_start_behavior_missing"));

    let missing_steady_state_comparison = WalGroupCommitConfig::adaptive_enabled_after_evidence(
        WalGroupCommitEvidence {
            adaptive_steady_state_behavior: None,
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(missing_steady_state_comparison
        .to_string()
        .contains("steady_state_behavior_missing"));

    let empty_policy_comparison = WalGroupCommitConfig::adaptive_enabled_after_evidence(
        WalGroupCommitEvidence {
            adaptive_steady_state_behavior: Some(WalGroupCommitAdaptiveSteadyStateEvidence {
                safety_net: WalGroupCommitAdaptivePolicyEvidence {
                    tail_latency: WalGroupCommitTailLatencyEvidence {
                        commit_count: 0,
                        ..steady_state_safety_net.tail_latency
                    },
                    ..steady_state_safety_net
                },
                ..accepted_evidence.adaptive_steady_state_behavior.unwrap()
            }),
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(empty_policy_comparison
        .to_string()
        .contains("steady_state_adaptive_policy_evidence_missing"));

    let policy_regression = WalGroupCommitConfig::adaptive_enabled_after_evidence(
        WalGroupCommitEvidence {
            adaptive_steady_state_behavior: Some(WalGroupCommitAdaptiveSteadyStateEvidence {
                safety_net: WalGroupCommitAdaptivePolicyEvidence {
                    paired_elapsed_regression_micros: 11,
                    paired_elapsed_mad_micros: 1,
                    max_accepted_elapsed_regression_micros: 10,
                    tail_latency: WalGroupCommitTailLatencyEvidence {
                        paired_p95_regression_micros: 11,
                        ..steady_state_safety_net.tail_latency
                    },
                },
                ..accepted_evidence.adaptive_steady_state_behavior.unwrap()
            }),
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(policy_regression
        .to_string()
        .contains("steady_state_adaptive_policy_elapsed_budget_exceeded"));
    assert!(policy_regression
        .to_string()
        .contains("steady_state_adaptive_policy_tail_latency_budget_exceeded"));

    let noisy_tail_latency = WalGroupCommitConfig::enabled_after_evidence(
        WalGroupCommitEvidence {
            concurrent_tail_latency: WalGroupCommitTailLatencyEvidence {
                paired_p95_regression_micros: 8,
                paired_p95_mad_micros: 21,
                max_accepted_p95_regression_micros: 10,
                ..accepted_evidence.concurrent_tail_latency
            },
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(noisy_tail_latency
        .to_string()
        .contains("tail_latency_insufficient_signal_quality"));

    let noisy_adaptive_elapsed = WalGroupCommitConfig::adaptive_enabled_after_evidence(
        WalGroupCommitEvidence {
            adaptive_steady_state_behavior: Some(WalGroupCommitAdaptiveSteadyStateEvidence {
                safety_net: WalGroupCommitAdaptivePolicyEvidence {
                    paired_elapsed_regression_micros: 8,
                    paired_elapsed_mad_micros: 21,
                    ..steady_state_safety_net
                },
                ..accepted_evidence.adaptive_steady_state_behavior.unwrap()
            }),
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(noisy_adaptive_elapsed
        .to_string()
        .contains("steady_state_adaptive_policy_elapsed_insufficient_signal_quality"));

    let strong_but_variable_improvement = WalGroupCommitConfig::adaptive_enabled_after_evidence(
        WalGroupCommitEvidence {
            adaptive_steady_state_behavior: Some(WalGroupCommitAdaptiveSteadyStateEvidence {
                safety_net: WalGroupCommitAdaptivePolicyEvidence {
                    paired_elapsed_regression_micros: -27_000,
                    paired_elapsed_mad_micros: 39_374,
                    max_accepted_elapsed_regression_micros: 1_790,
                    tail_latency: WalGroupCommitTailLatencyEvidence {
                        paired_p95_regression_micros: -27_000,
                        paired_p95_mad_micros: 39_374,
                        max_accepted_p95_regression_micros: 1_790,
                        ..steady_state_safety_net.tail_latency
                    },
                },
                ..accepted_evidence.adaptive_steady_state_behavior.unwrap()
            }),
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap();
    assert_eq!(
        strong_but_variable_improvement.delay_policy(),
        WalGroupCommitDelayPolicy::AdaptiveFsync
    );

    // Cold start is gated on behavior. Losing the fallback means the adaptive
    // policy stops coalescing until an fsync baseline exists, which is exactly
    // the regression the fallback was added to prevent.
    for (behavior, blocker) in [
        (
            WalGroupCommitAdaptiveColdStartEvidence {
                min_fallback_delay_count: 0,
                ..accepted_evidence.adaptive_cold_start_behavior.unwrap()
            },
            "cold_start_fallback_not_exercised",
        ),
        (
            WalGroupCommitAdaptiveColdStartEvidence {
                min_coalescing_wait_count: 0,
                ..accepted_evidence.adaptive_cold_start_behavior.unwrap()
            },
            "cold_start_coalescing_disabled",
        ),
        (
            WalGroupCommitAdaptiveColdStartEvidence {
                min_observed_group_entries: 1,
                ..accepted_evidence.adaptive_cold_start_behavior.unwrap()
            },
            "cold_start_grouping_not_observed",
        ),
    ] {
        let rejected = WalGroupCommitConfig::adaptive_enabled_after_evidence(
            WalGroupCommitEvidence {
                adaptive_cold_start_behavior: Some(behavior),
                ..accepted_evidence
            },
            bounds.0,
            bounds.1,
            bounds.2,
        )
        .unwrap_err();
        assert!(rejected.to_string().contains(blocker), "{rejected}");
    }

    // Cold-start admission must not depend on paired timing. The elapsed and
    // p95 spreads observed on real hardware are variance between two arms that
    // are meant to behave identically, so no timing value can block it.
    let unstable_cold_start_timing = WalGroupCommitConfig::adaptive_enabled_after_evidence(
        WalGroupCommitEvidence {
            adaptive_cold_start_behavior: Some(WalGroupCommitAdaptiveColdStartEvidence {
                commit_count: 256,
                min_fallback_delay_count: 3,
                min_coalescing_wait_count: 12,
                min_observed_group_entries: 4,
            }),
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap();
    assert_eq!(
        unstable_cold_start_timing.delay_policy(),
        WalGroupCommitDelayPolicy::AdaptiveFsync
    );

    // A warm window must prove it is the derived path being exercised: a
    // baseline exists, no decision falls back to the fixed delay, and
    // coalescing still groups.
    for (behavior, blocker) in [
        (
            WalGroupCommitAdaptiveSteadyStateEvidence {
                min_fsync_baseline_sample_count: 0,
                ..accepted_evidence.adaptive_steady_state_behavior.unwrap()
            },
            "steady_state_baseline_not_established",
        ),
        (
            WalGroupCommitAdaptiveSteadyStateEvidence {
                max_fallback_delay_count: 1,
                ..accepted_evidence.adaptive_steady_state_behavior.unwrap()
            },
            "steady_state_fell_back_to_fixed_delay",
        ),
        (
            WalGroupCommitAdaptiveSteadyStateEvidence {
                min_coalescing_wait_count: 0,
                ..accepted_evidence.adaptive_steady_state_behavior.unwrap()
            },
            "steady_state_coalescing_disabled",
        ),
        (
            WalGroupCommitAdaptiveSteadyStateEvidence {
                min_observed_group_entries: 1,
                ..accepted_evidence.adaptive_steady_state_behavior.unwrap()
            },
            "steady_state_grouping_not_observed",
        ),
    ] {
        let rejected = WalGroupCommitConfig::adaptive_enabled_after_evidence(
            WalGroupCommitEvidence {
                adaptive_steady_state_behavior: Some(behavior),
                ..accepted_evidence
            },
            bounds.0,
            bounds.1,
            bounds.2,
        )
        .unwrap_err();
        assert!(rejected.to_string().contains(blocker), "{rejected}");
    }

    let too_few_rounds = WalGroupCommitConfig::enabled_after_evidence(
        WalGroupCommitEvidence {
            measurement_rounds: 5,
            ..accepted_evidence
        },
        bounds.0,
        bounds.1,
        bounds.2,
    )
    .unwrap_err();
    assert!(too_few_rounds
        .to_string()
        .contains("insufficient_measurement_rounds"));
}

#[test]
fn wal_group_commit_skips_the_coalescing_window_without_contention() {
    let group_commit = WalGroupCommitConfig::benchmark_adaptive_candidate(
        NonZeroUsize::new(16).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::from_millis(10),
    )
    .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(Database::new(), group_commit);
    let mut transaction = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    transaction.query("CREATE (:Memory {id: 1})").unwrap();
    transaction.commit().unwrap();

    let snapshot = db.wal_group_commit_snapshot().unwrap();
    assert_eq!(snapshot.submitted_commits, 1);
    assert_eq!(snapshot.completed_commits, 1);
    assert_eq!(snapshot.coalescing_wait_count, 0);
    assert_eq!(
        snapshot.delay_policy,
        WalGroupCommitDelayPolicy::AdaptiveFsync
    );
    assert_eq!(
        snapshot.last_wait_decision,
        WalGroupCommitWaitDecision::SingleRequest
    );
    assert_eq!(snapshot.effective_delay_micros, 0);
}

#[test]
fn wal_group_commit_shares_one_sync_without_changing_record_order() {
    const WRITERS: usize = 8;
    let path = super::unique_test_dir("wal_group_commit");
    let mut database = Database::open(&path).unwrap();
    database
        .query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    let group_commit = WalGroupCommitConfig::benchmark_candidate(
        NonZeroUsize::new(WRITERS).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::from_millis(5),
    )
    .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(database, group_commit);
    db.set_group_commit_post_enqueue_barrier(Arc::new(Barrier::new(WRITERS)))
        .unwrap();
    let writers = (0..WRITERS)
        .map(|id| {
            let db = db.clone();
            std::thread::spawn(move || {
                let mut transaction = db
                    .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                        Duration::from_secs(1),
                    ))
                    .unwrap();
                transaction
                    .query_sql(&format!(
                        "INSERT INTO public.messages (id, body) VALUES ({id}, 'writer-{id}')"
                    ))
                    .unwrap();
                transaction.commit().unwrap();
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }

    let snapshot = db.wal_group_commit_snapshot().unwrap();
    assert_eq!(
        snapshot.activation,
        WalGroupCommitActivation::BenchmarkCandidate
    );
    assert_eq!(snapshot.submitted_commits, WRITERS as u64);
    assert_eq!(snapshot.completed_commits, WRITERS as u64);
    assert_eq!(snapshot.shared_sync_count, 1);
    assert_eq!(snapshot.grouped_wal_entries, WRITERS as u64);
    assert_eq!(snapshot.max_observed_group_entries, WRITERS);
    assert_eq!(db.commit_epoch().unwrap(), WRITERS as u64 + 2);
    drop(db);

    let wal = super::read_test_wal(&path).unwrap();
    let lsns = wal
        .lines()
        .map(|line| line.split('\t').next().unwrap().parse::<u64>().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lsns, (1..=WRITERS as u64 + 2).collect::<Vec<_>>());

    let mut reopened = Database::open(&path).unwrap();
    let rows = reopened
        .query_sql("SELECT id FROM public.messages ORDER BY id")
        .unwrap();
    assert_eq!(rows.rows.len(), WRITERS);
    assert_eq!(reopened.commit_epoch(), WRITERS as u64 + 2);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_group_commit_assigns_generated_order_in_serial_commit_order() {
    const WRITERS: usize = 4;
    let path = super::unique_test_dir("generated_order_group_commit");
    let mut database = Database::open(&path).unwrap();
    database
        .query_sql(
            "CREATE TABLE events (\
               stream_id TEXT NOT NULL, \
               sequence BIGINT NOT NULL, \
               payload TEXT NOT NULL\
             ) WITH (\
               storage_mode = 'strict_append', \
               partition_key = 'stream_id', \
               order_key = 'sequence', \
               generated_order = 'commit_sequence'\
             )",
        )
        .unwrap();
    let group_commit = WalGroupCommitConfig::benchmark_candidate(
        NonZeroUsize::new(WRITERS).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::from_millis(5),
    )
    .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(database, group_commit);
    db.set_group_commit_post_enqueue_barrier(Arc::new(Barrier::new(WRITERS)))
        .unwrap();

    let writers = (0..WRITERS)
        .map(|writer| {
            let db = db.clone();
            std::thread::spawn(move || {
                db.append_transaction_with_result(AppendTransaction {
                    writes: vec![AppendWrite::AppendGenerated {
                        table: "events".to_string(),
                        rows: vec![AppendGeneratedRow::new(vec![
                            RelationalValue::Text("thread-1".to_string()),
                            RelationalValue::Text(format!("writer-{writer}")),
                        ])],
                    }],
                })
                .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let mut committed = writers
        .into_iter()
        .map(|writer| writer.join().unwrap())
        .map(|result| {
            let sequence = match &result.mutations[0].generated_order_keys[0].0[0] {
                RelationalValue::BigInt(value) => *value,
                value => panic!("unexpected generated key {value:?}"),
            };
            (result.commit_epoch, sequence)
        })
        .collect::<Vec<_>>();
    committed.sort_unstable_by_key(|(commit_epoch, _)| *commit_epoch);
    assert!(committed.windows(2).all(|pair| pair[0].0 < pair[1].0));
    assert_eq!(
        committed
            .iter()
            .map(|(_, sequence)| *sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );

    let snapshot = db.wal_group_commit_snapshot().unwrap();
    assert_eq!(snapshot.shared_sync_count, 1);
    assert_eq!(snapshot.grouped_wal_entries, WRITERS as u64);
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    let rows = reopened
        .query_sql(
            "SELECT sequence FROM events WHERE stream_id = 'thread-1' \
             ORDER BY sequence LIMIT 10",
        )
        .unwrap();
    assert_eq!(
        rows.rows
            .iter()
            .map(|row| row["sequence"].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)]
    );
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn generated_order_retries_after_optimistic_conflict_and_matches_pessimistic_commit() {
    let mut database = Database::new();
    database
        .query_sql(
            "CREATE TABLE events (\
               stream_id TEXT NOT NULL, \
               sequence BIGINT NOT NULL, \
               payload TEXT NOT NULL\
             ) WITH (\
               storage_mode = 'strict_append', \
               partition_key = 'stream_id', \
               order_key = 'sequence', \
               generated_order = 'commit_sequence'\
             )",
        )
        .unwrap();
    let db = database.into_concurrent();

    let mut first = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    let mut stale = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    first
        .query_sql(
            "INSERT INTO events (stream_id, payload) VALUES ('thread-1', 'optimistic-first')",
        )
        .unwrap();
    stale
        .query_sql(
            "INSERT INTO events (stream_id, payload) VALUES ('thread-2', 'optimistic-stale')",
        )
        .unwrap();

    let first = first.commit_with_result().unwrap();
    assert_eq!(
        first.append_mutations[0].generated_order_keys,
        vec![crate::RelationalKey(vec![RelationalValue::BigInt(1)])]
    );
    assert!(stale
        .commit_with_result()
        .unwrap_err()
        .to_string()
        .contains("optimistic transaction conflict"));

    let mut retry = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    retry
        .query_sql(
            "INSERT INTO events (stream_id, payload) VALUES ('thread-2', 'optimistic-retry')",
        )
        .unwrap();
    let retry = retry.commit_with_result().unwrap();
    assert_eq!(
        retry.append_mutations[0].generated_order_keys,
        vec![crate::RelationalKey(vec![RelationalValue::BigInt(2)])]
    );

    let mut pessimistic = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    pessimistic
        .query_sql("INSERT INTO events (stream_id, payload) VALUES ('thread-1', 'pessimistic')")
        .unwrap();
    let pessimistic = pessimistic.commit_with_result().unwrap();
    assert_eq!(
        pessimistic.append_mutations[0].generated_order_keys,
        vec![crate::RelationalKey(vec![RelationalValue::BigInt(3)])]
    );

    let rows = db
        .query_sql(
            "SELECT sequence FROM events WHERE stream_id = 'thread-1' \
             ORDER BY sequence LIMIT 10",
        )
        .unwrap();
    assert_eq!(
        rows.rows
            .iter()
            .map(|row| row["sequence"].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(3)]
    );
}

#[test]
fn wal_group_commit_publishes_schema_row_checkpoint_after_group_sync() {
    let path = super::unique_test_dir("wal_group_schema_checkpoint");
    let mut database = Database::open(&path).unwrap();
    database
        .query_sql("CREATE TABLE public.documents (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    database
        .query_sql("INSERT INTO public.documents (id, body) VALUES (1, 'body')")
        .unwrap();
    database.checkpoint().unwrap();
    let group_commit = WalGroupCommitConfig::benchmark_candidate(
        NonZeroUsize::new(1).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::ZERO,
    )
    .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(database, group_commit);
    let mut transaction = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    transaction
        .query_sql("ALTER TABLE public.documents ADD COLUMN kind TEXT NOT NULL DEFAULT 'text'")
        .unwrap();

    transaction.commit().unwrap();

    let published = db.published_read_view().unwrap();
    assert!(published.physical_base_is_current());
    assert_eq!(
        db.query_sql("SELECT kind FROM public.documents WHERE id = 1")
            .unwrap()
            .rows[0]["kind"],
        Value::String("text".to_string())
    );
    let group = db.wal_group_commit_snapshot().unwrap();
    assert_eq!(group.completed_commits, 1);
    assert_eq!(group.shared_sync_count, 1);
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .query_sql("SELECT kind FROM public.documents WHERE id = 1")
            .unwrap()
            .rows[0]["kind"],
        Value::String("text".to_string())
    );
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_group_sync_failure_rejects_commit_and_poisons_until_reopen() {
    let path = super::unique_test_dir("wal_group_sync_failure");
    let mut database = Database::open(&path).unwrap();
    database
        .query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY)")
        .unwrap();
    let group_commit = WalGroupCommitConfig::benchmark_candidate(
        NonZeroUsize::new(1).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::ZERO,
    )
    .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(database, group_commit);
    let mut transaction = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    transaction
        .query_sql("INSERT INTO public.messages (id) VALUES (1)")
        .unwrap();

    crate::store::set_wal_group_sync_failpoint(true);
    let error = transaction.commit().unwrap_err();
    assert!(matches!(&error, HawDBError::StorageIntegrity(_)));
    assert!(error
        .to_string()
        .contains("WAL group durability barrier failed"));
    assert!(db
        .query_sql("SELECT id FROM public.messages")
        .unwrap_err()
        .to_string()
        .contains("close and reopen"));
    let snapshot = db.wal_group_commit_snapshot().unwrap();
    assert_eq!(snapshot.completed_commits, 0);
    assert_eq!(snapshot.shared_sync_count, 0);
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    let rows = reopened
        .query_sql("SELECT id FROM public.messages")
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn generated_order_recovers_after_an_uncertain_group_sync() {
    let path = super::unique_test_dir("generated_order_group_sync_failure");
    let mut database = Database::open(&path).unwrap();
    database
        .query_sql(
            "CREATE TABLE events (\
               stream_id TEXT NOT NULL, \
               sequence BIGINT NOT NULL, \
               payload TEXT NOT NULL\
             ) WITH (\
               storage_mode = 'strict_append', \
               partition_key = 'stream_id', \
               order_key = 'sequence', \
               generated_order = 'commit_sequence'\
             )",
        )
        .unwrap();
    let group_commit = WalGroupCommitConfig::benchmark_candidate(
        NonZeroUsize::new(1).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::ZERO,
    )
    .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(database, group_commit);

    crate::store::set_wal_group_sync_failpoint(true);
    let error = db
        .append_transaction_with_result(AppendTransaction {
            writes: vec![AppendWrite::AppendGenerated {
                table: "events".to_string(),
                rows: vec![AppendGeneratedRow::new(vec![
                    RelationalValue::Text("thread-1".to_string()),
                    RelationalValue::Text("uncertain".to_string()),
                ])],
            }],
        })
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("WAL group durability barrier failed"));
    assert!(db
        .query_sql(
            "SELECT sequence FROM events WHERE stream_id = 'thread-1' \
             ORDER BY sequence LIMIT 10",
        )
        .unwrap_err()
        .to_string()
        .contains("close and reopen"));
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    let next = reopened
        .append_transaction_with_result(AppendTransaction {
            writes: vec![AppendWrite::AppendGenerated {
                table: "events".to_string(),
                rows: vec![AppendGeneratedRow::new(vec![
                    RelationalValue::Text("thread-1".to_string()),
                    RelationalValue::Text("acknowledged".to_string()),
                ])],
            }],
        })
        .unwrap();
    assert_eq!(
        next.mutations[0].generated_order_keys,
        vec![crate::RelationalKey(vec![RelationalValue::BigInt(2)])]
    );
    let rows = reopened
        .query_sql(
            "SELECT sequence FROM events WHERE stream_id = 'thread-1' \
             ORDER BY sequence LIMIT 10",
        )
        .unwrap();
    assert_eq!(
        rows.rows
            .iter()
            .map(|row| row["sequence"].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(2)]
    );
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn conflicting_primary_key_point_lock_times_out_and_aborts_the_waiter() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY)")
        .unwrap();
    let mut owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    owner
        .query_sql("INSERT INTO public.messages (id) VALUES (1)")
        .unwrap();
    let mut waiter = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();

    let error = waiter
        .query_sql("INSERT INTO public.messages (id) VALUES (1)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    assert!(waiter.commit().unwrap_err().to_string().contains("aborted"));
    owner.commit().unwrap();
}

#[test]
fn shared_primary_key_range_blocks_phantoms_but_not_the_excluded_boundary() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY)")
        .unwrap();
    let mut reader = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    assert!(reader
        .query_sql("SELECT id FROM public.messages WHERE id >= 10 AND id < 20 FOR SHARE")
        .unwrap()
        .rows
        .is_empty());

    let mut outside = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    outside
        .query_sql("INSERT INTO public.messages (id) VALUES (20)")
        .unwrap();
    outside.commit().unwrap();

    let mut inside = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    let error = inside
        .query_sql("INSERT INTO public.messages (id) VALUES (15)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    reader.commit().unwrap();

    assert_eq!(
        db.query_sql("SELECT id FROM public.messages ORDER BY id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn ordinary_snapshot_select_does_not_block_an_exact_update() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    db.query_sql("INSERT INTO public.messages (id, body) VALUES (1, 'before')")
        .unwrap();
    let mut reader = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    assert_eq!(
        reader
            .query_sql("SELECT body FROM public.messages WHERE id = 1")
            .unwrap()
            .rows
            .len(),
        1
    );

    let mut writer = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    writer
        .query_sql("UPDATE public.messages SET body = 'after' WHERE id = 1")
        .unwrap();
    writer.commit().unwrap();
    reader.rollback();

    assert_eq!(
        db.query_sql("SELECT body FROM public.messages WHERE id = 1")
            .unwrap()
            .rows[0]
            .get("body"),
        Some(&Value::String("after".to_string()))
    );
}

#[test]
fn for_update_point_lock_blocks_exact_update_until_owner_finishes() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    db.query_sql("INSERT INTO public.messages (id, body) VALUES (1, 'before')")
        .unwrap();
    let mut owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    owner
        .query_sql("SELECT id FROM public.messages WHERE id = 1 FOR UPDATE")
        .unwrap();

    let mut waiter = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    let error = waiter
        .query_sql("UPDATE public.messages SET body = 'after' WHERE id = 1")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    owner.rollback();
}

#[test]
fn bigint_arithmetic_update_uses_the_existing_point_lock_contract() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.counters (id BIGINT PRIMARY KEY, count BIGINT NOT NULL)")
        .unwrap();
    db.query_sql("INSERT INTO public.counters (id, count) VALUES (1, 0)")
        .unwrap();

    let mut owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    owner
        .query_sql("UPDATE public.counters SET count = count + 1 WHERE id = 1")
        .unwrap();

    let mut waiter = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    let error = waiter
        .query_sql("UPDATE public.counters SET count = count + 1 WHERE id = 1")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    owner.commit().unwrap();

    let mut retry = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    retry
        .query_sql("UPDATE public.counters SET count = count + 1 WHERE id = 1")
        .unwrap();
    retry.commit().unwrap();
    assert_eq!(
        db.query_sql("SELECT count FROM public.counters WHERE id = 1")
            .unwrap()
            .rows[0]["count"],
        Value::Int(2)
    );
}

#[test]
fn delete_cascade_blocks_a_concurrent_child_insert_before_publication() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.parents (id BIGINT PRIMARY KEY)")
        .unwrap();
    db.query_sql(
        "CREATE TABLE public.children (\
            id BIGINT PRIMARY KEY, \
            parent_id BIGINT NOT NULL REFERENCES public.parents(id) ON DELETE CASCADE\
        )",
    )
    .unwrap();
    db.query_sql("INSERT INTO public.parents (id) VALUES (1)")
        .unwrap();

    let mut deleting = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    deleting
        .query_sql("DELETE FROM public.parents WHERE id = 1")
        .unwrap();

    let mut inserting = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    let error = inserting
        .query_sql("INSERT INTO public.children (id, parent_id) VALUES (10, 1)")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));

    deleting.commit().unwrap();
    assert!(db
        .query_sql("SELECT id FROM public.parents")
        .unwrap()
        .rows
        .is_empty());
    assert!(db
        .query_sql("SELECT id FROM public.children")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn uuidv7_defaults_preserve_staged_ids_across_concurrent_commit_modes() {
    let db = Database::new().into_concurrent();
    db.query_sql(
        "CREATE TABLE public.uuidv7_defaults (\
            id UUID PRIMARY KEY DEFAULT uuidv7(), \
            url TEXT NOT NULL UNIQUE\
        )",
    )
    .unwrap();

    for (url, options) in [
        (
            "https://optimistic.example",
            ConcurrentTransactionOptions::optimistic(),
        ),
        (
            "https://pessimistic.example",
            ConcurrentTransactionOptions::pessimistic(Duration::from_secs(1)),
        ),
    ] {
        let mut transaction = db.begin_transaction(options).unwrap();
        let staged = transaction
            .query_sql_with_result(&format!(
                "INSERT INTO public.uuidv7_defaults (url) VALUES ('{url}') RETURNING id"
            ))
            .unwrap();
        let staged_id = staged.mutation.unwrap().rows[0]["id"].clone();
        let Value::Uuid(uuid) = &staged_id else {
            panic!("uuidv7 default must stage a typed UUID");
        };
        assert_eq!(uuid.as_bytes()[6] >> 4, 7, "uuidv7 version bits");
        assert_eq!(uuid.as_bytes()[8] & 0b1100_0000, 0b1000_0000);

        let committed = transaction.commit_with_result().unwrap();
        assert_eq!(committed.output.rows[0]["id"], staged_id);
        assert_eq!(committed.mutations[0].rows[0]["id"], staged_id);
    }
}

#[test]
fn optimistic_transaction_rejects_locking_selects() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY)")
        .unwrap();
    let mut transaction = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();

    let error = transaction
        .query_sql("SELECT id FROM public.messages WHERE id = 1 FOR UPDATE")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("requires a pessimistic concurrent transaction"));
    transaction.rollback();
}

#[test]
fn database_without_lock_manager_rejects_locking_selects() {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY)")
        .unwrap();

    let error = database
        .query_sql("SELECT id FROM public.messages WHERE id = 1 FOR SHARE")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("requires a pessimistic concurrent transaction"));

    let database = database.into_concurrent();
    let error = database
        .query_sql("SELECT id FROM public.messages WHERE id = 1 FOR SHARE")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("requires a pessimistic concurrent transaction"));
}

#[test]
fn non_primary_key_mutation_falls_back_to_a_table_lock() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    let mut owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    owner
        .query_sql("UPDATE public.messages SET body = 'after' WHERE body = 'before'")
        .unwrap();

    let mut waiter = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    let error = waiter
        .query_sql("INSERT INTO public.messages (id, body) VALUES (1, 'new')")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    owner.rollback();
}

#[test]
fn exact_parent_delete_conflicts_with_foreign_key_validation_on_a_unique_key() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.parents (id BIGINT PRIMARY KEY, code TEXT UNIQUE NOT NULL)")
        .unwrap();
    db.query_sql(
        "CREATE TABLE public.children (id BIGINT PRIMARY KEY, parent_code TEXT REFERENCES public.parents(code))",
    )
    .unwrap();
    db.query_sql("INSERT INTO public.parents (id, code) VALUES (1, 'parent-1')")
        .unwrap();
    let mut owner = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    owner
        .query_sql("DELETE FROM public.parents WHERE id = 1")
        .unwrap();

    let mut waiter = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    let error = waiter
        .query_sql("INSERT INTO public.children (id, parent_code) VALUES (1, 'parent-1')")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("transaction lock wait timed out"));
    owner.rollback();
}

#[test]
fn repeated_covered_point_read_keeps_its_snapshot_after_a_disjoint_commit() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY)")
        .unwrap();
    db.query_sql("INSERT INTO public.messages (id) VALUES (1)")
        .unwrap();
    let mut reader = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    assert_eq!(
        reader
            .query_sql("SELECT id FROM public.messages WHERE id = 1 FOR SHARE")
            .unwrap()
            .rows
            .len(),
        1
    );

    let mut writer = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    writer
        .query_sql("INSERT INTO public.messages (id) VALUES (2)")
        .unwrap();
    writer.commit().unwrap();

    assert_eq!(
        reader
            .query_sql("SELECT id FROM public.messages WHERE id = 1 FOR SHARE")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(reader
        .query_sql("SELECT id FROM public.messages WHERE id = 2 FOR SHARE")
        .unwrap_err()
        .to_string()
        .contains("cannot acquire a new lock after its snapshot changed"));
}

#[test]
fn point_lock_upgrade_cycle_selects_one_deadlock_victim() {
    let db = Database::new().into_concurrent();
    db.query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY)")
        .unwrap();
    let mut first = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    let mut second = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    first
        .query_sql("SELECT id FROM public.messages WHERE id = 1 FOR SHARE")
        .unwrap();
    second
        .query_sql("SELECT id FROM public.messages WHERE id = 2 FOR SHARE")
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let first_barrier = Arc::clone(&barrier);
    let first_handle = std::thread::spawn(move || {
        first_barrier.wait();
        let result = first.query_sql("INSERT INTO public.messages (id) VALUES (2)");
        first.rollback();
        result
    });
    let second_handle = std::thread::spawn(move || {
        barrier.wait();
        let result = second.query_sql("INSERT INTO public.messages (id) VALUES (1)");
        second.rollback();
        result
    });
    let results = [first_handle.join().unwrap(), second_handle.join().unwrap()];

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let deadlock = results
        .iter()
        .find_map(|result| result.as_ref().err())
        .expect("one lock upgrader must be selected as the deadlock victim");
    assert!(deadlock.to_string().contains("deadlock detected"));
}

#[test]
fn pessimistic_transaction_keeps_uncommitted_data_invisible_to_snapshot_readers() {
    let db = Database::new().into_concurrent();
    let mut tx = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_secs(1),
        ))
        .unwrap();
    tx.query("CREATE (:Memory {id: 1, title: 'pending'})")
        .unwrap();

    let mut before_commit = db.begin_read_transaction().unwrap();
    assert!(before_commit
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());
    tx.commit().unwrap();

    assert!(before_commit
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());
    let mut after_commit = db.begin_read_transaction().unwrap();
    assert_eq!(
        after_commit
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn dropped_pessimistic_transaction_releases_the_database_lock() {
    let db = Database::new().into_concurrent();
    {
        let mut tx = db
            .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                Duration::from_secs(1),
            ))
            .unwrap();
        tx.query("CREATE (:Memory {id: 1})").unwrap();
    }

    let next = db
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(
            Duration::from_millis(25),
        ))
        .unwrap();
    next.rollback();
    assert!(db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn concurrent_transaction_publishes_graph_relational_and_append_writes_in_one_wal_epoch() {
    let path = super::unique_test_dir("concurrent_mixed_transaction");
    {
        let db = crate::ConcurrentDatabase::open(&path).unwrap();
        let mut tx = db
            .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                Duration::from_secs(1),
            ))
            .unwrap();
        tx.query("CREATE (:Marker {id: 'graph-1'})").unwrap();
        tx.query_sql("CREATE TABLE public.messages (id TEXT PRIMARY KEY)")
            .unwrap();
        tx.query_sql_with_params(
            "INSERT INTO public.messages (id) VALUES ($1)",
            &[Value::String("message-1".to_string())],
        )
        .unwrap();
        tx.query_sql(
            "CREATE TABLE public.events (\
               stream_id TEXT NOT NULL, \
               sequence BIGINT NOT NULL, \
               payload TEXT NOT NULL\
             ) WITH (\
               storage_mode = 'strict_append', \
               partition_key = 'stream_id', \
               order_key = 'sequence'\
             )",
        )
        .unwrap();
        tx.query_sql_with_params(
            "INSERT INTO public.events (stream_id, sequence, payload) VALUES ($1, $2, $3)",
            &[
                Value::String("thread-1".to_string()),
                Value::Int(1),
                Value::String("event-1".to_string()),
            ],
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(db.commit_epoch().unwrap(), 2);
    }

    let wal = super::read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 2);
    assert!(wal.contains("\tbatch\t"));
    {
        let db = crate::ConcurrentDatabase::open(&path).unwrap();
        assert_eq!(
            db.query("MATCH (m:Marker) RETURN m.id AS id")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert_eq!(
            db.query_sql("SELECT id FROM public.messages")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert_eq!(
            db.query_sql_with_params(
                "SELECT payload FROM public.events \
                 WHERE stream_id = $1 ORDER BY sequence ASC LIMIT 10",
                &[Value::String("thread-1".to_string())],
            )
            .unwrap()
            .rows
            .len(),
            1
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}
