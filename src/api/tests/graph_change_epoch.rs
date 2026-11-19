// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! The graph change commit epoch (#1840): the commit epoch of the newest
//! commit that changed graph data or graph schema. Relational-only commits do
//! not advance it; node, relationship and schema commits do. It survives
//! checkpoints and reopen, never regresses when the change log is pruned, and
//! a transaction can read it and commit only if it is still current.

use super::*;
use crate::{ConcurrentDatabase, ConcurrentTransactionOptions};

fn epochs(db: &Database) -> (u64, u64) {
    (
        db.commit_epoch().unwrap(),
        db.graph_change_commit_epoch().unwrap(),
    )
}

fn relational_write(db: &mut Database, id: i64) {
    db.query_sql_with_params(
        "INSERT INTO control (id, note) VALUES ($1, 'host')",
        &[Value::Int(id)],
    )
    .unwrap();
}

/// Creates the relational table used by the tests and returns the epochs
/// after a first graph write.
fn seeded(db: &mut Database) -> (u64, u64) {
    db.query_sql("CREATE TABLE control (id BIGINT PRIMARY KEY, note TEXT NOT NULL)")
        .unwrap();
    db.query("CREATE (:Memory {id: 'a'})").unwrap();
    db.query("CREATE (:Memory {id: 'b'})").unwrap();
    epochs(db)
}

#[test]
fn relational_commits_leave_it_and_graph_commits_advance_it() {
    let mut db = Database::new();
    assert_eq!(epochs(&db), (0, 0));
    // Relational DDL is not a graph change.
    db.query_sql("CREATE TABLE control (id BIGINT PRIMARY KEY, note TEXT NOT NULL)")
        .unwrap();
    assert_eq!(epochs(&db), (1, 0));

    // A node write advances it to its own commit epoch.
    db.query("CREATE (:Memory {id: 'a'})").unwrap();
    db.query("CREATE (:Memory {id: 'b'})").unwrap();
    let (commit, graph) = epochs(&db);
    assert_eq!(graph, commit);

    // Relational-only commits advance the commit epoch only.
    for id in 0..3 {
        relational_write(&mut db, id);
    }
    assert_eq!(epochs(&db), (commit + 3, graph));

    // So does a relational write inside an explicit transaction.
    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query_sql("INSERT INTO control (id, note) VALUES (10, 'tx')")
        .unwrap();
    transaction.commit().unwrap();
    assert_eq!(epochs(&db), (commit + 4, graph));

    // Each of these changes the graph and advances it to its commit epoch.
    for statement in [
        // relationship-only write
        "MATCH (a:Memory {id: 'a'}), (b:Memory {id: 'b'}) CREATE (a)-[:LINKS]->(b)",
        // relationship property
        "MATCH (:Memory)-[r:LINKS]->(:Memory) SET r.weight = 2",
        // node property
        "MATCH (m:Memory {id: 'a'}) SET m.title = 'A'",
        // graph schema: node table and index DDL
        "CREATE NODE TABLE Topic",
        "CREATE INDEX ON :Memory(title)",
        // relationship delete
        "MATCH (:Memory)-[r:LINKS]->(:Memory) DELETE r",
        // node delete
        "MATCH (m:Memory {id: 'b'}) DELETE m",
    ] {
        let before = epochs(&db);
        db.query(statement).unwrap();
        let (commit, graph) = epochs(&db);
        assert!(commit > before.0, "{statement}");
        assert_eq!(graph, commit, "{statement}");
        relational_write(&mut db, commit as i64 + 100);
        assert_eq!(epochs(&db), (commit + 1, graph), "{statement}");
    }
}

#[test]
fn reopen_preserves_it_from_the_checkpoint_and_from_wal_replay() {
    let path = unique_test_dir("graph_change_epoch_reopen");
    let expected = {
        let mut db = Database::open(&path).unwrap();
        let (_, graph) = seeded(&mut db);
        relational_write(&mut db, 1);
        relational_write(&mut db, 2);
        // The checkpoint is taken after relational-only commits, so it must
        // record the older graph change epoch explicitly.
        db.checkpoint().unwrap();
        let after_checkpoint = epochs(&db);
        assert_eq!(after_checkpoint.1, graph);
        assert!(after_checkpoint.0 > graph);
        after_checkpoint
    };
    let replayed = {
        let mut db = Database::open(&path).unwrap();
        assert_eq!(epochs(&db), expected, "reopen from the checkpoint");
        // WAL replay after the checkpoint: a graph write, then relational ones.
        db.query("MATCH (m:Memory {id: 'a'}) SET m.title = 'A'")
            .unwrap();
        let graph = db.commit_epoch().unwrap();
        relational_write(&mut db, 3);
        assert_eq!(epochs(&db), (graph + 1, graph));
        epochs(&db)
    };
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(epochs(&db), replayed, "reopen with WAL replay");
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn pruning_the_change_log_never_regresses_it() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_search_projection_change_log_entries: Some(1),
        ..DatabaseConfig::default()
    });
    let (_, graph) = seeded(&mut db);
    for id in 0..4 {
        relational_write(&mut db, id);
    }
    let status = db.search_projection_changefeed_status().unwrap();
    // The retained window has moved past the graph write ...
    assert!(status.resume_floor_commit_epoch >= graph, "{status:?}");
    // ... but the graph change epoch still names it.
    assert_eq!(db.graph_change_commit_epoch().unwrap(), graph);
}

#[test]
fn a_transaction_reads_it_and_commits_only_while_it_is_current() {
    let path = unique_test_dir("graph_change_epoch_guard");
    let mut db = Database::open(&path).unwrap();
    let (_, graph) = seeded(&mut db);
    let concurrent = ConcurrentDatabase::new(db);
    let options = ConcurrentTransactionOptions::default();
    let publish = |transaction: &mut crate::ConcurrentDatabaseTransaction, id: i64| {
        transaction
            .query_sql_with_params(
                "INSERT INTO control (id, note) VALUES ($1, 'published')",
                &[Value::Int(id)],
            )
            .unwrap();
    };
    let published = || {
        concurrent
            .query_sql("SELECT count(*) AS n FROM control WHERE note = 'published'")
            .unwrap()
            .rows[0]["n"]
            .clone()
    };

    // A concurrent relational-only commit does not disturb compare-and-publish.
    let mut transaction = concurrent.begin_transaction(options).unwrap();
    assert_eq!(transaction.graph_change_commit_epoch(), graph);
    concurrent
        .query_sql("INSERT INTO control (id, note) VALUES (100, 'other')")
        .unwrap();
    publish(&mut transaction, 1);
    transaction.commit().unwrap();
    assert_eq!(published(), Value::Int(1));

    // A concurrent graph commit refuses it, typed and retryable, and nothing
    // the transaction staged is written.
    let mut transaction = concurrent.begin_transaction(options).unwrap();
    assert_eq!(transaction.graph_change_commit_epoch(), graph);
    concurrent
        .query("MATCH (m:Memory {id: 'a'}) SET m.title = 'changed'")
        .unwrap();
    assert!(concurrent.graph_change_commit_epoch().unwrap() > graph);
    publish(&mut transaction, 2);
    let error = transaction.commit().unwrap_err();
    let HawDBError::TransactionConflict {
        read_epoch,
        committed_epoch,
        key,
    } = &error
    else {
        panic!("expected a graph change conflict, got {error:?}");
    };
    assert_eq!(key, "graph_change_commit_epoch");
    assert_eq!(*read_epoch, graph);
    assert!(*committed_epoch > graph);
    assert!(error.is_retryable_transaction_conflict());
    assert_eq!(published(), Value::Int(1));

    // A retry sees the new epoch and publishes.
    let mut transaction = concurrent.begin_transaction(options).unwrap();
    assert_eq!(transaction.graph_change_commit_epoch(), *committed_epoch);
    publish(&mut transaction, 2);
    transaction.commit().unwrap();
    assert_eq!(published(), Value::Int(2));

    // Without reading it, a transaction is not guarded.
    let mut transaction = concurrent.begin_transaction(options).unwrap();
    concurrent
        .query("MATCH (m:Memory {id: 'b'}) SET m.title = 'changed'")
        .unwrap();
    publish(&mut transaction, 3);
    transaction.commit().unwrap();
    assert_eq!(published(), Value::Int(3));
    drop(concurrent);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn a_database_transaction_guard_ignores_its_own_graph_writes() {
    let mut db = Database::new();
    let (_, graph) = seeded(&mut db);
    let mut transaction = db.begin_transaction().unwrap();
    assert_eq!(transaction.graph_change_commit_epoch(), graph);
    transaction
        .query_sql("INSERT INTO control (id, note) VALUES (1, 'published')")
        .unwrap();
    // The transaction's own graph writes do not count against its guard.
    transaction.query("CREATE (:Memory {id: 'own'})").unwrap();
    transaction.commit().unwrap();
    assert!(db.graph_change_commit_epoch().unwrap() > graph);
}

#[test]
fn embedded_status_reports_it_and_with_transaction_can_compare_and_publish() {
    use crate::{EmbeddedStoreHandle, EmbeddedStoreOpenOptions, GraphMode};
    let path = unique_test_dir("graph_change_epoch_embedded");
    let options = || EmbeddedStoreOpenOptions::graph_only(&path, GraphMode::WritableCutover);
    let status = |handle: &EmbeddedStoreHandle| {
        let status = handle.runtime_status().unwrap();
        let json = status.json();
        assert_eq!(
            json["graph_change_commit_epoch"],
            status.graph_change_commit_epoch
        );
        (status.graph_commit_epoch, status.graph_change_commit_epoch)
    };
    let graph = {
        let (handle, _) = EmbeddedStoreHandle::open_with_options(options()).unwrap();
        handle
            .with_transaction(|transaction| {
                transaction.query_sql(
                    "CREATE TABLE control (id BIGINT PRIMARY KEY, note TEXT NOT NULL)",
                )?;
                Ok(())
            })
            .unwrap();
        handle
            .with_transaction(|transaction| transaction.query("CREATE (:Memory {id: 'a'})"))
            .unwrap();
        let (commit, graph) = status(&handle);
        assert_eq!(graph, commit);
        // Compare-and-publish inside the write transaction: the host's
        // control row commits only if the graph is still at `graph`.
        handle
            .with_transaction(|transaction| {
                assert_eq!(transaction.graph_change_commit_epoch(), graph);
                transaction.query_sql("INSERT INTO control (id, note) VALUES (1, 'published')")
            })
            .unwrap();
        assert_eq!(status(&handle), (commit + 1, graph));
        let production = handle.production_status(None).unwrap().json();
        assert_eq!(production["graph"]["change_commit_epoch"], graph);
        graph
    };
    let (handle, _) = EmbeddedStoreHandle::open_with_options(options()).unwrap();
    assert_eq!(status(&handle).1, graph);
    drop(handle);
    std::fs::remove_dir_all(path).unwrap();
}
