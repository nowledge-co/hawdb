// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! The graph change commit epoch (#1840): the commit epoch of the newest
//! commit that changed graph data or graph schema. Relational-only commits do
//! not advance it; node, relationship and schema commits do. It never
//! regresses, across reopen or change-log pruning (a reopen may advance it
//! once, to the checkpoint's commit epoch), and a transaction can read it and
//! commit only if it is still current.

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
fn reopen_never_regresses_it_and_wal_replay_restores_it_exactly() {
    let path = unique_test_dir("graph_change_epoch_reopen");
    let (checkpoint, graph) = {
        let mut db = Database::open(&path).unwrap();
        let (_, graph) = seeded(&mut db);
        relational_write(&mut db, 1);
        relational_write(&mut db, 2);
        // The checkpoint is taken after relational-only commits.
        db.checkpoint().unwrap();
        let (checkpoint, after) = epochs(&db);
        assert_eq!(after, graph);
        assert!(checkpoint > graph);
        (checkpoint, graph)
    };
    let replayed = {
        let mut db = Database::open(&path).unwrap();
        // Checkpoints do not record it (no format change): a reopen starts
        // from the checkpoint's commit epoch, an upper bound of the last graph
        // change, so with no lost commits it never regresses and advances at
        // most once.
        let (commit, reopened) = epochs(&db);
        assert_eq!(commit, checkpoint);
        assert_eq!(reopened, checkpoint);
        assert!(reopened >= graph);
        // Relational commits after the reopen still leave it unchanged.
        relational_write(&mut db, 3);
        assert_eq!(epochs(&db), (checkpoint + 1, checkpoint));
        // WAL after the checkpoint: a graph write, then a relational one.
        db.query("MATCH (m:Memory {id: 'a'}) SET m.title = 'A'")
            .unwrap();
        let graph = db.commit_epoch().unwrap();
        relational_write(&mut db, 4);
        assert_eq!(epochs(&db), (graph + 1, graph));
        epochs(&db)
    };
    {
        // Replaying that WAL restores the exact value.
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
    assert_eq!(transaction.graph_change_commit_epoch().unwrap(), graph);
    concurrent
        .query_sql("INSERT INTO control (id, note) VALUES (100, 'other')")
        .unwrap();
    publish(&mut transaction, 1);
    transaction.commit().unwrap();
    assert_eq!(published(), Value::Int(1));

    // A concurrent graph commit refuses it, typed and retryable, and nothing
    // the transaction staged is written.
    let mut transaction = concurrent.begin_transaction(options).unwrap();
    assert_eq!(transaction.graph_change_commit_epoch().unwrap(), graph);
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
    assert_eq!(
        transaction.graph_change_commit_epoch().unwrap(),
        *committed_epoch
    );
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
    assert_eq!(transaction.graph_change_commit_epoch().unwrap(), graph);
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
                assert_eq!(transaction.graph_change_commit_epoch().unwrap(), graph);
                transaction.query_sql("INSERT INTO control (id, note) VALUES (1, 'published')")
            })
            .unwrap();
        assert_eq!(status(&handle), (commit + 1, graph));
        let production = handle.production_status(None).unwrap().json();
        assert_eq!(production["graph"]["change_commit_epoch"], graph);
        graph
    };
    let (handle, _) = EmbeddedStoreHandle::open_with_options(options()).unwrap();
    // Never regresses across a reopen, and never passes the commit epoch.
    let (commit, reopened) = status(&handle);
    assert!(
        reopened >= graph && reopened <= commit,
        "{reopened} {graph} {commit}"
    );
    drop(handle);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn append_and_initial_import_marker_commits_do_not_advance_it() {
    let path = unique_test_dir("graph_change_epoch_marker");
    // An append-only commit.
    let mut db = Database::new();
    let (_, graph) = seeded(&mut db);
    db.query_sql(
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
    let before = db.commit_epoch().unwrap();
    db.append_transaction(crate::AppendTransaction {
        writes: vec![crate::AppendWrite::AppendGenerated {
            table: "events".to_string(),
            rows: vec![crate::AppendGeneratedRow::new(vec![
                crate::RelationalValue::Text("thread-1".to_string()),
                crate::RelationalValue::Text("appended".to_string()),
            ])],
        }],
    })
    .unwrap();
    assert!(db.commit_epoch().unwrap() > before);
    assert_eq!(db.graph_change_commit_epoch().unwrap(), graph);

    // An initial import without graph rows commits only the import marker
    // and relational rows. The live value must match what WAL replay
    // derives, so a clean reopen never moves it backwards.
    let mut source = Database::new();
    source
        .query_sql("CREATE TABLE control (id BIGINT PRIMARY KEY, note TEXT NOT NULL)")
        .unwrap();
    relational_write(&mut source, 1);
    let export = source.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let imported = {
        let mut target = Database::open(&path).unwrap();
        let (before_commit, before_graph) = epochs(&target);
        let report = target
            .hawdb_lightning_initial_import_apply(
                &export.graph_stream.encoded,
                &export.relational_stream.encoded,
                &export.manifest,
                None,
                None,
            )
            .unwrap();
        assert!(report.applied);
        let (commit, graph) = epochs(&target);
        assert!(commit > before_commit);
        assert_eq!(
            graph, before_graph,
            "an import without graph rows changes no graph"
        );
        graph
    };
    let target = Database::open(&path).unwrap();
    assert!(target.graph_change_commit_epoch().unwrap() >= imported);
    drop(target);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn a_poisoned_concurrent_database_refuses_to_report_it() {
    use std::num::{NonZeroU64, NonZeroUsize};
    let path = unique_test_dir("graph_change_epoch_poisoned");
    let mut database = Database::open(&path).unwrap();
    seeded(&mut database);
    let group_commit = crate::WalGroupCommitConfig::benchmark_candidate(
        NonZeroUsize::new(1).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        std::time::Duration::ZERO,
    )
    .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(database, group_commit);
    // Opened before the failure, so the transaction accessor is exercised
    // on a poisoned store.
    let mut held = db
        .begin_transaction(ConcurrentTransactionOptions::default())
        .unwrap();
    let mut transaction = db
        .begin_transaction(ConcurrentTransactionOptions::default())
        .unwrap();
    transaction
        .query("MATCH (m:Memory {id: 'a'}) SET m.title = 'uncertain'")
        .unwrap();
    crate::store::set_wal_group_sync_failpoint(true);
    let error = transaction.commit().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("WAL group durability barrier failed"),
        "{error}"
    );
    // The epoch of the uncertain commit cannot be relied on: every accessor
    // reports the storage error instead.
    let refused = db.graph_change_commit_epoch().unwrap_err();
    assert!(
        refused.to_string().contains("close and reopen"),
        "{refused}"
    );
    let refused = held.graph_change_commit_epoch().unwrap_err();
    assert!(
        refused.to_string().contains("close and reopen"),
        "{refused}"
    );
    held.rollback();
    drop(db);
    let reopened = Database::open(&path).unwrap();
    assert!(reopened.graph_change_commit_epoch().is_ok());
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn a_poisoned_database_refuses_to_report_it_in_transactions_and_status() {
    let path = unique_test_dir("graph_change_epoch_poisoned_apply");
    let mut db = Database::open(&path).unwrap();
    seeded(&mut db);
    let mut transaction = db.begin_transaction().unwrap();
    transaction.query("CREATE (:Memory {id: 'c'})").unwrap();
    transaction.query("CREATE (:Memory {id: 'd'})").unwrap();
    crate::store::set_wal_apply_failpoint(Some(1));
    let commit_error = transaction.commit().unwrap_err();
    crate::store::set_wal_apply_failpoint(None);
    assert!(
        commit_error
            .to_string()
            .contains("injected failure while applying a durable WAL batch"),
        "{commit_error}"
    );
    assert!(db.storage_handle_poisoned().unwrap());

    let refused = db.graph_change_commit_epoch().unwrap_err();
    assert!(
        refused.to_string().contains("close and reopen"),
        "{refused}"
    );
    // A `DatabaseTransaction` borrows the database exclusively, so it cannot
    // be open while another commit poisons the store; a new one is refused
    // with the same storage error before its accessor can be reached.
    let refused = db.begin_transaction().map(drop).unwrap_err();
    assert!(
        refused.to_string().contains("close and reopen"),
        "{refused}"
    );
    let graph = crate::EmbeddedGraph::from_database(db, crate::GraphMode::WritableCutover);
    let store = crate::EmbeddedStore::new(graph, None);
    let refused = store.runtime_status().unwrap_err();
    assert!(
        refused.to_string().contains("close and reopen"),
        "{refused}"
    );
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
