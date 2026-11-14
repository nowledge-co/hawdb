// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn options() -> ConcurrentTransactionOptions {
    ConcurrentTransactionOptions::pessimistic(Duration::from_secs(5))
}

fn fixture() -> ConcurrentDatabase {
    let database = ConcurrentDatabase::new(Database::new());
    database
        .query_sql("CREATE TABLE warehouses (id BIGINT PRIMARY KEY, value BIGINT)")
        .unwrap();
    database
        .query_sql("INSERT INTO warehouses (id, value) VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    database
}

fn read(transaction: &mut ConcurrentDatabaseTransaction, id: i64) -> i64 {
    let output = transaction
        .query_sql_with_params(
            "SELECT value FROM warehouses WHERE id = $1",
            &[Value::Int(id)],
        )
        .unwrap();
    let Value::Int(value) = output.rows[0]["value"] else {
        panic!("expected integer")
    };
    value
}

fn retry(error: HawDBError) {
    assert!(
        matches!(error, HawDBError::TransactionRetryRequired(ref error)
        if matches!(error.reason, TransactionRetryReason::SnapshotChanged { .. })),
        "{error}"
    );
}

#[test]
fn disjoint_commits_allow_new_locks_without_refreshing_any_snapshot_rows() {
    let database = fixture();
    let mut first = database.begin_transaction(options()).unwrap();
    let original_epoch = first.base_commit_epoch;
    assert_eq!(read(&mut first, 2), 20);
    first
        .query_sql("UPDATE warehouses SET value = 11 WHERE id = 1")
        .unwrap();
    let mut second = database.begin_transaction(options()).unwrap();
    second
        .query_sql("UPDATE warehouses SET value = 21 WHERE id = 2")
        .unwrap();
    second.commit().unwrap();
    // This key was not covered by first's earlier lock. The other warehouse's
    // commit must not invalidate it, or refresh the previously observed row.
    first
        .query_sql("UPDATE warehouses SET value = 31 WHERE id = 3")
        .unwrap();
    assert_eq!(first.base_commit_epoch, original_epoch);
    assert_eq!(read(&mut first, 1), 11);
    assert_eq!(read(&mut first, 2), 20);
    assert_eq!(read(&mut first, 3), 31);
    first.commit().unwrap();
    let mut final_read = database.begin_transaction(options()).unwrap();
    assert_eq!(
        (
            read(&mut final_read, 1),
            read(&mut final_read, 2),
            read(&mut final_read, 3)
        ),
        (11, 21, 31)
    );
}

#[test]
fn changed_and_aba_keys_still_require_retry_and_release_prior_locks() {
    for sql in [
        vec!["UPDATE warehouses SET value = 22 WHERE id = 2"],
        vec!["DELETE FROM warehouses WHERE id = 2"],
        vec![
            "DELETE FROM warehouses WHERE id = 2",
            "INSERT INTO warehouses (id, value) VALUES (2, 20)",
        ],
    ] {
        let database = fixture();
        let mut stale = database.begin_transaction(options()).unwrap();
        stale
            .query_sql("UPDATE warehouses SET value = 11 WHERE id = 1")
            .unwrap();
        for statement in sql {
            let mut writer = database.begin_transaction(options()).unwrap();
            writer.query_sql(statement).unwrap();
            writer.commit().unwrap();
        }
        retry(
            stale
                .query_sql("UPDATE warehouses SET value = 23 WHERE id = 2")
                .unwrap_err(),
        );
        assert!(stale.lock_dependencies.is_none());
        retry(stale.commit().unwrap_err());
        let mut successor = database.begin_transaction(options()).unwrap();
        assert_eq!(read(&mut successor, 1), 10);
        successor
            .query_sql("UPDATE warehouses SET value = 12 WHERE id = 1")
            .unwrap();
        successor.commit().unwrap();
    }
}

#[test]
fn empty_ranges_detect_insertions_but_ignore_disjoint_changes() {
    for id in [4, 14] {
        let database = fixture();
        let mut stale = database.begin_transaction(options()).unwrap();
        assert_eq!(read(&mut stale, 1), 10);
        let mut writer = database.begin_transaction(options()).unwrap();
        writer
            .query_sql_with_params(
                "INSERT INTO warehouses (id, value) VALUES ($1, 40)",
                &[Value::Int(id)],
            )
            .unwrap();
        writer.commit().unwrap();
        let result =
            stale.query_sql("SELECT id FROM warehouses WHERE id >= 4 AND id < 10 FOR UPDATE");
        if id == 4 {
            retry(result.unwrap_err());
        } else {
            assert!(result.unwrap().rows.is_empty());
            stale.commit().unwrap();
        }
    }
}

#[test]
fn lock_wait_validation_observes_commit_or_rollback_after_the_waiter_queues() {
    for commit in [false, true] {
        let database = fixture();
        let mut stale = database.begin_transaction(options()).unwrap();
        assert_eq!(read(&mut stale, 1), 10);
        let mut writer = database.begin_transaction(options()).unwrap();
        writer
            .query_sql("UPDATE warehouses SET value = 21 WHERE id = 2")
            .unwrap();
        let waiter_id = stale.transaction_id;
        let handle = std::thread::spawn(move || {
            let result = stale.query_sql("UPDATE warehouses SET value = 22 WHERE id = 2");
            if result.is_ok() {
                stale.commit()?;
            }
            result
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        while !database.inner.locks.is_waiting(waiter_id) {
            assert!(Instant::now() < deadline, "waiter did not enter lock queue");
            std::thread::sleep(Duration::from_millis(1));
        }
        if commit {
            writer.commit().unwrap();
        } else {
            writer.rollback();
        }
        let result = handle.join().unwrap();
        if commit {
            retry(result.unwrap_err());
        } else {
            result.unwrap();
        }
    }
}

#[test]
fn schema_changes_keep_the_conservative_retry_fence() {
    let database = fixture();
    let mut stale = database.begin_transaction(options()).unwrap();
    assert_eq!(read(&mut stale, 1), 10);
    database
        .query_sql("ALTER TABLE warehouses ADD COLUMN extra BIGINT")
        .unwrap();
    retry(
        stale
            .query_sql("UPDATE warehouses SET value = 11 WHERE id = 1")
            .unwrap_err(),
    );
}

#[test]
fn empty_unproven_predicate_write_keeps_the_database_barrier() {
    let database = ConcurrentDatabase::new(Database::new());
    database
        .query_sql("CREATE TABLE warehouses (id BIGINT PRIMARY KEY, value BIGINT)")
        .unwrap();
    let mut stale = database.begin_transaction(options()).unwrap();
    assert!(stale
        .query_sql("SELECT id FROM warehouses")
        .unwrap()
        .rows
        .is_empty());
    let mut writer = database.begin_transaction(options()).unwrap();
    writer
        .query_sql("UPDATE warehouses SET value = 0 WHERE value >= 16")
        .unwrap();
    writer.commit().unwrap();
    retry(
        stale
            .query_sql("UPDATE warehouses SET value = 0 WHERE id = 2")
            .unwrap_err(),
    );
}

#[test]
fn initial_statement_can_refresh_before_any_snapshot_observation() {
    let database = fixture();
    let mut first = database.begin_transaction(options()).unwrap();
    let old_epoch = first.base_commit_epoch;
    database
        .query_sql("UPDATE warehouses SET value = 21 WHERE id = 2")
        .unwrap();
    first
        .query_sql("SELECT value FROM warehouses WHERE id = 2 FOR SHARE")
        .unwrap();
    assert!(first.base_commit_epoch > old_epoch);
    assert_eq!(
        first.lock_dependencies.as_ref().unwrap().source_epoch(),
        first.base_commit_epoch
    );
    assert_eq!(read(&mut first, 2), 21);
}

#[test]
fn dependency_history_survives_checkpoint_and_incremental_writes_recover() {
    for mode in [
        crate::StorageResidencyMode::Materialized,
        crate::StorageResidencyMode::OutOfCore,
    ] {
        let path = std::env::temp_dir().join(format!(
            "hawdb-lock-dependencies-{}-{mode:?}",
            std::process::id()
        ));
        let config = DatabaseConfig {
            storage_residency_mode: mode,
            ..DatabaseConfig::default()
        };
        let mut setup = Database::open_with_config(&path, config.clone()).unwrap();
        setup
            .query_sql("CREATE TABLE warehouses (id BIGINT PRIMARY KEY, value BIGINT)")
            .unwrap();
        setup
            .query_sql("INSERT INTO warehouses (id, value) VALUES (1, 10), (2, 20)")
            .unwrap();
        setup.checkpoint().unwrap();
        drop(setup);
        let database = Database::open_with_config(&path, config.clone())
            .unwrap()
            .into_concurrent();
        let original_epoch = database.commit_epoch().unwrap();
        let old_read = database.begin_read_transaction().unwrap();
        let mut first = database.begin_transaction(options()).unwrap();
        assert_eq!(read(&mut first, 2), 20);
        let mut second = database.begin_transaction(options()).unwrap();
        second
            .query_sql("UPDATE warehouses SET value = 21 WHERE id = 2")
            .unwrap();
        second.commit().unwrap();
        database.checkpoint().unwrap();
        first
            .query_sql("UPDATE warehouses SET value = 11 WHERE id = 1")
            .unwrap();
        assert_eq!(read(&mut first, 2), 20);
        first.commit().unwrap();
        assert_eq!(database.commit_epoch().unwrap(), original_epoch + 2);
        assert_eq!(
            old_read
                .query_sql("SELECT value FROM warehouses WHERE id = 1")
                .unwrap()
                .rows[0]["value"],
            Value::Int(10)
        );
        drop(old_read);
        drop(database);
        for checkpoint in [true, false] {
            let mut reopened = Database::open_with_config(&path, config.clone()).unwrap();
            let rows = reopened
                .query_sql("SELECT value FROM warehouses ORDER BY id")
                .unwrap()
                .rows;
            assert_eq!(
                rows.iter()
                    .map(|row| row["value"].clone())
                    .collect::<Vec<_>>(),
                vec![Value::Int(11), Value::Int(21)]
            );
            if checkpoint {
                reopened.checkpoint().unwrap();
            }
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}
