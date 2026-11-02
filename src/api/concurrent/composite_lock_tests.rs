// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_storage::transaction_locks::LockNamespace;

fn transaction(database: &ConcurrentDatabase) -> ConcurrentDatabaseTransaction {
    database
        .begin_transaction(ConcurrentTransactionOptions::pessimistic(Duration::ZERO))
        .unwrap()
}

fn timeout(result: Result<QueryOutput>) {
    let error = result.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("transaction lock wait timed out"),
        "{error}"
    );
}

fn retry(result: Result<QueryOutput>) {
    let error = result.unwrap_err();
    assert!(
        matches!(&error, HawDBError::TransactionRetryRequired(error)
        if matches!(error.reason, TransactionRetryReason::SnapshotChanged { .. })),
        "{error}"
    );
}

fn with_fixture(name: &str, mut test: impl FnMut(&ConcurrentDatabase)) {
    for mode in [
        crate::StorageResidencyMode::Materialized,
        crate::StorageResidencyMode::OutOfCore,
    ] {
        let path = std::env::temp_dir().join(format!(
            "hawdb-composite-lock-{name}-{}-{mode:?}",
            std::process::id()
        ));
        let config = DatabaseConfig {
            storage_residency_mode: mode,
            ..DatabaseConfig::default()
        };
        let mut setup = Database::open_with_config(&path, config.clone()).unwrap();
        setup.query_sql("CREATE TABLE lines (w BIGINT, d BIGINT, o BIGINT, n BIGINT, value BIGINT, PRIMARY KEY (w, d, o, n))").unwrap();
        setup.query_sql("INSERT INTO lines (w, d, o, n, value) VALUES (1, 1, 1, 1, 0), (1, 1, 1, 2, 0), (1, 1, 2, 1, 0), (2, 1, 1, 1, 0)").unwrap();
        setup.query_sql("CREATE TABLE customers (w BIGINT, d BIGINT, c BIGINT, last TEXT NOT NULL, first TEXT NOT NULL, value BIGINT, PRIMARY KEY (w, d, c))").unwrap();
        setup
            .query_sql("CREATE INDEX by_name ON customers (w, d, last, first, c)")
            .unwrap();
        setup.query_sql("INSERT INTO customers (w, d, c, last, first, value) VALUES (1, 1, 1, 'A', 'Alpha', 0), (1, 1, 2, 'B', 'Beta', 0), (2, 1, 1, 'A', 'Alpha', 0)").unwrap();
        setup.checkpoint().unwrap();
        drop(setup);
        let database = Database::open_with_config(&path, config)
            .unwrap()
            .into_concurrent();
        test(&database);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }
}

fn assert_narrow(transaction: &ConcurrentDatabaseTransaction, table: &str) {
    assert!(!transaction
        .inner
        .locks
        .covers_all(
            transaction.transaction_id,
            &[LockRequest::relational_table(LockMode::Shared, table)]
        )
        .unwrap());
}

#[test]
fn disjoint_composite_mutations_keep_order_prefixes_independent() {
    with_fixture("mutations", |database| {
        let mut first = transaction(database);
        let mut second = transaction(database);
        first
            .query_sql_with_params(
                "UPDATE lines SET value = $1 WHERE w = $2 AND d = 1 AND o = 1 AND value = 0",
                &[Value::Int(10), Value::Int(1)],
            )
            .unwrap();
        second
            .query_sql("DELETE FROM lines WHERE w = 2 AND d = 1 AND o = 1")
            .unwrap();
        assert_narrow(&first, "lines");
        assert_narrow(&second, "lines");
        let mut overlap = transaction(database);
        timeout(
            overlap
                .query_sql("UPDATE lines SET value = 99 WHERE w = 1 AND d = 1 AND o = 1 AND n = 2"),
        );
        first.commit().unwrap();
        second.commit().unwrap();
        let rows = database
            .query_sql("SELECT w, o, n, value FROM lines ORDER BY w, o, n")
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows.iter()
                .map(|row| row["value"].clone())
                .collect::<Vec<_>>(),
            vec![Value::Int(10), Value::Int(10), Value::Int(0)]
        );
    });
}

#[test]
fn composite_empty_and_exclusive_ranges_protect_every_suffix() {
    with_fixture("gaps", |database| {
        let mut guard = transaction(database);
        guard
            .query_sql("SELECT n FROM lines WHERE w = 1 AND d = 1 AND o > 1 AND o <= 3 FOR UPDATE")
            .unwrap();
        assert_narrow(&guard, "lines");
        for order in [2, 3] {
            let mut insert = transaction(database);
            timeout(insert.query_sql_with_params(
                "INSERT INTO lines (w, d, o, n, value) VALUES (1, 1, $1, 9, 0)",
                &[Value::Int(order)],
            ));
        }
        for order in [1, 4] {
            let mut insert = transaction(database);
            insert
                .query_sql_with_params(
                    "INSERT INTO lines (w, d, o, n, value) VALUES (1, 1, $1, 9, 0)",
                    &[Value::Int(order)],
                )
                .unwrap();
            insert.commit().unwrap();
        }
        let mut empty = transaction(database);
        assert!(empty
            .query_sql("SELECT n FROM lines WHERE w = 3 AND d = 1 FOR SHARE")
            .unwrap()
            .rows
            .is_empty());
        let mut blocked = transaction(database);
        timeout(
            blocked.query_sql("INSERT INTO lines (w, d, o, n, value) VALUES (3, 1, 99, 99, 0)"),
        );
        empty.rollback();
        let mut released = transaction(database);
        released
            .query_sql("INSERT INTO lines (w, d, o, n, value) VALUES (3, 1, 99, 99, 0)")
            .unwrap();
        released.commit().unwrap();
    });
}

#[test]
fn secondary_guards_coordinate_primary_locks_and_payload_writers() {
    with_fixture("secondary", |database| {
        let mut guard = transaction(database);
        guard
            .query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'A' FOR UPDATE")
            .unwrap();
        assert_narrow(&guard, "customers");
        for sql in [
            "SELECT c FROM customers WHERE w = 1 AND d = 1 AND c = 1 FOR SHARE",
            "UPDATE customers SET value = 1 WHERE w = 1 AND d = 1 AND c = 1",
            "DELETE FROM customers WHERE w = 1 AND d = 1 AND c = 1",
        ] {
            timeout(transaction(database).query_sql(sql));
        }
        let mut disjoint = transaction(database);
        disjoint
            .query_sql("UPDATE customers SET value = 2 WHERE w = 1 AND d = 1 AND c = 2")
            .unwrap();
        assert_narrow(&disjoint, "customers");
        disjoint.commit().unwrap();
        guard.rollback();
        let mut primary = transaction(database);
        primary
            .query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND c = 1 FOR UPDATE")
            .unwrap();
        timeout(
            transaction(database).query_sql(
                "SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'A' FOR UPDATE",
            ),
        );
        primary.rollback();
        let mut writer = transaction(database);
        writer
            .query_sql("UPDATE customers SET value = 3 WHERE w = 1 AND d = 1 AND c = 1")
            .unwrap();
        timeout(
            transaction(database).query_sql(
                "SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'A' FOR SHARE",
            ),
        );
    });
}

#[test]
fn secondary_gaps_cover_null_suffixes_and_old_and_new_index_keys() {
    with_fixture("secondary-gaps", |database| {
        let mut guard = transaction(database);
        guard.query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND last >= 'A' AND last < 'B' FOR UPDATE").unwrap();
        // Equal primary prefix width must not prefer the unbounded suffix over
        // the additional last-name interval.
        assert_narrow(&guard, "customers");
        for first in ["'Alpha'", "'Zulu'"] {
            timeout(transaction(database).query_sql(&format!("INSERT INTO customers (w, d, c, last, first, value) VALUES (1, 1, 3, 'A', {first}, 0)")));
        }
        timeout(
            transaction(database)
                .query_sql("UPDATE customers SET last = 'Z' WHERE w = 1 AND d = 1 AND c = 1"),
        );
        timeout(
            transaction(database)
                .query_sql("UPDATE customers SET last = 'A' WHERE w = 1 AND d = 1 AND c = 2"),
        );
        let mut disjoint = transaction(database);
        disjoint.query_sql("INSERT INTO customers (w, d, c, last, first, value) VALUES (1, 1, 3, 'B', 'Beta', 0)").unwrap();
        disjoint.commit().unwrap();
        guard.rollback();
        let mut empty = transaction(database);
        assert!(empty
            .query_sql(
                "SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'Missing' FOR UPDATE"
            )
            .unwrap()
            .rows
            .is_empty());
        timeout(transaction(database).query_sql("INSERT INTO customers (w, d, c, last, first, value) VALUES (1, 1, 4, 'Missing', 'Alpha', 0)"));
        empty.rollback();

        database.query_sql("CREATE TABLE nullable_keys (id BIGINT PRIMARY KEY, name TEXT NOT NULL, tail BIGINT)").unwrap();
        database
            .query_sql("CREATE INDEX nullable_name ON nullable_keys (name, tail)")
            .unwrap();
        let mut nullable = transaction(database);
        nullable
            .query_sql("SELECT id FROM nullable_keys WHERE name = 'A' FOR SHARE")
            .unwrap();
        timeout(
            transaction(database)
                .query_sql("INSERT INTO nullable_keys (id, name, tail) VALUES (1, 'A', NULL)"),
        );
        nullable.rollback();
        let mut null_writer = transaction(database);
        null_writer
            .query_sql("INSERT INTO nullable_keys (id, name, tail) VALUES (1, 'A', NULL)")
            .unwrap();
        timeout(
            transaction(database)
                .query_sql("SELECT id FROM nullable_keys WHERE name = 'A' FOR UPDATE"),
        );
    });
}

#[test]
fn literal_secondary_rekeys_lock_both_images_without_blocking_unrelated_names() {
    with_fixture("rekeys", |database| {
        let mut writer = transaction(database);
        writer
            .query_sql_with_params(
                "UPDATE customers SET last = $1 WHERE w = 1 AND d = 1 AND c = 1",
                &[Value::String("Z".into())],
            )
            .unwrap();
        assert_eq!(
            writer
                .query_sql(
                    "SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'Z' FOR UPDATE"
                )
                .unwrap()
                .rows
                .len(),
            1
        );
        assert_narrow(&writer, "customers");
        for last in ["A", "Z"] {
            timeout(transaction(database).query_sql_with_params(
                "SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = $1 FOR UPDATE",
                &[Value::String(last.into())],
            ));
        }
        let mut independent = transaction(database);
        independent
            .query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'B' FOR UPDATE")
            .unwrap();
        independent.rollback();
        writer.commit().unwrap();
        assert!(database
            .query_sql("SELECT c FROM customers WHERE last = 'A' AND w = 1")
            .unwrap()
            .rows
            .is_empty());
        assert_eq!(
            database
                .query_sql("SELECT c FROM customers WHERE last = 'Z' AND w = 1")
                .unwrap()
                .rows
                .len(),
            1
        );
    });
}

#[test]
fn repeated_upsert_targets_cannot_escape_through_combined_index_keys() {
    with_fixture("upsert-images", |database| {
        let mut guard = transaction(database);
        guard.query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'D' AND first = 'Alpha' FOR UPDATE").unwrap();
        timeout(transaction(database).query_sql(
            "INSERT INTO customers (w, d, c, last, first, value) VALUES \
             (1, 1, 3, 'C', 'Alpha', 0), (1, 1, 3, 'D', 'Beta', 0) \
             ON CONFLICT (w, d, c) DO UPDATE SET last = EXCLUDED.last",
        ));
        guard.rollback();
        assert!(database
            .query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND c = 3")
            .unwrap()
            .rows
            .is_empty());
    });
}

#[test]
fn autocommit_updates_use_the_same_primary_and_secondary_lock_protocol() {
    with_fixture("autocommit", |database| {
        for (update, locking_read) in [
            (
                "UPDATE customers SET value = 4 WHERE w = 1 AND d = 1 AND last = 'A'",
                "SELECT c FROM customers WHERE w = 1 AND d = 1 AND c = 1 FOR UPDATE",
            ),
            (
                "UPDATE customers SET value = 5 WHERE w = 1 AND d = 1 AND c = 1",
                "SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'A' FOR UPDATE",
            ),
        ] {
            let (ready, resume) =
                database.pause_next_autocommit_mutation(AutocommitMutationPhase::BeforeExecution);
            let writer_database = database.clone();
            let writer = std::thread::spawn(move || writer_database.query_sql(update));
            ready.recv_timeout(Duration::from_secs(5)).unwrap();
            let conflict = transaction(database).query_sql(locking_read);
            let independent = transaction(database)
                .query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND c = 2 FOR UPDATE");
            resume.send(()).unwrap();
            writer.join().unwrap().unwrap();
            timeout(conflict);
            independent.unwrap();
        }
    });
}

#[test]
fn short_prefix_validation_detects_changes_and_preserves_disjoint_snapshots() {
    with_fixture("validation", |database| {
        for (table, selection, write) in [
            (
                "lines",
                "w = 1 AND d = 1 AND o = 1",
                "UPDATE lines SET value = 1 WHERE w = 1 AND d = 1 AND o = 1 AND n = 1",
            ),
            (
                "customers",
                "w = 1 AND d = 1 AND last = 'A'",
                "UPDATE customers SET value = 1 WHERE w = 1 AND d = 1 AND c = 1",
            ),
        ] {
            let mut stale = transaction(database);
            stale
                .query_sql("SELECT value FROM lines WHERE w = 2")
                .unwrap();
            let mut writer = transaction(database);
            writer.query_sql(write).unwrap();
            writer.commit().unwrap();
            retry(stale.query_sql(&format!(
                "SELECT value FROM {table} WHERE {selection} FOR UPDATE"
            )));
        }
        let mut stale = transaction(database);
        let epoch = stale.base_commit_epoch;
        stale
            .query_sql("SELECT value FROM customers WHERE w = 2")
            .unwrap();
        let mut writer = transaction(database);
        writer
            .query_sql("UPDATE customers SET value = 9 WHERE w = 2 AND d = 1 AND c = 1")
            .unwrap();
        writer.commit().unwrap();
        stale
            .query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'A' FOR UPDATE")
            .unwrap();
        assert_eq!(stale.base_commit_epoch, epoch);
        assert_eq!(
            stale
                .query_sql("SELECT value FROM customers WHERE w = 2")
                .unwrap()
                .rows[0]["value"],
            Value::Int(0)
        );
        stale.commit().unwrap();
    });
}

fn wait_for_lock(database: &ConcurrentDatabase, transaction_id: u64) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !database.inner.locks.is_waiting(transaction_id) {
        assert!(
            Instant::now() < deadline,
            "waiter did not enter the lock queue"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn first_statement_wait_rederives_secondary_candidates_after_refresh() {
    with_fixture("refresh", |database| {
        let mut reader = database
            .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                Duration::from_secs(5),
            ))
            .unwrap();
        let original_epoch = reader.base_commit_epoch;
        let mut writer = transaction(database);
        writer.query_sql("INSERT INTO customers (w, d, c, last, first, value) VALUES (1, 1, 3, 'New', 'Alpha', 0)").unwrap();
        let id = reader.transaction_id;
        let handle = std::thread::spawn(move || {
            let rows = reader
                .query_sql(
                    "SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'New' FOR UPDATE",
                )
                .unwrap()
                .rows;
            (reader, rows)
        });
        wait_for_lock(database, id);
        writer.commit().unwrap();
        let (reader, rows) = handle.join().unwrap();
        assert!(reader.base_commit_epoch > original_epoch);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["c"], Value::Int(3));
        timeout(
            transaction(database)
                .query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND c = 3 FOR UPDATE"),
        );
        reader.rollback();
    });
}

#[test]
fn full_scan_wait_validates_unchanged_and_absent_write_intents() {
    with_fixture("full-scan-intents", |database| {
        for warehouse in [1, 9] {
            let mut reader = database
                .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                    Duration::from_secs(5),
                ))
                .unwrap();
            reader.query_sql("SELECT value FROM lines").unwrap();
            let original_epoch = reader.base_commit_epoch;
            let mut writer = transaction(database);
            writer
                .query_sql_with_params(
                    "UPDATE lines SET value = 0 WHERE w = $1 AND d = 1 AND o = 1 AND n = 1",
                    &[Value::Int(warehouse)],
                )
                .unwrap();
            let id = reader.transaction_id;
            let handle = std::thread::spawn(move || {
                retry(reader.query_sql("SELECT value FROM lines FOR SHARE"));
                assert_eq!(reader.base_commit_epoch, original_epoch);
            });
            wait_for_lock(database, id);
            writer.commit().unwrap();
            handle.join().unwrap();
        }
    });
}

#[test]
fn secondary_discovery_escalates_before_returning_a_partial_lock_set() {
    with_fixture("escalation", |database| {
        let rows = (3..=67)
            .map(|c| format!("(1, 1, {c}, 'Many', 'Alpha', 0)"))
            .collect::<Vec<_>>()
            .join(",");
        database
            .query_sql(&format!(
                "INSERT INTO customers (w, d, c, last, first, value) VALUES {rows}"
            ))
            .unwrap();
        let mut guard = transaction(database);
        let rows = guard.query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'Many' LIMIT 1 FOR UPDATE").unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert!(database
            .inner
            .locks
            .covers_all(
                guard.transaction_id,
                &[LockRequest::relational_table(
                    LockMode::Exclusive,
                    "customers"
                )]
            )
            .unwrap());
        timeout(
            transaction(database)
                .query_sql("UPDATE customers SET value = 1 WHERE w = 2 AND d = 1 AND c = 1"),
        );
    });
}

#[test]
fn sparse_discovery_work_budget_escalates_without_refusing_valid_statements() {
    for residency in [
        crate::StorageResidencyMode::Materialized,
        crate::StorageResidencyMode::OutOfCore,
    ] {
        for composite in [false, true] {
            let path = std::env::temp_dir().join(format!(
                "hawdb-discovery-work-{}-{residency:?}-{composite}",
                std::process::id()
            ));
            if path.exists() {
                std::fs::remove_dir_all(&path).unwrap();
            }
            let config = DatabaseConfig {
                storage_residency_mode: residency,
                ..Default::default()
            };
            let mut setup = Database::open_with_config(&path, config.clone()).unwrap();
            let primary = if composite { "w, id" } else { "id" };
            setup.query_sql(&format!("CREATE TABLE sparse_rows (w BIGINT, id BIGINT, status BIGINT, aux BIGINT, value BIGINT, PRIMARY KEY ({primary}))")).unwrap();
            setup
                .query_sql("CREATE INDEX by_aux ON sparse_rows (aux)")
                .unwrap();
            let values = (1..=5000)
                .map(|id| format!("(1,{id},{},0,0)", i64::from(id == 5000)))
                .chain(std::iter::once("(2,5001,0,0,0)".into()))
                .collect::<Vec<_>>()
                .join(",");
            setup
                .query_sql(&format!(
                    "INSERT INTO sparse_rows (w,id,status,aux,value) VALUES {values}"
                ))
                .unwrap();
            setup.checkpoint().unwrap();
            drop(setup);
            let database = Database::open_with_config(&path, config.clone())
                .unwrap()
                .into_concurrent();
            let predicate = if composite {
                "w = 1 AND id > 0 AND status = 1"
            } else {
                "id > 0 AND status = 1"
            };
            for sql in [
                format!("UPDATE sparse_rows SET value = 1 WHERE {predicate}"),
                format!("DELETE FROM sparse_rows WHERE {predicate}"),
                "SELECT id FROM sparse_rows WHERE aux >= 0 AND status = 1 FOR UPDATE".into(),
            ] {
                let mut owner = transaction(&database);
                owner.query_sql(&sql).unwrap();
                assert!(
                    database
                        .inner
                        .locks
                        .covers_all(
                            owner.transaction_id,
                            &[LockRequest::relational_table(
                                LockMode::Exclusive,
                                "sparse_rows"
                            )]
                        )
                        .unwrap(),
                    "{sql}"
                );
                timeout(
                    transaction(&database)
                        .query_sql("UPDATE sparse_rows SET value = 9 WHERE w = 2 AND id = 5001"),
                );
                owner.rollback();
            }
            database
                .query_sql(&format!(
                    "UPDATE sparse_rows SET value = 7 WHERE {predicate}"
                ))
                .unwrap();
            assert_eq!(
                database
                    .query_sql("SELECT value FROM sparse_rows WHERE w = 1 AND id = 5000")
                    .unwrap()
                    .rows[0]["value"],
                Value::Int(7)
            );
            database
                .query_sql(&format!("DELETE FROM sparse_rows WHERE {predicate}"))
                .unwrap();
            assert!(database
                .query_sql("SELECT id FROM sparse_rows WHERE w = 1 AND id = 5000")
                .unwrap()
                .rows
                .is_empty());
            assert_eq!(
                database
                    .query_sql("SELECT value FROM sparse_rows WHERE w = 2 AND id = 5001")
                    .unwrap()
                    .rows[0]["value"],
                Value::Int(0)
            );
            drop(database);

            let limited = Database::open_with_config(
                &path,
                DatabaseConfig {
                    max_read_result_rows: Some(128),
                    ..config
                },
            )
            .unwrap()
            .into_concurrent();
            let mut owner = transaction(&limited);
            let error = owner
                .query_sql(&format!(
                    "UPDATE sparse_rows SET value = 8 WHERE {predicate}"
                ))
                .unwrap_err();
            assert!(
                matches!(&error, HawDBError::ResourceBudgetExceeded(cause) if cause.budget == 128),
                "{error}"
            );
            assert_narrow(&owner, "sparse_rows");
            owner.rollback();
            drop(limited);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn implicit_table_escalation_does_not_skip_validation_for_new_keys() {
    with_fixture("escalation-validation", |database| {
        let mut stale = transaction(database);
        for order in 1..=DEFAULT_LOCK_ESCALATION_ENTRIES_PER_TABLE {
            stale
                .query_sql_with_params(
                    "SELECT n FROM lines WHERE w = 1 AND d = 1 AND o = $1 AND n = 1 FOR UPDATE",
                    &[Value::Int(order as i64)],
                )
                .unwrap();
        }
        let mut writer = transaction(database);
        writer
            .query_sql("UPDATE lines SET value = 7 WHERE w = 2 AND d = 1 AND o = 1 AND n = 1")
            .unwrap();
        writer.commit().unwrap();
        stale
            .query_sql("SELECT n FROM lines WHERE w = 1 AND d = 1 AND o = 65 AND n = 1 FOR UPDATE")
            .unwrap();
        assert!(database
            .inner
            .locks
            .covers_all(
                stale.transaction_id,
                &[LockRequest::relational_table(LockMode::Exclusive, "lines")]
            )
            .unwrap());
        retry(stale.query_sql(
            "SELECT value FROM lines WHERE w = 2 AND d = 1 AND o = 1 AND n = 1 FOR UPDATE",
        ));
    });
}

#[test]
fn large_index_replacements_escalate_before_multiplying_key_payloads() {
    with_fixture("key-bytes", |database| {
        let mut writer = transaction(database);
        let prepared = writer
            .runtime
            .relational_plan_template_cache
            .prepare("UPDATE customers SET first = $1 WHERE w = 1 AND d = 1 AND c = 1")
            .unwrap();
        writer
            .acquire_sql_locks(&prepared, &[Value::String("x".repeat(512 * 1024))], None)
            .unwrap();
        assert!(database
            .inner
            .locks
            .covers_all(
                writer.transaction_id,
                &[LockRequest::relational_table(
                    LockMode::Exclusive,
                    "customers"
                )]
            )
            .unwrap());
        timeout(
            transaction(database)
                .query_sql("UPDATE customers SET value = 1 WHERE w = 2 AND d = 1 AND c = 1"),
        );
        writer.rollback();
        assert_eq!(
            database
                .query_sql("SELECT first FROM customers WHERE w = 1 AND d = 1 AND c = 1")
                .unwrap()
                .rows[0]["first"],
            Value::String("Alpha".into())
        );
    });
}

#[test]
fn cross_namespace_deadlock_aborts_one_owner_and_releases_its_locks() {
    with_fixture("deadlock", |database| {
        let mut first = database
            .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                Duration::from_secs(5),
            ))
            .unwrap();
        let mut second = transaction(database);
        first
            .query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND c = 1 FOR UPDATE")
            .unwrap();
        second
            .query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'B' FOR UPDATE")
            .unwrap();
        let id = first.transaction_id;
        let handle = std::thread::spawn(move || {
            first.query_sql(
                "SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'B' FOR UPDATE",
            )?;
            first.commit()
        });
        wait_for_lock(database, id);
        let error = second
            .query_sql("SELECT c FROM customers WHERE w = 1 AND d = 1 AND last = 'A' FOR UPDATE")
            .unwrap_err();
        assert!(error.to_string().contains("deadlock"), "{error}");
        assert!(second.commit().is_err());
        handle.join().unwrap().unwrap();
    });
}

#[test]
fn canonical_composite_prefix_bounds_are_not_short_point_keys() {
    with_fixture("contract", |database| {
        let mut owner = transaction(database);
        owner
            .query_sql("SELECT n FROM lines WHERE w = 1 AND d = 1 AND o = 1 FOR SHARE")
            .unwrap();
        let columns = ["w", "d", "o", "n"].map(str::to_owned).to_vec();
        let key = |values: &[i64]| {
            RelationalKey(
                values
                    .iter()
                    .copied()
                    .map(RelationalValue::BigInt)
                    .collect(),
            )
        };
        assert!(database
            .inner
            .locks
            .covers_all(
                owner.transaction_id,
                &[LockRequest {
                    mode: LockMode::Shared,
                    target: LockTarget::RelationalRange {
                        namespace: LockNamespace::RelationalIndex {
                            table: "lines".into(),
                            columns
                        },
                        lower: Bound::Included(key(&[1, 1, 1])),
                        upper: Bound::Excluded(key(&[1, 1, 2])),
                    },
                }]
            )
            .unwrap());
    });
}

#[test]
fn single_key_contradictions_remain_lock_free() {
    let database = Database::new().into_concurrent();
    database
        .query_sql("CREATE TABLE keys (id BIGINT PRIMARY KEY)")
        .unwrap();
    let mut owner = transaction(&database);
    owner
        .query_sql("SELECT id FROM keys WHERE id = 1 FOR UPDATE")
        .unwrap();
    let mut disjoint = transaction(&database);
    assert!(disjoint
        .query_sql("SELECT id FROM keys WHERE id = 1 AND id = 2 FOR UPDATE")
        .unwrap()
        .rows
        .is_empty());
    disjoint.commit().unwrap();
}
