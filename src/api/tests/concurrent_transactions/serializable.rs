// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{TransactionIsolation, TransactionReadDependencyLimits};

fn options(mode: crate::ConcurrentTransactionMode) -> ConcurrentTransactionOptions {
    let options = match mode {
        crate::ConcurrentTransactionMode::Optimistic => ConcurrentTransactionOptions::optimistic(),
        crate::ConcurrentTransactionMode::Pessimistic => {
            ConcurrentTransactionOptions::pessimistic(Duration::ZERO)
        }
    };
    options.with_isolation(TransactionIsolation::Serializable)
}

fn fixture() -> Database {
    let mut db = Database::new();
    db.query_sql("CREATE TABLE inputs (id BIGINT PRIMARY KEY, team BIGINT, payload BIGINT)")
        .unwrap();
    db.query_sql("CREATE INDEX inputs_team ON inputs (team)")
        .unwrap();
    db.query_sql("CREATE TABLE outputs (id BIGINT PRIMARY KEY, value BIGINT)")
        .unwrap();
    db.query_sql("INSERT INTO inputs (id, team, payload) VALUES (1, 7, 10), (2, 8, 20)")
        .unwrap();
    db.query("CREATE (:Input {key: 'one', value: 10})").unwrap();
    db
}

fn update_input(db: &ConcurrentDatabase, sql: &str, parameters: &[Value]) {
    let mut tx = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    tx.query_sql_with_params(sql, parameters).unwrap();
    tx.commit().unwrap();
}

fn stage_output(tx: &mut crate::ConcurrentDatabaseTransaction) {
    tx.query_sql_with_params(
        "INSERT INTO outputs (id, value) VALUES ($1, $2)",
        &[Value::Int(1), Value::Int(99)],
    )
    .unwrap();
}

#[test]
fn dependency_read_transaction_owns_complete_sql_and_cypher_capture() {
    let mut db = fixture();
    let mut tx = db
        .begin_transaction_with_isolation(
            TransactionIsolation::Serializable,
            TransactionReadDependencyLimits::default(),
        )
        .unwrap();
    let epoch = tx.read_dependency_usage().unwrap().source_epoch;
    let rows = tx
        .query_sql_with_params("SELECT payload FROM inputs WHERE id = $1", &[Value::Int(1)])
        .unwrap()
        .rows;
    assert_eq!(rows[0]["payload"], Value::Int(10));
    let sql = tx.read_dependency_usage().unwrap();
    assert_eq!(sql.entries, 2, "schema and one exact relational key");
    assert_eq!(sql.exact_relational_rows, 1);
    tx.query_with_params(
        "MATCH (n:Input) WHERE n.key = $key RETURN n.value",
        &BTreeMap::from([("key".into(), Value::String("one".into()))]),
    )
    .unwrap();
    let graph = tx.read_dependency_usage().unwrap();
    assert_eq!(graph.source_epoch, epoch);
    assert!(graph.entries > sql.entries);
    assert!(!graph.refused);
    tx.query_sql("INSERT INTO outputs (id, value) VALUES (1, 10)")
        .unwrap();
    assert_eq!(
        tx.query_sql("SELECT value FROM outputs WHERE id = 1")
            .unwrap()
            .rows[0]["value"],
        Value::Int(10)
    );
    tx.commit().unwrap();
    assert_eq!(
        db.query_sql("SELECT value FROM outputs")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn serializable_write_skew_matches_a_serial_reference_and_preserves_default_si() {
    for isolation in [
        TransactionIsolation::SnapshotIsolation,
        TransactionIsolation::Serializable,
    ] {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE shifts (id BIGINT PRIMARY KEY, active BOOLEAN)")
            .unwrap();
        database
            .query_sql("INSERT INTO shifts (id, active) VALUES (1, true), (2, true)")
            .unwrap();
        let db = ConcurrentDatabase::new(database);
        let config = ConcurrentTransactionOptions::optimistic().with_isolation(isolation);
        let mut first = db.begin_transaction(config).unwrap();
        let mut second = db.begin_transaction(config).unwrap();
        for tx in [&mut first, &mut second] {
            assert_eq!(
                tx.query_sql("SELECT id FROM shifts WHERE active = true")
                    .unwrap()
                    .rows
                    .len(),
                2
            );
        }
        first
            .query_sql_with_params(
                "UPDATE shifts SET active = false WHERE id = $1",
                &[Value::Int(1)],
            )
            .unwrap();
        second
            .query_sql_with_params(
                "UPDATE shifts SET active = false WHERE id = $1",
                &[Value::Int(2)],
            )
            .unwrap();
        first.commit().unwrap();
        let epoch = db.commit_epoch().unwrap();
        let result = second.commit();
        if isolation == TransactionIsolation::Serializable {
            assert!(result.unwrap_err().is_retryable_transaction_conflict());
            assert_eq!(db.commit_epoch().unwrap(), epoch);
            // Either serial order disables one row; the second transaction's
            // read-check fails once it observes only one active row.
            assert_eq!(
                db.query_sql("SELECT id FROM shifts WHERE active = true")
                    .unwrap()
                    .rows
                    .len(),
                1
            );
        } else {
            result.unwrap();
            assert!(db
                .query_sql("SELECT id FROM shifts WHERE active = true")
                .unwrap()
                .rows
                .is_empty());
        }
    }
}

#[test]
fn serializable_empty_prefix_predicate_payload_aba_and_schema_changes_conflict() {
    for (read, parameters, writes) in [
        (
            "SELECT payload FROM inputs WHERE id = $1",
            vec![Value::Int(3)],
            vec!["INSERT INTO inputs (id, team, payload) VALUES (3, 9, 30)"],
        ),
        (
            "SELECT payload FROM inputs WHERE team = $1",
            vec![Value::Int(9)],
            vec!["INSERT INTO inputs (id, team, payload) VALUES (3, 9, 30)"],
        ),
        (
            "SELECT payload FROM inputs WHERE team = $1",
            vec![Value::Int(9)],
            vec!["UPDATE inputs SET team = 9 WHERE id = 1"],
        ),
        (
            "SELECT payload FROM inputs WHERE team = $1",
            vec![Value::Int(7)],
            vec!["UPDATE inputs SET team = 9 WHERE id = 1"],
        ),
        (
            "SELECT payload FROM inputs WHERE id = $1",
            vec![Value::Int(1)],
            vec!["UPDATE inputs SET payload = 11 WHERE id = 1"],
        ),
        (
            "SELECT payload FROM inputs WHERE id = $1",
            vec![Value::Int(1)],
            vec![
                "DELETE FROM inputs WHERE id = 1",
                "INSERT INTO inputs (id, team, payload) VALUES (1, 7, 10)",
            ],
        ),
        (
            "SELECT payload FROM inputs WHERE id = $1",
            vec![Value::Int(1)],
            vec!["CREATE TABLE new_schema (id BIGINT PRIMARY KEY)"],
        ),
    ] {
        let db = ConcurrentDatabase::new(fixture());
        let mut reader = db
            .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
            .unwrap();
        reader.query_sql_with_params(read, &parameters).unwrap();
        stage_output(&mut reader);
        for write in writes {
            update_input(&db, write, &[]);
        }
        let epoch = db.commit_epoch().unwrap();
        assert!(
            reader
                .commit()
                .unwrap_err()
                .is_retryable_transaction_conflict(),
            "{read}"
        );
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert!(db
            .query_sql("SELECT id FROM outputs")
            .unwrap()
            .rows
            .is_empty());
    }
}

#[test]
fn serializable_disjoint_exact_and_table_scopes_allow_progress() {
    for read in [
        "SELECT payload FROM inputs WHERE id = 1",
        "SELECT payload FROM inputs WHERE team = 7",
    ] {
        let db = ConcurrentDatabase::new(fixture());
        let mut reader = db
            .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
            .unwrap();
        reader.query_sql(read).unwrap();
        stage_output(&mut reader);
        update_input(&db, "UPDATE inputs SET payload = 21 WHERE id = 2", &[]);
        reader.commit().unwrap();
        assert_eq!(
            db.query_sql("SELECT value FROM outputs").unwrap().rows[0]["value"],
            Value::Int(99)
        );
    }
}

#[test]
fn serializable_earlier_statement_and_join_parent_changes_remain_dependencies() {
    for read in [
        "SELECT payload FROM inputs WHERE id = 1",
        "SELECT c.payload FROM inputs AS c INNER JOIN inputs AS p ON c.id = p.id WHERE c.id = 1",
    ] {
        let db = ConcurrentDatabase::new(fixture());
        let mut tx = db
            .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
            .unwrap();
        tx.query_sql(read).unwrap();
        tx.query_sql("SELECT payload FROM inputs WHERE id = 2")
            .unwrap();
        stage_output(&mut tx);
        update_input(&db, "UPDATE inputs SET payload = 12 WHERE id = 1", &[]);
        assert!(tx.commit().unwrap_err().is_retryable_transaction_conflict());
    }
}

#[test]
fn serializable_mixed_graph_sql_reads_and_empty_graph_plans_conflict() {
    for read in [
        "MATCH (n:Input) WHERE n.key = 'one' RETURN n.value",
        "MATCH (n:Input) WHERE n.key IN [] RETURN n.value",
    ] {
        let db = ConcurrentDatabase::new(fixture());
        let mut tx = db
            .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
            .unwrap();
        tx.query(read).unwrap();
        stage_output(&mut tx);
        db.query("MATCH (n:Input {key: 'one'}) SET n.value = 11")
            .unwrap();
        assert!(tx.commit().unwrap_err().is_retryable_transaction_conflict());
    }
}

#[test]
fn dependency_read_refusal_is_sticky_after_ignored_errors_and_statement_restore() {
    for bad in [
        "SELECT now()",
        "SELECT FROM",
        "INSERT INTO inputs (id, team, payload) VALUES (1, 7, 20)",
    ] {
        let db = ConcurrentDatabase::new(fixture());
        let mut tx = db
            .begin_transaction(options(crate::ConcurrentTransactionMode::Pessimistic))
            .unwrap();
        assert!(tx.query_sql(bad).is_err());
        assert!(tx.read_dependency_usage().unwrap().refused);
        // A rejected INSERT retains its statement locks under the existing
        // pessimistic contract. Read/parse failures own no such locks.
        if !bad.starts_with("INSERT") {
            update_input(&db, "UPDATE inputs SET payload = 12 WHERE id = 2", &[]);
        }
        assert!(matches!(
            tx.query_sql("SELECT id FROM inputs"),
            Err(HawDBError::TransactionReadDependencyRefused { .. })
        ));
        let epoch = db.commit_epoch().unwrap();
        let err = tx.commit().unwrap_err();
        assert!(matches!(
            err,
            HawDBError::TransactionReadDependencyRefused { .. }
        ));
        assert!(!err.is_retryable_transaction_conflict());
        assert_eq!(db.commit_epoch().unwrap(), epoch);
    }
}

#[test]
fn dependency_read_capture_overflow_preserves_old_evidence_and_never_commits() {
    let db = ConcurrentDatabase::new(fixture());
    let mut config = options(crate::ConcurrentTransactionMode::Optimistic);
    config.read_dependency_limits.entries = NonZeroUsize::new(1).unwrap();
    let mut tx = db.begin_transaction(config).unwrap();
    let before = tx.read_dependency_usage().unwrap();
    assert!(matches!(
        tx.query_sql("SELECT id FROM inputs WHERE id = 1"),
        Err(HawDBError::TransactionReadDependencyRefused { .. })
    ));
    let after = tx.read_dependency_usage().unwrap();
    assert_eq!(before.entries, after.entries);
    assert_eq!(before.estimated_bytes, after.estimated_bytes);
    assert!(after.refused);
    assert!(matches!(
        tx.commit(),
        Err(HawDBError::TransactionReadDependencyRefused { .. })
    ));
    config.read_dependency_limits.bytes = NonZeroUsize::new(1).unwrap();
    assert!(matches!(
        db.begin_transaction(config),
        Err(HawDBError::TransactionReadDependencyRefused { .. })
    ));
    assert_eq!(
        db.query_sql("SELECT id FROM outputs").unwrap().rows.len(),
        0
    );
}

#[test]
fn serializable_pessimistic_and_conflict_noop_rebase_cannot_bypass_reads() {
    for mode in [
        crate::ConcurrentTransactionMode::Optimistic,
        crate::ConcurrentTransactionMode::Pessimistic,
    ] {
        let db = ConcurrentDatabase::new(fixture());
        let mut tx = db.begin_transaction(options(mode)).unwrap();
        tx.query_sql("SELECT payload FROM inputs WHERE id = 1")
            .unwrap();
        update_input(&db, "UPDATE inputs SET payload = 12 WHERE id = 1", &[]);
        tx.query_sql("INSERT INTO outputs (id, value) VALUES (1, 99) ON CONFLICT (id) DO NOTHING")
            .unwrap();
        assert!(tx.commit().unwrap_err().is_retryable_transaction_conflict());
        assert!(db
            .query_sql("SELECT id FROM outputs")
            .unwrap()
            .rows
            .is_empty());
    }

    // A SELECT would disqualify optimistic conflict-noop rebase. Keep this
    // transaction mutation-only so the actual rebase eligibility path runs.
    for isolation in [
        TransactionIsolation::SnapshotIsolation,
        TransactionIsolation::Serializable,
    ] {
        let db = ConcurrentDatabase::new(fixture());
        let mut tx = db
            .begin_transaction(ConcurrentTransactionOptions::optimistic().with_isolation(isolation))
            .unwrap();
        tx.query_sql("INSERT INTO outputs (id, value) VALUES (1, 99) ON CONFLICT (id) DO NOTHING")
            .unwrap();
        update_input(&db, "INSERT INTO outputs (id, value) VALUES (2, 88)", &[]);
        match isolation {
            TransactionIsolation::SnapshotIsolation => {
                tx.commit().unwrap();
                assert_eq!(
                    db.query_sql("SELECT id FROM outputs").unwrap().rows.len(),
                    2
                );
            }
            TransactionIsolation::Serializable => {
                let error = tx.commit().unwrap_err();
                assert!(error.is_retryable_transaction_conflict());
                assert!(error.to_string().contains("read dependency"));
                assert_eq!(
                    db.query_sql("SELECT id FROM outputs").unwrap().rows[0]["id"],
                    Value::Int(2)
                );
            }
        }
    }
}

#[test]
fn serializable_conflict_never_appends_wal_and_success_reopens_whole() {
    for (group, durability, mode) in [false, true].into_iter().flat_map(|group| {
        [
            crate::DurabilityPolicy::SyncOnEveryWrite,
            crate::DurabilityPolicy::SyncOnCheckpoint,
        ]
        .into_iter()
        .flat_map(move |durability| {
            [
                crate::ConcurrentTransactionMode::Optimistic,
                crate::ConcurrentTransactionMode::Pessimistic,
            ]
            .into_iter()
            .map(move |mode| (group, durability, mode))
        })
    }) {
        let path = super::super::unique_test_dir("serializable_wal");
        let mut database = Database::open_with_durability(&path, durability).unwrap();
        database
            .query_sql("CREATE TABLE inputs (id BIGINT PRIMARY KEY, payload BIGINT)")
            .unwrap();
        database
            .query_sql("CREATE TABLE outputs (id BIGINT PRIMARY KEY, value BIGINT)")
            .unwrap();
        database
            .query_sql("INSERT INTO inputs (id, payload) VALUES (1, 10)")
            .unwrap();
        let db = if group {
            ConcurrentDatabase::new_with_wal_group_commit(
                database,
                WalGroupCommitConfig::benchmark_candidate(
                    NonZeroUsize::new(2).unwrap(),
                    NonZeroU64::new(1024 * 1024).unwrap(),
                    Duration::from_millis(1),
                )
                .unwrap(),
            )
        } else {
            ConcurrentDatabase::new(database)
        };
        let mut tx = db.begin_transaction(options(mode)).unwrap();
        tx.query_sql("SELECT payload FROM inputs WHERE id = 1")
            .unwrap();
        update_input(&db, "UPDATE inputs SET payload = 12 WHERE id = 1", &[]);
        // A staged pessimistic writer already excludes optimistic publishers.
        // Change the read input before acquiring this writer's output locks.
        stage_output(&mut tx);
        let wal_path = super::super::active_wal_path(&path);
        let before = std::fs::read(&wal_path).unwrap();
        let selector = super::active_runtime_manifest_path(&path);
        let selector_before = std::fs::read(&selector).unwrap();
        let epoch = db.commit_epoch().unwrap();
        assert!(tx.commit().unwrap_err().is_retryable_transaction_conflict());
        assert_eq!(std::fs::read(&wal_path).unwrap(), before);
        assert_eq!(std::fs::read(&selector).unwrap(), selector_before);
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        drop(db);
        let mut reopened = Database::open(&path).unwrap();
        assert_eq!(
            reopened
                .query_sql("SELECT payload FROM inputs WHERE id = 1")
                .unwrap()
                .rows[0]["payload"],
            Value::Int(12)
        );
        assert!(reopened
            .query_sql("SELECT id FROM outputs")
            .unwrap()
            .rows
            .is_empty());
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn dependency_read_null_duplicate_and_comparison_results_match_ordinary_execution() {
    let mut database = fixture();
    database
        .query_sql("INSERT INTO inputs (id, team, payload) VALUES (3, 7, 30), (4, NULL, 40)")
        .unwrap();
    for (sql, parameters, rows, prefixes) in [
        (
            "SELECT payload FROM inputs WHERE team = $1 ORDER BY payload",
            vec![Value::Int(7)],
            2,
            1,
        ),
        (
            "SELECT payload FROM inputs WHERE team = $1",
            vec![Value::Null],
            0,
            0,
        ),
        (
            "SELECT payload FROM inputs WHERE team IS NULL",
            vec![],
            1,
            0,
        ),
    ] {
        let expected = database
            .query_sql_with_params(sql, &parameters)
            .unwrap()
            .rows;
        let mut tx = database
            .begin_transaction_with_isolation(
                TransactionIsolation::Serializable,
                TransactionReadDependencyLimits::default(),
            )
            .unwrap();
        let actual = tx.query_sql_with_params(sql, &parameters).unwrap().rows;
        assert_eq!(actual, expected, "{sql}");
        assert_eq!(actual.len(), rows);
        assert_eq!(
            tx.read_dependency_usage()
                .unwrap()
                .relational_index_prefixes,
            prefixes
        );
        tx.rollback();
    }
}

#[test]
fn serializable_read_only_retains_its_snapshot_and_returning_rollback_stays_private() {
    let db = ConcurrentDatabase::new(fixture());
    let mut reader = db
        .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
        .unwrap();
    let before = reader
        .query_sql("SELECT payload FROM inputs WHERE id = 1")
        .unwrap()
        .rows;
    update_input(&db, "UPDATE inputs SET payload = 12 WHERE id = 1", &[]);
    assert_eq!(
        reader
            .query_sql("SELECT payload FROM inputs WHERE id = 1")
            .unwrap()
            .rows,
        before
    );
    reader.rollback();
    let mut writer = db
        .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
        .unwrap();
    let result = writer
        .query_sql_with_result("INSERT INTO outputs (id, value) VALUES (1, 99) RETURNING id, value")
        .unwrap();
    assert_eq!(result.output.rows[0]["value"], Value::Int(99));
    writer.rollback();
    assert!(db
        .query_sql("SELECT id FROM outputs")
        .unwrap()
        .rows
        .is_empty());
    let mut writer = db
        .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
        .unwrap();
    writer
        .query_sql_with_result("INSERT INTO outputs (id, value) VALUES (1, 99) RETURNING id, value")
        .unwrap();
    let committed = writer.commit_with_result().unwrap();
    assert_eq!(committed.output.rows[0]["value"], Value::Int(99));
}

#[test]
fn serializable_cancelled_capture_cannot_escape_and_refunds_host_permit() {
    let db = ConcurrentDatabase::new(fixture());
    let governor = super::admission::governor();
    let context = crate::RuntimeTaskContext::default();
    let mut tx = db
        .begin_admitted_transaction(
            options(crate::ConcurrentTransactionMode::Optimistic),
            governor.try_admit(super::admission::request()).unwrap(),
            context.clone(),
        )
        .unwrap();
    tx.query_sql("SELECT payload FROM inputs WHERE id = 1")
        .unwrap();
    stage_output(&mut tx);
    let epoch = db.commit_epoch().unwrap();
    context.cancellation().cancel();
    assert!(tx.query_sql("SELECT id FROM inputs").is_err());
    assert!(tx.read_dependency_usage().unwrap().refused);
    assert!(tx.commit().is_err());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert_eq!(governor.snapshot().active_foreground_tasks, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert!(db
        .query_sql("SELECT id FROM outputs")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn dependency_read_unversioned_and_oversized_inputs_refuse_before_private_mutation() {
    for graph in [true, false] {
        let db = ConcurrentDatabase::new(fixture());
        let mut tx = db
            .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
            .unwrap();
        let before = tx.read_dependency_usage().unwrap();
        let result = if graph {
            tx.query_with_params(
                "MATCH (n:Input) RETURN n.value",
                &BTreeMap::from([("unused".into(), Value::List(vec![Value::Int(1)]))]),
            )
        } else {
            tx.query_sql_with_params(
                "SELECT id FROM inputs WHERE id = $1",
                &[Value::String("x".repeat(65537))],
            )
        };
        assert!(matches!(
            result,
            Err(HawDBError::TransactionReadDependencyRefused { .. })
        ));
        let after = tx.read_dependency_usage().unwrap();
        assert_eq!(before.entries, after.entries);
        assert_eq!(before.estimated_bytes, after.estimated_bytes);
        assert!(after.refused);
        assert!(matches!(
            tx.commit(),
            Err(HawDBError::TransactionReadDependencyRefused { .. })
        ));
    }
}

#[test]
fn dependency_read_incompatible_types_and_volatile_values_never_seal() {
    let mut db = fixture();
    assert!(matches!(
        db.query_sql_with_params(
            "SELECT id FROM inputs WHERE id = $1",
            &[Value::String("1".into())]
        ),
        Err(HawDBError::Semantic(_))
    ));
    let mut tx = db
        .begin_transaction_with_isolation(
            TransactionIsolation::Serializable,
            TransactionReadDependencyLimits::default(),
        )
        .unwrap();
    assert!(tx
        .query_sql_with_params(
            "SELECT id FROM inputs WHERE id = $1",
            &[Value::String("1".into())]
        )
        .is_err());
    assert!(tx.read_dependency_usage().unwrap().refused);
    assert!(matches!(
        tx.commit(),
        Err(HawDBError::TransactionReadDependencyRefused { .. })
    ));
    db.query_sql("CREATE TABLE generated (id UUID PRIMARY KEY DEFAULT uuidv7(), payload BIGINT)")
        .unwrap();
    let mut tx = db
        .begin_transaction_with_isolation(
            TransactionIsolation::Serializable,
            TransactionReadDependencyLimits::default(),
        )
        .unwrap();
    assert!(matches!(
        tx.query_sql("INSERT INTO generated (payload) VALUES (1)"),
        Err(HawDBError::TransactionReadDependencyRefused { .. })
    ));
    assert!(matches!(
        tx.commit(),
        Err(HawDBError::TransactionReadDependencyRefused { .. })
    ));
    let mut tx = db
        .begin_transaction_with_isolation(
            TransactionIsolation::Serializable,
            TransactionReadDependencyLimits::default(),
        )
        .unwrap();
    assert!(matches!(
        tx.query("CREATE (:Volatile {at: CURRENT_TIMESTAMP()})"),
        Err(HawDBError::TransactionReadDependencyRefused { .. })
    ));
    assert!(matches!(
        tx.commit(),
        Err(HawDBError::TransactionReadDependencyRefused { .. })
    ));
    assert!(db
        .query("MATCH (n:Volatile) RETURN n")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn dependency_read_mixed_numeric_values_keep_exact_results_and_capture_boundaries() {
    let mut db = fixture();
    db.query_sql("INSERT INTO inputs (id, team, payload) VALUES (9007199254740993, 9, 30)")
        .unwrap();
    for (value, expected) in [
        (Value::Float(1.0), vec![Value::Int(1)]),
        (Value::Float(1.25), vec![]),
        (Value::Float(9_007_199_254_740_992.0), vec![]),
    ] {
        let rows = db
            .query_sql_with_params(
                "SELECT id FROM inputs WHERE id = $1",
                std::slice::from_ref(&value),
            )
            .unwrap()
            .rows;
        assert_eq!(
            rows.iter().map(|row| row["id"].clone()).collect::<Vec<_>>(),
            expected
        );
        let mut tx = db
            .begin_transaction_with_isolation(
                TransactionIsolation::Serializable,
                TransactionReadDependencyLimits::default(),
            )
            .unwrap();
        // Valid numeric execution does not authorize a cross-domain point
        // stamp. The existing capture protocol must continue to fail closed.
        assert!(matches!(
            tx.query_sql_with_params("SELECT id FROM inputs WHERE id = $1", &[value]),
            Err(HawDBError::TransactionReadDependencyRefused { .. })
        ));
        assert!(tx.read_dependency_usage().unwrap().refused);
        assert!(matches!(
            tx.commit(),
            Err(HawDBError::TransactionReadDependencyRefused { .. })
        ));
    }
    let mut tx = db
        .begin_transaction_with_isolation(
            TransactionIsolation::Serializable,
            TransactionReadDependencyLimits::default(),
        )
        .unwrap();
    let rows = tx
        .query_sql_with_params(
            "SELECT id FROM inputs WHERE id = $1",
            &[Value::Decimal("9007199254740993.000".parse().unwrap())],
        )
        .unwrap()
        .rows;
    assert_eq!(rows[0]["id"], Value::Int(9_007_199_254_740_993));
    assert_eq!(tx.read_dependency_usage().unwrap().exact_relational_rows, 1);
    tx.commit().unwrap();
}

#[test]
fn dependency_read_nullable_unique_suffix_uses_complete_domain_and_text_stays_case_sensitive() {
    let mut db = fixture();
    db.query_sql("CREATE TABLE nullable_keys (id BIGINT PRIMARY KEY, a BIGINT NOT NULL, b BIGINT, payload BIGINT)").unwrap();
    db.query_sql("CREATE UNIQUE INDEX nullable_ab ON nullable_keys (a, b)")
        .unwrap();
    db.query_sql(
        "INSERT INTO nullable_keys (id, a, b, payload) VALUES (1, 7, NULL, 10), (2, 7, NULL, 20)",
    )
    .unwrap();
    db.query_sql("CREATE TABLE words (id BIGINT PRIMARY KEY, word TEXT)")
        .unwrap();
    db.query_sql("CREATE INDEX words_word ON words (word)")
        .unwrap();
    db.query_sql("INSERT INTO words (id, word) VALUES (1, 'Case'), (2, 'case')")
        .unwrap();
    for word in ["Case", "case"] {
        let sql = "SELECT id FROM words WHERE word = $1";
        let params = [Value::String(word.into())];
        let expected = db.query_sql_with_params(sql, &params).unwrap().rows;
        let mut tx = db
            .begin_transaction_with_isolation(
                TransactionIsolation::Serializable,
                TransactionReadDependencyLimits::default(),
            )
            .unwrap();
        assert_eq!(
            tx.query_sql_with_params(sql, &params).unwrap().rows,
            expected
        );
        assert_eq!(expected.len(), 1);
        tx.rollback();
    }
    let db = ConcurrentDatabase::new(db);
    let mut tx = db
        .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
        .unwrap();
    assert_eq!(
        tx.query_sql_with_params(
            "SELECT payload FROM nullable_keys WHERE a = $1",
            &[Value::Int(7)]
        )
        .unwrap()
        .rows
        .len(),
        2
    );
    let usage = tx.read_dependency_usage().unwrap();
    assert_eq!(usage.relational_index_prefixes, 0);
    assert_eq!(usage.relational_table_domains, 1);
    stage_output(&mut tx);
    update_input(
        &db,
        "UPDATE nullable_keys SET payload = 21 WHERE id = 2",
        &[],
    );
    assert!(tx.commit().unwrap_err().is_retryable_transaction_conflict());
}

#[test]
fn dependency_read_graph_statement_restore_cannot_erase_failure() {
    let mut database = fixture();
    database
        .query("CREATE CONSTRAINT ON :Input(key) ASSERT UNIQUE")
        .unwrap();
    let db = ConcurrentDatabase::new(database);
    let mut tx = db
        .begin_transaction(options(crate::ConcurrentTransactionMode::Pessimistic))
        .unwrap();
    assert!(tx.query("CREATE (:Input {key: 'one', value: 99})").is_err());
    assert!(tx.read_dependency_usage().unwrap().refused);
    assert!(matches!(
        tx.commit(),
        Err(HawDBError::TransactionReadDependencyRefused { .. })
    ));
    let rows = db.query("MATCH (n:Input) RETURN n.value").unwrap().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["n.value"], Value::Int(10));
}

#[test]
fn serializable_cancellation_after_durable_group_flush_preserves_success() {
    let path = super::super::unique_test_dir("serializable_after_sync_cancel");
    let mut database = Database::open(&path).unwrap();
    database
        .query_sql("CREATE TABLE outputs (id BIGINT PRIMARY KEY, value BIGINT)")
        .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(
        database,
        WalGroupCommitConfig::benchmark_candidate(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            Duration::ZERO,
        )
        .unwrap(),
    );
    let governor = super::admission::governor();
    let context = crate::RuntimeTaskContext::default();
    let mut tx = db
        .begin_admitted_transaction(
            options(crate::ConcurrentTransactionMode::Optimistic),
            governor.try_admit(super::admission::request()).unwrap(),
            context.clone(),
        )
        .unwrap();
    stage_output(&mut tx);
    db.cancel_after_next_group_flush(context.cancellation().clone())
        .unwrap();
    tx.commit().unwrap();
    assert!(context.cancellation().is_cancelled());
    assert_eq!(governor.snapshot().active_foreground_tasks, 0);
    assert_eq!(
        db.query_sql("SELECT value FROM outputs").unwrap().rows[0]["value"],
        Value::Int(99)
    );
    drop(db);
    let mut reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .query_sql("SELECT value FROM outputs")
            .unwrap()
            .rows[0]["value"],
        Value::Int(99)
    );
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn serializable_queued_cancellation_prevents_wal_and_publication() {
    let path = super::super::unique_test_dir("serializable_queued_cancel");
    let mut database = Database::open(&path).unwrap();
    database
        .query_sql("CREATE TABLE outputs (id BIGINT PRIMARY KEY, value BIGINT)")
        .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(
        database,
        WalGroupCommitConfig::benchmark_candidate(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            Duration::ZERO,
        )
        .unwrap(),
    );
    let governor = super::admission::governor();
    let context = crate::RuntimeTaskContext::default();
    let mut tx = db
        .begin_admitted_transaction(
            options(crate::ConcurrentTransactionMode::Optimistic),
            governor.try_admit(super::admission::request()).unwrap(),
            context.clone(),
        )
        .unwrap();
    stage_output(&mut tx);
    let wal_path = super::super::active_wal_path(&path);
    let wal = std::fs::read(&wal_path).unwrap();
    let epoch = db.commit_epoch().unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    db.set_group_commit_enqueue_gate(Arc::clone(&gate)).unwrap();
    let worker = std::thread::spawn(move || tx.commit());
    super::crash_recovery::wait_submitted(&db, 1);
    context.cancellation().cancel();
    release_autocommit_reads(&gate);
    assert!(worker
        .join()
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("cancelled"));
    assert_eq!(governor.snapshot().active_foreground_tasks, 0);
    assert_eq!(std::fs::read(wal_path).unwrap(), wal);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert!(db
        .query_sql("SELECT id FROM outputs")
        .unwrap()
        .rows
        .is_empty());
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn serializable_group_sync_failure_poisons_access_and_reopens_complete_transactions() {
    let path = super::super::unique_test_dir("serializable_sync_failure");
    let mut database = Database::open(&path).unwrap();
    for table in ["first", "second"] {
        database
            .query_sql(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, value BIGINT, mirror BIGINT)"
            ))
            .unwrap();
        database
            .query_sql(&format!(
                "INSERT INTO {table} (id, value, mirror) VALUES (1, 0, 0)"
            ))
            .unwrap();
    }
    let db = ConcurrentDatabase::new_with_wal_group_commit(
        database,
        WalGroupCommitConfig::benchmark_candidate(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            Duration::ZERO,
        )
        .unwrap(),
    );
    let transactions = ["first", "second"].map(|table| {
        let mut tx = db
            .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
            .unwrap();
        tx.query_sql(&format!("SELECT value FROM {table} WHERE id = 1"))
            .unwrap();
        tx.query_sql(&format!(
            "UPDATE {table} SET value = 1, mirror = 1 WHERE id = 1"
        ))
        .unwrap();
        tx
    });
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    db.set_group_commit_enqueue_gate(Arc::clone(&gate)).unwrap();
    let handles = transactions
        .into_iter()
        .enumerate()
        .map(|(index, tx)| {
            let worker = std::thread::spawn(move || {
                crate::store::set_wal_group_sync_failpoint(true);
                let result = tx.commit();
                crate::store::set_wal_group_sync_failpoint(false);
                result
            });
            super::crash_recovery::wait_submitted(&db, index as u64 + 1);
            worker
        })
        .collect::<Vec<_>>();
    release_autocommit_reads(&gate);
    for worker in handles {
        assert!(worker
            .join()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("WAL group durability barrier failed"));
    }
    assert!(db.query_sql("SELECT id FROM first").is_err());
    drop(db);
    let mut reopened = Database::open(&path).unwrap();
    let mut recovered = Vec::new();
    for table in ["first", "second"] {
        let rows = reopened
            .query_sql(&format!("SELECT value, mirror FROM {table}"))
            .unwrap()
            .rows;
        assert_eq!(
            rows[0]["value"], rows[0]["mirror"],
            "no partial row transaction"
        );
        recovered.push(rows[0]["value"].clone());
    }
    assert!(
        matches!(
            recovered.as_slice(),
            [Value::Int(0), Value::Int(0)]
                | [Value::Int(1), Value::Int(0)]
                | [Value::Int(1), Value::Int(1)]
        ),
        "only the known queue prefix may recover: {recovered:?}"
    );
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

fn interleavings() -> Vec<Vec<usize>> {
    fn visit(steps: [usize; 2], schedule: &mut Vec<usize>, result: &mut Vec<Vec<usize>>) {
        if steps == [3, 3] {
            result.push(schedule.clone());
            return;
        }
        for actor in 0..2 {
            if steps[actor] == 3 {
                continue;
            }
            let mut next = steps;
            next[actor] += 1;
            schedule.push(actor);
            visit(next, schedule, result);
            schedule.pop();
        }
    }
    let mut result = Vec::new();
    visit([0, 0], &mut Vec::new(), &mut result);
    result
}

#[test]
fn serializable_all_twenty_read_write_commit_interleavings_match_serial_reference() {
    for schedule in interleavings() {
        let make = || {
            let mut db = Database::new();
            db.query_sql("CREATE TABLE shifts (id BIGINT PRIMARY KEY, active BOOLEAN)")
                .unwrap();
            db.query_sql("INSERT INTO shifts (id, active) VALUES (1, true), (2, true)")
                .unwrap();
            db
        };
        let db = ConcurrentDatabase::new(make());
        let mut transactions = [
            Some(
                db.begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
                    .unwrap(),
            ),
            Some(
                db.begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
                    .unwrap(),
            ),
        ];
        let mut steps = [0; 2];
        let mut committed = Vec::new();
        for actor in &schedule {
            match steps[*actor] {
                0 => assert_eq!(
                    transactions[*actor]
                        .as_mut()
                        .unwrap()
                        .query_sql("SELECT id FROM shifts WHERE active = true")
                        .unwrap()
                        .rows
                        .len(),
                    2
                ),
                1 => {
                    transactions[*actor]
                        .as_mut()
                        .unwrap()
                        .query_sql_with_params(
                            "UPDATE shifts SET active = false WHERE id = $1",
                            &[Value::Int(*actor as i64 + 1)],
                        )
                        .unwrap();
                }
                _ => match transactions[*actor].take().unwrap().commit() {
                    Ok(_) => committed.push(*actor),
                    Err(error) => assert!(
                        error.is_retryable_transaction_conflict(),
                        "{schedule:?}: {error}"
                    ),
                },
            }
            steps[*actor] += 1;
        }
        assert_eq!(committed.len(), 1, "{schedule:?}");
        let mut reference = make();
        for actor in committed {
            let rows = reference
                .query_sql("SELECT id FROM shifts WHERE active = true")
                .unwrap()
                .rows;
            if rows.len() > 1 {
                reference
                    .query_sql_with_params(
                        "UPDATE shifts SET active = false WHERE id = $1",
                        &[Value::Int(actor as i64 + 1)],
                    )
                    .unwrap();
            }
        }
        assert_eq!(
            db.query_sql("SELECT id, active FROM shifts ORDER BY id")
                .unwrap()
                .rows,
            reference
                .query_sql("SELECT id, active FROM shifts ORDER BY id")
                .unwrap()
                .rows,
            "{schedule:?}"
        );
    }
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn serializable_same_workload_latency_abort_and_memory_measurement() {
    use std::time::Instant;
    const CHILD: &str = "HAWDB_TEST_ISOLATION_MEASUREMENT";
    if std::env::var_os(CHILD).is_none() {
        for isolation in ["si", "serializable"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "api::tests::concurrent_transactions::serializable::serializable_same_workload_latency_abort_and_memory_measurement", "--nocapture", "--test-threads=1"])
                .env(CHILD, isolation)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            eprint!("{}", String::from_utf8_lossy(&output.stderr));
        }
        return;
    }
    let isolation = if std::env::var(CHILD).unwrap() == "si" {
        TransactionIsolation::SnapshotIsolation
    } else {
        TransactionIsolation::Serializable
    };
    {
        let db = ConcurrentDatabase::new(fixture());
        let before = hawdb_qos::ProcessMemorySnapshot::capture().unwrap();
        let mut commit_micros = Vec::new();
        let mut aborts = 0;
        let mut maximum_entries = 0;
        let mut maximum_payload = 0;
        let mut retained_rss = before.resident_bytes;
        for round in 0..32 {
            let mut transactions = Vec::new();
            for writer in 1..=2 {
                let mut tx = db
                    .begin_transaction(
                        ConcurrentTransactionOptions::optimistic().with_isolation(isolation),
                    )
                    .unwrap();
                for key in 0..64 {
                    tx.query_sql_with_params(
                        "SELECT payload FROM inputs WHERE id = $1",
                        &[Value::Int(key)],
                    )
                    .unwrap();
                }
                tx.query_sql_with_params(
                    "UPDATE inputs SET payload = $1 WHERE id = $2",
                    &[Value::Int(round), Value::Int(writer)],
                )
                .unwrap();
                if let Some(usage) = tx.read_dependency_usage() {
                    maximum_entries = maximum_entries.max(usage.entries);
                    maximum_payload = maximum_payload.max(usage.estimated_bytes);
                }
                transactions.push(tx);
            }
            retained_rss = retained_rss.max(
                hawdb_qos::ProcessMemorySnapshot::capture()
                    .unwrap()
                    .resident_bytes,
            );
            for tx in transactions {
                let start = Instant::now();
                let result = tx.commit();
                commit_micros.push(start.elapsed().as_micros());
                if let Err(error) = result {
                    assert!(error.is_retryable_transaction_conflict());
                    aborts += 1;
                }
            }
        }
        commit_micros.sort_unstable();
        assert_eq!(
            aborts,
            if isolation == TransactionIsolation::Serializable {
                32
            } else {
                0
            }
        );
        let after = hawdb_qos::ProcessMemorySnapshot::capture().unwrap();
        eprintln!("serializable_measurement isolation={isolation:?} attempts=64 rounds=32 point_reads_per_attempt=64 p50_commit_us={} p99_commit_us={} aborts={aborts} max_validation_entries={maximum_entries} max_estimated_capture_bytes={maximum_payload} rss_before={} rss_with_captures_max={retained_rss} rss_after={} process_peak_rss={}", commit_micros[32], commit_micros[63], before.resident_bytes, after.resident_bytes, after.peak_resident_bytes);
    }
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn serializable_process_crashes_recover_complete_schema_data_prefix() {
    const CHILD_PATH: &str = "HAWDB_TEST_SERIALIZABLE_CRASH_PATH";
    const CHILD_GROUP: &str = "HAWDB_TEST_SERIALIZABLE_CRASH_GROUP";
    const CHILD_RELAXED: &str = "HAWDB_TEST_SERIALIZABLE_CRASH_RELAXED";
    const SELF: &str = "api::tests::concurrent_transactions::serializable::serializable_process_crashes_recover_complete_schema_data_prefix";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let path = std::path::PathBuf::from(path);
        let relaxed = std::env::var(CHILD_RELAXED).unwrap() == "true";
        let durability = if relaxed {
            crate::DurabilityPolicy::SyncOnCheckpoint
        } else {
            crate::DurabilityPolicy::SyncOnEveryWrite
        };
        let database = Database::open_with_durability(&path, durability).unwrap();
        let db = if std::env::var(CHILD_GROUP).unwrap() == "true" {
            ConcurrentDatabase::new_with_wal_group_commit(
                database,
                WalGroupCommitConfig::benchmark_candidate(
                    NonZeroUsize::new(2).unwrap(),
                    NonZeroU64::new(1024 * 1024).unwrap(),
                    Duration::ZERO,
                )
                .unwrap(),
            )
        } else {
            ConcurrentDatabase::new(database)
        };
        let mut tx = db
            .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
            .unwrap();
        tx.query_sql("SELECT id FROM baseline WHERE id = 1")
            .unwrap();
        tx.query_sql("CREATE TABLE born (id BIGINT PRIMARY KEY, value BIGINT)")
            .unwrap();
        tx.query_sql("INSERT INTO born (id, value) VALUES (1, 73)")
            .unwrap();
        tx.query("CREATE (:SerialBorn {value: 73})").unwrap();
        tx.commit().unwrap();
        let evidence = std::fs::File::create(path.join("acknowledged")).unwrap();
        evidence.sync_all().unwrap();
        db.checkpoint().unwrap();
        panic!("configured process crash point was not reached");
    }
    for group in [false, true] {
        for relaxed in [false, true] {
            for (stage, minimum, maximum) in [
                ("before_wal_append", 0, 0),
                ("after_wal_append", 0, 1),
                ("after_wal_sync", 1, 1),
                ("during_checkpoint_publication", 1, 1),
            ] {
                // Relaxed commits need not execute the per-write sync hook.
                // Their explicit checkpoint still qualifies the stronger barrier.
                if relaxed && stage == "after_wal_sync" {
                    continue;
                }
                let path = super::super::unique_test_dir("serializable_process_crash");
                let baseline_epoch = {
                    let mut database = Database::open(&path).unwrap();
                    database
                        .query_sql("CREATE TABLE baseline (id BIGINT PRIMARY KEY)")
                        .unwrap();
                    database
                        .query_sql("INSERT INTO baseline (id) VALUES (1)")
                        .unwrap();
                    database.checkpoint().unwrap();
                    database.commit_epoch().unwrap()
                };
                let log = std::fs::File::create(path.join("child.log")).unwrap();
                let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", SELF, "--nocapture"])
                    .env(CHILD_PATH, &path)
                    .env(CHILD_GROUP, group.to_string())
                    .env(CHILD_RELAXED, relaxed.to_string())
                    .env("HAWDB_TEST_PROCESS_CRASH_POINT", stage)
                    .stdout(log.try_clone().unwrap())
                    .stderr(log)
                    .spawn()
                    .unwrap();
                let deadline = std::time::Instant::now() + Duration::from_secs(60);
                let status = loop {
                    if let Some(status) = child.try_wait().unwrap() {
                        break status;
                    }
                    if std::time::Instant::now() >= deadline {
                        child.kill().unwrap();
                        child.wait().unwrap();
                        panic!("process crash child timed out: {}", path.display());
                    }
                    std::thread::sleep(Duration::from_millis(10));
                };
                assert_eq!(
                    status.code(),
                    Some(86),
                    "{stage}/{group}/{relaxed}: {}",
                    std::fs::read_to_string(path.join("child.log")).unwrap()
                );
                let mut reopened = Database::open(&path).unwrap();
                let committed = reopened.commit_epoch().unwrap() - baseline_epoch;
                assert!(
                    (minimum..=maximum).contains(&committed),
                    "{stage}: {committed}"
                );
                let born = reopened.query_sql("SELECT value FROM born WHERE id = 1");
                let graph = reopened
                    .query("MATCH (n:SerialBorn) RETURN n.value AS value")
                    .unwrap()
                    .rows;
                if committed == 0 {
                    assert!(born.is_err(), "uncommitted schema must not survive alone");
                    assert!(graph.is_empty());
                } else {
                    assert_eq!(born.unwrap().rows[0]["value"], Value::Int(73));
                    assert_eq!(graph[0]["value"], Value::Int(73));
                }
                if path.join("acknowledged").exists() {
                    assert_eq!(committed, 1);
                }
                drop(reopened);
                std::fs::remove_dir_all(path).unwrap();
            }
        }
    }
}

#[test]
fn serializable_separate_join_parent_changes_and_absence_are_complete() {
    for (initial, change) in [
        (true, "UPDATE parents SET payload = 11 WHERE id = 1"),
        (true, "UPDATE parents SET visible = false WHERE id = 1"),
        (true, "DELETE FROM parents WHERE id = 1"),
        (
            false,
            "INSERT INTO parents (id, visible, payload) VALUES (1, true, 10)",
        ),
    ] {
        let mut database = fixture();
        database
            .query_sql(
                "CREATE TABLE parents (id BIGINT PRIMARY KEY, visible BOOLEAN, payload BIGINT)",
            )
            .unwrap();
        if initial {
            database
                .query_sql("INSERT INTO parents (id, visible, payload) VALUES (1, true, 10)")
                .unwrap();
        }
        let query = "SELECT p.payload FROM inputs AS c INNER JOIN parents AS p ON c.id = p.id WHERE c.id = $1 AND p.visible = true";
        let expected = database
            .query_sql_with_params(query, &[Value::Int(1)])
            .unwrap();
        let db = ConcurrentDatabase::new(database);
        let mut tx = db
            .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
            .unwrap();
        assert_eq!(
            tx.query_sql_with_params(query, &[Value::Int(1)])
                .unwrap()
                .rows,
            expected.rows
        );
        assert_eq!(
            tx.read_dependency_usage().unwrap().relational_table_domains,
            2
        );
        stage_output(&mut tx);
        update_input(&db, change, &[]);
        assert!(tx.commit().unwrap_err().is_retryable_transaction_conflict());
        assert!(db
            .query_sql("SELECT id FROM outputs")
            .unwrap()
            .rows
            .is_empty());
    }
}

#[test]
fn serializable_graph_aggregate_and_adjacency_changes_invalidate_inputs() {
    for query in [
        "MATCH (n:Input) RETURN count(n) AS total",
        "MATCH (n:Input)-[:LINK]->(m:Target) WHERE id(n) = $root RETURN m.value",
    ] {
        let mut database = fixture();
        database.query("CREATE (:Target {value: 1})").unwrap();
        database
            .query("MATCH (n:Input), (m:Target) CREATE (n)-[:LINK]->(m)")
            .unwrap();
        let root = database
            .query("MATCH (n:Input) RETURN id(n) AS id")
            .unwrap()
            .rows[0]["id"]
            .clone();
        let parameters = BTreeMap::from([("root".into(), root)]);
        let expected = database.query_with_params(query, &parameters).unwrap();
        let db = ConcurrentDatabase::new(database);
        let mut tx = db
            .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
            .unwrap();
        assert_eq!(
            tx.query_with_params(query, &parameters).unwrap().rows,
            expected.rows
        );
        stage_output(&mut tx);
        if query.contains("count") {
            db.query("CREATE (:Input {key: 'two', value: 20})").unwrap();
        } else {
            db.query("MATCH (n:Input)-[r:LINK]->(m:Target) DELETE r")
                .unwrap();
        }
        assert!(tx.commit().unwrap_err().is_retryable_transaction_conflict());
    }
}

#[test]
fn serializable_graph_label_scope_allows_unrelated_canonical_writer() {
    let mut database = fixture();
    database.query("CREATE (:Other {value: 1})").unwrap();
    let db = ConcurrentDatabase::new(database);
    let mut tx = db
        .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
        .unwrap();
    tx.query("MATCH (n:Input) RETURN n.value").unwrap();
    assert!(!tx.read_dependency_usage().unwrap().whole_database_barrier);
    stage_output(&mut tx);
    let mut other = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    other.query("MATCH (n:Other) SET n.value = 2").unwrap();
    other.commit().unwrap();
    tx.commit().unwrap();
}

#[test]
fn dependency_read_borrowed_cancellation_and_late_parse_error_are_sticky() {
    for cancel in [false, true] {
        let mut database = fixture();
        let context = crate::RuntimeTaskContext::default();
        let mut tx = database
            .begin_transaction_with_isolation_and_context(
                TransactionIsolation::Serializable,
                TransactionReadDependencyLimits::default(),
                &context,
            )
            .unwrap();
        tx.query_sql("INSERT INTO outputs (id, value) VALUES (1, 99)")
            .unwrap();
        if cancel {
            context.cancellation().cancel();
        }
        assert!(tx
            .query_sql(if cancel {
                "SELECT id FROM inputs"
            } else {
                "SELECT FROM"
            })
            .is_err());
        assert!(tx.read_dependency_usage().unwrap().refused);
        assert!(matches!(
            tx.commit().unwrap_err(),
            HawDBError::TransactionReadDependencyRefused { .. }
        ));
        assert!(database
            .query_sql("SELECT id FROM outputs")
            .unwrap()
            .rows
            .is_empty());
    }
}

#[test]
fn dependency_read_sql_aggregate_and_variable_traversal_are_explicitly_refused() {
    let db = ConcurrentDatabase::new(fixture());
    let mut tx = db
        .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
        .unwrap();
    assert!(matches!(
        tx.query_sql("SELECT count(*) FROM inputs"),
        Err(HawDBError::TransactionReadDependencyRefused { .. })
    ));
    assert!(tx.read_dependency_usage().unwrap().refused);
    let mut tx = db
        .begin_transaction(options(crate::ConcurrentTransactionMode::Optimistic))
        .unwrap();
    assert!(tx
        .query("MATCH (n:Input)-[:LINK*1..3]->(m) RETURN m")
        .is_err());
    assert!(tx.read_dependency_usage().unwrap().refused);
    assert!(matches!(
        tx.commit(),
        Err(HawDBError::TransactionReadDependencyRefused { .. })
    ));
}

#[test]
fn dependency_read_parameterized_results_match_profiled_ordinary_access() {
    let mut database = fixture();
    database
        .query_sql("CREATE TABLE fractions (id BIGINT PRIMARY KEY, value DOUBLE PRECISION)")
        .unwrap();
    database
        .query_sql("INSERT INTO fractions (id, value) VALUES (1, 1.5)")
        .unwrap();
    for (query, parameters, exact, prefix) in [
        (
            "SELECT payload FROM inputs WHERE id = $1",
            vec![Value::Int(1)],
            1,
            0,
        ),
        (
            "SELECT payload FROM inputs WHERE team = $1",
            vec![Value::Int(7)],
            0,
            1,
        ),
        (
            "SELECT id FROM inputs WHERE payload > $1 ORDER BY id LIMIT 1",
            vec![Value::Int(0)],
            0,
            0,
        ),
        (
            "SELECT id FROM fractions WHERE value = $1",
            vec![Value::Float(1.5)],
            0,
            0,
        ),
    ] {
        let profiled = database
            .begin_read_transaction()
            .unwrap()
            .query_sql_with_params_options_profiled(
                query,
                &parameters,
                crate::QueryStreamOptions::default(),
            )
            .unwrap();
        assert!(!profiled.profile.operator_cardinality_profiles.is_empty());
        let mut tx = database
            .begin_transaction_with_isolation(
                TransactionIsolation::Serializable,
                TransactionReadDependencyLimits::default(),
            )
            .unwrap();
        assert_eq!(
            tx.query_sql_with_params(query, &parameters).unwrap().rows,
            profiled.output.rows
        );
        let usage = tx.read_dependency_usage().unwrap();
        assert_eq!(usage.exact_relational_rows, exact);
        assert_eq!(usage.relational_index_prefixes, prefix);
        assert_eq!(
            usage.relational_table_domains,
            usize::from(exact + prefix == 0)
        );
        eprintln!("transaction_capture_access query={query:?} ordinary_operator={:?} exact={exact} prefix={prefix} domains={}", profiled.profile.operator_cardinality_profiles[0].operator, usage.relational_table_domains);
        tx.commit().unwrap();
    }
}

#[test]
fn serializable_borrowed_cancel_before_commit_discards_private_writes() {
    let mut database = fixture();
    let context = crate::RuntimeTaskContext::default();
    let mut tx = database
        .begin_transaction_with_isolation_and_context(
            TransactionIsolation::Serializable,
            TransactionReadDependencyLimits::default(),
            &context,
        )
        .unwrap();
    tx.query_sql("INSERT INTO outputs (id, value) VALUES (1, 99)")
        .unwrap();
    context.cancellation().cancel();
    assert!(tx.commit().is_err());
    assert!(database
        .query_sql("SELECT id FROM outputs")
        .unwrap()
        .rows
        .is_empty());
}
