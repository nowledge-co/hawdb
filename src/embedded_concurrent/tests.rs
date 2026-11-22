// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::Value;
use std::sync::mpsc;
use std::time::Duration;

const CLOCK_QUERY: &str = "SELECT CURRENT_TIMESTAMP AS t, now() AS n";

#[test]
fn sql_autocommit_rebinds_the_clock_without_republishing_data() {
    use hawdb_core::temporal::{parse_datetime, with_statement_clock};

    let engine = HawDBConcurrentEmbedded::new(HawDBEmbedded::open_in_memory());
    let publication = engine.owner.view().unwrap();
    for text in ["2026-11-22T01:00:00Z", "2026-11-22T02:00:00Z"] {
        let clock = parse_datetime(text).unwrap();
        let output = with_statement_clock(clock, || {
            engine.query_sql_request_admitted(SqlQueryRequest::new(CLOCK_QUERY))
        })
        .unwrap();
        assert_eq!(output.rows[0]["t"], Value::DateTime(clock));
        assert_eq!(output.rows[0]["n"], Value::DateTime(clock));
        assert!(Arc::ptr_eq(&publication, &engine.owner.view().unwrap()));
    }
}

#[test]
fn new_read_transactions_get_distinct_clocks_but_transaction_forks_keep_the_pin() {
    use hawdb_core::temporal::{parse_datetime, with_statement_clock};

    let engine = HawDBConcurrentEmbedded::new(HawDBEmbedded::open_in_memory());
    let publication = engine.owner.view().unwrap();
    let first_clock = parse_datetime("2026-11-22T01:00:00Z").unwrap();
    let second_clock = parse_datetime("2026-11-22T02:00:00Z").unwrap();
    let first = with_statement_clock(first_clock, || engine.begin_read_transaction()).unwrap();
    with_statement_clock(second_clock, || {
        let second = engine.begin_read_transaction().unwrap();
        let fork = first.fork_at_same_snapshot(None).unwrap();
        for _ in 0..2 {
            for (transaction, expected) in [
                (&first, first_clock),
                (&fork, first_clock),
                (&second, second_clock),
            ] {
                let output = transaction.query_sql(CLOCK_QUERY).unwrap();
                assert_eq!(output.rows[0]["t"], Value::DateTime(expected));
                assert_eq!(output.rows[0]["n"], Value::DateTime(expected));
            }
        }
    });
    assert_eq!(
        first.commit_epoch(),
        engine.begin_read_transaction().unwrap().commit_epoch()
    );
    assert!(Arc::ptr_eq(&publication, &engine.owner.view().unwrap()));
}

#[test]
fn replacements_share_the_owner_governor_for_ordinary_and_retained_work() {
    use crate::{RetainedQueryError, RetainedQueryOptions};
    use hawdb_qos::{RuntimeAdmissionCode, RuntimeWorkKind};
    use std::collections::BTreeMap;

    fn fixture() -> HawDBEmbedded {
        let mut embedded = HawDBEmbedded::open_in_memory();
        embedded.runtime_governor().pin_resources();
        for query in [
            "CREATE NODE TABLE Item",
            "CREATE PROPERTY ON NODE TABLE Item(value) TYPE INT",
            "CREATE INDEX ON :Item(value)",
            "CREATE CONSTRAINT ON :Item(value) ASSERT UNIQUE",
            "CREATE (:Item {value: 1})",
        ] {
            embedded.database_mut().query(query).unwrap();
        }
        embedded
    }

    const QUERY: &str = "MATCH (n:Item {value: $key}) RETURN n AS node";
    let parameters = BTreeMap::from([("key".into(), Value::Int(1))]);
    for replace_handle in [false, true] {
        let engine = HawDBConcurrentEmbedded::new(fixture());
        let governor = engine.runtime_governor().clone();
        let old_cursor = engine
            .query_with_params_retained_node(QUERY, &parameters, RetainedQueryOptions::default())
            .unwrap();
        let owner_charge = governor.retained_result_snapshot().retained_bytes;
        assert!(owner_charge > 0);
        for _ in 0..2 {
            let observed = engine.with_embedded_mut(|embedded| {
                embedded.database().retained_result_snapshot().unwrap()
            });
            assert_eq!(observed.retained_bytes, owner_charge);
        }

        let mut replacement = fixture();
        let foreign_governor = replacement.runtime_governor().clone();
        // Exercise an already bound replacement, not just a cold handle.
        let foreign_cursor = replacement
            .begin_read_transaction()
            .unwrap()
            .into_retained_node_query(QUERY, &parameters, RetainedQueryOptions::default())
            .unwrap();
        let foreign_charge = foreign_governor.retained_result_snapshot().retained_bytes;
        assert!(foreign_charge > 0);
        engine.with_embedded_mut(|embedded| {
            if replace_handle {
                *embedded = replacement;
            } else {
                *embedded.database_mut() =
                    std::mem::replace(replacement.database_mut(), crate::Database::new());
            }
        });
        let adopted = engine
            .with_embedded_mut(|embedded| embedded.database().retained_result_snapshot().unwrap());
        assert_eq!(adopted.retained_bytes, owner_charge);

        let permit = governor
            .try_admit(
                RuntimeWorkRequest::new(RuntimeWorkPriority::Foreground, RuntimeWorkKind::Control)
                    .with_cpu_slots(governor.snapshot().limits.effective_cpu_slots.get()),
            )
            .unwrap();
        for result in [
            engine.query_sql_request_admitted(SqlQueryRequest::new("SELECT 1")),
            engine.query_request_admitted(QueryRequest::new("MATCH (n) RETURN count(n) AS total")),
            engine.with_embedded_mut(|embedded| {
                embedded.query_sql_request_admitted(SqlQueryRequest::new("SELECT 1"))
            }),
        ] {
            assert!(
                matches!(result, Err(EmbeddedQueryError::Admission(ref error))
                if error.code == RuntimeAdmissionCode::CpuSaturated),
                "{result:?}"
            );
        }
        let retained = engine.query_with_params_retained_node(
            QUERY,
            &parameters,
            RetainedQueryOptions::default(),
        );
        assert!(
            matches!(retained, Err(RetainedQueryError::Admission(ref error))
            if error.code == RuntimeAdmissionCode::CpuSaturated),
            "{retained:?}"
        );
        drop(permit);

        engine
            .query_sql_request_admitted(SqlQueryRequest::new("SELECT 1"))
            .unwrap();
        let mut current = engine
            .query_with_params_retained_node(QUERY, &parameters, RetainedQueryOptions::default())
            .unwrap();
        let node = current.next_node().unwrap().unwrap();
        assert!(governor.retained_result_snapshot().retained_bytes > owner_charge);
        assert_eq!(
            foreign_governor.retained_result_snapshot().retained_bytes,
            foreign_charge
        );
        drop((current, node));
        assert_eq!(
            governor.retained_result_snapshot().retained_bytes,
            owner_charge
        );
        drop((old_cursor, foreign_cursor));
        assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
        assert_eq!(
            foreign_governor.retained_result_snapshot().retained_bytes,
            0
        );
        assert_eq!(governor.snapshot().active_cpu_slots, 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn cold_persistent_reads_reuse_the_admitted_sql_template_cache() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-shared-cold-sql-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let engine = HawDBConcurrentEmbedded::new(HawDBEmbedded::open(&path).unwrap());
    let query = SqlQueryRequest::new("SELECT 41 AS value");
    for _ in 0..2 {
        assert_eq!(
            engine.query_sql_request_admitted(query).unwrap().rows[0]["value"],
            Value::Int(41)
        );
    }
    let before = engine
        .owner
        .inspect()
        .database()
        .relational_plan_template_cache_stats()
        .unwrap();
    engine.query_sql_request_admitted(query).unwrap();
    let after = engine
        .owner
        .inspect()
        .database()
        .relational_plan_template_cache_stats()
        .unwrap();
    assert_eq!(after.hits, before.hits + 1);
    drop(engine);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn admitted_reads_complete_while_commit_owner_is_held() {
    let engine = HawDBConcurrentEmbedded::new(HawDBEmbedded::open_in_memory());
    engine
        .query_request_admitted(QueryRequest::new("CREATE (:Item {value: 1})"))
        .unwrap();
    engine
        .query_sql_request_admitted(SqlQueryRequest::new("CREATE TABLE items (value BIGINT)"))
        .unwrap();
    engine
        .query_sql_request_admitted(SqlQueryRequest::new("INSERT INTO items (value) VALUES (1)"))
        .unwrap();
    let concurrent = engine.concurrent().unwrap();
    let (held, acquired) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        concurrent.with_autocommit_exclusive(|database| {
            let staged = database
                .query("MATCH (n:Item) SET n.value = 2")
                .and_then(|_| database.query_sql("UPDATE items SET value = 2 WHERE value = 1"));
            held.send(staged.as_ref().map(|_| ()).map_err(ToString::to_string))
                .unwrap();
            staged?;
            resume.recv().unwrap();
            database.query_sql("SELECT value FROM items")
        })
    });
    acquired
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    let reader = engine.clone();
    let (done, completed) = mpsc::channel();
    let read = std::thread::spawn(move || {
        let result = (|| {
            let graph = reader.query_request_admitted(QueryRequest::new(
                "MATCH (n:Item) RETURN n.value AS value",
            ))?;
            let sql = reader
                .query_sql_request_admitted(SqlQueryRequest::new("SELECT value FROM items"))?;
            Ok::<_, EmbeddedQueryError>((
                graph.rows[0]["value"].clone(),
                sql.rows[0]["value"].clone(),
            ))
        })();
        done.send(result).unwrap();
    });
    let progress = completed.recv_timeout(Duration::from_secs(10));
    release.send(()).unwrap();
    writer.join().unwrap().unwrap();
    read.join().unwrap();
    assert_eq!(progress.unwrap().unwrap(), (Value::Int(1), Value::Int(1)));
    assert_eq!(
        engine
            .query_sql_request_admitted(SqlQueryRequest::new("SELECT value FROM items"))
            .unwrap()
            .rows[0]["value"],
        Value::Int(2)
    );
    assert_eq!(engine.runtime_governor().snapshot().active_cpu_slots, 0);
    assert_eq!(
        engine.runtime_governor().snapshot().admitted_memory_bytes,
        0
    );
}

#[test]
fn replacement_and_open_user_transaction_do_not_reuse_old_publication() {
    let query = QueryRequest::new("MATCH (n) RETURN count(n) AS total");
    let engine = HawDBConcurrentEmbedded::new(HawDBEmbedded::open_in_memory());
    engine
        .query_request_admitted(QueryRequest::new("CREATE (:Old)"))
        .unwrap();
    engine.with_embedded_mut(|embedded| {
        let mut replacement = crate::Database::new();
        replacement.set_runtime_governor(embedded.runtime_governor().clone());
        *embedded.database_mut() = replacement;
    });
    let output = engine.query_request_admitted(query).unwrap();
    assert_eq!(output.rows[0]["total"], Value::Int(0));
    engine.with_embedded_mut(|embedded| embedded.begin_transaction().unwrap());
    assert!(engine.query_request_admitted(query).is_err());
    assert!(engine
        .query_sql_request_admitted(SqlQueryRequest::new("SELECT 1"))
        .is_err());
    assert!(engine.begin_read_transaction().is_err());
    engine.with_embedded_mut(|embedded| embedded.rollback_transaction().unwrap());
    engine.query_request_admitted(query).unwrap();
}

#[test]
fn failed_returning_budget_keeps_both_languages_atomic() {
    let engine = HawDBConcurrentEmbedded::new(HawDBEmbedded::open_in_memory());
    engine
        .query_sql_request_admitted(SqlQueryRequest::new(
            "CREATE TABLE items (id BIGINT PRIMARY KEY)",
        ))
        .unwrap();
    let limits = QueryStreamOptions {
        max_rows: Some(0),
        max_payload_bytes: Some(0),
    };
    let sql = engine.query_sql_request_admitted(
        SqlQueryRequest::new("INSERT INTO items (id) VALUES (1) RETURNING id")
            .with_output_limits(limits),
    );
    let graph = engine.query_request_admitted(
        QueryRequest::new("CREATE (n:Item) RETURN n").with_output_limits(limits),
    );
    for result in [sql, graph] {
        assert!(
            matches!(
                result,
                Err(EmbeddedQueryError::Database(
                    HawDBError::ReadBudgetExceeded(_)
                ))
            ),
            "{result:?}",
        );
    }
    assert_eq!(
        engine
            .query_sql_request_admitted(SqlQueryRequest::new("SELECT count(*) AS total FROM items"))
            .unwrap()
            .rows[0]["total"],
        Value::Int(0)
    );
    assert_eq!(
        engine
            .query_request_admitted(QueryRequest::new("MATCH (n) RETURN count(n) AS total"))
            .unwrap()
            .rows[0]["total"],
        Value::Int(0)
    );
    assert_eq!(
        engine.runtime_governor().snapshot().admitted_memory_bytes,
        0
    );
}
