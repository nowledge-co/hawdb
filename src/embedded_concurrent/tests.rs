// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::Value;
use std::sync::mpsc;
use std::time::Duration;

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
            database.query("MATCH (n:Item) SET n.value = 2")?;
            database.query_sql("UPDATE items SET value = 2")?;
            held.send(()).unwrap();
            resume.recv().unwrap();
            database.query_sql("SELECT value FROM items")
        })
    });
    acquired.recv_timeout(Duration::from_secs(10)).unwrap();
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
