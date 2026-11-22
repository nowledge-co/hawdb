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

mod vector_schema;

fn database() -> Database {
    let mut database = Database::new();
    database
        .sql_session()
        .query(
            "CREATE TABLE pages (id BIGINT PRIMARY KEY, body TEXT, updated_at BIGINT)",
            &[],
        )
        .unwrap();
    database
}

fn rows(database: &mut Database) -> crate::executor::QueryRows {
    database
        .sql_session()
        .query("SELECT id, body FROM pages ORDER BY id", &[])
        .unwrap()
        .output
        .rows
}

#[test]
fn sql_session_returns_committed_dml_counts_and_returning_rows() {
    let mut database = database();
    let mut session = database.sql_session();
    let inserted = session
        .query(
            "INSERT INTO pages (id, body) VALUES ($1, $2), ($3, $4) RETURNING id, body",
            &[
                Value::Int(1),
                Value::String("first".into()),
                Value::Int(2),
                Value::String("second".into()),
            ],
        )
        .unwrap();
    assert_eq!(inserted.output.rows.len(), 2);
    let mutation = inserted.mutation.unwrap();
    assert_eq!(mutation.affected_rows, 2);
    assert!(!mutation.provisional);
    let duplicate = session
        .query(
            "INSERT INTO pages (id, body) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING RETURNING id",
            &[Value::Int(1), Value::String("ignored".into())],
        )
        .unwrap();
    assert!(duplicate.output.rows.is_empty());
    let mutation = duplicate.mutation.unwrap();
    assert_eq!(mutation.affected_rows, 0);
    assert_eq!(mutation.conflict_rows, 1);
    assert!(!mutation.provisional);
    assert_eq!(
        session
            .query(
                "UPDATE pages SET body = $1 WHERE id = $2",
                &[Value::String("updated".into()), Value::Int(1)]
            )
            .unwrap()
            .mutation
            .unwrap()
            .affected_rows,
        1
    );
    assert_eq!(
        session
            .query("DELETE FROM pages WHERE id = $1", &[Value::Int(2)])
            .unwrap()
            .mutation
            .unwrap()
            .affected_rows,
        1
    );
    assert_eq!(
        session
            .query("SELECT body FROM pages WHERE id = $1", &[Value::Int(1)])
            .unwrap()
            .output
            .rows[0]
            .get("body"),
        Some(&Value::String("updated".into()))
    );
}

#[test]
fn sql_session_update_delete_and_upsert_return_final_row_images() {
    let mut database = database();
    let mut session = database.sql_session();
    session
        .query(
            "INSERT INTO pages (id, body) VALUES (1, 'old'), (2, 'remove')",
            &[],
        )
        .unwrap();

    let updated = session
        .query(
            "UPDATE pages AS p SET body = 'new' WHERE p.id = 1 RETURNING p.id, p.body",
            &[],
        )
        .unwrap();
    assert_eq!(updated.output.rows[0]["id"], Value::Int(1));
    assert_eq!(updated.output.rows[0]["body"], Value::String("new".into()));
    assert_eq!(updated.mutation.unwrap().affected_rows, 1);

    let deleted = session
        .query(
            "DELETE FROM pages AS p WHERE p.id = 2 RETURNING p.id, p.body",
            &[],
        )
        .unwrap();
    assert_eq!(deleted.output.rows[0]["id"], Value::Int(2));
    assert_eq!(
        deleted.output.rows[0]["body"],
        Value::String("remove".into())
    );

    let upserted = session
        .query(
            "INSERT INTO pages (id, body) VALUES (1, 'replaced'), (3, 'inserted') \
             ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body RETURNING id, body",
            &[],
        )
        .unwrap();
    assert_eq!(upserted.output.rows.len(), 2);
    assert_eq!(
        upserted.output.rows[0]["body"],
        Value::String("replaced".into())
    );
    assert_eq!(
        upserted.output.rows[1]["body"],
        Value::String("inserted".into())
    );
    assert_eq!(upserted.mutation.unwrap().affected_rows, 2);

    let zero = session
        .query(
            "UPDATE pages SET body = 'missing' WHERE id = 99 RETURNING id, body",
            &[],
        )
        .unwrap();
    assert!(zero.output.rows.is_empty());
    assert_eq!(zero.mutation.unwrap().affected_rows, 0);
}

#[test]
fn sql_session_data_modifying_cte_is_atomic_and_exposes_only_returning_rows() {
    let mut database = database();
    database
        .sql_session()
        .query("INSERT INTO pages (id, body) VALUES (1, 'old')", &[])
        .unwrap();

    let output = database
        .sql_session()
        .query(
            "WITH changed(id, final_body) AS (\
                 UPDATE pages SET body = 'new' WHERE id = 1 RETURNING id, body\
             ) \
             SELECT changed.id, changed.final_body, pages.body AS snapshot_body \
             FROM changed INNER JOIN pages ON pages.id = changed.id",
            &[],
        )
        .unwrap();
    assert_eq!(output.output.rows.len(), 1);
    assert_eq!(
        output.output.rows[0]["final_body"],
        Value::String("new".into())
    );
    assert_eq!(
        output.output.rows[0]["snapshot_body"],
        Value::String("old".into())
    );
    assert!(output.mutation.is_none());
    assert_eq!(rows(&mut database)[0]["body"], Value::String("new".into()));

    let failure = database.sql_session().query(
        "WITH changed AS (UPDATE pages SET body = 'must-rollback' WHERE id = 1 RETURNING id, body) \
         SELECT missing FROM changed",
        &[],
    );
    assert!(failure.is_err());
    assert_eq!(rows(&mut database)[0]["body"], Value::String("new".into()));
}

#[test]
fn autocommit_data_modifying_ctes_share_one_commit_and_fail_before_publication() {
    let mut database = database();
    database
        .query_sql("CREATE TABLE archive (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    database
        .query_sql("INSERT INTO pages (id, body) VALUES (1, 'old')")
        .unwrap();
    database
        .query_sql("INSERT INTO archive (id, body) VALUES (2, 'gone')")
        .unwrap();
    database
        .query_sql(
            "CREATE TABLE generated_items (\
                 id BIGINT PRIMARY KEY, body TEXT NOT NULL DEFAULT 'generated'\
             )",
        )
        .unwrap();

    let inserted = database
        .query_sql(
            "WITH inserted AS (\
                 INSERT INTO generated_items (id) VALUES (1) RETURNING id, body\
             ) SELECT id, body FROM inserted",
        )
        .unwrap();
    assert_eq!(inserted.rows[0]["id"], Value::Int(1));
    assert_eq!(inserted.rows[0]["body"], Value::String("generated".into()));

    let constraint_failure = database.query_sql(
        "WITH page_added AS (\
             INSERT INTO pages (id, body) VALUES (4, 'must-rollback') RETURNING id\
         ), archive_conflict AS (\
             INSERT INTO archive (id, body) VALUES (2, 'duplicate') RETURNING id\
         ) SELECT id FROM page_added UNION ALL SELECT id FROM archive_conflict",
    );
    assert!(constraint_failure.is_err());
    assert!(database
        .query_sql("SELECT id FROM pages WHERE id = 4")
        .unwrap()
        .rows
        .is_empty());

    let sql = "WITH changed(id, body) AS (\
                   UPDATE pages SET body = 'new' WHERE id = 1 RETURNING id, body\
               ), removed(id, body) AS (\
                   DELETE FROM archive WHERE id = 2 RETURNING id, body\
               ) \
               SELECT id, body FROM changed UNION ALL SELECT id, body FROM removed ORDER BY id";
    let refused = database.query_sql_request(crate::SqlQueryRequest::new(sql).with_output_limits(
        crate::QueryStreamOptions {
            max_rows: Some(0),
            max_payload_bytes: Some(4096),
        },
    ));
    assert!(matches!(refused, Err(HawDBError::ReadBudgetExceeded(_))));
    assert_eq!(rows(&mut database)[0]["body"], Value::String("old".into()));
    assert_eq!(
        database
            .query_sql("SELECT body FROM archive WHERE id = 2")
            .unwrap()
            .rows[0]["body"],
        Value::String("gone".into())
    );

    let refused = database.query_sql_request(crate::SqlQueryRequest::new(sql).with_output_limits(
        crate::QueryStreamOptions {
            max_rows: Some(10),
            max_payload_bytes: Some(0),
        },
    ));
    assert!(matches!(refused, Err(HawDBError::ReadBudgetExceeded(_))));
    assert_eq!(rows(&mut database)[0]["body"], Value::String("old".into()));
    assert_eq!(
        database
            .query_sql("SELECT body FROM archive WHERE id = 2")
            .unwrap()
            .rows[0]["body"],
        Value::String("gone".into())
    );

    let output = database.query_sql(sql).unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0]["body"], Value::String("new".into()));
    assert_eq!(output.rows[1]["body"], Value::String("gone".into()));
    assert_eq!(rows(&mut database)[0]["body"], Value::String("new".into()));
    assert!(database
        .query_sql("SELECT id FROM archive")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn sql_session_multi_row_upsert_and_read_your_writes_commit_together() {
    let mut database = database();
    database
        .sql_session()
        .query("INSERT INTO pages (id, body) VALUES (1, 'old')", &[])
        .unwrap();
    let result = database
        .sql_session()
        .transaction(|transaction| {
            let inserted = transaction.query(
                "INSERT INTO pages (id, body) VALUES ($1, $2), ($3, $4) \
             ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body",
                &[
                    Value::Int(1),
                    Value::String("replaced".into()),
                    Value::Int(2),
                    Value::String("new".into()),
                ],
            )?;
            assert!(inserted.output.rows.is_empty());
            let mutation = inserted.mutation.unwrap();
            assert!(mutation.provisional);
            assert_eq!(mutation.affected_rows, 2);
            transaction.query("INSERT INTO pages (id, body) VALUES (3, 'last')", &[])?;
            let read = transaction.query("SELECT id, body FROM pages ORDER BY id", &[])?;
            assert_eq!(read.output.rows.len(), 3);
            assert_eq!(
                read.output.rows[0].get("body"),
                Some(&Value::String("replaced".into()))
            );
            Ok(read.output.rows)
        })
        .unwrap();
    assert_eq!(rows(&mut database), result);
}

#[test]
fn sql_session_caught_unique_failure_aborts_the_whole_transaction() {
    let mut database = database();
    let mut first_error = None;
    let failure = database
        .sql_session()
        .transaction(|transaction| {
            transaction.query("INSERT INTO pages (id, body) VALUES (1, 'pending')", &[])?;
            let error = transaction
                .query("INSERT INTO pages (id, body) VALUES (1, 'duplicate')", &[])
                .unwrap_err();
            assert_eq!(
                transaction
                    .query("INSERT INTO pages (id) VALUES (2)", &[])
                    .unwrap_err(),
                error
            );
            first_error = Some(error);
            Ok(())
        })
        .unwrap_err();
    assert_eq!(Some(failure), first_error);
    assert!(rows(&mut database).is_empty());
    database
        .sql_session()
        .query("INSERT INTO pages (id) VALUES (1)", &[])
        .unwrap();
    assert_eq!(rows(&mut database).len(), 1);
}

#[test]
fn sql_session_callback_failure_and_unwind_discard_all_staged_writes() {
    let mut database = database();
    let expected = HawDBError::Execution("host rejected transaction".into());
    let failure = database
        .sql_session()
        .transaction::<()>(|transaction| {
            transaction.query("INSERT INTO pages (id) VALUES (1)", &[])?;
            Err(expected.clone())
        })
        .unwrap_err();
    assert_eq!(failure, expected);
    assert!(rows(&mut database).is_empty());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        database.sql_session().transaction::<()>(|transaction| {
            transaction.query("INSERT INTO pages (id) VALUES (2)", &[])?;
            panic!("host callback unwound")
        })
    }));
    assert!(panic.is_err());
    assert!(rows(&mut database).is_empty());
    database
        .sql_session()
        .query("INSERT INTO pages (id) VALUES (3)", &[])
        .unwrap();
    assert_eq!(rows(&mut database).len(), 1);
}

#[test]
fn sql_session_unsupported_postgres_shapes_preserve_errors_and_rollback() {
    let mut database = database();
    for sql in [
        "CREATE EXTENSION vector",
        "CREATE EXTENSION pg_trgm",
        "CREATE EXTENSION pgcrypto",
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$ BEGIN END $$",
        "CREATE TRIGGER t BEFORE INSERT ON pages EXECUTE FUNCTION f()",
        "CREATE TABLE unsupported (body TSVECTOR)",
        "CREATE TABLE unsupported (body VECTOR(0))",
        "SELECT to_tsvector(body) FROM pages",
        "SELECT body::jsonb ->> 'name' FROM pages",
        "SELECT clock_timestamp() FROM pages",
        "SELECT body::date FROM pages",
        "SELECT body::timestamptz AT TIME ZONE 'UTC' FROM pages",
    ] {
        let expected = crate::sql::prepare_postgres_sql(sql).unwrap_err();
        assert!(matches!(
            expected,
            HawDBError::Parse(_) | HawDBError::Semantic(_)
        ));
        let error = database
            .sql_session()
            .transaction(|transaction| {
                transaction.query("INSERT INTO pages (id) VALUES (1)", &[])?;
                transaction.query(sql, &[])?;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error, expected, "{sql}");
        assert!(rows(&mut database).is_empty(), "{sql}");
    }
}

#[test]
fn sql_session_gbrain_upsert_rejection_cannot_commit_an_earlier_write() {
    let mut database = database();
    let sql = "INSERT INTO pages (id, body, updated_at) VALUES ($1, $2::jsonb, now()) \
               ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body, updated_at = now() RETURNING id";
    let expected = crate::sql::prepare_postgres_sql(sql).unwrap_err();
    let error = database
        .sql_session()
        .transaction(|transaction| {
            transaction.query("INSERT INTO pages (id, body) VALUES (1, 'pending')", &[])?;
            transaction.query(
                sql,
                &[Value::Int(2), Value::String(r#"{"name":"page"}"#.into())],
            )?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error, expected);
    assert!(rows(&mut database).is_empty());
}

#[test]
fn sql_session_parameter_and_multi_statement_failures_are_atomic() {
    let mut database = database();
    for (sql, parameters) in [
        (
            "INSERT INTO pages (id, body) VALUES ($1, $2)",
            vec![Value::Int(2)],
        ),
        (
            "INSERT INTO pages (id) VALUES ($2)",
            vec![Value::Int(1), Value::Int(2)],
        ),
        (
            "INSERT INTO pages (id) VALUES (2); INSERT INTO pages (id) VALUES (3)",
            vec![],
        ),
    ] {
        assert!(database
            .sql_session()
            .transaction(|transaction| {
                transaction.query("INSERT INTO pages (id) VALUES (1)", &[])?;
                transaction.query(sql, &parameters)?;
                Ok(())
            })
            .is_err());
        assert!(rows(&mut database).is_empty());
    }
}

#[test]
fn sql_session_context_cancellation_preserves_atomicity() {
    let mut database = database();
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    let context = RuntimeTaskContext::without_deadline(cancellation.clone());
    assert!(database
        .sql_session()
        .with_task_context(&context)
        .transaction(|transaction| {
            transaction.query("INSERT INTO pages (id) VALUES (1)", &[])?;
            cancellation.cancel();
            transaction.query("SELECT id FROM pages", &[])?;
            Ok(())
        })
        .is_err());
    assert!(rows(&mut database).is_empty());
}

#[test]
fn sql_session_durable_commit_and_aborted_transaction_survive_reopen() {
    let root = std::env::temp_dir().join(format!(
        "hawdb-sql-session-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let mut database = Database::open(&root).unwrap();
        database
            .sql_session()
            .transaction(|transaction| {
                transaction.query(
                    "CREATE TABLE pages (id BIGINT PRIMARY KEY, body TEXT, updated_at BIGINT)",
                    &[],
                )?;
                transaction.query("INSERT INTO pages (id, body) VALUES (1, 'durable')", &[])?;
                Ok(())
            })
            .unwrap();
        assert!(database
            .sql_session()
            .transaction(|transaction| {
                transaction.query(
                    "INSERT INTO pages (id, body) VALUES (2, 'must not persist')",
                    &[],
                )?;
                transaction.query("INSERT INTO pages (id) VALUES (1)", &[])?;
                Ok(())
            })
            .is_err());
    }
    let mut database = Database::open(&root).unwrap();
    let actual = rows(&mut database);
    assert_eq!(actual.len(), 1);
    assert_eq!(
        actual[0].get("body"),
        Some(&Value::String("durable".into()))
    );
    drop(database);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_session_read_budget_refusal_rolls_back_prior_writes() {
    for config in [
        crate::DatabaseConfig {
            max_read_result_rows: Some(1),
            ..Default::default()
        },
        crate::DatabaseConfig {
            max_read_result_payload_bytes: Some(64),
            ..Default::default()
        },
    ] {
        let mut database = Database::new_with_config(config);
        database
            .sql_session()
            .query("CREATE TABLE pages (id BIGINT PRIMARY KEY, body TEXT)", &[])
            .unwrap();
        database
            .sql_session()
            .query(
                "INSERT INTO pages (id, body) VALUES (1, $1), (2, $1)",
                &[Value::String("x".repeat(128))],
            )
            .unwrap();
        assert!(database
            .sql_session()
            .transaction(|transaction| {
                transaction.query("INSERT INTO pages (id) VALUES (3)", &[])?;
                transaction.query("SELECT id, body FROM pages", &[])?;
                Ok(())
            })
            .is_err());
        assert!(database
            .sql_session()
            .query("SELECT id FROM pages WHERE id = 3", &[])
            .unwrap()
            .output
            .rows
            .is_empty());
    }
}

#[test]
fn sql_session_read_only_queries_preserve_epoch_and_reject_writes() {
    let root = std::env::temp_dir().join(format!(
        "hawdb-sql-session-read-only-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let mut database = Database::open(&root).unwrap();
        database
            .sql_session()
            .query("CREATE TABLE pages (id BIGINT PRIMARY KEY)", &[])
            .unwrap();
        database
            .sql_session()
            .query("INSERT INTO pages (id) VALUES (1)", &[])
            .unwrap();
        database.checkpoint().unwrap();
    }
    let mut database = Database::open_with_config(
        &root,
        crate::DatabaseConfig {
            read_only: true,
            recovery_mode: crate::RecoveryMode::Strict,
            ..Default::default()
        },
    )
    .unwrap();
    let before = database.commit_epoch().unwrap();
    assert_eq!(
        database
            .sql_session()
            .query("SELECT id FROM pages", &[])
            .unwrap()
            .output
            .rows
            .len(),
        1
    );
    assert!(database
        .sql_session()
        .query("INSERT INTO pages (id) VALUES (2)", &[])
        .is_err());
    assert_eq!(database.commit_epoch().unwrap(), before);
    assert_eq!(
        database
            .sql_session()
            .transaction(|transaction| {
                Ok(transaction
                    .query("SELECT id FROM pages", &[])?
                    .output
                    .rows
                    .len())
            })
            .unwrap(),
        1
    );
    drop(database);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_session_upsert_returning_commits_with_earlier_writes() {
    let mut database = database();
    database
        .sql_session()
        .transaction(|transaction| {
            transaction.query("INSERT INTO pages (id, body) VALUES (1, 'old')", &[])?;
            let result = transaction.query(
                "INSERT INTO pages (id, body) VALUES (1, 'new') \
                 ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body RETURNING id, body",
                &[],
            )?;
            assert_eq!(result.output.rows[0]["id"], Value::Int(1));
            assert_eq!(result.output.rows[0]["body"], Value::String("new".into()));
            Ok(())
        })
        .unwrap();
    assert_eq!(rows(&mut database)[0]["body"], Value::String("new".into()));
}

#[test]
fn sql_session_cancellation_after_the_last_statement_prevents_commit() {
    let mut database = database();
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    let context = RuntimeTaskContext::without_deadline(cancellation.clone());
    assert!(database
        .sql_session()
        .with_task_context(&context)
        .transaction(|transaction| {
            transaction.query("INSERT INTO pages (id) VALUES (1)", &[])?;
            cancellation.cancel();
            Ok(())
        })
        .is_err());
    assert!(rows(&mut database).is_empty());
}
