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

use super::{tests, QueryRequest, SqlQueryRequest};
use crate::{
    ConcurrentDatabase, ConcurrentTransactionOptions, Database, HawDBEmbedded,
    HawDBEmbeddedOpenOptions, HawDBError, QueryOutput, QueryStreamOptions, ReadBudgetResource,
    Result, RuntimeTaskContext, ScoringCombination, Value,
};

fn assert_cypher_contract(
    mut execute: impl for<'a> FnMut(QueryRequest<'a>) -> Result<QueryOutput>,
) {
    let parameters = tests::parameters("x");
    let scoring =
        tests::scoring(ScoringCombination::WeightedSum, 0.0, 1.0).with_reference_time_millis(2_000);
    let request = QueryRequest::new(tests::QUERY)
        .with_params(&parameters)
        .with_scoring(&scoring)
        .with_output_limits(QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(8_192),
        });
    let output = execute(request).unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.schema().columns(),
        [
            "id",
            "score",
            "importance",
            "created",
            "scoring_rerank_score"
        ],
    );
    assert_eq!(output.rows[0]["id"], Value::String("late".into()));
    for (limits, resource, limit) in [
        (
            QueryStreamOptions {
                max_rows: Some(0),
                max_payload_bytes: None,
            },
            ReadBudgetResource::Rows,
            0,
        ),
        (
            QueryStreamOptions {
                max_rows: None,
                max_payload_bytes: Some(1),
            },
            ReadBudgetResource::PayloadBytes,
            1,
        ),
    ] {
        let error = execute(request.with_output_limits(limits)).unwrap_err();
        assert!(
            matches!(error, HawDBError::ReadBudgetExceeded(ref cause)
                if cause.resource == resource && cause.limit == limit),
            "unexpected output refusal: {error:?}"
        );
    }
    let access = crate::QueryAccessControlContext::visibility_scope(7, "space_id", "a");
    let result = execute(request.with_access_control(&access));
    #[cfg(feature = "acl")]
    assert_eq!(result.unwrap().rows[0]["id"], Value::String("early".into()));
    #[cfg(not(feature = "acl"))]
    assert!(matches!(
        result,
        Err(HawDBError::CapabilityUnavailable { .. })
    ));
    let stopped = RuntimeTaskContext::default();
    stopped.cancellation().cancel();
    assert!(execute(request.with_task_context(&stopped)).is_err());
    assert_eq!(
        execute(request).unwrap().rows[0]["id"],
        output.rows[0]["id"]
    );
}

#[test]
fn cypher_request_contract_survives_every_synchronous_workspace() {
    let mut database = tests::fixture();
    assert_cypher_contract(|request| database.query_request(request));
    let mut snapshot = database.begin_read_transaction().unwrap();
    assert_cypher_contract(|request| snapshot.query_request(request));
    let mut transaction = database.begin_transaction().unwrap();
    assert_cypher_contract(|request| transaction.query_request(request));
    transaction.rollback();
    let database = ConcurrentDatabase::new(database);
    assert_cypher_contract(|request| database.query_request(request));
    for options in [
        ConcurrentTransactionOptions::optimistic(),
        ConcurrentTransactionOptions::default(),
    ] {
        let mut transaction = database.begin_transaction(options).unwrap();
        assert_cypher_contract(|request| transaction.query_request(request));
        transaction.rollback();
    }
}

#[test]
fn embedded_requests_keep_scoring_acl_and_budgets() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-request-contract-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut embedded =
        HawDBEmbedded::open_with_options(HawDBEmbeddedOpenOptions::new(&path)).unwrap();
    *embedded.database_mut() = tests::fixture();
    assert_cypher_contract(|request| {
        embedded
            .query_request_admitted(request)
            .map_err(|error| match error {
                crate::EmbeddedQueryError::Database(error) => error,
                error => HawDBError::Execution(error.to_string()),
            })
    });
    embedded.begin_transaction().unwrap();
    assert_cypher_contract(|request| embedded.transaction_query_request(request));
    embedded.rollback_transaction().unwrap();
    *embedded.database_mut() = sql_fixture();
    embedded.begin_transaction().unwrap();
    assert_sql_contract(|request| embedded.transaction_query_sql_request(request));
    assert_rejected_returning_is_atomic(|request| embedded.transaction_query_sql_request(request));
    embedded.commit_transaction().unwrap();
    assert_eq!(
        embedded
            .database_mut()
            .query_sql("SELECT id FROM records WHERE id = 3")
            .unwrap()
            .rows
            .len(),
        1
    );
    drop(embedded);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn prepared_requests_rebind_scoring_and_reject_cross_policy_reuse() {
    let mut database = tests::fixture();
    let parameters = tests::parameters("x");
    let first =
        tests::scoring(ScoringCombination::WeightedSum, 1.0, 0.0).with_reference_time_millis(1_000);
    let next =
        tests::scoring(ScoringCombination::WeightedSum, 0.0, 1.0).with_reference_time_millis(2_000);
    let request = QueryRequest::new(tests::QUERY)
        .with_params(&parameters)
        .with_scoring(&first);
    let prepared = database.prepare_runtime_request(request).unwrap();
    let after_prepare = database.plan_cache_stats().unwrap();
    let output = database
        .query_prepared_request(prepared, request.with_scoring(&next))
        .unwrap();
    assert_eq!(output.rows[0]["id"], Value::String("late".into()));
    let after_execute = database.plan_cache_stats().unwrap();
    assert_eq!(after_execute.hits, after_prepare.hits);
    assert_eq!(after_execute.misses, after_prepare.misses);

    let prepared = database.prepare_runtime_request(request).unwrap();
    let access = crate::QueryAccessControlContext::visibility_scope(99, "space_id", "b");
    let result = database.query_prepared_request(prepared, request.with_access_control(&access));
    #[cfg(feature = "acl")]
    {
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("prepare and admit again"));
        // A different ACL changes the plan and requires its own admission.
        let scoped = request.with_access_control(&access);
        let prepared = database.prepare_runtime_request(scoped).unwrap();
        let output = database.query_prepared_request(prepared, scoped).unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["id"], Value::String("late".into()));
    }
    #[cfg(not(feature = "acl"))]
    assert!(matches!(
        result,
        Err(HawDBError::CapabilityUnavailable { .. })
    ));
}

fn sql_fixture() -> Database {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    database
        .query_sql("INSERT INTO records (id, body) VALUES (1, 'one'), (2, 'two')")
        .unwrap();
    database
}

fn assert_sql_contract(
    mut execute: impl for<'a> FnMut(SqlQueryRequest<'a>) -> Result<QueryOutput>,
) {
    let parameters = [Value::Int(1)];
    let request = SqlQueryRequest::new("SELECT id, body FROM records WHERE id >= $1 ORDER BY id")
        .with_params(&parameters)
        .with_output_limits(QueryStreamOptions {
            max_rows: Some(2),
            max_payload_bytes: Some(1_024),
        });
    let output = execute(request).unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0]["body"], Value::String("one".into()));
    assert_eq!(output.rows[1]["body"], Value::String("two".into()));
    for limits in [
        QueryStreamOptions {
            max_rows: None,
            max_payload_bytes: Some(0),
        },
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: None,
        },
        QueryStreamOptions {
            max_rows: Some(0),
            max_payload_bytes: None,
        },
        QueryStreamOptions {
            max_rows: None,
            max_payload_bytes: Some(1),
        },
    ] {
        assert!(execute(request.with_output_limits(limits)).is_err());
    }
    let stopped = RuntimeTaskContext::default();
    stopped.cancellation().cancel();
    assert!(execute(request.with_task_context(&stopped)).is_err());
    assert_eq!(execute(request).unwrap().rows, output.rows);
}

#[test]
fn sql_request_contract_survives_snapshots_and_both_transaction_modes() {
    let mut database = sql_fixture();
    assert_sql_contract(|request| database.query_sql_request(request));
    let snapshot = database.begin_read_transaction().unwrap();
    assert_sql_contract(|request| snapshot.query_sql_request(request));
    let mut transaction = database.begin_transaction().unwrap();
    assert_sql_contract(|request| transaction.query_sql_request(request));
    transaction.rollback();
    let database = ConcurrentDatabase::new(database);
    assert_sql_contract(|request| database.query_sql_request(request));
    for options in [
        ConcurrentTransactionOptions::optimistic(),
        ConcurrentTransactionOptions::default(),
    ] {
        let mut transaction = database.begin_transaction(options).unwrap();
        assert_sql_contract(|request| transaction.query_sql_request(request));
        transaction.rollback();
    }
}

fn assert_rejected_returning_is_atomic(
    mut execute: impl for<'a> FnMut(SqlQueryRequest<'a>) -> Result<QueryOutput>,
) {
    let parameters = [Value::Int(3), Value::String("three".into())];
    let request =
        SqlQueryRequest::new("INSERT INTO records (id, body) VALUES ($1, $2) RETURNING id, body")
            .with_params(&parameters);
    for limits in [
        QueryStreamOptions {
            max_rows: Some(0),
            max_payload_bytes: None,
        },
        QueryStreamOptions {
            max_rows: None,
            max_payload_bytes: Some(1),
        },
    ] {
        assert!(execute(request.with_output_limits(limits)).is_err());
        let output = execute(SqlQueryRequest::new("SELECT id FROM records WHERE id = 3")).unwrap();
        assert!(
            output.rows.is_empty(),
            "a refused result staged a partial mutation"
        );
    }
    execute(request).unwrap();
    let output = execute(SqlQueryRequest::new(
        "SELECT body FROM records WHERE id = 3",
    ))
    .unwrap();
    assert_eq!(output.rows[0]["body"], Value::String("three".into()));
}

#[test]
fn refused_sql_returning_never_publishes_or_stages_a_mutation() {
    let mut database = sql_fixture();
    assert_rejected_returning_is_atomic(|request| database.query_sql_request(request));
    let mut database = sql_fixture();
    let mut transaction = database.begin_transaction().unwrap();
    assert_rejected_returning_is_atomic(|request| transaction.query_sql_request(request));
    transaction.commit().unwrap();
    for options in [
        ConcurrentTransactionOptions::optimistic(),
        ConcurrentTransactionOptions::default(),
    ] {
        let database = ConcurrentDatabase::new(sql_fixture());
        let mut transaction = database.begin_transaction(options).unwrap();
        assert_rejected_returning_is_atomic(|request| transaction.query_sql_request(request));
        transaction.commit().unwrap();
        assert_eq!(
            database
                .query_sql("SELECT body FROM records WHERE id = 3")
                .unwrap()
                .rows
                .len(),
            1
        );
    }
}

fn graph_returning_fixture() -> Database {
    let mut database = Database::new();
    for id in [1, 2] {
        database
            .query_request(
                QueryRequest::new("CREATE (:OutputProbe {id: $id, score: 0})").with_params(
                    &std::collections::BTreeMap::from([("id".into(), Value::Int(id))]),
                ),
            )
            .unwrap();
    }
    database
}

fn assert_graph_returning_is_atomic(
    mut execute: impl for<'a> FnMut(QueryRequest<'a>) -> Result<QueryOutput>,
) {
    execute(QueryRequest::new("CREATE (:RetainedStatement {id: 1})")).unwrap();
    let parameters = std::collections::BTreeMap::from([("delta".into(), Value::Int(1))]);
    let project = "MATCH (p:OutputProbe) SET p.score = p.score + $delta RETURN p.score AS score";
    let count = "MATCH (p:OutputProbe) SET p.score = p.score + $delta RETURN count(p) AS total";
    for query in [project, count] {
        let mut limits = vec![
            (
                QueryStreamOptions {
                    max_rows: Some(0),
                    max_payload_bytes: None,
                },
                ReadBudgetResource::Rows,
                0,
            ),
            (
                QueryStreamOptions {
                    max_rows: None,
                    max_payload_bytes: Some(0),
                },
                ReadBudgetResource::PayloadBytes,
                0,
            ),
            (
                QueryStreamOptions {
                    max_rows: None,
                    max_payload_bytes: Some(12),
                },
                ReadBudgetResource::PayloadBytes,
                12,
            ),
        ];
        if query == project {
            limits.push((
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: None,
                },
                ReadBudgetResource::Rows,
                1,
            ));
        }
        for (limits, resource, limit) in limits {
            let error = execute(
                QueryRequest::new(query)
                    .with_params(&parameters)
                    .with_output_limits(limits),
            )
            .unwrap_err();
            assert!(
                matches!(&error, HawDBError::ReadBudgetExceeded(cause)
                if cause.resource == resource && cause.limit == limit),
                "{query}: {error:?}"
            );
            let output = execute(QueryRequest::new(
                "MATCH (p:OutputProbe) RETURN p.score AS score",
            ))
            .unwrap();
            assert_eq!(output.rows.len(), 2);
            assert!(
                output.rows.iter().all(|row| row["score"] == Value::Int(0)),
                "a refused RETURN changed the workspace"
            );
            assert_eq!(
                execute(QueryRequest::new(
                    "MATCH (p:RetainedStatement) RETURN p.id AS id"
                ))
                .unwrap()
                .rows
                .len(),
                1
            );
        }
    }
    let empty = execute(QueryRequest::new("MATCH (p:OutputProbe) WHERE p.id = 3 SET p.score = p.score + $delta RETURN p.score AS score")
        .with_params(&parameters).with_output_limits(QueryStreamOptions { max_rows: Some(0), max_payload_bytes: Some(0) })).unwrap();
    assert!(empty.rows.is_empty());
    let error = execute(QueryRequest::new("MATCH (p:OutputProbe) WHERE p.id = 3 SET p.score = p.score + $delta RETURN count(p) AS total")
        .with_params(&parameters).with_output_limits(QueryStreamOptions { max_rows: Some(0), max_payload_bytes: None })).unwrap_err();
    assert!(
        matches!(error, HawDBError::ReadBudgetExceeded(ref cause) if cause.resource == ReadBudgetResource::Rows && cause.limit == 0)
    );

    // The returned aggregate has one row even though two nodes are updated.
    let output = execute(
        QueryRequest::new(count)
            .with_params(&parameters)
            .with_output_limits(QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(13),
            }),
    )
    .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0]["total"], Value::Int(2));
    let output = execute(
        QueryRequest::new(project)
            .with_params(&parameters)
            .with_output_limits(QueryStreamOptions {
                max_rows: Some(2),
                max_payload_bytes: Some(26),
            }),
    )
    .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert!(output.rows.iter().all(|row| row["score"] == Value::Int(2)));
}

#[test]
fn cypher_returning_limits_reject_before_publication_or_statement_installation() {
    let mut database = graph_returning_fixture();
    assert_graph_returning_is_atomic(|request| {
        let epoch = database.commit_epoch().unwrap();
        let result = database.query_request(request);
        if result.is_err() {
            assert_eq!(database.commit_epoch().unwrap(), epoch);
        }
        result
    });
    let mut database = graph_returning_fixture();
    let mut transaction = database.begin_transaction().unwrap();
    assert_graph_returning_is_atomic(|request| transaction.query_request(request));
    transaction.commit().unwrap();
    assert_eq!(
        database
            .query("MATCH (p:RetainedStatement) RETURN p.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
    let database = ConcurrentDatabase::new(graph_returning_fixture());
    assert_graph_returning_is_atomic(|request| database.query_request(request));
    for options in [
        ConcurrentTransactionOptions::optimistic(),
        ConcurrentTransactionOptions::default(),
    ] {
        let database = ConcurrentDatabase::new(graph_returning_fixture());
        let mut transaction = database.begin_transaction(options).unwrap();
        assert_graph_returning_is_atomic(|request| transaction.query_request(request));
        transaction.commit().unwrap();
        let output = database
            .query("MATCH (p:OutputProbe) RETURN p.score AS score")
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        assert!(output.rows.iter().all(|row| row["score"] == Value::Int(2)));
        assert_eq!(
            database
                .query("MATCH (p:RetainedStatement) RETURN p.id AS id")
                .unwrap()
                .rows
                .len(),
            1
        );
    }
}

#[test]
fn embedded_cypher_returning_keeps_admission_and_transaction_limits() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-returning-contract-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut embedded =
        HawDBEmbedded::open_with_options(HawDBEmbeddedOpenOptions::new(&path)).unwrap();
    *embedded.database_mut() = graph_returning_fixture();
    assert_graph_returning_is_atomic(|request| {
        embedded
            .query_request_admitted(request)
            .map_err(|error| match error {
                crate::EmbeddedQueryError::Database(error) => error,
                error => HawDBError::Execution(error.to_string()),
            })
    });
    *embedded.database_mut() = graph_returning_fixture();
    embedded.begin_transaction().unwrap();
    assert_graph_returning_is_atomic(|request| embedded.transaction_query_request(request));
    embedded.commit_transaction().unwrap();
    drop(embedded);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn cypher_returning_refusal_remains_atomic_after_reopen() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-returning-reopen-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    {
        let mut database = Database::open(&path).unwrap();
        database
            .query("CREATE (:OutputProbe {id: 1, score: 0})")
            .unwrap();
        database
            .query("CREATE (:OutputProbe {id: 2, score: 0})")
            .unwrap();
        let mut transaction = database.begin_transaction().unwrap();
        assert_graph_returning_is_atomic(|request| transaction.query_request(request));
        transaction.commit().unwrap();
        let epoch = database.commit_epoch().unwrap();
        let error = database
            .query_request(
                QueryRequest::new("MATCH (p:OutputProbe) SET p.score = 99 RETURN p.score AS score")
                    .with_output_limits(QueryStreamOptions {
                        max_rows: None,
                        max_payload_bytes: Some(0),
                    }),
            )
            .unwrap_err();
        assert!(matches!(error, HawDBError::ReadBudgetExceeded(_)));
        assert_eq!(database.commit_epoch().unwrap(), epoch);
    }
    {
        let mut database = Database::open(&path).unwrap();
        let output = database
            .query("MATCH (p:OutputProbe) RETURN p.score AS score")
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        assert!(output.rows.iter().all(|row| row["score"] == Value::Int(2)));
        assert_eq!(
            database
                .query("MATCH (p:RetainedStatement) RETURN p.id AS id")
                .unwrap()
                .rows
                .len(),
            1
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn request_context_cannot_replace_snapshot_or_transaction_cancellation() {
    for cancel_parent in [false, true] {
        let mut database = sql_fixture();
        database.query("CREATE (:Probe {id: 1})").unwrap();
        let parent = RuntimeTaskContext::default();
        let request = RuntimeTaskContext::default();
        let mut snapshot = database
            .begin_read_transaction_with_context(&parent)
            .unwrap();
        let mut transaction = database.begin_transaction_with_context(&parent).unwrap();
        if cancel_parent {
            parent.cancellation().cancel();
        } else {
            request.cancellation().cancel();
        }
        let graph =
            QueryRequest::new("MATCH (p:Probe) RETURN p.id AS value").with_task_context(&request);
        let sql = SqlQueryRequest::new("SELECT id FROM records").with_task_context(&request);
        assert!(snapshot.query_request(graph).is_err());
        assert!(snapshot.query_sql_request(sql).is_err());
        assert!(transaction.query_request(graph).is_err());
        assert!(transaction.query_sql_request(sql).is_err());
        if !cancel_parent {
            assert!(!parent.cancellation().is_cancelled());
            assert_eq!(
                snapshot
                    .query_request(QueryRequest::new("MATCH (p:Probe) RETURN p.id AS value"))
                    .unwrap()
                    .rows
                    .len(),
                1
            );
        }
        transaction.rollback();
    }
}

#[test]
fn empty_scored_results_declare_projection_and_appended_score_columns() {
    let mut database = tests::fixture();
    let parameters = tests::parameters("missing");
    let scoring = tests::scoring(ScoringCombination::WeightedSum, 1.0, 0.0);
    let request = QueryRequest::new(tests::QUERY)
        .with_params(&parameters)
        .with_scoring(&scoring)
        .with_output_limits(QueryStreamOptions {
            max_rows: Some(0),
            max_payload_bytes: Some(0),
        });
    for _ in 0..2 {
        let output = database.query_request(request).unwrap();
        assert!(output.rows.is_empty());
        assert_eq!(
            output.schema().columns(),
            [
                "id",
                "score",
                "importance",
                "created",
                "scoring_rerank_score"
            ]
        );
    }
}
