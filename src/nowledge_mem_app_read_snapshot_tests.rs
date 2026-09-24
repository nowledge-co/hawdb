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

use super::{
    NowledgeMemEmbeddedStore, NowledgeMemEmbeddedStoreHandle, NowledgeMemGraph,
    NowledgeMemGraphMode, NowledgeMemReadSnapshotBudget, NowledgeMemSearchProjection,
};
use crate::search::{SearchProjectionKind, SearchProjectionRow};
use crate::{Database, DatabaseConfig, SearchIndex, Value};
use std::collections::BTreeMap;

fn app_read_handle() -> NowledgeMemEmbeddedStoreHandle {
    let mut database = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(16),
        max_read_result_payload_bytes: Some(16 * 1024),
        ..DatabaseConfig::default()
    });
    database
        .query("CREATE (:Memory {id: 'nearest', space_id: 'default', title: 'Nearest memory'})")
        .unwrap();
    database
        .query("CREATE (:Thread {id: 'thread-1', thread_id: 'logical-1', title: 'App thread'})")
        .unwrap();
    database
        .query_sql(
            "CREATE TABLE thread_messages (\
             content_message_id TEXT PRIMARY KEY, thread_storage_id TEXT NOT NULL, \
             order_index BIGINT NOT NULL, content TEXT NOT NULL)",
        )
        .unwrap();
    database
        .query_sql_with_params(
            "INSERT INTO thread_messages \
             (content_message_id, thread_storage_id, order_index, content) \
             VALUES ($1, $2, $3, $4)",
            &[
                Value::String("message-1".to_string()),
                Value::String("thread-1".to_string()),
                Value::Int(0),
                Value::String("App-owned relational payload".to_string()),
            ],
        )
        .unwrap();

    let graph = NowledgeMemGraph::from_database(database, NowledgeMemGraphMode::WritableCutover);
    let mut index = SearchIndex::in_memory();
    for (external_id, embedding, space_id) in [
        ("nearest", vec![1.0, 0.0], "default"),
        ("farther", vec![0.0, 1.0], "other"),
    ] {
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: external_id.to_string(),
                title: external_id.to_string(),
                body: "App search candidate".to_string(),
                embedding: Some(embedding),
                source_id: None,
                metadata: BTreeMap::from([("space_id".to_string(), space_id.to_string())]),
            })
            .unwrap();
    }
    let projection = NowledgeMemSearchProjection::from_index(index);
    NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, Some(projection)))
}

#[test]
fn bounded_read_snapshot_coordinates_search_graph_and_relational_queries() {
    let handle = app_read_handle();
    let output = handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 4,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                let search = snapshot.query_cypher(
                    "CALL vector_search($embedding, topK := 2) YIELD id, score \
                     MATCH (m:Memory) WHERE m.space_id = $space_id \
                     RETURN m.id AS memory_id, score LIMIT 1",
                    &BTreeMap::from([
                        (
                            "embedding".to_string(),
                            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
                        ),
                        ("space_id".to_string(), Value::String("default".to_string())),
                    ]),
                    1,
                )?;
                let thread = snapshot.query_cypher(
                    "MATCH (t:Thread) WHERE t.thread_id = $thread_id OR t.id = $thread_id \
                     RETURN t.id AS id, t.title AS title LIMIT 1",
                    &BTreeMap::from([(
                        "thread_id".to_string(),
                        Value::String("logical-1".to_string()),
                    )]),
                    1,
                )?;
                let messages = snapshot.query_sql(
                    "SELECT content_message_id, content FROM thread_messages \
                     WHERE thread_storage_id = $1 ORDER BY order_index LIMIT $2",
                    &[Value::String("thread-1".to_string()), Value::Int(1)],
                    1,
                )?;
                Ok((search, thread, messages, snapshot.report()))
            },
        )
        .unwrap();

    assert_eq!(
        output.0.rows[0].get("memory_id"),
        Some(&Value::String("nearest".to_string()))
    );
    assert_eq!(
        output.1.rows[0].get("id"),
        Some(&Value::String("thread-1".to_string()))
    );
    assert_eq!(
        output.2.rows[0].get("content_message_id"),
        Some(&Value::String("message-1".to_string()))
    );
    assert_eq!(output.3.max_rows, 4);
    assert_eq!(output.3.max_payload_bytes, 4096);
    assert_eq!(output.3.output_rows, 3);
    assert_eq!(output.3.cypher_statement_count, 2);
    assert_eq!(output.3.sql_statement_count, 1);
    assert_eq!(output.3.vector_seed_execution_count, 1);
    assert_eq!(output.3.remaining_rows, 1);
    assert!(output.3.search_projection_present);
    assert!(output.3.output_payload_bytes > 0);
    assert!(output.3.remaining_payload_bytes < 4096);
    let evidence = output.3.json();
    let encoded_evidence = evidence.to_string();
    assert_eq!(
        evidence["protocol"],
        "hawdb-nowledge-mem-read-snapshot-report-v1"
    );
    assert_eq!(evidence["cypher_statement_count"], 2);
    assert_eq!(evidence["sql_statement_count"], 1);
    assert_eq!(evidence["vector_seed_execution_count"], 1);
    assert!(!encoded_evidence.contains("nearest"));
    assert!(!encoded_evidence.contains("thread-1"));
    assert!(!encoded_evidence.contains("App-owned relational payload"));
}

#[test]
fn bounded_read_snapshot_reports_the_thread_detail_mixed_read_shape() {
    let handle = app_read_handle();
    let report = handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 4,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                let thread = snapshot.query_cypher(
                    "MATCH (t:Thread) WHERE t.thread_id = $thread_id OR t.id = $thread_id \
                     RETURN t.id AS id, t.title AS title LIMIT 1",
                    &BTreeMap::from([(
                        "thread_id".to_string(),
                        Value::String("logical-1".to_string()),
                    )]),
                    1,
                )?;
                let storage_id = thread.rows[0]
                    .get("id")
                    .cloned()
                    .expect("thread id is projected");
                snapshot.query_sql(
                    "SELECT COUNT(*) AS total_messages FROM thread_messages \
                     WHERE thread_storage_id = $1",
                    std::slice::from_ref(&storage_id),
                    1,
                )?;
                snapshot.query_sql(
                    "SELECT content_message_id, content FROM thread_messages \
                     WHERE thread_storage_id = $1 ORDER BY order_index LIMIT $2 OFFSET $3",
                    &[storage_id, Value::Int(1), Value::Int(0)],
                    1,
                )?;
                Ok(snapshot.report())
            },
        )
        .unwrap();

    assert_eq!(report.cypher_statement_count, 1);
    assert_eq!(report.sql_statement_count, 2);
    assert_eq!(report.vector_seed_execution_count, 0);
    assert_eq!(report.max_rows, 4);
    assert_eq!(report.max_payload_bytes, 4096);
    assert_eq!(report.output_rows, 3);
    assert_eq!(report.remaining_rows, 1);
    assert!(report.output_payload_bytes > 0);
    assert!(report.remaining_payload_bytes < 4096);
}

#[test]
fn bounded_read_snapshot_counts_only_successfully_budgeted_statements() {
    let handle = app_read_handle();
    let report = handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 2,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                snapshot
                    .query_sql("SELECT missing_column FROM thread_messages", &[], 1)
                    .expect_err("invalid SQL must fail before evidence accounting");
                let after_failure = snapshot.report();
                assert_eq!(after_failure.sql_statement_count, 0);
                assert_eq!(after_failure.vector_seed_execution_count, 0);
                assert_eq!(after_failure.output_rows, 0);
                assert_eq!(after_failure.remaining_rows, 2);
                assert_eq!(after_failure.remaining_payload_bytes, 4096);

                snapshot.query_cypher(
                    "MATCH (t:Thread {id: $thread_id}) RETURN t.id AS id LIMIT 1",
                    &BTreeMap::from([(
                        "thread_id".to_string(),
                        Value::String("thread-1".to_string()),
                    )]),
                    1,
                )?;
                Ok(snapshot.report())
            },
        )
        .unwrap();

    assert_eq!(report.cypher_statement_count, 1);
    assert_eq!(report.sql_statement_count, 0);
    assert_eq!(report.vector_seed_execution_count, 0);
    assert_eq!(report.output_rows, 1);
    assert_eq!(report.remaining_rows, 1);
}

#[test]
fn bounded_read_snapshot_enforces_the_cumulative_row_budget() {
    let handle = app_read_handle();
    let error = handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 1,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                snapshot.query_cypher(
                    "MATCH (t:Thread {id: $thread_id}) RETURN t.id AS id LIMIT 1",
                    &BTreeMap::from([(
                        "thread_id".to_string(),
                        Value::String("thread-1".to_string()),
                    )]),
                    1,
                )?;
                snapshot.query_sql(
                    "SELECT content_message_id FROM thread_messages \
                     WHERE thread_storage_id = $1 LIMIT 1",
                    &[Value::String("thread-1".to_string())],
                    1,
                )?;
                Ok(())
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("exhausted max_rows"));
}

#[test]
fn bounded_read_snapshot_enforces_the_cumulative_payload_budget() {
    let handle = app_read_handle();
    let error = handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 2,
                max_payload_bytes: 12,
            },
            |snapshot| {
                snapshot.query_cypher(
                    "MATCH (t:Thread {id: $thread_id}) RETURN t.id AS id LIMIT 1",
                    &BTreeMap::from([(
                        "thread_id".to_string(),
                        Value::String("thread-1".to_string()),
                    )]),
                    1,
                )?;
                snapshot.query_sql(
                    "SELECT content_message_id FROM thread_messages \
                     WHERE thread_storage_id = $1 LIMIT 1",
                    &[Value::String("thread-1".to_string())],
                    1,
                )?;
                Ok(())
            },
        )
        .unwrap_err();

    assert!(
        error.to_string().contains("max_output_payload_bytes 2"),
        "unexpected error: {error}"
    );
}

#[test]
fn user_keeps_a_stable_graph_and_sql_snapshot_while_a_writer_commits() {
    // Given a bounded graph/SQL snapshot and a real embedded writer.
    let handle = app_read_handle();
    let writer_handle = handle.clone();
    let (start_tx, start_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let Ok(()) = start_rx.recv() else { return };
        let result = writer_handle.with_transaction(|transaction| {
            transaction.query("MATCH (t:Thread) SET t.title = 'Updated thread'")?;
            transaction.query_sql("UPDATE thread_messages SET content = 'Updated payload' WHERE content_message_id = 'message-1'")?;
            Ok(())
        });
        done_tx.send(result).unwrap();
    });
    let read = |snapshot: &mut super::NowledgeMemReadSnapshot<'_>| -> crate::Result<_> {
        let graph = snapshot.query_cypher(
            "MATCH (t:Thread) RETURN t.title AS title LIMIT 1",
            &BTreeMap::new(),
            1,
        )?;
        let sql = snapshot.query_sql("SELECT content FROM thread_messages LIMIT 1", &[], 1)?;
        Ok((graph, sql))
    };
    let result = handle.with_bounded_graph_read_snapshot(
        NowledgeMemReadSnapshotBudget {
            max_rows: 4,
            max_payload_bytes: 4096,
        },
        |snapshot| {
            let before = read(snapshot)?;
            let epoch = snapshot.commit_epoch();
            // When the writer commits before this callback returns.
            start_tx.send(()).unwrap();
            let committed = done_rx.recv_timeout(std::time::Duration::from_secs(5));
            // Return before joining on failure, so regression cannot deadlock cleanup.
            let committed = committed.map_err(|_| {
                crate::HawDBError::Execution("writer blocked behind an immutable snapshot".into())
            })?;
            committed?;
            // Then both planes retain the old values and epoch.
            assert_eq!(read(snapshot)?, before);
            assert_eq!(snapshot.commit_epoch(), epoch);
            assert!(!snapshot.report().search_projection_present);
            assert_eq!(
                snapshot
                    .report()
                    .search_projection_source_graph_commit_epoch,
                None
            );
            Ok((before, epoch))
        },
    );
    drop(start_tx);
    writer.join().unwrap();
    let (before, epoch) = result.unwrap();
    handle
        .with_bounded_graph_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 4,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                let after = read(snapshot)?;
                assert_ne!(after.0, before.0);
                assert_ne!(after.1, before.1);
                assert!(snapshot.commit_epoch() > epoch);
                Ok(())
            },
        )
        .unwrap();
}

#[test]
#[cfg(feature = "vector-search")]
fn user_cannot_read_an_unpinned_projection_from_a_graph_snapshot() {
    let handle = app_read_handle();
    let query = "CALL vector_search($embedding, topK := 2) YIELD id, score \
                 MATCH (m:Memory) WHERE m.space_id = $space_id \
                 RETURN m.id AS memory_id, score LIMIT 1";
    let parameters = BTreeMap::from([
        (
            "embedding".into(),
            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
        ),
        ("space_id".into(), Value::String("default".into())),
    ]);
    let budget = NowledgeMemReadSnapshotBudget {
        max_rows: 4,
        max_payload_bytes: 4096,
    };
    // Given a valid vector query against the configured projection.
    handle
        .with_bounded_read_snapshot(budget, |snapshot| {
            assert_eq!(snapshot.query_cypher(query, &parameters, 1)?.rows.len(), 1);
            Ok(())
        })
        .unwrap();
    // When the same query uses a graph-only snapshot, the projection is unavailable.
    handle
        .with_bounded_graph_read_snapshot(budget, |snapshot| {
            let error = snapshot.query_cypher(query, &parameters, 1).unwrap_err();
            assert!(matches!(error, crate::HawDBError::Storage(ref message)
            if message == "nowledge mem search projection is not configured"));
            assert_eq!(
                snapshot
                    .report()
                    .search_projection_source_graph_commit_epoch,
                None
            );
            assert_eq!(
                snapshot
                    .report()
                    .search_projection_durable_source_graph_commit_epoch,
                None
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn user_retains_cumulative_budgets_without_a_projection_lock() {
    let handle = app_read_handle();
    handle
        .with_bounded_graph_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 1,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                snapshot.query_cypher(
                    "MATCH (m:Memory) RETURN m.id AS value LIMIT 1",
                    &BTreeMap::new(),
                    1,
                )?;
                assert!(snapshot
                    .query_sql("SELECT content FROM thread_messages LIMIT 1", &[], 1)
                    .is_err());
                assert_eq!(snapshot.report().output_rows, 1);
                assert_eq!(snapshot.report().remaining_rows, 0);
                Ok(())
            },
        )
        .unwrap();
}

#[test]
fn user_read_transaction_allows_a_writer_to_commit_before_it_finishes() {
    let handle = app_read_handle();
    let writer_handle = handle.clone();
    let (start_tx, start_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let Ok(()) = start_rx.recv() else { return };
        let result = writer_handle.with_transaction(|transaction| {
            transaction.query("MATCH (t:Thread) SET t.title = 'Updated thread'")?;
            Ok(())
        });
        done_tx.send(result).unwrap();
    });
    let result = handle.with_read_transaction(4096, |snapshot| {
        let query = "MATCH (t:Thread) RETURN t.title AS title LIMIT 1";
        let before = snapshot.query(query)?;
        start_tx.send(()).unwrap();
        done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .map_err(|_| {
                crate::HawDBError::Execution("writer blocked by read transaction".into())
            })??;
        assert_eq!(snapshot.query(query)?, before);
        Ok(())
    });
    drop(start_tx);
    writer.join().unwrap();
    result.unwrap();
}
