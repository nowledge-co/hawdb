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
    assert_eq!(output.3.output_rows, 3);
    assert_eq!(output.3.remaining_rows, 1);
    assert!(output.3.search_projection_present);
    assert!(output.3.output_payload_bytes > 0);
    assert!(output.3.remaining_payload_bytes < 4096);
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
