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

const THREAD_EXISTS_QUERY: &str =
    "MATCH (t:Thread {id: $thread_id}) RETURN id(t) AS thread_node_id LIMIT 1";

const THREAD_MESSAGE_PAGE_QUERY: &str =
    "MATCH (t:Thread {id: $thread_id})-[r:CONTAINS]->(m:Message) \
     RETURN m.id AS message_id, id(m) AS message_node_id, id(r) AS relationship_id, \
       m.role AS role, m.content AS content, \
       COALESCE(r.order_index, m.order_index) AS order_index, \
       r.order_index AS relationship_order_index, m.order_index AS message_order_index, \
       m.timestamp AS timestamp, m.token_count AS token_count, \
       m.created_at AS created_at, m.updated_at AS updated_at, m.metadata AS metadata \
     ORDER BY order_index ASC, message_id ASC, relationship_id ASC LIMIT $limit";

#[test]
fn thread_messages_use_snapshot_pinned_bounded_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Thread {id: 'thread-a'})").unwrap();
    db.query("CREATE (:Message {id: 'message-a', role: 'user', content: 'first', order_index: 2})")
        .unwrap();
    db.query(
        "CREATE (:Message {id: 'message-b', role: 'assistant', content: 'second', order_index: 1})",
    )
    .unwrap();
    db.query("MATCH (t:Thread {id: 'thread-a'}), (m:Message {id: 'message-a'}) CREATE (t)-[:CONTAINS {order_index: 1}]->(m)")
        .unwrap();
    db.query("MATCH (t:Thread {id: 'thread-a'}), (m:Message {id: 'message-b'}) CREATE (t)-[:CONTAINS]->(m)")
        .unwrap();
    let identity_parameters = BTreeMap::from([(
        "thread_id".to_string(),
        Value::String("thread-a".to_string()),
    )]);
    let page_parameters = BTreeMap::from([
        (
            "thread_id".to_string(),
            Value::String("thread-a".to_string()),
        ),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut read = db.begin_read_transaction();
    db.query("MATCH (m:Message {id: 'message-a'}) SET m.content = 'changed'")
        .unwrap();

    let identity = read
        .query_with_params_bounded(THREAD_EXISTS_QUERY, &identity_parameters, Some(1))
        .unwrap();
    assert_eq!(identity.rows.len(), 1);

    let first = read
        .query_with_params_bounded(THREAD_MESSAGE_PAGE_QUERY, &page_parameters, Some(2))
        .unwrap();
    let second = read
        .query_with_params_bounded(THREAD_MESSAGE_PAGE_QUERY, &page_parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("message_id"),
        Some(&Value::String("message-a".to_string()))
    );
    assert_eq!(
        first.rows[0].get("content"),
        Some(&Value::String("first".to_string()))
    );
    assert_eq!(first.rows[0].get("order_index"), Some(&Value::Int(1)));
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 1);
}
