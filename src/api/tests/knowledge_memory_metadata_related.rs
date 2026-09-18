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

const MEMORY_METADATA_RELATED_PAGE_QUERY: &str = "MATCH (m:Memory) \
     WHERE (m.space_id IS NULL OR m.space_id = '' OR m.space_id = $space_id) \
       AND (m.metadata CONTAINS $source_id_marker \
         OR m.metadata CONTAINS $source_id_compact_marker \
         OR m.metadata CONTAINS $source_thread_id_marker \
         OR m.metadata CONTAINS $source_thread_id_compact_marker) \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, m.title AS title, \
       m.future_field AS future_field, m.space_id AS space_id, \
       m.created_at AS created_at \
     ORDER BY created_at DESC, memory_id ASC LIMIT $limit";

#[test]
fn metadata_related_memories_use_one_fixed_bounded_query() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'related-old', title: 'Old', metadata: '{\"source_id\": \"thread-a\"}', space_id: '', created_at: 10, future_field: 'old'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'related-new', title: 'New', metadata: '{\"source_thread_id\":\"thread-a\"}', space_id: 'default', created_at: 20, future_field: 'new'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'wrong-space', metadata: '{\"source_id\":\"thread-a\"}', space_id: 'team', created_at: 30})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'wrong-source', metadata: '{\"source_id\":\"thread-b\"}', space_id: 'default', created_at: 40})")
        .unwrap();
    let parameters = BTreeMap::from([
        ("space_id".to_string(), Value::String("default".to_string())),
        (
            "source_id_marker".to_string(),
            Value::String("\"source_id\": \"thread-a\"".to_string()),
        ),
        (
            "source_id_compact_marker".to_string(),
            Value::String("\"source_id\":\"thread-a\"".to_string()),
        ),
        (
            "source_thread_id_marker".to_string(),
            Value::String("\"source_thread_id\": \"thread-a\"".to_string()),
        ),
        (
            "source_thread_id_compact_marker".to_string(),
            Value::String("\"source_thread_id\":\"thread-a\"".to_string()),
        ),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut read = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 'after-snapshot', metadata: '{\"source_id\":\"thread-a\"}', space_id: 'default', created_at: 50})")
        .unwrap();

    let first = read
        .query_with_params_bounded(MEMORY_METADATA_RELATED_PAGE_QUERY, &parameters, Some(2))
        .unwrap();
    let second = read
        .query_with_params_bounded(MEMORY_METADATA_RELATED_PAGE_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("memory_id"),
        Some(&Value::String("related-new".to_string()))
    );
    assert_eq!(
        first.rows[0].get("future_field"),
        Some(&Value::String("new".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 1);
}
