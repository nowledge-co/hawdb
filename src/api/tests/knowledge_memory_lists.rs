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

const MEMORY_DETAIL_PAGE_QUERY: &str = "MATCH (m:Memory) \
     WHERE m.id IN $memory_ids \
       AND (m.space_id IS NULL OR m.space_id = '' OR m.space_id = $space_id) \
       AND m.unit_type = $unit_type AND m.is_latest = $is_latest \
       AND m.is_crystal = $is_crystal \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, m.title AS title, \
       m.content AS content, m.metadata AS metadata, m.space_id AS space_id, \
       m.created_at AS created_at, m.updated_at AS updated_at, \
       m.importance AS importance, m.pagerank_score AS pagerank_score, \
       COALESCE(m.pagerank_score, m.importance, 0.5) AS score \
     ORDER BY score DESC, created_at DESC, memory_id ASC LIMIT $limit";

const MEMORY_FEATURE_PAGE_QUERY: &str = "MATCH (m:Memory) \
     WHERE m.unit_type = $unit_type AND m.is_latest = true AND m.is_crystal = false \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, m.title AS title, \
       m.future_field AS future_field, m.space_id AS space_id, \
       m.created_at AS created_at \
     ORDER BY created_at DESC, memory_id ASC LIMIT $limit";

#[test]
fn memory_detail_page_uses_one_fixed_bounded_query() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'memory-a', title: 'Alpha', content: 'A', metadata: '{\"rank\":1}', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: '', created_at: 10, importance: 0.4})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-b', title: 'Beta', content: 'B', metadata: '{\"rank\":2}', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: 'default', created_at: 20, pagerank_score: 0.9})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-team', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: 'team', created_at: 30})")
        .unwrap();
    let parameters = BTreeMap::from([
        (
            "memory_ids".to_string(),
            Value::List(vec![
                Value::String("memory-a".to_string()),
                Value::String("memory-b".to_string()),
                Value::String("missing".to_string()),
            ]),
        ),
        ("space_id".to_string(), Value::String("default".to_string())),
        ("unit_type".to_string(), Value::String("fact".to_string())),
        ("is_latest".to_string(), Value::Bool(true)),
        ("is_crystal".to_string(), Value::Bool(false)),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut read = db.begin_read_transaction();

    let first = read
        .query_with_params_bounded(MEMORY_DETAIL_PAGE_QUERY, &parameters, Some(2))
        .unwrap();
    let second = read
        .query_with_params_bounded(MEMORY_DETAIL_PAGE_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("memory_id"),
        Some(&Value::String("memory-b".to_string()))
    );
    assert_eq!(first.rows[0].get("score"), Some(&Value::Float(0.9)));
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 1);
}

#[test]
fn memory_business_projection_is_explicit_in_query() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory-a', title: 'Alpha', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: '', created_at: 10, future_field: 'future-a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-b', title: 'Beta', unit_type: 'fact', is_latest: true, is_crystal: false, space_id: 'team', created_at: 20, future_field: 'future-b'})")
        .unwrap();
    let parameters = BTreeMap::from([
        ("unit_type".to_string(), Value::String("fact".to_string())),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut read = db.begin_read_transaction();

    let output = read
        .query_with_params_bounded(MEMORY_FEATURE_PAGE_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("memory_id"),
        Some(&Value::String("memory-b".to_string()))
    );
    assert_eq!(
        output.rows[0].get("future_field"),
        Some(&Value::String("future-b".to_string()))
    );
    assert!(!output.rows[0].contains_key("content"));
}
