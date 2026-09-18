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

const MEMORY_TITLE_CONTENT_PAGE_QUERY: &str = "MATCH (m:Memory) WHERE m.id IN $memory_ids \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, m.title AS title, \
       m.content AS content, m.created_at AS created_at \
     ORDER BY created_at ASC, memory_id ASC LIMIT $limit";

#[test]
fn memory_title_contents_use_one_fixed_bounded_query() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query(
        "CREATE (:Memory {id: 'memory-b', title: 'Memory B', content: 'body b', created_at: 20})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 'memory-a', title: 'Memory A', content: 'body a', created_at: 10})",
    )
    .unwrap();
    db.query("CREATE (:Skill {id: 'memory-a', title: 'Wrong Label'})")
        .unwrap();
    let parameters = BTreeMap::from([
        (
            "memory_ids".to_string(),
            Value::List(vec![
                Value::String("memory-b".to_string()),
                Value::String("missing".to_string()),
                Value::String("memory-a".to_string()),
            ]),
        ),
        ("limit".to_string(), Value::Int(3)),
    ]);
    let mut read = db.begin_read_transaction();
    db.query("MATCH (m:Memory {id: 'memory-a'}) SET m.title = 'Changed'")
        .unwrap();

    let first = read
        .query_with_params_bounded(MEMORY_TITLE_CONTENT_PAGE_QUERY, &parameters, Some(3))
        .unwrap();
    let second = read
        .query_with_params_bounded(MEMORY_TITLE_CONTENT_PAGE_QUERY, &parameters, Some(3))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("memory_id"),
        Some(&Value::String("memory-a".to_string()))
    );
    assert_eq!(
        first.rows[0].get("title"),
        Some(&Value::String("Memory A".to_string()))
    );
    assert_eq!(
        first.rows[0].get("content"),
        Some(&Value::String("body a".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 1);
}
