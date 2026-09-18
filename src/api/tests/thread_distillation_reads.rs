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

const THREAD_DISTILLATION_COUNT_QUERY: &str = "MATCH (t:Thread) \
     WHERE t.thread_id IS NOT NULL AND t.thread_id <> '' \
       AND (t.space_id IS NULL OR t.space_id = '' OR t.space_id = $space_id) \
       AND ($source IS NULL OR t.source = $source) \
     RETURN count(t) AS matched_count";

const THREAD_DISTILLATION_PAGE_QUERY: &str = "MATCH (t:Thread) \
     WHERE t.thread_id IS NOT NULL AND t.thread_id <> '' \
       AND (t.space_id IS NULL OR t.space_id = '' OR t.space_id = $space_id) \
       AND ($source IS NULL OR t.source = $source) \
     RETURN t.id AS id, t.thread_id AS thread_id, id(t) AS node_id, \
       t.source AS source, t.space_id AS space_id, \
       COALESCE(t.updated_at, t.import_date, t.created_at) AS recent_at \
     ORDER BY recent_at DESC, thread_id ASC, id ASC, node_id ASC \
     SKIP $offset LIMIT $limit";

#[test]
fn thread_distillation_uses_fixed_count_and_page_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Thread {id: 'thread-a', thread_id: 'logical-a', source: 'codex', space_id: '', updated_at: 40})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread-b', thread_id: 'logical-b', source: 'codex', space_id: 'default', import_date: 50})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread-c', thread_id: 'logical-c', source: 'email', space_id: 'default', created_at: 60})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread-skip', source: 'codex', space_id: 'default', updated_at: 90})")
        .unwrap();
    let parameters = BTreeMap::from([
        ("space_id".to_string(), Value::String("default".to_string())),
        ("source".to_string(), Value::String("codex".to_string())),
        ("offset".to_string(), Value::Int(0)),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut read = db.begin_read_transaction();

    let count = read
        .query_with_params_bounded(THREAD_DISTILLATION_COUNT_QUERY, &parameters, Some(1))
        .unwrap();
    assert_eq!(count.rows[0].get("matched_count"), Some(&Value::Int(2)));

    let first = read
        .query_with_params_bounded(THREAD_DISTILLATION_PAGE_QUERY, &parameters, Some(2))
        .unwrap();
    let second = read
        .query_with_params_bounded(THREAD_DISTILLATION_PAGE_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("thread_id"),
        Some(&Value::String("logical-b".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 1);
}

#[test]
fn thread_distillation_optional_source_stays_in_query() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread-a', thread_id: 'logical-a', source: 'codex', space_id: '', updated_at: 40})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread-b', thread_id: 'logical-b', source: 'email', space_id: 'default', updated_at: 50})")
        .unwrap();
    let parameters = BTreeMap::from([
        ("space_id".to_string(), Value::String("default".to_string())),
        ("source".to_string(), Value::Null),
        ("offset".to_string(), Value::Int(0)),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut read = db.begin_read_transaction();

    let output = read
        .query_with_params_bounded(THREAD_DISTILLATION_PAGE_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("thread_id"),
        Some(&Value::String("logical-b".to_string()))
    );
}
