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

const MEMORY_CLEANUP_FINGERPRINT_QUERY: &str = "MATCH (m:Memory) \
     WHERE m.id IN $memory_ids \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, m.title AS title, \
       m.metadata AS metadata, m.is_latest AS is_latest, \
       m.decay_score_cached AS decay_score_cached, m.created_at AS created_at, \
       m.last_accessed_at AS last_accessed_at, \
       m.last_clicked_at AS last_clicked_at, m.access_count AS access_count, \
       m.appearances AS appearances, m.clicks AS clicks, \
       m.total_dwell_time_ms AS total_dwell_time_ms, \
       m.importance AS importance, m.unit_type AS unit_type, \
       m.semantic_field AS semantic_field, \
       m.future_cleanup_field AS future_cleanup_field \
     ORDER BY memory_id ASC LIMIT $limit";

#[test]
fn memory_cleanup_fingerprints_use_one_fixed_bounded_query() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'cleanup-fingerprint-a', title: 'Cleanup A', metadata: '{\"state\":\"active\"}', is_latest: true, decay_score_cached: 0.6, created_at: 10, last_accessed_at: 11, last_clicked_at: 12, access_count: 4, appearances: 1, clicks: 2, total_dwell_time_ms: 300, importance: 0.8, unit_type: 'fact', semantic_field: 'cleanup text', future_cleanup_field: 'future-a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'cleanup-fingerprint-b', title: 'Cleanup B', decay_score_cached: 0.2, created_at: 20, future_cleanup_field: 'future-b'})")
        .unwrap();
    let parameters = BTreeMap::from([
        (
            "memory_ids".to_string(),
            Value::List(vec![
                Value::String("cleanup-fingerprint-b".to_string()),
                Value::String("cleanup-fingerprint-a".to_string()),
                Value::String("missing".to_string()),
            ]),
        ),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    db.query("MATCH (m:Memory {id: 'cleanup-fingerprint-a'}) SET m.decay_score_cached = 0.9, m.future_cleanup_field = 'late-a'")
        .unwrap();

    let first = snapshot
        .query_with_params_bounded(MEMORY_CLEANUP_FINGERPRINT_QUERY, &parameters, Some(2))
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(MEMORY_CLEANUP_FINGERPRINT_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("memory_id"),
        Some(&Value::String("cleanup-fingerprint-a".to_string()))
    );
    assert_eq!(
        first.rows[0].get("decay_score_cached"),
        Some(&Value::Float(0.6))
    );
    assert_eq!(
        first.rows[0].get("future_cleanup_field"),
        Some(&Value::String("future-a".to_string()))
    );
    assert_eq!(
        first.rows[1].get("memory_id"),
        Some(&Value::String("cleanup-fingerprint-b".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn memory_cleanup_fingerprints_respect_query_limit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'cleanup-fingerprint-a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'cleanup-fingerprint-b'})")
        .unwrap();
    let parameters = BTreeMap::from([
        (
            "memory_ids".to_string(),
            Value::List(vec![
                Value::String("cleanup-fingerprint-a".to_string()),
                Value::String("cleanup-fingerprint-b".to_string()),
            ]),
        ),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    let output = snapshot
        .query_with_params_bounded(MEMORY_CLEANUP_FINGERPRINT_QUERY, &parameters, Some(1))
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("memory_id"),
        Some(&Value::String("cleanup-fingerprint-a".to_string()))
    );
}
