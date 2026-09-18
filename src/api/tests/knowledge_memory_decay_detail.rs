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

const MEMORY_DECAY_DETAIL_QUERY: &str = "MATCH (m:Memory {id: $memory_id}) \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, m.title AS title, \
       m.content AS content, m.unit_type AS unit_type, m.source AS source, \
       m.space_id AS space_id, m.created_at AS created_at, \
       m.decay_score_cached AS decay_score_cached, m.metadata AS metadata, \
       m.is_latest AS is_latest, m.lifecycle_state AS lifecycle_state, \
       m.future_decay_field AS future_decay_field \
     ORDER BY memory_node_id ASC LIMIT 1";

#[test]
fn memory_decay_detail_uses_one_fixed_bounded_query() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'scheduler-memory-decay-detail', title: 'Decay Detail', content: 'content', unit_type: 'fact', source: 'agent', space_id: 'default', created_at: 12, decay_score_cached: 0.4, metadata: '{}', is_latest: true, lifecycle_state: 'active', future_decay_field: 'future'})")
        .unwrap();
    let parameters = BTreeMap::from([(
        "memory_id".to_string(),
        Value::String("scheduler-memory-decay-detail".to_string()),
    )]);
    let mut snapshot = db.begin_read_transaction();

    db.query("MATCH (m:Memory {id: 'scheduler-memory-decay-detail'}) SET m.decay_score_cached = 0.9, m.future_decay_field = 'late'")
        .unwrap();

    let first = snapshot
        .query_with_params_bounded(MEMORY_DECAY_DETAIL_QUERY, &parameters, Some(1))
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(MEMORY_DECAY_DETAIL_QUERY, &parameters, Some(1))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 1);
    assert_eq!(
        first.rows[0].get("memory_id"),
        Some(&Value::String("scheduler-memory-decay-detail".to_string()))
    );
    assert_eq!(
        first.rows[0].get("decay_score_cached"),
        Some(&Value::Float(0.4))
    );
    assert_eq!(
        first.rows[0].get("future_decay_field"),
        Some(&Value::String("future".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn memory_decay_detail_missing_id_returns_no_rows() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'scheduler-memory-decay-detail'})")
        .unwrap();
    let parameters = BTreeMap::from([(
        "memory_id".to_string(),
        Value::String("missing".to_string()),
    )]);
    let mut snapshot = db.begin_read_transaction();

    let output = snapshot
        .query_with_params_bounded(MEMORY_DECAY_DETAIL_QUERY, &parameters, Some(1))
        .unwrap();

    assert!(output.rows.is_empty());
}
