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

const CONTEXT_MEMORY_TITLE_PREVIEW_QUERY: &str = "MATCH (m:Memory) \
     WHERE m.unit_type IN $unit_types \
       AND (m.is_latest IS NULL OR m.is_latest = true) \
       AND (m.is_crystal IS NULL OR m.is_crystal = false) \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, m.title AS title, \
       m.unit_type AS unit_type, m.created_at AS created_at \
     ORDER BY created_at DESC, memory_id ASC, memory_node_id ASC LIMIT $limit";

const CONTEXT_MEMORY_LABEL_PREVIEW_QUERY: &str = "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) \
     WHERE m.unit_type IN $unit_types AND m.is_latest = true \
       AND (m.is_crystal IS NULL OR m.is_crystal = false) \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, m.title AS title, \
       m.unit_type AS unit_type, m.created_at AS created_at, l.id AS label_id, \
       id(l) AS label_node_id, l.canonical_name AS label_canonical_name, \
       l.name AS label_name \
     ORDER BY created_at DESC, memory_id ASC, memory_node_id ASC, \
       label_canonical_name ASC, label_name ASC, label_id ASC, label_node_id ASC \
     LIMIT $limit";

fn context_preview_parameters(limit: i64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "unit_types".to_string(),
            Value::List(vec![Value::String("context-preview".to_string())]),
        ),
        ("limit".to_string(), Value::Int(limit)),
    ])
}

#[test]
fn context_memory_title_preview_uses_one_fixed_bounded_query() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'older', title: 'Older', unit_type: 'context-preview', is_crystal: false, created_at: 1000})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'newer', title: 'Newer', unit_type: 'context-preview', is_latest: true, is_crystal: false, created_at: 2000})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'stale', unit_type: 'context-preview', is_latest: false, is_crystal: false, created_at: 3000})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'crystal', unit_type: 'context-preview', is_latest: true, is_crystal: true, created_at: 4000})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'other', unit_type: 'other', is_latest: true, is_crystal: false, created_at: 5000})")
        .unwrap();
    let parameters = context_preview_parameters(2);
    let mut snapshot = db.begin_read_transaction();

    db.query("CREATE (:Memory {id: 'late', unit_type: 'context-preview', is_latest: true, is_crystal: false, created_at: 6000})")
        .unwrap();

    let first = snapshot
        .query_with_params_bounded(CONTEXT_MEMORY_TITLE_PREVIEW_QUERY, &parameters, Some(2))
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(CONTEXT_MEMORY_TITLE_PREVIEW_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("memory_id"),
        Some(&Value::String("newer".to_string()))
    );
    assert_eq!(
        first.rows[1].get("memory_id"),
        Some(&Value::String("older".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn context_memory_label_preview_uses_distinct_fixed_query_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory', title: 'Memory', unit_type: 'context-preview', is_latest: true, is_crystal: false, created_at: 2000})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label', name: 'Label', canonical_name: 'label'})")
        .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory'}), (l:Label {id: 'label'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    let parameters = context_preview_parameters(1);
    let mut snapshot = db.begin_read_transaction();

    let output = snapshot
        .query_with_params_bounded(CONTEXT_MEMORY_LABEL_PREVIEW_QUERY, &parameters, Some(1))
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("memory_id"),
        Some(&Value::String("memory".to_string()))
    );
    assert_eq!(
        output.rows[0].get("label_id"),
        Some(&Value::String("label".to_string()))
    );
    assert_eq!(
        output.rows[0].get("label_canonical_name"),
        Some(&Value::String("label".to_string()))
    );
}
