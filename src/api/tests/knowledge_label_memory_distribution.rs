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

const LABEL_MEMORY_DISTRIBUTION_QUERY: &str = "MATCH (m:Memory)-[:HAS_LABEL]->(l:Label) \
     WITH l.id AS label_id, id(l) AS label_node_id, l.name AS label_name, \
       count(DISTINCT m) AS memory_count \
     RETURN label_id, label_node_id, label_name, memory_count \
     ORDER BY memory_count DESC, label_name ASC, label_id ASC, label_node_id ASC \
     SKIP $offset LIMIT $limit";

#[test]
fn label_memory_distribution_uses_one_fixed_bounded_query() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Label {id: 'alpha', name: 'Alpha'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'beta', name: 'Beta'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'source-only', name: 'Source only'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-one'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory-two'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory-three'})").unwrap();
    db.query("CREATE (:Source {id: 'source-one'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory-one'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory-one'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory-two'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-three'}), (l:Label {id: 'beta'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();
    db.query("MATCH (s:Source {id: 'source-one'}), (l:Label {id: 'source-only'}) CREATE (s)-[:HAS_LABEL]->(l)")
        .unwrap();
    let parameters = BTreeMap::from([
        ("offset".to_string(), Value::Int(0)),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    db.query("CREATE (:Memory {id: 'late-memory'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'late-memory'}), (l:Label {id: 'beta'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();

    let first = snapshot
        .query_with_params_bounded(LABEL_MEMORY_DISTRIBUTION_QUERY, &parameters, Some(2))
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(LABEL_MEMORY_DISTRIBUTION_QUERY, &parameters, Some(2))
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("label_id"),
        Some(&Value::String("alpha".to_string()))
    );
    assert_eq!(first.rows[0].get("memory_count"), Some(&Value::Int(2)));
    assert_eq!(
        first.rows[1].get("label_id"),
        Some(&Value::String("beta".to_string()))
    );
    assert_eq!(first.rows[1].get("memory_count"), Some(&Value::Int(1)));
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn label_memory_distribution_respects_query_offset() {
    let mut db = Database::new();
    db.query("CREATE (:Label {id: 'alpha', name: 'Alpha'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'beta', name: 'Beta'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-one'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory-two'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory-one'}), (l:Label {id: 'alpha'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory-two'}), (l:Label {id: 'beta'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    let parameters = BTreeMap::from([
        ("offset".to_string(), Value::Int(1)),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    let output = snapshot
        .query_with_params_bounded(LABEL_MEMORY_DISTRIBUTION_QUERY, &parameters, Some(1))
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("label_id"),
        Some(&Value::String("beta".to_string()))
    );
}
