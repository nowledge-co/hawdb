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

const MEMORY_CRYSTAL_SYNTHESIS_COUNT_QUERY: &str =
    "MATCH (c:Memory)-[r:SYNTHESIZED_FROM]->(m:Memory) \
     WHERE m.id IN $memory_ids AND c.is_crystal = true \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, count(r) AS crystal_count \
     ORDER BY memory_id ASC, memory_node_id ASC LIMIT $limit";

#[test]
fn memory_crystal_synthesis_counts_use_one_fixed_bounded_query() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'decay-base-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'decay-base-b'})").unwrap();
    db.query("CREATE (:Memory {id: 'decay-crystal-a', is_crystal: true})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'decay-crystal-b', is_crystal: true})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'decay-non-crystal', is_crystal: false})")
        .unwrap();
    db.query("CREATE (:Source {id: 'decay-source-skip', is_crystal: true})")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'decay-crystal-a'}), (m:Memory {id: 'decay-base-a'}) CREATE (c)-[:SYNTHESIZED_FROM]->(m)")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'decay-crystal-b'}), (m:Memory {id: 'decay-base-a'}) CREATE (c)-[:SYNTHESIZED_FROM]->(m)")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'decay-non-crystal'}), (m:Memory {id: 'decay-base-a'}) CREATE (c)-[:SYNTHESIZED_FROM]->(m)")
        .unwrap();
    db.query("MATCH (s:Source {id: 'decay-source-skip'}), (m:Memory {id: 'decay-base-a'}) CREATE (s)-[:SYNTHESIZED_FROM]->(m)")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'decay-crystal-a'}), (m:Memory {id: 'decay-base-b'}) CREATE (c)-[:SYNTHESIZED_FROM]->(m)")
        .unwrap();
    let parameters = BTreeMap::from([
        (
            "memory_ids".to_string(),
            Value::List(vec![
                Value::String("decay-base-a".to_string()),
                Value::String("decay-base-b".to_string()),
                Value::String("missing".to_string()),
            ]),
        ),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    db.query("MATCH (c:Memory {id: 'decay-crystal-b'}), (m:Memory {id: 'decay-base-b'}) CREATE (c)-[:SYNTHESIZED_FROM]->(m)")
        .unwrap();

    let first = snapshot
        .query_with_params_bounded(MEMORY_CRYSTAL_SYNTHESIS_COUNT_QUERY, &parameters, Some(2))
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(MEMORY_CRYSTAL_SYNTHESIS_COUNT_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("memory_id"),
        Some(&Value::String("decay-base-a".to_string()))
    );
    assert_eq!(first.rows[0].get("crystal_count"), Some(&Value::Int(2)));
    assert_eq!(
        first.rows[1].get("memory_id"),
        Some(&Value::String("decay-base-b".to_string()))
    );
    assert_eq!(first.rows[1].get("crystal_count"), Some(&Value::Int(1)));
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn memory_crystal_synthesis_counts_respect_query_limit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'base-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'base-b'})").unwrap();
    db.query("CREATE (:Memory {id: 'crystal', is_crystal: true})")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'crystal'}), (m:Memory {id: 'base-a'}) CREATE (c)-[:SYNTHESIZED_FROM]->(m)")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'crystal'}), (m:Memory {id: 'base-b'}) CREATE (c)-[:SYNTHESIZED_FROM]->(m)")
        .unwrap();
    let parameters = BTreeMap::from([
        (
            "memory_ids".to_string(),
            Value::List(vec![
                Value::String("base-a".to_string()),
                Value::String("base-b".to_string()),
            ]),
        ),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    let output = snapshot
        .query_with_params_bounded(MEMORY_CRYSTAL_SYNTHESIS_COUNT_QUERY, &parameters, Some(1))
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("memory_id"),
        Some(&Value::String("base-a".to_string()))
    );
}
