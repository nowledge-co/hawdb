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

const MEMORY_EVOLVES_RELATION_COUNT_QUERY: &str = "MATCH (m:Memory)-[r:EVOLVES]->(n:Memory) \
     WHERE m.id IN $memory_ids AND r.content_relation IN $content_relations \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, count(r) AS relation_count \
     ORDER BY memory_id ASC, memory_node_id ASC LIMIT $limit";

#[test]
fn memory_evolves_relation_counts_use_one_fixed_bounded_query() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'decay-source-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'decay-source-b'})").unwrap();
    db.query("CREATE (:Memory {id: 'decay-target-confirm'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'decay-target-enrich'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'decay-target-ignore'})")
        .unwrap();
    db.query("CREATE (:Source {id: 'decay-not-memory'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'decay-source-a'}), (n:Memory {id: 'decay-target-confirm'}) CREATE (m)-[:EVOLVES {content_relation: 'confirms'}]->(n)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'decay-source-a'}), (n:Memory {id: 'decay-target-enrich'}) CREATE (m)-[:EVOLVES {content_relation: 'enriches'}]->(n)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'decay-source-a'}), (n:Memory {id: 'decay-target-ignore'}) CREATE (m)-[:EVOLVES {content_relation: 'contradicts'}]->(n)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'decay-source-a'}), (s:Source {id: 'decay-not-memory'}) CREATE (m)-[:EVOLVES {content_relation: 'confirms'}]->(s)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'decay-source-b'}), (n:Memory {id: 'decay-target-confirm'}) CREATE (m)-[:EVOLVES {content_relation: 'confirms'}]->(n)")
        .unwrap();
    let parameters = BTreeMap::from([
        (
            "memory_ids".to_string(),
            Value::List(vec![
                Value::String("decay-source-a".to_string()),
                Value::String("decay-source-b".to_string()),
                Value::String("missing".to_string()),
            ]),
        ),
        (
            "content_relations".to_string(),
            Value::List(vec![
                Value::String("confirms".to_string()),
                Value::String("enriches".to_string()),
            ]),
        ),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    db.query("MATCH (m:Memory {id: 'decay-source-b'}), (n:Memory {id: 'decay-target-enrich'}) CREATE (m)-[:EVOLVES {content_relation: 'enriches'}]->(n)")
        .unwrap();

    let first = snapshot
        .query_with_params_bounded(MEMORY_EVOLVES_RELATION_COUNT_QUERY, &parameters, Some(2))
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(MEMORY_EVOLVES_RELATION_COUNT_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("memory_id"),
        Some(&Value::String("decay-source-a".to_string()))
    );
    assert_eq!(first.rows[0].get("relation_count"), Some(&Value::Int(2)));
    assert_eq!(
        first.rows[1].get("memory_id"),
        Some(&Value::String("decay-source-b".to_string()))
    );
    assert_eq!(first.rows[1].get("relation_count"), Some(&Value::Int(1)));
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn memory_evolves_relation_counts_respect_query_limit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'source-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'source-b'})").unwrap();
    db.query("CREATE (:Memory {id: 'target'})").unwrap();
    db.query("MATCH (m:Memory {id: 'source-a'}), (n:Memory {id: 'target'}) CREATE (m)-[:EVOLVES {content_relation: 'confirms'}]->(n)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'source-b'}), (n:Memory {id: 'target'}) CREATE (m)-[:EVOLVES {content_relation: 'confirms'}]->(n)")
        .unwrap();
    let parameters = BTreeMap::from([
        (
            "memory_ids".to_string(),
            Value::List(vec![
                Value::String("source-a".to_string()),
                Value::String("source-b".to_string()),
            ]),
        ),
        (
            "content_relations".to_string(),
            Value::List(vec![Value::String("confirms".to_string())]),
        ),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    let output = snapshot
        .query_with_params_bounded(MEMORY_EVOLVES_RELATION_COUNT_QUERY, &parameters, Some(1))
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("memory_id"),
        Some(&Value::String("source-a".to_string()))
    );
}
