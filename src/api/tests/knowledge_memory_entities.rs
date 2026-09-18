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

const MEMORY_ENTITIES_QUERY: &str = "MATCH (m:Memory {id: $memory_id})-[r:MENTIONS]->(e:Entity) \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, e.id AS entity_id, \
       id(e) AS entity_node_id, e.name AS entity_name, \
       e.entity_type AS entity_type, e.confidence AS entity_confidence, \
       id(r) AS relationship_id, r.confidence AS relationship_confidence, \
       r.mention_count AS mention_count \
     ORDER BY entity_name ASC, entity_id ASC, relationship_id ASC LIMIT $limit";

#[test]
fn memory_entities_use_one_fixed_bounded_query_per_memory() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'memory'})").unwrap();
    db.query(
        "CREATE (:Entity {id: 'entity-b', name: 'Beta', entity_type: 'concept', confidence: 0.7})",
    )
    .unwrap();
    db.query(
        "CREATE (:Entity {id: 'entity-a', name: 'Alpha', entity_type: 'person', confidence: 0.9})",
    )
    .unwrap();
    db.query("CREATE (:Source {id: 'not-entity', name: 'Ignored'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory'}), (e:Entity {id: 'entity-b'}) CREATE (m)-[:MENTIONS {confidence: 0.42, mention_count: 2}]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory'}), (e:Entity {id: 'entity-a'}) CREATE (m)-[:MENTIONS {confidence: 0.84, mention_count: 3}]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory'}), (s:Source {id: 'not-entity'}) CREATE (m)-[:MENTIONS]->(s)")
        .unwrap();
    let parameters = BTreeMap::from([
        ("memory_id".to_string(), Value::String("memory".to_string())),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    db.query("CREATE (:Entity {id: 'entity-c', name: 'Gamma'})")
        .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory'}), (e:Entity {id: 'entity-c'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();

    let first = snapshot
        .query_with_params_bounded(MEMORY_ENTITIES_QUERY, &parameters, Some(2))
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(MEMORY_ENTITIES_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("entity_id"),
        Some(&Value::String("entity-a".to_string()))
    );
    assert_eq!(
        first.rows[0].get("entity_name"),
        Some(&Value::String("Alpha".to_string()))
    );
    assert_eq!(
        first.rows[0].get("entity_confidence"),
        Some(&Value::Float(0.9))
    );
    assert_eq!(
        first.rows[0].get("relationship_confidence"),
        Some(&Value::Float(0.84))
    );
    assert_eq!(first.rows[0].get("mention_count"), Some(&Value::Int(3)));
    assert_eq!(
        first.rows[1].get("entity_id"),
        Some(&Value::String("entity-b".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn memory_entities_missing_memory_returns_no_rows() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory'})").unwrap();
    let parameters = BTreeMap::from([
        (
            "memory_id".to_string(),
            Value::String("missing".to_string()),
        ),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    let output = snapshot
        .query_with_params_bounded(MEMORY_ENTITIES_QUERY, &parameters, Some(1))
        .unwrap();

    assert!(output.rows.is_empty());
}
