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

const ENTITY_DELETE_GUARD_ENTITY_QUERY: &str = "MATCH (e:Entity) \
     WHERE e.id = $entity_id \
     RETURN id(e) AS entity_node_id \
     LIMIT 1";

const ENTITY_DELETE_GUARD_OTHER_MENTIONS_QUERY: &str = "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) \
     WHERE id(e) = $entity_node_id AND m.id <> $excluded_memory_id \
     RETURN count(r) AS relationship_count";

const ENTITY_DELETE_GUARD_LABELS_QUERY: &str = "MATCH (e:Entity)-[r:HAS_LABEL]-() \
     WHERE id(e) = $entity_node_id \
     RETURN count(r) AS relationship_count";

const ENTITY_DELETE_GUARD_INCIDENT_QUERY: &str = "MATCH (e:Entity)-[r]-() \
     WHERE id(e) = $entity_node_id \
     RETURN count(r) AS relationship_count";

const ENTITY_DELETE_GUARD_INCOMING_QUERY: &str = "MATCH ()-[r]->(e:Entity) \
     WHERE id(e) = $entity_node_id \
     RETURN count(r) AS relationship_count";

fn query_delete_guard_count(
    snapshot: &mut DatabaseReadTransaction,
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> i64 {
    let output = snapshot
        .query_with_params_bounded(query, parameters, Some(1))
        .unwrap();
    match output.rows[0]["relationship_count"] {
        Value::Int(count) => count,
        ref value => panic!("aggregate count must be an integer, got {value:?}"),
    }
}

#[test]
fn entity_delete_guard_uses_fixed_queries_on_one_snapshot() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 16,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Entity {id: 'entity'})").unwrap();
    db.query("CREATE (:Memory {id: 'other-memory'})").unwrap();
    db.query("CREATE (:Memory {id: 'excluded-memory'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label'})").unwrap();
    db.query("CREATE (:Entity {id: 'out-entity'})").unwrap();
    db.query("CREATE (:Entity {id: 'in-entity'})").unwrap();
    db.query("MATCH (m:Memory {id: 'other-memory'}), (e:Entity {id: 'entity'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'excluded-memory'}), (e:Entity {id: 'entity'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query(
        "MATCH (e:Entity {id: 'entity'}), (l:Label {id: 'label'}) CREATE (e)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query("MATCH (e:Entity {id: 'entity'}), (out:Entity {id: 'out-entity'}) CREATE (e)-[:RELATES_TO]->(out)")
        .unwrap();
    db.query("MATCH (incoming:Entity {id: 'in-entity'}), (e:Entity {id: 'entity'}) CREATE (incoming)-[:RELATES_TO]->(e)")
        .unwrap();
    let mut parameters = BTreeMap::from([
        ("entity_id".to_string(), Value::String("entity".to_string())),
        (
            "excluded_memory_id".to_string(),
            Value::String("excluded-memory".to_string()),
        ),
    ]);
    let mut snapshot = db.begin_read_transaction();

    db.query("CREATE (:Memory {id: 'late-memory'})").unwrap();
    db.query("MATCH (m:Memory {id: 'late-memory'}), (e:Entity {id: 'entity'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();

    let entity = snapshot
        .query_with_params_bounded(ENTITY_DELETE_GUARD_ENTITY_QUERY, &parameters, Some(1))
        .unwrap();
    let entity_node_id = match entity.rows[0]["entity_node_id"] {
        Value::Int(node_id) => node_id,
        ref value => panic!("node id must be an integer, got {value:?}"),
    };
    parameters.insert("entity_node_id".to_string(), Value::Int(entity_node_id));

    let other_mentions = query_delete_guard_count(
        &mut snapshot,
        ENTITY_DELETE_GUARD_OTHER_MENTIONS_QUERY,
        &parameters,
    );
    let labels =
        query_delete_guard_count(&mut snapshot, ENTITY_DELETE_GUARD_LABELS_QUERY, &parameters);
    let incident = query_delete_guard_count(
        &mut snapshot,
        ENTITY_DELETE_GUARD_INCIDENT_QUERY,
        &parameters,
    );
    let incoming = query_delete_guard_count(
        &mut snapshot,
        ENTITY_DELETE_GUARD_INCOMING_QUERY,
        &parameters,
    );

    assert_eq!(other_mentions, 1);
    assert_eq!(labels, 1);
    assert_eq!(incident + incoming, 8);

    let repeated = query_delete_guard_count(
        &mut snapshot,
        ENTITY_DELETE_GUARD_INCOMING_QUERY,
        &parameters,
    );
    assert_eq!(repeated, incoming);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 5);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 5);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn entity_delete_guard_stops_after_missing_entity() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity'})").unwrap();
    let parameters = BTreeMap::from([(
        "entity_id".to_string(),
        Value::String("missing".to_string()),
    )]);
    let mut snapshot = db.begin_read_transaction();

    let entity = snapshot
        .query_with_params_bounded(ENTITY_DELETE_GUARD_ENTITY_QUERY, &parameters, Some(1))
        .unwrap();

    assert!(entity.rows.is_empty());
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 1);
}
