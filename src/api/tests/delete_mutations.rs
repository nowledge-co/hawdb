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

#[test]
fn delete_removes_node_and_property_index_entries() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Remove'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Keep'})").unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 DELETE m")
        .unwrap();
    assert_eq!(output.rows.len(), 1);

    let removed = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert!(removed.rows.is_empty());
    let by_old_index = db
        .query("MATCH (m:Memory) WHERE m.title = 'Remove' RETURN m.id AS id")
        .unwrap();
    assert!(by_old_index.rows.is_empty());
    let kept = db
        .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        kept.rows[0].get("title"),
        Some(&Value::String("Keep".to_string()))
    );
}

#[test]
fn delete_rejects_nodes_with_relationships() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();

    let error = db
        .query("MATCH (m:Memory) WHERE m.id = 1 DELETE m")
        .unwrap_err();
    assert!(error.to_string().contains("DETACH DELETE"));

    let output = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE m.id = 1 RETURN e.name AS entity")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
}

#[test]
fn detach_delete_removes_attached_relationships() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 DETACH DELETE m")
        .unwrap();
    assert_eq!(output.rows.len(), 1);

    let rels = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
        .unwrap();
    assert!(rels.rows.is_empty());
    let target = db
        .query("MATCH (e:Entity) WHERE e.id = 10 RETURN e.name AS name")
        .unwrap();
    assert_eq!(
        target.rows[0].get("name"),
        Some(&Value::String("Neo4j".to_string()))
    );
}

#[test]
fn delete_relationship_removes_edge_and_keeps_endpoint_nodes() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();

    let output = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 DELETE r")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("rel_id"), Some(&Value::Int(0)));

    let rels = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
        .unwrap();
    assert!(rels.rows.is_empty());
    let source = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        source.rows[0].get("title"),
        Some(&Value::String("Graph foundations".to_string()))
    );
    let target = db
        .query("MATCH (e:Entity) WHERE e.id = 10 RETURN e.name AS name")
        .unwrap();
    assert_eq!(
        target.rows[0].get("name"),
        Some(&Value::String("Neo4j".to_string()))
    );
}
