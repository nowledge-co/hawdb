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
fn merge_relationship_is_idempotent() {
    let mut db = Database::new();
    let first = db
            .query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 3}]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
    let second = db
            .query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 3}]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();

    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(first.rows[0].get("rel_id"), second.rows[0].get("rel_id"));

    let output = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE e.id = 10 RETURN m.title AS memory, e.name AS entity",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
}

#[test]
fn merge_relationship_reuses_existing_endpoint_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Existing memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 10, name: 'Existing entity'})")
        .unwrap();

    let output = db
            .query(
                "MERGE (:Memory {id: 1, title: 'Existing memory'})-[:MENTIONS]->(:Entity {id: 10, name: 'Existing entity'})",
            )
            .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    let memories = db.query("MATCH (m:Memory) RETURN m.id AS id").unwrap();
    assert_eq!(memories.rows.len(), 1);
    let entities = db.query("MATCH (e:Entity) RETURN e.id AS id").unwrap();
    assert_eq!(entities.rows.len(), 1);
    let rels = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
        .unwrap();
    assert_eq!(rels.rows.len(), 1);
}

#[test]
fn create_relationship_between_matched_nodes_covers_source_provenance_write() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Memory'})")
        .unwrap();
    db.query("CREATE (:Source {id: 's1', memory_count: 0})")
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) CREATE (m)-[:SOURCED_FROM {chunk_index: $chunk_index}]->(s)",
                &BTreeMap::from([
                    ("memory_id".to_string(), Value::String("m1".to_string())),
                    ("source_id".to_string(), Value::String("s1".to_string())),
                    ("chunk_index".to_string(), Value::Int(7)),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);

    let rels = db
            .query(
                "MATCH (m:Memory {id: 'm1'})-[r:SOURCED_FROM]->(s:Source {id: 's1'}) RETURN count(r) AS total, min(r.chunk_index) AS first_chunk",
            )
            .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
    assert_eq!(rels.rows[0].get("first_chunk"), Some(&Value::Int(7)));
}

#[test]
fn create_relationship_between_where_matched_nodes_covers_evolves_write() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'older', title: 'Older'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'newer', title: 'Newer'})")
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (a:Memory), (b:Memory) WHERE a.id = $older_id AND b.id = $newer_id CREATE (a)-[:EVOLVES {content_relation: $relation}]->(b)",
                &BTreeMap::from([
                    ("older_id".to_string(), Value::String("older".to_string())),
                    ("newer_id".to_string(), Value::String("newer".to_string())),
                    ("relation".to_string(), Value::String("replaces".to_string())),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);

    let rels = db
            .query(
                "MATCH (a:Memory {id: 'older'})-[r:EVOLVES]->(b:Memory {id: 'newer'}) RETURN count(r) AS total, min(r.content_relation) AS relation",
            )
            .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
    assert_eq!(
        rels.rows[0].get("relation"),
        Some(&Value::String("replaces".to_string()))
    );

    let missing = db
            .query_with_params(
                "MATCH (a:Memory), (b:Memory) WHERE a.id = $older_id AND b.id = $newer_id CREATE (a)-[:EVOLVES]->(b)",
                &BTreeMap::from([
                    ("older_id".to_string(), Value::String("older".to_string())),
                    ("newer_id".to_string(), Value::String("missing".to_string())),
                ]),
            )
            .unwrap();
    assert!(missing.rows.is_empty());
    let rels = db
        .query("MATCH (:Memory)-[r:EVOLVES]->(:Memory) RETURN count(r) AS total")
        .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
}

#[test]
fn merge_relationship_between_matched_nodes_on_create_set_writes_only_when_created() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Memory'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'l1', name: 'Important'})")
        .unwrap();

    let first = db
            .query_with_params(
                "MATCH (m:Memory {id: $memory_id}), (l:Label {id: $label_id}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = $assigned_by, r.properties = '{}'",
                &BTreeMap::from([
                    ("memory_id".to_string(), Value::String("m1".to_string())),
                    ("label_id".to_string(), Value::String("l1".to_string())),
                    ("assigned_by".to_string(), Value::String("system".to_string())),
                ]),
            )
            .unwrap();
    let second = db
            .query_with_params(
                "MATCH (m:Memory {id: $memory_id}), (l:Label {id: $label_id}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = 'overwritten'",
                &BTreeMap::from([
                    ("memory_id".to_string(), Value::String("m1".to_string())),
                    ("label_id".to_string(), Value::String("l1".to_string())),
                ]),
            )
            .unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(first.rows[0].get("rel_id"), second.rows[0].get("rel_id"));

    let rels = db
            .query(
                "MATCH (m:Memory {id: 'm1'})-[r:HAS_LABEL]->(l:Label {id: 'l1'}) RETURN count(r) AS total, min(r.assigned_by) AS assigned_by, min(r.properties) AS properties",
            )
            .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
    assert_eq!(
        rels.rows[0].get("assigned_by"),
        Some(&Value::String("system".to_string()))
    );
    assert_eq!(
        rels.rows[0].get("properties"),
        Some(&Value::String("{}".to_string()))
    );
}

#[test]
fn two_node_match_return_covers_source_provenance_existence_check() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Memory'})")
        .unwrap();
    db.query("CREATE (:Source {id: 's1', memory_count: 0})")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) RETURN count(m)",
            &BTreeMap::from([
                ("memory_id".to_string(), Value::String("m1".to_string())),
                ("source_id".to_string(), Value::String("s1".to_string())),
            ]),
        )
        .unwrap();
    assert_eq!(output.rows[0].get("count(m)"), Some(&Value::Int(1)));

    let missing = db
        .query_with_params(
            "MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) RETURN count(m)",
            &BTreeMap::from([
                ("memory_id".to_string(), Value::String("m1".to_string())),
                (
                    "source_id".to_string(),
                    Value::String("missing".to_string()),
                ),
            ]),
        )
        .unwrap();
    assert_eq!(missing.rows[0].get("count(m)"), Some(&Value::Int(0)));
}

#[test]
fn consecutive_two_node_match_return_covers_entity_endpoint_check() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'source', name: 'Source'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'target', name: 'Target'})")
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id",
                &BTreeMap::from([
                    (
                        "source_entity_id".to_string(),
                        Value::String("source".to_string()),
                    ),
                    (
                        "target_entity_id".to_string(),
                        Value::String("target".to_string()),
                    ),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("source.id"),
        Some(&Value::String("source".to_string()))
    );
    assert_eq!(
        output.rows[0].get("target.id"),
        Some(&Value::String("target".to_string()))
    );

    let missing = db
            .query_with_params(
                "MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id",
                &BTreeMap::from([
                    (
                        "source_entity_id".to_string(),
                        Value::String("source".to_string()),
                    ),
                    (
                        "target_entity_id".to_string(),
                        Value::String("missing".to_string()),
                    ),
                ]),
            )
            .unwrap();
    assert!(missing.rows.is_empty());
}

#[test]
fn matched_relationship_create_uses_one_wal_batch() {
    let path = unique_test_dir("matched_relationship_create_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'm1'})").unwrap();
        db.query("CREATE (:Source {id: 's1'})").unwrap();
        db.query("MATCH (m:Memory {id: 'm1'}), (s:Source {id: 's1'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 0}]->(s)")
                .unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 4);
    assert_eq!(wal.matches("create_node").count(), 2);
    assert_eq!(wal.matches("create_rel").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (:Memory)-[r:SOURCED_FROM]->(:Source) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn merge_relationship_existing_pattern_does_not_write_wal() {
    let path = unique_test_dir("merge_relationship_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
        db.query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 2);
    assert_eq!(wal.matches("create_node").count(), 2);
    assert_eq!(wal.matches("create_rel").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}
