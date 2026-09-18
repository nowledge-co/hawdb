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

const SOURCE_REFERENCE_ENTITIES_QUERY: &str =
    "MATCH (source:Entity)-[r:RELATES_TO]->(target:Entity) \
     WHERE r.source_reference = $source_reference \
     RETURN id(r) AS relationship_id, \
     source.id AS source_entity_id, id(source) AS source_node_id, \
     target.id AS target_entity_id, id(target) AS target_node_id \
     ORDER BY relationship_id ASC";

const SOURCE_REFERENCE_ENTITY_QUERY: &str =
    "MATCH (e:Entity) WHERE e.id = $entity_id RETURN id(e) AS entity_node_id LIMIT 1";

const SOURCE_REFERENCE_INCIDENT_COUNT_QUERY: &str =
    "MATCH (e:Entity {id: $entity_id})-[r:RELATES_TO]-(other:Entity) \
     WHERE r.source_reference IS NULL OR r.source_reference = '' \
        OR r.source_reference <> $excluded_source_reference \
     RETURN count(r) AS relationship_count";

const SOURCE_REFERENCE_INCOMING_COUNT_QUERY: &str =
    "MATCH (other:Entity)-[r:RELATES_TO]->(e:Entity {id: $entity_id}) \
     WHERE r.source_reference IS NULL OR r.source_reference = '' \
        OR r.source_reference <> $excluded_source_reference \
     RETURN count(r) AS relationship_count";

#[test]
fn source_reference_entities_use_parameterized_pinned_reads() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Entity {id: 'entity_a'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_b'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_c'})").unwrap();
    db.query("MATCH (a:Entity {id: 'entity_a'}), (b:Entity {id: 'entity_b'}) CREATE (a)-[:RELATES_TO {source_reference: 'source_1'}]->(b)")
        .unwrap();
    db.query("MATCH (c:Entity {id: 'entity_c'}), (a:Entity {id: 'entity_a'}) CREATE (c)-[:RELATES_TO {source_reference: 'source_1'}]->(a)")
        .unwrap();
    let parameters = BTreeMap::from([(
        "source_reference".to_string(),
        Value::String("source_1".to_string()),
    )]);
    let mut snapshot = db.begin_read_transaction();

    let first = snapshot
        .query_with_params_bounded(SOURCE_REFERENCE_ENTITIES_QUERY, &parameters, Some(2))
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(SOURCE_REFERENCE_ENTITIES_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);

    db.query("CREATE (:Entity {id: 'entity_d'})").unwrap();
    db.query("MATCH (d:Entity {id: 'entity_d'}), (a:Entity {id: 'entity_a'}) CREATE (d)-[:RELATES_TO {source_reference: 'source_1'}]->(a)")
        .unwrap();
    let pinned = snapshot
        .query_with_params_bounded(SOURCE_REFERENCE_ENTITIES_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(pinned.rows.len(), 2);

    let mut live = db.begin_read_transaction();
    let current = live
        .query_with_params_bounded(SOURCE_REFERENCE_ENTITIES_QUERY, &parameters, Some(3))
        .unwrap();
    assert_eq!(current.rows.len(), 3);
}

#[test]
fn source_reference_delete_guard_uses_named_count_queries() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity'})").unwrap();
    db.query("CREATE (:Entity {id: 'out'})").unwrap();
    db.query("CREATE (:Entity {id: 'in-empty'})").unwrap();
    db.query("MATCH (e:Entity {id: 'entity'}), (out:Entity {id: 'out'}) CREATE (e)-[:RELATES_TO {source_reference: 'other-source'}]->(out)")
        .unwrap();
    db.query("MATCH (incoming:Entity {id: 'in-empty'}), (e:Entity {id: 'entity'}) CREATE (incoming)-[:RELATES_TO {source_reference: ''}]->(e)")
        .unwrap();
    let parameters = BTreeMap::from([
        ("entity_id".to_string(), Value::String("entity".to_string())),
        (
            "excluded_source_reference".to_string(),
            Value::String("excluded-source".to_string()),
        ),
    ]);
    let mut read = db.begin_read_transaction();

    let entity = read
        .query_with_params_bounded(SOURCE_REFERENCE_ENTITY_QUERY, &parameters, Some(1))
        .unwrap();
    assert_eq!(entity.rows.len(), 1);
    let incident = read
        .query_with_params_bounded(SOURCE_REFERENCE_INCIDENT_COUNT_QUERY, &parameters, Some(1))
        .unwrap();
    let incoming = read
        .query_with_params_bounded(SOURCE_REFERENCE_INCOMING_COUNT_QUERY, &parameters, Some(1))
        .unwrap();
    assert_eq!(
        incident.rows[0].get("relationship_count"),
        Some(&Value::Int(2))
    );
    assert_eq!(
        incoming.rows[0].get("relationship_count"),
        Some(&Value::Int(1))
    );

    let hostile_parameters = BTreeMap::from([(
        "entity_id".to_string(),
        Value::String("entity'}) MATCH (n) RETURN n //".to_string()),
    )]);
    assert!(read
        .query_with_params_bounded(SOURCE_REFERENCE_ENTITY_QUERY, &hostile_parameters, Some(1),)
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn deletes_source_reference_relationships_for_nowledge_cleanup() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_1'})-[:RELATES_TO {source_reference: 'source_1'}]->(:Entity {id: 'entity_2'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_3'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_4'})").unwrap();
    db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
        source: KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_1".to_string(),
        },
        target: KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_3".to_string(),
        },
        relationship_type: "RELATES_TO".to_string(),
        properties: BTreeMap::from([(
            "source_reference".to_string(),
            Value::String("source_1".to_string()),
        )]),
    })
    .unwrap();
    db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
        source: KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_1".to_string(),
        },
        target: KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_4".to_string(),
        },
        relationship_type: "RELATES_TO".to_string(),
        properties: BTreeMap::from([(
            "source_reference".to_string(),
            Value::String("source_2".to_string()),
        )]),
    })
    .unwrap();

    let output = db
        .delete_knowledge_source_reference_relationships(
            &KnowledgeSourceReferenceRelationshipCleanupRequest {
                source_reference: "source_1".to_string(),
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 5);
    assert_eq!(output.graph_commit_epoch_after, 6);
    assert_eq!(output.candidate_count, 2);
    assert_eq!(output.deleted_relationship_count, 2);
    assert_eq!(output.rows.len(), 2);
    assert!(output.rows.iter().all(|row| row.deleted));
    assert!(output
        .rows
        .iter()
        .all(|row| row.source_external_id.as_deref() == Some("entity_1")));

    let dropped = db
        .query("MATCH (:Entity)-[r:RELATES_TO]->(:Entity) WHERE r.source_reference = 'source_1' RETURN count(r) AS total")
        .unwrap();
    assert_eq!(dropped.rows[0].get("total"), Some(&Value::Int(0)));
    let kept = db
        .query("MATCH (:Entity)-[r:RELATES_TO]->(:Entity) WHERE r.source_reference = 'source_2' RETURN count(r) AS total")
        .unwrap();
    assert_eq!(kept.rows[0].get("total"), Some(&Value::Int(1)));
    let nodes = db
        .query("MATCH (e:Entity) RETURN count(e) AS total")
        .unwrap();
    assert_eq!(nodes.rows[0].get("total"), Some(&Value::Int(4)));
}

#[test]
fn source_reference_relationship_cleanup_rejects_empty_reference_before_wal() {
    let path = unique_test_dir("source_reference_cleanup_empty_before_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})-[:RELATES_TO {source_reference: 'source_1'}]->(:Entity {id: 'entity_2'})")
            .unwrap();
    }
    let wal_before = read_test_wal(&path).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let epoch_before = db.store.commit_epoch();
        let error = db
            .delete_knowledge_source_reference_relationships(
                &KnowledgeSourceReferenceRelationshipCleanupRequest {
                    source_reference: " ".to_string(),
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("non-empty source_reference"));
        assert_eq!(db.store.commit_epoch(), epoch_before);
    }
    let wal_after = read_test_wal(&path).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_source_reference_relationship_cleanup_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_source_reference_cleanup_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})-[:RELATES_TO {source_reference: 'source_1'}]->(:Entity {id: 'entity_2'})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'entity_3'})").unwrap();
        db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_3".to_string(),
            },
            relationship_type: "RELATES_TO".to_string(),
            properties: BTreeMap::from([(
                "source_reference".to_string(),
                Value::String("source_1".to_string()),
            )]),
        })
        .unwrap();
    }
    let setup_wal = read_test_wal(&path).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .delete_knowledge_source_reference_relationships(
                &KnowledgeSourceReferenceRelationshipCleanupRequest {
                    source_reference: "source_1".to_string(),
                },
            )
            .unwrap();
        assert_eq!(output.candidate_count, 2);
        assert_eq!(output.deleted_relationship_count, 2);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_rel"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let mut db = Database::open(&path).unwrap();
        let relationships = db
            .query("MATCH (:Entity)-[r:RELATES_TO]->(:Entity) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(relationships.rows[0].get("total"), Some(&Value::Int(0)));
        let nodes = db
            .query("MATCH (e:Entity) RETURN count(e) AS total")
            .unwrap();
        assert_eq!(nodes.rows[0].get("total"), Some(&Value::Int(3)));
    }
    std::fs::remove_dir_all(path).unwrap();
}
