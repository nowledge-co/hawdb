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
fn deletes_knowledge_relationship_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL {assigned_by: 'system'}]->(:Label {id: 'label_1', name: 'Database'})")
        .unwrap();

    let output = db
        .delete_knowledge_relationship(&KnowledgeRelationshipDeleteRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_1".to_string(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            relationship_properties: BTreeMap::new(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(output.matched);
    assert!(!output.source_filtered_out);
    assert!(!output.target_filtered_out);
    assert_eq!(output.deleted_relationship_count, 1);
    let relationships = db
        .query_relationships_via_cypher(&KnowledgeRelationshipsRequest {
            seeds: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            }],
            relationship_type: Some("HAS_LABEL".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        })
        .unwrap();
    assert_eq!(relationships.relationship_count, 0);
    assert!(db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .unwrap()
        .entity
        .is_some());
    assert!(db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Label".to_string(),
            external_id: "label_1".to_string(),
        })
        .unwrap()
        .entity
        .is_some());
}

#[test]
fn typed_knowledge_relationship_delete_filters_relationship_properties() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS {source_reference: 'keep'}]->(:Entity {id: 'entity_1'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_1'}), (e:Entity {id: 'entity_1'}) CREATE (m)-[:MENTIONS {source_reference: 'drop'}]->(e)")
        .unwrap();

    let output = db
        .delete_knowledge_relationship(&KnowledgeRelationshipDeleteRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            relationship_type: "MENTIONS".to_string(),
            relationship_properties: BTreeMap::from([(
                "source_reference".to_string(),
                Value::String("drop".to_string()),
            )]),
        })
        .unwrap();

    assert_eq!(output.deleted_relationship_count, 1);
    let relationships = db
        .query_relationships_via_cypher(&KnowledgeRelationshipsRequest {
            seeds: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            }],
            relationship_type: Some("MENTIONS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        })
        .unwrap();
    assert_eq!(relationships.relationship_count, 1);
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("source_reference"),
        Some(&Value::String("keep".to_string()))
    );
}

#[test]
fn scoped_knowledge_relationship_delete_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL]->(:Label {id: 'label_1', name: 'Database'})")
        .unwrap();

    let output = db
        .delete_scoped_knowledge_relationship(&KnowledgeScopedRelationshipDeleteRequest {
            delete: KnowledgeRelationshipDeleteRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_1".to_string(),
                },
                relationship_type: "HAS_LABEL".to_string(),
                relationship_properties: BTreeMap::new(),
            },
            source_metadata_filters: BTreeMap::from([(
                "source_id".to_string(),
                "thread_2".to_string(),
            )]),
            target_metadata_filters: BTreeMap::new(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(!output.matched);
    assert!(output.source_filtered_out);
    assert!(!output.target_filtered_out);
    assert_eq!(output.deleted_relationship_count, 0);
    let relationships = db
        .query_relationships_via_cypher(&KnowledgeRelationshipsRequest {
            seeds: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            }],
            relationship_type: Some("HAS_LABEL".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        })
        .unwrap();
    assert_eq!(relationships.relationship_count, 1);
}

#[test]
fn knowledge_relationship_delete_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
        .unwrap();

    let error = db
        .delete_knowledge_relationship(&KnowledgeRelationshipDeleteRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_1".to_string(),
            },
            relationship_type: "HAS-LABEL".to_string(),
            relationship_properties: BTreeMap::new(),
        })
        .unwrap_err();

    assert!(error.to_string().contains("relationship type identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn read_only_database_rejects_typed_knowledge_relationship_delete() {
    let path = unique_test_dir("read_only_typed_knowledge_relationship_delete");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
            .unwrap();
    }
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let error = db
            .delete_knowledge_relationship(&KnowledgeRelationshipDeleteRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_1".to_string(),
                },
                relationship_type: "HAS_LABEL".to_string(),
                relationship_properties: BTreeMap::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_relationship_delete_persists_and_replays_from_wal() {
    let path = unique_test_dir("typed_knowledge_relationship_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
            .unwrap();
        db.delete_knowledge_relationship(&KnowledgeRelationshipDeleteRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_1".to_string(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            relationship_properties: BTreeMap::new(),
        })
        .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_rel"));
    {
        let db = Database::open(&path).unwrap();
        let output = db
            .query_relationships_via_cypher(&KnowledgeRelationshipsRequest {
                seeds: vec![KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                }],
                relationship_type: Some("HAS_LABEL".to_string()),
                direction: KnowledgeNeighborDirection::Outgoing,
                limit_per_seed: 4,
            })
            .unwrap();
        assert_eq!(output.relationship_count, 0);
    }
    std::fs::remove_dir_all(path).unwrap();
}
