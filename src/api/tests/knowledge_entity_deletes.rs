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
fn deletes_knowledge_entity_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'First'})-[:MENTIONS]->(:Entity {id: 'entity_1', name: 'HawDB'})")
        .unwrap();

    let output = db
        .delete_knowledge_entity(&KnowledgeEntityDeleteRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.node_id, Some(0));
    assert!(output.matched);
    assert!(!output.filtered_out);
    assert_eq!(output.deleted_node_count, 1);
    assert!(db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .unwrap()
        .entity
        .is_none());
    assert!(db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: "entity_1".to_string(),
        })
        .unwrap()
        .entity
        .is_some());
    let relationships = db
        .query_relationships_via_cypher(&KnowledgeRelationshipsRequest {
            seeds: vec![KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            }],
            relationship_type: None,
            direction: KnowledgeNeighborDirection::Incoming,
            limit_per_seed: 4,
        })
        .unwrap();
    assert_eq!(relationships.relationship_count, 0);
}

#[test]
fn scoped_knowledge_entity_delete_does_not_write_filtered_seed() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})")
        .unwrap();

    let output = db
        .delete_scoped_knowledge_entity(&KnowledgeScopedEntityDeleteRequest {
            delete: KnowledgeEntityDeleteRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
            },
            metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_2".to_string())]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.node_id, Some(0));
    assert!(!output.matched);
    assert!(output.filtered_out);
    assert_eq!(output.deleted_node_count, 0);
    assert!(db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .unwrap()
        .entity
        .is_some());
}

#[test]
fn knowledge_entity_delete_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();

    let output = db
        .delete_knowledge_entity(&KnowledgeEntityDeleteRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "0".to_string(),
            },
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.node_id, Some(0));
    assert!(!output.matched);
    assert!(!output.filtered_out);
    assert_eq!(output.deleted_node_count, 0);
}

#[test]
fn knowledge_entity_delete_rejects_invalid_identifier() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();

    let error = db
        .delete_knowledge_entity(&KnowledgeEntityDeleteRequest {
            entity: KnowledgeEntityRequest {
                label: "Bad-Label".to_string(),
                external_id: "memory_1".to_string(),
            },
        })
        .unwrap_err();

    assert!(error.to_string().contains("label identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn read_only_database_rejects_typed_knowledge_entity_delete() {
    let path = unique_test_dir("read_only_typed_knowledge_entity_delete");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
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
            .delete_knowledge_entity(&KnowledgeEntityDeleteRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_entity_delete_persists_and_replays_from_wal() {
    let path = unique_test_dir("typed_knowledge_entity_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
            .unwrap();
        db.delete_knowledge_entity(&KnowledgeEntityDeleteRequest {
            entity: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
        })
        .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .query_entity_via_cypher(&KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            })
            .unwrap()
            .entity
            .is_none());
        assert!(db
            .query_entity_via_cypher(&KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            })
            .unwrap()
            .entity
            .is_some());
    }
    std::fs::remove_dir_all(path).unwrap();
}
