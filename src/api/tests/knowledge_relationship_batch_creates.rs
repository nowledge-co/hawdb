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
fn creates_knowledge_relationship_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_1'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'HawDB'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_2', name: 'Graph'})")
        .unwrap();

    let output = db
        .create_knowledge_relationship_batch(&KnowledgeRelationshipCreateBatchRequest {
            creates: vec![
                KnowledgeRelationshipCreateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    properties: BTreeMap::from([("confidence".to_string(), Value::Float(0.9))]),
                },
                KnowledgeRelationshipCreateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_2".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    properties: BTreeMap::from([("confidence".to_string(), Value::Float(0.7))]),
                },
                KnowledgeRelationshipCreateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "missing".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    properties: BTreeMap::new(),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_endpoint_count, 1);
    assert_eq!(output.source_filtered_out_count, 0);
    assert_eq!(output.target_filtered_out_count, 0);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.created_relationship_count, 2);
    assert!(output.rows[0].matched);
    assert!(output.rows[1].matched);
    assert!(!output.rows[2].matched);
    assert_eq!(output.rows[2].source_node_id, None);
    assert_eq!(output.rows[2].target_node_id, Some(2));

    let relationships = db
        .query_relationships_via_cypher(&KnowledgeRelationshipsRequest {
            seeds: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            relationship_type: Some("MENTIONS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        })
        .unwrap();
    assert_eq!(relationships.relationship_count, 2);
    assert_eq!(relationships.groups[0].relationships.len(), 1);
    assert_eq!(relationships.groups[1].relationships.len(), 1);
}

#[test]
fn scoped_knowledge_relationship_batch_create_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_2', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', space_id: 'default'})")
        .unwrap();

    let output = db
        .create_scoped_knowledge_relationship_batch(
            &KnowledgeScopedRelationshipCreateBatchRequest {
                creates: vec![
                    KnowledgeRelationshipCreateRequest {
                        source: KnowledgeEntityRequest {
                            label: "Memory".to_string(),
                            external_id: "memory_1".to_string(),
                        },
                        target: KnowledgeEntityRequest {
                            label: "Entity".to_string(),
                            external_id: "entity_1".to_string(),
                        },
                        relationship_type: "MENTIONS".to_string(),
                        properties: BTreeMap::new(),
                    },
                    KnowledgeRelationshipCreateRequest {
                        source: KnowledgeEntityRequest {
                            label: "Memory".to_string(),
                            external_id: "memory_2".to_string(),
                        },
                        target: KnowledgeEntityRequest {
                            label: "Entity".to_string(),
                            external_id: "entity_1".to_string(),
                        },
                        relationship_type: "MENTIONS".to_string(),
                        properties: BTreeMap::new(),
                    },
                ],
                source_metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                target_metadata_filters: BTreeMap::from([(
                    "space_id".to_string(),
                    "default".to_string(),
                )]),
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.source_filtered_out_count, 1);
    assert_eq!(output.target_filtered_out_count, 0);
    assert_eq!(output.created_relationship_count, 1);
    assert!(output.rows[0].matched);
    assert!(!output.rows[1].matched);
    assert!(output.rows[1].source_filtered_out);

    let relationships = db
        .query_relationships_via_cypher(&KnowledgeRelationshipsRequest {
            seeds: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            relationship_type: Some("MENTIONS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        })
        .unwrap();
    assert_eq!(relationships.relationship_count, 1);
    assert_eq!(relationships.groups[0].relationships.len(), 1);
    assert!(relationships.groups[1].relationships.is_empty());
}

#[test]
fn knowledge_relationship_batch_create_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();

    let error = db
        .create_knowledge_relationship_batch(&KnowledgeRelationshipCreateBatchRequest {
            creates: vec![KnowledgeRelationshipCreateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
                relationship_type: "MENTIONS-WITH-DASH".to_string(),
                properties: BTreeMap::new(),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("relationship type identifier"));
    assert_eq!(db.store.commit_epoch(), 2);
}

#[test]
fn knowledge_relationship_batch_create_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();

    let output = db
        .create_knowledge_relationship_batch(&KnowledgeRelationshipCreateBatchRequest {
            creates: vec![KnowledgeRelationshipCreateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "0".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
                relationship_type: "MENTIONS".to_string(),
                properties: BTreeMap::new(),
            }],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.matched_count, 0);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.created_relationship_count, 0);
    assert!(output.rows[0].non_writable);
}

#[test]
fn read_only_database_rejects_typed_knowledge_relationship_batch_create() {
    let path = unique_test_dir("read_only_typed_knowledge_relationship_batch_create");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
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
            .create_knowledge_relationship_batch(&KnowledgeRelationshipCreateBatchRequest {
                creates: vec![KnowledgeRelationshipCreateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    properties: BTreeMap::new(),
                }],
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_relationship_batch_create_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_relationship_batch_create_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_2'})").unwrap();
        db.create_knowledge_relationship_batch(&KnowledgeRelationshipCreateBatchRequest {
            creates: vec![
                KnowledgeRelationshipCreateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    properties: BTreeMap::from([("confidence".to_string(), Value::Float(0.7))]),
                },
                KnowledgeRelationshipCreateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_2".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    properties: BTreeMap::from([("confidence".to_string(), Value::Float(0.8))]),
                },
            ],
        })
        .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_rel"));
    assert_eq!(wal.matches("\tbatch\t").count(), 2);
    {
        let db = Database::open(&path).unwrap();
        let output = db
            .query_relationships_via_cypher(&KnowledgeRelationshipsRequest {
                seeds: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                ],
                relationship_type: Some("MENTIONS".to_string()),
                direction: KnowledgeNeighborDirection::Outgoing,
                limit_per_seed: 4,
            })
            .unwrap();
        assert_eq!(output.relationship_count, 2);
    }
    std::fs::remove_dir_all(path).unwrap();
}
