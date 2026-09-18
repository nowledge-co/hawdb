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
fn creates_knowledge_relationship_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'First'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'HawDB'})")
        .unwrap();

    let output = db
        .create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            relationship_type: "MENTIONS".to_string(),
            properties: BTreeMap::from([
                ("confidence".to_string(), Value::Float(0.9)),
                ("mention_count".to_string(), Value::Int(1)),
            ]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(output.matched);
    assert!(!output.source_filtered_out);
    assert!(!output.target_filtered_out);
    assert_eq!(output.created_relationship_count, 1);

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
            .target_external_id
            .as_deref(),
        Some("entity_1")
    );
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("mention_count"),
        Some(&Value::Int(1))
    );
}

#[test]
fn scoped_knowledge_relationship_create_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'First', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'HawDB', space_id: 'default'})")
        .unwrap();

    let output = db
        .create_scoped_knowledge_relationship(&KnowledgeScopedRelationshipCreateRequest {
            create: KnowledgeRelationshipCreateRequest {
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
            source_metadata_filters: BTreeMap::from([(
                "source_id".to_string(),
                "thread_2".to_string(),
            )]),
            target_metadata_filters: BTreeMap::from([(
                "space_id".to_string(),
                "default".to_string(),
            )]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(!output.matched);
    assert!(output.source_filtered_out);
    assert!(!output.target_filtered_out);
    assert_eq!(output.created_relationship_count, 0);
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
    assert_eq!(relationships.relationship_count, 0);
}

#[test]
fn knowledge_relationship_create_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();

    let error = db
        .create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
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
        })
        .unwrap_err();

    assert!(error.to_string().contains("relationship type identifier"));
    assert_eq!(db.store.commit_epoch(), 2);
}

#[test]
fn knowledge_relationship_create_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();

    let output = db
        .create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
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
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(!output.matched);
    assert_eq!(output.created_relationship_count, 0);
}

#[test]
fn upserts_knowledge_relationship_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();

    let created = db
        .upsert_knowledge_relationship(&KnowledgeRelationshipUpsertRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_1".to_string(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            create_properties: BTreeMap::from([
                (
                    "assigned_by".to_string(),
                    Value::String("system".to_string()),
                ),
                (
                    "created_at".to_string(),
                    Value::String("2026-07-19".to_string()),
                ),
            ]),
        })
        .unwrap();

    assert_eq!(created.graph_commit_epoch_before, 2);
    assert_eq!(created.graph_commit_epoch_after, 3);
    assert_eq!(created.source_node_id, Some(0));
    assert_eq!(created.target_node_id, Some(1));
    assert!(created.matched);
    assert!(created.created);
    assert!(!created.already_exists);
    assert_eq!(created.created_relationship_count, 1);
    assert_eq!(created.relationship_id, Some(0));

    let epoch_before_second = db.store.commit_epoch();
    let existing = db
        .upsert_knowledge_relationship(&KnowledgeRelationshipUpsertRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_1".to_string(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            create_properties: BTreeMap::from([(
                "assigned_by".to_string(),
                Value::String("ignored".to_string()),
            )]),
        })
        .unwrap();

    assert_eq!(existing.graph_commit_epoch_before, epoch_before_second);
    assert_eq!(existing.graph_commit_epoch_after, epoch_before_second);
    assert!(existing.matched);
    assert!(!existing.created);
    assert!(existing.already_exists);
    assert_eq!(existing.relationship_id, Some(0));
    assert_eq!(existing.created_relationship_count, 0);
    let relationships = db
        .query_relationships_via_cypher(&KnowledgeRelationshipsRequest {
            seeds: vec![KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            }],
            relationship_type: Some("HAS_LABEL".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 10,
        })
        .unwrap();
    assert_eq!(relationships.relationship_count, 1);
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("assigned_by"),
        Some(&Value::String("system".to_string()))
    );
}

#[test]
fn knowledge_relationship_upsert_does_not_write_projected_idless_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();

    let output = db
        .upsert_knowledge_relationship(&KnowledgeRelationshipUpsertRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "0".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: "label_1".to_string(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            create_properties: BTreeMap::new(),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(!output.matched);
    assert!(output.non_writable);
    assert_eq!(output.created_relationship_count, 0);
}

#[test]
fn upserts_knowledge_relationship_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();
    db.query("MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'label_1'}) CREATE (m)-[:HAS_LABEL {assigned_by: 'existing'}]->(l)")
        .unwrap();

    let output = db
        .upsert_knowledge_relationship_batch(&KnowledgeRelationshipUpsertBatchRequest {
            upserts: vec![
                KnowledgeRelationshipUpsertRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    create_properties: BTreeMap::from([(
                        "assigned_by".to_string(),
                        Value::String("ignored".to_string()),
                    )]),
                },
                KnowledgeRelationshipUpsertRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    create_properties: BTreeMap::from([(
                        "assigned_by".to_string(),
                        Value::String("created".to_string()),
                    )]),
                },
                KnowledgeRelationshipUpsertRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    create_properties: BTreeMap::from([(
                        "assigned_by".to_string(),
                        Value::String("duplicate".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.matched_count, 3);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.already_exists_count, 2);
    assert_eq!(output.created_relationship_count, 1);
    assert!(output.rows[0].already_exists);
    assert_eq!(output.rows[0].relationship_id, Some(0));
    assert!(output.rows[1].created);
    assert!(output.rows[1].relationship_id.is_some());
    assert!(output.rows[2].already_exists);
    assert_eq!(output.rows[2].relationship_id, None);

    for memory_id in ["memory_1", "memory_2"] {
        let relationships = db
            .query_relationships_via_cypher(&KnowledgeRelationshipsRequest {
                seeds: vec![KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: memory_id.to_string(),
                }],
                relationship_type: Some("HAS_LABEL".to_string()),
                direction: KnowledgeNeighborDirection::Outgoing,
                limit_per_seed: 10,
            })
            .unwrap();
        assert_eq!(relationships.relationship_count, 1);
    }
}

#[test]
fn typed_knowledge_relationship_batch_upsert_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_relationship_batch_upsert_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
        db.query("CREATE (:Label {id: 'label_1'})").unwrap();
        let batch_count_before_upsert = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.upsert_knowledge_relationship_batch(&KnowledgeRelationshipUpsertBatchRequest {
            upserts: vec![
                KnowledgeRelationshipUpsertRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    create_properties: BTreeMap::from([(
                        "assigned_by".to_string(),
                        Value::String("system".to_string()),
                    )]),
                },
                KnowledgeRelationshipUpsertRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    create_properties: BTreeMap::from([(
                        "assigned_by".to_string(),
                        Value::String("system".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();
        let batch_count_after_upsert = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_upsert, batch_count_before_upsert + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_rel"));
    {
        let db = Database::open(&path).unwrap();
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
                relationship_type: Some("HAS_LABEL".to_string()),
                direction: KnowledgeNeighborDirection::Outgoing,
                limit_per_seed: 10,
            })
            .unwrap();
        assert_eq!(relationships.relationship_count, 2);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_database_rejects_typed_knowledge_relationship_create() {
    let path = unique_test_dir("read_only_typed_knowledge_relationship_create");
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
            .create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
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
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_relationship_create_persists_and_replays_from_wal() {
    let path = unique_test_dir("typed_knowledge_relationship_create_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
        db.create_knowledge_relationship(&KnowledgeRelationshipCreateRequest {
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
        })
        .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_rel"));
    {
        let db = Database::open(&path).unwrap();
        let output = db
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
        assert_eq!(output.relationship_count, 1);
        assert_eq!(
            output.groups[0].relationships[0]
                .relationship_properties
                .get("confidence"),
            Some(&Value::Float(0.7))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}
