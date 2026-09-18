use super::*;

#[test]
fn deletes_knowledge_relationship_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL {assigned_by: 'system'}]->(:Label {id: 'label_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})-[:HAS_LABEL {assigned_by: 'system'}]->(:Label {id: 'label_2'})")
        .unwrap();

    let output = db
        .delete_knowledge_relationship_batch(&KnowledgeRelationshipDeleteBatchRequest {
            deletes: vec![
                KnowledgeRelationshipDeleteRequest {
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
                KnowledgeRelationshipDeleteRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_2".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    relationship_properties: BTreeMap::new(),
                },
                KnowledgeRelationshipDeleteRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "missing".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_1".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    relationship_properties: BTreeMap::new(),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_endpoint_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.deleted_relationship_count, 2);
    assert!(output.rows[0].matched);
    assert!(output.rows[1].matched);
    assert!(!output.rows[2].matched);
    assert_eq!(output.rows[2].source_node_id, None);
    assert_eq!(output.rows[2].target_node_id, Some(1));

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
            limit_per_seed: 4,
        })
        .unwrap();
    assert_eq!(relationships.relationship_count, 0);
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
fn typed_knowledge_relationship_batch_delete_filters_relationship_properties() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS {source_reference: 'keep'}]->(:Entity {id: 'entity_1'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_1'}), (e:Entity {id: 'entity_1'}) CREATE (m)-[:MENTIONS {source_reference: 'drop'}]->(e)")
        .unwrap();

    let output = db
        .delete_knowledge_relationship_batch(&KnowledgeRelationshipDeleteBatchRequest {
            deletes: vec![KnowledgeRelationshipDeleteRequest {
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
            }],
        })
        .unwrap();

    assert_eq!(output.matched_count, 1);
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
fn scoped_knowledge_relationship_batch_delete_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL]->(:Label {id: 'label_1', name: 'Database'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_2', space_id: ''})-[:HAS_LABEL]->(:Label {id: 'label_2', name: 'Rust'})")
        .unwrap();

    let output = db
        .delete_scoped_knowledge_relationship_batch(
            &KnowledgeScopedRelationshipDeleteBatchRequest {
                deletes: vec![
                    KnowledgeRelationshipDeleteRequest {
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
                    KnowledgeRelationshipDeleteRequest {
                        source: KnowledgeEntityRequest {
                            label: "Memory".to_string(),
                            external_id: "memory_2".to_string(),
                        },
                        target: KnowledgeEntityRequest {
                            label: "Label".to_string(),
                            external_id: "label_2".to_string(),
                        },
                        relationship_type: "HAS_LABEL".to_string(),
                        relationship_properties: BTreeMap::new(),
                    },
                ],
                source_metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                target_metadata_filters: BTreeMap::new(),
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.source_filtered_out_count, 1);
    assert_eq!(output.deleted_relationship_count, 1);
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
            relationship_type: Some("HAS_LABEL".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit_per_seed: 4,
        })
        .unwrap();
    assert_eq!(relationships.relationship_count, 1);
    assert!(relationships.groups[0].relationships.is_empty());
    assert_eq!(relationships.groups[1].relationships.len(), 1);
}

#[test]
fn knowledge_relationship_batch_delete_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
        .unwrap();

    let error = db
        .delete_knowledge_relationship_batch(&KnowledgeRelationshipDeleteBatchRequest {
            deletes: vec![KnowledgeRelationshipDeleteRequest {
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
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("relationship type identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn knowledge_relationship_batch_delete_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();

    let output = db
        .delete_knowledge_relationship_batch(&KnowledgeRelationshipDeleteBatchRequest {
            deletes: vec![KnowledgeRelationshipDeleteRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "0".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Label".to_string(),
                    external_id: "label_1".to_string(),
                },
                relationship_type: "HAS_LABEL".to_string(),
                relationship_properties: BTreeMap::new(),
            }],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.matched_count, 0);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.deleted_relationship_count, 0);
    assert!(output.rows[0].non_writable);
}

#[test]
fn read_only_database_rejects_typed_knowledge_relationship_batch_delete() {
    let path = unique_test_dir("read_only_typed_knowledge_relationship_batch_delete");
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
            .delete_knowledge_relationship_batch(&KnowledgeRelationshipDeleteBatchRequest {
                deletes: vec![KnowledgeRelationshipDeleteRequest {
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
                }],
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_relationship_batch_delete_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_relationship_batch_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})-[:HAS_LABEL]->(:Label {id: 'label_2'})")
            .unwrap();
    }
    let setup_wal = read_test_wal(&path).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        db.delete_knowledge_relationship_batch(&KnowledgeRelationshipDeleteBatchRequest {
            deletes: vec![
                KnowledgeRelationshipDeleteRequest {
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
                KnowledgeRelationshipDeleteRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Label".to_string(),
                        external_id: "label_2".to_string(),
                    },
                    relationship_type: "HAS_LABEL".to_string(),
                    relationship_properties: BTreeMap::new(),
                },
            ],
        })
        .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_rel"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
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
                relationship_type: Some("HAS_LABEL".to_string()),
                direction: KnowledgeNeighborDirection::Outgoing,
                limit_per_seed: 4,
            })
            .unwrap();
        assert_eq!(output.relationship_count, 0);
    }
    std::fs::remove_dir_all(path).unwrap();
}
