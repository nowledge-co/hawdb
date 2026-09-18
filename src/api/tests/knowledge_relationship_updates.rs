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
fn updates_knowledge_relationship_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1'})-[:MENTIONS {source_reference: 'raw', weight: 1}]->(:Entity {id: 'entity_1'})")
        .unwrap();

    let output = db
        .update_knowledge_relationship(&KnowledgeRelationshipUpdateRequest {
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
                Value::String("raw".to_string()),
            )]),
            assignments: BTreeMap::from([
                ("weight".to_string(), Value::Int(9)),
                (
                    "review_status".to_string(),
                    Value::String("approved".to_string()),
                ),
            ]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.source_node_id, Some(0));
    assert_eq!(output.target_node_id, Some(1));
    assert!(output.matched);
    assert_eq!(output.updated_relationship_count, 1);
    assert_eq!(output.updated_property_count, 2);
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
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(9))
    );
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("review_status"),
        Some(&Value::String("approved".to_string()))
    );
}

#[test]
fn scoped_knowledge_relationship_update_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL {weight: 1}]->(:Label {id: 'label_1', name: 'Database'})")
        .unwrap();

    let output = db
        .update_scoped_knowledge_relationship(&KnowledgeScopedRelationshipUpdateRequest {
            update: KnowledgeRelationshipUpdateRequest {
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
                assignments: BTreeMap::from([("weight".to_string(), Value::Int(9))]),
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
    assert!(!output.matched);
    assert!(output.source_filtered_out);
    assert_eq!(output.updated_relationship_count, 0);
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
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(1))
    );
}

#[test]
fn updates_knowledge_relationship_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1'})-[:MENTIONS {source_reference: 'raw_1', weight: 1}]->(:Entity {id: 'entity_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_1'})-[:MENTIONS {source_reference: 'raw_2', weight: 2}]->(:Entity {id: 'entity_2'})")
        .unwrap();

    let output = db
        .update_knowledge_relationship_batch(&KnowledgeRelationshipUpdateBatchRequest {
            updates: vec![
                KnowledgeRelationshipUpdateRequest {
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
                        Value::String("raw_1".to_string()),
                    )]),
                    assignments: BTreeMap::from([("weight".to_string(), Value::Int(9))]),
                },
                KnowledgeRelationshipUpdateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_2".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    relationship_properties: BTreeMap::from([(
                        "source_reference".to_string(),
                        Value::String("raw_2".to_string()),
                    )]),
                    assignments: BTreeMap::from([("weight".to_string(), Value::Int(8))]),
                },
                KnowledgeRelationshipUpdateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "missing".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    relationship_properties: BTreeMap::new(),
                    assignments: BTreeMap::from([("weight".to_string(), Value::Int(7))]),
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
    assert_eq!(output.updated_relationship_count, 2);
    assert_eq!(output.updated_property_count, 2);
    assert!(output.rows[0].matched);
    assert!(output.rows[1].matched);
    assert!(!output.rows[2].matched);

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
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(9))
    );
    assert_eq!(
        relationships.groups[1].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(8))
    );
}

#[test]
fn scoped_knowledge_relationship_batch_update_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL {weight: 1}]->(:Label {id: 'label_1', name: 'Database'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_2', space_id: ''})-[:HAS_LABEL {weight: 2}]->(:Label {id: 'label_2', name: 'Rust'})")
        .unwrap();

    let output = db
        .update_scoped_knowledge_relationship_batch(
            &KnowledgeScopedRelationshipUpdateBatchRequest {
                updates: vec![
                    KnowledgeRelationshipUpdateRequest {
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
                        assignments: BTreeMap::from([("weight".to_string(), Value::Int(9))]),
                    },
                    KnowledgeRelationshipUpdateRequest {
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
                        assignments: BTreeMap::from([("weight".to_string(), Value::Int(8))]),
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
    assert_eq!(output.updated_relationship_count, 1);
    assert!(output.rows[0].matched);
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
    assert_eq!(
        relationships.groups[0].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(9))
    );
    assert_eq!(
        relationships.groups[1].relationships[0]
            .relationship_properties
            .get("weight"),
        Some(&Value::Int(2))
    );
}

#[test]
fn knowledge_relationship_update_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
        .unwrap();

    let error = db
        .update_knowledge_relationship(&KnowledgeRelationshipUpdateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            relationship_type: "MENTIONS".to_string(),
            relationship_properties: BTreeMap::new(),
            assignments: BTreeMap::from([("bad-name".to_string(), Value::Int(1))]),
        })
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("relationship property identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn knowledge_relationship_update_rejects_empty_assignments() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
        .unwrap();

    let error = db
        .update_knowledge_relationship(&KnowledgeRelationshipUpdateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: "memory_1".to_string(),
            },
            target: KnowledgeEntityRequest {
                label: "Entity".to_string(),
                external_id: "entity_1".to_string(),
            },
            relationship_type: "MENTIONS".to_string(),
            relationship_properties: BTreeMap::new(),
            assignments: BTreeMap::new(),
        })
        .unwrap_err();

    assert!(error.to_string().contains("at least one assignment"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn knowledge_relationship_batch_update_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();

    let output = db
        .update_knowledge_relationship_batch(&KnowledgeRelationshipUpdateBatchRequest {
            updates: vec![KnowledgeRelationshipUpdateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "0".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
                relationship_type: "MENTIONS".to_string(),
                relationship_properties: BTreeMap::new(),
                assignments: BTreeMap::from([("weight".to_string(), Value::Int(9))]),
            }],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.matched_count, 0);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.updated_relationship_count, 0);
    assert!(output.rows[0].non_writable);
}

#[test]
fn read_only_database_rejects_typed_knowledge_relationship_update() {
    let path = unique_test_dir("read_only_typed_knowledge_relationship_update");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
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
            .update_knowledge_relationship(&KnowledgeRelationshipUpdateRequest {
                source: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                target: KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_1".to_string(),
                },
                relationship_type: "MENTIONS".to_string(),
                relationship_properties: BTreeMap::new(),
                assignments: BTreeMap::from([("weight".to_string(), Value::Int(9))]),
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_relationship_batch_update_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_relationship_batch_update_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Memory {id: 'memory_1'})-[:MENTIONS {weight: 1}]->(:Entity {id: 'entity_1'})",
        )
        .unwrap();
        db.query(
            "CREATE (:Memory {id: 'memory_2'})-[:MENTIONS {weight: 2}]->(:Entity {id: 'entity_2'})",
        )
        .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_relationship_batch(&KnowledgeRelationshipUpdateBatchRequest {
            updates: vec![
                KnowledgeRelationshipUpdateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_1".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    relationship_properties: BTreeMap::new(),
                    assignments: BTreeMap::from([("weight".to_string(), Value::Int(9))]),
                },
                KnowledgeRelationshipUpdateRequest {
                    source: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    target: KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_2".to_string(),
                    },
                    relationship_type: "MENTIONS".to_string(),
                    relationship_properties: BTreeMap::new(),
                    assignments: BTreeMap::from([("weight".to_string(), Value::Int(8))]),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_rel_property"));
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
        assert_eq!(
            output.groups[0].relationships[0]
                .relationship_properties
                .get("weight"),
            Some(&Value::Int(9))
        );
        assert_eq!(
            output.groups[1].relationships[0]
                .relationship_properties
                .get("weight"),
            Some(&Value::Int(8))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}
