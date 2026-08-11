use super::*;

#[test]
fn scoped_knowledge_property_batch_update_does_not_write_filtered_rows() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'Old 1', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 'memory_2', title: 'Old 2', source_id: 'thread_2', space_id: ''})",
    )
    .unwrap();

    let output = db
        .update_scoped_knowledge_properties_batch(&KnowledgeScopedPropertyUpdateBatchRequest {
            updates: vec![
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("New 1".to_string()),
                    )]),
                },
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("New 2".to_string()),
                    )]),
                },
            ],
            metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.filtered_out_count, 1);
    assert_eq!(output.updated_property_count, 1);
    assert!(output.rows[0].matched);
    assert!(output.rows[1].filtered_out);
    let row = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_2".to_string(),
                },
            ],
            property_names: vec!["title".to_string()],
        })
        .unwrap();
    assert_eq!(
        row.rows[0].properties.get("title"),
        Some(&Some(Value::String("New 1".to_string())))
    );
    assert_eq!(
        row.rows[1].properties.get("title"),
        Some(&Some(Value::String("Old 2".to_string())))
    );
}

#[test]
fn knowledge_property_batch_update_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
        .unwrap();

    let error = db
        .update_knowledge_properties_batch(&KnowledgePropertyUpdateBatchRequest {
            updates: vec![KnowledgePropertyUpdateRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                assignments: BTreeMap::from([(
                    "bad-name".to_string(),
                    Value::String("New".to_string()),
                )]),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("property identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn knowledge_property_batch_update_rejects_empty_assignment_rows() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
        .unwrap();

    let error = db
        .update_knowledge_properties_batch(&KnowledgePropertyUpdateBatchRequest {
            updates: vec![KnowledgePropertyUpdateRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_1".to_string(),
                },
                assignments: BTreeMap::new(),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("at least one assignment"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn knowledge_property_batch_update_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();

    let output = db
        .update_knowledge_properties_batch(&KnowledgePropertyUpdateBatchRequest {
            updates: vec![KnowledgePropertyUpdateRequest {
                entity: KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "0".to_string(),
                },
                assignments: BTreeMap::from([(
                    "title".to_string(),
                    Value::String("New".to_string()),
                )]),
            }],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.matched_count, 0);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.updated_property_count, 0);
    assert!(output.rows[0].non_writable);
}

#[test]
fn read_only_database_rejects_typed_knowledge_property_batch_update() {
    let path = unique_test_dir("read_only_typed_knowledge_property_batch_update");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
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
            .update_knowledge_properties_batch(&KnowledgePropertyUpdateBatchRequest {
                updates: vec![KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("New".to_string()),
                    )]),
                }],
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_property_batch_update_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_knowledge_property_batch_update_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old 1'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', title: 'Old 2'})")
            .unwrap();
        db.update_knowledge_properties_batch(&KnowledgePropertyUpdateBatchRequest {
            updates: vec![
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("New 1".to_string()),
                    )]),
                },
                KnowledgePropertyUpdateRequest {
                    entity: KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                    assignments: BTreeMap::from([(
                        "title".to_string(),
                        Value::String("New 2".to_string()),
                    )]),
                },
            ],
        })
        .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    assert_eq!(wal.matches("\tbatch\t").count(), 2);
    {
        let db = Database::open(&path).unwrap();
        let output = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_2".to_string(),
                    },
                ],
                property_names: vec!["title".to_string()],
            })
            .unwrap();
        assert_eq!(
            output.rows[0].properties.get("title"),
            Some(&Some(Value::String("New 1".to_string())))
        );
        assert_eq!(
            output.rows[1].properties.get("title"),
            Some(&Some(Value::String("New 2".to_string())))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}
