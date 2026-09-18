use super::*;

#[test]
fn deletes_knowledge_entity_batch_through_typed_api() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_1'})-[:MENTIONS]->(:Entity {id: 'entity_2'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_3', source_id: 'thread_1'})")
        .unwrap();

    let output = db
        .delete_knowledge_entity_batch(&KnowledgeEntityDeleteBatchRequest {
            label: "Memory".to_string(),
            external_ids: vec![
                "memory_2".to_string(),
                "missing".to_string(),
                "memory_1".to_string(),
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.filtered_out_count, 0);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.deleted_node_count, 2);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.rows[0].external_id, "memory_2");
    assert!(output.rows[0].matched);
    assert_eq!(output.rows[1].external_id, "missing");
    assert!(!output.rows[1].matched);
    assert_eq!(output.rows[1].node_id, None);
    assert_eq!(output.rows[2].external_id, "memory_1");
    assert!(output.rows[2].matched);
    for external_id in ["memory_1", "memory_2"] {
        assert!(db
            .query_entity_via_cypher(&KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: external_id.to_string(),
            })
            .unwrap()
            .entity
            .is_none());
    }
    assert!(db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_3".to_string(),
        })
        .unwrap()
        .entity
        .is_some());
    assert_eq!(
        db.query("MATCH (a)-[r]->(b) RETURN count(r) AS relationships")
            .unwrap()
            .rows[0]
            .get("relationships"),
        Some(&Value::Int(0))
    );
}

#[test]
fn scoped_knowledge_entity_batch_delete_does_not_write_filtered_rows() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_2', space_id: ''})")
        .unwrap();

    let output = db
        .delete_scoped_knowledge_entity_batch(&KnowledgeScopedEntityDeleteBatchRequest {
            delete: KnowledgeEntityDeleteBatchRequest {
                label: "Memory".to_string(),
                external_ids: vec!["memory_1".to_string(), "memory_2".to_string()],
            },
            metadata_filters: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 2);
    assert_eq!(output.graph_commit_epoch_after, 3);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.filtered_out_count, 1);
    assert_eq!(output.deleted_node_count, 1);
    assert!(output.rows[0].matched);
    assert!(!output.rows[0].filtered_out);
    assert!(!output.rows[1].matched);
    assert!(output.rows[1].filtered_out);
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
            label: "Memory".to_string(),
            external_id: "memory_2".to_string(),
        })
        .unwrap()
        .entity
        .is_some());
}

#[test]
fn knowledge_entity_batch_delete_deduplicates_writes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();

    let output = db
        .delete_knowledge_entity_batch(&KnowledgeEntityDeleteBatchRequest {
            label: "Memory".to_string(),
            external_ids: vec!["memory_1".to_string(), "memory_1".to_string()],
        })
        .unwrap();

    assert_eq!(output.matched_count, 2);
    assert_eq!(output.deleted_node_count, 1);
    assert_eq!(output.rows.len(), 2);
    assert!(output.rows.iter().all(|row| row.matched));
    assert!(db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "memory_1".to_string(),
        })
        .unwrap()
        .entity
        .is_none());
}

#[test]
fn knowledge_entity_batch_delete_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();

    let output = db
        .delete_knowledge_entity_batch(&KnowledgeEntityDeleteBatchRequest {
            label: "Memory".to_string(),
            external_ids: vec!["0".to_string()],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 1);
    assert_eq!(output.matched_count, 0);
    assert_eq!(output.non_writable_count, 1);
    assert_eq!(output.deleted_node_count, 0);
    assert!(output.rows[0].non_writable);
    assert!(db
        .query_entity_via_cypher(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "0".to_string(),
        })
        .unwrap()
        .entity
        .is_some());
}

#[test]
fn knowledge_entity_batch_delete_rejects_invalid_identifier() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();

    let error = db
        .delete_knowledge_entity_batch(&KnowledgeEntityDeleteBatchRequest {
            label: "Bad-Label".to_string(),
            external_ids: vec!["memory_1".to_string()],
        })
        .unwrap_err();

    assert!(error.to_string().contains("label identifier"));
    assert_eq!(db.store.commit_epoch(), 1);
}

#[test]
fn read_only_database_rejects_typed_knowledge_entity_batch_delete() {
    let path = unique_test_dir("read_only_typed_knowledge_entity_batch_delete");
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
            .delete_knowledge_entity_batch(&KnowledgeEntityDeleteBatchRequest {
                label: "Memory".to_string(),
                external_ids: vec!["memory_1".to_string()],
            })
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_knowledge_entity_batch_delete_persists_and_replays_from_wal() {
    let path = unique_test_dir("typed_knowledge_entity_batch_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
        db.delete_knowledge_entity_batch(&KnowledgeEntityDeleteBatchRequest {
            label: "Memory".to_string(),
            external_ids: vec!["memory_1".to_string(), "memory_2".to_string()],
        })
        .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let db = Database::open(&path).unwrap();
        for external_id in ["memory_1", "memory_2"] {
            assert!(db
                .query_entity_via_cypher(&KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: external_id.to_string(),
                })
                .unwrap()
                .entity
                .is_none());
        }
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
