use super::*;

#[test]
fn transaction_merge_deduplicates_pending_nodes_in_one_wal_batch() {
    let path = unique_test_dir("merge_transaction_batch");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query("MERGE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        tx.query("MERGE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(false)));
        assert_eq!(output.rows[0].get("node_id"), output.rows[1].get("node_id"));
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 2);
    assert_eq!(wal.matches("create_node").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_merge_on_create_set_deduplicates_pending_nodes_by_match_key() {
    let path = unique_test_dir("merge_on_create_transaction_batch");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query(
            "MERGE (m:SchemaMigrationLog {id: 'migration-1'}) ON CREATE SET m.note = 'created'",
        )
        .unwrap();
        tx.query(
            "MERGE (m:SchemaMigrationLog {id: 'migration-1'}) ON CREATE SET m.note = 'duplicate'",
        )
        .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(false)));
        assert_eq!(output.rows[0].get("node_id"), output.rows[1].get("node_id"));
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 2);
    assert_eq!(wal.matches("create_node").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:SchemaMigrationLog {id: 'migration-1'}) RETURN m.note AS note")
            .unwrap();
        assert_eq!(
            output.rows[0].get("note"),
            Some(&Value::String("created".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_merge_node_on_match_set_updates_pending_create() {
    let path = unique_test_dir("merge_on_match_transaction_batch");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query(
                "MERGE (l:Label {id: 'label-1'}) ON CREATE SET l.name = 'Important', l.canonical_name = null, l.updated_at = 1 ON MATCH SET l.updated_at = 2, l.canonical_name = COALESCE(l.canonical_name, 'important')",
            )
            .unwrap();
        tx.query(
                "MERGE (l:Label {id: 'label-1'}) ON CREATE SET l.name = 'Duplicate' ON MATCH SET l.updated_at = 2, l.canonical_name = COALESCE(l.canonical_name, 'important')",
            )
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(false)));
        assert_eq!(output.rows[0].get("node_id"), output.rows[1].get("node_id"));
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 2);
    assert_eq!(wal.matches("create_node").count(), 1);
    assert_eq!(wal.matches("set_node_property").count(), 0);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
                .query("MATCH (l:Label {id: 'label-1'}) RETURN l.name AS name, l.canonical_name AS canonical, l.updated_at AS updated")
                .unwrap();
        assert_eq!(
            output.rows[0].get("name"),
            Some(&Value::String("Important".to_string()))
        );
        assert_eq!(
            output.rows[0].get("canonical"),
            Some(&Value::String("important".to_string()))
        );
        assert_eq!(output.rows[0].get("updated"), Some(&Value::Int(2)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_merge_node_post_set_updates_pending_create() {
    let path = unique_test_dir("merge_post_set_transaction_batch");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query(
                "MERGE (m:GraphMeta {meta_id: 'main'}) SET m.pagerank_applied = true, m.pagerank_algorithm = 'pagerank'",
            )
            .unwrap();
        tx.query(
                "MERGE (m:GraphMeta {meta_id: 'main'}) SET m.pagerank_iterations = 20, m.updated_at = CURRENT_TIMESTAMP()",
            )
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(false)));
        assert_eq!(output.rows[0].get("node_id"), output.rows[1].get("node_id"));
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 2);
    assert_eq!(wal.matches("create_node").count(), 1);
    assert_eq!(wal.matches("set_node_property").count(), 0);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
                .query(
                    "MATCH (m:GraphMeta {meta_id: 'main'}) RETURN m.pagerank_applied AS applied, m.pagerank_algorithm AS algorithm, m.pagerank_iterations AS iterations, count(m.updated_at) AS updated",
                )
                .unwrap();
        assert_eq!(output.rows[0].get("applied"), Some(&Value::Bool(true)));
        assert_eq!(
            output.rows[0].get("algorithm"),
            Some(&Value::String("pagerank".to_string()))
        );
        assert_eq!(output.rows[0].get("iterations"), Some(&Value::Int(20)));
        assert_eq!(output.rows[0].get("updated"), Some(&Value::Int(1)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_merge_relationship_on_create_set_deduplicates_pending_relationships() {
    let path = unique_test_dir("merge_relationship_on_create_transaction_batch");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'm1'})").unwrap();
        db.query("CREATE (:Label {id: 'l1'})").unwrap();
        let mut tx = db.begin_transaction();
        tx.query(
                "MATCH (m:Memory {id: 'm1'}), (l:Label {id: 'l1'}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = 'system'",
            )
            .unwrap();
        tx.query(
                "MATCH (m:Memory {id: 'm1'}), (l:Label {id: 'l1'}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = 'duplicate'",
            )
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(false)));
        assert_eq!(output.rows[0].get("rel_id"), output.rows[1].get("rel_id"));
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.matches("create_rel").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
                .query(
                    "MATCH (:Memory)-[r:HAS_LABEL]->(:Label) RETURN count(r) AS total, min(r.assigned_by) AS assigned_by",
                )
                .unwrap();
        assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));
        assert_eq!(
            output.rows[0].get("assigned_by"),
            Some(&Value::String("system".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_merge_relationship_deduplicates_pending_pattern() {
    let path = unique_test_dir("merge_relationship_transaction_batch");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
        tx.query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(false)));
        assert_eq!(output.rows[0].get("rel_id"), output.rows[1].get("rel_id"));
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

#[test]
fn transaction_relationship_replay_sees_pending_node_upsert() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source-v1'})").unwrap();

    for _ in 0..2 {
        let commit_epoch_before = db.store.commit_epoch();
        let mut transaction = db.begin_transaction();
        transaction
            .query("MERGE (:Source {id: 'source-v2'})")
            .unwrap();
        transaction
            .query(
                "MATCH (:Source {id: 'source-v2'})-[revision:REVISED_AS]->(:Source {id: 'source-v1'})
                 DELETE revision",
            )
            .unwrap();
        transaction
            .query(
                "MATCH (newer:Source {id: 'source-v2'}), (older:Source {id: 'source-v1'})
                 CREATE (newer)-[:REVISED_AS {revision_type: 'update'}]->(older)",
            )
            .unwrap();
        transaction.commit().unwrap();
        assert_eq!(db.store.commit_epoch(), commit_epoch_before + 1);
    }

    let output = db
        .query(
            "MATCH (newer:Source {id: 'source-v2'})-[revision:REVISED_AS]->(older:Source {id: 'source-v1'})
             RETURN revision.revision_type AS revision_type",
        )
        .unwrap();
    assert_eq!(
        output.rows,
        vec![BTreeMap::from([(
            "revision_type".to_string(),
            Value::String("update".to_string()),
        )])]
    );
}

#[test]
fn transaction_set_updates_pending_node_before_relationship_match() {
    let path = unique_test_dir("pending_set_before_relationship_match");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Source {id: 'source-v1'})").unwrap();
        let mut transaction = db.begin_transaction();
        transaction
            .query("MERGE (:Source {id: 'source-v2'})")
            .unwrap();
        transaction
            .query(
                "MATCH (s:Source {id: 'source-v2'})
                 SET s.lifecycle_state = 'indexed', s.version = s.version + 1",
            )
            .unwrap();
        transaction
            .query(
                "MATCH (newer:Source {id: 'source-v2', lifecycle_state: 'indexed'}), (older:Source {id: 'source-v1'})
                 CREATE (newer)-[:REVISED_AS {revision_type: 'update'}]->(older)",
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 3);
    assert_eq!(wal.matches("create_node").count(), 2);
    assert_eq!(wal.matches("set_node_property").count(), 0);
    assert_eq!(wal.matches("create_rel").count(), 1);

    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query(
                "MATCH (newer:Source {id: 'source-v2'})-[revision:REVISED_AS]->(:Source {id: 'source-v1'})
                 RETURN newer.lifecycle_state AS state, newer.version AS version, revision.revision_type AS revision_type",
            )
            .unwrap();
        assert_eq!(
            output.rows,
            vec![BTreeMap::from([
                ("state".to_string(), Value::String("indexed".to_string())),
                ("version".to_string(), Value::Int(1)),
                (
                    "revision_type".to_string(),
                    Value::String("update".to_string())
                ),
            ])]
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_set_updates_pending_relationship_properties() {
    let path = unique_test_dir("pending_relationship_set");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Source {id: 'source-v1'})").unwrap();
        db.query("CREATE (:Source {id: 'source-v2'})").unwrap();
        let mut transaction = db.begin_transaction();
        transaction
            .query(
                "MATCH (newer:Source {id: 'source-v2'}), (older:Source {id: 'source-v1'})
                 CREATE (newer)-[:REVISED_AS {revision_type: 'update'}]->(older)",
            )
            .unwrap();
        transaction
            .query(
                "MATCH (:Source {id: 'source-v2'})-[revision:REVISED_AS {revision_type: 'update'}]->(:Source {id: 'source-v1'})
                 SET revision.detected_by = 'filename_match'",
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.matches("create_rel").count(), 1);
    assert_eq!(wal.matches("set_rel_property").count(), 0);

    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query(
                "MATCH (:Source {id: 'source-v2'})-[revision:REVISED_AS]->(:Source {id: 'source-v1'})
                 RETURN revision.revision_type AS revision_type, revision.detected_by AS detected_by",
            )
            .unwrap();
        assert_eq!(
            output.rows,
            vec![BTreeMap::from([
                (
                    "detected_by".to_string(),
                    Value::String("filename_match".to_string())
                ),
                (
                    "revision_type".to_string(),
                    Value::String("update".to_string())
                ),
            ])]
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_delete_removes_pending_relationship_create() {
    let path = unique_test_dir("pending_relationship_delete");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Source {id: 'source-v1'})").unwrap();
        db.query("CREATE (:Source {id: 'source-v2'})").unwrap();
        let commit_epoch_before = db.commit_epoch();
        let mut transaction = db.begin_transaction();
        transaction
            .query(
                "MATCH (newer:Source {id: 'source-v2'}), (older:Source {id: 'source-v1'})
                 CREATE (newer)-[:REVISED_AS {revision_type: 'update'}]->(older)",
            )
            .unwrap();
        transaction
            .query(
                "MATCH (:Source {id: 'source-v2'})-[revision:REVISED_AS {revision_type: 'update'}]->(:Source {id: 'source-v1'})
                 DELETE revision",
            )
            .unwrap();
        transaction.commit().unwrap();
        assert_eq!(db.commit_epoch(), commit_epoch_before);
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.matches("create_rel").count(), 0);
    assert_eq!(wal.matches("delete_rel").count(), 0);

    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query(
                "MATCH (:Source {id: 'source-v2'})-[revision:REVISED_AS]->(:Source {id: 'source-v1'})
                 RETURN revision.revision_type AS revision_type",
            )
            .unwrap();
        assert!(output.rows.is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_delete_removes_pending_node_create() {
    let path = unique_test_dir("pending_node_delete");
    {
        let mut db = Database::open(&path).unwrap();
        let commit_epoch_before = db.commit_epoch();
        let mut transaction = db.begin_transaction();
        transaction
            .query("MERGE (s:Source {id: 'source-temp'})")
            .unwrap();
        transaction
            .query("MATCH (s:Source {id: 'source-temp'}) SET s.lifecycle_state = 'indexed'")
            .unwrap();
        transaction
            .query("MATCH (s:Source {id: 'source-temp'}) DELETE s")
            .unwrap();
        transaction.commit().unwrap();
        assert_eq!(db.commit_epoch(), commit_epoch_before);
    }

    let wal = read_test_wal(&path).unwrap_or_default();
    assert!(!wal.contains("source-temp"));
    assert_eq!(wal.matches("create_node").count(), 0);
    assert_eq!(wal.matches("set_node_property").count(), 0);
    assert_eq!(wal.matches("delete_node").count(), 0);

    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (s:Source {id: 'source-temp'}) RETURN s.id AS id")
            .unwrap();
        assert!(output.rows.is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_delete_pending_node_rejects_pending_relationship_without_detach() {
    let mut db = Database::new();
    let mut transaction = db.begin_transaction();
    transaction
        .query(
            "MERGE (:Source {id: 'source-v2'})-[:REVISED_AS {revision_type: 'update'}]->(:Source {id: 'source-v1'})",
        )
        .unwrap();
    let error = transaction
        .query("MATCH (s:Source {id: 'source-v2'}) DELETE s")
        .expect_err("transaction workspace must reject a non-detach delete immediately");
    assert!(error.to_string().contains("DETACH DELETE"));
    transaction.rollback();
}

#[test]
fn transaction_detach_delete_removes_pending_node_and_relationship_create() {
    let path = unique_test_dir("pending_node_detach_delete");
    {
        let mut db = Database::open(&path).unwrap();
        let commit_epoch_before = db.commit_epoch();
        let mut transaction = db.begin_transaction();
        transaction
            .query(
                "MERGE (:Source {id: 'source-v2'})-[:REVISED_AS {revision_type: 'update'}]->(:Source {id: 'source-v1'})",
            )
            .unwrap();
        transaction
            .query("MATCH (s:Source {id: 'source-v2'}) DETACH DELETE s")
            .unwrap();
        transaction.commit().unwrap();
        assert_eq!(db.commit_epoch(), commit_epoch_before + 1);
    }

    let wal = read_test_wal(&path).unwrap();
    assert!(!wal.contains("source-v2"));
    assert_eq!(wal.matches("create_rel").count(), 0);

    {
        let mut db = Database::open(&path).unwrap();
        let source = db
            .query("MATCH (s:Source {id: 'source-v2'}) RETURN s.id AS id")
            .unwrap();
        assert!(source.rows.is_empty());
        let relationship = db
            .query("MATCH (:Source)-[revision:REVISED_AS]->(:Source) RETURN revision.revision_type AS revision_type")
            .unwrap();
        assert!(relationship.rows.is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_detach_delete_target_nodes_sees_pending_relationship() {
    let path = unique_test_dir("pending_relationship_target_delete");
    {
        let mut db = Database::open(&path).unwrap();
        let commit_epoch_before = db.commit_epoch();
        let mut transaction = db.begin_transaction();
        transaction
            .query("MERGE (:Source {id: 'source-v2'})-[:REVISED_AS]->(:Source {id: 'source-v1'})")
            .unwrap();
        transaction
            .query(
                "MATCH (newer:Source {id: 'source-v2'})-[:REVISED_AS]->(older:Source {id: 'source-v1'})
                 DETACH DELETE older",
            )
            .unwrap();
        transaction.commit().unwrap();
        assert_eq!(db.commit_epoch(), commit_epoch_before + 1);
    }

    let wal = read_test_wal(&path).unwrap_or_default();
    assert!(!wal.contains("source-v1"));
    assert_eq!(wal.matches("create_rel").count(), 0);

    {
        let mut db = Database::open(&path).unwrap();
        let deleted_target = db
            .query("MATCH (s:Source {id: 'source-v1'}) RETURN s.id AS id")
            .unwrap();
        assert!(deleted_target.rows.is_empty());
        let source = db
            .query("MATCH (s:Source {id: 'source-v2'}) RETURN s.id AS id")
            .unwrap();
        assert_eq!(source.rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_retarget_to_matched_pending_target_sees_pending_old_relationship() {
    let path = unique_test_dir("pending_retarget_to_matched_target");
    {
        let mut db = Database::open(&path).unwrap();
        let mut transaction = db.begin_transaction();
        transaction
            .query("MERGE (:Memory {id: 'm1'})-[:HAS_LABEL]->(:Label {id: 'src'})")
            .unwrap();
        transaction.query("MERGE (:Label {id: 'dst'})").unwrap();
        transaction
            .query(
                "MATCH (m:Memory {id: 'm1'})-[:HAS_LABEL]->(src:Label {id: 'src'})
                 MATCH (dst:Label {id: 'dst'})
                 MERGE (m)-[r:HAS_LABEL]->(dst)
                 ON CREATE SET r.assigned_by = 'label_merge'",
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 2);
    assert_eq!(wal.matches("create_node").count(), 3);
    assert_eq!(wal.matches("create_rel").count(), 2);

    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query(
                "MATCH (:Memory {id: 'm1'})-[r:HAS_LABEL]->(label:Label)
                 RETURN count(r) AS total, min(r.assigned_by) AS assigned_by",
            )
            .unwrap();
        assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
        assert_eq!(
            output.rows[0].get("assigned_by"),
            Some(&Value::String("label_merge".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_retarget_from_pending_source_sees_pending_old_relationship() {
    let path = unique_test_dir("pending_retarget_from_matched_source");
    {
        let mut db = Database::open(&path).unwrap();
        let mut transaction = db.begin_transaction();
        transaction
            .query("MERGE (:Memory {id: 'old'})-[:HAS_LABEL]->(:Label {id: 'shared'})")
            .unwrap();
        transaction.query("MERGE (:Memory {id: 'new'})").unwrap();
        transaction
            .query(
                "MATCH (old:Memory {id: 'old'})-[:HAS_LABEL]->(label:Label {id: 'shared'}),
                       (new:Memory {id: 'new'})
                 MERGE (new)-[r:HAS_LABEL]->(label)
                 ON CREATE SET r.assigned_by = 'inherit'",
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 2);
    assert_eq!(wal.matches("create_node").count(), 3);
    assert_eq!(wal.matches("create_rel").count(), 2);

    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query(
                "MATCH (:Memory {id: 'new'})-[r:HAS_LABEL]->(:Label {id: 'shared'})
                 RETURN r.assigned_by AS assigned_by",
            )
            .unwrap();
        assert_eq!(
            output.rows,
            vec![BTreeMap::from([(
                "assigned_by".to_string(),
                Value::String("inherit".to_string())
            )])]
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_copy_merge_sees_pending_matched_relationship_properties() {
    let path = unique_test_dir("pending_copy_merge_relationship");
    {
        let mut db = Database::open(&path).unwrap();
        let mut transaction = db.begin_transaction();
        transaction
            .query(
                "MERGE (:Memory {id: 'child'})-[:CRYSTALLIZED_FROM {contribution_weight: 7}]->(:Memory {id: 'source'})",
            )
            .unwrap();
        transaction
            .query(
                "MATCH (child:Memory {id: 'child'})-[old:CRYSTALLIZED_FROM]->(source:Memory {id: 'source'})
                 MERGE (child)-[new:SYNTHESIZED_FROM]->(source)
                 ON CREATE SET new.weight = old.contribution_weight",
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 2);
    assert_eq!(wal.matches("create_node").count(), 2);
    assert_eq!(wal.matches("create_rel").count(), 2);

    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query(
                "MATCH (:Memory {id: 'child'})-[new:SYNTHESIZED_FROM]->(:Memory {id: 'source'})
                 RETURN new.weight AS weight",
            )
            .unwrap();
        assert_eq!(
            output.rows,
            vec![BTreeMap::from([("weight".to_string(), Value::Int(7))])]
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}
