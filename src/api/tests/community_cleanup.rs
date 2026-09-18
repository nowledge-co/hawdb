use super::*;

#[test]
fn deletes_communities_for_nowledge_replace_cleanup() {
    let mut db = Database::new();
    db.query("CREATE (:Community {id: 'community_1', name: 'One'})")
        .unwrap();
    db.query("CREATE (:Community {id: 'community_2', name: 'Two'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();

    let output = db
        .delete_knowledge_communities(&KnowledgeCommunityCleanupRequest { detach: false })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.candidate_count, 2);
    assert_eq!(output.deleted_count, 2);
    assert_eq!(output.rows.len(), 2);
    assert!(output.rows.iter().all(|row| row.deleted));
    assert_eq!(output.rows[0].id.as_deref(), Some("community_1"));
    assert_eq!(output.rows[1].id.as_deref(), Some("community_2"));

    let communities = db
        .query("MATCH (c:Community) RETURN count(c) AS total")
        .unwrap();
    assert_eq!(communities.rows[0].get("total"), Some(&Value::Int(0)));
    let entities = db
        .query("MATCH (e:Entity) RETURN count(e) AS total")
        .unwrap();
    assert_eq!(entities.rows[0].get("total"), Some(&Value::Int(1)));
}

#[test]
fn detach_deletes_communities_for_nowledge_undo_cleanup() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_1'})-[:BELONGS_TO]->(:Community {id: 'community_1'})")
        .unwrap();
    db.query("CREATE (:Community {id: 'community_2'})").unwrap();

    let output = db
        .delete_knowledge_communities(&KnowledgeCommunityCleanupRequest { detach: true })
        .unwrap();

    assert_eq!(output.candidate_count, 2);
    assert_eq!(output.deleted_count, 2);
    let communities = db
        .query("MATCH (c:Community) RETURN count(c) AS total")
        .unwrap();
    assert_eq!(communities.rows[0].get("total"), Some(&Value::Int(0)));
    let entities = db
        .query("MATCH (e:Entity) RETURN count(e) AS total")
        .unwrap();
    assert_eq!(entities.rows[0].get("total"), Some(&Value::Int(1)));
    let relationships = db
        .query("MATCH (:Entity)-[r:BELONGS_TO]->(:Community) RETURN count(r) AS total")
        .unwrap();
    assert_eq!(relationships.rows[0].get("total"), Some(&Value::Int(0)));
}

#[test]
fn community_cleanup_without_candidates_does_not_write_wal() {
    let path = unique_test_dir("community_cleanup_without_candidates");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
    }
    let wal_before = read_test_wal(&path).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let graph_commit_epoch_before = db.store.commit_epoch();
        let output = db
            .delete_knowledge_communities(&KnowledgeCommunityCleanupRequest { detach: true })
            .unwrap();
        assert_eq!(output.graph_commit_epoch_before, graph_commit_epoch_before);
        assert_eq!(output.graph_commit_epoch_after, graph_commit_epoch_before);
        assert_eq!(output.candidate_count, 0);
        assert_eq!(output.deleted_count, 0);
    }
    let wal_after = read_test_wal(&path).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_community_cleanup_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_community_cleanup_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Entity {id: 'entity_1'})-[:BELONGS_TO]->(:Community {id: 'community_1'})",
        )
        .unwrap();
        db.query("CREATE (:Community {id: 'community_2'})").unwrap();
    }
    let setup_wal = read_test_wal(&path).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .delete_knowledge_communities(&KnowledgeCommunityCleanupRequest { detach: true })
            .unwrap();
        assert_eq!(output.deleted_count, 2);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    assert!(wal.contains("delete_rel"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let mut db = Database::open(&path).unwrap();
        let communities = db
            .query("MATCH (c:Community) RETURN count(c) AS total")
            .unwrap();
        assert_eq!(communities.rows[0].get("total"), Some(&Value::Int(0)));
        let entities = db
            .query("MATCH (e:Entity) RETURN count(e) AS total")
            .unwrap();
        assert_eq!(entities.rows[0].get("total"), Some(&Value::Int(1)));
    }
    std::fs::remove_dir_all(path).unwrap();
}
