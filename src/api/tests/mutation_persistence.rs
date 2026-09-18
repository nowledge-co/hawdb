use super::*;

#[test]
fn delete_persists_and_replays_from_wal() {
    let path = unique_test_dir("delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Transient'})")
            .unwrap();
        db.query("MATCH (m:Memory) WHERE m.id = 1 DELETE m")
            .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert!(output.rows.is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relationship_delete_persists_and_replays_from_wal() {
    let path = unique_test_dir("relationship_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();
        db.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 DELETE r")
            .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_rel"));
    {
        let mut db = Database::open(&path).unwrap();
        let rels = db
            .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
            .unwrap();
        assert!(rels.rows.is_empty());
        let source = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(source.rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relationship_set_persists_and_replays_from_wal() {
    let path = unique_test_dir("relationship_set_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();
        db.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 SET r.weight = 2")
            .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_rel_property"));
    {
        let db = Database::open(&path).unwrap();
        let relationship = db.store.scan_relationships(None).next().unwrap();
        assert_eq!(relationship.properties.get("weight"), Some(&Value::Int(2)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_delete_commits_and_rolls_back() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Original'})")
        .unwrap();
    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory) WHERE m.id = 1 DELETE m")
            .unwrap();
        tx.rollback();
    }
    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(output.rows.len(), 1);

    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory) WHERE m.id = 1 DELETE m")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn transaction_relationship_delete_commits_and_rolls_back() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
    )
    .unwrap();
    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 DELETE r")
            .unwrap();
        tx.rollback();
    }
    let rels = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
        .unwrap();
    assert_eq!(rels.rows.len(), 1);

    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 DELETE r")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    let rels = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
        .unwrap();
    assert!(rels.rows.is_empty());
}

#[test]
fn transaction_detach_delete_after_relationship_match_commits_and_rolls_back() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread-1'})-[:CONTAINS]->(:Message {id: 'msg-1'})")
        .unwrap();
    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (t:Thread {id: 'thread-1'})-[:CONTAINS]->(m:Message) DETACH DELETE m")
            .unwrap();
        tx.rollback();
    }
    let messages = db
        .query("MATCH (m:Message) RETURN count(m) AS total")
        .unwrap();
    assert_eq!(messages.rows[0].get("total"), Some(&Value::Int(1)));

    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (t:Thread {id: 'thread-1'})-[:CONTAINS]->(m:Message) DETACH DELETE m")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    let messages = db
        .query("MATCH (m:Message) RETURN count(m) AS total")
        .unwrap();
    assert_eq!(messages.rows[0].get("total"), Some(&Value::Int(0)));
}

#[test]
fn transaction_relationship_set_commits_and_rolls_back() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
    )
    .unwrap();
    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 SET r.weight = 2")
            .unwrap();
        tx.rollback();
    }
    let relationship = db.store.scan_relationships(None).next().unwrap();
    assert_eq!(relationship.properties.get("weight"), Some(&Value::Int(1)));

    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 SET r.weight = 3")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    let relationship = db.store.scan_relationships(None).next().unwrap();
    assert_eq!(relationship.properties.get("weight"), Some(&Value::Int(3)));
}
