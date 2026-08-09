use super::*;

#[test]
fn transaction_rollback_discards_buffered_mutations() {
    let mut db = Database::new();
    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        tx.rollback();
    }

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn transaction_commit_applies_buffered_mutations() {
    let mut db = Database::new();
    let output = {
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        tx.query(
            "CREATE (:Memory {id: 2, title: 'Runtime strategy'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
        )
        .unwrap();
        tx.commit().unwrap()
    };

    assert_eq!(output.rows.len(), 2);
    let output = db
        .query(
            "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE e.id = 10 RETURN m.title AS memory, e.name AS entity",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("memory"),
        Some(&Value::String("Runtime strategy".to_string()))
    );
}

#[test]
fn transaction_commit_replays_as_one_wal_batch() {
    let path = unique_test_dir("transaction_batch_wal");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        tx.query(
            "CREATE (:Memory {id: 2, title: 'Runtime strategy'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
        )
        .unwrap();
        tx.commit().unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 2);
    assert!(wal.contains("\tbatch\t"));
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.title AS memory, e.name AS entity",
            )
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("entity"),
            Some(&Value::String("Rust".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_rejects_reads() {
    let mut db = Database::new();
    let mut tx = db.begin_transaction();
    let error = tx
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap_err();
    assert!(error.to_string().contains("must be a mutation"));
}

#[test]
fn transaction_commit_updates_property_index() {
    let mut db = Database::new();
    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 42, title: 'Indexed memory'})")
            .unwrap();
        for id in 100..116 {
            tx.query(&format!(
                "CREATE (:Memory {{id: {id}, title: 'Indexed extra {id}'}})"
            ))
            .unwrap();
        }
        tx.commit().unwrap();
    }

    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 42 RETURN m.title AS title")
        .unwrap();
    assert!(explain.trace.selected_plan.contains("IndexNodeSeek"));
    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 42 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Indexed memory".to_string()))
    );
}
