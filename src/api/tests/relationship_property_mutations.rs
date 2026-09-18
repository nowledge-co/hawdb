use super::*;

#[test]
fn relationship_delete_filters_target_node_property_patterns() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:HAS_LABEL]->(:Label {id: 'keep', name: 'Keep'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'drop', name: 'Drop'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 1}), (l:Label {id: 'drop'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();

    let deleted = db
        .query_with_params(
            "MATCH (m:Memory {id: $memory_id})-[r:HAS_LABEL]->(l:Label {id: $label_id}) DELETE r",
            &BTreeMap::from([
                ("memory_id".to_string(), Value::Int(1)),
                ("label_id".to_string(), Value::String("drop".to_string())),
            ]),
        )
        .unwrap();
    assert_eq!(deleted.rows.len(), 1);

    let remaining = db
        .query("MATCH (m:Memory {id: 1})-[r:HAS_LABEL]->(l:Label) RETURN l.id AS id")
        .unwrap();
    assert_eq!(remaining.rows.len(), 1);
    assert_eq!(
        remaining.rows[0].get("id"),
        Some(&Value::String("keep".to_string()))
    );
}

#[test]
fn detach_delete_after_relationship_match_removes_target_nodes() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Thread {id: 'thread-1'})-[:CONTAINS]->(:Message {id: 'msg-1', order_index: 1})",
    )
    .unwrap();
    db.query("CREATE (:Message {id: 'msg-orphan', order_index: 2})")
        .unwrap();

    let deleted = db
        .query_with_params(
            "MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) DETACH DELETE m",
            &BTreeMap::from([(
                "thread_uuid".to_string(),
                Value::String("thread-1".to_string()),
            )]),
        )
        .unwrap();
    assert_eq!(deleted.rows.len(), 1);

    let messages = db
        .query("MATCH (m:Message) RETURN m.id AS id ORDER BY id")
        .unwrap();
    assert_eq!(messages.rows.len(), 1);
    assert_eq!(
        messages.rows[0].get("id"),
        Some(&Value::String("msg-orphan".to_string()))
    );
    let thread = db
        .query("MATCH (t:Thread {id: 'thread-1'}) RETURN count(t) AS total")
        .unwrap();
    assert_eq!(thread.rows[0].get("total"), Some(&Value::Int(1)));
}

#[test]
fn set_relationship_property_updates_edge_and_keeps_endpoint_nodes() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
    )
    .unwrap();

    let output = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 SET r.weight = 2")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("rel_id"), Some(&Value::Int(0)));

    let relationship = db.store.scan_relationships(None).next().unwrap();
    assert_eq!(relationship.properties.get("weight"), Some(&Value::Int(2)));
    let source = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        source.rows[0].get("title"),
        Some(&Value::String("Graph foundations".to_string()))
    );
    let target = db
        .query("MATCH (e:Entity) WHERE e.id = 10 RETURN e.name AS name")
        .unwrap();
    assert_eq!(
        target.rows[0].get("name"),
        Some(&Value::String("Neo4j".to_string()))
    );
}

#[test]
fn relationship_pattern_property_filters_reads_with_parameters() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 2, title: 'Runtime strategy'})-[:MENTIONS {weight: 2}]->(:Entity {id: 11, name: 'Cypher'})",
    )
    .unwrap();

    let output = db
        .query_with_params(
            "MATCH (m:Memory)-[r:MENTIONS {weight: $weight}]->(e:Entity) RETURN e.name AS entity, r.weight AS weight",
            &BTreeMap::from([("weight".to_string(), Value::Int(2))]),
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("entity"),
        Some(&Value::String("Cypher".to_string()))
    );
    assert_eq!(output.rows[0].get("weight"), Some(&Value::Int(2)));
}

#[test]
fn relationship_pattern_property_filters_set_and_delete() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 2, title: 'Runtime strategy'})-[:MENTIONS {weight: 2}]->(:Entity {id: 11, name: 'Cypher'})",
    )
    .unwrap();

    let updated = db
        .query("MATCH (m:Memory)-[r:MENTIONS {weight: 1}]->(e:Entity) SET r.weight = 9")
        .unwrap();
    assert_eq!(updated.rows.len(), 1);
    let after_set = db
        .query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN e.name AS entity, r.weight AS weight ORDER BY weight ASC",
        )
        .unwrap();
    assert_eq!(after_set.rows.len(), 2);
    assert_eq!(
        after_set.rows[0].get("entity"),
        Some(&Value::String("Cypher".to_string()))
    );
    assert_eq!(after_set.rows[0].get("weight"), Some(&Value::Int(2)));
    assert_eq!(
        after_set.rows[1].get("entity"),
        Some(&Value::String("Neo4j".to_string()))
    );
    assert_eq!(after_set.rows[1].get("weight"), Some(&Value::Int(9)));

    let deleted = db
        .query("MATCH (m:Memory)-[r:MENTIONS {weight: 2}]->(e:Entity) DELETE r")
        .unwrap();
    assert_eq!(deleted.rows.len(), 1);
    let remaining = db
        .query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN e.name AS entity, r.weight AS weight",
        )
        .unwrap();
    assert_eq!(remaining.rows.len(), 1);
    assert_eq!(
        remaining.rows[0].get("entity"),
        Some(&Value::String("Neo4j".to_string()))
    );
    assert_eq!(remaining.rows[0].get("weight"), Some(&Value::Int(9)));
}

#[test]
fn relationship_mutation_where_filters_relationship_properties() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 2}]->(:Entity {id: 11, name: 'Cypher'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 2, title: 'Runtime strategy'})-[:MENTIONS {weight: 1}]->(:Entity {id: 12, name: 'Rust'})",
    )
    .unwrap();

    let updated = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 AND r.weight = 1 SET r.weight = 9")
        .unwrap();
    assert_eq!(updated.rows.len(), 1);

    let after_set = db
        .query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN e.name AS entity, r.weight AS weight ORDER BY entity ASC",
        )
        .unwrap();
    assert_eq!(after_set.rows.len(), 3);
    assert_eq!(
        after_set.rows[0].get("entity"),
        Some(&Value::String("Cypher".to_string()))
    );
    assert_eq!(after_set.rows[0].get("weight"), Some(&Value::Int(2)));
    assert_eq!(
        after_set.rows[1].get("entity"),
        Some(&Value::String("Neo4j".to_string()))
    );
    assert_eq!(after_set.rows[1].get("weight"), Some(&Value::Int(9)));
    assert_eq!(
        after_set.rows[2].get("entity"),
        Some(&Value::String("Rust".to_string()))
    );
    assert_eq!(after_set.rows[2].get("weight"), Some(&Value::Int(1)));

    let deleted = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE r.weight = 2 DELETE r")
        .unwrap();
    assert_eq!(deleted.rows.len(), 1);

    let remaining = db
        .query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN e.name AS entity, r.weight AS weight ORDER BY entity ASC",
        )
        .unwrap();
    assert_eq!(remaining.rows.len(), 2);
    assert_eq!(
        remaining.rows[0].get("entity"),
        Some(&Value::String("Neo4j".to_string()))
    );
    assert_eq!(remaining.rows[0].get("weight"), Some(&Value::Int(9)));
    assert_eq!(
        remaining.rows[1].get("entity"),
        Some(&Value::String("Rust".to_string()))
    );
    assert_eq!(remaining.rows[1].get("weight"), Some(&Value::Int(1)));
}

#[test]
fn relationship_mutation_rejects_mixed_variable_or_predicates() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 1}]->(:Entity {id: 10, name: 'Neo4j'})",
    )
    .unwrap();

    let error = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 OR r.weight = 1 SET r.weight = 9")
        .unwrap_err();
    assert!(error.to_string().contains(
        "relationship mutation OR predicates cannot mix node and relationship variables"
    ));
}

#[test]
fn bounded_relationship_pattern_properties_are_rejected() {
    let db = Database::new();
    let error = db
        .explain_query(
            "MATCH (m:Memory)-[:MENTIONS*1..2 {weight: 1}]->(e:Entity) RETURN e.name AS entity",
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("relationship property patterns are supported only for one-hop patterns"));
}

#[test]
fn returns_filters_orders_and_counts_relationship_properties() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'One'})-[:MENTIONS {weight: 2}]->(:Entity {id: 10, name: 'Rust'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 2, title: 'Two'})-[:MENTIONS {weight: 5}]->(:Entity {id: 11, name: 'Kuzu'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 3, title: 'Three'})-[:MENTIONS]->(:Entity {id: 12, name: 'Neo4j'})",
    )
    .unwrap();

    let output = db
        .query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE r.weight > 1 RETURN e.name AS entity, r.weight AS weight ORDER BY r.weight DESC",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("entity"),
        Some(&Value::String("Kuzu".to_string()))
    );
    assert_eq!(output.rows[0].get("weight"), Some(&Value::Int(5)));
    assert_eq!(
        output.rows[1].get("entity"),
        Some(&Value::String("Rust".to_string()))
    );
    assert_eq!(output.rows[1].get("weight"), Some(&Value::Int(2)));

    let output = db
        .query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN count(r) AS rels, count(r.weight) AS weighted",
        )
        .unwrap();
    assert_eq!(output.rows[0].get("rels"), Some(&Value::Int(3)));
    assert_eq!(output.rows[0].get("weighted"), Some(&Value::Int(2)));

    let output = db
        .query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN count(DISTINCT m) AS memories, count(DISTINCT r.weight) AS weights",
        )
        .unwrap();
    assert_eq!(output.rows[0].get("memories"), Some(&Value::Int(3)));
    assert_eq!(output.rows[0].get("weights"), Some(&Value::Int(2)));

    let output = db
        .query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN id(m) AS memory_id, id(r) AS rel_id, e.name AS entity ORDER BY rel_id DESC",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.rows[0].get("rel_id"), Some(&Value::Int(2)));
    assert_eq!(
        output.rows[0].get("entity"),
        Some(&Value::String("Neo4j".to_string()))
    );
    assert_eq!(output.rows[2].get("memory_id"), Some(&Value::Int(0)));
    assert_eq!(output.rows[2].get("rel_id"), Some(&Value::Int(0)));

    let output = db
        .query_with_params(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE id(m) = $memory_id AND id(r) IN [0, $rel_id] RETURN id(r) AS rel_id, e.name AS entity ORDER BY id(r) DESC",
            &BTreeMap::from([
                ("memory_id".to_string(), Value::Int(0)),
                ("rel_id".to_string(), Value::Int(2)),
            ]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("rel_id"), Some(&Value::Int(0)));
    assert_eq!(
        output.rows[0].get("entity"),
        Some(&Value::String("Rust".to_string()))
    );

    let updated = db
        .query("MATCH (m:Memory) WHERE id(m) = 0 SET m.title = 'Updated'")
        .unwrap();
    assert_eq!(updated.rows.len(), 1);
    assert_eq!(updated.rows[0].get("node_id"), Some(&Value::Int(0)));
    let output = db
        .query("MATCH (m:Memory) WHERE id(m) = 0 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Updated".to_string()))
    );

    let updated = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE id(r) = 1 SET r.weight = 9")
        .unwrap();
    assert_eq!(updated.rows.len(), 1);
    assert_eq!(updated.rows[0].get("rel_id"), Some(&Value::Int(1)));
    let output = db
        .query(
            "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE id(r) = 1 RETURN r.weight AS weight",
        )
        .unwrap();
    assert_eq!(output.rows[0].get("weight"), Some(&Value::Int(9)));

    let deleted = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE id(r) = 2 DELETE r")
        .unwrap();
    assert_eq!(deleted.rows.len(), 1);
    assert_eq!(deleted.rows[0].get("rel_id"), Some(&Value::Int(2)));
    let output = db
        .query("MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN id(r) AS rel_id")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert!(output
        .rows
        .iter()
        .all(|row| row.get("rel_id") != Some(&Value::Int(2))));
}
