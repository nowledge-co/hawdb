use super::*;

#[test]
fn undirected_one_hop_relationship_patterns_match_both_directions() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:EVOLVES]->(:Memory {id: 2})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3})-[:EVOLVES]->(:Memory {id: 1})")
        .unwrap();

    let output = db
        .query(
            "MATCH (m:Memory {id: 1})-[:EVOLVES]-(other:Memory) RETURN DISTINCT other.id AS id ORDER BY id ASC",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(3)));

    let error = db
        .query("MATCH (m:Memory)-[:EVOLVES*1..2]-(other:Memory) RETURN other.id AS id")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("non-outgoing relationship patterns are supported only for one-hop patterns"));
}

#[test]
fn one_hop_relationship_patterns_follow_ordered_adjacency_view() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 0})").unwrap();
    db.query("CREATE (:Memory {id: 10})").unwrap();
    db.query("CREATE (:Memory {id: 11})").unwrap();
    db.query("CREATE (:Memory {id: 12})").unwrap();
    db.query("CREATE (:Entity {id: 1})").unwrap();
    db.query("CREATE (:Entity {id: 2})").unwrap();
    db.query("CREATE (:Entity {id: 3})").unwrap();
    db.query("CREATE (:Entity {id: 99})").unwrap();
    db.query("MATCH (m:Memory {id: 0}), (e:Entity {id: 3}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 0}), (e:Entity {id: 1}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 0}), (e:Entity {id: 2}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 12}), (e:Entity {id: 99}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 10}), (e:Entity {id: 99}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 11}), (e:Entity {id: 99}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();

    let outgoing = db
        .query("MATCH (m:Memory {id: 0})-[:MENTIONS]->(e:Entity) RETURN e.id AS id")
        .unwrap();
    let incoming = db
        .query("MATCH (e:Entity {id: 99})<-[:MENTIONS]-(m:Memory) RETURN m.id AS id")
        .unwrap();

    assert_eq!(
        outgoing
            .rows
            .iter()
            .map(|row| row.get("id").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(2), Value::Int(3)]
    );
    assert_eq!(
        incoming
            .rows
            .iter()
            .map(|row| row.get("id").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![Value::Int(10), Value::Int(11), Value::Int(12)]
    );
}

#[test]
fn incoming_one_hop_relationship_patterns_match_nowledge_reads() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:MENTIONS {weight: 3}]->(:Entity {id: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2})-[:MENTIONS {weight: 4}]->(:Entity {id: 11})")
        .unwrap();

    let output = db
        .query("MATCH (e:Entity {id: 10})<-[:MENTIONS]-(m:Memory) RETURN m.id AS id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));

    let output = db
        .query("MATCH (e:Entity {id: 10})<-[r:MENTIONS]-(m:Memory) RETURN count(r) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));

    let error = db
        .query("MATCH (e:Entity)<-[:MENTIONS*1..2]-(m:Memory) RETURN m.id AS id")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("non-outgoing relationship patterns are supported only for one-hop patterns"));
}

#[test]
fn untyped_one_hop_relationship_patterns_project_relationship_type() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 10})")
        .unwrap();
    db.query("CREATE (:Entity {id: 10})-[:RELATES_TO]->(:Entity {id: 11})")
        .unwrap();

    let output = db
        .query(
            "MATCH (a)-[r]->(b) WHERE a.id IN [1, 10] AND b.id IN [10, 11] RETURN a.id AS source, b.id AS target, label(r) AS rel_type ORDER BY source ASC",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("source"), Some(&Value::Int(1)));
    assert_eq!(output.rows[0].get("target"), Some(&Value::Int(10)));
    assert_eq!(
        output.rows[0].get("rel_type"),
        Some(&Value::String("MENTIONS".to_string()))
    );
    assert_eq!(output.rows[1].get("source"), Some(&Value::Int(10)));
    assert_eq!(output.rows[1].get("target"), Some(&Value::Int(11)));
    assert_eq!(
        output.rows[1].get("rel_type"),
        Some(&Value::String("RELATES_TO".to_string()))
    );

    let error = db
        .query("MATCH (a)-[r*1..2]->(b) RETURN label(r) AS rel_type")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("untyped relationship patterns are supported only for one-hop patterns"));
}

#[test]
fn untyped_one_hop_relationship_patterns_without_a_variable_match_all_types() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 1})-[:FIRST]->(:Target {id: 10})")
        .unwrap();
    db.query("CREATE (:Source {id: 2})-[:SECOND]->(:Target {id: 20})")
        .unwrap();

    let output = db
        .query(
            "MATCH (a:Source)-[]->(b:Target) RETURN a.id AS source, b.id AS target ORDER BY source ASC",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("source"), Some(&Value::Int(1)));
    assert_eq!(output.rows[0].get("target"), Some(&Value::Int(10)));
    assert_eq!(output.rows[1].get("source"), Some(&Value::Int(2)));
    assert_eq!(output.rows[1].get("target"), Some(&Value::Int(20)));
}

#[test]
fn creates_and_expands_relationships_with_cypher() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 3}]->(:Entity {id: 10, name: 'Neo4j'})",
    )
    .unwrap();

    let output = db
        .query(
            "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE e.id = 10 RETURN m.title AS memory, e.name AS entity",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("memory"),
        Some(&Value::String("Graph foundations".to_string()))
    );
    assert_eq!(
        output.rows[0].get("entity"),
        Some(&Value::String("Neo4j".to_string()))
    );
}

#[test]
fn expands_bounded_relationship_patterns() {
    let mut db = Database::new();
    db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
        .unwrap();
    db.query("MERGE (:Entity {id: 2, name: 'Mid'})-[:LINKS]->(:Entity {id: 3, name: 'Leaf'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory)-[:LINKS*1..2]->(e:Entity) RETURN e.name AS name ORDER BY name ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("name"),
        Some(&Value::String("Leaf".to_string()))
    );
    assert_eq!(
        output.rows[1].get("name"),
        Some(&Value::String("Mid".to_string()))
    );

    let exact = db
        .query("MATCH (m:Memory)-[:LINKS*2]->(e:Entity) RETURN e.name AS name")
        .unwrap();
    assert_eq!(exact.rows.len(), 1);
    assert_eq!(
        exact.rows[0].get("name"),
        Some(&Value::String("Leaf".to_string()))
    );

    let explain = db
        .explain_query("MATCH (m:Memory)-[:LINKS*..2]->(e:Entity) RETURN count(e) AS total")
        .unwrap();
    assert!(explain.trace.selected_plan.contains("hops=1..2"));
    assert!(explain.trace.selected_plan.contains("source=m:Memory"));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand for Memory-[:LINKS*1..2]->Entity: path_count=1")
            && decision.contains("hop_rows=[1:exact:1,2:exact:1]")
            && decision.contains("estimated_rows=2")
    }));
}

#[test]
fn persists_relationship_expansion_across_reopen() {
    let path = unique_test_dir("rel_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.title AS memory, e.name AS entity",
            )
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("memory"),
            Some(&Value::String("Graph foundations".to_string()))
        );
        assert_eq!(
            output.rows[0].get("entity"),
            Some(&Value::String("Neo4j".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relationship_pattern_create_uses_single_wal_batch() {
    let path = unique_test_dir("rel_query_batch_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Neo4j'})",
        )
        .unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 2);
    assert!(wal.contains("\tbatch\t"));
    assert!(wal.contains("create_node"));
    assert!(wal.contains("create_rel"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relationship_variable_rejects_bounded_multi_hop_return() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:LINKS {weight: 1}]->(:Entity {id: 10})")
        .unwrap();

    let error = db
        .query("MATCH (m:Memory)-[r:LINKS*1..2]->(e:Entity) RETURN r.weight AS weight")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("relationship variables are supported only for one-hop patterns"));
}

#[test]
fn anonymous_relationship_endpoints_support_count_reads() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2})-[:MENTIONS]->(:Entity {id: 11})")
        .unwrap();
    db.query("CREATE (:Entity {id: 12})-[:RELATES_TO]->(:Entity {id: 13})")
        .unwrap();

    let output = db
        .query("MATCH ()-[r:MENTIONS]->() RETURN count(r) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));

    let output = db
        .query("MATCH (:Memory)-[r:MENTIONS]->(:Entity) RETURN count(r) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
}
