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

    // 1 -> 2 and 3 -> 1: undirected trails from 1 reach 2 and 3 in one hop
    // and nothing new in two hops, because no relationship is reused.
    let output = db
        .query(
            "MATCH (m:Memory {id: 1})-[:EVOLVES*1..2]-(other:Memory) RETURN other.id AS id ORDER BY id ASC",
        )
        .unwrap();
    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row.get("id").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![Value::Int(2), Value::Int(3)]
    );
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

    let output = db
        .query("MATCH (e:Entity {id: 10})<-[:MENTIONS*1..2]-(m:Memory) RETURN m.id AS id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
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
    // Engine bootstrap is already checkpointed; count the private user WAL.
    assert_eq!(wal.lines().count(), 1);
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

#[test]
fn pipeline_bounded_match_validation_is_independent_of_graph_contents() {
    for populated in [false, true] {
        let mut db = Database::new();
        if populated {
            db.query("CREATE (:Node {id: 1})-[:LINK {weight: 10}]->(:Node {id: 2})")
                .unwrap();
            db.query("CREATE (:Node {id: 3})").unwrap();
            db.query(
                "MATCH (a:Node {id: 2}), (b:Node {id: 3}) CREATE (a)-[:LINK {weight: 20}]->(b)",
            )
            .unwrap();
        }
        for pattern in [
            "(a:Node)-[r:LINK*1..2]->(b:Node)",
            "(a:Node)-[:LINK*1..2 {weight: 10}]->(b:Node)",
            "(a:Node)-[*1..2]->(b:Node)",
            "(a:Node)-[r:ABSENT*0..1]->(b:Node)",
            "(a:Node)-[:ABSENT*0..1 {weight: 10}]->(b:Node)",
            "(a:Node)-[*0..1]->(b:Node)",
        ] {
            let query = format!("MATCH {pattern} WITH a, b WITH a, b RETURN b.id AS id");
            let error = db.query(&query).unwrap_err();
            assert!(
                matches!(error, crate::error::HawDBError::Semantic(ref message)
                if message.contains("supported only for one-hop patterns")),
                "populated={populated}: {query}: {error}"
            );
        }
        let output = db.query("MATCH (a:Node {id: 1})-[:LINK*1..2]->(b:Node) WITH b WITH b RETURN b.id AS id ORDER BY id").unwrap();
        let ids = output
            .rows
            .iter()
            .map(|row| row.get("id").unwrap().clone())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            if populated {
                vec![Value::Int(2), Value::Int(3)]
            } else {
                vec![]
            }
        );
        let output = db.query("MATCH (a:Node {id: 1})-[r:LINK]->(b:Node) WITH b, r WITH b, r RETURN b.id AS id, r.weight AS weight").unwrap();
        assert_eq!(output.rows.len(), usize::from(populated));
        if populated {
            assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
            assert_eq!(output.rows[0].get("weight"), Some(&Value::Int(10)));
        }
    }
}

fn trail_fixture() -> Database {
    // r1: 1 -> 2, r2: 2 -> 1, r3: 1 -> 1 (self-loop).
    let mut db = Database::new();
    db.query("CREATE (:N {id: 1})-[:R]->(:N {id: 2})").unwrap();
    db.query("MATCH (a:N {id: 2}), (b:N {id: 1}) CREATE (a)-[:R]->(b)")
        .unwrap();
    db.query("MATCH (a:N {id: 1}), (b:N {id: 1}) CREATE (a)-[:R]->(b)")
        .unwrap();
    db
}

fn count(db: &mut Database, query: &str) -> Value {
    db.query(query).unwrap().rows[0].get("n").cloned().unwrap()
}

#[test]
fn variable_length_patterns_match_trails_without_repeating_relationships() {
    let mut db = trail_fixture();
    for (pattern, expected) in [
        // Length 1: r1, r3. Length 2: r1 r2, r3 r1. Length 3: r1 r2 r3, r3 r1 r2.
        ("(a:N {id: 1})-[:R*1..3]->(x:N)", 6),
        ("(a:N {id: 1})-[:R*3..3]->(x:N)", 2),
        ("(a:N {id: 1})-[:R*4..4]->(x:N)", 0),
        ("(a:N {id: 1})-[:R*0..1]->(x:N)", 3),
        // The reversed graph has the same shape.
        ("(a:N {id: 1})<-[:R*1..3]-(x:N)", 6),
        ("(a:N {id: 1})<-[:R*3..3]-(x:N)", 2),
        // Undirected: r1, r2 and the self-loop once; then r1 r2, r2 r1,
        // r3 r1 and r3 r2.
        ("(a:N {id: 1})-[:R*1..1]-(x:N)", 3),
        ("(a:N {id: 1})-[:R*1..2]-(x:N)", 7),
        ("(a:N {id: 1})-[:R*2..2]-(x:N)", 4),
    ] {
        for query in [
            format!("MATCH {pattern} RETURN count(*) AS n"),
            format!("MATCH {pattern} WITH x WITH x RETURN count(*) AS n"),
        ] {
            assert_eq!(count(&mut db, &query), Value::Int(expected), "{query}");
        }
    }
}

#[test]
fn variable_length_trails_exclude_relationships_bound_earlier_in_the_pattern() {
    let mut db = trail_fixture();
    // r1 is bound by the fixed step, so the trail from 2 may use r2 and then
    // only r3; r1 cannot be reused.
    for query in [
        "MATCH (a:N {id: 1})-[:R]->(b:N {id: 2})-[:R*1..3]->(c:N) RETURN count(*) AS n",
        "MATCH (a:N {id: 1})-[:R]->(b:N {id: 2})-[:R*1..3]->(c:N) WITH c WITH c RETURN count(*) AS n",
    ] {
        assert_eq!(count(&mut db, query), Value::Int(2), "{query}");
    }
}
