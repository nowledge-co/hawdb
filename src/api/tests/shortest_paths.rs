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

//! `shortestPath` / `allShortestPaths` through the embedded query API.

use super::*;

const IC13: &str = "MATCH (p1:Person {id: $a}), (p2:Person {id: $b}) OPTIONAL MATCH path = shortestPath((p1)-[:KNOWS*]-(p2)) RETURN CASE path IS NULL WHEN true THEN -1 ELSE length(path) END AS len";

fn params(values: &[(&str, Value)]) -> BTreeMap<String, Value> {
    values
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect()
}

fn int(value: i64) -> Value {
    Value::Int(value)
}

fn ints(values: &[i64]) -> Value {
    Value::List(values.iter().copied().map(Value::Int).collect())
}

/// Persons 1..=7. KNOWS: 1->2->3->4 and 1->5->4 (a shorter route to 4),
/// 6 is isolated, 7 only has an incoming KNOWS from 4.
fn social(db: &mut Database) {
    for id in 1..=7 {
        db.query_with_params(
            "CREATE (:Person {id: $id, name: $name})",
            &params(&[("id", int(id)), ("name", Value::String(format!("p{id}")))]),
        )
        .unwrap();
    }
    for (source, target) in [(1, 2), (2, 3), (3, 4), (1, 5), (5, 4), (4, 7)] {
        db.query_with_params(
            "MATCH (a:Person {id: $a}), (b:Person {id: $b}) CREATE (a)-[:KNOWS]->(b)",
            &params(&[("a", int(source)), ("b", int(target))]),
        )
        .unwrap();
    }
}

/// 1 -> {2, 3} -> 4 plus a longer detour 1 -> 5 -> 6 -> 4.
fn diamond(db: &mut Database) {
    for id in 1..=6 {
        db.query_with_params("CREATE (:Item {id: $id})", &params(&[("id", int(id))]))
            .unwrap();
    }
    for (source, target) in [(1, 2), (1, 3), (2, 4), (3, 4), (1, 5), (5, 6), (6, 4)] {
        db.query_with_params(
            "MATCH (a:Item {id: $a}), (b:Item {id: $b}) CREATE (a)-[:LINK]->(b)",
            &params(&[("a", int(source)), ("b", int(target))]),
        )
        .unwrap();
    }
}

fn column(output: &QueryOutput, name: &str) -> Vec<Value> {
    output
        .rows
        .iter()
        .map(|row| row.get(name).cloned().unwrap_or(Value::Null))
        .collect()
}

#[test]
fn ic13_style_optional_shortest_path_length() {
    let mut db = Database::new();
    social(&mut db);
    for (a, b, expected) in [(1, 4, 2), (4, 1, 2), (1, 7, 3), (1, 6, -1), (3, 3, -1)] {
        let output = db
            .query_with_params(IC13, &params(&[("a", int(a)), ("b", int(b))]))
            .unwrap();
        assert_eq!(column(&output, "len"), vec![int(expected)], "{a} -> {b}");
    }
    // A missing endpoint removes the row in the first, required MATCH.
    let output = db
        .query_with_params(IC13, &params(&[("a", int(1)), ("b", int(99))]))
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn directions_follow_the_relationship_pattern() {
    let mut db = Database::new();
    social(&mut db);
    let mut query = |pattern: &str, a: i64, b: i64| {
        let text = format!(
            "MATCH (a:Person {{id: $a}}), (b:Person {{id: $b}}) MATCH p = shortestPath({pattern}) RETURN properties(nodes(p), 'id') AS ids, length(p) AS hops"
        );
        db.query_with_params(&text, &params(&[("a", int(a)), ("b", int(b))]))
            .unwrap()
    };
    let outgoing = query("(a)-[:KNOWS*]->(b)", 1, 4);
    assert_eq!(column(&outgoing, "ids"), vec![ints(&[1, 5, 4])]);
    assert_eq!(column(&outgoing, "hops"), vec![int(2)]);
    assert!(query("(a)-[:KNOWS*]->(b)", 4, 1).rows.is_empty());
    let incoming = query("(a)<-[:KNOWS*]-(b)", 4, 1);
    assert_eq!(column(&incoming, "ids"), vec![ints(&[4, 5, 1])]);
    let undirected = query("(a)-[:KNOWS*]-(b)", 7, 2);
    assert_eq!(column(&undirected, "ids"), vec![ints(&[7, 4, 3, 2])]);
}

#[test]
fn bounded_ranges_limit_the_path_length() {
    let mut db = Database::new();
    social(&mut db);
    let query = "MATCH (a:Person {id: $a}), (b:Person {id: $b}) OPTIONAL MATCH p = shortestPath((a)-[:KNOWS*1..2]->(b)) RETURN length(p) AS hops";
    let mut hops = |a, b| {
        column(
            &db.query_with_params(query, &params(&[("a", int(a)), ("b", int(b))]))
                .unwrap(),
            "hops",
        )
    };
    assert_eq!(hops(1, 4), vec![int(2)]);
    assert_eq!(hops(1, 7), vec![Value::Null]);
    let error = db
        .query("MATCH (a:Person {id: 1}), (b:Person {id: 2}) MATCH p = shortestPath((a)-[:KNOWS*1..300]->(b)) RETURN length(p)")
        .unwrap_err()
        .to_string();
    assert!(error.contains("exceeds the limit of 256"), "{error}");
    let error = db
        .query("MATCH (a:Person {id: 1}), (b:Person {id: 2}) MATCH p = shortestPath((a)-[:KNOWS*2..3]->(b)) RETURN length(p)")
        .unwrap_err()
        .to_string();
    assert!(error.contains("minimum of 0 or 1 hops"), "{error}");
}

#[test]
fn same_endpoint_paths_follow_the_minimum_hop_count() {
    let mut db = Database::new();
    social(&mut db);
    let zero = db
        .query("MATCH (a:Person {id: 1}) MATCH p = shortestPath((a)-[:KNOWS*0..]-(a)) RETURN length(p) AS hops, properties(nodes(p), 'id') AS ids, relationships(p) AS rels")
        .unwrap();
    assert_eq!(column(&zero, "hops"), vec![int(0)]);
    assert_eq!(column(&zero, "ids"), vec![ints(&[1])]);
    assert_eq!(column(&zero, "rels"), vec![Value::List(Vec::new())]);
    let positive = db
        .query("MATCH (a:Person {id: 1}) OPTIONAL MATCH p = shortestPath((a)-[:KNOWS*]-(a)) RETURN p IS NULL AS missing")
        .unwrap();
    assert_eq!(column(&positive, "missing"), vec![Value::Bool(true)]);
}

#[test]
fn all_shortest_paths_return_exactly_the_shortest_diamond_paths() {
    let mut db = Database::new();
    diamond(&mut db);
    let output = db
        .query("MATCH (a:Item {id: 1}), (b:Item {id: 4}) MATCH p = allShortestPaths((a)-[:LINK*]->(b)) RETURN properties(nodes(p), 'id') AS ids ORDER BY ids")
        .unwrap();
    assert_eq!(
        column(&output, "ids"),
        vec![ints(&[1, 2, 4]), ints(&[1, 3, 4])]
    );
    let single = db
        .query("MATCH (a:Item {id: 1}), (b:Item {id: 4}) MATCH p = shortestPath((a)-[:LINK*]->(b)) RETURN properties(nodes(p), 'id') AS ids")
        .unwrap();
    assert_eq!(column(&single, "ids"), vec![ints(&[1, 2, 4])]);
    let limited = db
        .query("MATCH (a:Item {id: 1}), (b:Item {id: 4}) MATCH p = allShortestPaths((a)-[:LINK*]->(b)) RETURN length(p) AS hops LIMIT 1")
        .unwrap();
    assert_eq!(column(&limited, "hops"), vec![int(2)]);
    let relationships = db
        .query("MATCH (a:Item {id: 1}), (b:Item {id: 4}) MATCH p = allShortestPaths((a)-[:LINK*]->(b)) RETURN relationships(p) AS rels")
        .unwrap();
    for rels in column(&relationships, "rels") {
        let Value::List(rels) = rels else {
            panic!("relationships(p) returns a list");
        };
        assert_eq!(rels.len(), 2);
        assert!(rels.iter().all(|rel| matches!(rel, Value::Map(map)
            if map.get("type") == Some(&Value::String("LINK".into())))));
    }
}

#[test]
fn endpoints_bound_by_inline_maps_where_predicates_and_with() {
    let mut db = Database::new();
    social(&mut db);
    let inline = db
        .query("MATCH p = shortestPath((a:Person {id: 1})-[:KNOWS*]-(b:Person {id: 7})) RETURN length(p) AS hops")
        .unwrap();
    assert_eq!(column(&inline, "hops"), vec![int(3)]);
    let filtered = db
        .query_with_params(
            "MATCH p = shortestPath((a:Person)-[:KNOWS*]-(b:Person)) WHERE a.id = $a AND b.name = $b RETURN length(p) AS hops",
            &params(&[("a", int(1)), ("b", Value::String("p7".into()))]),
        )
        .unwrap();
    assert_eq!(column(&filtered, "hops"), vec![int(3)]);
    let with = db
        .query("MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id IN [4, 7] WITH a, b ORDER BY b.id MATCH p = shortestPath((a)-[:KNOWS*]->(b)) WITH b.id AS target, length(p) AS hops RETURN target, hops ORDER BY hops DESC LIMIT 1")
        .unwrap();
    assert_eq!(column(&with, "target"), vec![int(7)]);
    assert_eq!(column(&with, "hops"), vec![int(3)]);
    let path_with = db
        .query("MATCH (a:Person {id: 1}), (b:Person {id: 4}) OPTIONAL MATCH p = shortestPath((a)-[:KNOWS*]->(b)) WITH a, p RETURN a.id AS id, length(p) AS hops, p IS NOT NULL AS found")
        .unwrap();
    assert_eq!(column(&path_with, "hops"), vec![int(2)]);
    assert_eq!(column(&path_with, "found"), vec![Value::Bool(true)]);
}

#[test]
fn unbound_endpoints_and_unsupported_shapes_are_rejected() {
    let mut db = Database::new();
    social(&mut db);
    for (query, message) in [
        (
            "MATCH p = shortestPath((a:Person)-[:KNOWS*]-(b:Person {id: 1})) RETURN length(p)",
            "endpoint 'a' must be bound",
        ),
        (
            "MATCH (a:Person {id: 1}) MATCH p = shortestPath((a)-[:KNOWS*]-()) RETURN length(p)",
            "endpoint '()' must be bound",
        ),
        (
            "MATCH (a:Person {id: 1}), (b:Person {id: 2}) MATCH p = shortestPath((a)-[r:KNOWS*]-(b)) RETURN length(p)",
            "does not bind relationship variables",
        ),
        (
            "MATCH (a:Person {id: 1}), (b:Person {id: 2}) MATCH p = shortestPath((a)-[:KNOWS*]-(c)-[:KNOWS]-(b)) RETURN length(p)",
            "exactly one relationship pattern",
        ),
    ] {
        let error = db.query(query).unwrap_err().to_string();
        assert!(error.contains(message), "{query}: {error}");
    }
}

#[test]
fn ordering_and_limits_apply_after_the_search() {
    let mut db = Database::new();
    social(&mut db);
    let output = db
        .query("MATCH (a:Person {id: 1}), (b:Person) WHERE b.id <> 1 OPTIONAL MATCH p = shortestPath((a)-[:KNOWS*]->(b)) RETURN b.id AS id, CASE p IS NULL WHEN true THEN -1 ELSE length(p) END AS hops ORDER BY hops DESC, id ASC LIMIT 3")
        .unwrap();
    assert_eq!(column(&output, "id"), vec![int(7), int(3), int(4)]);
    assert_eq!(column(&output, "hops"), vec![int(3), int(2), int(2)]);
}

#[test]
fn parameters_are_bound_through_the_plan_cache() {
    // GraphMatch pipelines cache exact parameter variants: a new value is a
    // separate entry, a repeated value reuses it, and neither reuses stale rows.
    let mut db = Database::new();
    social(&mut db);
    let first = params(&[("a", int(1)), ("b", int(7))]);
    let second = params(&[("a", int(2)), ("b", int(6))]);
    for (parameters, expected) in [(&first, 3), (&second, -1)] {
        assert_eq!(
            db.explain_query_with_params(IC13, parameters)
                .unwrap()
                .plan_cache_lookup,
            PlanCacheLookup::Miss
        );
        assert_eq!(
            db.explain_query_with_params(IC13, parameters)
                .unwrap()
                .plan_cache_lookup,
            PlanCacheLookup::Hit
        );
        let output = db.query_with_params(IC13, parameters).unwrap();
        assert_eq!(column(&output, "len"), vec![int(expected)]);
    }
    let output = db.query_with_params(IC13, &first).unwrap();
    assert_eq!(column(&output, "len"), vec![int(3)]);
}

#[test]
fn unbounded_search_beyond_the_hop_cap_fails_closed() {
    let mut db = Database::new();
    for id in 0..=257 {
        db.query_with_params("CREATE (:Step {id: $id})", &params(&[("id", int(id))]))
            .unwrap();
    }
    for id in 0..257 {
        db.query_with_params(
            "MATCH (a:Step {id: $a}), (b:Step {id: $b}) CREATE (a)-[:NEXT]->(b)",
            &params(&[("a", int(id)), ("b", int(id + 1))]),
        )
        .unwrap();
    }
    let query = "MATCH (a:Step {id: 0}), (b:Step {id: $b}) OPTIONAL MATCH p = shortestPath((a)-[:NEXT*]->(b)) RETURN length(p) AS hops";
    let reachable = db
        .query_with_params(query, &params(&[("b", int(256))]))
        .unwrap();
    assert_eq!(column(&reachable, "hops"), vec![int(256)]);
    let error = db
        .query_with_params(query, &params(&[("b", int(257))]))
        .unwrap_err()
        .to_string();
    assert!(error.contains("256-hop limit"), "{error}");
}

#[test]
fn search_state_beyond_the_blocking_budget_fails_closed() {
    let execution_memory = crate::executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(4096).unwrap(),
        ..crate::executor::ExecutionMemoryConfig::default()
    };
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Hub {id: 0})").unwrap();
    db.query("CREATE (:Hub {id: -1})").unwrap();
    for id in 1..=400 {
        db.query_with_params("CREATE (:Leaf {id: $id})", &params(&[("id", int(id))]))
            .unwrap();
        db.query_with_params(
            "MATCH (h:Hub {id: 0}), (l:Leaf {id: $id}) CREATE (h)-[:LINK]->(l)",
            &params(&[("id", int(id))]),
        )
        .unwrap();
    }
    let error = db
        .query("MATCH (a:Hub {id: 0}), (b:Hub {id: -1}) MATCH p = shortestPath((a)-[:LINK*]-(b)) RETURN length(p)")
        .unwrap_err()
        .to_string();
    assert!(error.contains("blocking_operator_bytes"), "{error}");
}

#[test]
fn existing_all_shortest_shape_keeps_its_results() {
    let mut db = Database::new();
    diamond(&mut db);
    let output = db
        .query("MATCH p = (a)-[:LINK* ALL SHORTEST 1..3]->(b) WHERE a.id = 1 AND b.id = 4 RETURN properties(nodes(p), 'id') AS ids, length(p) AS hops")
        .unwrap();
    assert_eq!(
        column(&output, "ids"),
        vec![ints(&[1, 2, 4]), ints(&[1, 3, 4])]
    );
    assert_eq!(column(&output, "hops"), vec![int(2), int(2)]);
}

#[test]
fn path_functions_filter_and_order_rows() {
    let mut db = Database::new();
    social(&mut db);
    let output = db
        .query("MATCH (a:Person {id: 1}), (b:Person) WHERE b.id IN [2, 4, 6, 7] OPTIONAL MATCH p = shortestPath((a)-[:KNOWS*]->(b)) WITH b, p WHERE p IS NOT NULL AND length(p) >= 2 RETURN b.id AS id, nodes(p) AS nodes ORDER BY length(p) DESC")
        .unwrap();
    assert_eq!(column(&output, "id"), vec![int(7), int(4)]);
    let Value::List(nodes) = &column(&output, "nodes")[0] else {
        panic!("nodes(p) returns a list");
    };
    assert_eq!(nodes.len(), 4);
    assert!(matches!(&nodes[0], Value::Map(node) if node.get("id") == Some(&int(1))));
}

#[test]
fn list_expressions_read_shortest_path_nodes() {
    let mut db = Database::new();
    social(&mut db);
    // 1 -> 2 -> 3 -> 4 -> 7 and 1 -> 5 -> 4 -> 7: the shortest path is 1, 5, 4, 7.
    let output = db
        .query(
            "MATCH (a:Person {id: 1}), (b:Person {id: 7}) \
             MATCH p = shortestPath((a)-[:KNOWS*]->(b)) \
             RETURN [n IN nodes(p) | n.id] AS ids, size(nodes(p)) AS hops",
        )
        .unwrap();
    assert_eq!(column(&output, "ids"), vec![ints(&[1, 5, 4, 7])]);
    assert_eq!(column(&output, "hops"), vec![int(4)]);
}

#[test]
fn path_predicates_in_the_search_clause_are_rejected() {
    let mut db = Database::new();
    social(&mut db);
    let error = db
        .query(
            "MATCH (a:Person {id: 1}), (b:Person {id: 4}) \
             MATCH p = shortestPath((a)-[:KNOWS*]-(b)) WHERE length(p) > 2 RETURN p",
        )
        .unwrap_err();
    assert!(error.to_string().contains("WITH p WHERE"), "{error}");
    // The explicit post-filter form keeps working.
    let output = db
        .query(
            "MATCH (a:Person {id: 1}), (b:Person {id: 4}) \
             MATCH p = shortestPath((a)-[:KNOWS*]-(b)) WITH p WHERE length(p) > 1 RETURN length(p) AS hops",
        )
        .unwrap();
    assert_eq!(column(&output, "hops"), vec![int(2)]);
}

#[test]
fn untyped_shortest_path_patterns_search_every_relationship_type() {
    let mut db = Database::new();
    social(&mut db);
    db.query("MATCH (a:Person {id: 1}), (b:Person {id: 6}) CREATE (a)-[:FOLLOWS]->(b)")
        .unwrap();
    let result = db.query(
        "MATCH (a:Person {id: 1}), (b:Person {id: 6}) MATCH p = shortestPath((a)-[*]-(b)) RETURN length(p) AS hops",
    );
    match result {
        Ok(output) => assert_eq!(column(&output, "hops"), vec![int(1)]),
        Err(error) => assert!(error.to_string().contains("type"), "{error}"),
    }
}

/// The issue #1426 shape: endpoints and the search share one MATCH clause.
const SAME_CLAUSE: &str = "MATCH (p1:Person {id: $a}), (p2:Person {id: $b}), path = shortestPath((p1)-[:KNOWS*]-(p2)) RETURN length(path) AS len, properties(nodes(path), 'id') AS ids";
/// The equivalent form that binds the endpoints in their own clause first.
const SPLIT_CLAUSE: &str = "MATCH (p1:Person {id: $a}), (p2:Person {id: $b}) WITH p1, p2 MATCH path = shortestPath((p1)-[:KNOWS*]-(p2)) RETURN length(path) AS len, properties(nodes(path), 'id') AS ids";

/// Unconnected nodes that make an equality index seek cheaper than a label
/// scan, so indexed runs exercise the seek rather than the cost fallback.
fn index_worthy(db: &mut Database, label: &str) {
    db.query(&format!("CREATE INDEX ON :{label}(id)")).unwrap();
    for id in 100..140 {
        db.query_with_params(
            &format!("CREATE (:{label} {{id: $id}})"),
            &params(&[("id", int(id))]),
        )
        .unwrap();
    }
}

fn endpoint_plan(db: &Database, query: &str) -> String {
    db.explain_query_with_params(query, &params(&[("a", int(1)), ("b", int(4))]))
        .unwrap()
        .physical_plan
        .explain(0)
}

#[test]
fn same_clause_endpoints_seek_the_property_index() {
    let mut db = Database::new();
    social(&mut db);
    // Without an index each endpoint is one filtered label scan, combined as a
    // product, rather than a generic match that rescans per bound row.
    let plan = endpoint_plan(&db, SAME_CLAUSE);
    assert!(!plan.contains("IndexNodeSeek"), "{plan}");
    assert_eq!(plan.matches("GraphMatchExec").count(), 1, "{plan}");
    index_worthy(&mut db, "Person");
    for query in [
        SAME_CLAUSE,
        SPLIT_CLAUSE,
        "MATCH (p1:Person {id: $a}), (p2:Person {id: $b}), path = allShortestPaths((p1)-[:KNOWS*]-(p2)) RETURN length(path) AS len",
    ] {
        let plan = endpoint_plan(&db, query);
        assert_eq!(plan.matches("IndexNodeSeek").count(), 2, "{query}\n{plan}");
        assert_eq!(plan.matches("GraphMatchExec").count(), 1, "{query}\n{plan}");
    }
}

#[test]
fn same_clause_endpoints_return_the_split_clause_rows() {
    for indexed in [false, true] {
        let mut db = Database::new();
        social(&mut db);
        if indexed {
            index_worthy(&mut db, "Person");
            assert!(endpoint_plan(&db, SAME_CLAUSE).contains("IndexNodeSeek"));
        }
        // Found, reversed, longer, missing on either side, identical endpoints
        // (no positive-length path to itself), and unreachable endpoints.
        for (a, b, expected) in [
            (1, 4, Some(2)),
            (4, 1, Some(2)),
            (1, 7, Some(3)),
            (1, 99, None),
            (99, 1, None),
            (3, 3, None),
            (1, 6, None),
        ] {
            let parameters = params(&[("a", int(a)), ("b", int(b))]);
            let same = db.query_with_params(SAME_CLAUSE, &parameters).unwrap();
            let split = db.query_with_params(SPLIT_CLAUSE, &parameters).unwrap();
            assert_eq!(same.rows, split.rows, "indexed={indexed} {a} -> {b}");
            assert_eq!(
                column(&same, "len"),
                expected.map(int).into_iter().collect::<Vec<_>>(),
                "indexed={indexed} {a} -> {b}"
            );
        }
        // The same variable on both sides of a zero-length search.
        let same = db
            .query("MATCH (a:Person {id: 3}), p = shortestPath((a)-[:KNOWS*0..]-(a)) RETURN length(p) AS hops, properties(nodes(p), 'id') AS ids")
            .unwrap();
        let split = db
            .query("MATCH (a:Person {id: 3}) WITH a MATCH p = shortestPath((a)-[:KNOWS*0..]-(a)) RETURN length(p) AS hops, properties(nodes(p), 'id') AS ids")
            .unwrap();
        assert_eq!(same.rows, split.rows, "indexed={indexed}");
        assert_eq!(column(&same, "hops"), vec![int(0)]);
        // WHERE filters and constraints written on the search endpoints still
        // apply after the endpoint steps fold together.
        for (same, split, expected) in [
            (
                "MATCH (a:Person {id: 1}), (b:Person), p = shortestPath((a)-[:KNOWS*]->(b)) WHERE b.name = 'p7' RETURN length(p) AS hops",
                "MATCH (a:Person {id: 1}), (b:Person) WHERE b.name = 'p7' WITH a, b MATCH p = shortestPath((a)-[:KNOWS*]->(b)) RETURN length(p) AS hops",
                vec![int(3)],
            ),
            (
                "MATCH (a {id: 1}), (b), p = shortestPath((a:Person)-[:KNOWS*]->(b:Person {id: 7})) RETURN length(p) AS hops",
                "MATCH (a:Person {id: 1}), (b:Person {id: 7}) WITH a, b MATCH p = shortestPath((a)-[:KNOWS*]->(b)) RETURN length(p) AS hops",
                vec![int(3)],
            ),
            (
                "MATCH (a:Person {id: 1}), (b:Person {id: 7}), p = shortestPath((a)-[:KNOWS*]->(b {id: 4})) RETURN length(p) AS hops",
                "MATCH (a:Person {id: 1}), (b:Person {id: 7}) WHERE b.id = 4 WITH a, b MATCH p = shortestPath((a)-[:KNOWS*]->(b)) RETURN length(p) AS hops",
                Vec::new(),
            ),
        ] {
            let same = db.query(same).unwrap();
            assert_eq!(same.rows, db.query(split).unwrap().rows, "indexed={indexed}");
            assert_eq!(column(&same, "hops"), expected, "indexed={indexed}");
        }
    }
}

#[test]
fn same_clause_all_shortest_paths_return_the_split_clause_rows() {
    for indexed in [false, true] {
        let mut db = Database::new();
        diamond(&mut db);
        if indexed {
            index_worthy(&mut db, "Item");
        }
        for (a, b) in [(1, 4), (4, 1), (1, 99), (2, 3), (1, 1)] {
            let parameters = params(&[("a", int(a)), ("b", int(b))]);
            let same = db
                .query_with_params(
                    "MATCH (a:Item {id: $a}), (b:Item {id: $b}), p = allShortestPaths((a)-[:LINK*]->(b)) RETURN properties(nodes(p), 'id') AS ids ORDER BY ids",
                    &parameters,
                )
                .unwrap();
            let split = db
                .query_with_params(
                    "MATCH (a:Item {id: $a}), (b:Item {id: $b}) WITH a, b MATCH p = allShortestPaths((a)-[:LINK*]->(b)) RETURN properties(nodes(p), 'id') AS ids ORDER BY ids",
                    &parameters,
                )
                .unwrap();
            assert_eq!(same.rows, split.rows, "indexed={indexed} {a} -> {b}");
            if (a, b) == (1, 4) {
                assert_eq!(
                    column(&same, "ids"),
                    vec![ints(&[1, 2, 4]), ints(&[1, 3, 4])]
                );
            }
        }
    }
}
