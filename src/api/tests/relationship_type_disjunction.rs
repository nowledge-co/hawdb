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

//! Relationship type disjunctions `[:A|B]` in MATCH reads (#1489). A step
//! matches a relationship of any listed type; a variable-length trail may mix
//! them. Names absent from the catalog match nothing, and writes reject a
//! disjunction instead of using only its first type.

use super::*;

fn ids_with(db: &mut Database, query: &str, parameters: &BTreeMap<String, Value>) -> Vec<i64> {
    let mut ids = db
        .query_with_params(query, parameters)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
        .rows
        .iter()
        .map(|row| match row["id"] {
            Value::Int(id) => id,
            ref other => panic!("{query}: {other:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn ids(db: &mut Database, query: &str) -> Vec<i64> {
    ids_with(db, query, &BTreeMap::new())
}

/// Nodes `(:N {id: 0..=5})` and
/// `0-A->1, 0-B->2, 0-C->3, 1-B->4, 4-A->5, 2-C->5, 3-A->0, 5-B->0`.
fn graph() -> Database {
    let mut db = Database::new();
    for id in 0..=5 {
        db.query(&format!("CREATE (:N {{id: {id}}})")).unwrap();
    }
    for (source, rel_type, target) in [
        (0, "A", 1),
        (0, "B", 2),
        (0, "C", 3),
        (1, "B", 4),
        (4, "A", 5),
        (2, "C", 5),
        (3, "A", 0),
        (5, "B", 0),
    ] {
        db.query(&format!(
            "MATCH (a:N {{id: {source}}}), (b:N {{id: {target}}}) CREATE (a)-[:{rel_type}]->(b)"
        ))
        .unwrap();
    }
    db
}

fn union(mut left: Vec<i64>, right: Vec<i64>) -> Vec<i64> {
    left.extend(right);
    left.sort_unstable();
    left
}

#[test]
fn one_hop_disjunctions_match_the_union_of_their_types() {
    let mut db = graph();
    for (pattern, expected) in [
        ("(:N {id: 0})-[{}]->(b)", vec![1, 2]),
        ("(:N {id: 0})<-[{}]-(b)", vec![3, 5]),
        ("(:N {id: 0})-[{}]-(b)", vec![1, 2, 3, 5]),
        ("(a:N)-[{}]->(b)", vec![0, 0, 1, 2, 4, 5]),
    ] {
        let query =
            |types: &str| format!("MATCH {} RETURN b.id AS id", pattern.replace("{}", types));
        let a = ids(&mut db, &query(":A"));
        let b = ids(&mut db, &query(":B"));
        assert_eq!(ids(&mut db, &query(":A|B")), expected, "{pattern}");
        assert_eq!(ids(&mut db, &query(":A|:B")), expected, "{pattern}");
        assert_eq!(ids(&mut db, &query(":B|A")), expected, "{pattern}");
        assert_eq!(union(a, b), expected, "{pattern}");
    }
    // A bound relationship variable reports each match's own type.
    let output = db
        .query("MATCH (:N {id: 0})-[r:A|B|C]->(b) RETURN type(r) AS t, b.id AS id ORDER BY id")
        .unwrap();
    let rows = output
        .rows
        .iter()
        .map(|row| (row["t"].clone(), row["id"].clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            (Value::String("A".into()), Value::Int(1)),
            (Value::String("B".into()), Value::Int(2)),
            (Value::String("C".into()), Value::Int(3)),
        ]
    );
    // A later MATCH from a bound node and an OPTIONAL MATCH keep the union.
    assert_eq!(
        ids(
            &mut db,
            "MATCH (a:N {id: 0}) MATCH (a)-[:A|B]->(b) RETURN b.id AS id"
        ),
        vec![1, 2]
    );
    let output = db
        .query(
            "MATCH (a:N) WHERE a.id IN [0, 3] OPTIONAL MATCH (a)-[:B|C]->(b) \
             RETURN a.id AS a, b.id AS b ORDER BY a, b",
        )
        .unwrap();
    let rows = output
        .rows
        .iter()
        .map(|row| (row["a"].clone(), row["b"].clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            (Value::Int(0), Value::Int(2)),
            (Value::Int(0), Value::Int(3)),
            (Value::Int(3), Value::Null),
        ]
    );
    let output = db
        .query("MATCH (a:N {id: 0}) OPTIONAL MATCH (a)-[r:A|B]->() RETURN count(r) AS n")
        .unwrap();
    assert_eq!(output.rows[0]["n"], Value::Int(2));
}

#[test]
fn variable_length_disjunctions_mix_types_along_a_trail() {
    let mut db = graph();
    // 0-A->1-B->4-A->5 alternates types; neither type alone reaches 4 or 5.
    assert_eq!(
        ids(
            &mut db,
            "MATCH (:N {id: 0})-[:A*1..3]->(b) RETURN b.id AS id"
        ),
        vec![1]
    );
    assert_eq!(
        ids(
            &mut db,
            "MATCH (:N {id: 0})-[:B*1..3]->(b) RETURN b.id AS id"
        ),
        vec![2]
    );
    assert_eq!(
        ids(
            &mut db,
            "MATCH (:N {id: 0})-[:A|B*1..3]->(b) RETURN b.id AS id"
        ),
        vec![1, 2, 4, 5]
    );
    // Unbounded trails return to 0 over 5-B->0 and continue over the unused
    // 0-B->2; no trail repeats a relationship, so the cycle terminates.
    assert_eq!(
        ids(
            &mut db,
            "MATCH (:N {id: 0})-[:A|B*0..]->(b) RETURN b.id AS id"
        ),
        vec![0, 0, 1, 2, 2, 4, 5]
    );
    assert_eq!(
        ids(&mut db, "MATCH (:N {id: 0})-[:A|B*]->(b) RETURN b.id AS id"),
        vec![0, 1, 2, 2, 4, 5]
    );
    assert_eq!(
        ids(
            &mut db,
            "MATCH (:N {id: 5})<-[:A|B*1..2]-(b) RETURN b.id AS id"
        ),
        vec![1, 4]
    );
    assert_eq!(
        ids(
            &mut db,
            "MATCH (:N {id: 3})-[:A|C*1..2]-(b) RETURN b.id AS id"
        ),
        // 3 and 0 share one A and one C relationship: each first hop reaches
        // 0, then 1 or back to 3 over the other relationship.
        vec![0, 0, 1, 1, 3, 3]
    );
    // LDBC IC12 shape: a labeled target over a mixed-type unbounded trail.
    assert_eq!(
        ids(
            &mut db,
            "MATCH (s:N {id: 1})-[:B|A*0..]->(t:N) WHERE t.id >= 4 RETURN t.id AS id"
        ),
        vec![4, 5]
    );
}

#[test]
fn missing_type_names_match_no_relationships() {
    let mut db = graph();
    for (with_missing, without) in [
        (
            "MATCH (:N {id: 0})-[:A|Missing]->(b) RETURN b.id AS id",
            "MATCH (:N {id: 0})-[:A]->(b) RETURN b.id AS id",
        ),
        (
            "MATCH (:N {id: 0})-[:Missing|A]-(b) RETURN b.id AS id",
            "MATCH (:N {id: 0})-[:A]-(b) RETURN b.id AS id",
        ),
        (
            "MATCH (:N {id: 0})-[:A|Missing*1..3]->(b) RETURN b.id AS id",
            "MATCH (:N {id: 0})-[:A*1..3]->(b) RETURN b.id AS id",
        ),
        (
            "MATCH (:N {id: 0})-[:Missing1|Missing2*0..2]->(b) RETURN b.id AS id",
            "MATCH (:N {id: 0})-[:Missing*0..2]->(b) RETURN b.id AS id",
        ),
    ] {
        assert_eq!(
            ids(&mut db, with_missing),
            ids(&mut db, without),
            "{with_missing}"
        );
    }
    assert_eq!(
        ids(
            &mut db,
            "MATCH (:N {id: 0})-[:Missing1|Missing2*0..2]->(b) RETURN b.id AS id"
        ),
        vec![0]
    );
    assert!(ids(
        &mut db,
        "MATCH (:N {id: 0})-[:Missing1|Missing2]->(b) RETURN b.id AS id"
    )
    .is_empty());
    assert!(ids(
        &mut db,
        "MATCH (:N {id: 0})-[:Missing1|Missing2*1..]->(b) RETURN b.id AS id"
    )
    .is_empty());
}

#[test]
fn explain_and_plan_cache_distinguish_disjunctions() {
    let mut db = graph();
    let explain = db
        .explain_query("MATCH (:N {id: 0})-[:A|B]->(b) RETURN b.id AS id")
        .unwrap();
    assert!(
        explain.trace.selected_plan.contains("A|B"),
        "{}",
        explain.trace.selected_plan
    );
    let single = db
        .explain_query("MATCH (:N {id: 0})-[:A]->(b) RETURN b.id AS id")
        .unwrap();
    assert!(
        !single.trace.selected_plan.contains("A|B"),
        "{}",
        single.trace.selected_plan
    );
    let fingerprint = |db: &mut Database, query: &str| {
        db.explain_query(query)
            .unwrap()
            .physical_plan
            .instance_fingerprint()
    };
    // The instance fingerprint keeps every type of the set.
    let ab = fingerprint(&mut db, "MATCH (:N {id: 0})-[:A|B]->(b) RETURN b.id AS id");
    let ac = fingerprint(&mut db, "MATCH (:N {id: 0})-[:A|C]->(b) RETURN b.id AS id");
    let a = fingerprint(&mut db, "MATCH (:N {id: 0})-[:A]->(b) RETURN b.id AS id");
    assert_ne!(ab, ac);
    assert_ne!(ab, a);
    // Alternating shapes reuse cached plans without mixing their type sets.
    for _ in 0..2 {
        for (query, expected) in [
            ("MATCH (:N {id: 0})-[:A]->(b) RETURN b.id AS id", vec![1]),
            (
                "MATCH (:N {id: 0})-[:A|B]->(b) RETURN b.id AS id",
                vec![1, 2],
            ),
            (
                "MATCH (:N {id: 0})-[:A|C]->(b) RETURN b.id AS id",
                vec![1, 3],
            ),
            (
                "MATCH (:N {id: 0})-[:A|B*1..3]->(b) RETURN b.id AS id",
                vec![1, 2, 4, 5],
            ),
            (
                "MATCH (:N {id: 0})-[:A*1..3]->(b) RETURN b.id AS id",
                vec![1],
            ),
        ] {
            assert_eq!(ids(&mut db, query), expected, "{query}");
        }
        for (source, single, disjunction) in [(0, vec![1], vec![1, 2]), (1, vec![], vec![4])] {
            let parameters = BTreeMap::from([("id".to_string(), Value::Int(source))]);
            assert_eq!(
                ids_with(
                    &mut db,
                    "MATCH (:N {id: $id})-[:A]->(b) RETURN b.id AS id",
                    &parameters
                ),
                single
            );
            assert_eq!(
                ids_with(
                    &mut db,
                    "MATCH (:N {id: $id})-[:A|B]->(b) RETURN b.id AS id",
                    &parameters
                ),
                disjunction
            );
        }
    }
}

#[test]
fn writes_reject_relationship_type_disjunctions() {
    let mut db = graph();
    for query in [
        "MATCH (a:N {id: 1}), (b:N {id: 2}) CREATE (a)-[:A|B]->(b)",
        "MATCH (a:N {id: 1}), (b:N {id: 2}) MERGE (a)-[:A|B]->(b)",
        "MATCH ()-[r:A|B]->() DELETE r",
        "MATCH (:N {id: 0})-[r:A|B]->() SET r.w = 1",
        "MATCH (:N {id: 0})-[:A|B]->(b) SET b.w = 1",
    ] {
        let error = db
            .query(query)
            .err()
            .unwrap_or_else(|| panic!("{query} succeeded"));
        assert!(
            error.to_string().contains("disjunction"),
            "{query}: {error}"
        );
    }
    // Nothing was written.
    assert_eq!(
        ids(&mut db, "MATCH (a:N)-[r]->(b) RETURN b.id AS id").len(),
        8
    );
    assert!(ids(
        &mut db,
        "MATCH (b:N) WHERE b.w IS NOT NULL RETURN b.id AS id"
    )
    .is_empty());
    // Shortest paths test a single type.
    let query = "MATCH (a:N {id: 0}), (b:N {id: 5}) MATCH p = shortestPath((a)-[:A|B*]->(b)) RETURN length(p) AS id";
    let error = db
        .query(query)
        .err()
        .unwrap_or_else(|| panic!("{query} succeeded"));
    assert!(
        error.to_string().contains("disjunction"),
        "{query}: {error}"
    );
    // A pattern predicate matches the whole disjunction (#1488).
    assert_eq!(
        ids(
            &mut db,
            "MATCH (a:N) WHERE (a)-[:A|B]->() RETURN a.id AS id"
        ),
        ids(&mut db, "MATCH (a:N)-[:A|B]->() RETURN DISTINCT a.id AS id")
    );
    assert_eq!(
        ids(
            &mut db,
            "MATCH (a:N) WHERE (a)-[:A|B]->() RETURN a.id AS id"
        ),
        [0, 1, 3, 4, 5]
    );
}
