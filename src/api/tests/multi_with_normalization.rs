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

//! Statements with two or more WITH clauses use the same structural
//! normalization as every other read (#1425). These guards pin the operator
//! choice for the LDBC IC4 shape and the rows of common multi-WITH shapes
//! across plan-cache hits with different parameter values.

use super::*;

// LDBC SNB Interactive Complex 4 as run by the SF0.1 benchmark harness.
const IC4: &str = "MATCH (person:Person {id: $personId})-[:KNOWS]-(friend:Person), \
      (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag) \
    WITH DISTINCT tag, post \
    WITH tag, \
         CASE WHEN $endDate > post.creationDate >= $startDate THEN 1 ELSE 0 END AS valid, \
         CASE WHEN $startDate > post.creationDate THEN 1 ELSE 0 END AS inValid \
    WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount \
    WHERE postCount > 0 AND inValidPostCount = 0 \
    RETURN tag.name AS tagName, postCount \
    ORDER BY postCount DESC, tagName ASC \
    LIMIT 10";

/// Persons 1-4 form a KNOWS square (1-2, 1-3, 2-4, 3-4) with mixed stored
/// directions; persons 5 and 1000-1199 are isolated. Posts 101-105 have a
/// creator and tags.
fn social() -> Database {
    let mut db = Database::new();
    for query in [
        "CREATE (:Person {id: 1, name: 'Alice'})",
        "CREATE (:Person {id: 2, name: 'Bob'})",
        "CREATE (:Person {id: 3, name: 'Carol'})",
        "CREATE (:Person {id: 4, name: 'Dave'})",
        "CREATE (:Person {id: 5, name: 'Eve'})",
        "CREATE (:Tag {id: 11, name: 'Rust'})",
        "CREATE (:Tag {id: 12, name: 'Graphs'})",
        "CREATE (:Tag {id: 13, name: 'Cypher'})",
        "CREATE (:Post {id: 101, creationDate: 100})",
        "CREATE (:Post {id: 102, creationDate: 200})",
        "CREATE (:Post {id: 103, creationDate: 50})",
        "CREATE (:Post {id: 104, creationDate: 150})",
        "CREATE (:Post {id: 105, creationDate: 300})",
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person {id: 3}), (b:Person {id: 1}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person {id: 2}), (b:Person {id: 4}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person {id: 4}), (b:Person {id: 3}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (p:Post {id: 101}), (a:Person {id: 2}) CREATE (p)-[:HAS_CREATOR]->(a)",
        "MATCH (p:Post {id: 105}), (a:Person {id: 2}) CREATE (p)-[:HAS_CREATOR]->(a)",
        "MATCH (p:Post {id: 102}), (a:Person {id: 3}) CREATE (p)-[:HAS_CREATOR]->(a)",
        "MATCH (p:Post {id: 103}), (a:Person {id: 3}) CREATE (p)-[:HAS_CREATOR]->(a)",
        "MATCH (p:Post {id: 104}), (a:Person {id: 4}) CREATE (p)-[:HAS_CREATOR]->(a)",
        "MATCH (p:Post {id: 101}), (t:Tag {id: 11}) CREATE (p)-[:HAS_TAG]->(t)",
        "MATCH (p:Post {id: 101}), (t:Tag {id: 12}) CREATE (p)-[:HAS_TAG]->(t)",
        "MATCH (p:Post {id: 102}), (t:Tag {id: 11}) CREATE (p)-[:HAS_TAG]->(t)",
        "MATCH (p:Post {id: 103}), (t:Tag {id: 13}) CREATE (p)-[:HAS_TAG]->(t)",
        "MATCH (p:Post {id: 104}), (t:Tag {id: 12}) CREATE (p)-[:HAS_TAG]->(t)",
        "MATCH (p:Post {id: 105}), (t:Tag {id: 13}) CREATE (p)-[:HAS_TAG]->(t)",
    ] {
        db.query(query).unwrap();
    }
    // Isolated persons make the index seek cheaper than a Person label scan.
    for id in 1000..1200 {
        db.query(&format!("CREATE (:Person {{id: {id}, name: 'p{id}'}})"))
            .unwrap();
    }
    db.query("CREATE INDEX ON :Person(id)").unwrap();
    db
}

fn params(values: &[(&str, Value)]) -> BTreeMap<String, Value> {
    values
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect()
}

fn row(values: &[(&str, Value)]) -> BTreeMap<String, Value> {
    params(values)
}

fn ic4_params(person: i64) -> BTreeMap<String, Value> {
    params(&[
        ("personId", Value::Int(person)),
        ("startDate", Value::Int(100)),
        ("endDate", Value::Int(250)),
    ])
}

fn tag_count(name: &str, count: i64) -> BTreeMap<String, Value> {
    row(&[
        ("tagName", Value::String(name.to_string())),
        ("postCount", Value::Int(count)),
    ])
}

#[test]
fn multi_with_ic4_seeks_the_person_index_and_expands_natively() {
    let db = social();
    let explain = db.explain_query_with_params(IC4, &ic4_params(1)).unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("IndexNodeSeek"), "{physical_plan}");
    assert!(
        physical_plan.contains("AdjacencyExpandExec"),
        "{physical_plan}"
    );
    assert!(!physical_plan.contains("GraphMatchExec"), "{physical_plan}");
    assert!(!physical_plan.contains("SeqNodeScan"), "{physical_plan}");
}

#[test]
fn multi_with_ic4_rows_survive_plan_cache_reuse_across_seek_keys() {
    let mut db = social();
    // Friends 2 and 3 created posts 101 (Rust, Graphs, in range), 102 (Rust, in
    // range), 103 (Cypher, before the window) and 105 (Cypher, after it).
    let person_one = vec![tag_count("Rust", 2), tag_count("Graphs", 1)];
    // Friends 1 and 4: only post 104 (Graphs, in range).
    let person_two = vec![tag_count("Graphs", 1)];
    for (person, expected) in [
        (1, &person_one),
        (2, &person_two),
        (5, &Vec::new()),
        (99, &Vec::new()),
        (1, &person_one),
    ] {
        let parameters = ic4_params(person);
        assert_eq!(
            db.query_with_params(IC4, &parameters).unwrap().rows,
            *expected,
            "personId={person}"
        );
        assert_eq!(
            db.explain_query_with_params(IC4, &parameters)
                .unwrap()
                .plan_cache_lookup,
            PlanCacheLookup::Hit,
            "personId={person}"
        );
    }
    // The seek key is a template slot: every person reuses one entry, and the
    // repeated first value above still produced its own rows.
    assert_eq!(db.plan_cache_stats().unwrap().entries, 1);
}

/// Each case is a multi-WITH query and its expected rows for two or more
/// parameter sets. Cases cover WITH DISTINCT, aggregation, ORDER BY with a
/// parameterized LIMIT between WITH clauses, and OPTIONAL MATCH after WITH.
type MultiWithCase = (
    &'static str,
    Vec<(BTreeMap<String, Value>, Vec<BTreeMap<String, Value>>)>,
);

fn multi_with_cases() -> Vec<MultiWithCase> {
    let int = Value::Int;
    vec![
        (
            "MATCH (p:Person {id: $id})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(post:Post) \
             WITH DISTINCT f WITH f.id AS fid RETURN fid ORDER BY fid",
            vec![
                (
                    params(&[("id", int(1))]),
                    vec![row(&[("fid", int(2))]), row(&[("fid", int(3))])],
                ),
                (params(&[("id", int(2))]), vec![row(&[("fid", int(4))])]),
                (params(&[("id", int(5))]), vec![]),
            ],
        ),
        (
            "MATCH (p:Person {id: $id})-[:KNOWS]-(f:Person) \
             MATCH (f)<-[:HAS_CREATOR]-(post:Post) \
             WITH f, count(post) AS posts \
             WITH f.id AS fid, posts WHERE posts >= $min \
             RETURN fid, posts ORDER BY fid",
            vec![
                (
                    params(&[("id", int(1)), ("min", int(1))]),
                    vec![
                        row(&[("fid", int(2)), ("posts", int(2))]),
                        row(&[("fid", int(3)), ("posts", int(2))]),
                    ],
                ),
                (
                    params(&[("id", int(2)), ("min", int(1))]),
                    vec![row(&[("fid", int(4)), ("posts", int(1))])],
                ),
                (params(&[("id", int(2)), ("min", int(2))]), vec![]),
            ],
        ),
        (
            "MATCH (p:Person {id: $id})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(post:Post) \
             WITH post ORDER BY post.creationDate DESC LIMIT $n \
             WITH post.id AS pid, post.creationDate AS date \
             RETURN pid, date ORDER BY date",
            vec![
                (
                    params(&[("id", int(1)), ("n", int(2))]),
                    vec![
                        row(&[("pid", int(102)), ("date", int(200))]),
                        row(&[("pid", int(105)), ("date", int(300))]),
                    ],
                ),
                (
                    params(&[("id", int(1)), ("n", int(1))]),
                    vec![row(&[("pid", int(105)), ("date", int(300))])],
                ),
                (
                    params(&[("id", int(2)), ("n", int(2))]),
                    vec![row(&[("pid", int(104)), ("date", int(150))])],
                ),
            ],
        ),
        (
            "MATCH (p:Person {id: $id})-[:KNOWS]-(f:Person) \
             WITH DISTINCT f WITH f \
             OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(post:Post) WHERE post.creationDate >= $since \
             RETURN f.id AS fid, post.id AS pid ORDER BY fid, pid",
            vec![
                (
                    params(&[("id", int(1)), ("since", int(150))]),
                    vec![
                        row(&[("fid", int(2)), ("pid", int(105))]),
                        row(&[("fid", int(3)), ("pid", int(102))]),
                    ],
                ),
                (
                    params(&[("id", int(2)), ("since", int(150))]),
                    vec![
                        row(&[("fid", int(1)), ("pid", Value::Null)]),
                        row(&[("fid", int(4)), ("pid", int(104))]),
                    ],
                ),
                (
                    params(&[("id", int(4)), ("since", int(400))]),
                    vec![
                        row(&[("fid", int(2)), ("pid", Value::Null)]),
                        row(&[("fid", int(3)), ("pid", Value::Null)]),
                    ],
                ),
            ],
        ),
    ]
}

#[test]
fn multi_with_shapes_return_expected_rows_across_plan_cache_hits() {
    let mut db = social();
    for (query, cases) in multi_with_cases() {
        // Run every parameter set twice; the second pass executes cached plans
        // after other values were planned, so stale seek keys or filters would
        // surface as wrong rows.
        for pass in 0..2 {
            for (parameters, expected) in &cases {
                assert_eq!(
                    db.query_with_params(query, parameters).unwrap().rows,
                    *expected,
                    "pass={pass}, {parameters:?}: {query}"
                );
                assert_eq!(
                    db.explain_query_with_params(query, parameters)
                        .unwrap()
                        .plan_cache_lookup,
                    PlanCacheLookup::Hit,
                    "{parameters:?}: {query}"
                );
                let mut snapshot = db.begin_read_transaction().unwrap();
                assert_eq!(
                    snapshot.query_with_params(query, parameters).unwrap().rows,
                    *expected,
                    "read transaction, {parameters:?}: {query}"
                );
            }
        }
        let explain = db.explain_query_with_params(query, &cases[0].0).unwrap();
        let physical_plan = explain.physical_plan.explain(0);
        assert!(
            physical_plan.contains("IndexNodeSeek"),
            "{query}\n{physical_plan}"
        );
    }
}
