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

//! A MATCH whose pattern contains a node bound before it starts from that
//! node, so the per-row nested loop expands instead of scanning (#1423).
//! Two WITH clauses keep these patterns as generic `GraphMatchExec`.

use super::*;

/// A small forum graph shaped like LDBC IC5.
///
/// Person 1 knows 2 and 3 (and 4 knows 1); forums f1 (members 2, 3),
/// f2 (member 2), f3 (member 3). Posts: f1 holds m1 by 2 and m2 by 5;
/// f2 holds m3 by 3; f3 holds m4 and m5 by 3 and m6 by 2. Comment c1 is
/// by 3. Person 2 likes itself and 3.
fn forum_graph() -> Database {
    let mut db = Database::new();
    for id in 1..=5 {
        db.query(&format!("CREATE (:Person {{id: {id}}})")).unwrap();
    }
    for id in ["f1", "f2", "f3"] {
        db.query(&format!("CREATE (:Forum {{id: '{id}'}})"))
            .unwrap();
    }
    for id in ["m1", "m2", "m3", "m4", "m5", "m6"] {
        db.query(&format!("CREATE (:Post {{id: '{id}'}})")).unwrap();
    }
    db.query("CREATE (:Comment {id: 'c1'})").unwrap();
    for (source, target) in [(1, 2), (1, 3), (2, 3), (4, 1)] {
        db.query(&format!(
            "MATCH (a:Person {{id: {source}}}), (b:Person {{id: {target}}}) CREATE (a)-[:KNOWS]->(b)"
        ))
        .unwrap();
    }
    for (source, target) in [(2, 2), (2, 3)] {
        db.query(&format!(
            "MATCH (a:Person {{id: {source}}}), (b:Person {{id: {target}}}) CREATE (a)-[:LIKES]->(b)"
        ))
        .unwrap();
    }
    for (forum, person, year) in [
        ("f1", 2, 2020),
        ("f1", 3, 2021),
        ("f2", 2, 2020),
        ("f3", 3, 2020),
    ] {
        db.query(&format!(
            "MATCH (f:Forum {{id: '{forum}'}}), (p:Person {{id: {person}}}) \
             CREATE (f)-[:HAS_MEMBER {{year: {year}}}]->(p)"
        ))
        .unwrap();
    }
    for (forum, post) in [
        ("f1", "m1"),
        ("f1", "m2"),
        ("f2", "m3"),
        ("f3", "m4"),
        ("f3", "m5"),
        ("f3", "m6"),
    ] {
        db.query(&format!(
            "MATCH (f:Forum {{id: '{forum}'}}), (m:Post {{id: '{post}'}}) CREATE (f)-[:CONTAINER_OF]->(m)"
        ))
        .unwrap();
    }
    for (label, message, person) in [
        ("Post", "m1", 2),
        ("Post", "m2", 5),
        ("Post", "m3", 3),
        ("Post", "m4", 3),
        ("Post", "m5", 3),
        ("Post", "m6", 2),
        ("Comment", "c1", 3),
    ] {
        db.query(&format!(
            "MATCH (m:{label} {{id: '{message}'}}), (p:Person {{id: {person}}}) CREATE (m)-[:HAS_CREATOR]->(p)"
        ))
        .unwrap();
    }
    db
}

/// Rows as a sorted multiset; MATCH has no inherent row order.
fn rows(db: &mut Database, query: &str) -> Vec<String> {
    let output = db.query(query).unwrap();
    let mut rows = output
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|(column, value)| format!("{column}={value:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn expected(rows: &[&str]) -> Vec<String> {
    let mut rows = rows.iter().map(ToString::to_string).collect::<Vec<_>>();
    rows.sort();
    rows
}

/// Runs `query` and returns its rows and the node scans it started.
fn rows_and_scans(db: &mut Database, query: &str) -> (Vec<String>, u64) {
    let before = hawdb_storage::store::node_scans_for_test();
    let rows = rows(db, query);
    (rows, hawdb_storage::store::node_scans_for_test() - before)
}

/// The last `GraphMatchExec` starts at `anchor` and the optimizer says why.
fn assert_anchored(db: &Database, query: &str, anchor: &str) {
    let explain = db.explain_query(query).unwrap();
    let plan = explain.physical_plan.explain(0);
    let graph_match = plan
        .lines()
        .find(|line| line.contains("GraphMatchExec"))
        .unwrap_or_else(|| panic!("no GraphMatchExec in:\n{plan}"));
    assert!(
        graph_match.contains(&format!(
            "steps=[Node(GraphMatchNode {{ variable: \"{anchor}\""
        )),
        "{query}\n{graph_match}"
    );
    assert!(
        explain.trace.decisions.iter().any(|decision| decision
            .starts_with("anchor GraphMatchExec at bound variable(s)")
            && decision.contains(anchor)),
        "{:?}",
        explain.trace.decisions
    );
}

const IC5_PREFIX: &str = "MATCH (person:Person {id: 1})-[:KNOWS]-(friend:Person) \
     WITH DISTINCT friend \
     MATCH (friend)<-[:HAS_MEMBER]-(forum:Forum) \
     WITH forum, collect(friend) AS friends ";

#[test]
fn optional_match_with_imported_variable_last_starts_from_it() {
    let mut db = forum_graph();
    let text_order = format!(
        "{IC5_PREFIX} OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) \
         WHERE friend IN friends RETURN forum.id AS forum, friend.id AS friend, post.id AS post"
    );
    let bound_end = format!(
        "{IC5_PREFIX} OPTIONAL MATCH (forum)-[:CONTAINER_OF]->(post)-[:HAS_CREATOR]->(friend) \
         WHERE friend IN friends RETURN forum.id AS forum, friend.id AS friend, post.id AS post"
    );
    let (text_rows, text_scans) = rows_and_scans(&mut db, &text_order);
    let (bound_rows, bound_scans) = rows_and_scans(&mut db, &bound_end);
    // f2's only post is by 3, who is not among f2's friends: one padded row.
    assert_eq!(
        text_rows,
        expected(&[
            "forum=String(\"f1\"), friend=Int(2), post=String(\"m1\")",
            "forum=String(\"f2\"), friend=Null, post=Null",
            "forum=String(\"f3\"), friend=Int(3), post=String(\"m4\")",
            "forum=String(\"f3\"), friend=Int(3), post=String(\"m5\")",
        ])
    );
    assert_eq!(text_rows, bound_rows);
    // Without anchoring, the OPTIONAL MATCH scans every node once per forum.
    assert_eq!(text_scans, bound_scans);
    assert_anchored(&db, &text_order, "forum");

    let counts = format!(
        "{IC5_PREFIX} OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) \
         WHERE friend IN friends RETURN forum.id AS forum, count(post) AS posts"
    );
    assert_eq!(
        rows(&mut db, &counts),
        expected(&[
            "forum=String(\"f1\"), posts=Int(1)",
            "forum=String(\"f2\"), posts=Int(0)",
            "forum=String(\"f3\"), posts=Int(2)",
        ])
    );
}

#[test]
fn match_with_bound_middle_node_expands_both_sides() {
    let mut db = forum_graph();
    let query = "MATCH (m:Post) WITH m, m.id AS id WITH m \
                 MATCH (f:Forum)-[:CONTAINER_OF]->(m)-[:HAS_CREATOR]->(p:Person {id: 3}) \
                 RETURN f.id AS f, m.id AS m, p.id AS p";
    assert_eq!(
        rows(&mut db, query),
        expected(&[
            "f=String(\"f2\"), m=String(\"m3\"), p=Int(3)",
            "f=String(\"f3\"), m=String(\"m4\"), p=Int(3)",
            "f=String(\"f3\"), m=String(\"m5\"), p=Int(3)",
        ])
    );
    assert_anchored(&db, query, "m");
}

#[test]
fn variable_length_pattern_with_bound_end_keeps_trails() {
    let mut db = forum_graph();
    let query = "MATCH (b:Person {id: 3}) WITH b, b.id AS id WITH b \
                 MATCH (a:Person)-[:KNOWS*1..2]->(b) RETURN a.id AS a";
    // Trails 1->3, 2->3, 1->2->3 and 4->1->3: one row per trail.
    assert_eq!(
        rows(&mut db, query),
        expected(&["a=Int(1)", "a=Int(1)", "a=Int(2)", "a=Int(4)"])
    );
    assert_anchored(&db, query, "b");

    let undirected = "MATCH (b:Person {id: 3}) WITH b, b.id AS id WITH b \
                      MATCH (a:Person)-[:KNOWS*2..2]-(b) RETURN a.id AS a";
    // Trails 3-1-2, 3-1-4 and 3-2-1; no relationship is used twice.
    let reference = "MATCH (b:Person {id: 3}) WITH b, b.id AS id WITH b \
                     MATCH (b)-[:KNOWS*2..2]-(a:Person) RETURN a.id AS a";
    let anchored = rows(&mut db, undirected);
    assert_eq!(anchored, rows(&mut db, reference));
    assert_eq!(anchored, expected(&["a=Int(1)", "a=Int(2)", "a=Int(4)"]));
}

#[test]
fn relationship_property_filters_survive_reversal() {
    let mut db = forum_graph();
    let query = "MATCH (f:Forum) WITH f, f.id AS id WITH f \
                 MATCH (p:Person)<-[r:HAS_MEMBER {year: 2020}]-(f) \
                 RETURN f.id AS f, p.id AS p, r.year AS year";
    assert_eq!(
        rows(&mut db, query),
        expected(&[
            "f=String(\"f1\"), p=Int(2), year=Int(2020)",
            "f=String(\"f2\"), p=Int(2), year=Int(2020)",
            "f=String(\"f3\"), p=Int(3), year=Int(2020)",
        ])
    );
    assert_anchored(&db, query, "f");
}

#[test]
fn self_loops_match_once_from_the_bound_end() {
    let mut db = forum_graph();
    let directed = "MATCH (b:Person {id: 2}) WITH b, b.id AS id WITH b \
                    MATCH (a)-[:LIKES]->(b) RETURN a.id AS a";
    assert_eq!(rows(&mut db, directed), expected(&["a=Int(2)"]));
    assert_anchored(&db, directed, "b");

    let undirected = "MATCH (b:Person {id: 2}) WITH b, b.id AS id WITH b \
                      MATCH (a)-[:LIKES]-(b) RETURN a.id AS a";
    assert_eq!(
        rows(&mut db, undirected),
        expected(&["a=Int(2)", "a=Int(3)"])
    );

    // The self-loop can be used once, so the two-step cycle has no match.
    let cycle = "MATCH (b:Person {id: 2}) WITH b, b.id AS id WITH b \
                 MATCH (a)-[:LIKES]->(b)-[:LIKES]->(a) RETURN a.id AS a";
    assert_eq!(rows(&mut db, cycle), Vec::<String>::new());
}

#[test]
fn multi_label_pattern_starts_from_the_bound_creator() {
    let mut db = forum_graph();
    let query = "MATCH (p:Person) WITH p, p.id AS id WITH p \
                 MATCH (m:Post:Comment)-[:HAS_CREATOR]->(p) RETURN p.id AS p, m.id AS m";
    assert_eq!(
        rows(&mut db, query),
        expected(&[
            "p=Int(3), m=String(\"c1\")",
            "p=Int(2), m=String(\"m1\")",
            "p=Int(5), m=String(\"m2\")",
            "p=Int(3), m=String(\"m3\")",
            "p=Int(3), m=String(\"m4\")",
            "p=Int(3), m=String(\"m5\")",
            "p=Int(2), m=String(\"m6\")",
        ])
    );
    let reference = "MATCH (p:Person) WITH p, p.id AS id WITH p \
                     MATCH (p)<-[:HAS_CREATOR]-(m:Post:Comment) RETURN p.id AS p, m.id AS m";
    let (actual, scans) = rows_and_scans(&mut db, query);
    let (reference_rows, reference_scans) = rows_and_scans(&mut db, reference);
    assert_eq!(actual, reference_rows);
    // The shared Person input may use a projected scan, which the full-node
    // counter does not observe. Reversing the MATCH must add no full scan
    // compared with writing the already-bound end first.
    assert_eq!(scans, reference_scans);
    assert_anchored(&db, query, "p");
}

#[test]
fn null_anchors_reject_matches_and_preserve_optional_padding() {
    let mut db = forum_graph();
    let prefix = "MATCH (a:Person) OPTIONAL MATCH (a)-[:LIKES]->(x) WITH x WITH x ";
    let required =
        format!("{prefix} MATCH (y:Post:Comment)-[:HAS_CREATOR]->(x) RETURN count(*) AS rows");
    assert_eq!(rows(&mut db, &required), expected(&["rows=Int(6)"]));
    assert_anchored(&db, &required, "x");
    let optional = format!(
        "{prefix} OPTIONAL MATCH (y:Post:Comment)-[:HAS_CREATOR]->(x) \
         RETURN count(y) AS messages, count(*) AS rows"
    );
    assert_eq!(
        rows(&mut db, &optional),
        expected(&["messages=Int(6), rows=Int(10)"])
    );
    assert_anchored(&db, &optional, "x");
}
