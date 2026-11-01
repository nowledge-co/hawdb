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

//! A `GraphMatchExec` evaluates each WHERE conjunct right after the step that
//! binds its last variable, before later steps expand the candidate (#1424).
//! Two WITH clauses keep these patterns as generic `GraphMatchExec`.

use super::*;

/// A small message graph shaped like LDBC IC3.
///
/// Person 1 knows 2 and 3, and 2 knows 4. Countries X, Y and Z. Messages
/// (creator, creationDate, country): m1 (2, 5, X), m2 (2, 12, X),
/// m3 (2, 15, Z), m4 (3, 19, Y), m5 (3, 25, Y), m6 (3, no date, X),
/// m7 (4, 10, X), m8 (4, 20, Y), m9 (1, 12, X).
fn message_graph() -> Database {
    let mut db = Database::new();
    for id in 1..=4 {
        db.query(&format!("CREATE (:Person {{id: {id}}})")).unwrap();
    }
    for name in ["X", "Y", "Z"] {
        db.query(&format!("CREATE (:Country {{name: '{name}'}})"))
            .unwrap();
    }
    for (source, target) in [(1, 2), (1, 3), (2, 4)] {
        db.query(&format!(
            "MATCH (a:Person {{id: {source}}}), (b:Person {{id: {target}}}) CREATE (a)-[:KNOWS]->(b)"
        ))
        .unwrap();
    }
    for (message, creator, date, country) in [
        ("m1", 2, Some(5), "X"),
        ("m2", 2, Some(12), "X"),
        ("m3", 2, Some(15), "Z"),
        ("m4", 3, Some(19), "Y"),
        ("m5", 3, Some(25), "Y"),
        ("m6", 3, None, "X"),
        ("m7", 4, Some(10), "X"),
        ("m8", 4, Some(20), "Y"),
        ("m9", 1, Some(12), "X"),
    ] {
        let label = if message == "m4" { "Comment" } else { "Post" };
        let date = date
            .map(|date| format!(", creationDate: {date}"))
            .unwrap_or_default();
        db.query(&format!("CREATE (:{label} {{id: '{message}'{date}}})"))
            .unwrap();
        db.query(&format!(
            "MATCH (m:{label} {{id: '{message}'}}), (p:Person {{id: {creator}}}) CREATE (m)-[:HAS_CREATOR]->(p)"
        ))
        .unwrap();
        db.query(&format!(
            "MATCH (m:{label} {{id: '{message}'}}), (c:Country {{name: '{country}'}}) CREATE (m)-[:IS_LOCATED_IN]->(c)"
        ))
        .unwrap();
    }
    db
}

/// Rows as a sorted multiset; MATCH has no inherent row order.
fn rows(db: &mut Database, query: &str, parameters: &BTreeMap<String, Value>) -> Vec<String> {
    let output = db.query_with_params(query, parameters).unwrap();
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

/// Rows and the adjacency visits (one per expanded node and direction) the
/// query started on this thread.
fn rows_and_visits(
    db: &mut Database,
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> (Vec<String>, u64) {
    let before = hawdb_storage::store::adjacency_visits_for_test();
    let rows = rows(db, query, parameters);
    (
        rows,
        hawdb_storage::store::adjacency_visits_for_test() - before,
    )
}

fn expected(rows: &[&str]) -> Vec<String> {
    let mut rows = rows.iter().map(ToString::to_string).collect::<Vec<_>>();
    rows.sort();
    rows
}

fn dates() -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("startDate".to_string(), Value::Int(10)),
        ("endDate".to_string(), Value::Int(20)),
    ])
}

/// The plan line of the last `GraphMatchExec`, which runs the final MATCH.
fn last_graph_match(db: &Database, query: &str, parameters: &BTreeMap<String, Value>) -> String {
    let explain = db.explain_query_with_params(query, parameters).unwrap();
    let plan = explain.physical_plan.explain(0);
    plan.lines()
        .find(|line| line.contains("GraphMatchExec"))
        .unwrap_or_else(|| panic!("no GraphMatchExec in:\n{plan}"))
        .to_string()
}

const IC3_FRIENDS: &str = "MATCH (countryX:Country {name: 'X'}), (countryY:Country {name: 'Y'}), (person:Person {id: 1}) \
     WITH person, countryX, countryY \
     MATCH (person)-[:KNOWS*1..2]-(friend:Person) WHERE NOT person = friend \
     WITH DISTINCT friend, countryX, countryY ";

fn ic3(filter: &str) -> String {
    format!(
        "{IC3_FRIENDS}MATCH (friend)<-[:HAS_CREATOR]-(message), (message)-[:IS_LOCATED_IN]->(country) \
         WHERE {filter} \
         RETURN friend.id AS friend, message.id AS message, country.name AS country"
    )
}

#[test]
fn ic3_date_conjunct_runs_before_the_country_expansion() {
    let mut db = message_graph();
    let dated =
        ic3("$endDate > message.creationDate >= $startDate AND country IN [countryX, countryY]");
    let line = last_graph_match(&db, &dated, &dates());
    // Steps: friend, <-HAS_CREATOR- message, message, -IS_LOCATED_IN-> country.
    // Both date comparisons run once `message` is bound, before the country step.
    assert!(line.contains("predicate_after_steps=[2, 2, 4]"), "{line}");
    let explain = db.explain_query_with_params(&dated, &dates()).unwrap();
    assert!(
        explain.trace.decisions.iter().any(|decision| decision
            == "evaluate 2 of 3 GraphMatchExec WHERE conjunct(s) before the last step: after steps [2, 2, 4] of 4"),
        "{:?}",
        explain.trace.decisions
    );

    let (dated_rows, dated_visits) = rows_and_visits(&mut db, &dated, &dates());
    assert_eq!(
        dated_rows,
        expected(&[
            "friend=Int(2), message=String(\"m2\"), country=String(\"X\")",
            "friend=Int(3), message=String(\"m4\"), country=String(\"Y\")",
            "friend=Int(4), message=String(\"m7\"), country=String(\"X\")",
        ])
    );
    let (_, undated_visits) =
        rows_and_visits(&mut db, &ic3("country IN [countryX, countryY]"), &dates());
    // m1, m5, m6 (no date) and m8 fail the date conjunct and are never
    // expanded over IS_LOCATED_IN; the end-of-match filter expanded all eight.
    assert_eq!(undated_visits - dated_visits, 4);
}

#[test]
fn ic3_rows_match_the_end_of_match_filter() {
    let mut db = message_graph();
    for (filter, rows_expected) in [
        (
            "message.creationDate >= 15 AND country IN [countryX, countryY]",
            vec![
                "friend=Int(3), message=String(\"m4\"), country=String(\"Y\")",
                "friend=Int(3), message=String(\"m5\"), country=String(\"Y\")",
                "friend=Int(4), message=String(\"m8\"), country=String(\"Y\")",
            ],
        ),
        // A conjunct on an imported variable runs before the first step.
        (
            "friend.id = 2 AND message.creationDate < 13",
            vec![
                "friend=Int(2), message=String(\"m1\"), country=String(\"X\")",
                "friend=Int(2), message=String(\"m2\"), country=String(\"X\")",
            ],
        ),
        // NULL dates make the comparison and its negation unknown.
        (
            "NOT message.creationDate < 15 AND country = countryX",
            vec![],
        ),
        (
            "message.creationDate IS NULL AND country = countryX",
            vec!["friend=Int(3), message=String(\"m6\"), country=String(\"X\")"],
        ),
        // A disjunction across both variables is not split.
        (
            "(message.creationDate = 5 OR country.name = 'Z') AND friend.id <> 3",
            vec![
                "friend=Int(2), message=String(\"m1\"), country=String(\"X\")",
                "friend=Int(2), message=String(\"m3\"), country=String(\"Z\")",
            ],
        ),
        (
            "message.id IN [x IN ['m2', 'm4', 'm9'] WHERE x <> 'm4'] AND country.name = 'X'",
            vec!["friend=Int(2), message=String(\"m2\"), country=String(\"X\")"],
        ),
    ] {
        assert_eq!(
            rows(&mut db, &ic3(filter), &dates()),
            expected(&rows_expected),
            "{filter}"
        );
    }
}

#[test]
fn optional_match_pads_rows_whose_candidates_all_fail_an_early_conjunct() {
    let mut db = message_graph();
    let query = "MATCH (p:Person) WITH p WITH p \
         OPTIONAL MATCH (p)<-[:HAS_CREATOR]-(m)-[:IS_LOCATED_IN]->(c) \
         WHERE m.creationDate >= 15 AND c.name = 'Y' \
         RETURN p.id AS person, m.id AS message";
    let line = last_graph_match(&db, query, &BTreeMap::new());
    assert!(line.contains("optional=true"), "{line}");
    assert!(line.contains("predicate_after_steps=[2, 3]"), "{line}");
    // Person 1 has only m9 (date 12): every candidate fails the early date
    // conjunct. Person 2 has m3 (15) but in Z: it passes the date and fails
    // the country. Both are padded exactly once.
    assert_eq!(
        rows(&mut db, query, &BTreeMap::new()),
        expected(&[
            "person=Int(1), message=Null",
            "person=Int(2), message=Null",
            "person=Int(3), message=String(\"m4\")",
            "person=Int(3), message=String(\"m5\")",
            "person=Int(4), message=String(\"m8\")",
        ])
    );
    // A conjunct on the imported row alone pads every row it rejects.
    let query = "MATCH (p:Person) WITH p WITH p \
         OPTIONAL MATCH (p)<-[:HAS_CREATOR]-(m) WHERE p.id > 3 AND m.creationDate > 0 \
         RETURN p.id AS person, m.id AS message";
    assert!(last_graph_match(&db, query, &BTreeMap::new()).contains("predicate_after_steps=[0, 2]"));
    assert_eq!(
        rows(&mut db, query, &BTreeMap::new()),
        expected(&[
            "person=Int(1), message=Null",
            "person=Int(2), message=Null",
            "person=Int(3), message=Null",
            "person=Int(4), message=String(\"m7\")",
            "person=Int(4), message=String(\"m8\")",
        ])
    );
}

#[test]
fn variable_length_and_trail_semantics_are_unchanged_by_early_conjuncts() {
    let mut db = message_graph();
    // `friend` is bound only when a whole KNOWS trail is found; the
    // relationship of a trail cannot be reused by the second hop.
    let query = "MATCH (p:Person {id: 1}) WITH p WITH p \
         MATCH (p)-[:KNOWS*1..2]-(friend)-[k:KNOWS]-(other) \
         WHERE friend.id <> 3 AND other.id <> 4 \
         RETURN friend.id AS friend, other.id AS other";
    let line = last_graph_match(&db, query, &BTreeMap::new());
    assert!(line.contains("predicate_after_steps=[2, 3]"), "{line}");
    // Trails: 1-2 then 2-4 (other 4, rejected); 1-2-4 then 4-? (no unused
    // KNOWS); 1-3 rejected early.
    assert_eq!(rows(&mut db, query, &BTreeMap::new()), Vec::<String>::new());
    let query = "MATCH (p:Person {id: 1}) WITH p WITH p \
         MATCH (p)-[:KNOWS*1..2]-(friend)-[k:KNOWS]-(other) \
         WHERE friend.id <> 3 \
         RETURN friend.id AS friend, other.id AS other";
    assert_eq!(
        rows(&mut db, query, &BTreeMap::new()),
        expected(&["friend=Int(2), other=Int(4)"])
    );
}

#[test]
fn anchored_optional_match_places_conjuncts_after_the_reordered_steps() {
    let mut db = Database::new();
    for (label, id) in [("Forum", 1), ("Forum", 2), ("Person", 2), ("Person", 3)] {
        db.query(&format!("CREATE (:{label} {{id: {id}}})"))
            .unwrap();
    }
    for (id, year) in [(10, 2021), (11, 2018), (12, 2022)] {
        db.query(&format!("CREATE (:Post {{id: {id}, year: {year}}})"))
            .unwrap();
    }
    for (forum, post, person) in [(1, 10, 2), (1, 11, 3), (2, 12, 2)] {
        db.query(&format!(
            "MATCH (f:Forum {{id: {forum}}}), (m:Post {{id: {post}}}) \
             CREATE (f)-[:CONTAINER_OF]->(m)"
        ))
        .unwrap();
        db.query(&format!(
            "MATCH (m:Post {{id: {post}}}), (p:Person {{id: {person}}}) \
             CREATE (m)-[:HAS_CREATOR]->(p)"
        ))
        .unwrap();
    }
    let prefix = "MATCH (f:Forum) WITH f, f.id AS id WITH f ";
    let suffix = "WHERE m.year > $minYear AND f.id = 1 AND p.id = 2 \
                  RETURN f.id AS f, m.id AS m, p.id AS p";
    let query = format!(
        "{prefix} OPTIONAL MATCH (p:Person)<-[:HAS_CREATOR]-(m:Post:Comment)<-[:CONTAINER_OF]-(f) {suffix}"
    );
    let reference = format!(
        "{prefix} OPTIONAL MATCH (f)-[:CONTAINER_OF]->(m:Post:Comment)-[:HAS_CREATOR]->(p:Person) {suffix}"
    );
    let parameters = BTreeMap::from([("minYear".into(), Value::Int(2020))]);
    let actual = rows(&mut db, &query, &parameters);
    assert_eq!(
        actual,
        expected(&["f=Int(1), m=Int(10), p=Int(2)", "f=Int(2), m=Null, p=Null"])
    );
    assert_eq!(actual, rows(&mut db, &reference, &parameters));
    let plan = last_graph_match(&db, &query, &parameters);
    assert!(
        plan.contains("steps=[Node(GraphMatchNode { variable: \"f\""),
        "{plan}"
    );
    assert!(plan.contains("predicate_after_steps=[2, 0, 3]"), "{plan}");
    // `year` is used only by WHERE and must survive final property projection.
    assert!(plan.contains("projected_nodes="), "{plan}");
    assert!(plan.contains("\"m\": {\"id\", \"year\"}"), "{plan}");
}
