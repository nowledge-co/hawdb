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

//! `GraphMatchExec` reads only the node properties later operators use and
//! skips the records of anonymous relationships without property patterns
//! (#1427). Two WITH clauses keep these patterns as generic `GraphMatchExec`.
//! Every query runs on a resident store and on a checkpointed out-of-core
//! store, whose reads decode canonical records.

use super::*;
use hawdb_storage::store::{record_reads_for_test, RecordReadsForTest};

/// Persons 1..5 (`name` and a long `bio`) with KNOWS 1->2, 1->3, 2->4, 4->1
/// and the self-loop 3->3. Messages (label, creator, date): m1 (Post, 2, 3),
/// m2 (Comment, 2, 7), m3 (Post, 3, 9), m4 (Other, 3, 8). Person 1 LIKES
/// m1 (weight 2) and m3 (weight 5); person 4 LIKES m2 (weight 2).
fn populate(db: &mut Database) {
    for id in 1..=5 {
        db.query(&format!(
            "CREATE (:Person {{id: {id}, name: 'p{id}', bio: 'bio {id} {}'}})",
            "x".repeat(256)
        ))
        .unwrap();
    }
    for (source, target) in [(1, 2), (1, 3), (2, 4), (4, 1), (3, 3)] {
        db.query(&format!(
            "MATCH (a:Person {{id: {source}}}), (b:Person {{id: {target}}}) CREATE (a)-[:KNOWS]->(b)"
        ))
        .unwrap();
    }
    for (message, label, creator, date) in [
        ("m1", "Post", 2, 3),
        ("m2", "Comment", 2, 7),
        ("m3", "Post", 3, 9),
        ("m4", "Other", 3, 8),
    ] {
        db.query(&format!(
            "CREATE (:{label} {{id: '{message}', date: {date}, content: 'text of {message}'}})"
        ))
        .unwrap();
        db.query(&format!(
            "MATCH (m:{label} {{id: '{message}'}}), (p:Person {{id: {creator}}}) CREATE (m)-[:HAS_CREATOR]->(p)"
        ))
        .unwrap();
    }
    for (person, message, label, weight) in [
        (1, "m1", "Post", 2),
        (1, "m3", "Post", 5),
        (4, "m2", "Comment", 2),
    ] {
        db.query(&format!(
            "MATCH (p:Person {{id: {person}}}), (m:{label} {{id: '{message}'}}) \
             CREATE (p)-[:LIKES {{weight: {weight}}}]->(m)"
        ))
        .unwrap();
    }
}

/// The same graph resident in memory and checkpointed, reopened out of core.
fn stores() -> Vec<(Database, Option<std::path::PathBuf>)> {
    let mut resident = Database::new();
    populate(&mut resident);
    // Parallel fixtures must remain distinct even if the clock repeats.
    static NEXT_STORE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = NEXT_STORE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = unique_test_dir(&format!(
        "graph_match_reads_{}_{sequence}",
        std::process::id()
    ));
    {
        let mut db = Database::open(&path).unwrap();
        populate(&mut db);
        db.checkpoint().unwrap();
    }
    let canonical = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    vec![(resident, None), (canonical, Some(path))]
}

const START: &str = "MATCH (s:Person {id: 1}) WITH s WITH s ";

fn render(output: &QueryOutput) -> Vec<String> {
    output
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|(column, value)| format!("{column}={value:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect()
}

/// Rows in result order and the point reads the query started on this thread.
fn run(
    db: &mut Database,
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> (Vec<String>, RecordReadsForTest) {
    let before = record_reads_for_test();
    let output = db.query_with_params(query, parameters).unwrap();
    (render(&output), record_reads_for_test().since(before))
}

fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

fn expected(rows: &[&str]) -> Vec<String> {
    sorted(rows.iter().map(ToString::to_string).collect())
}

/// Checks a query on both stores against `rows`, as a multiset unless
/// `ordered`, and returns the reads of the out-of-core run.
fn check(query: &str, rows: &[&str], ordered: bool) -> RecordReadsForTest {
    check_with(query, &BTreeMap::new(), rows, ordered)
}

fn check_with(
    query: &str,
    parameters: &BTreeMap<String, Value>,
    rows: &[&str],
    ordered: bool,
) -> RecordReadsForTest {
    let mut reads = RecordReadsForTest::default();
    for (mut db, path) in stores() {
        let explain = db.explain_query_with_params(query, parameters).unwrap();
        let plan = explain.physical_plan.explain(0);
        assert!(plan.contains("GraphMatchExec"), "{query}:\n{plan}");
        let (actual, run_reads) = run(&mut db, query, parameters);
        if ordered {
            let expected = rows.iter().map(ToString::to_string).collect::<Vec<_>>();
            assert_eq!(actual, expected, "{query}");
        } else {
            assert_eq!(sorted(actual), expected(rows), "{query}");
        }
        reads = run_reads;
        drop(db);
        if let Some(path) = path {
            std::fs::remove_dir_all(path).unwrap();
        }
    }
    reads
}

fn last_graph_match(query: &str) -> String {
    let mut db = Database::new();
    populate(&mut db);
    let explain = db.explain_query(query).unwrap();
    let plan = explain.physical_plan.explain(0);
    plan.lines()
        .find(|line| line.contains("GraphMatchExec"))
        .unwrap_or_else(|| panic!("no GraphMatchExec in:\n{plan}"))
        .to_string()
}

#[test]
fn anonymous_relationships_and_unused_properties_are_not_read() {
    let query =
        format!("{START}MATCH (s)-[:KNOWS]->(f)<-[:HAS_CREATOR]-(m) RETURN f.name AS f, m.id AS m");
    let line = last_graph_match(&query);
    assert!(
        line.contains(r#"projected_nodes={"f": {"name"}, "m": {"id"}, "s": {}}"#),
        "{line}"
    );
    let reads = check(
        &query,
        &[
            r#"f=String("p2"), m=String("m1")"#,
            r#"f=String("p2"), m=String("m2")"#,
            r#"f=String("p3"), m=String("m3")"#,
            r#"f=String("p3"), m=String("m4")"#,
        ],
        false,
    );
    // Before #1427 this read every relationship record and every node in full.
    assert_eq!(reads.full_relationships, 0, "{reads:?}");
    assert_eq!(reads.full_nodes, 0, "{reads:?}");
    assert!(reads.projected_nodes >= 5, "{reads:?}");
}

#[test]
fn whole_node_values_keep_every_property() {
    let returned = format!("{START}MATCH (s)-[:KNOWS]->(f) RETURN f AS f");
    let mut db = Database::new();
    populate(&mut db);
    let (rows, reads) = run(&mut db, &returned, &BTreeMap::new());
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert!(row.contains("\"bio\"") && row.contains("\"name\""), "{row}");
    }
    assert!(reads.full_nodes >= 2, "{reads:?}");

    let collected = format!("{START}MATCH (s)-[:KNOWS]->(f) RETURN collect(f) AS fs");
    let (rows, _) = run(&mut db, &collected, &BTreeMap::new());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].matches("\"bio\"").count(), 2, "{}", rows[0]);
    assert!(!last_graph_match(&collected).contains("\"f\":"));
}

#[test]
fn node_equality_and_node_lists_compare_whole_nodes() {
    check(
        &format!("{START}MATCH (s)-[:KNOWS]->(f)-[:KNOWS]->(g) WHERE f = g RETURN f.id AS f"),
        &["f=Int(3)"],
        false,
    );
    check(
        &format!("{START}MATCH (s)-[:KNOWS]->(f)-[:KNOWS]->(g) WHERE NOT f = g RETURN g.id AS g"),
        &["g=Int(4)"],
        false,
    );
    check(
        &format!(
            "{START}MATCH (s)-[:KNOWS]->(f) WITH collect(f) AS fs \
             MATCH (x:Person)-[:KNOWS]->(y) WHERE y IN fs RETURN x.id AS x, y.id AS y"
        ),
        &[
            "x=Int(1), y=Int(2)",
            "x=Int(1), y=Int(3)",
            "x=Int(3), y=Int(3)",
        ],
        false,
    );
}

#[test]
fn where_order_by_and_variable_length_read_their_properties() {
    check(
        &format!(
            "{START}MATCH (s)-[:KNOWS]->(f)<-[:HAS_CREATOR]-(m) WHERE m.date > 5 RETURN m.id AS m"
        ),
        &[
            r#"m=String("m2")"#,
            r#"m=String("m3")"#,
            r#"m=String("m4")"#,
        ],
        false,
    );
    let ordered =
        format!("{START}MATCH (s)-[:KNOWS*1..2]->(f) RETURN f.id AS id ORDER BY f.name DESC");
    assert!(
        last_graph_match(&ordered).contains(r#""f": {"id", "name"}"#),
        "{}",
        last_graph_match(&ordered)
    );
    // Trails 1->2, 1->3, 1->2->4 and 1->3->3 (the self-loop).
    let reads = check(
        &ordered,
        &["id=Int(4)", "id=Int(3)", "id=Int(3)", "id=Int(2)"],
        true,
    );
    assert_eq!(reads.full_nodes, 0, "{reads:?}");
}

#[test]
fn multi_label_and_bound_targets_match_as_before() {
    check(
        &format!(
            "{START}MATCH (s)-[:KNOWS]->(f)<-[:HAS_CREATOR]-(m:Post:Comment) RETURN m.id AS m"
        ),
        &[
            r#"m=String("m1")"#,
            r#"m=String("m2")"#,
            r#"m=String("m3")"#,
        ],
        false,
    );
    // The last step reaches the bound start node again.
    check(
        &format!(
            "{START}MATCH (s)-[:KNOWS]->(a)-[:KNOWS]->(b)-[:KNOWS]->(s) RETURN a.id AS a, b.id AS b"
        ),
        &["a=Int(2), b=Int(4)"],
        false,
    );
    // A later MATCH filters a node the earlier MATCH bound.
    check(
        &format!(
            "{START}MATCH (s)-[:KNOWS]->(f) MATCH (f {{name: 'p2'}})-[:KNOWS]->(g) RETURN g.id AS g"
        ),
        &["g=Int(4)"],
        false,
    );
}

#[test]
fn trail_uniqueness_and_undirected_self_loops_are_unchanged() {
    // One MATCH: the second relationship differs from the first.
    check(
        &format!("{START}MATCH (s)-[:KNOWS]->(f)-[:KNOWS]-(g) RETURN f.id AS f, g.id AS g"),
        &["f=Int(2), g=Int(4)", "f=Int(3), g=Int(3)"],
        false,
    );
    // Two MATCH clauses may reuse a relationship; the self-loop counts once.
    check(
        &format!(
            "{START}MATCH (s)-[:KNOWS]->(f) MATCH (f)-[:KNOWS]-(g) RETURN f.id AS f, g.id AS g"
        ),
        &[
            "f=Int(2), g=Int(1)",
            "f=Int(2), g=Int(4)",
            "f=Int(3), g=Int(1)",
            "f=Int(3), g=Int(3)",
        ],
        false,
    );
}

#[test]
fn named_relationships_and_relationship_properties_are_read() {
    let reads = check(
        &format!("{START}MATCH (s)-[l:LIKES {{weight: 2}}]->(m) RETURN m.id AS m, l.weight AS w"),
        &[r#"m=String("m1"), w=Int(2)"#],
        false,
    );
    assert!(reads.full_relationships >= 2, "{reads:?}");
    check(
        &format!("{START}MATCH (s)-[r:KNOWS]->(f) RETURN type(r) AS t, f.id AS f"),
        &[
            r#"t=String("KNOWS"), f=Int(2)"#,
            r#"t=String("KNOWS"), f=Int(3)"#,
        ],
        false,
    );
}

#[test]
fn optional_match_pads_and_counts_as_before() {
    check(
        &format!(
            "{START}MATCH (s)-[:KNOWS*1..2]->(f) OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(m) \
             RETURN f.id AS f, m.id AS m"
        ),
        &[
            r#"f=Int(2), m=String("m1")"#,
            r#"f=Int(2), m=String("m2")"#,
            r#"f=Int(3), m=String("m3")"#,
            r#"f=Int(3), m=String("m3")"#,
            r#"f=Int(3), m=String("m4")"#,
            r#"f=Int(3), m=String("m4")"#,
            "f=Int(4), m=Null",
        ],
        false,
    );
    let counted = format!(
        "{START}MATCH (s)-[:KNOWS]->(f) WITH f OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(m) \
         WITH f, count(m) AS n RETURN f.id AS f, n"
    );
    assert!(
        last_graph_match(&counted).contains(r#""m": {}"#),
        "{}",
        last_graph_match(&counted)
    );
    check(
        &counted,
        &["f=Int(2), n=Int(2)", "f=Int(3), n=Int(2)"],
        false,
    );
}

#[test]
fn cached_plans_keep_their_projection_across_parameters() {
    let query = format!("{START}MATCH (s)-[:KNOWS]->(f) WHERE f.id = $id RETURN f.name AS name");
    for (mut db, path) in stores() {
        // The second and third runs reuse the cached plan with new values.
        for (id, name) in [(2, "p2"), (3, "p3"), (2, "p2")] {
            let parameters = BTreeMap::from([("id".to_string(), Value::Int(id))]);
            let (rows, reads) = run(&mut db, &query, &parameters);
            assert_eq!(rows, vec![format!(r#"name=String("{name}")"#)]);
            assert_eq!(reads.full_nodes, 0, "{reads:?}");
        }
        drop(db);
        if let Some(path) = path {
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
