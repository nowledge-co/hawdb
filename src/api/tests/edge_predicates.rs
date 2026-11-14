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

//! `GraphMatchExec` evaluates WHERE conjuncts that read only the relationship
//! a one-hop step binds as soon as it reads the relationship, before it reads
//! the target node, and reads only the relationship properties later
//! operators use (#1483). Two WITH clauses keep these patterns as generic
//! `GraphMatchExec`. Every query runs on a resident store and on a
//! checkpointed out-of-core store, whose reads decode canonical records.

use super::*;
use hawdb_storage::store::{record_reads_for_test, RecordReadsForTest};

/// Shaped like the HAS_MEMBER step of LDBC IC5. Persons 1..3. Forums f1..f5
/// (`title` and a long `about`) and the Group g1 are members' containers.
/// HAS_MEMBER (container -> person, joinDate, role): f1->1 (1), f2->1 (4,
/// admin), f3->1 (6), f4->1 (no date), f5->1 (9), g1->1 (7), f1->3 (2).
/// Roles default to `member`.
fn populate(db: &mut Database) {
    for id in 1..=3 {
        db.query(&format!("CREATE (:Person {{id: {id}}})")).unwrap();
    }
    for forum in 1..=5 {
        db.query(&format!(
            "CREATE (:Forum {{title: 'f{forum}', about: '{}'}})",
            "x".repeat(256)
        ))
        .unwrap();
    }
    db.query("CREATE (:Group {title: 'g1'})").unwrap();
    for (label, title, person, date, role) in [
        ("Forum", "f1", 1, Some(1), "member"),
        ("Forum", "f2", 1, Some(4), "admin"),
        ("Forum", "f3", 1, Some(6), "member"),
        ("Forum", "f4", 1, None, "member"),
        ("Forum", "f5", 1, Some(9), "member"),
        ("Group", "g1", 1, Some(7), "member"),
        ("Forum", "f1", 3, Some(2), "member"),
    ] {
        let date = date
            .map(|date| format!(", joinDate: {date}"))
            .unwrap_or_default();
        db.query(&format!(
            "MATCH (f:{label} {{title: '{title}'}}), (p:Person {{id: {person}}}) \
             CREATE (f)-[:HAS_MEMBER {{role: '{role}'{date}}}]->(p)"
        ))
        .unwrap();
    }
}

/// The same graph resident in memory and checkpointed, reopened out of core.
fn stores() -> Vec<(Database, Option<std::path::PathBuf>)> {
    let mut resident = Database::new();
    populate(&mut resident);
    static NEXT_STORE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = NEXT_STORE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = unique_test_dir(&format!(
        "edge_predicates_{}_{sequence}",
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

fn release(db: Database, path: Option<std::path::PathBuf>) {
    drop(db);
    if let Some(path) = path {
        std::fs::remove_dir_all(path).unwrap();
    }
}

const START: &str = "MATCH (p:Person {id: 1}) WITH p WITH p ";

/// Rows as a sorted multiset, and the point reads the query started on this
/// thread.
fn run(
    db: &mut Database,
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> (Vec<String>, RecordReadsForTest) {
    let before = record_reads_for_test();
    let output = db.query_with_params(query, parameters).unwrap();
    let reads = record_reads_for_test().since(before);
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
    (rows, reads)
}

fn check(query: &str, rows: &[&str]) {
    let mut expected = rows.iter().map(ToString::to_string).collect::<Vec<_>>();
    expected.sort();
    for (mut db, path) in stores() {
        let (actual, _) = run(&mut db, query, &BTreeMap::new());
        assert_eq!(actual, expected, "{query}");
        release(db, path);
    }
}

fn min_date(date: i64) -> BTreeMap<String, Value> {
    BTreeMap::from([("min".to_string(), Value::Int(date))])
}

fn graph_match_line(query: &str) -> String {
    let mut db = Database::new();
    populate(&mut db);
    let explain = db.explain_query_with_params(query, &min_date(0)).unwrap();
    let plan = explain.physical_plan.explain(0);
    plan.lines()
        .rfind(|line| line.contains("GraphMatchExec"))
        .unwrap_or_else(|| panic!("no GraphMatchExec in:\n{plan}"))
        .to_string()
}

const IC5: &str = "MATCH (p)<-[m:HAS_MEMBER]-(f:Forum) WHERE m.joinDate > $min \
                   RETURN f.title AS a, m.joinDate AS b";

#[test]
fn targets_of_rejected_relationships_are_not_read() {
    let query = format!("{START}{IC5}");
    let line = graph_match_line(&query);
    assert!(line.contains("edge_predicates=[0]"), "{line}");
    assert!(
        line.contains(r#"projected_relationships={"m": {"joinDate"}}"#),
        "{line}"
    );
    for (mut db, path) in stores() {
        let (rows, all) = run(&mut db, &query, &min_date(0));
        assert_eq!(
            rows,
            [
                r#"a=String("f1"), b=Int(1)"#,
                r#"a=String("f2"), b=Int(4)"#,
                r#"a=String("f3"), b=Int(6)"#,
                r#"a=String("f5"), b=Int(9)"#,
            ]
        );
        // The cached plan, with a date no membership passes.
        let (rows, none) = run(&mut db, &query, &min_date(100));
        assert!(rows.is_empty(), "{rows:?}");
        // Six memberships are read either way, each with only its joinDate.
        for reads in [all, none] {
            assert_eq!(reads.full_relationships, 0, "{reads:?}");
            assert_eq!(reads.projected_relationships, 6, "{reads:?}");
        }
        // Five of them pass `joinDate > 0` (g1 among them, which is no
        // Forum); f4 has no joinDate. Before #1483 every membership read its
        // target, so both runs read the same nodes.
        let nodes = |reads: RecordReadsForTest| reads.full_nodes + reads.projected_nodes;
        assert_eq!(nodes(all) - nodes(none), 5, "{all:?} {none:?}");
        release(db, path);
    }
}

#[test]
fn relationship_conjuncts_keep_three_valued_semantics() {
    let query = |filter: &str| {
        format!("{START}MATCH (p)<-[m:HAS_MEMBER]-(f:Forum) WHERE {filter} RETURN f.title AS a")
    };
    // Mixed with a target conjunct.
    check(
        &query("m.joinDate > 3 AND f.title <> 'f5'"),
        &[r#"a=String("f2")"#, r#"a=String("f3")"#],
    );
    check(
        &query("(m.joinDate < 2 OR m.joinDate > 8)"),
        &[r#"a=String("f1")"#, r#"a=String("f5")"#],
    );
    // NOT of NULL (f4) is NULL and drops the row.
    check(
        &query("NOT m.joinDate > 4"),
        &[r#"a=String("f1")"#, r#"a=String("f2")"#],
    );
    check(&query("m.joinDate IS NULL"), &[r#"a=String("f4")"#]);
    // A pattern property and a WHERE conjunct on the same relationship.
    check(
        &format!(
            "{START}MATCH (p)<-[m:HAS_MEMBER {{role: 'admin'}}]-(f:Forum) WHERE m.joinDate > 0 \
             RETURN f.title AS a, m.joinDate AS b"
        ),
        &[r#"a=String("f2"), b=Int(4)"#],
    );
}

#[test]
fn whole_relationships_keep_every_property() {
    let query = format!(
        "{START}MATCH (p)<-[m:HAS_MEMBER]-(f:Forum) WHERE m.joinDate > 8 RETURN m AS m, type(m) AS t"
    );
    assert!(!graph_match_line(&query).contains("projected_relationships"));
    for (mut db, path) in stores() {
        let (rows, reads) = run(&mut db, &query, &BTreeMap::new());
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert!(
            rows[0].contains("\"joinDate\"") && rows[0].contains("\"role\""),
            "{}",
            rows[0]
        );
        assert!(rows[0].contains(r#"t=String("HAS_MEMBER")"#), "{}", rows[0]);
        assert_eq!(reads.projected_relationships, 0, "{reads:?}");
        release(db, path);
    }
}

#[test]
fn optional_match_pads_when_every_relationship_fails_its_conjunct() {
    let query = |date: i64| {
        format!(
            "MATCH (p:Person) WITH p WITH p OPTIONAL MATCH (p)<-[m:HAS_MEMBER]-(f:Forum) \
             WHERE m.joinDate > {date} RETURN p.id AS a, f.title AS b"
        )
    };
    assert!(graph_match_line(&query(100)).contains("edge_predicates=[0]"));
    check(
        &query(100),
        &["a=Int(1), b=Null", "a=Int(2), b=Null", "a=Int(3), b=Null"],
    );
    check(
        &query(5),
        &[
            r#"a=Int(1), b=String("f3")"#,
            r#"a=Int(1), b=String("f5")"#,
            "a=Int(2), b=Null",
            "a=Int(3), b=Null",
        ],
    );
}

#[test]
fn relationship_conjunct_errors_are_raised_only_for_complete_matches() {
    // `head()` of an integer fails. Only g1's membership (7) fails it here,
    // and g1 is no Forum, so the end-of-match evaluation never sees it.
    let failing = |date: i64| {
        format!(
            "{START}MATCH (p)<-[m:HAS_MEMBER]-(f:Forum) \
             WHERE head(CASE WHEN m.joinDate = {date} THEN m.joinDate ELSE [1] END) = 1 \
             RETURN f.title AS a"
        )
    };
    assert!(graph_match_line(&failing(7)).contains("edge_predicates=[0]"));
    check(
        &failing(7),
        &[
            r#"a=String("f1")"#,
            r#"a=String("f2")"#,
            r#"a=String("f3")"#,
            r#"a=String("f4")"#,
            r#"a=String("f5")"#,
        ],
    );
    // f3's membership fails it and completes the match: the error is raised.
    for (mut db, path) in stores() {
        assert!(db.query(&failing(6)).is_err());
        release(db, path);
    }
}
