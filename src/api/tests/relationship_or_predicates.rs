// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Relationship SET filtered by `OR` and `IS NULL` predicates over
//! relationship properties and parameters, with node property filters on
//! the endpoints (#1792). Expected effects were checked against Ladybug.

use super::set_expressions::{column, on_every_surface, params, string};
use super::*;

const LINKS: &[&str] = &[
    "CREATE (:E {id: 'a'})",
    "CREATE (:E {id: 'b'})",
    "CREATE (:E {id: 'c'})",
    "MATCH (a:E {id: 'a'}), (b:E {id: 'b'}) CREATE (a)-[:R {t: 'x', n: 1}]->(b)",
    "MATCH (a:E {id: 'a'}), (b:E {id: 'b'}) CREATE (a)-[:R {s: 'old', n: 2}]->(b)",
    "MATCH (a:E {id: 'a'}), (b:E {id: 'b'}) CREATE (a)-[:R {s: 'other', t: 'x', n: 3}]->(b)",
    "MATCH (a:E {id: 'a'}), (c:E {id: 'c'}) CREATE (a)-[:R {n: 4}]->(c)",
];
const READ: &str = "MATCH ()-[r:R]->() RETURN r.n AS n, r.s AS s ORDER BY n";

const REPAIR: &str = "MATCH (:E {id: $source})-[r:R]->(:E {id: $target}) \
    WHERE r.s = $old OR (r.s IS NULL AND $old IS NULL) SET r.s = 'default'";

fn spaces(rows: &[BTreeMap<String, Value>]) -> Vec<Value> {
    column(rows, "s")
}

/// `$old` NULL selects relationships without the property; a value selects
/// equal ones. Only the pair the endpoint filters name is touched.
#[test]
fn null_aware_or_selects_relationships_between_filtered_endpoints() {
    for (old, expected) in [
        (
            Value::Null,
            vec![
                string("default"),
                string("old"),
                string("other"),
                Value::Null,
            ],
        ),
        (
            string("old"),
            vec![Value::Null, string("default"), string("other"), Value::Null],
        ),
        (
            string("missing"),
            vec![Value::Null, string("old"), string("other"), Value::Null],
        ),
    ] {
        on_every_surface(
            LINKS,
            REPAIR,
            &params(&[
                ("source", string("a")),
                ("target", string("b")),
                ("old", old.clone()),
            ]),
            READ,
            |surface, _, stored| {
                assert_eq!(spaces(&stored), expected, "{surface}: old={old:?}");
            },
        );
    }
}

/// Several null-aware equality groups joined by AND, and RETURN.
#[test]
fn conjunctions_of_null_aware_comparisons_and_return() {
    on_every_surface(
        LINKS,
        "MATCH (a:E {id: $source})-[r:R]->(b:E {id: $target}) WHERE r.s = $from \
         AND ((r.t IS NULL AND $t IS NULL) OR r.t = $t) \
         AND ((r.n IS NULL AND $n IS NULL) OR r.n = $n) \
         SET r.s = $to RETURN r.n AS n",
        &params(&[
            ("source", string("a")),
            ("target", string("b")),
            ("from", string("old")),
            ("t", Value::Null),
            ("n", Value::Int(2)),
            ("to", string("moved")),
        ]),
        READ,
        |surface, returned, stored| {
            assert_eq!(column(&returned, "n"), [Value::Int(2)], "{surface}");
            assert_eq!(
                spaces(&stored),
                [Value::Null, string("moved"), string("other"), Value::Null],
                "{surface}"
            );
        },
    );
    // A property filter the storage command evaluates keeps its plan.
    let mut db = Database::new();
    for query in LINKS {
        db.query(query).unwrap();
    }
    let plan = format!(
        "{:?}",
        db.query(
            "EXPLAIN MATCH (:E {id: 'a'})-[r:R]->(:E {id: 'b'}) WHERE r.s = 'old' SET r.s = 'x'"
        )
        .unwrap()
        .rows
    );
    assert!(plan.contains("SetRelationshipProperty"), "{plan}");
    let plan = format!(
        "{:?}",
        db.query_with_params(
            &format!("EXPLAIN {REPAIR}"),
            &params(&[
                ("source", string("a")),
                ("target", string("b")),
                ("old", Value::Null),
            ]),
        )
        .unwrap()
        .rows
    );
    assert!(plan.contains("MatchSetExec"), "{plan}");
}
