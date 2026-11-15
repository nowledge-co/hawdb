// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Relationship CREATE and MERGE between nodes bound by a general read
//! (#1790): `IN` lists, pattern predicates, `UNWIND` rows and self-loops.
//! Expected effects and rows were checked against Ladybug.

use super::set_expressions::{column, on_every_surface, params, string};
use super::*;

const NODES: &[&str] = &[
    "CREATE (:Owner {id: 'o'})",
    "CREATE (:Item {id: 'a', name: 'Alpha'})",
    "CREATE (:Item {id: 'b', name: 'Be'})",
];
const LINKS: &str = "MATCH (o)-[r:HAS]->(i) RETURN o.id AS source, i.id AS target, \
    r.w AS w, r.k AS k ORDER BY source, target, w, k";

fn ids(values: &[&str]) -> Value {
    Value::List(values.iter().map(|value| string(value)).collect())
}

fn links(rows: &[BTreeMap<String, Value>]) -> Vec<(Value, Value, Value, Value)> {
    rows.iter()
        .map(|row| {
            (
                row["source"].clone(),
                row["target"].clone(),
                row["w"].clone(),
                row["k"].clone(),
            )
        })
        .collect()
}

fn link(source: &str, target: &str, w: Value, k: Value) -> (Value, Value, Value, Value) {
    (string(source), string(target), w, k)
}

/// `IN $list` selects the targets: one relationship per matched node, a
/// missing or repeated id adds nothing, and a second run merges every one.
#[test]
fn merge_from_an_in_list_creates_once_per_matched_pair() {
    let statement = "MATCH (o:Owner {id: $owner}), (i:Item) WHERE i.id IN $items \
        MERGE (o)-[r:HAS]->(i) ON CREATE SET r.w = 1.0, r.k = $key";
    let parameters = params(&[
        ("owner", string("o")),
        ("items", ids(&["a", "missing", "b", "a"])),
        ("key", string("k1")),
    ]);
    let expected = vec![
        link("o", "a", Value::Float(1.0), string("k1")),
        link("o", "b", Value::Float(1.0), string("k1")),
    ];
    on_every_surface(
        NODES,
        statement,
        &parameters,
        LINKS,
        |surface, _, stored| {
            assert_eq!(links(&stored), expected, "{surface}");
        },
    );
    let mut setup = NODES.to_vec();
    let first_run = "MATCH (o:Owner {id: 'o'}), (i:Item) WHERE i.id IN ['a'] \
        MERGE (o)-[r:HAS]->(i) ON CREATE SET r.w = 2.0, r.k = 'old'";
    setup.push(first_run);
    // The existing relationship is reused: ON CREATE does not run again.
    on_every_surface(
        &setup,
        statement,
        &parameters,
        LINKS,
        |surface, _, stored| {
            assert_eq!(
                links(&stored),
                vec![
                    link("o", "a", Value::Float(2.0), string("old")),
                    link("o", "b", Value::Float(1.0), string("k1")),
                ],
                "{surface}"
            );
        },
    );
}

/// `UNWIND` rows drive a CREATE: one relationship and one returned row per
/// matched row, with property values read from the row.
#[test]
fn create_from_unwind_rows_returns_one_row_per_created_relationship() {
    on_every_surface(
        NODES,
        "MATCH (o:Owner {id: $owner}) UNWIND $items AS item MATCH (i:Item {id: item}) \
         CREATE (o)-[:HAS {w: $w, k: item}]->(i) RETURN i.id AS id",
        &params(&[
            ("owner", string("o")),
            ("items", ids(&["a", "b", "missing", "a"])),
            ("w", Value::Float(3.0)),
        ]),
        LINKS,
        |surface, returned, stored| {
            let mut returned = column(&returned, "id");
            returned.sort();
            assert_eq!(
                returned,
                [string("a"), string("a"), string("b")],
                "{surface}"
            );
            assert_eq!(
                links(&stored),
                vec![
                    link("o", "a", Value::Float(3.0), string("a")),
                    link("o", "a", Value::Float(3.0), string("a")),
                    link("o", "b", Value::Float(3.0), string("b")),
                ],
                "{surface}"
            );
        },
    );
    // A property read from a matched node, and an incoming pattern.
    on_every_surface(
        NODES,
        "MATCH (o:Owner {id: 'o'}), (i:Item) CREATE (i)<-[:HAS {w: 5, k: i.name}]-(o)",
        &BTreeMap::new(),
        LINKS,
        |surface, _, stored| {
            assert_eq!(
                links(&stored),
                vec![
                    link("o", "a", Value::Int(5), string("Alpha")),
                    link("o", "b", Value::Int(5), string("Be")),
                ],
                "{surface}"
            );
        },
    );
}

/// Later rows see the relationships earlier rows merged.
#[test]
fn merge_from_unwind_rows_reuses_a_relationship_an_earlier_row_created() {
    on_every_surface(
        NODES,
        "UNWIND $items AS item MATCH (o:Owner {id: 'o'}), (i:Item {id: item}) \
         MERGE (o)-[r:HAS {k: 'u'}]->(i) RETURN i.id AS id",
        &params(&[("items", ids(&["a", "a"]))]),
        LINKS,
        |surface, returned, stored| {
            assert_eq!(
                column(&returned, "id"),
                [string("a"), string("a")],
                "{surface}"
            );
            assert_eq!(
                links(&stored),
                vec![link("o", "a", Value::Null, string("u"))],
                "{surface}"
            );
        },
    );
}

/// `NOT EXISTS { ... }` guards the CREATE, so a repeated statement is a no-op.
#[test]
fn create_guarded_by_a_pattern_predicate_runs_once() {
    let statement = "MATCH (o:Owner {id: $owner}), (i:Item {id: $item}) \
        WHERE NOT EXISTS { MATCH (o)-[:HAS]->(i) } CREATE (o)-[:HAS {k: $key}]->(i)";
    let parameters = params(&[
        ("owner", string("o")),
        ("item", string("a")),
        ("key", string("first")),
    ]);
    let expected = vec![link("o", "a", Value::Null, string("first"))];
    on_every_surface(
        NODES,
        statement,
        &parameters,
        LINKS,
        |surface, _, stored| {
            assert_eq!(links(&stored), expected, "{surface}");
        },
    );
    let mut db = Database::new();
    for query in NODES {
        db.query(query).unwrap();
    }
    db.query_with_params(statement, &parameters).unwrap();
    let epoch = db.commit_epoch().unwrap();
    db.query_with_params(statement, &parameters).unwrap();
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    let stored = db.query(LINKS).unwrap().rows.into_rows();
    assert_eq!(links(&stored), expected);
}

/// A node at both ends of the pattern; MERGE reuses the loop. Ladybug's
/// self-loop MERGE neither returns nor creates a row, which HawDB does not
/// reproduce.
#[test]
fn self_loops_create_and_merge() {
    let read = "MATCH (a)-[r:LINK]->(b) RETURN a.id AS a, b.id AS b, r.n AS n ORDER BY n";
    on_every_surface(
        NODES,
        "MATCH (i:Item {id: 'a'}) CREATE (i)-[:LINK {n: 1}]->(i)",
        &BTreeMap::new(),
        read,
        |surface, _, stored| {
            assert_eq!(column(&stored, "a"), [string("a")], "{surface}");
            assert_eq!(column(&stored, "b"), [string("a")], "{surface}");
        },
    );
    let mut db = Database::new();
    for query in NODES {
        db.query(query).unwrap();
    }
    for _ in 0..2 {
        db.query("MATCH (i:Item {id: 'b'}) MERGE (i)-[l:LINK {n: 9}]->(i)")
            .unwrap();
    }
    let rows = db.query(read).unwrap().rows.into_rows();
    assert_eq!(column(&rows, "n"), [Value::Int(9)]);
    assert_eq!(column(&rows, "a"), [string("b")]);
}

#[test]
fn refusals_write_nothing() {
    let mut db = Database::new_with_config(DatabaseConfig {
        mutation_limits: crate::store::MutationLimits {
            max_affected_rows: NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        },
        ..Default::default()
    });
    for query in NODES {
        db.query(query).unwrap();
    }
    for statement in [
        // Two matched rows exceed the one-row budget.
        "MATCH (o:Owner), (i:Item) WHERE i.id IN ['a', 'b'] CREATE (o)-[:HAS]->(i)",
        // A type error in any row.
        "MATCH (o:Owner), (i:Item) WHERE i.id IN ['a'] CREATE (o)-[:HAS {w: i.name + 1}]->(i)",
        // The created relationship is not readable by RETURN.
        "MATCH (o:Owner), (i:Item) WHERE i.id IN ['a'] CREATE (o)-[r:HAS]->(i) RETURN r",
        // ON MATCH SET is not evaluated over rows yet.
        "MATCH (o:Owner), (i:Item) WHERE i.id IN ['a'] MERGE (o)-[r:HAS]->(i) \
         ON MATCH SET r.k = 'm'",
        // New endpoint nodes are not created from rows.
        "MATCH (o:Owner) WHERE o.id IN ['o'] CREATE (o)-[:HAS]->(:Item {id: 'new'})",
    ] {
        assert!(db.query(statement).is_err(), "{statement}");
        assert!(db.query(LINKS).unwrap().rows.is_empty(), "{statement}");
    }
    assert_eq!(
        db.query("MATCH (i:Item) RETURN count(i) AS n")
            .unwrap()
            .rows[0]
            .get("n"),
        Some(&Value::Int(2))
    );
}

#[test]
fn relationship_constraints_and_explain() {
    let mut db = Database::new();
    for query in NODES {
        db.query(query).unwrap();
    }
    db.query("CREATE CONSTRAINT ON -[:HAS(k)]-> ASSERT UNIQUE")
        .unwrap();
    let error = db
        .query(
            "MATCH (o:Owner), (i:Item) WHERE i.id IN ['a', 'b'] CREATE (o)-[:HAS {k: 'same'}]->(i)",
        )
        .unwrap_err();
    assert!(
        error.to_string().to_lowercase().contains("unique"),
        "{error}"
    );
    assert!(db.query(LINKS).unwrap().rows.is_empty());

    let output = db
        .query("EXPLAIN MATCH (o:Owner), (i:Item) WHERE i.id IN ['a'] CREATE (o)-[:HAS]->(i)")
        .unwrap();
    let plan = format!("{:?}", output.rows);
    assert!(plan.contains("MatchSetExec"), "{plan}");
    assert!(plan.contains("create=(o)-[:HAS]->(i)"), "{plan}");
    assert!(db.query(LINKS).unwrap().rows.is_empty());
}
