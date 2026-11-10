// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Grouped graph variables carried through `WITH` as graph slots (#1615),
//! through the embedded API with the optimizer, projected match reads and
//! out-of-core canonical reads. Each query runs on a fresh database whose
//! plans carry slots and on one planned with the map columns used before;
//! both must return the same rows. Executor-level differential tests also
//! compare identity grouping with map grouping and forced spills.

use super::*;

/// A deterministic xorshift generator; the graphs only need variety.
struct Random(u64);

impl Random {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

/// Persons with small property domains, so different persons often have
/// equal properties, some properties missing and a long `bio` that projected
/// reads skip; KNOWS and LIKES relationships with optional weights.
fn statements(seed: u64) -> Vec<String> {
    let mut random = Random(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let persons = 12 + random.below(12);
    let mut statements = Vec::new();
    for id in 0..persons {
        let mut properties = vec![format!("id: {id}"), format!("bio: '{}'", "b".repeat(200))];
        if random.below(4) != 0 {
            properties.push(format!(
                "name: '{}'",
                ["x", "y", "z"][random.below(3) as usize]
            ));
        }
        if random.below(3) != 0 {
            properties.push(format!("age: {}", random.below(4)));
        }
        if random.below(3) == 0 {
            properties.push("admin: true".to_string());
        }
        statements.push(format!("CREATE (:Person {{{}}})", properties.join(", ")));
    }
    for _ in 0..persons * 3 {
        let (source, target) = (random.below(persons), random.below(persons));
        let rel_type = if random.below(3) == 0 {
            "LIKES"
        } else {
            "KNOWS"
        };
        let weight = match random.below(3) {
            0 => String::new(),
            weight => format!(" {{weight: {weight}}}"),
        };
        statements.push(format!(
            "MATCH (a:Person {{id: {source}}}), (b:Person {{id: {target}}}) \
             CREATE (a)-[:{rel_type}{weight}]->(b)"
        ));
    }
    statements
}

/// The same graph resident in memory, or checkpointed and reopened out of core.
fn open(statements: &[String], out_of_core: bool) -> (Database, Option<std::path::PathBuf>) {
    if !out_of_core {
        let mut db = Database::new();
        for statement in statements {
            db.query(statement).unwrap();
        }
        return (db, None);
    }
    static NEXT_STORE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = NEXT_STORE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = unique_test_dir(&format!(
        "identity_group_keys_{}_{sequence}",
        std::process::id()
    ));
    {
        let mut db = Database::open(&path).unwrap();
        for statement in statements {
            db.query(statement).unwrap();
        }
        db.checkpoint().unwrap();
    }
    let db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    (db, Some(path))
}

/// Queries, whether they order totally, and the slot mode their plan shows.
const QUERIES: &[(&str, bool, Option<&str>)] = &[
    (
        "MATCH (a:Person)-[:KNOWS]->(b) WITH a, count(*) AS c RETURN a.name AS name, a.age AS age, c",
        false,
        Some("graph_keys=slots"),
    ),
    (
        "MATCH (a:Person)-[r:LIKES]->(b) WITH r, count(*) AS c RETURN r.weight AS w, id(r) AS i, c",
        false,
        Some("graph_keys=slots"),
    ),
    (
        "MATCH (a:Person)-[r]->(b) WITH a, b.name AS bn, count(*) AS c RETURN a.id AS ai, a.age AS age, bn, c",
        false,
        Some("graph_keys=slots"),
    ),
    (
        "MATCH (a:Person {admin: true}) OPTIONAL MATCH (a)-[:LIKES]->(b:Person) WITH a, b, count(*) AS c RETURN a.id AS ai, b.name AS bn, b IS NULL AS missing, c",
        false,
        Some("graph_keys=slots"),
    ),
    (
        "MATCH (a:Person)-[:KNOWS]->(b) WITH DISTINCT a, b MATCH (b)-[:LIKES]->(x) RETURN a.name AS an, x.age AS xa, x.id AS xi",
        false,
        Some("graph_keys=distinct_slots"),
    ),
    (
        "MATCH (a:Person)-[:KNOWS*1..2]-(f) WHERE NOT a = f WITH DISTINCT f RETURN f.id AS fi, f.name AS fn",
        false,
        Some("graph_keys=distinct_slots"),
    ),
    (
        "MATCH (a:Person)-[:KNOWS]->(b) WITH a, collect(b) AS bs, count(*) AS c ORDER BY c DESC, a.id LIMIT 5 RETURN a.id AS ai, a.bio AS bio, size(bs) AS n, c",
        true,
        Some("graph_keys=slots"),
    ),
    (
        "MATCH (a:Person)-[:KNOWS]->(b) WITH a, count(*) AS c WHERE c > 1 MATCH (a)-[:LIKES]->(x) RETURN a.name AS an, x.id AS xi, c",
        false,
        Some("graph_keys=slots"),
    ),
    (
        "MATCH (a:Person)-[:KNOWS]->(b) WITH a, count(*) AS c RETURN a.id AS ai, size([(a)-[:LIKES]->(x) | x.id]) AS likes, c",
        false,
        Some("graph_keys=slots"),
    ),
    (
        "MATCH (a:Person)-[:KNOWS]->(b) RETURN a, count(*) AS c",
        false,
        None,
    ),
    (
        "MATCH (a:Person)-[:KNOWS]->(b) WITH DISTINCT a, count(*) AS c RETURN a.id AS ai, c",
        false,
        None,
    ),
];

fn rows(db: &mut Database, query: &str) -> Vec<String> {
    let output = db
        .query(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
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

#[test]
fn slotted_groupings_return_the_rows_of_map_column_plans() {
    for seed in 1..=2u64 {
        let statements = statements(seed);
        for out_of_core in [false, true] {
            for (query, ordered, mode) in QUERIES {
                let context = format!("seed {seed}, out_of_core {out_of_core}: {query}");
                let (mut current, current_path) = open(&statements, out_of_core);
                let (mut legacy, legacy_path) = open(&statements, out_of_core);
                let plan = current
                    .explain_query(query)
                    .unwrap()
                    .physical_plan
                    .explain(0);
                match mode {
                    Some(mode) => assert!(plan.contains(mode), "{context}:\n{plan}"),
                    None => assert!(!plan.contains("graph_keys="), "{context}:\n{plan}"),
                }
                let mut actual = rows(&mut current, query);
                let mut expected = hawdb_plan_cypher::with_legacy_graph_group_keys(|| {
                    let plan = legacy
                        .explain_query(query)
                        .unwrap()
                        .physical_plan
                        .explain(0);
                    assert!(!plan.contains("graph_keys="), "{context}:\n{plan}");
                    rows(&mut legacy, query)
                });
                if !ordered {
                    actual.sort();
                    expected.sort();
                }
                assert!(!expected.is_empty(), "{context}");
                assert_eq!(actual, expected, "{context}");
                drop((current, legacy));
                for path in [current_path, legacy_path].into_iter().flatten() {
                    std::fs::remove_dir_all(path).unwrap();
                }
            }
        }
    }
}

/// Reads after a grouping see the carried record: the grouped variable
/// reads every property, so a later property access needs no new read.
#[test]
fn carried_records_hold_every_property_later_clauses_read() {
    let mut db = Database::new();
    for statement in statements(9) {
        db.query(&statement).unwrap();
    }
    let query = "MATCH (a:Person)-[:KNOWS]->(b) WITH a, count(*) AS c \
                 RETURN a.bio AS bio, a.name AS name, c";
    let plan = db.explain_query(query).unwrap().physical_plan.explain(0);
    // No import re-reads the grouped variable after the grouping.
    let after_grouping = plan.split("AggregateExec").next().unwrap();
    assert!(!after_grouping.contains("GraphMatchExec"), "{plan}");
    let output = rows(&mut db, query);
    assert!(!output.is_empty());
    assert!(
        output
            .iter()
            .all(|row| row.contains(&format!("bio=String(\"{}\")", "b".repeat(200)))),
        "{output:?}"
    );
}

/// The mutation binder accepts no grouping after a write, so a statement
/// cannot observe records of one entity that differ between rows; the
/// executor pins identity grouping for such rows (see
/// `blocking::aggregate::identity_tests`).
#[test]
fn writes_cannot_feed_a_grouping_in_the_same_statement() {
    let mut db = Database::new();
    db.query("CREATE (:Person {id: 1})").unwrap();
    for query in [
        "MATCH (a:Person) SET a.v = 1 WITH a, count(*) AS c RETURN c",
        "MATCH (a:Person) SET a.v = 1 RETURN a, count(*) AS c",
        "MATCH (a:Person) SET a.v = 1 WITH DISTINCT a RETURN a.v AS v",
    ] {
        assert!(db.query(query).is_err(), "{query}");
    }
}
