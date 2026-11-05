// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A pattern whose later node is index-seekable starts there when the
//! statistics show it is clearly cheaper (#1485). These differential tests
//! compare, on randomized small graphs, the chosen plan, the plan with the
//! rule disabled (the previous text-order or bound-node start) and the same
//! pattern written from the other end (a forced start). The row multisets
//! must be identical.

use super::*;
use crate::optimizer::{
    OptimizerQualificationOptions, OptimizerRuleAssertion, OptimizerSearchDirective,
    OptionalOptimizerRule,
};

const RULE: OptionalOptimizerRule = OptionalOptimizerRule::CostBasedPatternStart;

struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

const PERSONS: u64 = 60;
const ORGANISATIONS: u64 = 30;
const PLACES: u64 = 12;

fn list(rows: Vec<BTreeMap<String, Value>>) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "rows".to_string(),
        Value::List(rows.into_iter().map(Value::Map).collect()),
    )])
}

fn pair(a: u64, b: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("a".to_string(), Value::Int(a as i64)),
        ("b".to_string(), Value::Int(b as i64)),
    ])
}

/// Persons, organisations and places with skewed, parallel and self-loop
/// relationships, so trails and relationship uniqueness matter.
fn random_graph(seed: u64) -> Database {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Person(id)").unwrap();
    db.query("CREATE INDEX ON :Place(name)").unwrap();
    let ids = |count: u64| {
        (0..count)
            .map(|id| BTreeMap::from([("id".to_string(), Value::Int(id as i64))]))
            .collect::<Vec<_>>()
    };
    for (label, count) in [("Person", PERSONS), ("Organisation", ORGANISATIONS)] {
        db.query_with_params(
            &format!("UNWIND $rows AS r CREATE (:{label} {{id: r.id}})"),
            &list(ids(count)),
        )
        .unwrap();
    }
    let places = (0..PLACES)
        .map(|id| {
            BTreeMap::from([
                ("id".to_string(), Value::Int(id as i64)),
                ("name".to_string(), Value::String(format!("P{id}"))),
            ])
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Place {id: r.id, name: r.name})",
        &list(places),
    )
    .unwrap();
    let mut knows = Vec::new();
    for person in 0..PERSONS {
        for _ in 0..rng.below(4) {
            // Skewed towards low ids; self-loops and parallel edges allowed.
            let spread = rng.below(PERSONS) + 1;
            let target = rng.below(spread);
            knows.push(pair(person, target));
        }
    }
    let mut work = Vec::new();
    for person in 0..PERSONS {
        for _ in 0..rng.below(3) {
            let mut row = pair(person, rng.below(ORGANISATIONS));
            row.insert("year".to_string(), Value::Int(2000 + rng.below(7) as i64));
            work.push(row);
        }
    }
    let located = (0..ORGANISATIONS)
        .map(|organisation| {
            let spread = rng.below(PLACES) + 1;
            pair(organisation, rng.below(spread))
        })
        .collect::<Vec<_>>();
    let lives = (0..PERSONS)
        .map(|person| pair(person, rng.below(PLACES)))
        .collect::<Vec<_>>();
    for (query, rows) in [
        (
            "UNWIND $rows AS r MATCH (a:Person {id: r.a}), (b:Person {id: r.b}) CREATE (a)-[:KNOWS]->(b)",
            knows,
        ),
        (
            "UNWIND $rows AS r MATCH (a:Person {id: r.a}), (b:Organisation {id: r.b}) \
             CREATE (a)-[:WORK_AT {year: r.year}]->(b)",
            work,
        ),
        (
            "UNWIND $rows AS r MATCH (a:Organisation {id: r.a}), (b:Place {id: r.b}) CREATE (a)-[:IS_LOCATED_IN]->(b)",
            located,
        ),
        (
            "UNWIND $rows AS r MATCH (a:Person {id: r.a}), (b:Place {id: r.b}) CREATE (a)-[:IS_LOCATED_IN]->(b)",
            lives,
        ),
    ] {
        db.query_with_params(query, &list(rows)).unwrap();
    }
    db
}

/// Rows as a sorted multiset; MATCH has no inherent row order.
fn multiset(rows: &crate::executor::QueryRows) -> Vec<String> {
    let mut rows = rows
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

/// Runs `query` once with every optional rule and once without the
/// cost-based start, on one snapshot. Returns both multisets and whether the
/// start moved.
fn qualified(
    db: &Database,
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> (Vec<String>, Vec<String>, bool) {
    let mut read = db.begin_read_transaction().unwrap();
    let mut run = |options| {
        let output = read
            .qualify_optimizer_query(
                crate::QueryRequest::new(query).with_params(parameters),
                options,
                OptimizerSearchDirective::Auto,
            )
            .unwrap();
        let applied = output
            .verify_rules(&[OptimizerRuleAssertion::Required(RULE)])
            .is_ok();
        (multiset(&output.result.unwrap().output.rows), applied)
    };
    let (chosen, applied) = run(OptimizerQualificationOptions::default());
    let (previous, disabled_applied) =
        run(OptimizerQualificationOptions::default().without_rule(RULE));
    assert!(!disabled_applied, "{query}");
    (chosen, previous, applied)
}

struct Case {
    name: &'static str,
    /// Written with the seekable node last, or with a bound node first.
    query: &'static str,
    /// The same pattern written from the other end: a forced start.
    forced: &'static str,
    /// Whether the start must move on every graph.
    moves: bool,
}

const CASES: &[Case] = &[
    Case {
        name: "expand chain",
        query: "MATCH (f:Person)-[w:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(p:Place {name: $name}) \
                RETURN f.id AS f, w.year AS year, c.id AS c, p.id AS p",
        forced: "MATCH (p:Place {name: $name})<-[:IS_LOCATED_IN]-(c:Organisation)<-[w:WORK_AT]-(f:Person) \
                 RETURN f.id AS f, w.year AS year, c.id AS c, p.id AS p",
        moves: true,
    },
    Case {
        name: "variable-length chain",
        query: "MATCH (f:Person)-[:KNOWS*0..2]-(g:Person)-[:IS_LOCATED_IN]->(:Place {name: $name}) \
                RETURN f.id AS f, g.id AS g",
        forced: "MATCH (:Place {name: $name})<-[:IS_LOCATED_IN]-(g:Person)-[:KNOWS*0..2]-(f:Person) \
                 RETURN f.id AS f, g.id AS g",
        moves: false,
    },
    Case {
        name: "repeated relationship type keeps relationship uniqueness",
        query: "MATCH (f:Person)-[:KNOWS]-(g:Person)-[:KNOWS]-(h:Person {id: $id}) \
                RETURN f.id AS f, g.id AS g, h.id AS h",
        forced: "MATCH (h:Person {id: $id})-[:KNOWS]-(g:Person)-[:KNOWS]-(f:Person) \
                 RETURN f.id AS f, g.id AS g, h.id AS h",
        moves: true,
    },
    Case {
        name: "cycle through a seekable middle node",
        query: "MATCH (a:Person)-[:KNOWS]->(b:Person {id: $id})-[:KNOWS]->(a) RETURN a.id AS a, b.id AS b",
        forced: "MATCH (b:Person {id: $id})-[:KNOWS]->(a:Person)-[:KNOWS]->(b) RETURN a.id AS a, b.id AS b",
        moves: true,
    },
    Case {
        name: "optional match pads when the seek finds nothing",
        query: "MATCH (x:Person {id: $id}) WITH x, x.id AS i WITH x \
                OPTIONAL MATCH (f:Person)-[:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(:Place {name: $name}) \
                RETURN x.id AS x, f.id AS f, c.id AS c",
        forced: "MATCH (x:Person {id: $id}) WITH x, x.id AS i WITH x \
                 OPTIONAL MATCH (:Place {name: $name})<-[:IS_LOCATED_IN]-(c:Organisation)<-[:WORK_AT]-(f:Person) \
                 RETURN x.id AS x, f.id AS f, c.id AS c",
        moves: true,
    },
    Case {
        name: "bound endpoint stays the start",
        query: "MATCH (f:Person) WITH f, f.id AS i WITH f \
                MATCH (f)-[:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(:Place {name: $name}) \
                RETURN f.id AS f, c.id AS c",
        forced: "MATCH (f:Person) WITH f, f.id AS i WITH f \
                 MATCH (:Place {name: $name})<-[:IS_LOCATED_IN]-(c:Organisation)<-[:WORK_AT]-(f) \
                 RETURN f.id AS f, c.id AS c",
        moves: false,
    },
    Case {
        // The guard and the division it protects both read `f`, so they run
        // in the same step after the start moves, and must keep their order:
        // otherwise `f.id % 3 = 0` rows would divide by zero.
        name: "guarded division keeps its guard first",
        query: "MATCH (f:Person)-[:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(p:Place {name: $name}) \
                WHERE f.id % 3 <> 0 AND c.id / (f.id % 3) >= 0 \
                RETURN f.id AS f, c.id AS c, p.id AS p",
        forced: "MATCH (p:Place {name: $name})<-[:IS_LOCATED_IN]-(c:Organisation)<-[:WORK_AT]-(f:Person) \
                 WHERE f.id % 3 <> 0 AND c.id / (f.id % 3) >= 0 \
                 RETURN f.id AS f, c.id AS c, p.id AS p",
        moves: true,
    },
];

#[test]
fn chosen_previous_and_forced_starts_return_identical_multisets() {
    let mut moved = BTreeMap::new();
    for seed in 1..=6 {
        let mut db = random_graph(seed);
        for case in CASES {
            for (id, place) in [(0, 0), (3, 1), (17, 5), (59, 11), (2, 99)] {
                let parameters = BTreeMap::from([
                    ("id".to_string(), Value::Int(id)),
                    ("name".to_string(), Value::String(format!("P{place}"))),
                ]);
                let (chosen, previous, applied) = qualified(&db, case.query, &parameters);
                let context = format!("{} seed={seed} id={id} place={place}", case.name);
                assert_eq!(chosen, previous, "{context}");
                let (forced, _, _) = qualified(&db, case.forced, &parameters);
                assert_eq!(chosen, forced, "{context}");
                // The ordinary cached path replays the chosen plan with each
                // parameter value.
                let cached = db.query_with_params(case.query, &parameters).unwrap();
                assert_eq!(multiset(&cached.rows), chosen, "{context}");
                if case.moves && !applied {
                    let explain = db
                        .explain_query_with_params(case.query, &parameters)
                        .unwrap();
                    panic!(
                        "{context}: start did not move\n{}\n{:?}",
                        explain.physical_plan.explain(0),
                        explain.trace.decisions
                    );
                }
                *moved.entry(case.name).or_insert(0) += usize::from(applied);
            }
        }
    }
    assert_eq!(moved["bound endpoint stays the start"], 0, "{moved:?}");
}

#[test]
fn explain_names_the_chosen_start_and_the_seek() {
    let db = random_graph(7);
    let parameters = BTreeMap::from([("name".to_string(), Value::String("P0".to_string()))]);
    let explain = db
        .explain_query_with_params(CASES[0].query, &parameters)
        .unwrap();
    let plan = explain.physical_plan.explain(0);
    assert!(
        plan.lines().last().is_some_and(|line| line
            .trim_start()
            .starts_with("IndexNodeSeek variable=p label=Place property=name")),
        "{plan}"
    );
    assert!(
        explain.trace.decisions.iter().any(|decision| decision.starts_with(
            "apply transformation:cost_based_pattern_start: start Expand chain at p with an index seek on Place.name"
        )),
        "{:?}",
        explain.trace.decisions
    );

    let parameters = BTreeMap::from([("id".to_string(), Value::Int(3))]);
    let explain = db
        .explain_query_with_params(CASES[2].query, &parameters)
        .unwrap();
    let plan = explain.physical_plan.explain(0);
    let graph_match = plan
        .lines()
        .find(|line| line.contains("GraphMatchExec"))
        .unwrap_or_else(|| panic!("{plan}"));
    assert!(
        graph_match.contains("steps=[Node(GraphMatchNode { variable: \"h\""),
        "{graph_match}"
    );
    assert!(
        explain.trace.decisions.iter().any(|decision| decision.starts_with(
            "apply transformation:cost_based_pattern_start: start GraphMatchExec path at h with an index seek on Person.id"
        )),
        "{:?}",
        explain.trace.decisions
    );
}

/// The seek inside a longer pattern reads the matching node, not the label.
#[test]
fn a_graph_match_seek_scans_no_nodes() {
    let mut db = random_graph(3);
    let parameters = BTreeMap::from([("id".to_string(), Value::Int(3))]);
    let before = hawdb_storage::store::node_scans_for_test();
    db.query_with_params(CASES[2].query, &parameters).unwrap();
    assert_eq!(hawdb_storage::store::node_scans_for_test(), before);
}

/// Statistics decide the start, and the plan cache follows them: the
/// cached plan is keyed by the statistics generation and the index-size
/// classes, so growing the place label past the small-label boundary misses
/// the cached text-order plan and re-plans it from the seek.
#[test]
fn a_statistics_change_replans_the_start() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Place(name)").unwrap();
    db.query("CREATE (:Person {id: 1})-[:IS_LOCATED_IN]->(:Place {name: 'P0'})")
        .unwrap();
    let query = "MATCH (f:Person)-[:IS_LOCATED_IN]->(p:Place {name: $name}) RETURN f.id AS f";
    let parameters = BTreeMap::from([("name".to_string(), Value::String("P0".to_string()))]);
    let decided = |db: &Database| {
        db.explain_query_with_params(query, &parameters)
            .unwrap()
            .trace
            .decisions
            .iter()
            .any(|decision| decision.starts_with("apply transformation:cost_based_pattern_start"))
    };
    // One person: its scan is as cheap as the seek.
    assert!(!decided(&db));
    for _ in 0..2 {
        let output = db.query_with_params(query, &parameters).unwrap();
        assert_eq!(multiset(&output.rows), vec!["f=Int(1)".to_string()]);
    }
    let persons = (0..200)
        .map(|id| {
            BTreeMap::from([
                ("id".to_string(), Value::Int(id + 2)),
                ("name".to_string(), Value::String(format!("Q{id}"))),
            ])
        })
        .collect::<Vec<_>>();
    for query in [
        "UNWIND $rows AS r CREATE (:Person {id: r.id})",
        "UNWIND $rows AS r CREATE (:Place {name: r.name})",
        "UNWIND $rows AS r MATCH (a:Person {id: r.id}), (b:Place {name: r.name}) CREATE (a)-[:IS_LOCATED_IN]->(b)",
    ] {
        db.query_with_params(query, &list(persons.clone())).unwrap();
    }
    let before = db.plan_cache_stats().unwrap();
    let output = db.query_with_params(query, &parameters).unwrap();
    assert_eq!(multiset(&output.rows), vec!["f=Int(1)".to_string()]);
    assert!(db.plan_cache_stats().unwrap().misses > before.misses);
    assert!(decided(&db));
}

#[test]
fn the_guarded_division_case_errors_without_its_guard() {
    // Keeps the guarded-division case meaningful: without the guard the same
    // pattern divides by zero, so a guard evaluated after the division
    // would fail the query.
    let mut db = random_graph(1);
    let unguarded =
        "MATCH (f:Person)-[:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(p:Place {name: $name}) \
                     WHERE c.id / (f.id % 3) >= 0 RETURN f.id AS f";
    let results = (0..PLACES)
        .map(|place| {
            let parameters =
                BTreeMap::from([("name".to_string(), Value::String(format!("P{place}")))]);
            db.query_with_params(unguarded, &parameters).is_err()
        })
        .collect::<Vec<_>>();
    assert!(results.iter().any(|failed| *failed), "{results:?}");
}
