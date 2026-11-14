// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A correlated MATCH whose only import is a node can run as a hash join at
//! that meeting node, and `node IN collected_list` as an id-set semi-join
//! (#1485). These differential tests compare, on randomized small graphs,
//! the chosen plan with the plan that has both rules disabled (the per-row
//! nested loop and the list scan), and with the ordinary cached plan. Row
//! multisets, including duplicates, and errors must be identical.

use super::*;
use crate::optimizer::{
    OptimizerQualificationOptions, OptimizerRuleAssertion, OptimizerSearchDirective,
    OptionalOptimizerRule,
};

const HASH_JOIN: OptionalOptimizerRule = OptionalOptimizerRule::GraphMatchHashJoin;
const ID_SET: OptionalOptimizerRule = OptionalOptimizerRule::GraphMatchIdSetSemiJoin;

struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }

    /// Skewed towards low values.
    fn skewed(&mut self, bound: u64) -> u64 {
        let spread = self.below(bound) + 1;
        self.below(spread)
    }
}

const PERSONS: u64 = 150;
const ORGANISATIONS: u64 = 60;
const PLACES: u64 = 15;
const FORUMS: u64 = 40;
const POSTS: u64 = 300;

fn list(rows: Vec<BTreeMap<String, Value>>) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "rows".to_string(),
        Value::List(rows.into_iter().map(Value::Map).collect()),
    )])
}

fn row(pairs: &[(&str, i64)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(name, value)| (name.to_string(), Value::Int(*value)))
        .collect()
}

/// An LDBC-like graph with skewed degrees, parallel relationships and
/// self-loops, so trails, multiplicity and relationship uniqueness matter.
fn random_graph(seed: u64) -> Database {
    random_graph_with(seed, DatabaseConfig::default())
}

fn random_graph_with(seed: u64, config: DatabaseConfig) -> Database {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut db = Database::new_with_config(config);
    for index in [
        ":Person(id)",
        ":Place(name)",
        ":Organisation(id)",
        ":Forum(id)",
        ":Post(id)",
    ] {
        db.query(&format!("CREATE INDEX ON {index}")).unwrap();
    }
    let ids = |count: u64| (0..count).map(|id| row(&[("id", id as i64)])).collect();
    for (label, count) in [
        ("Person", PERSONS),
        ("Organisation", ORGANISATIONS),
        ("Forum", FORUMS),
        ("Post", POSTS),
    ] {
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
    let mut work = Vec::new();
    let mut lives = Vec::new();
    for person in 0..PERSONS as i64 {
        for _ in 0..rng.below(10) {
            // Self-loops and parallel edges are allowed.
            knows.push(row(&[("a", person), ("b", rng.skewed(PERSONS) as i64)]));
        }
        for _ in 0..rng.below(3) {
            work.push(row(&[
                ("a", person),
                ("b", rng.below(ORGANISATIONS) as i64),
                ("year", 2000 + rng.below(8) as i64),
            ]));
        }
        lives.push(row(&[("a", person), ("b", rng.below(PLACES) as i64)]));
    }
    let located = (0..ORGANISATIONS as i64)
        .map(|organisation| row(&[("a", organisation), ("b", rng.skewed(PLACES) as i64)]))
        .collect::<Vec<_>>();
    let mut members = Vec::new();
    for forum in 0..FORUMS as i64 {
        for _ in 0..1 + rng.below(8) {
            members.push(row(&[
                ("a", forum),
                ("b", rng.skewed(PERSONS) as i64),
                ("day", rng.below(100) as i64),
            ]));
        }
    }
    let mut contains = Vec::new();
    let mut creators = Vec::new();
    for post in 0..POSTS as i64 {
        contains.push(row(&[("a", rng.skewed(FORUMS) as i64), ("b", post)]));
        creators.push(row(&[("a", post), ("b", rng.skewed(PERSONS) as i64)]));
    }
    for (query, rows) in [
        ("MATCH (a:Person {id: r.a}), (b:Person {id: r.b}) CREATE (a)-[:KNOWS]->(b)", knows),
        (
            "MATCH (a:Person {id: r.a}), (b:Organisation {id: r.b}) CREATE (a)-[:WORK_AT {year: r.year}]->(b)",
            work,
        ),
        ("MATCH (a:Person {id: r.a}), (b:Place {id: r.b}) CREATE (a)-[:IS_LOCATED_IN]->(b)", lives),
        (
            "MATCH (a:Organisation {id: r.a}), (b:Place {id: r.b}) CREATE (a)-[:IS_LOCATED_IN]->(b)",
            located,
        ),
        (
            "MATCH (a:Forum {id: r.a}), (b:Person {id: r.b}) CREATE (a)-[:HAS_MEMBER {day: r.day}]->(b)",
            members,
        ),
        ("MATCH (a:Forum {id: r.a}), (b:Post {id: r.b}) CREATE (a)-[:CONTAINER_OF]->(b)", contains),
        ("MATCH (a:Post {id: r.a}), (b:Person {id: r.b}) CREATE (a)-[:HAS_CREATOR]->(b)", creators),
    ] {
        db.query_with_params(&format!("UNWIND $rows AS r {query}"), &list(rows))
            .unwrap();
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

type Outcome = std::result::Result<Vec<String>, String>;

/// The query with every optional rule, and with both #1485 part-2 rules
/// disabled, on one snapshot, and which of the two rules applied.
fn qualified(
    db: &Database,
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> (Outcome, Outcome, [bool; 2]) {
    let mut read = db.begin_read_transaction().unwrap();
    let mut run = |options| {
        let output = read
            .qualify_optimizer_query(
                crate::QueryRequest::new(query).with_params(parameters),
                options,
                OptimizerSearchDirective::Auto,
            )
            .unwrap();
        let applied = [HASH_JOIN, ID_SET].map(|rule| {
            output
                .verify_rules(&[OptimizerRuleAssertion::Required(rule)])
                .is_ok()
        });
        let outcome = output
            .result
            .map(|result| multiset(&result.output.rows))
            .map_err(|error| error.to_string());
        (outcome, applied)
    };
    let (chosen, applied) = run(OptimizerQualificationOptions::default());
    let (previous, disabled) = run(OptimizerQualificationOptions::default()
        .without_rule(HASH_JOIN)
        .without_rule(ID_SET));
    assert_eq!(disabled, [false, false], "{query}");
    (chosen, previous, applied)
}

struct Case {
    name: &'static str,
    query: &'static str,
    /// The rules that must apply on every graph: hash join, id set.
    requires: [bool; 2],
}

const IC11_SINGLE: &str = "MATCH (a:Person {id: $id})-[:KNOWS*1..2]-(f:Person)-[w:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(p:Place {name: $name}) \
     WHERE NOT a = f AND w.year < $year RETURN f.id AS f, c.id AS c, w.year AS year";

const CASES: &[Case] = &[
    Case {
        name: "IC11 single MATCH keeps one row per trail",
        query: IC11_SINGLE,
        requires: [false, false],
    },
    Case {
        name: "IC11 LDBC form over distinct friends",
        query: "MATCH (a:Person {id: $id})-[:KNOWS*1..2]-(f:Person) WHERE NOT a = f WITH DISTINCT f \
                MATCH (f)-[w:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(:Place {name: $name}) \
                WHERE w.year < $year RETURN f.id AS f, c.id AS c, w.year AS year",
        requires: [false, false],
    },
    Case {
        name: "duplicate meeting nodes keep their multiplicity",
        query: "MATCH (f:Person)-[:KNOWS]-(g:Person) WITH f \
                MATCH (f)-[w:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(:Place {name: $name}) \
                RETURN f.id AS f, c.id AS c, w.year AS year",
        requires: [false, false],
    },
    Case {
        name: "optional match pads, including with an empty build",
        query: "MATCH (f:Person) WITH f \
                OPTIONAL MATCH (f)-[w:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(:Place {name: $name}) \
                WHERE w.year < $year RETURN f.id AS f, c.id AS c, w.year AS year",
        requires: [false, false],
    },
    Case {
        name: "a NULL meeting node joins nothing",
        query: "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(f:Person) WITH f \
                OPTIONAL MATCH (f)-[:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(:Place {name: $name}) \
                RETURN f.id AS f, c.id AS c",
        requires: [false, false],
    },
    Case {
        name: "repeated relationship type keeps uniqueness inside the build",
        query: "MATCH (f:Person) WITH f \
                MATCH (f)-[:KNOWS]-(g:Person)-[:KNOWS]-(h:Person {id: $other}) \
                RETURN f.id AS f, g.id AS g, h.id AS h",
        requires: [false, false],
    },
    Case {
        name: "residual conjuncts read the input row and may fail",
        query: "MATCH (f:Person)-[:KNOWS]-(:Person) WITH f, f.id % 4 AS bucket \
                MATCH (f)-[w:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(:Place {name: $name}) \
                WHERE w.year < $year AND bucket <> 0 AND c.id / bucket >= 0 \
                RETURN f.id AS f, c.id AS c",
        requires: [false, false],
    },
    Case {
        name: "an unguarded residual division fails in both plans",
        query: "MATCH (f:Person)-[:KNOWS]-(:Person) WITH f, f.id % 4 AS bucket \
                MATCH (f)-[w:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(:Place {name: $name}) \
                WHERE c.id / bucket >= 0 RETURN f.id AS f, c.id AS c",
        requires: [false, false],
    },
    Case {
        // The per-row program places `bucket <> 0` at step 0, before the
        // division that follows it in the text; the join keeps that order.
        name: "an input-row guard written after the division still guards it",
        query: "MATCH (f:Person)-[:KNOWS]-(:Person) WITH f, f.id % 4 AS bucket \
                MATCH (f)-[w:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(:Place {name: $name}) \
                WHERE c.id / bucket >= 0 AND bucket <> 0 RETURN f.id AS f, c.id AS c",
        requires: [false, false],
    },
    Case {
        // Node 987654321 is not in the snapshot: both plans fail.
        name: "an imported node missing from the snapshot fails",
        query: "MATCH (p:Person) WITH collect(p) + [{_id: 987654321, labels: ['Person']}] AS people \
                MATCH (:Organisation) UNWIND people AS f \
                MATCH (f:Person)-[w:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(:Place {name: $name}) \
                RETURN f.id AS f, c.id AS c",
        requires: [false, false],
    },
    Case {
        name: "IC5 collected friends",
        query: "MATCH (a:Person {id: $id})-[:KNOWS*1..2]-(friend:Person) WHERE NOT a = friend \
                WITH DISTINCT friend MATCH (friend)<-[m:HAS_MEMBER]-(forum:Forum) WHERE m.day > 20 \
                WITH forum, collect(friend) AS friends \
                OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) WHERE friend IN friends \
                RETURN forum.id AS forum, count(post) AS posts",
        requires: [false, true],
    },
    Case {
        name: "NULLs and duplicates in the collected list",
        query: "MATCH (forum:Forum)-[:HAS_MEMBER]->(friend:Person) \
                WITH forum, collect(friend) + [null] + collect(friend) AS friends \
                OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) WHERE friend IN friends \
                RETURN forum.id AS forum, post.id AS post",
        requires: [false, true],
    },
    Case {
        name: "a NULL or empty list matches nothing",
        query: "MATCH (forum:Forum) WITH forum, CASE WHEN forum.id % 2 = 0 THEN null ELSE [] END AS friends \
                OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) WHERE friend IN friends \
                RETURN forum.id AS forum, post.id AS post",
        requires: [false, true],
    },
    Case {
        name: "a list of plain values never equals a node",
        query: "MATCH (forum:Forum) WITH forum, [forum.id, 1, 'x'] AS friends \
                OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) WHERE friend IN friends \
                RETURN forum.id AS forum, post.id AS post",
        requires: [false, true],
    },
    Case {
        // With a NULL element, `friend IN friends` is NULL for a creator
        // outside the list, so the division after it still runs.
        name: "a NULL membership does not skip a later failing conjunct",
        query: "MATCH (forum:Forum)-[:HAS_MEMBER]->(friend:Person) \
                WITH forum, collect(friend) + [null] AS friends \
                OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) \
                WHERE friend IN friends AND 1 / (friend.id % 7) >= 0 \
                RETURN forum.id AS forum, post.id AS post",
        requires: [false, true],
    },
    Case {
        // Without one it is false, which stops the conjunction first.
        name: "a false membership stops before a later failing conjunct",
        query: "MATCH (forum:Forum)-[:HAS_MEMBER]->(friend:Person) \
                WITH forum, collect(friend) AS friends \
                OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) \
                WHERE friend IN friends AND 1 / (friend.id % 7) >= 0 \
                RETURN forum.id AS forum, post.id AS post",
        requires: [false, true],
    },
    Case {
        name: "an earlier membership over a non-list keeps its error",
        query: "MATCH (forum:Forum)-[:HAS_MEMBER]->(friend:Person) \
                WITH forum, forum.id AS bad, collect(friend) AS friends \
                OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) \
                WHERE friend IN bad AND friend IN friends \
                RETURN forum.id AS forum, post.id AS post",
        requires: [false, true],
    },
    Case {
        name: "a non-list column keeps its error",
        query: "MATCH (forum:Forum) WITH forum, forum.id AS friends \
                OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) WHERE friend IN friends \
                RETURN forum.id AS forum, post.id AS post",
        requires: [false, true],
    },
];

#[test]
fn chosen_and_nested_loop_plans_return_identical_multisets() {
    let mut applied_somewhere = BTreeMap::new();
    for seed in 1..=5 {
        let mut db = random_graph(seed);
        for case in CASES {
            for (id, other, place, year) in [
                (0, 3, 0, 2004),
                (5, 0, 1, 2008),
                (37, 2, 3, 2002),
                (9, 9, 99, 2010),
            ] {
                let parameters = BTreeMap::from([
                    ("id".to_string(), Value::Int(id)),
                    ("other".to_string(), Value::Int(other)),
                    ("name".to_string(), Value::String(format!("P{place}"))),
                    ("year".to_string(), Value::Int(year)),
                ]);
                let (chosen, previous, applied) = qualified(&db, case.query, &parameters);
                let context = format!("{} seed={seed} id={id} place={place}", case.name);
                assert_eq!(chosen, previous, "{context}");
                let cached = db
                    .query_with_params(case.query, &parameters)
                    .map(|output| multiset(&output.rows))
                    .map_err(|error| error.to_string());
                assert_eq!(cached, chosen, "{context}");
                for (rule, (required, applied)) in case.requires.iter().zip(applied).enumerate() {
                    if *required && !applied {
                        let explain = db
                            .explain_query_with_params(case.query, &parameters)
                            .unwrap();
                        panic!(
                            "{context}: rule {rule} did not apply\n{}\n{:#?}",
                            explain.physical_plan.explain(0),
                            explain.trace.decisions
                        );
                    }
                    *applied_somewhere.entry((case.name, rule)).or_insert(0) +=
                        usize::from(applied);
                }
            }
        }
    }
    // Every hash join case runs as one on some graph, so the differential
    // compares two different plans there.
    for case in CASES
        .iter()
        .take_while(|case| case.name != "IC5 collected friends")
        .filter(|case| case.name != "IC11 LDBC form over distinct friends")
    {
        if applied_somewhere[&(case.name, 0)] == 0 {
            let db = random_graph(1);
            let parameters = BTreeMap::from([
                ("id".to_string(), Value::Int(5)),
                ("other".to_string(), Value::Int(0)),
                ("name".to_string(), Value::String("P1".to_string())),
                ("year".to_string(), Value::Int(2008)),
            ]);
            let explain = db
                .explain_query_with_params(case.query, &parameters)
                .unwrap();
            panic!(
                "{}: never a hash join\n{}\n{:#?}",
                case.name,
                explain.physical_plan.explain(0),
                explain
                    .trace
                    .decisions
                    .iter()
                    .filter(
                        |decision| decision.contains("hash_join") || decision.contains("estimate")
                    )
                    .collect::<Vec<_>>()
            );
        }
    }
}

const LDBC_SCAN: &str = "MATCH (f:Person) WITH f \
     MATCH (f)-[w:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(:Place {name: $name}) \
     RETURN f.id AS f, c.id AS c, w.year AS year";

fn place(name: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("name".to_string(), Value::String(name.to_string()))])
}

#[test]
fn explain_names_the_build_start_the_key_and_the_id_set() {
    let db = random_graph(2);
    let explain = db
        .explain_query_with_params(LDBC_SCAN, &place("P0"))
        .unwrap();
    let plan = explain.physical_plan.explain(0);
    let graph_match = plan
        .lines()
        .find(|line| line.contains("GraphMatchExec"))
        .unwrap_or_else(|| panic!("{plan}"));
    assert!(
        graph_match
            .contains("steps=[Node(GraphMatchNode { variable: \"__anon0\", label: \"Place\"")
            && graph_match.contains("hash_join=key:f build_conjuncts=[]"),
        "{graph_match}"
    );
    assert!(
        explain.trace.decisions.iter().any(|decision| decision.starts_with(
            "apply transformation:graph_match_hash_join: run GraphMatchExec once from __anon0 and hash its matches on f"
        )),
        "{:#?}",
        explain.trace.decisions
    );
    let mut read = db.begin_read_transaction().unwrap();
    let fingerprint = |read: &mut crate::DatabaseReadTransaction<_>, options| {
        read.qualify_optimizer_query(
            crate::QueryRequest::new(LDBC_SCAN).with_params(&place("P0")),
            options,
            OptimizerSearchDirective::Auto,
        )
        .unwrap()
        .optimizer_trace
        .selected_plan_fingerprint
    };
    assert_ne!(
        fingerprint(&mut read, OptimizerQualificationOptions::default()),
        fingerprint(
            &mut read,
            OptimizerQualificationOptions::default().without_rule(HASH_JOIN)
        )
    );

    let ic5 = CASES
        .iter()
        .find(|case| case.name == "IC5 collected friends")
        .unwrap();
    let parameters = BTreeMap::from([("id".to_string(), Value::Int(5))]);
    let explain = db
        .explain_query_with_params(ic5.query, &parameters)
        .unwrap();
    let plan = explain.physical_plan.explain(0);
    assert!(
        plan.lines()
            .any(|line| line.contains("GraphMatchExec optional=true")
                && line.contains("id_set_conjuncts=[0]")),
        "{plan}"
    );
    assert!(
        explain.trace.decisions.iter().any(|decision| decision
            == "apply transformation:graph_match_id_set_semi_join: check [friend IN friends] against each input row's list id set before reading the node"),
        "{:#?}",
        explain.trace.decisions
    );
}

/// The build side is charged to the blocking-operator budget and spills
/// under a small one; results do not depend on the budget.
#[test]
fn the_build_spills_under_a_small_blocking_budget() {
    use crate::executor::ExecutionMemoryConfig;
    use std::num::{NonZeroU64, NonZeroUsize};
    let reference = {
        let mut db = random_graph(4);
        // Wide build rows make the small budget matter.
        db.query("MATCH (c:Organisation) SET c.payload = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'")
            .unwrap();
        (0..PLACES)
            .map(|id| {
                multiset(
                    &db.query_with_params(LDBC_SCAN, &place(&format!("P{id}")))
                        .unwrap()
                        .rows,
                )
            })
            .collect::<Vec<_>>()
    };
    for budget in [4 * 1024, 64 * 1024, 1024 * 1024] {
        let directory = unique_test_dir("meeting-join-spill");
        let mut db = random_graph_with(
            4,
            DatabaseConfig {
                execution_memory: ExecutionMemoryConfig {
                    blocking_operator_bytes: NonZeroUsize::new(budget).unwrap(),
                    batch_rows: NonZeroUsize::new(5).unwrap(),
                    spill_directory: directory.clone(),
                    min_spill_free_bytes: NonZeroU64::MIN,
                    ..ExecutionMemoryConfig::default()
                },
                ..DatabaseConfig::default()
            },
        );
        db.query("MATCH (c:Organisation) SET c.payload = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'")
            .unwrap();
        for (id, expected) in reference.iter().enumerate() {
            let parameters = place(&format!("P{id}"));
            match db.query_with_params(LDBC_SCAN, &parameters) {
                Ok(output) => assert_eq!(&multiset(&output.rows), expected, "budget={budget}"),
                // A budget below a few build rows refuses instead of
                // returning a partial result.
                Err(error) => {
                    assert!(budget < 8 * 1024, "budget={budget}: {error}");
                    assert!(error.to_string().contains("budget"), "{error:?}");
                }
            }
        }
        if budget == 64 * 1024 {
            // The largest place's build outgrows the table and spills.
            let report = db
                .begin_read_transaction()
                .unwrap()
                .query_request_profiled(
                    crate::QueryRequest::new(LDBC_SCAN).with_params(&place("P0")),
                )
                .unwrap()
                .execution_profile
                .blocking_operator_memory_reports
                .into_iter()
                .find(|report| report.operator == "GraphMatchExec hash join")
                .expect("the hash join reports its memory");
            assert!(report.spilled_bytes > 0, "{report:?}");
        }
        let _ = std::fs::remove_dir_all(directory);
    }
}

#[test]
fn a_cancelled_hash_join_returns_no_rows() {
    let mut db = random_graph(3);
    let token = hawdb_core::RuntimeCancellationToken::new();
    token.cancel();
    let context = hawdb_core::RuntimeTaskContext::without_deadline(token);
    let parameters = place("P0");
    let error = db
        .query_request(
            crate::QueryRequest::new(LDBC_SCAN)
                .with_params(&parameters)
                .with_task_context(&context),
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("cancel"), "{error}");
    // A work limit stops the build or the probe instead of finishing.
    let limited = hawdb_core::RuntimeTaskContext::default().with_work_limit(50);
    let error = db
        .query_request(
            crate::QueryRequest::new(LDBC_SCAN)
                .with_params(&parameters)
                .with_task_context(&limited),
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("work limit"), "{error}");
}

#[cfg(feature = "acl")]
mod access_control {
    use super::*;
    use crate::{QueryAccessControlContext, RuntimeCapabilities, RuntimeCapability};

    /// The build side reads hidden people, companies and places; none of
    /// them, nor any row through them, reaches the result.
    #[test]
    fn the_hash_join_sees_only_visible_nodes() {
        let mut db = Database::new_with_config(DatabaseConfig {
            runtime_capabilities: RuntimeCapabilities::default()
                .with(RuntimeCapability::AccessControl, true),
            ..DatabaseConfig::default()
        });
        db.query("CREATE INDEX ON :Place(name)").unwrap();
        let spaced = |id: u64, hidden: bool| {
            BTreeMap::from([
                ("id".to_string(), Value::Int(id as i64)),
                (
                    "space".to_string(),
                    Value::String(if hidden { "hidden" } else { "public" }.to_string()),
                ),
            ])
        };
        // People with ids divisible by 7 and organisations 4, 9, 14, ...
        // are hidden. Place 0 is the public `P0`, place 1 a hidden `P0`.
        let people = (0..800).map(|id| spaced(id, id % 7 == 0)).collect();
        let organisations = (0..40).map(|id| spaced(id, (id + 1) % 5 == 0)).collect();
        let places = (0..20)
            .map(|id| {
                let mut place = spaced(id, id == 1);
                let name = if id < 2 {
                    "P0".to_string()
                } else {
                    format!("P{id}")
                };
                place.insert("name".to_string(), Value::String(name));
                place
            })
            .collect();
        for (query, rows) in [
            ("CREATE (:Person {id: r.id, space_id: r.space})", people),
            (
                "CREATE (:Organisation {id: r.id, space_id: r.space})",
                organisations,
            ),
            (
                "CREATE (:Place {id: r.id, name: r.name, space_id: r.space})",
                places,
            ),
        ] {
            db.query_with_params(&format!("UNWIND $rows AS r {query}"), &list(rows))
                .unwrap();
        }
        let work = (0..800)
            .map(|id| row(&[("a", id), ("b", id % 40)]))
            .collect();
        let located = (0..40)
            .map(|id| row(&[("a", id), ("b", id % 20)]))
            .collect();
        for (query, rows) in [
            (
                "MATCH (a:Person {id: r.a}), (b:Organisation {id: r.b}) CREATE (a)-[:WORK_AT {year: 2000}]->(b)",
                work,
            ),
            (
                "MATCH (a:Organisation {id: r.a}), (b:Place {id: r.b}) CREATE (a)-[:IS_LOCATED_IN]->(b)",
                located,
            ),
        ] {
            db.query_with_params(&format!("UNWIND $rows AS r {query}"), &list(rows))
                .unwrap();
        }
        let access = QueryAccessControlContext::visibility_scope(7, "space_id", "public");
        let parameters = place("P0");
        let mut read = db.begin_read_transaction().unwrap();
        let mut run = |options| {
            let output = read
                .qualify_optimizer_query(
                    crate::QueryRequest::new(LDBC_SCAN)
                        .with_params(&parameters)
                        .with_access_control(&access),
                    options,
                    OptimizerSearchDirective::Auto,
                )
                .unwrap();
            let applied = output
                .verify_rules(&[OptimizerRuleAssertion::Required(HASH_JOIN)])
                .is_ok();
            let trace = format!(
                "{}\n{:#?}",
                output.optimizer_trace.selected_plan,
                output
                    .optimizer_trace
                    .decisions
                    .iter()
                    .filter(|decision| decision.contains("hash_join"))
                    .collect::<Vec<_>>()
            );
            (
                multiset(&output.result.unwrap().output.rows),
                applied,
                trace,
            )
        };
        let (chosen, applied, trace) = run(OptimizerQualificationOptions::default());
        let (previous, _, _) =
            run(OptimizerQualificationOptions::default().without_rule(HASH_JOIN));
        assert!(applied, "{trace}");
        // The visibility conjuncts (constant `space_id IN [...]`) cannot
        // fail, so the build applies them instead of hashing hidden nodes.
        let build = trace
            .split("build_conjuncts=")
            .nth(1)
            .unwrap_or_else(|| panic!("{trace}"));
        assert!(!build.starts_with("[]"), "{trace}");
        assert_eq!(chosen, previous);
        // Visible people work at visible organisations in the public `P0`:
        // organisations 0 and 20. Organisations 1 and 21 are in the hidden
        // `P0`, so none of their people appear.
        let expected = (0..800)
            .filter(|id| id % 7 != 0 && [0, 20].contains(&(id % 40)))
            .map(|id| format!("f=Int({id}), c=Int({}), year=Int(2000)", id % 40))
            .collect::<BTreeSet<_>>();
        assert!(!expected.is_empty());
        assert_eq!(chosen.into_iter().collect::<BTreeSet<_>>(), expected);
    }
}
