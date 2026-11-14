// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A first MATCH whose WHERE disjuncts each pin a different node by an
//! indexed equality runs as a union of seeded branches (#1625). These
//! differential tests compare, on randomized small tag hierarchies, the
//! chosen plan with the plan that has the rule disabled (one scan of the
//! text-order start) and with the ordinary cached plan. Row multisets,
//! including the duplicates that parallel relationships and diamonds in the
//! hierarchy produce, and errors must be identical.

use super::*;
use crate::optimizer::{
    OptimizerQualificationOptions, OptimizerRuleAssertion, OptimizerSearchDirective,
    OptionalOptimizerRule,
};

const RULE: OptionalOptimizerRule = OptionalOptimizerRule::DisjunctiveSeedUnion;

struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

const TAGS: u64 = 80;
const CLASSES: u64 = 16;

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

/// The id, name and visibility of one node. Every fifth class and every
/// seventh tag have no name; names repeat within and across labels, so a row
/// can satisfy both disjuncts.
fn node(id: u64, name: Option<String>, hidden: bool) -> BTreeMap<String, Value> {
    let mut node = BTreeMap::from([
        ("id".to_string(), Value::Int(id as i64)),
        (
            "space".to_string(),
            Value::String(if hidden { "hidden" } else { "public" }.to_string()),
        ),
    ]);
    if let Some(name) = name {
        node.insert("name".to_string(), Value::String(name));
    }
    node
}

/// A class DAG with diamonds (so variable-length walks reach a class along
/// several trails) and tags with parallel `HAS_TYPE` relationships.
fn random_graph(seed: u64, config: DatabaseConfig) -> Database {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut db = Database::new_with_config(config);
    db.query("CREATE INDEX ON :Tag(name)").unwrap();
    db.query("CREATE INDEX ON :TagClass(name)").unwrap();
    let classes = (0..CLASSES)
        .map(|id| {
            node(
                id,
                (id % 5 != 4).then(|| format!("N{}", id % 8)),
                id % 6 == 5,
            )
        })
        .collect();
    let tags = (0..TAGS)
        .map(|id| {
            let name = (id % 7 != 3).then(|| format!("N{}", rng.below(12)));
            node(id, name, id % 9 == 4)
        })
        .collect();
    let mut subclass = Vec::new();
    for class in 1..CLASSES {
        for _ in 0..=rng.below(2) {
            subclass.push(pair(class, rng.below(class)));
        }
    }
    let mut typed = Vec::new();
    for tag in 0..TAGS {
        let class = rng.below(CLASSES);
        typed.push(pair(tag, class));
        match rng.below(4) {
            0 => typed.push(pair(tag, class)),
            1 => typed.push(pair(tag, rng.below(CLASSES))),
            _ => {}
        }
    }
    for (query, rows) in [
        (
            "UNWIND $rows AS r CREATE (:TagClass {id: r.id, name: r.name, space_id: r.space})",
            classes,
        ),
        (
            "UNWIND $rows AS r CREATE (:Tag {id: r.id, name: r.name, space_id: r.space})",
            tags,
        ),
        (
            "UNWIND $rows AS r MATCH (a:TagClass {id: r.a}), (b:TagClass {id: r.b}) \
             CREATE (a)-[:IS_SUBCLASS_OF]->(b)",
            subclass,
        ),
        (
            "UNWIND $rows AS r MATCH (a:Tag {id: r.a}), (b:TagClass {id: r.b}) \
             CREATE (a)-[:HAS_TYPE]->(b)",
            typed,
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

type Outcome = std::result::Result<Vec<String>, String>;

/// Runs `query` with every optional rule and without the union, on one
/// snapshot. Returns both outcomes and whether the union ran.
fn qualified(
    db: &Database,
    query: &str,
    parameters: &BTreeMap<String, Value>,
    access: Option<&crate::QueryAccessControlContext>,
) -> (Outcome, Outcome, bool) {
    let mut read = db.begin_read_transaction().unwrap();
    let mut run = |options| {
        let mut request = crate::QueryRequest::new(query).with_params(parameters);
        if let Some(access) = access {
            request = request.with_access_control(access);
        }
        let output = read
            .qualify_optimizer_query(request, options, OptimizerSearchDirective::Auto)
            .unwrap();
        let applied = output
            .verify_rules(&[OptimizerRuleAssertion::Required(RULE)])
            .is_ok();
        let outcome = output
            .result
            .map(|result| multiset(&result.output.rows))
            .map_err(|error| error.to_string());
        (outcome, applied)
    };
    let (chosen, applied) = run(OptimizerQualificationOptions::default());
    let (previous, disabled_applied) =
        run(OptimizerQualificationOptions::default().without_rule(RULE));
    assert!(!disabled_applied, "{query}");
    (chosen, previous, applied)
}

struct Case {
    name: &'static str,
    query: &'static str,
    /// Whether the union must run for every non-NULL name.
    applies: bool,
}

const IC12: &str = "MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(base:TagClass) \
     WHERE tag.name = $name OR base.name = $name RETURN tag.id AS t, base.id AS b";

const CASES: &[Case] = &[
    Case {
        name: "ic12",
        query: IC12,
        applies: true,
    },
    Case {
        name: "ic12 collected",
        query: "MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(base:TagClass) \
                WHERE tag.name = $name OR base.name = $name \
                WITH collect(tag.id) AS tags RETURN size(tags) AS n",
        applies: true,
    },
    Case {
        name: "three disjuncts, two over the same label",
        query: "MATCH (tag:Tag)-[:HAS_TYPE]->(c:TagClass)-[:IS_SUBCLASS_OF|HAS_TYPE*0..3]->(base:TagClass) \
                WHERE tag.name = $name OR c.name = $name OR base.name = $name \
                RETURN tag.id AS t, c.id AS c, base.id AS b",
        applies: true,
    },
    Case {
        name: "a disjunct with another conjunct",
        query: "MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*1..]->(base:TagClass) \
                WHERE (tag.name = $name AND tag.id > 20) OR base.name = $name \
                RETURN tag.id AS t, base.id AS b",
        applies: true,
    },
    Case {
        name: "a conjunct beside the disjunction",
        query: "MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(base:TagClass) \
                WHERE base.id < 9 AND (base.name = $name OR tag.name = $name) \
                RETURN tag.id AS t, base.id AS b",
        applies: true,
    },
    Case {
        name: "both disjuncts on one node",
        query: "MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(base:TagClass) \
                WHERE tag.name = $name OR tag.name = 'N1' RETURN tag.id AS t, base.id AS b",
        applies: false,
    },
    Case {
        name: "optional match",
        query: "MATCH (c:TagClass {id: 1}) \
                OPTIONAL MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(base:TagClass) \
                WHERE tag.name = $name OR base.name = $name \
                RETURN c.id AS c, tag.id AS t, base.id AS b",
        applies: false,
    },
    Case {
        // Fails on tag 5 in both plans: the union would skip that match.
        name: "a WHERE that may fail",
        query: "MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(base:TagClass) \
                WHERE (tag.name = $name OR base.name = $name) AND 100 / (tag.id - 5) <> 0 \
                RETURN tag.id AS t, base.id AS b",
        applies: false,
    },
];

fn names() -> Vec<Value> {
    ["N0", "N3", "N5", "N9", "N11", "missing"]
        .into_iter()
        .map(|name| Value::String(name.to_string()))
        .chain([Value::Null])
        .collect()
}

#[test]
fn chosen_previous_and_cached_plans_return_identical_multisets() {
    let mut applied_cases = BTreeMap::<&str, usize>::new();
    let mut duplicated = 0;
    let mut both = 0;
    for seed in 1..=5 {
        let mut db = random_graph(seed, DatabaseConfig::default());
        for case in CASES {
            for name in names() {
                let parameters = BTreeMap::from([("name".to_string(), name.clone())]);
                let context = format!("{} seed={seed} name={name:?}", case.name);
                let (chosen, previous, applied) = qualified(&db, case.query, &parameters, None);
                assert_eq!(chosen, previous, "{context}");
                let cached = db
                    .query_with_params(case.query, &parameters)
                    .map(|output| multiset(&output.rows))
                    .map_err(|error| error.to_string());
                assert_eq!(cached, chosen, "{context}");
                if case.applies && name != Value::Null && !applied {
                    let explain = db
                        .explain_query_with_params(case.query, &parameters)
                        .unwrap();
                    panic!(
                        "{context}: union did not run\n{}\n{:?}",
                        explain.physical_plan.explain(0),
                        explain.trace.decisions
                    );
                }
                if !case.applies {
                    assert!(!applied, "{context}");
                }
                *applied_cases.entry(case.name).or_default() += usize::from(applied);
                if case.name == "ic12" {
                    let rows = chosen.unwrap();
                    duplicated += usize::from(rows.windows(2).any(|pair| pair[0] == pair[1]));
                    let matched = db
                        .query_with_params(
                            "MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(base:TagClass) \
                             WHERE tag.name = $name AND base.name = $name RETURN count(*) AS n",
                            &parameters,
                        )
                        .unwrap();
                    both += usize::from(
                        matched
                            .rows
                            .iter()
                            .any(|row| row.get("n").is_some_and(|count| *count != Value::Int(0))),
                    );
                }
            }
        }
    }
    // Duplicate trails and rows that satisfy both disjuncts are covered.
    assert!(duplicated > 0 && both > 0, "{duplicated} {both}");
    assert!(applied_cases["three disjuncts, two over the same label"] > 0);
}

#[test]
fn a_fallible_where_fails_in_both_plans() {
    let db = random_graph(3, DatabaseConfig::default());
    let parameters = BTreeMap::from([("name".to_string(), Value::String("N0".into()))]);
    let (chosen, previous, applied) = qualified(&db, CASES[7].query, &parameters, None);
    assert!(!applied);
    assert!(chosen.is_err(), "{chosen:?}");
    assert_eq!(chosen, previous);
}

#[test]
fn explain_shows_the_seeded_branches() {
    let db = random_graph(2, DatabaseConfig::default());
    let parameters = BTreeMap::from([("name".to_string(), Value::String("N3".into()))]);
    let explain = db.explain_query_with_params(IC12, &parameters).unwrap();
    let plan = explain.physical_plan.explain(0);
    let graph_match = plan
        .lines()
        .find(|line| line.contains("GraphMatchExec"))
        .unwrap_or_else(|| panic!("{plan}"));
    for expected in [
        " seeded_union=[branch 0: steps=[Node(GraphMatchNode { variable: \"tag\", label: \"Tag\", properties: {\"name\": String(\"N3\")} })",
        "; branch 1: steps=[Node(GraphMatchNode { variable: \"base\", label: \"TagClass\", properties: {\"name\": String(\"N3\")} }), Expand { source: \"base\"",
        "direction: Incoming",
        "PropertyNotEq { variable: \"tag\", property: \"name\", value: String(\"N3\") }",
    ] {
        assert!(graph_match.contains(expected), "{expected}\n{graph_match}");
    }
    // The EXPLAIN table carries the same operator text.
    assert!(explain.to_string().contains("seeded_union=[branch 0"));
    assert!(
        explain.trace.decisions.iter().any(|decision| decision.starts_with(
            "apply transformation:disjunctive_seed_union: run GraphMatchExec as a union of 2 branches seeded by index on [tag:Tag.name, base:TagClass.name]"
        )),
        "{:?}",
        explain.trace.decisions
    );
    // The union is a different plan from the scan it replaces.
    let fingerprint = |options| {
        db.begin_read_transaction()
            .unwrap()
            .qualify_optimizer_query(
                crate::QueryRequest::new(IC12).with_params(&parameters),
                options,
                OptimizerSearchDirective::Auto,
            )
            .unwrap()
            .optimizer_trace
            .selected_plan_fingerprint
    };
    assert_ne!(
        fingerprint(OptimizerQualificationOptions::default()),
        fingerprint(OptimizerQualificationOptions::default().without_rule(RULE))
    );
}

#[cfg(feature = "acl")]
mod access_control {
    use super::*;
    use crate::{QueryAccessControlContext, RuntimeCapabilities, RuntimeCapability};

    /// Hidden tags and classes, including hidden seeds and hidden classes in
    /// the middle of a trail, reach no row of any branch.
    #[test]
    fn every_branch_sees_only_visible_nodes() {
        let access = QueryAccessControlContext::visibility_scope(7, "space_id", "public");
        let mut applied_runs = 0;
        for seed in 1..=4 {
            let mut db = random_graph(
                seed,
                DatabaseConfig {
                    runtime_capabilities: RuntimeCapabilities::default()
                        .with(RuntimeCapability::AccessControl, true),
                    ..DatabaseConfig::default()
                },
            );
            for name in names() {
                let parameters = BTreeMap::from([("name".to_string(), name.clone())]);
                let context = format!("seed={seed} name={name:?}");
                let (chosen, previous, applied) = qualified(&db, IC12, &parameters, Some(&access));
                assert_eq!(chosen, previous, "{context}");
                applied_runs += usize::from(applied);
                // No hidden tag or class is bound.
                let rows = chosen.unwrap();
                for row in &rows {
                    let ids = row
                        .split(", ")
                        .map(|field| {
                            let digits = field
                                .trim_start_matches(|c: char| !c.is_ascii_digit())
                                .trim_end_matches(')');
                            digits.parse::<u64>().unwrap()
                        })
                        .collect::<Vec<_>>();
                    assert!(ids[0] % 9 != 4 && ids[1] % 6 != 5, "{context}: {row}");
                }
                // Hidden tags and classes are hidden as endpoints; the walk
                // between them is unrestricted, as without the union.
                let expected = db
                    .query_with_params(
                        "MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(base:TagClass) \
                         WHERE (tag.name = $name OR base.name = $name) \
                         AND tag.space_id = 'public' AND base.space_id = 'public' \
                         RETURN tag.id AS t, base.id AS b",
                        &parameters,
                    )
                    .unwrap();
                assert_eq!(rows, multiset(&expected.rows), "{context}");
            }
        }
        assert!(applied_runs > 0);
    }
}
