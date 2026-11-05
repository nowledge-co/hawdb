// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_storage::join_sampling::JoinSamplingLimits;
use std::time::Duration;

const ONE: &str = "MATCH (a:A {kind: $kind})-[:R]->(b:B {kind: $kind}) RETURN a.id AS a, b.id AS b";
const TWO: &str = "MATCH (a:A {kind: $kind})-[:R]->(b:B {kind: $kind})-[:S]->(c:C {kind: $kind}) RETURN a.id AS a, b.id AS b, c.id AS c";

fn params(kind: i64) -> BTreeMap<String, Value> {
    BTreeMap::from([("kind".into(), Value::Int(kind))])
}

fn limits() -> JoinSamplingLimits {
    JoinSamplingLimits {
        max_duration: Duration::from_secs(10),
        ..Default::default()
    }
}

fn fixture(limits: Option<JoinSamplingLimits>) -> Database {
    let mut db = Database::new_with_config(DatabaseConfig {
        join_sampling: limits,
        ..Default::default()
    });
    {
        let mut access = db.runtime.get_mut().unwrap();
        let runtime = &mut *access;
        let mut node = |label, id, kind| {
            runtime
                .store
                .create_node(
                    &mut runtime.catalog,
                    label,
                    BTreeMap::from([
                        ("id".into(), Value::Int(id)),
                        ("kind".into(), Value::Int(kind)),
                    ]),
                )
                .unwrap()
        };
        let a = [node("A", 0, 1), node("A", 1, 1), node("A", 2, 1)];
        let b = [node("B", 0, 1), node("B", 1, 1), node("B", 2, 0)];
        let c = node("C", 0, 1);
        for (source, target, kind) in [
            (a[0], b[0], "R"),
            (a[0], b[0], "R"),
            (a[0], b[1], "R"),
            (a[1], b[0], "R"),
            (b[0], c, "S"),
            (b[0], c, "S"),
            (b[1], c, "S"),
        ] {
            runtime
                .store
                .create_relationship(&mut runtime.catalog, source, target, kind, BTreeMap::new())
                .unwrap();
        }
        // A small deterministic degree skew makes the reverse path cheaper.
        // These are fixture edges, not repeated attempts to find a random seed.
        for _ in 0..16 {
            runtime
                .store
                .create_relationship(&mut runtime.catalog, a[0], b[2], "R", BTreeMap::new())
                .unwrap();
        }
        for label in ["A", "B", "C"] {
            runtime
                .store
                .create_property_index(&mut runtime.catalog, label, "kind")
                .unwrap();
        }
    }
    db
}

fn tuples(output: QueryOutput) -> Vec<Vec<Value>> {
    let mut rows: Vec<_> = output
        .rows
        .iter()
        .map(|row| {
            ["a", "b", "c"]
                .into_iter()
                .filter_map(|name| row.get(name).cloned())
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

#[test]
fn sampled_orientation_preserves_multisets_and_parameter_cache_identity() {
    let mut baseline = fixture(None);
    let db = fixture(Some(limits()));
    let mut read = db.begin_read_transaction().unwrap();
    for (query, count) in [(ONE, 4), (TWO, 7)] {
        let expected = tuples(baseline.query_with_params(query, &params(1)).unwrap());
        assert_eq!(expected.len(), count);
        let explain = read.explain_query_with_params(query, &params(1)).unwrap();
        assert!(
            explain
                .trace
                .decisions
                .iter()
                .any(|line| line.contains("selected=Some(\"reverse\")")),
            "{:?}",
            explain.trace.decisions
        );
        let mut directions = Vec::new();
        hawdb_plan_cypher::visit_plan(&explain.physical_plan, &mut |plan| {
            if let hawdb_plan_cypher::PhysicalPlan::AdjacencyExpandExec { direction, .. } = plan {
                directions.push(*direction);
            }
        });
        assert_eq!(directions.len(), if query == ONE { 1 } else { 2 });
        assert!(directions
            .iter()
            .all(|direction| *direction == hawdb_core::RelationshipDirection::Incoming));
        assert_eq!(
            tuples(read.query_with_params(query, &params(1)).unwrap()),
            expected
        );
        let before = read.plan_cache_stats();
        assert!(read
            .query_with_params(query, &params(0))
            .unwrap()
            .rows
            .is_empty());
        assert!(
            read.plan_cache_stats().misses > before.misses,
            "different bound values must not reuse sampled costs"
        );
        let before = read.plan_cache_stats();
        assert_eq!(
            tuples(read.query_with_params(query, &params(1)).unwrap()),
            expected
        );
        assert!(read.plan_cache_stats().hits > before.hits);
    }
}

#[test]
fn sampled_plan_reuse_stays_on_the_original_snapshot() {
    let mut db = fixture(Some(limits()));
    let mut old = db.begin_read_transaction().unwrap();
    let expected = tuples(old.query_with_params(TWO, &params(1)).unwrap());
    db.query("MATCH (a:A {id: 2}), (b:B {id: 1}) CREATE (a)-[:R]->(b)")
        .unwrap();
    assert_eq!(
        tuples(old.query_with_params(TWO, &params(1)).unwrap()),
        expected
    );
    assert_eq!(db.query_with_params(TWO, &params(1)).unwrap().rows.len(), 8);
}

#[test]
fn sampling_refusal_and_ineligible_shapes_keep_ordinary_execution() {
    let mut baseline = fixture(None);
    let mut refused = fixture(Some(JoinSamplingLimits {
        max_probes: 0,
        ..limits()
    }));
    assert_eq!(
        tuples(refused.query_with_params(TWO, &params(1)).unwrap()),
        tuples(baseline.query_with_params(TWO, &params(1)).unwrap())
    );
    let explain = refused.explain_query_with_params(TWO, &params(1)).unwrap();
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|line| line.contains("ProbeBudget")));
    let mut candidate = fixture(Some(limits()));
    let null = BTreeMap::from([("kind".into(), Value::Null)]);
    assert!(candidate
        .query_with_params(TWO, &null)
        .unwrap()
        .rows
        .is_empty());
    assert!(!candidate
        .explain_query_with_params(TWO, &null)
        .unwrap()
        .trace
        .decisions
        .iter()
        .any(|line| line.starts_with("join sampling:")));
    for query in [
        "MATCH (a:A {kind: 1})-[:R]->(b:B {kind: 1}) RETURN a.id AS a LIMIT 1",
        "MATCH (a:A {kind: 1})-[:R]-(b:B {kind: 1}) RETURN a.id AS a, b.id AS b",
        "MATCH (a:A {kind: 1}) OPTIONAL MATCH (a)-[:R]->(b:B {kind: 1}) RETURN a.id AS a, b.id AS b",
        "MATCH (a:A {kind: 1})-[:R]->(b:B {kind: 1})<-[:R]-(c:A {kind: 1}) RETURN a.id AS a, b.id AS b, c.id AS c",
    ] {
        let explain = candidate.explain_query(query).unwrap();
        assert!(!explain.trace.decisions.iter().any(|line| line.starts_with("join sampling:")), "{query}");
        assert_eq!(tuples(candidate.query(query).unwrap()), tuples(baseline.query(query).unwrap()), "{query}");
    }
}

#[test]
fn sampled_two_hop_reversal_preserves_self_loops_and_relationship_bindings() {
    fn fixture(enabled: bool) -> Database {
        let mut db = Database::new_with_config(DatabaseConfig {
            join_sampling: enabled.then(limits),
            ..Default::default()
        });
        {
            let mut access = db.runtime.get_mut().unwrap();
            let runtime = &mut *access;
            let mut node = |id, kind| {
                runtime
                    .store
                    .create_node(
                        &mut runtime.catalog,
                        "X",
                        BTreeMap::from([
                            ("id".into(), Value::Int(id)),
                            ("kind".into(), Value::Int(kind)),
                        ]),
                    )
                    .unwrap()
            };
            let a = node(0, 1);
            let b = node(1, 1);
            let cold = node(2, 0);
            for (key, (source, target, kind)) in [
                (a, a, "R"),
                (a, a, "R"),
                (b, a, "R"),
                (a, a, "S"),
                (a, a, "S"),
                (a, b, "S"),
            ]
            .into_iter()
            .enumerate()
            {
                runtime
                    .store
                    .create_relationship(
                        &mut runtime.catalog,
                        source,
                        target,
                        kind,
                        BTreeMap::from([("key".into(), Value::Int(key as i64))]),
                    )
                    .unwrap();
            }
            for _ in 0..16 {
                runtime
                    .store
                    .create_relationship(&mut runtime.catalog, a, cold, "R", BTreeMap::new())
                    .unwrap();
            }
            runtime
                .store
                .create_property_index(&mut runtime.catalog, "X", "kind")
                .unwrap();
        }
        db
    }
    let query = "MATCH (a:X {kind: 1})-[r:R]->(b:X {kind: 1})-[s:S]->(c:X {kind: 1}) RETURN a.id AS a, b.id AS b, c.id AS c, r.key AS r, s.key AS s";
    let mut baseline = fixture(false);
    let mut candidate = fixture(true);
    let values = |output: QueryOutput| {
        let mut rows: Vec<Vec<_>> = output
            .rows
            .iter()
            .map(|row| row.iter().map(|(_, value)| value.clone()).collect())
            .collect();
        rows.sort();
        rows
    };
    let expected = values(baseline.query(query).unwrap());
    assert_eq!(expected.len(), 9);
    assert_eq!(values(candidate.query(query).unwrap()), expected);
    let explain = candidate.explain_query(query).unwrap();
    assert!(
        explain
            .trace
            .decisions
            .iter()
            .any(|line| line.contains("selected=Some(\"reverse\")")),
        "{:?}",
        explain.trace.decisions
    );
}
