// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Pattern-expression arguments and seeds passed to the match by entity
//! identity (#1615), through the embedded API with the optimizer, projected
//! match reads and out-of-core canonical reads. Each query runs on a fresh
//! database whose plans pass identities and on one planned to copy every
//! argument and seed value, as before; both must return the same rows, or
//! fail with the same error.

use super::identity_group_keys::{open, rows, statements};
use super::*;

/// Queries, and whether their plan passes some column by identity.
const QUERIES: &[(&str, bool)] = &[
    // IC10: a collected list element imported into a pattern predicate.
    (
        "MATCH (a:Person)-[:KNOWS]->(b) WITH a, collect(b) AS bs \
         RETURN a.id AS ai, size(bs) AS n, \
         size([p IN bs WHERE (p)-[:LIKES]->()<-[:KNOWS]-(a)]) AS common",
        true,
    ),
    (
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:LIKES]->(b) WITH a, collect(b) AS bs \
         RETURN a.id AS ai, [p IN bs | [(p)-[:KNOWS]->(c) | c.id]] AS v",
        true,
    ),
    // Relationship lists and WITH map columns.
    (
        "MATCH (a:Person)-[r:KNOWS]->(b) WITH a, collect(r) AS rs \
         RETURN a.id AS ai, [q IN rs | [(x)-[q]->(y) | [x.id, y.id, q.weight]]] AS v",
        true,
    ),
    (
        "MATCH (a:Person)-[r]->(b) WITH a.id AS ai, head(collect(b)) AS x, head(collect(r)) AS q \
         RETURN ai, [(x)-[:KNOWS]->(y) | y.age] AS v, [(s)-[q]->(t) | t.id] AS w",
        true,
    ),
    (
        "MATCH (a:Person)-[]->(b) WITH a, collect(b) AS bs UNWIND bs AS x \
         RETURN a.id AS ai, x.id AS xi, [(x)-[:LIKES]->(y) | y.name] AS v",
        true,
    ),
    // OPTIONAL MATCH NULLs.
    (
        "MATCH (a:Person {admin: true}) OPTIONAL MATCH (a)-[r:LIKES]->(b) \
         WITH a, head(collect(b)) AS x, head(collect(r)) AS q \
         RETURN a.id AS ai, [(x)-[]->(y) | y.id] AS v, [(s)-[q]->(t) | t.id] AS w",
        true,
    ),
    // Seeds from slots, endpoints, list elements and map columns (#1607).
    (
        "MATCH (x:Person)-[r:KNOWS]->(y) \
         RETURN id(r) AS i, [(a:Person)-[:LIKES]->(b) WHERE a = startNode(r) | b.id] AS v",
        true,
    ),
    (
        "MATCH (x:Person)-[r:LIKES]->(y) \
         RETURN id(r) AS i, [(a:Person)-[:KNOWS]->(m)-[:KNOWS]->(b) \
         WHERE (a = x AND b = y) OR (a = y AND b = x) | m.id] AS v",
        true,
    ),
    (
        "MATCH (a:Person)-[:KNOWS]->(b) WITH a, collect(b) AS bs \
         RETURN a.id AS ai, [p IN bs | [(c)-[:LIKES]->(d) WHERE c = p | d.id]] AS v",
        true,
    ),
    // Graph variables only; nothing is imported.
    (
        "MATCH (a:Person) RETURN a.id AS ai, [(a)-[:KNOWS]->(b) | b.id] AS v",
        false,
    ),
];

#[test]
fn identity_imports_return_the_rows_of_value_imports() {
    for seed in 1..=2u64 {
        let statements = statements(seed);
        for out_of_core in [false, true] {
            for (query, identity) in QUERIES {
                let context = format!("seed {seed}, out_of_core {out_of_core}: {query}");
                let (mut current, current_path) = open(&statements, out_of_core);
                let (mut legacy, legacy_path) = open(&statements, out_of_core);
                let plan = format!("{:?}", current.explain_query(query).unwrap().physical_plan);
                assert!(plan.contains("identity_columns: "), "{context}");
                assert_eq!(
                    !plan.contains("identity_columns: []"),
                    *identity,
                    "{context}:\n{plan}"
                );
                let mut actual = rows(&mut current, query);
                let mut expected = hawdb_plan_cypher::with_legacy_pattern_value_imports(|| {
                    let plan = format!("{:?}", legacy.explain_query(query).unwrap().physical_plan);
                    assert!(
                        !plan.contains("identity_columns: [\""),
                        "{context}:\n{plan}"
                    );
                    rows(&mut legacy, query)
                });
                actual.sort();
                expected.sort();
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

/// Values that are not entities of the import's kind, or name no stored
/// node, fail with the same errors as when every value was copied.
#[test]
fn import_errors_match_value_imports() {
    let statements = statements(4);
    for (query, expected) in [
        (
            "MATCH ()-[r]->() WITH collect(r) AS rs RETURN [q IN rs | size([(q)-[]->(x) | x])] AS v",
            "MATCH variable 'q' must be bound to a node, found a map",
        ),
        (
            "MATCH (a:Person)-[]->(b) WITH collect(b) AS bs RETURN [p IN bs | [(x)-[p]->(y) | 1]] AS v",
            "MATCH variable 'p' must be bound to a relationship, found a map",
        ),
        (
            "MATCH (a:Person) WITH collect(a.id) AS ids RETURN [p IN ids | size([(p)-[]->(x) | x])] AS v",
            "MATCH variable 'p' must be bound to a node, found an integer",
        ),
        (
            "MATCH (a:Person) WITH {_id: 999999, labels: []} AS x RETURN [(x)-[]->(y) | y] AS v",
            "bound node disappeared during MATCH",
        ),
    ] {
        let (mut current, _) = open(&statements, false);
        let (mut legacy, _) = open(&statements, false);
        let actual = current.query(query).unwrap_err().to_string();
        let previous = hawdb_plan_cypher::with_legacy_pattern_value_imports(|| {
            legacy.query(query).unwrap_err().to_string()
        });
        assert!(actual.contains(expected), "{query}: {actual}");
        assert_eq!(actual, previous, "{query}");
    }
}

#[cfg(feature = "acl")]
mod access_control {
    use super::*;
    use crate::{QueryAccessControlContext, RuntimeCapabilities, RuntimeCapability};

    /// `(1)-[:KNOWS]->(2 hidden)-[:KNOWS]->(3)`, `(1)-[:KNOWS]->(4)`,
    /// `(4)-[:KNOWS]->(2 hidden)` and `(3)-[:KNOWS]->(1)`, with `space_id`
    /// 'public' except on node 2.
    fn graph() -> Database {
        let mut db = Database::new_with_config(DatabaseConfig {
            runtime_capabilities: RuntimeCapabilities::default()
                .with(RuntimeCapability::AccessControl, true),
            ..DatabaseConfig::default()
        });
        for (id, space) in [(1, "public"), (2, "hidden"), (3, "public"), (4, "public")] {
            db.query(&format!(
                "CREATE (:Person {{id: {id}, ssn: 'ssn{id}', space_id: '{space}'}})"
            ))
            .unwrap();
        }
        for (a, b) in [(1, 2), (2, 3), (1, 4), (4, 2), (3, 1)] {
            db.query(&format!(
                "MATCH (a:Person {{id: {a}}}), (b:Person {{id: {b}}}) CREATE (a)-[:KNOWS]->(b)"
            ))
            .unwrap();
        }
        db
    }

    fn scoped(
        db: &mut Database,
        query: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Vec<Vec<Value>> {
        let output = db
            .query_with_params_access_control(
                query,
                parameters,
                QueryAccessControlContext::visibility_scope(7, "space_id", "public"),
            )
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        let mut rows = (0..output.rows.len())
            .map(|row| output.rows[row].values().cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| format!("{row:?}"));
        rows
    }

    /// Identity imports are matched under the visibility policy exactly as
    /// copied values were: a hidden node passed in by value, from a list or
    /// a seed, matches nothing and is never reached.
    #[test]
    fn identity_imports_see_only_visible_nodes() {
        // Node values read without the policy, including the hidden node 2.
        let nodes = rows_of(&mut graph(), "MATCH (n:Person) RETURN n ORDER BY n.id");
        let parameters = BTreeMap::from([("nodes".to_string(), Value::List(nodes))]);
        for (query, expected) in [
            (
                "MATCH (a:Person {id: 1}) WITH a, $nodes AS ns \
                 RETURN [p IN ns | [(p)-[:KNOWS]->(b) | b.ssn]] AS v",
                vec![vec![Value::List(vec![
                    Value::List(vec![Value::String("ssn4".into())]),
                    Value::List(Vec::new()),
                    Value::List(vec![Value::String("ssn1".into())]),
                    Value::List(Vec::new()),
                ])]],
            ),
            (
                "MATCH (a:Person {id: 1}) WITH $nodes[1] AS hidden \
                 RETURN [(hidden)-[:KNOWS]->(b) | b.ssn] AS v, \
                 [(c:Person)-[:KNOWS]->(d) WHERE c = hidden | d.ssn] AS w",
                vec![vec![Value::List(Vec::new()), Value::List(Vec::new())]],
            ),
            (
                "MATCH (x:Person)-[r:KNOWS]->(y:Person) \
                 RETURN x.ssn AS x, [(a:Person)-[:KNOWS]->(b) WHERE a = endNode(r) | b.ssn] AS v",
                vec![
                    vec![Value::String("ssn1".into()), Value::List(Vec::new())],
                    vec![
                        Value::String("ssn3".into()),
                        Value::List(vec![Value::String("ssn4".into())]),
                    ],
                ],
            ),
            (
                "MATCH (x:Person)-[:KNOWS]->(y:Person) WITH x, collect(y) AS ys \
                 RETURN x.ssn AS x, [p IN ys | size([(p)-[:KNOWS]->(z) | z])] AS v",
                vec![
                    vec![
                        Value::String("ssn1".into()),
                        Value::List(vec![Value::Int(0)]),
                    ],
                    vec![
                        Value::String("ssn3".into()),
                        Value::List(vec![Value::Int(1)]),
                    ],
                ],
            ),
        ] {
            let mut current = graph();
            let plan = current
                .explain_query_with_params_access_control(
                    query,
                    &parameters,
                    QueryAccessControlContext::visibility_scope(7, "space_id", "public"),
                )
                .unwrap()
                .physical_plan;
            let plan = format!("{plan:?}");
            assert!(plan.contains("identity_columns: [\""), "{query}:\n{plan}");
            let actual = scoped(&mut current, query, &parameters);
            let mut legacy = graph();
            let previous = hawdb_plan_cypher::with_legacy_pattern_value_imports(|| {
                scoped(&mut legacy, query, &parameters)
            });
            assert_eq!(actual, previous, "{query}");
            assert_eq!(actual, expected, "{query}");
        }
    }

    fn rows_of(db: &mut Database, query: &str) -> Vec<Value> {
        let output = db.query(query).unwrap();
        (0..output.rows.len())
            .map(|row| output.rows[row].value(0).unwrap().clone())
            .collect()
    }
}
