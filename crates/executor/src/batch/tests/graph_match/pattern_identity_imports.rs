// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Pattern-expression arguments and seeds passed to the match by entity
//! identity (#1615), compared with plans that copy every argument and seed
//! value into the subquery row (`with_legacy_pattern_value_imports`).
//!
//! Both sides run the same queries over the same random graph, in memory and
//! with forced spills, and must return the same rows and fail with the same
//! errors. Arguments come from graph slots, collected lists, `WITH` and
//! `UNWIND` map columns and OPTIONAL MATCH NULLs; seeds from slots, relationship
//! endpoints, list elements and map columns (#1607, #1610). The memory ledger
//! must settle to zero on both sides.

use super::identity_group_keys::{memory_configs, random_graph, run, spill_directory};
use super::*;
use hawdb_plan_cypher::PatternProjection;

/// Queries and whether their rows are totally ordered.
const QUERIES: &[(&str, bool)] = &[
    // Graph variables of the enclosing row.
    (
        "MATCH (a:A) RETURN id(a) AS i, [(a)-[:R]->(b) | b.name] AS v, NOT ((a)-[:S]->()) AS p",
        false,
    ),
    // Collected node lists, including a back reference to a slot.
    (
        "MATCH (a:A) OPTIONAL MATCH (a)-[:R]->(b) WITH a, collect(b) AS bs \
         RETURN id(a) AS i, [p IN bs | [(p)-[:S]->(c) | c.name]] AS v, \
         size([p IN bs WHERE (p)-[:R]->(a)]) AS back",
        false,
    ),
    (
        "MATCH (a)-[]->(b) WITH a, collect(b) AS bs \
         RETURN id(a) AS i, [p IN bs WHERE (p)-[:R]->()<-[:S]-(a) | p.age] AS v",
        false,
    ),
    // Collected relationship lists.
    (
        "MATCH (a)-[r:R]->(b) WITH a, collect(r) AS rs \
         RETURN id(a) AS i, [q IN rs | [(x)-[q]->(y) | [id(x), id(y), q.w]]] AS v",
        false,
    ),
    // Map columns carried by WITH and UNWIND.
    (
        "MATCH (a:A)-[r]->(b) WITH id(a) AS i, head(collect(b)) AS x, head(collect(r)) AS q \
         RETURN i, [(x)-[:R]->(y) | y.age] AS v, [(s)-[q]->(t) | id(t)] AS w",
        false,
    ),
    (
        "MATCH (a:A) WITH a AS x, a.name AS n RETURN n, [(x)-[]->(y) | id(y)] AS v",
        false,
    ),
    (
        "MATCH (a)-[]->(b) WITH a, collect(b) AS bs UNWIND bs AS x \
         RETURN id(a) AS i, x.name AS xn, [(x)-[:S]->(y) | y.name] AS v",
        false,
    ),
    // OPTIONAL MATCH NULLs as slots, map columns and list elements.
    (
        "MATCH (a:B) OPTIONAL MATCH (a)-[:S]->(b:A) \
         RETURN id(a) AS i, [(b)-[]->(y) | id(y)] AS v, NOT ((b)-[:R]->()) AS p",
        false,
    ),
    (
        "MATCH (a:B) OPTIONAL MATCH (a)-[r:S]->(b:A) WITH a, head(collect(b)) AS x, head(collect(r)) AS q \
         RETURN id(a) AS i, x IS NULL AS missing, [(x)-[]->(y) | id(y)] AS v, \
         [p IN [x, null] | size([(p)-[]->() | 1])] AS n, [(s)-[q]->(t) | id(t)] AS w",
        false,
    ),
    // Seeds from slots and from a relationship's bound endpoints.
    (
        "MATCH (x:A)-[r:R]->(y) RETURN id(r) AS i, \
         [(a)-[:S]->(b) WHERE a = startNode(r) | id(b)] AS v, \
         [(a)-[:R]->(b) WHERE b = endNode(r) | id(a)] AS w",
        false,
    ),
    (
        "MATCH (x:A)-[r]->(y) RETURN id(r) AS i, \
         [(a)-[:R]->(b) WHERE a = x OR a = y | [id(a), id(b)]] AS v",
        false,
    ),
    (
        "MATCH (x)-[r:S]->(y) RETURN id(r) AS i, \
         [(a)-[:R]->(m)-[:S]->(b) WHERE (a = x AND b = y) OR (a = y AND b = x) | id(m)] AS v",
        false,
    ),
    // Seeds that are relationships, NULL or not nodes equal no node.
    (
        "MATCH (x:A)-[r]->(y) RETURN id(r) AS i, \
         [(a)-[:R]->(b) WHERE a = r | id(b)] AS v, [(a)-[:R]->(b) WHERE a = null | id(b)] AS w, \
         [(a)-[:R]->(b) WHERE a = 1 | id(b)] AS z",
        false,
    ),
    (
        "MATCH (x:B) OPTIONAL MATCH (x)-[:S]->(y:A) \
         RETURN id(x) AS i, [(a)-[:R]->(b) WHERE a = y | id(b)] AS v",
        false,
    ),
    // Seeds from list elements and map columns.
    (
        "MATCH (a:A)-[]->(b) WITH a, collect(b) AS bs \
         RETURN id(a) AS i, [p IN bs | [(c)-[:R]->(d) WHERE c = p | id(d)]] AS v",
        false,
    ),
    (
        "MATCH (a:A)-[]->(b) WITH id(a) AS i, head(collect(b)) AS x \
         RETURN i, [(c)-[:S]->(d) WHERE d = x | id(c)] AS v",
        false,
    ),
    // A seed bound by an enclosing pattern's match.
    (
        "MATCH (a:A) RETURN id(a) AS i, \
         [(a)-[:R]->(b) | [(c)-[:S]->(d) WHERE c = b | id(d)]] AS v",
        false,
    ),
    // Ordered output and LIMIT over pattern results.
    (
        "MATCH (a)-[]->(b) WITH a, collect(b) AS bs \
         WITH a, size([p IN bs WHERE (p)-[:R]->()]) AS k ORDER BY k DESC, id(a) LIMIT 5 \
         RETURN id(a) AS i, k",
        true,
    ),
];

fn plan_with(query: &str, legacy: bool) -> (LogicalPlan, PhysicalPlan) {
    let planned = if legacy {
        hawdb_plan_cypher::with_legacy_pattern_value_imports(|| {
            hawdb_plan_cypher::plan_pipeline_query(query, &BTreeMap::new())
        })
    } else {
        hawdb_plan_cypher::plan_pipeline_query(query, &BTreeMap::new())
    };
    let planned = planned.unwrap_or_else(|error| panic!("{query}: {error}"));
    (planned.clone(), lower(planned))
}

/// Every pattern expression of `plan`'s projections, including nested ones.
fn patterns(plan: &PhysicalPlan) -> Vec<PatternProjection> {
    fn collect(expression: &ProjectionExpression, found: &mut Vec<PatternProjection>) {
        if let ProjectionExpression::Pattern(pattern) = expression {
            found.push(pattern.as_ref().clone());
        }
        expression.all_children(|child| {
            collect(child, found);
            true
        });
    }
    let mut found = Vec::new();
    hawdb_plan_cypher::visit_plan(plan, &mut |node| {
        if let PhysicalPlan::ProjectExec { items, .. } = node {
            for item in items {
                collect(&item.expression, &mut found);
            }
        }
    });
    found
}

#[test]
fn identity_imports_match_value_imports_on_random_graphs() {
    let mut refused = Vec::new();
    let mut runs = 0usize;
    for seed in 1..=8u64 {
        let graph = random_graph(seed);
        let directory = spill_directory("pattern-identity-imports", seed);
        for (config, memory) in memory_configs(&directory) {
            for (query, ordered) in QUERIES {
                let context = format!("seed {seed}, {config}: {query}");
                let (_, current) = plan_with(query, false);
                let (_, legacy) = plan_with(query, true);
                let current_patterns = patterns(&current);
                assert!(!current_patterns.is_empty(), "{context}");
                assert!(
                    current_patterns
                        .iter()
                        .any(|pattern| !pattern.identity_columns.is_empty()
                            || pattern.arguments.is_empty()),
                    "{context}"
                );
                assert!(
                    patterns(&legacy)
                        .iter()
                        .all(|pattern| pattern.identity_columns.is_empty()),
                    "{context}"
                );
                runs += 1;
                let actual = run(&graph, &current, &memory);
                let expected = run(&graph, &legacy, &memory);
                assert_eq!(actual.ledger.used_bytes, 0, "{context}");
                assert_eq!(expected.ledger.used_bytes, 0, "{context}");
                // No owned argument or seed copy is charged.
                assert!(
                    actual.ledger.peak_bytes <= expected.ledger.peak_bytes,
                    "{context}: peak {} > {}",
                    actual.ledger.peak_bytes,
                    expected.ledger.peak_bytes
                );
                let (mut actual, mut expected) = match (actual.rows, expected.rows) {
                    (Ok(actual), Ok(expected)) => (actual, expected),
                    // A row too large for the forced spill configuration is
                    // refused alike on both sides.
                    (Err(actual), Err(expected)) => {
                        assert_eq!(actual.to_string(), expected.to_string(), "{context}");
                        refused.push(context);
                        continue;
                    }
                    (actual, expected) => panic!(
                        "{context}: current {:?}, legacy {:?}",
                        actual.map(|rows| rows.len()),
                        expected.map(|rows| rows.len())
                    ),
                };
                if !ordered {
                    actual.sort();
                    expected.sort();
                }
                assert_eq!(actual, expected, "{context}");
            }
        }
        std::fs::remove_dir_all(&directory).ok();
    }
    assert!(
        refused.len() * 10 <= runs,
        "too many refused runs: {refused:#?}"
    );
}

/// Graph variables and seeds that name a slot record reach the match by
/// identity, without the existence read a seed value needs.
#[test]
fn slot_seeds_skip_the_existence_read() {
    let graph = random_graph(3);
    let memory = ExecutionMemoryConfig::default();
    for query in [
        "MATCH (x:A)-[r:R]->(y) RETURN [(a)-[:S]->(b) WHERE a = startNode(r) | id(b)] AS v",
        "MATCH (x:A)-[r]->(y) RETURN [(a)-[:R]->(b) WHERE a = x OR a = y | id(b)] AS v",
    ] {
        let current = run(&graph, &plan_with(query, false).1, &memory);
        let legacy = run(&graph, &plan_with(query, true).1, &memory);
        assert_eq!(current.rows.unwrap(), legacy.rows.unwrap(), "{query}");
        assert_eq!(current.node_owned_reads, 0, "{query}");
        assert!(legacy.node_owned_reads > 0, "{query}");
    }
    // A seed held as a value is still checked against the snapshot.
    let query = "MATCH (a:A)-[]->(b) WITH a, collect(b) AS bs \
                 RETURN [p IN bs | [(c)-[:R]->(d) WHERE c = p | id(d)]] AS v";
    let current = run(&graph, &plan_with(query, false).1, &memory);
    let legacy = run(&graph, &plan_with(query, true).1, &memory);
    assert_eq!(current.rows.unwrap(), legacy.rows.unwrap(), "{query}");
    assert!(current.node_owned_reads > 0, "{query}");
    assert_eq!(current.node_owned_reads, legacy.node_owned_reads, "{query}");
}

/// A value that is not an entity of the import's kind, and a variable or
/// column the row lacks, fail as they did when every value was copied.
#[test]
fn import_errors_are_unchanged() {
    let graph = random_graph(5);
    let memory = ExecutionMemoryConfig::default();
    let error = |plan: &PhysicalPlan| {
        let outcome = run(&graph, plan, &memory);
        assert_eq!(outcome.ledger.used_bytes, 0);
        match outcome.rows {
            Ok(rows) => panic!("expected an error, got {rows:?}"),
            Err(error) => error.to_string(),
        }
    };
    for (query, expected) in [
        // Relationships where the pattern needs nodes, and the reverse.
        (
            "MATCH ()-[r]->() WITH collect(r) AS rs RETURN [q IN rs | NOT ((q)-[]->())] AS v",
            "MATCH variable 'q' must be bound to a node, found a map",
        ),
        (
            "MATCH (a)-[]->(b) WITH collect(b) AS bs RETURN [p IN bs | [(x)-[p]->(y) | 1]] AS v",
            "MATCH variable 'p' must be bound to a relationship, found a map",
        ),
        (
            "MATCH (a)-[r]->(b) WITH head(collect(r)) AS x RETURN [(x)-[]->(y) | y] AS v",
            "MATCH variable 'x' must be bound to a node, found a map",
        ),
        // Scalars and maps that are not entities.
        (
            "MATCH (a:A) WITH collect(a.name) AS ns RETURN [p IN ns | NOT ((p)-[]->())] AS v",
            "must be bound to a node, found a string",
        ),
        (
            "MATCH (a:A) WITH {_id: 1} AS x RETURN [(x)-[]->(y) | y] AS v",
            "MATCH variable 'x' must be bound to a node, found a map",
        ),
        (
            "MATCH (a:A) WITH {_id: 'one', labels: []} AS x RETURN [(x)-[]->(y) | y] AS v",
            "typed graph value lacks identity field '_id'",
        ),
        // A node-shaped map that names no node.
        (
            "MATCH (a:A) WITH {_id: 999999, labels: []} AS x RETURN [(x)-[]->(y) | y] AS v",
            "bound node disappeared during MATCH",
        ),
    ] {
        let current = error(&plan_with(query, false).1);
        let legacy = error(&plan_with(query, true).1);
        assert!(current.contains(expected), "{query}: {current}");
        assert_eq!(current, legacy, "{query}");
    }
    // A graph variable or argument column the row lacks.
    let query = "MATCH (a:A)-[]->(b) WITH a, head(collect(b)) AS x \
                 RETURN [(x)-[]->(y) | 1] AS v, [(a)-[]->(y) | 1] AS w";
    for (rename, expected) in [
        ("a", "missing variable 'zz' during pattern evaluation"),
        ("x", "missing column 'zz' during projection"),
    ] {
        let mut errors = Vec::new();
        for legacy in [false, true] {
            let (logical, _) = plan_with(query, legacy);
            errors.push(error(&lower(rename_reads(logical, rename))));
        }
        assert!(errors[0].contains(expected), "{rename}: {}", errors[0]);
        assert_eq!(errors[0], errors[1], "{rename}");
    }
}

/// Renames what the final projection reads of `name` to `zz`, so the row
/// lacks it.
fn rename_reads(plan: LogicalPlan, name: &str) -> LogicalPlan {
    fn rename(expression: &mut ProjectionExpression, name: &str) {
        match expression {
            ProjectionExpression::Column(column) if column == name => *column = "zz".into(),
            ProjectionExpression::Pattern(pattern) => {
                for variable in &mut pattern.graph_variables {
                    if variable == name {
                        *variable = "zz".into();
                    }
                }
                for program in pattern.programs_mut() {
                    for step in &mut program.steps {
                        match step {
                            GraphMatchStep::Node(node) if node.variable == name => {
                                node.variable = "zz".into()
                            }
                            GraphMatchStep::Expand { source, .. } if source == name => {
                                *source = "zz".into()
                            }
                            _ => {}
                        }
                    }
                }
            }
            _ => {}
        }
        let _ = expression.try_for_each_child_mut(|child| {
            rename(child, name);
            Ok::<(), std::convert::Infallible>(())
        });
    }
    match plan {
        LogicalPlan::Project { mut items, input } => {
            for item in &mut items {
                rename(&mut item.expression, name);
            }
            LogicalPlan::Project { items, input }
        }
        plan => panic!("unexpected plan: {plan:?}"),
    }
}
