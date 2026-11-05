// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! openCypher `ORDER BY` semantics: `NULL` sorts last ascending and first
//! descending on every sort path (#1407), and an aggregating projection only
//! orders by projected names or grouping expressions (#1408).

use super::*;
use crate::executor::ExecutionMemoryConfig;
use std::num::{NonZeroU64, NonZeroUsize};

/// Five keyed rows; `c`, `d` and `e` each miss a key.
fn keyed() -> Database {
    let mut db = Database::new();
    for statement in [
        "CREATE (:P {name: 'a', g: 'x', v: 2})",
        "CREATE (:P {name: 'b', g: 'x', v: 1})",
        "CREATE (:P {name: 'c', g: 'y'})",
        "CREATE (:P {name: 'd', v: 3})",
        "CREATE (:P {name: 'e', g: 'x'})",
    ] {
        db.query(statement).unwrap();
    }
    db
}

fn column(output: &QueryOutput, name: &str) -> Vec<Value> {
    output.rows.iter().map(|row| row[name].clone()).collect()
}

fn names(db: &mut Database, query: &str) -> Vec<String> {
    let output = db.query(query).unwrap();
    column(&output, "name")
        .into_iter()
        .map(|value| match value {
            Value::String(name) => name,
            other => panic!("{query}: expected a name, got {other:?}"),
        })
        .collect()
}

fn ints(values: &[Option<i64>]) -> Vec<Value> {
    values
        .iter()
        .map(|value| value.map_or(Value::Null, Value::Int))
        .collect()
}

#[test]
fn issue_example_sorts_null_last_ascending_and_first_descending() {
    let mut db = Database::new();
    for statement in ["CREATE (:P {v: 2})", "CREATE (:P {v: 1})", "CREATE (:P)"] {
        db.query(statement).unwrap();
    }
    for (query, expected) in [
        (
            "MATCH (p:P) RETURN p.v ORDER BY p.v ASC",
            [Some(1), Some(2), None],
        ),
        (
            "MATCH (p:P) RETURN p.v ORDER BY p.v",
            [Some(1), Some(2), None],
        ),
        (
            "MATCH (p:P) RETURN p.v ORDER BY p.v DESC",
            [None, Some(2), Some(1)],
        ),
    ] {
        let output = db.query(query).unwrap();
        assert_eq!(column(&output, "p.v"), ints(&expected), "{query}");
    }
}

#[test]
fn property_column_and_expression_keys_place_null_like_open_cypher() {
    let mut db = keyed();
    for (query, expected) in [
        // Plain property keys, sorted directly.
        (
            "MATCH (p:P) RETURN p.name AS name ORDER BY p.v, p.name",
            ["b", "a", "d", "c", "e"],
        ),
        (
            "MATCH (p:P) RETURN p.name AS name ORDER BY p.v DESC, p.name",
            ["c", "e", "d", "a", "b"],
        ),
        // Projected columns.
        (
            "MATCH (p:P) RETURN p.name AS name, p.v AS v ORDER BY v, name",
            ["b", "a", "d", "c", "e"],
        ),
        (
            "MATCH (p:P) RETURN p.name AS name, p.v AS v ORDER BY v DESC, name",
            ["c", "e", "d", "a", "b"],
        ),
        // Expression keys (#1403) whose value is NULL.
        (
            "MATCH (p:P) RETURN p.name AS name ORDER BY p.v * 10, p.name",
            ["b", "a", "d", "c", "e"],
        ),
        (
            "MATCH (p:P) RETURN p.name AS name \
             ORDER BY CASE WHEN p.v > 1 THEN p.v END, p.name",
            ["a", "d", "b", "c", "e"],
        ),
        (
            "MATCH (p:P) RETURN p.name AS name \
             ORDER BY CASE WHEN p.v > 1 THEN p.v END DESC, p.name",
            ["b", "c", "e", "d", "a"],
        ),
        // NULL in the first key.
        (
            "MATCH (p:P) RETURN p.name AS name ORDER BY p.g, p.v",
            ["b", "a", "e", "c", "d"],
        ),
        (
            "MATCH (p:P) RETURN p.name AS name ORDER BY p.g DESC, p.v DESC",
            ["d", "c", "e", "a", "b"],
        ),
        // NULL in the second key, under the opposite direction.
        (
            "MATCH (p:P) RETURN p.name AS name ORDER BY p.g ASC, p.v DESC",
            ["e", "a", "b", "c", "d"],
        ),
        (
            "MATCH (p:P) RETURN p.name AS name ORDER BY p.g DESC, p.v ASC",
            ["d", "c", "b", "a", "e"],
        ),
    ] {
        assert_eq!(names(&mut db, query), expected, "{query}");
    }
    // Aggregate outputs and grouping keys order the same way.
    let output = db
        .query("MATCH (p:P) RETURN p.g AS g, count(*) AS n ORDER BY g")
        .unwrap();
    assert_eq!(
        column(&output, "g"),
        [
            Value::String("x".into()),
            Value::String("y".into()),
            Value::Null
        ]
    );
    let output = db
        .query("MATCH (p:P) RETURN p.g AS g, max(p.v) AS top ORDER BY top DESC, g")
        .unwrap();
    assert_eq!(column(&output, "top"), ints(&[None, Some(3), Some(2)]));
}

/// The keyed rows plus 64 rows missing both keys, enough for the bounded
/// top-k plan to win over a complete sort.
fn padded() -> Database {
    let mut db = keyed();
    let rows = (0..64)
        .map(|index| {
            Value::Map(BTreeMap::from([(
                "name".to_string(),
                Value::String(format!("n-{index:02}")),
            )]))
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:P {name: row.name})",
        &BTreeMap::from([("rows".to_string(), Value::List(rows))]),
    )
    .unwrap();
    db
}

#[test]
fn top_k_and_pagination_keep_nulls_out_of_ascending_pages() {
    let mut db = padded();
    let ascending = "MATCH (p:P) RETURN p.name AS name ORDER BY p.v, p.name LIMIT 3";
    let plan = db.explain_query(ascending).unwrap().trace.selected_plan;
    assert!(plan.contains("TopNExec"), "{plan}");
    for (query, expected) in [
        (ascending, vec!["b", "a", "d"]),
        (
            "MATCH (p:P) RETURN p.name AS name ORDER BY p.v, p.name SKIP 2 LIMIT 3",
            vec!["d", "c", "e"],
        ),
        (
            "MATCH (p:P) RETURN p.name AS name ORDER BY p.v DESC, p.name LIMIT 3",
            vec!["c", "e", "n-00"],
        ),
        (
            "MATCH (p:P) RETURN p.name AS name ORDER BY p.v DESC, p.name SKIP 66 LIMIT 3",
            vec!["d", "a", "b"],
        ),
        (
            "MATCH (p:P) RETURN p.name AS name ORDER BY p.v * 1, p.name LIMIT 3",
            vec!["b", "a", "d"],
        ),
        (
            "MATCH (p:P) RETURN p.name AS name ORDER BY p.g, p.v DESC LIMIT 4",
            vec!["e", "a", "b", "c"],
        ),
        // WITH … ORDER BY … LIMIT feeding a later clause.
        (
            "MATCH (p:P) WITH p ORDER BY p.v LIMIT 3 RETURN p.name AS name ORDER BY name",
            vec!["a", "b", "d"],
        ),
        (
            "MATCH (p:P) WITH p ORDER BY p.v DESC, p.name LIMIT 2 RETURN p.name AS name",
            vec!["c", "e"],
        ),
    ] {
        assert_eq!(names(&mut db, query), expected, "{query}");
    }
    let output = db
        .query("MATCH (p:P) WITH p.v AS v ORDER BY v LIMIT 4 RETURN v")
        .unwrap();
    assert_eq!(
        column(&output, "v"),
        ints(&[Some(1), Some(2), Some(3), None])
    );
}

#[test]
fn index_served_order_and_index_backed_sorts_keep_null_placement() {
    let mut db = Database::new();
    // Mostly negative ranks, so a positive range is selective enough for the
    // index; every eighth row from 1 has no rank.
    let rank = |id: i64| match id % 8 {
        0 => Some(id),
        1 => None,
        _ => Some(-id),
    };
    for id in 0..64 {
        match rank(id) {
            Some(rank) => db.query(&format!("CREATE (:Item {{id: {id}, rank: {rank}}})")),
            None => db.query(&format!("CREATE (:Item {{id: {id}}})")),
        }
        .unwrap();
    }
    db.query("CREATE RANGE INDEX ON :Item(rank)").unwrap();
    // Only a range predicate lets the index serve the order, and it rejects
    // NULL, so the elided sort cannot misplace one.
    let ranged = "MATCH (n:Item) WHERE n.rank > 0 RETURN n.rank AS rank ORDER BY n.rank";
    let plan = db.explain_query(ranged).unwrap().physical_plan.explain(0);
    assert!(
        !plan.contains("SortExec") && !plan.contains("TopNExec"),
        "{plan}"
    );
    let output = db.query(ranged).unwrap();
    assert_eq!(
        column(&output, "rank"),
        (1..8).map(|step| Value::Int(step * 8)).collect::<Vec<_>>()
    );
    // Without one, the indexed property is sorted and NULL still sorts last
    // ascending and first descending.
    let ids = |output: QueryOutput| column(&output, "id");
    let nulls = (0..64)
        .filter(|id| rank(*id).is_none())
        .map(Value::Int)
        .collect::<Vec<_>>();
    let output = db
        .query("MATCH (n:Item) RETURN n.id AS id ORDER BY n.rank, n.id SKIP 56")
        .unwrap();
    assert_eq!(ids(output), nulls);
    let output = db
        .query("MATCH (n:Item) RETURN n.id AS id ORDER BY n.rank DESC, n.id LIMIT 9")
        .unwrap();
    let mut expected = nulls.clone();
    expected.push(Value::Int(56));
    assert_eq!(ids(output), expected);
}

fn spilling(budget: usize) -> (Database, Vec<(i64, Option<i64>)>) {
    let directory = unique_test_dir("cypher-order-by-nulls-spill");
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory: ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(budget).unwrap(),
            batch_rows: NonZeroUsize::new(16).unwrap(),
            spill_directory: directory,
            min_spill_free_bytes: NonZeroU64::MIN,
            ..ExecutionMemoryConfig::default()
        },
        ..DatabaseConfig::default()
    });
    let rows = (0..400)
        .map(|id| (id, (id % 5 != 0).then_some((id * 7) % 37)))
        .collect::<Vec<_>>();
    let values = rows
        .iter()
        .map(|(id, v)| {
            let mut row = BTreeMap::from([
                ("id".to_string(), Value::Int(*id)),
                ("payload".to_string(), Value::String("x".repeat(192))),
            ]);
            if let Some(v) = v {
                row.insert("v".to_string(), Value::Int(*v));
            }
            Value::Map(row)
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:S {id: row.id, v: row.v, payload: row.payload})",
        &BTreeMap::from([("rows".to_string(), Value::List(values))]),
    )
    .unwrap();
    (db, rows)
}

/// Expected ids under openCypher order: by `v` with NULL greatest, then id.
fn oracle(rows: &[(i64, Option<i64>)], descending: bool) -> Vec<Value> {
    let mut sorted = rows.to_vec();
    sorted.sort_by(|(left_id, left), (right_id, right)| {
        let by_value = match (left, right) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some(_), None) => std::cmp::Ordering::Less,
            (Some(left), Some(right)) => left.cmp(right),
        };
        let by_value = if descending {
            by_value.reverse()
        } else {
            by_value
        };
        by_value.then(left_id.cmp(right_id))
    });
    sorted.into_iter().map(|(id, _)| Value::Int(id)).collect()
}

#[test]
fn spilled_sorts_and_top_k_merge_with_open_cypher_null_placement() {
    let (mut db, rows) = spilling(16 * 1024);
    for (query, descending, limit, operator) in [
        // Property keys merge spilled runs through encoded keys.
        (
            "MATCH (s:S) RETURN s.id AS id, s.payload AS payload ORDER BY s.v, s.id",
            false,
            None,
            "SortExec",
        ),
        (
            "MATCH (s:S) RETURN s.id AS id, s.payload AS payload ORDER BY s.v DESC, s.id",
            true,
            None,
            "SortExec",
        ),
        // Expression keys merge spilled runs by re-evaluating them.
        (
            "MATCH (s:S) RETURN s.id AS id, s.payload AS payload ORDER BY s.v + 0, s.id",
            false,
            None,
            "SortExec",
        ),
        (
            "MATCH (s:S) RETURN s.id AS id, s.payload AS payload ORDER BY s.v + 0 DESC, s.id",
            true,
            None,
            "SortExec",
        ),
        // A top-k whose retained rows exceed the budget spills its heap.
        (
            "MATCH (s:S) RETURN s.id AS id, s.payload AS payload ORDER BY s.v, s.id LIMIT 60",
            false,
            Some(60),
            "TopNExec",
        ),
        (
            "MATCH (s:S) RETURN s.id AS id, s.payload AS payload \
             ORDER BY s.v DESC, s.id SKIP 20 LIMIT 40",
            true,
            Some(60),
            "TopNExec",
        ),
    ] {
        let mut expected = oracle(&rows, descending);
        if let Some(limit) = limit {
            expected.truncate(limit);
        }
        if query.contains("SKIP 20") {
            expected.drain(..20);
        }
        let output = db.query(query).unwrap();
        assert_eq!(column(&output, "id"), expected, "{query}");
        let profile = db.explain_analyze_query(query).unwrap();
        let memory = profile
            .execution_profile
            .blocking_operator_memory_reports
            .iter()
            .find(|report| report.operator == operator)
            .unwrap_or_else(|| {
                panic!(
                    "{query}: no {operator} report in {}",
                    profile.physical_plan.explain(0)
                )
            });
        assert!(memory.spilled_bytes > 0, "{query}: {memory:?}");
    }
}

#[test]
fn sql_order_by_keeps_its_null_order_defaults_and_overrides() {
    let mut db = Database::new();
    db.query_sql("CREATE TABLE t (id BIGINT, v BIGINT)")
        .unwrap();
    db.query_sql("INSERT INTO t VALUES (1, 2), (2, NULL), (3, 1), (4, NULL), (5, 3)")
        .unwrap();
    for (query, expected) in [
        // PostgreSQL defaults: NULL is the largest value.
        ("SELECT id FROM t ORDER BY v, id", [3, 1, 5, 2, 4]),
        ("SELECT id FROM t ORDER BY v DESC, id", [2, 4, 5, 1, 3]),
        // Explicit placement overrides the direction default.
        (
            "SELECT id FROM t ORDER BY v NULLS FIRST, id",
            [2, 4, 3, 1, 5],
        ),
        (
            "SELECT id FROM t ORDER BY v DESC NULLS LAST, id",
            [5, 1, 3, 2, 4],
        ),
        (
            "SELECT id FROM t ORDER BY v ASC NULLS LAST, id",
            [3, 1, 5, 2, 4],
        ),
        (
            "SELECT id FROM t ORDER BY v DESC NULLS FIRST, id",
            [2, 4, 5, 1, 3],
        ),
    ] {
        let output = db.query_sql(query).unwrap();
        assert_eq!(
            column(&output, "id"),
            expected.map(Value::Int).to_vec(),
            "{query}"
        );
        // The bounded top-k path shares the executor's sort operators.
        let limited = db.query_sql(&format!("{query} LIMIT 3")).unwrap();
        assert_eq!(
            column(&limited, "id"),
            expected[..3]
                .iter()
                .copied()
                .map(Value::Int)
                .collect::<Vec<_>>(),
            "{query} LIMIT 3"
        );
    }
}

fn people() -> Database {
    let mut db = Database::new();
    for statement in [
        "CREATE (:Person {name: 'alice', lastName: 'Smith', age: 34})",
        "CREATE (:Person {name: 'bob', lastName: 'Jones', age: 27})",
        "CREATE (:Person {name: 'carol', lastName: 'Smith', age: 41})",
        "CREATE (:Person {name: 'dave', lastName: 'Brown', age: 19})",
        "CREATE (:Person {name: 'erin', lastName: 'Jones', age: 27})",
    ] {
        db.query(statement).unwrap();
    }
    db
}

#[test]
fn aggregating_projections_reject_order_keys_that_would_regroup() {
    let mut db = people();
    for query in [
        // The #1408 example: grouping by `n.age` would split the counts.
        "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people ORDER BY n.age",
        "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people ORDER BY n.age DESC",
        "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people ORDER BY id(n)",
        "MATCH (n:Person) RETURN count(*) AS people ORDER BY n.name",
        "MATCH (n:Person) WITH n.lastName AS last, count(*) AS people \
         ORDER BY n.age LIMIT 2 RETURN last, people",
        "MATCH (n:Person) WITH n, n.age AS age WITH n.lastName AS last, count(*) AS people \
         ORDER BY age RETURN last, people",
    ] {
        let error = db.query(query).unwrap_err();
        let hawdb_core::HawDBError::Semantic(message) = &error else {
            panic!("{query}: expected a semantic error, got {error:?}");
        };
        assert!(
            message.contains("is not in scope after an aggregating RETURN or WITH"),
            "{query}: {message}"
        );
    }
    let error = db
        .query("MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people ORDER BY n.age")
        .unwrap_err()
        .to_string();
    assert!(error.contains("'n.age'"), "{error}");
}

#[test]
fn aggregating_projections_order_by_projected_names_and_grouping_keys() {
    let mut db = people();
    let last = |values: &[&str]| {
        values
            .iter()
            .map(|value| Value::String(value.to_string()))
            .collect::<Vec<_>>()
    };
    for (query, expected_last, expected_people) in [
        (
            "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people ORDER BY last",
            last(&["Brown", "Jones", "Smith"]),
            [1, 2, 2],
        ),
        (
            "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people \
             ORDER BY people DESC, last",
            last(&["Jones", "Smith", "Brown"]),
            [2, 2, 1],
        ),
        // A key equal to a projected grouping expression is grouped already.
        (
            "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people \
             ORDER BY n.lastName DESC",
            last(&["Smith", "Jones", "Brown"]),
            [2, 2, 1],
        ),
        (
            "MATCH (n:Person) RETURN n.lastName, count(*) AS people ORDER BY n.lastName",
            last(&["Brown", "Jones", "Smith"]),
            [1, 2, 2],
        ),
        (
            "MATCH (n:Person) WITH n.lastName AS last, count(*) AS people \
             ORDER BY n.lastName LIMIT 2 RETURN last, people",
            last(&["Brown", "Jones"]),
            [1, 2, 0],
        ),
    ] {
        let output = db.query(query).unwrap();
        let key = if query.contains("RETURN n.lastName,") {
            "n.lastName"
        } else {
            "last"
        };
        assert_eq!(column(&output, key), expected_last, "{query}");
        assert_eq!(
            column(&output, "people"),
            expected_people[..expected_last.len()]
                .iter()
                .copied()
                .map(Value::Int)
                .collect::<Vec<_>>(),
            "{query}"
        );
    }
    // Non-aggregating projections may still order by unprojected values.
    let output = db
        .query("MATCH (n:Person) RETURN n.name AS name ORDER BY n.age DESC, n.name LIMIT 2")
        .unwrap();
    assert_eq!(column(&output, "name"), last(&["carol", "alice"]));
}
