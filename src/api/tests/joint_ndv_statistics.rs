// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_plan_cypher::PhysicalPlanKind;

fn load(db: &mut Database, label: &str, rows: Vec<Value>) {
    db.query_with_params(
        &format!("UNWIND $rows AS r CREATE (:{label} {{id: r.id, a: r.a, b: r.b}})"),
        &BTreeMap::from([("rows".into(), Value::List(rows))]),
    )
    .unwrap();
}

fn item(id: i64, a: Value, b: Value) -> Value {
    Value::Map(BTreeMap::from([
        ("id".into(), Value::Int(id)),
        ("a".into(), a),
        ("b".into(), b),
    ]))
}

fn group_rows(output: crate::QueryOutput) -> BTreeMap<(Value, Value), Value> {
    let mut groups = BTreeMap::new();
    for row in output.rows.iter() {
        assert!(
            groups
                .insert((row["a"].clone(), row["b"].clone()), row["total"].clone())
                .is_none(),
            "duplicate grouping tuple"
        );
    }
    groups
}

#[test]
fn joint_group_estimates_preserve_aliases_nulls_and_pinned_snapshot_statistics() {
    let group = "MATCH (n:Item) RETURN n.a AS a, n.b AS b, count(n) AS total ORDER BY a, b";
    let aliases="MATCH (n:Item) WITH n.a AS x, n.b AS y RETURN x AS a, y AS b, count(*) AS total ORDER BY a, b";
    for correlated in [false, true] {
        let mut db = Database::new();
        let mut expected = BTreeMap::<_, i64>::new();
        let rows = (0_i64..400)
            .map(|id| {
                let a = Value::Int(id % 20);
                let b = Value::Int(if correlated { id % 20 } else { id / 20 });
                *expected.entry((a.clone(), b.clone())).or_default() += 1;
                item(id, a, b)
            })
            .collect();
        load(&mut db, "Item", rows);
        db.query("CREATE INDEX ON :Item(a, b)").unwrap();
        let expected = expected
            .into_iter()
            .map(|(key, count)| (key, Value::Int(count)))
            .collect::<BTreeMap<_, _>>();
        let mut old = db.begin_read_transaction().unwrap();
        for query in [group, aliases] {
            let explain = old.explain_query(query).unwrap();
            let estimate = explain
                .trace
                .selected_plan_cardinality_estimates
                .iter()
                .find(|entry| entry.operator == PhysicalPlanKind::AggregateExec)
                .unwrap();
            assert_eq!(
                estimate.estimated_rows,
                expected.len() as u64,
                "{}",
                explain.physical_plan.explain(0)
            );
            assert!(
                explain
                    .trace
                    .decisions
                    .iter()
                    .any(|line| line.contains("source=catalog_composite_ndv operation=group")),
                "{:?}",
                explain.trace.decisions
            );
            assert_eq!(group_rows(old.query(query).unwrap()), expected);
        }
        for query in [
            "CREATE (:Item {id: 400, a: null, b: 3})",
            "CREATE (:Item {id: 401, b: 3})",
            "CREATE (:Item {id: 402, a: 3})",
            "CREATE (:Item {id: 403, a: '3', b: 'kind'})",
        ] {
            db.query(query).unwrap();
        }
        assert_eq!(
            group_rows(old.query(group).unwrap()),
            expected,
            "retained reader must keep old data and statistics"
        );
        let mut current = expected.clone();
        current.insert((Value::Null, Value::Int(3)), Value::Int(2));
        current.insert((Value::Int(3), Value::Null), Value::Int(1));
        current.insert(
            (Value::String("3".into()), Value::String("kind".into())),
            Value::Int(1),
        );
        let mut read = db.begin_read_transaction().unwrap();
        for mode in ["auto", "memo", "direct_fallback"] {
            for query in [group, aliases] {
                let query = if mode == "auto" {
                    query.to_owned()
                } else {
                    format!("CYPHER system.optimizer_search = '{mode}' {query}")
                };
                // One admission and two warm hits for each auto template.
                for _ in 0..3 {
                    assert_eq!(group_rows(read.query(&query).unwrap()), current);
                }
            }
        }
        assert!(read.plan_cache_stats().hits >= 4);
        let filter = "MATCH (n:Item) WHERE n.a = $a AND n.b = $b RETURN n.id AS id ORDER BY id";
        for (a, b, expected_len) in [
            (
                Value::Int(3),
                Value::Int(3),
                if correlated { 20 } else { 1 },
            ),
            (Value::Null, Value::Int(3), 0),
            (Value::String("3".into()), Value::String("kind".into()), 1),
        ] {
            let params = BTreeMap::from([("a".into(), a), ("b".into(), b)]);
            assert_eq!(
                read.query_with_params(filter, &params).unwrap().rows.len(),
                expected_len
            );
        }
    }
}

#[test]
fn joint_join_cardinalities_preserve_duplicate_pairs_and_sparse_fallback_results() {
    let mut db = Database::new();
    for label in ["Left", "Right"] {
        load(
            &mut db,
            label,
            (0_i64..120)
                .map(|id| item(id, Value::Int(id % 10), Value::Int((id / 10) % 2)))
                .collect(),
        );
        db.query(&format!("CREATE INDEX ON :{label}(a, b)"))
            .unwrap();
    }
    let query="MATCH (l:Left), (r:Right) WHERE l.a = r.a AND l.b = r.b RETURN l.id AS left_id, r.id AS right_id ORDER BY left_id, right_id";
    let expected = (0_i64..120)
        .flat_map(|left| {
            (0_i64..120)
                .filter(move |right| left % 10 == right % 10 && (left / 10) % 2 == (right / 10) % 2)
                .map(move |right| (Value::Int(left), Value::Int(right)))
        })
        .collect::<Vec<_>>();
    assert_eq!(expected.len(), 720);
    let explain = db.explain_query(query).unwrap();
    assert!(explain.physical_plan.explain(0).contains("HashJoinExec"));
    assert!(
        explain
            .trace
            .decisions
            .iter()
            .any(|line| line.contains("operation=join_filter keys=2")),
        "{:?}",
        explain.trace.decisions
    );
    let hash = explain
        .trace
        .selected_plan_cardinality_estimates
        .iter()
        .find(|entry| entry.operator == PhysicalPlanKind::HashJoinExec)
        .unwrap();
    assert_eq!(
        hash.estimated_rows, 1440,
        "charge the one-key intermediate before the second equality"
    );
    let tied_window="MATCH (l:Left), (r:Right) WHERE l.a = r.a AND l.b = r.b RETURN l.a AS sort_a, l.id AS left_id, r.id AS right_id ORDER BY sort_a, left_id, right_id LIMIT 200";
    let mut expected_window = expected.clone();
    expected_window.sort_by_key(|(left, right)| match (left, right) {
        (Value::Int(left), Value::Int(right)) => (*left % 10, *left, *right),
        _ => unreachable!(),
    });
    expected_window.truncate(200);
    let window_plan = db
        .explain_query(tied_window)
        .unwrap()
        .physical_plan
        .explain(0);
    assert!(
        window_plan.contains("SortExec") && window_plan.contains("LimitExec"),
        "{window_plan}"
    );
    for sparse in [false, true] {
        if sparse {
            db.query("CREATE (:Left {id: 900, b: 0})").unwrap();
            db.query("CREATE (:Right {id: 901, a: null, b: 0})")
                .unwrap();
        }
        for mode in ["auto", "memo", "direct_fallback"] {
            let query = format!("CYPHER system.optimizer_search = '{mode}' {query}");
            let result = db.query(&query).unwrap();
            assert_eq!(
                result
                    .rows
                    .iter()
                    .map(|row| (row["left_id"].clone(), row["right_id"].clone()))
                    .collect::<Vec<_>>(),
                expected
            );
            let window = db
                .query(&format!(
                    "CYPHER system.optimizer_search = '{mode}' {tied_window}"
                ))
                .unwrap();
            assert_eq!(
                window
                    .rows
                    .iter()
                    .map(|row| (row["left_id"].clone(), row["right_id"].clone()))
                    .collect::<Vec<_>>(),
                expected_window
            );
        }
    }
}

#[test]
fn joint_group_sort_choice_fits_the_actual_small_query_memory_budget() {
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory: crate::executor::ExecutionMemoryConfig {
            query_memory_bytes: std::num::NonZeroUsize::new(2 * 1024 * 1024).unwrap(),
            ..Default::default()
        },
        ..Default::default()
    });
    load(
        &mut db,
        "Item",
        (0_i64..400)
            .map(|id| item(id, Value::Int(id % 20), Value::Int(id % 20)))
            .collect(),
    );
    db.query("CREATE INDEX ON :Item(a, b)").unwrap();
    let query =
        "MATCH (n:Item) RETURN n.a AS a, n.b AS b, count(n) AS total ORDER BY a, b LIMIT 40";
    let plan = db.explain_query(query).unwrap().physical_plan.explain(0);
    assert!(
        plan.contains("SortExec") && plan.contains("LimitExec"),
        "{plan}"
    );
    let output = db.query(query).unwrap();
    assert_eq!(output.rows.len(), 20);
    for (id, row) in output.rows.iter().enumerate() {
        assert_eq!(row["a"], Value::Int(id as i64));
        assert_eq!(row["b"], Value::Int(id as i64));
        assert_eq!(row["total"], Value::Int(20));
    }
}

#[test]
fn skewed_joint_keys_preserve_optional_multiplicity_and_unmatched_null_rows() {
    let mut db = Database::new();
    let left = (0_i64..80)
        .map(|id| (id, if id < 60 { 0 } else { id - 59 }))
        .collect::<Vec<_>>();
    let right = (0_i64..16)
        .map(|id| (id, if id < 8 { 0 } else { id - 7 }))
        .collect::<Vec<_>>();
    for (label, rows) in [("Left", &left), ("Right", &right)] {
        load(
            &mut db,
            label,
            rows.iter()
                .map(|(id, key)| item(*id, Value::Int(*key), Value::Int(*key)))
                .chain([item(90, Value::Null, Value::Int(0))])
                .collect(),
        );
    }
    let mut expected = Vec::new();
    for (id, key) in &left {
        let matches = right.iter().filter(|(_, r)| r == key).collect::<Vec<_>>();
        if matches.is_empty() {
            expected.push((Value::Int(*id), Value::Null));
        } else {
            expected.extend(
                matches
                    .into_iter()
                    .map(|(r, _)| (Value::Int(*id), Value::Int(*r))),
            );
        }
    }
    expected.push((Value::Int(90), Value::Null));
    let query = "MATCH (l:Left) OPTIONAL MATCH (r:Right) WHERE l.a = r.a AND l.b = r.b RETURN l.id AS left_id, r.id AS right_id ORDER BY left_id, right_id";
    for indexed in [false, true] {
        if indexed {
            for label in ["Left", "Right"] {
                db.query(&format!("CREATE INDEX ON :{label}(a, b)"))
                    .unwrap();
            }
        }
        for mode in ["auto", "memo", "direct_fallback"] {
            let output = db
                .query(&format!(
                    "CYPHER system.optimizer_search = '{mode}' {query}"
                ))
                .unwrap();
            assert_eq!(
                output
                    .rows
                    .iter()
                    .map(|row| (row["left_id"].clone(), row["right_id"].clone()))
                    .collect::<Vec<_>>(),
                expected
            );
            let filtered = db.query(&format!("CYPHER system.optimizer_search = '{mode}' MATCH (l:Left) WHERE l.a = 0 AND l.b = 0 RETURN l.id AS id ORDER BY id")).unwrap();
            assert_eq!(
                filtered
                    .rows
                    .iter()
                    .map(|row| row["id"].clone())
                    .collect::<Vec<_>>(),
                (0..60).map(Value::Int).collect::<Vec<_>>()
            );
        }
    }
}
