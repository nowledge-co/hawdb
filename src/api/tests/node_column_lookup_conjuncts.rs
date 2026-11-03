// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A row-sourced single-node MATCH whose predicate conjoins `node.property =
//! column` with other infallible conditions plans one column lookup that
//! evaluates them as its node predicate, instead of a label scan per upstream
//! row (#1544).

use super::*;

/// `Item.key` repeats every seven ids, is NULL for every eleventh one and a
/// float for every seventeenth; every thirteenth `Item` has no `rank`. `Other`
/// nodes share keys so the lookup must honor the label.
fn database(indexed: bool) -> Database {
    let mut db = Database::new();
    if indexed {
        db.query("CREATE INDEX ON :Item(key)").unwrap();
    }
    for id in 0..120_i64 {
        let key = if id % 11 == 0 {
            Value::Null
        } else if id % 17 == 0 {
            Value::Float((id % 7) as f64)
        } else {
            Value::Int(id % 7)
        };
        let create = if id % 13 == 0 {
            "CREATE (:Item {id: $id, key: $key, flag: $flag})"
        } else {
            "CREATE (:Item {id: $id, key: $key, flag: $flag, rank: $rank})"
        };
        db.query_with_params(
            create,
            &BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("key".into(), key.clone()),
                ("flag".into(), Value::Bool(id % 3 != 0)),
                ("rank".into(), Value::Int(id % 5)),
            ]),
        )
        .unwrap();
        db.query_with_params(
            "CREATE (:Other {id: $id, key: $key, flag: true, rank: 0})",
            &BTreeMap::from([("id".into(), Value::Int(id)), ("key".into(), key)]),
        )
        .unwrap();
    }
    db
}

fn plan(db: &mut Database, query: &str, parameters: &BTreeMap<String, Value>) -> String {
    let explain = db
        .query_with_params(&format!("EXPLAIN {query}"), parameters)
        .unwrap();
    let Value::String(plan) = &explain.rows[0]["plan"] else {
        panic!("missing plan")
    };
    plan.clone()
}

fn rows(db: &mut Database, query: &str, parameters: &BTreeMap<String, Value>) -> Vec<Vec<Value>> {
    db.query_with_params(query, parameters)
        .unwrap()
        .rows
        .into_rows()
        .into_iter()
        .map(|row| row.values().cloned().collect())
        .collect()
}

#[test]
fn conjunctive_column_lookup_matches_the_per_row_scan_in_order() {
    for indexed in [false, true] {
        let mut db = database(indexed);
        // Duplicate, NULL, missing, float and repeated keys, in a deliberate order.
        let keys = Value::List(
            [3, 1, 3, 9, 0, 6, 1]
                .into_iter()
                .map(Value::Int)
                .chain([
                    Value::Null,
                    Value::Float(5.0),
                    Value::Int(5),
                    Value::Float(2.5),
                ])
                .collect(),
        );
        let parameters = BTreeMap::from([("keys".into(), keys), ("rank".into(), Value::Int(2))]);
        for (tail, limit) in [
            ("", ""),
            ("", " LIMIT 9"),
            (" AND n.rank <> $rank", ""),
            (" AND n.rank <> $rank", " LIMIT 4"),
        ] {
            let lookup = format!(
                "UNWIND $keys AS k WITH k MATCH (n:Item) WHERE n.key = k AND n.flag <> false{tail} \
             RETURN k, n.id{limit}"
            );
            // `column = property` is not a lookup key, so this keeps the scan.
            let reference = format!(
                "UNWIND $keys AS k WITH k MATCH (n:Item) WHERE k = n.key AND n.flag <> false{tail} \
             RETURN k, n.id{limit}"
            );
            let lookup_plan = plan(&mut db, &lookup, &parameters);
            assert!(
                lookup_plan.contains("NodeColumnLookupExec"),
                "{lookup_plan}"
            );
            assert!(lookup_plan.contains("visibility=Some"), "{lookup_plan}");
            let reference_plan = plan(&mut db, &reference, &parameters);
            assert!(
                !reference_plan.contains("NodeColumnLookupExec"),
                "{reference_plan}"
            );
            let expected = rows(&mut db, &reference, &parameters);
            assert!(!expected.is_empty());
            assert_eq!(
                rows(&mut db, &lookup, &parameters),
                expected,
                "{indexed} {lookup}"
            );
        }
    }
}

/// Another equality on the node may be the indexed one, so it keeps the
/// per-row path, which seeks whichever conjunct has an index.
#[test]
fn another_node_equality_keeps_the_per_row_path() {
    let mut db = database(false);
    db.query("CREATE INDEX ON :Item(rank)").unwrap();
    let parameters = BTreeMap::from([(
        "rows".into(),
        Value::List(
            (0..6)
                .map(|i| {
                    Value::Map(BTreeMap::from([
                        ("key".into(), Value::Int(i % 7)),
                        ("rank".into(), Value::Int(i % 5)),
                    ]))
                })
                .collect(),
        ),
    )]);
    for equality in [
        "n.rank = r.rank",
        "r.rank = n.rank",
        "n.rank = 2",
        "n.rank IN [1, 2]",
    ] {
        let query = format!(
            "UNWIND $rows AS r WITH r.key AS k, r AS r MATCH (n:Item) WHERE n.key = k AND {equality} \
             RETURN k, n.id"
        );
        let explain = plan(&mut db, &query, &parameters);
        assert!(
            !explain.contains("NodeColumnLookupExec"),
            "{equality}: {explain}"
        );
        assert!(!rows(&mut db, &query, &parameters).is_empty(), "{equality}");
    }
}

/// A row limit keeps the per-row lookup, which stops reading at the limit
/// instead of reading every match of the batch first.
#[test]
fn a_row_limit_keeps_the_early_stop() {
    let mut db = database(false);
    let parameters = BTreeMap::from([(
        "keys".into(),
        Value::List(vec![Value::Int(3), Value::Int(1), Value::Int(4)]),
    )]);
    let candidates = |db: &mut Database, query: &str| {
        let output = db
            .explain_analyze_query_with_params(query, &parameters)
            .unwrap();
        let reports = &output.execution_profile.scan_pruning_reports;
        (
            output.output.rows.len(),
            reports
                .iter()
                .map(|report| report.candidate_count_before_filter)
                .sum::<usize>(),
        )
    };
    let all = "UNWIND $keys AS k WITH k MATCH (n:Item) WHERE n.key = k RETURN n.id";
    let (rows_all, read_all) = candidates(&mut db, all);
    assert!(rows_all > 20, "{rows_all}");
    assert_eq!(read_all, rows_all);
    let (rows_one, read_one) = candidates(&mut db, &format!("{all} LIMIT 1"));
    assert_eq!(rows_one, 1);
    assert_eq!(read_one, 1, "the lookup must stop at the limit");
    let conjunctive =
        "UNWIND $keys AS k WITH k MATCH (n:Item) WHERE n.key = k AND n.flag <> false \
                       RETURN n.id LIMIT 1";
    let explain = plan(&mut db, conjunctive, &parameters);
    assert!(explain.contains("NodeColumnLookupExec"), "{explain}");
    let (rows_one, read_one) = candidates(&mut db, conjunctive);
    assert_eq!(rows_one, 1);
    assert!(
        read_one < rows_all,
        "read {read_one} of {rows_all} matches for LIMIT 1"
    );
}

#[test]
fn optional_conjunctive_match_keeps_its_null_rows() {
    let mut db = database(false);
    let parameters = BTreeMap::from([(
        "keys".into(),
        Value::List(vec![Value::Int(3), Value::Int(9), Value::Null]),
    )]);
    let query =
        "UNWIND $keys AS k WITH k OPTIONAL MATCH (n:Item) WHERE n.key = k AND n.flag = true \
                 RETURN k, n.id";
    let explain = plan(&mut db, query, &parameters);
    assert!(!explain.contains("NodeColumnLookupExec"), "{explain}");
    let output = rows(&mut db, query, &parameters);
    // Key 9 matches nothing and NULL matches nothing: both keep a NULL row.
    assert!(
        output.contains(&vec![Value::Int(9), Value::Null]),
        "{output:?}"
    );
    assert!(
        output.contains(&vec![Value::Null, Value::Null]),
        "{output:?}"
    );
}
