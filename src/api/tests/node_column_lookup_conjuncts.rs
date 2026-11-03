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
//! column` with other conditions plans one batched column lookup and a
//! residual filter, instead of a label scan per upstream row (#1544).

use super::*;

/// `Item.key` repeats every seven ids and is NULL for every eleventh one;
/// `Other` nodes share keys so the lookup must honor the label.
fn database() -> Database {
    let mut db = Database::new();
    for id in 0..120_i64 {
        let key = if id % 11 == 0 {
            Value::Null
        } else {
            Value::Int(id % 7)
        };
        db.query_with_params(
            "CREATE (:Item {id: $id, key: $key, flag: $flag, rank: $rank})",
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
    let mut db = database();
    // Duplicate, NULL, missing and repeated keys, in a deliberate order.
    let keys = Value::List(
        [3, 1, 3, 9, 0, 6, 1]
            .into_iter()
            .map(Value::Int)
            .chain([Value::Null, Value::Int(5)])
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
            "UNWIND $keys AS k WITH k MATCH (n:Item) WHERE n.key = k AND n.flag = true{tail} \
             RETURN k, n.id{limit}"
        );
        // `column = property` is not a lookup key, so this keeps the scan.
        let reference = format!(
            "UNWIND $keys AS k WITH k MATCH (n:Item) WHERE k = n.key AND n.flag = true{tail} \
             RETURN k, n.id{limit}"
        );
        let lookup_plan = plan(&mut db, &lookup, &parameters);
        assert!(
            lookup_plan.contains("NodeColumnLookupExec"),
            "{lookup_plan}"
        );
        assert!(lookup_plan.contains("FilterExec"), "{lookup_plan}");
        let reference_plan = plan(&mut db, &reference, &parameters);
        assert!(
            !reference_plan.contains("NodeColumnLookupExec"),
            "{reference_plan}"
        );
        let expected = rows(&mut db, &reference, &parameters);
        assert!(!expected.is_empty());
        assert_eq!(rows(&mut db, &lookup, &parameters), expected, "{lookup}");
    }
}

#[test]
fn optional_conjunctive_match_keeps_its_null_rows() {
    let mut db = database();
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
