// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::QueryRequest;

#[test]
fn estimated_row_goals_do_not_truncate_skewed_inputs_or_weaken_result_budgets() {
    let mut db = Database::new();
    let rows = (0..1000)
        .map(|id| {
            Value::Map(BTreeMap::from([
                ("id".into(), Value::Int(id)),
                (
                    "space".into(),
                    Value::String(if id >= 970 { "rare" } else { "common" }.into()),
                ),
            ]))
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Memory {id: r.id, space: r.space})",
        &BTreeMap::from([("rows".into(), Value::List(rows))]),
    )
    .unwrap();
    let mut read = db.begin_read_transaction().unwrap();
    let query = "MATCH (m:Memory) WHERE m.space = $space RETURN m.id AS id LIMIT 20";
    let parameters = BTreeMap::from([("space".into(), Value::String("rare".into()))]);
    // Actual qualifying rows are clustered at the end. A costing hint must
    // never become an executor input cap or return a successful partial page.
    for options in [
        QueryStreamOptions {
            max_rows: Some(19),
            max_payload_bytes: Some(2048),
        },
        QueryStreamOptions {
            max_rows: Some(20),
            max_payload_bytes: Some(1),
        },
    ] {
        let error = read
            .query_with_params_streaming(query, &parameters, options, |_| Ok(()))
            .unwrap_err();
        assert!(matches!(error, HawDBError::ReadBudgetExceeded(_)));
    }
    let mut ids = Vec::new();
    read.query_with_params_streaming(
        query,
        &parameters,
        QueryStreamOptions {
            max_rows: Some(20),
            max_payload_bytes: Some(2048),
        },
        |row| {
            ids.push(row["id"].clone());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(ids, (970..990).map(Value::Int).collect::<Vec<_>>());

    // Sorting and aggregation must consume the whole qualifying cohort.
    let sorted = read.query_with_params("MATCH (m:Memory) WHERE m.space = $space RETURN m.id AS id ORDER BY id DESC SKIP 5 LIMIT 20", &parameters).unwrap();
    assert_eq!(
        sorted
            .rows
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>(),
        (975..995).rev().map(Value::Int).collect::<Vec<_>>()
    );
    let counted = read
        .query_with_params(
            "MATCH (m:Memory) WHERE m.space = $space RETURN count(m) AS total LIMIT 1",
            &parameters,
        )
        .unwrap();
    assert_eq!(counted.rows[0]["total"], Value::Int(30));

    let context = hawdb_core::RuntimeTaskContext::default();
    context.cancellation().cancel();
    let error = read
        .query_request_streaming(
            QueryRequest::new(query)
                .with_params(&parameters)
                .with_task_context(&context),
            |_| panic!("cancelled request delivered a row"),
        )
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
}

#[test]
fn row_goal_windows_rebind_and_preserve_nulls_ties_and_search_directives() {
    let mut db = Database::new();
    for id in 0..10 {
        db.query(&format!("CREATE (:Item {{id: {id}, kind: 1, rank: 4}})"))
            .unwrap();
    }
    db.query("CREATE (:Item {id: 10, kind: null, rank: 4})")
        .unwrap();
    db.query("CREATE (:Item {id: 11, rank: 4})").unwrap();
    let mut read = db.begin_read_transaction().unwrap();
    let query = "MATCH (n:Item) WHERE n.kind = $kind RETURN n.id AS id, n.rank AS rank ORDER BY rank, id SKIP $offset LIMIT $limit";
    for (kind, offset, limit, expected) in [
        (Value::Int(1), 0, 3, vec![0, 1, 2]),
        (Value::Int(1), 4, 2, vec![4, 5]),
        (Value::Null, 0, 3, vec![]),
        (Value::String("1".into()), 0, 3, vec![]),
        (Value::Int(1), 5, 0, vec![]),
        (Value::Int(1), 0, 3, vec![0, 1, 2]),
    ] {
        let params = BTreeMap::from([
            ("kind".into(), kind),
            ("offset".into(), Value::Int(offset)),
            ("limit".into(), Value::Int(limit)),
        ]);
        for mode in ["auto", "memo", "direct_fallback"] {
            let directed = if mode == "auto" {
                query.to_owned()
            } else {
                format!("CYPHER system.optimizer_search = '{mode}' {query}")
            };
            // Repeat each window, exercising a physical hit for auto and
            // explicit non-cached lowering for both forced search modes.
            for _ in 0..2 {
                let result = read.query_with_params(&directed, &params).unwrap();
                assert_eq!(
                    result
                        .rows
                        .iter()
                        .map(|row| row["id"].clone())
                        .collect::<Vec<_>>(),
                    expected.iter().copied().map(Value::Int).collect::<Vec<_>>(),
                    "mode={mode} params={params:?}"
                );
            }
        }
    }
    assert!(
        read.plan_cache_stats().hits >= 6,
        "repeated bound windows must exercise cached plans"
    );
    let nested = read
        .query("MATCH (n:Item) WITH n.id AS id ORDER BY id SKIP 2 LIMIT 6 RETURN id SKIP 1 LIMIT 2")
        .unwrap();
    assert_eq!(
        nested
            .rows
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(3), Value::Int(4)]
    );
}
