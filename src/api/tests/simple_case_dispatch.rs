// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn binding_template_replans_folded_guards_that_are_also_projected() {
    let mut db = Database::new();
    db.query("CREATE (:Item {id: 0, flag: false})").unwrap();
    db.query("CREATE (:Item {id: 1, flag: true})").unwrap();
    let mut read = db.begin_read_transaction().unwrap();
    for (query, initial, changed) in [
        (
            "MATCH (n:Item) WHERE ($gate = 0 OR n.id = $gate) RETURN n.id AS id, CASE n.id WHEN 0 THEN $gate ELSE $gate END AS gate, CASE n.id WHEN 0 THEN $other ELSE $other END AS other, CASE n.id WHEN 0 THEN $score ELSE $score END AS score",
            Value::Int(1),
            Value::Int(0),
        ),
        (
            "MATCH (n:Item) WHERE ($gate = true OR n.flag = $gate) RETURN n.id AS id, CASE n.id WHEN 0 THEN $gate ELSE $gate END AS gate, CASE n.id WHEN 0 THEN $other ELSE $other END AS other, CASE n.id WHEN 0 THEN $score ELSE $score END AS score",
            Value::Bool(false),
            Value::Bool(true),
        ),
    ] {
        for (gate, expected) in [(initial, vec![Value::Int(1)]), (changed, vec![Value::Int(0), Value::Int(1)])] {
            let parameters = BTreeMap::from([
                ("gate".into(), gate),
                ("other".into(), Value::Int(0)),
                ("score".into(), Value::Float(0.5)),
            ]);
            let mut ids = Vec::new();
            read.query_with_params_streaming(query, &parameters, QueryStreamOptions { max_rows: Some(2), max_payload_bytes: Some(1024) }, |row| {
                ids.push(row["id"].clone());
                Ok(())
            }).unwrap();
            ids.sort();
            if expected.len() == 1 {
                assert_eq!(ids.len(), 1);
            } else {
                assert_eq!(ids, expected, "folded guard must be replanned");
            }
        }
    }
}

#[test]
fn simple_case_dispatch_rebinds_values_through_scan_and_projection() {
    for column in [false, true] {
        let mut db = Database::new_with_config(DatabaseConfig {
            max_plan_cache_entries: Some(8),
            ..Default::default()
        });
        let mut parameters = BTreeMap::new();
        let mut expression = if column { "CASE key" } else { "CASE n.id" }.to_owned();
        for index in 0..20 {
            db.query(&format!("CREATE (:Item {{id: 'item-{index}'}})"))
                .unwrap();
            parameters.insert(
                format!("id_{index}"),
                Value::String(format!("item-{index}")),
            );
            parameters.insert(format!("score_{index}"), Value::Int(index));
            expression.push_str(&format!(" WHEN $id_{index} THEN $score_{index}"));
        }
        expression.push_str(" ELSE null END AS score");
        let query = if column {
            format!("MATCH (n:Item) WITH n.id AS key RETURN key, {expression} ORDER BY key")
        } else {
            format!("MATCH (n:Item) RETURN n.id AS key, {expression} ORDER BY key")
        };
        let mut read = db.begin_read_transaction().unwrap();
        for value in [0, 77, -3] {
            parameters.insert("score_0".into(), Value::Int(value));
            let mut rows = Vec::new();
            let output = read
                .query_with_params_streaming(
                    &query,
                    &parameters,
                    QueryStreamOptions {
                        max_rows: Some(20),
                        max_payload_bytes: Some(4096),
                    },
                    |row| {
                        rows.push(row);
                        Ok(())
                    },
                )
                .unwrap();
            assert_eq!(rows.len(), 20);
            assert_eq!(rows[0]["score"], Value::Int(value));
            let memory = output.execution_profile.pipeline_memory_report;
            assert_eq!(memory.simple_case_dispatch_tables, 1);
            assert_eq!(memory.simple_case_dispatch_lookups, 20);
            assert_eq!(memory.query_memory_completion_bytes, 0);
        }
        // CASE marker trees are reusable under the physical cache's separate,
        // complete requested-heap budget, including keys and traces.
        let stats = read.plan_cache_stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.bypasses, 0);
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.max_bytes, Some(16 << 20));
        assert_eq!(stats.max_entry_bytes, Some(8 << 20));
        assert!(stats.charged_bytes.unwrap() <= 16 << 20);
        let explain = read.explain_query_with_params(&query, &parameters).unwrap();
        assert_eq!(
            explain.plan_cache_lookup,
            hawdb_plan_cache::PlanCacheLookup::Hit
        );
        assert!(explain.trace.decisions.iter().any(|decision| {
            decision == "parameter binding cache hit: proven logical template"
        }));
    }
}

#[test]
fn physical_case_cache_budget_refusals_preserve_bound_results() {
    for (total, entry) in [(0, 8 << 20), (64, 8 << 20), (16 << 20, 0), (16 << 20, 1)] {
        let mut db = Database::new_with_config(DatabaseConfig {
            max_physical_plan_cache_bytes: total,
            max_physical_plan_cache_entry_bytes: entry,
            ..Default::default()
        });
        db.query("CREATE (:Item {id: 0})").unwrap();
        let mut read = db.begin_read_transaction().unwrap();
        let query = "MATCH (n:Item) RETURN CASE n.id WHEN $id THEN $score ELSE null END AS score";
        for score in [1, 99] {
            let parameters = BTreeMap::from([
                ("id".into(), Value::Int(0)),
                ("score".into(), Value::Int(score)),
            ]);
            let mut rows = Vec::new();
            read.query_with_params_streaming(
                query,
                &parameters,
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(1024),
                },
                |row| {
                    rows.push(row);
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(rows[0]["score"], Value::Int(score));
        }
        let stats = read.plan_cache_stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.admissions, 0);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 2);
        assert_eq!(stats.rejected_admissions, 2);
        assert_eq!(stats.charged_bytes, Some(0));
        assert_eq!(stats.max_bytes, Some(total));
        assert_eq!(stats.max_entry_bytes, Some(entry));
    }
}

#[test]
fn physical_cache_admission_trace_matches_the_reported_outcome() {
    for (entry_limit, expected_admitted) in [(8 << 20, true), (1, false)] {
        let mut db = Database::new_with_config(DatabaseConfig {
            max_physical_plan_cache_entry_bytes: entry_limit,
            ..Default::default()
        });
        db.query("CREATE (:Item {id: 0})").unwrap();
        let mut read = db.begin_read_transaction().unwrap();
        let query = "MATCH (n:Item) RETURN CASE n.id WHEN $id THEN $score ELSE null END AS score";
        let parameters = BTreeMap::from([
            ("id".into(), Value::Int(0)),
            ("score".into(), Value::Int(99)),
        ]);
        let explain = read.explain_query_with_params(query, &parameters).unwrap();
        let admitted = explain
            .trace
            .decisions
            .iter()
            .any(|decision| decision.starts_with("physical plan cache admission accepted:"));
        let refused = explain
            .trace
            .decisions
            .iter()
            .any(|decision| decision.starts_with("physical plan cache admission refused:"));
        assert_eq!(admitted, expected_admitted);
        assert_eq!(refused, !expected_admitted);
        let stats = read.plan_cache_stats();
        assert_eq!(stats.admissions, u64::from(expected_admitted));
        assert_eq!(stats.rejected_admissions, u64::from(!expected_admitted));
        assert_eq!(stats.entries, usize::from(expected_admitted));
        let output = read.query_with_params(query, &parameters).unwrap();
        assert_eq!(output.rows[0]["score"], Value::Int(99));
    }
}

#[test]
fn physical_case_hits_keep_lazy_errors_and_recover_after_failed_reads() {
    let mut db = Database::new();
    db.query("CREATE (:Item {id: 0})").unwrap();
    let mut read = db.begin_read_transaction().unwrap();
    let query = "MATCH (n:Item) RETURN CASE n.id WHEN $id THEN $score ELSE lower(7) END AS score";
    let mut parameters = BTreeMap::from([
        ("id".into(), Value::Int(0)),
        ("score".into(), Value::Int(1)),
    ]);
    // A function-valued CASE branch uses exact parameter variants. Reuse that
    // variant to exercise a physical hit with an unselected failing branch;
    // the slot-eligible CASE test above covers changing score values.
    for score in [77, 77] {
        parameters.insert("score".into(), Value::Int(score));
        let mut rows = Vec::new();
        read.query_with_params_streaming(
            query,
            &parameters,
            QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(1024),
            },
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(rows[0]["score"], Value::Int(score));
    }
    assert_eq!(read.plan_cache_stats().hits, 1);
    parameters.insert("id".into(), Value::Int(1));
    for _ in 0..2 {
        assert!(read
            .query_with_params_streaming(
                query,
                &parameters,
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(1024),
                },
                |_| Ok(())
            )
            .is_err());
    }
    parameters.insert("id".into(), Value::Int(0));
    let mut rows = Vec::new();
    read.query_with_params_streaming(
        query,
        &parameters,
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(1024),
        },
        |row| {
            rows.push(row);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(rows[0]["score"], Value::Int(77));
    assert_eq!(read.plan_cache_stats().hits, 3);
}

#[test]
fn physical_case_cache_hits_preserve_request_budgets_and_cancellation() {
    let mut db = Database::new();
    db.query("CREATE (:Item {id: 0})").unwrap();
    let mut read = db.begin_read_transaction().unwrap();
    let query = "MATCH (n:Item) RETURN CASE n.id WHEN $id THEN $score ELSE null END AS score";
    let parameters = BTreeMap::from([
        ("id".into(), Value::Int(0)),
        ("score".into(), Value::Int(77)),
    ]);
    read.query_with_params(query, &parameters).unwrap();
    assert_eq!(
        read.explain_query_with_params(query, &parameters)
            .unwrap()
            .plan_cache_lookup,
        PlanCacheLookup::Hit
    );
    for options in [
        QueryStreamOptions {
            max_rows: Some(0),
            max_payload_bytes: Some(1024),
        },
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(1),
        },
    ] {
        let error = read
            .query_with_params_streaming(query, &parameters, options, |_| Ok(()))
            .unwrap_err();
        assert!(matches!(error, HawDBError::ReadBudgetExceeded(_)));
    }
    let context = hawdb_core::RuntimeTaskContext::default();
    context.cancellation().cancel();
    let error = read
        .query_with_params_streaming_context(
            query,
            &parameters,
            QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(1024),
            },
            &context,
            |_| panic!("cancelled requests must deliver no rows"),
        )
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert_eq!(read.plan_cache_stats().entries, 1);
    assert_eq!(
        read.query_with_params(query, &parameters).unwrap().rows[0]["score"],
        Value::Int(77)
    );
}
