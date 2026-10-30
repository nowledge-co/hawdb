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
        // CASE marker trees must not escape the logical cache's byte budget
        // through a separate count-only physical cache.
        let stats = read.plan_cache_stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.bypasses, 3);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        let explain = read.explain_query_with_params(&query, &parameters).unwrap();
        assert_eq!(
            explain.plan_cache_lookup,
            hawdb_plan_cache::PlanCacheLookup::Bypass(
                hawdb_plan_cache::PlanCacheBypassReason::ParameterizedCaseTemplate
            )
        );
        assert!(explain.trace.decisions.iter().any(|decision| {
            decision == "parameter binding cache hit: proven logical template"
        }));
    }
}
