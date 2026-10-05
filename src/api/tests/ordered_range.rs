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

use super::*;
use hawdb_core::HawDBError;

fn populate(db: &mut Database, rotation: usize) {
    let ranks = ["30", "10", "20", "15.5", "10", "null", "30.0"];
    for id in 0..64 {
        let rank = if id < ranks.len() {
            ranks[(id + rotation) % ranks.len()].to_string()
        } else {
            format!("-{id}")
        };
        db.query(&format!("CREATE (:Item {{id: {id}, rank: {rank}}})"))
            .unwrap();
    }
    db.query("CREATE (:Item {id: 99})").unwrap();
}

fn assert_elision(db: &mut Database, require_index: bool) {
    let params = BTreeMap::from([("a".to_string(), Value::Int(0))]);
    for (projection, order, suffix) in [
        ("n.rank AS rank", "n.rank", ""),
        ("n.rank AS rank", "rank", " SKIP 1 LIMIT 4"),
        ("n.id AS id", "n.rank", " LIMIT 3"),
        ("n", "n.rank", ""),
    ] {
        let query = format!(
            "MATCH (n:Item) WHERE n.rank > $a RETURN {projection} ORDER BY {order}{suffix}"
        );
        let oracle = format!("MATCH (n:Item) WHERE n.rank > $a RETURN {projection} ORDER BY coalesce(n.rank, 0){suffix}");
        let plan = db
            .explain_query_with_params(&query, &params)
            .unwrap()
            .physical_plan;
        let explain = plan.explain(0);
        let indexed = explain.contains("IndexNodeRangeSeek");
        assert!(!require_index || indexed, "{explain}");
        assert_eq!(
            !explain.contains("SortExec") && !explain.contains("TopNExec"),
            indexed,
            "{explain}"
        );
        let expected_plan = db
            .explain_query_with_params(&oracle, &params)
            .unwrap()
            .physical_plan
            .explain(0);
        assert!(
            expected_plan.contains("SortExec") || expected_plan.contains("TopNExec"),
            "{expected_plan}"
        );
        assert_eq!(
            db.query_with_params(&query, &params).unwrap().rows,
            db.query_with_params(&oracle, &params).unwrap().rows,
            "{query}"
        );
    }
    for order in ["n.rank DESC", "n.id ASC", "n.rank ASC, n.id DESC"] {
        let query = format!(
            "MATCH (n:Item) WHERE n.rank > $a RETURN n.rank AS rank, n.id AS id ORDER BY {order}"
        );
        let explain = db
            .explain_query_with_params(&query, &params)
            .unwrap()
            .physical_plan
            .explain(0);
        assert!(explain.contains("SortExec"), "{explain}");
        let oracle = query
            .replace("ORDER BY n.rank", "ORDER BY coalesce(n.rank, 0)")
            .replace("ORDER BY n.id", "ORDER BY coalesce(n.id, 0)");
        assert_eq!(
            db.query_with_params(&query, &params).unwrap().rows,
            db.query_with_params(&oracle, &params).unwrap().rows
        );
    }
}

#[test]
fn ordered_range_query_differential_campaign() {
    for rotation in 0..7 {
        let mut db = Database::new();
        populate(&mut db, rotation);
        db.query("CREATE RANGE INDEX ON :Item(rank)").unwrap();
        assert_elision(&mut db, true);
    }
}

#[test]
fn ordered_range_duplicate_aliases_keep_explicit_sort() {
    let mut db = Database::new();
    populate(&mut db, 0);
    db.query("CREATE RANGE INDEX ON :Item(rank)").unwrap();
    let query =
        "MATCH (n:Item) WHERE n.rank > 0 RETURN n.id AS score, n.rank AS score ORDER BY score";
    let explain = db.explain_query(query).unwrap().physical_plan.explain(0);
    assert!(explain.contains("SortExec"), "{explain}");
    let rows = db.query(query).unwrap().rows;
    let scores: Vec<_> = rows.iter().map(|row| row["score"].clone()).collect();
    assert!(scores.windows(2).all(|pair| pair[0] <= pair[1]));
}

#[test]
fn ordered_range_removes_blocking_sort_memory_for_small_and_bulk_queries() {
    for input_rows in [64, 2048] {
        let mut db = Database::new();
        for id in (1..=input_rows).rev() {
            let rank = if id % 8 == 0 { id } else { -id };
            db.query(&format!("CREATE (:Item {{rank: {rank}}})"))
                .unwrap();
        }
        db.query("CREATE RANGE INDEX ON :Item(rank)").unwrap();
        let query = "MATCH (n:Item) WHERE n.rank > 0 RETURN n.rank AS rank ORDER BY n.rank";
        let oracle =
            "MATCH (n:Item) WHERE n.rank > 0 RETURN n.rank AS rank ORDER BY coalesce(n.rank, 0)";
        let (actual, trace) = db
            .query_with_params_trace_and_external_with_context(
                query,
                &BTreeMap::new(),
                true,
                &mut crate::executor::NoExternalReadOperator,
                None,
                None,
            )
            .unwrap();
        let (expected, sorted_trace) = db
            .query_with_params_trace_and_external_with_context(
                oracle,
                &BTreeMap::new(),
                true,
                &mut crate::executor::NoExternalReadOperator,
                None,
                None,
            )
            .unwrap();
        assert_eq!(actual.rows, expected.rows);
        let trace = trace.execution_profile.unwrap();
        let sorted = sorted_trace.execution_profile.unwrap();
        assert!(trace.blocking_operator_memory_reports.is_empty());
        let sort = sorted
            .blocking_operator_memory_reports
            .iter()
            .find(|report| report.operator == "SortExec")
            .unwrap();
        assert!(sort.peak_tracked_bytes > 0);
        eprintln!("ordered_range input_rows={input_rows} output_rows={} eliminated_sort_peak_bytes={} streaming_query_peak_bytes={} sorted_query_peak_bytes={}",
            actual.rows.len(), sort.peak_tracked_bytes, trace.pipeline_memory_report.query_memory_peak_bytes, sorted.pipeline_memory_report.query_memory_peak_bytes);
    }
}

#[test]
fn ordered_range_query_merges_checkpoint_delta_and_reopens() {
    let path = unique_test_dir("ordered_range_query");
    let config = DatabaseConfig {
        storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        populate(&mut db, 0);
        db.query("CREATE RANGE INDEX ON :Item(rank)").unwrap();
        db.checkpoint().unwrap();
        db.query("MATCH (n:Item) WHERE n.id = 0 SET n.rank = 25")
            .unwrap();
        db.query("MATCH (n:Item) WHERE n.id = 1 DELETE n").unwrap();
        db.query("CREATE (:Item {id: 1, rank: 5})").unwrap();
        db.query("CREATE (:Item {id: 100, rank: 15})").unwrap();
        assert_elision(&mut db, false);
    }
    {
        let mut db = Database::open_with_config(&path, config).unwrap();
        assert_elision(&mut db, false);
        db.checkpoint().unwrap();
        assert_elision(&mut db, false);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn ordered_range_executor_propagates_consumer_errors() {
    let mut db = Database::new();
    populate(&mut db, 0);
    db.query("CREATE RANGE INDEX ON :Item(rank)").unwrap();
    let mut calls = 0;
    let expected = HawDBError::Execution("ordered range consumer stopped".to_string());
    let error = hawdb_executor::store::GraphExecutionRead::visit_nodes_by_property_range_owned(
        &db.runtime.get().unwrap().store,
        db.runtime.get().unwrap().catalog.label_id("Item").unwrap(),
        "rank",
        Some(&(Value::Int(0), false)),
        None,
        &mut |_| {
            calls += 1;
            Err(expected.clone())
        },
    )
    .unwrap_err();
    assert_eq!(error, expected);
    assert_eq!(calls, 1);
}

#[test]
fn ordered_range_cache_and_prepared_plans_track_projection_availability() {
    let path = unique_test_dir("ordered_range_plan_cache");
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
            max_plan_cache_entries: Some(16),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    populate(&mut db, 0);
    db.checkpoint().unwrap();
    // Declaring an index after checkpoint does not construct a canonical
    // projection. Planning must retain Sort even though the schema has it.
    db.query("CREATE RANGE INDEX ON :Item(rank)").unwrap();
    let query = "MATCH (n:Item) WHERE n.rank > $a RETURN n.rank AS rank ORDER BY n.rank";
    let parameters = BTreeMap::from([("a".to_string(), Value::Int(0))]);
    let old = db.runtime_planning_snapshot().unwrap();
    let prepare = || old.prepare(query.to_string(), &parameters).unwrap();
    let (_, cold) = prepare().into_execution(
        &db.runtime.get().unwrap().catalog,
        &db.runtime.get().unwrap().store,
    );
    let (_, warm) = prepare().into_execution(
        &db.runtime.get().unwrap().catalog,
        &db.runtime.get().unwrap().store,
    );
    assert!(cold
        .optimized
        .unwrap()
        .physical_plan
        .explain(0)
        .contains("SortExec"));
    assert_eq!(
        warm.optimized.unwrap().plan_cache_lookup,
        PlanCacheLookup::Hit
    );
    let old_prepared = prepare();
    let epoch = db.runtime.get().unwrap().store.commit_epoch();
    let before = db.query_with_params(query, &parameters).unwrap().rows;
    db.checkpoint().unwrap();
    assert_eq!(db.runtime.get().unwrap().store.commit_epoch(), epoch);
    let (_, stale) = old_prepared.into_execution(
        &db.runtime.get().unwrap().catalog,
        &db.runtime.get().unwrap().store,
    );
    assert!(
        stale.optimized.is_none(),
        "capability change must invalidate a prepared plan without DDL"
    );
    let new = db.runtime_planning_snapshot().unwrap();
    let (_, cold) = new
        .prepare(query.to_string(), &parameters)
        .unwrap()
        .into_execution(
            &db.runtime.get().unwrap().catalog,
            &db.runtime.get().unwrap().store,
        );
    let cold = cold.optimized.unwrap();
    assert_eq!(cold.plan_cache_lookup, PlanCacheLookup::Miss);
    // Plan legality changes independently of costing: missing out-of-core
    // statistics may still select a sequential scan with explicit Sort.
    let explain = cold.physical_plan.explain(0);
    assert_eq!(
        !explain.contains("SortExec"),
        explain.contains("IndexNodeRangeSeek")
    );
    let (_, warm) = new
        .prepare(query.to_string(), &parameters)
        .unwrap()
        .into_execution(
            &db.runtime.get().unwrap().catalog,
            &db.runtime.get().unwrap().store,
        );
    assert_eq!(
        warm.optimized.unwrap().plan_cache_lookup,
        PlanCacheLookup::Hit
    );
    // An older snapshot shares the cache but must never reuse the new plan.
    let (_, old_again) = prepare().into_execution(
        &db.runtime.get().unwrap().catalog,
        &db.runtime.get().unwrap().store,
    );
    assert!(old_again.optimized.is_none());
    assert_eq!(
        db.query_with_params(query, &parameters).unwrap().rows,
        before
    );
    drop(old);
    drop(new);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}
