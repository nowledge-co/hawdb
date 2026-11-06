// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::GraphStatistics;

fn populate(db: &mut Database) {
    db.query("CREATE INDEX ON :Memory(space_id)").unwrap();
    let rows = (0..200)
        .map(|id| {
            Value::Map(BTreeMap::from([
                ("id".into(), Value::Int(id)),
                (
                    "space".into(),
                    Value::String(if id < 190 { "hot" } else { "cold" }.into()),
                ),
                (
                    "score".into(),
                    Value::Int(if id % 2 == 0 { 1000 } else { id }),
                ),
            ]))
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:Memory {id: row.id, space_id: row.space, score: row.score})",
        &BTreeMap::from([("rows".into(), Value::List(rows))]),
    )
    .unwrap();
}

fn space(value: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("space".into(), Value::String(value.into()))])
}

#[test]
fn compressed_histogram_preserves_skew_and_parameter_specific_plan_variants() {
    let mut db = Database::new();
    populate(&mut db);
    let query =
        "MATCH (m:Memory) WHERE m.space_id = $space AND m.score >= 0 RETURN m.id AS id ORDER BY id";
    let stats = db.statistics().unwrap();
    let histogram = stats
        .property_distributions
        .iter()
        .find(|((_, property), _)| property == "space_id")
        .unwrap()
        .1;
    assert!(histogram.is_valid());
    assert!(!histogram.is_sampled());
    assert_eq!(
        histogram
            .equality(&Value::String("hot".into()), 200, Some(2))
            .rows,
        190
    );
    assert_eq!(
        histogram
            .equality(&Value::String("cold".into()), 200, Some(2))
            .rows,
        10
    );
    let hot = db.explain_query_with_params(query, &space("hot")).unwrap();
    let cold = db.explain_query_with_params(query, &space("cold")).unwrap();
    assert_ne!(
        hot.trace.selected_plan_fingerprint, cold.trace.selected_plan_fingerprint,
        "hot={} cold={}",
        hot.trace.selected_plan, cold.trace.selected_plan
    );
    let hot_again = db.explain_query_with_params(query, &space("hot")).unwrap();
    assert_eq!(
        hot.trace.selected_plan_fingerprint,
        hot_again.trace.selected_plan_fingerprint
    );
    assert!(hot_again
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("plan cache hit")));
    for (name, expected) in [
        ("hot", (0..190).collect::<Vec<i64>>()),
        ("cold", (190..200).collect()),
    ] {
        let output = db.query_with_params(query, &space(name)).unwrap();
        let actual = output
            .rows
            .iter()
            .map(|row| row.get("id").cloned().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            expected.into_iter().map(Value::Int).collect::<Vec<_>>()
        );
    }
}

#[test]
fn compressed_histogram_update_delete_and_old_snapshot_keep_separate_populations() {
    let mut db = Database::new();
    populate(&mut db);
    let mut old = db.begin_read_transaction().unwrap();
    let old_statistics = old.statistics();
    db.query("MATCH (m:Memory) WHERE m.id < 100 DELETE m")
        .unwrap();
    db.query("MATCH (m:Memory) WHERE m.space_id = 'cold' SET m.space_id = 'hot'")
        .unwrap();
    let current = db.statistics().unwrap();
    let distribution = |statistics: &GraphStatistics| {
        statistics
            .property_distributions
            .iter()
            .find(|((_, property), _)| property == "space_id")
            .unwrap()
            .1
            .clone()
    };
    assert_eq!(distribution(&old_statistics).population_count, 200);
    assert_eq!(distribution(&current).population_count, 100);
    assert_eq!(
        distribution(&current)
            .equality(&Value::String("hot".into()), 100, Some(1))
            .rows,
        100
    );
    let output = old
        .query("MATCH (m:Memory) WHERE m.space_id = 'cold' RETURN m.id AS id")
        .unwrap();
    assert_eq!(output.rows.len(), 10);
}

#[test]
fn external_compressed_histograms_survive_checkpoint_reopen_and_keep_null_counts() {
    let path = unique_test_dir("compressed_histogram_reopen");
    let config = DatabaseConfig {
        storage_residency_mode: crate::StorageResidencyMode::OutOfCore,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config.clone()).unwrap();
    populate(&mut db);
    db.query("CREATE (:Memory {id: 200, score: NULL})").unwrap();
    db.checkpoint().unwrap();
    db.refresh_optimizer_statistics_external(&crate::OptimizerStatisticsRefreshOptions {
        memory_budget_bytes: 2 * 1024 * 1024,
        max_spill_bytes: 16 * 1024 * 1024,
        max_spill_runs: 256,
        max_input_records: 10_000,
        max_generated_facts: 100_000,
        max_path_expansions: 1000,
        spill_directory: path.join("statistics-spill"),
    })
    .unwrap();
    let before = db.statistics().unwrap();
    let histogram = before
        .property_distributions
        .iter()
        .find(|((_, property), _)| property == "space_id")
        .unwrap()
        .1;
    assert_eq!(histogram.nulls(201).rows, 1);
    db.checkpoint().unwrap();
    drop(db);
    let mut db = Database::open_with_config(&path, config).unwrap();
    assert_eq!(
        before.property_distributions,
        db.statistics().unwrap().property_distributions
    );
    assert_eq!(
        db.query("MATCH (m:Memory) WHERE m.space_id IS NULL RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}
