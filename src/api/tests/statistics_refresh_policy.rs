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

//! Optimizer-statistics refresh policy of resident stores (#2017). Small
//! graphs recompute exact statistics per plan; larger graphs plan from cached
//! distributions that the bound-crossing commit, amortized commit refreshes
//! (non-durable stores) or the background statistics task (durable stores)
//! refresh. Planning never recomputes above the bound.

use super::*;
use crate::store::{
    OptimizerStatisticsMaintenanceReport, OptimizerStatisticsPath, SYNC_STATISTICS_MAX_RECORDS,
};
use hawdb_qos::BackgroundWorkHint;

fn report(db: &Database) -> OptimizerStatisticsMaintenanceReport {
    db.optimizer_statistics_maintenance_report().unwrap()
}

fn rows(rows: impl Iterator<Item = BTreeMap<String, Value>>) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "rows".to_string(),
        Value::List(rows.map(Value::Map).collect()),
    )])
}

fn create_nodes(db: &mut Database, label: &str, from: i64, to_exclusive: i64) {
    let nodes = (from..to_exclusive).map(|id| {
        BTreeMap::from([
            ("id".to_string(), Value::Int(id)),
            ("g".to_string(), Value::Int(id % 7)),
        ])
    });
    db.query_with_params(
        &format!("UNWIND $rows AS r CREATE (:{label} {{id: r.id, g: r.g, v: 0}})"),
        &rows(nodes),
    )
    .unwrap();
}

fn refresh_options(
    spill_directory: std::path::PathBuf,
) -> crate::OptimizerStatisticsRefreshOptions {
    crate::OptimizerStatisticsRefreshOptions {
        memory_budget_bytes: 1024 * 1024,
        max_spill_bytes: 64 * 1024 * 1024,
        max_spill_runs: 64,
        max_input_records: 1_000_000,
        max_generated_facts: 10_000_000,
        max_path_expansions: 1_000_000,
        spill_directory,
    }
}

fn count(db: &mut Database, query: &str) -> i64 {
    match db.query(query).unwrap().rows[0]["c"] {
        Value::Int(count) => count,
        ref other => panic!("count returned {other:?}"),
    }
}

fn distinct_count(statistics: &crate::schema::GraphStatistics, property: &str) -> Option<u64> {
    statistics
        .property_distinct_counts
        .iter()
        .find(|((_, name), _)| name == property)
        .map(|(_, count)| *count)
}

#[test]
fn crossing_the_bound_publishes_distributions_once_and_planning_stops_recomputing() {
    assert_eq!(SYNC_STATISTICS_MAX_RECORDS, 1_000);
    let mut db = Database::new();
    create_nodes(&mut db, "P", 0, 999);
    let small = report(&db);
    assert_eq!(small.path, OptimizerStatisticsPath::SynchronousSmallGraph);
    assert_eq!(small.committed_records, 999);
    assert_eq!(
        count(&mut db, "MATCH (p:P) WHERE p.g = 3 RETURN count(*) AS c"),
        143
    );
    assert!(report(&db).recomputes.planning > small.recomputes.planning);
    assert_eq!(report(&db).recomputes.commit, 0);

    create_nodes(&mut db, "P", 999, 1_001);
    let crossed = report(&db);
    assert_eq!(crossed.path, OptimizerStatisticsPath::Cached);
    assert_eq!(crossed.committed_records, 1_001);
    assert_eq!(crossed.recomputes.commit, 1);
    assert_eq!(crossed.commit_lag, 0);
    assert_eq!(crossed.dirty_mutation_operations, 0);
    let statistics = db.statistics().unwrap();
    assert!(statistics.advanced_statistics_complete);
    assert_eq!(distinct_count(&statistics, "id"), Some(1_001));
    assert_eq!(distinct_count(&statistics, "g"), Some(7));
    assert!(!statistics.property_histograms.is_empty());

    for round in 0..40_i64 {
        let g = round % 7;
        let expected = (0..1_001 + round).filter(|id| id % 7 == g).count() as i64;
        assert_eq!(
            count(
                &mut db,
                &format!("MATCH (p:P) WHERE p.g = {g} RETURN count(*) AS c")
            ),
            expected
        );
        create_nodes(&mut db, "P", 1_001 + round, 1_002 + round);
        assert_eq!(
            count(
                &mut db,
                &format!(
                    "MATCH (p:P) WHERE p.id = {} RETURN count(*) AS c",
                    1_001 + round
                )
            ),
            1
        );
    }
    let after = report(&db);
    assert_eq!(after.recomputes.planning, crossed.recomputes.planning);
    assert_eq!(after.recomputes.commit, 1);
    assert_eq!(after.dirty_mutation_operations, 40);
    assert_eq!(after.commit_lag, 40);
}

#[test]
fn non_durable_growth_refreshes_statistics_at_commit_not_at_planning() {
    let mut db = Database::new();
    create_nodes(&mut db, "N", 0, 500);
    assert_eq!(
        report(&db).path,
        OptimizerStatisticsPath::SynchronousSmallGraph
    );
    let mut next = 500;
    let mut planning_after_crossing = None;
    while next < 50_000 {
        create_nodes(&mut db, "N", next, next + 5_000);
        next += 5_000;
        let current = report(&db);
        assert_eq!(current.path, OptimizerStatisticsPath::Cached);
        let planning = *planning_after_crossing.get_or_insert(current.recomputes.planning);
        assert_eq!(
            count(&mut db, "MATCH (n:N) WHERE n.g = 2 RETURN count(*) AS c"),
            (0..next).filter(|id| id % 7 == 2).count() as i64
        );
        let statistics = db.statistics().unwrap();
        assert_eq!(statistics.node_count, next as u64);
        assert_eq!(
            statistics
                .label_counts
                .values()
                .copied()
                .collect::<Vec<_>>(),
            vec![next as u64]
        );
        assert_eq!(report(&db).recomputes.planning, planning);
    }
    let grown = report(&db);
    assert_eq!(grown.committed_records, 50_500);
    // The crossing commit and the amortized refreshes while every batch
    // exceeded max(1000, 10% of records).
    assert!(grown.recomputes.commit >= 2, "{grown:?}");
    assert!(grown.dirty_mutation_operations <= grown.refresh_dirty_operation_threshold);
    let distinct_ids = distinct_count(&db.statistics().unwrap(), "id").unwrap();
    assert!(distinct_ids >= 10_000, "{distinct_ids}");
    assert_eq!(grown.recomputes.background, 0);
}

#[test]
fn explicit_transaction_writes_do_not_recompute_statistics() {
    let mut db = Database::new();
    create_nodes(&mut db, "M", 0, 2_000);
    db.query("CREATE INDEX ON :M(id)").unwrap();
    let before = report(&db);
    assert_eq!(before.path, OptimizerStatisticsPath::Cached);

    let mut transaction = db.begin_transaction().unwrap();
    for id in 0..256_i64 {
        transaction
            .query_with_params(
                "MATCH (m:M {id: $id}) SET m.v = $v",
                &BTreeMap::from([
                    ("id".to_string(), Value::Int(id * 7)),
                    ("v".to_string(), Value::Int(id + 1)),
                ]),
            )
            .unwrap();
    }
    transaction.commit().unwrap();

    let after = report(&db);
    assert_eq!(after.recomputes.planning, before.recomputes.planning);
    assert_eq!(after.recomputes.commit, before.recomputes.commit);
    // Staged writes count once, when the transaction commits.
    assert_eq!(
        after.dirty_mutation_operations,
        before.dirty_mutation_operations + 256
    );
    assert_eq!(after.commit_lag, before.commit_lag + 1);
    assert_eq!(
        count(&mut db, "MATCH (m:M) WHERE m.v > 0 RETURN count(*) AS c"),
        256
    );
    assert_eq!(
        db.query("MATCH (m:M {id: 1785}) RETURN m.v AS v")
            .unwrap()
            .rows[0]["v"],
        Value::Int(256)
    );
}

#[test]
fn durable_resident_store_defers_refresh_to_the_background_task() {
    let path = unique_test_dir("statistics_refresh_policy_durable");
    let spill_root = path.join("statistics-spill");
    let mut db = Database::open(&path).unwrap();
    assert!(!db.storage_residency_report().unwrap().out_of_core);
    create_nodes(&mut db, "D", 0, 2_000);
    let crossed = report(&db);
    assert_eq!(crossed.path, OptimizerStatisticsPath::Cached);
    assert_eq!(crossed.recomputes.commit, 1);
    assert!(db
        .optimizer_statistics_refresh_background_work_plan(BackgroundWorkHint::default())
        .unwrap()
        .is_none());

    db.query("MATCH (d:D) WHERE d.id < 1500 SET d.v = d.id % 3 + 1")
        .unwrap();
    let dirty = report(&db);
    assert_eq!(dirty.recomputes.commit, crossed.recomputes.commit);
    assert_eq!(dirty.recomputes.planning, crossed.recomputes.planning);
    assert_eq!(dirty.dirty_mutation_operations, 1_500);
    assert!(dirty.dirty_mutation_operations > dirty.refresh_dirty_operation_threshold);
    assert!(dirty.commit_lag > 0);
    let plan = db
        .optimizer_statistics_refresh_background_work_plan(BackgroundWorkHint::default())
        .unwrap()
        .unwrap();
    assert_eq!(plan.hint.recent_delta_operations, 1_500);
    assert_eq!(distinct_count(&db.statistics().unwrap(), "v"), Some(1));

    // The default policy caps background tasks at 1024 estimated operations;
    // a statistics refresh is charged at most one such slot.
    assert_eq!(plan.request.estimated_operations, 1_024);
    let refreshed = db
        .refresh_background_optimizer_statistics(
            &hawdb_qos::LocalQosPolicy::default(),
            &hawdb_qos::LocalQosState::default(),
            &refresh_options(spill_root),
            BackgroundWorkHint::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(refreshed.node_records_read, 2_000);
    assert!(!refreshed.checkpoint_persisted);
    let after = report(&db);
    assert_eq!(
        after.recomputes.background,
        crossed.recomputes.background + 1
    );
    assert_eq!(after.recomputes.commit, crossed.recomputes.commit);
    assert_eq!(after.commit_lag, 0);
    assert_eq!(after.dirty_mutation_operations, 0);
    assert_eq!(distinct_count(&db.statistics().unwrap(), "v"), Some(4));
    assert!(db
        .optimizer_statistics_refresh_background_work_plan(BackgroundWorkHint::default())
        .unwrap()
        .is_none());
    assert_eq!(
        count(&mut db, "MATCH (d:D) WHERE d.v = 2 RETURN count(*) AS c"),
        500
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn stale_and_refreshed_statistics_produce_identical_results() {
    fn load(db: &mut Database) {
        db.query("CREATE INDEX ON :S(id)").unwrap();
        create_nodes(db, "S", 0, 1_200);
        create_nodes(db, "T", 0, 300);
        let edges = (0..1_200_i64).map(|id| {
            BTreeMap::from([
                ("s".to_string(), Value::Int(id)),
                ("t".to_string(), Value::Int(id % 300)),
                ("w".to_string(), Value::Int(id % 5)),
            ])
        });
        db.query_with_params(
            "UNWIND $rows AS r MATCH (s:S {id: r.s}), (t:T {id: r.t}) CREATE (s)-[:R {w: r.w}]->(t)",
            &rows(edges),
        )
        .unwrap();
    }
    let writes = [
        "MATCH (s:S) WHERE s.id < 600 SET s.g = 100 + s.id % 3",
        "MATCH (s:S {id: 7}) SET s.v = 42",
        "MATCH (t:T) WHERE t.id % 2 = 0 SET t.g = 9",
        "UNWIND $rows AS r CREATE (:S {id: r.id, g: 100, v: 1})",
        "MATCH (s:S)-[r:R]->(t:T) WHERE t.g = 9 AND s.g = 101 SET r.w = 50",
    ];
    let reads = [
        "MATCH (s:S) WHERE s.g = 100 RETURN count(*) AS c",
        "MATCH (s:S {id: 7}) RETURN s.v AS v, s.g AS g",
        "MATCH (s:S)-[r:R]->(t:T) WHERE t.g = 9 RETURN s.g AS g, sum(r.w) AS w ORDER BY g",
        "MATCH (s:S)-[:R]->(t:T) WHERE s.id >= 1100 RETURN s.id AS s, t.id AS t ORDER BY s",
        "MATCH (s:S) WHERE s.g > 99 AND s.v = 1 RETURN count(*) AS c",
    ];
    let mut stale = Database::new();
    let mut fresh = Database::new();
    load(&mut stale);
    load(&mut fresh);
    fresh.refresh_optimizer_statistics_now().unwrap();
    let created =
        rows((5_000..5_100_i64).map(|id| BTreeMap::from([("id".to_string(), Value::Int(id))])));
    for write in writes {
        assert_eq!(
            stale.query_with_params(write, &created).unwrap().rows,
            fresh.query_with_params(write, &created).unwrap().rows,
            "{write}"
        );
        fresh.refresh_optimizer_statistics_now().unwrap();
        assert_eq!(report(&fresh).commit_lag, 0);
        for read in reads {
            assert_eq!(
                stale.query(read).unwrap().rows,
                fresh.query(read).unwrap().rows,
                "{read} after {write}"
            );
        }
    }
    let stale_report = report(&stale);
    assert_eq!(stale_report.path, OptimizerStatisticsPath::Cached);
    assert!(stale_report.commit_lag > 0);
    assert!(stale_report.dirty_mutation_operations > 0);
    assert_eq!(stale_report.recomputes.forced, 0);
    assert_eq!(report(&fresh).recomputes.forced, 1 + writes.len() as u64);
    assert_eq!(
        stale_report.recomputes.planning,
        report(&fresh).recomputes.planning
    );
}

#[test]
fn default_policy_refreshes_a_large_durable_resident_store() {
    let path = unique_test_dir("statistics_refresh_policy_default_resident");
    let spill_root = path.join("statistics-spill");
    let mut db = Database::open(&path).unwrap();
    for batch in 0..5_i64 {
        create_nodes(&mut db, "D", batch * 10_000, (batch + 1) * 10_000);
    }
    let due = report(&db);
    assert!(!db.storage_residency_report().unwrap().out_of_core);
    assert_eq!(due.committed_records, 50_000);
    assert_eq!(due.recomputes.commit, 1);
    assert!(due.dirty_mutation_operations > due.refresh_dirty_operation_threshold);
    assert!(due.commit_lag > 0);
    assert_eq!(due.last_refresh_write_guard_hold, None);
    assert_eq!(
        distinct_count(&db.statistics().unwrap(), "id"),
        Some(10_000)
    );

    let scheduler = db.local_qos_scheduler();
    assert_eq!(scheduler.policy(), &hawdb_qos::LocalQosPolicy::default());
    let refreshed = db
        .refresh_scheduled_background_optimizer_statistics(
            &refresh_options(spill_root),
            BackgroundWorkHint::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(refreshed.node_records_read, 50_000);
    assert_eq!(scheduler.state().running_background_operations, 0);
    let after = report(&db);
    assert_eq!(after.recomputes.background, due.recomputes.background + 1);
    assert_eq!(after.recomputes.commit, due.recomputes.commit);
    assert_eq!(after.commit_lag, 0);
    assert_eq!(after.dirty_mutation_operations, 0);
    assert_eq!(after.background_refresh_refused, 0);
    assert_eq!(after.background_refresh_skipped, 0);
    let hold = after.last_refresh_write_guard_hold.unwrap();
    assert!(hold > std::time::Duration::ZERO);
    assert!(after.max_refresh_write_guard_hold >= hold);
    assert_eq!(
        distinct_count(&db.statistics().unwrap(), "id"),
        Some(50_000)
    );
    assert!(db
        .optimizer_statistics_refresh_background_work_plan(BackgroundWorkHint::default())
        .unwrap()
        .is_none());
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn default_policy_refreshes_an_out_of_core_store_above_the_per_task_cap() {
    let path = unique_test_dir("statistics_refresh_policy_default_out_of_core");
    let spill_root = path.join("statistics-spill");
    let config = DatabaseConfig {
        storage_residency_mode: crate::StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: 1024 * 1024,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config).unwrap();
    create_nodes(&mut db, "O", 0, 3_000);
    db.checkpoint().unwrap();
    assert!(db.storage_residency_report().unwrap().out_of_core);
    let due = report(&db);
    assert_eq!(due.committed_records, 3_000);
    let plan = db
        .optimizer_statistics_refresh_background_work_plan(BackgroundWorkHint::default())
        .unwrap()
        .unwrap();
    assert_eq!(plan.request.estimated_operations, 1_024);

    let refreshed = db
        .refresh_scheduled_background_optimizer_statistics(
            &refresh_options(spill_root),
            BackgroundWorkHint::default(),
        )
        .unwrap()
        .unwrap();
    assert!(refreshed.checkpoint_persisted);
    // Node and bounded-path passes each read every node.
    assert!(refreshed.node_records_read >= 3_000);
    let after = report(&db);
    assert_eq!(after.recomputes.background, due.recomputes.background + 1);
    assert_eq!(after.background_refresh_refused, 0);
    assert!(after.last_refresh_write_guard_hold.is_some());
    assert_eq!(
        db.statistics()
            .unwrap()
            .advanced_statistics_freshness(db.runtime.get().unwrap().store.commit_epoch()),
        crate::AdvancedStatisticsFreshness::Fresh
    );
    assert!(db
        .optimizer_statistics_refresh_background_work_plan(BackgroundWorkHint::default())
        .unwrap()
        .is_none());
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn unavailable_memory_admission_skips_the_refresh_without_waiting() {
    let path = unique_test_dir("statistics_refresh_policy_memory_skip");
    let spill_root = path.join("statistics-spill");
    let mut db = Database::open(&path).unwrap();
    create_nodes(&mut db, "D", 0, 2_000);
    db.query("MATCH (d:D) WHERE d.id < 1500 SET d.v = 1")
        .unwrap();
    let governor = hawdb_qos::RuntimeGovernor::detect(
        hawdb_qos::RuntimeGovernorConfig {
            memory_budget_bytes: Some(64 * 1024 * 1024),
            ..hawdb_qos::RuntimeGovernorConfig::shared_host()
        },
        hawdb_qos::IoConcurrencyBudget::new(4, 2),
    );
    governor.pin_resources();
    db.set_runtime_governor(governor);
    let due = report(&db);
    let scheduler = db.local_qos_scheduler();
    let oversized = crate::OptimizerStatisticsRefreshOptions {
        memory_budget_bytes: 1 << 40,
        ..refresh_options(spill_root.clone())
    };

    let started = std::time::Instant::now();
    assert_eq!(
        db.refresh_scheduled_background_optimizer_statistics(
            &oversized,
            BackgroundWorkHint::default()
        )
        .unwrap(),
        None
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert_eq!(scheduler.state().running_background_operations, 0);
    let skipped = report(&db);
    assert_eq!(skipped.background_refresh_skipped, 1);
    assert_eq!(skipped.background_refresh_refused, 0);
    assert_eq!(skipped.recomputes.background, due.recomputes.background);
    assert_eq!(
        skipped.dirty_mutation_operations,
        due.dirty_mutation_operations
    );
    assert_eq!(skipped.last_refresh_write_guard_hold, None);

    // A later pass with an admissible budget performs the refresh.
    db.refresh_scheduled_background_optimizer_statistics(
        &refresh_options(spill_root),
        BackgroundWorkHint::default(),
    )
    .unwrap()
    .unwrap();
    let retried = report(&db);
    assert_eq!(retried.recomputes.background, due.recomputes.background + 1);
    assert_eq!(retried.dirty_mutation_operations, 0);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}
