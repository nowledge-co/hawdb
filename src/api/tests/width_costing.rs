// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn width_costing_tiny_sort_obeys_the_query_memory_budget() {
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory: crate::executor::ExecutionMemoryConfig {
            query_memory_bytes: NonZeroUsize::new(2 * 1024 * 1024).unwrap(),
            ..Default::default()
        },
        ..Default::default()
    });
    for (id, score) in [("a", 3), ("b", 3), ("c", 2), ("d", 1)] {
        db.query_with_params(
            "CREATE (:Memory {id:$id,score:$score})",
            &BTreeMap::from([
                ("id".into(), Value::String(id.into())),
                ("score".into(), Value::Int(score)),
            ]),
        )
        .unwrap();
    }
    let query = "MATCH (m:Memory) RETURN m.id AS id, m.score AS score ORDER BY score DESC, id ASC SKIP 1 LIMIT 2";
    let explain = db.explain_query(query).unwrap();
    assert!(explain.trace.selected_plan.contains("SortExec"));
    assert!(!explain.trace.selected_plan.contains("TopNExec"));
    let output = db.query(query).unwrap();
    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>(),
        vec![Value::String("b".into()), Value::String("c".into())]
    );
}

#[test]
fn width_costing_underestimated_correlated_filter_keeps_bounded_state() {
    let spill = unique_test_dir("width-costing-underestimated-filter");
    std::fs::create_dir_all(&spill).unwrap();
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory: crate::executor::ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(256 * 1024).unwrap(),
            query_memory_bytes: NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
            spill_directory: spill.clone(),
            ..Default::default()
        },
        ..Default::default()
    });
    db.query("CREATE NODE TABLE Item").unwrap();
    for property in ["id", "rank", "a", "b", "c", "d", "e"] {
        db.query(&format!(
            "CREATE PROPERTY ON NODE TABLE Item({property}) TYPE INT NOT NULL"
        ))
        .unwrap();
    }
    db.query("CREATE PROPERTY ON NODE TABLE Item(body) TYPE TEXT")
        .unwrap();
    let body = "x".repeat(4096);
    for start in (0..512).step_by(64) {
        let rows = (start..start + 64)
            .map(|id| {
                Value::Map(BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("rank".into(), Value::Int(id % 17)),
                    ("flag".into(), Value::Int(id % 2)),
                    ("body".into(), Value::String(body.clone())),
                ]))
            })
            .collect();
        db.query_with_params(
            "UNWIND $rows AS r CREATE (:Item {id:r.id,rank:r.rank,body:r.body,a:r.flag,b:r.flag,c:r.flag,d:r.flag,e:r.flag})",
            &BTreeMap::from([("rows".into(), Value::List(rows))]),
        ).unwrap();
    }
    let query = "MATCH (n:Item) WHERE n.a=0 AND n.b=0 AND n.c=0 AND n.d=0 AND n.e=0 RETURN n.id AS id,n.rank AS rank,n.body AS body ORDER BY rank,id SKIP 3 LIMIT 16";
    let explain = db.explain_query(query).unwrap();
    let selected = &explain.trace.selected_plan;
    assert!(selected.contains("TopNExec"), "{selected}");
    let input_rows = explain
        .trace
        .selected_plan_cardinality_estimates
        .iter()
        .find(|entry| entry.operator.as_str() == "NodeProjectionScanExec")
        .unwrap()
        .estimated_rows;
    assert!(input_rows <= 19, "{input_rows}");
    let mut ids = (0..512).step_by(2).collect::<Vec<_>>();
    ids.sort_by_key(|id| (id % 17, *id));
    let expected = ids
        .into_iter()
        .skip(3)
        .take(16)
        .map(|id| {
            BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("rank".into(), Value::Int(id % 17)),
                ("body".into(), Value::String(body.clone())),
            ])
        })
        .collect::<Vec<_>>();
    let mut read = db.begin_read_transaction().unwrap();
    for run in 0..2 {
        let before = read.plan_cache_stats();
        let mut rows = Vec::new();
        let report = read
            .query_request_streaming(crate::QueryRequest::new(query), |row| {
                rows.push(row);
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, expected);
        if run == 1 {
            assert_eq!(read.plan_cache_stats().hits - before.hits, 1);
        }
        let top_n = report
            .execution_profile
            .blocking_operator_memory_reports
            .iter()
            .find(|entry| entry.operator == "TopNExec")
            .unwrap();
        assert_eq!(top_n.input_rows, 256);
        assert_eq!(top_n.spilled_bytes, 0);
        assert_eq!(top_n.spill_run_count, 0);
        assert!(top_n.peak_tracked_bytes <= 256 * 1024);
        assert_eq!(
            report
                .execution_profile
                .pipeline_memory_report
                .query_memory_completion_bytes,
            0
        );
    }
    drop(read);
    drop(db);
    std::fs::remove_dir_all(spill).unwrap();
}

fn width_fixture(config: DatabaseConfig, path: Option<&std::path::Path>) -> Database {
    let mut db = match path {
        Some(path) => Database::open_with_config(path, config).unwrap(),
        None => Database::new_with_config(config),
    };
    db.query("CREATE NODE TABLE Item").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(id) TYPE INT NOT NULL")
        .unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(rank) TYPE INT NOT NULL")
        .unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(body) TYPE TEXT")
        .unwrap();
    db.query("CREATE (:Item {id:0,rank:0})").unwrap();
    for start in (1..320).step_by(64) {
        let rows = (start..(start + 64).min(320))
            .map(|id| {
                Value::Map(BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("rank".into(), Value::Int(id % 17)),
                    (
                        "body".into(),
                        if id % 31 == 0 {
                            Value::Null
                        } else {
                            Value::String("x".repeat(4096))
                        },
                    ),
                ]))
            })
            .collect();
        db.query_with_params(
            "UNWIND $rows AS r CREATE (:Item {id:r.id,rank:r.rank,body:r.body})",
            &BTreeMap::from([("rows".into(), Value::List(rows))]),
        )
        .unwrap();
    }
    db
}

fn width_reference() -> Vec<BTreeMap<String, Value>> {
    let mut ids = (0..320).collect::<Vec<_>>();
    ids.sort_by_key(|id| (id % 17, *id));
    ids.into_iter()
        .take(64)
        .map(|id| {
            BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("rank".into(), Value::Int(id % 17)),
                (
                    "body".into(),
                    if id == 0 || id % 31 == 0 {
                        Value::Null
                    } else {
                        Value::String("x".repeat(4096))
                    },
                ),
            ])
        })
        .collect()
}

const WIDTH_QUERY: &str="MATCH (n:Item) RETURN n.rank AS rank, n.id AS id, n.body AS body ORDER BY rank, id LIMIT $limit";

fn verify_width_reader(
    read: &mut crate::DatabaseReadTransaction,
    expected: &[BTreeMap<String, Value>],
) {
    let params = BTreeMap::from([("limit".into(), Value::Int(64))]);
    for run in 0..2 {
        let before = read.plan_cache_stats();
        let mut rows = Vec::new();
        let report = read
            .query_request_streaming(
                crate::QueryRequest::new(WIDTH_QUERY)
                    .with_params(&params)
                    .with_output_limits(crate::QueryStreamOptions {
                        max_rows: Some(64),
                        max_payload_bytes: Some(64 * 5120),
                    }),
                |row| {
                    rows.push(row);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(rows, expected);
        assert!(report.fully_streamed);
        assert_eq!(
            report
                .execution_profile
                .pipeline_memory_report
                .query_memory_completion_bytes,
            0
        );
        if run == 1 {
            assert_eq!(read.plan_cache_stats().hits - before.hits, 1);
        }
    }
}

#[test]
fn width_costing_preserves_nulls_ties_pinned_readers_and_warm_templates_under_pressure() {
    for max_groups in [0, 128] {
        for memory_mib in [1, 2, 8] {
            let config = DatabaseConfig {
                max_optimizer_groups: Some(max_groups),
                execution_memory: crate::executor::ExecutionMemoryConfig {
                    blocking_operator_bytes: NonZeroUsize::new(memory_mib * 1024 * 1024).unwrap(),
                    query_memory_bytes: NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
                    ..Default::default()
                },
                ..Default::default()
            };
            let mut db = width_fixture(config, None);
            let params = BTreeMap::from([("limit".into(), Value::Int(64))]);
            let explain = db.explain_query_with_params(WIDTH_QUERY, &params).unwrap();
            assert_eq!(
                explain.trace.selected_plan.contains("TopNExec"),
                memory_mib == 1,
                "{}",
                explain.trace.selected_plan
            );
            assert!(explain
                .trace
                .decisions
                .iter()
                .any(|entry| entry.starts_with("physical width cost: ")
                    && entry.contains("units=logical_estimates")));
            let mut pinned = db.begin_read_transaction().unwrap();
            let expected = width_reference();
            verify_width_reader(&mut pinned, &expected);
            db.query("MATCH (n:Item) WHERE n.id=0 SET n.body='changed'")
                .unwrap();
            verify_width_reader(&mut pinned, &expected);
            let mut current = db.begin_read_transaction().unwrap();
            let mut changed = expected;
            changed[0].insert("body".into(), Value::String("changed".into()));
            verify_width_reader(&mut current, &changed);
        }
    }
}

#[test]
fn width_costing_keeps_full_results_across_checkpoint_and_all_residency_modes() {
    for mode in [
        crate::StorageResidencyMode::Materialized,
        crate::StorageResidencyMode::OutOfCore,
        crate::StorageResidencyMode::Auto,
    ] {
        let path = unique_test_dir("width-costing");
        let mut config = DatabaseConfig {
            storage_residency_mode: crate::StorageResidencyMode::Materialized,
            execution_memory: crate::executor::ExecutionMemoryConfig {
                blocking_operator_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut db = width_fixture(config.clone(), Some(&path));
        db.checkpoint().unwrap();
        drop(db);
        config.storage_residency_mode = mode;
        let db = Database::open_with_config(&path, config).unwrap();
        let mut read = db.begin_read_transaction().unwrap();
        verify_width_reader(&mut read, &width_reference());
        drop(read);
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}
