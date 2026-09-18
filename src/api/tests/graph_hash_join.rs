use super::*;
use crate::executor::ExecutionMemoryConfig;
use std::num::{NonZeroU64, NonZeroUsize};

#[test]
fn graph_hash_join_facade_preserves_residuals_parameters_profiles_and_snapshot() {
    for budget in [128 * 1024, 8 * 1024 * 1024] {
        let directory = unique_test_dir("graph-hash-join-spill");
        let mut db = Database::new_with_config(DatabaseConfig {
            execution_memory: ExecutionMemoryConfig {
                blocking_operator_bytes: NonZeroUsize::new(budget).unwrap(),
                batch_rows: NonZeroUsize::new(7).unwrap(),
                spill_directory: directory.clone(),
                min_spill_free_bytes: NonZeroU64::MIN,
                ..ExecutionMemoryConfig::default()
            },
            ..DatabaseConfig::default()
        });
        for label in ["Left", "Right"] {
            for id in 0..192 {
                db.query_with_params(
                    &format!(
                    "CREATE (:{label} {{id: $id, key: $key, parity: $parity, payload: $payload}})"
                ),
                    &BTreeMap::from([
                        ("id".into(), Value::Int(id)),
                        ("key".into(), Value::Int(id % 31)),
                        ("parity".into(), Value::Int(id % 2)),
                        ("payload".into(), Value::String("x".repeat(256))),
                    ]),
                )
                .unwrap();
            }
        }
        let query = "MATCH (a:Left), (b:Right) WHERE a.key = b.key AND a.parity = b.parity AND a.id >= $minimum RETURN a.id AS a, b.id AS b";
        let expected = |minimum| {
            (minimum..192)
                .flat_map(|a| {
                    (0..192)
                        .filter(move |b| a % 31 == b % 31 && a % 2 == b % 2)
                        .map(move |b| (Value::Int(a), Value::Int(b)))
                })
                .collect::<Vec<_>>()
        };
        for minimum in [0, 70, 170, 20] {
            let parameters = BTreeMap::from([("minimum".into(), Value::Int(minimum))]);
            let output = db.query_with_params(query, &parameters).unwrap();
            let mut actual = output
                .rows
                .iter()
                .map(|row| (row.get("a").unwrap().clone(), row.get("b").unwrap().clone()))
                .collect::<Vec<_>>();
            actual.sort();
            assert_eq!(actual, expected(minimum));
        }
        let profile = db
            .explain_analyze_query_with_params(
                query,
                &BTreeMap::from([("minimum".into(), Value::Int(0))]),
            )
            .unwrap();
        assert!(profile.physical_plan.explain(0).contains("HashJoinExec"));
        assert!(!profile
            .physical_plan
            .explain(0)
            .contains("NodeCartesianProductExec"));
        let cardinality = profile
            .execution_profile
            .operator_cardinality_profiles
            .iter()
            .find(|report| report.operator.as_str() == "HashJoinExec")
            .unwrap();
        assert!(cardinality.actual_rows.unwrap() < 192 * 192);
        let memory = profile
            .execution_profile
            .blocking_operator_memory_reports
            .iter()
            .find(|report| report.operator == "HashJoinExec")
            .unwrap();
        assert_eq!(memory.spilled_bytes > 0, budget == 128 * 1024);
        assert!(memory.peak_tracked_bytes <= memory.budget_bytes);

        let mut snapshot = db.begin_read_transaction();
        db.query("CREATE (:Right {id: 999, key: 0, parity: 0})")
            .unwrap();
        let parameters = BTreeMap::from([("minimum".into(), Value::Int(0))]);
        assert_eq!(
            snapshot
                .query_with_params(query, &parameters)
                .unwrap()
                .rows
                .len(),
            expected(0).len()
        );
        assert!(db.query_with_params(query, &parameters).unwrap().rows.len() > expected(0).len());
        drop(snapshot);
        drop(db);
        if directory.exists() {
            std::fs::remove_dir(directory).unwrap();
        }
    }
}

#[cfg(feature = "acl")]
#[test]
fn graph_hash_join_facade_preserves_both_input_visibility_and_policy_cache_identity() {
    use crate::{QueryAccessControlContext, RuntimeCapabilities, RuntimeCapability};
    let mut db = Database::new_with_config(DatabaseConfig {
        runtime_capabilities: RuntimeCapabilities::default()
            .with(RuntimeCapability::AccessControl, true),
        ..DatabaseConfig::default()
    });
    for label in ["Left", "Right"] {
        for space in ["one", "two"] {
            db.query(&format!(
                "CREATE (:{label} {{key: 7, space_id: '{space}'}})"
            ))
            .unwrap();
        }
    }
    let query =
        "MATCH (a:Left), (b:Right) WHERE a.key = b.key RETURN a.space_id AS a, b.space_id AS b";
    for (epoch, space) in [(1, "one"), (1, "two"), (2, "one")] {
        let output = db
            .explain_analyze_query_with_params_access_control(
                query,
                &BTreeMap::new(),
                QueryAccessControlContext::visibility_scope(epoch, "space_id", space),
            )
            .unwrap();
        assert!(output.physical_plan.explain(0).contains("HashJoinExec"));
        assert_eq!(output.output.rows.len(), 1);
        for name in ["a", "b"] {
            assert_eq!(
                output.output.rows[0].get(name),
                Some(&Value::String(space.into()))
            );
        }
    }
}
