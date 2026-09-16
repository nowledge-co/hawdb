use crate::{
    Database, DatabaseConfig, QueryStreamOptions, RelationalJoinPlanningDirective,
    RelationalJoinPlanningReason, RelationalJoinPlanningStrategy, Value,
};
use skein_optimizer::RelationalOperatorKind;

#[test]
fn cost_based_reordering_selects_hash_join_and_matches_syntax_order() {
    assert_costed_algorithm(false, RelationalOperatorKind::HashJoin, false);
}

#[test]
fn cost_based_reordering_selects_merge_join_with_compatible_indexes() {
    assert_costed_algorithm(true, RelationalOperatorKind::MergeJoin, false);
}

#[test]
fn costed_hash_subtree_spills_within_the_existing_budget() {
    assert_costed_algorithm(false, RelationalOperatorKind::HashJoin, true);
}

fn assert_costed_algorithm(indexed: bool, expected: RelationalOperatorKind, spill: bool) {
    let mut config = DatabaseConfig::default();
    let directory = super::constrained_hash_join_memory()
        .spill_directory
        .with_extension(format!("costed-{indexed}-{spill}"));
    if spill {
        config.execution_memory.blocking_operator_bytes =
            std::num::NonZeroUsize::new(4096).unwrap();
        config.execution_memory.min_spill_free_bytes = std::num::NonZeroU64::MIN;
        config.execution_memory.spill_directory = directory.clone();
    }
    let mut database = Database::new_with_config(config);
    for (table, count) in [("costed_a", 96), ("costed_b", 64), ("costed_c", 8)] {
        database
            .query_sql(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, k BIGINT)"
            ))
            .unwrap();
        let values = (0..count)
            .map(|id| format!("({id}, {})", id % 8))
            .collect::<Vec<_>>()
            .join(", ");
        database
            .query_sql(&format!("INSERT INTO {table} (id, k) VALUES {values}"))
            .unwrap();
    }
    if indexed {
        for table in ["costed_a", "costed_b"] {
            database
                .query_sql(&format!("CREATE INDEX {table}_key ON {table} (k)"))
                .unwrap();
        }
    }
    let sql = "SELECT a.id AS a_id, b.id AS b_id, c.id AS c_id \
               FROM costed_c AS c \
               JOIN costed_a AS a ON a.k = c.id \
               JOIN costed_b AS b ON b.k = a.k \
               ORDER BY a.id, b.id, c.id";
    // Isolate the hash spill budget from an unrelated 768-row external sort.
    let sql = if spill {
        sql.split(" ORDER BY").next().unwrap()
    } else {
        sql
    };
    let read = database.begin_read_transaction();
    let planned = read
        .query_sql_with_params_options_profiled(sql, &[], QueryStreamOptions::default())
        .unwrap();
    assert_eq!(
        planned.profile.join_planning.strategy,
        RelationalJoinPlanningStrategy::CsgCmpMemo
    );
    assert!(
        planned
            .profile
            .operator_cardinality_profiles
            .iter()
            .any(|operator| operator.operator == expected),
        "costed plan must select {expected:?}: {:?}",
        planned.profile
    );
    assert_eq!(
        planned.profile.join_planning.reason,
        RelationalJoinPlanningReason::CostReordered
    );
    let syntax = read
        .query_sql_with_params_options_profiled_with_join_planning(
            sql,
            &[],
            QueryStreamOptions::default(),
            RelationalJoinPlanningDirective::SyntaxOrder,
        )
        .unwrap();
    let pairs = |rows: &crate::QueryRows| {
        let mut rows = rows
            .iter()
            .map(|row| {
                (
                    row["a_id"].clone(),
                    row["b_id"].clone(),
                    row["c_id"].clone(),
                )
            })
            .collect::<Vec<_>>();
        if spill {
            rows.sort();
        }
        rows
    };
    assert_eq!(pairs(&planned.output.rows), pairs(&syntax.output.rows));
    assert_eq!(planned.output.schema(), syntax.output.schema());
    assert_eq!(planned.output.rows.len(), 768);
    if !indexed {
        assert!(planned.profile.row_read.rows_visited < syntax.profile.row_read.rows_visited);
    }
    let cancellation = skein_core::RuntimeCancellationToken::new();
    cancellation.cancel();
    let context = skein_core::RuntimeTaskContext::without_deadline(cancellation);
    assert!(read
        .query_sql_with_params_options_context(sql, &[], QueryStreamOptions::default(), &context)
        .unwrap_err()
        .to_string()
        .contains("cancelled"));
    let recovered = read.query_sql_with_params(sql, &[]).unwrap();
    assert_eq!(pairs(&recovered.rows), pairs(&planned.output.rows));
    if spill {
        assert!(planned
            .profile
            .blocking_operator_memory_reports
            .iter()
            .any(|report| {
                report.operator == "RelationalHashJoinGrace" && report.spilled_rows > 0
            }));
    }
    if directory.exists() {
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
        std::fs::remove_dir(&directory).unwrap();
    }
}

#[test]
fn seeded_costed_joins_preserve_nulls_duplicates_and_residual_predicates() {
    let mut hash_cases = 0;
    let mut merge_cases = 0;
    for seed in 0..24_i64 {
        for indexed in [false, true] {
            let mut database = Database::new();
            let rows = |count, salt| {
                (0..count)
                    .map(|id| {
                        (
                            id,
                            ((id + seed + salt) % 7 != 0).then_some((id * 3 + seed + salt) % 5),
                        )
                    })
                    .collect::<Vec<(i64, Option<i64>)>>()
            };
            let left = rows(32, 0);
            let right = rows(40, 2);
            for (table, rows) in [("oracle_a", &left), ("oracle_b", &right)] {
                database
                    .query_sql(&format!(
                        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, k BIGINT)"
                    ))
                    .unwrap();
                let values = rows
                    .iter()
                    .map(|(id, key)| {
                        format!(
                            "({id}, {})",
                            key.map_or_else(|| "NULL".to_owned(), |key| key.to_string())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                database
                    .query_sql(&format!("INSERT INTO {table} (id, k) VALUES {values}"))
                    .unwrap();
                if indexed {
                    database
                        .query_sql(&format!("CREATE INDEX {table}_key ON {table} (k)"))
                        .unwrap();
                }
            }
            for outer in [false, true] {
                let sql = format!("SELECT a.id AS a_id, b.id AS b_id FROM oracle_a AS a {} JOIN oracle_b AS b ON a.k = b.k AND a.id < b.id ORDER BY a.id, b.id", if outer { "LEFT" } else { "INNER" });
                let read = database.begin_read_transaction();
                let actual = read
                    .query_sql_with_params_options_profiled(
                        &sql,
                        &[],
                        QueryStreamOptions::default(),
                    )
                    .unwrap();
                let mut expected = Vec::new();
                // Independent nested-loop oracle over fixture values, not executor keys.
                for (a, a_key) in &left {
                    let mut matched = false;
                    for (b, b_key) in &right {
                        if a_key.is_some() && a_key == b_key && a < b {
                            expected.push((Value::Int(*a), Value::Int(*b)));
                            matched = true;
                        }
                    }
                    if outer && !matched {
                        expected.push((Value::Int(*a), Value::Null));
                    }
                }
                let pairs = actual
                    .output
                    .rows
                    .iter()
                    .map(|row| (row["a_id"].clone(), row["b_id"].clone()))
                    .collect::<Vec<_>>();
                assert_eq!(
                    pairs, expected,
                    "seed={seed}, indexed={indexed}, outer={outer}"
                );
                assert_eq!(
                    actual.profile.join_planning.strategy,
                    RelationalJoinPlanningStrategy::CsgCmpMemo
                );
                hash_cases += usize::from(
                    actual
                        .profile
                        .operator_cardinality_profiles
                        .iter()
                        .any(|operator| operator.operator == RelationalOperatorKind::HashJoin),
                );
                merge_cases += usize::from(
                    actual
                        .profile
                        .operator_cardinality_profiles
                        .iter()
                        .any(|operator| operator.operator == RelationalOperatorKind::MergeJoin),
                );
            }
        }
    }
    assert!(
        hash_cases >= 24,
        "hash path must be exercised: {hash_cases}"
    );
    assert!(
        merge_cases >= 24,
        "merge path must be exercised: {merge_cases}"
    );
}

#[test]
fn costed_implementation_budget_falls_back_without_changing_results() {
    let mut database = Database::new_with_config(DatabaseConfig {
        max_relational_join_expressions: Some(1),
        ..DatabaseConfig::default()
    });
    for table in ["budget_a", "budget_b"] {
        database
            .query_sql(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, k BIGINT)"
            ))
            .unwrap();
        database
            .query_sql(&format!(
                "INSERT INTO {table} (id, k) VALUES (1, 7), (2, 7)"
            ))
            .unwrap();
    }
    let read = database.begin_read_transaction();
    let sql = "SELECT a.id AS a_id, b.id AS b_id FROM budget_a AS a JOIN budget_b AS b ON a.k = b.k ORDER BY a.id, b.id";
    let planned = read
        .query_sql_with_params_options_profiled(sql, &[], QueryStreamOptions::default())
        .unwrap();
    assert!(planned
        .profile
        .join_planning
        .attempts
        .iter()
        .any(|attempt| {
            attempt.strategy == RelationalJoinPlanningStrategy::CsgCmpMemo
                && attempt.reason == RelationalJoinPlanningReason::ExpressionBudgetExceeded
        }));
    let syntax = read
        .query_sql_with_params_options_profiled_with_join_planning(
            sql,
            &[],
            QueryStreamOptions::default(),
            RelationalJoinPlanningDirective::SyntaxOrder,
        )
        .unwrap();
    assert_eq!(planned.output.rows, syntax.output.rows);
    assert_eq!(planned.output.rows.len(), 4);
}
