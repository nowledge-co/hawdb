use crate::{
    Database, DatabaseConfig, QueryStreamOptions, RelationalJoinPlanningDirective,
    RelationalJoinPlanningReason, RelationalJoinPlanningStatus, RelationalJoinPlanningStrategy,
    Value,
};
use std::num::NonZeroUsize;

#[test]
fn long_inner_chain_with_like_filter_uses_cost_based_order_and_matches_syntax() {
    for count in [13, 15] {
        let (database, sql) = fixture(count, DatabaseConfig::default());
        let read = database.begin_read_transaction();
        let parameters = [Value::String("keep%".to_owned())];
        let planned = read
            .query_sql_with_params_options_profiled(
                &sql,
                &parameters,
                QueryStreamOptions::default(),
            )
            .unwrap();
        let outcome = &planned.profile.join_planning;
        assert_eq!(
            outcome.strategy,
            RelationalJoinPlanningStrategy::InnerJoinMemo
        );
        assert_eq!(
            outcome.status,
            RelationalJoinPlanningStatus::Selected,
            "{outcome:?}"
        );
        assert_eq!(outcome.memo_groups, Some(count * (count + 1) / 2));
        assert_eq!(outcome.memo_expressions, Some(count * count));
        assert_eq!(outcome.selected_order.len(), count);
        assert_eq!(outcome.reason, RelationalJoinPlanningReason::CostReordered);
        assert_eq!(outcome.selected_order[0], format!("c{}", count - 1));
        assert!(outcome.cost.is_some());
        assert_eq!(
            outcome.attempts[0].strategy,
            RelationalJoinPlanningStrategy::CsgCmpMemo
        );
        assert_eq!(
            outcome.attempts[0].reason,
            RelationalJoinPlanningReason::UnsupportedPostJoinFilter
        );
        let syntax = read
            .query_sql_with_params_options_profiled_with_join_planning(
                &sql,
                &parameters,
                QueryStreamOptions::default(),
                RelationalJoinPlanningDirective::SyntaxOrder,
            )
            .unwrap();
        assert_eq!(planned.output.rows, syntax.output.rows);
        assert_eq!(planned.output.schema(), syntax.output.schema());
        assert_eq!(planned.output.rows.len(), 1);
        assert_eq!(planned.output.rows[0]["id"], Value::Int(1));
        assert_eq!(
            planned.output.rows[0]["label"],
            Value::String("keep-row".to_owned())
        );
    }
}

#[test]
fn exhausted_connected_join_budget_preserves_reported_syntax_fallback() {
    for (count, max_groups, required_groups) in [(13, Some(90), 91), (17, None, 129)] {
        let (database, sql) = fixture(
            count,
            DatabaseConfig {
                max_optimizer_groups: max_groups,
                ..DatabaseConfig::default()
            },
        );
        let read = database.begin_read_transaction();
        let parameters = [Value::String("keep%".to_owned())];
        let planned = read
            .query_sql_with_params_options_profiled(
                &sql,
                &parameters,
                QueryStreamOptions::default(),
            )
            .unwrap();
        let outcome = &planned.profile.join_planning;
        assert_eq!(
            outcome.strategy,
            RelationalJoinPlanningStrategy::InnerJoinMemo,
            "{outcome:?}"
        );
        assert_eq!(outcome.status, RelationalJoinPlanningStatus::Fallback);
        assert_eq!(outcome.memo_groups, Some(required_groups));
        assert_eq!(outcome.budget.max_groups, required_groups - 1);
        assert_eq!(
            outcome.reason,
            RelationalJoinPlanningReason::GroupBudgetExceeded
        );
        assert!(outcome.attempts.iter().any(|attempt| {
            attempt.strategy == RelationalJoinPlanningStrategy::InnerJoinMemo
                && attempt.reason == RelationalJoinPlanningReason::GroupBudgetExceeded
        }));
        let fallback = outcome.attempts.last().unwrap();
        assert_eq!(
            fallback.strategy,
            RelationalJoinPlanningStrategy::SyntaxOrder
        );
        assert_eq!(
            fallback.reason,
            RelationalJoinPlanningReason::SyntaxFallback
        );
        let syntax = read
            .query_sql_with_params_options_profiled_with_join_planning(
                &sql,
                &parameters,
                QueryStreamOptions::default(),
                RelationalJoinPlanningDirective::SyntaxOrder,
            )
            .unwrap();
        assert_eq!(planned.output.rows, syntax.output.rows);
        assert_eq!(planned.output.rows.len(), 1);
        assert_eq!(
            outcome.selected_order,
            syntax.profile.join_planning.selected_order
        );
    }
}

fn fixture(count: usize, mut config: DatabaseConfig) -> (Database, String) {
    // Deep probe chains reserve one transfer batch per level. The three-row
    // fixture needs small batches, not a larger query or optimizer budget.
    config.execution_memory.batch_payload_bytes = NonZeroUsize::new(64 * 1024).unwrap();
    let mut database = Database::new_with_config(config);
    for index in 0..count {
        database
            .query_sql(&format!(
                "CREATE TABLE connected_{index} (id BIGINT PRIMARY KEY, label TEXT)"
            ))
            .unwrap();
        let rows = if index + 1 == count {
            "(1, 'keep-row')"
        } else {
            "(1, 'keep-row'), (2, 'drop-row'), (3, NULL)"
        };
        database
            .query_sql(&format!(
                "INSERT INTO connected_{index} (id, label) VALUES {rows}"
            ))
            .unwrap();
    }
    let mut sql = "SELECT c0.id, c0.label FROM connected_0 AS c0".to_owned();
    for index in 1..count {
        sql.push_str(&format!(
            " INNER JOIN connected_{index} AS c{index} ON c{index}.id = c{}.id",
            index - 1
        ));
    }
    sql.push_str(" WHERE c0.label LIKE $1 ORDER BY c0.id");
    (database, sql)
}
