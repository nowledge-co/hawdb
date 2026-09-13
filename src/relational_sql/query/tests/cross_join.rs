use super::*;

#[test]
fn cross_join_preserves_candidate_intermediate_and_cancellation_limits() {
    let state = batched_index_join_state();
    let read_modes = RelationalQueryReadModes::new(
        RelationalIndexReadMode::Materialized,
        RelationalRowReadMode::CanonicalMemory,
    );
    let SqlStatement::Select(select) = skein_sql::parse_postgres_sql(
        "SELECT o.id AS outer_id, i.id AS inner_id \
         FROM batch_outer o CROSS JOIN batch_inner i",
    )
    .unwrap() else {
        panic!("expected SELECT");
    };
    let prepared = prepare_relational_select(
        select,
        &[],
        &state,
        read_modes,
        batched_index_join_limits(),
        RelationalJoinPlanningContext::default(),
        RelationalSqlStageTimings::default(),
    )
    .unwrap();
    assert_eq!(
        prepared.join_planning.strategy,
        RelationalJoinPlanningStrategy::SyntaxOrder
    );
    let memory = skein_executor::ExecutionMemoryConfig::default();
    let run = |limits, task_context| {
        let admitted = prepared.execution.admit(
            &state,
            read_modes,
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                limits,
                &memory,
                task_context,
            ),
        )?;
        execute_select(&prepared, &[], admitted)
    };
    let output = run(batched_index_join_limits(), None).unwrap();
    assert_eq!(output.rows.len(), 12);
    assert!(output
        .operator_cardinality_profiles
        .iter()
        .any(
            |profile| profile.operator == RelationalOperatorKind::NestedLoopJoin
                && profile.actual_rows == Some(12)
                && profile.fully_consumed
        ));
    for (limits, expected) in [
        (
            RelationalQueryLimits {
                max_candidate_work: 1,
                ..batched_index_join_limits()
            },
            "max_candidate_work 1",
        ),
        (
            RelationalQueryLimits {
                max_intermediate_rows: 1,
                ..batched_index_join_limits()
            },
            "max_intermediate_rows 1",
        ),
    ] {
        let error = run(limits, None).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
    let cancellation = skein_core::RuntimeCancellationToken::new();
    let context = skein_core::RuntimeTaskContext::without_deadline(cancellation.clone());
    cancellation.cancel();
    let error = run(batched_index_join_limits(), Some(&context)).unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
}
