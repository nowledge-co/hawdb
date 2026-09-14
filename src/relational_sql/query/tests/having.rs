use super::*;

#[test]
fn having_keeps_input_work_and_cancellation_limits_when_all_groups_are_rejected() {
    let state = batched_index_join_state();
    let read_modes = RelationalQueryReadModes::new(
        RelationalIndexReadMode::Materialized,
        RelationalRowReadMode::CanonicalMemory,
    );
    for sql in [
        "SELECT COUNT(*) FROM batch_outer HAVING FALSE",
        "SELECT join_key FROM batch_outer GROUP BY join_key HAVING FALSE",
        "SELECT COUNT(*) FROM batch_outer o CROSS JOIN batch_inner i HAVING FALSE",
    ] {
        let SqlStatement::Select(select) = skein_sql::parse_postgres_sql(sql).unwrap() else {
            panic!("expected SELECT")
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
        let memory = skein_executor::ExecutionMemoryConfig::default();
        let run = |limits, context| {
            let admitted = prepared.execution.admit(
                &state,
                read_modes,
                RelationalQueryResourceContext::new(
                    RelationalJoinEnumerationConfig::default(),
                    limits,
                    &memory,
                    context,
                ),
            )?;
            execute_select(&prepared, &[], admitted)
        };
        assert!(run(batched_index_join_limits(), None)
            .unwrap()
            .rows
            .is_empty());
        let error = run(
            RelationalQueryLimits {
                max_intermediate_rows: 1,
                ..batched_index_join_limits()
            },
            None,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("max_intermediate_rows 1"),
            "{error}"
        );
        if !prepared.statement.joins.is_empty() {
            let error = run(
                RelationalQueryLimits {
                    max_candidate_work: 1,
                    ..batched_index_join_limits()
                },
                None,
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("max_candidate_work 1"),
                "{error}"
            );
        }
        let cancellation = skein_core::RuntimeCancellationToken::new();
        let context = skein_core::RuntimeTaskContext::without_deadline(cancellation.clone());
        cancellation.cancel();
        let error = run(batched_index_join_limits(), Some(&context)).unwrap_err();
        assert!(error.to_string().contains("cancelled"), "{error}");
    }
}
