use super::*;

fn state() -> RelationalState {
    let mut state = RelationalState::default();
    let ddl = compile_relational_statement_sql(
        "CREATE TABLE aggregate_records (id BIGINT PRIMARY KEY, n BIGINT, body TEXT)",
        &[],
        &state,
    )
    .unwrap();
    state = state
        .stage_transaction(ddl, Default::default(), Default::default())
        .unwrap();
    for id in 0..65i64 {
        let n = if id % 3 == 0 {
            Value::Null
        } else {
            Value::Int(id - 32)
        };
        let body = if id % 5 == 0 {
            Value::Null
        } else {
            Value::String("\u{e9}\u{1f980}".repeat((id % 7) as usize))
        };
        let insert = compile_relational_statement_sql(
            "INSERT INTO aggregate_records (id, n, body) VALUES ($1, $2, $3)",
            &[Value::Int(id), n, body],
            &state,
        )
        .unwrap();
        state = state
            .stage_transaction(
                insert,
                Default::default(),
                RelationalOverflowConfig {
                    threshold_bytes: 8,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    state
}

fn read_modes() -> RelationalQueryReadModes<'static> {
    RelationalQueryReadModes::new(
        RelationalIndexReadMode::Materialized,
        RelationalRowReadMode::CanonicalMemory,
    )
}

#[test]
fn columnar_and_row_aggregation_match_through_real_bindings_and_parameters() {
    let state = state();
    for batch_rows in [1, 7, 64, 65] {
        let memory = skein_executor::ExecutionMemoryConfig {
            batch_rows: NonZeroUsize::new(batch_rows).unwrap(),
            ..Default::default()
        };
        for (lower, offset, limit) in [(-1, 0, 1), (64, 0, 1), (999, 0, 1), (-1, 1, 1), (-1, 0, 0)]
        {
            let mut outputs = Vec::new();
            for filter in ["", " FILTER (WHERE id IS NOT NULL)"] {
                let sql = format!(
                    "SELECT COUNT(*){filter} AS rows, COUNT(n){filter} AS present, SUM(n){filter} AS total, SUM(OCTET_LENGTH(body)){filter} AS body_bytes FROM aggregate_records WHERE id >= $1 LIMIT $2 OFFSET $3"
                );
                let output = execute_relational_query_sql_with_runtime(
                    &sql,
                    &[Value::Int(lower), Value::Int(limit), Value::Int(offset)],
                    &state,
                    read_modes(),
                    batched_index_join_limits(),
                    &memory,
                    None,
                )
                .unwrap();
                assert_eq!(
                    output.hydration.hydrated_rows, 0,
                    "metadata-only length: {sql}"
                );
                outputs.push(output.rows);
            }
            assert_eq!(
                outputs[0], outputs[1],
                "batch={batch_rows} lower={lower} offset={offset} limit={limit}"
            );
            if offset != 0 || limit == 0 {
                assert!(outputs[0].is_empty());
                continue;
            }
            let selected = (0..65i64).filter(|id| *id >= lower).collect::<Vec<_>>();
            let values = selected
                .iter()
                .filter(|id| *id % 3 != 0)
                .map(|id| *id - 32)
                .collect::<Vec<_>>();
            let lengths = selected
                .iter()
                .filter(|id| *id % 5 != 0)
                .map(|id| (id % 7) * 6)
                .collect::<Vec<_>>();
            let expected: Row = [
                ("rows", Value::Int(selected.len() as i64)),
                ("present", Value::Int(values.len() as i64)),
                (
                    "total",
                    if values.is_empty() {
                        Value::Null
                    } else {
                        Value::Int(values.iter().sum())
                    },
                ),
                (
                    "body_bytes",
                    if lengths.is_empty() {
                        Value::Null
                    } else {
                        Value::Int(lengths.iter().sum())
                    },
                ),
            ]
            .into_iter()
            .map(|(name, value)| (name.into(), value))
            .collect();
            assert_eq!(outputs[0].len(), 1);
            assert_eq!(outputs[0][0], expected);
        }
    }
}

#[test]
fn columnar_aggregation_retains_facade_limits_and_cancellation() {
    let state = state();
    let sql = "SELECT COUNT(*) AS rows, SUM(n) AS total FROM aggregate_records";
    let limits = batched_index_join_limits();
    let memory = skein_executor::ExecutionMemoryConfig::default();
    for (limits, memory, message) in [
        (
            RelationalQueryLimits {
                max_output_rows: 0,
                ..limits
            },
            memory.clone(),
            "max_output_rows",
        ),
        (
            RelationalQueryLimits {
                max_output_payload_bytes: 1,
                ..limits
            },
            memory.clone(),
            "max_output_payload_bytes",
        ),
        (
            RelationalQueryLimits {
                max_intermediate_rows: 1,
                ..limits
            },
            memory.clone(),
            "max_intermediate_rows",
        ),
        (
            limits,
            skein_executor::ExecutionMemoryConfig {
                batch_payload_bytes: NonZeroUsize::MIN,
                ..memory.clone()
            },
            "batch_payload_bytes",
        ),
        (
            limits,
            skein_executor::ExecutionMemoryConfig {
                blocking_operator_bytes: NonZeroUsize::MIN,
                ..memory.clone()
            },
            "blocking_operator_bytes",
        ),
    ] {
        let error = execute_relational_query_sql_with_runtime(
            sql,
            &[],
            &state,
            read_modes(),
            limits,
            &memory,
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }
    let cancellation = skein_core::RuntimeCancellationToken::new();
    let context = skein_core::RuntimeTaskContext::without_deadline(cancellation.clone());
    cancellation.cancel();
    let error = execute_relational_query_sql_with_runtime(
        sql,
        &[],
        &state,
        read_modes(),
        limits,
        &memory,
        Some(&context),
    )
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
}
