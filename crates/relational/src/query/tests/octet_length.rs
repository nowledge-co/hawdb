// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

const OVERFLOW_PAYLOAD_BYTES: usize = 96 * 1024;
const OVERFLOW_THRESHOLD_BYTES: usize = 8;

#[test]
fn octet_length_reads_overflow_metadata_without_hydrating_payloads() {
    let mut state = RelationalState::default();
    let text_unit = "\u{e9}\u{1f980}";
    let binary_unit = [0, 255, 17];
    let body = text_unit.repeat(OVERFLOW_PAYLOAD_BYTES / text_unit.len());
    let payload = binary_unit.repeat(OVERFLOW_PAYLOAD_BYTES / binary_unit.len());
    assert_eq!(body.len(), OVERFLOW_PAYLOAD_BYTES);
    assert_eq!(payload.len(), OVERFLOW_PAYLOAD_BYTES);
    let expected_bytes = i64::try_from(OVERFLOW_PAYLOAD_BYTES).unwrap();
    for (sql, parameters) in [
        (
            "CREATE TABLE lengths (id BIGINT PRIMARY KEY, body TEXT, payload BYTEA)",
            vec![],
        ),
        (
            "INSERT INTO lengths (id, body, payload) VALUES ($1, $2, $3)",
            vec![
                Value::Int(1),
                Value::String(body.clone()),
                Value::Binary(payload),
            ],
        ),
    ] {
        let transaction = compile_relational_statement_sql(sql, &parameters, &state).unwrap();
        state = state
            .stage_transaction(
                transaction,
                Default::default(),
                RelationalOverflowConfig {
                    threshold_bytes: OVERFLOW_THRESHOLD_BYTES,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    let run = |sql: &str, parameters: &[Value]| {
        let modes = RelationalQueryReadModes::new(
            RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
            RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
        );
        execute_relational_query_sql_with_runtime(
            sql,
            parameters,
            &state,
            modes,
            batched_index_join_limits(),
            &hawdb_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
    };
    for (sql, parameters) in [
        ("SELECT octet_length(body) AS bytes FROM lengths", vec![]),
        (
            "SELECT octet_length(body) AS bytes FROM lengths ORDER BY id",
            vec![],
        ),
        (
            "SELECT octet_length(body) AS bytes FROM lengths WHERE octet_length(payload) = $1",
            vec![Value::Int(expected_bytes)],
        ),
        (
            "SELECT SUM(OCTET_LENGTH(body)) AS bytes FROM lengths WHERE octet_length(payload) = $1",
            vec![Value::Int(expected_bytes)],
        ),
    ] {
        let output = run(sql, &parameters);
        assert_eq!(output.rows.len(), 1, "{sql}");
        assert_eq!(output.rows[0]["bytes"], Value::Int(expected_bytes), "{sql}");
        assert_eq!(
            output.hydration.hydrated_rows, 0,
            "lengths must use overflow metadata: {sql}"
        );
    }
    // A raw comparison on the same column still needs the complete value.
    for (body, expected_rows) in [(body, 1), ("z".repeat(OVERFLOW_PAYLOAD_BYTES), 0)] {
        let output = run(
            "SELECT octet_length(body) AS bytes FROM lengths WHERE body = $1",
            &[Value::String(body)],
        );
        assert_eq!(output.rows.len(), expected_rows);
        if expected_rows != 0 {
            assert_eq!(output.rows[0]["bytes"], Value::Int(expected_bytes));
        }
        assert!(
            output.hydration.hydrated_rows > 0,
            "raw predicates must hydrate even when a length is also projected"
        );
    }
}
