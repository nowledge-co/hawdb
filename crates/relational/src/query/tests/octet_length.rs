// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn octet_length_reads_overflow_metadata_without_hydrating_payloads() {
    let mut state = RelationalState::default();
    for (sql, parameters) in [
        (
            "CREATE TABLE lengths (id BIGINT PRIMARY KEY, body TEXT, payload BYTEA)",
            vec![],
        ),
        (
            "INSERT INTO lengths (id, body, payload) VALUES ($1, $2, $3)",
            vec![
                Value::Int(1),
                Value::String("\u{e9}\u{1f980}".repeat(16 * 1024)),
                Value::Binary([0, 255, 17].repeat(32 * 1024)),
            ],
        ),
    ] {
        let transaction = compile_relational_statement_sql(sql, &parameters, &state).unwrap();
        state = state
            .stage_transaction(
                transaction,
                Default::default(),
                RelationalOverflowConfig {
                    threshold_bytes: 8,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    for sql in [
        "SELECT octet_length(body) AS bytes FROM lengths",
        "SELECT octet_length(body) AS bytes FROM lengths ORDER BY id",
        "SELECT octet_length(body) AS bytes FROM lengths WHERE octet_length(payload) = 98304",
        "SELECT SUM(OCTET_LENGTH(body)) AS bytes FROM lengths WHERE octet_length(payload) = 98304",
    ] {
        let modes = RelationalQueryReadModes::new(
            RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
            RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
        );
        let output = execute_relational_query_sql_with_runtime(
            sql,
            &[],
            &state,
            modes,
            batched_index_join_limits(),
            &hawdb_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(output.rows.len(), 1, "{sql}");
        assert_eq!(output.rows[0]["bytes"], Value::Int(98304), "{sql}");
        assert_eq!(
            output.hydration.hydrated_rows, 0,
            "lengths must use overflow metadata: {sql}"
        );
    }
}
