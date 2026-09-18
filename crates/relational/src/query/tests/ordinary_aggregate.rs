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

use super::*;

fn read_modes() -> RelationalQueryReadModes<'static> {
    RelationalQueryReadModes::new(
        RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
        RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
    )
}

#[test]
fn grouped_having_preserves_nullable_join_bindings_and_distinct_results() {
    let state = batched_index_join_state();
    let memory = hawdb_executor::ExecutionMemoryConfig::default();
    let sql = "SELECT o.join_key AS bucket, COUNT(i.id) AS matches, COUNT(DISTINCT i.value) AS distinct_values, MAX(i.value) AS largest FROM batch_outer o LEFT JOIN batch_inner i ON o.join_key = i.join_key GROUP BY o.join_key HAVING COUNT(i.id) >= $1";
    for minimum in [0, 1, 3, 5] {
        let output = execute_relational_query_sql_with_runtime(
            sql,
            &[Value::Int(minimum)],
            &state,
            read_modes(),
            batched_index_join_limits(),
            &memory,
            None,
        )
        .unwrap();
        let expected = [
            (Value::Null, 0, 0, Value::Null),
            (
                Value::String("shared".into()),
                4,
                2,
                Value::String("second".into()),
            ),
            (
                Value::String("solo".into()),
                1,
                1,
                Value::String("only".into()),
            ),
        ]
        .into_iter()
        .filter(|(_, matches, _, _)| *matches >= minimum)
        .map(|(key, matches, distinct, largest)| {
            Row::from([
                ("bucket".into(), key),
                ("matches".into(), Value::Int(matches)),
                ("distinct_values".into(), Value::Int(distinct)),
                ("largest".into(), largest),
            ])
        })
        .collect::<Vec<_>>();
        assert_eq!(output.rows.len(), expected.len(), "minimum={minimum}");
        for row in expected {
            assert!(
                output.rows.iter().any(|actual| actual == row),
                "minimum={minimum}: {row:?}"
            );
        }
    }
}

#[test]
fn having_retains_empty_groups_and_primary_key_functional_dependencies() {
    let state = batched_index_join_state();
    let memory = hawdb_executor::ExecutionMemoryConfig::default();
    let empty = execute_relational_query_sql_with_runtime(
        "SELECT COUNT(*) AS rows, COUNT(DISTINCT join_key) AS distinct_keys FROM batch_outer WHERE id = $1 HAVING COUNT(*) = $2",
        &[Value::String("missing".into()), Value::Int(0)],
        &state, read_modes(), batched_index_join_limits(), &memory, None,
    ).unwrap();
    assert_eq!(
        empty.rows,
        vec![Row::from([
            ("rows".into(), Value::Int(0)),
            ("distinct_keys".into(), Value::Int(0))
        ])]
    );
    let grouped = execute_relational_query_sql_with_runtime(
        "SELECT id, join_key, COUNT(*) AS rows FROM batch_outer GROUP BY id HAVING COUNT(*) > 0",
        &[],
        &state,
        read_modes(),
        batched_index_join_limits(),
        &memory,
        None,
    )
    .unwrap();
    let expected = [
        ("outer-1", Some("shared")),
        ("outer-2", Some("shared")),
        ("outer-3", Some("solo")),
        ("outer-4", None),
    ]
    .into_iter()
    .map(|(id, key)| {
        Row::from([
            ("id".into(), Value::String(id.into())),
            (
                "join_key".into(),
                key.map_or(Value::Null, |key| Value::String(key.into())),
            ),
            ("rows".into(), Value::Int(1)),
        ])
    })
    .collect::<Vec<_>>();
    assert_eq!(grouped.rows.len(), expected.len());
    for row in expected {
        assert!(grouped.rows.iter().any(|actual| actual == row), "{row:?}");
    }
}

#[test]
fn grouped_aggregate_order_by_retains_its_explicit_unsupported_error() {
    let state = batched_index_join_state();
    let memory = hawdb_executor::ExecutionMemoryConfig::default();
    for sql in [
        "SELECT join_key, COUNT(*) FROM batch_outer GROUP BY join_key HAVING COUNT(*) >= 0 ORDER BY join_key NULLS FIRST",
        "SELECT id, join_key, COUNT(*) FROM batch_outer GROUP BY id HAVING COUNT(*) >= 0 ORDER BY id",
    ] {
        let error = execute_relational_query_sql_with_runtime(
            sql, &[], &state, read_modes(), batched_index_join_limits(), &memory, None,
        ).unwrap_err();
        assert!(matches!(error, HawDBError::Semantic(ref message) if message == "aggregate SELECT does not yet support statement DISTINCT or ORDER BY"));
    }
}

#[test]
fn having_rejection_does_not_bypass_work_memory_or_output_limits() {
    let state = batched_index_join_state();
    let memory = hawdb_executor::ExecutionMemoryConfig::default();
    let limits = batched_index_join_limits();
    for (sql, limits, memory, expected) in [
        (
            "SELECT COUNT(*) FROM batch_outer HAVING COUNT(*) < 0",
            RelationalQueryLimits {
                max_intermediate_rows: 1,
                ..limits
            },
            memory.clone(),
            "max_intermediate_rows",
        ),
        (
            "SELECT COUNT(*) FROM batch_outer HAVING COUNT(*) >= 0",
            RelationalQueryLimits {
                max_output_rows: 0,
                ..limits
            },
            memory.clone(),
            "max_output_rows",
        ),
        (
            "SELECT COUNT(*) FROM batch_outer HAVING COUNT(*) >= 0",
            RelationalQueryLimits {
                max_output_payload_bytes: 1,
                ..limits
            },
            memory.clone(),
            "max_output_payload_bytes",
        ),
        (
            "SELECT COUNT(DISTINCT join_key) FROM batch_outer HAVING COUNT(*) >= 0",
            limits,
            hawdb_executor::ExecutionMemoryConfig {
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
        assert!(error.to_string().contains(expected), "{error}");
    }
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    let context = hawdb_core::RuntimeTaskContext::without_deadline(cancellation.clone());
    cancellation.cancel();
    let error = execute_relational_query_sql_with_runtime(
        "SELECT COUNT(*) FROM batch_outer HAVING COUNT(*) < 0",
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
