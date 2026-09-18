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

#[test]
fn cross_join_preserves_candidate_intermediate_and_cancellation_limits() {
    let state = batched_index_join_state();
    let read_modes = RelationalQueryReadModes::new(
        RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
        RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
    );
    let SqlStatement::Select(select) = hawdb_sql::parse_postgres_sql(
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
    let memory = hawdb_executor::ExecutionMemoryConfig::default();
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
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    let context = hawdb_core::RuntimeTaskContext::without_deadline(cancellation.clone());
    cancellation.cancel();
    let error = run(batched_index_join_limits(), Some(&context)).unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
}
