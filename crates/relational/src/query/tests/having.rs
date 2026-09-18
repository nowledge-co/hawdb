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
fn having_keeps_input_work_and_cancellation_limits_when_all_groups_are_rejected() {
    let state = batched_index_join_state();
    let read_modes = RelationalQueryReadModes::new(
        RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
        RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
    );
    for sql in [
        "SELECT COUNT(*) FROM batch_outer HAVING FALSE",
        "SELECT join_key FROM batch_outer GROUP BY join_key HAVING FALSE",
        "SELECT COUNT(*) FROM batch_outer o CROSS JOIN batch_inner i HAVING FALSE",
    ] {
        let SqlStatement::Select(select) = hawdb_sql::parse_postgres_sql(sql).unwrap() else {
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
        let memory = hawdb_executor::ExecutionMemoryConfig::default();
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
        let cancellation = hawdb_core::RuntimeCancellationToken::new();
        let context = hawdb_core::RuntimeTaskContext::without_deadline(cancellation.clone());
        cancellation.cancel();
        let error = run(batched_index_join_limits(), Some(&context)).unwrap_err();
        assert!(error.to_string().contains("cancelled"), "{error}");
    }
}
