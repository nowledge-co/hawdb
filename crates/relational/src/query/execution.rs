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

use super::{
    elapsed_nanos, execute_aggregate_select, execute_blocking_projection,
    execute_ordered_index_projection, execute_streaming_projection, format_relational_explain,
    plan_relational_field_plan, planned_operator_cardinality_profiles, prepared_access_descriptors,
    AdmittedRelationalExecution, BindingId, HawDBError, Instant, PlannedJoin,
    PreparedRelationalExecutionMode, PreparedRelationalSelect, QueryRows, RefCell,
    RelationalIndexRuntime, RelationalPhysicalJoinExecution, RelationalPipelineState,
    RelationalQueryLimits, RelationalQueryOutput, RelationalQueryStoreReader,
    RelationalRowExecutionEvidence, RelationalSqlStageTimings, Result, Value,
};

pub(super) fn explain_select(
    prepared: &PreparedRelationalSelect,
    parameters: &[Value],
    limits: RelationalQueryLimits,
) -> Result<RelationalQueryOutput> {
    let (access_path, join_access_paths) = prepared_access_descriptors(&prepared.access_plan);
    format_relational_explain(
        &prepared.statement,
        parameters,
        RelationalQueryOutput {
            rows: QueryRows::empty(),
            stage_timings: prepared.stage_timings,
            join_planning: prepared.join_planning.clone(),
            operator_cardinality_profiles: planned_operator_cardinality_profiles(prepared)?,
            intermediate_rows: 0,
            hydration: limits.hydration,
            access_path,
            join_access_paths,
            index_execution_evidence: Vec::new(),
            row_execution_evidence: RelationalRowExecutionEvidence {
                runtime_path: "not_executed",
                ..RelationalRowExecutionEvidence::default()
            },
            blocking_operator_memory_reports: Vec::new(),
        },
        false,
        limits,
    )
}

pub(super) fn execute_select_timed<'state>(
    prepared: &'state PreparedRelationalSelect,
    parameters: &[Value],
    execution: AdmittedRelationalExecution<'state, '_, impl RelationalQueryStoreReader>,
) -> Result<RelationalQueryOutput> {
    let started = Instant::now();
    let mut output = execute_select(prepared, parameters, execution)?;
    output.stage_timings = prepared.stage_timings;
    output.stage_timings.execute_nanos = elapsed_nanos(started);
    Ok(output)
}

pub(super) fn execute_select<'state>(
    prepared: &'state PreparedRelationalSelect,
    parameters: &[Value],
    execution: AdmittedRelationalExecution<'state, '_, impl RelationalQueryStoreReader>,
) -> Result<RelationalQueryOutput> {
    let select = &prepared.statement;
    let join_planning = &prepared.join_planning;
    let operator_cardinality_profiles = planned_operator_cardinality_profiles(prepared)?;
    let AdmittedRelationalExecution {
        state,
        index_read_mode,
        row_read_mode,
        limits,
        execution_memory,
        memory_ledger,
        task_context,
    } = execution;
    let base_schema = state.table_schema(&select.from.name).ok_or_else(|| {
        HawDBError::Semantic(format!("unknown relational table {}", select.from.name))
    })?;
    let base_qualifier = select
        .from_alias
        .clone()
        .unwrap_or_else(|| select.from.name.clone());
    let base_access = &prepared.access_plan.base_access;
    let (access_path, join_access_paths) = prepared_access_descriptors(&prepared.access_plan);
    let mut planned_joins = Vec::with_capacity(select.joins.len());
    for (index, (join, join_access)) in select
        .joins
        .iter()
        .zip(&prepared.access_plan.join_accesses)
        .enumerate()
    {
        let join_schema = state.table_schema(&join.table.name).ok_or_else(|| {
            HawDBError::Semantic(format!("unknown relational table {}", join.table.name))
        })?;
        let qualifier = join
            .alias
            .clone()
            .unwrap_or_else(|| join.table.name.clone());
        planned_joins.push(PlannedJoin {
            binding: BindingId::new(u32::try_from(index.saturating_add(1)).map_err(|_| {
                HawDBError::Execution(
                    "relational planned join exceeds the binding-id range".to_string(),
                )
            })?),
            join,
            schema: join_schema,
            qualifier,
            access: join_access.access.clone(),
        });
    }
    let default_task = hawdb_core::RuntimeTaskContext::default();
    let row_task = task_context.unwrap_or(&default_task);
    let field_plan = plan_relational_field_plan(select, state)?;
    let row_runtime = row_read_mode.open_runtime(
        state,
        field_plan,
        limits.row_read,
        limits.hydration,
        row_task,
    )?;
    let mut pipeline = RelationalPipelineState::new(
        task_context,
        limits,
        execution_memory.batch_rows,
        operator_cardinality_profiles,
    );
    let index_runtime = RelationalIndexRuntime::new(index_read_mode, limits.index_read);
    let physical_execution = RelationalPhysicalJoinExecution {
        tree: prepared.access_plan.physical_join_plan()?,
        memory: execution_memory,
        memory_ledger: &memory_ledger,
        reports: RefCell::new(Vec::new()),
    };
    if prepared.execution.mode == PreparedRelationalExecutionMode::OrderedIndexProjection {
        let output = execute_ordered_index_projection(
            select,
            parameters,
            state,
            base_schema,
            &base_qualifier,
            &base_access.access,
            &mut pipeline,
            &index_runtime,
            &row_runtime,
            limits,
            execution_memory,
            &memory_ledger,
        )?;
        pipeline.finish()?;
        return Ok(RelationalQueryOutput {
            rows: output.rows,
            stage_timings: RelationalSqlStageTimings::default(),
            join_planning: join_planning.clone(),
            operator_cardinality_profiles: pipeline.operator_cardinality_profiles(),
            intermediate_rows: pipeline.intermediate_rows,
            hydration: row_runtime.hydration(),
            access_path,
            join_access_paths,
            index_execution_evidence: index_runtime.evidence(),
            row_execution_evidence: row_runtime.evidence(),
            blocking_operator_memory_reports: output.blocking_operator_memory_reports,
        });
    }
    if prepared.execution.mode == PreparedRelationalExecutionMode::StreamingProjection {
        let mut output = execute_streaming_projection(
            select,
            parameters,
            state,
            base_schema,
            &base_qualifier,
            &base_access.access,
            &planned_joins,
            Some(&physical_execution),
            &mut pipeline,
            &index_runtime,
            &row_runtime,
            limits,
        )?;
        output
            .blocking_operator_memory_reports
            .extend(physical_execution.take_reports());
        pipeline.finish()?;
        return Ok(RelationalQueryOutput {
            rows: output.rows,
            stage_timings: RelationalSqlStageTimings::default(),
            join_planning: join_planning.clone(),
            operator_cardinality_profiles: pipeline.operator_cardinality_profiles(),
            intermediate_rows: pipeline.intermediate_rows,
            hydration: row_runtime.hydration(),
            access_path,
            join_access_paths,
            index_execution_evidence: index_runtime.evidence(),
            row_execution_evidence: row_runtime.evidence(),
            blocking_operator_memory_reports: output.blocking_operator_memory_reports,
        });
    }

    if prepared.execution.mode == PreparedRelationalExecutionMode::Aggregate {
        let mut output = execute_aggregate_select(
            select,
            parameters,
            state,
            base_schema,
            &base_qualifier,
            &base_access.access,
            &planned_joins,
            Some(&physical_execution),
            &mut pipeline,
            &index_runtime,
            &row_runtime,
            limits,
            execution_memory,
            &memory_ledger,
            access_path,
            join_access_paths,
            join_planning,
        )?;
        output
            .blocking_operator_memory_reports
            .extend(physical_execution.take_reports());
        return Ok(output);
    }

    let mut output = execute_blocking_projection(
        select,
        parameters,
        state,
        base_schema,
        &base_qualifier,
        &base_access.access,
        &planned_joins,
        Some(&physical_execution),
        &mut pipeline,
        &index_runtime,
        &row_runtime,
        limits,
        execution_memory,
        &memory_ledger,
    )?;
    output
        .blocking_operator_memory_reports
        .extend(physical_execution.take_reports());
    pipeline.finish()?;
    Ok(RelationalQueryOutput {
        rows: output.rows,
        stage_timings: RelationalSqlStageTimings::default(),
        join_planning: join_planning.clone(),
        operator_cardinality_profiles: pipeline.operator_cardinality_profiles(),
        intermediate_rows: pipeline.intermediate_rows,
        hydration: row_runtime.hydration(),
        access_path,
        join_access_paths,
        index_execution_evidence: index_runtime.evidence(),
        row_execution_evidence: row_runtime.evidence(),
        blocking_operator_memory_reports: output.blocking_operator_memory_reports,
    })
}
