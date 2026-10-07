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

#[cfg(test)]
use super::RelationalJoinPlanningContext;
use super::{
    choose_base_access, choose_join_access, elapsed_nanos, measure_nanos,
    plan_relational_field_plan, projection_access_planning, projection_contains_aggregate,
    reject_non_public_schema, resolve_relational_order_target,
    validate_non_aggregate_coalesce_projections, HawDBError, Instant, PreparedRelationalAccessPlan,
    PreparedRelationalExecutionDescriptor, PreparedRelationalSelect,
    RelationalAccessPathDescriptor, RelationalBaseAccessPlanning, RelationalQueryLimits,
    RelationalQueryReadModes, RelationalQueryResourceContext, RelationalQueryStoreReader,
    RelationalSqlStageTimings, RelationalState, Result, SelectStatement, Value,
};
pub(super) use crate::field_plan::resolved_access_order_by;
use crate::row_runtime::{map_snapshot_error, RelationalRowReadMode};
use hawdb_core::RuntimeTaskContext;
use hawdb_expression::BindingId;
use hawdb_optimizer::{RelationalAccessCostContext, RelationalJoinCostContexts};
use hawdb_storage::relational::RelationalRowPageSnapshotReader;
use std::num::NonZeroU64;

pub(super) struct RelationalQueryPlanningSnapshot {
    pub(super) reader: Option<RelationalRowPageSnapshotReader>,
    pub(super) cost_contexts: RelationalJoinCostContexts,
    pub(super) index_context: crate::index_runtime::RelationalIndexRuntimeContext,
}

struct RelationalQueryRowPlanningSnapshot {
    reader: Option<RelationalRowPageSnapshotReader>,
    cost_contexts: RelationalJoinCostContexts,
}

impl RelationalQueryRowPlanningSnapshot {
    fn open(
        select: &SelectStatement,
        state: &RelationalState,
        row_mode: RelationalRowReadMode<'_, impl RelationalQueryStoreReader>,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        hawdb_executor::pipeline::runtime_checkpoint(Some(task))?;
        let reader = row_mode.open_snapshot_reader()?;
        let mut cost_contexts = RelationalJoinCostContexts::default();
        if let Some(reader) = &reader {
            for (index, table) in std::iter::once(&select.from_table().name)
                .chain(select.joins.iter().map(|join| &join.table.name))
                .enumerate()
            {
                let binding = BindingId::new(u32::try_from(index).map_err(|_| {
                    HawDBError::Execution("relational planning exceeds the binding-id range".into())
                })?);
                if row_mode.is_projection_table(table) {
                    continue;
                }
                let Some(root) = reader
                    .checkpoint_table_root(table, task)
                    .map_err(map_snapshot_error)?
                else {
                    continue;
                };
                if state.table_schema(table) != Some(&root.schema)
                    || u64::try_from(state.row_count(table)).ok() != Some(root.row_count)
                {
                    continue;
                }
                if let (Some(rows), Some(pages)) = (
                    NonZeroU64::new(root.row_count),
                    NonZeroU64::new(root.page_count),
                ) {
                    cost_contexts = cost_contexts.with_relation(
                        binding,
                        RelationalAccessCostContext::for_snapshot_rows(rows, pages),
                    );
                }
            }
        }
        hawdb_executor::pipeline::runtime_checkpoint(Some(task))?;
        Ok(Self {
            reader,
            cost_contexts,
        })
    }
}

#[cfg(test)]
pub(super) fn prepare_relational_select(
    select: SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_, impl RelationalQueryStoreReader>,
    limits: RelationalQueryLimits,
    join_planning: RelationalJoinPlanningContext,
    initial_stage_timings: RelationalSqlStageTimings,
) -> Result<PreparedRelationalSelect> {
    let execution_memory = hawdb_executor::ExecutionMemoryConfig::default();
    let (prepared, _) = prepare_relational_select_with_snapshot(
        select,
        parameters,
        state,
        read_modes,
        RelationalQueryResourceContext {
            join_planning,
            limits,
            execution_memory: &execution_memory,
            task_context: None,
        },
        initial_stage_timings,
    )?;
    Ok(prepared)
}

pub(super) fn prepare_relational_select_with_snapshot(
    mut select: SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_, impl RelationalQueryStoreReader>,
    resources: RelationalQueryResourceContext<'_>,
    initial_stage_timings: RelationalSqlStageTimings,
) -> Result<(PreparedRelationalSelect, RelationalQueryPlanningSnapshot)> {
    let limits = resources.limits;
    let join_planning = resources.join_planning;
    let prepare_started = Instant::now();
    let mut current_state_bind_nanos = 0;
    measure_nanos(&mut current_state_bind_nanos, || -> Result<()> {
        reject_non_public_schema(select.from_table().schema.as_deref())?;
        if state.table_schema(&select.from_table().name).is_none() {
            return Err(HawDBError::Semantic(format!(
                "unknown relational table {}",
                select.from_table().name
            )));
        }
        for join in &select.joins {
            reject_non_public_schema(join.table.schema.as_deref())?;
            if state.table_schema(&join.table.name).is_none() {
                return Err(HawDBError::Semantic(format!(
                    "unknown relational table {}",
                    join.table.name
                )));
            }
        }
        join_order::bind_from_scopes(&mut select, state)?;
        super::having::validate_having(&select, parameters, state)?;
        validate_non_aggregate_coalesce_projections(&select, parameters, state)?;
        for item in &select.order_by {
            resolve_relational_order_target(&select, item)?;
        }
        Ok(())
    })?;
    let default_task = RuntimeTaskContext::default();
    let snapshot = RelationalQueryRowPlanningSnapshot::open(
        &select,
        state,
        read_modes.row,
        resources.task_context.unwrap_or(&default_task),
    )?;
    let index_runtime = crate::index_runtime::RelationalIndexRuntime::with_context(
        read_modes.index,
        limits.index_read,
        crate::index_runtime::RelationalIndexRuntimeContext::new(
            limits.index_read,
            resources.task_context.unwrap_or(&default_task).clone(),
        ),
    );
    let read_modes = read_modes
        .with_cost_contexts(&snapshot.cost_contexts)
        .with_index_runtime(&index_runtime);
    let planned = join_order::plan_select_join_order(
        select,
        parameters,
        state,
        read_modes,
        limits,
        join_planning,
        &mut current_state_bind_nanos,
    )?;
    let mut access_plan = match planned.access_plan {
        Some(access_plan) => access_plan,
        None => {
            prepare_syntax_access_plan(&planned.statement, parameters, state, read_modes, limits)?
        }
    };
    let field_plan = plan_relational_field_plan(&planned.statement, state)?;
    access_plan.finalize_physical_join_plan_with_cost_contexts(
        &planned.statement,
        state,
        read_modes.index,
        &snapshot.cost_contexts,
    )?;
    access_plan.apply_physical_index_coverage_with_cost_contexts(
        state,
        &field_plan,
        &snapshot.cost_contexts,
    )?;
    let execution =
        PreparedRelationalExecutionDescriptor::prepare(&planned.statement, &access_plan)?;
    let prepare_nanos = elapsed_nanos(prepare_started);
    let prepared = PreparedRelationalSelect {
        statement: planned.statement,
        access_plan,
        join_planning: planned.join_planning,
        execution,
        stage_timings: RelationalSqlStageTimings {
            parse_nanos: initial_stage_timings.parse_nanos,
            bind_nanos: initial_stage_timings
                .bind_nanos
                .saturating_add(current_state_bind_nanos),
            plan_nanos: prepare_nanos.saturating_sub(current_state_bind_nanos),
            execute_nanos: 0,
        },
    };
    prepared.validate_with_cost_contexts(&snapshot.cost_contexts)?;
    Ok((
        prepared,
        RelationalQueryPlanningSnapshot {
            reader: snapshot.reader,
            cost_contexts: snapshot.cost_contexts,
            index_context: index_runtime.into_context(),
        },
    ))
}

pub(super) fn prepare_syntax_access_plan(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_, impl RelationalQueryStoreReader>,
    limits: RelationalQueryLimits,
) -> Result<PreparedRelationalAccessPlan> {
    let base_schema = state
        .table_schema(&select.from_table().name)
        .ok_or_else(|| {
            HawDBError::Semantic(format!(
                "unknown relational table {}",
                select.from_table().name
            ))
        })?;
    let base_qualifier = select
        .from_alias
        .clone()
        .unwrap_or_else(|| select.from_table().name.clone());
    let has_aggregate =
        select.having.is_some() || select.projection.iter().any(projection_contains_aggregate);
    let prefer_ordered_access =
        select.joins.is_empty() && !select.distinct && !has_aggregate && select.group_by.is_empty();
    let access_order_by = resolved_access_order_by(select)?;
    let fields = plan_relational_field_plan(select, state)?;
    let base_access = choose_base_access(RelationalBaseAccessPlanning {
        cost_context: read_modes.cost_context(BindingId::new(0)),
        index_read_mode: read_modes.index,
        index_runtime: read_modes.index_runtime,
        fields: &fields,
        predicate: select.selection.as_ref(),
        order_by: &access_order_by,
        prefer_ordered_access,
        parameters,
        state,
        schema: base_schema,
        table: &select.from_table().name,
        qualifier: &base_qualifier,
        cardinality_limit: limits.max_intermediate_rows.saturating_add(1),
        projection: projection_access_planning(read_modes.row, &select.from_table().name),
    })?;
    let join_accesses = select
        .joins
        .iter()
        .enumerate()
        .map(|(index, join)| {
            let join_schema = state.table_schema(&join.table.name).ok_or_else(|| {
                HawDBError::Semantic(format!("unknown relational table {}", join.table.name))
            })?;
            let qualifier = join
                .alias
                .clone()
                .unwrap_or_else(|| join.table.name.clone());
            choose_join_access(
                &join.on,
                state,
                join_schema,
                &join.table.name,
                &qualifier,
                read_modes.index,
                projection_access_planning(read_modes.row, &join.table.name),
                &fields,
                read_modes.cost_context(BindingId::new(
                    u32::try_from(index.saturating_add(1)).map_err(|_| {
                        HawDBError::Execution(
                            "relational planning exceeds the binding-id range".into(),
                        )
                    })?,
                )),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PreparedRelationalAccessPlan {
        base_access,
        join_accesses,
        join_selection: None,
        physical_join_plan: None,
    })
}

pub(super) fn prepared_access_descriptors(
    plan: &PreparedRelationalAccessPlan,
) -> (
    RelationalAccessPathDescriptor,
    Vec<RelationalAccessPathDescriptor>,
) {
    if let Some(tree) = &plan.physical_join_plan {
        let base = tree.root.first_relation().access.descriptor().clone();
        let mut joins = Vec::with_capacity(tree.root.relation_count().saturating_sub(1));
        tree.root.visit_join_right_relations(&mut |relation| {
            joins.push(relation.access.descriptor().clone());
        });
        return (base, joins);
    }
    (
        plan.base_access.descriptor.clone(),
        plan.join_accesses
            .iter()
            .map(|access| access.descriptor.clone())
            .collect(),
    )
}

use super::join_order;
