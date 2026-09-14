use super::{
    choose_base_access, choose_join_access, elapsed_nanos, measure_nanos,
    plan_relational_field_plan, projection_access_planning, projection_contains_aggregate,
    reject_non_public_schema, resolve_relational_order_target,
    validate_non_aggregate_coalesce_projections, Instant, PreparedRelationalAccessPlan,
    PreparedRelationalExecutionDescriptor, PreparedRelationalSelect,
    RelationalAccessPathDescriptor, RelationalBaseAccessPlanning, RelationalJoinPlanningContext,
    RelationalOrderTarget, RelationalQueryLimits, RelationalQueryReadModes,
    RelationalSqlStageTimings, RelationalState, Result, SelectStatement, SkeinError, Value,
};

pub(super) fn prepare_relational_select(
    mut select: SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_>,
    limits: RelationalQueryLimits,
    join_planning: RelationalJoinPlanningContext,
    initial_stage_timings: RelationalSqlStageTimings,
) -> Result<PreparedRelationalSelect> {
    let prepare_started = Instant::now();
    let mut current_state_bind_nanos = 0;
    measure_nanos(&mut current_state_bind_nanos, || -> Result<()> {
        reject_non_public_schema(select.from.schema.as_deref())?;
        if state.table_schema(&select.from.name).is_none() {
            return Err(SkeinError::Semantic(format!(
                "unknown relational table {}",
                select.from.name
            )));
        }
        for join in &select.joins {
            reject_non_public_schema(join.table.schema.as_deref())?;
            if state.table_schema(&join.table.name).is_none() {
                return Err(SkeinError::Semantic(format!(
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
    access_plan.finalize_physical_join_plan(&planned.statement, state, read_modes.index)?;
    access_plan.apply_physical_index_coverage(state, &field_plan)?;
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
    prepared.validate()?;
    Ok(prepared)
}

pub(super) fn prepare_syntax_access_plan(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_>,
    limits: RelationalQueryLimits,
) -> Result<PreparedRelationalAccessPlan> {
    let base_schema = state.table_schema(&select.from.name).ok_or_else(|| {
        SkeinError::Semantic(format!("unknown relational table {}", select.from.name))
    })?;
    let base_qualifier = select
        .from_alias
        .clone()
        .unwrap_or_else(|| select.from.name.clone());
    let has_aggregate =
        select.having.is_some() || select.projection.iter().any(projection_contains_aggregate);
    let prefer_ordered_access =
        select.joins.is_empty() && !select.distinct && !has_aggregate && select.group_by.is_empty();
    let access_order_by = resolved_access_order_by(select)?;
    let base_access = choose_base_access(RelationalBaseAccessPlanning {
        predicate: select.selection.as_ref(),
        order_by: &access_order_by,
        prefer_ordered_access,
        parameters,
        state,
        schema: base_schema,
        table: &select.from.name,
        qualifier: &base_qualifier,
        cardinality_limit: limits.max_intermediate_rows.saturating_add(1),
        projection: projection_access_planning(read_modes.row, &select.from.name),
    })?;
    let join_accesses = select
        .joins
        .iter()
        .map(|join| {
            let join_schema = state.table_schema(&join.table.name).ok_or_else(|| {
                SkeinError::Semantic(format!("unknown relational table {}", join.table.name))
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

pub(super) fn resolved_access_order_by(
    select: &SelectStatement,
) -> Result<Vec<crate::sql::SqlOrderItem>> {
    let mut resolved = Vec::with_capacity(select.order_by.len());
    let mut supports_ordered_access = true;
    for item in &select.order_by {
        let column = match resolve_relational_order_target(select, item)? {
            RelationalOrderTarget::InputColumn(column) => Some(column),
            RelationalOrderTarget::ProjectionColumn { column, .. } => Some(column),
            RelationalOrderTarget::ProjectionExpression { .. } => {
                supports_ordered_access = false;
                None
            }
        };
        if let Some(column) = column {
            resolved.push(crate::sql::SqlOrderItem {
                expression: crate::sql::Expr {
                    kind: crate::sql::ExprKind::Column(column.clone()),
                    span: item.expression.span,
                },
                direction: item.direction,
                nulls: item.nulls,
            });
        }
    }
    Ok(if supports_ordered_access {
        resolved
    } else {
        Vec::new()
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
