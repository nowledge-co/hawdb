use super::{
    choose_base_access, choose_join_access, prepare_syntax_access_plan, projection_access_planning,
    projection_contains_aggregate, PreparedRelationalAccessPlan, PreparedRelationalJoinSelection,
    RelationalAccessCandidate, RelationalBaseAccess, RelationalBaseAccessPlanning,
    RelationalJoinAccess, RelationalJoinAccessCandidate, RelationalJoinPlanningContext,
    RelationalOperatorId, RelationalPhysicalAccess, RelationalPhysicalJoinNode,
    RelationalPhysicalJoinPlan, RelationalQueryLimits, RelationalQueryReadModes,
};
use crate::error::{Result, SkeinError};
use crate::relational_sql::{
    resolve_relational_order_target, RelationalJoinPlanningAttempt, RelationalJoinPlanningOutcome,
    RelationalJoinPlanningReason, RelationalJoinPlanningStrategy, RelationalOrderTarget,
};
use crate::sql::{
    SelectProjection, SelectStatement, SqlColumnRef, SqlExpression, SqlFunctionArgument, SqlJoin,
    SqlJoinKind, SqlPredicate, SqlTableName,
};
use crate::Value;
use skein_expression::{
    BindingId, BindingSet, BoundPredicate, BoundScalarExpression, ScalarNullability,
};
use skein_optimizer::{
    enumerate_relational_csg_cmp_joins_with_right_input_policy, enumerate_relational_inner_joins,
    enumerate_relational_join_rewrites, RelationalAccessPathDescriptor, RelationalAccessPathKind,
    RelationalCsgCmpPlan, RelationalCsgCmpPlanNode, RelationalCsgCmpRightInputPolicy,
    RelationalJoinAccessPath, RelationalJoinEnumerationConfig, RelationalJoinEnumerationError,
    RelationalJoinGraph, RelationalJoinOperator, RelationalJoinOperatorId,
    RelationalJoinOperatorKind, RelationalJoinPlanningDirective, RelationalJoinPredicate,
    RelationalJoinPredicateId, RelationalJoinRelation, RelationalJoinRewriteError,
    RelationalJoinRewritePlan, RelationalJoinRewriteProblem, RelationalJoinTree,
    RequiredProperties,
};
use skein_sql::timing::measure_nanos;
use skein_storage::{RelationalState, RelationalTableSchema};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct PlannedSelectStatement {
    pub(super) statement: SelectStatement,
    pub(super) access_plan: Option<PreparedRelationalAccessPlan>,
    pub(super) join_planning: RelationalJoinPlanningOutcome,
}

struct BoundRelation<'a> {
    binding: BindingId,
    table: SqlTableName,
    alias: Option<String>,
    qualifier: String,
    schema: &'a RelationalTableSchema,
}

struct BoundJoinPredicate {
    id: RelationalJoinPredicateId,
    predicate: SqlPredicate,
    bindings: BindingSet,
}

struct BoundJoinOperator {
    operator: RelationalJoinOperator,
    right_binding: BindingId,
}

struct BoundJoinInputs {
    predicates: Vec<BoundJoinPredicate>,
    operators: Vec<BoundJoinOperator>,
}

struct PreparedGraphRelation {
    optimizer_relation: RelationalJoinRelation,
    base_accesses: Vec<(RelationalJoinAccessPath, RelationalAccessCandidate)>,
    join_accesses: Vec<(RelationalJoinAccessPath, RelationalJoinAccessCandidate)>,
}

pub(super) fn plan_select_join_order(
    select: SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_>,
    limits: RelationalQueryLimits,
    planning: RelationalJoinPlanningContext,
    binding_nanos: &mut u64,
) -> Result<PlannedSelectStatement> {
    let config = planning.enumeration;
    let syntax_order = select_relation_order(&select);
    if planning.directive == RelationalJoinPlanningDirective::SyntaxOrder {
        let outcome = RelationalJoinPlanningOutcome::explicit_syntax_order(syntax_order, config);
        return Ok(unchanged(select, outcome));
    }
    if let Err(reason) = join_enumeration_eligibility(&select) {
        let outcome = RelationalJoinPlanningOutcome::not_eligible(reason, syntax_order, config);
        return Ok(unchanged(select, outcome));
    }
    let relations = measure_nanos(binding_nanos, || bind_relations(&select, state))?;
    if !measure_nanos(binding_nanos, || {
        select_columns_resolve(&select, &relations)
    }) {
        let outcome = RelationalJoinPlanningOutcome::not_eligible(
            RelationalJoinPlanningReason::UnresolvedColumns,
            syntax_order,
            config,
        );
        return Ok(unchanged(select, outcome));
    }
    let Some(bound_joins) = measure_nanos(binding_nanos, || bind_join_inputs(&select, &relations))
    else {
        let outcome = RelationalJoinPlanningOutcome::not_eligible(
            RelationalJoinPlanningReason::UnsupportedJoinPredicate,
            syntax_order,
            config,
        );
        return Ok(unchanged(select, outcome));
    };
    if uses_relaxed_enumeration_gate(&select) && select.joins.len() == 1 {
        let mut syntax_access_plan =
            prepare_syntax_access_plan(&select, parameters, state, read_modes, limits)?;
        syntax_access_plan.finalize_physical_join_plan(&select, state, read_modes.index)?;
        if syntax_access_plan.uses_specialized_materialized_join() {
            let outcome = RelationalJoinPlanningOutcome::not_eligible(
                RelationalJoinPlanningReason::SpecializedJoinNotEnumerated,
                syntax_order,
                config,
            );
            return Ok(PlannedSelectStatement {
                statement: select,
                access_plan: Some(syntax_access_plan),
                join_planning: outcome,
            });
        }
    }
    let predicates = &bound_joins.predicates;
    let Some(graph_relations) = build_graph_relations(
        &select, parameters, state, read_modes, limits, &relations, predicates,
    )?
    else {
        let outcome = RelationalJoinPlanningOutcome::not_eligible(
            RelationalJoinPlanningReason::UnavailableAccessBinding,
            syntax_order,
            config,
        );
        return Ok(unchanged(select, outcome));
    };
    let Some(initial_tree) = build_initial_join_tree(&relations, &bound_joins.operators) else {
        return Err(SkeinError::Execution(
            "relational join planner invariant violated while building the initial join tree"
                .to_string(),
        ));
    };
    let all_inner = bound_joins
        .operators
        .iter()
        .all(|operator| operator.operator.kind == RelationalJoinOperatorKind::Inner);
    let post_join_filter = select.selection.as_ref().and_then(|predicate| {
        measure_nanos(binding_nanos, || {
            qualify_predicate(predicate, &relations)
                .and_then(|predicate| bind_null_rejection_predicate(&predicate, &relations))
        })
    });
    let problem = (select.selection.is_none() || post_join_filter.is_some()).then(|| {
        RelationalJoinRewriteProblem {
            relations: graph_relations
                .iter()
                .map(|relation| relation.optimizer_relation.clone())
                .collect(),
            initial_tree: initial_tree.clone(),
            post_join_filter: post_join_filter.clone(),
        }
    });
    let mut attempts = Vec::new();
    if let Some(problem) = &problem {
        match enumerate_relational_csg_cmp_joins_with_right_input_policy(
            problem,
            &RequiredProperties::default(),
            config,
            RelationalCsgCmpRightInputPolicy::ProbeOnly,
        ) {
            Ok(enumeration) => {
                let selected_bindings = csg_cmp_binding_order(&enumeration.plan.root);
                let selected_order = binding_order_names(&selected_bindings, &relations);
                let syntax_bindings = relations
                    .iter()
                    .map(|relation| relation.binding)
                    .collect::<Vec<_>>();
                let attempt = RelationalJoinPlanningAttempt::selected(
                    RelationalJoinPlanningStrategy::CsgCmpMemo,
                    selected_bindings != syntax_bindings
                        || enumeration.plan.root.has_materialized_right(),
                    enumeration.memo_groups,
                    enumeration.memo_expressions,
                    enumeration.plan.cost_breakdown,
                );
                let outcome = RelationalJoinPlanningOutcome::selected(
                    attempt,
                    selected_order,
                    config,
                    attempts,
                );
                return prepare_csg_cmp_select(
                    select,
                    &relations,
                    predicates,
                    &graph_relations,
                    enumeration.plan,
                    outcome,
                );
            }
            Err(error) => attempts.push(fallback_rewrite_attempt(
                RelationalJoinPlanningStrategy::CsgCmpMemo,
                &error,
            )?),
        }
    } else {
        attempts.push(RelationalJoinPlanningAttempt::not_eligible(
            RelationalJoinPlanningStrategy::CsgCmpMemo,
            RelationalJoinPlanningReason::UnsupportedPostJoinFilter,
        ));
    }
    if all_inner {
        let graph = RelationalJoinGraph {
            relations: graph_relations
                .iter()
                .map(|relation| relation.optimizer_relation.clone())
                .collect(),
            predicates: predicates
                .iter()
                .map(|predicate| RelationalJoinPredicate {
                    id: predicate.id,
                    bindings: predicate.bindings.clone(),
                })
                .collect(),
        };
        match enumerate_relational_inner_joins(&graph, &RequiredProperties::default(), config) {
            Ok(enumeration) => {
                let selected_bindings = enumeration.plan.binding_order();
                let syntax_bindings = relations
                    .iter()
                    .map(|relation| relation.binding)
                    .collect::<Vec<_>>();
                let reordered = selected_bindings != syntax_bindings;
                let selected_order = binding_order_names(&selected_bindings, &relations);
                let attempt = RelationalJoinPlanningAttempt::selected(
                    RelationalJoinPlanningStrategy::InnerJoinMemo,
                    reordered,
                    enumeration.memo_groups,
                    enumeration.memo_expressions,
                    enumeration.plan.cost_breakdown,
                );
                let outcome = RelationalJoinPlanningOutcome::selected(
                    attempt,
                    selected_order,
                    config,
                    attempts,
                );
                return prepare_inner_select(
                    select,
                    &relations,
                    predicates,
                    &graph_relations,
                    enumeration.plan,
                    outcome,
                );
            }
            Err(error) => attempts.push(fallback_enumeration_attempt(
                RelationalJoinPlanningStrategy::InnerJoinMemo,
                &error,
            )?),
        }
        let outcome = syntax_fallback_outcome(attempts, syntax_order, config)?;
        return Ok(unchanged(select, outcome));
    }

    let Some(problem) = problem else {
        let outcome = RelationalJoinPlanningOutcome::not_eligible_after_attempts(
            RelationalJoinPlanningReason::UnsupportedPostJoinFilter,
            syntax_order,
            config,
            attempts,
        );
        return Ok(unchanged(select, outcome));
    };
    let enumeration = match enumerate_relational_join_rewrites(
        &problem,
        &RequiredProperties::default(),
        config,
    ) {
        Ok(enumeration) => enumeration,
        Err(error) => {
            attempts.push(fallback_rewrite_attempt(
                RelationalJoinPlanningStrategy::InnerLeftJoinRewriteMemo,
                &error,
            )?);
            let outcome = syntax_fallback_outcome(attempts, syntax_order, config)?;
            return Ok(unchanged(select, outcome));
        }
    };
    let reordered = !rewrite_plan_matches_syntax(&enumeration.plan, &bound_joins.operators);
    let selected_order = binding_order_names(&enumeration.plan.binding_order(), &relations);
    let attempt = RelationalJoinPlanningAttempt::selected(
        RelationalJoinPlanningStrategy::InnerLeftJoinRewriteMemo,
        reordered,
        enumeration.memo_groups,
        enumeration.memo_expressions,
        enumeration.plan.cost_breakdown,
    );
    let outcome =
        RelationalJoinPlanningOutcome::selected(attempt, selected_order, config, attempts);
    prepare_outer_select(
        select,
        &relations,
        predicates,
        &graph_relations,
        enumeration.plan,
        outcome,
    )
}

fn fallback_enumeration_attempt(
    strategy: RelationalJoinPlanningStrategy,
    error: &RelationalJoinEnumerationError,
) -> Result<RelationalJoinPlanningAttempt> {
    RelationalJoinPlanningAttempt::fallback_from_enumeration(strategy, error)
        .ok_or_else(|| invariant_planning_error(strategy, error))
}

fn fallback_rewrite_attempt(
    strategy: RelationalJoinPlanningStrategy,
    error: &RelationalJoinRewriteError,
) -> Result<RelationalJoinPlanningAttempt> {
    RelationalJoinPlanningAttempt::fallback_from_rewrite(strategy, error)
        .ok_or_else(|| invariant_planning_error(strategy, error))
}

fn syntax_fallback_outcome(
    attempts: Vec<RelationalJoinPlanningAttempt>,
    selected_order: Vec<String>,
    config: RelationalJoinEnumerationConfig,
) -> Result<RelationalJoinPlanningOutcome> {
    RelationalJoinPlanningOutcome::fallback_to_syntax(attempts, selected_order, config).ok_or_else(
        || {
            SkeinError::Execution(
                "relational join planner invariant violated: syntax fallback has no fallback-eligible attempt"
                    .to_string(),
            )
        },
    )
}

fn invariant_planning_error(
    strategy: RelationalJoinPlanningStrategy,
    error: &dyn std::fmt::Display,
) -> SkeinError {
    SkeinError::Execution(format!(
        "relational join planner invariant violated in {}: {error}",
        strategy.as_str()
    ))
}

fn csg_cmp_binding_order(node: &RelationalCsgCmpPlanNode) -> Vec<BindingId> {
    fn collect(node: &RelationalCsgCmpPlanNode, bindings: &mut Vec<BindingId>) {
        match node {
            RelationalCsgCmpPlanNode::Relation { binding, .. } => bindings.push(*binding),
            RelationalCsgCmpPlanNode::Join { left, right, .. } => {
                collect(left, bindings);
                collect(right, bindings);
            }
        }
    }

    let mut bindings = Vec::new();
    collect(node, &mut bindings);
    bindings
}

fn unchanged(
    statement: SelectStatement,
    join_planning: RelationalJoinPlanningOutcome,
) -> PlannedSelectStatement {
    PlannedSelectStatement {
        statement,
        access_plan: None,
        join_planning,
    }
}

fn join_enumeration_eligibility(
    select: &SelectStatement,
) -> std::result::Result<(), RelationalJoinPlanningReason> {
    if select.joins.is_empty() {
        return Err(RelationalJoinPlanningReason::NoJoin);
    }
    if !select
        .joins
        .iter()
        .all(|join| matches!(join.kind, SqlJoinKind::Inner | SqlJoinKind::Left))
    {
        return Err(RelationalJoinPlanningReason::UnsupportedJoinKind);
    }
    if select.lock_strength.is_some() {
        return Err(RelationalJoinPlanningReason::LockingSelect);
    }
    Ok(())
}

fn uses_relaxed_enumeration_gate(select: &SelectStatement) -> bool {
    select
        .projection
        .iter()
        .any(|projection| matches!(projection, SelectProjection::Wildcard))
        || !(select.distinct
            || !select.order_by.is_empty()
            || !select.group_by.is_empty()
            || select.projection.iter().any(projection_contains_aggregate))
}

fn select_relation_order(select: &SelectStatement) -> Vec<String> {
    std::iter::once(
        select
            .from_alias
            .clone()
            .unwrap_or_else(|| select.from.name.clone()),
    )
    .chain(select.joins.iter().map(|join| {
        join.alias
            .clone()
            .unwrap_or_else(|| join.table.name.clone())
    }))
    .collect()
}

fn bind_relations<'a>(
    select: &SelectStatement,
    state: &'a RelationalState,
) -> Result<Vec<BoundRelation<'a>>> {
    let sources = std::iter::once((&select.from, &select.from_alias))
        .chain(select.joins.iter().map(|join| (&join.table, &join.alias)));
    sources
        .enumerate()
        .map(|(ordinal, (table, alias))| {
            let binding = BindingId::new(u32::try_from(ordinal).map_err(|_| {
                SkeinError::Execution("relational join has too many bindings".to_string())
            })?);
            let schema = state.table_schema(&table.name).ok_or_else(|| {
                SkeinError::Semantic(format!("unknown relational table {}", table.name))
            })?;
            Ok(BoundRelation {
                binding,
                table: table.clone(),
                alias: alias.clone(),
                qualifier: alias.clone().unwrap_or_else(|| table.name.clone()),
                schema,
            })
        })
        .collect()
}

fn bind_join_inputs(
    select: &SelectStatement,
    relations: &[BoundRelation<'_>],
) -> Option<BoundJoinInputs> {
    let mut predicates = Vec::new();
    let mut operators = Vec::new();
    for (ordinal, join) in select.joins.iter().enumerate() {
        let predicate = qualify_predicate(&join.on, relations)?;
        let mut conjuncts = Vec::new();
        collect_conjuncts(&predicate, &mut conjuncts);
        let mut predicate_ids = Vec::with_capacity(conjuncts.len());
        for conjunct in conjuncts {
            let bindings = predicate_bindings(&conjunct, relations)?;
            if bindings.len() < 2 {
                return None;
            }
            let id = RelationalJoinPredicateId::new(u32::try_from(predicates.len()).ok()?);
            predicate_ids.push(id);
            predicates.push(BoundJoinPredicate {
                id,
                predicate: conjunct,
                bindings,
            });
        }
        let operator_id = RelationalJoinOperatorId::new(u32::try_from(ordinal).ok()?);
        operators.push(BoundJoinOperator {
            operator: RelationalJoinOperator {
                id: operator_id,
                kind: match join.kind {
                    SqlJoinKind::Inner => RelationalJoinOperatorKind::Inner,
                    SqlJoinKind::Left => RelationalJoinOperatorKind::LeftOuter,
                },
                predicate_ids,
                predicate: bind_null_rejection_predicate(&predicate, relations)?,
            },
            right_binding: relations.get(ordinal + 1)?.binding,
        });
    }
    Some(BoundJoinInputs {
        predicates,
        operators,
    })
}

fn build_initial_join_tree(
    relations: &[BoundRelation<'_>],
    operators: &[BoundJoinOperator],
) -> Option<RelationalJoinTree> {
    let mut tree = RelationalJoinTree::Relation(relations.first()?.binding);
    for operator in operators {
        tree = RelationalJoinTree::join(
            operator.operator.clone(),
            tree,
            RelationalJoinTree::Relation(operator.right_binding),
        );
    }
    Some(tree)
}

fn build_graph_relations(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_>,
    limits: RelationalQueryLimits,
    relations: &[BoundRelation<'_>],
    predicates: &[BoundJoinPredicate],
) -> Result<Option<Vec<PreparedGraphRelation>>> {
    let mut graph_relations = Vec::with_capacity(relations.len());
    for relation in relations {
        let Some(graph_relation) = build_graph_relation(
            select, parameters, state, read_modes, limits, relations, relation, predicates,
        )?
        else {
            return Ok(None);
        };
        graph_relations.push(graph_relation);
    }
    Ok(Some(graph_relations))
}

#[allow(clippy::too_many_arguments)]
fn build_graph_relation(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_>,
    limits: RelationalQueryLimits,
    relations: &[BoundRelation<'_>],
    relation: &BoundRelation<'_>,
    predicates: &[BoundJoinPredicate],
) -> Result<Option<PreparedGraphRelation>> {
    let base = choose_base_access(RelationalBaseAccessPlanning {
        predicate: select.selection.as_ref(),
        order_by: &[],
        prefer_ordered_access: false,
        parameters,
        state,
        schema: relation.schema,
        table: &relation.table.name,
        qualifier: &relation.qualifier,
        cardinality_limit: limits.max_intermediate_rows.saturating_add(1),
        projection: projection_access_planning(read_modes.row, &relation.table.name),
    })?;
    let full_scan = full_scan_descriptor(
        state,
        &relation.table.name,
        read_modes
            .row
            .projection_estimated_rows(&relation.table.name),
    );
    let full_scan_path = RelationalJoinAccessPath::base_and_probe(full_scan.clone());
    let mut access_paths = vec![full_scan_path.clone()];
    let mut base_accesses = vec![(
        full_scan_path.clone(),
        RelationalAccessCandidate {
            descriptor: full_scan.clone(),
            access: RelationalBaseAccess::FullScan,
        },
    )];
    let mut join_accesses = vec![(
        full_scan_path,
        RelationalJoinAccessCandidate {
            descriptor: full_scan.clone(),
            access: RelationalJoinAccess::FullScan,
        },
    )];
    if base.descriptor != full_scan {
        let path = RelationalJoinAccessPath::base(base.descriptor.clone());
        access_paths.push(path.clone());
        base_accesses.push((path, base));
    }

    let mut predicate_groups = BTreeMap::<BindingSet, Vec<SqlPredicate>>::new();
    let mut candidate_predicates = Vec::new();
    for predicate in predicates
        .iter()
        .filter(|predicate| predicate.bindings.contains(relation.binding))
    {
        candidate_predicates.push(predicate.predicate.clone());
        predicate_groups
            .entry(predicate.bindings.clone())
            .or_default()
            .push(predicate.predicate.clone());
    }
    for group in predicate_groups.values() {
        if group.len() > 1 {
            candidate_predicates.push(combine_predicates(group.iter().cloned()));
        }
    }
    if predicate_groups.len() > 1 {
        candidate_predicates.push(combine_predicates(
            predicates
                .iter()
                .filter(|predicate| predicate.bindings.contains(relation.binding))
                .map(|predicate| predicate.predicate.clone()),
        ));
    }
    for predicate in candidate_predicates {
        let candidate = choose_join_access(
            &predicate,
            state,
            relation.schema,
            &relation.table.name,
            &relation.qualifier,
            read_modes.index,
            projection_access_planning(read_modes.row, &relation.table.name),
        )?;
        if candidate.descriptor.kind == RelationalAccessPathKind::FullScan {
            continue;
        }
        let Some(required_bindings) =
            join_access_bindings(&candidate.access, relations, relation.binding)
        else {
            return Ok(None);
        };
        let path = RelationalJoinAccessPath::probe(candidate.descriptor.clone(), required_bindings);
        if !access_paths.iter().any(|existing| existing == &path) {
            access_paths.push(path.clone());
            join_accesses.push((path, candidate));
        }
    }
    Ok(Some(PreparedGraphRelation {
        optimizer_relation: RelationalJoinRelation {
            binding: relation.binding,
            access_paths,
        },
        base_accesses,
        join_accesses,
    }))
}

fn full_scan_descriptor(
    state: &RelationalState,
    table: &str,
    row_count_override: Option<usize>,
) -> RelationalAccessPathDescriptor {
    RelationalAccessPathDescriptor {
        kind: RelationalAccessPathKind::FullScan,
        name: "__full_scan".to_string(),
        index_columns: Vec::new(),
        access_columns: BTreeSet::new(),
        equality_prefix_len: 0,
        order_prefix_len: 0,
        exclusive_range: false,
        reverse_order: false,
        unique_point: false,
        covering: false,
        requires_row_fetch: false,
        estimated_rows: row_count_override
            .unwrap_or_else(|| state.row_count(table))
            .max(1),
    }
}

fn join_access_bindings(
    access: &RelationalJoinAccess,
    relations: &[BoundRelation<'_>],
    target: BindingId,
) -> Option<BindingSet> {
    let columns = match access {
        RelationalJoinAccess::PrimaryKey(columns) => columns,
        RelationalJoinAccess::Index { columns, .. } => columns,
        RelationalJoinAccess::FullScan => return Some(BindingSet::new()),
    };
    let bindings: BindingSet = columns
        .iter()
        .map(|(_, column)| resolve_column_binding(column, relations))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .collect();
    (!bindings.contains(target) && !bindings.is_empty()).then_some(bindings)
}

#[derive(Clone, Copy)]
enum PreparedTreeRelationRole {
    Base,
    Probe,
}

fn prepare_csg_cmp_select(
    select: SelectStatement,
    relations: &[BoundRelation<'_>],
    predicates: &[BoundJoinPredicate],
    prepared_relations: &[PreparedGraphRelation],
    plan: RelationalCsgCmpPlan,
    join_planning: RelationalJoinPlanningOutcome,
) -> Result<PlannedSelectStatement> {
    let predicate_by_id = predicates
        .iter()
        .map(|predicate| (predicate.id, predicate.predicate.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut next_join_plan_index = 1usize;
    let root = prepare_csg_cmp_node(
        &plan.root,
        PreparedTreeRelationRole::Base,
        relations,
        prepared_relations,
        &predicate_by_id,
        &mut next_join_plan_index,
    )?;
    let mut leaves = Vec::new();
    root.visit_relations(&mut |relation| leaves.push(relation.clone()));
    let Some(first) = leaves.first() else {
        return Err(SkeinError::Execution(
            "CSG-CMP selected an empty relational join tree".to_string(),
        ));
    };
    let RelationalPhysicalAccess::Base(base_access) = &first.access else {
        return Err(SkeinError::Execution(
            "CSG-CMP join tree does not start with a base access".to_string(),
        ));
    };
    let join_accesses = leaves
        .iter()
        .skip(1)
        .map(|relation| match &relation.access {
            RelationalPhysicalAccess::Probe(access) => Ok(access.clone()),
            RelationalPhysicalAccess::Base(_) => {
                selected_materialized_join_display_access(prepared_relations, relation.binding)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PlannedSelectStatement {
        statement: select,
        access_plan: Some(PreparedRelationalAccessPlan {
            base_access: base_access.clone(),
            join_accesses,
            join_selection: None,
            physical_join_plan: Some(RelationalPhysicalJoinPlan::new(root, plan.cost_breakdown)),
        }),
        join_planning,
    })
}

fn prepare_csg_cmp_node(
    node: &RelationalCsgCmpPlanNode,
    role: PreparedTreeRelationRole,
    relations: &[BoundRelation<'_>],
    prepared_relations: &[PreparedGraphRelation],
    predicates: &BTreeMap<RelationalJoinPredicateId, SqlPredicate>,
    next_join_plan_index: &mut usize,
) -> Result<RelationalPhysicalJoinNode> {
    match node {
        RelationalCsgCmpPlanNode::Relation {
            binding,
            access_path,
        } => {
            let relation = relation_by_binding(relations, *binding);
            let access = match role {
                PreparedTreeRelationRole::Base => RelationalPhysicalAccess::Base(
                    selected_base_access(prepared_relations, *binding, access_path)?,
                ),
                PreparedTreeRelationRole::Probe => RelationalPhysicalAccess::Probe(
                    selected_join_access(prepared_relations, *binding, access_path)?,
                ),
            };
            Ok(RelationalPhysicalJoinNode::relation(
                *binding,
                relation.table.name.clone(),
                relation.qualifier.clone(),
                access,
            ))
        }
        RelationalCsgCmpPlanNode::Join {
            operator_id: _,
            operator_kind,
            predicate_ids,
            left,
            right,
        } => {
            let left = prepare_csg_cmp_node(
                left,
                PreparedTreeRelationRole::Base,
                relations,
                prepared_relations,
                predicates,
                next_join_plan_index,
            )?;
            let right_role = if matches!(right.as_ref(), RelationalCsgCmpPlanNode::Relation { .. })
            {
                PreparedTreeRelationRole::Probe
            } else {
                PreparedTreeRelationRole::Base
            };
            let right = prepare_csg_cmp_node(
                right,
                right_role,
                relations,
                prepared_relations,
                predicates,
                next_join_plan_index,
            )?;
            let predicates = predicate_ids
                .iter()
                .map(|id| {
                    predicates.get(id).cloned().ok_or_else(|| {
                        SkeinError::Execution(format!(
                            "CSG-CMP selected unknown relational predicate {}",
                            id.get()
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let prepared_operator_id = RelationalOperatorId::from_plan_index(*next_join_plan_index);
            *next_join_plan_index = next_join_plan_index.saturating_add(1);
            RelationalPhysicalJoinNode::join(
                prepared_operator_id,
                match operator_kind {
                    RelationalJoinOperatorKind::Inner => SqlJoinKind::Inner,
                    RelationalJoinOperatorKind::LeftOuter => SqlJoinKind::Left,
                },
                predicates,
                left,
                right,
            )
        }
    }
}

fn selected_materialized_join_display_access(
    prepared_relations: &[PreparedGraphRelation],
    binding: BindingId,
) -> Result<RelationalJoinAccessCandidate> {
    prepared_relation(prepared_relations, binding)?
        .join_accesses
        .iter()
        .find(|(path, _)| path.descriptor.kind == RelationalAccessPathKind::FullScan)
        .map(|(_, access)| access.clone())
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "CSG-CMP materialized binding {} has no full-scan display access",
                binding.get()
            ))
        })
}

fn prepare_inner_select(
    mut select: SelectStatement,
    relations: &[BoundRelation<'_>],
    predicates: &[BoundJoinPredicate],
    prepared_relations: &[PreparedGraphRelation],
    plan: skein_optimizer::RelationalJoinPlan,
    join_planning: RelationalJoinPlanningOutcome,
) -> Result<PlannedSelectStatement> {
    let reordered = join_planning.join_order_reordered();
    let access_plan = prepare_selected_access_plan(
        prepared_relations,
        plan.base_binding,
        &plan.base_access_path,
        plan.steps
            .iter()
            .map(|step| (step.binding, &step.access_path)),
        plan.cost_breakdown,
    )?;
    if reordered {
        let base = relation_by_binding(relations, plan.base_binding);
        select.from = base.table.clone();
        select.from_alias = base.alias.clone();
        let predicate_by_id = predicates
            .iter()
            .map(|predicate| (predicate.id, predicate.predicate.clone()))
            .collect::<BTreeMap<_, _>>();
        select.joins = plan
            .steps
            .into_iter()
            .map(|step| {
                let relation = relation_by_binding(relations, step.binding);
                let on = combine_predicates(step.activated_predicates.into_iter().map(|id| {
                    predicate_by_id
                        .get(&id)
                        .expect("enumerated predicate id came from the bound graph")
                        .clone()
                }));
                SqlJoin {
                    kind: SqlJoinKind::Inner,
                    table: relation.table.clone(),
                    alias: relation.alias.clone(),
                    on,
                }
            })
            .collect();
    }
    Ok(PlannedSelectStatement {
        statement: select,
        access_plan: Some(access_plan),
        join_planning,
    })
}

fn prepare_outer_select(
    mut select: SelectStatement,
    relations: &[BoundRelation<'_>],
    predicates: &[BoundJoinPredicate],
    prepared_relations: &[PreparedGraphRelation],
    plan: RelationalJoinRewritePlan,
    join_planning: RelationalJoinPlanningOutcome,
) -> Result<PlannedSelectStatement> {
    let reordered = join_planning.join_order_reordered();
    let access_plan = prepare_selected_access_plan(
        prepared_relations,
        plan.base_binding,
        &plan.base_access_path,
        plan.steps
            .iter()
            .map(|step| (step.binding, &step.access_path)),
        plan.cost_breakdown,
    )?;
    if reordered {
        let base = relation_by_binding(relations, plan.base_binding);
        select.from = base.table.clone();
        select.from_alias = base.alias.clone();
        let predicate_by_id = predicates
            .iter()
            .map(|predicate| (predicate.id, predicate.predicate.clone()))
            .collect::<BTreeMap<_, _>>();
        select.joins = plan
            .steps
            .into_iter()
            .map(|step| {
                let relation = relation_by_binding(relations, step.binding);
                let on = combine_predicates(step.predicate_ids.into_iter().map(|id| {
                    predicate_by_id
                        .get(&id)
                        .expect("enumerated predicate id came from the bound join operator")
                        .clone()
                }));
                SqlJoin {
                    kind: match step.operator_kind {
                        RelationalJoinOperatorKind::Inner => SqlJoinKind::Inner,
                        RelationalJoinOperatorKind::LeftOuter => SqlJoinKind::Left,
                    },
                    table: relation.table.clone(),
                    alias: relation.alias.clone(),
                    on,
                }
            })
            .collect();
    }
    Ok(PlannedSelectStatement {
        statement: select,
        access_plan: Some(access_plan),
        join_planning,
    })
}

fn prepare_selected_access_plan<'a>(
    prepared_relations: &[PreparedGraphRelation],
    base_binding: BindingId,
    base_path: &RelationalJoinAccessPath,
    joins: impl IntoIterator<Item = (BindingId, &'a RelationalJoinAccessPath)>,
    cost_breakdown: skein_optimizer::PlanCostBreakdown,
) -> Result<PreparedRelationalAccessPlan> {
    let base_access = selected_base_access(prepared_relations, base_binding, base_path)?;
    let selected_joins = joins.into_iter().collect::<Vec<_>>();
    let join_accesses = selected_joins
        .iter()
        .map(|(binding, path)| selected_join_access(prepared_relations, *binding, path))
        .collect::<Result<Vec<_>>>()?;
    Ok(PreparedRelationalAccessPlan {
        base_access,
        join_accesses,
        join_selection: Some(PreparedRelationalJoinSelection {
            base_binding,
            join_bindings: selected_joins
                .into_iter()
                .map(|(binding, _)| binding)
                .collect(),
            cost_breakdown,
        }),
        physical_join_plan: None,
    })
}

fn selected_base_access(
    prepared_relations: &[PreparedGraphRelation],
    binding: BindingId,
    path: &RelationalJoinAccessPath,
) -> Result<RelationalAccessCandidate> {
    prepared_relation(prepared_relations, binding)?
        .base_accesses
        .iter()
        .find(|(candidate, _)| candidate == path)
        .map(|(_, access)| access.clone())
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "optimizer selected an unavailable base access path for binding {}",
                binding.get()
            ))
        })
}

fn selected_join_access(
    prepared_relations: &[PreparedGraphRelation],
    binding: BindingId,
    path: &RelationalJoinAccessPath,
) -> Result<RelationalJoinAccessCandidate> {
    prepared_relation(prepared_relations, binding)?
        .join_accesses
        .iter()
        .find(|(candidate, _)| candidate == path)
        .map(|(_, access)| access.clone())
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "optimizer selected an unavailable join access path for binding {}",
                binding.get()
            ))
        })
}

fn prepared_relation(
    prepared_relations: &[PreparedGraphRelation],
    binding: BindingId,
) -> Result<&PreparedGraphRelation> {
    prepared_relations
        .iter()
        .find(|relation| relation.optimizer_relation.binding == binding)
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "optimizer selected an unknown relational binding {}",
                binding.get()
            ))
        })
}

fn rewrite_plan_matches_syntax(
    plan: &RelationalJoinRewritePlan,
    operators: &[BoundJoinOperator],
) -> bool {
    plan.steps.len() == operators.len()
        && plan.steps.iter().zip(operators).all(|(step, operator)| {
            step.operator_id == operator.operator.id
                && step.operator_kind == operator.operator.kind
                && step.binding == operator.right_binding
        })
}

fn relation_by_binding<'relations, 'schema>(
    relations: &'relations [BoundRelation<'schema>],
    binding: BindingId,
) -> &'relations BoundRelation<'schema> {
    relations
        .iter()
        .find(|relation| relation.binding == binding)
        .expect("enumerated binding came from the bound relation set")
}

fn binding_order_names(bindings: &[BindingId], relations: &[BoundRelation<'_>]) -> Vec<String> {
    bindings
        .iter()
        .map(|binding| relation_by_binding(relations, *binding).qualifier.clone())
        .collect()
}

fn combine_predicates(predicates: impl IntoIterator<Item = SqlPredicate>) -> SqlPredicate {
    predicates
        .into_iter()
        .reduce(|left, right| SqlPredicate::And(Box::new(left), Box::new(right)))
        .expect("join predicate group is non-empty")
}

fn collect_conjuncts(predicate: &SqlPredicate, output: &mut Vec<SqlPredicate>) {
    match predicate {
        SqlPredicate::And(left, right) => {
            collect_conjuncts(left, output);
            collect_conjuncts(right, output);
        }
        predicate => output.push(predicate.clone()),
    }
}

fn qualify_predicate(
    predicate: &SqlPredicate,
    relations: &[BoundRelation<'_>],
) -> Option<SqlPredicate> {
    Some(match predicate {
        SqlPredicate::And(left, right) => SqlPredicate::And(
            Box::new(qualify_predicate(left, relations)?),
            Box::new(qualify_predicate(right, relations)?),
        ),
        SqlPredicate::Or(left, right) => SqlPredicate::Or(
            Box::new(qualify_predicate(left, relations)?),
            Box::new(qualify_predicate(right, relations)?),
        ),
        SqlPredicate::Not(predicate) => {
            SqlPredicate::Not(Box::new(qualify_predicate(predicate, relations)?))
        }
        SqlPredicate::Compare { left, op, right } => SqlPredicate::Compare {
            left: qualify_column(left, relations)?,
            op: *op,
            right: right.clone(),
        },
        SqlPredicate::CompareColumns { left, op, right } => SqlPredicate::CompareColumns {
            left: qualify_column(left, relations)?,
            op: *op,
            right: qualify_column(right, relations)?,
        },
        SqlPredicate::InList {
            left,
            values,
            negated,
        } => SqlPredicate::InList {
            left: qualify_column(left, relations)?,
            values: values.clone(),
            negated: *negated,
        },
        SqlPredicate::Like {
            left,
            pattern,
            case_insensitive,
            negated,
            escape,
        } => SqlPredicate::Like {
            left: qualify_column(left, relations)?,
            pattern: pattern.clone(),
            case_insensitive: *case_insensitive,
            negated: *negated,
            escape: *escape,
        },
        SqlPredicate::IsNull { column, negated } => SqlPredicate::IsNull {
            column: qualify_column(column, relations)?,
            negated: *negated,
        },
    })
}

fn bind_null_rejection_predicate(
    predicate: &SqlPredicate,
    relations: &[BoundRelation<'_>],
) -> Option<BoundPredicate> {
    Some(match predicate {
        SqlPredicate::And(left, right) => BoundPredicate::And(vec![
            bind_null_rejection_predicate(left, relations)?,
            bind_null_rejection_predicate(right, relations)?,
        ]),
        SqlPredicate::Or(left, right) => BoundPredicate::Or(vec![
            bind_null_rejection_predicate(left, relations)?,
            bind_null_rejection_predicate(right, relations)?,
        ]),
        SqlPredicate::Not(predicate) => BoundPredicate::Not(Box::new(
            bind_null_rejection_predicate(predicate, relations)?,
        )),
        SqlPredicate::Compare { left, right, .. } => BoundPredicate::Comparison {
            left: bind_null_rejection_column(left, relations)?,
            right: bind_null_rejection_value(right),
        },
        SqlPredicate::CompareColumns { left, right, .. } => BoundPredicate::Comparison {
            left: bind_null_rejection_column(left, relations)?,
            right: bind_null_rejection_column(right, relations)?,
        },
        SqlPredicate::InList {
            left,
            values,
            negated,
        } => BoundPredicate::InList {
            expression: bind_null_rejection_column(left, relations)?,
            values: values.iter().map(bind_null_rejection_value).collect(),
            negated: *negated,
        },
        SqlPredicate::Like { .. } => return None,
        SqlPredicate::IsNull { column, negated } => {
            let column = bind_null_rejection_column(column, relations)?;
            if *negated {
                BoundPredicate::IsNotNull(column)
            } else {
                BoundPredicate::IsNull(column)
            }
        }
    })
}

fn bind_null_rejection_column(
    column: &SqlColumnRef,
    relations: &[BoundRelation<'_>],
) -> Option<BoundScalarExpression> {
    let binding = resolve_column_binding(column, relations)?;
    let relation = relation_by_binding(relations, binding);
    let position = relation.schema.column_position(&column.name)?;
    Some(BoundScalarExpression::BindingValue {
        binding,
        nullability: if relation.schema.columns[position].nullable {
            ScalarNullability::MaybeNull
        } else {
            ScalarNullability::AlwaysNonNull
        },
    })
}

fn bind_null_rejection_value(value: &crate::sql::SqlValue) -> BoundScalarExpression {
    match value {
        crate::sql::SqlValue::Literal(Value::Null) => BoundScalarExpression::LiteralNull,
        crate::sql::SqlValue::Literal(_) => BoundScalarExpression::LiteralNonNull,
        crate::sql::SqlValue::Parameter(_) => BoundScalarExpression::Parameter {
            nullability: ScalarNullability::MaybeNull,
        },
    }
}

fn qualify_column(column: &SqlColumnRef, relations: &[BoundRelation<'_>]) -> Option<SqlColumnRef> {
    let binding = resolve_column_binding(column, relations)?;
    let relation = relation_by_binding(relations, binding);
    Some(SqlColumnRef {
        qualifier: Some(relation.qualifier.clone()),
        name: column.name.clone(),
    })
}

fn resolve_column_binding(
    column: &SqlColumnRef,
    relations: &[BoundRelation<'_>],
) -> Option<BindingId> {
    let mut candidates = relations.iter().filter(|relation| {
        column.qualifier.as_deref().is_none_or(|qualifier| {
            qualifier == relation.qualifier || qualifier == relation.table.name
        }) && relation.schema.column_position(&column.name).is_some()
    });
    let binding = candidates.next()?.binding;
    candidates.next().is_none().then_some(binding)
}

fn predicate_bindings(
    predicate: &SqlPredicate,
    relations: &[BoundRelation<'_>],
) -> Option<BindingSet> {
    let mut columns = Vec::new();
    collect_predicate_columns(predicate, &mut columns);
    columns
        .into_iter()
        .map(|column| resolve_column_binding(column, relations))
        .collect()
}

fn collect_predicate_columns<'a>(predicate: &'a SqlPredicate, output: &mut Vec<&'a SqlColumnRef>) {
    match predicate {
        SqlPredicate::And(left, right) | SqlPredicate::Or(left, right) => {
            collect_predicate_columns(left, output);
            collect_predicate_columns(right, output);
        }
        SqlPredicate::Not(predicate) => collect_predicate_columns(predicate, output),
        SqlPredicate::Compare { left, .. }
        | SqlPredicate::InList { left, .. }
        | SqlPredicate::Like { left, .. }
        | SqlPredicate::IsNull { column: left, .. } => output.push(left),
        SqlPredicate::CompareColumns { left, right, .. } => {
            output.push(left);
            output.push(right);
        }
    }
}

fn select_columns_resolve(select: &SelectStatement, relations: &[BoundRelation<'_>]) -> bool {
    select.projection.iter().all(|projection| match projection {
        SelectProjection::Wildcard => true,
        SelectProjection::Column { name, .. } => resolve_column_binding(name, relations).is_some(),
        SelectProjection::Expression { expression, .. } => {
            expression_columns_resolve(expression, relations)
        }
    }) && select
        .selection
        .as_ref()
        .is_none_or(|predicate| predicate_bindings(predicate, relations).is_some())
        && select
            .group_by
            .iter()
            .all(|column| resolve_column_binding(column, relations).is_some())
        && select.order_by.iter().all(|item| {
            resolve_relational_order_target(select, item).is_ok_and(|target| match target {
                RelationalOrderTarget::InputColumn(column) => {
                    resolve_column_binding(column, relations).is_some()
                }
                RelationalOrderTarget::ProjectionColumn { column, .. } => {
                    resolve_column_binding(column, relations).is_some()
                }
                RelationalOrderTarget::ProjectionExpression { expression, .. } => {
                    expression_columns_resolve(expression, relations)
                }
            })
        })
}

fn expression_columns_resolve(expression: &SqlExpression, relations: &[BoundRelation<'_>]) -> bool {
    match expression {
        SqlExpression::Column(column) => resolve_column_binding(column, relations).is_some(),
        SqlExpression::Value(_) => true,
        SqlExpression::Function { arguments, .. } => {
            arguments.iter().all(|argument| match argument {
                SqlFunctionArgument::Expression(expression) => {
                    expression_columns_resolve(expression, relations)
                }
                SqlFunctionArgument::Wildcard => true,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::SqlStatement;

    fn select(sql: &str) -> SelectStatement {
        let prepared = skein_sql::prepare_postgres_sql(sql).expect("valid PostgreSQL SELECT");
        let SqlStatement::Select(select) = prepared.statement else {
            panic!("expected SELECT statement");
        };
        select
    }

    #[test]
    fn join_enumeration_accepts_supported_join_shapes() {
        for sql in [
            "SELECT c.id FROM chunks AS c INNER JOIN documents AS d ON d.id = c.document_id WHERE d.owner = $1 ORDER BY c.id",
            "SELECT COUNT(*) FROM chunks AS c INNER JOIN documents AS d ON d.id = c.document_id",
            "SELECT c.id FROM chunks AS c INNER JOIN documents AS d ON d.id = c.document_id INNER JOIN owners AS o ON o.id = d.owner_id ORDER BY c.id",
            "SELECT c.id FROM chunks AS c LEFT JOIN documents AS d ON d.id = c.document_id WHERE d.id IS NOT NULL ORDER BY c.id",
            "SELECT c.id FROM chunks AS c INNER JOIN documents AS d ON d.id = c.document_id",
            "SELECT * FROM chunks AS c INNER JOIN documents AS d ON d.id = c.document_id",
        ] {
            assert!(join_enumeration_eligibility(&select(sql)).is_ok(), "{sql}");
        }
    }

    #[test]
    fn join_enumeration_rejects_locking_selects() {
        let sql = "SELECT c.id FROM chunks AS c INNER JOIN documents AS d ON d.id = c.document_id ORDER BY c.id FOR UPDATE";
        assert_eq!(
            join_enumeration_eligibility(&select(sql)),
            Err(RelationalJoinPlanningReason::LockingSelect)
        );
    }

    #[test]
    fn join_enumeration_reports_no_join() {
        assert_eq!(
            join_enumeration_eligibility(&select("SELECT id FROM chunks ORDER BY id")),
            Err(RelationalJoinPlanningReason::NoJoin)
        );
    }

    #[test]
    fn invariant_enumeration_failure_is_not_a_fallback_attempt() {
        let error = fallback_enumeration_attempt(
            RelationalJoinPlanningStrategy::InnerJoinMemo,
            &RelationalJoinEnumerationError::EmptyGraph,
        )
        .expect_err("invalid optimizer state must fail closed");
        assert!(matches!(
            error,
            SkeinError::Execution(message)
                if message.contains("relational join planner invariant violated")
                    && message.contains("inner_join_memo")
        ));

        let error = syntax_fallback_outcome(
            Vec::new(),
            vec!["a".to_string(), "b".to_string()],
            RelationalJoinEnumerationConfig::default(),
        )
        .expect_err("syntax fallback requires an eligible failed attempt");
        assert!(matches!(
            error,
            SkeinError::Execution(message)
                if message.contains("syntax fallback has no fallback-eligible attempt")
        ));
    }

    #[test]
    fn selected_access_plan_retains_the_optimizer_access_contract() {
        let outer = BindingId::new(0);
        let inner = BindingId::new(1);
        let base_descriptor = RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::FullScan,
            name: "__full_scan".to_string(),
            index_columns: Vec::new(),
            access_columns: BTreeSet::new(),
            equality_prefix_len: 0,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: false,
            estimated_rows: 5,
        };
        let join_descriptor = RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::Index,
            name: "idx_inner_outer".to_string(),
            index_columns: vec!["outer_id".to_string()],
            access_columns: BTreeSet::from(["outer_id".to_string()]),
            equality_prefix_len: 1,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: true,
            estimated_rows: 7,
        };
        let base_path = RelationalJoinAccessPath::base(base_descriptor.clone());
        let join_path =
            RelationalJoinAccessPath::probe(join_descriptor.clone(), BindingSet::from([outer]));
        let prepared_relations = vec![
            PreparedGraphRelation {
                optimizer_relation: RelationalJoinRelation {
                    binding: outer,
                    access_paths: vec![base_path.clone()],
                },
                base_accesses: vec![(
                    base_path.clone(),
                    RelationalAccessCandidate {
                        descriptor: base_descriptor,
                        access: RelationalBaseAccess::FullScan,
                    },
                )],
                join_accesses: Vec::new(),
            },
            PreparedGraphRelation {
                optimizer_relation: RelationalJoinRelation {
                    binding: inner,
                    access_paths: vec![join_path.clone()],
                },
                base_accesses: Vec::new(),
                join_accesses: vec![(
                    join_path.clone(),
                    RelationalJoinAccessCandidate {
                        descriptor: join_descriptor,
                        access: RelationalJoinAccess::Index {
                            name: "idx_inner_outer".to_string(),
                            columns: vec![(
                                "outer_id".to_string(),
                                SqlColumnRef {
                                    qualifier: Some("outer".to_string()),
                                    name: "id".to_string(),
                                },
                            )],
                        },
                    },
                )],
            },
        ];
        let cost_breakdown = skein_optimizer::PlanCostBreakdown::new(35, 40, 0, 0, 0);

        let prepared = prepare_selected_access_plan(
            &prepared_relations,
            outer,
            &base_path,
            [(inner, &join_path)],
            cost_breakdown,
        )
        .expect("prepare optimizer-selected relational access paths");

        assert_eq!(prepared.base_access.descriptor.name, "__full_scan");
        assert_eq!(prepared.join_accesses[0].descriptor.name, "idx_inner_outer");
        let RelationalJoinAccess::Index { name, columns } = &prepared.join_accesses[0].access
        else {
            panic!("expected prepared index probe");
        };
        assert_eq!(name, "idx_inner_outer");
        assert_eq!(columns[0].1.qualifier.as_deref(), Some("outer"));
        assert_eq!(
            prepared.join_selection,
            Some(PreparedRelationalJoinSelection {
                base_binding: outer,
                join_bindings: vec![inner],
                cost_breakdown,
            })
        );
    }
}
