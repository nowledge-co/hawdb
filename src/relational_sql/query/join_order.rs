use super::{
    choose_base_access, choose_join_access, projection_contains_aggregate,
    RelationalBaseAccessPlanning, RelationalJoinAccess, RelationalQueryLimits,
};
use crate::error::{Result, SkeinError};
use crate::sql::{
    SelectProjection, SelectStatement, SqlColumnRef, SqlExpression, SqlFunctionArgument, SqlJoin,
    SqlJoinKind, SqlPredicate, SqlTableName,
};
use crate::Value;
use skein_expression::{
    BindingId, BindingSet, BoundPredicate, BoundScalarExpression, ScalarNullability,
};
use skein_optimizer::{
    enumerate_relational_inner_joins, enumerate_relational_join_rewrites,
    RelationalAccessPathDescriptor, RelationalAccessPathKind, RelationalJoinAccessPath,
    RelationalJoinEnumerationConfig, RelationalJoinGraph, RelationalJoinOperator,
    RelationalJoinOperatorId, RelationalJoinOperatorKind, RelationalJoinPredicate,
    RelationalJoinPredicateId, RelationalJoinRelation, RelationalJoinRewritePlan,
    RelationalJoinRewriteProblem, RelationalJoinTree, RequiredProperties,
};
use skein_storage::{RelationalState, RelationalTableSchema};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct PlannedSelectStatement {
    pub(super) statement: SelectStatement,
    pub(super) join_order_reordered: bool,
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

pub(super) fn plan_select_join_order(
    select: SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    limits: RelationalQueryLimits,
) -> Result<PlannedSelectStatement> {
    if !supports_join_enumeration(&select) {
        return Ok(unchanged(select));
    }
    let relations = bind_relations(&select, state)?;
    if !select_columns_resolve(&select, &relations) {
        return Ok(unchanged(select));
    }
    let Some(bound_joins) = bind_join_inputs(&select, &relations) else {
        return Ok(unchanged(select));
    };
    let predicates = &bound_joins.predicates;
    let Some(graph_relations) =
        build_graph_relations(&select, parameters, state, limits, &relations, predicates)?
    else {
        return Ok(unchanged(select));
    };
    if bound_joins
        .operators
        .iter()
        .all(|operator| operator.operator.kind == RelationalJoinOperatorKind::Inner)
    {
        let graph = RelationalJoinGraph {
            relations: graph_relations,
            predicates: predicates
                .iter()
                .map(|predicate| RelationalJoinPredicate {
                    id: predicate.id,
                    bindings: predicate.bindings.clone(),
                })
                .collect(),
        };
        let Ok(enumeration) = enumerate_relational_inner_joins(
            &graph,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        ) else {
            return Ok(unchanged(select));
        };
        let selected_order = enumeration.plan.binding_order();
        let syntax_order = relations
            .iter()
            .map(|relation| relation.binding)
            .collect::<Vec<_>>();
        if selected_order == syntax_order {
            return Ok(unchanged(select));
        }
        return rebuild_inner_select(select, &relations, predicates, enumeration.plan);
    }

    let Some(initial_tree) = build_initial_join_tree(&relations, &bound_joins.operators) else {
        return Ok(unchanged(select));
    };
    let post_join_filter = match select.selection.as_ref() {
        Some(predicate) => {
            let Some(predicate) = qualify_predicate(predicate, &relations)
                .and_then(|predicate| bind_null_rejection_predicate(&predicate, &relations))
            else {
                return Ok(unchanged(select));
            };
            Some(predicate)
        }
        None => None,
    };
    let problem = RelationalJoinRewriteProblem {
        relations: graph_relations,
        initial_tree,
        post_join_filter,
    };
    let Ok(enumeration) = enumerate_relational_join_rewrites(
        &problem,
        &RequiredProperties::default(),
        RelationalJoinEnumerationConfig::default(),
    ) else {
        return Ok(unchanged(select));
    };
    if rewrite_plan_matches_syntax(&enumeration.plan, &bound_joins.operators) {
        return Ok(unchanged(select));
    }
    rebuild_outer_select(select, &relations, predicates, enumeration.plan)
}

fn unchanged(statement: SelectStatement) -> PlannedSelectStatement {
    PlannedSelectStatement {
        statement,
        join_order_reordered: false,
    }
}

fn supports_join_enumeration(select: &SelectStatement) -> bool {
    !select.joins.is_empty()
        && select
            .joins
            .iter()
            .all(|join| matches!(join.kind, SqlJoinKind::Inner | SqlJoinKind::Left))
        && select.lock_strength.is_none()
        && !select
            .projection
            .iter()
            .any(|projection| matches!(projection, SelectProjection::Wildcard))
        && (select.distinct
            || !select.order_by.is_empty()
            || !select.group_by.is_empty()
            || select.projection.iter().any(projection_contains_aggregate))
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
    limits: RelationalQueryLimits,
    relations: &[BoundRelation<'_>],
    predicates: &[BoundJoinPredicate],
) -> Result<Option<Vec<RelationalJoinRelation>>> {
    let mut graph_relations = Vec::with_capacity(relations.len());
    for relation in relations {
        let Some(graph_relation) = build_graph_relation(
            select, parameters, state, limits, relations, relation, predicates,
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
    limits: RelationalQueryLimits,
    relations: &[BoundRelation<'_>],
    relation: &BoundRelation<'_>,
    predicates: &[BoundJoinPredicate],
) -> Result<Option<RelationalJoinRelation>> {
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
    })?;
    let full_scan = full_scan_descriptor(state, &relation.table.name);
    let mut access_paths = vec![RelationalJoinAccessPath::base_and_probe(full_scan.clone())];
    if base.descriptor != full_scan {
        access_paths.push(RelationalJoinAccessPath::base(base.descriptor));
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
        )?;
        if candidate.descriptor.kind == RelationalAccessPathKind::FullScan {
            continue;
        }
        let Some(required_bindings) =
            join_access_bindings(&candidate.access, relations, relation.binding)
        else {
            return Ok(None);
        };
        let path = RelationalJoinAccessPath::probe(candidate.descriptor, required_bindings);
        if !access_paths.iter().any(|existing| existing == &path) {
            access_paths.push(path);
        }
    }
    Ok(Some(RelationalJoinRelation {
        binding: relation.binding,
        access_paths,
    }))
}

fn full_scan_descriptor(state: &RelationalState, table: &str) -> RelationalAccessPathDescriptor {
    RelationalAccessPathDescriptor {
        kind: RelationalAccessPathKind::FullScan,
        name: "__full_scan".to_string(),
        index_columns: Vec::new(),
        access_columns: BTreeSet::new(),
        equality_prefix_len: 0,
        order_prefix_len: 0,
        unique_point: false,
        covering: false,
        requires_row_fetch: false,
        estimated_rows: state.row_count(table).max(1),
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

fn rebuild_inner_select(
    mut select: SelectStatement,
    relations: &[BoundRelation<'_>],
    predicates: &[BoundJoinPredicate],
    plan: skein_optimizer::RelationalJoinPlan,
) -> Result<PlannedSelectStatement> {
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
    Ok(PlannedSelectStatement {
        statement: select,
        join_order_reordered: true,
    })
}

fn rebuild_outer_select(
    mut select: SelectStatement,
    relations: &[BoundRelation<'_>],
    predicates: &[BoundJoinPredicate],
    plan: RelationalJoinRewritePlan,
) -> Result<PlannedSelectStatement> {
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
    Ok(PlannedSelectStatement {
        statement: select,
        join_order_reordered: true,
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
        | SqlPredicate::IsNull { column: left, .. } => output.push(left),
        SqlPredicate::CompareColumns { left, right, .. } => {
            output.push(left);
            output.push(right);
        }
    }
}

fn select_columns_resolve(select: &SelectStatement, relations: &[BoundRelation<'_>]) -> bool {
    select.projection.iter().all(|projection| match projection {
        SelectProjection::Wildcard => false,
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
            .chain(select.order_by.iter().map(|item| &item.column))
            .all(|column| resolve_column_binding(column, relations).is_some())
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
    fn join_enumeration_accepts_multi_join_stable_shapes() {
        for sql in [
            "SELECT c.id FROM chunks AS c INNER JOIN documents AS d ON d.id = c.document_id WHERE d.owner = $1 ORDER BY c.id",
            "SELECT COUNT(*) FROM chunks AS c INNER JOIN documents AS d ON d.id = c.document_id",
            "SELECT c.id FROM chunks AS c INNER JOIN documents AS d ON d.id = c.document_id INNER JOIN owners AS o ON o.id = d.owner_id ORDER BY c.id",
            "SELECT c.id FROM chunks AS c LEFT JOIN documents AS d ON d.id = c.document_id WHERE d.id IS NOT NULL ORDER BY c.id",
        ] {
            assert!(supports_join_enumeration(&select(sql)), "{sql}");
        }
    }

    #[test]
    fn join_enumeration_rejects_semantically_unstable_shapes() {
        for sql in [
            "SELECT * FROM chunks AS c INNER JOIN documents AS d ON d.id = c.document_id ORDER BY c.id",
            "SELECT c.id FROM chunks AS c INNER JOIN documents AS d ON d.id = c.document_id",
            "SELECT c.id FROM chunks AS c INNER JOIN documents AS d ON d.id = c.document_id ORDER BY c.id FOR UPDATE",
        ] {
            assert!(!supports_join_enumeration(&select(sql)), "{sql}");
        }
    }
}
