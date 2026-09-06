//! Hypergraph csg-cmp enumeration for bound relational join operators.

use crate::{
    analyze_relational_join_conflicts,
    relational_join_cost::{
        estimate_relational_access_cost, estimate_relational_join_cost, RelationalJoinCardinality,
        RelationalJoinRightInput, RelationalJoinSelectivity,
    },
    relational_join_rewrite::validate_problem_relations,
    GroupId, Memo, PhysicalProperties, PlanCost, PlanCostBreakdown, RelationalJoinAccessPath,
    RelationalJoinConflictAnalysis, RelationalJoinEnumerationConfig,
    RelationalJoinEnumerationError, RelationalJoinOperatorId, RelationalJoinOperatorKind,
    RelationalJoinPredicateId, RelationalJoinRewriteError, RelationalJoinRewriteProblem,
    RequiredProperties,
};
use skein_expression::{BindingId, BindingSet};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Execution capability for a CSG-CMP join's non-singleton right input.
///
/// A materialized right input is only safe when the executor has a bounded
/// materialization implementation, such as a spill-capable blocking operator.
/// Callers must select this policy according to their executor's capabilities.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RelationalCsgCmpRightInputPolicy {
    #[default]
    AllowMaterialized,
    ProbeOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RelationalEquiJoinAlgorithm {
    Hash,
    Merge,
}

impl RelationalEquiJoinAlgorithm {
    pub const fn right_input(self) -> RelationalJoinRightInput {
        match self {
            Self::Hash => RelationalJoinRightInput::Hash,
            Self::Merge => RelationalJoinRightInput::Merge,
        }
    }
}

/// A pre-bound implementation supported by the caller's two-relation kernel.
/// It competes inside memo selection, not after choosing a join order. Predicate
/// IDs name the complete operator, including residual predicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalCsgCmpJoinImplementation {
    pub operator_id: RelationalJoinOperatorId,
    pub operator_kind: RelationalJoinOperatorKind,
    pub predicate_ids: Vec<RelationalJoinPredicateId>,
    pub left_binding: BindingId,
    pub left_access: RelationalJoinAccessPath,
    pub right_binding: BindingId,
    pub right_access: RelationalJoinAccessPath,
    pub algorithm: RelationalEquiJoinAlgorithm,
    pub selectivity: RelationalJoinSelectivity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalCsgCmpAlternative {
    pub left_bindings: BindingSet,
    pub right_bindings: BindingSet,
    pub operator_id: RelationalJoinOperatorId,
    pub operator_kind: RelationalJoinOperatorKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalCsgCmpPlanNode {
    Relation {
        binding: BindingId,
        access_path: RelationalJoinAccessPath,
    },
    Join {
        operator_id: RelationalJoinOperatorId,
        operator_kind: RelationalJoinOperatorKind,
        predicate_ids: Vec<RelationalJoinPredicateId>,
        implementation: Option<Box<RelationalCsgCmpJoinImplementation>>,
        left: Box<Self>,
        right: Box<Self>,
    },
}

impl RelationalCsgCmpPlanNode {
    pub fn bindings(&self) -> BindingSet {
        match self {
            Self::Relation { binding, .. } => (*binding).into(),
            Self::Join { left, right, .. } => binding_union(&left.bindings(), &right.bindings()),
        }
    }

    fn stable_key(&self) -> String {
        match self {
            Self::Relation {
                binding,
                access_path,
            } => format!("{}:{}", binding.get(), access_path.descriptor.name),
            Self::Join {
                operator_id,
                implementation,
                left,
                right,
                ..
            } => format!(
                "({})/{}:{:?}:({})",
                left.stable_key(),
                operator_id.get(),
                implementation
                    .as_ref()
                    .map(|implementation| implementation.algorithm),
                right.stable_key()
            ),
        }
    }

    pub fn has_materialized_right(&self) -> bool {
        match self {
            Self::Relation { .. } => false,
            Self::Join { left, right, .. } => {
                matches!(right.as_ref(), Self::Join { .. })
                    || left.has_materialized_right()
                    || right.has_materialized_right()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalCsgCmpPlan {
    pub root: RelationalCsgCmpPlanNode,
    pub cost_breakdown: PlanCostBreakdown,
    pub properties: PhysicalProperties,
}

impl RelationalCsgCmpPlan {
    pub fn cost(&self) -> PlanCost {
        self.cost_breakdown.as_plan_cost()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalCsgCmpEnumeration {
    pub plan: RelationalCsgCmpPlan,
    pub conflict_analysis: RelationalJoinConflictAnalysis,
    pub alternatives: Vec<RelationalCsgCmpAlternative>,
    pub memo_groups: usize,
    pub memo_expressions: usize,
    pub root_alternatives: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SemanticKey {
    bindings: BindingSet,
    applied_operators: BTreeSet<RelationalJoinOperatorId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CsgCmpExpression {
    Relation(BindingId),
    Join {
        left: GroupId,
        right: GroupId,
        operator_id: RelationalJoinOperatorId,
        operator_kind: RelationalJoinOperatorKind,
        predicate_ids: Vec<RelationalJoinPredicateId>,
    },
}

struct CsgCmpMemo {
    memo: Memo<CsgCmpExpression>,
    groups: BTreeMap<SemanticKey, GroupId>,
    keys: BTreeMap<GroupId, SemanticKey>,
    alternatives: Vec<RelationalCsgCmpAlternative>,
    expression_count: usize,
}

impl CsgCmpMemo {
    fn new() -> Self {
        Self {
            memo: Memo::default(),
            groups: BTreeMap::new(),
            keys: BTreeMap::new(),
            alternatives: Vec::new(),
            expression_count: 0,
        }
    }

    fn add(
        &mut self,
        key: SemanticKey,
        expression: CsgCmpExpression,
        alternative: Option<RelationalCsgCmpAlternative>,
        config: RelationalJoinEnumerationConfig,
    ) -> Result<(), RelationalJoinRewriteError> {
        let required_expressions = self.expression_count.saturating_add(1);
        if required_expressions > config.max_expressions {
            return Err(RelationalJoinEnumerationError::ExpressionBudgetExceeded {
                required_expressions,
                max_expressions: config.max_expressions,
            }
            .into());
        }
        if let Some(group) = self.groups.get(&key).copied() {
            self.memo
                .group_mut(group)
                .expect("csg-cmp memo group should exist")
                .push(expression);
        } else {
            let required_groups = self.memo.group_count().saturating_add(1);
            if required_groups > config.max_groups {
                return Err(RelationalJoinEnumerationError::GroupBudgetExceeded {
                    required_groups,
                    max_groups: config.max_groups,
                }
                .into());
            }
            let group = self.memo.insert_group(expression);
            self.groups.insert(key.clone(), group);
            self.keys.insert(group, key);
        }
        if let Some(alternative) = alternative {
            self.alternatives.push(alternative);
        }
        self.expression_count = required_expressions;
        Ok(())
    }
}

pub fn enumerate_relational_csg_cmp_joins(
    problem: &RelationalJoinRewriteProblem,
    required_properties: &RequiredProperties,
    config: RelationalJoinEnumerationConfig,
) -> Result<RelationalCsgCmpEnumeration, RelationalJoinRewriteError> {
    enumerate_relational_csg_cmp_joins_with_right_input_policy(
        problem,
        required_properties,
        config,
        RelationalCsgCmpRightInputPolicy::default(),
    )
}

pub fn enumerate_relational_csg_cmp_joins_with_right_input_policy(
    problem: &RelationalJoinRewriteProblem,
    required_properties: &RequiredProperties,
    config: RelationalJoinEnumerationConfig,
    right_input_policy: RelationalCsgCmpRightInputPolicy,
) -> Result<RelationalCsgCmpEnumeration, RelationalJoinRewriteError> {
    enumerate_relational_csg_cmp_joins_with_implementations(
        problem,
        required_properties,
        config,
        right_input_policy,
        &[],
    )
}

pub fn enumerate_relational_csg_cmp_joins_with_implementations(
    problem: &RelationalJoinRewriteProblem,
    required_properties: &RequiredProperties,
    config: RelationalJoinEnumerationConfig,
    right_input_policy: RelationalCsgCmpRightInputPolicy,
    implementations: &[RelationalCsgCmpJoinImplementation],
) -> Result<RelationalCsgCmpEnumeration, RelationalJoinRewriteError> {
    let analysis = analyze_relational_join_conflicts(
        &problem.initial_tree,
        problem.post_join_filter.as_ref(),
    )?;
    validate_problem_relations(problem, &analysis)?;
    let logical_expression_budget = config
        .max_expressions
        .checked_sub(implementations.len())
        .ok_or(RelationalJoinEnumerationError::ExpressionBudgetExceeded {
            required_expressions: implementations.len(),
            max_expressions: config.max_expressions,
        })?;
    let memo = build_csg_cmp_memo(
        problem,
        &analysis,
        RelationalJoinEnumerationConfig {
            max_expressions: logical_expression_budget,
            ..config
        },
    )
    .map_err(|error| match error {
        RelationalJoinRewriteError::Enumeration(
            RelationalJoinEnumerationError::ExpressionBudgetExceeded {
                required_expressions,
                ..
            },
        ) => RelationalJoinEnumerationError::ExpressionBudgetExceeded {
            required_expressions: required_expressions.saturating_add(implementations.len()),
            max_expressions: config.max_expressions,
        }
        .into(),
        other => other,
    })?;
    let root_key = SemanticKey {
        bindings: analysis.root_bindings.clone(),
        applied_operators: analysis.descriptors.keys().copied().collect(),
    };
    let root = memo
        .groups
        .get(&root_key)
        .copied()
        .ok_or(RelationalJoinRewriteError::NoLegalRewrite)?;
    let root_alternatives = memo
        .memo
        .group(root)
        .expect("complete csg-cmp memo group should exist")
        .expressions()
        .len();
    let mut cache = HashMap::new();
    let plan = best_csg_cmp_plan(
        problem,
        &memo,
        root,
        required_properties,
        right_input_policy,
        implementations,
        &mut cache,
    )
    .ok_or(RelationalJoinEnumerationError::RequiredPropertiesUnsatisfied)?;
    Ok(RelationalCsgCmpEnumeration {
        plan,
        conflict_analysis: analysis,
        alternatives: memo.alternatives,
        memo_groups: memo.memo.group_count(),
        memo_expressions: memo.expression_count.saturating_add(implementations.len()),
        root_alternatives,
    })
}

fn build_csg_cmp_memo(
    problem: &RelationalJoinRewriteProblem,
    analysis: &RelationalJoinConflictAnalysis,
    config: RelationalJoinEnumerationConfig,
) -> Result<CsgCmpMemo, RelationalJoinRewriteError> {
    let mut memo = CsgCmpMemo::new();
    let mut bindings = problem
        .relations
        .iter()
        .map(|relation| relation.binding)
        .collect::<Vec<_>>();
    bindings.sort_unstable();
    for binding in bindings {
        memo.add(
            SemanticKey {
                bindings: binding.into(),
                applied_operators: BTreeSet::new(),
            },
            CsgCmpExpression::Relation(binding),
            None,
            config,
        )?;
    }

    for target_size in 2..=problem.relations.len() {
        let groups = memo
            .keys
            .iter()
            .filter(|(_, key)| key.bindings.len() < target_size)
            .map(|(group, key)| (*group, key.clone()))
            .collect::<Vec<_>>();
        for left_index in 0..groups.len() {
            for right_index in left_index + 1..groups.len() {
                let (left_group, left_key) = &groups[left_index];
                let (right_group, right_key) = &groups[right_index];
                if left_key.bindings.len() + right_key.bindings.len() != target_size
                    || left_key.bindings.intersects(&right_key.bindings)
                    || !left_key
                        .applied_operators
                        .is_disjoint(&right_key.applied_operators)
                {
                    continue;
                }
                let applied = left_key
                    .applied_operators
                    .union(&right_key.applied_operators)
                    .copied()
                    .collect::<BTreeSet<_>>();
                for descriptor in analysis
                    .descriptors
                    .values()
                    .filter(|descriptor| !applied.contains(&descriptor.operator_id))
                {
                    if descriptor.is_applicable(&left_key.bindings, &right_key.bindings) {
                        add_csg_cmp_join(
                            &mut memo,
                            *left_group,
                            left_key,
                            *right_group,
                            right_key,
                            descriptor,
                            &applied,
                            config,
                        )?;
                        if descriptor.effective_kind.is_commutative() {
                            add_csg_cmp_join(
                                &mut memo,
                                *right_group,
                                right_key,
                                *left_group,
                                left_key,
                                descriptor,
                                &applied,
                                config,
                            )?;
                        }
                    } else if descriptor.is_applicable(&right_key.bindings, &left_key.bindings) {
                        add_csg_cmp_join(
                            &mut memo,
                            *right_group,
                            right_key,
                            *left_group,
                            left_key,
                            descriptor,
                            &applied,
                            config,
                        )?;
                        if descriptor.effective_kind.is_commutative() {
                            add_csg_cmp_join(
                                &mut memo,
                                *left_group,
                                left_key,
                                *right_group,
                                right_key,
                                descriptor,
                                &applied,
                                config,
                            )?;
                        }
                    }
                }
            }
        }
    }
    Ok(memo)
}

#[allow(clippy::too_many_arguments)]
fn add_csg_cmp_join(
    memo: &mut CsgCmpMemo,
    left_group: GroupId,
    left_key: &SemanticKey,
    right_group: GroupId,
    right_key: &SemanticKey,
    descriptor: &crate::RelationalJoinConflictDescriptor,
    applied: &BTreeSet<RelationalJoinOperatorId>,
    config: RelationalJoinEnumerationConfig,
) -> Result<(), RelationalJoinRewriteError> {
    let mut applied_operators = applied.clone();
    applied_operators.insert(descriptor.operator_id);
    let left_bindings = left_key.bindings.clone();
    let right_bindings = right_key.bindings.clone();
    memo.add(
        SemanticKey {
            bindings: binding_union(&left_bindings, &right_bindings),
            applied_operators,
        },
        CsgCmpExpression::Join {
            left: left_group,
            right: right_group,
            operator_id: descriptor.operator_id,
            operator_kind: descriptor.effective_kind,
            predicate_ids: descriptor.predicate_ids.clone(),
        },
        Some(RelationalCsgCmpAlternative {
            left_bindings,
            right_bindings,
            operator_id: descriptor.operator_id,
            operator_kind: descriptor.effective_kind,
        }),
        config,
    )
}

#[allow(clippy::too_many_arguments)]
fn best_csg_cmp_plan(
    problem: &RelationalJoinRewriteProblem,
    memo: &CsgCmpMemo,
    group: GroupId,
    required_properties: &RequiredProperties,
    right_input_policy: RelationalCsgCmpRightInputPolicy,
    implementations: &[RelationalCsgCmpJoinImplementation],
    cache: &mut HashMap<(GroupId, RequiredProperties), Option<RelationalCsgCmpPlan>>,
) -> Option<RelationalCsgCmpPlan> {
    let key = (group, required_properties.clone());
    if let Some(plan) = cache.get(&key) {
        return plan.clone();
    }
    let mut selected = None;
    for expression in memo
        .memo
        .group(group)
        .expect("csg-cmp memo group should exist")
        .expressions()
    {
        let candidate = match expression {
            CsgCmpExpression::Relation(binding) => problem
                .relations
                .iter()
                .find(|relation| relation.binding == *binding)
                .and_then(|relation| {
                    relation
                        .access_paths
                        .iter()
                        .filter(|access| access.supports_base())
                        .filter(|access| access.properties.satisfies(required_properties))
                        .map(|access| RelationalCsgCmpPlan {
                            root: RelationalCsgCmpPlanNode::Relation {
                                binding: *binding,
                                access_path: access.clone(),
                            },
                            cost_breakdown: estimate_relational_access_cost(
                                access.descriptor.estimated_rows,
                            ),
                            properties: access.properties.clone(),
                        })
                        .min_by(compare_csg_cmp_plans)
                }),
            CsgCmpExpression::Join {
                left,
                right,
                operator_id,
                operator_kind,
                predicate_ids,
            } => {
                let left_group = *left;
                let right_group = *right;
                let left = best_csg_cmp_plan(
                    problem,
                    memo,
                    left_group,
                    required_properties,
                    right_input_policy,
                    implementations,
                    cache,
                );
                let right_key = memo
                    .keys
                    .get(&right_group)
                    .expect("csg-cmp memo tracks every right group");
                let probe = if right_key.bindings.len() == 1 {
                    let binding = right_key
                        .bindings
                        .iter()
                        .next()
                        .expect("singleton group has one binding");
                    Some(best_probe_relation_plan(
                        problem,
                        binding,
                        &memo
                            .keys
                            .get(&left_group)
                            .expect("csg-cmp memo tracks every left group")
                            .bindings,
                    ))
                } else {
                    None
                };
                let (right, materialized_right) = match probe.flatten() {
                    Some(plan) => (Some(plan), false),
                    None if matches!(
                        right_input_policy,
                        RelationalCsgCmpRightInputPolicy::AllowMaterialized
                    ) =>
                    {
                        (
                            best_csg_cmp_plan(
                                problem,
                                memo,
                                right_group,
                                &RequiredProperties::default(),
                                right_input_policy,
                                implementations,
                                cache,
                            ),
                            true,
                        )
                    }
                    None => (None, true),
                };
                let structural = left.zip(right).map(|(left, right)| {
                    let cardinality = match operator_kind {
                        RelationalJoinOperatorKind::Inner => RelationalJoinCardinality::Inner,
                        RelationalJoinOperatorKind::LeftOuter => {
                            RelationalJoinCardinality::PreserveLeft
                        }
                    };
                    let right_input = if materialized_right {
                        RelationalJoinRightInput::Materialized
                    } else {
                        RelationalJoinRightInput::Probe
                    };
                    RelationalCsgCmpPlan {
                        properties: left.properties.clone(),
                        cost_breakdown: estimate_relational_join_cost(
                            left.cost_breakdown,
                            right.cost_breakdown,
                            cardinality,
                            right_input,
                            RelationalJoinSelectivity::Unknown,
                        ),
                        root: RelationalCsgCmpPlanNode::Join {
                            operator_id: *operator_id,
                            operator_kind: *operator_kind,
                            predicate_ids: predicate_ids.clone(),
                            implementation: None,
                            left: Box::new(left.root),
                            right: Box::new(right.root),
                        },
                    }
                });
                let left_bindings = &memo.keys.get(&left_group)?.bindings;
                structural
                    .into_iter()
                    .chain(
                        implementations
                            .iter()
                            .filter(|implementation| {
                                implementation.operator_id == *operator_id
                                    && implementation.operator_kind == *operator_kind
                                    && implementation.predicate_ids == *predicate_ids
                                    && *left_bindings
                                        == BindingSet::from(implementation.left_binding)
                                    && right_key.bindings
                                        == BindingSet::from(implementation.right_binding)
                                    && implementation.left_access.supports_base()
                                    && implementation.right_access.supports_base()
                            })
                            .filter_map(|implementation| {
                                let properties = match implementation.algorithm {
                                    // Grace partitioning does not preserve input ordering.
                                    RelationalEquiJoinAlgorithm::Hash => {
                                        PhysicalProperties::default()
                                    }
                                    RelationalEquiJoinAlgorithm::Merge => {
                                        implementation.left_access.properties.clone()
                                    }
                                };
                                properties.satisfies(required_properties).then(|| {
                                    RelationalCsgCmpPlan {
                                        properties,
                                        cost_breakdown: estimate_relational_join_cost(
                                            estimate_relational_access_cost(
                                                implementation
                                                    .left_access
                                                    .descriptor
                                                    .estimated_rows,
                                            ),
                                            estimate_relational_access_cost(
                                                implementation
                                                    .right_access
                                                    .descriptor
                                                    .estimated_rows,
                                            ),
                                            match operator_kind {
                                                RelationalJoinOperatorKind::Inner => {
                                                    RelationalJoinCardinality::Inner
                                                }
                                                RelationalJoinOperatorKind::LeftOuter => {
                                                    RelationalJoinCardinality::PreserveLeft
                                                }
                                            },
                                            implementation.algorithm.right_input(),
                                            implementation.selectivity,
                                        ),
                                        root: RelationalCsgCmpPlanNode::Join {
                                            operator_id: *operator_id,
                                            operator_kind: *operator_kind,
                                            predicate_ids: predicate_ids.clone(),
                                            implementation: Some(Box::new(implementation.clone())),
                                            left: Box::new(RelationalCsgCmpPlanNode::Relation {
                                                binding: implementation.left_binding,
                                                access_path: implementation.left_access.clone(),
                                            }),
                                            right: Box::new(RelationalCsgCmpPlanNode::Relation {
                                                binding: implementation.right_binding,
                                                access_path: implementation.right_access.clone(),
                                            }),
                                        },
                                    }
                                })
                            }),
                    )
                    .min_by(compare_csg_cmp_plans)
            }
        };
        if let Some(candidate) = candidate
            && selected
                .as_ref()
                .is_none_or(|current| compare_csg_cmp_plans(&candidate, current).is_lt())
        {
            selected = Some(candidate);
        }
    }
    cache.insert(key, selected.clone());
    selected
}

fn best_probe_relation_plan(
    problem: &RelationalJoinRewriteProblem,
    binding: BindingId,
    outer_bindings: &BindingSet,
) -> Option<RelationalCsgCmpPlan> {
    problem
        .relations
        .iter()
        .find(|relation| relation.binding == binding)?
        .access_paths
        .iter()
        .filter(|access| {
            access.supports_probe() && access.required_bindings.is_subset(outer_bindings)
        })
        .map(|access| RelationalCsgCmpPlan {
            root: RelationalCsgCmpPlanNode::Relation {
                binding,
                access_path: access.clone(),
            },
            cost_breakdown: estimate_relational_access_cost(access.descriptor.estimated_rows),
            properties: access.properties.clone(),
        })
        .min_by(compare_probe_plans)
}

fn compare_probe_plans(
    left: &RelationalCsgCmpPlan,
    right: &RelationalCsgCmpPlan,
) -> std::cmp::Ordering {
    left.cost_breakdown
        .estimated_rows
        .cmp(&right.cost_breakdown.estimated_rows)
        .then_with(|| match (&left.root, &right.root) {
            (
                RelationalCsgCmpPlanNode::Relation {
                    access_path: left, ..
                },
                RelationalCsgCmpPlanNode::Relation {
                    access_path: right, ..
                },
            ) => right
                .descriptor
                .unique_point
                .cmp(&left.descriptor.unique_point)
                .then_with(|| {
                    right
                        .descriptor
                        .equality_prefix_len
                        .cmp(&left.descriptor.equality_prefix_len)
                })
                .then_with(|| left.descriptor.name.cmp(&right.descriptor.name)),
            _ => std::cmp::Ordering::Equal,
        })
}

fn compare_csg_cmp_plans(
    left: &RelationalCsgCmpPlan,
    right: &RelationalCsgCmpPlan,
) -> std::cmp::Ordering {
    left.cost_breakdown
        .cost
        .cmp(&right.cost_breakdown.cost)
        .then_with(|| {
            left.cost_breakdown
                .estimated_rows
                .cmp(&right.cost_breakdown.estimated_rows)
        })
        .then_with(|| left.root.stable_key().cmp(&right.root.stable_key()))
}

fn binding_union(left: &BindingSet, right: &BindingSet) -> BindingSet {
    left.iter().chain(right.iter()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    mod implementations;
    use crate::{
        RelationalAccessPathDescriptor, RelationalAccessPathKind, RelationalJoinOperator,
        RelationalJoinRelation, RelationalJoinTree,
    };
    use skein_expression::{BoundPredicate, BoundScalarExpression, ScalarNullability};
    use std::collections::BTreeSet;

    const A: BindingId = BindingId::new(1);
    const B: BindingId = BindingId::new(2);
    const C: BindingId = BindingId::new(3);
    const D: BindingId = BindingId::new(4);

    fn value(binding: BindingId) -> BoundScalarExpression {
        BoundScalarExpression::BindingValue {
            binding,
            nullability: ScalarNullability::MaybeNull,
        }
    }

    fn predicate(bindings: &[BindingId]) -> BoundPredicate {
        BoundPredicate::And(
            bindings
                .windows(2)
                .map(|pair| BoundPredicate::Comparison {
                    left: value(pair[0]),
                    right: value(pair[1]),
                })
                .collect(),
        )
    }

    fn operator(
        id: u32,
        kind: RelationalJoinOperatorKind,
        bindings: &[BindingId],
    ) -> RelationalJoinOperator {
        RelationalJoinOperator {
            id: RelationalJoinOperatorId::new(id),
            kind,
            predicate_ids: vec![RelationalJoinPredicateId::new(id)],
            predicate: predicate(bindings),
        }
    }

    fn relation(binding: BindingId, rows: usize) -> RelationalJoinRelation {
        RelationalJoinRelation {
            binding,
            access_paths: vec![RelationalJoinAccessPath::base_and_probe(
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
                    estimated_rows: rows,
                },
            )],
        }
    }

    fn indexed_relation(
        binding: BindingId,
        rows: usize,
        required: BindingId,
    ) -> RelationalJoinRelation {
        let mut relation = relation(binding, rows);
        relation.access_paths.push(RelationalJoinAccessPath::probe(
            RelationalAccessPathDescriptor {
                kind: RelationalAccessPathKind::Index,
                name: "by_outer".to_string(),
                index_columns: vec!["id".to_string()],
                access_columns: BTreeSet::from(["id".to_string()]),
                equality_prefix_len: 1,
                order_prefix_len: 0,
                exclusive_range: false,
                reverse_order: false,
                unique_point: true,
                covering: false,
                requires_row_fetch: true,
                estimated_rows: 1,
            },
            required.into(),
        ));
        relation
    }

    fn problem(
        relations: &[BindingId],
        tree: RelationalJoinTree,
        filter: Option<BoundPredicate>,
    ) -> RelationalJoinRewriteProblem {
        RelationalJoinRewriteProblem {
            relations: relations
                .iter()
                .enumerate()
                .map(|(index, binding)| relation(*binding, (index + 1) * 10))
                .collect(),
            initial_tree: tree,
            post_join_filter: filter,
        }
    }

    #[test]
    fn csg_cmp_enumerates_bushy_hypergraph_partitions_into_one_semantic_group() {
        let left = RelationalJoinTree::join(
            operator(1, RelationalJoinOperatorKind::Inner, &[A, B]),
            RelationalJoinTree::Relation(A),
            RelationalJoinTree::Relation(B),
        );
        let right = RelationalJoinTree::join(
            operator(2, RelationalJoinOperatorKind::Inner, &[C, D]),
            RelationalJoinTree::Relation(C),
            RelationalJoinTree::Relation(D),
        );
        let tree = RelationalJoinTree::join(
            operator(3, RelationalJoinOperatorKind::Inner, &[B, C]),
            left,
            right,
        );
        let result = enumerate_relational_csg_cmp_joins(
            &problem(&[A, B, C, D], tree, None),
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        assert_eq!(result.plan.root.bindings(), BindingSet::from([A, B, C, D]));
        assert!(result.root_alternatives > 1);
        assert!(result.alternatives.iter().any(|alternative| {
            alternative.operator_id == RelationalJoinOperatorId::new(3)
                && alternative.left_bindings == BindingSet::from([A, B])
                && alternative.right_bindings == BindingSet::from([C, D])
        }));
    }

    #[test]
    fn csg_cmp_probe_only_policy_excludes_unbounded_materialization() {
        let left = RelationalJoinTree::join(
            operator(1, RelationalJoinOperatorKind::Inner, &[A, B]),
            RelationalJoinTree::Relation(A),
            RelationalJoinTree::Relation(B),
        );
        let right = RelationalJoinTree::join(
            operator(2, RelationalJoinOperatorKind::Inner, &[C, D]),
            RelationalJoinTree::Relation(C),
            RelationalJoinTree::Relation(D),
        );
        let tree = RelationalJoinTree::join(
            operator(3, RelationalJoinOperatorKind::Inner, &[B, C]),
            left,
            right,
        );

        let result = enumerate_relational_csg_cmp_joins_with_right_input_policy(
            &problem(&[A, B, C, D], tree, None),
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
            RelationalCsgCmpRightInputPolicy::ProbeOnly,
        )
        .expect("probe-only CSG-CMP can choose a left-deep alternative");

        assert!(!result.plan.root.has_materialized_right());
    }

    #[test]
    fn csg_cmp_costing_uses_a_singleton_index_probe_from_the_outer_group() {
        let tree = RelationalJoinTree::join(
            operator(1, RelationalJoinOperatorKind::Inner, &[A, B]),
            RelationalJoinTree::Relation(A),
            RelationalJoinTree::Relation(B),
        );
        let problem = RelationalJoinRewriteProblem {
            relations: vec![relation(A, 1), indexed_relation(B, 1_000, A)],
            initial_tree: tree,
            post_join_filter: None,
        };
        let result = enumerate_relational_csg_cmp_joins(
            &problem,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        assert_eq!(
            result.plan.cost(),
            PlanCost {
                estimated_rows: 1,
                cost: 2,
            }
        );
        let RelationalCsgCmpPlanNode::Join { left, right, .. } = result.plan.root else {
            panic!("expected costed join root");
        };
        assert_eq!(left.bindings(), A.into());
        let RelationalCsgCmpPlanNode::Relation { access_path, .. } = *right else {
            panic!("expected singleton probe on the right");
        };
        assert_eq!(access_path.descriptor.name, "by_outer");
    }

    #[test]
    fn csg_cmp_probe_tie_prefers_the_longer_equality_prefix() {
        let tree = RelationalJoinTree::join(
            operator(1, RelationalJoinOperatorKind::Inner, &[A, B]),
            RelationalJoinTree::Relation(A),
            RelationalJoinTree::Relation(B),
        );
        let mut inner = relation(B, 10);
        inner.access_paths.push(RelationalJoinAccessPath::probe(
            RelationalAccessPathDescriptor {
                kind: RelationalAccessPathKind::Index,
                name: "by_outer".to_string(),
                index_columns: vec!["outer_id".to_string()],
                access_columns: BTreeSet::from(["outer_id".to_string()]),
                equality_prefix_len: 1,
                order_prefix_len: 0,
                exclusive_range: false,
                reverse_order: false,
                unique_point: false,
                covering: false,
                requires_row_fetch: true,
                estimated_rows: 10,
            },
            A.into(),
        ));
        let problem = RelationalJoinRewriteProblem {
            relations: vec![relation(A, 1), inner],
            initial_tree: tree,
            post_join_filter: None,
        };

        let result = enumerate_relational_csg_cmp_joins(
            &problem,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        let RelationalCsgCmpPlanNode::Join { right, .. } = result.plan.root else {
            panic!("expected a join root");
        };
        let RelationalCsgCmpPlanNode::Relation { access_path, .. } = *right else {
            panic!("expected a singleton probe");
        };
        assert_eq!(access_path.descriptor.name, "by_outer");
    }

    #[test]
    fn multi_relation_hyperedge_waits_for_its_complete_endpoint() {
        let lower = RelationalJoinTree::join(
            operator(1, RelationalJoinOperatorKind::Inner, &[A, B]),
            RelationalJoinTree::Relation(A),
            RelationalJoinTree::Relation(B),
        );
        let tree = RelationalJoinTree::join(
            operator(2, RelationalJoinOperatorKind::Inner, &[A, B, C]),
            lower,
            RelationalJoinTree::Relation(C),
        );
        let result = enumerate_relational_csg_cmp_joins(
            &problem(&[A, B, C], tree, None),
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        assert!(result
            .alternatives
            .iter()
            .filter(|alternative| alternative.operator_id == RelationalJoinOperatorId::new(2))
            .all(|alternative| {
                alternative.left_bindings == BindingSet::from([A, B])
                    || alternative.right_bindings == BindingSet::from([A, B])
            }));
    }

    #[test]
    fn csg_cmp_never_inserts_the_illegal_outer_join_association() {
        let lower = RelationalJoinTree::join(
            operator(1, RelationalJoinOperatorKind::LeftOuter, &[A, B]),
            RelationalJoinTree::Relation(A),
            RelationalJoinTree::Relation(B),
        );
        let tree = RelationalJoinTree::join(
            operator(2, RelationalJoinOperatorKind::Inner, &[B, C]),
            lower,
            RelationalJoinTree::Relation(C),
        );
        let result = enumerate_relational_csg_cmp_joins(
            &problem(&[A, B, C], tree, None),
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        assert!(!result.alternatives.iter().any(|alternative| {
            alternative.operator_id == RelationalJoinOperatorId::new(2)
                && binding_union(&alternative.left_bindings, &alternative.right_bindings)
                    == BindingSet::from([B, C])
        }));
    }

    #[test]
    fn null_rejection_opens_the_inner_hyperedge_only_after_outer_simplification() {
        let lower = RelationalJoinTree::join(
            operator(1, RelationalJoinOperatorKind::LeftOuter, &[A, B]),
            RelationalJoinTree::Relation(A),
            RelationalJoinTree::Relation(B),
        );
        let tree = RelationalJoinTree::join(
            operator(2, RelationalJoinOperatorKind::Inner, &[B, C]),
            lower,
            RelationalJoinTree::Relation(C),
        );
        let filter = BoundPredicate::IsNotNull(value(B));
        let result = enumerate_relational_csg_cmp_joins(
            &problem(&[A, B, C], tree, Some(filter)),
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
        )
        .unwrap();

        assert!(result.alternatives.iter().any(|alternative| {
            alternative.operator_id == RelationalJoinOperatorId::new(2)
                && binding_union(&alternative.left_bindings, &alternative.right_bindings)
                    == BindingSet::from([B, C])
        }));
    }
}
