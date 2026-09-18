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

//! Shared semantic memo, admission and dynamic programming for relational joins.

use super::*;
use crate::{
    relational_join_cost::{
        estimate_relational_access_path_cost, estimate_relational_join_cost,
        RelationalJoinCardinality,
    },
    GroupId, Memo, RelationalInnerJoinEnumeration, RelationalJoinEnumerationError,
    RelationalJoinGraph, RelationalJoinPlan, RelationalJoinPredicate, RelationalJoinRelation,
    RelationalJoinRewriteEnumeration, RelationalJoinRewritePlan, RelationalJoinRewriteStep,
    RelationalJoinStep,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write;

#[derive(Debug, Clone, Copy)]
enum Connectivity<'a> {
    Predicates(&'a [RelationalJoinPredicate]),
    Operators(&'a RelationalJoinConflictAnalysis),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnumerationDomain {
    // The domain fixes both legal partitions and the frontend's stable tie key.
    Inner,
    Rewrite,
    CsgCmp,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SemanticKey {
    bindings: BindingSet,
    applied_operators: BTreeSet<RelationalJoinOperatorId>,
}

impl SemanticKey {
    fn relation(binding: BindingId) -> Self {
        Self {
            bindings: binding.into(),
            applied_operators: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JoinExpression {
    Relation(BindingId),
    Join {
        left: GroupId,
        right: GroupId,
        // Raw predicate graphs have no syntactic operator identity.
        operator_id: Option<RelationalJoinOperatorId>,
        operator_kind: RelationalJoinOperatorKind,
        predicate_ids: Vec<RelationalJoinPredicateId>,
    },
}

struct JoinMemo {
    memo: Memo<JoinExpression>,
    groups: BTreeMap<SemanticKey, GroupId>,
    keys: BTreeMap<GroupId, SemanticKey>,
    alternatives: Vec<RelationalCsgCmpAlternative>,
    expression_count: usize,
}

impl JoinMemo {
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
        expression: JoinExpression,
        config: RelationalJoinEnumerationConfig,
    ) -> Result<(), RelationalJoinEnumerationError> {
        let required_expressions = self.expression_count.saturating_add(1);
        if required_expressions > config.max_expressions {
            return Err(RelationalJoinEnumerationError::ExpressionBudgetExceeded {
                required_expressions,
                max_expressions: config.max_expressions,
            });
        }
        if let Some(group) = self.groups.get(&key).copied() {
            self.memo
                .group_mut(group)
                .expect("join memo group exists")
                .push(expression);
        } else {
            let required_groups = self.memo.group_count().saturating_add(1);
            if required_groups > config.max_groups {
                return Err(RelationalJoinEnumerationError::GroupBudgetExceeded {
                    required_groups,
                    max_groups: config.max_groups,
                });
            }
            let group = self.memo.insert_group(expression);
            self.groups.insert(key.clone(), group);
            self.keys.insert(group, key);
        }
        self.expression_count = required_expressions;
        Ok(())
    }

    fn add_join(
        &mut self,
        left: GroupId,
        right: GroupId,
        operator: Option<&crate::RelationalJoinConflictDescriptor>,
        predicate_ids: Vec<RelationalJoinPredicateId>,
        collect_alternatives: bool,
        config: RelationalJoinEnumerationConfig,
    ) -> Result<(), RelationalJoinEnumerationError> {
        let left_key = &self.keys[&left];
        let right_key = &self.keys[&right];
        let mut applied_operators = left_key
            .applied_operators
            .union(&right_key.applied_operators)
            .copied()
            .collect::<BTreeSet<_>>();
        let operator_id = operator.map(|descriptor| descriptor.operator_id);
        let operator_kind = operator.map_or(RelationalJoinOperatorKind::Inner, |descriptor| {
            descriptor.effective_kind
        });
        applied_operators.extend(operator_id);
        let alternative = operator.filter(|_| collect_alternatives).map(|descriptor| {
            RelationalCsgCmpAlternative {
                left_bindings: left_key.bindings.clone(),
                right_bindings: right_key.bindings.clone(),
                operator_id: descriptor.operator_id,
                operator_kind,
            }
        });
        self.add(
            SemanticKey {
                bindings: binding_union(&left_key.bindings, &right_key.bindings),
                applied_operators,
            },
            JoinExpression::Join {
                left,
                right,
                operator_id,
                operator_kind,
                predicate_ids,
            },
            config,
        )?;
        // Admission failure must not publish any alternative or partial group.
        self.alternatives.extend(alternative);
        Ok(())
    }
}

fn build_join_memo(
    relations: &[RelationalJoinRelation],
    connectivity: Connectivity<'_>,
    domain: EnumerationDomain,
    config: RelationalJoinEnumerationConfig,
) -> Result<JoinMemo, RelationalJoinEnumerationError> {
    let mut memo = JoinMemo::new();
    let mut bindings = relations
        .iter()
        .map(|relation| relation.binding)
        .collect::<Vec<_>>();
    bindings.sort_unstable();
    let mut singleton_groups = BTreeMap::new();
    for binding in bindings {
        let key = SemanticKey::relation(binding);
        memo.add(key.clone(), JoinExpression::Relation(binding), config)?;
        singleton_groups.insert(binding, memo.groups[&key]);
    }
    for target_size in 2..=relations.len() {
        let groups = memo
            .keys
            .iter()
            .filter(|(_, key)| match domain {
                EnumerationDomain::CsgCmp => key.bindings.len() < target_size,
                EnumerationDomain::Inner => key.bindings.len() == target_size - 1,
                EnumerationDomain::Rewrite => {
                    key.bindings.len() == 1 || key.bindings.len() == target_size - 1
                }
            })
            .map(|(group, key)| (*group, key.clone()))
            .collect::<Vec<_>>();
        for (left_index, (left, left_key)) in groups.iter().enumerate() {
            if domain != EnumerationDomain::CsgCmp && left_key.bindings.len() != target_size - 1 {
                continue;
            }
            match connectivity {
                Connectivity::Predicates(predicates) => {
                    // Only an edge missing exactly one binding can extend a
                    // connected prefix without introducing a Cartesian step.
                    let mut complements: BTreeMap<BindingId, Vec<RelationalJoinPredicateId>> =
                        BTreeMap::new();
                    for predicate in predicates {
                        let mut missing = predicate
                            .bindings
                            .iter()
                            .filter(|binding| !left_key.bindings.contains(*binding));
                        if let Some(right) = missing.next()
                            && missing.next().is_none()
                        {
                            complements.entry(right).or_default().push(predicate.id);
                        }
                    }
                    for (right, predicate_ids) in complements {
                        memo.add_join(
                            *left,
                            singleton_groups[&right],
                            None,
                            predicate_ids,
                            false,
                            config,
                        )?;
                    }
                }
                Connectivity::Operators(analysis) => {
                    let right_groups = if domain == EnumerationDomain::CsgCmp {
                        &groups[left_index + 1..]
                    } else {
                        &groups[..singleton_groups.len()]
                    };
                    for (right, right_key) in right_groups {
                        if left_key.bindings.len() + right_key.bindings.len() != target_size
                            || left_key.bindings.intersects(&right_key.bindings)
                            || !left_key
                                .applied_operators
                                .is_disjoint(&right_key.applied_operators)
                        {
                            continue;
                        }
                        for descriptor in analysis.descriptors.values().filter(|descriptor| {
                            !left_key.applied_operators.contains(&descriptor.operator_id)
                                && !right_key
                                    .applied_operators
                                    .contains(&descriptor.operator_id)
                        }) {
                            let forward =
                                descriptor.is_applicable(&left_key.bindings, &right_key.bindings);
                            let reverse =
                                descriptor.is_applicable(&right_key.bindings, &left_key.bindings);
                            let commutative = descriptor.effective_kind.is_commutative();
                            if domain != EnumerationDomain::CsgCmp {
                                if forward || (commutative && reverse) {
                                    memo.add_join(
                                        *left,
                                        *right,
                                        Some(descriptor),
                                        descriptor.predicate_ids.clone(),
                                        false,
                                        config,
                                    )?;
                                }
                            } else if forward || reverse {
                                let (first, second) = if forward {
                                    (*left, *right)
                                } else {
                                    (*right, *left)
                                };
                                memo.add_join(
                                    first,
                                    second,
                                    Some(descriptor),
                                    descriptor.predicate_ids.clone(),
                                    true,
                                    config,
                                )?;
                                if commutative {
                                    memo.add_join(
                                        second,
                                        first,
                                        Some(descriptor),
                                        descriptor.predicate_ids.clone(),
                                        true,
                                        config,
                                    )?;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(memo)
}

#[derive(Debug, Clone)]
enum SelectedNode {
    Relation {
        binding: BindingId,
        access_path: RelationalJoinAccessPath,
    },
    Join {
        operator_id: Option<RelationalJoinOperatorId>,
        operator_kind: RelationalJoinOperatorKind,
        predicate_ids: Vec<RelationalJoinPredicateId>,
        implementation: Option<Box<RelationalCsgCmpJoinImplementation>>,
        left: Box<Self>,
        right: Box<Self>,
    },
}

impl SelectedNode {
    fn stable_key(&self, domain: EnumerationDomain) -> String {
        let mut key = String::new();
        self.write_stable_key(&mut key, domain)
            .expect("writing to a string cannot fail");
        key
    }

    fn write_stable_key(&self, key: &mut String, domain: EnumerationDomain) -> std::fmt::Result {
        match self {
            Self::Relation {
                binding,
                access_path,
            } => write!(key, "{}:{}", binding.get(), access_path.descriptor.name),
            Self::Join {
                operator_id,
                implementation,
                left,
                right,
                ..
            } => {
                // One buffer keeps flat-plan key construction linear after
                // adopting the shared tree representation.
                if domain == EnumerationDomain::CsgCmp {
                    key.push('(');
                }
                left.write_stable_key(key, domain)?;
                match domain {
                    EnumerationDomain::Inner => key.push('/'),
                    EnumerationDomain::Rewrite => write!(
                        key,
                        "/{}:",
                        operator_id.expect("rewrite operator has an identity").get()
                    )?,
                    EnumerationDomain::CsgCmp => write!(
                        key,
                        ")/{}:{:?}:(",
                        operator_id.expect("csg-cmp operator has an identity").get(),
                        implementation
                            .as_ref()
                            .map(|implementation| implementation.algorithm)
                    )?,
                }
                right.write_stable_key(key, domain)?;
                if domain == EnumerationDomain::CsgCmp {
                    key.push(')');
                }
                Ok(())
            }
        }
    }

    fn into_csg_cmp(self) -> RelationalCsgCmpPlanNode {
        match self {
            Self::Relation {
                binding,
                access_path,
            } => RelationalCsgCmpPlanNode::Relation {
                binding,
                access_path,
            },
            Self::Join {
                operator_id,
                operator_kind,
                predicate_ids,
                implementation,
                left,
                right,
            } => RelationalCsgCmpPlanNode::Join {
                operator_id: operator_id.expect("csg-cmp operator has an identity"),
                operator_kind,
                predicate_ids,
                implementation,
                left: Box::new(left.into_csg_cmp()),
                right: Box::new(right.into_csg_cmp()),
            },
        }
    }

    fn into_left_deep(
        self,
        steps: &mut Vec<SelectedStep>,
    ) -> (BindingId, RelationalJoinAccessPath) {
        match self {
            Self::Relation {
                binding,
                access_path,
            } => (binding, access_path),
            Self::Join {
                operator_id,
                operator_kind,
                predicate_ids,
                implementation,
                left,
                right,
            } => {
                assert!(
                    implementation.is_none(),
                    "left-deep adapters select probe joins"
                );
                let base = left.into_left_deep(steps);
                let Self::Relation {
                    binding,
                    access_path,
                } = *right
                else {
                    unreachable!("left-deep adapters have singleton right groups");
                };
                steps.push(SelectedStep {
                    binding,
                    access_path,
                    operator_id,
                    operator_kind,
                    predicate_ids,
                });
                base
            }
        }
    }
}

struct SelectedStep {
    binding: BindingId,
    access_path: RelationalJoinAccessPath,
    operator_id: Option<RelationalJoinOperatorId>,
    operator_kind: RelationalJoinOperatorKind,
    predicate_ids: Vec<RelationalJoinPredicateId>,
}

#[derive(Debug, Clone)]
struct SelectedPlan {
    root: SelectedNode,
    cost_breakdown: PlanCostBreakdown,
    properties: PhysicalProperties,
}

pub(crate) fn enumerate_inner_graph(
    graph: &RelationalJoinGraph,
    required_properties: &RequiredProperties,
    config: RelationalJoinEnumerationConfig,
) -> Result<RelationalInnerJoinEnumeration, RelationalJoinEnumerationError> {
    let memo = build_join_memo(
        &graph.relations,
        Connectivity::Predicates(&graph.predicates),
        EnumerationDomain::Inner,
        config,
    )?;
    let root_key = SemanticKey {
        bindings: graph
            .relations
            .iter()
            .map(|relation| relation.binding)
            .collect(),
        applied_operators: BTreeSet::new(),
    };
    let root = *memo
        .groups
        .get(&root_key)
        .ok_or(RelationalJoinEnumerationError::Disconnected)?;
    let plan = best_plan(
        &graph.relations,
        &memo,
        root,
        required_properties,
        RelationalCsgCmpRightInputPolicy::ProbeOnly,
        &[],
        EnumerationDomain::Inner,
        &mut HashMap::new(),
    )
    .ok_or(RelationalJoinEnumerationError::RequiredPropertiesUnsatisfied)?;
    let mut steps = Vec::new();
    let (base_binding, base_access_path) = plan.root.into_left_deep(&mut steps);
    Ok(RelationalInnerJoinEnumeration {
        plan: RelationalJoinPlan {
            base_binding,
            base_access_path,
            steps: steps
                .into_iter()
                .map(|step| RelationalJoinStep {
                    binding: step.binding,
                    access_path: step.access_path,
                    activated_predicates: step.predicate_ids,
                })
                .collect(),
            cost_breakdown: plan.cost_breakdown,
            properties: plan.properties,
        },
        memo_groups: memo.memo.group_count(),
        memo_expressions: memo.expression_count,
    })
}

pub(crate) fn enumerate_left_deep_rewrites(
    problem: &RelationalJoinRewriteProblem,
    analysis: RelationalJoinConflictAnalysis,
    required_properties: &RequiredProperties,
    config: RelationalJoinEnumerationConfig,
) -> Result<RelationalJoinRewriteEnumeration, RelationalJoinRewriteError> {
    let memo = build_join_memo(
        &problem.relations,
        Connectivity::Operators(&analysis),
        EnumerationDomain::Rewrite,
        config,
    )?;
    let root_key = SemanticKey {
        bindings: analysis.root_bindings.clone(),
        applied_operators: analysis.descriptors.keys().copied().collect(),
    };
    let root = *memo
        .groups
        .get(&root_key)
        .ok_or(RelationalJoinRewriteError::NoLegalRewrite)?;
    let plan = best_plan(
        &problem.relations,
        &memo,
        root,
        required_properties,
        RelationalCsgCmpRightInputPolicy::ProbeOnly,
        &[],
        EnumerationDomain::Rewrite,
        &mut HashMap::new(),
    )
    .ok_or(RelationalJoinEnumerationError::RequiredPropertiesUnsatisfied)?;
    let mut steps = Vec::new();
    let (base_binding, base_access_path) = plan.root.into_left_deep(&mut steps);
    Ok(RelationalJoinRewriteEnumeration {
        plan: RelationalJoinRewritePlan {
            base_binding,
            base_access_path,
            steps: steps
                .into_iter()
                .map(|step| RelationalJoinRewriteStep {
                    binding: step.binding,
                    access_path: step.access_path,
                    operator_id: step.operator_id.expect("rewrite operator has an identity"),
                    operator_kind: step.operator_kind,
                    predicate_ids: step.predicate_ids,
                })
                .collect(),
            cost_breakdown: plan.cost_breakdown,
            properties: plan.properties,
        },
        conflict_analysis: analysis,
        memo_groups: memo.memo.group_count(),
        memo_expressions: memo.expression_count,
    })
}

pub(super) fn enumerate_csg_cmp(
    problem: &RelationalJoinRewriteProblem,
    analysis: RelationalJoinConflictAnalysis,
    required_properties: &RequiredProperties,
    config: RelationalJoinEnumerationConfig,
    right_input_policy: RelationalCsgCmpRightInputPolicy,
    implementations: &[RelationalCsgCmpJoinImplementation],
) -> Result<RelationalCsgCmpEnumeration, RelationalJoinRewriteError> {
    let logical_expression_budget = config
        .max_expressions
        .checked_sub(implementations.len())
        .ok_or(RelationalJoinEnumerationError::ExpressionBudgetExceeded {
            required_expressions: implementations.len(),
            max_expressions: config.max_expressions,
        })?;
    let memo = build_join_memo(
        &problem.relations,
        Connectivity::Operators(&analysis),
        EnumerationDomain::CsgCmp,
        RelationalJoinEnumerationConfig {
            max_expressions: logical_expression_budget,
            ..config
        },
    )
    .map_err(|error| match error {
        RelationalJoinEnumerationError::ExpressionBudgetExceeded {
            required_expressions,
            ..
        } => RelationalJoinEnumerationError::ExpressionBudgetExceeded {
            required_expressions: required_expressions.saturating_add(implementations.len()),
            max_expressions: config.max_expressions,
        },
        other => other,
    })?;
    let root_key = SemanticKey {
        bindings: analysis.root_bindings.clone(),
        applied_operators: analysis.descriptors.keys().copied().collect(),
    };
    let root = *memo
        .groups
        .get(&root_key)
        .ok_or(RelationalJoinRewriteError::NoLegalRewrite)?;
    let root_alternatives = memo
        .memo
        .group(root)
        .expect("root group exists")
        .expressions()
        .len();
    let plan = best_plan(
        &problem.relations,
        &memo,
        root,
        required_properties,
        right_input_policy,
        implementations,
        EnumerationDomain::CsgCmp,
        &mut HashMap::new(),
    )
    .ok_or(RelationalJoinEnumerationError::RequiredPropertiesUnsatisfied)?;
    Ok(RelationalCsgCmpEnumeration {
        plan: RelationalCsgCmpPlan {
            root: plan.root.into_csg_cmp(),
            cost_breakdown: plan.cost_breakdown,
            properties: plan.properties,
        },
        conflict_analysis: analysis,
        alternatives: memo.alternatives,
        memo_groups: memo.memo.group_count(),
        memo_expressions: memo.expression_count.saturating_add(implementations.len()),
        root_alternatives,
    })
}

#[allow(clippy::too_many_arguments)]
fn best_plan(
    relations: &[RelationalJoinRelation],
    memo: &JoinMemo,
    group: GroupId,
    required_properties: &RequiredProperties,
    right_input_policy: RelationalCsgCmpRightInputPolicy,
    implementations: &[RelationalCsgCmpJoinImplementation],
    domain: EnumerationDomain,
    cache: &mut HashMap<(GroupId, RequiredProperties), Option<SelectedPlan>>,
) -> Option<SelectedPlan> {
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
            JoinExpression::Relation(binding) => relations
                .iter()
                .find(|relation| relation.binding == *binding)
                .and_then(|relation| {
                    relation
                        .access_paths
                        .iter()
                        .filter(|access| access.supports_base())
                        .filter(|access| access.properties.satisfies(required_properties))
                        .map(|access| SelectedPlan {
                            root: SelectedNode::Relation {
                                binding: *binding,
                                access_path: access.clone(),
                            },
                            cost_breakdown: estimate_relational_access_path_cost(
                                &access.descriptor,
                            ),
                            properties: access.properties.clone(),
                        })
                        .min_by(|left, right| compare_plans(left, right, domain))
                }),
            JoinExpression::Join {
                left,
                right,
                operator_id,
                operator_kind,
                predicate_ids,
            } => {
                let left_group = *left;
                let right_group = *right;
                let left = best_plan(
                    relations,
                    memo,
                    left_group,
                    required_properties,
                    right_input_policy,
                    implementations,
                    domain,
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
                        relations,
                        binding,
                        &memo
                            .keys
                            .get(&left_group)
                            .expect("csg-cmp memo tracks every left group")
                            .bindings,
                        domain,
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
                            best_plan(
                                relations,
                                memo,
                                right_group,
                                &RequiredProperties::default(),
                                right_input_policy,
                                implementations,
                                domain,
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
                    SelectedPlan {
                        properties: left.properties.clone(),
                        cost_breakdown: estimate_relational_join_cost(
                            left.cost_breakdown,
                            right.cost_breakdown,
                            cardinality,
                            right_input,
                            RelationalJoinSelectivity::Unknown,
                        ),
                        root: SelectedNode::Join {
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
                                Some(implementation.operator_id) == *operator_id
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
                                properties
                                    .satisfies(required_properties)
                                    .then(|| SelectedPlan {
                                        properties,
                                        cost_breakdown: estimate_relational_join_cost(
                                            estimate_relational_access_path_cost(
                                                &implementation.left_access.descriptor,
                                            ),
                                            estimate_relational_access_path_cost(
                                                &implementation.right_access.descriptor,
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
                                        root: SelectedNode::Join {
                                            operator_id: *operator_id,
                                            operator_kind: *operator_kind,
                                            predicate_ids: predicate_ids.clone(),
                                            implementation: Some(Box::new(implementation.clone())),
                                            left: Box::new(SelectedNode::Relation {
                                                binding: implementation.left_binding,
                                                access_path: implementation.left_access.clone(),
                                            }),
                                            right: Box::new(SelectedNode::Relation {
                                                binding: implementation.right_binding,
                                                access_path: implementation.right_access.clone(),
                                            }),
                                        },
                                    })
                            }),
                    )
                    .min_by(|left, right| compare_plans(left, right, domain))
            }
        };
        if let Some(candidate) = candidate
            && selected
                .as_ref()
                .is_none_or(|current| compare_plans(&candidate, current, domain).is_lt())
        {
            selected = Some(candidate);
        }
    }
    cache.insert(key, selected.clone());
    selected
}

fn best_probe_relation_plan(
    relations: &[RelationalJoinRelation],
    binding: BindingId,
    outer_bindings: &BindingSet,
    domain: EnumerationDomain,
) -> Option<SelectedPlan> {
    let access = relations
        .iter()
        .find(|relation| relation.binding == binding)?
        .access_paths
        .iter()
        .filter(|access| {
            access.supports_probe() && access.required_bindings.is_subset(outer_bindings)
        })
        .min_by(|left, right| compare_probe_access_paths(left, right, domain))?;
    Some(SelectedPlan {
        root: SelectedNode::Relation {
            binding,
            access_path: access.clone(),
        },
        cost_breakdown: estimate_relational_access_path_cost(&access.descriptor),
        properties: access.properties.clone(),
    })
}

fn compare_probe_access_paths(
    left: &RelationalJoinAccessPath,
    right: &RelationalJoinAccessPath,
    domain: EnumerationDomain,
) -> std::cmp::Ordering {
    let rows = |access: &RelationalJoinAccessPath| {
        // CSG-CMP ranks costed probes; the flat frontends rank raw estimates.
        // Preserve the distinction at the zero/one cardinality boundary.
        if domain == EnumerationDomain::CsgCmp {
            estimate_relational_access_path_cost(&access.descriptor).estimated_rows
        } else {
            u64::try_from(access.descriptor.estimated_rows).unwrap_or(u64::MAX)
        }
    };
    estimate_relational_access_path_cost(&left.descriptor)
        .cost
        .cmp(&estimate_relational_access_path_cost(&right.descriptor).cost)
        .then_with(|| rows(left).cmp(&rows(right)))
        .then_with(|| {
            right
                .descriptor
                .unique_point
                .cmp(&left.descriptor.unique_point)
        })
        .then_with(|| {
            right
                .descriptor
                .equality_prefix_len
                .cmp(&left.descriptor.equality_prefix_len)
        })
        .then_with(|| left.descriptor.name.cmp(&right.descriptor.name))
}

#[cfg(test)]
mod connected_tests;

fn compare_plans(
    left: &SelectedPlan,
    right: &SelectedPlan,
    domain: EnumerationDomain,
) -> std::cmp::Ordering {
    left.cost_breakdown
        .cost
        .cmp(&right.cost_breakdown.cost)
        .then_with(|| {
            left.cost_breakdown
                .estimated_rows
                .cmp(&right.cost_breakdown.estimated_rows)
        })
        .then_with(|| {
            left.root
                .stable_key(domain)
                .cmp(&right.root.stable_key(domain))
        })
}
