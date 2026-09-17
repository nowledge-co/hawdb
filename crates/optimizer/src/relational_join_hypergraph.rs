//! Hypergraph csg-cmp enumeration for bound relational join operators.

use crate::{
    analyze_relational_join_conflicts,
    relational_join_cost::{RelationalJoinRightInput, RelationalJoinSelectivity},
    relational_join_rewrite::validate_problem_relations,
    PhysicalProperties, PlanCost, PlanCostBreakdown, RelationalJoinAccessPath,
    RelationalJoinConflictAnalysis, RelationalJoinEnumerationConfig, RelationalJoinOperatorId,
    RelationalJoinOperatorKind, RelationalJoinPredicateId, RelationalJoinRewriteError,
    RelationalJoinRewriteProblem, RequiredProperties,
};
use skein_expression::{BindingId, BindingSet};

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

mod memo;
pub(crate) use memo::{enumerate_inner_graph, enumerate_left_deep_rewrites};

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
    memo::enumerate_csg_cmp(
        problem,
        analysis,
        required_properties,
        config,
        right_input_policy,
        implementations,
    )
}

fn binding_union(left: &BindingSet, right: &BindingSet) -> BindingSet {
    left.iter().chain(right.iter()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    mod implementations;
    mod memo_contracts;
    use crate::{
        RelationalAccessPathDescriptor, RelationalAccessPathKind, RelationalJoinEnumerationError,
        RelationalJoinOperator, RelationalJoinRelation, RelationalJoinTree,
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
                cost: 14,
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
    fn csg_cmp_probe_prefers_a_cheaper_scan_over_a_longer_equality_prefix() {
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
        assert_eq!(access_path.descriptor.name, "__full_scan");
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
