use super::*;
use crate::{
    enumerate_relational_inner_joins, enumerate_relational_join_rewrites, RelationalJoinGraph,
    RelationalJoinPredicate,
};

fn graph(problem: &RelationalJoinRewriteProblem) -> RelationalJoinGraph {
    let analysis = analyze_relational_join_conflicts(&problem.initial_tree, None).unwrap();
    RelationalJoinGraph {
        relations: problem.relations.clone(),
        predicates: analysis
            .descriptors
            .values()
            .flat_map(|descriptor| {
                descriptor
                    .predicate_ids
                    .iter()
                    .map(|id| RelationalJoinPredicate {
                        id: *id,
                        bindings: descriptor.predicate_bindings.clone(),
                    })
            })
            .collect(),
    }
}

fn binding_order(node: &RelationalCsgCmpPlanNode) -> Vec<BindingId> {
    match node {
        RelationalCsgCmpPlanNode::Relation { binding, .. } => vec![*binding],
        RelationalCsgCmpPlanNode::Join { left, right, .. } => binding_order(left)
            .into_iter()
            .chain(binding_order(right))
            .collect(),
    }
}

#[test]
fn zero_estimate_probe_ranking_preserves_each_frontend_contract() {
    for reverse in [false, true] {
        let mut inner = indexed_relation(B, 100_000, A);
        let mut zero = inner.access_paths[1].clone();
        zero.descriptor.name = "z_zero".into();
        zero.descriptor.estimated_rows = 0;
        zero.descriptor.unique_point = false;
        inner.access_paths[1].descriptor.name = "a_one".into();
        inner.access_paths.push(zero);
        if reverse {
            inner.access_paths.reverse();
        }
        let problem = RelationalJoinRewriteProblem {
            relations: vec![relation(A, 1), inner],
            initial_tree: RelationalJoinTree::join(
                operator(1, RelationalJoinOperatorKind::Inner, &[A, B]),
                RelationalJoinTree::Relation(A),
                RelationalJoinTree::Relation(B),
            ),
            post_join_filter: None,
        };
        let required = RequiredProperties::default();
        let config = RelationalJoinEnumerationConfig::default();
        let flat = enumerate_relational_inner_joins(&graph(&problem), &required, config).unwrap();
        let rewrite = enumerate_relational_join_rewrites(&problem, &required, config).unwrap();
        let csg = enumerate_relational_csg_cmp_joins(&problem, &required, config).unwrap();
        assert_eq!(flat.plan.binding_order(), [A, B]);
        assert_eq!(rewrite.plan.binding_order(), [A, B]);
        assert_eq!(binding_order(&csg.plan.root), [A, B]);
        assert_eq!(flat.plan.steps[0].access_path.descriptor.name, "z_zero");
        assert_eq!(rewrite.plan.steps[0].access_path.descriptor.name, "z_zero");
        let RelationalCsgCmpPlanNode::Join { right, .. } = &csg.plan.root else {
            panic!("expected join")
        };
        let RelationalCsgCmpPlanNode::Relation { access_path, .. } = right.as_ref() else {
            panic!("expected probe")
        };
        assert_eq!(access_path.descriptor.name, "a_one");
        assert_eq!(flat.plan.cost_breakdown, rewrite.plan.cost_breakdown);
        assert_eq!(flat.plan.cost_breakdown, csg.plan.cost_breakdown);
    }
}

#[test]
fn probe_only_selection_does_not_expand_the_flat_search_budget() {
    let bindings = [A, B, C, D];
    let tree = bindings.windows(2).enumerate().fold(
        RelationalJoinTree::Relation(A),
        |left, (index, pair)| {
            RelationalJoinTree::join(
                operator(index as u32, RelationalJoinOperatorKind::Inner, pair),
                left,
                RelationalJoinTree::Relation(pair[1]),
            )
        },
    );
    let problem = problem(&bindings, tree, None);
    let required = RequiredProperties::default();
    let config = RelationalJoinEnumerationConfig {
        max_groups: 10,
        max_expressions: 16,
    };
    let flat = enumerate_relational_inner_joins(&graph(&problem), &required, config).unwrap();
    let rewrite = enumerate_relational_join_rewrites(&problem, &required, config).unwrap();
    assert_eq!((flat.memo_groups, flat.memo_expressions), (10, 16));
    assert_eq!((rewrite.memo_groups, rewrite.memo_expressions), (10, 16));
    for policy in [
        RelationalCsgCmpRightInputPolicy::ProbeOnly,
        RelationalCsgCmpRightInputPolicy::AllowMaterialized,
    ] {
        assert_eq!(
            enumerate_relational_csg_cmp_joins_with_right_input_policy(
                &problem, &required, config, policy
            ),
            Err(RelationalJoinEnumerationError::ExpressionBudgetExceeded {
                required_expressions: 17,
                max_expressions: 16
            }
            .into())
        );
        let result = enumerate_relational_csg_cmp_joins_with_right_input_policy(
            &problem,
            &required,
            RelationalJoinEnumerationConfig {
                max_expressions: 24,
                ..config
            },
            policy,
        )
        .unwrap();
        assert_eq!(
            (
                result.memo_groups,
                result.memo_expressions,
                result.root_alternatives
            ),
            (10, 24, 6)
        );
        assert!(result
            .alternatives
            .iter()
            .any(|alternative| alternative.left_bindings.len() == 2
                && alternative.right_bindings.len() == 2));
    }
}

#[test]
fn base_only_accesses_require_the_materialization_capability() {
    let mut problem = problem(
        &[A, B],
        RelationalJoinTree::join(
            operator(1, RelationalJoinOperatorKind::Inner, &[A, B]),
            RelationalJoinTree::Relation(A),
            RelationalJoinTree::Relation(B),
        ),
        None,
    );
    for (relation, rows) in problem.relations.iter_mut().zip([2, 3]) {
        let mut descriptor = relation.access_paths[0].descriptor.clone();
        descriptor.estimated_rows = rows;
        relation.access_paths = vec![RelationalJoinAccessPath::base(descriptor)];
    }
    let required = RequiredProperties::default();
    let config = RelationalJoinEnumerationConfig::default();
    assert_eq!(
        enumerate_relational_inner_joins(&graph(&problem), &required, config),
        Err(RelationalJoinEnumerationError::RequiredPropertiesUnsatisfied)
    );
    assert_eq!(
        enumerate_relational_join_rewrites(&problem, &required, config),
        Err(RelationalJoinEnumerationError::RequiredPropertiesUnsatisfied.into())
    );
    assert_eq!(
        enumerate_relational_csg_cmp_joins_with_right_input_policy(
            &problem,
            &required,
            config,
            RelationalCsgCmpRightInputPolicy::ProbeOnly
        ),
        Err(RelationalJoinEnumerationError::RequiredPropertiesUnsatisfied.into())
    );
    let csg = enumerate_relational_csg_cmp_joins(&problem, &required, config).unwrap();
    assert_eq!(
        csg.plan.cost(),
        PlanCost {
            estimated_rows: 1,
            cost: 11
        }
    );
}

#[test]
fn equal_cost_ties_preserve_predicate_and_operator_ordering_domains() {
    let ten = BindingId::new(10);
    let tree = RelationalJoinTree::join(
        operator(10, RelationalJoinOperatorKind::Inner, &[ten, B]),
        RelationalJoinTree::join(
            operator(2, RelationalJoinOperatorKind::Inner, &[A, ten]),
            RelationalJoinTree::Relation(A),
            RelationalJoinTree::Relation(ten),
        ),
        RelationalJoinTree::Relation(B),
    );
    let mut problem = problem(&[A, ten, B], tree, None);
    for relation in &mut problem.relations {
        relation.access_paths[0].descriptor.estimated_rows = 1;
    }
    let config = RelationalJoinEnumerationConfig::default();
    let required = RequiredProperties::default();
    let flat = enumerate_relational_inner_joins(&graph(&problem), &required, config).unwrap();
    let rewrite = enumerate_relational_join_rewrites(&problem, &required, config).unwrap();
    let csg = enumerate_relational_csg_cmp_joins(&problem, &required, config).unwrap();
    assert_eq!(flat.plan.binding_order(), [ten, A, B]);
    assert_eq!(rewrite.plan.binding_order(), [ten, B, A]);
    assert_eq!(binding_order(&csg.plan.root), [ten, B, A]);
    assert_eq!(
        flat.plan.cost(),
        PlanCost {
            estimated_rows: 1,
            cost: 3
        }
    );
    assert_eq!(rewrite.plan.cost(), flat.plan.cost());
    assert_eq!(csg.plan.cost(), flat.plan.cost());
}
