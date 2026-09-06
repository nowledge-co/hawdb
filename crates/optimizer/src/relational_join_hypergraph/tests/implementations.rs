use super::*;

fn fixture() -> (
    RelationalJoinRewriteProblem,
    RelationalCsgCmpJoinImplementation,
) {
    let left = relation(A, 100);
    let right = relation(B, 200);
    let implementation = RelationalCsgCmpJoinImplementation {
        operator_id: RelationalJoinOperatorId::new(1),
        operator_kind: RelationalJoinOperatorKind::Inner,
        predicate_ids: vec![RelationalJoinPredicateId::new(1)],
        left_binding: A,
        left_access: RelationalJoinAccessPath::base(left.access_paths[0].descriptor.clone()),
        right_binding: B,
        right_access: RelationalJoinAccessPath::base(right.access_paths[0].descriptor.clone()),
        algorithm: RelationalEquiJoinAlgorithm::Hash,
        selectivity: RelationalJoinSelectivity::equi_join(Some(100), Some(200)),
    };
    let problem = RelationalJoinRewriteProblem {
        relations: vec![left, right],
        initial_tree: RelationalJoinTree::join(
            operator(1, RelationalJoinOperatorKind::Inner, &[A, B]),
            RelationalJoinTree::Relation(A),
            RelationalJoinTree::Relation(B),
        ),
        post_join_filter: None,
    };
    (problem, implementation)
}

fn enumerate(
    problem: &RelationalJoinRewriteProblem,
    implementations: &[RelationalCsgCmpJoinImplementation],
    required: &RequiredProperties,
    max_expressions: usize,
) -> Result<RelationalCsgCmpEnumeration, RelationalJoinRewriteError> {
    enumerate_relational_csg_cmp_joins_with_implementations(
        problem,
        required,
        RelationalJoinEnumerationConfig {
            max_expressions,
            ..RelationalJoinEnumerationConfig::default()
        },
        RelationalCsgCmpRightInputPolicy::AllowMaterialized,
        implementations,
    )
}

#[test]
fn costed_implementations_compete_and_charge_the_same_expression_budget() {
    let (problem, hash) = fixture();
    let old = enumerate(&problem, &[], &RequiredProperties::default(), 128).unwrap();
    let result = enumerate(
        &problem,
        std::slice::from_ref(&hash),
        &RequiredProperties::default(),
        128,
    )
    .unwrap();
    assert!(result.plan.cost().cost < old.plan.cost().cost);
    assert_eq!(result.plan.cost_breakdown.estimated_rows, 100);
    assert_eq!(result.plan.cost_breakdown.cpu, 900);
    assert_eq!(result.memo_expressions, old.memo_expressions + 1);
    let exact = result.memo_expressions;
    assert_eq!(
        enumerate(
            &problem,
            std::slice::from_ref(&hash),
            &RequiredProperties::default(),
            exact
        )
        .unwrap(),
        result
    );
    assert!(matches!(
        enumerate(&problem, std::slice::from_ref(&hash), &RequiredProperties::default(), exact - 1),
        Err(RelationalJoinRewriteError::Enumeration(RelationalJoinEnumerationError::ExpressionBudgetExceeded {
            required_expressions, max_expressions,
        })) if required_expressions == exact && max_expressions == exact - 1
    ));
    let mut merge = hash.clone();
    merge.algorithm = RelationalEquiJoinAlgorithm::Merge;
    let result = enumerate(
        &problem,
        &[hash, merge],
        &RequiredProperties::default(),
        128,
    )
    .unwrap();
    let RelationalCsgCmpPlanNode::Join {
        implementation: Some(selected),
        ..
    } = result.plan.root
    else {
        panic!("expected costed implementation")
    };
    assert_eq!(selected.algorithm, RelationalEquiJoinAlgorithm::Merge);
    assert_eq!(result.plan.cost_breakdown.cpu, 700);
}

#[test]
fn implementations_cannot_cross_binding_operator_or_predicate_boundaries() {
    let (problem, hash) = fixture();
    let old = enumerate(&problem, &[], &RequiredProperties::default(), 128).unwrap();
    for fault in 0..4 {
        let mut invalid = hash.clone();
        match fault {
            0 => invalid.left_binding = C,
            1 => invalid.operator_id = RelationalJoinOperatorId::new(2),
            2 => invalid.predicate_ids.clear(),
            _ => invalid.operator_kind = RelationalJoinOperatorKind::LeftOuter,
        }
        let result = enumerate(&problem, &[invalid], &RequiredProperties::default(), 128).unwrap();
        assert_eq!(result.plan, old.plan);
    }
}

#[test]
fn grace_hash_does_not_claim_left_ordering() {
    let (mut problem, mut hash) = fixture();
    let required = RequiredProperties {
        ordering: vec!["key".to_owned()],
        ..RequiredProperties::default()
    };
    for relation in &mut problem.relations {
        relation.access_paths[0].properties.ordering = required.ordering.clone();
    }
    hash.left_access.properties.ordering = required.ordering.clone();
    let result = enumerate(&problem, &[hash.clone()], &required, 128).unwrap();
    let RelationalCsgCmpPlanNode::Join { implementation, .. } = &result.plan.root else {
        panic!("expected join")
    };
    assert!(implementation.is_none());
    assert!(result.plan.properties.satisfies(&required));
    hash.algorithm = RelationalEquiJoinAlgorithm::Merge;
    let result = enumerate(&problem, &[hash], &required, 128).unwrap();
    let RelationalCsgCmpPlanNode::Join { implementation, .. } = &result.plan.root else {
        panic!("expected join")
    };
    assert!(implementation.is_some());
}
