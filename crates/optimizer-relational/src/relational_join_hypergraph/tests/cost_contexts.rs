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

use super::*;
use crate::{
    enumerate_relational_inner_joins, enumerate_relational_inner_joins_with_cost_contexts,
    enumerate_relational_join_rewrites, enumerate_relational_join_rewrites_with_cost_contexts,
    RelationalAccessCostContext, RelationalJoinCostContexts, RelationalJoinGraph,
    RelationalJoinPredicate,
};
use std::num::NonZeroU64;

fn snapshot(rows: u64, pages: u64) -> RelationalAccessCostContext {
    RelationalAccessCostContext::for_snapshot_rows(
        NonZeroU64::new(rows).unwrap(),
        NonZeroU64::new(pages).unwrap(),
    )
}

fn probe_fixture() -> (RelationalJoinGraph, RelationalJoinRewriteProblem) {
    let mut left = relation(A, 2);
    left.access_paths[0] = RelationalJoinAccessPath::base(left.access_paths[0].descriptor.clone());
    let scan = relation(B, 1000).access_paths.remove(0).descriptor;
    // A conservative, untrusted estimate still has to use the same context
    // when the memo ranks a probe and when it charges the selected probe.
    let mut point = scan.clone();
    point.kind = RelationalAccessPathKind::PrimaryKey;
    point.name = "pk".into();
    point.index_columns = vec!["id".into()];
    point.access_columns = BTreeSet::from(["id".into()]);
    point.equality_prefix_len = 1;
    point.unique_point = true;
    point.estimated_rows = 500;
    // All three frontends require a base access for every relation. Keep B's
    // unfiltered base alternative expensive so this fixture isolates probes.
    let mut unfiltered = scan.clone();
    unfiltered.estimated_rows = 1_000_000;
    let right = RelationalJoinRelation {
        binding: B,
        access_paths: vec![
            RelationalJoinAccessPath::base(unfiltered),
            RelationalJoinAccessPath::probe(point, A.into()),
            RelationalJoinAccessPath::probe(scan, A.into()),
        ],
    };
    let relations = vec![left, right];
    let graph = RelationalJoinGraph {
        relations: relations.clone(),
        predicates: vec![RelationalJoinPredicate {
            id: RelationalJoinPredicateId::new(1),
            bindings: BindingSet::from([A, B]),
        }],
    };
    let problem = RelationalJoinRewriteProblem {
        relations,
        initial_tree: RelationalJoinTree::join(
            operator(1, RelationalJoinOperatorKind::Inner, &[A, B]),
            RelationalJoinTree::Relation(A),
            RelationalJoinTree::Relation(B),
        ),
        post_join_filter: None,
    };
    (graph, problem)
}

fn probe_results(
    graph: &RelationalJoinGraph,
    problem: &RelationalJoinRewriteProblem,
    contexts: &RelationalJoinCostContexts,
) -> [(RelationalAccessPathKind, PlanCostBreakdown); 3] {
    let required = RequiredProperties::default();
    let config = RelationalJoinEnumerationConfig::default();
    let inner =
        enumerate_relational_inner_joins_with_cost_contexts(graph, &required, config, contexts)
            .unwrap();
    let rewrite =
        enumerate_relational_join_rewrites_with_cost_contexts(problem, &required, config, contexts)
            .unwrap();
    let csg = enumerate_relational_csg_cmp_joins_with_cost_contexts(
        problem,
        &required,
        config,
        RelationalCsgCmpRightInputPolicy::AllowMaterialized,
        &[],
        contexts,
    )
    .unwrap();
    let RelationalCsgCmpPlanNode::Join { right, .. } = &csg.plan.root else {
        panic!("expected one probe join");
    };
    let RelationalCsgCmpPlanNode::Relation { access_path, .. } = right.as_ref() else {
        panic!("expected one probe relation");
    };
    [
        (
            inner.plan.steps[0].access_path.descriptor.kind,
            inner.plan.cost_breakdown,
        ),
        (
            rewrite.plan.steps[0].access_path.descriptor.kind,
            rewrite.plan.cost_breakdown,
        ),
        (access_path.descriptor.kind, csg.plan.cost_breakdown),
    ]
}

#[test]
fn every_memo_frontend_uses_per_binding_context_for_base_and_probe_selection() {
    let (graph, problem) = probe_fixture();
    let required = RequiredProperties::default();
    let config = RelationalJoinEnumerationConfig::default();
    let empty = RelationalJoinCostContexts::default();
    assert_eq!(
        enumerate_relational_inner_joins(&graph, &required, config).unwrap(),
        enumerate_relational_inner_joins_with_cost_contexts(&graph, &required, config, &empty)
            .unwrap(),
    );
    assert_eq!(
        enumerate_relational_join_rewrites(&problem, &required, config).unwrap(),
        enumerate_relational_join_rewrites_with_cost_contexts(&problem, &required, config, &empty)
            .unwrap(),
    );
    assert_eq!(
        enumerate_relational_csg_cmp_joins(&problem, &required, config).unwrap(),
        enumerate_relational_csg_cmp_joins_with_cost_contexts(
            &problem,
            &required,
            config,
            RelationalCsgCmpRightInputPolicy::AllowMaterialized,
            &[],
            &empty,
        )
        .unwrap(),
    );
    let legacy = PlanCostBreakdown::new(1000, 1006, 1000, 2, 1002);
    assert_eq!(
        probe_results(&graph, &problem, &empty),
        [(RelationalAccessPathKind::PrimaryKey, legacy); 3]
    );

    let left_only = empty.with_relation(A, snapshot(2, 1));
    let point = PlanCostBreakdown::new(1000, 1009, 1000, 5, 1002);
    assert_eq!(
        probe_results(&graph, &problem, &left_only),
        [(RelationalAccessPathKind::PrimaryKey, point); 3]
    );
    let mixed = left_only.clone().with_relation(B, snapshot(1000, 4));
    // A costs CPU9/sequential5. B scans CPU1016/sequential1012 per outer row,
    // including its four ordered descriptor records and eight bound keys.
    // Raw components compose once; cardinality remains independently 2*1000.
    let scan = PlanCostBreakdown::new(2000, 2041, 0, 2029, 2002);
    assert_eq!(
        probe_results(&graph, &problem, &mixed),
        [(RelationalAccessPathKind::FullScan, scan); 3]
    );
    assert_eq!(
        probe_results(
            &graph,
            &problem,
            &mixed.clone().with_relation(C, snapshot(1, u64::MAX))
        ),
        probe_results(&graph, &problem, &mixed),
    );
    assert_eq!(
        probe_results(
            &graph,
            &problem,
            &mixed.with_relation(B, RelationalAccessCostContext::default())
        ),
        probe_results(&graph, &problem, &left_only),
        "a previous enumeration's context must not leak through memo caching",
    );
}

#[test]
fn implementation_costs_include_each_bindings_snapshot_hash_locator_replay() {
    let (problem, hash) = super::implementations::fixture();
    let contexts = RelationalJoinCostContexts::default()
        .with_relation(A, snapshot(100, 4))
        .with_relation(B, snapshot(200, 16));
    for (algorithm, cpu, random) in [
        // Input scans cost CPU368/sequential360/output300. Hash adds CPU600
        // and re-fetches both sides for each of the 100 estimated matches:
        // A(depth3): CPU600/random1000; B(depth5): CPU800/random1600.
        (RelationalEquiJoinAlgorithm::Hash, 2368, 2600),
        // Merge keeps its projected rows and has no locator point replay.
        (RelationalEquiJoinAlgorithm::Merge, 768, 0),
    ] {
        let mut implementation = hash.clone();
        implementation.algorithm = algorithm;
        let result = enumerate_relational_csg_cmp_joins_with_cost_contexts(
            &problem,
            &RequiredProperties::default(),
            RelationalJoinEnumerationConfig::default(),
            RelationalCsgCmpRightInputPolicy::AllowMaterialized,
            &[implementation],
            &contexts,
        )
        .unwrap();
        let RelationalCsgCmpPlanNode::Join {
            implementation: Some(selected),
            ..
        } = result.plan.root
        else {
            panic!("expected costed implementation");
        };
        assert_eq!(selected.algorithm, algorithm);
        assert_eq!(
            result.plan.cost_breakdown,
            PlanCostBreakdown::new(100, cpu, random, 360, 300)
        );
    }
}

#[test]
fn recursive_materialized_right_retains_every_relation_context() {
    let problem = RelationalJoinRewriteProblem {
        relations: vec![relation(A, 10), relation(B, 3), relation(C, 4)],
        initial_tree: RelationalJoinTree::join(
            operator(1, RelationalJoinOperatorKind::LeftOuter, &[A, B, C]),
            RelationalJoinTree::Relation(A),
            RelationalJoinTree::join(
                operator(2, RelationalJoinOperatorKind::Inner, &[B, C]),
                RelationalJoinTree::Relation(B),
                RelationalJoinTree::Relation(C),
            ),
        ),
        post_join_filter: None,
    };
    let contexts = RelationalJoinCostContexts::default()
        .with_relation(A, snapshot(10, 1))
        .with_relation(B, snapshot(3, 2))
        .with_relation(C, snapshot(4, 4));
    let result = enumerate_relational_csg_cmp_joins_with_cost_contexts(
        &problem,
        &RequiredProperties::default(),
        RelationalJoinEnumerationConfig::default(),
        RelationalCsgCmpRightInputPolicy::AllowMaterialized,
        &[],
        &contexts,
    )
    .unwrap();
    assert!(result.plan.root.has_materialized_right());
    // Independently enumerate both legal inner orders inside the preserved
    // right subtree. A is CPU17/sequential13/output10, B is 13/9/3 and C is
    // 20/16/4. Each inner probe produces 12 rows; the outer materialization
    // pays that subtree once plus 10*12 pair operations.
    let alternatives = [
        (
            B,
            C,
            PlanCostBreakdown::new(
                12,
                17 + 13 + 3 * 20 + 10 * 12,
                0,
                13 + 9 + 3 * 16,
                10 + 3 + 3 * 4,
            ),
        ),
        (
            C,
            B,
            PlanCostBreakdown::new(
                12,
                17 + 20 + 4 * 13 + 10 * 12,
                0,
                13 + 16 + 4 * 9,
                10 + 4 + 4 * 3,
            ),
        ),
    ];
    let (first, second, expected) = alternatives
        .into_iter()
        .min_by_key(|(_, _, cost)| cost.cost)
        .unwrap();
    let RelationalCsgCmpPlanNode::Join {
        operator_kind,
        left,
        right,
        ..
    } = &result.plan.root
    else {
        panic!("expected preserved outer join");
    };
    assert_eq!(*operator_kind, RelationalJoinOperatorKind::LeftOuter);
    assert!(
        matches!(left.as_ref(), RelationalCsgCmpPlanNode::Relation { binding, .. } if *binding == A)
    );
    let RelationalCsgCmpPlanNode::Join {
        operator_kind,
        left,
        right,
        ..
    } = right.as_ref()
    else {
        panic!("expected the materialized right inner subtree");
    };
    assert_eq!(*operator_kind, RelationalJoinOperatorKind::Inner);
    assert!(
        matches!(left.as_ref(), RelationalCsgCmpPlanNode::Relation { binding, .. } if *binding == first)
    );
    assert!(
        matches!(right.as_ref(), RelationalCsgCmpPlanNode::Relation { binding, .. } if *binding == second)
    );
    assert_eq!(result.plan.cost_breakdown, expected);
}
