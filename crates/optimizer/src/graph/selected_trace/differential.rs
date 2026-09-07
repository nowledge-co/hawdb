use super::*;
use crate::{visit_plan_with_ids, OptimizerCatalogIndexes, OptimizerCatalogStatistics};
use skein_core::Value;
use skein_cypher::RelationshipDirection;
use skein_plan::{PhysicalPlanKind, Predicate};
use std::collections::BTreeMap;

fn catalog(rows: u64) -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [("Node".into(), "score".into())], []),
        OptimizerCatalogStatistics::new(
            [("Node".into(), rows)],
            [("LINK".into(), rows.saturating_mul(3))],
            [("LINK".into(), rows)],
            [],
            [],
            [(("Node".into(), "score".into()), 16)],
            [(
                ("Node".into(), "score".into()),
                (0..16).map(Value::Int).collect(),
            )],
        ),
    )
}

fn range_seek(cutoff: i64) -> PhysicalPlan {
    PhysicalPlan::IndexNodeRangeSeek {
        variable: "n".into(),
        label: "Node".into(),
        property: "score".into(),
        lower: Some((Value::Int(cutoff), false)),
        upper: None,
    }
}

fn wrap(kind: usize, input: PhysicalPlan) -> PhysicalPlan {
    let input = Box::new(input);
    match kind {
        0 => PhysicalPlan::ProjectExec {
            items: Vec::new(),
            input,
        },
        1 => PhysicalPlan::FilterExec {
            predicate: Predicate::ConstantBool(false),
            input,
        },
        2 => PhysicalPlan::DistinctExec { input },
        3 => PhysicalPlan::SortExec {
            items: Vec::new(),
            input,
        },
        4 => PhysicalPlan::TopNExec {
            items: Vec::new(),
            offset: 3,
            limit: 7,
            input,
        },
        5 => PhysicalPlan::LimitExec {
            offset: 2,
            limit: Some(5),
            input,
        },
        6 => PhysicalPlan::AggregateExec {
            group_keys: Vec::new(),
            items: Vec::new(),
            input,
        },
        7 => PhysicalPlan::NodeColumnLookupExec {
            variable: "n".into(),
            label: "Node".into(),
            property: "score".into(),
            column: "score".into(),
            optional: true,
            input,
        },
        8 => PhysicalPlan::AdjacencyExistsExec {
            source_variable: "n".into(),
            rel_type: "LINK".into(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "m".into(),
            input,
        },
        9 => PhysicalPlan::OptionalDegreeExec {
            source_variable: "n".into(),
            rel_type: "LINK".into(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Incoming,
            target_label: "Node".into(),
            target_properties: BTreeMap::new(),
            alias: "degree".into(),
            input,
        },
        10 => PhysicalPlan::AdjacencyExpandExec {
            source_variable: "n".into(),
            source_label: "Node".into(),
            rel_variable: Some("r".into()),
            rel_type: "LINK".into(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Undirected,
            target_variable: "m".into(),
            target_label: "Node".into(),
            min_hops: 1,
            max_hops: 2,
            optional: false,
            graph_budget: None,
            input,
        },
        _ => unreachable!("unknown unary fixture"),
    }
}

fn assert_matches_recursive_oracle(plan: &PhysicalPlan, catalog: &OptimizerCatalog) -> usize {
    let expected_cost = costing::estimate_physical_plan_cost_breakdown(plan, catalog);
    let mut expected = Vec::new();
    // Retain the old trace strategy as a differential oracle, including its
    // independent metadata traversal and preorder ID assignment.
    visit_plan_with_ids(plan, &mut |operator_id, operator| {
        expected.push(OperatorCardinalityEstimate {
            operator_id,
            operator: operator.kind(),
            estimated_rows: costing::estimate_physical_plan_cost(operator, catalog).estimated_rows,
        });
    });
    costing::take_cost_evaluations();
    let trace = selected_plan_trace(plan, catalog, &OptimizerContext::default());
    let evaluations = costing::take_cost_evaluations();
    assert_eq!(evaluations, expected.len());
    assert_eq!(trace.cost_breakdown, expected_cost);
    assert_eq!(trace.cost, expected_cost.as_plan_cost());
    assert_eq!(trace.cardinality_estimates, expected);
    evaluations
}

#[test]
fn every_unary_cost_rule_preserves_preorder_rows_and_components() {
    for rows in [0, 1, 1024, u64::MAX] {
        let catalog = catalog(rows);
        for kind in 0..11 {
            for cutoff in [0, 7, 15] {
                let plan = wrap(kind, range_seek(cutoff));
                assert_eq!(assert_matches_recursive_oracle(&plan, &catalog), 2);
            }
        }
    }
}

#[test]
fn asymmetric_binary_costs_keep_left_and_right_operator_ids() {
    let plan = PhysicalPlan::NodeCartesianProductExec {
        left: Box::new(wrap(4, range_seek(1))),
        right: Box::new(wrap(6, wrap(3, range_seek(12)))),
    };
    let catalog = catalog(1024);
    assert_eq!(assert_matches_recursive_oracle(&plan, &catalog), 6);
    let trace = selected_plan_trace(&plan, &catalog, &OptimizerContext::default());
    assert_eq!(
        trace.cardinality_estimates[1].operator,
        PhysicalPlanKind::TopNExec
    );
    assert_eq!(
        trace.cardinality_estimates[3].operator,
        PhysicalPlanKind::AggregateExec
    );
    assert_ne!(
        trace.cardinality_estimates[1].estimated_rows,
        trace.cardinality_estimates[3].estimated_rows
    );
}

#[test]
fn saturated_binary_costs_are_unchanged() {
    let scan = || PhysicalPlan::SeqNodeScan {
        variable: "n".into(),
        label: "Node".into(),
    };
    let plan = PhysicalPlan::NodeCartesianProductExec {
        left: Box::new(scan()),
        right: Box::new(scan()),
    };
    let catalog = catalog(u64::MAX);
    assert_eq!(assert_matches_recursive_oracle(&plan, &catalog), 3);
    let trace = selected_plan_trace(&plan, &catalog, &OptimizerContext::default());
    assert_eq!(trace.cost.cost, u64::MAX);
    assert_eq!(trace.cost.estimated_rows, u64::MAX);
}

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn plan(&mut self, depth: usize, remaining: &mut usize) -> PhysicalPlan {
        *remaining -= 1;
        let choice = self.next() % 15;
        if depth == 0 || *remaining == 0 || choice >= 12 {
            return range_seek((self.next() % 18) as i64);
        }
        if choice == 11 && *remaining >= 2 {
            // Reserve a node for the right branch before expanding the left.
            *remaining -= 1;
            let left = self.plan(depth - 1, remaining);
            *remaining += 1;
            let right = self.plan(depth - 1, remaining);
            PhysicalPlan::NodeCartesianProductExec {
                left: Box::new(left),
                right: Box::new(right),
            }
        } else {
            let input = self.plan(depth - 1, remaining);
            wrap((choice % 11) as usize, input)
        }
    }
}

#[test]
#[ignore = "manual deterministic trace-cost differential campaign"]
fn selected_trace_cost_differential_campaign() {
    for seed in [326, 7, 0x5eed] {
        let mut random = Random(seed);
        let mut operator_checks = 0;
        for case in 0..256 {
            let mut remaining = 64;
            let plan = random.plan(10, &mut remaining);
            let rows = if case % 8 == 0 {
                u64::MAX
            } else {
                random.next() % 4096
            };
            let checks = assert_matches_recursive_oracle(&plan, &catalog(rows));
            assert_eq!(checks, 64 - remaining);
            operator_checks += checks;
        }
        eprintln!("trace-cost seed={seed} cases=256 operator_checks={operator_checks}");
    }
}
