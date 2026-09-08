use super::{costing, properties, OptimizerCatalog, PhysicalPlan};
use crate::{
    plan_class_counts, plan_operator_counts, OperatorCardinalityEstimate, OptimizerContext,
    PlanCostBreakdown, SelectedPlanTrace,
};
use skein_plan::PhysicalOperatorId;

#[cfg(test)]
mod differential;

pub(super) fn selected_plan_trace(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
    context: &OptimizerContext,
) -> SelectedPlanTrace {
    let mut cardinality_estimates = Vec::new();
    let cost_breakdown = collect_operator_costs(plan, catalog, &mut cardinality_estimates);
    SelectedPlanTrace {
        query_digest: context.query_digest().map(str::to_string),
        explain: plan.explain(0),
        fingerprint: plan.fingerprint(),
        cost: cost_breakdown.as_plan_cost(),
        cost_breakdown,
        properties: properties::selected_plan_properties(plan),
        cardinality_estimates,
        operator_counts: plan_operator_counts(plan),
        class_counts: plan_class_counts(plan),
    }
}

fn collect_operator_costs(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
    estimates: &mut Vec<OperatorCardinalityEstimate>,
) -> PlanCostBreakdown {
    collect_costed_plan(plan, catalog, estimates).cost
}

fn collect_costed_plan<'a>(
    plan: &'a PhysicalPlan,
    catalog: &OptimizerCatalog,
    estimates: &mut Vec<OperatorCardinalityEstimate>,
) -> costing::CostedPlan<'a> {
    // Reserve IDs before traversing children, matching visit_plan_with_ids even
    // though a parent's cost is only available after both child costs.
    let ordinal = estimates.len();
    estimates.push(OperatorCardinalityEstimate {
        operator_id: PhysicalOperatorId::from_ordinal(ordinal),
        operator: plan.kind(),
        estimated_rows: 0,
    });
    let inputs =
        costing::estimate_input_costs(plan, |input| collect_costed_plan(input, catalog, estimates));
    let costed = costing::estimate_operator_cost(plan, catalog, inputs);
    estimates[ordinal].estimated_rows = costed.cost.estimated_rows;
    costed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CascadesOptimizer, OptimizationSearchReport, OptimizerCatalogIndexes,
        OptimizerCatalogStatistics, OptimizerContext,
    };
    use skein_plan::PhysicalPlanKind;

    #[test]
    fn selected_trace_derives_cardinality_metadata_once_per_operator() {
        let mut measurements = Vec::new();
        for depth in [0, 8, 16, 32] {
            let mut catalog = OptimizerCatalog::default();
            catalog.label_counts.insert("Node".to_string(), 64);
            catalog
                .property_distinct_counts
                .insert(("Node".to_string(), "key".to_string()), 1);
            let mut plan = PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Node".to_string(),
            };
            for _ in 0..depth {
                plan = PhysicalPlan::FilterExec {
                    predicate: skein_plan::Predicate::PropertyEq {
                        variable: "n".to_string(),
                        property: "key".to_string(),
                        value: skein_core::Value::Int(7),
                    },
                    input: Box::new(plan),
                };
            }
            super::super::cardinality::take_metadata_visits();
            let trace = selected_plan_trace(&plan, &catalog, &OptimizerContext::default());
            let visits = super::super::cardinality::take_metadata_visits();
            assert_eq!(trace.cardinality_estimates.len(), depth + 1);
            assert!(trace
                .cardinality_estimates
                .iter()
                .all(|item| item.estimated_rows == 64));
            assert_eq!(
                trace.cost_breakdown,
                PlanCostBreakdown::new(64, depth as u64 * 64, 0, 68, 0)
            );
            measurements.push((depth, visits));
        }
        eprintln!("Cardinality metadata (depth, node visits): {measurements:?}");
        for (depth, visits) in measurements {
            assert!(
                visits <= depth + 1,
                "depth {depth}: {visits} metadata node visits"
            );
        }
    }

    #[test]
    fn selected_trace_costs_each_operator_once() {
        for depth in [32, 0, 1] {
            let mut plan = PhysicalPlan::IndexNodeRangeSeek {
                variable: "n".to_string(),
                label: "Node".to_string(),
                property: "score".to_string(),
                lower: Some((skein_core::Value::Int(5), false)),
                upper: None,
            };
            for _ in 0..depth {
                plan = PhysicalPlan::ProjectExec {
                    items: Vec::new(),
                    input: Box::new(plan),
                };
            }
            costing::take_cost_evaluations();
            let trace = selected_plan_trace(
                &plan,
                &OptimizerCatalog::default(),
                &OptimizerContext::default(),
            );
            let evaluations = costing::take_cost_evaluations();
            assert_eq!(trace.cardinality_estimates.len(), depth + 1);
            assert_eq!(evaluations, depth + 1, "each operator must be costed once");
        }
    }

    #[test]
    fn deep_cost_collection_does_not_reenter_operator_cost_frames() {
        for depth in [127, 255] {
            let mut plan = PhysicalPlan::EmptyExec;
            for _ in 0..depth {
                plan = PhysicalPlan::ProjectExec {
                    items: Vec::new(),
                    input: Box::new(plan),
                };
            }
            let mut estimates = Vec::new();
            costing::take_cost_evaluations();
            let cost = collect_operator_costs(&plan, &OptimizerCatalog::default(), &mut estimates);
            assert_eq!(costing::take_cost_evaluations(), depth + 1);
            assert_eq!(estimates.len(), depth + 1);
            assert_eq!(cost.estimated_rows, 1);
            assert_eq!(cost.cost, depth as u64);
            assert_eq!(
                cost,
                costing::estimate_physical_plan_cost_breakdown(&plan, &OptimizerCatalog::default())
            );
        }
    }

    #[test]
    fn selected_trace_assigns_estimates_to_stable_operator_ids() {
        let plan = PhysicalPlan::ProjectExec {
            items: Vec::new(),
            input: Box::new(PhysicalPlan::FilterExec {
                predicate: skein_plan::Predicate::ConstantBool(false),
                input: Box::new(PhysicalPlan::SeqNodeScan {
                    variable: "n".to_string(),
                    label: "Node".to_string(),
                }),
            }),
        };

        let trace = selected_plan_trace(
            &plan,
            &OptimizerCatalog::default(),
            &OptimizerContext::default(),
        );

        assert_eq!(trace.cardinality_estimates.len(), 3);
        assert_eq!(
            trace
                .cardinality_estimates
                .iter()
                .map(|estimate| (estimate.operator_id, estimate.operator))
                .collect::<Vec<_>>(),
            vec![
                (
                    PhysicalOperatorId::from_ordinal(0),
                    PhysicalPlanKind::ProjectExec,
                ),
                (
                    PhysicalOperatorId::from_ordinal(1),
                    PhysicalPlanKind::FilterExec,
                ),
                (
                    PhysicalOperatorId::from_ordinal(2),
                    PhysicalPlanKind::SeqNodeScan,
                ),
            ]
        );
        assert_eq!(
            trace.cardinality_estimates[0].estimated_rows,
            trace.cost.estimated_rows
        );
    }

    #[test]
    fn refreshing_a_trace_replaces_parameter_sensitive_costing() {
        fn range_seek(cutoff: i64) -> PhysicalPlan {
            PhysicalPlan::IndexNodeRangeSeek {
                variable: "m".to_string(),
                label: "Memory".to_string(),
                property: "score".to_string(),
                lower: Some((skein_core::Value::Int(cutoff), false)),
                upper: None,
            }
        }

        let histogram = (0..10).map(skein_core::Value::Int).collect::<Vec<_>>();
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [("Memory".to_string(), "score".to_string())], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 10)],
                [],
                [],
                [],
                [],
                [],
                [(("Memory".to_string(), "score".to_string()), histogram)],
            ),
        );
        let context = OptimizerContext::default();
        let initial = selected_plan_trace(&range_seek(1), &catalog, &context);
        let mut report = OptimizationSearchReport::memo(1);
        report.record_selected_plan_cost(initial.cost);
        let mut trace = report.into_trace(initial);
        let expected = selected_plan_trace(&range_seek(8), &catalog, &context);

        CascadesOptimizer::with_context(context).refresh_trace_for_physical_plan(
            &mut trace,
            &range_seek(8),
            &catalog,
        );

        assert_eq!(trace.selected_plan_cost, expected.cost);
        assert_eq!(trace.selected_plan_cost_breakdown, expected.cost_breakdown);
        assert_eq!(
            trace.selected_plan_cardinality_estimates,
            expected.cardinality_estimates
        );
        let expected_detail = format!(
            "estimated_rows={} cost={}",
            expected.cost.estimated_rows, expected.cost.cost
        );
        assert!(trace.decisions.iter().any(|decision| {
            decision == &format!("selected physical plan cost: {expected_detail}")
        }));
        assert!(trace.rule_events.iter().any(|event| {
            event.rule() == "physical plan cost" && event.detail() == expected_detail
        }));
    }
}
