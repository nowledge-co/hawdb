use super::{costing, properties, OptimizerCatalog, PhysicalPlan};
use crate::{
    plan_class_counts, plan_operator_counts, visit_plan_with_ids, OperatorCardinalityEstimate,
    OptimizerContext, SelectedPlanTrace,
};

pub(super) fn selected_plan_trace(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
    context: &OptimizerContext,
) -> SelectedPlanTrace {
    let cost_breakdown = costing::estimate_physical_plan_cost_breakdown(plan, catalog);
    SelectedPlanTrace {
        query_digest: context.query_digest().map(str::to_string),
        explain: plan.explain(0),
        fingerprint: plan.fingerprint(),
        cost: cost_breakdown.as_plan_cost(),
        cost_breakdown,
        properties: properties::selected_plan_properties(plan),
        cardinality_estimates: operator_cardinality_estimates(plan, catalog),
        operator_counts: plan_operator_counts(plan),
        class_counts: plan_class_counts(plan),
    }
}

fn operator_cardinality_estimates(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
) -> Vec<OperatorCardinalityEstimate> {
    let mut estimates = Vec::new();
    visit_plan_with_ids(plan, &mut |operator_id, operator| {
        estimates.push(OperatorCardinalityEstimate {
            operator_id,
            operator: operator.kind(),
            estimated_rows: costing::estimate_physical_plan_cost(operator, catalog).estimated_rows,
        });
    });
    estimates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CascadesOptimizer, OptimizationSearchReport, OptimizerCatalogIndexes,
        OptimizerCatalogStatistics, OptimizerContext,
    };
    use skein_plan::{PhysicalOperatorId, PhysicalPlanKind};

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
