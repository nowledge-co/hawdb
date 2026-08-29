use super::{costing, properties, OptimizerCatalog, PhysicalPlan};
use crate::{plan_class_counts, plan_operator_counts, OptimizerContext, SelectedPlanTrace};

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
        operator_counts: plan_operator_counts(plan),
        class_counts: plan_class_counts(plan),
    }
}
