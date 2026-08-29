use crate::cost::{PlanCost, PlanCostBreakdown};
use crate::properties::PhysicalProperties;
use crate::search::{RuleEvent, SearchMode};
use crate::stage::StageTrace;
use skein_plan::{PhysicalOperatorId, PhysicalPlanKind};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerConfig {
    pub max_groups: usize,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self { max_groups: 128 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerTrace {
    pub groups: usize,
    pub search_mode: SearchMode,
    pub query_digest: Option<String>,
    pub selected_plan: String,
    /// The physical operator-tree shape. Bound values are intentionally absent.
    pub selected_plan_fingerprint: String,
    pub selected_plan_cost: PlanCost,
    pub selected_plan_cost_breakdown: PlanCostBreakdown,
    pub selected_plan_properties: PhysicalProperties,
    /// Per-operator cardinalities keyed by stable, plan-local pre-order identities.
    pub selected_plan_cardinality_estimates: Vec<OperatorCardinalityEstimate>,
    pub selected_plan_operator_counts: BTreeMap<String, usize>,
    pub selected_plan_class_counts: BTreeMap<String, usize>,
    pub warnings: Vec<String>,
    pub decisions: Vec<String>,
    pub rule_events: Vec<RuleEvent>,
    pub stage_events: Vec<StageTrace>,
}

/// The optimizer's output-cardinality estimate for one physical operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperatorCardinalityEstimate {
    pub operator_id: PhysicalOperatorId,
    pub operator: PhysicalPlanKind,
    pub estimated_rows: u64,
}
