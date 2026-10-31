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

use crate::cost::{PlanCost, PlanCostBreakdown};
use crate::properties::PhysicalProperties;
use crate::search::{RuleEvent, SearchMode};
use crate::stage::StageTrace;
use hawdb_plan_cypher::{PhysicalOperatorId, PhysicalPlanKind};
use std::collections::BTreeMap;

pub use hawdb_cascades::OptimizerConfig;

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

impl hawdb_core::RetainedHeapBytes for OptimizerTrace {
    fn retained_heap_bytes(&self) -> Option<usize> {
        let Self {
            groups,
            search_mode,
            query_digest,
            selected_plan,
            selected_plan_fingerprint,
            selected_plan_cost,
            selected_plan_cost_breakdown,
            selected_plan_properties,
            selected_plan_cardinality_estimates,
            selected_plan_operator_counts,
            selected_plan_class_counts,
            warnings,
            decisions,
            rule_events,
            stage_events,
        } = self;
        fn leaf<T: Copy>(_: &T) {}
        leaf(groups);
        leaf(search_mode);
        leaf(selected_plan_cost);
        leaf(selected_plan_cost_breakdown);
        fn vector<T>(values: &Vec<T>, heap: impl Fn(&T) -> Option<usize>) -> Option<usize> {
            values.iter().try_fold(
                values.capacity().checked_mul(std::mem::size_of::<T>())?,
                |bytes, value| bytes.checked_add(heap(value)?),
            )
        }
        hawdb_core::checked_retained_heap_sum([
            query_digest.retained_heap_bytes(),
            selected_plan.retained_heap_bytes(),
            selected_plan_fingerprint.retained_heap_bytes(),
            selected_plan_properties.retained_heap_bytes(),
            vector(selected_plan_cardinality_estimates, |estimate| {
                leaf(estimate);
                Some(0)
            }),
            selected_plan_operator_counts.retained_heap_bytes(),
            selected_plan_class_counts.retained_heap_bytes(),
            warnings.retained_heap_bytes(),
            decisions.retained_heap_bytes(),
            vector(rule_events, RuleEvent::retained_heap_bytes),
            vector(stage_events, StageTrace::retained_heap_bytes),
        ])
    }
}
