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
use crate::stage::StageTrace;
use crate::trace::{OperatorCardinalityEstimate, OptimizerTrace};
use std::collections::BTreeMap;

pub use hawdb_cascades::{
    OptimizerSearchDirective, OptimizerSearchDirectiveError, RuleEvent, RuleOutcome, SearchMode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizationSearchReport {
    groups: usize,
    mode: SearchMode,
    warnings: Vec<String>,
    decisions: Vec<String>,
    rule_events: Vec<RuleEvent>,
    stage_events: Vec<StageTrace>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedPlanTrace {
    pub query_digest: Option<String>,
    pub explain: String,
    pub fingerprint: String,
    pub cost: PlanCost,
    pub cost_breakdown: PlanCostBreakdown,
    pub properties: PhysicalProperties,
    pub cardinality_estimates: Vec<OperatorCardinalityEstimate>,
    pub operator_counts: BTreeMap<String, usize>,
    pub class_counts: BTreeMap<String, usize>,
}

impl OptimizationSearchReport {
    pub fn memo(groups: usize) -> Self {
        Self {
            groups,
            mode: SearchMode::Memo,
            warnings: Vec::new(),
            decisions: Vec::new(),
            rule_events: Vec::new(),
            stage_events: Vec::new(),
        }
    }

    pub fn direct_fallback(required_groups: usize, max_groups: usize) -> Self {
        let mut report = Self {
            groups: required_groups,
            mode: SearchMode::DirectFallback,
            warnings: Vec::new(),
            decisions: Vec::new(),
            rule_events: Vec::new(),
            stage_events: Vec::new(),
        };
        report.push_decision(format!(
            "selected direct child resolution: required_groups={required_groups} max_groups={max_groups}; same physical alternatives as memo lowering"
        ));
        report
    }

    pub fn forced_direct_fallback(required_groups: usize) -> Self {
        let mut report = Self {
            groups: required_groups,
            mode: SearchMode::DirectFallback,
            warnings: Vec::new(),
            decisions: Vec::new(),
            rule_events: Vec::new(),
            stage_events: Vec::new(),
        };
        report.push_decision(
            "selected direct physical fallback: explicit optimizer search directive",
        );
        report
    }

    pub fn groups(&self) -> usize {
        self.groups
    }

    pub fn mode(&self) -> SearchMode {
        self.mode
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    pub fn decisions(&self) -> &[String] {
        &self.decisions
    }

    pub fn rule_events(&self) -> &[RuleEvent] {
        &self.rule_events
    }

    pub fn stage_events(&self) -> &[StageTrace] {
        &self.stage_events
    }

    pub fn push_stage_event(&mut self, event: StageTrace) {
        self.stage_events.push(event);
    }

    pub(crate) fn push_warning(&mut self, warning: impl Into<String>) {
        self.warnings.push(warning.into());
    }

    pub fn push_decision(&mut self, decision: impl Into<String>) {
        let decision = decision.into();
        if let Some(event) = RuleEvent::from_decision(&decision) {
            self.rule_events.push(event);
        }
        self.decisions.push(decision);
    }

    pub fn extend_decisions(&mut self, decisions: impl IntoIterator<Item = String>) {
        for decision in decisions {
            self.push_decision(decision);
        }
    }

    pub fn push_rule_event(&mut self, event: RuleEvent) {
        self.decisions.push(event.clone().into_decision());
        self.rule_events.push(event);
    }

    pub fn record_selected_plan_cost(&mut self, cost: PlanCost) {
        let cost = cost.with_cardinality_floor();
        self.push_decision(format!(
            "selected physical plan cost: estimated_rows={} cost={}",
            cost.estimated_rows, cost.cost
        ));
    }

    pub fn into_trace(self, selected: SelectedPlanTrace) -> OptimizerTrace {
        OptimizerTrace {
            groups: self.groups,
            search_mode: self.mode,
            query_digest: selected.query_digest,
            selected_plan: selected.explain,
            selected_plan_fingerprint: selected.fingerprint,
            selected_plan_cost: selected.cost.with_cardinality_floor(),
            selected_plan_cost_breakdown: selected.cost_breakdown.with_cardinality_floor(),
            selected_plan_properties: selected.properties,
            selected_plan_cardinality_estimates: selected.cardinality_estimates,
            selected_plan_operator_counts: selected.operator_counts,
            selected_plan_class_counts: selected.class_counts,
            warnings: self.warnings,
            decisions: self.decisions,
            rule_events: self.rule_events,
            stage_events: self.stage_events,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OptimizationSearchReport, RuleOutcome, SearchMode, SelectedPlanTrace};
    use crate::{ApplyOrder, OptimizationStage, PlanCost, PlanCostBreakdown, StageStats};
    use std::collections::BTreeMap;

    #[test]
    fn direct_fallback_report_records_budget_decision_without_degradation() {
        let report = OptimizationSearchReport::direct_fallback(9, 4);

        assert_eq!(report.groups(), 9);
        assert_eq!(report.mode(), SearchMode::DirectFallback);
        assert!(report.warnings().is_empty());
        assert_eq!(report.decisions().len(), 1);
        assert!(report.decisions()[0].contains("required_groups=9 max_groups=4"));
        assert!(report.decisions()[0].contains("same physical alternatives"));
        assert_eq!(report.rule_events().len(), 1);
        assert!(report.stage_events().is_empty());
    }

    #[test]
    fn forced_direct_fallback_reports_directive_without_budget_warning() {
        let report = OptimizationSearchReport::forced_direct_fallback(9);

        assert_eq!(report.groups(), 9);
        assert_eq!(report.mode(), SearchMode::DirectFallback);
        assert!(report.warnings().is_empty());
        assert!(report
            .decisions()
            .iter()
            .any(|decision| decision.contains("explicit optimizer search directive")));
    }

    #[test]
    fn search_report_preserves_structured_rule_events_from_legacy_decisions() {
        let mut report = OptimizationSearchReport::memo(1);
        report.push_decision(
            "apply implementation:node_equality_index_seek: priority=100 property=id",
        );
        report.push_decision("choose IndexNodeSeek");

        assert_eq!(report.decisions().len(), 2);
        assert_eq!(report.rule_events().len(), 1);
        assert_eq!(
            report.rule_events()[0].rule(),
            "implementation:node_equality_index_seek"
        );
        assert_eq!(report.rule_events()[0].outcome(), RuleOutcome::Applied);
        assert_eq!(report.rule_events()[0].detail(), "priority=100 property=id");
    }

    #[test]
    fn search_report_builds_legacy_trace_surface() {
        let mut report = OptimizationSearchReport::memo(2);
        report.push_stage_event(
            OptimizationStage::new("physical_search", ApplyOrder::BottomUp)
                .trace(StageStats::new(2, 1).with_rule_counts(1, 0)),
        );
        report.push_decision("choose IndexNodeSeek");
        report.record_selected_plan_cost(PlanCost {
            estimated_rows: 1,
            cost: 4,
        });

        let trace = report.into_trace(SelectedPlanTrace {
            query_digest: None,
            explain: "IndexNodeSeek".to_string(),
            fingerprint: "IndexNodeSeek(1:m:6:Memory.id=i:1)".to_string(),
            cost: PlanCost {
                estimated_rows: 1,
                cost: 4,
            },
            cost_breakdown: PlanCostBreakdown::new(1, 1, 3, 0, 0),
            properties: Default::default(),
            cardinality_estimates: Vec::new(),
            operator_counts: BTreeMap::from([("IndexNodeSeek".to_string(), 1)]),
            class_counts: BTreeMap::from([("access".to_string(), 1)]),
        });

        assert_eq!(trace.groups, 2);
        assert_eq!(trace.search_mode, SearchMode::Memo);
        assert!(trace.warnings.is_empty());
        assert_eq!(trace.stage_events[0].name(), "physical_search");
        assert_eq!(trace.stage_events[0].apply_order(), ApplyOrder::BottomUp);
        assert_eq!(trace.stage_events[0].stats().applied_rules, 1);
        assert_eq!(trace.decisions[0], "choose IndexNodeSeek");
        assert_eq!(
            trace.decisions[1],
            "selected physical plan cost: estimated_rows=1 cost=4"
        );
        assert_eq!(trace.selected_plan_operator_counts["IndexNodeSeek"], 1);
        assert_eq!(trace.selected_plan_class_counts["access"], 1);
    }
}
