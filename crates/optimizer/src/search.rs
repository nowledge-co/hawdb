use crate::cost::{PlanCost, PlanCostBreakdown};
use crate::properties::PhysicalProperties;
use crate::stage::StageTrace;
use crate::trace::{OperatorCardinalityEstimate, OptimizerTrace};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Memo,
    /// Direct child resolution without allocating graph memo groups. The graph
    /// lowerer considers the same physical alternatives in both modes; this
    /// legacy name does not imply reduced plan quality.
    DirectFallback,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerSearchDirective {
    #[default]
    Auto,
    Memo,
    DirectFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerSearchDirectiveError {
    MemoGroupBudgetExceeded {
        required_groups: usize,
        max_groups: usize,
    },
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleEvent {
    rule: String,
    outcome: RuleOutcome,
    detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleOutcome {
    Applied,
    Skipped,
    Estimated,
    Selected,
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

impl RuleEvent {
    pub fn applied(rule: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new(rule, RuleOutcome::Applied, detail)
    }

    pub fn skipped(rule: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new(rule, RuleOutcome::Skipped, detail)
    }

    pub fn estimated(rule: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new(rule, RuleOutcome::Estimated, detail)
    }

    pub fn selected(rule: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new(rule, RuleOutcome::Selected, detail)
    }

    pub fn new(rule: impl Into<String>, outcome: RuleOutcome, detail: impl Into<String>) -> Self {
        Self {
            rule: rule.into(),
            outcome,
            detail: detail.into(),
        }
    }

    pub fn rule(&self) -> &str {
        &self.rule
    }

    pub fn outcome(&self) -> RuleOutcome {
        self.outcome
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn into_decision(self) -> String {
        format!("{} {}: {}", self.outcome.as_str(), self.rule, self.detail)
    }

    fn from_decision(decision: &str) -> Option<Self> {
        let (outcome, rest) = decision.split_once(' ')?;
        let (rule, detail) = rest.split_once(": ")?;
        Some(Self::new(
            rule,
            outcome.parse::<RuleOutcome>().ok()?,
            detail,
        ))
    }
}

impl SearchMode {
    pub fn all() -> &'static [Self] {
        const ALL: &[SearchMode] = &[SearchMode::Memo, SearchMode::DirectFallback];
        ALL
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SearchMode::Memo => "memo",
            SearchMode::DirectFallback => "direct_fallback",
        }
    }
}

impl OptimizerSearchDirective {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Memo => "memo",
            Self::DirectFallback => "direct_fallback",
        }
    }
}

impl FromStr for OptimizerSearchDirective {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "auto" => Ok(Self::Auto),
            "memo" => Ok(Self::Memo),
            "direct_fallback" => Ok(Self::DirectFallback),
            _ => Err("unknown optimizer search directive"),
        }
    }
}

impl Display for OptimizerSearchDirectiveError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoGroupBudgetExceeded {
                required_groups,
                max_groups,
            } => write!(
                formatter,
                "memo search directive requires {required_groups} groups but max_groups is {max_groups}"
            ),
        }
    }
}

impl std::error::Error for OptimizerSearchDirectiveError {}

impl RuleOutcome {
    pub fn all() -> &'static [Self] {
        const ALL: &[RuleOutcome] = &[
            RuleOutcome::Applied,
            RuleOutcome::Skipped,
            RuleOutcome::Estimated,
            RuleOutcome::Selected,
        ];
        ALL
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RuleOutcome::Applied => "apply",
            RuleOutcome::Skipped => "skip",
            RuleOutcome::Estimated => "estimate",
            RuleOutcome::Selected => "selected",
        }
    }
}

impl FromStr for SearchMode {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::all()
            .iter()
            .copied()
            .find(|mode| mode.as_str() == value)
            .ok_or("unknown optimizer search mode")
    }
}

impl FromStr for RuleOutcome {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::all()
            .iter()
            .copied()
            .find(|outcome| outcome.as_str() == value)
            .ok_or("unknown optimizer rule outcome")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OptimizationSearchReport, OptimizerSearchDirective, RuleEvent, RuleOutcome, SearchMode,
        SelectedPlanTrace,
    };
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
    fn optimizer_search_directive_strings_round_trip() {
        for directive in [
            OptimizerSearchDirective::Auto,
            OptimizerSearchDirective::Memo,
            OptimizerSearchDirective::DirectFallback,
        ] {
            assert_eq!(
                directive.as_str().parse::<OptimizerSearchDirective>(),
                Ok(directive)
            );
        }
        assert!("unknown".parse::<OptimizerSearchDirective>().is_err());
    }

    #[test]
    fn search_mode_strings_round_trip_for_diagnostics() {
        for mode in SearchMode::all() {
            assert_eq!(mode.as_str().parse::<SearchMode>(), Ok(*mode));
        }
        assert!("unknown".parse::<SearchMode>().is_err());
    }

    #[test]
    fn rule_event_formats_stable_decision_text() {
        let event = RuleEvent::estimated("index_seek", "rows=1 cost=3");

        assert_eq!(event.rule(), "index_seek");
        assert_eq!(event.outcome(), RuleOutcome::Estimated);
        assert_eq!(event.detail(), "rows=1 cost=3");
        assert_eq!(event.into_decision(), "estimate index_seek: rows=1 cost=3");
    }

    #[test]
    fn rule_outcome_strings_round_trip_for_diagnostics() {
        for outcome in RuleOutcome::all() {
            assert_eq!(outcome.as_str().parse::<RuleOutcome>(), Ok(*outcome));
        }
        assert!("unknown".parse::<RuleOutcome>().is_err());
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
}
