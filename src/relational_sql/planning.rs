use skein_optimizer::{
    PlanCostBreakdown, RelationalJoinEnumerationConfig, RelationalJoinEnumerationError,
    RelationalJoinRewriteError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalJoinPlanningStrategy {
    SyntaxOrder,
    InnerJoinMemo,
    InnerLeftJoinRewriteMemo,
}

impl RelationalJoinPlanningStrategy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SyntaxOrder => "syntax_order",
            Self::InnerJoinMemo => "inner_join_memo",
            Self::InnerLeftJoinRewriteMemo => "inner_left_join_rewrite_memo",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalJoinPlanningStatus {
    NotEligible,
    Selected,
    Fallback,
}

impl RelationalJoinPlanningStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotEligible => "not_eligible",
            Self::Selected => "selected",
            Self::Fallback => "fallback",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalJoinPlanningReason {
    NoJoin,
    UnsupportedJoinKind,
    LockingSelect,
    WildcardProjection,
    UnstableOutputOrder,
    UnresolvedColumns,
    UnsupportedJoinPredicate,
    UnavailableAccessBinding,
    UnsupportedPostJoinFilter,
    InvalidJoinTree,
    GroupBudgetExceeded,
    ExpressionBudgetExceeded,
    DisconnectedGraph,
    RequiredPropertiesUnsatisfied,
    NoLegalPlan,
    InvalidJoinProblem,
    SyntaxOrderOptimal,
    CostReordered,
}

impl RelationalJoinPlanningReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoJoin => "no_join",
            Self::UnsupportedJoinKind => "unsupported_join_kind",
            Self::LockingSelect => "locking_select",
            Self::WildcardProjection => "wildcard_projection",
            Self::UnstableOutputOrder => "unstable_output_order",
            Self::UnresolvedColumns => "unresolved_columns",
            Self::UnsupportedJoinPredicate => "unsupported_join_predicate",
            Self::UnavailableAccessBinding => "unavailable_access_binding",
            Self::UnsupportedPostJoinFilter => "unsupported_post_join_filter",
            Self::InvalidJoinTree => "invalid_join_tree",
            Self::GroupBudgetExceeded => "group_budget_exceeded",
            Self::ExpressionBudgetExceeded => "expression_budget_exceeded",
            Self::DisconnectedGraph => "disconnected_graph",
            Self::RequiredPropertiesUnsatisfied => "required_properties_unsatisfied",
            Self::NoLegalPlan => "no_legal_plan",
            Self::InvalidJoinProblem => "invalid_join_problem",
            Self::SyntaxOrderOptimal => "syntax_order_optimal",
            Self::CostReordered => "cost_reordered",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalJoinPlanningBudget {
    pub max_groups: usize,
    pub max_expressions: usize,
}

impl From<RelationalJoinEnumerationConfig> for RelationalJoinPlanningBudget {
    fn from(config: RelationalJoinEnumerationConfig) -> Self {
        Self {
            max_groups: config.max_groups,
            max_expressions: config.max_expressions,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalJoinPlanningCost {
    pub estimated_rows: u64,
    pub cost: u64,
    pub cpu: u64,
    pub random_io: u64,
    pub sequential_io: u64,
    pub output_rows: u64,
}

impl From<PlanCostBreakdown> for RelationalJoinPlanningCost {
    fn from(cost: PlanCostBreakdown) -> Self {
        Self {
            estimated_rows: cost.estimated_rows,
            cost: cost.cost,
            cpu: cost.cpu,
            random_io: cost.random_io,
            sequential_io: cost.sequential_io,
            output_rows: cost.output_rows,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalJoinPlanningOutcome {
    pub strategy: RelationalJoinPlanningStrategy,
    pub status: RelationalJoinPlanningStatus,
    pub reason: RelationalJoinPlanningReason,
    pub memo_groups: Option<usize>,
    pub memo_expressions: Option<usize>,
    pub budget: RelationalJoinPlanningBudget,
    pub selected_order: Vec<String>,
    pub cost: Option<RelationalJoinPlanningCost>,
}

impl RelationalJoinPlanningOutcome {
    pub(crate) fn not_eligible(
        reason: RelationalJoinPlanningReason,
        selected_order: Vec<String>,
        config: RelationalJoinEnumerationConfig,
    ) -> Self {
        Self {
            strategy: RelationalJoinPlanningStrategy::SyntaxOrder,
            status: RelationalJoinPlanningStatus::NotEligible,
            reason,
            memo_groups: None,
            memo_expressions: None,
            budget: config.into(),
            selected_order,
            cost: None,
        }
    }

    pub(crate) fn selected(
        strategy: RelationalJoinPlanningStrategy,
        reordered: bool,
        memo_groups: usize,
        memo_expressions: usize,
        selected_order: Vec<String>,
        cost: PlanCostBreakdown,
        config: RelationalJoinEnumerationConfig,
    ) -> Self {
        Self {
            strategy,
            status: RelationalJoinPlanningStatus::Selected,
            reason: if reordered {
                RelationalJoinPlanningReason::CostReordered
            } else {
                RelationalJoinPlanningReason::SyntaxOrderOptimal
            },
            memo_groups: Some(memo_groups),
            memo_expressions: Some(memo_expressions),
            budget: config.into(),
            selected_order,
            cost: Some(cost.into()),
        }
    }

    pub(crate) fn fallback_from_enumeration(
        strategy: RelationalJoinPlanningStrategy,
        error: &RelationalJoinEnumerationError,
        selected_order: Vec<String>,
        config: RelationalJoinEnumerationConfig,
    ) -> Self {
        let (reason, memo_groups, memo_expressions) = match error {
            RelationalJoinEnumerationError::GroupBudgetExceeded {
                required_groups, ..
            } => (
                RelationalJoinPlanningReason::GroupBudgetExceeded,
                Some(*required_groups),
                None,
            ),
            RelationalJoinEnumerationError::ExpressionBudgetExceeded {
                required_expressions,
                ..
            } => (
                RelationalJoinPlanningReason::ExpressionBudgetExceeded,
                None,
                Some(*required_expressions),
            ),
            RelationalJoinEnumerationError::Disconnected => {
                (RelationalJoinPlanningReason::DisconnectedGraph, None, None)
            }
            RelationalJoinEnumerationError::RequiredPropertiesUnsatisfied => (
                RelationalJoinPlanningReason::RequiredPropertiesUnsatisfied,
                None,
                None,
            ),
            _ => (RelationalJoinPlanningReason::InvalidJoinProblem, None, None),
        };
        Self::fallback(
            strategy,
            reason,
            memo_groups,
            memo_expressions,
            selected_order,
            config,
        )
    }

    pub(crate) fn fallback_from_rewrite(
        error: &RelationalJoinRewriteError,
        selected_order: Vec<String>,
        config: RelationalJoinEnumerationConfig,
    ) -> Self {
        if let RelationalJoinRewriteError::Enumeration(error) = error {
            return Self::fallback_from_enumeration(
                RelationalJoinPlanningStrategy::InnerLeftJoinRewriteMemo,
                error,
                selected_order,
                config,
            );
        }
        let reason = match error {
            RelationalJoinRewriteError::NoLegalRewrite => RelationalJoinPlanningReason::NoLegalPlan,
            _ => RelationalJoinPlanningReason::InvalidJoinProblem,
        };
        Self::fallback(
            RelationalJoinPlanningStrategy::InnerLeftJoinRewriteMemo,
            reason,
            None,
            None,
            selected_order,
            config,
        )
    }

    fn fallback(
        strategy: RelationalJoinPlanningStrategy,
        reason: RelationalJoinPlanningReason,
        memo_groups: Option<usize>,
        memo_expressions: Option<usize>,
        selected_order: Vec<String>,
        config: RelationalJoinEnumerationConfig,
    ) -> Self {
        Self {
            strategy,
            status: RelationalJoinPlanningStatus::Fallback,
            reason,
            memo_groups,
            memo_expressions,
            budget: config.into(),
            selected_order,
            cost: None,
        }
    }

    pub fn join_order_reordered(&self) -> bool {
        self.status == RelationalJoinPlanningStatus::Selected
            && self.reason == RelationalJoinPlanningReason::CostReordered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planning_enums_expose_stable_diagnostic_names() {
        assert_eq!(
            RelationalJoinPlanningStrategy::InnerJoinMemo.as_str(),
            "inner_join_memo"
        );
        assert_eq!(RelationalJoinPlanningStatus::Fallback.as_str(), "fallback");
        assert_eq!(
            RelationalJoinPlanningReason::ExpressionBudgetExceeded.as_str(),
            "expression_budget_exceeded"
        );
    }

    #[test]
    fn budget_failure_retains_required_and_configured_counts() {
        let config = RelationalJoinEnumerationConfig {
            max_groups: 8,
            max_expressions: 16,
        };
        let outcome = RelationalJoinPlanningOutcome::fallback_from_enumeration(
            RelationalJoinPlanningStrategy::InnerJoinMemo,
            &RelationalJoinEnumerationError::ExpressionBudgetExceeded {
                required_expressions: 17,
                max_expressions: 16,
            },
            vec!["a".to_string(), "b".to_string()],
            config,
        );

        assert_eq!(outcome.status, RelationalJoinPlanningStatus::Fallback);
        assert_eq!(outcome.memo_expressions, Some(17));
        assert_eq!(outcome.budget.max_expressions, 16);
        assert!(!outcome.join_order_reordered());
    }
}
