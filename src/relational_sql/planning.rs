use skein_optimizer::{
    PlanCostBreakdown, RelationalJoinEnumerationConfig, RelationalJoinEnumerationError,
    RelationalJoinRewriteError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalJoinPlanningStrategy {
    SyntaxOrder,
    InnerJoinMemo,
    InnerLeftJoinRewriteMemo,
    CsgCmpMemo,
}

impl RelationalJoinPlanningStrategy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SyntaxOrder => "syntax_order",
            Self::InnerJoinMemo => "inner_join_memo",
            Self::InnerLeftJoinRewriteMemo => "inner_left_join_rewrite_memo",
            Self::CsgCmpMemo => "csg_cmp_memo",
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
    GroupBudgetExceeded,
    ExpressionBudgetExceeded,
    DisconnectedGraph,
    RequiredPropertiesUnsatisfied,
    NoLegalPlan,
    SyntaxFallback,
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
            Self::GroupBudgetExceeded => "group_budget_exceeded",
            Self::ExpressionBudgetExceeded => "expression_budget_exceeded",
            Self::DisconnectedGraph => "disconnected_graph",
            Self::RequiredPropertiesUnsatisfied => "required_properties_unsatisfied",
            Self::NoLegalPlan => "no_legal_plan",
            Self::SyntaxFallback => "syntax_fallback",
            Self::SyntaxOrderOptimal => "syntax_order_optimal",
            Self::CostReordered => "cost_reordered",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalJoinPlanningFallbackClass {
    Budget,
    Unsupported,
}

impl RelationalJoinPlanningFallbackClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Budget => "budget",
            Self::Unsupported => "unsupported",
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
pub struct RelationalJoinPlanningAttempt {
    pub strategy: RelationalJoinPlanningStrategy,
    pub status: RelationalJoinPlanningStatus,
    pub reason: RelationalJoinPlanningReason,
    pub fallback_class: Option<RelationalJoinPlanningFallbackClass>,
    pub memo_groups: Option<usize>,
    pub memo_expressions: Option<usize>,
    pub cost: Option<RelationalJoinPlanningCost>,
}

impl RelationalJoinPlanningAttempt {
    pub(crate) fn not_eligible(
        strategy: RelationalJoinPlanningStrategy,
        reason: RelationalJoinPlanningReason,
    ) -> Self {
        Self {
            strategy,
            status: RelationalJoinPlanningStatus::NotEligible,
            reason,
            fallback_class: None,
            memo_groups: None,
            memo_expressions: None,
            cost: None,
        }
    }

    pub(crate) fn selected(
        strategy: RelationalJoinPlanningStrategy,
        reordered: bool,
        memo_groups: usize,
        memo_expressions: usize,
        cost: PlanCostBreakdown,
    ) -> Self {
        Self {
            strategy,
            status: RelationalJoinPlanningStatus::Selected,
            reason: if reordered {
                RelationalJoinPlanningReason::CostReordered
            } else {
                RelationalJoinPlanningReason::SyntaxOrderOptimal
            },
            fallback_class: None,
            memo_groups: Some(memo_groups),
            memo_expressions: Some(memo_expressions),
            cost: Some(cost.into()),
        }
    }

    pub(crate) fn syntax_fallback() -> Self {
        Self {
            strategy: RelationalJoinPlanningStrategy::SyntaxOrder,
            status: RelationalJoinPlanningStatus::Selected,
            reason: RelationalJoinPlanningReason::SyntaxFallback,
            fallback_class: None,
            memo_groups: None,
            memo_expressions: None,
            cost: None,
        }
    }

    pub(crate) fn fallback_from_enumeration(
        strategy: RelationalJoinPlanningStrategy,
        error: &RelationalJoinEnumerationError,
    ) -> Option<Self> {
        let (reason, fallback_class, memo_groups, memo_expressions) = match error {
            RelationalJoinEnumerationError::GroupBudgetExceeded {
                required_groups, ..
            } => (
                RelationalJoinPlanningReason::GroupBudgetExceeded,
                RelationalJoinPlanningFallbackClass::Budget,
                Some(*required_groups),
                None,
            ),
            RelationalJoinEnumerationError::ExpressionBudgetExceeded {
                required_expressions,
                ..
            } => (
                RelationalJoinPlanningReason::ExpressionBudgetExceeded,
                RelationalJoinPlanningFallbackClass::Budget,
                None,
                Some(*required_expressions),
            ),
            RelationalJoinEnumerationError::Disconnected => (
                RelationalJoinPlanningReason::DisconnectedGraph,
                RelationalJoinPlanningFallbackClass::Unsupported,
                None,
                None,
            ),
            RelationalJoinEnumerationError::RequiredPropertiesUnsatisfied => (
                RelationalJoinPlanningReason::RequiredPropertiesUnsatisfied,
                RelationalJoinPlanningFallbackClass::Unsupported,
                None,
                None,
            ),
            RelationalJoinEnumerationError::EmptyGraph
            | RelationalJoinEnumerationError::DuplicateBinding(_)
            | RelationalJoinEnumerationError::DuplicatePredicate(_)
            | RelationalJoinEnumerationError::PredicateHasFewerThanTwoBindings(_)
            | RelationalJoinEnumerationError::UnknownPredicateBinding { .. }
            | RelationalJoinEnumerationError::UnknownAccessBinding { .. }
            | RelationalJoinEnumerationError::AccessWithoutPredicate { .. }
            | RelationalJoinEnumerationError::SelfDependentAccess(_)
            | RelationalJoinEnumerationError::MissingBaseAccess(_)
            | RelationalJoinEnumerationError::InvalidAccessPath { .. } => return None,
        };
        Some(Self {
            strategy,
            status: RelationalJoinPlanningStatus::Fallback,
            reason,
            fallback_class: Some(fallback_class),
            memo_groups,
            memo_expressions,
            cost: None,
        })
    }

    pub(crate) fn fallback_from_rewrite(
        strategy: RelationalJoinPlanningStrategy,
        error: &RelationalJoinRewriteError,
    ) -> Option<Self> {
        match error {
            RelationalJoinRewriteError::Enumeration(error) => {
                Self::fallback_from_enumeration(strategy, error)
            }
            RelationalJoinRewriteError::NoLegalRewrite => Some(Self {
                strategy,
                status: RelationalJoinPlanningStatus::Fallback,
                reason: RelationalJoinPlanningReason::NoLegalPlan,
                fallback_class: Some(RelationalJoinPlanningFallbackClass::Unsupported),
                memo_groups: None,
                memo_expressions: None,
                cost: None,
            }),
            RelationalJoinRewriteError::DuplicateTreeBinding(_)
            | RelationalJoinRewriteError::DuplicateOperator(_)
            | RelationalJoinRewriteError::PredicateOutsideOperatorSubtree { .. }
            | RelationalJoinRewriteError::RelationSetMismatch
            | RelationalJoinRewriteError::OperatorCountMismatch { .. } => None,
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
    pub attempts: Vec<RelationalJoinPlanningAttempt>,
}

impl RelationalJoinPlanningOutcome {
    pub(crate) fn not_eligible(
        reason: RelationalJoinPlanningReason,
        selected_order: Vec<String>,
        config: RelationalJoinEnumerationConfig,
    ) -> Self {
        Self::not_eligible_after_attempts(reason, selected_order, config, Vec::new())
    }

    pub(crate) fn not_eligible_after_attempts(
        reason: RelationalJoinPlanningReason,
        selected_order: Vec<String>,
        config: RelationalJoinEnumerationConfig,
        mut attempts: Vec<RelationalJoinPlanningAttempt>,
    ) -> Self {
        let attempt = RelationalJoinPlanningAttempt::not_eligible(
            RelationalJoinPlanningStrategy::SyntaxOrder,
            reason,
        );
        attempts.push(attempt);
        Self {
            strategy: RelationalJoinPlanningStrategy::SyntaxOrder,
            status: RelationalJoinPlanningStatus::NotEligible,
            reason,
            memo_groups: None,
            memo_expressions: None,
            budget: config.into(),
            selected_order,
            cost: None,
            attempts,
        }
    }

    pub(crate) fn selected(
        attempt: RelationalJoinPlanningAttempt,
        selected_order: Vec<String>,
        config: RelationalJoinEnumerationConfig,
        mut attempts: Vec<RelationalJoinPlanningAttempt>,
    ) -> Self {
        debug_assert_eq!(attempt.status, RelationalJoinPlanningStatus::Selected);
        debug_assert!(attempt.fallback_class.is_none());
        let strategy = attempt.strategy;
        let reason = attempt.reason;
        let memo_groups = attempt.memo_groups;
        let memo_expressions = attempt.memo_expressions;
        let cost = attempt.cost;
        attempts.push(attempt);
        Self {
            strategy,
            status: RelationalJoinPlanningStatus::Selected,
            reason,
            memo_groups,
            memo_expressions,
            budget: config.into(),
            selected_order,
            cost,
            attempts,
        }
    }

    pub(crate) fn fallback_to_syntax(
        mut attempts: Vec<RelationalJoinPlanningAttempt>,
        selected_order: Vec<String>,
        config: RelationalJoinEnumerationConfig,
    ) -> Option<Self> {
        let failed = attempts
            .iter()
            .rev()
            .find(|attempt| attempt.status == RelationalJoinPlanningStatus::Fallback)
            .cloned()?;
        attempts.push(RelationalJoinPlanningAttempt::syntax_fallback());
        Some(Self {
            strategy: failed.strategy,
            status: RelationalJoinPlanningStatus::Fallback,
            reason: failed.reason,
            memo_groups: failed.memo_groups,
            memo_expressions: failed.memo_expressions,
            budget: config.into(),
            selected_order,
            cost: None,
            attempts,
        })
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
        assert_eq!(
            RelationalJoinPlanningStrategy::CsgCmpMemo.as_str(),
            "csg_cmp_memo"
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
        let attempt = RelationalJoinPlanningAttempt::fallback_from_enumeration(
            RelationalJoinPlanningStrategy::InnerJoinMemo,
            &RelationalJoinEnumerationError::ExpressionBudgetExceeded {
                required_expressions: 17,
                max_expressions: 16,
            },
        )
        .expect("budget exhaustion is fallback eligible");
        let outcome = RelationalJoinPlanningOutcome::fallback_to_syntax(
            vec![attempt],
            vec!["a".to_string(), "b".to_string()],
            config,
        )
        .expect("budget exhaustion produces a syntax fallback outcome");

        assert_eq!(outcome.status, RelationalJoinPlanningStatus::Fallback);
        assert_eq!(outcome.memo_expressions, Some(17));
        assert_eq!(outcome.budget.max_expressions, 16);
        assert_eq!(outcome.attempts.len(), 2);
        assert_eq!(
            outcome.attempts[0].fallback_class,
            Some(RelationalJoinPlanningFallbackClass::Budget)
        );
        assert_eq!(
            outcome.attempts[1].strategy,
            RelationalJoinPlanningStrategy::SyntaxOrder
        );
        assert!(!outcome.join_order_reordered());
    }

    #[test]
    fn invalid_join_graph_is_not_fallback_eligible() {
        let attempt = RelationalJoinPlanningAttempt::fallback_from_enumeration(
            RelationalJoinPlanningStrategy::CsgCmpMemo,
            &RelationalJoinEnumerationError::EmptyGraph,
        );
        assert!(attempt.is_none());

        let rewrite = RelationalJoinPlanningAttempt::fallback_from_rewrite(
            RelationalJoinPlanningStrategy::CsgCmpMemo,
            &RelationalJoinRewriteError::RelationSetMismatch,
        );
        assert!(rewrite.is_none());
    }
}
