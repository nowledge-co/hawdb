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

use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Memo,
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

    pub fn from_decision(decision: &str) -> Option<Self> {
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
    use super::{OptimizerSearchDirective, RuleEvent, RuleOutcome, SearchMode};

    #[test]
    fn search_directive_strings_round_trip() {
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
    fn search_mode_and_rule_outcome_strings_round_trip() {
        for mode in SearchMode::all() {
            assert_eq!(mode.as_str().parse::<SearchMode>(), Ok(*mode));
        }
        assert!("unknown".parse::<SearchMode>().is_err());
        for outcome in RuleOutcome::all() {
            assert_eq!(outcome.as_str().parse::<RuleOutcome>(), Ok(*outcome));
        }
        assert!("unknown".parse::<RuleOutcome>().is_err());
    }

    #[test]
    fn rule_event_round_trips_through_legacy_decision_text() {
        let event = RuleEvent::estimated("index_seek", "rows=1 cost=3");

        assert_eq!(event.into_decision(), "estimate index_seek: rows=1 cost=3");
        let parsed = RuleEvent::from_decision("estimate index_seek: rows=1 cost=3").unwrap();
        assert_eq!(parsed.rule(), "index_seek");
        assert_eq!(parsed.outcome(), RuleOutcome::Estimated);
        assert_eq!(parsed.detail(), "rows=1 cost=3");
    }
}
