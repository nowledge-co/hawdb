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

use crate::search::{RuleEvent, RuleOutcome};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuleKind {
    Implementation,
    Exploration,
    Transformation,
    Enforcer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuleId {
    name: &'static str,
    kind: RuleKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RulePromise {
    priority: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleApplication<E> {
    expression: E,
    detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedRule<E> {
    id: RuleId,
    application: RuleApplication<E>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleBatch<E> {
    expressions: Vec<AppliedRule<E>>,
    events: Vec<RuleEvent>,
}

pub trait OptimizerRule<E> {
    fn id(&self) -> RuleId;

    fn promise(&self, expression: &E) -> RulePromise;

    fn apply(&self, expression: &E) -> Option<RuleApplication<E>>;
}

impl RuleKind {
    pub fn all() -> &'static [Self] {
        const ALL: &[RuleKind] = &[
            RuleKind::Implementation,
            RuleKind::Exploration,
            RuleKind::Transformation,
            RuleKind::Enforcer,
        ];
        ALL
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RuleKind::Implementation => "implementation",
            RuleKind::Exploration => "exploration",
            RuleKind::Transformation => "transformation",
            RuleKind::Enforcer => "enforcer",
        }
    }
}

impl FromStr for RuleKind {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::all()
            .iter()
            .copied()
            .find(|kind| kind.as_str() == value)
            .ok_or("unknown optimizer rule kind")
    }
}

impl RuleId {
    pub const fn new(name: &'static str, kind: RuleKind) -> Self {
        Self { name, kind }
    }

    pub fn name(self) -> &'static str {
        self.name
    }

    pub fn kind(self) -> RuleKind {
        self.kind
    }

    pub fn stable_name(self) -> String {
        format!("{}:{}", self.kind.as_str(), self.name)
    }
}

impl RulePromise {
    pub const NEVER: Self = Self { priority: 0 };

    pub const fn new(priority: u16) -> Self {
        Self { priority }
    }

    pub fn priority(self) -> u16 {
        self.priority
    }

    pub fn should_apply(self) -> bool {
        self.priority > 0
    }
}

impl<E> RuleApplication<E> {
    pub fn new(expression: E, detail: impl Into<String>) -> Self {
        Self {
            expression,
            detail: detail.into(),
        }
    }

    pub fn expression(&self) -> &E {
        &self.expression
    }

    pub fn into_expression(self) -> E {
        self.expression
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl<E> AppliedRule<E> {
    pub fn id(&self) -> RuleId {
        self.id
    }

    pub fn application(&self) -> &RuleApplication<E> {
        &self.application
    }

    pub fn into_application(self) -> RuleApplication<E> {
        self.application
    }
}

impl<E> RuleBatch<E> {
    pub fn expressions(&self) -> &[AppliedRule<E>] {
        &self.expressions
    }

    pub fn events(&self) -> &[RuleEvent] {
        &self.events
    }

    pub fn into_parts(self) -> (Vec<AppliedRule<E>>, Vec<RuleEvent>) {
        (self.expressions, self.events)
    }
}

pub fn apply_rule_batch<E>(expression: &E, rules: &[&dyn OptimizerRule<E>]) -> RuleBatch<E> {
    let mut candidates = rules
        .iter()
        .enumerate()
        .filter_map(|(index, rule)| {
            let promise = rule.promise(expression);
            promise
                .should_apply()
                .then(|| (index, rule.id(), promise, *rule))
        })
        .collect::<Vec<_>>();

    candidates.sort_by(
        |(left_index, left_id, left_promise, _), (right_index, right_id, right_promise, _)| {
            right_promise
                .priority()
                .cmp(&left_promise.priority())
                .then_with(|| left_id.stable_name().cmp(&right_id.stable_name()))
                .then_with(|| left_index.cmp(right_index))
        },
    );

    let mut expressions = Vec::new();
    let mut events = Vec::new();

    for (_, id, promise, rule) in candidates {
        match rule.apply(expression) {
            Some(application) => {
                events.push(RuleEvent::new(
                    id.stable_name(),
                    RuleOutcome::Applied,
                    format!("priority={} {}", promise.priority(), application.detail()),
                ));
                expressions.push(AppliedRule { id, application });
            }
            None => events.push(RuleEvent::skipped(
                id.stable_name(),
                format!("priority={} no expression produced", promise.priority()),
            )),
        }
    }

    RuleBatch {
        expressions,
        events,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_rule_batch, OptimizerRule, RuleApplication, RuleId, RuleKind, RuleOutcome,
        RulePromise,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestExpr(&'static str);

    struct RenameRule {
        id: RuleId,
        priority: u16,
        input: &'static str,
        output: Option<&'static str>,
    }

    impl OptimizerRule<TestExpr> for RenameRule {
        fn id(&self) -> RuleId {
            self.id
        }

        fn promise(&self, expression: &TestExpr) -> RulePromise {
            if expression.0 == self.input {
                RulePromise::new(self.priority)
            } else {
                RulePromise::NEVER
            }
        }

        fn apply(&self, _expression: &TestExpr) -> Option<RuleApplication<TestExpr>> {
            self.output
                .map(|output| RuleApplication::new(TestExpr(output), format!("to={output}")))
        }
    }

    #[test]
    fn rule_identity_exposes_stable_names() {
        let id = RuleId::new("scan_to_seek", RuleKind::Implementation);

        assert_eq!(id.name(), "scan_to_seek");
        assert_eq!(id.kind(), RuleKind::Implementation);
        assert_eq!(id.stable_name(), "implementation:scan_to_seek");
    }

    #[test]
    fn rule_kind_strings_round_trip_for_diagnostics() {
        for kind in RuleKind::all() {
            assert_eq!(kind.as_str().parse::<RuleKind>(), Ok(*kind));
        }
        assert!("unknown".parse::<RuleKind>().is_err());
    }

    #[test]
    fn rule_promise_zero_means_never_apply() {
        assert!(!RulePromise::NEVER.should_apply());
        assert!(RulePromise::new(1).should_apply());
        assert_eq!(RulePromise::new(7).priority(), 7);
    }

    #[test]
    fn rule_batch_orders_by_promise_then_stable_rule_name() {
        let low = RenameRule {
            id: RuleId::new("low", RuleKind::Transformation),
            priority: 1,
            input: "scan",
            output: Some("low"),
        };
        let high_b = RenameRule {
            id: RuleId::new("b", RuleKind::Implementation),
            priority: 9,
            input: "scan",
            output: Some("high_b"),
        };
        let high_a = RenameRule {
            id: RuleId::new("a", RuleKind::Implementation),
            priority: 9,
            input: "scan",
            output: Some("high_a"),
        };
        let batch = apply_rule_batch(&TestExpr("scan"), &[&low, &high_b, &high_a]);

        let expressions = batch.expressions();
        assert_eq!(expressions.len(), 3);
        assert_eq!(expressions[0].id().stable_name(), "implementation:a");
        assert_eq!(expressions[1].id().stable_name(), "implementation:b");
        assert_eq!(expressions[2].id().stable_name(), "transformation:low");
        assert_eq!(
            expressions[0].application().expression(),
            &TestExpr("high_a")
        );
        assert_eq!(batch.events()[0].outcome(), RuleOutcome::Applied);
        assert_eq!(batch.events()[0].detail(), "priority=9 to=high_a");
    }

    #[test]
    fn rule_batch_records_skipped_promising_rule_without_output() {
        let skipped = RenameRule {
            id: RuleId::new("no_output", RuleKind::Exploration),
            priority: 3,
            input: "scan",
            output: None,
        };
        let unrelated = RenameRule {
            id: RuleId::new("unrelated", RuleKind::Exploration),
            priority: 8,
            input: "filter",
            output: Some("should_not_apply"),
        };
        let batch = apply_rule_batch(&TestExpr("scan"), &[&skipped, &unrelated]);

        assert!(batch.expressions().is_empty());
        assert_eq!(batch.events().len(), 1);
        assert_eq!(batch.events()[0].rule(), "exploration:no_output");
        assert_eq!(batch.events()[0].outcome(), RuleOutcome::Skipped);
        assert_eq!(
            batch.events()[0].detail(),
            "priority=3 no expression produced"
        );
    }
}
