// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded, statement-scoped optimizer qualification controls. Legality and
//! resource checks are not optional rules and cannot be disabled here.

/// Optional proof transformations supported by the qualification interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OptionalOptimizerRule {
    RedundantOrdering,
    RedundantDistinct,
    SingletonGroup,
}

impl OptionalOptimizerRule {
    pub const ALL: [Self; 3] = [
        Self::RedundantOrdering,
        Self::RedundantDistinct,
        Self::SingletonGroup,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RedundantOrdering => "transformation:proof_redundant_ordering",
            Self::RedundantDistinct => "transformation:proof_redundant_distinct",
            Self::SingletonGroup => "transformation:proof_singleton_group",
        }
    }

    const fn bit(self) -> u8 {
        1 << self as u8
    }
}

/// Reproducible ranking noise. It never changes a raw reported cost, a search
/// budget, an expression, or whether a candidate is legal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptimizerCostPerturbation {
    seed: u64,
    percent: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidOptimizerCostPerturbation {
    percent: u8,
}

impl std::fmt::Display for InvalidOptimizerCostPerturbation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "optimizer cost perturbation must be at most 25 percent, got {}",
            self.percent
        )
    }
}
impl std::error::Error for InvalidOptimizerCostPerturbation {}

impl OptimizerCostPerturbation {
    pub fn new(seed: u64, percent: u8) -> Result<Self, InvalidOptimizerCostPerturbation> {
        if percent > 25 {
            return Err(InvalidOptimizerCostPerturbation { percent });
        }
        Ok(Self { seed, percent })
    }
    pub const fn seed(self) -> u64 {
        self.seed
    }
    pub const fn percent(self) -> u8 {
        self.percent
    }

    /// A stable byte hash makes replay independent of process hash randomization
    /// and host pointer width. Caller supplies the legal candidate's fingerprint.
    pub fn ranking_cost(self, raw_cost: u64, fingerprint: &str) -> u64 {
        if self.percent == 0 || raw_cost == 0 {
            return raw_cost;
        }
        let mut hash = 0xcbf2_9ce4_8422_2325u64 ^ self.seed;
        for byte in fingerprint.bytes() {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3);
        }
        let spread = u64::from(self.percent) * 2 + 1;
        let factor = 100 - u64::from(self.percent) + hash % spread;
        ((u128::from(raw_cost) * u128::from(factor) / 100).min(u128::from(u64::MAX)) as u64).max(1)
    }
}

/// Fixed-size controls; unknown/essential rules cannot enter the disabled mask.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OptimizerQualificationOptions {
    disabled_rules: u8,
    perturbation: Option<OptimizerCostPerturbation>,
}

impl OptimizerQualificationOptions {
    pub fn without_rule(mut self, rule: OptionalOptimizerRule) -> Self {
        self.disabled_rules |= rule.bit();
        self
    }
    pub fn with_cost_perturbation(mut self, perturbation: OptimizerCostPerturbation) -> Self {
        self.perturbation = Some(perturbation);
        self
    }
    pub const fn rule_enabled(self, rule: OptionalOptimizerRule) -> bool {
        self.disabled_rules & rule.bit() == 0
    }
    pub const fn cost_perturbation(self) -> Option<OptimizerCostPerturbation> {
        self.perturbation
    }
    pub fn is_active(self) -> bool {
        self != Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn qualification_is_bounded_and_cannot_disable_unlisted_legality_checks() {
        let normal = OptimizerQualificationOptions::default();
        assert!(!normal.is_active());
        for rule in OptionalOptimizerRule::ALL {
            assert!(normal.rule_enabled(rule));
        }
        let qualified = normal.without_rule(OptionalOptimizerRule::RedundantDistinct);
        assert!(qualified.is_active());
        assert!(qualified.rule_enabled(OptionalOptimizerRule::RedundantOrdering));
        assert!(!qualified.rule_enabled(OptionalOptimizerRule::RedundantDistinct));
        assert!(OptimizerCostPerturbation::new(7, 26).is_err());
    }
    #[test]
    fn perturbation_replays_with_bounded_saturating_costs() {
        for percent in [0, 1, 25] {
            for raw in [0, 1, 1000, u64::MAX] {
                for seed in 0..64 {
                    let p = OptimizerCostPerturbation::new(seed, percent).unwrap();
                    let ranked = p.ranking_cost(raw, "SortExec:stable-shape");
                    assert_eq!(ranked, p.ranking_cost(raw, "SortExec:stable-shape"));
                    let low = (u128::from(raw) * u128::from(100 - percent) / 100)
                        .max(u128::from(raw != 0));
                    let high = (u128::from(raw) * u128::from(100 + percent) / 100)
                        .min(u128::from(u64::MAX));
                    assert!(
                        u128::from(ranked) >= low && u128::from(ranked) <= high.max(1),
                        "seed={seed} raw={raw} percent={percent}"
                    );
                    if percent == 0 || raw == 0 {
                        assert_eq!(ranked, raw);
                    }
                }
            }
        }
        let costs = (0..64)
            .map(|seed| {
                OptimizerCostPerturbation::new(seed, 25)
                    .unwrap()
                    .ranking_cost(1000, "TopNExec:stable-shape")
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            costs.len() > 20,
            "seeds must exercise materially distinct ranking weights"
        );
    }

    #[test]
    fn rule_assertions_use_applied_events_and_bound_the_request() {
        let rule = OptionalOptimizerRule::RedundantDistinct;
        let events = [
            crate::RuleEvent::skipped(rule.as_str(), "candidate retained"),
            crate::RuleEvent::selected(rule.as_str(), "diagnostic mentions rule"),
        ];
        assert_eq!(
            verify_optimizer_rule_assertions(&events, &[OptimizerRuleAssertion::Required(rule)]),
            Err(OptimizerRuleAssertionError::MissingRequired(rule))
        );
        assert!(verify_optimizer_rule_assertions(
            &events,
            &[OptimizerRuleAssertion::Forbidden(rule)]
        )
        .is_ok());
        let events = [crate::RuleEvent::applied(rule.as_str(), "removed")];
        assert!(verify_optimizer_rule_assertions(
            &events,
            &[OptimizerRuleAssertion::Required(rule)]
        )
        .is_ok());
        assert_eq!(
            verify_optimizer_rule_assertions(&events, &[OptimizerRuleAssertion::Forbidden(rule)]),
            Err(OptimizerRuleAssertionError::ForbiddenApplied(rule))
        );
        assert_eq!(
            verify_optimizer_rule_assertions(&[], &[OptimizerRuleAssertion::Required(rule); 7]),
            Err(OptimizerRuleAssertionError::TooManyAssertions)
        );
    }
}

/// Assertions inspect structured applied outcomes, not mentions in plan text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerRuleAssertion {
    Required(OptionalOptimizerRule),
    Forbidden(OptionalOptimizerRule),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerRuleAssertionError {
    TooManyAssertions,
    MissingRequired(OptionalOptimizerRule),
    ForbiddenApplied(OptionalOptimizerRule),
}
impl std::fmt::Display for OptimizerRuleAssertionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyAssertions => {
                f.write_str("optimizer qualification accepts at most six rule assertions")
            }
            Self::MissingRequired(rule) => write!(
                f,
                "required optimizer rule did not apply: {}",
                rule.as_str()
            ),
            Self::ForbiddenApplied(rule) => {
                write!(f, "forbidden optimizer rule applied: {}", rule.as_str())
            }
        }
    }
}
impl std::error::Error for OptimizerRuleAssertionError {}

pub fn verify_optimizer_rule_assertions(
    events: &[crate::RuleEvent],
    assertions: &[OptimizerRuleAssertion],
) -> Result<(), OptimizerRuleAssertionError> {
    if assertions.len() > OptionalOptimizerRule::ALL.len() * 2 {
        return Err(OptimizerRuleAssertionError::TooManyAssertions);
    }
    let mut applied = 0u8;
    for event in events {
        if event.outcome() != crate::RuleOutcome::Applied {
            continue;
        }
        if let Some(rule) = OptionalOptimizerRule::ALL
            .iter()
            .find(|rule| rule.as_str() == event.rule())
        {
            applied |= rule.bit();
        }
    }
    for assertion in assertions {
        match *assertion {
            OptimizerRuleAssertion::Required(rule) if applied & rule.bit() == 0 => {
                return Err(OptimizerRuleAssertionError::MissingRequired(rule))
            }
            OptimizerRuleAssertion::Forbidden(rule) if applied & rule.bit() != 0 => {
                return Err(OptimizerRuleAssertionError::ForbiddenApplied(rule))
            }
            _ => {}
        }
    }
    Ok(())
}
