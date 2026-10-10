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

use super::{ScoreFeature, ScoringEvaluation, ScoringFeatureSource, ScoringSpec, ScoringSpecError};
use std::fmt;

/// Composition of the ordered terms, before the shared decay factors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScoringCombination {
    /// `sum(weight * value)`, preserving the existing ScoringSpec arithmetic.
    WeightedSum,
    /// `product(value.powf(weight))`. A zero weight contributes one.
    /// Negative bases with fractional exponents fail with `NonFiniteScore`.
    WeightedProduct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MissingScoringFeature {
    /// Fail rather than silently ranking an incomplete set of declared signals.
    /// One missing signal fails the entire scored request, including OPTIONAL NULLs.
    Reject,
    /// Missing sum terms contribute zero; product terms and decays contribute one.
    /// A missing timestamp therefore receives no age penalty, like a future timestamp.
    Neutral,
}

/// Validated reusable scoring template with coefficients supplied by the host.
///
/// Unlike the legacy sum-only ScoringSpec, a program explicitly declares both
/// composition and missing-signal behavior. Time is an execution input, never
/// captured while constructing or caching a program.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoringProgram {
    combination: ScoringCombination,
    missing: MissingScoringFeature,
    spec: ScoringSpec,
}

/// Typed cache identity, deliberately excluding coefficients and request time.
/// Strings remain individual fields; punctuation cannot collide across terms.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScoringProgramShape {
    version: u32,
    combination: ScoringCombination,
    missing: MissingScoringFeature,
    terms: Vec<ScoreFeature>,
    decay: Vec<ScoreFeature>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScoringProgramError {
    InvalidSpecification(ScoringSpecError),
    MissingFeature(ScoreFeature),
    NonFiniteScore,
}

impl fmt::Display for ScoringProgramError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSpecification(error) => error.fmt(formatter),
            Self::MissingFeature(feature) => {
                write!(formatter, "missing declared scoring feature: {feature:?}")
            }
            Self::NonFiniteScore => {
                formatter.write_str("combined scoring arithmetic is not finite")
            }
        }
    }
}

impl std::error::Error for ScoringProgramError {}

impl ScoringProgram {
    pub fn new(
        combination: ScoringCombination,
        missing: MissingScoringFeature,
        spec: ScoringSpec,
    ) -> Result<Self, ScoringProgramError> {
        spec.validate()
            .map_err(ScoringProgramError::InvalidSpecification)?;
        Ok(Self {
            combination,
            missing,
            spec,
        })
    }

    pub fn specification(&self) -> &ScoringSpec {
        &self.spec
    }

    pub fn shape(&self) -> ScoringProgramShape {
        ScoringProgramShape {
            version: 1,
            combination: self.combination,
            missing: self.missing,
            terms: self
                .spec
                .terms
                .iter()
                .map(|term| term.feature.clone())
                .collect(),
            decay: self
                .spec
                .decay
                .iter()
                .map(|term| term.feature.clone())
                .collect(),
        }
    }

    /// A cache template retains no previous request's coefficients.
    pub fn neutral_template(&self) -> Self {
        let mut template = self.clone();
        for term in &mut template.spec.terms {
            term.weight = 1.0;
        }
        for decay in &mut template.spec.decay {
            decay.half_life = 1.0;
            decay.min_factor = 0.0;
        }
        template
    }

    pub fn evaluate_score(
        &self,
        source: &impl ScoringFeatureSource,
        reference_time_millis: u64,
    ) -> Result<f64, ScoringProgramError> {
        let mut missing = None;
        let score = self.spec.evaluate_into(
            source,
            reference_time_millis,
            self.combination,
            &mut |_| {},
            &mut |_| {},
            &mut |feature: &ScoreFeature| {
                if self.missing == MissingScoringFeature::Reject && missing.is_none() {
                    missing = Some(feature.clone());
                }
            },
        );
        self.check_result(score, missing.as_ref())
    }

    pub fn evaluate(
        &self,
        source: &impl ScoringFeatureSource,
        reference_time_millis: u64,
    ) -> Result<ScoringEvaluation, ScoringProgramError> {
        let mut evaluation = ScoringEvaluation {
            combined_score: 0.0,
            term_contributions: Vec::with_capacity(self.spec.terms.len()),
            decay_factors: Vec::with_capacity(self.spec.decay.len()),
            missing_features: Vec::new(),
        };
        evaluation.combined_score = self.spec.evaluate_into(
            source,
            reference_time_millis,
            self.combination,
            &mut |value| evaluation.term_contributions.push(value),
            &mut |value| evaluation.decay_factors.push(value),
            &mut |feature: &ScoreFeature| evaluation.missing_features.push(feature.clone()),
        );
        self.check_result(
            evaluation.combined_score,
            evaluation.missing_features.first(),
        )?;
        Ok(evaluation)
    }

    fn check_result(
        &self,
        score: f64,
        missing: Option<&ScoreFeature>,
    ) -> Result<f64, ScoringProgramError> {
        if self.missing == MissingScoringFeature::Reject
            && let Some(feature) = missing
        {
            return Err(ScoringProgramError::MissingFeature(feature.clone()));
        }
        if !score.is_finite() {
            return Err(ScoringProgramError::NonFiniteScore);
        }
        Ok(score)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_rag::{DecayTerm, ScoringTerm};

    struct Features(f64);
    impl ScoringFeatureSource for Features {
        fn search_score(&self) -> Option<f64> {
            Some(self.0)
        }
        fn graph_seed_score(&self) -> Option<f64> {
            Some(0.8)
        }
        fn hop_distance(&self) -> Option<usize> {
            Some(2)
        }
        fn numeric_property(&self, _: &str) -> Option<f64> {
            None
        }
        fn timestamp_millis(&self, _: &str) -> Option<u64> {
            Some(1_000)
        }
    }

    fn spec() -> ScoringSpec {
        ScoringSpec {
            terms: vec![
                ScoringTerm {
                    feature: ScoreFeature::SearchScore,
                    weight: 2.0,
                },
                ScoringTerm {
                    feature: ScoreFeature::GraphSeedScore,
                    weight: 1.0,
                },
            ],
            decay: vec![DecayTerm {
                feature: ScoreFeature::TimestampProperty("created".into()),
                half_life: 1.0,
                min_factor: 0.0,
            }],
        }
    }

    #[test]
    fn product_and_sum_use_declared_weights_decay_and_execution_time() {
        for (combination, expected) in [
            (ScoringCombination::WeightedSum, 1.8),
            (ScoringCombination::WeightedProduct, 0.2),
        ] {
            let specification = spec();
            let program = ScoringProgram::new(
                combination,
                MissingScoringFeature::Reject,
                specification.clone(),
            )
            .unwrap();
            for (anchor, factor) in [(1_000, 1.0), (2_000, 0.5), (3_000, 0.25)] {
                let scalar = program.evaluate_score(&Features(0.5), anchor).unwrap();
                let diagnostic = program.evaluate(&Features(0.5), anchor).unwrap();
                assert_eq!(scalar, expected * factor);
                assert_eq!(scalar.to_bits(), diagnostic.combined_score.to_bits());
                if combination == ScoringCombination::WeightedSum {
                    assert_eq!(
                        scalar.to_bits(),
                        specification
                            .evaluate_score(&Features(0.5), anchor)
                            .to_bits()
                    );
                }
            }
        }
    }

    #[test]
    fn structural_identity_excludes_numbers_but_keeps_composition_and_missing_policy() {
        let original = ScoringProgram::new(
            ScoringCombination::WeightedSum,
            MissingScoringFeature::Reject,
            spec(),
        )
        .unwrap();
        let mut changed = spec();
        changed.terms[0].weight = 7.0;
        changed.decay[0].half_life = 50.0;
        changed.decay[0].min_factor = 0.3;
        let rebound = ScoringProgram::new(
            ScoringCombination::WeightedSum,
            MissingScoringFeature::Reject,
            changed,
        )
        .unwrap();
        assert_eq!(original.shape(), rebound.shape());
        assert_eq!(original.neutral_template(), rebound.neutral_template());
        assert_ne!(
            original.shape(),
            ScoringProgram::new(
                ScoringCombination::WeightedProduct,
                MissingScoringFeature::Reject,
                spec()
            )
            .unwrap()
            .shape()
        );
        assert_ne!(
            original.shape(),
            ScoringProgram::new(
                ScoringCombination::WeightedSum,
                MissingScoringFeature::Neutral,
                spec()
            )
            .unwrap()
            .shape()
        );
    }

    #[test]
    fn negative_product_bases_preserve_integer_powers_and_reject_fractional_powers() {
        for (weight, expected) in [
            (0.0, Ok(1.0)),
            (0.5, Err(ScoringProgramError::NonFiniteScore)),
            (1.0, Ok(-0.25)),
            (2.0, Ok(0.0625)),
        ] {
            let program = ScoringProgram::new(
                ScoringCombination::WeightedProduct,
                MissingScoringFeature::Reject,
                ScoringSpec {
                    terms: vec![ScoringTerm {
                        feature: ScoreFeature::SearchScore,
                        weight,
                    }],
                    decay: Vec::new(),
                },
            )
            .unwrap();
            assert_eq!(program.evaluate_score(&Features(-0.25), 1_000), expected);
            assert_eq!(
                program
                    .evaluate(&Features(-0.25), 1_000)
                    .map(|evaluation| evaluation.combined_score),
                expected
            );
        }
    }

    #[test]
    fn missing_features_are_explicit_and_nonfinite_products_fail() {
        let mut specification = spec();
        specification.terms[0].feature = ScoreFeature::NodeProperty("absent".into());
        let strict = ScoringProgram::new(
            ScoringCombination::WeightedProduct,
            MissingScoringFeature::Reject,
            specification.clone(),
        )
        .unwrap();
        assert_eq!(
            strict.evaluate_score(&Features(0.5), 1_000),
            Err(ScoringProgramError::MissingFeature(
                ScoreFeature::NodeProperty("absent".into())
            ))
        );
        let neutral = ScoringProgram::new(
            ScoringCombination::WeightedProduct,
            MissingScoringFeature::Neutral,
            specification,
        )
        .unwrap();
        assert_eq!(neutral.evaluate_score(&Features(0.5), 1_000).unwrap(), 0.8);
        let mut specification = spec();
        specification.terms[1].weight = f64::MAX;
        specification.terms[0].weight = f64::MAX;
        // Finite inputs can underflow safely; overflow is checked separately.
        let program = ScoringProgram::new(
            ScoringCombination::WeightedProduct,
            MissingScoringFeature::Reject,
            specification,
        )
        .unwrap();
        assert_eq!(program.evaluate_score(&Features(0.5), 1_000).unwrap(), 0.0);
        assert_eq!(
            program.evaluate_score(&Features(2.0), 1_000),
            Err(ScoringProgramError::NonFiniteScore)
        );
    }
}
