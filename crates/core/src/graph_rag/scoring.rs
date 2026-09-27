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

//! Host-injectable scoring for graph-augmented retrieval.
//!
//! The spec is defined here, in the lowest crate that both the query engine and
//! the host-facing contracts can depend on, because the scoring operator runs in
//! the executor while the request contract lives above the search crate.

use std::fmt;

/// Typed, host-injectable rerank scoring.
///
/// A spec is a weighted sum of features multiplied by exponential decay
/// factors. The engine evaluates it one candidate at a time; a plan carries
/// only [`ScoringSpec::shape_fingerprint`], so changing weights never
/// invalidates a cached plan.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoringSpec {
    pub terms: Vec<ScoringTerm>,
    pub decay: Vec<DecayTerm>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScoringTerm {
    pub weight: f64,
    pub feature: ScoreFeature,
}

/// Exponential decay `0.5^(age / half_life)` over a distance or timestamp
/// feature, clamped to `[min_factor, 1]` and multiplied into the combined
/// score.
#[derive(Debug, Clone, PartialEq)]
pub struct DecayTerm {
    pub feature: ScoreFeature,
    pub half_life: f64,
    pub min_factor: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScoreFeature {
    /// Ranking score returned by the search projection.
    SearchScore,
    /// Graph-side seed score for the same canonical node.
    GraphSeedScore,
    /// Bounded graph distance from the seed (`0` for the seed itself).
    HopDistance,
    /// Numeric canonical node property.
    NodeProperty(String),
    /// Canonical timestamp property, aged against the request clock.
    TimestampProperty(String),
}

/// Feature values the engine can supply for one candidate.
pub trait ScoringFeatureSource {
    fn search_score(&self) -> Option<f64>;
    fn graph_seed_score(&self) -> Option<f64>;
    fn hop_distance(&self) -> Option<usize>;
    fn numeric_property(&self, property: &str) -> Option<f64>;
    fn timestamp_millis(&self, property: &str) -> Option<u64>;
}

/// One candidate's evaluation, carrying per-term provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoringEvaluation {
    pub combined_score: f64,
    /// Contribution of each [`ScoringSpec::terms`] entry, in spec order.
    pub term_contributions: Vec<f64>,
    /// Factor of each [`ScoringSpec::decay`] entry, in spec order.
    pub decay_factors: Vec<f64>,
    /// Features the source could not supply; they neither add nor multiply.
    pub missing_features: Vec<ScoreFeature>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScoringSpecError {
    NoTerms,
    NonFiniteWeight,
    NegativeWeight,
    NonPositiveHalfLife,
    InvalidMinFactor,
    /// A timestamp feature carries no rank value of its own.
    TimestampTermNotAllowed,
    /// Only `HopDistance` and `TimestampProperty` define an age to decay.
    UnsupportedDecayFeature,
}

impl fmt::Display for ScoringSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::NoTerms => "a scoring spec needs at least one term",
            Self::NonFiniteWeight => "scoring term weights must be finite",
            Self::NegativeWeight => "scoring term weights must not be negative",
            Self::NonPositiveHalfLife => "decay half-lives must be positive and finite",
            Self::InvalidMinFactor => "decay floors must lie in [0, 1]",
            Self::TimestampTermNotAllowed => {
                "a timestamp property is a decay feature, not an additive term"
            }
            Self::UnsupportedDecayFeature => {
                "only hop distance and timestamp properties define an age to decay"
            }
        };
        formatter.write_str(text)
    }
}

impl std::error::Error for ScoringSpecError {}

impl ScoringSpec {
    /// Weighted sum over the two retriever scores.
    pub fn weighted_scores(search_weight: f64, graph_seed_weight: f64) -> Self {
        Self {
            terms: vec![
                ScoringTerm {
                    weight: search_weight,
                    feature: ScoreFeature::SearchScore,
                },
                ScoringTerm {
                    weight: graph_seed_weight,
                    feature: ScoreFeature::GraphSeedScore,
                },
            ],
            decay: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<(), ScoringSpecError> {
        if self.terms.is_empty() {
            return Err(ScoringSpecError::NoTerms);
        }
        for term in &self.terms {
            if !term.weight.is_finite() {
                return Err(ScoringSpecError::NonFiniteWeight);
            }
            if term.weight < 0.0 {
                return Err(ScoringSpecError::NegativeWeight);
            }
            if matches!(term.feature, ScoreFeature::TimestampProperty(_)) {
                return Err(ScoringSpecError::TimestampTermNotAllowed);
            }
        }
        for decay in &self.decay {
            if !decay.half_life.is_finite() || decay.half_life <= 0.0 {
                return Err(ScoringSpecError::NonPositiveHalfLife);
            }
            if !decay.min_factor.is_finite() || !(0.0..=1.0).contains(&decay.min_factor) {
                return Err(ScoringSpecError::InvalidMinFactor);
            }
            if !matches!(
                decay.feature,
                ScoreFeature::HopDistance | ScoreFeature::TimestampProperty(_)
            ) {
                return Err(ScoringSpecError::UnsupportedDecayFeature);
            }
        }
        Ok(())
    }

    /// Feature shape without weights: the identity that belongs in a plan
    /// fingerprint or cache key.
    pub fn shape_fingerprint(&self) -> String {
        let terms = self
            .terms
            .iter()
            .map(|term| score_feature_name(&term.feature))
            .collect::<Vec<_>>()
            .join(",");
        let decay = self
            .decay
            .iter()
            .map(|decay| score_feature_name(&decay.feature))
            .collect::<Vec<_>>()
            .join(",");
        format!("terms=[{terms}] decay=[{decay}]")
    }

    /// Whether this spec reads canonical node properties. Callers use it to
    /// avoid loading a node record for specs that only need retriever scores.
    pub fn needs_canonical_node_properties(&self) -> bool {
        self.terms
            .iter()
            .any(|term| matches!(term.feature, ScoreFeature::NodeProperty(_)))
            || self.decay.iter().any(|decay| {
                matches!(
                    decay.feature,
                    ScoreFeature::NodeProperty(_) | ScoreFeature::TimestampProperty(_)
                )
            })
    }

    /// Evaluates one candidate. Missing features contribute nothing and are
    /// reported instead of silently ranking as zero-valued hits.
    pub fn evaluate(
        &self,
        source: &impl ScoringFeatureSource,
        reference_time_millis: u64,
    ) -> ScoringEvaluation {
        let mut missing_features = Vec::new();
        let mut term_contributions = Vec::with_capacity(self.terms.len());
        let mut combined_score = 0.0;
        for term in &self.terms {
            let value = match self.term_value(&term.feature, source) {
                Some(value) => value,
                None => {
                    missing_features.push(term.feature.clone());
                    0.0
                }
            };
            let contribution = term.weight * value;
            combined_score += contribution;
            term_contributions.push(contribution);
        }
        let mut decay_factors = Vec::with_capacity(self.decay.len());
        for decay in &self.decay {
            let age = match self.decay_age(&decay.feature, source, reference_time_millis) {
                Some(age) => age,
                None => {
                    missing_features.push(decay.feature.clone());
                    decay_factors.push(1.0);
                    continue;
                }
            };
            let factor = 0.5f64
                .powf(age / decay.half_life)
                .clamp(decay.min_factor, 1.0);
            combined_score *= factor;
            decay_factors.push(factor);
        }
        ScoringEvaluation {
            combined_score,
            term_contributions,
            decay_factors,
            missing_features,
        }
    }

    fn term_value(
        &self,
        feature: &ScoreFeature,
        source: &impl ScoringFeatureSource,
    ) -> Option<f64> {
        match feature {
            ScoreFeature::SearchScore => source.search_score().filter(|v| v.is_finite()),
            ScoreFeature::GraphSeedScore => source.graph_seed_score().filter(|v| v.is_finite()),
            ScoreFeature::HopDistance => source
                .hop_distance()
                .map(|hops| u32::try_from(hops).map_or(f64::from(u32::MAX), f64::from)),
            ScoreFeature::NodeProperty(property) => {
                source.numeric_property(property).filter(|v| v.is_finite())
            }
            // Decay-only features carry no additive value.
            ScoreFeature::TimestampProperty(_) => None,
        }
    }

    fn decay_age(
        &self,
        feature: &ScoreFeature,
        source: &impl ScoringFeatureSource,
        reference_time_millis: u64,
    ) -> Option<f64> {
        match feature {
            ScoreFeature::HopDistance => source.hop_distance().map(|hops| hops as f64),
            ScoreFeature::TimestampProperty(property) => {
                let timestamp = source.timestamp_millis(property)?;
                let age_millis = reference_time_millis.saturating_sub(timestamp);
                Some(age_millis as f64 / 1_000.0)
            }
            _ => None,
        }
    }
}

fn score_feature_name(feature: &ScoreFeature) -> String {
    match feature {
        ScoreFeature::SearchScore => "search_score".to_string(),
        ScoreFeature::GraphSeedScore => "graph_seed_score".to_string(),
        ScoreFeature::HopDistance => "hop_distance".to_string(),
        ScoreFeature::NodeProperty(property) => format!("node_property:{property}"),
        ScoreFeature::TimestampProperty(property) => format!("timestamp:{property}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DecayTerm, ScoreFeature, ScoringFeatureSource, ScoringSpec, ScoringSpecError, ScoringTerm,
    };
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct StubFeatures {
        search_score: Option<f64>,
        graph_seed_score: Option<f64>,
        hop_distance: Option<usize>,
        numeric: BTreeMap<String, f64>,
        timestamps: BTreeMap<String, u64>,
    }

    impl ScoringFeatureSource for StubFeatures {
        fn search_score(&self) -> Option<f64> {
            self.search_score
        }

        fn graph_seed_score(&self) -> Option<f64> {
            self.graph_seed_score
        }

        fn hop_distance(&self) -> Option<usize> {
            self.hop_distance
        }

        fn numeric_property(&self, property: &str) -> Option<f64> {
            self.numeric.get(property).copied()
        }

        fn timestamp_millis(&self, property: &str) -> Option<u64> {
            self.timestamps.get(property).copied()
        }
    }

    fn term(weight: f64, feature: ScoreFeature) -> ScoringTerm {
        ScoringTerm { weight, feature }
    }

    fn decay(feature: ScoreFeature, half_life: f64, min_factor: f64) -> DecayTerm {
        DecayTerm {
            feature,
            half_life,
            min_factor,
        }
    }

    #[test]
    fn scoring_spec_combines_weighted_features() {
        let spec = ScoringSpec {
            terms: vec![
                term(0.5, ScoreFeature::SearchScore),
                term(2.0, ScoreFeature::NodeProperty("pagerank".to_string())),
            ],
            decay: Vec::new(),
        };
        let source = StubFeatures {
            search_score: Some(4.0),
            numeric: BTreeMap::from([("pagerank".to_string(), 3.0)]),
            ..StubFeatures::default()
        };
        let evaluation = spec.evaluate(&source, 0);
        assert_eq!(evaluation.combined_score, 0.5 * 4.0 + 2.0 * 3.0);
        assert_eq!(evaluation.term_contributions, vec![2.0, 6.0]);
        assert!(evaluation.missing_features.is_empty());
    }

    #[test]
    fn scoring_spec_reports_missing_features_instead_of_ranking_them_zero() {
        let spec = ScoringSpec {
            terms: vec![
                term(1.0, ScoreFeature::SearchScore),
                term(1.0, ScoreFeature::GraphSeedScore),
            ],
            decay: Vec::new(),
        };
        let source = StubFeatures {
            search_score: Some(2.0),
            ..StubFeatures::default()
        };
        let evaluation = spec.evaluate(&source, 0);
        assert_eq!(evaluation.combined_score, 2.0);
        assert_eq!(
            evaluation.missing_features,
            vec![ScoreFeature::GraphSeedScore]
        );
    }

    #[test]
    fn hop_decay_multiplies_and_holds_its_floor() {
        let spec = ScoringSpec {
            terms: vec![term(1.0, ScoreFeature::SearchScore)],
            decay: vec![decay(ScoreFeature::HopDistance, 1.0, 0.25)],
        };
        let evaluate = |hops: usize| {
            spec.evaluate(
                &StubFeatures {
                    search_score: Some(8.0),
                    hop_distance: Some(hops),
                    ..StubFeatures::default()
                },
                0,
            )
        };
        assert_eq!(evaluate(0).combined_score, 8.0);
        assert_eq!(evaluate(1).combined_score, 4.0);
        assert_eq!(evaluate(2).combined_score, 2.0);
        assert_eq!(evaluate(9).combined_score, 2.0);
        assert_eq!(evaluate(2).decay_factors, vec![0.25]);
    }

    #[test]
    fn timestamp_decay_ages_against_the_request_clock() {
        let spec = ScoringSpec {
            terms: vec![term(1.0, ScoreFeature::SearchScore)],
            decay: vec![decay(
                ScoreFeature::TimestampProperty("updated_at".to_string()),
                600.0,
                0.0,
            )],
        };
        let aged = StubFeatures {
            search_score: Some(10.0),
            timestamps: BTreeMap::from([("updated_at".to_string(), 400_000)]),
            ..StubFeatures::default()
        };
        // 600 seconds old against a 600-second half-life.
        assert_eq!(spec.evaluate(&aged, 1_000_000).combined_score, 5.0);
        let future = StubFeatures {
            search_score: Some(10.0),
            timestamps: BTreeMap::from([("updated_at".to_string(), 2_000_000)]),
            ..StubFeatures::default()
        };
        assert_eq!(spec.evaluate(&future, 1_000_000).combined_score, 10.0);
    }

    #[test]
    fn scoring_spec_validation_rejects_unusable_shapes() {
        let missing = StubFeatures::default();
        assert_eq!(
            ScoringSpec {
                terms: Vec::new(),
                decay: Vec::new(),
            }
            .validate(),
            Err(ScoringSpecError::NoTerms)
        );
        assert_eq!(
            ScoringSpec {
                terms: vec![term(-1.0, ScoreFeature::SearchScore)],
                decay: Vec::new(),
            }
            .validate(),
            Err(ScoringSpecError::NegativeWeight)
        );
        assert_eq!(
            ScoringSpec {
                terms: vec![term(f64::NAN, ScoreFeature::SearchScore)],
                decay: Vec::new(),
            }
            .validate(),
            Err(ScoringSpecError::NonFiniteWeight)
        );
        assert_eq!(
            ScoringSpec {
                terms: vec![term(1.0, ScoreFeature::SearchScore)],
                decay: vec![decay(ScoreFeature::HopDistance, 0.0, 0.0)],
            }
            .validate(),
            Err(ScoringSpecError::NonPositiveHalfLife)
        );
        assert_eq!(
            ScoringSpec {
                terms: vec![term(1.0, ScoreFeature::SearchScore)],
                decay: vec![decay(ScoreFeature::HopDistance, 1.0, 1.5)],
            }
            .validate(),
            Err(ScoringSpecError::InvalidMinFactor)
        );
        assert_eq!(
            ScoringSpec {
                terms: vec![term(1.0, ScoreFeature::TimestampProperty("t".to_string()))],
                decay: Vec::new(),
            }
            .validate(),
            Err(ScoringSpecError::TimestampTermNotAllowed)
        );
        assert_eq!(
            ScoringSpec {
                terms: vec![term(1.0, ScoreFeature::SearchScore)],
                decay: vec![decay(ScoreFeature::SearchScore, 1.0, 0.0)],
            }
            .validate(),
            Err(ScoringSpecError::UnsupportedDecayFeature)
        );
        assert!(ScoringSpec::weighted_scores(1.0, 1.0).validate().is_ok());
        // A property that is merely absent at runtime is valid, and reported
        // through `missing_features` instead of failing the request.
        assert!(ScoringSpec {
            terms: vec![term(1.0, ScoreFeature::NodeProperty("absent".to_string()))],
            decay: Vec::new(),
        }
        .validate()
        .is_ok());
        assert!(missing.numeric_property("absent").is_none());
        assert!(ScoringSpecError::NoTerms
            .to_string()
            .contains("at least one term"));
    }

    #[test]
    fn scoring_shape_fingerprint_ignores_weights() {
        let lightness = ScoringSpec {
            terms: vec![
                term(0.1, ScoreFeature::SearchScore),
                term(0.2, ScoreFeature::HopDistance),
            ],
            decay: vec![decay(ScoreFeature::HopDistance, 30.0, 0.1)],
        };
        let heavy = ScoringSpec {
            terms: vec![
                term(9.0, ScoreFeature::SearchScore),
                term(5.0, ScoreFeature::HopDistance),
            ],
            decay: vec![decay(ScoreFeature::HopDistance, 1.0, 0.9)],
        };
        assert_eq!(lightness.shape_fingerprint(), heavy.shape_fingerprint());
        assert!(!lightness.needs_canonical_node_properties());
        let property = ScoringSpec {
            terms: vec![term(
                1.0,
                ScoreFeature::NodeProperty("pagerank".to_string()),
            )],
            decay: Vec::new(),
        };
        assert!(property.needs_canonical_node_properties());
    }
}
