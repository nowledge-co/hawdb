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

//! Row-level feature source for the typed scoring contract.

use crate::binding::Binding;
use hawdb_core::graph_rag::ScoringFeatureSource;
use hawdb_core::Value;

/// Reads scoring features from one row.
///
/// The search score comes from the declared score column the seed stage
/// produced; property features come from row values of the same name, which is
/// how node and relationship properties reach a row. Features this stage cannot
/// supply — the graph-seed score and the hop distance — are reported absent so
/// the specification records them instead of scoring them as zero.
pub struct BindingScoreFeatures<'a> {
    binding: &'a Binding,
    score_column: &'a str,
}

impl<'a> BindingScoreFeatures<'a> {
    pub fn new(binding: &'a Binding, score_column: &'a str) -> Self {
        Self {
            binding,
            score_column,
        }
    }

    fn numeric(&self, value: Option<&Value>) -> Option<f64> {
        match value? {
            Value::Float(number) => Some(*number),
            Value::Int(number) => Some(*number as f64),
            _ => None,
        }
    }
}

impl ScoringFeatureSource for BindingScoreFeatures<'_> {
    fn search_score(&self) -> Option<f64> {
        self.numeric(self.binding.values.get(self.score_column))
    }

    fn graph_seed_score(&self) -> Option<f64> {
        None
    }

    fn hop_distance(&self) -> Option<usize> {
        None
    }

    fn numeric_property(&self, property: &str) -> Option<f64> {
        self.numeric(self.binding.values.get(property))
    }

    fn timestamp_millis(&self, property: &str) -> Option<u64> {
        match self.binding.values.get(property)? {
            Value::Int(millis) => u64::try_from(*millis).ok(),
            _ => None,
        }
    }
}

/// Wall clock the specification ages timestamp features against, in epoch
/// milliseconds.
pub fn reference_time_millis() -> u64 {
    hawdb_core::time::SystemTime::now()
        .duration_since(hawdb_core::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::BindingScoreFeatures;
    use crate::binding::Binding;
    use hawdb_core::graph_rag::{ScoreFeature, ScoringFeatureSource, ScoringSpec, ScoringTerm};
    use hawdb_core::Value;
    use std::collections::BTreeMap;

    fn binding(values: impl IntoIterator<Item = (&'static str, Value)>) -> Binding {
        Binding {
            values: values
                .into_iter()
                .map(|(name, value)| (name.to_string(), value))
                .collect::<BTreeMap<_, _>>(),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }
    }

    #[test]
    fn reads_the_declared_score_column_and_properties() {
        let row = binding([
            ("score", Value::Float(0.25)),
            ("pagerank", Value::Int(65_000)),
            ("updated_at", Value::Int(1_000)),
            ("title", Value::String("not numeric".to_string())),
        ]);
        let features = BindingScoreFeatures::new(&row, "score");

        assert_eq!(features.search_score(), Some(0.25));
        assert_eq!(features.numeric_property("pagerank"), Some(65_000.0));
        assert_eq!(features.timestamp_millis("updated_at"), Some(1_000));
        assert_eq!(features.numeric_property("title"), None);
        assert_eq!(features.graph_seed_score(), None);
        assert_eq!(features.hop_distance(), None);

        let spec = ScoringSpec {
            terms: vec![
                ScoringTerm {
                    weight: 2.0,
                    feature: ScoreFeature::SearchScore,
                },
                ScoringTerm {
                    weight: 1.0,
                    feature: ScoreFeature::NodeProperty("pagerank".to_string()),
                },
            ],
            decay: Vec::new(),
        };
        let evaluation = spec.evaluate(&features, 0);
        assert_eq!(evaluation.combined_score, 2.0 * 0.25 + 65_000.0);
    }

    #[test]
    fn absent_and_non_numeric_features_report_instead_of_scoring_zero() {
        let row = binding([
            ("score", Value::Float(0.5)),
            ("title", Value::String("text".to_string())),
            ("negative_time", Value::Int(-5)),
        ]);
        let features = BindingScoreFeatures::new(&row, "score");
        assert_eq!(features.numeric_property("absent"), None);
        assert_eq!(features.numeric_property("title"), None);
        assert_eq!(features.timestamp_millis("negative_time"), None);

        let spec = ScoringSpec {
            terms: vec![ScoringTerm {
                weight: 1.0,
                feature: ScoreFeature::NodeProperty("absent".to_string()),
            }],
            decay: Vec::new(),
        };
        let evaluation = spec.evaluate(&features, 0);
        assert_eq!(evaluation.combined_score, 0.0);
        assert_eq!(
            evaluation.missing_features,
            vec![ScoreFeature::NodeProperty("absent".to_string())]
        );
    }

    #[test]
    fn a_missing_score_column_reports_missing_instead_of_zero() {
        let row = binding([("id", Value::String("row".to_string()))]);
        let features = BindingScoreFeatures::new(&row, "score");
        assert_eq!(features.search_score(), None);

        let spec = ScoringSpec {
            terms: vec![ScoringTerm {
                weight: 1.0,
                feature: ScoreFeature::SearchScore,
            }],
            decay: Vec::new(),
        };
        let evaluation = spec.evaluate(&features, 0);
        assert!(evaluation
            .missing_features
            .contains(&ScoreFeature::SearchScore));
    }

    #[test]
    fn rerank_keeps_the_best_rows_with_a_deterministic_tie_break() {
        let row = |value: i64| Binding {
            values: BTreeMap::from([("value".to_string(), Value::Int(value))]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        };
        let mut retained = vec![
            (1.0, 0usize, row(0)),
            (3.0, 1, row(1)),
            (3.0, 2, row(2)),
            (2.0, 3, row(3)),
        ];
        crate::transform::retain_best_scored(&mut retained, 3);
        let order: Vec<usize> = retained.iter().map(|(_, order, _)| *order).collect();
        // Highest score first, and equal scores keep their input order.
        assert_eq!(order, vec![1, 2, 3]);
        assert_eq!(retained.len(), 3);

        crate::transform::retain_best_scored(&mut retained, 0);
        assert!(retained.is_empty());
    }
}
