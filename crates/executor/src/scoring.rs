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
use hawdb_core::{HawDBError, Result, RuntimeTaskContext, Value};
use std::num::NonZeroU64;

/// Validated identity and logical per-row cost for a host's batch scorer.
/// A future query attachment must include name/version in its cache identity
/// or bypass the cache. This cost is a planning hint, not elapsed time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostScorerDescriptor<'a> {
    name: &'a str,
    version: &'a str,
    cpu_units_per_row: NonZeroU64,
}

impl<'a> HostScorerDescriptor<'a> {
    pub fn new(name: &'a str, version: &'a str, cpu_units_per_row: NonZeroU64) -> Result<Self> {
        if [name, version]
            .iter()
            .any(|value| value.is_empty() || value.chars().any(char::is_control))
        {
            return Err(HawDBError::Semantic(
                "host scorer name and version must be nonempty without control characters".into(),
            ));
        }
        Ok(Self {
            name,
            version,
            cpu_units_per_row,
        })
    }

    pub fn name(self) -> &'a str {
        self.name
    }

    pub fn version(self) -> &'a str {
        self.version
    }

    pub fn cpu_units_per_row(self) -> NonZeroU64 {
        self.cpu_units_per_row
    }
}

/// Borrowed inputs for the host-scoring escape hatch.
/// Scratch allocations must reserve this query account before allocation and
/// release their leases before returning; feature rows cannot escape the call.
pub struct HostScorerBatch<'a> {
    pub features: &'a [&'a dyn ScoringFeatureSource],
    pub reference_time_millis: u64,
    pub task_context: Option<&'a RuntimeTaskContext>,
    pub scratch_account: &'a crate::QueryMemoryAccount,
}

impl HostScorerBatch<'_> {
    pub fn checkpoint(&self) -> Result<()> {
        crate::pipeline::runtime_checkpoint(self.task_context)
    }
}

/// Batch scorer contract for a concrete formula that templates cannot express.
///
/// This declaration does not register or execute callbacks in queries. When
/// integrated, the engine owns a score slice exactly matching the feature-row
/// count, checkpoints cancellation before/after the call, and validates every
/// finite output before TopN or consumer delivery. Implementations must write
/// every score in input order, be deterministic for the declared identity,
/// parameters and clock, and obey the scratch/cancellation contract.
pub trait HostScorer {
    fn descriptor(&self) -> HostScorerDescriptor<'_>;

    fn score_batch(&mut self, request: HostScorerBatch<'_>, scores: &mut [f64]) -> Result<()>;
}

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
        use crate::observer::NoopExecutionObserver;
        use crate::pipeline::{
            BatchControl, BatchExecutionContext, BindingBatch, BindingBatchSource,
        };
        use crate::{ExecutionLimit, ExecutionMemoryConfig, QueryMemoryLedger};
        use hawdb_core::{Catalog, Result};
        use hawdb_plan_cypher::PhysicalPlan;

        struct Source(Vec<Binding>);
        impl BindingBatchSource for Source {
            fn execute(
                &mut self,
                _: &PhysicalPlan,
                limit: ExecutionLimit,
                emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
            ) -> Result<BatchControl> {
                assert_eq!(limit, ExecutionLimit::unlimited());
                emit(std::mem::take(&mut self.0))
            }
        }
        let original: Vec<_> = [1.0, 3.0, 3.0, 2.0]
            .into_iter()
            .enumerate()
            .map(|(ordinal, score)| {
                binding([
                    ("value", Value::Int(ordinal as i64)),
                    ("score", Value::Float(score)),
                ])
            })
            .collect();
        let catalog = Catalog::default();
        let memory = ExecutionMemoryConfig::default();
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let spec = ScoringSpec {
            terms: vec![ScoringTerm {
                weight: 1.0,
                feature: ScoreFeature::SearchScore,
            }],
            decay: Vec::new(),
        };
        for limit in [3, 0] {
            let mut source = Source(original.clone());
            let mut retained = Vec::new();
            crate::transform::stream_scoring_rerank_batches(
                &PhysicalPlan::EmptyExec,
                "score",
                &spec,
                limit,
                &mut source,
                BatchExecutionContext {
                    catalog: &catalog,
                    memory: &memory,
                    memory_ledger: &ledger,
                    task_context: None,
                    observer: &NoopExecutionObserver,
                },
                ExecutionLimit::unlimited(),
                &mut |batch| {
                    retained.extend(batch);
                    Ok(BatchControl::Continue)
                },
            )
            .unwrap();
            let expected = if limit == 0 {
                Vec::new()
            } else {
                vec![Value::Int(1), Value::Int(2), Value::Int(3)]
            };
            assert_eq!(
                retained
                    .iter()
                    .map(|row| row.values["value"].clone())
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(retained.len(), limit);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
}
