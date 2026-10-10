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

use super::{ScoringSeedGraphInput, SCORING_RERANK_SCORE_COLUMN};
use hawdb_core::{HawDBError, Result};
use std::num::{NonZeroU64, NonZeroUsize};

/// How a host's secondary scores affect the already ordered candidate cohort.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HostScoringRankPolicy {
    #[default]
    ScoreDescending,
    /// Preserve adjacent direct-evidence bands; rerank only exact score/reason
    /// ties. Separated occurrences of the same evidence remain separate bands.
    ContiguousEvidenceTies { reason_column: String },
}

impl HostScoringRankPolicy {
    pub fn validate(&self, score_column: &str) -> Result<()> {
        if let Self::ContiguousEvidenceTies { reason_column } = self
            && (reason_column.is_empty()
                || reason_column.contains('\0')
                || reason_column == score_column
                || reason_column == SCORING_RERANK_SCORE_COLUMN)
        {
            return Err(HawDBError::Semantic(
                "contiguous evidence ranking requires a distinct nonempty reason column".into(),
            ));
        }
        Ok(())
    }
}

/// Cacheable host-scoring metadata. It never owns a callback or execution clock.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HostScoringPlan {
    name: String,
    version: String,
    cpu_units_per_row: NonZeroU64,
    score_column: String,
    max_candidate_rows: NonZeroUsize,
    limit: usize,
    seed_graph_input: Option<ScoringSeedGraphInput>,
    rank_policy: HostScoringRankPolicy,
}

impl HostScoringPlan {
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        cpu_units_per_row: NonZeroU64,
        score_column: impl Into<String>,
        max_candidate_rows: NonZeroUsize,
        limit: usize,
    ) -> Result<Self> {
        let name = name.into();
        let version = version.into();
        let score_column = score_column.into();
        if [&name, &version]
            .iter()
            .any(|value| value.is_empty() || value.chars().any(char::is_control))
        {
            return Err(HawDBError::Semantic(
                "host scorer name and version must be nonempty without control characters".into(),
            ));
        }
        if score_column.is_empty()
            || score_column.contains('\0')
            || score_column == SCORING_RERANK_SCORE_COLUMN
        {
            return Err(HawDBError::Semantic("host scoring requires a nonempty input score column distinct from its result column".into()));
        }
        Ok(Self {
            name,
            version,
            cpu_units_per_row,
            score_column,
            max_candidate_rows,
            limit,
            seed_graph_input: None,
            rank_policy: HostScoringRankPolicy::ScoreDescending,
        })
    }

    pub fn with_vector_graph_input(
        mut self,
        seed_variable: impl Into<String>,
        candidate_variable: impl Into<String>,
    ) -> Result<Self> {
        self.seed_graph_input = Some(ScoringSeedGraphInput::new(
            seed_variable,
            candidate_variable,
        )?);
        Ok(self)
    }

    pub fn with_graph_seed_input(
        mut self,
        seed_variable: impl Into<String>,
        candidate_variable: impl Into<String>,
    ) -> Result<Self> {
        self.seed_graph_input = Some(ScoringSeedGraphInput::new_graph(
            seed_variable,
            candidate_variable,
        )?);
        Ok(self)
    }

    pub fn with_text_graph_input(
        mut self,
        seed_variable: impl Into<String>,
        candidate_variable: impl Into<String>,
    ) -> Result<Self> {
        self.seed_graph_input = Some(ScoringSeedGraphInput::new_text(
            seed_variable,
            candidate_variable,
        )?);
        Ok(self)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn version(&self) -> &str {
        &self.version
    }
    pub fn cpu_units_per_row(&self) -> NonZeroU64 {
        self.cpu_units_per_row
    }
    pub fn score_column(&self) -> &str {
        &self.score_column
    }
    pub fn max_candidate_rows(&self) -> NonZeroUsize {
        self.max_candidate_rows
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
    pub fn seed_graph_input(&self) -> Option<&ScoringSeedGraphInput> {
        self.seed_graph_input.as_ref()
    }

    pub fn with_contiguous_evidence_ties(
        mut self,
        reason_column: impl Into<String>,
    ) -> Result<Self> {
        self.rank_policy = HostScoringRankPolicy::ContiguousEvidenceTies {
            reason_column: reason_column.into(),
        };
        self.rank_policy.validate(&self.score_column)?;
        Ok(self)
    }

    pub fn rank_policy(&self) -> &HostScoringRankPolicy {
        &self.rank_policy
    }
}
