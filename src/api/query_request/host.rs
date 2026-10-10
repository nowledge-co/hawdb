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

use super::{HawDBError, Result};
use hawdb_executor::pipeline::BatchControl;
use hawdb_executor::scoring::{HostScorer, HostScorerProvider};
use hawdb_plan_cypher::HostScoringPlan;
use std::cell::RefCell;
use std::num::NonZeroUsize;

/// Host injection for a concrete whole-cohort formula that templates cannot
/// express. One request owns the borrowed callback; cached plans own metadata.
pub struct HostScoringRequest<'a> {
    pub(super) plan: HostScoringPlan,
    pub(super) candidate_window: bool,
    pub(super) reference_time_millis: Option<u64>,
    scorer: RefCell<&'a mut dyn HostScorer>,
}

impl<'a> HostScoringRequest<'a> {
    pub fn new(
        scorer: &'a mut dyn HostScorer,
        score_column: impl Into<String>,
        max_candidate_rows: NonZeroUsize,
        limit: usize,
    ) -> Result<Self> {
        let descriptor = scorer.descriptor();
        let plan = HostScoringPlan::new(
            descriptor.name(),
            descriptor.version(),
            descriptor.cpu_units_per_row(),
            score_column,
            max_candidate_rows,
            limit,
        )?;
        Ok(Self {
            plan,
            candidate_window: false,
            reference_time_millis: None,
            scorer: RefCell::new(scorer),
        })
    }

    /// Retain and rank the query's existing explicit candidate LIMIT/OFFSET.
    pub fn with_candidate_window(mut self) -> Self {
        self.candidate_window = true;
        self
    }

    pub fn with_reference_time_millis(mut self, time: u64) -> Self {
        self.reference_time_millis = Some(time);
        self
    }

    /// Preserve the candidate stream's direct-evidence order. The host score
    /// settles only contiguous exact Float-score/String-reason ties.
    pub fn with_contiguous_evidence_ties(
        mut self,
        reason_column: impl Into<String>,
    ) -> Result<Self> {
        self.plan = self.plan.with_contiguous_evidence_ties(reason_column)?;
        Ok(self)
    }

    pub fn with_vector_graph_input(
        mut self,
        seed_variable: impl Into<String>,
        candidate_variable: impl Into<String>,
    ) -> Result<Self> {
        self.plan = self
            .plan
            .with_vector_graph_input(seed_variable, candidate_variable)?;
        Ok(self)
    }

    pub fn with_graph_seed_input(
        mut self,
        seed_variable: impl Into<String>,
        candidate_variable: impl Into<String>,
    ) -> Result<Self> {
        self.plan = self
            .plan
            .with_graph_seed_input(seed_variable, candidate_variable)?;
        Ok(self)
    }

    pub fn with_text_graph_input(
        mut self,
        seed_variable: impl Into<String>,
        candidate_variable: impl Into<String>,
    ) -> Result<Self> {
        self.plan = self
            .plan
            .with_text_graph_input(seed_variable, candidate_variable)?;
        Ok(self)
    }
}

impl HostScorerProvider for HostScoringRequest<'_> {
    fn with_scorer(
        &self,
        run: &mut dyn FnMut(&mut dyn HostScorer) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let mut scorer = self.scorer.try_borrow_mut().map_err(|_| {
            HawDBError::Execution("host scorer is already executing for this request".into())
        })?;
        run(&mut **scorer)
    }
}

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "evidence_band_tests.rs"]
mod evidence_band_tests;
