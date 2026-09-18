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

//! Host-provided physical read operators.

#[doc(hidden)]
pub mod seed;

use crate::VectorExecutionReport;
use hawdb_core::{HawDBError, Result, RuntimeTaskContext};
use hawdb_plan::VectorPhysicalPlan;
use std::collections::BTreeMap;
use std::mem::size_of;
use std::num::NonZeroUsize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalReadResultBudget {
    pub max_rows: usize,
    pub max_memory_bytes: NonZeroUsize,
}

#[derive(Debug, Clone, Copy)]
pub struct ExternalReadResourceContract<'a> {
    pub priority: u8,
    pub max_parallelism: NonZeroUsize,
    pub max_working_memory_bytes: NonZeroUsize,
    pub result: ExternalReadResultBudget,
    pub task_context: Option<&'a RuntimeTaskContext>,
}

impl ExternalReadResourceContract<'_> {
    pub fn checkpoint(&self) -> Result<()> {
        match self.task_context {
            Some(task_context) => task_context.checkpoint().map_err(|reason| {
                HawDBError::Execution(format!("external read task stopped: {reason}"))
            }),
            None => Ok(()),
        }
    }

    pub fn reserved_memory_bytes(&self) -> usize {
        self.max_working_memory_bytes
            .get()
            .saturating_add(self.result.max_memory_bytes.get())
    }
}

pub struct VectorSeedExecutionRequest<'a> {
    pub embedding: &'a [f32],
    pub metadata_filters: &'a BTreeMap<String, String>,
    pub vector_plan: &'a VectorPhysicalPlan,
    pub resources: ExternalReadResourceContract<'a>,
}

pub struct VectorSeedExecutionRow {
    pub id: String,
    pub external_id: Option<String>,
    pub score: f64,
}

pub struct VectorSeedExecutionOutput {
    pub rows: Vec<VectorSeedExecutionRow>,
    pub report: VectorExecutionReport,
}

impl VectorSeedExecutionOutput {
    pub fn estimated_memory_bytes(&self) -> usize {
        self.rows
            .capacity()
            .saturating_mul(size_of::<VectorSeedExecutionRow>())
            .saturating_add(self.rows.iter().fold(0usize, |total, row| {
                total.saturating_add(row.id.capacity()).saturating_add(
                    row.external_id
                        .as_ref()
                        .map_or(0, |external_id| external_id.capacity()),
                )
            }))
    }

    pub fn validate_result_budget(&self, budget: ExternalReadResultBudget) -> Result<()> {
        if self.rows.len() > budget.max_rows {
            return Err(HawDBError::Execution(format!(
                "external vector read returned {} rows, exceeding result row budget {}",
                self.rows.len(),
                budget.max_rows
            )));
        }
        let memory_bytes = self.estimated_memory_bytes();
        if memory_bytes > budget.max_memory_bytes.get() {
            return Err(HawDBError::Execution(format!(
                "external vector read returned {memory_bytes} estimated bytes, exceeding result memory budget {}",
                budget.max_memory_bytes
            )));
        }
        Ok(())
    }
}

pub trait ExternalReadOperator {
    fn execute_vector_seed(
        &mut self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput>;
}

#[doc(hidden)]
pub struct NoExternalReadOperator;

impl ExternalReadOperator for NoExternalReadOperator {
    fn execute_vector_seed(
        &mut self,
        _request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        Err(HawDBError::Execution(
            "vector search capability is unavailable without a search projection".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VectorCompressionMode, VectorExecutionBackend, VectorFallbackReasonCode, VectorScoreSource,
    };
    use hawdb_core::RuntimeCancellationToken;
    use hawdb_plan::VectorCandidateSource;

    pub(super) fn empty_report() -> VectorExecutionReport {
        VectorExecutionReport {
            backend: VectorExecutionBackend::ScalarFlat,
            compression_mode: VectorCompressionMode::Disabled,
            candidate_source: VectorCandidateSource::Scalar,
            backend_selection_reason: None,
            estimated_raw_vector_bytes: None,
            filter_selectivity_per_million: None,
            candidate_score_source: VectorScoreSource::RawVector,
            final_score_source: VectorScoreSource::RawVector,
            generated_candidate_count: 0,
            descriptor_pruned_count: 0,
            scalar_filtered_count: 0,
            residual_filtered_count: 0,
            candidate_scan_rounds: 0,
            reranked_candidate_count: 0,
            returned_count: 0,
            raw_vector_bytes_read: 0,
            candidate_scan_metrics: None,
            index_covered_document_count: None,
            index_candidate_document_count: None,
            index_coverage_complete: None,
            fallback_reason_codes: Vec::<VectorFallbackReasonCode>::new(),
        }
    }

    #[test]
    fn external_vector_output_fails_closed_on_row_and_memory_budgets() {
        let output = VectorSeedExecutionOutput {
            rows: vec![VectorSeedExecutionRow {
                id: "memory-1".to_string(),
                external_id: Some("external-1".to_string()),
                score: 1.0,
            }],
            report: empty_report(),
        };

        assert!(output
            .validate_result_budget(ExternalReadResultBudget {
                max_rows: 0,
                max_memory_bytes: NonZeroUsize::new(1024).unwrap(),
            })
            .is_err());
        assert!(output
            .validate_result_budget(ExternalReadResultBudget {
                max_rows: 1,
                max_memory_bytes: NonZeroUsize::MIN,
            })
            .is_err());
    }

    #[test]
    fn external_read_contract_observes_cancellation() {
        let cancellation = RuntimeCancellationToken::new();
        let task_context = RuntimeTaskContext::without_deadline(cancellation.clone());
        let contract = ExternalReadResourceContract {
            priority: 128,
            max_parallelism: NonZeroUsize::MIN,
            max_working_memory_bytes: NonZeroUsize::MIN,
            result: ExternalReadResultBudget {
                max_rows: 1,
                max_memory_bytes: NonZeroUsize::MIN,
            },
            task_context: Some(&task_context),
        };

        assert!(contract.checkpoint().is_ok());
        assert!(cancellation.cancel());
        assert!(contract.checkpoint().is_err());
    }
}
