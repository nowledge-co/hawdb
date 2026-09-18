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

use crate::{OptimizerContext, VectorPrecision};
use hawdb_plan::{
    VectorBackendSelectionReason, VectorCandidateSource, VectorPhysicalPlan,
    VectorSearchLogicalPlan,
};
use std::fmt::{Display, Formatter};

const SELECTIVITY_SCALE: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorCompressionPreference {
    Disabled,
    Preferred,
    Required,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveVectorBackend {
    ScalarFlat,
    QuantizedProjection,
    RequiredProjectionUnavailable,
}

impl AdaptiveVectorBackend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScalarFlat => "scalar_flat",
            Self::QuantizedProjection => "quantized_projection",
            Self::RequiredProjectionUnavailable => "required_projection_unavailable",
        }
    }

    pub const fn candidate_source(self) -> VectorCandidateSource {
        match self {
            Self::ScalarFlat | Self::RequiredProjectionUnavailable => VectorCandidateSource::Scalar,
            Self::QuantizedProjection => VectorCandidateSource::Quantized,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveVectorBackendPolicy {
    pub flat_scan_max_documents: usize,
    pub high_filter_selectivity_per_million: u32,
    pub flat_scan_memory_budget_bytes: u64,
}

impl Default for AdaptiveVectorBackendPolicy {
    fn default() -> Self {
        Self {
            flat_scan_max_documents: 4_096,
            high_filter_selectivity_per_million: 750_000,
            flat_scan_memory_budget_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveVectorBackendInput {
    pub compression_preference: VectorCompressionPreference,
    pub document_count: usize,
    pub filtered_document_count: usize,
    pub embedding_dimension: usize,
    pub recall_validation_probe: bool,
    pub quantized_projection_available: bool,
    pub quantized_projection_covered_document_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveVectorBackendDecision {
    pub backend: AdaptiveVectorBackend,
    pub reason: VectorBackendSelectionReason,
    pub estimated_raw_vector_bytes: u64,
    pub filter_selectivity_per_million: u32,
    pub quantized_projection_coverage_complete: bool,
}

pub fn select_adaptive_vector_backend(
    input: AdaptiveVectorBackendInput,
    policy: AdaptiveVectorBackendPolicy,
) -> AdaptiveVectorBackendDecision {
    let estimated_raw_vector_bytes = u64::try_from(input.filtered_document_count)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(input.embedding_dimension).unwrap_or(u64::MAX))
        .saturating_mul(std::mem::size_of::<f32>() as u64);
    let filtered_out_count = input
        .document_count
        .saturating_sub(input.filtered_document_count);
    let filter_selectivity_per_million = if input.document_count == 0 {
        0
    } else {
        u32::try_from(
            u64::try_from(filtered_out_count)
                .unwrap_or(u64::MAX)
                .saturating_mul(SELECTIVITY_SCALE)
                / u64::try_from(input.document_count).unwrap_or(u64::MAX),
        )
        .unwrap_or(u32::MAX)
    };
    let quantized_projection_coverage_complete = input.quantized_projection_available
        && input.quantized_projection_covered_document_count >= input.filtered_document_count;
    let decision = |backend, reason| AdaptiveVectorBackendDecision {
        backend,
        reason,
        estimated_raw_vector_bytes,
        filter_selectivity_per_million,
        quantized_projection_coverage_complete,
    };

    if input.recall_validation_probe {
        return decision(
            AdaptiveVectorBackend::ScalarFlat,
            VectorBackendSelectionReason::RecallValidationProbe,
        );
    }
    if input.compression_preference == VectorCompressionPreference::Disabled {
        return decision(
            AdaptiveVectorBackend::ScalarFlat,
            VectorBackendSelectionReason::CompressionDisabled,
        );
    }
    if input.compression_preference == VectorCompressionPreference::Required {
        if !input.quantized_projection_available {
            return decision(
                AdaptiveVectorBackend::RequiredProjectionUnavailable,
                VectorBackendSelectionReason::QuantizedProjectionUnavailable,
            );
        }
        if !quantized_projection_coverage_complete {
            return decision(
                AdaptiveVectorBackend::RequiredProjectionUnavailable,
                VectorBackendSelectionReason::QuantizedProjectionCoverageIncomplete,
            );
        }
        return decision(
            AdaptiveVectorBackend::QuantizedProjection,
            VectorBackendSelectionReason::QuantizedRequired,
        );
    }
    if input.filtered_document_count <= policy.flat_scan_max_documents {
        return decision(
            AdaptiveVectorBackend::ScalarFlat,
            VectorBackendSelectionReason::SmallFilteredCandidateSet,
        );
    }
    if filter_selectivity_per_million >= policy.high_filter_selectivity_per_million {
        return decision(
            AdaptiveVectorBackend::ScalarFlat,
            VectorBackendSelectionReason::HighFilterSelectivity,
        );
    }
    if estimated_raw_vector_bytes <= policy.flat_scan_memory_budget_bytes {
        return decision(
            AdaptiveVectorBackend::ScalarFlat,
            VectorBackendSelectionReason::RawVectorsWithinMemoryBudget,
        );
    }
    if !input.quantized_projection_available {
        return decision(
            AdaptiveVectorBackend::ScalarFlat,
            VectorBackendSelectionReason::QuantizedProjectionUnavailable,
        );
    }
    if !quantized_projection_coverage_complete {
        return decision(
            AdaptiveVectorBackend::ScalarFlat,
            VectorBackendSelectionReason::QuantizedProjectionCoverageIncomplete,
        );
    }
    decision(
        AdaptiveVectorBackend::QuantizedProjection,
        VectorBackendSelectionReason::QuantizedPreferred,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorPlanError {
    EmptyEmbedding,
    EmptyTopK,
    CandidateLimitBelowTopK,
    InitialCandidateLimitBelowTopK,
    InitialCandidateLimitAboveBudget,
    MissingFilter,
    MissingCandidateScan,
    MissingRawRerank,
    MissingTopK,
}

impl Display for VectorPlanError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::EmptyEmbedding => "vector embedding dimension must be non-zero",
            Self::EmptyTopK => "vector top-k must be non-zero",
            Self::CandidateLimitBelowTopK => "vector candidate limit must cover top-k",
            Self::InitialCandidateLimitBelowTopK => {
                "initial vector candidate limit must cover top-k"
            }
            Self::InitialCandidateLimitAboveBudget => {
                "initial vector candidate limit must not exceed the candidate budget"
            }
            Self::MissingFilter => "vector pipeline must start with filter",
            Self::MissingCandidateScan => {
                "vector pipeline must include candidate scan after filter"
            }
            Self::MissingRawRerank => "vector pipeline must raw-rerank candidates",
            Self::MissingTopK => "vector pipeline must apply top-k after raw rerank",
        })
    }
}

impl std::error::Error for VectorPlanError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorPlanProperties {
    pub precision: VectorPrecision,
    pub priority: u8,
    pub max_parallelism: usize,
    pub max_memory_bytes: Option<u64>,
}

impl VectorPlanProperties {
    pub fn execution_resource_profile(&self) -> hawdb_plan::VectorExecutionResourceProfile {
        hawdb_plan::VectorExecutionResourceProfile {
            priority: self.priority,
            max_parallelism: self.max_parallelism.max(1),
            max_working_memory_bytes: self.max_memory_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedVectorSearch {
    pub plan: VectorPhysicalPlan,
    pub properties: VectorPlanProperties,
}

pub fn plan_vector_search(
    logical: &VectorSearchLogicalPlan,
    context: &OptimizerContext,
) -> Result<PlannedVectorSearch, VectorPlanError> {
    if logical.embedding_dimension == 0 {
        return Err(VectorPlanError::EmptyEmbedding);
    }
    if logical.top_k == 0 {
        return Err(VectorPlanError::EmptyTopK);
    }
    if logical.candidate_limit < logical.top_k {
        return Err(VectorPlanError::CandidateLimitBelowTopK);
    }
    if logical.initial_candidate_limit < logical.top_k {
        return Err(VectorPlanError::InitialCandidateLimitBelowTopK);
    }
    if logical.initial_candidate_limit > logical.candidate_limit {
        return Err(VectorPlanError::InitialCandidateLimitAboveBudget);
    }

    let filter = VectorPhysicalPlan::Filter {
        fields: logical.filter_fields.clone(),
    };
    let candidates = VectorPhysicalPlan::VectorCandidateScan {
        source: logical.candidate_source,
        embedding_dimension: logical.embedding_dimension,
        candidate_limit: logical.candidate_limit,
        input: Box::new(filter),
    };
    let candidates = if logical.residual_filter_fields.is_empty() {
        candidates
    } else {
        VectorPhysicalPlan::ResidualFilter {
            fields: logical.residual_filter_fields.clone(),
            initial_candidate_limit: logical.initial_candidate_limit,
            input: Box::new(candidates),
        }
    };
    let rerank = VectorPhysicalPlan::RawVectorRerank {
        embedding_dimension: logical.embedding_dimension,
        input: Box::new(candidates),
    };
    let plan = VectorPhysicalPlan::TopK {
        limit: logical.top_k,
        input: Box::new(rerank),
    };
    validate_vector_pipeline(&plan)?;

    Ok(PlannedVectorSearch {
        plan,
        properties: VectorPlanProperties {
            precision: VectorPrecision::RawReranked,
            priority: context.resource_hints().priority,
            max_parallelism: context.resource_hints().max_parallelism.max(1),
            max_memory_bytes: context.resource_hints().max_memory_bytes,
        },
    })
}

pub fn validate_vector_pipeline(plan: &VectorPhysicalPlan) -> Result<(), VectorPlanError> {
    let VectorPhysicalPlan::TopK { input, .. } = plan else {
        return Err(VectorPlanError::MissingTopK);
    };
    let VectorPhysicalPlan::RawVectorRerank { input, .. } = input.as_ref() else {
        return Err(VectorPlanError::MissingRawRerank);
    };
    let input = match input.as_ref() {
        VectorPhysicalPlan::ResidualFilter { input, .. } => input.as_ref(),
        input => input,
    };
    let VectorPhysicalPlan::VectorCandidateScan { input, .. } = input else {
        return Err(VectorPlanError::MissingCandidateScan);
    };
    if !matches!(input.as_ref(), VectorPhysicalPlan::Filter { .. }) {
        return Err(VectorPlanError::MissingFilter);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{QueryFamily, ResourceHints};
    use hawdb_plan::VectorCandidateSource;

    #[test]
    fn vector_pipeline_enforces_filter_candidate_raw_rerank_top_k() {
        let logical = VectorSearchLogicalPlan {
            embedding_dimension: 384,
            filter_fields: vec!["space_id".to_string(), "unit_type".to_string()],
            residual_filter_fields: Vec::new(),
            initial_candidate_limit: 64,
            candidate_source: VectorCandidateSource::Quantized,
            candidate_limit: 64,
            top_k: 10,
        };
        let context = OptimizerContext::default()
            .with_query_family(QueryFamily::VectorSearch)
            .with_resource_hints(ResourceHints {
                priority: 128,
                max_memory_bytes: Some(8 * 1024 * 1024),
                max_parallelism: 2,
            });

        let planned = plan_vector_search(&logical, &context).unwrap();

        assert_eq!(
            planned.plan.operator_pipeline(),
            vec!["Filter", "VectorCandidateScan", "RawVectorRerank", "TopK"]
        );
        assert_eq!(planned.properties.precision, VectorPrecision::RawReranked);
        assert_eq!(planned.properties.priority, 128);
        assert_eq!(planned.properties.max_parallelism, 2);
        assert_eq!(planned.properties.max_memory_bytes, Some(8 * 1024 * 1024));
    }

    #[test]
    fn vector_pipeline_rejects_final_candidate_scores_without_raw_rerank() {
        let invalid = VectorPhysicalPlan::TopK {
            limit: 10,
            input: Box::new(VectorPhysicalPlan::VectorCandidateScan {
                source: VectorCandidateSource::Ann,
                embedding_dimension: 384,
                candidate_limit: 40,
                input: Box::new(VectorPhysicalPlan::Filter { fields: Vec::new() }),
            }),
        };

        assert_eq!(
            validate_vector_pipeline(&invalid),
            Err(VectorPlanError::MissingRawRerank)
        );
    }

    #[test]
    fn vector_pipeline_places_residual_filter_before_raw_rerank() {
        let logical = VectorSearchLogicalPlan {
            embedding_dimension: 384,
            filter_fields: vec!["space_id".to_string()],
            residual_filter_fields: vec!["complex_visibility".to_string()],
            initial_candidate_limit: 10,
            candidate_source: VectorCandidateSource::Ann,
            candidate_limit: 80,
            top_k: 10,
        };

        let planned = plan_vector_search(&logical, &OptimizerContext::default()).unwrap();

        assert_eq!(
            planned.plan.operator_pipeline(),
            vec![
                "Filter",
                "VectorCandidateScan",
                "ResidualFilter",
                "RawVectorRerank",
                "TopK"
            ]
        );
        assert_eq!(validate_vector_pipeline(&planned.plan), Ok(()));
    }

    fn adaptive_input(
        compression_preference: VectorCompressionPreference,
    ) -> AdaptiveVectorBackendInput {
        AdaptiveVectorBackendInput {
            compression_preference,
            document_count: 100_000,
            filtered_document_count: 100_000,
            embedding_dimension: 768,
            recall_validation_probe: false,
            quantized_projection_available: true,
            quantized_projection_covered_document_count: 100_000,
        }
    }

    #[test]
    fn adaptive_vector_backend_prefers_scalar_for_small_filtered_sets() {
        let decision = select_adaptive_vector_backend(
            AdaptiveVectorBackendInput {
                filtered_document_count: 512,
                quantized_projection_covered_document_count: 512,
                ..adaptive_input(VectorCompressionPreference::Preferred)
            },
            AdaptiveVectorBackendPolicy::default(),
        );

        assert_eq!(decision.backend, AdaptiveVectorBackend::ScalarFlat);
        assert_eq!(
            decision.reason,
            VectorBackendSelectionReason::SmallFilteredCandidateSet
        );
    }

    #[test]
    fn adaptive_vector_backend_prefers_scalar_after_selective_filtering() {
        let decision = select_adaptive_vector_backend(
            AdaptiveVectorBackendInput {
                filtered_document_count: 10_000,
                quantized_projection_covered_document_count: 10_000,
                ..adaptive_input(VectorCompressionPreference::Preferred)
            },
            AdaptiveVectorBackendPolicy {
                flat_scan_max_documents: 1_000,
                flat_scan_memory_budget_bytes: 1,
                ..AdaptiveVectorBackendPolicy::default()
            },
        );

        assert_eq!(decision.backend, AdaptiveVectorBackend::ScalarFlat);
        assert_eq!(
            decision.reason,
            VectorBackendSelectionReason::HighFilterSelectivity
        );
        assert_eq!(decision.filter_selectivity_per_million, 900_000);
    }

    #[test]
    fn adaptive_vector_backend_uses_quantized_projection_over_memory_budget() {
        let decision = select_adaptive_vector_backend(
            adaptive_input(VectorCompressionPreference::Preferred),
            AdaptiveVectorBackendPolicy::default(),
        );

        assert_eq!(decision.backend, AdaptiveVectorBackend::QuantizedProjection);
        assert_eq!(
            decision.reason,
            VectorBackendSelectionReason::QuantizedPreferred
        );
        assert!(decision.estimated_raw_vector_bytes > 16 * 1024 * 1024);
    }

    #[test]
    fn adaptive_vector_backend_keeps_recall_validation_exact() {
        let decision = select_adaptive_vector_backend(
            AdaptiveVectorBackendInput {
                recall_validation_probe: true,
                ..adaptive_input(VectorCompressionPreference::Required)
            },
            AdaptiveVectorBackendPolicy::default(),
        );

        assert_eq!(decision.backend, AdaptiveVectorBackend::ScalarFlat);
        assert_eq!(
            decision.reason,
            VectorBackendSelectionReason::RecallValidationProbe
        );
    }

    #[test]
    fn adaptive_vector_backend_fails_closed_for_required_incomplete_projection() {
        let decision = select_adaptive_vector_backend(
            AdaptiveVectorBackendInput {
                quantized_projection_covered_document_count: 99_999,
                ..adaptive_input(VectorCompressionPreference::Required)
            },
            AdaptiveVectorBackendPolicy::default(),
        );

        assert_eq!(
            decision.backend,
            AdaptiveVectorBackend::RequiredProjectionUnavailable
        );
        assert_eq!(
            decision.reason,
            VectorBackendSelectionReason::QuantizedProjectionCoverageIncomplete
        );
        assert!(!decision.quantized_projection_coverage_complete);
    }
}
