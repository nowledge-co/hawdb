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

use hawdb_plan::{VectorBackendSelectionReason, VectorCandidateSource, VectorPhysicalPlan};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorScoreSource {
    Unavailable,
    RawVector,
    AnnApproximate,
    QuantizedApproximate,
}

impl VectorScoreSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::RawVector => "raw_vector",
            Self::AnnApproximate => "ann_approximate",
            Self::QuantizedApproximate => "quantized_approximate",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorExecutionBackend {
    ScalarFlat,
    AnnProjection,
    QuantizedProjection,
    Unavailable,
}

impl VectorExecutionBackend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScalarFlat => "scalar_flat",
            Self::AnnProjection => "ann_projection",
            Self::QuantizedProjection => "quantized_projection",
            Self::Unavailable => "unavailable",
        }
    }

    const fn from_candidate_source(source: VectorCandidateSource) -> Self {
        match source {
            VectorCandidateSource::Scalar => Self::ScalarFlat,
            VectorCandidateSource::Ann => Self::AnnProjection,
            VectorCandidateSource::Quantized => Self::QuantizedProjection,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorCompressionMode {
    Unspecified,
    Disabled,
    Preferred,
    Required,
}

impl VectorCompressionMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Disabled => "disabled",
            Self::Preferred => "preferred",
            Self::Required => "required",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorFallbackReasonCode {
    VectorDimensionMismatch,
    VectorIndexEmpty,
    CompressedVectorProjectionUnavailable,
    QueryEmbeddingMissing,
}

impl VectorFallbackReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::VectorDimensionMismatch => "vector_dimension_mismatch",
            Self::VectorIndexEmpty => "vector_index_empty",
            Self::CompressedVectorProjectionUnavailable => {
                "compressed_vector_projection_unavailable"
            }
            Self::QueryEmbeddingMissing => "query_embedding_missing",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorCandidate {
    pub id: String,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorCandidateBatch {
    pub score_source: VectorScoreSource,
    pub candidates: Vec<VectorCandidate>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorRawScore {
    pub id: String,
    pub score: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct VectorCandidateScanRequest<'a> {
    pub source: VectorCandidateSource,
    pub embedding_dimension: usize,
    pub candidate_limit: usize,
    pub filter_fields: &'a [String],
}

#[derive(Debug, Clone, Copy)]
pub struct VectorRawRerankRequest<'a> {
    pub embedding_dimension: usize,
    pub candidates: &'a [VectorCandidate],
}

#[derive(Debug)]
pub struct VectorResidualFilterRequest<'a> {
    pub fields: &'a [String],
    pub candidates: Vec<VectorCandidate>,
}

pub trait VectorExecutionSource {
    type Error;

    fn scan_candidates(
        &mut self,
        request: VectorCandidateScanRequest<'_>,
    ) -> Result<VectorCandidateBatch, Self::Error>;

    fn filter_residual(
        &mut self,
        request: VectorResidualFilterRequest<'_>,
    ) -> Result<Vec<VectorCandidate>, Self::Error>;

    fn rerank_raw(
        &mut self,
        request: VectorRawRerankRequest<'_>,
    ) -> Result<Vec<VectorRawScore>, Self::Error>;

    fn raw_vector_bytes_read(&self) -> u64 {
        0
    }

    fn candidate_scan_metrics(&self) -> Option<VectorCandidateScanMetrics> {
        None
    }

    fn capture_candidate_ids(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorCandidateScanMetrics {
    pub kernel: String,
    pub worker_count: usize,
    pub segment_count: usize,
    pub scanned_segment_count: usize,
    pub scored_document_count: usize,
    pub filtered_document_count: usize,
    pub scanned_block_count: usize,
    pub skipped_block_count: usize,
    pub payload_bytes_read: u64,
    pub admitted_working_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorExecutionReport {
    pub backend: VectorExecutionBackend,
    pub compression_mode: VectorCompressionMode,
    pub candidate_source: VectorCandidateSource,
    pub backend_selection_reason: Option<VectorBackendSelectionReason>,
    pub estimated_raw_vector_bytes: Option<u64>,
    pub filter_selectivity_per_million: Option<u32>,
    pub candidate_score_source: VectorScoreSource,
    pub final_score_source: VectorScoreSource,
    pub generated_candidate_count: usize,
    pub descriptor_pruned_count: usize,
    pub scalar_filtered_count: usize,
    pub residual_filtered_count: usize,
    pub candidate_scan_rounds: usize,
    pub reranked_candidate_count: usize,
    pub returned_count: usize,
    pub raw_vector_bytes_read: u64,
    pub candidate_scan_metrics: Option<VectorCandidateScanMetrics>,
    pub index_covered_document_count: Option<usize>,
    pub index_candidate_document_count: Option<usize>,
    pub index_coverage_complete: Option<bool>,
    pub fallback_reason_codes: Vec<VectorFallbackReasonCode>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorExecutionOutput {
    pub scores: Vec<VectorRawScore>,
    pub candidate_ids: Vec<String>,
    pub report: VectorExecutionReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorExecutionError<E> {
    InvalidPlan(&'static str),
    Source(E),
    DuplicateCandidate,
    DuplicateRawScore,
    NonFiniteCandidateScore,
    NonFiniteRawScore,
    ResidualFilterAddedCandidate,
    RawScoreOutsideCandidateSet,
}

impl<E: Display> Display for VectorExecutionError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPlan(reason) => write!(formatter, "invalid vector plan: {reason}"),
            Self::Source(error) => write!(formatter, "vector execution source failed: {error}"),
            Self::DuplicateCandidate => formatter.write_str("vector candidate ids must be unique"),
            Self::DuplicateRawScore => formatter.write_str("raw vector score ids must be unique"),
            Self::NonFiniteCandidateScore => {
                formatter.write_str("vector candidate score must be finite")
            }
            Self::NonFiniteRawScore => formatter.write_str("raw vector score must be finite"),
            Self::ResidualFilterAddedCandidate => {
                formatter.write_str("residual vector filter may only retain generated candidates")
            }
            Self::RawScoreOutsideCandidateSet => {
                formatter.write_str("raw vector score must reference a generated candidate")
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for VectorExecutionError<E> {}

pub fn execute_vector_plan<S: VectorExecutionSource>(
    plan: &VectorPhysicalPlan,
    source: &mut S,
) -> Result<VectorExecutionOutput, VectorExecutionError<S::Error>> {
    let VectorPhysicalPlan::TopK {
        limit: top_k,
        input,
    } = plan
    else {
        return Err(VectorExecutionError::InvalidPlan("missing TopK root"));
    };
    let VectorPhysicalPlan::RawVectorRerank {
        embedding_dimension: rerank_dimension,
        input,
    } = input.as_ref()
    else {
        return Err(VectorExecutionError::InvalidPlan(
            "TopK must consume RawVectorRerank",
        ));
    };
    let (residual_filter, input) = match input.as_ref() {
        VectorPhysicalPlan::ResidualFilter {
            fields,
            initial_candidate_limit,
            input,
        } => (
            Some((fields.as_slice(), *initial_candidate_limit)),
            input.as_ref(),
        ),
        input => (None, input),
    };
    let VectorPhysicalPlan::VectorCandidateScan {
        source: candidate_source,
        embedding_dimension: candidate_dimension,
        candidate_limit,
        input,
    } = input
    else {
        return Err(VectorExecutionError::InvalidPlan(
            "RawVectorRerank must consume VectorCandidateScan",
        ));
    };
    let VectorPhysicalPlan::Filter { fields } = input.as_ref() else {
        return Err(VectorExecutionError::InvalidPlan(
            "VectorCandidateScan must consume Filter",
        ));
    };
    if candidate_dimension != rerank_dimension {
        return Err(VectorExecutionError::InvalidPlan(
            "candidate and rerank dimensions differ",
        ));
    }
    if *top_k == 0 || *candidate_limit < *top_k {
        return Err(VectorExecutionError::InvalidPlan(
            "candidate and top-k limits are inconsistent",
        ));
    }
    if let Some((_, initial_limit)) = residual_filter
        && (initial_limit < *top_k || initial_limit > *candidate_limit)
    {
        return Err(VectorExecutionError::InvalidPlan(
            "residual candidate window is outside the top-k candidate budget",
        ));
    }

    let mut scan_limit = residual_filter
        .map(|(_, initial_limit)| initial_limit)
        .unwrap_or(*candidate_limit);
    let mut candidate_scan_rounds = 0;
    let (batch, generated_candidate_count) = loop {
        candidate_scan_rounds += 1;
        let mut batch = source
            .scan_candidates(VectorCandidateScanRequest {
                source: *candidate_source,
                embedding_dimension: *candidate_dimension,
                candidate_limit: scan_limit,
                filter_fields: fields,
            })
            .map_err(VectorExecutionError::Source)?;
        validate_candidates(&batch.candidates)?;
        sort_candidates(&mut batch.candidates);
        batch.candidates.truncate(scan_limit);
        let generated_candidate_count = batch.candidates.len();
        if let Some((residual_fields, _)) = residual_filter {
            let generated_ids = batch
                .candidates
                .iter()
                .map(|candidate| candidate.id.clone())
                .collect::<BTreeSet<_>>();
            batch.candidates = source
                .filter_residual(VectorResidualFilterRequest {
                    fields: residual_fields,
                    candidates: batch.candidates,
                })
                .map_err(VectorExecutionError::Source)?;
            validate_candidates(&batch.candidates)?;
            if batch
                .candidates
                .iter()
                .any(|candidate| !generated_ids.contains(&candidate.id))
            {
                return Err(VectorExecutionError::ResidualFilterAddedCandidate);
            }
        }
        if residual_filter.is_none()
            || batch.candidates.len() >= *top_k
            || scan_limit >= *candidate_limit
            || generated_candidate_count < scan_limit
        {
            break (batch, generated_candidate_count);
        }
        scan_limit = scan_limit.saturating_mul(2).min(*candidate_limit);
    };
    let residual_filtered_count = generated_candidate_count.saturating_sub(batch.candidates.len());
    let candidate_ids = if source.capture_candidate_ids() {
        batch
            .candidates
            .iter()
            .map(|candidate| candidate.id.clone())
            .collect()
    } else {
        Vec::new()
    };

    let mut raw_scores = source
        .rerank_raw(VectorRawRerankRequest {
            embedding_dimension: *rerank_dimension,
            candidates: &batch.candidates,
        })
        .map_err(VectorExecutionError::Source)?;
    validate_raw_scores(&batch.candidates, &raw_scores)?;
    sort_raw_scores(&mut raw_scores);
    let reranked_candidate_count = raw_scores.len();
    raw_scores.truncate(*top_k);

    let final_score_source = if batch.score_source == VectorScoreSource::Unavailable {
        VectorScoreSource::Unavailable
    } else {
        VectorScoreSource::RawVector
    };
    Ok(VectorExecutionOutput {
        candidate_ids,
        report: VectorExecutionReport {
            backend: VectorExecutionBackend::from_candidate_source(*candidate_source),
            compression_mode: VectorCompressionMode::Unspecified,
            candidate_source: *candidate_source,
            backend_selection_reason: None,
            estimated_raw_vector_bytes: None,
            filter_selectivity_per_million: None,
            candidate_score_source: batch.score_source,
            final_score_source,
            generated_candidate_count,
            descriptor_pruned_count: 0,
            scalar_filtered_count: residual_filtered_count,
            residual_filtered_count,
            candidate_scan_rounds,
            reranked_candidate_count,
            returned_count: raw_scores.len(),
            raw_vector_bytes_read: source.raw_vector_bytes_read(),
            candidate_scan_metrics: source.candidate_scan_metrics(),
            index_covered_document_count: None,
            index_candidate_document_count: None,
            index_coverage_complete: None,
            fallback_reason_codes: Vec::new(),
        },
        scores: raw_scores,
    })
}

fn validate_candidates<E>(candidates: &[VectorCandidate]) -> Result<(), VectorExecutionError<E>> {
    let mut ids = BTreeSet::new();
    for candidate in candidates {
        if !candidate.score.is_finite() {
            return Err(VectorExecutionError::NonFiniteCandidateScore);
        }
        if !ids.insert(candidate.id.as_str()) {
            return Err(VectorExecutionError::DuplicateCandidate);
        }
    }
    Ok(())
}

fn validate_raw_scores<E>(
    candidates: &[VectorCandidate],
    raw_scores: &[VectorRawScore],
) -> Result<(), VectorExecutionError<E>> {
    let candidate_ids = candidates
        .iter()
        .map(|candidate| candidate.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut score_ids = BTreeSet::new();
    for score in raw_scores {
        if !score.score.is_finite() {
            return Err(VectorExecutionError::NonFiniteRawScore);
        }
        if !candidate_ids.contains(score.id.as_str()) {
            return Err(VectorExecutionError::RawScoreOutsideCandidateSet);
        }
        if !score_ids.insert(score.id.as_str()) {
            return Err(VectorExecutionError::DuplicateRawScore);
        }
    }
    Ok(())
}

fn sort_candidates(candidates: &mut [VectorCandidate]) {
    candidates.sort_by(|left, right| {
        descending_score_order(left.score, &left.id, right.score, &right.id)
    });
}

fn sort_raw_scores(scores: &mut [VectorRawScore]) {
    scores.sort_by(|left, right| {
        descending_score_order(left.score, &left.id, right.score, &right.id)
    });
}

fn descending_score_order(
    left_score: f64,
    left_id: &str,
    right_score: f64,
    right_id: &str,
) -> Ordering {
    right_score
        .partial_cmp(&left_score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left_id.cmp(right_id))
}

#[cfg(test)]
mod tests;
