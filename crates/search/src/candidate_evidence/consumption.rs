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

//! Checked consumption of candidate results that must prove exhaustion.

use std::fmt;

use super::{
    search_candidate_report, SearchCandidateOutput, SearchCandidateReport, SearchCandidateRequest,
};
use crate::{
    CompressedVectorSearchMode, SearchFallbackReasonCode, SearchHit, SearchMode,
    SearchOutOfCoreMetrics, SearchResultSet, SearchRetrieverReport,
};

/// Why an otherwise valid bounded result cannot certify source exhaustion.
///
/// This is not a search execution error. The original output remains usable
/// under its existing bounded top-k contract. Diagnostics contain no query,
/// hit, metadata, or provider error text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SearchCandidateExhaustionError {
    CandidateSetNotExact,
    ResultWindowLimited,
    RankingWindowLimited,
    RetrieverUnavailable { retriever: &'static str },
    RetrieverCoverageUnproven { retriever: &'static str },
}

/// An explicitly accepted bounded result together with its original evidence.
///
/// This type is not an exhaustion proof. Use the private checked view for
/// completeness-dependent decisions before transferring a bounded result.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundedSearchCandidateOutput {
    pub result: SearchResultSet,
    pub report: SearchCandidateReport,
    pub out_of_core_metrics: Option<SearchOutOfCoreMetrics>,
}

impl fmt::Display for SearchCandidateExhaustionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CandidateSetNotExact => {
                formatter.write_str("candidate eligibility does not prove an exact set")
            }
            Self::ResultWindowLimited => {
                formatter.write_str("candidate result window does not prove source exhaustion")
            }
            Self::RankingWindowLimited => {
                formatter.write_str("candidate ranking window does not prove source exhaustion")
            }
            Self::RetrieverUnavailable { retriever } => {
                write!(formatter, "requested {retriever} retriever was unavailable")
            }
            Self::RetrieverCoverageUnproven { retriever } => {
                write!(
                    formatter,
                    "{retriever} candidate generation does not prove source exhaustion"
                )
            }
        }
    }
}

impl std::error::Error for SearchCandidateExhaustionError {}

/// Immutable evidence that all query candidates from the selected projection
/// and request filters were returned.
///
/// This does not establish canonical-store authority, projection freshness,
/// or eligibility predicates applied by a host outside this request. Use the
/// existing pinned snapshot/admission contract for those boundaries.
///
/// The view borrows its output, preventing mutation of the validated evidence
/// while the view is used. It cannot be constructed by callers.
#[must_use = "retain the checked candidate view while relying on exhaustion"]
#[derive(Clone, Copy)]
pub struct ExhaustiveSearchCandidates<'a> {
    output: &'a SearchCandidateOutput,
}

impl fmt::Debug for ExhaustiveSearchCandidates<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExhaustiveSearchCandidates")
            .field("mode", &self.output.report.mode)
            .field("hit_count", &self.output.result.hits.len())
            .finish_non_exhaustive()
    }
}

impl<'a> ExhaustiveSearchCandidates<'a> {
    pub fn hits(self) -> &'a [SearchHit] {
        &self.output.result.hits
    }

    pub fn report(self) -> &'a SearchCandidateReport {
        &self.output.report
    }
}

impl SearchCandidateOutput {
    pub(super) fn from_search_result(
        request: &SearchCandidateRequest,
        compression: CompressedVectorSearchMode,
        result: SearchResultSet,
        out_of_core_metrics: Option<SearchOutOfCoreMetrics>,
    ) -> crate::Result<Self> {
        let report = search_candidate_report(request, compression, &result);
        report.ensure_requested_exact_vector_search()?;
        Ok(Self {
            result,
            report,
            out_of_core_metrics,
        })
    }

    pub fn report(&self) -> &SearchCandidateReport {
        &self.report
    }

    pub fn out_of_core_metrics(&self) -> Option<&SearchOutOfCoreMetrics> {
        self.out_of_core_metrics.as_ref()
    }

    /// Explicitly accept incomplete candidate coverage for bounded top-k use.
    ///
    /// An empty post-filtered slice is not an exhaustive empty result. Keep
    /// `require_exhaustive` at boundaries that depend on that distinction.
    pub fn accept_bounded(&self) -> &SearchResultSet {
        &self.result
    }

    /// Transfer a bounded result after explicitly accepting incomplete coverage.
    pub fn into_bounded(self) -> BoundedSearchCandidateOutput {
        BoundedSearchCandidateOutput {
            result: self.result,
            report: self.report,
            out_of_core_metrics: self.out_of_core_metrics,
        }
    }

    /// Require an exhaustion proof before interpreting a candidate window as
    /// the complete query result, including after additional host filtering.
    ///
    /// Ordinary bounded top-k remains available through `accept_bounded`; a refusal
    /// here neither reruns retrieval nor discards that result. An exact
    /// metadata candidate set alone is insufficient: ANN candidate generation
    /// remains incomplete even when its final scores use raw vectors.
    pub fn require_exhaustive(
        &self,
    ) -> Result<ExhaustiveSearchCandidates<'_>, SearchCandidateExhaustionError> {
        use SearchCandidateExhaustionError as Refusal;

        let result = &self.result;
        if !result.candidate_set.exact {
            return Err(Refusal::CandidateSetNotExact);
        }

        // An exactly empty eligible universe needs no retriever execution.
        // A short ANN response or an empty page does not establish this fact.
        let empty_eligible_set = result.candidate_set.cardinality == 0
            && result.filtered_document_count == 0
            && result.total_hits == 0
            && result.hits.is_empty();
        if !empty_eligible_set {
            if result
                .rank_window
                .is_some_and(|window| window < result.candidate_set.cardinality)
            {
                return Err(Refusal::RankingWindowLimited);
            }
            if result.offset != 0
                || result.truncated
                || !result.truncation_reason_codes.is_empty()
                || result.total_hits != result.hits.len()
            {
                return Err(Refusal::ResultWindowLimited);
            }

            let required: &[&'static str] = match self.report.mode {
                SearchMode::Text => &["text"][..],
                SearchMode::Vector => &["vector"][..],
                SearchMode::Hybrid => &["text", "vector"][..],
            };
            for &name in required {
                let retriever = result
                    .retrievers
                    .iter()
                    .find(|retriever| retriever.name == name && retriever.available)
                    .ok_or(Refusal::RetrieverUnavailable { retriever: name })?;
                if !exhaustive_retriever(retriever) {
                    return Err(Refusal::RetrieverCoverageUnproven { retriever: name });
                }
            }
        }

        Ok(ExhaustiveSearchCandidates { output: self })
    }
}

fn exhaustive_retriever(retriever: &SearchRetrieverReport) -> bool {
    if !retriever.input_candidate_set.exact {
        return false;
    }
    match retriever.name.as_str() {
        "vector" => {
            // Raw rescoring can follow a bounded ANN candidate generation.
            retriever.candidate_score_source == "raw_vector"
                && retriever.final_score_source == "raw_vector"
        }
        "text" => {
            // Scoring retained terms exactly does not cover documents that
            // matched only query terms discarded by the lexical policy.
            !retriever
                .fallback_reason_codes
                .contains(&SearchFallbackReasonCode::TextQueryTermsReduced)
                && matches!(
                    retriever.candidate_score_source.as_str(),
                    "bm25" | "segmented_bm25"
                )
                && matches!(
                    retriever.final_score_source.as_str(),
                    "bm25" | "segmented_bm25"
                )
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests;
