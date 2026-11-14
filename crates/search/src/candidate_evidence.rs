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

//! Candidate request, result qualification, and shadow evidence ownership.
//!
//! The embedded facade remains the host API. Evidence is bound to existing
//! retrieval execution here; this module does not control production activation.

mod consumption;
mod execution;
mod probe;
mod report;
mod shadow;

pub use consumption::{
    BoundedSearchCandidateOutput, ExhaustiveSearchCandidates, SearchCandidateExhaustionError,
};
pub use probe::parse_search_candidate_shadow_probe;
pub use report::{
    advised_compressed_vector_search_mode, effective_search_candidate_mode,
    retrieval_projection_advisor_blocker_codes, retrieval_projection_advisor_json,
    search_candidate_report, RetrievalProjectionAdvisor, SearchCandidateOutput,
    SearchCandidateReadinessOptions, SearchCandidateReadinessReport, SearchCandidateReport,
    SearchCandidateRequest,
};
pub use shadow::{
    search_candidate_shadow_evidence_json, SearchCandidateFieldSummary,
    SearchCandidateFilterPushdownEvidence, SearchCandidateShadowAccumulator,
    SearchCandidateShadowEvidence,
};

pub use hawdb_evidence::replacement_contract::{
    SEARCH_CANDIDATE_EVIDENCE_ROUTE, SEARCH_CANDIDATE_EVIDENCE_SOURCE,
    SEARCH_CANDIDATE_PRIMARY_ENGINE, SEARCH_CANDIDATE_READINESS_PROTOCOL,
    SEARCH_CANDIDATE_REPORT_PROTOCOL, SEARCH_CANDIDATE_SHADOW_ENGINE,
    SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL, SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE,
    SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE, SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE,
};

#[cfg(test)]
mod boundaries;
#[cfg(test)]
mod differential;
#[cfg(test)]
#[path = "candidate_evidence/tests/probe_fixtures.rs"]
pub(crate) mod probe_fixtures;
#[cfg(test)]
mod test_report;
#[cfg(test)]
mod tests;
