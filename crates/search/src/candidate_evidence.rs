//! Candidate request, result qualification, and shadow evidence ownership.
//!
//! The embedded facade remains the host API; this module does not execute
//! database queries or control production activation.

mod probe;
mod report;
mod shadow;

pub use probe::parse_search_candidate_shadow_probe;
pub use report::{
    advised_compressed_vector_search_mode, effective_search_candidate_mode,
    nowledge_mem_search_candidate_report, retrieval_projection_advisor_blocker_codes,
    retrieval_projection_advisor_json, NowledgeMemRetrievalProjectionAdvisor,
    NowledgeMemSearchCandidateOutput, NowledgeMemSearchCandidateReadinessOptions,
    NowledgeMemSearchCandidateReadinessReport, NowledgeMemSearchCandidateReport,
    NowledgeMemSearchCandidateRequest,
};
pub use shadow::{
    nowledge_mem_search_candidate_shadow_evidence_json, NowledgeMemSearchCandidateFieldSummary,
    NowledgeMemSearchCandidateFilterPushdownEvidence, NowledgeMemSearchCandidateShadowAccumulator,
    NowledgeMemSearchCandidateShadowEvidence,
};

pub use skein_evidence::replacement_contract::{
    NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE, NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE,
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
