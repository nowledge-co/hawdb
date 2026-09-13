use super::*;
use skein_search::candidate_evidence as owner;

#[test]
fn candidate_facade_preserves_owner_types_and_entrypoints() {
    let advisor: owner::NowledgeMemRetrievalProjectionAdvisor =
        crate::nowledge_mem::NowledgeMemRetrievalProjectionAdvisor::default();
    let request: owner::NowledgeMemSearchCandidateRequest =
        crate::NowledgeMemSearchCandidateRequest::text("candidate", 4)
            .with_retrieval_projection_advisor(advisor);
    let facade: crate::NowledgeMemSearchCandidateRequest = request.clone();
    assert_eq!(facade, request);
    let options: owner::NowledgeMemSearchCandidateReadinessOptions =
        crate::NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read();
    let _: crate::NowledgeMemSearchCandidateReadinessOptions = options;
    let _: fn(
        owner::NowledgeMemSearchCandidateReport,
        &owner::NowledgeMemSearchCandidateReadinessOptions,
    ) -> crate::NowledgeMemSearchCandidateReadinessReport =
        crate::NowledgeMemSearchCandidateReadinessReport::from_candidate_report;
    let _: fn(
        &crate::NowledgeMemSearchCandidateOutput,
        &owner::NowledgeMemSearchCandidateReadinessOptions,
    ) -> owner::NowledgeMemSearchCandidateReadinessReport =
        owner::NowledgeMemSearchCandidateOutput::readiness_report;

    let mut accumulator: owner::NowledgeMemSearchCandidateShadowAccumulator =
        crate::NowledgeMemSearchCandidateShadowAccumulator::new();
    accumulator.record_compare_candidate_ids(&["candidate"], &["candidate"]);
    accumulator.record_filter_pushdown_fields(
        1,
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .copied(),
    );
    let evidence: crate::NowledgeMemSearchCandidateShadowEvidence = accumulator.evidence();
    let filter: crate::NowledgeMemSearchCandidateFilterPushdownEvidence =
        evidence.filter_pushdown.clone().unwrap();
    let _: owner::NowledgeMemSearchCandidateFilterPushdownEvidence = filter.clone();
    let _: crate::NowledgeMemSearchCandidateFieldSummary = filter.field_summaries[0].clone();
    assert_eq!(
        crate::nowledge_mem_search_candidate_shadow_evidence_json(&evidence),
        owner::nowledge_mem_search_candidate_shadow_evidence_json(&evidence)
    );
    let _: fn(&serde_json::Value) -> Result<owner::NowledgeMemSearchCandidateShadowAccumulator> =
        crate::parse_search_candidate_shadow_probe;
}
