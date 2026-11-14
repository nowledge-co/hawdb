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

use super::*;
use hawdb_search::candidate_evidence as owner;

#[test]
fn candidate_facade_preserves_owner_types_and_entrypoints() {
    let advisor: owner::RetrievalProjectionAdvisor =
        crate::embedded_store::RetrievalProjectionAdvisor::default();
    let request: owner::SearchCandidateRequest =
        crate::SearchCandidateRequest::text("candidate", 4)
            .with_retrieval_projection_advisor(advisor);
    let facade: crate::SearchCandidateRequest = request.clone();
    assert_eq!(facade, request);
    let options: owner::SearchCandidateReadinessOptions =
        crate::SearchCandidateReadinessOptions::lancedb_replacement_candidate_read();
    let _: crate::SearchCandidateReadinessOptions = options;
    let _: fn(
        owner::SearchCandidateReport,
        &owner::SearchCandidateReadinessOptions,
    ) -> crate::SearchCandidateReadinessReport =
        crate::SearchCandidateReadinessReport::from_candidate_report;
    let _: fn(
        &crate::SearchCandidateOutput,
        &owner::SearchCandidateReadinessOptions,
    ) -> owner::SearchCandidateReadinessReport = owner::SearchCandidateOutput::readiness_report;

    let mut accumulator: owner::SearchCandidateShadowAccumulator =
        crate::SearchCandidateShadowAccumulator::new();
    accumulator.record_compare_candidate_ids(&["candidate"], &["candidate"]);
    accumulator.record_filter_pushdown_fields(
        1,
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .copied(),
    );
    let evidence: crate::SearchCandidateShadowEvidence = accumulator.evidence();
    let filter: crate::SearchCandidateFilterPushdownEvidence =
        evidence.filter_pushdown.clone().unwrap();
    let _: owner::SearchCandidateFilterPushdownEvidence = filter.clone();
    let _: crate::SearchCandidateFieldSummary = filter.field_summaries[0].clone();
    assert_eq!(
        crate::search_candidate_shadow_evidence_json(&evidence),
        owner::search_candidate_shadow_evidence_json(&evidence)
    );
    let _: fn(&serde_json::Value) -> Result<owner::SearchCandidateShadowAccumulator> =
        crate::parse_search_candidate_shadow_probe;
}
