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

use super::{
    NowledgeMemSearchCandidateOutput, NowledgeMemSearchCandidateReadinessOptions,
    NowledgeMemSearchCandidateReadinessReport, NowledgeMemSearchCandidateReport,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE, NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
};
use crate::{SearchMode, NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS};
use std::collections::{BTreeMap, BTreeSet};

const NOWLEDGE_SEARCH_CANDIDATE_VALUE_SUMMARY_FIELDS: &[&str] = &[
    "kind",
    "external_id",
    "source_id",
    "space_id",
    "unit_type",
    "lifecycle_state",
    "is_latest",
];
const NOWLEDGE_SEARCH_CANDIDATE_NUMERIC_RANGE_FIELDS: &[&str] = &["importance", "confidence"];
const NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS: &[&str] =
    &["created_at", "updated_at", "event_start", "event_end"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchCandidateShadowEvidence {
    pub request_count: u64,
    pub primary_candidate_count: u64,
    pub shadow_candidate_count: u64,
    pub matched_candidate_count: u64,
    pub primary_only_candidate_count: u64,
    pub text_retriever_available: bool,
    pub vector_retriever_available: bool,
    pub text_retriever_candidate_count: u64,
    pub vector_retriever_candidate_count: u64,
    pub fts_top_k_overlap_observed: bool,
    pub fts_top_k_overlap_ready: bool,
    pub vector_top_k_overlap_observed: bool,
    pub vector_top_k_overlap_ready: bool,
    pub source_chunk_identity_ready: bool,
    pub fail_soft_observed: bool,
    pub projection_marker_status_visible: bool,
    pub projection_watermark_ready: bool,
    pub embedding_identity_ready: bool,
    pub primary_candidate_identity_checksum: Option<u64>,
    pub shadow_candidate_identity_checksum: Option<u64>,
    pub matched_candidate_identity_checksum: Option<u64>,
    pub filter_pushdown: Option<NowledgeMemSearchCandidateFilterPushdownEvidence>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchCandidateFilterPushdownEvidence {
    pub pushed_predicate_count: u64,
    pub shadow_scan_present: bool,
    pub field_summaries: Vec<NowledgeMemSearchCandidateFieldSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchCandidateFieldSummary {
    pub field: String,
    pub source: String,
    pub segment_count: usize,
    pub value_summary_used: bool,
    pub value_summary_segment_count: usize,
    pub numeric_range_summary_used: bool,
    pub numeric_range_segment_count: usize,
    pub timestamp_range_summary_used: bool,
    pub timestamp_range_segment_count: usize,
}

impl NowledgeMemSearchCandidateFieldSummary {
    fn merge_capabilities(&mut self, other: &Self) {
        self.segment_count = self.segment_count.max(other.segment_count);
        self.value_summary_used |= other.value_summary_used;
        self.value_summary_segment_count = self
            .value_summary_segment_count
            .max(other.value_summary_segment_count);
        self.numeric_range_summary_used |= other.numeric_range_summary_used;
        self.numeric_range_segment_count = self
            .numeric_range_segment_count
            .max(other.numeric_range_segment_count);
        self.timestamp_range_summary_used |= other.timestamp_range_summary_used;
        self.timestamp_range_segment_count = self
            .timestamp_range_segment_count
            .max(other.timestamp_range_segment_count);
        if self.source != other.source {
            self.source = "merged".to_string();
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchCandidateShadowAccumulator {
    request_count: u64,
    primary_candidate_count: u64,
    shadow_candidate_count: u64,
    matched_candidate_count: u64,
    primary_only_candidate_count: u64,
    text_retriever_available: bool,
    vector_retriever_available: bool,
    text_retriever_candidate_count: u64,
    vector_retriever_candidate_count: u64,
    fts_top_k_overlap_observed: bool,
    fts_top_k_overlap_ready: bool,
    vector_top_k_overlap_observed: bool,
    vector_top_k_overlap_ready: bool,
    source_chunk_identity_ready: bool,
    fail_soft_observed: bool,
    projection_marker_status_visible: bool,
    projection_watermark_ready: bool,
    embedding_identity_ready: bool,
    primary_candidate_identity_checksum: Option<u64>,
    shadow_candidate_identity_checksum: Option<u64>,
    matched_candidate_identity_checksum: Option<u64>,
    filter_pushdown: Option<NowledgeMemSearchCandidateFilterPushdownEvidence>,
    blocker_codes: BTreeSet<String>,
}

impl NowledgeMemSearchCandidateShadowAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_compare(
        &mut self,
        primary_candidate_count: u64,
        shadow_candidate_count: u64,
        matched_candidate_count: u64,
    ) {
        self.request_count = self.request_count.saturating_add(1);
        self.primary_candidate_count = self
            .primary_candidate_count
            .saturating_add(primary_candidate_count);
        self.shadow_candidate_count = self
            .shadow_candidate_count
            .saturating_add(shadow_candidate_count);
        self.matched_candidate_count = self
            .matched_candidate_count
            .saturating_add(matched_candidate_count);
        self.primary_only_candidate_count = self
            .primary_only_candidate_count
            .saturating_add(primary_candidate_count.saturating_sub(matched_candidate_count));
        if matched_candidate_count > primary_candidate_count
            || matched_candidate_count > shadow_candidate_count
        {
            self.blocker_codes
                .insert("search_candidate_invalid_match_count".to_string());
        }
    }

    pub fn add_blocker_code(&mut self, code: impl Into<String>) {
        self.blocker_codes.insert(code.into());
    }

    pub fn record_retriever_leg(
        &mut self,
        name: impl AsRef<str>,
        available: bool,
        candidate_count: u64,
    ) {
        match name.as_ref() {
            "text" => {
                self.text_retriever_available |= available;
                self.text_retriever_candidate_count = self
                    .text_retriever_candidate_count
                    .saturating_add(candidate_count);
            }
            "vector" => {
                self.vector_retriever_available |= available;
                self.vector_retriever_candidate_count = self
                    .vector_retriever_candidate_count
                    .saturating_add(candidate_count);
            }
            _ => {
                self.blocker_codes
                    .insert("search_candidate_unknown_retriever_leg".to_string());
            }
        }
    }

    pub fn record_filter_pushdown_fields<I, S>(&mut self, pushed_predicate_count: u64, fields: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut observed_fields = self
            .filter_pushdown
            .as_ref()
            .map(|filter| {
                filter
                    .field_summaries
                    .iter()
                    .map(|summary| summary.field.clone())
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        observed_fields.extend(fields.into_iter().map(|field| field.as_ref().to_string()));
        self.filter_pushdown = Some(NowledgeMemSearchCandidateFilterPushdownEvidence {
            pushed_predicate_count: self
                .filter_pushdown
                .as_ref()
                .map(|filter| filter.pushed_predicate_count)
                .unwrap_or_default()
                .saturating_add(pushed_predicate_count),
            shadow_scan_present: self
                .filter_pushdown
                .as_ref()
                .map(|filter| filter.shadow_scan_present)
                .unwrap_or(true),
            field_summaries: observed_fields
                .into_iter()
                .map(|field| {
                    nowledge_mem_search_candidate_descriptor_contract_field_summary(&field)
                })
                .collect(),
        });
    }

    pub fn record_filter_pushdown_report(&mut self, report: &NowledgeMemSearchCandidateReport) {
        let field_summaries = if report.persisted_segment_descriptor_used {
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
                .iter()
                .map(|field| nowledge_mem_search_candidate_descriptor_contract_field_summary(field))
                .collect::<Vec<_>>()
        } else {
            report
                .candidate_set
                .metadata_predicate_pushdown
                .field_summaries
                .iter()
                .map(nowledge_mem_search_candidate_field_summary_from_pruning_report)
                .collect::<Vec<_>>()
        };
        self.record_filter_pushdown_summaries(
            report.pushed_predicate_count as u64,
            true,
            field_summaries,
        );
        if !report.persisted_segment_descriptor_used {
            self.add_blocker_code("search_candidate_segment_descriptor_not_used");
        }
        if report.residual_predicate_count > 0 {
            self.add_blocker_code("search_candidate_metadata_filter_residual");
        }
    }

    pub(crate) fn record_filter_pushdown_summaries(
        &mut self,
        pushed_predicate_count: u64,
        shadow_scan_present: bool,
        summaries: Vec<NowledgeMemSearchCandidateFieldSummary>,
    ) {
        let mut observed = self
            .filter_pushdown
            .as_ref()
            .map(|filter| {
                filter
                    .field_summaries
                    .iter()
                    .map(|summary| (summary.field.clone(), summary.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        for summary in summaries {
            observed
                .entry(summary.field.clone())
                .and_modify(|existing| existing.merge_capabilities(&summary))
                .or_insert(summary);
        }
        self.filter_pushdown = Some(NowledgeMemSearchCandidateFilterPushdownEvidence {
            pushed_predicate_count: self
                .filter_pushdown
                .as_ref()
                .map(|filter| filter.pushed_predicate_count)
                .unwrap_or_default()
                .saturating_add(pushed_predicate_count),
            shadow_scan_present: self
                .filter_pushdown
                .as_ref()
                .map(|filter| filter.shadow_scan_present && shadow_scan_present)
                .unwrap_or(shadow_scan_present),
            field_summaries: observed.into_values().collect(),
        });
    }

    pub fn record_compare_candidate_ids(
        &mut self,
        primary_candidate_ids: &[impl AsRef<str>],
        shadow_candidate_ids: &[impl AsRef<str>],
    ) {
        let primary = primary_candidate_ids
            .iter()
            .map(|id| id.as_ref().to_string())
            .collect::<BTreeSet<_>>();
        let shadow = shadow_candidate_ids
            .iter()
            .map(|id| id.as_ref().to_string())
            .collect::<BTreeSet<_>>();
        let matched = primary
            .intersection(&shadow)
            .cloned()
            .collect::<BTreeSet<_>>();
        self.record_compare(
            primary.len() as u64,
            shadow.len() as u64,
            matched.len() as u64,
        );
        update_search_candidate_identity_checksum(
            &mut self.primary_candidate_identity_checksum,
            &primary,
        );
        update_search_candidate_identity_checksum(
            &mut self.shadow_candidate_identity_checksum,
            &shadow,
        );
        update_search_candidate_identity_checksum(
            &mut self.matched_candidate_identity_checksum,
            &matched,
        );
    }

    pub fn record_search_candidate_output<I, S>(
        &mut self,
        primary_candidate_ids: I,
        shadow_output: &NowledgeMemSearchCandidateOutput,
    ) where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let primary_candidate_ids = primary_candidate_ids
            .into_iter()
            .map(|id| id.as_ref().to_string())
            .collect::<Vec<_>>();
        let shadow_candidate_ids = shadow_output
            .result
            .hits
            .iter()
            .map(|hit| hit.id.clone())
            .collect::<Vec<_>>();
        self.record_compare_candidate_ids(&primary_candidate_ids, &shadow_candidate_ids);
        self.record_top_k_overlap_candidate_ids(
            shadow_output.report.mode,
            &primary_candidate_ids,
            &shadow_candidate_ids,
        );
        self.record_retriever_leg_report(&shadow_output.report);
        self.record_filter_pushdown_report(&shadow_output.report);
        self.record_candidate_readiness_report(&shadow_output.readiness_report(
            &NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read(),
        ));
    }

    pub fn record_candidate_readiness_report(
        &mut self,
        report: &NowledgeMemSearchCandidateReadinessReport,
    ) {
        self.record_candidate_readiness_signals(
            report.source_chunk_identity_ready,
            report.fail_soft_observed,
            report.projection_marker_status_visible,
            report.projection_watermark_ready,
            report.embedding_identity_ready,
        );
    }

    pub fn record_candidate_readiness_signals(
        &mut self,
        source_chunk_identity_ready: bool,
        fail_soft_observed: bool,
        projection_marker_status_visible: bool,
        projection_watermark_ready: bool,
        embedding_identity_ready: bool,
    ) {
        self.source_chunk_identity_ready |= source_chunk_identity_ready;
        self.fail_soft_observed |= fail_soft_observed;
        self.projection_marker_status_visible |= projection_marker_status_visible;
        self.projection_watermark_ready |= projection_watermark_ready;
        self.embedding_identity_ready |= embedding_identity_ready;
    }

    pub fn record_top_k_overlap_candidate_ids(
        &mut self,
        mode: SearchMode,
        primary_candidate_ids: &[impl AsRef<str>],
        shadow_candidate_ids: &[impl AsRef<str>],
    ) {
        let primary_candidate_ids = primary_candidate_ids
            .iter()
            .map(|id| id.as_ref().to_string())
            .collect::<Vec<_>>();
        let shadow_candidate_ids = shadow_candidate_ids
            .iter()
            .map(|id| id.as_ref().to_string())
            .collect::<Vec<_>>();
        self.record_top_k_overlap(mode, &primary_candidate_ids, &shadow_candidate_ids);
    }

    fn record_top_k_overlap(
        &mut self,
        mode: SearchMode,
        primary_candidate_ids: &[String],
        shadow_candidate_ids: &[String],
    ) {
        let ready = !primary_candidate_ids.is_empty()
            && primary_candidate_ids.len() == shadow_candidate_ids.len()
            && primary_candidate_ids
                .iter()
                .zip(shadow_candidate_ids)
                .all(|(primary, shadow)| primary == shadow);
        match mode {
            SearchMode::Text => {
                self.fts_top_k_overlap_observed = true;
                self.fts_top_k_overlap_ready |= ready;
            }
            SearchMode::Vector => {
                self.vector_top_k_overlap_observed = true;
                self.vector_top_k_overlap_ready |= ready;
            }
            SearchMode::Hybrid => {}
        }
    }

    fn record_retriever_leg_report(&mut self, report: &NowledgeMemSearchCandidateReport) {
        self.record_retriever_leg(
            "text",
            report
                .retriever_available
                .get("text")
                .copied()
                .unwrap_or(false),
            retriever_candidate_count(report, "text"),
        );
        self.record_retriever_leg(
            "vector",
            report
                .retriever_available
                .get("vector")
                .copied()
                .unwrap_or(false),
            retriever_candidate_count(report, "vector"),
        );
    }

    pub fn evidence(&self) -> NowledgeMemSearchCandidateShadowEvidence {
        NowledgeMemSearchCandidateShadowEvidence {
            request_count: self.request_count,
            primary_candidate_count: self.primary_candidate_count,
            shadow_candidate_count: self.shadow_candidate_count,
            matched_candidate_count: self.matched_candidate_count,
            primary_only_candidate_count: self.primary_only_candidate_count,
            text_retriever_available: self.text_retriever_available,
            vector_retriever_available: self.vector_retriever_available,
            text_retriever_candidate_count: self.text_retriever_candidate_count,
            vector_retriever_candidate_count: self.vector_retriever_candidate_count,
            fts_top_k_overlap_observed: self.fts_top_k_overlap_observed,
            fts_top_k_overlap_ready: self.fts_top_k_overlap_observed
                && self.fts_top_k_overlap_ready,
            vector_top_k_overlap_observed: self.vector_top_k_overlap_observed,
            vector_top_k_overlap_ready: self.vector_top_k_overlap_observed
                && self.vector_top_k_overlap_ready,
            source_chunk_identity_ready: self.source_chunk_identity_ready,
            fail_soft_observed: self.fail_soft_observed,
            projection_marker_status_visible: self.projection_marker_status_visible,
            projection_watermark_ready: self.projection_watermark_ready,
            embedding_identity_ready: self.embedding_identity_ready,
            primary_candidate_identity_checksum: self.primary_candidate_identity_checksum,
            shadow_candidate_identity_checksum: self.shadow_candidate_identity_checksum,
            matched_candidate_identity_checksum: self.matched_candidate_identity_checksum,
            filter_pushdown: self.filter_pushdown.clone(),
            blocker_codes: self.blocker_codes.iter().cloned().collect(),
        }
    }

    pub fn json(&self) -> serde_json::Value {
        self.evidence().json()
    }
}

impl NowledgeMemSearchCandidateShadowEvidence {
    pub fn ready(
        request_count: u64,
        primary_candidate_count: u64,
        shadow_candidate_count: u64,
        matched_candidate_count: u64,
    ) -> Self {
        Self {
            request_count,
            primary_candidate_count,
            shadow_candidate_count,
            matched_candidate_count,
            primary_only_candidate_count: 0,
            text_retriever_available: false,
            vector_retriever_available: false,
            text_retriever_candidate_count: 0,
            vector_retriever_candidate_count: 0,
            fts_top_k_overlap_observed: false,
            fts_top_k_overlap_ready: false,
            vector_top_k_overlap_observed: false,
            vector_top_k_overlap_ready: false,
            source_chunk_identity_ready: false,
            fail_soft_observed: false,
            projection_marker_status_visible: false,
            projection_watermark_ready: false,
            embedding_identity_ready: false,
            primary_candidate_identity_checksum: None,
            shadow_candidate_identity_checksum: None,
            matched_candidate_identity_checksum: None,
            filter_pushdown: None,
            blocker_codes: Vec::new(),
        }
    }

    pub fn json(&self) -> serde_json::Value {
        nowledge_mem_search_candidate_shadow_evidence_json(self)
    }
}

pub fn nowledge_mem_search_candidate_shadow_evidence_json(
    evidence: &NowledgeMemSearchCandidateShadowEvidence,
) -> serde_json::Value {
    let blocker_codes = nowledge_mem_search_candidate_shadow_blocker_codes(evidence);
    let candidate_identity = nowledge_mem_search_candidate_shadow_identity_json(evidence);
    let filter_pushdown = nowledge_mem_search_candidate_filter_pushdown_json(evidence);
    let ready = blocker_codes.is_empty();
    let row_count_parity = evidence.request_count > 0
        && evidence.primary_candidate_count == evidence.shadow_candidate_count
        && evidence.matched_candidate_count == evidence.shadow_candidate_count
        && evidence.primary_only_candidate_count == 0;
    let shadow_scan_filter_pushdown_ready = filter_pushdown
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let shadow_scan_field_pruning_ready = filter_pushdown
        .get("field_capabilities_ready")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        && filter_pushdown
            .get("missing_required_fields")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty);
    let shadow_scan_field_summary_count = filter_pushdown
        .get("field_summary_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    serde_json::json!({
        "protocol": NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        "route": NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
        "evidence_source": NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
        "engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
        "ready": ready,
        "candidate_primary_engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
        "request_count": evidence.request_count,
        "primary_candidate_count": evidence.primary_candidate_count,
        "shadow_candidate_count": evidence.shadow_candidate_count,
        "matched_candidate_count": evidence.matched_candidate_count,
        "primary_only_candidate_count": evidence.primary_only_candidate_count,
        "row_count_parity": row_count_parity,
        "text_retriever_ready": evidence.text_retriever_available
            && evidence.text_retriever_candidate_count > 0,
        "vector_retriever_ready": evidence.vector_retriever_available
            && evidence.vector_retriever_candidate_count > 0,
        "fts_top_k_overlap_ready": evidence.fts_top_k_overlap_ready,
        "vector_top_k_overlap_ready": evidence.vector_top_k_overlap_ready,
        "top_k_overlap_observed": {
            "fts": evidence.fts_top_k_overlap_observed,
            "vector": evidence.vector_top_k_overlap_observed,
        },
        "candidate_readiness": {
            "source_chunk_identity_ready": evidence.source_chunk_identity_ready,
            "fail_soft_observed": evidence.fail_soft_observed,
            "projection_marker_status_visible": evidence.projection_marker_status_visible,
            "projection_watermark_ready": evidence.projection_watermark_ready,
            "embedding_identity_ready": evidence.embedding_identity_ready,
        },
        "retriever_leg_candidate_counts": {
            "text": evidence.text_retriever_candidate_count,
            "vector": evidence.vector_retriever_candidate_count,
        },
        "candidate_identity": candidate_identity,
        "shadow_scan_present": evidence
            .filter_pushdown
            .as_ref()
            .map(|filter| filter.shadow_scan_present)
            .unwrap_or(false),
        "shadow_scan_filter_pushdown_ready": shadow_scan_filter_pushdown_ready,
        "shadow_scan_field_pruning_ready": shadow_scan_field_pruning_ready,
        "shadow_scan_field_summary_count": shadow_scan_field_summary_count,
        "filter_pushdown_ready": filter_pushdown
            .get("ready")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        "filter_pushdown": filter_pushdown,
        "blocker_codes": blocker_codes,
    })
}

fn retriever_candidate_count(report: &NowledgeMemSearchCandidateReport, name: &str) -> u64 {
    report
        .retriever_candidate_counts
        .get(name)
        .copied()
        .unwrap_or_default() as u64
}

fn nowledge_mem_search_candidate_shadow_identity_json(
    evidence: &NowledgeMemSearchCandidateShadowEvidence,
) -> serde_json::Value {
    let primary_checksum = evidence.primary_candidate_identity_checksum;
    let shadow_checksum = evidence.shadow_candidate_identity_checksum;
    let matched_checksum = evidence.matched_candidate_identity_checksum;
    let parity = primary_checksum.is_some()
        && primary_checksum == shadow_checksum
        && matched_checksum == shadow_checksum;
    serde_json::json!({
        "ready": parity,
        "id_space": "search_candidate_id",
        "representation": "per_request_sorted_candidate_ids",
        "primary_checksum": primary_checksum,
        "shadow_checksum": shadow_checksum,
        "matched_checksum": matched_checksum,
        "parity": parity,
    })
}

fn nowledge_mem_search_candidate_shadow_blocker_codes(
    evidence: &NowledgeMemSearchCandidateShadowEvidence,
) -> Vec<String> {
    let mut blockers = evidence
        .blocker_codes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if evidence.request_count == 0 {
        blockers.insert("search_candidate_shadow_no_requests".to_string());
    }
    if evidence.primary_candidate_count != evidence.shadow_candidate_count
        || evidence.matched_candidate_count != evidence.shadow_candidate_count
    {
        blockers.insert("search_candidate_mismatch".to_string());
    }
    if evidence.primary_only_candidate_count != 0 {
        blockers.insert("search_candidate_primary_only".to_string());
    }
    let identity_ready = evidence.primary_candidate_identity_checksum.is_some()
        && evidence.primary_candidate_identity_checksum
            == evidence.shadow_candidate_identity_checksum
        && evidence.matched_candidate_identity_checksum
            == evidence.shadow_candidate_identity_checksum;
    if evidence.primary_candidate_identity_checksum.is_none()
        || evidence.shadow_candidate_identity_checksum.is_none()
        || evidence.matched_candidate_identity_checksum.is_none()
    {
        blockers.insert("search_candidate_identity_missing".to_string());
    } else if !identity_ready {
        blockers.insert("search_candidate_identity_mismatch".to_string());
    }
    blockers.extend(nowledge_mem_search_candidate_filter_pushdown_blockers(
        evidence,
    ));
    blockers.into_iter().collect()
}

fn nowledge_mem_search_candidate_filter_pushdown_json(
    evidence: &NowledgeMemSearchCandidateShadowEvidence,
) -> serde_json::Value {
    let blocker_codes = nowledge_mem_search_candidate_filter_pushdown_blockers(evidence);
    let missing_required_fields =
        nowledge_mem_search_candidate_missing_filter_fields(evidence.filter_pushdown.as_ref());
    let missing_value_summary_fields = nowledge_mem_search_candidate_missing_capability_fields(
        evidence.filter_pushdown.as_ref(),
        NOWLEDGE_SEARCH_CANDIDATE_VALUE_SUMMARY_FIELDS,
        CandidateFieldCapability::Value,
    );
    let missing_numeric_range_fields = nowledge_mem_search_candidate_missing_capability_fields(
        evidence.filter_pushdown.as_ref(),
        NOWLEDGE_SEARCH_CANDIDATE_NUMERIC_RANGE_FIELDS,
        CandidateFieldCapability::NumericRange,
    );
    let missing_timestamp_range_fields = nowledge_mem_search_candidate_missing_capability_fields(
        evidence.filter_pushdown.as_ref(),
        NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS,
        CandidateFieldCapability::TimestampRange,
    );
    let field_summaries = evidence
        .filter_pushdown
        .as_ref()
        .map(|filter| {
            filter
                .field_summaries
                .iter()
                .map(|summary| {
                    serde_json::json!({
                        "field": summary.field,
                        "source": summary.source,
                        "segment_count": summary.segment_count,
                        "value_summary_used": summary.value_summary_used,
                        "value_summary_segment_count": summary.value_summary_segment_count,
                        "numeric_range_summary_used": summary.numeric_range_summary_used,
                        "numeric_range_segment_count": summary.numeric_range_segment_count,
                        "timestamp_range_summary_used": summary.timestamp_range_summary_used,
                        "timestamp_range_segment_count": summary.timestamp_range_segment_count,
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    serde_json::json!({
        "ready": blocker_codes.is_empty(),
        "pushed_predicate_count": evidence
            .filter_pushdown
            .as_ref()
            .map(|filter| filter.pushed_predicate_count),
        "shadow_scan_present": evidence
            .filter_pushdown
            .as_ref()
            .map(|filter| filter.shadow_scan_present),
        "required_fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
        "missing_required_fields": missing_required_fields,
        "missing_value_summary_fields": missing_value_summary_fields,
        "missing_numeric_range_fields": missing_numeric_range_fields,
        "missing_timestamp_range_fields": missing_timestamp_range_fields,
        "field_capabilities_ready": missing_value_summary_fields.is_empty()
            && missing_numeric_range_fields.is_empty()
            && missing_timestamp_range_fields.is_empty(),
        "field_summary_count": field_summaries.len(),
        "field_summaries": field_summaries,
        "blocker_codes": blocker_codes,
    })
}

fn nowledge_mem_search_candidate_filter_pushdown_blockers(
    evidence: &NowledgeMemSearchCandidateShadowEvidence,
) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    let Some(filter_pushdown) = evidence.filter_pushdown.as_ref() else {
        return vec!["search_candidate_filter_pushdown_missing".to_string()];
    };
    if filter_pushdown.pushed_predicate_count == 0 {
        blockers.insert("search_candidate_filter_pushdown_no_predicates".to_string());
    }
    if !filter_pushdown.shadow_scan_present {
        blockers.insert("search_candidate_shadow_scan_missing".to_string());
    }
    let missing_required_fields =
        nowledge_mem_search_candidate_missing_filter_fields(Some(filter_pushdown));
    if !missing_required_fields.is_empty() {
        blockers.insert("search_candidate_field_pruning_missing".to_string());
    }
    let missing_value_summary_fields = nowledge_mem_search_candidate_missing_capability_fields(
        Some(filter_pushdown),
        NOWLEDGE_SEARCH_CANDIDATE_VALUE_SUMMARY_FIELDS,
        CandidateFieldCapability::Value,
    );
    let missing_numeric_range_fields = nowledge_mem_search_candidate_missing_capability_fields(
        Some(filter_pushdown),
        NOWLEDGE_SEARCH_CANDIDATE_NUMERIC_RANGE_FIELDS,
        CandidateFieldCapability::NumericRange,
    );
    let missing_timestamp_range_fields = nowledge_mem_search_candidate_missing_capability_fields(
        Some(filter_pushdown),
        NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS,
        CandidateFieldCapability::TimestampRange,
    );
    if !missing_value_summary_fields.is_empty()
        || !missing_numeric_range_fields.is_empty()
        || !missing_timestamp_range_fields.is_empty()
    {
        blockers.insert("search_candidate_field_pruning_capability_missing".to_string());
    }
    blockers.into_iter().collect()
}

fn nowledge_mem_search_candidate_missing_filter_fields(
    filter_pushdown: Option<&NowledgeMemSearchCandidateFilterPushdownEvidence>,
) -> Vec<&'static str> {
    let observed_fields = filter_pushdown
        .map(|filter| {
            filter
                .field_summaries
                .iter()
                .map(|summary| summary.field.as_str())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .copied()
        .filter(|field| !observed_fields.contains(field))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateFieldCapability {
    Value,
    NumericRange,
    TimestampRange,
}

fn nowledge_mem_search_candidate_missing_capability_fields(
    filter_pushdown: Option<&NowledgeMemSearchCandidateFilterPushdownEvidence>,
    required_fields: &'static [&'static str],
    capability: CandidateFieldCapability,
) -> Vec<&'static str> {
    let summaries = filter_pushdown
        .map(|filter| filter.field_summaries.as_slice())
        .unwrap_or(&[]);
    required_fields
        .iter()
        .copied()
        .filter(|field| {
            !summaries.iter().any(|summary| {
                summary.field == *field && candidate_field_has_capability(summary, capability)
            })
        })
        .collect()
}

fn candidate_field_has_capability(
    summary: &NowledgeMemSearchCandidateFieldSummary,
    capability: CandidateFieldCapability,
) -> bool {
    match capability {
        CandidateFieldCapability::Value => {
            summary.value_summary_used && summary.value_summary_segment_count > 0
        }
        CandidateFieldCapability::NumericRange => {
            summary.numeric_range_summary_used && summary.numeric_range_segment_count > 0
        }
        CandidateFieldCapability::TimestampRange => {
            (summary.timestamp_range_summary_used && summary.timestamp_range_segment_count > 0)
                || (summary.numeric_range_summary_used && summary.numeric_range_segment_count > 0)
        }
    }
}

fn nowledge_mem_search_candidate_descriptor_contract_field_summary(
    field: &str,
) -> NowledgeMemSearchCandidateFieldSummary {
    NowledgeMemSearchCandidateFieldSummary {
        field: field.to_string(),
        source: "persisted_segment_descriptor_contract".to_string(),
        segment_count: 1,
        value_summary_used: NOWLEDGE_SEARCH_CANDIDATE_VALUE_SUMMARY_FIELDS.contains(&field),
        value_summary_segment_count: usize::from(
            NOWLEDGE_SEARCH_CANDIDATE_VALUE_SUMMARY_FIELDS.contains(&field),
        ),
        numeric_range_summary_used: NOWLEDGE_SEARCH_CANDIDATE_NUMERIC_RANGE_FIELDS.contains(&field)
            || NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS.contains(&field),
        numeric_range_segment_count: usize::from(
            NOWLEDGE_SEARCH_CANDIDATE_NUMERIC_RANGE_FIELDS.contains(&field)
                || NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS.contains(&field),
        ),
        timestamp_range_summary_used: NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS
            .contains(&field),
        timestamp_range_segment_count: usize::from(
            NOWLEDGE_SEARCH_CANDIDATE_TIMESTAMP_RANGE_FIELDS.contains(&field),
        ),
    }
}

fn nowledge_mem_search_candidate_field_summary_from_pruning_report(
    report: &crate::SearchPredicateFieldPruningReport,
) -> NowledgeMemSearchCandidateFieldSummary {
    NowledgeMemSearchCandidateFieldSummary {
        field: report.field.clone(),
        source: "search_predicate_pruning_report".to_string(),
        segment_count: report.segment_count,
        value_summary_used: report.value_summary_used,
        value_summary_segment_count: usize::from(report.value_summary_used) * report.segment_count,
        numeric_range_summary_used: report.numeric_range_summary_used,
        numeric_range_segment_count: usize::from(report.numeric_range_summary_used)
            * report.segment_count,
        timestamp_range_summary_used: report.timestamp_range_summary_used,
        timestamp_range_segment_count: usize::from(report.timestamp_range_summary_used)
            * report.segment_count,
    }
}

fn update_search_candidate_identity_checksum(
    checksum: &mut Option<u64>,
    candidate_ids: &BTreeSet<String>,
) {
    let mut value = checksum.unwrap_or(FNV64_OFFSET);
    value = fnv64_update(value, b"request\n");
    for candidate_id in candidate_ids {
        value = fnv64_update(value, candidate_id.as_bytes());
        value = fnv64_update(value, b"\0");
    }
    *checksum = Some(value);
}

const FNV64_OFFSET: u64 = 0xcbf29ce484222325;
const FNV64_PRIME: u64 = 0x100000001b3;

fn fnv64_update(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV64_PRIME);
    }
    hash
}
