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
    NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL,
};
use crate::{
    CompressedVectorSearchMode, SearchCandidateSetReport, SearchFusionWeights, SearchMode,
    SearchOutOfCoreMetrics, SearchResultSet, VectorRecallValidationReport,
    VECTOR_RECALL_VALIDATION_PROTOCOL,
};
use hawdb_optimizer::AdaptiveVectorBackendPolicy;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NowledgeMemRetrievalProjectionAdvisor {
    pub recall_evidence_ready: bool,
    pub recall_evidence_protocol: Option<String>,
    pub recall_sample_count: usize,
    pub recall_at_k_per_million: Option<u32>,
    pub parity_evidence_ready: bool,
    pub cold_or_constrained_local_segment: bool,
}

impl NowledgeMemRetrievalProjectionAdvisor {
    pub fn cold_local_with_recall_parity(report: &VectorRecallValidationReport) -> Self {
        Self {
            recall_evidence_ready: report.validates_required_approximate_backend(),
            recall_evidence_protocol: Some(report.protocol.clone()),
            recall_sample_count: report.executed_sample_count,
            recall_at_k_per_million: Some(report.recall_at_k_per_million),
            parity_evidence_ready: true,
            cold_or_constrained_local_segment: true,
        }
    }

    pub fn ready(&self) -> bool {
        self.recall_evidence_present()
            && self.recall_evidence_ready
            && self.parity_evidence_ready
            && self.cold_or_constrained_local_segment
    }

    fn recall_evidence_present(&self) -> bool {
        self.recall_evidence_protocol.as_deref() == Some(VECTOR_RECALL_VALIDATION_PROTOCOL)
            && self.recall_sample_count > 0
    }

    fn blocker_codes(&self) -> Vec<String> {
        let mut blockers = Vec::new();
        if !self.recall_evidence_present() {
            blockers.push("retrieval_projection_recall_evidence_missing".to_string());
        } else if !self.recall_evidence_ready {
            blockers.push("retrieval_projection_recall_evidence_not_ready".to_string());
        }
        if !self.parity_evidence_ready {
            blockers.push("retrieval_projection_parity_evidence_missing".to_string());
        }
        if !self.cold_or_constrained_local_segment {
            blockers.push("retrieval_projection_segment_not_advised".to_string());
        }
        blockers
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "recall_evidence_ready": self.recall_evidence_ready,
            "recall_evidence_protocol": self.recall_evidence_protocol,
            "recall_sample_count": self.recall_sample_count,
            "recall_at_k_per_million": self.recall_at_k_per_million,
            "parity_evidence_ready": self.parity_evidence_ready,
            "cold_or_constrained_local_segment": self.cold_or_constrained_local_segment,
            "blocker_codes": self.blocker_codes(),
        })
    }
}

pub fn advised_compressed_vector_search_mode(
    requested: CompressedVectorSearchMode,
    advisor: &NowledgeMemRetrievalProjectionAdvisor,
) -> CompressedVectorSearchMode {
    if requested == CompressedVectorSearchMode::Disabled || advisor.ready() {
        requested
    } else {
        CompressedVectorSearchMode::Disabled
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemSearchCandidateRequest {
    pub query_text: String,
    pub query_embedding: Option<Vec<f32>>,
    pub mode: SearchMode,
    pub limit: usize,
    pub offset: usize,
    pub rank_window: Option<usize>,
    pub fusion_weights: SearchFusionWeights,
    pub metadata_filters: BTreeMap<String, String>,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub adaptive_vector_backend_policy: AdaptiveVectorBackendPolicy,
    pub recall_validation_probe: bool,
    pub retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor,
}

impl NowledgeMemSearchCandidateRequest {
    pub fn text(query_text: impl Into<String>, limit: usize) -> Self {
        Self {
            query_text: query_text.into(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit,
            offset: 0,
            rank_window: None,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            adaptive_vector_backend_policy: AdaptiveVectorBackendPolicy::default(),
            recall_validation_probe: false,
            retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor::default(),
        }
    }

    pub fn vector(query_embedding: Vec<f32>, limit: usize) -> Self {
        Self {
            query_text: String::new(),
            query_embedding: Some(query_embedding),
            mode: SearchMode::Vector,
            limit,
            offset: 0,
            rank_window: None,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            adaptive_vector_backend_policy: AdaptiveVectorBackendPolicy::default(),
            recall_validation_probe: false,
            retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor::default(),
        }
    }

    pub fn hybrid(query_text: impl Into<String>, query_embedding: Vec<f32>, limit: usize) -> Self {
        Self {
            query_text: query_text.into(),
            query_embedding: Some(query_embedding),
            mode: SearchMode::Hybrid,
            limit,
            offset: 0,
            rank_window: None,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            adaptive_vector_backend_policy: AdaptiveVectorBackendPolicy::default(),
            recall_validation_probe: false,
            retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor::default(),
        }
    }

    pub fn with_rank_window(mut self, rank_window: Option<usize>) -> Self {
        self.rank_window = rank_window;
        self
    }

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    pub fn with_fusion_weights(mut self, fusion_weights: SearchFusionWeights) -> Self {
        self.fusion_weights = fusion_weights;
        self
    }

    pub fn with_metadata_filters(mut self, metadata_filters: BTreeMap<String, String>) -> Self {
        self.metadata_filters = metadata_filters;
        self
    }

    pub fn with_compressed_vector_search_mode(
        mut self,
        compressed_vector_search_mode: CompressedVectorSearchMode,
    ) -> Self {
        self.compressed_vector_search_mode = compressed_vector_search_mode;
        self
    }

    pub fn with_adaptive_vector_backend_policy(
        mut self,
        policy: AdaptiveVectorBackendPolicy,
    ) -> Self {
        self.adaptive_vector_backend_policy = policy;
        self
    }

    pub fn as_recall_validation_probe(mut self) -> Self {
        self.recall_validation_probe = true;
        self
    }

    pub fn with_retrieval_projection_advisor(
        mut self,
        advisor: NowledgeMemRetrievalProjectionAdvisor,
    ) -> Self {
        self.retrieval_projection_advisor = advisor;
        self
    }

    fn effective_compressed_vector_search_mode(&self) -> CompressedVectorSearchMode {
        advised_compressed_vector_search_mode(
            self.compressed_vector_search_mode,
            &self.retrieval_projection_advisor,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemSearchCandidateReport {
    pub protocol: String,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub requested_compressed_vector_search_mode: CompressedVectorSearchMode,
    pub retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor,
    pub retrieval_projection_advisor_blocker_codes: Vec<String>,
    pub mode: SearchMode,
    pub query_embedding_dimension: Option<usize>,
    pub limit: usize,
    pub offset: usize,
    pub rank_window: Option<usize>,
    pub document_count: usize,
    pub filtered_document_count: usize,
    pub total_hits: usize,
    pub returned_hit_count: usize,
    pub returned_kind_counts: BTreeMap<String, usize>,
    pub returned_missing_external_id_count: usize,
    pub returned_missing_source_id_count: usize,
    pub truncated: bool,
    pub candidate_set: SearchCandidateSetReport,
    pub filtered_out_count: usize,
    pub metadata_filter_count: usize,
    pub pushed_predicate_count: usize,
    pub residual_predicate_count: usize,
    pub segment_count: usize,
    pub pruned_segment_count: usize,
    pub scanned_segment_count: usize,
    pub segment_pruning_candidate_document_count: usize,
    pub segment_pruned_document_count: usize,
    pub segment_scanned_document_count: usize,
    pub persisted_segment_descriptor_used: bool,
    pub physical_range_read_count: usize,
    pub physical_bytes_read: u64,
    pub retriever_backends: BTreeMap<String, String>,
    pub retriever_backend_selection_reasons: BTreeMap<String, String>,
    pub retriever_estimated_raw_vector_bytes: BTreeMap<String, u64>,
    pub retriever_filter_selectivity_per_million: BTreeMap<String, u32>,
    pub retriever_available: BTreeMap<String, bool>,
    pub retriever_candidate_counts: BTreeMap<String, usize>,
    pub retriever_candidate_score_sources: BTreeMap<String, String>,
    pub retriever_final_score_sources: BTreeMap<String, String>,
    pub fallback_reason_codes: Vec<String>,
    pub empty_reason_codes: Vec<String>,
    pub truncation_reason_codes: Vec<String>,
    pub projection_full_reindex_needed: bool,
    pub projection_metadata_repair_needed: bool,
    pub projection_source_graph_commit_epoch: Option<u64>,
    pub projection_durable_source_graph_commit_epoch: Option<u64>,
    pub projection_embedding_model: Option<String>,
    pub projection_embedding_version: Option<String>,
    pub projection_embedding_dimension: Option<usize>,
}

impl NowledgeMemSearchCandidateReport {
    pub fn json(&self) -> serde_json::Value {
        let mut value = serde_json::json!({
            "protocol": self.protocol,
            "compressed_vector_search_mode": self.compressed_vector_search_mode.as_str(),
            "requested_compressed_vector_search_mode": self.requested_compressed_vector_search_mode.as_str(),
            "retrieval_projection_advisor": self.retrieval_projection_advisor.json(),
            "retrieval_projection_advisor_blocker_codes": self.retrieval_projection_advisor_blocker_codes,
            "mode": search_mode_name(self.mode),
            "query_embedding_dimension": self.query_embedding_dimension,
            "limit": self.limit,
            "rank_window": self.rank_window,
            "document_count": self.document_count,
            "filtered_document_count": self.filtered_document_count,
            "total_hits": self.total_hits,
            "returned_hit_count": self.returned_hit_count,
            "returned_kind_counts": self.returned_kind_counts,
            "returned_missing_external_id_count": self.returned_missing_external_id_count,
            "returned_missing_source_id_count": self.returned_missing_source_id_count,
            "truncated": self.truncated,
            "candidate_set": search_candidate_set_report_json(&self.candidate_set),
            "filtered_out_count": self.filtered_out_count,
            "metadata_filter_count": self.metadata_filter_count,
            "pushed_predicate_count": self.pushed_predicate_count,
            "residual_predicate_count": self.residual_predicate_count,
            "segment_count": self.segment_count,
            "pruned_segment_count": self.pruned_segment_count,
            "scanned_segment_count": self.scanned_segment_count,
            "segment_pruning_candidate_document_count": self.segment_pruning_candidate_document_count,
            "segment_pruned_document_count": self.segment_pruned_document_count,
            "segment_scanned_document_count": self.segment_scanned_document_count,
            "persisted_segment_descriptor_used": self.persisted_segment_descriptor_used,
            "retriever_backends": self.retriever_backends,
            "retriever_available": self.retriever_available,
            "retriever_candidate_counts": self.retriever_candidate_counts,
            "fallback_reason_codes": self.fallback_reason_codes,
            "empty_reason_codes": self.empty_reason_codes,
            "truncation_reason_codes": self.truncation_reason_codes,
            "projection_full_reindex_needed": self.projection_full_reindex_needed,
            "projection_metadata_repair_needed": self.projection_metadata_repair_needed,
            "projection_source_graph_commit_epoch": self.projection_source_graph_commit_epoch,
            "projection_embedding_model": self.projection_embedding_model,
            "projection_embedding_version": self.projection_embedding_version,
            "projection_embedding_dimension": self.projection_embedding_dimension,
        });
        let object = value.as_object_mut().expect("report JSON is an object");
        object.insert("offset".to_string(), serde_json::json!(self.offset));
        object.insert(
            "projection_durable_source_graph_commit_epoch".to_string(),
            serde_json::json!(self.projection_durable_source_graph_commit_epoch),
        );
        object.insert(
            "physical_range_read_count".to_string(),
            serde_json::json!(self.physical_range_read_count),
        );
        object.insert(
            "physical_bytes_read".to_string(),
            serde_json::json!(self.physical_bytes_read),
        );
        object.insert(
            "retriever_backend_selection_reasons".to_string(),
            serde_json::json!(self.retriever_backend_selection_reasons),
        );
        object.insert(
            "retriever_estimated_raw_vector_bytes".to_string(),
            serde_json::json!(self.retriever_estimated_raw_vector_bytes),
        );
        object.insert(
            "retriever_filter_selectivity_per_million".to_string(),
            serde_json::json!(self.retriever_filter_selectivity_per_million),
        );
        object.insert(
            "retriever_candidate_score_sources".to_string(),
            serde_json::json!(self.retriever_candidate_score_sources),
        );
        object.insert(
            "retriever_final_score_sources".to_string(),
            serde_json::json!(self.retriever_final_score_sources),
        );
        value
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemSearchCandidateOutput {
    pub result: SearchResultSet,
    pub report: NowledgeMemSearchCandidateReport,
    pub out_of_core_metrics: Option<SearchOutOfCoreMetrics>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSearchCandidateReadinessOptions {
    pub require_hits: bool,
    pub require_metadata_pushdown: bool,
    pub require_segment_descriptor: bool,
    pub require_text_retriever: bool,
    pub require_vector_retriever: bool,
    pub require_source_chunk_identity: bool,
    pub require_fail_soft_observation: bool,
    pub require_projection_marker_status: bool,
    pub require_projection_watermark: bool,
    pub require_embedding_identity: bool,
    pub active_embedding_model: Option<String>,
    pub active_embedding_dimension: Option<usize>,
}

impl Default for NowledgeMemSearchCandidateReadinessOptions {
    fn default() -> Self {
        Self {
            require_hits: true,
            require_metadata_pushdown: false,
            require_segment_descriptor: false,
            require_text_retriever: false,
            require_vector_retriever: false,
            require_source_chunk_identity: false,
            require_fail_soft_observation: false,
            require_projection_marker_status: true,
            require_projection_watermark: false,
            require_embedding_identity: false,
            active_embedding_model: None,
            active_embedding_dimension: None,
        }
    }
}

impl NowledgeMemSearchCandidateReadinessOptions {
    pub fn lancedb_replacement_candidate_read() -> Self {
        Self {
            require_metadata_pushdown: true,
            require_segment_descriptor: true,
            require_projection_marker_status: true,
            require_projection_watermark: true,
            require_embedding_identity: true,
            ..Self::default()
        }
    }

    pub fn with_source_chunk_identity(mut self, required: bool) -> Self {
        self.require_source_chunk_identity = required;
        self
    }

    pub fn with_vector_retriever(mut self, required: bool) -> Self {
        self.require_vector_retriever = required;
        self
    }

    pub fn with_text_retriever(mut self, required: bool) -> Self {
        self.require_text_retriever = required;
        self
    }

    pub fn with_fail_soft_observation(mut self, required: bool) -> Self {
        self.require_fail_soft_observation = required;
        self
    }

    pub fn with_projection_watermark(mut self, required: bool) -> Self {
        self.require_projection_watermark = required;
        self
    }

    pub fn with_embedding_identity(
        mut self,
        active_model: impl Into<String>,
        active_dimension: usize,
    ) -> Self {
        self.require_embedding_identity = true;
        self.active_embedding_model = Some(active_model.into());
        self.active_embedding_dimension = Some(active_dimension);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemSearchCandidateReadinessReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub mode: SearchMode,
    pub blocker_codes: Vec<String>,
    pub candidate_report: NowledgeMemSearchCandidateReport,
    pub metadata_pushdown_ready: bool,
    pub segment_descriptor_ready: bool,
    pub text_retriever_ready: bool,
    pub vector_retriever_ready: bool,
    pub source_chunk_identity_ready: bool,
    pub fail_soft_observed: bool,
    pub projection_marker_status_visible: bool,
    pub projection_watermark_ready: bool,
    pub embedding_identity_ready: bool,
}

impl NowledgeMemSearchCandidateReadinessReport {
    pub fn from_candidate_report(
        candidate_report: NowledgeMemSearchCandidateReport,
        options: &NowledgeMemSearchCandidateReadinessOptions,
    ) -> Self {
        let metadata_pushdown_ready = candidate_report.metadata_filter_count > 0
            && candidate_report.pushed_predicate_count >= candidate_report.metadata_filter_count
            && candidate_report.residual_predicate_count == 0;
        let segment_descriptor_ready = candidate_report.persisted_segment_descriptor_used;
        let text_retriever_ready = candidate_report
            .retriever_available
            .get("text")
            .copied()
            .unwrap_or(false);
        let vector_retriever_ready = candidate_report
            .retriever_available
            .get("vector")
            .copied()
            .unwrap_or(false);
        let source_chunk_identity_ready = candidate_report
            .returned_kind_counts
            .get("source_chunk")
            .copied()
            .unwrap_or_default()
            > 0
            && candidate_report.returned_missing_external_id_count == 0
            && candidate_report.returned_missing_source_id_count == 0;
        let fail_soft_observed = !candidate_report.fallback_reason_codes.is_empty()
            && candidate_report.returned_hit_count > 0;
        let projection_marker_status_visible =
            candidate_report.protocol == NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL;
        let projection_watermark_ready = candidate_report
            .projection_durable_source_graph_commit_epoch
            .is_some();
        let embedding_identity_ready =
            search_candidate_embedding_identity_ready(&candidate_report, options);
        let blocker_codes = search_candidate_readiness_blocker_codes(
            &candidate_report,
            options,
            metadata_pushdown_ready,
            segment_descriptor_ready,
            text_retriever_ready,
            vector_retriever_ready,
            source_chunk_identity_ready,
            fail_soft_observed,
            projection_marker_status_visible,
            projection_watermark_ready,
            embedding_identity_ready,
        );

        Self {
            protocol: NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL.to_string(),
            present: true,
            ready: blocker_codes.is_empty(),
            mode: candidate_report.mode,
            blocker_codes,
            candidate_report,
            metadata_pushdown_ready,
            segment_descriptor_ready,
            text_retriever_ready,
            vector_retriever_ready,
            source_chunk_identity_ready,
            fail_soft_observed,
            projection_marker_status_visible,
            projection_watermark_ready,
            embedding_identity_ready,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "mode": search_mode_name(self.mode),
            "blocker_codes": self.blocker_codes,
            "candidate_report": self.candidate_report.json(),
            "metadata_pushdown_ready": self.metadata_pushdown_ready,
            "segment_descriptor_ready": self.segment_descriptor_ready,
            "text_retriever_ready": self.text_retriever_ready,
            "vector_retriever_ready": self.vector_retriever_ready,
            "source_chunk_identity_ready": self.source_chunk_identity_ready,
            "fail_soft_observed": self.fail_soft_observed,
            "projection_marker_status_visible": self.projection_marker_status_visible,
            "projection_watermark_ready": self.projection_watermark_ready,
            "embedding_identity_ready": self.embedding_identity_ready,
        })
    }
}

impl NowledgeMemSearchCandidateOutput {
    pub fn readiness_report(
        &self,
        options: &NowledgeMemSearchCandidateReadinessOptions,
    ) -> NowledgeMemSearchCandidateReadinessReport {
        NowledgeMemSearchCandidateReadinessReport::from_candidate_report(
            self.report.clone(),
            options,
        )
    }
}

pub fn nowledge_mem_search_candidate_report(
    request: &NowledgeMemSearchCandidateRequest,
    effective_compressed_vector_search_mode: CompressedVectorSearchMode,
    result: &SearchResultSet,
) -> NowledgeMemSearchCandidateReport {
    let pushdown = &result.candidate_set.metadata_predicate_pushdown;
    let returned_kind_counts = search_candidate_returned_kind_counts(&result.hits);
    let returned_missing_external_id_count = result
        .hits
        .iter()
        .filter(|hit| hit.external_id.is_none())
        .count();
    let returned_missing_source_id_count = result
        .hits
        .iter()
        .filter(|hit| hit.source_id.is_none())
        .count();
    NowledgeMemSearchCandidateReport {
        protocol: NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL.to_string(),
        compressed_vector_search_mode: effective_compressed_vector_search_mode,
        requested_compressed_vector_search_mode: request.compressed_vector_search_mode,
        retrieval_projection_advisor: request.retrieval_projection_advisor.clone(),
        retrieval_projection_advisor_blocker_codes: if request.compressed_vector_search_mode
            == effective_compressed_vector_search_mode
        {
            Vec::new()
        } else {
            request.retrieval_projection_advisor.blocker_codes()
        },
        mode: request.mode,
        query_embedding_dimension: request.query_embedding.as_ref().map(std::vec::Vec::len),
        limit: result.limit,
        offset: result.offset,
        rank_window: result.rank_window,
        document_count: result.document_count,
        filtered_document_count: result.filtered_document_count,
        total_hits: result.total_hits,
        returned_hit_count: result.hits.len(),
        returned_kind_counts,
        returned_missing_external_id_count,
        returned_missing_source_id_count,
        truncated: result.truncated,
        candidate_set: result.candidate_set.clone(),
        filtered_out_count: result.candidate_set.filtered_out_count,
        metadata_filter_count: result.candidate_set.metadata_filters.len(),
        pushed_predicate_count: pushdown.pushed_predicate_count,
        residual_predicate_count: pushdown.residual_predicate_count,
        segment_count: pushdown.segment_count,
        pruned_segment_count: pushdown.pruned_segment_count,
        scanned_segment_count: pushdown.scanned_segment_count,
        segment_pruning_candidate_document_count: pushdown.segment_pruning_candidate_document_count,
        segment_pruned_document_count: pushdown.segment_pruned_document_count,
        segment_scanned_document_count: pushdown.segment_scanned_document_count,
        persisted_segment_descriptor_used: pushdown.persisted_segment_descriptor_used,
        physical_range_read_count: pushdown.physical_range_read_count,
        physical_bytes_read: pushdown.physical_bytes_read,
        retriever_backends: result
            .retrievers
            .iter()
            .map(|retriever| (retriever.name.clone(), retriever.backend.clone()))
            .collect(),
        retriever_backend_selection_reasons: result
            .retrievers
            .iter()
            .filter_map(|retriever| {
                retriever
                    .backend_selection_reason
                    .map(|reason| (retriever.name.clone(), reason.as_str().to_string()))
            })
            .collect(),
        retriever_estimated_raw_vector_bytes: result
            .retrievers
            .iter()
            .filter_map(|retriever| {
                retriever
                    .estimated_raw_vector_bytes
                    .map(|bytes| (retriever.name.clone(), bytes))
            })
            .collect(),
        retriever_filter_selectivity_per_million: result
            .retrievers
            .iter()
            .filter_map(|retriever| {
                retriever
                    .filter_selectivity_per_million
                    .map(|selectivity| (retriever.name.clone(), selectivity))
            })
            .collect(),
        retriever_available: result
            .retrievers
            .iter()
            .map(|retriever| (retriever.name.clone(), retriever.available))
            .collect(),
        retriever_candidate_counts: result
            .retrievers
            .iter()
            .map(|retriever| (retriever.name.clone(), retriever.candidate_count))
            .collect(),
        retriever_candidate_score_sources: result
            .retrievers
            .iter()
            .map(|retriever| {
                (
                    retriever.name.clone(),
                    retriever.candidate_score_source.clone(),
                )
            })
            .collect(),
        retriever_final_score_sources: result
            .retrievers
            .iter()
            .map(|retriever| (retriever.name.clone(), retriever.final_score_source.clone()))
            .collect(),
        fallback_reason_codes: result
            .fallback_reason_codes
            .iter()
            .map(|code| code.as_str().to_string())
            .collect(),
        empty_reason_codes: result
            .empty_reason_codes
            .iter()
            .map(|code| code.as_str().to_string())
            .collect(),
        truncation_reason_codes: result
            .truncation_reason_codes
            .iter()
            .map(|code| code.as_str().to_string())
            .collect(),
        projection_full_reindex_needed: result.projection_freshness.full_reindex_needed,
        projection_metadata_repair_needed: result.projection_freshness.metadata_repair_needed,
        projection_source_graph_commit_epoch: result.projection_freshness.source_graph_commit_epoch,
        projection_durable_source_graph_commit_epoch: result
            .projection_freshness
            .durable_source_graph_commit_epoch,
        projection_embedding_model: result.projection_freshness.embedding_model.clone(),
        projection_embedding_version: result.projection_freshness.embedding_version.clone(),
        projection_embedding_dimension: result.projection_freshness.embedding_dimension,
    }
}

#[allow(clippy::too_many_arguments)]
fn search_candidate_readiness_blocker_codes(
    candidate_report: &NowledgeMemSearchCandidateReport,
    options: &NowledgeMemSearchCandidateReadinessOptions,
    metadata_pushdown_ready: bool,
    segment_descriptor_ready: bool,
    text_retriever_ready: bool,
    vector_retriever_ready: bool,
    source_chunk_identity_ready: bool,
    fail_soft_observed: bool,
    projection_marker_status_visible: bool,
    projection_watermark_ready: bool,
    embedding_identity_ready: bool,
) -> Vec<String> {
    let mut blockers = BTreeSet::new();

    if candidate_report.protocol != NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL {
        blockers.insert("search_candidate_report_protocol_mismatch");
    }
    if options.require_hits && candidate_report.returned_hit_count == 0 {
        blockers.insert("search_candidate_no_hits");
    }
    if options.require_metadata_pushdown {
        if candidate_report.metadata_filter_count == 0 {
            blockers.insert("search_candidate_metadata_filter_missing");
        }
        if !metadata_pushdown_ready {
            blockers.insert("search_candidate_metadata_filter_not_fully_pushed");
        }
    }
    if candidate_report.residual_predicate_count > 0 {
        blockers.insert("search_candidate_metadata_filter_residual");
    }
    if options.require_segment_descriptor && !segment_descriptor_ready {
        blockers.insert("search_candidate_segment_descriptor_not_used");
    }
    if options.require_text_retriever && !text_retriever_ready {
        blockers.insert("search_candidate_text_retriever_unavailable");
    }
    if options.require_vector_retriever && !vector_retriever_ready {
        blockers.insert("search_candidate_vector_retriever_unavailable");
    }
    if options.require_source_chunk_identity && !source_chunk_identity_ready {
        blockers.insert("search_candidate_source_chunk_identity_missing");
    }
    if options.require_fail_soft_observation && !fail_soft_observed {
        blockers.insert("search_candidate_fail_soft_not_observed");
    }
    if options.require_projection_marker_status && !projection_marker_status_visible {
        blockers.insert("search_candidate_projection_marker_status_missing");
    }
    if options.require_projection_watermark && !projection_watermark_ready {
        blockers.insert("search_candidate_projection_watermark_missing");
    }
    if options.require_embedding_identity && !embedding_identity_ready {
        blockers.insert("search_candidate_embedding_identity_not_ready");
    }

    blockers
        .into_iter()
        .map(std::string::ToString::to_string)
        .collect()
}

fn search_candidate_embedding_identity_ready(
    candidate_report: &NowledgeMemSearchCandidateReport,
    options: &NowledgeMemSearchCandidateReadinessOptions,
) -> bool {
    if !options.require_embedding_identity {
        return true;
    }
    let manifest_present = candidate_report.projection_embedding_model.is_some()
        && candidate_report.projection_embedding_dimension.is_some();
    if !manifest_present {
        return false;
    }
    let model_matches = options
        .active_embedding_model
        .as_deref()
        .is_none_or(|active| {
            candidate_report.projection_embedding_model.as_deref() == Some(active)
        });
    let dimension_matches = options
        .active_embedding_dimension
        .is_none_or(|active| candidate_report.projection_embedding_dimension == Some(active));
    model_matches && dimension_matches
}

fn search_candidate_returned_kind_counts(hits: &[crate::SearchHit]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for hit in hits {
        let kind = hit.kind.as_deref().unwrap_or("unknown");
        *counts.entry(kind.to_string()).or_insert(0) += 1;
    }
    counts
}

fn search_candidate_set_report_json(report: &SearchCandidateSetReport) -> serde_json::Value {
    serde_json::json!({
        "id_space": report.id_space,
        "representation": report.representation,
        "cardinality": report.cardinality,
        "exact": report.exact,
        "snapshot_source_graph_commit_epoch": report.snapshot_source_graph_commit_epoch,
        "policy_epoch": report.policy_epoch,
        "filtered_out_count": report.filtered_out_count,
        "metadata_filters": report.metadata_filters,
        "metadata_predicate_pushdown": search_predicate_pushdown_report_json(&report.metadata_predicate_pushdown),
    })
}

fn search_predicate_pushdown_report_json(
    report: &crate::SearchPredicatePushdownReport,
) -> serde_json::Value {
    serde_json::json!({
        "input_predicate_count": report.input_predicate_count,
        "pushed_predicate_count": report.pushed_predicate_count,
        "residual_predicate_count": report.residual_predicate_count,
        "unsatisfiable": report.unsatisfiable,
        "parse_error": report.parse_error,
        "segment_count": report.segment_count,
        "pruned_segment_count": report.pruned_segment_count,
        "scanned_segment_count": report.scanned_segment_count,
        "segment_pruning_candidate_document_count": report.segment_pruning_candidate_document_count,
        "segment_pruned_document_count": report.segment_pruned_document_count,
        "segment_scanned_document_count": report.segment_scanned_document_count,
        "persisted_segment_descriptor_used": report.persisted_segment_descriptor_used,
        "physical_range_read_count": report.physical_range_read_count,
        "physical_bytes_read": report.physical_bytes_read,
        "field_summaries": report.field_summaries.iter().map(search_predicate_field_pruning_report_json).collect::<Vec<_>>(),
    })
}

fn search_predicate_field_pruning_report_json(
    report: &crate::SearchPredicateFieldPruningReport,
) -> serde_json::Value {
    serde_json::json!({
        "field": report.field,
        "value_kind": report.value_kind,
        "operation_kinds": report.operation_kinds,
        "segment_count": report.segment_count,
        "pruned_segment_count": report.pruned_segment_count,
        "scanned_segment_count": report.scanned_segment_count,
        "numeric_range_summary_used": report.numeric_range_summary_used,
        "timestamp_range_summary_used": report.timestamp_range_summary_used,
        "value_summary_used": report.value_summary_used,
    })
}

fn search_mode_name(mode: SearchMode) -> &'static str {
    match mode {
        SearchMode::Hybrid => "hybrid",
        SearchMode::Vector => "vector",
        SearchMode::Text => "text",
    }
}

// Keep facade-only adapters off the methods of public host types.
pub fn effective_search_candidate_mode(
    request: &NowledgeMemSearchCandidateRequest,
) -> CompressedVectorSearchMode {
    request.effective_compressed_vector_search_mode()
}

pub fn retrieval_projection_advisor_blocker_codes(
    advisor: &NowledgeMemRetrievalProjectionAdvisor,
) -> Vec<String> {
    advisor.blocker_codes()
}

pub fn retrieval_projection_advisor_json(
    advisor: &NowledgeMemRetrievalProjectionAdvisor,
) -> serde_json::Value {
    advisor.json()
}
