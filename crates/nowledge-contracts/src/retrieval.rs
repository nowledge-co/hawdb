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

use std::collections::BTreeMap;
use std::str::FromStr;

use hawdb_qos::{
    BackgroundWorkDecision, BackgroundWorkHint, BackgroundWorkPlan, LocalQosPolicy,
    LocalQosSnapshot, LocalQosState, QosAdmission, QosAdmissionCode, WorkClass, WorkPriority,
    WorkRequest,
};
use hawdb_search::{
    SearchCandidateSetReport, SearchFallbackReasonCode, SearchFusionWeights, SearchMatchedSpan,
    SearchMode, SearchProjectionFreshness, SearchResultSet, SearchRetrieverCandidateSetReport,
    SearchTruncationReasonCode,
};

use crate::{KnowledgeEntity, KnowledgeGraphContextPath};

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrievalRequest {
    pub query_text: String,
    pub query_embedding: Option<Vec<f32>>,
    pub mode: SearchMode,
    pub limit: usize,
    pub offset: usize,
    pub rank_window: Option<usize>,
    pub search_fusion_weights: SearchFusionWeights,
    pub metadata_filters: BTreeMap<String, String>,
    pub candidate_limit: Option<usize>,
    pub candidate_scoring: KnowledgeCandidateScoringPolicy,
    pub graph_seed_limit: usize,
    pub graph_context_limit: usize,
    pub graph_context_max_hops: usize,
}

pub const NOWLEDGE_DEEP_SEARCH_MIN_RANK_WINDOW: usize = 20;
pub const NOWLEDGE_DEEP_SEARCH_RANK_WINDOW_MULTIPLIER: usize = 5;
pub const NOWLEDGE_DEEP_SEARCH_FILTERED_RANK_WINDOW: usize = 200;
pub const NOWLEDGE_DEEP_SEARCH_MIN_GRAPH_SEED_LIMIT: usize = 12;
pub const NOWLEDGE_DEEP_SEARCH_MAX_GRAPH_SEED_LIMIT: usize = 40;
pub const NOWLEDGE_DEEP_SEARCH_GRAPH_SEED_MULTIPLIER: usize = 2;
pub const NOWLEDGE_DEEP_SEARCH_GRAPH_CONTEXT_MAX_HOPS: usize = 2;

impl KnowledgeRetrievalRequest {
    pub fn nowledge_deep(
        query_text: impl Into<String>,
        query_embedding: Option<Vec<f32>>,
        mode: SearchMode,
        limit: usize,
        offset: usize,
        metadata_filters: BTreeMap<String, String>,
    ) -> Self {
        let page_end = offset.saturating_add(limit);
        let rank_window = nowledge_deep_search_rank_window(page_end, !metadata_filters.is_empty());
        let graph_seed_limit = nowledge_deep_search_graph_seed_limit(page_end);
        Self {
            query_text: query_text.into(),
            query_embedding,
            mode,
            limit,
            offset,
            rank_window: Some(rank_window),
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters,
            candidate_limit: Some(rank_window),
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit,
            graph_context_limit: rank_window,
            graph_context_max_hops: NOWLEDGE_DEEP_SEARCH_GRAPH_CONTEXT_MAX_HOPS,
        }
    }
}

pub fn nowledge_deep_search_rank_window(page_end: usize, has_filters: bool) -> usize {
    if has_filters {
        return NOWLEDGE_DEEP_SEARCH_FILTERED_RANK_WINDOW;
    }
    page_end
        .saturating_mul(NOWLEDGE_DEEP_SEARCH_RANK_WINDOW_MULTIPLIER)
        .max(NOWLEDGE_DEEP_SEARCH_MIN_RANK_WINDOW)
}

pub fn nowledge_deep_search_graph_seed_limit(page_end: usize) -> usize {
    page_end
        .saturating_mul(NOWLEDGE_DEEP_SEARCH_GRAPH_SEED_MULTIPLIER)
        .clamp(
            NOWLEDGE_DEEP_SEARCH_MIN_GRAPH_SEED_LIMIT,
            NOWLEDGE_DEEP_SEARCH_MAX_GRAPH_SEED_LIMIT,
        )
}

pub use hawdb_search::{
    KnowledgeRetrievalPipelineReport, KnowledgeRetrievalStage, SearchProjectionChangeBatch,
    SearchProjectionGraphDeltaRequest, SearchProjectionRelationalDelta,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundMaintenanceOptions {
    pub hint: BackgroundWorkHint,
    pub include_storage_checkpoint: bool,
    pub include_schema_maintenance: bool,
    pub include_property_index_projection: bool,
    pub include_optimizer_statistics_refresh: bool,
    pub include_search_projection_graph_delta_freshness: bool,
    pub include_search_projection_rebuild: bool,
    pub include_search_projection_metadata_repair: bool,
    pub include_hawdb_lightning_bootstrap_export: bool,
    pub include_external_content_artifact_jobs: bool,
    pub external_content_artifact_estimated_operations: usize,
    pub search_projection_graph_delta: Option<SearchProjectionGraphDeltaRequest>,
}

impl Default for BackgroundMaintenanceOptions {
    fn default() -> Self {
        Self {
            hint: BackgroundWorkHint::default(),
            include_storage_checkpoint: true,
            include_schema_maintenance: true,
            include_property_index_projection: true,
            include_optimizer_statistics_refresh: true,
            include_search_projection_graph_delta_freshness: true,
            include_search_projection_rebuild: true,
            include_search_projection_metadata_repair: true,
            include_hawdb_lightning_bootstrap_export: true,
            include_external_content_artifact_jobs: true,
            external_content_artifact_estimated_operations: 1,
            search_projection_graph_delta: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundMaintenanceCandidate {
    pub kind: BackgroundMaintenanceKind,
    pub name: String,
    pub plan: BackgroundWorkPlan,
    pub search_projection_graph_delta: Option<SearchProjectionGraphDeltaRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedBackgroundMaintenance {
    pub kind: BackgroundMaintenanceKind,
    pub name: String,
    pub plan: BackgroundWorkPlan,
    pub decision: BackgroundWorkDecision,
    pub search_projection_graph_delta: Option<SearchProjectionGraphDeltaRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackgroundMaintenanceSummary {
    pub total_candidates: usize,
    pub admitted_count: usize,
    pub deferred_count: usize,
    pub rejected_count: usize,
    pub total_estimated_operations: usize,
    pub admitted_estimated_operations: usize,
    pub deferred_estimated_operations: usize,
    pub rejected_estimated_operations: usize,
    pub executable_search_projection_graph_delta_count: usize,
    pub admitted_search_projection_graph_delta_count: usize,
    pub deferred_search_projection_graph_delta_count: usize,
    pub rejected_search_projection_graph_delta_count: usize,
    pub executable_search_projection_graph_delta_operations: usize,
    pub admitted_search_projection_graph_delta_operations: usize,
    pub max_search_projection_graph_delta_complete_through_graph_commit_epoch: Option<u64>,
    pub foreground_admission_probe_ready: bool,
    pub foreground_admission_probe_admission_name: Option<String>,
    pub qos_snapshot: Option<LocalQosSnapshot>,
    pub top_admitted_kind: Option<BackgroundMaintenanceKind>,
    pub top_admitted_name: Option<String>,
    pub ranked: Vec<BackgroundMaintenanceSummaryItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundMaintenanceSummaryItem {
    pub kind: BackgroundMaintenanceKind,
    pub name: String,
    pub work_class: WorkClass,
    pub work_class_name: String,
    pub priority: WorkPriority,
    pub priority_name: String,
    pub estimated_operations: usize,
    pub hint_active_topic: bool,
    pub hint_recent_delta_operations: usize,
    pub hint_source_graph_commit_lag: u64,
    pub hint_query_probability_per_million: u32,
    pub hint_staleness_millis: u64,
    pub hint_staleness_ttl_millis: Option<u64>,
    pub hint_freshness_slo_millis: Option<u64>,
    pub hint_tenant_budget_remaining_operations: Option<usize>,
    pub admission: QosAdmission,
    pub admission_name: String,
    pub admission_code: Option<QosAdmissionCode>,
    pub admission_code_name: Option<String>,
    pub score: u64,
    pub reason_code_names: Vec<String>,
    pub reasons: Vec<String>,
    pub has_executable_search_projection_graph_delta: bool,
    pub search_projection_graph_delta_operation_count: Option<usize>,
    pub search_projection_graph_delta_upsert_node_count: Option<usize>,
    pub search_projection_graph_delta_delete_document_count: Option<usize>,
    pub search_projection_graph_delta_complete_through_graph_commit_epoch: Option<u64>,
    pub search_projection_graph_delta_max_operations: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundMaintenanceKind {
    StorageCheckpoint,
    SchemaMaintenance,
    PropertyIndexProjection,
    OptimizerStatisticsRefresh,
    SearchProjectionGraphDelta,
    SearchProjectionRebuild,
    SearchProjectionMetadataRepair,
    HawDBLightningBootstrapExport,
    ExternalContentArtifactJob,
}

impl BackgroundMaintenanceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            BackgroundMaintenanceKind::StorageCheckpoint => "storage_checkpoint",
            BackgroundMaintenanceKind::SchemaMaintenance => "schema_maintenance",
            BackgroundMaintenanceKind::PropertyIndexProjection => "property_index_projection",
            BackgroundMaintenanceKind::OptimizerStatisticsRefresh => "optimizer_statistics_refresh",
            BackgroundMaintenanceKind::SearchProjectionGraphDelta => {
                "search_projection_graph_delta"
            }
            BackgroundMaintenanceKind::SearchProjectionRebuild => "search_projection_rebuild",
            BackgroundMaintenanceKind::SearchProjectionMetadataRepair => {
                "search_projection_metadata_repair"
            }
            BackgroundMaintenanceKind::HawDBLightningBootstrapExport => {
                "hawdb_lightning_bootstrap_export"
            }
            BackgroundMaintenanceKind::ExternalContentArtifactJob => {
                "external_content_artifact_job"
            }
        }
    }
}

impl FromStr for BackgroundMaintenanceKind {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "storage_checkpoint" => Ok(BackgroundMaintenanceKind::StorageCheckpoint),
            "schema_maintenance" => Ok(BackgroundMaintenanceKind::SchemaMaintenance),
            "property_index_projection" => Ok(BackgroundMaintenanceKind::PropertyIndexProjection),
            "optimizer_statistics_refresh" => {
                Ok(BackgroundMaintenanceKind::OptimizerStatisticsRefresh)
            }
            "search_projection_graph_delta" => {
                Ok(BackgroundMaintenanceKind::SearchProjectionGraphDelta)
            }
            "search_projection_rebuild" => Ok(BackgroundMaintenanceKind::SearchProjectionRebuild),
            "search_projection_metadata_repair" => {
                Ok(BackgroundMaintenanceKind::SearchProjectionMetadataRepair)
            }
            "hawdb_lightning_bootstrap_export" => {
                Ok(BackgroundMaintenanceKind::HawDBLightningBootstrapExport)
            }
            "external_content_artifact_job" => {
                Ok(BackgroundMaintenanceKind::ExternalContentArtifactJob)
            }
            _ => Err("unknown background maintenance kind"),
        }
    }
}

impl BackgroundMaintenanceCandidate {
    pub fn new(kind: BackgroundMaintenanceKind, plan: BackgroundWorkPlan) -> Self {
        Self {
            kind,
            name: kind.as_str().to_string(),
            plan,
            search_projection_graph_delta: None,
        }
    }

    pub fn with_search_projection_graph_delta(
        mut self,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Self {
        self.search_projection_graph_delta = Some(request);
        self
    }
}

impl BackgroundMaintenanceSummary {
    #[doc(hidden)]
    pub fn from_ranked(
        ranked: Vec<RankedBackgroundMaintenance>,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
    ) -> Self {
        let foreground_admission = policy.admit(
            state,
            &WorkRequest::foreground(WorkClass::Query, usize::MAX),
        );
        let qos_snapshot = policy.snapshot(state);
        let mut summary = Self {
            total_candidates: ranked.len(),
            foreground_admission_probe_ready: qos_snapshot.foreground_admitted,
            foreground_admission_probe_admission_name: Some(
                foreground_admission.as_str().to_string(),
            ),
            qos_snapshot: Some(qos_snapshot),
            ..Self::default()
        };

        for ranked_item in ranked {
            let item = BackgroundMaintenanceSummaryItem::from_ranked(ranked_item);
            summary.total_estimated_operations = summary
                .total_estimated_operations
                .saturating_add(item.estimated_operations);
            match item.admission {
                QosAdmission::Admit => {
                    summary.admitted_count += 1;
                    summary.admitted_estimated_operations = summary
                        .admitted_estimated_operations
                        .saturating_add(item.estimated_operations);
                    if summary.top_admitted_kind.is_none() {
                        summary.top_admitted_kind = Some(item.kind);
                        summary.top_admitted_name = Some(item.name.clone());
                    }
                    if item.has_executable_search_projection_graph_delta {
                        summary.admitted_search_projection_graph_delta_count += 1;
                        summary.admitted_search_projection_graph_delta_operations = summary
                            .admitted_search_projection_graph_delta_operations
                            .saturating_add(
                                item.search_projection_graph_delta_operation_count
                                    .unwrap_or_default(),
                            );
                    }
                }
                QosAdmission::Defer { .. } => {
                    summary.deferred_count += 1;
                    summary.deferred_estimated_operations = summary
                        .deferred_estimated_operations
                        .saturating_add(item.estimated_operations);
                    if item.has_executable_search_projection_graph_delta {
                        summary.deferred_search_projection_graph_delta_count += 1;
                    }
                }
                QosAdmission::Reject { .. } => {
                    summary.rejected_count += 1;
                    summary.rejected_estimated_operations = summary
                        .rejected_estimated_operations
                        .saturating_add(item.estimated_operations);
                    if item.has_executable_search_projection_graph_delta {
                        summary.rejected_search_projection_graph_delta_count += 1;
                    }
                }
            }
            if item.has_executable_search_projection_graph_delta {
                summary.executable_search_projection_graph_delta_count += 1;
                summary.executable_search_projection_graph_delta_operations = summary
                    .executable_search_projection_graph_delta_operations
                    .saturating_add(
                        item.search_projection_graph_delta_operation_count
                            .unwrap_or_default(),
                    );
                if let Some(epoch) =
                    item.search_projection_graph_delta_complete_through_graph_commit_epoch
                {
                    summary.max_search_projection_graph_delta_complete_through_graph_commit_epoch =
                        Some(
                            summary
                                .max_search_projection_graph_delta_complete_through_graph_commit_epoch
                                .map_or(epoch, |current| current.max(epoch)),
                        );
                }
            }
            summary.ranked.push(item);
        }

        summary
    }
}

impl BackgroundMaintenanceSummaryItem {
    fn from_ranked(ranked: RankedBackgroundMaintenance) -> Self {
        let admission_code = ranked.decision.admission.code();
        let search_projection_graph_delta = ranked.search_projection_graph_delta.as_ref();
        Self {
            kind: ranked.kind,
            name: ranked.name,
            work_class: ranked.plan.request.class,
            work_class_name: ranked.plan.request.class.as_str().to_string(),
            priority: ranked.plan.request.priority,
            priority_name: ranked.plan.request.priority.as_str().to_string(),
            estimated_operations: ranked.plan.request.estimated_operations,
            hint_active_topic: ranked.plan.hint.active_topic,
            hint_recent_delta_operations: ranked.plan.hint.recent_delta_operations,
            hint_source_graph_commit_lag: ranked.plan.hint.source_graph_commit_lag,
            hint_query_probability_per_million: ranked.plan.hint.query_probability_per_million,
            hint_staleness_millis: ranked.plan.hint.staleness_millis,
            hint_staleness_ttl_millis: ranked.plan.hint.staleness_ttl_millis,
            hint_freshness_slo_millis: ranked.plan.hint.freshness_slo_millis,
            hint_tenant_budget_remaining_operations: ranked
                .plan
                .hint
                .tenant_budget_remaining_operations,
            admission_name: ranked.decision.admission.as_str().to_string(),
            admission_code,
            admission_code_name: admission_code.map(|code| code.as_str().to_string()),
            admission: ranked.decision.admission,
            score: ranked.decision.score,
            reason_code_names: ranked
                .decision
                .reason_codes
                .iter()
                .map(|code| code.as_str().to_string())
                .collect(),
            reasons: ranked.decision.reasons,
            has_executable_search_projection_graph_delta: search_projection_graph_delta.is_some(),
            search_projection_graph_delta_operation_count: search_projection_graph_delta
                .map(SearchProjectionGraphDeltaRequest::operation_count),
            search_projection_graph_delta_upsert_node_count: search_projection_graph_delta
                .map(|request| request.upsert_node_ids.len()),
            search_projection_graph_delta_delete_document_count: search_projection_graph_delta
                .map(|request| request.delete_document_ids.len()),
            search_projection_graph_delta_complete_through_graph_commit_epoch:
                search_projection_graph_delta
                    .and_then(|request| request.complete_through_graph_commit_epoch),
            search_projection_graph_delta_max_operations: search_projection_graph_delta
                .and_then(|request| request.max_operations),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrievalOutput {
    pub graph_commit_epoch: u64,
    pub projection_freshness: SearchProjectionFreshness,
    pub search: SearchResultSet,
    pub retrievers: Vec<KnowledgeRetrieverReport>,
    pub diagnostics: KnowledgeRetrievalDiagnostics,
    pub candidates: Vec<KnowledgeCandidate>,
    pub evidence: Vec<KnowledgeEvidence>,
    pub graph_seeds: Vec<KnowledgeGraphSeed>,
    pub graph_context_paths: Vec<KnowledgeGraphContextPath>,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrievalDiagnostics {
    pub graph_commit_epoch: u64,
    pub projection_source_graph_commit_epoch: Option<u64>,
    pub projection_commit_lag: u64,
    pub projection_stale: bool,
    pub projection_full_reindex_needed: bool,
    pub projection_full_reindex_reasons: Vec<String>,
    pub projection_metadata_repair_needed: bool,
    pub projection_metadata_repair_reasons: Vec<String>,
    pub search_document_count: usize,
    pub search_filtered_document_count: usize,
    pub search_total_hits: usize,
    pub search_candidate_set: SearchCandidateSetReport,
    pub search_candidate_filtered_out_count: usize,
    pub search_limit: usize,
    pub search_truncated: bool,
    pub search_truncation_reason_codes: Vec<SearchTruncationReasonCode>,
    pub search_truncation_reasons: Vec<String>,
    pub search_fallback_reason_codes: Vec<SearchFallbackReasonCode>,
    pub search_fallback_reasons: Vec<String>,
    pub rank_window: Option<usize>,
    pub search_fusion_weights: SearchFusionWeights,
    pub graph_seed_input_candidate_set: SearchCandidateSetReport,
    pub graph_seed_candidate_set: SearchRetrieverCandidateSetReport,
    pub graph_seed_candidate_count: usize,
    pub graph_seed_returned_count: usize,
    pub graph_seed_limit: usize,
    pub graph_seed_truncated: bool,
    pub graph_seed_truncation_reason_codes: Vec<KnowledgeTruncationReasonCode>,
    pub graph_seed_truncation_reasons: Vec<String>,
    pub graph_context_input_candidate_set: SearchCandidateSetReport,
    pub graph_context_candidate_set: SearchRetrieverCandidateSetReport,
    pub graph_context_path_count: usize,
    pub graph_context_node_count: usize,
    pub graph_context_relationship_count: usize,
    pub graph_context_limit: usize,
    pub graph_context_max_hops: usize,
    pub graph_context_truncated: bool,
    pub graph_context_truncation_reason_codes: Vec<KnowledgeTruncationReasonCode>,
    pub graph_context_truncation_reasons: Vec<String>,
    pub graph_context_fallback_reason_codes: Vec<KnowledgeFallbackReasonCode>,
    pub graph_context_fallback_reasons: Vec<String>,
    pub fanout_reason_count: usize,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub fanout_reasons: Vec<String>,
    pub candidate_count: usize,
    pub candidate_total_count: usize,
    pub candidate_limit: Option<usize>,
    pub candidate_truncated: bool,
    pub candidate_truncation_reason_codes: Vec<KnowledgeTruncationReasonCode>,
    pub candidate_truncation_reasons: Vec<String>,
    pub warnings: Vec<String>,
    pub empty_reason_codes: Vec<KnowledgeRetrievalEmptyReasonCode>,
    pub empty_reasons: Vec<String>,
    pub pipeline: KnowledgeRetrievalPipelineReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeFanoutReasonDetail {
    pub code: KnowledgeFanoutReasonCode,
    pub message: String,
    pub operation: Option<String>,
    pub limit: Option<usize>,
    pub total: Option<usize>,
    pub seed_hit_id: Option<String>,
    pub node_id: Option<u64>,
    pub relationship_type: Option<String>,
    pub direction: Option<String>,
    pub degree: Option<usize>,
}

impl KnowledgeFanoutReasonDetail {
    #[doc(hidden)]
    pub fn dense_adjacency(
        operation: &str,
        relationship_type: &str,
        direction: &str,
        node_id: u64,
        degree: usize,
    ) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::DenseAdjacency,
            message: format!(
                "{operation} dense_adjacency {relationship_type} {direction} node {node_id} degree {degree}"
            ),
            operation: Some(operation.to_string()),
            limit: None,
            total: None,
            seed_hit_id: None,
            node_id: Some(node_id),
            relationship_type: Some(relationship_type.to_string()),
            direction: Some(direction.to_string()),
            degree: Some(degree),
        }
    }

    #[doc(hidden)]
    pub fn graph_context_limit(limit: usize, seed_hit_id: &str) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::GraphContextLimitReached,
            message: format!(
                "graph_context_limit {limit} reached while expanding hit {seed_hit_id}"
            ),
            operation: Some("graph_context".to_string()),
            limit: Some(limit),
            total: None,
            seed_hit_id: Some(seed_hit_id.to_string()),
            node_id: None,
            relationship_type: None,
            direction: None,
            degree: None,
        }
    }

    #[doc(hidden)]
    pub fn graph_seed_limit(limit: usize, total: usize) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::GraphSeedLimitReached,
            message: format!(
                "knowledge_graph_seed_limit {limit} returned from {total} matching graph seeds"
            ),
            operation: Some("graph_seed".to_string()),
            limit: Some(limit),
            total: Some(total),
            seed_hit_id: None,
            node_id: None,
            relationship_type: None,
            direction: None,
            degree: None,
        }
    }

    #[doc(hidden)]
    pub fn candidate_limit(limit: usize, total: usize) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::CandidateLimitReached,
            message: format!(
                "knowledge_candidate_limit {limit} returned from {total} merged candidates"
            ),
            operation: Some("candidate".to_string()),
            limit: Some(limit),
            total: Some(total),
            seed_hit_id: None,
            node_id: None,
            relationship_type: None,
            direction: None,
            degree: None,
        }
    }

    #[doc(hidden)]
    pub fn path_limit(operation: &str, limit: usize, target: &str) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::PathLimitReached,
            message: format!("{operation} limit {limit} reached while expanding {target}"),
            operation: Some(operation.to_string()),
            limit: Some(limit),
            total: None,
            seed_hit_id: None,
            node_id: None,
            relationship_type: None,
            direction: None,
            degree: None,
        }
    }

    #[doc(hidden)]
    pub fn node_limit(limit: usize) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::NodeLimitReached,
            message: format!("knowledge_subgraph node_limit {limit} reached"),
            operation: Some("knowledge_subgraph".to_string()),
            limit: Some(limit),
            total: None,
            seed_hit_id: None,
            node_id: None,
            relationship_type: None,
            direction: None,
            degree: None,
        }
    }

    #[doc(hidden)]
    pub fn relationship_limit(limit: usize) -> Self {
        Self {
            code: KnowledgeFanoutReasonCode::RelationshipLimitReached,
            message: format!("knowledge_subgraph relationship_limit {limit} reached"),
            operation: Some("knowledge_subgraph".to_string()),
            limit: Some(limit),
            total: None,
            seed_hit_id: None,
            node_id: None,
            relationship_type: None,
            direction: None,
            degree: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeFanoutReasonCode {
    DenseAdjacency,
    GraphContextLimitReached,
    GraphSeedLimitReached,
    CandidateLimitReached,
    PathLimitReached,
    NodeLimitReached,
    RelationshipLimitReached,
}

impl KnowledgeFanoutReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DenseAdjacency => "dense_adjacency",
            Self::GraphContextLimitReached => "graph_context_limit_reached",
            Self::GraphSeedLimitReached => "graph_seed_limit_reached",
            Self::CandidateLimitReached => "candidate_limit_reached",
            Self::PathLimitReached => "path_limit_reached",
            Self::NodeLimitReached => "node_limit_reached",
            Self::RelationshipLimitReached => "relationship_limit_reached",
        }
    }
}

impl FromStr for KnowledgeFanoutReasonCode {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "dense_adjacency" => Ok(Self::DenseAdjacency),
            "graph_context_limit_reached" => Ok(Self::GraphContextLimitReached),
            "graph_seed_limit_reached" => Ok(Self::GraphSeedLimitReached),
            "candidate_limit_reached" => Ok(Self::CandidateLimitReached),
            "path_limit_reached" => Ok(Self::PathLimitReached),
            "node_limit_reached" => Ok(Self::NodeLimitReached),
            "relationship_limit_reached" => Ok(Self::RelationshipLimitReached),
            _ => Err("unknown knowledge fanout reason code"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeFallbackReasonCode {
    GraphSeedLimitZero,
    GraphContextLimitZero,
    GraphContextMaxHopsZero,
}

impl KnowledgeFallbackReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GraphSeedLimitZero => "graph_seed_limit_zero",
            Self::GraphContextLimitZero => "graph_context_limit_zero",
            Self::GraphContextMaxHopsZero => "graph_context_max_hops_zero",
        }
    }
}

impl FromStr for KnowledgeFallbackReasonCode {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "graph_seed_limit_zero" => Ok(Self::GraphSeedLimitZero),
            "graph_context_limit_zero" => Ok(Self::GraphContextLimitZero),
            "graph_context_max_hops_zero" => Ok(Self::GraphContextMaxHopsZero),
            _ => Err("unknown knowledge fallback reason code"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeTruncationReasonCode {
    RankWindowExceeded,
    SearchLimitExceeded,
    PartialCandidateReturn,
    GraphSeedLimitExceeded,
    GraphContextLimitExceeded,
    CandidateLimitExceeded,
}

impl KnowledgeTruncationReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RankWindowExceeded => "rank_window_exceeded",
            Self::SearchLimitExceeded => "search_limit_exceeded",
            Self::PartialCandidateReturn => "partial_candidate_return",
            Self::GraphSeedLimitExceeded => "graph_seed_limit_exceeded",
            Self::GraphContextLimitExceeded => "graph_context_limit_exceeded",
            Self::CandidateLimitExceeded => "candidate_limit_exceeded",
        }
    }
}

impl FromStr for KnowledgeTruncationReasonCode {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "rank_window_exceeded" => Ok(Self::RankWindowExceeded),
            "search_limit_exceeded" => Ok(Self::SearchLimitExceeded),
            "partial_candidate_return" => Ok(Self::PartialCandidateReturn),
            "graph_seed_limit_exceeded" => Ok(Self::GraphSeedLimitExceeded),
            "graph_context_limit_exceeded" => Ok(Self::GraphContextLimitExceeded),
            "candidate_limit_exceeded" => Ok(Self::CandidateLimitExceeded),
            _ => Err("unknown knowledge truncation reason code"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeRetrievalEmptyReasonCode {
    SearchProjectionEmpty,
    SearchMetadataFilterEmpty,
    SearchRetrieverNoHits,
    SearchLimitExcludedAllHits,
    GraphSeedLimitZero,
    GraphSeedNoCandidates,
    CandidateLimitExcludedAllCandidates,
    NoCandidates,
}

impl KnowledgeRetrievalEmptyReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SearchProjectionEmpty => "search_projection_empty",
            Self::SearchMetadataFilterEmpty => "search_metadata_filter_empty",
            Self::SearchRetrieverNoHits => "search_retriever_no_hits",
            Self::SearchLimitExcludedAllHits => "search_limit_excluded_all_hits",
            Self::GraphSeedLimitZero => "graph_seed_limit_zero",
            Self::GraphSeedNoCandidates => "graph_seed_no_candidates",
            Self::CandidateLimitExcludedAllCandidates => "candidate_limit_excluded_all_candidates",
            Self::NoCandidates => "no_candidates",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrieverReport {
    pub name: String,
    pub backend: String,
    pub available: bool,
    pub input_candidate_set: SearchCandidateSetReport,
    pub candidate_count: usize,
    pub candidate_set: SearchRetrieverCandidateSetReport,
    pub limit: Option<usize>,
    pub rank_window: Option<usize>,
    pub fusion_weight: Option<f64>,
    pub fallback_reason_codes: Vec<SearchFallbackReasonCode>,
    pub knowledge_fallback_reason_codes: Vec<KnowledgeFallbackReasonCode>,
    pub fallback_reasons: Vec<String>,
    pub truncated: bool,
    pub truncation_reason_codes: Vec<KnowledgeTruncationReasonCode>,
    pub truncation_reasons: Vec<String>,
    pub top_candidates: Vec<KnowledgeRetrieverCandidate>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeRetrieverCandidate {
    pub id: String,
    pub kind: Option<String>,
    pub external_id: Option<String>,
    pub source_id: Option<String>,
    pub canonical_node_id: Option<u64>,
    pub rank: usize,
    pub score: f64,
    pub matched_spans: Vec<SearchMatchedSpan>,
    pub graph_context_path_count: usize,
    pub projection_freshness: Option<SearchProjectionFreshness>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeCandidate {
    pub id: String,
    pub canonical_node_id: Option<u64>,
    pub source: KnowledgeCandidateSource,
    pub source_rank: usize,
    pub merged_sources: Vec<KnowledgeCandidateSource>,
    pub score: f64,
    pub score_breakdown: KnowledgeCandidateScoreBreakdown,
    pub entity: Option<KnowledgeEntity>,
    pub evidence: Option<KnowledgeEvidence>,
    pub matched_properties: Vec<String>,
    pub graph_context_path_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum KnowledgeCandidateScoringPolicy {
    Max,
    WeightedSum {
        search_weight: f64,
        graph_seed_weight: f64,
    },
    /// Typed weighted-feature scoring with optional exponential decay.
    Spec(ScoringSpec),
}

/// Typed, host-injectable rerank scoring.
///
/// A spec is a weighted sum of features multiplied by exponential decay
/// factors. The retrieval rerank stage evaluates it against one candidate at a
/// time; the plan carries only [`ScoringSpec::shape_fingerprint`], so changing
/// weights never invalidates a cached plan.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoringSpec {
    pub terms: Vec<ScoringTerm>,
    pub decay: Vec<DecayTerm>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScoringTerm {
    pub weight: f64,
    pub feature: ScoreFeature,
}

/// Exponential decay `0.5^(age / half_life)` over a distance or timestamp
/// feature, clamped to `[min_factor, 1]` and multiplied into the combined
/// score.
#[derive(Debug, Clone, PartialEq)]
pub struct DecayTerm {
    pub feature: ScoreFeature,
    pub half_life: f64,
    pub min_factor: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScoreFeature {
    /// Ranking score returned by the search projection.
    SearchScore,
    /// Graph-side seed score for the same canonical node.
    GraphSeedScore,
    /// Bounded graph distance from the seed (`0` for the seed itself).
    HopDistance,
    /// Numeric canonical node property.
    NodeProperty(String),
    /// Canonical timestamp property, aged against the request clock.
    TimestampProperty(String),
}

/// Feature values the engine can supply for one candidate.
pub trait ScoringFeatureSource {
    fn search_score(&self) -> Option<f64>;
    fn graph_seed_score(&self) -> Option<f64>;
    fn hop_distance(&self) -> Option<usize>;
    fn numeric_property(&self, property: &str) -> Option<f64>;
    fn timestamp_millis(&self, property: &str) -> Option<u64>;
}

/// One candidate's evaluation, carrying per-term provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoringEvaluation {
    pub combined_score: f64,
    /// Contribution of each [`ScoringSpec::terms`] entry, in spec order.
    pub term_contributions: Vec<f64>,
    /// Factor of each [`ScoringSpec::decay`] entry, in spec order.
    pub decay_factors: Vec<f64>,
    /// Features the source could not supply; they neither add nor multiply.
    pub missing_features: Vec<ScoreFeature>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScoringSpecError {
    NoTerms,
    NonFiniteWeight,
    NegativeWeight,
    NonPositiveHalfLife,
    InvalidMinFactor,
    /// A timestamp feature carries no rank value of its own.
    TimestampTermNotAllowed,
    /// Only `HopDistance` and `TimestampProperty` define an age to decay.
    UnsupportedDecayFeature,
}

impl ScoringSpec {
    /// Weighted sum over the two retriever scores.
    pub fn weighted_scores(search_weight: f64, graph_seed_weight: f64) -> Self {
        Self {
            terms: vec![
                ScoringTerm {
                    weight: search_weight,
                    feature: ScoreFeature::SearchScore,
                },
                ScoringTerm {
                    weight: graph_seed_weight,
                    feature: ScoreFeature::GraphSeedScore,
                },
            ],
            decay: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<(), ScoringSpecError> {
        if self.terms.is_empty() {
            return Err(ScoringSpecError::NoTerms);
        }
        for term in &self.terms {
            if !term.weight.is_finite() {
                return Err(ScoringSpecError::NonFiniteWeight);
            }
            if term.weight < 0.0 {
                return Err(ScoringSpecError::NegativeWeight);
            }
            if matches!(term.feature, ScoreFeature::TimestampProperty(_)) {
                return Err(ScoringSpecError::TimestampTermNotAllowed);
            }
        }
        for decay in &self.decay {
            if !decay.half_life.is_finite() || decay.half_life <= 0.0 {
                return Err(ScoringSpecError::NonPositiveHalfLife);
            }
            if !decay.min_factor.is_finite() || !(0.0..=1.0).contains(&decay.min_factor) {
                return Err(ScoringSpecError::InvalidMinFactor);
            }
            if !matches!(
                decay.feature,
                ScoreFeature::HopDistance | ScoreFeature::TimestampProperty(_)
            ) {
                return Err(ScoringSpecError::UnsupportedDecayFeature);
            }
        }
        Ok(())
    }

    /// Feature shape without weights: the identity that belongs in a plan
    /// fingerprint or cache key.
    pub fn shape_fingerprint(&self) -> String {
        let terms = self
            .terms
            .iter()
            .map(|term| score_feature_name(&term.feature))
            .collect::<Vec<_>>()
            .join(",");
        let decay = self
            .decay
            .iter()
            .map(|decay| score_feature_name(&decay.feature))
            .collect::<Vec<_>>()
            .join(",");
        format!("terms=[{terms}] decay=[{decay}]")
    }

    /// Whether this spec reads canonical node properties. Callers use it to
    /// avoid loading a node record for specs that only need retriever scores.
    pub fn needs_canonical_node_properties(&self) -> bool {
        self.terms
            .iter()
            .any(|term| matches!(term.feature, ScoreFeature::NodeProperty(_)))
            || self.decay.iter().any(|decay| {
                matches!(
                    decay.feature,
                    ScoreFeature::NodeProperty(_) | ScoreFeature::TimestampProperty(_)
                )
            })
    }

    /// Evaluates one candidate. Missing features contribute nothing and are
    /// reported instead of silently ranking as zero-valued hits.
    pub fn evaluate(
        &self,
        source: &impl ScoringFeatureSource,
        reference_time_millis: u64,
    ) -> ScoringEvaluation {
        let mut missing_features = Vec::new();
        let mut term_contributions = Vec::with_capacity(self.terms.len());
        let mut combined_score = 0.0;
        for term in &self.terms {
            let value = match self.term_value(&term.feature, source) {
                Some(value) => value,
                None => {
                    missing_features.push(term.feature.clone());
                    0.0
                }
            };
            let contribution = term.weight * value;
            combined_score += contribution;
            term_contributions.push(contribution);
        }
        let mut decay_factors = Vec::with_capacity(self.decay.len());
        for decay in &self.decay {
            let age = match self.decay_age(&decay.feature, source, reference_time_millis) {
                Some(age) => age,
                None => {
                    missing_features.push(decay.feature.clone());
                    decay_factors.push(1.0);
                    continue;
                }
            };
            let factor = 0.5f64
                .powf(age / decay.half_life)
                .clamp(decay.min_factor, 1.0);
            combined_score *= factor;
            decay_factors.push(factor);
        }
        ScoringEvaluation {
            combined_score,
            term_contributions,
            decay_factors,
            missing_features,
        }
    }

    fn term_value(
        &self,
        feature: &ScoreFeature,
        source: &impl ScoringFeatureSource,
    ) -> Option<f64> {
        match feature {
            ScoreFeature::SearchScore => source.search_score().filter(|v| v.is_finite()),
            ScoreFeature::GraphSeedScore => source.graph_seed_score().filter(|v| v.is_finite()),
            ScoreFeature::HopDistance => source
                .hop_distance()
                .map(|hops| u32::try_from(hops).map_or(f64::from(u32::MAX), f64::from)),
            ScoreFeature::NodeProperty(property) => {
                source.numeric_property(property).filter(|v| v.is_finite())
            }
            // Decay-only features carry no additive value.
            ScoreFeature::TimestampProperty(_) => None,
        }
    }

    fn decay_age(
        &self,
        feature: &ScoreFeature,
        source: &impl ScoringFeatureSource,
        reference_time_millis: u64,
    ) -> Option<f64> {
        match feature {
            ScoreFeature::HopDistance => source.hop_distance().map(|hops| hops as f64),
            ScoreFeature::TimestampProperty(property) => {
                let timestamp = source.timestamp_millis(property)?;
                let age_millis = reference_time_millis.saturating_sub(timestamp);
                Some(age_millis as f64 / 1_000.0)
            }
            _ => None,
        }
    }
}

fn score_feature_name(feature: &ScoreFeature) -> String {
    match feature {
        ScoreFeature::SearchScore => "search_score".to_string(),
        ScoreFeature::GraphSeedScore => "graph_seed_score".to_string(),
        ScoreFeature::HopDistance => "hop_distance".to_string(),
        ScoreFeature::NodeProperty(property) => format!("node_property:{property}"),
        ScoreFeature::TimestampProperty(property) => format!("timestamp:{property}"),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeCandidateScoreBreakdown {
    pub search_score: Option<f64>,
    pub graph_seed_score: Option<f64>,
    pub combined_score: f64,
    /// Per-term provenance when a typed scoring spec drove this rerank.
    pub scoring_spec: Option<ScoringEvaluation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeCandidateSource {
    SearchHit,
    GraphSeed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeGraphSeed {
    pub entity: KnowledgeEntity,
    pub score: f64,
    pub matched_properties: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeEvidence {
    pub hit_id: String,
    pub kind: Option<String>,
    pub external_id: Option<String>,
    pub source_id: Option<String>,
    pub canonical_node_id: Option<u64>,
    pub graph_context_path_count: usize,
    pub matched_terms: Vec<String>,
    pub matched_spans: Vec<SearchMatchedSpan>,
    pub score: f64,
    pub rrf_score: f64,
    pub vector_rrf_score: f64,
    pub text_rrf_score: f64,
    pub vector_score: f64,
    pub text_score: f64,
    pub vector_rank: Option<usize>,
    pub text_rank: Option<usize>,
}

#[cfg(test)]
mod contract_tests {
    use super::{
        DecayTerm, KnowledgeRetrievalPipelineReport, KnowledgeRetrievalStage, ScoreFeature,
        ScoringFeatureSource, ScoringSpec, ScoringSpecError, ScoringTerm,
    };
    use std::any::TypeId;
    use std::collections::BTreeMap;

    #[test]
    fn pipeline_contract_preserves_search_owner_identity() {
        assert_eq!(
            TypeId::of::<KnowledgeRetrievalStage>(),
            TypeId::of::<hawdb_search::KnowledgeRetrievalStage>(),
        );
        assert_eq!(
            TypeId::of::<KnowledgeRetrievalPipelineReport>(),
            TypeId::of::<hawdb_search::KnowledgeRetrievalPipelineReport>(),
        );
        assert_eq!(
            KnowledgeRetrievalStage::TopK.as_str(),
            hawdb_search::KnowledgeRetrievalStage::TopK.as_str(),
        );
    }

    #[derive(Default)]
    struct StubFeatures {
        search_score: Option<f64>,
        graph_seed_score: Option<f64>,
        hop_distance: Option<usize>,
        numeric: BTreeMap<String, f64>,
        timestamps: BTreeMap<String, u64>,
    }

    impl ScoringFeatureSource for StubFeatures {
        fn search_score(&self) -> Option<f64> {
            self.search_score
        }

        fn graph_seed_score(&self) -> Option<f64> {
            self.graph_seed_score
        }

        fn hop_distance(&self) -> Option<usize> {
            self.hop_distance
        }

        fn numeric_property(&self, property: &str) -> Option<f64> {
            self.numeric.get(property).copied()
        }

        fn timestamp_millis(&self, property: &str) -> Option<u64> {
            self.timestamps.get(property).copied()
        }
    }

    fn term(weight: f64, feature: ScoreFeature) -> ScoringTerm {
        ScoringTerm { weight, feature }
    }

    fn decay(feature: ScoreFeature, half_life: f64, min_factor: f64) -> DecayTerm {
        DecayTerm {
            feature,
            half_life,
            min_factor,
        }
    }

    #[test]
    fn scoring_spec_combines_weighted_features() {
        let spec = ScoringSpec {
            terms: vec![
                term(0.5, ScoreFeature::SearchScore),
                term(2.0, ScoreFeature::NodeProperty("pagerank".to_string())),
            ],
            decay: Vec::new(),
        };
        let source = StubFeatures {
            search_score: Some(4.0),
            numeric: BTreeMap::from([("pagerank".to_string(), 3.0)]),
            ..StubFeatures::default()
        };
        let evaluation = spec.evaluate(&source, 0);
        assert_eq!(evaluation.combined_score, 0.5 * 4.0 + 2.0 * 3.0);
        assert_eq!(evaluation.term_contributions, vec![2.0, 6.0]);
        assert!(evaluation.missing_features.is_empty());
    }

    #[test]
    fn scoring_spec_reports_missing_features_instead_of_ranking_them_zero() {
        let spec = ScoringSpec {
            terms: vec![
                term(1.0, ScoreFeature::SearchScore),
                term(1.0, ScoreFeature::GraphSeedScore),
            ],
            decay: Vec::new(),
        };
        let source = StubFeatures {
            search_score: Some(2.0),
            ..StubFeatures::default()
        };
        let evaluation = spec.evaluate(&source, 0);
        assert_eq!(evaluation.combined_score, 2.0);
        assert_eq!(
            evaluation.missing_features,
            vec![ScoreFeature::GraphSeedScore]
        );
    }

    #[test]
    fn hop_decay_multiplies_and_holds_its_floor() {
        let spec = ScoringSpec {
            terms: vec![term(1.0, ScoreFeature::SearchScore)],
            decay: vec![decay(ScoreFeature::HopDistance, 1.0, 0.25)],
        };
        let evaluate = |hops: usize| {
            spec.evaluate(
                &StubFeatures {
                    search_score: Some(8.0),
                    hop_distance: Some(hops),
                    ..StubFeatures::default()
                },
                0,
            )
        };
        assert_eq!(evaluate(0).combined_score, 8.0);
        assert_eq!(evaluate(1).combined_score, 4.0);
        assert_eq!(evaluate(2).combined_score, 2.0);
        assert_eq!(evaluate(9).combined_score, 2.0);
        assert_eq!(evaluate(2).decay_factors, vec![0.25]);
    }

    #[test]
    fn timestamp_decay_ages_against_the_request_clock() {
        let spec = ScoringSpec {
            terms: vec![term(1.0, ScoreFeature::SearchScore)],
            decay: vec![decay(
                ScoreFeature::TimestampProperty("updated_at".to_string()),
                600.0,
                0.0,
            )],
        };
        let aged = StubFeatures {
            search_score: Some(10.0),
            timestamps: BTreeMap::from([("updated_at".to_string(), 400_000)]),
            ..StubFeatures::default()
        };
        // 600 seconds old against a 600-second half-life.
        assert_eq!(spec.evaluate(&aged, 1_000_000).combined_score, 5.0);
        let future = StubFeatures {
            search_score: Some(10.0),
            timestamps: BTreeMap::from([("updated_at".to_string(), 2_000_000)]),
            ..StubFeatures::default()
        };
        assert_eq!(spec.evaluate(&future, 1_000_000).combined_score, 10.0);
    }

    #[test]
    fn scoring_spec_validation_rejects_unusable_shapes() {
        let missing = StubFeatures::default();
        assert_eq!(
            ScoringSpec {
                terms: Vec::new(),
                decay: Vec::new(),
            }
            .validate(),
            Err(ScoringSpecError::NoTerms)
        );
        assert_eq!(
            ScoringSpec {
                terms: vec![term(-1.0, ScoreFeature::SearchScore)],
                decay: Vec::new(),
            }
            .validate(),
            Err(ScoringSpecError::NegativeWeight)
        );
        assert_eq!(
            ScoringSpec {
                terms: vec![term(f64::NAN, ScoreFeature::SearchScore)],
                decay: Vec::new(),
            }
            .validate(),
            Err(ScoringSpecError::NonFiniteWeight)
        );
        assert_eq!(
            ScoringSpec {
                terms: vec![term(1.0, ScoreFeature::SearchScore)],
                decay: vec![decay(ScoreFeature::HopDistance, 0.0, 0.0)],
            }
            .validate(),
            Err(ScoringSpecError::NonPositiveHalfLife)
        );
        assert_eq!(
            ScoringSpec {
                terms: vec![term(1.0, ScoreFeature::SearchScore)],
                decay: vec![decay(ScoreFeature::HopDistance, 1.0, 1.5)],
            }
            .validate(),
            Err(ScoringSpecError::InvalidMinFactor)
        );
        assert_eq!(
            ScoringSpec {
                terms: vec![term(1.0, ScoreFeature::TimestampProperty("t".to_string()))],
                decay: Vec::new(),
            }
            .validate(),
            Err(ScoringSpecError::TimestampTermNotAllowed)
        );
        assert_eq!(
            ScoringSpec {
                terms: vec![term(1.0, ScoreFeature::SearchScore)],
                decay: vec![decay(ScoreFeature::SearchScore, 1.0, 0.0)],
            }
            .validate(),
            Err(ScoringSpecError::UnsupportedDecayFeature)
        );
        assert!(ScoringSpec::weighted_scores(1.0, 1.0).validate().is_ok());
        // A property that is merely absent at runtime is valid, and reported
        // through `missing_features` instead of failing the request.
        assert!(ScoringSpec {
            terms: vec![term(1.0, ScoreFeature::NodeProperty("absent".to_string()))],
            decay: Vec::new(),
        }
        .validate()
        .is_ok());
        assert!(missing.numeric_property("absent").is_none());
    }

    #[test]
    fn scoring_shape_fingerprint_ignores_weights() {
        let lightness = ScoringSpec {
            terms: vec![
                term(0.1, ScoreFeature::SearchScore),
                term(0.2, ScoreFeature::HopDistance),
            ],
            decay: vec![decay(ScoreFeature::HopDistance, 30.0, 0.1)],
        };
        let heavy = ScoringSpec {
            terms: vec![
                term(9.0, ScoreFeature::SearchScore),
                term(5.0, ScoreFeature::HopDistance),
            ],
            decay: vec![decay(ScoreFeature::HopDistance, 1.0, 0.9)],
        };
        assert_eq!(lightness.shape_fingerprint(), heavy.shape_fingerprint());
        assert!(!lightness.needs_canonical_node_properties());
        let property = ScoringSpec {
            terms: vec![term(
                1.0,
                ScoreFeature::NodeProperty("pagerank".to_string()),
            )],
            decay: Vec::new(),
        };
        assert!(property.needs_canonical_node_properties());
    }
}
