use super::super::*;
use super::*;

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

pub use skein_search::{
    SearchProjectionChangeBatch, SearchProjectionGraphDeltaRequest, SearchProjectionRelationalDelta,
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
    pub include_skein_lightning_bootstrap_export: bool,
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
            include_skein_lightning_bootstrap_export: true,
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
    SkeinLightningBootstrapExport,
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
            BackgroundMaintenanceKind::SkeinLightningBootstrapExport => {
                "skein_lightning_bootstrap_export"
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
            "skein_lightning_bootstrap_export" => {
                Ok(BackgroundMaintenanceKind::SkeinLightningBootstrapExport)
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
    pub(in crate::api) fn from_ranked(
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
                qos_admission_name(&foreground_admission).to_string(),
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
            admission_name: qos_admission_name(&ranked.decision.admission).to_string(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeRetrievalStage {
    SearchCandidate,
    MetadataFilter,
    AuthorizedGraphExpand,
    Rerank,
    TopK,
    CanonicalHydration,
}

impl KnowledgeRetrievalStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SearchCandidate => "search_candidate",
            Self::MetadataFilter => "metadata_filter",
            Self::AuthorizedGraphExpand => "authorized_graph_expand",
            Self::Rerank => "rerank",
            Self::TopK => "top_k",
            Self::CanonicalHydration => "canonical_hydration",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRetrievalPipelineReport {
    pub stages: Vec<KnowledgeRetrievalStage>,
    pub graph_snapshot_commit_epoch: u64,
    pub query_memory_budget_bytes: usize,
    pub peak_tracked_memory_bytes: usize,
    pub result_payload_budget_bytes: usize,
    pub result_payload_bytes: usize,
    pub canonical_identity_filtered_out_count: usize,
    pub canonical_output_hydrated_node_count: usize,
    pub canonical_output_hydrated_candidate_count: usize,
    pub canonical_output_hydration_after_top_k: bool,
    pub metadata_filter_authorized_graph_expansion: bool,
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
    pub(in crate::api) fn dense_adjacency(
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

    pub(in crate::api) fn graph_context_limit(limit: usize, seed_hit_id: &str) -> Self {
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

    pub(in crate::api) fn graph_seed_limit(limit: usize, total: usize) -> Self {
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

    pub(in crate::api) fn candidate_limit(limit: usize, total: usize) -> Self {
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

    #[cfg(test)]
    pub(in crate::api) fn path_limit(operation: &str, limit: usize, target: &str) -> Self {
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

    #[cfg(test)]
    pub(in crate::api) fn node_limit(limit: usize) -> Self {
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

    #[cfg(test)]
    pub(in crate::api) fn relationship_limit(limit: usize) -> Self {
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum KnowledgeCandidateScoringPolicy {
    Max,
    WeightedSum {
        search_weight: f64,
        graph_seed_weight: f64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KnowledgeCandidateScoreBreakdown {
    pub search_score: Option<f64>,
    pub graph_seed_score: Option<f64>,
    pub combined_score: f64,
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
